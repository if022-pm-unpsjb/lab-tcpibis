defmodule Libremarket.Infracciones do
  def detectar_infraccion(_id_compra) do
    probabilidad = :rand.uniform(100)
    probabilidad > 70
  end
end

defmodule Libremarket.Infracciones.Server do
  @moduledoc """
  Infracciones
  """

  use GenServer
  require Logger
  alias Libremarket.Replicacion

  @global_name {:global, __MODULE__}

  @doc """
  Crea un nuevo servidor de Infracciones
  """
  def start_link(opts \\ %{}) do
    container_name = System.get_env("CONTAINER_NAME") || "default"

    # 🔧 Espera activa hasta que el Leader esté registrado
    wait_for_leader()

    {:global, base_name} = @global_name

    # 👉 SIEMPRE: nombre global por contenedor (para réplicas y leader)
    name = {:global, :"#{base_name}_#{container_name}"}

    IO.puts("📡nombre #{inspect(name)}")

    {:ok, pid} = GenServer.start_link(__MODULE__, opts, name: name)

    # 👉 nombre LOCAL para que el Leader de este nodo le hable
    Process.register(pid, __MODULE__)

    Libremarket.Replicacion.Registry.registrar(__MODULE__, container_name, pid)
    {:ok, pid}
  end

  defp wait_for_leader() do
    if Process.whereis(Libremarket.Infracciones.Leader) == nil do
      IO.puts("⏳ Esperando a que arranque Libremarket.Infracciones.Leader...")
      :timer.sleep(500)
      wait_for_leader()
    else
      :ok
    end
  end

  def replicas() do
    my_pid = GenServer.whereis(local_name())

    Libremarket.Replicacion.Registry.replicas(__MODULE__)
    |> Enum.reject(&(&1 == my_pid))
  end

  defp local_name() do
    container = System.get_env("CONTAINER_NAME") || "default"
    {:global, base_name} = @global_name
    {:global, :"#{base_name}_#{container}"}
  end

  def detectar_infraccion(pid \\ @global_name, id_compra) do
    GenServer.call(pid, {:detectar_infraccion, id_compra})
  end

  def listar_infracciones() do
    GenServer.call(@global_name, :listar_infracciones)
  end

  def actualizar_estado() do
    GenServer.call(@global_name, :actualizar_estado)
  end

  @doc """
  Inicializa el estado del servidor
  """
  @impl true
  def init(_state) do
    {:ok, %{}, {:continue, :start_amqp_if_leader}}
  end

  @impl true
  def handle_continue(:start_amqp_if_leader, state) do
    if Libremarket.Infracciones.Leader.leader?() do
      register_as_leader()

      Supervisor.start_child(
        Libremarket.Supervisor,
        {Libremarket.Infracciones.AMQP, %{}}
      )
    end

    {:noreply, state}
  end

  @doc """
  Callback para un call :detectar_infraccion
  """
  @impl true
  def handle_call({:detectar_infraccion, id_compra}, _from, state) do
    if Libremarket.Infracciones.Leader.leader?() do
      infraccion = Libremarket.Infracciones.detectar_infraccion(id_compra)
      new_state = Map.put(state, id_compra, infraccion)
      Replicacion.replicar_estado(new_state, replicas(), __MODULE__)
      {:reply, infraccion, new_state}
    else
      Logger.warning("Nodo réplica (no líder) no debe detectar infracciones directamente")
      {:reply, :replica, state}
    end
  end

  @impl true
  def handle_call(:listar_infracciones, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call(:actualizar_estado, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call({:sync_state, new_state}, _from, _old_state) do
    Logger.info("📡 Estado sincronizado por llamada directa (#{map_size(new_state)} entradas)")
    {:reply, :ok, new_state}
  end

  def lider_cambio(es_lider) do
    # 👉 hablar SIEMPRE con el server local del nodo
    GenServer.cast(__MODULE__, {:lider_cambio, es_lider})
  end

  @impl true
  def handle_cast({:lider_cambio, true}, state) do
    IO.puts("🟢 Ahora soy líder → levantando AMQP")

    register_as_leader()

    Supervisor.start_child(
      Libremarket.Supervisor,
      {Libremarket.Infracciones.AMQP, %{}}
    )

    {:noreply, state}
  end

  @impl true
  def handle_cast({:lider_cambio, false}, state) do
    IO.puts("🔴 Perdí liderazgo → sigo como réplica")
    unregister_as_leader()
    {:noreply, state}
  end

  defp register_as_leader() do
    # Si existe el alias global del líder apuntando a otro proceso → borrarlo
    case :global.whereis_name(__MODULE__) do
      pid when is_pid(pid) and pid != self() ->
        :global.unregister_name(__MODULE__)

      _ ->
        :ok
    end

    # Si este mismo proceso tenía un nombre global con sufijo → borrarlo
    # Ej: :"Elixir.Libremarket.Infracciones.Server_infracciones_2"
    Enum.each(:global.registered_names(), fn name ->
      if name != __MODULE__ and
           String.starts_with?(to_string(name), "Elixir.Libremarket.Infracciones.Server_") do
        if :global.whereis_name(name) == self() do
          :global.unregister_name(name)
        end
      end
    end)

    # Registrar alias líder
    case :global.register_name(__MODULE__, self()) do
      :yes -> :ok
      {:error, :already_registered} -> :ok
    end
  end

  defp unregister_as_leader() do
    case :global.whereis_name(__MODULE__) do
      pid when pid == self() ->
        :global.unregister_name(__MODULE__)

      _ ->
        :ok
    end
  end
end

defmodule Libremarket.Infracciones.Leader do
  use GenServer

  @base_path "/libremarket/infracciones"
  @leader_path "/libremarket/infracciones/leader"
  @interval 2_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def leader?(), do: GenServer.call(__MODULE__, :leader?)

  @impl true
  def init(_opts) do
    # Crear ETS global del nodo si no existe
    case :ets.whereis(:lider_infracciones) do
      :undefined ->
        :ets.new(:lider_infracciones, [:named_table, :public, read_concurrency: true])

      _ ->
        :ok
    end

    {:ok, zk} = Libremarket.ZK.connect()
    wait_for_zk(zk, @base_path)
    wait_for_zk(zk, @leader_path)

    {:ok, my_znode} =
      :erlzk.create(zk, @leader_path <> "/nodo-", :ephemeral_sequential)

    leader? = compute_leader?(zk, my_znode)

    # Guardar estado inicial de liderazgo
    :ets.insert(:lider_infracciones, {:is_leader, leader?})

    IO.puts("🟣 Soy líder? #{leader?} (#{my_znode})")

    Process.send_after(self(), :check_leader, @interval)
    {:ok, %{zk: zk, my_znode: my_znode, leader?: leader?}}
  end

  @impl true
  def handle_call(:leader?, _from, state) do
    {:reply, state.leader?, state}
  end

  @impl true
  def handle_info(:check_leader, %{zk: zk, my_znode: my_znode} = state) do
    new_state =
      case :erlzk.get_children(zk, @leader_path) do
        {:ok, children} ->
          sorted = Enum.map(children, &List.to_string/1) |> Enum.sort()
          my_name = List.to_string(my_znode) |> Path.basename()
          new_leader = hd(sorted) == my_name

          if new_leader != state.leader? do
            IO.puts(
              "🔄 [Infracciones.Leader] cambio de liderazgo: #{state.leader?} -> #{new_leader}"
            )

            :ets.insert(:lider_infracciones, {:is_leader, new_leader})
            Libremarket.Infracciones.Server.lider_cambio(new_leader)
          end

          %{state | leader?: new_leader}

        {:error, _} ->
          IO.puts("⚠️ No hay líderes → recreando nodo")

          wait_for_zk(zk, @leader_path)

          {:ok, new_znode} =
            :erlzk.create(zk, @leader_path <> "/nodo-", :ephemeral_sequential)

          new_leader = compute_leader?(zk, new_znode)

          # 🔄 Notificar liderazgo recién recreado
          if new_leader != state.leader? do
            IO.puts("🔄 Cambio de liderazgo (recreado): ahora líder=#{new_leader}")
            Libremarket.Infracciones.Server.lider_cambio(new_leader)
          end

          %{state | my_znode: new_znode, leader?: new_leader}
      end

    # 🔁 Programar próximo chequeo
    Process.send_after(self(), :check_leader, @interval)

    {:noreply, new_state}
  end

  defp compute_leader?(zk, my_znode) do
    {:ok, children} = :erlzk.get_children(zk, @leader_path)

    sorted =
      children
      |> Enum.map(&List.to_string/1)
      |> Enum.sort()

    my_name =
      my_znode
      |> List.to_string()
      |> Path.basename()

    [first | _] = sorted
    my_name == first
  end

  defp wait_for_zk(zk, path, retries \\ 5)

  defp wait_for_zk(_zk, path, 0),
    do: raise("ZooKeeper no respondió creando #{path}")

  defp wait_for_zk(zk, path, retries) do
    case Libremarket.ZK.ensure_path(zk, path) do
      :ok ->
        :ok

      {:error, _} ->
        IO.puts("⚠️ reintentando crear #{path}…")
        :timer.sleep(800)
        wait_for_zk(zk, path, retries - 1)
    end
  end
end

defmodule Libremarket.Infracciones.AMQP do
  @moduledoc "Proceso que consume infracciones.req y publica resultados en compras.resp"
  use GenServer
  require Logger
  alias AMQP.{Connection, Channel, Queue, Basic}

  @infracciones_req_q "infracciones.req"
  @infracciones_resp_q "compras.infracciones.resp"
  @exchange ""

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @impl true
  def init(_state) do
    amqp_url = System.get_env("AMQP_URL")

    {:ok, conn} = Connection.open(amqp_url, ssl_options: [verify: :verify_none])
    {:ok, chan} = Channel.open(conn)

    {:ok, _} = Queue.declare(chan, @infracciones_req_q, durable: false, auto_delete: false)
    :ok = Basic.qos(chan, prefetch_count: 0)
    {:ok, _ctag} = Basic.consume(chan, @infracciones_req_q, nil, no_ack: false)

    Logger.info("Infracciones.AMQP conectado y escuchando #{@infracciones_req_q}")
    {:ok, %{conn: conn, chan: chan}}
  end

  @impl true
  def handle_info({:basic_consume_ok, %{consumer_tag: _tag}}, state) do
    Logger.info("Consumidor registrado en #{@infracciones_req_q}")
    {:noreply, state}
  end

  @impl true
  def handle_info({:basic_cancel, %{consumer_tag: _tag}}, state) do
    Logger.warning("Consumo cancelado en #{@infracciones_req_q}")
    {:stop, :normal, state}
  end

  @impl true
  def handle_info({:basic_cancel_ok, %{consumer_tag: _tag}}, state) do
    Logger.info("Cancelación confirmada de consumo en #{@infracciones_req_q}")
    {:noreply, state}
  end

  @impl true
  def handle_info({:basic_deliver, payload, meta}, %{chan: chan} = st) do
    Logger.info("Mensaje recibido: #{inspect(payload)}")

    %{"id_compra" => id} = Jason.decode!(payload)
    infr? = Libremarket.Infracciones.Server.detectar_infraccion(id)
    response = Jason.encode!(%{id_compra: id, infraccion: infr?})

    {:ok, _} = Queue.declare(chan, @infracciones_resp_q, durable: false, auto_delete: false)

    :ok =
      Basic.publish(chan, @exchange, @infracciones_resp_q, response,
        content_type: "application/json",
        persistent: true
      )

    Logger.info("Verificación de infracción para #{id}: #{inspect(infr?)}")
    :ok = Basic.ack(chan, meta.delivery_tag)
    {:noreply, st}
  end

  @impl true
  def handle_info(_, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{chan: chan, conn: conn}) do
    Channel.close(chan)
    Connection.close(conn)
    :ok
  end
end
