defmodule Libremarket.Envio do
  def calcular_costo_envio() do
    :rand.uniform(100)
  end

  def enviar_producto() do
    IO.puts("Producto enviado correctamente")
  end

  def agendar_envio() do
    IO.puts("Producto agendado para envío correctamente")
  end
end

defmodule Libremarket.Envio.Server do
  use GenServer
  require Logger
  alias Libremarket.Replicacion

  @global_name {:global, __MODULE__}

  # ==========================
  # START
  # ==========================
  def start_link(opts \\ %{}) do
    container = System.get_env("CONTAINER_NAME") || "default"

    wait_for_leader()

    {:global, base} = @global_name
    name = {:global, :"#{base}_#{container}"}

    IO.puts("📡 [ENVIO] nombre #{inspect(name)}")

    {:ok, pid} = GenServer.start_link(__MODULE__, opts, name: name)

    Process.register(pid, __MODULE__)
    Libremarket.Replicacion.Registry.registrar(__MODULE__, container, pid)

    {:ok, pid}
  end

  defp wait_for_leader() do
    if Process.whereis(Libremarket.Envio.Leader) == nil do
      IO.puts("⏳ Esperando Libremarket.Envio.Leader...")
      :timer.sleep(500)
      wait_for_leader()
    else
      :ok
    end
  end

  def replicas() do
    me = GenServer.whereis(local_name())

    Libremarket.Replicacion.Registry.replicas(__MODULE__)
    |> Enum.reject(&(&1 == me))
  end

  defp local_name() do
    container = System.get_env("CONTAINER_NAME") || "default"
    {:global, base} = @global_name
    {:global, :"#{base}_#{container}"}
  end

  # ==========================
  # API
  # ==========================
  def calcular_costo_envio(pid \\ @global_name, id),
    do: GenServer.call(pid, {:calcular_costo_envio, id})

  def enviar_producto(pid \\ @global_name, id, costo),
    do: GenServer.call(pid, {:enviar_producto, id, costo})

  def agendar_envio(pid \\ @global_name, id),
    do: GenServer.call(pid, {:agendar_envio, id})

  def obtener_envios(),
    do: GenServer.call(@global_name, :obtener_envios)

  # ==========================
  # INIT
  # ==========================
  @impl true
  def init(_opts) do
    {:ok, %{}, {:continue, :start_amqp_if_leader}}
  end

  @impl true
  def handle_continue(:start_amqp_if_leader, state) do
    if Libremarket.Envio.Leader.leader?() do
      register_as_leader()

      Supervisor.start_child(
        Libremarket.Supervisor,
        {Libremarket.Envio.AMQP, %{}}
      )
    end

    {:noreply, state}
  end

  # ==========================
  # CALLS
  # ==========================
  @impl true
  def handle_call({:calcular_costo_envio, id}, _from, state) do
    if Libremarket.Envio.Leader.leader?() do
      costo = Libremarket.Envio.calcular_costo_envio()
      new_state = Map.put(state, id, costo)

      Replicacion.replicar_estado(new_state, replicas(), __MODULE__)
      {:reply, costo, new_state}
    else
      {:reply, :replica, state}
    end
  end

  @impl true
  def handle_call({:enviar_producto, id, costo}, _from, state) do
    if Libremarket.Envio.Leader.leader?() do
      Libremarket.Envio.enviar_producto()
      new_state = Map.put(state, id, %{estado: :enviado, precio_envio: costo})

      Replicacion.replicar_estado(new_state, replicas(), __MODULE__)
      {:reply, %{estado: :enviado, precio_envio: costo}, new_state}
    else
      {:reply, :replica, state}
    end
  end

  @impl true
  def handle_call({:agendar_envio, id}, _from, state) do
    if Libremarket.Envio.Leader.leader?() do
      Libremarket.Envio.agendar_envio()
      new_state = Map.put(state, id, %{estado: :agendado})

      Replicacion.replicar_estado(new_state, replicas(), __MODULE__)
      {:reply, %{estado: :agendado}, new_state}
    else
      {:reply, :replica, state}
    end
  end

  @impl true
  def handle_call(:obtener_envios, _from, state),
    do: {:reply, state, state}

  @impl true
  def handle_call({:sync_state, new}, _from, _old) do
    Logger.info("📡 [ENVIO] Estado sincronizado (#{map_size(new)})")
    {:reply, :ok, new}
  end

  # ==========================
  # LÍDER DINÁMICO
  # ==========================
  def lider_cambio(es_lider) do
    GenServer.cast(__MODULE__, {:lider_cambio, es_lider})
  end

  @impl true
  def handle_cast({:lider_cambio, true}, state) do
    IO.puts("🟢 [ENVIO] Ahora soy líder → levantando AMQP")
    register_as_leader()

    Supervisor.start_child(
      Libremarket.Supervisor,
      {Libremarket.Envio.AMQP, %{}}
    )

    {:noreply, state}
  end

  @impl true
  def handle_cast({:lider_cambio, false}, state) do
    IO.puts("🔴 [ENVIO] Perdí liderazgo → sigo como réplica")
    unregister_as_leader()
    {:noreply, state}
  end

  # ==========================
  # GLOBAL NAMES
  # ==========================
  defp register_as_leader() do
    # eliminar alias viejo si no es este proceso
    case :global.whereis_name(__MODULE__) do
      pid when is_pid(pid) and pid != self() ->
        :global.unregister_name(__MODULE__)

      _ ->
        :ok
    end

    # eliminar nombres "Server_envio-2" si apuntan a este PID
    Enum.each(:global.registered_names(), fn name ->
      if name != __MODULE__ and
           String.starts_with?(to_string(name), "Elixir.Libremarket.Envio.Server_") and
           :global.whereis_name(name) == self() do
        :global.unregister_name(name)
      end
    end)

    :global.register_name(__MODULE__, self())
  end

  defp unregister_as_leader() do
    if :global.whereis_name(__MODULE__) == self() do
      :global.unregister_name(__MODULE__)
    end

    :ok
  end
end

defmodule Libremarket.Envio.AMQP do
  @moduledoc "Worker AMQP de Envíos: consume envios.req y publica compras.resp (tipo: \"envio\")"
  use GenServer
  require Logger
  alias AMQP.{Connection, Channel, Queue, Basic}

  @envios_req_q "envios.req"
  @envios_resp_q "compras.envios.resp"
  @exchange ""

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @impl true
  def init(_state) do
    amqp_url = System.get_env("AMQP_URL")
    {:ok, conn} = Connection.open(amqp_url, ssl_options: [verify: :verify_none])
    {:ok, chan} = Channel.open(conn)

    {:ok, _} = Queue.declare(chan, @envios_req_q, durable: false, auto_delete: false)
    :ok = Basic.qos(chan, prefetch_count: 0)
    {:ok, _} = Basic.consume(chan, @envios_req_q, nil, no_ack: false)

    Logger.info("Envio.AMQP conectado y escuchando #{@envios_req_q}")
    {:ok, %{conn: conn, chan: chan}}
  end

  @impl true
  def handle_info({:basic_deliver, payload, meta}, %{chan: chan} = st) do
    %{"id_compra" => id_compra, "accion" => accion} = Jason.decode!(payload)

    case accion do
      "enviar" ->
        costo = Libremarket.Envio.calcular_costo_envio()
        _ = Libremarket.Envio.Server.enviar_producto(id_compra, costo)

        resp =
          Jason.encode!(%{
            tipo: "envio",
            id_compra: id_compra,
            estado: "enviado",
            precio_envio: costo
          })

        Basic.publish(chan, @exchange, @envios_resp_q, resp, content_type: "application/json")

      "agendar" ->
        _ = Libremarket.Envio.Server.agendar_envio(id_compra)

        resp =
          Jason.encode!(%{
            tipo: "envio",
            id_compra: id_compra,
            estado: "agendado"
          })

        Basic.publish(chan, @exchange, @envios_resp_q, resp, content_type: "application/json")

      other ->
        Logger.warning("Envio.AMQP: acción desconocida #{inspect(other)}")
    end

    Basic.ack(chan, meta.delivery_tag)
    {:noreply, st}
  end

  @impl true
  def handle_info({:basic_consume_ok, _}, st), do: {:noreply, st}
  def handle_info({:basic_cancel, _}, st), do: {:stop, :normal, st}
  def handle_info({:basic_cancel_ok, _}, st), do: {:noreply, st}
  def handle_info(_other, st), do: {:noreply, st}

  @impl true
  def terminate(_reason, %{chan: chan, conn: conn}) do
    Channel.close(chan)
    Connection.close(conn)
    :ok
  end
end

defmodule Libremarket.Envio.Leader do
  use GenServer

  @base_path "/libremarket/envio"
  @leader_path "/libremarket/envio/leader"
  @interval 2_000

  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  def leader?(),
    do: GenServer.call(__MODULE__, :leader?)

  @impl true
  def init(_opts) do
    # ETS global
    case :ets.whereis(:lider_envio) do
      :undefined -> :ets.new(:lider_envio, [:named_table, :public])
      _ -> :ok
    end

    {:ok, zk} = Libremarket.ZK.connect()

    wait_for_zk(zk, @base_path)
    wait_for_zk(zk, @leader_path)

    {:ok, my_znode} =
      :erlzk.create(zk, @leader_path <> "/nodo-", :ephemeral_sequential)

    leader? = compute_leader?(zk, my_znode)

    :ets.insert(:lider_envio, {:is_leader, leader?})

    IO.puts("🟣 [ENVIO] Soy líder? #{leader?} (#{my_znode})")

    Process.send_after(self(), :check_leader, @interval)

    {:ok, %{zk: zk, my_znode: my_znode, leader?: leader?}}
  end

  @impl true
  def handle_call(:leader?, _from, st),
    do: {:reply, st.leader?, st}

  @impl true
  def handle_info(:check_leader, %{zk: zk, my_znode: my_znode} = st) do
    new_state =
      case :erlzk.get_children(zk, @leader_path) do
        {:ok, children} ->
          sorted = children |> Enum.map(&List.to_string/1) |> Enum.sort()
          my_name = Path.basename(List.to_string(my_znode))
          new_leader = hd(sorted) == my_name

          if new_leader != st.leader? do
            IO.puts("🔄 [ENVIO] Cambio de liderazgo #{st.leader?} → #{new_leader}")
            :ets.insert(:lider_envio, {:is_leader, new_leader})
            Libremarket.Envio.Server.lider_cambio(new_leader)
          end

          %{st | leader?: new_leader}

        {:error, _} ->
          IO.puts("⚠️ [ENVIO] Znode perdido → recreando nodo")

          wait_for_zk(zk, @leader_path)

          {:ok, new_z} =
            :erlzk.create(zk, @leader_path <> "/nodo-", :ephemeral_sequential)

          new_leader = compute_leader?(zk, new_z)

          if new_leader != st.leader? do
            IO.puts("🔄 [ENVIO] Liderazgo recreado → #{new_leader}")
            Libremarket.Envio.Server.lider_cambio(new_leader)
          end

          %{st | my_znode: new_z, leader?: new_leader}
      end

    Process.send_after(self(), :check_leader, @interval)
    {:noreply, new_state}
  end

  # ==========================
  # HELPERS
  # ==========================
  defp compute_leader?(zk, my_z) do
    {:ok, children} = :erlzk.get_children(zk, @leader_path)

    sorted =
      children
      |> Enum.map(&List.to_string/1)
      |> Enum.sort()

    my_name =
      my_z
      |> List.to_string()
      |> Path.basename()

    hd(sorted) == my_name
  end

  defp wait_for_zk(zk, path, retries \\ 5)

  defp wait_for_zk(_zk, _path, 0),
    do: raise("ZooKeeper no respondió creando path")

  defp wait_for_zk(zk, path, retries) do
    case Libremarket.ZK.ensure_path(zk, path) do
      :ok ->
        :ok

      {:error, _} ->
        IO.puts("⚠️ reintentando crear #{path}")
        :timer.sleep(800)
        wait_for_zk(zk, path, retries - 1)
    end
  end
end
