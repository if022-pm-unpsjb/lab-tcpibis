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
  @moduledoc """
  Envio
  """

  use GenServer
  require Logger
  alias Libremarket.Replicacion

  @global_name {:global, __MODULE__}

  @doc """
  Crea un nuevo servidor de Envio
  """
  def start_link(opts \\ %{}) do
    container_name = System.get_env("CONTAINER_NAME") || "default"

    # 🔧 Espera activa hasta que el Leader esté registrado
    wait_for_leader()

    is_primary =
      case safe_leader_check() do
        {:ok, result} -> result
        _ -> false
      end

    {:global, base_name} = @global_name

    name =
      if is_primary do
        @global_name
      else
        {:global, :"#{base_name}_#{container_name}"}
      end

    IO.puts("📡nombre #{inspect(name)}")
    {:ok, pid} = GenServer.start_link(__MODULE__, opts, name: name)
    Libremarket.Replicacion.Registry.registrar(__MODULE__, container_name, pid)
    {:ok, pid}
  end

  defp wait_for_leader() do
    if Process.whereis(Libremarket.Envio.Leader) == nil do
      IO.puts("⏳ Esperando a que arranque Libremarket.Envio.Leader...")
      :timer.sleep(500)
      wait_for_leader()
    else
      :ok
    end
  end

  defp safe_leader_check() do
    try do
      {:ok, Libremarket.Envio.Leader.leader?()}
    catch
      :exit, _ -> {:error, :not_alive}
    end
  end

  def replicas() do
    my_pid = GenServer.whereis(local_name())

    Libremarket.Replicacion.Registry.replicas(__MODULE__)
    |> Enum.reject(&(&1 == my_pid))
  end

  defp local_name() do
    container = System.get_env("CONTAINER_NAME") || "default"
    is_primary = Libremarket.Envio.Leader.leader?()
    {:global, base_name} = @global_name
    if is_primary, do: @global_name, else: {:global, :"#{base_name}_#{container}"}
  end

  def calcular_costo_envio(pid \\ @global_name, id_compra) do
    GenServer.call(pid, {:calcular_costo_envio, id_compra})
  end

  def enviar_producto(pid \\ @global_name, id_compra, costo) do
    GenServer.call(pid, {:enviar_producto, id_compra, costo})
  end

  def agendar_envio(pid \\ @global_name, id_compra) do
    GenServer.call(pid, {:agendar_envio, id_compra})
  end

  def obtener_envios() do
    GenServer.call(@global_name, :obtener_envios)
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
    if Libremarket.Envio.Leader.leader?() do
      Supervisor.start_child(
        Libremarket.Supervisor,
        {Libremarket.Envio.AMQP, %{}}
      )
    end

    {:noreply, state}
  end

  @impl true
  def handle_call({:calcular_costo_envio, id_compra}, _from, state) do
    if Libremarket.Envio.Leader.leader?() do
      result = Libremarket.Envio.calcular_costo_envio()
      new_state = Map.put(state, id_compra, result)
      Replicacion.replicar_estado(new_state, replicas(), __MODULE__)
      {:reply, result, new_state}
    else
      Logger.warning("Nodo réplica no debe detectar calcular costo de envio directamente")
      {:reply, :replica, state}
    end
  end

  @impl true
  def handle_call({:enviar_producto, id_compra, costo}, _from, state) do
    if Libremarket.Envio.Leader.leader?() do
      Libremarket.Envio.enviar_producto()
      new_state = Map.put(state, id_compra, %{estado: :enviado, precio_envio: costo})
      Replicacion.replicar_estado(new_state, replicas(), __MODULE__)
      {:reply, %{estado: :enviado, precio_envio: costo}, new_state}
    else
      Logger.warning("Nodo réplica no debe detectar enviar productos directamente")
      {:reply, :replica, state}
    end
  end

  @impl true
  def handle_call({:agendar_envio, id_compra}, _from, state) do
    if Libremarket.Envio.Leader.leader?() do
      Libremarket.Envio.agendar_envio()
      new_state = Map.put(state, id_compra, %{estado: :agendado})
      Replicacion.replicar_estado(new_state, replicas(), __MODULE__)
      {:reply, %{estado: :agendado}, new_state}
    else
      Logger.warning("Nodo réplica no debe detectar agendar envios directamente")
      {:reply, :replica, state}
    end
  end

  def handle_call(:obtener_envios, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call({:sync_state, new_state}, _from, _old_state) do
    Logger.info("📡 Estado sincronizado por llamada directa (#{map_size(new_state)} entradas)")
    {:reply, :ok, new_state}
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

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def leader? do
    GenServer.call(__MODULE__, :leader?)
  end

  @impl true
  def init(_opts) do
    {:ok, zk} = Libremarket.ZK.connect()

    # aseguramos la jerarquía usando la versión “simple”
    wait_for_zk(zk, @base_path)
    wait_for_zk(zk, @leader_path)

    # creamos el znode efímero secuencial
    {:ok, my_znode} =
      :erlzk.create(
        zk,
        @leader_path <> "/nodo-",
        :ephemeral_sequential
      )

    leader? = compute_leader?(zk, my_znode)
    IO.puts("🟣 Envios: soy líder? #{leader?} (#{my_znode})")

    {:ok, %{zk: zk, my_znode: my_znode, leader?: leader?}}
  end

  defp wait_for_zk(zk, path, retries \\ 5)
  defp wait_for_zk(_zk, path, 0), do: raise("ZooKeeper no respondió creando #{path}")

  defp wait_for_zk(zk, path, retries) do
    case Libremarket.ZK.ensure_path(zk, path) do
      :ok ->
        :ok

      {:error, _} ->
        IO.puts("⚠️ reintentando crear #{path}…")
        :timer.sleep(1_000)
        wait_for_zk(zk, path, retries - 1)
    end
  end

  @impl true
  def handle_call(:leader?, _from, state) do
    {:reply, state.leader?, state}
  end

  defp compute_leader?(zk, my_znode) do
    {:ok, children} = :erlzk.get_children(zk, @leader_path)
    sorted = children |> Enum.map(&List.to_string/1) |> Enum.sort()
    my_name = Path.basename(List.to_string(my_znode))
    [first | _] = sorted
    my_name == first
  end
end
