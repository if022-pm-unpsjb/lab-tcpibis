defmodule Libremarket.Pagos do
  def autorizar_pago(_id_compra) do
    probabilidad = :rand.uniform(100)
    probabilidad < 70
  end
end

defmodule Libremarket.Pagos.Server do
  use GenServer
  require Logger
  alias Libremarket.Replicacion

  @global_name {:global, __MODULE__}

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
    if Process.whereis(Libremarket.Pagos.Leader) == nil do
      IO.puts("⏳ Esperando a que arranque Libremarket.Pagos.Leader...")
      :timer.sleep(500)
      wait_for_leader()
    else
      :ok
    end
  end

  defp safe_leader_check() do
    try do
      {:ok, Libremarket.Pagos.Leader.leader?()}
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
    is_primary = Libremarket.Pagos.Leader.leader?()
    {:global, base_name} = @global_name
    if is_primary, do: @global_name, else: {:global, :"#{base_name}_#{container}"}
  end

  def autorizar_pago(pid \\ @global_name, id_compra) do
    GenServer.call(pid, {:autorizar_pago, id_compra})
  end

  def obtener_pagos() do
    GenServer.call(@global_name, :obtener_pagos)
  end

  @impl true
  def init(_opts) do
    {:ok, %{}}
  end

  def handle_call({:autorizar_pago, id_compra}, _from, state) do
    if Libremarket.Pagos.Leader.leader?() do
      autorizado = Libremarket.Pagos.autorizar_pago(id_compra)
      new_state = Map.put(state, id_compra, autorizado)
      Replicacion.replicar_estado(new_state, replicas(), __MODULE__)
      {:reply, autorizado, new_state}
    else
      Logger.warning("Nodo réplica (no lider) no debe detectar autorizar pagos directamente")
      {:reply, :replica, state}
    end
  end

  def handle_call(:obtener_pagos, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call({:sync_state, new_state}, _from, _old_state) do
    Logger.info("📡 Estado sincronizado por llamada directa (#{map_size(new_state)} entradas)")
    {:reply, :ok, new_state}
  end
end

defmodule Libremarket.Pagos.AMQP do
  @moduledoc "Proceso que consume pagos.req y publica resultados en compras.pagos.resp"
  use GenServer
  require Logger
  alias AMQP.{Connection, Channel, Queue, Basic}

  @pagos_req_q "pagos.req"
  @pagos_resp_q "compras.pagos.resp"
  @exchange ""

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @impl true
  def init(_state) do
    amqp_url = System.get_env("AMQP_URL")

    {:ok, conn} = Connection.open(amqp_url, ssl_options: [verify: :verify_none])
    {:ok, chan} = Channel.open(conn)

    {:ok, _} = Queue.declare(chan, @pagos_req_q, durable: false, auto_delete: false)
    :ok = Basic.qos(chan, prefetch_count: 0)
    {:ok, _} = Basic.consume(chan, @pagos_req_q, nil, no_ack: false)

    Logger.info("Pagos.AMQP conectado y escuchando #{@pagos_req_q}")
    {:ok, %{conn: conn, chan: chan}}
  end

  @impl true
  def handle_info({:basic_deliver, payload, meta}, %{chan: chan} = st) do
    %{"id_compra" => id_compra} = Jason.decode!(payload)

    autorizado = Libremarket.Pagos.Server.autorizar_pago(id_compra)

    resp =
      Jason.encode!(%{
        tipo: "pago",
        id_compra: id_compra,
        autorizado: autorizado
      })

    Basic.publish(chan, @exchange, @pagos_resp_q, resp, content_type: "application/json")
    Basic.ack(chan, meta.delivery_tag)

    Logger.info("Pagos.AMQP → procesó pago #{id_compra}, resultado=#{autorizado}")
    {:noreply, st}
  end

  @impl true
  def handle_info({:basic_consume_ok, _}, st), do: {:noreply, st}
  def handle_info({:basic_cancel, _}, st), do: {:stop, :normal, st}
  def handle_info({:basic_cancel_ok, _}, st), do: {:noreply, st}
  def handle_info(_, st), do: {:noreply, st}

  @impl true
  def terminate(_reason, %{chan: chan, conn: conn}) do
    Channel.close(chan)
    Connection.close(conn)
    :ok
  end
end

defmodule Libremarket.Pagos.Leader do
  use GenServer

  @base_path "/libremarket/pagos"
  @leader_path "/libremarket/pagos/leader"

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
    IO.puts("🟣 Pagos: soy líder? #{leader?} (#{my_znode})")

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
