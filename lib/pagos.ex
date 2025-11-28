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

  # -------------------------
  # START
  # -------------------------
  def start_link(opts \\ %{}) do
    container_name = System.get_env("CONTAINER_NAME") || "default"

    # esperar a que arranque el Leader
    wait_for_leader()

    {:global, base_name} = @global_name

    # siempre global por contenedor
    name = {:global, :"#{base_name}_#{container_name}"}

    IO.puts("📡 [PAGOS] nombre #{inspect(name)}")

    {:ok, pid} = GenServer.start_link(__MODULE__, opts, name: name)

    # nombre local para hablar desde Leader
    Process.register(pid, __MODULE__)

    Libremarket.Replicacion.Registry.registrar(__MODULE__, container_name, pid)
    {:ok, pid}
  end

  defp wait_for_leader() do
    if Process.whereis(Libremarket.Pagos.Leader) == nil do
      IO.puts("⏳ Esperando a Leader Pagos...")
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

  # -------------------------
  # API
  # -------------------------
  def autorizar_pago(pid \\ @global_name, id),
    do: GenServer.call(pid, {:autorizar_pago, id})

  def obtener_pagos(),
    do: GenServer.call(@global_name, :obtener_pagos)

  # -------------------------
  # INIT
  # -------------------------
  @impl true
  def init(_opts) do
    {:ok, %{}, {:continue, :start_amqp_if_leader}}
  end

  @impl true
  def handle_continue(:start_amqp_if_leader, state) do
    if Libremarket.Pagos.Leader.leader?() do
      register_as_leader()

      Supervisor.start_child(
        Libremarket.Supervisor,
        {Libremarket.Pagos.AMQP, %{}}
      )
    end

    {:noreply, state}
  end

  # -------------------------
  # LÓGICA
  # -------------------------
  @impl true
  def handle_call({:autorizar_pago, id_compra}, _from, state) do
    if Libremarket.Pagos.Leader.leader?() do
      autorizado = Libremarket.Pagos.autorizar_pago(id_compra)
      new_state = Map.put(state, id_compra, autorizado)

      Replicacion.replicar_estado(new_state, replicas(), __MODULE__)
      {:reply, autorizado, new_state}
    else
      Logger.warning("Nodo réplica recibió autorizar_pago → NO corresponde")
      {:reply, :replica, state}
    end
  end

  def handle_call(:obtener_pagos, _from, state),
    do: {:reply, state, state}

  @impl true
  def handle_call({:sync_state, new}, _from, _old) do
    Logger.info("📡 [PAGOS] Estado sincronizado (#{map_size(new)} items)")
    {:reply, :ok, new}
  end

  # -------------------------
  # CAMBIO DE LÍDER
  # -------------------------
  def lider_cambio(es_lider) do
    GenServer.cast(__MODULE__, {:lider_cambio, es_lider})
  end

  @impl true
  def handle_cast({:lider_cambio, true}, state) do
    IO.puts("🟢 [PAGOS] Ahora soy líder → levantando AMQP")
    register_as_leader()

    Supervisor.start_child(
      Libremarket.Supervisor,
      {Libremarket.Pagos.AMQP, %{}}
    )

    {:noreply, state}
  end

  @impl true
  def handle_cast({:lider_cambio, false}, state) do
    IO.puts("🔴 [PAGOS] Perdí liderazgo → sigo como réplica")
    unregister_as_leader()
    {:noreply, state}
  end

  # -------------------------
  # REGISTRO GLOBAL
  # -------------------------
  defp register_as_leader() do
    # borrar alias viejo si existe
    case :global.whereis_name(__MODULE__) do
      pid when is_pid(pid) and pid != self() ->
        :global.unregister_name(__MODULE__)

      _ ->
        :ok
    end

    # eliminar nombres antiguos tipo Server_pagos-2
    Enum.each(:global.registered_names(), fn name ->
      if name != __MODULE__ and
           String.starts_with?(to_string(name), "Elixir.Libremarket.Pagos.Server_") and
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
  @interval 2_000

  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  def leader?(),
    do: GenServer.call(__MODULE__, :leader?)

  @impl true
  def init(_opts) do
    # ETS global del nodo
    case :ets.whereis(:lider_pagos) do
      :undefined -> :ets.new(:lider_pagos, [:named_table, :public])
      _ -> :ok
    end

    {:ok, zk} = Libremarket.ZK.connect()

    wait_for_zk(zk, @base_path)
    wait_for_zk(zk, @leader_path)

    {:ok, my_znode} =
      :erlzk.create(zk, @leader_path <> "/nodo-", :ephemeral_sequential)

    leader? = compute_leader?(zk, my_znode)

    :ets.insert(:lider_pagos, {:is_leader, leader?})

    IO.puts("🟣 [PAGOS] Soy líder? #{leader?} (#{my_znode})")

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
          my_name = List.to_string(my_znode) |> Path.basename()

          new_leader = hd(sorted) == my_name

          if new_leader != st.leader? do
            IO.puts("🔄 [PAGOS] Cambio de liderazgo #{st.leader?} → #{new_leader}")
            :ets.insert(:lider_pagos, {:is_leader, new_leader})
            Libremarket.Pagos.Server.lider_cambio(new_leader)
          end

          %{st | leader?: new_leader}

        {:error, _} ->
          IO.puts("⚠️ [PAGOS] Znode perdido → recreando nodo")

          wait_for_zk(zk, @leader_path)

          {:ok, new_z} =
            :erlzk.create(zk, @leader_path <> "/nodo-", :ephemeral_sequential)

          new_leader = compute_leader?(zk, new_z)

          if new_leader != st.leader? do
            IO.puts("🔄 [PAGOS] Liderazgo recreado → #{new_leader}")
            Libremarket.Pagos.Server.lider_cambio(new_leader)
          end

          %{st | my_znode: new_z, leader?: new_leader}
      end

    Process.send_after(self(), :check_leader, @interval)
    {:noreply, new_state}
  end

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
