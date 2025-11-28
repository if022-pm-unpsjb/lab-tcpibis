defmodule Libremarket.Ventas do
  def productos_disponibles() do
    %{
      1 => %{nombre: "Zapatillas Adadis", cantidad: :rand.uniform(10)},
      2 => %{nombre: "Piluso bokita", cantidad: :rand.uniform(10)},
      3 => %{nombre: "Don satur saladas", cantidad: :rand.uniform(10)},
      4 => %{nombre: "Mate de calabaza", cantidad: :rand.uniform(10)},
      5 => %{nombre: "Pañuelo seca lagrimas", cantidad: :rand.uniform(10)},
      6 => %{nombre: "Gorra pumita", cantidad: :rand.uniform(10)},
      7 => %{nombre: "Piluso velez", cantidad: :rand.uniform(10)},
      8 => %{nombre: "Piluso newells", cantidad: :rand.uniform(10)},
      9 => %{nombre: "Piluso river", cantidad: :rand.uniform(10)},
      10 => %{nombre: "Taza verde", cantidad: :rand.uniform(10)}
    }
  end

  def liberar_reserva(id, state) do
    case state[id] do
      nil -> {:error, :not_found}
      %{cantidad: c} = p -> {:ok, Map.put(state, id, %{p | cantidad: c + 1})}
    end
  end

  def reservar_producto(id, state) do
    case state[id] do
      nil -> {:error, :not_found}
      %{cantidad: c} = p when c > 0 -> {:ok, Map.put(state, id, %{p | cantidad: c - 1})}
      _ -> {:error, :sin_stock}
    end
  end

  def seleccionar_producto(id, state) do
    case state[id] do
      nil ->
        {:error, :not_found}

      %{cantidad: 0} = p ->
        {:error, :sin_stock, p}

      p ->
        {:ok, p}
    end
  end
end

defmodule Libremarket.Ventas.Server do
  use GenServer
  require Logger
  alias Libremarket.Replicacion

  @global_name {:global, __MODULE__}

  # -------------------------
  #  START
  # -------------------------
  def start_link(opts \\ %{}) do
    container_name = System.get_env("CONTAINER_NAME") || "default"

    # esperar a que arranque el Leader
    wait_for_leader()

    {:global, base_name} = @global_name

    # siempre un nombre global único por contenedor
    name = {:global, :"#{base_name}_#{container_name}"}

    IO.puts("📡 [VENTAS] nombre #{inspect(name)}")

    {:ok, pid} = GenServer.start_link(__MODULE__, opts, name: name)

    # registrar nombre local simple (igual que Infracciones)
    Process.register(pid, __MODULE__)

    Libremarket.Replicacion.Registry.registrar(__MODULE__, container_name, pid)
    {:ok, pid}
  end

  defp wait_for_leader() do
    if Process.whereis(Libremarket.Ventas.Leader) == nil do
      IO.puts("⏳ Esperando a que arranque Libremarket.Ventas.Leader...")
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
  #  API
  # -------------------------
  def productos_disponibles(pid \\ @global_name),
    do: GenServer.call(pid, :productos_disponibles)

  def liberar_reserva(pid \\ @global_name, id),
    do: GenServer.call(pid, {:liberar_reserva, id})

  def reservar_producto(pid \\ @global_name, id),
    do: GenServer.call(pid, {:reservar_producto, id})

  def seleccionar_producto(pid \\ @global_name, id),
    do: GenServer.call(pid, {:seleccionar_producto, id})

  def obtener_productos(),
    do: GenServer.call(@global_name, :obtener_productos)

  # -------------------------
  #  INIT
  # -------------------------
  @impl true
  def init(_opts) do
    productos = Libremarket.Ventas.productos_disponibles()
    {:ok, productos, {:continue, :start_amqp_if_leader}}
  end

  @impl true
  def handle_continue(:start_amqp_if_leader, state) do
    if Libremarket.Ventas.Leader.leader?() do
      register_as_leader()

      Supervisor.start_child(
        Libremarket.Supervisor,
        {Libremarket.Ventas.AMQP, %{}}
      )
    end

    {:noreply, state}
  end

  # -------------------------
  #  LOGIC
  # -------------------------
  @impl true
  def handle_call(:productos_disponibles, _from, state) do
    {:reply, Libremarket.Ventas.productos_disponibles(), state}
  end

  @impl true
  def handle_call({:liberar_reserva, id}, _from, state) do
    if Libremarket.Ventas.Leader.leader?() do
      case Libremarket.Ventas.liberar_reserva(id, state) do
        {:ok, new} ->
          Replicacion.replicar_estado(new, replicas(), __MODULE__)
          {:reply, {:ok, new}, new}

        err ->
          {:reply, err, state}
      end
    else
      {:reply, {:error, :solo_primario}, state}
    end
  end

  @impl true
  def handle_call({:reservar_producto, id}, _from, state) do
    if Libremarket.Ventas.Leader.leader?() do
      case Libremarket.Ventas.reservar_producto(id, state) do
        {:ok, new} ->
          Replicacion.replicar_estado(new, replicas(), __MODULE__)
          {:reply, {:ok, new}, new}

        err ->
          {:reply, err, state}
      end
    else
      {:reply, {:error, :solo_primario}, state}
    end
  end

  @impl true
  def handle_call({:seleccionar_producto, id}, _from, state) do
    case Libremarket.Ventas.seleccionar_producto(id, state) do
      {:ok, prod} -> {:reply, {:ok, prod}, state}
      {:error, reason, prod} -> {:reply, {:error, reason, prod}, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:obtener_productos, _from, state),
    do: {:reply, state, state}

  @impl true
  def handle_call({:sync_state, new}, _from, _old) do
    Logger.info("📡 [VENTAS] Estado sincronizado (#{map_size(new)} items)")
    {:reply, :ok, new}
  end

  # -------------------------
  #  LIDERAZGO DINÁMICO
  # -------------------------
  def lider_cambio(es_lider) do
    GenServer.cast(__MODULE__, {:lider_cambio, es_lider})
  end

  @impl true
  def handle_cast({:lider_cambio, true}, state) do
    IO.puts("🟢 [VENTAS] Ahora soy líder → levantando AMQP")
    register_as_leader()

    Supervisor.start_child(
      Libremarket.Supervisor,
      {Libremarket.Ventas.AMQP, %{}}
    )

    {:noreply, state}
  end

  @impl true
  def handle_cast({:lider_cambio, false}, state) do
    IO.puts("🔴 [VENTAS] Perdí liderazgo → sigo como réplica")
    unregister_as_leader()
    {:noreply, state}
  end

  # -------------------------
  #  GLOBAL NAMES
  # -------------------------
  defp register_as_leader() do
    # si ya había otro líder → borrar
    case :global.whereis_name(__MODULE__) do
      pid when is_pid(pid) and pid != self() ->
        :global.unregister_name(__MODULE__)

      _ ->
        :ok
    end

    # limpiar nombres viejos tipo Server_ventas-2
    Enum.each(:global.registered_names(), fn name ->
      if name != __MODULE__ and
           String.starts_with?(to_string(name), "Elixir.Libremarket.Ventas.Server_") and
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

defmodule Libremarket.Ventas.AMQP do
  @moduledoc "Worker AMQP de Ventas: consume ventas.req y publica compras.ventas.resp (tipo: \"venta\")"
  use GenServer
  require Logger
  alias AMQP.{Connection, Channel, Queue, Basic}

  @ventas_req_q "ventas.req"
  @ventas_resp_q "compras.ventas.resp"
  @exchange ""

  def start_link(_opts \\ []), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  @impl true
  def init(_state) do
    amqp_url = System.get_env("AMQP_URL")

    {:ok, conn} = Connection.open(amqp_url, ssl_options: [verify: :verify_none])
    {:ok, chan} = Channel.open(conn)

    {:ok, _} = Queue.declare(chan, @ventas_req_q, durable: false, auto_delete: false)
    :ok = Basic.qos(chan, prefetch_count: 0)
    {:ok, _} = Basic.consume(chan, @ventas_req_q, nil, no_ack: false)

    Logger.info("Ventas.AMQP conectado y escuchando #{@ventas_req_q}")
    {:ok, %{conn: conn, chan: chan}}
  end

  @impl true
  def handle_info({:basic_deliver, payload, meta}, %{chan: chan} = st) do
    %{"id_compra" => id_compra, "id_producto" => id_producto, "accion" => accion} =
      Jason.decode!(payload)

    resp =
      case accion do
        "seleccionar" ->
          case Libremarket.Ventas.Server.seleccionar_producto(id_producto) do
            {:ok, producto} ->
              %{
                tipo: "venta",
                op: "seleccionar",
                ok: true,
                id_compra: id_compra,
                producto: producto
              }

            {:error, :sin_stock, producto} ->
              %{
                tipo: "venta",
                op: "seleccionar",
                ok: false,
                id_compra: id_compra,
                motivo: "sin_stock",
                producto: producto
              }

            {:error, :not_found} ->
              %{
                tipo: "venta",
                op: "seleccionar",
                ok: false,
                id_compra: id_compra,
                motivo: "not_found"
              }

            {:error, reason} ->
              %{
                tipo: "venta",
                op: "seleccionar",
                ok: false,
                id_compra: id_compra,
                motivo: to_string(reason)
              }
          end

        "reservar" ->
          case Libremarket.Ventas.Server.reservar_producto(id_producto) do
            {:ok, _nuevo_estado} ->
              %{tipo: "venta", op: "reservar", ok: true, id_compra: id_compra}

            {:error, :sin_stock} ->
              %{
                tipo: "venta",
                op: "reservar",
                ok: false,
                id_compra: id_compra,
                motivo: "sin_stock"
              }

            {:error, :not_found} ->
              %{
                tipo: "venta",
                op: "reservar",
                ok: false,
                id_compra: id_compra,
                motivo: "not_found"
              }

            {:error, reason} ->
              %{
                tipo: "venta",
                op: "reservar",
                ok: false,
                id_compra: id_compra,
                motivo: to_string(reason)
              }
          end

        "liberar" ->
          _ = Libremarket.Ventas.Server.liberar_reserva(id_producto)
          %{tipo: "venta", op: "liberar", ok: true, id_compra: id_compra}

        other ->
          Logger.warning("Ventas.AMQP: acción desconocida #{inspect(other)}")

          %{
            tipo: "venta",
            op: "desconocido",
            ok: false,
            id_compra: id_compra,
            motivo: "accion_desconocida"
          }
      end

    :ok =
      Basic.publish(chan, @exchange, @ventas_resp_q, Jason.encode!(resp),
        content_type: "application/json"
      )

    :ok = Basic.ack(chan, meta.delivery_tag)
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

defmodule Libremarket.Ventas.Leader do
  use GenServer

  @base_path "/libremarket/ventas"
  @leader_path "/libremarket/ventas/leader"
  @interval 2_000

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def leader?(), do: GenServer.call(__MODULE__, :leader?)

  @impl true
  def init(_opts) do
    # ETS global
    case :ets.whereis(:lider_ventas) do
      :undefined -> :ets.new(:lider_ventas, [:named_table, :public])
      _ -> :ok
    end

    {:ok, zk} = Libremarket.ZK.connect()

    wait_for_zk(zk, @base_path)
    wait_for_zk(zk, @leader_path)

    {:ok, my_znode} =
      :erlzk.create(zk, @leader_path <> "/nodo-", :ephemeral_sequential)

    leader? = compute_leader?(zk, my_znode)

    :ets.insert(:lider_ventas, {:is_leader, leader?})

    IO.puts("🟣 [VENTAS] Soy líder? #{leader?} (#{my_znode})")

    Process.send_after(self(), :check_leader, @interval)

    {:ok, %{zk: zk, my_znode: my_znode, leader?: leader?}}
  end

  @impl true
  def handle_call(:leader?, _from, st),
    do: {:reply, st.leader?, st}

  @impl true
  def handle_info(:check_leader, %{zk: zk, my_znode: my_znode} = state) do
    new_state =
      case :erlzk.get_children(zk, @leader_path) do
        {:ok, children} ->
          sorted = children |> Enum.map(&List.to_string/1) |> Enum.sort()
          my_name = List.to_string(my_znode) |> Path.basename()
          new_leader = hd(sorted) == my_name

          if new_leader != state.leader? do
            IO.puts("🔄 [VENTAS] Cambio de liderazgo: #{state.leader?} -> #{new_leader}")
            :ets.insert(:lider_ventas, {:is_leader, new_leader})
            Libremarket.Ventas.Server.lider_cambio(new_leader)
          end

          %{state | leader?: new_leader}

        {:error, _} ->
          IO.puts("⚠️ [VENTAS] No hay znode → recreando nodo")

          wait_for_zk(zk, @leader_path)

          {:ok, new_znode} =
            :erlzk.create(zk, @leader_path <> "/nodo-", :ephemeral_sequential)

          new_leader = compute_leader?(zk, new_znode)

          if new_leader != state.leader? do
            IO.puts("🔄 [VENTAS] Liderazgo recreado → líder=#{new_leader}")
            Libremarket.Ventas.Server.lider_cambio(new_leader)
          end

          %{state | my_znode: new_znode, leader?: new_leader}
      end

    Process.send_after(self(), :check_leader, @interval)
    {:noreply, new_state}
  end

  defp compute_leader?(zk, my_znode) do
    {:ok, children} = :erlzk.get_children(zk, @leader_path)

    sorted =
      children |> Enum.map(&List.to_string/1) |> Enum.sort()

    my_name =
      my_znode
      |> List.to_string()
      |> Path.basename()

    hd(sorted) == my_name
  end

  # ensure_path robusto
  defp wait_for_zk(zk, path, retries \\ 5)

  defp wait_for_zk(_zk, path, 0),
    do: raise("ZooKeeper no respondió creando #{path}")

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
