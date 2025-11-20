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
    if Process.whereis(Libremarket.Ventas.Leader) == nil do
      IO.puts("⏳ Esperando a que arranque Libremarket.Ventas.Leader...")
      :timer.sleep(500)
      wait_for_leader()
    else
      :ok
    end
  end

  defp safe_leader_check() do
    try do
      {:ok, Libremarket.Ventas.Leader.leader?()}
    catch
      :exit, _ -> {:error, :not_alive}
    end
  end

  # PIDs de réplicas (excluye el propio PID para evitar deadlock)
  def replicas() do
    my_pid = GenServer.whereis(local_name())

    Libremarket.Replicacion.Registry.replicas(__MODULE__)
    |> Enum.reject(&(&1 == my_pid))
  end

  # Nombre local (global o con sufijo de contenedor)
  defp local_name() do
    container = System.get_env("CONTAINER_NAME") || "default"
    is_primary = Libremarket.Ventas.Leader.leader?()
    {:global, base_name} = @global_name
    if is_primary, do: @global_name, else: {:global, :"#{base_name}_#{container}"}
  end

  def productos_disponibles(pid \\ @global_name) do
    GenServer.call(pid, :productos_disponibles)
  end

  def liberar_reserva(pid \\ @global_name, id_producto) do
    GenServer.call(pid, {:liberar_reserva, id_producto})
  end

  def reservar_producto(pid \\ @global_name, id_producto) do
    GenServer.call(pid, {:reservar_producto, id_producto})
  end

  def seleccionar_producto(pid \\ @global_name, id_producto) do
    GenServer.call(pid, {:seleccionar_producto, id_producto})
  end

  def obtener_productos() do
    GenServer.call(@global_name, :obtener_productos)
  end

  @impl true
  def init(_state) do
    is_leader = Libremarket.Ventas.Leader.leader?()

    # Solo el líder levanta el AMQP
    if is_leader do
      Supervisor.start_child(
        Libremarket.Supervisor,
        {Libremarket.Ventas.AMQP, %{}}
      )
    end

    productos = Libremarket.Ventas.productos_disponibles()
    {:ok, productos}
  end

  @impl true
  def handle_call(:productos_disponibles, _from, state) do
    result = Libremarket.Ventas.productos_disponibles()
    {:reply, result, state}
  end

  @impl true
  def handle_call({:liberar_reserva, id_producto}, _from, state) do
    primario = Libremarket.Ventas.Leader.leader?()

    if primario do
      case Libremarket.Ventas.liberar_reserva(id_producto, state) do
        {:ok, new_state} ->
          Replicacion.replicar_estado(new_state, replicas(), __MODULE__)
          {:reply, {:ok, new_state}, new_state}

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    else
      {:reply, {:error, :solo_primario}, state}
    end
  end

  def handle_call({:reservar_producto, id}, _from, state) do
    primario = Libremarket.Ventas.Leader.leader?()

    if primario do
      case Libremarket.Ventas.reservar_producto(id, state) do
        {:ok, new_state} ->
          Replicacion.replicar_estado(new_state, replicas(), __MODULE__)
          {:reply, {:ok, new_state}, new_state}

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    else
      {:reply, {:error, :solo_primario}, state}
    end
  end

  @impl true
  def handle_call({:seleccionar_producto, id_producto}, _from, state) do
    case Libremarket.Ventas.seleccionar_producto(id_producto, state) do
      {:ok, producto} ->
        {:reply, {:ok, producto}, state}

      {:error, :not_found} ->
        {:reply, {:error, :not_found}, state}

      {:error, :sin_stock, producto} ->
        {:reply, {:error, :sin_stock, producto}, state}
    end
  end

  def handle_call(:obtener_productos, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call({:sync_state, new_state}, _from, _old_state) do
    Logger.info("📡 Estado sincronizado por llamada directa (#{map_size(new_state)} entradas)")
    {:reply, :ok, new_state}
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
    IO.puts("🟣 Ventas: soy líder? #{leader?} (#{my_znode})")

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
