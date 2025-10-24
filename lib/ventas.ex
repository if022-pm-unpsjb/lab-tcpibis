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
  @moduledoc """
  Ventas
  """

  use GenServer

  @global_name {:global, __MODULE__}

  @doc """
  Crea un nuevo servidor de Ventas
  """
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: @global_name)
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

  @doc """
  Inicializa el estado del servidor
  """
  @impl true
  def init(_state) do
    productos = Libremarket.Ventas.productos_disponibles()
    {:ok, productos}
  end

  @doc """
  Callbacks
  """
  @impl true
  def handle_call(:productos_disponibles, _from, state) do
    result = Libremarket.Ventas.productos_disponibles()
    {:reply, result, state}
  end

  @impl true
  def handle_call({:liberar_reserva, id_producto}, _from, state) do
    case Libremarket.Ventas.liberar_reserva(id_producto, state) do
      {:ok, new_state} ->
        {:reply, {:ok, new_state}, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:reservar_producto, id}, _from, state) do
    case Libremarket.Ventas.reservar_producto(id, state) do
      {:ok, new_state} ->
        {:reply, {:ok, new_state}, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
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
