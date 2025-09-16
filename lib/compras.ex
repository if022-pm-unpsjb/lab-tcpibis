defmodule Libremarket.Compras do

  def comprar(id_producto, forma_envio, forma_pago) do
      # selecciona producto
      producto = Libremarket.Ventas.Server.seleccionar_producto(id_producto)

      # para sacar el {:ok, ...}
      producto_sin_ok =
        case producto do
          {:ok, p} -> p
          p -> p
        end

      if forma_envio == :correo do
        costo_envio = Libremarket.Envio.Server.calcular_costo_envio()
        IO.inspect(costo_envio, label: "Costo de envío")
      end

      # reservar producto
      stock = Libremarket.Ventas.Server.reservar_producto(id_producto)

      cond do
        stock == {:error, :sin_stock} ->
          IO.puts("No se pudo reservar el producto #{producto.nombre} por falta de stock")

          {:error,
           %{
             producto: producto_sin_ok,
             pago: forma_pago,
             envio: forma_envio,
             motivo: :sin_stock
           }}

        Libremarket.Infracciones.Server.detectar_infraccion(1) ->
          IO.puts("Se detectó una infracción")
          Libremarket.Ventas.Server.liberar_reserva(id_producto)

          {:error,
           %{
             producto: producto_sin_ok,
             pago: forma_pago,
             envio: forma_envio,
             motivo: :infraccion_detectada
           }}

        not Libremarket.Pagos.Server.autorizar_pago() ->
          IO.puts("Error pago rechazado")
          Libremarket.Ventas.Server.liberar_reserva(id_producto)

          {:error,
           %{
             producto: producto_sin_ok,
             pago: forma_pago,
             envio: forma_envio,
             motivo: :pago_rechazado
           }}

        true ->
          # si pago ok enviar producto
          if forma_envio == :correo do
            Libremarket.Envio.Server.agendar_envio()
            Libremarket.Envio.Server.enviar_producto()
          end

          case GenServer.call(Libremarket.Ventas.Server, {:seleccionar_producto, id_producto}) do
            {:ok, producto_ok} ->
              producto_sin_ok =
                case producto_ok do
                  {:ok, p} -> p
                  p -> p
                end

              {:ok,
                %{
                  producto: producto_sin_ok,
                  pago: forma_pago,
                  envio: forma_envio,
                  motivo: :ok
                }}

            {:error, reason} ->
              {:error,
                %{
                  producto: producto_sin_ok,
                  pago: forma_pago,
                  envio: forma_envio,
                  motivo: reason
                }}
          end

      end
    end
  end

defmodule Libremarket.Compras.Server do
  @moduledoc """
  Compras
  """

  use GenServer

  # API del cliente

  @doc """
  Crea un nuevo servidor de Compras
  """
  def start_link(_opts \\ %{}) do
    GenServer.start_link(__MODULE__, %{next_id: 0}, name: __MODULE__)
  end

  def comprar(pid \\ __MODULE__, id_producto, forma_envio, forma_pago) do
    GenServer.call(pid, {:comprar, id_producto, forma_envio, forma_pago})
  end
  @doc """
  Inicializa el estado del servidor
  """
  @impl true
  def init(state) do
    {:ok, state}
  end

  @impl true
  def handle_call({:comprar, id_producto, forma_envio, forma_pago}, _from, %{next_id: id} = state) do
    compra_id = id + 1

    result = Libremarket.Compras.comprar(id_producto, forma_envio, forma_pago)

    reply =
      case result do
        {:ok, data} -> {:ok, Map.put(data, :id, compra_id)}
        {:error, data} -> {:error, Map.put(data, :id, compra_id)}
      end

    {:reply, reply, %{state | next_id: compra_id}}
  end


end
