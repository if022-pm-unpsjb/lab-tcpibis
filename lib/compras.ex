defmodule Libremarket.Compras do

  def comprar(id_producto, forma_envio, forma_pago) do
  #selecciona producto
  producto = Libremarket.Ventas.Server.seleccionar_producto(id_producto)
  if forma_envio == :correo do
    costo_envio = Libremarket.Envio.Server.calcular_costo_envio()
    IO.inspect(costo_envio, label: "Costo de envío")
  end
  #confirmar compra
  #reservar producto
  stock = Libremarket.Ventas.Server.reservar_producto(id_producto)
  if (stock === {:error, :sin_stock}) do
    IO.puts("No se pudo reservar el producto #{producto.nombre} por falta de stock")
    {:error, :sin_stock}
  else
    infraccion = Libremarket.Infracciones.Server.detectar_infraccion(1)
    if infraccion do
      IO.inspect(infraccion, label: "Se detectó una infracción")
      #IO.puts("Infracción detectada: #{inspect(infraccion)}")
      Libremarket.Ventas.Server.liberar_reserva(id_producto)
      :error
    else
      pago_ok = Libremarket.Pagos.Server.autorizar_pago()

      if not pago_ok do
        IO.inspect(pago_ok, label: "Error pago rechazado")
        #IO.puts("Infracción detectada: #{inspect(infraccion)}")
        Libremarket.Ventas.Server.liberar_reserva(id_producto)
        :error
      else

        #si pago ok enviar producto
      if forma_envio == :correo do
        Libremarket.Envio.Server.agendar_envio()
        Libremarket.Envio.Server.enviar_producto()
      end

      case GenServer.call(Libremarket.Ventas.Server, {:seleccionar_producto, id_producto}) do
        {:ok, producto} ->
          IO.puts("Compra exitosa del producto #{producto.nombre} con pago #{forma_pago} y envio #{forma_envio}")
          {:ok, producto}

        {:error, reason} ->
          {:error, reason}
      end

      {:ok, %{producto: producto, pago: forma_pago, envio: forma_envio}}

    end
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
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
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

  @doc """
  Callback para un call :comprar
  """
  @impl true
  def handle_call({:comprar, id_producto, forma_envio, forma_pago}, _from, state) do
    result = Libremarket.Compras.comprar(id_producto, forma_envio, forma_pago)
    {:reply, result, state}
  end

end
