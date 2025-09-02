defmodule Libremarket.Ventas do

  def productos_disponibles() do
    [
        %{nombre: "Zapatillas Adadis", cantidad: :rand.uniform(10)},
        %{nombre: "Piluso bokita", cantidad: :rand.uniform(10)},
        %{nombre: "Don satur saladas", cantidad: :rand.uniform(10)},
        %{nombre: "Mate de calabaza", cantidad: :rand.uniform(10)},
        %{nombre: "Pañuelo seca lagrimas", cantidad: :rand.uniform(10)},
        %{nombre: "Gorra pumita", cantidad: :rand.uniform(10)},
        %{nombre: "Piluso velez", cantidad: :rand.uniform(10)},
        %{nombre: "Piluso newells", cantidad: :rand.uniform(10)},
        %{nombre: "Piluso river", cantidad: :rand.uniform(10)},
        %{nombre: "Taza verde", cantidad: :rand.uniform(10)}
    ]
  end

def liberar_reserva(producto) do
  %{producto | cantidad: producto.cantidad + 1}
end

def reservar_producto(producto) do
  if producto.cantidad == 0 do
    IO.puts("ERROR NO HAY STOCK")
    {:error, :sin_stock}
  else
    %{producto | cantidad: producto.cantidad - 1}
  end
end

def seleccionar_producto(stock_productos) do
  Enum.random(stock_productos)
end


end

defmodule Libremarket.Ventas.Server do
  @moduledoc """
  Ventas
  """

  use GenServer

  # API del cliente

  @doc """
  Crea un nuevo servidor de Ventas
  """
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def productos_disponibles(pid \\ __MODULE__) do
    GenServer.call(pid, :productos_disponibles)
  end

  def liberar_reserva(pid \\ __MODULE__, producto) do
    GenServer.call(pid, {:liberar_reserva, producto})
  end

  def reservar_producto(pid \\ __MODULE__, producto) do
    GenServer.call(pid, {:reservar_producto, producto})
  end

  def seleccionar_producto(pid \\ __MODULE__, productos) do
    GenServer.call(pid, {:seleccionar_producto, productos})
  end

  # Callbacks

  @doc """
  Inicializa el estado del servidor
  """
  @impl true
  def init(state) do
    {:ok, state}
  end

  @doc """
  Callbacks
  """
  @impl true
  def handle_call(:productos_disponibles, _from, state) do
    result = Libremarket.Ventas.productos_disponibles
    {:reply, result, state}
  end

  @impl true
  def handle_call({:liberar_reserva, producto}, _from, state) do
    result = Libremarket.Ventas.liberar_reserva(producto)
    {:reply, result, state}
  end


  @impl true
  def handle_call({:reservar_producto, producto}, _from, state) do
    result = Libremarket.Ventas.reservar_producto(producto)
    {:reply, result, state}
  end


  @impl true
  def handle_call({:seleccionar_producto, productos}, _from, state) do
    result = Libremarket.Ventas.seleccionar_producto(productos)
    {:reply, result, state}
  end

end
