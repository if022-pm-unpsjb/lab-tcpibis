
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

#recibe el prodcuto pero tiene que recibir el id
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
      nil -> {:error, :not_found}
      p -> {:ok, p}
    end
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

  def liberar_reserva(pid \\ __MODULE__, id_producto) do
    GenServer.call(pid, {:liberar_reserva, id_producto})
  end

  def reservar_producto(pid \\ __MODULE__, id_producto) do
    GenServer.call(pid, {:reservar_producto, id_producto})
  end

  def seleccionar_producto(pid \\ __MODULE__, id_producto) do
    GenServer.call(pid, {:seleccionar_producto, id_producto})
  end

  # Callbacks

  @doc """
  Inicializa el estado del servidor
  """
  @impl true
  def init(state) do
    productos = Libremarket.Ventas.productos_disponibles()
    {:ok, productos}
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
    result = Libremarket.Ventas.seleccionar_producto(id_producto, state)
    {:reply, result, state}
  end

end
