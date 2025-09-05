defmodule Libremarket.Pagos do

  def pago_autorizado() do
    probabilidad=:rand.uniform(100)
    probabilidad<70
  end

end

defmodule Libremarket.Pagos.Server do
  @moduledoc """
  Pagos
  """

  use GenServer

  # API del cliente

  @doc """
  Crea un nuevo servidor de Pagos
  """
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def pago_autorizado(pid \\ __MODULE__, id_compra) do
    GenServer.call(pid, {:pago_autorizado, id_compra})
  end

  def listar_Pagos() do
    GenServer.call(__MODULE__, :listar_Pagos)
  end

  # Callbacks

  @doc """
  Inicializa el estado del servidor
  """
  @impl true
  def init(state) do
    {:ok, %{}}
  end

  @doc """
  Callback para un call :pago_autorizado
  """
  @impl true
  def handle_call({:pago_autorizado, id_compra}, _from, state) do
    infraccion = Libremarket.Pagos.pago_autorizado(id_compra)
    new_state = Map.put(state, id_compra, infraccion)
    {:reply, infraccion, new_state}
  end

  @impl true
  def handle_call(:listar_Pagos, _from, state) do
    {:reply, state, state}
  end

end
