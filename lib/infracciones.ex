defmodule Libremarket.Infracciones do

  def detectar_infraccion(id_compra) do
    probabilidad=:rand.uniform(100)
    probabilidad>70
  end

end

defmodule Libremarket.Infracciones.Server do
  @moduledoc """
  Infracciones
  """

  use GenServer

  # API del cliente

  @doc """
  Crea un nuevo servidor de Infracciones
  """
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def detectar_infraccion(pid \\ __MODULE__, id_compra) do
    GenServer.call(pid, {:detectar_infraccion, id_compra})
  end

  def listar_infracciones() do
    GenServer.call(__MODULE__, :listar_infracciones)
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
  Callback para un call :detectar_infraccion
  """
  @impl true
  def handle_call({:detectar_infraccion, id_compra}, _from, state) do
    infraccion = Libremarket.Infracciones.detectar_infraccion(id_compra)
    new_state = Map.put(state, id_compra, infraccion)
    {:reply, infraccion, new_state}
  end

  @impl true
  def handle_call(:listar_infracciones, _from, state) do
    {:reply, state, state}
  end

end
