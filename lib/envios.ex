defmodule Libremarket.Envio do

  def calcular_costo_envio() do
    :rand.uniform(100)
  end

  def enviar_producto() do
    IO.puts("Producto enviado correctamente")
  end

  def agendar_envio() do
    IO.puts("Producto agendado para envío correctamente")
  end


end

defmodule Libremarket.Envio.Server do
  @moduledoc """
  Envio
  """

  use GenServer

  @global_name {:global, __MODULE__}

  # API del cliente

  @doc """
  Crea un nuevo servidor de Envio
  """
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: @global_name)
  end

  def calcular_costo_envio(pid \\ @global_name) do
    GenServer.call(pid, :calcular_costo_envio)
  end

  def enviar_producto(pid \\ @global_name) do
    GenServer.call(pid, :enviar_producto)
  end

  def agendar_envio(pid \\ @global_name) do
    GenServer.call(pid, :agendar_envio)
  end

  # Callbacks

  @doc """
  Inicializa el estado del servidor
  """
  @impl true
  def init(state) do
    {:ok, state}
  end

  @impl true
  def handle_call(:calcular_costo_envio, _from, state) do
    result = Libremarket.Envio.calcular_costo_envio
    {:reply, result, state}
  end

  @impl true
  def handle_call(:enviar_producto, _from, state) do
    result = Libremarket.Envio.enviar_producto
    {:reply, result, state}
  end

  @impl true
  def handle_call(:agendar_envio, _from, state) do
    result = Libremarket.Envio.agendar_envio
    {:reply, result, state}
  end

end
