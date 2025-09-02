defmodule Libremarket.Pagos do

  def autorizar_pago() do
    probabilidad=:rand.uniform(100)
    probabilidad<70
  end

  def elegir_metodo_pago()do
    pago = Enum.random([:efectivo, :transferencia, :td, :tc])
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

  def autorizar_pago(pid \\ __MODULE__) do
    GenServer.call(pid, :autorizar_pago)
  end

  def elegir_metodo_pago(pid \\ __MODULE__) do
    GenServer.call(pid, :elegir_metodo_pago)
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
  def handle_call(:autorizar_pago, _from, state) do
    result = Libremarket.Pagos.autorizar_pago()
    {:reply, result, state}
  end


  @impl true
  def handle_call(:elegir_metodo_pago, _from, state) do
    result = Libremarket.Pagos.elegir_metodo_pago()
    {:reply, result, state}
  end

end
