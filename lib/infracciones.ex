defmodule Libremarket.Infracciones do

  def detectar_infraccion(_id_compra) do
    probabilidad=:rand.uniform(100)
    probabilidad>70
  end

end

defmodule Libremarket.Infracciones.Server do
  @moduledoc """
  Infracciones
  """

  use GenServer
<<<<<<< HEAD
  @global_name {:global, __MODULE__}
=======

  @global_name {:global, __MODULE__}

>>>>>>> 78f359e664279a9183aab18ccb3ccc416fbd66e5
  # API del cliente

  @doc """
  Crea un nuevo servidor de Infracciones
  """
  def start_link(opts \\ %{}) do
<<<<<<< HEAD
    GenServer.start_link(__MODULE__, opts, @global_name {:global, __MODULE__})
  end

  def detectar_infraccion(@global_name {:global, __MODULE__}, id_compra) do
=======
    GenServer.start_link(__MODULE__, opts, name: @global_name)
  end

  def detectar_infraccion(pid \\ @global_name, id_compra) do
>>>>>>> 78f359e664279a9183aab18ccb3ccc416fbd66e5
    GenServer.call(pid, {:detectar_infraccion, id_compra})
  end

  def listar_infracciones() do
<<<<<<< HEAD
    GenServer.call(@global_name {:global, __MODULE__}, :listar_infracciones)
=======
    GenServer.call(@global_name, :listar_infracciones)
>>>>>>> 78f359e664279a9183aab18ccb3ccc416fbd66e5
  end

  # Callbacks

  @doc """
  Inicializa el estado del servidor
  """
  @impl true
  def init(_state) do
    {:ok, %{}}
  end

  @doc """
  Callback para un call :detectar_infraccion
  """
  @impl true
  def handle_call({:detectar_infraccion, id_compra}, _from, state) do
    infraccion = Libremarket.Infracciones.detectar_infraccion(id_compra)
    if not infraccion do
        Libremarket.Pagos.autorizar_pago()
    end
    new_state = Map.put(state, id_compra, infraccion)
    {:reply, infraccion, new_state}
  end

  @impl true
  def handle_call(:listar_infracciones, _from, state) do
    {:reply, state, state}
  end

end
