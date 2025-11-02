defmodule Libremarket.Infracciones do
  def detectar_infraccion(_id_compra) do
    probabilidad = :rand.uniform(100)
    probabilidad > 70
  end
end

defmodule Libremarket.Infracciones.Server do
  @moduledoc """
  Infracciones
  """

  use GenServer
  require Logger
  alias Libremarket.Replicacion

  @global_name {:global, __MODULE__}

  @doc """
  Crea un nuevo servidor de Infracciones
  """
  def start_link(opts \\ %{}) do
    container_name = System.get_env("CONTAINER_NAME") || "default"
    is_primary = System.get_env("PRIMARY") == "true"
    {:global, base_name} = @global_name

    name =
      if is_primary do
        @global_name
      else
        {:global, :"#{base_name}_#{container_name}"}
      end

    IO.puts("📡nombre #{inspect(name)}")
    {:ok, pid} = GenServer.start_link(__MODULE__, opts, name: name)
    Libremarket.Replicacion.Registry.registrar(__MODULE__, container_name, pid)
    {:ok, pid}
  end

  def replicas() do
    my_pid = GenServer.whereis(local_name())

    Libremarket.Replicacion.Registry.replicas(__MODULE__)
    |> Enum.reject(&(&1 == my_pid))
  end

  defp local_name() do
    container = System.get_env("CONTAINER_NAME") || "default"
    is_primary = System.get_env("PRIMARY") == "true"
    {:global, base_name} = @global_name
    if is_primary, do: @global_name, else: {:global, :"#{base_name}_#{container}"}
  end

  def detectar_infraccion(pid \\ @global_name, id_compra) do
    GenServer.call(pid, {:detectar_infraccion, id_compra})
  end

  def listar_infracciones() do
    GenServer.call(@global_name, :listar_infracciones)
  end

  def actualizar_estado() do
    GenServer.call(@global_name, :actualizar_estado)
  end

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
    primario = System.get_env("PRIMARY") == "true"

    if primario do
      infraccion = Libremarket.Infracciones.detectar_infraccion(id_compra)
      new_state = Map.put(state, id_compra, infraccion)
      Replicacion.replicar_estado(new_state, replicas(), __MODULE__)
      {:reply, infraccion, new_state}
    else
      Logger.warning("Nodo réplica no debe detectar infracciones directamente")
      {:reply, :replica, state}
    end
  end

  @impl true
  def handle_call(:listar_infracciones, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call(:actualizar_estado, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call({:sync_state, new_state}, _from, _old_state) do
    Logger.info("📡 Estado sincronizado por llamada directa (#{map_size(new_state)} entradas)")
    {:reply, :ok, new_state}
  end
end

defmodule Libremarket.Infracciones.AMQP do
  @moduledoc "Proceso que consume infracciones.req y publica resultados en compras.resp"
  use GenServer
  require Logger
  alias AMQP.{Connection, Channel, Queue, Basic}

  @infracciones_req_q "infracciones.req"
  @infracciones_resp_q "compras.infracciones.resp"
  @exchange ""

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @impl true
  def init(_state) do
    amqp_url = System.get_env("AMQP_URL")

    {:ok, conn} = Connection.open(amqp_url, ssl_options: [verify: :verify_none])
    {:ok, chan} = Channel.open(conn)

    {:ok, _} = Queue.declare(chan, @infracciones_req_q, durable: false, auto_delete: false)
    :ok = Basic.qos(chan, prefetch_count: 0)
    {:ok, _ctag} = Basic.consume(chan, @infracciones_req_q, nil, no_ack: false)

    Logger.info("Infracciones.AMQP conectado y escuchando #{@infracciones_req_q}")
    {:ok, %{conn: conn, chan: chan}}
  end

  @impl true
  def handle_info({:basic_consume_ok, %{consumer_tag: _tag}}, state) do
    Logger.info("Consumidor registrado en #{@infracciones_req_q}")
    {:noreply, state}
  end

  @impl true
  def handle_info({:basic_cancel, %{consumer_tag: _tag}}, state) do
    Logger.warning("Consumo cancelado en #{@infracciones_req_q}")
    {:stop, :normal, state}
  end

  @impl true
  def handle_info({:basic_cancel_ok, %{consumer_tag: _tag}}, state) do
    Logger.info("Cancelación confirmada de consumo en #{@infracciones_req_q}")
    {:noreply, state}
  end

  @impl true
  def handle_info({:basic_deliver, payload, meta}, %{chan: chan} = st) do
    Logger.info("Mensaje recibido: #{inspect(payload)}")

    %{"id_compra" => id} = Jason.decode!(payload)
    infr? = Libremarket.Infracciones.Server.detectar_infraccion(id)
    response = Jason.encode!(%{id_compra: id, infraccion: infr?})

    {:ok, _} = Queue.declare(chan, @infracciones_resp_q, durable: false, auto_delete: false)

    :ok =
      Basic.publish(chan, @exchange, @infracciones_resp_q, response,
        content_type: "application/json",
        persistent: true
      )

    Logger.info("Verificación de infracción para #{id}: #{inspect(infr?)}")
    :ok = Basic.ack(chan, meta.delivery_tag)
    {:noreply, st}
  end

  @impl true
  def handle_info(_, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{chan: chan, conn: conn}) do
    Channel.close(chan)
    Connection.close(conn)
    :ok
  end
end
