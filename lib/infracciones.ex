defmodule Libremarket.Infracciones do
  def detectar_infraccion(_id_compra) do
    probabilidad = :rand.uniform(100)
    probabilidad > 90
  end
end

defmodule Libremarket.Infracciones.Server do
  @moduledoc """
  Infracciones
  """

  use GenServer

  @global_name {:global, __MODULE__}

  @doc """
  Crea un nuevo servidor de Infracciones
  """
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: @global_name)
  end

  def detectar_infraccion(pid \\ @global_name, id_compra) do
    GenServer.call(pid, {:detectar_infraccion, id_compra})
  end

  def listar_infracciones() do
    GenServer.call(@global_name, :listar_infracciones)
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
    infraccion = Libremarket.Infracciones.detectar_infraccion(id_compra)
    new_state = Map.put(state, id_compra, infraccion)
    {:reply, infraccion, new_state}
  end

  @impl true
  def handle_call(:listar_infracciones, _from, state) do
    {:reply, state, state}
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
    amqp_url =
      System.get_env("AMQP_URL") ||
        "amqps://euurcdqx:pXErClaP-kSXdF8YZypEyZb5brqWRthx@jackal.rmq.cloudamqp.com/euurcdqx"

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
