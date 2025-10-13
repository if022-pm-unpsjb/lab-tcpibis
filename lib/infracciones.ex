defmodule Libremarket.Infracciones do
  @moduledoc false

  def detectar_infraccion(_id_compra) do
    probabilidad = :rand.uniform(100)
    probabilidad > 70
  end
end

defmodule Libremarket.Infracciones.AMQP do
  @moduledoc """
  Consumidor AMQP para pedidos de infracciones.
  - Escucha en `@exchange` + `@queue`.
  - Responde via `reply_to` manteniendo `correlation_id`.
  """

  use GenServer
  require Logger
  alias AMQP.{Connection, Channel, Exchange, Queue, Basic}

  @exchange "compras_infracciones"     # direct
  @queue    "infracciones_queue"       # pedidos (request)
  @route    "infracciones.detectar"

  # API
  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  # Callbacks
  @impl true
  def init(state) do
    with {:ok, url} <- fetch_url(),
         {:ok, conn} <- Connection.open(url, ssl_options: ssl_opts()), #aca desactiva la verificacion
         {:ok, chan} <- Channel.open(conn),
         :ok <- Basic.qos(chan, prefetch_count: 10),
         :ok <- Exchange.declare(chan, @exchange, :direct, durable: true),
         {:ok, _} <- Queue.declare(chan, @queue, durable: true),
         :ok <- Queue.bind(chan, @queue, @exchange, routing_key: @route),
         {:ok, _tag} <- Basic.consume(chan, @queue, nil, no_ack: false) do
      {:ok, %{conn: conn, chan: chan}}
    else
      err ->
        {:stop, {:amqp_init_failed, err}}
    end
  end

  defp ssl_opts() do
    case System.get_env("INSECURE_AMQPS") do
      "1" -> [verify: :verify_none]
      _   -> []
    end
  end

  @impl true
  # Mensaje entrante desde RabbitMQ
  def handle_info({:basic_deliver, payload, %{delivery_tag: tag, reply_to: reply_to, correlation_id: cid} = meta}, %{chan: chan} = st) do
    Logger.debug("Pedido infracción recibido cid=#{inspect(cid)} reply_to=#{inspect(reply_to)} meta=#{inspect(meta, limit: 50)}")
    result =
      case safe_decode(payload) do
        {:ok, %{"id_compra" => id}} ->
          infrac = Libremarket.Infracciones.detectar_infraccion(id)
          %{id_compra: id, infraccion: infrac}
        _ ->
          %{error: "payload_invalido"}
      end

    # Responder al reply_to con el mismo correlation_id
    :ok =
      Basic.publish(
        chan,
        "",            # default exchange directo a la queue reply_to
        reply_to,
        Jason.encode!(result),
        correlation_id: cid,
        content_type: "application/json"
      )

    :ok = Basic.ack(chan, tag)
    {:noreply, st}
  end

  @impl true
  def terminate(reason, %{conn: conn, chan: chan}) do
    Logger.warning("Infracciones.AMQP terminando: #{inspect(reason)}")
    Channel.close(chan)
    Connection.close(conn)
    :ok
  end

  defp fetch_url() do
    case System.get_env("AMQP_URL") do
      nil -> {:error, :missing_amqp_url}
      url -> {:ok, url}
    end
  end

  defp safe_decode(bin) do
    try do
      {:ok, Jason.decode!(bin)}
    rescue
      _ -> {:error, :json}
    end
  end
end

defmodule Libremarket.Infracciones.Server do
  @moduledoc """
  Infracciones
  """
  use GenServer
  require Logger

  @global_name {:global, __MODULE__}

  # API del cliente
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: @global_name)
  end

  def detectar_infraccion(pid \\ @global_name, id_compra) do
    GenServer.call(pid, {:detectar_infraccion, id_compra})
  end

  def listar_infracciones() do
    GenServer.call(@global_name, :listar_infracciones)
  end

  # Callbacks
  @impl true
  def init(_state) do
    # Levantar el consumidor AMQP al iniciar el server
    case Libremarket.Infracciones.AMQP.start_link() do
      {:ok, _pid} -> Logger.info("Infracciones.AMQP iniciado")
      {:error, reason} -> Logger.error("No se pudo iniciar Infracciones.AMQP: #{inspect(reason)}")
    end
    {:ok, %{}}
  end

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
