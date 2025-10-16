defmodule Libremarket.Infracciones do
  @moduledoc """
  Servicio de Infracciones con comunicación AMQP (RabbitMQ/CloudAMQP).

  Este módulo:
  - Escucha mensajes AMQP en la cola `"infracciones_queue"`.
  - Ejecuta `detectar_infraccion/1` para determinar si hay infracción.
  - Responde al remitente (compras) mediante la cola `reply_to`.
  """

  use GenServer
  require Logger

  @exchange "compras_infracciones"
  @queue "infracciones_queue"
  @global_name {:global, __MODULE__}

  # === API pública ===
  def start_link(_opts \\ %{}) do
    GenServer.start_link(__MODULE__, %{}, name: @global_name)
  end

  # === Lógica de negocio ===
  @doc "Simula la detección de infracciones con una probabilidad del 30%."
  def detectar_infraccion(_id_compra) do
    probabilidad = :rand.uniform(100)
    probabilidad > 70
  end

  # === Callbacks del GenServer ===
  @impl true
  def init(_) do
    Logger.info("🧩 Servidor de infracciones iniciado")

    amqp_url =
      System.get_env("AMQP_URL") ||
        raise "❌ Falta variable de entorno AMQP_URL (URL de CloudAMQP)"

    {:ok, conn} = AMQP.Connection.open(amqp_url)
    {:ok, chan} = AMQP.Channel.open(conn)

    :ok = AMQP.Exchange.declare(chan, @exchange, :direct, durable: true)
    {:ok, _} = AMQP.Queue.declare(chan, @queue, durable: true)
    :ok = AMQP.Queue.bind(chan, @queue, @exchange, routing_key: "check_infraccion")

    {:ok, _consumer_tag} = AMQP.Basic.consume(chan, @queue)
    Logger.info("📡 Escuchando mensajes en '#{@queue}' (exchange: #{@exchange})")

    {:ok, %{channel: chan}}
  end

  # Recibe mensajes desde compras
  @impl true
  def handle_info({:basic_deliver, payload, %{delivery_tag: tag, reply_to: reply_to}}, state) do
    Logger.info("📨 Mensaje recibido: #{payload}")

    with {:ok, data} <- Jason.decode(payload),
         id when is_integer(id) <- data["id_compra"] do
      resultado = detectar_infraccion(id)
      response = Jason.encode!(%{id_compra: id, infraccion: resultado})

      # Publicar respuesta en la cola indicada por reply_to
      AMQP.Basic.publish(state.channel, "", reply_to, response)
      Logger.info("✅ Respuesta enviada a '#{reply_to}': #{response}")
    else
      error ->
        Logger.error("⚠️ Error procesando mensaje #{payload}: #{inspect(error)}")
    end

    AMQP.Basic.ack(state.channel, tag)
    {:noreply, state}
  end


  @impl true
  def handle_info({:basic_consume_ok, _meta}, state) do
    Logger.debug("AMQP consume OK")
    {:noreply, state}
  end

  @impl true
  def handle_info({:basic_cancel, _meta}, state) do
    Logger.warning("AMQP consume cancelado por el broker")
    {:noreply, state}
  end

  @impl true
  def handle_info({:basic_cancel_ok, _meta}, state) do
    Logger.warning("AMQP cancel_ok: deteniendo consumidor")
    {:stop, :normal, state}
  end

  @impl true
  def handle_info(other, state) do
    Logger.debug("Mensaje no esperado en Infracciones.AMQP: #{inspect(other)}")
    {:noreply, state}
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
    # El consumidor AMQP ya vive en Libremarket.Infracciones (este módulo no arranca nada extra)
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
