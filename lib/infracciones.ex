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

  def handle_info(msg, state) do
    Logger.debug("Ignorado: #{inspect(msg)}")
    {:noreply, state}
  end
end
