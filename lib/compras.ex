defmodule Libremarket.Compras do
  @moduledoc """
  Orquesta el flujo de compra, comunicándose con otros servicios por AMQP.
  Incluye comunicación con `Infracciones` vía CloudAMQP.
  """

  require Logger
  alias __MODULE__.AMQPClient

  # === API principal ===
  @spec comprar(integer, :correo | :retira, atom) :: {:ok, map} | {:error, map}
  def comprar(id_producto, forma_envio, forma_pago) do
    with {:ok, producto} <- seleccionar_producto_normalizado(id_producto),
         {:ok, precio_envio} <- calcular_envio_si_corresponde(forma_envio),
         :ok <- reservar_producto(id_producto),
         :ok <- validar_infracciones!(id_producto),
         :ok <- autorizar_pago!(),
         :ok <- despachar_si_correo(forma_envio) do

      case revalidar_producto(id_producto) do
        {:ok, p_final} ->
          ok(%{
            producto: p_final,
            pago: forma_pago,
            envio: forma_envio,
            precio_envio: precio_envio,
            motivo: :ok
          })

        {:error, :sin_stock, p} ->
          error(%{producto: p, pago: forma_pago, envio: forma_envio, motivo: :sin_stock})

        {:error, reason} ->
          error(%{producto: producto, pago: forma_pago, envio: forma_envio, motivo: reason})
      end

    else
      {:error, :sin_stock, p} ->
        error(%{producto: p, pago: forma_pago, envio: forma_envio, motivo: :sin_stock})

      {:error, :pago_rechazado} ->
        _ = liberar_reserva(id_producto)
        error(%{producto: nil, pago: forma_pago, envio: forma_envio, motivo: :pago_rechazado})

      {:error, reason} ->
        error(%{producto: nil, pago: forma_pago, envio: forma_envio, motivo: reason})
    end
  end

  # === Paso 1: seleccionar producto y normalizar ===
  defp seleccionar_producto_normalizado(id_producto) do
    case Libremarket.Ventas.Server.seleccionar_producto(id_producto) do
      {:ok, p} -> {:ok, p}
      {:error, :sin_stock, p} -> {:error, :sin_stock, p}
      {:error, reason} -> {:error, reason}
      p when is_map(p) -> {:ok, p}
    end
  end

  # === Paso 2: costo de envío condicional ===
  defp calcular_envio_si_corresponde(:correo),
    do: {:ok, Libremarket.Envio.Server.calcular_costo_envio()}
  defp calcular_envio_si_corresponde(:retira), do: {:ok, 0}

  # === Paso 3: reservar / liberar ===
  defp reservar_producto(id_producto) do
    case Libremarket.Ventas.Server.reservar_producto(id_producto) do
      :ok -> :ok
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
      other -> {:error, {:respuesta_reservar_desconocida, other}}
    end
  end

  defp liberar_reserva(id_producto) do
    _ = Libremarket.Ventas.Server.liberar_reserva(id_producto)
    :ok
  end

  # === Paso 4: validar infracciones vía AMQP ===
  defp validar_infracciones!(id_producto) do
    case AMQPClient.check_infraccion(id_producto) do
      true ->
        Logger.warning("🚫 Infracción detectada para producto #{id_producto}")
        liberar_reserva(id_producto)
        {:error, :infraccion_detectada}

      false ->
        :ok

      :timeout ->
        Logger.warning("⚠️ Timeout esperando respuesta de infracciones")
        :ok
    end
  end

  # === Paso 5: autorizar pago (simulado) ===
  defp autorizar_pago!() do
    if Libremarket.Pagos.Server.autorizar_pago() do
      :ok
    else
      {:error, :pago_rechazado}
    end
  end

  # === Paso 6: fulfillment ===
  defp despachar_si_correo(:correo) do
    Libremarket.Envio.Server.agendar_envio()
    Libremarket.Envio.Server.enviar_producto()
    :ok
  end

  defp despachar_si_correo(:retira), do: :ok

  # === Paso 7: revalidar producto ===
  defp revalidar_producto(id_producto) do
    case Libremarket.Ventas.Server.seleccionar_producto(id_producto) do
      {:ok, p} -> {:ok, p}
      {:error, :sin_stock, p} -> {:error, :sin_stock, p}
      {:error, reason} -> {:error, reason}
      p when is_map(p) -> {:ok, p}
    end
  end

  # === Helpers ===
  defp ok(map) when is_map(map), do: {:ok, map}
  defp error(map) when is_map(map), do: {:error, map}

end

# --------------------------------------------------------
# === Submódulo: Cliente AMQP para comunicación con Infracciones ===
# --------------------------------------------------------
defmodule Libremarket.Compras.AMQPClient do
  @moduledoc false
  require Logger
  @exchange "compras_infracciones"

  def check_infraccion(id_compra) do
    amqp_url = System.get_env("AMQP_URL") || raise "❌ Falta AMQP_URL"
    {:ok, conn} = AMQP.Connection.open(amqp_url, ssl_options: [verify: :verify_none])
    {:ok, chan} = AMQP.Channel.open(conn)
    # --- asegurar topología (idempotente) ---
    :ok = AMQP.Exchange.declare(chan, @exchange, :direct, durable: true)
    {:ok, _} = AMQP.Queue.declare(chan, "infracciones_queue", durable: true)
    :ok = AMQP.Queue.bind(chan, "infracciones_queue", @exchange, routing_key: "check_infraccion")
    # ----------------------------------------


    # Cola temporal para recibir respuesta
    {:ok, %{queue: reply_queue}} = AMQP.Queue.declare(chan, "", exclusive: true)

    # Consumir sincrónicamente
    {:ok, _consumer_tag} = AMQP.Basic.consume(chan, reply_queue, nil, no_ack: true)

    payload = Jason.encode!(%{id_compra: id_compra})
    AMQP.Basic.publish(chan, @exchange, "check_infraccion", payload, reply_to: reply_queue)
    Logger.info("📤 Enviado mensaje a Infracciones: #{payload}")

    # Esperar la respuesta correcta
   infraccion = wait_reply(10_000)

    AMQP.Connection.close(conn)
    infraccion

  end
  defp wait_reply(timeout) do
    receive do
      {:basic_deliver, msg, _meta} ->
        case Jason.decode(msg) do
          {:ok, %{"infraccion" => val}} -> val
          _ -> :timeout
        end

      {:basic_consume_ok, _} ->
        wait_reply(timeout)

      {:basic_cancel, _} ->
        :timeout

      {:basic_cancel_ok, _} ->
        :timeout

      other ->
        # Ignorar cualquier otro mensaje y seguir esperando
        wait_reply(timeout)
    after
      timeout -> :timeout
    end
  end

end

# ===================================================================
# === Servidor de Compras (sin supervisor propio, lo arranca el global)
# ===================================================================
defmodule Libremarket.Compras.Server do
  @moduledoc "Servidor de Compras: genera IDs y coordina el flujo de compra."
  use GenServer
  require Logger

  @global_name {:global, __MODULE__}

  # === API pública ===
  def start_link(_opts \\ %{}) do
    GenServer.start_link(__MODULE__, %{next_id: 0, compras: %{}}, name: @global_name)
  end

  @spec comprar(pid | atom, integer, :correo | :retira, atom, non_neg_integer) ::
        {:ok, map} | {:error, map}
  def comprar(pid \\ @global_name, id_producto, forma_envio, forma_pago, timeout \\ 30_000) do
    GenServer.call(pid, {:comprar, id_producto, forma_envio, forma_pago}, timeout)
  end


  # === Callbacks ===
  @impl true
  def init(state) do
    Logger.info("🛒 Servidor de Compras iniciado")
    {:ok, state}
  end

  @impl true
  def handle_call({:comprar, id_producto, forma_envio, forma_pago}, _from, %{next_id: id, compras: compras} = st) do
    compra_id = id + 1
    result = Libremarket.Compras.comprar(id_producto, forma_envio, forma_pago)

    reply =
      case result do
        {:ok, data} -> {:ok, Map.put(data, :id, compra_id)}
        {:error, data} -> {:error, Map.put(data, :id, compra_id)}
      end

    nuevo_compras = Map.put(compras, compra_id, reply)
    {:reply, reply, %{st | next_id: compra_id, compras: nuevo_compras}}
  end
end
