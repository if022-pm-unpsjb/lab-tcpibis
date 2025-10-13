defmodule Libremarket.Compras.AMQP do
  @moduledoc """
  Cliente RPC para consultar infracciones vía AMQP.
  Abre conexión, declara una reply-queue exclusiva, publica el request y espera la respuesta.
  """

  require Logger
  alias AMQP.{Connection, Channel, Exchange, Queue, Basic}

  @exchange "compras_infracciones"
  @route    "infracciones.detectar"
  @timeout  5_000

  @spec detectar_infraccion_rpc(non_neg_integer(), keyword()) ::
          {:ok, boolean()} | {:error, term()}
  def detectar_infraccion_rpc(id_compra, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @timeout)

    with {:ok, url} <- fetch_url(),
         {:ok, conn} <- Connection.open(url, ssl_options: ssl_opts()),
         {:ok, chan} <- Channel.open(conn),
         :ok <- Exchange.declare(chan, @exchange, :direct, durable: true),
         {:ok, %{queue: reply_queue}} <- Queue.declare(chan, "", exclusive: true, auto_delete: true),
         {:ok, _tag} <- Basic.consume(chan, reply_queue, nil, no_ack: true) do

      cid = correlation_id()
      payload = Jason.encode!(%{id_compra: id_compra})

      :ok =
        Basic.publish(
          chan,
          @exchange,
          @route,
          payload,
          reply_to: reply_queue,
          correlation_id: cid,
          content_type: "application/json"
        )

      # Esperar respuesta con el mismo correlation_id
      res =
        receive do
          {:basic_deliver, body, %{correlation_id: ^cid}} ->
            decode_reply(body)

          # librería puede entregar como tupla desglosada
          {:"basic.deliver", body, %{correlation_id: ^cid}} ->
            decode_reply(body)

          other ->
            Logger.debug("Mensaje no esperado en RPC: #{inspect(other)}")
            {:error, :unexpected_message}
        after
          timeout -> {:error, :timeout}
        end

      Channel.close(chan)
      Connection.close(conn)
      res
    else
      err ->
        Logger.error("Compras.AMQP RPC error: #{inspect(err)}")
        {:error, {:amqp_error, err}}
    end
  end

  defp ssl_opts() do
    case System.get_env("INSECURE_AMQPS") do
      "1" -> [verify: :verify_none]
      _   -> []  
    end
  end

  defp fetch_url() do
    case System.get_env("AMQP_URL") do
      nil -> {:error, :missing_amqp_url}
      url -> {:ok, url}
    end
  end

  defp correlation_id() do
    base = :erlang.unique_integer([:positive, :monotonic]) |> Integer.to_string()
    <<"cid-", base::binary>>
  end

  defp decode_reply(bin) do
    case Jason.decode(bin) do
      {:ok, %{"infraccion" => infrac}} when is_boolean(infrac) -> {:ok, infrac}
      {:ok, %{"error" => reason}} -> {:error, {:remote_error, reason}}
      other -> {:error, {:decode_error, other}}
    end
  end
end

defmodule Libremarket.Compras do
  @moduledoc """
  Orquesta el flujo de compra (sin efectos de IO).
  Expone `comprar/3` y helpers puros para cada paso.
  """

  require Logger

  # === API principal ===
  @spec comprar(id_producto :: integer, forma_envio :: :correo | :retira, forma_pago :: atom) ::
          {:ok, map} | {:error, map}
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
            producto: p_final, pago: forma_pago, envio: forma_envio,
            precio_envio: precio_envio, motivo: :ok
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
      {:ok, p}                -> {:ok, p}
      {:error, :sin_stock, p} -> {:error, :sin_stock, p}
      {:error, reason}        -> {:error, reason}
      p when is_map(p)        -> {:ok, p}
    end
  end

  # === Paso 2: costo de envío condicional ===
  defp calcular_envio_si_corresponde(:correo), do: {:ok, Libremarket.Envio.Server.calcular_costo_envio()}
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

  # === Paso 4: validaciones (infracciones) via AMQP RPC ===
  defp validar_infracciones!(id_producto) do
    case Libremarket.Compras.AMQP.detectar_infraccion_rpc(id_producto) do
      {:ok, true} ->
        # Hay infracción: compenso y corto el flujo
        _ = liberar_reserva(id_producto)
        {:error, :infraccion_detectada}

      {:ok, false} ->
        :ok

      {:error, reason} ->
        Logger.error("No se pudo validar infracciones via AMQP: #{inspect(reason)}")
        # Podés elegir política: fallar duro o degradar.
        # Acá elijo **fallar** para no entregar si el validador está caído.
        _ = liberar_reserva(id_producto)
        {:error, :infracciones_no_disponibles}
    end
  end

  defp autorizar_pago!() do
    if Libremarket.Pagos.Server.autorizar_pago(), do: :ok, else: {:error, :pago_rechazado}
  end

  # === Paso 5: fulfillment si envío por correo ===
  defp despachar_si_correo(:correo) do
    Libremarket.Envio.Server.agendar_envio()
    Libremarket.Envio.Server.enviar_producto()
    :ok
  end
  defp despachar_si_correo(:retira), do: :ok

  # === Paso 6: revalidar estado del producto al finalizar ===
  defp revalidar_producto(id_producto) do
    case Libremarket.Ventas.Server.seleccionar_producto(id_producto) do
      {:ok, p}                -> {:ok, p}
      {:error, :sin_stock, p} -> {:error, :sin_stock, p}
      {:error, reason}        -> {:error, reason}
      p when is_map(p)        -> {:ok, p}
    end
  end

  # === Helpers de forma ===
  defp ok(map) when is_map(map), do: {:ok, map}
  defp error(map) when is_map(map), do: {:error, map}
end

defmodule Libremarket.Compras.Server do
  @moduledoc "Servidor de Compras: genera IDs y delega en `Libremarket.Compras`."
  use GenServer
  @global_name {:global, __MODULE__}

  def start_link(_opts \\ %{}) do
    GenServer.start_link(__MODULE__, %{next_id: 0, compras: %{}}, name: @global_name)
  end

  @spec comprar(pid :: pid | atom, integer, :correo | :retira, atom) ::
        {:ok, map} | {:error, map}
  def comprar(pid \\ @global_name, id_producto, forma_envio, forma_pago) do
    GenServer.call(pid, {:comprar, id_producto, forma_envio, forma_pago})
  end

  def obtener_compra(pid \\ @global_name, id_compra),  do: GenServer.call(pid, {:obtener_compra, id_compra})
  def eliminar_compra(pid \\ @global_name, id_compra), do: GenServer.call(pid, {:eliminar_compra, id_compra})
  def actualizar_compra(pid \\ @global_name, id_compra, cambios), do: GenServer.call(pid, {:actualizar_compra, id_compra, cambios})

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:comprar, id_producto, forma_envio, forma_pago}, _from, %{next_id: id, compras: compras} = st) do
    compra_id = id + 1
    result = Libremarket.Compras.comprar(id_producto, forma_envio, forma_pago)

    reply = case result do
      {:ok, data}    -> {:ok, Map.put(data, :id, compra_id)}
      {:error, data} -> {:error, Map.put(data, :id, compra_id)}
    end

    nuevo_compras = Map.put(compras, compra_id, reply)
    {:reply, reply, %{st | next_id: compra_id, compras: nuevo_compras}}
  end

  def handle_call({:actualizar_compra, id_compra, cambios}, _from, %{compras: compras} = state) do
    case Map.get(compras, id_compra) do
      {:ok, compra} ->
        compra_actualizada = Map.merge(compra, cambios)
        nuevo_compras = Map.put(compras, id_compra, {:ok, compra_actualizada})
        {:reply, {:ok, compra_actualizada}, %{state | compras: nuevo_compras}}

      {:error, _} = err ->
        {:reply, err, state}

      nil ->
        {:reply, {:error, :no_encontrada}, state}
    end
  end

  @impl true
  def handle_call({:obtener_compra, id_compra}, _from, %{compras: compras} = state) do
    compra = Map.get(compras, id_compra, :no_encontrada)
    {:reply, compra, state}
  end

  @impl true
  def handle_call({:eliminar_compra, id_compra}, _from, %{compras: compras} = state) do
    nuevo_compras = Map.delete(compras, id_compra)
    {:reply, :ok, %{state | compras: nuevo_compras}}
  end
end
