defmodule Libremarket.Compras do
  @moduledoc """
  Orquesta el flujo de compra (sin efectos de IO).
  Expone `comprar/3` y helpers puros para cada paso.
  """

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
      # Confirmamos estado del producto tras el flujo
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
          error(%{
            producto: p,
            pago: forma_pago,
            envio: forma_envio,
            motivo: :sin_stock
          })

        {:error, reason} ->
          error(%{
            producto: producto,
            pago: forma_pago,
            envio: forma_envio,
            motivo: reason
          })
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
      {:ok, p}                 -> {:ok, p}
      {:error, :sin_stock, p}  -> {:error, :sin_stock, p}
      {:error, reason}         -> {:error, reason}
      p when is_map(p)         -> {:ok, p}   # por si el server ya devuelve el producto directo
    end
  end

  # === Paso 2: costo de envío condicional ===
  defp calcular_envio_si_corresponde(:correo) do
    {:ok, Libremarket.Envio.Server.calcular_costo_envio()}
  end
  defp calcular_envio_si_corresponde(:retira), do: {:ok, 0}

  # === Paso 3: reservar / liberar ===
    defp reservar_producto(id_producto) do
      case Libremarket.Ventas.Server.reservar_producto(id_producto) do
        :ok -> :ok
        {:ok, _state_or_stock} -> :ok   # 👈 tu server devuelve {:ok, inventario}; lo tratamos como éxito
        {:error, reason} -> {:error, reason}
        other -> {:error, {:respuesta_reservar_desconocida, other}}
      end
    end

  defp liberar_reserva(id_producto) do
    _ = Libremarket.Ventas.Server.liberar_reserva(id_producto)
    :ok
  end

  # === Paso 4: validaciones (infracciones, pago) con compensación ===
  defp validar_infracciones!(id_producto) do
    if Libremarket.Infracciones.Server.detectar_infraccion(1) do
      liberar_reserva(id_producto)
      {:error, :infraccion_detectada}
    else
      :ok
    end
  end

  defp autorizar_pago!() do
    if Libremarket.Pagos.Server.autorizar_pago() do
      :ok
    else
      {:error, :pago_rechazado}
    end
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

  # -- API pÃºblica --
  def start_link(_opts \\ %{}) do
    GenServer.start_link(__MODULE__, %{next_id: 0, compras: %{}}, name: @global_name)
  end


  @spec comprar(pid :: pid | atom, integer, :correo | :retira, atom) ::
          {:ok, map} | {:error, map}
  def comprar(pid \\ @global_name, id_producto, forma_envio, forma_pago) do
    GenServer.call(pid, {:comprar, id_producto, forma_envio, forma_pago})
  end

  def obtener_compra(pid \\ @global_name, id_compra) do
    GenServer.call(pid, {:obtener_compra, id_compra})
  end

  def eliminar_compra(pid \\ @global_name, id_compra) do
    GenServer.call(pid, {:eliminar_compra, id_compra})
  end

  def actualizar_compra(pid \\ @global_name, id_compra, cambios) do
    GenServer.call(pid, {:actualizar_compra, id_compra, cambios})
  end

  # -- GenServer callbacks --
  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:comprar, id_producto, forma_envio, forma_pago}, _from, %{next_id: id, compras: compras} = st) do
    compra_id = id + 1

    result = Libremarket.Compras.comprar(id_producto, forma_envio, forma_pago)

    reply =
      case result do
        {:ok, data}    -> {:ok, Map.put(data, :id, compra_id)}
        {:error, data} -> {:error, Map.put(data, :id, compra_id)}
      end

    # Guardar la compra en el estado
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
