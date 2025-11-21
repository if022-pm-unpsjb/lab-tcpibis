defmodule Libremarket.Compras do
  @moduledoc """
  Orquesta el flujo de compra (sin efectos de IO).
  Expone `comprar/3` y helpers puros para cada paso.
  """

  @spec comprar(id_producto :: integer, forma_envio :: :correo | :retira, forma_pago :: atom) ::
          {:ok, map} | {:error, map}
  def comprar(id_producto, forma_envio, forma_pago) do
    with {:ok, producto} <- seleccionar_producto_normalizado(id_producto),
         {:ok, precio_envio} <- calcular_envio_si_corresponde(forma_envio),
         :ok <- reservar_producto(id_producto),
         :ok <- despachar_si_correo(forma_envio) do
      ok(%{
        producto: producto,
        pago: forma_pago,
        envio: forma_envio,
        precio_envio: precio_envio
      })
    else
      {:error, _reason} ->
        error(%{producto: nil, pago: forma_pago, envio: forma_envio})
    end
  end

  defp seleccionar_producto_normalizado(_id_producto),
    do: {:ok, %{nombre: :pendiente, cantidad: :pendiente}}

  defp calcular_envio_si_corresponde(:correo), do: {:ok, :pendiente}
  defp calcular_envio_si_corresponde(:retira), do: {:ok, 0}

  defp reservar_producto(_id_producto), do: :ok

  defp despachar_si_correo(:correo), do: :ok
  defp despachar_si_correo(:retira), do: :ok

  defp ok(map) when is_map(map), do: {:ok, map}
  defp error(map) when is_map(map), do: {:error, map}
end

defmodule Libremarket.Compras.Server do
  @moduledoc "Servidor de Compras: genera IDs y delega en `Libremarket.Compras`."
  use GenServer
  require Logger
  alias Libremarket.Replicacion

  @global_name {:global, __MODULE__}

  def start_link(opts \\ %{}) do
    container_name = System.get_env("CONTAINER_NAME") || "default"

    # 🔧 Espera activa hasta que el Leader esté registrado
    wait_for_leader()

    is_primary =
      case safe_leader_check() do
        {:ok, result} -> result
        _ -> false
      end

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

  defp wait_for_leader() do
    if Process.whereis(Libremarket.Compras.Leader) == nil do
      IO.puts("⏳ Esperando a que arranque Libremarket.Infracciones.Leader...")
      :timer.sleep(500)
      wait_for_leader()
    else
      :ok
    end
  end

  defp safe_leader_check() do
    try do
      {:ok, Libremarket.Compras.Leader.leader?()}
    catch
      :exit, _ -> {:error, :not_alive}
    end
  end

  # PIDs de réplicas (excluye el propio PID para evitar deadlock)
  def replicas() do
    my_pid = GenServer.whereis(local_name())

    Libremarket.Replicacion.Registry.replicas(__MODULE__)
    |> Enum.reject(&(&1 == my_pid))
  end

  # Nombre local (global o con sufijo de contenedor)
  defp local_name() do
    container = System.get_env("CONTAINER_NAME") || "default"
    is_primary = Libremarket.Compras.Leader.leader?()
    {:global, base_name} = @global_name

    if is_primary do
      @global_name
    else
      {:global, :"#{base_name}_#{container}"}
    end
  end

  @spec comprar(pid | atom, integer, :correo | :retira, atom, non_neg_integer) ::
          {:ok, map} | {:error, map}
  def comprar(pid \\ @global_name, id_producto, forma_envio, forma_pago, timeout \\ 30_000) do
    GenServer.call(pid, {:comprar, id_producto, forma_envio, forma_pago}, timeout)
  end

  def obtener_compra(pid \\ @global_name, id_compra) do
    GenServer.call(pid, {:obtener_compra, id_compra})
  end

  def obtener_compras(pid \\ @global_name) do
    GenServer.call(pid, :obtener_compras)
  end

  def eliminar_compra(pid \\ @global_name, id_compra) do
    GenServer.call(pid, {:eliminar_compra, id_compra})
  end

  def actualizar_compra(pid \\ @global_name, id_compra, cambios) do
    GenServer.call(pid, {:actualizar_compra, id_compra, cambios})
  end

  @impl true
  def init(_state) do
    {:ok, %{next_id: 0, compras: %{}}, {:continue, :start_amqp}}
  end

  @impl true
  def handle_continue(:start_amqp, state) do
    if Libremarket.Compras.Leader.leader?() do
      Supervisor.start_child(
        Libremarket.Supervisor,
        {Libremarket.Compras.AMQP, %{}}
      )
    end

    {:noreply, state}
  end

  @impl true
  def handle_call(
        {:comprar, id_producto, forma_envio, forma_pago},
        _from,
        %{next_id: id, compras: compras} = st
      ) do
    if Libremarket.Compras.Leader.leader?() do
      compra_id = id + 1

      result = Libremarket.Compras.comprar(id_producto, forma_envio, forma_pago)

      data_base =
        case result do
          {:ok, data} -> Map.put(data, :id, compra_id)
          {:error, data} -> Map.put(data, :id, compra_id)
        end

      compra_en_proceso =
        Map.merge(data_base, %{
          id_producto: id_producto,
          infraccion: nil,
          reservado: false,
          liberado: false
        })

      nuevo_compras = Map.put(compras, compra_id, {:en_proceso, compra_en_proceso})

      Libremarket.Compras.AMQP.publish_verificacion(compra_id)
      Libremarket.Compras.AMQP.publish_pago(compra_id)

      case compra_en_proceso[:envio] do
        :correo ->
          Libremarket.Compras.AMQP.publish_envio(compra_id, "agendar")
          Libremarket.Compras.AMQP.publish_envio(compra_id, "enviar")

        _ ->
          :ok
      end

      # Selección y reserva siempre ocurren; la liberación se decide cuando corresponda (pago/infracción)
      Libremarket.Compras.AMQP.publish_venta(compra_id, id_producto, "seleccionar")
      Libremarket.Compras.AMQP.publish_venta(compra_id, id_producto, "reservar")

      new_state = %{st | next_id: compra_id, compras: nuevo_compras}
      Replicacion.replicar_estado(new_state, replicas(), __MODULE__)
      {:reply, {:en_proceso, compra_en_proceso}, new_state}
    else
      Logger.warning("Nodo réplica no debe ejecutar compras directamente")
      {:reply, :replica, st}
    end
  end

  @impl true
  def handle_call(:obtener_compras, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call({:actualizar_compra, id_compra, cambios}, _from, %{compras: compras} = st) do
    case Map.get(compras, id_compra) do
      {estado_actual, compra} when estado_actual in [:en_proceso, :ok, :error] ->
        compra2 = Map.merge(compra, cambios)
        nuevo_compras = Map.put(compras, id_compra, {estado_actual, compra2})

        new_state = %{st | compras: nuevo_compras}
        # 🔁 si es primario, replicar
        if Libremarket.Compras.Leader.leader?() do
          Replicacion.replicar_estado(new_state, replicas(), __MODULE__)
        end

        {:reply, {estado_actual, compra2}, new_state}

      nil ->
        {:reply, {:error, :no_encontrada}, st}
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

  @impl true
  def handle_call({:procesar_infraccion, id_compra, infr?}, _from, %{compras: compras} = st) do
    case Map.get(compras, id_compra) do
      {:en_proceso, compra} ->
        compra2 = Map.put(compra, :infraccion, infr?)

        # Liberar si ya estaba reservado y aún no liberado
        compra2 =
          if infr? and compra2[:reservado] == true and compra2[:liberado] != true do
            Libremarket.Compras.AMQP.publish_venta(id_compra, compra2[:id_producto], "liberar")
            Map.put(compra2, :liberado, true)
          else
            compra2
          end

        new_compras =
          if infr? or compra2[:pago_estado] == :rechazado or compra2[:motivo] == :sin_stock do
            Map.put(compras, id_compra, {:error, compra2})
          else
            Map.put(compras, id_compra, {:ok, compra2})
          end

        new_state = %{st | compras: new_compras}

        if Libremarket.Compras.Leader.leader?() do
          Replicacion.replicar_estado(new_state, replicas(), __MODULE__)
        end

        {:reply, Map.get(new_compras, id_compra), new_state}

      {:ok, compra} ->
        compra2 = Map.put(compra, :infraccion, infr?)

        compra2 =
          if infr? and compra2[:reservado] == true and compra2[:liberado] != true do
            Libremarket.Compras.AMQP.publish_venta(id_compra, compra2[:id_producto], "liberar")
            Map.put(compra2, :liberado, true)
          else
            compra2
          end

        new_state = %{st | compras: Map.put(compras, id_compra, {:ok, compra2})}

        if Libremarket.Compras.Leader.leader?() do
          Replicacion.replicar_estado(new_state, replicas(), __MODULE__)
        end

        {:reply, {:ok, compra2}, new_state}

      {:error, compra} ->
        compra2 = Map.put(compra, :infraccion, infr?)

        compra2 =
          if infr? and compra2[:reservado] == true and compra2[:liberado] != true do
            Libremarket.Compras.AMQP.publish_venta(id_compra, compra2[:id_producto], "liberar")
            Map.put(compra2, :liberado, true)
          else
            compra2
          end

        new_state = %{st | compras: Map.put(compras, id_compra, {:error, compra2})}

        if Libremarket.Compras.Leader.leader?() do
          Replicacion.replicar_estado(new_state, replicas(), __MODULE__)
        end

        {:reply, {:error, compra2}, new_state}

      nil ->
        {:reply, {:error, :no_encontrada}, st}
    end
  end

  @impl true
  def handle_call({:sync_state, new_state}, _from, _old_state) do
    Logger.info("📡 Estado sincronizado por llamada directa (#{map_size(new_state)} entradas)")
    {:reply, :ok, new_state}
  end

  def procesar_infraccion(pid \\ @global_name, id_compra, infraccion?) do
    GenServer.call(pid, {:procesar_infraccion, id_compra, infraccion?})
  end
end

defmodule Libremarket.Compras.Leader do
  @moduledoc """
  Maneja la elección de líder para el servicio Compras usando ZooKeeper.
  """

  use GenServer

  @base_path "/libremarket/compras"
  @leader_path "/libremarket/compras/leader"

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Indica si este nodo es el líder."
  def leader? do
    GenServer.call(__MODULE__, :leader?)
  end

  @impl true
  def init(_opts) do
    {:ok, zk} = Libremarket.ZK.connect()

    # Aseguramos estructura en ZooKeeper (con reintentos seguros)
    wait_for_zk(zk, @base_path)
    wait_for_zk(zk, @leader_path)

    # Creamos znode efímero secuencial como candidato a líder
    {:ok, my_znode} =
      :erlzk.create(
        zk,
        @leader_path <> "/nodo-",
        :ephemeral_sequential
      )

    leader? = compute_leader?(zk, my_znode)

    IO.puts("🟢 Compras.Leader → soy líder? #{leader?} (#{my_znode})")

    {:ok, %{zk: zk, my_znode: my_znode, leader?: leader?}}
  end

  @impl true
  def handle_call(:leader?, _from, state) do
    {:reply, state.leader?, state}
  end

  # Garantiza que se puede crear la jerarquía en ZK
  defp wait_for_zk(zk, path, retries \\ 5)
  defp wait_for_zk(_zk, path, 0), do: raise("ZooKeeper no respondió creando #{path}")

  defp wait_for_zk(zk, path, retries) do
    case Libremarket.ZK.ensure_path(zk, path) do
      :ok ->
        :ok

      {:error, _} ->
        IO.puts("⚠️  Compras.Leader: reintentando crear #{path}…")
        :timer.sleep(1_000)
        wait_for_zk(zk, path, retries - 1)
    end
  end

  # Determina si este nodo es el líder comparando nombres lexicográficamente
  defp compute_leader?(zk, my_znode) do
    {:ok, children} = :erlzk.get_children(zk, @leader_path)

    sorted =
      children
      |> Enum.map(&List.to_string/1)
      |> Enum.sort()

    my_name = Path.basename(List.to_string(my_znode))

    [first | _] = sorted

    my_name == first
  end
end

defmodule Libremarket.Compras.AMQP do
  use GenServer
  alias AMQP.{Connection, Channel, Basic, Queue}
  require Logger

  @exchange ""
  @infracciones_req_q "infracciones.req"
  @infracciones_resp_q "compras.infracciones.resp"
  @pagos_req_q "pagos.req"
  @pagos_resp_q "compras.pagos.resp"
  @envios_resp_q "compras.envios.resp"
  @envios_req_q "envios.req"
  @ventas_req_q "ventas.req"
  @ventas_resp_q "compras.ventas.resp"

  def start_link(_opts), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  @impl true
  def init(_) do
    amqp_url = System.get_env("AMQP_URL")
    {:ok, conn} = Connection.open(amqp_url, ssl_options: [verify: :verify_none])
    {:ok, chan} = Channel.open(conn)
    {:ok, _} = Queue.declare(chan, @infracciones_resp_q, durable: false)
    {:ok, _} = Queue.declare(chan, @pagos_resp_q, durable: false)
    {:ok, _} = Queue.declare(chan, @envios_resp_q, durable: false)
    {:ok, _} = Queue.declare(chan, @ventas_resp_q, durable: false)
    {:ok, _} = Basic.consume(chan, @ventas_resp_q, nil, no_ack: true)
    {:ok, _} = Basic.consume(chan, @infracciones_resp_q, nil, no_ack: true)
    {:ok, _} = Basic.consume(chan, @pagos_resp_q, nil, no_ack: true)
    {:ok, _} = Basic.consume(chan, @envios_resp_q, nil, no_ack: true)
    {:ok, %{conn: conn, chan: chan}}
  end

  def publish_verificacion(id_compra) do
    GenServer.cast(__MODULE__, {:verificar, id_compra})
  end

  def publish_pago(id_compra) do
    GenServer.cast(__MODULE__, {:pago, id_compra})
  end

  def publish_envio(id_compra, accion) when accion in ["agendar", "enviar"] do
    GenServer.cast(__MODULE__, {:envio, id_compra, accion})
  end

  def publish_venta(id_compra, id_producto, accion)
      when accion in ["seleccionar", "reservar", "liberar"] do
    GenServer.cast(__MODULE__, {:venta, id_compra, id_producto, accion})
  end

  @impl true
  def handle_cast({:verificar, id_compra}, %{chan: chan} = st) do
    payload = Jason.encode!(%{id_compra: id_compra})

    :ok =
      Basic.publish(chan, @exchange, @infracciones_req_q, payload,
        content_type: "application/json",
        reply_to: @infracciones_resp_q
      )

    Logger.info("Compras → publicó solicitud de verificación #{id_compra}")
    {:noreply, st}
  end

  @impl true
  def handle_cast({:pago, id_compra}, %{chan: chan} = st) do
    payload = Jason.encode!(%{id_compra: id_compra})
    :ok = Basic.publish(chan, @exchange, @pagos_req_q, payload, content_type: "application/json")
    Logger.info("Compras.AMQP → publicado pago para compra #{id_compra}")
    {:noreply, st}
  end

  @impl true
  def handle_cast({:venta, id_compra, id_producto, accion}, %{chan: chan} = st) do
    payload = Jason.encode!(%{id_compra: id_compra, id_producto: id_producto, accion: accion})
    :ok = Basic.publish(chan, @exchange, @ventas_req_q, payload, content_type: "application/json")

    Logger.info(
      "Compras.AMQP → publicado ventas '#{accion}' (compra #{id_compra}, prod #{id_producto})"
    )

    {:noreply, st}
  end

  @impl true
  def handle_cast({:envio, id_compra, accion}, %{chan: chan} = st) do
    payload = Jason.encode!(%{id_compra: id_compra, accion: accion})
    :ok = Basic.publish(chan, @exchange, @envios_req_q, payload, content_type: "application/json")
    Logger.info("Compras.AMQP → publicado envío '#{accion}' para compra #{id_compra}")
    {:noreply, st}
  end

  @impl true
  def handle_info({:basic_consume_ok, %{consumer_tag: tag}}, state) do
    Logger.info("Compras.AMQP → consumo registrado (#{tag})")
    {:noreply, state}
  end

  @impl true
  def handle_info({:basic_cancel, %{consumer_tag: tag}}, state) do
    Logger.warning("Compras.AMQP → consumo cancelado (#{tag})")
    {:stop, :normal, state}
  end

  @impl true
  def handle_info({:basic_cancel_ok, %{consumer_tag: tag}}, state) do
    Logger.info("Compras.AMQP → cancelación confirmada (#{tag})")
    {:noreply, state}
  end

  @impl true
  def handle_info({:basic_deliver, payload, _meta}, state) do
    msg = Jason.decode!(payload)

    cond do
      Map.has_key?(msg, "infraccion") ->
        id = msg["id_compra"]
        infr? = msg["infraccion"]
        Libremarket.Compras.Server.procesar_infraccion(id, infr?)
        {:noreply, state}

      msg["tipo"] == "envio" ->
        id = msg["id_compra"]

        cambios =
          if Map.has_key?(msg, "precio_envio"),
            do: %{precio_envio: msg["precio_envio"]},
            else: %{}

        if map_size(cambios) > 0 do
          _ = Libremarket.Compras.Server.actualizar_compra(id, cambios)
        end

        {:noreply, state}

      msg["tipo"] == "pago" ->
        id = msg["id_compra"]
        autorizado = msg["autorizado"]

        if autorizado do
          _ = Libremarket.Compras.Server.actualizar_compra(id, %{pago_estado: :autorizado})
          Logger.info("Compras.AMQP → pago autorizado para #{id}")
        else
          _ =
            Libremarket.Compras.Server.actualizar_compra(id, %{
              pago_estado: :rechazado,
              motivo: :pago_rechazado
            })

          # Intentar liberar si ya estaba reservado (y no liberado)
          case Libremarket.Compras.Server.obtener_compra(id) do
            {_, compra} when is_map(compra) ->
              if compra[:reservado] == true and compra[:liberado] != true do
                Libremarket.Compras.AMQP.publish_venta(id, compra[:id_producto], "liberar")
                _ = Libremarket.Compras.Server.actualizar_compra(id, %{liberado: true})
              end

            _ ->
              :ok
          end

          Logger.warning("Compras.AMQP → pago RECHAZADO para #{id}")
        end

        {:noreply, state}

      msg["tipo"] == "venta" ->
        id = msg["id_compra"]
        op = msg["op"]
        ok? = msg["ok"]

        cambios =
          case {op, ok?} do
            {"seleccionar", true} ->
              %{producto: msg["producto"]}

            {"seleccionar", false} ->
              case msg["motivo"] do
                "sin_stock" -> %{motivo: :sin_stock, producto: msg["producto"]}
                "not_found" -> %{motivo: :producto_inexistente}
                other -> %{motivo: String.to_atom(other || "error_ventas")}
              end

            {"reservar", true} ->
              # Marcar reserva efectiva
              %{reservado: true}

            {"reservar", false} ->
              case msg["motivo"] do
                "sin_stock" -> %{motivo: :sin_stock}
                "not_found" -> %{motivo: :producto_inexistente}
                other -> %{motivo: String.to_atom(other || "error_ventas")}
              end

            {"liberar", _} ->
              # Estado local tras liberación
              %{reservado: false, liberado: true}

            _ ->
              %{}
          end

        if map_size(cambios) > 0 do
          case Libremarket.Compras.Server.actualizar_compra(id, cambios) do
            {estado, compra2} when is_map(compra2) ->
              # Si llegó la confirmación de reserva pero ya tenemos error (pago/infracción),
              # liberar ahora (si no se liberó antes) para evitar fugas de stock
              cond do
                op == "reservar" and ok? == true and
                  compra2[:reservado] == true and compra2[:liberado] != true and
                    (compra2[:pago_estado] == :rechazado or compra2[:infraccion] == true) ->
                  Libremarket.Compras.AMQP.publish_venta(id, compra2[:id_producto], "liberar")
                  _ = Libremarket.Compras.Server.actualizar_compra(id, %{liberado: true})
                  :ok

                true ->
                  :ok
              end

            _ ->
              :ok
          end
        end

        {:noreply, state}

      true ->
        Logger.warning("Compras.AMQP: mensaje desconocido #{inspect(msg)}")
        {:noreply, state}
    end
  end
end
