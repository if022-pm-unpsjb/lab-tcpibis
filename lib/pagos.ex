defmodule Libremarket.Pagos do
  def autorizar_pago(_id_compra) do
    probabilidad = :rand.uniform(100)
    probabilidad < 70
  end
end

defmodule Libremarket.Pagos.Server do
  use GenServer
  require Logger

  @global_name {:global, __MODULE__}

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

    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def replicas() do
    {:ok, hostname} = :inet.gethostname()
    hostname_str = List.to_string(hostname)

    [
      {String.to_atom("pagos_replica_1@#{hostname_str}"), __MODULE__},
      {String.to_atom("pagos_replica_2@#{hostname_str}"), __MODULE__}
    ]
  end

  def autorizar_pago(pid \\ @global_name, id_compra) do
    GenServer.call(pid, {:autorizar_pago, id_compra})
  end

  def obtener_pagos() do
    GenServer.call(@global_name, :obtener_pagos)
  end

  @impl true
  def init(_opts) do
    {:ok, %{}}
  end

  def handle_call({:autorizar_pago, id_compra}, _from, state) do
    primario = System.get_env("PRIMARY") == "true"

    if primario do
      autorizado = Libremarket.Pagos.autorizar_pago(id_compra)
      new_state = Map.put(state, id_compra, autorizado)
      replicar_estado(new_state)
      {:reply, autorizado, new_state}
    else
      Logger.warning("Nodo réplica no debe detectar autorizar pagos directamente")
      {:reply, :replica, state}
    end
  end

  def handle_call(:obtener_pagos, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call({:sync_state, new_state}, _from, _old_state) do
    Logger.info("📡 Estado sincronizado por llamada directa (#{map_size(new_state)} entradas)")
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_cast({:sync_state, new_state}, _state) do
    Logger.info("📡 Estado actualizado desde primario (#{map_size(new_state)} entradas)")
    {:noreply, new_state}
  end

  # =======================
  # Replicación RPC
  # =======================
  defp replicar_estado(state) do
    Enum.each(replicas(), fn {nodo, mod} ->
      Logger.info("📤 Replicando estado a #{nodo}")

      try do
        case :rpc.call(nodo, mod, :sincronizar_estado_remoto, [state]) do
          :ok -> Logger.info("✅ Estado sincronizado con #{nodo}")
          other -> Logger.warning("⚠️ Respuesta inesperada de #{nodo}: #{inspect(other)}")
        end
      catch
        :exit, reason ->
          Logger.error("❌ Error replicando a #{nodo}: #{inspect(reason)}")
      end
    end)
  end

  def sincronizar_estado_remoto(new_state) do
    container_name = System.get_env("CONTAINER_NAME") || "default"
    is_primary = System.get_env("PRIMARY") == "true"

    {:global, base_name} = @global_name

    local_name =
      if is_primary do
        @global_name
      else
        {:global, :"#{base_name}_#{container_name}"}
      end

    Logger.info("📥 Recibido nuevo estado en #{inspect(local_name)}")
    GenServer.call(local_name, {:sync_state, new_state})
    :ok
  end
end

defmodule Libremarket.Pagos.AMQP do
  @moduledoc "Proceso que consume pagos.req y publica resultados en compras.pagos.resp"
  use GenServer
  require Logger
  alias AMQP.{Connection, Channel, Queue, Basic}

  @pagos_req_q "pagos.req"
  @pagos_resp_q "compras.pagos.resp"
  @exchange ""

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @impl true
  def init(_state) do
    amqp_url = System.get_env("AMQP_URL")

    {:ok, conn} = Connection.open(amqp_url, ssl_options: [verify: :verify_none])
    {:ok, chan} = Channel.open(conn)

    {:ok, _} = Queue.declare(chan, @pagos_req_q, durable: false, auto_delete: false)
    :ok = Basic.qos(chan, prefetch_count: 0)
    {:ok, _} = Basic.consume(chan, @pagos_req_q, nil, no_ack: false)

    Logger.info("Pagos.AMQP conectado y escuchando #{@pagos_req_q}")
    {:ok, %{conn: conn, chan: chan}}
  end

  @impl true
  def handle_info({:basic_deliver, payload, meta}, %{chan: chan} = st) do
    %{"id_compra" => id_compra} = Jason.decode!(payload)

    autorizado = Libremarket.Pagos.Server.autorizar_pago(id_compra)

    resp =
      Jason.encode!(%{
        tipo: "pago",
        id_compra: id_compra,
        autorizado: autorizado
      })

    Basic.publish(chan, @exchange, @pagos_resp_q, resp, content_type: "application/json")
    Basic.ack(chan, meta.delivery_tag)

    Logger.info("Pagos.AMQP → procesó pago #{id_compra}, resultado=#{autorizado}")
    {:noreply, st}
  end

  @impl true
  def handle_info({:basic_consume_ok, _}, st), do: {:noreply, st}
  def handle_info({:basic_cancel, _}, st), do: {:stop, :normal, st}
  def handle_info({:basic_cancel_ok, _}, st), do: {:noreply, st}
  def handle_info(_, st), do: {:noreply, st}

  @impl true
  def terminate(_reason, %{chan: chan, conn: conn}) do
    Channel.close(chan)
    Connection.close(conn)
    :ok
  end
end
