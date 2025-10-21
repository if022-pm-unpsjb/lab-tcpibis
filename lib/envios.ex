defmodule Libremarket.Envio do
  def calcular_costo_envio() do
    :rand.uniform(100)
  end

  def enviar_producto() do
    IO.puts("Producto enviado correctamente")
  end

  def agendar_envio() do
    IO.puts("Producto agendado para envío correctamente")
  end
end

defmodule Libremarket.Envio.Server do
  @moduledoc """
  Envio
  """

  use GenServer

  @global_name {:global, __MODULE__}

  @doc """
  Crea un nuevo servidor de Envio
  """
  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: @global_name)
  end

  def calcular_costo_envio(pid \\ @global_name) do
    GenServer.call(pid, :calcular_costo_envio)
  end

  def enviar_producto(pid \\ @global_name) do
    GenServer.call(pid, :enviar_producto)
  end

  def agendar_envio(pid \\ @global_name) do
    GenServer.call(pid, :agendar_envio)
  end

  @doc """
  Inicializa el estado del servidor
  """
  @impl true
  def init(state) do
    {:ok, state}
  end

  @impl true
  def handle_call(:calcular_costo_envio, _from, state) do
    result = Libremarket.Envio.calcular_costo_envio()
    {:reply, result, state}
  end

  @impl true
  def handle_call(:enviar_producto, _from, state) do
    result = Libremarket.Envio.enviar_producto()
    {:reply, result, state}
  end

  @impl true
  def handle_call(:agendar_envio, _from, state) do
    result = Libremarket.Envio.agendar_envio()
    {:reply, result, state}
  end
end

defmodule Libremarket.Envio.AMQP do
  @moduledoc "Worker AMQP de Envíos: consume envios.req y publica compras.resp (tipo: \"envio\")"
  use GenServer
  require Logger
  alias AMQP.{Connection, Channel, Queue, Basic}

  @envios_req_q "envios.req"
  @envios_resp_q "compras.envios.resp"
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

    {:ok, _} = Queue.declare(chan, @envios_req_q, durable: false, auto_delete: false)
    :ok = Basic.qos(chan, prefetch_count: 0)
    {:ok, _} = Basic.consume(chan, @envios_req_q, nil, no_ack: false)

    Logger.info("Envio.AMQP conectado y escuchando #{@envios_req_q}")
    {:ok, %{conn: conn, chan: chan}}
  end

  @impl true
  def handle_info({:basic_deliver, payload, meta}, %{chan: chan} = st) do
    %{"id_compra" => id_compra, "accion" => accion} = Jason.decode!(payload)

    case accion do
      "enviar" ->
        costo = Libremarket.Envio.calcular_costo_envio()
        _ = Libremarket.Envio.Server.enviar_producto()

        resp =
          Jason.encode!(%{
            tipo: "envio",
            id_compra: id_compra,
            estado: "enviado",
            precio_envio: costo
          })

        Basic.publish(chan, @exchange, @envios_resp_q, resp, content_type: "application/json")

      "agendar" ->
        _ = Libremarket.Envio.Server.agendar_envio()

        resp =
          Jason.encode!(%{
            tipo: "envio",
            id_compra: id_compra,
            estado: "agendado"
          })

        Basic.publish(chan, @exchange, @envios_resp_q, resp, content_type: "application/json")

      other ->
        Logger.warning("Envio.AMQP: acción desconocida #{inspect(other)}")
    end

    Basic.ack(chan, meta.delivery_tag)
    {:noreply, st}
  end

  @impl true
  def handle_info({:basic_consume_ok, _}, st), do: {:noreply, st}
  def handle_info({:basic_cancel, _}, st), do: {:stop, :normal, st}
  def handle_info({:basic_cancel_ok, _}, st), do: {:noreply, st}
  def handle_info(_other, st), do: {:noreply, st}

  @impl true
  def terminate(_reason, %{chan: chan, conn: conn}) do
    Channel.close(chan)
    Connection.close(conn)
    :ok
  end
end
