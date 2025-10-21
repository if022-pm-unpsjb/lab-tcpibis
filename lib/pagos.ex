defmodule Libremarket.Pagos do
  def autorizar_pago(_id_compra) do
    probabilidad = :rand.uniform(100)
    probabilidad < 70
  end
end

defmodule Libremarket.Pagos.Server do
  use GenServer

  @global_name {:global, __MODULE__}

  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: @global_name)
  end

  def autorizar_pago(pid \\ @global_name, id_compra) do
    GenServer.call(pid, {:autorizar_pago, id_compra})
  end

  def obtener_pagos() do
    GenServer.call(@global_name, :obtener_pagos)
  end

  def init(_opts) do
    {:ok, %{}}
  end

  def handle_call({:autorizar_pago, id_compra}, _from, state) do
    autorizado = Libremarket.Pagos.autorizar_pago(id_compra)
    new_state = Map.put(state, id_compra, autorizado)
    {:reply, autorizado, new_state}
  end

  def handle_call(:obtener_pagos, _from, state) do
    {:reply, state, state}
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
    amqp_url =
      System.get_env("AMQP_URL") ||
        "amqps://euurcdqx:pXErClaP-kSXdF8YZypEyZb5brqWRthx@jackal.rmq.cloudamqp.com/euurcdqx"

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
