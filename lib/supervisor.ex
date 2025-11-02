defmodule Libremarket.Supervisor do
  use Supervisor

  @doc """
  Inicia el supervisor
  """
  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    topologies = [
      gossip: [
        strategy: Cluster.Strategy.Gossip,
        config: [
          port: 45892,
          if_addr: "0.0.0.0",
          multicast_addr: "127.0.0.1",
          broadcast_only: true,
          secret: "secret"
        ]
      ]
    ]

    server_to_run =
      case System.get_env("SERVER_TO_RUN") do
        nil -> []
        server_to_run -> [{String.to_existing_atom(server_to_run), %{}}]
      end

    amqp_to_run =
      if System.get_env("PRIMARY") == "true" do
        case System.get_env("AMQP_TO_RUN") do
          nil -> []
          amqp_to_run -> [{String.to_existing_atom(amqp_to_run), %{}}]
        end
      else
        []
      end

    childrens =
      [
        {Libremarket.Replicacion.Registry, []},
        {Cluster.Supervisor, [topologies, [name: Libremarket.ClusterSupervisor]]}
      ] ++ server_to_run ++ amqp_to_run

    Supervisor.init(childrens, strategy: :one_for_one)
  end
end
