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
          multicast_addr: "172.19.255.255",
          broadcast_only: true,
          secret: "secret"
        ]
      ]
    ]

    leader_to_run = System.get_env("LEADER_TO_RUN")
    server_to_run = System.get_env("SERVER_TO_RUN")

    # Construir la lista de procesos a levantar
    processes_to_run = []

    # Agregar el Leader si existe
    processes_to_run =
      case leader_to_run do
        nil ->
          processes_to_run

        leader ->
          leader_atom = String.to_existing_atom(leader)
          processes_to_run ++ [{leader_atom, %{}}]
      end

    # Agregar el Server si existe
    processes_to_run =
      case server_to_run do
        nil ->
          processes_to_run

        server ->
          server_atom = String.to_existing_atom(server)
          processes_to_run ++ [{server_atom, %{}}]
      end

    childrens =
      [
        {Libremarket.Replicacion.Registry, []},
        {Cluster.Supervisor, [topologies, [name: Libremarket.ClusterSupervisor]]}
      ] ++ processes_to_run

    Supervisor.init(childrens, strategy: :one_for_one)
  end
end
