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

    server_to_run = [
      String.to_existing_atom(System.get_env("SERVER_TO_RUN"))
    ]

    Supervisor.init(server_to_run, strategy: :one_for_one)
  end
end
