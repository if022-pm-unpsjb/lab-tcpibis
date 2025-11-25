defmodule Libremarket.Replicacion do
  require Logger

  # Ahora 'replicas' es una lista de PIDs (incluye remotos).
  def replicar_estado(state, replica_pids, modulo) do
    Enum.each(replica_pids, fn pid ->
      # Evitar autollamada (deadlock dentro de handle_call)
      if pid != self() do
        destino = "#{inspect(pid)} (#{node(pid)})"
        Logger.info("📤 Replicando estado de #{modulo} a #{destino}")

        try do
          # GenServer.call a PID remoto funciona sobre distribución de Erlang
          :ok = GenServer.call(pid, {:sync_state, state})
          Logger.info("✅ Estado de #{modulo} sincronizado con #{destino}")
        catch
          :exit, reason ->
            Logger.error("❌ Error replicando a #{destino}: #{inspect(reason)}")
        end
      end
    end)
  end

  # Podés dejar este helper por compatibilidad, pero ya no se usa
  def sincronizar_estado_remoto(modulo, new_state) do
    Logger.info("📥 Recibido nuevo estado de #{modulo} (vía RPC)")
    :ok = GenServer.call(self(), {:sync_state, new_state})
    :ok
  end
end

defmodule Libremarket.Replicacion.Registry do
  @registry_name __MODULE__

  # Usamos :pg como registro distribuido
  def start_link(_opts \\ []) do
    :pg.start_link(@registry_name)
  end

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 500
    }
  end

  # Registrar el PID del servidor en un grupo identificado por el módulo
  def registrar(modulo, _container_name, pid) when is_pid(pid) do
    :pg.join(@registry_name, modulo, pid)
  end

  # Devolver TODOS los PIDs (locales y remotos) registrados para ese módulo
  def replicas(modulo) do
    :pg.get_members(@registry_name, modulo)
  end
end
