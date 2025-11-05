defmodule Libremarket.ZK do
  @moduledoc false

  @zk_host System.get_env("ZK_HOST") || "zookeeper:2181"

  def connect() do
    {hostname, port} = parse_host(@zk_host)
    hosts = [{hostname, port}]
    timeout = 30_000

    IO.puts("🔌 Conectando a ZooKeeper en #{hostname}:#{port}")

    case :erlzk.connect(hosts, timeout) do
      {:ok, zk} ->
        :timer.sleep(500)
        IO.puts("✅ Conexión establecida con ZooKeeper")
        {:ok, zk}

      {:error, reason} ->
        IO.puts("❌ No se pudo conectar a ZooKeeper: #{inspect(reason)}")
        {:error, reason}
    end
  end

  # crea recursivamente /a/b/c usando SOLO la API soportada por erlzk
  def ensure_path(zk, path, retries \\ 3)

  def ensure_path(_zk, _path, 0) do
    {:error, :zk_not_ready}
  end

  def ensure_path(zk, path, retries) do
    path = to_charlist(path)

    case :erlzk.exists(zk, path) do
      {:ok, _stat} ->
        :ok

      {:error, :no_node} ->
        parent = Path.dirname(List.to_string(path))

        if parent != "/" do
          :ok = ensure_path(zk, parent, retries)
        end

        # OJO: acá usamos la forma simple
        case :erlzk.create(zk, path, :persistent) do
          {:ok, _} ->
            :ok

          {:error, :node_exists} ->
            :ok

          {:error, :closed} ->
            IO.puts("⚠️ ZooKeeper aún no listo (closed) para #{path}, reintento…")
            :timer.sleep(1_000)
            ensure_path(zk, path, retries - 1)

          {:error, reason} ->
            IO.puts("⚠️ Error creando #{path}: #{inspect(reason)}, reintento…")
            :timer.sleep(1_000)
            ensure_path(zk, path, retries - 1)
        end

      {:error, :closed} ->
        IO.puts("⚠️ ZooKeeper aún no listo para exists(#{path}), reintento…")
        :timer.sleep(1_000)
        ensure_path(zk, path, retries - 1)

      other ->
        IO.puts("⚠️ Resultado inesperado de exists(): #{inspect(other)}")
        {:error, :unexpected_result}
    end
  end

  defp parse_host(str) do
    case String.split(str, ":") do
      [host, port] -> {String.to_charlist(host), String.to_integer(port)}
      [host] -> {String.to_charlist(host), 2181}
    end
  end
end
