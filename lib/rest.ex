defmodule Libremarket.Rest do
  @moduledoc """
  API REST de Compras (Plug). No modifica el dominio.
  Rutas:
    POST   /compras           -> iniciar compra
    GET    /compras/:id       -> obtener compra
    PUT    /compras/:id       -> actualizar compra (parcial)
    DELETE /compras/:id       -> eliminar compra
    GET    /_health           -> healthcheck
  """
  use Plug.Router
  use Plug.ErrorHandler
  import Plug.Conn

  plug(Plug.Logger)

  plug(Plug.Parsers,
    parsers: [:json, :multipart, :urlencoded],
    pass: ["*/*"],
    json_decoder: Jason
  )

  plug(:match)
  plug(:dispatch)

  # --- Rutas ---

  get "/_health" do
    json(conn, 200, %{ok: true})
  end

  # Iniciar compra
  post "/compras" do
    with %{"id_producto" => id_producto, "forma_envio" => fe, "forma_pago" => fp} <-
           conn.body_params,
         {:ok, forma_envio} <- parse_envio(fe),
         {:ok, forma_pago} <- parse_pago(fp),
         reply <- Libremarket.Compras.Server.comprar(id_producto, forma_envio, forma_pago) do
      case reply do
        {:ok, data} -> json(conn, 201, data)
        {:error, data} -> json(conn, 422, data)
      end
    else
      _ -> json(conn, 400, %{error: "payload_invalido"})
    end
  end

  # Obtener compra
  get "/compras/:id" do
    id = safe_int(id)

    case Libremarket.Compras.Server.obtener_compra(id) do
      {:ok, data} -> json(conn, 200, data)
      # devolvés el intento con estado de error
      {:error, data} -> json(conn, 200, data)
      :no_encontrada -> json(conn, 404, %{error: "no_encontrada"})
    end
  end

  # Actualizar compra (parcial)
  put "/compras/:id" do
    id = safe_int(id)

    cambios =
      conn.body_params
      |> Map.take(["producto", "pago", "envio", "precio_envio", "motivo"])
      |> normalize_updates()

    case Libremarket.Compras.Server.actualizar_compra(id, cambios) do
      {:ok, compra_actualizada} -> json(conn, 200, compra_actualizada)
      {:error, :no_encontrada} -> json(conn, 404, %{error: "no_encontrada"})
      {:error, data} -> json(conn, 422, data)
      other -> json(conn, 500, %{error: "respuesta_desconocida", detalle: inspect(other)})
    end
  end

  # Eliminar compra
  delete "/compras/:id" do
    id = safe_int(id)

    case Libremarket.Compras.Server.eliminar_compra(id) do
      :ok -> send_resp(conn, 204, "")
      other -> json(conn, 500, %{error: "no_se_pudo_eliminar", detalle: inspect(other)})
    end
  end

  match _ do
    json(conn, 404, %{error: "ruta_no_encontrada"})
  end

  # --- Helpers ---

  defp json(conn, status, data) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, Jason.encode!(data))
  end

  defp safe_int(str) when is_binary(str) do
    case Integer.parse(str) do
      {n, _} -> n
      :error -> -1
    end
  end

  defp parse_envio("correo"), do: {:ok, :correo}
  defp parse_envio("retira"), do: {:ok, :retira}
  defp parse_envio(_), do: {:error, :envio_invalido}

  # Podés whitelistear medios de pago que uses en tu dominio; si no, convertir a átomo bajo riesgo.
  defp parse_pago(p) when is_binary(p) do
    # whitelist ejemplo:
    case p do
      "td" -> {:ok, :td}
      "tc" -> {:ok, :tc}
      "mp" -> {:ok, :mp}
      # si querés permitir cualquier símbolo controlado por vos
      _ -> {:ok, String.to_atom(p)}
    end
  end

  defp parse_pago(_), do: {:error, :pago_invalido}

  defp normalize_updates(map) do
    map
    |> maybe_atomize_key("envio", &parse_envio_strict/1)
    |> maybe_atomize_key("pago", &String.to_atom/1)
    |> keys_to_atoms()
  end

  defp parse_envio_strict("correo"), do: :correo
  defp parse_envio_strict("retira"), do: :retira
  defp parse_envio_strict(other), do: String.to_atom(other)

  defp maybe_atomize_key(map, key, fun) do
    case Map.fetch(map, key) do
      {:ok, v} when is_binary(v) -> Map.put(map, key, fun.(v))
      _ -> map
    end
  end

  defp keys_to_atoms(map) do
    for {k, v} <- map, into: %{} do
      {to_atom_safe(k), v}
    end
  end

  defp to_atom_safe(k) when is_binary(k), do: String.to_atom(k)
  defp to_atom_safe(k), do: k

  # Manejo de errores no capturados
  @impl true
  def handle_errors(conn, %{kind: _k, reason: r, stack: _s}) do
    json(conn, 500, %{error: "excepcion", detalle: Exception.format(:error, r)})
  end
end

defmodule Libremarket.Rest.Server do
  @moduledoc "Levanta HTTP con Plug.Cowboy para el router Libremarket.Rest."
  def child_spec(_arg) do
    port = String.to_integer(System.get_env("REST_PORT") || "4000")

    Plug.Cowboy.child_spec(
      scheme: :http,
      plug: Libremarket.Rest,
      options: [port: port]
    )
  end
end
