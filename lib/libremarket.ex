defmodule Libremarket do
  @moduledoc """
  Configuración de Libremarket
  """

  use Application

  def start(_type, _args) do
    Libremarket.Supervisor.start_link()
  end
end
