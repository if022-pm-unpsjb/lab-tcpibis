defmodule Libremarket.MixProject do
  use Mix.Project

  def project do
    [
      app: :libremarket,
      version: "0.1.0",
      elixir: "~> 1.15",              # <<-- RELAJADO para correr con 1.15.7
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      mod: {Libremarket, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:libcluster, "~> 3.5"},
      {:plug_cowboy, "~> 2.7"},
      {:jason, "~> 1.4"},
      {:amqp, "~> 3.3"}               # <<-- SIN overrides ni 3.13
      # NO agregues {:amqp_client, ...} ni {:rabbit_common, ...}
    ]
  end
end
