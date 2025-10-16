    #!/bin/bash

    start() {
        # Levantar los contenedores en segundo plano
        export DOCKER_UID=$UID
        export DOCKER_GID=$GID
        export SECRET=secret
        export AMQP_URL="amqps://euurcdqx:pXErClaP-kSXdF8YZypEyZb5brqWRthx@jackal.rmq.cloudamqp.com/euurcdqx"
        docker compose up -d "$@"
    }

    stop() {
        export DOCKER_UID=$UID
        export DOCKER_GID=$GID
        export SECRET=secret
        export AMQP_URL="amqps://euurcdqx:pXErClaP-kSXdF8YZypEyZb5brqWRthx@jackal.rmq.cloudamqp.com/euurcdqx"
        docker compose down
    }

    # Comprobar el argumento proporcionado
    if [[ $1 == "start" ]]; then
        shift
        start "$@"
    elif [[ $1 == "stop" ]]; then
        stop
    elif [[ $1 == "build" ]]; then
        docker run -it --rm -v "$(pwd)":/app -w /app -u $(id -u)\:$(id -g) -e MIX_HOME=/app/mix_home -e HEX_HOME=/app/hex_home --network host elixir:1.15.7-alpine mix compile
    elif [[ $1 == "iex" ]]; then
        docker attach $2
    elif [[ $1 == "todo" ]]; then
        stop
        docker run -it --rm \
        -v "$(pwd)":/app -w /app \
        -u $(id -u):$(id -g) \
        -e MIX_HOME=/app/mix_home -e HEX_HOME=/app/hex_home \
        --network host \
        elixir:1.15.7-alpine \
        sh -lc 'mix local.hex --force && mix local.rebar --force && mix deps.get && mix deps.compile && mix compile'
        shift
        start "$@"
    else
        echo "Uso: $0 {start|stop|iex nombre_de_contenedor}"
        exit 1
    fi
