# Docker Compose

## What it is

Docker Compose reads a YAML file (`docker-compose.yml`) that describes one or more containers and starts them together with one command. It adds the network between them, published ports, environment variables and health checks. This repository uses Compose v2 (`docker compose`, with a space), tested with v2.23.

## Why this repository uses it

The project needs two servers, Kafka and PostgreSQL, running together. One file describes both; `make up` starts them and waits until both are healthy.

## Where it appears

| Path | What it does |
| --- | --- |
| `docker-compose.yml` | The description: services `kafka` and `postgres`. |
| `Makefile` | `docker compose up -d --wait`, `exec` into either service, `down -v`. |
| `.env` (created from `.env.example`) | Compose reads it automatically to fill `${...}` placeholders. |

## How it works underneath

`docker compose up` reads the file, creates a project (named by `name:`), a default network, pulls images, creates and starts the containers. `--wait` polls each container's health check until healthy. Service names work as host names inside the network. Before it starts, Compose replaces `${NAME}` and `${NAME:-default}` and `${NAME:?message}` using environment variables and the `.env` file in the same folder: the first uses the value, the second uses `default` when unset, the third stops with `message` when unset.

## Walkthrough of this repository's use

```yaml
name: kafka-idempotent-consumer

services:
  kafka:
    image: apache/kafka:4.0.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1"]
      interval: 5s
      timeout: 10s
      retries: 20
```

(the file in two parts)

- `name: kafka-idempotent-consumer`: the project name, so container names do not depend on the folder name.
- Service `kafka`: image `apache/kafka:4.0.0`; `ports: "9092:9092"` publishes the client port; `environment:` holds the broker settings (`tech/apache-kafka.md` explains each); the health check runs `kafka-broker-api-versions.sh`, which succeeds only when the broker answers (`interval: 5s` between tries, `timeout: 10s` per try, `retries: 20` tolerated failures, `start_period: 10s` grace time).
- Service `postgres`: image `postgres:18`; the port line `"${POSTGRES_PORT:-5433}:5432"` uses the host port from `.env`, or 5433; the three `POSTGRES_*` variables create the user, password and database on first start. `${POSTGRES_PASSWORD:?copy .env.example to .env first}` has no default and stops the start with a message when missing, so no password is written in the file. The health check `pg_isready -U $${POSTGRES_USER} -d $${POSTGRES_DB}` uses `$$` so the shell inside the container reads the variable, not Compose.

There is no `volumes:` entry: both servers keep their data inside the container, and `make down` (`docker compose down -v`) removes it. That is wanted: each run starts clean.

## Try it yourself

1. `docker compose config` prints the file with the placeholders filled (after `make env` has created `.env`); it refuses to print if `POSTGRES_PASSWORD` is missing.
2. `make up`, then `docker compose ps`: both services `healthy`.
3. `docker compose exec -T postgres psql -U bank -d bank -c "select 1"` prints a one-row table with `1`.
4. `docker compose logs --tail=5 kafka` shows the broker's last lines.
5. `make down` removes both containers and the network.

## Mistakes people make

- Changing `9092:9092` but not `KAFKA_ADVERTISED_LISTENERS` (`tech/apache-kafka.md`).
- Writing a password into the compose file. Here it is `${POSTGRES_PASSWORD:?...}`.
- Using `$VAR` for the container's shell; Compose reads it first. Write `$$VAR`.
- Assuming a started container is ready. `--wait` and the health checks handle that.
- Typing `docker-compose` (the old v1 tool).

## Check yourself

1. What does `${X:-5433}` mean? *The value of `X`, or 5433 if unset.*
2. What does `${X:?msg}` do? *Stops Compose with `msg` if `X` is unset.*
3. Why `$$` in the health check? *So the container's shell expands it instead of Compose.*
4. What does `up -d --wait` do? *Starts in the background and returns when the health checks pass.*
5. Why no volume? *Data should disappear on `down -v`.*

## Further reading

- Compose file reference: https://docs.docker.com/reference/compose-file/
- Interpolation: https://docs.docker.com/reference/compose-file/interpolation/
- `docker compose up`: https://docs.docker.com/reference/cli/docker/compose/up/
