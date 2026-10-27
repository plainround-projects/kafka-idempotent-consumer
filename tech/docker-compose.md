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

The whole `docker-compose.yml` is read below. First the ideas:

- A **container** is a running program packaged with everything it needs and isolated from the rest of the machine: it has its own files, its own network address and its own process list. It is lighter than a virtual machine because it shares the host's operating-system kernel.
- An **image** is the read-only package a container starts from. `apache/kafka:4.0.0` means the image named `kafka` published by the account `apache`, at version (tag) `4.0.0`.
- A **port** is a numbered door on a network address. A program listens on a port and clients connect to it. **Publishing** a port connects a port of the host to a port inside the container.
- **Docker Compose** reads a YAML file describing one or several containers (services) and starts them together.
- An **environment variable** is a named piece of text a program receives when it starts.

### The header

```yaml
# Single-node Kafka 4 (KRaft) and PostgreSQL, both for local use only.
name: kafka-idempotent-consumer

services:
  kafka:
    image: apache/kafka:4.0.0
    ports:
      - "9092:9092"
```

- Line 1: a comment. KRaft is Kafka's built-in way to agree on cluster metadata, replacing the older ZooKeeper program; "for local use only" means no security is configured.
- Line 2: `name: kafka-idempotent-consumer` is the **project name**; Compose puts it in the names of the containers and networks it creates.
- Line 4: `services:` begins the list of containers. Line 5: the first is named `kafka`; `docker compose exec kafka` uses that name.
- Line 6: the image. Lines 7 and 8: `ports: - "9092:9092"` is `host:container`: a request to port 9092 on this computer reaches port 9092 in the container.

### Kafka's settings

```yaml
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
```

`environment:` sets environment variables in the container. The Kafka image turns each `KAFKA_*` variable into a broker setting.

- `KAFKA_NODE_ID: 1`: this server's number (`node.id`), unique in a cluster.
- `KAFKA_PROCESS_ROLES: broker,controller`: this one process plays both roles. A **broker** stores partitions and answers producers and consumers. A **controller** keeps the cluster's records of who leads which partition.
- `KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093`: a **listener** is a named address the broker listens on, `NAME://host:port`; an empty host means every network address of the container. Clients use 9092; controller traffic uses 9093.
- `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092`: a client first connects to any address it knows (the **bootstrap** address); the broker answers with the addresses to use from then on. This is the address it gives out: `localhost:9092`, because the Python programs and the Kafka tools run on this machine and reach the broker there.
- `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT`: the security of each listener. `PLAINTEXT` means no encryption and no login: right for a laptop demo, wrong for anything else.
- `KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093`: the controllers that vote, as `id@host:port`. A **quorum** is the group whose majority decides; here it is node 1 alone.
- `KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER`: which listener carries controller traffic.
- `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR`, `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR`, `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR`: all `1`. Kafka keeps each group's committed offsets in an internal topic called `__consumer_offsets` and the state of transactions in another. Their default replication factor is 3, which one broker cannot satisfy, so they are set to 1.
- `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0`: by default the broker waits 3 seconds after the first member joins a group, hoping more will join, before handing out partitions. Zero starts at once, so the crash test restarts quickly.

### Kafka's health check

```yaml
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1"]
      interval: 5s
      timeout: 10s
      retries: 20
      start_period: 10s
```

A **health check** is a command Docker runs repeatedly inside the container to decide whether the service works.

- `test: ["CMD-SHELL", "..."]`: the first word says "run the next string through the container's shell". `kafka-broker-api-versions.sh --bootstrap-server localhost:9092` asks the broker which API versions it supports; it succeeds only when the broker is up. `> /dev/null 2>&1` throws the output away.
- `interval: 5s`: run it every 5 seconds. `timeout: 10s`: allow 10 seconds per run. `retries: 20`: after 20 failures in a row the container is marked `unhealthy`. `start_period: 10s`: failures in the first 10 seconds do not count.
- `docker compose up -d --wait` returns only when every service's check passes.

### PostgreSQL

```yaml
  postgres:
    image: postgres:18
    ports:
      - "${POSTGRES_PORT:-5433}:5432"
    environment:
      # Credentials come from .env (make up copies .env.example the first time); none is written here.
      POSTGRES_USER: ${POSTGRES_USER:-bank}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:?copy .env.example to .env first}
      POSTGRES_DB: ${POSTGRES_DB:-bank}
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U $${POSTGRES_USER} -d $${POSTGRES_DB}"]
      interval: 3s
      timeout: 5s
      retries: 20
```

- Line 28: the second service is called `postgres` (the name `docker compose exec postgres` uses). Line 29: `image: postgres:18` is the official PostgreSQL image, version 18.
- Lines 30 and 31: `"${POSTGRES_PORT:-5433}:5432"`. The `${NAME:-default}` form is **variable substitution**: Compose replaces it with the value of `POSTGRES_PORT` from the environment or from the `.env` file next to the compose file, or `5433` when it is unset. So the host's port is 5433 (not the usual 5432, so it does not clash with a PostgreSQL already on the machine) and the container's is 5432.
- Lines 33 to 36: the comment says credentials come from `.env` and none is written in this file. `POSTGRES_USER: ${POSTGRES_USER:-bank}` and `POSTGRES_DB: ${POSTGRES_DB:-bank}` default to `bank`. `POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:?copy .env.example to .env first}` uses the `${NAME:?message}` form: if the variable is unset or empty, Compose **stops with that message** and starts nothing. So no default password can ever exist in this repository, and a forgotten `.env` gives a clear instruction. The image reads the three variables on its first start to create the user, the password and the database.
- Lines 37 to 41: the health check. `pg_isready -U $${POSTGRES_USER} -d $${POSTGRES_DB}` is PostgreSQL's own readiness tool. The doubled dollar signs matter: Compose reads a single `$` as its own substitution, so `$$` is how you pass a literal `$` through, and the container's shell then expands `${POSTGRES_USER}` from the container's own environment. It is checked every 3 seconds, 5 seconds allowed per try, up to 20 failures.

### Starting and stopping

`make up` runs `docker compose up -d --wait` and then creates the topics and the tables. `make down` runs `docker compose down -v`: stop and remove the containers and any volumes they used. The file declares no volumes of its own, so nothing is kept between a `make down` and the next `make up`.

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
