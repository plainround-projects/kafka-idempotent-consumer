# Docker

## What it is

Docker runs programs inside **containers**. A container is a normal process started in a sealed-off box: it sees its own file system, its own network and its own process list, so a program runs the same way on every computer. A container starts from an **image**, a read-only package holding the program and everything it needs. On a Mac, Docker runs a small Linux virtual machine in the background and starts the containers inside it.

## Why this repository uses it

Kafka and PostgreSQL are servers you would otherwise have to install and configure. With Docker, `make up` pulls the official Kafka 4.0.0 and PostgreSQL 18 images and runs both. `make down` deletes the container and its data, so you start clean. A second, smaller use is `docker build`, which packages this project's own programs.

## Where it appears

| Path | What it does |
| --- | --- |
| `docker-compose.yml` | Describes the Kafka and PostgreSQL containers (see `tech/docker-compose.md`). |
| `Makefile` | `docker compose up/down`, `docker compose exec ...` to run Kafka's tools, and `docker build`. |
| `Dockerfile` | Recipe for an image of this project's programs (see `tech/dockerfile.md`). |
| `.dockerignore` | Files that must not be sent to `docker build`. |
| `Makefile` (`db`, `migrate`, `queries`) | `docker compose exec ... psql` runs SQL files inside the Postgres container. |

## How it works underneath

The Docker **client** (the `docker` command) sends your request to the Docker **engine** (a background service). The engine pulls images from a **registry** (a download site; here Docker Hub), creates containers and wires their network. A container's files vanish when the container is removed, unless you attach a **volume** (storage that outlives it). The compose file publishes a **port**: `"9092:9092"` forwards port 9092 of your computer to port 9092 inside the container. `docker compose exec` starts one more process inside a running container; that is how Kafka's command-line tools run without being installed on your Mac.

## Walkthrough of this repository's use

```make
up: env $(STAMP) ## start Kafka and PostgreSQL, create topics and the balances table
	docker compose up -d --wait
	$(MAKE) --no-print-directory topics db
```

- `docker compose up -d --wait`: create and start what `docker-compose.yml` describes. `-d` (detached) returns to your prompt. `--wait` returns only when the container's health check passes.
- `docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh ...` (through the `KAFKA_BIN` variable): run a program inside the `kafka` container; the `PSQL` variable does the same for `psql` in the `postgres` container. `-T` means "no terminal", which is needed when the output goes through a pipe or into a script.

```make
down: ## stop Kafka and PostgreSQL and delete their data
	docker compose down -v
```

`docker compose down -v` stops and removes the containers and the network, and `-v` also deletes volumes, so no data survives.

```make
docker-build: ## build the container image of the command-line programs
	docker build -t kafka-idempotent-consumer:dev .
```

`docker build -t kafka-idempotent-consumer:dev .` builds an image from the `Dockerfile` in the current folder (`.`) and names it (`-t`) `kafka-idempotent-consumer` with the label `dev`.

## Try it yourself

1. `docker --version` prints the client version (24.0.6 was used here).
2. `make up`, then `docker compose ps`: services `kafka` (ports `0.0.0.0:9092->9092/tcp`) and `postgres` (`0.0.0.0:5433->5432/tcp`), both `healthy`.
3. `docker compose exec -T kafka ls /opt/kafka/bin | head -5` lists Kafka's tools inside the container.
4. `make docker-build` then `docker run --rm kafka-idempotent-consumer:dev id -u` prints `10001`: the container does not run as root.
5. `make down` and `docker compose ps` shows nothing.

## Mistakes people make

- Forgetting that container data disappears. Here that is wanted: `make down` gives a clean start.
- A port already in use on your computer: another Kafka on 9092 or Postgres on 5433 stops this one from starting.
- Running `docker compose exec` without `-T` in a script, which fails with "the input device is not a TTY".
- Using the tag `latest` for images. This repository pins `apache/kafka:4.0.0`, `postgres:18` and a digest for the Python base image.

## Check yourself

1. What is the difference between an image and a container? *An image is the read-only package; a container is a running process started from it.*
2. What does `-d` do? *Runs the containers in the background.*
3. What does `ports: "9092:9092"` do? *Forwards your computer's port 9092 to the container's port 9092.*
4. How do Kafka's tools and `psql` run without being installed? *`docker compose exec` runs them inside the containers.*
5. What does `down -v` remove that `down` keeps? *The volumes (stored data).*

## Further reading

- Docker Engine 24 documentation: https://docs.docker.com/engine/
- `docker build`: https://docs.docker.com/reference/cli/docker/image/build/
- `docker compose exec`: https://docs.docker.com/reference/cli/docker/compose/exec/
