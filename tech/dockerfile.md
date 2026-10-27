# Dockerfile syntax

## What it is

A `Dockerfile` is a text file of instructions that tells Docker how to build an **image** (a read-only package holding a program and everything it needs). Each instruction adds a **layer** on top of the one before. When you rebuild, Docker reuses layers whose inputs did not change, so order matters.

## Why this repository uses it

The programs here normally run from your own Python virtual environment. The Dockerfile packages them as an image with a built-in health check, so the same programs could run in a container next to Kafka and PostgreSQL. It also shows the production habits the template asks for: a pinned base image, a non-root user, and installing only locked, hash-checked dependencies.

## Where it appears

| Path | What it does |
| --- | --- |
| `Dockerfile` | The recipe. |
| `.dockerignore` | Lists files `docker build` must not send (the virtual environment, logs, tests, `.env`). |
| `Makefile` (`docker-build`) | Builds it as `kafka-idempotent-consumer:dev`. |

## How it works underneath

`docker build .` sends the folder (minus `.dockerignore` entries) to the engine, called the **build context**. The engine runs the instructions in order, each in a temporary container, and saves the result as a layer. `COPY` can only use files from the build context.

## Walkthrough of this repository's use

```dockerfile
# The command-line programs as an image. Nothing here serves traffic; the health command is the image's check.
# Run against Kafka and PostgreSQL the container can reach, for example:
#   docker run --rm --network host -e BOOTSTRAP_SERVERS=localhost:9092 kafka-idempotent-consumer:dev idempotent-consumer-produce --payments 10
FROM python:3.12.8-slim-bookworm@sha256:2199a62885a12290dc9c5be3ca0681d367576ab7bf037da120e564723292a2f0

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY requirements.lock ./
RUN pip install --require-hashes -r requirements.lock

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-deps .

RUN useradd --system --uid 10001 --no-create-home app
USER app

HEALTHCHECK --interval=30s --timeout=20s --retries=3 CMD ["idempotent-consumer-health"]
CMD ["idempotent-consumer-health"]
```

- `FROM python:3.12.8-slim-bookworm@sha256:...`: start from an image that already has Python 3.12.8 on a small Debian 12 ("bookworm") system. The part after `@` is a **digest**, a fingerprint of the exact bytes. Tags can be re-pointed; a digest cannot, so the build is repeatable.
- `ENV ...`: set environment variables for every later step and for the running container. `PYTHONDONTWRITEBYTECODE=1` stops Python writing `.pyc` cache files. `PYTHONUNBUFFERED=1` makes output appear immediately. `PIP_NO_CACHE_DIR=1` skips pip's download cache, which keeps the image small. `PIP_DISABLE_PIP_VERSION_CHECK=1` silences pip's "new version available" call.
- `WORKDIR /app`: later instructions run in `/app` (created if missing).
- `COPY requirements.lock ./` then `RUN pip install --require-hashes -r requirements.lock`: copy only the lock file and install from it first. Because this layer depends only on that file, Docker reuses it when only the source code changes. `--require-hashes` makes pip refuse any package whose fingerprint is not in the file.
- `COPY pyproject.toml README.md ./`, `COPY src ./src`, `RUN pip install --no-deps .`: copy the project and install it; `--no-deps` because the dependencies were installed already.
- `RUN useradd --system --uid 10001 --no-create-home app` and `USER app`: create an unprivileged user and run as it. If the program were compromised, it would not be root inside the container.
- `HEALTHCHECK ... CMD ["idempotent-consumer-health"]`: every 30 seconds Docker runs this command; exit code 0 means healthy. The square-bracket form is the **exec form**: the command runs directly, not through a shell.
- `CMD ["idempotent-consumer-health"]`: the default command when you run the image without one.

## Try it yourself

1. `make docker-build` ends with the image named `kafka-idempotent-consumer:dev` (the output ends with a hint about `docker scout`).
2. `docker run --rm kafka-idempotent-consumer:dev id -u` prints `10001`.
3. `docker run --rm -e BOOTSTRAP_SERVERS=not-valid kafka-idempotent-consumer:dev` prints `configuration error: BOOTSTRAP_SERVERS must be host:port[,host:port], got 'not-valid'` and exits with code 2.
4. `docker history kafka-idempotent-consumer:dev | head` lists the layers, one per instruction.

## Mistakes people make

- Copying the whole project before installing dependencies, which re-installs everything on each code change.
- Running as root.
- Using `latest` or a moving tag for the base image.
- Writing `CMD idempotent-consumer-health` (shell form) and then wondering why signals do not reach the program.

## Check yourself

1. What is a layer? *The saved result of one instruction; unchanged layers are reused.*
2. Why copy `requirements.lock` before the source? *So the slow install layer is reused when only code changes.*
3. What does a digest add to a tag? *An exact, unchangeable fingerprint of the image.*
4. What does `USER app` do? *Runs the rest of the build and the container as an unprivileged user.*
5. What does `HEALTHCHECK` do? *Runs a command regularly; exit code 0 marks the container healthy.*

## Further reading

- Dockerfile reference: https://docs.docker.com/reference/dockerfile/
- Base image used: https://hub.docker.com/_/python (tag `3.12.8-slim-bookworm`)
