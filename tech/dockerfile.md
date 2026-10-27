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

The whole `Dockerfile` and the whole `.dockerignore` are read below. Ideas first:

- An **image** is a read-only package holding a small operating system's files plus your program. A **container** is an image that is running.
- Building an image runs the instructions in order. Each instruction that changes files produces a **layer**, a saved set of file changes. Docker keeps layers in a **cache**: if an instruction and everything before it are unchanged, Docker reuses the saved layer instead of running it again. So instructions that rarely change go first and instructions that change often go last.
- A **tag** (`python:3.12.8-slim-bookworm`) is a movable label on an image. A **digest** (`@sha256:...`) is a fingerprint computed from the image's content; it cannot be moved.
- **root** is the all-powerful user on Linux. A program that runs as root inside a container is more dangerous if someone breaks into it than one running as an ordinary user.

### The comment and the base image

```dockerfile
# The command-line programs as an image. Nothing here serves traffic; the health command is the image's check.
# Run against Kafka and PostgreSQL the container can reach, for example:
#   docker run --rm --network host -e BOOTSTRAP_SERVERS=localhost:9092 kafka-idempotent-consumer:dev idempotent-consumer-produce --payments 10
FROM python:3.12.8-slim-bookworm@sha256:2199a62885a12290dc9c5be3ca0681d367576ab7bf037da120e564723292a2f0
```

- Lines 1 to 3: comments. They state that the image is not a server; its health command is the image's own check. Line 3 is an example of how to run it: `docker run --rm --network host -e BOOTSTRAP_SERVERS=localhost:9092 kafka-idempotent-consumer:dev idempotent-consumer-produce --payments 10`. In that command `--rm` deletes the container when it ends, `--network host` lets it use the host's network so `localhost:9092` is the real broker, `-e NAME=value` sets an environment variable, `kafka-idempotent-consumer:dev` is the image built by `make docker-build`, and everything after it replaces the default command, here `idempotent-consumer-produce --payments 10`. (The programs also need `POSTGRES_PASSWORD` and, unless the defaults apply, the other settings; pass each with `-e NAME=value`.)
- Line 4: `FROM python:3.12.8-slim-bookworm@sha256:...`. Every image starts `FROM` another. This one is the official Python image, version 3.12.8, built on "bookworm", which is the name of Debian Linux 12, in its "slim" form (fewer extra programs). The long `@sha256:` part pins the exact image content, so a build today and a build in a year use the same base even if the tag has been moved.

### Settings through the environment

```dockerfile
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1
```

`ENV` sets environment variables for the rest of the build and for the running container. A backslash at the end of a line continues the instruction on the next line.

- `PYTHONDONTWRITEBYTECODE=1`: Python normally saves a compiled copy of each module as a `.pyc` file; in a container that is wasted space.
- `PYTHONUNBUFFERED=1`: Python sends output at once instead of holding it in a buffer, so `docker logs` shows lines as they are written.
- `PIP_NO_CACHE_DIR=1`: pip keeps no download cache, which keeps the image small.
- `PIP_DISABLE_PIP_VERSION_CHECK=1`: pip does not call the internet to ask if a newer pip exists.

### Dependencies, then code

```dockerfile
WORKDIR /app
COPY requirements.lock ./
RUN pip install --require-hashes -r requirements.lock

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-deps .
```

- Line 11: `WORKDIR /app` sets the current folder for all later instructions and creates it.
- Line 12: `COPY requirements.lock ./` copies that one file from the build folder on your machine into `/app`.
- Line 13: `RUN pip install --require-hashes -r requirements.lock` runs a command during the build. It installs the runtime dependencies (`kafka-python` and `psycopg`) and refuses any file whose hash differs from the lock file. Because the layer depends only on `requirements.lock`, Docker reuses it from the cache whenever only the source code changed.
- Line 15: `COPY pyproject.toml README.md ./`. Both are needed to install the project: `pyproject.toml` describes it and names `README.md` as its readme.
- Line 16: `COPY src ./src` copies the code.
- Line 17: `RUN pip install --no-deps .` installs the project itself. `--no-deps` because the dependencies are already installed. This is where the commands `idempotent-consumer-produce`, `-consume`, `-check`, `-crash-test` and `-health` (from `[project.scripts]`) get created on the image's search path.

### Not running as root

```dockerfile
RUN useradd --system --uid 10001 --no-create-home app
USER app
```

- Line 19: `useradd --system --uid 10001 --no-create-home app` creates a **system user** called `app` with the fixed number 10001 and no home folder. A high fixed number is a convention: it will not match a real person's account on the host.
- Line 20: `USER app` makes every later instruction, and the container itself, run as that user.

### The health check and the default command

```dockerfile
HEALTHCHECK --interval=30s --timeout=20s --retries=3 CMD ["idempotent-consumer-health"]
CMD ["idempotent-consumer-health"]
```

- Line 22: `HEALTHCHECK --interval=30s --timeout=20s --retries=3 CMD ["idempotent-consumer-health"]`. Docker runs that command inside the container every 30 seconds. Exit code 0 means healthy; 1 means unhealthy; after three failures in a row the container is marked unhealthy. The health command checks that Kafka has the two topics and that PostgreSQL has the `balances` table, so the container needs the connection settings (including `POSTGRES_PASSWORD`) in its environment to report healthy. The square-bracket form is the **exec form**: the program is started directly, without a shell in between.
- Line 23: `CMD ["idempotent-consumer-health"]` is the command that runs if you start the image without giving one. A command given on the `docker run` line replaces it.

### `.dockerignore`

```text
.git
.venv
logs
tests
docs
**/__pycache__
.mypy_cache
.ruff_cache
.pytest_cache
.coverage
.env
tech
```

When you build, Docker sends the **build context** (the folder given at the end of the command, here `.`) to the Docker engine. `.dockerignore` lists what is left out of it, in the same style as `.gitignore`: `.git` (the history), `.venv`, `logs`, `tests`, `docs`, `tech`, the caches of Python and of each tool, `.coverage` and `.env`. `**/__pycache__` means a folder of that name at any depth. Leaving these out keeps the build fast and the image small, and `.env` is excluded so that a file holding local settings or secrets can never be copied into an image by accident.

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
