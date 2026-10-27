# GitHub Actions

## What it is

GitHub Actions is GitHub's built-in automation service. A **workflow** is a YAML file in `.github/workflows/`. GitHub starts it on events such as a push. A workflow has **jobs**; each job runs on a fresh virtual machine (a **runner**) and is a list of **steps**. This is **continuous integration** (CI): every change is checked automatically.

## Why this repository uses it

The template says CI must run format, lint, type check, tests and the dependency audit on every push. The workflow repeats exactly the `make` commands you run locally, so a green CI means the same thing as a green laptop. (It cannot run until the repository is pushed to GitHub; the author checked the YAML and ran the same `make` commands locally.)

## Where it appears

| Path | What it does |
| --- | --- |
| `.github/workflows/ci.yml` | Two jobs: `check` and `integration`. |

## How it works underneath

On `push` or `pull_request`, GitHub reserves an Ubuntu virtual machine for each job, checks the repository out onto it, runs the steps in order and stops a job at the first failing step. Jobs run in parallel unless one declares it needs another. A job's `services:` are extra Docker containers started before the steps and reachable on `localhost`; GitHub waits for their health check.

## Walkthrough of this repository's use

The file `.github/workflows/ci.yml` is read below in full, line by line. First the words:

- A **workflow** is one automation described in one YAML file under `.github/workflows/`. GitHub starts it when something happens (an **event**).
- A **job** is a group of steps that runs on one fresh machine. Jobs in one workflow run at the same time unless one says it must wait for another.
- A **runner** is the machine a job runs on. GitHub provides a fresh virtual machine for each job; it is thrown away when the job ends, so nothing from a previous run is left over.
- A **step** is one thing the job does: either a **action** (reusable code someone published, named with `uses:`) or a shell command (`run:`).
- A **service container** is a Docker container that GitHub starts next to the job, for the job to talk to (here a Kafka broker and a PostgreSQL server).

### Name and trigger

```yaml
name: ci

on: [push, pull_request]
```

- Line 1: `name: ci` is the label shown in GitHub's list of runs.
- Line 3: `on: [push, pull_request]` lists the events that start the workflow: every `push` (someone uploads commits to the repository) and every `pull_request` (someone proposes a change). The square brackets make a YAML list. See `tech/yaml.md` for YAML.

### Job 1: `check`

```yaml
jobs:
  check:
    name: Format, lint, types, import rules, unit tests with coverage, dependency audit
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: make check
      - run: make audit
```

- Line 5: `jobs:` begins the table of jobs. Line 6: this job's identifier, `check`. Line 7: `name:` is the longer text shown in the interface.
- Line 8: `runs-on: ubuntu-latest` chooses a virtual machine running the newest Ubuntu Linux that GitHub offers.
- Line 9: `steps:` begins the list; each item starts with `-`.
- Line 10: `uses: actions/checkout@v4` runs the published action `checkout`, version 4, which copies this repository's files onto the runner. Without it the machine would have an empty folder.
- Lines 11 to 13: `uses: actions/setup-python@v5` installs Python; `with:` passes it inputs, here `python-version: "3.12"`. The version is in quotes because `3.12` unquoted would be read as a number, and the trailing zero of something like `3.10` would be lost.
- Line 14: `run: make check` runs the shell command. It builds the virtual environment from the lock file and then runs the formatter check, the linter, mypy, the import rules, and the unit tests with the 95% coverage minimum.
- Line 15: `run: make audit` runs `pip-audit` on the locked packages. A step fails the job when its command exits non-zero, and a failed step stops the later ones.

### Job 2: `integration`

```yaml
  integration:
    name: Integration and end-to-end tests (Kafka 4 and PostgreSQL service containers)
    runs-on: ubuntu-latest
    services:
      kafka:
        image: apache/kafka:4.0.0
        ports:
          - 9092:9092
```

- Lines 17 to 19: the identifier, the name and the runner, as above. This job does not say `needs:`, so it runs at the same time as `check`.
- Line 20: `services:` starts the list of service containers. Line 21: `kafka:` names this one.
- Line 22: `image: apache/kafka:4.0.0` is the Docker image to start: the official Apache Kafka, version 4.0.0.
- Lines 23 and 24: `ports: - 9092:9092` connects port 9092 of the runner machine to port 9092 inside the container (`host:container`). Because of this, the tests on the runner can connect to `localhost:9092`.

```yaml
        env:
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

`env:` sets **environment variables** inside the container. The Kafka image reads variables that start with `KAFKA_` and turns each into a broker setting (`KAFKA_NODE_ID` becomes `node.id`, `KAFKA_PROCESS_ROLES` becomes `process.roles`). They are the same as in `docker-compose.yml`; each is explained in `tech/docker-compose.md`, and in short:

- `KAFKA_NODE_ID: 1` names this server's id in the cluster.
- `KAFKA_PROCESS_ROLES: broker,controller`: one process does both jobs, serving clients (broker) and managing cluster metadata (controller).
- `KAFKA_LISTENERS`: the ports it listens on: 9092 for clients, 9093 for controller traffic.
- `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092`: the address it tells clients to use afterwards. Here it is `localhost:9092`, which works because the tests run on the same machine as the published port.
- `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`, `KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093`, `KAFKA_CONTROLLER_LISTENER_NAMES`: how each listener is secured (here: not at all, `PLAINTEXT`), who votes for the controller (node 1 at port 9093), and which listener is the controller's.
- `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR`, `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR`, `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR`: set to 1 because there is one broker and Kafka's own internal topics cannot have more copies than brokers.
- `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0`: do not wait three seconds for more group members before the first rebalance, so tests start faster.

```yaml
        options: >-
          --health-cmd "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092"
          --health-interval 5s
          --health-timeout 10s
          --health-retries 20
          --health-start-period 10s
```

- Lines 37 to 42: `options: >-` passes extra command-line options to `docker create`. The `>-` is a YAML **folded block scalar**: the indented lines are joined into one line with spaces and no trailing newline. The options set a **health check**: Docker runs `kafka-broker-api-versions.sh --bootstrap-server localhost:9092` inside the container (it succeeds only when the broker answers) every 5 seconds (`--health-interval`), allowing 10 seconds per try (`--health-timeout`), up to 20 failed tries (`--health-retries`) and ignoring failures during the first 10 seconds (`--health-start-period`). GitHub waits until the service is healthy before it starts the job's steps.

```yaml
      postgres:
        image: postgres:18
        ports:
          - 5433:5432
        env:
          POSTGRES_USER: bank
          POSTGRES_PASSWORD: ci-only-throwaway
          POSTGRES_DB: bank
        options: >-
          --health-cmd "pg_isready -U bank -d bank"
          --health-interval 3s
          --health-timeout 5s
          --health-retries 20
```

- Lines 43 to 46: the second service container, `postgres`, from the image `postgres:18`. `5433:5432` publishes the container's port 5432 on the runner's port 5433, which is where the settings expect it.
- Lines 47 to 50: its environment: the user and database `bank` and the password `ci-only-throwaway`. This password is a throwaway value for a database that exists only for the minutes of the run and is deleted with the runner; it protects nothing. It is not a secret and nothing else uses it.
- Lines 51 to 55: its health check, `pg_isready -U bank -d bank` (PostgreSQL's own readiness tool), every 3 seconds, 5 seconds per try, 20 retries.

```yaml
    env:
      POSTGRES_PASSWORD: ci-only-throwaway
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: make test-integration test-e2e
```

- Lines 56 and 57: `env:` at the job level sets an environment variable for every step: `POSTGRES_PASSWORD`. The tests read the database password from the environment only (the code has no default), and this line supplies it. All other settings use their defaults, which match the ports published above.
- Lines 58 to 62: the checkout and Python steps, as in the first job.
- Line 63: `run: make test-integration test-e2e` runs the adapter tests and then the end-to-end tests against both service containers.

### What this workflow does not do

It does not publish anything or log in anywhere, so it needs no repository secrets; the only password in the file is the throwaway one for the temporary database. A failed run shows a red mark on the commit or pull request.

## Try it yourself

1. Read the file: `sed -n 1,20p .github/workflows/ci.yml` shows the `check` job.
2. Run exactly what job `check` runs: `make check && make audit`. If it passes here, it passes in CI (apart from machine differences).
3. Run what job `integration` runs: `make up && make test-integration test-e2e && make down`.

## Mistakes people make

- Putting a secret in the workflow file. This repository needs none.
- Running different commands in CI than locally. Here CI calls `make`.
- Forgetting that CI starts empty: no `.venv`, no running Kafka. `make` creates the environment; the `services:` block provides Kafka.
- Using an unpinned action (`@main`). Here the actions are pinned to major versions.

## Check yourself

1. What is a job? *A group of steps run on one fresh machine.*
2. What does `uses:` do? *Runs a reusable action.*
3. What is `services:` for? *Starts containers (like Kafka) before the steps.*
4. Why does CI call `make` targets? *So CI and local runs execute identical commands.*
5. When does the workflow run? *On every push and pull request.*

## Further reading

- Workflow syntax: https://docs.github.com/en/actions/using-workflows/workflow-syntax-for-github-actions
- Service containers: https://docs.github.com/en/actions/using-containerized-services/about-service-containers
- `actions/checkout` v4: https://github.com/actions/checkout/tree/v4 and `actions/setup-python` v5: https://github.com/actions/setup-python/tree/v5
