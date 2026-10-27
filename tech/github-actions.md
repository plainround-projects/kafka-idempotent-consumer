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

```yaml
name: ci

on: [push, pull_request]

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

- `name: ci` and `on: [push, pull_request]`: the workflow's name and its triggers.
- `jobs:` / `check:`: the first job. `runs-on: ubuntu-latest` picks the machine type.
- `uses: actions/checkout@v4`: run a ready-made step (an **action**) that downloads the repository. `@v4` pins its major version.
- `uses: actions/setup-python@v5` with `python-version: "3.12"`: installs Python 3.12. `with:` passes the action's inputs.
- `run: make check`: run a shell command. `make check` is lint plus unit tests with coverage. `make audit` checks the locked dependencies for known security problems.

```yaml
  integration:
    name: Integration and end-to-end tests (Kafka 4 and PostgreSQL service containers)
    runs-on: ubuntu-latest
    services:
      kafka:
        image: apache/kafka:4.0.0
        ports:
          - 9092:9092
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
        options: >-
          --health-cmd "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092"
          --health-interval 5s
          --health-timeout 10s
          --health-retries 20
          --health-start-period 10s
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
    env:
      POSTGRES_PASSWORD: ci-only-throwaway
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: make test-integration test-e2e
```

- `services:` starts two containers before the steps: `kafka` and `postgres`. `image`, `ports` and `env` mirror `docker-compose.yml` (`tech/docker-compose.md`). The values under `env:` are the broker's settings.
- `options: >-` passes Docker flags; the `--health-*` flags make GitHub wait until the broker answers (`tech/yaml.md` explains `>-`).
- `postgres:` is the database (`image: postgres:18`, host port 5433, a throwaway password for the CI machine only). The job-level `env: POSTGRES_PASSWORD: ci-only-throwaway` hands the same value to the tests, which read it from the environment like any other setting.
- The final step runs the integration and end-to-end tests against both services.

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
