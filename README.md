# kafka-idempotent-consumer

A consumer that reads payment events from Kafka and keeps an account-balance table in PostgreSQL correct even though Kafka delivers at least once: it records each event id in the same database transaction as the balance update, survives crashes, parks poison messages in a dead-letter topic and shows what a slow poll loop does to a consumer group.

| | |
| --- | --- |
| **Lesson** | `/prep/kafka/project-idempotent-consumer`, "Build a consumer that survives crashes and poison" (`data-structure-explainer/src/content/tracks/kafka/15-project-idempotent-consumer.md` in the Plainround app) |
| **Planned hours** | 10 |
| **Level** | Intermediate |
| **Architecture** | [ARCHITECTURE.md](ARCHITECTURE.md), runbook: [docs/RUNBOOK.md](docs/RUNBOOK.md), decisions: [docs/adr](docs/adr) |

## Prerequisites

| Tool | Version | Used for |
| --- | --- | --- |
| Docker with Compose v2 | 24 or newer (tested with 24.0.6 and Compose v2.23) | Kafka and PostgreSQL |
| Python | 3.12 or 3.13 (tested with 3.13; the CI uses 3.12) | the programs and tests |
| GNU Make | 3.81 or newer | every command below |
| Kafka image | `apache/kafka:4.0.0`, KRaft, single node | pulled on first `make up` |
| PostgreSQL image | `postgres:18` | pulled on first `make up` |
| Python packages | `kafka-python` 3.0.12 and `psycopg[binary]` 3.2 or newer at run time (the lesson shows `psycopg2`; version 3 has an explicit `transaction()` block); pytest, pytest-cov, ruff, mypy, import-linter, pip-audit for development. All pinned with hashes in `requirements.lock` and `requirements-dev.lock`; `make` installs them into `.venv/` | everything |

No cloud account. The database password is read from the environment, never from code: `make up` copies `.env.example` to `.env` the first time (a throwaway value for a database that lives only on this machine while the container runs; change it if you share the machine) and docker compose and the programs both read it. Ports 9092 (Kafka) and 5433 (PostgreSQL; set `POSTGRES_PORT` in `.env` to change) must be free.

## Setup, run, test, stop

```bash
make up                  # Kafka + PostgreSQL, the topics, and sql/001_schema.sql + sql/002_seed.sql
make run                 # the happy path: 10,000 payments, idempotent consumer, balance check, queries
make demo-baseline       # step 1: naive consumer, no failures, balances match
make demo-crash          # step 2: naive consumer crashes between DB commit and offset commit, a balance is wrong
make crash-test          # step 3: 20 crash-and-restart runs with the idempotent consumer (about 20 minutes)
make demo-poison         # step 4: a malformed event goes to the dead-letter topic, the consumer keeps going
make demo-evict          # step 5: slow batches get the consumer evicted; smaller batches do not
make queries             # sql/003_queries.sql: balances, applied events, duplicates (none), progress
make health              # exit 0 when Kafka has the topics and PostgreSQL has the balances table
make check               # format check, lint, types, import rules, unit tests with coverage (no services needed)
make test-integration    # the adapters against the running services (needs make up first)
make test-e2e            # the lesson's scenarios through the command-line programs (needs make up; about 2.5 minutes)
make audit               # known-vulnerability check of the locked dependencies
make down                # stops both containers and deletes their data
```

`make help` lists every target. `make run`, `demo-baseline`, `demo-crash`, `crash-test` and `demo-evict` start with `make data`, which recreates the topics, zeroes the balances and produces fresh payments, so each one is independent. `PAYMENTS=2000 make crash-test RUNS=3` is a quick variant. Operational log lines (structured `logfmt`) go to standard error; results go to standard output.

## The SQL

Every statement the project runs against PostgreSQL is a file under [sql/](sql/), run in this order:

| File | Kind | What it holds | Run by |
| --- | --- | --- | --- |
| [001_schema.sql](sql/001_schema.sql) | DDL | `balances` (account id, `numeric(14,2)` total, updated time) | `make up` (`make db`) |
| [002_seed.sql](sql/002_seed.sql) | DML | the 50 accounts, each at zero | `make up` (`make db`) |
| [migrations/001_processed_events.sql](sql/migrations/001_processed_events.sql) | DDL (the lesson's step-3 change) | `processed_events`, primary key `event_id` | `make migrate` (also by `make run`, `crash-test`, `demo-poison`, `demo-evict`) |
| [003_queries.sql](sql/003_queries.sql) | queries | balances, grand total, applied-event count, duplicate check, progress per partition, top accounts | `make queries` |
| [reset.sql](sql/reset.sql) | DML (outside the run order) | zero every balance, empty `processed_events` | `make reset-db` and `make data`; the integration tests |

The migration is separate on purpose: the naive consumer of steps 1 and 2 does not need the table, and adding it is the change step 3 teaches.

## Done when: the lesson's checklist and the proof for each line

| # | Done when (from the lesson) | Command | What proves it |
| --- | --- | --- | --- |
| 1 | With no failures, the balance table matches the expected totals exactly. | `make demo-baseline` | `[balance-updater] done: {'applied': 10000, 'duplicate': 0, ...}` then `all 50 balances match expected totals exactly`. The checker compares every account with `expected_totals.json`, which the producer computed from the events alone. |
| 2 | After one crash, at least one account's balance is wrong by exactly the amounts that were reprocessed. | `make demo-crash` | `kill switch: crashing after the database commit of record 4731, before its offset commit`, `crash exit code: 1`, then after the restart `MISMATCH acct-041: expected 48763.88, got 49095.92 (off by +332.04)`: exactly one account, off by exactly one payment's amount. The restart's `done:` line shows 5,270 applied, one more than the 5,269 left, because the crashed record came back. |
| 3 | 20 runs with a crash at a random point all end with correct balances. | `make crash-test` | Twenty lines `run N/20 (kill at K): crashed, restarted, all 50 balances match expected totals exactly`, each at a different record number (the points are fixed and spread over the topic, so the run is repeatable), exit code 0. Each run uses a fresh consumer group, zeroed tables, a crash exit code of 1 and a clean restart. |
| 4 | The consumer keeps processing after the bad event, and the dead-letter topic holds it with enough context to replay it. | `make demo-poison` | `level=WARNING ... msg=dead-lettered topic=payments partition=1 offset=1058 error="could not convert amount to a number: 'not-a-number'"`; `done: {'applied': 10000, 'duplicate': 0, 'dead-lettered': 1, ...}`; balances match; the dead-letter record has `originalTopic`, `originalPartition`, `originalOffset`, `error`, `raw` and `rawBase64`; `LAG` is 0 on every partition of `payments`, so the offset moved past the bad record. |
| 5 | You have seen the eviction caused by `max.poll.interval.ms` in your logs, and lowering `max.poll.records` or batching the writes stops it. | `make demo-evict` | With `--max-poll-records 500` and 100 ms per record: `Consumer poll timeout has expired. This means the time between subsequent calls to poll() was longer than the configured max_poll_interval_ms` followed by `EVICTED: offset commit rejected; rejoining on the next poll error=CommitFailedError`, repeating about every 12 seconds. With `--max-poll-records 50`: no such line and `'evictions': 0`. (`max.poll.interval.ms` is set to 10 seconds for the demo so it shows in seconds; the default is 5 minutes.) Lowering `max.poll.records` is built; batching the writes is described in ARCHITECTURE.md but not built. |
| 6 | The runbook entry fits on one page and each signal is something you can see in your metrics or logs. | read [docs/RUNBOOK.md](docs/RUNBOOK.md) | One table, four rows in incident order (lag, dead-letter count, balance check, process death), each row with a signal that this project prints or can query. |

The same scenarios (crash, idempotent crash, poison, eviction) run as automated end-to-end tests in [tests/e2e](tests/e2e), and the adapters are tested in [tests/integration](tests/integration) (the two ledgers against PostgreSQL, the Kafka consumer, publisher and dead-letter sink, the health probes).

## Stretch goals

| Stretch (from the lesson) | Built? |
| --- | --- |
| Store the consumer's offsets in the same database transaction as the balances and skip the dedupe table; compare the two designs. | No. The comparison is written up in ARCHITECTURE.md, "How to extend it". |
| Add a replay tool that reads the dead-letter topic and republishes fixed events. | No. The dead-letter record carries everything a replay needs (`raw`, `rawBase64`, origin); the tool itself is not written. |
| Run the consumer with `group.protocol=consumer` and compare rebalance behavior. | No. The pinned `kafka-python` 3.0.12 implements the classic group protocol only. |

## What is simplified for teaching

Only what the lesson scopes out, plus where this build differs from the lesson's text:

- Single-node Kafka (replication factor 1), one PostgreSQL container, plain-text listeners (the lesson's setup).
- Offsets are committed after every record by default (the lesson's shape); real consumers commit per batch. `--commit-every N` shows the batched form, and `make crash-test` uses 50 so a crash redelivers up to 50 applied events.
- The lesson's producer sends float amounts and computes expected totals with floats. This one generates amounts in whole cents, sends two-decimal numbers, and computes the expected totals with `Decimal`, so the comparison is exact.
- A malformed event is inserted into the middle of the payments (`--poison`), so the consumer has valid payments left to process after it, which is what "keeps processing" needs to prove.
- The lesson's step-5 settings take 50 seconds to evict with the default five-minute poll interval; the demo sets the interval to 10 seconds.
- `make crash-test` takes about a minute per run: each run replays 10,000 records through a real database transaction, and after a crash the group coordinator waits for the dead member's session (10 seconds) to expire before it hands the partitions to the restarted consumer.
- No `Dockerfile` is needed to run the lesson, since the programs run from the host's virtual environment; the `Dockerfile` here packages them (non-root, pinned base image, the health command as its check) and is built by `make docker-build`.

## Project structure

```text
src/idempotent_consumer/
  domain/            payments and their validation, dead letters, balance comparison, kill points,
                     named errors. Depends on nothing outside the standard library.
  application/       use cases (produce payments, consume payments, check balances, crash test, health) and the ports
                     they need (PaymentSource, OffsetCommitter, DeadLetterSink, BalanceLedger, ...).
                     Depends on domain only.
  infrastructure/    adapters: kafka-python consumer, publisher and dead-letter sink; psycopg
                     connection and the two ledgers; settings (environment, validated), logging,
                     retry with backoff, signals, the subprocess runner. Depends on application and domain.
  interfaces/        command-line programs: produce, consume, check, crash test, health.
                     Wire adapters into use cases and own exit codes. Nothing depends on it.
sql/                 001_schema, 002_seed, 003_queries, reset, migrations/001_processed_events.
tests/
  unit/              domain, application (with in-memory fakes), infrastructure helpers, CLI arguments. No services.
  integration/       the ledgers against PostgreSQL; the Kafka adapters against the broker.
  e2e/               the lesson's scenarios through the command-line programs.
docs/                RUNBOOK.md (the step-6 runbook entry) and adr/ (decision records: event id in the same
                     transaction, ports and adapters, the libraries).
docker-compose.yml   Kafka 4 (KRaft, one node) and PostgreSQL 18.      Dockerfile  the programs as an image.
pyproject.toml       project metadata and every tool's configuration (ruff, mypy, pytest, import-linter).
requirements*.lock   hash-pinned dependencies (runtime, and runtime plus development tools).
Makefile             the only commands a reader needs (make help).
```

The dependency rule (domain depends on nothing; application on domain; infrastructure and interfaces on both; nothing on interfaces) is enforced by `make lint` through import-linter. A second contract forbids `domain` and `application` from importing `kafka` or `psycopg`.

## Author and license

Written by Sumit Srivastava. Released under the MIT License, see [LICENSE](LICENSE). Changes are in [CHANGELOG.md](CHANGELOG.md).
