# GNU Make

## What it is

`make` is a command-line tool that runs named recipes from a file called `Makefile`. Each **target** (a name) lists its **prerequisites** (other targets or files it needs first) and a **recipe** (the shell commands that produce it). Make was built to rebuild files only when their inputs changed, but it is also a common way to give a project short, memorable commands.

This repository uses GNU Make 3.81 (the version Apple ships). Every feature below works in it.

## Why this repository uses it

The template says a reader should need only `make up`, `make run`, `make test`, `make lint`, `make down`. The Makefile hides the long commands (virtual environment, Docker, Kafka tools) behind those names.

## Where it appears

| Path | What it does |
| --- | --- |
| `Makefile` | Every command the README mentions. |
| `.github/workflows/ci.yml` | Runs `make check`, `make audit`, `make test-integration`, `make test-e2e`. |
| `README.md` | Lists the targets. |

## How it works underneath

You type `make up`. Make reads `Makefile`, finds the target `up`, first builds each prerequisite that is out of date, then runs the recipe lines one at a time, each in a fresh shell. If any line exits with a failure code, Make stops. A target that is a **file** is rebuilt only if a prerequisite file is newer; a **phony** target (not a real file) always runs.

## Walkthrough of this repository's use

The whole `Makefile` is read below, top to bottom, every line. First, three ideas.

- A **shell** is the program that reads a command line and starts the programs named in it. Make does not run commands itself; it hands each recipe line to a shell (`/bin/sh`).
- An **environment variable** is a named piece of text that a process receives from the process that started it. A Python program reads one with `os.environ["TOPIC"]`.
- A file's **modification time** is the moment it was last changed. Make compares these times to decide whether work is needed.

### The first line and the variables

```make
# make up | run | demo-baseline | demo-crash | crash-test | demo-poison | demo-evict | check | down   (make help lists all)
PYTHON ?= $(shell command -v python3.13 || command -v python3.12 || command -v python3)
VENV := .venv
PY := $(VENV)/bin/python
STAMP := $(VENV)/.installed
PAYMENTS ?= 10000
RUNS ?= 20
DEMO_KILL_AT ?= 4731
KAFKA_BIN := docker compose exec -T kafka /opt/kafka/bin
POSTGRES_USER ?= bank
POSTGRES_DB ?= bank
PSQL := docker compose exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -v ON_ERROR_STOP=1 -q
PRODUCE := $(PY) -m idempotent_consumer.interfaces.produce_cli
CONSUME := $(PY) -m idempotent_consumer.interfaces.consume_cli
CHECK := $(PY) -m idempotent_consumer.interfaces.check_cli
```

A line starting with `#` is a **comment**, ignored by Make. Line 1 lists the targets a reader should remember. A **variable** gives a name to a piece of text: set it with `NAME = text`, `NAME := text` or `NAME ?= text`, and use it as `$(NAME)`. `?=` sets it **only if it is not already set**, so a person can override it (`make PAYMENTS=500 data`). `:=` evaluates once, now.

- Line 2: `PYTHON ?= $(shell command -v python3.13 || command -v python3.12 || command -v python3)`. `$(shell ...)` runs a command and uses its output. `command -v NAME` prints the path of a program if the shell can find it. `||` means "if that failed, try the next". The result is the first of Python 3.13, 3.12 or plain `python3` that exists.
- Line 3: `VENV := .venv`, the folder of the virtual environment (an isolated folder of installed packages; see `tech/pip-venv-and-lock-files.md`). Line 4: `PY := $(VENV)/bin/python` is the Python inside it, used directly with no "activate". Line 5: `STAMP := $(VENV)/.installed`, an empty marker file that the install step creates.
- Lines 6 to 8: three numbers that can be overridden: how many payments (`PAYMENTS`, 10,000), how many crash-test runs (`RUNS`, 20), and the record at which the demo crash happens (`DEMO_KILL_AT`, 4731). The last name says "demo" on purpose: a plain name such as `KILL_AT` would be exported to the programs (see the `export` line) and could collide with a real setting.
- Line 9: `KAFKA_BIN := docker compose exec -T kafka /opt/kafka/bin`. `docker compose exec` runs a command inside the running container called `kafka`; `-T` means no terminal is attached (needed when output is piped or in CI); `/opt/kafka/bin` is where Kafka's command-line tools live inside the image. Recipes add a tool name, as in `$(KAFKA_BIN)/kafka-topics.sh`.
- Lines 10 and 11: the PostgreSQL user and database names, overridable.
- Line 12: `PSQL := docker compose exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -v ON_ERROR_STOP=1 -q`. `psql` is PostgreSQL's command-line client; here it runs inside the container called `postgres`. `-U` is the user, `-d` the database, `-v ON_ERROR_STOP=1` sets a psql variable that makes it stop at the first SQL error and exit with a failure code (so Make notices), and `-q` is quiet (no banner for every statement).
- Lines 13 to 15: the Python commands for the producer, the consumer and the balance checker.

```make
-include .env
export

.PHONY: help install env up topics db migrate queries reset-db reset-topics data run demo-baseline demo-crash \
        crash-test demo-poison demo-evict health lint format test test-integration test-e2e audit check \
        docker-build down clean
```

- Line 17: `-include .env` reads the file `.env` as part of the Makefile if it exists. Its `NAME=value` lines become Make variables. The leading `-` means "no error if the file is missing". See `tech/dotenv.md`.
- Line 18: `export` on a line by itself puts **every** Make variable into the environment of the commands Make starts. That is how settings from `.env`, including the database password, reach the Python programs.
- Lines 20 to 22: `.PHONY` lists the targets that are **names for actions**, not files. Without it, a file called `test` in the folder would make `make test` say "nothing to do". The backslashes continue the list over lines.

### `help`

```make
help: ## list the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | sed -E 's/:.*## /\t/' | sort
```

`help: ## list the targets`: a target with no prerequisites. The recipe: `@` hides the command itself. `grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST)` finds lines that start with a lowercase name, a colon, anything, then `## `. `$(MAKEFILE_LIST)` is the built-in list of Makefiles read (here `Makefile`). `| sed -E 's/:.*## /\t/'` turns each into `name<TAB>description`. `| sort` puts them in order. That is why most targets end in `## description`; the ones with none (helpers such as `env`, `topics`, `reset-db`, `reset-topics`, `data`, `clean`) stay out of the list.

### The virtual environment (a file target)

```make
$(STAMP): requirements-dev.lock pyproject.toml
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install -q --require-hashes -r requirements-dev.lock
	$(VENV)/bin/pip install -q --no-deps -e .
	touch $@

install: $(STAMP) ## create .venv from the lock file
```

- Line 27: `$(STAMP): requirements-dev.lock pyproject.toml`. This target **is a file**, `.venv/.installed`, with two **prerequisites**. Make's rule: if the target is missing or any prerequisite is newer than it, run the recipe.
- Line 28: `$(PYTHON) -m venv $(VENV)` creates the environment folder.
- Line 29: `pip install -q --require-hashes -r requirements-dev.lock` installs every locked package. `-q` is quiet; `--require-hashes` makes pip refuse any file whose fingerprint (hash) is not in the lock file; `-r FILE` reads requirements from a file.
- Line 30: `pip install -q --no-deps -e .` installs this project in editable mode (`-e`: the installed copy points at `src/`, so code edits need no reinstall) without dependencies (`--no-deps`, since line 29 did them).
- Line 31: `touch $@`. `$@` means "the target being built". `touch` creates an empty file or updates its time: the stamp is the record that the install finished.
- Line 33: `install: $(STAMP) ## ...` is a phony target whose only prerequisite is the stamp.

### Creating `.env` once

```make
env:
	@[ -f .env ] || { cp .env.example .env; echo "created .env from .env.example"; }
```

`env` has no prerequisites. The recipe `@[ -f .env ] || { cp .env.example .env; echo "created .env from .env.example"; }` is shell: `[ -f .env ]` is true if the file `.env` exists. `||` runs what follows only if the test was false. The braces `{ ...; }` group two commands: copy the template to `.env` (`cp`) and print a message. So the first `make up` creates a local `.env` from the template and every later one leaves it alone. The copy is a convenience for a demo; `.env` is listed in `.gitignore` and `.dockerignore`, so a changed copy never gets committed or baked into an image.

### Starting everything

```make
# Start Kafka and PostgreSQL, create the topics and the balances table (DDL + seed).
up: env $(STAMP) ## start Kafka and PostgreSQL, create topics and the balances table
	docker compose up -d --wait
	$(MAKE) --no-print-directory topics db

# payments: six partitions, keyed by accountId. payments-dlq: one partition, kept 30 days for investigation.
topics:
	$(KAFKA_BIN)/kafka-topics.sh --create --if-not-exists --topic payments --partitions 6 \
	  --replication-factor 1 --bootstrap-server localhost:9092
	$(KAFKA_BIN)/kafka-topics.sh --create --if-not-exists --topic payments-dlq --partitions 1 \
	  --replication-factor 1 --config retention.ms=2592000000 --bootstrap-server localhost:9092
```

- Line 39: `up: env $(STAMP) ## ...`: the prerequisites are built left to right, so `.env` exists and the environment is installed before anything starts.
- Line 40: `docker compose up -d --wait` starts the services in `docker-compose.yml` (Kafka and PostgreSQL): `-d` runs them in the background, `--wait` returns only when their health checks pass. See `tech/docker-compose.md`.
- Line 41: `$(MAKE) --no-print-directory topics db` runs Make again for two targets, `topics` and `db`, in order. `$(MAKE)` is the Make program currently running, so flags carry over.
- Lines 43 to 48: the `topics` target (a comment explains the design). Each long command is split with a trailing `\`. `kafka-topics.sh --create --if-not-exists --topic payments --partitions 6 --replication-factor 1 --bootstrap-server localhost:9092` makes the payments topic with six partitions and one copy of each; `--if-not-exists` makes it safe to repeat. The second command makes `payments-dlq` with one partition and `--config retention.ms=2592000000`, which keeps records for 2,592,000,000 ms, thirty days, so a parked record is still there when someone investigates.

### The database

```make
db: ## apply sql/001_schema.sql and sql/002_seed.sql
	$(PSQL) < sql/001_schema.sql
	$(PSQL) < sql/002_seed.sql

# Lesson step 3's schema change: the processed_events table.
migrate: ## apply sql/migrations/*.sql (adds processed_events)
	@for file in sql/migrations/*.sql; do echo "applying $$file"; $(PSQL) < $$file; done

queries: ## run sql/003_queries.sql
	docker compose exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -v ON_ERROR_STOP=1 < sql/003_queries.sql

reset-db:
	$(PSQL) < sql/reset.sql
```

- Lines 50 to 52: `db` runs two SQL scripts. `$(PSQL) < sql/001_schema.sql` starts `psql` inside the container and feeds the script file to its standard input with `<`. The first creates the `balances` table; the second (`002_seed.sql`) inserts the 50 accounts at zero. See `tech/sql.md`.
- Lines 54 to 56: a comment, then `migrate`. A **migration** is a script that changes an existing database structure. The recipe is shell: `for file in sql/migrations/*.sql; do echo "applying $$file"; $(PSQL) < $$file; done`. The `*` expands to every migration file in name order. Make reads `$` as the start of its own variable, so a dollar meant for the shell is written `$$`. Here there is one migration, `001_processed_events.sql`, which adds the `processed_events` table of lesson step 3.
- Lines 58 and 59: `queries` runs `sql/003_queries.sql` the same way, without `-q`, so the query results are printed.
- Lines 61 and 62: `reset-db` runs `sql/reset.sql` (zero every balance, empty `processed_events`).

### Starting from a clean slate

```make
# Delete and recreate both topics (this also drops every consumer group's offsets for them).
reset-topics:
	-$(KAFKA_BIN)/kafka-topics.sh --delete --topic payments --bootstrap-server localhost:9092
	-$(KAFKA_BIN)/kafka-topics.sh --delete --topic payments-dlq --bootstrap-server localhost:9092
	@sleep 3
	$(MAKE) --no-print-directory topics

# A clean slate: empty topics, zeroed balances, then PAYMENTS fresh payments and expected_totals.json.
data: $(STAMP) reset-topics reset-db
	$(PRODUCE) --payments $(PAYMENTS)
```

- Lines 65 to 69: `reset-topics` deletes both topics and creates them again. A line that starts with `-` (here the two `--delete` lines) **ignores a failure**: normally a non-zero exit code stops Make, but on the first run the topics do not exist yet. `@sleep 3` waits three seconds because Kafka deletes a topic in the background. Then `$(MAKE) ... topics` re-creates them. The comment notes that deleting a topic also drops every consumer group's offsets for it.
- Lines 71 to 73: `data: $(STAMP) reset-topics reset-db` has three prerequisites and one recipe line: `$(PRODUCE) --payments $(PAYMENTS)`, which publishes the payments and writes `expected_totals.json`. After `make data` the topics hold exactly those payments, the balances are zero, and the answer file is fresh.

### The lesson's steps

```make
run: data migrate ## the happy path: 10,000 payments, idempotent consumer, balance check, queries
	$(CONSUME) --mode idempotent --group run-$$(date +%s) --until-idle 5
	$(CHECK)
	$(MAKE) --no-print-directory queries

demo-baseline: data ## step 1: naive consumer, no failures, balances match
	$(CONSUME) --mode naive --group baseline-$$(date +%s) --until-idle 5
	$(CHECK)

demo-crash: data ## step 2: naive consumer crashes between DB commit and offset commit, a balance is wrong
	@group=crash-$$(date +%s); \
	$(CONSUME) --mode naive --group $$group --kill-at $(DEMO_KILL_AT); \
	echo "crash exit code: $$?"; \
	$(CONSUME) --mode naive --group $$group --until-idle 5; \
	$(CHECK) && echo "UNEXPECTED: no mismatch" || echo "(the mismatch above is the bug this step shows)"

crash-test: data migrate ## step 3: RUNS crash-and-restart runs with the idempotent consumer, each checked
	$(PY) -m idempotent_consumer.interfaces.crash_test_cli --runs $(RUNS) --payments $(PAYMENTS)
```

- Lines 75 to 78: `run: data migrate`. After the prerequisites, three commands: the idempotent consumer reads everything (`--group run-$$(date +%s)`: `$$` passes a `$` to the shell, which runs `date +%s`, the time in seconds, so every run has a brand-new group and starts from the beginning; `--until-idle 5` exits after five quiet seconds); `$(CHECK)` compares the balances with the expected totals (exit code 1 on any mismatch, which makes Make fail); and `queries` prints the SQL queries' results.
- Lines 80 to 82: `demo-baseline` (step 1): the naive consumer with no failures, then the check. The balances match.
- Lines 84 to 89: `demo-crash` (step 2). A backslash joins the lines into one shell command, which matters because **each recipe line normally runs in its own new shell**, and a shell variable would be lost between lines. `group=crash-$$(date +%s);` stores a group name. The naive consumer runs with `--kill-at $(DEMO_KILL_AT)` and dies. `echo "crash exit code: $$?"` prints the previous command's exit code (`$?` in the shell; `$$?` in the Makefile), 1. The consumer runs again in the same group and finishes. `$(CHECK) && echo "UNEXPECTED: no mismatch" || echo "(the mismatch above is the bug this step shows)"` prints the first message only if the check *succeeded* (which would mean the demo failed to show the bug) and the second if it failed, which is expected. The separators are `;`, so a failure of the consumer does not stop the chain.
- Lines 91 and 92: `crash-test` (step 3): runs the crash-test program with `--runs $(RUNS) --payments $(PAYMENTS)`.

```make
demo-poison: $(STAMP) reset-topics reset-db migrate ## step 4: a malformed event goes to the dead-letter topic
	$(PRODUCE) --payments $(PAYMENTS) --poison 1
	@group=poison-$$(date +%s); \
	$(CONSUME) --mode idempotent --group $$group --until-idle 5; \
	$(CHECK); \
	echo "== the dead-letter topic"; \
	$(KAFKA_BIN)/kafka-console-consumer.sh --topic payments-dlq --from-beginning --max-messages 1 \
	  --timeout-ms 10000 --bootstrap-server localhost:9092 2>/dev/null; \
	echo "== the group's lag per partition (0 everywhere: it moved past the bad record)"; \
	$(KAFKA_BIN)/kafka-consumer-groups.sh --describe --group $$group --bootstrap-server localhost:9092

# max.poll.interval.ms is lowered to 10 s so the eviction shows within seconds (the default is 5 minutes).
demo-evict: data migrate ## step 5: slow batches get the consumer evicted; smaller batches do not
	@echo "== max.poll.records=500: one batch is 50 s of work, longer than the 10 s poll interval"
	$(CONSUME) --mode idempotent --group evict-$$(date +%s) --delay 0.1 \
	  --max-poll-records 500 --max-poll-interval-ms 10000 --run-seconds 40 2>&1 | grep -E "EVICTED|poll timeout|done:" | cut -c1-200
	@echo "== max.poll.records=50: one batch is 5 s of work, inside the 10 s poll interval"
	$(CONSUME) --mode idempotent --group evict-fixed-$$(date +%s) --delay 0.1 \
	  --max-poll-records 50 --max-poll-interval-ms 10000 --run-seconds 40 2>&1 | grep -E "EVICTED|poll timeout|done:" | cut -c1-200
```

- Lines 94 to 103: `demo-poison` (step 4). It resets the topics and database, runs the migration, produces the payments plus one malformed event (`--poison 1`) and runs the idempotent consumer. `$(CHECK);` runs the check and the chain goes on whatever it returns. Then it prints the dead-letter topic with `kafka-console-consumer.sh --topic payments-dlq --from-beginning --max-messages 1 --timeout-ms 10000`, reading one message and giving up after 10 seconds (`2>/dev/null` discards the tool's error messages; `/dev/null` is a file that swallows what is written to it). Finally it describes the consumer group with `kafka-consumer-groups.sh --describe --group $$group`: the lag is 0 on every partition, because the consumer moved past the bad record.
- Lines 105 to 112: `demo-evict` (step 5). The comment says why `max.poll.interval.ms` is lowered to 10 seconds: so the eviction shows within seconds (the default is five minutes). `@echo "..."` prints a heading. The consumer runs with `--delay 0.1` (0.1 second of work per record), `--max-poll-records 500` and `--max-poll-interval-ms 10000`, so one batch is 50 seconds of work, longer than the interval, and the member is evicted; the second run uses `--max-poll-records 50` (5 seconds of work per batch) and is not. `--run-seconds 40` ends each after 40 seconds. In `2>&1 | grep -E "EVICTED|poll timeout|done:" | cut -c1-200`, `2>&1` sends standard error into the same stream as standard output, the pipe sends it on to `grep` which keeps only the lines containing one of the three phrases, and `cut -c1-200` shortens each line to its first 200 characters.

### Health, lint, format, tests

```make
health: $(STAMP) ## exit 0 when Kafka has the topics and PostgreSQL has the balances table
	$(PY) -m idempotent_consumer.interfaces.health_cli

lint: $(STAMP) ## ruff check, ruff format check, mypy, import-linter
	$(VENV)/bin/ruff check .
	$(VENV)/bin/ruff format --check .
	$(VENV)/bin/mypy
	$(VENV)/bin/lint-imports

format: $(STAMP) ## apply ruff's formatting and safe fixes
	$(VENV)/bin/ruff check --fix .
	$(VENV)/bin/ruff format .

# Unit tests need no services. Coverage is reported for the package; the domain and application must stay at 95% or more.
test: $(STAMP) ## unit tests with coverage (no Kafka or PostgreSQL needed)
	$(PY) -m pytest tests/unit --cov=idempotent_consumer --cov-report=term
	$(PY) -m coverage report --include='src/idempotent_consumer/domain/*,src/idempotent_consumer/application/*' --fail-under=95

test-integration: $(STAMP) ## adapters against the running services (make up first)
	$(PY) -m pytest tests/integration -m integration

test-e2e: $(STAMP) ## the lesson's scenarios through the command-line programs (make up first, about 3 minutes)
	$(PY) -m pytest tests/e2e -m e2e
```

- Lines 114 and 115: `health` runs the health program; its exit code becomes Make's.
- Lines 117 to 121: `lint` runs four checkers in order: `ruff check .` (style and common mistakes), `ruff format --check .` (fails if any file is not formatted), `mypy` (type hints) and `lint-imports` (the layer rules). The first failure stops Make. See `tech/ruff.md`, `tech/mypy.md` and `tech/import-linter.md`.
- Lines 123 to 125: `format` fixes what can be fixed: `ruff check --fix .` and `ruff format .`.
- Lines 127 to 130: `test` runs the unit tests with coverage measurement (`--cov=idempotent_consumer`, `--cov-report=term` prints a table), then `coverage report --include=... --fail-under=95` prints the coverage of only `domain/` and `application/` and exits non-zero below 95 percent. The pattern is in single quotes so the shell does not expand the `*`.
- Lines 132 and 133: `test-integration` runs `pytest tests/integration -m integration` (`-m` selects tests carrying that mark). Lines 135 and 136: `test-e2e` is the same for the end-to-end tests (about three minutes).

### Audit, check, build, stop, clean

```make
audit: $(STAMP) ## check the locked dependencies for known vulnerabilities
	$(VENV)/bin/pip-audit --no-deps --disable-pip -r requirements-dev.lock

check: lint test ## everything that needs no services

docker-build: ## build the container image of the command-line programs
	docker build -t kafka-idempotent-consumer:dev .

down: ## stop Kafka and PostgreSQL and delete their data
	docker compose down -v

clean: down
	rm -rf $(VENV) logs .pytest_cache .mypy_cache .ruff_cache .coverage expected_totals.json
```

- Lines 138 and 139: `audit` runs `pip-audit` on the lock file: `--no-deps` (the lock lists everything), `--disable-pip` (do not build a new environment), `-r` (read from the file). See `tech/pip-audit.md`.
- Line 141: `check: lint test` has no recipe; it only has prerequisites, so `make check` runs `lint` then `test`.
- Lines 143 and 144: `docker-build` runs `docker build -t kafka-idempotent-consumer:dev .`: `-t` names (tags) the image and `.` says "use the Dockerfile in this folder".
- Lines 146 and 147: `down` runs `docker compose down -v`: stop the containers and remove them and their volumes, which is why the target says it deletes the data.
- Lines 149 and 150: `clean: down` first runs `down`, then `rm -rf` removes the virtual environment, the logs, the tool caches, `.coverage` and `expected_totals.json`. `-r` includes folders, `-f` suppresses questions and missing-file errors. It cannot be undone, but the names are fixed and are only things the project itself creates.

## Try it yourself

1. `make help` lists every target with its description.
2. `make -n up` (dry run) prints the commands `up` would run without running them.
3. `make -n crash-test RUNS=3` shows `... crash_test_cli --runs 3 --payments 10000`.
4. `make -n test` shows the pytest and coverage commands.
5. `make PYTHON=python3.12 -n install` shows the chosen interpreter in the first recipe line (if the target is already built, run `make -B -n install`).

## Mistakes people make

- Using spaces instead of a tab at the start of a recipe line.
- Forgetting `.PHONY`, so a folder named like a target blocks it.
- Expecting `cd` in one recipe line to affect the next: each line is a fresh shell. Join lines with `&&` and `\` (as `run-unkeyed` does).
- Writing `$` for the shell when Make reads it first. Write `$$`.
- Exporting every variable. This repository's `export` is deliberate, and it bit once: a Make variable named `KILL_AT` reached the consumer's environment and made every run crash. The demo variable is now `DEMO_KILL_AT`, a name no program reads.

## Check yourself

1. What is a prerequisite? *A target or file that must be up to date before this target runs.*
2. What does `?=` do? *Sets a variable only if it is not already set.*
3. Why list `.PHONY` targets? *To stop a file with the same name from blocking them.*
4. What does a leading `-` on a recipe line mean? *Ignore a failure of that line.*
5. How does the install stamp avoid reinstalling? *The stamp file is rebuilt only when `requirements-dev.lock` or `pyproject.toml` is newer than it.*

## Further reading

- GNU Make manual (3.81 behavior; later versions only add): https://www.gnu.org/software/make/manual/make.html
