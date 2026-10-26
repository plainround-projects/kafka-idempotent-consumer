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

-include .env
export

.PHONY: help install env up topics db migrate queries reset-db reset-topics data run demo-baseline demo-crash \
        crash-test demo-poison demo-evict health lint format test test-integration test-e2e audit check \
        docker-build down clean

help: ## list the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | sed -E 's/:.*## /\t/' | sort

$(STAMP): requirements-dev.lock pyproject.toml
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install -q --require-hashes -r requirements-dev.lock
	$(VENV)/bin/pip install -q --no-deps -e .
	touch $@

install: $(STAMP) ## create .venv from the lock file

env:
	@[ -f .env ] || { cp .env.example .env; echo "created .env from .env.example"; }

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

# Delete and recreate both topics (this also drops every consumer group's offsets for them).
reset-topics:
	-$(KAFKA_BIN)/kafka-topics.sh --delete --topic payments --bootstrap-server localhost:9092
	-$(KAFKA_BIN)/kafka-topics.sh --delete --topic payments-dlq --bootstrap-server localhost:9092
	@sleep 3
	$(MAKE) --no-print-directory topics

# A clean slate: empty topics, zeroed balances, then PAYMENTS fresh payments and expected_totals.json.
data: $(STAMP) reset-topics reset-db
	$(PRODUCE) --payments $(PAYMENTS)

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

audit: $(STAMP) ## check the locked dependencies for known vulnerabilities
	$(VENV)/bin/pip-audit --no-deps --disable-pip -r requirements-dev.lock

check: lint test ## everything that needs no services

docker-build: ## build the container image of the command-line programs
	docker build -t kafka-idempotent-consumer:dev .

down: ## stop Kafka and PostgreSQL and delete their data
	docker compose down -v

clean: down
	rm -rf $(VENV) logs .pytest_cache .mypy_cache .ruff_cache .coverage expected_totals.json
