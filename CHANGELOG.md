# Changelog

All notable changes to this project are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-10-26

### Added

- Kafka 4 (KRaft, one node) and PostgreSQL 18 in `docker-compose.yml`; the `payments` and `payments-dlq` topics.
- SQL scripts: `balances` schema and seed, the `processed_events` migration, the lesson's queries and a reset script.
- A producer of payments (and malformed events) with an independently computed `expected_totals.json`.
- A balance updater with a naive mode (steps 1 and 2) and an idempotent mode (step 3): event id and balance in one transaction, offsets committed after the database commit, a kill switch, dead-lettering of poison records and eviction handling.
- A balance checker, a 20-run crash harness and a health command.
- Layered package (`domain`, `application`, `infrastructure`, `interfaces`) with the dependency rule enforced by import-linter, unit, integration and end-to-end tests, ruff, mypy, hash-pinned lock files and a CI workflow.
- `docs/RUNBOOK.md`: the lesson's step-6 one-page entry.
