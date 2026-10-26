# 2. Ports and adapters around Kafka and PostgreSQL

Status: accepted, 2026-10-26

## Context

The consume loop (apply, commit after, dead-letter poison, survive an eviction) is the lesson's substance and should be testable without a broker or a database, including the crash and eviction paths that are hard to provoke on demand.

## Decision

Four layers: `domain` (payments, validation, dead letters, balance comparison, kill points), `application` (the consume, check and crash-test use cases and the ports `PaymentSource`, `OffsetCommitter`, `DeadLetterSink`, `BalanceLedger`, `BalanceReader`, `LedgerAdmin`, `ConsumerRunner`, `HealthProbe`), `infrastructure` (kafka-python and psycopg adapters, settings, logging, retry, signals), `interfaces` (command-line programs). Dependencies point downward only; import-linter enforces it and forbids `domain` and `application` from importing `kafka` or `psycopg`. The kill switch is a function passed into the use case, and the clock and sleeping are injected.

## Alternatives considered

- **A script with the loop inline** (the lesson's starting point): the crash and eviction behavior could only be tested end to end.
- **An ORM or a repository framework:** hides the two SQL statements the lesson is about.

## Consequences

- The consume loop is tested with in-memory fakes, including a rejected commit and a poison record, at 100% coverage of `domain` and `application`.
- The two ledgers (`NaiveLedger`, `IdempotentLedger`) are separate classes, so the bug and its fix sit side by side.
- One more layer of indirection than a script.
