# 3. psycopg 3 and kafka-python 3.0.12

Status: accepted, 2026-10-26

## Context

The lesson's starting point uses `psycopg2` and `kafka-python`.

## Decision

Use `psycopg` 3 (`psycopg[binary]`), whose `connection.transaction()` context manager makes the transaction boundary explicit and needs no manual commit, and `kafka-python` 3.0.12, pinned. Both are installed from hash-pinned lock files.

## Alternatives considered

- **psycopg2:** the lesson's choice and still common, but its implicit transactions are easier to get wrong; version 3 is the maintained line.
- **confluent-kafka:** faster, but compiled per interpreter and platform (the author's default Python had no wheel).

## Consequences

- Connections run in autocommit mode so `with connection.transaction()` is the only transaction boundary.
- kafka-python 3 defaults to idempotent producers with `acks=all`; the adapters set `acks="all"` explicitly.
