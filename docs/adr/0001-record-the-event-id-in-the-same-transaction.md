# 1. Record the event id in the same database transaction as the balance update

Status: accepted, 2026-10-26

## Context

Kafka delivers at least once. A consumer that writes to PostgreSQL and then commits its offset has a gap: a crash between the two redelivers a payment that is already in the balance. The consumer must leave the balance right whatever the timing of a crash, a rebalance or an eviction.

## Decision

Keep a table `processed_events` with `event_id` as primary key. For each payment, in one database transaction, insert the event id (`ON CONFLICT DO NOTHING`) and, only if a row was inserted, add the amount to the balance. Commit the Kafka offset after the database commit. A redelivered event inserts nothing and is skipped.

## Alternatives considered

- **Check, then write** (read the event id, then update): races between consumers and between check and write; the primary key is the arbiter instead.
- **Store the offsets in the database** (the lesson's first stretch): one row per partition instead of one per event, but the consumer then owns partition assignment and seeking, and must handle rebalances itself.
- **Kafka transactions** (exactly once between topics): do not reach into PostgreSQL.
- **Commit the offset first** (at most once): loses a payment when the consumer crashes after the commit.

## Consequences

- The balance is correct after any crash; the crash test runs 20 of them.
- `processed_events` grows with every event; production prunes it by the topic's retention (ARCHITECTURE.md, limits).
- The idempotence guarantee ends at this database; an email or an API call made by the consumer is still at least once.
