-- Migration for lesson step 3, "make processing idempotent".
-- Adds the table that records every event id the moment its payment is applied.
-- The consumer inserts here and updates balances in ONE transaction, so an event id is
-- present if and only if its amount is already in the balance.

CREATE TABLE IF NOT EXISTS processed_events (
    event_id     uuid        PRIMARY KEY,   -- the producer's eventId; the primary key makes a second insert fail
    topic        text        NOT NULL,      -- where the event was first read, for audit only
    partition    integer     NOT NULL,
    "offset"     bigint      NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE processed_events IS 'Idempotence guard: one row per event whose amount has been applied to balances.';
