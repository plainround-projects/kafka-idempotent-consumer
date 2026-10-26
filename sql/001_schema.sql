-- DDL: the account-balance table the consumer keeps up to date.
-- Run order: 001_schema.sql, 002_seed.sql, then migrations/ (make db, make migrate).

CREATE TABLE IF NOT EXISTS balances (
    account_id text          PRIMARY KEY,
    -- numeric, never float: a running total of currency must not drift.
    total      numeric(14,2) NOT NULL DEFAULT 0,
    updated_at timestamptz   NOT NULL DEFAULT now()
);

COMMENT ON TABLE balances IS 'One row per account; total is the sum of every payment applied to it.';
COMMENT ON COLUMN balances.total IS 'Running total in currency units, two decimals.';
