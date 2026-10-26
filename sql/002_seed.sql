-- DML: the 50 accounts the lesson's producer pays into (acct-001 ... acct-050), each starting at zero.
-- The consumer's UPDATE needs a row to match, so every account exists before any event is read.
-- Safe to run twice: existing rows are left alone.

INSERT INTO balances (account_id, total)
SELECT 'acct-' || lpad(n::text, 3, '0'), 0
FROM generate_series(1, 50) AS n
ON CONFLICT (account_id) DO NOTHING;
