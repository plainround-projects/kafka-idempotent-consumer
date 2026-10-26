-- Queries the lesson runs. Read-only, safe to run at any time: make queries

-- Balance per account, the table the checker compares with expected_totals.json.
SELECT account_id, total FROM balances ORDER BY account_id;

-- All 50 accounts added up: compare with the sum of expected_totals.json.
SELECT count(*) AS accounts, sum(total) AS grand_total FROM balances;

-- How many events have been applied (equals the number of payments after a clean run).
SELECT count(*) AS applied_events FROM processed_events;

-- Events recorded twice: always zero rows, because event_id is the primary key.
SELECT event_id, count(*) FROM processed_events GROUP BY event_id HAVING count(*) > 1;

-- Where in the topic each partition's last applied event was (progress per partition).
SELECT partition, max("offset") AS last_applied_offset, count(*) AS events
FROM processed_events GROUP BY partition ORDER BY partition;

-- The busiest accounts by balance.
SELECT account_id, total FROM balances ORDER BY total DESC LIMIT 5;
