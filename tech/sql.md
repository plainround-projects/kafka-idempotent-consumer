# SQL

## What it is

SQL (Structured Query Language) is the language used to ask a relational database to store, change and read data. A relational database keeps data in **tables**: a table has named **columns** (what each value is) and **rows** (one record each). You write **statements**; the database runs them. Statements that define the shape of data (`CREATE TABLE`) are **DDL** (data definition language). Statements that change or read rows (`INSERT`, `UPDATE`, `SELECT`) are **DML** (data manipulation language).

## Why this repository uses it

The consumer must keep account balances in a database. The lesson's whole point is a database trick: record each event's id in the same **transaction** as the balance change, so a repeated event changes nothing. That trick is a few lines of SQL.

## Where it appears

| Path | Kind | What it does |
| --- | --- | --- |
| `sql/001_schema.sql` | DDL | Creates the `balances` table. |
| `sql/002_seed.sql` | DML | Inserts the 50 accounts, each at zero. |
| `sql/migrations/001_processed_events.sql` | DDL | Adds the `processed_events` table (the lesson's step 3). |
| `sql/003_queries.sql` | queries | Reads: balances, totals, duplicates, progress. |
| `sql/reset.sql` | DML | Zeroes balances and empties `processed_events`. |
| `src/idempotent_consumer/infrastructure/postgres_ledger.py` | DML in Python strings | The `UPDATE` and `INSERT` the consumer runs for every payment. |

## How it works underneath

You send a statement as text to the database server. The server parses it, plans how to run it, runs it, and returns rows or a count of changed rows. A **primary key** is a column (or columns) whose values must be unique and not empty; the database refuses a second row with the same key. A **transaction** groups statements so that all of them take effect or none do. A statement ends with `;`. `--` starts a comment.

## Walkthrough of this repository's use

### The balances table

```sql
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
```

- `CREATE TABLE IF NOT EXISTS` makes the statement safe to run twice.
- `account_id text PRIMARY KEY`: text, unique, never empty. One row per account.
- `total numeric(14,2) NOT NULL DEFAULT 0`: `numeric(14,2)` is an exact decimal with up to 14 digits, two after the point. Floating-point numbers cannot store 0.1 exactly; money must use an exact type. `NOT NULL` forbids "no value". `DEFAULT 0` fills in zero when an insert gives none.
- `updated_at timestamptz ... DEFAULT now()`: a time with a time zone, set to the current time when the row is created.
- `COMMENT ON ...` attaches a description stored in the database.

### Seeding with a generated series

```sql
-- DML: the 50 accounts the lesson's producer pays into (acct-001 ... acct-050), each starting at zero.
-- The consumer's UPDATE needs a row to match, so every account exists before any event is read.
-- Safe to run twice: existing rows are left alone.

INSERT INTO balances (account_id, total)
SELECT 'acct-' || lpad(n::text, 3, '0'), 0
FROM generate_series(1, 50) AS n
ON CONFLICT (account_id) DO NOTHING;
```

`generate_series(1, 50)` produces the numbers 1 to 50 as rows. `lpad(n::text, 3, '0')` turns the number into text (`::text` is a cast) padded with zeros on the left to three characters, and `'acct-' || ...` joins text (`||` is concatenation). So the rows are `acct-001` to `acct-050`, each with total 0. `ON CONFLICT (account_id) DO NOTHING` means "if the key already exists, skip this row", so running the seed twice is harmless.

### The migration: the idempotence guard

```sql
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
```

A **migration** is a script that changes an existing database's shape. Here it adds one table. `event_id uuid PRIMARY KEY` is the heart of the lesson: a `uuid` is a 128-bit unique id, and as a primary key it can exist only once. `"offset"` is quoted because `offset` is a reserved word in SQL. The other columns record where the event was first read, for audit.

### The two statements the consumer runs

```python
ADD_TO_BALANCE = "UPDATE balances SET total = total + %s, updated_at = now() WHERE account_id = %s"
RECORD_EVENT = (
    'INSERT INTO processed_events (event_id, topic, partition, "offset") VALUES (%s, %s, %s, %s) '
    "ON CONFLICT (event_id) DO NOTHING"
)
```

- `UPDATE balances SET total = total + %s ... WHERE account_id = %s`: add an amount to one account's row. `%s` marks a place for a value that the driver sends separately (never pasted into the text), which prevents SQL injection. `WHERE` chooses the row.
- `INSERT INTO processed_events (...) VALUES (...) ON CONFLICT (event_id) DO NOTHING`: record the event id. If it is already there, nothing is inserted and the database reports `rowcount` 0. That count is how the code learns "this event was already applied".

### Queries and the reset

```sql
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
```

`SELECT ... FROM ... ORDER BY` reads rows. `count(*)` and `sum(total)` are **aggregates**: one result for many rows. `GROUP BY ... HAVING count(*) > 1` finds ids that appear twice (always zero rows, because of the primary key). `LIMIT 5` keeps five rows.

```sql
-- Back to a clean start for the next experiment: every balance zero, no processed events.
-- Not part of the numbered run order; make reset-db runs it.

UPDATE balances SET total = 0, updated_at = now();

DO $$
BEGIN
    IF to_regclass('processed_events') IS NOT NULL THEN
        TRUNCATE processed_events;
    END IF;
END
$$;
```

`UPDATE balances SET ...` with no `WHERE` changes every row. The `DO $$ ... $$` block runs a small program inside the database: `to_regclass('processed_events')` is not null only when that table exists, so the script works before the migration too. `TRUNCATE` empties a table quickly.

## Try it yourself

(Start the services with `make up`.)

1. `make queries` prints the balance of all 50 accounts, then `50 | <grand total>`, then the number of applied events.
2. `docker compose exec -T postgres psql -U bank -d bank -c "\d balances"` shows the table's columns and its primary key.
3. Run the consumer (`make run`), then `make queries`: `applied_events` equals 10000 and the "events recorded twice" query returns `(0 rows)`.
4. Watch the guard refuse a duplicate: `docker compose exec -T postgres psql -U bank -d bank -c "INSERT INTO processed_events (event_id, topic, partition, \"offset\") VALUES ('6f1c2b6e-3d7e-4c55-9d52-0d6a4b7a1e11','t',0,0) ON CONFLICT (event_id) DO NOTHING" -c "INSERT INTO processed_events (event_id, topic, partition, \"offset\") VALUES ('6f1c2b6e-3d7e-4c55-9d52-0d6a4b7a1e11','t',0,0) ON CONFLICT (event_id) DO NOTHING"` prints `INSERT 0 1` then `INSERT 0 0`.

## Mistakes people make

- Using floating-point types for money.
- Building SQL by pasting values into the text. Always send values as parameters.
- A migration that fails when run twice. These use `IF NOT EXISTS` and `ON CONFLICT`.
- Using `UPDATE` without `WHERE` by accident. Only the reset script does it on purpose.
- Forgetting that reserved words like `offset` need quotes as column names.

## Check yourself

1. What is the difference between DDL and DML? *DDL defines structure; DML changes or reads rows.*
2. Why `numeric(14,2)` for money? *It is exact; floats are not.*
3. What does `ON CONFLICT (event_id) DO NOTHING` do? *Skips the insert if that key exists, and reports zero rows inserted.*
4. What does a primary key guarantee? *Unique, non-empty values; a duplicate is refused.*
5. Why can the reset script run before the migration? *It checks `to_regclass` before truncating.*

## Further reading

- PostgreSQL 18 SQL commands: https://www.postgresql.org/docs/18/sql-commands.html
- `INSERT ... ON CONFLICT`: https://www.postgresql.org/docs/18/sql-insert.html
- Numeric types: https://www.postgresql.org/docs/18/datatype-numeric.html
