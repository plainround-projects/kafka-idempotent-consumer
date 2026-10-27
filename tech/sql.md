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

Five SQL files are read below, every line. First the ideas they rest on.

- A **relational database** stores data in **tables**. A table has named **columns** (each with a **type**, such as text or number) and holds **rows** (one record each). Think of a spreadsheet with strict column types.
- A **statement** is one instruction, ended by a semicolon. A line that starts with `--` is a **comment**: the database ignores it.
- **DDL** (data definition language) statements create or change the structure (`CREATE TABLE`). **DML** (data manipulation language) statements work on the data (`INSERT`, `UPDATE`, `SELECT`, `TRUNCATE`).
- A **primary key** is a column (or set of columns) whose value must be unique in every row. The database refuses a second row with the same key. It is also indexed, so lookups by key are fast.
- **NULL** means "no value". `NOT NULL` on a column forbids it. A **default** is the value used when an insert does not give one.
- A **transaction** is a group of statements that either all take effect or none does. The database keeps other sessions from seeing a half-done transaction.

The run order, as the comment in the first file says: `001_schema.sql`, `002_seed.sql`, then the files in `migrations/` (`make db`, then `make migrate`). `003_queries.sql` and `reset.sql` are run on demand.

### `sql/001_schema.sql`: the balances table

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

- Lines 1 and 2: comments. "DDL" is stated, and the run order.
- Line 4: `CREATE TABLE IF NOT EXISTS balances (` creates a table named `balances`; `IF NOT EXISTS` makes the statement do nothing when the table is already there, so it is safe to run twice.
- Line 5: `account_id text PRIMARY KEY`: a column of type `text` (any length) that is the primary key: one row per account, no repeats, never NULL.
- Lines 6 and 7: the comment says why `total` is `numeric` and never a floating-point type: a running total of currency must not drift. `total numeric(14,2) NOT NULL DEFAULT 0`: `numeric(14,2)` is an exact decimal number with up to 14 digits, 2 of them after the decimal point (so up to 999,999,999,999.99). Floating-point numbers cannot store 0.1 exactly; `numeric` can. `NOT NULL` forbids a missing total and `DEFAULT 0` fills in 0 when a row is created without one.
- Line 8: `updated_at timestamptz NOT NULL DEFAULT now()`: `timestamptz` is a date and time **with time zone**; `now()` is the function that returns the current time, so a new row is stamped when created. The consumer's update sets it again each time a payment is applied.
- Line 9: `);` closes the column list and ends the statement.
- Lines 11 and 12: `COMMENT ON TABLE balances IS '...'` and `COMMENT ON COLUMN balances.total IS '...'` store a description inside the database itself, visible with `\d+ balances` in `psql`. Text in SQL is written in single quotes.

### `sql/002_seed.sql`: the 50 accounts

```sql
-- DML: the 50 accounts the lesson's producer pays into (acct-001 ... acct-050), each starting at zero.
-- The consumer's UPDATE needs a row to match, so every account exists before any event is read.
-- Safe to run twice: existing rows are left alone.

INSERT INTO balances (account_id, total)
SELECT 'acct-' || lpad(n::text, 3, '0'), 0
FROM generate_series(1, 50) AS n
ON CONFLICT (account_id) DO NOTHING;
```

**Seeding** means inserting the starting rows. The consumer's `UPDATE` needs a row to match (an update of a missing account changes nothing, and the code treats that as a poison message), so every account must exist before any event is read.

- Line 5: `INSERT INTO balances (account_id, total)` says: insert rows, giving values for these two columns (the others use their defaults).
- Line 6: `SELECT 'acct-' || lpad(n::text, 3, '0'), 0` produces the rows to insert. `||` joins text. `n::text` is a **cast**: it turns the number `n` into text. `lpad(text, 3, '0')` pads the text on the left with `0` to a length of 3, so 7 becomes `007`. The second value, `0`, is the starting total.
- Line 7: `FROM generate_series(1, 50) AS n`: `generate_series(1, 50)` is a function that produces the numbers 1 to 50 as 50 rows; `AS n` names the column `n`. So 50 rows come out: `acct-001, 0` to `acct-050, 0`.
- Line 8: `ON CONFLICT (account_id) DO NOTHING;`: if a row with that account id already exists (a primary-key clash), skip it silently instead of failing. The comment on line 3 says what it buys: it is safe to run twice, existing balances are left alone.

### `sql/migrations/001_processed_events.sql`: the idempotence guard

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

A **migration** is a script that changes the structure of an existing database. This one is lesson step 3's schema change.

- Lines 1 to 4: comments. The key sentence: the consumer inserts into this table and updates `balances` **in one transaction**, so an event id is present **if and only if** its amount is already in the balance.
- Line 6: `CREATE TABLE IF NOT EXISTS processed_events (`.
- Line 7: `event_id uuid PRIMARY KEY`. `uuid` is a 128-bit unique identifier type. As the primary key it can exist only once in the table. This one rule is the heart of the project: a second attempt to insert the same event id fails or, with `ON CONFLICT DO NOTHING`, inserts nothing, and the code reads that as "already applied".
- Line 8: `topic text NOT NULL`: where the event was first read, for audit only. Line 9: `partition integer NOT NULL`: a whole number. Line 10: `"offset" bigint NOT NULL`: a larger whole number (up to about 9 quintillion), the record's number in the partition. The word `offset` is a reserved word in SQL (it is part of `LIMIT ... OFFSET ...`), so the column name is put in double quotes everywhere it appears.
- Line 11: `processed_at timestamptz NOT NULL DEFAULT now()`: when the row was added.
- Line 12: `);`. Line 14: a `COMMENT ON TABLE` describing the table as the idempotence guard.

### `sql/003_queries.sql`: the lesson's questions

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

`SELECT` reads data and changes nothing. `make queries` runs the file.

- Line 4: `SELECT account_id, total FROM balances ORDER BY account_id;`: two columns of every row, sorted by account id. This is the table the checker compares with `expected_totals.json`.
- Line 7: `SELECT count(*) AS accounts, sum(total) AS grand_total FROM balances;`: **aggregate functions** collapse many rows into one: `count(*)` counts rows, `sum(total)` adds a column. `AS accounts` names the result column. The result is one row.
- Line 10: the number of rows in `processed_events`: how many events were applied. After a clean run it equals the number of payments.
- Line 13: `SELECT event_id, count(*) FROM processed_events GROUP BY event_id HAVING count(*) > 1;`. `GROUP BY event_id` makes one group per id; `HAVING count(*) > 1` keeps only groups with more than one row. The comment says the result is always empty, because `event_id` is the primary key; running the query is a way to see that.
- Lines 16 and 17: for each partition, `max("offset")` is the highest offset applied and `count(*)` the number of events; `GROUP BY partition ORDER BY partition` gives one row per partition in order. It shows progress per partition.
- Line 20: `ORDER BY total DESC LIMIT 5`: the five largest balances (`DESC` is descending; `LIMIT 5` keeps five rows).

### `sql/reset.sql`: back to a clean start

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

- Line 4: `UPDATE balances SET total = 0, updated_at = now();` changes **every** row (there is no `WHERE`): zero the total, refresh the time.
- Lines 6 to 12: `DO $$ ... $$;` runs a small block of procedural code inside the database, written in PostgreSQL's built-in language. `$$` is a quoting style for the block's text (any quotes inside need no escaping). `BEGIN ... END` brackets the code. `IF to_regclass('processed_events') IS NOT NULL THEN TRUNCATE processed_events; END IF;`: `to_regclass('name')` returns the table's identity if it exists and NULL if it does not; so the table is emptied (`TRUNCATE` deletes every row at once) only if it exists. That makes the reset work both before and after the migration has been applied.

### The two statements the consumer runs

These are the Python strings in `postgres_ledger.py`; each `%s` is a placeholder for a value that the driver sends separately from the SQL text.

```python
ADD_TO_BALANCE = "UPDATE balances SET total = total + %s, updated_at = now() WHERE account_id = %s"
RECORD_EVENT = (
    'INSERT INTO processed_events (event_id, topic, partition, "offset") VALUES (%s, %s, %s, %s) '
    "ON CONFLICT (event_id) DO NOTHING"
)
```

- `UPDATE balances SET total = total + %s, updated_at = now() WHERE account_id = %s` adds the amount to one account's row. The `WHERE` picks the row; with no matching row nothing changes and the database reports 0 rows changed.
- `INSERT INTO processed_events (event_id, topic, partition, "offset") VALUES (%s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING` records the event. On a clash with an existing `event_id`, nothing is inserted, and the reported row count is 0. That count is how the code learns "already applied".

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
