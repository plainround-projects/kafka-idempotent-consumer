# psycopg 3

## What it is

psycopg is a Python library (a **driver**) that lets Python code talk to a PostgreSQL server: open a connection, send SQL, read rows back. "3" is the major version, a rewrite of the older `psycopg2`. This repository pins psycopg 3.3.6 and `psycopg-binary` 3.3.6, which bundles the compiled C part so nothing has to be built on your machine.

## Why this repository uses it

The consumer must run two SQL statements in one transaction for every payment. psycopg 3 makes the transaction boundary a visible `with` block.

## Where it appears

| Path | What it does |
| --- | --- |
| `pyproject.toml`, `requirements*.lock` | Pin `psycopg[binary]` (3.2 or newer, locked to 3.3.6). |
| `src/idempotent_consumer/infrastructure/postgres.py` | Opens the connection with a timeout and retries. |
| `src/idempotent_consumer/infrastructure/postgres_ledger.py` | Runs the SQL (ledger, reader, admin). |
| `src/idempotent_consumer/infrastructure/health_probes.py` | A probe that checks the `balances` table exists. |
| `tests/conftest.py`, `tests/integration/` | Set up and read the database in tests. |

## How it works underneath

`psycopg.connect(...)` opens a TCP connection and logs in. A **cursor** is the handle used to send a statement and read its result. `cursor.execute(sql, params)` sends the SQL text with `%s` placeholders and the values *separately*, and the server fills them in; values are never pasted into the text. Python's `Decimal` maps to Postgres `numeric` and back, so money stays exact. In **autocommit** mode each statement is its own transaction; `connection.transaction()` opens a real transaction for a block of statements.

## Walkthrough of this repository's use

```python
def connect(settings: PostgresSettings, attempts: int = 5) -> psycopg.Connection[Any]:
    """Open a connection in autocommit mode, so `with connection.transaction()` is the only boundary."""

    def open_connection() -> psycopg.Connection[Any]:
        return psycopg.connect(
            host=settings.host,
            port=settings.port,
            dbname=settings.database,
            user=settings.user,
            password=settings.password,
            connect_timeout=settings.connect_timeout_seconds,
            autocommit=True,
        )

    return retry(
        open_connection,
        attempts=attempts,
        base_delay_seconds=0.5,
        retry_on=(psycopg.OperationalError,),
        on_retry=lambda attempt, error: logger.warning(
            "retrying database connection", extra=fields(attempt=attempt, error=error)
        ),
    )
```

- `psycopg.connect(host=..., port=..., dbname=..., user=..., password=..., connect_timeout=...)`: the parts come from validated settings; no connection string is built or logged, so the password cannot leak into a log line. `connect_timeout` limits how long one attempt may take.
- `autocommit=True`: no hidden transaction is left open between statements. The only transaction boundary is the explicit `with connection.transaction():` blocks.
- `retry(..., retry_on=(psycopg.OperationalError,))`: if the connection fails (the server is still starting), wait and try again with growing pauses (`tech/python.md`, the `retry` function).

```python
def _add_to_balance(cursor: psycopg.Cursor[Any], payment: Payment) -> None:
    cursor.execute(ADD_TO_BALANCE, (payment.amount, payment.account_id))
    if cursor.rowcount == 0:
        raise PoisonMessageError(f"unknown account: {payment.account_id}")
```

`cursor.execute(ADD_TO_BALANCE, (payment.amount, payment.account_id))`: the first argument is the SQL, the second a tuple of values for the two `%s`. `cursor.rowcount` is the number of rows the statement changed; zero means no account with that id, so the payment can never be applied and is treated as poison.

```python
class PostgresBalanceReader:
    """Reads balances from PostgreSQL. Implements `BalanceReader`."""

    def __init__(self, connection: psycopg.Connection[Any]) -> None:
        self._connection = connection

    def read_balances(self) -> Mapping[str, Decimal]:
        rows = self._connection.execute("SELECT account_id, total FROM balances").fetchall()
        return {str(account): Decimal(total) for account, total in rows}
```

`connection.execute(sql).fetchall()` sends a query and returns all rows as tuples; `Decimal(total)` keeps the exact value.

## Try it yourself

1. `.venv/bin/python -c "import psycopg; print(psycopg.__version__)"` prints `3.3.6`.
2. `.venv/bin/python -c "import psycopg; print(psycopg.pq.__impl__)"` prints `binary` when the bundled C part is used.
3. With Postgres running and `.env` loaded (`set -a; . ./.env; set +a`): `.venv/bin/python -c "from idempotent_consumer.infrastructure import postgres; from idempotent_consumer.infrastructure.settings import Settings; c = postgres.connect(Settings.from_env().postgres); print(c.execute('select 1').fetchone())"` prints `(1,)`.

## Mistakes people make

- Building SQL with f-strings. Use placeholders.
- Leaving a transaction open (an exception inside a block is fine: the `with` rolls back; a forgotten `commit` in non-autocommit mode is not).
- Using floats for money. `Decimal` stays exact.
- Printing the connection details including the password. This repository passes the parts separately and the settings class hides the password from its printed form.

## Check yourself

1. What are `%s` placeholders for? *Values are sent separately from the SQL, preventing injection.*
2. What does `with connection.transaction():` do? *Starts a transaction; commits at the end, rolls back on error.*
3. What does `rowcount` tell you? *How many rows the last statement changed.*
4. Why `autocommit=True`? *So explicit `transaction()` blocks are the only boundaries.*
5. What does `psycopg-binary` add? *A prebuilt compiled part, so nothing needs compiling.*

## Further reading

- psycopg 3.3 documentation: https://www.psycopg.org/psycopg3/docs/
- Transactions: https://www.psycopg.org/psycopg3/docs/basic/transactions.html
- Passing parameters: https://www.psycopg.org/psycopg3/docs/basic/params.html
