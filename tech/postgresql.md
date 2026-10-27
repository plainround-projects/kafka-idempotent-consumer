# PostgreSQL

## What it is

PostgreSQL ("Postgres") is a database server: a program that stores data in tables on disk, accepts connections over the network, runs SQL statements (`tech/sql.md`) and guarantees that data stays consistent even if a program crashes in the middle of a change. This repository runs version 18 in a container.

## Why this repository uses it

Kafka delivers messages at least once, so the same payment can arrive twice. Postgres is where the money balance lives, and its **transactions** and **primary keys** are what make the second delivery harmless.

## Where it appears

| Path | What it does |
| --- | --- |
| `docker-compose.yml` | Starts the `postgres` service (image `postgres:18`, host port 5433). |
| `Makefile` (`db`, `migrate`, `queries`, `reset-db`) | Run SQL files inside the container with `psql`. |
| `sql/` | All the SQL scripts. |
| `src/idempotent_consumer/infrastructure/postgres*.py` | The Python connection and the two ledger classes. |
| `.env.example` | The connection settings (`POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`). |

## How it works underneath

The server is one main process plus a helper process per connection. A client (here the Python program, or `psql`) opens a **TCP connection** to the port, proves who it is with a user name and password, then sends statements. By default every statement is its own transaction. With `BEGIN ... COMMIT` several statements form one transaction: other connections see none of its changes until `COMMIT`, and if the program dies before that, the database throws all of them away. Data are written to a log first (the **write-ahead log**), which is how committed changes survive a crash. If two connections try to insert the same primary key, the second waits for the first to finish and then fails or skips (`ON CONFLICT`).

`psql` is the command-line client that ships with Postgres. `psql -U bank -d bank` connects as user `bank` to database `bank`.

## Walkthrough of this repository's use

```yaml
  postgres:
    image: postgres:18
    ports:
      - "${POSTGRES_PORT:-5433}:5432"
    environment:
      # Credentials come from .env (make up copies .env.example the first time); none is written here.
      POSTGRES_USER: ${POSTGRES_USER:-bank}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:?copy .env.example to .env first}
      POSTGRES_DB: ${POSTGRES_DB:-bank}
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U $${POSTGRES_USER} -d $${POSTGRES_DB}"]
      interval: 3s
      timeout: 5s
      retries: 20
```

- `image: postgres:18`: the official image, version 18.
- `ports: "${POSTGRES_PORT:-5433}:5432"`: Postgres listens on 5432 inside; your computer's port is 5433 unless `.env` says otherwise (`:-5433` means "or 5433 if unset"). 5433 avoids a clash with a Postgres you may already run on 5432.
- `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`: the image creates that user, password and database on first start. The password uses `${POSTGRES_PASSWORD:?copy .env.example to .env first}`: if the variable is missing, Compose stops with that message. No password is written in the file.
- The health check `pg_isready -U ... -d ...` succeeds when the server accepts connections. `$${...}` writes a literal `$` so the shell inside the container expands the variable, not Compose.

```make
PSQL := docker compose exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -v ON_ERROR_STOP=1 -q
```

`docker compose exec -T postgres psql -U ... -d ... -v ON_ERROR_STOP=1 -q`: run `psql` inside the container. `-v ON_ERROR_STOP=1` makes psql stop at the first SQL error and return a failing exit code (the default keeps going). `-q` hides routine messages. The Makefile then feeds a file to its standard input: `$(PSQL) < sql/001_schema.sql`.

```python
class IdempotentLedger:
    """Lesson step 3: record the event id and add the amount in one transaction.

    A second delivery of the same event id inserts nothing, so the amount is not added again.
    """

    def __init__(self, connection: psycopg.Connection[Any]) -> None:
        self._connection = connection

    def apply(self, payment: Payment, delivery: Delivery) -> Outcome:
        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute(RECORD_EVENT, (payment.event_id, delivery.topic, delivery.partition, delivery.offset))
            if cursor.rowcount == 0:
                return Outcome.DUPLICATE
            _add_to_balance(cursor, payment)
        return Outcome.APPLIED
```

The core of the lesson. `with self._connection.transaction():` opens a transaction (`BEGIN`) and ends it with `COMMIT` when the block finishes normally, or `ROLLBACK` if an error escapes. Inside, the event id is inserted first. If that inserted nothing (`rowcount == 0`), the event was seen: return `DUPLICATE` and the transaction ends with nothing changed. Otherwise the balance is updated in the *same* transaction. If the process dies between the two statements, the database discards both.

## Try it yourself

(Start services with `make up`.)

1. `docker compose ps` shows the `postgres` service `healthy` on `0.0.0.0:5433->5432/tcp`.
2. `docker compose exec -T postgres psql -U bank -d bank -c "select version()"` prints a line starting `PostgreSQL 18`.
3. `docker compose exec -T postgres psql -U bank -d bank -c "\dt"` lists the tables `balances` and (after `make migrate`) `processed_events`.
4. `make demo-baseline`, then `make queries`: balances add up to the expected totals. The naive crash demo `make demo-crash` leaves one account too high.

## Mistakes people make

- Putting the password in a file that is committed. Here it comes from `.env`, which git ignores.
- Assuming the database is ready when the container is up. The health check waits for it.
- Forgetting that a database in a container loses its data on `make down`. That is wanted here.
- Using one connection from two threads. This repository uses one connection per process.

## Check yourself

1. What is a transaction? *A group of statements that take effect together or not at all.*
2. What makes a duplicate event fail to insert? *The primary key on `event_id`.*
3. What does `ON_ERROR_STOP=1` do? *Stops psql at the first error and returns a failing exit code.*
4. Why port 5433 on the host? *To avoid clashing with a Postgres already using 5432.*
5. Where does the password come from? *The environment (`.env`), never the code.*

## Further reading

- PostgreSQL 18 documentation: https://www.postgresql.org/docs/18/
- Transactions: https://www.postgresql.org/docs/18/tutorial-transactions.html
- `psql`: https://www.postgresql.org/docs/18/app-psql.html
- Docker image: https://hub.docker.com/_/postgres
