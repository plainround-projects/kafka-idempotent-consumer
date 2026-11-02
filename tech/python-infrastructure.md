# Python: the infrastructure layer, file by file

## What it is

The folder `src/idempotent_consumer/infrastructure/` holds the **adapters**: the code that touches the outside world. Kafka (through the `kafka-python` library), PostgreSQL (through `psycopg`), environment variables, log output, the clock's `sleep`, files on disk, other processes, and operating-system signals. Each adapter fits one of the **ports** (the descriptions in `application/ports.py`), so the use cases never know what is really behind them.

This is the only layer allowed to `import kafka` or `import psycopg`. It may use the application and the domain. They may not use it: `make lint` fails if `domain/` or `application/` imports `infrastructure/`.

Words used below:

- A **port** is a description of what a use case needs; an **adapter** is a class that really does it.
- A **library** is code written by other people that you install and import. `kafka-python` speaks Kafka's network protocol; `psycopg` speaks PostgreSQL's.
- The **environment** is a set of named text values (like `TOPIC=payments`) that the operating system gives every program when it starts: **environment variables**.
- A **database transaction** is a group of changes that either all happen or none do. A **commit** makes them permanent; a **rollback** throws them away.

## Why this repository uses it

Everything that touches the outside world is kept in one layer: the Kafka producer, consumer and dead-letter sink, the PostgreSQL connection and the SQL statements, the environment and the password, logs, retries, signals and the child process of the crash test. Each adapter fits a port, so `kafka` and `psycopg` are imported in this layer only.

## Where it appears

| File | Role |
| --- | --- |
| `infrastructure/__init__.py` | Marks the folder as a package. |
| `settings.py` | Reads and checks the environment variables once at startup, including the database password. |
| `logging.py` | Writes one `key=value` log line per event. |
| `retry.py` | Retries a failing network call with growing pauses. |
| `shutdown.py` | Turns Ctrl+C and `docker stop` into a flag the loops check. |
| `kafka_producers.py` | Publishes payments and parks dead letters. |
| `kafka_consumer.py` | Reads payments as a group member and commits offsets by hand. |
| `postgres.py` | Opens database connections, with retries. |
| `postgres_ledger.py` | The SQL that applies payments, reads balances and resets the tables. |
| `expected_file.py` | Saves and loads the expected totals as a JSON file. |
| `subprocess_runner.py` | Runs the consumer as a separate process for the crash test. |
| `health_probes.py` | Health checks for Kafka and PostgreSQL. |

## How it works underneath

An **adapter** is a class that wraps a library and exposes only the methods of a port. Creating a client object (a producer, a consumer, a connection) opens network connections, and the Kafka clients also start background threads that send and fetch. Settings are read once at start from `os.environ`, the dictionary of environment variables the operating system gave the process, and checked at once. **Retry with backoff** repeats a failing call with growing, slightly random pauses. A **signal** such as Ctrl+C or `docker stop` makes the operating system call a function registered with `signal.signal`; Python runs it in the main thread between two steps of the program, and here it only raises a flag that the work loops test. Library errors are caught at the edge and re-raised as the application's own named errors, so the layers above never see a library class. Here the consumer has auto-commit off, so offsets move only when the use case commits them, and the database connection uses `autocommit=True`, so a `with connection.transaction():` block is the only transaction boundary. The password comes from the environment only and is hidden from the printed form of the settings.

## Walkthrough of this repository's use

Every source file of this layer is read below, in the order of the table above. Each block is a piece of the real file, copied exactly, followed by an explanation in plain words.

### `infrastructure/__init__.py`

```python
"""Adapters: Kafka clients, PostgreSQL, settings, logging, retries, signals, subprocesses."""
```

Only a docstring: it marks the folder as a package and states the layer's job.

### `settings.py`: the environment, checked once

A regular expression (**regex**) is a short pattern that describes text. `re` is Python's module for them.

```python
"""Configuration from the environment, validated once at startup (twelve-factor). Secrets only from here."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field

_SERVERS = re.compile(r"^[\w.\-]+:\d{1,5}(,[\w.\-]+:\d{1,5})*$")
_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR"}


class ConfigurationError(ValueError):
    """An environment variable is missing or has an unusable value."""


def _text(environ: Mapping[str, str], name: str, default: str | None = None) -> str:
    value = environ.get(name, default if default is not None else "").strip()
    if not value:
        raise ConfigurationError(f"{name} must be set and not empty")
    return value


def _positive_int(environ: Mapping[str, str], name: str, default: int) -> int:
    raw = environ.get(name, str(default)).strip()
    if not raw.isdigit() or int(raw) < 1:
        raise ConfigurationError(f"{name} must be a positive integer, got {raw!r}")
    return int(raw)

```

- Line 1: "twelve-factor" is a set of rules for programs; the one used says *configuration comes from the environment*, so the same code runs on a laptop and in a container without edits. "Secrets only from here" means the database password is read from the environment and from nowhere else: never from a file in the repository.
- Line 5: `os.environ` is the environment as a dictionary.
- Line 10: `_SERVERS = re.compile(r"^[\w.\-]+:\d{1,5}(,[\w.\-]+:\d{1,5})*$")`. The `r` before the quote makes a **raw string** (backslashes are kept). The pattern: `^` start of the text; `[\w.\-]+` one or more letters, digits, underscore, dot or hyphen (a host name); `:`; `\d{1,5}` one to five digits (a port); the group `(,host:port)*` allows more `,host:port` parts; `$` end. The leading underscore on the name means "private to this file".
- Line 11: `_LEVELS` is a **set** of the allowed log levels.
- Lines 14 and 15: `ConfigurationError`, a named error built on `ValueError`.
- Lines 18 to 22: `_text` reads one variable. `environ.get(name, default if default is not None else "")` returns the value, or the default, or an empty string when there is no default. `.strip()` removes spaces at both ends. An empty result raises `ConfigurationError("NAME must be set and not empty")`. So a variable without a default (the password) must be provided, and a variable set to nothing is an error, not silently the default.
- Lines 25 to 29: `_positive_int` reads a variable as text and requires a whole number of at least 1. `raw.isdigit()` is true only if every character is a digit (so `-5`, `1.5` and `abc` fail). `{raw!r}` prints the value with quotes.

```python
@dataclass(frozen=True, slots=True)
class KafkaSettings:
    """The Kafka part of the configuration."""

    bootstrap_servers: str
    topic: str
    dlq_topic: str
    group_id: str
    request_timeout_ms: int
    retry_backoff_ms: int


@dataclass(frozen=True, slots=True)
class PostgresSettings:
    """The PostgreSQL part of the configuration; the password is never printed."""

    host: str
    port: int
    database: str
    user: str
    password: str = field(repr=False)  # never printed or logged
    connect_timeout_seconds: int


@dataclass(frozen=True, slots=True)
class Settings:
    """Everything the programs read from the environment. See .env.example for each variable."""

    kafka: KafkaSettings
    postgres: PostgresSettings
    log_level: str
```

- Lines 32 to 41: `KafkaSettings`, a frozen (read-only) dataclass holding the Kafka part: the bootstrap address(es), the topic, the dead-letter topic, the consumer group, and two client timeouts in milliseconds. A **dataclass** is a class whose repetitive methods Python writes from a list of fields.
- Lines 44 to 53: `PostgresSettings`: host, port, database, user, password and the connect timeout. `password: str = field(repr=False)`: `field(repr=False)` removes the password from the automatic text form of the object, so a settings object printed by accident, in a log line or in an error message, never shows it. The comment says it. A test checks this.
- Lines 56 to 62: `Settings` holds both parts and the log level. The docstring points to `.env.example` for the variables.

```python
    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> Settings:
        env = os.environ if environ is None else environ
        servers = _text(env, "BOOTSTRAP_SERVERS", "localhost:9092")
        if not _SERVERS.match(servers):
            raise ConfigurationError(f"BOOTSTRAP_SERVERS must be host:port[,host:port], got {servers!r}")
        level = _text(env, "LOG_LEVEL", "INFO").upper()
        if level not in _LEVELS:
            raise ConfigurationError(f"LOG_LEVEL must be one of {sorted(_LEVELS)}, got {level!r}")
        return cls(
            kafka=KafkaSettings(
                bootstrap_servers=servers,
                topic=_text(env, "TOPIC", "payments"),
                dlq_topic=_text(env, "DLQ_TOPIC", "payments-dlq"),
                group_id=_text(env, "GROUP_ID", "balance-updater"),
                request_timeout_ms=_positive_int(env, "KAFKA_REQUEST_TIMEOUT_MS", 30_000),
                retry_backoff_ms=_positive_int(env, "KAFKA_RETRY_BACKOFF_MS", 100),
            ),
            postgres=PostgresSettings(
                host=_text(env, "POSTGRES_HOST", "localhost"),
                port=_positive_int(env, "POSTGRES_PORT", 5433),
                database=_text(env, "POSTGRES_DB", "bank"),
                user=_text(env, "POSTGRES_USER", "bank"),
                password=_text(env, "POSTGRES_PASSWORD"),
                connect_timeout_seconds=_positive_int(env, "POSTGRES_CONNECT_TIMEOUT_SECONDS", 5),
            ),
            log_level=level,
        )
```

- Line 64: `@classmethod` makes `from_env` a method of the class, called as `Settings.from_env()`. Its first parameter `cls` is the class itself.
- Line 65: `environ: Mapping[str, str] | None = None`: pass a dictionary (tests do), or nothing to use the real environment (line 66).
- Lines 67 to 69: `BOOTSTRAP_SERVERS` (default `localhost:9092`), checked against the pattern.
- Lines 70 to 72: `LOG_LEVEL` (default `INFO`), upper-cased and checked.
- Lines 73 to 91: builds the nested objects. The variables and defaults:

| Variable | Default | Meaning |
| --- | --- | --- |
| `BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka address(es), `host:port` separated by commas. |
| `TOPIC` | `payments` | The topic of payment events. |
| `DLQ_TOPIC` | `payments-dlq` | The dead-letter topic. |
| `GROUP_ID` | `balance-updater` | The consumer group. |
| `KAFKA_REQUEST_TIMEOUT_MS` | `30000` | How long a Kafka client waits for the broker's answer. |
| `KAFKA_RETRY_BACKOFF_MS` | `100` | The pause between the Kafka client's own retries. |
| `POSTGRES_HOST` | `localhost` | Where PostgreSQL runs. |
| `POSTGRES_PORT` | `5433` | The port PostgreSQL is published on (not the usual 5432, so it does not clash with a PostgreSQL already on the machine). |
| `POSTGRES_DB` | `bank` | The database name. |
| `POSTGRES_USER` | `bank` | The database user. |
| `POSTGRES_PASSWORD` | none: must be set | The password. It has no default, deliberately: no password is ever written into the code. |
| `POSTGRES_CONNECT_TIMEOUT_SECONDS` | `5` | How long to wait when connecting. |
| `LOG_LEVEL` | `INFO` | One of DEBUG, INFO, WARNING, ERROR. |

**Fail fast:** every wrong value stops the program at startup with a message that names the variable, instead of crashing later in the middle of a run.

### `kafka_producers.py`: publishing and parking

```python
"""Adapters: publish payments and park dead letters with kafka-python."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from kafka import KafkaProducer

from ..domain.dead_letter import DeadLetter
from .settings import KafkaSettings


def _producer(settings: KafkaSettings) -> Any:
    return KafkaProducer(
        bootstrap_servers=settings.bootstrap_servers,
        acks="all",
        request_timeout_ms=settings.request_timeout_ms,
        retry_backoff_ms=settings.retry_backoff_ms,
    )
```

- Line 5: `json` turns dictionaries into JSON text. Line 9: `from kafka import KafkaProducer` is the library's producer class: the import the layer rule allows here and nowhere inside `domain/` or `application/`.
- Lines 15 to 21: `_producer(settings)` builds a producer. A leading underscore means "private to this file". `bootstrap_servers` is the address(es) used to find the cluster. `acks="all"` means a send counts as successful only when **all in-sync replicas** have the record (here the one broker). `request_timeout_ms` is how long to wait for an answer; `retry_backoff_ms` the pause between the client's own retries. The return type `Any` switches type checking off for this untyped library's object.

```python
class KafkaPaymentPublisher:
    """Publishes payment payloads keyed by account id, so one account's payments share a partition."""

    def __init__(self, settings: KafkaSettings, topic: str | None = None) -> None:
        self._topic = topic or settings.topic
        self._producer = _producer(settings)

    def publish(self, payload: Mapping[str, Any]) -> None:
        self._producer.send(self._topic, key=str(payload["accountId"]).encode(), value=json.dumps(payload).encode())

    def flush(self) -> None:
        self._producer.flush(timeout=60)

    def close(self) -> None:
        self._producer.close(timeout=60)
```

- Lines 27 to 29: the constructor takes the settings and an optional topic (`topic or settings.topic` uses the first if given).
- Line 32: `publish` calls `self._producer.send(topic, key=..., value=...)`. The key is the **account id** as bytes (`str(...).encode()`), so all payments of one account go to the same partition and stay in order. The value is the payload as JSON text turned into bytes. `send` does not wait: it puts the record in a buffer and returns at once.
- Lines 34 and 35: `flush` waits up to 60 seconds for everything buffered to be acknowledged. Lines 37 and 38: `close` shuts the connection.

```python
class KafkaDeadLetterSink:
    """Implements `DeadLetterSink`: waits for the broker's acknowledgment before returning."""

    def __init__(self, settings: KafkaSettings, topic: str | None = None, timeout_seconds: float = 30.0) -> None:
        self._topic = topic or settings.dlq_topic
        self._timeout = timeout_seconds
        self._producer = _producer(settings)

    def send(self, letter: DeadLetter) -> None:
        self._producer.send(self._topic, value=json.dumps(letter.payload()).encode()).get(timeout=self._timeout)

    def close(self) -> None:
        self._producer.close(timeout=self._timeout)
```

- Line 44: `timeout_seconds` defaults to 30. Line 45: the topic defaults to the dead-letter topic.
- Line 50: `self._producer.send(...).get(timeout=self._timeout)`. `send` returns a **future**, an object standing for a result that is not ready yet. `.get(timeout)` waits for it: it returns when the broker has acknowledged the record, and raises an error if the wait ends first or the send fails. That is the promise of `DeadLetterSink.send` ("returns only once safely stored"). No key is given, so the letter is spread over partitions.

### `kafka_consumer.py`: reading and committing by hand

```python
"""Adapter: read payments as a group member and commit offsets with kafka-python."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata

from ..application.ports import CommitRejectedError, Delivery
from .settings import KafkaSettings


class KafkaPaymentConsumer:
    """Implements `PaymentSource` and `OffsetCommitter`.

    Auto-commit is off: the use case commits an offset only after the database transaction for that
    record committed. `max_poll_records` and `max_poll_interval_ms` are the eviction knobs of step 5.
    """
```

- Line 8: `TopicPartition` is the library's name for the pair (topic, partition number). Line 9: `KafkaError` is the base class of the library's errors. Line 10: `OffsetAndMetadata` is the library's record of a committed position.
- Lines 16 to 21: the class implements two ports at once, `PaymentSource` and `OffsetCommitter`. The docstring names the design: auto-commit is off, and `max_poll_records` and `max_poll_interval_ms` are the eviction knobs of lesson step 5.

```python
    def __init__(
        self,
        settings: KafkaSettings,
        *,
        group_id: str | None = None,
        topic: str | None = None,
        client_id: str = "balance-updater",
        max_poll_records: int = 500,
        max_poll_interval_ms: int = 300_000,
    ) -> None:
        self._consumer: Any = KafkaConsumer(
            topic or settings.topic,
            bootstrap_servers=settings.bootstrap_servers,
            group_id=group_id or settings.group_id,
            client_id=client_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=max_poll_records,
            max_poll_interval_ms=max_poll_interval_ms,
            request_timeout_ms=settings.request_timeout_ms,
            retry_backoff_ms=settings.retry_backoff_ms,
        )
```

- Lines 23 to 32: the parameters after the `*` on line 26 must be passed by name. Defaults: client id `balance-updater`, `max_poll_records=500` (the most records one poll returns) and `max_poll_interval_ms=300_000` (five minutes).
- Lines 33 to 44: the consumer is built **already subscribed** to the topic (the first argument). `group_id` is the consumer group. `enable_auto_commit=False` is the key line: the library will never commit offsets by itself; the use case commits them after the database commit. `auto_offset_reset="earliest"` means a group with no committed offset starts from the oldest record. `max_poll_interval_ms` is the longest the member may go between polls: a consumer that takes longer (because processing is slow) is considered dead, removed from the group, and its partitions are given to someone else. That removal is called **eviction**. The two timeouts come from the settings.

```python
    def poll(self, timeout_seconds: float) -> Sequence[Delivery]:
        batches = self._consumer.poll(timeout_ms=int(timeout_seconds * 1000))
        return [
            Delivery(record.topic, record.partition, record.offset, record.key, record.value)
            for batch in batches.values()
            for record in batch
        ]

    def has_assignment(self) -> bool:
        return bool(self._consumer.assignment())
```

- Lines 46 to 52: `poll` waits up to the given seconds (converted to milliseconds with `int(timeout_seconds * 1000)`), and the library returns a dictionary of lists of records grouped by partition. A comprehension with two `for` parts (`for batch in batches.values() for record in batch`) flattens it into one list of `Delivery` objects, copying the fields the application needs.
- Lines 54 and 55: `has_assignment` is true when the library reports at least one assigned partition. `bool(...)` of an empty collection is `False`.

```python
    def commit(self, next_offsets: Mapping[tuple[str, int], int]) -> None:
        offsets = {
            TopicPartition(topic, partition): OffsetAndMetadata(offset, "", -1)
            for (topic, partition), offset in next_offsets.items()
        }
        try:
            self._consumer.commit(offsets)
        except KafkaError as error:
            raise CommitRejectedError(type(error).__name__) from error

    def close(self) -> None:
        self._consumer.close()
```

- Lines 58 to 61: builds the dictionary the library wants: `TopicPartition(topic, partition)` keys and `OffsetAndMetadata(offset, "", -1)` values (the offset to commit, an empty metadata text, and `-1` meaning "leader epoch unknown"). The comprehension unpacks each key pair `(topic, partition)` directly in the `for`.
- Lines 62 to 65: `self._consumer.commit(offsets)` sends the commit. If the group refuses (for example because this member was evicted) the library raises a `KafkaError`; the code converts it to the application's `CommitRejectedError`, carrying the original error's class name (`type(error).__name__`). The application layer never sees a Kafka-specific class.
- Lines 67 and 68: `close` leaves the group.

### `postgres.py`: opening connections

A **connection** is an open conversation with the database.

```python
"""Adapter: connections to PostgreSQL, with a connect timeout and retries with backoff."""

from __future__ import annotations

import logging
from typing import Any

import psycopg

from .logging import fields
from .retry import retry
from .settings import PostgresSettings

logger = logging.getLogger(__name__)


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

- Line 8: `import psycopg` loads the PostgreSQL client library (version 3).
- Line 17: the function returns a `psycopg.Connection[Any]`. The square brackets are a type hint for what the rows look like.
- Lines 20 to 29: the inner function `open_connection` calls `psycopg.connect(...)` with the host, port, database name (`dbname`), user, password and `connect_timeout` (seconds before giving up on a connection attempt). **`autocommit=True`** means the connection does not open an implicit transaction around each statement: every statement commits at once unless the code opens a transaction itself. The docstring on line 18 states the consequence: `with connection.transaction():` is the only transaction boundary, so what is in a transaction is visible in the code.
- Lines 31 to 38: `retry(...)` calls `open_connection` up to `attempts` times (default 5), starting with a 0.5-second pause that doubles, but only for `psycopg.OperationalError` (the library's error for "cannot connect", "server closed the connection"). Each retry is logged by the `lambda`, a one-line nameless function. The retry helper is explained below in `retry.py`.

### `postgres_ledger.py`: the SQL

**SQL** is the language for talking to a relational database. See `tech/sql.md`. The two statements of this project are strings in Python; the values are never pasted into the text.

```python
"""Adapters: the balance ledger, its reader and its admin, as plain SQL on PostgreSQL."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Any

import psycopg

from ..application.ports import Delivery, Outcome
from ..domain.errors import PoisonMessageError
from ..domain.payments import Payment

ADD_TO_BALANCE = "UPDATE balances SET total = total + %s, updated_at = now() WHERE account_id = %s"
RECORD_EVENT = (
    'INSERT INTO processed_events (event_id, topic, partition, "offset") VALUES (%s, %s, %s, %s) '
    "ON CONFLICT (event_id) DO NOTHING"
)


def _add_to_balance(cursor: psycopg.Cursor[Any], payment: Payment) -> None:
    cursor.execute(ADD_TO_BALANCE, (payment.amount, payment.account_id))
    if cursor.rowcount == 0:
        raise PoisonMessageError(f"unknown account: {payment.account_id}")

```

- Lines 15: `ADD_TO_BALANCE = "UPDATE balances SET total = total + %s, updated_at = now() WHERE account_id = %s"`. It adds the amount to one account's `total` and sets `updated_at`. Each `%s` is a **placeholder**: psycopg sends the SQL text and the values separately, so a value can never be read as SQL (this prevents "SQL injection"). The values come in a tuple when the statement is executed.
- Lines 16 to 19: `RECORD_EVENT` is a Python string written as two pieces in brackets (adjacent strings are joined). `INSERT INTO processed_events (event_id, topic, partition, "offset") VALUES (%s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING`. It records the event id. `"offset"` is quoted because `OFFSET` is a reserved word in SQL. **`ON CONFLICT (event_id) DO NOTHING`** is the heart of idempotence: `event_id` is the table's primary key, which must be unique; if the event id is already there, the insert silently does nothing instead of failing, and the number of rows inserted is 0.
- Lines 22 to 25: `_add_to_balance(cursor, payment)` runs the update. A **cursor** is the object used to run statements and read results on a connection. `cursor.rowcount` is the number of rows the last statement changed; 0 means no account with that id exists, which can never be fixed by retrying, so it raises `PoisonMessageError` (the use case then parks the record). The amount is a `Decimal`; psycopg sends it to a `numeric` column exactly.

```python
class NaiveLedger:
    """Lesson steps 1 and 2: add the amount, with no memory of which events were seen."""

    def __init__(self, connection: psycopg.Connection[Any]) -> None:
        self._connection = connection

    def apply(self, payment: Payment, delivery: Delivery) -> Outcome:
        with self._connection.transaction(), self._connection.cursor() as cursor:
            _add_to_balance(cursor, payment)
        return Outcome.APPLIED
```

- `NaiveLedger` is the lesson's steps 1 and 2: add the amount and remember nothing. It exists to show what goes wrong.
- Line 35: `with self._connection.transaction(), self._connection.cursor() as cursor:` opens two context managers in one `with`. A **context manager** sets something up when the block starts and tidies up when it ends, even on error. `connection.transaction()` begins a transaction and, when the block ends normally, commits; if an error is raised inside, it rolls back. `connection.cursor()` gives a cursor and closes it afterwards.
- Line 36 adds the amount. Line 37 returns `APPLIED`. A redelivered event is added **again**: that is the double-count the project demonstrates.

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

- `IdempotentLedger` is lesson step 3. In **one transaction** it records the event id and, only if that insert actually inserted a row, adds the amount.
- Line 51: `cursor.execute(RECORD_EVENT, (event_id, topic, partition, offset))` runs the insert with the four values.
- Lines 52 and 53: `if cursor.rowcount == 0: return Outcome.DUPLICATE`. No row inserted means the id was already there. Returning from inside the `with` block ends the transaction normally (committing nothing of consequence).
- Line 54: otherwise add the amount. If it raises `PoisonMessageError` (unknown account), the exception passes through the `with` block, which **rolls back the insert**, so the event id is not recorded for a payment that was never applied.
- Line 55: after the block (a successful commit) return `APPLIED`.
- The invariant, as the migration says: an event id is present in `processed_events` if and only if its amount is in the balance, because both happen in one transaction.

```python
class PostgresBalanceReader:
    """Reads balances from PostgreSQL. Implements `BalanceReader`."""

    def __init__(self, connection: psycopg.Connection[Any]) -> None:
        self._connection = connection

    def read_balances(self) -> Mapping[str, Decimal]:
        rows = self._connection.execute("SELECT account_id, total FROM balances").fetchall()
        return {str(account): Decimal(total) for account, total in rows}


class PostgresLedgerAdmin:
    """Zeroes balances and empties processed_events. Implements `LedgerAdmin`."""

    def __init__(self, connection: psycopg.Connection[Any]) -> None:
        self._connection = connection

    def reset(self) -> None:
        with self._connection.transaction():
            self._connection.execute("UPDATE balances SET total = 0, updated_at = now()")
            self._connection.execute("TRUNCATE processed_events")
```

- Lines 58 to 66: `PostgresBalanceReader.read_balances` runs `SELECT account_id, total FROM balances` and `.fetchall()` returns all rows as tuples. The dictionary comprehension builds `{account: Decimal(total)}`; `Decimal(total)` is safe because the column returns exact decimals.
- Lines 69 to 78: `PostgresLedgerAdmin.reset` runs, in one transaction, `UPDATE balances SET total = 0, updated_at = now()` and `TRUNCATE processed_events` (delete every row at once). The crash test calls it between runs.

### `expected_file.py`: the expected totals on disk

```python
"""Adapter: the expected totals the producer writes and the checker reads (a JSON file)."""

from __future__ import annotations

import json
from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path


def save_expected(path: Path, totals: Mapping[str, Decimal]) -> None:
    """Write the expected totals as JSON."""
    path.write_text(json.dumps({account: str(total) for account, total in totals.items()}, indent=1))


def load_expected(path: Path) -> dict[str, Decimal]:
    """Read the expected totals written by `save_expected`."""
    return {account: Decimal(total) for account, total in json.loads(path.read_text()).items()}


class JsonExpectedStore:
    """Implements `ExpectedTotalsStore` with a JSON file."""

    def __init__(self, path: Path) -> None:
        """Remember where to write."""
        self._path = path

    def save(self, totals: Mapping[str, Decimal]) -> None:
        """Write the totals to the file."""
        save_expected(self._path, totals)
```

- Line 8: `Path` from `pathlib` represents a file location as an object with methods.
- Lines 11 to 13: `save_expected` writes a JSON object from account to total. `str(total)` writes the exact decimal text (JSON has no decimal type, so text keeps the cents exact); `indent=1` makes the file readable; `path.write_text(...)` creates or replaces the file.
- Lines 16 to 18: `load_expected` reads it back; `path.read_text()` gives the text, `json.loads` parses it, and `Decimal(total)` turns each text back into an exact number.
- Lines 21 to 30: `JsonExpectedStore` is the adapter for the `ExpectedTotalsStore` port: it remembers a path and its `save` calls `save_expected`.
- The producer writes the file and the checker reads it later, even in a separate program, so the check does not depend on the producer still running.

### `subprocess_runner.py`: a real crash needs a real process

A **process** is a running program. Killing a process is different from raising an error inside it: nothing gets to clean up.

```python
"""Adapter: run the idempotent consumer as a real process, so a crash is a real crash."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


class SubprocessConsumerRunner:
    """Implements `ConsumerRunner`. Output goes to a log file, not the terminal."""

    def __init__(self, log_path: Path, environment: dict[str, str] | None = None) -> None:
        self._log_path = log_path
        self._environment = {**os.environ, **(environment or {})}

    def run(self, group_id: str, kill_at: int | None, commit_every: int) -> int:
        command = [
            sys.executable, "-m", "idempotent_consumer.interfaces.consume_cli",
            "--mode", "idempotent", "--group", group_id, "--commit-every", str(commit_every),
        ]  # fmt: skip
        command += ["--kill-at", str(kill_at)] if kill_at else ["--until-idle", "5"]
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        with self._log_path.open("a") as log:
            return subprocess.run(command, env=self._environment, stdout=log, stderr=log, check=False).returncode
```

- Line 6: `subprocess` starts other programs. Line 7: `sys.executable` is the path of the Python that is running now.
- Lines 14 to 16: the constructor remembers where to write the log and builds the child's environment: `{**os.environ, **(environment or {})}` copies the current environment and lets the given values override it (`**` spreads a dictionary into another).
- Lines 19 to 22: the command is a list of words: Python, `-m idempotent_consumer.interfaces.consume_cli`, `--mode idempotent`, the group, and `--commit-every N`. `# fmt: skip` tells the formatter to leave the layout of this statement as written.
- Line 23: `command += [...] if kill_at else [...]` adds either `--kill-at N` (die at record N) or `--until-idle 5` (a clean finish when no record has come for 5 seconds).
- Lines 24 to 26: make the log folder (`parents=True, exist_ok=True` means create missing parents and no error if present), open the log file for appending (`"a"`), and run the command with both output streams sent to the file. `check=False` means a non-zero exit code is returned, not raised. `.returncode` is the exit code: the crash test needs to see the killed consumer's exit code (1).

### `health_probes.py`: are the dependencies usable?

```python
"""Adapters: is Kafka reachable with the payments topics, and is PostgreSQL reachable with the schema?"""

from __future__ import annotations

from typing import Any

import psycopg
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from ..application.health import ProbeFailedError
from .retry import retry
from .settings import KafkaSettings, PostgresSettings


class KafkaTopicsProbe:
    """Health probe: the broker answers and the payments topics exist."""

    name = "kafka"

    def __init__(self, settings: KafkaSettings) -> None:
        self._settings = settings

    def check(self) -> str:
        try:
            missing = retry(self._missing_topics, attempts=3, base_delay_seconds=0.2, retry_on=(KafkaError,))
        except KafkaError as error:
            raise ProbeFailedError(f"cannot reach {self._settings.bootstrap_servers}: {error}") from error
        if missing:
            raise ProbeFailedError(f"missing topics: {', '.join(missing)}")
        return f"topics {self._settings.topic} and {self._settings.dlq_topic} exist"
```

- Line 19: `name = "kafka"` is a class attribute: every probe object carries it.
- Lines 24 to 28: `check` calls `retry(self._missing_topics, attempts=3, ...)` so a broker that is only slow is tried three times. If a `KafkaError` still escapes, it is wrapped in `ProbeFailedError` with the address in the message (`from error` keeps the cause).
- Lines 29 and 30: if any topic is missing, fail with `missing topics: ...` (`', '.join(missing)` glues the names).
- Line 31: on success the detail names both topics.

```python
    def _missing_topics(self) -> list[str]:
        consumer: Any = KafkaConsumer(
            bootstrap_servers=self._settings.bootstrap_servers,
            enable_auto_commit=False,
            request_timeout_ms=self._settings.request_timeout_ms,
        )
        try:
            return [t for t in (self._settings.topic, self._settings.dlq_topic) if not consumer.partitions_for_topic(t)]
        finally:
            consumer.close(autocommit=False)
```

- Lines 34 to 38: builds a consumer without a group (so it joins nothing) and with auto-commit off. Line 40: the list comprehension keeps the topics for which `consumer.partitions_for_topic(t)` is empty or `None` (the topic does not exist). Lines 41 and 42: `finally: consumer.close(autocommit=False)` always closes the connection, even if an error was raised.

```python
class PostgresSchemaProbe:
    """Health probe: PostgreSQL answers and the balances table exists."""

    name = "postgres"

    def __init__(self, settings: PostgresSettings) -> None:
        self._settings = settings

    def check(self) -> str:
        try:
            with psycopg.connect(
                host=self._settings.host,
                port=self._settings.port,
                dbname=self._settings.database,
                user=self._settings.user,
                password=self._settings.password,
                connect_timeout=self._settings.connect_timeout_seconds,
            ) as connection:
                row = connection.execute("SELECT to_regclass('balances') IS NOT NULL").fetchone()
        except psycopg.Error as error:
            raise ProbeFailedError(f"cannot query {self._settings.host}:{self._settings.port}: {error}") from error
        if not row or not row[0]:
            raise ProbeFailedError("table balances does not exist (run make db)")
        return "table balances exists"
```

- Lines 53 to 63: `with psycopg.connect(...) as connection:` opens a connection that closes at the end of the block. `connection.execute("SELECT to_regclass('balances') IS NOT NULL").fetchone()` asks PostgreSQL whether a table named `balances` exists: `to_regclass` returns the table's identity or NULL if there is no such table, and `IS NOT NULL` turns that into true or false. `.fetchone()` returns one row.
- Lines 64 and 65: `psycopg.Error` is the library's base error; it becomes `ProbeFailedError("cannot query host:port: ...")`.
- Lines 66 and 67: if the row is missing or false, the message says the table does not exist and tells the reader to run `make db`.
- Line 68: on success, `table balances exists`.

### `logging.py`: one line per event

**Logging** is a program writing a line of text about what it is doing. **Logfmt** is a simple format: `key=value key=value ...` on one line. See `tech/logfmt.md`.

```python
"""Structured logging: one logfmt line per event (`ts=... level=... logger=... msg=... key=value`)."""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any, TextIO


def _quote(value: object) -> str:
    text = str(value)
    return json.dumps(text) if not text or any(char in text for char in ' ="\n') else text
```

- Line 5: `json` is used only to quote text safely (below).
- Line 6: `import logging` is Python's own logging module. The file is also *named* `logging.py` inside this package; because the import is absolute (`import logging`, not `from . import logging`) Python loads the standard module, not this file.
- Line 7: `sys` gives access to `sys.stderr`, the stream where programs write error and diagnostic messages. Standard output (stdout) is kept for results.
- Line 8: `datetime` and `UTC` (the time zone with no offset) for the timestamp.
- Line 9: `Any` means "any type"; `TextIO` means "an open text stream".
- Lines 12 to 14: `_quote` turns any value into text. If the text is empty or contains a space, an equals sign, a double quote or a newline, `json.dumps(text)` wraps it in double quotes and escapes special characters. Otherwise it is returned as is. The result: values with spaces still parse as one value.

```python
class LogfmtFormatter(logging.Formatter):
    """Format a log record as one logfmt line, with the record's extra fields as key=value pairs."""

    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(record.created, tz=UTC).isoformat(timespec="milliseconds")
        parts = [
            f"ts={timestamp}",
            f"level={record.levelname}",
            f"logger={record.name}",
            f"msg={_quote(record.getMessage())}",
        ]
        parts.extend(f"{key}={_quote(value)}" for key, value in getattr(record, "fields", {}).items())
        if record.exc_info:
            parts.append(f"exc={_quote(self.formatException(record.exc_info))}")
        return " ".join(parts)
```

- Line 17: `LogfmtFormatter(logging.Formatter)` is a class built on the standard `Formatter`, which decides how a log record becomes text. Here the method `format` is replaced.
- Line 21: `record.created` is the time the record was made, in seconds since 1970. `datetime.fromtimestamp(..., tz=UTC)` makes a date-time in UTC. `.isoformat(timespec="milliseconds")` writes it like `2026-10-26T12:34:56.789+00:00`.
- Lines 22 to 27: a list of four `key=value` strings: time, level (`INFO`, `WARNING`...), the logger's name (which module wrote it), and the message with `record.getMessage()`.
- Line 28: `parts.extend(...)` adds more items from a generator expression. `getattr(record, "fields", {})` reads an attribute named `fields` from the record and returns `{}` if it is absent. Each pair becomes `key=value`.
- Lines 29 and 30: if the log call came from inside an error handler, `record.exc_info` is set; then the formatted traceback is added as `exc=...`, quoted so it stays on one line.
- Line 31: `" ".join(parts)` glues the pieces together with single spaces.

```python
def fields(**values: Any) -> dict[str, Any]:
    """Use as `logger.info("message", extra=fields(order_id=...))` to attach key=value pairs."""
    return {"fields": values}


def configure_logging(level: str, stream: TextIO | None = None) -> None:
    """Send every log record to `stream` (stderr by default) as logfmt. Safe to call twice."""
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setFormatter(LogfmtFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    logging.getLogger("kafka").setLevel(max(logging.getLevelName(level), logging.WARNING))
```

- Lines 34 to 36: `fields(**values)`. The `**values` means "collect every keyword argument into a dictionary named `values`". `fields(order_id="x", count=3)` returns `{"fields": {"order_id": "x", "count": 3}}`. The standard logger accepts an `extra=` dictionary and copies its keys onto the record as attributes, so the record gets an attribute named `fields`, which the formatter reads on line 28.
- Lines 39 to 46: `configure_logging(level, stream=None)`. `logging.StreamHandler(stream or sys.stderr)` is a handler that writes records to a stream; `stream or sys.stderr` uses the given stream or, when `None`, stderr. `handler.setFormatter(...)` attaches the formatter. `root = logging.getLogger()` is the **root logger**, the parent of all loggers. `root.handlers[:] = [handler]` replaces its list of handlers in place, so calling the function twice does not print every line twice ("safe to call twice"). `root.setLevel(level)` drops records below the level.
- Line 46: the library `kafka-python` logs a lot at INFO level. `logging.getLogger("kafka").setLevel(max(logging.getLevelName(level), logging.WARNING))` makes it at least `WARNING`. `getLevelName("INFO")` returns the number 20; `max` keeps the higher of that and `WARNING`'s 30. So the library is quiet unless it has a problem, whatever level the programs use.

### `retry.py`: trying again, politely

```python
"""Retry with exponential backoff and jitter for calls that cross the network."""

from __future__ import annotations

import random
import time
from collections.abc import Callable


def retry[T](
    operation: Callable[[], T],
    *,
    attempts: int,
    base_delay_seconds: float,
    retry_on: tuple[type[BaseException], ...],
    sleep: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
    max_delay_seconds: float = 10.0,
    on_retry: Callable[[int, BaseException], None] | None = None,
) -> T:
```

- Line 5: `random` for jitter. Line 6: `time` for `time.sleep`. Line 7: `Callable`.
- Line 10: `def retry[T](...)`. This is **new syntax in Python 3.12**: the `[T]` after the name declares a **type variable**. It says "whatever type `operation` returns, `retry` returns the same type". It lets `mypy` know that retrying a function that returns `int` gives an `int`.
- Line 11: `operation: Callable[[], T]` is a function that takes nothing and returns a `T`.
- Line 12: the `*` makes all following parameters keyword-only.
- Lines 13 to 19: `attempts` (how many tries), `base_delay_seconds` (first pause), `retry_on` (a tuple of error classes that count as retryable; `type[BaseException]` is "a class that is an error"), `sleep` (default `time.sleep`; a test passes a fake), `rng` (random generator or `None`), `max_delay_seconds` (the cap, default 10), `on_retry` (an optional function called with the attempt number and the error, used for logging).

```python
    """Call `operation` up to `attempts` times, doubling the pause after each failure in `retry_on`.

    The pause is `base * 2**(attempt - 1)` capped at `max_delay_seconds`, then scaled by a random
    factor in [0.5, 1.0] so many clients do not retry in step. The last failure is raised.
    """
    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    jitter = rng or random.Random()
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except retry_on as error:
            if attempt == attempts:
                raise
            if on_retry:
                on_retry(attempt, error)
            sleep(min(max_delay_seconds, base_delay_seconds * 2 ** (attempt - 1)) * jitter.uniform(0.5, 1.0))
    raise AssertionError("unreachable")  # pragma: no cover
```

- Lines 21 to 25: the docstring gives the formula: the pause is `base * 2**(attempt - 1)`, capped, then multiplied by a random number between 0.5 and 1.0. `**` is "to the power of". This is **exponential backoff** (the wait doubles after each failure, so a struggling service gets room) with **jitter** (randomness, so many clients that failed together do not all retry in the same instant).
- Lines 26 and 27: refuse `attempts < 1`.
- Line 28: `jitter = rng or random.Random()` uses the given generator or makes one.
- Line 29: `for attempt in range(1, attempts + 1):` runs `attempts` times, numbering from 1.
- Lines 30 and 31: `try: return operation()`. A success returns at once.
- Line 32: `except retry_on as error:` catches only the listed kinds of error; any other error passes straight through to the caller, because retrying a bug does not help.
- Lines 33 and 34: on the last attempt, a bare `raise` re-throws the error just caught, so the caller sees the real last failure.
- Lines 35 and 36: otherwise tell the `on_retry` function.
- Line 37: sleep the computed time. `min(cap, base * 2 ** (attempt - 1))` is the capped pause; `jitter.uniform(0.5, 1.0)` is the random factor.
- Line 38: this line can never run (the loop either returns or raises), but `mypy` cannot prove that. `raise AssertionError("unreachable")` satisfies it, and the comment `# pragma: no cover` tells the coverage tool not to count the line as untested.

### `shutdown.py`: leaving cleanly

A **signal** is a message the operating system sends to a running program. `SIGINT` is sent when you press Ctrl+C in the terminal. `SIGTERM` is the polite "please stop" that `docker stop` sends. By default Python stops on SIGINT with an error and the program may die in the middle of a batch.

```python
"""Graceful shutdown: SIGINT and SIGTERM set a flag the work loops check between units of work."""

from __future__ import annotations

import signal
import threading
from types import FrameType


class ShutdownSignal:
    """A flag that SIGINT and SIGTERM set, which work loops check between units of work."""

    def __init__(self) -> None:
        self._requested = threading.Event()

    def install(self) -> None:
        """Route SIGINT and SIGTERM to `request`, so in-flight work finishes and connections close."""
        for number in (signal.SIGINT, signal.SIGTERM):
            signal.signal(number, self._handle)

    def _handle(self, _number: int, _frame: FrameType | None) -> None:
        self.request()

    def request(self) -> None:
        self._requested.set()

    @property
    def requested(self) -> bool:
        return self._requested.is_set()
```

- Line 5: `signal` is Python's signal module. Line 6: `threading` provides `Event`. Line 7: `FrameType` is a hint name for the "frame" argument handlers receive (the place in the code where the signal arrived).
- Lines 13 and 14: `__init__` is the method that runs when you build an object. `self._requested = threading.Event()` creates a flag: an `Event` is an object with `set()` (raise the flag), `is_set()` (is it raised?) and `wait()`. Events are safe to use from signal handlers and other threads.
- Lines 16 to 19: `install` loops over the two signal numbers and calls `signal.signal(number, self._handle)`, which tells the operating system "when this signal comes, run this function" instead of the default.
- Lines 21 and 22: `_handle` is the function that runs. It ignores both arguments (the underscore prefix says so) and calls `request()`.
- Lines 24 and 25: `request` raises the flag. The programs also call it themselves when a limit is reached.
- Lines 27 to 29: the `requested` property reports whether the flag is raised.

The work loops (`produce_payments`, `consume_payments`) call `stop()` (which here is `lambda: shutdown.requested`) between units of work. So a Ctrl+C lets the current record finish: the producer flushes, and the consumer commits its offsets and leaves its group, instead of dying half-way. The crash test's kill switch is the opposite on purpose: it kills the consumer with no cleanup at all.

## Try it yourself

Settings, with no Kafka or PostgreSQL needed:

```bash
POSTGRES_PASSWORD=demo-not-a-secret .venv/bin/python -c "
from idempotent_consumer.infrastructure.settings import Settings
settings = Settings.from_env()
print(settings.kafka.topic, settings.postgres.port)
print(settings.postgres)
"
```

Expected: `payments 5433`, then a `PostgresSettings(...)` line that shows the host, port, database, user and timeout but **no password**, because that field is hidden from the printed form. Run it without the first line's variable and `Settings.from_env()` stops with `ConfigurationError: POSTGRES_PASSWORD must be set and not empty`.

The idempotence rule on its own, with a fake connection, is shown by the integration tests in `tests/integration/test_postgres_ledger.py` (see `python-tests.md`); they need `make up`.

Retry timing is the same as in the other repositories: `retry(flaky, attempts=5, base_delay_seconds=1, retry_on=(OSError,), sleep=print, rng=random.Random(0))` prints two pauses (about 0.9 and 1.8 seconds) and returns the result.

## Mistakes people make

- Building SQL by pasting values into the text (`f"... {amount} ..."`). Always use placeholders.
- Committing the Kafka offset before the database transaction: a crash in between loses the payment.
- Letting a long `poll` interval pass: processing slower than `max_poll_interval_ms` evicts the consumer and the next commit is rejected.
- Putting a password in code or in a file in the repository: it is read from the environment only, and hidden from `repr`.

## Check yourself

1. What does `ON CONFLICT (event_id) DO NOTHING` do, and how does the code notice it happened? (It inserts nothing for an existing id; `cursor.rowcount` is then 0.)
2. Why is `autocommit=True` used together with `connection.transaction()`? (So that the transaction blocks in the code are the only transaction boundaries.)
3. Why does the dead-letter sink call `.get(timeout=...)`? (It waits for the broker's acknowledgment, so the offset is never committed for a record that was not parked.)
4. What does `field(repr=False)` do for the password? (It leaves the password out of the printed form of the settings object, so logs and error messages never show it.)
5. What does `cursor.rowcount == 0` after the `RECORD_EVENT` insert mean? (The event id already existed (`ON CONFLICT DO NOTHING` inserted nothing), so the payment is a duplicate.)

## Further reading

- Python 3.12 `os` (environment variables): https://docs.python.org/3.12/library/os.html
- Python 3.12 `logging`: https://docs.python.org/3.12/library/logging.html
- Python 3.12 `signal`: https://docs.python.org/3.12/library/signal.html
- Python 3.12 `threading`: https://docs.python.org/3.12/library/threading.html
- Python 3.12 `re`: https://docs.python.org/3.12/library/re.html
- kafka-python documentation (the repository pins version 3.0.12): https://kafka-python.readthedocs.io/en/master/
- psycopg 3: transactions: https://www.psycopg.org/psycopg3/docs/basic/transactions.html
- Python 3.12 `subprocess`: https://docs.python.org/3.12/library/subprocess.html
- Python 3.12 `pathlib`: https://docs.python.org/3.12/library/pathlib.html
