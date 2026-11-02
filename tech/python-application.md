# Python: the application layer, file by file

## What it is

The folder `src/idempotent_consumer/application/` holds the **use cases**: the things the program does, written as plain functions: publish payments, consume payments safely, check balances, run the crash test, check health. A use case does not know about Kafka or PostgreSQL. It only knows **ports**, small descriptions of what it needs from the outside ("something that can apply a payment to the ledger", "something that can commit offsets"). The real Kafka and PostgreSQL code (in `infrastructure/`) and the in-memory fakes used by tests both fit those descriptions.

This layer may import from `domain/` and from the standard library. It may not import `kafka` or `psycopg`. `make lint` checks that.

Words used below, explained where they first appear:

- A **function** is a named piece of code that takes inputs (**parameters**) and gives back an output (its **return value**).
- A **type hint** is a note such as `count: int` saying what kind of value is expected. Python ignores hints while running; `mypy` reads them.
- A **transaction** (in a database) is a group of changes that either all happen or none do. A **commit** makes them permanent. A **rollback** throws them away.
- An **offset** is the number of a record inside one partition of a Kafka topic, counted from 0. A consumer **commits an offset** to tell Kafka "everything before this is done; if I restart, start here".

## Why this repository uses it

The use cases hold the lesson's logic: apply each payment once, commit the offset only after the database commit, park a bad record, count an eviction, run the crash test. Writing them against ports instead of Kafka and PostgreSQL means a test can replay a crash, a duplicate or a rejected commit with in-memory fakes, in milliseconds, and the order of the two commits is visible in one place.

## Where it appears

| File | What it holds |
| --- | --- |
| `application/__init__.py` | Marks the folder as a package. |
| `application/ports.py` | The descriptions (ports) of everything the use cases need from outside, plus `Delivery`, `Outcome` and `CommitRejectedError`. |
| `application/produce_payments.py` | The use case "publish payments, with some malformed ones mixed in, and store the expected totals". |
| `application/consume_payments.py` | The heart of the project: apply each payment once, then commit the offset. |
| `application/check_balances.py` | The use case "compare the database with the expected totals". |
| `application/crash_test.py` | The use case "crash the consumer at many points and check the balances after each restart". |
| `application/health.py` | The use case "ask every dependency if it works". |

## How it works underneath

A **port** is written as a `Protocol` class: a list of method names and signatures. Python does not check at run time that an object has them; `mypy` checks, before the program runs, that whatever is passed where a port is expected has the right methods (so an adapter or a fake needs no "implements" word). A use case is an ordinary function that receives its collaborators as parameters, including the clock, the sleep function and the stop test as small functions (**dependency injection**), so a test passes fakes and a real program passes `time.time` and `time.sleep`. Control flow is plain Python: loops, `try`/`except`, early `return`. The rule that this layer imports only the domain and the standard library is checked by `import-linter`. Here the order of two commits is the whole point: `_handle_batch` applies a record (the database transaction commits inside `ledger.apply`), calls `after_apply`, and only then commits the offset. A crash between the two makes Kafka deliver the record again, and the ledger answers `DUPLICATE`.

## Walkthrough of this repository's use

Every source file of this layer is read below, in the order of the table above. Each block is a piece of the real file, copied exactly, followed by an explanation in plain words.

### `application/__init__.py`

```python
"""Use cases and the ports they need. Depends on the domain only."""
```

Only a docstring. Its presence makes the folder a package, and its sentence states the layer's rule: depends on the domain only.

### `application/ports.py`

A **port** is a description of what a use case needs, written without saying how it is done. The word comes from the "ports and adapters" design: the use cases sit in the middle, the ports are sockets in their wall, and **adapters** in `infrastructure/` are plugs that fit. You can swap plugs without touching the middle.

#### Imports, the error and the outcome

```python
"""The ports the use cases need. Infrastructure adapters implement them; tests use fakes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any, Protocol

from ..domain.dead_letter import DeadLetter
from ..domain.payments import Payment


class CommitRejectedError(Exception):
    """The group no longer lets this member commit offsets, for example after an eviction."""


class Outcome(StrEnum):
    """What happened to one payment."""

    APPLIED = "applied"
    DUPLICATE = "duplicate"
    DEAD_LETTERED = "dead-lettered"


@dataclass(frozen=True, slots=True)
class Delivery:
    """A record as read from the log."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes
```

- Line 5: `Mapping` (a read-only dictionary-like value) and `Sequence` (an ordered collection such as a list or tuple) are hint names.
- Line 8: `StrEnum` is the base for a fixed list of named text values.
- Line 9: `Protocol` is the key import. A class that inherits from `Protocol` describes a **shape**: the methods something must have. Any class with those methods fits, without declaring so (**structural typing**).
- Lines 11 and 12: imports from the domain. The two dots in `..domain` mean "go up one package, then into `domain`".
- Lines 15 and 16: `CommitRejectedError` is the application's own error for "the group no longer lets this member commit". It happens when the consumer took too long between polls and Kafka evicted it from the group (**eviction**); the offsets it tries to commit belong to someone else now.
- Lines 19 to 24: `Outcome` is an enumeration of the three things that can happen to one payment: `APPLIED` (added to the balance), `DUPLICATE` (this event was already applied; nothing was changed) and `DEAD_LETTERED` (unusable; parked). Members are also plain text (`"applied"`, `"duplicate"`, `"dead-lettered"`).
- Lines 27 to 35: `Delivery` is a frozen (read-only) dataclass: a record as read from the log, with its topic, partition, offset, key (`bytes | None`: raw bytes, or no value) and value (raw bytes). `@dataclass` writes the boring methods; `frozen=True` forbids changing it; `slots=True` saves memory.

#### The ports

```python
class PaymentPublisher(Protocol):
    """Sends payment payloads (well-formed or deliberately malformed) to the log."""

    def publish(self, payload: Mapping[str, Any]) -> None:
        """Send one payload, keyed by its account id."""

    def flush(self) -> None:
        """Block until every published payload is acknowledged."""


class ExpectedTotalsStore(Protocol):
    """Where the producer leaves the answer the checker compares the balances with."""

    def save(self, totals: Mapping[str, Decimal]) -> None:
        """Store the expected total of every account."""


class PaymentSource(Protocol):
    """Reads payment records from the log as a member of a consumer group."""

    def poll(self, timeout_seconds: float) -> Sequence[Delivery]:
        """Return the next records, or an empty sequence when none arrived in time."""

    def has_assignment(self) -> bool:
        """Whether the group has given this member any partition yet."""

    def close(self) -> None:
        """Leave the consumer group."""
```

- Lines 38 to 45: `PaymentPublisher`: `publish(payload)` sends one JSON-ready dictionary (the docstring says it is keyed by account id, so one account's payments stay in order); `flush()` blocks until all are acknowledged. The bodies hold only docstrings: a Protocol method describes, it does not run. `Mapping[str, Any]` is "a dictionary from text to anything".
- Lines 48 to 52: `ExpectedTotalsStore.save(totals)`: where the producer leaves the answer the checker compares against later.
- Lines 55 to 65: `PaymentSource`: `poll(timeout_seconds)` returns the next deliveries (or none after waiting); `has_assignment()` says whether the group has given this member any partition yet; `close()` leaves the group.

```python
class OffsetCommitter(Protocol):
    """Commits consumer offsets to the group."""

    def commit(self, next_offsets: Mapping[tuple[str, int], int]) -> None:
        """Commit, per (topic, partition), the offset of the next record to read.

        Raises `CommitRejectedError` when the group refuses the commit.
        """


class DeadLetterSink(Protocol):
    """Parks records that can never be processed."""

    def send(self, letter: DeadLetter) -> None:
        """Park an unprocessable record. Returns only once it is safely stored."""


class BalanceLedger(Protocol):
    """Applies payments to account balances."""

    def apply(self, payment: Payment, delivery: Delivery) -> Outcome:
        """Add the payment to its account in one database transaction.

        Returns APPLIED or DUPLICATE. Raises `PoisonMessageError` for a payment that can never be
        applied (an unknown account); nothing is written in that case.
        """
```

- Lines 68 to 75: `OffsetCommitter.commit(next_offsets)` takes a dictionary whose key is a pair (topic, partition) and whose value is the offset of the **next** record to read, and raises `CommitRejectedError` if the group refuses. "Next record" matters: after processing offset 41 you commit 42.
- Lines 78 to 82: `DeadLetterSink.send(letter)` parks a `DeadLetter`. "Returns only once it is safely stored" is a promise the use case relies on: the offset is committed only after the park has succeeded.
- Lines 85 to 93: `BalanceLedger.apply(payment, delivery)` adds the payment to its account "in one database transaction" and returns `APPLIED` or `DUPLICATE`. For a payment that can never be applied (an unknown account) it raises `PoisonMessageError` and writes nothing.

```python
class BalanceReader(Protocol):
    """Reads every account's balance."""

    def read_balances(self) -> Mapping[str, Decimal]:
        """Every account's balance."""


class LedgerAdmin(Protocol):
    """Resets the ledger between experiments."""

    def reset(self) -> None:
        """Zero every balance and forget every processed event."""


class ConsumerRunner(Protocol):
    """Runs the idempotent consumer as a separate process."""

    def run(self, group_id: str, kill_at: int | None, commit_every: int) -> int:
        """Run one idempotent consumer process to its end and return its exit code."""


class HealthProbe(Protocol):
    """A dependency check that reports a short detail or fails with ProbeFailedError."""

    name: str

    def check(self) -> str:
        """Return a short detail on success; raise `ProbeFailedError` when the dependency is unusable."""
```

- Lines 96 to 100: `BalanceReader.read_balances()` returns every account's balance. Lines 103 to 107: `LedgerAdmin.reset()` zeroes every balance and forgets every processed event (used by the crash test between runs).
- Lines 110 to 114: `ConsumerRunner.run(group_id, kill_at, commit_every)` runs the consumer **as a separate process** and returns its exit code. The crash test needs a separate process because it kills one.
- Lines 117 to 123: `HealthProbe` has a class attribute `name: str` and a method `check()` returning a short detail or raising `ProbeFailedError`.

### `application/produce_payments.py`

```python
"""Use case: publish payments, optionally mixed with malformed events, and record the expected totals."""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass

from ..domain.payments import build_payments, expected_totals, with_poison
from .ports import ExpectedTotalsStore, PaymentPublisher


@dataclass(frozen=True, slots=True)
class ProducePlan:
    """How many payments to send and how many malformed events to mix among them."""

    payments: int = 10_000
    poison: int = 0

    def __post_init__(self) -> None:
        if self.payments < 0 or self.poison < 0:
            raise ValueError(f"invalid produce plan: {self}")


@dataclass(frozen=True, slots=True)
class ProduceResult:
    """What was sent, and whether the run was cut short by a stop request."""

    payments: int
    poison: int
    stopped: bool

```

- Lines 5 to 10: imports. `random` and `Callable` are for hints; the domain supplies `build_payments`, `expected_totals` and `with_poison`; the ports file supplies two protocols.
- Lines 13 to 22: `ProducePlan` is a frozen dataclass with two fields, each with a default: `payments` (default 10,000) and `poison` (default 0). `10_000` is `10000`. `__post_init__` runs right after the object is built and refuses negatives with `ValueError`; `{self}` in the f-string prints the plan so the message shows the bad values.
- Lines 25 to 31: `ProduceResult` records how many payments and poison events were sent and whether the run `stopped` early.

```python
def produce_payments(
    publisher: PaymentPublisher,
    store: ExpectedTotalsStore,
    plan: ProducePlan,
    rng: random.Random,
    stop: Callable[[], bool] = lambda: False,
) -> ProduceResult:
    """Publish the payments, then store the totals they must add up to.

    Malformed events are inserted among the payments and never counted in the totals. When `stop()`
    turns true (a shutdown signal) the run ends after flushing what was sent and the totals are not
    stored, since they would describe payments that were never sent.
    """
    payments = build_payments(plan.payments, rng)
    payloads = with_poison([payment.payload() for payment in payments], plan.poison, rng)
    sent = 0
    for payload in payloads:
        if stop():
            break
        publisher.publish(payload)
        sent += 1
    publisher.flush()
    stopped = sent < len(payloads)
    if payments and not stopped:
        store.save(expected_totals(payments))
    return ProduceResult(len(payments), plan.poison, stopped)
```

- Lines 34 to 40: the function's inputs: the publisher, the store for expected totals, the plan, a random-number generator, and `stop`, a function answering "should we stop?" with a default `lambda: False`. A **lambda** is a nameless one-line function; this one always answers `False`.
- Line 47: build the payments (seeded generator, so the same every time for the same seed).
- Line 48: `[payment.payload() for payment in payments]` is a **list comprehension**: a list of the payments' dictionaries. `with_poison` inserts malformed payloads among them.
- Lines 49 to 54: loop over the payloads. Before each, `if stop(): break` leaves the loop if a shutdown was requested; otherwise publish it and add one to `sent`.
- Line 55: after the loop, `publisher.flush()` waits until everything sent is acknowledged by Kafka.
- Line 56: `stopped = sent < len(payloads)`: the run was cut short if fewer were sent than planned.
- Lines 57 and 58: only if there are payments and the run was *not* cut short, save the expected totals. The docstring explains: totals of a run that was cut short would describe payments that were never sent. Poison events are never part of the totals, because they must not change any balance.
- Line 59: return the result.

### `application/consume_payments.py`: the heart of the project

The promise: **every payment changes a balance exactly once**, even though Kafka delivers at-least-once (a record may come again after a crash, a rebalance or a retry). Two mechanisms make that true: the database refuses to apply an event id twice (in `infrastructure/postgres_ledger.py`), and the offset is committed to Kafka only **after** the database commit. If the consumer dies between the two, the record comes again and the database says "duplicate".

#### The plan and the counters

```python
"""Use case: keep balances correct from a stream of payments that may arrive more than once."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from ..domain.dead_letter import DeadLetter
from ..domain.errors import PoisonMessageError
from ..domain.payments import parse_payment
from .ports import (
    BalanceLedger,
    CommitRejectedError,
    DeadLetterSink,
    Delivery,
    OffsetCommitter,
    Outcome,
    PaymentSource,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ConsumePlan:
    """How the consumer runs: commit interval, artificial work, and when to stop."""

    commit_every: int = 1  # 1 commits after every record, the lesson's shape
    work_delay_seconds: float = 0.0  # artificial work per record
    idle_timeout_seconds: float | None = None  # stop after this long without a record (after assignment)
    run_seconds: float | None = None  # stop after this long in total
    poll_seconds: float = 1.0

    def __post_init__(self) -> None:
        if self.commit_every < 1 or self.work_delay_seconds < 0 or self.poll_seconds <= 0:
            raise ValueError(f"invalid consume plan: {self}")


@dataclass
class ConsumeCounts:
    """What the consumer did: payments applied, skipped as duplicates, dead-lettered, and evictions."""

    applied: int = 0
    duplicate: int = 0
    dead_lettered: int = 0
    evictions: int = 0
    processed: int = field(default=0, repr=False)

    def summary(self) -> dict[str, int]:
        return {
            "applied": self.applied,
            "duplicate": self.duplicate,
            "dead-lettered": self.dead_lettered,
            "evictions": self.evictions,
        }
```

- Line 5: `logging` is Python's logging module. Line 22: `logger = logging.getLogger(__name__)` gets a logger named after this module (`__name__` holds the module's dotted name), so log lines say where they came from.
- Line 7: `field` is used below to configure one dataclass field.
- Lines 12 to 20: imports from the ports file, in a bracketed list over several lines.
- Lines 25 to 37: `ConsumePlan`, a frozen dataclass. Each field has a default and a `#` comment (ignored by Python) saying what it does: `commit_every` (commit offsets after this many records; 1 means after every record, "the lesson's shape"), `work_delay_seconds` (artificial work per record, to make a slow consumer), `idle_timeout_seconds` (stop after this long without a record, counted only after the group has assigned partitions), `run_seconds` (stop after this long in total), `poll_seconds` (how long one poll waits). The check in `__post_init__` refuses a `commit_every` under 1, a negative delay or a poll time of 0 or less.
- Lines 40 to 56: `ConsumeCounts` is a *changeable* dataclass (plain `@dataclass`, because the numbers grow). Fields: `applied`, `duplicate`, `dead_lettered`, `evictions` and `processed`, each starting at 0. `field(default=0, repr=False)` gives `processed` a default and leaves it out of the automatic printout. `summary()` returns the four counters as a dictionary for printing.

#### The main loop

```python
def consume_payments(
    source: PaymentSource,
    committer: OffsetCommitter,
    ledger: BalanceLedger,
    dead_letters: DeadLetterSink,
    plan: ConsumePlan,
    *,
    stop: Callable[[], bool],
    clock: Callable[[], float],
    sleep: Callable[[float], None],
    after_apply: Callable[[int], None] = lambda _processed: None,
) -> ConsumeCounts:
    """Poll, apply each payment in one transaction, and commit offsets only after the database commit.

    `after_apply(n)` runs after the n-th record's database commit and before its offset commit, which
    is where the crash test injects its kill switch.
    """
    counts = ConsumeCounts()
    started = last_record_at = clock()
    while not stop():
        deliveries = source.poll(plan.poll_seconds)
        if deliveries or not source.has_assignment():
            # The idle clock only runs once the group has given this member partitions: after a crash
            # the coordinator holds the dead member's partitions until its session expires.
            last_record_at = clock()
        _handle_batch(deliveries, committer, ledger, dead_letters, plan, counts, sleep, after_apply)
        if plan.run_seconds is not None and clock() - started > plan.run_seconds:
            break
        if (
            plan.idle_timeout_seconds is not None
            and not deliveries
            and clock() - last_record_at > plan.idle_timeout_seconds
        ):
            break
    return counts
```

- Lines 59 to 70: the inputs are the five collaborators (source, committer, ledger, dead-letter sink, plan). The lone `*` on line 65 makes the parameters after it **keyword-only**: callers must write `stop=...`, `clock=...`. `stop` (should we end?), `clock` (the time in seconds), `sleep` (wait) are passed in so tests can fake time. `after_apply` is a function called after each record's database commit and before its offset commit; its default does nothing.
- Lines 73 and 74: the docstring says why `after_apply` exists: the crash test uses it to inject a kill switch at exactly the dangerous moment.
- Line 76: a fresh counter object.
- Line 77: `started = last_record_at = clock()` assigns the same value to two names.
- Line 78: `while not stop():` repeats until a stop is requested.
- Line 79: `source.poll(plan.poll_seconds)` returns the next records or an empty list.
- Lines 80 to 83: `if deliveries or not source.has_assignment():` restart the idle clock when records came **or** when the group has not yet given this member any partition. The comment explains: after a crash the group coordinator keeps the dead member's partitions until its session expires, so a restarted consumer may wait for them; idle time must not be counted before it has been given any.
- Line 84: `_handle_batch(...)` processes the batch (below).
- Lines 85 and 86: `break` if the total run time passed `run_seconds`.
- Lines 87 to 92: `break` if an idle timeout is set, no records came this time, and more than that long has passed since the last record.
- Line 93: return the counts.

#### Handling one batch

```python
def _handle_batch(
    deliveries: Sequence[Delivery],
    committer: OffsetCommitter,
    ledger: BalanceLedger,
    dead_letters: DeadLetterSink,
    plan: ConsumePlan,
    counts: ConsumeCounts,
    sleep: Callable[[float], None],
    after_apply: Callable[[int], None],
) -> None:
    pending: dict[tuple[str, int], int] = {}
    since_commit = 0
    for delivery in deliveries:
        outcome = _apply(delivery, ledger, dead_letters)
        _count(counts, outcome)
        after_apply(counts.processed)
        pending[(delivery.topic, delivery.partition)] = delivery.offset + 1
        since_commit += 1
        if plan.work_delay_seconds:
            sleep(plan.work_delay_seconds)
        if since_commit >= plan.commit_every:
            if not _commit(committer, pending, counts):
                return  # the rest of this batch belongs to whoever owns the partitions now
            pending, since_commit = {}, 0
    if pending:
        _commit(committer, pending, counts)
```

A **batch** is the list of records one `poll` returned. A leading underscore in `_handle_batch` means "private to this file".

- Line 106: `pending` is a dictionary of the offsets to commit: key (topic, partition), value the next offset. Line 107: `since_commit` counts records since the last commit.
- Line 108: loop over the records in order.
- Line 109: `_apply(...)` parses and applies the record and returns its `Outcome`.
- Lines 110 and 111: count the outcome, then call `after_apply(counts.processed)`. At this moment the database change is **committed** but the offset is **not**. If the process is killed right here, Kafka will deliver this record again on restart, and the ledger will report `DUPLICATE`. This is exactly the risk the project tests.
- Line 112: remember that, for this partition, the next offset to read is this one plus 1.
- Lines 113 to 115: count it, and if an artificial work delay is set, sleep.
- Lines 116 to 119: when `commit_every` records have been handled, commit what is pending. `_commit` returns `False` if the group refused (the member was evicted). In that case `return` leaves the function at once: the remaining records of the batch now belong to whoever owns the partitions, and their database changes have not been made. Otherwise `pending, since_commit = {}, 0` resets both together (a **tuple assignment**: two names, two values).
- Lines 120 and 121: at the end of the batch, if records are still waiting for a commit (the batch ended before `commit_every` was reached), commit them.

#### Applying one record

```python
def _apply(delivery: Delivery, ledger: BalanceLedger, dead_letters: DeadLetterSink) -> Outcome:
    try:
        return ledger.apply(parse_payment(delivery.value), delivery)
    except PoisonMessageError as error:
        dead_letters.send(DeadLetter(delivery.topic, delivery.partition, delivery.offset, str(error), delivery.value))
        logger.warning(
            "dead-lettered",
            extra={
                "fields": {
                    "topic": delivery.topic,
                    "partition": delivery.partition,
                    "offset": delivery.offset,
                    "error": error,
                }
            },
        )
        return Outcome.DEAD_LETTERED
```

- Line 126: `ledger.apply(parse_payment(delivery.value), delivery)`. First the domain validates the bytes (`parse_payment` raises `PoisonMessageError` if unusable), then the ledger applies the payment in one transaction and returns `APPLIED` or `DUPLICATE`.
- Lines 127 and 128: `except PoisonMessageError as error:` catches an unusable record, whether invalid on arrival or refused by the ledger (an unknown account). A `DeadLetter` is built with the record's original topic, partition and offset, the reason `str(error)` and the raw bytes, and sent to the dead-letter sink. Because `send` returns only when the letter is safely stored, and the offset is committed later, a poison record is never lost: it is either parked or will be delivered again.
- Lines 129 to 139: logs a warning. `extra={"fields": {...}}` attaches the key-value pairs that the logfmt formatter prints (see `python-infrastructure.md`).
- Line 140: returns `DEAD_LETTERED`.

#### Counting and committing

```python
def _count(counts: ConsumeCounts, outcome: Outcome) -> None:
    counts.processed += 1
    if outcome is Outcome.APPLIED:
        counts.applied += 1
    elif outcome is Outcome.DUPLICATE:
        counts.duplicate += 1
    else:
        counts.dead_lettered += 1


def _commit(committer: OffsetCommitter, pending: dict[tuple[str, int], int], counts: ConsumeCounts) -> bool:
    """Commit; False when the group refused (the member was evicted)."""
    try:
        committer.commit(dict(pending))
    except CommitRejectedError as error:
        counts.evictions += 1
        logger.warning(
            "EVICTED: offset commit rejected; rejoining on the next poll", extra={"fields": {"error": error}}
        )
        return False
    return True
```

- Lines 143 to 150: `_count` adds one to `processed`, then to the counter that matches the outcome. `is` compares identity, the right test for enum members. `elif` means "otherwise, if"; `else` catches the last case.
- Lines 153 to 163: `_commit` calls `committer.commit(dict(pending))` (a copy, so later changes to `pending` cannot affect the call). On `CommitRejectedError` it counts an eviction, logs `EVICTED: offset commit rejected; rejoining on the next poll` and returns `False`; otherwise `True`. The next `poll` makes the client rejoin the group.

### `application/check_balances.py`

```python
"""Use case: compare the balance table with the totals it must have."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal

from ..domain.balances import Mismatch, diff_balances
from .ports import BalanceReader


def check_balances(reader: BalanceReader, expected: Mapping[str, Decimal]) -> list[Mismatch]:
    """The accounts whose balance differs from the expected total."""
    return diff_balances(expected, reader.read_balances())
```

One line of work (line 14): read every balance through the port and let the domain compare it with the expected totals. It returns the list of mismatches; an empty list means everything is right.

### `application/crash_test.py`

```python
"""Use case: crash the idempotent consumer at many points and check the balances after each restart."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal

from ..domain.balances import Mismatch
from ..domain.crash_plan import kill_points
from .check_balances import check_balances
from .ports import BalanceReader, ConsumerRunner, LedgerAdmin


@dataclass(frozen=True, slots=True)
class CrashRun:
    """The result of one crash-and-restart run."""

    number: int
    kill_at: int
    crash_exit_code: int
    restart_exit_code: int
    mismatches: tuple[Mismatch, ...]

    @property
    def passed(self) -> bool:
        """The consumer crashed (exit 1), restarted cleanly (exit 0) and every balance is right."""
        return self.crash_exit_code == 1 and self.restart_exit_code == 0 and not self.mismatches

```

- Lines 15 to 28: `CrashRun` is a frozen dataclass: the run's number, the record at which the consumer was killed, the exit code of the crashed process, the exit code of the restarted process, and a tuple of mismatches. The `passed` property (line 25, `@property`, so you write `run.passed` without brackets) is true only when the crashed consumer exited with code 1 (it was killed on purpose), the restarted one exited with 0 (a clean finish) and there are no mismatches.

```python
def run_crash_test(
    runner: ConsumerRunner,
    admin: LedgerAdmin,
    reader: BalanceReader,
    expected: Mapping[str, Decimal],
    *,
    runs: int,
    payments: int,
    commit_every: int,
    group_for_run: Callable[[int], str],
    on_run: Callable[[CrashRun], None] = lambda _run: None,
) -> list[CrashRun]:
    """Each run: zero the tables, crash a fresh group at a different record, restart it, compare."""
    results = []
    for number, kill_at in enumerate(kill_points(runs, payments), start=1):
        admin.reset()
        group = group_for_run(number)
        crash_code = runner.run(group, kill_at, commit_every)
        restart_code = runner.run(group, None, commit_every)
        run = CrashRun(number, kill_at, crash_code, restart_code, tuple(check_balances(reader, expected)))
        results.append(run)
        on_run(run)
    return results
```

- Lines 31 to 42: the inputs: the process runner, the ledger admin, the balance reader, the expected totals, then (keyword-only after the `*`) the number of runs, the number of payments, `commit_every`, a function that gives each run its own consumer group name, and an optional `on_run` callback that is told about each finished run (the command-line program prints a line there).
- Line 45: `for number, kill_at in enumerate(kill_points(runs, payments), start=1):` loops over the kill positions from the domain, numbering the runs from 1. `enumerate` yields (counter, item) pairs.
- Line 46: `admin.reset()` zeroes the tables so every run starts clean.
- Line 47: a fresh group per run, so the group has no committed offsets and starts at the beginning of the topic.
- Line 48: run a consumer that is killed at record `kill_at`. Line 49: run the same group again with no kill switch; it resumes from the last committed offset, re-reading some records the dead one had already applied.
- Line 50: compare the balances with the expected totals and record the result. Lines 51 and 52: keep it and report it.
- Line 53: return all runs. The caller prints a verdict.

### `application/health.py`

```python
"""Use case: ask every dependency whether it is usable."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from .ports import HealthProbe


class ProbeFailedError(Exception):
    """A dependency is not usable; the message says why."""


@dataclass(frozen=True, slots=True)
class ProbeResult:
    """The outcome of one health probe."""

    name: str
    ok: bool
    detail: str


@dataclass(frozen=True, slots=True)
class HealthReport:
    """The outcomes of every probe; healthy only when all of them are."""

    results: tuple[ProbeResult, ...]

    @property
    def healthy(self) -> bool:
        return all(result.ok for result in self.results)


def check_health(probes: Iterable[HealthProbe]) -> HealthReport:
    """Run every probe and collect the outcomes; a failed probe is reported, not raised."""
    results = []
    for probe in probes:
        try:
            results.append(ProbeResult(probe.name, True, probe.check()))
        except ProbeFailedError as error:
            results.append(ProbeResult(probe.name, False, str(error)))
    return HealthReport(tuple(results))
```

- Line 11: `ProbeFailedError` is the application's named error for "a dependency is not usable".
- Lines 15 to 21: `ProbeResult` records a probe's name, whether it worked and a detail text.
- Lines 24 to 32: `HealthReport` holds a tuple of results. The `healthy` property uses `all(...)`, which is true only if every item is true (and is true for an empty list).
- Lines 35 to 43: `check_health` loops over the probes. `try:` runs `probe.check()`; if it returns, the result is marked `True` with the detail it returned; if `ProbeFailedError` is raised, the `except` block adds a result marked `False` with the message. A failed probe is **reported, not allowed to crash the check**, so every probe's state is printed. Any other error (a real bug) is not caught and still stops the program.

## Try it yourself

```bash
.venv/bin/python -c "
import random
from decimal import Decimal
from idempotent_consumer.application.produce_payments import ProducePlan, produce_payments

class Publisher:
    def __init__(self): self.sent = []
    def publish(self, payload): self.sent.append(payload)
    def flush(self): pass

class Store:
    def save(self, totals): self.totals = totals

publisher, store = Publisher(), Store()
result = produce_payments(publisher, store, ProducePlan(payments=5, poison=1), random.Random(3))
print(result)
print(len(publisher.sent), 'payloads sent;', sum(store.totals.values()), 'is the sum of the expected totals')
"
```

Expected: a `ProduceResult` with `payments=5, poison=1, stopped=False`; `6 payloads sent`; and a sum that adds up only the five valid payments. The `Publisher` and `Store` classes list only the methods of the two protocols and never say they implement them.

## Mistakes people make

- Committing the offset before the database commit: a crash in between loses the payment. This repository commits the offset last.
- Treating a poison message like an ordinary failure and retrying it forever: the consumer would never move on. It is parked once and skipped.
- Changing a balance without a way to recognise a repeat: then a redelivery double-counts. The duplicate check lives in the ledger's transaction.

## Check yourself

1. Why is the offset committed after the database commit? (A crash in between causes redelivery, which the ledger recognises; the reverse order would lose payments.)
2. What does `after_apply` exist for? (The crash test's kill switch at the dangerous moment between the two commits.)
3. What happens to the rest of a batch when a commit is rejected? (It is abandoned; it belongs to the new owner of the partitions.)
4. What does the outcome `DUPLICATE` mean? (The event id was already recorded, so nothing was changed.)
5. Besides arriving records, when does the consumer restart its idle clock? (While the member has no partition assignment, because after a crash the group may hold the dead member's partitions until its session expires.)

## Further reading

- PEP 544, Protocols (structural typing): https://peps.python.org/pep-0544/
- Python 3.12 `typing`: https://docs.python.org/3.12/library/typing.html
- Python 3.12 `dataclasses`: https://docs.python.org/3.12/library/dataclasses.html
- Python 3.12 `logging`: https://docs.python.org/3.12/library/logging.html
