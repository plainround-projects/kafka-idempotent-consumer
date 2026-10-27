# Python: the tests, file by file

## What this note covers

Everything under `tests/` is Python written to check the rest of the code. This note walks through **every test file and every test**. How the tests are *started* (the `pytest` command, markers, coverage numbers) is in `tech/pytest.md` and `tech/pytest-cov.md`; here the focus is on what each test proves and how it is written.

Ideas you need first:

- A **test** is a function whose name starts with `test_`. pytest finds such functions in files whose names start with `test_`, calls each one, and marks it passed if it returns without an error and failed if an error is raised.
- **`assert condition`** raises an error if the condition is false. That is how a test says "this must be true". pytest rewrites assertions so a failure shows both sides of `==`.
- **`with pytest.raises(SomeError):`** says "the lines inside must raise this error; the test fails if they do not". `match="text"` also requires the error message to contain that text (it is a regular expression).
- A **fake** (or stand-in) is a simple object that plays the role of something real so the test needs no network. The fakes live in `tests/unit/application/fakes.py`.
- **Unit tests** (in `tests/unit/`) need neither Kafka nor PostgreSQL and run in well under a second. **Integration tests** (`tests/integration/`) use the real Kafka broker and PostgreSQL from `docker-compose.yml`. **End-to-end tests** (`tests/e2e/`) start the real command-line programs as separate processes.
- The folders of tests are packages (each has an empty `__init__.py`), which is why a test can write `from tests.unit.application.fakes import ...`. The eight empty `__init__.py` files under `tests/` contain nothing at all; their presence is the whole job.


## The files

| File | What it checks |
| --- | --- |
| `tests/conftest.py` | Shared fixtures: settings, a clean database connection and fresh topics. |
| `tests/unit/domain/test_payments.py` | Building, validating and totalling payments. |
| `tests/unit/domain/test_dead_letter_balances_and_crash_plan.py` | The dead letter, the balance comparison and the kill points. |
| `tests/unit/application/fakes.py` | The in-memory stand-ins (not a test file itself). |
| `tests/unit/application/test_produce_payments.py` | The produce use case. |
| `tests/unit/application/test_consume_payments.py` | The consume use case: duplicates, poison, commits, eviction, idle rules. |
| `tests/unit/application/test_crash_test_and_health.py` | The crash-test loop, the balance check and the health report. |
| `tests/unit/infrastructure/test_settings.py` | Environment settings, including the password rules. |
| `tests/unit/infrastructure/test_retry.py` | Retry with backoff. |
| `tests/unit/infrastructure/test_logging_and_shutdown.py` | Log format and the signal flag. |
| `tests/unit/infrastructure/test_expected_file_and_runner.py` | The expected-totals file and the subprocess command. |
| `tests/unit/interfaces/test_arguments_and_clis.py` | Argument checks and exit codes. |
| `tests/integration/test_postgres_ledger.py` | The SQL ledgers on a real PostgreSQL. |
| `tests/integration/test_kafka_adapters.py` | The Kafka adapters on a real broker. |
| `tests/e2e/test_lesson_scenarios.py` | The lesson's scenarios through the real programs. |

## `tests/conftest.py`: fixtures

```python
"""Fixtures for the tests that need Kafka and PostgreSQL from docker-compose.yml."""

import pathlib
import uuid
from collections.abc import Iterator
from typing import Any

import psycopg
import pytest
from kafka import KafkaAdminClient
from kafka.admin import NewTopic

from idempotent_consumer.infrastructure import postgres
from idempotent_consumer.infrastructure.settings import Settings

SQL = pathlib.Path(__file__).resolve().parent.parent / "sql"


@pytest.fixture
def settings() -> Settings:
    """From the environment (make loads .env; CI sets the variables); the password has no default."""
    return Settings.from_env()


@pytest.fixture
def connection(settings: Settings) -> Iterator[psycopg.Connection[Any]]:
    """The balances and processed_events tables, created if missing and emptied for this test."""
    with postgres.connect(settings.postgres) as db:
        for script in [SQL / "001_schema.sql", SQL / "002_seed.sql", *sorted((SQL / "migrations").glob("*.sql"))]:
            db.execute(script.read_text())
        db.execute((SQL / "reset.sql").read_text())
        yield db


@pytest.fixture
def topics(settings: Settings) -> Iterator[tuple[str, str]]:
    """A fresh payments topic and dead-letter topic with their own names, removed afterwards."""
    suffix = uuid.uuid4().hex[:8]
    names = (f"test-payments-{suffix}", f"test-payments-dlq-{suffix}")
    admin = KafkaAdminClient(bootstrap_servers=settings.kafka.bootstrap_servers)
    admin.create_topics(
        [
            NewTopic(names[0], num_partitions=6, replication_factor=1),
            NewTopic(names[1], num_partitions=1, replication_factor=1),
        ]
    )
    yield names
    admin.delete_topics(list(names))
    admin.close()
```

**`conftest.py`** is a special file name: pytest loads it automatically and makes the **fixtures** in it available to every test below it. A fixture is a function that prepares something a test needs. A test asks for one by putting its name as a parameter.

- Line 3: `pathlib` gives file paths as objects. Line 4: `uuid` makes random unique ids. Line 8: `psycopg` is the PostgreSQL library. Lines 10 and 11: Kafka's admin client and `NewTopic`, to create and delete topics.
- Line 16: `SQL = pathlib.Path(__file__).resolve().parent.parent / "sql"`. `__file__` is this file's path; `.resolve()` makes it absolute; `.parent.parent` goes up two folders (from `tests/conftest.py` to the repository root); `/ "sql"` appends the folder name. (The `/` operator joins paths.)
- Lines 19 to 22: the `settings` fixture. `@pytest.fixture` turns the function into a fixture. It reads the real environment with `Settings.from_env()`; `make` loads `.env` into the environment and CI sets the variables, and the password has no default, so a missing password fails here with a clear message.
- Lines 25 to 32: the `connection` fixture. It opens a database connection in a `with` block and, for every script in the list (`001_schema.sql`, `002_seed.sql`, then every file under `sql/migrations/` in name order), runs the SQL (`db.execute(script.read_text())`). `*sorted(...)` inside the list spreads the sorted file list into it. Then it runs `reset.sql` to empty the tables. `yield db` hands the connection to the test; when the test ends, pytest resumes the function after `yield`, the `with` block closes the connection. The scripts use `IF NOT EXISTS`, so running them every time is safe.
- Lines 35 to 49: the `topics` fixture. It builds two unique topic names with a random 8-character suffix (`uuid.uuid4().hex[:8]`), creates the payments topic with six partitions and the dead-letter topic with one, `yield`s the pair, and after the test deletes both topics and closes the admin client. Every test gets topics of its own and cannot see another test's records.

## `tests/unit/domain/test_payments.py`

```python
import json
import random
from decimal import Decimal

import pytest

from idempotent_consumer.domain.errors import PoisonMessageError
from idempotent_consumer.domain.payments import (
    ACCOUNTS,
    Payment,
    build_payments,
    expected_totals,
    parse_payment,
    poison_payload,
    with_poison,
)

VALID = {"eventId": "6f1c2b6e-3d7e-4c55-9d52-0d6a4b7a1e11", "accountId": "acct-012", "amount": 120.5}


def raw(**changes: object) -> bytes:
    return json.dumps({**VALID, **changes}).encode()


def test_a_valid_payment_parses_with_an_exact_decimal_amount():
    payment = parse_payment(raw())
    assert payment == Payment(VALID["eventId"], "acct-012", Decimal("120.5"))  # type: ignore[arg-type]


@pytest.mark.parametrize("amount", ["not-a-number", None, True, [], "NaN", "Infinity", 0, -5, 1.234, "12.345"])
def test_a_bad_amount_is_poison(amount):
    with pytest.raises(PoisonMessageError):
        parse_payment(raw(amount=amount))


@pytest.mark.parametrize("value", [b"{broken", b"\xff\xfe", b"[1, 2]", b'"text"', b"null"])
def test_unreadable_or_non_object_payloads_are_poison(value):
    with pytest.raises(PoisonMessageError):
        parse_payment(value)


@pytest.mark.parametrize("changes", [{"eventId": "nope"}, {"eventId": None}, {"accountId": ""}, {"accountId": 7}])
def test_bad_ids_are_poison(changes):
    with pytest.raises(PoisonMessageError):
        parse_payment(raw(**changes))


def test_a_missing_field_is_poison():
    value = {key: item for key, item in VALID.items() if key != "eventId"}
    with pytest.raises(PoisonMessageError):
        parse_payment(json.dumps(value).encode())

```

- Line 18: `VALID` is a valid payment as a dictionary. Lines 21 and 22: the helper `raw(**changes)` builds the JSON bytes of `VALID` with some fields changed. `**changes` collects keyword arguments into a dictionary; `{**VALID, **changes}` merges two dictionaries, the later one winning.
- Lines 25 to 27: a valid payment parses with an exact `Decimal` (`120.5` becomes `Decimal("120.5")`). The comment `# type: ignore[arg-type]` tells `mypy` an argument type is deliberately loose here.
- Lines 30 to 33: `@pytest.mark.parametrize("amount", [...])` runs the same test once per item. Ten bad amounts each must raise `PoisonMessageError`: text that is not a number, `None`, `True`, a list, `"NaN"`, `"Infinity"`, zero, a negative, a number with three decimals (`1.234`) and the text `"12.345"`.
- Lines 36 to 39: five payloads that are not objects or not readable JSON: broken JSON, bytes that are not valid text (`b"\xff\xfe"`), a list, a JSON string and `null`.
- Lines 42 to 45: bad ids: an event id that is not a UUID, `None`, an empty account id and an account id that is a number.
- Lines 48 to 51: a payload missing `eventId` is poison.

```python
def test_the_lessons_poison_event_is_rejected_with_the_reason():
    with pytest.raises(PoisonMessageError, match="not-a-number"):
        parse_payment(json.dumps(poison_payload(random.Random(1))).encode())


def test_payments_are_repeatable_for_a_seed_and_have_unique_ids():
    first = build_payments(500, random.Random(3))
    assert first == build_payments(500, random.Random(3))
    assert len({payment.event_id for payment in first}) == 500
    assert all(payment.account_id in ACCOUNTS and Decimal(1) <= payment.amount <= Decimal(500) for payment in first)


def test_every_generated_payment_survives_a_json_round_trip_and_validation():
    for payment in build_payments(200, random.Random(8)):
        assert parse_payment(json.dumps(payment.payload()).encode()) == payment


def test_expected_totals_add_amounts_exactly_per_account():
    payments = [
        Payment("1", "acct-001", Decimal("0.1")),
        Payment("2", "acct-001", Decimal("0.2")),
        Payment("3", "acct-002", Decimal("5.0")),
    ]
    totals = expected_totals(payments)
    assert totals["acct-001"] == Decimal("0.3")  # with floats this sum is 0.30000000000000004
    assert totals["acct-002"] == Decimal("5.0")
    assert totals["acct-003"] == Decimal(0)
    assert len(totals) == 50


def test_with_poison_spreads_malformed_events_among_the_payments():
    payloads = [payment.payload() for payment in build_payments(10, random.Random(1))]
    mixed = with_poison(payloads, 1, random.Random(2))
    assert len(mixed) == 11
    assert [item for item in mixed if item["amount"] != "not-a-number"] == payloads
    position = next(index for index, item in enumerate(mixed) if item["amount"] == "not-a-number")
    assert 0 < position < len(mixed) - 1  # valid payments before and after it


def test_with_poison_zero_changes_nothing():
    payloads = [payment.payload() for payment in build_payments(5, random.Random(1))]
    assert with_poison(payloads, 0, random.Random(2)) == payloads


def test_payments_are_immutable():
    payment = Payment("1", "acct-001", Decimal(1))
    with pytest.raises(AttributeError):
        payment.amount = Decimal(2)  # type: ignore[misc]
```

- Lines 54 to 56: the lesson's poison event is rejected with the reason `not-a-number` in the message.
- Lines 59 to 63: 500 payments from the same seed are equal to a second build (repeatable); all 500 event ids are different (`len({...})` of a set); every account is one of the 50 and every amount is between 1 and 500.
- Lines 66 to 68: **round trip**: every generated payment survives being turned into JSON and parsed again, equal to the original.
- Lines 71 to 81: `expected_totals` adds exactly: `0.1 + 0.2` gives `Decimal("0.3")` (with floats it is `0.30000000000000004`, as the comment says); an account with no payments is zero; 50 accounts in total.
- Lines 84 to 90: `with_poison` with one poison event gives 11 payloads; removing the poison one leaves the original list in the original order; and its position is strictly inside the list (valid payments before and after it).
- Lines 93 to 95: zero poison changes nothing.
- Lines 98 to 101: payments are immutable; assigning raises `AttributeError`.

## `tests/unit/domain/test_dead_letter_balances_and_crash_plan.py`

```python
import base64
from decimal import Decimal

import pytest

from idempotent_consumer.domain.balances import Mismatch, diff_balances
from idempotent_consumer.domain.crash_plan import kill_points
from idempotent_consumer.domain.dead_letter import DeadLetter


def test_dead_letter_keeps_the_origin_the_error_and_the_exact_bytes():
    original = b'{"eventId": "x", "amount": "not-a-number"}\xff'
    payload = DeadLetter("payments", 3, 5512, "bad amount", original).payload()
    assert (payload["originalTopic"], payload["originalPartition"], payload["originalOffset"]) == ("payments", 3, 5512)
    assert payload["error"] == "bad amount"
    assert base64.b64decode(payload["rawBase64"]) == original  # replayable even though \xff is not text
    assert payload["raw"].startswith('{"eventId"')


def test_equal_balances_have_no_mismatch():
    assert diff_balances({"a": Decimal("1.50")}, {"a": Decimal("1.5")}) == []


def test_a_double_applied_payment_shows_as_off_by_exactly_its_amount():
    mismatches = diff_balances({"acct-017": Decimal("4821.33")}, {"acct-017": Decimal("4903.81")})
    assert mismatches == [Mismatch("acct-017", Decimal("4821.33"), Decimal("4903.81"))]
    assert mismatches[0].off_by == Decimal("82.48")
    assert str(mismatches[0]) == "MISMATCH acct-017: expected 4821.33, got 4903.81 (off by +82.48)"


def test_an_account_missing_on_one_side_counts_as_zero():
    assert diff_balances({"a": Decimal(2)}, {}) == [Mismatch("a", Decimal(2), Decimal(0))]
    assert diff_balances({}, {"b": Decimal(3)}) == [Mismatch("b", Decimal(0), Decimal(3))]


def test_twenty_different_kill_points_inside_the_topic():
    points = kill_points(20, 10_000)
    assert len(set(points)) == 20
    assert all(1 <= point <= 10_000 for point in points)
    assert points == sorted(points)


def test_kill_points_are_the_same_every_time():
    assert kill_points(5, 3_000) == kill_points(5, 3_000)


def test_a_tiny_topic_still_gets_valid_kill_points():
    assert all(1 <= point <= 50 for point in kill_points(5, 50))


@pytest.mark.parametrize(("runs", "payments"), [(0, 10), (3, 0)])
def test_kill_points_need_at_least_one_run_and_one_payment(runs, payments):
    with pytest.raises(ValueError, match="at least 1"):
        kill_points(runs, payments)
```

- Lines 11 to 17: a dead letter keeps its origin (topic, partition 3, offset 5512), the error, and the exact bytes: decoding `rawBase64` gives back the original bytes, including the byte `\xff` that is not valid text, and the readable `raw` still starts with the JSON.
- Lines 20 and 21: `1.50` and `1.5` are equal, so no mismatch (decimal equality ignores trailing zeros).
- Lines 24 to 28: a double-applied payment shows as a mismatch whose `off_by` is exactly the payment's amount (82.48), and its text form is `MISMATCH acct-017: expected 4821.33, got 4903.81 (off by +82.48)`.
- Lines 31 to 33: an account missing on one side counts as zero on that side.
- Lines 36 to 40: the 20 kill points for 10,000 payments are 20 different numbers (`set`), all inside 1 to 10,000, in increasing order.
- Lines 43 to 48: the same inputs give the same output; a tiny topic of 50 payments still gets valid points.
- Lines 51 to 54: parametrized with two names at once, `(runs, payments)` of `(0, 10)` and `(3, 0)`: both raise `ValueError` mentioning "at least 1".

## `tests/unit/application/fakes.py`

```python
"""In-memory stand-ins for the application's ports."""

from collections.abc import Mapping, Sequence
from decimal import Decimal

from idempotent_consumer.application.ports import CommitRejectedError, Delivery, Outcome
from idempotent_consumer.domain.dead_letter import DeadLetter
from idempotent_consumer.domain.errors import PoisonMessageError
from idempotent_consumer.domain.payments import Payment


class ScriptedSource:
    """Returns the scripted batches one per poll, then empty polls forever."""

    def __init__(self, batches: Sequence[Sequence[Delivery]], assigned: bool = True) -> None:
        self._batches = list(batches)
        self._assigned = assigned

    def poll(self, timeout_seconds: float) -> Sequence[Delivery]:
        return self._batches.pop(0) if self._batches else []

    def has_assignment(self) -> bool:
        return self._assigned

    def close(self) -> None:
        pass


class RecordingCommitter:
    def __init__(self, reject_on_call: int | None = None) -> None:
        self.commits: list[dict[tuple[str, int], int]] = []
        self._calls = 0
        self._reject_on_call = reject_on_call

    def commit(self, next_offsets: Mapping[tuple[str, int], int]) -> None:
        self._calls += 1
        if self._calls == self._reject_on_call:
            raise CommitRejectedError("CommitFailedError")
        self.commits.append(dict(next_offsets))


class InMemoryLedger:
    """A ledger with the idempotent semantics: a seen event id changes nothing; unknown accounts are poison."""

    def __init__(self, accounts: Sequence[str], *, idempotent: bool = True) -> None:
        self.balances = {account: Decimal(0) for account in accounts}
        self.seen: set[str] = set()
        self._idempotent = idempotent

    def apply(self, payment: Payment, delivery: Delivery) -> Outcome:
        if payment.account_id not in self.balances:
            raise PoisonMessageError(f"unknown account: {payment.account_id}")
        if self._idempotent:
            if payment.event_id in self.seen:
                return Outcome.DUPLICATE
            self.seen.add(payment.event_id)
        self.balances[payment.account_id] += payment.amount
        return Outcome.APPLIED


class CollectingDeadLetters:
    def __init__(self) -> None:
        self.letters: list[DeadLetter] = []

    def send(self, letter: DeadLetter) -> None:
        self.letters.append(letter)


class FakeClock:
    """Every reading advances time by `step`; sleeping is recorded, not performed."""

    def __init__(self, step: float = 1.0) -> None:
        self.now = 0.0
        self.step = step
        self.slept: list[float] = []

    def __call__(self) -> float:
        self.now += self.step
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
```

- Lines 12 to 26: `ScriptedSource` returns the scripted batches one per poll and then empty lists forever (`self._batches.pop(0) if self._batches else []`). `assigned` says whether the fake group has given it partitions; `close` does nothing (`pass`).
- Lines 29 to 39: `RecordingCommitter` stores every commit it accepts. With `reject_on_call=N` it raises `CommitRejectedError` on its N-th call, the way an evicted member would.
- Lines 42 to 58: `InMemoryLedger` has the idempotent semantics in memory: balances in a dictionary, a set of seen event ids. An unknown account raises `PoisonMessageError`. With `idempotent=True`, a seen id returns `DUPLICATE` and changes nothing; otherwise it adds the amount. With `idempotent=False` it counts duplicates twice, like `NaiveLedger`.
- Lines 61 to 66: `CollectingDeadLetters` keeps the letters sent to it.
- Lines 69 to 82: `FakeClock`. `__call__` is the special method that makes an object callable like a function: each reading of the clock adds `step` (1 second) to `now`. `sleep` records the requested seconds and does not wait.
- None of these classes names the protocol it fits. They have the right methods, which is enough.

## `tests/unit/application/test_produce_payments.py`

```python
import json
import random
from decimal import Decimal

import pytest

from idempotent_consumer.application.produce_payments import ProducePlan, produce_payments
from idempotent_consumer.domain.errors import PoisonMessageError
from idempotent_consumer.domain.payments import parse_payment


class RecordingPublisher:
    def __init__(self):
        self.payloads = []
        self.flushed = 0

    def publish(self, payload):
        self.payloads.append(payload)

    def flush(self):
        self.flushed += 1


class RecordingStore:
    def __init__(self):
        self.saved = None

    def save(self, totals):
        self.saved = totals


def run(plan, stop=lambda: False):
    publisher, store = RecordingPublisher(), RecordingStore()
    result = produce_payments(publisher, store, plan, random.Random(1), stop)
    return result, publisher, store


def test_payments_are_published_and_the_expected_totals_are_stored():
    result, publisher, store = run(ProducePlan(payments=100))
    assert (result.payments, result.poison, result.stopped) == (100, 0, False)
    assert len(publisher.payloads) == 100
    assert publisher.flushed == 1
    assert sum(store.saved.values()) == sum(Decimal(str(payload["amount"])) for payload in publisher.payloads)


def test_malformed_events_are_mixed_in_and_not_counted_in_the_expected_totals():
    result, publisher, store = run(ProducePlan(payments=50, poison=2))
    assert len(publisher.payloads) == 52
    assert result.poison == 2
    bad = 0
    for payload in publisher.payloads:
        try:
            parse_payment(json.dumps(payload).encode())
        except PoisonMessageError:
            bad += 1
    assert bad == 2
    good = sum(Decimal(str(p["amount"])) for p in publisher.payloads if p["amount"] != "not-a-number")
    assert sum(store.saved.values()) == good


def test_a_poison_only_run_stores_no_expected_totals():
    _, _, store = run(ProducePlan(payments=0, poison=1))
    assert store.saved is None


def test_a_stop_request_flushes_what_was_sent_and_stores_no_totals():
    stops = iter([False, False, True])
    result, publisher, store = run(ProducePlan(payments=10), stop=lambda: next(stops))
    assert result.stopped
    assert len(publisher.payloads) == 2
    assert publisher.flushed == 1
    assert store.saved is None


@pytest.mark.parametrize("kwargs", [{"payments": -1}, {"poison": -1}])
def test_an_invalid_plan_is_rejected(kwargs):
    with pytest.raises(ValueError, match="invalid produce plan"):
        ProducePlan(**kwargs)
```

- Lines 12 to 29: two tiny fakes with the right methods: a publisher that records payloads and flushes, and a store that remembers what was saved. Lines 32 to 35: the helper `run` calls the use case with a fixed seed.
- Lines 38 to 43: 100 payments: all published, one flush, and the saved expected total equals the sum of the published amounts.
- Lines 46 to 58: with two poison events among 50 payments, 52 payloads are published, exactly two of them fail `parse_payment`, and the expected total equals the sum of the valid amounts only.
- Lines 61 to 63: a run with only a poison event stores no expected totals (there are no payments).
- Lines 66 to 72: with `stops = iter([False, False, True])` and `stop=lambda: next(stops)`, the run ends after two payloads, flushes once, reports `stopped` and stores no totals.
- Lines 75 to 78: negative plan values are refused with "invalid produce plan"; `ProducePlan(**kwargs)` unpacks a dictionary into named arguments.

## `tests/unit/application/test_consume_payments.py`

```python
import json
from decimal import Decimal

import pytest

from idempotent_consumer.application.consume_payments import ConsumePlan, consume_payments
from idempotent_consumer.application.ports import Delivery
from tests.unit.application.fakes import (
    CollectingDeadLetters,
    FakeClock,
    InMemoryLedger,
    RecordingCommitter,
    ScriptedSource,
)


def delivery(
    offset: int, event_id: str, amount: object = 10, account: str = "acct-001", partition: int = 0
) -> Delivery:
    value = json.dumps({"eventId": event_id, "accountId": account, "amount": amount}).encode()
    return Delivery("payments", partition, offset, account.encode(), value)


def uid(number: int) -> str:
    return f"00000000-0000-4000-8000-{number:012d}"


def consume(batches, plan=None, *, ledger=None, committer=None, after_apply=None, clock=None, assigned=True):
    ledger = ledger or InMemoryLedger(["acct-001", "acct-002"])
    committer = committer or RecordingCommitter()
    dead_letters = CollectingDeadLetters()
    clock = clock or FakeClock()
    counts = consume_payments(
        ScriptedSource(batches, assigned),
        committer,
        ledger,
        dead_letters,
        plan or ConsumePlan(idle_timeout_seconds=3),
        stop=lambda: False,
        clock=clock,
        sleep=clock.sleep,
        after_apply=after_apply or (lambda _processed: None),
    )
    return counts, ledger, committer, dead_letters, clock

```

- Lines 17 to 21: helper `delivery(...)` builds a `Delivery` whose value is the JSON of a payment; `uid(number)` (lines 24 and 25) makes predictable UUIDs such as `00000000-0000-4000-8000-000000000001` (`{number:012d}` pads to 12 digits).
- Lines 28 to 44: helper `consume(...)` runs the use case with the fakes: a ledger with two accounts, a recording committer, collecting dead letters, a fake clock, and the default plan `ConsumePlan(idle_timeout_seconds=3)`. `ledger or InMemoryLedger(...)` uses the given ledger or builds one. It returns the counts and all the fakes so a test can inspect them.

```python
def test_each_payment_is_applied_and_its_offset_committed_after_it():
    counts, ledger, committer, _, _ = consume([[delivery(0, uid(1), 10), delivery(1, uid(2), 5)]])
    assert counts.summary() == {"applied": 2, "duplicate": 0, "dead-lettered": 0, "evictions": 0}
    assert ledger.balances["acct-001"] == Decimal(15)
    assert committer.commits == [{("payments", 0): 1}, {("payments", 0): 2}]


def test_a_redelivered_event_is_skipped_and_changes_nothing():
    counts, ledger, _, _, _ = consume([[delivery(0, uid(1)), delivery(0, uid(1))]])
    assert (counts.applied, counts.duplicate) == (1, 1)
    assert ledger.balances["acct-001"] == Decimal(10)


def test_without_the_guard_a_redelivery_counts_twice():
    ledger = InMemoryLedger(["acct-001"], idempotent=False)
    counts, ledger, _, _, _ = consume([[delivery(0, uid(1)), delivery(0, uid(1))]], ledger=ledger)
    assert counts.applied == 2
    assert ledger.balances["acct-001"] == Decimal(20)


def test_a_poison_record_is_dead_lettered_with_its_origin_and_processing_continues():
    batch = [delivery(0, uid(1)), delivery(1, uid(2), amount="not-a-number", partition=2), delivery(2, uid(3))]
    counts, ledger, committer, dead_letters, _ = consume([batch])
    assert counts.summary() == {"applied": 2, "duplicate": 0, "dead-lettered": 1, "evictions": 0}
    assert ledger.balances["acct-001"] == Decimal(20)
    [letter] = dead_letters.letters
    assert (letter.original_topic, letter.original_partition, letter.original_offset) == ("payments", 2, 1)
    assert "not-a-number" in letter.raw.decode()
    assert committer.commits[-1] == {("payments", 0): 3}  # the offset moved past the bad record


def test_a_payment_for_an_unknown_account_is_dead_lettered():
    counts, _, _, dead_letters, _ = consume([[delivery(0, uid(1), account="acct-999")]])
    assert counts.dead_lettered == 1
    assert "unknown account" in dead_letters.letters[0].error


def test_offsets_commit_every_n_records_and_the_rest_at_the_end_of_the_batch():
    batch = [delivery(offset, uid(offset)) for offset in range(5)]
    _, _, committer, _, _ = consume([batch], ConsumePlan(commit_every=2, idle_timeout_seconds=3))
    assert committer.commits == [{("payments", 0): 2}, {("payments", 0): 4}, {("payments", 0): 5}]


def test_the_kill_hook_runs_after_the_apply_and_before_that_records_commit():
    seen: list[tuple[int, int]] = []
    committer = RecordingCommitter()

    def hook(processed: int) -> None:
        seen.append((processed, len(committer.commits)))

    consume([[delivery(0, uid(1)), delivery(1, uid(2))]], committer=committer, after_apply=hook)
    assert seen == [(1, 0), (2, 1)]  # record 1 was applied while no commit had happened yet


def test_a_rejected_commit_counts_an_eviction_and_abandons_the_rest_of_the_batch():
    batch = [delivery(0, uid(1)), delivery(1, uid(2)), delivery(2, uid(3))]
    counts, ledger, committer, _, _ = consume(
        [batch, [delivery(1, uid(2))]], committer=RecordingCommitter(reject_on_call=1)
    )
    # record 1 applied, its commit rejected, records 2 and 3 not touched; the redelivery of 2 is applied next poll
    assert counts.evictions == 1
    assert ledger.balances["acct-001"] == Decimal(20)
    assert committer.commits == [{("payments", 0): 2}]
```

- Lines 47 to 51: two payments are applied; the balance is 15; offsets are committed after each: first `{("payments", 0): 1}`, then `{("payments", 0): 2}` (the next offset to read each time).
- Lines 54 to 57: the same delivery twice: one `applied`, one `duplicate`, and the balance is 10, not 20.
- Lines 60 to 64: with the guard switched off (`idempotent=False`), the redelivery counts twice: balance 20. This is the lesson's bug, shown in a unit test.
- Lines 67 to 75: a bad amount in the middle: two applied, one dead-lettered; the letter carries topic `payments`, partition 2, offset 1, and the raw bytes include `not-a-number`; processing goes on; and the last commit moves past the bad record (`{("payments", 0): 3}`). `[letter] = dead_letters.letters` unpacks a list that must have exactly one item.
- Lines 78 to 81: a payment for `acct-999`, which does not exist, is dead-lettered with the reason "unknown account".
- Lines 84 to 87: with `commit_every=2` and five records, commits are at 2, 4 and then 5 (the rest at the end of the batch).
- Lines 90 to 98: the kill hook runs after the apply and before that record's commit. The hook records `(processed, number of commits so far)`; the result `[(1, 0), (2, 1)]` shows record 1 was applied while no commit had happened.
- Lines 101 to 109: the committer rejects its first call. The sequence (the comment on line 106 spells it out): record 1 is applied (+10) and its commit is rejected, so one eviction is counted and records 2 and 3 of that batch are left untouched; the next poll delivers record 2 again, which is applied (+10); the balance is 20; and the only commit recorded is that later one, `{("payments", 0): 2}`.

```python
def test_the_work_delay_sleeps_once_per_record():
    _, _, _, _, clock = consume(
        [[delivery(0, uid(1)), delivery(1, uid(2))]], ConsumePlan(work_delay_seconds=0.1, idle_timeout_seconds=3)
    )
    assert clock.slept == [0.1, 0.1]


def test_it_stops_after_the_idle_timeout_once_the_group_has_assigned_partitions():
    _, _, _, _, clock = consume([], ConsumePlan(idle_timeout_seconds=2.5))
    assert clock.now > 2.5


def test_the_idle_clock_does_not_run_while_the_member_has_no_assignment():
    clock = FakeClock()
    consume([], ConsumePlan(idle_timeout_seconds=2.5, run_seconds=20), assigned=False, clock=clock)
    assert clock.now > 20  # no idle exit: only the overall run limit ended the loop


def test_it_stops_after_the_run_time_even_with_records_flowing():
    batches = [[delivery(offset, uid(offset))] for offset in range(100)]
    counts, _, _, _, _ = consume(batches, ConsumePlan(run_seconds=5))
    assert 0 < counts.applied < 100


def test_it_returns_when_asked_to_stop():
    clock = FakeClock()
    polls = iter([False, False, True])
    counts = consume_payments(
        ScriptedSource([[delivery(0, uid(1))]]),
        RecordingCommitter(),
        InMemoryLedger(["acct-001"]),
        CollectingDeadLetters(),
        ConsumePlan(),
        stop=lambda: next(polls),
        clock=clock,
        sleep=clock.sleep,
    )
    assert counts.applied == 1


@pytest.mark.parametrize("kwargs", [{"commit_every": 0}, {"work_delay_seconds": -1}, {"poll_seconds": 0}])
def test_an_invalid_plan_is_rejected(kwargs):
    with pytest.raises(ValueError, match="invalid consume plan"):
        ConsumePlan(**kwargs)
```

- Lines 112 to 116: the work delay sleeps once per record: `[0.1, 0.1]`.
- Lines 119 to 121: with no records and an idle timeout of 2.5 seconds, the loop ends once the fake clock has passed 2.5.
- Lines 124 to 127: with `assigned=False`, the idle timeout never fires; only the overall run limit of 20 seconds ends the loop (`clock.now > 20`). This is the "idle clock waits for the assignment" rule.
- Lines 130 to 133: with records flowing, `run_seconds=5` still ends the run before all 100 are applied.
- Lines 136 to 149: the loop returns when `stop()` says so; `next(polls)` yields False, False, then True.
- Lines 152 to 155: three invalid plans are refused with "invalid consume plan".

## `tests/unit/application/test_crash_test_and_health.py`

```python
from decimal import Decimal

from idempotent_consumer.application.check_balances import check_balances
from idempotent_consumer.application.crash_test import CrashRun, run_crash_test
from idempotent_consumer.application.health import ProbeFailedError, check_health


class FakeRunner:
    def __init__(self, crash_code: int = 1, restart_code: int = 0) -> None:
        self.calls: list[tuple[str, int | None, int]] = []
        self._codes = (crash_code, restart_code)

    def run(self, group_id: str, kill_at: int | None, commit_every: int) -> int:
        self.calls.append((group_id, kill_at, commit_every))
        return self._codes[0] if kill_at else self._codes[1]


class FakeAdmin:
    def __init__(self) -> None:
        self.resets = 0

    def reset(self) -> None:
        self.resets += 1


class FixedReader:
    def __init__(self, balances: dict[str, Decimal]) -> None:
        self._balances = balances

    def read_balances(self) -> dict[str, Decimal]:
        return self._balances


EXPECTED = {"acct-001": Decimal(10)}


def crash_test(runner, reader, runs=3):
    seen: list[CrashRun] = []
    admin = FakeAdmin()
    results = run_crash_test(
        runner, admin, reader, EXPECTED, runs=runs, payments=300, commit_every=50,
        group_for_run=lambda number: f"g-{number}", on_run=seen.append,
    )  # fmt: skip
    return results, admin, seen


def test_every_run_resets_crashes_a_fresh_group_restarts_it_and_compares():
    runner = FakeRunner()
    results, admin, seen = crash_test(runner, FixedReader(EXPECTED))
    assert admin.resets == 3
    assert [run.number for run in results] == [1, 2, 3]
    assert seen == results
    assert all(run.passed for run in results)
    groups = [call[0] for call in runner.calls]
    assert groups == ["g-1", "g-1", "g-2", "g-2", "g-3", "g-3"]  # crash then restart of the same group
    assert [call[1] is not None for call in runner.calls] == [True, False] * 3
    assert {call[2] for call in runner.calls} == {50}


def test_a_run_fails_when_a_balance_is_wrong():
    results, _, _ = crash_test(FakeRunner(), FixedReader({"acct-001": Decimal(20)}), runs=1)
    assert not results[0].passed
    assert len(results[0].mismatches) == 1


def test_a_run_fails_when_the_kill_switch_did_not_fire_or_the_restart_failed():
    assert not crash_test(FakeRunner(crash_code=0), FixedReader(EXPECTED), runs=1)[0][0].passed
    assert not crash_test(FakeRunner(restart_code=3), FixedReader(EXPECTED), runs=1)[0][0].passed


def test_check_balances_lists_the_accounts_that_differ():
    assert check_balances(FixedReader(EXPECTED), EXPECTED) == []
    assert [m.account_id for m in check_balances(FixedReader({"acct-001": Decimal(11)}), EXPECTED)] == ["acct-001"]


class Healthy:
    name = "kafka"

    def check(self) -> str:
        return "topics exist"


class Broken:
    name = "postgres"

    def check(self) -> str:
        raise ProbeFailedError("connection refused")


def test_a_report_is_healthy_only_when_every_probe_is():
    assert check_health([Healthy()]).healthy
    report = check_health([Healthy(), Broken()])
    assert not report.healthy
    assert [(r.name, r.ok, r.detail) for r in report.results] == [
        ("kafka", True, "topics exist"),
        ("postgres", False, "connection refused"),
    ]
```

- Lines 8 to 15: `FakeRunner` records each call; it returns the crash exit code when a kill point is given and the restart code otherwise. Lines 18 to 31: a fake admin that counts resets and a reader that returns fixed balances.
- Lines 37 to 44: the helper `crash_test` runs the use case with three runs of 300 payments; `# fmt: skip` keeps the formatter off the multi-line call.
- Lines 47 to 57: each of the three runs resets the tables; the runs are numbered 1 to 3; the callback saw each; all passed; the calls are `g-1, g-1, g-2, g-2, g-3, g-3` (a crash then a restart of the same group), alternating "with kill point" and "without"; and every call used `commit_every` 50.
- Lines 60 to 63: a wrong balance makes the run fail with one mismatch.
- Lines 66 to 68: a run also fails when the kill switch did not fire (crash exit code 0) or the restart failed (exit code 3).
- Lines 71 to 73: `check_balances` returns an empty list when the balances match and lists `acct-001` when they differ.
- Lines 76 to 97: the two health fakes and the health report test, as in the other repositories: healthy only when every probe is, with each probe's name, flag and detail listed.

## `tests/unit/infrastructure/test_settings.py`

```python
import pytest

from idempotent_consumer.infrastructure.settings import ConfigurationError, Settings

MINIMAL = {"POSTGRES_PASSWORD": "s3cret-value"}


def test_defaults_describe_the_local_compose_setup():
    settings = Settings.from_env(MINIMAL)
    assert settings.kafka.bootstrap_servers == "localhost:9092"
    assert (settings.kafka.topic, settings.kafka.dlq_topic, settings.kafka.group_id) == (
        "payments",
        "payments-dlq",
        "balance-updater",
    )
    assert (settings.postgres.host, settings.postgres.port, settings.postgres.database, settings.postgres.user) == (
        "localhost",
        5433,
        "bank",
        "bank",
    )
    assert settings.log_level == "INFO"


def test_the_database_password_has_no_default():
    with pytest.raises(ConfigurationError, match="POSTGRES_PASSWORD"):
        Settings.from_env({})


def test_the_password_never_appears_in_a_printed_settings_object():
    assert "s3cret-value" not in repr(Settings.from_env(MINIMAL))


def test_every_variable_can_be_overridden():
    settings = Settings.from_env(
        {
            **MINIMAL,
            "BOOTSTRAP_SERVERS": "a:1,b:2",
            "TOPIC": "t",
            "DLQ_TOPIC": "d",
            "GROUP_ID": "g",
            "POSTGRES_HOST": "db",
            "POSTGRES_PORT": "6543",
            "POSTGRES_DB": "x",
            "POSTGRES_USER": "u",
            "LOG_LEVEL": "debug",
            "KAFKA_REQUEST_TIMEOUT_MS": "5000",
            "KAFKA_RETRY_BACKOFF_MS": "50",
            "POSTGRES_CONNECT_TIMEOUT_SECONDS": "2",
        }
    )
    assert (settings.kafka.topic, settings.kafka.dlq_topic, settings.kafka.group_id) == ("t", "d", "g")
    assert (settings.postgres.host, settings.postgres.port, settings.postgres.database) == ("db", 6543, "x")
    assert (settings.postgres.user, settings.postgres.password) == ("u", "s3cret-value")
    assert (settings.log_level, settings.kafka.request_timeout_ms, settings.postgres.connect_timeout_seconds) == (
        "DEBUG",
        5000,
        2,
    )


@pytest.mark.parametrize(
    "extra",
    [
        {"BOOTSTRAP_SERVERS": "no-port"},
        {"TOPIC": "  "},
        {"POSTGRES_PORT": "0"},
        {"POSTGRES_PORT": "five"},
        {"LOG_LEVEL": "LOUD"},
        {"KAFKA_REQUEST_TIMEOUT_MS": "-1"},
    ],
)
def test_an_unusable_value_is_rejected_with_the_variable_name(extra):
    with pytest.raises(ConfigurationError, match=next(iter(extra))):
        Settings.from_env({**MINIMAL, **extra})


def test_settings_read_the_process_environment_by_default(monkeypatch):
    monkeypatch.setenv("POSTGRES_PASSWORD", "from-the-process")
    assert Settings.from_env().postgres.password == "from-the-process"
```

- Line 5: `MINIMAL = {"POSTGRES_PASSWORD": "s3cret-value"}` is the one variable that has no default; the value is a made-up test value.
- Lines 8 to 22: with only the password given, every other setting has the default that suits `docker-compose.yml` (broker `localhost:9092`, topics `payments` and `payments-dlq`, group `balance-updater`, database `localhost:5433`, name and user `bank`, level `INFO`).
- Lines 25 to 27: with an empty environment, the error names `POSTGRES_PASSWORD`.
- Lines 30 and 31: the password text does not appear in `repr(settings)`; the printed form hides it.
- Lines 34 to 59: every variable can be overridden (`"debug"` becomes `"DEBUG"`). `{**MINIMAL, ...}` merges dictionaries.
- Lines 62 to 75: six unusable values (a server list without a port, a blank topic, ports `0` and `five`, an unknown level, a negative timeout) each raise `ConfigurationError` with the variable's name (`match=next(iter(extra))` takes the first key).
- Lines 78 to 80: `monkeypatch` is a built-in fixture that changes the environment for one test and puts it back; `Settings.from_env()` with no argument reads the real process environment.

## `tests/unit/infrastructure/test_retry.py`

```python
import random

import pytest

from idempotent_consumer.infrastructure.retry import retry


class Flaky:
    def __init__(self, failures: int) -> None:
        self.failures, self.calls = failures, 0

    def __call__(self) -> str:
        self.calls += 1
        if self.calls <= self.failures:
            raise ConnectionError("down")
        return "ok"


def test_it_returns_the_first_success_without_sleeping():
    pauses: list[float] = []
    assert retry(Flaky(0), attempts=3, base_delay_seconds=1, retry_on=(ConnectionError,), sleep=pauses.append) == "ok"
    assert pauses == []


def test_it_retries_with_doubling_pauses_inside_the_jitter_band():
    pauses: list[float] = []
    operation = Flaky(3)
    result = retry(
        operation,
        attempts=5,
        base_delay_seconds=1,
        retry_on=(ConnectionError,),
        sleep=pauses.append,
        rng=random.Random(1),
    )
    assert result == "ok"
    assert operation.calls == 4
    for pause, ceiling in zip(pauses, (1, 2, 4), strict=True):
        assert ceiling * 0.5 <= pause <= ceiling


def test_the_pause_is_capped():
    pauses: list[float] = []
    retry(
        Flaky(4),
        attempts=6,
        base_delay_seconds=10,
        retry_on=(ConnectionError,),
        sleep=pauses.append,
        max_delay_seconds=15,
    )
    assert max(pauses) <= 15


def test_the_last_failure_is_raised_after_the_final_attempt():
    operation = Flaky(10)
    with pytest.raises(ConnectionError):
        retry(operation, attempts=3, base_delay_seconds=0, retry_on=(ConnectionError,), sleep=lambda _: None)
    assert operation.calls == 3


def test_other_errors_are_not_retried():
    def broken() -> str:
        raise KeyError("x")

    with pytest.raises(KeyError):
        retry(broken, attempts=3, base_delay_seconds=0, retry_on=(ConnectionError,), sleep=lambda _: None)


def test_each_retry_is_reported():
    seen: list[int] = []
    retry(
        Flaky(2),
        attempts=4,
        base_delay_seconds=0,
        retry_on=(ConnectionError,),
        sleep=lambda _: None,
        on_retry=lambda attempt, _: seen.append(attempt),
    )
    assert seen == [1, 2]


def test_at_least_one_attempt_is_required():
    with pytest.raises(ValueError, match="attempts"):
        retry(Flaky(0), attempts=0, base_delay_seconds=0, retry_on=(ConnectionError,))
```

- Lines 8 to 16: `Flaky` is a callable object that fails a set number of times and then returns `"ok"`; `__call__` makes `Flaky(3)()` work like a function.
- Lines 19 to 22: no failure, no pause.
- Lines 25 to 39: three failures, then success on the fourth call; the recorded pauses sit between half of, and the whole of, 1, 2 and 4 seconds. `zip(..., strict=True)` pairs the two lists and raises an error if their lengths differ.
- Lines 42 to 52: the cap of 15 seconds holds although the formula would give 10, 20, 40, 80.
- Lines 55 to 59: if every attempt fails, the last error is raised and there were exactly three calls.
- Lines 62 to 67: a `KeyError` is not in `retry_on`, so it passes through at once.
- Lines 70 to 80: `on_retry` is called with attempts 1 and 2.
- Lines 83 to 85: zero attempts raises `ValueError`.

## `tests/unit/infrastructure/test_logging_and_shutdown.py`

```python
import io
import logging
import signal

from idempotent_consumer.infrastructure.logging import configure_logging, fields
from idempotent_consumer.infrastructure.shutdown import ShutdownSignal


def test_a_record_becomes_one_logfmt_line_with_its_fields():
    stream = io.StringIO()
    configure_logging("INFO", stream)
    logging.getLogger("demo").info("partitions assigned", extra=fields(member="consumer-1", partitions=[0, 1]))
    line = stream.getvalue().strip()
    assert line.startswith("ts=")
    assert "level=INFO" in line
    assert "logger=demo" in line
    assert 'msg="partitions assigned"' in line
    assert "member=consumer-1" in line
    assert 'partitions="[0, 1]"' in line


def test_records_below_the_level_are_dropped_and_kafka_stays_at_warning():
    stream = io.StringIO()
    configure_logging("INFO", stream)
    logging.getLogger("demo").debug("hidden")
    assert stream.getvalue() == ""
    assert logging.getLogger("kafka").level == logging.WARNING


def test_an_exception_is_attached_to_the_line():
    stream = io.StringIO()
    configure_logging("INFO", stream)
    try:
        raise ValueError("boom")
    except ValueError:
        logging.getLogger("demo").exception("failed")
    assert "exc=" in stream.getvalue()
    assert "boom" in stream.getvalue()


def test_configuring_twice_does_not_duplicate_lines():
    stream = io.StringIO()
    configure_logging("INFO", stream)
    configure_logging("INFO", stream)
    logging.getLogger("demo").info("once")
    assert stream.getvalue().count("msg=once") == 1


def test_a_shutdown_signal_starts_unset_and_is_set_by_request():
    shutdown = ShutdownSignal()
    assert not shutdown.requested
    shutdown.request()
    assert shutdown.requested


def test_sigterm_sets_the_flag_once_installed():
    shutdown = ShutdownSignal()
    previous = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
    try:
        shutdown.install()
        signal.raise_signal(signal.SIGTERM)
        assert shutdown.requested
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)
```

- Lines 9 to 19: `io.StringIO()` is an in-memory text stream; logging to it lets the test read what would have been written to the terminal. The line must start with `ts=` and contain the level, the logger name, the quoted message (it has a space), the plain `member=consumer-1`, and the list quoted because it contains spaces.
- Lines 22 to 27: a debug record is dropped at level INFO, and the `kafka` logger is at WARNING.
- Lines 30 to 38: inside `except`, `logging.getLogger("demo").exception("failed")` logs with the error attached; the line holds `exc=` and the text `boom`.
- Lines 41 to 46: configuring twice does not duplicate lines.
- Lines 49 to 53: a new `ShutdownSignal` is not set; `request()` sets it.
- Lines 56 to 65: really sends the process `SIGTERM` with `signal.raise_signal(signal.SIGTERM)` after `install()`, and checks the flag. The `try ... finally` puts the original signal handlers back so other tests are not affected.

## `tests/unit/infrastructure/test_expected_file_and_runner.py`

```python
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

from idempotent_consumer.infrastructure.expected_file import load_expected, save_expected
from idempotent_consumer.infrastructure.subprocess_runner import SubprocessConsumerRunner


def test_expected_totals_round_trip_through_json(tmp_path: Path):
    path = tmp_path / "expected.json"
    totals = {"a": Decimal("10.25"), "b": Decimal("0.00")}
    save_expected(path, totals)
    assert load_expected(path) == totals


class FakeRun:
    def __init__(self) -> None:
        self.commands: list[list[str]] = []

    def __call__(self, command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        self.commands.append(command)
        return subprocess.CompletedProcess(command, 7)


def test_a_crash_run_passes_the_kill_point_and_a_restart_waits_for_idle(tmp_path: Path, monkeypatch):
    fake = FakeRun()
    monkeypatch.setattr(subprocess, "run", fake)
    runner = SubprocessConsumerRunner(tmp_path / "logs" / "crash.log", {"POSTGRES_PASSWORD": "x"})

    assert runner.run("g-1", kill_at=731, commit_every=50) == 7
    runner.run("g-1", kill_at=None, commit_every=50)

    crash, restart = fake.commands
    assert crash[:3] == [sys.executable, "-m", "idempotent_consumer.interfaces.consume_cli"]
    assert crash[crash.index("--kill-at") + 1] == "731"
    assert "--until-idle" not in crash
    assert restart[restart.index("--until-idle") + 1] == "5"
    assert "--kill-at" not in restart
    assert crash[crash.index("--group") + 1] == "g-1"
    assert crash[crash.index("--commit-every") + 1] == "50"
    assert (tmp_path / "logs" / "crash.log").exists()
```

- `tmp_path` is a built-in fixture: a fresh empty folder for this test, deleted later.
- Lines 10 to 14: **round trip**: expected totals saved to JSON and loaded back are equal, decimals included.
- Lines 17 to 23: `FakeRun` stands in for `subprocess.run`. `__call__` records the command and returns a `CompletedProcess` with exit code 7. `**_: object` accepts and ignores any keyword arguments.
- Lines 26 to 42: `monkeypatch.setattr(subprocess, "run", fake)` replaces `subprocess.run` for this test only, so no real process starts. The runner is asked for a crash run (`kill_at=731`) and a restart. The test checks: the command starts with the running Python and `-m idempotent_consumer.interfaces.consume_cli`; the crash command has `--kill-at 731` and no `--until-idle`; the restart has `--until-idle 5` and no `--kill-at`; both carry the group and `--commit-every 50`; the exit code 7 is returned; and the log file was created. `crash.index("--kill-at") + 1` finds the position of the word after `--kill-at`.

## `tests/unit/interfaces/test_arguments_and_clis.py`

```python
import argparse

import pytest

from idempotent_consumer.interfaces import check_cli, consume_cli, crash_test_cli, health_cli, produce_cli
from idempotent_consumer.interfaces.arguments import non_negative_float, non_negative_int, positive_int


def test_validators_accept_valid_numbers():
    assert positive_int("3") == 3
    assert non_negative_int("0") == 0
    assert non_negative_float("0.5") == 0.5


@pytest.mark.parametrize(
    ("validator", "text"), [(positive_int, "0"), (non_negative_int, "-1"), (non_negative_float, "-0.1")]
)
def test_validators_reject_out_of_range_numbers(validator, text):
    with pytest.raises(argparse.ArgumentTypeError):
        validator(text)


def test_the_producer_defaults_match_the_lessons_run():
    args = produce_cli.build_parser().parse_args([])
    assert (args.payments, args.poison) == (10_000, 0)


def test_the_consumer_defaults_are_the_idempotent_per_record_commit():
    args = consume_cli.build_parser().parse_args([])
    assert (args.mode, args.commit_every, args.kill_at, args.max_poll_records) == ("idempotent", 1, 0, 500)
    assert args.max_poll_interval_ms == 300_000


def test_the_consumer_rejects_an_unknown_mode():
    with pytest.raises(SystemExit):
        consume_cli.build_parser().parse_args(["--mode", "maybe"])


def test_a_missing_database_password_stops_every_program_with_exit_code_two(monkeypatch, capsys):
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    assert produce_cli.main([]) == 2
    assert consume_cli.main([]) == 2
    assert check_cli.main([]) == 2
    assert crash_test_cli.main([]) == 2
    assert health_cli.main([]) == 2
    assert "POSTGRES_PASSWORD" in capsys.readouterr().err
```

- Lines 9 to 20: the three number checkers accept valid input and raise `ArgumentTypeError` for out-of-range input; the second test is parametrized with two names at once.
- Lines 23 to 25: the producer's defaults are 10,000 payments and 0 poison. Lines 28 to 31: the consumer's defaults are the idempotent mode, commit after every record, no kill point, 500 records per poll and a 300,000 ms poll interval.
- Lines 34 to 36: an unknown `--mode` makes `argparse` exit (`SystemExit`).
- Lines 39 to 46: with `POSTGRES_PASSWORD` removed (`monkeypatch.delenv(..., raising=False)` means no error if it was not set), each of the five programs returns exit code 2, and the captured standard error (`capsys.readouterr().err`) names the variable.

## `tests/integration/test_postgres_ledger.py`

These tests need `make up` first (they use the real PostgreSQL).

```python
"""The ledger adapters against the real PostgreSQL: make up && make test-integration."""

from decimal import Decimal
from typing import Any

import psycopg
import pytest

from idempotent_consumer.application.ports import Delivery, Outcome
from idempotent_consumer.domain.errors import PoisonMessageError
from idempotent_consumer.domain.payments import Payment
from idempotent_consumer.infrastructure.postgres_ledger import (
    IdempotentLedger,
    NaiveLedger,
    PostgresBalanceReader,
    PostgresLedgerAdmin,
)

pytestmark = pytest.mark.integration

PAYMENT = Payment("6f1c2b6e-3d7e-4c55-9d52-0d6a4b7a1e11", "acct-017", Decimal("82.48"))
DELIVERY = Delivery("payments", 3, 5512, b"acct-017", b"{}")


def balance(connection: psycopg.Connection[Any], account: str = "acct-017") -> Decimal:
    return PostgresBalanceReader(connection).read_balances()[account]


def test_the_seed_has_fifty_accounts_at_zero(connection: psycopg.Connection[Any]) -> None:
    balances = PostgresBalanceReader(connection).read_balances()
    assert len(balances) == 50
    assert set(balances.values()) == {Decimal(0)}


def test_the_naive_ledger_counts_a_redelivered_payment_twice(connection: psycopg.Connection[Any]) -> None:
    ledger = NaiveLedger(connection)
    assert ledger.apply(PAYMENT, DELIVERY) is Outcome.APPLIED
    assert ledger.apply(PAYMENT, DELIVERY) is Outcome.APPLIED
    assert balance(connection) == Decimal("164.96")


def test_the_idempotent_ledger_skips_a_redelivered_payment(connection: psycopg.Connection[Any]) -> None:
    ledger = IdempotentLedger(connection)
    assert ledger.apply(PAYMENT, DELIVERY) is Outcome.APPLIED
    assert ledger.apply(PAYMENT, DELIVERY) is Outcome.DUPLICATE
    assert balance(connection) == Decimal("82.48")
    assert connection.execute("SELECT count(*) FROM processed_events").fetchone() == (1,)


def test_the_event_id_is_recorded_with_where_it_was_first_read(connection: psycopg.Connection[Any]) -> None:
    IdempotentLedger(connection).apply(PAYMENT, DELIVERY)
    row = connection.execute('SELECT topic, partition, "offset" FROM processed_events').fetchone()
    assert row == ("payments", 3, 5512)


def test_a_payment_for_an_unknown_account_is_poison_and_leaves_no_trace(connection: psycopg.Connection[Any]) -> None:
    stranger = Payment(PAYMENT.event_id, "acct-999", Decimal(5))
    with pytest.raises(PoisonMessageError, match="unknown account"):
        IdempotentLedger(connection).apply(stranger, DELIVERY)
    # the event id insert was rolled back with the failed update, so a later fix can still apply it
    assert connection.execute("SELECT count(*) FROM processed_events").fetchone() == (0,)


def test_reset_zeroes_the_balances_and_forgets_the_events(connection: psycopg.Connection[Any]) -> None:
    IdempotentLedger(connection).apply(PAYMENT, DELIVERY)
    PostgresLedgerAdmin(connection).reset()
    assert balance(connection) == Decimal(0)
    assert connection.execute("SELECT count(*) FROM processed_events").fetchone() == (0,)
```

- Line 19: `pytestmark = pytest.mark.integration` puts the mark `integration` on every test in the file. `make test-integration` runs `pytest tests/integration -m integration`, which selects them; `make test` runs only `tests/unit`. (`--strict-markers` in `pyproject.toml` makes an unregistered mark an error.)
- Lines 21 and 22: one payment of 82.48 for `acct-017` and a delivery that says where it was read.
- Lines 25 and 26: helper `balance(connection, account)` reads one balance.
- Lines 29 to 32: the seed has 50 accounts, all at 0 (`set(balances.values()) == {Decimal(0)}`).
- Lines 35 to 39: the naive ledger applies the same payment twice: both return `APPLIED`, and the balance is 164.96, twice 82.48. `is` compares with the exact enum member.
- Lines 42 to 47: the idempotent ledger returns `APPLIED` then `DUPLICATE`; the balance is 82.48; and `SELECT count(*) FROM processed_events` returns the tuple `(1,)`.
- Lines 50 to 53: the recorded row has where the event was first read: topic `payments`, partition 3, offset 5512. The column `"offset"` is quoted because it is an SQL reserved word.
- Lines 56 to 61: a payment for an unknown account raises `PoisonMessageError` and leaves **no row** in `processed_events`, because the insert was rolled back together with the failed update.
- Lines 64 to 68: `reset` zeroes the balance and forgets the events.

## `tests/integration/test_kafka_adapters.py`

These tests need `make up` first (they use the real broker).

```python
"""The Kafka adapters against the real broker: make up && make test-integration."""

import json
import time
import uuid

import pytest
from kafka import KafkaConsumer, TopicPartition

from idempotent_consumer.application.health import check_health
from idempotent_consumer.application.ports import CommitRejectedError, Delivery
from idempotent_consumer.domain.dead_letter import DeadLetter
from idempotent_consumer.infrastructure.health_probes import KafkaTopicsProbe, PostgresSchemaProbe
from idempotent_consumer.infrastructure.kafka_consumer import KafkaPaymentConsumer
from idempotent_consumer.infrastructure.kafka_producers import KafkaDeadLetterSink, KafkaPaymentPublisher
from idempotent_consumer.infrastructure.settings import KafkaSettings, Settings

pytestmark = pytest.mark.integration


def kafka_for(settings: Settings, topics: tuple[str, str]) -> KafkaSettings:
    return KafkaSettings(settings.kafka.bootstrap_servers, topics[0], topics[1], settings.kafka.group_id, 30_000, 100)


def read_until(consumer: KafkaPaymentConsumer, count: int, seconds: float = 30) -> list[Delivery]:
    deliveries: list[Delivery] = []
    deadline = time.time() + seconds
    while len(deliveries) < count and time.time() < deadline:
        deliveries.extend(consumer.poll(0.5))
    return deliveries

```

- Line 22: `kafka_for(settings, topics)` builds `KafkaSettings` pointing at the test's own topics. The positional arguments are the fields in order: servers, topic, dead-letter topic, group, request timeout, retry backoff.
- Lines 25 to 30: `read_until(consumer, count, seconds)` polls until `count` records are read or the time is up.

```python
def test_payments_are_published_keyed_by_account_and_read_back(settings: Settings, topics: tuple[str, str]) -> None:
    kafka = kafka_for(settings, topics)
    publisher = KafkaPaymentPublisher(kafka)
    for number in range(30):
        publisher.publish({"eventId": str(uuid.uuid4()), "accountId": f"acct-{number % 3:03d}", "amount": 1.5})
    publisher.flush()
    publisher.close()

    consumer = KafkaPaymentConsumer(kafka, group_id=f"read-{uuid.uuid4().hex}")
    deliveries = read_until(consumer, 30)
    consumer.close()

    assert len(deliveries) == 30
    partitions_by_account: dict[bytes | None, set[int]] = {}
    for delivery in deliveries:
        partitions_by_account.setdefault(delivery.key, set()).add(delivery.partition)
    assert all(len(partitions) == 1 for partitions in partitions_by_account.values())  # one account, one partition


def test_a_committed_offset_survives_the_member_and_the_next_member_resumes_after_it(
    settings: Settings, topics: tuple[str, str]
) -> None:
    kafka, group = kafka_for(settings, topics), f"resume-{uuid.uuid4().hex}"
    publisher = KafkaPaymentPublisher(kafka)
    for number in range(10):
        publisher.publish({"eventId": str(uuid.uuid4()), "accountId": "acct-001", "amount": number + 1})
    publisher.flush()
    publisher.close()

    first = KafkaPaymentConsumer(kafka, group_id=group)
    deliveries = read_until(first, 10)
    third = deliveries[2]
    first.commit({(third.topic, third.partition): third.offset + 1})  # commit through the third record only
    first.close()

    second = KafkaPaymentConsumer(kafka, group_id=group)
    resumed = read_until(second, 7)
    second.close()
    assert [d.offset for d in resumed] == [d.offset for d in deliveries[3:]]

```

- Lines 33 to 49: 30 payments for three accounts are published and read back through a new group. Each account's records all sit in one partition (one key, one partition): the dictionary `partitions_by_account` maps each key to the set of partitions seen, and `setdefault(key, set()).add(...)` creates the set when needed.
- Lines 52 to 71: **offsets survive the member**. Ten payments are read by a first consumer; it commits through the third record only (`third.offset + 1`); a second consumer of the same group then resumes with exactly the records after the third (`deliveries[3:]`).

```python
def test_a_commit_after_the_poll_interval_expired_is_rejected(settings: Settings, topics: tuple[str, str]) -> None:
    kafka = kafka_for(settings, topics)
    publisher = KafkaPaymentPublisher(kafka)
    publisher.publish({"eventId": str(uuid.uuid4()), "accountId": "acct-001", "amount": 1})
    publisher.flush()
    publisher.close()

    consumer = KafkaPaymentConsumer(kafka, group_id=f"slow-{uuid.uuid4().hex}", max_poll_interval_ms=2000)
    [delivery] = read_until(consumer, 1)
    time.sleep(6)  # no poll for longer than max.poll.interval.ms: the group evicts this member
    try:
        with pytest.raises(CommitRejectedError):
            consumer.commit({(delivery.topic, delivery.partition): delivery.offset + 1})
    finally:
        consumer.close()


def test_a_dead_letter_is_stored_with_its_origin_before_send_returns(
    settings: Settings, topics: tuple[str, str]
) -> None:
    kafka = kafka_for(settings, topics)
    sink = KafkaDeadLetterSink(kafka)
    sink.send(DeadLetter(topics[0], 3, 5512, "bad amount", b'{"amount": "not-a-number"}'))
    sink.close()

    reader = KafkaConsumer(
        topics[1], bootstrap_servers=kafka.bootstrap_servers, auto_offset_reset="earliest", consumer_timeout_ms=10_000
    )
    letters = [json.loads(record.value) for record in reader]
    reader.close()
    assert len(letters) == 1
    assert (letters[0]["originalTopic"], letters[0]["originalPartition"], letters[0]["originalOffset"]) == (
        topics[0],
        3,
        5512,
    )
    assert TopicPartition(topics[1], 0) is not None


def test_the_health_probes_pass_against_the_running_services(settings: Settings, topics: tuple[str, str]) -> None:
    kafka = kafka_for(settings, topics)
    report = check_health([KafkaTopicsProbe(kafka), PostgresSchemaProbe(settings.postgres)])
    assert report.healthy, report
```

- Lines 74 to 88: **eviction**. A consumer with `max_poll_interval_ms=2000` reads one record, then sleeps six seconds without polling, longer than the allowed interval. The group removes it, so committing now raises `CommitRejectedError`. `[delivery] = read_until(consumer, 1)` unpacks a one-item list. The `try ... finally` closes the consumer even if the assertion fails.
- Lines 91 to 110: **dead letters**. `sink.send(...)` returns only after the broker acknowledged. A plain consumer with `consumer_timeout_ms=10_000` (stop iterating after 10 quiet seconds) then reads the dead-letter topic; there is exactly one letter and it carries the original topic, partition 3 and offset 5512. The last assertion line (`TopicPartition(...) is not None`) is always true and only makes use of the import.
- Lines 113 to 116: the two health probes pass against the running services; `assert report.healthy, report` prints the report if the assertion fails.

## `tests/e2e/test_lesson_scenarios.py`

```python
"""The lesson's scenarios through the command-line programs, as the Makefile runs them.

make up && make test-e2e. Each scenario starts real processes, including one that crashes on purpose.
"""

import os
import re
import subprocess
import sys
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
import pytest

from idempotent_consumer.domain.balances import diff_balances
from idempotent_consumer.infrastructure.expected_file import load_expected
from idempotent_consumer.infrastructure.postgres_ledger import PostgresBalanceReader

pytestmark = pytest.mark.e2e


def program(module: str, topics: tuple[str, str], *args: str) -> subprocess.CompletedProcess[str]:
    environment = {**os.environ, "TOPIC": topics[0], "DLQ_TOPIC": topics[1]}
    return subprocess.run(
        [sys.executable, "-m", f"idempotent_consumer.interfaces.{module}", *args],
        capture_output=True,
        text=True,
        env=environment,
        timeout=240,
        check=False,
    )


def produce(topics: tuple[str, str], expected: Path, payments: int, poison: int = 0) -> None:
    result = program(
        "produce_cli",
        topics,
        "--payments",
        str(payments),
        "--poison",
        str(poison),
        "--seed",
        "1",
        "--expected",
        str(expected),
    )
    assert result.returncode == 0, result.stderr


def wrong_balances(connection: psycopg.Connection[Any], expected: Path) -> list[Any]:
    return diff_balances(load_expected(expected), PostgresBalanceReader(connection).read_balances())

```

- Lines 25 to 34: helper `program(...)` starts one of the command-line programs as a separate **process** with `subprocess.run`. `sys.executable` is the path of the Python running the tests. `capture_output=True, text=True` collects stdout and stderr as text. `env={**os.environ, "TOPIC": ..., "DLQ_TOPIC": ...}` copies the environment and points the program at the test's own topics. `timeout=240` stops a hung program; `check=False` means a non-zero exit code does not raise, the test examines `returncode`.
- Lines 37 to 50: helper `produce(...)` runs the producer with a fixed seed and an expected-totals file in the test's temporary folder, and asserts it succeeded (`assert ..., result.stderr` prints the error output if it did not).
- Lines 53 and 54: `wrong_balances` compares the file's expected totals with the database.

```python
def test_the_naive_consumer_double_counts_one_payment_after_a_crash(
    connection: psycopg.Connection[Any], topics: tuple[str, str], tmp_path: Path
) -> None:
    expected = tmp_path / "expected.json"
    produce(topics, expected, payments=300)
    group = f"naive-{uuid.uuid4().hex}"

    assert program("consume_cli", topics, "--mode", "naive", "--group", group, "--kill-at", "100").returncode == 1
    assert program("consume_cli", topics, "--mode", "naive", "--group", group, "--until-idle", "4").returncode == 0

    wrong = wrong_balances(connection, expected)
    assert len(wrong) == 1  # exactly one payment was applied twice: the one in flight at the crash
    assert wrong[0].off_by > Decimal(0)


def test_the_idempotent_consumer_ends_with_correct_balances_after_a_crash(
    connection: psycopg.Connection[Any], topics: tuple[str, str], tmp_path: Path
) -> None:
    expected = tmp_path / "expected.json"
    produce(topics, expected, payments=300)
    group = f"idempotent-{uuid.uuid4().hex}"

    assert program("consume_cli", topics, "--group", group, "--kill-at", "100").returncode == 1
    resumed = program("consume_cli", topics, "--group", group, "--until-idle", "4")

    assert resumed.returncode == 0
    assert "'duplicate': 1" in resumed.stdout  # the redelivered payment was found and skipped
    assert wrong_balances(connection, expected) == []
    assert connection.execute("SELECT count(*) FROM processed_events").fetchone() == (300,)


def test_a_poison_message_goes_to_the_dead_letter_topic_and_the_consumer_keeps_going(
    connection: psycopg.Connection[Any], topics: tuple[str, str], tmp_path: Path
) -> None:
    expected = tmp_path / "expected.json"
    produce(topics, expected, payments=200, poison=1)

    finished = program("consume_cli", topics, "--group", f"poison-{uuid.uuid4().hex}", "--until-idle", "4")

    assert finished.returncode == 0
    assert "'dead-lettered': 1" in finished.stdout
    assert "'applied': 200" in finished.stdout
    assert wrong_balances(connection, expected) == []  # the payments before and after the bad one were all applied
```

- Lines 57 to 69: **the naive consumer after a crash.** 300 payments are produced. The naive consumer is started with `--kill-at 100` and exits with code 1. It is started again with `--until-idle 4` and exits with code 0. Then exactly one account is wrong (`len(wrong) == 1`), and it holds too much (`off_by > 0`): the payment in flight at the crash was applied twice.
- Lines 72 to 85: **the idempotent consumer after the same crash.** The restarted consumer reports `'duplicate': 1` in its output (the redelivered payment found and skipped), no balance is wrong, and `processed_events` holds exactly 300 rows.
- Lines 88 to 99: **a poison message.** 200 payments plus 1 malformed event: the consumer finishes with code 0, reports `'dead-lettered': 1` and `'applied': 200`, and every balance is right.

```python
def evictions(output: str) -> int:
    match = re.search(r"'evictions': (\d+)", output)
    assert match, output
    return int(match.group(1))


def test_slow_batches_get_the_consumer_evicted_and_smaller_batches_do_not(
    connection: psycopg.Connection[Any], topics: tuple[str, str], tmp_path: Path
) -> None:
    produce(topics, tmp_path / "expected.json", payments=300)
    slow = ("--delay", "0.1", "--max-poll-interval-ms", "3000", "--run-seconds", "10")

    evicted = program("consume_cli", topics, "--group", f"evict-{uuid.uuid4().hex}", *slow, "--max-poll-records", "100")
    fixed = program("consume_cli", topics, "--group", f"fixed-{uuid.uuid4().hex}", *slow, "--max-poll-records", "10")

    assert evictions(evicted.stdout) >= 1
    assert "poll timeout" in evicted.stderr.lower()
    assert evictions(fixed.stdout) == 0


def test_the_checker_and_health_commands_report_the_state(
    connection: psycopg.Connection[Any], topics: tuple[str, str], tmp_path: Path
) -> None:
    expected = tmp_path / "expected.json"
    produce(topics, expected, payments=50)
    program("consume_cli", topics, "--group", f"check-{uuid.uuid4().hex}", "--until-idle", "4")

    assert "all 50 balances match" in program("check_cli", topics, "--expected", str(expected)).stdout
    connection.execute("UPDATE balances SET total = total + 1 WHERE account_id = 'acct-001'")
    wrong = program("check_cli", topics, "--expected", str(expected))
    assert wrong.returncode == 1
    assert "MISMATCH acct-001" in wrong.stdout
    assert program("health_cli", topics).returncode == 0
```

- Lines 102 to 105: helper `evictions(output)` pulls the eviction count out of the printed counters with a regular expression (`re.search(r"'evictions': (\d+)", output)`; `\d+` is one or more digits).
- Lines 108 to 119: **eviction.** With 0.1 s of work per record and a 3-second poll interval, a consumer that takes 100 records per poll (10 seconds of work) is evicted at least once and its error output mentions a poll timeout; one that takes 10 per poll (1 second) is never evicted. This is the lesson's fix: smaller batches.
- Lines 122 to 134: **the checker and the health command.** After 50 payments the checker says `all 50 balances match`. The test then adds 1 to one account directly in SQL, and the checker exits 1 and prints `MISMATCH acct-001`. The health command exits 0.

## Try it yourself

```bash
.venv/bin/python -m pytest tests/unit/domain -v
```

Expected: a list of the domain tests, each marked `PASSED`, and a summary line `37 passed`.

Break something on purpose: in `src/idempotent_consumer/domain/payments.py` change `amount <= 0` to `amount < 0`, run the same command, and see the zero-amount case of `test_a_bad_amount_is_poison` fail, with both sides of the failed `assert` printed. Then undo the change with `git checkout src/idempotent_consumer/domain/payments.py`.

## Mistakes people make

- A test that depends on another test's leftovers: each integration test gets its own topics and its own group id, and the `connection` fixture empties the tables first.
- Real `sleep` in unit tests: this repository injects fake clocks and sleeps so the 98 unit tests finish in well under a second.
- Using a real password in a test: tests use a made-up value (`s3cret-value`) and the real password is only ever in the environment.

## Check yourself

1. What makes pytest treat a function as a test? (A name starting with `test_` in a file whose name starts with `test_`.)
2. What does a fixture with `yield` do after the test finishes? (Runs the code after the `yield`: the clean-up.)
3. Why does the idempotent end-to-end test expect `'duplicate': 1`? (The kill happens after the database commit and before the offset commit, so exactly the record in flight is delivered again.)
