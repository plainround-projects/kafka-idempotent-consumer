# Python: the domain layer, file by file

## What it is

The folder `src/idempotent_consumer/domain/` holds the **business rules** of the project: what a payment event is, how a bad one is recognised, how to tell whether a balance is right, what a parked "dead letter" contains, and where the crash test kills the consumer. "Business rules" means rules that would stay true if Kafka and PostgreSQL were thrown away. This is the innermost layer, and its one rule is: it may import only from Python's standard library (the modules that ship with Python). `make lint` fails if a file here imports `kafka` or `psycopg`.

Every file is walked through below, with every line quoted and explained. If you know no Python, each idea is explained at the line where it first appears. `tech/python.md` introduces the same ideas in a different order.

Two words you need before starting:

- A **module** is one file whose name ends in `.py`. A **package** is a folder of modules that contains a file named `__init__.py`. Here `idempotent_consumer` is the package and `idempotent_consumer.domain` a package inside it.
- A **docstring** is text in triple quotes (`"""..."""`) at the start of a file, class or function. Python keeps it so people and tools can read what the thing is for. It does nothing when the program runs.

## Why this repository uses it

The lesson's promise is that every payment changes a balance exactly once. The rules behind it (what a valid payment is, why money is an exact decimal, how to tell that a balance is wrong, what a parked bad record must contain, where the crash test strikes) need no database and no broker. Keeping them in a package that imports only the standard library means they can be tested in milliseconds and cannot depend on how data is stored.

## Where it appears

| File | What it holds |
| --- | --- |
| `src/idempotent_consumer/__init__.py` | The package's docstring and version. |
| `domain/__init__.py` | Marks the folder as a package. |
| `domain/errors.py` | The one named error. |
| `domain/payments.py` | The payment event, building test payments, the expected totals and validating an incoming record. |
| `domain/balances.py` | Comparing the database's balances with the totals they must have. |
| `domain/dead_letter.py` | The record that is parked when an event can never be processed. |
| `domain/crash_plan.py` | The positions at which the crash test kills the consumer. |

## How it works underneath

When Python loads a module it runs the file once, top to bottom. `import` lines load other modules; `class` and `def` lines create classes and functions; nothing inside a function runs until the function is called. A **class** is a blueprint and an **object** is one thing built from it. The `@dataclass` decorator reads the class's field annotations when the class is created and writes `__init__`, `__eq__` and `__repr__` for it; `frozen=True` makes assignment to a field raise an error. When code **raises** an error, Python stops the current function and looks up the chain of callers for a matching `except`; if none is found the program ends with a traceback. **Type hints** are ignored when the program runs; `mypy` reads them without running the code. The rule that this layer imports only the standard library is checked by `import-linter`, which reads the `import` lines of every file, builds a graph of who imports whom and fails if an arrow points the wrong way. Here money is a `Decimal`, built from text, never a `float`: `0.1 + 0.2` is exactly `0.3` for a `Decimal` and `0.30000000000000004` for a float. Randomness is passed in as a seeded generator, so the same seed gives the same payments and the same event ids.

## Walkthrough of this repository's use

Every source file of this layer is read below, in the order of the table above. Each block is a piece of the real file, copied exactly, followed by an explanation in plain words.

### `src/idempotent_consumer/__init__.py` and `domain/__init__.py`

```python
"""A Kafka consumer that keeps PostgreSQL account balances correct under at-least-once delivery."""

__version__ = "0.1.0"
```

- Line 1 is the docstring: one sentence stating what the project guarantees. "At-least-once delivery" means Kafka may deliver the same record more than once but never loses one, which is why balances can go wrong without care.
- Line 3 creates a variable `__version__` holding the text `"0.1.0"`. The double underscores signal "read by tools". `pyproject.toml` has the same number.

```python
"""Pure business rules: payments, validation, dead letters, balance comparison. Standard library only."""
```

Only a docstring. A file with this name makes the folder a package. The other layer folders have an `__init__.py` of the same kind.

### `domain/errors.py`

```python
"""Named errors of the domain."""


class PoisonMessageError(ValueError):
    """The record can never be processed, however often it is retried."""
```

- Line 4: `class PoisonMessageError(ValueError):` defines a new kind of error. A **class** is a recipe for making objects; the name in brackets is its **parent**. `ValueError` is a built-in error meaning "the value has the right type but is not acceptable"; the new error is a specialised version of it.
- Line 5: the docstring gives the meaning. A **poison message** is a record that can never be processed, however often it is retried (it is not valid JSON, or the amount is text). The consumer's rule is: do not retry a poison message forever; park it in the dead-letter topic and move on.
- The class has no body beyond the docstring. Its value is its name, which a log line or a test can match exactly.

### `domain/payments.py`

This is the largest file. It is read in seven pieces.

#### Imports and constants

```python
"""The payment event: how it is built, validated and compared. No I/O beyond reading JSON bytes."""

from __future__ import annotations

import json
import random
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from .errors import PoisonMessageError

ACCOUNTS: tuple[str, ...] = tuple(f"acct-{number:03d}" for number in range(1, 51))
CENTS = Decimal("0.01")
```

- Line 3: `from __future__ import annotations` keeps type hints (notes such as `count: int`) as text, so a hint may mention a class defined later in the file. Python itself ignores hints; `mypy` reads them.
- Lines 5 to 7: `json` (read and write JSON text), `random` (the type of the random-number generator passed in) and `uuid` (universally unique identifiers).
- Line 8: `Iterable` is a hint for "anything you can loop over".
- Line 9: `dataclass` is a decorator (explained at line 19).
- Line 10: `from decimal import Decimal, InvalidOperation`. **`Decimal` is the type for money.** Computers store ordinary decimal fractions such as 0.1 as binary approximations: in Python, `0.1 + 0.2` is `0.30000000000000004`. For money that is not acceptable, so amounts are `Decimal`, which stores digits exactly: `Decimal("0.1") + Decimal("0.2")` is exactly `0.3`. `InvalidOperation` is the error `Decimal` raises when text is not a number.
- Line 11: `Any` in a hint means "any type".
- Line 13: `from .errors import PoisonMessageError`: the single dot means "from the same package".
- Line 15: `ACCOUNTS` is a tuple (a read-only list) of 50 account ids. The expression in round brackets is a **generator expression**: `f"acct-{number:03d}" for number in range(1, 51)` builds one text per number from 1 to 50. In `{number:03d}`, the part after the colon is a format: `d` whole number, `3` at least three characters, `0` pad with zeros. So the ids are `acct-001` to `acct-050`. The hint `tuple[str, ...]` says "a tuple of any number of texts".
- Line 16: `CENTS = Decimal("0.01")` is one cent. The name in capitals is a constant.

#### The payment

```python
@dataclass(frozen=True, slots=True)
class Payment:
    """One payment into one account. `event_id` identifies the event, not its place in the log."""

    event_id: str
    account_id: str
    amount: Decimal

    def payload(self) -> dict[str, Any]:
        """The event as the producer sends it: two-decimal numbers in JSON."""
        return {"eventId": self.event_id, "accountId": self.account_id, "amount": float(self.amount)}
```

- Line 19: `@dataclass(frozen=True, slots=True)`. A line starting with `@` above a class is a **decorator**: it changes the class below. `@dataclass` writes the repetitive methods for a data-holding class: how to build one, compare two and print one. `frozen=True` makes the object read-only after it is built, so no other code can change a payment by accident. `slots=True` stores the fields in a fixed compact layout.
- Lines 23 to 25: three **fields**, each `name: type`. `event_id` is a text that identifies the *event*; the docstring stresses it identifies the event and not its place in the log. This is the whole point of the project: Kafka may deliver one event twice (at two different offsets), and the event id is how the database recognises that it has already seen it. `account_id` is a text. `amount` is a `Decimal`.
- Lines 27 to 29: a **method** (a function inside a class; its first parameter `self` is the object itself) that returns the dictionary the producer sends as JSON. `float(self.amount)` turns the exact decimal into an ordinary number for JSON, which has no decimal type. Because every amount has at most two decimals, the number prints back exactly as typed.

#### Building test payments

```python
def build_payments(count: int, rng: random.Random, accounts: tuple[str, ...] = ACCOUNTS) -> list[Payment]:
    """`count` payments spread over the accounts, amounts from 1.00 to 500.00, ids drawn from `rng`."""
    return [
        Payment(
            event_id=str(uuid.UUID(int=rng.getrandbits(128), version=4)),
            account_id=rng.choice(accounts),
            amount=Decimal(rng.randint(100, 50_000)) * CENTS,
        )
        for _ in range(count)
    ]
```

- Line 32: the function takes `count`, a random-number generator `rng` (passed in, so a seeded generator gives the same payments every time, which the crash test depends on), and the accounts to spread over (default `ACCOUNTS`).
- Lines 34 to 41: a **list comprehension** (`[ ... for _ in range(count)]`) builds `count` payments. The loop variable is called `_`, the convention for "I do not use this value".
- Line 36: `uuid.UUID(int=rng.getrandbits(128), version=4)` makes a UUID from 128 random bits taken from the seeded generator, marked as version 4. `str(...)` writes it as text like `cd613e30-d8f1-4adf-91b7-584a2265b1f5`. Using the seeded generator and not `uuid.uuid4()` makes the ids repeatable.
- Line 37: `rng.choice(accounts)` picks one account at random.
- Line 38: `Decimal(rng.randint(100, 50_000)) * CENTS`: a random whole number of cents from 100 to 50,000 (`50_000` is just `50000`, underscores make long numbers readable), multiplied by one cent, so the amount is between 1.00 and 500.00 with exactly two decimals.

#### What the balances must be

```python
def expected_totals(payments: Iterable[Payment], accounts: tuple[str, ...] = ACCOUNTS) -> dict[str, Decimal]:
    """What each balance must be, computed from the payments alone and never from the database."""
    totals = {account: Decimal("0.00") for account in accounts}
    for payment in payments:
        totals[payment.account_id] += payment.amount
    return totals
```

- Line 46: a **dictionary comprehension** `{account: Decimal("0.00") for account in accounts}` makes a dictionary (name to value) with every account at zero.
- Lines 47 and 48: loop over the payments and add each amount to its account. `totals[payment.account_id] += payment.amount` means "add to the value stored under that key".
- The docstring states the key idea: expected totals are computed from the *payments alone*, never read back from the database. The check at the end of a test compares the database with this independent calculation.

#### Validating an incoming record

```python
def parse_payment(raw: bytes) -> Payment:
    """Validate one record's value. Raises PoisonMessageError with the reason when it is unusable."""
    try:
        data = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as error:
        raise PoisonMessageError(f"not valid JSON: {error}") from error
    if not isinstance(data, dict):
        raise PoisonMessageError("payload is not a JSON object")
    try:
        event_id = str(uuid.UUID(str(data["eventId"])))
    except (KeyError, ValueError) as error:
        raise PoisonMessageError(f"eventId missing or not a uuid: {data.get('eventId')!r}") from error
    account_id = data.get("accountId")
    if not isinstance(account_id, str) or not account_id:
        raise PoisonMessageError(f"accountId missing or not text: {account_id!r}")
    return Payment(event_id, account_id, parse_amount(data.get("amount")))
```

- Line 52: `parse_payment(raw: bytes) -> Payment` takes the record's value (**bytes**: raw numbers from 0 to 255; the network carries bytes) and returns a `Payment` or raises.
- Lines 54 to 57: `try:` begins a block whose errors can be caught. `json.loads(raw)` parses JSON text into Python values; bad JSON raises a `ValueError` (`json.JSONDecodeError` is a kind of it) and bytes that are not valid text raise `UnicodeDecodeError`. `except (ValueError, UnicodeDecodeError) as error:` catches both and `raise PoisonMessageError(...) from error` throws the named error, keeping the original as its cause (`from error`) so the traceback still shows what failed.
- Lines 58 and 59: `isinstance(data, dict)` asks "is this a dictionary?". JSON such as `[]` parses fine but is not an object, so it is poison.
- Lines 60 to 63: `str(uuid.UUID(str(data["eventId"])))` reads the key (a missing key raises `KeyError`), checks the text is a valid UUID (an invalid one raises `ValueError`) and writes it back in the standard form. `data.get('eventId')!r` in the message: `.get` returns `None` when the key is missing, and `!r` inside an f-string prints the value with quotes so spaces and `None` are visible.
- Lines 64 to 66: the account id must be text and not empty. `not isinstance(...) or not account_id` is true for a missing value, a number or an empty string.
- Line 67: builds the `Payment`, with the amount checked by the next function.

```python
def parse_amount(value: object) -> Decimal:
    """Validate a payment amount: a positive number with at most two decimals."""
    if isinstance(value, bool) or value is None:
        raise PoisonMessageError(f"amount is not a number: {value!r}")
    try:
        amount = Decimal(str(value))
    except InvalidOperation as error:
        raise PoisonMessageError(f"could not convert amount to a number: {value!r}") from error
    exponent = amount.as_tuple().exponent
    if not amount.is_finite() or amount <= 0 or (isinstance(exponent, int) and exponent < -2):
        raise PoisonMessageError(f"amount must be positive with at most two decimals: {value!r}")
    return amount
```

- Line 72: `isinstance(value, bool) or value is None`. In Python `True` is a kind of number (`True == 1`), so `bool` must be rejected on its own, or a payment of `true` would become 1.
- Lines 74 to 77: `Decimal(str(value))` converts via text. Going through `str` matters: `Decimal(0.1)` would copy the binary approximation of 0.1, but `Decimal("0.1")` is exactly 0.1. Text such as `"abc"` raises `InvalidOperation`, converted to the named error.
- Line 78: `amount.as_tuple().exponent` is the position of the last decimal digit: `-2` for `12.34`, `-1` for `12.3`, `0` for `12`, `-3` for `12.345`.
- Lines 79 and 80: poison if the value is not finite (`NaN`, infinity), is zero or negative, or has more than two decimals (`exponent < -2`). The `isinstance(exponent, int)` check satisfies the type checker, because the exponent of `NaN` is a letter code, not a number. The checks mean that `"12.34"`, `12.34` and `5` are accepted and `12.345`, `0`, `-1` and `"NaN"` are refused. (A value written with a positive exponent such as `"1e2"` is read as the number 100 and accepted.)
- Line 81: return the validated `Decimal`.

#### Making the lesson's poison event

```python
def poison_payload(rng: random.Random, account_id: str = "acct-012") -> dict[str, Any]:
    """The lesson's malformed event: the amount is text that is not a number."""
    return {
        "eventId": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "accountId": account_id,
        "amount": "not-a-number",
    }


def with_poison(payloads: list[dict[str, Any]], poison_count: int, rng: random.Random) -> list[dict[str, Any]]:
    """The payloads with `poison_count` malformed ones inserted at evenly spaced positions.

    Valid payments follow every malformed one, so the consumer has to move on to prove it can.
    """
    mixed = list(payloads)
    # Insert from the back so the positions computed for earlier ones stay valid.
    for number in reversed(range(poison_count)):
        mixed.insert(len(payloads) * (number + 1) // (poison_count + 1), poison_payload(rng))
    return mixed
```

- Lines 84 to 90: `poison_payload` returns a dictionary shaped like a payment whose amount is the text `"not-a-number"`. Its id again comes from the seeded generator. The default `account_id="acct-012"` is a default parameter value.
- Lines 93 to 102: `with_poison(payloads, poison_count, rng)` returns a copy of the list of payloads with `poison_count` poison events inserted at evenly spaced positions, so valid payments follow every poison event ("the consumer has to move on to prove it can"). `list(payloads)` makes a copy so the caller's list is untouched. The loop `for number in reversed(range(poison_count))` goes from the last to the first; inserting from the back keeps the positions of the earlier insertions valid (the comment says so). `//` is whole-number division. For 10 payloads and 3 poison events the positions come out as 2, 6 and 9 in the final list.

### `domain/balances.py`

```python
"""Compare balances with the totals they must have."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class Mismatch:
    """An account whose balance differs from what it must be."""

    account_id: str
    expected: Decimal
    actual: Decimal

    @property
    def off_by(self) -> Decimal:
        return self.actual - self.expected

    def __str__(self) -> str:
        return f"MISMATCH {self.account_id}: expected {self.expected}, got {self.actual} (off by {self.off_by:+})"


def diff_balances(expected: Mapping[str, Decimal], actual: Mapping[str, Decimal]) -> list[Mismatch]:
    """The accounts whose actual balance differs from the expected one, sorted by account id."""
    zero = Decimal("0")
    return [
        Mismatch(account, expected.get(account, zero), actual.get(account, zero))
        for account in sorted(set(expected) | set(actual))
        if expected.get(account, zero) != actual.get(account, zero)
    ]
```

- Lines 10 to 16: `Mismatch` is a frozen dataclass: the account, the expected balance and the actual balance.
- Lines 18 to 20: `@property` lets you write `mismatch.off_by` with no brackets while a function runs underneath. It returns actual minus expected.
- Lines 22 to 23: `__str__` is the special method Python calls to turn the object into text. The format `{self.off_by:+}` always writes the sign: `+0.50` or `-0.50`. A printed mismatch looks like `MISMATCH acct-047: expected 10.00, got 10.50 (off by +0.50)`.
- Lines 26 to 33: `diff_balances(expected, actual)` takes two dictionaries (`Mapping` is the hint for a read-only one). `set(expected) | set(actual)` is the **union** of the two sets of account ids, so an account that appears on only one side is still compared; `sorted(...)` gives account order. The `if` at the end of the comprehension keeps only the accounts whose values differ. `expected.get(account, zero)` is the value or zero when absent. The comparison uses `Decimal` equality, so `1` and `1.00` are equal and do not produce a false mismatch. An empty list means every balance is right.

### `domain/dead_letter.py`

```python
"""A record parked in the dead-letter topic, with everything needed to find and replay it."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class DeadLetter:
    """A record parked in the dead-letter topic, with where it came from and why."""

    original_topic: str
    original_partition: int
    original_offset: int
    error: str
    raw: bytes

    def payload(self) -> dict[str, Any]:
        return {
            "originalTopic": self.original_topic,
            "originalPartition": self.original_partition,
            "originalOffset": self.original_offset,
            "error": self.error,
            "raw": self.raw.decode("utf-8", errors="replace"),
            # The exact bytes, so a replay is byte-faithful even when they were not valid text.
            "rawBase64": base64.b64encode(self.raw).decode("ascii"),
        }
```

A **dead-letter topic** is a Kafka topic where unprocessable records are parked, with enough information to find and replay them.

- Line 5: `base64` encodes any bytes as plain text characters.
- Lines 10 to 18: the frozen dataclass has five fields: the original topic, partition and offset (exactly where the record sat), the error text and the record's raw bytes.
- Lines 20 to 29: `payload()` builds the dictionary written to the dead-letter topic as JSON. `self.raw.decode("utf-8", errors="replace")` turns bytes into readable text, swapping any invalid byte for the replacement character; that copy is for humans. `base64.b64encode(self.raw).decode("ascii")` is the exact bytes as text, so a later replay is byte-for-byte faithful even if the original was not valid text (the comment says so). Example: the bytes `ff 20 61 62 63` become the readable `� abc` and the exact `/yBhYmM=`.

### `domain/crash_plan.py`

```python
"""Where the crash test kills the consumer."""

from __future__ import annotations


def kill_points(runs: int, payments: int) -> list[int]:
    """`runs` different record numbers spread over 1..payments, the same every time."""
    if runs < 1 or payments < 1:
        raise ValueError("runs and payments must be at least 1")
    step = max(payments // (runs + 1), 1)
    return [min(payments, step * run + (run * 37) % max(step // 2, 1)) for run in range(1, runs + 1)]
```

The crash test kills the consumer in the middle of its work, several times, at different record numbers.

- Lines 8 and 9: refuse a number of runs or payments below 1.
- Line 10: `step = max(payments // (runs + 1), 1)`: the gap between kill points, at least 1. `max` returns the larger of two values.
- Line 11: for run number 1, 2, ... `runs`, the kill point is `step * run + (run * 37) % max(step // 2, 1)`, never above `payments` (`min`). `%` is the remainder after division. The multiplication by 37 adds a little irregular offset so the kill points do not sit on round numbers like 2,500 and 5,000. The result is the same every time (there is no randomness): `kill_points(3, 10000)` is `[2537, 5074, 7611]`, and `kill_points(5, 10000)` is `[1703, 3406, 5109, 6812, 8515]`.

## Try it yourself

```bash
.venv/bin/python -c "
import random
from idempotent_consumer.domain.payments import build_payments, parse_amount, PoisonMessageError
payments = build_payments(3, random.Random(1))
print(payments[0])
for text in ['12.34', '12.345', 0, True]:
    try:
        print(repr(text), '->', parse_amount(text))
    except PoisonMessageError as error:
        print(repr(text), 'refused:', error)
"
```

Expected: the first payment (with an exact `Decimal` amount); then `'12.34' -> 12.34`, and `12.345`, `0` and `True` each refused with the reason.

```bash
.venv/bin/python -c "
from idempotent_consumer.domain.crash_plan import kill_points
print(kill_points(3, 10000))
"
```

Expected: `[2537, 5074, 7611]`.

## Mistakes people make

- Using `float` for money. Always `Decimal`, built from text.
- Forgetting that `True` is a number in Python (`isinstance(True, int)` is true).
- Computing expected totals by reading the database: then a bug that corrupts the database also corrupts the expectation, and the check passes.

## Check yourself

1. Why does `parse_amount` call `Decimal(str(value))` and not `Decimal(value)`? (A `float` carries a binary approximation; going through text keeps what was typed.)
2. Why is the dead letter stored both as readable text and as base64? (Readable for people; base64 for an exact replay.)
3. What does an empty result from `diff_balances` mean? (Every account's balance equals what it must be.)
4. What does `parse_amount` accept and what does it refuse? (It accepts a positive, finite number with at most two decimals (`"12.34"`, `12.34`, `5`). It refuses 0, negatives, `NaN`, a boolean, `None`, text that is not a number and anything with more than two decimals.)
5. Why do `kill_points(3, 10000)` give the same list every time? (The positions come from arithmetic on the number of runs and payments, with no randomness: `[2537, 5074, 7611]`.)

## Further reading

- Python 3.12 `dataclasses`: https://docs.python.org/3.12/library/dataclasses.html
- Python 3.12 `enum`: https://docs.python.org/3.12/library/enum.html
- Python 3.12 data model (classes, special methods): https://docs.python.org/3.12/reference/datamodel.html
- Python 3.12 built-in exceptions: https://docs.python.org/3.12/library/exceptions.html
- Python 3.12 `decimal`: https://docs.python.org/3.12/library/decimal.html
- Python 3.12 `json`: https://docs.python.org/3.12/library/json.html
- Python 3.12 `uuid`: https://docs.python.org/3.12/library/uuid.html
- Python 3.12 `base64`: https://docs.python.org/3.12/library/base64.html
