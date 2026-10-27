# Python

## What it is

Python is a programming language. You write instructions in text files that end in `.py`. A program called the **interpreter** reads the file and carries out the instructions one at a time. This repository needs Python 3.12 or newer (`requires-python = ">=3.12"` in `pyproject.toml`); the continuous-integration job uses 3.12 and the author tested with 3.13.

A **module** is one `.py` file. A **package** is a folder of modules that has a name. This repository's package is `idempotent_consumer`.

## Why this repository uses it

The lesson's starting code is Python. All the code here is Python, so reading it well means knowing the language features it relies on. They are listed below, each at the place it is used.

## Where it appears

| Path | What it does with Python |
| --- | --- |
| `src/idempotent_consumer/domain/` | Payments, validation, dead letters, balance comparison. Standard library only. |
| `src/idempotent_consumer/application/` | Use cases and *ports* (descriptions of what the use cases need from the outside world). |
| `src/idempotent_consumer/infrastructure/` | Code that talks to Kafka and PostgreSQL, reads the environment, logs, retries. |
| `src/idempotent_consumer/interfaces/` | The command-line programs; each has a `main()` function. |
| `tests/` | Python test functions run by pytest. |
| `pyproject.toml` | Which Python version is needed and where the packages are. |

## How it works underneath

You start a program with `python -m idempotent_consumer.interfaces.consume_cli`. The `-m` flag means "run this module by its package name". The interpreter finds the package, reads the file, turns it into bytecode and runs it from the top.

Everything is an **object**: numbers, text, functions, classes. A **variable** is a name pointing at an object. A **class** is a recipe for making objects.

Python does not check types before it runs. **Type hints** (`amount: Decimal`) say what you expect; the interpreter ignores them. mypy (`tech/mypy.md`) reads them and finds contradictions without running the code.

An **import** loads another module once. The **standard library** is what ships with Python; anything else must be installed (`tech/pip-venv-and-lock-files.md`).

## Walkthrough of this repository's use

### Postponed type hints

```python
from __future__ import annotations
```

Almost every file starts with this. It keeps type hints as text instead of working them out when the file loads.

### Dataclasses: small objects with named fields

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

`@dataclass` is a **decorator** (a line starting with `@` that wraps what follows). It writes `__init__`, `__eq__` and `__repr__` from the fields. `frozen=True` makes instances read-only: assigning to `payment.amount` raises an error. That is an **immutable value object**. `slots=True` fixes the set of fields, saving memory. `payload` is a **method** (a function inside a class); `self` is the object it is called on.

### Exact decimals, not floats

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

`Decimal` is an exact decimal number. A plain `float` stores 0.1 slightly wrong, and sums drift; money must not. `Decimal(str(value))` builds it from the text of the number. `try: ... except InvalidOperation as error:` catches a failure to convert (such as `"not-a-number"`) and `raise PoisonMessageError(...) from error` replaces it with this project's own named error, keeping the original as the cause. `isinstance(value, bool)` is checked first because in Python `True` is also the number 1. `amount.as_tuple().exponent` is the power of ten of the last digit (`-2` for cents); an exponent below `-2` means more than two decimals, which is refused too.

### Comprehensions and generated ids

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

A **list comprehension** `[expression for _ in range(count)]` builds a list by repeating the expression. `rng` is a `random.Random` passed in, so a test with a fixed seed gets the same payments every time. `uuid.UUID(int=rng.getrandbits(128), version=4)` builds a unique id from random bits.

### Protocols: describing what you need

```python
class BalanceLedger(Protocol):
    """Applies payments to account balances."""

    def apply(self, payment: Payment, delivery: Delivery) -> Outcome:
        """Add the payment to its account in one database transaction.

        Returns APPLIED or DUPLICATE. Raises `PoisonMessageError` for a payment that can never be
        applied (an unknown account); nothing is written in that case.
        """


class BalanceReader(Protocol):
```

A `Protocol` lists methods a class must have. `NaiveLedger` and `IdempotentLedger` both have `apply`, so both count as a `BalanceLedger` without saying so. The use case asks for a `BalanceLedger`; tests pass a fake in memory, the program passes the PostgreSQL one.

### Functions passed as values, a closure, and `os._exit`

```python
def kill_switch(kill_at: int, name: str) -> Callable[[int], None]:
    """Crash like a killed process: no cleanup, no offset commit, no group leave."""

    def after_apply(processed: int) -> None:
        if kill_at and processed == kill_at:
            print(
                f"[{name}] kill switch: crashing after the database commit of record {processed}, "
                "before its offset commit",
                flush=True,
            )
            os._exit(1)

    return after_apply
```

`kill_switch` returns a function (`after_apply`) that remembers `kill_at` and `name` from the outside: a **closure**. `Callable[[int], None]` means "a function taking one int and returning nothing". `os._exit(1)` ends the process immediately with exit code 1, with no cleanup, no commit and no goodbye to Kafka, which is exactly what a crash looks like.

### Generic functions, keyword-only arguments

```python
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

`def retry[T](...)` (Python 3.12 syntax) declares a type variable: whatever `operation` returns, `retry` returns. The `*,` makes every following parameter keyword-only (`attempts=5`).

### The entry point

```python
if __name__ == "__main__":
    raise SystemExit(main())
```

`main` returns an exit code; `0` means success. `__name__ == "__main__"` is true only when the file is run directly or with `-m`, so a test can import the module without starting the program.

### Other standard-library parts used

`argparse` (command-line options), `signal` and `threading.Event` (graceful shutdown, `infrastructure/shutdown.py`), `logging` (`tech/logfmt.md`), `json` (`tech/json.md`), `subprocess` (the crash test runs the consumer as a separate process), `base64` (exact bytes in a dead letter), `os.environ` (settings).

## Try it yourself

1. `.venv/bin/python --version` prints `Python 3.13.x` (3.12 or newer is fine).
2. `.venv/bin/python -m idempotent_consumer.interfaces.consume_cli --help | head -3` prints `usage: consume_cli.py [-h] [--mode {naive,idempotent}] ...`.
3. Floats drift, decimals do not: `.venv/bin/python -c "from decimal import Decimal; print(0.1 + 0.2, Decimal('0.1') + Decimal('0.2'))"` prints `0.30000000000000004 0.3`.
4. A frozen dataclass: `.venv/bin/python -c "from decimal import Decimal; from idempotent_consumer.domain.payments import Payment; p = Payment('1','a',Decimal(1)); p.amount = Decimal(2)"` ends with `dataclasses.FrozenInstanceError: cannot assign to field 'amount'`.
5. Validation by raising: `.venv/bin/python -c "from idempotent_consumer.domain.payments import parse_amount; parse_amount('not-a-number')"` ends with `PoisonMessageError: could not convert amount to a number: 'not-a-number'`.

## Mistakes people make

- Using floats for money.
- Treating type hints as run-time checks.
- Catching every exception with a bare `except:`. This code catches named errors only.
- Using a mutable default argument (`def f(x=[])`). Defaults here are immutable or `lambda`.
- Letting `import` start the program. The `__main__` guard prevents it.

## Check yourself

1. Why `Decimal` for amounts? *It is exact; floats drift.*
2. What does `frozen=True` do? *Instances cannot be changed after they are built.*
3. What does `os._exit(1)` skip that `sys.exit(1)` does not? *Cleanup: no `finally` blocks, no closing of connections.*
4. What is a closure? *A function that remembers variables from where it was defined.*
5. Why does `main` return an int? *It becomes the process exit code.*

## Further reading

- Python 3.12 documentation: https://docs.python.org/3.12/
- `decimal`: https://docs.python.org/3.12/library/decimal.html
- Data classes: https://docs.python.org/3.12/library/dataclasses.html
- `typing.Protocol`: https://docs.python.org/3.12/library/typing.html#typing.Protocol
- Generic syntax (PEP 695): https://docs.python.org/3.12/reference/compound_stmts.html#generic-functions
