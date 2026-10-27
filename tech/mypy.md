# mypy

## What it is

mypy is a **type checker** for Python. You add **type hints** to your code (`def f(x: int) -> str`), which say what kinds of values go in and out. Python itself ignores hints when it runs. mypy reads the code without running it and reports places where the hints contradict each other, such as passing text where a number is expected. This repository pins mypy 2.4.0.

## Why this repository uses it

The template asks for typed code checked in CI. Types also document the code: the ports in `application/ports.py` say exactly what an adapter must provide.

## Where it appears

| Path | What it does |
| --- | --- |
| `pyproject.toml` | `[tool.mypy]`: strict mode, which files to check, exceptions for libraries without types. |
| `Makefile` (`lint`) | Runs `mypy`. |
| `src/`, `tests/` | The code being checked. |

## How it works underneath

mypy parses each file, works out the type of every expression from the hints and from what it can infer, and compares. `strict = true` switches on all the strictness options: every function needs hints, `Any` is not allowed to leak silently, unused ignores are errors. It reports `file:line: error: message  [code]`, or `Success: no issues found`.

## Walkthrough of this repository's use

```toml
[tool.mypy]
python_version = "3.12"
strict = true
files = ["src", "tests"]

[[tool.mypy.overrides]]
module = ["kafka", "kafka.*"]
ignore_missing_imports = true

# Test functions are named for the behavior they check; annotating every one adds noise, not safety.
[[tool.mypy.overrides]]
module = ["tests.*"]
disallow_untyped_defs = false
disallow_incomplete_defs = false
disallow_untyped_calls = false
```

- `python_version = "3.12"`, `strict = true`, `files = ["src", "tests"]`: check both folders as Python 3.12 in strict mode, so you just type `mypy`.
- The first `[[tool.mypy.overrides]]` says the modules `kafka` and `kafka.*` have no type information; `ignore_missing_imports = true` tells mypy to treat them as `Any` instead of failing. That is the reason `Any` appears in the Kafka adapters (psycopg ships its own types and needs no override).
- The second override relaxes only `tests.*`: test functions need not carry hints, because their names already describe them and annotating all of them adds noise.

This repository has no `# type: ignore` comments: the only untyped library is `kafka`, and the override above covers it.

Where hints do real work: the ports.

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

An adapter whose `apply` takes different arguments, or returns something other than an `Outcome`, makes mypy fail wherever it is passed as a `BalanceLedger`. Both `NaiveLedger` and `IdempotentLedger` must satisfy this one description.

## Try it yourself

1. `.venv/bin/mypy` prints `Success: no issues found in 57 source files`.
2. Introduce a type error: in `src/idempotent_consumer/domain/balances.py` change `-> list[Mismatch]:` to `-> int:` on `diff_balances` and run `.venv/bin/mypy`; it reports `Incompatible return value type`. Undo with `git checkout src`.
3. `.venv/bin/mypy --version` prints `mypy 2.4.0`.

## Mistakes people make

- Believing hints are enforced at run time. They are not.
- Using `Any` to make errors go away. Here `Any` is limited to the untyped Kafka client.
- Adding `# type: ignore` without a code or a reason.
- Forgetting that mypy only sees what is annotated; `strict` forces annotations.

## Check yourself

1. Does mypy run your program? *No, it analyses the code.*
2. What does `strict = true` do? *Turns on all strictness checks, such as requiring hints on every function.*
3. Why is `Any` used for the Kafka client? *The library ships no type information.*
4. What does `ignore_missing_imports` do? *Treats an untyped library as `Any` instead of reporting an error.*
5. Why are tests exempt from the "must be annotated" rule? *Their names describe them; annotations would add noise.*

## Further reading

- mypy 2.4 documentation: https://mypy.readthedocs.io/en/stable/
- Type hints cheat sheet: https://mypy.readthedocs.io/en/stable/cheat_sheet_py3.html
- Python typing module: https://docs.python.org/3.12/library/typing.html
