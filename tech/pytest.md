# pytest

## What it is

pytest is a Python tool that finds and runs tests. A **test** is a function whose name starts with `test_`. It runs some of your code and uses `assert` to say what must be true. If an `assert` is false, the test fails and pytest shows the values. This repository pins pytest 9.1.1.

## Why this repository uses it

The template asks for tests at three levels. pytest runs all three with different commands and keeps them apart with folders and markers.

## Where it appears

| Path | What it does |
| --- | --- |
| `tests/unit/` | Fast tests with no Kafka or PostgreSQL: domain, application (with fake ports), infrastructure helpers, CLI arguments. |
| `tests/integration/` | The two ledgers against PostgreSQL; the Kafka adapters against the broker. |
| `tests/e2e/` | The lesson's scenarios (crash, idempotent crash, poison, eviction) through the command-line programs, including a process that crashes on purpose. |
| `tests/conftest.py` | Shared **fixtures** (setup code the tests can ask for). |
| `pyproject.toml` | `[tool.pytest.ini_options]`: where tests are and the marker names. |
| `Makefile` | `test`, `test-integration`, `test-e2e`. |

## How it works underneath

pytest imports every `test_*.py` file under the folders you give it, collects the functions that start with `test_`, and runs each. When a test function has a parameter (such as `settings` or `topic`), pytest looks for a fixture with that name, runs it first, and passes the result in. A fixture that uses `yield` runs its code before the `yield` as setup and the code after it as cleanup, even if the test fails. **Markers** (`@pytest.mark.integration`) label tests so you can choose them with `-m`.

## Walkthrough of this repository's use

### Tests that read as behavior, with fakes

```python
def test_each_payment_is_applied_and_its_offset_committed_after_it():
    counts, ledger, committer, _, _ = consume([[delivery(0, uid(1), 10), delivery(1, uid(2), 5)]])
    assert counts.summary() == {"applied": 2, "duplicate": 0, "dead-lettered": 0, "evictions": 0}
    assert ledger.balances["acct-001"] == Decimal(15)
    assert committer.commits == [{("payments", 0): 1}, {("payments", 0): 2}]
```

Each function name is a sentence. The test hands the use case in-memory stand-ins (`InMemoryLedger`, `RecordingCommitter`) and checks what happened with `assert`. pytest rewrites `assert` so a failure prints both sides.

### Parametrized tests: one test, many inputs

```python
@pytest.mark.parametrize("amount", ["not-a-number", None, True, [], "NaN", "Infinity", 0, -5, 1.234, "12.345"])
def test_a_bad_amount_is_poison(amount):
    with pytest.raises(PoisonMessageError):
        parse_payment(raw(amount=amount))
```

`@pytest.mark.parametrize("amount", [...])` runs the test once per list item. `with pytest.raises(PoisonMessageError):` passes only if the code inside raises that error. Each bad amount (`"NaN"`, `0`, `-5`, `1.234`, ...) is a separate result.

### Fixtures with setup and cleanup

```python
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

`@pytest.fixture` marks setup code a test can ask for by naming it as a parameter. `connection` creates the tables if missing, empties them, and `yield`s the connection to the test; the `with` block closes it afterwards. `topics` creates two uniquely named topics, `yield`s their names, and deletes them after the test. A file named `conftest.py` is read automatically.

### Markers and selecting levels

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-ra --strict-markers"
markers = [
    "integration: needs Kafka and PostgreSQL from docker-compose.yml (make up)",
    "e2e: runs the lesson's scenarios through the command-line programs against Kafka and PostgreSQL",
]
```

- `testpaths = ["tests"]`: where to look when no folder is given.
- `addopts = "-ra --strict-markers"`: options added to every run. `-ra` prints a summary of non-passing tests. `--strict-markers` makes an unknown marker an error, which catches typos.
- `markers = [...]`: the two allowed markers. A test file sets `pytestmark = pytest.mark.integration` once to label every test in it.

`make test` runs `pytest tests/unit` only. `make test-integration` runs `pytest tests/integration -m integration`.

## Try it yourself

1. `.venv/bin/python -m pytest tests/unit -q` prints a row of dots and `98 passed`.
2. `.venv/bin/python -m pytest tests/unit/domain/test_payments.py -v` lists each test name with `PASSED`.
3. `.venv/bin/python -m pytest tests/unit -k poison -v` runs only tests with `poison` in their name.
4. `.venv/bin/python -m pytest --collect-only -q -m integration tests | tail -2` prints `11/114 tests collected (103 deselected)`; with `-m e2e` it is `5/114`.
5. Break a test on purpose: in `tests/unit/domain/test_dead_letter_balances_and_crash_plan.py` change the expected `Decimal("82.48")` in `test_a_double_applied_payment_shows_as_off_by_exactly_its_amount` and watch pytest print both values; undo it with `git checkout tests`.

## Mistakes people make

- Tests that depend on each other's data. Fixtures with unique topic names, and a table reset before each test, avoid it.
- Giving a test the same name in two files without `__init__.py`, which confuses pytest. This repository has `__init__.py` in the test folders.
- Calling real time or randomness in unit tests. The code takes a clock and a `Random` as arguments, so tests are repeatable.
- Forgetting the marker, so the slow tests run with the fast ones.

## Check yourself

1. How does pytest know a function is a test? *Its name starts with `test_` in a `test_*.py` file.*
2. What does `yield` do in a fixture? *Hands the value to the test; the code after it is cleanup.*
3. What does `-m integration` do? *Runs only tests marked `integration`.*
4. What does `parametrize` do? *Runs one test function once per listed input.*
5. Why `--strict-markers`? *An unknown marker name becomes an error instead of being silently ignored.*

## Further reading

- pytest 9.1 documentation: https://docs.pytest.org/en/9.1.x/
- Fixtures: https://docs.pytest.org/en/9.1.x/how-to/fixtures.html
- Markers: https://docs.pytest.org/en/9.1.x/how-to/mark.html
