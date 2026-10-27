# pytest-cov and coverage.py

## What it is

**Coverage** measures how much of your code the tests actually ran. `coverage.py` does the measuring (it watches which lines and which branches of `if` statements execute). `pytest-cov` is a pytest plugin that turns it on with `--cov`. This repository pins pytest-cov 7.1.0 and coverage 7.17.0.

## Why this repository uses it

The template says coverage must be reported and the domain should be at or near full coverage. A number answers "did any test touch this line?", which finds code that no test protects.

## Where it appears

| Path | What it does |
| --- | --- |
| `Makefile` (`test`) | Runs pytest with `--cov=idempotent_consumer`, then checks a minimum for the domain and application. |
| `pyproject.toml` | `[tool.coverage.run]` and `[tool.coverage.report]` settings. |
| `.coverage` (ignored by git) | The data file coverage writes. |

## How it works underneath

While the tests run, coverage hooks into the Python interpreter and records every line executed in the files you named. With `branch = true` it also records, for each `if` or loop, which of its two outcomes happened. After the run, a report divides lines run by lines that exist. `skip_covered = false` keeps fully covered files in the table.

## Walkthrough of this repository's use

```make
test: $(STAMP) ## unit tests with coverage (no Kafka or PostgreSQL needed)
	$(PY) -m pytest tests/unit --cov=idempotent_consumer --cov-report=term
	$(PY) -m coverage report --include='src/idempotent_consumer/domain/*,src/idempotent_consumer/application/*' --fail-under=95
```

- `--cov=idempotent_consumer` measures the whole package; `--cov-report=term` prints the table in the terminal.
- The second command, `coverage report --include='src/idempotent_consumer/domain/*,src/idempotent_consumer/application/*' --fail-under=95`, reports only the domain and application folders and exits with an error if their total is below 95 percent. That is what makes `make test` fail when a rule in the core loses its tests.

```toml
[tool.coverage.run]
source = ["idempotent_consumer"]
branch = true

[tool.coverage.report]
show_missing = true
skip_covered = false
```

`source = ["idempotent_consumer"]` limits measuring to this package. `branch = true` turns on branch coverage. `show_missing = true` lists the line numbers that never ran.

The table columns are `Stmts` (statements), `Miss` (never run), `Branch`, `BrPart` (branches taken only one way) and `Cover`.

## Try it yourself

1. `make test` ends with a table; the `TOTAL` row for domain and application shows `100%`.
2. `.venv/bin/python -m coverage report --include='src/idempotent_consumer/infrastructure/*'` shows the adapters at lower numbers, because only integration tests use the real Kafka.
3. `.venv/bin/python -m coverage report -m --include='src/idempotent_consumer/interfaces/consume_cli.py'` lists the missing line numbers in `Missing`.

## Mistakes people make

- Treating 100 percent as proof of correct code. It only proves the lines ran.
- Chasing a number by testing trivial code. Here the minimum applies to the core logic, and adapters are covered by integration tests instead.
- Measuring the tests themselves. `source` names only the package.

## Check yourself

1. What does branch coverage add to line coverage? *It checks both outcomes of each `if` or loop.*
2. What does `--fail-under=95` do? *Makes the command fail when the total is below 95 percent.*
3. Why measure only the domain and application for the limit? *They hold the rules; adapters need a real broker.*
4. Which file stores the raw data? *`.coverage`.*
5. What does `Miss` count? *Statements that never ran.*

## Further reading

- coverage.py 7.17 documentation: https://coverage.readthedocs.io/en/7.17.0/
- pytest-cov 7.1 documentation: https://pytest-cov.readthedocs.io/en/v7.1.0/
