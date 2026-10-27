# TOML (`pyproject.toml`)

## What it is

TOML is a plain-text format for settings made of `[section]` headers and `name = value` lines. Values are text in double quotes, numbers, `true`/`false`, lists in square brackets, or small tables in curly braces. `pyproject.toml` is the standard file where a Python project states what it is and how its tools are set up.

## Why this repository uses it

The template asks for one tool-configuration file. Here `pyproject.toml` describes the package and configures ruff, mypy, pytest, coverage and import-linter, so there is no scatter of config files.

## Where it appears

| Path | What it holds |
| --- | --- |
| `pyproject.toml` | Everything below. |

## How it works underneath

Tools find `pyproject.toml` by walking up from the current folder. Each tool reads only its own section: `[tool.ruff]` for ruff, `[tool.mypy]` for mypy, and so on. pip and `setuptools` read `[build-system]` and `[project]` to know how to build and install the package.

## Walkthrough of this repository's use

```toml
[build-system]
requires = ["setuptools>=75"]
build-backend = "setuptools.build_meta"

[project]
name = "kafka-idempotent-consumer"
version = "0.1.0"
description = "A Kafka consumer that keeps PostgreSQL balances correct under at-least-once delivery: idempotence, dead letters, evictions."
readme = "README.md"
requires-python = ">=3.12"
license = { text = "MIT" }
authors = [{ name = "Sumit Srivastava" }]
```

- `[build-system]`: which program builds the package: `setuptools`, version 75 or newer.
- `[project]`: the package's name, version, text description, `readme` file, the required Python (`>=3.12`), license, authors.

```toml
dependencies = ["kafka-python==3.0.12", "psycopg[binary]>=3.2,<4"]

[project.optional-dependencies]
dev = [
    "pytest>=8,<10",
    "pytest-cov>=5",
    "ruff>=0.6",
    "mypy>=1.11",
    "import-linter>=2.0",
    "pip-audit>=2.7",
]

[project.scripts]
idempotent-consumer-produce = "idempotent_consumer.interfaces.produce_cli:main"
idempotent-consumer-consume = "idempotent_consumer.interfaces.consume_cli:main"
idempotent-consumer-check = "idempotent_consumer.interfaces.check_cli:main"
idempotent-consumer-crash-test = "idempotent_consumer.interfaces.crash_test_cli:main"
idempotent-consumer-health = "idempotent_consumer.interfaces.health_cli:main"
```

- `dependencies = ["kafka-python==3.0.12", "psycopg[binary]>=3.2,<4"]`: what the program needs to run. `==` pins one exact version; `>=3.2,<4` accepts any 3.x from 3.2 (the lock file then fixes the exact one, 3.3.6); `[binary]` is an "extra" that also installs a prebuilt compiled part.
- `[project.optional-dependencies]` / `dev = [...]`: extras installed only for development: pytest, pytest-cov, ruff, mypy, import-linter, pip-audit. The `>=8,<10` style is a range.
- `[project.scripts]`: each line makes a command (`idempotent-consumer-produce`) that runs the function `main` in the named module.

```toml
[tool.setuptools.packages.find]
where = ["src"]
```

Look for packages under `src/`.

```toml
[tool.ruff]
line-length = 120
target-version = "py312"
src = ["src", "tests"]
# tech/ quotes parts of files exactly as they are, so its code blocks are not reformatted.
extend-exclude = ["tech"]

[tool.ruff.lint]
select = ["E", "F", "I", "B", "UP", "SIM", "N", "C4", "RUF", "PT", "D100", "D101", "D103", "D104"]

[tool.ruff.lint.per-file-ignores]
"tests/**" = ["D"]
```

Ruff's settings: line length 120 characters, target Python 3.12, the lint rule families to turn on (`select`), and a per-file exception (`tests/**` does not need docstrings). Details in `tech/ruff.md`.

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

mypy's settings. `[[tool.mypy.overrides]]` (double brackets) is an element of a *list* of tables, so there can be several. See `tech/mypy.md`.

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-ra --strict-markers"
markers = [
    "integration: needs Kafka and PostgreSQL from docker-compose.yml (make up)",
    "e2e: runs the lesson's scenarios through the command-line programs against Kafka and PostgreSQL",
]
```

pytest's settings: where tests live, extra options for every run (`addopts`) and the registered marker names.

```toml
[tool.coverage.run]
source = ["idempotent_consumer"]
branch = true

[tool.coverage.report]
show_missing = true
skip_covered = false
```

```toml
[tool.importlinter]
root_package = "idempotent_consumer"
include_external_packages = true

[[tool.importlinter.contracts]]
name = "Layers: interfaces -> infrastructure -> application -> domain, never upward"
type = "layers"
layers = [
    "idempotent_consumer.interfaces",
    "idempotent_consumer.infrastructure",
    "idempotent_consumer.application",
    "idempotent_consumer.domain",
]

[[tool.importlinter.contracts]]
name = "Domain and application know nothing about Kafka or PostgreSQL"
type = "forbidden"
source_modules = ["idempotent_consumer.domain", "idempotent_consumer.application"]
forbidden_modules = ["kafka", "psycopg"]
```

Import-linter's two contracts (see `tech/import-linter.md`).

## Try it yourself

1. Parse the file with Python's built-in reader: `.venv/bin/python -c "import tomllib; d = tomllib.load(open('pyproject.toml','rb')); print(d['project']['name'], d['project']['dependencies'])"` prints `kafka-idempotent-consumer ['kafka-python==3.0.12', 'psycopg[binary]>=3.2,<4']`.
2. List the commands the project installs: `.venv/bin/python -c "import tomllib; print(list(tomllib.load(open('pyproject.toml','rb'))['project']['scripts']))"`.
3. Run one of them: `.venv/bin/idempotent-consumer-produce --help | head -2`.

## Mistakes people make

- Putting a string without quotes. TOML text needs double quotes.
- Confusing `[table]` with `[[array of tables]]`.
- Repeating the same section twice, which is an error.
- Pinning with `>=` and expecting repeatable installs. That is why this repository adds lock files (`tech/pip-venv-and-lock-files.md`).

## Check yourself

1. Which section lists what the program needs at run time? *`[project]` `dependencies`.*
2. What does `[[x]]` mean? *One entry of a list of tables called `x`.*
3. What does `==3.0.12` mean? *Exactly that version.*
4. Where does ruff read its settings? *`[tool.ruff]` and its sub-sections.*
5. What does `[project.scripts]` create? *Command-line commands that call a function.*

## Further reading

- TOML 1.0.0: https://toml.io/en/v1.0.0
- `pyproject.toml` specification: https://packaging.python.org/en/latest/specifications/pyproject-toml/
- Python 3.12 `tomllib`: https://docs.python.org/3.12/library/tomllib.html
