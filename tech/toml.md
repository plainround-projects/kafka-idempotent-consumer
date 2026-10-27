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

Tools find `pyproject.toml` by walking up from the current folder. Each tool reads only its own section: `[tool.ruff]` for ruff, `[tool.mypy]` for mypy, `[tool.pytest.ini_options]` for pytest. pip and `setuptools` read `[build-system]` and `[project]` to know how to build and install the package.

## Walkthrough of this repository's use

The whole file is read below, in order, every line. Four ideas about the TOML format first:

- A line in square brackets, such as `[project]`, starts a **table**: a named group of settings. Everything under it, until the next header, belongs to it.
- A header with dots, such as `[tool.ruff.lint]`, is a table named `lint` inside `ruff` inside `tool`. The dots show nesting.
- A header with **double** brackets, such as `[[tool.mypy.overrides]]`, adds one more item to a **list of tables**. Writing it twice makes a list of two tables. That is how a tool is given several separate rule groups.
- A value is text in double quotes (`"MIT"`), a number (`120`), `true` or `false`, a **list** in square brackets (`["a", "b"]`), or an **inline table** in curly braces (`{ text = "MIT" }`). A `#` starts a comment.

### How the package is built: `[build-system]`

```toml
[build-system]
requires = ["setuptools>=75"]
build-backend = "setuptools.build_meta"
```

Installing a Python project means turning its folder into something pip can install. That work is done by a **build backend**. Line 2 says "to build this package you first need `setuptools` at version 75 or newer", and line 3 names the part of `setuptools` that does the build. `pip install .` reads these two lines, downloads that build tool, and uses it. (The Docker build and `make install` both run `pip install` on this project.)

### What the package is: `[project]`

```toml
[project]
name = "kafka-idempotent-consumer"
version = "0.1.0"
description = "A Kafka consumer that keeps PostgreSQL balances correct under at-least-once delivery: idempotence, dead letters, evictions."
readme = "README.md"
requires-python = ">=3.12"
license = { text = "MIT" }
authors = [{ name = "Sumit Srivastava" }]
dependencies = ["kafka-python==3.0.12", "psycopg[binary]>=3.2,<4"]
```

- Line 6: `name`, the package's name on a package index. Line 7: `version`, the same `0.1.0` as `__version__` in `src/idempotent_consumer/__init__.py`.
- Line 8: `description`, a one-sentence summary. Line 9: `readme` names the file that is the long description.
- Line 10: `requires-python = ">=3.12"`: pip refuses to install on an older Python. The code uses syntax introduced in 3.12 (`def retry[T](...)`), so this is a real requirement.
- Line 11: `license = { text = "MIT" }`: an inline table naming the license. Line 12: the authors, a list of one inline table.
- Line 13: `dependencies = ["kafka-python==3.0.12", "psycopg[binary]>=3.2,<4"]`: the packages this project needs to *run*. `==` pins an exact version. `psycopg[binary]>=3.2,<4` is a range (3.2 or newer, older than 4) for the PostgreSQL client library; the part in square brackets, `[binary]`, is an **extra** that also installs a precompiled speed-up so no C compiler is needed on your machine.

### Packages for development: `[project.optional-dependencies]`

```toml
[project.optional-dependencies]
dev = [
    "pytest>=8,<10",
    "pytest-cov>=5",
    "ruff>=0.6",
    "mypy>=1.11",
    "import-linter>=2.0",
    "pip-audit>=2.7",
]
```

An **optional dependency group** (an "extra") is a named list installed only when asked for: `pip install ".[dev]"`. This one is called `dev`. The entries use **version ranges**: `pytest>=8,<10` means "8 or newer but older than 10"; `ruff>=0.6` means "0.6 or newer". Each tool:

- `pytest` runs the tests. `pytest-cov` adds coverage measurement to pytest.
- `ruff` checks style and common mistakes and formats code.
- `mypy` checks the type hints.
- `import-linter` checks the layer rules.
- `pip-audit` checks installed packages against a list of known security problems.

(The ranges are wide so the project does not break when a tool ships a new version; the exact versions actually used are fixed, with hashes, in `requirements-dev.lock`. See `tech/pip-venv-and-lock-files.md`.)

### Commands that the install creates: `[project.scripts]`

```toml
[project.scripts]
idempotent-consumer-produce = "idempotent_consumer.interfaces.produce_cli:main"
idempotent-consumer-consume = "idempotent_consumer.interfaces.consume_cli:main"
idempotent-consumer-check = "idempotent_consumer.interfaces.check_cli:main"
idempotent-consumer-crash-test = "idempotent_consumer.interfaces.crash_test_cli:main"
idempotent-consumer-health = "idempotent_consumer.interfaces.health_cli:main"
```

Each line has the form `command-name = "package.module:function"`. When the project is installed, pip writes a small program into the environment's `bin` folder with that name, which imports the module and calls the function. There are five: `idempotent-consumer-produce`, `-consume`, `-check`, `-crash-test` and `-health`. So `.venv/bin/idempotent-consumer-produce --payments 10` does the same as `python -m idempotent_consumer.interfaces.produce_cli --payments 10`. The Docker image uses `idempotent-consumer-health` as its health command. The function is `main`, which returns the exit code; the wrapper passes that to the operating system.

### Where the code lives: `[tool.setuptools.packages.find]`

```toml
[tool.setuptools.packages.find]
where = ["src"]
```

`setuptools` must know which folders hold Python packages. `where = ["src"]` tells it to look inside `src/` and find `idempotent_consumer` there. Keeping code under `src/` (the "src layout") stops tests from accidentally importing the folder next to them instead of the installed package.

### Ruff: `[tool.ruff]`, `[tool.ruff.lint]`

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

- Line 36: `line-length = 120` is the longest line ruff's formatter produces and its length rule allows.
- Line 37: `target-version = "py312"`: ruff may use Python 3.12 rules and syntax.
- Line 38: `src = ["src", "tests"]`: where first-party code lives, so imports are sorted into "mine" and "other people's" groups correctly.
- Lines 39 and 40: a comment, then `extend-exclude = ["tech"]`: ruff skips the `tech/` folder, because the notes there quote code exactly as it appears in the repository and a formatter must not rewrite them.
- Line 43: `select` lists the **rule families** to switch on. Each is a code made of letters:
  - `E`: style errors from pycodestyle (line too long, bad spacing).
  - `F`: Pyflakes: unused imports, unused variables, undefined names.
  - `I`: isort: imports must be sorted and grouped.
  - `B`: flake8-bugbear: patterns that are probably bugs (a changeable default argument, a bare `except`).
  - `UP`: pyupgrade: old syntax that has a newer form (for example `typing.List` instead of `list`).
  - `SIM`: simplify: code that can be shorter (`if x: return True else: return False`).
  - `N`: pep8-naming: classes in `CapWords`, functions in `snake_case`.
  - `C4`: comprehension style (unnecessary `list(...)` around a comprehension).
  - `RUF`: Ruff's own rules (for example, unused `# noqa` comments).
  - `PT`: pytest style (for example, how `parametrize` is written).
  - `D100`, `D101`, `D103`, `D104`: docstring rules: a public module, class, function or package must have a docstring. Only these four are chosen; the others in the `D` family (formatting of docstrings) are left off.
- Lines 45 and 46: `per-file-ignores` with `"tests/**" = ["D"]` switches the whole `D` family off for test files. Test functions are named for the behavior they check, and a docstring on each adds nothing.

### mypy: `[tool.mypy]`

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

- Line 49: `python_version = "3.12"`: check as if running on 3.12.
- Line 50: `strict = true`: switch on the whole bundle of strict checks: every function needs type hints, no implicit `Any`, no returning `Any` from a typed function, warnings for unneeded ignore comments, and more.
- Line 51: `files = ["src", "tests"]`: what to check when run with no arguments (`make lint` runs plain `mypy`).
- Lines 53 to 55: the first `[[tool.mypy.overrides]]` is a rule group for the modules named in `module`: `kafka` and anything below it (`kafka.*`). `ignore_missing_imports = true` says "this library ships no type information; do not report that as an error". Without it, strict mode would fail on `import kafka`.
- Lines 57 to 62: the second group, for `tests.*`, relaxes three strict checks (`disallow_untyped_defs`, `disallow_incomplete_defs`, `disallow_untyped_calls`) so test functions need no annotations. The comment above (line 56) gives the reason.

### pytest: `[tool.pytest.ini_options]`

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-ra --strict-markers"
markers = [
    "integration: needs Kafka and PostgreSQL from docker-compose.yml (make up)",
    "e2e: runs the lesson's scenarios through the command-line programs against Kafka and PostgreSQL",
]
```

- Line 65: `testpaths = ["tests"]` is where pytest looks when run with no path.
- Line 66: `addopts` are options added to every run. `-ra` prints a short summary of every outcome except passes (skipped, failed, errors). `--strict-markers` makes a mark that is not registered an error, which catches a typo such as `@pytest.mark.integraton`.
- Lines 67 to 70: `markers` registers the two marks used, each with the text shown by `pytest --markers`: `integration` and `e2e`.

### Coverage: `[tool.coverage.run]`, `[tool.coverage.report]`

```toml
[tool.coverage.run]
source = ["idempotent_consumer"]
branch = true

[tool.coverage.report]
show_missing = true
skip_covered = false
```

- Line 73: `source = ["idempotent_consumer"]`: measure this package only (not the tests, not libraries).
- Line 74: `branch = true`: also check that each `if` was taken both ways, not just that its lines ran.
- Line 77: `show_missing = true`: the report lists the line numbers that never ran. Line 78: `skip_covered = false`: files with 100% are still listed.

### Import rules: `[tool.importlinter]`

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

- Line 81: `root_package`: the package whose imports are analyzed. Line 82: `include_external_packages = true` lets a contract mention packages outside the project (here `kafka` and `psycopg`); without it the second contract could not be written.
- Lines 84 to 92: the first contract, `type = "layers"`. The list names layers from the **top** to the **bottom**. A module may import from layers below it but never from above. So `interfaces` can import all the others, and `domain` can import none of them.
- Lines 94 to 98: the second contract, `type = "forbidden"`: nothing in `source_modules` (`domain` and `application`, including everything below them) may import anything in `forbidden_modules` (`kafka` and `psycopg`). That is the rule behind the sentence "the use cases know nothing about Kafka or PostgreSQL".
- `make lint` runs `lint-imports`, which reads these tables, builds the graph of who imports whom, and fails on a violation. See `tech/import-linter.md`.

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
