# Technology notes for `kafka-idempotent-consumer`

These notes teach every technology this repository uses, from zero, by walking through the repository's own files. You do not need to know any of them first. Each file has the same parts: what it is, why it is here, where it appears, how it works underneath, a walkthrough of this repository's use (which quotes every relevant line of every relevant file and explains each), things to try, common mistakes, check-yourself questions, and links to the official documentation. The five `python-*` files walk through the source and test files one by one.

## The files

| File | Its role here |
| --- | --- |
| [python.md](python.md) | The language all the code is written in (3.12 and newer). |
| [python-domain.md](python-domain.md) | Every file of `src/idempotent_consumer/domain/`, line by line. |
| [python-application.md](python-application.md) | Every file of `src/idempotent_consumer/application/`: the use cases and ports. |
| [python-infrastructure.md](python-infrastructure.md) | Every file of `src/idempotent_consumer/infrastructure/`: the adapters, settings, logging, retry and shutdown. |
| [python-interfaces.md](python-interfaces.md) | Every file of `src/idempotent_consumer/interfaces/`: the command-line programs. |
| [python-tests.md](python-tests.md) | Every test file and every test under `tests/`. |
| [apache-kafka.md](apache-kafka.md) | The message server: topics, consumer groups, offsets and commits, dead letters, evictions. |
| [kafka-python.md](kafka-python.md) | The library that lets the Python code talk to Kafka. |
| [postgresql.md](postgresql.md) | The database that holds the balances; transactions and primary keys. |
| [sql.md](sql.md) | The `sql/` scripts and the two statements the consumer runs. |
| [psycopg.md](psycopg.md) | The library that lets the Python code talk to PostgreSQL. |
| [json.md](json.md) | The text format of payments, dead letters and the expected-totals file. |
| [logfmt.md](logfmt.md) | The shape of every log line the programs write. |
| [dotenv.md](dotenv.md) | The `.env.example` file and how settings (and the password) reach the programs. |
| [bash.md](bash.md) | The shell logic inside the Makefile and the CI workflow. |
| [make.md](make.md) | The `Makefile`: the short commands (`make up`, `make run`, `make demo-crash`, ...). |
| [docker.md](docker.md) | Runs Kafka and PostgreSQL (and the packaged programs) in containers. |
| [dockerfile.md](dockerfile.md) | The recipe that packages the programs as an image. |
| [docker-compose.md](docker-compose.md) | The file that starts the Kafka and PostgreSQL containers. |
| [yaml.md](yaml.md) | The format of the compose file and the CI workflow. |
| [toml.md](toml.md) | The format of `pyproject.toml`, where the project and its tools are configured. |
| [github-actions.md](github-actions.md) | The automatic checks that run on every push. |
| [pytest.md](pytest.md) | Runs the unit, integration and end-to-end tests. |
| [pytest-cov.md](pytest-cov.md) | Measures how much code the tests ran. |
| [ruff.md](ruff.md) | The linter and formatter. |
| [mypy.md](mypy.md) | The type checker. |
| [import-linter.md](import-linter.md) | Enforces which code layer may import which. |
| [pip-venv-and-lock-files.md](pip-venv-and-lock-files.md) | How packages are installed and pinned (pip, venv, uv). |
| [pip-audit.md](pip-audit.md) | Checks the pinned packages for known vulnerabilities. |
| [editorconfig.md](editorconfig.md) | Shared editor settings. |
| [git.md](git.md) | Git, the `.gitignore` file and the commit message convention. |
| [markdown.md](markdown.md) | The README, ARCHITECTURE and decision-record files, and Mermaid diagrams. |
| [license-and-versioning.md](license-and-versioning.md) | The MIT license, the changelog and the version number. |

## Reading order

1. The language: [python.md](python.md), then the code itself, layer by layer: [python-domain.md](python-domain.md), [python-application.md](python-application.md), [python-infrastructure.md](python-infrastructure.md), [python-interfaces.md](python-interfaces.md), [python-tests.md](python-tests.md).
2. The two servers and their libraries: [apache-kafka.md](apache-kafka.md), [kafka-python.md](kafka-python.md), [postgresql.md](postgresql.md), [sql.md](sql.md), [psycopg.md](psycopg.md).
3. Data and settings formats: [json.md](json.md), [logfmt.md](logfmt.md), [dotenv.md](dotenv.md).
4. The build and run tools: [make.md](make.md), [bash.md](bash.md), [pip-venv-and-lock-files.md](pip-venv-and-lock-files.md), [toml.md](toml.md), [yaml.md](yaml.md).
5. The runtime pieces: [docker.md](docker.md), [docker-compose.md](docker-compose.md), [dockerfile.md](dockerfile.md).
6. The tests and checks: [pytest.md](pytest.md), [pytest-cov.md](pytest-cov.md), [ruff.md](ruff.md), [mypy.md](mypy.md), [import-linter.md](import-linter.md), [pip-audit.md](pip-audit.md), [github-actions.md](github-actions.md), [editorconfig.md](editorconfig.md), [git.md](git.md), [markdown.md](markdown.md), [license-and-versioning.md](license-and-versioning.md).

## If you have ten minutes

Read [apache-kafka.md](apache-kafka.md) (why the same payment can arrive twice), [sql.md](sql.md) (the two statements that make a second delivery harmless) and [postgresql.md](postgresql.md) (why they sit in one transaction).
