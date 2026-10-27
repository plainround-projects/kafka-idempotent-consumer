# `.env` files (environment variables)

## What it is

An **environment variable** is a named setting that every program started by a shell can read, such as `POSTGRES_PORT=5433`. A **`.env` file** is a plain text file of `NAME=value` lines that tools load into the environment so you do not have to type them each time. This repository ships `.env.example` (the template, safe to commit) and creates `.env` (your copy, ignored by git) from it.

## Why this repository uses it

The template requires configuration to come from the environment, and secrets only from there. Listing every variable with its default in one file shows a reader all the knobs.

## Where it appears

| Path | What it does |
| --- | --- |
| `.env.example` | Every variable the project reads, with its default. Committed. |
| `.env` | Your local copy. Not committed (`.gitignore`). |
| `Makefile` | `-include .env` and `export` load it into every command Make runs; `make up` copies the template if `.env` is missing. |
| `docker-compose.yml` | Compose reads `.env` to fill `${...}` placeholders. |
| `src/idempotent_consumer/infrastructure/settings.py` | Reads `os.environ`, checks each value, and refuses wrong ones. |

## How it works underneath

`.env` is just text. Make includes it as if its lines were Make assignments, and `export` passes them on to every program it starts. Docker Compose has its own reader. Python reads the final result through `os.environ`. A value already set in your shell wins over the file in Compose. Lines starting with `#` are comments.

## Walkthrough of this repository's use

First, what an **environment variable** is: a named piece of text that a program receives from the program that started it. Every process has its own set. A shell sets one for a single command like this: `TOPIC=other .venv/bin/python -m idempotent_consumer.interfaces.check_cli`.

A `.env` file is a plain text file of `NAME=value` lines, one per line; a `#` starts a comment. It is a convenient place for the values for one machine. This repository ships `.env.example`, a copy-me template for the variables the programs read (the docstring of `Settings` points to it). **A secret never belongs in a file that is committed.** Here the one secret is the database password, and the rules are:

- `.env` (the real file) is listed in `.gitignore` and `.dockerignore`, so it is never committed and never copied into an image.
- The code has **no default password**, anywhere. `POSTGRES_PASSWORD` must be provided by the environment, or the programs stop with a clear error.
- The compose file reads the password from the same variable, and refuses to start without it.

### How `.env` gets created

```make
env:
	@[ -f .env ] || { cp .env.example .env; echo "created .env from .env.example"; }
```

`make up` runs the `env` target first. The shell test `[ -f .env ]` is true if `.env` exists; if it does not, `cp .env.example .env` copies the template and a message is printed. After that you may edit `.env`.

### How the values reach the programs

```make
-include .env
export
```

- `-include .env` makes Make read `.env` if the file exists. Make understands `NAME=value` lines as variable assignments. The `-` means "no error if the file is missing".
- `export` (alone on a line) passes every Make variable to the commands Make starts. So the password and the other values reach `python -m idempotent_consumer...`, which reads its environment.

Docker Compose also reads `.env` by itself (it looks for a file of that name next to `docker-compose.yml`) to fill in the `${...}` placeholders in the compose file. So one file configures both the containers and the programs.

### What reads them

```python
def _text(environ: Mapping[str, str], name: str, default: str | None = None) -> str:
    value = environ.get(name, default if default is not None else "").strip()
    if not value:
        raise ConfigurationError(f"{name} must be set and not empty")
    return value


def _positive_int(environ: Mapping[str, str], name: str, default: int) -> int:
    raw = environ.get(name, str(default)).strip()
    if not raw.isdigit() or int(raw) < 1:
        raise ConfigurationError(f"{name} must be a positive integer, got {raw!r}")
    return int(raw)
```

`_text` reads one variable, strips spaces and refuses an empty value; without a default (the password) the variable must be present. `_positive_int` requires a whole number of at least 1. Their messages begin with the variable's name.

```python
    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> Settings:
        env = os.environ if environ is None else environ
        servers = _text(env, "BOOTSTRAP_SERVERS", "localhost:9092")
        if not _SERVERS.match(servers):
            raise ConfigurationError(f"BOOTSTRAP_SERVERS must be host:port[,host:port], got {servers!r}")
        level = _text(env, "LOG_LEVEL", "INFO").upper()
        if level not in _LEVELS:
            raise ConfigurationError(f"LOG_LEVEL must be one of {sorted(_LEVELS)}, got {level!r}")
        return cls(
            kafka=KafkaSettings(
                bootstrap_servers=servers,
                topic=_text(env, "TOPIC", "payments"),
                dlq_topic=_text(env, "DLQ_TOPIC", "payments-dlq"),
                group_id=_text(env, "GROUP_ID", "balance-updater"),
                request_timeout_ms=_positive_int(env, "KAFKA_REQUEST_TIMEOUT_MS", 30_000),
                retry_backoff_ms=_positive_int(env, "KAFKA_RETRY_BACKOFF_MS", 100),
            ),
            postgres=PostgresSettings(
                host=_text(env, "POSTGRES_HOST", "localhost"),
                port=_positive_int(env, "POSTGRES_PORT", 5433),
                database=_text(env, "POSTGRES_DB", "bank"),
                user=_text(env, "POSTGRES_USER", "bank"),
                password=_text(env, "POSTGRES_PASSWORD"),
                connect_timeout_seconds=_positive_int(env, "POSTGRES_CONNECT_TIMEOUT_SECONDS", 5),
            ),
            log_level=level,
        )
```

`Settings.from_env` is the single place that knows every variable name. The thirteen variables, their defaults and meanings are in the table in `python-infrastructure.md` (section `settings.py`); in short: `BOOTSTRAP_SERVERS`, `TOPIC`, `DLQ_TOPIC`, `GROUP_ID`, `KAFKA_REQUEST_TIMEOUT_MS` and `KAFKA_RETRY_BACKOFF_MS` for Kafka; `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` and `POSTGRES_CONNECT_TIMEOUT_SECONDS` for PostgreSQL; and `LOG_LEVEL`. Everything except `POSTGRES_PASSWORD` has a default that suits the compose setup.

The password is held in `PostgresSettings.password`, a field marked `field(repr=False)`, so printing the settings object, logging it, or seeing it in an error message never shows it. A unit test checks that.

### Where the password is also used

```yaml
      # Credentials come from .env (make up copies .env.example the first time); none is written here.
      POSTGRES_USER: ${POSTGRES_USER:-bank}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:?copy .env.example to .env first}
      POSTGRES_DB: ${POSTGRES_DB:-bank}
```

The compose file takes the same three database variables. `${POSTGRES_PASSWORD:?copy .env.example to .env first}` stops Compose with that message if the variable is unset or empty. The GitHub workflow supplies a throwaway value for its temporary database (`tech/github-actions.md`).

## Try it yourself

1. `ls -a | grep env` lists `.env.example` (and `.env` after `make up` or `make env`).
2. Missing password: `env -i PATH="$PATH" .venv/bin/python -m idempotent_consumer.interfaces.health_cli` prints `configuration error: POSTGRES_PASSWORD must be set and not empty` and exits with code 2.
3. A wrong value: `BOOTSTRAP_SERVERS=oops .venv/bin/python -m idempotent_consumer.interfaces.health_cli` prints `configuration error: BOOTSTRAP_SERVERS must be host:port[,host:port], got 'oops'`.
4. `git check-ignore -v .env` shows the `.gitignore` rule that keeps it out of git.

## Mistakes people make

- Committing `.env` with real secrets. Only `.env.example` is committed.
- Quoting values inconsistently: Make, Compose and shells treat quotes differently. This file uses bare values.
- Forgetting that a variable already set in the shell can override the file.
- Letting the program invent defaults for secrets. Here the password has none.

## Check yourself

1. Which file is committed, `.env` or `.env.example`? *`.env.example`.*
2. What does `export` in the Makefile do? *Passes every Make variable to the commands it runs.*
3. Why does the password have no default? *A built-in default would be a secret in the code.*
4. What happens if `POSTGRES_PASSWORD` is missing? *The program prints a configuration error and exits with code 2.*
5. Who reads `.env`, besides Make? *Docker Compose (for `${...}` placeholders).*

## Further reading

- Compose environment files: https://docs.docker.com/compose/how-tos/environment-variables/set-environment-variables/
- Twelve-factor app, config: https://12factor.net/config
- GNU Make `include` and `export`: https://www.gnu.org/software/make/manual/make.html#Include
