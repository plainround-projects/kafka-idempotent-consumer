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

The file has these groups (see `.env.example` in the repository root): the Kafka address and topic names, the PostgreSQL connection parts (`POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`), the log level and the Kafka timeouts. The password in the template is a throwaway for a database that lives only while the container runs.

```make
-include .env
```

```make
export
```

```make
env:
	@[ -f .env ] || { cp .env.example .env; echo "created .env from .env.example"; }
```

```python
def _text(environ: Mapping[str, str], name: str, default: str | None = None) -> str:
    value = environ.get(name, default if default is not None else "").strip()
    if not value:
        raise ConfigurationError(f"{name} must be set and not empty")
    return value
```

`_text` reads one variable, strips spaces and refuses an empty value. `Settings.from_env` applies defaults and checks the formats (a server list like `host:port`, a known log level). `POSTGRES_PASSWORD` is the only variable with **no default**: leave it out and the program stops with `POSTGRES_PASSWORD must be set and not empty`. The password field is excluded from the settings object's printed form, so it cannot appear in a log.

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
