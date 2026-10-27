# import-linter

## What it is

import-linter is a command-line tool (`lint-imports`) that checks which Python modules import which other modules and fails if a rule you wrote is broken. A rule is called a **contract**. This repository pins import-linter 2.15; it builds the import graph with a helper library, `grimp`.

## Why this repository uses it

The code is arranged in four layers, `domain`, `application`, `infrastructure`, `interfaces`. The layers only help if the arrows between them point one way. A comment cannot enforce that; a failing check can.

## Where it appears

| Path | What it does |
| --- | --- |
| `pyproject.toml` | `[tool.importlinter]` and two `[[tool.importlinter.contracts]]`. |
| `Makefile` (`lint`) | Runs `lint-imports`. |

## How it works underneath

The tool reads the source files, records every `import` statement as an edge from one module to another, and then tests each contract against that graph. It does not run the code. A **layers** contract lists packages from highest to lowest: a higher layer may import a lower one, never the reverse. A **forbidden** contract says a set of modules must not import a set of other modules, even through intermediate modules.

## Walkthrough of this repository's use

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

- `root_package = "idempotent_consumer"`: the package to analyse.
- `include_external_packages = true`: also track imports of outside libraries such as `kafka` and `psycopg`; needed for the forbidden contract.
- Contract 1, `type = "layers"`, with the list `interfaces`, `infrastructure`, `application`, `domain` (top to bottom). So `domain` cannot import anything above it, and `infrastructure` cannot import `interfaces`.
- Contract 2, `type = "forbidden"`: `domain` and `application` may not import `kafka` or `psycopg`. They talk about Kafka and the database only through the ports (`tech/python.md`), and adapters in `infrastructure` do the real talking.

## Try it yourself

1. `.venv/bin/lint-imports` prints both contracts as `KEPT` and `Contracts: 2 kept, 0 broken.`.
2. Break the layer rule: add `from idempotent_consumer.infrastructure import settings` to the top of `src/idempotent_consumer/domain/balances.py` and run `.venv/bin/lint-imports`. The first contract is `BROKEN` and the output names the import chain. Undo with `git checkout src`.
3. Break the forbidden rule: add `import psycopg` to `src/idempotent_consumer/application/health.py`; the second contract breaks. Undo.

## Mistakes people make

- Writing the layer list in the wrong order. The first entry is the highest layer.
- Forgetting `include_external_packages` and wondering why `forbidden` ignores `kafka` and `psycopg`.
- Only checking directly imported modules; the forbidden contract also catches imports through other modules.

## Check yourself

1. What may a higher layer do? *Import lower layers.*
2. What may `domain` import? *Nothing from the other three layers (and, by the second contract, not `kafka` or `psycopg`).*
3. What is a contract? *A named rule the tool checks against the import graph.*
4. Does it run the code? *No.*
5. Why is `include_external_packages` set? *So rules can mention outside libraries such as `kafka` and `psycopg`.*

## Further reading

- import-linter 2.15 documentation: https://import-linter.readthedocs.io/en/v2.15/
- Contract types: https://import-linter.readthedocs.io/en/v2.15/contract_types/
