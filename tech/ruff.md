# ruff

## What it is

ruff is a command-line tool for Python that does two jobs fast. As a **linter** it reads your code without running it and reports likely mistakes and style problems. As a **formatter** it rewrites the layout (spaces, line breaks, quote style) into one consistent style. It is written in Rust, which is why it runs in a fraction of a second. This repository pins ruff 0.17.2.

## Why this repository uses it

The template asks for a formatter and a linter configured in the repository and run in CI. One tool does both, and its settings sit in `pyproject.toml`.

## Where it appears

| Path | What it does |
| --- | --- |
| `pyproject.toml` | `[tool.ruff]`, `[tool.ruff.lint]`: line length, target version, the rule families, exceptions. |
| `Makefile` (`lint`, `format`) | `ruff check .`, `ruff format --check .`; `ruff check --fix .`, `ruff format .`. |
| `.github/workflows/ci.yml` | Runs `make check`, which includes `lint`. |

## How it works underneath

`ruff check` parses each Python file into a tree and tests it against rules. Each rule has a code: a letter family and a number (`E501` is "line too long", `F401` is "imported but unused"). `select` chooses which families to apply. `ruff format` parses the file and writes it back in one canonical layout; `--check` only reports whether it would change anything. Neither command runs your code.

## Walkthrough of this repository's use

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

- `line-length = 120`: lines longer than 120 characters are reported (`E501`) and the formatter wraps at 120.
- `target-version = "py312"`: assume Python 3.12 syntax, so `def retry[T](...)` is accepted and suggestions fit 3.12.
- `src = ["src", "tests"]`: where first-party imports live; the import sorter groups them correctly.
- `extend-exclude = ["tech"]`: skip the `tech/` folder. Newer ruff also formats Python code blocks inside Markdown files, and these notes quote the repository's code exactly, so they are left alone.
- `select = [...]` turns on these families: `E` (pycodestyle errors), `F` (pyflakes: unused imports and names), `I` (import order), `B` (likely bugs), `UP` (upgrade to newer syntax), `SIM` (simplifications), `N` (naming), `C4` (comprehensions), `RUF` (ruff's own), `PT` (pytest style), and `D100`, `D101`, `D103`, `D104` (docstrings on modules, classes, functions, packages).
- `[tool.ruff.lint.per-file-ignores]` `"tests/**" = ["D"]`: tests do not need docstrings.

The `make` targets:

```make
lint: $(STAMP) ## ruff check, ruff format check, mypy, import-linter
	$(VENV)/bin/ruff check .
	$(VENV)/bin/ruff format --check .
	$(VENV)/bin/mypy
	$(VENV)/bin/lint-imports

format: $(STAMP) ## apply ruff's formatting and safe fixes
	$(VENV)/bin/ruff check --fix .
	$(VENV)/bin/ruff format .
```

`lint` checks without changing files (this is what CI runs). `format` fixes what can be fixed safely and rewrites the layout.

## Try it yourself

1. `.venv/bin/ruff --version` prints `ruff 0.17.2`.
2. `.venv/bin/ruff check .` prints `All checks passed!`.
3. `.venv/bin/ruff format --check .` prints `... files already formatted`.
4. Add `import os` at the top of `src/idempotent_consumer/domain/balances.py` and run `.venv/bin/ruff check src`; it reports `F401 ... imported but unused`. Undo with `git checkout src`.
5. `.venv/bin/ruff rule E501` prints the rule's explanation.

## Mistakes people make

- Arguing about style by hand. The formatter decides.
- Silencing a rule with a comment instead of fixing the cause. This repository has no `noqa` comments.
- Running `ruff format` on Markdown that quotes code exactly. Excluding `tech/` avoids that.

## Check yourself

1. What is the difference between a linter and a formatter? *A linter reports problems; a formatter rewrites layout.*
2. What does `E501` mean? *A line is longer than the configured length.*
3. What does `--check` do? *Reports without changing files.*
4. Why set `target-version`? *So ruff knows which syntax and upgrade suggestions apply.*
5. Where are ruff's settings? *In `pyproject.toml`.*

## Further reading

- ruff documentation: https://docs.astral.sh/ruff/ (this repository uses 0.17.2)
- Rule list: https://docs.astral.sh/ruff/rules/
- Formatter: https://docs.astral.sh/ruff/formatter/
