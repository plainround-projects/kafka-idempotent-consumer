# License, changelog and version numbers

## What it is

Three small files and one number say who may use the code and what changed:

- A **license** is a legal text that tells other people what they may do with your code. Without one, nobody else is allowed to copy or reuse it. The **MIT license** is a short, permissive one: anyone may use, copy, change and share the code, even commercially, as long as the license text stays with it, and the author gives no warranty.
- A **changelog** is a list of changes for each released version, written for people, not machines.
- A **version number** such as `0.1.0` labels a release. **Semantic Versioning** (SemVer) gives the three numbers meaning: `MAJOR.MINOR.PATCH`. Raise MAJOR for a change that breaks users, MINOR for a new feature that does not break them, PATCH for a fix. Versions below `1.0.0` mean "still settling".

## Why this repository uses it

The template requires a license and a changelog in every reference repository. A learner who copies code from here knows they may, and what the terms are; and anyone can see what each version added.

## Where it appears

| Path | What it holds |
| --- | --- |
| `LICENSE` | The MIT license text with the author and year. |
| `CHANGELOG.md` | Version `0.1.0` and what it added. |
| `pyproject.toml` (`version = "0.1.0"`) and `src/idempotent_consumer/__init__.py` (`__version__ = "0.1.0"`) | The same version number, in the two places Python tools look. |

## How it works underneath

Nothing in the program reads these files; they are for people and for tools that scan licenses. The one rule is consistency: the number in `pyproject.toml`, the number in `__version__` and the newest heading in `CHANGELOG.md` must agree.

## Walkthrough of this repository's use

### `LICENSE`, paragraph by paragraph

```text
MIT License

Copyright (c) 2026 Sumit Srivastava

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```

- Line 1 and 3: the license's name, and the copyright line: who holds the copyright and in which year.
- Line 5: the **grant**. Anyone who has a copy of the software may "deal in the Software without restriction": the listed rights (use, copy, modify, merge, publish, distribute, sublicense, sell) are named so nobody has to guess. "Sublicense" means pass those rights on to others.
- Line 7: the **condition**. The copyright notice and this permission text must come along with every copy or substantial part. This is the only duty the license places on users.
- Line 9: the **disclaimer**, in capitals by custom. The software is provided "as is": no promise that it works, is suitable for a purpose, or does not infringe someone else's rights, and the authors are not liable for damage from using it.

### `CHANGELOG.md`

```markdown
  # Changelog

  All notable changes to this project are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/).

  ## [0.1.0] - 2026-10-26

  ### Added

  - Kafka 4 (KRaft, one node) and PostgreSQL 18 in `docker-compose.yml`; the `payments` and `payments-dlq` topics.
  - SQL scripts: `balances` schema and seed, the `processed_events` migration, the lesson's queries and a reset script.
  - A producer of payments (and malformed events) with an independently computed `expected_totals.json`.
  - A balance updater with a naive mode (steps 1 and 2) and an idempotent mode (step 3): event id and balance in one transaction, offsets committed after the database commit, a kill switch, dead-lettering of poison records and eviction handling.
  - A balance checker, a 20-run crash harness and a health command.
  - Layered package (`domain`, `application`, `infrastructure`, `interfaces`) with the dependency rule enforced by import-linter, unit, integration and end-to-end tests, ruff, mypy, hash-pinned lock files and a CI workflow.
  - `docs/RUNBOOK.md`: the lesson's step-6 one-page entry.
```

(The block is shown indented by two spaces so that its heading lines are not mistaken for headings of this note.)

- Line 3 says the file follows the **Keep a Changelog** format (a convention for how to write one) and the project follows Semantic Versioning.
- Line 5: a heading per version, with the release date in year-month-day form. In brackets the version number.
- Line 7: a sub-heading names the kind of change. Keep a Changelog uses `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed` and `Security`. Version 0.1.0 is the first release, so everything is under `Added`.
- Lines 9 to 15: one bullet per notable addition, written as a sentence.

## Try it yourself

```bash
grep -n 'version' pyproject.toml | head -2
grep -n '__version__' src/idempotent_consumer/__init__.py
grep -n '^## \[' CHANGELOG.md
```

Expected: `0.1.0` in all three places.

## Mistakes people make

- Copying code with no license: you have no right to reuse it, however public the file is.
- Bumping the version in one place and not the others.
- A changelog that lists every commit. Its audience wants the things a user would notice.

## Check yourself

1. What is the one condition of the MIT license? (Keep the copyright and permission notice with copies.)
2. Which number do you raise when you add a feature without breaking anyone? (MINOR.)
3. Why does `0.1.0` not promise stability? (SemVer treats versions below 1.0.0 as still changing.)

## Further reading

- MIT license text and explanation: https://opensource.org/license/mit
- Keep a Changelog 1.1.0: https://keepachangelog.com/en/1.1.0/
- Semantic Versioning 2.0.0: https://semver.org/
