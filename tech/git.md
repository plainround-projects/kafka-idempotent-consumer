# Git and `.gitignore`

## What it is

**Git** is a program that records the history of a folder of files. You tell it when to take a snapshot, called a **commit**, and attach a message saying what changed and why. Every commit remembers the files as they were, who made the commit, when, and which commit came before. The whole history lives in a hidden folder named `.git` inside the project. Because commits are stored with a fingerprint (a hash such as `8afb9c0`), nobody can change the past without everybody noticing.

Words you will meet:

- A **repository** (repo) is a folder that Git is watching, together with its `.git` history folder.
- The **working tree** is the files as they are on disk right now. A **change** is a difference between the working tree and the last commit.
- The **staging area** is a waiting room: `git add FILE` puts a file's current state there, and `git commit` turns everything in the waiting room into one commit.
- A **branch** is a movable name for a line of commits. The commits of this project are on one branch.
- A **remote** is a copy of the repository on another machine (such as GitHub). Nothing in this project has been pushed anywhere; `git push` is what would send commits to a remote.

## Why this repository uses it

The template asks that each reference project be its own repository with a clean history, so a learner can read the project in the order it was built: one commit adds the domain, the next the application layer, then the infrastructure layer, then the interfaces, then the tests, the CI workflow and the documents. The commit messages follow a convention (below) so each one says what kind of change it is.

## Where it appears

| Path | What it does |
| --- | --- |
| `.git/` | The history. Not part of the code; never edit by hand. |
| `.gitignore` | Names the files Git must not track. |
| `.github/workflows/ci.yml` | GitHub reads it when commits are pushed (see `tech/github-actions.md`). |
| `.dockerignore` | Looks like `.gitignore` but is read by Docker (see `tech/dockerfile.md`). |

## How it works underneath

Git stores each version of each file as an object named by the hash of its content, groups them into a tree for each commit, and links every commit to its parent. A branch is a small file holding the hash of its latest commit. `git log` walks the parent links. Adding a file to `.gitignore` does not delete it; it only tells Git not to offer it for tracking.

## Walkthrough of this repository's use

### `.gitignore`: every line

A `.gitignore` file lists patterns. Git does not track, and `git status` does not show, files that match. A line ending in `/` matches only folders. `*` matches any characters within one name. A `#` starts a comment.

```text
.venv/
__pycache__/
*.pyc
*.egg-info/
build/
dist/
.pytest_cache/
.mypy_cache/
.ruff_cache/
.import_linter_cache/
.coverage
.env
.DS_Store
logs/
expected_totals.json
.omc/
```

- `.venv/`: the virtual environment (the folder of installed packages created by `make install`). It can be rebuilt from `requirements-dev.lock` and is large and specific to one machine.
- `__pycache__/` and `*.pyc`: Python's compiled copies of modules, created automatically.
- `*.egg-info/`: metadata folders that `pip install -e .` creates in `src/`.
- `build/` and `dist/`: folders that packaging tools create.
- `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`, `.import_linter_cache/`: the caches that pytest, mypy, ruff and import-linter keep to run faster next time.
- `.coverage`: the file coverage writes with its measurements.
- `.env`: a personal file of environment variables. It may hold values that must never be shared, so it is ignored by default. The tracked copy-me template is `.env.example`.
- `.DS_Store`: a file macOS creates in folders to remember how Finder shows them.
- `logs/`: the log files written by the demo scripts and the crash test.
- `expected_totals.json`: the answer file the producer writes (`make data`). It is regenerated on every run, so it is not tracked.
- `.omc/`: a folder that some coding tools create for their own notes.

### Commit messages: Conventional Commits

Each commit message in this repository starts with a **type** and an optional **scope** in brackets, then a colon and a short summary in the imperative mood. This convention is called **Conventional Commits**. Reading the first word tells you what kind of change it is:

| Type | Meaning | Example from this repository's history |
| --- | --- | --- |
| `feat` | A new capability. | `feat(domain): add payments, validation, dead letters, balance comparison and kill points` |
| `fix` | A bug fix. | (no commit of this type exists in this repository's history; a bug fix would be written `fix: ...`) |
| `docs` | Only documentation changed. | `docs: add README, ARCHITECTURE, RUNBOOK, ADRs and CHANGELOG` |
| `test` | Tests added or changed. | `test: add unit, integration and end-to-end tests` |
| `build` | How the project is built or packaged. | `build: add compose file, SQL scripts, project metadata, lock files and Makefile` |
| `ci` | The continuous-integration workflow. | `ci: run lint, types, import rules, coverage, audit and the service-container tests` |

The scope in brackets (`domain`, `application`, `infrastructure`, `interfaces`, `tech`) says which part. The history was written layer by layer on purpose: `git log --oneline` reads as the order in which the project was built.

### Commands you can use to read the history

- `git log --oneline` lists every commit, newest first, one line each.
- `git show 8afb9c0` (use a hash from the list) shows one commit's message and changes.
- `git status` lists changed and untracked files; in a finished repository it says `nothing to commit, working tree clean`.
- `git diff` shows changes not yet staged.

## Try it yourself

```bash
git log --oneline | head -5
git status
git check-ignore -v .venv .env .coverage
```

Expected: the five newest commit lines; a clean status; and one line per ignored path showing which `.gitignore` line caused it (for example `.gitignore:1:.venv/	.venv`). `git check-ignore` answers by pattern, so it lists `.env` even if you never created that file. A path that is not ignored prints nothing.

## Mistakes people make

- Committing a file with a secret, then deleting it in the next commit. The secret stays in the history. Put such files in `.gitignore` before the first commit.
- Believing `.gitignore` removes a file that is already tracked. It does not; use `git rm --cached FILE` for that.
- Writing the message "fix stuff". The convention exists so that the type and scope say what kind of change it is and where.

## Check yourself

1. What does `git commit` turn into a commit? (Whatever is in the staging area.)
2. Why is `.venv/` ignored? (It is large, machine-specific and can be rebuilt from the lock file.)
3. What does the first word of `feat(domain): ...` tell you? (That the commit adds a capability, in the domain part.)

## Further reading

- Pro Git, the free book: https://git-scm.com/book
- Conventional Commits 1.0.0: https://www.conventionalcommits.org/en/v1.0.0/
- Git documentation of `.gitignore` patterns: https://git-scm.com/docs/gitignore
