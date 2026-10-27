# pip-audit

## What it is

pip-audit is a command-line tool that checks the Python packages you depend on against a public list of known security problems (**vulnerabilities**). For each package and version it asks the Python Packaging Advisory Database, through the service at `pypi.org`, whether a problem has been published. This repository pins pip-audit 2.10.1.

## Why this repository uses it

The template says dependencies must be checked by an audit step in CI. Pinned versions never update themselves, so a known flaw in a pinned package would otherwise stay unnoticed.

## Where it appears

| Path | What it does |
| --- | --- |
| `Makefile` (`audit`) | Runs `pip-audit` on `requirements-dev.lock`. |
| `.github/workflows/ci.yml` | Runs `make audit` in the `check` job. |

## How it works underneath

pip-audit reads the lock file (`-r requirements-dev.lock`), takes each `name==version`, sends them over the internet to the vulnerability service and prints any matches with the fixed version. It therefore needs a network connection.

## Walkthrough of this repository's use

```make
audit: $(STAMP) ## check the locked dependencies for known vulnerabilities
	$(VENV)/bin/pip-audit --no-deps --disable-pip -r requirements-dev.lock
```

- `-r requirements-dev.lock`: read the list from that file.
- `--no-deps`: do not look up further dependencies; the lock file already lists all of them.
- `--disable-pip`: do not start pip to resolve packages (the versions are already exact).

pip-audit prints two warnings that it prefers fully hashed input and `pip-compile`; the lock already has hashes.

## Try it yourself

1. `make audit` (needs internet) ends with `No known vulnerabilities found`.
2. `.venv/bin/pip-audit --version` prints `pip-audit 2.10.1`.
3. See what a finding looks like: `echo 'jinja2==2.4.1' > /tmp/old.txt && .venv/bin/pip-audit --no-deps --disable-pip -r /tmp/old.txt` lists vulnerabilities for that old version.

## Mistakes people make

- Running it offline and reading the failure as a finding.
- Ignoring a finding because "it is only a dev tool". Development tools run on your machine too.
- Auditing the wrong file. The development lock includes the tools; the run-time lock is a subset.

## Check yourself

1. What does the audit compare? *Each pinned package version against published vulnerabilities.*
2. Why does it need the internet? *The advisory data is fetched from a service.*
3. What does `--no-deps` do? *Skips resolving more dependencies; the list is already complete.*
4. When does CI run it? *In the `check` job on every push.*
5. What fixes a finding? *Updating the pin in `pyproject.toml`, recomputing the lock, and re-running tests.*

## Further reading

- pip-audit 2.10 documentation: https://pypi.org/project/pip-audit/2.10.1/
- Python Packaging Advisory Database: https://github.com/pypa/advisory-database
