# Bash (shell commands in the Makefile and CI)

## What it is

A **shell** is a program that reads commands and runs them. **Bash** is the most common one. Besides typing commands, you can put several in one line or one recipe, with a few operators that join them. This repository has no standalone `.sh` files; its shell appears inside `Makefile` recipes and in the `run:` lines of the CI workflow. Make hands each recipe line to a shell (`/bin/sh`, which on a Mac is Bash in compatibility mode).

## Why this repository uses it

The demos need a few small pieces of logic: remember the exit code of a crash, run a command for every migration file, create `.env` only if missing, pick a unique group name from the clock. Shell does that in a line or two.

## Where it appears

| Path | What it does with shell |
| --- | --- |
| `Makefile` (`env`, `migrate`, `demo-crash`, `demo-poison`, `demo-evict`) | Loops, `;`, `&&`, `||`, `$$(...)`, pipes, redirects. |
| `.github/workflows/ci.yml` | Each `run:` line is run by Bash on the CI machine. |
| `docker-compose.yml` (`healthcheck`) | `CMD-SHELL` entries are run by a shell inside the container. |

## How it works underneath

The shell starts each command as a new **process** (a running program) and waits. When it ends it leaves an **exit code**: `0` means success, anything else means failure, and `$?` holds the last one. `;` runs commands one after another. `A && B` runs B only if A succeeded. `A || B` runs B only if A failed. `|` (pipe) feeds the output of one command to the next. `<` feeds a file to a command's input. `>` sends output to a file; `2>&1` sends error output to the same place as normal output. `$(command)` is replaced by the command's output.

## Walkthrough of this repository's use

### Create `.env` only when it is missing

```make
env:
	@[ -f .env ] || { cp .env.example .env; echo "created .env from .env.example"; }
```

`[ -f .env ]` tests "is there a regular file named `.env`?". If yes, the left side succeeds and `||` skips the right. If not, `{ cp .env.example .env; echo "..."; }` runs: copy the template and say so. The `@` at the start stops Make printing the line.

### A loop over files

```make
migrate: ## apply sql/migrations/*.sql (adds processed_events)
	@for file in sql/migrations/*.sql; do echo "applying $$file"; $(PSQL) < $$file; done
```

`for file in sql/migrations/*.sql; do ...; done` repeats the commands once per matching file, in name order. `$$file` is the loop variable (Make needs `$$` for one literal `$`). The body prints a line and then pipes the file into `psql` with `<`.

### Keeping an exit code across lines

```make
demo-crash: data ## step 2: naive consumer crashes between DB commit and offset commit, a balance is wrong
	@group=crash-$$(date +%s); \
	$(CONSUME) --mode naive --group $$group --kill-at $(DEMO_KILL_AT); \
	echo "crash exit code: $$?"; \
	$(CONSUME) --mode naive --group $$group --until-idle 5; \
	$(CHECK) && echo "UNEXPECTED: no mismatch" || echo "(the mismatch above is the bug this step shows)"
```

- `group=crash-$$(date +%s)` stores a unique name in a variable (`date +%s` prints the clock as seconds since 1970). The trailing `\` makes the lines one command, so the variable lives through all of them (Make normally runs each line in a fresh shell).
- The first consumer is meant to crash; `echo "crash exit code: $$?"` prints its exit code (`1`, set by `os._exit(1)`).
- `$(CHECK) && echo "UNEXPECTED..." || echo "(the mismatch ...)"`: the checker exits `1` when balances differ. Then the `&&` branch is skipped and the `||` branch prints the explanation. If the checker unexpectedly passes, the first message prints instead.

### Pipes that filter output

```make
demo-evict: data migrate ## step 5: slow batches get the consumer evicted; smaller batches do not
	@echo "== max.poll.records=500: one batch is 50 s of work, longer than the 10 s poll interval"
	$(CONSUME) --mode idempotent --group evict-$$(date +%s) --delay 0.1 \
	  --max-poll-records 500 --max-poll-interval-ms 10000 --run-seconds 40 2>&1 | grep -E "EVICTED|poll timeout|done:" | cut -c1-200
```

`2>&1 | grep -E "EVICTED|poll timeout|done:" | cut -c1-200` joins the program's error and normal output, keeps only lines matching any of the three words (`grep -E` uses extended patterns, `|` means "or" inside the quotes), and cuts each line to 200 characters. The long log lines of the eviction demo become readable.

## Try it yourself

1. `bash -c '[ -f Makefile ] && echo yes || echo no'` prints `yes`; with a missing file name it prints `no`.
2. `for f in sql/migrations/*.sql; do echo "applying $f"; done` prints `applying sql/migrations/001_processed_events.sql`.
3. `bash -c 'false || echo recovered; true && echo ok'` prints `recovered` then `ok`.
4. `make -n demo-crash` prints the whole joined command without running it.
5. `printf 'a EVICTED\nb ok\n' | grep -E "EVICTED|poll timeout"` prints only `a EVICTED`.

## Mistakes people make

- Writing `$` where Make needs `$$` in a recipe.
- Expecting a variable set on one recipe line to exist on the next. Join the lines with `\`.
- Reading `A && B || C` as if-then-else: if B itself fails, C also runs. It is fine here because `echo` never fails.
- Forgetting that a failing command stops a Make recipe, unless the line starts with `-` or ends in `|| true`.

## Check yourself

1. What does `$?` hold? *The exit code of the last command.*
2. What does `A && B` mean? *Run B only if A succeeded.*
3. Why `$$` in a Makefile? *Make would read `$`; `$$` passes one `$` to the shell.*
4. What does `2>&1` do? *Sends error output to the same place as normal output.*
5. Why join recipe lines with `\`? *So they run in one shell and share variables.*

## Further reading

- Bash reference manual: https://www.gnu.org/software/bash/manual/bash.html
- POSIX shell command language: https://pubs.opengroup.org/onlinepubs/9699919799/utilities/V3_chap02.html
- `grep`: https://pubs.opengroup.org/onlinepubs/9699919799/utilities/grep.html
