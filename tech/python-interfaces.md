# Python: the interfaces layer, file by file

## What this note covers

The folder `src/idempotent_consumer/interfaces/` holds the **command-line programs**: the things you can start from a terminal. Each one reads its arguments, builds the real adapters, hands them to a use case, prints a result, and decides the **exit code**. They are the outermost layer: they may import everything below them, and nothing may import them.

Words you need first:

- A **terminal** is a window where you type commands as text. A **command** is a program name followed by **arguments** (extra words that change what it does). `python -m idempotent_consumer.interfaces.produce_cli --payments 100` runs Python, asks it to run a module by its package name (`-m`), and passes it the argument `--payments` with the value `100`.
- A **process** is a running program. When it ends it hands the operating system a number called its **exit code**. By convention `0` means success and any other number means a problem. The shell, `make` and CI read that number to decide what to do next. This repository uses `0` for success, `1` for "ran but found a problem" (unhealthy, a mismatch, a failed crash run), and `2` for "refused to start because the configuration is wrong".
- **Standard output** (stdout) and **standard error** (stderr) are the two text streams every process has. This repository prints results to stdout and log lines to stderr, so results can be saved to a file without the logs mixed in.

## The files

| File | Program it defines |
| --- | --- |
| `interfaces/__init__.py` | Marks the folder as a package. |
| `arguments.py` | Three small checks for command-line numbers. |
| `common.py` | The start-up step all programs share: read the settings or exit with code 2. |
| `produce_cli.py` | The producer: publishes payments (and, on request, malformed events) and writes the expected totals. |
| `consume_cli.py` | The balance updater: the consumer, naive or idempotent, with the crash switch. |
| `crash_test_cli.py` | The lesson's crash test, many runs. |
| `check_cli.py` | Compares the balances with the expected totals. |
| `health_cli.py` | The health check: exit 0 when healthy. |

## `interfaces/__init__.py`

```python
"""Command-line entry points. They wire adapters into use cases and own exit codes."""
```

Only a docstring; the folder becomes a package, and the sentence states the layer's job.

## `arguments.py`: checking numbers typed on the command line

```python
"""Argument validation shared by the command-line programs."""

from __future__ import annotations

import argparse


def positive_int(text: str) -> int:
    """Parse an integer of at least 1, for argparse."""
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError(f"must be at least 1, got {value}")
    return value


def non_negative_int(text: str) -> int:
    """Parse an integer of at least 0, for argparse."""
    value = int(text)
    if value < 0:
        raise argparse.ArgumentTypeError(f"must not be negative, got {value}")
    return value


def non_negative_float(text: str) -> float:
    """Parse a float of at least 0, for argparse."""
    value = float(text)
    if value < 0:
        raise argparse.ArgumentTypeError(f"must not be negative, got {value}")
    return value
```

**`argparse`** is Python's standard module for reading command-line arguments. For each argument you declare a `type`: a function that receives the text the user typed and returns the value, or raises an error if it is not acceptable.

- Lines 8 to 13: `positive_int` converts the text to a whole number with `int(text)` (a text such as `abc` makes `int` raise `ValueError`, which `argparse` also turns into an error message), then refuses numbers below 1 by raising `argparse.ArgumentTypeError`. `argparse` catches that error, prints the usage line plus the message, and exits with code 2.
- Lines 16 to 21: `non_negative_int` is the same with 0 allowed (used for counts that may be zero, such as `--poison 0`).
- Lines 24 to 29: `non_negative_float` parses a number with a decimal part (seconds), at least 0.

Together they mean `--orders -5` is refused with a clear message before any Kafka client is created.

## `common.py`: the start-up all programs share

```python
"""Startup shared by the programs: settings or a clear exit code 2."""

from __future__ import annotations

import sys

from ..infrastructure.logging import configure_logging
from ..infrastructure.settings import ConfigurationError, Settings


def load_settings() -> Settings | None:
    """Read and validate the environment, configure logging, or print the problem and return None."""
    try:
        settings = Settings.from_env()
    except ConfigurationError as error:
        print(f"configuration error: {error}", file=sys.stderr)
        return None
    configure_logging(settings.log_level)
    return settings
```

- Line 11: `load_settings() -> Settings | None` returns the settings, or `None` when they are unusable.
- Lines 13 to 17: `Settings.from_env()` reads and checks the environment. On `ConfigurationError` the message goes to stderr (`file=sys.stderr`) and the function returns `None`; each program then returns exit code 2. Because the error message names the variable (and never prints the password), the user sees what to fix.
- Lines 18 and 19: otherwise logging is set up at the configured level and the settings are returned.

## `produce_cli.py`: the producer program

```python
"""Publish payments (and, on request, malformed events) to the `payments` topic.

python -m idempotent_consumer.interfaces.produce_cli            # 10,000 payments, writes expected_totals.json
python -m idempotent_consumer.interfaces.produce_cli --payments 0 --poison 1
"""

from __future__ import annotations

import argparse
import random
from collections.abc import Sequence
from pathlib import Path

from ..application.produce_payments import ProducePlan, produce_payments
from ..infrastructure.expected_file import JsonExpectedStore
from ..infrastructure.kafka_producers import KafkaPaymentPublisher
from ..infrastructure.shutdown import ShutdownSignal
from .arguments import non_negative_int
from .common import load_settings

```

- Lines 1 to 5: this docstring is also the program's help text (line 24 passes `__doc__`, the special variable that holds the file's docstring). It shows two ways to run it.
- Lines 9 to 12: standard-library imports. `Path` is an object for a file location.
- Lines 14 to 19: imports from the layers below: the produce use case, the JSON file store, the Kafka publisher, the shutdown flag, the argument checker and the start-up helper.

```python
def build_parser() -> argparse.ArgumentParser:
    """The argument parser of the producer program."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--payments", type=non_negative_int, default=10_000)
    parser.add_argument(
        "--poison", type=non_negative_int, default=0, help="malformed events, spread evenly among the payments"
    )
    parser.add_argument("--seed", type=int, default=None, help="fixed seed for a repeatable run")
    parser.add_argument("--topic", default=None, help="defaults to $TOPIC")
    parser.add_argument("--expected", type=Path, default=Path("expected_totals.json"))
    return parser
```

**`argparse`** is Python's standard module for reading command-line arguments. `add_argument("--name", type=..., default=..., help=...)` declares an option: `type` is a checking function, `default` the value when it is absent, `help` the text for `--help`.

- Line 24: `RawTextHelpFormatter` keeps the docstring's line breaks in the help text.
- Line 25: `--payments`, a non-negative whole number, default 10,000.
- Lines 26 to 28: `--poison`, how many malformed events to spread among the payments, default 0.
- Line 29: `--seed` takes any whole number; `None` means a random seed. A seed makes a run repeatable (same payments, same ids).
- Line 30: `--topic` defaults to `None`, meaning "use `$TOPIC`".
- Line 31: `--expected` is the path to the expected-totals file, as a `Path`; default `expected_totals.json` in the current folder.

```python
def main(argv: Sequence[str] | None = None) -> int:
    """Publish payments and write the expected totals; returns the process exit code."""
    args = build_parser().parse_args(argv)
    settings = load_settings()
    if settings is None:
        return 2
    shutdown = ShutdownSignal()
    shutdown.install()
    publisher = KafkaPaymentPublisher(settings.kafka, args.topic)
    try:
        result = produce_payments(
            publisher,
            JsonExpectedStore(args.expected),
            ProducePlan(args.payments, args.poison),
            random.Random(args.seed),
            stop=lambda: shutdown.requested,
        )
    finally:
        publisher.close()
    if result.payments and not result.stopped:
        print(f"wrote {args.expected} (the expected total of each of the 50 accounts)")
    note = " (stopped early)" if result.stopped else ""
    topic = args.topic or settings.kafka.topic
    print(f"produced {result.payments} payments and {result.poison} malformed events to {topic}{note}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- Line 37: `parse_args(argv)` reads and checks the arguments. A bad one makes `argparse` print a usage line and exit with code 2 by itself.
- Lines 38 to 40: settings or exit code 2.
- Lines 41 and 42: create the shutdown flag and install the signal handlers.
- Line 43: the real Kafka publisher.
- Lines 44 to 53: the use case runs inside `try ... finally`: `finally` always runs, so `publisher.close()` happens even if an error is raised or a signal arrived. `ProducePlan(args.payments, args.poison)` is the plan; `random.Random(args.seed)` the generator; `stop=lambda: shutdown.requested` links the use case to the signal flag.
- Lines 54 to 58: print results to stdout. The expected-totals message appears only if the run was complete (`result.payments and not result.stopped`). The note says if the run was stopped early.
- Line 59: return 0.

```python
if __name__ == "__main__":
    raise SystemExit(main())
```

`__name__` is a special variable. When a file is started as a program (`python -m ...`) its value is `"__main__"`; when another file merely imports it, it is the module's name. So these two lines run `main()` only when the file is started as a program, never when a test imports it. `SystemExit(code)` ends the process with that exit code.

## `consume_cli.py`: the balance updater

```python
"""The balance updater: reads payments and keeps the `balances` table up to date.

python -m idempotent_consumer.interfaces.consume_cli --mode naive --kill-at 4731
python -m idempotent_consumer.interfaces.consume_cli --mode idempotent --until-idle 5
"""

from __future__ import annotations

import argparse
import os
import time
from collections.abc import Callable, Sequence

from ..application.consume_payments import ConsumePlan, consume_payments
from ..infrastructure import postgres
from ..infrastructure.kafka_consumer import KafkaPaymentConsumer
from ..infrastructure.kafka_producers import KafkaDeadLetterSink
from ..infrastructure.postgres_ledger import IdempotentLedger, NaiveLedger
from ..infrastructure.shutdown import ShutdownSignal
from .arguments import non_negative_float, non_negative_int, positive_int
from .common import load_settings
```

The imports show the whole assembly: the use case, the Kafka consumer and dead-letter sink, the two ledgers, the PostgreSQL connection helper and the shutdown flag.

```python
def build_parser() -> argparse.ArgumentParser:
    """The argument parser of the consumer program."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--mode", choices=["naive", "idempotent"], default="idempotent")
    parser.add_argument("--group", default=None, help="defaults to $GROUP_ID")
    parser.add_argument("--topic", default=None, help="defaults to $TOPIC")
    parser.add_argument("--dlq-topic", default=None, help="defaults to $DLQ_TOPIC")
    parser.add_argument("--name", default="balance-updater", help="client id and label in the output")
    parser.add_argument(
        "--kill-at",
        type=non_negative_int,
        default=0,
        help="crash (os._exit) after the DB commit of this record, before its offset commit; 0 for never",
    )
    parser.add_argument("--commit-every", type=positive_int, default=1, help="commit offsets after this many records")
    parser.add_argument("--delay", type=non_negative_float, default=0.0, help="artificial seconds of work per record")
    parser.add_argument("--max-poll-records", type=positive_int, default=500)
    parser.add_argument("--max-poll-interval-ms", type=positive_int, default=300_000)
    parser.add_argument(
        "--until-idle", type=non_negative_float, default=None, help="exit after this many seconds with no records"
    )
    parser.add_argument(
        "--run-seconds", type=non_negative_float, default=None, help="exit after this many seconds in total"
    )
    return parser
```

- Line 27: `--mode` with `choices=["naive", "idempotent"]`: only those two words are accepted. `naive` is the lesson's first two steps (add and forget); `idempotent` is the fix.
- Lines 28 to 31: `--group`, `--topic` and `--dlq-topic` default to the environment; `--name` is the Kafka client id and the label printed with results.
- Lines 32 to 37: `--kill-at N` makes the process crash right after the database commit of its N-th record and before that record's offset commit; 0 (the default) means never.
- Line 38: `--commit-every` is how many records to process between offset commits.
- Line 39: `--delay` is artificial seconds of work per record.
- Lines 40 and 41: `--max-poll-records` and `--max-poll-interval-ms` set the eviction knobs of the consumer.
- Lines 42 to 47: `--until-idle` and `--run-seconds` end the program after quiet seconds or after total seconds.

```python
def kill_switch(kill_at: int, name: str) -> Callable[[int], None]:
    """Crash like a killed process: no cleanup, no offset commit, no group leave."""

    def after_apply(processed: int) -> None:
        if kill_at and processed == kill_at:
            print(
                f"[{name}] kill switch: crashing after the database commit of record {processed}, "
                "before its offset commit",
                flush=True,
            )
            os._exit(1)

    return after_apply
```

- Line 51: `kill_switch(kill_at, name)` returns a function. The return type `Callable[[int], None]` means "a function taking a whole number and returning nothing".
- Lines 54 to 61: the inner function `after_apply(processed)` is what the use case calls after each record's database commit. If `kill_at` is set and the number of processed records equals it, it prints a message (`flush=True` forces the text out at once, because the process is about to die without flushing) and calls `os._exit(1)`.
- **`os._exit(1)` is a hard stop.** It ends the process immediately with exit code 1 and skips everything a normal exit does: no `finally` blocks run, no cleanup, no offset commit, no leaving the consumer group. That is what a real crash or `kill -9` looks like, which is the whole point of the test.
- Line 63: returns the inner function. This is a **closure**: the inner function remembers `kill_at` and `name` from where it was made.

```python
def main(argv: Sequence[str] | None = None) -> int:
    """Run the balance updater; returns the process exit code."""
    args = build_parser().parse_args(argv)
    settings = load_settings()
    if settings is None:
        return 2
    shutdown = ShutdownSignal()
    shutdown.install()

    connection = postgres.connect(settings.postgres)
    ledger = NaiveLedger(connection) if args.mode == "naive" else IdempotentLedger(connection)
    dead_letters = KafkaDeadLetterSink(settings.kafka, args.dlq_topic)
    consumer = KafkaPaymentConsumer(
        settings.kafka,
        group_id=args.group,
        topic=args.topic,
        client_id=args.name,
        max_poll_records=args.max_poll_records,
        max_poll_interval_ms=args.max_poll_interval_ms,
    )
    plan = ConsumePlan(
        commit_every=args.commit_every,
        work_delay_seconds=args.delay,
        idle_timeout_seconds=args.until_idle,
        run_seconds=args.run_seconds,
    )
    try:
        counts = consume_payments(
            consumer, consumer, ledger, dead_letters, plan,
            stop=lambda: shutdown.requested, clock=time.time, sleep=time.sleep,
            after_apply=kill_switch(args.kill_at, args.name),
        )  # fmt: skip
    finally:
        consumer.close()
        dead_letters.close()
        connection.close()
    print(f"[{args.name}] done: {counts.summary()}", flush=True)
    return 0
```

- Lines 68 to 71: arguments, then settings or exit 2. Lines 72 and 73: shutdown flag.
- Line 75: `postgres.connect(settings.postgres)` opens the database connection (with retries).
- Line 76: `NaiveLedger(connection) if args.mode == "naive" else IdempotentLedger(connection)` picks the ledger: a one-line choice.
- Line 77: the dead-letter sink. Lines 78 to 85: the Kafka consumer, with the group, topic, client id and the two poll limits from the arguments.
- Lines 86 to 91: the `ConsumePlan` from the options.
- Lines 92 to 97: the use case is run inside `try ... finally`. The consumer plays two roles, so it is passed twice: as the source and as the committer. `stop=lambda: shutdown.requested`, `clock=time.time` and `sleep=time.sleep` connect real time and signals. `after_apply=kill_switch(...)` plugs in the crash switch. `# fmt: skip` tells the formatter to leave this statement's layout alone.
- Lines 98 to 101: `finally` closes the consumer (leaves the group), the dead-letter producer and the database connection, whether the run finished, failed or was stopped by a signal.
- Line 102: prints `[name] done: {...}` with the four counters. Line 103: return 0.

## `crash_test_cli.py`: the lesson's crash test

```python
"""Lesson step 3: crash the idempotent consumer at many points and check the balances after each.

    python -m idempotent_consumer.interfaces.crash_test_cli --runs 20
Each run: zero the balances, start a fresh consumer group, crash it at a different record, restart it
to completion, compare with expected_totals.json. Consumer output goes to logs/crash-test.log.
"""

from __future__ import annotations

import argparse
import time
from collections.abc import Sequence
from pathlib import Path

from ..application.crash_test import CrashRun, run_crash_test
from ..infrastructure import postgres
from ..infrastructure.expected_file import load_expected
from ..infrastructure.postgres_ledger import PostgresBalanceReader, PostgresLedgerAdmin
from ..infrastructure.subprocess_runner import SubprocessConsumerRunner
from .arguments import positive_int
from .common import load_settings


def main(argv: Sequence[str] | None = None) -> int:
    """Run the crash test; returns 1 when any run fails."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--runs", type=positive_int, default=20)
    parser.add_argument("--payments", type=positive_int, default=10_000, help="payments in the topic")
    parser.add_argument("--expected", type=Path, default=Path("expected_totals.json"))
    parser.add_argument(
        "--commit-every",
        type=positive_int,
        default=50,
        help="offset commit interval; a crash then redelivers up to this many applied events",
    )
    args = parser.parse_args(argv)
```

- Lines 1 to 6: the docstring is the help text and tells the story: each run zeroes the balances, starts a fresh consumer group, crashes it at a different record, restarts it to completion and compares with the expected totals.
- Lines 27 to 35: `--runs` (default 20), `--payments` (how many payments are in the topic, default 10,000), `--expected` (the file), and `--commit-every` (default 50): "a crash then redelivers up to this many applied events", so the idempotence is actually exercised.

```python
    settings = load_settings()
    if settings is None:
        return 2
    expected = load_expected(args.expected)
    stamp = int(time.time())

    def report(run: CrashRun) -> None:
        label = f"run {run.number}/{args.runs} (kill at {run.kill_at})"
        if run.passed:
            print(
                f"{label}: crashed, restarted, all {len(expected)} balances match expected totals exactly", flush=True
            )
        else:
            print(
                f"{label}: FAILED crash exit {run.crash_exit_code}, restart exit {run.restart_exit_code}, "
                f"{len(run.mismatches)} mismatched balances",
                flush=True,
            )

    with postgres.connect(settings.postgres) as connection:
        results = run_crash_test(
            SubprocessConsumerRunner(Path("logs/crash-test.log")),
            PostgresLedgerAdmin(connection),
            PostgresBalanceReader(connection),
            expected,
            runs=args.runs,
            payments=args.payments,
            commit_every=args.commit_every,
            group_for_run=lambda number: f"crash-test-{stamp}-{number}",
            on_run=report,
        )
    return 0 if all(run.passed for run in results) else 1
```

- Lines 37 to 41: settings or exit 2; load the expected totals; `stamp = int(time.time())` is the current time in seconds, used to give every run unique group names.
- Lines 43 to 54: `report(run)` is an inner function printed once per run: `run 3/20 (kill at 5074): crashed, restarted, all 50 balances match expected totals exactly`, or, if it failed, the exit codes and the number of mismatched balances.
- Lines 56 to 67: `with postgres.connect(settings.postgres) as connection:` opens the database connection and closes it at the end of the block. The use case gets the real process runner (which writes `logs/crash-test.log`), the admin and reader that share the connection, the expected totals, and the options. `group_for_run=lambda number: f"crash-test-{stamp}-{number}"` is a nameless function that makes each run's group name.
- Line 68: `0 if all(run.passed for run in results) else 1`: exit 0 only if every run passed.

## `check_cli.py`: compare the balances with the answer

```python
"""Compare the balances table with expected_totals.json. Exit code 1 on any mismatch."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

from ..application.check_balances import check_balances
from ..infrastructure import postgres
from ..infrastructure.expected_file import load_expected
from ..infrastructure.postgres_ledger import PostgresBalanceReader
from .common import load_settings


def main(argv: Sequence[str] | None = None) -> int:
    """Compare balances with the expected totals; returns 1 on any mismatch."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected", type=Path, default=Path("expected_totals.json"))
    args = parser.parse_args(argv)
    settings = load_settings()
    if settings is None:
        return 2
    expected = load_expected(args.expected)
    with postgres.connect(settings.postgres) as connection:
        mismatches = check_balances(PostgresBalanceReader(connection), expected)
    for mismatch in mismatches:
        print(mismatch)
    if mismatches:
        return 1
    print(f"all {len(expected)} balances match expected totals exactly")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- Lines 18 to 20: a small parser inside `main` with one option, `--expected`.
- Lines 21 to 24: settings or exit 2; load the expected totals.
- Lines 25 and 26: connect and ask the use case for mismatches. Lines 27 and 28: print each one (using `Mismatch.__str__`).
- Lines 29 to 32: exit 1 if there was any mismatch; otherwise print `all 50 balances match expected totals exactly` and return 0. The number printed is `len(expected)`, the number of accounts.

## `health_cli.py`: the health check

```python
"""Health command: exit 0 when Kafka has the topics and PostgreSQL has the balances table.

python -m idempotent_consumer.interfaces.health_cli
"""

from __future__ import annotations

from collections.abc import Sequence

from ..application.health import check_health
from ..infrastructure.health_probes import KafkaTopicsProbe, PostgresSchemaProbe
from .common import load_settings


def main(_argv: Sequence[str] | None = None) -> int:
    """Check Kafka and PostgreSQL; returns 0 when healthy."""
    settings = load_settings()
    if settings is None:
        return 2
    report = check_health([KafkaTopicsProbe(settings.kafka), PostgresSchemaProbe(settings.postgres)])
    for result in report.results:
        print(f"{'ok' if result.ok else 'FAIL'} {result.name}: {result.detail}")
    return 0 if report.healthy else 1


if __name__ == "__main__":
    raise SystemExit(main())
```

- Line 15: the parameter is named `_argv` with a leading underscore because this program takes no arguments; the name says the value is unused.
- Lines 17 to 19: settings or exit 2.
- Line 20: runs two probes, Kafka and PostgreSQL.
- Lines 21 and 22: prints a line per probe: `ok kafka: topics payments and payments-dlq exist`, `ok postgres: table balances exists` or `FAIL ...`. The expression `{'ok' if result.ok else 'FAIL'}` is a one-line choice inside an f-string.
- Line 23: `return 0 if report.healthy else 1`. This is the exit code `make health` and the Docker `HEALTHCHECK` use.

## Try it yourself

```bash
.venv/bin/python -m idempotent_consumer.interfaces.consume_cli --mode fast; echo "exit code: $?"
```

Expected: a usage line and `consume_cli.py: error: argument --mode: invalid choice: 'fast' (choose from naive, idempotent)`, and `exit code: 2`. No broker needed.

```bash
BOOTSTRAP_SERVERS=nonsense .venv/bin/python -m idempotent_consumer.interfaces.health_cli; echo "exit code: $?"
```

Expected: `configuration error: BOOTSTRAP_SERVERS must be host:port[,host:port], got 'nonsense'` on stderr and `exit code: 2`.

## Mistakes people make

- Printing logs to stdout: then `program > result.txt` fills the file with log lines. Logs go to stderr here.
- Using `sys.exit` deep inside the code. Only the `__main__` block here ends the process; everything else returns values or raises named errors. The single exception is the deliberate `os._exit(1)` of the crash switch.
- Using `os._exit` to "stop cleanly". It is for crash simulation only: it skips every cleanup.

## Check yourself

1. What are the exit codes here? (0 success, 1 a problem found, 2 a configuration or argument error.)
2. What does `os._exit(1)` skip that `sys.exit(1)` does not? (`finally` blocks and cleanup, including the offset commit and leaving the group.)
3. Why is the consumer passed twice to `consume_payments`? (One object plays two roles: source and committer.)
