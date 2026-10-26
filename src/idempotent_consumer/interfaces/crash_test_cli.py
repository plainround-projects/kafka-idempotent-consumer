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


if __name__ == "__main__":
    raise SystemExit(main())
