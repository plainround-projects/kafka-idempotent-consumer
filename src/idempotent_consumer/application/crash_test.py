"""Use case: crash the idempotent consumer at many points and check the balances after each restart."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal

from ..domain.balances import Mismatch
from ..domain.crash_plan import kill_points
from .check_balances import check_balances
from .ports import BalanceReader, ConsumerRunner, LedgerAdmin


@dataclass(frozen=True, slots=True)
class CrashRun:
    number: int
    kill_at: int
    crash_exit_code: int
    restart_exit_code: int
    mismatches: tuple[Mismatch, ...]

    @property
    def passed(self) -> bool:
        """The consumer crashed (exit 1), restarted cleanly (exit 0) and every balance is right."""
        return self.crash_exit_code == 1 and self.restart_exit_code == 0 and not self.mismatches


def run_crash_test(
    runner: ConsumerRunner,
    admin: LedgerAdmin,
    reader: BalanceReader,
    expected: Mapping[str, Decimal],
    *,
    runs: int,
    payments: int,
    commit_every: int,
    group_for_run: Callable[[int], str],
    on_run: Callable[[CrashRun], None] = lambda _run: None,
) -> list[CrashRun]:
    """Each run: zero the tables, crash a fresh group at a different record, restart it, compare."""
    results = []
    for number, kill_at in enumerate(kill_points(runs, payments), start=1):
        admin.reset()
        group = group_for_run(number)
        crash_code = runner.run(group, kill_at, commit_every)
        restart_code = runner.run(group, None, commit_every)
        run = CrashRun(number, kill_at, crash_code, restart_code, tuple(check_balances(reader, expected)))
        results.append(run)
        on_run(run)
    return results
