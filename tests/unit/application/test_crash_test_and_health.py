from decimal import Decimal

from idempotent_consumer.application.check_balances import check_balances
from idempotent_consumer.application.crash_test import CrashRun, run_crash_test
from idempotent_consumer.application.health import ProbeFailedError, check_health


class FakeRunner:
    def __init__(self, crash_code: int = 1, restart_code: int = 0) -> None:
        self.calls: list[tuple[str, int | None, int]] = []
        self._codes = (crash_code, restart_code)

    def run(self, group_id: str, kill_at: int | None, commit_every: int) -> int:
        self.calls.append((group_id, kill_at, commit_every))
        return self._codes[0] if kill_at else self._codes[1]


class FakeAdmin:
    def __init__(self) -> None:
        self.resets = 0

    def reset(self) -> None:
        self.resets += 1


class FixedReader:
    def __init__(self, balances: dict[str, Decimal]) -> None:
        self._balances = balances

    def read_balances(self) -> dict[str, Decimal]:
        return self._balances


EXPECTED = {"acct-001": Decimal(10)}


def crash_test(runner, reader, runs=3):
    seen: list[CrashRun] = []
    admin = FakeAdmin()
    results = run_crash_test(
        runner, admin, reader, EXPECTED, runs=runs, payments=300, commit_every=50,
        group_for_run=lambda number: f"g-{number}", on_run=seen.append,
    )  # fmt: skip
    return results, admin, seen


def test_every_run_resets_crashes_a_fresh_group_restarts_it_and_compares():
    runner = FakeRunner()
    results, admin, seen = crash_test(runner, FixedReader(EXPECTED))
    assert admin.resets == 3
    assert [run.number for run in results] == [1, 2, 3]
    assert seen == results
    assert all(run.passed for run in results)
    groups = [call[0] for call in runner.calls]
    assert groups == ["g-1", "g-1", "g-2", "g-2", "g-3", "g-3"]  # crash then restart of the same group
    assert [call[1] is not None for call in runner.calls] == [True, False] * 3
    assert {call[2] for call in runner.calls} == {50}


def test_a_run_fails_when_a_balance_is_wrong():
    results, _, _ = crash_test(FakeRunner(), FixedReader({"acct-001": Decimal(20)}), runs=1)
    assert not results[0].passed
    assert len(results[0].mismatches) == 1


def test_a_run_fails_when_the_kill_switch_did_not_fire_or_the_restart_failed():
    assert not crash_test(FakeRunner(crash_code=0), FixedReader(EXPECTED), runs=1)[0][0].passed
    assert not crash_test(FakeRunner(restart_code=3), FixedReader(EXPECTED), runs=1)[0][0].passed


def test_check_balances_lists_the_accounts_that_differ():
    assert check_balances(FixedReader(EXPECTED), EXPECTED) == []
    assert [m.account_id for m in check_balances(FixedReader({"acct-001": Decimal(11)}), EXPECTED)] == ["acct-001"]


class Healthy:
    name = "kafka"

    def check(self) -> str:
        return "topics exist"


class Broken:
    name = "postgres"

    def check(self) -> str:
        raise ProbeFailedError("connection refused")


def test_a_report_is_healthy_only_when_every_probe_is():
    assert check_health([Healthy()]).healthy
    report = check_health([Healthy(), Broken()])
    assert not report.healthy
    assert [(r.name, r.ok, r.detail) for r in report.results] == [
        ("kafka", True, "topics exist"),
        ("postgres", False, "connection refused"),
    ]
