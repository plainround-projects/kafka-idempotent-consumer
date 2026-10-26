import subprocess
import sys
from decimal import Decimal
from pathlib import Path

from idempotent_consumer.infrastructure.expected_file import load_expected, save_expected
from idempotent_consumer.infrastructure.subprocess_runner import SubprocessConsumerRunner


def test_expected_totals_round_trip_through_json(tmp_path: Path):
    path = tmp_path / "expected.json"
    totals = {"a": Decimal("10.25"), "b": Decimal("0.00")}
    save_expected(path, totals)
    assert load_expected(path) == totals


class FakeRun:
    def __init__(self) -> None:
        self.commands: list[list[str]] = []

    def __call__(self, command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        self.commands.append(command)
        return subprocess.CompletedProcess(command, 7)


def test_a_crash_run_passes_the_kill_point_and_a_restart_waits_for_idle(tmp_path: Path, monkeypatch):
    fake = FakeRun()
    monkeypatch.setattr(subprocess, "run", fake)
    runner = SubprocessConsumerRunner(tmp_path / "logs" / "crash.log", {"POSTGRES_PASSWORD": "x"})

    assert runner.run("g-1", kill_at=731, commit_every=50) == 7
    runner.run("g-1", kill_at=None, commit_every=50)

    crash, restart = fake.commands
    assert crash[:3] == [sys.executable, "-m", "idempotent_consumer.interfaces.consume_cli"]
    assert crash[crash.index("--kill-at") + 1] == "731"
    assert "--until-idle" not in crash
    assert restart[restart.index("--until-idle") + 1] == "5"
    assert "--kill-at" not in restart
    assert crash[crash.index("--group") + 1] == "g-1"
    assert crash[crash.index("--commit-every") + 1] == "50"
    assert (tmp_path / "logs" / "crash.log").exists()
