"""Adapter: run the idempotent consumer as a real process, so a crash is a real crash."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


class SubprocessConsumerRunner:
    """Implements `ConsumerRunner`. Output goes to a log file, not the terminal."""

    def __init__(self, log_path: Path, environment: dict[str, str] | None = None) -> None:
        self._log_path = log_path
        self._environment = {**os.environ, **(environment or {})}

    def run(self, group_id: str, kill_at: int | None, commit_every: int) -> int:
        command = [
            sys.executable, "-m", "idempotent_consumer.interfaces.consume_cli",
            "--mode", "idempotent", "--group", group_id, "--commit-every", str(commit_every),
        ]  # fmt: skip
        command += ["--kill-at", str(kill_at)] if kill_at else ["--until-idle", "5"]
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        with self._log_path.open("a") as log:
            return subprocess.run(command, env=self._environment, stdout=log, stderr=log, check=False).returncode
