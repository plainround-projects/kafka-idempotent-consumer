"""The lesson's scenarios through the command-line programs, as the Makefile runs them.

make up && make test-e2e. Each scenario starts real processes, including one that crashes on purpose.
"""

import os
import re
import subprocess
import sys
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
import pytest

from idempotent_consumer.domain.balances import diff_balances
from idempotent_consumer.infrastructure.expected_file import load_expected
from idempotent_consumer.infrastructure.postgres_ledger import PostgresBalanceReader

pytestmark = pytest.mark.e2e


def program(module: str, topics: tuple[str, str], *args: str) -> subprocess.CompletedProcess[str]:
    environment = {**os.environ, "TOPIC": topics[0], "DLQ_TOPIC": topics[1]}
    return subprocess.run(
        [sys.executable, "-m", f"idempotent_consumer.interfaces.{module}", *args],
        capture_output=True,
        text=True,
        env=environment,
        timeout=240,
        check=False,
    )


def produce(topics: tuple[str, str], expected: Path, payments: int, poison: int = 0) -> None:
    result = program(
        "produce_cli",
        topics,
        "--payments",
        str(payments),
        "--poison",
        str(poison),
        "--seed",
        "1",
        "--expected",
        str(expected),
    )
    assert result.returncode == 0, result.stderr


def wrong_balances(connection: psycopg.Connection[Any], expected: Path) -> list[Any]:
    return diff_balances(load_expected(expected), PostgresBalanceReader(connection).read_balances())


def test_the_naive_consumer_double_counts_one_payment_after_a_crash(
    connection: psycopg.Connection[Any], topics: tuple[str, str], tmp_path: Path
) -> None:
    expected = tmp_path / "expected.json"
    produce(topics, expected, payments=300)
    group = f"naive-{uuid.uuid4().hex}"

    assert program("consume_cli", topics, "--mode", "naive", "--group", group, "--kill-at", "100").returncode == 1
    assert program("consume_cli", topics, "--mode", "naive", "--group", group, "--until-idle", "4").returncode == 0

    wrong = wrong_balances(connection, expected)
    assert len(wrong) == 1  # exactly one payment was applied twice: the one in flight at the crash
    assert wrong[0].off_by > Decimal(0)


def test_the_idempotent_consumer_ends_with_correct_balances_after_a_crash(
    connection: psycopg.Connection[Any], topics: tuple[str, str], tmp_path: Path
) -> None:
    expected = tmp_path / "expected.json"
    produce(topics, expected, payments=300)
    group = f"idempotent-{uuid.uuid4().hex}"

    assert program("consume_cli", topics, "--group", group, "--kill-at", "100").returncode == 1
    resumed = program("consume_cli", topics, "--group", group, "--until-idle", "4")

    assert resumed.returncode == 0
    assert "'duplicate': 1" in resumed.stdout  # the redelivered payment was found and skipped
    assert wrong_balances(connection, expected) == []
    assert connection.execute("SELECT count(*) FROM processed_events").fetchone() == (300,)


def test_a_poison_message_goes_to_the_dead_letter_topic_and_the_consumer_keeps_going(
    connection: psycopg.Connection[Any], topics: tuple[str, str], tmp_path: Path
) -> None:
    expected = tmp_path / "expected.json"
    produce(topics, expected, payments=200, poison=1)

    finished = program("consume_cli", topics, "--group", f"poison-{uuid.uuid4().hex}", "--until-idle", "4")

    assert finished.returncode == 0
    assert "'dead-lettered': 1" in finished.stdout
    assert "'applied': 200" in finished.stdout
    assert wrong_balances(connection, expected) == []  # the payments before and after the bad one were all applied


def evictions(output: str) -> int:
    match = re.search(r"'evictions': (\d+)", output)
    assert match, output
    return int(match.group(1))


def test_slow_batches_get_the_consumer_evicted_and_smaller_batches_do_not(
    connection: psycopg.Connection[Any], topics: tuple[str, str], tmp_path: Path
) -> None:
    produce(topics, tmp_path / "expected.json", payments=300)
    slow = ("--delay", "0.1", "--max-poll-interval-ms", "3000", "--run-seconds", "10")

    evicted = program("consume_cli", topics, "--group", f"evict-{uuid.uuid4().hex}", *slow, "--max-poll-records", "100")
    fixed = program("consume_cli", topics, "--group", f"fixed-{uuid.uuid4().hex}", *slow, "--max-poll-records", "10")

    assert evictions(evicted.stdout) >= 1
    assert "poll timeout" in evicted.stderr.lower()
    assert evictions(fixed.stdout) == 0


def test_the_checker_and_health_commands_report_the_state(
    connection: psycopg.Connection[Any], topics: tuple[str, str], tmp_path: Path
) -> None:
    expected = tmp_path / "expected.json"
    produce(topics, expected, payments=50)
    program("consume_cli", topics, "--group", f"check-{uuid.uuid4().hex}", "--until-idle", "4")

    assert "all 50 balances match" in program("check_cli", topics, "--expected", str(expected)).stdout
    connection.execute("UPDATE balances SET total = total + 1 WHERE account_id = 'acct-001'")
    wrong = program("check_cli", topics, "--expected", str(expected))
    assert wrong.returncode == 1
    assert "MISMATCH acct-001" in wrong.stdout
    assert program("health_cli", topics).returncode == 0
