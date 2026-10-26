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
