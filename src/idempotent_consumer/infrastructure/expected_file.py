"""Adapter: the expected totals the producer writes and the checker reads (a JSON file)."""

from __future__ import annotations

import json
from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path


def save_expected(path: Path, totals: Mapping[str, Decimal]) -> None:
    """Write the expected totals as JSON."""
    path.write_text(json.dumps({account: str(total) for account, total in totals.items()}, indent=1))


def load_expected(path: Path) -> dict[str, Decimal]:
    """Read the expected totals written by `save_expected`."""
    return {account: Decimal(total) for account, total in json.loads(path.read_text()).items()}
