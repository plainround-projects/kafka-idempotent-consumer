"""Use case: compare the balance table with the totals it must have."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal

from ..domain.balances import Mismatch, diff_balances
from .ports import BalanceReader


def check_balances(reader: BalanceReader, expected: Mapping[str, Decimal]) -> list[Mismatch]:
    return diff_balances(expected, reader.read_balances())
