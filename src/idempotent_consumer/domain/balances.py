"""Compare balances with the totals they must have."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class Mismatch:
    account_id: str
    expected: Decimal
    actual: Decimal

    @property
    def off_by(self) -> Decimal:
        return self.actual - self.expected

    def __str__(self) -> str:
        return f"MISMATCH {self.account_id}: expected {self.expected}, got {self.actual} (off by {self.off_by:+})"


def diff_balances(expected: Mapping[str, Decimal], actual: Mapping[str, Decimal]) -> list[Mismatch]:
    zero = Decimal("0")
    return [
        Mismatch(account, expected.get(account, zero), actual.get(account, zero))
        for account in sorted(set(expected) | set(actual))
        if expected.get(account, zero) != actual.get(account, zero)
    ]
