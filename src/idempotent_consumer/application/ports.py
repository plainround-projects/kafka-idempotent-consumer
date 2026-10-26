"""The ports the use cases need. Infrastructure adapters implement them; tests use fakes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Protocol

from ..domain.dead_letter import DeadLetter
from ..domain.payments import Payment


class CommitRejectedError(Exception):
    """The group no longer lets this member commit offsets, for example after an eviction."""


class Outcome(StrEnum):
    """What happened to one payment."""

    APPLIED = "applied"
    DUPLICATE = "duplicate"
    DEAD_LETTERED = "dead-lettered"


@dataclass(frozen=True, slots=True)
class Delivery:
    """A record as read from the log."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes


class PaymentSource(Protocol):
    """Reads payment records from the log as a member of a consumer group."""

    def poll(self, timeout_seconds: float) -> Sequence[Delivery]:
        """Return the next records, or an empty sequence when none arrived in time."""

    def has_assignment(self) -> bool:
        """Whether the group has given this member any partition yet."""

    def close(self) -> None:
        """Leave the consumer group."""


class OffsetCommitter(Protocol):
    """Commits consumer offsets to the group."""

    def commit(self, next_offsets: Mapping[tuple[str, int], int]) -> None:
        """Commit, per (topic, partition), the offset of the next record to read.

        Raises `CommitRejectedError` when the group refuses the commit.
        """


class DeadLetterSink(Protocol):
    """Parks records that can never be processed."""

    def send(self, letter: DeadLetter) -> None:
        """Park an unprocessable record. Returns only once it is safely stored."""


class BalanceLedger(Protocol):
    """Applies payments to account balances."""

    def apply(self, payment: Payment, delivery: Delivery) -> Outcome:
        """Add the payment to its account in one database transaction.

        Returns APPLIED or DUPLICATE. Raises `PoisonMessageError` for a payment that can never be
        applied (an unknown account); nothing is written in that case.
        """


class BalanceReader(Protocol):
    """Reads every account's balance."""

    def read_balances(self) -> Mapping[str, Decimal]:
        """Every account's balance."""


class LedgerAdmin(Protocol):
    """Resets the ledger between experiments."""

    def reset(self) -> None:
        """Zero every balance and forget every processed event."""


class ConsumerRunner(Protocol):
    """Runs the idempotent consumer as a separate process."""

    def run(self, group_id: str, kill_at: int | None, commit_every: int) -> int:
        """Run one idempotent consumer process to its end and return its exit code."""


class HealthProbe(Protocol):
    """A dependency check that reports a short detail or fails with ProbeFailedError."""

    name: str

    def check(self) -> str:
        """Return a short detail on success; raise `ProbeFailedError` when the dependency is unusable."""
