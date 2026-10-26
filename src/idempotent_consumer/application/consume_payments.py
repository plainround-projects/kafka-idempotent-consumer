"""Use case: keep balances correct from a stream of payments that may arrive more than once."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from ..domain.dead_letter import DeadLetter
from ..domain.errors import PoisonMessageError
from ..domain.payments import parse_payment
from .ports import (
    BalanceLedger,
    CommitRejectedError,
    DeadLetterSink,
    Delivery,
    OffsetCommitter,
    Outcome,
    PaymentSource,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ConsumePlan:
    """How the consumer runs: commit interval, artificial work, and when to stop."""

    commit_every: int = 1  # 1 commits after every record, the lesson's shape
    work_delay_seconds: float = 0.0  # artificial work per record
    idle_timeout_seconds: float | None = None  # stop after this long without a record (after assignment)
    run_seconds: float | None = None  # stop after this long in total
    poll_seconds: float = 1.0

    def __post_init__(self) -> None:
        if self.commit_every < 1 or self.work_delay_seconds < 0 or self.poll_seconds <= 0:
            raise ValueError(f"invalid consume plan: {self}")


@dataclass
class ConsumeCounts:
    """What the consumer did: payments applied, skipped as duplicates, dead-lettered, and evictions."""

    applied: int = 0
    duplicate: int = 0
    dead_lettered: int = 0
    evictions: int = 0
    processed: int = field(default=0, repr=False)

    def summary(self) -> dict[str, int]:
        return {
            "applied": self.applied,
            "duplicate": self.duplicate,
            "dead-lettered": self.dead_lettered,
            "evictions": self.evictions,
        }


def consume_payments(
    source: PaymentSource,
    committer: OffsetCommitter,
    ledger: BalanceLedger,
    dead_letters: DeadLetterSink,
    plan: ConsumePlan,
    *,
    stop: Callable[[], bool],
    clock: Callable[[], float],
    sleep: Callable[[float], None],
    after_apply: Callable[[int], None] = lambda _processed: None,
) -> ConsumeCounts:
    """Poll, apply each payment in one transaction, and commit offsets only after the database commit.

    `after_apply(n)` runs after the n-th record's database commit and before its offset commit, which
    is where the crash test injects its kill switch.
    """
    counts = ConsumeCounts()
    started = last_record_at = clock()
    while not stop():
        deliveries = source.poll(plan.poll_seconds)
        if deliveries or not source.has_assignment():
            # The idle clock only runs once the group has given this member partitions: after a crash
            # the coordinator holds the dead member's partitions until its session expires.
            last_record_at = clock()
        _handle_batch(deliveries, committer, ledger, dead_letters, plan, counts, sleep, after_apply)
        if plan.run_seconds is not None and clock() - started > plan.run_seconds:
            break
        if (
            plan.idle_timeout_seconds is not None
            and not deliveries
            and clock() - last_record_at > plan.idle_timeout_seconds
        ):
            break
    return counts


def _handle_batch(
    deliveries: Sequence[Delivery],
    committer: OffsetCommitter,
    ledger: BalanceLedger,
    dead_letters: DeadLetterSink,
    plan: ConsumePlan,
    counts: ConsumeCounts,
    sleep: Callable[[float], None],
    after_apply: Callable[[int], None],
) -> None:
    pending: dict[tuple[str, int], int] = {}
    since_commit = 0
    for delivery in deliveries:
        outcome = _apply(delivery, ledger, dead_letters)
        _count(counts, outcome)
        after_apply(counts.processed)
        pending[(delivery.topic, delivery.partition)] = delivery.offset + 1
        since_commit += 1
        if plan.work_delay_seconds:
            sleep(plan.work_delay_seconds)
        if since_commit >= plan.commit_every:
            if not _commit(committer, pending, counts):
                return  # the rest of this batch belongs to whoever owns the partitions now
            pending, since_commit = {}, 0
    if pending:
        _commit(committer, pending, counts)


def _apply(delivery: Delivery, ledger: BalanceLedger, dead_letters: DeadLetterSink) -> Outcome:
    try:
        return ledger.apply(parse_payment(delivery.value), delivery)
    except PoisonMessageError as error:
        dead_letters.send(DeadLetter(delivery.topic, delivery.partition, delivery.offset, str(error), delivery.value))
        logger.warning(
            "dead-lettered",
            extra={
                "fields": {
                    "topic": delivery.topic,
                    "partition": delivery.partition,
                    "offset": delivery.offset,
                    "error": error,
                }
            },
        )
        return Outcome.DEAD_LETTERED


def _count(counts: ConsumeCounts, outcome: Outcome) -> None:
    counts.processed += 1
    if outcome is Outcome.APPLIED:
        counts.applied += 1
    elif outcome is Outcome.DUPLICATE:
        counts.duplicate += 1
    else:
        counts.dead_lettered += 1


def _commit(committer: OffsetCommitter, pending: dict[tuple[str, int], int], counts: ConsumeCounts) -> bool:
    """Commit; False when the group refused (the member was evicted)."""
    try:
        committer.commit(dict(pending))
    except CommitRejectedError as error:
        counts.evictions += 1
        logger.warning(
            "EVICTED: offset commit rejected; rejoining on the next poll", extra={"fields": {"error": error}}
        )
        return False
    return True
