"""Use case: publish payments, optionally mixed with malformed events, and record the expected totals."""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass

from ..domain.payments import build_payments, expected_totals, with_poison
from .ports import ExpectedTotalsStore, PaymentPublisher


@dataclass(frozen=True, slots=True)
class ProducePlan:
    """How many payments to send and how many malformed events to mix among them."""

    payments: int = 10_000
    poison: int = 0

    def __post_init__(self) -> None:
        if self.payments < 0 or self.poison < 0:
            raise ValueError(f"invalid produce plan: {self}")


@dataclass(frozen=True, slots=True)
class ProduceResult:
    """What was sent, and whether the run was cut short by a stop request."""

    payments: int
    poison: int
    stopped: bool


def produce_payments(
    publisher: PaymentPublisher,
    store: ExpectedTotalsStore,
    plan: ProducePlan,
    rng: random.Random,
    stop: Callable[[], bool] = lambda: False,
) -> ProduceResult:
    """Publish the payments, then store the totals they must add up to.

    Malformed events are inserted among the payments and never counted in the totals. When `stop()`
    turns true (a shutdown signal) the run ends after flushing what was sent and the totals are not
    stored, since they would describe payments that were never sent.
    """
    payments = build_payments(plan.payments, rng)
    payloads = with_poison([payment.payload() for payment in payments], plan.poison, rng)
    sent = 0
    for payload in payloads:
        if stop():
            break
        publisher.publish(payload)
        sent += 1
    publisher.flush()
    stopped = sent < len(payloads)
    if payments and not stopped:
        store.save(expected_totals(payments))
    return ProduceResult(len(payments), plan.poison, stopped)
