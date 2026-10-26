"""In-memory stand-ins for the application's ports."""

from collections.abc import Mapping, Sequence
from decimal import Decimal

from idempotent_consumer.application.ports import CommitRejectedError, Delivery, Outcome
from idempotent_consumer.domain.dead_letter import DeadLetter
from idempotent_consumer.domain.errors import PoisonMessageError
from idempotent_consumer.domain.payments import Payment


class ScriptedSource:
    """Returns the scripted batches one per poll, then empty polls forever."""

    def __init__(self, batches: Sequence[Sequence[Delivery]], assigned: bool = True) -> None:
        self._batches = list(batches)
        self._assigned = assigned

    def poll(self, timeout_seconds: float) -> Sequence[Delivery]:
        return self._batches.pop(0) if self._batches else []

    def has_assignment(self) -> bool:
        return self._assigned

    def close(self) -> None:
        pass


class RecordingCommitter:
    def __init__(self, reject_on_call: int | None = None) -> None:
        self.commits: list[dict[tuple[str, int], int]] = []
        self._calls = 0
        self._reject_on_call = reject_on_call

    def commit(self, next_offsets: Mapping[tuple[str, int], int]) -> None:
        self._calls += 1
        if self._calls == self._reject_on_call:
            raise CommitRejectedError("CommitFailedError")
        self.commits.append(dict(next_offsets))


class InMemoryLedger:
    """A ledger with the idempotent semantics: a seen event id changes nothing; unknown accounts are poison."""

    def __init__(self, accounts: Sequence[str], *, idempotent: bool = True) -> None:
        self.balances = {account: Decimal(0) for account in accounts}
        self.seen: set[str] = set()
        self._idempotent = idempotent

    def apply(self, payment: Payment, delivery: Delivery) -> Outcome:
        if payment.account_id not in self.balances:
            raise PoisonMessageError(f"unknown account: {payment.account_id}")
        if self._idempotent:
            if payment.event_id in self.seen:
                return Outcome.DUPLICATE
            self.seen.add(payment.event_id)
        self.balances[payment.account_id] += payment.amount
        return Outcome.APPLIED


class CollectingDeadLetters:
    def __init__(self) -> None:
        self.letters: list[DeadLetter] = []

    def send(self, letter: DeadLetter) -> None:
        self.letters.append(letter)


class FakeClock:
    """Every reading advances time by `step`; sleeping is recorded, not performed."""

    def __init__(self, step: float = 1.0) -> None:
        self.now = 0.0
        self.step = step
        self.slept: list[float] = []

    def __call__(self) -> float:
        self.now += self.step
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
