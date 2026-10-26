"""The payment event: how it is built, validated and compared. No I/O beyond reading JSON bytes."""

from __future__ import annotations

import json
import random
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from .errors import PoisonMessageError

ACCOUNTS: tuple[str, ...] = tuple(f"acct-{number:03d}" for number in range(1, 51))
CENTS = Decimal("0.01")


@dataclass(frozen=True, slots=True)
class Payment:
    """One payment into one account. `event_id` identifies the event, not its place in the log."""

    event_id: str
    account_id: str
    amount: Decimal

    def payload(self) -> dict[str, Any]:
        """The event as the producer sends it: two-decimal numbers in JSON."""
        return {"eventId": self.event_id, "accountId": self.account_id, "amount": float(self.amount)}


def build_payments(count: int, rng: random.Random, accounts: tuple[str, ...] = ACCOUNTS) -> list[Payment]:
    """`count` payments spread over the accounts, amounts from 1.00 to 500.00, ids drawn from `rng`."""
    return [
        Payment(
            event_id=str(uuid.UUID(int=rng.getrandbits(128), version=4)),
            account_id=rng.choice(accounts),
            amount=Decimal(rng.randint(100, 50_000)) * CENTS,
        )
        for _ in range(count)
    ]


def expected_totals(payments: Iterable[Payment], accounts: tuple[str, ...] = ACCOUNTS) -> dict[str, Decimal]:
    """What each balance must be, computed from the payments alone and never from the database."""
    totals = {account: Decimal("0.00") for account in accounts}
    for payment in payments:
        totals[payment.account_id] += payment.amount
    return totals


def parse_payment(raw: bytes) -> Payment:
    """Validate one record's value. Raises PoisonMessageError with the reason when it is unusable."""
    try:
        data = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as error:
        raise PoisonMessageError(f"not valid JSON: {error}") from error
    if not isinstance(data, dict):
        raise PoisonMessageError("payload is not a JSON object")
    try:
        event_id = str(uuid.UUID(str(data["eventId"])))
    except (KeyError, ValueError) as error:
        raise PoisonMessageError(f"eventId missing or not a uuid: {data.get('eventId')!r}") from error
    account_id = data.get("accountId")
    if not isinstance(account_id, str) or not account_id:
        raise PoisonMessageError(f"accountId missing or not text: {account_id!r}")
    return Payment(event_id, account_id, parse_amount(data.get("amount")))


def parse_amount(value: object) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise PoisonMessageError(f"amount is not a number: {value!r}")
    try:
        amount = Decimal(str(value))
    except InvalidOperation as error:
        raise PoisonMessageError(f"could not convert amount to a number: {value!r}") from error
    exponent = amount.as_tuple().exponent
    if not amount.is_finite() or amount <= 0 or (isinstance(exponent, int) and exponent < -2):
        raise PoisonMessageError(f"amount must be positive with at most two decimals: {value!r}")
    return amount


def poison_payload(rng: random.Random, account_id: str = "acct-012") -> dict[str, Any]:
    """The lesson's malformed event: the amount is text that is not a number."""
    return {
        "eventId": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "accountId": account_id,
        "amount": "not-a-number",
    }


def with_poison(payloads: list[dict[str, Any]], poison_count: int, rng: random.Random) -> list[dict[str, Any]]:
    """The payloads with `poison_count` malformed ones inserted at evenly spaced positions.

    Valid payments follow every malformed one, so the consumer has to move on to prove it can.
    """
    mixed = list(payloads)
    # Insert from the back so the positions computed for earlier ones stay valid.
    for number in reversed(range(poison_count)):
        mixed.insert(len(payloads) * (number + 1) // (poison_count + 1), poison_payload(rng))
    return mixed
