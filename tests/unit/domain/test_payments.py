import json
import random
from decimal import Decimal

import pytest

from idempotent_consumer.domain.errors import PoisonMessageError
from idempotent_consumer.domain.payments import (
    ACCOUNTS,
    Payment,
    build_payments,
    expected_totals,
    parse_payment,
    poison_payload,
    with_poison,
)

VALID = {"eventId": "6f1c2b6e-3d7e-4c55-9d52-0d6a4b7a1e11", "accountId": "acct-012", "amount": 120.5}


def raw(**changes: object) -> bytes:
    return json.dumps({**VALID, **changes}).encode()


def test_a_valid_payment_parses_with_an_exact_decimal_amount():
    payment = parse_payment(raw())
    assert payment == Payment(VALID["eventId"], "acct-012", Decimal("120.5"))  # type: ignore[arg-type]


@pytest.mark.parametrize("amount", ["not-a-number", None, True, [], "NaN", "Infinity", 0, -5, 1.234, "12.345"])
def test_a_bad_amount_is_poison(amount):
    with pytest.raises(PoisonMessageError):
        parse_payment(raw(amount=amount))


@pytest.mark.parametrize("value", [b"{broken", b"\xff\xfe", b"[1, 2]", b'"text"', b"null"])
def test_unreadable_or_non_object_payloads_are_poison(value):
    with pytest.raises(PoisonMessageError):
        parse_payment(value)


@pytest.mark.parametrize("changes", [{"eventId": "nope"}, {"eventId": None}, {"accountId": ""}, {"accountId": 7}])
def test_bad_ids_are_poison(changes):
    with pytest.raises(PoisonMessageError):
        parse_payment(raw(**changes))


def test_a_missing_field_is_poison():
    value = {key: item for key, item in VALID.items() if key != "eventId"}
    with pytest.raises(PoisonMessageError):
        parse_payment(json.dumps(value).encode())


def test_the_lessons_poison_event_is_rejected_with_the_reason():
    with pytest.raises(PoisonMessageError, match="not-a-number"):
        parse_payment(json.dumps(poison_payload(random.Random(1))).encode())


def test_payments_are_repeatable_for_a_seed_and_have_unique_ids():
    first = build_payments(500, random.Random(3))
    assert first == build_payments(500, random.Random(3))
    assert len({payment.event_id for payment in first}) == 500
    assert all(payment.account_id in ACCOUNTS and Decimal(1) <= payment.amount <= Decimal(500) for payment in first)


def test_every_generated_payment_survives_a_json_round_trip_and_validation():
    for payment in build_payments(200, random.Random(8)):
        assert parse_payment(json.dumps(payment.payload()).encode()) == payment


def test_expected_totals_add_amounts_exactly_per_account():
    payments = [
        Payment("1", "acct-001", Decimal("0.1")),
        Payment("2", "acct-001", Decimal("0.2")),
        Payment("3", "acct-002", Decimal("5.0")),
    ]
    totals = expected_totals(payments)
    assert totals["acct-001"] == Decimal("0.3")  # with floats this sum is 0.30000000000000004
    assert totals["acct-002"] == Decimal("5.0")
    assert totals["acct-003"] == Decimal(0)
    assert len(totals) == 50


def test_with_poison_spreads_malformed_events_among_the_payments():
    payloads = [payment.payload() for payment in build_payments(10, random.Random(1))]
    mixed = with_poison(payloads, 1, random.Random(2))
    assert len(mixed) == 11
    assert [item for item in mixed if item["amount"] != "not-a-number"] == payloads
    position = next(index for index, item in enumerate(mixed) if item["amount"] == "not-a-number")
    assert 0 < position < len(mixed) - 1  # valid payments before and after it


def test_with_poison_zero_changes_nothing():
    payloads = [payment.payload() for payment in build_payments(5, random.Random(1))]
    assert with_poison(payloads, 0, random.Random(2)) == payloads


def test_payments_are_immutable():
    payment = Payment("1", "acct-001", Decimal(1))
    with pytest.raises(AttributeError):
        payment.amount = Decimal(2)  # type: ignore[misc]
