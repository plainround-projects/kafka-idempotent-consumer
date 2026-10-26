import base64
from decimal import Decimal

import pytest

from idempotent_consumer.domain.balances import Mismatch, diff_balances
from idempotent_consumer.domain.crash_plan import kill_points
from idempotent_consumer.domain.dead_letter import DeadLetter


def test_dead_letter_keeps_the_origin_the_error_and_the_exact_bytes():
    original = b'{"eventId": "x", "amount": "not-a-number"}\xff'
    payload = DeadLetter("payments", 3, 5512, "bad amount", original).payload()
    assert (payload["originalTopic"], payload["originalPartition"], payload["originalOffset"]) == ("payments", 3, 5512)
    assert payload["error"] == "bad amount"
    assert base64.b64decode(payload["rawBase64"]) == original  # replayable even though \xff is not text
    assert payload["raw"].startswith('{"eventId"')


def test_equal_balances_have_no_mismatch():
    assert diff_balances({"a": Decimal("1.50")}, {"a": Decimal("1.5")}) == []


def test_a_double_applied_payment_shows_as_off_by_exactly_its_amount():
    mismatches = diff_balances({"acct-017": Decimal("4821.33")}, {"acct-017": Decimal("4903.81")})
    assert mismatches == [Mismatch("acct-017", Decimal("4821.33"), Decimal("4903.81"))]
    assert mismatches[0].off_by == Decimal("82.48")
    assert str(mismatches[0]) == "MISMATCH acct-017: expected 4821.33, got 4903.81 (off by +82.48)"


def test_an_account_missing_on_one_side_counts_as_zero():
    assert diff_balances({"a": Decimal(2)}, {}) == [Mismatch("a", Decimal(2), Decimal(0))]
    assert diff_balances({}, {"b": Decimal(3)}) == [Mismatch("b", Decimal(0), Decimal(3))]


def test_twenty_different_kill_points_inside_the_topic():
    points = kill_points(20, 10_000)
    assert len(set(points)) == 20
    assert all(1 <= point <= 10_000 for point in points)
    assert points == sorted(points)


def test_kill_points_are_the_same_every_time():
    assert kill_points(5, 3_000) == kill_points(5, 3_000)


def test_a_tiny_topic_still_gets_valid_kill_points():
    assert all(1 <= point <= 50 for point in kill_points(5, 50))


@pytest.mark.parametrize(("runs", "payments"), [(0, 10), (3, 0)])
def test_kill_points_need_at_least_one_run_and_one_payment(runs, payments):
    with pytest.raises(ValueError, match="at least 1"):
        kill_points(runs, payments)
