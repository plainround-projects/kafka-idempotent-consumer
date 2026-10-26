import json
import random
from decimal import Decimal

import pytest

from idempotent_consumer.application.produce_payments import ProducePlan, produce_payments
from idempotent_consumer.domain.errors import PoisonMessageError
from idempotent_consumer.domain.payments import parse_payment


class RecordingPublisher:
    def __init__(self):
        self.payloads = []
        self.flushed = 0

    def publish(self, payload):
        self.payloads.append(payload)

    def flush(self):
        self.flushed += 1


class RecordingStore:
    def __init__(self):
        self.saved = None

    def save(self, totals):
        self.saved = totals


def run(plan, stop=lambda: False):
    publisher, store = RecordingPublisher(), RecordingStore()
    result = produce_payments(publisher, store, plan, random.Random(1), stop)
    return result, publisher, store


def test_payments_are_published_and_the_expected_totals_are_stored():
    result, publisher, store = run(ProducePlan(payments=100))
    assert (result.payments, result.poison, result.stopped) == (100, 0, False)
    assert len(publisher.payloads) == 100
    assert publisher.flushed == 1
    assert sum(store.saved.values()) == sum(Decimal(str(payload["amount"])) for payload in publisher.payloads)


def test_malformed_events_are_mixed_in_and_not_counted_in_the_expected_totals():
    result, publisher, store = run(ProducePlan(payments=50, poison=2))
    assert len(publisher.payloads) == 52
    assert result.poison == 2
    bad = 0
    for payload in publisher.payloads:
        try:
            parse_payment(json.dumps(payload).encode())
        except PoisonMessageError:
            bad += 1
    assert bad == 2
    good = sum(Decimal(str(p["amount"])) for p in publisher.payloads if p["amount"] != "not-a-number")
    assert sum(store.saved.values()) == good


def test_a_poison_only_run_stores_no_expected_totals():
    _, _, store = run(ProducePlan(payments=0, poison=1))
    assert store.saved is None


def test_a_stop_request_flushes_what_was_sent_and_stores_no_totals():
    stops = iter([False, False, True])
    result, publisher, store = run(ProducePlan(payments=10), stop=lambda: next(stops))
    assert result.stopped
    assert len(publisher.payloads) == 2
    assert publisher.flushed == 1
    assert store.saved is None


@pytest.mark.parametrize("kwargs", [{"payments": -1}, {"poison": -1}])
def test_an_invalid_plan_is_rejected(kwargs):
    with pytest.raises(ValueError, match="invalid produce plan"):
        ProducePlan(**kwargs)
