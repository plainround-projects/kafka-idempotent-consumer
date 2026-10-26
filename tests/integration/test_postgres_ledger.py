"""The ledger adapters against the real PostgreSQL: make up && make test-integration."""

from decimal import Decimal
from typing import Any

import psycopg
import pytest

from idempotent_consumer.application.ports import Delivery, Outcome
from idempotent_consumer.domain.errors import PoisonMessageError
from idempotent_consumer.domain.payments import Payment
from idempotent_consumer.infrastructure.postgres_ledger import (
    IdempotentLedger,
    NaiveLedger,
    PostgresBalanceReader,
    PostgresLedgerAdmin,
)

pytestmark = pytest.mark.integration

PAYMENT = Payment("6f1c2b6e-3d7e-4c55-9d52-0d6a4b7a1e11", "acct-017", Decimal("82.48"))
DELIVERY = Delivery("payments", 3, 5512, b"acct-017", b"{}")


def balance(connection: psycopg.Connection[Any], account: str = "acct-017") -> Decimal:
    return PostgresBalanceReader(connection).read_balances()[account]


def test_the_seed_has_fifty_accounts_at_zero(connection: psycopg.Connection[Any]) -> None:
    balances = PostgresBalanceReader(connection).read_balances()
    assert len(balances) == 50
    assert set(balances.values()) == {Decimal(0)}


def test_the_naive_ledger_counts_a_redelivered_payment_twice(connection: psycopg.Connection[Any]) -> None:
    ledger = NaiveLedger(connection)
    assert ledger.apply(PAYMENT, DELIVERY) is Outcome.APPLIED
    assert ledger.apply(PAYMENT, DELIVERY) is Outcome.APPLIED
    assert balance(connection) == Decimal("164.96")


def test_the_idempotent_ledger_skips_a_redelivered_payment(connection: psycopg.Connection[Any]) -> None:
    ledger = IdempotentLedger(connection)
    assert ledger.apply(PAYMENT, DELIVERY) is Outcome.APPLIED
    assert ledger.apply(PAYMENT, DELIVERY) is Outcome.DUPLICATE
    assert balance(connection) == Decimal("82.48")
    assert connection.execute("SELECT count(*) FROM processed_events").fetchone() == (1,)


def test_the_event_id_is_recorded_with_where_it_was_first_read(connection: psycopg.Connection[Any]) -> None:
    IdempotentLedger(connection).apply(PAYMENT, DELIVERY)
    row = connection.execute('SELECT topic, partition, "offset" FROM processed_events').fetchone()
    assert row == ("payments", 3, 5512)


def test_a_payment_for_an_unknown_account_is_poison_and_leaves_no_trace(connection: psycopg.Connection[Any]) -> None:
    stranger = Payment(PAYMENT.event_id, "acct-999", Decimal(5))
    with pytest.raises(PoisonMessageError, match="unknown account"):
        IdempotentLedger(connection).apply(stranger, DELIVERY)
    # the event id insert was rolled back with the failed update, so a later fix can still apply it
    assert connection.execute("SELECT count(*) FROM processed_events").fetchone() == (0,)


def test_reset_zeroes_the_balances_and_forgets_the_events(connection: psycopg.Connection[Any]) -> None:
    IdempotentLedger(connection).apply(PAYMENT, DELIVERY)
    PostgresLedgerAdmin(connection).reset()
    assert balance(connection) == Decimal(0)
    assert connection.execute("SELECT count(*) FROM processed_events").fetchone() == (0,)
