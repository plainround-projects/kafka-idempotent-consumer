"""Adapters: the balance ledger, its reader and its admin, as plain SQL on PostgreSQL."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Any

import psycopg

from ..application.ports import Delivery, Outcome
from ..domain.errors import PoisonMessageError
from ..domain.payments import Payment

ADD_TO_BALANCE = "UPDATE balances SET total = total + %s, updated_at = now() WHERE account_id = %s"
RECORD_EVENT = (
    'INSERT INTO processed_events (event_id, topic, partition, "offset") VALUES (%s, %s, %s, %s) '
    "ON CONFLICT (event_id) DO NOTHING"
)


def _add_to_balance(cursor: psycopg.Cursor[Any], payment: Payment) -> None:
    cursor.execute(ADD_TO_BALANCE, (payment.amount, payment.account_id))
    if cursor.rowcount == 0:
        raise PoisonMessageError(f"unknown account: {payment.account_id}")


class NaiveLedger:
    """Lesson steps 1 and 2: add the amount, with no memory of which events were seen."""

    def __init__(self, connection: psycopg.Connection[Any]) -> None:
        self._connection = connection

    def apply(self, payment: Payment, delivery: Delivery) -> Outcome:
        with self._connection.transaction(), self._connection.cursor() as cursor:
            _add_to_balance(cursor, payment)
        return Outcome.APPLIED


class IdempotentLedger:
    """Lesson step 3: record the event id and add the amount in one transaction.

    A second delivery of the same event id inserts nothing, so the amount is not added again.
    """

    def __init__(self, connection: psycopg.Connection[Any]) -> None:
        self._connection = connection

    def apply(self, payment: Payment, delivery: Delivery) -> Outcome:
        with self._connection.transaction(), self._connection.cursor() as cursor:
            cursor.execute(RECORD_EVENT, (payment.event_id, delivery.topic, delivery.partition, delivery.offset))
            if cursor.rowcount == 0:
                return Outcome.DUPLICATE
            _add_to_balance(cursor, payment)
        return Outcome.APPLIED


class PostgresBalanceReader:
    def __init__(self, connection: psycopg.Connection[Any]) -> None:
        self._connection = connection

    def read_balances(self) -> Mapping[str, Decimal]:
        rows = self._connection.execute("SELECT account_id, total FROM balances").fetchall()
        return {str(account): Decimal(total) for account, total in rows}


class PostgresLedgerAdmin:
    def __init__(self, connection: psycopg.Connection[Any]) -> None:
        self._connection = connection

    def reset(self) -> None:
        with self._connection.transaction():
            self._connection.execute("UPDATE balances SET total = 0, updated_at = now()")
            self._connection.execute("TRUNCATE processed_events")
