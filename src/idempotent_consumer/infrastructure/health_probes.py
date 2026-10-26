"""Adapters: is Kafka reachable with the payments topics, and is PostgreSQL reachable with the schema?"""

from __future__ import annotations

from typing import Any

import psycopg
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from ..application.health import ProbeFailedError
from .retry import retry
from .settings import KafkaSettings, PostgresSettings


class KafkaTopicsProbe:
    """Health probe: the broker answers and the payments topics exist."""

    name = "kafka"

    def __init__(self, settings: KafkaSettings) -> None:
        self._settings = settings

    def check(self) -> str:
        try:
            missing = retry(self._missing_topics, attempts=3, base_delay_seconds=0.2, retry_on=(KafkaError,))
        except KafkaError as error:
            raise ProbeFailedError(f"cannot reach {self._settings.bootstrap_servers}: {error}") from error
        if missing:
            raise ProbeFailedError(f"missing topics: {', '.join(missing)}")
        return f"topics {self._settings.topic} and {self._settings.dlq_topic} exist"

    def _missing_topics(self) -> list[str]:
        consumer: Any = KafkaConsumer(
            bootstrap_servers=self._settings.bootstrap_servers,
            enable_auto_commit=False,
            request_timeout_ms=self._settings.request_timeout_ms,
        )
        try:
            return [t for t in (self._settings.topic, self._settings.dlq_topic) if not consumer.partitions_for_topic(t)]
        finally:
            consumer.close(autocommit=False)


class PostgresSchemaProbe:
    """Health probe: PostgreSQL answers and the balances table exists."""

    name = "postgres"

    def __init__(self, settings: PostgresSettings) -> None:
        self._settings = settings

    def check(self) -> str:
        try:
            with psycopg.connect(
                host=self._settings.host,
                port=self._settings.port,
                dbname=self._settings.database,
                user=self._settings.user,
                password=self._settings.password,
                connect_timeout=self._settings.connect_timeout_seconds,
            ) as connection:
                row = connection.execute("SELECT to_regclass('balances') IS NOT NULL").fetchone()
        except psycopg.Error as error:
            raise ProbeFailedError(f"cannot query {self._settings.host}:{self._settings.port}: {error}") from error
        if not row or not row[0]:
            raise ProbeFailedError("table balances does not exist (run make db)")
        return "table balances exists"
