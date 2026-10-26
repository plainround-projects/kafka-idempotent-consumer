"""Adapters: publish payments and park dead letters with kafka-python."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from kafka import KafkaProducer

from ..domain.dead_letter import DeadLetter
from .settings import KafkaSettings


def _producer(settings: KafkaSettings) -> Any:
    return KafkaProducer(
        bootstrap_servers=settings.bootstrap_servers,
        acks="all",
        request_timeout_ms=settings.request_timeout_ms,
        retry_backoff_ms=settings.retry_backoff_ms,
    )


class KafkaPaymentPublisher:
    """Publishes payment payloads keyed by account id, so one account's payments share a partition."""

    def __init__(self, settings: KafkaSettings, topic: str | None = None) -> None:
        self._topic = topic or settings.topic
        self._producer = _producer(settings)

    def publish(self, payload: Mapping[str, Any]) -> None:
        self._producer.send(self._topic, key=str(payload["accountId"]).encode(), value=json.dumps(payload).encode())

    def flush(self) -> None:
        self._producer.flush(timeout=60)

    def close(self) -> None:
        self._producer.close(timeout=60)


class KafkaDeadLetterSink:
    """Implements `DeadLetterSink`: waits for the broker's acknowledgment before returning."""

    def __init__(self, settings: KafkaSettings, topic: str | None = None, timeout_seconds: float = 30.0) -> None:
        self._topic = topic or settings.dlq_topic
        self._timeout = timeout_seconds
        self._producer = _producer(settings)

    def send(self, letter: DeadLetter) -> None:
        self._producer.send(self._topic, value=json.dumps(letter.payload()).encode()).get(timeout=self._timeout)

    def close(self) -> None:
        self._producer.close(timeout=self._timeout)
