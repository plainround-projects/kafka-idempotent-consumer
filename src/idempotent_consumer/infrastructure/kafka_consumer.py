"""Adapter: read payments as a group member and commit offsets with kafka-python."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata

from ..application.ports import CommitRejectedError, Delivery
from .settings import KafkaSettings


class KafkaPaymentConsumer:
    """Implements `PaymentSource` and `OffsetCommitter`.

    Auto-commit is off: the use case commits an offset only after the database transaction for that
    record committed. `max_poll_records` and `max_poll_interval_ms` are the eviction knobs of step 5.
    """

    def __init__(
        self,
        settings: KafkaSettings,
        *,
        group_id: str | None = None,
        topic: str | None = None,
        client_id: str = "balance-updater",
        max_poll_records: int = 500,
        max_poll_interval_ms: int = 300_000,
    ) -> None:
        self._consumer: Any = KafkaConsumer(
            topic or settings.topic,
            bootstrap_servers=settings.bootstrap_servers,
            group_id=group_id or settings.group_id,
            client_id=client_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=max_poll_records,
            max_poll_interval_ms=max_poll_interval_ms,
            request_timeout_ms=settings.request_timeout_ms,
            retry_backoff_ms=settings.retry_backoff_ms,
        )

    def poll(self, timeout_seconds: float) -> Sequence[Delivery]:
        batches = self._consumer.poll(timeout_ms=int(timeout_seconds * 1000))
        return [
            Delivery(record.topic, record.partition, record.offset, record.key, record.value)
            for batch in batches.values()
            for record in batch
        ]

    def has_assignment(self) -> bool:
        return bool(self._consumer.assignment())

    def commit(self, next_offsets: Mapping[tuple[str, int], int]) -> None:
        offsets = {
            TopicPartition(topic, partition): OffsetAndMetadata(offset, "", -1)
            for (topic, partition), offset in next_offsets.items()
        }
        try:
            self._consumer.commit(offsets)
        except KafkaError as error:
            raise CommitRejectedError(type(error).__name__) from error

    def close(self) -> None:
        self._consumer.close()
