"""The Kafka adapters against the real broker: make up && make test-integration."""

import json
import time
import uuid

import pytest
from kafka import KafkaConsumer, TopicPartition

from idempotent_consumer.application.health import check_health
from idempotent_consumer.application.ports import CommitRejectedError, Delivery
from idempotent_consumer.domain.dead_letter import DeadLetter
from idempotent_consumer.infrastructure.health_probes import KafkaTopicsProbe, PostgresSchemaProbe
from idempotent_consumer.infrastructure.kafka_consumer import KafkaPaymentConsumer
from idempotent_consumer.infrastructure.kafka_producers import KafkaDeadLetterSink, KafkaPaymentPublisher
from idempotent_consumer.infrastructure.settings import KafkaSettings, Settings

pytestmark = pytest.mark.integration


def kafka_for(settings: Settings, topics: tuple[str, str]) -> KafkaSettings:
    return KafkaSettings(settings.kafka.bootstrap_servers, topics[0], topics[1], settings.kafka.group_id, 30_000, 100)


def read_until(consumer: KafkaPaymentConsumer, count: int, seconds: float = 30) -> list[Delivery]:
    deliveries: list[Delivery] = []
    deadline = time.time() + seconds
    while len(deliveries) < count and time.time() < deadline:
        deliveries.extend(consumer.poll(0.5))
    return deliveries


def test_payments_are_published_keyed_by_account_and_read_back(settings: Settings, topics: tuple[str, str]) -> None:
    kafka = kafka_for(settings, topics)
    publisher = KafkaPaymentPublisher(kafka)
    for number in range(30):
        publisher.publish({"eventId": str(uuid.uuid4()), "accountId": f"acct-{number % 3:03d}", "amount": 1.5})
    publisher.flush()
    publisher.close()

    consumer = KafkaPaymentConsumer(kafka, group_id=f"read-{uuid.uuid4().hex}")
    deliveries = read_until(consumer, 30)
    consumer.close()

    assert len(deliveries) == 30
    partitions_by_account: dict[bytes | None, set[int]] = {}
    for delivery in deliveries:
        partitions_by_account.setdefault(delivery.key, set()).add(delivery.partition)
    assert all(len(partitions) == 1 for partitions in partitions_by_account.values())  # one account, one partition


def test_a_committed_offset_survives_the_member_and_the_next_member_resumes_after_it(
    settings: Settings, topics: tuple[str, str]
) -> None:
    kafka, group = kafka_for(settings, topics), f"resume-{uuid.uuid4().hex}"
    publisher = KafkaPaymentPublisher(kafka)
    for number in range(10):
        publisher.publish({"eventId": str(uuid.uuid4()), "accountId": "acct-001", "amount": number + 1})
    publisher.flush()
    publisher.close()

    first = KafkaPaymentConsumer(kafka, group_id=group)
    deliveries = read_until(first, 10)
    third = deliveries[2]
    first.commit({(third.topic, third.partition): third.offset + 1})  # commit through the third record only
    first.close()

    second = KafkaPaymentConsumer(kafka, group_id=group)
    resumed = read_until(second, 7)
    second.close()
    assert [d.offset for d in resumed] == [d.offset for d in deliveries[3:]]


def test_a_commit_after_the_poll_interval_expired_is_rejected(settings: Settings, topics: tuple[str, str]) -> None:
    kafka = kafka_for(settings, topics)
    publisher = KafkaPaymentPublisher(kafka)
    publisher.publish({"eventId": str(uuid.uuid4()), "accountId": "acct-001", "amount": 1})
    publisher.flush()
    publisher.close()

    consumer = KafkaPaymentConsumer(kafka, group_id=f"slow-{uuid.uuid4().hex}", max_poll_interval_ms=2000)
    [delivery] = read_until(consumer, 1)
    time.sleep(6)  # no poll for longer than max.poll.interval.ms: the group evicts this member
    try:
        with pytest.raises(CommitRejectedError):
            consumer.commit({(delivery.topic, delivery.partition): delivery.offset + 1})
    finally:
        consumer.close()


def test_a_dead_letter_is_stored_with_its_origin_before_send_returns(
    settings: Settings, topics: tuple[str, str]
) -> None:
    kafka = kafka_for(settings, topics)
    sink = KafkaDeadLetterSink(kafka)
    sink.send(DeadLetter(topics[0], 3, 5512, "bad amount", b'{"amount": "not-a-number"}'))
    sink.close()

    reader = KafkaConsumer(
        topics[1], bootstrap_servers=kafka.bootstrap_servers, auto_offset_reset="earliest", consumer_timeout_ms=10_000
    )
    letters = [json.loads(record.value) for record in reader]
    reader.close()
    assert len(letters) == 1
    assert (letters[0]["originalTopic"], letters[0]["originalPartition"], letters[0]["originalOffset"]) == (
        topics[0],
        3,
        5512,
    )
    assert TopicPartition(topics[1], 0) is not None


def test_the_health_probes_pass_against_the_running_services(settings: Settings, topics: tuple[str, str]) -> None:
    kafka = kafka_for(settings, topics)
    report = check_health([KafkaTopicsProbe(kafka), PostgresSchemaProbe(settings.postgres)])
    assert report.healthy, report
