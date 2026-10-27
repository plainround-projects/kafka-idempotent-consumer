# kafka-python

## What it is

`kafka-python` is a Python **library**: a bundle of ready-made code you import. It speaks Kafka's network protocol, so Python code can write records to Kafka and read them back without handling sockets and byte layouts. It is pure Python (no compiled parts). This repository pins version 3.0.12.

## Why this repository uses it

The lesson's starting code uses it. It provides a producer, a consumer that joins a group and commits offsets **by hand**, and the offset and eviction behavior the lesson studies.

## Where it appears

| Path | What it does with the library |
| --- | --- |
| `requirements.lock`, `pyproject.toml` | Pin `kafka-python==3.0.12`. |
| `src/idempotent_consumer/infrastructure/kafka_consumer.py` | `KafkaConsumer`: reads payments, commits offsets. |
| `src/idempotent_consumer/infrastructure/kafka_producers.py` | `KafkaProducer`: publishes payments and dead letters. |
| `src/idempotent_consumer/infrastructure/health_probes.py` | `KafkaConsumer` to ask which partitions a topic has. |
| `tests/conftest.py`, `tests/integration/` | `KafkaAdminClient` to create and delete test topics. |

Only `infrastructure` imports it; import-linter forbids `domain` and `application` from importing `kafka` (`tech/import-linter.md`).

## How it works underneath

Your program holds TCP connections to the brokers. `producer.send(...)` puts a record in a buffer and returns a *future* (a promise of a result); a background thread sends batches. `future.get(timeout=...)` waits for the broker's answer. `acks="all"` makes the broker answer only after all in-sync copies have the record.

`consumer.poll(timeout_ms)` returns the records that arrived, as a dictionary from partition to list of records. A background thread sends **heartbeats** so the coordinator knows the process lives. If the program goes too long between `poll` calls (`max_poll_interval_ms`), the heartbeat thread leaves the group, and the next `commit` fails with `CommitFailedError`.

## Walkthrough of this repository's use

### The consumer

```python
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
```

- `enable_auto_commit=False`: nothing is committed unless the program asks. The use case asks after the database commit.
- `max_poll_records` and `max_poll_interval_ms`: the number of records one `poll` may return, and the longest allowed gap between polls. The eviction demo changes them.
- `poll` flattens the dictionary of batches into one list of plain `Delivery` objects, so the rest of the program never sees library types.
- `commit` builds `{TopicPartition(topic, partition): OffsetAndMetadata(offset, "", -1)}` and calls `self._consumer.commit(offsets)`. `OffsetAndMetadata(offset, metadata, leader_epoch)` carries the offset of the next record to read; `""` and `-1` mean "no extra text, epoch unknown".
- Any `KafkaError` from the commit becomes the project's own `CommitRejectedError`, so the use case needs no Kafka import.

### The producers

```python
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
```

- `_producer` builds a `KafkaProducer` with `acks="all"` and the configured timeout and retry pause.
- `KafkaPaymentPublisher.publish` sends the JSON bytes with the **account id as the key**, so one account's payments share a partition.
- `KafkaDeadLetterSink.send` calls `.get(timeout=...)` on the future: it **waits** for the broker's acknowledgment. The dead-letter record must be safely stored before the offset of the bad record is committed.

## Try it yourself

1. `.venv/bin/python -c "import kafka; print(kafka.__version__)"` prints `3.0.12`.
2. `make up`, then `make demo-poison`: the dead-letter record appears in the output of the console consumer with `originalTopic`, `originalPartition`, `originalOffset`.
3. `make demo-evict`: the `Consumer poll timeout has expired` line comes from the library's heartbeat thread; the `EVICTED` line comes from this repository's reaction to the `CommitFailedError`.

## Mistakes people make

- Assuming `send` has delivered the record. Only the future's result or `flush` says so.
- Using auto-commit when the work has side effects elsewhere: the offset may be committed before the database write.
- Not polling often enough, which gets the consumer evicted.
- Catching the library's errors all over the code. Here one adapter translates them.

## Check yourself

1. Why is auto-commit off? *So the offset is committed only after the database commit.*
2. What does `.get(timeout=30)` do on a send? *Waits up to 30 seconds for the broker to acknowledge.*
3. What does `OffsetAndMetadata` hold here? *The offset of the next record to read.*
4. Which error does an evicted consumer get on commit? *`CommitFailedError`, translated here to `CommitRejectedError`.*
5. Why do both producers use `acks="all"`? *To wait for all in-sync copies before treating a record as stored.*

## Further reading

- kafka-python 3.0.12: https://pypi.org/project/kafka-python/3.0.12/
- Documentation: https://kafka-python.readthedocs.io/
- Consumer settings in Kafka: https://kafka.apache.org/40/documentation.html#consumerconfigs
