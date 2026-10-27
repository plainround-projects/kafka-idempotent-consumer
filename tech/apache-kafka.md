# Apache Kafka

## What it is

Kafka is a server that stores a stream of messages and lets many programs write to it and read from it. Think of a very long notebook that only grows at the end. Writers add lines at the bottom. Readers each keep their own bookmark and read forward at their own speed. Lines are not erased when read; they are deleted only when they get old.

Words you need:

- **Record** (message): one entry with an optional **key**, a **value** and a time.
- **Topic**: a named notebook. Here `payments` and `payments-dlq`.
- **Partition**: a topic is cut into numbered notebooks (0, 1, 2, ...). Order is guaranteed inside one partition only.
- **Offset**: the line number of a record inside one partition.
- **Broker**: one Kafka server process. This repository runs one.
- **Consumer group**: a team of consumers that share a topic; each partition goes to one member at a time. The group has a name (`balance-updater`).
- **Commit**: a consumer tells Kafka "my bookmark for this group is now offset N". On restart it continues from the bookmark.
- **Group coordinator**: the broker that tracks the members and bookmarks of a group.
- **Rebalance**: the coordinator re-deals partitions when a member joins, leaves or is judged dead.

## Why this repository uses it

Kafka delivers each record **at least once**: after a crash a consumer restarts from its last bookmark and sees some records again. The lesson shows what that does to a bank balance and how to make the result correct anyway. It also shows the other two classic consumer troubles: a record that can never be processed, and a consumer evicted from its group for being slow.

## Where it appears

| Path | What it does with Kafka |
| --- | --- |
| `docker-compose.yml` | Starts one broker (4.0.0, KRaft mode, no ZooKeeper). |
| `Makefile` (`topics`, `reset-topics`, `demo-*`) | Creates topics and reads them with Kafka's own tools. |
| `src/idempotent_consumer/infrastructure/kafka_consumer.py` | Reads payments and commits offsets. |
| `src/idempotent_consumer/infrastructure/kafka_producers.py` | Publishes payments and parks dead letters. |
| `src/idempotent_consumer/application/consume_payments.py` | Decides *when* to commit: only after the database commit. |

## How it works underneath

The producer sends a payment keyed by the account id, so one account's payments share a partition. The consumer asks for records starting at its bookmark. **At-least-once** comes from the order "process, then commit": if the program dies after processing but before committing, the bookmark is old and the record is delivered again. The opposite order (commit, then process) would lose records on a crash (**at-most-once**).

The consumer must call `poll` regularly. `max.poll.interval.ms` is the longest allowed gap between two polls. If processing a batch takes longer, the coordinator decides the consumer is stuck, removes it from the group and gives its partitions away (an **eviction**). The consumer's next commit is then refused. Separately, a heartbeat thread tells the coordinator the process is alive; a crashed process stops its heartbeats and is removed after `session.timeout.ms` (10 seconds for this client), which is why a restart after a crash waits a few seconds before it gets its partitions.

A **dead-letter topic** is an ordinary topic used as a parking place for records that cannot be processed, so the consumer can move on.

## Walkthrough of this repository's use

### The two topics

```make
# payments: six partitions, keyed by accountId. payments-dlq: one partition, kept 30 days for investigation.
topics:
	$(KAFKA_BIN)/kafka-topics.sh --create --if-not-exists --topic payments --partitions 6 \
	  --replication-factor 1 --bootstrap-server localhost:9092
	$(KAFKA_BIN)/kafka-topics.sh --create --if-not-exists --topic payments-dlq --partitions 1 \
	  --replication-factor 1 --config retention.ms=2592000000 --bootstrap-server localhost:9092
```

`payments` has six partitions. `payments-dlq` has one partition and keeps records for 30 days (`retention.ms=2592000000` milliseconds), long enough to investigate and replay. Replication factor 1 because there is one broker. `kafka-topics.sh` runs inside the container through `docker compose exec` (`tech/docker.md`).

### The broker

```yaml
  kafka:
    image: apache/kafka:4.0.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1"]
      interval: 5s
      timeout: 10s
      retries: 20
```

The `KAFKA_*` variables become the broker's settings (underscores become dots). `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092` is the address the broker tells clients to use next, so it must match the published port `9092:9092`. `KAFKA_PROCESS_ROLES: broker,controller` makes one process do both jobs (KRaft). `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0` skips the default three-second wait before a group's first rebalance.

### Reading without auto-commit

```python
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
```

`enable_auto_commit=False`: the consumer never commits by itself, so the program can commit exactly when it chooses. `auto_offset_reset="earliest"`: a new group starts at the oldest record. `max_poll_records` and `max_poll_interval_ms` are the two eviction knobs; the demo changes them.

### Commit after the database commit

```python
def _commit(committer: OffsetCommitter, pending: dict[tuple[str, int], int], counts: ConsumeCounts) -> bool:
    """Commit; False when the group refused (the member was evicted)."""
    try:
        committer.commit(dict(pending))
    except CommitRejectedError as error:
        counts.evictions += 1
        logger.warning(
            "EVICTED: offset commit rejected; rejoining on the next poll", extra={"fields": {"error": error}}
        )
        return False
    return True
```

`next_offsets` maps (topic, partition) to the offset of the *next* record to read, which is the processed record's offset plus one. If the group refuses the commit (the member was evicted), the adapter raises `CommitRejectedError`, the counter `evictions` goes up, and the batch is abandoned; the redelivered records will be skipped by the database guard.

### Parking poison

```python
def _apply(delivery: Delivery, ledger: BalanceLedger, dead_letters: DeadLetterSink) -> Outcome:
    try:
        return ledger.apply(parse_payment(delivery.value), delivery)
    except PoisonMessageError as error:
        dead_letters.send(DeadLetter(delivery.topic, delivery.partition, delivery.offset, str(error), delivery.value))
        logger.warning(
            "dead-lettered",
            extra={
                "fields": {
                    "topic": delivery.topic,
                    "partition": delivery.partition,
                    "offset": delivery.offset,
                    "error": error,
                }
            },
        )
        return Outcome.DEAD_LETTERED
```

A record that raises `PoisonMessageError` is wrapped with its origin (topic, partition, offset), the error and the raw bytes, sent to `payments-dlq`, and counted. Only then is its offset committed with the rest, so the consumer moves past it.

### Provoking the eviction

```make
# max.poll.interval.ms is lowered to 10 s so the eviction shows within seconds (the default is 5 minutes).
demo-evict: data migrate ## step 5: slow batches get the consumer evicted; smaller batches do not
	@echo "== max.poll.records=500: one batch is 50 s of work, longer than the 10 s poll interval"
	$(CONSUME) --mode idempotent --group evict-$$(date +%s) --delay 0.1 \
	  --max-poll-records 500 --max-poll-interval-ms 10000 --run-seconds 40 2>&1 | grep -E "EVICTED|poll timeout|done:" | cut -c1-200
	@echo "== max.poll.records=50: one batch is 5 s of work, inside the 10 s poll interval"
	$(CONSUME) --mode idempotent --group evict-fixed-$$(date +%s) --delay 0.1 \
	  --max-poll-records 50 --max-poll-interval-ms 10000 --run-seconds 40 2>&1 | grep -E "EVICTED|poll timeout|done:" | cut -c1-200
```

With 100 ms of work per record and 500 records per poll, one batch needs 50 seconds, but the allowed gap is 10 seconds. The consumer is evicted about every 12 seconds. With 50 records per poll a batch takes 5 seconds and no eviction happens.

## Try it yourself

(Start services with `make up`.)

1. `docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --describe --topic payments --bootstrap-server localhost:9092` shows `PartitionCount: 6`; the same for `payments-dlq` shows `PartitionCount: 1` and `retention.ms=2592000000`.
2. `make demo-poison`: the last table shows `LAG 0` on every partition, meaning the group read everything, including past the bad record.
3. `make demo-evict`: lines with `Consumer poll timeout has expired` and `EVICTED: offset commit rejected` appear for the 500-record run and not for the 50-record run.
4. `make demo-crash`: the consumer exits with code 1 midway; after the restart `MISMATCH acct-... (off by +...)` names the account that was counted twice.

## Mistakes people make

- Committing before the work is done (loses data on a crash) or never committing (re-reads everything).
- Assuming "exactly once" covers a database. Kafka's guarantees stop at Kafka; the database needs its own guard.
- Retrying a poison record forever instead of parking it.
- Taking a long time between polls and then wondering why the group evicts a healthy consumer. Fix by smaller batches, not by only raising the limit.

## Check yourself

1. Why does "process, then commit" give at-least-once? *A crash between the two leaves an old bookmark, so the record is delivered again.*
2. What is an eviction? *The coordinator removes a member that did not poll within `max.poll.interval.ms`; its commits are then refused.*
3. What does the dead-letter topic hold? *Records that can never be processed, with their origin and error.*
4. Why is a consumer's restart after a crash a few seconds late? *The coordinator keeps the dead member's partitions until its session timeout passes.*
5. What does `next_offsets` contain? *For each partition, the processed offset plus one.*

## Further reading

- Kafka 4.0 documentation: https://kafka.apache.org/40/documentation.html
- Consumer configuration (`max.poll.interval.ms`, `session.timeout.ms`): https://kafka.apache.org/40/documentation.html#consumerconfigs
- Delivery semantics: https://kafka.apache.org/40/documentation.html#semantics
