"""The balance updater: reads payments and keeps the `balances` table up to date.

python -m idempotent_consumer.interfaces.consume_cli --mode naive --kill-at 4731
python -m idempotent_consumer.interfaces.consume_cli --mode idempotent --until-idle 5
"""

from __future__ import annotations

import argparse
import os
import time
from collections.abc import Callable, Sequence

from ..application.consume_payments import ConsumePlan, consume_payments
from ..infrastructure import postgres
from ..infrastructure.kafka_consumer import KafkaPaymentConsumer
from ..infrastructure.kafka_producers import KafkaDeadLetterSink
from ..infrastructure.postgres_ledger import IdempotentLedger, NaiveLedger
from ..infrastructure.shutdown import ShutdownSignal
from .arguments import non_negative_float, non_negative_int, positive_int
from .common import load_settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--mode", choices=["naive", "idempotent"], default="idempotent")
    parser.add_argument("--group", default=None, help="defaults to $GROUP_ID")
    parser.add_argument("--topic", default=None, help="defaults to $TOPIC")
    parser.add_argument("--dlq-topic", default=None, help="defaults to $DLQ_TOPIC")
    parser.add_argument("--name", default="balance-updater", help="client id and label in the output")
    parser.add_argument(
        "--kill-at",
        type=non_negative_int,
        default=0,
        help="crash (os._exit) after the DB commit of this record, before its offset commit; 0 for never",
    )
    parser.add_argument("--commit-every", type=positive_int, default=1, help="commit offsets after this many records")
    parser.add_argument("--delay", type=non_negative_float, default=0.0, help="artificial seconds of work per record")
    parser.add_argument("--max-poll-records", type=positive_int, default=500)
    parser.add_argument("--max-poll-interval-ms", type=positive_int, default=300_000)
    parser.add_argument(
        "--until-idle", type=non_negative_float, default=None, help="exit after this many seconds with no records"
    )
    parser.add_argument(
        "--run-seconds", type=non_negative_float, default=None, help="exit after this many seconds in total"
    )
    return parser


def kill_switch(kill_at: int, name: str) -> Callable[[int], None]:
    """Crash like a killed process: no cleanup, no offset commit, no group leave."""

    def after_apply(processed: int) -> None:
        if kill_at and processed == kill_at:
            print(
                f"[{name}] kill switch: crashing after the database commit of record {processed}, "
                "before its offset commit",
                flush=True,
            )
            os._exit(1)

    return after_apply


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = load_settings()
    if settings is None:
        return 2
    shutdown = ShutdownSignal()
    shutdown.install()

    connection = postgres.connect(settings.postgres)
    ledger = NaiveLedger(connection) if args.mode == "naive" else IdempotentLedger(connection)
    dead_letters = KafkaDeadLetterSink(settings.kafka, args.dlq_topic)
    consumer = KafkaPaymentConsumer(
        settings.kafka,
        group_id=args.group,
        topic=args.topic,
        client_id=args.name,
        max_poll_records=args.max_poll_records,
        max_poll_interval_ms=args.max_poll_interval_ms,
    )
    plan = ConsumePlan(
        commit_every=args.commit_every,
        work_delay_seconds=args.delay,
        idle_timeout_seconds=args.until_idle,
        run_seconds=args.run_seconds,
    )
    try:
        counts = consume_payments(
            consumer, consumer, ledger, dead_letters, plan,
            stop=lambda: shutdown.requested, clock=time.time, sleep=time.sleep,
            after_apply=kill_switch(args.kill_at, args.name),
        )  # fmt: skip
    finally:
        consumer.close()
        dead_letters.close()
        connection.close()
    print(f"[{args.name}] done: {counts.summary()}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
