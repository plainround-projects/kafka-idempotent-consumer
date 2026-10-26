"""Publish payments (and, on request, malformed events) to the `payments` topic.

python -m idempotent_consumer.interfaces.produce_cli            # 10,000 payments, writes expected_totals.json
python -m idempotent_consumer.interfaces.produce_cli --payments 0 --poison 1
"""

from __future__ import annotations

import argparse
import random
from collections.abc import Sequence
from pathlib import Path

from ..application.produce_payments import ProducePlan, produce_payments
from ..infrastructure.expected_file import JsonExpectedStore
from ..infrastructure.kafka_producers import KafkaPaymentPublisher
from ..infrastructure.shutdown import ShutdownSignal
from .arguments import non_negative_int
from .common import load_settings


def build_parser() -> argparse.ArgumentParser:
    """The argument parser of the producer program."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--payments", type=non_negative_int, default=10_000)
    parser.add_argument(
        "--poison", type=non_negative_int, default=0, help="malformed events, spread evenly among the payments"
    )
    parser.add_argument("--seed", type=int, default=None, help="fixed seed for a repeatable run")
    parser.add_argument("--topic", default=None, help="defaults to $TOPIC")
    parser.add_argument("--expected", type=Path, default=Path("expected_totals.json"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Publish payments and write the expected totals; returns the process exit code."""
    args = build_parser().parse_args(argv)
    settings = load_settings()
    if settings is None:
        return 2
    shutdown = ShutdownSignal()
    shutdown.install()
    publisher = KafkaPaymentPublisher(settings.kafka, args.topic)
    try:
        result = produce_payments(
            publisher,
            JsonExpectedStore(args.expected),
            ProducePlan(args.payments, args.poison),
            random.Random(args.seed),
            stop=lambda: shutdown.requested,
        )
    finally:
        publisher.close()
    if result.payments and not result.stopped:
        print(f"wrote {args.expected} (the expected total of each of the 50 accounts)")
    note = " (stopped early)" if result.stopped else ""
    topic = args.topic or settings.kafka.topic
    print(f"produced {result.payments} payments and {result.poison} malformed events to {topic}{note}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
