"""Publish payments (and, on request, malformed events) to the `payments` topic.

python -m idempotent_consumer.interfaces.produce_cli            # 10,000 payments, writes expected_totals.json
python -m idempotent_consumer.interfaces.produce_cli --payments 0 --poison 1
"""

from __future__ import annotations

import argparse
import random
from collections.abc import Sequence
from pathlib import Path

from ..domain.payments import build_payments, expected_totals, with_poison
from ..infrastructure.expected_file import save_expected
from ..infrastructure.kafka_producers import KafkaPaymentPublisher
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
    rng = random.Random(args.seed)
    payments = build_payments(args.payments, rng)
    payloads = with_poison([payment.payload() for payment in payments], args.poison, rng)
    publisher = KafkaPaymentPublisher(settings.kafka, args.topic)
    try:
        for payload in payloads:
            publisher.publish(payload)
        publisher.flush()
    finally:
        publisher.close()
    if payments:
        save_expected(args.expected, expected_totals(payments))
        print(f"wrote {args.expected} (the expected total of each of the 50 accounts)")
    print(
        f"produced {len(payments)} payments and {args.poison} malformed events to {args.topic or settings.kafka.topic}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
