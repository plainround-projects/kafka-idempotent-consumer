"""Health command: exit 0 when Kafka has the topics and PostgreSQL has the balances table.

python -m idempotent_consumer.interfaces.health_cli
"""

from __future__ import annotations

from collections.abc import Sequence

from ..application.health import check_health
from ..infrastructure.health_probes import KafkaTopicsProbe, PostgresSchemaProbe
from .common import load_settings


def main(_argv: Sequence[str] | None = None) -> int:
    settings = load_settings()
    if settings is None:
        return 2
    report = check_health([KafkaTopicsProbe(settings.kafka), PostgresSchemaProbe(settings.postgres)])
    for result in report.results:
        print(f"{'ok' if result.ok else 'FAIL'} {result.name}: {result.detail}")
    return 0 if report.healthy else 1


if __name__ == "__main__":
    raise SystemExit(main())
