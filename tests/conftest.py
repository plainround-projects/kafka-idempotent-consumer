"""Fixtures for the tests that need Kafka and PostgreSQL from docker-compose.yml."""

import pathlib
import uuid
from collections.abc import Iterator
from typing import Any

import psycopg
import pytest
from kafka import KafkaAdminClient
from kafka.admin import NewTopic

from idempotent_consumer.infrastructure import postgres
from idempotent_consumer.infrastructure.settings import Settings

SQL = pathlib.Path(__file__).resolve().parent.parent / "sql"


@pytest.fixture
def settings() -> Settings:
    """From the environment (make loads .env; CI sets the variables); the password has no default."""
    return Settings.from_env()


@pytest.fixture
def connection(settings: Settings) -> Iterator[psycopg.Connection[Any]]:
    """The balances and processed_events tables, created if missing and emptied for this test."""
    with postgres.connect(settings.postgres) as db:
        for script in [SQL / "001_schema.sql", SQL / "002_seed.sql", *sorted((SQL / "migrations").glob("*.sql"))]:
            db.execute(script.read_text())
        db.execute((SQL / "reset.sql").read_text())
        yield db


@pytest.fixture
def topics(settings: Settings) -> Iterator[tuple[str, str]]:
    """A fresh payments topic and dead-letter topic with their own names, removed afterwards."""
    suffix = uuid.uuid4().hex[:8]
    names = (f"test-payments-{suffix}", f"test-payments-dlq-{suffix}")
    admin = KafkaAdminClient(bootstrap_servers=settings.kafka.bootstrap_servers)
    admin.create_topics(
        [
            NewTopic(names[0], num_partitions=6, replication_factor=1),
            NewTopic(names[1], num_partitions=1, replication_factor=1),
        ]
    )
    yield names
    admin.delete_topics(list(names))
    admin.close()
