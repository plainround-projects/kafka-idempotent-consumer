"""Adapter: connections to PostgreSQL, with a connect timeout and retries with backoff."""

from __future__ import annotations

import logging
from typing import Any

import psycopg

from .logging import fields
from .retry import retry
from .settings import PostgresSettings

logger = logging.getLogger(__name__)


def connect(settings: PostgresSettings, attempts: int = 5) -> psycopg.Connection[Any]:
    """Open a connection in autocommit mode, so `with connection.transaction()` is the only boundary."""

    def open_connection() -> psycopg.Connection[Any]:
        return psycopg.connect(
            host=settings.host,
            port=settings.port,
            dbname=settings.database,
            user=settings.user,
            password=settings.password,
            connect_timeout=settings.connect_timeout_seconds,
            autocommit=True,
        )

    return retry(
        open_connection,
        attempts=attempts,
        base_delay_seconds=0.5,
        retry_on=(psycopg.OperationalError,),
        on_retry=lambda attempt, error: logger.warning(
            "retrying database connection", extra=fields(attempt=attempt, error=error)
        ),
    )
