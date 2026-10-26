"""Configuration from the environment, validated once at startup (twelve-factor). Secrets only from here."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field

_SERVERS = re.compile(r"^[\w.\-]+:\d{1,5}(,[\w.\-]+:\d{1,5})*$")
_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR"}


class ConfigurationError(ValueError):
    """An environment variable is missing or has an unusable value."""


def _text(environ: Mapping[str, str], name: str, default: str | None = None) -> str:
    value = environ.get(name, default if default is not None else "").strip()
    if not value:
        raise ConfigurationError(f"{name} must be set and not empty")
    return value


def _positive_int(environ: Mapping[str, str], name: str, default: int) -> int:
    raw = environ.get(name, str(default)).strip()
    if not raw.isdigit() or int(raw) < 1:
        raise ConfigurationError(f"{name} must be a positive integer, got {raw!r}")
    return int(raw)


@dataclass(frozen=True, slots=True)
class KafkaSettings:
    """The Kafka part of the configuration."""

    bootstrap_servers: str
    topic: str
    dlq_topic: str
    group_id: str
    request_timeout_ms: int
    retry_backoff_ms: int


@dataclass(frozen=True, slots=True)
class PostgresSettings:
    """The PostgreSQL part of the configuration; the password is never printed."""

    host: str
    port: int
    database: str
    user: str
    password: str = field(repr=False)  # never printed or logged
    connect_timeout_seconds: int


@dataclass(frozen=True, slots=True)
class Settings:
    """Everything the programs read from the environment. See .env.example for each variable."""

    kafka: KafkaSettings
    postgres: PostgresSettings
    log_level: str

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> Settings:
        env = os.environ if environ is None else environ
        servers = _text(env, "BOOTSTRAP_SERVERS", "localhost:9092")
        if not _SERVERS.match(servers):
            raise ConfigurationError(f"BOOTSTRAP_SERVERS must be host:port[,host:port], got {servers!r}")
        level = _text(env, "LOG_LEVEL", "INFO").upper()
        if level not in _LEVELS:
            raise ConfigurationError(f"LOG_LEVEL must be one of {sorted(_LEVELS)}, got {level!r}")
        return cls(
            kafka=KafkaSettings(
                bootstrap_servers=servers,
                topic=_text(env, "TOPIC", "payments"),
                dlq_topic=_text(env, "DLQ_TOPIC", "payments-dlq"),
                group_id=_text(env, "GROUP_ID", "balance-updater"),
                request_timeout_ms=_positive_int(env, "KAFKA_REQUEST_TIMEOUT_MS", 30_000),
                retry_backoff_ms=_positive_int(env, "KAFKA_RETRY_BACKOFF_MS", 100),
            ),
            postgres=PostgresSettings(
                host=_text(env, "POSTGRES_HOST", "localhost"),
                port=_positive_int(env, "POSTGRES_PORT", 5433),
                database=_text(env, "POSTGRES_DB", "bank"),
                user=_text(env, "POSTGRES_USER", "bank"),
                password=_text(env, "POSTGRES_PASSWORD"),
                connect_timeout_seconds=_positive_int(env, "POSTGRES_CONNECT_TIMEOUT_SECONDS", 5),
            ),
            log_level=level,
        )
