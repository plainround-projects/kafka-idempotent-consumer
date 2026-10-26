import pytest

from idempotent_consumer.infrastructure.settings import ConfigurationError, Settings

MINIMAL = {"POSTGRES_PASSWORD": "s3cret-value"}


def test_defaults_describe_the_local_compose_setup():
    settings = Settings.from_env(MINIMAL)
    assert settings.kafka.bootstrap_servers == "localhost:9092"
    assert (settings.kafka.topic, settings.kafka.dlq_topic, settings.kafka.group_id) == (
        "payments",
        "payments-dlq",
        "balance-updater",
    )
    assert (settings.postgres.host, settings.postgres.port, settings.postgres.database, settings.postgres.user) == (
        "localhost",
        5433,
        "bank",
        "bank",
    )
    assert settings.log_level == "INFO"


def test_the_database_password_has_no_default():
    with pytest.raises(ConfigurationError, match="POSTGRES_PASSWORD"):
        Settings.from_env({})


def test_the_password_never_appears_in_a_printed_settings_object():
    assert "s3cret-value" not in repr(Settings.from_env(MINIMAL))


def test_every_variable_can_be_overridden():
    settings = Settings.from_env(
        {
            **MINIMAL,
            "BOOTSTRAP_SERVERS": "a:1,b:2",
            "TOPIC": "t",
            "DLQ_TOPIC": "d",
            "GROUP_ID": "g",
            "POSTGRES_HOST": "db",
            "POSTGRES_PORT": "6543",
            "POSTGRES_DB": "x",
            "POSTGRES_USER": "u",
            "LOG_LEVEL": "debug",
            "KAFKA_REQUEST_TIMEOUT_MS": "5000",
            "KAFKA_RETRY_BACKOFF_MS": "50",
            "POSTGRES_CONNECT_TIMEOUT_SECONDS": "2",
        }
    )
    assert (settings.kafka.topic, settings.kafka.dlq_topic, settings.kafka.group_id) == ("t", "d", "g")
    assert (settings.postgres.host, settings.postgres.port, settings.postgres.database) == ("db", 6543, "x")
    assert (settings.postgres.user, settings.postgres.password) == ("u", "s3cret-value")
    assert (settings.log_level, settings.kafka.request_timeout_ms, settings.postgres.connect_timeout_seconds) == (
        "DEBUG",
        5000,
        2,
    )


@pytest.mark.parametrize(
    "extra",
    [
        {"BOOTSTRAP_SERVERS": "no-port"},
        {"TOPIC": "  "},
        {"POSTGRES_PORT": "0"},
        {"POSTGRES_PORT": "five"},
        {"LOG_LEVEL": "LOUD"},
        {"KAFKA_REQUEST_TIMEOUT_MS": "-1"},
    ],
)
def test_an_unusable_value_is_rejected_with_the_variable_name(extra):
    with pytest.raises(ConfigurationError, match=next(iter(extra))):
        Settings.from_env({**MINIMAL, **extra})


def test_settings_read_the_process_environment_by_default(monkeypatch):
    monkeypatch.setenv("POSTGRES_PASSWORD", "from-the-process")
    assert Settings.from_env().postgres.password == "from-the-process"
