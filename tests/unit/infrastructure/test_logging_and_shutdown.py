import io
import logging
import signal

from idempotent_consumer.infrastructure.logging import configure_logging, fields
from idempotent_consumer.infrastructure.shutdown import ShutdownSignal


def test_a_record_becomes_one_logfmt_line_with_its_fields():
    stream = io.StringIO()
    configure_logging("INFO", stream)
    logging.getLogger("demo").info("partitions assigned", extra=fields(member="consumer-1", partitions=[0, 1]))
    line = stream.getvalue().strip()
    assert line.startswith("ts=")
    assert "level=INFO" in line
    assert "logger=demo" in line
    assert 'msg="partitions assigned"' in line
    assert "member=consumer-1" in line
    assert 'partitions="[0, 1]"' in line


def test_records_below_the_level_are_dropped_and_kafka_stays_at_warning():
    stream = io.StringIO()
    configure_logging("INFO", stream)
    logging.getLogger("demo").debug("hidden")
    assert stream.getvalue() == ""
    assert logging.getLogger("kafka").level == logging.WARNING


def test_an_exception_is_attached_to_the_line():
    stream = io.StringIO()
    configure_logging("INFO", stream)
    try:
        raise ValueError("boom")
    except ValueError:
        logging.getLogger("demo").exception("failed")
    assert "exc=" in stream.getvalue()
    assert "boom" in stream.getvalue()


def test_configuring_twice_does_not_duplicate_lines():
    stream = io.StringIO()
    configure_logging("INFO", stream)
    configure_logging("INFO", stream)
    logging.getLogger("demo").info("once")
    assert stream.getvalue().count("msg=once") == 1


def test_a_shutdown_signal_starts_unset_and_is_set_by_request():
    shutdown = ShutdownSignal()
    assert not shutdown.requested
    shutdown.request()
    assert shutdown.requested


def test_sigterm_sets_the_flag_once_installed():
    shutdown = ShutdownSignal()
    previous = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
    try:
        shutdown.install()
        signal.raise_signal(signal.SIGTERM)
        assert shutdown.requested
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)
