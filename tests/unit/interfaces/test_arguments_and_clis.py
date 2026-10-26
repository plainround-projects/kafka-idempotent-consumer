import argparse

import pytest

from idempotent_consumer.interfaces import check_cli, consume_cli, crash_test_cli, health_cli, produce_cli
from idempotent_consumer.interfaces.arguments import non_negative_float, non_negative_int, positive_int


def test_validators_accept_valid_numbers():
    assert positive_int("3") == 3
    assert non_negative_int("0") == 0
    assert non_negative_float("0.5") == 0.5


@pytest.mark.parametrize(
    ("validator", "text"), [(positive_int, "0"), (non_negative_int, "-1"), (non_negative_float, "-0.1")]
)
def test_validators_reject_out_of_range_numbers(validator, text):
    with pytest.raises(argparse.ArgumentTypeError):
        validator(text)


def test_the_producer_defaults_match_the_lessons_run():
    args = produce_cli.build_parser().parse_args([])
    assert (args.payments, args.poison) == (10_000, 0)


def test_the_consumer_defaults_are_the_idempotent_per_record_commit():
    args = consume_cli.build_parser().parse_args([])
    assert (args.mode, args.commit_every, args.kill_at, args.max_poll_records) == ("idempotent", 1, 0, 500)
    assert args.max_poll_interval_ms == 300_000


def test_the_consumer_rejects_an_unknown_mode():
    with pytest.raises(SystemExit):
        consume_cli.build_parser().parse_args(["--mode", "maybe"])


def test_a_missing_database_password_stops_every_program_with_exit_code_two(monkeypatch, capsys):
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    assert produce_cli.main([]) == 2
    assert consume_cli.main([]) == 2
    assert check_cli.main([]) == 2
    assert crash_test_cli.main([]) == 2
    assert health_cli.main([]) == 2
    assert "POSTGRES_PASSWORD" in capsys.readouterr().err
