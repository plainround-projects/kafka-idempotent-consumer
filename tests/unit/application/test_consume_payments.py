import json
from decimal import Decimal

import pytest

from idempotent_consumer.application.consume_payments import ConsumePlan, consume_payments
from idempotent_consumer.application.ports import Delivery
from tests.unit.application.fakes import (
    CollectingDeadLetters,
    FakeClock,
    InMemoryLedger,
    RecordingCommitter,
    ScriptedSource,
)


def delivery(
    offset: int, event_id: str, amount: object = 10, account: str = "acct-001", partition: int = 0
) -> Delivery:
    value = json.dumps({"eventId": event_id, "accountId": account, "amount": amount}).encode()
    return Delivery("payments", partition, offset, account.encode(), value)


def uid(number: int) -> str:
    return f"00000000-0000-4000-8000-{number:012d}"


def consume(batches, plan=None, *, ledger=None, committer=None, after_apply=None, clock=None, assigned=True):
    ledger = ledger or InMemoryLedger(["acct-001", "acct-002"])
    committer = committer or RecordingCommitter()
    dead_letters = CollectingDeadLetters()
    clock = clock or FakeClock()
    counts = consume_payments(
        ScriptedSource(batches, assigned),
        committer,
        ledger,
        dead_letters,
        plan or ConsumePlan(idle_timeout_seconds=3),
        stop=lambda: False,
        clock=clock,
        sleep=clock.sleep,
        after_apply=after_apply or (lambda _processed: None),
    )
    return counts, ledger, committer, dead_letters, clock


def test_each_payment_is_applied_and_its_offset_committed_after_it():
    counts, ledger, committer, _, _ = consume([[delivery(0, uid(1), 10), delivery(1, uid(2), 5)]])
    assert counts.summary() == {"applied": 2, "duplicate": 0, "dead-lettered": 0, "evictions": 0}
    assert ledger.balances["acct-001"] == Decimal(15)
    assert committer.commits == [{("payments", 0): 1}, {("payments", 0): 2}]


def test_a_redelivered_event_is_skipped_and_changes_nothing():
    counts, ledger, _, _, _ = consume([[delivery(0, uid(1)), delivery(0, uid(1))]])
    assert (counts.applied, counts.duplicate) == (1, 1)
    assert ledger.balances["acct-001"] == Decimal(10)


def test_without_the_guard_a_redelivery_counts_twice():
    ledger = InMemoryLedger(["acct-001"], idempotent=False)
    counts, ledger, _, _, _ = consume([[delivery(0, uid(1)), delivery(0, uid(1))]], ledger=ledger)
    assert counts.applied == 2
    assert ledger.balances["acct-001"] == Decimal(20)


def test_a_poison_record_is_dead_lettered_with_its_origin_and_processing_continues():
    batch = [delivery(0, uid(1)), delivery(1, uid(2), amount="not-a-number", partition=2), delivery(2, uid(3))]
    counts, ledger, committer, dead_letters, _ = consume([batch])
    assert counts.summary() == {"applied": 2, "duplicate": 0, "dead-lettered": 1, "evictions": 0}
    assert ledger.balances["acct-001"] == Decimal(20)
    [letter] = dead_letters.letters
    assert (letter.original_topic, letter.original_partition, letter.original_offset) == ("payments", 2, 1)
    assert "not-a-number" in letter.raw.decode()
    assert committer.commits[-1] == {("payments", 0): 3}  # the offset moved past the bad record


def test_a_payment_for_an_unknown_account_is_dead_lettered():
    counts, _, _, dead_letters, _ = consume([[delivery(0, uid(1), account="acct-999")]])
    assert counts.dead_lettered == 1
    assert "unknown account" in dead_letters.letters[0].error


def test_offsets_commit_every_n_records_and_the_rest_at_the_end_of_the_batch():
    batch = [delivery(offset, uid(offset)) for offset in range(5)]
    _, _, committer, _, _ = consume([batch], ConsumePlan(commit_every=2, idle_timeout_seconds=3))
    assert committer.commits == [{("payments", 0): 2}, {("payments", 0): 4}, {("payments", 0): 5}]


def test_the_kill_hook_runs_after_the_apply_and_before_that_records_commit():
    seen: list[tuple[int, int]] = []
    committer = RecordingCommitter()

    def hook(processed: int) -> None:
        seen.append((processed, len(committer.commits)))

    consume([[delivery(0, uid(1)), delivery(1, uid(2))]], committer=committer, after_apply=hook)
    assert seen == [(1, 0), (2, 1)]  # record 1 was applied while no commit had happened yet


def test_a_rejected_commit_counts_an_eviction_and_abandons_the_rest_of_the_batch():
    batch = [delivery(0, uid(1)), delivery(1, uid(2)), delivery(2, uid(3))]
    counts, ledger, committer, _, _ = consume(
        [batch, [delivery(1, uid(2))]], committer=RecordingCommitter(reject_on_call=1)
    )
    # record 1 applied, its commit rejected, records 2 and 3 not touched; the redelivery of 2 is applied next poll
    assert counts.evictions == 1
    assert ledger.balances["acct-001"] == Decimal(20)
    assert committer.commits == [{("payments", 0): 2}]


def test_the_work_delay_sleeps_once_per_record():
    _, _, _, _, clock = consume(
        [[delivery(0, uid(1)), delivery(1, uid(2))]], ConsumePlan(work_delay_seconds=0.1, idle_timeout_seconds=3)
    )
    assert clock.slept == [0.1, 0.1]


def test_it_stops_after_the_idle_timeout_once_the_group_has_assigned_partitions():
    _, _, _, _, clock = consume([], ConsumePlan(idle_timeout_seconds=2.5))
    assert clock.now > 2.5


def test_the_idle_clock_does_not_run_while_the_member_has_no_assignment():
    clock = FakeClock()
    consume([], ConsumePlan(idle_timeout_seconds=2.5, run_seconds=20), assigned=False, clock=clock)
    assert clock.now > 20  # no idle exit: only the overall run limit ended the loop


def test_it_stops_after_the_run_time_even_with_records_flowing():
    batches = [[delivery(offset, uid(offset))] for offset in range(100)]
    counts, _, _, _, _ = consume(batches, ConsumePlan(run_seconds=5))
    assert 0 < counts.applied < 100


def test_it_returns_when_asked_to_stop():
    clock = FakeClock()
    polls = iter([False, False, True])
    counts = consume_payments(
        ScriptedSource([[delivery(0, uid(1))]]),
        RecordingCommitter(),
        InMemoryLedger(["acct-001"]),
        CollectingDeadLetters(),
        ConsumePlan(),
        stop=lambda: next(polls),
        clock=clock,
        sleep=clock.sleep,
    )
    assert counts.applied == 1


@pytest.mark.parametrize("kwargs", [{"commit_every": 0}, {"work_delay_seconds": -1}, {"poll_seconds": 0}])
def test_an_invalid_plan_is_rejected(kwargs):
    with pytest.raises(ValueError, match="invalid consume plan"):
        ConsumePlan(**kwargs)
