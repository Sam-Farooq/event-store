from __future__ import annotations

import pytest

from eventstore.aggregate import Aggregate
from eventstore.errors import EventStoreError, ReplayGap
from eventstore.events import Event, Record, utcnow
from tests.conftest import AmountAdded, Tally


def record_of(version: int, amount: int, aggregate_id: str = "t-1") -> Record:
    return Record(
        aggregate_id=aggregate_id,
        aggregate_type="tally",
        version=version,
        event_type="tally.added",
        payload={"amount": amount},
        recorded_at=utcnow(),
        position=f"{version:012d}",
    )


def test_a_command_applies_and_queues_in_one_step() -> None:
    tally = Tally.start("t-1", label="march")
    tally.add(5)
    assert tally.total == 5
    assert tally.version == 2
    assert tally.committed_version == 0
    assert [type(e).__name__ for e in tally.pending] == ["TallyStarted", "AmountAdded"]


def test_a_refused_command_leaves_no_trace() -> None:
    tally = Tally.start("t-1", label="march")
    tally.mark_committed()
    with pytest.raises(ValueError, match="no-op"):
        tally.add(0)
    assert tally.pending == ()
    assert tally.version == 1


def test_replay_folds_the_stream_and_tracks_the_version() -> None:
    tally = Tally("t-1")
    tally.replay([record_of(1, 10), record_of(2, 5), record_of(3, -3)])
    assert tally.total == 12
    assert tally.entries == 3
    assert tally.version == 3
    assert tally.pending == ()


def test_a_gap_in_the_stream_stops_the_load() -> None:
    tally = Tally("t-1")
    with pytest.raises(ReplayGap) as caught:
        tally.replay([record_of(1, 10), record_of(3, 5)])
    assert caught.value.expected_version == 2
    assert caught.value.found_version == 3


def test_replay_refuses_to_interleave_with_uncommitted_events() -> None:
    tally = Tally.start("t-1", label="march")
    with pytest.raises(EventStoreError, match="uncommitted"):
        tally.replay([record_of(1, 10)])


def test_an_aggregate_that_snapshots_without_restoring_says_so() -> None:
    class Lopsided(Aggregate):
        aggregate_type = "lopsided"

        def apply(self, occurrence: Event) -> None:
            return None

        def snapshot(self) -> dict[str, object]:
            return {"anything": 1}

    with pytest.raises(NotImplementedError, match="restore"):
        Lopsided("x").restore({"anything": 1})


def test_a_foreign_event_is_a_bug_not_a_migration() -> None:
    class Tagged(Event):
        pass

    with pytest.raises(TypeError, match="Tagged"):
        Tally("t-1").apply(Tagged())
    assert AmountAdded(amount=1).amount == 1
