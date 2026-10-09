from __future__ import annotations

import pytest

from eventstore import Repository
from eventstore.errors import ConcurrencyError
from eventstore.snapshots import InMemorySnapshotStore, Snapshot
from eventstore.stores import InMemoryEventStore
from tests.conftest import Tally


def repo(store: InMemoryEventStore, **kwargs: object) -> Repository[Tally]:
    return Repository(store, Tally, **kwargs)  # type: ignore[arg-type]


async def test_an_aggregate_nobody_wrote_loads_as_none(store: InMemoryEventStore) -> None:
    assert await repo(store).load("t-404") is None


async def test_a_round_trip_rebuilds_the_same_state(store: InMemoryEventStore) -> None:
    repository = repo(store)
    tally = Tally.start("t-1", label="march")
    tally.add(10)
    tally.add(-4)
    await repository.save(tally)

    loaded = await repository.load("t-1")
    assert loaded is not None
    assert (loaded.label, loaded.total, loaded.entries, loaded.version) == ("march", 6, 2, 3)
    assert loaded.pending == ()


async def test_saving_twice_appends_rather_than_rewrites(store: InMemoryEventStore) -> None:
    repository = repo(store)
    tally = Tally.start("t-1", label="march")
    await repository.save(tally)
    tally.add(5)
    written = await repository.save(tally)
    assert [r.version for r in written] == [2]
    assert len(store) == 2


async def test_saving_nothing_does_not_touch_the_store(store: InMemoryEventStore) -> None:
    repository = repo(store)
    tally = Tally.start("t-1", label="march")
    await repository.save(tally)
    assert await repository.save(tally) == []
    assert len(store) == 1


async def test_two_loads_of_one_aggregate_means_the_second_save_fails(
    store: InMemoryEventStore,
) -> None:
    repository = repo(store)
    await repository.save(Tally.start("t-1", label="march"))

    first = await repository.load("t-1")
    second = await repository.load("t-1")
    assert first is not None and second is not None

    first.add(10)
    await repository.save(first)

    second.add(999)
    with pytest.raises(ConcurrencyError) as caught:
        await repository.save(second)
    assert caught.value.expected_version == 1

    survivor = await repository.load("t-1")
    assert survivor is not None
    assert survivor.total == 10


async def test_a_lost_race_is_retried_against_the_newer_state(
    store: InMemoryEventStore,
) -> None:
    """The point of ConcurrencyError: the command is replayable, not lost."""
    repository = repo(store)
    await repository.save(Tally.start("t-1", label="march"))

    stale = await repository.load("t-1")
    winner = await repository.load("t-1")
    assert stale is not None and winner is not None

    winner.add(7)
    await repository.save(winner)

    stale.add(3)
    with pytest.raises(ConcurrencyError):
        await repository.save(stale)

    retried = await repository.load("t-1")
    assert retried is not None
    retried.add(3)
    await repository.save(retried)

    final = await repository.load("t-1")
    assert final is not None
    assert (final.total, final.version) == (10, 3)


async def test_an_aggregate_without_a_type_is_refused_at_construction(
    store: InMemoryEventStore,
) -> None:
    class Nameless(Tally):
        aggregate_type = ""

    with pytest.raises(ValueError, match="aggregate_type"):
        Repository(store, Nameless)


async def test_a_negative_snapshot_interval_is_refused(store: InMemoryEventStore) -> None:
    with pytest.raises(ValueError, match="negative"):
        Repository(store, Tally, snapshots=InMemorySnapshotStore(), snapshot_every=-1)


async def test_a_snapshot_from_another_aggregate_type_is_ignored(
    store: InMemoryEventStore, snapshots: InMemorySnapshotStore
) -> None:
    repository = repo(store, snapshots=snapshots, snapshot_every=1)
    tally = Tally.start("t-1", label="march")
    tally.add(4)
    await repository.save(tally)

    await snapshots.save(
        Snapshot(
            aggregate_id="t-1",
            aggregate_type="something.else",
            version=2,
            state={"label": "wrong", "total": 10_000, "entries": 99},
        )
    )
    loaded = await repository.load("t-1")
    assert loaded is not None
    assert loaded.total == 4
