"""Snapshots exist so a long stream does not get slower every day."""

from __future__ import annotations

from eventstore import Repository
from eventstore.snapshots import InMemorySnapshotStore, Snapshot
from eventstore.stores import InMemoryEventStore
from tests.conftest import CountingStore, Tally

INTERVAL = 50
EVENTS = 512


async def build(repository: Repository[Tally], count: int) -> None:
    tally = Tally.start("t-1", label="march")
    await repository.save(tally)
    for _ in range(count - 1):
        tally.add(1)
        await repository.save(tally)


async def test_no_snapshot_store_means_every_load_is_a_full_replay() -> None:
    store = CountingStore()
    repository: Repository[Tally] = Repository(store, Tally)
    await build(repository, 60)
    store.read_sizes.clear()
    loaded = await repository.load("t-1")
    assert loaded is not None and loaded.version == 60
    assert store.read_sizes == [60]


async def test_a_long_stream_loads_from_one_snapshot_plus_the_tail() -> None:
    store = CountingStore()
    snapshots = InMemorySnapshotStore()
    repository: Repository[Tally] = Repository(
        store, Tally, snapshots=snapshots, snapshot_every=INTERVAL
    )
    await build(repository, EVENTS)

    assert snapshots.writes == EVENTS // INTERVAL == 10
    latest = await snapshots.load("t-1")
    assert latest is not None and latest.version == 500

    store.read_sizes.clear()
    loaded = await repository.load("t-1")
    assert loaded is not None
    assert loaded.version == EVENTS
    assert loaded.total == EVENTS - 1
    assert store.read_sizes == [EVENTS - 500], "the load must read the tail, not the stream"


async def test_a_batch_that_jumps_several_intervals_writes_one_snapshot() -> None:
    store = InMemoryEventStore()
    snapshots = InMemorySnapshotStore()
    repository: Repository[Tally] = Repository(
        store, Tally, snapshots=snapshots, snapshot_every=INTERVAL
    )
    tally = Tally.start("t-1", label="march")
    for _ in range(139):
        tally.add(1)
    await repository.save(tally)

    assert snapshots.writes == 1
    latest = await snapshots.load("t-1")
    assert latest is not None and latest.version == 140


async def test_an_aggregate_that_opts_out_of_snapshots_gets_none() -> None:
    class Unsnapshotted(Tally):
        aggregate_type = "tally.unsnapshotted"

        def snapshot(self) -> None:
            return None

    store = InMemoryEventStore()
    snapshots = InMemorySnapshotStore()
    repository: Repository[Unsnapshotted] = Repository(
        store, Unsnapshotted, snapshots=snapshots, snapshot_every=1
    )
    aggregate = Unsnapshotted.start("t-1", label="march")
    await repository.save(aggregate)
    assert snapshots.writes == 0
    assert await snapshots.load("t-1") is None


async def test_an_older_snapshot_cannot_overwrite_a_newer_one() -> None:
    snapshots = InMemorySnapshotStore()
    newer = Snapshot(
        aggregate_id="t-1", aggregate_type="tally", version=9, state={"total": 9}
    )
    older = Snapshot(
        aggregate_id="t-1", aggregate_type="tally", version=4, state={"total": 4}
    )
    assert await snapshots.save(newer) is True
    assert await snapshots.save(older) is False
    assert await snapshots.save(newer) is False
    stored = await snapshots.load("t-1")
    assert stored is not None and stored.version == 9
    assert snapshots.writes == 1
