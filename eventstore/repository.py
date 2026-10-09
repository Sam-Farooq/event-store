"""Load an aggregate, save an aggregate. The only place the three parts meet."""

from __future__ import annotations

from typing import Generic, TypeVar

from eventstore.aggregate import Aggregate
from eventstore.events import Record
from eventstore.snapshots import Snapshot, SnapshotStore
from eventstore.stores.base import EventStore

A = TypeVar("A", bound=Aggregate)


class Repository(Generic[A]):
    """Snapshot, then the tail of the stream, then the fold.

    `snapshot_every` is a version interval, not a count of saves. A snapshot
    is written on the first save that crosses a multiple of it, so the stored
    version is wherever the aggregate landed rather than the multiple itself.
    Setting it to 0 turns snapshotting off and makes every load a full replay,
    which is correct and gets slower every day the aggregate stays alive.
    """

    def __init__(
        self,
        store: EventStore,
        aggregate_cls: type[A],
        *,
        snapshots: SnapshotStore | None = None,
        snapshot_every: int = 0,
    ) -> None:
        if not aggregate_cls.aggregate_type:
            raise ValueError(f"{aggregate_cls.__name__} needs a non-empty aggregate_type")
        if snapshot_every < 0:
            raise ValueError("snapshot_every cannot be negative")
        self._store = store
        self._cls = aggregate_cls
        self._snapshots = snapshots
        self._snapshot_every = snapshot_every

    async def load(self, aggregate_id: str) -> A | None:
        """Rebuild the aggregate, or None if nothing was ever written for it."""
        aggregate = self._cls(aggregate_id)
        from_version = 0

        if self._snapshots is not None:
            snapshot = await self._snapshots.load(aggregate_id)
            if snapshot is not None and snapshot.aggregate_type == self._cls.aggregate_type:
                aggregate.restore(snapshot.state)
                aggregate.version = snapshot.version
                from_version = snapshot.version

        aggregate.replay(await self._store.read(aggregate_id, from_version=from_version))
        return None if aggregate.version == 0 else aggregate

    async def save(self, aggregate: A) -> list[Record]:
        """Append the queued events at the version the aggregate was loaded at."""
        pending = aggregate.pending
        if not pending:
            return []

        committed = aggregate.committed_version
        written = await self._store.append(
            aggregate_id=aggregate.id,
            aggregate_type=self._cls.aggregate_type,
            expected_version=committed,
            events=pending,
        )
        aggregate.mark_committed()
        await self._take_snapshot(aggregate, crossed_from=committed)
        return written

    async def _take_snapshot(self, aggregate: A, *, crossed_from: int) -> bool:
        if self._snapshots is None or self._snapshot_every == 0:
            return False
        interval = self._snapshot_every
        if aggregate.version // interval == crossed_from // interval:
            return False
        state = aggregate.snapshot()
        if state is None:
            return False
        return await self._snapshots.save(
            Snapshot(
                aggregate_id=aggregate.id,
                aggregate_type=self._cls.aggregate_type,
                version=aggregate.version,
                state=state,
            )
        )
