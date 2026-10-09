"""Drive projections from the log, or from a change stream."""

from __future__ import annotations

from collections.abc import AsyncIterable, Sequence
from dataclasses import dataclass

from eventstore.events import Record
from eventstore.projections.base import Projection, ProjectionTarget
from eventstore.stores.base import EventStore


@dataclass
class RunStats:
    """What a pass did. `skipped` is the number the guard refused."""

    records: int = 0
    applied: int = 0
    skipped: int = 0
    position: str | None = None


class ProjectionRunner:
    """Feed records to whichever projections handle them.

    One aggregate's records must arrive in ascending version order. The guard
    makes a repeat harmless and makes an out-of-order arrival a dropped
    update, which is quieter and worse, so order is a requirement here rather
    than something the target can rescue.
    """

    def __init__(self, target: ProjectionTarget, projections: Sequence[Projection]) -> None:
        if not projections:
            raise ValueError("a runner with no projections has nothing to do")
        names = [projection.name for projection in projections]
        if "" in names:
            raise ValueError("every projection needs a name; it becomes part of the document id")
        if len(set(names)) != len(names):
            raise ValueError(f"two projections share a name: {sorted(names)}")
        for projection in projections:
            if not projection.handles:
                raise ValueError(f"{projection.name} handles no event types")
        self._target = target
        self._projections = tuple(projections)
        self.handles = frozenset(
            event_type for projection in projections for event_type in projection.handles
        )

    async def handle(self, record: Record) -> tuple[int, int]:
        applied = skipped = 0
        for projection in self._projections:
            if record.event_type not in projection.handles:
                continue
            mutations = projection.plan(record)
            if not mutations:
                continue
            landed = await self._target.apply(
                projection=projection.name, record=record, mutations=mutations
            )
            applied += landed
            skipped += len(mutations) - landed
        return applied, skipped

    async def catchup(
        self,
        store: EventStore,
        *,
        after: str | None = None,
        batch_size: int = 500,
    ) -> RunStats:
        """Read the log forward from a cursor until it runs out.

        The event-type filter is the union of what the projections handle, so
        adding a projection widens the query instead of needing one.
        """
        stats = RunStats(position=after)
        while True:
            batch = await store.read_all(
                after=stats.position, limit=batch_size, event_types=self.handles
            )
            if not batch:
                return stats
            await self._fold(batch, stats)
            if len(batch) < batch_size:
                return stats

    async def consume(
        self,
        records: AsyncIterable[Record],
        *,
        limit: int | None = None,
    ) -> RunStats:
        """Fold a live feed, such as `MongoEventStore.follow()`."""
        stats = RunStats()
        async for record in records:
            await self._fold([record], stats)
            if limit is not None and stats.records >= limit:
                break
        return stats

    async def _fold(self, batch: Sequence[Record], stats: RunStats) -> None:
        for record in batch:
            applied, skipped = await self.handle(record)
            stats.records += 1
            stats.applied += applied
            stats.skipped += skipped
            stats.position = record.position
