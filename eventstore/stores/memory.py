"""A store in a dict, with the same constraints as the Mongo one."""

from __future__ import annotations

from collections.abc import Collection, Sequence

from eventstore.errors import ConcurrencyError
from eventstore.events import Event, Record, check_aggregate_id, type_name, utcnow
from eventstore.stores.base import EventStore


class InMemoryEventStore(EventStore):
    """For tests, examples, and the first hour of a new domain.

    It is not a mock. It enforces uniqueness on (aggregate_id, version) by
    refusing a key it already holds, which is what the Mongo unique index
    does, and it inserts in version order and stops at the first clash, which
    is what an ordered insert does. The concurrency tests are therefore
    testing the real rule and not a stub of it.
    """

    def __init__(self) -> None:
        self._keys: set[tuple[str, int]] = set()
        self._log: list[Record] = []
        self._streams: dict[str, list[Record]] = {}
        self._sequence = 0

    async def append(
        self,
        *,
        aggregate_id: str,
        aggregate_type: str,
        expected_version: int,
        events: Sequence[Event],
    ) -> list[Record]:
        check_aggregate_id(aggregate_id)
        if expected_version < 0:
            raise ValueError("expected_version cannot be negative")
        written: list[Record] = []
        for offset, occurrence in enumerate(events, start=1):
            version = expected_version + offset
            if (aggregate_id, version) in self._keys:
                raise ConcurrencyError(aggregate_id, expected_version, version)
            self._sequence += 1
            stored = Record(
                aggregate_id=aggregate_id,
                aggregate_type=aggregate_type,
                version=version,
                event_type=type_name(occurrence),
                payload=occurrence.model_dump(mode="json"),
                recorded_at=utcnow(),
                position=f"{self._sequence:012d}",
            )
            self._keys.add((aggregate_id, version))
            self._log.append(stored)
            self._streams.setdefault(aggregate_id, []).append(stored)
            written.append(stored)
        return written

    async def read(self, aggregate_id: str, *, from_version: int = 0) -> list[Record]:
        stream = self._streams.get(aggregate_id, [])
        return sorted(
            (r for r in stream if r.version > from_version),
            key=lambda r: r.version,
        )

    async def read_all(
        self,
        *,
        after: str | None = None,
        limit: int = 500,
        event_types: Collection[str] | None = None,
    ) -> list[Record]:
        wanted = frozenset(event_types) if event_types is not None else None
        out: list[Record] = []
        for stored in self._log:
            if after is not None and stored.position <= after:
                continue
            if wanted is not None and stored.event_type not in wanted:
                continue
            out.append(stored)
            if len(out) == limit:
                break
        return out

    def __len__(self) -> int:
        return len(self._log)
