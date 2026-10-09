"""What a store has to do, and nothing about how."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Collection, Sequence

from eventstore.events import Event, Record


class EventStore(ABC):
    """Append-only storage for events, addressed by aggregate and version."""

    @abstractmethod
    async def append(
        self,
        *,
        aggregate_id: str,
        aggregate_type: str,
        expected_version: int,
        events: Sequence[Event],
    ) -> list[Record]:
        """Write events as versions `expected_version + 1 .. + len(events)`.

        `expected_version` of 0 asserts the stream does not exist yet. Raise
        `ConcurrencyError` if any of those versions is taken.

        The check has to be the write. A read of the current version followed
        by an insert is two statements with a gap between them, and the whole
        point of the version is to close that gap.
        """

    @abstractmethod
    async def read(self, aggregate_id: str, *, from_version: int = 0) -> list[Record]:
        """One aggregate's events above `from_version`, in version order."""

    @abstractmethod
    async def read_all(
        self,
        *,
        after: str | None = None,
        limit: int = 500,
        event_types: Collection[str] | None = None,
    ) -> list[Record]:
        """The whole log forward from a cursor, for building projections.

        Ordered by `position`, which is per-writer monotonic and not a global
        sequence. Two aggregates' events may interleave differently from one
        read to the next; one aggregate's own events will not.
        """
