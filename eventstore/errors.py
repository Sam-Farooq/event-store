"""Every error this library raises on purpose."""

from __future__ import annotations


class EventStoreError(Exception):
    """Base class, so a caller can catch the library without catching Exception."""


class ConcurrencyError(EventStoreError):
    """Someone else wrote to this stream between the load and the save.

    This is the expected outcome of a lost race, not a fault. The caller
    reloads the aggregate, replays the command against the newer state, and
    saves again, or gives up and tells the user.
    """

    def __init__(self, aggregate_id: str, expected_version: int, conflicting_version: int) -> None:
        super().__init__(
            f"{aggregate_id} was at a different version: expected {expected_version}, "
            f"version {conflicting_version} already exists"
        )
        self.aggregate_id = aggregate_id
        self.expected_version = expected_version
        self.conflicting_version = conflicting_version


class ReplayGap(EventStoreError):
    """A stream handed back versions with a hole in it.

    Folding events 1, 2 and 4 produces state that is wrong and looks fine, so
    the gap has to stop the load rather than be logged.
    """

    def __init__(self, aggregate_id: str, expected_version: int, found_version: int) -> None:
        super().__init__(
            f"{aggregate_id} expected version {expected_version}, got {found_version}"
        )
        self.aggregate_id = aggregate_id
        self.expected_version = expected_version
        self.found_version = found_version


class InvalidAggregateId(EventStoreError, ValueError):
    """An id that cannot be used as a MongoDB field path.

    The projection guard stores a version per aggregate under `_seen.<id>`, so
    the id becomes part of an update path. A dot in it addresses a nested
    document instead of a key, which works until two ids overlap as a prefix
    and then fails on a write nothing else explains.
    """


class UnknownEventType(EventStoreError):
    """A stored event names a type this process has no class for.

    Deleting an event class does not delete the events, so this is what a
    premature cleanup looks like from the read side.
    """


class DuplicateEventType(EventStoreError):
    """Two classes claimed the same wire name."""
