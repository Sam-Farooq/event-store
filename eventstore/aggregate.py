"""The aggregate base class: fold events into state, collect new ones."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Any, ClassVar

from eventstore.errors import EventStoreError, ReplayGap
from eventstore.events import Event, Record


class Aggregate(ABC):
    """A consistency boundary with a version number.

    Two methods matter and they have opposite rules.

    `apply` folds one event into state. It runs on every load, over events
    written by every version of the code that ever ran, so it must not
    validate and must not refuse. An `apply` that raises on a three-year-old
    event makes the aggregate permanently unloadable, and no amount of
    deploying fixes it because the event is already stored.

    A command method is where refusal belongs. It inspects current state,
    decides, and on success calls `record`, which applies the event and queues
    it for the next save. A command that raises leaves state untouched.
    """

    aggregate_type: ClassVar[str] = ""

    def __init__(self, aggregate_id: str) -> None:
        self.id = aggregate_id
        self.version = 0
        self._pending: list[Event] = []

    @abstractmethod
    def apply(self, occurrence: Event) -> None:
        """Fold one event into state. No validation, no I/O, no refusal."""

    def record(self, occurrence: Event) -> None:
        """Apply an event and queue it to be written on the next save."""
        self.apply(occurrence)
        self.version += 1
        self._pending.append(occurrence)

    @property
    def pending(self) -> tuple[Event, ...]:
        return tuple(self._pending)

    @property
    def committed_version(self) -> int:
        """The version the store last agreed with, which is what a save expects."""
        return self.version - len(self._pending)

    def mark_committed(self) -> None:
        self._pending.clear()

    def replay(self, records: Iterable[Record]) -> None:
        """Fold a stream in version order, refusing a gap."""
        if self._pending:
            raise EventStoreError(
                f"{self.id} has {len(self._pending)} uncommitted events; replaying onto it "
                "would interleave them with stored history"
            )
        for stored in records:
            if stored.version != self.version + 1:
                raise ReplayGap(self.id, self.version + 1, stored.version)
            self.apply(stored.to_event())
            self.version = stored.version

    def snapshot(self) -> dict[str, Any] | None:
        """Return state as JSON-safe primitives, or None to opt out.

        A snapshot is a cache of a fold, never a source of truth. Deleting the
        snapshot collection costs replay time and loses no information.
        """
        return None

    def restore(self, state: dict[str, Any]) -> None:
        """Load state produced by `snapshot`. The repository sets the version."""
        raise NotImplementedError(
            f"{type(self).__name__}.snapshot() returns state but restore() was not implemented"
        )
