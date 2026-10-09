"""Event-sourced aggregates with pluggable storage."""

from eventstore.errors import (
    ConcurrencyError,
    DuplicateEventType,
    EventStoreError,
    ReplayGap,
    UnknownEventType,
)
from eventstore.events import Event, Record, event

__all__ = [
    "ConcurrencyError",
    "DuplicateEventType",
    "Event",
    "EventStoreError",
    "Record",
    "ReplayGap",
    "UnknownEventType",
    "event",
]
