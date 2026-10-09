"""Event-sourced aggregates with storage behind an interface.

The public surface is deliberately five names. Storage adapters live in
`eventstore.stores`, `eventstore.snapshots` and `eventstore.projections`, so
importing the domain side of this library pulls in no driver.
"""

from eventstore.aggregate import Aggregate
from eventstore.errors import (
    ConcurrencyError,
    DuplicateEventType,
    EventStoreError,
    InvalidAggregateId,
    ReplayGap,
    UnknownEventType,
)
from eventstore.events import Event, Record, event
from eventstore.repository import Repository

__all__ = [
    "Aggregate",
    "ConcurrencyError",
    "DuplicateEventType",
    "Event",
    "EventStoreError",
    "InvalidAggregateId",
    "Record",
    "ReplayGap",
    "Repository",
    "UnknownEventType",
    "event",
]
