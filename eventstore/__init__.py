"""Event-sourced aggregates with storage behind an interface.

Five names and the errors: Aggregate, Event, Record, Repository, event.
Storage adapters live in `eventstore.stores`, `eventstore.snapshots` and
`eventstore.projections`, so importing the domain side pulls in no driver.
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
