"""A throwaway domain, small enough that a failing test points at the library."""

from __future__ import annotations

from typing import Any

import pytest

from eventstore import Event, event
from eventstore.aggregate import Aggregate
from eventstore.events import Record
from eventstore.snapshots import InMemorySnapshotStore
from eventstore.stores import InMemoryEventStore


@event("tally.started")
class TallyStarted(Event):
    label: str


@event("tally.added")
class AmountAdded(Event):
    amount: int


class Tally(Aggregate):
    aggregate_type = "tally"

    def __init__(self, aggregate_id: str) -> None:
        super().__init__(aggregate_id)
        self.label = ""
        self.total = 0
        self.entries = 0

    @classmethod
    def start(cls, aggregate_id: str, label: str) -> Tally:
        tally = cls(aggregate_id)
        tally.record(TallyStarted(label=label))
        return tally

    def add(self, amount: int) -> None:
        if amount == 0:
            raise ValueError("adding zero is a no-op, not an event")
        self.record(AmountAdded(amount=amount))

    def apply(self, occurrence: Event) -> None:
        match occurrence:
            case TallyStarted():
                self.label = occurrence.label
            case AmountAdded():
                self.total += occurrence.amount
                self.entries += 1
            case _:
                raise TypeError(f"{type(occurrence).__name__} does not belong to a tally")

    def snapshot(self) -> dict[str, Any]:
        return {"label": self.label, "total": self.total, "entries": self.entries}

    def restore(self, state: dict[str, Any]) -> None:
        self.label = state["label"]
        self.total = state["total"]
        self.entries = state["entries"]


@pytest.fixture
def store() -> InMemoryEventStore:
    return InMemoryEventStore()


@pytest.fixture
def snapshots() -> InMemorySnapshotStore:
    return InMemorySnapshotStore()


class CountingStore(InMemoryEventStore):
    """Records what each read actually had to fetch, so a test can assert it."""

    def __init__(self) -> None:
        super().__init__()
        self.read_sizes: list[int] = []

    async def read(self, aggregate_id: str, *, from_version: int = 0) -> list[Record]:
        records = await super().read(aggregate_id, from_version=from_version)
        self.read_sizes.append(len(records))
        return records
