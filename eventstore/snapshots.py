"""Snapshots: a cache of a fold, so replay does not get slower forever."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field
from pymongo.errors import DuplicateKeyError

from eventstore.events import utcnow

if TYPE_CHECKING:  # pragma: no cover - import cost, not behaviour
    from motor.motor_asyncio import AsyncIOMotorCollection


class Snapshot(BaseModel):
    """State as of a version, plus which aggregate type produced it.

    The type is stored because the state shape belongs to a class. A snapshot
    written by one aggregate and restored into another is a silent wrong
    answer, and the check costs one comparison.
    """

    model_config = ConfigDict(frozen=True)

    aggregate_id: str = Field(min_length=1)
    aggregate_type: str = Field(min_length=1)
    version: int = Field(ge=1)
    state: dict[str, Any]
    taken_at: datetime = Field(default_factory=utcnow)


class SnapshotStore(ABC):
    """One snapshot per aggregate, replaced in place, never accumulated."""

    @abstractmethod
    async def load(self, aggregate_id: str) -> Snapshot | None: ...

    @abstractmethod
    async def save(self, snapshot: Snapshot) -> bool:
        """Store it unless a newer one is already there.

        Returns False when an equal or newer snapshot exists. That is the
        normal outcome of two workers saving the same aggregate, and it is not
        an error: the newer snapshot is the one worth keeping and the loser has
        nothing to do.
        """


class InMemorySnapshotStore(SnapshotStore):
    def __init__(self) -> None:
        self._latest: dict[str, Snapshot] = {}
        self.writes = 0

    async def load(self, aggregate_id: str) -> Snapshot | None:
        return self._latest.get(aggregate_id)

    async def save(self, snapshot: Snapshot) -> bool:
        current = self._latest.get(snapshot.aggregate_id)
        if current is not None and current.version >= snapshot.version:
            return False
        self._latest[snapshot.aggregate_id] = snapshot
        self.writes += 1
        return True


class MongoSnapshotStore(SnapshotStore):
    def __init__(self, collection: AsyncIOMotorCollection) -> None:
        self._snapshots = collection

    async def load(self, aggregate_id: str) -> Snapshot | None:
        document = await self._snapshots.find_one({"_id": aggregate_id})
        if document is None:
            return None
        return Snapshot(
            aggregate_id=document["_id"],
            aggregate_type=document["aggregate_type"],
            version=document["version"],
            state=document["state"],
            taken_at=document["taken_at"],
        )

    async def save(self, snapshot: Snapshot) -> bool:
        try:
            result = await self._snapshots.update_one(
                {"_id": snapshot.aggregate_id, "version": {"$lt": snapshot.version}},
                {
                    "$set": {
                        "aggregate_type": snapshot.aggregate_type,
                        "version": snapshot.version,
                        "state": snapshot.state,
                        "taken_at": snapshot.taken_at,
                    }
                },
                upsert=True,
            )
        except DuplicateKeyError:
            # An upsert whose filter matches nothing inserts a new document.
            # When a newer snapshot already holds this _id, that insert
            # collides with itself. The collision means someone else got there
            # first with a better snapshot, so there is nothing left to do.
            return False
        return result.modified_count > 0 or result.upserted_id is not None
