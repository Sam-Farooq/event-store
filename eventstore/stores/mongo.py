"""The MongoDB event store. One collection, one unique index, one change stream."""

from __future__ import annotations

from collections.abc import AsyncIterator, Collection, Sequence
from typing import TYPE_CHECKING, Any

from bson import ObjectId
from pymongo import ASCENDING
from pymongo.errors import BulkWriteError, DuplicateKeyError

from eventstore.errors import ConcurrencyError
from eventstore.events import Event, Record, check_aggregate_id, type_name, utcnow
from eventstore.stores.base import EventStore

if TYPE_CHECKING:  # pragma: no cover - import cost, not behaviour
    from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase

DUPLICATE_KEY = 11000
STREAM_INDEX = "aggregate_id_1_version_1"
TYPE_INDEX = "event_type_1__id_1"


def connect(uri: str, database: str = "eventstore") -> AsyncIOMotorDatabase:
    """A motor database handle with the one option that is not optional.

    `tz_aware=True` makes stored datetimes come back with UTC attached.
    Without it they come back naive, comparing a stored timestamp against
    `datetime.now(UTC)` raises, and the only place that shows up is the code
    path that handles a late event.
    """
    from motor.motor_asyncio import AsyncIOMotorClient

    return AsyncIOMotorClient(uri, tz_aware=True)[database]


def duplicate_offset(exc: Exception) -> int | None:
    """The batch offset that hit the unique index, or None for any other error.

    This exists because `insert_many` does not raise `DuplicateKeyError`. It
    raises `BulkWriteError`, with code 11000 inside
    `details["writeErrors"][0]`. Catching only `DuplicateKeyError` turns a
    lost race, which is routine, into an unhandled error, which is a 500, and
    the two look identical in a log that prints the class name.
    """
    if isinstance(exc, DuplicateKeyError):
        details = exc.details or {}
        return int(details.get("index", 0))
    if isinstance(exc, BulkWriteError):
        for write_error in (exc.details or {}).get("writeErrors", ()):
            if write_error.get("code") == DUPLICATE_KEY:
                return int(write_error.get("index", 0))
    return None


def to_record(document: dict[str, Any]) -> Record:
    return Record(
        aggregate_id=document["aggregate_id"],
        aggregate_type=document["aggregate_type"],
        version=document["version"],
        event_type=document["event_type"],
        payload=document["payload"],
        recorded_at=document["recorded_at"],
        position=str(document["_id"]),
    )


class MongoEventStore(EventStore):
    """Events in one collection, keyed by `_id`, constrained by one index.

    The unique index on (aggregate_id, version) is the entire concurrency
    mechanism. Nothing else in this class is load bearing: no transaction, no
    per-aggregate lock, no read before the write. Drop that index and every
    guarantee here goes with it while the tests that use the in-memory store
    keep passing, which is why `create_indexes` is a method a caller must
    run and not a comment in a README.
    """

    def __init__(self, collection: AsyncIOMotorCollection) -> None:
        self._events = collection

    async def create_indexes(self) -> None:
        await self._events.create_index(
            [("aggregate_id", ASCENDING), ("version", ASCENDING)],
            unique=True,
            name=STREAM_INDEX,
        )
        # read_all filters on event_type and sorts by _id. Without this the
        # catchup scan reads every event ever written to find the few a
        # projection handles.
        await self._events.create_index(
            [("event_type", ASCENDING), ("_id", ASCENDING)],
            name=TYPE_INDEX,
        )

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
        if not events:
            return []

        recorded_at = utcnow()
        documents = [
            {
                "_id": ObjectId(),
                "aggregate_id": aggregate_id,
                "aggregate_type": aggregate_type,
                "version": expected_version + offset,
                "event_type": type_name(occurrence),
                "payload": occurrence.model_dump(mode="json"),
                "recorded_at": recorded_at,
            }
            for offset, occurrence in enumerate(events, start=1)
        ]

        try:
            # ordered=True stops at the first rejected document. Versions are
            # contiguous per aggregate, so a conflict always lands on offset 0
            # and nothing before it exists to be left half written.
            await self._events.insert_many(documents, ordered=True)
        except (BulkWriteError, DuplicateKeyError) as exc:
            offset = duplicate_offset(exc)
            if offset is None:
                raise
            raise ConcurrencyError(
                aggregate_id, expected_version, expected_version + offset + 1
            ) from exc

        return [to_record(document) for document in documents]

    async def read(self, aggregate_id: str, *, from_version: int = 0) -> list[Record]:
        cursor = self._events.find(
            {"aggregate_id": aggregate_id, "version": {"$gt": from_version}}
        ).sort("version", ASCENDING)
        return [to_record(document) async for document in cursor]

    async def read_all(
        self,
        *,
        after: str | None = None,
        limit: int = 500,
        event_types: Collection[str] | None = None,
    ) -> list[Record]:
        query: dict[str, Any] = {}
        if after is not None:
            query["_id"] = {"$gt": ObjectId(after)}
        if event_types is not None:
            query["event_type"] = {"$in": sorted(event_types)}
        cursor = self._events.find(query).sort("_id", ASCENDING).limit(limit)
        return [to_record(document) async for document in cursor]

    async def follow(
        self,
        *,
        resume_after: Any = None,
        event_types: Collection[str] | None = None,
    ) -> AsyncIterator[Record]:
        """Yield records as they are written, from a change stream.

        Needs a replica set: `watch` reads the oplog and a standalone mongod
        has none. The `$match` on inserts is not decoration. The collection is
        append-only by intent, not by enforcement, and an update that slips
        through should not reach a projection as if it were a new event.

        `resume_after` is accepted and not persisted here. Losing the token
        costs a re-read from the last known position, and a re-read is free
        because the projection target refuses to apply a record twice.
        """
        matched: dict[str, Any] = {"operationType": "insert"}
        if event_types is not None:
            matched["fullDocument.event_type"] = {"$in": sorted(event_types)}
        async with self._events.watch([{"$match": matched}], resume_after=resume_after) as stream:
            async for change in stream:
                yield to_record(change["fullDocument"])
