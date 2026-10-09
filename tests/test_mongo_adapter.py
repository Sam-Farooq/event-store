"""The Mongo adapters, with no MongoDB. Document shapes, not round trips."""

from __future__ import annotations

import pytest
from bson import ObjectId
from pymongo import ASCENDING
from pymongo.errors import BulkWriteError, DuplicateKeyError, OperationFailure

from eventstore.errors import ConcurrencyError
from eventstore.events import utcnow
from eventstore.projections import Mutation
from eventstore.projections.mongo import MongoProjectionTarget
from eventstore.snapshots import MongoSnapshotStore, Snapshot
from eventstore.stores.mongo import STREAM_INDEX, MongoEventStore, duplicate_offset, to_record
from tests.conftest import AmountAdded, TallyStarted
from tests.fakes import FakeCollection, FakeUpdateResult


def bulk_error(index: int = 0, code: int = 11000) -> BulkWriteError:
    return BulkWriteError(
        {"writeErrors": [{"index": index, "code": code, "errmsg": "E11000 duplicate key"}]}
    )


def stored(version: int = 1) -> dict[str, object]:
    return {
        "_id": ObjectId(),
        "aggregate_id": "t-1",
        "aggregate_type": "tally",
        "version": version,
        "event_type": "tally.added",
        "payload": {"amount": 3},
        "recorded_at": utcnow(),
    }


async def test_the_unique_index_is_exactly_aggregate_id_then_version() -> None:
    collection = FakeCollection()
    await MongoEventStore(collection).create_indexes()

    keys, options = collection.index_calls[0]
    assert keys == [("aggregate_id", ASCENDING), ("version", ASCENDING)]
    assert options == {"unique": True, "name": STREAM_INDEX}


async def test_a_second_index_covers_the_catchup_query() -> None:
    collection = FakeCollection()
    await MongoEventStore(collection).create_indexes()

    keys, options = collection.index_calls[1]
    assert keys == [("event_type", ASCENDING), ("_id", ASCENDING)]
    assert "unique" not in options, "only the stream index is allowed to reject a write"


async def test_append_numbers_the_batch_and_inserts_in_order() -> None:
    collection = FakeCollection()
    written = await MongoEventStore(collection).append(
        aggregate_id="t-1",
        aggregate_type="tally",
        expected_version=4,
        events=[TallyStarted(label="x"), AmountAdded(amount=2)],
    )

    documents, ordered = collection.insert_calls[0]
    assert ordered is True, "an unordered insert writes past the conflict"
    assert [document["version"] for document in documents] == [5, 6]
    assert documents[0]["event_type"] == "tally.started"
    assert documents[1]["payload"] == {"amount": 2}
    assert [record.version for record in written] == [5, 6]
    assert written[0].position == str(documents[0]["_id"])


async def test_append_with_no_events_does_not_reach_the_collection() -> None:
    collection = FakeCollection()
    assert (
        await MongoEventStore(collection).append(
            aggregate_id="t-1", aggregate_type="tally", expected_version=0, events=[]
        )
        == []
    )
    assert collection.insert_calls == []


async def test_a_bulk_write_error_becomes_a_concurrency_error() -> None:
    collection = FakeCollection(raise_on_insert=bulk_error(index=0))
    with pytest.raises(ConcurrencyError) as caught:
        await MongoEventStore(collection).append(
            aggregate_id="t-1",
            aggregate_type="tally",
            expected_version=7,
            events=[AmountAdded(amount=1)],
        )
    assert caught.value.conflicting_version == 8


async def test_the_offset_in_the_write_error_names_the_version() -> None:
    collection = FakeCollection(raise_on_insert=bulk_error(index=2))
    with pytest.raises(ConcurrencyError) as caught:
        await MongoEventStore(collection).append(
            aggregate_id="t-1",
            aggregate_type="tally",
            expected_version=7,
            events=[AmountAdded(amount=i) for i in range(1, 5)],
        )
    assert caught.value.conflicting_version == 10


async def test_a_single_document_duplicate_key_error_is_also_a_conflict() -> None:
    collection = FakeCollection(
        raise_on_insert=DuplicateKeyError("E11000", 11000, {"index": 0, "code": 11000})
    )
    with pytest.raises(ConcurrencyError):
        await MongoEventStore(collection).append(
            aggregate_id="t-1",
            aggregate_type="tally",
            expected_version=0,
            events=[TallyStarted(label="x")],
        )


async def test_a_write_error_that_is_not_a_duplicate_key_is_not_swallowed() -> None:
    collection = FakeCollection(raise_on_insert=bulk_error(code=16500))
    with pytest.raises(BulkWriteError):
        await MongoEventStore(collection).append(
            aggregate_id="t-1",
            aggregate_type="tally",
            expected_version=0,
            events=[TallyStarted(label="x")],
        )


def test_duplicate_offset_ignores_unrelated_failures() -> None:
    assert duplicate_offset(OperationFailure("not master")) is None
    assert duplicate_offset(RuntimeError("boom")) is None
    assert duplicate_offset(bulk_error(index=3)) == 3


async def test_read_asks_for_one_stream_sorted_by_version() -> None:
    collection = FakeCollection(documents=[stored(1), stored(2)])
    records = await MongoEventStore(collection).read("t-1", from_version=1)

    assert collection.queries[0] == {"aggregate_id": "t-1", "version": {"$gt": 1}}
    assert collection.cursors[0].sorts == [("version", ASCENDING)]
    assert [record.version for record in records] == [1, 2]


async def test_read_all_pages_by_id_and_filters_by_event_type() -> None:
    cursor_position = ObjectId()
    collection = FakeCollection(documents=[stored(1)])
    await MongoEventStore(collection).read_all(
        after=str(cursor_position), limit=25, event_types={"tally.added", "tally.started"}
    )

    assert collection.queries[0] == {
        "_id": {"$gt": cursor_position},
        "event_type": {"$in": ["tally.added", "tally.started"]},
    }
    assert collection.cursors[0].sorts == [("_id", ASCENDING)]
    assert collection.cursors[0].limits == [25]


def test_a_document_becomes_a_record_with_its_id_as_the_cursor() -> None:
    document = stored(4)
    record = to_record(document)
    assert record.position == str(document["_id"])
    assert record.to_event() == AmountAdded(amount=3)


async def test_the_projection_update_carries_the_guard_and_the_change() -> None:
    collection = FakeCollection()
    target = MongoProjectionTarget(collection)
    record = to_record(stored(9))

    applied = await target.apply(
        projection="totals",
        record=record,
        mutations=[Mutation(key="GBP", inc={"net": 500}, assign={"label": "sterling"})],
    )

    criteria, update, upsert = collection.update_calls[0]
    assert applied == 1
    assert upsert is True
    assert criteria == {
        "_id": "totals/GBP",
        "_seen.t-1": {"$not": {"$gte": 9}},
    }
    assert update["$inc"] == {"net": 500}
    assert update["$set"]["_seen.t-1"] == 9
    assert update["$set"]["label"] == "sterling"
    assert update["$set"]["_key"] == "GBP"


async def test_a_mutation_with_no_inc_sends_no_inc_operator() -> None:
    collection = FakeCollection()
    await MongoProjectionTarget(collection).apply(
        projection="totals",
        record=to_record(stored(2)),
        mutations=[Mutation(key="GBP", assign={"label": "sterling"})],
    )
    _, update, _ = collection.update_calls[0]
    assert "$inc" not in update


async def test_an_upsert_that_collides_counts_as_already_applied() -> None:
    collection = FakeCollection(raise_on_update=DuplicateKeyError("E11000", 11000, {"index": 0}))
    applied = await MongoProjectionTarget(collection).apply(
        projection="totals",
        record=to_record(stored(2)),
        mutations=[Mutation(key="GBP", inc={"net": 1})],
    )
    assert applied == 0


async def test_an_update_that_matched_nothing_counts_nothing() -> None:
    collection = FakeCollection(update_results=[FakeUpdateResult(modified_count=0)])
    applied = await MongoProjectionTarget(collection).apply(
        projection="totals",
        record=to_record(stored(2)),
        mutations=[Mutation(key="GBP", inc={"net": 1})],
    )
    assert applied == 0


async def test_a_snapshot_is_written_only_over_an_older_version() -> None:
    collection = FakeCollection()
    saved = await MongoSnapshotStore(collection).save(
        Snapshot(aggregate_id="t-1", aggregate_type="tally", version=50, state={"total": 3})
    )

    criteria, update, upsert = collection.update_calls[0]
    assert saved is True
    assert criteria == {"_id": "t-1", "version": {"$lt": 50}}
    assert update["$set"]["version"] == 50
    assert upsert is True


async def test_a_snapshot_losing_to_a_newer_one_is_not_an_error() -> None:
    collection = FakeCollection(raise_on_update=DuplicateKeyError("E11000", 11000, {"index": 0}))
    assert (
        await MongoSnapshotStore(collection).save(
            Snapshot(aggregate_id="t-1", aggregate_type="tally", version=9, state={})
        )
        is False
    )


async def test_loading_a_snapshot_maps_the_id_back_to_the_aggregate() -> None:
    collection = FakeCollection(
        documents=[
            {
                "_id": "t-1",
                "aggregate_type": "tally",
                "version": 12,
                "state": {"total": 7},
                "taken_at": utcnow(),
            }
        ]
    )
    snapshot = await MongoSnapshotStore(collection).load("t-1")
    assert snapshot is not None
    assert (snapshot.aggregate_id, snapshot.version) == ("t-1", 12)
    assert await MongoSnapshotStore(collection).load("t-404") is None
