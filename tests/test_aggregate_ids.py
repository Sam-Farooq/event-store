"""An id is also a field path, so not every string is one."""

from __future__ import annotations

import uuid

import pytest
from pydantic import ValidationError

from eventstore.errors import InvalidAggregateId
from eventstore.events import Record, check_aggregate_id, utcnow
from eventstore.projections import InMemoryProjectionTarget, Mutation
from eventstore.stores import InMemoryEventStore, MongoEventStore
from tests.conftest import TallyStarted
from tests.fakes import FakeCollection


@pytest.mark.parametrize("rejected", ["", "org.acme", "$set", "a$b", "tenant.1/a-1"])
def test_an_id_that_cannot_be_a_field_path_is_refused(rejected: str) -> None:
    with pytest.raises(InvalidAggregateId):
        check_aggregate_id(rejected)


@pytest.mark.parametrize(
    "accepted",
    ["a-1", "ledger_account:42", "68f1c0b2a4e5d6f70918ab23", "tenant-7/account-9"],
)
def test_the_id_shapes_anyone_actually_uses_are_accepted(accepted: str) -> None:
    assert check_aggregate_id(accepted) == accepted
    assert check_aggregate_id(str(uuid.uuid4()))


def test_an_invalid_id_is_a_value_error_too() -> None:
    with pytest.raises(ValueError, match="field path"):
        check_aggregate_id("org.acme")


def test_a_record_refuses_to_be_built_with_one() -> None:
    with pytest.raises(ValidationError):
        Record(
            aggregate_id="org.acme",
            aggregate_type="tally",
            version=1,
            event_type="tally.started",
            payload={"label": "x"},
            recorded_at=utcnow(),
            position="000000000001",
        )


async def test_the_memory_store_refuses_before_it_writes(
    store: InMemoryEventStore,
) -> None:
    with pytest.raises(InvalidAggregateId):
        await store.append(
            aggregate_id="org.acme",
            aggregate_type="tally",
            expected_version=0,
            events=[TallyStarted(label="x")],
        )
    assert len(store) == 0


async def test_the_mongo_store_refuses_before_it_writes() -> None:
    collection = FakeCollection()
    with pytest.raises(InvalidAggregateId):
        await MongoEventStore(collection).append(
            aggregate_id="org.acme",
            aggregate_type="tally",
            expected_version=0,
            events=[TallyStarted(label="x")],
        )
    assert collection.insert_calls == []


async def test_a_dotted_id_would_have_nested_the_seen_map() -> None:
    """Why the check exists, shown against the in-memory target.

    The target keys `_seen` by aggregate id. Mongo keys it by the same string
    used as an update path, so `a.b` and `a` collide there and not here. The
    two targets have to agree, so the id is validated instead.
    """
    target = InMemoryProjectionTarget()
    outer = Record(
        aggregate_id="a",
        aggregate_type="tally",
        version=1,
        event_type="tally.added",
        payload={"amount": 1},
        recorded_at=utcnow(),
        position="000000000001",
    )
    await target.apply(projection="t", record=outer, mutations=[Mutation(key="k", inc={"n": 1})])
    document = await target.fetch(projection="t", key="k")
    assert document is not None
    assert document["_seen"] == {"a": 1}
