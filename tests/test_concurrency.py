"""The lost-update tests. No MongoDB, but the same rule under test."""

from __future__ import annotations

import pytest

from eventstore.errors import ConcurrencyError
from eventstore.stores import InMemoryEventStore
from tests.conftest import AmountAdded, TallyStarted


async def seed(store: InMemoryEventStore, count: int = 3) -> None:
    await store.append(
        aggregate_id="t-1",
        aggregate_type="tally",
        expected_version=0,
        events=[TallyStarted(label="march")] + [AmountAdded(amount=1) for _ in range(count - 1)],
    )


async def test_versions_start_at_one_and_stay_contiguous(store: InMemoryEventStore) -> None:
    written = await store.append(
        aggregate_id="t-1",
        aggregate_type="tally",
        expected_version=0,
        events=[TallyStarted(label="march"), AmountAdded(amount=4)],
    )
    assert [r.version for r in written] == [1, 2]


async def test_claiming_a_fresh_stream_that_exists_is_a_conflict(
    store: InMemoryEventStore,
) -> None:
    await seed(store)
    with pytest.raises(ConcurrencyError) as caught:
        await store.append(
            aggregate_id="t-1",
            aggregate_type="tally",
            expected_version=0,
            events=[TallyStarted(label="again")],
        )
    assert caught.value.conflicting_version == 1


async def test_the_second_writer_from_the_same_version_loses(store: InMemoryEventStore) -> None:
    await seed(store)

    await store.append(
        aggregate_id="t-1",
        aggregate_type="tally",
        expected_version=3,
        events=[AmountAdded(amount=10), AmountAdded(amount=20)],
    )
    with pytest.raises(ConcurrencyError) as caught:
        await store.append(
            aggregate_id="t-1",
            aggregate_type="tally",
            expected_version=3,
            events=[AmountAdded(amount=99), AmountAdded(amount=98)],
        )

    assert caught.value.expected_version == 3
    assert caught.value.conflicting_version == 4
    assert len(store) == 5, "the losing batch must not leave half of itself behind"
    assert [r.payload["amount"] for r in await store.read("t-1", from_version=3)] == [10, 20]


async def test_the_clash_is_always_on_the_first_event_of_the_batch(
    store: InMemoryEventStore,
) -> None:
    """Versions are contiguous and every writer starts from a version it read,
    so a batch can only collide at its own first offset. That is why an
    ordered insert needs no transaction to avoid a partial write."""
    await seed(store, count=6)
    with pytest.raises(ConcurrencyError) as caught:
        await store.append(
            aggregate_id="t-1",
            aggregate_type="tally",
            expected_version=1,
            events=[AmountAdded(amount=1), AmountAdded(amount=2), AmountAdded(amount=3)],
        )
    assert caught.value.conflicting_version == caught.value.expected_version + 1


async def test_two_aggregates_do_not_collide(store: InMemoryEventStore) -> None:
    await store.append(
        aggregate_id="t-1",
        aggregate_type="tally",
        expected_version=0,
        events=[TallyStarted(label="a")],
    )
    written = await store.append(
        aggregate_id="t-2",
        aggregate_type="tally",
        expected_version=0,
        events=[TallyStarted(label="b")],
    )
    assert written[0].version == 1
    assert len(store) == 2


async def test_appending_nothing_is_allowed_and_writes_nothing(
    store: InMemoryEventStore,
) -> None:
    await seed(store)
    assert (
        await store.append(
            aggregate_id="t-1", aggregate_type="tally", expected_version=3, events=[]
        )
        == []
    )
    assert len(store) == 3


async def test_a_negative_expected_version_is_a_caller_bug(store: InMemoryEventStore) -> None:
    with pytest.raises(ValueError, match="negative"):
        await store.append(
            aggregate_id="t-1",
            aggregate_type="tally",
            expected_version=-1,
            events=[TallyStarted(label="x")],
        )
