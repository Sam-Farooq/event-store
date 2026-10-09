from __future__ import annotations

from eventstore.stores import InMemoryEventStore
from tests.conftest import AmountAdded, TallyStarted


async def fill(store: InMemoryEventStore) -> None:
    for aggregate_id in ("t-1", "t-2"):
        await store.append(
            aggregate_id=aggregate_id,
            aggregate_type="tally",
            expected_version=0,
            events=[TallyStarted(label=aggregate_id), AmountAdded(amount=2)],
        )


async def test_read_returns_one_stream_in_version_order(store: InMemoryEventStore) -> None:
    await fill(store)
    assert [r.version for r in await store.read("t-1")] == [1, 2]
    assert {r.aggregate_id for r in await store.read("t-1")} == {"t-1"}


async def test_read_from_version_excludes_the_version_it_names(
    store: InMemoryEventStore,
) -> None:
    await fill(store)
    assert [r.version for r in await store.read("t-1", from_version=1)] == [2]
    assert await store.read("t-1", from_version=2) == []


async def test_an_unknown_aggregate_reads_empty(store: InMemoryEventStore) -> None:
    assert await store.read("nobody") == []


async def test_read_all_pages_forward_without_repeating(store: InMemoryEventStore) -> None:
    await fill(store)
    first = await store.read_all(limit=3)
    assert len(first) == 3
    second = await store.read_all(after=first[-1].position, limit=3)
    assert [r.position for r in second] == ["000000000004"]
    assert await store.read_all(after=second[-1].position) == []


async def test_read_all_filters_to_the_event_types_asked_for(
    store: InMemoryEventStore,
) -> None:
    await fill(store)
    filtered = await store.read_all(event_types={"tally.added"})
    assert [r.event_type for r in filtered] == ["tally.added", "tally.added"]
