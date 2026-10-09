"""The replay tests. A second pass over the same log must change nothing."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence

import pytest

from eventstore import Repository
from eventstore.events import Record
from eventstore.projections import (
    InMemoryProjectionTarget,
    Mutation,
    Projection,
    ProjectionRunner,
)
from eventstore.projections.base import SEEN
from eventstore.stores import InMemoryEventStore
from ledger import AccountBalance, CurrencyTotals, LedgerAccount

RECORDS = 7
MUTATIONS = 11


async def seed(store: InMemoryEventStore) -> None:
    """Seven events over three accounts and two currencies."""
    accounts: Repository[LedgerAccount] = Repository(store, LedgerAccount)

    nadia = LedgerAccount.open("a-1", owner="Nadia", currency="GBP")
    nadia.deposit(10_000, reference="salary")
    nadia.withdraw(2_500, reference="rent")
    await accounts.save(nadia)

    omar = LedgerAccount.open("a-2", owner="Omar", currency="GBP")
    omar.deposit(4_000, reference="float")
    await accounts.save(omar)

    priya = LedgerAccount.open("a-3", owner="Priya", currency="USD")
    priya.deposit(7_000, reference="float")
    await accounts.save(priya)


def runner_over(target: InMemoryProjectionTarget) -> ProjectionRunner:
    return ProjectionRunner(target, [AccountBalance(), CurrencyTotals()])


async def feed(records: Sequence[Record]) -> AsyncIterator[Record]:
    for record in records:
        yield record


async def balances(target: InMemoryProjectionTarget) -> dict[str, int]:
    out = {}
    for key in target.keys("account_balance"):
        document = await target.fetch(projection="account_balance", key=key)
        assert document is not None
        out[key] = document["balance"]
    return out


async def test_a_first_pass_builds_both_read_models(
    store: InMemoryEventStore, target: InMemoryProjectionTarget
) -> None:
    await seed(store)
    stats = await runner_over(target).catchup(store)

    assert (stats.records, stats.applied, stats.skipped) == (RECORDS, MUTATIONS, 0)
    assert await balances(target) == {"a-1": 7_500, "a-2": 4_000, "a-3": 7_000}

    gbp = await target.fetch(projection="currency_totals", key="GBP")
    usd = await target.fetch(projection="currency_totals", key="USD")
    assert gbp is not None and usd is not None
    assert (gbp["net"], gbp["movements"]) == (11_500, 3)
    assert (usd["net"], usd["movements"]) == (7_000, 1)


async def test_a_second_pass_over_the_same_log_changes_nothing(
    store: InMemoryEventStore, target: InMemoryProjectionTarget
) -> None:
    await seed(store)
    runner = runner_over(target)
    await runner.catchup(store)
    before = await balances(target)

    again = await runner.catchup(store)

    assert (again.records, again.applied, again.skipped) == (RECORDS, 0, MUTATIONS)
    assert await balances(target) == before
    gbp = await target.fetch(projection="currency_totals", key="GBP")
    assert gbp is not None and gbp["movements"] == 3


async def test_a_cursor_resumes_without_refolding_what_it_already_saw(
    store: InMemoryEventStore, target: InMemoryProjectionTarget
) -> None:
    await seed(store)
    runner = runner_over(target)
    first = await runner.catchup(store)

    accounts: Repository[LedgerAccount] = Repository(store, LedgerAccount)
    omar = await accounts.load("a-2")
    assert omar is not None
    omar.deposit(1_000, reference="interest")
    await accounts.save(omar)

    resumed = await runner.catchup(store, after=first.position)
    assert (resumed.records, resumed.applied, resumed.skipped) == (1, 2, 0)
    assert (await balances(target))["a-2"] == 5_000


async def test_a_small_batch_size_still_reaches_the_end(
    store: InMemoryEventStore, target: InMemoryProjectionTarget
) -> None:
    await seed(store)
    stats = await runner_over(target).catchup(store, batch_size=2)
    assert (stats.records, stats.applied) == (RECORDS, MUTATIONS)


async def test_a_live_feed_folds_the_same_way_as_a_catchup(
    store: InMemoryEventStore, target: InMemoryProjectionTarget
) -> None:
    await seed(store)
    records = await store.read_all()
    stats = await runner_over(target).consume(feed(records))

    assert (stats.records, stats.applied, stats.skipped) == (RECORDS, MUTATIONS, 0)
    assert await balances(target) == {"a-1": 7_500, "a-2": 4_000, "a-3": 7_000}


async def test_a_feed_can_be_stopped_after_a_fixed_number_of_records(
    store: InMemoryEventStore, target: InMemoryProjectionTarget
) -> None:
    await seed(store)
    records = await store.read_all()
    stats = await runner_over(target).consume(feed(records), limit=2)
    assert stats.records == 2
    assert target.keys("currency_totals") == ["GBP"]


async def test_a_projection_is_never_handed_an_event_it_did_not_ask_for(
    store: InMemoryEventStore, target: InMemoryProjectionTarget
) -> None:
    class Nosy(Projection):
        name = "nosy"
        handles = frozenset({"ledger.withdrawn"})

        def __init__(self) -> None:
            self.saw: list[str] = []

        def plan(self, record: Record) -> Sequence[Mutation]:
            self.saw.append(record.event_type)
            return [Mutation(key="all", inc={"count": 1})]

    nosy = Nosy()
    await seed(store)
    await ProjectionRunner(target, [nosy]).catchup(store)
    assert nosy.saw == ["ledger.withdrawn"]


async def test_out_of_order_delivery_loses_updates_instead_of_repeating_them(
    store: InMemoryEventStore, target: InMemoryProjectionTarget
) -> None:
    """The guard is a high-water mark, so it is idempotent and not commutative.

    Fed version 3 first, versions 2 and 1 are then below the mark and are
    dropped. The balance is wrong and no error is raised anywhere, which is
    why version order per aggregate is a requirement on the runner rather
    than something the target can repair. Both feeds that exist here provide
    it: read_all returns one aggregate's events in insertion order, and a
    change stream delivers them in commit order.
    """
    await seed(store)
    stream = await store.read("a-1")
    stats = await runner_over(target).consume(feed(list(reversed(stream))))

    assert stats.records == 3
    assert stats.applied == 2, "one mutation per projection, from version 3 only"
    assert stats.skipped == 3

    document = await target.fetch(projection="account_balance", key="a-1")
    assert document is not None
    assert document["balance"] == -2_500, "only the withdrawal survived the mark"
    assert "owner" not in document
    assert document[SEEN] == {"a-1": 3}


async def test_a_crash_between_two_documents_leaves_the_other_applicable(
    store: InMemoryEventStore,
) -> None:
    class FlakyTarget(InMemoryProjectionTarget):
        def __init__(self, fail_on: str) -> None:
            super().__init__()
            self.fail_on: str | None = fail_on

        async def apply(
            self,
            *,
            projection: str,
            record: Record,
            mutations: Sequence[Mutation],
        ) -> int:
            if projection == self.fail_on:
                self.fail_on = None
                raise RuntimeError(f"{projection} is unreachable")
            return await super().apply(projection=projection, record=record, mutations=mutations)

    await seed(store)
    target = FlakyTarget("currency_totals")
    runner = runner_over(target)

    with pytest.raises(RuntimeError, match="currency_totals"):
        await runner.catchup(store)

    partial = await target.fetch(projection="account_balance", key="a-1")
    assert partial is not None and partial["balance"] == 10_000
    assert await target.fetch(projection="currency_totals", key="GBP") is None

    recovered = await runner.catchup(store)
    assert (recovered.applied, recovered.skipped) == (MUTATIONS - 2, 2)
    assert await balances(target) == {"a-1": 7_500, "a-2": 4_000, "a-3": 7_000}
    gbp = await target.fetch(projection="currency_totals", key="GBP")
    assert gbp is not None and (gbp["net"], gbp["movements"]) == (11_500, 3)


async def test_the_cross_aggregate_document_remembers_every_account(
    store: InMemoryEventStore, target: InMemoryProjectionTarget
) -> None:
    await seed(store)
    await runner_over(target).catchup(store)

    per_account = await target.fetch(projection="account_balance", key="a-1")
    per_currency = await target.fetch(projection="currency_totals", key="GBP")
    assert per_account is not None and per_currency is not None
    assert len(per_account[SEEN]) == 1
    assert len(per_currency[SEEN]) == 2, "this map is what grows with the customer base"
