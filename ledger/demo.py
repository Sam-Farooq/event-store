"""Run the whole thing against the in-memory store: python -m ledger.demo"""

from __future__ import annotations

import asyncio

from eventstore import ConcurrencyError, Repository
from eventstore.projections import InMemoryProjectionTarget, ProjectionRunner
from eventstore.projections.base import SEEN
from eventstore.snapshots import InMemorySnapshotStore
from eventstore.stores import InMemoryEventStore
from ledger import AccountBalance, CurrencyTotals, InsufficientFunds, LedgerAccount

SNAPSHOT_EVERY = 50


async def main() -> None:
    store = InMemoryEventStore()
    snapshots = InMemorySnapshotStore()
    accounts: Repository[LedgerAccount] = Repository(
        store, LedgerAccount, snapshots=snapshots, snapshot_every=SNAPSHOT_EVERY
    )

    print("ledger demo: in-memory store, no MongoDB, no network\n")

    for account_id, owner, currency in (
        ("a-1", "Nadia", "GBP"),
        ("a-2", "Omar", "GBP"),
        ("a-3", "Priya", "USD"),
    ):
        await accounts.save(LedgerAccount.open(account_id, owner=owner, currency=currency))
        print(f"opened   {account_id} {owner} {currency}")

    busy = await accounts.load("a-1")
    assert busy is not None
    for batch in range(12):
        for index in range(10):
            busy.deposit(1_000 + batch * 10 + index, reference=f"payroll-{batch:02d}-{index}")
        await accounts.save(busy)
    busy.withdraw(5_000, reference="rent")
    await accounts.save(busy)
    print(f"\n{busy.id}: 121 movements over 13 saves, balance {busy.balance} at v{busy.version}")

    omar = await accounts.load("a-2")
    priya = await accounts.load("a-3")
    assert omar is not None and priya is not None
    omar.deposit(25_000, reference="opening-float")
    priya.deposit(40_000, reference="opening-float")
    priya.withdraw(1_250, reference="fees")
    await accounts.save(omar)
    await accounts.save(priya)

    try:
        omar.withdraw(90_000, reference="too-much")
    except InsufficientFunds as refused:
        print(f"refused  {refused}")

    stale = await accounts.load("a-2")
    fresh = await accounts.load("a-2")
    assert stale is not None and fresh is not None
    fresh.deposit(500, reference="interest")
    await accounts.save(fresh)
    stale.deposit(500, reference="interest-again")
    try:
        await accounts.save(stale)
    except ConcurrencyError as lost:
        print(f"conflict {lost}")

    snapshot = await snapshots.load("a-1")
    assert snapshot is not None
    reloaded = await accounts.load("a-1")
    assert reloaded is not None
    print(
        f"\nevents stored: {len(store)}"
        f"\nsnapshots for a-1: {snapshots.writes}, latest v{snapshot.version}"
        f"\nloading a-1: snapshot v{snapshot.version} plus "
        f"{reloaded.version - snapshot.version} events replayed"
    )

    target = InMemoryProjectionTarget()
    runner = ProjectionRunner(target, [AccountBalance(), CurrencyTotals()])
    first = await runner.catchup(store, batch_size=64)
    second = await runner.catchup(store, batch_size=64)
    print(
        f"\ncatchup 1: {first.records} records, {first.applied} applied, {first.skipped} skipped"
        f"\ncatchup 2: {second.records} records, {second.applied} applied, {second.skipped} skipped"
    )

    print()
    for key in target.keys("account_balance"):
        document = await target.fetch(projection="account_balance", key=key)
        assert document is not None
        print(
            f"account_balance/{key}: owner {document['owner']} "
            f"balance {document['balance']} "
            f"credits {document.get('credits', 0)} debits {document.get('debits', 0)}"
        )
    for key in target.keys("currency_totals"):
        document = await target.fetch(projection="currency_totals", key=key)
        assert document is not None
        print(
            f"currency_totals/{key}: net {document['net']} "
            f"movements {document['movements']} "
            f"seen {len(document[SEEN])} account(s)"
        )


if __name__ == "__main__":
    asyncio.run(main())
