"""Two read models over the same events, with different growth properties."""

from __future__ import annotations

from collections.abc import Sequence

from eventstore.events import Record
from eventstore.projections import Mutation, Projection
from ledger.events import AccountClosed, AccountOpened, Deposited, Withdrawn


class AccountBalance(Projection):
    """One document per account, so `_seen` holds exactly one entry forever."""

    name = "account_balance"
    handles = frozenset(
        {
            AccountOpened.event_type,
            Deposited.event_type,
            Withdrawn.event_type,
            AccountClosed.event_type,
        }
    )

    def plan(self, record: Record) -> Sequence[Mutation]:
        occurrence = record.to_event()
        key = record.aggregate_id
        match occurrence:
            case AccountOpened():
                return [
                    Mutation(
                        key=key,
                        inc={"balance": 0},
                        assign={
                            "owner": occurrence.owner,
                            "currency": occurrence.currency,
                            "closed": False,
                        },
                    )
                ]
            case Deposited():
                return [Mutation(key=key, inc={"balance": occurrence.minor_units, "credits": 1})]
            case Withdrawn():
                return [Mutation(key=key, inc={"balance": -occurrence.minor_units, "debits": 1})]
            case AccountClosed():
                return [Mutation(key=key, assign={"closed": True, "reason": occurrence.reason})]
            case _:
                return []


class CurrencyTotals(Projection):
    """One document per currency, fed by every account that uses it.

    This is the shape that pays for the guard. `_seen` gains an entry for
    every account that ever moves money in this currency and never loses one,
    so the document grows with the customer base rather than staying still.
    """

    name = "currency_totals"
    handles = frozenset({Deposited.event_type, Withdrawn.event_type})

    def plan(self, record: Record) -> Sequence[Mutation]:
        occurrence = record.to_event()
        match occurrence:
            case Deposited():
                return [
                    Mutation(
                        key=occurrence.currency,
                        inc={
                            "deposited": occurrence.minor_units,
                            "net": occurrence.minor_units,
                            "movements": 1,
                        },
                    )
                ]
            case Withdrawn():
                return [
                    Mutation(
                        key=occurrence.currency,
                        inc={
                            "withdrawn": occurrence.minor_units,
                            "net": -occurrence.minor_units,
                            "movements": 1,
                        },
                    )
                ]
            case _:
                return []
