"""The four things that happen to an account."""

from __future__ import annotations

from pydantic import Field

from eventstore import Event, event


@event("ledger.opened")
class AccountOpened(Event):
    owner: str = Field(min_length=1)
    currency: str = Field(min_length=3, max_length=3)


@event("ledger.deposited")
class Deposited(Event):
    """Money in, in minor units, with the currency repeated.

    The account already knows its currency, so storing it again on every
    movement is denormalisation. It is here because a projection cannot look
    anything up: `CurrencyTotals` keys documents by currency and only ever
    sees this event. The cost is that changing an account's currency would
    need a new event type and a reader that understands both.
    """

    minor_units: int = Field(gt=0)
    currency: str = Field(min_length=3, max_length=3)
    reference: str = Field(min_length=1)


@event("ledger.withdrawn")
class Withdrawn(Event):
    minor_units: int = Field(gt=0)
    currency: str = Field(min_length=3, max_length=3)
    reference: str = Field(min_length=1)


@event("ledger.closed")
class AccountClosed(Event):
    reason: str = Field(min_length=1)
