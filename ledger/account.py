"""The account aggregate. Commands refuse, apply never does."""

from __future__ import annotations

from typing import Any

from eventstore import Event
from eventstore.aggregate import Aggregate
from ledger.events import AccountClosed, AccountOpened, Deposited, Withdrawn


class LedgerError(Exception):
    """A command the account refused."""


class NotOpen(LedgerError):
    pass


class AccountIsClosed(LedgerError):
    pass


class InsufficientFunds(LedgerError):
    def __init__(self, account_id: str, requested: int, balance: int) -> None:
        super().__init__(
            f"{account_id} cannot pay {requested} minor units out of a balance of {balance}"
        )
        self.requested = requested
        self.balance = balance


class LedgerAccount(Aggregate):
    """Balances in integer minor units, never floats.

    1.15 is not representable in binary floating point, so a few thousand
    additions of it drift from the sum anyone checking by hand will get. The
    balance here is pennies, and the display layer divides.
    """

    aggregate_type = "ledger.account"

    def __init__(self, aggregate_id: str) -> None:
        super().__init__(aggregate_id)
        self.owner = ""
        self.currency = ""
        self.balance = 0
        self.is_open = False
        self.is_closed = False

    @classmethod
    def open(cls, account_id: str, *, owner: str, currency: str) -> LedgerAccount:
        """A new account at version 0.

        Nothing here refuses a second open, because an aggregate built from an
        empty stream cannot tell an unused id from one it was not shown. The
        store refuses it: saving this expects version 0, and a stream that
        already has events rejects that with a ConcurrencyError.
        """
        account = cls(account_id)
        account.record(AccountOpened(owner=owner, currency=currency))
        return account

    def deposit(self, minor_units: int, *, reference: str) -> None:
        self._require_usable()
        self.record(Deposited(minor_units=minor_units, currency=self.currency, reference=reference))

    def withdraw(self, minor_units: int, *, reference: str) -> None:
        self._require_usable()
        if minor_units > self.balance:
            raise InsufficientFunds(self.id, minor_units, self.balance)
        self.record(Withdrawn(minor_units=minor_units, currency=self.currency, reference=reference))

    def close(self, *, reason: str) -> None:
        self._require_usable()
        if self.balance != 0:
            raise LedgerError(f"{self.id} still holds {self.balance} minor units")
        self.record(AccountClosed(reason=reason))

    def apply(self, occurrence: Event) -> None:
        match occurrence:
            case AccountOpened():
                self.owner = occurrence.owner
                self.currency = occurrence.currency
                self.is_open = True
            case Deposited():
                self.balance += occurrence.minor_units
            case Withdrawn():
                self.balance -= occurrence.minor_units
            case AccountClosed():
                self.is_closed = True
            case _:
                # A type this class has never heard of is a wiring mistake, not
                # old data. Old data arrives as a known type with fields the
                # current model gives defaults to, and that has to keep working.
                raise TypeError(f"{type(occurrence).__name__} is not a ledger event")

    def _require_usable(self) -> None:
        if self.is_closed:
            raise AccountIsClosed(f"{self.id} was closed")
        if not self.is_open:
            raise NotOpen(f"{self.id} has not been opened")

    def snapshot(self) -> dict[str, Any]:
        return {
            "owner": self.owner,
            "currency": self.currency,
            "balance": self.balance,
            "is_open": self.is_open,
            "is_closed": self.is_closed,
        }

    def restore(self, state: dict[str, Any]) -> None:
        self.owner = state["owner"]
        self.currency = state["currency"]
        self.balance = state["balance"]
        self.is_open = state["is_open"]
        self.is_closed = state["is_closed"]
