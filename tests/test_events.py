from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from eventstore import Event, event
from eventstore.errors import DuplicateEventType, UnknownEventType
from eventstore.events import rebuild, registered, type_name, utcnow
from tests.conftest import AmountAdded


def test_an_event_cannot_be_mutated_after_construction() -> None:
    occurrence = AmountAdded(amount=7)
    with pytest.raises(ValidationError):
        occurrence.amount = 8


def test_an_unexpected_field_is_rejected_rather_than_kept() -> None:
    with pytest.raises(ValidationError):
        AmountAdded(amount=7, currency="GBP")


def test_the_wire_name_survives_a_round_trip() -> None:
    assert type_name(AmountAdded(amount=7)) == "tally.added"
    assert registered("tally.added") is AmountAdded
    assert rebuild("tally.added", {"amount": 7}) == AmountAdded(amount=7)


def test_an_unregistered_name_names_itself_in_the_error() -> None:
    with pytest.raises(UnknownEventType, match="tally.retired"):
        rebuild("tally.retired", {})


def test_two_classes_cannot_claim_one_name() -> None:
    with pytest.raises(DuplicateEventType, match="AmountAdded"):

        @event("tally.added")
        class Rival(Event):
            amount: int


def test_a_class_that_skipped_the_decorator_has_no_wire_name() -> None:
    class Unregistered(Event):
        pass

    with pytest.raises(UnknownEventType, match="Unregistered"):
        type_name(Unregistered())


def test_payloads_are_stored_as_json_safe_primitives() -> None:
    @event("tally.audited")
    class Audited(Event):
        at: datetime

    moment = datetime(2026, 3, 1, 9, 30, tzinfo=UTC)
    payload = Audited(at=moment).model_dump(mode="json")
    assert isinstance(payload["at"], str)
    assert rebuild("tally.audited", payload).at == moment


def test_utcnow_is_timezone_aware() -> None:
    assert utcnow().tzinfo is not None
