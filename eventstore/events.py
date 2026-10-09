"""Domain events, their wire names, and the record that stores one."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, ClassVar, TypeVar

from pydantic import BaseModel, ConfigDict, Field, field_validator

from eventstore.errors import DuplicateEventType, InvalidAggregateId, UnknownEventType

_REGISTRY: dict[str, type[Event]] = {}


FORBIDDEN_IN_ID = (".", "$")


def utcnow() -> datetime:
    return datetime.now(UTC)


def check_aggregate_id(value: str) -> str:
    """Refuse an id the projection target could not address.

    UUIDs, ULIDs and ObjectId hex all pass. A dotted or dollar-prefixed id
    does not, and it has to be caught here rather than at the update, because
    the in-memory target uses the id as a plain dict key and would never
    notice.
    """
    if not value:
        raise InvalidAggregateId("an aggregate id cannot be empty")
    for character in FORBIDDEN_IN_ID:
        if character in value:
            raise InvalidAggregateId(
                f"an aggregate id cannot contain {character!r}, it becomes a field path: {value!r}"
            )
    return value


class Event(BaseModel):
    """One thing that happened. Immutable, and named by a string.

    Subclasses hold only the facts a reader needs, which is often more than
    the aggregate needed to decide. See the currency field on the ledger's
    money events for why.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    event_type: ClassVar[str] = ""


_E = TypeVar("_E", bound=Event)


def event(name: str) -> Callable[[type[_E]], type[_E]]:
    """Register an event class under a stable wire name.

    The name is stored with every event and is not the class name. Renaming
    `Deposited` is then a refactor rather than an act that strands years of
    stored events behind a name nothing answers to.
    """

    def decorate(cls: type[_E]) -> type[_E]:
        claimed = _REGISTRY.get(name)
        if claimed is not None and claimed is not cls:
            raise DuplicateEventType(f"{name!r} is already registered to {claimed.__name__}")
        cls.event_type = name
        _REGISTRY[name] = cls
        return cls

    return decorate


def registered(name: str) -> type[Event]:
    try:
        return _REGISTRY[name]
    except KeyError:
        raise UnknownEventType(f"no event class is registered as {name!r}") from None


def type_name(instance: Event) -> str:
    if not instance.event_type:
        raise UnknownEventType(
            f"{type(instance).__name__} was never passed to @event(...), so it has no wire name"
        )
    return instance.event_type


def rebuild(event_type: str, payload: dict[str, Any]) -> Event:
    """Turn a stored payload back into its event class."""
    return registered(event_type).model_validate(payload)


class Record(BaseModel):
    """A stored event: the payload, plus where it sits in its stream.

    `version` counts from 1 within one aggregate and is the field the unique
    index guards. `position` is an opaque store-assigned cursor used only for
    reading the whole log forward; it sorts lexicographically.
    """

    model_config = ConfigDict(frozen=True)

    aggregate_id: str = Field(min_length=1)
    aggregate_type: str = Field(min_length=1)
    version: int = Field(ge=1)
    event_type: str = Field(min_length=1)
    payload: dict[str, Any]
    recorded_at: datetime
    position: str = Field(min_length=1)

    @field_validator("aggregate_id")
    @classmethod
    def _usable_as_a_field_path(cls, value: str) -> str:
        return check_aggregate_id(value)

    def to_event(self) -> Event:
        return rebuild(self.event_type, self.payload)
