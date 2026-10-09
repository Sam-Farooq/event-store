"""Projections: a pure plan, and a target that owns the idempotency guard."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, ClassVar

from eventstore.events import Record

SEEN = "_seen"
RESERVED = frozenset({"_id", SEEN, "_projection", "_key"})


@dataclass(frozen=True, slots=True)
class Mutation:
    """One change to one projection document.

    Only two operations, and the restriction is the design. `inc` commutes, so
    the same event arriving twice can be made harmless by a version check
    alone. Anything that needs read, decide, write cannot: two workers reading
    the same document both decide against the same stale value. A projection
    that wants that has to take a lock, and no lock is implemented here.
    """

    key: str
    inc: Mapping[str, int] = field(default_factory=dict)
    assign: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.key:
            raise ValueError("a mutation needs a document key")
        if not self.inc and not self.assign:
            raise ValueError(
                f"mutation on {self.key!r} changes nothing; return no mutation instead"
            )
        overlap = set(self.inc) & set(self.assign)
        if overlap:
            # Mongo rejects $set and $inc on one path with "would create a
            # conflict", at runtime, on the one event that happens to do both.
            raise ValueError(f"{sorted(overlap)} appear in both inc and assign")
        trespass = (set(self.inc) | set(self.assign)) & RESERVED
        if trespass:
            raise ValueError(f"{sorted(trespass)} are owned by the target")


class Projection(ABC):
    """A pure function from a record to a list of mutations.

    `plan` gets the record and nothing else: no store, no target, no current
    document. That is what makes a projection replayable. It also means an
    event has to carry every fact its readers need, because a projection
    cannot look anything up. See `ledger.events`, where a deposit repeats the
    account's currency that the account already knew.
    """

    name: ClassVar[str] = ""
    handles: ClassVar[frozenset[str]] = frozenset()

    @abstractmethod
    def plan(self, record: Record) -> Sequence[Mutation]:
        """Mutations for this record. Return an empty sequence for no change."""


class ProjectionTarget(ABC):
    """Where projection documents live, and the only place the guard runs."""

    @abstractmethod
    async def apply(
        self,
        *,
        projection: str,
        record: Record,
        mutations: Sequence[Mutation],
    ) -> int:
        """Apply mutations that have not been applied, return how many landed.

        Each document remembers the highest version it has folded in per
        aggregate, under `_seen`. A record at or below that version is
        dropped. The counter is per document and not per runner, so a crash
        halfway through a record that touches three documents leaves the other
        two still applicable.
        """

    @abstractmethod
    async def fetch(self, *, projection: str, key: str) -> dict[str, Any] | None:
        """One projection document, or None."""
