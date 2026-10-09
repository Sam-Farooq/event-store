"""What a mutation and a runner refuse to be built out of."""

from __future__ import annotations

from collections.abc import Sequence

import pytest

from eventstore.events import Record
from eventstore.projections import (
    InMemoryProjectionTarget,
    Mutation,
    Projection,
    ProjectionRunner,
)
from ledger import AccountBalance, CurrencyTotals


class Named(Projection):
    def __init__(self, name: str, handles: frozenset[str]) -> None:
        self.name = name
        self.handles = handles

    def plan(self, record: Record) -> Sequence[Mutation]:
        return []


def test_a_mutation_needs_a_key() -> None:
    with pytest.raises(ValueError, match="document key"):
        Mutation(key="", inc={"n": 1})


def test_a_mutation_that_changes_nothing_is_refused() -> None:
    with pytest.raises(ValueError, match="changes nothing"):
        Mutation(key="GBP")


def test_one_field_cannot_be_both_incremented_and_assigned() -> None:
    with pytest.raises(ValueError, match="net"):
        Mutation(key="GBP", inc={"net": 1}, assign={"net": 0})


@pytest.mark.parametrize("reserved", ["_id", "_seen", "_projection", "_key"])
def test_the_targets_own_fields_are_off_limits(reserved: str) -> None:
    with pytest.raises(ValueError, match=reserved):
        Mutation(key="GBP", assign={reserved: "mine now"})


def test_a_mutation_is_immutable_once_built() -> None:
    mutation = Mutation(key="GBP", inc={"net": 1})
    with pytest.raises(AttributeError):
        mutation.key = "USD"  # type: ignore[misc]


def test_a_runner_with_no_projections_is_refused() -> None:
    with pytest.raises(ValueError, match="nothing to do"):
        ProjectionRunner(InMemoryProjectionTarget(), [])


def test_two_projections_cannot_share_a_name() -> None:
    with pytest.raises(ValueError, match="share a name"):
        ProjectionRunner(
            InMemoryProjectionTarget(),
            [Named("totals", frozenset({"a"})), Named("totals", frozenset({"b"}))],
        )


def test_a_projection_without_a_name_is_refused() -> None:
    with pytest.raises(ValueError, match="needs a name"):
        ProjectionRunner(InMemoryProjectionTarget(), [Named("", frozenset({"a"}))])


def test_a_projection_that_handles_nothing_is_refused() -> None:
    with pytest.raises(ValueError, match="handles no event types"):
        ProjectionRunner(InMemoryProjectionTarget(), [Named("idle", frozenset())])


def test_the_runner_asks_the_store_for_the_union_of_what_it_handles() -> None:
    runner = ProjectionRunner(InMemoryProjectionTarget(), [AccountBalance(), CurrencyTotals()])
    assert runner.handles == {
        "ledger.opened",
        "ledger.deposited",
        "ledger.withdrawn",
        "ledger.closed",
    }
