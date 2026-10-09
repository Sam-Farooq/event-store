"""Projection documents in a dict, with the Mongo target's exact semantics."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from eventstore.events import Record
from eventstore.projections.base import SEEN, Mutation, ProjectionTarget


class InMemoryProjectionTarget(ProjectionTarget):
    def __init__(self) -> None:
        self._documents: dict[tuple[str, str], dict[str, Any]] = {}

    async def apply(
        self,
        *,
        projection: str,
        record: Record,
        mutations: Sequence[Mutation],
    ) -> int:
        applied = 0
        for mutation in mutations:
            address = (projection, mutation.key)
            document = self._documents.get(address)
            seen = 0 if document is None else document[SEEN].get(record.aggregate_id, 0)
            if seen >= record.version:
                # The Mongo target reaches this state as a DuplicateKeyError
                # from an upsert whose guard did not match. Same outcome: the
                # record has already been folded in and there is nothing to do.
                continue
            if document is None:
                document = {
                    "_id": f"{projection}/{mutation.key}",
                    "_projection": projection,
                    "_key": mutation.key,
                    SEEN: {},
                }
                self._documents[address] = document
            for name, delta in mutation.inc.items():
                document[name] = document.get(name, 0) + delta
            document.update(mutation.assign)
            document[SEEN][record.aggregate_id] = record.version
            applied += 1
        return applied

    async def fetch(self, *, projection: str, key: str) -> dict[str, Any] | None:
        document = self._documents.get((projection, key))
        return None if document is None else dict(document)

    def keys(self, projection: str) -> list[str]:
        return sorted(key for name, key in self._documents if name == projection)
