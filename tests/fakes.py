"""A hand-written stand-in for a motor collection.

mongomock would hide the thing worth checking. These tests assert the exact
filter and update documents the adapter builds, because a projection guard
that silently stops guarding looks identical from the outside: every number
is still plausible, just doubled.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any


@dataclass
class FakeUpdateResult:
    modified_count: int = 0
    upserted_id: Any = None


class FakeCursor:
    def __init__(self, documents: Iterable[dict[str, Any]]) -> None:
        self.documents = list(documents)
        self.sorts: list[tuple[str, int]] = []
        self.limits: list[int] = []

    def sort(self, key: str, direction: int = 1) -> FakeCursor:
        self.sorts.append((key, direction))
        return self

    def limit(self, count: int) -> FakeCursor:
        self.limits.append(count)
        self.documents = self.documents[:count]
        return self

    async def __aiter__(self) -> AsyncIterator[dict[str, Any]]:
        for document in self.documents:
            yield document


@dataclass
class FakeCollection:
    """Records what it was asked to do. It does not pretend to be a database."""

    documents: list[dict[str, Any]] = field(default_factory=list)
    index_calls: list[tuple[Any, dict[str, Any]]] = field(default_factory=list)
    insert_calls: list[tuple[list[dict[str, Any]], bool]] = field(default_factory=list)
    update_calls: list[tuple[dict[str, Any], dict[str, Any], bool]] = field(default_factory=list)
    queries: list[dict[str, Any]] = field(default_factory=list)
    cursors: list[FakeCursor] = field(default_factory=list)
    raise_on_insert: Exception | None = None
    raise_on_update: Exception | None = None
    update_results: list[FakeUpdateResult] = field(default_factory=list)

    async def create_index(self, keys: Any, **options: Any) -> str:
        self.index_calls.append((keys, options))
        return str(options.get("name", "unnamed"))

    async def insert_many(
        self, documents: Sequence[dict[str, Any]], ordered: bool = True
    ) -> None:
        self.insert_calls.append((list(documents), ordered))
        if self.raise_on_insert is not None:
            raise self.raise_on_insert
        self.documents.extend(documents)

    def find(self, query: dict[str, Any]) -> FakeCursor:
        self.queries.append(query)
        cursor = FakeCursor(self.documents)
        self.cursors.append(cursor)
        return cursor

    async def find_one(self, query: dict[str, Any]) -> dict[str, Any] | None:
        self.queries.append(query)
        wanted = query.get("_id")
        for document in self.documents:
            if document.get("_id") == wanted:
                return document
        return None

    async def update_one(
        self,
        criteria: dict[str, Any],
        update: dict[str, Any],
        upsert: bool = False,
    ) -> FakeUpdateResult:
        self.update_calls.append((criteria, update, upsert))
        if self.raise_on_update is not None:
            raise self.raise_on_update
        if self.update_results:
            return self.update_results.pop(0)
        return FakeUpdateResult(modified_count=1)
