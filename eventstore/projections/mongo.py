"""Projection documents in MongoDB, one guarded upsert per mutation."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from pymongo.errors import DuplicateKeyError

from eventstore.events import Record
from eventstore.projections.base import SEEN, Mutation, ProjectionTarget

if TYPE_CHECKING:  # pragma: no cover - import cost, not behaviour
    from motor.motor_asyncio import AsyncIOMotorCollection


class MongoProjectionTarget(ProjectionTarget):
    """Every projection in one collection, `_id` namespaced by projection name.

    One update per mutation, no transaction, no checkpoint collection. The
    guard rides along in the same update as the change it guards, which is the
    only way to make the two atomic without a transaction, and transactions
    over a document per mutation would serialise the whole catchup.
    """

    def __init__(self, collection: AsyncIOMotorCollection) -> None:
        self._documents = collection

    @staticmethod
    def document_id(projection: str, key: str) -> str:
        return f"{projection}/{key}"

    @staticmethod
    def guard(record: Record) -> dict[str, Any]:
        """Match only documents that have not already folded in this version.

        `$not` over `$gte` also matches a document where the field is absent,
        which `$lt` does not. The alternative is an `$or` of `$exists` and
        `$lt`, and an upsert filter builds its new document from top-level
        equality only, so keeping the filter free of `$or` keeps what gets
        inserted obvious.
        """
        return {f"{SEEN}.{record.aggregate_id}": {"$not": {"$gte": record.version}}}

    async def apply(
        self,
        *,
        projection: str,
        record: Record,
        mutations: Sequence[Mutation],
    ) -> int:
        applied = 0
        for mutation in mutations:
            update: dict[str, Any] = {
                "$set": {
                    **dict(mutation.assign),
                    "_projection": projection,
                    "_key": mutation.key,
                    f"{SEEN}.{record.aggregate_id}": record.version,
                }
            }
            if mutation.inc:
                update["$inc"] = dict(mutation.inc)
            try:
                result = await self._documents.update_one(
                    {"_id": self.document_id(projection, mutation.key), **self.guard(record)},
                    update,
                    upsert=True,
                )
            except DuplicateKeyError:
                # The guard did not match, so the upsert tried to insert a
                # second document under an _id that already exists. That is
                # the already-applied path, not a failure.
                continue
            if result.modified_count > 0 or result.upserted_id is not None:
                applied += 1
        return applied

    async def fetch(self, *, projection: str, key: str) -> dict[str, Any] | None:
        return await self._documents.find_one({"_id": self.document_id(projection, key)})
