# event-store

Events appended one stream per aggregate, a version number that makes two
concurrent writers collide instead of overwriting each other, replay that
starts from a snapshot, and projections that can be re-run over the same log
without counting anything twice. MongoDB underneath, behind an interface whose
other implementation is a dict.

```python
from eventstore import Repository
from eventstore.stores import InMemoryEventStore
from ledger import LedgerAccount

accounts = Repository(InMemoryEventStore(), LedgerAccount, snapshot_every=50)

account = LedgerAccount.open("a-1", owner="Nadia", currency="GBP")
account.deposit(10_000, reference="salary")
account.withdraw(2_500, reference="rent")
await accounts.save(account)

reloaded = await accounts.load("a-1")
reloaded.balance  # 7500, folded back from three events
```

## The unique index is the concurrency control

```python
await collection.create_index(
    [("aggregate_id", 1), ("version", 1)], unique=True, name="aggregate_id_1_version_1"
)
```

That is the whole mechanism. `append` takes the version the caller loaded,
numbers its events from there, and inserts. If someone else has already written that
version, the index rejects the insert and the rejection becomes a
`ConcurrencyError`. There is no transaction, no per-aggregate lock, and no
read of the current version before the write, because reading the version and
then inserting is two statements with a gap between them and the gap is the
entire problem.

Two details decide whether it actually holds.

**`insert_many` does not raise `DuplicateKeyError`.** It raises
`BulkWriteError`, with code 11000 inside `details["writeErrors"][0]`. Catching
only `DuplicateKeyError` leaves a lost race, which is routine and retryable,
arriving as an unhandled error, which is a 500. In a log that prints the
exception class the two look the same. `duplicate_offset` in
`eventstore/stores/mongo.py` handles both shapes, and the write error's
`index` field says which event of the batch collided, so the error can name
the version.

**`ordered=True`, so a batch cannot be half written.** A plain ordered insert
stops at the first rejected document, which is enough here without a
transaction: versions are contiguous within an aggregate and every writer
starts from a version it read, so a batch can only ever collide at its own
first offset. `test_the_clash_is_always_on_the_first_event_of_the_batch`
is that argument written down.

## Replay has one rule and it is not obvious

`Aggregate.apply` folds one event into state. It runs on every load, over
events written by every version of the code that ever ran. So it must not
validate and must not refuse.

An `apply` that raises on a three-year-old event makes that aggregate
permanently unloadable, and no deploy fixes it, because the event is already
stored and the only way out is editing history. Refusal belongs in the command
method, which sees current state, decides once, and either raises before
anything is recorded or calls `record`.

Replay does refuse one thing: a gap. Folding versions 1, 2 and 4 produces
state that is wrong and looks completely normal, so a missing version stops
the load with a `ReplayGap` rather than being logged somewhere.

## Snapshots, so replay does not get slower forever

`snapshot_every` is a version interval. A snapshot is written on the first save
that crosses a multiple of it, which means the stored version is wherever the
aggregate landed and not the multiple itself.

`test_a_long_stream_loads_from_one_snapshot_plus_the_tail` appends 512 events
one save at a time with the interval at 50. It asserts 10 snapshot writes, a
latest snapshot at version 500, and that loading the aggregate afterwards reads
exactly 12 records. Without a snapshot store the same test asserts a load reads
all 60 of a 60-event stream.

A snapshot is a cache of a fold and never a source of truth. Dropping the
snapshot collection costs replay time and loses nothing. There is no schema
versioning on the stored state, so changing what `snapshot()` returns means
dropping the collection, which is a cheap operation precisely because the
events are still there.

Writing one uses an upsert whose filter requires a lower stored version:

```python
{"_id": aggregate_id, "version": {"$lt": new_version}}
```

When a newer snapshot already exists the filter matches nothing, so the upsert
tries to insert, and the insert collides with the `_id` that is already there.
`DuplicateKeyError` is the success path of somebody else's better snapshot, not
an error, and `save` returns False.

## Projections carry their own idempotency

A projection is a pure function from a record to a list of mutations. `plan`
gets the record and nothing else: no store, no target, no current document.
Two operations only, `inc` and `assign`.

The target applies them. Each projection document holds `_seen`, a map from
aggregate id to the highest version folded into it, and every update carries
the guard in the same statement as the change:

```python
criteria = {"_id": "currency_totals/GBP", "_seen.a-1": {"$not": {"$gte": 9}}}
update = {"$inc": {"net": 500}, "$set": {"_seen.a-1": 9}}
```

`$not` over `$gte` rather than `$lt`, because `$lt` does not match a document
where the field is absent, and the alternative is an `$or` of `$exists` and
`$lt`, which muddies what an upsert would insert.

The guard and the change land in one update, so they are atomic without a
transaction. The payoff is upstream: a change stream's resume token becomes a
performance concern instead of a correctness one. Lose the token, re-read from
further back, and the second pass changes nothing.

### Four things this design gives up

**Read-modify-write is not available.** Only `inc` and `assign`, because `inc`
commutes and a guarded `$set` is replayable. Anything that has to read the
document, decide, and write needs a lock, and no lock is implemented here. A
projection like "highest balance this account ever reached" does not fit.

**`_seen` grows on any projection keyed across aggregates.** One document per
aggregate holds one entry forever. `CurrencyTotals` in the ledger holds one per
account that has ever moved money in that currency, and never drops one. A
24-character id plus an int is around 30 bytes of BSON, so against the 16 MB
document limit a single such document takes something on the order of half a
million accounts before it stops accepting writes. That is arithmetic on the
BSON layout rather than a measurement. The ways out are keying the projection
per aggregate, or moving the guard into its own collection and paying for a
transaction.

**The guard is a high-water mark, so it is idempotent and not commutative.**
Re-delivering a record is free. Delivering one *late* is a silent lost update:
fed version 3 first, versions 2 and 1 are below the mark and get dropped, and
nothing raises.
`test_out_of_order_delivery_loses_updates_instead_of_repeating_them` pins that
behaviour down to a balance of -2500 where the right answer is 7500. Version
order per aggregate is therefore a requirement on the runner. Both feeds here
provide it, `read_all` by insertion order and a change stream by commit order.

**There is no way to re-apply on purpose.** The same mark that makes a repeat
harmless makes rebuilding one aggregate's slice of a projection impossible.
Rebuilding means dropping the projection collection and running catchup from
the beginning.

## An event sometimes has to repeat what the aggregate already knew

`Deposited` carries the currency, even though the account it belongs to has had
a currency since `AccountOpened`. That is deliberate. `CurrencyTotals` keys its
documents by currency and only ever sees money events, and `plan` cannot look
anything up. An event has to carry every fact its readers need, not just the
facts the aggregate needed in order to decide.

The cost is a real one: an account cannot change currency without a new event
type and a reader that understands both.

## Ordering across aggregates is not available

`read_all` pages by `_id` ascending, and the cursor is an ObjectId hex string
which sorts lexicographically. An ObjectId is a second-resolution timestamp
plus a per-process counter, so that order is monotonic per writer and not a
global sequence. Two aggregates' events can interleave differently between one
read and the next. One aggregate's own events will not, which is the only
guarantee the projection guard needs.

A true global sequence would need a counter document that every append
contends on. This repo does not have one, and does not pretend to offer
cross-aggregate causality.

## Layout

```
eventstore/
  events.py        Event, the wire-name registry, the stored Record
  aggregate.py     fold, queue, replay with a gap check
  repository.py    snapshot, then the tail, then the fold
  snapshots.py     the snapshot interface and both implementations
  errors.py        everything raised on purpose
  stores/          base.py is the interface, then memory.py and mongo.py
  projections/     Projection and Mutation, two targets, the runner
ledger/            a worked domain: four events, two read models, a demo
tests/             97 tests, none of which open a socket
```

`eventstore` exports five names, `Aggregate`, `Event`, `Record`, `Repository`
and `event`, plus the error classes. Adapters are reached through their own
submodule, so importing the domain side of this library pulls in no driver.

Events register under a string, not a class name:

```python
@event("ledger.deposited")
class Deposited(Event):
    ...
```

The string is what gets stored. Renaming the class is then a refactor rather
than an act that strands every event already written behind a name nothing
answers to.

## Running it

```bash
python3.11 -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/python -m ledger.demo
.venv/bin/python -m pytest -q
.venv/bin/ruff check eventstore ledger tests
```

`python -m ledger.demo` runs the whole library against the in-memory store:

```
a-1: 121 movements over 13 saves, balance 122140 at v122
refused  a-2 cannot pay 90000 minor units out of a balance of 25000
conflict a-2 was at a different version: expected 2, version 3 already exists

events stored: 128
snapshots for a-1: 2, latest v101
loading a-1: snapshot v101 plus 21 events replayed

catchup 1: 128 records, 253 applied, 0 skipped
catchup 2: 128 records, 0 applied, 253 skipped

currency_totals/GBP: net 147640 movements 123 seen 2 account(s)
```

The two catchup lines are the point of the whole repo. The same 128 records,
the second time, apply nothing.

### Against a real MongoDB

Not a runnable command here, and it needs a replica set, because `watch` reads
the oplog and a standalone `mongod` has none.

```python
from eventstore.projections.mongo import MongoProjectionTarget
from eventstore.snapshots import MongoSnapshotStore
from eventstore.stores import MongoEventStore, connect

db = connect("mongodb://localhost:27017/?replicaSet=rs0")
store = MongoEventStore(db.events)
await store.create_indexes()          # nothing above holds without this

runner = ProjectionRunner(MongoProjectionTarget(db.projections), [AccountBalance()])
await runner.catchup(store)                    # the backlog
await runner.consume(store.follow())           # then the change stream
```

`connect` passes `tz_aware=True`. Without it stored datetimes come back naive,
comparing one against `datetime.now(UTC)` raises, and the only code path that
notices is the one handling a late event.

## Tests

97, all offline, no Docker and no cloud account. `pytest -q` takes well under a
second.

Storage sits behind `EventStore`, and `InMemoryEventStore` is not a mock. It
refuses a key it already holds, which is what the unique index does, and it
inserts in version order stopping at the first clash, which is what the ordered
insert does. The concurrency and replay tests are therefore testing the real
rule rather than a stub of it.

The Mongo adapters are tested against a hand-written fake collection in
`tests/fakes.py` that records what it was asked to do. mongomock would hide the
thing worth asserting, which is the exact filter and update document. A guard
that silently stops guarding looks fine from outside: every number is still
plausible, just doubled. So `test_the_projection_update_carries_the_guard_and_the_change`
asserts the criteria dict literally.

Worth reading, in order:

- `test_the_second_writer_from_the_same_version_loses`, including that the
  losing batch leaves nothing behind
- `test_a_second_pass_over_the_same_log_changes_nothing`, which asserts 11
  mutations applied then 0 applied and 11 skipped
- `test_a_crash_between_two_documents_leaves_the_other_applicable`, where a
  target fails partway through one record and the retry applies only what is
  missing
- `test_out_of_order_delivery_loses_updates_instead_of_repeating_them`, which
  documents the limitation rather than hiding it

## Not built

- **No outbox.** Nothing publishes events to anything outside this process. A
  consumer reads the change stream directly, which couples it to the
  collection.
- **No upcasting.** An old payload is handled by giving the current model
  defaults, which covers added fields and not a renamed or retyped one. Real
  upcasting needs a per-type chain of migration functions.
- **No process managers.** Nothing reacts to an event by issuing a command to
  another aggregate, so there is no saga, no compensation and no timeout.
- **No archival.** Streams grow forever. Snapshots stop replay from getting
  slower; they do nothing about storage, and nothing here can delete an event,
  which also means there is no answer to a deletion request.
- **No global sequence**, as above.
- **Mongo is wired, not exercised.** The adapters' document shapes are
  asserted and their behaviour against a live replica set is not. Nothing in
  this repo has talked to a running mongod.
