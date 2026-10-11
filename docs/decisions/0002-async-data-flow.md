---
status: accepted
date: 2026-09-27
deciders: Michael Ries
---

# Data Flow: Synchronous Capture, Asynchronous Exactly-Once Derivation

*The design elements that carry the architecture are marked **Decision
point**. Logical-replication capture and apply-layer ordering of absolute
writes are rejected alternatives below, with the measurements that rejected
them.*

Trellis derives tables from other tables. Two questions decide the whole
design: *where* a derivation runs, and *how* a source change is captured and
ordered so that the derivation is applied exactly once. This ADR answers
both. [ADR-0017](0017-partitioned-sources.md) extends its capture and truncate
decisions to partitioned sources.

**Derivation is asynchronous.** Source changes are batched, and derived
values are computed and written by drain workers outside the application's
transactions, for the reasons in
[Derivation is asynchronous](#derivation-is-asynchronous).

**Capture is synchronous.** A source change is recorded into Trellis's
staging ring by an `AFTER … FOR EACH STATEMENT` trigger inside the writer's
own transaction. The ring row's transaction id is therefore the source
commit's, and that identity is what makes every derived write orderable.

**Ordering is owned by the target.** Every target keeps a ledger with one
entry per source row: the contribution the target currently counts for that
row, the ring position last applied to it, and the snapshot it was last
derived under. Every write that derives a value from a *read* rather than
from carried images locks the entry first and reads in the same statement as
its snapshot; every change is applied or skipped by checking its transaction
against that snapshot and its position against the last applied one. Group
values are sums of entries and are only ever incremented. A build is the same
operation over chunks of the source, and a definition applies changes from
its first chunk.

The reason for the ledger is the class of write that ordering by position
alone leaves unordered. A **delta** (`+f(new) − f(old)` into a group) commutes with every
other delta, so parallel workers may apply deltas in any order and the
exactly-once argument of
[stage 05](../staging-and-claiming/05-apply-and-exactly-once-deltas.md)
holds. An **absolute write**, any value derived from a read, does not
commute: it is right only if nothing later in the change order has already
been applied to the same row or group. Trellis has many producers of absolute
writes (an image-less recompute, the relationship reverse path, a projection
refresh, a go-live re-read), each reading live `READ COMMITTED` state and
each racing out-of-order parallel drains. Ordering them by position needs one
precedence rule per pairing of producers; one ordering
mechanism for every absolute write replaces those rules. My measurements also
found scaling cliffs in the alternatives, a logical-replication capture and a
one-pass build, that this design avoids:

- I measured a logical replication slot staging at most about 120k rows/s
  whatever the writers do, and it never captured a 10M-row `COPY`;
- I ran a one-pass go-live re-read at 100M rows on a 31 GB box and it could not
  finish, because a drain batch held its whole share of a segment in memory;
- I ran a relationship reverse path that keeps a settled projection as
  correctness state under to-side churn and it lost updates, never converging to
  the right target.

Conventions: **must** is a requirement the evidence forces; **should** is a
recommendation the evidence supports but does not force.

## Derivation is asynchronous

Update derived tables in the transaction that changes the source, or later,
in batches? Trellis derives **later**, and that choice shapes the user
experience.

Synchronous derivation (triggers or functions that write the derived table in
the source transaction) is consistent by construction and fast for shallow
same-row derivations, but it fails where Trellis's use-cases live:

- **Cross-relationship formulas are unsound.** Concurrent transactions don't
  see each other's changes until they commit, so a derivation that reads a
  related row inside the writer's transaction can compute from a value that
  is already stale when it commits.
- **No batching.** Five hundred updates folding into one aggregate row run
  five hundred derivations and serialize on that row's lock. Asynchronous
  batches collapse them into one write.
- **One failed derivation fails the whole write.** There is no way to
  quarantine the one broken dependent and let the other ninety-nine proceed.
- **Highly connected rows get slow.** Updating an account with millions of
  line items becomes a huge transaction on the application's commit path.

Asynchronous derivation keeps derivation cost and latency off the
application's write path, batches N source changes into M target writes,
quarantines a failing dependent instead of failing the write
([ADR-0003](0003-quarantine-storage-and-api.md)), and lets Trellis lag under
load rather than slow the source. Its costs are the ones Trellis documents:
derived data is eventually consistent, with `await_converged` as the
read-your-writes primitive
([stage 07](../staging-and-claiming/07-convergence-and-await.md)); errors are
reported through an API rather than at write time; and calculated columns live
on a neighbour table.

Only *derivation* is asynchronous. The next section moves *capture* into the
writer's transaction, and the objections above do not apply to it: capture is
one append of the key and the read columns, it reads no related row, it
serializes on nothing but the ring's append path, and a derivation failure
still quarantines. Two objections do carry over to capture and are taken on as
invariants below: a disabled trigger strands data (I6's audit), and a
Trellis-side schema change must not fail the application's statement (I6's
never-fail rule).

## Invariants

The design is these invariants; every element below exists to hold one of
them. I0 to I5 are the ledger's ordering rules as my experiments
refined them; I6 to I8 are the rules the capture and build experiments forced.

| | Invariant | Evidence and refinements |
|---|---|---|
| **I0** | **One database.** Sources, the ring, every ledger and every target live in one Postgres database, so one snapshot orders every commit Trellis will see. | Design premise; made exact by trigger capture, which makes a ring row's transaction the source commit's. Nothing here works across databases. |
| **I1** | **Read after lock.** Every live read that feeds an absolute write happens after the writer holds the lock on the ledger state it will write, and the snapshot it stores is taken **in the same statement** as the read. One write reads first: a build chunk's insert of the entry of a key that has none, from a read in the same statement. Its uniqueness check comes after the read, and finding no entry there means no Apply has written the key since the read's snapshot, because the tombstone GC, the one thing that removes an entry an Apply wrote, skips a ledger under a build (I4). A page deletes only a placeholder it wrote no change to, which holds no applied change, and a source truncate commits before the chunk's first read, which holds the source's lock, so it empties no change the snapshot doesn't see. A key that has an entry by then is locked and read afresh. The rule needs `READ COMMITTED`, where each statement reads from a snapshot of its own, so a read after the lock sees what the lock waited for; at `REPEATABLE READ` or `SERIALIZABLE` it would read from the transaction's first snapshot, taken before the lock. Every session Trellis opens sets `default_transaction_isolation` to `read committed`, whatever the server's, database's or role's default, and a transaction that needs one snapshot for several reads asks for its level explicitly. | In my interleaving experiment (seventeen hand-driven scenarios of Apply racing a Re-derive and other producers), the scenarios where a stalled Re-derive meets a change to the same key, committed before or after its snapshot, hold, and Apply demonstrably blocks on the entry lock. In a snapshot-timing experiment I measured a snapshot taken in a separate statement differing from the read's in 99.7% of samples under load; the stored basis is the full `pg_snapshot`, not `xmin`/`xmax`. |
| **I2** | **Visibility-checked application.** A change C for row r is applied iff C's transaction is **not** visible in r's basis snapshot **and** C's ring position is above r's `applied_lsn` (and above the target's truncate floor, [Truncate](#truncate-ddl-drop)). Skipping is exact, never "maybe counted, re-derive". | In the interleaving experiment, skip-iff-visible alone fails three scenarios (same-key order across batches is not decidable from visibility); stamping Apply's own snapshot fails two more; visible-or-`applied_lsn` passes all seventeen. An in-flight id is decidable from the stored list (0 disagreements over ~1.1M pairs in the snapshot-timing experiment); my 10M chunked-build runs saw 0 in-progress cases. |
| **I3** | **Per-row ordering state; groups are pure sums.** The ledger entry is the only place a row's applied contribution and group live. A group value is the sum of its entries' contributions, so group updates commute and a group row is only ever incremented. A relationship's parent is read under the child's entry lock ([Relationships](#relationships-the-parent-is-read-under-the-childs-entry-lock)). | In the interleaving experiment, eight workers creating the same groups at once, and a row passing through a group within one folded batch while that group rebuilds, both hold with groups as sums and no pre-lock or probe. |
| **I4** | **Tombstones live until the batch watermark passes.** A deleted row's entry stays, with its `applied_lsn` and `applied_seg`, until every batch at or below its `applied_seg` is fully drained ([Convergence and status](#convergence-and-status)). | In the interleaving experiment an older update resurrects a deleted row unless the tombstone is kept. The batch-watermark form is exact under triggers because a same-key predecessor of a delete committed before the delete's trigger ran. |
| **I5** | **One lock order, taken as one sorted batch.** A page's 1-1 targets, then its aggregate targets, each in target order; per target its ledger entries, then its rows or groups, each locked in key order in one statement per class. Never a per-row loop. | In the interleaving experiment a per-row loop deadlocked 19–22 times in 20 s; the sorted batch never did. The implemented paths held it in my benchmark round (no deadlock in any benchmark, and group moves and the hot-key case converge with none). The unbuilt factored relationship layout is the one variant that deadlocked ([Relationships](#relationships-the-parent-is-read-under-the-childs-entry-lock)). |
| **I6** | **Never block, and never fail, an application writer.** No Trellis transaction takes a lock an application write can queue behind, except the join and drop fences, which are bounded by `lock_timeout` and retried. A schema change to a read column never fails the application's statement. A missing ring table or a revoked privilege does, loudly ([Consequences](#consequences-and-costs)). | I measured a bare `CREATE TRIGGER` stalling every writer for 25 s; with a 50 ms `lock_timeout` retry the worst wait was 52 ms. I renamed a read column under the capture function and every insert failed until regeneration. The retire path already takes its `TRUNCATE` lock `NOWAIT`. |
| **I7** | **No Trellis transaction waits for a lock while it holds a snapshot open.** Every lock statement runs under a `lock_timeout`; on timeout the transaction rolls back and the work is retried from outside any transaction. | In my 100M chunked-build run, a drain batch's ledger insert waited 1 h 50 min behind chunk transactions; the open transaction held back vacuum and the sealer for the whole wait. |
| **I8** | **Memory is bounded by a batch cap, never by segment size.** A drain batch folds, re-reads and applies at most a fixed number of changes and pages through a larger bucket. | In my one-pass build, a drain held ~650 B per staged row per worker: 6.32 GB at 10M, OOM at 100M; fewer workers bound nothing because each worker then holds more buckets. |

## Capture by statement triggers

**Decision point.** Trellis code runs inside the application's transactions.
What it does there is one append.

- **Must:** five statement triggers per captured table: `AFTER` insert,
  update, delete (a trigger with transition tables takes one event) and
  `TRUNCATE`, each appending the statement's rows to the active ring segment in the
  writer's transaction. The ring row's `row_txid` (`DEFAULT
  pg_current_xact_id()`) is therefore the source commit's `xid8`, and it is
  the identity I2 checks. *Evidence:* I measured statement triggers beating
  row triggers 5–10x above 1 row/commit; capture scaling with writers to
  ~1.9M rows/s at 1,000 rows/commit and ~190k at 1 row/commit; and, at 1
  row/commit on disk, the tax vanishing under a real fsync (within 1% of no
  capture at 16 writers).
- **Must:** the generated function names only the primary key and the
  columns the table's definitions read (plus relationship `from_col`s), emits
  **NEW-only** images (a delete emits the key), renders every value with
  `format('%s', col)` under five output settings (`datestyle`,
  `bytea_output`, `extra_float_digits`, `intervalstyle`, `timezone`) pinned as
  `SET` clauses on the function, carries
  a `WHEN` clause that skips an update touching no read column, is
  `SECURITY DEFINER` owned by the Trellis role (the one role that owns
  Trellis's schema and performs every Trellis operation) with
  a pinned `search_path`, is `ENABLE ALWAYS`, and reads the active slot
  schema-qualified through the sequence mirror
  ([The seal fence](#the-seal-fence-under-application-writers)). *Evidence:*
  I compared image encodings across 38 type families: only `format()` and
  `hstore` with pinned settings are byte-identical to `tuple_to_json`;
  `::text` and `to_jsonb` are not; an unpinned image silently depends on the application session's
  `DateStyle`, `TimeZone`, `bytea_output`, `IntervalStyle` and
  `extra_float_digits`; an encoding that reads every column costs 3.3 ms and
  323 KB per row on a 100 KB column, 40x the control, where naming only the
  read columns costs +30 µs. NEW-only is exact because the ledger holds the
  old side. The `WHEN` clause is a filter inside the function (a statement
  trigger with transition tables can't take a `WHEN`): an update row whose
  imaged columns are all unchanged (compared over the typed values) stages
  nothing. The NEW image is the **live row** whenever it can differ from the
  transition row: the fifth trigger, `<schema>_capture_begin` (`BEFORE … FOR
  EACH STATEMENT`), marks each statement's span, and capture re-reads its
  rows by primary key only when another write to the same table ran inside
  that span (a nested same-key write), or when an update's old rows hold a
  key twice (an FK action's update merged into the same capture call).
  Otherwise it images the transition tables and reads no relation. An
  unconditional re-read took btree predicate locks and failed 50–87% of
  concurrent `SERIALIZABLE` auto-increment inserts with `40001`; gated, the
  rate is 0, as with no capture. **The ring still carries the old image:**
  every remaining reader of it is relationship machinery (the to-one
  projection and guards, the to-many reverse path, the generation bump,
  prior-image hints). An aggregate Apply takes a key's old group and
  contribution from its ledger entry, not from the image. On the privilege
  model, I checked that the application needs nothing on Trellis's
  schema. The Trellis role must own each source table or be a member of its
  owning role, because `ENABLE ALWAYS` needs ownership, not just `TRIGGER`.
  It needs no superuser.
- **Must (I6):** `CREATE TRIGGER` and `DROP TRIGGER` run under a short
  `lock_timeout` in a retry loop, one attempt per interval, until they land.
  *Evidence:* the `lock_timeout` retry measurement under I6.
  The join's commit is also the **join fence**: `SHARE ROW EXCLUSIVE` waits
  out every writer in flight, so every commit either precedes the trigger
  and is visible to any snapshot taken after it, or ran the trigger. No
  separate fence, marker or generation is needed to make a table's join
  gap-free.
- **Must (I6):** a schema change to a read column never fails the
  application's write. The generated function counts, in the
  empty-statement guard's query, how many of its imaged columns the table
  still has. On a miss it appends a *schema-changed* marker row naming the
  missing columns and images the rows over the columns left, by `EXECUTE`
  (if a key column is gone, it writes the marker only). It has no `EXCEPTION`
  block, so no subtransaction per statement. The drain that meets the marker
  pauses every definition reading a missing column with a status error before
  it applies anything later, and the staging worker's reconcile regenerates
  the function over the columns the remaining readers need. Definitions that
  don't read a missing column keep applying. *Evidence:*
  I ran schema changes against the capture function: rename or drop of a
  named column fails every insert until regenerated; add, rename or drop of
  any *other* column is harmless. The guard's column count costs +3.4–5 µs
  per single-row statement. I also found event triggers unavailable on some
  hosts, which is why regeneration does not depend on one.
- **Must:** the self-check audit
  ([ADR-0013](0013-self-check-production-recompute-audit.md)) verifies from
  `pg_trigger` that every captured table's triggers exist, are enabled
  `ALWAYS` and call functions owned by the Trellis role, that the role still
  holds the privileges the functions use, and that the table hasn't joined a
  partition or inheritance hierarchy, and reports any fault before any
  recompute comparison (five triggers, one Trellis
  role). Replica-mode sessions are covered by
  `ENABLE ALWAYS`; an owner who disables or drops the trigger by name is
  documented as uncaptured until the audit runs (see also the known
  correctness gaps
  [Capture switched off and back on between two reconcile passes](../known-correctness-gaps.md#5-capture-switched-off-and-back-on-between-two-reconcile-passes) and
  [A capture function body replaced by hand](../known-correctness-gaps.md#6-a-capture-function-body-replaced-by-hand)).
- **Only the staging worker creates or drops triggers**, from the catalog
  alone, on its reconcile pass: a table gains triggers when something
  registers a reader of it and loses them when its last reader is dropped.
  Registration itself does no source reads and no capture work; it writes
  the definition and its ledger tables, and the staging worker starts its
  build once capture covers it
  ([A build](#a-build-is-re-derive-over-chunks-and-applies-from-its-first-chunk)).
  Registering processes need catalog access and the right to create target
  tables, nothing on the sources.
- There is one capture path. Trellis has no replication slot, publication
  or `wal_level = logical` requirement, and no second path needs its own
  proof of the ordering argument.
- **Cost to state.** On disk at 1,000 rows/commit with 16 writers the
  writer's throughput is 0.49x no-capture and the ring row is ~2.9x the
  source row's WAL bytes (I measured both on disk);
  the NEW-only and `WHEN` cuts are unmeasured. No synchronous capture can
  lose less than 25% at 1,000 rows/commit, so the bar is: batched writes cost
  no more than one expression index's CPU per row; p99 commit latency at 1
  row/commit rises by less than 2.5x on tmpfs and is unchanged on disk. A
  bulk-load pattern (drop the triggers, load, rebuild the readers) is
  documented and is exact under the build below.

### The seal fence under application writers

The ring, the two-phase seal and its fence
([stage 02](../staging-and-claiming/02-the-staging-ring.md),
[stage 03](../staging-and-claiming/03-sealing-and-the-fence.md)) carry over.
A ring writer is the application, at whatever isolation level it chose, not
a Trellis `READ COMMITTED` transaction.

- **Must:** the writer reads the active slot through the sequence mirror
  (`ring_slot_mirror`, `pg_sequence_last_value()`), which the seal sets in
  phase 2 before the `xmax` bump, and the writer's xid is assigned in the
  same statement as that read. *Evidence:*
  I ran `REPEATABLE READ` and `SERIALIZABLE` writers across seals: one read a
  pointer two flips old from `segment_pointer` and its rows landed in a slot
  no batch would read.
  Stage 03 states the proof for the mirror.
- **Must:** the truncate barrier decides `has_truncate` after the batch's
  contents are fixed.
- **Per-key order is `(lsn, change_id)`** with `lsn =
  pg_current_wal_insert_lsn()` read in the trigger; the fold's ordering rule
  is unchanged. Cross-key commit order is not available from triggers and,
  under I3, not needed. *Evidence:*
  I ran concurrent writers on one key: per-key order held in every
  interleaving; a `CACHE`d sequence alone
  inverted a key's order; with the LSN it holds because a second writer on
  the same key runs its trigger only after the first commits.
- The maintenance loop is not "the only sealer" in any argument here; nothing
  below relies on what drains before or after a seal.

## The ledger: one target-owned entry per source row

**Decision point.** The target, not the ring and not a projection, owns the
state that orders its writes.

- **Must:** per aggregate target, `<target>__ledger`, one entry per source
  row, keyed by `__from_key` and indexed on the group key: the target's own
  typed `GROUP BY` columns; `contrib`, one typed column per distinct
  aggregate argument (`__arg<n>`, shared by every field over the same
  argument, none at all for `COUNT(*)`) plus a `__member` flag; and the
  ordering state `__applied_lsn`, `__applied_seg` (the tombstone GC
  watermark, I4), `__basis` and `__tombstone`. It is
  written in the same transaction as the target, entries locked in key
  order before any group row is touched. *Evidence:*
  I prototyped this schema in a ledger prototype.
- **Must:** a 1-1 target's ledger is a slim side table,
  `<target>__ledger(from_key primary key, applied_lsn, applied_seg, basis,
  tombstone)`, beside an unchanged target table; the target row holds the
  values. A tombstone row in the target would leave a deleted row readable
  after `await_converged` until GC runs; a tombstone in the side table gives
  a never-seen key a single row to lock. The side table is one lock domain
  per key (a placeholder insert, then a sorted `for update`). Its cost is
  one more row write per change: ~200 B of WAL per row in `throughput-ramp`
  (about 500 B right after a checkpoint, with its full-page images), and
  the placeholder insert settles a new key's Apply on its own.
- **Must:** exactly two operations exist, and every producer is one of them.
  - **Re-derive(r).** Lock r's entry (inserting a placeholder if absent;
    a build chunk instead inserts an absent entry from its read, I1).
    In one statement: take the snapshot, read r's source row and, through
    each relationship, the parent's current values (read live, after the
    lock), compute the group key
    and contribution. Diff against the entry: subtract the old contribution
    from the old group, add the new to the new group, replace the entry with
    the new values and the new basis, and leave `applied_lsn` as it was. If
    r no longer exists in its source, the entry becomes a tombstone and its
    contribution leaves its group. It does not store the read's position as
    `applied_lsn`: a ring row's position is the writer's pre-commit insert
    position, so a change whose trigger ran before the read but which committed after it
    would sit below that position, invisible to the basis, and be skipped
    for good. The basis alone records what the read saw: every change it
    saw is refused by visibility, and every change it missed is still
    pending under its own ring row. The exact point a read stores is
    therefore the full `pg_snapshot` taken in the same statement as the
    read (I1), on every entry that read wrote; a read in keyspace chunks
    stores each chunk's own, which a test of chunked keyspace reads
    checks.
  - **Apply(r, C, image).** Lock r's entry. If C is visible in the entry's
    basis, or C's position is at or below `applied_lsn`, stop (I2).
    Otherwise `delta = f(NEW) − entry.contrib`, group += delta, entry :=
    (new group, new contribution, C's position, basis unchanged). A delete
    is `f(NEW) = 0` and the entry becomes a tombstone. A row with no entry
    is an insert of NEW, which the lock's own insert writes as the entry
    (for a 1-1 target and an aggregate ledger alike, except one that reads a
    relationship, whose parents are read after the lock).

  | Producer | Operation |
  |---|---|
  | Captured insert/update/delete | Apply |
  | Re-derive build, resume rebuild, `request_backfill`, quarantine release, column resume, `ALTER TRANSFORM` added column | Re-derive over the definition's key space, chunked |
  | Relationship parent change | The parent operation below; per-child Re-derive only for non-linear fields |
  | To-side `TRUNCATE` | Parent operation with `f = 0` for every parent of the table; the join-key index names the children |
  | Source `TRUNCATE` | The ledger is emptied, the target's truncate floor is raised to the truncate's ring position, and the target is cleared ([Truncate](#truncate-ddl-drop)) |
  | Chained hop | Apply, where C is the upstream apply transaction ([Multi-hop](#multi-hop-a-targets-writes-reach-its-readers-through-the-mutation-seam)) |

- **Must keep contributions.** A membership-only ledger (group key and basis,
  contribution recomputed from source) saves 1–2% of WAL and produced wrong
  relationship targets on both shapes I ran it on.
- **Non-invertible fields** (`MIN`, `MAX`, `BOOL_AND`, composed expressions)
  are recomputed from the ledger's contributions by an index scan on
  `group_key` under the group's lock, never from the source. This is the one
  place a group value is written absolutely, and it is ordered by the same
  ledger locks as every increment. A build's merger folds a group it only
  added entries to instead: each `MIN`/`MAX`/`BOOL_AND`/`BOOL_OR` becomes
  itself over the stored value and those entries' current contributions,
  read by key under the same group lock. A group a value may have
  left, or whose row the merge creates, is recomputed.
- **Cost to state.** I measured the implementation
  against the old aggregate path on trigger capture, same session, 8
  workers: the 40k- and 400k-group shapes drain (the old path never did),
  4k groups is 19–32% faster end to end, a 5,000-row page cap costs
  1.4–1.5x instead of 6–8x, and the 10M build converges in 65 s against
  791 s, with no deadlocks anywhere. **The cost bar is missed** at a high
  fold-in ratio: 400 groups is 33–57% slower in-window (39–51% end to end),
  because the old path wrote one group row per group per page and the
  ledger writes one entry per source row. WAL per folded row is 2.5–2.6x
  the old path's in that measurement, taken before Apply's updates were made
  HOT-eligible, and Postgres CPU per folded row
  1.8–2.6x.
- **Ledger size and WAL.** The ledger is 2.6–3.1x a narrow source table on
  disk (~100 B per entry) and adds 1.5–1.8x a ledger-less apply's WAL, 40–45%
  of the ledger's share being full-page images after a checkpoint. `contrib`
  is not narrowed further: the entry is dominated by the ordering state
  (`__basis` averages 44 B, the tuple header 24 B), not the contribution.
  Only a tombstone carries `__applied_seg` (I4), so an Apply that leaves an
  entry live changes no indexed column and can be HOT, and ledgers stay at
  `fillfactor` 100 (80 gained nothing on the round's loads and cost a 10–16%
  larger ledger and a 4–9% slower 10M build). A page's Apply of a key with
  no entry writes the entry in the insert that locks it. No separate
  tablespace is documented: the ledger is sized like any other derived
  table, and nothing measured shows its I/O needs a device of its own.

### Apply is one set-based statement per batch per target, and the batch is bounded

- **Should:** with `row_txid` exact, I2's skip rule is one predicate over the
  batch joined to the ledger, so a batch's Apply on a target is: one sorted
  lock statement over the entries it touches, one `update … from batch`
  that computes deltas and rewrites entries, one sorted upsert of group
  increments. The fold's "mixed visibility → re-derive" case disappears for
  image-bearing changes.
- **Must (fold):** an image-less `op = 'delete'` is the key's final state
  within a fold window, whatever precedes it. *Evidence:*
  in a 1M-row build dry run under NEW-only capture, a delete sharing a fold
  window with an earlier write of the same key was dropped by the "latest row
  with any image" rule, leaving 11–12 groups wrong.
- **Must (fold):** the fold statement never plans a nested loop over stale
  ring statistics.
- **Must (I8):** a batch folds, re-reads and applies at most a fixed number
  of changes (a per-batch cap on folded records, on the order of 10^5) and
  pages through a larger bucket in key-range pages; per-key `(lsn,
  change_id)` order holds inside the fold. Pages are keyset ranges on
  `(route, src_table, key)` with a truncate sentinel first, so a key never
  splits inside a segment and the fold's whole-window rules (first old
  image, last new image, an image-less delete ending the key) hold per page
  unchanged. Cross-key order is arbitrary, which I3 allows, so this order is
  permanent: the ledger would make split keys safe (Apply is per-key ordered
  by `applied_lsn` and takes its old side from the ledger) but does not need
  them. Each page is its own transaction behind a claim check, advancing a
  durable per-bucket cursor; only a bucket's last page marks it drained.
  *Evidence:* the one-pass build's memory measurement under I8.
- **Must (I7):** every lock statement in Apply runs under `lock_timeout`; a
  timeout releases the claim's transaction and retries the page after a
  backoff, outside any transaction.

## Relationships: the parent is read under the child's entry lock

**Decision point.** An aggregate that reads a to-one relationship, as a field
or a `GROUP BY` key, is on the ledger like any other. Its Apply and Re-derive
statements left-join the to-side and read the parent live, after the child's
entry lock and in the same statement as the child's own read (I1).

- **Must:** a parent change stages an image-less recompute for each child it
  reaches (only when some definition on the from-table reads the
  relationship), and the ledger re-derives each child's entry. A child that
  has since moved off the parent has a change of its own pending, whose write
  reads its new parent. A relationship can therefore never double count or
  drop a child: the entry records the group the child was last counted under.
- **Invariant: a value derived through a relationship is read after the
  entry lock it is written under.** Every write of such a value reads the
  related rows after taking the entry lock it writes under, in the same
  transaction. A read made before the lock may feed evaluation or planning,
  but never the value written, except a page's Apply's, which the next
  bullet orders by I2 instead. A writer can sit between an early read and
  its lock for a long time (a stalled page, a chunk between its plan and its
  lock), and a parent change and the page of the recompute it stages can
  both commit in that window. The early read would then be written over the
  newer value, and nothing would heal it: the parent change has drained, and
  the child has no change left to apply.
- **Must:** a 1-1 target that reads through a relationship reads the
  relationship projection. An Apply evaluates its change's image against the
  projection the page resolves in Phase 2
  ([stage 05](../staging-and-claiming/05-apply-and-exactly-once-deltas.md)).
  That read is not the value of record. While the Apply's change is in
  flight, guard (c) defers the reverse of a to-one parent change that
  commits after the read, until the reverse's fairness escalation advances
  the projection anyway. Either way the reverse stages a recompute of the
  child, and a to-many relationship's to-side change stages one at once.
  The recompute's Re-derive reads the child's row after the Apply's change,
  so I2 refuses the Apply if it lands later still.
  A Re-derive, a page's or a field build chunk's (a column resume), reads the
  projection after the entry lock and its read of the rows, for exactly the
  rows it read, by the projection's primary key. A parent change whose
  reverse commits before that read is in it; one whose reverse commits after
  stages a recompute that waits on the entry lock and writes after it. The
  exception, for an Apply and a Re-derive alike, is a child that joins a
  parent while the parent's reverse page is uncommitted, after that page
  found the parent's children: the child reads the old value, and the
  reverse's recompute doesn't reach it (see
  [known correctness gaps: a to-one relationship field keeps a superseded
  parent value under concurrent writes](../known-correctness-gaps.md#21-a-to-one-relationship-field-keeps-a-superseded-parent-value-under-concurrent-writes)). A
  Re-derive whose row joins a parent outside the set the page's Phase 2
  bumps the generation of is harmless: guard (b) only defers a parent's
  reverse, and the reverse's recomputes come from a live read of the
  from-side under its own lock.
- **Replica identity is not a requirement on any table.** The ledger holds
  the group a child was last counted under, so a from-side FK re-point needs
  no pre-image, and a to-side change needs only its key. This replaces
  [ADR-0006](0006-relationships.md)'s replica-identity gates.
- **Cost.** A parent change with fan-out N rewrites N child entries, where a
  factored layout (a per-`(group, parent)` partial count `P` and a parent
  table `T` of applied to-side values, so a parent change never touches a
  child) writes one row per group. I
  prototyped the factored layout: the oracle matched on all six shapes and
  five converged in 21 s against the per-child path's 24–113 s, but it
  deadlocked 9–16 times per 100k-fan-out run on disk and never on tmpfs, and
  the cycle was not found. It is not built.

## A build is Re-derive over chunks, and applies from its first chunk

**Decision point.** A new or rebuilt definition is not built by one
set-based pass with its changes withheld and recovered afterwards. It is
built by the same operation apply uses, over chunks of the source, while
applying.

- **Must:** registration writes the definition and its ledger tables. Once
  capture covers it, the staging worker starts its build in one transaction
  that moves it to `backfilling` and enqueues a plan job, and the definition
  is **applying from that commit** (registration does no more than register). The plan job enqueues the chunk plan
  (primary-key ranges over the source, discovered by `max()` over `LIMIT` so
  every row falls in exactly one range) in batches, on a drain worker. Every
  drain worker claims chunks. A chunk
  is one short transaction. One statement takes the snapshot, reads the
  range and inserts, in key order, the entry of every key that has none,
  written from that read, and records their group deltas (next bullet).
  Then the chunk locks the other keys' entries in key order, and one more
  statement takes a snapshot, reads them, replaces their entries and
  records their deltas. "No entry" means "not yet counted": a captured
  change for a key whose chunk has not run applies as an insert of NEW, a
  delete of one records a tombstone, and the chunk's insert then finds that
  entry and leaves it to the locked Re-derive, which replaces it under a
  snapshot that includes that commit, so the group ends up counted once.
  The insert's snapshot is taken before its uniqueness check, so the insert
  relies on "no entry" meaning "no Apply since the snapshot". The tombstone
  GC is the one thing that removes an entry an Apply wrote, and it skips a
  definition under a build (I4); a page's deleted placeholder holds no
  applied change (I1).
  There is no go-live re-read, no orphan sweep and no catch-up: nothing was
  skipped. Batches drain out of order, so a change committed
  before the start can drain after it while a later change to the same key
  drained before it, unapplied. Its image is then stale, and a key deleted
  that way has no row for a chunk to find. The start records the segment
  active at its commit (seal phase 1 can't move it until the start commits),
  every change committed before the start is in a batch at or below it, and
  a page re-derives rather than applies the keys of such a batch. *Evidence:*
  in four 10M-row runs of a chunked-build prototype under 2,000 writes/s the
  oracle matched, with 196–361k changes correctly skipped by chunk bases and 0
  in-progress ids.
- **A source that is another definition's target takes a per-build fence.**
  Such a source is fed by the mutation seam ([Multi-hop](#multi-hop-a-targets-writes-reach-its-readers-through-the-mutation-seam)),
  not by capture. A seam writer decides inside its own apply transaction
  whether a definition applies, so one that decided before the build's start
  committed can commit after a chunk's snapshot, and neither the chunk nor
  the ring carries its write. The plan job's first act is a fence, the id of
  a transaction begun after the start commit (the marker fence's rule),
  and it plans no chunk until the oldest running transaction began
  after it. That covers a writer only if it held its transaction id when it
  checked, so the seam's check takes the id first, in a statement of its
  own. Only a seam-fed build waits, and only on older transactions;
  `status` names the fence (`BuildWait::Fence`). The seam stays, so this
  fence stays. *Evidence:* a test freezes an upstream writer after its reader
  check and starts the downstream: without the fence the downstream misses the
  writer's row.
  A source that is another aggregate's target is keyed by its nullable
  `GROUP BY` columns, and a key with a `NULL` part is in no chunk's range
  (the range is a row comparison). An aggregate's plan job stages an
  image-less `Recompute` for each such key when its walk ends, which a page
  re-derives under the entry lock. Those rows can still be in the ring when
  the build flips `live`; their origin is unknown, so a token taken after the
  flip waits for them, as for any seam row.
- **Builds that keep the one-pass path.** The Re-derive build serves an
  aggregate or 1-1 definition on a captured source or on another
  definition's target. Two kinds keep the one-pass build: a definition with a
  relationship path in any field (1-1 or aggregate), and a 1-1 definition
  whose alias chain is cyclic. They build in one pass, with a fence wait and a
  go-live catch-up that re-reads every table the build read and sweeps the
  target for unbacked rows (the definition reports `catching_up` meanwhile).
- **Must (I3):** group application during a build is a **separate,
  commutative, batched step**. A chunk records its group deltas without
  locking any group row, and a merger applies accumulated deltas to group
  rows in key order as one sorted upsert. The deltas are rows in a
  per-target `<target>__deltas` table, each carrying its group's merge
  partition, and the merger is any drain worker claiming a partition's rows:
  this needs no new locks, keeps I5, and survives a crash because the delta
  rows are durable. *Evidence:*
  in my 100M chunked-build run, a 100k-row chunk touched ~63% of 100k groups,
  so chunk transactions that lock groups serialize on them; at 10M, 10x larger
  chunks cut define-to-live by only a third.
- **Must (I7):** chunk transactions are short (one range, no group locks,
  under `lock_timeout`), and a captured-change batch never waits inside its
  transaction for a chunk's lock.
- **Must (backpressure):** build work yields to sealing and to the drain of
  captured changes. A worker claims a chunk only when the ring's undrained
  backlog is under a bound, and the sealer is never refused for the duration
  of a build. *Evidence:*
  in my 10M chunked-build runs without backpressure the seal was refused for
  the whole build, with a tail of 90–213 s after `live`; in the 100M run a
  6.09M-row segment sat undrained.
- **Must (acceptance):** per-row build cost is independent of table size;
  a build of N rows runs in bounded memory and bounded WAL retention, in
  time linear in N, and converges to the oracle at 100M under load.
  *Evidence:* per-chunk cost grew from 3.2 to 5.7 worker-seconds per 10k
  rows between 1M and 10M, and the chunk rate fell from 0.96 to 0.4/s at
  100M.
- **Should:** the peak `xmin` hold during a build is one chunk's duration,
  not the build's (270–658 s at 10M in the prototype).
- **Chunks are a durable, claimable work queue on drain threads**, not an
  in-call loop, and the failure contract is: a chunk is heartbeated and
  reclaimed like a sealed segment, a crashed worker's chunk is re-run
  (Re-derive is idempotent), a failing chunk backs off with its error on
  `Trellis::status` and never starves other work, a resume supersedes held
  chunks through the claim fence so no stale write lands after it,
  and no definition is ever `backfilling` without a chunk row or delta row
  driving it forward. A staging worker is still required for anything to
  seal; drain threads are required for anything to build.
- **Cost to state.** At 10M with the prototype's shape (group locks inside
  chunks), define-to-live was 2.0–3.2x the one-pass build's and WAL 3.7–5.1x.
  The implementation's 10M build converges in 65 s against the old path's
  791 s; the acceptance bullet above stays the bar
  ([validation 3](#validation-and-acceptance)).

| Build at 10M rows, 8 workers, 2,000 writes/s, disk | define → `live` | WAL | oracle | tail after `live` |
|---|---|---|---|---|
| one-pass `GROUP BY` + go-live re-read | 226 s | 5.8 GB | ok | 153 s |
| prototype, 10k-row chunks | 672–716 s | 29.3–29.8 GB | ok | 90–213 s |
| prototype, 100k-row chunks | 450 s | 21.7 GB | ok | 61 s |
| one-pass at 100M | 2,305–2,322 s | – | none: OOM in the drain | – |
| prototype at 100M | never: 4,131 of 10,001 chunks in 2 h 8 min | 190 GB retained | none: stopped by the disk guard | – |

## Convergence and status

- **Should:** the watermark token is `pg_current_wal_insert_lsn()` read after
  the caller's commit, and `origin_lsn` is stamped from the trigger's
  pre-commit `pg_current_wal_insert_lsn()`. A commit the caller could see
  before taking its token has its ring rows in the same transaction, each
  with `origin_lsn` at or below the commit's position, so the predicate of
  [stage 07](../staging-and-claiming/07-convergence-and-await.md) over ring
  rows alone is sound; a transaction straddling the token over-reports, the
  safe direction. The token is the insert position because the write
  position, `pg_current_wal_lsn()`, lags a commit made with
  `synchronous_commit = off` and can sit below its rows' `origin_lsn`. There
  is no capture watermark to check.
- **Must:** three stored statuses: `waiting_to_backfill` (registered, chunk
  plan not yet written; normally momentary), `backfilling` (applying, chunks
  or deltas outstanding), `live`. A one-pass build adds
  `catching_up` (applying, awaiting its go-live re-read). Plus `paused` and
  `quarantined` from
  [ADR-0014](0014-pause-and-drop-a-transform.md) and
  [ADR-0003](0003-quarantine-storage-and-api.md).
- **Must:** a `live` reader of an upstream (another definition's target it
  reads directly or through a relationship) that is not `live` reports the
  upstream's state, transitively, computed when status is read and never
  stored.
- **Must (I4):** tombstone GC by a batch watermark. Every entry records
  `applied_seg`, the newest segment (`seg_seq`) whose changes it reflects,
  and maintenance deletes each tombstone whose `applied_seg` is at or below
  the **contiguous drained prefix**: the highest `seg_seq` at or below which
  every segment is drained. The
  prefix, not the segment just drained, because segments drain out of order
  and a fence is a snapshot, not a position. A same-key predecessor of a
  delete committed before the delete's trigger ran, so it is in the delete's
  batch or an earlier one, and at or below the prefix it has been applied or
  skipped. A Re-derive's read is live, so a page draining an old batch can
  see a later batch's delete; a Re-derive therefore stamps at least the
  newest segment its snapshot sees. Every change the snapshot saw is in that
  segment or an earlier one, so the tombstone outlives every change its
  `basis` would refuse. Only a tombstone carries the stamp: a write that
  leaves an entry a tombstone raises `applied_seg` as above, and one that
  leaves it live leaves it alone, so the GC's partial index can key on it
  and an Apply to a live entry still changes no indexed column (HOT). A
  live entry's stale stamp, or none, is never what protects a tombstone: an
  Apply that deletes the key applies only a change D its `basis` doesn't
  see, so every change the `basis` does see completed before D and is in D's
  batch or an earlier one, which D's page's stamp covers; a Re-derive that
  deletes it stamps its own read's segment. A revival keeps the old stamp,
  which can only delay the GC. The GC
  skips the ledger of a definition under a build (`backfilling`, or a
  frozen Re-derive build), holding the definition's row `for key share`
  while a batch runs, which a build's start waits on: a build chunk's insert
  of a new key's entry needs every tombstone written after its snapshot to
  stay until the insert.
- Every repair of a definition on the Re-derive build is a rebuild:
  Re-derive over the key space, which I2 makes safe against any pending
  change. A resume starts one, as does an explicit `request_backfill` and a
  capture re-install (the call's own transaction moves each `live` reader to
  `backfilling` and enqueues its plan and sweep jobs, so the status already
  reads `backfilling` when the call returns). The only repairs that are not
  rebuilds are a quarantine release (`release_key`), which is a per-key
  Re-derive (a staged `Recompute`) of the one key, and a relationship
  projection's refresh. A repair a caller waits on is therefore visible in
  the status, never parked where only the repair knows it hasn't run.

### What `live` promises

`live` tells an operator that a
transform is in its **steady state**. Once a definition reports `live`, a
watermark token taken after any commit and awaited with
`Trellis::await_converged` guarantees that the transform's values reflect
every commit at or before that token. Nothing else is needed.

For that to hold, `live` is strict: it is reported only once every chunk has
committed and every recorded group delta is merged. Every captured change
since registration has been applied (the definition applied from its first
chunk), and every commit before registration was read by a chunk under a
snapshot later than the join fence, so nothing is outstanding.

The two signals stay separate on purpose. `await_converged` is a pure LSN
wait over captured changes. It never reads definition status, so it can
return while a definition that isn't `live` yet is still building. Backfill
is an operator concern, reported by `Trellis::status`. A reader that needs a
settled target checks both: `live` from status, then its token. Because a
rebuild is a status transition made in the repairing call's own transaction,
"poll `live`, then take a token" also covers a repair made before the poll.
`self_check` reads the status too: it compares only a `live` definition, and
reports any other as `NotLive` with its status, instead of comparing a target
that is still changing.

## Truncate, DDL, drop

- `AFTER TRUNCATE` statement triggers per captured table stage one truncate
  row; the drain barrier is restated under xid order. A to-side
  truncate is the parent operation with zero values for every parent of the
  table.
- **A source truncate resets the target and raises a floor**, an O(1)
  write rather than a tombstone per entry. The page that applies it empties the target's ledger
  (and its build deltas), clears the target, and raises the target's
  truncate floor (`ledger_truncate_floor`) to the truncate's ring `lsn`.
  I2 then also refuses any change at or below the floor. The floor is
  exact because `TRUNCATE` takes `ACCESS EXCLUSIVE`: every writer that
  touched the table before it committed first, so its trigger's position is
  below the truncate's, and every writer after it is above. That includes a
  later statement in the truncate's own transaction: the truncate's trigger
  writes its ring row after reading its position, which moves the insert
  position on before the next statement's trigger reads it. The drain
  barrier already applies the truncate's batch after every earlier batch and
  before every later one, and the fold voids the batch's own earlier rows, so
  the floor is a second line of defence: any change from before the truncate
  that still reaches a page is refused, the way Postgres's own lock orders
  them.
- Source `ALTER TABLE` on a read column follows the schema-changed marker
  and regeneration rule; on any other column it is invisible.
- Dropping the last definition on a table drops its triggers on the next
  reconcile pass under I6's retry loop.

## Multi-hop: a target's writes reach its readers through the mutation seam

A target is never captured by triggers. A definition that reads another
definition's target (a chained hop) learns of its changes from rows the
writer stages in the same transaction as the write: image-less recomputes, or
change-shaped rows for a relationship endpoint.
A building target's writes reach its readers
the same way, so a rebuilt upstream needs no catch-up for its readers. A
worker propagating downstream appends to the active segment, never the batch
it is draining.

Targets are not trigger-captured, though the writer stages their changes in
the same ring row shape the triggers write for a source, so intake and the
drain path are shared and only the producer differs. Trellis is a target's
only writer and already knows each change as it makes it; a trigger would
re-derive what the writer has in hand.

- A trigger adds per-row cost to every target write; the seam adds none
  beyond the staging insert itself.
- `hop_gen` and cycle detection stay in process. A session setting could
  carry `hop_gen` to a trigger, but that rebuilds in SQL what the writer
  already has.
- Cascading truncates on a captured target would need their own handling.
- One propagation path serves multi-hop.

## Rejected alternatives

Each alternative below is recorded with the number that rejected it.

| Alternative | Why not |
|---|---|
| **Capture by logical replication** (a `pgoutput` slot decoded by an intake thread). It keeps everything off the write path and needs no trigger on user tables. | I measured staging capped at ~120k rows/s at 1,000 rows/commit and 18.6k at 1 row/commit whatever the writers do, with a 10M-row `COPY` never staged, and total CPU per row 4–5x the trigger's, paid later. A stopped Trellis pins the whole database's WAL rather than growing a ring at ~188 B/row. The decoder's 32-bit xid must be widened to `xid8` for I2, and I found the naive widening silently wrong for every id before an epoch boundary. Decoding needs `REPLICA IDENTITY FULL` on every table a relationship touches. Slot loss, failover and the replication privilege are operational surface the trigger has none of. |
| **Row-level triggers.** | I measured 5–10x the statement trigger's cost above 1 row/commit. |
| **Column-agnostic image encodings** (`to_jsonb`, `jsonb_each_text`, `::text`, `hstore(NEW)`). | Not byte-identical to `tuple_to_json`, or identical but reading every column, which I measured costing 40x on a 100 KB column. |
| **Ordering by LSN with live reads guarded by horizons and basis checks:** the recompute horizon and extinct horizon, a minimum-image LSN, the build horizon, per-key basis locks, the watermark wait before enumeration, recompute and vanished-image flags, bucketed advisory locks, a keep-every-image fold, prior image on enumeration. | Each closes one pairing of one producer against one drain order, found in review of the previous fix at roughly two new problems per fix, and the generative suite found none of them. A live read has no way to tell which commits it reflects; only a snapshot does (I2). |
| **Visibility-only I2**, and **Apply stamping its own snapshot as the basis.** | Each fails scenarios of the interleaving experiment: visibility-only fails three, and stamping Apply's own snapshot fails two. |
| **Membership-only ledger** (no stored contribution). | Wrong for relationships on both shapes I ran it on; saves 1–2% of WAL. |
| **Group rows as absolute writes:** forced recomputes from a live `GROUP BY`, an existence probe and pre-lock before incrementing. | Not orderable without a horizon; the probe's scan is the 40k-group cliff where nothing drains; the pre-lock deadlocks on new groups. Groups as sums with sorted increments: 0 deadlocks in the interleaving experiment. |
| **The settled projection as correctness state** for to-one relationships of aggregates (a relationship-enriched 1-1 target still reads it), with the reverse fast path, a to-side-superseded flag, the `for share` stamp, refresh markers, seeding and widening. | Loses updates under to-side churn (I ran it: never converged, wrong target, reproducibly); needs `REPLICA IDENTITY FULL` on both sides; every consistency rule for it is a pairing rule. The factored T table is the parent's applied value under the ledger's own locks. |
| **Per-child contribution rewrite on a parent change.** | I measured 2.3–3.1x the time and 5x the WAL at 100k fan-out on tmpfs, 2.4–2.9x on disk. |
| **Finding a parent's children by the ledger index alone**, or **by the index plus a live from-side scan**, or **by locking the to-side inside the application's transaction.** | The index alone misses a child moving in uncommitted (a scenario of the interleaving experiment); the scan works but keeps a live read; the lock puts Trellis on the application's commit path (I6). T as dependency lock replaces all three. |
| **One set-based `GROUP BY` build with changes withheld, then a go-live re-read and orphan sweep.** | The re-read is a full enumeration per build that scales with table size, not with what changed; I ran it at 100M, where it reaches `live` after 2,305 s and its drain then runs out of memory. Nothing can tell a table unchanged since the build from one changed and changed back, so the re-read could never be skipped. The one-pass build's algorithmic wins (scan once, chunk the writes) do not carry over: the new build is per-row work with no `GROUP BY` per chunk, and its acceptance bar is linear time. |
| **`catching_up` as a stored status with marker fences, generations and go-live catch-ups.** | Needed only by a build that skips changes. A definition on the Re-derive build applies from its first chunk, so there is nothing to catch up; builds not yet on it keep the status ([A build](#a-build-is-re-derive-over-chunks-and-applies-from-its-first-chunk)). |
| **Bounding drain memory by running fewer workers.** | Each worker then holds more buckets; I measured that it bounds nothing. |
| **Fixing the build's cost by chunk size.** | I ran 10x larger chunks and they left 2.0x the time and 3.7x the WAL. |
| **Resolve every image-less trigger into an image at staging time**, so Phase 2 never reads live state. | Under trigger capture the only image-less change is a delete, and every other image-less producer is an enumeration this ADR replaces with Re-derive under lock. What this leaves untouched is out-of-order parallel batches for one key, which needs `applied_lsn`, not images. A resolver on the seal path is itself a live read racing the drains. |
| **Serialize absolute writes per target** through one writer or one advisory lock. | Does not fix identity: a serialized write still reads live state ahead of pending deltas and still needs a horizon. Serializes the build, the largest producer of absolute writes, which is already CPU-bound on eight workers at 14–15k rows/s. |
| **Thread source transaction ids through the ring.** | Adopted, in the exact form `row_txid` gives for free under trigger capture. |
| **LSN-versioned projection rows.** | Covers the relationship half only. The T table is this idea in a cheaper shape: one applied value with its position per parent, not a version history per row. |

## Consequences and costs

- **Capture is on the application's write path.** The cost is one ring
  append per statement and about 2.9x the source row's WAL bytes on disk.
  It is paid by the writer at commit, not by a background process later, so
  it is visible and it cannot fall behind: there is no capture lag, only
  drain lag. The write-heavy user gets a documented bulk-load pattern.
- **Trellis runs inside every writer's transaction.** Every future change to
  the ring's shape, the seal, or maintenance inherits I6: a lock the
  application can queue behind is a bug, and a condition that fails the
  application's statement is a bug. Both get a written rule and a test
  ([validation 5](#validation-and-acceptance)).
- **Storage grows with the source.** Each aggregate target carries a ledger
  of one row per source row (2.6–3.1x a narrow source's on-disk size), and a
  1-1 target a slim one. WAL on the hot path is 1.5–1.8x a ledger-less
  apply. In exchange, no full old row is ever staged (the ring carries the
  imaged columns only) and `REPLICA IDENTITY FULL` is not required anywhere.
- **The failure mode of a broken capture is loud, not silent.** A schema
  change to a read column never fails the statement (I6). A missing ring
  table or revoked privilege does, with a `CONTEXT` line naming the capture
  function:
  the application sees the fault at once, not after data is lost. One silent-uncapture path is an owner disabling
  the trigger by name, which the audit reports; the others are the
  disable-and-restore and replaced-function cases, the known correctness gaps
  [Capture switched off and back on between two reconcile passes](../known-correctness-gaps.md#5-capture-switched-off-and-back-on-between-two-reconcile-passes) and
  [A capture function body replaced by hand](../known-correctness-gaps.md#6-a-capture-function-body-replaced-by-hand).
- **Hosted compatibility** is core Postgres for everything required
  (statement triggers, transition tables, `SECURITY DEFINER`, the `TRIGGER`
  privilege); no event trigger is needed.
  No replication privilege, slot or publication is needed, and a
  backup-and-restore carries the ring with the data.
- **Drain backpressure becomes the operator's number.** Capture can outrun
  the drain by ~20x (1.9M rows/s against ~85k folded rows/s with the
  ledger), and I measured the ring in the application's database at ~188 B per
  captured row when nothing drains.
  The bound on that growth is designed but not built:
  - **Gauges** report the ring's rows and bytes, the age of its oldest
    undrained row, and its undrained bytes per source table.
  - **A soft bound, `ring_warn_bytes`, is on by default.** Above it,
    `self_check` and `status()` report a backlog warning, and the drain takes
    priority over optional work, as a build already yields to it
    ([A build](#a-build-is-re-derive-over-chunks-and-applies-from-its-first-chunk)).
    Nothing is paused.
  - **A hard bound, `ring_max_bytes`, is opt-in and off by default.** At it,
    capture pauses for the definitions whose source tables hold the most
    undrained bytes, largest first, until the ring is back under the bound.
    Their pause kind is `backpressure`, their triggers are dropped under the
    `lock_timeout` fence (I6), and their ring rows are discarded. `status()`
    shows them paused with the reason, and `RESUME` rebuilds them from the
    source: they are stale until then, never silently wrong. No application
    writer is slowed or failed, and no captured row is skipped while capture
    is on.
  - **No bound applies while every Trellis process is down.** The capture
    triggers keep appending, and only the database's own disk alerts guard
    the disk. On restart, the bounds are checked first. Sizing the disk for
    an outage:
    [recommendations — Ring sizing](../recommendations.md#ring-sizing).
- **Build time is per-row work on every worker**, not one `GROUP BY`. The
  acceptance bar is linear time with a per-row cost independent of table
  size, checked at 100M ([validation 3](#validation-and-acceptance)).
- **Registration latency does not depend on table size**, and no definition
  is `live` when registration returns: callers poll `Trellis::status` to
  `live`, then use a token
  ([embedding](../embedding.md#poll-to-live-dont-wait)).
- **I took every number above on one box.** The capture-throughput
  experiments and the first four ledger experiments ran on tmpfs; the
  disk-backed numbers are from a single NVMe with ~0.4 ms fsyncs and, for the
  ledger experiments' disk tier, a box that was not always quiet.
  Within-session ratios stand; absolute bars are restated in
  [validation 4](#validation-and-acceptance).

## Validation and acceptance

Nothing below polls for convergence.

1. **One deterministic interleaving test per scenario of the interleaving
   experiment** (all seventeen) on the real engine, hand-driven with advisory
   locks, plus one for each of the nine superseded problems in the inventory of
   producer pairings that motivated this design.
2. **The generative concurrent tier** with three planted violations (skip
   the lock, compare LSN instead of visibility, delete tombstones early), each
   caught within the nightly budget. **Merge gate.**
3. **`build-under-load` at 100M** on the implementation branch, disk-backed,
   under a 16 GB memory cap: oracle ok, memory bounded, WAL retention
   bounded, time linear in N, per-row cost flat from 1M to 100M.
   **Milestone exit for the build.**
4. **A quiet-box, disk-backed benchmark round** with a checkpoint inside the
   window and storage stated on every number: `fold-in-ratio`,
   `group-contention` (400/4k/40k), `rel-churn` (10/1k/100k children per
   parent, 100 and 1,000 parent updates/s, child-only churn),
   `build-under-load`, and the two capture benchmarks (statement against row
   triggers, and scaling with writers), each with a same-session control on
   the pre-change code. The bars in this ADR are restated from that round.
   *My implementation round ran the aggregate half* (`fold-in-ratio`,
   `group-contention`, `build-under-load` at 10M, on tmpfs and disk, against a
   same-session control). `rel-churn` and the capture benchmarks have not been
   run in that round.
5. **Writer-coupling tests:** schema change of a read column under load
   (writes succeed, definition pauses, regeneration restores capture); join
   and drop under a 30 s open transaction (worst writer wait under 100 ms);
   revoked privilege and disabled trigger (audit reports); `REPEATABLE
   READ` and `SERIALIZABLE` writers straddling a seal (no lost rows).
6. **Drain paging:** a segment several times the batch cap drains with peak
   RSS bounded by the cap; a key whose changes straddle two segments, a
   paged truncate segment, and a reclaim between two pages all converge.

## Reusable material

Prototype branches on the fork (mmmries/trellis) hold the ledger Apply and
`rel-churn` experiments with their results and handoff notes, the factored P
and T tables, the chunked ledger build with `build-under-load` (the disk
columns and the memory sampler), a trigger-capture variant of that build, and
the trigger-capture spike (the capture SQL with the `new_only` and `skip_noop`
shapes, and the phase-1 harness). The fence mirror is already in `main`.

## More information

- [Documenting Architecture Decisions](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)
  for the ADR form ([ADR-0001](0001-use-adr.md)).
