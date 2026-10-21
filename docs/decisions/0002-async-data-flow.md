---
status: draft
date: 2026-10-17
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
decisions to partitioned sources, and
[ADR-0019](0019-join-transforms.md) builds JOIN transforms on its group-level
basis.

**Derivation is asynchronous.** Source changes are batched, and derived
values are computed and written by drain workers outside the application's
transactions, for the reasons in
[Derivation is asynchronous](#derivation-is-asynchronous).

**Capture is synchronous.** A source change is recorded into Trellis's
staging ring by an `AFTER … FOR EACH STATEMENT` trigger inside the writer's
own transaction. The ring row's transaction id is therefore the source
commit's, and that identity is what makes every derived write orderable.

**Ordering is owned by the target, at the grain the target is kept at.** A
1-1 target keeps a slim ledger with one entry per source row: the ring
position last applied to the row and the snapshot it was last derived under.
An aggregate target keeps the same ordering state once per *group*, beside
hidden per-group partials (a row count, non-null counts and `numeric` sums)
that every admitted aggregate is a function of. Every write that derives a
value from a *read* rather than from carried images locks the entry or group
first and reads in the same statement as its snapshot; every change is
applied or skipped by checking its transaction against that snapshot (and,
per row, its position against the last applied one). A build is the same
operation over chunks of the source, and a definition applies changes from
its first chunk.

The reason for the ordering state is the class of write that ordering by
position alone leaves unordered. A **delta** (`+f(new) − f(old)` into a
group) commutes with every other delta, so parallel workers may apply deltas
in any order and the exactly-once argument of
[stage 05](../staging-and-claiming/05-apply-and-exactly-once-deltas.md)
holds. An **absolute write**, any value derived from a read, does not
commute: it is right only if nothing later in the change order has already
been applied to the same row or group. Trellis has several producers of
absolute writes (a build, a rebuild, an image-less recompute, a partition
correction), each reading live `READ COMMITTED` state and each racing
out-of-order parallel drains. Ordering them by position needs one precedence
rule per pairing of producers; one ordering mechanism for every absolute
write replaces those rules. My measurements also found scaling cliffs in the
alternatives, a logical-replication capture, a one-pass build and a per-row
ledger for aggregates, that this design avoids:

- I measured a logical replication slot staging at most about 120k rows/s
  whatever the writers do, and it never captured a 10M-row `COPY`;
- I ran a one-pass go-live re-read at 100M rows on a 31 GB box and it could not
  finish, because a drain batch held its whole share of a segment in memory;
- I measured a per-row aggregate ledger at 15.1–15.3 GB for a 100M-row source
  in 50k groups, where the group basis keeps 5 MB, and its 100M build at
  370–380 s and 58–71 GB of WAL, where one `GROUP BY` per partition took
  1.4–1.9 s.

Conventions: **must** is a requirement the evidence forces; **should** is a
recommendation the evidence supports but does not force.

A concrete aggregate, over `order_line_items(id bigint primary key, order_id
bigint, amount numeric(12,2))`:

```
TRANSFORM order_totals FROM order_line_items GROUP BY order_id
SELECT
  order_id AS order_id,
  COUNT(*) AS line_count,
  SUM(amount) AS total_amount,
  AVG(amount) AS avg_amount
```

Its target holds one visible row per `order_id` with at least one line item.
Beside it, Trellis keeps per group the row count, the non-null count and the
`numeric` sum of `amount`, the snapshot of the last absolute write and that
write's segment stamp. A line item moving from order 7 to order 9 is two
sides, `−1` and `−amount` against order 7 and `+1` and `+amount` against order
9, each checked against its own group's snapshot.

## Derivation is asynchronous

Update derived tables in the transaction that changes the source, or later,
in batches? Trellis derives **later**, and that choice shapes the user
experience.

Synchronous derivation (triggers or functions that write the derived table in
the source transaction) is consistent by construction and fast for shallow
same-row derivations, but it fails where Trellis's use-cases live:

- **Cross-table formulas are unsound.** Concurrent transactions don't
  see each other's changes until they commit, so a derivation that reads a
  row of another table inside the writer's transaction can compute from a
  value that is already stale when it commits.
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
them. I0 to I5 are the ordering rules as my experiments refined them, stated
for both grains: a 1-1 target's ledger entry per source row, and an aggregate
target's group. I6 to I8 are the rules the capture and build experiments
forced.

| | Invariant | Evidence and refinements |
|---|---|---|
| **I0** | **One database.** Sources, the ring, every ledger, every group's state and every target live in one Postgres database, so one snapshot orders every commit Trellis will see. | Design premise; made exact by trigger capture, which makes a ring row's transaction the source commit's. Nothing here works across databases. |
| **I1** | **Read after lock.** Every live read that feeds an absolute write happens after the writer holds the lock on the state it will write (a ledger entry, or a group), and the snapshot it stores is taken **in the same statement** as the read. A group-level absolute write (a recompute, a build batch, a partition correction) locks its groups first in one sorted statement; the snapshot, the `GROUP BY`, the new partials, the basis and the stamp are then one later statement. A page reads each group's basis in a statement after its lock statement. One write reads first: a 1-1 build chunk's insert of the entry of a key that has none, from a read in the same statement. Its uniqueness check comes after the read, and finding no entry there means no Apply has written the key since the read's snapshot, because the tombstone GC, the one thing that removes an entry an Apply wrote, skips a ledger under a build (I4). A page deletes only a placeholder it wrote no change to, which holds no applied change, and a source truncate commits before the chunk's first read, which holds the source's lock, so it empties no change the snapshot doesn't see. A key that has an entry by then is locked and read afresh. The rule needs `READ COMMITTED`, where each statement reads from a snapshot of its own, so a read after the lock sees what the lock waited for; at `REPEATABLE READ` or `SERIALIZABLE` it would read from the transaction's first snapshot, taken before the lock. Every session Trellis opens sets `default_transaction_isolation` to `read committed`, whatever the server's, database's or role's default, and a transaction that needs one snapshot for several reads asks for its level explicitly. | In my interleaving experiment (seventeen hand-driven scenarios of Apply racing a Re-derive and other producers), the scenarios where a stalled Re-derive meets a change to the same key, committed before or after its snapshot, hold, and Apply demonstrably blocks on the entry lock. In a snapshot-timing experiment I measured a snapshot taken in a separate statement differing from the read's in 99.7% of samples under load; the stored basis is the full `pg_snapshot`, not `xmin`/`xmax`. In my group-basis model a page that read a group's basis before its lock wait double counted; read after the lock it was exact. |
| **I2** | **Visibility-checked application.** Per row: a change C for row r is applied iff C's transaction is **not** visible in r's basis snapshot **and** C's ring position is above r's `applied_lsn`. Per group: a *side* (a group, a transaction) is applied iff its transaction is not visible in the group's basis (a group with no basis sees nothing); there is no per-row `applied_lsn`, because sides are commutative deltas and exactly-once comes from the page's claim and cursor in the page's own transaction. Both also refuse a change at or below the target's truncate floor ([Truncate](#truncate-ddl-drop)). Skipping is exact, never "maybe counted, re-derive". Ordering state is per database: it holds this cluster's WAL positions and transaction ids. A logical restore brings another cluster's, so the capture pass pauses every definition and discards the ring, and a `RESUME` resets the ledger's `applied_lsn` and `basis`, the groups' basis and stamp, the truncate floors and a `JOIN`'s hidden tables before it rebuilds. An ordinary rebuild keeps `applied_lsn`. | In the interleaving experiment, skip-iff-visible alone fails three scenarios (same-key order across batches is not decidable from visibility); stamping Apply's own snapshot fails two more; visible-or-`applied_lsn` passes all seventeen. The group basis passes every one of them that applies to an aggregate on visibility alone, including the three that fail visibility-only on the ledger, because its writes to a group commute. An in-flight id is decidable from the stored list (0 disagreements over ~1.1M pairs in the snapshot-timing experiment); in my group-basis model an id in progress at the snapshot, in its `xip` list or above its `xmax`, applied exactly. |
| **I3** | **Ordering state at the target's grain; group values are functions of sums.** A 1-1 row's ordering state is its ledger entry. A group's is its basis and stamp; its partials are the sum of the sides applied since its last absolute write, so group updates commute between absolute writes. Images form a chain per row ([Capture](#capture-by-statement-triggers)), so a row's sides telescope to its current contribution. | In the interleaving experiment, eight workers creating the same groups at once, and a row passing through a group within one folded batch while that group rebuilds, both hold with groups as sums and no pre-lock or probe. In my group-basis model every failure I found with images that didn't chain went away once they did. |
| **I4** | **Tombstones live until the batch watermark passes.** A deleted row's entry stays, with its `applied_lsn` and `applied_seg`, until every batch at or below its `applied_seg` is fully drained. A group whose partials are all zero and that has a basis stays until the contiguous drained prefix passes its stamp ([Convergence and status](#convergence-and-status)). | In the interleaving experiment an older update resurrects a deleted row unless the tombstone is kept. The batch-watermark form is exact under triggers because a same-key predecessor of a delete committed before the delete's trigger ran. In my group-basis model, a GC that ignored the drained prefix was wrong in every soak run, and one that tested the row count alone was wrong in a deterministic scenario. |
| **I5** | **One lock order, taken as one sorted batch (R7).** Build chunk rows in id order, then the page row; then a page's 1-1 targets, then its aggregate targets, each in target order; per target its ledger entries, then its rows or groups, each locked in key order in one statement per class. Every statement that locks them follows it, including a truncate's wipe and a chunk's retirement. Never a per-row loop. | In the interleaving experiment a per-row loop deadlocked 19–22 times in 20 s; the sorted batch never did. The implemented paths held it in my benchmark round (no deadlock in any benchmark, and group moves and the hot-key case converge with none). In my group-basis model, a truncate wipe and a chunk retirement that locked in scan order deadlocked with pages tens of times per soak matrix; sorted, 0 times. |
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
  columns the table's definitions read, emits the read columns' old and new
  images (a delete's new image is absent), renders every value with
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
  read columns costs +30 µs. The `WHEN` clause is a filter inside the function (a statement
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
  rate is 0, as with no capture.
- **Must (R3, the image chain):** a row's images form a chain, each change's old
  image being the previous change's new image, so a group-basis reader's
  sides telescope to the row's current contribution. A 1-1 Apply writes the
  new image and needs no old one; a group-basis Apply takes every change's
  old side from the ring. The live re-read breaks the chain where it fires:
  a nested write stages its own change first, and the outer statement then
  stages its transition old image against the live new image, so the row
  counts as `2c − a − b` instead of `c − a`; a self-referencing `ON UPDATE
  CASCADE` merges two updates of one row into one statement, and pairing by
  key uses one old image twice. So a group-basis reader counts every
  transition row exactly once, `Σf(NEW TABLE) − Σf(OLD TABLE)` per statement
  (dropping a no-op pair is still exact, as a cancellation), and a ring row
  the re-read wrote carries the statement's transition images for it
  (`transition_images`, old and new); duplicate keys pair one to one, and
  the sum is identical. The re-read also fires when an update's new rows hold a
  key twice. *Evidence:* in my group-basis model the live images left
  groups wrong in every soak run with application triggers that rewrite rows
  (about 1,200 wrong groups per version); unpaired transition images were
  exact in every run and in each nested-write shape: a nested update, a
  nested delete and re-insert, an insert followed by a nested update or
  delete, and both cascade shapes.
- On the privilege model, I checked that the application needs nothing on
  Trellis's schema. The Trellis role must own each source table or be a
  member of its owning role, because `ENABLE ALWAYS` needs ownership, not
  just `TRIGGER`. It needs no superuser.
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
  `ENABLE ALWAYS`; an owner who disables, drops or recreates a trigger of a
  plain table is seen by the capture pass, which pauses the table's readers
  until a `RESUME` (see also
  [known correctness gaps — Capture disrupted and restored between two passes](../known-correctness-gaps.md#capture-disrupted-and-restored-between-two-passes)).
- **Only the staging worker creates or drops triggers**, from the catalog
  alone, on its reconcile pass: a table gains triggers when something
  registers a reader of it and loses them when its last reader is dropped.
  Registration itself does no source reads and no capture work; it writes
  the definition and its ledger or group-state tables, and the staging
  worker starts its
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
  the `WHEN` cut and the transition image a re-read row carries for a
  group-basis reader are unmeasured. No synchronous capture can
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

## The 1-1 ledger: one target-owned entry per source row

**Decision point.** The target, not the ring and not a projection, owns the
state that orders its writes. A 1-1 target owns it per source row.

- **Must:** a 1-1 target's ledger is a slim side table,
  `<target>__ledger(from_key primary key, applied_lsn, applied_seg, basis,
  tombstone)`, beside an unchanged target table; the target row holds the
  values. It is written in the same transaction as the target, entries
  locked in key order before any target row is touched. A tombstone row in
  the target would leave a deleted row readable after `await_converged`
  until GC runs; a tombstone in the side table gives a never-seen key a
  single row to lock. The side table is one lock domain per key (a
  placeholder insert, then a sorted `for update`). Its cost is one more row
  write per change: ~200 B of WAL per row in `throughput-ramp` (about 500 B
  right after a checkpoint, with its full-page images), and the placeholder
  insert settles a new key's Apply on its own. Only a tombstone carries
  `applied_seg` (I4), so an Apply that leaves an entry live changes no
  indexed column and can be HOT.
- **Must:** exactly two operations exist, and every producer is one of them.
  - **Re-derive(r).** Lock r's entry (inserting a placeholder if absent;
    a build chunk instead inserts an absent entry from its read, I1).
    In one statement: take the snapshot, read r's source row, compute the
    target row, write it, and replace the entry's basis with the new
    snapshot, leaving `applied_lsn` as it was. If r no longer exists in its
    source, the target row is deleted and the entry becomes a tombstone. It
    does not store the read's position as `applied_lsn`: a ring row's
    position is the writer's pre-commit insert position, so a change whose
    trigger ran before the read but which committed after it would sit below
    that position, invisible to the basis, and be skipped for good. The
    basis alone records what the read saw: every change it saw is refused by
    visibility, and every change it missed is still pending under its own
    ring row. The exact point a read stores is therefore the full
    `pg_snapshot` taken in the same statement as the read (I1), on every
    entry that read wrote; a read in keyspace chunks stores each chunk's
    own, which a test of chunked keyspace reads checks.
  - **Apply(r, C, image).** Lock r's entry. If C is visible in the entry's
    basis, or C's position is at or below `applied_lsn`, stop (I2).
    Otherwise write the target row from C's new image and set the entry's
    `applied_lsn` to C's position, basis unchanged. A delete removes the
    target row and the entry becomes a tombstone. A row with no entry is an
    insert of NEW, which the lock's own insert writes as the entry.

  | Producer | Operation |
  |---|---|
  | Captured insert/update/delete | Apply |
  | Re-derive build, resume rebuild, `request_backfill`, quarantine release, column resume, `ALTER TRANSFORM` added column | Re-derive over the definition's key space, chunked |
  | Source `TRUNCATE` | The ledger is emptied, the target's truncate floor is raised to the truncate's ring position, and the target is cleared ([Truncate](#truncate-ddl-drop)) |
  | Chained hop | Apply, where C is the upstream apply transaction ([Multi-hop](#multi-hop-a-targets-writes-reach-its-readers-through-the-mutation-seam)) |

## Aggregates: a basis per group, kept by deltas

**Decision point.** An aggregate target keeps its ordering state per group,
not per source row. Each group carries hidden partials, its basis (the
`pg_snapshot` of the last absolute write to it) and its stamp (the newest
segment that snapshot saw). Live changes reach it only as signed deltas
computed from captured images; absolute writes reach it only through the
group lock. Maintaining it never reads the source on the hot path and never
needs a source index.

A per-row ledger is not needed for old values: the ring carries every
change's old image. It exists for ordering, and one row's changes form a
prefix in any snapshot, so in any region (a row, a group, a partition, a
build chunk) the changes a region's basis doesn't see sum exactly to what
the region still owes. I modelled this path on real Postgres 15 and 18:
121 deterministic scenarios (89 exact, 25 negative controls each removing
one rule and each wrong as intended, the rest out of scope or
characterizing a failure), every scenario of the interleaving experiment
that applies to an aggregate among them, and a soak of 800 runs, 4.7M
writes, 618k pages, 25k
recomputes, 754 source truncates and about 1,140 partition DDLs, every run
ending with 0 differences from a fresh `GROUP BY` and no deadlock on any
Trellis path. An independent review mutated the model; three mutations got
past every scenario, and the scenarios that now catch them are the R4, R5
and R6 rules below. The group-level rules are labelled R1 to R9 where they
are stated: R1 and R2 restate I1 and I2 per group, R3 is the image chain,
R4 restates I4, R7 is I5, and R9 is the truncate barrier.

### The admitted aggregates are the ones partials keep exact

- **Must:** a plain aggregate, or a JOIN transform
  ([ADR-0019](0019-join-transforms.md)), admits exactly these aggregates:
  - `COUNT(*)` and `COUNT(col)`;
  - `SUM(col)` and `AVG(col)`, where `col` is `int2`, `int4`, `int8`, or
    `numeric(p,s)` with a declared scale (next section).
- **Must:** an argument is a column or an expression over one row's
  columns in the transform grammar (`SUM(a + b)`, `COUNT(<expr>)`), as long
  as its result type is admitted. The group basis evaluates it on each
  change's old and new images, so its delta is `f(new) − f(old)`, exactly
  as for a column. On a JOIN an argument may use only one side's columns; an
  argument mixing both sides is refused at define.
- **Must:** a field may compose admitted aggregates (`SUM(a) + COUNT(b)`,
  `COALESCE(SUM(a), 0)`). It is computed from the group's partials whenever
  the group's row is written. Neither expression arguments nor composed
  fields change lock order or apply order.
- **Must:** result types match Postgres: `count` is `bigint`,
  `sum(int2|int4)` is `bigint`, `sum(int8|numeric)` is `numeric`, and `avg`
  is `numeric`.
- **Must:** every admitted aggregate is a function of three per-group
  partials: the row count, each argument's non-null count, and each `SUM` or
  `AVG` argument's sum, kept as `numeric` whatever the argument type so a
  sum (or, on a JOIN, a product of a sum and a count) never overflows where
  Postgres's own `sum()` wouldn't.
- **Must:** every other aggregate is refused at define, named by family:
  `MIN`/`MAX`; `bool_and`/`bool_or` and `bit_and`/`bit_or`; `SUM`/`AVG`
  over a float, `interval`, unconstrained `numeric` or any other type; and
  `array_agg`, `string_agg` and `jsonb_agg`. The
  [Supported sources and targets](../transforms.md#supported-sources-and-targets)
  table is the reference. Each can be added later, through hidden counters,
  a value-count table or a recompute, without changing what is admitted
  now.

`MIN` and `MAX` would need a group recompute from the source whenever the
current extreme leaves: the only source read on the hot path, a source index
for every argument, and a cost that grows with the group (deleting the
current maximum of a 100M-row group rescans the group). Keeping the per-row
ledger for the aggregates partials can't keep would ship two aggregate
engines with different concurrency rules.

### `numeric` arguments declare their scale

Postgres shows `sum(numeric)` with as many decimal places as the most
precise value currently in the group. A running sum keeps the widest scale
it ever held: after `0.125` leaves `{1.5, 0.125}` the group reads `1.500`
where `sum()` over the source prints `1.5` (I ran it on Postgres 15 and 18).
Postgres stores every value of a `numeric(p,s)` column with exactly `s`
places, so over such a column the mismatch can't arise.

- **Must:** `SUM` and `AVG` admit a `numeric` argument only when its column
  declares a scale. The running sum, every JOIN product and the average
  then carry exactly the scale Postgres would show, through a build and
  through pages alike. `COUNT(col)` over any `numeric`, and `numeric` as a
  `GROUP BY` or join key, are unaffected.
- **Must:** a retype of an argument column after define (a new scale, or
  unconstrained `numeric`) pauses the definition through the schema-change
  check that compares a definition's columns with the live schema. A resume
  re-validates: it refuses unconstrained `numeric`, and rebuilds at a new
  scale.

Keeping a counter per scale would make unconstrained `numeric` exact too; it
is additive, so it can come later without changing this rule.

### Apply sums sides per group and transaction

- **Must (R1, I1 per group):** a page locks every group it touches in one
  sorted statement, then reads each group's basis in a later statement.
- **Must (R2, I2 per group):** a page splits each change into *sides*: the
  old image's contribution, negated, against the old image's group, and the
  new image's against the new image's group. It sums the sides per (group,
  `row_txid`) and applies a sum only when its transaction is not visible in
  that group's basis and its position is above the truncate floor. A group
  move is therefore checked against each group's own basis.
- **Must:** sides are never folded per key. A fold (first old image, last
  new image) is wrong when a group's basis falls between two of a key's
  changes, since the fold's old side carries the refused change's
  transaction and its new side the applied one's, and wrong for a key that
  passes through three groups, since it names only the first and last. In
  my model, folding left wrong groups in every soak run. Summing per
  (group, transaction) costs about 30% more statement time than the fold on
  a 100k-change page where every key changed twice.
- **Must:** the page returns, per group, the change it actually applied, for
  the derived structures of a JOIN transform
  ([ADR-0019](0019-join-transforms.md)).

### Absolute group writes lock first and cover every group they could owe

The producers of absolute group writes are a build or rebuild, a
quarantine release that recomputes its groups, a partition correction
([ADR-0017](0017-partitioned-sources.md)) and the recompute of a group an
image-less change names.

- **Must (R1):** an absolute write locks its groups in a sorted statement
  first; the snapshot, the source `GROUP BY` over those groups, the new
  partials, the basis and the stamp are then one later statement.
- **Must (R5):** the stamp's segment is read in the same statement as the
  snapshot, or after it, never before.
- **Must (R6):** a recompute is never a single `UPDATE … FROM (GROUP BY)`
  with no prior lock statement.
- **Must (R8):** each producer states the groups it covers, and covers every
  group it could owe: the source's groups, plus the groups named by the old
  images of ring changes not yet drained, plus the target's own groups. A
  build that enumerated only the source's groups missed a group whose only
  pre-existing row moved out before the enumeration, so the move's old side
  later subtracted a row nothing had counted; in my model that naive
  enumeration was wrong in 39 of 50 soak runs. A partition correction is a
  recompute of the groups routed to the partition, never a blind zero:
  `TRUNCATE` of a partition and an `INSERT` into it in one transaction
  leave a row that a zero would erase and the basis would then refuse. Its
  groups include those a page still undrained from before the DDL can
  create. Which groups an absolute
  write must cover is where a new producer's bugs would appear, so each
  producer's enumeration is stated and tested on its own.
- A recompute holds its groups' locks for its whole `GROUP BY`: 1.3 ms for
  a 10k-row group, 9 ms at 100k and about 80 ms at 890k rows, with an index
  whose leading column is the group key. A page of 5,000 changes to one
  group holds the same lock for 18–28 ms.

### Readers see a group only while its row count is above 0

Pages apply in parallel and commit out of order, so between them a group's
partials can describe no real data: a negative count, `count(x) >
count(*)`, a sum no source state could produce. The converged state is
exact.

- **Must:** the partials are hidden from readers. A group's row is visible
  in the target only while its row count is above 0, and is written in the
  same transaction as the partials. Nothing else filters, orders or checks
  what readers see.
- **Must:** the target is a plain table in the schema the user named,
  holding exactly the declared columns and the `UNIQUE NULLS NOT DISTINCT`
  group identity, with no partial columns, so users can query, join and
  index it, and a chained reader finds its key through the constraint. The
  partials, basis and stamp of every group, zero groups and transiently
  negative ones included, live in a hidden table in the instance's catalog
  schema, named by definition id. Readers get no grant on it, and its shape
  is internal. A page writes the hidden row, then the visible row, for each
  group it touches, and skips the visible write when no visible value
  changed. A drop removes the hidden table by definition id; a resume drops
  and recreates it.
- **The contract:** before `await_converged`, aggregate values may be
  inconsistent, including combinations no source state could produce;
  after it they are exact. In my model, draining every batch up to a cut
  and none above it left exactly the state at the cut, and 5.0% of
  mid-drain reads of the raw partials showed a group with a negative or
  hidden count.

Writing the visible row only from partials that look sane would narrow the
anomalies but can't catch a wrong sum or average; applying each group's
changes strictly in order would serialize hot groups. Neither is built.

### Apply is one set-based statement per batch per target, and the batch is bounded

- **Should:** with `row_txid` exact, I2's skip rule is one predicate over the
  batch joined to the ordering state. A batch's Apply on a 1-1 target is one
  sorted lock statement over the entries it touches and one statement that
  writes the target rows and entries. On an aggregate target it is one
  sorted lock statement over the groups, one read of their bases, and one
  statement that applies the per-(group, transaction) sums to the partials
  and the visible rows.
- **Must (fold):** for a 1-1 target, an image-less `op = 'delete'` is the
  key's final state within a fold window, whatever precedes it. *Evidence:*
  in a 1M-row build dry run, a delete sharing a fold window with an earlier
  write of the same key was dropped by the "latest row with any image" rule,
  leaving 11–12 groups wrong. An aggregate target doesn't fold per key
  ([Apply sums sides](#apply-sums-sides-per-group-and-transaction)).
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
  permanent. Each page is its own transaction behind a claim check,
  advancing a durable per-bucket cursor; only a bucket's last page marks it
  drained. The claim and cursor are what make a group-basis page's deltas
  exactly-once. A segment's pages become visible to the drainers in one
  step, never page by page.
  *Evidence:* the one-pass build's memory measurement under I8.
- **Must (I7):** every lock statement in Apply runs under `lock_timeout`; a
  timeout releases the claim's transaction and retries the page after a
  backoff, outside any transaction.

## A build is Re-derive over chunks, and applies from its first chunk

**Decision point.** A new or rebuilt definition is not built by one
set-based pass with its changes withheld and recovered afterwards. It is
built by the same operation apply uses, over chunks of the source, while
applying: Re-derive per row for a 1-1 target, and a locked `GROUP BY` of the
chunk's rows for an aggregate
([An aggregate's build](#an-aggregates-build-adds-each-chunks-partials-under-its-group-locks)).

- **Must:** registration writes the definition and its ledger or group-state
  tables. Once
  capture covers it, the staging worker starts its build in one transaction
  that moves it to `backfilling` and enqueues a plan job, and the definition
  is **applying from that commit** (registration does no more than register). The plan job enqueues the chunk plan
  (primary-key ranges over the source, discovered by `max()` over `LIMIT` so
  every row falls in exactly one range) in batches, on a drain worker. Every
  drain worker claims chunks. A chunk
  is one short transaction. One statement takes the snapshot, reads the
  range and inserts, in key order, the entry and target row of every key
  that has none, written from that read. Then the chunk locks the other
  keys' entries in key order, and one more statement takes a snapshot,
  reads them and replaces their entries and rows. "No entry" means "not yet
  written": a captured change for a key whose chunk has not run applies as
  an insert of NEW, a delete of one records a tombstone, and the chunk's
  insert then finds that entry and leaves it to the locked Re-derive, which
  replaces it under a snapshot that includes that commit, so the row ends
  up written once.
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
  a 1-1 target's page re-derives rather than applies the keys of such a
  batch. An aggregate's sides commute, so it needs no such rule. *Evidence:*
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
  A source that is an aggregate's target is keyed by its nullable `GROUP BY`
  columns, and a key with a `NULL` part is in no chunk's range (the range is
  a row comparison). A 1-1 reader's plan job stages an image-less
  `Recompute` for each such key when its walk ends, which a page re-derives
  under the entry lock. Those rows can still be in the ring when the build
  flips `live`; their origin is unknown, so a token taken after the flip
  waits for them, as for any seam row. An aggregate reader's build covers
  such rows itself
  ([An aggregate's build](#an-aggregates-build-adds-each-chunks-partials-under-its-group-locks)).
- **Builds that keep the one-pass path.** The chunked build serves an
  aggregate or 1-1 definition on a captured source or on another
  definition's target. A 1-1 definition whose alias chain is cyclic keeps
  the one-pass build, with a fence wait and a go-live catch-up that re-reads
  every table the build read and sweeps the target for unbacked rows (the
  definition reports `catching_up` meanwhile).
- **Must (I7):** chunk transactions are short (one range, under
  `lock_timeout`), and a captured-change batch never waits inside its
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
  and no definition is ever `backfilling` without a chunk row driving it
  forward. A staging worker is still required for anything to
  seal; drain threads are required for anything to build.
- **Cost to state.** At 10M with the prototype's shape (group locks inside
  chunks), define-to-live was 2.0–3.2x the one-pass build's and WAL 3.7–5.1x.
  The implementation's 10M build converges in 65 s against the old path's
  791 s; the acceptance bullet above stays the bar
  ([validation 4](#validation-and-acceptance)).

| Build at 10M rows, 8 workers, 2,000 writes/s, disk | define → `live` | WAL | oracle | tail after `live` |
|---|---|---|---|---|
| one-pass `GROUP BY` + go-live re-read | 226 s | 5.8 GB | ok | 153 s |
| prototype, 10k-row chunks | 672–716 s | 29.3–29.8 GB | ok | 90–213 s |
| prototype, 100k-row chunks | 450 s | 21.7 GB | ok | 61 s |
| one-pass at 100M | 2,305–2,322 s | – | none: OOM in the drain | – |
| prototype at 100M | never: 4,131 of 10,001 chunks in 2 h 8 min | 190 GB retained | none: stopped by the disk guard | – |

### An aggregate's build adds each chunk's partials under its group locks

An aggregate's build keeps no per-row entries and needs no delta table or
merger: a chunk is a region of the source (a key range, or a partition), and
it adds its region's partials to the groups as increments.

- **Must:** a chunk locks its chunk row `FOR UPDATE`, then in one statement
  takes its snapshot, runs the `GROUP BY` of its region's rows, adds the
  partials to the groups under their locks (taken in key order) and marks
  itself done with that snapshot.
- **Must (the chunk gate):** a page locks `FOR SHARE` the chunk rows its
  sides fall in, before the page row and the groups (I5). A side is judged
  by the chunk its own row falls in (for a partition region, the partition
  of the side's own group): a chunk that hasn't run skips the side, because
  the chunk will count the row; a done chunk refuses a side its snapshot
  saw; a retired chunk applies it. A side applies only if the chunk gate and
  the group's basis both let it.
- **Must:** a chunk row retires only once the contiguous drained prefix
  passes its stamp.
- **Must (a recompute during a build):** a group recompute locks the chunk
  rows `FOR SHARE` and counts only rows whose chunk is done or retired; a
  not-run chunk will add the rest.
- **Must:** a source truncate's page resets every chunk whose snapshot saw
  the truncate to not-run ([Truncate](#truncate-ddl-drop)).
- **Must:** over a source keyed by nullable columns (another aggregate's
  target, or a source with `UNIQUE NULLS NOT DISTINCT`), the rows whose key
  has a `NULL` part, which no key range holds, get a build chunk of their
  own, whose region is "any key part is `NULL`". It is a chunk like any
  other: it takes its snapshot, runs its `GROUP BY` and adds its partials
  under the group locks; the chunk gate judges a side whose key has a
  `NULL` part by it (not run skips, done refuses what its snapshot saw,
  retired applies); a recompute during the build counts those rows only
  once it is done or retired; and a source truncate resets it like any
  chunk whose snapshot saw the truncate.
- **Must:** a rebuild over a target that already has groups recomputes
  every group in place, without pausing the definition. That is
  `request_backfill`, a capture re-install, and the automatic rebuilds
  (`capture_auto_rebuilds_total`), but not `RESUME`, which truncates and
  builds fresh. It is one absolute write per structure (one per source, or
  one per partition on a partitioned source, each group routing to one
  partition under
  [ADR-0017](0017-partitioned-sources.md)'s group-key rule), each the
  ordinary group recompute applied to all of the structure's groups in one
  transaction: it locks the groups in key order in a statement of their
  own (R6), runs the `GROUP BY` of the structure's source rows from one
  snapshot, and **replaces**, not adds to, each group's hidden partials and
  visible row, with that snapshot as the group's basis and its stamp per
  R5.
- **Must:** the groups a rebuild covers are the enumeration union (R8): the
  source's groups, the groups named by old images of undrained ring
  changes, and the target's own groups. A group with no source rows goes to
  zero and is collected under R4; its visible row is removed in the same
  transaction. A group the enumeration doesn't see (its first change
  commits after it) is not locked, and the rebuild leaves it to its pages,
  which apply each of its changes once.
- **Must:** pages keep applying during a rebuild. A change the snapshot saw
  is refused by its group's basis, a later one applies as a delta, and a
  page touching a locked group waits for the statement to commit. Readers
  see the old values until a structure's statement commits, then the new
  ones: the target never empties and no group shows a partial build. The
  status reads `backfilling` from the call, or from the pass that starts an
  automatic rebuild, until every structure has committed, then `live`.
  *Cost:* pages of a structure's groups, in practice its source's drain,
  wait while its statement runs: 1.4–1.9 s at 100M source rows, about
  15–20 s at 1B (extrapolated), one partition's worth at a time on a
  partitioned source. No ring segment retires while the statement's
  transaction is open, so once the ring's slots fill no segment seals and
  every source's drain waits for the commit too; writers aren't held back,
  and the active segment grows meanwhile. Clearing the target and building
  fresh would show readers an empty, then filling target, and a shadow
  build and swap would make pages write to both states; both are rejected.

*Evidence:* in my model, chunk builds with recomputes, garbage collection
and retirement, by one runner and by eight, were exact in every soak run.
Removing one rule at a time: a recompute counting not-run chunks was wrong
in 47 of 50 runs, chunk rows retired before the prefix in 50 of 50. A page
that didn't lock the chunk rows was wrong only in a deterministic scenario
(an insert into a group the open chunk never touches), as was a truncate
page that didn't reset chunks; the soak can't reach those windows, so those
scenarios are the evidence. I measured the group-basis build of a 10M-row
source at 0.28 s and 11 MB of WAL with one `GROUP BY` per partition,
against 26–32 s and 5.0 GB for the per-row ledger's chunked build, and
1.4–1.9 s at 100M against 370–380 s; the peak `xmin` hold was 0.3–0.5 s per
partition at 100M.

[OPEN: the build plan: one `GROUP BY` per structure (one chunk per source,
or per partition) or key-range chunks, and whether pages apply from the
build's first moment or wait for it. Recommendation: one `GROUP BY` per
structure, the measured cheapest, once the build-path model confirms it is
exact with pages applying throughout.]

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
  outstanding), `live`. A one-pass build adds
  `catching_up` (applying, awaiting its go-live re-read). Plus `paused` and
  `quarantined` from
  [ADR-0014](0014-pause-and-drop-a-transform.md) and
  [ADR-0003](0003-quarantine-storage-and-api.md).
- **Must:** a `live` reader of an upstream (another definition's target it
  reads, directly or as a JOIN side) that is not `live` reports the
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
- **Must (R4, I4 per group):** a group whose partials are all zero and that
  has a basis stays, as a zero group, until the contiguous drained prefix
  passes its stamp. The stamp is set by an absolute write, and only by one:
  the newest segment holding a change its snapshot saw (R5). A zero group
  with no basis protects nothing and may go at once; a group with any
  non-zero partial is never collected, whatever its row count. The GC claims
  groups `FOR UPDATE SKIP LOCKED` in key order, so it never waits on a page
  or a recompute: a recompute that loses its group to the GC writes a fresh
  group with a fresh basis. Zero groups left in place are not free: in my
  JOIN model, 4,771 of them on 10 hot keys made pages 16x slower until they
  were collected.
- Every repair of a definition is a rebuild: Re-derive over the key space
  for a 1-1 target, a group rebuild for an aggregate
  ([An aggregate's build](#an-aggregates-build-adds-each-chunks-partials-under-its-group-locks)),
  each safe against any pending change by I2. A resume starts one, as does
  an explicit `request_backfill` and a capture re-install (the call's own
  transaction moves each `live` reader to `backfilling` and enqueues its
  plan and sweep jobs, so the status already reads `backfilling` when the
  call returns). The only repair that is not a rebuild is a quarantine
  release (`release_key`): on a 1-1 target a per-key Re-derive (a staged
  `Recompute`) of the one key, and on an aggregate a recompute of the key's
  groups (below). A repair a caller waits on is therefore visible in the
  status, never parked where only the repair knows it hasn't run.
- **Must (a release on an aggregate):** while a key is held its changes are
  skipped for the aggregate, so the groups it feeds drift until the release;
  `await_converged` and `self_check` hold back on a held key with parked
  changes. A release, and the
  [automatic retry](0003-quarantine-storage-and-api.md#retry-policy) of a
  held key, recompute every group the key could owe (R8): every group named
  by the old and new images of the changes skipped while it was held,
  including the change that evicted it, plus the group of the key's current
  row. A resume releases
  every held key and rebuilds
  ([ADR-0003](0003-quarantine-storage-and-api.md#resume-releases-the-transforms-held-keys)),
  and its rebuild covers these groups among the target's own and the
  source's (R8). The recompute is the ordinary group recompute, an absolute
  write under R1 (its lock statement first), R5, R6 and R8; the key's
  changes still in the ring after the release apply as deltas, filtered by
  each group's basis like any page's.
- **Must:** the parked changes are discarded, not replayed, so a held key's
  parked row stays one folded row per key and carries no transition images.
  The recompute is absolute, so it is exact whatever other absolute writes
  (another key's release, the recompute of a group an image-less change
  names, a rebuild) reached those groups during the hold.
- **Must:** on a JOIN side, the release recomputes the side's `P`/`M` cells
  for those groups and carries the change to `T` under the `J` lock (JR1–JR3
  in [ADR-0019](0019-join-transforms.md)).
- A release's recompute holds its groups' locks for its whole `GROUP BY`
  ([the cost above](#absolute-group-writes-lock-first-and-cover-every-group-they-could-owe));
  without an index leading with the group key it scans the source, and pages
  touching those groups wait for it.

### A change Trellis never saw stays wrong until it is detected and repaired

A per-row ledger repaired a missed change (capture switched off around a
write, a partial restore) the next time the row was written, because its
Apply compared the new image with the entry. A group-basis Apply computes
each delta from the change's own old image, so a group that missed a change
stays wrong until something recomputes it.

- **Must:** nothing repairs an aggregate on its own: no background sweep and
  no automatic recompute. `self_check` detects the drift
  ([ADR-0013](0013-self-check-production-recompute-audit.md)), and the
  operator repairs it with `request_backfill` or `PAUSE`/`RESUME`, both
  rebuilds.

A rolling background recompute would put a continuous source `GROUP BY`
load on the database, hold group locks that compete with pages, need a
cycle-length setting, and silently mask Trellis's own bugs. A targeted
repair of only the groups `self_check` names is additive, on top of the
locked group recompute, so it can come later.

### What `live` promises

`live` tells an operator that a
transform is in its **steady state**. Once a definition reports `live`, a
watermark token taken after any commit and awaited with
`Trellis::await_converged` guarantees that the transform's values reflect
every commit at or before that token. Nothing else is needed.

For that to hold, `live` is strict: it is reported only once every chunk has
committed. Every captured change
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

Before the token is reached, an aggregate target makes no promise about the
combination of values a reader sees
([Readers see a group only while its row count is above 0](#readers-see-a-group-only-while-its-row-count-is-above-0)).

## Truncate, DDL, drop

- `AFTER TRUNCATE` statement triggers per captured table stage one truncate
  row; the drain barrier is restated under xid order.
- **Must (R9, the barrier holds on both sides):** a truncate's page waits
  for every earlier page of its table and every later page waits for it. It
  passes the same claim check as any page. Sealed segments are planned
  strictly in order, or the barrier is evaluated when a page is claimed,
  not when it is planned. *Evidence:* in my group-basis model the floor
  alone let a page that had locked its groups before the wipe, and read the
  floor before it was raised, re-create counts the wipe had removed; a
  planner that could plan a segment before its predecessor lost rows across
  a truncate in 7 of 220 runs with the race widened, and 0 of 280 once it
  planned in order.
- **A source truncate resets the target and raises a floor**, an O(1)
  write rather than a tombstone per entry or group. The page that applies
  it empties the target's ordering state (a 1-1 target's ledger; an
  aggregate's partials, bases and zero groups, resetting to not-run every
  build chunk whose snapshot saw the truncate), clears the target, and
  raises the target's
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
  them. A truncate of a leaf or middle of a partitioned source resets
  nothing and sets no floor or barrier: it queues that node's correction
  ([ADR-0017](0017-partitioned-sources.md#a-leaf-or-middle-truncate-queues-that-nodes-correction)).
- Source `ALTER TABLE` on a read column follows the schema-changed marker
  and regeneration rule; on any other column it is invisible.
- Dropping the last definition on a table drops its triggers on the next
  reconcile pass under I6's retry loop.

## Multi-hop: a target's writes reach its readers through the mutation seam

A target is never captured by triggers. A definition that reads another
definition's target (a chained hop) learns of its changes from rows the
writer stages in the same transaction as the write. A worker propagating
downstream appends to the active segment, never the batch it is draining.

**Decision point.** The seam's image feed is how a target is a change
source. An image-less `Recompute` of a key tells a group-basis reader the
row's new group but not the group it left, so whenever the reader's group
depends on the upstream row's values the old group keeps a member it lost.
A group-basis or JOIN reader needs exact images, as capture gives a source.

- **Must:** every target with at least one applying aggregate or JOIN
  reader stages, per changed key, the exact old and new images, ordered by
  the write token. The old image comes from the writer's own read under its
  lock (a delete's from its `RETURNING`), the new image from a re-read by
  key, and the `row_txid` is the writer's transaction, which orders the
  change against the reader's bases. An upstream group whose row count is
  not above 0 has no visible row, so it appears in the images as absent,
  and the reader's sums telescope.
- **Must (one feed per target):** if any reader needs images, the target
  stages images, and its 1-1 readers consume the same rows. A target whose
  readers are all 1-1 may stage key-only `Recompute` rows. A target with no
  readers stages nothing.
- **Must:** a group-basis aggregate, or a JOIN side, reading a target
  consumes the images exactly as it consumes captured ones; the image chain
  (I3) holds because the upstream writes a row only under its lock, in
  write-token order.
- **Must:** a write that never reaches the seam (an upstream's own build or
  rebuild, at a resume or by its own Re-derive build) parks a catch-up for
  its readers. For a group-basis or JOIN reader that catch-up is a rebuild
  of the reader.
- **Must:** a writer that resolved the target's feed before a new reader's
  define committed must not leave the reader's build missing its write. A
  deterministic test freezes such a writer between its check and its
  commit.
- **The chains this serves:** 1-1 over a 1-1 or aggregate target; an
  aggregate over a 1-1 target (the computed-measure pattern: a 1-1
  transform computes what an aggregate argument can't express, since the
  grammar has no `*` or `/`, and an aggregate then sums it); an aggregate
  over an aggregate target (a
  rollup); and a JOIN side that is a target.
- **Cost to state.** A target with an aggregate or JOIN reader pays a
  re-read by key per changed key and wider staged rows. A terminal target
  pays nothing. *Evidence:* in my group-basis model, a downstream grouped
  by an upstream value was wrong under key-only recomputes and exact under
  images, with upstream pages drained out of order and a downstream
  recompute between them.

Refusing aggregates and JOIN sides over a target would lose the
computed-measure pattern. A rollup kept by recomputing its groups from the
upstream would be a narrow special case with refusal rules of its own.

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
| **Capture by logical replication** (a `pgoutput` slot decoded by an intake thread). It keeps everything off the write path and needs no trigger on user tables. | I measured staging capped at ~120k rows/s at 1,000 rows/commit and 18.6k at 1 row/commit whatever the writers do, with a 10M-row `COPY` never staged, and total CPU per row 4–5x the trigger's, paid later. A stopped Trellis pins the whole database's WAL rather than growing a ring at ~188 B/row. The decoder's 32-bit xid must be widened to `xid8` for I2, and I found the naive widening silently wrong for every id before an epoch boundary. Decoding needs `REPLICA IDENTITY FULL` on every source an aggregate reads, for its old images. Slot loss, failover and the replication privilege are operational surface the trigger has none of. |
| **Row-level triggers.** | I measured 5–10x the statement trigger's cost above 1 row/commit. |
| **Column-agnostic image encodings** (`to_jsonb`, `jsonb_each_text`, `::text`, `hstore(NEW)`). | Not byte-identical to `tuple_to_json`, or identical but reading every column, which I measured costing 40x on a 100 KB column. |
| **Ordering by LSN with live reads guarded by horizons and basis checks:** the recompute horizon and extinct horizon, a minimum-image LSN, the build horizon, per-key basis locks, the watermark wait before enumeration, recompute and vanished-image flags, bucketed advisory locks, a keep-every-image fold, prior image on enumeration. | Each closes one pairing of one producer against one drain order, found in review of the previous fix at roughly two new problems per fix, and the generative suite found none of them. A live read has no way to tell which commits it reflects; only a snapshot does (I2). |
| **Visibility-only I2** on the per-row ledger, and **Apply stamping its own snapshot as the basis.** | Each fails scenarios of the interleaving experiment: visibility-only fails three, and stamping Apply's own snapshot fails two. A group basis needs no position check, because its writes to a group commute. |
| **Group rows as absolute writes ordered by position:** forced recomputes from a live `GROUP BY`, an existence probe and pre-lock before incrementing. | Not orderable without a horizon; the probe's scan is the 40k-group cliff where nothing drains; the pre-lock deadlocks on new groups. The group basis orders the same absolute writes by a snapshot per group, taken in the statement that reads, after a sorted lock. |
| **A per-row ledger for aggregates**: one entry per source row holding its group, contribution and ordering state. | I measured 15.1–15.3 GB of ledger for a 100M-row source in 50k groups (151–153 B per entry, its basis 33–39 B), against 5 MB of group state; a 100M build at 370–380 s and 58–71 GB of WAL against 1.4–1.9 s; a page of 100k changes at 3.2–3.35 s against 0.18–0.31 s; and a partition `DROP` of 10M rows at 45–51 s and 6.2 GB of WAL against 10–30 ms. What the ledger gave in return was self-repair on a row's next write, and the `MIN`/`MAX`-style aggregates this ADR refuses. |
| **One set-based `GROUP BY` build with changes withheld, then a go-live re-read and orphan sweep.** | The re-read is a full enumeration per build that scales with table size, not with what changed; I ran it at 100M, where it reaches `live` after 2,305 s and its drain then runs out of memory. Nothing can tell a table unchanged since the build from one changed and changed back, so the re-read could never be skipped. A 1-1 build is per-row work and its acceptance bar is linear time; an aggregate's build keeps the one-pass `GROUP BY` and drops the withholding, because its chunks apply alongside pages under the chunk gate. |
| **`catching_up` as a stored status with marker fences, generations and go-live catch-ups.** | Needed only by a build that skips changes. A definition on the Re-derive build applies from its first chunk, so there is nothing to catch up; builds not yet on it keep the status ([A build](#a-build-is-re-derive-over-chunks-and-applies-from-its-first-chunk)). |
| **Bounding drain memory by running fewer workers.** | Each worker then holds more buckets; I measured that it bounds nothing. |
| **Fixing the build's cost by chunk size.** | I ran 10x larger chunks and they left 2.0x the time and 3.7x the WAL. |
| **Resolve every image-less trigger into an image at staging time**, so Phase 2 never reads live state. | Under trigger capture the only image-less change is a delete, and every other image-less producer is an enumeration this ADR replaces with Re-derive or a recompute under lock. What this leaves untouched on a 1-1 target is out-of-order parallel batches for one key, which needs `applied_lsn`, not images. A resolver on the seal path is itself a live read racing the drains. |
| **Serialize absolute writes per target** through one writer or one advisory lock. | Does not fix identity: a serialized write still reads live state ahead of pending deltas and still needs a horizon. Serializes the build, the largest producer of absolute writes, which is already CPU-bound on eight workers at 14–15k rows/s. |
| **Thread source transaction ids through the ring.** | Adopted, in the exact form `row_txid` gives for free under trigger capture. |

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
  ([validation 6](#validation-and-acceptance)).
- **Storage grows with the source only for 1-1 targets.** A 1-1 target
  carries a slim ledger of one row per source row; an aggregate target
  carries state per group (5 MB for 50k groups, whatever the source's
  size). No full old row is ever staged (the ring carries the imaged columns
  only) and `REPLICA IDENTITY FULL` is not required anywhere.
- **An aggregate doesn't repair itself.** A change Trellis never saw leaves
  its groups wrong until `self_check` finds them and an operator rebuilds
  the definition, which at a billion rows is a full build
  ([A change Trellis never saw](#a-change-trellis-never-saw-stays-wrong-until-it-is-detected-and-repaired)).
- **Readers before convergence see a weaker picture than a per-row ledger
  gave.** The ledger counted each row at most once, so its counts were never
  negative; the group basis can show impossible combinations between pages
  ([Readers](#readers-see-a-group-only-while-its-row-count-is-above-0)).
- **The aggregate surface is narrow.** `COUNT`, and `SUM`/`AVG` over
  integers and `numeric(p,s)`; every other aggregate is refused at define,
  and a definition using one doesn't work until its family is added. A
  plain `numeric` column must declare a scale to be summed.
- **Each producer of an absolute group write owns its enumeration** (R8).
  That is where a new producer's bugs would appear, so each is stated and
  tested on its own.
- **The failure mode of a broken capture is loud, not silent.** A schema
  change to a read column never fails the statement (I6). A missing ring
  table or revoked privilege does, with a `CONTEXT` line naming the capture
  function:
  the application sees the fault at once, not after data is lost. An owner who disables, drops or recreates a
  plain table's trigger, or replaces its function, pauses the table's readers
  at the next pass; the silent-uncapture paths left are in
  [known correctness gaps — Capture disrupted and restored between two passes](../known-correctness-gaps.md#capture-disrupted-and-restored-between-two-passes).
- **Hosted compatibility** is core Postgres for everything required
  (statement triggers, transition tables, `SECURITY DEFINER`, the `TRIGGER`
  privilege); no event trigger is needed.
  No replication privilege, slot or publication is needed, and a
  physical backup-and-restore carries the ring with the data. A logical dump
  carries it too, and the restore discards it ([I2](#invariants)).
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
- **A 1-1 build is per-row work on every worker**, not one pass. The
  acceptance bar is linear time with a per-row cost independent of table
  size, checked at 100M ([validation 4](#validation-and-acceptance)). An
  aggregate's build is a `GROUP BY` per chunk, and holds the `xmin` horizon
  for one chunk's statement.
- **Registration latency does not depend on table size**, and no definition
  is `live` when registration returns: callers poll `Trellis::status` to
  `live`, then use a token
  ([embedding](../embedding.md#poll-to-live-dont-wait)).
- **I took every number above on one box.** The capture-throughput
  experiments and the first four ledger experiments ran on tmpfs; the
  disk-backed numbers are from a single NVMe with ~0.4 ms fsyncs and, for the
  ledger experiments' disk tier, a box that was not always quiet.
  Within-session ratios stand; absolute bars are restated in
  [validation 5](#validation-and-acceptance).

## Validation and acceptance

Nothing below polls for convergence.

1. **One deterministic interleaving test per scenario of the interleaving
   experiment** (all seventeen) on the real engine, hand-driven with advisory
   locks, plus one for each of the nine superseded problems in the inventory of
   producer pairings that motivated this design.
2. **One deterministic test per scenario of the group-basis model** that
   applies to the engine: the read-after-lock and stale-basis cases, the
   fold that straddles a basis, the image chains under nested writes and
   cascades, the enumeration of a build over vacated groups, zero groups
   and the GC racing a recompute, the truncate barrier's earlier side and
   the chunk reset, the chunk gate (a page that doesn't lock the chunk
   rows), a recompute during a chunked build, and the chained-source cases
   ([Multi-hop](#multi-hop-a-targets-writes-reach-its-readers-through-the-mutation-seam)).
   Each scenario the soak can't reach is the only evidence for its rule.
3. **The generative concurrent tier** with planted violations (skip the
   lock, compare LSN instead of visibility, delete tombstones early, fold
   sides per key, collect zero groups early), each caught within the
   nightly budget. **Merge gate.**
4. **`build-under-load` at 100M** on the implementation branch, disk-backed,
   under a 16 GB memory cap: oracle ok, memory bounded, WAL retention
   bounded, time linear in N, per-row cost flat from 1M to 100M.
   **Milestone exit for the build.**
5. **A quiet-box, disk-backed benchmark round** with a checkpoint inside the
   window and storage stated on every number: `fold-in-ratio`,
   `group-contention` (400/4k/40k), `throughput-ramp`, `build-under-load`,
   and the two capture benchmarks (statement against row triggers, and
   scaling with writers, including the cost of the transition image a
   re-read row carries), each with a same-session control on the
   pre-change code: the group basis against the per-row ledger. The bars in
   this ADR are restated from that round. **Milestone exit for the group
   basis.**
6. **Writer-coupling tests:** schema change of a read column under load
   (writes succeed, definition pauses, regeneration restores capture); join
   and drop under a 30 s open transaction (worst writer wait under 100 ms);
   revoked privilege and disabled trigger (audit reports); `REPEATABLE
   READ` and `SERIALIZABLE` writers straddling a seal (no lost rows).
7. **Drain paging:** a segment several times the batch cap drains with peak
   RSS bounded by the cap; a key whose changes straddle two segments, a
   paged truncate segment, and a reclaim between two pages all converge.

## Reusable material

Prototype branches on the fork (mmmries/trellis) hold the ledger Apply
experiments with their results and handoff notes, the chunked ledger build
with `build-under-load` (the disk columns and the memory sampler), a
trigger-capture variant of that build, and the trigger-capture spike (the
capture SQL with the `new_only` and `skip_noop` shapes, and the phase-1
harness). The fence mirror is already in `main`. The group-basis model is a
SQL and Python harness against real Postgres, with its scenarios, soak
matrix and negative controls, each re-runnable from one script.

## More information

- [Documenting Architecture Decisions](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)
  for the ADR form ([ADR-0001](0001-use-adr.md)).
