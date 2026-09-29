---
status: accepted
date: 2026-09-27
deciders: Michael Ries
consulted:
informed:
---

# Data Flow: Synchronous Capture, Asynchronous Exactly-Once Derivation

*Rewritten 2026-09-27 for epic #556. The original record of 2026-08-15 chose
logical replication for capture and left the ordering of absolute writes to
the apply layer; both are now rejected alternatives below, with the
measurements that rejected them. The design elements that changed the
architecture are marked **Decision point**.*

Trellis derives tables from other tables. Two questions decide the whole
design: *where* a derivation runs, and *how* a source change is captured and
ordered so that the derivation is applied exactly once. This ADR answers
both.

**Derivation is asynchronous.** Source changes are batched, and derived
values are computed and written by drain workers outside the application's
transactions. That part of the 2026-08-15 decision stands, for the reasons in
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

The reason for the rewrite is the class of write the original design left
unordered. A **delta** (`+f(new) − f(old)` into a group) commutes with every
other delta, so parallel workers may apply deltas in any order and the
exactly-once argument of
[stage 05](../staging-and-claiming/05-apply-and-exactly-once-deltas.md)
holds. An **absolute write**, any value derived from a read, does not
commute: it is right only if nothing later in the change order has already
been applied to the same row or group. Trellis had many producers of absolute
writes (an image-less recompute, an existence probe, the relationship reverse
fallback, a projection refresh, a go-live re-read), each reading live
`READ COMMITTED` state and each racing out-of-order parallel drains. Nearly
every correctness bug of September 2026 sat on that seam, and every fix added
one more precedence rule for one more pairing (#556's inventory). This ADR
establishes one ordering mechanism for every absolute write and removes the
rules. The research behind it (#558, #565, #617) also found scaling cliffs in
the capture and build paths that the same mechanism removes:

- the logical replication slot staged at most about 120k rows/s whatever the
  writers did, and never captured a 10M-row `COPY`
  (#565 [E1](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5842011977));
- the go-live re-read could not finish at 100M rows on a 31 GB box, because
  a drain batch held its whole share of a segment in memory
  (#617 [step 3](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5857993459));
- the relationship reverse path lost updates under to-side churn (#582,
  #558 [experiment 4](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5845224204)).

Conventions: **must** is a requirement the evidence forces; **should** is the
current recommendation and is open to the debate on #618; evidence links
point at the comment that produced the number.

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
invariants below: a disabled trigger strands data (I6's audit), and a broken
ring must not fail the application's statement (I6's never-fail rule).

## Invariants

The design is these invariants; every element below exists to hold one of
them. I0 to I5 are the ledger note's (#558) with the amendments the
experiments forced. I6 to I8 were learned from #565 and #617.

| | Invariant | Evidence, and the amendment it needed |
|---|---|---|
| **I0** | **One database.** Sources, the ring, every ledger and every target live in one Postgres database, so one snapshot orders every commit Trellis will see. | Design premise; made exact by trigger capture, which makes a ring row's transaction the source commit's. Nothing here works across databases. |
| **I1** | **Read after lock.** Every live read that feeds an absolute write happens after the writer holds the lock on the ledger state it will write, and the snapshot it stores is taken **in the same statement** as the read. | [Exp 2](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5840353484) scenarios 2/2b hold (Apply demonstrably blocks on the entry lock). [Exp 1b](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5840353484): a snapshot taken in a separate statement differed from the read's in 99.7% of samples under load; the stored basis is the full `pg_snapshot`, not `xmin`/`xmax`. |
| **I2** | **Visibility-checked application.** A change C for row r is applied iff C's transaction is **not** visible in r's basis snapshot **and** C's ring position is above r's `applied_lsn`. Skipping is exact, never "maybe counted, re-derive". | Exp 2: skip-iff-visible alone fails scenarios 5b/9/9c (same-key order across batches is not decidable from visibility); stamping Apply's own snapshot fails 9b/9d; visible-or-`applied_lsn` passes all seventeen. An in-flight id is decidable from the stored list ([exp 1b](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5840353484): 0 disagreements over ~1.1M pairs); #617 saw 0 in-progress cases at 10M. |
| **I3** | **Per-row ordering state; groups are pure sums.** The ledger entry is the only place a row's applied contribution and group live. A group value is the sum of its entries' contributions, so group updates commute and a group row is only ever incremented. For relationships the to-side value is factored out ([Relationships](#relationships-are-factored)). | Exp 2 scenarios 4 and 5; [exp 4b](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5851003675) for the factoring. |
| **I4** | **Tombstones live until the batch watermark passes.** A deleted row's entry stays, with its `applied_lsn`, until every batch at or below its own is fully applied on that target. | Exp 2 scenario 9 (an older update resurrects a deleted row without it). The batch-watermark form is exact under triggers because a same-key predecessor of a delete committed before the delete's trigger ran ([trigger amendment](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5847674780)). |
| **I5** | **One lock order, taken as one sorted batch.** Ledger entries, then parent rows, then partials, then groups, each locked in key order in one statement per class. Never a per-row loop. | Exp 2 finding 3: a per-row loop deadlocked 19–22 times in 20 s; the sorted batch never did. **Not yet total:** the factored variant deadlocked 9–16 times per 100k-fan-out run on disk and 0 on tmpfs ([exp 4b](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5851003675)). The cycle must be found before I5 is stated as proven ([open question 3](#open-questions)). |
| **I6** | **Never block, and never fail, an application writer.** No Trellis transaction takes a lock an application write can queue behind, except the join and drop fences, which are bounded by `lock_timeout` and retried. No Trellis-side condition (a missing column, a broken ring) fails the application's statement. | [E7](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119): a bare `CREATE TRIGGER` stalled every writer for 25 s; with a 50 ms `lock_timeout` retry the worst wait was 52 ms. [E4](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5842011977): a renamed read column failed every insert until regeneration. The retire path already takes its `TRUNCATE` lock `NOWAIT`. |
| **I7** | **No Trellis transaction waits for a lock while it holds a snapshot open.** Every lock statement runs under a `lock_timeout`; on timeout the transaction rolls back and the work is retried from outside any transaction. | [#617 final](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5859141160): a drain batch's ledger insert waited 1 h 50 min behind chunk transactions; the open transaction pinned the slot's `restart_lsn`, `pg_wal` reached 190 GB and the sealer was refused for the whole wait. Without a slot the WAL pin goes, but an open snapshot still holds back vacuum and the sealer, so the invariant stays. |
| **I8** | **Memory is bounded by a batch cap, never by segment size.** A drain batch folds, re-reads and applies at most a fixed number of changes and pages through a larger bucket. | [#617 step 3](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5857993459): ~650 B per staged row held per worker, 6.32 GB at 10M, OOM at 100M; fewer workers bound nothing because each worker then holds more buckets. |

## Capture by statement triggers

**Decision point.** Trellis code runs inside the application's transactions.
What it does there is one append.

- **Must:** three `AFTER … FOR EACH STATEMENT` triggers per captured table
  (insert, update, delete; a trigger with transition tables takes one event),
  each appending the statement's rows to the active ring segment in the
  writer's transaction. The ring row's `row_txid` (`DEFAULT
  pg_current_xact_id()`) is therefore the source commit's `xid8`, and it is
  the identity I2 checks. *Evidence:* statement triggers beat row triggers
  5–10x above 1 row/commit
  ([E1](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5842011977));
  capture scales with writers to ~1.9M rows/s at 1,000 rows/commit and ~190k
  at 1 row/commit
  ([E2](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119));
  at 1 row/commit the tax vanishes under a real fsync (within 1% of no
  capture at 16 writers,
  [E1 disk](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5850979670)).
- **Must:** the generated function names only the primary key and the
  columns the table's definitions read (plus relationship `from_col`s), emits
  **NEW-only** images (a delete emits the key), renders every value with
  `format('%s', col)` under the five output settings Trellis pins
  (`DETERMINISTIC_TEXT_OUTPUT_GUCS`) as `SET` clauses on the function, carries
  a `WHEN` clause that skips an update touching no read column, is
  `SECURITY DEFINER` owned by the Trellis role (the one role that owns
  Trellis's schema and performs every Trellis operation; #622 plan Q3) with
  a pinned `search_path`, is `ENABLE ALWAYS`, and reads the active slot
  schema-qualified through the sequence mirror
  ([The seal fence](#the-seal-fence-under-application-writers)). *Evidence:*
  [E3](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119):
  only `format()` and `hstore` with pinned settings are byte-identical to
  `tuple_to_json` across all 38 type families; `::text` and `to_jsonb` are
  not; an unpinned image silently depends on the application session's
  `DateStyle`, `TimeZone`, `bytea_output`, `IntervalStyle` and
  `extra_float_digits`; an encoding that reads every column costs 3.3 ms and
  323 KB per row on a 100 KB column, 40x the control, where naming only the
  read columns costs +30 µs. NEW-only is exact because the ledger holds the
  old side ([trigger amendment](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5847674780)).
  [E6](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119)
  for the privilege model: the application needs nothing on Trellis's
  schema. The Trellis role must own each source table or be a member of its
  owning role, because `ENABLE ALWAYS` needs ownership, not just `TRIGGER`
  (#622 plan finding 3). It needs no superuser.
- **Must (I6):** `CREATE TRIGGER` and `DROP TRIGGER` run under a short
  `lock_timeout` in a retry loop, one attempt per interval, until they land.
  *Evidence:* [E7](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119).
  The join's commit is also the **join fence**: `SHARE ROW EXCLUSIVE` waits
  out every writer in flight, so every commit either precedes the trigger
  and is visible to any snapshot taken after it, or ran the trigger. No
  separate fence, marker or generation is needed to make a table's join
  gap-free.
- **Must (I6):** a schema change to a read column never fails the
  application's write. **Should:** the generated function catches the
  undefined-column error, writes a *schema-changed* marker row for the table
  instead of an image, and returns; the drain that meets the marker pauses
  every definition reading the column with a status error and regenerates
  the function; where the host allows `CREATE EVENT TRIGGER`, an event trigger
  regenerates the function inside the DDL's own transaction and the marker
  path is the fallback. *Evidence:*
  [E4](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5842011977)
  (rename or drop of a named column fails every insert until regenerated;
  add, rename or drop of any *other* column is harmless),
  [E10](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119)
  (event triggers available to the customer's admin role everywhere surveyed
  except, undocumented, Azure Flexible and Crunchy Bridge). The `EXCEPTION`
  block costs a subtransaction per statement
  ([open question 9](#open-questions)).
- **Must:** the self-check audit
  ([ADR-0013](0013-self-check-production-recompute-audit.md)) verifies from
  `pg_trigger` that every captured table's three triggers exist, are owned by
  the Trellis role and are enabled, and reports a missing or disabled one
  before any recompute comparison. Replica-mode sessions are covered by
  `ENABLE ALWAYS`; an owner who disables or drops the trigger by name is
  documented as uncaptured until the audit runs. *Evidence:*
  [E6](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119).
- **Only the staging worker creates or drops triggers**, from the catalog
  alone, on its reconcile pass: a table gains triggers when something
  registers a reader of it and loses them when its last reader is dropped.
  Registration itself does no source reads and no capture work; it writes
  the definition, its ledger tables and its chunk plan
  ([A build](#a-build-is-re-derive-over-chunks-and-applies-from-its-first-chunk)).
  Registering processes need catalog access and the right to create target
  tables, nothing on the sources.
- **Should:** there is one capture path. The slot path is deleted, not kept
  behind a flag: keeping it keeps the decoder-xid widening and its anchor
  rule, a `src_xid` column, intake and its watermark, `REPLICA IDENTITY
  FULL`, the intake wait before enumeration, and a second proof of every
  ordering argument ([open question 1](#open-questions)).
- **Cost to state.** On disk at 1,000 rows/commit with 16 writers the
  writer's throughput is 0.49x no-capture and the ring row is ~2.9x the
  source row's WAL bytes
  ([E1 disk](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5850979670));
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
What changes is who the writers are: every ring writer used to be Trellis's
own `READ COMMITTED` transaction; now a writer is the application at whatever
isolation level it chose.

- **Must:** the writer reads the active slot through the sequence mirror
  (`ring_slot_mirror`, `pg_sequence_last_value()`), which the seal sets in
  phase 2 before the `xmax` bump, and the writer's xid is assigned in the
  same statement as that read. *Evidence:*
  [E4 inference 6](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5842011977):
  a `REPEATABLE READ` or `SERIALIZABLE` writer read a pointer two flips old
  from `segment_pointer` and its rows landed in a slot no batch would read.
  Landed by #597 (#595); stage 03 restates the proof for the mirror.
- **Must:** the truncate barrier decides `has_truncate` after the batch's
  contents are fixed (#598).
- **Per-key order is `(lsn, change_id)`** with `lsn =
  pg_current_wal_insert_lsn()` read in the trigger; the fold's ordering rule
  is unchanged. Cross-key commit order is not available from triggers and,
  under I3, not needed. *Evidence:*
  [E4](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5842011977):
  per-key order held in every interleaving; a `CACHE`d sequence alone
  inverted a key's order; with the LSN it holds because a second writer on
  the same key runs its trigger only after the first commits.
- The maintenance loop is not "the only sealer" in any argument here; nothing
  below relies on what drains before or after a seal.

## The ledger: one target-owned entry per source row

**Decision point.** The target, not the ring and not a projection, owns the
state that orders its writes.

- **Must:** per aggregate target, `<target>__ledger(from_key primary key,
  group_key, join_key, contrib, applied_lsn pg_lsn, basis pg_snapshot)`,
  indexed on `group_key` and on `join_key`; written in the same transaction
  as the target, entries locked in key order before any group row is
  touched. For a 1-1 target the ledger is the target row itself plus
  `applied_lsn`, `basis` and a tombstone marker. *Evidence:*
  [#558 design note](https://github.com/salesforce-misc/trellis/issues/558),
  [exp 3](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5842993962)
  for the schema as prototyped.
- **Must:** exactly two operations exist, and every producer is one of them.
  - **Re-derive(r).** Lock r's entry (inserting a placeholder if absent).
    In one statement: take the snapshot, read r's source row and, through
    each relationship, the parent's applied values, compute the group key
    and contribution. Diff against the entry: subtract the old contribution
    from the old group, add the new to the new group, replace the entry with
    the new values, the new basis and the read's position. If r no longer
    exists in its source, the entry becomes a tombstone and its contribution
    leaves its group.
  - **Apply(r, C, image).** Lock r's entry. If C is visible in the entry's
    basis, or C's position is at or below `applied_lsn`, stop (I2).
    Otherwise `delta = f(NEW) − entry.contrib`, group += delta, entry :=
    (new group, new contribution, C's position, basis unchanged). A delete
    is `f(NEW) = 0` and the entry becomes a tombstone. A row with no entry
    is an insert of NEW.

  | Producer | Operation |
  |---|---|
  | Captured insert/update/delete | Apply |
  | Build (any shape), resume rebuild, `request_backfill`, quarantine release, column resume, `ALTER TRANSFORM` added column | Re-derive over the definition's key space, chunked |
  | Relationship parent change | The parent operation below; per-child Re-derive only for non-linear fields |
  | To-side `TRUNCATE` | Parent operation with `f = 0` for every parent of the table; the join-key index names the children |
  | Source `TRUNCATE` | Every entry from that source becomes a tombstone with the truncate's position; groups decrement |
  | Chained hop | Apply, where C is the upstream apply transaction ([Multi-hop](#multi-hop-a-target-is-captured-like-a-source)) |

- **Must keep contributions.** A membership-only ledger (group key and basis,
  contribution recomputed from source) saves 1–2% of WAL and produced wrong
  relationship targets on both shapes it ran
  ([exp 3](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5842993962),
  [exp 4](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5845224204)).
- **Non-invertible fields** (`MIN`, `MAX`, `BOOL_AND`, composed expressions)
  are recomputed from the ledger's contributions by an index scan on
  `group_key` under the group's lock, never from the source. This is the one
  place a group value is written absolutely, and it is ordered by the same
  ledger locks as every increment.
- **Cost to state.** Hot path with the prototype (the existence probe and
  pre-lock still in place): 1–9% at 8 workers on 400 and 4k groups, 19% with
  one worker; WAL 1.6–1.7x from the heap tuple and PK index entry per source
  row; the ledger on disk was ~3x the source table at 10M
  ([exp 3](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5842993962),
  [#617 step 2](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5852918496)).
  The upside of deleting the probe and pre-lock (#326's 40k-group cliff,
  where nothing drains in any mode) is unmeasured because the prototype kept
  them ([open question 4](#open-questions)).

### Apply is one set-based statement per batch per target, and the batch is bounded

- **Should:** with `row_txid` exact, I2's skip rule is one predicate over the
  batch joined to the ledger, so a batch's Apply on a target is: one sorted
  lock statement over the entries it touches, one `update … from batch`
  that computes deltas and rewrites entries, one sorted upsert of group
  increments. The fold's "mixed visibility → re-derive" case disappears for
  image-bearing changes
  ([trigger amendment](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5847674780);
  [exp 2](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5840353484)
  finding 1).
- **Must (fold):** an image-less `op = 'delete'` is the key's final state
  within a fold window, whatever precedes it. *Evidence:*
  [#617 step 4 dry run](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5853104094):
  under NEW-only capture, a delete sharing a fold window with an earlier
  write of the same key was dropped by the "latest row with any image" rule;
  11–12 groups wrong at 1M.
- **Must (fold):** the fold statement never plans a nested loop over stale
  ring statistics (#581).
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
  durable per-bucket cursor; only a bucket's last page marks it drained
  (#620). *Evidence:*
  [#617 step 3](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5857993459),
  [#556 requirement](https://github.com/salesforce-misc/trellis/issues/556#issuecomment-5859042432).
- **Must (I7):** every lock statement in Apply runs under `lock_timeout`; a
  timeout releases the claim's transaction and retries the page after a
  backoff, outside any transaction.

## Relationships are factored

**Decision point.** A to-one relationship's parent value is not a settled
projection with a lifecycle of its own. It is an applied value in a table the
target owns, under the same lock discipline as the ledger, and the projection
table has no correctness role.

- **Must:** three tables per relationship-aggregate target: **L** (the
  ledger: the child's from-side contribution and its join key, factored
  fields blank), **P** (partials: `n` per `(group, parent)`, how many of
  that parent's children currently contribute to that group), **T** (parent:
  each parent's applied to-side values, `applied_lsn`, basis). A factored
  field's group value is `Σ_p P[g,p].n × T[p].v`. A child change upserts L,
  moves its P count (−1 old parent/group, +1 new) and computes its group
  delta with T's **applied** parent value, never the live one. A parent
  change locks `T[p]`, skips per I2, reads P's rows for p under the lock,
  adds `n × Δv` to each group, and sets T. **A parent change never reads or
  writes a child.** T is the child's dependency lock, so no live from-side
  scan is needed to find a child moving into the parent (exp 2 scenario 6b).
  Lock order L → T → P → groups, each sorted (I5). *Evidence:*
  [exp 4b](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5851003675):
  oracle matched on all six shapes; 21 s to converge (tail under 1 s) on
  five of six against the reverse path's 24–113 s and one never-converged,
  wrong target; WAL 0.6–1.6x; on disk the 100k-child shape is 0.9x with
  0.65x the WAL, where the per-child rewrite is 2.4–2.9x
  ([exp 4](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5845224204)).
- **Must:** a field that is not linear in the to-side value (anything but
  `SUM`/`AVG` of a bare to-one path times a from-side factor) keeps the
  per-child path: the parent change re-derives each child through the
  ledger's join-key index, with T as the dependency lock. Detected per field
  at install and reported.
- **Must:** a 1-1 target that reads through a relationship gains only the T
  row as its dependency lock; it has no contribution to factor.
- **Should:** the projection table is deleted, or kept purely as a read
  cache with no lifecycle of its own.
- **Replica identity is not a requirement on any table.** The ledger holds
  the join key and group a child was last counted under, so a from-side FK
  re-point needs no pre-image, and T holds the parent's applied values, so a
  to-side change needs only its key. This replaces
  [ADR-0006](0006-relationships.md)'s replica-identity gates.
- **Open cost:** the forward path writes one P row per child change; at low
  fan-out P is as large as L and a random child update touches a cold P page
  (+37% WAL at fan-out 10 on tmpfs, 0.96x at fan-out 1k). Unmeasured on
  disk.

## A build is Re-derive over chunks, and applies from its first chunk

**Decision point.** A new or rebuilt definition is not built by one
set-based pass with its changes withheld and recovered afterwards. It is
built by the same operation apply uses, over chunks of the source, while
applying.

- **Must:** registration writes the definition, its ledger tables and a
  chunk plan (primary-key ranges over the source, discovered by `max()` over
  `LIMIT` so every row falls in exactly one range), and the definition is
  **applying from that commit**. Every drain worker claims chunks. A chunk
  is one short transaction: lock its entries in key order (placeholders for
  keys with no entry), one statement that takes the snapshot and reads the
  range, replace the entries, record the group deltas (next bullet). "No
  entry" means "not yet counted": a captured change for a key whose chunk
  has not run applies as an insert of NEW, a delete of one records a
  tombstone, and the chunk's later Re-derive replaces the entry under a
  snapshot that includes that commit, so the group ends up counted once.
  There is no go-live re-read, no orphan sweep and no catch-up: nothing was
  skipped. *Evidence:*
  [#617 step 2](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5852918496):
  oracle matched on four 10M runs under 2,000 writes/s, with 196–361k changes
  correctly skipped by chunk bases and 0 in-progress ids.
- **Must (I3):** group application during a build is a **separate,
  commutative, batched step**. A chunk records its group deltas without
  locking any group row, and a merger applies accumulated deltas to group
  rows in key order as one sorted upsert. **Should:** the deltas are rows in
  a per-target `__group_deltas` table (append-only, `(chunk, group_key,
  deltas)`), and the merger is any drain worker claiming a range of them
  ([open question 2](#open-questions)). *Evidence:*
  [#617 final](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5859141160):
  a 100k-row chunk touches ~63% of 100k groups, so chunk transactions that
  lock groups serialize on them; 10x larger chunks cut define-to-live by
  only a third.
- **Must (I7):** chunk transactions are short (one range, no group locks,
  under `lock_timeout`), and a captured-change batch never waits inside its
  transaction for a chunk's lock.
- **Must (backpressure):** build work yields to sealing and to the drain of
  captured changes. A worker claims a chunk only when the ring's undrained
  backlog is under a bound, and the sealer is never refused for the duration
  of a build. *Evidence:*
  [step 2](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5852918496):
  seal refused for the whole build, tail 90–213 s after `live`;
  [final](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5859141160):
  a 6.09M-row undrained segment at 100M.
- **Must (acceptance):** per-row build cost is independent of table size;
  a build of N rows runs in bounded memory and bounded WAL retention, in
  time linear in N, and converges to the oracle at 100M under load.
  *Evidence:* per-chunk cost grew from 3.2 to 5.7 worker-seconds per 10k
  rows between 1M and 10M, and the chunk rate fell from 0.96 to 0.4/s at
  100M. The prototype's ~360 µs per row needs a profile (one worker against
  eight, wait events) before the implementation plan fixes its shape.
- **Should:** the peak `xmin` hold during a build is one chunk's duration,
  not the build's (270–658 s at 10M in the prototype).
- **Chunks are a durable, claimable work queue on drain threads**, not an
  in-call loop, and the failure contract is: a chunk is heartbeated and
  reclaimed like a sealed segment, a crashed worker's chunk is re-run
  (Re-derive is idempotent), a failing chunk backs off with its error on
  `Trellis::status` and never starves other work, a resume supersedes held
  chunks through the claim fence so no stale write lands after it (#434),
  and no definition is ever `backfilling` without a chunk row or delta row
  driving it forward. A staging worker is still required for anything to
  seal; drain threads are required for anything to build.
- **Cost to state.** At 10M with the prototype's shape (group locks inside
  chunks, probe present), define-to-live was 2.0–3.2x the one-pass build's
  and WAL 3.7–5.1x. The ADR commits to the acceptance bullet, and the bars
  are restated on the implementation
  ([validation 3](#validation-and-acceptance)).

| Build at 10M rows, 8 workers, 2,000 writes/s, disk | define → `live` | WAL | oracle | tail after `live` |
|---|---|---|---|---|
| one-pass `GROUP BY` + go-live re-read | 226 s | 5.8 GB | ok | 153 s |
| prototype, 10k-row chunks | 672–716 s | 29.3–29.8 GB | ok | 90–213 s |
| prototype, 100k-row chunks | 450 s | 21.7 GB | ok | 61 s |
| one-pass at 100M | 2,305–2,322 s | – | none: OOM in the drain | – |
| prototype at 100M | never: 4,131 of 10,001 chunks in 2 h 8 min | 190 GB retained | none: stopped by the disk guard | – |

## Convergence and status

- **Should:** the watermark token stays `pg_current_wal_lsn()` read after
  the caller's commit, and `origin_lsn` is stamped from the trigger's
  pre-commit `pg_current_wal_insert_lsn()`. A commit the caller could see
  before taking its token has its ring rows in the same transaction, each
  with `origin_lsn` at or below the commit's position, so the predicate of
  [stage 07](../staging-and-claiming/07-convergence-and-await.md) over ring
  rows alone is sound; a transaction straddling the token over-reports, the
  safe direction. There is no capture watermark to check. *Evidence:*
  [E5](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5842011977).
- **Must:** three stored statuses: `waiting_to_backfill` (registered, chunk
  plan not yet written; normally momentary), `backfilling` (applying, chunks
  or deltas outstanding), `live`. Plus `paused` and `quarantined` from
  [ADR-0014](0014-pause-and-drop-a-transform.md) and
  [ADR-0003](0003-quarantine-storage-and-api.md).
- **Must:** a `live` reader of an upstream (another definition's target it
  reads directly or through a relationship) that is not `live` reports the
  upstream's state, transitively, computed when status is read and never
  stored (#497's rule).
- **Must (I4):** tombstone GC by a per-target batch watermark: once a segment
  is fully drained, the retire path deletes every tombstone on each target
  whose `applied_lsn` is at or below that segment's fence.
- Every repair that used to be a re-read (an explicit `request_backfill`, a
  resume, a quarantine release) is a rebuild: Re-derive over the key space,
  which I2 makes safe against any pending change.

### What `live` promises

*Decided 2026-09-24 (#476) and unchanged.* `live` tells an operator that a
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
settled target checks both: `live` from status, then its token.

## Truncate, DDL, drop

- `AFTER TRUNCATE` statement triggers per captured table stage one truncate
  row; the drain barrier is restated under xid order (#598). A source
  truncate tombstones every entry from that source; a to-side truncate is
  the parent operation with zero values for every parent of the table.
- Source `ALTER TABLE` on a read column follows the schema-changed marker
  and regeneration rule; on any other column it is invisible.
- Dropping the last definition on a table drops its triggers on the next
  reconcile pass under I6's retry loop.

## Multi-hop: a target is captured like a source

**Decision point, not prototyped.**

- **Should:** every target table carries the same three capture triggers a
  source does, generated from the definitions that read it, and there is no
  separate target-mutation path. The upstream apply transaction's `row_txid`
  is then C for the downstream's I2. A group row incremented by a batch
  produces one ring row per group per batch. A building target's writes
  reach its readers the same way, which is why a rebuilt upstream needs no
  catch-up for its readers.
- **Open:** the factored P table is structurally a first hop (`GROUP BY
  group, parent SELECT COUNT(*)`), which suggests the chained-hop machinery
  carries relationships; the per-child fallback would re-stage dependent
  keys into the ring (#354's shape). Cycle detection and `hop_gen` under
  trigger capture, and #272 (demand-driven sealing, no longer multiplying
  any seam), are [open question 6](#open-questions).

## Rejected alternatives

The mechanisms that preceded this decision are recorded here as
alternatives, each with the number that rejected it. Where one is still in
the code, [What the implementation removes](#what-the-implementation-removes)
names it.

| Alternative | Why not |
|---|---|
| **Capture by logical replication** (a `pgoutput` slot decoded by an intake thread; the 2026-08-15 decision). Chosen then because it kept everything off the write path and needed no trigger on user tables. | Staging is capped at ~120k rows/s at 1,000 rows/commit and 18.6k at 1 row/commit whatever the writers do, and a 10M-row `COPY` was never staged; total CPU per row is 4–5x the trigger's, paid later ([E1](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5842011977), [E9](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119)). A stopped Trellis pins the whole database's WAL rather than growing a ring at ~188 B/row ([E6](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119)). The decoder's 32-bit xid must be widened to `xid8` for I2, and the naive widening is silently wrong for every id before an epoch boundary ([exp 1a](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5840353484)). Decoding needs `REPLICA IDENTITY FULL` on every table a relationship touches. Slot loss, failover and the replication privilege are operational surface the trigger has none of. |
| **Row-level triggers.** | 5–10x the statement trigger's cost above 1 row/commit ([E1](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5842011977)). |
| **Column-agnostic image encodings** (`to_jsonb`, `jsonb_each_text`, `::text`, `hstore(NEW)`). | Not byte-identical to `tuple_to_json`, or identical but reading every column, which costs 40x on a 100 KB column ([E3](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119)). |
| **Ordering by LSN with live reads guarded by horizons and basis checks:** the recompute horizon and extinct horizon (#321), `min_image_lsn`, the build horizon (#442), per-key basis locks (#344), the watermark wait before enumeration (#312), `has_recompute` and `vanished_images` (#486), bucketed advisory locks (#389), keep-every-image fold (#494), prior image on enumeration (#392). | Each closes one pairing of one producer against one drain order, found in review of the previous fix at roughly two new issues per fix, and none was found by the generative suite (#556). A live read has no way to tell which commits it reflects; only a snapshot does (I2). |
| **Visibility-only I2**, and **Apply stamping its own snapshot as the basis.** | Each fails named exp 2 scenarios (5b/9/9c and 9b/9d) ([exp 2](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5840353484)). |
| **Membership-only ledger** (no stored contribution). | Wrong for relationships on both shapes it ran; saves 1–2% of WAL ([exp 3](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5842993962), [exp 4](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5845224204)). |
| **Group rows as absolute writes:** forced recomputes from a live `GROUP BY`, an existence probe and pre-lock before incrementing. | Not orderable without a horizon; the probe's scan is #326's 40k-group cliff where nothing drains; the pre-lock deadlocks on new groups (#539). Groups as sums with sorted increments: 0 deadlocks ([exp 2](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5840353484) scenario 4). |
| **The settled projection as correctness state** for to-one relationships, with the reverse fast path, `to_side_superseded` (#507), the `for share` stamp (#531), refresh markers (#529/#533/#547), seeding and widening (#543/#544). | Loses updates under to-side churn (#582: never converged, wrong target, reproducibly); needs `REPLICA IDENTITY FULL` on both sides; every consistency rule for it is a pairing rule. The factored T table is the parent's applied value under the ledger's own locks ([exp 4b](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5851003675)). |
| **Per-child contribution rewrite on a parent change.** | 2.3–3.1x the time and 5x the WAL at 100k fan-out on tmpfs, 2.4–2.9x on disk ([exp 4](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5845224204), [exp 4b](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5851003675)). |
| **Finding a parent's children by the ledger index alone**, or **by the index plus a live from-side scan**, or **by locking the to-side inside the application's transaction.** | The index alone misses a child moving in uncommitted (exp 2 scenario 6b); the scan works but keeps a live read; the lock puts Trellis on the application's commit path (I6). T as dependency lock replaces all three. |
| **One set-based `GROUP BY` build with changes withheld, then a go-live re-read and orphan sweep** (the 2026-08 and 2026-09 build designs, #485/#436). | The re-read is a full enumeration per build that scales with table size, not with what changed; at 100M it reaches `live` after 2,305 s and its drain then runs out of memory ([#617 step 3](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5857993459)). Nothing can tell a table unchanged since the build from one changed and changed back (#468), so the re-read could never be skipped. The one-pass build's algorithmic wins (scan once, chunk the writes) do not carry over: the new build is per-row work with no `GROUP BY` per chunk, and its acceptance bar is linear time. |
| **`catching_up` as a stored status with marker fences, generations and go-live catch-ups.** | Exists only because a build skipped changes. With the definition applying from its first chunk there is nothing to catch up. |
| **Bounding drain memory by running fewer workers.** | Each worker then holds more buckets; bounds nothing ([#617 step 3](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5857993459)). |
| **Fixing the build's cost by chunk size.** | 10x larger chunks leave 2.0x the time and 3.7x the WAL ([#617 final](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5859141160)). |
| **#556 (B): resolve every image-less trigger into an image at staging time**, so Phase 2 never reads live state. | Under trigger capture the only image-less change is a delete, and every other image-less producer is an enumeration this ADR replaces with Re-derive under lock. What B leaves untouched is out-of-order parallel batches for one key, which needs `applied_lsn`, not images. A resolver on the seal path is itself a live read racing the drains. |
| **#556 (C): serialize absolute writes per target** through one writer or one advisory lock. | Does not fix identity: a serialized write still reads live state ahead of pending deltas and still needs a horizon. Serializes the build, the largest producer of absolute writes, which is already CPU-bound on eight workers at 14–15k rows/s. |
| **#556 (D): thread source transaction ids through the ring.** | Adopted, in the exact form `row_txid` gives for free under trigger capture. |
| **#556 (E): LSN-versioned projection rows.** | Covers the relationship half only. The T table is E's idea in a cheaper shape: one applied value with its position per parent, not a version history per row. |

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
  of one row per source row (~3x the source's on-disk size at 10M in the
  prototype), and a relationship target carries P and T. WAL on the hot path
  is 1.6–1.7x a ledger-less apply for plain aggregates and 0.6–1.6x for
  relationships. In exchange, no full old image is ever staged and
  `REPLICA IDENTITY FULL` is not required anywhere.
- **The failure mode of a broken capture is loud, not silent.** A missing
  ring table or revoked privilege fails the application's statement with a
  `CONTEXT` line naming the capture function
  ([E6](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119)).
  The only silent uncapture is an owner disabling the trigger by name, which
  the audit reports.
- **Hosted compatibility** is core Postgres for everything required
  (statement triggers, transition tables, `SECURITY DEFINER`, the `TRIGGER`
  privilege); event-trigger regeneration is an optional extra where the
  host's admin role can create event triggers
  ([E10](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119)).
  No replication privilege, slot or publication is needed, and a
  backup-and-restore carries the ring with the data.
- **Drain backpressure becomes the operator's number.** Capture can outrun
  the drain by ~20x (1.9M rows/s against ~85k folded rows/s with the
  ledger), and the ring lives in the application's database at ~188 B per
  captured row when nothing drains
  ([E6](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119)).
  A growth bound and the policy at the bound are
  [open question 7](#open-questions).
- **Build time is per-row work on every worker**, not one `GROUP BY`. The
  acceptance bar is linear time with a per-row cost independent of table
  size; the prototype does not meet it yet, and the implementation must, at
  100M, before the build is called done.
- **Registration latency does not depend on table size**, and no definition
  is `live` when registration returns: callers poll `Trellis::status` to
  `live`, then use a token
  ([embedding](../embedding.md#poll-to-live-dont-wait)).
- **Every number above was taken on one box.** #565's phase 1 and #558's
  experiments 1–4 ran on tmpfs; the disk-backed numbers are from a single
  NVMe with ~0.4 ms fsyncs and, for #558's disk tier, a box that was not
  always quiet
  ([disk tier](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5851008357),
  [caveat](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5847942345)).
  Within-session ratios stand; absolute bars are restated in
  [validation 4](#validation-and-acceptance).

### What the implementation removes

The code on `main` still carries the rejected mechanisms. This list is the
implementation plan's backbone; each item names what landed it.

- **Intake and the slot:** `pgwire-replication`, `TxnBuffer` and its spill,
  the LSN watermark persist and acknowledgement, slot-loss detection and
  recovery, the `pg_logical_emit_message` nudge (#452),
  `replication_progress`, the publication and its reconcile.
- **`REPLICA IDENTITY FULL`** as a requirement on any table (#589 closes).
- **Horizons and bases:** the recompute horizon, extinct horizon and
  `min_image_lsn` (#321/#390); the build horizon (#442); basis rows and
  per-key locks (#344/#356); the watermark wait before enumeration
  (#312/#333); `has_recompute` and `vanished_images` (#486/#493).
- **Projection correctness state:** `to_side_superseded` (#507/#532); the
  `for share` stamp (#531/#554); refresh markers and `refreshed_lsn`
  (#529/#533/#547); projection seeding and widening (#543/#544).
- **Decided, never landed:** bucketed advisory locks (#389); keep-every-image
  fold (#494); prior image on enumeration (#392).
- **Capture and build paths:** the go-live re-read and orphan sweep
  (#485/#436); the ring enumeration fallback; `pending_backfill` markers,
  fences, generations and `catching_up`; the rebuilt-target catch-up (#507);
  the one-pass `GROUP BY` build and its temp staging table; the
  target-mutation seam (should).
- **Group probes:** the existence probe and group pre-lock (#326);
  `apply_forced_groups_bulk`.
- **Docs:** [stage 01](../staging-and-claiming/01-intake-and-lsn-confirmation.md)
  in full; stage 04's fold half; stage 05's basis-check and
  recompute-horizon sections; the capture sections of
  [data-flow](../data-flow.md), [embedding](../embedding.md) and
  [observability](../observability.md), which describe the code as it stands
  until the code changes (#556 deliverable 2).

Open issues resolved or reshaped by this decision: #272, #326, #354, #455,
#456, #458, #495, #496, #529, #535, #536, #543, #544, #546, #547, #581, #582,
#589, #598.

## Validation and acceptance

Deliverable 4 of #556. Nothing below polls for convergence (#297).

1. **One deterministic interleaving test per exp 2 scenario** (2, 2b, 3, 4,
   5, 5b, 6, 6b, 6c, 7, 8, 8b, 9, 9b, 9c, 9d, 10) on the real engine,
   hand-driven with advisory locks, plus one per superseded issue in #556's
   matrix (#389, #392, #494, #524, #525, #528, #539, #549, #550).
2. **The generative concurrent tier** (#557) with the three planted
   violations of #558's experiment 6 (skip the lock, compare LSN instead of
   visibility, delete tombstones early), each caught within the nightly
   budget. **Merge gate.**
3. **`build-under-load` at 100M** on the implementation branch, disk-backed,
   under a 16 GB memory cap: oracle ok, memory bounded, WAL retention
   bounded, time linear in N, per-row cost flat from 1M to 100M.
   **Milestone exit for the build.**
4. **A quiet-box, disk-backed benchmark round** with a checkpoint inside the
   window and storage stated on every number: `fold-in-ratio`,
   `group-contention` (400/4k/40k), `rel-churn` (10/1k/100k children per
   parent, 100 and 1,000 parent updates/s, child-only churn),
   `build-under-load`, #565's E1 and E2, each with a same-session control on
   the pre-change code. The bars in this ADR are restated from that round.
5. **Writer-coupling tests:** schema change of a read column under load
   (writes succeed, definition pauses, regeneration restores capture); join
   and drop under a 30 s open transaction (worst writer wait under 100 ms);
   revoked privilege and disabled trigger (audit reports); `REPEATABLE
   READ` and `SERIALIZABLE` writers straddling a seal (no lost rows).
6. **Drain paging:** a segment several times the batch cap drains with peak
   RSS bounded by the cap; a key whose changes straddle two segments, a
   paged truncate segment, and a reclaim between two pages all converge.

## Open questions

For the debate on #618; each has a recommendation where one exists.

1. **Do the two capture paths coexist for any period?** Recommendation: the
   slot path is deleted in the first milestone. Coexistence keeps the
   widening, `src_xid`, intake and a second proof for every ordering
   argument, for a migration story Trellis does not need pre-release.
2. **The mechanism for the build's commutative group application:** a
   per-target delta table with a merger (recommended, keeps I5 and needs no
   new locks), per-worker in-memory accumulators flushed per chunk (cheaper,
   but a crash loses the accumulator and the chunk must re-run), or
   group-key affinity between chunks and workers (only works when the group
   key is correlated with the PK).
3. **Why I5 is not total on disk.** The factored variant deadlocked 9–16
   times per 100k-fan-out run on disk and never on tmpfs. Find the cycle
   (candidates: the P index's page splits, and the parent lock taken in a
   different class order by the forward and reverse paths) before the ADR
   claims I5.
4. **What the ADR promises about ledger size and WAL amplification.** ~3x
   the source and 1.6–1.7x WAL at 10M. Can `contrib` be narrowed for plain
   invertible aggregates (a `SUM` needs the value, a `COUNT` needs nothing),
   and does the ledger belong on a separate tablespace?
5. **The status model.** Three stored states is the recommendation; the
   derived "upstream not live" rule stays. Is there any remaining repair that
   is not a rebuild?
6. **Multi-hop under triggers.** Targets as captured tables and the seam's
   deletion; whether the factored P table *is* a hop; cycle handling; and
   what this does to #272 and #354.
7. **The drain backpressure policy.** What happens when capture outruns the
   drain by 20x: a ring size bound, and at the bound, slow the writer (I6
   says no), pause capture (loses changes; no), or page the drain harder and
   alert. Needs the disk-backed E2 and E6 numbers.
8. **Hosted disks.** At ~1 ms fsync the 1-row-per-commit ceiling drops for
   every variant alike; is a throttled benchmark tier needed before release?
9. **The schema-changed marker row.** Is the `EXCEPTION` block's
   subtransaction per statement acceptable on the hot path, or should the
   function be regenerated only from an event trigger and a rename without
   one be a documented outage of that definition (never of the application)?
   How do the audit and the rebuild pick the marker up?
10. **Truncate under xid order** (#598) and cascading truncates on targets
    that are captured tables.

## Reusable material

Prototype branches on the fork (mmmries/trellis):
`exp/issue-558-ledger-experiments` (ledger Apply, `rel-churn`,
`experiments/issue-558/RESULTS.md` and `HANDOFF.md`),
`exp/issue-558-factored` (P and T tables), `exp/issue-558-exp5` (chunked
ledger build in `trellis/src/staging/ledger_build.rs`, `build-under-load`
with the disk columns and the memory sampler), `exp/issue-558-exp5-trigger`,
`spike/565-trigger-capture` (`experiments/issue-565/capture_sql.py` with
the `new_only` and `skip_noop` shapes, the phase-1 harness). Already in
`main`: #597 (the fence mirror).

## More information

- [Documenting Architecture Decisions](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)
  for the ADR form ([ADR-0001](0001-use-adr.md)).
- Epic #556 (the seam and its inventory), #558 (ledger design note and
  experiments 1–4b), #565 (trigger capture, E1–E10), #617 (the build under
  load at 10M and 100M), #618 (the outline this ADR was drafted from, and
  the debate).
