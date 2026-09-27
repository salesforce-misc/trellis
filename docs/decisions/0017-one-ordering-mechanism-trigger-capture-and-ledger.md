---
status: proposed
date: 2026-09-27
deciders: Michael Ries
consulted:
informed:
---

# One Ordering Mechanism for Absolute Writes: Trigger Capture into the Ring, a Target-Owned Ledger with an Exact Snapshot Basis, and a Build that Applies from Its First Chunk

*Draft for debate. Epic #556, outlined in #618. Every requirement below points
at the experiment that produced it; "must" is what the evidence forces,
"should" is the current recommendation and is open. The decision points that
change today's architecture are collected in
[Where this diverges from today](#where-this-diverges-from-today) and marked
**Decision point** where they appear.*

Trellis derives tables from other tables, asynchronously, exactly once
([ADR-0002](0002-async-data-flow.md)). The pipeline that does it is sound for
one class of write and unsound for another. A **delta** (`+f(new) − f(old)`
into a group) commutes with every other delta, so parallel workers can apply
deltas in any order and the exactly-once argument of
[stage 05](../staging-and-claiming/05-apply-and-exactly-once-deltas.md) holds.
An **absolute write**, any value derived from a *read* rather than from carried
images, does not commute: it is right only if nothing later in the change order
has already been applied to the same row or group. Trellis has many producers
of absolute writes (an image-less recompute, an existence probe, the
relationship reverse fallback, a projection refresh, a go-live re-read), each
reading live `READ COMMITTED` state and each racing out-of-order parallel batch
drains. Nearly every correctness bug of September 2026 sat on that seam, and
each fix added one more precedence rule for one more pairing (#556's
inventory). This ADR establishes one ordering mechanism for every absolute
write, and removes the rules.

The research behind it (#558, #565, #617) also found that today's capture and
build paths have scaling cliffs of their own, which the same mechanism removes:

- the logical replication slot stages at most about 120k rows/s whatever the
  writers do, and never captured a 10M-row `COPY`
  (#565 [E1](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5842011977));
- the go-live re-read cannot finish at 100M rows on a 31 GB box, because a
  drain batch holds its whole share of a segment in memory
  (#617 [step 3](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5857993459));
- `main`'s relationship reverse path loses updates under to-side churn (#582,
  #558 [experiment 4](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5845224204)).

## Decision in one paragraph

Source changes are captured by `AFTER … FOR EACH STATEMENT` triggers that
append to the staging ring inside the writer's own transaction, so a ring
row's `row_txid` *is* the source commit's transaction id; the logical
replication slot and intake are removed. Every target owns a **ledger** with
one entry per source row holding the contribution the target currently counts
for that row, the last applied ring position, and the full snapshot the entry
was last derived under. Every absolute write **locks the entry first and reads
in the same statement as its snapshot**, and every change is **applied or
skipped by checking its transaction against that snapshot and its position
against the last applied one**. Group rows are pure sums of ledger entries and
are only ever incremented. Relationships factor the to-side value out of the
child's contribution so a parent change touches the parent and its groups,
never its children. A build is the same re-derive operation over primary-key
chunks of the source, and the definition applies changes from its first
chunk, so there is no go-live re-read and no orphan sweep. Two operational
invariants learned at 100M rows are part of the design: no Trellis transaction
waits for a lock while it holds a snapshot open, and a drain batch's memory is
bounded by a row cap, never by the size of a segment.

## Where this diverges from today

Each row is a decision point. The rest of the document gives the argument and
the evidence; this table is the map.

| # | Today | Under this ADR | Why (evidence) |
|---|---|---|---|
| D1 | Changes are decoded from the WAL by a logical replication slot and staged by an intake thread, asynchronously, off the application's write path (ADR-0002). | Statement triggers append to the ring **in the writer's transaction**. Derivation stays asynchronous; only capture moves onto the write path. | The slot caps capture at ~120k rows/s and misses bulk loads; triggers reach 1.9M rows/s and cost 4–5x less CPU per row ([E1](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5842011977), [E2](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119)). Cost: at 1,000 rows/commit the writer's throughput halves on disk ([E1 disk](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5850979670)). |
| D2 | A change's identity for ordering is its LSN; a live read has no way to tell which commits it already reflects. | Identity is the source commit's `xid8` (`row_txid`), decided against a stored `pg_snapshot`, plus the ring position against `applied_lsn`. | Visibility alone cannot order same-key changes across batches; `applied_lsn` passes every scenario ([exp 2](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5840353484)). |
| D3 | The old side of a delta comes from the CDC old image, so every source needs `REPLICA IDENTITY FULL`; the fold's first image names the old group. | The old side comes from the ledger; the ring carries **NEW-only** images of the key and the read columns. | Ledger holds the applied contribution ([trigger amendment](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5847674780)); a trigger reading every column costs 40x on a 100 KB column ([E3](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119)). |
| D4 | Group rows are written absolutely by forced recomputes, probed for existence, pre-locked, and guarded by recompute and extinct horizons. | Group rows are **sums**, only ever incremented in key order; no probe, no pre-lock, no horizon. | Groups as sums with sorted increments: 0 deadlocks with 8 workers creating the same groups ([exp 2](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5840353484) scenario 4). |
| D5 | Phase 2 reads live state under `READ COMMITTED`, Phase 3 writes it later under a per-key basis check (#344). | **Read after lock**, snapshot taken in the same statement as the read, stored whole. | A separate `pg_current_snapshot()` differed from the read's snapshot in 99.7% of samples ([exp 1b](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5840353484)). |
| D6 | The relationship projection is correctness state: settled parent values, `for share` stamps, `to_side_superseded`, refresh markers. | The projection is at most a cache. Correctness lives in the **factored** partial and parent tables; a parent change never touches a child. | Factored: correct on every shape, WAL 0.6–1.6x `main`, 100k-child parents 0.9x `main` on disk ([exp 4b](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5851003675)). |
| D7 | A build is one `GROUP BY` (ADR-0007) during which apply skips the definition, then a go-live re-read of every key and an orphan sweep (ADR-0016). | A build is **re-derive over PK-range chunks** on every drain worker, and the definition **applies from its first chunk**. No re-read, no sweep. | Oracle matched on four 10M runs under load with no re-read ([#617 step 2](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5852918496)); the re-read's drain OOMs at 100M ([step 3](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5857993459)). |
| D8 | Four stored statuses (`waiting_to_backfill`, `backfilling`, `catching_up`, `live`) and a marker lifecycle with fences, generations and go-live catch-ups. | Three stored statuses; `catching_up` and the catch-up markers go. `live` stays strict. | Nothing is skipped during a build, so there is nothing to catch up ([#558 note](https://github.com/salesforce-misc/trellis/issues/558)). |
| D9 | Convergence needs `confirmed_lsn ≥ token`, a `pg_logical_emit_message` nudge and an intake wait before enumeration. | The token is unchanged; the predicate is over ring rows only. | Under triggers a commit's ring rows are in the same transaction as the commit ([E5](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5842011977)). |
| D10 | The maintenance loop is the only sealer, and the go-live discharge runs there so nothing staged after it drains before the flip. | No "only sealer" premise. Build work runs on drain workers and **yields to sealing**. | Seal refused for a whole build, backlog 6.09M rows ([#617 final](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5859141160)). |
| D11 | Lock waits inside Trellis transactions are unbounded. | **No Trellis transaction waits for a lock while it holds a snapshot open**: `lock_timeout`, roll back, retry. | A drain batch waited 1 h 50 min inside its transaction, pinned the slot, 190 GB of WAL ([#617 final](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5859141160)). |
| D12 | A drain batch folds, re-reads and plans its whole bucket share. | **Memory bounded by a row cap**; a bucket is paged. | ~650 B per staged row held per worker; OOM at 100M ([#617 step 3](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5857993459), [#556 requirement](https://github.com/salesforce-misc/trellis/issues/556#issuecomment-5859042432)). |
| D13 | A table joins the stream by `ALTER PUBLICATION`, and a marker is fenced by a later transaction id to cover the join's window (ADR-0016). | `CREATE TRIGGER`'s commit **is** the join fence: `SHARE ROW EXCLUSIVE` waits out every in-flight writer. | No writer can straddle it ([E7](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119)). |
| D14 | Trellis never blocks an application writer because it never runs in one. | Trellis code runs in every writer's transaction, so **never blocking a writer** and **never failing a writer on a Trellis-side condition** become written invariants with tests. | A bare `CREATE TRIGGER` stalled every writer for 25 s; a dropped read column fails every insert ([E4](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5842011977), [E7](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119)). |
| D15 | A target that is another definition's source feeds it through the target-mutation seam, a second capture path with its own image rules. | **Should:** targets carry the same capture triggers as sources; the seam is deleted. | One capture mechanism; the seam already stamps pre-commit positions exactly as a trigger does ([E4 inventory](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5842011977)). Not prototyped; [open question 6](#open-questions). |

ADR-0002's case for asynchrony was mostly a case against *synchronous
derivation*: cross-relationship formulas become unsound, one failed derivation
blocks the write, hot groups serialize the writer. None of that changes here:
derivation stays asynchronous and batched, quarantine stays, and a hot group is
still incremented by a drain worker, not by the application. What moves onto
the write path is capture alone, an append of the key and the read columns.
Two of ADR-0002's objections do apply to capture and are taken on as
invariants: a disabled trigger strands data (I6's audit and `ENABLE ALWAYS`),
and a broken ring fails the application's statement (element 1's marker row
and the never-fail rule).

## Invariants

The design is these invariants; every element below exists to hold one of
them. I0 to I5 are the ledger note's (#558) with the amendments the
experiments forced. I6 to I8 were learned from #565 and #617.

| | Invariant | Evidence, and the amendment it needed |
|---|---|---|
| **I0** | **One database.** Sources, the ring, every ledger and every target live in one Postgres database, so one snapshot orders every commit Trellis will see. | Design premise; made exact by element 1, which makes a ring row's transaction the source commit's. Nothing here works across databases. |
| **I1** | **Read after lock.** Every live read that feeds an absolute write happens after the writer holds the lock on the ledger state it will write, and the snapshot it stores is taken **in the same statement** as the read. | [Exp 2](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5840353484) scenarios 2/2b hold (Apply demonstrably blocks on the entry lock). [Exp 1b](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5840353484): a snapshot taken in a separate statement differed from the read's in 99.7% of samples under load; the stored basis is the full `pg_snapshot`, not `xmin`/`xmax`. |
| **I2** | **Visibility-checked application.** A change C for row r is applied iff C's transaction is **not** visible in r's basis snapshot **and** C's ring position is above r's `applied_lsn`. Skipping is exact, never "maybe counted, re-derive". | Exp 2: skip-iff-visible alone fails 5b/9/9c (same-key order across batches is not decidable from visibility); stamping Apply's own snapshot fails 9b/9d; visible-or-`applied_lsn` passes all seventeen. An in-flight id is decidable from the stored list ([exp 1b](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5840353484): 0 disagreements over ~1.1M pairs); #617 saw 0 in-progress cases at 10M. |
| **I3** | **Per-row ordering state; groups are pure sums.** The ledger entry is the only place a row's applied contribution and group live. A group value is the sum of its entries' contributions, so group updates commute and a group row is only ever incremented. For relationships the to-side value is factored out (element 5). | Exp 2 scenarios 4 and 5; [exp 4b](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5851003675) for the factoring. |
| **I4** | **Tombstones live until the batch watermark passes.** A deleted row's entry stays, with its `applied_lsn`, until every batch at or below its own is fully applied on that target. | Exp 2 scenario 9 (an older update resurrects a deleted row without it). The batch-watermark form is exact under triggers because a same-key predecessor of a delete committed before the delete's trigger ran ([trigger amendment](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5847674780)). |
| **I5** | **One lock order, taken as one sorted batch.** Ledger entries, then parent rows, then partials, then groups, each locked in key order in one statement per class. Never a per-row loop. | Exp 2 finding 3: a per-row loop deadlocked 19–22 times in 20 s; the sorted batch never did. **Not yet total:** the factored variant deadlocked 9–16 times per 100k-fan-out run on disk and 0 on tmpfs ([exp 4b](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5851003675)). The cycle must be found before I5 is stated as proven ([open question 3](#open-questions)). |
| **I6** | **Never block, and never fail, an application writer.** No Trellis transaction takes a lock an application write can queue behind, except the join and drop fences, which are bounded by `lock_timeout` and retried. No Trellis-side condition (a missing column, a broken ring) fails the application's statement. | [E7](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119): a bare `CREATE TRIGGER` stalled every writer for 25 s; with a 50 ms `lock_timeout` retry the worst wait was 52 ms. [E4](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5842011977): a renamed read column failed every insert until regeneration. Today's retire path already takes its `TRUNCATE` lock `NOWAIT`. |
| **I7** | **No Trellis transaction waits for a lock while it holds a snapshot open.** Every lock statement runs under a `lock_timeout`; on timeout the transaction rolls back and the work is retried from outside any transaction. | [#617 final](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5859141160): a drain batch's ledger insert waited 1 h 50 min behind chunk transactions; the open transaction pinned the slot's `restart_lsn`, `pg_wal` reached 190 GB and the sealer was refused for the whole wait. Without a slot the WAL pin goes, but an open snapshot still holds back vacuum and the sealer, so the invariant stays. |
| **I8** | **Memory is bounded by a batch cap, never by segment size.** A drain batch folds, re-reads and applies at most a fixed number of changes and pages through a larger bucket. | [#617 step 3](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5857993459): ~650 B per staged row held per worker, 6.32 GB at 10M, OOM at 100M; fewer workers bound nothing because each worker then holds more buckets. |

## Decision: the design elements

### 1. Capture by statement triggers that append to the ring inside the writer's transaction; the slot path is removed

**Decision point (D1, D3, D13, D14).** This is the largest divergence:
Trellis code runs inside the application's transactions. What it does there
is one append.

- **Must:** three `AFTER … FOR EACH STATEMENT` triggers per captured table
  (insert, update, delete; a trigger with transition tables takes one event),
  each appending the statement's rows to the active segment in the writer's
  transaction. The ring row's `row_txid` (already `DEFAULT
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
  The slot path would instead need the decoder xid widened to `xid8`, which
  [exp 1a](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5840353484)
  showed is silently wrong across an epoch unless the anchor is re-read after
  every change.
- **Must:** the generated function names only the primary key and the
  columns the table's definitions read (plus relationship `from_col`s), emits
  **NEW-only** images (a delete emits the key), renders every value with
  `format('%s', col)` under the five output settings Trellis pins
  (`DETERMINISTIC_TEXT_OUTPUT_GUCS`) as `SET` clauses on the function, carries
  a `WHEN` clause that skips an update touching no read column, is
  `SECURITY DEFINER` owned by a dedicated capture role with a pinned
  `search_path`, is `ENABLE ALWAYS`, and reads the active slot
  schema-qualified through the sequence mirror (element 2). *Evidence:*
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
  for the privilege model.
- **Must (I6):** `CREATE TRIGGER` and `DROP TRIGGER` run under a short
  `lock_timeout` in a retry loop, one attempt per interval, until they land.
  *Evidence:* [E7](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119).
  The join's commit is also the join fence (D13): `SHARE ROW EXCLUSIVE` waits
  out every writer in flight, so every commit either precedes the trigger and
  is visible to any snapshot taken after it, or runs the trigger. ADR-0016's
  fence-after-park and generation machinery has nothing left to cover.
- **Must (I6):** a schema change to a read column never fails the
  application's write. **Should:** the generated function catches the
  undefined-column error, writes a *schema-changed* marker row for the table
  instead of an image, and returns; the drain that meets the marker pauses
  every definition reading the column with a status error and regenerates
  the function; where the host allows `CREATE EVENT TRIGGER`, an event trigger
  regenerates the function inside the DDL's own transaction so the marker
  path is the fallback. *Evidence:*
  [E4](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5842011977)
  (rename or drop of a named column fails every insert until regenerated;
  add, rename or drop of any *other* column is harmless),
  [E10](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119)
  (event triggers available to the customer's admin role everywhere surveyed
  except, undocumented, Azure Flexible and Crunchy Bridge). The
  `EXCEPTION` block costs a subtransaction per statement; whether that cost
  is acceptable on the hot path is [open question 9](#open-questions).
- **Must:** the self-check audit ([ADR-0013](0013-self-check-production-recompute-audit.md))
  verifies from `pg_trigger` that every captured table's three triggers exist,
  are owned by the capture role and are enabled, and reports a missing or
  disabled one before any recompute comparison. Replica-mode sessions are
  covered by `ENABLE ALWAYS`; an owner who disables or drops the trigger by
  name is documented as uncaptured until the audit runs. *Evidence:*
  [E6](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119):
  the table owner can `DISABLE TRIGGER` ours and writes then go uncaptured
  without error.
- **Should:** the slot path is **deleted in the first milestone**, not kept
  behind a flag. Keeping it keeps the xid widening and its anchor rule, the
  `src_xid` column, intake and its watermark, `REPLICA IDENTITY FULL`, the
  intake wait before enumeration, and a second proof of every ordering
  argument. *Evidence:*
  [#565 closing](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5852506725)
  (recommendation on record);
  [E8](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119)
  for what intake costs at idle (58 transactions/s, 0.8% CPU). This is
  [open question 1](#open-questions).
- **Cost to state.** On disk at 1,000 rows/commit with 16 writers, the
  writer's throughput is 0.49x no-capture and the ring row is ~2.9x the
  source row's WAL bytes
  ([E1 disk](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5850979670));
  the NEW-only and `WHEN` cuts are unmeasured. The bar of "at most 25% loss at
  1,000 rows/commit" cannot be met by any synchronous capture and is re-based
  to "batched writes cost no more than one expression index's CPU per row;
  p99 commit latency at 1 row/commit rises by less than 2.5x on tmpfs and is
  unchanged on disk". A bulk-load pattern (drop the triggers, load, rebuild
  the readers) is documented and is exact under element 6.

### 2. The seal fence stays; its pointer read is snapshot-independent

The two-phase seal and its fence
([stage 03](../staging-and-claiming/03-sealing-and-the-fence.md)) carry over.
What changes is who the writers are: today every ring writer is Trellis's own
`READ COMMITTED` transaction; under element 1 a writer is the application at
whatever isolation level it chose.

- **Must:** the writer reads the active slot through the sequence mirror
  (`ring_slot_mirror`, `pg_sequence_last_value()`), which the seal sets in
  phase 2 before the `xmax` bump, and the writer's xid is assigned in the
  same statement as that read. *Evidence:*
  [E4 inference 6](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5842011977):
  a `REPEATABLE READ` or `SERIALIZABLE` writer read a pointer two flips old
  from `segment_pointer` and its rows landed in a slot no batch would ever
  read. Fixed in `main` by #597 (#595); the doc-03 proof is restated for the
  mirror there.
- **Must:** the truncate barrier decides `has_truncate` after the batch's
  contents are fixed (#598; the mirror widens the window by one round trip).
- **Per-key order is `(lsn, change_id)`** with `lsn =
  pg_current_wal_insert_lsn()` read in the trigger; the fold's ordering rule
  is unchanged. Cross-key commit order is not available from triggers and,
  under I3, not needed: ordering state is per row, groups are sums, and
  relationships go through per-parent locks. *Evidence:*
  [E4](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5842011977):
  per-key order held in every interleaving; a `CACHE`d sequence alone
  inverted a key's order; with the LSN it holds because a second writer on
  the same key runs its trigger only after the first commits;
  [trigger amendment](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5847674780)
  for why cross-key order was never needed.

### 3. The ledger: one target-owned entry per source row

**Decision point (D2, D3, D4, D5).** The target, not the ring and not a
projection, owns the state that orders its writes.

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
    each relationship, the parent's applied values (element 5), compute the
    group key and contribution. Diff against the entry: subtract the old
    contribution from the old group, add the new to the new group, replace
    the entry with the new values, the new basis and the read's position.
    If r no longer exists in its source, the entry becomes a tombstone and
    its contribution leaves its group.
  - **Apply(r, C, image).** Lock r's entry. If C is visible in the entry's
    basis, or C's position is at or below `applied_lsn`, stop (I2).
    Otherwise `delta = f(NEW) − entry.contrib`, group += delta, entry :=
    (new group, new contribution, C's position, basis unchanged). A delete
    is `f(NEW) = 0` and the entry becomes a tombstone. A row with no entry
    is an insert of NEW.

  | Producer today | Under this ADR |
  |---|---|
  | CDC insert/update/delete | Apply |
  | Build (any shape), resume rebuild, `request_backfill`, quarantine release, column resume, `ALTER TRANSFORM` added column | Re-derive over the definition's key space, chunked (element 6) |
  | Go-live re-read, orphan sweep, image-less `Recompute` enumeration | Gone |
  | Relationship parent change | Element 5's parent operation; per-child Re-derive only for non-linear fields |
  | To-side `TRUNCATE` | Parent operation with `f = 0` for every parent of the table; the ledger's join-key index names the children |
  | Source `TRUNCATE` | Every entry from that source becomes a tombstone with the truncate's position; groups decrement |
  | Chained hop | Apply, where C is the upstream apply transaction (element 9) |
  | Forced group recompute, existence probe, extinction delete | Gone: groups are sums |

- **Must keep contributions.** A membership-only ledger (group key and basis,
  contribution recomputed from source) saves 1–2% of WAL and produced wrong
  relationship targets on both shapes it ran. *Evidence:*
  [exp 3](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5842993962),
  [exp 4](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5845224204).
- **Non-invertible fields** (`MIN`, `MAX`, `BOOL_AND`, composed expressions)
  are recomputed from the ledger's contributions by an index scan on
  `group_key` under the group's lock, never from the source. This is the one
  place a group value is written absolutely, and it is ordered by the same
  ledger locks as every increment.
- **Cost to state.** Hot path with the prototype (probe and pre-lock still
  in place): 1–9% at 8 workers on 400 and 4k groups, 19% with one worker;
  WAL 1.6–1.7x from the heap tuple and PK index entry per source row; the
  ledger on disk was ~3x the source table at 10M
  ([exp 3](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5842993962),
  [#617 step 2](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5852918496)).
  The upside of deleting the existence probe and pre-lock (#326's 40k-group
  cliff, where nothing drains in any mode today) is unmeasured because the
  prototype kept them. Ledger size and whether `contrib` can be narrowed for
  plain aggregates is [open question 4](#open-questions).

### 4. Apply is one set-based statement per batch per target, and the batch is bounded

- **Should:** with `row_txid` exact, I2's skip rule is one predicate over the
  batch joined to the ledger, so a batch's Apply on a target is: one sorted
  lock statement over the entries it touches, one `update … from batch`
  that computes deltas and rewrites entries, one sorted upsert of group
  increments. The fold's "mixed visibility → re-derive" case disappears for
  image-bearing changes. *Evidence:*
  [trigger amendment](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5847674780);
  [exp 2](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5840353484)
  finding 1.
- **Must (fold):** an image-less `op = 'delete'` is the key's final state
  within a fold window, whatever precedes it. *Evidence:*
  [#617 step 4 dry run](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5853104094):
  under NEW-only capture, a delete sharing a fold window with an earlier
  write of the same key was dropped by today's "latest row with any image"
  rule; 11–12 groups wrong at 1M.
- **Must (fold):** the fold statement never plans a nested loop over stale
  ring statistics (#581).
- **Must (I8):** a batch folds, re-reads and applies at most a fixed number
  of changes (a per-batch row cap on the order of 10^5) and pages
  through a larger bucket in `(lsn, change_id)` order. Splitting one key's
  changes across two pages is safe under this ADR and was not under today's:
  Apply is per-key ordered by `applied_lsn` and takes its old side from the
  ledger, so the second page needs nothing from the first, where today's
  fold needed all of a key's changes together to name the old group from the
  first image. *Evidence:*
  [#617 step 3](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5857993459),
  [#556 requirement](https://github.com/salesforce-misc/trellis/issues/556#issuecomment-5859042432).
- **Must (I7):** every lock statement in Apply runs under `lock_timeout`; a
  timeout releases the claim's transaction and retries the page after a
  backoff, outside any transaction.

### 5. Relationships are factored; the projection loses its correctness role

**Decision point (D6).** Today a to-one relationship's *settled projection*
is the value a child's contribution was computed from, and everything that
keeps it consistent (the `for share` stamp, `to_side_superseded`, the
refresh markers, the reverse fast-path precondition) exists because the
projection is correctness state. Here the parent's applied value lives in a
table the target owns, under the same lock discipline as the ledger.

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
  writes a child.** T is the child's dependency lock, so the "ledger index ∪
  live from-side scan" amendment (exp 2 scenario 6b) is not needed. Lock
  order L → T → P → groups, each sorted (I5). *Evidence:*
  [exp 4b](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5851003675):
  oracle matched on all six shapes; 21 s to converge (tail under 1 s) on
  five of six against `main`'s 24–113 s and one never-converged, wrong
  target; WAL 0.6–1.6x `main`; on disk the 100k-child shape is 0.9x `main`
  with 0.65x the WAL, where the per-child rewrite (`contrib`) is 2.4–2.9x
  ([exp 4](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5845224204)).
- **Must:** a field that is not linear in the to-side value (anything but
  `SUM`/`AVG` of a bare to-one path times a from-side factor) keeps the
  per-child path: the parent change re-derives each child through the
  ledger's join-key index, with T as the dependency lock. Detected per field
  at install and reported.
- **Must:** a 1-1 target that reads through a relationship gains only the T
  row as its dependency lock; it has no contribution to factor.
- **Should:** the projection table is deleted, or kept purely as a read
  cache with no lifecycle of its own. With it go the `for share` stamp,
  `to_side_superseded`, the reverse fast-path precondition, projection
  seeding and widening, and the refresh markers (#529, #531, #533, #543,
  #544, #547 are resolved or reshaped by this element).
- **Open cost:** the forward path writes one P row per child change; at low
  fan-out P is as large as L and a random child update touches a cold P page
  (+37% WAL at fan-out 10 on tmpfs, 0.96x at fan-out 1k). Unmeasured on
  disk.

### 6. A build is Re-derive over PK-range chunks, and the definition applies from its first chunk

**Decision point (D7, D10).** This replaces
[ADR-0007](0007-direct-set-based-backfill.md)'s single-pass `GROUP BY` and
the whole go-live half of [ADR-0016](0016-single-background-capture-path.md).

- **Must:** no go-live re-read, no orphan sweep, no catching-up re-read.
  Registration writes the definition, its ledger tables and a chunk plan
  (PK ranges over the source, discovered as ADR-0007 discovers 1-1 ranges),
  and the definition is **applying from that commit**. Every drain worker
  claims chunks. A chunk is one short transaction: lock its entries in key
  order (placeholders for keys with no entry), one statement that takes the
  snapshot and reads the range, replace the entries, record the group deltas
  (next bullet). "No entry" means "not yet counted": a CDC change for a key
  whose chunk has not run applies as an insert of NEW, a delete of one
  records a tombstone, and the chunk's later re-derive replaces the entry
  under a snapshot that includes that commit, so the group ends up counted
  once. When the last chunk commits and its deltas are merged the definition
  is `live`. *Evidence:*
  [#617 step 2](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5852918496):
  oracle matched on four 10M runs under 2,000 writes/s, with 196–361k changes
  correctly skipped by chunk bases and 0 in-progress ids;
  [step 3](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5857993459):
  today's path reaches `live` at 100M only after a 2,305 s re-read and then
  dies draining it.
- **Must (I3, new):** group application during a build is a **separate,
  commutative, batched step**. A chunk records its group deltas without
  locking any group row, and a merger applies accumulated deltas to group
  rows in key order as one sorted upsert. **Should:** the deltas are rows in
  a per-target `__group_deltas` table (append-only, `(chunk, group_key,
  deltas)`), and the merger is any drain worker claiming a range of them; the
  alternatives are [open question 2](#open-questions). *Evidence:*
  [#617 final](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5859141160):
  a 100k-row chunk touches ~63% of 100k groups, so chunk transactions
  serialize on group rows; 10x larger chunks cut define-to-live by only a
  third.
- **Must (I7):** chunk transactions are short (one range, no group locks,
  under `lock_timeout`), and a CDC batch never waits inside its transaction
  for a chunk's lock. *Evidence:* the 1 h 50 min wait above.
- **Must (backpressure):** build work yields to sealing and to the CDC
  drain. A worker claims a chunk only when the ring's undrained backlog for
  its instance is under a bound, and the sealer is never refused for the
  duration of a build. *Evidence:*
  [step 2](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5852918496):
  seal refused for the whole build, tail 90–213 s after `live`;
  [final](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5859141160):
  a 6.09M-row undrained segment at 100M.
- **Must (acceptance):** per-row build cost is independent of table size;
  a build of N rows runs in bounded memory and bounded WAL retention, in
  time linear in N, and converges to the oracle at 100M under load. *Evidence:*
  per-chunk cost grew from 3.2 to 5.7 worker-seconds per 10k rows between 1M
  and 10M, and the chunk rate fell from 0.96 to 0.4/s at 100M. The prototype's
  ~360 µs per row needs a profile (one worker against eight, wait events)
  before the implementation plan fixes its shape.
- **Should:** the peak `xmin` hold during a build is one chunk's duration,
  not the build's (270–658 s at 10M in the prototype, from a group-existence
  read the merger removes).
- **Failure and resume contract** carries over from ADR-0016 unchanged in
  spirit: a chunk is a claimable, heartbeated, reclaimable work item; a
  crashed worker's chunk is re-run (Re-derive is idempotent); a failed chunk
  backs off with its error on status; a resume supersedes held chunks
  through the claim fence.
- **Cost to state.** At 10M with the prototype's shape, define-to-live was
  2.0–3.2x the control's and WAL 3.7–5.1x. Those are the prototype's numbers
  with group locks inside chunks and the probe still present; the ADR commits
  to the acceptance bullet, and the bars are restated on the implementation
  ([validation 3](#validation-and-acceptance)).

| Build at 10M rows, 8 workers, 2,000 writes/s, disk | define → `live` | WAL | oracle | tail after `live` |
|---|---|---|---|---|
| today (`GROUP BY` + re-read) | 226 s | 5.8 GB | ok | 153 s |
| prototype, 10k-row chunks | 672–716 s | 29.3–29.8 GB | ok | 90–213 s |
| prototype, 100k-row chunks | 450 s | 21.7 GB | ok | 61 s |
| today at 100M | 2,305–2,322 s | – | none: OOM in the drain | – |
| prototype at 100M | never: 4,131 of 10,001 chunks in 2 h 8 min | 190 GB retained | none: stopped by the disk guard | – |

### 7. Convergence and status

**Decision point (D8, D9).**

- **Should:** the watermark token stays `pg_current_wal_lsn()` read after
  the caller's commit, and `origin_lsn` is stamped from the trigger's
  pre-commit `pg_current_wal_insert_lsn()`. A commit the caller could see
  before taking its token has its ring rows in the same transaction, each
  with `origin_lsn` at or below the commit's position, so the predicate of
  [stage 07](../staging-and-claiming/07-convergence-and-await.md) over ring
  rows alone is sound; a transaction straddling the token over-reports, the
  safe direction. Condition 1 (`confirmed_lsn ≥ token`), the #452 nudge and
  the #312 intake wait go. *Evidence:*
  [E5](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5842011977).
- **Must:** three stored statuses: `waiting_to_backfill` (registered, chunk
  plan not yet written; normally momentary), `backfilling` (applying, chunks
  outstanding), `live`. `catching_up` is not stored. `live` is strict: it is
  reported only once every chunk has committed and every recorded group delta
  is merged, and from then on a token taken after any commit and awaited is
  the whole guarantee, as ADR-0016's [What `live`
  promises](0016-single-background-capture-path.md#what-live-promises)
  states. The two signals stay separate: `await_converged` never reads
  status.
- **Must:** the derived rule for a reader whose upstream is not `live`
  (ADR-0016, #497) stays, computed when status is read: a `live` reader of a
  `backfilling` upstream reports `backfilling`, transitively. The stored
  catch-up parks for a rebuilt target's readers (`park_target_catchup_if_read`,
  #507) go: under element 9 a target's writes are captured like a source's,
  so a rebuild reaches its readers through the ring and I2.
- **Must (I4):** tombstone GC by a per-target batch watermark: the retire
  path, once a segment is fully drained, deletes every tombstone on each
  target whose `applied_lsn` is at or below that segment's fence.
- Every park that exists today because something was missed (fresh install
  under an old catalog, slot loss, a rejoining table, an explicit
  `request_backfill`) becomes a rebuild: Re-derive over the key space,
  which I2 makes safe against any pending change. Slot loss itself no longer
  exists.

### 8. Truncate, DDL, drop

- `AFTER TRUNCATE` statement triggers per captured table stage one truncate
  row; the drain barrier is restated under xid order (#598) and its effect
  is element 3's source-truncate row. A to-side truncate is element 5's
  parent operation with zero values for every parent of the table.
- Source `ALTER TABLE` on a read column follows element 1's marker and
  regeneration rule; on any other column it is invisible.
- Dropping the last definition on a table drops its triggers on the next
  reconcile pass under I6's retry loop. Only the staging worker creates or
  drops triggers; registering processes need catalog access and the right to
  create target tables, as ADR-0016 already requires, and the capture role
  needs `TRIGGER` on each source, which the table owner grants once.

### 9. Multi-hop: a target is captured like a source

**Decision point (D15), not prototyped.** Today a target that is another
definition's source feeds it through the target-mutation seam, which writes
CDC-shaped rows into the ring from inside apply with its own image rules.

- **Should:** every target table carries the same three capture triggers a
  source does, generated from the definitions that read it, and the seam is
  deleted. The upstream apply transaction's `row_txid` is then C for the
  downstream's I2, which is exactly the chained-hop rule the ledger note
  states. A group row incremented by a batch produces one ring row per group
  per batch, which is the batching the seam does today. A building target's
  writes reach its readers the same way, which is what lets element 7 drop
  the rebuilt-target catch-up.
- **Open:** the factored P table is structurally a first hop (`GROUP BY
  group, parent SELECT COUNT(*)`), which suggests the chained-hop machinery
  carries relationships; the per-child fallback would re-stage dependent
  keys into the ring (#354's shape). Cycle detection and `hop_gen` under
  trigger capture, and #272 (demand-driven sealing, unblocked by this ADR
  since finer sealing no longer multiplies any seam), are
  [open question 6](#open-questions).

## What goes away

The removal list is the implementation plan's backbone. Each item names what
patched it.

- **Intake and the slot:** `pgwire-replication`, `TxnBuffer` and its spill,
  the LSN watermark persist and acknowledgement, slot-loss detection and
  recovery, the `pg_logical_emit_message` nudge (#452), `replication_progress`.
- **`REPLICA IDENTITY FULL`** as a requirement on any table (#589 closes).
- **Horizons and bases:** the recompute horizon and extinct horizon and
  `min_image_lsn` (#321/#390); the build horizon (#442); basis rows and
  per-key locks (#344/#356); the watermark wait before enumeration
  (#312/#333); `has_recompute` and `vanished_images` (#486/#493).
- **Projection correctness state:** `to_side_superseded` (#507/#532); the
  `for share` stamp (#531/#554); refresh markers and `refreshed_lsn`
  (#529/#533/#547); projection seeding and widening (#543/#544).
- **Decided, never landed:** bucketed advisory locks (#389); keep-every-image
  fold (#494); prior image on enumeration (#392).
- **Capture paths:** the go-live re-read and orphan sweep (#485/#436); the
  ring enumeration fallback; `pending_backfill` markers, fences, generations
  and `catching_up`; the rebuilt-target catch-up (#507); the target-mutation
  seam (element 9, should).
- **Group probes:** the existence probe and group pre-lock (#326);
  `apply_forced_groups_bulk`.
- **The xid widening** and the `src_xid` migration (V52) from the experiment
  branches; they never reach `main`.
- **Docs:** ADR-0016's go-live, catch-up and marker sections and its "only
  sealer" premise; ADR-0007's single-pass aggregate build; stage 01
  (intake) in full; stage 05's basis-check and recompute-horizon sections
  ([#556 deliverable 2](https://github.com/salesforce-misc/trellis/issues/556)).

## Rejected alternatives

- **Membership-only ledger** (group key and basis, contribution recomputed
  from source on each change): wrong for relationships on both shapes it ran,
  saves 1–2% of WAL
  ([exp 3](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5842993962),
  [exp 4](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5845224204)).
- **Per-child contribution rewrite on a parent change** (`contrib` for
  relationships): 2.3–3.1x `main`'s time and 5x its WAL at 100k fan-out on
  tmpfs, 2.4–2.9x on disk
  ([exp 4](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5845224204),
  [exp 4b](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5851003675)).
- **Visibility-only I2**, and **Apply stamping its own snapshot as the
  basis**: each fails named exp 2 scenarios (5b/9/9c and 9b/9d).
- **Enumerating a parent's children from the ledger index alone** (exp 2
  scenario 6b): misses a child moving into the parent in an uncommitted
  transaction. The live-scan union works but keeps the from-side scan; the
  factored T lock replaces both.
- **Locking the to-side inside the application's transaction** to close
  6b: a synchronous materialized view; puts Trellis locks on the
  application's commit path (I6).
- **Row-level triggers:** 5–10x the statement trigger's cost above 1
  row/commit ([E1](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5842011977)).
  **`to_jsonb`, `jsonb_each_text`, `::text` encodings:** not byte-identical
  to `tuple_to_json`; **`hstore(NEW)`:** identical but reads every column
  ([E3](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119)).
- **Keeping the slot and widening the decoder xid:** exact only with the
  anchor re-read after every change; the naive form is silently wrong for
  every pre-boundary id across an epoch
  ([exp 1a](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5840353484)).
- **Bounding drain memory by running fewer workers:** each worker then holds
  more buckets; bounds nothing
  ([#617 step 3](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5857993459)).
- **Fixing the build's cost by chunk size:** 10x larger chunks leave 2.0x
  the time and 3.7x the WAL
  ([#617 final](https://github.com/salesforce-misc/trellis/issues/617#issuecomment-5859141160)).
- **#556's other candidates.**
  - **(B) Resolve every image-less trigger at staging time**, so Phase 2
    never reads live state. Under trigger capture the only image-less CDC
    row is a delete, and every other image-less producer is an enumeration
    this ADR replaces with Re-derive under lock; what B leaves untouched is
    out-of-order parallel batches for one key, which exp 2 showed needs
    `applied_lsn`, not images. A resolver on the seal path is itself a live
    read racing the drains, the seam again. Not priced further.
  - **(C) Serialize absolute writes per target** through one writer or one
    advisory lock. It does not fix identity: a serialized write still reads
    live state ahead of pending deltas and still needs a horizon to know
    what it absorbed. It also serializes the build, the largest producer of
    absolute writes, which experiment 5 shows is already CPU-bound on eight
    workers at 14–15k rows/s. Its parallelism cost is unmeasured because the
    structural objection comes first.
  - **(D) Thread source transaction ids through the ring:** adopted, in the
    exact form `row_txid` gives for free under element 1.
  - **(E) LSN-versioned projection rows:** covers the relationship half only
    and needs (A) for plain aggregates. Element 5's T table is E's idea in
    a cheaper shape: one applied value with its position per parent, not a
    version history per row.

## Consequences and costs

- **Capture is on the application's write path.** The cost is one ring
  append per statement and about 2.9x the source row's WAL bytes on disk.
  It is paid by the writer at commit, not by a background process later, so
  it is visible and it cannot fall behind: there is no capture lag, only
  drain lag. The write-heavy user gets a documented bulk-load pattern.
- **Trellis runs inside every writer's transaction.** Every future change to
  the ring's shape, the seal, or maintenance inherits I6: a lock the
  application can queue behind is a bug, and a condition that fails the
  application's statement is a bug. Both get a written rule and a test in
  the implementation ([validation 5](#validation-and-acceptance)).
- **Storage grows with the source.** Each aggregate target carries a ledger
  of one row per source row (~3x the source's on-disk size at 10M in the
  prototype), and a relationship target carries P and T. WAL on the hot path
  is 1.6–1.7x today's for plain aggregates and 0.6–1.6x for relationships.
  In exchange, every full old image leaves the ring and `REPLICA IDENTITY
  FULL` is no longer required anywhere.
- **The failure mode of a broken capture is loud, not silent.** A missing
  ring table or revoked privilege fails the application's statement with a
  `CONTEXT` line naming the capture function
  ([E6](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119)).
  The only silent uncapture is an owner disabling the trigger by name, which
  the audit reports.
- **Hosted compatibility** is core Postgres for everything required
  (statement triggers, transition tables, `SECURITY DEFINER`, `TRIGGER`
  privilege); the event-trigger regeneration is an optional extra where the
  host's admin role can create event triggers
  ([E10](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119)).
- **Drain backpressure becomes the operator's number.** Capture can outrun
  the drain by ~20x (1.9M rows/s against ~85k folded rows/s with the
  ledger), and the ring lives in the application's database at ~188 B per
  captured row when nothing drains
  ([E6](https://github.com/salesforce-misc/trellis/issues/565#issuecomment-5844307119)).
  A growth bound and the policy at the bound are
  [open question 7](#open-questions).
- **Build time is no longer one `GROUP BY`.** A build is per-row work on
  every worker. The acceptance bar is linear time with a per-row cost
  independent of table size; the prototype does not meet it yet, and the
  implementation must, at 100M, before element 6 is called done.
- **Every number above was taken on this box.** Phase 1 of #565 and
  experiments 1–4 of #558 ran on tmpfs; the disk-backed numbers are from a
  single NVMe with ~0.4 ms fsyncs and, for #558's disk tier, a box that was
  not always quiet
  ([disk tier](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5851008357),
  [caveat](https://github.com/salesforce-misc/trellis/issues/558#issuecomment-5847942345)).
  Within-session ratios stand; absolute bars are restated in
  [validation 4](#validation-and-acceptance).

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
   **Milestone exit for element 6.**
4. **A quiet-box, disk-backed benchmark round** with a checkpoint inside the
   window and storage stated on every number: `fold-in-ratio`,
   `group-contention` (400/4k/40k), `rel-churn` (10/1k/100k children per
   parent, 100 and 1,000 parent updates/s, child-only churn),
   `build-under-load`, #565's E1 and E2, each with a same-session `main`
   control. The bars in this ADR are restated from that round.
5. **Writer-coupling tests:** schema change of a read column under load
   (writes succeed, definition pauses, regeneration restores capture); join
   and drop under a 30 s open transaction (worst writer wait under 100 ms);
   revoked privilege and disabled trigger (audit reports); `REPEATABLE
   READ` and `SERIALIZABLE` writers straddling a seal (no lost rows).
6. **Drain paging:** a segment several times the batch cap drains with peak
   RSS bounded by the cap, and a key whose changes straddle two pages
   converges.

## Open questions

For the debate; each has a recommendation where one exists.

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
   (the candidates are the P index's page splits and the parent lock taken
   in a different class order by the forward and reverse paths) before the
   ADR claims I5.
4. **What the ADR promises about ledger size and WAL amplification.** ~3x
   the source and 1.6–1.7x WAL at 10M. Can `contrib` be narrowed for plain
   invertible aggregates (a `SUM` needs the value, a `COUNT` needs nothing),
   and does the ledger belong on a separate tablespace?
5. **The status model without a re-read.** Three stored states is the
   recommendation; the derived "upstream not live" rule stays. Is there any
   remaining park that is not a rebuild?
6. **Multi-hop under triggers.** Targets as captured tables and the seam's
   deletion (element 9); whether the factored P table *is* a hop; cycle
   handling; and what this does to #272 and #354.
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

## Supersedes and reshapes

- [ADR-0002](0002-async-data-flow.md): the "via logical replication"
  clause of its decision and its slot-cost consequences. Asynchronous
  derivation stands.
- [ADR-0006](0006-relationships.md): the replica-identity requirements and
  the settled-projection paragraphs of "Incremental maintenance".
- [ADR-0007](0007-direct-set-based-backfill.md): the single-pass aggregate
  build and overwrite-by-group-key; PK-range chunking of 1-1 builds stands
  as element 6's chunk plan.
- [ADR-0016](0016-single-background-capture-path.md): the join fence, the
  go-live catch-up, the orphan sweep, `catching_up`, the marker lifecycle
  and the "only sealer" premise. "Registration does no source reads" and
  "the staging worker owns publication changes" stand, with triggers in
  place of the publication. The `live` contract of 2026-09-24 stands
  unchanged.
- Open issues resolved or reshaped: #326, #354, #455, #456, #458, #495,
  #496, #529, #535, #536, #543, #544, #546, #547, #581, #582, #589, #598,
  #272.

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
