# Glossary

One line per term: what it means here, the section that owns it, and the module
that owns the code. Where one thing has several names, the term is the one to
use and the [aliases](#aliases) point to it. A decision's terms are owned by its
ADR; the [module map](architecture.md) says where each module sits.

## Fences

Each fence is an ordering device. A doc that says just "the fence" means the
one its stage owns: the seal fence in stage 03, the install fence in stage 01.

| Term | Means | Owner |
|---|---|---|
| **seal fence** | The snapshot a seal publishes on a segment's registry row (`fence_snapshot`), which cuts a batch in visibility space. | [03](staging-and-claiming/03-sealing-and-the-fence.md#the-fence), `staging::seal` |
| **version fence** | The per-source-table `source_table_versions` row that a definition change bumps and a page holds `FOR SHARE`, so no page commits values computed under a superseded definition. | [05](staging-and-claiming/05-apply-and-exactly-once-deltas.md#the-version-fence-the-one-failure-idempotency-cannot-fix), `staging::apply` |
| **build fence** | The id of a transaction begun after a build starts (`backfill_chunks.fence_xid`), which a build over a seam-fed source waits out before it reads. | [builds](staging-and-claiming/builds.md#a-seam-fed-source-takes-a-fence), `staging::build` |
| **claim fence** | The lock on a chunk's claim row that a build write holds, so a chunk a resume superseded can't write. | [builds](staging-and-claiming/builds.md#a-1-1-build-re-derive-over-chunks), `defs::chunk_queue` |
| **install fence** | The `SHARE ROW EXCLUSIVE` lock under which a capture install or widen changes a table's triggers, so every commit either precedes the change or ran the trigger; an uninstall's `ACCESS EXCLUSIVE` lock is its mirror. | [01](staging-and-claiming/01-capture-by-triggers.md#the-install-is-the-fence), `capture::install` |
| **call fence** | The first lock of a state-changing call's write transaction, which refuses it if another version is live or the catalog floor is above this release. | [ADR-0023](decisions/0023-version-handover.md#the-rules), `locks::fence_call_txn` |
| **seal gate** | The seal's guard that no writer of the previous batch is still in flight; it is backpressure, not a fence. | [03](staging-and-claiming/03-sealing-and-the-fence.md#the-two-guards--both-are-backpressure-never-overwrite), `staging::seal` |
| **capture gate** | A table's WAL position, recorded by an install or widen, at or below which changes may lack a column; it holds a build until none is pending. | [01](staging-and-claiming/01-capture-by-triggers.md#which-definitions-may-start-a-build), `capture::gate` |
| **chunk gate** | A rejected design: a lock in every page path that a chunked aggregate build would have needed. A 1-1 build chunk's backlog bound is `chunk_allowed`, not a gate. | [ADR-0002](decisions/0002-async-data-flow.md#rejected-alternatives), `staging::build` |

## Stops and waits

| Term | Means | Owner |
|---|---|---|
| **paused** | A definition frozen at its last value, by an operator's `PAUSE` or by Trellis with a recorded reason. | [status](status-reference.md#paused), `defs::lifecycle` |
| **quarantined** | A definition whose whole-transform fuse tripped (too many held keys); frozen like `paused`. | [status](status-reference.md#quarantined), `staging::quarantine` |
| **quarantine** | The mechanism that isolates a failing key or column so the rest keeps applying. | [06](staging-and-claiming/06-cleanup-and-reclaim.md#quarantine-when-a-change-deterministically-kills-its-worker), `staging::quarantine` |
| **held key** | A source key left out of one definition's apply because its change kept failing there. | [status](status-reference.md#held_keys), `staging::quarantine` |
| **paused column** | A field of a 1-1 target frozen by the column fuse or `PAUSE TRANSFORM t.col`. | [ADR-0003](decisions/0003-quarantine-storage-and-api.md#fuse-per-column-then-transform-wide), `staging::quarantine` |
| **halted** | Paused by a halting failure, one no key can be blamed for (`capture_failure.kind = halt`). | [observability](observability.md#a-halting-failure), `staging::halt` |
| **disruption** | Capture changed outside Trellis, a captured table gone, or a logical restore (`capture_failure.kind = disruption`); a pause. | [capture drift](staging-and-claiming/capture-drift.md#capture-changed-outside-trellis), `capture::triggers` |
| **drain holdup** | A page the drain keeps failing on while charging no key and pausing nothing (`drain_failure`). | [status](status-reference.md#drain_failure), `staging::holdup` |
| **capture holdup** | A table whose capture install is held up, by a lock (`capture_wait`) or a failure (`capture_failure`). | [status](status-reference.md#capture_wait), `capture::reconcile` |
| **capture wait** | `capture_wait`: the install can't yet take its lock; cleared when the holder lets go. | [status](status-reference.md#capture_wait), `capture::install` |
| **build wait** | `build_wait`: a build waiting on a [build fence](#fences) rather than on its own work. | [status](status-reference.md#build_wait), `staging::build` |
| **`catching_up`** | Reported, never stored: a `live` definition reading an upstream that isn't `live`. | [status](status-reference.md#catching_up), `defs::model` |

## Builds

| Term | Means | Owner |
|---|---|---|
| **build** | Filling a target from the rows its source already held, while it applies; `waiting_to_backfill`, `backfilling`, `live`. | [builds](staging-and-claiming/builds.md), `staging::build` |
| **rebuild** | A build of an already-built definition: a resume, `request_backfill`, a capture re-install or an automatic rebuild. | [builds](staging-and-claiming/builds.md#a-1-1-build-re-derive-over-chunks), `staging::build` |
| **rebuild in place** | A rebuild of a `live` definition that keeps applying: readers see the old values, then the new, never a partial target. | [builds](staging-and-claiming/builds.md#an-aggregates-or-a-joins-build), `staging::build` |
| **Re-derive** | The 1-1 operation that re-reads one key's source row under its entry lock and writes the target row and the entry's basis; a build is Re-derive over chunks. | [ADR-0002](decisions/0002-async-data-flow.md#the-1-1-ledger-one-target-owned-entry-per-source-row), `staging::one_to_one_ledger` |
| **absolute write** | The group-side counterpart of Re-derive: replaces groups' partials, basis and stamp from one read of the source. | [group basis](staging-and-claiming/group-basis.md#absolute-writes), `staging::group_basis` |
| **recompute** | Not a term of its own: Re-derive for a 1-1 key, an absolute write for a group; a staged `Recompute` change asks for one. | [ADR-0002](decisions/0002-async-data-flow.md#aggregates-a-basis-per-group-kept-by-deltas), `staging::append` |
| **field build** | The chunks that rewrite just the fields an `ALTER TRANSFORM` or a column resume changed, leaving the entries alone. | [builds](staging-and-claiming/builds.md#a-1-1-build-re-derive-over-chunks), `staging::build` |
| **correction** | Re-deriving the groups of one node of a partitioned source after its tree changed; `status().correcting`. | [ADR-0017](decisions/0017-partitioned-sources.md#each-changed-node-is-corrected-in-the-pass-that-sees-it-with-no-full-rescan), `staging::partition_correction` |
| **sweep** | Three things: a 1-1 rebuild's closing job over entries no chunk reached; the every-pass sweep of rows routing to no leaf; the generative suite's planted-bug sweep. | [builds](staging-and-claiming/builds.md#a-1-1-build-re-derive-over-chunks), `staging::build` |
| **catch-up** | A retired design: applying what a build skipped once it finished. Every build applies from its start now; `intake::catch_up` keeps the name for rebuilding a table's readers. | [data-flow](data-flow.md#capturing-a-tables-existing-rows), `intake::catch_up` |
| **reconcile pass** | One iteration of the staging worker's maintenance loop: it brings capture to the catalog, pauses readers of drift, and starts ready builds. | [01](staging-and-claiming/01-capture-by-triggers.md#which-definitions-may-start-a-build), `capture::reconcile` |

## Units of work

| Term | Means | Owner |
|---|---|---|
| **segment** | One ring slot's append-only rows, `ACTIVE` until sealed. | [02](staging-and-claiming/02-the-staging-ring.md#the-objects), `staging::seal` |
| **batch** | A sealed segment's fenced rows: the unit of batching, not always of work. | [04](staging-and-claiming/04-claiming-and-the-fold.md#partitioning-a-batch-across-workers), `staging::claim` |
| **bucket** | A slice of a batch, split by key route at seal time; the unit of a claim. | [04](staging-and-claiming/04-claiming-and-the-fold.md#partitioning-a-batch-across-workers), `staging::claim` |
| **share** | The buckets one claim takes: `ceil(free buckets / live workers)`. | [04](staging-and-claiming/04-claiming-and-the-fold.md#partitioning-a-batch-across-workers), `staging::claim` |
| **window** | A batch's fenced window is its rows under its seal fence; the drainer window is how long a `drainers` row counts as live. | [03](staging-and-claiming/03-sealing-and-the-fence.md#the-batch-is-sized-when-its-fence-is-published), `staging::claim` |
| **page** | At most `drain_batch_cap` folded records applied in one transaction. | [04](staging-and-claiming/04-claiming-and-the-fold.md#paging-a-share-larger-than-the-cap), `staging::page` |
| **chunk** | A range of a source's key that a build re-derives in one transaction; also a fan-out step of a 1-1 `JOIN`. | [builds](staging-and-claiming/builds.md#a-1-1-build-re-derive-over-chunks), `defs::chunk_queue` |
| **structure** | A hidden table a build writes in one absolute write: an aggregate's `agg_<id>`, a `JOIN`'s `P`, `M` and `T`. | [ADR-0002](decisions/0002-async-data-flow.md#an-aggregates-build-is-one-absolute-write-per-structure), `staging::build` |
| **side** | Half of a change: its negated old image or its new image. Also one of a `JOIN`'s tables, `a` or `b`. | [group basis](staging-and-claiming/group-basis.md#a-page), `staging::group_basis` |
| **cell** | One row of a `JOIN`'s `P` or `M`: a (group, join key) pair. | [ADR-0019](decisions/0019-join-transforms.md#each-side-is-aggregated-before-the-join-and-the-target-is-their-combination), `staging::join` |

## Ordering state

| Term | Means | Owner |
|---|---|---|
| **basis** | The `pg_snapshot` of the last absolute write (a Re-derive or a group's absolute write) to a key's entry or a group; a change is applied only if its transaction isn't visible in it. | [05](staging-and-claiming/05-apply-and-exactly-once-deltas.md#the-ledger), `defs::ledger` |
| **stamp** | The newest segment a group's basis saw. | [ADR-0002](decisions/0002-async-data-flow.md#aggregates-a-basis-per-group-kept-by-deltas), `staging::group_basis` |
| **xbasis**, **xstamp** | A group's own basis and stamp for its `MIN`/`MAX` extremes. | [group basis](staging-and-claiming/group-basis.md#where-a-groups-state-lives), `staging::group_basis` |
| **floor** | Two things: the truncate floor (`ledger_truncate_floor`), at or below which a target ignores changes; and a compatibility floor, the oldest release that can read the catalog or a definition. | [ADR-0002](decisions/0002-async-data-flow.md#truncate-ddl-drop), [ADR-0024](decisions/0024-compatibility-floors-and-rollback.md#the-rules), `staging::ledger`, `defs::floor` |
| **drained prefix** | The highest segment at or below which every segment is drained; tombstones and zero groups are collected only past it. | [05](staging-and-claiming/05-apply-and-exactly-once-deltas.md#the-ledger), `staging::retire` |
| **watermark token** | The caller's WAL insert position, read after its write commits, which `await_converged` waits to pass. | [07](staging-and-claiming/07-convergence-and-await.md#watermark-tokens), `staging::converge` |
| **origin** | `origin_lsn`: the oldest un-reflected trigger position a pending change carries, which the token compares against. | [07](staging-and-claiming/07-convergence-and-await.md#the-predicate), `staging::converge` |

## Hidden tables

| Term | Means | Owner |
|---|---|---|
| **`<target>__ledger`** | A 1-1 target's per-key ledger, in the target's schema. | [05](staging-and-claiming/05-apply-and-exactly-once-deltas.md#the-ledger), `defs::ledger` |
| **`agg_<id>`** | An aggregate's hidden table of per-group partials, basis and stamp. | [group basis](staging-and-claiming/group-basis.md#where-a-groups-state-lives), `staging::group_basis` |
| **`P`, `M`, `J`, `T`** | An aggregate `JOIN`'s partials of `a` by (group, key), of `b` by (key, group), the per-key lock, and the target's groups. | [ADR-0019](decisions/0019-join-transforms.md#each-side-is-aggregated-before-the-join-and-the-target-is-their-combination), `defs::join` |
| **`R`, `M`, `J`** | A 1-1 `JOIN`'s copy of `a`, one copy per joined table, and the per-key lock. | [05](staging-and-claiming/05-apply-and-exactly-once-deltas.md#a-1-1-joins-hidden-state), `defs::join_one_to_one` |
| **`join1_fan_out`** | Not hidden: the catalog table of 1-1 `JOIN` fan-out markers, one per `b` change reaching more `a` rows than one chunk, until its last chunk commits. | [05](staging-and-claiming/05-apply-and-exactly-once-deltas.md#a-1-1-joins-hidden-state), `staging::join_one_to_one` |

## Other terms

| Term | Means | Owner |
|---|---|---|
| **seam** | The one path every write to a Trellis-owned target takes, which feeds the target's readers. | [ADR-0002](decisions/0002-async-data-flow.md#multi-hop-a-targets-writes-reach-its-readers-through-the-mutation-seam), `staging::target_mutations` |
| **tombstone** | A deleted row's ledger entry, kept until the drained prefix passes it. | [05](staging-and-claiming/05-apply-and-exactly-once-deltas.md#the-ledger), `staging::retire` |
| **typed key index** | Not built: a key index comparing decoded values, which would admit key types the `::text` allowlist refuses. | [type support](type-support.md#text-rendering), `defs::join_key_types` |

## Aliases

| Alias | Use |
|---|---|
| the fence (stage 03), published fence, `seal_snapshot` | [seal fence](#fences) |
| table fence, source fence, pause fence | [version fence](#fences): a pause bumps it |
| seam fence, per-build fence, a build's fence | [build fence](#fences) |
| join fence, drop fence, the fence (stage 01) | [install fence](#fences): ADR-0002's names for the install's lock and the uninstall's |
| handover fence | [call fence](#fences) |
| marker fence | Rejected ([ADR-0002](decisions/0002-async-data-flow.md#rejected-alternatives)) |
| re-read | [rebuild](#builds) |
| backfill | [build](#builds); the status words and `request_backfill` keep it |
| drain batch | [page](#units-of-work) |
| capture pass, staging pass, staging worker's pass | [reconcile pass](#builds) |
