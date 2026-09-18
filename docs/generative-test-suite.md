# The Generative Test Suite

How Trellis proves the claim in its README — *"once caught up to a given LSN,
each incrementally-maintained target is exactly equal to a full recompute of its
definition against the source data at that LSN, for any interleaving of source
changes"* (`docs/data-flow.md#correctness`).

Hand-written examples cannot cover a combinatorial space (definition shapes ×
value domains × op sequences × drain interleavings × concurrency), and each
encodes the same assumptions the implementation does. The suite instead
**generates random valid programs** — a schema, transform definitions, and a
sequence of source mutations — drives each through the real engine against a real
Postgres (`testkit`), compares the settled derived state against an **independent
recompute oracle**, and shrinks any failure to a minimal reproducing program.

The build plan lives in the tracking epic (salesforce-misc/trellis, label `Epic`:
*Generative correctness test suite*); this is the design behind it.

---

## 1. Architecture

The suite is the `generative` crate — a workspace member, so ordinary
`cargo build`/`cargo test` compile it and a signature change can't rot it
silently. It dev-depends on `testkit` and `trellis`.

`testkit` already provides:

- **A disposable cluster** — `TestCluster` runs its own `initdb`/`postgres`/`pg_ctl`
  on a private socket, `wal_level=logical`, torn down on `Drop`. Unlocks the whole
  database-administration action space (restart, checkpoint, slot drop).
- **Process lifecycle** — `crash::CrashGuard` supervises a child it can `SIGKILL`,
  so "kill a worker mid-drain" is a real code path.
- **A straddler primitive** — `crash::OpenTransaction` holds a transaction open
  across a concurrent operation.

The suite's modules are strictly separated:

```
model/      plain-data description of a generated program. Names no engine internals.
generate/   generators producing only valid programs from a Config. Makes no engine calls.
oracle/     independent recompute. Uses only the shared components named in §2.
backend/    the ONLY module that drives the engine's pipeline and reads back.
run/        drives a program through the backend, asserts the properties.
tests/      property declarations, hand-built pins, meta-tests.
```

The seam is a structural guarantee: the oracle *cannot* cheat by calling
maintenance code if that code is only reachable through `backend/`, and a second
backend (concurrent, later subprocess-supervised) drops in without touching
generators or oracle.

### The model

A `Program` is plain, `Debug`-legible data — what the shrinker minimizes and what
prints on failure:

```
Program {
  tables:      [ { name, pk_col, columns: [{name, type}] } ]
  defs:        [ TransformDef ]         // reuses trellis::defs::ast, see §2
  ops:         [ Insert{table,row} | Update{table,pk,changes} | Delete{table,pk} ]
}
```

Use small fixed name pools (`t0`, `d0`, `c0`): a shrunk counterexample you can
read at a glance beats one that is technically smaller. Give every table its PK
column **unconditionally**, so no shrink step can strand a definition referencing
it.

`defs` reuses `trellis::defs::ast::TransformDef` directly — 1-1 definitions
(including numeric `+`), `GROUP BY` aggregates, and named relationship
declarations with the three relationship-reading field shapes of ADR-0006. The
model is shaped so cross-join definitions and non-DML actions slot in later.

### The backend seam

One module drives the engine, and its job is narrow:

- create source tables; install transform definitions and their target tables;
- apply each op as **raw source DML** — never an application-level notification
  API, because the production path is logical-replication change capture
  (`docs/data-flow.md#ingestion-via-logical-replication`);
- `quiesce()` — block until the engine catches up, defined exactly as a *client's*
  read-your-own-writes check: watermark the LSN after the last commit, `await`
  until derived state converges past it (stage 07, #12). Not a `sleep`.
- `snapshot()` — read back merged source+derived state as a deterministic,
  **ordered** map (table → pk → column), so diffs and equality are stable.

## 2. The oracle is Postgres itself

Per ADR-0004, the calculation grammar is an *immutable subset of PostgreSQL's
operators and functions* — every accepted expression has identical semantics to
the same expression in Postgres. So the oracle *is Postgres*: render a
definition's formulas (and predicate, and grain) back to a `SELECT` against the
source, run it in the same cluster, and assert the persisted target equals it.

- **1-1:** `SELECT pk, (price + tax) AS total FROM orders` → compare row-by-row.
- **Aggregate:** `SELECT grain, sum(measure) FROM src GROUP BY grain` → compare.

This oracle shares **no evaluation code** with the engine — only the parser AST
and a small AST→`SELECT` printer. It catches every class of bug, including one in
the engine's own evaluator (the thing the "Rust layer that mirrors Postgres"
thesis rests on), and is fast at volume. It can't outgrow the oracle: the grammar
is *committed* to a Postgres subset, so a construct with no Postgres equivalent is
out of scope by definition.

### The evaluator recompute is a secondary cross-check

`trellis::defs::oracle::recompute` (evaluator-driven) is retained as a parity
check on the mirror-Postgres claim. Per op we compare:

- persisted target **vs. Postgres-SQL oracle** — correctness (a mismatch is a
  pipeline / fold / exactly-once-apply / ordering / convergence bug);
- engine evaluator **vs. Postgres-SQL oracle** — the ADR-0004 claim itself (a
  mismatch localizes an eval-layer drift bug).

A divergence therefore localizes cleanly to *which* layer is wrong.

### Refuse to guess on unmodeled shapes

Meeting a definition shape the generator isn't supposed to produce, the oracle
**panics with a message naming the missing work** — never a best-effort value. A
silent fallback means the day the generator widens, someone gets a flood of false
differentials that look like an engine bug.

## 3. Generate valid programs, not garbage

Filtering random garbage down to valid programs is slow, biases the distribution
invisibly, and breaks shrinking. We generate within the rules:

- **Seed before mutate:** inserts with fixed small PKs first, so updates and
  deletes have rows to hit.
- **Definitions reference only columns that exist**, built from the declared schema.
- **Bounded value domains, with the numeric-path pairing written down beside the
  domain as a load-bearing invariant.** A sum over small integers matches the
  engine's exact path; widen the domain and the oracle overflows into a different
  numeric path while the column type is unchanged. Both sides settle on Trellis's
  `numeric.rs` exact-decimal type; domains keep them on the same path.
- **Deliberately generate the awkward values:** nulls in every nullable column,
  empty strings (distinct from NULL), the literal text `"NULL"`, strings
  containing any key-encoding delimiter.

Two rules protect the suite from lying to itself:

- **An install rejection is a hard failure, never a skip.** If generators emit
  only valid programs, a definition-time rejection is a generator or engine bug —
  a `_ => skip` arm silently stops testing whole shapes.
- **Generator coverage meta-tests** (no database): every scalar type appears both
  as a column and via a derivation; every operator appears over every supported
  argument type. Coverage that silently drops out is otherwise invisible.

## 4. The properties (lit up as engine stages land)

Ordering matches the dependency chain; each property names the stage that
unblocks it.

- **Convergence, per op** *(idempotent 1-1 pipeline: fold #10, apply #11)*. After
  each op: `quiesce`, assert materialized state equals the oracle. Per-op checking
  localizes shrinking to the *first* diverging op. Cost is a full round trip per
  op, so case counts stay low.
- **Operation errors are checked, not swallowed.** A rejected op (update of a
  never-inserted row) is applied to the oracle too, and state must still match — an
  op that errors changed nothing.
- **Idempotency** *(exactly-once apply #11)*. Run once normally, then again
  applying every op twice; both converge identically. Catches at-least-once
  replication delivery treated as exactly-once.
- **Order-insensitivity over commuting ops.** Reordering ops on *distinct*
  `(table, pk)` targets must converge identically.
- **Read-your-own-writes** *(await #12)*. Trellis ships `await(LSN, timeout)` as
  its freshness promise, so it earns its own property: take a token after op *k*,
  await it, assert the visible derived state includes every effect of ops 1..k.
- **Non-idempotent delta convergence** *(aggregate slice)*. The hardest guarantee —
  byte-identical convergence to a from-scratch `GROUP BY` oracle under aggregate
  delta maintenance. Grain domains stay *tiny* (`0`, `1`, `2` — see
  `generative::generate::strategy::grain_value`'s doc comment) so many rows share a
  group and deletes really kill groups. **Not** `NULL`: a legal SQL `NULL` grouping
  value hits a real, still-open engine defect (a NULL-keyed group is silently and
  permanently dropped, no operator-driven recovery — mechanism and out-of-scope
  reasoning in `grain_value`'s doc comment), so the generator never draws one until
  that lands.
- **A second, structural oracle** *(needs an engine consistency auditor, if one
  exists)*. Run the engine's internal consistency check at end of run, require zero
  findings, prove the wiring with a negative test.

### Two runtimes, one oracle

The same programs run in two modes against the same oracle:

- **Manual:** harness-driven, one worker, lockstep apply → quiesce → compare.
  Deterministic; source of clean shrinks and reproducible seeds.
- **Concurrent:** the real worker pool and multi-worker claiming (#14), quiescence
  as the client `await` check. Exercises wakeup, parallel claiming, and await
  semantics.

Identical shape means a divergence in only one mode localizes immediately to
concurrency. We do **not** run a property in concurrent mode for a path known to
be un-safe yet.

## 5. Comparison semantics: exactness is a per-type decision

- **Exact equality** for integers, text, dates, timestamps, UUIDs, booleans.
- **Exact decimals compared by value, ignoring scale** (`2.50 ≡ 2.5`). Byte-level
  rendering is pinned in a separate test.
- **Floats within a combined absolute+relative tolerance** (~1e-9), NaN ≡ NaN. (Not
  reachable until the value language gains floats.)
- **A column present on one side and absent on the other is a divergence,** not a
  missing-key skip — this is how an unwritten derived cell surfaces.

On failure, emit one line per differing cell
(`table[pk].column: expected=… got=…`), present/missing lines for rows and tables,
**and the whole `Program`**. A diff without the program is unactionable.

## 6. What keeps a green run meaningful

**A green run must prove it ran.** `testkit` provisions its own cluster, so the
classic "misconfigured cluster skips, every skip counts as pass" trap is mostly
designed out — but we still enforce the shape:

- Split the outcome into `Ran` / `Unavailable` (nothing to provision — genuinely
  inconclusive, never green) / `BackendUnusable` (provisioned but couldn't stand
  up — never a pass, carries the error text). One `outcome.as_pass()` decision
  point every test asserts through.
- A **meta-test** that fails fast if the harness can't stand up a cluster it should
  have been able to.
- **A README rule of thumb:** a real full run is minutes, not seconds. A suite that
  "passed" in one second executed nothing.
- **Print the resolved connection target**; refuse to run against a database the
  run did not name.

**Findings terminate as deterministic pins; generative runs are lead generation.**
A seed reproduces the *program*, not reliably a *race*, and does not survive
generator refactors. Three artifact kinds:

- **Saved seeds**, replayed first every run — cheap exact-regression catching.
- **Hand-built minimized programs** in the model's types — durable pins that double
  as documentation, immune to generator refactors.
- **A control test beside each finding** — a case that must *converge* — proving the
  harness can tell green from red.

## 7. The action space, and how it grows

Every candidate action falls in one of three buckets by its effect on the oracle:

- **Bucket 1 — oracle identities:** restart Postgres, kill/scale workers,
  checkpoint, `VACUUM`, drop a slot, DML/DDL on *untracked* tables, session-setting
  changes. None change the correct converged state, so the oracle needs **no
  changes** — the action drops into the op stream, then quiesce and compare.
- **Bucket 2 — cheap for a recompute oracle to model:** define/redefine/remove a
  transform mid-stream, change a grain. One line on the definition list. Unlocks
  define-then-load ≡ load-then-define.
- **Bucket 3 — source data/shape changes:** source DDL, `TRUNCATE`, restore. The
  source effect is easy; the real work is modeling the engine's *policy* when a
  definition's dependencies change under it — Trellis's quarantine policy (stage 06,
  #16), still partly undecided (`docs/open-questions.md`).

Sequencing, cheapest-first (each its own future story under the epic):

1. **Untracked-object noise** (bucket 1) — nearly free; converged state must be
   unchanged by DML/DDL on tables Trellis doesn't track.
2. **Definition lifecycle** (bucket 2) — unlocks define-then-load ≡ load-then-define.
3. **Engine lifecycle** (bucket 1) — kill/restart/scale workers; where resume and
   idempotent re-application get tested.
4. **Transaction shapes** (bucket 3) — a transaction held open across a batch
   boundary (via `OpenTransaction`).
5. **Database administration** (bucket 1) — restart PG, checkpoint, slot
   drop/invalidation.
6. **Out-of-band tampering** (bucket 1, detection) — direct writes to derived
   tables; promotes the auditor's negative wiring test to a swept property.

Two structural notes for when faults enter the stream:

- **Bulk operations are their own code paths.** `COPY`, `INSERT…SELECT`, one huge
  transaction, mass grain migration, and especially **`TRUNCATE` (its own
  logical-decoding message, not a bulk delete)** each reach batching/spill/streaming
  paths row-at-a-time DML never does. Model bulk as a generated dimension with a
  **shrinkable row-count**.
- **Keep shrinking alive under faults.** Fault *placement* is deterministic (named
  failpoints where races live); the *schedule* is not. Seeds replay the placement;
  the oracle judges converged state at quiescence, which is schedule-independent.

## 8. Four layers, one action vocabulary

| Layer | Trigger | Judged by | Reproducible? | Cadence |
|---|---|---|---|---|
| 1. Deterministic (unit, integration, scripted interleaving pins) | scripted | assertion | fully | every run |
| 2. Generative, healthy path | random program, seeded | recompute oracle at quiescence | at program level (shrinking) | every run |
| 3. Generative + fault actions | random program incl. faults, seeded | same oracle at quiescence | at program level (shrinking) | minutes–hours |
| 4. Black-box chaos | wall-clock random | invariants over recorded history | no | nightly / pre-release |

- **Layer 1 already exists** — `trellis/tests/*` and the `testkit` crash/straddler
  pins. Every race bug found by layers 2–4 terminates as a layer-1 pin here.
- **Layer 2 is what this epic stands up first.**
- **Layer 3** is reachable only because a fault action is an oracle identity (§7)
  and because the generative and scripted harnesses **share one action vocabulary**:
  a scripted scenario is a generated program with its draws pinned.

## 9. Operational shape

- Reuse one replication slot and publication across cases; reset only the schema
  between them. Serialize shared-cluster access through a process-wide lock.
- Dial case counts down (12–24 per property, env override for deep runs); give each
  property its own count by cost; give shrinking a generous iteration cap.
- Persist failing seeds to a checked-in regression file, replayed first.
- Document how to run one property alone on a clean database; every property
  bootstraps what it reads, so it never depends on run order.

## 10. Open decisions

- **Property-testing framework.** Shrinking is central and no framework is in the
  tree (only `rand`). `proptest` (typed strategies + integrated shrinking) is the
  natural fit; hand-rolling avoids a dependency at the cost of reimplementing
  shrinking. New dependency — **gated on explicit approval**.
- **Concurrent-runtime shrink trust.** Until the engine's concurrency is settled,
  concurrent-mode shrinks are advisory; re-shrink under the manual runtime when a
  failure reproduces there.
- **How the Postgres-SQL oracle models the quarantined subset.** A row that makes a
  formula error (division by zero, overflow, type error) is *quarantined* by Trellis
  and kept out of the target, whereas one `SELECT` over the whole source aborts on
  the first such row. So once erroring operations exist, the oracle must model
  quarantine (compute per-row and exclude erroring rows, or render error-to-`NULL`).
  **Left open — resolve alongside aggregates.**
- **Quarantine policy under definition-dependency changes** (bucket 3) is likewise
  undecided (`docs/open-questions.md`).
