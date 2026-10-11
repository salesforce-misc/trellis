---
status: accepted
date: 2026-09-19
deciders: Michael Ries
---

# `self_check`: A Production Recompute Audit

Trellis's one hard correctness promise is convergence to a from-scratch recompute
at any caught-up LSN — **byte-identical** for almost every value, and **up to the
type's own `=`** for the narrow class of aggregate results where byte-identity
would be stricter than correctness (see [Comparison semantics](#comparison-semantics-byte-identical-except-non-injective-aggregate-folds)
below). The failure mode that promise guards
against is a *silently stale target*: a wrong answer with no error raised and no
metric out of range, invisible without an independent recompute. `self_check` makes
that recompute a shipped, operator-callable capability instead of something only the
test suite can perform.

## Decisions

### `self_check` is a public method on `Trellis`

It audits one target at a time, is read-only, and returns a `SelfCheckReport`
describing any divergences (cell, missing row, extra row, missing/extra column, and a
broken capture), the
LSN checked through, the rows compared, and whether the scan was bounded. Fleet-wide
sweeps are a caller-side loop over `definitions()`, not a behaviour of the primitive.

### The capture audit runs first

Capture is statement triggers
([ADR-0002](0002-async-data-flow.md#capture-by-statement-triggers)), so a target is only
as current as the capture feeding it, and a broken capture
is invisible to the convergence wait: convergence is a predicate over ring
rows, and a capture that has stopped writes none. So before it awaits or
compares anything, `self_check` reads from the catalog (`pg_trigger`,
`pg_proc`, `pg_class`, `pg_inherits` and the `has_*_privilege` functions) that every table
the target is computed from (its source and the to-side of each relationship
it reads through, less any this instance's seam feeds) is still captured as
the staging worker installed it:

- all five capture triggers exist, are `ENABLE ALWAYS` (`tgenabled = 'A'`),
  and call their event's function;
- each function exists, is `SECURITY DEFINER`, and is owned by the role that
  owns the ring (the one Trellis role that runs the migrations and installs
  and owns capture). That is not necessarily the schema's owner: a DBA can
  pre-create the schema as another role;
- the role each function runs as still has `USAGE` on the schema, `INSERT` on
  every ring segment, and `USAGE` on the change-id sequence and the ring slot
  mirror;
- the table is still a plain table outside any partition or inheritance
  hierarchy. Acceptance refuses such a table, but nothing refuses a later
  `ATTACH PARTITION` or `INHERIT`, or the table dropped and recreated as one,
  and a statement trigger fires only for the table a statement names. The
  staging worker's reconcile pass runs the same check
  (`defs::hierarchy`) and pauses the table's readers.

Any fault is reported as a `capture` divergence, and the report stops there:
no recompute comparison runs, because it would only show the symptom. The
audit applies once a definition's capture must be installed (`backfilling`,
`catching_up`, `live`). A fault is a catalog fact, not a race, so it isn't
re-checked. The staging worker's reconcile pass reinstalls a missing or
disabled trigger, so those faults last only while no worker runs or its
reconcile can't land; the others persist until an operator fixes them.

### Only a `live` definition is compared

The convergence wait never reads a definition's status, and a repair that
rebuilds a definition (an explicit `request_backfill`, a resume, an `ALTER
TRANSFORM`, a capture re-install) does its work in background jobs the wait
knows nothing about. A comparison made while the definition is being built
would agree or disagree with a state the build is about to change. After the
capture audit, `self_check` therefore reads the definition's status and
compares only a `live` one. Any other (`waiting_to_backfill`, `backfilling`,
`catching_up`, `paused`, `quarantined`) returns the outcome `NotLive`, which
carries that status. It compares nothing and waits for nothing. Like
`NotCaughtUp` it is not a verdict on correctness, and the remedy is the same
kind: poll the status until it is `live`, then check again.

A rebuild is a status transition made in the repairing call's own
transaction ([ADR-0002](0002-async-data-flow.md#convergence-and-status)), so a
check made right after `request_backfill` returns is told `backfilling`
instead of comparing a target the rebuild is still changing.

### Postgres is the oracle; the comparison is two-way

`self_check` compares the persisted target against an equivalent recompute *query*
executed by Postgres. It does not run the engine's Rust evaluator as part of the
comparison. Re-running the
evaluator would check the engine against itself; the authority is Postgres computing
the answer from source rows independently. This also matches the failure class the
audit exists to catch: a stale target diverges as persisted-vs-recompute, where an
independent query is exactly the detector and the evaluator adds nothing.

### Comparison semantics: byte-identical, except non-injective aggregate folds

`self_check` compares a persisted cell against its recompute **byte-for-byte** by
default. Byte-identity is the strongest, simplest yardstick — "diff the text" — and
it is exactly what the join/`GROUP BY`/primary-key roles rely on, so keys are
**always** compared byte-for-byte, no exception.

There is one narrow class where byte-identity is *stricter than correctness*:
`MIN`/`MAX` folded over a type whose `=` admits several textually-distinct
representatives of the same value. `real`/`double precision` (`-0` `=` `0`,
rendered `-0` and `0`) and `interval` (`'1 day'` `=` `'24 hours'`) are the known
cases. Postgres's `min`/`max` is a fold that returns *whichever tied representative
the scan saw last* (`float8larger(a,b)` is `a > b ? a : b`), so the persisted text
and a recompute's text can differ while both are genuinely-correct answers. A
byte-exact check reports that as a divergence no recomputation can ever settle — a
false positive in the audit, not a caught bug.

So for **aggregate result cells produced by such a fold**, the promise holds up to the
type's own `=` (`persisted = recompute`), not the text: a truly wrong `MAX` still fails
(`3 = 5` is false), but a `-0`/`0` tie does not. The carve-out is tight: it covers only
`MIN`/`MAX` result cells of these types, never keys, passthrough, or invertible
aggregates (`SUM`/`AVG`/`COUNT`), which stay byte-identical. `self_check` audits 1-1
targets only (see [Scope](#scope-1-1-targets)), so every cell it compares is compared
byte-for-byte.

### The audit query is rendered independently of the write path

`self_check` renders its recompute query — down to the leaf expression level —
separately from the rendering code the backfill and apply paths use to *write* target
rows. If the audit reused the write path's renderer, a rendering bug would agree with
itself: backfilled, never-modified rows would be re-derived by the same code that
produced them, and the audit would pass on a wrong answer. Independent rendering is
what makes "Postgres is the oracle" true rather than nominal. The leaf renderer is
small; duplicating it is the price of an honest audit.

### Quiescence: distinguish "diverged" from "not yet caught up"

The correctness promise is conditional on being caught up to an LSN, so `self_check`
must never report a merely-lagging target as diverged. It takes a watermark token,
awaits convergence through it (bounded by a timeout; on timeout it reports "not caught
up," never a divergence), and reads the target and runs the recompute under one
snapshot: one statement reads both sides, which gives it one snapshot under any
isolation level.

A snapshot alone is not sufficient under live load: a single snapshot pins source and
target at one instant, but a correctly-working target legitimately lags its source by
CDC apply latency. Therefore a divergence is reported as real only if it **survives a
re-check after a fresh await** — a genuine divergence is stable; a convergence race
resolves. A strict mode, sound only when writes to the audited tables are stopped, is
the documented strong guarantee for callers that can quiesce.

### Scope: 1-1 targets

`self_check` audits 1-1 targets; asked for an aggregate or a relationship-enriched
target it returns an error naming the limit
([known correctness gaps](../known-correctness-gaps.md)). The audit query projects the
target's key so a divergence is reported per key.

### Respect column-level quarantine

`self_check` excludes paused (quarantined) columns from the comparison. A paused
column holds a deliberately stale value; auditing it would report a false divergence
on exactly the targets an operator is most likely to be inspecting.

### Independence from the generative oracle, verified continuously

`self_check`'s renderer and the generative suite's SQL oracle are independently
authored; neither imports the other, and neither shares `SELECT`-assembly logic with
the engine's evaluator. The generative suite runs both renderers over the same
definitions and asserts they agree, on every run. This keeps the two implementations
from drifting and turns the deliberate duplication into a live guarantee rather than a
standing cost.

## Consequences

- `self_check` is a real read load — a full recompute scans source and target — so
  bounded, keyset-scoped calls are mandatory; there is no unbounded "check everything"
  convenience method.
- Trellis gains a shipped, public answer to "is this target correct right now," usable
  by operators, by a thin CLI wrapper, and as a shared end-of-test assertion that
  turns integration tests into correctness tests.
- The engine's evaluator stays internal to the engine and the test suite; the public
  audit path is the independently-rendered query in `self_check`.
