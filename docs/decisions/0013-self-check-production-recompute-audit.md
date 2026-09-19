---
status: accepted
date: 2026-09-19
deciders: Michael Ries
---

# `self_check`: A Production Recompute Audit

Trellis's one hard correctness promise is byte-identical convergence to a
from-scratch recompute at any caught-up LSN. The failure mode that promise guards
against is a *silently stale target*: a wrong answer with no error raised and no
metric out of range, invisible without an independent recompute. `self_check` makes
that recompute a shipped, operator-callable capability instead of something only the
test suite can perform.

## Decisions

### `self_check` is a public method on `Trellis`

It audits one target at a time, is read-only, and returns a `SelfCheckReport`
describing any divergences (cell, missing row, extra row, missing/extra column), the
LSN checked through, the rows compared, and whether the scan was bounded. Fleet-wide
sweeps are a caller-side loop over `definitions()`, not a behaviour of the primitive.

### Postgres is the oracle; the comparison is two-way

`self_check` compares the persisted target against an equivalent recompute *query*
executed by Postgres — for an aggregate target, the corresponding `GROUP BY`. It does
not run the engine's Rust evaluator as part of the comparison. Re-running the
evaluator would check the engine against itself; the authority is Postgres computing
the answer from source rows independently. This also matches the failure class the
audit exists to catch: a stale target diverges as persisted-vs-recompute, where an
independent query is exactly the detector and the evaluator adds nothing.

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
up," never a divergence), and reads the target and runs the recompute inside a single
`REPEATABLE READ` transaction.

A snapshot alone is not sufficient under live load: a single snapshot pins source and
target at one instant, but a correctly-working target legitimately lags its source by
CDC apply latency. Therefore a divergence is reported as real only if it **survives a
re-check after a fresh await** — a genuine divergence is stable; a convergence race
resolves. A strict mode, sound only when writes to the audited tables are stopped, is
the documented strong guarantee for callers that can quiesce.

### Scope: 1-1 first, then aggregates and relationships

The audit query projects the target's key so a divergence is reported per key.
Extending to aggregate and relationship-enriched targets is in scope; scoping an
aggregate audit bounds the *group-key space* and then scans all source rows belonging
to those groups — a source-row predicate would change the answer, since a group's
value depends on every row in it.

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
