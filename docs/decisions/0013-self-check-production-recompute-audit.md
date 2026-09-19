---
status: proposed
date: 2026-09-19
deciders: Michael Ries
consulted:
informed:
---

# `self_check`: A Production Recompute Audit, Kept Independent of Both the Evaluator and the Generative Oracle

Issue #174 (part of epic #2) proposes promoting the `generative` crate's recompute
oracle — today a test-only fixture — into a shipped, production-callable
`self_check` capability. The motivation is concrete: every one of Trellis's worst
historical bugs (#98/#165/#168 stale TRUNCATE enrichment, #47 aggregate replica-identity
drift, #65 publication silently dropping CDC, #79) is a **silently stale target** — no
error, no metric out of range, just a wrong answer nobody notices without a recompute.
`docs/data-flow.md#correctness` states Trellis's one hard promise (byte-identical
convergence to a from-scratch recompute at any caught-up LSN); today that promise is
checkable only from inside the test suite.

The issue itself flags a load-bearing design question: the two recompute oracles
(the existing test-only one and a new production one) must stay genuinely
independent — sharing no rendering code with each other, or with the engine's own
evaluator — or the very thing meant to catch a bug in either one becomes unable to.
That's the property this ADR exists to nail down before implementation, per the
issue's own request ("worth an ADR, since it's an intentional, load-bearing
duplication that a future reader will otherwise 'clean up'").

Grounded against the code at commit `98f9672` (current `main`), not the issue text
alone. Reading the actual code changed one important fact the issue text doesn't
mention: **a second SQL-rendering oracle already exists inside the `trellis`
crate itself**, separate from `generative`'s, and `self_check` should be built by
hardening it rather than porting `generative`'s from scratch. Reading it closely
also turned up a complication the first draft of this ADR got wrong and §4 now
reckons with: that renderer's *leaf* expression function is not test-only — the
production backfill and aggregate-apply paths already render through it. Both
findings drive the sizing and independence reasoning below.

## What's already there

### `generative::oracle` (`generative/src/oracle/mod.rs`, ~1730 lines)

The suite's independent recompute oracle. Renders a `TransformDef` back to a
`SELECT` (`render_select`/`render_rel_select`, covering 1-1, `GROUP BY` aggregate,
and to-one/to-many relationship enrichment), runs it against Postgres, and
three-way-compares it against the persisted target and against
`trellis::defs::oracle::recompute` (the engine's evaluator run from scratch). Its
own module doc is explicit about the independence property: it "shares no code
with the thing it checks" and is deliberately independent of
`trellis::defs::oracle::render_expr_sql` too (not just the evaluator) — see its
`render_expr` doc comment. It never imports `trellis::client`/`trellis::staging`;
it only reads.

### `trellis::defs::oracle` (`trellis/src/defs/oracle.rs`, 1019 lines) — not `generative`'s

This module lives **inside `trellis` itself**, and already contains two
independent things, cleanly documented apart:

1. **`recompute`/`recompute_aggregate`** — evaluator-driven (reads source rows,
   runs them through `evaluate`/`evaluate_aggregate`, the exact functions the
   incremental-maintenance path uses). Explicitly documented "a *secondary*
   cross-check, not the authority."
2. **`render_expr_sql`/`render_aggregate_select_sql`/`render_relationship_select_sql`/
   `render_aggregate_relationship_select_sql`** — a from-scratch AST→`SELECT`
   renderer that shares no code with `evaluate` and covers 1-1, `GROUP BY`
   aggregate, to-one, and to-many-aggregate relationship shapes. Every function's
   doc comment says the same thing: "test/benchmark oracle only."

Today this module is `pub mod oracle` (unconditional, no feature gate) under
`pub mod defs` in `trellis/src/defs.rs`. Its three top-level `SELECT` renderers
(`render_aggregate_select_sql`, `render_relationship_select_sql`,
`render_aggregate_relationship_select_sql`) are indeed called only from tests
and the benchmark crate — roughly ten files under `trellis/tests/`, plus
`benchmark/src/scenario.rs:295`.

**But the leaf expression renderer they are all built on is not test-only, and
this is load-bearing for everything below.** `render_expr_sql` — and the two
`pub(crate)` helpers beside it, `render_to_one_rel_expr_sql` and
`to_one_join_clauses` — are called directly from the production write paths:

- `trellis/src/defs/backfill.rs:83,592` — `write_one_to_one_range`, i.e. the
  **1-1 direct backfill** that produces a 1-1 target's initial contents
  (ADR-0007). Also `backfill.rs:911,930` for the aggregate direct build.
- `trellis/src/staging/apply_aggregate.rs:2272` — `render_agg_expr`, on the
  **incremental aggregate apply** path, plus `to_one_join_clauses` at
  `:1365` and `render_to_one_rel_expr_sql` at `:1325`.

`oracle.rs`'s own doc comment (lines 443-447) states this outright:
`render_to_one_rel_expr_sql` "is the shared renderer for every place a to-one
path has to become SQL over a real `LEFT JOIN` … the aggregate direct build
(`super::backfill::backfill_aggregate`), the aggregate incremental recompute
(`staging::apply_aggregate::apply_forced_groups_bulk`), and this module's own
`render_aggregate_relationship_select_sql` oracle." The per-function
"test/benchmark oracle only" doc comments apply to the top-level `SELECT`
renderers, not to the expression leaf.

The four renderers *are* evaluator-independent in the sense that matters most
(`render_expr_sql` is a pure `match` over the AST — traced end-to-end, it never
calls into `defs::eval`). But "barely used, nothing production depends on it" is
wrong, and §4 has to reckon with the consequence rather than assume it away.

So the codebase has **two SQL renderers that share no linkage** —
`trellis::defs::oracle`'s and `generative::oracle`'s (verified: `generative`
imports only `trellis::defs::oracle::{OracleError, recompute,
recompute_aggregate}` at `generative/src/oracle/mod.rs:43`, and `recompute` is
confined to `evaluator_oracle`, never `sql_oracle`). `self_check` does not need
to be invented from nothing; it needs `trellis::defs::oracle`'s existing renderer
**hardened, scoped, and exposed**. That is still less work than a from-scratch
port of `generative`'s — but see §5 for why "already exists" is not the same as
"almost done."

### One caveat on how independent the two renderers really are

At the `SELECT`-assembly level they are genuinely different designs:
`generative` casts every projection to text and emits a leading pk expression,
dispatches through one `render_select` that branches on `uses_relationships`,
and renders join `ON` operands in the opposite order; `trellis` has four separate
entry points, runs `backfill::substituted_field_exprs` first, and emits no cast
and no pk. A pipeline, fold, or join-assembly bug on one side would show up
against the other.

At the **leaf expression** level they are not independent derivations.
`generative::oracle::render_expr` (`mod.rs:308-338`) is a near-line-for-line
transcription of `trellis::defs::oracle::render_expr_sql` (`oracle.rs:401-435`):
same arm order, same `COUNT`-with-empty-args guard ahead of the generic arm,
byte-identical `format!("({} {symbol} {})", …)` and
`format!("'{}'::text", text.replace('\'', "''"))`. Combined with the finding
above — that `render_expr_sql` is what production backfill and aggregate-apply
render through — the honest picture is that **there is effectively one
expression-rendering implementation across all three sites**. A reasoning bug at
that level (the wrong Postgres spelling for an operator, a collation-sensitive
form, a `COUNT` semantics mistake) is today invisible to *both* the generative
suite's three-way check and a `self_check` built on this renderer.

This does not sink the proposal — the bug class #174 is actually aimed at
(§"Consequences" below, and #98/#165/#47/#65) is pipeline staleness, not
expression rendering, and against that class the sharing is harmless. But it
narrows what `self_check` can honestly claim, and it makes the §4 cross-check
test weaker than it first appears: two transcriptions of the same arms will
agree with each other by construction. Flagged as open question 7.

## Decision

**`self_check` ships as a new method on `Trellis` (`trellis/src/app.rs`), built by
productionizing `trellis::defs::oracle`'s existing SQL-rendering functions —
adding the operator-safety and quiescence contract they don't have today — while
`generative::oracle` stays exactly as it is: a third, independently-authored
renderer that never imports or is imported by `self_check`'s code.** The
duplication the issue asks to preserve is between `generative::oracle` and
whatever `self_check` uses — not a reason to write a *fourth* renderer when a
second, already-independent one already exists in the right crate.

### 1. What `self_check` actually checks (v1 scope)

- **One target at a time**, addressed the same way quarantine's API already
  addresses targets (`QuarantineTarget::parse`, `"transform"` — see open question
  6 on whether the same string form fits). No "check every target" call in v1;
  fleet-wide sweeps are a caller-side loop over `Trellis::definitions()`, not
  something `self_check` does internally — that keeps the expensive part
  (deciding *when* to sweep everything) out of the audited primitive.
- **v1 covers 1-1 targets only**, matching the issue's own phase 2 — but because
  `trellis::defs::oracle` already renders aggregate and relationship shapes too,
  extending scope (issue's phase 3) is a much smaller lift than porting fresh
  code: it's wiring the report/API layer to functions that already exist and
  already pass their own unit tests, not writing new renderers.
- **Two-way, not three-way.** Production `self_check` compares only *persisted
  target* vs. *SQL-rendered recompute*. It does **not** also run the
  evaluator-driven leg (`defs::oracle::recompute`) the way the generative suite's
  three-way comparison does. Reasoning: the evaluator leg's whole value is
  catching a drift between the engine's Rust evaluator and Postgres semantics
  (the ADR-0004 "engine mirrors Postgres" claim) — a *testing* concern, already
  covered continuously by the generative suite. In production, it adds a second
  full source scan and a dependency on an internal engine module
  (`defs::eval::evaluate`) for no operator-facing benefit: an operator asking "is
  my target correct" only needs the answer, not which of two *other* things
  would also have been wrong. (Open question 1 asks whether this should be a
  debug-only opt-in instead of omitted entirely.)

  *Checked against the cited bugs, not asserted.* #98/#165 diverged as
  `target_vs_sql` specifically — #98's issue body reports
  `Diverged { target_vs_sql: [Cell { column: "rel_enrich", expected: None,
  got: Some("22") }] }`, i.e. the persisted target held a stale value where the
  `LEFT JOIN` oracle said `NULL`. The evaluator leg contributed nothing to
  catching it. #47 (aggregates never enforcing `REPLICA IDENTITY`, so deletes
  under-recompute and in-place updates double-count) and #65 (publication
  silently dropping CDC) are likewise pure persisted-vs-recompute divergences:
  the evaluator run from scratch would agree with the SQL recompute and both
  would disagree with the target. So the two-way check covers the whole cited
  class; the evaluator leg's distinct value is Rust-evaluator-vs-Postgres
  semantic drift, which is not what any of #98/#165/#168/#47/#65/#79 were.
  Dropping it loses no coverage of the bug class this ADR exists to serve.
- **Report shape**: a `SelfCheckReport` local to `trellis`, structurally like
  `generative::oracle::Divergence`/`ThreeWayReport` but simpler (one comparison
  leg, not three) — cell/missing-row/extra-row/missing-column/extra-column
  divergences, the checked-through LSN, rows actually compared, and whether the
  scan was truncated by a limit. `trellis` cannot import `generative`'s types
  (dependency runs the other way — `generative` depends on `trellis`), so this is
  a new, small type, not a re-export.
- **Audience**: a library method first (`Trellis::self_check`), matching the
  issue's own framing of "operator-facing audit" / "support engineer looking at a
  suspected drift report" and the house style of `quarantined`/`quarantine_status`/
  `sample_quarantined`. A CLI subcommand (`trellis self-check <target>`) is a thin
  wrapper over the same method, deferred to phase 4 below (issue's own phasing) —
  no reason to design a second surface now.

### 2. Read-only, but not casually cheap — sampling and scoping are mandatory, not optional

A full recompute scans the whole source and the whole target. On a large table
that is a real sequential-scan cost, not a free operation. v1 requires an
explicit bound on every call, mirroring `sample_quarantined`'s own
keyset-pagination shape (`after: Option<(pk, ...)>`, `limit: i64`) rather than
offering an unbounded "check everything" convenience method that an operator
could fire against a hot table by accident. This also directly serves the
issue's "support engineer looking at one suspect row" use case: a keyset cursor
lets a caller check a specific key range (or resume a partial sweep) without
paying for the whole table.

### 3. The quiescence contract — reuse stage 07's primitive, don't reinvent it

The issue names this the hardest part: the correctness promise is conditional on
"once caught up to a given LSN," and `self_check` must distinguish "diverged" from
"not caught up yet" or it produces false positives under live load.
`docs/staging-and-claiming/07-convergence-and-await.md` already owns exactly this
question, and the primitive is **real code today**, not a proposal:
`watermark_token`/`converged_through`/`await_converged` live in
`trellis/src/staging/converge.rs` (lines 259, 110, 275), re-exported at
`staging/mod.rs:70-72`, and are already used by
`generative/src/backend/{manual,subprocess}.rs` and `trellis/tests/converge.rs`.

Two corrections to an earlier draft of this section, because they change the
dependency story:

- There is **no** client-facing `await(LSN, timeout)` on `Trellis` today.
  `app.rs` exposes no converge/await method at all and never calls
  `staging::converge`. Closing that facade gap *is* issue #192, which is **open
  and unstarted** (no branch content, no PR). So `self_check` either depends on
  #192 landing or calls `staging::converge` crate-internally itself.
- ADR-0012 (#190, PR #213) is **merged**, and it demotes `defs`/`staging`/
  `intake` to `pub(crate)` while explicitly carving out
  `defs::oracle::{OracleError, recompute, recompute_aggregate, …}` for
  `generative`. `staging::converge` is in the same demotion, so the
  crate-internal route is the expected one.

`self_check` reuses the primitive rather than building a second "quiet window"
concept:

1. Take a watermark token (`pg_current_wal_lsn()`) before reading anything.
2. `await_converged` on that token (bounded by a timeout) — if it doesn't
   converge in time, `self_check` returns "not yet caught up," never a false
   divergence.
3. Read the persisted target and run the rendered `SELECT` **inside one
   `REPEATABLE READ` transaction**, so the two reads are not smeared across an
   arbitrary interval while the recompute scan runs.

**This combination is necessary but not sufficient, and the gap should be
settled before implementation rather than discovered in it.** Two distinct
races survive it:

- **`await_converged` → `BEGIN` gap.** Convergence is established at token time
  `T1`; the snapshot is taken at `T2 > T1`. Any source transaction committing in
  `(T1, T2)` is visible in the snapshot's *source* read but its CDC apply has
  not necessarily landed in the snapshot's *target* read. That is a false
  divergence, and it is exactly the failure mode step 3 was meant to remove —
  moved one step earlier rather than eliminated.
- **`REPEATABLE READ` freezes the wrong pair.** A single snapshot pins source
  and target at the *same* instant. But under live load the target legitimately
  lags the source by the CDC apply latency — that lag is the system working
  correctly, not drift. Freezing both at one instant therefore *guarantees* a
  false divergence for any source write in flight, rather than preventing one.
  Reordering doesn't rescue it: if the snapshot is taken first and
  `await_converged` runs inside it, the awaited applies are invisible to the
  already-frozen target read.

The generative suite's oracle sidesteps all of this because its harness truly
quiesces the workload before comparing — no writer is running. Production
`self_check` cannot assume that, and a snapshot alone does not substitute for it.
The realistic options, none free:

a. **Require a genuine quiet window** (the honest analogue of what the test
   harness does): document `self_check` as sound only when source writes to the
   audited tables are stopped, and have it report "not quiescent" otherwise
   (e.g. `pending_count` non-zero at both ends of the check).
b. **Re-check suspected divergences.** A real divergence is stable across
   repeated checks; a convergence race resolves. Report a cell as diverged only
   if it survives a second check after a fresh `await_converged`. Cheap,
   defensible, and it makes the check sound under load at the cost of latency
   on a dirty result.
c. **Bound the check to keys with no in-flight staged work** — intersect the
   audited key range with what `staging` reports as pending and exclude it.
   Precise but couples `self_check` to staging internals.

(b) is the recommended default, with (a) as the documented strong mode. This is
open question 8.

### 4. What can be shared between the (now three) renderers without weakening independence — reasoned explicitly, not assumed

The issue frames this as a binary (share nothing vs. share the printer and lose
the property). The actual boundary is narrower than either extreme:

**Safe to share — already shared, by precedent:**
- **The parser AST** (`trellis::defs::ast::TransformDef`/`Expr`/...).
  `generative::oracle` already takes this as its input rather than re-parsing
  independently, and its own design doc (`docs/generative-test-suite.md` §2)
  calls this fine: the AST is a *data representation* of what the definition
  says, not the *logic* that turns it into either incremental-maintenance code
  or a recompute query. A parser bug would show up identically everywhere that
  consumes the AST — sharing the AST doesn't create a blind spot that parsing
  twice independently would close, because nothing here re-derives the AST from
  raw text via two different parsers.
- **Registry *type* lookups for choosing a comparison strategy**
  (`registry::operator_spec`/`lookup_function` → does this field compare by
  decimal value or exact text?). `generative::oracle::field_value_type` already
  does this "rather than keeping its own separate copy of 'which operator/function
  returns which type' that could silently drift from the registry's" (its own doc
  comment). This is metadata about the grammar, not value-computation logic — a
  wrong registry entry is a validation-time bug, not something a self_check
  cross-check would ever be relied on to catch.
- **Trivial, semantics-free syntax helpers** like `quote_ident` (escape a `"` by
  doubling it). `generative::oracle` already duplicates `trellis::pool`'s
  crate-private `quote_ident` rather than importing it, and that's the right
  call to keep making: one line of escaping logic carries no interpretive
  judgment to get subtly wrong in a way worth cross-checking, so a shared copy
  costs nothing in independence and saves nothing meaningful in a third
  near-identical implementation either. Leave each renderer with its own.

**Must stay separately authored — this is the load-bearing part, and it is
*not* fully true today:**
- **The actual `SELECT` assembly** (`render_select`/`render_rel_select` and
  siblings) must stay separate across: `defs::eval::evaluate` (the production
  incremental-maintenance path), `trellis::defs::oracle`'s SQL renderer (what
  `self_check` productionizes), and `generative::oracle`'s SQL renderer. This
  holds today and must keep holding.
- **The leaf expression rendering does not hold today.** Per "What's already
  there" above, `render_expr_sql` is shared by the production 1-1 backfill and
  aggregate-apply paths, and `generative`'s `render_expr` is a transcription of
  it. So for expression-level rendering the intended three-way independence is
  currently one-way. `self_check` v1 built on this renderer therefore has a real
  blind spot: a 1-1 target's **backfilled, never-since-modified rows** were
  written by `render_expr_sql` and would be re-derived by `render_expr_sql`,
  so a rendering bug agrees with itself. Rows modified since backfill were
  written by the evaluator (`staging/apply.rs` uses `eval::evaluate`, not
  oracle SQL — verified), so those legs stay independent. Open question 7 asks
  whether to close this (give `self_check` its own expression renderer) or
  accept and document it.
- Whatever is decided, the linkage rule stands: `self_check` must not call into
  `generative::oracle`, and `generative` must not import `self_check`'s renderer. `self_check` must not call into
  `generative::oracle` (wrong dependency direction besides), and `generative`
  must not import `self_check`'s renderer — if it ever did, a bug in that shared
  code would be invisible to the very comparison meant to catch it, which is the
  issue's whole point.
- **The evaluator itself** stays out of `self_check`'s comparison authority
  entirely (§1) — sharing it would reduce `self_check` to exactly the "cheaper,
  much weaker" alternative the issue explicitly rejects.

**Making the duplication earn its keep, not just exist:** add a `generative`-crate
test (phase 2, below) that runs the *same* definitions through both
`trellis::defs::oracle`'s renderer (via `self_check`, or the function directly)
and `generative::oracle::sql_oracle`, and asserts they agree. This is exactly the
"fourth comparison" the issue itself suggests ("the generative suite gains a
fourth comparison that validates `self_check` itself"). It turns the duplication
from a one-time cost with only latent value into something CI exercises on every
run — a divergence between the two renderers for the same definition is caught
immediately as a real finding in one of them, not discovered by luck months
later.

### 5. Rollout / phasing

Refines the issue's own suggested phasing now that `trellis::defs::oracle`
already exists:

1. **This ADR.**
2. **`Trellis::self_check`, 1-1 targets, single-transform scope**, built on
   `trellis::defs::oracle`'s existing renderer (hardened: explicit
   quiescence/LSN contract per §3, mandatory limit/cursor per §2, a
   `SelfCheckReport` type, public API wrapper). Add the `generative`-crate
   cross-check test from §4.
3. **Extend to aggregate and relationship-enriched targets.** The renderers
   exist and are well exercised (heavily, by `trellis/tests/defs_oracle.rs`,
   `apply_aggregate.rs`, `defs_aggregate_relationship.rs` and others — more
   than "unit-tested against fixtures"), and assembling a target's live
   `HashMap<String, RelationshipDef>` from the catalog follows the pattern
   `defs/backfill.rs` already uses.

**Do not read phases 2-3 as "almost entirely the safety contract, not new
rendering logic."** Reading the renderers, the gap between a test oracle and a
production-callable audit is larger than "wiring," and understating it here is
the same mistake #190's original draft made with its "three-line fix":

- **No primary key in the projection.** `render_relationship_select_sql`
  (`oracle.rs:654`) and `render_aggregate_select_sql` (`:278`) emit only the
  calculated fields — no pk, no group key. Today's callers compare whole
  result sets; a `self_check` that reports "which key diverged" needs the key
  in the projection. That is a change to the renderers, not around them.
- **No scoping hook at all.** Both emit an unbounded
  `select … from <source> [joins] [group by …]` with no `WHERE`, `ORDER BY`, or
  `LIMIT` seam. §2 makes keyset scoping *mandatory*, so every renderer needs a
  bounding clause threaded through. For an **aggregate** target this is not a
  predicate push-down: a group's value depends on every source row in the
  group, so bounding the source scan changes the answer. Scoping an aggregate
  self-check has to bound the *group-key* space and then scan all source rows
  belonging to those groups — a genuinely different query shape than what
  `render_aggregate_select_sql` emits today. This is the single most
  underestimated item in the plan.
- **Panics on the unhappy path.** `oracle.rs` has ten `panic!`/`assert!`/
  `expect()` sites reachable from these renderers (unresolved relationship
  path, wrong `KeySpace`, unknown relationship name, non-substitutable
  definition). Acceptable in a test oracle; not acceptable in a method an
  operator can call against production. Converting these to a `Result` is
  mechanical but touches every function.
- **No column-level quarantine awareness.** `backfill.rs` consults
  `paused_columns_for` before rendering; `oracle.rs` has no notion of a paused
  column (zero references). A `self_check` that ignores ADR-0003's column-level
  quarantine will report a paused column as diverged on every run — a false
  positive on exactly the targets an operator is most likely to audit.

None of this argues against building on this renderer; it argues that phase 2
is "harden a real renderer into a production API," not "add a safety contract
to a finished one."
4. **CLI (`trellis self-check <target>`)**, alongside the Prometheus/status
   surfaces from epic #49 — e.g., "time since last self-check," "last self-check
   result" as an exported metric for whatever schedules it externally (open
   question 5).
5. **Wire as a shared end-of-test assertion across `trellis/tests/*.rs`** — the
   step that pays the regression dividend (turns each of the 42+ integration
   tests into a correctness test for free), gated on #172's fast lane per the
   issue.
6. **Public API surface** for the Ruby/Elixir clients (epic #140).

## Consequences

- Trellis gains a shipped answer to "is this target actually correct right now,"
  closing the gap `local_docs/transit-comparison.md` (if/when it exists) and the
  issue both flag: today the only way to know is inside the test suite.
- `trellis::defs::oracle` moves from an undocumented-audience "test/benchmark
  oracle" to a load-bearing part of the public API surface. Its doc comments
  ("test/benchmark oracle only") need updating as part of phase 2 so they don't
  mislead a future reader about who calls it. This interacts directly with
  **ADR-0012 (#190, PR #213), now merged**, which demotes `defs` to
  `pub(crate)` while carving out `defs::oracle::{OracleError, recompute,
  recompute_aggregate, …}` for `generative`. `self_check` should be a new `pub`
  surface that wraps these functions, rather than widening that carve-out or
  leaving them reachable by accident.
- `self_check` depends on issue **#192** (converge facade), which is open and
  unstarted. Phase 2 either waits on it or uses `staging::converge`
  crate-internally — worth deciding explicitly, since #192 also demotes
  `staging::converge` to `pub(crate)`.
- The two SQL renderers (`trellis::defs::oracle`'s and `generative::oracle`'s)
  remain permanently double-maintained. That's accepted, not incidental — see §4
  — and now has a concrete CI mechanism (the cross-check test) keeping the cost
  visible and the benefit real, rather than a "trust me, don't merge these"
  comment.
- `self_check` is not free to run casually: it is a real read load (§2), and its
  correctness depends on the quiescence contract in §3 actually being honored by
  every caller (a caller that skips `await_converged` and just fires the compare
  can get a false divergence under load — the API should make bypassing that
  contract awkward, not merely documented against).

## Alternatives Considered

**Build `self_check` directly on `generative::oracle`.** Rejected on dependency
direction alone: `generative` depends on `trellis`, not the reverse, so `trellis`
cannot import `generative`'s renderer without either restructuring the workspace
or introducing a cycle. Even ignoring that, it would be the same code checking
itself in production and in the property suite — exactly the shared-path risk the
issue is warning about, just inverted (the *test* oracle would no longer be
independent of what ships).

**Build `self_check` on the evaluator-driven `defs::oracle::recompute`.** The
issue's own rejected alternative, and this ADR agrees with its reasoning: cheaper,
but shares the evaluator with the incremental-maintenance path, so an evaluator
bug is invisible to it. `trellis/src/defs/oracle.rs`'s own doc comment already
calls this "a secondary cross-check, not the authority."

**Write a fresh fourth renderer from scratch**, treating `trellis::defs::oracle`'s
existing SQL renderer as off-limits too (on the theory that *any* pre-existing
code is suspect). Rejected **for the `SELECT`-assembly layer**: that layer is
independently authored from both the evaluator and from `generative::oracle`
(confirmed by reading both — see "What's already there"), so a fourth
implementation of it would roughly double phase 2's cost for no safety gain.

Rejected only **partially for the leaf expression renderer**, because the
premise turns out not to hold there: `render_expr_sql` is shared with production
backfill/aggregate-apply, so re-deriving it is not duplicating an already-
independent thing — it is creating the independence §4 assumes. It is also a
~35-line `match`, not a meaningful share of phase 2. See open question 7.

## Open Questions for @mmmries

This ADR is a proposal for sign-off, not a final word — flagging the calls that
need a decision from the repo owner rather than assuming one:

1. **Should the evaluator leg be entirely omitted from production `self_check`
   (as proposed in §1), or offered as a debug-only opt-in** (e.g. `--deep` on the
   CLI, or a `SelfCheckOptions` flag) for a support engineer specifically chasing
   a suspected evaluator-drift bug rather than a pipeline/apply bug? It's "free"
   in the sense that `defs::oracle::recompute` already exists, but it doubles the
   read cost and exposes another internal module through the public surface.
2. **Is there a hard cap on unscoped/default `limit`, or does v1 simply refuse to
   run without an explicit one?** §2 proposes mandatory scoping; whether that
   means "no default, caller must always pass a limit" or "a conservative default
   that can be raised" is a product call about how forgiving the API should be
   for the common ad hoc case.
3. **How should "diverged" vs. "not yet caught up" surface to a caller** — a
   distinct error variant (e.g. `TrellisError::NotConverged`) that a caller must
   handle separately from a real `SelfCheckReport`, or a `caught_up: bool` field
   embedded in the report alongside any divergences? This shapes how a monitoring
   integration is written against it, and is worth deciding before the API is
   public rather than after.
4. **Should the phase-2 generative-crate cross-check (§4) run on every
   `generative` invocation, or only as a periodic/nightly meta-test?** Running it
   every time gives continuous drift protection between the two renderers but
   adds cost to the fast-running property suite; running it rarely weakens the
   guarantee the duplication is supposed to buy.
5. **Does `self_check` need any persisted history of its own** (a table recording
   past runs and outcomes), or is it purely a synchronous, callable primitive with
   history left entirely to whatever schedules it (a cron job, an operator
   script, the Prometheus counters from phase 4)? This decides whether phase 2
   needs any new DDL at all, or is Rust-only.
6. **Does `self_check`'s target addressing reuse `QuarantineTarget`'s
   `"transform"`/`"transform.column"` string parsing** (consistent with the rest
   of the operator-facing API, per ADR-0003's amendment) **or does a self-check
   scope need a richer selector** (a key range, a specific pk list, a cursor) that
   doesn't fit that string shape and wants its own type from the start?
7. **The expression-renderer blind spot (§4).** `render_expr_sql` is shared with
   the production 1-1 backfill and aggregate-apply paths, and `generative`'s
   `render_expr` is a transcription of it — so there is effectively one
   expression-rendering implementation across all three sites. Options:
   (a) accept and document it, on the grounds that the bug class #174 targets is
   pipeline staleness rather than expression rendering (this ADR's working
   assumption); (b) give `self_check` its own leaf expression renderer, which is
   a small function — the one place where the rejected "write a fourth renderer"
   alternative might actually be worth it, precisely because it's cheap here;
   (c) rewrite `generative`'s `render_expr` as a genuinely independent
   derivation. (b) is cheap and closes the production-vs-audit half; (c) is what
   the generative suite's own design doc already implies it should be.
8. **Which quiescence strategy (§3)?** `await_converged` + `REPEATABLE READ` is
   not sufficient on its own — the await→snapshot gap, and the fact that a
   single snapshot freezes source and target at an instant where the target
   legitimately lags. §3 proposes re-checking suspected divergences (option b)
   as the default with a documented quiet-window strong mode (option a). This
   needs a decision before phase 2, because it shapes the API (does
   `self_check` take a retry budget? does it report "not quiescent" as a third
   outcome alongside converged/diverged, interacting with open question 3?).
