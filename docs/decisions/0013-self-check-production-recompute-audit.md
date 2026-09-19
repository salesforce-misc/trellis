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
mention: **a second, already-independent SQL-rendering oracle already exists inside
the `trellis` crate itself**, separate from `generative`'s. That finding drives most
of the sizing and independence reasoning below.

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
`pub mod defs` in `trellis/src/defs.rs` — reachable, but used by only one
integration test today (`trellis/tests/client_e2e.rs`). It is not called from
anywhere on the production apply/staging path.

So the codebase already has **two separately-authored, evaluator-independent SQL
renderers** — `trellis::defs::oracle`'s and `generative::oracle`'s — that happen
to live in different crates and were never plumbed together. `self_check` does not
need to be invented from nothing; it needs `trellis::defs::oracle`'s existing
renderer **hardened and exposed**, not a from-scratch port of `generative`'s. This
materially shrinks the "how big is this" estimate in the issue's own phasing.

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
question — `watermark_token`/`converged_through`/`await_converged` is the
primitive the client-facing `await(LSN, timeout)` already uses. `self_check`
reuses it rather than building a second "quiet window" concept:

1. Take a watermark token (`pg_current_wal_lsn()`) before reading anything.
2. `await_converged` on that token (bounded by a timeout) — if it doesn't
   converge in time, `self_check` returns "not yet caught up," never a false
   divergence.
3. Once converged, read the persisted target and run the rendered `SELECT`
   **inside one `REPEATABLE READ` transaction**, so both reads observe the same
   snapshot even if source writes continue to land during the query. Without
   this, a write landing between the two reads (persisted target already
   reflects it, but the recompute `SELECT` reads a source row mid-flight, or vice
   versa) would look like a divergence that never really existed — the same
   failure mode the issue is worried about, just moved one step later. The
   generative suite's oracle gets this for free today because its harness
   quiesces the whole workload before comparing; production `self_check` cannot
   assume that, so it has to earn point-in-time consistency explicitly.

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

**Must stay separately authored — this is the load-bearing part:**
- **The actual expression/`SELECT` rendering** (`render_expr`/`render_select` and
  siblings) in all three places that have it today or will:
  `defs::eval::evaluate` (the production incremental-maintenance path),
  `trellis::defs::oracle`'s SQL renderer (what `self_check` productionizes), and
  `generative::oracle`'s SQL renderer. This is precisely the logic whose bugs
  the whole mechanism exists to catch. `self_check` must not call into
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
   cross-check test from §4. Smaller than a port because the renderer already
   exists and is already unit-tested; the new work is almost entirely the
   safety contract and the API surface, not new rendering logic.
3. **Extend to aggregate and relationship-enriched targets.** Mostly wiring:
   `trellis::defs::oracle::render_aggregate_select_sql`/
   `render_relationship_select_sql`/`render_aggregate_relationship_select_sql`
   already exist and are already unit-tested against fixture definitions; this
   phase is assembling a target's live `HashMap<String, RelationshipDef>` from
   the catalog (the pattern `defs::backfill.rs` already uses) and threading it
   through, not writing new SQL generation.
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
  mislead a future reader about who calls it. This also interacts with the
  in-flight ADR-0012 (PR #213, not yet merged): if `defs` is demoted to
  `pub(crate)`, `defs::oracle`'s rendering functions need an explicit carve-out
  (or a move to a new `pub` module `self_check` re-exporting/wrapping them)
  rather than staying reachable only by accident of `defs` being `pub`.
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
code is suspect). Rejected: `trellis::defs::oracle`'s renderer is already
independently authored from both the evaluator and from `generative::oracle`
(confirmed by reading both — see "What's already there" above), so a fourth
implementation would add authorship-independence between two things that don't
need it (production's target vs. `self_check`'s own past self) while doing
nothing to strengthen the one relationship that matters (production's target vs.
an authority independent of the code that wrote it). It would also roughly
double phase 2's cost for no corresponding safety gain.

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
