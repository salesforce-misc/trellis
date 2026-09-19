---
status: proposed
date: 2026-09-19
deciders: Michael Ries
consulted:
informed:
---

# Curate the Public API Surface; Demote `defs`/`staging`/`intake` to `pub(crate)`

Issue #82 / [ADR-0008](0008-public-api-design.md) set out a "deep interface": `Trellis`
/ `Client` / `BlockingTrellis` as the only front doors, funneling into shared internal
engines. An audit driven by #186/#187 found the crate violates this by construction:
`defs`, `staging`, and `intake` are all `pub mod` in `trellis/src/lib.rs`, and nearly
every submodule beneath them is also `pub mod` with its own `pub use` re-export list —
so the entire internal engine is externally reachable, not just the curated set at the
crate root. #189 is the epic tracking the fix; #190 (T1) is the module-visibility
demotion itself, and asked for this short design note first, "since it drives
everything else." This ADR is that note — a proposal for #190's sign-off, not a final
word on #191–#194's open product questions.

Grounded against the crate at commit `98f9672` (current `main`), not the epic's issue
text alone — the epic was filed from an audit and `main` has moved since (`test-util`
landed via #166 after the audit, for one).

## Current state

`lib.rs`'s own module doc already describes two intentional tiers, and gets them right:

1. **The facade**: `app::Trellis`, `client::Client`, `blocking::BlockingTrellis` — "one
   facade covering the whole lifecycle... [`blocking::BlockingTrellis`] wraps the same
   facade... Everything else in the crate is machinery these compose."
2. **Composable primitives** an embedder is deliberately allowed to reach directly:
   `config` (`Config`), `pool` (`Pool`), `migrate`, `identity` (`Identity`), `metrics`,
   `error`/`error_code` (`Error`/`ErrorCode`), `numeric` (`Numeric`), and `otel` (behind
   the `otlp` feature). The crate-root `pub use` block re-exports exactly the curated
   set from tiers 1 and 2 today — `Trellis`, `Client`, `BlockingTrellis`, `Config`,
   `Definition`, `TransformStatus`, `RelationshipCardinality`, `RelationshipDefinition`,
   the summary/quarantine types (`DefinitionSummary`, `RelationshipSummary`,
   `PoisonEntry`, `PoisonSample`, `QuarantineEntry`, `QuarantineState`,
   `QuarantineTarget`), the option structs (`TrellisOptions`, `ClientOptions`), the
   error types (`TrellisError`, `ClientError`, `Error`, `ErrorCode`), `Identity`,
   `migrate`, `Numeric`, `Pool`. This list is already correct and needs no change.
   (`metrics`, `config`, `pool`, `identity`, `error`, `error_code`, `numeric`, and
   `otel` are tier-2 *modules* — `pub mod` at the crate root — not entries in the
   `pub use` block; only the named types above are re-exported flat.)

The violation is the third tier that was never supposed to exist as public:
**`defs`, `staging`, `intake`** — the engine. Today:

- `defs/mod.rs`: 13 `pub mod` submodules (`ast`, `backfill`, `catalog`, `chunk_queue`,
  `ddl`, `error`, `eval`, `invertibility`, `model`, `oracle`, `pg_type`, `registry`,
  `validate`), each with its own `pub use` re-export list at the `defs` level.
- `staging/mod.rs`: 14 `pub mod` submodules (`append`, `apply`, `apply_aggregate`,
  `claim`, `converge`, `error`, `fold`, `liveness`, `quarantine`, `retire`, `seal`,
  `session`, `state`, `watermark`), same pattern.
- `intake/mod.rs`: 5 `pub mod` submodules (`error`, `pgoutput`, `publication`,
  `replica_identity`, `spill`).
- One further nested `pub mod` beyond the 32 above: `defs::registry::precedence`
  (`defs/registry.rs`), for **33** total.
- Across the three: **~300** `pub fn`/`pub struct`/`pub enum`/`pub const`/`pub async fn`
  items (`grep -rE '^\s*pub (fn|struct|enum|const|async fn) ' trellis/src/{defs,staging,intake}`
  gives 303 at `98f9672`: 160 in `defs`, 104 in `staging`, 39 in `intake`; the count
  drops to 214 if `pub async fn` is excluded, which is probably where the epic-era
  figure of 257 came from — treat the number as "order of 300," not exact). None of
  them gated, all reachable today as `trellis::staging::seal::seal_phase1`,
  `trellis::defs::oracle::recompute`, `trellis::defs::registry::FUNCTIONS`, etc. —
  regardless of what the crate root re-exports.

Because `defs`/`staging`/`intake` are themselves `pub mod` at the crate root, the
curated re-export list at the top doesn't actually curate anything for an external
crate willing to name the raw path — it's a suggestion, not a boundary.

## Decision 1: the curated surface — confirm the epic's proposal, with corrections

The epic (#189) proposes: `Trellis`, `Client`, `BlockingTrellis`, `Config`,
`Definition`, status/enum types (e.g. `TransformStatus`), and error types, "everything
else `pub(crate)`."

**Confirmed as-is**, and it matches what's already re-exported at the crate root today
(see "Current state" above) — no crate-root re-export needs to change.

**Corrections, from what's actually in the crate:**

1. **The epic's list undercounts tier 2.** `Pool`, `migrate`, `Identity`, `metrics`,
   `Numeric`, `ErrorCode`, `Error` aren't in the epic's enumerated list, but they're
   already public, already re-exported at the crate root, and `lib.rs`'s own doc
   comment describes them as deliberate composable primitives, not incidental leaks.
   None of them are `defs`/`staging`/`intake`. **Recommendation: this second tier stays
   public as-is; T1's scope is only the third tier (the engine).** Worth saying
   explicitly so nobody reads "curate the surface" as license to also narrow `Pool` or
   `migrate`.

2. **Three lines seal the surface, but they do not compile.** Rust's visibility model
   makes an item's effective external visibility the *minimum* of its own declared
   visibility and every ancestor module's, so changing `lib.rs`'s `pub mod defs` /
   `pub mod staging` / `pub mod intake` to `pub(crate) mod` is sufficient to *seal* the
   entire subtree — every inner `pub mod` submodule and every inner `pub use` re-export
   list included — without editing visibility on a single file inside `defs/`,
   `staging/`, or `intake/`.

   **But it is not sufficient to keep the build green, and this is the single largest
   piece of mechanical work T1 actually carries.** Applied literally to `98f9672`
   (verified: `cargo clippy -p trellis --all-features` is clean before, and emits
   **76 warnings** after), the three-line edit produces:

   - **~55 `dead_code` lints.** Every engine item whose only callers live in
     `benchmark`, `generative`, or `trellis/tests/*.rs` becomes, from the compiler's
     point of view, an unreachable crate-private item with no in-crate caller. Examples
     across all three modules: `defs::eval::{evaluate, evaluate_with_relationships}`,
     the whole `defs::oracle` render/recompute family (14 items), `staging::converge`'s
     five functions, `staging::liveness`'s pause-lease family, `staging::apply::drain_once`,
     `staging::seal::{fenced_rows, SealOutcome}`'s unread fields,
     `staging::quarantine::{release_key, halting_stop_stats, HaltingStopStats}`,
     `intake::replica_identity::needs_old_image`, `intake::publication::record_backfill_coverage`.
   - **21 `unused_imports` lints** — the `pub use` re-export blocks in `defs/mod.rs`,
     `staging/mod.rs`, and `intake/mod.rs` stop being re-exports (nothing outside the
     crate can reach them) and become plain unused imports.

   CI runs `cargo clippy --all-targets --all-features -- -D warnings`
   (`.github/workflows/ci.yml`), so **all 76 are hard build failures.** Decision 2's
   `test_util` module absorbs the subset it re-exports (it is `cfg`-gated on
   `feature = "test-util"`, which `--all-features` turns on), but it does not cover the
   residue — items with genuinely *no* caller anywhere in the workspace, which the
   `pub mod` was masking as "someone external might use this." Those are a real
   discovery, not noise: T1 must triage each into delete / `#[allow(dead_code)]` with a
   justification / add to the `test_util` surface. **Budget T1 as a real sweep with a
   triage pass, not a three-line PR.** (Noted here as a design fact for whoever does
   T1's mechanical work; this ADR itself makes no `.rs` changes.)

3. **`migrate::migrate` (flagged by #194) is not a leak.** It's already re-exported at
   the crate root, `Trellis::migrate()`/`BlockingTrellis::migrate()` delegate to it by
   design, and `cli`'s production path goes through the `Trellis` wrapper, not the free
   function directly — the free function's only non-wrapper caller is
   `testkit/src/cluster.rs`'s shared test bootstrap, a legitimate embedder-shaped use
   (composable-primitive access, same as any embedder could do). **Recommendation:
   confirm it stays public; #194 just needs to fix the doc language that made it look
   like a mismatch, not touch the visibility.**

4. **`app::qualified_source_tables` (also #194) is a genuine leak**, and its own doc
   comment already says so ("isn't part of `Trellis`'s public surface") while
   `pub mod app` makes it reachable anyway. It sits in `app.rs` — a tier-1 (facade)
   file, not `defs`/`staging`/`intake` — so it's technically outside T1's literal
   `defs`/`staging`/`intake` scope, but it's the same kind of problem and should be
   fixed in the same pass. Its only real caller is `trellis/tests/app.rs`, which makes
   it belong in Decision 2's test-only surface, not tier 1 or 2.

## Decision 2: a dev-only `test-util` surface, sized to actual usage

The epic proposes moving `benchmark`/`generative`/`testkit`'s internal access "behind
a dev-only / `test-util` cargo feature." A `test-util` Cargo feature already exists
(`trellis/Cargo.toml`, landed by #166) — but today it only gates one private function
body (`staging::apply::pause_before_commit_for_tests`, called nowhere externally). It
was never sized for visibility gating. This decision widens its scope rather than
inventing a second feature.

**Correction to the epic: `testkit` doesn't reach into internals today.** Its only
`trellis::` usage is `Config`, `Pool`, `migrate` (four references total, all in
`testkit/src/cluster.rs` and `testkit/src/fixtures.rs`; `testkit/tests/harness.rs` has
zero) — all already crate-root public. It names nothing under `defs`/`staging`/`intake`
in code or comments. It needs nothing from this decision. (Worth a line in the doc so a future contributor
doesn't add a feature dependency `testkit` doesn't need.)

**What `benchmark` and `generative` actually import today** (grepped from `use
trellis::...` lines, not the epic's guess):

- `defs::ast::*` — `Expr`, `FieldDef`, `GroupByKey`, `KeySpace`, `Operator`,
  `Predicate`, `TransformDef`, `ValueType` (essentially the whole AST module;
  `generative` builds and inspects `TransformDef`s directly for its generator/oracle).
- `defs::{CatalogError, DdlError, create_relationship, create_definition_without_backfill,
  install_definition, parse, qualified_target_table, require_single_column_pk,
  source_primary_key, create_target_table, create_aggregate_target_table,
  backfill_definition}`.
- `defs::oracle::{OracleError, recompute, recompute_aggregate,
  render_aggregate_select_sql}`.
- `defs::registry` as a module (`registry::FUNCTIONS`, `AGGREGATE_FUNCTIONS`,
  `AGGREGATE_FUNCTION_SPECS`, `operator_spec`, `lookup_function`, `OPERATORS` — used via
  `use trellis::defs::registry;` then `registry::X`, not individual re-exports).
- `staging::{StagingError, await_converged, has_pending, watermark_token, seal_phase1,
  seal_phase2}`.

Two items the list above missed on first pass, both real:
`defs::invertibility::{AggregateArg, CountArg, Invertibility, classify}`
(`generative/tests/coverage.rs:19-20`) and `config::DEFAULT_SCHEMA` (`benchmark` and
both `generative` backends — tier 2, not in scope, but worth noting it is *not* one of
the crate-root flat re-exports, so it is reached as `trellis::config::DEFAULT_SCHEMA`).
`watermark_token` is notable as the one `staging` item with **no** in-crate test caller
at all — `generative` is its only consumer workspace-wide.

**What only `trellis`'s own `tests/*.rs` need** (no `benchmark`/`generative` caller):
`defs::eval::{evaluate, evaluate_aggregate, evaluate_with_relationships}` (bare,
non-`_excluding` variants — #193's own audit already found their only callers are the
in-crate oracle and test files), `app::qualified_source_tables` (Decision 1,
correction 4), `staging::converge::{converged_through, pending_count}`,
`staging::{retire_drained_segments, StagedWatermark, RING_SIZE}`,
`staging::seal::SealOutcome`, `staging::apply::drain_once`,
`staging::liveness`'s pause-lease family (see #191), and — the one the module-level
framing above hid — **`intake::publication`**, used as a module by
`trellis/tests/backfill_coverage.rs:21` alongside `intake_core.rs` /
`intake_robustness.rs`. Decision 2's draft surface listed nothing from `intake` at all;
that was wrong.

**Sizing correction.** `trellis/tests/` alone contains ~125 `trellis::defs`, ~110
`trellis::staging`, and 8 `trellis::intake` references across 53 files (`seal_phase1`
in 28 files, `has_pending` in 17). The "around 25 symbols" figure below is the
*cross-crate* group only; the full `test_util` surface once `trellis`'s own
integration tests are included is materially larger — plan for 50–70 symbols and
re-derive it mechanically at PR time rather than trusting either number here.

**Recommendation:** don't re-`pub mod` any of `defs`/`staging`/`intake` behind the
feature — that reproduces the same over-exposure problem one flag away. Instead, add a
single curated re-export surface at the crate root, gated the same way the existing
hook is:

```rust
#[cfg(any(test, feature = "test-util"))]
pub mod test_util {
    //! Re-exports for `benchmark`, `generative`, and this crate's own
    //! `tests/` integration suite. Not part of the public API; anything
    //! reached only through here can change without a semver bump.
    pub use crate::app::qualified_source_tables;
    pub use crate::defs::ast::{
        Expr, FieldDef, GroupByKey, KeySpace, Operator, Predicate, TransformDef, ValueType,
    };
    pub use crate::defs::oracle::{
        recompute, recompute_aggregate, render_aggregate_select_sql, OracleError,
    };
    pub use crate::defs::registry;
    pub use crate::defs::{
        backfill_definition, create_aggregate_target_table, create_definition_without_backfill,
        create_relationship, create_target_table, evaluate, evaluate_with_relationships,
        install_definition, parse, qualified_target_table, require_single_column_pk,
        source_primary_key, CatalogError, DdlError,
    };
    pub use crate::staging::{
        await_converged, has_pending, seal_phase1, seal_phase2, watermark_token, StagingError,
    };
}
```

(Illustrative, not literal — the actual demotion PR should re-derive the list from
call sites at that time, and confirm nothing above changed shape.) `create_definition`
(the internal ring-enumeration fallback, per #187) is deliberately *not* here: #187
already resolved it to `pub(crate)`-only, no cross-crate caller.

**A structural risk to flag, not silently work around:** `pub(crate)` does not reach
across two boundaries that matter here — sibling workspace crates (`benchmark`,
`generative`) *and* this crate's own `tests/*.rs` integration tests, which Cargo
compiles as separate crates linked against the library's normal (non-`cfg(test)`)
build. That's exactly why the existing `test-util` feature's doc comment already notes
`cfg(any(test, feature = "test-util"))` "means every `trellis` unit test build gets it
for free" — deliberately scoped to *unit* tests, because integration tests don't get
`cfg(test)` for free the way unit tests do. Once `seal_phase1`/`seal_phase2` and the
rest move behind this gate, `trellis`'s own `tests/*.rs` (dozens of files call
`seal_phase1`/`seal_phase2` directly) need the feature active to link at all. `cargo
test -p trellis` with no flags would start failing to compile unless `trellis` adds a
self `[dev-dependencies]` entry enabling its own `test-util` feature — the same
self-dependency pattern `generative`'s own `Cargo.toml` already uses
(`generative = { path = ".", features = ["proptest", "subprocess-backend"] }`) to turn
a feature on for its own test binaries without leaking it workspace-wide. This is a
real mechanical requirement of Decision 2, not a hypothetical — flagged as Open
Question 2 below since it's a small but consequential build-graph choice.

**A second, harder build-graph problem this ADR cannot wave through:
`benchmark` and `generative` name these items from their own `src/`, not from
`tests/`.** `benchmark/src/scenario.rs` calls `defs::parse`, `create_target_table`,
`backfill_definition` etc.; `generative/src/backend/manual.rs` calls
`staging::seal_phase1`/`seal_phase2`/`await_converged`, and `generative/src/oracle/mod.rs`
uses `defs::registry` and `defs::oracle::recompute`. Both crates take `trellis` as a
**normal** (non-dev) dependency (`benchmark/Cargo.toml:7`, `generative/Cargo.toml:16`).
So the self-dev-dependency trick does *not* transfer: a self dev-dep only enables a
feature for a crate's own test targets, and these are library/binary call sites.
Enabling `trellis/test-util` on a normal dependency edge is precisely what
`trellis/Cargo.toml`'s own feature comment forbids — "Cargo unifies features across a
whole workspace build, so any crate that names this feature on a **normal** (non-dev)
`trellis` dependency also switches it on for `cli`'s production binary under
`cargo build --workspace`." `generative` escapes this today only because
`backend::subprocess` "compiles either way (it drives the hook purely through env vars,
and never names a gated item)" — visibility gating is the opposite case: the call sites
*do* name the gated items and won't compile without the feature.

**There is no free lunch here; T1 must pick one and say so:**

1. Make the engine access in `benchmark`/`generative` *itself* feature-gated inside
   those crates, so their default (feature-off) build doesn't name gated items — large
   and invasive, since it's the core of what both crates do.
2. Move the offending code out of `src/` into `tests/`/`benches/` targets that a self
   dev-dep can cover — plausible for `generative`'s backends, implausible for
   `benchmark`.
3. Accept a normal `trellis = { features = ["test-util"] }` edge and *split* the
   feature: a new, dependency-free `internal-api` feature for pure visibility
   widening (safe to unify into `cli`, since it only re-exports existing symbols and
   changes no runtime behaviour), keeping the behaviour-changing pause hook on the
   existing `test-util`. This preserves #166's invariant — the one that actually
   matters is "no pause hook in the production binary," not "no feature at all" — and
   is the only option that doesn't require restructuring a whole crate.
4. Don't gate at all: keep a plain, ungated, clearly-documented
   `pub mod internal_api` at the crate root. Weaker (it *is* public API, semver-wise)
   but honest, and still a ~25-symbol curated list rather than ~300.

This ADR leans toward **(3)**, but it is a genuine decision with a workspace-wide
blast radius and belongs in the Open Questions, not in a parenthetical. See Open
Question 6.

## Decision 3: sequencing with T2–T5

- **#191 (T2, `staging::liveness` pause-lease fate)** is a product decision this ADR
  doesn't make (see Open Questions). It matters for sequencing because
  `acquire_pause_lease`/`heartbeat_pause_lease`/`release_pause_lease`/
  `claiming_is_paused`/`claim_unless_paused`/`heartbeat_inline` are reachable today only
  via `pub mod staging::liveness`, with `trellis/tests/liveness.rs` as the sole caller.
  **Recommendation:** decide #191 in parallel with this ADR's sign-off — it's small and
  self-contained. If "delete," T1 has one fewer thing to carry into Decision 2's
  surface. If "integrate," the new `Trellis`/`Client` pause/resume methods should land
  *before* `staging::liveness` is demoted (mirroring #192's own stated order below), so
  there's no window where the subsystem is both wired-nowhere and inaccessible to its
  only test.

- **#192 (T3, `converge` read-your-writes facade gap)** must land its new
  `Trellis`/`BlockingTrellis` await-converged method *before* `staging::converge` is
  demoted — #192 says so directly ("Add... Then demote... coordinate with T1"). Until
  it lands, `converge`'s items (`converged_through`, `has_pending`, `pending_count`,
  `watermark_token`, `await_converged`) belong in Decision 2's test-util surface as an
  interim measure, since `generative`'s fuzz harness already depends on
  `await_converged`/`watermark_token`/`has_pending` raw. Once #192 ships the facade
  method, whether `generative` switches to it or keeps the raw primitive is a call for
  whoever owns #192 (a fuzz harness legitimately wants the unabstracted primitive) —
  flagged, not decided, here.

- **#193 (T4, test-only public leaks)** is almost entirely mechanical fallout of
  Decision 2: the seal two-phase primitives and the `defs::oracle` render/recompute
  family are exactly the cross-crate group already enumerated above. The one piece of
  #193 that isn't mechanical — renaming the `eval` `_excluding` variants now that the
  bare names are test-only — has no visibility implication and can ride in the same PR
  as the demotion (it touches the same call sites) without blocking on anything here.

- **#194 (T5, doc/visibility mismatches)**: `migrate::migrate` resolves via Decision
  1's correction 3 — doc fix only, no visibility change. `app::qualified_source_tables`
  needs the same test-util treatment as Decision 2's group even though `app.rs` isn't a
  `defs`/`staging`/`intake` file — called out explicitly so it isn't dropped for
  sitting outside T1's literal module list.

**Recommended order:**

1. This ADR's sign-off (open questions below).
2. #191's integrate-or-delete decision, in parallel — doesn't block anything else.
3. #192's facade addition, landed before any `staging::converge` visibility change.
4. Resolve Open Question 5 (how `benchmark`/`generative` get the gated surface) —
   this blocks step 5 and may change its shape.
5. The T1 demotion itself: `lib.rs`'s three `pub mod` → `pub(crate) mod`, the
   `test_util` re-export module from Decision 2 (including the interim `converge`
   entries, the `intake::publication` entry, and `app::qualified_source_tables`),
   the `lib.rs` module-doc rewrite, the ~55-item dead-code triage, the
   `Cargo.toml` feature/dependency changes, and whichever of #191's outcome it needs
   to carry — done as one coordinated visibility edit, since half-demoting
   `defs`/`staging`/`intake` isn't meaningfully safer than doing it in one pass. This
   is a substantial PR, not a three-line one; expect to re-derive the surface by
   iterating `cargo clippy --all-targets --all-features -- -D warnings` to zero.
6. #193's `eval` rename, same PR as step 4 or an immediate follow-up.
7. #194's `migrate` doc-language fix — trivial, any time, no ordering dependency.

## Consequences

- ~300 `pub` items and 33 `pub mod` submodule declarations across `defs`/`staging`/
  `intake` become `pub(crate)`; the crate-root public surface stays at roughly its
  current ~20 items, plus a new, explicit, dev-only re-export list (Decision 2) sized
  to what `benchmark`/`generative`/the crate's own integration tests actually call —
  around 25 symbols today, not a rubber-stamped whole-module re-exposure.
- `benchmark` and `generative` gain one explicit dependency edge (on whichever
  feature Open Question 5 settles on) in place of implicit reach through `pub mod`;
  note this edge is a **normal**, not dev, dependency for both — see Decision 2's
  second build-graph problem for why that matters. A future
  addition is a one-line addition to `test_util`'s re-export list, which is
  self-documenting in a way "everything's already public" never was.
- `trellis`'s own `cargo test` needs the self-dev-dependency change described in
  Decision 2 to keep working unmodified — a one-time build-graph fix, not a per-test
  cost.
- **`lib.rs`'s own module doc comment needs rewriting in the same PR.** It links
  `[`defs`]`, `[`staging`]`, `[`intake`]`, `[`staging::RING_SIZE`]`,
  `[`staging::retire_drained_segments`]`, and `[`staging::seal_if_active_nonempty`]`
  from public documentation. Once those are `pub(crate)`, rustdoc's warn-by-default
  `private_intra_doc_links` fires on every one, and the tiers the doc describes stop
  matching the surface it documents.
- **Public error types become unnameable from outside the crate.** `TrellisError`
  wraps `ParseError`/`CatalogError`/`ApplyError`; `ClientError` wraps
  `StagingError`/`IntakeError`/`ApplyError`. These stay `pub` inside `pub(crate)`
  modules, so matching on the variants still works, but an embedder can no longer
  *name* the payload type (no `fn handle(e: CatalogError)`, no `impl From<...>`), and
  rustdoc renders them as unlinkable. `unnameable_types` is allow-by-default so this
  won't break CI — it is an API-design call, not a build error. T1 should either
  re-export these six error types at the crate root or state deliberately that they
  are opaque.
- No `.rs` files change as part of this ADR; it hands the mechanical work a concrete
  list to execute rather than a restatement of the epic's already-known problem.

## Open questions for @mmmries

1. **#191's product call** (integrate `staging::liveness`'s pause-lease gate into the
   real drain path + expose pause/resume on the facade, or delete it as dead code) —
   this ADR assumes an answer exists before the mechanical T1 work lands, but doesn't
   make the call itself.
2. **Self `[dev-dependencies]` for `trellis` on its own `test-util` feature** (Decision
   2) — acceptable, or is a `--features test-util` flag on the CI/local test
   invocation (e.g. via `.cargo/config.toml` aliasing) preferred instead? Both work;
   they trade off "works with bare `cargo test`" against "one more explicit thing in
   the crate's own dependency graph."
3. **Naming collision risk:** Decision 2 proposes a `test_util` *module* gated behind
   the existing `test-util` Cargo *feature* — same name, different namespace. Confirm
   that's not confusing before it's load-bearing across dozens of call sites in three
   crates, or suggest a different module name (`internal_test_support`, `for_tests`,
   etc.).
4. **`generative`'s long-term relationship to `staging::converge`'s raw primitives**
   once #192 ships a facade method (Decision 3) — migrate the fuzz harness to the
   facade, or keep it on the raw primitive permanently as an intentionally low-level
   test tool? Not urgent, but worth an explicit answer whenever #192 lands rather than
   leaving it implicit.
5. **How should `benchmark`/`generative` acquire the gated surface** given they name
   it from `src/`, not `tests/` (Decision 2's second build-graph problem)? Options 1–4
   there; this ADR leans toward (3), a dependency-free `internal-api` feature split
   out from the behaviour-changing `test-util`. This is the one open question that can
   change T1's shape rather than just its details.
6. **Dead-code triage policy.** The demotion surfaces ~55 `dead_code` lints, a real
   subset of which have no caller anywhere in the workspace (e.g.
   `staging::quarantine::{release_key, halting_stop_stats}`, `staging::seal::fenced_rows`,
   `staging::session::ProducerSession::append`, `staging::watermark::StagedWatermark::at`,
   `intake::spill`'s `is_empty`). Default to deleting them, or default to
   `#[allow(dead_code)]` with a "reserved for X" note?
7. **Decision 1's tier-2 scope** (confirming `Pool`, `migrate`, `Identity`, `metrics`,
   `Numeric`, `ErrorCode`, `Error` all stay public as-is) is inferred from `lib.rs`'s
   own doc comment, not from a prior explicit sign-off anywhere in the issue tracker —
   worth a direct confirmation that this inference is correct before it's treated as
   settled.

## Related issues

#82 / [ADR-0008](0008-public-api-design.md) (the original deep-interface design),
#186 / #177 (the audit that surfaced the violation), #189 (epic), #190 (T1, this
ADR), #187 (definition-creation cluster, subsumed by #190), #191 (T2, liveness fate),
#192 (T3, converge facade gap), #193 (T4, test-only leaks), #194 (T5, doc/visibility
mismatches).
