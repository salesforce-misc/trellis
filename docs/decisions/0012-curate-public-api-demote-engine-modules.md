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
   `QuarantineTarget`), the error types (`TrellisError`, `ClientError`, `Error`,
   `ErrorCode`), `Identity`, `migrate`, `Numeric`, `Pool`. This list is already correct
   and needs no change.

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
- Across the three: **257** `pub fn`/`pub struct`/`pub enum`/`pub const` items, none of
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

2. **The mechanical fix is three lines, not a file-by-file sweep.** Because the
   crate-root `pub use` block already re-exports exactly the intended items, and Rust's
   visibility model makes an item's effective external visibility the *minimum* of its
   own declared visibility and every ancestor module's, changing `lib.rs`'s
   `pub mod defs` / `pub mod staging` / `pub mod intake` to `pub(crate) mod` is
   sufficient to seal the entire subtree — every inner `pub mod` submodule and every
   inner `pub use` re-export list included — without editing visibility on a single file
   inside `defs/`, `staging/`, or `intake/`. (Noted here as a design fact for whoever
   does T1's mechanical work; this ADR itself makes no `.rs` changes.)

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
`trellis::` usage is `Config`, `Pool`, `migrate` — all already crate-root public. It
needs nothing from this decision. (Worth a line in the doc so a future contributor
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

**What only `trellis`'s own `tests/*.rs` need** (no `benchmark`/`generative` caller):
`defs::eval::{evaluate, evaluate_with_relationships}` (bare, non-`_excluding`
variants — #193's own audit already found their only callers are the in-crate oracle
and test files) and `app::qualified_source_tables` (Decision 1, correction 4).

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
4. The mechanical T1 demotion itself: `lib.rs`'s three `pub mod` → `pub(crate) mod`,
   the `test_util` re-export module from Decision 2 (including the interim `converge`
   entries and `app::qualified_source_tables`), and whichever of #191's outcome it
   needs to carry — done as one coordinated visibility edit, since half-demoting
   `defs`/`staging`/`intake` isn't meaningfully safer than doing it in one pass.
5. #193's `eval` rename, same PR as step 4 or an immediate follow-up.
6. #194's `migrate` doc-language fix — trivial, any time, no ordering dependency.

## Consequences

- 257 `pub` items and 32 `pub mod` submodule declarations across `defs`/`staging`/
  `intake` become `pub(crate)`; the crate-root public surface stays at roughly its
  current ~20 items, plus a new, explicit, dev-only re-export list (Decision 2) sized
  to what `benchmark`/`generative`/the crate's own integration tests actually call —
  around 25 symbols today, not a rubber-stamped whole-module re-exposure.
- `benchmark` and `generative` gain one explicit dependency edge
  (`trellis/test-util`) in place of implicit reach through `pub mod`; a future
  addition is a one-line addition to `test_util`'s re-export list, which is
  self-documenting in a way "everything's already public" never was.
- `trellis`'s own `cargo test` needs the self-dev-dependency change described in
  Decision 2 to keep working unmodified — a one-time build-graph fix, not a per-test
  cost.
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
5. **Decision 1's tier-2 scope** (confirming `Pool`, `migrate`, `Identity`, `metrics`,
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
