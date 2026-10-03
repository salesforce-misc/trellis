//! Program generation (design doc §1/§3): generators producing only valid
//! [`crate::model::Program`]s. Makes no engine calls — the module boundary
//! this crate enforces is that only [`crate::backend`] drives the engine's
//! pipeline.
//!
//! Today's scope is the *trivial* generator (issue #6): every source table is
//! shaped `(numeric pk, numeric c1, numeric c2)` and every definition is a
//! 1-1 numeric-`+`, each with a seed-before-mutate op stream. Improvement-plan
//! task **B3** (`local_docs/generative-suite-improvement-plan.md`) widened
//! this from *exactly* one table/one definition to **1-3 tables and 1-3
//! definitions**, each definition independently drawing one of the tables as
//! its source — see [`build_program_multi`] and [`TableSpec`]. The proptest
//! [`Strategy`]s that draw these live behind the `proptest` feature (see
//! `Cargo.toml`); the pure builders they map onto ([`build_program`],
//! [`build_program_multi`]) are always available so a hand-built pin can
//! reuse the exact same shape without pulling in proptest.
//!
//! `build_program` is kept as a single-table/single-def convenience
//! wrapper around [`build_program_multi`] (rather than changing its
//! signature) because every existing hand-built pin/test across
//! `generative/tests/*.rs` and this module already calls it with the old
//! two-argument shape; delegating keeps every one of those call sites
//! unchanged while sharing the exact same op-construction and pk-liveness
//! logic multi-table programs use.
//!
//! Improvement-plan task **B1** ("Multi-type schemas and the remaining
//! awkward values") widens each table further: every table now also gets
//! exactly one column each of [`ValueType::Text`], [`ValueType::Boolean`],
//! and [`ValueType::Uuid`] — unconditionally, not a probabilistically-drawn
//! dimension stacked on top of B3's table/def-count widening (mirroring
//! `crate::model::Table::new`'s existing "every table gets its pk column
//! unconditionally" philosophy) — and every definition sourced from that
//! table gets a matching identity-passthrough field for each (`SELECT <col>
//! AS <col>`, the same bare-`Expr::Column` shape
//! `trellis/tests/defs_backfill_direct.rs` already exercises by hand, and the
//! same narrowing `defs::ddl::passthrough_source_column` already
//! special-cases). See [`TableSpec`]'s `text_values`/`bool_values`/
//! `uuid_values` fields and the `text_value`/`bool_value`/`uuid_value`
//! strategies below.
//!
//! Improvement-plan task **B4** ("Aggregates") widens the generator past
//! `KeySpace::OneToOne` for the first time: every table now *also*
//! gets a "grain" column — one more [`ValueType::Numeric`] column,
//! unconditionally, the same "every table gets it" philosophy B1 used for
//! `Text`/`Boolean`/`Uuid` (see [`TableSpec`]'s `grain_values` field and the
//! `grain_value` strategy below) — and each definition independently draws
//! either the existing `total = c1 + c2` [`KeySpace::OneToOne`] shape, or a
//! new [`KeySpace::Aggregate`] shape grouped by that table's grain column,
//! with 2–5 fields drawn from `SUM`/`COUNT`/`AVG`/`MIN`/`MAX` over `c1`/`c2`
//! (see [`DefShape`], [`AggregateFn`], and the `aggregate_functions`/
//! `def_shape` strategies below). Issue #255 adds `BOOL_AND`/`BOOL_OR` over
//! the `Boolean` column to that pool. The grain column is deliberately **not**
//! `c1`/`c2` (which keep their existing wider `0..=VALUE_MAX` range and
//! remain the *values being aggregated*) — it's a tiny three-value domain
//! (`0`, `1`, or `2`, see [`GRAIN_MAX`]) specifically so a handful of seed
//! rows collide onto the same group, and a handful of `Delete` mutates has
//! real odds of emptying one out entirely — the single hardest edge in
//! aggregate maintenance (`docs/generative-test-suite.md` §4 calls the
//! aggregate delta path "the hardest guarantee": the only non-idempotent
//! maintenance in the engine), per `local_docs/generative-suite-improvement-plan.md`'s
//! task B4.
//!
//! **The domain *is* now "0, 1, 2, plus `NULL`"**, matching the improvement
//! plan's original text (issue #128). It wasn't always: a `NULL` grouping
//! value was implemented and drawn early in task B4's development, and
//! immediately found a real engine bug — `defs::ddl::create_aggregate_target_table`
//! declared the `GROUP BY` columns as the target's Postgres `PRIMARY KEY`,
//! which is unconditionally `NOT NULL`, so a source row with a `NULL`
//! grouping value made every attempted write to that group fail with a
//! genuine `null value in column ... violates not-null constraint` error.
//! The failure itself was never a liveness wedge on its own — `trellis::client`'s
//! `app_worker_loop` releases a failed claim and re-fetches the same segment,
//! and `staging::quarantine`'s isolate-before-blaming machinery
//! (`quarantine::classify` routes a plain Postgres constraint-violation error
//! to `FailureClass::Isolate`) probes the offending row alone, charges it a
//! death, and evicts/parks it once past `quarantine::DEFAULT_DEATH_THRESHOLD`,
//! letting the rest of the batch drain normally — but the parked row's
//! contribution was then permanently and silently excluded from its
//! aggregate group, with no automatic recovery, and not even a manual
//! `quarantine::release_key` could recover it, since replaying the same row
//! just reproduced the identical constraint violation and re-quarantined it.
//! And because `staging::converge::converged_through`'s condition 4
//! deliberately treats any live `poison_held` row as "not converged" until an
//! operator releases it, a token at or past the poisoned row's LSN never
//! converged — observed directly as this suite's own `quiesce` hanging past
//! its 30-second `QUIESCE_TIMEOUT`.
//!
//! Issue #128 fixed the root cause: `create_aggregate_target_table` now keys
//! the `GROUP BY` columns with a `UNIQUE NULLS NOT DISTINCT` constraint
//! instead of a bare `PRIMARY KEY`, so a NULL-keyed group is representable
//! and still deduplicated correctly, `defs::backfill`'s direct aggregate
//! build no longer silently drops a NULL-keyed group, and
//! `ddl::source_primary_key` falls back to a unique constraint when a source
//! table has no `PRIMARY KEY` so chaining off an aggregate target still
//! works. A NULL grouping value no longer hits the not-null violation at
//! all, so it's never quarantined and never blocks `converge`/`quiesce` in
//! the first place — see [`grain_value`]'s doc comment for how the domain is
//! drawn now.
//!
//! **Design choice: every table gets a grain column, not just tables an
//! `Aggregate` def happens to source from.** The alternative (only tables
//! selected as an aggregate def's source get one) would need the table shape
//! itself to depend on how the *definitions* built on top of it turn out to
//! be drawn — but `table_spec` draws a `TableSpec` independently of, and
//! before, any def that might reference it (`trivial_program_with` draws all
//! tables first, then draws each def's source table index and shape). Making
//! every table's shape identical regardless of how it ends up used keeps
//! `TableSpec`/`Table::new` exactly as uniform as B1 already left them, and
//! costs nothing: an `Aggregate` def can then source from *any* drawn table
//! without a second table-shape variant to thread through, and a `OneToOne`
//! def sourced from a table just leaves that table's grain column
//! undrawn-from (present in the schema, never referenced by a field) —
//! exactly how a `OneToOne` def already leaves the pk column's *identity* as
//! a pk unused as a value.
//!
//! **B4 scope cuts:** the grain column is seeded once at `INSERT` time and
//! never touched by `Update`/`Delete`/`DuplicateInsert` (same B1 precedent —
//! a group changing membership via delete/insert of whole rows is already
//! the sharpest edge; *migrating* a live row from one group to another via
//! `Update` is real, separate coverage `trellis/tests/apply_aggregate.rs`
//! already exercises by hand, not drawn by `trivial_program`; issue #557's
//! concurrent tier draws it as [`Mutate::MoveGroup`]). `KeySpace::Aggregate.group_by`
//! is always exactly one column (never a composite/multi-column group-by,
//! which the engine grammar supports but this generator does not draw).
//! `OneToOne` defs never get a grain passthrough field (unlike the B1
//! `Text`/`Boolean`/`Uuid` columns) — the grain column's role here is
//! structural (an aggregate grouping key), not a type-coverage dimension, so
//! giving it a bare passthrough field would add surface without adding
//! coverage of anything the derivation-type coverage meta-test
//! (`tests/coverage.rs`'s `every_column_scalar_type_appears_via_a_derivation`)
//! doesn't already assert via `c1`/`c2`.
//!
//! **B1 scope cuts, stated plainly (design doc §3's own standard for honest
//! narrowing):**
//! - The new columns are seeded once at `INSERT` time and never touched by
//!   [`Mutate::Update`]/[`Mutate::Delete`]/[`Mutate::DuplicateInsert`], which
//!   continue to read/write only `c1`/`c2` exactly as before. (The one later
//!   exception is the `Boolean` column, which issue #255's
//!   [`Mutate::UpdateFlag`] rewrites in place so `BOOL_AND`/`BOOL_OR` see
//!   value flips, not just rows joining and leaving a group.) This fully
//!   covers "every column scalar type appears via a derivation"
//!   (`generative/tests/coverage.rs`'s
//!   `every_column_scalar_type_appears_via_a_derivation`) without also
//!   having to model heterogeneous per-type `Update`/`DuplicateInsert`
//!   payloads — a separate future widening, not this task.
//! - At the time B1 landed, no operators/functions/comparisons over the new
//!   types were drawn yet (that widening was gated on a precedence-table
//!   prerequisite, deferred to a later session). Improvement-plan task
//!   **B2** ("Operators, functions, and literals") is that later session: it
//!   adds `Operator::GreaterThan`, `Expr::NumberLiteral`/`Expr::StringLiteral`,
//!   and the five scalar functions (`STRPOS`/`OCTET_LENGTH`/`CHAR_LENGTH`/
//!   `REGEXP_COUNT`/`COALESCE`) as one additional, nested "derived" field per
//!   definition — see [`DerivedShape`] and
//!   [`build_program_multi_with_shapes_and_derived`]. **B2 predates B4**
//!   (this doc comment is written after both have merged): a derived field
//!   is attached only to `OneToOne` defs, never `Aggregate` ones — see that
//!   function's own doc comment for why (in short,
//!   `defs::validate::validate`'s `UngroupedColumnReference` check
//!   would reject a derived field's bare, unaggregated column reference on
//!   an `Aggregate` def; every `DerivedShape` references `c1`/`c2`/the text
//!   column bare, none of them wrapped in `SUM`/`COUNT`/`AVG`/`MIN`/`MAX`).
//! - Only syntactically-valid UUID text is ever drawn, or `NULL` — never a
//!   malformed UUID string. A malformed one would fail the `INSERT`/
//!   `UPDATE` statement's own `$n::text::uuid` cast, which is real, separate
//!   future coverage (a new `OpOutcome::Fails` case), not this task.
//! - Awkward text values (empty string, the literal text `"NULL"`, a
//!   U+001F-containing string, a comma/quote/backslash string) are drawn
//!   here, but this does **not** close the improvement plan's "attacks the
//!   key encoding" framing for B1: `defs::ddl::join_pk_key`'s
//!   composite-key delimiter and `defs::oracle::group_key`'s `Aggregate`
//!   grouping-key encoding only matter for a *multi-column* primary key
//!   (never drawn — the pk stays single-column `Numeric`) or a *`Text`-typed*
//!   `Aggregate` key-space `GROUP BY` column — B4's `Aggregate` support
//!   groups only by the grain column, which is `Numeric`, not `Text`, so even
//!   with `Aggregate` defs now real, an awkward text value drawn here still
//!   never reaches a `GROUP BY` column. The awkward text values drawn here
//!   are still real, valuable coverage of plain text round-tripping (SQL
//!   binding, `::text` casts, this harness's own snapshot diffing) — just not
//!   of that specific key-encoding risk, which remains open.
//!
//! Issue **#34** ("relationship edges in the program generator", ADR-0006)
//! adds the first *cross-table* derivations. Every table gains two more
//! `Text` columns — a `UNIQUE` relationship key ([`REL_KEY_COLUMN`]) and a
//! nullable foreign key ([`REL_FK_COLUMN`]) — and a definition may draw an
//! extra field that reads a *related* table through a named relationship.
//! The three shapes drawn are exactly the three the engine supports; see
//! [`RelFieldKind`] for the full shape table and
//! [`attach_relationship_fields`] for how a declaration's endpoints are
//! fixed. Two invariants are worth naming here because they are what keep
//! "only valid programs" structural rather than probabilistic:
//!
//! - **A relationship's join key must be text-stable** (see
//!   [`TEXT_STABLE_JOIN_KEY_TYPES`]), which is why both endpoint columns are
//!   `Text` and not the numeric primary key.
//! - **Relationships always point from a lower-indexed table to a
//!   higher-indexed one**, so the cross-table dependency graph ADR-0006
//!   requires to be acyclic is acyclic by construction.
//!
//! Like the grain column, the relationship columns are seeded once and never
//! mutated — but unlike it, the *related* table's own `c1` is freely mutated
//! and deleted, which is what makes a generated program exercise ADR-0006's
//! reverse propagation rather than only the forward direction.
//!
//! Issue **#505** makes that reverse direction reachable in earnest. The
//! builders emit one table's whole op stream before the next, so on their
//! own every parent-side op would come after every from-side op: a parent
//! change would only ever meet a settled from-side, and no from-side change
//! would ever land after a parent change it has to observe.
//! `trivial_program_with` therefore draws one interleaving of the per-table
//! streams ([`interleave_tables`]); only each table's own order is a real
//! constraint. [`Mutate::ToggleNull`] then moves a row's `c1` across `NULL`
//! on purpose, which on a referenced parent is the sharpest reverse-path
//! edge. `run::Coverage::relationship_path_cases` reports how often each
//! reverse path is reached.
//!
//! # Awkward values (issue #7, design doc §3)
//!
//! Of the four awkward-value classes the design doc calls out:
//!
//! - **NULL in a nullable column** — implemented. `c1`/`c2` are already
//!   nullable at the schema level (`ManualBackend::create_source_table` never
//!   emits `NOT NULL` for a non-primary-key column), and the engine's `+`
//!   evaluator already treats a `NULL` operand as short-circuiting to `NULL`
//!   (`defs::eval`'s `Operator::Add` arm), matching Postgres's own
//!   `numeric + NULL = NULL`. So drawing `None` for `c1`/`c2` needed no
//!   engine or model change — see [`Mutate`] and the `value` strategy below.
//! - **Empty string, the literal text `"NULL"`, delimiter-containing
//!   strings** — implemented by task B1 above, on the new `Text` column: see
//!   the `awkward_text_literal`/`text_value` strategies below. As noted
//!   above, this is real coverage of plain-text round-tripping, but it does
//!   *not* yet reach the delimiter-sensitive key-encoding paths (those need
//!   a multi-column pk or an `Aggregate` key space, both still future work).
//!
//! [`Strategy`]: proptest::strategy::Strategy

use std::collections::HashMap;

use trellis::dev::defs::ast::{
    Expr, FieldDef, GroupByKey, KeySpace, Operator, Predicate, TransformDef, ValueType,
};

use crate::model::{
    Burst, BurstAction, Cardinality, Column, ConcurrentPlan, NamePool, Op, OpOutcome, Program,
    Relationship, Table, TimedAction,
};

/// The inclusive upper bound of the calculated-field value domain.
///
/// **Numeric-path pairing (design doc §3, a load-bearing invariant).** The
/// generator only ever emits small non-negative *integers* here, and the sole
/// derivation is `c1 + c2`. Both the engine (`trellis::numeric::Numeric`) and
/// the SQL oracle (Postgres `numeric`) are arbitrary-precision base-10: an
/// integer sum has one exact representation on both sides, with no rounding
/// and no float path to fall off of, so the two can never disagree by
/// construction. The bound is therefore purely for legibility — a shrunk
/// counterexample reads at a glance — and for staying deliberately far from
/// any future column-type narrowing (e.g. an `int4` source column) that could
/// introduce a value one side represents differently. Widen this, or add
/// fractional/negative values, only together with the comparison-semantics
/// review the widening implies.
pub const VALUE_MAX: i64 = 99;

/// The most rows the trivial generator seeds before mutating. Kept tiny: each
/// op costs a full apply → quiesce → snapshot → compare round trip, and a
/// legible counterexample beats a large one (design doc §1, §4).
pub const MAX_SEED_ROWS: usize = 4;

/// The most mutate ops appended after the seed phase. Kept small because
/// every op is a full apply → quiesce → snapshot → compare round trip against
/// a real cluster, so the per-case cost scales with the op count.
pub const MAX_MUTATES: usize = 4;

/// The most source tables a drawn program has (improvement-plan task B3).
/// `1..=MAX_TABLES` via `prop_flat_map`, matching the `MAX_SEED_ROWS`/
/// `MAX_MUTATES` idiom above, so proptest's default numeric-range shrinking
/// reduces the table *count* first — a 1-table counterexample is the
/// readable one. Kept at 3 (not e.g. 2) so "two defs sharing one source" and
/// "defs over different sources" are both reachable without a def count that
/// forces one of them.
pub const MAX_TABLES: usize = 3;

/// The most definitions a drawn program has (improvement-plan task B3). Each
/// definition independently draws one of the program's tables as its
/// source — see [`build_program_multi`] — so this is deliberately not tied
/// to `MAX_TABLES`: two definitions can and do land on the same table.
pub const MAX_DEFS: usize = 3;

/// The inclusive upper bound of the grain column's *non-`NULL`* value domain
/// (improvement-plan task B4): `0..=GRAIN_MAX` is the grain column's non-NULL
/// value space — kept at `2` (three possible non-`NULL` values; see
/// [`grain_value`]'s doc comment for the fourth, `NULL`, value) so it stays
/// "tiny, heavily-repeating" (the plan's own words): with `MAX_SEED_ROWS` (4)
/// rows drawn from a 3-value domain, a real collision (two seed rows landing
/// in the same group) is the common case, not a rare one, and a handful of
/// `Delete` mutates has real odds of emptying a group entirely — see the
/// module doc comment's B4 section.
pub const GRAIN_MAX: i64 = 2;

/// The concurrent tier's hot-key shape (issue #557): the range of rows its
/// hot table seeds. Small next to [`HOT_MUTATES`], so every key takes many
/// writes per burst, but large enough that the rows spread across all of a
/// split batch's buckets (a row's bucket is a hash of its key).
pub const HOT_KEYS: std::ops::RangeInclusive<usize> = 8..=24;

/// The hot-key shape's hottest keys: half of its mutates pick a pk from
/// `1..=HOT_CORE_KEYS`, the other half from every seeded pk.
pub const HOT_CORE_KEYS: i64 = 3;

/// The range of mutates the hot-key shape draws for its hot table. The top
/// of the range is bounded by the per-case time: every op is a real
/// statement, and a burst is checked against the oracle once it settles.
pub const HOT_MUTATES: std::ops::RangeInclusive<usize> = 1_000..=2_400;

/// The width of the group window [`Mutate::MoveGroup`] draws from: a drawn
/// grain is `0..=HOT_GRAIN_MAX` above the window's current bottom. Seeds
/// only use `0..=GRAIN_MAX`, so the groups above it don't exist until a row
/// moves into one mid-run: the new-group bursts of #389 and #539.
pub const HOT_GRAIN_MAX: i64 = 7;

/// Every this many mutates, the hot-key shape's group window slides up by
/// [`HOT_GROUP_SLIDE`], so groups nobody has used before keep appearing all
/// through a run, in every burst rather than only the first.
pub const HOT_GROUP_EPOCH: usize = 150;

/// How far the group window slides each [`HOT_GROUP_EPOCH`].
pub const HOT_GROUP_SLIDE: i64 = 2;

/// The range of ops per burst the hot-key shape draws: at least twice the
/// engine's split threshold (256 rows in one sealed batch), so a burst can
/// seal into several batches that are each large enough to split, and small
/// enough next to [`HOT_MUTATES`] that every case has two bursts or more (a
/// new group can only appear after the first).
pub const HOT_BURST_SIZE: std::ops::RangeInclusive<usize> = 500..=1_000;

/// The range of drain workers the hot-key shape runs the engine with: more
/// than one to split a batch across, and up to its bucket count (8).
pub const HOT_WORKERS: std::ops::RangeInclusive<usize> = 4..=8;

/// The range of seal cadences, in milliseconds, the hot-key shape runs the
/// engine with ([`ConcurrentCase::seal_interval_ms`]). Short next to a
/// burst's issue time, so a burst seals into several batches. How many rows
/// a batch gets depends on how fast the harness issues, which varies by
/// machine, so the range is wide: its short end seals many small batches,
/// its long end fewer that clear the split threshold.
pub const HOT_SEAL_INTERVAL_MS: std::ops::RangeInclusive<u64> = 20..=200;

/// The cooling-key shape's key count (issue #557 part 3b): how many keys its
/// cooling table takes, each written a few times and then never again.
/// Hundreds, so every burst holds many keys going quiet at once.
pub const COOLING_KEYS: std::ops::RangeInclusive<usize> = 150..=400;

/// How many writes one cooling key takes: its insert, then one to three
/// updates (the last sometimes a delete).
pub const COOLING_WRITES: std::ops::RangeInclusive<usize> = 2..=4;

/// Where in its burst a cooling key's last write goes, in thousandths of the
/// burst ([`CoolingPlace::last`]): the middle, so the batches holding it
/// drain while the rest of the burst still writes.
pub const COOLING_LAST: std::ops::RangeInclusive<u16> = 200..=800;

/// How far apart a cooling key's writes are, in thousandths of the burst
/// ([`CoolingPlace::gap`]): close together. The harness issues a burst in
/// about 15 to 40ms on a tmpfs cluster, so at [`COOLING_SEAL_INTERVAL_MS`]
/// some of a key's writes share a batch and some straddle a seal.
pub const COOLING_GAP: std::ops::RangeInclusive<u16> = 30..=200;

/// The range of drain workers the cooling-key shape runs the engine with.
/// Fewer than [`HOT_WORKERS`]: in tmpfs probes, 4 to 8 workers caught a
/// stale 1-1 write in about 1 case in 15, 2 to 6 in about 1 in 4 (#557 part
/// 3b's PR). The likely reason is that a busier pool lets sealed batches
/// queue, so one worker's drain of a coalesced backlog overlaps another's
/// drain of a fresh, short batch, and the newer batch can commit first.
pub const COOLING_WORKERS: std::ops::RangeInclusive<usize> = 3..=6;

/// The cooling-key shape's seal cadences, in milliseconds. Shorter than
/// [`HOT_SEAL_INTERVAL_MS`]: the harness issues a whole burst in tens of
/// milliseconds, so the hot-key cadence seals most bursts into one or two
/// batches, and a cooling key's writes into the same one.
pub const COOLING_SEAL_INTERVAL_MS: std::ops::RangeInclusive<u64> = 5..=20;

/// Index, within every generated source table's `columns`, of the
/// relationship **key** column (issue #34): a `Text` column carrying a real
/// single-column `UNIQUE` constraint (`crate::model::Table::unique_cols`),
/// so a relationship whose *to*-side is this column is **to-one** under
/// ADR-0006's cardinality rule.
///
/// `Text` specifically, not the numeric primary key, because a
/// relationship's join key must be a *text-stable* type — see
/// [`TEXT_STABLE_JOIN_KEY_TYPES`].
pub const REL_KEY_COLUMN: usize = 7;

/// Index, within every generated source table's `columns`, of the
/// relationship **foreign-key** column (issue #34): a nullable `Text` column
/// with no unique constraint, so a relationship whose *to*-side is this
/// column is **to-many**. As a *from*-side it is the ordinary FK of a to-one
/// relationship.
pub const REL_FK_COLUMN: usize = 8;

/// The Postgres type names whose equality is *text-stable* — the engine
/// rejects any relationship whose join key isn't one of these
/// (`ValidationError::RelationshipUnsupportedJoinKeyType`), because Trellis's
/// join is `a::text = b::text` while the Postgres oracle's is the type's
/// native `=`, and the two disagree for `numeric`/`real` (scale:
/// `1.0::text != 1.00::text`), `character(n)` (blank padding), `citext`
/// (case), and the date/time types (session-dependent rendering).
///
/// **Deliberately duplicated here rather than imported.** The engine's own
/// copy (`defs::catalog`'s `TEXT_STABLE_JOIN_KEY_TYPES`) is private
/// to that module, and the design doc's seam (§1/§2) says this crate should
/// not reach into engine internals to decide what a valid program is — the
/// generator's job is to *independently* know the rules and only emit
/// programs that satisfy them, exactly as [`crate::oracle`] independently
/// renders SQL rather than calling the engine's renderer. Exposing the
/// engine's constant would also be an `trellis/` change, which issue #34 is
/// explicitly scoped out of. The cost of the duplication is bounded: if the
/// two ever drift, the generator emits a relationship the engine rejects,
/// and an install rejection is a **hard failure, never a skip** (design
/// doc §3) — so the drift surfaces as a loud test failure on the very next
/// run, not as silently-lost coverage.
///
/// Only [`trellis::dev::defs::ast::ValueType::Text`] and
/// [`trellis::dev::defs::ast::ValueType::Uuid`] of this crate's four value types
/// map into this set (`Numeric` renders as `numeric` and `Boolean` as
/// `boolean`, neither of which is listed), which is why the relationship
/// key/FK columns are `Text`.
pub const TEXT_STABLE_JOIN_KEY_TYPES: &[&str] = &[
    "smallint",
    "integer",
    "bigint",
    "uuid",
    "text",
    "character varying",
];

/// The relationship **key** column's value for seeded primary key `pk`
/// (issue #34): derived from the pk rather than drawn, so it is distinct for
/// every row of a table by construction and its `UNIQUE` constraint can
/// never be violated by any draw. Shares its `k`-prefixed shape with
/// [`strategy::rel_fk_value`]'s pool so a foreign key really does find
/// matching related rows some of the time.
pub fn rel_key_value(pk: i64) -> String {
    format!("k{pk}")
}

/// Renders an `Option<i64>` value to the rendered-text form
/// [`crate::model::Op`] wants: `None` (SQL `NULL`) stays `None`.
fn render(value: Option<i64>) -> Option<String> {
    value.map(|v| v.to_string())
}

/// One post-seed mutation, parameterized only by primary-key integers and
/// (for updates/duplicate-inserts) new field values — the plain-data draw a
/// proptest strategy shrinks, kept separate from the [`Op`] it renders into so
/// the builder stays proptest-free.
///
/// `c1`/`c2` are `Option<i64>`, `None` meaning SQL `NULL`: `c1`/`c2` are
/// nullable columns at the schema level (`ManualBackend::create_source_table`
/// emits no `NOT NULL` for any non-primary-key column), so a null value here
/// is exercising real, already-representable schema surface — no widening of
/// [`crate::model::Column`]/[`Table`] was needed for this (see the
/// module-level doc comment's "awkward values" note).
#[derive(Debug, Clone, PartialEq)]
pub enum Mutate {
    /// Set `c1`/`c2` on row `pk`. When `pk` names no seeded row this is a
    /// no-op at the source (Postgres updates zero rows, no error), which is
    /// exactly the "operation errors are checked, not swallowed" case the
    /// convergence property must still converge on (design doc §4).
    Update {
        pk: i64,
        c1: Option<i64>,
        c2: Option<i64>,
    },
    /// Set row `pk`'s `Boolean` column to `flag` (`"true"`/`"false"`, or
    /// `None` for SQL `NULL`), and nothing else (issue #255). The one
    /// exception to the "non-numeric columns are seeded once, never mutated"
    /// scope cut (see the module doc comment): without it, a `BOOL_AND`/
    /// `BOOL_OR` field would only ever see whole rows join or leave its group,
    /// never a value flip inside it — the retraction that makes a
    /// `RecomputeOnly` aggregate hard, and the one `MIN`/`MAX` get from
    /// [`Mutate::Update`]'s `c1`/`c2` writes. A `pk` naming no live row is a
    /// source no-op, exactly like `Update`.
    UpdateFlag { pk: i64, flag: Option<String> },
    /// Move row `pk`'s `c1` across `NULL` (issue #505): to `NULL` if it is
    /// currently non-`NULL`, otherwise to `value`. Renders to an `UPDATE` of
    /// `c1` alone. A plain [`Mutate::Update`] draws `c1` independently of
    /// what the row holds, so it crosses `NULL` only about a third of the
    /// time; this always does. A relationship reads the to-side table's
    /// `c1`, so on a referenced parent this is the sharpest reverse-path
    /// edge: the related value leaves or joins every fold that reads it, and
    /// `COUNT(<rel>.<col>)` starts or stops counting the row. A `pk` naming
    /// no live row is a source no-op, exactly like `Update`.
    ToggleNull { pk: i64, value: i64 },
    /// Delete row `pk`. A `pk` naming no seeded row is likewise a no-op.
    Delete { pk: i64 },
    /// Insert a *second* row at an already-seeded `pk`. When that pk is
    /// still live (the common case) this is a genuine `apply()` failure, not
    /// a source no-op: the source table's primary key is a real Postgres
    /// `primary key` constraint, so this statement is rejected with a
    /// unique-violation error and the source is left unchanged (a single
    /// `INSERT` is atomic — it cannot partially apply). Closes the issue #6
    /// gap where every generated op used to succeed, so "an op that errors
    /// changed nothing" (design doc §4) was never exercised against a *real*
    /// rejection (a missing-pk update/delete comes back as `Ok(0 rows)`, not
    /// an `Err`).
    ///
    /// If an earlier mutate in the same stream already deleted this pk, it
    /// is no longer live: this "duplicate" insert is then an ordinary
    /// successful insert that revives it — [`build_program`] simulates pk
    /// liveness across the whole mutate stream to expect the right outcome
    /// either way (see its `expect: OpOutcome` derivation).
    ///
    /// A revival's rendered [`Op::Insert`] ([`render_mutate`]) carries the
    /// *original* seed row's `Text`/`Boolean`/`Uuid`/grain column values
    /// (tasks B1/B4), not fresh `NULL`s: those columns are "seeded once,
    /// never touched again" (this enum only ever carries `c1`/`c2`, plus
    /// [`Mutate::UpdateFlag`]'s `Boolean` value), so a revival is still
    /// logically the same row coming back, and must keep its original
    /// non-numeric content. (A revival after an `UpdateFlag` brings back the
    /// *seed* flag, not the updated one — just an ordinary insert value, since
    /// the SQL oracle reads whatever Postgres holds.) This matters well beyond
    /// legibility for the grain column specifically — see the
    /// `grain_value` proptest strategy's doc comment for issue #128, the
    /// engine bug a `NULL` grain value used to hit and which grain_value's
    /// own occasional `NULL` draw now exercises deliberately; a revival that
    /// carelessly *reset* the grain column to some other value (`NULL` or
    /// otherwise) instead of preserving whatever it originally was would
    /// silently change which group the revived row belongs to.
    DuplicateInsert {
        pk: i64,
        c1: Option<i64>,
        c2: Option<i64>,
    },
    /// Clears every row of this table in one statement (improvement-plan task
    /// E6). Renders to [`Op::Truncate`]. Whether this "affects rows" tracks
    /// the table's *current* liveness set exactly like every other variant
    /// here (`OpOutcome::Succeeds` if any pk is still live, `AffectsNoRows`
    /// if the table happens to already be empty) — but unlike
    /// `Update`/`Delete`, which touch one pk, this clears every remaining
    /// live pk at once: [`render_mutate`] empties the whole `live` map, so
    /// every mutate after a `Truncate` sees an empty table exactly as real
    /// Postgres would.
    Truncate,
    /// Moves row `pk` into group `grain` by setting its grain column (issue
    /// #557), and nothing else. The one exception to the B4 scope cut that
    /// the grain column is seeded once and never updated: only the
    /// concurrent tier's hot-key strategy draws it (see [`HOT_GRAIN_MAX`]),
    /// so a group can lose and regain members mid-burst (#389, #539) and a
    /// group nobody seeded can appear. `trivial_program` never draws it. A
    /// `pk` naming no live row is a source no-op, exactly like `Update`.
    MoveGroup { pk: i64, grain: Option<i64> },
}

/// Which of a source table's two aggregated columns (`c1`/`c2`) a generated
/// `SUM`/`AVG`/`MIN`/`MAX` field aggregates over (improvement-plan task B4).
/// `COUNT(*)` has no column argument at all (see [`AggregateFn::Count`]), and
/// `BOOL_AND`/`BOOL_OR` always fold the table's one `Boolean` column (see
/// [`AggregateFn::BoolAnd`]), so this only shows up nested inside the four
/// numeric variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateColumn {
    C1,
    C2,
}

/// One calculated field of a generated [`KeySpace::Aggregate`] definition
/// (task B4): one of the functions
/// `trellis::dev::defs::registry::AGGREGATE_FUNCTIONS` accepts, carrying which
/// column it aggregates where there is a choice. Kept as a small enum — rather
/// than a bare `(name, column)` pair — so [`aggregate_field_def`]'s match
/// stays exhaustive: adding another aggregate function to the generator needs
/// a new variant here, which then has to be given an argument before it
/// compiles, not just a new string someone forgot to type-check.
///
/// `SUM`/`COUNT`/`AVG` are
/// [`trellis::dev::defs::invertibility::Invertibility::Invertible`]
/// (delta-maintained); `MIN`/`MAX`/`BOOL_AND`/`BOOL_OR` are always
/// [`trellis::dev::defs::invertibility::Invertibility::RecomputeOnly`] — see that
/// module's doc comment. Drawing a real mix of both classes in the same
/// aggregate def (not just across different defs) is exactly what
/// `tests/coverage.rs`'s B4 floor tests assert actually happens.
///
/// The registry's other aggregates (`BIT_AND`/`BIT_OR`/`JSONB_AGG`) are not
/// drawn: they need argument types this generator's schema doesn't have.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFn {
    Sum(AggregateColumn),
    /// `COUNT(*)`: row counting, no argument.
    Count,
    Avg(AggregateColumn),
    Min(AggregateColumn),
    Max(AggregateColumn),
    /// `BOOL_AND(<bool col>)` over the table's one `Boolean` column (issue
    /// #255) — the only column `BOOL_AND` type-checks against, so there is
    /// nothing to draw. Postgres's semantics are the oracle: `NULL`s are
    /// skipped, and a group whose values are all `NULL` folds to `NULL`.
    BoolAnd,
    /// `BOOL_OR(<bool col>)`, the same shape as [`AggregateFn::BoolAnd`].
    BoolOr,
}

impl AggregateFn {
    /// This function's canonical uppercased name, exactly as
    /// `trellis::dev::defs::registry::AGGREGATE_FUNCTIONS` spells it.
    fn name(self) -> &'static str {
        match self {
            AggregateFn::Sum(_) => "SUM",
            AggregateFn::Count => "COUNT",
            AggregateFn::Avg(_) => "AVG",
            AggregateFn::Min(_) => "MIN",
            AggregateFn::Max(_) => "MAX",
            AggregateFn::BoolAnd => "BOOL_AND",
            AggregateFn::BoolOr => "BOOL_OR",
        }
    }
}

/// Which shape a generated [`TransformDef`] takes (improvement-plan task B4
/// widens this past the previous always-`OneToOne` assumption): the existing
/// `total = c1 + c2` [`KeySpace::OneToOne`] shape, or a `GROUP BY`
/// [`KeySpace::Aggregate`] shape over one of its source table's grain column,
/// with `functions` giving its calculated fields beyond the grain-column
/// passthrough (see [`build_program_multi_with_shapes`]).
#[derive(Debug, Clone, PartialEq)]
pub enum DefShape {
    OneToOne,
    Aggregate { functions: Vec<AggregateFn> },
}

/// One drawn table's seed rows and post-seed mutate stream, for
/// [`build_program_multi`].
///
/// Every [`Op`] already names the table it targets (`crate::model::Op`), but
/// [`Mutate`] deliberately does not — it has no notion of "which table" at
/// all, only a pk within *some* table's own pk space. Rather than teach
/// `Mutate` a table field (which would let a single mutate stream reference a
/// table it wasn't drawn against, a whole class of invalid-program bug this
/// type is built to make unrepresentable), a multi-table program instead
/// groups each table's seed values with the mutates meant for *that* table.
/// [`build_program_multi`] then runs the same per-table pk-liveness
/// simulation [`build_program`] always ran, once per `TableSpec`, so table A's
/// deletes can never be mistaken for table B's — see that function's doc
/// comment.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TableSpec {
    /// `seed_values[i]` is the `(c1, c2)` pair for seeded primary key
    /// `i + 1`, exactly as [`build_program`]'s `seed_values` parameter.
    pub seed_values: Vec<(Option<i64>, Option<i64>)>,
    /// `text_values[i]` is the rendered value (already in [`Op`]'s
    /// `Option<String>` convention — `None` is SQL `NULL`) for seeded
    /// primary key `i + 1`'s `Text` column (improvement-plan task B1). Must
    /// have exactly `seed_values.len()` entries — [`build_program_multi`]
    /// panics otherwise, a generator-bug-style check matching this module's
    /// other `assert!`s.
    pub text_values: Vec<Option<String>>,
    /// `bool_values[i]` is the rendered value (`"true"`/`"false"`, or `None`
    /// for SQL `NULL`) for seeded primary key `i + 1`'s `Boolean` column
    /// (task B1). Same length contract as `text_values`.
    pub bool_values: Vec<Option<String>>,
    /// `uuid_values[i]` is the rendered value (a syntactically-valid UUID
    /// string, or `None` for SQL `NULL` — never a malformed UUID string, see
    /// the module doc comment's B1 scope cuts) for seeded primary key
    /// `i + 1`'s `Uuid` column (task B1). Same length contract as
    /// `text_values`.
    pub uuid_values: Vec<Option<String>>,
    /// `grain_values[i]` is the rendered value (`"0"`/`"1"`/`"2"`, see
    /// [`GRAIN_MAX`], or `None` for a `NULL` group — see [`grain_value`]'s
    /// doc comment for issue #128) for seeded primary key `i + 1`'s grain
    /// column (improvement-plan task B4). Stays `Option<String>` — the same
    /// shape every other seeded column here uses — so a hand-built pin can
    /// construct a `None` (SQL `NULL`) grain value directly for a targeted
    /// regression pin, same as the `grain_value` proptest strategy itself now
    /// draws one on its own. Same length contract as `text_values`. Only
    /// [`Mutate::MoveGroup`] (issue #557's concurrent tier) touches it — see
    /// the module doc comment's B4 scope cuts.
    pub grain_values: Vec<Option<String>>,
    /// `rel_fk_values[i]` is the rendered value for seeded primary key
    /// `i + 1`'s relationship **foreign-key** column (issue #34) — the
    /// non-unique `Text` column at index [`REL_FK_COLUMN`]. Drawn from
    /// [`strategy::rel_fk_value`]'s tiny pool so a generated relationship
    /// really exercises all three join outcomes: a value that matches a
    /// related row's key, a value that matches nothing, and SQL `NULL`.
    /// Same length contract as `text_values`. Never touched by [`Mutate`],
    /// like every other non-numeric column here.
    ///
    /// The matching relationship **key** column ([`REL_KEY_COLUMN`]) has no
    /// field here on purpose: it is derived from the row's own primary key
    /// ([`rel_key_value`]) rather than drawn, which is what makes its
    /// `UNIQUE` constraint unfalsifiable by any draw — a drawn key column
    /// could collide across two seed rows and turn a legal program into an
    /// insert that Postgres rejects.
    pub rel_fk_values: Vec<Option<String>>,
    /// Mutates appended after this table's seed inserts (seed-before-mutate,
    /// design doc §3), targeting only this table's own pks. Never touches
    /// the `Text`/`Boolean`/`Uuid`/grain columns above — see the module doc
    /// comment's B1/B4 scope cuts.
    pub mutates: Vec<Mutate>,
}

impl TableSpec {
    /// Builds a `TableSpec` whose `Text`/`Boolean`/`Uuid` columns (task B1)
    /// and grain column (task B4) are all `NULL` for every seed row — a
    /// convenience for callers that only care about the numeric seed/mutate
    /// shape (e.g. the pk-liveness unit tests below, and [`build_program`]'s
    /// two-argument convenience wrapper), sparing them from hand-counting a
    /// matching-length `NULL` vector for each of the four new columns.
    pub fn numeric_only(
        seed_values: Vec<(Option<i64>, Option<i64>)>,
        mutates: Vec<Mutate>,
    ) -> Self {
        let row_count = seed_values.len();
        TableSpec {
            seed_values,
            text_values: vec![None; row_count],
            bool_values: vec![None; row_count],
            uuid_values: vec![None; row_count],
            grain_values: vec![None; row_count],
            rel_fk_values: vec![None; row_count],
            mutates,
        }
    }
}

/// Renders one [`Mutate`] into the [`Op`] it becomes against `table`
/// (`table.columns[1..=2]` are `c1`/`c2`, `[3..=5]` are `Text`/`Boolean`/
/// `Uuid`, `[6]` is the grain column — the same layout
/// [`build_program_multi_with_shapes`] builds, see its doc comment),
/// threading `live` — that table's own pk-liveness set — through so the
/// emitted op's `expect` tracks the pk's *current* state, not just whether it
/// was originally seeded (see [`Mutate::DuplicateInsert`]).
///
/// Shared by [`build_program`] and [`build_program_multi`] so single- and
/// multi-table programs run the exact same liveness logic — this is the one
/// place that logic lives, specifically so a per-table bug (liveness leaking
/// across tables, or one table's pks silently continuing another's
/// numbering) has nowhere to hide a second, diverging copy.
///
/// `spec` is the same [`TableSpec`] the caller already has the seed values
/// in, so a [`Mutate::DuplicateInsert`] revival can look its original row's
/// values back up by `pk` (always in `1..=spec.seed_values.len()`, since that
/// variant's pk is only ever drawn from an originally-seeded pk — see
/// [`Mutate::DuplicateInsert`]'s doc comment for why a revival must carry
/// them, not default them to `NULL`).
///
/// `live` maps each live pk to its current `c1`, which is all of a row's
/// state a mutate's rendering depends on ([`Mutate::ToggleNull`] reads it).
fn render_mutate(
    mutate: &Mutate,
    table: &Table,
    spec: &TableSpec,
    live: &mut HashMap<i64, Option<i64>>,
) -> Op {
    let table_name = &table.name;
    let c1 = &table.columns[1].name;
    let c2 = &table.columns[2].name;
    match mutate {
        Mutate::Update { pk, c1: a, c2: b } => Op::Update {
            table: table_name.clone(),
            pk: pk.to_string(),
            changes: vec![(c1.clone(), render(*a)), (c2.clone(), render(*b))],
            expect: match live.get_mut(pk) {
                Some(current) => {
                    *current = *a;
                    OpOutcome::Succeeds
                }
                None => OpOutcome::AffectsNoRows,
            },
        },
        Mutate::UpdateFlag { pk, flag } => Op::Update {
            table: table_name.clone(),
            pk: pk.to_string(),
            changes: vec![(table.columns[4].name.clone(), flag.clone())],
            expect: if live.contains_key(pk) {
                OpOutcome::Succeeds
            } else {
                OpOutcome::AffectsNoRows
            },
        },
        Mutate::ToggleNull { pk, value } => {
            let (new, expect) = match live.get_mut(pk) {
                Some(current) => {
                    *current = match current {
                        Some(_) => None,
                        None => Some(*value),
                    };
                    (*current, OpOutcome::Succeeds)
                }
                None => (Some(*value), OpOutcome::AffectsNoRows),
            };
            Op::Update {
                table: table_name.clone(),
                pk: pk.to_string(),
                changes: vec![(c1.clone(), render(new))],
                expect,
            }
        }
        Mutate::MoveGroup { pk, grain } => Op::Update {
            table: table_name.clone(),
            pk: pk.to_string(),
            changes: vec![(table.columns[6].name.clone(), render(*grain))],
            expect: if live.contains_key(pk) {
                OpOutcome::Succeeds
            } else {
                OpOutcome::AffectsNoRows
            },
        },
        Mutate::Delete { pk } => Op::Delete {
            table: table_name.clone(),
            pk: pk.to_string(),
            // `remove` reports whether `pk` was live, and (whether or
            // not it was) leaves it dead afterward — exactly the delete
            // semantics we're simulating.
            expect: if live.remove(pk).is_some() {
                OpOutcome::Succeeds
            } else {
                OpOutcome::AffectsNoRows
            },
        },
        Mutate::DuplicateInsert { pk, c1: a, c2: b } => {
            // Only a genuine primary-key violation while `pk` is still
            // live. If an earlier mutate already deleted it, this isn't
            // a duplicate anymore — it's an ordinary successful insert
            // that revives the pk.
            let expect = if live.contains_key(pk) {
                OpOutcome::Fails
            } else {
                live.insert(*pk, *a);
                OpOutcome::Succeeds
            };
            // This variant's pk is always one of `1..=spec.seed_values.len()`
            // (the `dup_pk` strategy never draws outside that range), so the
            // original seed row's text/bool/uuid/grain values are always
            // available here by index — see this function's own doc comment
            // for why a revival must carry them forward rather than leaving
            // them to default to `NULL` (a real, already-live row when this
            // op `Fails` doesn't matter either way, since a failed `INSERT`
            // changes nothing — Postgres never applies any of `row`).
            let seed_index = (*pk as usize) - 1;
            Op::Insert {
                table: table_name.clone(),
                row: vec![
                    (table.pk_col.clone(), Some(pk.to_string())),
                    (c1.clone(), render(*a)),
                    (c2.clone(), render(*b)),
                    (
                        table.columns[3].name.clone(),
                        spec.text_values[seed_index].clone(),
                    ),
                    (
                        table.columns[4].name.clone(),
                        spec.bool_values[seed_index].clone(),
                    ),
                    (
                        table.columns[5].name.clone(),
                        spec.uuid_values[seed_index].clone(),
                    ),
                    (
                        table.columns[6].name.clone(),
                        spec.grain_values[seed_index].clone(),
                    ),
                    // Issue #34: the relationship key/foreign-key columns
                    // are seeded once and never mutated either, so a
                    // revival carries them forward for the same reason.
                    // The key column specifically *must* come back with its
                    // original value: it carries a real `UNIQUE` constraint
                    // (see `crate::model::Table::unique_cols`), and a
                    // revival that defaulted it to `NULL` would silently
                    // stop being the row a to-many relationship joins to.
                    (
                        table.columns[REL_KEY_COLUMN].name.clone(),
                        Some(rel_key_value(*pk)),
                    ),
                    (
                        table.columns[REL_FK_COLUMN].name.clone(),
                        spec.rel_fk_values[seed_index].clone(),
                    ),
                ],
                expect,
            }
        }
        Mutate::Truncate => {
            let expect = if live.is_empty() {
                OpOutcome::AffectsNoRows
            } else {
                OpOutcome::Succeeds
            };
            live.clear();
            Op::Truncate {
                table: table_name.clone(),
                expect,
            }
        }
    }
}

/// Builds the trivial single-table/single-def program from already-drawn
/// data: `seed_values[i]` is the `(c1, c2)` pair for seeded primary key
/// `i + 1`, and `mutates` are appended after every seed insert
/// (seed-before-mutate, design doc §3, so every update and delete has real
/// rows to hit).
///
/// The schema and definition are fixed — one source table `t0` with a numeric
/// primary key `c0` and two numeric columns `c1`/`c2`, one 1-1 target `t1`
/// computing `c1 + c2` — so the only thing that varies (and shrinks) between
/// cases is the data and the mutate stream.
///
/// A thin convenience wrapper around [`build_program_multi`] (one
/// `TableSpec`, one def sourced from it) kept at its original two-argument
/// signature rather than folded away: every existing hand-built pin across
/// `generative/tests/*.rs` and this module's own tests already calls it this
/// shape, and there was no reason to touch two dozen call sites for a change
/// scoped to table/def *count* (improvement-plan task B3).
pub fn build_program(seed_values: &[(Option<i64>, Option<i64>)], mutates: &[Mutate]) -> Program {
    build_program_multi(
        &[TableSpec::numeric_only(
            seed_values.to_vec(),
            mutates.to_vec(),
        )],
        &[0],
    )
}

/// Builds a program over 1 or more tables and 1 or more definitions
/// (improvement-plan task B3): `tables[i]` describes source table `i`'s seed
/// rows and mutate stream (see [`TableSpec`]), and `def_sources[j]` is the
/// index into `tables` that definition `j` reads from — independently drawn,
/// so two definitions landing on the same table (fan-out) and definitions
/// spread across different tables are both representable, including every
/// mix of the two in one program.
///
/// Every table gets its own numeric pk/`c1`/`c2` schema, plus (task B1) one
/// `Text`/`Boolean`/`Uuid` column each, and its own independent pk-liveness
/// simulation (a fresh liveness map per `TableSpec`, via [`render_mutate`]):
/// table A's deletes and inserts can never be mistaken for table B's, and
/// each table's seeded pks start at `1` regardless of how many rows an
/// earlier table seeded — table identity, not draw order, is what a pk is
/// scoped to. Every definition sourced from a table gets the same field
/// list: `total = c1 + c2`, plus an identity-passthrough field for each of
/// that table's `Text`/`Boolean`/`Uuid` columns (task B1) — this function
/// widens *how many* tables/defs a program has and, since B1, *how many
/// scalar types* each table/def touches; it does not draw operators,
/// functions, or comparisons over those types (that's B2, out of scope
/// here).
///
/// Ops are emitted one table at a time, in `tables` order: all of table 0's
/// seeds and mutates, then all of table 1's, and so on. This keeps a
/// hand-built pin's op stream legible (every op naming table N groups
/// together) and is not a claim that real traffic interleaves tables that
/// way — nothing about the model or the backend assumes any particular
/// interleaving. The drawn strategies interleave the tables afterward with
/// [`interleave_tables`] (issue #505).
///
/// Panics if `tables` or `def_sources` is empty, or if a `def_sources` entry
/// is out of range for `tables` — both are generator bugs (every strategy
/// below draws `1..=MAX_TABLES`/`1..=MAX_DEFS` and indexes accordingly), not
/// conditions a caller should need to handle.
///
/// A thin wrapper around [`build_program_multi_with_shapes`] (every def
/// `OneToOne`) kept at its original `&[usize]` signature rather than folded
/// away — the same "don't touch two dozen call sites for an orthogonal
/// widening" reasoning [`build_program`]'s own doc comment gives for staying
/// at its two-argument shape (improvement-plan task B4 widens *which shapes*
/// a def can take, not how many tables/defs a program has, which is what
/// this signature already expresses).
pub fn build_program_multi(tables: &[TableSpec], def_sources: &[usize]) -> Program {
    let defs: Vec<(usize, DefShape)> = def_sources
        .iter()
        .map(|&idx| (idx, DefShape::OneToOne))
        .collect();
    build_program_multi_with_shapes(tables, &defs)
}

/// [`build_program_multi`]'s general form (improvement-plan task B4): `defs[j]`
/// is `(source table index, shape)` for definition `j`, so each definition
/// independently draws not just *which* table it sources from but *which
/// shape* it takes — the existing `total = c1 + c2` [`KeySpace::OneToOne`], or
/// a `GROUP BY` [`KeySpace::Aggregate`] over that table's grain column with
/// [`DefShape::Aggregate`]'s `functions` as its non-grouping fields.
///
/// Every table gets its own numeric pk/`c1`/`c2` schema, plus (task B1) one
/// `Text`/`Boolean`/`Uuid` column each, plus (task B4) one further Numeric
/// "grain" column (see the module doc comment), and its own independent
/// pk-liveness simulation (a fresh liveness map per `TableSpec`, via
/// [`render_mutate`]): table A's deletes and inserts can never be mistaken
/// for table B's, and each table's seeded pks start at `1` regardless of how
/// many rows an earlier table seeded — table identity, not draw order, is
/// what a pk is scoped to.
///
/// Ops are emitted one table at a time, in `tables` order: all of table 0's
/// seeds and mutates, then all of table 1's, and so on. This keeps a
/// hand-built pin's op stream legible (every op naming table N groups
/// together) and is not a claim that real traffic interleaves tables that
/// way — nothing about the model or the backend assumes any particular
/// interleaving. The drawn strategies interleave the tables afterward with
/// [`interleave_tables`] (issue #505).
///
/// Panics if `tables` or `defs` is empty, or if a `defs` entry's table index
/// is out of range for `tables` — both are generator bugs (every strategy
/// below draws `1..=MAX_TABLES`/`1..=MAX_DEFS` and indexes accordingly), not
/// conditions a caller should need to handle.
pub fn build_program_multi_with_shapes(
    tables: &[TableSpec],
    defs: &[(usize, DefShape)],
) -> Program {
    assert!(
        !tables.is_empty(),
        "build_program_multi_with_shapes: a program must draw at least one table"
    );
    assert!(
        !defs.is_empty(),
        "build_program_multi_with_shapes: a program must draw at least one definition"
    );

    let mut pool = NamePool::new();
    let mut built_tables = Vec::with_capacity(tables.len());
    let mut ops = Vec::new();

    for spec in tables {
        let row_count = spec.seed_values.len();
        assert_eq!(
            spec.text_values.len(),
            row_count,
            "build_program_multi_with_shapes: text_values must have one entry per seed row \
             ({row_count} seed rows, {} text values) — a generator bug",
            spec.text_values.len()
        );
        assert_eq!(
            spec.bool_values.len(),
            row_count,
            "build_program_multi_with_shapes: bool_values must have one entry per seed row \
             ({row_count} seed rows, {} bool values) — a generator bug",
            spec.bool_values.len()
        );
        assert_eq!(
            spec.uuid_values.len(),
            row_count,
            "build_program_multi_with_shapes: uuid_values must have one entry per seed row \
             ({row_count} seed rows, {} uuid values) — a generator bug",
            spec.uuid_values.len()
        );
        assert_eq!(
            spec.grain_values.len(),
            row_count,
            "build_program_multi_with_shapes: grain_values must have one entry per seed row \
             ({row_count} seed rows, {} grain values) — a generator bug",
            spec.grain_values.len()
        );
        assert_eq!(
            spec.rel_fk_values.len(),
            row_count,
            "build_program_multi_with_shapes: rel_fk_values must have one entry per seed row \
             ({row_count} seed rows, {} relationship FK values) — a generator bug",
            spec.rel_fk_values.len()
        );

        // Tasks B1/B4: every table gets a Text/Boolean/Uuid column and a
        // grain column, always — see the module doc comment. `Table::new`
        // builds columns in the order given, so `columns[3..=5]` are
        // Text/Boolean/Uuid and `columns[6]` is the grain column, after the
        // pk (`columns[0]`) and `c1`/`c2` (`columns[1..=2]`). The grain
        // column is appended last (rather than interleaved) so every
        // existing `columns[1..=5]` index above is untouched by this
        // widening.
        let mut source = Table::new(
            &mut pool,
            &[
                ValueType::Numeric,
                ValueType::Numeric,
                ValueType::Text,
                ValueType::Boolean,
                ValueType::Uuid,
                ValueType::Numeric,
                // Issue #34: the relationship key (index `REL_KEY_COLUMN`)
                // and foreign key (index `REL_FK_COLUMN`) columns, appended
                // last for the same "leave every existing index untouched"
                // reason the grain column was.
                ValueType::Text,
                ValueType::Text,
            ],
        );
        // The key column is what makes a to-one relationship *provably*
        // to-one: ADR-0006 derives cardinality from a primary-key/UNIQUE
        // index on the to-side column, introspected live at
        // `create_relationship` time. Without this the engine would resolve
        // every generated relationship as to-many and reject the bare
        // enrichment shape outright.
        source
            .unique_cols
            .push(source.columns[REL_KEY_COLUMN].name.clone());
        let c1 = source.columns[1].name.clone();
        let c2 = source.columns[2].name.clone();
        let text_col = source.columns[3].name.clone();
        let bool_col = source.columns[4].name.clone();
        let uuid_col = source.columns[5].name.clone();
        let grain_col = source.columns[6].name.clone();
        let rel_key_col = source.columns[REL_KEY_COLUMN].name.clone();
        let rel_fk_col = source.columns[REL_FK_COLUMN].name.clone();

        // This table's own pk-liveness simulation, independent of every
        // other table's — see [`render_mutate`] and the function doc
        // comment above. Every seeded pk starts live; every seed insert
        // always succeeds (pks are freshly minted, `1..=seed_count`, never
        // colliding *within this table*).
        let mut live: HashMap<i64, Option<i64>> = spec
            .seed_values
            .iter()
            .enumerate()
            .map(|(i, (a, _))| ((i + 1) as i64, *a))
            .collect();

        for (i, (a, b)) in spec.seed_values.iter().enumerate() {
            let pk = (i + 1) as i64;
            ops.push(Op::Insert {
                table: source.name.clone(),
                row: vec![
                    (source.pk_col.clone(), Some(pk.to_string())),
                    (c1.clone(), render(*a)),
                    (c2.clone(), render(*b)),
                    // Task B1: seeded once, here, and never touched again
                    // (except the Boolean column, which issue #255's
                    // [`Mutate::UpdateFlag`] rewrites) — see [`Mutate`]/the
                    // module doc comment's scope cut.
                    (text_col.clone(), spec.text_values[i].clone()),
                    (bool_col.clone(), spec.bool_values[i].clone()),
                    (uuid_col.clone(), spec.uuid_values[i].clone()),
                    // Task B4: likewise seeded once and never mutated — see
                    // the module doc comment's B4 scope cuts.
                    (grain_col.clone(), spec.grain_values[i].clone()),
                    // Issue #34: the key column is derived from the pk (so
                    // its UNIQUE constraint holds by construction), the
                    // foreign key is drawn (so it matches, misses, or is
                    // NULL) — see `TableSpec::rel_fk_values`.
                    (rel_key_col.clone(), Some(rel_key_value(pk))),
                    (rel_fk_col.clone(), spec.rel_fk_values[i].clone()),
                ],
                expect: OpOutcome::Succeeds,
            });
        }
        // A mutate's outcome depends on whether its pk is *currently* live
        // within this table, not just whether it started out seeded: the
        // `mutate` strategy below can draw several mutates against the same
        // pk (e.g. a `Delete` followed by an `Update`/`Delete`/
        // `DuplicateInsert` on that same now-gone pk), and each one's real
        // Postgres outcome tracks the row's live/dead state at the moment it
        // runs, not the original seed.
        for mutate in &spec.mutates {
            ops.push(render_mutate(mutate, &source, spec, &mut live));
        }

        built_tables.push(source);
    }

    let defs: Vec<TransformDef> = defs
        .iter()
        .map(|(idx, shape)| {
            let source = built_tables.get(*idx).unwrap_or_else(|| {
                panic!(
                    "build_program_multi_with_shapes: defs index {idx} out of range for {} \
                     tables — a generator bug",
                    built_tables.len()
                )
            });
            let c1 = source.columns[1].name.clone();
            let c2 = source.columns[2].name.clone();
            let text_col = source.columns[3].name.clone();
            let bool_col = source.columns[4].name.clone();
            let uuid_col = source.columns[5].name.clone();
            let grain_col = source.columns[6].name.clone();

            let (key_space, fields) = match shape {
                DefShape::OneToOne => (
                    KeySpace::OneToOne,
                    vec![
                        FieldDef {
                            name: "total".to_string(),
                            expr: Expr::BinaryOp {
                                op: Operator::Add,
                                lhs: Box::new(Expr::Column(c1)),
                                rhs: Box::new(Expr::Column(c2)),
                            },
                        },
                        // Task B1: one identity-passthrough field per new
                        // column, reusing the column's own name as the field
                        // name (`SELECT <col> AS <col>`) — the exact shape
                        // `trellis/tests/defs_backfill_direct.rs` already
                        // exercises by hand, and every def sourced from this
                        // table gets the same three, determined by the
                        // table's shape rather than drawn independently per
                        // def (matching how `total` already works). The
                        // grain column (task B4) deliberately gets no
                        // matching passthrough field here — see the module
                        // doc comment's B4 scope cuts.
                        FieldDef {
                            name: text_col.clone(),
                            expr: Expr::Column(text_col),
                        },
                        FieldDef {
                            name: bool_col.clone(),
                            expr: Expr::Column(bool_col),
                        },
                        FieldDef {
                            name: uuid_col.clone(),
                            expr: Expr::Column(uuid_col),
                        },
                    ],
                ),
                DefShape::Aggregate { functions } => {
                    // The grouping-column passthrough field
                    // (`SELECT <grain> AS <grain>`) is required —
                    // `defs::validate::validate` rejects any other
                    // expression under a grouping column's own name — and
                    // every other field is one of `functions`, drawn from
                    // `SUM`/`COUNT(*)`/`AVG`/`MIN`/`MAX` over `c1`/`c2` and
                    // `BOOL_AND`/`BOOL_OR` over the Boolean column (see
                    // [`AggregateFn`]/`aggregate_field_def`).
                    let mut fields = vec![FieldDef {
                        name: grain_col.clone(),
                        expr: Expr::Column(grain_col.clone()),
                    }];
                    fields.extend(
                        functions
                            .iter()
                            .map(|func| aggregate_field_def(*func, &c1, &c2, Some(&bool_col))),
                    );
                    (
                        KeySpace::Aggregate {
                            group_by: vec![GroupByKey::Column(grain_col)],
                        },
                        fields,
                    )
                }
            };

            TransformDef {
                target: pool.next_table_name(),
                source: source.name.clone(),
                key_space,
                fields,
                predicate: Predicate::True,
                explicit_source_schema: None,
                explicit_target_schema: None,
            }
        })
        .collect();

    // Improvement-plan task E2: every definition built here installs up
    // front (`0`) by default — deferring one is [`defer_def_install`]'s job,
    // layered on top of this builder's output exactly like
    // [`build_program_multi_with_shapes_and_derived`] layers derived fields
    // on top of it.
    let def_install_after_op = vec![0; defs.len()];

    Program {
        tables: built_tables,
        // Relationships are layered on afterward by
        // [`attach_relationship_fields`], the same "layer a widening on top
        // rather than thread it through the base builder" idiom
        // [`build_program_multi_with_shapes_and_derived`] uses for derived
        // fields.
        relationships: Vec::new(),
        defs,
        def_install_after_op,
        ops,
        restart_after_ops: Vec::new(),
        scale_out_after_ops: Vec::new(),
    }
}

/// Builds one [`FieldDef`] for a drawn [`AggregateFn`] (improvement-plan task
/// B4): the field name encodes both the function and its aggregated column
/// (`sum_c1`, `avg_c2`, `bool_and_c4`, ...) so distinct `(function, column)`
/// draws — even two functions sharing the same column, e.g. `SUM(c1)` and
/// `AVG(c1)`, which deliberately exercises `defs::ddl::count_column_names`'s
/// shared hidden-count-column path — never collide; `COUNT(*)` has no column
/// and always takes the fixed name `cnt`. `c1`/`c2`/`bool_col` are the source
/// table's own rendered column names (as everywhere else in this module);
/// `bool_col` is `None` only for [`build_bulk_insert_program`]'s all-numeric
/// table, which never builds a `BOOL_AND`/`BOOL_OR` field.
fn aggregate_field_def(func: AggregateFn, c1: &str, c2: &str, bool_col: Option<&str>) -> FieldDef {
    let column = match func {
        AggregateFn::Sum(column)
        | AggregateFn::Avg(column)
        | AggregateFn::Min(column)
        | AggregateFn::Max(column) => match column {
            AggregateColumn::C1 => c1,
            AggregateColumn::C2 => c2,
        },
        AggregateFn::BoolAnd | AggregateFn::BoolOr => bool_col.unwrap_or_else(|| {
            panic!(
                "aggregate_field_def: {} needs the source table's Boolean column, and this \
                 table has none — a generator bug",
                func.name()
            )
        }),
        AggregateFn::Count => {
            return FieldDef {
                name: "cnt".to_string(),
                expr: Expr::FunctionCall {
                    name: func.name().to_string(),
                    args: Vec::new(),
                },
            };
        }
    };
    FieldDef {
        name: format!("{}_{column}", func.name().to_lowercase()),
        expr: Expr::FunctionCall {
            name: func.name().to_string(),
            args: vec![Expr::Column(column.to_string())],
        },
    }
}

/// Improvement-plan task B2's D0 investigation ("with `>` and the five new
/// functions in play, is there now a genuine, row-data-dependent way for a
/// *valid* (post-`validate()`) definition to still error at eval time?").
///
/// **Finding: no.** Every shape [`DerivedShape`] draws stays eval-time-
/// infallible for the *values* this generator actually produces, the same
/// way AVG's hidden count-partial already keeps `+`/`AVG` divide-by-zero-free
/// (see this module's numeric-path pairing note above). Walked one shape at a
/// time (`trellis/src/defs/eval.rs`'s `apply_operator`/`apply_function`):
///
/// - **`Operator::GreaterThan`**: both operands are `trellis::numeric::Numeric`
///   (arbitrary-precision decimal, like `+`), and `Numeric::compare` never
///   errors — there is no overflow, division, or precision loss to trigger
///   one. `Postgres::numeric >` cannot error either.
/// - **`STRPOS`**: `haystack.find(needle)` is a total function over any two
///   `&str`s (`""`, no match, unicode — all handled, see
///   `trellis/tests/defs_text_functions.rs`'s `STRPOS_CASES`); it always
///   returns a `usize`, never fails. Postgres's `strpos` is equally total.
/// - **`OCTET_LENGTH`/`CHAR_LENGTH`**: `str::len`/`str::chars().count()` never
///   fail for any valid Rust `String` (which every `Text` value already is,
///   having come from a Postgres `text` column — always valid UTF-8).
/// - **`REGEXP_COUNT`**: the *only* eval-time failure mode `eval::regexp_count`
///   has is `Regex::new(pattern)` failing to compile — and `validate.rs`'s
///   `validate_regexp_pattern` already compiles the pattern with the exact
///   same `regex` crate at *validate* time and rejects the definition before
///   it ever reaches eval, for every pattern this generator draws (a string
///   literal, never a column reference, so it's always the specific literal
///   `validate` checked). A validated definition can therefore never hit a
///   pattern-compile failure at eval time. The one caveat worth naming for a
///   future widening: Rust's `regex` crate and Postgres's own ARE dialect are
///   different engines, so a pattern the Rust crate compiles is not
///   guaranteed to be one Postgres's `regexp_count` also accepts (or accepts
///   with the same semantics) — a *dialect* mismatch, not a data-dependent
///   one. This generator sidesteps that risk entirely by only ever drawing
///   `REGEXP_COUNT` patterns from `strategy::REGEXP_COUNT_PATTERN_POOL`, the
///   same dialect-common subset (literal text, `.`, `*`, `+`, `?`, `[...]`,
///   `|`, `^`, `$`) `trellis/tests/defs_text_functions.rs` already vets against
///   real Postgres — never an arbitrary pattern that might expose that gap.
///   A future widening that draws *arbitrary* regex syntax would need to
///   cross exactly this boundary (and would be the first place a genuine
///   SQL-oracle-vs-engine divergence, not a row-data-dependent eval error,
///   could show up).
/// - **`COALESCE`**: pure control flow (`eval_expr`'s `COALESCE` arm
///   short-circuits on the first non-`None` argument) — there is no
///   computation of its own to fail.
///
/// Since no shape here has a real, generator-reachable, row-data-dependent
/// error path, D0's "error-to-NULL oracle rendering" design (the plan's
/// option 2) has nothing to quarantine yet and is **not built** in this
/// session — building it now would be unused machinery with nothing to
/// exercise it, the same call this module's original numeric-`+`-only D0
/// pass made, just re-verified against the wider operator/function set this
/// task adds. The day a future widening draws something genuinely
/// data-dependent-fallible (an arbitrary regex pattern against arbitrary
/// text, a numeric cast that can overflow a *narrower* column type, division
/// by a column that can be zero, ...), this is the comment to update and the
/// decision to revisit.
///
/// **Scoped to `OneToOne` defs only (post-B4 design note).** Every
/// `DerivedShape` variant references `c1`/`c2`/the text column *bare*
/// (`Expr::Column`), never wrapped in an aggregate function — that's exactly
/// what makes a `OneToOne` field a legal `SELECT` projection. On an
/// `Aggregate` def, a bare reference to a non-grouping-key source column is
/// rejected by `defs::validate::validate`'s
/// `UngroupedColumnReference` check (every row in a group must be folded to
/// one value before it can appear in the target). So a `DerivedShape` field
/// could never be attached to an `Aggregate` def and still validate — see
/// [`build_program_multi_with_shapes_and_derived`]'s doc comment for how that
/// scoping is enforced.
#[derive(Debug, Clone, PartialEq)]
pub enum DerivedShape {
    /// `STRPOS(<text_col>, '<needle>')` — Numeric.
    Strpos { needle: String },
    /// `OCTET_LENGTH(<text_col>)` — Numeric.
    OctetLength,
    /// `CHAR_LENGTH(<text_col>)` — Numeric.
    CharLength,
    /// `REGEXP_COUNT(<text_col>, '<pattern>')` — Numeric. `pattern` is always
    /// one of `strategy::REGEXP_COUNT_PATTERN_POOL` — see this enum's own
    /// doc comment (the D0 finding) for why that pool, specifically, is what
    /// keeps this shape eval-time-infallible.
    RegexpCount { pattern: String },
    /// `COALESCE(<c1>, <fallback>)` — Numeric.
    CoalesceNumeric { fallback: i64 },
    /// `<c1> > <c2>` — Boolean; the plain, unnested `Operator::GreaterThan`
    /// case.
    PlainGreaterThan,
    /// `(<c1> + <c2>) > <c1>` — Boolean, depth 3: a `BinaryOp` nested inside
    /// another `BinaryOp` (the `c1 + c2 > c1` shape improvement-plan task B2
    /// names explicitly).
    ArithmeticGreaterThan,
    /// `STRPOS(<text_col>, '<needle>') > <threshold>` — Boolean, depth 3: a
    /// `FunctionCall` nested inside a `BinaryOp` (the `STRPOS(...) > 0` shape
    /// improvement-plan task B2 names explicitly) — the mixed
    /// operator-and-function nesting `tests/convergence.rs`'s hand-built pin
    /// exercises end-to-end.
    StrposGreaterThan { needle: String, threshold: i64 },
}

impl DerivedShape {
    /// A `STRPOS(<text_col>, '<needle>')` call, shared by [`Self::Strpos`]
    /// and [`Self::StrposGreaterThan`] so the two variants can't drift.
    fn strpos_call(text_col: &str, needle: &str) -> Expr {
        Expr::FunctionCall {
            name: "STRPOS".to_string(),
            args: vec![
                Expr::Column(text_col.to_string()),
                Expr::StringLiteral(needle.to_string()),
            ],
        }
    }

    /// Builds this shape's [`Expr`] tree against a table's own `c1`/`c2`
    /// (numeric) and `text_col` (text) column names — the same three columns
    /// [`build_program_multi_with_shapes_and_derived`] already threads
    /// through for the `total`/passthrough fields.
    pub fn build_expr(&self, c1: &str, c2: &str, text_col: &str) -> Expr {
        match self {
            DerivedShape::Strpos { needle } => Self::strpos_call(text_col, needle),
            DerivedShape::OctetLength => Expr::FunctionCall {
                name: "OCTET_LENGTH".to_string(),
                args: vec![Expr::Column(text_col.to_string())],
            },
            DerivedShape::CharLength => Expr::FunctionCall {
                name: "CHAR_LENGTH".to_string(),
                args: vec![Expr::Column(text_col.to_string())],
            },
            DerivedShape::RegexpCount { pattern } => Expr::FunctionCall {
                name: "REGEXP_COUNT".to_string(),
                args: vec![
                    Expr::Column(text_col.to_string()),
                    Expr::StringLiteral(pattern.clone()),
                ],
            },
            DerivedShape::CoalesceNumeric { fallback } => Expr::FunctionCall {
                name: "COALESCE".to_string(),
                args: vec![
                    Expr::Column(c1.to_string()),
                    Expr::NumberLiteral(fallback.to_string()),
                ],
            },
            DerivedShape::PlainGreaterThan => Expr::BinaryOp {
                op: Operator::GreaterThan,
                lhs: Box::new(Expr::Column(c1.to_string())),
                rhs: Box::new(Expr::Column(c2.to_string())),
            },
            DerivedShape::ArithmeticGreaterThan => Expr::BinaryOp {
                op: Operator::GreaterThan,
                lhs: Box::new(Expr::BinaryOp {
                    op: Operator::Add,
                    lhs: Box::new(Expr::Column(c1.to_string())),
                    rhs: Box::new(Expr::Column(c2.to_string())),
                }),
                rhs: Box::new(Expr::Column(c1.to_string())),
            },
            DerivedShape::StrposGreaterThan { needle, threshold } => Expr::BinaryOp {
                op: Operator::GreaterThan,
                lhs: Box::new(Self::strpos_call(text_col, needle)),
                rhs: Box::new(Expr::NumberLiteral(threshold.to_string())),
            },
        }
    }
}

/// [`build_program_multi_with_shapes`]'s general form, folding in
/// improvement-plan task B2's "derived" field on top of task B4's per-def
/// shape choice: `defs[j]` is `(source table index, shape)` exactly as
/// [`build_program_multi_with_shapes`] takes, and `derived[j]` is the
/// optional [`DerivedShape`] to append as one more calculated field (named
/// `"derived"`) to definition `j`.
///
/// **Why `Option`, and why the restriction it encodes.** A derived field is
/// only ever attached when `derived[j]` is `Some` *and* `defs[j]`'s shape is
/// `DefShape::OneToOne` — see [`DerivedShape`]'s own doc comment for why an
/// `Aggregate` def can never legally carry one (every `DerivedShape` variant
/// references a source column bare, and `defs::validate::validate`
/// rejects a bare, ungrouped column reference on an `Aggregate` def). This
/// function enforces that pairing with an assertion rather than silently
/// ignoring a `Some` paired with `DefShape::Aggregate`: a caller that drew
/// one anyway has a generator bug worth surfacing loudly (design doc §2
/// "refuse to guess"), not a case worth quietly downgrading to a no-op.
/// `strategy::trivial_program_with` below never draws that combination
/// (`def_shape_and_derived` only ever pairs a `Some` with `OneToOne`).
///
/// Deliberately layered *on top of* [`build_program_multi_with_shapes`]
/// (calling it, then pushing one field per eligible def) rather than folded
/// into it: every existing caller of `build_program_multi`/
/// `build_program_multi_with_shapes` — every hand-built pin across
/// `generative/tests/*.rs`, this module's own unit tests, `tests/coverage.rs`'s
/// exact-field-count assertions — keeps its exact prior behavior (task B3's
/// fixed `total`/passthrough shape and task B4's aggregate shape, both
/// untouched), and only the (new) default proptest strategy (see
/// `strategy::trivial_program_with`) actually draws a `derived` field.
///
/// Panics if `derived.len() != defs.len()` — a generator bug, matching this
/// module's other length-contract checks ([`TableSpec`]'s
/// `text_values`/`bool_values`/`uuid_values`/`grain_values`).
pub fn build_program_multi_with_shapes_and_derived(
    tables: &[TableSpec],
    defs: &[(usize, DefShape)],
    derived: &[Option<DerivedShape>],
) -> Program {
    assert_eq!(
        derived.len(),
        defs.len(),
        "build_program_multi_with_shapes_and_derived: derived must have one entry per \
         definition ({} definitions, {} derived slots) — a generator bug",
        defs.len(),
        derived.len()
    );

    let mut program = build_program_multi_with_shapes(tables, defs);

    // Looked up by table name up front, before the mutable loop over
    // `program.defs` below, so the two loops don't need to borrow
    // `program.tables` and `program.defs` simultaneously.
    let columns_by_table: HashMap<String, (String, String, String)> = program
        .tables
        .iter()
        .map(|table| {
            (
                table.name.clone(),
                (
                    table.columns[1].name.clone(),
                    table.columns[2].name.clone(),
                    table.columns[3].name.clone(),
                ),
            )
        })
        .collect();

    for (def, shape) in program.defs.iter_mut().zip(derived) {
        let Some(shape) = shape else {
            continue;
        };
        assert!(
            def.key_space == KeySpace::OneToOne,
            "build_program_multi_with_shapes_and_derived: a derived field was requested for \
             def {:?} (target {:?}), whose key_space is {:?} — derived fields are scoped to \
             OneToOne defs only (see this function's and DerivedShape's doc comments); an \
             Aggregate def's non-grouping fields must be aggregate function calls, and \
             validate() rejects a bare/derived reference to an ungrouped column",
            def.source,
            def.target,
            def.key_space
        );
        let (c1, c2, text_col) = columns_by_table.get(&def.source).unwrap_or_else(|| {
            panic!(
                "build_program_multi_with_shapes_and_derived: def.source {:?} names no table in \
                 the program — a generator bug",
                def.source
            )
        });
        def.fields.push(FieldDef {
            name: "derived".to_string(),
            expr: shape.build_expr(c1, c2, text_col),
        });
    }

    program
}

// ---------------------------------------------------------------------
// Issue #34: relationship edges in the program generator (ADR-0006).
// ---------------------------------------------------------------------

/// The aggregate functions a generated relationship path may be wrapped in
/// (issue #34). A deliberately narrower set than [`AggregateFn`]: the
/// argument is always a `<rel>.<column>` path (the to-side table's numeric
/// `c1`), never `c1`/`c2`, so there is no `COUNT(*)` form and no
/// `BOOL_AND`/`BOOL_OR` (no Boolean path is drawn). [`RelAggregateFn::Count`]
/// is `COUNT(<rel>.<column>)`, the non-`NULL` count of the related values.
///
/// Every variant is drawn both over a **to-many** path in a row-grain
/// (`OneToOne`) definition and over a **to-one** path inside a `GROUP BY`
/// definition — see [`strategy::rel_aggregate_fn`]. (Before #120 the parser
/// accepted only `COUNT(*)` inside a `GROUP BY` definition, so `Count` was
/// kept out of [`RelFieldKind::ToOneAggregate`]; issue #294 lifted that.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelAggregateFn {
    Sum,
    Min,
    Max,
    Avg,
    Count,
}

impl RelAggregateFn {
    /// This function's canonical uppercased name, as
    /// `trellis::dev::defs::registry::AGGREGATE_FUNCTIONS` spells it.
    pub fn name(self) -> &'static str {
        match self {
            RelAggregateFn::Sum => "SUM",
            RelAggregateFn::Min => "MIN",
            RelAggregateFn::Max => "MAX",
            RelAggregateFn::Avg => "AVG",
            RelAggregateFn::Count => "COUNT",
        }
    }
}

/// Which of the three **engine-supported** relationship-reference shapes a
/// generated definition takes (issue #34). Every other combination of
/// cardinality × wrapping × key-space is rejected by
/// `defs::validate`, and an install rejection is a hard failure,
/// never a skip (design doc §3), so those are structurally unrepresentable
/// here rather than merely undrawn:
///
/// | key-space | to-one bare | to-one in aggregate | to-many bare | to-many in aggregate |
/// |---|---|---|---|---|
/// | `OneToOne` | [`RelFieldKind::ToOneBare`] | rejected (`RelationshipToOneWrappedInAggregate`) | rejected (`RelationshipToManyRequiresAggregate`) | [`RelFieldKind::ToManyAggregate`] |
/// | `Aggregate` | rejected (`UngroupedRelationshipReference`) | [`RelFieldKind::ToOneAggregate`] | rejected | rejected (`RelationshipPathInAggregate`) |
///
/// The `Aggregate` row's rejected corners are worth naming explicitly
/// because they are the *newest* boundary: aggregating a to-one path inside
/// a `GROUP BY` only became legal on this branch, while a bare path and a
/// nested to-many-inside-`GROUP BY` (aggregating an aggregate) both remain
/// unimplemented.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelFieldKind {
    /// A bare `<rel>.<column>` enrichment on a `OneToOne` definition: a
    /// `LEFT JOIN` to the related row, `NULL` when the FK matches nothing or
    /// is itself `NULL` (ADR-0006's to-one nullability rule).
    ToOneBare,
    /// `<fn>(<rel>.<column>)` over a **to-many** relationship on a
    /// `OneToOne` definition: a correlated aggregate over the related rows,
    /// with Postgres's empty-set semantics (`COUNT` → `0`, everything else →
    /// `NULL`).
    ToManyAggregate(RelAggregateFn),
    /// `<fn>(<rel>.<column>)` over a **to-one** relationship inside a
    /// `GROUP BY` definition: an ordinary aggregate over the `LEFT JOIN`ed
    /// column, folding one related value per source row.
    ToOneAggregate(RelAggregateFn),
}

impl RelFieldKind {
    /// The [`Cardinality`] the relationship this field reads must have.
    fn cardinality(self) -> Cardinality {
        match self {
            RelFieldKind::ToOneBare | RelFieldKind::ToOneAggregate(_) => Cardinality::ToOne,
            RelFieldKind::ToManyAggregate(_) => Cardinality::ToMany,
        }
    }

    /// Whether this shape belongs on a `GROUP BY` definition (`true`) or a
    /// row-grain one (`false`) — see the variant table on [`RelFieldKind`].
    fn wants_aggregate_key_space(self) -> bool {
        matches!(self, RelFieldKind::ToOneAggregate(_))
    }
}

/// One definition's optional relationship enrichment (issue #34):
/// *which* other table it relates to, and *how* it reads it.
///
/// `to_table` is an index into the program's `tables`, and must be **greater
/// than the definition's own source table's index**. That ordering rule is
/// what keeps the cross-table dependency graph acyclic without any cycle
/// detection of this crate's own: ADR-0006 makes every relationship an edge
/// in the same graph as transforms, and cycles are rejected at definition
/// time — so two relationships pointing at each other (`t0 → t1` and
/// `t1 → t0`) would be an install rejection, i.e. a hard failure. Ordering
/// every edge low-index → high-index makes a cycle unrepresentable.
/// [`attach_relationship_fields`] asserts it rather than trusting callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelFieldSpec {
    pub to_table: usize,
    pub kind: RelFieldKind,
}

/// Layers relationship declarations and the calculated fields that read them
/// onto an already-built `program` (issue #34): `rel_fields[j]` is
/// definition `j`'s optional enrichment, index-aligned with `program.defs`
/// exactly as [`build_program_multi_with_shapes_and_derived`]'s `derived` is.
///
/// Relationships are **deduplicated by endpoint**: two definitions reading
/// the same `(from_table, from_col, to_table, to_col)` share one declaration
/// rather than each minting a differently-named duplicate. That is both more
/// realistic (ADR-0006's whole point is that a relationship is reusable
/// across transforms) and closer to the interesting engine path — one
/// relationship's reverse propagation feeding several targets.
///
/// Each relationship's endpoints are fixed by the drawn [`RelFieldKind`]'s
/// cardinality, using the two columns issue #34 gives every table:
///
/// * **to-one** — `FROM <source>.<fk> TO <to>.<key>`; the to-side key column
///   carries a real `UNIQUE` constraint ([`crate::model::Table::unique_cols`]),
///   which is what makes the engine's live `pg_catalog` introspection resolve
///   it as to-one.
/// * **to-many** — `FROM <source>.<key> TO <to>.<fk>`; the to-side FK column
///   has no unique constraint, so the same introspection resolves it as
///   to-many.
///
/// Both endpoints are `Text`, the only one of this crate's value types that
/// is both in [`TEXT_STABLE_JOIN_KEY_TYPES`] and freely drawable — see that
/// constant for why the engine rejects anything else.
///
/// The referenced to-side column is always that table's `c1` (Numeric), so
/// every shape — bare enrichment and all five aggregate functions — is
/// type-correct without the spec having to carry a column choice too.
///
/// # Panics
///
/// On any index/shape mismatch — an out-of-range or non-increasing
/// `to_table`, a `rel_fields` list of the wrong length, or a [`RelFieldKind`]
/// paired with the wrong key-space. All generator bugs, all of which would
/// otherwise reach the engine as an install rejection.
pub fn attach_relationship_fields(
    mut program: Program,
    rel_fields: &[Option<RelFieldSpec>],
) -> Program {
    assert_eq!(
        rel_fields.len(),
        program.defs.len(),
        "attach_relationship_fields: rel_fields must have one entry per definition ({} \
         definitions, {} relationship slots) — a generator bug",
        program.defs.len(),
        rel_fields.len()
    );

    // Snapshotted up front so the mutable loop over `program.defs` below
    // doesn't need to borrow `program.tables` at the same time.
    let table_index: HashMap<String, usize> = program
        .tables
        .iter()
        .enumerate()
        .map(|(i, t)| (t.name.clone(), i))
        .collect();
    let tables = program.tables.clone();

    let mut relationships: Vec<Relationship> = std::mem::take(&mut program.relationships);

    for (def, spec) in program.defs.iter_mut().zip(rel_fields) {
        let Some(spec) = spec else {
            continue;
        };
        let from_index = *table_index.get(&def.source).unwrap_or_else(|| {
            panic!(
                "attach_relationship_fields: def.source {:?} names no table in the program — a \
                 generator bug",
                def.source
            )
        });
        assert!(
            spec.to_table < tables.len(),
            "attach_relationship_fields: to_table index {} out of range for {} tables — a \
             generator bug",
            spec.to_table,
            tables.len()
        );
        assert!(
            spec.to_table > from_index,
            "attach_relationship_fields: a relationship must point from a lower-indexed table \
             to a higher-indexed one (got {from_index} -> {}), so the cross-table dependency \
             graph stays acyclic by construction — a generator bug (see RelFieldSpec)",
            spec.to_table
        );
        let is_aggregate_def = matches!(def.key_space, KeySpace::Aggregate { .. });
        assert_eq!(
            spec.kind.wants_aggregate_key_space(),
            is_aggregate_def,
            "attach_relationship_fields: relationship shape {:?} does not belong on a \
             definition whose key_space is {:?} (target {:?}) — see RelFieldKind's shape table; \
             the engine would reject this at validation time, and an install rejection is a \
             hard failure, never a skip",
            spec.kind,
            def.key_space,
            def.target
        );

        let from_table = &tables[from_index];
        let to_table = &tables[spec.to_table];
        let cardinality = spec.kind.cardinality();
        let (from_col, to_col) = match cardinality {
            Cardinality::ToOne => (
                from_table.columns[REL_FK_COLUMN].name.clone(),
                to_table.columns[REL_KEY_COLUMN].name.clone(),
            ),
            Cardinality::ToMany => (
                from_table.columns[REL_KEY_COLUMN].name.clone(),
                to_table.columns[REL_FK_COLUMN].name.clone(),
            ),
        };

        let existing = relationships.iter().find(|r| {
            r.from_table == from_table.name
                && r.from_col == from_col
                && r.to_table == to_table.name
                && r.to_col == to_col
        });
        let rel_name = match existing {
            Some(rel) => rel.name.clone(),
            None => {
                let name = format!("r{}", relationships.len());
                relationships.push(Relationship {
                    name: name.clone(),
                    from_table: from_table.name.clone(),
                    from_col,
                    to_table: to_table.name.clone(),
                    to_col,
                    cardinality,
                });
                name
            }
        };

        // Always the to-side table's `c1`: Numeric, nullable, and mutated by
        // the op stream, so a to-side update really does have to propagate
        // back into this field (ADR-0006's reverse propagation).
        let path = Expr::RelationshipPath {
            rel: rel_name,
            column: to_table.columns[1].name.clone(),
        };
        let field = match spec.kind {
            RelFieldKind::ToOneBare => FieldDef {
                name: "rel_enrich".to_string(),
                expr: path,
            },
            RelFieldKind::ToManyAggregate(func) | RelFieldKind::ToOneAggregate(func) => FieldDef {
                name: "rel_agg".to_string(),
                expr: Expr::FunctionCall {
                    name: func.name().to_string(),
                    args: vec![path],
                },
            },
        };
        def.fields.push(field);
    }

    program.relationships = relationships;
    program
}

/// [`build_program_multi_with_shapes_and_derived`]'s relationship-aware form
/// (issue #34): the same three lists, plus `rel_fields[j]` giving definition
/// `j`'s optional relationship enrichment (see [`RelFieldSpec`]).
///
/// A thin composition — build, layer derived fields, layer relationship
/// fields — kept as its own entry point so `rel_fields` doesn't have to be
/// threaded through the two dozen existing call sites of the narrower
/// builders, exactly as [`build_program_multi_with_derived`]'s doc comment
/// argues for its own signature.
pub fn build_program_multi_with_relationships(
    tables: &[TableSpec],
    defs: &[(usize, DefShape)],
    derived: &[Option<DerivedShape>],
    rel_fields: &[Option<RelFieldSpec>],
) -> Program {
    let program = build_program_multi_with_shapes_and_derived(tables, defs, derived);
    attach_relationship_fields(program, rel_fields)
}

// ---------------------------------------------------------------------
// Issue #138 (epic #127 phase 2): the relationship-delta interleaving
// scenarios. #34 already gave every table a to-one-capable relationship
// key/FK pair and #34/#132 the reverse-propagation machinery that keeps a
// to-one enrichment settled; what #138 asks the generative suite to stress
// is *timing* — a parent (to-side) change landing close enough to a
// from-side change on the *same* parent that the two can race across a
// seal boundary, within one batch, or through an out-of-order segment drain
// (see `trellis/tests/spike_102.rs`'s `spike_a2_a_from_side_insert_drains_before_the_parents_reverse_work`,
// branch `spike/issue-102-validation-v2`, for the real-engine scenario
// shape this mirrors).
//
// [`build_program_multi_with_shapes_and_derived`] can't produce that shape
// on its own: it emits one table's *entire* seed+mutate stream before the
// next table's (see `render_mutate`'s call site above), and a relationship
// always points from a lower-indexed table to a higher-indexed one, so the
// parent table's ops always come strictly after every from-side op. Issue
// #505's [`interleave_tables`] now lets the drawn strategies reach it at
// random; the scenarios below stay as its deterministic pins.
// [`build_relationship_interleaving_scenario`] reuses the builder for the
// base schema/relationship/definition (via
// [`build_program_multi_with_relationships`]), then appends the two
// critical, deliberately-adjacent ops by hand.
// ---------------------------------------------------------------------

/// Which shape the parent-side and from-side halves of a
/// [`build_relationship_interleaving_scenario`] take. The first is the
/// canonical shape (the parent's own enriched field changes while a
/// from-side row starts pointing at it); the remaining four are #138's own
/// named variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelInterleavingVariant {
    /// The parent (pk 1, key `k1`) already exists; its own `c1` changes
    /// while a brand-new from-side row is inserted pointing at it.
    ParentFieldUpdate,
    /// The parent does not exist yet: it is inserted (a fresh key, `k3`)
    /// while a brand-new from-side row that names that same fresh key is
    /// inserted alongside it.
    ParentInsert,
    /// The parent (pk 1, key `k1`) is deleted while a still-live from-side
    /// row (previously pointing at the *other* seeded parent) is
    /// re-pointed onto it in the same window.
    ParentDelete,
    /// The parent's own `c1` changes while, in the same window, the
    /// from-side row that used to point at it re-points to a key that
    /// names no row at all.
    RepointToNonexistentParent,
    /// The parent's own `c1` changes while, in the same window, the
    /// from-side row that used to point at it has its foreign key set to
    /// `NULL`.
    RepointToNullParent,
}

/// One [`Program`] built by [`build_relationship_interleaving_scenario`],
/// plus the indices of its two deliberately-adjacent critical ops: the
/// parent-side change ([`RelInterleavingScenario::parent_op`]) and the
/// from-side change on the same parent
/// ([`RelInterleavingScenario::from_side_op`], always `parent_op + 1`).
/// Every op before `parent_op` is ordinary seeding, safe to apply and
/// quiesce on one at a time; a caller stressing #138's race applies
/// `parent_op` and `from_side_op` back-to-back with no intervening
/// `quiesce()` (and, for a deterministic seal-boundary reproduction,
/// forces a seal between them — see `generative::backend::ManualBackend::
/// force_seal_active_segment`).
#[derive(Debug, Clone)]
pub struct RelInterleavingScenario {
    pub program: Program,
    pub parent_op: usize,
    pub from_side_op: usize,
}

/// Builds the base two-table relationship program every
/// [`RelInterleavingVariant`] shares (issue #138): `tables[0]` is the
/// from-side ("child"/`post_tags`-shaped) table, `tables[1]` the to-side
/// ("parent"/`posts`-shaped) one, joined by a to-one bare enrichment
/// (`rel_enrich = <rel>.c1`, exactly [`RelFieldKind::ToOneBare`]) — the
/// shape that reads the parent's own `c1` back onto every from-side row,
/// so a parent-side `c1` change is exactly the kind of change whose
/// reverse propagation this whole scenario stresses.
///
/// Seeds three from-side rows (pk 1 -> key `k1`, pk 2 -> key `k2`, pk 3 ->
/// `NULL`) and two parent rows (pk 1 = key `k1`/`c1` 100, pk 2 = key
/// `k2`/`c1` 200) — parent 2 and from-side rows 2/3 are decoys that no
/// variant's critical section ever touches, so a divergence localizes to
/// the row the scenario actually interleaves rather than a blanket
/// relationship bug.
fn base_interleaving_program() -> Program {
    let from_side = TableSpec {
        seed_values: vec![(Some(10), None), (Some(20), None), (Some(30), None)],
        text_values: vec![None; 3],
        bool_values: vec![None; 3],
        uuid_values: vec![None; 3],
        grain_values: vec![None; 3],
        rel_fk_values: vec![Some("k1".to_string()), Some("k2".to_string()), None],
        mutates: Vec::new(),
    };
    let to_side = TableSpec {
        seed_values: vec![(Some(100), None), (Some(200), None)],
        text_values: vec![None; 2],
        bool_values: vec![None; 2],
        uuid_values: vec![None; 2],
        grain_values: vec![None; 2],
        rel_fk_values: vec![None; 2],
        mutates: Vec::new(),
    };
    build_program_multi_with_relationships(
        &[from_side, to_side],
        &[(0, DefShape::OneToOne)],
        &[None],
        &[Some(RelFieldSpec {
            to_table: 1,
            kind: RelFieldKind::ToOneBare,
        })],
    )
}

/// Builds one [`RelInterleavingScenario`] for `variant` (issue #138). See
/// [`RelInterleavingVariant`] for what each variant's two critical ops are.
pub fn build_relationship_interleaving_scenario(
    variant: RelInterleavingVariant,
) -> RelInterleavingScenario {
    let mut program = base_interleaving_program();

    let from_table = program.tables[0].name.clone();
    let from_pk_col = program.tables[0].pk_col.clone();
    let from_c1_col = program.tables[0].columns[1].name.clone();
    let from_fk_col = program.tables[0].columns[REL_FK_COLUMN].name.clone();
    let to_table = program.tables[1].name.clone();
    let to_pk_col = program.tables[1].pk_col.clone();
    let to_c1_col = program.tables[1].columns[1].name.clone();
    let to_key_col = program.tables[1].columns[REL_KEY_COLUMN].name.clone();

    match variant {
        RelInterleavingVariant::ParentFieldUpdate => {
            program.ops.push(Op::Update {
                table: to_table,
                pk: "1".to_string(),
                changes: vec![(to_c1_col, Some("999".to_string()))],
                expect: OpOutcome::Succeeds,
            });
            program.ops.push(Op::Insert {
                table: from_table,
                row: vec![
                    (from_pk_col, Some("4".to_string())),
                    (from_c1_col, Some("40".to_string())),
                    (from_fk_col, Some("k1".to_string())),
                ],
                expect: OpOutcome::Succeeds,
            });
        }
        RelInterleavingVariant::ParentInsert => {
            let fresh_key = rel_key_value(3);
            program.ops.push(Op::Insert {
                table: to_table,
                row: vec![
                    (to_pk_col, Some("3".to_string())),
                    (to_c1_col, Some("777".to_string())),
                    (to_key_col, Some(fresh_key.clone())),
                ],
                expect: OpOutcome::Succeeds,
            });
            program.ops.push(Op::Insert {
                table: from_table,
                row: vec![
                    (from_pk_col, Some("4".to_string())),
                    (from_c1_col, Some("40".to_string())),
                    (from_fk_col, Some(fresh_key)),
                ],
                expect: OpOutcome::Succeeds,
            });
        }
        RelInterleavingVariant::ParentDelete => {
            program.ops.push(Op::Delete {
                table: to_table,
                pk: "1".to_string(),
                expect: OpOutcome::Succeeds,
            });
            // From-side pk 2 was pointing at the *other*, untouched parent
            // (key `k2`); re-pointing it onto the parent being deleted in
            // the same window is the sharper case: its enrichment must
            // land NULL, never a stale copy of the deleted parent's `c1`.
            program.ops.push(Op::Update {
                table: from_table,
                pk: "2".to_string(),
                changes: vec![(from_fk_col, Some("k1".to_string()))],
                expect: OpOutcome::Succeeds,
            });
        }
        RelInterleavingVariant::RepointToNonexistentParent => {
            program.ops.push(Op::Update {
                table: to_table,
                pk: "1".to_string(),
                changes: vec![(to_c1_col, Some("999".to_string()))],
                expect: OpOutcome::Succeeds,
            });
            program.ops.push(Op::Update {
                table: from_table,
                pk: "1".to_string(),
                changes: vec![(from_fk_col, Some("k9".to_string()))],
                expect: OpOutcome::Succeeds,
            });
        }
        RelInterleavingVariant::RepointToNullParent => {
            program.ops.push(Op::Update {
                table: to_table,
                pk: "1".to_string(),
                changes: vec![(to_c1_col, Some("999".to_string()))],
                expect: OpOutcome::Succeeds,
            });
            program.ops.push(Op::Update {
                table: from_table,
                pk: "1".to_string(),
                changes: vec![(from_fk_col, None)],
                expect: OpOutcome::Succeeds,
            });
        }
    }

    let from_side_op = program.ops.len() - 1;
    let parent_op = from_side_op - 1;
    RelInterleavingScenario {
        program,
        parent_op,
        from_side_op,
    }
}

// ---------------------------------------------------------------------
// Issue #505: interleaving the per-table op streams.
//
// The builders above emit each table's whole stream before the next
// table's, and a relationship always points to a higher-indexed table, so
// in a table-ordered program every parent-side op comes after every
// from-side op. The engine's reverse path (a parent change recomputing
// from-side targets) then only ever runs against a settled from-side, and a
// from-side change never lands after a parent change it has to observe.
//
// The only ordering a program really needs is *within* a table: its seeds
// come first (every mutate draws its pk from the seeded range, and the
// pk-liveness simulation in `render_mutate` assumes that order), and its
// mutates keep their drawn order (that simulation's `expect`s depend on
// it). Across tables there is nothing to respect: a relationship is a
// logical join, not a Postgres foreign key, so a from-side row may name a
// parent that doesn't exist yet (the join simply misses until it does), and
// no op on one table changes another table's rows. That is also what
// `ops_commute` already says: ops on different tables always commute at the
// source.
//
// So a program is a set of per-table streams ([`table_streams`]) plus one
// linearization of them ([`interleave_tables`]). The concurrent tier (#557)
// can take the same streams and issue each from its own task, since the
// streams are exactly the ordering constraints; the serial tiers draw one
// linearization per case.
// ---------------------------------------------------------------------

/// `program.ops`' indices split into one stream per `program.tables` entry,
/// in table order, each stream in its original op order (issue #505). The
/// only ordering constraints a generated program carries are within a
/// stream; see the section comment above.
///
/// # Panics
///
/// If an op names a table that isn't in `program.tables`, a generator bug.
pub fn table_streams(program: &Program) -> Vec<Vec<usize>> {
    let mut streams = vec![Vec::new(); program.tables.len()];
    for (index, op) in program.ops.iter().enumerate() {
        let table = op_table(op);
        let position = program
            .tables
            .iter()
            .position(|t| t.name == table)
            .unwrap_or_else(|| {
                panic!(
                    "table_streams: op {index} names table {table:?}, which the program doesn't \
                     have — a generator bug"
                )
            });
        streams[position].push(index);
    }
    streams
}

/// How many values each [`interleave_tables`] schedule entry is drawn from:
/// divisible by every possible count of tables with ops still to emit
/// (`1..=MAX_TABLES`), so `pick % remaining` is uniform whatever the count.
pub const INTERLEAVE_PICK_RANGE: usize = 6;
const _: () =
    assert!(INTERLEAVE_PICK_RANGE.is_multiple_of(2) && INTERLEAVE_PICK_RANGE.is_multiple_of(3));

/// Reorders `program.ops` into one interleaving of its per-table streams
/// ([`table_streams`]), keeping every table's own ops in order (issue #505).
///
/// Step `i` emits the next op of one of the tables that still has ops left:
/// `schedule[i] % remaining` picks among those tables in table order, and a
/// missing entry counts as `0`. An all-zero schedule therefore reproduces
/// the table-by-table order exactly, which is what proptest shrinks a drawn
/// schedule toward: a shrunk counterexample only stays interleaved where the
/// interleaving is needed to fail.
///
/// Layered on top of an already-built program, like [`defer_def_install`];
/// apply it *before* anything that records an op index.
///
/// # Panics
///
/// If `program` already has a deferred install, a restart or a scale-out:
/// each is keyed to an op index this function would silently re-point.
pub fn interleave_tables(mut program: Program, schedule: &[usize]) -> Program {
    assert!(
        program.def_install_after_op.iter().all(|&at| at == 0)
            && program.restart_after_ops.is_empty()
            && program.scale_out_after_ops.is_empty(),
        "interleave_tables: interleave before scheduling anything by op index — a generator bug"
    );
    let mut streams: Vec<std::collections::VecDeque<usize>> = table_streams(&program)
        .into_iter()
        .map(Into::into)
        .collect();
    let mut order = Vec::with_capacity(program.ops.len());
    for step in 0..program.ops.len() {
        let remaining: Vec<usize> = (0..streams.len())
            .filter(|&t| !streams[t].is_empty())
            .collect();
        let pick = schedule.get(step).copied().unwrap_or(0) % remaining.len();
        let next = streams[remaining[pick]]
            .pop_front()
            .expect("a remaining stream is non-empty");
        order.push(next);
    }
    reorder_ops(&mut program, order);
    program
}

/// Puts `program.ops` in `order`, a permutation of their indices.
fn reorder_ops(program: &mut Program, order: Vec<usize>) {
    let mut ops: Vec<Option<Op>> = std::mem::take(&mut program.ops)
        .into_iter()
        .map(Some)
        .collect();
    program.ops = order
        .into_iter()
        .map(|i| ops[i].take().expect("each op index is emitted once"))
        .collect();
}

/// Of the streams with ops left to emit, the one furthest behind its own
/// share: the smallest fraction `emitted[t] / totals[t]`, the first on a tie
/// (issue #557). `None` once every stream is done.
fn furthest_behind(totals: &[usize], emitted: &[usize]) -> Option<usize> {
    // Smallest fraction emitted, compared without division.
    (0..totals.len())
        .filter(|&t| emitted[t] < totals[t])
        .min_by(|&a, &b| (emitted[a] * totals[b]).cmp(&(emitted[b] * totals[a])))
}

// ---------------------------------------------------------------------
// Issue #557 part 3b: cooling keys.
//
// The hot-key tier's keys keep changing until their burst ends, so a batch
// holding a key's change is nearly always followed by a later batch that
// rewrites the key. A 1-1 write that lands out of order (#344's bug) is then
// healed before the burst is checked. What a stale write needs to survive
// is a key that goes quiet mid-burst: its last change sits in one batch, an
// older change in another, both drain while hot traffic keeps the workers
// busy, and nothing writes the key again.
//
// A cooling table is that population: hundreds of keys, each inserted and
// then written one to three more times close together, and never again.
// [`place_cooling_keys`] places each key's writes inside one burst, with the
// last one in the burst's middle rather than its tail, and spreads the rest
// of the program (the hot table, and any other) evenly around them.
// ---------------------------------------------------------------------

/// Where one cooling key's writes go (issue #557 part 3b). Plain numbers,
/// resolved against the program by [`place_cooling_keys`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoolingPlace {
    /// Picks the burst, modulo how many there are.
    pub burst: usize,
    /// Where in the burst the key's last write goes, in thousandths of the
    /// burst's ops.
    pub last: u16,
    /// How far apart the key's writes are, in thousandths of the burst's
    /// ops (at least one op).
    pub gap: u16,
}

/// Orders `program.ops` so every key of table `cooling_table` has its writes
/// where `places` says (issue #557 part 3b), one [`CoolingPlace`] per key in
/// order of the key's first op. The other tables' ops are spread evenly
/// through the rest of the program, as the hot-key tier spreads them, each
/// table keeping its own order.
///
/// Each key's writes go in the burst its place picks (bursts being
/// consecutive runs of `burst_size` ops, as [`concurrent_plan`] cuts them,
/// leaving out a short last one): the last at `last` thousandths of the way
/// through, each earlier one `gap` thousandths before the next, pulled
/// closer together if they would start before the burst does. Where two writes want the same position the
/// later-placed one moves up a slot, so a busy stretch drifts a little later;
/// a key's own writes always stay in order.
///
/// # Panics
///
/// If `places` doesn't have one entry per cooling key, `burst_size` is `0`,
/// or `program` already schedules anything by op index (as
/// [`interleave_tables`]).
pub fn place_cooling_keys(
    mut program: Program,
    cooling_table: usize,
    places: &[CoolingPlace],
    burst_size: usize,
) -> Program {
    assert!(
        program.def_install_after_op.iter().all(|&at| at == 0)
            && program.restart_after_ops.is_empty()
            && program.scale_out_after_ops.is_empty(),
        "place_cooling_keys: place keys before scheduling anything by op index — a generator bug"
    );
    assert!(
        burst_size > 0,
        "place_cooling_keys: burst_size must be at least 1"
    );
    let mut streams = table_streams(&program);
    let cooling = streams.remove(cooling_table);
    let spec = &program.tables[cooling_table];

    // The cooling table's ops, key by key, each key's in program order.
    let mut keys: Vec<Vec<usize>> = Vec::new();
    let mut key_of: HashMap<String, usize> = HashMap::new();
    for index in cooling {
        let pk = op_pk_value(&program.ops[index], spec)
            .expect("a cooling table takes only single-row ops — a generator bug");
        let next = keys.len();
        let key = *key_of.entry(pk).or_insert(next);
        if key == keys.len() {
            keys.push(Vec::new());
        }
        keys[key].push(index);
    }
    assert_eq!(
        keys.len(),
        places.len(),
        "place_cooling_keys: one place per cooling key — a generator bug"
    );

    let total = program.ops.len();
    // Full bursts only: a short last burst has no middle to speak of.
    let bursts = (total / burst_size).max(1);
    // (wanted position, key, op index), sorted: a key's positions strictly
    // increase, so its ops stay in order.
    let mut wanted: Vec<(usize, usize, usize)> = Vec::new();
    for (key, (ops, place)) in keys.iter().zip(places).enumerate() {
        let start = (place.burst % bursts) * burst_size;
        let len = burst_size.min(total - start);
        let last = start + usize::from(place.last.min(1000)) * (len - 1) / 1000;
        let spans = ops.len() - 1;
        let mut gap = (usize::from(place.gap) * len / 1000).max(1);
        if let Some(room) = (last - start).checked_div(spans) {
            gap = gap.min(room).max(1);
        }
        for (j, &index) in ops.iter().enumerate() {
            wanted.push((last.saturating_sub((spans - j) * gap), key, index));
        }
    }
    wanted.sort_unstable();

    // The other tables, spread evenly, fill every slot a cooling op doesn't
    // want.
    let totals: Vec<usize> = streams.iter().map(Vec::len).collect();
    let mut emitted = vec![0usize; streams.len()];
    let mut wanted = wanted.into_iter().peekable();
    let mut order = Vec::with_capacity(total);
    while order.len() < total {
        let slot = order.len();
        let take_cooling = match (wanted.peek(), furthest_behind(&totals, &emitted)) {
            (Some(&(at, _, _)), Some(_)) => at <= slot,
            (Some(_), None) => true,
            (None, _) => false,
        };
        if take_cooling {
            order.push(wanted.next().expect("peeked").2);
        } else {
            let table = furthest_behind(&totals, &emitted).expect("a stream has ops left");
            order.push(streams[table][emitted[table]]);
            emitted[table] += 1;
        }
    }
    reorder_ops(&mut program, order);
    program
}

// ---------------------------------------------------------------------
// Issue #557: the concurrent tier's lanes.
//
// #505 made each table's op stream the only ordering constraint a program
// carries across tables. The concurrent tier goes one step further and
// issues a burst's ops from several harness tasks at once, each on its own
// connection, so Postgres commits them in no order the harness chooses.
//
// What has to stay ordered is narrower than `ops_commute`'s relation.
// `ops_commute` answers "does reordering these two ops leave the converged
// target the same", which is what `tests/order_insensitivity.rs` compares
// two serial runs on. The concurrent tier never compares against a fixed
// serial order: the oracle recomputes every target from the source as it
// actually is after the burst. All the harness still has to predict is each
// op's own outcome (`Op::expect`), and that depends only on earlier ops on
// the same source row, or on a whole-table op of the same table. So ops on
// different rows may race even when they feed the same aggregate group. That
// is exactly the traffic the tier exists to produce: many rows of one hot
// group written at once, and drained by several workers.
//
// A lane key is therefore `(table, pk)`, or the whole table when the
// program ever issues a whole-table op on it (`Truncate`/`BulkInsert`),
// since one of those touches every row. Every lane key lives inside one of
// `table_streams`' streams, so each table's own order is kept too.
// ---------------------------------------------------------------------

/// The most harness tasks one burst is split across (issue #557).
pub const MAX_LANES: usize = 4;

/// Splits `program.ops` into bursts of `burst_size` consecutive ops, and
/// each burst into at most `lanes` lanes that the concurrent tier issues
/// from separate tasks (issue #557; see the section comment above for which
/// ops may be split apart). Within a burst, every op on one row (or on one
/// table, if the program ever truncates or bulk-inserts it) lands in the
/// same lane, in program order. Distinct keys are dealt to lanes round-robin
/// in order of first appearance, so the lanes stay balanced and the plan is
/// a pure function of its arguments: a shrunk case replays the same plan.
///
/// A deferred install (`program.def_install_after_op[i] == n`, issue #557
/// part 2) becomes a [`BurstAction::Install`] in the burst holding op `n`,
/// firing once the burst ops before `n` have been applied. The lanes race,
/// so "before op `n`" becomes "after that many of the burst's ops": the
/// build runs while the rest of the burst still writes the source.
///
/// # Panics
///
/// If `burst_size` or `lanes` is `0`, or `program` schedules a restart or a
/// scale-out, which the concurrent runner doesn't support.
pub fn concurrent_plan(program: &Program, burst_size: usize, lanes: usize) -> ConcurrentPlan {
    assert!(
        burst_size > 0 && lanes > 0,
        "concurrent_plan: burst_size and lanes must both be at least 1"
    );
    assert!(
        program.restart_after_ops.is_empty() && program.scale_out_after_ops.is_empty(),
        "concurrent_plan: the concurrent tier doesn't schedule a restart or a scale-out"
    );
    let whole_table: std::collections::HashSet<&str> = program
        .ops
        .iter()
        .filter(|op| matches!(op, Op::Truncate { .. } | Op::BulkInsert { .. }))
        .map(op_table)
        .collect();
    let lane_key = |op: &Op| -> (String, Option<String>) {
        let table = op_table(op);
        let pk = if whole_table.contains(table) {
            None
        } else {
            let spec = program
                .tables
                .iter()
                .find(|t| t.name == table)
                .expect("an op names one of the program's tables");
            op_pk_value(op, spec)
        };
        (table.to_string(), pk)
    };

    let indices: Vec<usize> = (0..program.ops.len()).collect();
    let bursts = indices
        .chunks(burst_size)
        .map(|chunk| {
            let mut lane_of: HashMap<(String, Option<String>), usize> = HashMap::new();
            let mut out = vec![Vec::new(); lanes];
            for &index in chunk {
                let key = lane_key(&program.ops[index]);
                let next = lane_of.len() % lanes;
                let lane = *lane_of.entry(key).or_insert(next);
                out[lane].push(index);
            }
            out.retain(|lane| !lane.is_empty());
            Burst {
                lanes: out,
                actions: Vec::new(),
            }
        })
        .collect();
    let mut plan = ConcurrentPlan {
        bursts,
        steady_load: None,
    };
    for (def, &at) in program.def_install_after_op.iter().enumerate() {
        if at == 0 {
            continue;
        }
        plan.bursts[at / burst_size].actions.push(TimedAction {
            after: at % burst_size,
            action: BurstAction::Install { def },
        });
    }
    sort_actions(&mut plan);
    plan
}

/// Orders every burst's actions by when they fire, keeping the order of
/// actions that fire at the same point (a pause stays ahead of its resume).
fn sort_actions(plan: &mut ConcurrentPlan) {
    for burst in &mut plan.bursts {
        burst.actions.sort_by_key(|action| action.after);
    }
}

/// Which mid-burst operator action an [`ActionDraw`] asks for (issue #557
/// part 2). Mid-burst installs aren't drawn here: they come from
/// [`Program::def_install_after_op`] through [`concurrent_plan`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionKind {
    /// [`BurstAction::RequestBackfill`] on a table an installed definition
    /// reads.
    RequestBackfill,
    /// A whole-transform [`BurstAction::Pause`], then its
    /// [`BurstAction::Resume`] later in the same burst.
    PauseResume,
    /// A column [`BurstAction::Pause`] on a 1-1 definition, then its column
    /// [`BurstAction::Resume`] later in the same burst.
    ColumnPauseResume,
}

/// One drawn mid-burst action, before [`add_burst_actions`] resolves it
/// against a program and its plan. Plain numbers, so it shrinks toward
/// "earliest burst, first target, at the burst's start".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActionDraw {
    pub kind: ActionKind,
    /// Picks a burst after the first, modulo how many there are.
    pub burst: usize,
    /// Picks the table, definition or column, modulo how many qualify.
    pub target: usize,
    /// Where in the burst the action (or a pair's pause) fires, in
    /// thousandths of the burst's ops.
    pub start: u16,
    /// How much further on a pair's resume fires, in thousandths of the
    /// burst's ops, capped at the burst's end.
    pub span: u16,
}

/// Resolves `draws` against `program` and adds them to `plan`'s bursts
/// (issue #557 part 2). Every action lands in a burst after the first: by
/// then the previous burst has quiesced, so every definition installed up
/// front is `live` and its tables are captured, which `request_backfill`
/// and a column resume both require. Only definitions installed up front
/// (`def_install_after_op == 0`) are paused, and their tables re-read.
///
/// A draw that can't apply is dropped rather than bent into something
/// else: a plan with one burst, a column pair with no 1-1 definition, and a
/// second pair on a definition that already has one in that burst (two
/// overlapping pairs on one definition would resume it twice, and a
/// whole-transform resume leaves the definition rebuilding, where a column
/// resume is refused).
pub fn add_burst_actions(
    mut plan: ConcurrentPlan,
    program: &Program,
    draws: &[ActionDraw],
) -> ConcurrentPlan {
    if plan.bursts.len() < 2 {
        return plan;
    }
    let up_front: Vec<&TransformDef> = program
        .defs
        .iter()
        .zip(&program.def_install_after_op)
        .filter(|(_, at)| **at == 0)
        .map(|(def, _)| def)
        .collect();
    let mut tables: Vec<&str> = Vec::new();
    for def in &up_front {
        if !tables.contains(&def.source.as_str()) {
            tables.push(&def.source);
        }
    }
    let one_to_one: Vec<&TransformDef> = up_front
        .iter()
        .copied()
        .filter(|def| matches!(def.key_space, KeySpace::OneToOne) && !def.fields.is_empty())
        .collect();
    let mut paired: std::collections::HashSet<(usize, &str)> = std::collections::HashSet::new();

    for draw in draws {
        let burst_index = 1 + draw.burst % (plan.bursts.len() - 1);
        let ops = plan.bursts[burst_index].ops().len();
        let at = usize::from(draw.start.min(1000)) * ops / 1000;
        let end = (at + usize::from(draw.span.min(1000)) * ops / 1000).min(ops);
        let actions = &mut plan.bursts[burst_index].actions;
        match draw.kind {
            ActionKind::RequestBackfill => {
                if tables.is_empty() {
                    continue;
                }
                actions.push(TimedAction {
                    after: at,
                    action: BurstAction::RequestBackfill {
                        table: tables[draw.target % tables.len()].to_string(),
                    },
                });
            }
            ActionKind::PauseResume | ActionKind::ColumnPauseResume => {
                let (def, column) = if draw.kind == ActionKind::PauseResume {
                    if up_front.is_empty() {
                        continue;
                    }
                    (up_front[draw.target % up_front.len()], None)
                } else {
                    if one_to_one.is_empty() {
                        continue;
                    }
                    let def = one_to_one[draw.target % one_to_one.len()];
                    let field = &def.fields[(draw.target / one_to_one.len()) % def.fields.len()];
                    (def, Some(field.name.clone()))
                };
                if !paired.insert((burst_index, def.target.as_str())) {
                    continue;
                }
                actions.push(TimedAction {
                    after: at,
                    action: BurstAction::Pause {
                        target: def.target.clone(),
                        column: column.clone(),
                    },
                });
                actions.push(TimedAction {
                    after: end,
                    action: BurstAction::Resume {
                        target: def.target.clone(),
                        column,
                    },
                });
            }
        }
    }
    sort_actions(&mut plan);
    plan
}

/// One concurrent-tier case (issue #557): the program, how it is issued,
/// and the engine shape it is drained by.
#[derive(Debug, Clone, PartialEq)]
pub struct ConcurrentCase {
    pub program: Program,
    /// The ops per burst the plan was cut with, kept for the failure report.
    pub burst_size: usize,
    pub plan: ConcurrentPlan,
    /// Application (drain) workers the engine runs.
    pub workers: usize,
    /// The engine's maintenance cadence, which is its seal cadence: short
    /// enough that one burst seals into several batches, so one hot key's
    /// changes sit in several batches at once.
    pub seal_interval_ms: u64,
    /// The rows per Re-derive build chunk the engine plans, or `None` for
    /// the engine's default.
    pub build_chunk_rows: Option<i64>,
    /// How often the engine's reconcile pass runs, which is where a build
    /// starts, in milliseconds, or `None` for the engine's default (5s).
    pub reconcile_interval_ms: Option<u64>,
}

/// [`build_program_multi_with_derived`] is [`build_program_multi_with_shapes_and_derived`]'s
/// convenience wrapper for the common "every def is `OneToOne`" case
/// (improvement-plan task B2, kept at its original `&[usize]`/`&[DerivedShape]`
/// signature after task B4 introduced per-def shapes): every existing
/// hand-built pin across `generative/tests/*.rs` that calls this two-list
/// shape keeps working unchanged, the same "don't touch two dozen call
/// sites for an orthogonal widening" reasoning [`build_program`]'s own doc
/// comment gives for its own two-argument shape.
pub fn build_program_multi_with_derived(
    tables: &[TableSpec],
    def_sources: &[usize],
    derived: &[DerivedShape],
) -> Program {
    let defs: Vec<(usize, DefShape)> = def_sources
        .iter()
        .map(|&idx| (idx, DefShape::OneToOne))
        .collect();
    let derived: Vec<Option<DerivedShape>> = derived.iter().cloned().map(Some).collect();
    build_program_multi_with_shapes_and_derived(tables, &defs, &derived)
}

// ---------------------------------------------------------------------
// Improvement-plan task E2: definition lifecycle — install mid-stream.
// ---------------------------------------------------------------------

/// Defers `program.defs[def_index]`'s install to just before `program.ops[after_op]`
/// runs, instead of up front alongside every other definition (every builder
/// above always sets `def_install_after_op[i] == 0` for every `i` — see
/// [`Program::def_install_after_op`]'s own doc comment). Layered on top of an
/// already-built `Program`, the same "layer a widening on top rather than
/// thread it through the base builder" idiom
/// [`build_program_multi_with_shapes_and_derived`] already uses for derived
/// fields.
///
/// By the time `defs[def_index]` installs, `program.ops[0..after_op]` have
/// already run — including, if `after_op` is chosen past that definition's
/// own source table's first seed insert, real pre-existing source rows for
/// the engine's backfill discharge to build from (exactly the scenario `generative/tests/backfill.rs` exercises
/// by hand against a bare [`crate::backend::ManualBackend`], now reachable
/// from inside [`crate::run::run_convergence`]'s own op-stream loop). This
/// function itself does not require that ordering — it only enforces the
/// index bounds below — so a caller can also use it to defer a definition
/// whose source table hasn't been touched yet at all, which is a legal (if
/// less interesting) case too.
///
/// # Panics
///
/// - if `def_index` is out of range for `program.defs`.
/// - if `after_op` is not strictly between `0` (exclusive — that's just the
///   default, not a "defer") and `program.ops.len()` (exclusive): deferring
///   to on/after the very last op would never reach a subsequent per-op
///   convergence check inside `run_convergence`'s loop (see that function's
///   doc comment), so a generator asking for it is a bug worth panicking on
///   loudly rather than silently dropping the install.
pub fn defer_def_install(mut program: Program, def_index: usize, after_op: usize) -> Program {
    assert!(
        def_index < program.defs.len(),
        "defer_def_install: def_index {def_index} out of range for {} definitions — a generator \
         bug",
        program.defs.len()
    );
    assert!(
        after_op >= 1 && after_op < program.ops.len(),
        "defer_def_install: after_op ({after_op}) must be a real, still-to-come op index \
         (1..{}) — 0 is just the default install-up-front timing, and on/after the last op would \
         never reach a subsequent convergence check",
        program.ops.len()
    );
    program.def_install_after_op[def_index] = after_op;
    program
}

// ---------------------------------------------------------------------
// Improvement-plan task E3: engine lifecycle — in-process client
// restart/scale-out.
// ---------------------------------------------------------------------

/// Schedules a [`crate::backend::Backend::restart`] (simulating an in-process
/// engine client crash-and-restart) right before `program.ops[after_op]` runs
/// (improvement-plan task E3). Layered on top of an already-built `Program`,
/// same idiom as [`defer_def_install`].
///
/// # Panics
///
/// If `after_op` is not strictly between `0` and `program.ops.len()`
/// (exclusive on both ends) — see [`defer_def_install`]'s doc comment for why
/// the same bound applies here (a restart scheduled on/after the last op
/// would never be observed by a subsequent convergence check).
pub fn schedule_restart(mut program: Program, after_op: usize) -> Program {
    assert!(
        after_op >= 1 && after_op < program.ops.len(),
        "schedule_restart: after_op ({after_op}) must be a real, still-to-come op index \
         (1..{}) — see defer_def_install's doc comment for why",
        program.ops.len()
    );
    program.restart_after_ops.push(after_op);
    program
}

/// Schedules a [`crate::backend::Backend::scale_out`] (starting an additional
/// application-worker-only engine client) right before `program.ops[after_op]`
/// runs (improvement-plan task E3). Same shape and bounds as
/// [`schedule_restart`] — see its doc comment.
pub fn schedule_scale_out(mut program: Program, after_op: usize) -> Program {
    assert!(
        after_op >= 1 && after_op < program.ops.len(),
        "schedule_scale_out: after_op ({after_op}) must be a real, still-to-come op index \
         (1..{}) — see defer_def_install's doc comment for why",
        program.ops.len()
    );
    program.scale_out_after_ops.push(after_op);
    program
}

// ---------------------------------------------------------------------
// Improvement-plan task E6: bulk operations (TRUNCATE, bulk insert).
// ---------------------------------------------------------------------

/// The most rows [`build_bulk_insert_program`] draws into its single
/// [`Op::BulkInsert`] (improvement-plan task E6) — far larger than
/// [`MAX_SEED_ROWS`] (4, tuned for a *legible shrunk counterexample* across
/// many per-row round trips) since a bulk insert is deliberately the opposite
/// shape: one row count, one `apply` → `quiesce` → `snapshot` → compare round
/// trip regardless of how large it is, so drawing it large costs one
/// statement's worth of extra data, not extra round trips.
pub const MAX_BULK_INSERT_ROWS: usize = 500;

/// Builds a small, self-contained program exercising [`Op::BulkInsert`]
/// (improvement-plan task E6): one table (the same numeric
/// pk/`c1`/`c2`/grain shape [`build_program_multi_with_shapes`] uses, minus
/// the `Text`/`Boolean`/`Uuid` columns — bulk-insert coverage is about row
/// *count*, not type coverage, which task B1 already covers elsewhere), a
/// single `Op::BulkInsert` seeding `row_count` rows in one statement
/// (`pk = 1..=row_count`, `c1 = pk`, `c2 = pk * 2`, `grain = pk % 3`), one
/// `OneToOne` def (`total = c1 + c2`) and one `Aggregate` def (`SUM(c1)`/
/// `COUNT(*)` grouped by grain) over it — so a single large multi-row insert
/// is proven to backfill/maintain correctly under both key-spaces at once —
/// and (mirroring [`Mutate::Update`]/[`Mutate::Delete`]'s post-seed coverage)
/// one `Update` and one `Delete` afterward, against pks guaranteed live by
/// construction, so a bulk-inserted row is also provably visible to ordinary
/// single-row mutation right after.
///
/// `row_count` must be at least 1 (a zero-row `INSERT ... VALUES` has no
/// valid SQL rendering — see [`crate::backend::ManualBackend::apply`]'s
/// `Op::BulkInsert` arm, which panics on an empty `rows`).
pub fn build_bulk_insert_program(row_count: usize) -> Program {
    assert!(
        row_count >= 1,
        "build_bulk_insert_program: row_count must be at least 1 (a zero-row bulk insert has no \
         valid SQL rendering)"
    );

    let mut pool = NamePool::new();
    let table = Table::new(
        &mut pool,
        &[ValueType::Numeric, ValueType::Numeric, ValueType::Numeric],
    );
    let c1 = table.columns[1].name.clone();
    let c2 = table.columns[2].name.clone();
    let grain = table.columns[3].name.clone();

    let rows: Vec<Vec<(String, Option<String>)>> = (1..=row_count as i64)
        .map(|pk| {
            vec![
                (table.pk_col.clone(), Some(pk.to_string())),
                (c1.clone(), Some(pk.to_string())),
                (c2.clone(), Some((pk * 2).to_string())),
                (grain.clone(), Some((pk % 3).to_string())),
            ]
        })
        .collect();

    let mut ops = vec![Op::BulkInsert {
        table: table.name.clone(),
        rows,
        expect: OpOutcome::Succeeds,
    }];
    // A post-bulk-insert `Update`/`Delete` against pk 1 (always live: every
    // `row_count >= 1` bulk insert seeds it) — proves ordinary single-row
    // mutation composes with a preceding bulk insert.
    ops.push(Op::Update {
        table: table.name.clone(),
        pk: "1".to_string(),
        changes: vec![(c1.clone(), Some("1000".to_string()))],
        expect: OpOutcome::Succeeds,
    });
    ops.push(Op::Delete {
        table: table.name.clone(),
        pk: "1".to_string(),
        expect: OpOutcome::Succeeds,
    });

    let one_to_one = TransformDef {
        target: pool.next_table_name(),
        source: table.name.clone(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "total".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column(c1.clone())),
                rhs: Box::new(Expr::Column(c2.clone())),
            },
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };
    let aggregate = TransformDef {
        target: pool.next_table_name(),
        source: table.name.clone(),
        key_space: KeySpace::Aggregate {
            group_by: vec![GroupByKey::Column(grain.clone())],
        },
        fields: vec![
            FieldDef {
                name: grain.clone(),
                expr: Expr::Column(grain),
            },
            aggregate_field_def(AggregateFn::Sum(AggregateColumn::C1), &c1, &c2, None),
            aggregate_field_def(AggregateFn::Count, &c1, &c2, None),
        ],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };
    let defs = vec![one_to_one, aggregate];
    let def_install_after_op = vec![0; defs.len()];

    Program {
        tables: vec![table],
        relationships: Vec::new(),
        defs,
        def_install_after_op,
        ops,
        restart_after_ops: Vec::new(),
        scale_out_after_ops: Vec::new(),
    }
}

// ---------------------------------------------------------------------
// Improvement-plan workstream D, task D2: order-insensitivity over
// commuting ops.
// ---------------------------------------------------------------------
//
// The property this section supports (`generative/tests/order_insensitivity.rs`)
// is: reorder ops that target *distinct derived-target keys*, and
// convergence must be identical. The load-bearing design point is what
// "distinct derived-target keys" means: it must be keyed off **the target
// key each op maps to under each installed definition sourced from that
// op's table**, not off the op's raw source `(table, pk)` — since
// improvement-plan task B4 added real [`KeySpace::Aggregate`] definitions,
// those two things no longer always coincide: two ops on *different* source
// pks can land in the *same* aggregate group (the `GROUP BY` columns
// match), so reordering them relative to an op that reads a *different*
// group is not obviously safe the way "different pk, different row" is for
// `OneToOne`. Keying the whole analysis off [`target_key_for`] — which
// reduces to the source pk for `OneToOne`, and to the row's own `group_by`
// column values for `Aggregate` — means both key-spaces share one
// correctness argument instead of two.

/// The table an [`Op`] targets, regardless of its kind.
fn op_table(op: &Op) -> &str {
    match op {
        Op::Insert { table, .. }
        | Op::Update { table, .. }
        | Op::Delete { table, .. }
        | Op::Truncate { table, .. }
        | Op::BulkInsert { table, .. } => table,
    }
}

/// The value of `table`'s own primary-key column that `op` (which must
/// target `table`) reads or writes — `None` only if `op` doesn't actually
/// carry that column (a generator bug: every [`Op::Insert`] this crate
/// builds always carries the pk column, non-`NULL`, and `Update`/`Delete`
/// carry it directly as `pk`), or if `op` inherently touches more than one
/// pk at once (improvement-plan task E6's `Op::Truncate`/`Op::BulkInsert` —
/// `None` here is the same "conservative, may conflict with anything on this
/// table" signal [`ops_commute`] already treats an unresolved pk as, which is
/// exactly right for a whole-table clear or a multi-row insert).
fn op_pk_value(op: &Op, table: &Table) -> Option<String> {
    match op {
        Op::Insert { row, .. } => row
            .iter()
            .find(|(name, _)| name == &table.pk_col)
            .and_then(|(_, value)| value.clone()),
        Op::Update { pk, .. } | Op::Delete { pk, .. } => Some(pk.clone()),
        Op::Truncate { .. } | Op::BulkInsert { .. } => None,
    }
}

/// The key of the target-table row `op` contributes to under `def`, if
/// `def.source` names `op`'s own table (`None` otherwise — the definition is
/// irrelevant to this op) — see the module-section doc comment above for why
/// this, rather than the raw source pk, is what [`ops_commute`] must be keyed
/// on.
///
/// `None` also covers "the key can't be determined from `op` alone": for
/// `KeySpace::Aggregate`, an `Update`/`Delete` op carries no guarantee it
/// touches (or reveals) every `group_by` column — an `Update` may leave every
/// grouping column untouched, and a `Delete` carries no columns at all — so
/// resolving the *actual* group a pre-existing row belongs to would need a
/// source-row lookup this model-only function deliberately never does.
/// Callers must treat `None` conservatively, as "may conflict with anything
/// on this table", never as "definitely independent" — see [`ops_commute`].
///
/// **The `Aggregate` arm is real, exercised code as of improvement-plan task
/// B4** (it was written, and this doc comment previously described it, as an
/// honest-but-unreachable seam before B4 added any generator that actually
/// draws a `KeySpace::Aggregate` definition — that generator now exists, see
/// [`DefShape::Aggregate`]/`strategy::def_shape`). An `Insert` that carries
/// every `group_by` column in its own row resolves its group exactly the way
/// `OneToOne` reads the pk column off the same row; `Update`/`Delete` still
/// conservatively resolve to `None` for the reason above.
pub fn target_key_for(def: &TransformDef, table: &Table, op: &Op) -> Option<String> {
    if op_table(op) != def.source {
        return None;
    }
    match &def.key_space {
        KeySpace::OneToOne => op_pk_value(op, table),
        KeySpace::Aggregate { group_by } => match op {
            Op::Insert { row, .. } => {
                let mut parts = Vec::with_capacity(group_by.len());
                for key in group_by {
                    let column = key.target_column_name();
                    let value = row.iter().find(|(name, _)| name == column)?.1.clone();
                    // A `NULL` grouping value is itself a value Postgres's
                    // `GROUP BY` treats as one group (nulls compare equal for
                    // grouping purposes) — encode it as a value distinct from
                    // any real rendered column text, rather than collapsing
                    // it into the empty string a real value could also
                    // render as.
                    parts.push(value.unwrap_or_else(|| "\u{0}NULL\u{0}".to_string()));
                }
                Some(parts.join("\u{1f}"))
            }
            Op::Update { .. } | Op::Delete { .. } => None,
            // Improvement-plan task E6: a whole-table `Truncate` or a
            // multi-row `BulkInsert` can touch more than one group at once —
            // conservatively unresolved, same as `Update`/`Delete` above.
            Op::Truncate { .. } | Op::BulkInsert { .. } => None,
        },
    }
}

/// Whether `a` and `b` — two ops belonging to `program` — may be freely
/// reordered relative to each other without changing the program's eventual
/// converged state (D2).
///
/// Ops on different tables always commute: every definition this generator
/// installs gets its own freshly-minted target ([`NamePool::next_table_name`]),
/// so two different source tables can never feed the same target row, and a
/// source table's own rows are obviously independent of another table's.
///
/// Ops on the *same* table commute only if **both**:
/// - they touch different rows of that table's own primary key (a table's
///   raw rows are part of [`crate::backend::Snapshot`] too, independent of
///   any definition reading them — design doc §1), and
/// - under *every* definition sourced from that table, they resolve to
///   different target keys via [`target_key_for`] — an unresolved (`None`)
///   key is a conflict, never a pass.
pub fn ops_commute(program: &Program, a: &Op, b: &Op) -> bool {
    let (table_a, table_b) = (op_table(a), op_table(b));
    if table_a != table_b {
        return true;
    }
    let Some(table) = program.tables.iter().find(|t| t.name == table_a) else {
        // An op names a table the program never declared — a generator bug
        // this function has no business papering over by claiming
        // independence.
        return false;
    };

    let same_row = match (op_pk_value(a, table), op_pk_value(b, table)) {
        (Some(pk_a), Some(pk_b)) => pk_a == pk_b,
        // An unresolved pk is a generator bug, not evidence of
        // independence — be conservative.
        _ => true,
    };
    if same_row {
        return false;
    }

    program
        .defs
        .iter()
        .filter(|def| def.source == table_a)
        .all(
            |def| match (target_key_for(def, table, a), target_key_for(def, table, b)) {
                (Some(key_a), Some(key_b)) => key_a != key_b,
                _ => false,
            },
        )
}

/// Partitions `program.ops`' indices into commute groups: ops sharing a
/// group must keep their original relative order; ops in different groups
/// may be freely interleaved in any order ([`ops_commute`]). Each group's own
/// indices are kept in original relative (increasing) order.
///
/// Built with a small union-find over the pairwise [`ops_commute`] predicate,
/// rather than hardcoding "group by (table, pk)" directly: that pairwise
/// predicate is the one place `Aggregate` support would need to grow
/// (via [`target_key_for`]), so grouping through it — instead of re-deriving
/// an equivalent-but-separate key here — keeps this function correct for
/// free the day that widening lands, and safe (via the union step) even if a
/// future predicate is no longer transitive across three or more ops.
fn commute_groups(program: &Program) -> Vec<Vec<usize>> {
    let n = program.ops.len();
    let mut parent: Vec<usize> = (0..n).collect();

    fn find(parent: &mut [usize], x: usize) -> usize {
        if parent[x] != x {
            parent[x] = find(parent, parent[x]);
        }
        parent[x]
    }
    fn union(parent: &mut [usize], a: usize, b: usize) {
        let (root_a, root_b) = (find(parent, a), find(parent, b));
        if root_a != root_b {
            parent[root_a] = root_b;
        }
    }

    for i in 0..n {
        for j in (i + 1)..n {
            if !ops_commute(program, &program.ops[i], &program.ops[j]) {
                union(&mut parent, i, j);
            }
        }
    }

    let mut groups: HashMap<usize, Vec<usize>> = HashMap::new();
    for i in 0..n {
        let root = find(&mut parent, i);
        groups.entry(root).or_default().push(i);
    }

    let mut result: Vec<Vec<usize>> = groups.into_values().collect();
    // Deterministic output order (by each group's first/smallest index), so
    // `reordered_by_commute_groups` below is itself deterministic.
    result.sort_by_key(|group| group[0]);
    result
}

/// Builds a second, independently-valid op ordering for `program`: the same
/// tables/defs and the same *set* of ops, but with the commute groups
/// ([`commute_groups`]) concatenated in reverse order — each group's own
/// internal relative order is left untouched.
///
/// That internal-order guarantee is what keeps every op's `expect()` — set by
/// [`build_program_multi`]'s per-table pk-liveness simulation, which only
/// ever depends on *earlier same-pk ops* — still valid against the new
/// ordering: an op's real `apply()` outcome is unaffected by ops outside its
/// own commute group being shuffled around it.
///
/// Reversing group order (rather than drawing an arbitrary permutation) is a
/// deliberately simple construction that differs from the input whenever
/// there is more than one group: `generative/tests/order_insensitivity.rs`
/// only needs *some* second valid ordering to diff the original against, not
/// a random sample of every valid ordering.
pub fn reordered_by_commute_groups(program: &Program) -> Program {
    let groups = commute_groups(program);
    let mut ops = Vec::with_capacity(program.ops.len());
    for group in groups.iter().rev() {
        for &index in group {
            ops.push(program.ops[index].clone());
        }
    }
    Program {
        tables: program.tables.clone(),
        // Carried through unchanged: a relationship is a schema-level
        // declaration, not an op, so reordering the op stream cannot affect
        // it — and dropping it would leave every definition that reads one
        // referencing an undeclared relationship, which the engine rejects
        // at install time.
        relationships: program.relationships.clone(),
        defs: program.defs.clone(),
        // Carried over unchanged, not reinterpreted against the new op
        // order: this function is only ever exercised (`tests/order_insensitivity.rs`)
        // against programs from `trivial_program_with`/`build_program`, which
        // always draw every def install up front (`0`) and never schedule a
        // restart/scale-out — the only values reordering an op stream out
        // from under an op-index-anchored field could silently invalidate.
        def_install_after_op: program.def_install_after_op.clone(),
        ops,
        restart_after_ops: program.restart_after_ops.clone(),
        scale_out_after_ops: program.scale_out_after_ops.clone(),
    }
}

/// A noise table's fixed shape (improvement-plan Workstream E, task E1:
/// untracked-object noise): a `Numeric` primary key column plus one extra
/// column of `extra_type`. Kept to exactly two columns (pk + one extra) —
/// noise doesn't need [`Table`]'s full multi-type-column richness, just
/// enough shape for DML (`Insert`/`Update`/`Delete`) and DDL
/// (`AddColumn`/`DropColumn`) noise to have somewhere to land. Never routed
/// through [`NamePool`]: a noise table's name/columns must stay stable and
/// caller-chosen (see [`adversarial_noise_table`]), not drawn from the same
/// counter a real [`Program`]'s tables use.
pub fn noise_table(name: &str, pk_col: &str, extra_col: &str, extra_type: ValueType) -> Table {
    Table {
        name: name.to_string(),
        pk_col: pk_col.to_string(),
        columns: vec![
            Column {
                name: pk_col.to_string(),
                value_type: ValueType::Numeric,
            },
            Column {
                name: extra_col.to_string(),
                value_type: extra_type,
            },
        ],
        unique_cols: Vec::new(),
    }
}

/// A fixed, deliberately adversarial default noise table (task E1): named
/// `"noise_untracked"` so it can never collide with a drawn [`Program`]'s own
/// table/def names — [`NamePool`] only ever produces `t0`, `t1`, ... and
/// `d0`, `d1`, ..., and `"noise_untracked"` matches neither pattern — but
/// whose pk column is deliberately named `"c0"` (the exact name `NamePool`
/// gives the very first table's pk column) and whose extra column is
/// deliberately named `"total"` (a common def target field name — see
/// [`build_program`]'s `total = c1 + c2` field), so a hand-built pin can
/// demonstrate that even a noise table shaped to collide with a real table's
/// *column* names causes no oracle confusion:
/// `crate::backend::ManualBackend::snapshot`/`crate::run::check_program`
/// only ever key off table *name*, and
/// `crate::backend::ManualBackend::install_noise_table` never touches
/// `self.tables`, and no definition reads it — a same-named column
/// on an entirely different, untracked table cannot collide with anything.
pub fn adversarial_noise_table() -> Table {
    noise_table("noise_untracked", "c0", "total", ValueType::Text)
}

#[cfg(feature = "proptest")]
mod strategy {
    use super::*;
    use crate::model::{
        BackupKind, DbAdminAction, DbAdminEvent, DbAdminPlan, NoiseAction, NoiseEvent,
        NoiseEventKind, NoisePlan, RestartMode, RestorePlan, SteadyLoad,
    };
    use proptest::prelude::*;

    /// One in this many awkward-value draws comes back `None` (SQL `NULL`)
    /// when awkward values are enabled — frequent enough that a handful of
    /// seed rows/mutates reliably hits one, rare enough that most cases still
    /// exercise the plain numeric path.
    const NULL_WEIGHT: u32 = 1;
    const VALUE_WEIGHT: u32 = 4;

    /// A single calculated-field / column value: a small non-negative integer
    /// (see [`VALUE_MAX`]'s numeric-path pairing), or — when `awkward_values`
    /// is set — occasionally `None` (SQL `NULL`, see the module doc comment's
    /// "awkward values" note).
    ///
    /// When `awkward_values` is unset this is *exactly* the strategy the
    /// generator used before issue #7 (a bare `0..=VALUE_MAX` draw, just
    /// wrapped in `Some` outside the strategy), so a coverage meta-test can
    /// assert the flag-off path draws unchanged (design doc §3 "coverage
    /// meta-tests").
    fn value(awkward_values: bool) -> BoxedStrategy<Option<i64>> {
        if awkward_values {
            prop_oneof![
                VALUE_WEIGHT => (0..=VALUE_MAX).prop_map(Some),
                NULL_WEIGHT => Just(None),
            ]
            .boxed()
        } else {
            (0..=VALUE_MAX).prop_map(Some).boxed()
        }
    }

    /// Weight of one specific awkward text literal, relative to a plain
    /// short string, when awkward values are enabled (task B1). Kept small
    /// like `NULL_WEIGHT` — rare per draw, reliably hit across a handful of
    /// seed rows.
    const AWKWARD_TEXT_WEIGHT: u32 = 1;

    /// A short, plain ASCII string — the "ordinary" `Text` column value both
    /// with and without awkward values enabled.
    fn plain_text() -> BoxedStrategy<String> {
        "[a-zA-Z0-9 ]{0,8}".boxed()
    }

    /// One specific awkward *text* value (design doc §3 / improvement-plan
    /// task B1): the empty string; the literal four-character text `"NULL"`
    /// (distinct from SQL `NULL`, i.e. `None` — that's [`value`]'s job, this
    /// is the string that spells the word); a string containing the U+001F
    /// unit-separator (`defs::ddl::join_pk_key`'s composite-key
    /// delimiter — see the module doc comment's scope-cut note on why this
    /// doesn't yet reach that risk); and a string containing a
    /// comma/quote/backslash (SQL-binding/escaping awkwardness, independent
    /// of any delimiter).
    fn awkward_text_literal() -> BoxedStrategy<String> {
        prop_oneof![
            Just(String::new()),
            Just("NULL".to_string()),
            Just("has\u{1f}unit-separator".to_string()),
            Just("has,comma'quote\"and\\backslash".to_string()),
        ]
        .boxed()
    }

    /// A single `Text` column value (task B1): a plain short string, or —
    /// when `awkward_values` is set — occasionally `None` (SQL `NULL`) or
    /// one of [`awkward_text_literal`]'s specific awkward strings.
    fn text_value(awkward_values: bool) -> BoxedStrategy<Option<String>> {
        if awkward_values {
            prop_oneof![
                VALUE_WEIGHT => plain_text().prop_map(Some),
                NULL_WEIGHT => Just(None),
                AWKWARD_TEXT_WEIGHT => awkward_text_literal().prop_map(Some),
            ]
            .boxed()
        } else {
            plain_text().prop_map(Some).boxed()
        }
    }

    /// A single `Boolean` column value (task B1): the rendered text form
    /// [`Op`] wants (`"true"`/`"false"`), or — when `awkward_values` is set —
    /// occasionally `None` (SQL `NULL`).
    fn bool_value(awkward_values: bool) -> BoxedStrategy<Option<String>> {
        let plain = prop_oneof![
            Just(Some("true".to_string())),
            Just(Some("false".to_string())),
        ];
        if awkward_values {
            prop_oneof![
                VALUE_WEIGHT => plain,
                NULL_WEIGHT => Just(None),
            ]
            .boxed()
        } else {
            plain.boxed()
        }
    }

    /// Formats 16 random bytes as a syntactically-valid UUID string
    /// (`xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx`) — hand-rolled rather than
    /// pulling in the `uuid` crate: `uuid` is not a workspace dependency
    /// anywhere reachable from `generative` (checked `Cargo.lock` and every
    /// crate's `Cargo.toml` in the workspace before writing this), and
    /// proptest's own random bytes are all a validly-*shaped* UUID string
    /// needs — Postgres's `uuid` type only checks the 32-hex-digit/dash-
    /// grouping shape, not the RFC 4122 version/variant bits. This function
    /// sets them anyway (version 4, the common "random UUID" form) purely so
    /// a shrunk counterexample looks like a real application-generated UUID
    /// rather than an obviously-synthetic one.
    fn uuid_string(mut bytes: [u8; 16]) -> String {
        bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
        bytes[8] = (bytes[8] & 0x3f) | 0x80; // RFC 4122 variant
        format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-\
             {:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            bytes[0],
            bytes[1],
            bytes[2],
            bytes[3],
            bytes[4],
            bytes[5],
            bytes[6],
            bytes[7],
            bytes[8],
            bytes[9],
            bytes[10],
            bytes[11],
            bytes[12],
            bytes[13],
            bytes[14],
            bytes[15],
        )
    }

    fn plain_uuid() -> BoxedStrategy<String> {
        prop::array::uniform16(any::<u8>())
            .prop_map(uuid_string)
            .boxed()
    }

    /// A single `Uuid` column value (task B1): a freshly-generated,
    /// syntactically-valid UUID string, or — when `awkward_values` is set —
    /// occasionally `None` (SQL `NULL`). Deliberately **never** a malformed
    /// UUID string (see the module doc comment's B1 scope cuts): a bad UUID
    /// would fail the `INSERT`/`UPDATE` statement's own `$n::text::uuid`
    /// cast — real, separate future coverage (a new `OpOutcome::Fails`
    /// case), not this task.
    fn uuid_value(awkward_values: bool) -> BoxedStrategy<Option<String>> {
        if awkward_values {
            prop_oneof![
                VALUE_WEIGHT => plain_uuid().prop_map(Some),
                NULL_WEIGHT => Just(None),
            ]
            .boxed()
        } else {
            plain_uuid().prop_map(Some).boxed()
        }
    }

    /// A short ASCII string, drawn from the same alphabet as [`plain_text`]
    /// (improvement-plan task B2), used as `STRPOS`'s needle argument. Kept
    /// in that alphabet (rather than fully arbitrary text) so a meaningful
    /// fraction of draws actually land a hit against a table's `Text` column
    /// (also drawn from [`plain_text`]'s alphabet) — a `STRPOS` that only
    /// ever returns `0` would still be correct, but a needle that sometimes
    /// hits is better coverage of the function's non-zero branch. Can be
    /// empty (Postgres's own `strpos(x, '') = 1` convention).
    fn strpos_needle() -> BoxedStrategy<String> {
        "[a-zA-Z0-9 ]{0,3}".boxed()
    }

    /// Regex patterns [`DerivedShape::RegexpCount`] draws from — restricted
    /// to syntax common to Rust's `regex` crate and Postgres's default ARE
    /// dialect (literal text, `.`, `*`, `+`, `?`, `[...]`, `|`, `^`, `$`),
    /// the exact same restriction `trellis/tests/defs_text_functions.rs`'s
    /// `REGEXP_COUNT_METACHARACTER_CASES` already vets against real
    /// Postgres. See [`DerivedShape`]'s doc comment (the D0 finding) for why
    /// this specific restriction is what keeps `REGEXP_COUNT` eval-time-
    /// infallible for what this generator draws — a pattern outside this
    /// pool could in principle compile under the `regex` crate (so pass
    /// `validate()`) yet mean something different, or nothing, under
    /// Postgres's own ARE dialect, which is a dialect-mismatch risk this
    /// pool exists specifically to avoid.
    const REGEXP_COUNT_PATTERN_POOL: &[&str] = &[
        "a", "o", "e", "a.c", "colou?r", "cat|dog", "[a-z]+", "^a", "a$",
    ];

    fn regexp_pattern() -> BoxedStrategy<String> {
        proptest::sample::select(REGEXP_COUNT_PATTERN_POOL)
            .prop_map(str::to_string)
            .boxed()
    }

    /// Draws one [`DerivedShape`] (improvement-plan task B2), equally
    /// weighted across every variant — each of the five scalar functions,
    /// the plain and nested `Operator::GreaterThan` shapes, all reachable
    /// with the same probability, since the coverage floor
    /// (`tests/coverage.rs`) needs every one of them to show up across a
    /// bounded number of samples, not just the common case.
    fn derived_shape() -> impl Strategy<Value = DerivedShape> {
        prop_oneof![
            strpos_needle().prop_map(|needle| DerivedShape::Strpos { needle }),
            Just(DerivedShape::OctetLength),
            Just(DerivedShape::CharLength),
            regexp_pattern().prop_map(|pattern| DerivedShape::RegexpCount { pattern }),
            (0..=VALUE_MAX).prop_map(|fallback| DerivedShape::CoalesceNumeric { fallback }),
            Just(DerivedShape::PlainGreaterThan),
            Just(DerivedShape::ArithmeticGreaterThan),
            (strpos_needle(), 0..=VALUE_MAX).prop_map(|(needle, threshold)| {
                DerivedShape::StrposGreaterThan { needle, threshold }
            }),
        ]
    }

    /// A single grain column value (improvement-plan task B4): `0..=GRAIN_MAX`
    /// most of the time, occasionally `None` (SQL `NULL`) — the "0, 1, 2, plus
    /// NULL" domain the improvement plan originally asked for. This was
    /// deliberately cut down to the three non-`NULL` values for a while
    /// (issue #128): a `NULL` grouping value is real, standard SQL (Postgres's
    /// `GROUP BY` puts every `NULL` into one group, like any other value),
    /// but `defs::ddl::create_aggregate_target_table` used to declare
    /// the `group_by` columns as the target's `PRIMARY KEY`, and Postgres
    /// primary-key columns are `NOT NULL` unconditionally — so a source row
    /// whose grouping column was `NULL` made every write to that group's
    /// target row fail with a real Postgres `null value in column ...
    /// violates not-null constraint` error. The failure itself was never a
    /// liveness wedge on its own (`staging::quarantine`'s isolate-before-blaming
    /// machinery parks the offending row alone after a few deaths and lets
    /// the rest of the batch drain), but the parked row's contribution was
    /// then permanently and silently excluded from its aggregate group with
    /// no automatic recovery, and `staging::converge::converged_through`'s
    /// condition 4 — which deliberately treats any live `poison_held` row as
    /// "not converged" until an operator releases it — meant a token at or
    /// past that row's LSN never converged, observed directly as this
    /// suite's own `quiesce` hanging past its 30s `QUIESCE_TIMEOUT`.
    ///
    /// Issue #128 fixed the root cause: `create_aggregate_target_table` now
    /// keys the grouping columns with a `UNIQUE NULLS NOT DISTINCT`
    /// constraint instead of a bare `PRIMARY KEY`, `defs::backfill`'s direct
    /// aggregate build no longer drops a NULL-keyed group, and
    /// `ddl::source_primary_key` falls back to a unique constraint so
    /// chaining off an aggregate target still works. A `NULL` grouping value
    /// no longer hits the not-null violation, so it's never quarantined and
    /// never blocks convergence — safe to draw again.
    ///
    /// Not gated by `awkward_values` (see [`trivial_program_with`]'s doc
    /// comment) — `NULL` here is a structural grouping-domain dimension, the
    /// same "always available regardless of the flag" treatment `def_shape`/
    /// `DerivedShape`/`Mutate::DuplicateInsert` already get, not a per-column
    /// awkward-value knob. `tests/coverage.rs`'s
    /// `awkward_values_off_never_draws_null` accounts for this by excluding
    /// each table's grain column (`columns[6]`) from its "never draws NULL"
    /// check rather than expecting this function to respect the flag.
    fn grain_value() -> impl Strategy<Value = Option<String>> {
        prop_oneof![
            VALUE_WEIGHT => Just(Some("0".to_string())),
            VALUE_WEIGHT => Just(Some("1".to_string())),
            VALUE_WEIGHT => Just(Some("2".to_string())),
            NULL_WEIGHT => Just(None),
        ]
    }

    /// Which of a source table's two aggregated columns (`c1`/`c2`) a drawn
    /// `SUM`/`AVG`/`MIN`/`MAX` field aggregates over (task B4).
    fn aggregate_column() -> impl Strategy<Value = AggregateColumn> {
        prop_oneof![Just(AggregateColumn::C1), Just(AggregateColumn::C2),]
    }

    /// The function *kinds* [`aggregate_functions`] can draw, without their
    /// column argument — kept as its own tiny enum so
    /// `proptest::sample::subsequence` (which needs a concrete, `Clone`
    /// element type to draw an order-preserving, duplicate-free subset from)
    /// has something to draw over; [`aggregate_functions`] then pairs each
    /// drawn numeric kind with an independently-drawn column.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum FnKind {
        Sum,
        Count,
        Avg,
        Min,
        Max,
        BoolAnd,
        BoolOr,
    }

    const ALL_FN_KINDS: [FnKind; 7] = [
        FnKind::Sum,
        FnKind::Count,
        FnKind::Avg,
        FnKind::Min,
        FnKind::Max,
        FnKind::BoolAnd,
        FnKind::BoolOr,
    ];

    /// A drawn `Aggregate` def's non-grouping fields (task B4): 2 to 7 of the
    /// seven aggregate functions in [`ALL_FN_KINDS`], each drawn at most once
    /// (`subsequence` never repeats an element), so multiple functions
    /// genuinely co-occur on the same def without ever needing two fields of
    /// the same name. Every numeric function's column is drawn independently
    /// per *function* (not per occurrence, since each function occurs at
    /// most once anyway), so two different functions landing on the *same*
    /// column — e.g. `SUM(c1)` and `AVG(c1)` — is a real, reachable draw:
    /// that's precisely the shape that exercises
    /// `defs::ddl::count_column_names`'s shared hidden-count-column path (two
    /// fields aggregating the exact same argument share one partial column)
    /// from the generative side, not just `trellis/tests/apply_aggregate.rs`'s
    /// hand-built fixture. `BOOL_AND`/`BOOL_OR` (issue #255) always fold the
    /// table's `Boolean` column — the only column they type-check against —
    /// so when both are drawn they share an argument too.
    ///
    /// The lower bound of 2 (not 1) guarantees at least two functions always
    /// co-occur, per the improvement-plan task's explicit ask ("draw at least
    /// 2-3 of the five per aggregate def so multiple functions actually
    /// co-occur"); the upper bound lets every function appear in the same
    /// def when proptest happens to draw it. Shrinking is `subsequence`'s
    /// own: it drops functions from the def toward the two-function floor.
    fn aggregate_functions() -> impl Strategy<Value = Vec<AggregateFn>> {
        (
            proptest::sample::subsequence(ALL_FN_KINDS.to_vec(), 2..=ALL_FN_KINDS.len()),
            aggregate_column(),
            aggregate_column(),
            aggregate_column(),
            aggregate_column(),
        )
            .prop_map(|(kinds, sum_col, avg_col, min_col, max_col)| {
                kinds
                    .into_iter()
                    .map(|kind| match kind {
                        FnKind::Sum => AggregateFn::Sum(sum_col),
                        FnKind::Count => AggregateFn::Count,
                        FnKind::Avg => AggregateFn::Avg(avg_col),
                        FnKind::Min => AggregateFn::Min(min_col),
                        FnKind::Max => AggregateFn::Max(max_col),
                        FnKind::BoolAnd => AggregateFn::BoolAnd,
                        FnKind::BoolOr => AggregateFn::BoolOr,
                    })
                    .collect()
            })
    }

    /// Which shape a drawn definition takes (task B4): the existing
    /// `OneToOne` `total = c1 + c2` shape, or a new `Aggregate` shape.
    /// Weighted evenly so both shapes get substantial, comparable coverage
    /// across a run — this is the dimension `tests/coverage.rs`'s B4 floor
    /// tests sample against, so it must not be so lopsided that 500 samples
    /// has a real chance of missing one of the aggregate functions.
    fn def_shape() -> impl Strategy<Value = DefShape> {
        prop_oneof![
            1 => Just(DefShape::OneToOne),
            1 => aggregate_functions().prop_map(|functions| DefShape::Aggregate { functions }),
        ]
    }

    /// One seed row's relationship **foreign-key** value (issue #34):
    /// drawn from a tiny fixed pool so all three join outcomes ADR-0006
    /// distinguishes really occur across a run, rather than only the happy
    /// one:
    ///
    /// * `"k1"`/`"k2"`/`"k3"` — match a related row, since a table's
    ///   relationship *key* column is [`rel_key_value`] of its primary key
    ///   and seeded pks run `1..=MAX_SEED_ROWS`;
    /// * `"k9"` — syntactically fine, matches nothing (no program ever seeds
    ///   pk 9), so a to-one enrichment must come back `NULL` and a to-many
    ///   aggregate must see the empty set;
    /// * `None` — a `NULL` foreign key, which under `LEFT JOIN` semantics
    ///   also matches nothing but reaches that answer down a different code
    ///   path (`NULL = anything` is `NULL`, not `false`).
    ///
    /// The "matches nothing" case is deliberately *not* rare: together with
    /// the `NULL` case it is the whole nullability contract issue #33
    /// pinned on the engine side, and a run that only ever drew matching
    /// keys would never check it.
    ///
    /// `awkward_values` gates the `NULL` draw only, exactly as it does for
    /// [`value`]/[`text_value`]/[`bool_value`]/[`uuid_value`] — a `NULL`
    /// foreign key is an awkward value like any other. The `"k9"`
    /// matches-nothing case is *not* gated: it is an ordinary non-`NULL`
    /// text value that simply happens to resolve to no related row, so the
    /// unmatched-join half of the nullability contract stays covered even
    /// with awkward values off.
    fn rel_fk_value(awkward_values: bool) -> BoxedStrategy<Option<String>> {
        let present = prop_oneof![
            2 => Just(Some("k1".to_string())),
            2 => Just(Some("k2".to_string())),
            2 => Just(Some("k3".to_string())),
            1 => Just(Some("k9".to_string())),
        ];
        if !awkward_values {
            return present.boxed();
        }
        prop_oneof![
            NULL_WEIGHT => Just(None),
            VALUE_WEIGHT => present,
        ]
        .boxed()
    }

    /// Which aggregate function wraps a generated relationship path.
    ///
    /// Every [`RelAggregateFn`] is legal in both places a path can be
    /// aggregated: over a to-many path in a row-grain definition, and over a
    /// to-one path inside a `GROUP BY` definition. `COUNT(<rel>.<col>)` is
    /// the interesting one in both: it is the only aggregate whose empty-set
    /// answer is `0` rather than `NULL` (the to-many half of ADR-0006's
    /// nullability rule), and inside a `GROUP BY` it counts only the source
    /// rows whose related value is non-`NULL` — a row whose foreign key
    /// misses, is `NULL`, or points at a row with a `NULL` `c1` still belongs
    /// to the group but must not be counted (issue #294; the parser has
    /// accepted `COUNT(<expr>)` since #120).
    fn rel_aggregate_fn() -> impl Strategy<Value = RelAggregateFn> {
        prop_oneof![
            Just(RelAggregateFn::Sum),
            Just(RelAggregateFn::Min),
            Just(RelAggregateFn::Max),
            Just(RelAggregateFn::Avg),
            Just(RelAggregateFn::Count),
        ]
    }

    /// The optional relationship enrichment for a definition sourced from
    /// table `source_index` of `table_count` (issue #34).
    ///
    /// Always `None` when `source_index` is the last table: a relationship
    /// must point at a strictly higher-indexed table so the cross-table
    /// dependency graph stays acyclic by construction (see [`RelFieldSpec`]).
    ///
    /// Otherwise the shape is fully determined by the definition's
    /// key-space, because only one of the three legal shapes fits each (see
    /// [`RelFieldKind`]'s table): a `GROUP BY` definition can only aggregate
    /// a *to-one* path, while a row-grain one can either read a to-one path
    /// bare or aggregate a *to-many* one. Drawing the shape from the
    /// already-drawn key-space — rather than independently, then filtering —
    /// is what keeps "the generator emits only valid programs" structural
    /// rather than probabilistic.
    ///
    /// Weighted 2:1 toward drawing a relationship at all, so the three
    /// relationship shapes get real coverage across a 16-case default run
    /// while plain relationship-free programs stay common.
    fn rel_field_spec(
        source_index: usize,
        table_count: usize,
        is_aggregate_def: bool,
    ) -> BoxedStrategy<Option<RelFieldSpec>> {
        if source_index + 1 >= table_count {
            return Just(None).boxed();
        }
        let kind = if is_aggregate_def {
            rel_aggregate_fn()
                .prop_map(RelFieldKind::ToOneAggregate)
                .boxed()
        } else {
            prop_oneof![
                1 => Just(RelFieldKind::ToOneBare),
                1 => rel_aggregate_fn().prop_map(RelFieldKind::ToManyAggregate),
            ]
            .boxed()
        };
        let present = ((source_index + 1)..table_count, kind)
            .prop_map(|(to_table, kind)| Some(RelFieldSpec { to_table, kind }));
        prop_oneof![
            1 => Just(None),
            2 => present,
        ]
        .boxed()
    }

    /// One definition's full draw (issue #34): which table it sources from,
    /// its [`DefShape`], its optional [`DerivedShape`], and its optional
    /// [`RelFieldSpec`]. Drawn as one unit — rather than four independent
    /// vectors zipped up later — so an illegal pairing (a derived field on
    /// an `Aggregate` def, a to-many relationship inside a `GROUP BY`, a
    /// relationship pointing at a table that can't be a to-side) is
    /// unrepresentable rather than merely unlikely.
    fn def_draw(
        table_count: usize,
    ) -> impl Strategy<Value = (usize, DefShape, Option<DerivedShape>, Option<RelFieldSpec>)> {
        (0..table_count, def_shape_and_derived()).prop_flat_map(
            move |(source_index, (shape, derived))| {
                let is_aggregate = matches!(shape, DefShape::Aggregate { .. });
                (
                    Just(source_index),
                    Just(shape),
                    Just(derived),
                    rel_field_spec(source_index, table_count, is_aggregate),
                )
            },
        )
    }

    /// Pairs a drawn [`DefShape`] with the optional [`DerivedShape`]
    /// (improvement-plan task B2) that rides along with it: `Some` when the
    /// shape is `OneToOne` (every `OneToOne` def always gets one derived
    /// field — the same "unconditional, not a probabilistically-drawn
    /// dimension" choice task B1 made for the `Text`/`Boolean`/`Uuid`
    /// columns), `None` when it's `Aggregate` (see [`DerivedShape`]'s and
    /// [`build_program_multi_with_shapes_and_derived`]'s doc comments for why
    /// a derived field can never legally attach to an `Aggregate` def).
    /// Drawing the pair together, rather than two independent vectors zipped
    /// up later, makes it structurally impossible for
    /// `trivial_program_with` to draw a `Some` paired with `Aggregate`.
    fn def_shape_and_derived() -> impl Strategy<Value = (DefShape, Option<DerivedShape>)> {
        def_shape().prop_flat_map(|shape| {
            let derived = match &shape {
                DefShape::OneToOne => derived_shape().prop_map(Some).boxed(),
                DefShape::Aggregate { .. } => Just(None).boxed(),
            };
            (Just(shape), derived)
        })
    }

    /// One mutate targeting `seed_count` seeded rows. The primary key for
    /// `Update`/`Delete` is drawn from `1..=seed_count + 1`: values
    /// `1..=seed_count` hit a seeded (or otherwise still-live) row, and
    /// `seed_count + 1` deliberately misses (a source no-op) so the property
    /// exercises the "op that errors changed nothing" path (design doc §4).
    /// `DuplicateInsert`'s pk is drawn from `1..=seed_count` only — a pk
    /// that started out seeded. Whether it actually collides at apply time
    /// depends on whether an earlier mutate in the same draw already deleted
    /// it ([`build_program`] simulates this to expect the right outcome
    /// either way, see [`Mutate::DuplicateInsert`]); most of the time it is
    /// still live, so this is the generator's main source of genuine
    /// primary-key-violation `apply()` errors (issue #6's gap).
    /// `UpdateFlag` (issue #255) draws its pk exactly like `Update`, at
    /// weight `2`: it only matters to a def reading the `Boolean` column, so
    /// it shouldn't crowd out the `c1`/`c2` writes every other def reads.
    /// `Truncate` (improvement-plan task E6) is weighted at `1` against the
    /// other per-pk variants' `3` apiece — rare enough that most mutates still
    /// exercise the ordinary per-pk paths (a `Truncate` wipes every remaining
    /// live pk at once, so drawing it often would starve `Update`/`Delete`/
    /// `DuplicateInsert` of live rows to act on across the rest of the same
    /// mutate stream), common enough that a handful of seed rows/mutates
    /// reliably hits one across many samples — the same
    /// rare-but-reliable balance [`NULL_WEIGHT`]/[`AWKWARD_TEXT_WEIGHT`]
    /// strike for their own awkward values.
    ///
    /// `ToggleNull` (issue #505) draws its pk like `Update`, at weight `2`
    /// and only with awkward values on (it writes `NULL`). It thins the other
    /// variants by about an eighth; see [`Mutate::ToggleNull`] for what it
    /// buys.
    fn mutate(seed_count: usize, awkward_values: bool) -> BoxedStrategy<Mutate> {
        let pk = 1..=(seed_count as i64 + 1);
        let dup_pk = 1..=(seed_count as i64);
        let update = (pk.clone(), value(awkward_values), value(awkward_values))
            .prop_map(|(pk, c1, c2)| Mutate::Update { pk, c1, c2 });
        let update_flag = (pk.clone(), bool_value(awkward_values))
            .prop_map(|(pk, flag)| Mutate::UpdateFlag { pk, flag });
        let toggle_null =
            (pk.clone(), 0..=VALUE_MAX).prop_map(|(pk, value)| Mutate::ToggleNull { pk, value });
        let delete = pk.prop_map(|pk| Mutate::Delete { pk });
        let duplicate_insert = (dup_pk, value(awkward_values), value(awkward_values))
            .prop_map(|(pk, c1, c2)| Mutate::DuplicateInsert { pk, c1, c2 });
        // Issue #505: `ToggleNull` only with awkward values on, since it
        // writes `NULL`. It is left out of the union rather than weighted
        // `0`, because a `prop_oneof!` shrinks toward its earlier arms
        // whatever their weight: a weight-`0` arm would still be reached by
        // shrinking a `Delete`, `DuplicateInsert` or `Truncate`.
        if awkward_values {
            prop_oneof![
                3 => update,
                2 => update_flag,
                TOGGLE_NULL_WEIGHT => toggle_null,
                3 => delete,
                3 => duplicate_insert,
                1 => Just(Mutate::Truncate),
            ]
            .boxed()
        } else {
            prop_oneof![
                3 => update,
                2 => update_flag,
                3 => delete,
                3 => duplicate_insert,
                1 => Just(Mutate::Truncate),
            ]
            .boxed()
        }
    }

    /// [`Mutate::ToggleNull`]'s weight in [`mutate`] (issue #505), next to
    /// `Update`'s `3` and `UpdateFlag`'s `2`.
    const TOGGLE_NULL_WEIGHT: u32 = 2;

    /// One drawn table's seed rows and mutate stream (see [`TableSpec`]):
    /// seed `1..=MAX_SEED_ROWS` rows with random values (now including one
    /// `Text`/`Boolean`/`Uuid` value and one grain value per row, tasks
    /// B1/B4), then append `0..=MAX_MUTATES` mutates over the numeric columns
    /// and the Boolean column (the other new columns are never mutated — see
    /// the module doc comment's scope cuts) — the per-table shape
    /// [`trivial_program_with`] always drew, now reusable once per table in a
    /// multi-table program (improvement-plan task B3).
    fn table_spec(awkward_values: bool) -> impl Strategy<Value = TableSpec> {
        (1..=MAX_SEED_ROWS)
            .prop_flat_map(move |seed_count| {
                let seeds = prop::collection::vec(
                    (value(awkward_values), value(awkward_values)),
                    seed_count,
                );
                let texts = prop::collection::vec(text_value(awkward_values), seed_count);
                let bools = prop::collection::vec(bool_value(awkward_values), seed_count);
                let uuids = prop::collection::vec(uuid_value(awkward_values), seed_count);
                let grains = prop::collection::vec(grain_value(), seed_count);
                let rel_fks = prop::collection::vec(rel_fk_value(awkward_values), seed_count);
                let mutates =
                    prop::collection::vec(mutate(seed_count, awkward_values), 0..=MAX_MUTATES);
                (seeds, texts, bools, uuids, grains, rel_fks, mutates)
            })
            .prop_map(
                |(
                    seed_values,
                    text_values,
                    bool_values,
                    uuid_values,
                    grain_values,
                    rel_fk_values,
                    mutates,
                )| {
                    TableSpec {
                        seed_values,
                        text_values,
                        bool_values,
                        uuid_values,
                        grain_values,
                        rel_fk_values,
                        mutates,
                    }
                },
            )
    }

    /// Draws a [`Program`] over `1..=MAX_TABLES` tables and `1..=MAX_DEFS`
    /// definitions (improvement-plan task B3): each table independently
    /// draws its own seed/mutate stream ([`table_spec`]), and each
    /// definition independently draws both *which* table it reads from —
    /// uniformly over `0..table_count`, with no bias toward distinct
    /// sources — and (task B4) *which shape* it takes, paired (task B2) with
    /// an optional derived field ([`def_shape_and_derived`]), so "two
    /// definitions sharing one source table" and "definitions spread across
    /// different tables" are both reachable in the same program, including a
    /// mix of `OneToOne` and `Aggregate` defs, each `OneToOne` def also
    /// carrying its own independently-drawn derived field. Everything maps
    /// through [`build_program_multi_with_shapes_and_derived`], so
    /// proptest's integrated shrinking reduces the def count, then the table
    /// count (both ahead of any table's row count, values, or mutate
    /// stream — the same `1..=N`-via-`prop_flat_map` idiom
    /// [`MAX_SEED_ROWS`]/[`MAX_MUTATES`] already use), toward the smallest
    /// reproducing program — a 1-table/1-def counterexample is the readable
    /// one.
    ///
    /// Issue #34: each definition additionally draws an optional
    /// relationship enrichment ([`rel_field_spec`]), so a multi-table
    /// program's definitions sometimes read a *related* table too. The
    /// relationship's shape is derived from the definition's already-drawn
    /// key-space rather than drawn independently, so an illegal pairing is
    /// unrepresentable — see [`def_draw`].
    ///
    /// `awkward_values` gates every other column's `NULL` draw (see
    /// [`value`], [`text_value`], [`bool_value`], [`uuid_value`],
    /// [`rel_fk_value`]); it does
    /// not gate [`grain_value`] (which draws its own occasional `NULL`
    /// unconditionally — see its own doc comment for issue #128, the engine
    /// bug that used to force a scope cut here), nor does it affect
    /// [`Mutate::DuplicateInsert`], [`def_shape`] (which of a def's fields
    /// are `SUM`/`COUNT`/`AVG`/`MIN`/`MAX`/`BOOL_AND`/`BOOL_OR` over), or
    /// [`DerivedShape`] (every `OneToOne` def always gets one `derived`
    /// field) — all distinct, structural widenings, always available
    /// regardless of the flag.
    ///
    /// Issue #505: the built program's per-table op streams are then
    /// interleaved by a drawn schedule ([`interleave_schedule`],
    /// [`interleave_tables`]), so parent-side and from-side ops mix. The
    /// schedule shrinks toward the table-by-table order.
    pub fn trivial_program_with(awkward_values: bool) -> impl Strategy<Value = Program> {
        prop::collection::vec(table_spec(awkward_values), 1..=MAX_TABLES)
            .prop_flat_map(|tables| {
                let table_count = tables.len();
                let defs = prop::collection::vec(def_draw(table_count), 1..=MAX_DEFS);
                let op_count = tables
                    .iter()
                    .map(|t| t.seed_values.len() + t.mutates.len())
                    .sum();
                // The schedule comes before the defs so it shrinks first
                // (see `interleave_schedule`).
                (Just(tables), interleave_schedule(op_count), defs)
            })
            .prop_map(|(tables, schedule, draws)| {
                let mut defs: Vec<(usize, DefShape)> = Vec::with_capacity(draws.len());
                let mut derived: Vec<Option<DerivedShape>> = Vec::with_capacity(draws.len());
                let mut rel_fields: Vec<Option<RelFieldSpec>> = Vec::with_capacity(draws.len());
                for (idx, shape, derived_shape, rel_field) in draws {
                    defs.push((idx, shape));
                    derived.push(derived_shape);
                    rel_fields.push(rel_field);
                }
                interleave_tables(
                    build_program_multi_with_relationships(&tables, &defs, &derived, &rel_fields),
                    &schedule,
                )
            })
    }

    /// The [`interleave_tables`] schedule `trivial_program_with` draws (issue
    /// #505): one uniform pick per op slot. Each entry shrinks toward `0`,
    /// and an all-zero schedule is the table-by-table order, so a shrunk
    /// counterexample keeps only the interleaving it needs to fail.
    ///
    /// Proptest caps shrinking at `4 * cases` iterations, and each op the
    /// flat-map's outer shrink removes redraws this schedule at random, so
    /// the schedule has to be cheap to shrink or the cap lands mid-schedule
    /// and leaves a needlessly interleaved counterexample. It is therefore
    /// exactly `op_count` long (no dead entries past the last op to walk
    /// down to `0`), and `trivial_program_with` shrinks it before the defs.
    fn interleave_schedule(op_count: usize) -> impl Strategy<Value = Vec<usize>> {
        prop::collection::vec(0..INTERLEAVE_PICK_RANGE, op_count)
    }

    /// The generator's default strategy: awkward values (NULLs) on, so real
    /// runs exercise them (design doc §3).
    pub fn trivial_program() -> impl Strategy<Value = Program> {
        trivial_program_with(true)
    }

    /// One hot-key mutate against `seed_count` seeded rows (issue #557):
    /// half the draws pick a pk from the first [`HOT_CORE_KEYS`], so a few
    /// keys are much hotter than the rest. Mostly value updates, with enough
    /// [`Mutate::MoveGroup`]s, deletes and revivals that groups gain, lose
    /// and regain members inside one burst. Revivals outweigh deletes two to
    /// one, so about two thirds of the rows are live at any time and most
    /// writes land; a revival of a live row is a rejected insert, which is a
    /// shape worth keeping too. No `Truncate`: it forces its
    /// whole batch to seal as one bucket and pins its table to one lane,
    /// which is the opposite of what this shape is for, and the serial tiers
    /// already draw it. Every pk is a seeded one, so a miss only happens
    /// after a delete.
    fn hot_mutate(seed_count: usize) -> BoxedStrategy<Mutate> {
        let seeds = seed_count as i64;
        let pk = || prop_oneof![1 => 1..=seeds, 1 => 1..=HOT_CORE_KEYS.min(seeds)];
        let grain = prop_oneof![
            VALUE_WEIGHT => (0..=HOT_GRAIN_MAX).prop_map(Some),
            NULL_WEIGHT => Just(None),
        ];
        prop_oneof![
            6 => (pk(), value(true), value(true))
                .prop_map(|(pk, c1, c2)| Mutate::Update { pk, c1, c2 }),
            1 => (pk(), bool_value(true)).prop_map(|(pk, flag)| Mutate::UpdateFlag { pk, flag }),
            1 => (pk(), 0..=VALUE_MAX).prop_map(|(pk, value)| Mutate::ToggleNull { pk, value }),
            3 => (pk(), grain).prop_map(|(pk, grain)| Mutate::MoveGroup { pk, grain }),
            1 => pk().prop_map(|pk| Mutate::Delete { pk }),
            2 => (pk(), value(true), value(true))
                .prop_map(|(pk, c1, c2)| Mutate::DuplicateInsert { pk, c1, c2 }),
        ]
        .boxed()
    }

    /// Slides every [`Mutate::MoveGroup`]'s drawn grain up by
    /// [`HOT_GROUP_SLIDE`] per [`HOT_GROUP_EPOCH`] mutates before it (issue
    /// #557), so the group window moves through the run.
    fn slide_group_window(mut mutates: Vec<Mutate>) -> Vec<Mutate> {
        for (index, mutate) in mutates.iter_mut().enumerate() {
            if let Mutate::MoveGroup {
                grain: Some(grain), ..
            } = mutate
            {
                *grain += (index / HOT_GROUP_EPOCH) as i64 * HOT_GROUP_SLIDE;
            }
        }
        mutates
    }

    /// [`table_spec`] with the hot-key shape's ranges and [`hot_mutate`]
    /// (issue #557).
    fn hot_table_spec(
        seeds: std::ops::RangeInclusive<usize>,
        mutates: std::ops::RangeInclusive<usize>,
    ) -> impl Strategy<Value = TableSpec> {
        seeds
            .prop_flat_map(move |seed_count| {
                (
                    prop::collection::vec((value(true), value(true)), seed_count),
                    prop::collection::vec(text_value(true), seed_count),
                    prop::collection::vec(bool_value(true), seed_count),
                    prop::collection::vec(uuid_value(true), seed_count),
                    prop::collection::vec(grain_value(), seed_count),
                    prop::collection::vec(rel_fk_value(true), seed_count),
                    prop::collection::vec(hot_mutate(seed_count), mutates.clone()),
                )
            })
            .prop_map(
                |(
                    seed_values,
                    text_values,
                    bool_values,
                    uuid_values,
                    grain_values,
                    rel_fk_values,
                    mutates,
                )| TableSpec {
                    seed_values,
                    text_values,
                    bool_values,
                    uuid_values,
                    grain_values,
                    rel_fk_values,
                    mutates: slide_group_window(mutates),
                },
            )
    }

    /// The most mutates the hot-key shape's optional second table draws. It
    /// is there to be a relationship's parent, written now and then while
    /// the hot table churns, not to be hot itself.
    const HOT_PARENT_MUTATES: usize = 60;

    /// The concurrent tier's hot-key case (issue #557): a hot table of
    /// [`HOT_KEYS`] rows taking [`HOT_MUTATES`] writes, always read by a
    /// `GROUP BY` definition over its grain column, plus up to two more
    /// drawn definitions. Half the cases add a small second table that
    /// definitions can reach through a relationship ([`rel_field_spec`]),
    /// its ops spread evenly through the hot table's (see
    /// [`spread_schedule`]).
    ///
    /// Issued in bursts of [`HOT_BURST_SIZE`] ops over `2..=MAX_LANES`
    /// lanes ([`concurrent_plan`]), against an engine with [`HOT_WORKERS`]
    /// drain workers sealing every [`HOT_SEAL_INTERVAL_MS`].
    pub fn hot_key_case() -> impl Strategy<Value = ConcurrentCase> {
        hot_key_case_with(Just(CaseExtras::None).boxed())
    }

    /// The concurrent tier's mid-burst case (issue #557 part 2): a
    /// [`hot_key_case`] whose bursts also stage re-reads while CDC for the
    /// same keys is in flight.
    ///
    /// - Three cases in four install one more `GROUP BY` definition over
    ///   the hot table mid-burst (always with `COUNT(*)`), so its build runs
    ///   while the hot table takes writes: #625's build-under-load shape.
    ///   Half of those are invertible-only ([`install_functions`]), so the
    ///   Re-derive build runs it (#625 F3).
    /// - One to [`MAX_ACTION_DRAWS`] operator actions ([`ActionDraw`]),
    ///   placed by [`add_burst_actions`]: `request_backfill`, a pause and
    ///   resume, or a column pause and resume.
    /// - Three cases in four that have a relationship's to-side table also
    ///   truncate it once, somewhere in its op stream, while the from-side
    ///   hot table keeps changing. Parent updates come with the to-side
    ///   table's ops, spread through the hot table's (#505).
    pub fn mid_burst_case() -> impl Strategy<Value = ConcurrentCase> {
        hot_key_case_with(mid_burst_draws().prop_map(CaseExtras::MidBurst).boxed())
    }

    /// The concurrent tier's steady-load case (#720, #725): a
    /// [`mid_burst_case`] with no relationship, run under a steady load
    /// ([`SteadyLoad`]).
    ///
    /// - Each lane waits a drawn [`LOAD_PACE_MICROS`] after every op, from
    ///   an action that starts a build to the end of its burst, so the rest
    ///   of the burst's writes trickle in while the build runs.
    /// - Every page and build chunk stalls now and then at its entry-lock
    ///   step, for up to [`LOAD_STALL_SEALS`] seal intervals, so some pages
    ///   are slow next to others and segments drain out of order.
    /// - Its builds plan [`LOAD_CHUNK_ROWS`] rows per chunk, and start at a
    ///   reconcile pass on the seal cadence rather than the engine's 5s
    ///   default, so they run while the burst that started them writes.
    ///
    /// A case with a relationship is drawn again: under the load, the
    /// relationship-fed shapes diverged in about one case in ten (#719,
    /// #726), more than a sweep's baseline bar, before #623 D5 put them on
    /// the ledger, and they haven't been measured enough since to take back
    /// in. Drawn apart from [`mid_burst_case`] so that tier's cases, seed for
    /// seed, stay what they were.
    pub fn steady_load_case() -> impl Strategy<Value = ConcurrentCase> {
        let extras = (mid_burst_draws(), load_draws()).prop_map(|(mut draws, load)| {
            draws.load = Some(load);
            CaseExtras::MidBurst(draws)
        });
        hot_key_case_with(extras.boxed()).prop_filter(
            "a steady-load case has no relationship (#719, #726)",
            |case| case.program.relationships.is_empty(),
        )
    }

    /// The concurrent tier's cooling-key case (issue #557 part 3b): a
    /// [`hot_key_case`] plus a cooling table read by a 1-1 definition.
    ///
    /// The cooling table has [`COOLING_KEYS`] keys. Each is inserted, then
    /// written one to three more times close together (one key in five
    /// ending in a delete), and never again, with its last write in the
    /// middle of a burst ([`place_cooling_keys`]). The hot table keeps
    /// taking writes all around them. The engine seals every
    /// [`COOLING_SEAL_INTERVAL_MS`], so a key's writes seal into different
    /// batches that drain alongside each other: the shape where a 1-1 write
    /// applied out of order (#344) survives to the end of the burst,
    /// because no later change to the key heals it.
    pub fn cooling_key_case() -> impl Strategy<Value = ConcurrentCase> {
        hot_key_case_with(cooling_draws().prop_map(CaseExtras::Cooling).boxed())
    }

    /// What a case adds to the hot-key shape.
    #[derive(Debug, Clone)]
    enum CaseExtras {
        None,
        MidBurst(MidBurstDraws),
        Cooling(CoolingDraws),
    }

    /// What [`cooling_key_case`] adds to a [`hot_key_case`].
    #[derive(Debug, Clone)]
    struct CoolingDraws {
        /// One seeded row per cooling key, and each key's later writes, key
        /// by key.
        table: TableSpec,
        /// The 1-1 definition's derived field.
        derived: DerivedShape,
        places: Vec<CoolingPlace>,
        workers: usize,
        seal_interval_ms: u64,
    }

    /// Pk `pk`'s version of a drawn cooling write.
    fn with_pk(mutate: &Mutate, pk: i64) -> Mutate {
        match mutate.clone() {
            Mutate::Update { c1, c2, .. } => Mutate::Update { pk, c1, c2 },
            Mutate::UpdateFlag { flag, .. } => Mutate::UpdateFlag { pk, flag },
            Mutate::Delete { .. } => Mutate::Delete { pk },
            other => unreachable!("cooling writes are updates and deletes, not {other:?}"),
        }
    }

    /// One cooling key's writes after its insert (the pk is filled in
    /// later): mostly value updates, some flag updates, and one key in five
    /// ends in a delete.
    fn cooling_writes() -> impl Strategy<Value = Vec<Mutate>> {
        let write = prop_oneof![
            4 => (value(true), value(true)).prop_map(|(c1, c2)| Mutate::Update { pk: 0, c1, c2 }),
            1 => bool_value(true).prop_map(|flag| Mutate::UpdateFlag { pk: 0, flag }),
        ];
        (
            prop::collection::vec(
                write,
                (COOLING_WRITES.start() - 1)..=(COOLING_WRITES.end() - 1),
            ),
            prop::bool::weighted(0.2),
        )
            .prop_map(|(mut writes, delete)| {
                if delete {
                    *writes.last_mut().expect("at least one write") = Mutate::Delete { pk: 0 };
                }
                writes
            })
    }

    fn cooling_draws() -> impl Strategy<Value = CoolingDraws> {
        let place = (0..64usize, COOLING_LAST, COOLING_GAP)
            .prop_map(|(burst, last, gap)| CoolingPlace { burst, last, gap });
        COOLING_KEYS
            .prop_flat_map(move |keys| {
                (
                    prop::collection::vec((value(true), value(true)), keys),
                    prop::collection::vec(text_value(true), keys),
                    prop::collection::vec(bool_value(true), keys),
                    prop::collection::vec(uuid_value(true), keys),
                    prop::collection::vec(cooling_writes(), keys),
                    prop::collection::vec(place.clone(), keys),
                    derived_shape(),
                    COOLING_WORKERS,
                    COOLING_SEAL_INTERVAL_MS,
                )
            })
            .prop_map(
                |(
                    seed_values,
                    text_values,
                    bool_values,
                    uuid_values,
                    writes,
                    places,
                    derived,
                    workers,
                    seal_interval_ms,
                )| {
                    let keys = seed_values.len();
                    let mutates = writes
                        .iter()
                        .zip(1i64..)
                        .flat_map(|(writes, pk)| writes.iter().map(move |w| with_pk(w, pk)))
                        .collect();
                    CoolingDraws {
                        table: TableSpec {
                            seed_values,
                            text_values,
                            bool_values,
                            uuid_values,
                            // Only the 1-1 definition reads the cooling
                            // table: no group, no relationship.
                            grain_values: vec![None; keys],
                            rel_fk_values: vec![None; keys],
                            mutates,
                        },
                        derived,
                        places,
                        workers,
                        seal_interval_ms,
                    }
                },
            )
    }

    /// The most [`ActionDraw`]s one [`mid_burst_case`] draws.
    pub const MAX_ACTION_DRAWS: usize = 4;

    /// The longest a drawn pause lasts before its resume, in thousandths of
    /// the burst. Short enough that most resumes fire with a good part of
    /// the burst still to be written, rather than piling up at its end.
    const MAX_ACTION_SPAN: u16 = 400;

    /// What [`mid_burst_case`] adds to a [`hot_key_case`].
    #[derive(Debug, Clone)]
    struct MidBurstDraws {
        /// Where in the to-side table's mutates a `TRUNCATE` goes, in
        /// thousandths.
        parent_truncate: Option<u16>,
        /// A `GROUP BY` definition over the hot table, and where in the
        /// program it installs, in thousandths of the ops.
        deferred: Option<(Vec<AggregateFn>, u16)>,
        actions: Vec<ActionDraw>,
        /// The steady load: a [`steady_load_case`]'s, `None` for a
        /// [`mid_burst_case`].
        load: Option<LoadDraws>,
    }

    /// The steady load a [`steady_load_case`] runs under (#720, #725).
    #[derive(Debug, Clone, Copy)]
    struct LoadDraws {
        /// [`ConcurrentCase::build_chunk_rows`]: a few rows, so a build over
        /// the hot table's [`HOT_KEYS`] rows runs as many chunks, each a
        /// claim of its own, spread through the burst, as a build of a large
        /// table does at the engine's default.
        chunk_rows: i64,
        /// [`SteadyLoad::pace`], in microseconds.
        pace_micros: u64,
        /// [`SteadyLoad::stall`], in seal intervals.
        stall_seals: u32,
    }

    /// The range of rows per build chunk a [`steady_load_case`] draws.
    pub const LOAD_CHUNK_ROWS: std::ops::RangeInclusive<i64> = 1..=4;

    /// The range of [`SteadyLoad::pace`]s a [`steady_load_case`] draws, in
    /// microseconds: a lane's few hundred ops take a few hundred
    /// milliseconds, long enough for a build of the hot table to run under
    /// them.
    pub const LOAD_PACE_MICROS: std::ops::RangeInclusive<u64> = 500..=2_000;

    /// The range of [`SteadyLoad::stall`]s a [`steady_load_case`] draws, in
    /// multiples of its seal interval. A page that stalls before its entry
    /// lock for over two seal intervals can still be waiting when a later
    /// segment has sealed and drained and the maintenance tick after that
    /// collects tombstones: the shape `early_tombstone_gc` needs.
    pub const LOAD_STALL_SEALS: std::ops::RangeInclusive<u32> = 2..=4;

    fn load_draws() -> impl Strategy<Value = LoadDraws> {
        (LOAD_CHUNK_ROWS, LOAD_PACE_MICROS, LOAD_STALL_SEALS).prop_map(
            |(chunk_rows, pace_micros, stall_seals)| LoadDraws {
                chunk_rows,
                pace_micros,
                stall_seals,
            },
        )
    }

    fn mid_burst_draws() -> impl Strategy<Value = MidBurstDraws> {
        let action = (
            // A column pair only applies when the case has a 1-1
            // definition, which about half don't, so it is drawn twice as
            // often.
            prop_oneof![
                1 => Just(ActionKind::RequestBackfill),
                1 => Just(ActionKind::PauseResume),
                2 => Just(ActionKind::ColumnPauseResume),
            ],
            0..8usize,
            0..16usize,
            0..=1000u16,
            0..=MAX_ACTION_SPAN,
        )
            .prop_map(|(kind, burst, target, start, span)| ActionDraw {
                kind,
                burst,
                target,
                start,
                span,
            });
        (
            prop::option::weighted(0.75, 0..=1000u16),
            prop::option::weighted(0.75, (install_functions(), 0..=1000u16)),
            prop::collection::vec(action, 1..=MAX_ACTION_DRAWS),
        )
            .prop_map(|(parent_truncate, deferred, actions)| MidBurstDraws {
                parent_truncate,
                deferred,
                actions,
                load: None,
            })
    }

    /// The fields of [`mid_burst_case`]'s mid-burst install: half the time
    /// any of [`aggregate_functions`], and half the time only `SUM`, `AVG`
    /// and `COUNT` of plain columns, the shape the Re-derive build takes
    /// (#625 F3). Few draws of the first kind are invertible-only, so
    /// without the second the build (and its plants) would run in few cases.
    fn install_functions() -> impl Strategy<Value = Vec<AggregateFn>> {
        let invertible = (
            proptest::sample::subsequence(vec![FnKind::Sum, FnKind::Count, FnKind::Avg], 1..=3),
            aggregate_column(),
            aggregate_column(),
        )
            .prop_map(|(kinds, sum_col, avg_col)| {
                kinds
                    .into_iter()
                    .map(|kind| match kind {
                        FnKind::Sum => AggregateFn::Sum(sum_col),
                        FnKind::Avg => AggregateFn::Avg(avg_col),
                        _ => AggregateFn::Count,
                    })
                    .collect::<Vec<_>>()
            });
        prop_oneof![1 => aggregate_functions(), 1 => invertible]
    }

    fn hot_key_case_with(
        extras: BoxedStrategy<CaseExtras>,
    ) -> impl Strategy<Value = ConcurrentCase> {
        (1..=2usize)
            .prop_flat_map(move |table_count| {
                (
                    hot_table_spec(HOT_KEYS, HOT_MUTATES),
                    prop::collection::vec(
                        hot_table_spec(3..=MAX_SEED_ROWS, 0..=HOT_PARENT_MUTATES),
                        table_count - 1,
                    ),
                    (aggregate_functions(), rel_field_spec(0, table_count, true)),
                    prop::collection::vec(def_draw(table_count), 0..MAX_DEFS),
                    HOT_BURST_SIZE,
                    2..=MAX_LANES,
                    HOT_WORKERS,
                    HOT_SEAL_INTERVAL_MS,
                    extras.clone(),
                )
            })
            .prop_map(
                |(
                    hot,
                    others,
                    (functions, hot_rel),
                    extra,
                    burst_size,
                    lanes,
                    workers,
                    seal,
                    extras,
                )| {
                    let mut tables = vec![hot];
                    tables.extend(others);
                    let mut defs = vec![(0, DefShape::Aggregate { functions })];
                    let mut derived = vec![None];
                    let mut rel_fields = vec![hot_rel];
                    for (source, shape, derived_shape, rel_field) in extra {
                        defs.push((source, shape));
                        derived.push(derived_shape);
                        rel_fields.push(rel_field);
                    }
                    let (extras, cooling) = match extras {
                        CaseExtras::None => (None, None),
                        CaseExtras::MidBurst(extras) => (Some(extras), None),
                        CaseExtras::Cooling(cooling) => (None, Some(cooling)),
                    };
                    let extras = extras.unwrap_or(MidBurstDraws {
                        parent_truncate: None,
                        deferred: None,
                        actions: Vec::new(),
                        load: None,
                    });
                    if let (Some(at), Some(parent)) = (extras.parent_truncate, tables.get_mut(1)) {
                        let position = usize::from(at) * parent.mutates.len() / 1000;
                        parent.mutates.insert(position, Mutate::Truncate);
                    }
                    if let Some((functions, _)) = &extras.deferred {
                        let mut functions = functions.clone();
                        if !functions.contains(&AggregateFn::Count) {
                            functions.insert(0, AggregateFn::Count);
                        }
                        defs.push((0, DefShape::Aggregate { functions }));
                        derived.push(None);
                        rel_fields.push(None);
                    }
                    // The cooling table goes last, so no relationship drawn
                    // above can point at it.
                    let cooling_table = tables.len();
                    if let Some(cooling) = &cooling {
                        tables.push(cooling.table.clone());
                        defs.push((cooling_table, DefShape::OneToOne));
                        derived.push(Some(cooling.derived.clone()));
                        rel_fields.push(None);
                    }
                    let program = build_program_multi_with_relationships(
                        &tables,
                        &defs,
                        &derived,
                        &rel_fields,
                    );
                    let mut program = match &cooling {
                        Some(cooling) => {
                            place_cooling_keys(program, cooling_table, &cooling.places, burst_size)
                        }
                        None => {
                            let schedule = spread_schedule(&program);
                            interleave_tables(program, &schedule)
                        }
                    };
                    if let Some((_, at)) = extras.deferred {
                        let (def, ops) = (program.defs.len() - 1, program.ops.len());
                        let after_op = 1 + usize::from(at) * (ops - 2) / 1000;
                        program = defer_def_install(program, def, after_op);
                    }
                    let load = extras.load;
                    let mut plan = concurrent_plan(&program, burst_size, lanes);
                    plan.steady_load = load.map(|load| SteadyLoad {
                        pace: std::time::Duration::from_micros(load.pace_micros),
                        stall: std::time::Duration::from_millis(seal * u64::from(load.stall_seals)),
                    });
                    let plan = add_burst_actions(plan, &program, &extras.actions);
                    ConcurrentCase {
                        program,
                        burst_size,
                        plan,
                        workers: cooling.as_ref().map_or(workers, |c| c.workers),
                        seal_interval_ms: cooling.map_or(seal, |c| c.seal_interval_ms),
                        build_chunk_rows: load.map(|load| load.chunk_rows),
                        // A build starts at the reconcile pass after its
                        // install or resume: at the engine's 5s default,
                        // long after the burst that started it.
                        reconcile_interval_ms: load.map(|_| seal),
                    }
                },
            )
    }

    /// An [`interleave_tables`] schedule that spreads every table's ops
    /// evenly through the program: each step emits from the table furthest
    /// behind its own share (issue #557). The hot-key shape uses it rather
    /// than drawing a schedule, because a drawn one over ~1,000 ops is slow
    /// to shrink and a burst is issued concurrently anyway.
    fn spread_schedule(program: &Program) -> Vec<usize> {
        let totals: Vec<usize> = table_streams(program).iter().map(Vec::len).collect();
        let mut emitted = vec![0usize; totals.len()];
        let mut schedule = Vec::with_capacity(program.ops.len());
        for _ in 0..program.ops.len() {
            let table = furthest_behind(&totals, &emitted)
                .expect("a table still has ops while steps remain");
            let pick = (0..table).filter(|&t| emitted[t] < totals[t]).count();
            emitted[table] += 1;
            schedule.push(pick);
        }
        schedule
    }

    /// A single [`NoiseAction`] against [`adversarial_noise_table`]'s shape
    /// (task E1): a small pk domain (so `Update`/`Delete` sometimes land on a
    /// row an earlier `Insert` in the same draw actually seeded, and
    /// sometimes miss — mirroring [`mutate`]'s own pk-liveness-agnostic
    /// design, since nothing here checks a noise op's outcome either way),
    /// an occasional `NULL` value, and a small fixed pool of column names for
    /// `AddColumn`/`DropColumn` that includes both a plausible fresh name
    /// (`"extra1"`/`"extra2"`) and the noise table's own real column names
    /// (`"c0"`/`"total"`) — so a drawn program sometimes tries to add a
    /// column that already exists, or drop the pk itself, both of which
    /// Postgres rejects and [`ManualBackend::fire_noise_event`]'s error
    /// swallowing must tolerate.
    ///
    /// [`ManualBackend::fire_noise_event`]: crate::backend::ManualBackend::fire_noise_event
    fn noise_pk() -> impl Strategy<Value = i64> {
        1i64..=6
    }

    fn noise_value() -> impl Strategy<Value = Option<String>> {
        prop_oneof![
            3 => "[a-zA-Z0-9]{0,6}".prop_map(Some),
            1 => Just(None),
        ]
    }

    fn noise_column_name() -> impl Strategy<Value = String> {
        proptest::sample::select(&["extra1", "extra2", "total", "c0"][..]).prop_map(str::to_string)
    }

    fn noise_extra_type() -> impl Strategy<Value = ValueType> {
        prop_oneof![Just(ValueType::Numeric), Just(ValueType::Text)]
    }

    fn noise_action() -> impl Strategy<Value = NoiseAction> {
        prop_oneof![
            (noise_pk(), noise_value()).prop_map(|(pk, value)| NoiseAction::Insert { pk, value }),
            (noise_pk(), noise_value()).prop_map(|(pk, value)| NoiseAction::Update { pk, value }),
            noise_pk().prop_map(|pk| NoiseAction::Delete { pk }),
            (noise_column_name(), noise_extra_type())
                .prop_map(|(name, value_type)| NoiseAction::AddColumn { name, value_type }),
            noise_column_name().prop_map(|name| NoiseAction::DropColumn { name }),
        ]
    }

    /// Draws a task-E1 noise plan sized to a program with `op_count` real
    /// ops: [`adversarial_noise_table`] (shared across every draw — task E1
    /// only asks for "one extra noise table", not a whole drawn-table-shape
    /// dimension) plus 0-5 [`NoiseEvent`]s at random positions in
    /// `0..=op_count`, so a drawn plan sometimes fires before the very first
    /// op, sometimes after the very last, and sometimes not at all.
    pub fn noise_plan_for(op_count: usize) -> impl Strategy<Value = NoisePlan> {
        prop::collection::vec((0..=op_count, noise_action()), 0..=5).prop_map(|events| NoisePlan {
            table: Some(adversarial_noise_table()),
            events: events
                .into_iter()
                .map(|(before_op, action)| NoiseEvent {
                    before_op,
                    kind: NoiseEventKind::Table(action),
                })
                .collect(),
        })
    }

    /// Draws a task-E5 administration-only noise plan: no noise table at
    /// all, just 0-3 bare `CHECKPOINT` statements at random positions in
    /// `0..=op_count`.
    pub fn checkpoint_plan_for(op_count: usize) -> impl Strategy<Value = NoisePlan> {
        prop::collection::vec(0..=op_count, 0..=3).prop_map(|positions| NoisePlan {
            table: None,
            events: positions
                .into_iter()
                .map(|before_op| NoiseEvent {
                    before_op,
                    kind: NoiseEventKind::Admin("CHECKPOINT".to_string()),
                })
                .collect(),
        })
    }

    /// Issue #236: one [`DbAdminAction`], weighted toward the cheap ones.
    fn db_admin_action() -> impl Strategy<Value = DbAdminAction> {
        prop_oneof![
            2 => Just(DbAdminAction::Checkpoint),
            2 => Just(DbAdminAction::RestartPostgres(RestartMode::Fast)),
            1 => Just(DbAdminAction::RestartPostgres(RestartMode::Immediate)),
        ]
    }

    /// Issue #236: a database-administration plan for `program`, 0-3 events
    /// anchored anywhere in its ops.
    ///
    /// At most three, so a short program isn't dominated by restarts.
    pub fn db_admin_plan_for(program: &Program) -> impl Strategy<Value = DbAdminPlan> + use<> {
        let op_count = program.ops.len();
        let event =
            (0..op_count, db_admin_action()).prop_map(|(op, action)| DbAdminEvent { op, action });
        prop::collection::vec(event, 0..=3).prop_map(|events| DbAdminPlan { events })
    }

    /// Issue #236: a backup-and-restore plan for `program`, backing up after
    /// any of its ops. Every plan is a [`BackupKind::ColdCopy`] today; the
    /// `pg_basebackup` kind is held for #558 (see [`BackupKind`]).
    pub fn restore_plan_for(program: &Program) -> impl Strategy<Value = RestorePlan> + use<> {
        (0..program.ops.len()).prop_map(|backup_op| RestorePlan {
            backup_op,
            kind: BackupKind::ColdCopy,
        })
    }

    /// Improvement-plan task E2: draws a [`trivial_program_with`] program,
    /// then defers exactly one of its definitions' installs
    /// ([`defer_def_install`]) to some point strictly after the first seed
    /// insert into that definition's own source table — so the deferred
    /// install always has at least one real, pre-existing source row to
    /// backfill from, exercising the same build over pre-existing rows
    /// `generative/tests/backfill.rs` exercises by hand, now from
    /// inside the harness's own op-stream loop.
    ///
    /// A def whose source table's ops leave no room after that first insert
    /// (its source table's own last op *is* that first insert, i.e. no
    /// mutates follow it) isn't a candidate — deferring it would violate
    /// [`defer_def_install`]'s `after_op < ops.len()` bound. If *no* def in
    /// the drawn program has room, the program is returned unmodified (every
    /// definition installs up front, same as [`trivial_program_with`] alone)
    /// rather than this strategy failing to produce a value at all.
    pub fn program_with_mid_stream_def_install(
        awkward_values: bool,
    ) -> impl Strategy<Value = Program> {
        trivial_program_with(awkward_values).prop_flat_map(|program| {
            let candidates: Vec<(usize, usize)> = program
                .defs
                .iter()
                .enumerate()
                .filter_map(|(def_index, def)| {
                    let first_insert = program.ops.iter().position(
                        |op| matches!(op, Op::Insert { table, .. } if table == &def.source),
                    )?;
                    let earliest = first_insert + 1;
                    (earliest < program.ops.len()).then_some((def_index, earliest))
                })
                .collect();

            if candidates.is_empty() {
                return Just(program).boxed();
            }

            (proptest::sample::select(candidates), Just(program))
                .prop_flat_map(|((def_index, earliest), program)| {
                    let ops_len = program.ops.len();
                    (Just(def_index), earliest..ops_len, Just(program))
                })
                .prop_map(|(def_index, after_op, program)| {
                    defer_def_install(program, def_index, after_op)
                })
                .boxed()
        })
    }

    /// [`trivial_program_with`], restricted to `KeySpace::OneToOne`
    /// definitions only (every def drawn via [`build_program_multi_with_derived`],
    /// which — like [`build_program_multi`] — never draws `DefShape::Aggregate`).
    /// Built for [`program_with_client_restart`] below (see that function's
    /// doc comment for why a restart-carrying program is scoped away from
    /// `Aggregate` definitions specifically) and reused, `pub`, by
    /// `generative/tests/self_check_cross_check.rs`'s swept
    /// out-of-band-tampering property (issue #237): `self_check`
    /// (`trellis/src/staging/self_check.rs`) only audits a `OneToOne` target
    /// this issue (`SelfCheckError::UnsupportedKeySpace`), so that property
    /// needs the same `Aggregate`-free restriction this strategy already
    /// enforces, for the same underlying reason.
    pub fn trivial_one_to_one_program_with(awkward_values: bool) -> impl Strategy<Value = Program> {
        prop::collection::vec(table_spec(awkward_values), 1..=MAX_TABLES)
            .prop_flat_map(|tables| {
                let table_count = tables.len();
                let defs = prop::collection::vec((0..table_count, derived_shape()), 1..=MAX_DEFS);
                (Just(tables), defs)
            })
            .prop_map(|(tables, defs_and_derived)| {
                let (def_sources, derived): (Vec<usize>, Vec<DerivedShape>) =
                    defs_and_derived.into_iter().unzip();
                build_program_multi_with_derived(&tables, &def_sources, &derived)
            })
    }

    /// Improvement-plan task E3: draws a [`trivial_one_to_one_program_with`]
    /// program (see that function's own doc comment for the scope cut this
    /// applies), then schedules a [`schedule_restart`] at a uniformly-drawn
    /// mid-stream op index — a program with fewer than 2 ops (no room for a
    /// "before some still-to-come op" index) is returned unmodified.
    ///
    /// **Scoped to `KeySpace::OneToOne` definitions, not
    /// [`trivial_program_with`]'s full mix — a deliberate, documented scope
    /// cut, discovered *while building this task*, mirroring
    /// [`grain_value`]'s own history of a found-but-then-out-of-scope engine
    /// bug (issue #128, since fixed).** Restarting the primary client
    /// mid-stream originally reproduced a real engine bug in the replication
    /// intake Trellis ran before #622: a fresh connection resumed from the
    /// slot's server-tracked position rather than the durably persisted
    /// watermark, so Postgres redelivered already-applied transactions,
    /// silently double-counting an `Aggregate` target's `SUM`/`COUNT`. Fixed
    /// then, and gone with intake.
    ///
    /// **A second, deeper bug survived that fix — since root-caused and
    /// fixed too, unrelated to replication or to `OneToOne` at all.** A
    /// wider, independently-run 40-case sweep over exactly this
    /// `OneToOne`-restricted strategy still failed after only 13 successes
    /// (a genuinely missing row, `present in SQL oracle, absent in
    /// candidate` — a lost write, not a duplicate), and a fixed, non-
    /// adversarial hand-built pin using this same restart primitive
    /// reproduced the identical shape in roughly 1 of every 3-4 isolated
    /// runs — see `generative/tests/client_lifecycle.rs`'s
    /// `property_convergence_holds_across_a_mid_stream_client_restart`/
    /// `a_restart_and_a_scale_out_interleaved_mid_stream_still_converge` doc
    /// comments for the confirmed root cause: a seal/append race in
    /// `staging::seal::seal_if_active_nonempty`, latent regardless
    /// of restart or `OneToOne` vs. `Aggregate`, that a restart's extra
    /// timing perturbation (and a tiny test program's near-instant drain)
    /// simply made likely to hit. The `OneToOne` restriction here predates
    /// that finding and is no longer load-bearing for *this* bug — it is
    /// left in place anyway (harmless, and this strategy has its own reasons
    /// to stay narrow per [`trivial_one_to_one_program_with`]'s doc comment)
    /// rather than churned as part of an unrelated bug fix.
    ///
    /// **[`program_with_scale_out`] hit the same bug independently,
    /// confirming it has nothing to do with restart specifically.** An
    /// independent re-review found scale-out — which never touches
    /// intake/replication at all — could also lose a brand-new row/group
    /// entirely (reproduced on the very first case generated in a fresh run,
    /// no restart involved). That ruled out both the intake resume position
    /// *and* the next hypothesis considered
    /// (`claim`'s live-worker-count bucket-share math miscounting a
    /// joining/leaving worker — ruled out directly: every program this
    /// generator draws stays far below `claim::MIN_ROWS_TO_SPLIT`, so every
    /// batch seals to one bucket, and `ceil(1 / live_workers)` is `1`
    /// regardless of the count). See
    /// `property_convergence_holds_across_a_mid_stream_scale_out`'s doc comment for
    /// the confirmed mechanism. Both properties are re-enabled in
    /// `client_lifecycle.rs`.
    pub fn program_with_client_restart(awkward_values: bool) -> impl Strategy<Value = Program> {
        trivial_one_to_one_program_with(awkward_values).prop_flat_map(|program| {
            let ops_len = program.ops.len();
            if ops_len < 2 {
                return Just(program).boxed();
            }
            (1..ops_len, Just(program))
                .prop_map(|(after_op, program)| schedule_restart(program, after_op))
                .boxed()
        })
    }

    /// Improvement-plan task E3's other lifecycle event: [`schedule_scale_out`]
    /// at a uniformly-drawn mid-stream op index, same shape as
    /// [`program_with_client_restart`].
    pub fn program_with_scale_out(awkward_values: bool) -> impl Strategy<Value = Program> {
        trivial_program_with(awkward_values).prop_flat_map(|program| {
            let ops_len = program.ops.len();
            if ops_len < 2 {
                return Just(program).boxed();
            }
            (1..ops_len, Just(program))
                .prop_map(|(after_op, program)| schedule_scale_out(program, after_op))
                .boxed()
        })
    }

    /// Improvement-plan task E6: [`build_bulk_insert_program`] with a
    /// shrinkable row count, following the same `1..=MAX` via
    /// `prop_flat_map` idiom [`MAX_SEED_ROWS`]/[`MAX_MUTATES`] already use —
    /// proptest's integrated shrinking reduces the row count toward the
    /// smallest one that still reproduces a failure.
    pub fn bulk_insert_program() -> impl Strategy<Value = Program> {
        (1..=MAX_BULK_INSERT_ROWS).prop_map(build_bulk_insert_program)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use proptest::strategy::ValueTree;
        use proptest::test_runner::TestRunner;

        /// With awkward values off, a mutate must never become a
        /// `ToggleNull` (it writes `NULL`), not even while shrinking: a
        /// `prop_oneof!` shrinks toward its earlier arms whatever their
        /// weight, so a weight-`0` arm would still be reachable by shrinking.
        #[test]
        fn awkward_values_off_never_shrinks_a_mutate_into_toggle_null() {
            let mut runner = TestRunner::deterministic();
            let strategy = mutate(3, false);
            for _ in 0..200 {
                let mut tree = strategy.new_tree(&mut runner).expect("a mutate");
                loop {
                    assert!(
                        !matches!(tree.current(), Mutate::ToggleNull { .. }),
                        "awkward_values=false reached a ToggleNull: {:?}",
                        tree.current()
                    );
                    if !tree.simplify() {
                        break;
                    }
                }
            }
        }
    }
}

#[cfg(feature = "proptest")]
pub use strategy::{
    LOAD_CHUNK_ROWS, LOAD_PACE_MICROS, LOAD_STALL_SEALS, MAX_ACTION_DRAWS, bulk_insert_program,
    checkpoint_plan_for, cooling_key_case, db_admin_plan_for, hot_key_case, mid_burst_case,
    noise_plan_for, program_with_client_restart, program_with_mid_stream_def_install,
    program_with_scale_out, restore_plan_for, steady_load_case, trivial_one_to_one_program_with,
    trivial_program, trivial_program_with,
};

#[cfg(test)]
mod tests {
    use super::*;

    /// Two tables, one definition, one relationship — the smallest program
    /// that can carry a relationship at all, reused by the tests below.
    fn two_table_rel_program(kind: RelFieldKind) -> Program {
        let shape = match kind {
            RelFieldKind::ToOneAggregate(_) => DefShape::Aggregate {
                functions: vec![AggregateFn::Count],
            },
            _ => DefShape::OneToOne,
        };
        let spec = || TableSpec {
            seed_values: vec![(Some(1), Some(2))],
            text_values: vec![None],
            bool_values: vec![None],
            uuid_values: vec![None],
            grain_values: vec![Some("0".to_string())],
            rel_fk_values: vec![Some("k1".to_string())],
            mutates: vec![Mutate::Delete { pk: 1 }],
        };
        build_program_multi_with_relationships(
            &[spec(), spec()],
            &[(0, shape)],
            &[None],
            &[Some(RelFieldSpec { to_table: 1, kind })],
        )
    }

    /// Issue #34: a to-one relationship joins the from-side's foreign key to
    /// the to-side's *unique* key column — that uniqueness is the whole
    /// reason the engine resolves it as to-one — and a to-many one is the
    /// same declaration with the endpoints swapped so the to-side column is
    /// the non-unique one.
    #[test]
    fn a_relationships_endpoints_follow_its_cardinality() {
        let one = two_table_rel_program(RelFieldKind::ToOneBare);
        let rel = &one.relationships[0];
        assert_eq!(rel.cardinality, Cardinality::ToOne);
        assert_eq!(rel.from_col, one.tables[0].columns[REL_FK_COLUMN].name);
        assert_eq!(rel.to_col, one.tables[1].columns[REL_KEY_COLUMN].name);
        assert!(one.tables[1].unique_cols.contains(&rel.to_col));

        let many = two_table_rel_program(RelFieldKind::ToManyAggregate(RelAggregateFn::Sum));
        let rel = &many.relationships[0];
        assert_eq!(rel.cardinality, Cardinality::ToMany);
        assert_eq!(rel.from_col, many.tables[0].columns[REL_KEY_COLUMN].name);
        assert_eq!(rel.to_col, many.tables[1].columns[REL_FK_COLUMN].name);
        assert!(!many.tables[1].unique_cols.contains(&rel.to_col));
    }

    /// A relationship is a schema-level declaration, so reordering the op
    /// stream must carry it through untouched. Regression pin: an earlier
    /// version of `reordered_by_commute_groups` rebuilt the `Program` with
    /// an empty relationship list, which left every definition referencing
    /// an undeclared relationship and made the order-insensitivity property
    /// fail at *install* time rather than on a real ordering difference.
    #[test]
    fn reordering_a_program_preserves_its_relationship_declarations() {
        let program = two_table_rel_program(RelFieldKind::ToOneBare);
        assert_eq!(program.relationships.len(), 1);
        let reordered = reordered_by_commute_groups(&program);
        assert_eq!(reordered.relationships, program.relationships);
    }

    /// Issue #98 removed the `TRUNCATE`-on-relationship-to-side scope cut
    /// (formerly `without_truncates_on_relationship_to_sides`) once the
    /// engine defect it steered around was fixed: a relationship to-side
    /// table's truncate is now generated like any other table's, and its
    /// reverse-propagation is exercised for real by the property suite
    /// rather than being excluded from it.
    #[test]
    fn a_truncate_is_generated_on_a_relationship_to_side_table_like_any_other() {
        let spec = || TableSpec {
            seed_values: vec![(Some(1), Some(2))],
            text_values: vec![None],
            bool_values: vec![None],
            uuid_values: vec![None],
            grain_values: vec![Some("0".to_string())],
            rel_fk_values: vec![Some("k1".to_string())],
            mutates: vec![Mutate::Truncate],
        };
        let program = build_program_multi_with_relationships(
            &[spec(), spec()],
            &[(0, DefShape::OneToOne)],
            &[None],
            &[Some(RelFieldSpec {
                to_table: 1,
                kind: RelFieldKind::ToOneBare,
            })],
        );
        let truncated: Vec<&str> = program
            .ops
            .iter()
            .filter_map(|op| match op {
                Op::Truncate { table, .. } => Some(table.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(
            truncated,
            vec![
                program.tables[0].name.as_str(),
                program.tables[1].name.as_str(),
            ],
            "both tables' truncates are generated, including the relationship to-side: {:?}",
            program.ops
        );
    }

    #[test]
    fn build_program_seeds_before_mutating() {
        let program = build_program(
            &[(Some(1), Some(2)), (Some(3), Some(4))],
            &[
                Mutate::Update {
                    pk: 1,
                    c1: Some(5),
                    c2: Some(6),
                },
                Mutate::Delete { pk: 2 },
            ],
        );
        assert_eq!(program.tables.len(), 1);
        assert_eq!(program.defs.len(), 1);
        // Two inserts (the seeds) come first, then the two mutates.
        assert_eq!(program.ops.len(), 4);
        assert!(matches!(program.ops[0], Op::Insert { .. }));
        assert!(matches!(program.ops[1], Op::Insert { .. }));
        assert!(matches!(program.ops[2], Op::Update { .. }));
        assert!(matches!(program.ops[3], Op::Delete { .. }));
    }

    #[test]
    fn seeded_rows_get_consecutive_primary_keys_from_one() {
        let program = build_program(
            &[(Some(0), Some(0)), (Some(0), Some(0)), (Some(0), Some(0))],
            &[],
        );
        let pks: Vec<&str> = program
            .ops
            .iter()
            .filter_map(|op| match op {
                Op::Insert { row, .. } => row.first().and_then(|(_, v)| v.as_deref()),
                _ => None,
            })
            .collect();
        assert_eq!(pks, vec!["1", "2", "3"]);
    }

    #[test]
    fn the_single_def_is_a_one_to_one_numeric_add() {
        let program = build_program(&[(Some(1), Some(1))], &[]);
        let def = &program.defs[0];
        assert_eq!(def.key_space, KeySpace::OneToOne);
        assert_eq!(def.predicate, Predicate::True);
        // `total` plus one identity-passthrough field per new Text/Boolean/
        // Uuid column (task B1) — see `build_program_multi`'s doc comment.
        assert_eq!(def.fields.len(), 4);
        assert_eq!(def.fields[0].name, "total");
        assert!(matches!(
            def.fields[0].expr,
            Expr::BinaryOp {
                op: Operator::Add,
                ..
            }
        ));
        for field in &def.fields[1..] {
            assert!(
                matches!(&field.expr, Expr::Column(name) if name == &field.name),
                "passthrough field {:?} must be a bare `Expr::Column` referencing its own name: \
                 {field:?}",
                field.name
            );
        }
    }

    /// A `None` seed value must render as SQL `NULL` (no bound text), not the
    /// literal string `"None"` or `"NULL"` — the awkward-value machinery
    /// (issue #7) reuses the same `Option<String>` = `NULL` convention
    /// [`crate::model::Op`] already documents.
    #[test]
    fn a_null_seed_value_renders_as_sql_null_not_a_string() {
        let program = build_program(&[(None, Some(4))], &[]);
        let Op::Insert { row, .. } = &program.ops[0] else {
            panic!("expected an insert");
        };
        let c1 = row.iter().find(|(name, _)| name == "c1").unwrap();
        assert_eq!(c1.1, None);
    }

    /// [`Mutate::DuplicateInsert`] renders to a second `Op::Insert` at a
    /// pk that already exists in the seed — the shape that gives `apply()` a
    /// real primary-key violation to fail on (issue #6's gap).
    #[test]
    fn duplicate_insert_renders_a_second_insert_at_the_same_pk() {
        let program = build_program(
            &[(Some(1), Some(2))],
            &[Mutate::DuplicateInsert {
                pk: 1,
                c1: Some(9),
                c2: Some(9),
            }],
        );
        assert_eq!(program.ops.len(), 2);
        let Op::Insert { row, .. } = &program.ops[1] else {
            panic!("expected the duplicate insert to render as Op::Insert");
        };
        let pk = row.first().unwrap();
        assert_eq!(pk.1.as_deref(), Some("1"));
    }

    /// Issue #255: an `UpdateFlag` writes only the `Boolean` column, and
    /// tracks pk liveness exactly like `Update` (a deleted pk affects no
    /// rows).
    #[test]
    fn update_flag_renders_an_update_of_only_the_boolean_column() {
        let program = build_program(
            &[(Some(1), Some(2))],
            &[
                Mutate::UpdateFlag {
                    pk: 1,
                    flag: Some("false".to_string()),
                },
                Mutate::Delete { pk: 1 },
                Mutate::UpdateFlag { pk: 1, flag: None },
            ],
        );
        let bool_col = &program.tables[0].columns[4];
        assert_eq!(bool_col.value_type, ValueType::Boolean);
        let expected = [
            (Some("false".to_string()), OpOutcome::Succeeds),
            (None, OpOutcome::AffectsNoRows),
        ];
        for (op, (value, outcome)) in [&program.ops[1], &program.ops[3]].into_iter().zip(expected) {
            let Op::Update {
                pk,
                changes,
                expect,
                ..
            } = op
            else {
                panic!("expected an UpdateFlag to render as Op::Update, got {op:?}");
            };
            assert_eq!(pk, "1");
            assert_eq!(changes, &vec![(bool_col.name.clone(), value)]);
            assert_eq!(expect, &outcome);
        }
    }

    /// Issue #255: `BOOL_AND`/`BOOL_OR` fold the source table's `Boolean`
    /// column, each under its own field name.
    #[test]
    fn bool_aggregates_fold_the_boolean_column() {
        let program = build_program_multi_with_shapes(
            &[TableSpec::numeric_only(
                vec![(Some(1), Some(2))],
                Vec::new(),
            )],
            &[(
                0,
                DefShape::Aggregate {
                    functions: vec![AggregateFn::BoolAnd, AggregateFn::BoolOr],
                },
            )],
        );
        let bool_col = program.tables[0].columns[4].name.clone();
        let calls: Vec<(String, String, Vec<Expr>)> = program.defs[0].fields[1..]
            .iter()
            .map(|field| match &field.expr {
                Expr::FunctionCall { name, args } => {
                    (field.name.clone(), name.clone(), args.clone())
                }
                other => panic!("expected an aggregate call, got {other:?}"),
            })
            .collect();
        assert_eq!(
            calls,
            vec![
                (
                    format!("bool_and_{bool_col}"),
                    "BOOL_AND".to_string(),
                    vec![Expr::Column(bool_col.clone())]
                ),
                (
                    format!("bool_or_{bool_col}"),
                    "BOOL_OR".to_string(),
                    vec![Expr::Column(bool_col.clone())]
                ),
            ]
        );
    }

    /// `build_program`'s pk-liveness simulation must track a pk across
    /// *multiple* mutates in the same stream, not just whether it was
    /// originally seeded — these are fast, DB-free pins for sequences the
    /// slow DB-backed proptest property only exercises when it happens to
    /// draw them (issue #4/A1: an op's expected outcome must always match
    /// what real Postgres would do).
    mod pk_liveness {
        use super::*;

        #[test]
        fn update_after_delete_on_the_same_pk_affects_no_rows() {
            let program = build_program(
                &[(Some(1), Some(2))],
                &[
                    Mutate::Delete { pk: 1 },
                    Mutate::Update {
                        pk: 1,
                        c1: Some(9),
                        c2: Some(9),
                    },
                ],
            );
            // ops: [seed insert, delete, update]
            assert_eq!(program.ops[1].expect(), &OpOutcome::Succeeds);
            assert_eq!(program.ops[2].expect(), &OpOutcome::AffectsNoRows);
        }

        #[test]
        fn delete_after_delete_on_the_same_pk_affects_no_rows() {
            let program = build_program(
                &[(Some(1), Some(2))],
                &[Mutate::Delete { pk: 1 }, Mutate::Delete { pk: 1 }],
            );
            assert_eq!(program.ops[1].expect(), &OpOutcome::Succeeds);
            assert_eq!(program.ops[2].expect(), &OpOutcome::AffectsNoRows);
        }

        #[test]
        fn duplicate_insert_after_delete_revives_the_pk_and_succeeds() {
            let program = build_program(
                &[(Some(1), Some(2))],
                &[
                    Mutate::Delete { pk: 1 },
                    Mutate::DuplicateInsert {
                        pk: 1,
                        c1: Some(9),
                        c2: Some(9),
                    },
                ],
            );
            assert_eq!(program.ops[1].expect(), &OpOutcome::Succeeds);
            assert_eq!(program.ops[2].expect(), &OpOutcome::Succeeds);
        }

        #[test]
        fn a_second_duplicate_insert_against_a_revived_pk_fails_again() {
            let program = build_program(
                &[(Some(1), Some(2))],
                &[
                    Mutate::Delete { pk: 1 },
                    Mutate::DuplicateInsert {
                        pk: 1,
                        c1: Some(9),
                        c2: Some(9),
                    },
                    Mutate::DuplicateInsert {
                        pk: 1,
                        c1: Some(7),
                        c2: Some(7),
                    },
                ],
            );
            assert_eq!(program.ops[2].expect(), &OpOutcome::Succeeds);
            assert_eq!(program.ops[3].expect(), &OpOutcome::Fails);
        }

        #[test]
        fn duplicate_insert_against_a_still_live_seeded_pk_fails() {
            let program = build_program(
                &[(Some(1), Some(2))],
                &[Mutate::DuplicateInsert {
                    pk: 1,
                    c1: Some(9),
                    c2: Some(9),
                }],
            );
            assert_eq!(program.ops[1].expect(), &OpOutcome::Fails);
        }

        #[test]
        fn update_or_delete_on_an_unseeded_pk_affects_no_rows() {
            let program = build_program(
                &[(Some(1), Some(2))],
                &[
                    Mutate::Update {
                        pk: 2,
                        c1: Some(9),
                        c2: Some(9),
                    },
                    Mutate::Delete { pk: 2 },
                ],
            );
            assert_eq!(program.ops[1].expect(), &OpOutcome::AffectsNoRows);
            assert_eq!(program.ops[2].expect(), &OpOutcome::AffectsNoRows);
        }
    }

    /// [`build_program_multi`]'s pk-liveness simulation must be *per table*,
    /// not shared across tables (improvement-plan task B3) — fast, DB-free
    /// pins for exactly the class of bug the task's own validation section
    /// calls out: multi-table is new surface where a liveness-tracking bug
    /// (or a pk-numbering bug) could slip in undetected without a test
    /// aimed at it specifically.
    mod pk_liveness_multi_table {
        use super::*;

        /// A `Delete` on table A's pk 1 must not affect table B's pk 1: an
        /// `Update` on table B's still-live pk 1, issued right after table
        /// A's delete, must still predict `Succeeds` — never `AffectsNoRows`
        /// — proving the two tables' `live` maps don't leak into each other.
        #[test]
        fn a_delete_on_one_table_does_not_affect_the_same_pk_on_another_table() {
            let program = build_program_multi(
                &[
                    TableSpec::numeric_only(
                        vec![(Some(1), Some(2))],
                        vec![Mutate::Delete { pk: 1 }],
                    ),
                    TableSpec::numeric_only(
                        vec![(Some(9), Some(9))],
                        vec![Mutate::Update {
                            pk: 1,
                            c1: Some(5),
                            c2: Some(5),
                        }],
                    ),
                ],
                &[0],
            );
            // ops: [seed A pk1, delete A pk1, seed B pk1, update B pk1]
            assert_eq!(program.tables.len(), 2);
            assert_eq!(program.ops.len(), 4);
            assert!(matches!(program.ops[0], Op::Insert { .. }));
            assert_eq!(program.ops[1].expect(), &OpOutcome::Succeeds); // delete A pk1
            assert!(matches!(program.ops[2], Op::Insert { .. }));
            assert_eq!(
                program.ops[3].expect(),
                &OpOutcome::Succeeds,
                "table B's pk 1 must still be live even though table A's pk 1 was deleted: {program:#?}"
            );
        }

        /// The mirror case: a `DuplicateInsert` on table A's pk 1 (a real
        /// primary-key violation there) must not make table B's pk 1 look
        /// non-live — an `Update` on table B's pk 1 right after must still
        /// predict `Succeeds`.
        #[test]
        fn a_duplicate_insert_on_one_table_does_not_affect_the_same_pk_on_another_table() {
            let program = build_program_multi(
                &[
                    TableSpec::numeric_only(
                        vec![(Some(1), Some(2))],
                        vec![Mutate::DuplicateInsert {
                            pk: 1,
                            c1: Some(9),
                            c2: Some(9),
                        }],
                    ),
                    TableSpec::numeric_only(
                        vec![(Some(3), Some(4))],
                        vec![Mutate::Update {
                            pk: 1,
                            c1: Some(5),
                            c2: Some(5),
                        }],
                    ),
                ],
                &[1],
            );
            // ops: [seed A pk1, dup-insert A pk1 (fails), seed B pk1, update B pk1]
            assert_eq!(program.ops[1].expect(), &OpOutcome::Fails);
            assert_eq!(
                program.ops[3].expect(),
                &OpOutcome::Succeeds,
                "table A's still-live pk 1 must not affect table B's own pk 1: {program:#?}"
            );
        }

        /// Each table's seeded pks start at 1 independently — table B's
        /// first seeded pk must render `"1"`, not `"3"` (i.e. not continue
        /// numbering after table A's two seeded rows).
        #[test]
        fn each_tables_seeded_pks_start_at_one_independent_of_earlier_tables() {
            let program = build_program_multi(
                &[
                    TableSpec::numeric_only(vec![(Some(1), Some(1)), (Some(2), Some(2))], vec![]),
                    TableSpec::numeric_only(vec![(Some(3), Some(3))], vec![]),
                ],
                &[0, 1],
            );
            let table_b = &program.tables[1];
            let pks_for_table_b: Vec<&str> = program
                .ops
                .iter()
                .filter_map(|op| match op {
                    Op::Insert { table, row, .. } if table == &table_b.name => {
                        row.first().and_then(|(_, v)| v.as_deref())
                    }
                    _ => None,
                })
                .collect();
            assert_eq!(
                pks_for_table_b,
                vec!["1"],
                "table B's own pk numbering must start at 1, independent of table A's row count: {program:#?}"
            );
        }

        /// Sanity check on the multi-def side of B3: two definitions can
        /// independently draw the *same* source table (fan-out), and each
        /// still gets its own target and its own `total = c1 + c2` shape.
        #[test]
        fn two_definitions_can_share_one_source_table() {
            let program = build_program_multi(
                &[TableSpec::numeric_only(vec![(Some(1), Some(2))], vec![])],
                &[0, 0],
            );
            assert_eq!(program.tables.len(), 1);
            assert_eq!(program.defs.len(), 2);
            assert_eq!(program.defs[0].source, program.tables[0].name);
            assert_eq!(program.defs[1].source, program.tables[0].name);
            assert_ne!(
                program.defs[0].target, program.defs[1].target,
                "two defs over the same source must still get distinct targets"
            );
        }
    }

    /// Improvement-plan task D2: fast, DB-free pins on the commutation
    /// analysis itself ([`ops_commute`]/[`commute_groups`]/
    /// [`reordered_by_commute_groups`]) — the slow, DB-backed half (do two
    /// commuting orderings actually converge identically against a real
    /// cluster) lives in `generative/tests/order_insensitivity.rs`.
    mod order_insensitivity {
        use super::*;

        #[test]
        fn ops_on_different_tables_always_commute() {
            let program = build_program_multi(
                &[
                    TableSpec::numeric_only(vec![(Some(1), Some(2))], vec![]),
                    TableSpec::numeric_only(vec![(Some(3), Some(4))], vec![]),
                ],
                &[0, 1],
            );
            // ops[0] seeds table A's pk 1, ops[1] seeds table B's pk 1 —
            // different tables, so they commute even though the pk (1)
            // happens to coincide.
            assert!(ops_commute(&program, &program.ops[0], &program.ops[1]));
        }

        #[test]
        fn ops_on_the_same_table_and_pk_never_commute() {
            let program = build_program(
                &[(Some(1), Some(2))],
                &[Mutate::Update {
                    pk: 1,
                    c1: Some(9),
                    c2: Some(9),
                }],
            );
            // ops[0] seeds pk 1, ops[1] updates that same pk 1.
            assert!(!ops_commute(&program, &program.ops[0], &program.ops[1]));
        }

        #[test]
        fn ops_on_the_same_table_but_different_pks_commute() {
            let program = build_program(&[(Some(1), Some(2)), (Some(3), Some(4))], &[]);
            // ops[0] seeds pk 1, ops[1] seeds pk 2 — same table, distinct
            // rows, no def-level collision (OneToOne: target key == pk).
            assert!(ops_commute(&program, &program.ops[0], &program.ops[1]));
        }

        #[test]
        fn target_key_for_one_to_one_is_the_source_pk() {
            let program = build_program(&[(Some(1), Some(2))], &[]);
            let table = &program.tables[0];
            let def = &program.defs[0];
            assert_eq!(
                target_key_for(def, table, &program.ops[0]),
                Some("1".to_string())
            );
        }

        #[test]
        fn target_key_for_returns_none_for_a_definition_over_a_different_table() {
            let program = build_program_multi(
                &[
                    TableSpec::numeric_only(vec![(Some(1), Some(2))], vec![]),
                    TableSpec::numeric_only(vec![(Some(3), Some(4))], vec![]),
                ],
                // def 0 sources table 0 only.
                &[0],
            );
            let table_b = &program.tables[1];
            let def = &program.defs[0];
            // ops[1] is table B's seed insert; def 0 is sourced from table A.
            assert_eq!(target_key_for(def, table_b, &program.ops[1]), None);
        }

        #[test]
        fn reordered_by_commute_groups_preserves_the_same_multiset_of_ops() {
            let program = build_program(
                &[(Some(1), Some(2)), (Some(3), Some(4))],
                &[
                    Mutate::Update {
                        pk: 1,
                        c1: Some(10),
                        c2: Some(20),
                    },
                    Mutate::Update {
                        pk: 2,
                        c1: Some(30),
                        c2: Some(40),
                    },
                ],
            );
            let reordered = reordered_by_commute_groups(&program);

            let mut original_ops = program.ops.clone();
            let mut reordered_ops = reordered.ops.clone();
            original_ops.sort_by_key(|op| format!("{op:?}"));
            reordered_ops.sort_by_key(|op| format!("{op:?}"));
            assert_eq!(
                original_ops, reordered_ops,
                "reordering must never add, drop, or mutate an op — only its position: \
                 original {:#?} vs reordered {:#?}",
                program.ops, reordered.ops
            );
        }

        #[test]
        fn reordered_by_commute_groups_preserves_relative_order_within_a_pk() {
            let program = build_program(
                &[(Some(1), Some(2))],
                &[
                    Mutate::Update {
                        pk: 1,
                        c1: Some(10),
                        c2: Some(20),
                    },
                    Mutate::Delete { pk: 1 },
                ],
            );
            let reordered = reordered_by_commute_groups(&program);

            // pk 1's own ops (seed, update, delete) all belong to one commute
            // group (there's only one row in play at all), so the whole
            // program has exactly one group and the reordering must be the
            // identity — this is the pin that a same-pk sequence never gets
            // scrambled internally.
            assert_eq!(reordered.ops, program.ops);
        }

        #[test]
        fn reordered_by_commute_groups_can_actually_change_order_across_independent_pks() {
            let program = build_program(
                &[(Some(1), Some(2)), (Some(3), Some(4)), (Some(5), Some(6))],
                &[],
            );
            let reordered = reordered_by_commute_groups(&program);
            assert_ne!(
                reordered.ops, program.ops,
                "three independent single-row groups must reorder under a group-order reversal: \
                 {reordered:#?}"
            );
        }
    }

    /// Issue #505: `ToggleNull` and the table interleaving.
    mod reverse_path {
        use super::*;

        fn c1_change(op: &Op) -> (Option<String>, &OpOutcome) {
            let Op::Update {
                changes, expect, ..
            } = op
            else {
                panic!("expected an update, got {op:?}");
            };
            assert_eq!(changes.len(), 1, "ToggleNull writes c1 alone: {op:?}");
            (changes[0].1.clone(), expect)
        }

        #[test]
        fn toggle_null_moves_c1_across_null_each_way() {
            let program = build_program(
                &[(Some(5), None), (None, None)],
                &[
                    Mutate::ToggleNull { pk: 1, value: 7 },
                    Mutate::ToggleNull { pk: 2, value: 8 },
                    Mutate::ToggleNull { pk: 1, value: 9 },
                ],
            );
            // ops: [seed 1, seed 2, toggle 1, toggle 2, toggle 1]
            assert_eq!(c1_change(&program.ops[2]), (None, &OpOutcome::Succeeds));
            assert_eq!(
                c1_change(&program.ops[3]),
                (Some("8".to_string()), &OpOutcome::Succeeds)
            );
            assert_eq!(
                c1_change(&program.ops[4]),
                (Some("9".to_string()), &OpOutcome::Succeeds),
                "the second toggle of pk 1 must see the NULL the first one wrote"
            );
        }

        #[test]
        fn toggle_null_tracks_updates_revivals_and_dead_rows() {
            let program = build_program(
                &[(Some(5), None)],
                &[
                    Mutate::Update {
                        pk: 1,
                        c1: None,
                        c2: None,
                    },
                    Mutate::ToggleNull { pk: 1, value: 3 },
                    Mutate::Delete { pk: 1 },
                    Mutate::ToggleNull { pk: 1, value: 4 },
                    Mutate::DuplicateInsert {
                        pk: 1,
                        c1: Some(6),
                        c2: None,
                    },
                    Mutate::ToggleNull { pk: 1, value: 4 },
                ],
            );
            // ops: [seed, update, toggle, delete, toggle, revive, toggle]
            assert_eq!(
                c1_change(&program.ops[2]),
                (Some("3".to_string()), &OpOutcome::Succeeds),
                "the toggle must see the NULL the update wrote"
            );
            assert_eq!(c1_change(&program.ops[4]).1, &OpOutcome::AffectsNoRows);
            assert_eq!(
                c1_change(&program.ops[6]),
                (None, &OpOutcome::Succeeds),
                "the toggle must see the revived row's c1"
            );
        }

        fn three_table_program() -> Program {
            build_program_multi(
                &[
                    TableSpec::numeric_only(
                        vec![(Some(1), None), (Some(2), None)],
                        vec![Mutate::Delete { pk: 1 }],
                    ),
                    TableSpec::numeric_only(vec![(Some(3), None)], vec![Mutate::Truncate]),
                    TableSpec::numeric_only(vec![(Some(4), None)], Vec::new()),
                ],
                &[0],
            )
        }

        #[test]
        fn an_all_zero_schedule_keeps_the_table_by_table_order() {
            let program = three_table_program();
            assert_eq!(interleave_tables(program.clone(), &[]), program);
            assert_eq!(
                interleave_tables(program.clone(), &[0, INTERLEAVE_PICK_RANGE, 0]),
                program,
                "a pick is taken modulo the tables left, so a full-range pick is 0 too"
            );
        }

        #[test]
        fn interleaving_keeps_each_tables_order_and_moves_ops_across_tables() {
            let program = three_table_program();
            // Always the last table with ops left.
            let schedule = [2, 1, 1, 1, 0, 0];
            let interleaved = interleave_tables(program.clone(), &schedule);
            let tables: Vec<&str> = interleaved.ops.iter().map(op_table).collect();
            let t = |i: usize| program.tables[i].name.as_str();
            assert_eq!(tables, vec![t(2), t(1), t(1), t(0), t(0), t(0)]);
            for (stream_before, stream_after) in table_streams(&program)
                .iter()
                .zip(table_streams(&interleaved))
            {
                let before: Vec<&Op> = stream_before.iter().map(|&i| &program.ops[i]).collect();
                let after: Vec<&Op> = stream_after.iter().map(|&i| &interleaved.ops[i]).collect();
                assert_eq!(before, after, "a table's own ops must keep their order");
            }
        }

        #[test]
        #[should_panic(expected = "interleave before scheduling anything by op index")]
        fn interleaving_after_scheduling_by_op_index_is_a_generator_bug() {
            let program = schedule_restart(three_table_program(), 2);
            interleave_tables(program, &[1]);
        }
    }

    /// Issue #557: the concurrent tier's lanes and the hot-key shape's
    /// group window.
    mod concurrent_lanes {
        use super::*;

        /// Every op on one row stays in one lane, in program order; rows are
        /// dealt round-robin; and each burst covers its slice of the program.
        #[test]
        fn a_rows_ops_share_a_lane_and_rows_are_dealt_round_robin() {
            let program = build_program(
                &[(Some(1), None), (Some(2), None), (Some(3), None)],
                &[
                    Mutate::Update {
                        pk: 1,
                        c1: Some(5),
                        c2: None,
                    },
                    Mutate::Delete { pk: 2 },
                    Mutate::Update {
                        pk: 1,
                        c1: Some(6),
                        c2: None,
                    },
                    Mutate::Update {
                        pk: 3,
                        c1: Some(7),
                        c2: None,
                    },
                ],
            );
            // ops: [seed 1, seed 2, seed 3, update 1, delete 2, update 1, update 3]
            let plan = concurrent_plan(&program, 4, 2);
            assert_eq!(
                plan.bursts,
                vec![
                    // pk 1 -> lane 0, pk 2 -> lane 1, pk 3 -> lane 0.
                    Burst {
                        lanes: vec![vec![0, 2, 3], vec![1]],
                        ..Default::default()
                    },
                    // pk 2 -> lane 0, pk 1 -> lane 1, pk 3 -> lane 0.
                    Burst {
                        lanes: vec![vec![4, 6], vec![5]],
                        ..Default::default()
                    },
                ]
            );
        }

        /// A table the program ever truncates is one lane per burst: the
        /// truncate touches every row, so nothing on that table may race it.
        /// Other tables still split by row.
        #[test]
        fn a_truncated_table_stays_in_one_lane() {
            let program = build_program_multi(
                &[
                    TableSpec::numeric_only(
                        vec![(Some(1), None), (Some(2), None)],
                        vec![Mutate::Truncate],
                    ),
                    TableSpec::numeric_only(vec![(Some(3), None), (Some(4), None)], Vec::new()),
                ],
                &[0],
            );
            // ops: [t0 seed 1, t0 seed 2, t0 truncate, t1 seed 1, t1 seed 2]
            let plan = concurrent_plan(&program, 10, 4);
            assert_eq!(
                plan.bursts,
                vec![Burst {
                    lanes: vec![vec![0, 1, 2], vec![3], vec![4]],
                    ..Default::default()
                }]
            );
        }

        #[test]
        #[should_panic(expected = "doesn't schedule a restart or a scale-out")]
        fn a_program_with_a_restart_is_refused() {
            let program = build_program(&[(Some(1), None), (Some(2), None)], &[]);
            concurrent_plan(&schedule_restart(program, 1), 1, 1);
        }

        /// Issue #557 part 2: a deferred install becomes an install action
        /// in the burst holding its op, firing after the burst's ops before
        /// it.
        #[test]
        fn a_deferred_install_fires_mid_burst_after_the_ops_before_it() {
            let program = build_program_multi(
                &[TableSpec::numeric_only(
                    (1..=6).map(|i| (Some(i), None)).collect(),
                    Vec::new(),
                )],
                &[0, 0],
            );
            let program = defer_def_install(program, 1, 5);
            let plan = concurrent_plan(&program, 4, 2);
            assert!(plan.bursts[0].actions.is_empty());
            assert_eq!(
                plan.bursts[1].actions,
                vec![TimedAction {
                    after: 1,
                    action: BurstAction::Install { def: 1 },
                }]
            );
        }

        /// A program of two 1-1 definitions over one table, cut into three
        /// bursts of 4 ops.
        fn three_burst_plan() -> (Program, ConcurrentPlan) {
            let program = build_program_multi(
                &[TableSpec::numeric_only(
                    (1..=12).map(|i| (Some(i), None)).collect(),
                    Vec::new(),
                )],
                &[0, 0],
            );
            let plan = concurrent_plan(&program, 4, 2);
            (program, plan)
        }

        /// Draws resolve to actions in a burst after the first, a pair's
        /// resume after its pause, and a column pair names one of the
        /// definition's own fields.
        #[test]
        fn burst_actions_land_after_the_first_burst_in_order() {
            let (program, plan) = three_burst_plan();
            let draw = |kind, burst, target, start, span| ActionDraw {
                kind,
                burst,
                target,
                start,
                span,
            };
            let plan = add_burst_actions(
                plan,
                &program,
                &[
                    draw(ActionKind::PauseResume, 0, 1, 500, 250),
                    draw(ActionKind::RequestBackfill, 0, 0, 0, 0),
                    draw(ActionKind::ColumnPauseResume, 1, 0, 1000, 1000),
                ],
            );
            assert!(plan.bursts[0].actions.is_empty());
            let t0 = program.tables[0].name.clone();
            let (d0, d1) = (
                program.defs[0].target.clone(),
                program.defs[1].target.clone(),
            );
            assert_eq!(
                plan.bursts[1].actions,
                vec![
                    TimedAction {
                        after: 0,
                        action: BurstAction::RequestBackfill { table: t0 },
                    },
                    TimedAction {
                        after: 2,
                        action: BurstAction::Pause {
                            target: d1.clone(),
                            column: None,
                        },
                    },
                    TimedAction {
                        after: 3,
                        action: BurstAction::Resume {
                            target: d1,
                            column: None,
                        },
                    },
                ]
            );
            let column = Some(program.defs[0].fields[0].name.clone());
            assert_eq!(
                plan.bursts[2].actions,
                vec![
                    TimedAction {
                        after: 4,
                        action: BurstAction::Pause {
                            target: d0.clone(),
                            column: column.clone(),
                        },
                    },
                    TimedAction {
                        after: 4,
                        action: BurstAction::Resume { target: d0, column },
                    },
                ]
            );
        }

        /// A second pair on a definition that already has one in the burst
        /// is dropped, and a one-burst plan takes no actions at all.
        #[test]
        fn burst_actions_that_cannot_apply_are_dropped() {
            let (program, plan) = three_burst_plan();
            let pair = |kind| ActionDraw {
                kind,
                burst: 0,
                target: 0,
                start: 0,
                span: 1000,
            };
            let plan = add_burst_actions(
                plan,
                &program,
                &[
                    pair(ActionKind::PauseResume),
                    pair(ActionKind::ColumnPauseResume),
                ],
            );
            assert_eq!(plan.bursts[1].actions.len(), 2);

            let one_burst = concurrent_plan(&program, 100, 2);
            let one_burst =
                add_burst_actions(one_burst, &program, &[pair(ActionKind::PauseResume)]);
            assert!(one_burst.bursts[0].actions.is_empty());
        }

        #[cfg(feature = "proptest")]
        /// The drawn hot-key cases really are hot-key cases: every op is in
        /// exactly one lane, lanes keep program order, and a `MoveGroup` late
        /// in the stream can name a group above the first window.
        #[test]
        fn hot_key_cases_cover_every_op_and_slide_the_group_window() {
            use proptest::strategy::{Strategy, ValueTree};
            use proptest::test_runner::TestRunner;

            let mut runner = TestRunner::deterministic();
            let mut highest_group = 0;
            for _ in 0..8 {
                let case = hot_key_case()
                    .new_tree(&mut runner)
                    .expect("a case")
                    .current();
                let mut covered: Vec<usize> =
                    case.plan.bursts.iter().flat_map(|b| b.ops()).collect();
                covered.sort_unstable();
                assert!(covered.iter().copied().eq(0..case.program.ops.len()));
                assert!(
                    case.plan.bursts.len() >= 2,
                    "every case has two bursts or more"
                );
                for burst in &case.plan.bursts {
                    assert!(burst.lanes.iter().all(|lane| lane.is_sorted()));
                }
                let grain = &case.program.tables[0].columns[6].name;
                for op in &case.program.ops {
                    if let Op::Update { changes, .. } = op
                        && changes[0].0 == *grain
                        && let Some(value) = &changes[0].1
                    {
                        highest_group = highest_group.max(value.parse::<i64>().unwrap());
                    }
                }
            }
            assert!(
                highest_group > HOT_GRAIN_MAX + HOT_GROUP_SLIDE,
                "the group window must slide past its first position: highest group \
                 {highest_group}"
            );
        }

        #[cfg(feature = "proptest")]
        /// The drawn steady-load cases (#720, #725): none has a
        /// relationship, each has small build chunks, a reconcile pass at
        /// its seal cadence and a stall of a few seal intervals; and the
        /// mid-burst tier's cases stay flat out, at the engine's defaults.
        #[test]
        fn steady_load_cases_have_no_relationship_and_mid_burst_cases_no_load() {
            use proptest::strategy::{Strategy, ValueTree};
            use proptest::test_runner::TestRunner;

            let mut runner = TestRunner::deterministic();
            for _ in 0..16 {
                let case = steady_load_case()
                    .new_tree(&mut runner)
                    .expect("a case")
                    .current();
                let load = case.plan.steady_load.expect("a steady load");
                assert!(case.program.relationships.is_empty());
                assert!(LOAD_CHUNK_ROWS.contains(&case.build_chunk_rows.expect("rows")));
                assert_eq!(case.reconcile_interval_ms, Some(case.seal_interval_ms));
                let pace = u64::try_from(load.pace.as_micros()).expect("micros");
                assert!(LOAD_PACE_MICROS.contains(&pace));
                let seals =
                    u64::try_from(load.stall.as_millis()).expect("millis") / case.seal_interval_ms;
                assert!(LOAD_STALL_SEALS.contains(&u32::try_from(seals).expect("seals")));
            }
            let mut related = 0usize;
            for _ in 0..16 {
                let case = mid_burst_case()
                    .new_tree(&mut runner)
                    .expect("a case")
                    .current();
                assert_eq!(case.plan.steady_load, None);
                assert_eq!(case.build_chunk_rows, None);
                assert_eq!(case.reconcile_interval_ms, None);
                related += usize::from(!case.program.relationships.is_empty());
            }
            assert!(related > 0, "the mid-burst tier still draws relationships");
        }

        #[cfg(feature = "proptest")]
        /// The drawn cooling-key cases (#557 part 3b): every op is in one
        /// lane, a 1-1 definition reads the cooling table, and each cooling
        /// key's writes stay in order, inside one burst, with the last of
        /// them in the burst's middle rather than its tail.
        #[test]
        fn cooling_key_cases_go_quiet_mid_burst() {
            use proptest::strategy::{Strategy, ValueTree};
            use proptest::test_runner::TestRunner;

            let mut runner = TestRunner::deterministic();
            let (mut keys, mut in_middle) = (0usize, 0usize);
            for _ in 0..8 {
                let case = cooling_key_case()
                    .new_tree(&mut runner)
                    .expect("a case")
                    .current();
                let mut covered: Vec<usize> =
                    case.plan.bursts.iter().flat_map(|b| b.ops()).collect();
                covered.sort_unstable();
                assert!(covered.iter().copied().eq(0..case.program.ops.len()));
                assert!(COOLING_SEAL_INTERVAL_MS.contains(&case.seal_interval_ms));
                let cooling = case.program.tables.last().expect("a cooling table");
                assert!(
                    case.program.defs.iter().any(
                        |def| def.source == cooling.name && def.key_space == KeySpace::OneToOne
                    )
                );

                // Each cooling key's writes, as (burst, position in burst).
                let mut writes: HashMap<String, Vec<(usize, usize, usize)>> = HashMap::new();
                for (burst_index, burst) in case.plan.bursts.iter().enumerate() {
                    let mut ops = burst.ops();
                    ops.sort_unstable();
                    for (position, &index) in ops.iter().enumerate() {
                        let op = &case.program.ops[index];
                        if op_table(op) == cooling.name {
                            let pk = op_pk_value(op, cooling).expect("a single-row op");
                            writes
                                .entry(pk)
                                .or_default()
                                .push((burst_index, position, ops.len()));
                        }
                    }
                }
                assert!(COOLING_KEYS.contains(&writes.len()));
                for (pk, writes) in &writes {
                    assert!(
                        COOLING_WRITES.contains(&writes.len()),
                        "key {pk}: {} writes",
                        writes.len()
                    );
                    let &(burst, last, len) = writes.last().expect("a write");
                    assert!(
                        writes.iter().all(|w| w.0 == burst),
                        "key {pk}'s writes span bursts: {writes:?}"
                    );
                    keys += 1;
                    // Placement drifts a little later where writes crowd.
                    if last * 10 >= len && last * 10 <= len * 9 {
                        in_middle += 1;
                    }
                }
            }
            assert!(
                in_middle * 100 >= keys * 95,
                "{in_middle} of {keys} cooling keys have their last write mid-burst"
            );
        }
    }
}
