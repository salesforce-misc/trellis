//! Plain-data description of a generated program (design doc §1,
//! `docs/generative-test-suite.md`).
//!
//! A [`Program`] is exactly what the (future) shrinker minimizes and what
//! prints on a failing case: no engine internals beyond the AST types it
//! deliberately reuses ([`TransformDef`], [`ValueType`]) so a definition
//! this crate generates is byte-for-byte the same shape the parser produces
//! from concrete syntax.

use trellis::dev::defs::ast::{TransformDef, ValueType};

/// One column on a [`Table`].
#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
    pub value_type: ValueType,
}

/// A source table's shape. `pk_col` always names one of `columns` — see
/// [`Table::new`].
#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    pub name: String,
    pub pk_col: String,
    pub columns: Vec<Column>,
    /// Names of columns that carry a single-column `UNIQUE` constraint
    /// (issue #34). Every entry must also appear in `columns`; the primary
    /// key is *not* listed here (it's already unique by virtue of being the
    /// primary key — see `pk_col`).
    ///
    /// This exists for relationships: ADR-0006 derives a relationship's
    /// cardinality from whether its **to-side** column is provably unique
    /// (primary key or `UNIQUE`), and `defs::catalog` introspects
    /// that live against `pg_catalog` at `create_relationship` time. A
    /// generated to-one relationship therefore needs a real `UNIQUE`
    /// constraint on the to-side column, which the backend can only emit if
    /// the model says it exists. A to-many relationship is the same
    /// declaration with a *non*-unique to-side column, so this one list
    /// decides both cardinalities.
    pub unique_cols: Vec<String>,
}

/// The Postgres type every generated table's primary-key column is declared
/// with.
///
/// Not derived from a [`ValueType`] on purpose. The generator seeds primary
/// keys as consecutive integers (`1..=seed_count`, see
/// `crate::generate::build_program_multi_with_shapes`), but [`ValueType`]'s
/// only numeric variant maps to Postgres `numeric` — and the engine rejects a
/// `numeric` primary key outright with
/// `defs::ddl::DdlError::UnsupportedPrimaryKeyType`, because
/// `source_primary_key` gates the resolved PK type on
/// `is_text_stable_join_key_type`'s allowlist (issue #107): every consumer
/// compares that key via a `::text` cast, and `numeric` is not text-stable
/// (`1.0::text != 1.00::text` though the two are numerically equal).
///
/// Declaring the PK `numeric` therefore made *every* generated program fail
/// to install, which is what broke `backend_seam` and the `backfill.rs` /
/// `bulk_operations.rs` suites. `bigint` is on that allowlist, is what the
/// hand-written engine tests actually use (`trellis/tests/apply.rs`'s
/// `orders` table declares `id integer primary key`), and comfortably holds
/// the `i64` pks the generator mints.
pub const PRIMARY_KEY_PG_TYPE: &str = "bigint";

impl Table {
    /// Builds a table from `pool`, giving it a primary-key column
    /// unconditionally — before any of `column_types` — so no future shrink
    /// step can strand a definition that references it (design doc §1).
    ///
    /// The PK column's Postgres type is [`PRIMARY_KEY_PG_TYPE`], *not* the
    /// [`ValueType`] stored on its [`Column`]. [`ValueType`] is
    /// `trellis::dev::defs::ast`'s expression-level type and has no integer
    /// variant, so it simply cannot name the type a primary key needs; the
    /// `ValueType::Numeric` recorded below is an unavoidable placeholder.
    /// Nothing may render the PK column's type from it — see
    /// [`PRIMARY_KEY_PG_TYPE`] for why a `numeric` PK is rejected outright by
    /// the engine, and `crate::backend::manual`'s `column_pg_type`, which is
    /// the one place every DDL and cast site resolves a column's declared
    /// type through.
    pub fn new(pool: &mut NamePool, column_types: &[ValueType]) -> Table {
        let name = pool.next_table_name();
        let pk_col = pool.next_column_name();
        let mut columns = Vec::with_capacity(column_types.len() + 1);
        columns.push(Column {
            name: pk_col.clone(),
            value_type: ValueType::Numeric,
        });
        for value_type in column_types {
            columns.push(Column {
                name: pool.next_column_name(),
                value_type: *value_type,
            });
        }
        Table {
            name,
            pk_col,
            columns,
            unique_cols: Vec::new(),
        }
    }

    /// The type of `column` on this table, or `None` if it has no such
    /// column. Used by [`crate::oracle`] to type a `<rel>.<column>`
    /// enrichment field, whose type lives on the *to-side* table rather than
    /// the definition's own source.
    pub fn column_type(&self, column: &str) -> Option<ValueType> {
        self.columns
            .iter()
            .find(|c| c.name == column)
            .map(|c| c.value_type)
    }
}

/// How many related rows a [`Relationship`] resolves to (ADR-0006), derived
/// — not declared — from whether the **to-side** column is provably unique:
/// the concrete-syntax declaration
/// (`RELATIONSHIP <name> FROM <t>.<c> TO <t>.<c>`) carries no cardinality
/// keyword at all, and `defs::catalog` introspects `pg_catalog` for a
/// primary-key/`UNIQUE` index on the to-side column to decide.
///
/// This enum records the cardinality the *generator* built the relationship
/// to have, so [`crate::oracle`] can render the right SQL (a `LEFT JOIN`
/// versus a correlated aggregate subquery) without a catalog round trip. It
/// is a claim the generator is responsible for keeping true — every
/// generated [`Relationship`] whose cardinality is [`Cardinality::ToOne`]
/// must name a to-side column listed in that table's
/// [`Table::unique_cols`], and every [`Cardinality::ToMany`] one must name a
/// column that is *not*. [`crate::generate`] is the one place that
/// invariant is established (see its `relationship_for`), and a violation is
/// a generator bug the engine would catch as an install rejection — a hard
/// failure, never a skip (design doc §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cardinality {
    ToOne,
    ToMany,
}

/// A standalone named relationship declaration (ADR-0006, issue #34):
/// `RELATIONSHIP <name> FROM <from_table>.<from_col> TO <to_table>.<to_col>`.
///
/// Plain data, like every other part of a [`Program`] — the concrete syntax
/// the backend hands `trellis::dev::defs::create_relationship` is rendered from
/// these fields (see `crate::backend::ManualBackend`), and the oracle renders
/// its own independent `SELECT` from them too.
///
/// A relationship is installed *before* any definition that references it
/// (`Program::relationships` is installed alongside the tables, ahead of
/// every definition — see `crate::run::run_convergence`), since a definition
/// naming an undeclared relationship is rejected at validation time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relationship {
    /// Unique per from-table (ADR-0006's naming scope). The generator draws
    /// globally-unique `r0`, `r1`, ... names, which satisfies that trivially.
    pub name: String,
    pub from_table: String,
    pub from_col: String,
    pub to_table: String,
    pub to_col: String,
    /// What the generator built this relationship to be — see
    /// [`Cardinality`] for why this is recorded rather than re-derived.
    pub cardinality: Cardinality,
}

/// What a generator expects an op's `apply()` to actually do at the backend
/// (design doc §4 "operation errors are checked, not swallowed"). Carried on
/// the [`Op`] itself (not a parallel `Vec`) so it shrinks with the op and
/// prints in a failing proptest counterexample.
#[derive(Debug, Clone, PartialEq)]
pub enum OpOutcome {
    /// The op is accepted and affects at least one row.
    Succeeds,
    /// The op is rejected outright — `apply()` returns `Err`. The
    /// classifier that produces this (see `run::run_convergence`) does not
    /// inspect *why* — any `Err` counts, not specifically the constraint
    /// violation a generator like `DuplicateInsert` is built to trigger. An
    /// op expecting `Fails` that instead errors for an unrelated reason
    /// (e.g. a transient connection failure) would still "match"; an op
    /// expecting `Succeeds`/`AffectsNoRows` that errors for any reason is
    /// still caught. A `FailsWith(kind)` variant would close that gap; not
    /// worth it until a generator actually needs to distinguish error kinds.
    Fails,
    /// The op is accepted but affects zero rows (e.g. an update/delete
    /// targeting a primary key that was never seeded).
    AffectsNoRows,
    /// Any of the listed outcomes is acceptable. Unused today — kept so the
    /// classifier stays exhaustive-ready for future nondeterministic
    /// (fault-injection) cases, where more than one real outcome is valid.
    AnyOf(Vec<OpOutcome>),
}

impl OpOutcome {
    /// Whether `actual` — a real, observed outcome, never itself `AnyOf` —
    /// satisfies `self`. For every variant but `AnyOf` this is plain
    /// equality; `AnyOf` is satisfied if any of its members is.
    pub fn matches(&self, actual: &OpOutcome) -> bool {
        match self {
            OpOutcome::AnyOf(options) => options.iter().any(|option| option.matches(actual)),
            _ => self == actual,
        }
    }
}

/// One source-table mutation. Values are the column's rendered *text* form
/// (`None` is SQL `NULL`), not a typed value: the backend seam applies each
/// op as raw source DML (design doc §1), where every bound parameter is
/// cast to its column's type in SQL text anyway, so there is no separate
/// typed representation to keep in sync here.
#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    Insert {
        table: String,
        row: Vec<(String, Option<String>)>,
        expect: OpOutcome,
    },
    Update {
        table: String,
        pk: String,
        changes: Vec<(String, Option<String>)>,
        expect: OpOutcome,
    },
    Delete {
        table: String,
        pk: String,
        expect: OpOutcome,
    },
    /// Clears every row of `table` in one statement (improvement-plan task
    /// E6). Rendered by [`crate::backend::ManualBackend::apply`] as a plain
    /// `TRUNCATE TABLE`, **not** trusted for its own affected-row count:
    /// Postgres's `TRUNCATE` command tag always reports `0` regardless of how
    /// many rows existed, so the backend synthesizes a real count itself (a
    /// `SELECT count(*)` in the same transaction, before issuing the
    /// `TRUNCATE`) rather than letting `tokio_postgres::Client::execute`'s
    /// raw return value flow into `run_convergence`'s
    /// `Ok(0) => AffectsNoRows` classifier — see that function's module doc
    /// comment and `ManualBackend::apply`'s own `Op::Truncate` arm for why
    /// this is load-bearing: a naive pass-through would misclassify every
    /// non-empty truncate as `AffectsNoRows`, silently defeating the "an op
    /// that errors changed nothing" / "operation errors are checked, not
    /// swallowed" invariant the whole run loop is built on.
    Truncate { table: String, expect: OpOutcome },
    /// Inserts every row of `rows` in a single `INSERT ... VALUES (...),
    /// (...), ...` statement (improvement-plan task E6's bulk-insert
    /// dimension) rather than [`Op::Insert`]'s one-row-per-statement shape.
    /// Every entry of `rows` must carry the same columns, in the same order,
    /// as every other entry — [`crate::backend::ManualBackend::apply`] takes
    /// the column list from `rows[0]` and panics (a generator bug) if a later
    /// row disagrees. A single multi-row `INSERT` is atomic exactly like a
    /// single-row one (Postgres either inserts every row or none), so
    /// `expect` covers the whole statement, not each row individually.
    BulkInsert {
        table: String,
        rows: Vec<Vec<(String, Option<String>)>>,
        expect: OpOutcome,
    },
}

impl Op {
    /// The outcome the generator expects this op's `apply()` to produce
    /// (design doc §4). See [`OpOutcome`].
    pub fn expect(&self) -> &OpOutcome {
        match self {
            Op::Insert { expect, .. }
            | Op::Update { expect, .. }
            | Op::Delete { expect, .. }
            | Op::Truncate { expect, .. }
            | Op::BulkInsert { expect, .. } => expect,
        }
    }
}

/// A generated program: a schema, the transform definitions over it, and a
/// sequence of source mutations (design doc §1).
#[derive(Debug, Clone, PartialEq)]
pub struct Program {
    pub tables: Vec<Table>,
    /// Named relationship declarations (issue #34, ADR-0006) installed once,
    /// alongside `tables` and ahead of every definition — a definition that
    /// names an undeclared relationship is rejected at validation time, and
    /// an install rejection is a hard failure, never a skip (design doc §3).
    ///
    /// Empty for every program that predates issue #34, which is why it sits
    /// here rather than being folded into `defs`: a relationship is a
    /// standalone declaration reusable across many definitions (ADR-0006),
    /// not a clause inside one.
    pub relationships: Vec<Relationship>,
    pub defs: Vec<TransformDef>,
    /// `def_install_after_op[i]` is how many of `ops` must already have been
    /// applied before `defs[i]` is installed (improvement-plan task E2):
    /// `0` — every existing program's default, drawn by every strategy that
    /// predates E2 — means "install up front, alongside every table, before
    /// any op runs" (today's only behavior, and [`crate::run::run_convergence`]'s
    /// default). A nonzero value `N` means "install this definition only once
    /// `ops[0..N]` have already been applied" — real rows can already exist
    /// in `defs[i].source` by then, exercising `defs::catalog::install_definition`'s
    /// direct-backfill-over-preexisting-rows path (the same path
    /// `generative/tests/backfill.rs` exercises by hand, now reachable from
    /// inside the harness's own op-stream loop). Must be index-aligned with
    /// `defs` (same length) — see [`crate::generate::defer_def_install`] for
    /// the one supported way to set an entry away from `0`, which enforces
    /// `1 <= N < ops.len()` (deferring to before op 0 is just the default;
    /// deferring to on/after the very last op would never reach a subsequent
    /// per-op convergence check, so [`crate::run::run_convergence`] never
    /// looks for it and a generator asking for it is a bug worth panicking on
    /// rather than silently dropping the install).
    pub def_install_after_op: Vec<usize>,
    pub ops: Vec<Op>,
    /// Op indices (improvement-plan task E3) after which
    /// [`crate::run::run_convergence`] calls [`crate::backend::Backend::restart`]
    /// on the backend — simulating an in-process engine client crash-and-restart
    /// mid-stream (see that trait method's doc comment). Entries must satisfy
    /// `1 <= n < ops.len()` (see [`crate::generate::schedule_restart`], the one
    /// supported way to add one) for the same "never after the last op" reason
    /// `def_install_after_op` documents. Empty for every program that predates
    /// E3.
    pub restart_after_ops: Vec<usize>,
    /// Op indices (improvement-plan task E3) after which
    /// [`crate::run::run_convergence`] calls [`crate::backend::Backend::scale_out`]
    /// on the backend — starting an additional application-worker-only engine
    /// client alongside the existing one(s). Same index contract as
    /// `restart_after_ops` — see [`crate::generate::schedule_scale_out`].
    pub scale_out_after_ops: Vec<usize>,
}

/// One action against a workstream-E, task-E1 "noise" table: a table
/// [`crate::backend::ManualBackend::install_noise_table`] creates (a real
/// Postgres table, so DDL/DML against it behaves exactly like against any
/// other) but never registers as tracked — never in a [`Program`]'s own
/// `tables`, so `run::check_program`'s oracle can never resolve a
/// definition's source/target against it (that function only ever walks
/// `Program.tables`/`Program.defs`), and never handed to the engine's
/// `ClientOptions.source_tables`, so CDC intake never watches it either.
/// Values follow the same rendered-text convention [`Op`] uses (`None` is
/// SQL `NULL`).
#[derive(Debug, Clone, PartialEq)]
pub enum NoiseAction {
    Insert {
        pk: i64,
        value: Option<String>,
    },
    Update {
        pk: i64,
        value: Option<String>,
    },
    Delete {
        pk: i64,
    },
    /// `ALTER TABLE ... ADD COLUMN` — DDL noise, not just DML (task E1's
    /// "ideally also some DDL" ask).
    AddColumn {
        name: String,
        value_type: ValueType,
    },
    /// `ALTER TABLE ... DROP COLUMN`.
    DropColumn {
        name: String,
    },
}

/// What a [`NoiseEvent`] fires: either a [`NoiseAction`] against the
/// [`NoisePlan`]'s own noise table (task E1), or a bare administrative SQL
/// statement with no table involved at all (task E5, e.g. `"CHECKPOINT"`).
#[derive(Debug, Clone, PartialEq)]
pub enum NoiseEventKind {
    Table(NoiseAction),
    Admin(String),
}

/// One point in a program's op stream where a [`NoiseEventKind`] fires,
/// interleaved with the real ops by
/// [`crate::run::run_convergence_with_noise`]. `before_op` is
/// `0..=ops.len()`: `0` fires once, immediately after install and before
/// `ops[0]`; `i` (`i >= 1`) fires once, immediately after `ops[i - 1]`'s own
/// apply -> quiesce -> snapshot -> compare cycle completes. Multiple events
/// may share the same `before_op` and fire in the order they appear in
/// [`NoisePlan::events`].
#[derive(Debug, Clone, PartialEq)]
pub struct NoiseEvent {
    pub before_op: usize,
    pub kind: NoiseEventKind,
}

/// A full noise/administration schedule for one program run (workstream E,
/// tasks E1 and E5): an optional untracked table to create once at install
/// time (`None` for an administration-only schedule, e.g. a `CHECKPOINT`-only
/// plan that needs no table at all — task E5), plus the [`NoiseEvent`]s to
/// fire against it (or standalone) as the real op stream plays out.
///
/// Deliberately never checked against the oracle, and — by construction,
/// since neither the noise table nor a bare admin statement is ever named in
/// a [`Program`]'s own `tables`/`defs` — structurally unable to perturb
/// `run::check_program`'s per-op comparison (see that function's doc
/// comment: it only ever resolves a definition's source/target through
/// `Program.tables`/`Program.defs`).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct NoisePlan {
    pub table: Option<Table>,
    pub events: Vec<NoiseEvent>,
}

impl NoisePlan {
    /// A schedule with no table and no events — the property/harness case
    /// where a draw happens to produce zero noise, and a convenient base for
    /// a hand-built pin that only needs `events.push`.
    pub fn empty() -> Self {
        Self::default()
    }
}

/// The composite row-key convention for an [`trellis::dev::defs::ast::KeySpace::Aggregate`]
/// target (improvement-plan task B4): every row in a `GROUP BY` target is
/// keyed by its grouping column(s)' rendered text values, but unlike a 1-1
/// target's primary key (never `NULL`, enforced by the source table's own
/// `primary key` constraint), a grouping column genuinely can be `NULL` —
/// Postgres's `GROUP BY` groups every `NULL` grouping value together as one
/// group — but the generative suite's grain column never actually draws
/// `NULL` (see `crate::generate::strategy::grain_value`'s doc comment for the
/// real engine bug a `NULL` grouping value hits, which is why this stays
/// scoped out of the generator for now); this function still handles a
/// `NULL` component correctly regardless, since a hand-built pin can
/// construct one directly (see [`crate::generate::TableSpec::grain_values`]'s
/// doc comment), and [`trellis::dev::defs::oracle::recompute_aggregate`]'s own
/// private `group_key` this mirrors has no such restriction either.
/// [`crate::oracle`]'s SQL
/// oracle, its evaluator oracle (via [`trellis::dev::defs::oracle::recompute_aggregate`],
/// whose own private `group_key` uses this exact same length-prefixing
/// scheme independently), and [`crate::backend::ManualBackend`]'s persisted-
/// target reader all key their rows through this one function, so a group's
/// identity lines up across all three sources of the three-way comparison
/// regardless of whether any grouping column is `NULL`.
///
/// Each component is length-prefixed (`"{len}:{value}"`, `NULL` rendered as
/// the empty string before prefixing) rather than joined on a bare
/// separator, so a grouping column whose value itself contains the
/// separator character can never make two distinct groupings collide.
pub fn group_key(values: &[Option<String>]) -> String {
    values
        .iter()
        .map(|v| v.clone().unwrap_or_default())
        .map(|v| format!("{}:{v}", v.len()))
        .collect()
}

/// Small, fixed name pools (`t0`, `d0`, `c0`, ...) rather than random
/// identifiers — a shrunk counterexample you can read at a glance beats one
/// that is technically smaller (design doc §1). Each kind of name has its
/// own counter, so tables, definitions, and columns each start at 0
/// independently.
#[derive(Debug, Default, Clone)]
pub struct NamePool {
    tables: usize,
    defs: usize,
    columns: usize,
}

impl NamePool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn next_table_name(&mut self) -> String {
        let n = self.tables;
        self.tables += 1;
        format!("t{n}")
    }

    pub fn next_def_name(&mut self) -> String {
        let n = self.defs;
        self.defs += 1;
        format!("d{n}")
    }

    pub fn next_column_name(&mut self) -> String {
        let n = self.columns;
        self.columns += 1;
        format!("c{n}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_pools_are_stable_and_independent_across_construction() {
        let mut pool = NamePool::new();
        assert_eq!(pool.next_table_name(), "t0");
        assert_eq!(pool.next_table_name(), "t1");
        assert_eq!(pool.next_def_name(), "d0");
        assert_eq!(pool.next_column_name(), "c0");
        assert_eq!(pool.next_column_name(), "c1");
        assert_eq!(pool.next_def_name(), "d1");
        assert_eq!(pool.next_table_name(), "t2");
    }

    #[test]
    fn every_table_gets_its_pk_column_unconditionally() {
        let mut pool = NamePool::new();
        let table = Table::new(&mut pool, &[]);
        assert_eq!(table.columns.len(), 1);
        assert_eq!(table.columns[0].name, table.pk_col);
        assert_eq!(table.columns[0].value_type, ValueType::Numeric);
    }

    #[test]
    fn a_table_with_extra_columns_still_has_the_pk_first() {
        let mut pool = NamePool::new();
        let table = Table::new(&mut pool, &[ValueType::Numeric, ValueType::Text]);
        assert_eq!(table.pk_col, "c0");
        assert_eq!(
            table
                .columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            vec!["c0", "c1", "c2"]
        );
        assert_eq!(table.columns[1].value_type, ValueType::Numeric);
        assert_eq!(table.columns[2].value_type, ValueType::Text);
    }

    #[test]
    fn group_key_distinguishes_null_from_a_real_value() {
        assert_ne!(group_key(&[None]), group_key(&[Some("0".to_string())]));
    }

    #[test]
    fn group_key_is_stable_for_equal_inputs() {
        assert_eq!(
            group_key(&[Some("1".to_string()), None]),
            group_key(&[Some("1".to_string()), None])
        );
    }

    #[test]
    fn group_key_length_prefixing_avoids_a_naive_join_collision() {
        // A bare `,`-joined key would make these two distinct groupings
        // collide (`("x,y", "z")` vs. `("x", "y,z")`); length-prefixing each
        // component keeps them apart.
        let a = group_key(&[Some("x,y".to_string()), Some("z".to_string())]);
        let b = group_key(&[Some("x".to_string()), Some("y,z".to_string())]);
        assert_ne!(a, b);
    }

    #[test]
    fn a_program_round_trips_and_prints_legibly_via_debug() {
        let mut pool = NamePool::new();
        let table = Table::new(&mut pool, &[ValueType::Numeric]);
        let program = Program {
            tables: vec![table.clone()],
            relationships: Vec::new(),
            defs: Vec::new(),
            def_install_after_op: Vec::new(),
            ops: vec![Op::Insert {
                table: table.name.clone(),
                row: vec![
                    (table.pk_col.clone(), Some("1".to_string())),
                    ("c1".to_string(), Some("2".to_string())),
                ],
                expect: OpOutcome::Succeeds,
            }],
            restart_after_ops: Vec::new(),
            scale_out_after_ops: Vec::new(),
        };
        let printed = format!("{program:?}");
        assert!(printed.contains("Program"));
        assert!(printed.contains("t0"));
        assert!(printed.contains("Insert"));
        assert_eq!(program, program.clone());
    }
}
