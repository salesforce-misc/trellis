//! Plain-data description of a generated program (design doc §1,
//! `docs/generative-test-suite.md`).
//!
//! A [`Program`] is exactly what the (future) shrinker minimizes and what
//! prints on a failing case: no engine internals beyond the AST types it
//! deliberately reuses ([`TransformDef`], [`ValueType`]) so a definition
//! this crate generates is byte-for-byte the same shape the parser produces
//! from concrete syntax.

use trellis::dev::defs::IntWidth;
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
    /// primary key — see `pk_col`). The generator's tables leave it empty;
    /// a hand-built pin sets it to give a source a unique column.
    pub unique_cols: Vec<String>,
    /// How the table is partitioned (ADR-0017), or `None` for a plain table.
    /// A partitioned table's primary key is `(pk_col, column)`, the
    /// partition column is `NOT NULL`, and it has no `UNIQUE` constraint
    /// (`unique_cols` is empty): Postgres requires every unique index on a
    /// partitioned table to hold the partition key.
    pub partitioning: Option<Partitioning>,
}

/// A partitioned [`Table`]'s single level: the column it is partitioned by,
/// and how.
#[derive(Debug, Clone, PartialEq)]
pub struct Partitioning {
    pub column: String,
    pub strategy: PartitionStrategy,
}

/// How a partitioned [`Table`] routes its rows to its leaves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionStrategy {
    /// `HASH`, with this many leaves (`1..`).
    Hash { modulus: u8 },
    /// `LIST`: one leaf per value in `0..=values - 1`, plus a `DEFAULT` leaf
    /// when `default`. Without one, the strategy must hold every value the
    /// program writes.
    List { values: u8, default: bool },
    /// `RANGE`: leaves `(MINVALUE, 1)`, `[1, 2)`, ..., `[bounds - 1,
    /// MAXVALUE)`, so every value routes.
    Range { bounds: u8 },
}

/// The [`ValueType`] every generated table's primary-key column carries.
///
/// The generator seeds primary keys as consecutive integers
/// (`1..=seed_count`, see `crate::generate::build_program_multi_with_shapes`),
/// and since issue #111 [`ValueType`] can finally *say* that: `bigint`.
///
/// This used to be a bare `PRIMARY_KEY_PG_TYPE: &str = "bigint"` constant
/// that every DDL and cast site had to special-case, precisely because the
/// `ValueType` on the PK's own [`Column`] could not name the type the column
/// was declared with. The only numeric variant mapped to Postgres `numeric`,
/// which the engine rejects as a primary key outright
/// (`defs::ddl::DdlError::UnsupportedPrimaryKeyType`, issue #107: `::text`
/// key matching is not sound for `numeric`, since `1.0::text != 1.00::text`
/// though the two are numerically equal) — so declaring the PK from its
/// `ValueType` made *every* generated program fail to install. Recording the
/// real type removes the special case and, as a bonus, gives the whole
/// generative suite continuous end-to-end coverage of an exact-integer
/// primary key against the Postgres oracle.
pub const PRIMARY_KEY_VALUE_TYPE: ValueType = ValueType::Integer(IntWidth::Int8);

impl Table {
    /// Builds a table from `pool`, giving it a primary-key column
    /// unconditionally — before any of `column_types` — so no future shrink
    /// step can strand a definition that references it (design doc §1).
    ///
    /// The PK column's [`ValueType`] is [`PRIMARY_KEY_VALUE_TYPE`], and
    /// since issue #111 that is an honest `bigint` rather than a
    /// `ValueType::Numeric` placeholder the DDL and cast sites had to
    /// override — see that constant.
    pub fn new(pool: &mut NamePool, column_types: &[ValueType]) -> Table {
        let name = pool.next_table_name();
        let pk_col = pool.next_column_name();
        let mut columns = Vec::with_capacity(column_types.len() + 1);
        columns.push(Column {
            name: pk_col.clone(),
            value_type: PRIMARY_KEY_VALUE_TYPE,
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
            partitioning: None,
        }
    }

    /// The type of `column` on this table, or `None` if it has no such
    /// column.
    pub fn column_type(&self, column: &str) -> Option<ValueType> {
        self.columns
            .iter()
            .find(|c| c.name == column)
            .map(|c| c.value_type)
    }
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
    /// `op`, a write on a partitioned table (ADR-0017), issued naming the
    /// table `leaf` instead of the root: a leaf of the tree, or a table
    /// detached from it. `op` names the root, whose [`Table`] gives the
    /// columns' types; only the statement's relation changes. `op.expect()`
    /// is this op's expectation.
    Leaf { leaf: String, op: Box<Op> },
    /// Partition maintenance on the partitioned `table` (ADR-0017): DDL that
    /// changes which rows the root holds without a write any capture trigger
    /// sees. A backend reports a successful one as one affected row, so its
    /// `expect` is [`OpOutcome::Succeeds`] or [`OpOutcome::Fails`].
    Partition {
        table: String,
        action: PartitionAction,
        expect: OpOutcome,
    },
}

/// One [`Op::Partition`] statement. Every leaf is named unqualified, beside
/// its root, and `bound` is a partition bound clause as `CREATE TABLE ...
/// PARTITION OF` takes it (`for values in (1)`, `default`).
#[derive(Debug, Clone, PartialEq)]
pub enum PartitionAction {
    /// `ALTER TABLE <root> DETACH PARTITION <leaf> [CONCURRENTLY]`. The
    /// table stays, with its rows, outside the tree.
    Detach { leaf: String, concurrently: bool },
    /// `ALTER TABLE <root> ATTACH PARTITION <leaf> <bound>` of a table
    /// detached earlier, with whatever rows it holds.
    Attach { leaf: String, bound: String },
    /// `DROP TABLE <leaf>` of a leaf in the tree.
    Drop { leaf: String },
    /// A new leaf holding `rows` (each a full row of the root's columns), in
    /// one transaction: created `PARTITION OF` the root and then written
    /// directly, or, when `standalone`, created `LIKE` the root, written,
    /// and then attached.
    Create {
        leaf: String,
        bound: String,
        rows: Vec<Vec<(String, Option<String>)>>,
        standalone: bool,
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
            | Op::BulkInsert { expect, .. }
            | Op::Partition { expect, .. } => expect,
            Op::Leaf { op, .. } => op.expect(),
        }
    }

    /// Sets the outcome the generator expects of this op (of the wrapped
    /// write, for an [`Op::Leaf`]).
    pub fn set_expect(&mut self, outcome: OpOutcome) {
        match self {
            Op::Insert { expect, .. }
            | Op::Update { expect, .. }
            | Op::Delete { expect, .. }
            | Op::Truncate { expect, .. }
            | Op::BulkInsert { expect, .. }
            | Op::Partition { expect, .. } => *expect = outcome,
            Op::Leaf { op, .. } => op.set_expect(outcome),
        }
    }

    /// The program table this op writes: the root, for an [`Op::Leaf`].
    pub fn table(&self) -> &str {
        match self {
            Op::Insert { table, .. }
            | Op::Update { table, .. }
            | Op::Delete { table, .. }
            | Op::Truncate { table, .. }
            | Op::BulkInsert { table, .. }
            | Op::Partition { table, .. } => table,
            Op::Leaf { op, .. } => op.table(),
        }
    }
}

/// A generated program: a schema, the transform definitions over it, and a
/// sequence of source mutations (design doc §1).
#[derive(Debug, Clone, PartialEq)]
pub struct Program {
    pub tables: Vec<Table>,
    pub defs: Vec<TransformDef>,
    /// `def_install_after_op[i]` is how many of `ops` must already have been
    /// applied before `defs[i]` is installed (improvement-plan task E2):
    /// `0` — every existing program's default, drawn by every strategy that
    /// predates E2 — means "install up front, alongside every table, before
    /// any op runs" (today's only behavior, and [`crate::run::run_convergence`]'s
    /// default). A nonzero value `N` means "install this definition only once
    /// `ops[0..N]` have already been applied" — real rows can already exist
    /// in `defs[i].source` by then, exercising the build
    /// over pre-existing rows (the same path
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
/// `Program.tables`/`Program.defs`), and no definition reads it, so the
/// engine never captures it either.
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

/// How the concurrent tier (issue #557) issues a [`Program`]'s ops: a
/// sequence of [`Burst`]s, run one after another. Every burst is checked
/// against the oracle once, after all of its lanes have finished and the
/// engine has quiesced; nothing is checked mid-burst.
///
/// Built by `crate::generate::concurrent_plan`, which is the one place the
/// rules for what may share a burst or be split across lanes live.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ConcurrentPlan {
    pub bursts: Vec<Burst>,
    /// A steady load the whole run is issued under, or `None` to issue every
    /// burst flat out (#720, #725).
    pub steady_load: Option<SteadyLoad>,
    /// Whether the run waits, after installing, for every definition to
    /// settle before the first burst's first op. The JOIN tier (#1113) draws
    /// it for some cases, so their up-front definitions build over empty
    /// tables; the others race the first burst. Every other tier leaves it
    /// off.
    pub settle_after_install: bool,
    /// The longest stall each page takes between its last write and its
    /// commit, for the whole run (`trellis::dev::interleave::set_commit_stall`),
    /// or `None`. The JOIN tier (#1114) draws it for some cases, so a page
    /// that skipped a lock reads another's uncommitted write now and then.
    pub commit_stall: Option<std::time::Duration>,
}

/// A steady load on the engine, for a whole [`ConcurrentPlan`] (#720,
/// #725). Flat out, a burst's lanes issue every op in tens of milliseconds:
/// a build an action starts runs its chunks after the writes are over, and
/// a page is never slow next to another, so segments drain in order. Two
/// races the tier exists to catch then never get a window: a build chunk
/// reading an entry that a page has locked but not yet written, and a
/// segment draining past an older one whose page hasn't taken its entry
/// lock yet. The pace holds only from an action that starts a build to the
/// end of its burst, unless [`SteadyLoad::pace_from_start`]: every other
/// burst still goes flat out, so it still seals into batches large enough
/// to split across workers.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SteadyLoad {
    /// How long each lane waits after each op, once an action has started a
    /// build, so the rest of the burst's writes trickle in, as a steady
    /// source's do, while the build runs.
    pub pace: std::time::Duration,
    /// Whether the pace holds from every burst's start, rather than only
    /// once an action starts a build. The cooling-backfill tier (#734) paces
    /// its whole run, so a `request_backfill`'s rebuild runs among the
    /// writes of the keys going quiet around it rather than after a burst
    /// written flat out in tens of milliseconds.
    pub pace_from_start: bool,
    /// The longest stall a page or a build chunk takes at its entry-lock
    /// step (`trellis::dev::interleave::set_stall`), all run long, so some
    /// pages are slow next to others, as a busy worker's are.
    pub stall: std::time::Duration,
}

/// One burst of a [`ConcurrentPlan`]: `lanes[i]` is the `program.ops`
/// indices lane `i` applies, in order, from its own harness task and its own
/// connection, concurrently with every other lane. Across lanes there is no
/// order at all, so Postgres commits their ops in whatever order the tasks
/// happen to reach it.
///
/// `actions` are operator actions the harness takes while the lanes run
/// (issue #557 part 2), in order of [`TimedAction::after`]: each fires once
/// that many of the burst's ops have been applied, by whichever lanes, so
/// the work it starts overlaps the burst's CDC rather than landing at a
/// quiescent point.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Burst {
    pub lanes: Vec<Vec<usize>>,
    pub actions: Vec<TimedAction>,
}

/// A [`BurstAction`] and when in its burst it fires (issue #557 part 2).
#[derive(Debug, Clone, PartialEq)]
pub struct TimedAction {
    /// How many of the burst's ops must have been applied, across all its
    /// lanes, before the action fires: `0` fires as the lanes start, the
    /// burst's op count once they have all applied their last op (while the
    /// engine is still draining them).
    pub after: usize,
    pub action: BurstAction,
}

/// An operator action the concurrent tier takes mid-burst (issue #557 part
/// 2). Each is named after the public API call an operator makes, not the
/// engine mechanism behind it. None of them changes what the converged
/// targets should hold, so the oracle is unchanged: the burst is
/// checked once it settles, like any other.
#[derive(Debug, Clone, PartialEq)]
pub enum BurstAction {
    /// `Trellis::request_backfill(table)`: re-derive every definition that
    /// reads `table`.
    RequestBackfill { table: String },
    /// `PAUSE TRANSFORM <target>` or, with a column, `PAUSE TRANSFORM
    /// <target>.<column>`. A generated plan always resumes it later in the
    /// same burst: a definition left paused would never converge.
    Pause {
        target: String,
        column: Option<String>,
    },
    /// `RESUME TRANSFORM <target>[.<column>]`. A whole-transform resume
    /// rebuilds the target from the source; a column resume re-derives the
    /// column on every row. The column form is the same call that recovers a
    /// column after its poison fuse trips (ADR-0014: one paused state, two
    /// triggers).
    Resume {
        target: String,
        column: Option<String>,
    },
    /// `DEFINE TRANSFORM` for `program.defs[def]`, whose
    /// [`Program::def_install_after_op`] falls in this burst: its build runs
    /// while the burst still writes the source (the shape #625's
    /// build-under-load divergence needs).
    Install { def: usize },
}

impl BurstAction {
    /// Whether the action starts a Re-derive build: an install, or a
    /// whole-transform resume, which rebuilds over the target's existing
    /// ledger entries.
    pub fn starts_build(&self) -> bool {
        matches!(
            self,
            BurstAction::Install { .. } | BurstAction::Resume { column: None, .. }
        )
    }

    /// The name coverage reports this action under.
    pub fn name(&self) -> &'static str {
        match self {
            BurstAction::RequestBackfill { .. } => "request_backfill",
            BurstAction::Pause { column: None, .. } => "pause",
            BurstAction::Pause {
                column: Some(_), ..
            } => "pause_column",
            BurstAction::Resume { column: None, .. } => "resume",
            BurstAction::Resume {
                column: Some(_), ..
            } => "resume_column",
            BurstAction::Install { .. } => "install",
        }
    }
}

impl Burst {
    /// The highest op index in this burst: the op a divergence found after
    /// it is reported against, as in `crate::run::run_convergence_bursty`.
    /// `None` for a burst with no ops.
    pub fn last_op(&self) -> Option<usize> {
        self.lanes.iter().flatten().copied().max()
    }

    /// Every op index in this burst, in program order.
    pub fn ops(&self) -> Vec<usize> {
        let mut ops: Vec<usize> = self.lanes.iter().flatten().copied().collect();
        ops.sort_unstable();
        ops
    }
}

/// How a [`DbAdminAction::RestartPostgres`] stops the server before starting
/// it again (issue #236).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartMode {
    /// An orderly shutdown: open transactions roll back, clients are
    /// disconnected, a shutdown checkpoint is written.
    Fast,
    /// No shutdown checkpoint, so the next start replays WAL through crash
    /// recovery — what a power loss or an OOM kill of the postmaster leaves.
    Immediate,
}

/// A database-administration action (issue #236, layer-3 bucket 5 in
/// `docs/generative-test-suite.md` §7). Each one leaves the correct
/// converged state unchanged, so the oracle needs no changes: the run
/// quiesces after the op the action is anchored to and compares as usual.
/// What each action adds is a demand on the engine, stated per variant.
#[derive(Debug, Clone, PartialEq)]
pub enum DbAdminAction {
    /// `CHECKPOINT`, fired after the anchor op is applied and before the
    /// run waits for it, so the op's change is usually still in flight.
    Checkpoint,
    /// Restarts the Postgres server, at the same point as `Checkpoint`.
    /// Every connection the engine holds (producer session, pool, wake
    /// listener) is severed while work is in flight, and
    /// the engine has to reconnect by itself: the harness never restarts the
    /// engine client for it.
    RestartPostgres(RestartMode),
}

/// One [`DbAdminAction`], anchored to `ops[op]` (so `op < ops.len()`). See
/// each action for where exactly around that op it fires. Several events
/// may share an anchor. They fire in the order they appear in
/// [`DbAdminPlan::events`].
#[derive(Debug, Clone, PartialEq)]
pub struct DbAdminEvent {
    pub op: usize,
    pub action: DbAdminAction,
}

/// The database-administration schedule for one program run (issue #236),
/// interleaved with its ops by `crate::run::run_convergence_with_db_admin`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DbAdminPlan {
    pub events: Vec<DbAdminEvent>,
}

/// How a [`RestorePlan`] backs the source cluster up (issue #236). Trellis's
/// catalog, ring, capture triggers and targets all live in the backed-up
/// database, so a restore rolls them all back together (the 2026-09-24
/// decision on issue #236).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackupKind {
    /// A file-level copy of the whole data directory, taken while the
    /// server is stopped. Capture carries on from the restored point with no
    /// pause.
    ColdCopy,
    // TODO(#558): `BaseBackup`, a `pg_basebackup` restore. Capture state
    // lives in ordinary tables (#622), so it should behave like `ColdCopy`.
}

/// A backup-and-restore schedule for one program run (issue #236), driven by
/// `crate::run::run_convergence_with_restore`: back the cluster up right
/// after `ops[backup_op]` is applied, run the program to its end, restore
/// the backup into a fresh cluster, then replay `ops[backup_op + 1..]`
/// against the restored database and converge again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestorePlan {
    pub backup_op: usize,
    pub kind: BackupKind,
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
        // Issue #111: an honest `bigint`, not the `Numeric` placeholder the
        // DDL and cast sites used to have to override — see
        // [`PRIMARY_KEY_VALUE_TYPE`].
        assert_eq!(table.columns[0].value_type, PRIMARY_KEY_VALUE_TYPE);
        assert_eq!(
            table.columns[0].value_type,
            ValueType::Integer(IntWidth::Int8)
        );
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
