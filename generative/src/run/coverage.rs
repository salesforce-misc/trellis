//! A green run is currently a single bit: nobody can see that a `Delete` op
//! stopped being drawn, or that a run drew zero mutates, or that the
//! op-outcome paths added in A1 (`OpOutcome::Fails`/`AffectsNoRows`) stopped
//! occurring at all (`local_docs/generative-suite-improvement-plan.md` §A2).
//! [`Coverage`] is the accumulator that makes a run report what it actually
//! exercised: it walks a [`crate::model::Program`]'s plain-data structure —
//! no database access — so the exact same accounting works both against
//! programs actually driven through a backend (`tests/convergence.rs`'s
//! `Harness`) and against raw strategy draws sampled with no cluster at all
//! (`tests/coverage.rs`'s floor test).

use std::collections::{HashMap, HashSet};
use std::fmt;

use trellis::dev::defs::ast::{Expr, KeySpace, Operator, TransformDef, ValueType};
use trellis::dev::defs::registry;

use crate::backend::{DrainAudit, SPLIT_THRESHOLD_ROWS};
use crate::model::{
    BurstAction, Cardinality, ConcurrentPlan, Op, OpOutcome, Program, Relationship,
};

/// Per-run coverage tallies over any number of [`Program`]s. Every field is
/// keyed on the `&'static str` name of the variant it tallies, not the
/// trellis/model enum itself — [`ValueType`] in particular has no `Hash` impl
/// (it's an engine type, not owned by this crate), so this follows the same
/// name-mapping convention `tests/coverage.rs`'s `sorted_value_types` helper
/// already uses.
#[derive(Debug, Default)]
pub struct Coverage {
    /// Number of `record_program` calls, i.e. cases accounted for.
    pub cases: usize,
    /// `"Insert"` / `"Update"` / `"Delete"`, tallied from [`crate::model::Op`].
    pub ops_by_kind: HashMap<&'static str, usize>,
    /// `"Succeeds"` / `"Fails"` / `"AffectsNoRows"` / `"AnyOf"`, tallied from
    /// each op's [`Op::expect`] (added in A1).
    pub ops_by_outcome: HashMap<&'static str, usize>,
    /// Which [`Expr`] variant names appear across every `FieldDef.expr` on
    /// every def, walked recursively.
    pub expr_shapes: HashSet<&'static str>,
    /// Which [`Operator`] names appear on any `Expr::BinaryOp` encountered
    /// while walking `expr_shapes`.
    pub operators: HashSet<&'static str>,
    /// Which function names (the [`Expr::FunctionCall`] `name` field, one of
    /// `trellis::dev::defs::registry::FUNCTIONS`/`AGGREGATE_FUNCTION_SPECS`'s
    /// canonical uppercased names) appear across every `FieldDef.expr` on
    /// every def, walked recursively alongside `expr_shapes` (improvement-plan
    /// task B2). An unrecognized function name is tallied as `"Other"` rather
    /// than panicking — this accumulator has no failure mode of its own, it
    /// just can't name what it's never been told about.
    pub functions: HashSet<&'static str>,
    /// Which [`ValueType`] names appear on any column of any table.
    pub types_exercised: HashSet<&'static str>,
    /// `"OneToOne"` / `"Aggregate"`, tallied from each def's [`KeySpace`].
    pub key_spaces: HashMap<&'static str, usize>,
    /// The deepest `Expr` tree seen across every `FieldDef.expr` on every
    /// def, a leaf (`Column`/`NumberLiteral`/`StringLiteral`/
    /// `RelationshipPath`) counting as depth 1 (improvement-plan task B2):
    /// the floor a nested, multi-level expression (e.g. `STRPOS(t, 'x') > 0`,
    /// depth 3) needs to prove itself against, distinct from `expr_shapes`
    /// (which shapes appear at all) since a program could draw both
    /// `BinaryOp` and `FunctionCall` shapes without ever nesting one inside
    /// the other.
    pub max_expr_depth: usize,
    /// Number of definitions, across every recorded program, whose
    /// [`Program::def_install_after_op`] entry is nonzero — i.e. actually
    /// deferred mid-stream rather than installed up front (improvement-plan
    /// task E2). A floor test over [`crate::generate::program_with_mid_stream_def_install`]
    /// asserts this is nonzero across enough samples: a coverage bit for "a
    /// mid-stream install was actually drawn", not just "the strategy exists".
    pub mid_stream_def_installs: usize,
    /// Total [`Program::restart_after_ops`] entries across every recorded
    /// program (improvement-plan task E3) — how many scheduled client
    /// restarts were actually drawn.
    pub client_restarts: usize,
    /// Total [`Program::scale_out_after_ops`] entries across every recorded
    /// program (improvement-plan task E3) — how many scheduled scale-outs
    /// were actually drawn.
    pub client_scale_outs: usize,
    /// Total [`Program::relationships`] declared across every recorded
    /// program (issue #34) — how many `RELATIONSHIP` declarations the
    /// generator actually emitted, as opposed to merely being able to.
    pub relationships_declared: usize,
    /// `"to_one"` / `"to_many"`, tallied from each declared relationship's
    /// [`Cardinality`] (issue #34). Both cardinalities have materially
    /// different engine paths — a `LEFT JOIN`-shaped lookup versus an
    /// aggregate over related rows, with different reverse-propagation and
    /// replica-identity requirements — so a run that drew only one of them
    /// is only half-covered.
    pub relationship_cardinalities: HashMap<&'static str, usize>,
    /// Which of the three engine-supported relationship *reference* shapes
    /// appear across every recorded program's calculated fields (issue #34):
    /// `"to_one_bare"`, `"to_many_in_aggregate"`, and
    /// `"to_one_in_aggregate_def"` — see
    /// [`crate::generate::RelFieldKind`]'s shape table. Tracked separately
    /// from `relationship_cardinalities` because cardinality alone doesn't
    /// say how a field *reads* the relationship, and the third shape in
    /// particular (a to-one path aggregated inside a `GROUP BY`) is a
    /// distinct engine code path from the other two.
    ///
    /// A shape this accumulator can't classify is tallied as `"other"`
    /// rather than panicking — like [`function_name`], this has no failure
    /// mode of its own. A nonzero `"other"` in a coverage report is itself
    /// the signal: it means the generator drew a shape nobody taught this
    /// accumulator about, which is exactly the "coverage that silently drops
    /// out" case a floor test should catch.
    pub relationship_shapes: HashMap<&'static str, usize>,
    /// How many recorded programs (cases, not fields) contain each aggregate
    /// call shape, keyed `(key_space, function, argument)` — e.g.
    /// `("Aggregate", "COUNT", "rel")` for `COUNT(<rel>.<col>)` inside a
    /// `GROUP BY` def (issue #294), or `("Aggregate", "BOOL_AND", "column")`
    /// (issue #255). `functions` above only says a name appeared somewhere;
    /// this says how often each *shape* was actually driven, which is what a
    /// deep run needs to report to show a newly-drawn shape got real
    /// exercise. Only calls to `registry::AGGREGATE_FUNCTIONS` names are
    /// tallied; the argument is `"*"` (no argument), `"column"`, `"rel"` (a
    /// relationship path), or `"expr"` (anything else).
    pub aggregate_call_cases: HashMap<(&'static str, &'static str, &'static str), usize>,
    /// How many recorded programs (cases) drive each relationship
    /// **reverse-propagation path**, keyed `(shape, path)` where `shape` is a
    /// [`Self::relationship_shapes`] name and `path` is one of the
    /// [`RelPath`] names (issue #505). Computed by replaying the program's
    /// ops against an in-memory copy of the source tables, so "referenced"
    /// means a live from-side row joins the parent row *at that moment*, not
    /// merely that the program declares a relationship. A reverse-path bug
    /// can only be caught by a case that reaches its path, so this is the
    /// number a deep run reports to show the parent side really got fuzzed.
    pub relationship_path_cases: HashMap<(&'static str, &'static str), usize>,
    /// The largest [`Op::BulkInsert`] row count seen across every recorded
    /// program (improvement-plan task E6) — a floor test asserts this
    /// actually gets large across enough samples of
    /// [`crate::generate::bulk_insert_program`], not just that a small one
    /// occasionally shows up.
    pub max_bulk_insert_rows: usize,
    /// Bursts recorded by [`Coverage::record_concurrent_plan`] (issue #557).
    pub concurrent_bursts: usize,
    /// How many recorded concurrent-tier cases contain at least one burst of
    /// each [`ConcurrentShape`], keyed by its name (issue #557). Worked out
    /// from the program and its plan alone, in program order: the lanes of a
    /// burst race, so the engine sees some interleaving of it, but which
    /// keys, groups and row counts a burst carries doesn't depend on that.
    pub concurrent_shape_cases: HashMap<&'static str, usize>,
    /// How many of each mid-burst operator action the recorded concurrent
    /// plans take, keyed by [`BurstAction::name`] (issue #557 part 2).
    pub concurrent_actions: HashMap<&'static str, usize>,
    /// Every [`DrainAudit`] recorded by [`Coverage::record_drain`], added up
    /// (issue #557): what the engine actually sealed and claimed, where
    /// `concurrent_shape_cases` is only what the program offered it.
    pub drain: DrainAudit,
    /// How many recorded runs' drain audits show each of: a split batch
    /// (`"split"`), a split batch claimed by two or more workers
    /// (`"split_across_workers"`), and a key sealed into several batches
    /// within one burst (`"key_in_several_batches"`) (issue #557).
    pub drain_cases: HashMap<&'static str, usize>,
}

impl Coverage {
    pub fn new() -> Self {
        Self::default()
    }

    /// Tallies one program into this accumulator: one case, its ops (by kind
    /// and by expected outcome), every calculated field's expression shape
    /// and operator across `program.defs`, every column's scalar type across
    /// `program.tables`, and every def's key-space.
    pub fn record_program(&mut self, program: &Program) {
        self.cases += 1;

        for op in &program.ops {
            *self.ops_by_kind.entry(op_kind(op)).or_insert(0) += 1;
            *self
                .ops_by_outcome
                .entry(op_outcome_name(op.expect()))
                .or_insert(0) += 1;
        }

        for def in &program.defs {
            *self
                .key_spaces
                .entry(key_space_name(&def.key_space))
                .or_insert(0) += 1;
            for field in &def.fields {
                self.record_expr(&field.expr);
            }
        }

        for table in &program.tables {
            for column in &table.columns {
                self.types_exercised
                    .insert(value_type_name(column.value_type));
            }
        }

        // Improvement-plan task E2.
        self.mid_stream_def_installs += program
            .def_install_after_op
            .iter()
            .filter(|&&at| at != 0)
            .count();
        // Improvement-plan task E3.
        self.client_restarts += program.restart_after_ops.len();
        self.client_scale_outs += program.scale_out_after_ops.len();
        // Improvement-plan task E6.
        for op in &program.ops {
            if let Op::BulkInsert { rows, .. } = op {
                self.max_bulk_insert_rows = self.max_bulk_insert_rows.max(rows.len());
            }
        }

        // Issues #255/#294: per-case aggregate call shapes.
        let mut aggregate_calls = HashSet::new();
        for def in &program.defs {
            let key_space = key_space_name(&def.key_space);
            for field in &def.fields {
                collect_aggregate_calls(&field.expr, key_space, &mut aggregate_calls);
            }
        }
        for shape in aggregate_calls {
            *self.aggregate_call_cases.entry(shape).or_insert(0) += 1;
        }

        // Issue #34.
        self.relationships_declared += program.relationships.len();
        for rel in &program.relationships {
            *self
                .relationship_cardinalities
                .entry(cardinality_name(rel.cardinality))
                .or_insert(0) += 1;
        }
        let by_name: HashMap<&str, &Relationship> = program
            .relationships
            .iter()
            .map(|r| (r.name.as_str(), r))
            .collect();
        let mut reads = Vec::new();
        for def in &program.defs {
            for field in &def.fields {
                collect_relationship_reads(&field.expr, def, &by_name, false, &mut reads);
            }
        }
        for read in &reads {
            *self.relationship_shapes.entry(read.shape).or_insert(0) += 1;
        }

        // Issue #505.
        for key in relationship_path_events(program, &reads) {
            *self.relationship_path_cases.entry(key).or_insert(0) += 1;
        }
    }

    /// Tallies one concurrent-tier case's [`ConcurrentShape`]s into
    /// [`Self::concurrent_shape_cases`], and its bursts into
    /// [`Self::concurrent_bursts`] (issue #557). Separate from
    /// [`Self::record_program`], which the harness calls as well.
    pub fn record_concurrent_plan(&mut self, program: &Program, plan: &ConcurrentPlan) {
        self.concurrent_bursts += plan.bursts.len();
        for timed in plan.bursts.iter().flat_map(|b| &b.actions) {
            *self
                .concurrent_actions
                .entry(timed.action.name())
                .or_insert(0) += 1;
        }
        for shape in concurrent_shapes(program, plan) {
            *self.concurrent_shape_cases.entry(shape.name()).or_insert(0) += 1;
        }
    }

    /// Adds one run's [`DrainAudit`] into [`Self::drain`] and
    /// [`Self::drain_cases`] (issue #557).
    pub fn record_drain(&mut self, audit: &DrainAudit) {
        self.drain.add(audit);
        for (name, reached) in [
            ("split", audit.split > 0),
            ("split_across_workers", audit.split_across_workers > 0),
            ("key_in_several_batches", audit.keys_in_several_batches > 0),
        ] {
            let count = self.drain_cases.entry(name).or_insert(0);
            *count += usize::from(reached);
        }
    }

    /// Recursively tallies `expr`'s own shape and, for `BinaryOp`, its
    /// operator (and for `FunctionCall`, its function name), then descends
    /// into its subexpressions, returning `expr`'s own tree depth (a leaf is
    /// depth 1) so [`Self::max_expr_depth`] can track the deepest tree seen
    /// across every call.
    fn record_expr(&mut self, expr: &Expr) -> usize {
        let depth = match expr {
            Expr::Column(_) => {
                self.expr_shapes.insert("Column");
                1
            }
            Expr::NumberLiteral(_) => {
                self.expr_shapes.insert("NumberLiteral");
                1
            }
            Expr::StringLiteral(_) => {
                self.expr_shapes.insert("StringLiteral");
                1
            }
            Expr::TypedLiteral { .. } => {
                self.expr_shapes.insert("TypedLiteral");
                1
            }
            Expr::RelationshipPath { .. } => {
                self.expr_shapes.insert("RelationshipPath");
                1
            }
            Expr::BinaryOp { op, lhs, rhs } => {
                self.expr_shapes.insert("BinaryOp");
                self.operators.insert(operator_name(*op));
                let lhs_depth = self.record_expr(lhs);
                let rhs_depth = self.record_expr(rhs);
                1 + lhs_depth.max(rhs_depth)
            }
            Expr::FunctionCall { name, args } => {
                self.expr_shapes.insert("FunctionCall");
                self.functions.insert(function_name(name));
                let max_arg_depth = args
                    .iter()
                    .map(|arg| self.record_expr(arg))
                    .max()
                    .unwrap_or(0);
                1 + max_arg_depth
            }
        };
        self.max_expr_depth = self.max_expr_depth.max(depth);
        depth
    }
}

/// Adds every aggregate call in `expr` to `out` as a
/// [`Coverage::aggregate_call_cases`] key.
fn collect_aggregate_calls(
    expr: &Expr,
    key_space: &'static str,
    out: &mut HashSet<(&'static str, &'static str, &'static str)>,
) {
    match expr {
        Expr::FunctionCall { name, args } => {
            if registry::AGGREGATE_FUNCTIONS.contains(&name.as_str()) {
                let argument = match args.as_slice() {
                    [] => "*",
                    [Expr::Column(_)] => "column",
                    [Expr::RelationshipPath { .. }] => "rel",
                    _ => "expr",
                };
                out.insert((key_space, function_name(name), argument));
            }
            for arg in args {
                collect_aggregate_calls(arg, key_space, out);
            }
        }
        Expr::BinaryOp { lhs, rhs, .. } => {
            collect_aggregate_calls(lhs, key_space, out);
            collect_aggregate_calls(rhs, key_space, out);
        }
        Expr::Column(_)
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::TypedLiteral { .. }
        | Expr::RelationshipPath { .. } => {}
    }
}

/// One calculated field's read of a relationship: which declaration it
/// names (`None` if the name resolves to nothing), which to-side column it
/// reads, and which of the three engine-supported shapes it is.
struct RelationshipRead<'a> {
    rel: Option<&'a Relationship>,
    column: &'a str,
    shape: &'static str,
}

/// Walks `expr` collecting every relationship *reference* it reads, with its
/// [`Coverage::relationship_shapes`] name (issue #34). `wrapped` tracks
/// whether the current subexpression sits directly under an aggregate call,
/// since that, together with the relationship's cardinality and the
/// definition's key-space, is exactly what distinguishes the three legal
/// shapes from each other.
fn collect_relationship_reads<'a>(
    expr: &'a Expr,
    def: &TransformDef,
    by_name: &HashMap<&str, &'a Relationship>,
    wrapped: bool,
    out: &mut Vec<RelationshipRead<'a>>,
) {
    match expr {
        Expr::RelationshipPath { rel, column } => {
            let rel = by_name.get(rel.as_str()).copied();
            let in_aggregate_def = matches!(def.key_space, KeySpace::Aggregate { .. });
            let shape = match (rel.map(|r| r.cardinality), wrapped, in_aggregate_def) {
                (Some(Cardinality::ToOne), false, false) => "to_one_bare",
                (Some(Cardinality::ToMany), true, false) => "to_many_in_aggregate",
                (Some(Cardinality::ToOne), true, true) => "to_one_in_aggregate_def",
                _ => "other",
            };
            out.push(RelationshipRead { rel, column, shape });
        }
        Expr::Column(_)
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::TypedLiteral { .. } => {}
        Expr::BinaryOp { lhs, rhs, .. } => {
            collect_relationship_reads(lhs, def, by_name, false, out);
            collect_relationship_reads(rhs, def, by_name, false, out);
        }
        Expr::FunctionCall { args, .. } => {
            // Only a *single*-argument call can be the aggregate-over-a-
            // path shape (ADR-0006: "wrapped in exactly one aggregate
            // function"), so a path buried among several arguments of a
            // scalar call is deliberately not counted as wrapped.
            let wrapped = args.len() == 1;
            for arg in args {
                collect_relationship_reads(arg, def, by_name, wrapped, out);
            }
        }
    }
}

/// The relationship reverse-propagation paths
/// [`Coverage::relationship_path_cases`] tallies (issue #505). "Parent" is
/// the relationship's to-side row, whichever cardinality: it is the row whose
/// change the engine must propagate *back* into from-side targets. A parent
/// is "referenced" when some live from-side row joins it at that moment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RelPath {
    /// A referenced parent's read column moves between `NULL` and
    /// non-`NULL`, either way. The sharpest reverse-path edge for
    /// `COUNT(<rel>.<col>)` (the row stops or starts counting) and for
    /// `MIN`/`MAX`/`AVG` (a value leaves or joins the fold).
    ParentNullFlip,
    /// A referenced parent's read column changes between two different
    /// non-`NULL` values.
    ParentValueChange,
    /// A parent row appears while a live from-side row already joins it.
    ParentInsert,
    /// A referenced parent row is deleted.
    ParentDelete,
    /// A referenced parent row is removed by a `TRUNCATE` of its table.
    ParentTruncate,
    /// A from-side row is written after the parent it joins (before or after
    /// the write) was deleted, truncated, or had its read column changed
    /// earlier in the same program. Only reachable when a program interleaves
    /// tables. The serial tiers quiesce after every op, so there the parent
    /// change has settled before the from-side write lands; only a tier that
    /// applies several ops before quiescing (the bursty concurrent tier,
    /// #557) can have both in the engine's pipeline at once, the lost-update
    /// shape of #582.
    FromSideAfterParentChange,
}

impl RelPath {
    pub const ALL: [RelPath; 6] = [
        RelPath::ParentNullFlip,
        RelPath::ParentValueChange,
        RelPath::ParentInsert,
        RelPath::ParentDelete,
        RelPath::ParentTruncate,
        RelPath::FromSideAfterParentChange,
    ];

    /// The name [`Coverage::relationship_path_cases`] keys on.
    pub fn name(self) -> &'static str {
        match self {
            RelPath::ParentNullFlip => "parent_null_flip",
            RelPath::ParentValueChange => "parent_value_change",
            RelPath::ParentInsert => "parent_insert",
            RelPath::ParentDelete => "parent_delete",
            RelPath::ParentTruncate => "parent_truncate",
            RelPath::FromSideAfterParentChange => "from_side_after_parent_change",
        }
    }
}

/// A source row as the replay in [`relationship_path_events`] holds it:
/// column name to rendered value (`None` is SQL `NULL`, and so is a column
/// the inserting op didn't name).
type SimRow = HashMap<String, Option<String>>;

/// Applies `op` to `rows` (one table, keyed by rendered pk) exactly as
/// Postgres would. It ignores `op.expect()`, so the replay's accounting
/// can't be skewed by a generator bug in the expected outcomes.
fn apply_to_rows(op: &Op, pk_col: &str, rows: &mut HashMap<String, SimRow>) {
    let pk_of = |row: &[(String, Option<String>)]| {
        row.iter()
            .find(|(c, _)| c == pk_col)
            .and_then(|(_, v)| v.clone())
    };
    let to_row = |row: &[(String, Option<String>)]| row.iter().cloned().collect::<SimRow>();
    match op {
        Op::Insert { row, .. } => {
            if let Some(pk) = pk_of(row)
                && !rows.contains_key(&pk)
            {
                rows.insert(pk, to_row(row));
            }
        }
        Op::BulkInsert { rows: new_rows, .. } => {
            let pks: Vec<Option<String>> = new_rows.iter().map(|r| pk_of(r)).collect();
            let distinct: HashSet<&Option<String>> = pks.iter().collect();
            let atomic_ok = distinct.len() == pks.len()
                && pks
                    .iter()
                    .all(|pk| pk.as_ref().is_some_and(|pk| !rows.contains_key(pk)));
            if atomic_ok {
                for (pk, row) in pks.into_iter().zip(new_rows) {
                    rows.insert(pk.expect("checked above"), to_row(row));
                }
            }
        }
        Op::Update { pk, changes, .. } => {
            if let Some(row) = rows.get_mut(pk) {
                for (column, value) in changes {
                    row.insert(column.clone(), value.clone());
                }
            }
        }
        Op::Delete { pk, .. } => {
            rows.remove(pk);
        }
        Op::Truncate { .. } => rows.clear(),
    }
}

fn op_table_name(op: &Op) -> &str {
    match op {
        Op::Insert { table, .. }
        | Op::Update { table, .. }
        | Op::Delete { table, .. }
        | Op::Truncate { table, .. }
        | Op::BulkInsert { table, .. } => table,
    }
}

/// Replays `program.ops` against in-memory copies of its source tables and
/// returns every `(shape, path)` pair (see [`RelPath`]) the op stream drives
/// for any of `reads` (issue #505).
fn relationship_path_events(
    program: &Program,
    reads: &[RelationshipRead<'_>],
) -> HashSet<(&'static str, &'static str)> {
    let mut out = HashSet::new();
    let reads: Vec<(&Relationship, &str, &'static str)> = reads
        .iter()
        .filter_map(|r| Some((r.rel?, r.column, r.shape)))
        .collect();
    if reads.is_empty() {
        return out;
    }
    let pk_cols: HashMap<&str, &str> = program
        .tables
        .iter()
        .map(|t| (t.name.as_str(), t.pk_col.as_str()))
        .collect();
    let mut state: HashMap<&str, HashMap<String, SimRow>> = HashMap::new();
    // `(relationship name, join value)` of every parent changed so far.
    let mut changed_parents: HashSet<(&str, String)> = HashSet::new();
    let value = |row: &SimRow, column: &str| row.get(column).cloned().flatten();

    for op in &program.ops {
        let table = op_table_name(op);
        let Some(pk_col) = pk_cols.get(table) else {
            continue;
        };
        let before = state.get(table).cloned().unwrap_or_default();
        apply_to_rows(op, pk_col, state.entry(table).or_default());
        let after = &state[table];

        let mut pks: Vec<&String> = before.keys().chain(after.keys()).collect();
        pks.sort_unstable();
        pks.dedup();
        for pk in pks {
            let old = before.get(pk);
            let new = after.get(pk);
            let touched = old != new || (matches!(op, Op::Update { pk: p, .. } if p == pk));
            if !touched {
                continue;
            }
            for &(rel, column, shape) in &reads {
                if rel.to_table == table {
                    let referenced = |row: &SimRow| {
                        value(row, &rel.to_col).is_some_and(|key| {
                            state.get(rel.from_table.as_str()).is_some_and(|from| {
                                from.values()
                                    .any(|f| value(f, &rel.from_col).as_ref() == Some(&key))
                            })
                        })
                    };
                    let path = match (old, new) {
                        (None, Some(n)) => referenced(n).then_some(RelPath::ParentInsert),
                        (Some(o), None) if matches!(op, Op::Truncate { .. }) => {
                            referenced(o).then_some(RelPath::ParentTruncate)
                        }
                        (Some(o), None) => referenced(o).then_some(RelPath::ParentDelete),
                        (Some(o), Some(n)) => {
                            let (was, is) = (value(o, column), value(n, column));
                            if was == is || !referenced(n) {
                                None
                            } else if was.is_none() != is.is_none() {
                                Some(RelPath::ParentNullFlip)
                            } else {
                                Some(RelPath::ParentValueChange)
                            }
                        }
                        (None, None) => None,
                    };
                    if let Some(path) = path {
                        out.insert((shape, path.name()));
                    }
                    // Any mutation of an existing parent counts for the
                    // from-side path, referenced or not: a from-side row that
                    // starts joining it afterward must still see the new
                    // state. A parent *insert* doesn't: a from-side row
                    // arriving after it is the ordinary forward read.
                    let changed = match (old, new) {
                        (Some(o), Some(n)) => value(o, column) != value(n, column),
                        (Some(_), None) => true,
                        (None, _) => false,
                    };
                    if changed {
                        for row in old.into_iter().chain(new) {
                            if let Some(key) = value(row, &rel.to_col) {
                                changed_parents.insert((rel.name.as_str(), key));
                            }
                        }
                    }
                }
                if rel.from_table == table {
                    let joins_a_changed_parent = old.into_iter().chain(new).any(|row| {
                        value(row, &rel.from_col)
                            .is_some_and(|key| changed_parents.contains(&(rel.name.as_str(), key)))
                    });
                    if joins_a_changed_parent {
                        out.insert((shape, RelPath::FromSideAfterParentChange.name()));
                    }
                }
            }
        }
    }
    out
}

/// A burst shape the concurrent tier exists to reach (issue #557). A case
/// has a shape when at least one of its bursts does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConcurrentShape {
    /// The burst is split across two or more lanes, so its ops race.
    ConcurrentLanes,
    /// One row is written at least [`HOT_WRITES`] times in the burst.
    HotKey,
    /// One aggregate group is written at least [`HOT_WRITES`] times, by two
    /// or more rows, in the burst: rows the engine may hash to different
    /// buckets of a split batch, and so drain on different workers.
    HotGroup,
    /// The burst carries at least `SPLIT_THRESHOLD_ROWS` source changes and
    /// no whole-table op (a `TRUNCATE` batch always seals unsplit), so it
    /// can seal into a split batch if the harness outruns the seal cadence.
    SplitSized,
    /// A group that has never had a member gets its first one, after the
    /// first burst.
    NewGroup,
    /// A group loses its last member and gains one again within the burst.
    GroupRefill,
    /// A `request_backfill` fires mid-burst on a table the burst writes
    /// both before and after it (issue #557 part 2): the re-read overlaps
    /// in-flight CDC for the same keys. This and the three shapes below
    /// place an action by the burst's program order; the lanes race, so the
    /// engine sees roughly that.
    MidBurstBackfill,
    /// A whole-transform `RESUME` fires on a definition whose source the
    /// burst writes both before and after it.
    MidBurstResume,
    /// A column `RESUME` fires on a definition whose source the burst writes
    /// both before and after it.
    MidBurstColumnResume,
    /// A definition installs on a source the burst writes both before and
    /// after the install, so its build overlaps CDC (#625's shape).
    MidBurstInstall,
    /// A relationship's to-side table is truncated in a burst that also
    /// writes its from-side table.
    ToSideTruncate,
    /// A relationship's to-side row is updated in a burst that also writes
    /// its from-side table (#505).
    ParentUpdate,
    /// A row a 1-1 definition reads is written twice or more in a burst
    /// that also has a hot key, and its last write in the whole program
    /// falls in the burst's middle fifth to four fifths (#557 part 3b): a
    /// key that goes quiet while the rest of the burst still writes, so an
    /// out-of-order 1-1 write to it is never healed by a later change.
    CoolingKey,
}

/// How many writes to one row, or to one group, make it hot
/// ([`ConcurrentShape::HotKey`], [`ConcurrentShape::HotGroup`]).
pub const HOT_WRITES: usize = 8;

impl ConcurrentShape {
    pub const ALL: [ConcurrentShape; 13] = [
        ConcurrentShape::ConcurrentLanes,
        ConcurrentShape::HotKey,
        ConcurrentShape::HotGroup,
        ConcurrentShape::SplitSized,
        ConcurrentShape::NewGroup,
        ConcurrentShape::GroupRefill,
        ConcurrentShape::MidBurstBackfill,
        ConcurrentShape::MidBurstResume,
        ConcurrentShape::MidBurstColumnResume,
        ConcurrentShape::MidBurstInstall,
        ConcurrentShape::ToSideTruncate,
        ConcurrentShape::ParentUpdate,
        ConcurrentShape::CoolingKey,
    ];

    /// The shapes part 1 of #557 added, which every hot-key case offers.
    /// The rest come from mid-burst actions and to-side ops, which only
    /// some cases draw.
    pub const HOT_KEY: [ConcurrentShape; 6] = [
        ConcurrentShape::ConcurrentLanes,
        ConcurrentShape::HotKey,
        ConcurrentShape::HotGroup,
        ConcurrentShape::SplitSized,
        ConcurrentShape::NewGroup,
        ConcurrentShape::GroupRefill,
    ];

    /// The name [`Coverage::concurrent_shape_cases`] keys on.
    pub fn name(self) -> &'static str {
        match self {
            ConcurrentShape::ConcurrentLanes => "concurrent_lanes",
            ConcurrentShape::HotKey => "hot_key",
            ConcurrentShape::HotGroup => "hot_group",
            ConcurrentShape::SplitSized => "split_sized",
            ConcurrentShape::NewGroup => "new_group",
            ConcurrentShape::GroupRefill => "group_refill",
            ConcurrentShape::MidBurstBackfill => "mid_burst_backfill",
            ConcurrentShape::MidBurstResume => "mid_burst_resume",
            ConcurrentShape::MidBurstColumnResume => "mid_burst_column_resume",
            ConcurrentShape::MidBurstInstall => "mid_burst_install",
            ConcurrentShape::ToSideTruncate => "to_side_truncate",
            ConcurrentShape::ParentUpdate => "parent_update",
            ConcurrentShape::CoolingKey => "cooling_key",
        }
    }
}

/// One aggregate group, as [`concurrent_shapes`] tracks it: the source
/// table, its grouping column, and the group's value (`None` is the `NULL`
/// group).
type Group<'a> = (&'a str, &'a str, Option<String>);

/// Replays `program.ops` burst by burst, in program order, and returns every
/// [`ConcurrentShape`] some burst reaches (issue #557). Groups are those of
/// every `GROUP BY` definition's single-column key.
fn concurrent_shapes(program: &Program, plan: &ConcurrentPlan) -> HashSet<ConcurrentShape> {
    let mut out = HashSet::new();
    let pk_cols: HashMap<&str, &str> = program
        .tables
        .iter()
        .map(|t| (t.name.as_str(), t.pk_col.as_str()))
        .collect();
    let mut groupings: Vec<(&str, &str)> = Vec::new();
    for def in &program.defs {
        if let KeySpace::Aggregate { group_by } = &def.key_space {
            for key in group_by {
                let grouping = (def.source.as_str(), key.target_column_name());
                if !groupings.contains(&grouping) {
                    groupings.push(grouping);
                }
            }
        }
    }
    let value = |row: &SimRow, column: &str| row.get(column).cloned().flatten();
    // Each row's last single-row write, and the tables a 1-1 definition
    // reads, for `CoolingKey`.
    let mut last_write: HashMap<(&str, String), usize> = HashMap::new();
    for (index, op) in program.ops.iter().enumerate() {
        let (table, pk) = match op {
            Op::Insert { table, row, .. } => {
                let pk_col = pk_cols.get(table.as_str()).copied().unwrap_or_default();
                let pk = row
                    .iter()
                    .find(|(c, _)| c == pk_col)
                    .and_then(|(_, v)| v.clone());
                (table, pk)
            }
            Op::Update { table, pk, .. } | Op::Delete { table, pk, .. } => {
                (table, Some(pk.clone()))
            }
            Op::Truncate { .. } | Op::BulkInsert { .. } => continue,
        };
        if let Some(pk) = pk {
            last_write.insert((table.as_str(), pk), index);
        }
    }
    let one_to_one: HashSet<&str> = program
        .defs
        .iter()
        .filter(|d| d.key_space == KeySpace::OneToOne)
        .map(|d| d.source.as_str())
        .collect();
    let mut state: HashMap<&str, HashMap<String, SimRow>> = HashMap::new();
    let mut sizes: HashMap<Group<'_>, usize> = HashMap::new();
    let mut ever: HashSet<Group<'_>> = HashSet::new();

    for (burst_index, burst) in plan.bursts.iter().enumerate() {
        if burst.lanes.len() >= 2 {
            out.insert(ConcurrentShape::ConcurrentLanes);
        }
        let mut row_writes: HashMap<(&str, String), usize> = HashMap::new();
        let mut group_writes: HashMap<Group<'_>, (usize, HashSet<String>)> = HashMap::new();
        let mut emptied: HashSet<Group<'_>> = HashSet::new();
        let (mut changes, mut whole_table) = (0usize, false);
        let mut cooling = false;
        let ops = burst.ops();

        for (position, &index) in ops.iter().enumerate() {
            let op = &program.ops[index];
            let table = op_table_name(op);
            let Some(pk_col) = pk_cols.get(table) else {
                continue;
            };
            whole_table |= matches!(op, Op::Truncate { .. } | Op::BulkInsert { .. });
            if matches!(op.expect(), OpOutcome::Succeeds) {
                changes += match op {
                    Op::BulkInsert { rows, .. } => rows.len(),
                    _ => 1,
                };
            }
            // Only the rows `op` can reach are copied: a hot-key case replays
            // thousands of ops, nearly all of them on one row.
            let rows = state.entry(table).or_default();
            let mut pks: Vec<String> = match op {
                Op::Insert { row, .. } => row
                    .iter()
                    .filter(|(c, _)| c == pk_col)
                    .filter_map(|(_, v)| v.clone())
                    .collect(),
                Op::BulkInsert { rows: new_rows, .. } => new_rows
                    .iter()
                    .flat_map(|row| row.iter().filter(|(c, _)| c == pk_col))
                    .filter_map(|(_, v)| v.clone())
                    .collect(),
                Op::Update { pk, .. } | Op::Delete { pk, .. } => vec![pk.clone()],
                Op::Truncate { .. } => rows.keys().cloned().collect(),
            };
            pks.sort_unstable();
            pks.dedup();
            let before: HashMap<String, SimRow> = pks
                .iter()
                .filter_map(|pk| Some((pk.clone(), rows.get(pk)?.clone())))
                .collect();
            apply_to_rows(op, pk_col, rows);
            let after = &state[table];

            for pk in &pks {
                let (old, new) = (before.get(pk), after.get(pk));
                let touched = old != new
                    || (new.is_some() && matches!(op, Op::Update { pk: p, .. } if p == pk));
                if !touched {
                    continue;
                }
                let writes = row_writes.entry((table, pk.clone())).or_insert(0);
                *writes += 1;
                cooling |= *writes >= 2
                    && one_to_one.contains(table)
                    && last_write.get(&(table, pk.clone())) == Some(&index)
                    && position * 5 >= ops.len()
                    && position * 5 <= ops.len() * 4;
                for &(group_table, column) in &groupings {
                    if group_table != table {
                        continue;
                    }
                    let was = old.map(|row| (table, column, value(row, column)));
                    let is = new.map(|row| (table, column, value(row, column)));
                    for group in was
                        .iter()
                        .chain(is.iter().filter(|g| Some(*g) != was.as_ref()))
                    {
                        let writes = group_writes.entry(group.clone()).or_default();
                        writes.0 += 1;
                        writes.1.insert(pk.clone());
                    }
                    if was == is {
                        continue;
                    }
                    if let Some(group) = was {
                        let size = sizes.entry(group.clone()).or_insert(0);
                        *size = size.saturating_sub(1);
                        if *size == 0 {
                            emptied.insert(group);
                        }
                    }
                    if let Some(group) = is {
                        let size = sizes.entry(group.clone()).or_insert(0);
                        *size += 1;
                        if *size == 1 {
                            if emptied.contains(&group) {
                                out.insert(ConcurrentShape::GroupRefill);
                            }
                            if burst_index > 0 && !ever.contains(&group) {
                                out.insert(ConcurrentShape::NewGroup);
                            }
                            ever.insert(group);
                        }
                    }
                }
            }
        }

        out.extend(burst_action_shapes(program, &burst.ops(), &burst.actions));

        if row_writes.values().any(|&n| n >= HOT_WRITES) {
            out.insert(ConcurrentShape::HotKey);
            if cooling {
                out.insert(ConcurrentShape::CoolingKey);
            }
        }
        if group_writes
            .values()
            .any(|(n, rows)| *n >= HOT_WRITES && rows.len() >= 2)
        {
            out.insert(ConcurrentShape::HotGroup);
        }
        if changes >= SPLIT_THRESHOLD_ROWS && !whole_table {
            out.insert(ConcurrentShape::SplitSized);
        }
    }
    out
}

/// The mid-burst action and to-side shapes one burst reaches (issue #557
/// part 2). `ops` is the burst's op indices in program order; an action
/// `after` `k` ops fires between `ops[k - 1]` and `ops[k]`.
fn burst_action_shapes(
    program: &Program,
    ops: &[usize],
    actions: &[crate::model::TimedAction],
) -> HashSet<ConcurrentShape> {
    let mut out = HashSet::new();
    let writes = |table: &str, range: std::ops::Range<usize>| {
        ops[range]
            .iter()
            .any(|&i| op_table_name(&program.ops[i]) == table)
    };
    let overlaps = |table: &str, at: usize| writes(table, 0..at) && writes(table, at..ops.len());
    let source_of = |target: &str| {
        program
            .defs
            .iter()
            .find(|d| d.target == target)
            .map(|d| d.source.as_str())
    };
    for timed in actions {
        let (shape, table) = match &timed.action {
            BurstAction::RequestBackfill { table } => {
                (ConcurrentShape::MidBurstBackfill, Some(table.as_str()))
            }
            BurstAction::Resume {
                target,
                column: None,
            } => (ConcurrentShape::MidBurstResume, source_of(target)),
            BurstAction::Resume {
                target,
                column: Some(_),
            } => (ConcurrentShape::MidBurstColumnResume, source_of(target)),
            BurstAction::Install { def } => (
                ConcurrentShape::MidBurstInstall,
                program.defs.get(*def).map(|d| d.source.as_str()),
            ),
            BurstAction::Pause { .. } => continue,
        };
        if table.is_some_and(|table| overlaps(table, timed.after.min(ops.len()))) {
            out.insert(shape);
        }
    }
    for rel in &program.relationships {
        if !writes(&rel.from_table, 0..ops.len()) {
            continue;
        }
        for &i in ops {
            let op = &program.ops[i];
            if op_table_name(op) != rel.to_table || !matches!(op.expect(), OpOutcome::Succeeds) {
                continue;
            }
            match op {
                Op::Truncate { .. } => out.insert(ConcurrentShape::ToSideTruncate),
                Op::Update { .. } => out.insert(ConcurrentShape::ParentUpdate),
                _ => false,
            };
        }
    }
    out
}

fn op_kind(op: &Op) -> &'static str {
    match op {
        Op::Insert { .. } => "Insert",
        Op::Update { .. } => "Update",
        Op::Delete { .. } => "Delete",
        Op::Truncate { .. } => "Truncate",
        Op::BulkInsert { .. } => "BulkInsert",
    }
}

fn op_outcome_name(outcome: &OpOutcome) -> &'static str {
    match outcome {
        OpOutcome::Succeeds => "Succeeds",
        OpOutcome::Fails => "Fails",
        OpOutcome::AffectsNoRows => "AffectsNoRows",
        OpOutcome::AnyOf(_) => "AnyOf",
    }
}

fn key_space_name(key_space: &KeySpace) -> &'static str {
    match key_space {
        KeySpace::OneToOne => "OneToOne",
        KeySpace::Aggregate { .. } => "Aggregate",
    }
}

fn value_type_name(value_type: ValueType) -> &'static str {
    match value_type {
        ValueType::Numeric => "numeric",
        ValueType::Integer(width) => width.pg_name(),
        ValueType::Float(width) => width.pg_name(),
        ValueType::Text => "text",
        ValueType::Boolean => "boolean",
        ValueType::Uuid => "uuid",
        ValueType::Other(pg_type) => pg_type.name(),
    }
}

fn cardinality_name(cardinality: Cardinality) -> &'static str {
    match cardinality {
        Cardinality::ToOne => "to_one",
        Cardinality::ToMany => "to_many",
    }
}

fn operator_name(op: Operator) -> &'static str {
    match op {
        Operator::Add => "Add",
        Operator::GreaterThan => "GreaterThan",
    }
}

/// Maps a [`Expr::FunctionCall`] name to a stable `&'static str` for
/// [`Coverage::functions`], matching the canonical uppercased names
/// `trellis::dev::defs::registry::FUNCTIONS`/`AGGREGATE_FUNCTION_SPECS` already use
/// (and the generator only ever builds). A name outside that fixed set
/// collapses to `"Other"` rather than leaking an arbitrary caller-owned
/// `String` into a `HashSet<&'static str>`.
fn function_name(name: &str) -> &'static str {
    match name {
        "STRPOS" => "STRPOS",
        "OCTET_LENGTH" => "OCTET_LENGTH",
        "CHAR_LENGTH" => "CHAR_LENGTH",
        "REGEXP_COUNT" => "REGEXP_COUNT",
        "COALESCE" => "COALESCE",
        "SUM" => "SUM",
        "MIN" => "MIN",
        "MAX" => "MAX",
        "AVG" => "AVG",
        "COUNT" => "COUNT",
        "BOOL_AND" => "BOOL_AND",
        "BOOL_OR" => "BOOL_OR",
        _ => "Other",
    }
}

/// Sorted `(name, count)` pairs, since `HashMap` iteration order isn't
/// stable across runs and this is meant to be read/diffed by a human.
fn sorted_counts(map: &HashMap<&'static str, usize>) -> Vec<(&'static str, usize)> {
    let mut entries: Vec<(&'static str, usize)> = map.iter().map(|(k, v)| (*k, *v)).collect();
    entries.sort_unstable_by_key(|(name, _)| *name);
    entries
}

/// Sorted names, for the same reason as [`sorted_counts`].
fn sorted_names(set: &HashSet<&'static str>) -> Vec<&'static str> {
    let mut names: Vec<&'static str> = set.iter().copied().collect();
    names.sort_unstable();
    names
}

impl fmt::Display for Coverage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "cases: {}", self.cases)?;

        write!(f, "ops_by_kind:")?;
        for (name, count) in sorted_counts(&self.ops_by_kind) {
            write!(f, " {name}={count}")?;
        }
        writeln!(f)?;

        write!(f, "ops_by_outcome:")?;
        for (name, count) in sorted_counts(&self.ops_by_outcome) {
            write!(f, " {name}={count}")?;
        }
        writeln!(f)?;

        writeln!(
            f,
            "expr_shapes: {}",
            sorted_names(&self.expr_shapes).join(", ")
        )?;
        writeln!(f, "operators: {}", sorted_names(&self.operators).join(", "))?;
        writeln!(f, "functions: {}", sorted_names(&self.functions).join(", "))?;
        writeln!(
            f,
            "types_exercised: {}",
            sorted_names(&self.types_exercised).join(", ")
        )?;

        write!(f, "key_spaces:")?;
        for (name, count) in sorted_counts(&self.key_spaces) {
            write!(f, " {name}={count}")?;
        }
        writeln!(f)?;

        writeln!(f, "max_expr_depth: {}", self.max_expr_depth)?;
        writeln!(
            f,
            "mid_stream_def_installs: {}",
            self.mid_stream_def_installs
        )?;
        writeln!(f, "client_restarts: {}", self.client_restarts)?;
        writeln!(f, "client_scale_outs: {}", self.client_scale_outs)?;
        writeln!(f, "max_bulk_insert_rows: {}", self.max_bulk_insert_rows)?;

        writeln!(f, "relationships_declared: {}", self.relationships_declared)?;
        write!(f, "relationship_cardinalities:")?;
        for (name, count) in sorted_counts(&self.relationship_cardinalities) {
            write!(f, " {name}={count}")?;
        }
        writeln!(f)?;
        write!(f, "relationship_shapes:")?;
        for (name, count) in sorted_counts(&self.relationship_shapes) {
            write!(f, " {name}={count}")?;
        }
        writeln!(f)?;
        write!(f, "aggregate_call_cases:")?;
        let mut calls: Vec<_> = self.aggregate_call_cases.iter().collect();
        calls.sort_unstable();
        for ((key_space, function, argument), count) in calls {
            write!(f, " {key_space}:{function}({argument})={count}")?;
        }
        writeln!(f)?;
        write!(f, "relationship_path_cases:")?;
        let mut paths: Vec<_> = self.relationship_path_cases.iter().collect();
        paths.sort_unstable();
        for ((shape, path), count) in paths {
            write!(f, " {shape}:{path}={count}")?;
        }
        writeln!(f)?;
        writeln!(f, "concurrent_bursts: {}", self.concurrent_bursts)?;
        write!(f, "concurrent_shape_cases:")?;
        for (name, count) in sorted_counts(&self.concurrent_shape_cases) {
            write!(f, " {name}={count}")?;
        }
        writeln!(f)?;
        write!(f, "concurrent_actions:")?;
        for (name, count) in sorted_counts(&self.concurrent_actions) {
            write!(f, " {name}={count}")?;
        }
        writeln!(f)?;
        let drain = &self.drain;
        writeln!(
            f,
            "drain: sealed={} split={} split_across_workers={} max_workers_per_batch={} \
             max_rows_per_batch={} keys_in_several_batches={}",
            drain.sealed,
            drain.split,
            drain.split_across_workers,
            drain.max_workers_per_batch,
            drain.max_rows_per_batch,
            drain.keys_in_several_batches
        )?;
        write!(f, "drain_cases:")?;
        for (name, count) in sorted_counts(&self.drain_cases) {
            write!(f, " {name}={count}")?;
        }
        writeln!(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate::{
        AggregateFn, DefShape, Mutate, RelFieldKind, RelFieldSpec, TableSpec, build_program,
        build_program_multi_with_relationships, build_program_multi_with_shapes, interleave_tables,
    };
    use crate::model::Burst;

    /// Issue #557: four rows grouped by grain (`0`, `0`, `1`, `1`) under a
    /// `COUNT(*)` `GROUP BY`. After the seeds, one burst moves both rows of
    /// group `0` into the never-used group `5` and one back (group `0`
    /// empties and refills), then writes group `1`'s rows twelve times,
    /// eight of them to pk 3.
    fn grouped_program() -> Program {
        let mut spec = TableSpec::numeric_only(vec![(Some(1), None); 4], Vec::new());
        spec.grain_values = ["0", "0", "1", "1"]
            .iter()
            .map(|g| Some(g.to_string()))
            .collect();
        spec.mutates = vec![
            Mutate::MoveGroup {
                pk: 1,
                grain: Some(5),
            },
            Mutate::MoveGroup {
                pk: 2,
                grain: Some(5),
            },
            Mutate::MoveGroup {
                pk: 1,
                grain: Some(0),
            },
        ];
        for i in 0..12 {
            spec.mutates.push(Mutate::Update {
                pk: if i % 3 == 2 { 4 } else { 3 },
                c1: Some(i),
                c2: None,
            });
        }
        build_program_multi_with_shapes(
            &[spec],
            &[(
                0,
                DefShape::Aggregate {
                    functions: vec![AggregateFn::Count],
                },
            )],
        )
    }

    fn shapes(program: &Program, plan: &ConcurrentPlan) -> Vec<&'static str> {
        let mut names: Vec<&'static str> = concurrent_shapes(program, plan)
            .into_iter()
            .map(ConcurrentShape::name)
            .collect();
        names.sort_unstable();
        names
    }

    #[test]
    fn a_hot_burst_after_the_seeds_reaches_every_group_shape() {
        let program = grouped_program();
        let rest: Vec<usize> = (4..program.ops.len()).collect();
        let (even, odd) = rest.iter().partition(|&&i| i % 2 == 0);
        let plan = ConcurrentPlan {
            bursts: vec![
                Burst {
                    lanes: vec![vec![0, 1, 2, 3]],
                    ..Default::default()
                },
                Burst {
                    lanes: vec![even, odd],
                    ..Default::default()
                },
            ],
        };
        assert_eq!(
            shapes(&program, &plan),
            vec![
                "concurrent_lanes",
                "group_refill",
                "hot_group",
                "hot_key",
                "new_group"
            ]
        );
    }

    /// The same ops in one single-lane burst: no race, and a group that
    /// first appears in the first burst isn't a new-group burst, since the
    /// seeds' groups appear there too.
    #[test]
    fn one_serial_burst_is_neither_concurrent_nor_a_new_group_burst() {
        let program = grouped_program();
        let plan = ConcurrentPlan {
            bursts: vec![Burst {
                lanes: vec![(0..program.ops.len()).collect()],
                ..Default::default()
            }],
        };
        assert_eq!(
            shapes(&program, &plan),
            vec!["group_refill", "hot_group", "hot_key"]
        );
    }

    /// A burst of `SPLIT_THRESHOLD_ROWS` inserts is split-sized; a `TRUNCATE`
    /// in the same burst makes it seal unsplit, so it no longer is.
    #[test]
    fn split_sized_needs_enough_changes_and_no_truncate() {
        let seeds = vec![(Some(1), None); SPLIT_THRESHOLD_ROWS];
        let one_burst = |program: &Program| ConcurrentPlan {
            bursts: vec![Burst {
                lanes: vec![(0..program.ops.len()).collect()],
                ..Default::default()
            }],
        };
        let program = build_program(&seeds, &[]);
        assert!(shapes(&program, &one_burst(&program)).contains(&"split_sized"));
        let program = build_program(&seeds[1..], &[]);
        assert!(!shapes(&program, &one_burst(&program)).contains(&"split_sized"));
        let program = build_program(&seeds, &[Mutate::Truncate]);
        assert!(!shapes(&program, &one_burst(&program)).contains(&"split_sized"));
    }

    #[test]
    fn record_drain_adds_up_and_counts_cases() {
        let mut coverage = Coverage::new();
        let audit = DrainAudit {
            sealed: 3,
            split: 1,
            split_across_workers: 1,
            max_workers_per_batch: 4,
            max_rows_per_batch: 300,
            keys_in_several_batches: 0,
        };
        coverage.record_drain(&audit);
        coverage.record_drain(&DrainAudit {
            sealed: 2,
            max_workers_per_batch: 1,
            ..DrainAudit::default()
        });
        assert_eq!(coverage.drain.sealed, 5);
        assert_eq!(coverage.drain.max_workers_per_batch, 4);
        assert_eq!(coverage.drain_cases["split_across_workers"], 1);
        assert_eq!(coverage.drain_cases["key_in_several_batches"], 0);
    }

    /// A from-side row joined to a parent whose `c1` is toggled to `NULL`,
    /// read through a bare to-one enrichment (issue #505).
    fn toggled_parent_program() -> Program {
        let mut from_side = TableSpec::numeric_only(vec![(Some(1), None)], Vec::new());
        from_side.rel_fk_values = vec![Some("k1".to_string())];
        let parent = TableSpec::numeric_only(
            vec![(Some(5), None)],
            vec![Mutate::ToggleNull { pk: 1, value: 0 }],
        );
        build_program_multi_with_relationships(
            &[from_side, parent],
            &[(0, DefShape::OneToOne)],
            &[None],
            &[Some(RelFieldSpec {
                to_table: 1,
                kind: RelFieldKind::ToOneBare,
            })],
        )
    }

    fn paths(program: &Program) -> Vec<&'static str> {
        let mut coverage = Coverage::new();
        coverage.record_program(program);
        let mut paths: Vec<&'static str> = coverage
            .relationship_path_cases
            .keys()
            .map(|&(shape, path)| {
                assert_eq!(shape, "to_one_bare");
                path
            })
            .collect();
        paths.sort_unstable();
        paths
    }

    #[test]
    fn a_table_ordered_program_reaches_the_parent_paths_only() {
        // ops: [from-side seed, parent seed, parent toggle]
        assert_eq!(
            paths(&toggled_parent_program()),
            vec!["parent_insert", "parent_null_flip"]
        );
    }

    #[test]
    fn a_from_side_write_after_the_parent_changed_is_its_own_path() {
        // ops: [parent seed, parent toggle, from-side seed]: the parent is
        // unreferenced when it changes, and the from-side row then joins it.
        let interleaved = interleave_tables(toggled_parent_program(), &[1, 1, 0]);
        assert_eq!(paths(&interleaved), vec!["from_side_after_parent_change"]);
    }

    #[test]
    fn record_program_tallies_a_trivial_convergent_program() {
        let mut coverage = Coverage::new();
        let program = build_program(&[(Some(1), Some(2))], &[]);
        coverage.record_program(&program);

        assert_eq!(coverage.cases, 1);
        assert_eq!(coverage.ops_by_kind.get("Insert"), Some(&1));
        assert_eq!(coverage.ops_by_outcome.get("Succeeds"), Some(&1));
        assert!(coverage.expr_shapes.contains("Column"));
        assert!(coverage.expr_shapes.contains("BinaryOp"));
        assert!(coverage.operators.contains("Add"));
        assert!(coverage.types_exercised.contains("numeric"));
        assert_eq!(coverage.key_spaces.get("OneToOne"), Some(&1));
    }

    #[test]
    fn record_program_accumulates_across_calls() {
        let mut coverage = Coverage::new();
        coverage.record_program(&build_program(&[(Some(1), Some(2))], &[]));
        coverage.record_program(&build_program(&[(Some(3), Some(4))], &[]));

        assert_eq!(coverage.cases, 2);
        assert_eq!(coverage.ops_by_kind.get("Insert"), Some(&2));
    }

    #[test]
    fn display_output_is_stable_and_sorted() {
        let mut coverage = Coverage::new();
        coverage.record_program(&build_program(&[(Some(1), Some(2))], &[]));
        let printed = format!("{coverage}");
        assert!(printed.contains("cases: 1"));
        assert!(printed.contains("Insert=1"));
        assert!(printed.contains("OneToOne=1"));
    }
}
