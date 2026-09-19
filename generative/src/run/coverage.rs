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

use trellis::defs::ast::{Expr, KeySpace, Operator, TransformDef, ValueType};

use crate::model::{Cardinality, Op, OpOutcome, Program, Relationship};

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
    /// `trellis::defs::registry::FUNCTIONS`/`AGGREGATE_FUNCTION_SPECS`'s
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
    /// The largest [`Op::BulkInsert`] row count seen across every recorded
    /// program (improvement-plan task E6) — a floor test asserts this
    /// actually gets large across enough samples of
    /// [`crate::generate::bulk_insert_program`], not just that a small one
    /// occasionally shows up.
    pub max_bulk_insert_rows: usize,
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
        for def in &program.defs {
            for field in &def.fields {
                self.record_relationship_shapes(&field.expr, def, &by_name, false);
            }
        }
    }

    /// Walks `expr` tallying every relationship *reference* shape it reads
    /// into [`Self::relationship_shapes`] (issue #34). `wrapped` tracks
    /// whether the current subexpression sits directly under an aggregate
    /// call, since that — together with the relationship's cardinality and
    /// the definition's key-space — is exactly what distinguishes the three
    /// legal shapes from each other.
    fn record_relationship_shapes(
        &mut self,
        expr: &Expr,
        def: &TransformDef,
        by_name: &HashMap<&str, &Relationship>,
        wrapped: bool,
    ) {
        match expr {
            Expr::RelationshipPath { rel, .. } => {
                let cardinality = by_name.get(rel.as_str()).map(|r| r.cardinality);
                let in_aggregate_def = matches!(def.key_space, KeySpace::Aggregate { .. });
                let name = match (cardinality, wrapped, in_aggregate_def) {
                    (Some(Cardinality::ToOne), false, false) => "to_one_bare",
                    (Some(Cardinality::ToMany), true, false) => "to_many_in_aggregate",
                    (Some(Cardinality::ToOne), true, true) => "to_one_in_aggregate_def",
                    _ => "other",
                };
                *self.relationship_shapes.entry(name).or_insert(0) += 1;
            }
            Expr::Column(_) | Expr::NumberLiteral(_) | Expr::StringLiteral(_) => {}
            Expr::BinaryOp { lhs, rhs, .. } => {
                self.record_relationship_shapes(lhs, def, by_name, false);
                self.record_relationship_shapes(rhs, def, by_name, false);
            }
            Expr::FunctionCall { args, .. } => {
                // Only a *single*-argument call can be the aggregate-over-a-
                // path shape (ADR-0006: "wrapped in exactly one aggregate
                // function"), so a path buried among several arguments of a
                // scalar call is deliberately not counted as wrapped.
                let wrapped = args.len() == 1;
                for arg in args {
                    self.record_relationship_shapes(arg, def, by_name, wrapped);
                }
            }
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
/// `trellis::defs::registry::FUNCTIONS`/`AGGREGATE_FUNCTION_SPECS` already use
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
        writeln!(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate::build_program;

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
