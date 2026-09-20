//! Generator coverage meta-tests (issue #7, design doc §3 "Generator coverage
//! meta-tests"): no database, run in milliseconds. These check the
//! *generator's own surface*, not any oracle comparison — that coverage that
//! silently drops out (a type stops appearing in a derivation, an operator
//! stops being exercised, a widening quietly changes what used to be drawn)
//! is otherwise invisible until someone notices a property stopped catching
//! anything.

use std::collections::HashSet;

use generative::generate::{
    Mutate, build_program, bulk_insert_program, checkpoint_plan_for, noise_plan_for,
    program_with_client_restart, program_with_mid_stream_def_install, program_with_scale_out,
    trivial_program, trivial_program_with,
};
use generative::model::{NoiseAction, NoiseEventKind, Op, Program, Table};
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::TestRunner;
use trellis::dev::defs::ast::{Expr, KeySpace, Operator, ValueType};
use trellis::dev::defs::invertibility::{AggregateArg, CountArg, Invertibility, classify};

/// `ValueType` has no `Hash` impl (it's an engine type, not owned by this
/// crate), so scalar-type-surface comparisons here go through a sorted `Vec`
/// instead of a `HashSet`.
fn sorted_value_types(types: impl IntoIterator<Item = ValueType>) -> Vec<&'static str> {
    let mut names: Vec<&'static str> = types
        .into_iter()
        .map(|t| match t {
            ValueType::Numeric => "numeric",
            ValueType::Integer(width) => width.pg_name(),
            ValueType::Float(width) => width.pg_name(),
            ValueType::Text => "text",
            ValueType::Boolean => "boolean",
            ValueType::Uuid => "uuid",
            ValueType::Other(pg_type) => pg_type.name(),
        })
        .collect();
    names.sort_unstable();
    names.dedup();
    names
}

/// Every **value** column's scalar type appears both as a plain column and
/// via a derivation.
///
/// Improvement-plan task B1 widened the generator's whole type surface past
/// Numeric-only (design doc §3): every table now also gets one `Text`, one
/// `Boolean`, and one `Uuid` column, each with its own identity-passthrough
/// field (`SELECT <col> AS <col>`) on every def sourced from that table — see
/// `generate::build_program_multi`'s doc comment. This test's whole point is
/// to fail loudly the day a scalar type is added to the generated schema
/// without a matching derivation landing alongside it — exactly the
/// "coverage that silently drops out" the design doc warns about — so its
/// expected-surface assertion is widened here, not weakened or dropped.
///
/// The **primary-key** column is excluded, and that exclusion is the
/// assertion's shape, not a hole in it: the pk exists to be a *key*, and its
/// `bigint` type (`model::PRIMARY_KEY_VALUE_TYPE`, honest since issue #111
/// rather than the `Numeric` placeholder it used to carry) is exercised on
/// every single op of every single program — as the target's own primary
/// key, as the identity every `Update`/`Delete` addresses a row by, and as
/// the value every snapshot comparison is keyed on. Requiring it to *also*
/// appear inside a derivation would pin a passthrough field the generator
/// has no reason to emit, since a 1-1 target already carries the key column.
#[test]
fn every_column_scalar_type_appears_via_a_derivation() {
    let program = build_program(&[(Some(1), Some(2))], &[]);
    let source = &program.tables[0];

    let column_types = sorted_value_types(value_column_types(source));
    assert_eq!(
        column_types,
        vec!["boolean", "numeric", "text", "uuid"],
        "the generator's column type surface changed — widen this assertion (and the \
         derivation check below) alongside it, don't just let it pass silently"
    );

    let def = &program.defs[0];
    assert_eq!(
        def.fields.len(),
        4,
        "expected `total` plus one passthrough field per new column (task B1)"
    );
    let mut derivation_types_raw = Vec::new();
    for field in &def.fields {
        collect_column_types(&field.expr, source, &mut derivation_types_raw);
    }
    let derivation_types = sorted_value_types(derivation_types_raw);
    assert_eq!(
        derivation_types, column_types,
        "every column scalar type must appear via a derivation, not just as a column"
    );
}

/// Every column's [`ValueType`] *except* the primary key's — see
/// [`every_column_scalar_type_appears_via_a_derivation`] for why the pk is
/// held to the key role rather than the derivation role.
fn value_column_types(table: &Table) -> impl Iterator<Item = ValueType> + '_ {
    table
        .columns
        .iter()
        .filter(|c| c.name != table.pk_col)
        .map(|c| c.value_type)
}

fn collect_column_types(expr: &Expr, source: &Table, out: &mut Vec<ValueType>) {
    match expr {
        Expr::Column(name) => {
            let column = source
                .columns
                .iter()
                .find(|c| &c.name == name)
                .expect("the derivation must only reference declared columns");
            out.push(column.value_type);
        }
        Expr::BinaryOp { lhs, rhs, .. } => {
            collect_column_types(lhs, source, out);
            collect_column_types(rhs, source, out);
        }
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_column_types(arg, source, out);
            }
        }
        // A literal carries no *source column* type — including issue
        // #109's typed literal, whose type is its own, not a column's.
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::TypedLiteral { .. } => {}
        // Issue #34: a relationship path's column lives on another table
        // entirely, so there is no type to collect from `source`. The one
        // caller feeds this a `build_program` program — a single table, one
        // definition, no relationships declared at all, so no path can
        // reach here. Refusing rather than returning nothing keeps that
        // precondition enforced instead of assumed.
        Expr::RelationshipPath { rel, column } => {
            unreachable!(
                "collect_column_types is only ever called on `build_program`'s single-table, \
                 relationship-free program, which cannot contain the path '{rel}.{column}'"
            )
        }
    }
}

/// `build_program`'s fixed `total = c1 + c2` field (unaffected by
/// improvement-plan task B2's new "derived" field — see
/// `generate::build_program_multi_with_derived`'s doc comment, which layers
/// on top of `build_program_multi` rather than changing its output) still
/// draws exactly `Operator::Add` over `Numeric, Numeric`, unchanged from
/// before task B2. `Operator::GreaterThan` is drawn too now, but only by the
/// proptest strategy's independent `derived` field — see
/// `trivial_program_sometimes_draws_greater_than` below for that floor.
#[test]
fn every_supported_operator_appears_over_every_supported_argument_type() {
    let program = build_program(&[(Some(1), Some(2))], &[]);
    let def = &program.defs[0];
    let Expr::BinaryOp { op, lhs, rhs } = &def.fields[0].expr else {
        panic!("expected the sole field's derivation to be a binary op");
    };
    assert_eq!(
        *op,
        Operator::Add,
        "the generator's only operator today is `+`"
    );

    for operand in [lhs.as_ref(), rhs.as_ref()] {
        match operand {
            Expr::Column(name) => {
                let column = program.tables[0]
                    .columns
                    .iter()
                    .find(|c| &c.name == name)
                    .expect("operand column must be declared");
                assert_eq!(
                    column.value_type,
                    ValueType::Numeric,
                    "`+`'s only supported argument type today is Numeric"
                );
            }
            other => panic!("expected a column operand, got {other:?}"),
        }
    }
}

/// `table`'s grain column name in `program`, if `table` is one of
/// `program`'s tables. Every table gets a grain column unconditionally
/// (improvement-plan task B4) at a fixed position (`Table::new`'s
/// `columns[6]` — see `generate::build_program_multi_with_shapes`'s doc
/// comment on that index), and `generate::strategy::grain_value` draws an
/// occasional `NULL` for it *unconditionally*, independent of the
/// `awkward_values` flag (issue #128) — so a "never draws NULL" check scoped
/// to the flag must exclude this one structurally-always-on column.
fn grain_column(program: &Program, table: &str) -> Option<String> {
    program
        .tables
        .iter()
        .find(|t| t.name == table)
        .map(|t| t.columns[6].name.clone())
}

/// Every value drawn from an `Op::Insert`/`Op::Update`/`Op::BulkInsert` in
/// `program`, in draw order, excluding each row's grain-column value (see
/// [`grain_column`] — it draws `NULL` unconditionally, independent of
/// `awkward_values`, so it isn't part of what this helper's callers are
/// checking). Ignores `Op::Delete`/`Op::Truncate` (neither carries a field
/// value — improvement-plan task E6's `Truncate` is exactly as value-free as
/// `Delete` here).
fn all_op_values(program: &generative::model::Program) -> Vec<Option<String>> {
    let not_grain = |table: &str, name: &str| grain_column(program, table).as_deref() != Some(name);
    program
        .ops
        .iter()
        .flat_map(|op| match op {
            Op::Insert { table, row, .. } => row
                .iter()
                .filter(|(name, _)| not_grain(table, name))
                .map(|(_, v)| v.clone())
                .collect::<Vec<_>>(),
            Op::Update { table, changes, .. } => changes
                .iter()
                .filter(|(name, _)| not_grain(table, name))
                .map(|(_, v)| v.clone())
                .collect(),
            Op::Delete { .. } | Op::Truncate { .. } => Vec::new(),
            Op::BulkInsert { table, rows, .. } => rows
                .iter()
                .flat_map(|row| {
                    row.iter()
                        .filter(|(name, _)| not_grain(table, name))
                        .map(|(_, v)| v.clone())
                })
                .collect(),
        })
        .collect()
}

/// With the awkward-value feature flag off, the generator's *flag-gated*
/// value draws are structurally identical to before issue #7 widened the
/// generator: a bare `0..=VALUE_MAX` integer, never `None`/SQL `NULL`. Every
/// table's grain column is excluded from this check ([`all_op_values`]/
/// [`grain_column`]) because it draws an occasional `NULL` unconditionally,
/// regardless of this flag (issue #128) — see `generate::strategy::grain_value`'s
/// doc comment. Sampling many programs (rather than instrumenting the exact
/// strategy call sequence) is the practical way to prove the *behavior* —
/// what actually gets drawn — hasn't silently changed; see
/// `trivial_program_with`'s doc comment for why the off-path strategy shape
/// is deliberately kept byte-for-byte the same for every flag-gated column.
#[test]
fn awkward_values_off_never_draws_null() {
    let mut runner = TestRunner::default();
    let strategy = trivial_program_with(false);
    for _ in 0..200 {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        assert!(
            all_op_values(&program).iter().all(Option::is_some),
            "awkward_values=false must never draw NULL: {program:#?}"
        );
    }
}

/// With the awkward-value feature flag on, the new NULL shapes actually
/// appear — the other half of the same coverage meta-test (design doc §3).
#[test]
fn awkward_values_on_sometimes_draws_null() {
    let mut runner = TestRunner::default();
    let strategy = trivial_program_with(true);
    let saw_null = (0..500).any(|_| {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        all_op_values(&program).iter().any(Option::is_none)
    });
    assert!(
        saw_null,
        "awkward_values=true must draw NULL at least once across 500 samples"
    );
}

/// A grouping (grain) value's `NULL` draw (improvement-plan task B4, issue
/// #128) actually appears, and does so **regardless of `awkward_values`** —
/// unlike every other awkward-value column, `generate::strategy::grain_value`
/// is not gated by the flag (see `trivial_program_with`'s doc comment), so
/// this samples both `false` and `true` and expects to see it either way.
/// Complements [`awkward_values_off_never_draws_null`]/
/// `awkward_values_on_sometimes_draws_null` above, which deliberately
/// exclude the grain column from their own check via [`all_op_values`]/
/// [`grain_column`] — this is the other half, asserting what those two
/// leave out actually happens.
#[test]
fn grain_value_sometimes_draws_null_regardless_of_awkward_values() {
    for awkward_values in [false, true] {
        let mut runner = TestRunner::default();
        let strategy = trivial_program_with(awkward_values);
        let saw_null_grain = (0..500).any(|_| {
            let program = strategy
                .new_tree(&mut runner)
                .expect("strategy must produce a value")
                .current();
            program.ops.iter().any(|op| match op {
                Op::Insert { table, row, .. } => row.iter().any(|(name, v)| {
                    v.is_none() && grain_column(&program, table).as_deref() == Some(name)
                }),
                Op::BulkInsert { table, rows, .. } => rows.iter().any(|row| {
                    row.iter().any(|(name, v)| {
                        v.is_none() && grain_column(&program, table).as_deref() == Some(name)
                    })
                }),
                _ => false,
            })
        });
        assert!(
            saw_null_grain,
            "grain_value must draw NULL at least once across 500 samples \
             (awkward_values={awkward_values})"
        );
    }
}

/// The default [`trivial_program`] strategy (awkward values on) also
/// occasionally draws a genuine `apply()`-failing duplicate-pk insert
/// (issue #6's gap) — sampled the same way as the NULL check above.
#[test]
fn trivial_program_sometimes_draws_a_duplicate_pk_insert() {
    let mut runner = TestRunner::default();
    let strategy = trivial_program();
    let saw_duplicate = (0..500).any(|_| {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        program.ops.iter().any(|op| matches!(op, Op::Insert { .. }))
            && has_duplicate_pk_insert(&program)
    });
    assert!(
        saw_duplicate,
        "the generator must sometimes draw a duplicate-pk insert across 500 samples"
    );
}

/// Whether any table in `program` has two inserts at the same pk. Keyed by
/// `(table name, pk)`, not pk alone: since B3 (improvement-plan), a program
/// can draw multiple tables, and every table's seeded pks independently
/// start at 1 (see `generate::tests::pk_liveness_multi_table`) — table A's
/// pk 1 and table B's pk 1 are two different rows, not a duplicate, so
/// pk-alone dedup would false-positive on the common case of two tables each
/// seeding a pk-1 row. Each insert's pk column is looked up on *its own*
/// target table (via `Op::Insert`'s `table` field), not assumed to be
/// `program.tables[0]`'s — a different table's pk column has a different
/// name (see `NamePool`), so that assumption would otherwise panic the
/// first time an insert targeted any table but the first one drawn.
fn has_duplicate_pk_insert(program: &generative::model::Program) -> bool {
    let mut seen = HashSet::new();
    for op in &program.ops {
        if let Op::Insert {
            table: table_name,
            row,
            ..
        } = op
        {
            let table = program
                .tables
                .iter()
                .find(|t| &t.name == table_name)
                .expect("insert must target a declared table");
            let pk = row
                .iter()
                .find(|(name, _)| *name == table.pk_col)
                .and_then(|(_, v)| v.clone())
                .expect("insert must carry a pk value");
            if !seen.insert((table_name.clone(), pk)) {
                return true;
            }
        }
    }
    false
}

/// Generator invariants survive generation (design doc §2's structural
/// invariant): the primary-key column is always declared on its table, and
/// every `Insert` always carries a non-NULL value for it. `Mutate` is a
/// hand-built enum with no representable state that could null or drop the
/// pk (see [`Mutate`] and `build_program`, which always writes
/// `Some(pk.to_string())` for the pk column of every insert it emits) — so
/// there is no proptest shrink step that could strand this invariant, and
/// sampling broadly here is a check on the generator's actual behavior, not
/// just its types.
#[test]
fn pk_column_is_never_null_or_missing_across_many_generated_programs() {
    let mut runner = TestRunner::default();
    let strategy = trivial_program();
    for _ in 0..200 {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        for table in &program.tables {
            assert!(
                table.columns.iter().any(|c| c.name == table.pk_col),
                "every table must declare its own pk column: {program:#?}"
            );
        }
        for op in &program.ops {
            if let Op::Insert {
                table: table_name,
                row,
                ..
            } = op
            {
                let table = program
                    .tables
                    .iter()
                    .find(|t| &t.name == table_name)
                    .expect("insert must target a declared table");
                let pk_value = row
                    .iter()
                    .find(|(name, _)| name == &table.pk_col)
                    .map(|(_, v)| v);
                assert!(
                    matches!(pk_value, Some(Some(_))),
                    "the pk column must be present and non-NULL on every insert: {program:#?}"
                );
            }
        }
    }
}

/// Sanity check on the `Mutate` shape itself, independent of sampling: a
/// `DuplicateInsert` targets a seeded pk (never `seed_count + 1`, which would
/// miss and not be a duplicate). Cheap, no DB, and pins the invariant the
/// `mutate()` strategy relies on.
#[test]
fn duplicate_insert_is_a_distinct_mutate_from_update_and_delete() {
    let program = build_program(
        &[(Some(1), Some(2))],
        &[Mutate::DuplicateInsert {
            pk: 1,
            c1: Some(3),
            c2: Some(4),
        }],
    );
    assert_eq!(program.ops.len(), 2);
    assert!(matches!(program.ops[1], Op::Insert { .. }));
}

/// A2 (`local_docs/generative-suite-improvement-plan.md`): the [`Coverage`]
/// accumulator's floors — no database, no `Harness`, just the default
/// strategy sampled many times, same as every other test in this file. This
/// is the fast half of A2's payoff: if the generator's own machinery ever
/// stopped drawing one of these shapes (a `Delete`, a genuinely-failing op, a
/// zero-row no-op, `+`, a numeric column, a `OneToOne` def), this test would
/// catch it in milliseconds, without ever standing up a cluster. It
/// deliberately does not check NULL or duplicate-pk-insert specifically — the
/// two tests above already cover those, and `Coverage` itself only looks at
/// op kind/outcome/structure, never op values.
#[test]
fn a_real_run_of_the_default_strategy_meets_its_coverage_floors() {
    let mut runner = TestRunner::default();
    let strategy = trivial_program();
    let mut coverage = generative::run::Coverage::new();
    for _ in 0..500 {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();

        // Improvement-plan task B1: unlike NULL/duplicate-insert (genuinely
        // probabilistic, checked only in aggregate below), every table
        // always gets one Text/Boolean/Uuid column — so this is checked on
        // *every* sampled program, not just "at least one of 500", the
        // stronger form the task calls for where it actually holds.
        for table in &program.tables {
            let types = sorted_value_types(value_column_types(table));
            assert_eq!(
                types,
                vec!["boolean", "numeric", "text", "uuid"],
                "every table must always have exactly this value-column type surface: \
                 {program:#?}"
            );
            // The pk is excluded above because it is a key, not a value —
            // but it is still pinned, since issue #111 made its recorded
            // type an honest `bigint` that every DDL and cast site now
            // renders straight from (`backend::sql::column_pg_type`).
            let pk = table
                .columns
                .iter()
                .find(|c| c.name == table.pk_col)
                .expect("every table has its pk column");
            assert_eq!(
                pk.value_type,
                generative::model::PRIMARY_KEY_VALUE_TYPE,
                "the primary key's recorded type must stay the one the DDL renders: \
                 {program:#?}"
            );
        }

        coverage.record_program(&program);
    }

    for kind in ["Insert", "Update", "Delete"] {
        assert!(
            coverage.ops_by_kind.get(kind).copied().unwrap_or(0) > 0,
            "coverage floor failed: expected at least one {kind} op across 500 samples:\n{coverage}"
        );
    }

    for outcome in ["Succeeds", "Fails", "AffectsNoRows"] {
        assert!(
            coverage.ops_by_outcome.get(outcome).copied().unwrap_or(0) > 0,
            "coverage floor failed: expected at least one op with outcome {outcome} across 500 \
             samples:\n{coverage}"
        );
    }

    // Improvement-plan task B1: every table always gets one Text, one
    // Boolean, and one Uuid column (not a probabilistically-drawn shape like
    // NULL/duplicate-insert above), so these are an unconditional floor —
    // "every single sample", not "at least one of 500" — over 500 samples of
    // a strategy that always draws at least one table.
    for value_type in ["numeric", "text", "boolean", "uuid"] {
        assert!(
            coverage.types_exercised.contains(value_type),
            "coverage floor failed: expected {value_type:?} among types_exercised:\n{coverage}"
        );
    }

    assert!(
        coverage.key_spaces.contains_key("OneToOne"),
        "coverage floor failed: expected \"OneToOne\" among key_spaces:\n{coverage}"
    );

    for shape in ["Column", "BinaryOp"] {
        assert!(
            coverage.expr_shapes.contains(shape),
            "coverage floor failed: expected {shape:?} among expr_shapes:\n{coverage}"
        );
    }

    assert!(
        coverage.operators.contains("Add"),
        "coverage floor failed: expected \"Add\" among operators:\n{coverage}"
    );
}

/// Improvement-plan task B3: the default strategy must sometimes draw more
/// than one source table — sampled the same way as
/// `awkward_values_on_sometimes_draws_null`/
/// `trivial_program_sometimes_draws_a_duplicate_pk_insert` above. Before B3
/// this was structurally impossible (`build_program` always built exactly
/// one table); this is the coverage meta-test that would catch B3's widening
/// silently regressing back to always-one (design doc §3 "coverage that
/// silently drops out").
#[test]
fn trivial_program_sometimes_draws_more_than_one_table() {
    let mut runner = TestRunner::default();
    let strategy = trivial_program();
    let saw_multiple_tables = (0..500).any(|_| {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        program.tables.len() > 1
    });
    assert!(
        saw_multiple_tables,
        "the generator must sometimes draw more than one table across 500 samples"
    );
}

/// The definition-count half of B3's coverage floor: the default strategy
/// must sometimes draw more than one definition in the same program
/// (whether or not those definitions share a source table — this test only
/// asserts the count, not the fan-out shape).
#[test]
fn trivial_program_sometimes_draws_more_than_one_definition() {
    let mut runner = TestRunner::default();
    let strategy = trivial_program();
    let saw_multiple_defs = (0..500).any(|_| {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        program.defs.len() > 1
    });
    assert!(
        saw_multiple_defs,
        "the generator must sometimes draw more than one definition across 500 samples"
    );
}

/// Both "two defs sharing one source table" and "defs spread across
/// different source tables" must be reachable — B3's stated goal is that
/// neither shape is forced out by the other. Sampled together (rather than
/// as two separate single-shape tests) so a single 500-sample run has to
/// produce both, matching how the strategy actually draws (independently,
/// not correlated).
#[test]
fn trivial_program_sometimes_draws_defs_sharing_a_source_and_sometimes_draws_defs_on_different_sources()
 {
    let mut runner = TestRunner::default();
    let strategy = trivial_program();
    let mut saw_shared_source = false;
    let mut saw_different_sources = false;
    for _ in 0..500 {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        if program.defs.len() < 2 {
            continue;
        }
        let sources: Vec<&str> = program.defs.iter().map(|d| d.source.as_str()).collect();
        let mut deduped = sources.clone();
        deduped.sort_unstable();
        deduped.dedup();
        if deduped.len() < sources.len() {
            // At least two defs collapsed onto the same source.
            saw_shared_source = true;
        }
        if deduped.len() == sources.len() {
            // Every def in this sample has its own distinct source.
            saw_different_sources = true;
        }
    }
    assert!(
        saw_shared_source,
        "expected at least one sample where two or more defs share a source table across 500 samples"
    );
    assert!(
        saw_different_sources,
        "expected at least one sample where all defs draw distinct source tables across 500 samples"
    );
}

/// Improvement-plan task B2: the default strategy's new "derived" field
/// (`generate::DerivedShape`) must sometimes draw `Operator::GreaterThan` —
/// sampled the same way as every other genuinely-probabilistic floor above
/// (many samples, not every sample: which `DerivedShape` variant a def draws
/// is random).
#[test]
fn trivial_program_sometimes_draws_greater_than() {
    let mut runner = TestRunner::default();
    let strategy = trivial_program();
    let mut coverage = generative::run::Coverage::new();
    for _ in 0..500 {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        coverage.record_program(&program);
    }
    assert!(
        coverage.operators.contains("GreaterThan"),
        "coverage floor failed: expected \"GreaterThan\" among operators across 500 samples:\n{coverage}"
    );
}

/// Improvement-plan task B2: each of the five scalar functions
/// (`STRPOS`/`OCTET_LENGTH`/`CHAR_LENGTH`/`REGEXP_COUNT`/`COALESCE`) must
/// appear at least once across many samples — the same "each shape reachable
/// across a bounded number of samples" floor as `GreaterThan` above, applied
/// to every function `generate::DerivedShape` can draw.
#[test]
fn trivial_program_draws_every_scalar_function_at_least_once() {
    let mut runner = TestRunner::default();
    let strategy = trivial_program();
    let mut coverage = generative::run::Coverage::new();
    for _ in 0..500 {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        coverage.record_program(&program);
    }
    for function in [
        "STRPOS",
        "OCTET_LENGTH",
        "CHAR_LENGTH",
        "REGEXP_COUNT",
        "COALESCE",
    ] {
        assert!(
            coverage.functions.contains(function),
            "coverage floor failed: expected {function:?} among functions across 500 samples:\n{coverage}"
        );
    }
}

/// Improvement-plan task B2: the default strategy must sometimes draw a
/// nested (depth >= 3) expression tree — e.g. `(c1 + c2) > c1`
/// (`DerivedShape::ArithmeticGreaterThan`) or `STRPOS(text_col, 'x') > 0`
/// (`DerivedShape::StrposGreaterThan`) — not just the single-operator/
/// single-function leaves every other `DerivedShape` variant (and `total`)
/// draws. `Coverage::max_expr_depth` is the accumulator this asserts
/// against; see its doc comment for why it's tracked separately from
/// `expr_shapes`.
#[test]
fn trivial_program_sometimes_draws_a_nested_expression() {
    let mut runner = TestRunner::default();
    let strategy = trivial_program();
    let mut coverage = generative::run::Coverage::new();
    for _ in 0..500 {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        coverage.record_program(&program);
    }
    assert!(
        coverage.max_expr_depth >= 3,
        "coverage floor failed: expected a nested expression of depth >= 3 across 500 samples \
         (max depth seen: {}):\n{coverage}",
        coverage.max_expr_depth
    );
}

/// Every [`Expr::FunctionCall`] name appearing anywhere in `program`'s defs
/// (recursing into arguments, though today's `KeySpace::Aggregate` generator
/// only ever nests one level deep — a `SUM`/`AVG`/`MIN`/`MAX` call wrapping a
/// bare `Column`, or a bare `COUNT`).
fn all_function_call_names(program: &Program) -> HashSet<String> {
    let mut names = HashSet::new();
    fn walk(expr: &Expr, out: &mut HashSet<String>) {
        if let Expr::FunctionCall { name, args } = expr {
            out.insert(name.clone());
            for arg in args {
                walk(arg, out);
            }
        }
    }
    for def in &program.defs {
        for field in &def.fields {
            walk(&field.expr, &mut names);
        }
    }
    names
}

/// Improvement-plan task B4 (the widening unit's coverage meta-test): all
/// five aggregate functions (`SUM`/`COUNT`/`AVG`/`MIN`/`MAX`,
/// `trellis::dev::defs::registry::AGGREGATE_FUNCTIONS`) must actually get drawn
/// across enough sampled programs — this is the floor that would catch B4's
/// aggregate-function widening silently regressing (design doc §3 "coverage
/// that silently drops out"), the same principle every other floor test in
/// this file already checks for an earlier widening.
///
/// **Post-merge fix.** The early-exit condition below used to read
/// `seen.len() >= 5` unfiltered — a correct "all five aggregate names have
/// been seen" check back when a generated program's `FunctionCall`s could
/// only ever be aggregate ones. Now that improvement-plan task B2's scalar
/// `DerivedShape` functions (`STRPOS`/`OCTET_LENGTH`/`CHAR_LENGTH`/
/// `REGEXP_COUNT`/`COALESCE`) are drawn too (see
/// [`AGGREGATE_FUNCTION_NAMES`]'s own doc comment), `seen` can reach 5
/// distinct names from scalar functions alone, breaking out before any
/// aggregate def — let alone `AVG` specifically — has actually been drawn.
/// The loop now only counts aggregate names toward the early exit.
#[test]
fn trivial_program_draws_all_five_aggregate_functions_across_enough_samples() {
    let mut runner = TestRunner::default();
    let strategy = trivial_program();
    let mut seen: HashSet<String> = HashSet::new();
    for _ in 0..1000 {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        seen.extend(all_function_call_names(&program));
        let aggregate_names_seen = seen
            .iter()
            .filter(|name| AGGREGATE_FUNCTION_NAMES.contains(&name.as_str()))
            .count();
        if aggregate_names_seen >= AGGREGATE_FUNCTION_NAMES.len() {
            break;
        }
    }
    for name in AGGREGATE_FUNCTION_NAMES {
        assert!(
            seen.contains(*name),
            "expected {name} to be drawn among aggregate function calls across 1000 samples; \
             saw: {seen:?}"
        );
    }
}

/// The five `KeySpace::Aggregate` function names
/// (`trellis::dev::defs::registry::AGGREGATE_FUNCTIONS`), duplicated here as a
/// `const` rather than imported: `generative` doesn't re-export the engine's
/// `registry` module, and this list is short/stable enough (the same
/// `SUM`/`COUNT`/`AVG`/`MIN`/`MAX` five [`AggregateFn`] already hardcodes) that
/// a local copy is clearer than plumbing a new import through for one test.
///
/// **Why this filter is needed post-merge.** [`all_function_call_names`]
/// collects *every* `FunctionCall` name in a program's defs, including the
/// five scalar functions (`STRPOS`/`OCTET_LENGTH`/`CHAR_LENGTH`/
/// `REGEXP_COUNT`/`COALESCE`) improvement-plan task B2's `DerivedShape` draws
/// on `OneToOne` defs — B4 was developed before B2 merged in, when the only
/// `FunctionCall`s a generated program could ever contain were aggregate
/// ones, so `trivial_program_draws_both_invertibility_classes_across_enough_samples`
/// below used to call [`classify`] on every collected name unfiltered. Now
/// that both widenings are merged, an unfiltered name can be a scalar
/// function `classify` has never heard of (it only knows the aggregate
/// registry) — filtering to this list keeps that test checking what it always
/// meant to check.
///
/// [`AggregateFn`]: generative::generate::AggregateFn
const AGGREGATE_FUNCTION_NAMES: &[&str] = &["SUM", "COUNT", "AVG", "MIN", "MAX"];

/// The other half of B4's coverage floor: both invertibility classes
/// (`trellis::dev::defs::invertibility::Invertibility`) must appear across a run —
/// at least one `Invertible` function (`SUM`/`COUNT`/`AVG`) and at least one
/// `RecomputeOnly` function (`MIN`/`MAX`) — not just "all five names appear"
/// in the abstract. This is what makes the generative suite a real exerciser
/// of the engine's invertibility split (`docs/generative-test-suite.md` §4
/// calls the aggregate delta path "the hardest guarantee" precisely because
/// of this split), not just a name-coverage checklist.
#[test]
fn trivial_program_draws_both_invertibility_classes_across_enough_samples() {
    let mut runner = TestRunner::default();
    let strategy = trivial_program();
    let mut saw_invertible = false;
    let mut saw_recompute_only = false;
    for _ in 0..1000 {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        // Only classify the aggregate functions — see
        // `AGGREGATE_FUNCTION_NAMES`'s doc comment for why a post-merge
        // program's `FunctionCall`s aren't all aggregate ones anymore.
        for name in all_function_call_names(&program)
            .into_iter()
            .filter(|name| AGGREGATE_FUNCTION_NAMES.contains(&name.as_str()))
        {
            let arg = if name == "COUNT" {
                AggregateArg::Count(CountArg::Star)
            } else {
                AggregateArg::Column(ValueType::Numeric)
            };
            let verdict = classify(&name, arg)
                .unwrap_or_else(|| panic!("{name} must be a known aggregate function"));
            match verdict.invertibility {
                Invertibility::Invertible => saw_invertible = true,
                Invertibility::RecomputeOnly => saw_recompute_only = true,
            }
        }
        if saw_invertible && saw_recompute_only {
            break;
        }
    }
    assert!(
        saw_invertible,
        "expected at least one Invertible aggregate function (SUM/COUNT/AVG) across 1000 samples"
    );
    assert!(
        saw_recompute_only,
        "expected at least one RecomputeOnly aggregate function (MIN/MAX) across 1000 samples"
    );
}

/// The key-space half of B4's coverage floor: the default strategy must
/// sometimes draw a `KeySpace::Aggregate` definition at all (not just
/// `OneToOne`, the only shape before this task) — sampled the same way as
/// `trivial_program_sometimes_draws_more_than_one_table` above.
#[test]
fn trivial_program_sometimes_draws_an_aggregate_key_space() {
    let mut runner = TestRunner::default();
    let strategy = trivial_program();
    let saw_aggregate = (0..500).any(|_| {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        program
            .defs
            .iter()
            .any(|def| matches!(def.key_space, KeySpace::Aggregate { .. }))
    });
    assert!(
        saw_aggregate,
        "the generator must sometimes draw a KeySpace::Aggregate definition across 500 samples"
    );
}

/// Regression coverage floor for a real engine bug (`apply_aggregate.rs`'s
/// `add_contributions`/`sub_contributions`): a brand-new `Aggregate` group
/// whose only source row has a `NULL` value for its sole `SUM`/`AVG`
/// argument used to never get a target row written at all. The generator's
/// existing `awkward_values` `NULL`-drawing logic (`generate::strategy::value`,
/// exercised on every column including a table's very first seed row) already
/// draws exactly this shape by construction — nothing new needed there, see
/// `awkward_values_on_sometimes_draws_null` above for the general NULL floor
/// this specializes — so this is purely a coverage floor confirming the
/// specific "NULL on the table's very first live row, for a column that is
/// some Aggregate def's SUM/AVG argument" shape stays reliably reachable, the
/// same "coverage that silently drops out" principle every other floor test
/// in this file already checks for its own shape.
#[test]
fn trivial_program_sometimes_draws_a_null_first_row_for_an_aggregate_sum_avg_argument() {
    let mut runner = TestRunner::default();
    let strategy = trivial_program();
    let saw_it = (0..2000).any(|_| {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        program_has_null_first_row_sum_avg_arg(&program)
    });
    assert!(
        saw_it,
        "expected at least one sample with a NULL value on a table's very first \
         live row for a column used as some Aggregate def's SUM/AVG argument, \
         across 2000 samples"
    );
}

/// Whether any [`KeySpace::Aggregate`] def in `program` has a `SUM`/`AVG`
/// field whose argument column is `NULL` on its source table's very first
/// `Insert` op (in program order — seed-before-mutate means this is always
/// the table's first-ever seeded row, never a later mutate). Mirrors the
/// exact shape that used to make `add_contributions`'s missing-entry bug
/// bite: a group whose only contributing row is that same first row, with a
/// `NULL` argument and no other row yet to anchor a write.
fn program_has_null_first_row_sum_avg_arg(program: &Program) -> bool {
    for def in &program.defs {
        if !matches!(def.key_space, KeySpace::Aggregate { .. }) {
            continue;
        }
        let sum_avg_columns = def.fields.iter().filter_map(|field| match &field.expr {
            Expr::FunctionCall { name, args } if name == "SUM" || name == "AVG" => {
                match args.first() {
                    Some(Expr::Column(col)) => Some(col.as_str()),
                    _ => None,
                }
            }
            _ => None,
        });
        let Some(Op::Insert { row, .. }) = program
            .ops
            .iter()
            .find(|op| matches!(op, Op::Insert { table, .. } if table == &def.source))
        else {
            continue;
        };
        for col in sum_avg_columns {
            if row
                .iter()
                .any(|(name, value)| name == col && value.is_none())
            {
                return true;
            }
        }
    }
    false
}

/// Workstream E, task E1: `noise_plan_for` must sometimes draw a non-empty
/// plan at all (as opposed to always drawing zero events, which would make
/// `tests/noise.rs`'s property vacuous) — sampled at a representative op
/// count (4, `generate::MAX_MUTATES`'s own ceiling) the same way every other
/// floor test here samples `trivial_program()`.
#[test]
fn noise_plan_for_sometimes_draws_at_least_one_event() {
    let mut runner = TestRunner::default();
    let strategy = noise_plan_for(4);
    let saw_nonempty = (0..500).any(|_| {
        let plan = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        !plan.events.is_empty()
    });
    assert!(
        saw_nonempty,
        "noise_plan_for must sometimes draw at least one event across 500 samples"
    );
}

/// E1: every [`NoiseAction`] variant — both DML (`Insert`/`Update`/`Delete`)
/// and DDL (`AddColumn`/`DropColumn`) — must be reachable across enough
/// samples, matching this suite's own "every shape reachable across a
/// bounded number of samples" convention (e.g.
/// `trivial_program_draws_every_scalar_function_at_least_once` above). If
/// `AddColumn`/`DropColumn` silently stopped being drawn, the DDL half of
/// E1's "ideally also some DDL" ask would quietly regress to DML-only noise
/// without any test noticing.
#[test]
fn noise_plan_for_draws_every_action_variant_across_enough_samples() {
    let mut runner = TestRunner::default();
    let strategy = noise_plan_for(4);
    let mut saw_insert = false;
    let mut saw_update = false;
    let mut saw_delete = false;
    let mut saw_add_column = false;
    let mut saw_drop_column = false;
    for _ in 0..500 {
        let plan = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        for event in &plan.events {
            let NoiseEventKind::Table(action) = &event.kind else {
                panic!("noise_plan_for must only ever draw Table(..) events, got {event:?}");
            };
            match action {
                NoiseAction::Insert { .. } => saw_insert = true,
                NoiseAction::Update { .. } => saw_update = true,
                NoiseAction::Delete { .. } => saw_delete = true,
                NoiseAction::AddColumn { .. } => saw_add_column = true,
                NoiseAction::DropColumn { .. } => saw_drop_column = true,
            }
        }
        if saw_insert && saw_update && saw_delete && saw_add_column && saw_drop_column {
            break;
        }
    }
    assert!(
        saw_insert,
        "expected an Insert noise action across 500 samples"
    );
    assert!(
        saw_update,
        "expected an Update noise action across 500 samples"
    );
    assert!(
        saw_delete,
        "expected a Delete noise action across 500 samples"
    );
    assert!(
        saw_add_column,
        "expected an AddColumn noise action across 500 samples"
    );
    assert!(
        saw_drop_column,
        "expected a DropColumn noise action across 500 samples"
    );
}

/// E1: `noise_plan_for` must sometimes draw an event at position `0` (fires
/// before any real op) and sometimes at the last legal position (`op_count`,
/// fires after the very last real op) — both ends of the interleaving range
/// need real coverage, not just the middle.
#[test]
fn noise_plan_for_sometimes_fires_at_both_ends_of_the_op_stream() {
    let mut runner = TestRunner::default();
    let op_count = 4;
    let strategy = noise_plan_for(op_count);
    let mut saw_start = false;
    let mut saw_end = false;
    for _ in 0..500 {
        let plan = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        if plan.events.iter().any(|e| e.before_op == 0) {
            saw_start = true;
        }
        if plan.events.iter().any(|e| e.before_op == op_count) {
            saw_end = true;
        }
        if saw_start && saw_end {
            break;
        }
    }
    assert!(
        saw_start,
        "expected a noise event at position 0 (before any real op) across 500 samples"
    );
    assert!(
        saw_end,
        "expected a noise event at the last position (after the last real op) across 500 samples"
    );
}

/// Workstream E, task E5: `checkpoint_plan_for` must sometimes draw at least
/// one `CHECKPOINT` event (not always zero, which would make
/// `tests/noise.rs`'s checkpoint property vacuous), and every drawn event
/// must actually be the `CHECKPOINT` admin statement (task E5's whole
/// point), never a `Table(..)` event (this plan never installs a table at
/// all).
#[test]
fn checkpoint_plan_for_sometimes_draws_at_least_one_checkpoint() {
    let mut runner = TestRunner::default();
    let strategy = checkpoint_plan_for(4);
    let mut saw_checkpoint = false;
    for _ in 0..500 {
        let plan = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        assert!(
            plan.table.is_none(),
            "checkpoint_plan_for must never install a noise table: {plan:?}"
        );
        for event in &plan.events {
            match &event.kind {
                NoiseEventKind::Admin(sql) => {
                    assert_eq!(
                        sql, "CHECKPOINT",
                        "checkpoint_plan_for must only draw CHECKPOINT"
                    );
                    saw_checkpoint = true;
                }
                NoiseEventKind::Table(action) => {
                    panic!("checkpoint_plan_for must never draw a Table(..) event, got {action:?}")
                }
            }
        }
    }
    assert!(
        saw_checkpoint,
        "checkpoint_plan_for must sometimes draw at least one CHECKPOINT across 500 samples"
    );
}

/// Improvement-plan task E2's coverage floor: `program_with_mid_stream_def_install`
/// must actually draw a deferred install (a nonzero `def_install_after_op`
/// entry) across enough samples — sampled the same way as
/// `trivial_program_sometimes_draws_more_than_one_table` above, via the
/// `Coverage` accumulator's own tally rather than hand-walking the field here
/// a second time.
#[test]
fn mid_stream_def_installs_actually_get_drawn() {
    let mut runner = TestRunner::default();
    let strategy = program_with_mid_stream_def_install(true);
    let mut coverage = generative::run::Coverage::new();
    for _ in 0..500 {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        coverage.record_program(&program);
    }
    assert!(
        coverage.mid_stream_def_installs > 0,
        "coverage floor failed: expected at least one deferred (mid-stream) definition install \
         across 500 samples:\n{coverage}"
    );
}

/// Improvement-plan task E3's coverage floor, restart half:
/// `program_with_client_restart` must actually schedule a restart across
/// enough samples.
#[test]
fn client_restarts_actually_get_drawn() {
    let mut runner = TestRunner::default();
    let strategy = program_with_client_restart(true);
    let mut coverage = generative::run::Coverage::new();
    for _ in 0..500 {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        coverage.record_program(&program);
    }
    assert!(
        coverage.client_restarts > 0,
        "coverage floor failed: expected at least one scheduled client restart across 500 \
         samples:\n{coverage}"
    );
}

/// Improvement-plan task E3's coverage floor, scale-out half: same shape as
/// `client_restarts_actually_get_drawn` above, over `program_with_scale_out`.
#[test]
fn client_scale_outs_actually_get_drawn() {
    let mut runner = TestRunner::default();
    let strategy = program_with_scale_out(true);
    let mut coverage = generative::run::Coverage::new();
    for _ in 0..500 {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        coverage.record_program(&program);
    }
    assert!(
        coverage.client_scale_outs > 0,
        "coverage floor failed: expected at least one scheduled client scale-out across 500 \
         samples:\n{coverage}"
    );
}

/// Improvement-plan task E6's coverage floor, TRUNCATE half: the *default*
/// strategy (`trivial_program`, not a dedicated one — `Mutate::Truncate` is
/// drawn by the same shared `mutate()` strategy every other `Mutate` variant
/// is) must sometimes draw a `Truncate` op across enough samples.
#[test]
fn trivial_program_sometimes_draws_a_truncate() {
    let mut runner = TestRunner::default();
    let strategy = trivial_program();
    let mut coverage = generative::run::Coverage::new();
    for _ in 0..500 {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        coverage.record_program(&program);
    }
    assert!(
        coverage.ops_by_kind.get("Truncate").copied().unwrap_or(0) > 0,
        "coverage floor failed: expected at least one Truncate op across 500 samples:\n{coverage}"
    );
}

/// Improvement-plan task E6's coverage floor, bulk-insert half:
/// `bulk_insert_program`'s row-count dimension must actually get
/// meaningfully large (not just "sometimes more than 1") across its shrink
/// range — sampled the same way as the other floor tests above, checking
/// `Coverage::max_bulk_insert_rows` against a threshold well above
/// `MAX_SEED_ROWS`/`MAX_MUTATES`-scale counts so this floor could only pass
/// if the dimension is real.
#[test]
fn bulk_insert_row_count_gets_meaningfully_large() {
    let mut runner = TestRunner::default();
    let strategy = bulk_insert_program();
    let mut coverage = generative::run::Coverage::new();
    for _ in 0..500 {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        coverage.record_program(&program);
    }
    assert!(
        coverage.max_bulk_insert_rows > 100,
        "coverage floor failed: expected a bulk insert of more than 100 rows across 500 \
         samples:\n{coverage}"
    );
}

// ---------------------------------------------------------------------
// Issue #34: relationship coverage floors (ADR-0006).
// ---------------------------------------------------------------------

/// Accumulates [`generative::run::Coverage`] over `samples` draws of the
/// default strategy — the shape the three relationship floors below share.
/// Uses the same accumulator the live convergence property reports with, so
/// these floors and that report can never disagree about what a shape is
/// called.
fn sampled_coverage(samples: usize) -> generative::run::Coverage {
    let mut runner = TestRunner::default();
    let strategy = trivial_program();
    let mut coverage = generative::run::Coverage::new();
    for _ in 0..samples {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        coverage.record_program(&program);
    }
    coverage
}

/// The generator must actually *declare* relationships, not merely be
/// capable of it. A program needs two or more tables for a relationship to
/// have anywhere to point (see `generate::strategy::rel_field_spec`), so
/// this is a floor on the composition of two independent draws, not one.
#[test]
fn trivial_program_sometimes_declares_a_relationship() {
    let coverage = sampled_coverage(500);
    assert!(
        coverage.relationships_declared > 0,
        "the generator must sometimes declare a relationship across 500 samples: {coverage}"
    );
}

/// Both cardinalities must appear. They are not two spellings of one
/// feature: a to-one relationship resolves through a unique to-side column
/// and reverse-propagates off the to-side's own key, while a to-many one
/// folds many related rows and requires `REPLICA IDENTITY FULL` on the
/// to-side for its non-PK join key (ADR-0006). A run that drew only to-one
/// relationships would leave the whole to-many path untested while still
/// reporting relationship coverage.
#[test]
fn trivial_program_draws_both_relationship_cardinalities() {
    let coverage = sampled_coverage(500);
    for cardinality in ["to_one", "to_many"] {
        assert!(
            coverage
                .relationship_cardinalities
                .get(cardinality)
                .is_some_and(|&n| n > 0),
            "the generator must sometimes declare a {cardinality} relationship across 500 \
             samples: {coverage}"
        );
    }
}

/// Each of the three engine-supported relationship *reference* shapes must
/// be drawn — this is the floor that would catch any one of them silently
/// ceasing to be generated (design doc §3's "coverage that silently drops
/// out is otherwise invisible"):
///
/// * `to_one_bare` — a bare `<rel>.<col>` enrichment on a row-grain def;
/// * `to_many_in_aggregate` — an aggregate over a to-many path on a
///   row-grain def;
/// * `to_one_in_aggregate_def` — an aggregate over a to-one path inside a
///   `GROUP BY` def, the newest of the three.
///
/// `other` must stay at zero: it is the accumulator's catch-all for a shape
/// nobody taught it about, which in this generator can only mean a shape the
/// engine's validator would have rejected.
#[test]
fn trivial_program_draws_every_supported_relationship_reference_shape() {
    let coverage = sampled_coverage(500);
    for shape in [
        "to_one_bare",
        "to_many_in_aggregate",
        "to_one_in_aggregate_def",
    ] {
        assert!(
            coverage
                .relationship_shapes
                .get(shape)
                .is_some_and(|&n| n > 0),
            "the generator must sometimes draw the {shape} relationship shape across 500 \
             samples: {coverage}"
        );
    }
    assert_eq!(
        coverage.relationship_shapes.get("other").copied(),
        None,
        "the generator drew a relationship reference shape the coverage accumulator cannot \
         classify — in this generator that can only be a shape validate() rejects: {coverage}"
    );
}

/// The relationship key/foreign-key columns must really exercise all three
/// join outcomes ADR-0006 distinguishes, not just the matching one: a
/// foreign key that resolves, one that resolves to nothing, and a `NULL`
/// one. The two miss cases are the entire nullability contract (issue #33),
/// so a run that only ever drew matching keys would check none of it.
#[test]
fn trivial_program_draws_matching_missing_and_null_relationship_foreign_keys() {
    let mut runner = TestRunner::default();
    let strategy = trivial_program();
    let mut saw_match = false;
    let mut saw_miss = false;
    let mut saw_null = false;
    for _ in 0..500 {
        let program = strategy
            .new_tree(&mut runner)
            .expect("strategy must produce a value")
            .current();
        for table in &program.tables {
            let fk_col = &table.columns[generative::generate::REL_FK_COLUMN].name;
            for op in &program.ops {
                let Op::Insert { row, .. } = op else { continue };
                let Some((_, value)) = row.iter().find(|(c, _)| c == fk_col) else {
                    continue;
                };
                match value.as_deref() {
                    None => saw_null = true,
                    // `rel_key_value` of a seeded pk; pks run from 1, and no
                    // program seeds anywhere near 9 rows.
                    Some("k9") => saw_miss = true,
                    Some(_) => saw_match = true,
                }
            }
        }
    }
    assert!(
        saw_match && saw_miss && saw_null,
        "relationship foreign keys must cover all three join outcomes across 500 samples \
         (matching={saw_match}, missing={saw_miss}, null={saw_null})"
    );
}
