//! Issue #234 (layer-3 bucket 1 remainder, epic #238): drive **two Trellis
//! instances side by side**, each running its own independent program, each
//! converging against its **own** oracle.
//!
//! This is the bug class a single-instance property structurally cannot see.
//! Every other property in this crate stands up exactly one engine against
//! exactly one database, so nothing it does can ever observe a resource one
//! instance was supposed to own privately but actually shares with another —
//! a hardcoded object name that isn't really schema-qualified, an advisory
//! lock keyed by a global constant rather than by the instance, a capture
//! trigger whose name collides, a global registry that isn't
//! instance-scoped. A run with one instance in it has nothing to interfere
//! with, so it is green by construction on every one of those.
//!
//! # What makes this an *interaction* test rather than two unrelated runs
//!
//! Running two instances that never overlap in time would prove nothing. This
//! runner deliberately makes them overlap on every axis it can:
//!
//! * **Both engines are live for the whole run.** Both are installed before
//!   either applies its first op, and neither is torn down until the run
//!   ends, so every op of either program is applied while the other
//!   instance's staging worker (capture reconcile included), maintenance
//!   loop, and application workers are all running.
//! * **Ops are interleaved, not batched per instance.** Step `n` applies
//!   instance A's op `n` *and* instance B's op `n` before *either* instance
//!   is asked to quiesce, so at the moment the harness asks for convergence
//!   both instances have work genuinely in flight at once, through the same
//!   Postgres WAL.
//! * **They are as close together as the isolation mechanism allows.** The
//!   intended caller (`generative/tests/two_instance_noise.rs`) puts both
//!   instances in the *same database* of the same cluster, separated only by
//!   `docs/instance-identity.md`'s named-schema mechanism — the tightest
//!   coexistence that document claims to support, and therefore the one with
//!   the most shared surface to get wrong.
//!
//! # Each instance is checked against its own oracle, independently
//!
//! [`check_program`] is run once per instance at every checkpoint, against that
//! instance's own [`Pool`] (whose `search_path` is pinned to that instance's
//! own schema) and that instance's own [`Program`]. A divergence carries the
//! [`InstanceLabel`] it was found on, and the runner returns on the first
//! one — so a divergence on A can never be masked by B being fine, or vice
//! versa. There is deliberately no combined/merged comparison anywhere in
//! this file: two instances that are correctly isolated have two entirely
//! separate correctness stories, and merging them would be exactly the way to
//! let one hide the other.

use std::collections::HashMap;
use std::fmt;

use trellis::Pool;
use trellis::dev::defs::ast::{
    Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType,
};

use crate::backend::Backend;
use crate::model::{Column, OpOutcome, PRIMARY_KEY_VALUE_TYPE, Program, Table};
use crate::oracle;

use super::{Divergence, Outcome, RunError, check_program};

/// Which of the two side-by-side instances something happened on. Carried on
/// every error this module produces so a failure message never leaves the
/// reader guessing which instance actually diverged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceLabel {
    A,
    B,
}

impl fmt::Display for InstanceLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InstanceLabel::A => write!(f, "A"),
            InstanceLabel::B => write!(f, "B"),
        }
    }
}

/// A [`RunError`] tagged with the instance it happened on.
///
/// Deliberately a wrapper around the existing [`RunError`] rather than a
/// parallel error enum: every failure mode a single-instance run can hit is
/// still exactly a failure mode here, and the *only* new information a
/// two-instance run adds is which instance it was.
#[derive(Debug)]
pub struct TwoInstanceError {
    pub instance: InstanceLabel,
    pub error: RunError,
}

impl fmt::Display for TwoInstanceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "instance {}: {:?}", self.instance, self.error)
    }
}

/// One side of a two-instance run: the backend driving it, the oracle's read
/// handle into *that instance's own* schema, and the program it runs.
///
/// The two sides are independent in every respect — different generated
/// programs, different schemas, different pools — which is the point: nothing
/// about either one is supposed to be visible to the other.
pub struct InstanceRun<'a, B: Backend> {
    pub label: InstanceLabel,
    pub backend: &'a mut B,
    /// The oracle's read handle into this instance's schema. Must have its
    /// `search_path` pinned there (build it from a
    /// `trellis::Config::with_schema` carrying this instance's schema, not
    /// from `Config::from_dsn`, which resolves a single process-global
    /// `TRELLIS_SCHEMA` and so cannot give two in-process instances two
    /// different schemas).
    pub pool: &'a Pool,
    pub program: &'a Program,
}

impl<B: Backend> InstanceRun<'_, B> {
    /// Applies this instance's op at `op_index`, checking its actual outcome
    /// against what the generator expected of it — the same "operation errors
    /// are checked, not swallowed" discipline
    /// [`super::run_convergence`] applies, just tagged with an instance.
    ///
    /// A no-op when this instance's program has already run out of ops (the
    /// two programs are independently generated, so they are almost never the
    /// same length — the shorter instance simply goes quiet and stays live,
    /// which is itself a useful state to hold the other instance against).
    async fn apply_step(&mut self, op_index: usize) -> Result<(), TwoInstanceError> {
        let Some(op) = self.program.ops.get(op_index) else {
            return Ok(());
        };
        let actual = match self.backend.apply(op).await {
            Err(_) => OpOutcome::Fails,
            Ok(0) => OpOutcome::AffectsNoRows,
            Ok(_) => OpOutcome::Succeeds,
        };
        let expected = op.expect();
        if !expected.matches(&actual) {
            return Err(self.tag(RunError::UnexpectedOpOutcome {
                op_index,
                expected: expected.clone(),
                actual,
            }));
        }
        Ok(())
    }

    /// Quiesces this instance, snapshots it, and runs the three-way oracle
    /// check for its own program against its own pool — reporting any
    /// divergence localized to `op_index` and tagged with this instance.
    async fn quiesce_and_check(&mut self, op_index: usize) -> Result<(), TwoInstanceError> {
        self.backend
            .quiesce()
            .await
            .map_err(|e| self.tag(RunError::Quiesce(format!("{e:?}"))))?;

        let snapshot = self
            .backend
            .snapshot()
            .await
            .map_err(|e| self.tag(RunError::Snapshot(format!("{e:?}"))))?;

        let checked = check_program(self.pool, self.program, &snapshot)
            .await
            .map_err(|e| self.tag(RunError::Oracle(e)))?;
        if let Some(divergence) = Divergence::from_checked(op_index, checked) {
            return Err(self.tag(RunError::Diverged(divergence)));
        }
        Ok(())
    }

    fn tag(&self, error: RunError) -> TwoInstanceError {
        TwoInstanceError {
            instance: self.label,
            error,
        }
    }
}

/// Installs both instances, then interleaves their op streams step by step,
/// checking each instance against its own oracle at up to `max_checks`
/// points spread across the run (always including the last step).
///
/// The ordering within one step is deliberate and load-bearing:
///
/// 1. **A's op, then B's op** — both applied before anything quiesces, so
///    both instances have uncommitted-to-the-target work in flight at the
///    same time rather than taking strict turns.
/// 2. **A quiesces and is checked, then B quiesces and is checked.** A's
///    check runs before B is even asked to quiesce, so A converging cannot
///    depend on B having been given a chance to catch up first — if A only
///    settles once B's workers have drained, that is precisely the
///    cross-instance coupling this property exists to catch, and it shows up
///    here as A timing out or diverging rather than being papered over by a
///    "quiesce everything, then check everything" shape.
///
/// # Why `max_checks`, instead of checking after every single step
///
/// [`super::run_convergence`] checks after every op, and does so cheaply,
/// because a single instance's `quiesce()` almost always returns as soon as
/// its own commit has drained. The bounded number of checks here was sized
/// when a co-tenant instance's writes made every quiesce wait out a
/// replication keepalive (~10s). Trigger capture (#622) has no such wait,
/// and the bound stays until someone decides per-op localization is worth
/// the extra quiesces.
///
/// **This makes divergence localization coarser**, exactly like
/// [`super::run_convergence_bursty`]'s: a divergence is only known to have
/// appeared somewhere within the window since the previous check, and the
/// [`Divergence::op_index`] reported is that window's *last* step. Follow
/// the same convention that file's module doc comment sets out — treat a
/// shrunk case as advisory and hand-transcribe it into a `build_program`
/// pin — before trusting a failure here as a minimal repro.
///
/// Returns `Ok(Outcome::Ran)` once both programs are exhausted — whether or
/// not the property held, since a divergence is a separate, still-fatal
/// condition surfaced via `Err`. Same contract as
/// [`super::run_convergence`]: there is no path to an `Ok` outside having
/// driven both backends through their whole programs *and* checked both at
/// least once.
///
/// Panics if `max_checks` is `0`: a run that never checks anything would
/// report `Outcome::Ran` having proven nothing, which is precisely the
/// "green run that is not evidence" [`Outcome`] exists to rule out.
pub async fn run_two_instance_convergence<A: Backend, B: Backend>(
    mut a: InstanceRun<'_, A>,
    mut b: InstanceRun<'_, B>,
    max_checks: usize,
) -> Result<Outcome, TwoInstanceError> {
    assert!(
        max_checks > 0,
        "run_two_instance_convergence: max_checks must be at least 1"
    );
    // Task E2's deferred definition installs and E3's scheduled
    // restarts/scale-outs are deliberately *not* supported here, and are
    // refused loudly rather than silently ignored — a program carrying one
    // would otherwise be quietly under-tested (every definition installed up
    // front, every lifecycle event dropped) while still reporting
    // `Outcome::Ran`. `trivial_program`, the only strategy this runner is
    // driven with today, never produces either; layering them on top of two
    // live instances is a coherent follow-up, not something to acquire by
    // accident.
    reject_unsupported_schedule(&a)?;
    reject_unsupported_schedule(&b)?;

    // Both instances stand up before either applies an op, so every op below
    // runs against a cluster where *both* engines are live. This is also the
    // first point a shared-resource collision can surface at all: if the two
    // instances contend for something neither is supposed to own globally
    // (a trigger name, an advisory lock), B's install is where
    // that shows up — and in fact is exactly where both of the bugs #234
    // found did show up.
    a.backend
        .install(a.program)
        .await
        .map_err(|e| a.tag(RunError::Install(format!("{e:?}"))))?;
    b.backend
        .install(b.program)
        .await
        .map_err(|e| b.tag(RunError::Install(format!("{e:?}"))))?;

    let steps = a.program.ops.len().max(b.program.ops.len());
    if steps == 0 {
        // Neither program has an op (nothing `trivial_program` draws, but a
        // hand-built `build_program(&[], &[])` would). The freshly-installed
        // state is still a state both oracles have an opinion about, so it
        // is still checked — `Outcome::Ran` must never mean "checked
        // nothing".
        a.quiesce_and_check(0).await?;
        b.quiesce_and_check(0).await?;
        return Ok(Outcome::Ran);
    }

    // Spread `max_checks` checks over `steps` steps. `div_ceil` rounds the
    // stride *up*, so the number of checks never exceeds `max_checks`; the
    // final step always checks regardless, so a run always ends on a check
    // no matter how the arithmetic lands.
    let stride = steps.div_ceil(max_checks).max(1);
    for op_index in 0..steps {
        a.apply_step(op_index).await?;
        b.apply_step(op_index).await?;

        let is_last = op_index + 1 == steps;
        if is_last || (op_index + 1) % stride == 0 {
            a.quiesce_and_check(op_index).await?;
            b.quiesce_and_check(op_index).await?;
        }
    }

    Ok(Outcome::Ran)
}

/// Refuses a program whose schedule this runner does not implement — see the
/// call site in [`run_two_instance_convergence`] for why this is a loud
/// refusal rather than a silent no-op.
fn reject_unsupported_schedule<B: Backend>(
    run: &InstanceRun<'_, B>,
) -> Result<(), TwoInstanceError> {
    let program = run.program;
    if program.def_install_after_op.iter().any(|at| *at != 0) {
        return Err(run.tag(RunError::Install(
            "two-instance runs do not implement task E2's mid-stream definition installs \
             (`def_install_after_op`)"
                .to_string(),
        )));
    }
    if !program.restart_after_ops.is_empty() || !program.scale_out_after_ops.is_empty() {
        return Err(run.tag(RunError::Install(
            "two-instance runs do not implement task E3's scheduled restarts/scale-outs \
             (`restart_after_ops`/`scale_out_after_ops`)"
                .to_string(),
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The shapes planned for production use (issue #879, epic #806)
// ---------------------------------------------------------------------------
//
// [`run_two_instance_convergence`] already holds two programs side by side and
// checks each against its own oracle. It does not care whether the programs
// share a source table, or whether one reads the other's target: that is
// decided by what the two programs *declare* and by where each backend puts
// their tables ([`crate::backend::SourceTables`]). These functions build the
// program pairs for the two cross-instance shapes, so the harness can hand
// them to the same runner.

/// What `run_two_instance_convergence` needs from a program that is not the
/// one writing: nothing scheduled, so every definition installs up front and
/// no op, restart or scale-out is left for the other side to run.
fn read_only_program(
    tables: Vec<Table>,
    relationships: Vec<crate::model::Relationship>,
    defs: Vec<TransformDef>,
) -> Program {
    Program {
        def_install_after_op: vec![0; defs.len()],
        tables,
        relationships,
        defs,
        ops: Vec::new(),
        restart_after_ops: Vec::new(),
        scale_out_after_ops: Vec::new(),
    }
}

/// Splits `program` between two instances that share its source tables.
///
/// The first program is `program` with the first half of its definitions and
/// every op: its instance creates the tables and writes them. The second is
/// the second half of the definitions over the same tables and no ops: its
/// instance only reads. The halves overlap by one definition when there is an
/// odd number of them, and with a single definition both instances define it,
/// so every program has at least one definition and the two instances
/// capture at least one table in common.
///
/// Each instance is checked against the oracle over the one set of source
/// tables, so a write one instance's capture mishandles, or a trigger of one
/// that disturbs the other's, shows up as a divergence on one side only.
///
/// `program` must not schedule anything the runner refuses
/// ([`run_two_instance_convergence`]).
pub fn share_source(program: &Program) -> (Program, Program) {
    let n = program.defs.len();
    assert!(n > 0, "share_source needs a definition to give each side");
    let mut writer = program.clone();
    writer.defs.truncate(n.div_ceil(2));
    writer.def_install_after_op.truncate(n.div_ceil(2));
    let reader = read_only_program(
        program.tables.clone(),
        program.relationships.clone(),
        program.defs[n / 2..].to_vec(),
    );
    (writer, reader)
}

/// The second instance's program for a chain: it reads the first instance's
/// one-to-one targets as its source tables and has nothing of its own to
/// write. `None` when the first program defines no one-to-one transform. An
/// aggregate target can't be read by another instance (it has no primary key
/// to capture by, issue #376), so those are left out.
///
/// Each source table is modelled from the definition that builds it: the
/// source's primary key column, then one column per field, typed as the
/// oracle types the field. The second instance's definition for it copies
/// every field, and adds `<field>_twice` (`field + field`) for the first
/// numeric one, so its target holds both values read straight off the first
/// instance's target and a value computed from it. Its targets are named
/// `e0`, `e1`, ... in the order of the first program's definitions.
///
/// The first instance must keep its targets where the second instance's
/// `search_path` finds them by bare name (`public`).
pub fn chain_off(program: &Program) -> Option<Program> {
    let mut tables = Vec::new();
    let mut defs = Vec::new();
    for def in &program.defs {
        if def.key_space != KeySpace::OneToOne {
            continue;
        }
        let source = program
            .tables
            .iter()
            .find(|t| t.name == def.source)
            .unwrap_or_else(|| {
                panic!("definition reads {:?}, which the program lacks", def.source)
            });
        let source_columns: HashMap<String, ValueType> = source
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.value_type))
            .collect();
        let field_types = oracle::field_types(program, def, &source_columns);

        let mut columns = vec![Column {
            name: source.pk_col.clone(),
            value_type: PRIMARY_KEY_VALUE_TYPE,
        }];
        columns.extend(field_types.iter().map(|(name, value_type)| Column {
            name: name.clone(),
            value_type: *value_type,
        }));
        let mut fields: Vec<FieldDef> = field_types
            .iter()
            .map(|(name, _)| FieldDef {
                name: name.clone(),
                expr: Expr::Column(name.clone()),
            })
            .collect();
        if let Some((name, _)) = field_types
            .iter()
            .find(|(_, value_type)| *value_type == ValueType::Numeric)
        {
            fields.push(FieldDef {
                name: format!("{name}_twice"),
                expr: Expr::BinaryOp {
                    op: Operator::Add,
                    lhs: Box::new(Expr::Column(name.clone())),
                    rhs: Box::new(Expr::Column(name.clone())),
                },
            });
        }
        defs.push(TransformDef {
            target: format!("e{}", defs.len()),
            explicit_target_schema: None,
            source: def.target.clone(),
            explicit_source_schema: None,
            key_space: KeySpace::OneToOne,
            fields,
            predicate: Predicate::True,
        });
        tables.push(Table {
            name: def.target.clone(),
            pk_col: source.pk_col.clone(),
            columns,
            unique_cols: Vec::new(),
        });
    }
    (!defs.is_empty()).then(|| read_only_program(tables, Vec::new(), defs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate::{Mutate, TableSpec, build_program, build_program_multi};

    fn seeds() -> Vec<(Option<i64>, Option<i64>)> {
        vec![(Some(1), Some(2)), (Some(3), None)]
    }

    #[test]
    fn a_shared_source_goes_to_both_sides_and_only_the_first_writes() {
        let program = build_program(&seeds(), &[Mutate::Delete { pk: 1 }]);
        let (writer, reader) = share_source(&program);
        assert_eq!(writer.tables, reader.tables);
        assert_eq!(writer.ops, program.ops);
        assert!(reader.ops.is_empty());
        // One definition: both define it.
        assert_eq!(writer.defs, program.defs);
        assert_eq!(reader.defs, program.defs);
        assert_eq!(reader.def_install_after_op, vec![0]);
    }

    #[test]
    fn the_definitions_split_with_one_in_common_when_odd() {
        let spec = || TableSpec::numeric_only(seeds(), Vec::new());
        let program = build_program_multi(&[spec()], &[0, 0, 0]);
        let (writer, reader) = share_source(&program);
        assert_eq!(writer.defs, program.defs[..2]);
        assert_eq!(reader.defs, program.defs[1..]);
        assert_eq!(writer.def_install_after_op.len(), writer.defs.len());
        let program = build_program_multi(&[spec()], &[0, 0]);
        let (writer, reader) = share_source(&program);
        assert_eq!(writer.defs, program.defs[..1]);
        assert_eq!(reader.defs, program.defs[1..]);
    }

    #[test]
    fn a_chain_reads_the_first_programs_target_as_a_table_of_its_own() {
        let program = build_program(&seeds(), &[]);
        let def = &program.defs[0];
        let chained = chain_off(&program).expect("the program has a one-to-one definition");
        assert!(chained.ops.is_empty());
        let [table] = &chained.tables[..] else {
            panic!("one source table: {:?}", chained.tables)
        };
        let [read] = &chained.defs[..] else {
            panic!("one definition: {:?}", chained.defs)
        };
        assert_eq!(table.name, def.target);
        assert_eq!(read.source, def.target);
        // The primary key, then every field of the first program's definition.
        assert_eq!(table.columns.len(), 1 + def.fields.len());
        assert_eq!(table.columns[0].name, table.pk_col);
        // Every field is copied, and the numeric one is doubled.
        let copied: Vec<&str> = read.fields.iter().map(|f| f.name.as_str()).collect();
        assert!(copied.contains(&"total"), "{copied:?}");
        assert!(copied.contains(&"total_twice"), "{copied:?}");
    }
}
