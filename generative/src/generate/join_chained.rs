//! The chained-reader tier's programs (#1488): a `JOIN` one of whose sides is
//! another definition's target in the same instance, so the target-mutation
//! seam feeds it (`trellis::staging::target_mutations`: the image feed and
//! the write token), rather than capture.
//!
//! A case is a case of the aggregate `JOIN` tier ([`super::join`]) or of the
//! 1-1 `JOIN` tier ([`super::join_one_to_one`]), over the same two tables
//! and op stream, with one side moved one hop downstream. An **upstream**, a
//! plain 1-1 definition, reads that side's table (`ja` or `jb`), passes every
//! column through but one, and computes that one ([`Computed`]): the join key
//! (`k`, or `t` beside a 1-1 `JOIN`'s chained `b`) or the value column (`x` or
//! `y`), as `<column> + n` or `COALESCE(<column>, n)` under the name
//! `<column>1`. Every `JOIN` reads the upstream's target (`ua` for `ja`, `ub`
//! for `jb`) in place of the table, and its computed column in place of the
//! column. A 1-1 `JOIN`'s `b` side is joined on columns that include its
//! primary key, as define requires of a 1-1 target there (#1482); an `a` side
//! and either side of an aggregate `JOIN` on any of the tier's `ON` shapes.
//!
//! The op stream is the tier's own, so it writes both tables (the upstream's
//! source among them) with key moves and `TRUNCATE`s, plus primary-key
//! updates of the upstream's source ([`JoinOpKind::Rekey`]), which reach the
//! `JOIN` as a delete and an insert through the seam. A burst that rekeys a
//! row issues all of that table's ops from one lane, so no lane writes the
//! new id before the rekey commits.
//!
//! The bursts after the first pause and resume the upstream (a `RESUME`
//! rebuilds it, which bypasses the seam, and so rebuilds the `JOIN`s that
//! read it) and redefine it (`ALTER TRANSFORM … ALTER <column>1 AS …`, a field
//! build that rewrites every row through the seam), besides the tier's own
//! installs, pauses and re-reads ([`super::join::add_join_actions`], which
//! may also pick the upstream). An upstream pause never spans an install of
//! a `JOIN` (define refuses a side whose upstream isn't `live`), and an
//! `ALTER` never lands inside an upstream pause.
//!
//! The oracle of each `JOIN` is its query written over the upstream's query
//! (`crate::oracle::chained_relation`), once both have converged, and the
//! upstream is checked against its own query.

use trellis::dev::defs::ast::TransformDef;

use super::join::{JoinBuildDraws, JoinCase, JoinDefSpec, JoinOpDraw, KeyShape};
use super::join_one_to_one::{OnShape, OneToOneOpDraw, OneToOneSpec};
use crate::model::{BurstAction, ConcurrentPlan, Op, Program, TimedAction};

/// The upstream's target when it reads the `FROM` table.
pub const UPSTREAM_OF_FROM: &str = "ua";
/// The upstream's target when it reads the joined table.
pub const UPSTREAM_OF_JOINED: &str = "ub";

/// Which of the tiers a chained case is drawn from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainedKind {
    /// `JOIN … GROUP BY` ([`super::join`]).
    Aggregate,
    /// 1-1 `JOIN` ([`super::join_one_to_one`]).
    OneToOne,
}

/// How the upstream computes its computed column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Form {
    /// `<column> + n`: every key or value moves by `n`.
    Plus(u8),
    /// `COALESCE(<column>, n)`: a `NULL` becomes `n`, so a row with no key
    /// joins key `n`.
    Coalesce(u8),
}

impl Form {
    fn text(self, column: &str) -> String {
        match self {
            Form::Plus(n) => format!("{column} + {n}"),
            Form::Coalesce(n) => format!("COALESCE({column}, {n})"),
        }
    }
}

/// The upstream's computed column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Computed {
    /// The join key column rather than the value column.
    pub key: bool,
    pub form: Form,
}

/// What a [`ChainActionDraw`] does to the upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainActionKind {
    /// `PAUSE TRANSFORM`, then `RESUME TRANSFORM` later in the burst: a
    /// rebuild of the upstream that bypasses the seam.
    PauseResume,
    /// `ALTER TRANSFORM <upstream> ALTER <column>1 AS <form>`.
    Alter(Form),
}

/// One drawn action on the upstream, before [`add_chain_actions`] resolves
/// it. Plain numbers, as [`super::join::JoinActionDraw`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChainActionDraw {
    pub kind: ChainActionKind,
    /// Picks a burst after the first, modulo how many there are.
    pub burst: usize,
    /// Where in the burst the action (or the pause) fires, in thousandths of
    /// the burst's ops.
    pub start: u16,
    /// How much further on a resume fires, in thousandths, capped at the
    /// burst's end.
    pub span: u16,
}

/// The chain a case draws: which side the upstream feeds, what it computes,
/// and what the bursts do to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainSpec {
    /// The upstream reads the joined table (`jb`) rather than the `FROM`
    /// table (`ja`).
    pub joined: bool,
    pub computed: Computed,
    pub actions: Vec<ChainActionDraw>,
}

/// The column of a side's table `computed` names.
fn computed_column(kind: ChainedKind, joined: bool, computed: Computed) -> &'static str {
    match (kind, joined, computed.key) {
        (_, false, false) => "x",
        (_, true, false) => "y",
        (ChainedKind::Aggregate, _, true) | (ChainedKind::OneToOne, false, true) => "k",
        // A 1-1 `JOIN`'s chained `b` is joined on its primary key, and `t`
        // is the other column of `PkPlusT`.
        (ChainedKind::OneToOne, true, true) => "t",
    }
}

/// The upstream's definition text, computing its column with `form`.
pub fn upstream_text(
    kind: ChainedKind,
    tables: &[crate::model::Table],
    chain: &ChainSpec,
    form: Form,
) -> String {
    let (source, target) = if chain.joined {
        (super::join::JOINED_TABLE, UPSTREAM_OF_JOINED)
    } else {
        (super::join::FROM_TABLE, UPSTREAM_OF_FROM)
    };
    let table = tables
        .iter()
        .find(|t| t.name == source)
        .expect("the tier declares both tables");
    let computed = computed_column(kind, chain.joined, chain.computed);
    let select: Vec<String> = table
        .columns
        .iter()
        .filter(|c| c.name != table.pk_col)
        .map(|c| {
            if c.name == computed {
                format!("{} AS {computed}1", form.text(computed))
            } else {
                format!("{0} AS {0}", c.name)
            }
        })
        .collect();
    format!(
        "TRANSFORM {target} FROM {source} SELECT {}",
        select.join(", ")
    )
}

fn parse(text: &str) -> TransformDef {
    trellis::dev::defs::parse(text).unwrap_or_else(|e| panic!("{text}: {e:?}"))
}

/// `text`, a `JOIN` over `ja` and `jb`, read over the upstream: every
/// `<source>` (the table name, and the qualifier of a column) becomes the
/// upstream's target, and `<source>.<computed>` its computed column.
fn chained_join_text(text: &str, source: &str, target: &str, computed: &str) -> String {
    let mut out = String::with_capacity(text.len() + 8);
    let mut previous: Option<String> = None;
    let mut rest = text;
    while !rest.is_empty() {
        let word = rest
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .unwrap_or(rest.len());
        if word == 0 {
            let c = rest.chars().next().expect("non-empty");
            out.push(c);
            rest = &rest[c.len_utf8()..];
            if c != '.' {
                previous = None;
            }
            continue;
        }
        let token = &rest[..word];
        rest = &rest[word..];
        let qualified = out.ends_with(&format!("{target}.")) && previous.as_deref() == Some(source);
        if token == source {
            out.push_str(target);
        } else if qualified && token == computed {
            out.push_str(computed);
            out.push('1');
        } else {
            out.push_str(token);
        }
        previous = Some(token.to_string());
    }
    out
}

/// `program`, built by its tier over `ja` and `jb`, with the chain: the
/// upstream first, installed up front, and every `JOIN` (`texts`, in order)
/// reading its target.
fn chain_program(
    kind: ChainedKind,
    mut program: Program,
    texts: &[String],
    chain: &ChainSpec,
) -> Program {
    let (source, target) = if chain.joined {
        (super::join::JOINED_TABLE, UPSTREAM_OF_JOINED)
    } else {
        (super::join::FROM_TABLE, UPSTREAM_OF_FROM)
    };
    let computed = computed_column(kind, chain.joined, chain.computed);
    let upstream = parse(&upstream_text(
        kind,
        &program.tables,
        chain,
        chain.computed.form,
    ));
    let mut defs = vec![upstream];
    defs.extend(
        texts
            .iter()
            .map(|text| parse(&chained_join_text(text, source, target, computed))),
    );
    program.defs = defs;
    program.def_install_after_op.insert(0, 0);
    program
}

/// Whether `op` updates its row's primary key.
pub fn is_rekey(op: &Op) -> bool {
    matches!(op, Op::Update { changes, .. } if changes.iter().any(|(c, _)| c == "id"))
}

/// `plan` with all of a burst's ops on `table` on one lane, in program
/// order, in every burst that rekeys a row of `table`: a lane keyed by the
/// old id could otherwise race one keyed by the new id.
pub fn one_lane_where_rekeyed(
    mut plan: ConcurrentPlan,
    program: &Program,
    table: &str,
) -> ConcurrentPlan {
    let on_table = |index: usize| program.ops[index].table() == table;
    for burst in &mut plan.bursts {
        if !burst
            .lanes
            .iter()
            .flatten()
            .any(|&i| on_table(i) && is_rekey(&program.ops[i]))
        {
            continue;
        }
        let Some(target) = burst
            .lanes
            .iter()
            .position(|lane| lane.iter().any(|&i| on_table(i)))
        else {
            continue;
        };
        let mut moved = Vec::new();
        for (n, lane) in burst.lanes.iter_mut().enumerate() {
            if n != target {
                lane.retain(|&i| {
                    if on_table(i) {
                        moved.push(i);
                        false
                    } else {
                        true
                    }
                });
            }
        }
        burst.lanes[target].extend(moved);
        burst.lanes[target].sort_unstable();
        burst.lanes.retain(|lane| !lane.is_empty());
    }
    plan
}

/// Adds `chain.actions` on the upstream (`program.defs[0]`) to `plan`'s
/// bursts after the first, then drops what can't apply:
///
/// - an upstream pause whose span holds an install (the `JOIN`'s define
///   would find its side's upstream paused), with its resume;
/// - an `ALTER` inside an upstream pause;
/// - a second pause of the upstream in one burst.
///
/// An `ALTER`'s `def` is the upstream as it leaves it, after every earlier
/// `ALTER` in plan order.
pub fn add_chain_actions(
    kind: ChainedKind,
    mut plan: ConcurrentPlan,
    program: &Program,
    chain: &ChainSpec,
) -> ConcurrentPlan {
    if plan.bursts.len() < 2 {
        return plan;
    }
    let upstream = program.defs[0].target.clone();
    let field = format!("{}1", computed_column(kind, chain.joined, chain.computed));
    let mut draws: Vec<(usize, usize, ChainActionDraw)> = chain
        .actions
        .iter()
        .map(|draw| {
            let burst = 1 + draw.burst % (plan.bursts.len() - 1);
            let ops = plan.bursts[burst].ops().len();
            (burst, usize::from(draw.start.min(1000)) * ops / 1000, *draw)
        })
        .collect();
    draws.sort_by_key(|(burst, at, _)| (*burst, *at));
    for (burst, at, draw) in draws {
        let ops = plan.bursts[burst].ops().len();
        let actions = &mut plan.bursts[burst].actions;
        let pauses: Vec<(usize, usize)> = upstream_pauses(actions, &upstream);
        match draw.kind {
            ChainActionKind::PauseResume => {
                let end = (at + usize::from(draw.span.min(1000)) * ops / 1000).min(ops);
                let spans_install = actions.iter().any(|a| {
                    matches!(a.action, BurstAction::Install { .. }) && (at..=end).contains(&a.after)
                });
                let spans_alter = actions.iter().any(|a| {
                    matches!(&a.action, BurstAction::Alter { .. }) && (at..=end).contains(&a.after)
                });
                if spans_install || spans_alter || !pauses.is_empty() {
                    continue;
                }
                actions.push(TimedAction {
                    after: at,
                    action: BurstAction::Pause {
                        target: upstream.clone(),
                        column: None,
                    },
                });
                actions.push(TimedAction {
                    after: end,
                    action: BurstAction::Resume {
                        target: upstream.clone(),
                        column: None,
                    },
                });
            }
            ChainActionKind::Alter(form) => {
                if pauses.iter().any(|&(from, to)| (from..=to).contains(&at)) {
                    continue;
                }
                actions.push(TimedAction {
                    after: at,
                    action: BurstAction::Alter {
                        // Filled in below, in plan order.
                        def: Box::new(program.defs[0].clone()),
                        field: field.clone(),
                        expr: form.text(computed_column(kind, chain.joined, chain.computed)),
                    },
                });
            }
        }
        actions.sort_by_key(|a| a.after);
    }
    // Pauses the tier's own actions put on the upstream that span an install.
    for burst in &mut plan.bursts {
        let installs: Vec<usize> = burst
            .actions
            .iter()
            .filter(|a| matches!(a.action, BurstAction::Install { .. }))
            .map(|a| a.after)
            .collect();
        let spans: Vec<(usize, usize)> = upstream_pauses(&burst.actions, &upstream)
            .into_iter()
            .filter(|&(from, to)| installs.iter().any(|at| (from..=to).contains(at)))
            .collect();
        for (from, to) in spans {
            burst.actions.retain(|a| match &a.action {
                BurstAction::Pause { target, .. } => !(target == &upstream && a.after == from),
                BurstAction::Resume { target, .. } => !(target == &upstream && a.after == to),
                _ => true,
            });
        }
    }
    // Each `ALTER`'s definition, in plan order.
    let tables = program.tables.clone();
    for burst in &mut plan.bursts {
        for timed in &mut burst.actions {
            if let BurstAction::Alter { def, expr, .. } = &mut timed.action {
                let form = form_of(expr);
                **def = parse(&upstream_text(kind, &tables, chain, form));
            }
        }
    }
    plan
}

/// `plan` without the re-reads of a table no definition installed in an
/// earlier burst reads: the tier's re-reads pick a table, and only the
/// `JOIN`s read the table the upstream doesn't, so before the first of them
/// is installed that table isn't captured (`request_backfill` refuses it).
fn drop_uncaptured_rereads(mut plan: ConcurrentPlan, program: &Program) -> ConcurrentPlan {
    let mut installed_in = vec![0; program.defs.len()];
    for (index, burst) in plan.bursts.iter().enumerate() {
        for timed in &burst.actions {
            if let BurstAction::Install { def } = timed.action {
                installed_in[def] = index;
            }
        }
    }
    let reads = |def: &TransformDef, table: &str| {
        def.source == table
            || match &def.key_space {
                trellis::dev::defs::ast::KeySpace::Join(join) => join.joins[0].right == table,
                trellis::dev::defs::ast::KeySpace::JoinOneToOne(join) => {
                    join.joins[0].right == table
                }
                _ => false,
            }
    };
    for (index, burst) in plan.bursts.iter_mut().enumerate() {
        burst.actions.retain(|timed| match &timed.action {
            BurstAction::RequestBackfill { table } => program
                .defs
                .iter()
                .zip(&installed_in)
                .any(|(def, &at)| at < index && reads(def, table)),
            _ => true,
        });
    }
    plan
}

/// The `(pause, resume)` points of every pause of `upstream` in `actions`.
fn upstream_pauses(actions: &[TimedAction], upstream: &str) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut open = None;
    for timed in actions {
        match &timed.action {
            BurstAction::Pause { target, .. } if target == upstream => open = Some(timed.after),
            BurstAction::Resume { target, .. } if target == upstream => {
                if let Some(from) = open.take() {
                    out.push((from, timed.after));
                }
            }
            _ => {}
        }
    }
    out
}

/// The [`Form`] an `ALTER`'s expression text spells.
fn form_of(expr: &str) -> Form {
    if let Some(rest) = expr.strip_prefix("COALESCE(") {
        let n = rest
            .rsplit(", ")
            .next()
            .and_then(|n| n.trim_end_matches(')').parse().ok())
            .expect("a COALESCE form");
        Form::Coalesce(n)
    } else {
        let n = expr
            .rsplit(" + ")
            .next()
            .and_then(|n| n.parse().ok())
            .expect("a plus form");
        Form::Plus(n)
    }
}

/// An aggregate `JOIN` case ([`super::join::join_case`]) with `chain`.
/// `draws` may hold [`JoinOpKind::Rekey`]s on the upstream's table.
///
/// [`JoinOpKind::Rekey`]: super::join::JoinOpKind::Rekey
#[allow(clippy::too_many_arguments)]
pub fn aggregate_chained_case(
    specs: &[JoinDefSpec],
    chain: &ChainSpec,
    shape: KeyShape,
    draws: &[JoinOpDraw],
    builds: &JoinBuildDraws,
    burst_size: usize,
    lanes: usize,
    workers: usize,
    seal_interval_ms: u64,
) -> JoinCase {
    let base = super::join::build_join_program(specs, shape, draws, &builds.installs);
    let texts: Vec<String> = specs
        .iter()
        .enumerate()
        .map(|(i, spec)| super::join::join_definition_text(&format!("j{i}"), spec))
        .collect();
    let program = chain_program(ChainedKind::Aggregate, base, &texts, chain);
    finish(
        ChainedKind::Aggregate,
        program,
        chain,
        shape,
        builds,
        burst_size,
        lanes,
        workers,
        seal_interval_ms,
    )
}

/// The `ON` shapes a 1-1 `JOIN` may join a chained `b` on: those whose `b`
/// columns include its primary key (#1482).
pub const CHAINED_B_ON: [OnShape; 3] = [OnShape::PkPk, OnShape::KeyPk, OnShape::PkPlusT];

/// A 1-1 `JOIN` case ([`super::join_one_to_one::one_to_one_case`]) with
/// `chain`. A chained `b` side must be joined on its primary key, so every
/// spec's `ON` must be one of [`CHAINED_B_ON`] then.
///
/// # Panics
///
/// On a spec that joins a chained `b` on another shape: a generator bug.
#[allow(clippy::too_many_arguments)]
pub fn one_to_one_chained_case(
    specs: &[OneToOneSpec],
    chain: &ChainSpec,
    shape: KeyShape,
    draws: &[OneToOneOpDraw],
    builds: &JoinBuildDraws,
    burst_size: usize,
    lanes: usize,
    workers: usize,
    seal_interval_ms: u64,
) -> JoinCase {
    assert!(
        !chain.joined || specs.iter().all(|s| CHAINED_B_ON.contains(&s.on)),
        "a chained b is joined on its primary key: {specs:?}"
    );
    let base =
        super::join_one_to_one::build_one_to_one_program(specs, shape, draws, &builds.installs);
    let texts: Vec<String> = specs
        .iter()
        .enumerate()
        .map(|(i, spec)| super::join_one_to_one::one_to_one_definition_text(&format!("j{i}"), spec))
        .collect();
    let program = chain_program(ChainedKind::OneToOne, base, &texts, chain);
    finish(
        ChainedKind::OneToOne,
        program,
        chain,
        shape,
        builds,
        burst_size,
        lanes,
        workers,
        seal_interval_ms,
    )
}

#[allow(clippy::too_many_arguments)]
fn finish(
    kind: ChainedKind,
    program: Program,
    chain: &ChainSpec,
    shape: KeyShape,
    builds: &JoinBuildDraws,
    burst_size: usize,
    lanes: usize,
    workers: usize,
    seal_interval_ms: u64,
) -> JoinCase {
    let source = if chain.joined {
        super::join::JOINED_TABLE
    } else {
        super::join::FROM_TABLE
    };
    let mut plan = super::concurrent_plan(&program, burst_size, lanes);
    if kind == ChainedKind::OneToOne {
        plan = super::join_one_to_one::one_joined_lane(plan, &program);
    }
    plan = one_lane_where_rekeyed(plan, &program, source);
    plan = super::join::add_join_actions(plan, &program, &builds.actions);
    plan = drop_uncaptured_rereads(plan, &program);
    plan = add_chain_actions(kind, plan, &program, chain);
    plan.settle_after_install = builds.settle_after_install;
    JoinCase {
        program,
        shape,
        burst_size,
        plan,
        workers,
        seal_interval_ms,
    }
}

#[cfg(feature = "proptest")]
pub use strategy::chained_case_strategy;

#[cfg(feature = "proptest")]
mod strategy {
    use super::*;
    use crate::generate::join::JoinOpKind;
    use crate::generate::join::strategy as aggregate;
    use crate::generate::join_one_to_one::strategy as one_to_one;
    use proptest::prelude::*;

    fn form() -> impl Strategy<Value = Form> {
        prop_oneof![
            (0u8..3).prop_map(Form::Plus),
            (0u8..3).prop_map(Form::Coalesce)
        ]
    }

    fn chain_action() -> impl Strategy<Value = ChainActionDraw> {
        (
            prop_oneof![
                1 => Just(ChainActionKind::PauseResume),
                1 => form().prop_map(ChainActionKind::Alter),
            ],
            any::<usize>(),
            0u16..=600,
            prop_oneof![1 => Just(1000u16), 1 => 0u16..=1000],
        )
            .prop_map(|(kind, burst, start, span)| ChainActionDraw {
                kind,
                burst,
                start,
                span,
            })
    }

    fn chain() -> impl Strategy<Value = ChainSpec> {
        (
            any::<bool>(),
            any::<bool>(),
            form(),
            proptest::collection::vec(chain_action(), 1..=4),
        )
            .prop_map(|(joined, key, form, actions)| ChainSpec {
                joined,
                computed: Computed { key, form },
                actions,
            })
    }

    /// Turns every `every`th update on the upstream's table into a rekey.
    fn rekey_every<D>(
        draws: &mut [D],
        every: usize,
        mut on_upstream: impl FnMut(&mut D) -> Option<&mut JoinOpKind>,
    ) {
        let mut seen = 0;
        for draw in draws {
            if let Some(kind) = on_upstream(draw).filter(|k| **k == JoinOpKind::Update) {
                seen += 1;
                if seen % every == 0 {
                    *kind = JoinOpKind::Rekey;
                }
            }
        }
    }

    /// The key shapes the tier draws: the JOIN tiers' but `racing`, whose
    /// seconds-long commit stalls the short bursts here can't afford.
    fn key_shape() -> impl Strategy<Value = KeyShape> {
        prop_oneof![
            1 => Just(KeyShape::Spread),
            1 => Just(KeyShape::HotKey),
            2 => Just(KeyShape::FanOut),
            1 => Just(KeyShape::Wide),
        ]
    }

    fn burst_size(shape: KeyShape) -> std::ops::RangeInclusive<usize> {
        if shape == KeyShape::Wide {
            260..=600
        } else {
            30..=150
        }
    }

    fn joined_weight(shape: KeyShape) -> u32 {
        if matches!(shape, KeyShape::FanOut | KeyShape::Wide) {
            5
        } else {
            3
        }
    }

    /// Makes the first spec read the computed column, so every case's
    /// `JOIN`s do.
    fn read_computed_aggregate(specs: &mut [JoinDefSpec], chain: &ChainSpec) {
        use crate::generate::join::JoinField;
        // The key is in every `ON`; a value is read by a field.
        if chain.computed.key {
            return;
        }
        let field = if chain.joined {
            JoinField::SumY
        } else {
            JoinField::SumX
        };
        if !specs[0].fields.contains(&field) {
            specs[0].fields.insert(0, field);
        }
    }

    fn read_computed_one_to_one(specs: &mut [OneToOneSpec], chain: &ChainSpec) {
        use crate::generate::join_one_to_one::OneToOneField;
        if chain.joined {
            // `b`'s on its primary key: one of the shapes that include it.
            for spec in specs.iter_mut() {
                if !CHAINED_B_ON.contains(&spec.on) {
                    spec.on = CHAINED_B_ON[spec.on as usize % CHAINED_B_ON.len()];
                }
            }
            if chain.computed.key {
                specs[0].on = OnShape::PkPlusT;
            } else if !specs[0].fields.contains(&OneToOneField::BY) {
                specs[0].fields.insert(0, OneToOneField::BY);
            }
        } else if chain.computed.key {
            if matches!(
                specs[0].on,
                OnShape::PkPk | OnShape::Composite | OnShape::Text
            ) {
                specs[0].on = OnShape::KeyUnique;
            }
        } else if !specs[0].fields.contains(&OneToOneField::AX) {
            specs[0].fields.insert(0, OneToOneField::AX);
        }
    }

    /// An aggregate `JOIN` case with a chain: the JOIN tier's specs and op
    /// draws, the 1-1 JOIN tier's short bursts and build draws.
    fn aggregate_case() -> impl Strategy<Value = JoinCase> {
        (key_shape(), prop_oneof![1 => Just(false), 3 => Just(true)]).prop_flat_map(
            |(shape, truncating)| {
                (
                    proptest::collection::vec(aggregate::spec(), 1..=3),
                    chain(),
                    proptest::collection::vec(
                        aggregate::draw(joined_weight(shape), truncating),
                        120..=600,
                    ),
                    4usize..=10,
                    burst_size(shape),
                    2usize..=4,
                    2usize..=6,
                    20u64..=200,
                    one_to_one::builds(),
                    proptest::option::of(aggregate::COMMIT_STALL_MS),
                )
                    .prop_map(
                        move |(
                            mut specs,
                            chain,
                            mut draws,
                            every,
                            burst_size,
                            lanes,
                            workers,
                            seal_interval_ms,
                            builds,
                            commit_stall_ms,
                        )| {
                            read_computed_aggregate(&mut specs, &chain);
                            let joined = chain.joined;
                            rekey_every(&mut draws, every, |d: &mut JoinOpDraw| {
                                (d.joined == joined).then_some(&mut d.kind)
                            });
                            let mut case = aggregate_chained_case(
                                &specs,
                                &chain,
                                shape,
                                &draws,
                                &builds,
                                burst_size,
                                lanes,
                                workers,
                                seal_interval_ms,
                            );
                            case.plan.commit_stall =
                                commit_stall_ms.map(std::time::Duration::from_millis);
                            case
                        },
                    )
            },
        )
    }

    /// A 1-1 `JOIN` case with a chain: the 1-1 JOIN tier's non-`racing`
    /// cases, tail truncates included.
    fn one_to_one_case() -> impl Strategy<Value = JoinCase> {
        (key_shape(), prop_oneof![1 => Just(false), 3 => Just(true)]).prop_flat_map(
            |(shape, truncating)| {
                (
                    proptest::collection::vec(one_to_one::spec(), 1..=3),
                    chain(),
                    proptest::collection::vec(
                        one_to_one::draw(joined_weight(shape), truncating),
                        120..=600,
                    ),
                    4usize..=10,
                    burst_size(shape),
                    2usize..=4,
                    2usize..=6,
                    20u64..=200,
                    one_to_one::builds(),
                    proptest::option::of(aggregate::COMMIT_STALL_MS),
                    proptest::collection::vec(
                        one_to_one::tail_truncate(),
                        if truncating { 2..=4 } else { 0..=0 },
                    ),
                )
                    .prop_map(
                        move |(
                            mut specs,
                            chain,
                            mut draws,
                            every,
                            burst_size,
                            lanes,
                            workers,
                            seal_interval_ms,
                            builds,
                            commit_stall_ms,
                            tails,
                        )| {
                            read_computed_one_to_one(&mut specs, &chain);
                            crate::generate::join_one_to_one::place_tail_truncates(
                                &mut draws, burst_size, &tails,
                            );
                            let joined = chain.joined;
                            rekey_every(&mut draws, every, |d: &mut OneToOneOpDraw| {
                                (d.position == u8::from(joined)).then_some(&mut d.kind)
                            });
                            let mut case = one_to_one_chained_case(
                                &specs,
                                &chain,
                                shape,
                                &draws,
                                &builds,
                                burst_size,
                                lanes,
                                workers,
                                seal_interval_ms,
                            );
                            case.plan.commit_stall =
                                commit_stall_ms.map(std::time::Duration::from_millis);
                            case
                        },
                    )
            },
        )
    }

    /// A chained-reader case (see the module doc): half aggregate `JOIN`s,
    /// half 1-1 `JOIN`s, over the `spread`, `hot_key`, `fan_out` (twice as
    /// often) and `wide` key shapes; 120 to 600 ops, in bursts of 30 to 150
    /// (`wide`: 260 to 600); in three cases in four about one op in a
    /// hundred a `TRUNCATE` (and, for a 1-1 `JOIN`, two to four more at the
    /// ends of bursts); every fourth to tenth update of the upstream's table
    /// a primary-key update; one to four pauses and `ALTER`s of the upstream
    /// and two to six of the tier's installs, pauses and re-reads; 2 to 6
    /// workers sealing every 20 to 200 ms over 2 to 4 lanes; and in about
    /// half the cases a commit stall of up to 5 to 40 ms.
    pub fn chained_case_strategy() -> impl Strategy<Value = JoinCase> {
        prop_oneof![aggregate_case(), one_to_one_case()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_join_text_is_rewritten_over_the_upstream() {
        assert_eq!(
            chained_join_text(
                "TRANSFORM j0 FROM ja LEFT JOIN jb ON ja.k = jb.k GROUP BY jb.gb SELECT jb.gb AS \
                 gb, COUNT(*) AS n, SUM(jb.y) AS sy, SUM(ja.x) + SUM(jb.y) AS sxy",
                "jb",
                "ub",
                "y"
            ),
            "TRANSFORM j0 FROM ja LEFT JOIN ub ON ja.k = ub.k GROUP BY ub.gb SELECT ub.gb AS \
             gb, COUNT(*) AS n, SUM(ub.y1) AS sy, SUM(ja.x) + SUM(ub.y1) AS sxy"
        );
        assert_eq!(
            chained_join_text(
                "TRANSFORM j0 FROM ja INNER JOIN jb ON ja.k = jb.id AND ja.t = jb.t SELECT \
                 CASE WHEN jb.id IS NULL THEN 'none' ELSE jb.w END AS case_w, ja.k AS k",
                "ja",
                "ua",
                "k"
            ),
            "TRANSFORM j0 FROM ua INNER JOIN jb ON ua.k1 = jb.id AND ua.t = jb.t SELECT \
             CASE WHEN jb.id IS NULL THEN 'none' ELSE jb.w END AS case_w, ua.k1 AS k"
        );
    }

    #[test]
    fn an_alter_form_round_trips_through_its_text() {
        for form in [Form::Plus(0), Form::Plus(2), Form::Coalesce(1)] {
            assert_eq!(form_of(&form.text("y")), form);
        }
    }
}
