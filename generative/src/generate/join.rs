//! The JOIN tier's programs (#1113): two tables, [`FROM_TABLE`] and
//! [`JOINED_TABLE`], one to three `JOIN … GROUP BY` definitions over them,
//! and an op stream that writes both tables.
//!
//! The tables:
//!
//! - `ja (id bigint pk, ga int, t int, k int, x int)`: the `FROM` side, the
//!   facts. `ga` is its group column, `x` its aggregate argument.
//! - `jb (id bigint pk, t int, k int, gb int, y int)`: the joined side, the
//!   dimension rows. `gb` is its group column, `y` its aggregate argument.
//!   `k` is not unique, so a key can match many `jb` rows (fan-out).
//!
//! Each definition ([`JoinDefSpec`]) is `LEFT` or `INNER`, joins on `k`
//! alone or on the pair `(t, k)` ([`JoinKey`]), and groups by `jb.gb`
//! (#1091 J2's S1), by `ja.ga, jb.gb` (S2) or by `ja.ga` alone
//! ([`JoinGroups`]). Its fields are `COUNT(*)` and a draw of [`JoinField`]s,
//! including composed and expression-argument ones. The oracle is the
//! definition's own `JOIN … GROUP BY` query
//! ([`crate::oracle::render_join_select`]).
//!
//! The op stream ([`build_join_program`]) inserts, updates and deletes rows
//! of both tables, and now and then truncates one of them (ADR-0019's
//! per-side `TRUNCATE`, #1110), one statement per op, with values `NULL` now
//! and then
//! (a `NULL` join key joins nothing). Its [`KeyShape`] decides how the join
//! keys spread: over a dozen keys, mostly onto one hot key, over three keys
//! that both tables keep writing (fan-out on both sides), or over enough
//! keys that one worker's page often names a key on one side only while
//! another worker's names it on the other (the race only the `J` lock
//! orders: with denser keys every page locks the same `P` and `M` cells,
//! which order the pages anyway). Updates move
//! rows between keys and groups. The concurrent runner issues the stream in
//! bursts over several lanes, so both sides' writes race each other into
//! the same sealed batches and onto the same keys.
//!
//! **Builds over rows.** A definition is installed up front or part way
//! through the stream ([`JoinBuildDraws::installs`]), so most `JOIN`s build
//! (#1109's per-side steps and `T` step) over tables that already hold rows,
//! or that both lanes are still writing. Later bursts pause and resume a
//! definition (a `RESUME` rebuilds it in place) and re-read one side
//! ([`BurstAction::RequestBackfill`] on `ja` or `jb`), each while the lanes
//! keep writing both tables ([`add_join_actions`]). The oracle is unchanged:
//! each burst's targets are compared with the definition's own query over the
//! tables, so the build's outcome is part of every check, and every
//! definition must be `live` at the end. In a quarter of the cases the run
//! first waits for the up-front definitions to go `live` over the empty
//! tables ([`crate::model::ConcurrentPlan::settle_after_install`]), the
//! shape the tier had before it drew builds. [`build_coverage`] counts, from
//! a case's program and plan, the builds that run over rows, so a run can show
//! that they happen.
//!
//! The tables are unpartitioned and no definition reads another's target;
//! partitioned sides and chained readers are separate draw dimensions.

use trellis::IntWidth;
use trellis::dev::defs::ast::{TransformDef, ValueType};

use crate::model::{
    BurstAction, Column, ConcurrentPlan, Op, OpOutcome, PRIMARY_KEY_VALUE_TYPE, Program, Table,
    TimedAction,
};

/// The `FROM` table.
pub const FROM_TABLE: &str = "ja";
/// The joined table.
pub const JOINED_TABLE: &str = "jb";

/// The two tables, empty.
pub fn join_tables() -> Vec<Table> {
    let int4 = ValueType::Integer(IntWidth::Int4);
    let table = |name: &str, columns: &[&str]| Table {
        name: name.to_string(),
        pk_col: "id".to_string(),
        columns: std::iter::once(Column {
            name: "id".to_string(),
            value_type: PRIMARY_KEY_VALUE_TYPE,
        })
        .chain(columns.iter().map(|c| Column {
            name: (*c).to_string(),
            value_type: int4,
        }))
        .collect(),
        unique_cols: Vec::new(),
        partitioning: None,
    };
    vec![
        table(FROM_TABLE, &["ga", "t", "k", "x"]),
        table(JOINED_TABLE, &["t", "k", "gb", "y"]),
    ]
}

/// What a definition joins on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKey {
    /// `ja.k = jb.k`.
    Single,
    /// `ja.t = jb.t AND ja.k = jb.k`.
    Pair,
}

/// What a definition groups by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinGroups {
    /// `jb.gb`: #1091 J2's S1, every group fed through the join.
    Joined,
    /// `ja.ga, jb.gb`: J2's S2, a group column on each side.
    Both,
    /// `ja.ga`: the joined side contributes only its aggregates.
    From,
}

/// One aggregate field besides `COUNT(*) AS n`, which every definition has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinField {
    CountX,
    SumX,
    CountY,
    SumY,
    AvgX,
    AvgY,
    /// An expression argument on the `FROM` side: `SUM(ja.x + 1)`.
    SumXPlusOne,
    /// A composed field: `COALESCE(SUM(jb.y), 0)`.
    CoalesceSumY,
    /// Aggregates of both sides composed: `SUM(ja.x) + SUM(jb.y)`.
    SumXPlusSumY,
}

impl JoinField {
    pub const ALL: [JoinField; 9] = [
        JoinField::CountX,
        JoinField::SumX,
        JoinField::CountY,
        JoinField::SumY,
        JoinField::AvgX,
        JoinField::AvgY,
        JoinField::SumXPlusOne,
        JoinField::CoalesceSumY,
        JoinField::SumXPlusSumY,
    ];

    fn select(self) -> &'static str {
        match self {
            JoinField::CountX => "COUNT(ja.x) AS cx",
            JoinField::SumX => "SUM(ja.x) AS sx",
            JoinField::CountY => "COUNT(jb.y) AS cy",
            JoinField::SumY => "SUM(jb.y) AS sy",
            JoinField::AvgX => "AVG(ja.x) AS ax",
            JoinField::AvgY => "AVG(jb.y) AS ay",
            JoinField::SumXPlusOne => "SUM(ja.x + 1) AS sx1",
            JoinField::CoalesceSumY => "COALESCE(SUM(jb.y), 0) AS sy0",
            JoinField::SumXPlusSumY => "SUM(ja.x) + SUM(jb.y) AS sxy",
        }
    }
}

/// One `JOIN` definition's shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinDefSpec {
    pub left: bool,
    pub key: JoinKey,
    pub groups: JoinGroups,
    /// The fields after `COUNT(*) AS n`, in order, without repeats.
    pub fields: Vec<JoinField>,
}

/// The definition text of `spec`, targeting `name`.
pub fn join_definition_text(name: &str, spec: &JoinDefSpec) -> String {
    let groups: &[&str] = match spec.groups {
        JoinGroups::Joined => &["jb.gb"],
        JoinGroups::Both => &["ja.ga", "jb.gb"],
        JoinGroups::From => &["ja.ga"],
    };
    let mut select: Vec<String> = groups
        .iter()
        .map(|g| format!("{g} AS {}", &g[3..]))
        .collect();
    select.push("COUNT(*) AS n".to_string());
    let mut seen = Vec::new();
    for field in &spec.fields {
        if !seen.contains(field) {
            seen.push(*field);
            select.push(field.select().to_string());
        }
    }
    format!(
        "TRANSFORM {name} FROM {FROM_TABLE} {} {JOINED_TABLE} ON {} GROUP BY {} SELECT {}",
        if spec.left { "LEFT JOIN" } else { "INNER JOIN" },
        match spec.key {
            JoinKey::Single => "ja.k = jb.k",
            JoinKey::Pair => "ja.t = jb.t AND ja.k = jb.k",
        },
        groups.join(", "),
        select.join(", ")
    )
}

/// `spec` targeting `name`, parsed.
///
/// # Panics
///
/// If the text doesn't parse: a generator bug.
pub fn join_definition(name: &str, spec: &JoinDefSpec) -> TransformDef {
    let text = join_definition_text(name, spec);
    trellis::dev::defs::parse(&text).unwrap_or_else(|e| panic!("{text}: {e:?}"))
}

/// How an op stream's join keys spread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyShape {
    /// Over [`SPREAD_KEYS`] keys.
    Spread,
    /// Mostly onto key 0, the rest over [`HOT_OTHER_KEYS`] more: one key's
    /// pages queue on its `J` row and cells from both sides.
    HotKey,
    /// Over [`FAN_OUT_KEYS`] keys, both tables written alike: every key ends
    /// up with many rows on each side, so one change fans out over many
    /// cells of the other side.
    FanOut,
    /// Over [`WIDE_KEYS`] keys, both tables written alike: a key has few
    /// rows in any one page, so two workers' pages of one batch often
    /// change one key from opposite sides without sharing a `P` or `M`
    /// cell, and only the `J` lock orders them.
    Wide,
}

pub const SPREAD_KEYS: u8 = 12;
pub const HOT_OTHER_KEYS: u8 = 6;
pub const FAN_OUT_KEYS: u8 = 3;
pub const WIDE_KEYS: u8 = 64;

impl KeyShape {
    pub fn name(self) -> &'static str {
        match self {
            KeyShape::Spread => "spread",
            KeyShape::HotKey => "hot_key",
            KeyShape::FanOut => "fan_out",
            KeyShape::Wide => "wide",
        }
    }

    /// The join key a draw of `raw` names, or `None` (`NULL`) for the top
    /// few draws.
    fn key(self, raw: u8) -> Option<u8> {
        if raw >= 250 {
            return None;
        }
        Some(match self {
            KeyShape::Spread => raw % SPREAD_KEYS,
            KeyShape::HotKey if raw < 200 => 0,
            KeyShape::HotKey => 1 + raw % HOT_OTHER_KEYS,
            KeyShape::FanOut => raw % FAN_OUT_KEYS,
            KeyShape::Wide => raw % WIDE_KEYS,
        })
    }
}

/// What a [`JoinOpDraw`] does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinOpKind {
    Insert,
    Update,
    Delete,
    /// A `TRUNCATE` of the draw's table.
    Truncate,
}

/// One drawn op, before [`build_join_program`] resolves it against the rows
/// the stream has written so far. Plain numbers, so it shrinks toward an
/// insert into `ja` of small values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JoinOpDraw {
    /// `jb` rather than `ja`.
    pub joined: bool,
    pub kind: JoinOpKind,
    /// Which live row an update or delete names, modulo how many there are.
    pub pick: usize,
    /// The join key, through [`KeyShape`].
    pub key: u8,
    /// `t`, modulo 2.
    pub t: u8,
    /// The group column (`ga` or `gb`), modulo 4, or `NULL`.
    pub group: Option<u8>,
    /// The aggregate argument (`x` or `y`), or `NULL`.
    pub value: Option<i8>,
    /// Which columns an update sets: bit 0 the key, 1 `t`, 2 the group, 3
    /// the value. No bit set means the value.
    pub columns: u8,
}

/// Resolves `draws` into ops on [`join_tables`], and builds the program with
/// one definition per spec (`j0`, `j1`, …). Definition `i` is installed up
/// front when `installs[i]` is `0` or missing, and otherwise before the op
/// that `installs[i]` thousandths of the way through the stream name; a
/// share that rounds down to the first op installs it up front too.
///
/// An update, delete or truncate names a row the stream has inserted and
/// not deleted yet (a truncate, every one of its table's); with none left on
/// its table, it becomes an insert. Every op therefore succeeds and changes
/// a row. Ids aren't reused after a truncate.
pub fn build_join_program(
    specs: &[JoinDefSpec],
    shape: KeyShape,
    draws: &[JoinOpDraw],
    installs: &[u16],
) -> Program {
    let tables = join_tables();
    let mut live: [Vec<i64>; 2] = [Vec::new(), Vec::new()];
    let mut next_id = [1i64, 1i64];
    let text = |v: Option<i64>| v.map(|v| v.to_string());
    let mut ops = Vec::with_capacity(draws.len());
    for draw in draws {
        let side = usize::from(draw.joined);
        let (table, group_col, value_col) = if draw.joined {
            (JOINED_TABLE, "gb", "y")
        } else {
            (FROM_TABLE, "ga", "x")
        };
        let key = text(shape.key(draw.key).map(i64::from));
        let t = text(Some(i64::from(draw.t % 2)));
        let group = text(draw.group.map(|g| i64::from(g % 4)));
        let value = text(draw.value.map(i64::from));
        let kind = if live[side].is_empty() {
            JoinOpKind::Insert
        } else {
            draw.kind
        };
        let op = match kind {
            JoinOpKind::Insert => {
                let id = next_id[side];
                next_id[side] += 1;
                live[side].push(id);
                Op::Insert {
                    table: table.to_string(),
                    row: vec![
                        ("id".to_string(), Some(id.to_string())),
                        ("t".to_string(), t),
                        ("k".to_string(), key),
                        (group_col.to_string(), group),
                        (value_col.to_string(), value),
                    ],
                    expect: OpOutcome::Succeeds,
                }
            }
            JoinOpKind::Update => {
                let id = live[side][draw.pick % live[side].len()];
                let mut changes = Vec::new();
                if draw.columns & 1 != 0 {
                    changes.push(("k".to_string(), key));
                }
                if draw.columns & 2 != 0 {
                    changes.push(("t".to_string(), t));
                }
                if draw.columns & 4 != 0 {
                    changes.push((group_col.to_string(), group));
                }
                if draw.columns & 8 != 0 || changes.is_empty() {
                    changes.push((value_col.to_string(), value));
                }
                Op::Update {
                    table: table.to_string(),
                    pk: id.to_string(),
                    changes,
                    expect: OpOutcome::Succeeds,
                }
            }
            JoinOpKind::Delete => {
                let id = live[side].swap_remove(draw.pick % live[side].len());
                Op::Delete {
                    table: table.to_string(),
                    pk: id.to_string(),
                    expect: OpOutcome::Succeeds,
                }
            }
            JoinOpKind::Truncate => {
                live[side].clear();
                Op::Truncate {
                    table: table.to_string(),
                    expect: OpOutcome::Succeeds,
                }
            }
        };
        ops.push(op);
    }
    let defs: Vec<TransformDef> = specs
        .iter()
        .enumerate()
        .map(|(i, spec)| join_definition(&format!("j{i}"), spec))
        .collect();
    Program {
        tables,
        def_install_after_op: (0..defs.len())
            .map(|i| {
                let thousandths = usize::from(installs.get(i).copied().unwrap_or(0).min(999));
                thousandths * ops.len() / 1000
            })
            .collect(),
        defs,
        ops,
        restart_after_ops: Vec::new(),
        scale_out_after_ops: Vec::new(),
    }
}

/// One JOIN-tier case: a program, its concurrent plan, and the engine it
/// runs against.
#[derive(Debug, Clone)]
pub struct JoinCase {
    pub program: Program,
    pub shape: KeyShape,
    /// The ops per burst the plan was cut with, kept for the failure report.
    pub burst_size: usize,
    pub plan: crate::model::ConcurrentPlan,
    /// Application (drain) workers the engine runs.
    pub workers: usize,
    /// The engine's seal cadence, short enough that one burst seals into
    /// several batches.
    pub seal_interval_ms: u64,
}

/// What a case draws besides its definitions and ops: when each definition
/// installs, which operator actions the later bursts take, and whether the
/// run first settles the up-front definitions over the empty tables.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct JoinBuildDraws {
    /// Per definition, how far into the stream it installs, in thousandths
    /// of the ops; `0` (or missing) installs it up front, before any write.
    pub installs: Vec<u16>,
    /// The pauses, resumes and re-reads of the bursts after the first.
    pub actions: Vec<JoinActionDraw>,
    /// Whether the run waits for the up-front definitions to go `live`
    /// before the first op, so they build over empty tables.
    pub settle_after_install: bool,
}

/// What a [`JoinActionDraw`] asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinActionKind {
    /// `PAUSE TRANSFORM`, then `RESUME TRANSFORM` later in the same burst:
    /// the resume rebuilds the target over the rows as they are then.
    PauseResume,
    /// `request_backfill(ja)`: every definition rebuilds in place.
    RebuildFrom,
    /// `request_backfill(jb)`.
    RebuildJoined,
}

/// One drawn operator action, before [`add_join_actions`] resolves it. Plain
/// numbers, so it shrinks toward the earliest burst and the first
/// definition, at the burst's start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JoinActionDraw {
    pub kind: JoinActionKind,
    /// Picks a burst after the first, modulo how many there are.
    pub burst: usize,
    /// Picks the definition of a pause, modulo how many qualify.
    pub target: usize,
    /// Where in the burst the action (or a pair's pause) fires, in
    /// thousandths of the burst's ops.
    pub start: u16,
    /// How much further on a pair's resume fires, in thousandths of the
    /// burst's ops, capped at the burst's end.
    pub span: u16,
}

/// The burst each definition of `program` is installed in: `0` for one
/// installed up front, otherwise the burst holding its install action.
fn install_bursts(program: &Program, plan: &ConcurrentPlan) -> Vec<usize> {
    let mut out = vec![0; program.defs.len()];
    for (index, burst) in plan.bursts.iter().enumerate() {
        for timed in &burst.actions {
            if let BurstAction::Install { def } = timed.action {
                out[def] = index;
            }
        }
    }
    out
}

/// Adds `draws` to `plan`'s bursts. Every action lands in a burst after the
/// first, and names only definitions installed in an earlier burst: by then
/// the earlier burst has settled, so the definition is `live` and its tables
/// are captured, which a pause and a re-read both need.
///
/// A draw that can't apply is dropped rather than bent into something else:
/// a plan with one burst, no definition installed early enough, and a second
/// pause pair on a definition in a burst that already has one.
pub fn add_join_actions(
    mut plan: ConcurrentPlan,
    program: &Program,
    draws: &[JoinActionDraw],
) -> ConcurrentPlan {
    if plan.bursts.len() < 2 {
        return plan;
    }
    let installed_in = install_bursts(program, &plan);
    let mut paired: std::collections::HashSet<(usize, usize)> = std::collections::HashSet::new();
    for draw in draws {
        let burst_index = 1 + draw.burst % (plan.bursts.len() - 1);
        let eligible: Vec<usize> = (0..program.defs.len())
            .filter(|&def| installed_in[def] < burst_index)
            .collect();
        if eligible.is_empty() {
            continue;
        }
        let ops = plan.bursts[burst_index].ops().len();
        let at = usize::from(draw.start.min(1000)) * ops / 1000;
        let end = (at + usize::from(draw.span.min(1000)) * ops / 1000).min(ops);
        let actions = &mut plan.bursts[burst_index].actions;
        match draw.kind {
            JoinActionKind::RebuildFrom | JoinActionKind::RebuildJoined => {
                let table = if draw.kind == JoinActionKind::RebuildFrom {
                    FROM_TABLE
                } else {
                    JOINED_TABLE
                };
                actions.push(TimedAction {
                    after: at,
                    action: BurstAction::RequestBackfill {
                        table: table.to_string(),
                    },
                });
            }
            JoinActionKind::PauseResume => {
                let def = eligible[draw.target % eligible.len()];
                if !paired.insert((burst_index, def)) {
                    continue;
                }
                let target = program.defs[def].target.clone();
                actions.push(TimedAction {
                    after: at,
                    action: BurstAction::Pause {
                        target: target.clone(),
                        column: None,
                    },
                });
                actions.push(TimedAction {
                    after: end,
                    action: BurstAction::Resume {
                        target,
                        column: None,
                    },
                });
            }
        }
    }
    for burst in &mut plan.bursts {
        burst.actions.sort_by_key(|action| action.after);
    }
    plan
}

/// How often a case's builds run over rows, counted from its program and
/// plan. A build "over rows" is an install, a resume or a re-read in a burst
/// whose predecessors, already quiesced, left rows on both tables, more on
/// each than the burst's own deletes and truncates can remove: whenever in
/// the burst it fires, it reads rows on both sides. An install is also
/// counted as racing when it fires part way through its burst, whose own
/// writes are still landing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BuildCoverage {
    /// Definitions installed after the first op.
    pub deferred_installs: usize,
    /// Of those, ones that fire with rows on both tables from earlier bursts.
    pub installs_over_rows: usize,
    /// Installs that fire after some of their own burst's ops (the tables may
    /// have been empty before the burst).
    pub installs_racing: usize,
    pub pauses: usize,
    /// `RESUME`s (each a rebuild in place of the target).
    pub resumes: usize,
    pub resumes_over_rows: usize,
    pub rebuilds_from: usize,
    pub rebuilds_joined: usize,
    pub rebuilds_over_rows: usize,
}

impl BuildCoverage {
    /// Builds of every kind that fire over rows from earlier bursts.
    pub fn builds_over_rows(&self) -> usize {
        self.installs_over_rows + self.resumes_over_rows + self.rebuilds_over_rows
    }

    pub fn add(&mut self, other: &BuildCoverage) {
        self.deferred_installs += other.deferred_installs;
        self.installs_over_rows += other.installs_over_rows;
        self.installs_racing += other.installs_racing;
        self.pauses += other.pauses;
        self.resumes += other.resumes;
        self.resumes_over_rows += other.resumes_over_rows;
        self.rebuilds_from += other.rebuilds_from;
        self.rebuilds_joined += other.rebuilds_joined;
        self.rebuilds_over_rows += other.rebuilds_over_rows;
    }
}

/// [`BuildCoverage`] of `program` issued as `plan`.
pub fn build_coverage(program: &Program, plan: &ConcurrentPlan) -> BuildCoverage {
    let mut out = BuildCoverage::default();
    // Rows the model says each table holds before each burst.
    let mut rows = [0i64; 2];
    for burst in &plan.bursts {
        let ops = burst.ops();
        // The lanes race, so an action may fire after any of the burst's
        // ops: a side certainly holds rows throughout the burst only if it
        // held more rows than the burst deletes from it, and the burst
        // doesn't truncate it.
        let mut emptying = [0i64; 2];
        for &i in &ops {
            let (table, by) = match &program.ops[i] {
                Op::Delete { table, .. } => (table, 1),
                Op::Truncate { table, .. } => (table, i64::MAX),
                _ => continue,
            };
            let side = usize::from(table == JOINED_TABLE);
            emptying[side] = emptying[side].saturating_add(by);
        }
        let over_rows = (0..2).all(|side| rows[side] > emptying[side]);
        for timed in &burst.actions {
            match &timed.action {
                BurstAction::Install { .. } => {
                    out.deferred_installs += 1;
                    out.installs_over_rows += usize::from(over_rows);
                    out.installs_racing += usize::from(timed.after > 0);
                }
                BurstAction::Pause { .. } => out.pauses += 1,
                BurstAction::Resume { .. } => {
                    out.resumes += 1;
                    out.resumes_over_rows += usize::from(over_rows);
                }
                BurstAction::RequestBackfill { table } => {
                    if table == FROM_TABLE {
                        out.rebuilds_from += 1;
                    } else {
                        out.rebuilds_joined += 1;
                    }
                    out.rebuilds_over_rows += usize::from(over_rows);
                }
            }
        }
        for &i in &ops {
            let (table, delta) = match &program.ops[i] {
                Op::Insert { table, .. } => (table, 1),
                Op::Delete { table, .. } => (table, -1),
                Op::Truncate { table, .. } => (table, i64::MIN),
                _ => continue,
            };
            let side = usize::from(table == JOINED_TABLE);
            rows[side] = if delta == i64::MIN {
                0
            } else {
                (rows[side] + delta).max(0)
            };
        }
    }
    out
}

/// [`build_join_program`] issued by [`super::concurrent_plan`] in bursts of
/// `burst_size` over `lanes` lanes, with `builds`' installs and actions. The
/// run waits for the up-front definitions to go `live` before the first op
/// only if `builds` says so.
#[allow(clippy::too_many_arguments)]
pub fn join_case(
    specs: &[JoinDefSpec],
    shape: KeyShape,
    draws: &[JoinOpDraw],
    builds: &JoinBuildDraws,
    burst_size: usize,
    lanes: usize,
    workers: usize,
    seal_interval_ms: u64,
) -> JoinCase {
    let program = build_join_program(specs, shape, draws, &builds.installs);
    let mut plan = super::concurrent_plan(&program, burst_size, lanes);
    plan = add_join_actions(plan, &program, &builds.actions);
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
pub use strategy::join_case_strategy;

#[cfg(feature = "proptest")]
mod strategy {
    use super::*;
    use proptest::prelude::*;

    /// The most ops one case draws: a few bursts' worth at the largest
    /// burst size.
    pub const MAX_OPS: usize = 900;

    fn spec() -> impl Strategy<Value = JoinDefSpec> {
        (
            any::<bool>(),
            prop_oneof![Just(JoinKey::Single), Just(JoinKey::Pair)],
            prop_oneof![
                Just(JoinGroups::Joined),
                Just(JoinGroups::Both),
                Just(JoinGroups::From)
            ],
            proptest::sample::subsequence(JoinField::ALL.to_vec(), 1..=4),
        )
            .prop_map(|(left, key, groups, fields)| JoinDefSpec {
                left,
                key,
                groups,
                fields,
            })
    }

    fn action() -> impl Strategy<Value = JoinActionDraw> {
        (
            prop_oneof![
                3 => Just(JoinActionKind::PauseResume),
                1 => Just(JoinActionKind::RebuildFrom),
                1 => Just(JoinActionKind::RebuildJoined),
            ],
            any::<usize>(),
            any::<usize>(),
            0u16..=1000,
            0u16..=1000,
        )
            .prop_map(|(kind, burst, target, start, span)| JoinActionDraw {
                kind,
                burst,
                target,
                start,
                span,
            })
    }

    /// When each of up to three definitions installs (a third up front, the
    /// rest anywhere in the stream's first 95%, so most build over rows),
    /// up to four pauses and re-reads, and (a quarter of the cases) a settle
    /// over the empty tables before the first op.
    fn builds() -> impl Strategy<Value = JoinBuildDraws> {
        (
            proptest::collection::vec(prop_oneof![1 => Just(0u16), 2 => 1u16..=950], 3),
            proptest::collection::vec(action(), 0..=4),
            prop_oneof![3 => Just(false), 1 => Just(true)],
        )
            .prop_map(|(installs, actions, settle_after_install)| JoinBuildDraws {
                installs,
                actions,
                settle_after_install,
            })
    }

    /// One op draw. Without `truncating`, a drawn truncate is a delete.
    fn draw(joined_weight: u32, truncating: bool) -> impl Strategy<Value = JoinOpDraw> {
        (
            prop_oneof![
                10 - joined_weight => Just(false),
                joined_weight => Just(true),
            ],
            prop_oneof![
                40 => Just(JoinOpKind::Insert),
                40 => Just(JoinOpKind::Update),
                20 => Just(JoinOpKind::Delete),
                1 => Just(JoinOpKind::Truncate),
            ],
            any::<usize>(),
            any::<u8>(),
            0u8..2,
            prop_oneof![1 => Just(None), 9 => (0u8..4).prop_map(Some)],
            prop_oneof![1 => Just(None), 9 => (-20i8..60).prop_map(Some)],
            0u8..16,
        )
            .prop_map(move |(joined, kind, pick, key, t, group, value, columns)| {
                JoinOpDraw {
                    joined,
                    kind: if kind == JoinOpKind::Truncate && !truncating {
                        JoinOpKind::Delete
                    } else {
                        kind
                    },
                    pick,
                    key,
                    t,
                    group,
                    value,
                    columns,
                }
            })
    }

    /// A JOIN-tier case: one to three definitions, each installed up front
    /// or part way through the stream, with pauses, resumes and re-reads
    /// in the later bursts ([`JoinBuildDraws`]), a [`KeyShape`], 120 to
    /// [`MAX_OPS`] ops (half on each side for fan-out and wide, about a
    /// third on the joined side otherwise; in half the cases, about one in a
    /// hundred a `TRUNCATE`), bursts of 40 to 600 ops over 2
    /// to 4 lanes, against 2 to 6 drain workers sealing every 20 to 200 ms.
    /// A burst past the engine's split threshold (256 rows) can seal into
    /// one batch that several workers claim parts of. A `wide` case draws
    /// its burst size past that threshold, as its race needs two workers on
    /// one batch. A case that truncates a table issues all of that table's
    /// ops from one lane (`super::concurrent_plan`), so the other half keeps
    /// several lanes writing each table.
    pub fn join_case_strategy() -> impl Strategy<Value = JoinCase> {
        prop_oneof![
            Just(KeyShape::Spread),
            Just(KeyShape::HotKey),
            Just(KeyShape::FanOut),
            Just(KeyShape::Wide)
        ]
        .prop_flat_map(|shape| (Just(shape), any::<bool>()))
        .prop_flat_map(|(shape, truncating)| {
            let joined_weight = if matches!(shape, KeyShape::FanOut | KeyShape::Wide) {
                5
            } else {
                3
            };
            let burst_size = if shape == KeyShape::Wide {
                260usize..=600
            } else {
                40usize..=600
            };
            (
                proptest::collection::vec(spec(), 1..=3),
                proptest::collection::vec(draw(joined_weight, truncating), 120..=MAX_OPS),
                burst_size,
                2usize..=4,
                2usize..=6,
                20u64..=200,
                builds(),
            )
                .prop_map(
                    move |(specs, draws, burst_size, lanes, workers, seal_interval_ms, builds)| {
                        join_case(
                            &specs,
                            shape,
                            &draws,
                            &builds,
                            burst_size,
                            lanes,
                            workers,
                            seal_interval_ms,
                        )
                    },
                )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn every_shape() -> Vec<JoinDefSpec> {
        let mut specs = Vec::new();
        for left in [true, false] {
            for key in [JoinKey::Single, JoinKey::Pair] {
                for groups in [JoinGroups::Joined, JoinGroups::Both, JoinGroups::From] {
                    specs.push(JoinDefSpec {
                        left,
                        key,
                        groups,
                        fields: JoinField::ALL.to_vec(),
                    });
                }
            }
        }
        specs
    }

    /// Every definition shape the strategy can draw parses as a `JOIN`.
    #[test]
    fn every_drawable_definition_parses() {
        for spec in every_shape() {
            let def = join_definition("j", &spec);
            assert!(
                matches!(def.key_space, trellis::dev::defs::ast::KeySpace::Join(_)),
                "{spec:?}"
            );
            assert_eq!(
                def.fields.len(),
                spec.fields.len()
                    + 1
                    + match spec.groups {
                        JoinGroups::Both => 2,
                        _ => 1,
                    }
            );
        }
    }

    /// An update, delete or truncate always names a live row of its own
    /// table, so the stream's every op succeeds, and a table with no live row
    /// inserts; a truncate leaves its table none, and ids aren't reused.
    #[test]
    fn ops_name_live_rows_of_their_own_table() {
        let draw = |joined, kind, pick| JoinOpDraw {
            joined,
            kind,
            pick,
            key: 1,
            t: 0,
            group: Some(1),
            value: Some(2),
            columns: 0,
        };
        let program = build_join_program(
            &[],
            KeyShape::Spread,
            &[
                draw(false, JoinOpKind::Delete, 0),
                draw(true, JoinOpKind::Update, 0),
                draw(false, JoinOpKind::Insert, 0),
                draw(false, JoinOpKind::Delete, 1),
                draw(false, JoinOpKind::Update, 0),
                draw(true, JoinOpKind::Truncate, 0),
                draw(true, JoinOpKind::Update, 0),
                draw(true, JoinOpKind::Truncate, 0),
                draw(false, JoinOpKind::Truncate, 0),
                draw(true, JoinOpKind::Delete, 0),
            ],
            &[],
        );
        let shapes: Vec<String> = program
            .ops
            .iter()
            .map(|op| match op {
                Op::Insert { table, row, .. } => format!("insert {table} {:?}", row[0].1),
                Op::Update { table, pk, .. } => format!("update {table} {pk}"),
                Op::Delete { table, pk, .. } => format!("delete {table} {pk}"),
                Op::Truncate { table, .. } => format!("truncate {table}"),
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(
            shapes,
            [
                "insert ja Some(\"1\")",
                "insert jb Some(\"1\")",
                "insert ja Some(\"2\")",
                "delete ja 2",
                "update ja 1",
                "truncate jb",
                "insert jb Some(\"2\")",
                "truncate jb",
                "truncate ja",
                "insert jb Some(\"3\")",
            ]
        );
    }

    /// The hot-key shape puts most keys on key 0, and every shape draws a
    /// `NULL` key now and then.
    #[test]
    fn key_shapes_spread_as_documented() {
        let hot = (0..=255u8)
            .filter(|&raw| KeyShape::HotKey.key(raw) == Some(0))
            .count();
        assert!(hot > 190, "{hot}");
        for shape in [
            KeyShape::Spread,
            KeyShape::HotKey,
            KeyShape::FanOut,
            KeyShape::Wide,
        ] {
            assert_eq!(shape.key(255), None);
            assert!((0..250u8).all(|raw| shape.key(raw).is_some()));
        }
        assert!((0..250u8).all(|raw| KeyShape::FanOut.key(raw).unwrap() < FAN_OUT_KEYS));
        let wide: std::collections::BTreeSet<u8> = (0..250u8)
            .filter_map(|raw| KeyShape::Wide.key(raw))
            .collect();
        assert_eq!(wide.len(), usize::from(WIDE_KEYS));
    }

    fn inserts(n: usize) -> Vec<JoinOpDraw> {
        (0..n)
            .map(|i| JoinOpDraw {
                joined: i % 2 == 1,
                kind: JoinOpKind::Insert,
                pick: 0,
                key: i as u8,
                t: 0,
                group: Some(1),
                value: Some(1),
                columns: 0,
            })
            .collect()
    }

    fn one_spec() -> Vec<JoinDefSpec> {
        let spec = JoinDefSpec {
            left: true,
            key: JoinKey::Single,
            groups: JoinGroups::Joined,
            fields: vec![JoinField::SumX],
        };
        vec![spec.clone(), spec.clone(), spec]
    }

    /// A definition installs up front for `0`, otherwise after the share of
    /// the ops its draw names, and the plan installs each exactly once.
    #[test]
    fn installs_land_at_their_drawn_point() {
        let builds = JoinBuildDraws {
            installs: vec![0, 500, 999],
            ..JoinBuildDraws::default()
        };
        let case = join_case(
            &one_spec(),
            KeyShape::Spread,
            &inserts(100),
            &builds,
            30,
            2,
            2,
            50,
        );
        assert_eq!(case.program.def_install_after_op, [0, 50, 99]);
        assert!(!case.plan.settle_after_install);
        let installs: Vec<(usize, usize)> = case
            .plan
            .bursts
            .iter()
            .enumerate()
            .flat_map(|(i, b)| {
                b.actions.iter().filter_map(move |a| match a.action {
                    BurstAction::Install { def } => Some((i, def)),
                    _ => None,
                })
            })
            .collect();
        assert_eq!(installs, [(1, 1), (3, 2)]);
    }

    /// Actions land only in bursts after the first, name only definitions
    /// installed in an earlier burst, and pair each pause with a later resume.
    #[test]
    fn actions_name_only_definitions_installed_earlier() {
        let builds = JoinBuildDraws {
            installs: vec![0, 500, 999],
            actions: (0..12)
                .map(|i| JoinActionDraw {
                    kind: [
                        JoinActionKind::PauseResume,
                        JoinActionKind::RebuildFrom,
                        JoinActionKind::RebuildJoined,
                    ][i % 3],
                    burst: i,
                    target: i,
                    start: 100 * (i as u16 % 5),
                    span: 300,
                })
                .collect(),
            settle_after_install: true,
        };
        let case = join_case(
            &one_spec(),
            KeyShape::Spread,
            &inserts(100),
            &builds,
            30,
            2,
            2,
            50,
        );
        assert!(case.plan.settle_after_install);
        let installed_in = install_bursts(&case.program, &case.plan);
        assert_eq!(installed_in, [0, 1, 3]);
        assert!(case.plan.bursts[0].actions.is_empty());
        let mut pauses = 0;
        for (index, burst) in case.plan.bursts.iter().enumerate() {
            let mut open = std::collections::HashSet::new();
            for timed in &burst.actions {
                match &timed.action {
                    BurstAction::Pause { target, .. } => {
                        pauses += 1;
                        let def = case
                            .program
                            .defs
                            .iter()
                            .position(|d| &d.target == target)
                            .unwrap();
                        assert!(installed_in[def] < index, "{target} in burst {index}");
                        assert!(open.insert(target.clone()), "paired twice in {index}");
                    }
                    BurstAction::Resume { target, .. } => {
                        assert!(open.remove(target), "resume without pause");
                    }
                    _ => {}
                }
            }
            assert!(open.is_empty(), "burst {index}: {open:?}");
        }
        assert!(pauses > 0);
        let coverage = build_coverage(&case.program, &case.plan);
        assert_eq!(coverage.pauses, pauses);
        assert_eq!(coverage.resumes, pauses);
        assert!(coverage.rebuilds_from > 0 && coverage.rebuilds_joined > 0);
    }

    /// The coverage counts a build as over rows only when earlier bursts
    /// left rows on both tables: an install in the first burst never is.
    #[test]
    fn coverage_counts_builds_over_rows() {
        let builds = JoinBuildDraws {
            installs: vec![100, 600, 0],
            actions: vec![JoinActionDraw {
                kind: JoinActionKind::RebuildJoined,
                burst: 0,
                target: 0,
                start: 0,
                span: 0,
            }],
            settle_after_install: false,
        };
        let case = join_case(
            &one_spec(),
            KeyShape::Spread,
            &inserts(100),
            &builds,
            30,
            2,
            2,
            50,
        );
        let coverage = build_coverage(&case.program, &case.plan);
        assert_eq!(coverage.deferred_installs, 2);
        // Install at op 10 is in burst 0 (no earlier rows); at op 60, in
        // burst 2.
        assert_eq!(coverage.installs_over_rows, 1);
        assert_eq!(coverage.installs_racing, 1);
        assert_eq!(coverage.rebuilds_joined, 1);
        assert_eq!(coverage.rebuilds_over_rows, 1);
        assert_eq!(coverage.builds_over_rows(), 2);
    }

    /// A burst that could empty a side before its action fires doesn't count
    /// that action as over rows: the lanes race, so it may fire after the
    /// delete.
    #[test]
    fn coverage_skips_a_burst_that_can_empty_a_side() {
        let mut draws = inserts(2);
        // Burst 1: delete `ja`'s only row, then insert another.
        draws.push(JoinOpDraw {
            kind: JoinOpKind::Delete,
            ..draws[0]
        });
        draws.push(draws[0]);
        // Burst 2: inserts only.
        draws.extend(inserts(2));
        let action = |burst| JoinActionDraw {
            kind: JoinActionKind::RebuildJoined,
            burst,
            target: 0,
            start: 1000,
            span: 0,
        };
        let builds = JoinBuildDraws {
            installs: vec![0],
            actions: vec![action(0), action(1)],
            settle_after_install: false,
        };
        let case = join_case(
            &one_spec()[..1],
            KeyShape::Spread,
            &draws,
            &builds,
            2,
            2,
            2,
            50,
        );
        assert!(matches!(case.program.ops[2], Op::Delete { .. }));
        let coverage = build_coverage(&case.program, &case.plan);
        assert_eq!(coverage.rebuilds_joined, 2);
        assert_eq!(coverage.rebuilds_over_rows, 1);
    }
}
