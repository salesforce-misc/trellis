//! The 1-1 JOIN tier's programs (ADR-0021, #1392 step 9a): two tables,
//! [`FROM_TABLE`] and [`JOINED_TABLE`], one to three
//! `FROM ja [LEFT | INNER] JOIN jb ON … SELECT …` definitions over them with
//! no `GROUP BY`, and an op stream that writes both tables. A definition has
//! one target row per `ja` row (`LEFT`) or per matched `ja` row (`INNER`),
//! keyed by `ja`'s primary key, and copies `jb`'s columns onto it. The oracle
//! is the definition's own query ([`crate::oracle::render_join_one_to_one_select`]).
//!
//! The tables share their names with the aggregate JOIN tier's ([`super::join`]),
//! so its build draws, operator actions and coverage are reused as they are;
//! their columns differ:
//!
//! - `ja (id bigint pk, t int, k bigint, c1 int, c2 int, s text, x int)`:
//!   the `FROM` side. `k`, `(c1, c2)` and `s` are one drawn key spelled as a
//!   bigint, a pair of ints and a text.
//! - `jb (id bigint pk, t int, u bigint unique, c1 int, c2 int, s text unique,
//!   y int, w text, unique (c1, c2))`: the joined side. A 1-1 JOIN needs the
//!   `ON` columns of `jb` to hold a unique key, and every [`OnShape`] below
//!   does: its primary key, `u`, `(c1, c2)` or `s`, alone or with `t`.
//!
//! Each definition ([`OneToOneSpec`]) is `LEFT` or `INNER`, joins on one of
//! the [`OnShape`]s (`a` on its primary key or on a plain column; one
//! column, two columns with the extra one a plain column, a composite
//! unique key, a text key), and selects a draw of [`OneToOneField`]s that
//! read either side, with `NULL`s for an unmatched `LEFT` row.
//!
//! The model of a correct target is the same as the pins' of the seam-fed
//! families (`trellis/tests/join_one_to_one_over_targets.rs`): after each
//! step the pipeline settles and every target equals its query over the
//! tables, and `self_check` finds nothing.
//!
//! The op stream ([`build_one_to_one_program`]) inserts, updates and deletes
//! rows of both tables, and now and then truncates one of them, with the
//! join key `NULL` now and then, or only `c2` of the pair (a `NULL` in one
//! `ON` column joins nothing). Because `jb`'s key is unique, an insert or
//! update of a `jb` row draws a key no other live `jb` row holds (it probes
//! on from the drawn key, and takes `NULL` when none is free), and the
//! plan puts all of a burst's `jb` ops on one lane, so the lanes never race
//! two `jb` rows for one key. `ja` ops spread over the other lanes, so
//! writes to both tables race into the same sealed batches. `jb`'s ids come
//! from as many values as there are keys, and `ja`'s from [`A_IDS`], so a
//! `jb` row has many `ja` rows on its key under [`KeyShape::FanOut`]: one
//! change fans out over many target rows.
//!
//! Builds, pauses, resumes and re-reads are drawn as the aggregate JOIN tier
//! draws them ([`super::join::JoinBuildDraws`]).
//!
//! Not drawn: sides that are another transform's target (the seam-fed
//! families of `join_one_to_one_over_targets`; the draw dimension is #1488),
//! partitioned sides, `DDL` between a build's steps, and a value that makes a
//! field fail (a held row: `join_one_to_one_quarantine` pins it).

use trellis::dev::defs::IntWidth;
use trellis::dev::defs::ast::{TransformDef, ValueType};

use super::join::{JoinCase, JoinOpKind, KeyShape};
use crate::model::{Column, Op, OpOutcome, PRIMARY_KEY_VALUE_TYPE, Program, Table};

/// The `FROM` table.
pub const FROM_TABLE: &str = super::join::FROM_TABLE;
/// The joined table.
pub const JOINED_TABLE: &str = super::join::JOINED_TABLE;
/// How many ids `ja` draws its rows' primary keys from. A row id comes back
/// after a delete, so a key's `ja` rows are rewritten, deleted and
/// re-inserted under one id.
pub const A_IDS: u8 = 64;

/// The two tables, empty.
pub fn one_to_one_tables() -> Vec<Table> {
    let int4 = ValueType::Integer(IntWidth::Int4);
    let int8 = ValueType::Integer(IntWidth::Int8);
    let table = |name: &str, columns: &[(&str, ValueType)]| Table {
        name: name.to_string(),
        pk_col: "id".to_string(),
        columns: std::iter::once(Column {
            name: "id".to_string(),
            value_type: PRIMARY_KEY_VALUE_TYPE,
        })
        .chain(columns.iter().map(|(c, value_type)| Column {
            name: (*c).to_string(),
            value_type: *value_type,
        }))
        .collect(),
        unique_cols: Vec::new(),
        unique_sets: Vec::new(),
        partitioning: None,
    };
    let ja = table(
        FROM_TABLE,
        &[
            ("t", int4),
            ("k", int8),
            ("c1", int4),
            ("c2", int4),
            ("s", ValueType::Text),
            ("x", int4),
        ],
    );
    let mut jb = table(
        JOINED_TABLE,
        &[
            ("t", int4),
            ("u", int8),
            ("c1", int4),
            ("c2", int4),
            ("s", ValueType::Text),
            ("y", int4),
            ("w", ValueType::Text),
        ],
    );
    jb.unique_cols = vec!["u".to_string(), "s".to_string()];
    jb.unique_sets = vec![vec!["c1".to_string(), "c2".to_string()]];
    vec![ja, jb]
}

/// What a definition joins on. `jb`'s side of every shape holds a unique
/// key, so each is a valid 1-1 JOIN.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnShape {
    /// `ja.id = jb.id`: `a` on its primary key.
    PkPk,
    /// `ja.k = jb.id`: `a` on a plain column, `b` on its primary key.
    KeyPk,
    /// `ja.k = jb.u`: `b` on a unique column (which may be `NULL`).
    KeyUnique,
    /// `ja.k = jb.id AND ja.t = jb.t`: two columns, the second a plain
    /// column on both sides (`b`'s on a superset of its primary key).
    PkPlusT,
    /// `ja.t = jb.t AND ja.k = jb.u`: the same over a unique column.
    UniquePlusT,
    /// `ja.c1 = jb.c1 AND ja.c2 = jb.c2`: a composite unique key, either
    /// column of which may be `NULL`.
    Composite,
    /// `ja.s = jb.s`: a text key.
    Text,
}

impl OnShape {
    pub const ALL: [OnShape; 7] = [
        OnShape::PkPk,
        OnShape::KeyPk,
        OnShape::KeyUnique,
        OnShape::PkPlusT,
        OnShape::UniquePlusT,
        OnShape::Composite,
        OnShape::Text,
    ];

    fn text(self) -> &'static str {
        match self {
            OnShape::PkPk => "ja.id = jb.id",
            OnShape::KeyPk => "ja.k = jb.id",
            OnShape::KeyUnique => "ja.k = jb.u",
            OnShape::PkPlusT => "ja.k = jb.id AND ja.t = jb.t",
            OnShape::UniquePlusT => "ja.t = jb.t AND ja.k = jb.u",
            OnShape::Composite => "ja.c1 = jb.c1 AND ja.c2 = jb.c2",
            OnShape::Text => "ja.s = jb.s",
        }
    }
}

/// One field of a definition: an expression over either table's columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OneToOneField {
    /// `jb.id`: which `jb` row the target row joined.
    BId,
    /// `jb.y`.
    BY,
    /// `jb.w`.
    BW,
    /// `ja.x`.
    AX,
    /// `ja.s`.
    AS,
    /// `ja.x + jb.y`: `NULL` when either is.
    SumXy,
    /// `ja.x + COALESCE(jb.y, 0)`.
    CoalesceXy,
    /// `CASE WHEN jb.id IS NULL THEN 'none' ELSE jb.w END`.
    CaseW,
    /// `jb.y - ja.x`.
    DiffYx,
}

impl OneToOneField {
    pub const ALL: [OneToOneField; 9] = [
        OneToOneField::BId,
        OneToOneField::BY,
        OneToOneField::BW,
        OneToOneField::AX,
        OneToOneField::AS,
        OneToOneField::SumXy,
        OneToOneField::CoalesceXy,
        OneToOneField::CaseW,
        OneToOneField::DiffYx,
    ];

    fn select(self) -> &'static str {
        match self {
            OneToOneField::BId => "jb.id AS b_id",
            OneToOneField::BY => "jb.y AS b_y",
            OneToOneField::BW => "jb.w AS b_w",
            OneToOneField::AX => "ja.x AS a_x",
            OneToOneField::AS => "ja.s AS a_s",
            OneToOneField::SumXy => "ja.x + jb.y AS sum_xy",
            OneToOneField::CoalesceXy => "ja.x + COALESCE(jb.y, 0) AS co_xy",
            OneToOneField::CaseW => "CASE WHEN jb.id IS NULL THEN 'none' ELSE jb.w END AS case_w",
            OneToOneField::DiffYx => "jb.y - ja.x AS diff_yx",
        }
    }
}

/// One 1-1 `JOIN` definition's shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OneToOneSpec {
    pub left: bool,
    pub on: OnShape,
    /// The fields, in order, without repeats.
    pub fields: Vec<OneToOneField>,
}

/// The definition text of `spec`, targeting `name`.
pub fn one_to_one_definition_text(name: &str, spec: &OneToOneSpec) -> String {
    let mut seen = Vec::new();
    let mut select = Vec::new();
    for field in &spec.fields {
        if !seen.contains(field) {
            seen.push(*field);
            select.push(field.select());
        }
    }
    format!(
        "TRANSFORM {name} FROM {FROM_TABLE} {} {JOINED_TABLE} ON {} SELECT {}",
        if spec.left { "LEFT JOIN" } else { "INNER JOIN" },
        spec.on.text(),
        select.join(", ")
    )
}

/// `spec` targeting `name`, parsed.
///
/// # Panics
///
/// If the text doesn't parse: a generator bug.
pub fn one_to_one_definition(name: &str, spec: &OneToOneSpec) -> TransformDef {
    let text = one_to_one_definition_text(name, spec);
    trellis::dev::defs::parse(&text).unwrap_or_else(|e| panic!("{text}: {e:?}"))
}

/// One drawn op, before [`build_one_to_one_program`] resolves it against the
/// rows the stream has written so far. Plain numbers, so it shrinks toward an
/// insert into `ja` of small values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OneToOneOpDraw {
    /// `jb` rather than `ja`.
    pub joined: bool,
    pub kind: JoinOpKind,
    /// Which live row an update or delete names, modulo how many there are.
    pub pick: usize,
    /// The join key, through [`KeyShape`] (`NULL` for the top few draws).
    pub key: u8,
    /// The primary key an insert draws, modulo the table's id count.
    pub id: u8,
    /// `t`: `NULL` for the top few draws, otherwise `0` or `1`.
    pub t: u8,
    /// Whether the pair's second column, `c2`, is `NULL` though the key isn't.
    pub half_null: bool,
    /// The value column (`x` or `y`), or `NULL`.
    pub value: Option<i8>,
    /// Which columns an update sets: bit 0 the key (`k`/`u`, `c1`, `c2`,
    /// `s`), 1 `t`, 2 only `c2` (to `NULL` or back), 3 the value (and `w`).
    /// No bit set means the value.
    pub columns: u8,
}

/// The number of join keys a [`KeyShape`] draws over.
pub fn key_count(shape: KeyShape) -> u8 {
    match shape {
        KeyShape::Spread => super::join::SPREAD_KEYS,
        KeyShape::HotKey => 1 + super::join::HOT_OTHER_KEYS,
        KeyShape::FanOut => super::join::FAN_OUT_KEYS,
        KeyShape::Wide => super::join::WIDE_KEYS,
        KeyShape::Racing => super::join::RACING_KEYS,
    }
}

/// The text of key `key`: distinct per key, and spelled with leading and
/// trailing blanks, quotes and a non-ASCII letter now and then.
fn key_text(key: u8) -> String {
    const BASES: [&str; 8] = ["k", "K", "k ", " k", "é", "k'", "k\\", "k\""];
    format!("{}{}", BASES[usize::from(key) % BASES.len()], key / 8)
}

/// A live row of the stream's model: its primary key and the key it holds.
#[derive(Debug, Clone, Copy)]
struct Live {
    id: i64,
    key: Option<u8>,
}

/// The key columns of a row holding `key`, with `c2` null when `half_null`.
/// `ja` spells the bigint key `k` and `jb` `u`.
fn key_columns(joined: bool, key: Option<u8>, half_null: bool) -> Vec<(String, Option<String>)> {
    let (big, c1, c2, s) = match key {
        Some(key) => (
            Some(key.to_string()),
            Some((key / 4).to_string()),
            (!half_null).then(|| (key % 4).to_string()),
            Some(key_text(key)),
        ),
        None => (None, None, None, None),
    };
    vec![
        ((if joined { "u" } else { "k" }).to_string(), big),
        ("c1".to_string(), c1),
        ("c2".to_string(), c2),
        ("s".to_string(), s),
    ]
}

/// The first of `from`, `from + 1`, … (modulo `range`) that `taken` doesn't
/// hold, or `None` when it holds them all.
fn first_free(from: u8, range: u8, taken: impl Fn(u8) -> bool) -> Option<u8> {
    (0..range)
        .map(|offset| (from % range + offset) % range)
        .find(|&candidate| !taken(candidate))
}

/// Resolves `draws` into ops on [`one_to_one_tables`], and builds the program
/// with one definition per spec (`j0`, `j1`, …). Definition `i` is installed
/// up front when `installs[i]` is `0` or missing, and otherwise before the op
/// that `installs[i]` thousandths of the way through the stream names.
///
/// An update, delete or truncate names a row the stream has inserted and not
/// deleted yet (a truncate, every one of its table's); with none left on its
/// table, it becomes an insert; an insert with no free id becomes an update.
/// Every op therefore succeeds and changes a row. A `jb` key is never held by
/// two live rows (see the module doc).
pub fn build_one_to_one_program(
    specs: &[OneToOneSpec],
    shape: KeyShape,
    draws: &[OneToOneOpDraw],
    installs: &[u16],
) -> Program {
    let tables = one_to_one_tables();
    let keys = key_count(shape);
    let mut live: [Vec<Live>; 2] = [Vec::new(), Vec::new()];
    let text = |v: Option<i64>| v.map(|v| v.to_string());
    let mut ops = Vec::with_capacity(draws.len());
    for draw in draws {
        let side = usize::from(draw.joined);
        let (table, value_col) = if draw.joined {
            (JOINED_TABLE, "y")
        } else {
            (FROM_TABLE, "x")
        };
        let ids = if draw.joined { keys } else { A_IDS };
        let t = (draw.t < 240).then(|| (draw.t % 2).to_string());
        let value = text(draw.value.map(i64::from));
        let payload = draw
            .value
            .map(|v| format!("w{}", v.rem_euclid(5)))
            .filter(|_| draw.joined);
        // The key this op writes: any for `ja`, a free one for `jb`.
        let key_for = |live: &[Live], own: Option<i64>| -> Option<u8> {
            let drawn = shape.key(draw.key)?;
            if !draw.joined {
                return Some(drawn);
            }
            first_free(drawn, keys, |candidate| {
                live.iter()
                    .any(|row| Some(row.id) != own && row.key == Some(candidate))
            })
        };
        let mut kind = if live[side].is_empty() {
            JoinOpKind::Insert
        } else {
            draw.kind
        };
        let mut insert_id = None;
        if kind == JoinOpKind::Insert {
            insert_id = first_free(draw.id, ids, |candidate| {
                live[side].iter().any(|row| row.id == i64::from(candidate))
            });
            if insert_id.is_none() {
                kind = JoinOpKind::Update;
            }
        }
        let op = match kind {
            JoinOpKind::Insert => {
                let id = i64::from(insert_id.expect("a free id"));
                let key = key_for(&live[side], None);
                live[side].push(Live { id, key });
                let mut row = vec![
                    ("id".to_string(), Some(id.to_string())),
                    ("t".to_string(), t),
                ];
                row.extend(key_columns(draw.joined, key, draw.half_null));
                row.push((value_col.to_string(), value));
                if draw.joined {
                    row.push(("w".to_string(), payload));
                }
                Op::Insert {
                    table: table.to_string(),
                    row,
                    expect: OpOutcome::Succeeds,
                }
            }
            JoinOpKind::Update => {
                let at = draw.pick % live[side].len();
                let row = live[side][at];
                let mut changes = Vec::new();
                if draw.columns & 1 != 0 {
                    let key = key_for(&live[side], Some(row.id));
                    live[side][at].key = key;
                    changes.extend(key_columns(draw.joined, key, draw.half_null));
                }
                if draw.columns & 2 != 0 {
                    changes.push(("t".to_string(), t));
                }
                if draw.columns & 4 != 0 && draw.columns & 1 == 0 {
                    let current = row.key;
                    let c2 = current
                        .filter(|_| !draw.half_null)
                        .map(|key| (key % 4).to_string());
                    changes.push(("c2".to_string(), c2));
                }
                if draw.columns & 8 != 0 || changes.is_empty() {
                    changes.push((value_col.to_string(), value));
                    if draw.joined {
                        changes.push(("w".to_string(), payload));
                    }
                }
                Op::Update {
                    table: table.to_string(),
                    pk: row.id.to_string(),
                    changes,
                    expect: OpOutcome::Succeeds,
                }
            }
            JoinOpKind::Delete => {
                let row = live[side].swap_remove(draw.pick % live[side].len());
                Op::Delete {
                    table: table.to_string(),
                    pk: row.id.to_string(),
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
        .map(|(i, spec)| one_to_one_definition(&format!("j{i}"), spec))
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

/// `plan` with every one of a burst's `jb` ops on one lane, in program order.
///
/// `jb`'s join key is unique, so two `jb` rows on two lanes could race for a
/// key one is about to free (the program's order frees it first, the lanes'
/// need not): the plan keeps the table's ops together. The `ja` ops stay
/// spread over the other lanes by primary key.
pub fn one_joined_lane(
    mut plan: crate::model::ConcurrentPlan,
    program: &Program,
) -> crate::model::ConcurrentPlan {
    let joined = |index: usize| match &program.ops[index] {
        Op::Insert { table, .. }
        | Op::Update { table, .. }
        | Op::Delete { table, .. }
        | Op::Truncate { table, .. } => table == JOINED_TABLE,
        other => unreachable!("the 1-1 JOIN tier draws no {other:?}"),
    };
    for burst in &mut plan.bursts {
        let Some(target) = burst
            .lanes
            .iter()
            .position(|lane| lane.iter().any(|&index| joined(index)))
        else {
            continue;
        };
        let mut moved = Vec::new();
        for (n, lane) in burst.lanes.iter_mut().enumerate() {
            if n != target {
                lane.retain(|&index| {
                    let is_joined = joined(index);
                    if is_joined {
                        moved.push(index);
                    }
                    !is_joined
                });
            }
        }
        burst.lanes[target].extend(moved);
        burst.lanes[target].sort_unstable();
        burst.lanes.retain(|lane| !lane.is_empty());
    }
    plan
}

/// [`build_one_to_one_program`] issued by [`super::concurrent_plan`] in bursts
/// of `burst_size` over `lanes` lanes ([`one_joined_lane`]), with `builds`'
/// installs and actions (the aggregate JOIN tier's, [`super::join::add_join_actions`]).
#[allow(clippy::too_many_arguments)]
pub fn one_to_one_case(
    specs: &[OneToOneSpec],
    shape: KeyShape,
    draws: &[OneToOneOpDraw],
    builds: &super::join::JoinBuildDraws,
    burst_size: usize,
    lanes: usize,
    workers: usize,
    seal_interval_ms: u64,
) -> JoinCase {
    let program = build_one_to_one_program(specs, shape, draws, &builds.installs);
    let mut plan = super::concurrent_plan(&program, burst_size, lanes);
    plan = one_joined_lane(plan, &program);
    plan = super::join::add_join_actions(plan, &program, &builds.actions);
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

/// A `TRUNCATE` placed near the end of a burst ([`place_tail_truncates`]):
/// the burst settles and is checked soon after it, before the writes that
/// follow rewrite what it changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TailTruncate {
    /// Picks the burst, modulo how many there are.
    pub burst: usize,
    /// `jb` rather than `ja`.
    pub joined: bool,
    /// How many ops before the burst's last one it lands.
    pub back: u8,
}

/// Turns the draws `tails` name into truncates: `back` ops before the end of
/// the burst `burst` picks, for bursts of `burst_size`. A truncate of a
/// table with no live rows is resolved as an insert, as any other.
pub fn place_tail_truncates(
    draws: &mut [OneToOneOpDraw],
    burst_size: usize,
    tails: &[TailTruncate],
) {
    if draws.is_empty() || burst_size == 0 {
        return;
    }
    let bursts = draws.len().div_ceil(burst_size);
    for tail in tails {
        let burst = tail.burst % bursts;
        let end = ((burst + 1) * burst_size).min(draws.len());
        let at = end - 1 - usize::from(tail.back).min(end - 1 - burst * burst_size);
        draws[at].kind = JoinOpKind::Truncate;
        draws[at].joined = tail.joined;
    }
}

#[cfg(feature = "proptest")]
pub use strategy::one_to_one_case_strategy;

#[cfg(feature = "proptest")]
mod strategy {
    use super::*;
    use crate::generate::join::{
        COMMIT_STALL_MS, JoinActionDraw, JoinActionKind, JoinBuildDraws, MAX_OPS,
        RACING_COMMIT_STALL_MS,
    };
    use proptest::prelude::*;

    fn spec() -> impl Strategy<Value = OneToOneSpec> {
        (
            any::<bool>(),
            proptest::sample::select(OnShape::ALL.to_vec()),
            proptest::sample::subsequence(OneToOneField::ALL.to_vec(), 1..=4),
        )
            .prop_map(|(left, on, fields)| OneToOneSpec { left, on, fields })
    }

    fn action() -> impl Strategy<Value = JoinActionDraw> {
        (
            prop_oneof![
                4 => Just(JoinActionKind::PauseResume),
                1 => Just(JoinActionKind::RebuildFrom),
                1 => Just(JoinActionKind::RebuildJoined),
            ],
            any::<usize>(),
            any::<usize>(),
            0u16..=600,
            prop_oneof![1 => Just(1000u16), 1 => 0u16..=1000],
        )
            .prop_map(|(kind, burst, target, start, span)| JoinActionDraw {
                kind,
                burst,
                target,
                start,
                span,
            })
    }

    /// When each of up to three definitions installs (half up front, the
    /// rest in the stream's first half, so most are live in time for a
    /// pause), two to six pauses and re-reads (mostly a pause and its
    /// `RESUME`, which rebuilds the target in place over the rows the
    /// paused stretch left; half the resumes at the burst's end, so the check
    /// after the burst sees the rebuild before later writes repair it), and
    /// (a quarter of the cases) a settle over the empty tables before the
    /// first op.
    fn builds() -> impl Strategy<Value = JoinBuildDraws> {
        (
            proptest::collection::vec(prop_oneof![1 => Just(0u16), 1 => 1u16..=500], 3),
            proptest::collection::vec(action(), 2..=6),
            prop_oneof![3 => Just(false), 1 => Just(true)],
        )
            .prop_map(|(installs, actions, settle_after_install)| JoinBuildDraws {
                installs,
                actions,
                settle_after_install,
            })
    }

    /// One op draw. Without `truncating`, a drawn truncate is a delete.
    fn draw(joined_weight: u32, truncating: bool) -> impl Strategy<Value = OneToOneOpDraw> {
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
            // One key in ten is `NULL` (`KeyShape::key`'s top draws): a row
            // with no `J` row, which only its entry lock orders.
            prop_oneof![9 => 0u8..250, 1 => 250u8..=255],
            any::<u8>(),
            any::<u8>(),
            prop_oneof![7 => Just(false), 1 => Just(true)],
            prop_oneof![1 => Just(None), 9 => (-20i8..60).prop_map(Some)],
            0u8..16,
        )
            .prop_map(
                move |(joined, kind, pick, key, id, t, half_null, value, columns)| OneToOneOpDraw {
                    joined,
                    kind: if kind == JoinOpKind::Truncate && !truncating {
                        JoinOpKind::Delete
                    } else {
                        kind
                    },
                    pick,
                    key,
                    id,
                    t,
                    half_null,
                    value,
                    columns,
                },
            )
    }

    fn tail_truncate() -> impl Strategy<Value = TailTruncate> {
        (any::<usize>(), any::<bool>(), 0u8..4).prop_map(|(burst, joined, back)| TailTruncate {
            burst,
            joined,
            back,
        })
    }

    /// A 1-1 JOIN tier case: one to three definitions, a [`KeyShape`], and
    /// writes to both tables (about a third on `jb`, half under the shapes
    /// whose keys are few or many), against 2 to 6 drain workers sealing
    /// every 20 to 200 ms over 2 to 4 lanes. Every burst is checked once it
    /// settles, so a short burst checks soon after what it did.
    ///
    /// - A [`KeyShape::Racing`] case (two in seven) is the aggregate JOIN
    ///   tier's ([`crate::generate::join::join_case_strategy`]): 500 to
    ///   [`MAX_OPS`] ops in bursts of 600 to 900, paced from the first op,
    ///   every definition live over the empty tables first and no operator
    ///   actions, every page stalled for seconds before its commit.
    /// - A `wide` case draws 120 to 600 ops in bursts of 260 to 600 (past
    ///   the engine's split threshold, so several workers claim one batch).
    /// - Every other case draws 120 to 600 ops in short bursts of 30 to 150,
    ///   each definition installed up front or part way through, one to five
    ///   pauses (each with its `RESUME`) and re-reads in the bursts after
    ///   the first, so the rebuilds run over rows that the paused stretch
    ///   deleted, moved and re-inserted, and in about half the cases a commit
    ///   stall of 5 to 40 ms.
    ///
    /// In three in four non-`racing` cases about one op in a hundred is a
    /// `TRUNCATE` of either table, and two to four more land in the last few
    /// ops of a burst ([`place_tail_truncates`]), so the check after the
    /// burst sees the truncate before later writes repair what it left. One
    /// join key in ten is `NULL`.
    pub fn one_to_one_case_strategy() -> impl Strategy<Value = JoinCase> {
        prop_oneof![
            1 => Just(KeyShape::Spread),
            1 => Just(KeyShape::HotKey),
            2 => Just(KeyShape::FanOut),
            1 => Just(KeyShape::Wide),
            2 => Just(KeyShape::Racing)
        ]
        .prop_flat_map(|shape| (Just(shape), prop_oneof![1 => Just(false), 3 => Just(true)]))
        .prop_flat_map(|(shape, truncating)| {
            let racing = shape == KeyShape::Racing;
            let truncating = truncating && !racing;
            let joined_weight =
                if matches!(shape, KeyShape::FanOut | KeyShape::Wide | KeyShape::Racing) {
                    5
                } else {
                    3
                };
            let ops = if racing { 500..=MAX_OPS } else { 120..=600 };
            let burst_size = if racing {
                600usize..=900
            } else if shape == KeyShape::Wide {
                260usize..=600
            } else {
                30usize..=150
            };
            (
                proptest::collection::vec(spec(), 1..=3),
                proptest::collection::vec(draw(joined_weight, truncating), ops),
                burst_size,
                2usize..=4,
                2usize..=6,
                20u64..=200,
                builds(),
                proptest::option::of(COMMIT_STALL_MS),
                RACING_COMMIT_STALL_MS,
                5_000u64..=10_000,
                proptest::collection::vec(tail_truncate(), if truncating { 2..=4 } else { 0..=0 }),
            )
                .prop_map(
                    move |(
                        specs,
                        mut draws,
                        burst_size,
                        lanes,
                        workers,
                        seal_interval_ms,
                        builds,
                        commit_stall_ms,
                        racing_stall_ms,
                        racing_pace_micros,
                        tails,
                    )| {
                        place_tail_truncates(&mut draws, burst_size, &tails);
                        // A `racing` case builds nothing over rows.
                        let builds = if racing {
                            JoinBuildDraws {
                                settle_after_install: true,
                                ..JoinBuildDraws::default()
                            }
                        } else {
                            builds
                        };
                        let mut case = one_to_one_case(
                            &specs,
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
                        if racing {
                            case.seal_interval_ms = 20 + case.seal_interval_ms % 21;
                            case.workers = case.workers.max(3);
                            case.plan.commit_stall =
                                Some(std::time::Duration::from_millis(racing_stall_ms));
                            case.plan.steady_load = Some(crate::model::SteadyLoad {
                                pace: std::time::Duration::from_micros(racing_pace_micros),
                                pace_from_start: true,
                                stall: std::time::Duration::ZERO,
                            });
                        }
                        case
                    },
                )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn every_shape() -> Vec<OneToOneSpec> {
        let mut specs = Vec::new();
        for left in [true, false] {
            for on in OnShape::ALL {
                specs.push(OneToOneSpec {
                    left,
                    on,
                    fields: OneToOneField::ALL.to_vec(),
                });
            }
        }
        specs
    }

    /// Every definition shape the strategy can draw parses as a 1-1 `JOIN`
    /// with a field per drawn field.
    #[test]
    fn every_drawable_definition_parses() {
        for spec in every_shape() {
            let def = one_to_one_definition("j", &spec);
            assert!(
                matches!(
                    def.key_space,
                    trellis::dev::defs::ast::KeySpace::JoinOneToOne(_)
                ),
                "{spec:?}"
            );
            assert_eq!(def.fields.len(), spec.fields.len());
        }
    }

    fn draw(joined: bool, kind: JoinOpKind, pick: usize, key: u8, id: u8) -> OneToOneOpDraw {
        OneToOneOpDraw {
            joined,
            kind,
            pick,
            key,
            id,
            t: 0,
            half_null: false,
            value: Some(2),
            columns: 0,
        }
    }

    fn key_of(row: &[(String, Option<String>)], column: &str) -> Option<String> {
        row.iter().find(|(c, _)| c == column).unwrap().1.clone()
    }

    /// A `jb` insert never takes a key another live `jb` row holds: the draw
    /// probes on to a free key. A `ja` row takes the drawn key, and an insert
    /// with no free id is an update.
    #[test]
    fn a_joined_key_is_never_held_by_two_live_rows() {
        let mut draws = Vec::new();
        // Seven inserts of key 0 into `jb` over FanOut's three keys (and ids).
        for i in 0..7u8 {
            draws.push(draw(true, JoinOpKind::Insert, 0, 0, i));
        }
        // Key 0 into `ja`: taken as drawn.
        draws.push(draw(false, JoinOpKind::Insert, 0, 0, 0));
        let program = build_one_to_one_program(&[], KeyShape::FanOut, &draws, &[]);
        let inserted = |wanted: &str, column: &str| -> Vec<Option<String>> {
            program
                .ops
                .iter()
                .filter_map(|op| match op {
                    Op::Insert { table, row, .. } if table == wanted => Some(key_of(row, column)),
                    _ => None,
                })
                .collect()
        };
        assert_eq!(
            inserted(JOINED_TABLE, "u"),
            [Some("0".into()), Some("1".into()), Some("2".into())]
        );
        assert_eq!(inserted(FROM_TABLE, "k"), [Some("0".into())]);
        assert!(
            program.ops[3..7]
                .iter()
                .all(|op| matches!(op, Op::Update { .. })),
            "an insert with no free id is an update"
        );
    }

    /// An update moving a `jb` row to a key another live row holds takes the
    /// next free one, and a row can keep its own key.
    #[test]
    fn a_joined_update_probes_past_other_rows_keys() {
        let mut moving = draw(true, JoinOpKind::Update, 0, 1, 0);
        moving.columns = 1;
        let program = build_one_to_one_program(
            &[],
            KeyShape::Spread,
            &[
                draw(true, JoinOpKind::Insert, 0, 0, 0),
                draw(true, JoinOpKind::Insert, 0, 1, 1),
                // Row 0 (key 0) asks for key 1, which row 1 holds.
                moving,
                // Row 0 asks for the key it holds now.
                OneToOneOpDraw { key: 2, ..moving },
            ],
            &[],
        );
        for op in &program.ops[2..] {
            let Op::Update { changes, .. } = op else {
                panic!("{op:?}")
            };
            assert_eq!(key_of(changes, "u"), Some("2".to_string()));
        }
    }

    /// A tail truncate lands `back` ops before its burst's last op (the
    /// stream's last op for a short last burst), and never before the burst.
    #[test]
    fn a_tail_truncate_lands_at_the_end_of_its_burst() {
        let mut draws: Vec<OneToOneOpDraw> = (0..25u8)
            .map(|i| draw(false, JoinOpKind::Insert, 0, i, i))
            .collect();
        let tail = |burst, joined, back| TailTruncate {
            burst,
            joined,
            back,
        };
        place_tail_truncates(
            &mut draws,
            10,
            &[tail(0, true, 0), tail(4, false, 2), tail(2, true, 3)],
        );
        let truncated: Vec<(usize, bool)> = draws
            .iter()
            .enumerate()
            .filter(|(_, d)| d.kind == JoinOpKind::Truncate)
            .map(|(i, d)| (i, d.joined))
            .collect();
        // Burst 0 ends at 9; burst 4 % 3 = 1 ends at 19; burst 2 (20..25)
        // ends at 24.
        assert_eq!(truncated, [(9, true), (17, false), (21, true)]);
        let mut short = vec![draw(false, JoinOpKind::Insert, 0, 0, 0); 2];
        place_tail_truncates(&mut short, 10, &[tail(0, true, 3)]);
        assert_eq!(short[0].kind, JoinOpKind::Truncate);
    }

    /// The plan puts every `jb` op of a burst on one lane, and keeps every
    /// op exactly once, in program order within a lane.
    #[test]
    fn the_plan_keeps_a_bursts_joined_ops_on_one_lane() {
        let draws: Vec<OneToOneOpDraw> = (0..120u8)
            .map(|i| draw(i % 3 == 0, JoinOpKind::Insert, 0, i, i))
            .collect();
        let case = one_to_one_case(
            &every_shape()[..1],
            KeyShape::Spread,
            &draws,
            &crate::generate::join::JoinBuildDraws::default(),
            40,
            4,
            2,
            50,
        );
        let mut seen = Vec::new();
        for burst in &case.plan.bursts {
            let joined_lanes = burst
                .lanes
                .iter()
                .filter(|lane| {
                    lane.iter().any(|&i| match &case.program.ops[i] {
                        Op::Insert { table, .. } | Op::Update { table, .. } => {
                            table == JOINED_TABLE
                        }
                        _ => false,
                    })
                })
                .count();
            assert_eq!(joined_lanes, 1);
            for lane in &burst.lanes {
                assert!(lane.windows(2).all(|w| w[0] < w[1]));
                seen.extend(lane.iter().copied());
            }
        }
        seen.sort_unstable();
        assert_eq!(seen, (0..draws.len()).collect::<Vec<_>>());
    }
}
