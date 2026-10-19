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
//! **Not drawn yet:** rows written before a definition exists. Until the
//! `JOIN` build (J4, #1109) a `JOIN` goes `live` only over two empty
//! tables, so every definition is installed up front and the run waits for
//! it before the first op
//! ([`crate::model::ConcurrentPlan::settle_after_install`]).

use trellis::IntWidth;
use trellis::dev::defs::ast::{TransformDef, ValueType};

use crate::model::{Column, Op, OpOutcome, PRIMARY_KEY_VALUE_TYPE, Program, Table};

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
/// one definition per spec (`j0`, `j1`, …), all installed up front.
///
/// An update, delete or truncate names a row the stream has inserted and
/// not deleted yet (a truncate, every one of its table's); with none left on
/// its table, it becomes an insert. Every op therefore succeeds and changes
/// a row. Ids aren't reused after a truncate.
pub fn build_join_program(specs: &[JoinDefSpec], shape: KeyShape, draws: &[JoinOpDraw]) -> Program {
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
        relationships: Vec::new(),
        def_install_after_op: vec![0; defs.len()],
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

/// [`build_join_program`] issued by [`super::concurrent_plan`] in bursts of
/// `burst_size` over `lanes` lanes, waiting for the definitions to go
/// `live` before the first op.
pub fn join_case(
    specs: &[JoinDefSpec],
    shape: KeyShape,
    draws: &[JoinOpDraw],
    burst_size: usize,
    lanes: usize,
    workers: usize,
    seal_interval_ms: u64,
) -> JoinCase {
    let program = build_join_program(specs, shape, draws);
    let mut plan = super::concurrent_plan(&program, burst_size, lanes);
    plan.settle_after_install = true;
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

    /// A JOIN-tier case: one to three definitions, a [`KeyShape`], 120 to
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
            )
                .prop_map(
                    move |(specs, draws, burst_size, lanes, workers, seal_interval_ms)| {
                        join_case(
                            &specs,
                            shape,
                            &draws,
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
}
