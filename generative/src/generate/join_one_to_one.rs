//! The 1-1 JOIN tier's programs (ADR-0021, #1392 step 9a): two tables,
//! [`FROM_TABLE`] and [`JOINED_TABLE`], one to three
//! `FROM ja [LEFT | INNER] JOIN jb ON … SELECT …` definitions over them with
//! no `GROUP BY`, and an op stream that writes both tables. A definition has
//! one target row per `ja` row (`LEFT`) or per matched `ja` row (`INNER`),
//! keyed by `ja`'s primary key, and copies `jb`'s columns onto it. The oracle
//! is the definition's own query ([`crate::oracle::render_join_one_to_one_select`]).
//!
//! The same code draws the star tier's programs (#1460, #1781): a root `ja`
//! joined to two to four tables, one per position ([`JOINED_TABLES`]: `jb`
//! at position 1, then `jc`, `jd`, `je`), each `LEFT` or `INNER`
//! ([`StarCase`], [`star_case_strategy`]). The two-table JOIN is the
//! one-position case: a draw names its table by position
//! ([`OneToOneOpDraw::position`], `0` for `ja`), and a definition lists its
//! positions past the first in [`OneToOneSpec::more`]. Whether the star tier
//! runs is [`star_tier_enabled`].
//!
//! The tables share their names with the aggregate JOIN tier's ([`super::join`]),
//! so its build draws, operator actions and coverage are reused as they are;
//! their columns differ:
//!
//! - `ja (id bigint pk, t int, k bigint, c1 int, c2 int, s text, x int)`:
//!   the `FROM` side. `k`, `(c1, c2)` and `s` are one drawn key spelled as a
//!   bigint, a pair of ints and a text. A star's `ja` also has `k2` up to
//!   `k4`, a bigint key of its own for each position from 2
//!   ([`star_tables`]).
//! - `jb (id bigint pk, t int, u bigint unique, c1 int, c2 int, s text unique,
//!   y int, w text, unique (c1, c2))`: the joined side. A 1-1 JOIN needs the
//!   `ON` columns of `jb` to hold a unique key, and every [`OnShape`] below
//!   does: its primary key, `u`, `(c1, c2)` or `s`, alone or with `t`. A
//!   star's other joined tables have `jb`'s columns.
//!
//! Each definition ([`OneToOneSpec`]) is `LEFT` or `INNER`, joins on one of
//! the [`OnShape`]s (`a` on its primary key or on a plain column; one
//! column, two columns with the extra one a plain column, a composite
//! unique key, a text key), and selects a draw of [`OneToOneField`]s that
//! read either side, with `NULL`s for an unmatched `LEFT` row. A star's
//! position from 2 ([`StarPosition`]) draws its own kind and shape, reads
//! `ja`'s shared columns (`id`, `(c1, c2)`, `s`, `t`) or its own `k{p}`, and
//! selects fields that read its table.
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
//! change fans out over many target rows. A star's joined tables are each
//! written the same way, each on a lane of its own per burst.
//!
//! Builds, pauses, resumes and re-reads are drawn as the aggregate JOIN tier
//! draws them ([`super::join::JoinBuildDraws`]).
//!
//! Not drawn here: sides that are another transform's target (the
//! chained-reader tier, [`super::join_chained`], draws them over these
//! programs), partitioned sides, `DDL` between a build's steps, and a value
//! that makes a field fail (a held row: `join_one_to_one_quarantine` pins
//! it).

use trellis::dev::defs::IntWidth;
use trellis::dev::defs::ast::{TransformDef, ValueType};

use super::join::{JoinCase, JoinOpKind, KeyShape};
use crate::model::{Column, Op, OpOutcome, PRIMARY_KEY_VALUE_TYPE, Program, Table};

/// The `FROM` table.
pub const FROM_TABLE: &str = super::join::FROM_TABLE;
/// The joined table.
pub const JOINED_TABLE: &str = super::join::JOINED_TABLE;
/// The joined tables of a star, by position: `JOINED_TABLES[p - 1]` is at
/// position `p`.
pub const JOINED_TABLES: [&str; 4] = [JOINED_TABLE, "jc", "jd", "je"];
/// The most tables a star joins, the root included (#1459's cap).
pub const MAX_TABLES: usize = 1 + JOINED_TABLES.len();
/// How many ids `ja` draws its rows' primary keys from. A row id comes back
/// after a delete, so a key's `ja` rows are rewritten, deleted and
/// re-inserted under one id.
pub const A_IDS: u8 = 64;

/// Whether the star tier runs: the development knob `TRELLIS_DEV_STAR_JOIN`
/// is on, and the engine runs a star's pages and builds. This is the one
/// gate of the star tier's property, sweep and metamorphic check; with it
/// off they return at once, and nothing else the 1-1 JOIN tier draws or
/// checks changes.
///
/// A star defines behind the knob, but its first page or build halts
/// ("whose apply is not built yet") until the joined apply lands (#1774,
/// sub-step 3.8 of #1460), which sets `ENGINE_RUNS_A_STAR` here.
pub fn star_tier_enabled() -> bool {
    const ENGINE_RUNS_A_STAR: bool = false;
    ENGINE_RUNS_A_STAR && trellis::dev::defs::star_join_enabled()
}

/// The table at `position`: `ja` at 0, then [`JOINED_TABLES`].
///
/// # Panics
///
/// Past the last position.
pub fn table_at(position: u8) -> &'static str {
    match position {
        0 => FROM_TABLE,
        p => JOINED_TABLES[usize::from(p) - 1],
    }
}

/// The position of `table`, if it is a star's.
pub fn position_of(table: &str) -> Option<u8> {
    if table == FROM_TABLE {
        return Some(0);
    }
    JOINED_TABLES
        .iter()
        .position(|t| *t == table)
        .map(|i| i as u8 + 1)
}

/// `ja`'s own key column for position `position`, from 2: `k2` to `k4`.
pub fn own_key_column(position: u8) -> String {
    format!("k{position}")
}

/// The two tables, empty.
pub fn one_to_one_tables() -> Vec<Table> {
    star_tables(2)
}

/// A star's `tables` tables, empty: `ja` with an own key column per position
/// from 2, and a joined table per position with `jb`'s columns. Two tables
/// are [`one_to_one_tables`].
///
/// # Panics
///
/// Outside 2 to [`MAX_TABLES`].
pub fn star_tables(tables: usize) -> Vec<Table> {
    assert!(
        (2..=MAX_TABLES).contains(&tables),
        "a star has 2 to {MAX_TABLES} tables, not {tables}"
    );
    let int4 = ValueType::Integer(IntWidth::Int4);
    let int8 = ValueType::Integer(IntWidth::Int8);
    let table = |name: &str, columns: &[(String, ValueType)]| Table {
        name: name.to_string(),
        pk_col: "id".to_string(),
        columns: std::iter::once(Column {
            name: "id".to_string(),
            value_type: PRIMARY_KEY_VALUE_TYPE,
        })
        .chain(columns.iter().map(|(c, value_type)| Column {
            name: c.clone(),
            value_type: *value_type,
        }))
        .collect(),
        unique_cols: Vec::new(),
        unique_sets: Vec::new(),
        partitioning: None,
    };
    let named = |columns: &[(&str, ValueType)]| -> Vec<(String, ValueType)> {
        columns
            .iter()
            .map(|(c, value_type)| ((*c).to_string(), *value_type))
            .collect()
    };
    let mut root = named(&[("t", int4), ("k", int8)]);
    root.extend((2..tables as u8).map(|p| (own_key_column(p), int8)));
    root.extend(named(&[
        ("c1", int4),
        ("c2", int4),
        ("s", ValueType::Text),
        ("x", int4),
    ]));
    let mut out = vec![table(FROM_TABLE, &root)];
    for joined in &JOINED_TABLES[..tables - 1] {
        let mut jb = table(
            joined,
            &named(&[
                ("t", int4),
                ("u", int8),
                ("c1", int4),
                ("c2", int4),
                ("s", ValueType::Text),
                ("y", int4),
                ("w", ValueType::Text),
            ]),
        );
        jb.unique_cols = vec!["u".to_string(), "s".to_string()];
        jb.unique_sets = vec![vec!["c1".to_string(), "c2".to_string()]];
        out.push(jb);
    }
    out
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

    /// The `ON` text joining `joined`, with `ja`'s bigint key spelled `key`
    /// (`k`, or a star position's own `k{p}`).
    fn text_at(self, joined: &str, key: &str) -> String {
        let b = joined;
        match self {
            OnShape::PkPk => format!("ja.id = {b}.id"),
            OnShape::KeyPk => format!("ja.{key} = {b}.id"),
            OnShape::KeyUnique => format!("ja.{key} = {b}.u"),
            OnShape::PkPlusT => format!("ja.{key} = {b}.id AND ja.t = {b}.t"),
            OnShape::UniquePlusT => format!("ja.t = {b}.t AND ja.{key} = {b}.u"),
            OnShape::Composite => format!("ja.c1 = {b}.c1 AND ja.c2 = {b}.c2"),
            OnShape::Text => format!("ja.s = {b}.s"),
        }
    }

    /// Whether the shape reads `ja`'s bigint key (`k`, or a position's own).
    pub fn reads_root_key(self) -> bool {
        matches!(
            self,
            OnShape::KeyPk | OnShape::KeyUnique | OnShape::PkPlusT | OnShape::UniquePlusT
        )
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

    /// The fields that read the joined table: what a star position from 2
    /// selects.
    pub const JOINED: [OneToOneField; 7] = [
        OneToOneField::BId,
        OneToOneField::BY,
        OneToOneField::BW,
        OneToOneField::SumXy,
        OneToOneField::CoalesceXy,
        OneToOneField::CaseW,
        OneToOneField::DiffYx,
    ];

    /// Whether the field reads the joined table.
    pub fn reads_joined(self) -> bool {
        !matches!(self, OneToOneField::AX | OneToOneField::AS)
    }

    /// The select item over `joined`, its name ending in `suffix` (`""` at
    /// position 1, `_p{p}` past it).
    fn select_at(self, joined: &str, suffix: &str) -> String {
        let b = joined;
        match self {
            OneToOneField::BId => format!("{b}.id AS b_id{suffix}"),
            OneToOneField::BY => format!("{b}.y AS b_y{suffix}"),
            OneToOneField::BW => format!("{b}.w AS b_w{suffix}"),
            OneToOneField::AX => "ja.x AS a_x".to_string(),
            OneToOneField::AS => "ja.s AS a_s".to_string(),
            OneToOneField::SumXy => format!("ja.x + {b}.y AS sum_xy{suffix}"),
            OneToOneField::CoalesceXy => format!("ja.x + COALESCE({b}.y, 0) AS co_xy{suffix}"),
            OneToOneField::CaseW => {
                format!("CASE WHEN {b}.id IS NULL THEN 'none' ELSE {b}.w END AS case_w{suffix}")
            }
            OneToOneField::DiffYx => format!("{b}.y - ja.x AS diff_yx{suffix}"),
        }
    }
}

/// One 1-1 `JOIN` definition's shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OneToOneSpec {
    /// Position 1's kind.
    pub left: bool,
    /// Position 1's `ON`, over `ja.k`.
    pub on: OnShape,
    /// The fields, in order, without repeats.
    pub fields: Vec<OneToOneField>,
    /// A star's positions from 2, in order: empty for the two-table JOIN.
    pub more: Vec<StarPosition>,
}

/// One position of a star past the first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StarPosition {
    pub left: bool,
    pub on: OnShape,
    /// Whether a shape that reads `ja`'s bigint key reads the position's own
    /// `k{p}` rather than the `k` position 1 reads.
    pub own_key: bool,
    /// The fields over this position's table ([`OneToOneField::JOINED`];
    /// any other is left out), named with a `_p{p}` suffix.
    pub fields: Vec<OneToOneField>,
}

impl OneToOneSpec {
    /// The tables the definition joins, the root included.
    pub fn tables(&self) -> usize {
        2 + self.more.len()
    }

    /// Whether position `p` (from 1) is a `LEFT` join.
    pub fn left_at(&self, p: u8) -> bool {
        match p {
            1 => self.left,
            p => self.more[usize::from(p) - 2].left,
        }
    }

    /// Position `p`'s `ON` text.
    fn on_at(&self, p: u8) -> String {
        match p {
            1 => self.on.text_at(JOINED_TABLE, "k"),
            p => {
                let position = &self.more[usize::from(p) - 2];
                let key = if position.own_key {
                    own_key_column(p)
                } else {
                    "k".to_string()
                };
                position.on.text_at(table_at(p), &key)
            }
        }
    }
}

/// The definition text of `spec`, targeting `name`.
pub fn one_to_one_definition_text(name: &str, spec: &OneToOneSpec) -> String {
    let order: Vec<u8> = (1..spec.tables() as u8).collect();
    star_definition_text(name, spec, &order)
}

/// The definition text of `spec`, targeting `name`, with its `JOIN` clauses
/// in `order` (a permutation of the positions `1..tables`): the metamorphic
/// case's twin. The select list and each clause's table, kind and `ON` stay
/// as `spec` has them, so only the clauses' order (and so the engine's
/// numbering of the positions) changes.
///
/// # Panics
///
/// When `order` isn't a permutation of the positions.
pub fn star_definition_text(name: &str, spec: &OneToOneSpec, order: &[u8]) -> String {
    let mut sorted = order.to_vec();
    sorted.sort_unstable();
    assert_eq!(
        sorted,
        (1..spec.tables() as u8).collect::<Vec<_>>(),
        "{order:?} isn't an order of {spec:?}'s positions"
    );
    let mut select = Vec::new();
    let mut seen = Vec::new();
    for field in &spec.fields {
        if !seen.contains(field) {
            seen.push(*field);
            select.push(field.select_at(JOINED_TABLE, ""));
        }
    }
    for (i, position) in spec.more.iter().enumerate() {
        let p = i as u8 + 2;
        let mut seen = Vec::new();
        for field in position.fields.iter().filter(|f| f.reads_joined()) {
            if !seen.contains(field) {
                seen.push(*field);
                select.push(field.select_at(table_at(p), &format!("_p{p}")));
            }
        }
    }
    let joins: Vec<String> = order
        .iter()
        .map(|&p| {
            format!(
                "{} {} ON {}",
                if spec.left_at(p) {
                    "LEFT JOIN"
                } else {
                    "INNER JOIN"
                },
                table_at(p),
                spec.on_at(p)
            )
        })
        .collect();
    format!(
        "TRANSFORM {name} FROM {FROM_TABLE} {} SELECT {}",
        joins.join(" "),
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
    /// The table: `0` for `ja`, `p` for the joined table at position `p`
    /// (`1` for `jb`), modulo the program's tables.
    pub position: u8,
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
    /// `s`, and a star's `k2` to `k4`), 1 `t`, 2 only `c2` (to `NULL` or
    /// back), 3 the value (and `w`). No bit set means the value.
    pub columns: u8,
    /// A star's `ja` keys at positions 2 to 4 (`k2` to `k4`), through
    /// [`KeyShape`] as `key` is; unread below three tables. The star tier
    /// draws each equal to `key` half the time, so a root row is hot at two
    /// or more positions at once.
    pub own_keys: [u8; 3],
}

/// The number of join keys a [`KeyShape`] draws over.
pub fn key_count(shape: KeyShape) -> u8 {
    match shape {
        KeyShape::Spread => super::join::SPREAD_KEYS,
        KeyShape::HotKey => 1 + super::join::HOT_OTHER_KEYS,
        KeyShape::FanOut => super::join::FAN_OUT_KEYS,
        KeyShape::Wide => super::join::WIDE_KEYS,
        KeyShape::Racing => super::join::RACING_KEYS,
        KeyShape::Star => super::join::STAR_KEYS,
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

/// A star `ja` row's own keys, `k2` up to the last position's: none below
/// three tables.
fn own_key_columns(
    draw: &OneToOneOpDraw,
    shape: KeyShape,
    tables: usize,
) -> Vec<(String, Option<String>)> {
    (2..tables as u8)
        .map(|p| {
            let raw = draw.own_keys[usize::from(p) - 2];
            (own_key_column(p), shape.key(raw).map(|k| k.to_string()))
        })
        .collect()
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
    build_star_program(specs, 2, shape, draws, installs)
}

/// [`build_one_to_one_program`] over a star's `tables` tables
/// ([`star_tables`]): a draw writes the table at its position, modulo
/// `tables`, and each joined table's keys stay unique among its live rows.
///
/// # Panics
///
/// When a spec joins another number of tables.
pub fn build_star_program(
    specs: &[OneToOneSpec],
    tables: usize,
    shape: KeyShape,
    draws: &[OneToOneOpDraw],
    installs: &[u16],
) -> Program {
    for spec in specs {
        assert_eq!(spec.tables(), tables, "{spec:?}");
    }
    let schema = star_tables(tables);
    let keys = key_count(shape);
    let mut live: Vec<Vec<Live>> = vec![Vec::new(); tables];
    let text = |v: Option<i64>| v.map(|v| v.to_string());
    let mut ops = Vec::with_capacity(draws.len());
    for draw in draws {
        let side = usize::from(draw.position) % tables;
        let joined = side > 0;
        let table = table_at(side as u8);
        let value_col = if joined { "y" } else { "x" };
        let ids = if joined { keys } else { A_IDS };
        let t = (draw.t < 240).then(|| (draw.t % 2).to_string());
        let value = text(draw.value.map(i64::from));
        let payload = draw
            .value
            .map(|v| format!("w{}", v.rem_euclid(5)))
            .filter(|_| joined);
        // The key this op writes: any for `ja`, a free one for a joined table.
        let key_for = |live: &[Live], own: Option<i64>| -> Option<u8> {
            let drawn = shape.key(draw.key)?;
            if !joined {
                return Some(drawn);
            }
            first_free(drawn, keys, |candidate| {
                live.iter()
                    .any(|row| Some(row.id) != own && row.key == Some(candidate))
            })
        };
        // Every key column a key update of this table sets.
        let keyed = |key: Option<u8>| {
            let mut columns = key_columns(joined, key, draw.half_null);
            if !joined {
                columns.extend(own_key_columns(draw, shape, tables));
            }
            columns
        };
        let mut kind = if live[side].is_empty() {
            JoinOpKind::Insert
        } else {
            draw.kind
        };
        let mut insert_id = None;
        if matches!(kind, JoinOpKind::Insert | JoinOpKind::Rekey) {
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
                row.extend(keyed(key));
                row.push((value_col.to_string(), value));
                if joined {
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
                    changes.extend(keyed(key));
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
                    if joined {
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
                let at = draw.pick % live[side].len();
                let row = live[side].swap_remove(at);
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
            JoinOpKind::Rekey => {
                let id = i64::from(insert_id.expect("a free id"));
                let at = draw.pick % live[side].len();
                let row = live[side][at];
                live[side][at].id = id;
                let mut changes = vec![("id".to_string(), Some(id.to_string()))];
                if draw.columns & 1 != 0 {
                    let key = key_for(&live[side], Some(id));
                    live[side][at].key = key;
                    changes.extend(keyed(key));
                }
                Op::Update {
                    table: table.to_string(),
                    pk: row.id.to_string(),
                    changes,
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
        tables: schema,
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

/// `plan` with every one of a burst's ops on a joined table on one lane, in
/// program order: one lane per joined table (a star's tables may share one).
///
/// A joined table's join key is unique, so two of its rows on two lanes
/// could race for a key one is about to free (the program's order frees it
/// first, the lanes' need not): the plan keeps the table's ops together. The
/// `ja` ops stay spread over the other lanes by primary key.
pub fn one_joined_lane(
    mut plan: crate::model::ConcurrentPlan,
    program: &Program,
) -> crate::model::ConcurrentPlan {
    let table_of = |index: usize| match &program.ops[index] {
        Op::Insert { table, .. }
        | Op::Update { table, .. }
        | Op::Delete { table, .. }
        | Op::Truncate { table, .. } => table.as_str(),
        other => unreachable!("the 1-1 JOIN tier draws no {other:?}"),
    };
    for burst in &mut plan.bursts {
        let mut moved_any = false;
        for joined in JOINED_TABLES {
            let Some(target) = burst
                .lanes
                .iter()
                .position(|lane| lane.iter().any(|&index| table_of(index) == joined))
            else {
                continue;
            };
            moved_any = true;
            let mut moved = Vec::new();
            for (n, lane) in burst.lanes.iter_mut().enumerate() {
                if n != target {
                    lane.retain(|&index| {
                        let is_joined = table_of(index) == joined;
                        if is_joined {
                            moved.push(index);
                        }
                        !is_joined
                    });
                }
            }
            burst.lanes[target].extend(moved);
            burst.lanes[target].sort_unstable();
        }
        if moved_any {
            burst.lanes.retain(|lane| !lane.is_empty());
        }
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
    planned_case(
        program,
        shape,
        builds,
        burst_size,
        lanes,
        workers,
        seal_interval_ms,
    )
}

/// `program` planned as [`one_to_one_case`] plans it.
fn planned_case(
    program: Program,
    shape: KeyShape,
    builds: &super::join::JoinBuildDraws,
    burst_size: usize,
    lanes: usize,
    workers: usize,
    seal_interval_ms: u64,
) -> JoinCase {
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
    /// The table's position, as [`OneToOneOpDraw::position`].
    pub position: u8,
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
        draws[at].position = tail.position;
    }
}

/// The workers a star case drains with (#1464 section 4).
pub const STAR_WORKERS: usize = 8;
/// The longest stall a star case's pages take at a lock step.
pub const STAR_LOCK_STEP_STALL_MS: u64 = 3;
/// The share of lock steps a star case stalls at, in thousandths.
pub const STAR_STALLED_LOCK_STEPS_PER_MILLE: u16 = 300;
/// The share of pages a star case re-derives, in thousandths.
pub const STAR_REDERIVED_PAGES_PER_MILLE: u16 = 120;
/// The largest page a star case drains, in rows.
pub const STAR_MAX_PAGE_ROWS: usize = 15;

/// How a star case's engine runs (#1464 section 4's generator changes 3
/// to 5), for the runner to apply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StarKnobs {
    /// The most rows a drain holds (`ManualBackend::set_drain_batch_cap`):
    /// pages of 1 to [`STAR_MAX_PAGE_ROWS`] rows.
    pub drain_batch_cap: usize,
    /// The longest stall a page takes at a lock step.
    pub lock_step_stall: std::time::Duration,
    /// The share of lock steps that stall, in thousandths.
    pub stalled_lock_steps_per_mille: u16,
    /// The share of pages re-derived, in thousandths.
    pub rederived_pages_per_mille: u16,
    /// The "late" position (#1459's F11 orientation): in a mixed page its
    /// draws come last ([`shape_pages`]).
    pub late_position: u8,
    /// Whether the case also runs its metamorphic twin
    /// ([`StarCase::permuted_program`]).
    pub metamorphic: bool,
}

/// One drawn page of the stream ([`shape_pages`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageDraw {
    /// Its draws: 1 to [`STAR_MAX_PAGE_ROWS`].
    pub rows: u8,
    /// Whether its draws span the positions on one root key.
    pub mixed: bool,
    /// Seeds its shuffle.
    pub shuffle: u64,
}

/// SplitMix64: the shuffle's numbers, from a drawn seed.
fn split_mix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Cuts `draws` into the pages `pages` draws, in turn, and shuffles each
/// (#1464 section 4, change 3). In a `mixed` page of two or more draws every
/// draw takes the page's first key, the draws cycle over the positions
/// (`ja` first, then each joined table), a `ja` draw takes the key at every
/// position of its own too, and the draws at `late` come after the others:
/// one root key changed at every position, with the late position's change
/// last, in rows a drain of [`StarKnobs::drain_batch_cap`] rows takes
/// together.
pub fn shape_pages(draws: &mut [OneToOneOpDraw], pages: &[PageDraw], tables: usize, late: u8) {
    if pages.is_empty() {
        return;
    }
    let mut start = 0;
    for page in pages.iter().cycle() {
        if start >= draws.len() {
            break;
        }
        let end = (start + usize::from(page.rows.max(1))).min(draws.len());
        let chunk = &mut draws[start..end];
        let mixed = page.mixed && chunk.len() >= 2;
        if mixed {
            let key = chunk[0].key;
            for (i, draw) in chunk.iter_mut().enumerate() {
                draw.position = (i % tables) as u8;
                draw.key = key;
                if draw.position == 0 {
                    draw.own_keys = [key; 3];
                }
            }
        }
        let mut state = page.shuffle;
        for i in (1..chunk.len()).rev() {
            let j = (split_mix(&mut state) % (i as u64 + 1)) as usize;
            chunk.swap(i, j);
        }
        if mixed {
            // Stable: the late position's draws move to the end.
            chunk.sort_by_key(|draw| draw.position == late);
        }
        start = end;
    }
}

/// A star tier case (#1781): a [`JoinCase`] over `tables` tables, the specs
/// its definitions were drawn from, how its engine runs, and the `JOIN`
/// order of its metamorphic twin.
#[derive(Debug, Clone)]
pub struct StarCase {
    pub case: JoinCase,
    pub tables: usize,
    pub specs: Vec<OneToOneSpec>,
    pub knobs: StarKnobs,
    /// A permutation of the positions `1..tables`: the `JOIN` clauses' order
    /// in [`StarCase::permuted_program`].
    pub join_order: Vec<u8>,
}

impl StarCase {
    /// The metamorphic twin (#1464 section 4, change 7): the same tables, ops
    /// and plan, with each definition's `JOIN` clauses in
    /// [`StarCase::join_order`]. It reaches the same targets: each `ON` is a
    /// null-rejecting equality over the root and one table, so the clauses'
    /// order changes nothing a row joins, only the engine's numbering of the
    /// positions (and so its `(position, key)` lock order).
    pub fn permuted_program(&self) -> Program {
        let mut program = self.case.program.clone();
        program.defs = self
            .specs
            .iter()
            .enumerate()
            .map(|(i, spec)| {
                let text = star_definition_text(&format!("j{i}"), spec, &self.join_order);
                trellis::dev::defs::parse(&text).unwrap_or_else(|e| panic!("{text}: {e:?}"))
            })
            .collect();
        program
    }
}

/// A star case: [`build_star_program`] over `tables` tables under
/// [`KeyShape::Star`], planned as [`one_to_one_case`] plans it, drained by
/// [`STAR_WORKERS`] workers with a commit stall of up to
/// [`STAR_LOCK_STEP_STALL_MS`].
#[allow(clippy::too_many_arguments)]
pub fn star_case(
    specs: &[OneToOneSpec],
    tables: usize,
    draws: &[OneToOneOpDraw],
    builds: &super::join::JoinBuildDraws,
    burst_size: usize,
    lanes: usize,
    seal_interval_ms: u64,
    knobs: StarKnobs,
    join_order: Vec<u8>,
) -> StarCase {
    let program = build_star_program(specs, tables, KeyShape::Star, draws, &builds.installs);
    let mut case = planned_case(
        program,
        KeyShape::Star,
        builds,
        burst_size,
        lanes,
        STAR_WORKERS,
        seal_interval_ms,
    );
    case.plan.commit_stall = Some(knobs.lock_step_stall);
    StarCase {
        case,
        tables,
        specs: specs.to_vec(),
        knobs,
        join_order,
    }
}

#[cfg(feature = "proptest")]
pub use strategy::{one_to_one_case_strategy, star_case_strategy};

#[cfg(feature = "proptest")]
pub(crate) mod strategy {
    use super::*;
    use crate::generate::join::{
        COMMIT_STALL_MS, JoinActionDraw, JoinActionKind, JoinBuildDraws, MAX_OPS,
        RACING_COMMIT_STALL_MS,
    };
    use proptest::prelude::*;

    pub(crate) fn spec() -> impl Strategy<Value = OneToOneSpec> {
        (
            any::<bool>(),
            proptest::sample::select(OnShape::ALL.to_vec()),
            proptest::sample::subsequence(OneToOneField::ALL.to_vec(), 1..=4),
        )
            .prop_map(|(left, on, fields)| OneToOneSpec {
                left,
                on,
                fields,
                more: Vec::new(),
            })
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
    pub(crate) fn builds() -> impl Strategy<Value = JoinBuildDraws> {
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
    pub(crate) fn draw(
        joined_weight: u32,
        truncating: bool,
    ) -> impl Strategy<Value = OneToOneOpDraw> {
        (
            prop_oneof![
                10 - joined_weight => Just(0u8),
                joined_weight => Just(1u8),
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
                move |(position, kind, pick, key, id, t, half_null, value, columns)| OneToOneOpDraw {
                    position,
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
                    own_keys: [0; 3],
                },
            )
    }

    pub(crate) fn tail_truncate() -> impl Strategy<Value = TailTruncate> {
        (any::<usize>(), any::<bool>(), 0u8..4).prop_map(|(burst, joined, back)| TailTruncate {
            burst,
            position: u8::from(joined),
            back,
        })
    }

    /// A 1-1 JOIN tier case: one to three definitions, a [`KeyShape`], and
    /// writes to both tables (about a third on `jb`, half under the shapes
    /// whose keys are few or many), against 2 to 6 drain workers sealing
    /// every 20 to 200 ms over 2 to 4 lanes. Every burst is checked once it
    /// settles, so a short burst checks soon after what it did.
    ///
    /// - A [`KeyShape::Racing`] case (two in seven) is shaped like the
    ///   aggregate JOIN tier's ([`crate::generate::join::join_case_strategy`]):
    ///   500 to [`MAX_OPS`] ops in bursts of 600 to 900, paced from the
    ///   first op, every definition live over the empty tables first and no
    ///   operator actions, but with every page stalled for seconds before
    ///   its commit, its draws unpaired and its drains uncapped.
    /// - A `wide` case draws 120 to 600 ops in bursts of 260 to 600 (past
    ///   the engine's split threshold, so several workers claim one batch),
    ///   with the installs, pauses and re-reads below.
    /// - Every other case draws 120 to 600 ops in short bursts of 30 to 150,
    ///   each definition installed up front or part way through, two to six
    ///   pauses (each with its `RESUME`) and re-reads in the bursts after
    ///   the first (a draw that can't apply is dropped), so the rebuilds run over rows that the paused stretch
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
                                group_lock_stall: false,
                            });
                        }
                        case
                    },
                )
        })
    }

    fn star_position() -> impl Strategy<Value = StarPosition> {
        (
            any::<bool>(),
            proptest::sample::select(OnShape::ALL.to_vec()),
            any::<bool>(),
            proptest::sample::subsequence(OneToOneField::JOINED.to_vec(), 0..=2),
        )
            .prop_map(|(left, on, own_key, fields)| StarPosition {
                left,
                on,
                own_key,
                fields,
            })
    }

    /// A star definition over `tables` tables: a kind, an `ON` shape and
    /// fields per position.
    pub(crate) fn star_spec(tables: usize) -> impl Strategy<Value = OneToOneSpec> {
        (
            spec(),
            proptest::collection::vec(star_position(), tables - 2),
        )
            .prop_map(|(spec, more)| OneToOneSpec { more, ..spec })
    }

    /// One star op draw over `tables` tables: two in five on `ja`, the rest
    /// spread over the positions, and each of `ja`'s own keys equal to `key`
    /// half the time (a root row hot at two or more positions at once).
    pub(crate) fn star_draw(tables: usize) -> impl Strategy<Value = OneToOneOpDraw> {
        let own = || prop_oneof![1 => Just(None), 1 => any::<u8>().prop_map(Some)];
        (
            draw(5, true),
            prop_oneof![2 => Just(0u8), 3 => 1u8..tables as u8],
            [own(), own(), own()],
        )
            .prop_map(|(draw, position, own)| OneToOneOpDraw {
                position,
                own_keys: own.map(|raw| raw.unwrap_or(draw.key)),
                ..draw
            })
    }

    fn page() -> impl Strategy<Value = PageDraw> {
        (
            1u8..=STAR_MAX_PAGE_ROWS as u8,
            prop_oneof![1 => Just(false), 1 => Just(true)],
            any::<u64>(),
        )
            .prop_map(|(rows, mixed, shuffle)| PageDraw {
                rows,
                mixed,
                shuffle,
            })
    }

    /// A star tier case (#1781, #1464 section 4): 2 to [`MAX_TABLES`]
    /// tables (the two-table JOIN is the one-position case), one or two
    /// definitions, each with a kind and an `ON` shape per position, and 200
    /// to 600 writes over every table under [`KeyShape::Star`]'s narrow key
    /// space, cut into shuffled pages of 1 to [`STAR_MAX_PAGE_ROWS`] draws of
    /// which half span the positions on one root key ([`shape_pages`]), with
    /// a drawn "late" position. [`STAR_WORKERS`] workers drain pages of at
    /// most a drawn 1 to 15 rows, sealing every 20 to 200 ms over 2 to 4
    /// lanes, stalling up to [`STAR_LOCK_STEP_STALL_MS`] before each commit;
    /// the knobs carry the share of stalled lock steps and re-derived pages.
    /// Builds, pauses and re-reads are the two-table tier's ([`builds`]), and
    /// about one op in a hundred is a `TRUNCATE` of any table. One case in
    /// four also runs its metamorphic twin, under a drawn `JOIN` order.
    pub fn star_case_strategy() -> impl Strategy<Value = StarCase> {
        (2usize..=MAX_TABLES).prop_flat_map(|tables| {
            let positions = (1..tables as u8).collect::<Vec<u8>>();
            (
                proptest::collection::vec(star_spec(tables), 1..=2),
                proptest::collection::vec(star_draw(tables), 200..=600),
                proptest::collection::vec(page(), 8..=32),
                proptest::sample::select(positions.clone()),
                1usize..=STAR_MAX_PAGE_ROWS,
                Just(positions).prop_shuffle(),
                30usize..=150,
                2usize..=4,
                20u64..=200,
                builds(),
                0u64..=STAR_LOCK_STEP_STALL_MS,
                prop_oneof![3 => Just(false), 1 => Just(true)],
            )
                .prop_map(
                    move |(
                        specs,
                        mut draws,
                        pages,
                        late_position,
                        drain_batch_cap,
                        join_order,
                        burst_size,
                        lanes,
                        seal_interval_ms,
                        builds,
                        stall_ms,
                        metamorphic,
                    )| {
                        shape_pages(&mut draws, &pages, tables, late_position);
                        star_case(
                            &specs,
                            tables,
                            &draws,
                            &builds,
                            burst_size,
                            lanes,
                            seal_interval_ms,
                            StarKnobs {
                                drain_batch_cap,
                                lock_step_stall: std::time::Duration::from_millis(stall_ms),
                                stalled_lock_steps_per_mille: STAR_STALLED_LOCK_STEPS_PER_MILLE,
                                rederived_pages_per_mille: STAR_REDERIVED_PAGES_PER_MILLE,
                                late_position,
                                metamorphic,
                            },
                            join_order,
                        )
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
                    more: Vec::new(),
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

    /// The oracle's query is built from the parsed definition, so a parse
    /// that dropped an `ON` pair, swapped `LEFT` for `INNER` or misread a
    /// field would agree with the engine's target. This pins each drawable
    /// shape's oracle, written out by hand from the text the generator
    /// emits.
    #[test]
    fn the_oracle_of_every_drawable_shape_is_its_own_query() {
        let on = |shape| match shape {
            OnShape::PkPk => r#""ja"."id" = "jb"."id""#,
            OnShape::KeyPk => r#""ja"."k" = "jb"."id""#,
            OnShape::KeyUnique => r#""ja"."k" = "jb"."u""#,
            OnShape::PkPlusT => r#""ja"."k" = "jb"."id" and "ja"."t" = "jb"."t""#,
            OnShape::UniquePlusT => r#""ja"."t" = "jb"."t" and "ja"."k" = "jb"."u""#,
            OnShape::Composite => r#""ja"."c1" = "jb"."c1" and "ja"."c2" = "jb"."c2""#,
            OnShape::Text => r#""ja"."s" = "jb"."s""#,
        };
        let fields = [
            r#"("jb"."id")::text as "b_id""#,
            r#"("jb"."y")::text as "b_y""#,
            r#"("jb"."w")::text as "b_w""#,
            r#"("ja"."x")::text as "a_x""#,
            r#"("ja"."s")::text as "a_s""#,
            r#"(("ja"."x" + "jb"."y"))::text as "sum_xy""#,
            r#"(("ja"."x" + coalesce("jb"."y", 0::integer)))::text as "co_xy""#,
            r#"((case when ("jb"."id" is null) then 'none'::text else "jb"."w" end))::text as "case_w""#,
            r#"(("jb"."y" - "ja"."x"))::text as "diff_yx""#,
        ];
        for spec in every_shape() {
            let def = one_to_one_definition("j", &spec);
            assert_eq!(
                crate::oracle::render_join_one_to_one_select(&def, "id"),
                format!(
                    r#"select "ja"."id"::text, {} from "ja" {} "jb" on {}"#,
                    fields.join(", "),
                    if spec.left { "left join" } else { "join" },
                    on(spec.on)
                ),
                "{spec:?}"
            );
        }
    }

    fn draw(joined: bool, kind: JoinOpKind, pick: usize, key: u8, id: u8) -> OneToOneOpDraw {
        OneToOneOpDraw {
            position: u8::from(joined),
            kind,
            pick,
            key,
            id,
            t: 0,
            half_null: false,
            value: Some(2),
            columns: 0,
            own_keys: [0; 3],
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
        let tail = |burst, joined: bool, back| TailTruncate {
            burst,
            position: u8::from(joined),
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
            .map(|(i, d)| (i, d.position == 1))
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

    /// Over the strategy's own cases, replayed as one `jb` lane applies them
    /// (in program order), no `jb` write breaks one of its `UNIQUE`
    /// constraints as Postgres judges them (a `NULL` in a set conflicts with
    /// nothing), every update and delete names a live row, and every burst
    /// keeps its `jb` ops on one lane. This checks the rows the stream
    /// writes, not the generator's own model of them.
    #[cfg(feature = "proptest")]
    #[test]
    fn drawn_streams_never_break_a_joined_unique_constraint() {
        use proptest::strategy::Strategy;
        use std::collections::{BTreeMap, BTreeSet};
        type Row = BTreeMap<String, Option<String>>;
        let strategy = one_to_one_case_strategy().boxed();
        let (mut jb_writes, mut half_null_writes) = (0usize, 0usize);
        for seed in 1..=8 {
            for case in crate::plant_sweep::seeded_cases(&strategy, seed, 40) {
                let mut live: [BTreeMap<String, Row>; 2] = [BTreeMap::new(), BTreeMap::new()];
                for (index, op) in case.program.ops.iter().enumerate() {
                    let (table, written) = match op {
                        Op::Insert { table, row, .. } => {
                            let side = usize::from(table == JOINED_TABLE);
                            let row: Row = row.iter().cloned().collect();
                            let id = row["id"].clone().expect("an id");
                            assert!(
                                live[side].insert(id.clone(), row).is_none(),
                                "op {index} inserts live id {id} into {table}"
                            );
                            (table, Some(id))
                        }
                        Op::Update {
                            table, pk, changes, ..
                        } => {
                            let side = usize::from(table == JOINED_TABLE);
                            let row = live[side]
                                .get_mut(pk)
                                .unwrap_or_else(|| panic!("op {index} updates dead {table} {pk}"));
                            row.extend(changes.iter().cloned());
                            (table, Some(pk.clone()))
                        }
                        Op::Delete { table, pk, .. } => {
                            let side = usize::from(table == JOINED_TABLE);
                            assert!(
                                live[side].remove(pk).is_some(),
                                "op {index} deletes dead {table} {pk}"
                            );
                            (table, None)
                        }
                        Op::Truncate { table, .. } => {
                            live[usize::from(table == JOINED_TABLE)].clear();
                            (table, None)
                        }
                        other => panic!("{other:?}"),
                    };
                    let Some(id) = written.filter(|_| table == JOINED_TABLE) else {
                        continue;
                    };
                    jb_writes += 1;
                    let row = &live[1][&id];
                    half_null_writes += usize::from(row["c1"].is_some() && row["c2"].is_none());
                    for set in [&["u"][..], &["s"], &["c1", "c2"]] {
                        let key: Option<Vec<&String>> =
                            set.iter().map(|c| row[*c].as_ref()).collect();
                        let Some(key) = key else { continue };
                        for (other_id, other) in &live[1] {
                            let theirs: Option<Vec<&String>> =
                                set.iter().map(|c| other[*c].as_ref()).collect();
                            assert!(
                                *other_id == id || theirs.as_ref() != Some(&key),
                                "seed {seed} op {index}: jb {id} and {other_id} share {set:?}"
                            );
                        }
                    }
                }
                for burst in &case.plan.bursts {
                    let lanes: BTreeSet<usize> = burst
                        .lanes
                        .iter()
                        .enumerate()
                        .filter(|(_, lane)| {
                            lane.iter().any(|&i| {
                                matches!(&case.program.ops[i],
                                    Op::Insert { table, .. } | Op::Update { table, .. }
                                    | Op::Delete { table, .. } | Op::Truncate { table, .. }
                                    if table == JOINED_TABLE)
                            })
                        })
                        .map(|(n, _)| n)
                        .collect();
                    assert!(lanes.len() <= 1, "seed {seed}: jb ops on lanes {lanes:?}");
                }
            }
        }
        assert!(jb_writes > 10_000, "{jb_writes}");
        assert!(half_null_writes > 100, "{half_null_writes}");
    }

    /// A star over `tables` tables with every position `LEFT` or `INNER` in
    /// turn, on the shapes in turn, every other one on its own key.
    fn star_spec(tables: usize, offset: usize) -> OneToOneSpec {
        OneToOneSpec {
            left: offset % 2 == 0,
            on: OnShape::ALL[offset % OnShape::ALL.len()],
            fields: vec![OneToOneField::AX, OneToOneField::BY],
            more: (2..tables)
                .map(|p| StarPosition {
                    left: (p + offset) % 2 == 1,
                    on: OnShape::ALL[(p + offset) % OnShape::ALL.len()],
                    own_key: p % 2 == 0,
                    fields: vec![OneToOneField::BId, OneToOneField::AX, OneToOneField::CaseW],
                })
                .collect(),
        }
    }

    /// Every star shape parses as a 1-1 `JOIN` of one clause per position,
    /// each position's fields named apart (a position's root-only field is
    /// left out), and its permuted twin parses with the clauses in the
    /// drawn order.
    #[test]
    fn every_star_definition_parses_in_any_join_order() {
        use trellis::dev::defs::ast::KeySpace;
        for tables in 3..=MAX_TABLES {
            for offset in 0..OnShape::ALL.len() {
                let spec = star_spec(tables, offset);
                let def = one_to_one_definition("j", &spec);
                let KeySpace::JoinOneToOne(join) = &def.key_space else {
                    panic!("{spec:?}")
                };
                let rights: Vec<&str> = join.joins.iter().map(|j| j.right.as_str()).collect();
                assert_eq!(rights, JOINED_TABLES[..tables - 1], "{spec:?}");
                assert_eq!(def.fields.len(), 2 + 2 * (tables - 2), "{spec:?}");
                let order: Vec<u8> = (1..tables as u8).rev().collect();
                let text = star_definition_text("j", &spec, &order);
                let twin = trellis::dev::defs::parse(&text).unwrap_or_else(|e| panic!("{text}: {e:?}"));
                let KeySpace::JoinOneToOne(twin_join) = &twin.key_space else {
                    panic!("{text}")
                };
                let twin_rights: Vec<&str> =
                    twin_join.joins.iter().map(|j| j.right.as_str()).collect();
                let mut reversed = JOINED_TABLES[..tables - 1].to_vec();
                reversed.reverse();
                assert_eq!(twin_rights, reversed, "{text}");
                assert_eq!(twin.fields, def.fields, "{text}");
            }
        }
    }

    /// The star's oracle is its own multi-join query: a clause per position,
    /// in the definition's order, each with its kind and `ON`.
    #[test]
    fn the_oracle_of_a_star_joins_every_position_in_order() {
        let spec = OneToOneSpec {
            left: true,
            on: OnShape::KeyUnique,
            fields: vec![OneToOneField::AX, OneToOneField::BY],
            more: vec![
                StarPosition {
                    left: false,
                    on: OnShape::KeyPk,
                    own_key: true,
                    fields: vec![OneToOneField::BW],
                },
                StarPosition {
                    left: true,
                    on: OnShape::Text,
                    own_key: false,
                    fields: vec![OneToOneField::SumXy],
                },
            ],
        };
        let def = one_to_one_definition("j", &spec);
        let select = r#"select "ja"."id"::text, ("ja"."x")::text as "a_x", ("jb"."y")::text as "b_y", ("jc"."w")::text as "b_w_p2", (("ja"."x" + "jd"."y"))::text as "sum_xy_p3" from "ja""#;
        assert_eq!(
            crate::oracle::render_join_one_to_one_select(&def, "id"),
            format!(
                r#"{select} left join "jb" on "ja"."k" = "jb"."u" join "jc" on "ja"."k2" = "jc"."id" left join "jd" on "ja"."s" = "jd"."s""#
            )
        );
        let twin = trellis::dev::defs::parse(&star_definition_text("j", &spec, &[3, 1, 2]))
            .expect("the twin parses");
        assert_eq!(
            crate::oracle::render_join_one_to_one_select(&twin, "id"),
            format!(
                r#"{select} left join "jd" on "ja"."s" = "jd"."s" left join "jb" on "ja"."k" = "jb"."u" join "jc" on "ja"."k2" = "jc"."id""#
            )
        );
    }

    /// A star's `ja` rows carry a key per own-key position, and a key update
    /// sets them too; each joined table's keys stay unique among its rows.
    #[test]
    fn a_star_program_writes_every_position() {
        let mut draws = Vec::new();
        for i in 0..20u8 {
            let mut d = draw(false, JoinOpKind::Insert, 0, i % 3, i);
            d.position = i % 4;
            d.own_keys = [i % 3, 150, 251];
            draws.push(d);
        }
        let program = build_star_program(&[], 4, KeyShape::Star, &draws, &[]);
        let tables: Vec<&str> = program.tables.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(tables, ["ja", "jb", "jc", "jd"]);
        let ja: Vec<&str> = program.tables[0]
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(ja, ["id", "t", "k", "k2", "k3", "c1", "c2", "s", "x"]);
        let Op::Insert { row, .. } = &program.ops[0] else {
            panic!("{:?}", program.ops[0])
        };
        assert_eq!(key_of(row, "k2"), Some("0".to_string()));
        // 150 is past the hot draws: key 3 + 150 % 77.
        assert_eq!(key_of(row, "k3"), Some("76".to_string()));
        for position in 1..4u8 {
            let keys: Vec<Option<String>> = program
                .ops
                .iter()
                .filter_map(|op| match op {
                    Op::Insert { table, row, .. } if table == table_at(position) => {
                        Some(key_of(row, "u"))
                    }
                    _ => None,
                })
                .collect();
            assert_eq!(keys.len(), 5, "{position}");
            let mut unique = keys.clone();
            unique.sort();
            unique.dedup();
            assert_eq!(unique.len(), keys.len(), "{position}: {keys:?}");
        }
    }

    /// A mixed page spans the positions on its first key, with the late
    /// position's draws last; a plain page is only shuffled.
    #[test]
    fn a_mixed_page_spans_the_positions_with_the_late_one_last() {
        let mut draws: Vec<OneToOneOpDraw> = (0..12u8)
            .map(|i| draw(false, JoinOpKind::Insert, 0, i, i))
            .collect();
        let pages = [
            PageDraw {
                rows: 8,
                mixed: true,
                shuffle: 7,
            },
            PageDraw {
                rows: 4,
                mixed: false,
                shuffle: 9,
            },
        ];
        shape_pages(&mut draws, &pages, 4, 2);
        let mixed = &draws[..8];
        assert!(mixed.iter().all(|d| d.key == mixed[0].key), "{mixed:?}");
        let positions: Vec<u8> = mixed.iter().map(|d| d.position).collect();
        assert_eq!(positions[6..], [2, 2], "{positions:?}");
        for p in 0..4 {
            assert!(positions.contains(&p), "{positions:?}");
        }
        assert!(mixed.iter().filter(|d| d.position == 0).all(|d| d.own_keys == [d.key; 3]));
        let mut plain: Vec<u8> = draws[8..].iter().map(|d| d.key).collect();
        assert!(draws[8..].iter().all(|d| d.position == 0));
        plain.sort_unstable();
        assert_eq!(plain, [8, 9, 10, 11]);
    }

    /// The star tier stays off until the engine runs a star, whatever the
    /// knob says (sub-step 3.8 of #1460 turns it on, and this test with it).
    #[test]
    fn the_star_tier_is_off_until_the_engine_runs_a_star() {
        assert!(!star_tier_enabled());
    }
}
