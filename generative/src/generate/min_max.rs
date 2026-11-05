//! The concurrent tier's `MIN`/`MAX` shape (#1117): one source table, [`TABLE`],
//! read by one to three `GROUP BY` definitions whose fields are `MIN` and
//! `MAX` of its columns, and an op stream shaped like retention.
//!
//! The table is `mm (id bigint pk, g1 bigint, g2 int, v bigint, n numeric,
//! f float8, ts timestamptz, d date, iv interval)`. `g1` and `g2` are the
//! group columns, both nullable (a group key with a `NULL` part is a group of
//! its own, as the target's `UNIQUE NULLS NOT DISTINCT` has it). The others
//! are the arguments ([`Arg`]): every type the engine keeps an extreme of
//! but `time` and `oid`, each nullable but `ts`, which is never `NULL` and
//! grows with each insert (a row's creation time). Values are drawn as
//! ordinals in the type's order and rendered as text, some of them
//! equal-but-distinguishable: `numeric` `1`, `1.0` and `1.00`, `float8`
//! `0` and `-0`, `interval` `26 hours`, `26:00:00` and `1 day 02:00:00`
//! (Postgres keeps the days apart from the hours, so these print
//! differently, and `=` counts a day as 24 hours), and `float8`'s
//! `Infinity`, `-Infinity` and `NaN`.
//!
//! Each definition ([`MinMaxDefDraw`]) groups by `g1`, by `(g1, g2)` or by
//! `g2`, and has `COUNT(*)` and one to four `MIN`/`MAX` fields, sometimes
//! with a `SUM` and `COUNT` of `v` beside them. The backend creates the
//! index `(group columns…, argument)` for each `MIN`/`MAX` argument (#1117
//! rule 4).
//!
//! The op stream ([`build_min_max_program`]) keeps a model of the live rows
//! and draws, each step, one of:
//!
//! - an insert, a third of them with a value past the current domain (a
//!   new extreme, three times in four of an argument and direction a field
//!   reads). Half of those are deleted again 4 to 150 ops later, often
//!   after a seal, and half of those are alone in a group nobody else uses,
//!   whose delete is followed by an insert of another row of the group
//!   (`NULL` arguments one time in two): #1117's F2, F5 and F6 shapes, where
//!   the deletion's page drains before the insertion's;
//! - an insert tied with a group's current extreme, spelled afresh;
//! - an update of one to three arguments, to or from `NULL` too;
//! - an update that moves a row to another group;
//! - a delete of a group's current extreme and every row tied with it;
//! - a retention delete of the oldest rows (lowest `ts`);
//! - a delete of a whole group;
//! - a delete of a random row;
//! - in a quarter of the cases, now and then a `TRUNCATE` (which pins the
//!   table to one lane, `super::concurrent_plan`).
//!
//! The concurrent runner issues the stream in bursts over several lanes,
//! each lane paced from its burst's start (`SteadyLoad::pace`, 0.3 to 2 ms
//! an op: flat out, a burst is written in a few milliseconds, into one or
//! two sealed batches), and the engine drains it with several workers and a
//! short seal cadence, so a row's insertion and its deletion often land in
//! different batches, and two pages of one group race each other's probes.
//! A case may stall pages at their entry lock and before their group lock
//! (`SteadyLoad::stall` with `group_lock_stall`), so a later batch's page of
//! a group can drain before an older one's, or before their commit
//! (`ConcurrentPlan::commit_stall`). Both are process-wide, so only a
//! planted-bug sweep's own process draws them (`min_max_case`'s
//! `stalls`).
//! Later bursts pause and resume a definition (a rebuild) or re-read the
//! table, and one definition may install part way through (a build over
//! rows). The housekeeping (the zero-group GC) runs on a short cadence.
//!
//! The oracle is each definition's own `GROUP BY` query
//! (`crate::oracle`), and every burst is audited by a `Strict` `self_check`.

use trellis::dev::defs::ast::{Expr, FieldDef, KeySpace, Predicate, TransformDef, ValueType};
use trellis::dev::defs::{FloatWidth, IntWidth, PgType};

use crate::model::{Column, Op, OpOutcome, Program, SteadyLoad, Table};

use super::{ActionDraw, ConcurrentCase, add_burst_actions, concurrent_plan, defer_def_install};

/// The tier's source table.
pub const TABLE: &str = "mm";
/// Its primary key.
pub const PK: &str = "id";
/// Its group columns.
pub const GROUP_COLUMNS: [&str; 2] = ["g1", "g2"];

/// One argument column of [`TABLE`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Arg {
    /// `v bigint`.
    V,
    /// `n numeric`, rendered at one of three scales.
    N,
    /// `f float8`, with `-0` and the infinities and `NaN` now and then.
    F,
    /// `ts timestamptz`, never `NULL`: the row's creation time, which grows
    /// with each insert and no update changes.
    Ts,
    /// `d date`.
    D,
    /// `iv interval`, in whole hours, spelled in hours alone or in days and
    /// hours, so equal values print differently (`26:00:00` and
    /// `1 day 02:00:00`).
    Iv,
}

impl Arg {
    pub const ALL: [Arg; 6] = [Arg::V, Arg::N, Arg::F, Arg::Ts, Arg::D, Arg::Iv];

    pub fn column(self) -> &'static str {
        match self {
            Arg::V => "v",
            Arg::N => "n",
            Arg::F => "f",
            Arg::Ts => "ts",
            Arg::D => "d",
            Arg::Iv => "iv",
        }
    }

    pub fn value_type(self) -> ValueType {
        match self {
            Arg::V => ValueType::Integer(IntWidth::Int8),
            Arg::N => ValueType::Numeric,
            Arg::F => ValueType::Float(FloatWidth::Float8),
            Arg::Ts => ValueType::Other(PgType::TimestampTz),
            Arg::D => ValueType::Other(PgType::Date),
            Arg::Iv => ValueType::Other(PgType::Interval),
        }
    }

    fn index(self) -> usize {
        Arg::ALL.iter().position(|a| *a == self).expect("in ALL")
    }

    /// The ordinals an ordinary value is drawn from; a new extreme lies past
    /// either end ([`OUTLIER`]). `ts` is drawn from the clock instead.
    fn domain(self) -> (i64, i64) {
        match self {
            Arg::V | Arg::N => (-30, 30),
            Arg::F => (-40, 40),
            Arg::Ts => (0, 0),
            Arg::D => (0, 60),
            // Hours from one day to three: many ties, each with a spelling
            // in days and hours as well as in hours.
            Arg::Iv => (24, 72),
        }
    }

    /// The text of the value of ordinal `ordinal`; `spelling` picks among
    /// equal values' spellings.
    fn render(self, ordinal: i64, spelling: u64) -> String {
        match self {
            Arg::V => ordinal.to_string(),
            Arg::N => match spelling % 3 {
                0 => ordinal.to_string(),
                1 => format!("{ordinal}.0"),
                _ => format!("{ordinal}.00"),
            },
            Arg::F => match ordinal {
                FLOAT_NAN => "NaN".to_string(),
                FLOAT_INFINITY => "Infinity".to_string(),
                o if o == -FLOAT_INFINITY => "-Infinity".to_string(),
                0 if spelling % 2 == 1 => "-0".to_string(),
                o => format!("{}", o as f64 / 2.0),
            },
            Arg::Ts => {
                let (days, secs) = (ordinal.div_euclid(86_400), ordinal.rem_euclid(86_400));
                format!(
                    "{} {:02}:{:02}:{:02}+00",
                    civil_date(days),
                    secs / 3600,
                    secs / 60 % 60,
                    secs % 60
                )
            }
            Arg::D => civil_date(ordinal),
            Arg::Iv => {
                let (days, hours) = (ordinal.div_euclid(24), ordinal.rem_euclid(24));
                match spelling % 4 {
                    0 => format!("{ordinal} hours"),
                    1 => format!(
                        "{}{:02}:00:00",
                        if ordinal < 0 { "-" } else { "" },
                        ordinal.abs()
                    ),
                    // Each field signed on its own: `-1 days 19:00:00` is
                    // minus five hours.
                    _ if hours == 0 => format!("{days} days"),
                    _ => format!("{days} days {hours:02}:00:00"),
                }
            }
        }
    }
}

/// `float8`'s `Infinity`, as an ordinal: past every finite value drawn.
const FLOAT_INFINITY: i64 = 1_000;
/// `float8`'s `NaN`, which Postgres orders above `Infinity`.
const FLOAT_NAN: i64 = 1_001;
/// How far past its domain a new extreme lies, at most.
const OUTLIER: i64 = 12;

/// The date `days` after 2026-01-01, as `YYYY-MM-DD` (Howard Hinnant's
/// civil-from-days, so no date crate is needed).
fn civil_date(days: i64) -> String {
    // Days from 1970-01-01 to 2026-01-01.
    let z = days + 20_454 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02}")
}

/// How a definition groups.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Grouping {
    G1,
    G1G2,
    G2,
}

impl Grouping {
    fn columns(self) -> Vec<String> {
        match self {
            Grouping::G1 => vec!["g1".into()],
            Grouping::G1G2 => vec!["g1".into(), "g2".into()],
            Grouping::G2 => vec!["g2".into()],
        }
    }

    /// The key of `row`'s group under this grouping.
    fn key(self, row: &Row) -> (Option<i64>, Option<i64>) {
        match self {
            Grouping::G1 => (row.g1, None),
            Grouping::G1G2 => (row.g1, row.g2),
            Grouping::G2 => (None, row.g2),
        }
    }
}

/// One drawn definition: its grouping, its `MIN`/`MAX` fields (`true` for
/// `MAX`), and whether it has `SUM(v)` and `COUNT(v)` beside them.
#[derive(Debug, Clone, PartialEq)]
pub struct MinMaxDefDraw {
    pub grouping: Grouping,
    pub extremes: Vec<(bool, Arg)>,
    pub partials: bool,
}

impl MinMaxDefDraw {
    /// The definition, `target` reading [`TABLE`].
    pub fn definition(&self, target: String) -> TransformDef {
        let group_by = self.grouping.columns();
        let mut fields: Vec<FieldDef> = group_by
            .iter()
            .map(|c| FieldDef {
                name: c.clone(),
                expr: Expr::Column(c.clone()),
            })
            .collect();
        fields.push(FieldDef {
            name: "cnt".into(),
            expr: Expr::FunctionCall {
                name: "COUNT".into(),
                args: Vec::new(),
            },
        });
        let mut seen = Vec::new();
        for &(max, arg) in &self.extremes {
            if seen.contains(&(max, arg)) {
                continue;
            }
            seen.push((max, arg));
            let name = if max { "MAX" } else { "MIN" };
            fields.push(FieldDef {
                name: format!("{}_{}", name.to_lowercase(), arg.column()),
                expr: Expr::FunctionCall {
                    name: name.into(),
                    args: vec![Expr::Column(arg.column().into())],
                },
            });
        }
        if self.partials {
            for name in ["SUM", "COUNT"] {
                fields.push(FieldDef {
                    name: format!("{}_v", name.to_lowercase()),
                    expr: Expr::FunctionCall {
                        name: name.into(),
                        args: vec![Expr::Column("v".into())],
                    },
                });
            }
        }
        TransformDef {
            target,
            source: TABLE.to_string(),
            key_space: KeySpace::Aggregate { group_by },
            fields,
            predicate: Predicate::True,
            without_source_index: false,
            adopt_target: false,
            explicit_source_schema: None,
            explicit_target_schema: None,
        }
    }
}

/// Whether `def` has a `MIN` or `MAX` field, which the runner audits by a
/// strict `self_check`.
pub fn has_extremes(def: &TransformDef) -> bool {
    fn any(expr: &Expr) -> bool {
        match expr {
            Expr::FunctionCall { name, args } => {
                name == "MIN" || name == "MAX" || args.iter().any(any)
            }
            _ => false,
        }
    }
    def.fields.iter().any(|f| any(&f.expr))
}

/// Everything a [`build_min_max_case`] draws, as plain data.
#[derive(Debug, Clone, PartialEq)]
pub struct MinMaxDraws {
    /// Seeds the op stream's own generator ([`build_min_max_program`]).
    pub seed: u64,
    pub defs: Vec<MinMaxDefDraw>,
    /// A definition (modulo how many) installed part way through, at this
    /// many thousandths of the ops, so its build runs over rows.
    pub deferred: Option<(usize, u16)>,
    /// Rows inserted before anything else.
    pub seed_rows: usize,
    /// Ops after the seed rows.
    pub ops: usize,
    /// How many `TRUNCATE`s, placed by the seed.
    pub truncates: usize,
    pub burst_size: usize,
    pub lanes: usize,
    pub workers: usize,
    pub seal_interval_ms: u64,
    pub housekeeping_interval_ms: u64,
    pub actions: Vec<ActionDraw>,
    /// [`SteadyLoad::pace`], from each burst's start, in microseconds: flat
    /// out, a burst is written in a few milliseconds, into one or two
    /// sealed batches, and a row's insertion and its deletion share a page.
    pub pace_micros: u64,
    /// [`SteadyLoad::stall`], in seal intervals, before each page's group
    /// lock too; `None` for no stall.
    pub entry_stall_seals: Option<u32>,
    /// [`crate::model::ConcurrentPlan::commit_stall`], in milliseconds.
    pub commit_stall_ms: Option<u64>,
}

/// One live row of the model.
#[derive(Debug, Clone)]
struct Row {
    pk: i64,
    g1: Option<i64>,
    g2: Option<i64>,
    /// Each argument's ordinal, by [`Arg::index`].
    values: [Option<i64>; 6],
}

/// splitmix64: the op stream's generator, so a seed is the whole stream.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in `lo..=hi`.
    fn range(&mut self, lo: i64, hi: i64) -> i64 {
        lo + (self.next() % (hi - lo + 1) as u64) as i64
    }

    fn chance(&mut self, numerator: u64, denominator: u64) -> bool {
        self.next() % denominator < numerator
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[(self.next() % items.len() as u64) as usize]
    }
}

/// The model the op stream is drawn against: the live rows, in insert order.
struct Model {
    rng: Rng,
    rows: Vec<Row>,
    next_pk: i64,
    /// Every definition's `MIN`/`MAX` fields (`true` for `MAX`).
    fields: Vec<(bool, Arg)>,
    /// `ts`'s clock, in seconds: each insert moves it on.
    clock: i64,
    ops: Vec<Op>,
    /// Rows to delete again soon: (the op count to delete at, pk, whether
    /// a row of the same group replaces it).
    pending: Vec<(usize, i64, bool)>,
}

impl Model {
    fn group_value(&mut self, hi: i64) -> Option<i64> {
        if self.rng.chance(1, 6) {
            None
        } else {
            Some(self.rng.range(0, hi))
        }
    }

    /// A value of `arg`: `NULL` one time in five (never for `ts`), past the
    /// domain's top (`Some(true)`) or bottom (`Some(false)`) when `outlier`
    /// says so, else inside it.
    fn value(&mut self, arg: Arg, outlier: Option<bool>) -> Option<i64> {
        if arg == Arg::Ts {
            self.clock += self.rng.range(1, 90);
            return Some(self.clock);
        }
        if outlier.is_none() && self.rng.chance(1, 5) {
            return None;
        }
        if arg == Arg::F && outlier.is_none() && self.rng.chance(1, 25) {
            return Some(*self.rng.pick(&[FLOAT_INFINITY, -FLOAT_INFINITY, FLOAT_NAN]));
        }
        let (lo, hi) = arg.domain();
        Some(match outlier {
            Some(true) => hi + self.rng.range(1, OUTLIER),
            Some(false) => lo - self.rng.range(1, OUTLIER),
            None => self.rng.range(lo, hi),
        })
    }

    fn text(&mut self, arg: Arg, value: Option<i64>) -> Option<String> {
        let spelling = self.rng.next();
        value.map(|v| arg.render(v, spelling))
    }

    /// Inserts a row: with `outlier`, one or two of its arguments lie past
    /// an end of the domain; with `fresh`, its `g1` is drawn from
    /// [`FRESH_GROUPS`], so it is most likely the only row of its group.
    fn insert(&mut self, outlier: bool, fresh: bool) -> i64 {
        let g1 = if fresh {
            Some(self.rng.range(*FRESH_GROUPS.start(), *FRESH_GROUPS.end()))
        } else {
            self.group_value(4)
        };
        let g2 = self.group_value(1);
        self.insert_into(outlier, (g1, g2), false, None)
    }

    /// Inserts a row into the group `(g1, g2)`; with `sparse`, each argument
    /// but `ts` is `NULL` one time in two; with `tie`, that argument takes
    /// that ordinal (in a spelling of its own).
    fn insert_into(
        &mut self,
        outlier: bool,
        (g1, g2): (Option<i64>, Option<i64>),
        sparse: bool,
        tie: Option<(Arg, i64)>,
    ) -> i64 {
        let pk = self.next_pk;
        self.next_pk += 1;
        let mut values = [None; 6];
        let mut row = vec![
            (PK.to_string(), Some(pk.to_string())),
            ("g1".to_string(), g1.map(|g| g.to_string())),
            ("g2".to_string(), g2.map(|g| g.to_string())),
        ];
        // An outlier insert pushes one or two arguments past an end.
        // Three times in four, the way a definition's field reads it: up for
        // a `MAX`, down for a `MIN`.
        let pushed: Vec<(Arg, bool)> = if outlier {
            (0..self.rng.range(1, 2))
                .map(|_| {
                    if self.rng.chance(3, 4) {
                        let &(max, arg) = self.rng.pick(&self.fields.clone());
                        (arg, max)
                    } else {
                        (*self.rng.pick(&Arg::ALL), self.rng.chance(1, 2))
                    }
                })
                .collect()
        } else {
            Vec::new()
        };
        for arg in Arg::ALL {
            let push = pushed.iter().find(|(a, _)| *a == arg).map(|(_, up)| *up);
            let value = if let Some((_, ordinal)) = tie.filter(|(a, _)| *a == arg) {
                Some(ordinal)
            } else if sparse && arg != Arg::Ts && self.rng.chance(1, 2) {
                None
            } else {
                self.value(arg, push.filter(|_| arg != Arg::Ts))
            };
            values[arg.index()] = value;
            let text = self.text(arg, value);
            row.push((arg.column().to_string(), text));
        }
        self.rows.push(Row { pk, g1, g2, values });
        self.ops.push(Op::Insert {
            table: TABLE.to_string(),
            row,
            expect: OpOutcome::Succeeds,
        });
        pk
    }

    fn delete(&mut self, pk: i64) {
        let Some(at) = self.rows.iter().position(|r| r.pk == pk) else {
            return;
        };
        self.rows.remove(at);
        self.ops.push(Op::Delete {
            table: TABLE.to_string(),
            pk: pk.to_string(),
            expect: OpOutcome::Succeeds,
        });
    }

    fn update(&mut self, at: usize, changes: Vec<(String, Option<String>)>) {
        let pk = self.rows[at].pk;
        self.ops.push(Op::Update {
            table: TABLE.to_string(),
            pk: pk.to_string(),
            changes,
            expect: OpOutcome::Succeeds,
        });
    }

    fn random_row(&mut self) -> usize {
        (self.rng.next() % self.rows.len() as u64) as usize
    }

    /// Updates one to three arguments of a random row (never `ts`), now and
    /// then past an end of the domain.
    fn update_values(&mut self) {
        let at = self.random_row();
        let count = self.rng.range(1, 3);
        let mut changes = Vec::new();
        for _ in 0..count {
            let arg = *self.rng.pick(&[Arg::V, Arg::N, Arg::F, Arg::D, Arg::Iv]);
            if changes.iter().any(|(c, _)| c == arg.column()) {
                continue;
            }
            let outlier = self.rng.chance(1, 6).then(|| self.rng.chance(1, 2));
            let value = self.value(arg, outlier);
            self.rows[at].values[arg.index()] = value;
            let text = self.text(arg, value);
            changes.push((arg.column().to_string(), text));
        }
        self.update(at, changes);
    }

    /// Moves a random row to another group: a new `g1`, `g2` or both.
    fn move_group(&mut self) {
        let at = self.random_row();
        let mut changes = Vec::new();
        let which = self.rng.range(0, 2);
        if which != 1 {
            let g1 = self.group_value(4);
            self.rows[at].g1 = g1;
            changes.push(("g1".to_string(), g1.map(|g| g.to_string())));
        }
        if which != 0 {
            let g2 = self.group_value(1);
            self.rows[at].g2 = g2;
            changes.push(("g2".to_string(), g2.map(|g| g.to_string())));
        }
        self.update(at, changes);
    }

    /// The pks of the rows in the group of a random row under `grouping`.
    fn random_group(&mut self, grouping: Grouping) -> Vec<i64> {
        let at = self.random_row();
        let key = grouping.key(&self.rows[at]);
        self.rows
            .iter()
            .filter(|r| grouping.key(r) == key)
            .map(|r| r.pk)
            .collect()
    }

    /// A random group's current extreme of one of a definition's fields:
    /// the field's argument, the group's rows with their ordinals of it, and
    /// the extreme, if any row has one.
    fn extreme_of_a_group(
        &mut self,
        defs: &[MinMaxDefDraw],
    ) -> (Arg, Vec<(i64, i64)>, Option<i64>) {
        let def = self.rng.pick(defs).clone();
        let &(max, arg) = self.rng.pick(&def.extremes);
        let group = self.random_group(def.grouping);
        let values: Vec<(i64, i64)> = self
            .rows
            .iter()
            .filter(|r| group.contains(&r.pk))
            .filter_map(|r| r.values[arg.index()].map(|v| (r.pk, v)))
            .collect();
        let extreme = if max {
            values.iter().map(|(_, v)| *v).max()
        } else {
            values.iter().map(|(_, v)| *v).min()
        };
        (arg, values, extreme)
    }

    /// Deletes a group's current extreme of one of a definition's fields,
    /// and every row tied with it.
    fn delete_extreme(&mut self, defs: &[MinMaxDefDraw]) {
        let (_, values, extreme) = self.extreme_of_a_group(defs);
        let Some(extreme) = extreme else {
            return;
        };
        for (pk, v) in values {
            if v == extreme {
                self.delete(pk);
            }
        }
    }

    /// Inserts a row tied with a group's current extreme of one of a
    /// definition's fields, in a spelling drawn afresh: equal values printed
    /// differently (`1.0` and `1.00`, `26:00:00` and `1 day 02:00:00`) meet
    /// at the extreme, where `MIN`/`MAX` keep one of them.
    fn tie_extreme(&mut self, defs: &[MinMaxDefDraw]) {
        let (arg, values, extreme) = self.extreme_of_a_group(defs);
        let (Some(extreme), Some(&(pk, _))) = (extreme, values.first()) else {
            return;
        };
        if arg == Arg::Ts {
            return;
        }
        let row = self.rows.iter().find(|r| r.pk == pk).expect("a live row");
        let group = (row.g1, row.g2);
        self.insert_into(false, group, false, Some((arg, extreme)));
    }

    /// Retention: deletes the oldest one to six rows, oldest first.
    fn delete_oldest(&mut self) {
        let count = self.rng.range(1, 6) as usize;
        let oldest: Vec<i64> = self.rows.iter().take(count).map(|r| r.pk).collect();
        for pk in oldest {
            self.delete(pk);
        }
    }

    fn delete_group(&mut self, defs: &[MinMaxDefDraw]) {
        let grouping = self.rng.pick(defs).grouping;
        for pk in self.random_group(grouping) {
            self.delete(pk);
        }
    }

    fn truncate(&mut self) {
        let expect = if self.rows.is_empty() {
            OpOutcome::AffectsNoRows
        } else {
            OpOutcome::Succeeds
        };
        self.rows.clear();
        self.pending.clear();
        self.ops.push(Op::Truncate {
            table: TABLE.to_string(),
            expect,
        });
    }
}

/// The `g1` values a row alone in its group is drawn from: past
/// [`Model::group_value`]'s.
const FRESH_GROUPS: std::ops::RangeInclusive<i64> = 10..=40;

/// How many ops after a new extreme's insert its delete goes, when it is
/// deleted again: enough that a seal often falls between them.
const DELETE_AGAIN: (i64, i64) = (4, 150);

/// The fewest and most live rows the op stream keeps: below the floor it
/// only inserts, above the ceiling it only deletes.
const LIVE_ROWS: (usize, usize) = (8, 120);

/// [`TABLE`]'s shape.
pub fn min_max_table() -> Table {
    let mut columns = vec![
        Column {
            name: PK.into(),
            value_type: crate::model::PRIMARY_KEY_VALUE_TYPE,
        },
        Column {
            name: "g1".into(),
            value_type: ValueType::Integer(IntWidth::Int8),
        },
        Column {
            name: "g2".into(),
            value_type: ValueType::Integer(IntWidth::Int4),
        },
    ];
    columns.extend(Arg::ALL.iter().map(|arg| Column {
        name: arg.column().into(),
        value_type: arg.value_type(),
    }));
    Table {
        name: TABLE.to_string(),
        pk_col: PK.to_string(),
        columns,
        unique_cols: Vec::new(),
        unique_sets: Vec::new(),
        partitioning: None,
    }
}

/// The program `draws` describes: [`TABLE`], its definitions (targets
/// `mm_t1`…), and the op stream the module doc describes, drawn from
/// `draws.seed`.
///
/// # Panics
///
/// If `draws.defs` is empty, or a definition has no `MIN`/`MAX` field.
pub fn build_min_max_program(draws: &MinMaxDraws) -> Program {
    assert!(
        !draws.defs.is_empty() && draws.defs.iter().all(|d| !d.extremes.is_empty()),
        "every min_max definition has a MIN or MAX field — a generator bug"
    );
    let mut model = Model {
        rng: Rng(draws.seed),
        rows: Vec::new(),
        next_pk: 1,
        fields: draws.defs.iter().flat_map(|d| d.extremes.clone()).collect(),
        clock: 0,
        ops: Vec::new(),
        pending: Vec::new(),
    };
    for _ in 0..draws.seed_rows {
        model.insert(false, false);
    }
    let start = model.ops.len();
    let truncate_at: Vec<usize> = (0..draws.truncates)
        .map(|_| start + (model.rng.next() % draws.ops.max(1) as u64) as usize)
        .collect();
    while model.ops.len() < start + draws.ops {
        let now = model.ops.len();
        if truncate_at.contains(&now) {
            model.truncate();
            continue;
        }
        if let Some(at) = model.pending.iter().position(|(due, _, _)| *due <= now) {
            let (_, pk, replace) = model.pending.remove(at);
            let group = model.rows.iter().find(|r| r.pk == pk).map(|r| (r.g1, r.g2));
            if let Some(group) = group {
                model.delete(pk);
                // The F6 shape (#1117): the group's only row deleted and
                // another inserted, so a page that drains before the first
                // row's insertion nets to a zero group whose extremes a
                // probe read.
                if replace {
                    model.insert_into(false, group, true, None);
                }
                continue;
            }
        }
        let live = model.rows.len();
        let step = if live < LIVE_ROWS.0 {
            0
        } else if live > LIVE_ROWS.1 {
            model.rng.range(30, 77)
        } else {
            model.rng.range(0, 77)
        };
        match step {
            0..=21 => {
                // A new extreme, and half of those deleted again some ops
                // later, often after a seal: its insertion's page and its
                // deletion's may then drain in either order. Half of those
                // are alone in their group, whose extreme is `NULL` without
                // them and whose count goes back to zero.
                let outlier = model.rng.chance(1, 3);
                let doomed = outlier && model.rng.chance(1, 2);
                let fresh = doomed && model.rng.chance(1, 2);
                let pk = model.insert(outlier, fresh);
                if doomed {
                    let due = now + model.rng.range(DELETE_AGAIN.0, DELETE_AGAIN.1) as usize;
                    model.pending.push((due, pk, fresh));
                }
            }
            22..=27 => model.tie_extreme(&draws.defs),
            28..=39 => model.update_values(),
            40..=49 => model.move_group(),
            50..=61 => model.delete_extreme(&draws.defs),
            62..=69 => model.delete_oldest(),
            70..=73 => model.delete_group(&draws.defs),
            _ => {
                let at = model.random_row();
                let pk = model.rows[at].pk;
                model.delete(pk);
            }
        }
    }
    let defs: Vec<TransformDef> = draws
        .defs
        .iter()
        .enumerate()
        .map(|(i, d)| d.definition(format!("mm_t{}", i + 1)))
        .collect();
    let mut program = Program {
        tables: vec![min_max_table()],
        def_install_after_op: vec![0; defs.len()],
        defs,
        ops: model.ops,
        restart_after_ops: Vec::new(),
        scale_out_after_ops: Vec::new(),
    };
    if let Some((def, at)) = draws.deferred
        && program.defs.len() > 1
        && program.ops.len() > 2
    {
        let def = def % program.defs.len();
        let ops = program.ops.len();
        let after_op = 1 + usize::from(at.min(1000)) * (ops - 2) / 1000;
        program = defer_def_install(program, def, after_op);
    }
    program
}

/// The concurrent case `draws` describes: [`build_min_max_program`]'s
/// program, issued in bursts of `draws.burst_size` over `draws.lanes`
/// lanes, with the drawn mid-burst actions and stalls.
pub fn build_min_max_case(draws: &MinMaxDraws) -> ConcurrentCase {
    let program = build_min_max_program(draws);
    let mut plan = concurrent_plan(&program, draws.burst_size, draws.lanes);
    plan.steady_load = Some(SteadyLoad {
        pace: std::time::Duration::from_micros(draws.pace_micros),
        pace_from_start: true,
        stall: std::time::Duration::from_millis(
            draws.seal_interval_ms * u64::from(draws.entry_stall_seals.unwrap_or(0)),
        ),
        group_lock_stall: draws.entry_stall_seals.is_some(),
    });
    plan.commit_stall = draws.commit_stall_ms.map(std::time::Duration::from_millis);
    let plan = add_burst_actions(plan, &program, &draws.actions);
    ConcurrentCase {
        program,
        burst_size: draws.burst_size,
        plan,
        workers: draws.workers,
        seal_interval_ms: draws.seal_interval_ms,
        build_chunk_rows: None,
        reconcile_interval_ms: Some(draws.seal_interval_ms),
        housekeeping_interval_ms: Some(draws.housekeeping_interval_ms),
    }
}

#[cfg(feature = "proptest")]
pub use strategy::{min_max_case, min_max_def};

#[cfg(feature = "proptest")]
mod strategy {
    use proptest::prelude::*;

    use super::super::ActionKind;
    use super::*;

    /// One definition of the shape (see the module doc).
    pub fn min_max_def() -> impl Strategy<Value = MinMaxDefDraw> {
        let grouping = prop_oneof![
            2 => Just(Grouping::G1),
            2 => Just(Grouping::G1G2),
            1 => Just(Grouping::G2),
        ];
        let extreme = (any::<bool>(), proptest::sample::select(Arg::ALL.to_vec()));
        (
            grouping,
            prop::collection::vec(extreme, 1..=4),
            prop::bool::weighted(0.3),
        )
            .prop_map(|(grouping, extremes, partials)| MinMaxDefDraw {
                grouping,
                extremes,
                partials,
            })
    }

    fn action() -> impl Strategy<Value = ActionDraw> {
        (
            prop_oneof![
                1 => Just(ActionKind::RequestBackfill),
                2 => Just(ActionKind::PauseResume),
            ],
            0..8usize,
            0..8usize,
            0..=1000u16,
            0..=400u16,
        )
            .prop_map(|(kind, burst, target, start, span)| ActionDraw {
                kind,
                burst,
                target,
                start,
                span,
            })
    }

    /// The concurrent tier's `MIN`/`MAX` case (see the module doc). With
    /// `stalls`, a case may stall pages at their entry lock or before their
    /// commit, which are process-wide: only a planted-bug sweep's own
    /// process asks for them.
    pub fn min_max_case(stalls: bool) -> impl Strategy<Value = ConcurrentCase> {
        let stall = move |weight: f64, range: std::ops::RangeInclusive<u64>| {
            if stalls {
                prop::option::weighted(weight, range).boxed()
            } else {
                Just(None).boxed()
            }
        };
        (
            (
                any::<u64>(),
                prop::collection::vec(min_max_def(), 1..=3),
                prop::option::weighted(0.4, (0..3usize, 0..=1000u16)),
                20..=60usize,
                500..=1_100usize,
                prop::option::weighted(0.25, 1..=2usize),
            ),
            (
                120..=320usize,
                2..=crate::generate::MAX_LANES,
                2..=6usize,
                5..=25u64,
                20..=150u64,
                300..=2_000u64,
                prop::collection::vec(action(), 0..=3),
                stall(0.8, 6..=25),
                stall(0.35, 20..=150),
            ),
        )
            .prop_map(
                |(
                    (seed, defs, deferred, seed_rows, ops, truncates),
                    (
                        burst_size,
                        lanes,
                        workers,
                        seal,
                        housekeeping,
                        pace,
                        actions,
                        entry_stall,
                        commit_stall,
                    ),
                )| {
                    build_min_max_case(&MinMaxDraws {
                        seed,
                        defs,
                        deferred,
                        seed_rows,
                        ops,
                        truncates: truncates.unwrap_or(0),
                        burst_size,
                        lanes,
                        workers,
                        seal_interval_ms: seal,
                        housekeeping_interval_ms: housekeeping,
                        pace_micros: pace,
                        actions,
                        entry_stall_seals: entry_stall.map(|s| s as u32),
                        commit_stall_ms: commit_stall,
                    })
                },
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn draws(seed: u64, truncates: usize) -> MinMaxDraws {
        MinMaxDraws {
            seed,
            defs: vec![
                MinMaxDefDraw {
                    grouping: Grouping::G1,
                    extremes: vec![(false, Arg::Ts), (true, Arg::F)],
                    partials: true,
                },
                MinMaxDefDraw {
                    grouping: Grouping::G1G2,
                    extremes: vec![(true, Arg::V), (false, Arg::N), (false, Arg::Iv)],
                    partials: false,
                },
            ],
            deferred: Some((1, 500)),
            seed_rows: 30,
            ops: 600,
            truncates,
            burst_size: 200,
            lanes: 3,
            workers: 3,
            seal_interval_ms: 10,
            housekeeping_interval_ms: 100,
            actions: Vec::new(),
            pace_micros: 500,
            entry_stall_seals: None,
            commit_stall_ms: None,
        }
    }

    #[test]
    fn civil_date_counts_from_2026() {
        assert_eq!(civil_date(0), "2026-01-01");
        assert_eq!(civil_date(31), "2026-02-01");
        assert_eq!(civil_date(365), "2027-01-01");
        assert_eq!(civil_date(-1), "2025-12-31");
    }

    #[test]
    fn values_render_in_their_types_order() {
        assert_eq!(Arg::F.render(FLOAT_NAN, 0), "NaN");
        assert_eq!(Arg::F.render(0, 1), "-0");
        assert_eq!(Arg::F.render(3, 0), "1.5");
        assert_eq!(Arg::N.render(-4, 2), "-4.00");
        assert_eq!(Arg::Ts.render(3_661, 0), "2026-01-01 01:01:01+00");
        assert_eq!(Arg::Iv.render(26, 0), "26 hours");
        assert_eq!(Arg::Iv.render(26, 1), "26:00:00");
        assert_eq!(Arg::Iv.render(26, 2), "1 days 02:00:00");
        assert_eq!(Arg::Iv.render(48, 3), "2 days");
        assert_eq!(Arg::Iv.render(-5, 1), "-05:00:00");
        assert_eq!(Arg::Iv.render(-5, 2), "-1 days 19:00:00");
    }

    /// The stream is a pure function of its draws, every op names a row the
    /// model holds (so its expected outcome is right in any lane order),
    /// and it holds every shape the module doc promises.
    #[test]
    fn the_stream_is_deterministic_and_draws_every_shape() {
        let a = build_min_max_program(&draws(7, 0));
        assert_eq!(a, build_min_max_program(&draws(7, 0)));
        assert_ne!(a.ops, build_min_max_program(&draws(8, 0)).ops);
        let mut live = std::collections::HashSet::new();
        let (mut updates, mut deletes, mut moves) = (0, 0, 0);
        for op in &a.ops {
            match op {
                Op::Insert { row, .. } => {
                    let pk = row[0].1.clone().expect("a pk");
                    assert!(live.insert(pk), "a pk is never reused");
                }
                Op::Update { pk, changes, .. } => {
                    assert!(live.contains(pk), "an update names a live row");
                    updates += 1;
                    moves += usize::from(changes.iter().any(|(c, _)| c.starts_with('g')));
                }
                Op::Delete { pk, .. } => {
                    assert!(live.remove(pk), "a delete names a live row");
                    deletes += 1;
                }
                other => panic!("no truncate drawn: {other:?}"),
            }
        }
        // A step that deletes several rows can run past the count.
        assert!((630..640).contains(&a.ops.len()), "{}", a.ops.len());
        assert!(
            updates > 50 && deletes > 50 && moves > 10,
            "{updates} {deletes} {moves}"
        );
        assert_eq!(
            a.def_install_after_op.iter().filter(|&&at| at > 0).count(),
            1
        );
        assert!(a.defs.iter().all(has_extremes));
    }

    #[test]
    fn a_truncate_empties_the_model() {
        let program = build_min_max_program(&draws(3, 2));
        let truncates = program
            .ops
            .iter()
            .filter(|op| matches!(op, Op::Truncate { .. }))
            .count();
        assert!(truncates >= 1);
        let case = build_min_max_case(&draws(3, 2));
        // A truncated table is one lane.
        assert!(case.plan.bursts.iter().all(|b| b.lanes.len() == 1));
    }
}
