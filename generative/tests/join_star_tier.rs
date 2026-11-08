//! Meta-tests of the 1-1 JOIN tier's star tier (#1781, `generate::join_one_to_one::star_case_strategy`):
//! no engine apply runs here, so they run while a star's apply isn't built.
//!
//! - The two-table tiers draw exactly what they drew before the star tier:
//!   the star tier's draws are its own, and the two-table JOIN's generator
//!   (the one-position case of the star's) resolves every seed's draws to the
//!   same cases.
//! - The star strategy reaches what #1464 section 4 asks of it, with the tier
//!   enabled artificially (the strategy is drawn directly, without
//!   `star_tier_enabled`): every table count, kind and position, root rows
//!   hot at two or more positions at once, mixed pages spanning positions,
//!   the late position, the page sizes and the metamorphic draw.
//! - The oracle (`generative::oracle::render_join_one_to_one_select`) is the
//!   star's multi-join over a real Postgres, against this file's own model of
//!   a star, on hand-built states at 3, 4 and 5 tables and on drawn cases,
//!   and gives the same rows under a permuted `JOIN` order.
//! - With the knob on, define admits every drawn star definition.

use std::collections::{BTreeMap, BTreeSet};

use generative::generate::join::{JoinCase, JoinOpKind, KeyShape, STAR_HOT_KEYS};
use generative::generate::join_chained::chained_case_strategy;
use generative::generate::join_one_to_one::{
    MAX_TABLES, OnShape, OneToOneField, OneToOneOpDraw, OneToOneSpec, STAR_WORKERS, StarCase,
    StarPosition, build_star_program, one_to_one_case_strategy, own_key_column, star_case_strategy,
    star_definition_text, table_at,
};
use generative::model::{Op, Program, Table};
use proptest::strategy::Strategy;
use testkit::TestCluster;
use trellis::dev::defs::ast::{KeySpace, TransformDef, ValueType};

/// FNV-1a over `bytes`: a hash that never changes across Rust releases.
fn fnv(hash: u64, bytes: &[u8]) -> u64 {
    bytes.iter().fold(hash, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// A digest of what a case runs: its ops, its definitions' text, its plan,
/// and its engine settings.
fn digest(cases: &[JoinCase]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325;
    for case in cases {
        for def in &case.program.defs {
            hash = fnv(
                hash,
                trellis::dev::defs::render_definition_text(def).as_bytes(),
            );
        }
        let rest = format!(
            "{:?}|{:?}|{:?}|{:?}|{}|{}|{}|{}",
            case.program.ops,
            case.program.def_install_after_op,
            case.plan,
            case.shape,
            case.burst_size,
            case.workers,
            case.seal_interval_ms,
            case.program.tables.len()
        );
        hash = fnv(hash, rest.as_bytes());
    }
    hash
}

fn digests(strategy: &proptest::strategy::BoxedStrategy<JoinCase>) -> Vec<u64> {
    (1..=4)
        .map(|seed| digest(&generative::plant_sweep::seeded_cases(strategy, seed, 12)))
        .collect()
}

/// The two-table 1-1 JOIN tier and the chained-reader tier draw, seed for
/// seed, the cases they drew before the generator took positions: the
/// digests of their sweep budget's cases (4 seeds × 12), recorded on the
/// generator before the star tier (#1781). A change that means to change
/// what they draw updates these.
#[test]
fn the_two_table_tiers_draw_what_they_drew_before_the_star_tier() {
    assert_eq!(
        digests(&one_to_one_case_strategy().boxed()),
        [
            17_962_304_100_747_754_684,
            4_091_506_942_938_126_918,
            8_600_531_309_418_350_733,
            7_267_421_071_787_496_964
        ]
    );
    assert_eq!(
        digests(&chained_case_strategy().boxed()),
        [
            13_184_901_160_770_402_170,
            5_538_369_240_495_716_062,
            8_137_699_462_753_141_007,
            1_132_712_386_592_658_724
        ]
    );
}

/// 4 seeds × 40 star cases.
fn star_cases() -> Vec<StarCase> {
    let strategy = star_case_strategy().boxed();
    (1..=4)
        .flat_map(|seed| generative::plant_sweep::seeded_cases(&strategy, seed, 40))
        .collect()
}

fn op_table(op: &Op) -> &str {
    match op {
        Op::Insert { table, .. }
        | Op::Update { table, .. }
        | Op::Delete { table, .. }
        | Op::Truncate { table, .. } => table,
        other => panic!("the star tier draws no {other:?}"),
    }
}

/// Every table count from 2 to 5 is drawn; at each, every position is
/// drawn `LEFT` and `INNER`, on its own key and on `ja.k`, and written,
/// truncates included; and every definition joins one clause per position.
#[test]
fn the_star_strategy_draws_every_table_count_kind_and_position() {
    let cases = star_cases();
    let mut kinds: BTreeSet<(usize, u8, bool)> = BTreeSet::new();
    let mut own_keys: BTreeSet<(usize, u8, bool)> = BTreeSet::new();
    let mut written: BTreeSet<(usize, u8)> = BTreeSet::new();
    let mut truncated: BTreeSet<(usize, u8)> = BTreeSet::new();
    let mut counts: BTreeMap<usize, usize> = BTreeMap::new();
    for case in &cases {
        *counts.entry(case.tables).or_insert(0) += 1;
        assert_eq!(case.case.program.tables.len(), case.tables);
        assert_eq!(case.case.workers, STAR_WORKERS);
        assert_eq!(case.case.shape, KeyShape::Star);
        for (spec, def) in case.specs.iter().zip(&case.case.program.defs) {
            let KeySpace::JoinOneToOne(join) = &def.key_space else {
                panic!("{def:?}")
            };
            assert_eq!(join.joins.len(), case.tables - 1, "{spec:?}");
            for p in 1..case.tables as u8 {
                kinds.insert((case.tables, p, spec.left_at(p)));
            }
            for (i, position) in spec.more.iter().enumerate() {
                if position.on.reads_root_key() {
                    own_keys.insert((case.tables, i as u8 + 2, position.own_key));
                }
            }
        }
        for op in &case.case.program.ops {
            let p = generative::generate::join_one_to_one::position_of(op_table(op))
                .expect("a star table");
            written.insert((case.tables, p));
            if matches!(op, Op::Truncate { .. }) {
                truncated.insert((case.tables, p));
            }
        }
    }
    assert_eq!(
        counts.keys().copied().collect::<Vec<_>>(),
        [2, 3, 4, 5],
        "{counts:?}"
    );
    assert!(counts.values().all(|&n| n >= 20), "{counts:?}");
    for tables in 2..=MAX_TABLES {
        for p in 1..tables as u8 {
            for left in [true, false] {
                assert!(
                    kinds.contains(&(tables, p, left)),
                    "{tables} tables, position {p}"
                );
            }
            if p >= 2 {
                for own in [true, false] {
                    assert!(own_keys.contains(&(tables, p, own)), "{tables} tables, {p}");
                }
            }
        }
        for p in 0..tables as u8 {
            assert!(
                written.contains(&(tables, p)),
                "{tables} tables, position {p}"
            );
        }
    }
    assert!(truncated.len() >= 10, "{truncated:?}");
}

/// The value of `column` an insert or key update writes.
fn written<'a>(columns: &'a [(String, Option<String>)], column: &str) -> Option<&'a str> {
    columns
        .iter()
        .find(|(c, _)| c == column)
        .and_then(|(_, v)| v.as_deref())
}

/// The narrow key space has its hot keys, and a root row is often hot at two
/// or more positions at once: its `k` and an own key `k{p}` on one hot key.
#[test]
fn the_star_strategy_draws_root_rows_hot_at_several_positions() {
    let (mut root_writes, mut hot, mut hot_at_two, mut hot_at_three) = (0, 0, 0, 0);
    for case in star_cases().iter().filter(|c| c.tables >= 3) {
        for op in &case.case.program.ops {
            let columns = match op {
                Op::Insert { table, row, .. } if table == "ja" => row,
                Op::Update { table, changes, .. } if table == "ja" => changes,
                _ => continue,
            };
            let Some(k) = written(columns, "k") else {
                continue;
            };
            root_writes += 1;
            let is_hot = k.parse::<u8>().unwrap() < STAR_HOT_KEYS;
            hot += usize::from(is_hot);
            let positions = 1
                + (2..case.tables as u8)
                    .filter(|&p| written(columns, &own_key_column(p)) == Some(k))
                    .count();
            hot_at_two += usize::from(is_hot && positions >= 2);
            hot_at_three += usize::from(is_hot && positions >= 3);
        }
    }
    assert!(root_writes > 5_000, "{root_writes}");
    // Two draws in five are hot.
    assert!(hot * 10 > root_writes * 3, "{hot} of {root_writes}");
    assert!(hot_at_two * 10 > hot * 5, "{hot_at_two} of {hot}");
    assert!(hot_at_three > 500, "{hot_at_three}");
}

/// Mixed pages: one key written at the root and at every joined position
/// within a few consecutive ops of the stream, the late position's write of
/// it last. Counted over the stream cut into tiles of 30 ops (two of the
/// largest pages).
#[test]
fn the_star_strategy_draws_mixed_pages_spanning_positions() {
    let (mut tiles, mut spanning, mut late_last) = (0, 0, 0);
    for case in star_cases().iter().filter(|c| c.tables >= 3) {
        let key_of = |op: &Op| -> Option<(u8, String)> {
            let (table, columns) = match op {
                Op::Insert { table, row, .. } => (table, row),
                Op::Update { table, changes, .. } => (table, changes),
                _ => return None,
            };
            let p = generative::generate::join_one_to_one::position_of(table)?;
            let key = written(columns, if p == 0 { "k" } else { "u" })?;
            Some((p, key.to_string()))
        };
        for tile in case.case.program.ops.chunks(30) {
            tiles += 1;
            let mut by_key: BTreeMap<String, Vec<u8>> = BTreeMap::new();
            for (p, key) in tile.iter().filter_map(key_of) {
                by_key.entry(key).or_default().push(p);
            }
            if let Some(positions) = by_key
                .values()
                .find(|ps| ps.iter().collect::<BTreeSet<_>>().len() == case.tables)
            {
                spanning += 1;
                late_last += usize::from(*positions.last().unwrap() == case.knobs.late_position);
            }
        }
    }
    assert!(spanning * 4 > tiles, "{spanning} of {tiles} tiles");
    assert!(late_last * 2 > spanning, "{late_last} of {spanning}");
}

/// The knobs: every late position, page sizes from 1 to 15, the
/// metamorphic case about one in four with an order other than the
/// definition's, the stall and the shares #1464 section 4 names.
#[test]
fn the_star_strategy_draws_its_knobs() {
    let cases = star_cases();
    let late: BTreeSet<(usize, u8)> = cases
        .iter()
        .map(|c| (c.tables, c.knobs.late_position))
        .collect();
    for tables in 2..=MAX_TABLES {
        for p in 1..tables as u8 {
            assert!(late.contains(&(tables, p)), "{tables} tables, late {p}");
        }
    }
    let caps: BTreeSet<usize> = cases.iter().map(|c| c.knobs.drain_batch_cap).collect();
    assert!(caps.contains(&1) && caps.contains(&15), "{caps:?}");
    assert!(caps.iter().all(|c| (1..=15).contains(c)), "{caps:?}");
    let metamorphic = cases.iter().filter(|c| c.knobs.metamorphic).count();
    assert!(
        (20..=70).contains(&metamorphic),
        "{metamorphic} of {}",
        cases.len()
    );
    let permuted = cases
        .iter()
        .filter(|c| c.join_order.windows(2).any(|w| w[0] > w[1]))
        .count();
    assert!(permuted > 60, "{permuted}");
    for case in &cases {
        let mut order = case.join_order.clone();
        order.sort_unstable();
        assert_eq!(order, (1..case.tables as u8).collect::<Vec<_>>());
        assert_eq!(case.knobs.stalled_lock_steps_per_mille, 300);
        assert_eq!(case.knobs.rederived_pages_per_mille, 120);
        assert!(case.knobs.lock_step_stall <= std::time::Duration::from_millis(3));
        assert_eq!(
            case.case.plan.commit_stall,
            Some(case.knobs.lock_step_stall)
        );
        let twin = case.permuted_program();
        assert_eq!(twin.ops, case.case.program.ops);
        for (def, original) in twin.defs.iter().zip(&case.case.program.defs) {
            assert_eq!(def.fields, original.fields);
        }
    }
}

/// Replayed in program order, no joined table's write breaks one of its
/// `UNIQUE` constraints, every update and delete names a live row, and every
/// burst keeps each joined table's ops on one lane.
#[test]
fn star_streams_keep_each_joined_key_unique_and_on_one_lane() {
    let mut joined_writes = 0usize;
    for case in star_cases() {
        let mut model = Model::new(&case.case.program.tables);
        for (index, op) in case.case.program.ops.iter().enumerate() {
            model.apply(op);
            let (table, id) = match op {
                Op::Insert { table, row, .. } => (table, written(row, "id").unwrap().to_string()),
                Op::Update {
                    table, pk, changes, ..
                } => (table, written(changes, "id").unwrap_or(pk).to_string()),
                _ => continue,
            };
            if table == "ja" {
                continue;
            }
            joined_writes += 1;
            let rows = &model.tables[table];
            let row = &rows[&id];
            for set in [&["u"][..], &["s"], &["c1", "c2"]] {
                let key: Option<Vec<&String>> = set.iter().map(|c| row[*c].as_ref()).collect();
                let Some(key) = key else { continue };
                for (other_id, other) in rows {
                    let theirs: Option<Vec<&String>> =
                        set.iter().map(|c| other[*c].as_ref()).collect();
                    assert!(
                        *other_id == id || theirs.as_ref() != Some(&key),
                        "op {index}: {table} {id} and {other_id} share {set:?}"
                    );
                }
            }
        }
        for burst in &case.case.plan.bursts {
            for p in 1..case.tables as u8 {
                let lanes = burst
                    .lanes
                    .iter()
                    .filter(|lane| {
                        lane.iter()
                            .any(|&i| op_table(&case.case.program.ops[i]) == table_at(p))
                    })
                    .count();
                assert!(lanes <= 1, "position {p} on {lanes} lanes");
            }
        }
    }
    assert!(joined_writes > 20_000, "{joined_writes}");
}

/// A row of the model: each column's text.
type Row = BTreeMap<String, Option<String>>;

/// This file's own model of the source tables, from the ops alone.
struct Model {
    tables: BTreeMap<String, BTreeMap<String, Row>>,
}

impl Model {
    fn new(tables: &[Table]) -> Self {
        Model {
            tables: tables
                .iter()
                .map(|t| (t.name.clone(), BTreeMap::new()))
                .collect(),
        }
    }

    fn apply(&mut self, op: &Op) {
        match op {
            Op::Insert { table, row, .. } => {
                let row: Row = row.iter().cloned().collect();
                let id = row["id"].clone().expect("an id");
                assert!(
                    self.tables
                        .get_mut(table)
                        .unwrap()
                        .insert(id, row)
                        .is_none(),
                    "{op:?}"
                );
            }
            Op::Update {
                table, pk, changes, ..
            } => {
                let rows = self.tables.get_mut(table).unwrap();
                let mut row = rows.remove(pk).unwrap_or_else(|| panic!("{op:?}"));
                row.extend(changes.iter().cloned());
                let id = row["id"].clone().expect("an id");
                rows.insert(id, row);
            }
            Op::Delete { table, pk, .. } => {
                assert!(
                    self.tables.get_mut(table).unwrap().remove(pk).is_some(),
                    "{op:?}"
                );
            }
            Op::Truncate { table, .. } => self.tables.get_mut(table).unwrap().clear(),
            other => panic!("{other:?}"),
        }
    }

    /// `spec`'s rows as this model joins them: per `ja` row, the joined row
    /// at each position whose `ON` columns all equal `ja`'s (a `NULL` equals
    /// nothing), dropped when an `INNER` position has none; then its fields
    /// in the select list's order.
    fn rows(&self, spec: &OneToOneSpec) -> BTreeMap<String, Vec<Option<String>>> {
        let mut out = BTreeMap::new();
        'rows: for (id, a) in &self.tables["ja"] {
            let mut matched: Vec<Option<&Row>> = Vec::new();
            for p in 1..spec.tables() as u8 {
                let pairs = on_pairs(spec, p);
                let b = self.tables[table_at(p)].values().find(|b| {
                    pairs.iter().all(|(ac, bc)| {
                        a[ac.as_str()].is_some() && a[ac.as_str()] == b[bc.as_str()]
                    })
                });
                if b.is_none() && !spec.left_at(p) {
                    continue 'rows;
                }
                matched.push(b);
            }
            let mut values = Vec::new();
            let mut seen = Vec::new();
            for field in &spec.fields {
                if !seen.contains(field) {
                    seen.push(*field);
                    values.push(field_value(*field, a, matched[0]));
                }
            }
            for (i, position) in spec.more.iter().enumerate() {
                let mut seen = Vec::new();
                for field in position.fields.iter().filter(|f| f.reads_joined()) {
                    if !seen.contains(field) {
                        seen.push(*field);
                        values.push(field_value(*field, a, matched[i + 1]));
                    }
                }
            }
            out.insert(id.clone(), values);
        }
        out
    }
}

/// Position `p`'s `ON` columns, `(ja's, the joined table's)`.
fn on_pairs(spec: &OneToOneSpec, p: u8) -> Vec<(String, String)> {
    let (on, own) = match p {
        1 => (spec.on, false),
        p => {
            let position = &spec.more[usize::from(p) - 2];
            (position.on, position.own_key)
        }
    };
    let k = if own {
        own_key_column(p)
    } else {
        "k".to_string()
    };
    let pairs: Vec<(&str, &str)> = match on {
        OnShape::PkPk => vec![("id", "id")],
        OnShape::KeyPk => vec![(&k, "id")],
        OnShape::KeyUnique => vec![(&k, "u")],
        OnShape::PkPlusT => vec![(&k, "id"), ("t", "t")],
        OnShape::UniquePlusT => vec![("t", "t"), (&k, "u")],
        OnShape::Composite => vec![("c1", "c1"), ("c2", "c2")],
        OnShape::Text => vec![("s", "s")],
    };
    pairs
        .into_iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect()
}

fn field_value(field: OneToOneField, a: &Row, b: Option<&Row>) -> Option<String> {
    let int = |v: &Option<String>| v.as_deref().map(|v| v.parse::<i64>().unwrap());
    let col = |column: &str| b.and_then(|b| b[column].clone());
    let x = int(&a["x"]);
    let y = int(&col("y"));
    match field {
        OneToOneField::BId => col("id"),
        OneToOneField::BY => col("y"),
        OneToOneField::BW => col("w"),
        OneToOneField::AX => a["x"].clone(),
        OneToOneField::AS => a["s"].clone(),
        OneToOneField::SumXy => Some(x? + y?).map(|v| v.to_string()),
        OneToOneField::CoalesceXy => x.map(|x| (x + y.unwrap_or(0)).to_string()),
        OneToOneField::CaseW => match b {
            None => Some("none".to_string()),
            Some(_) => col("w"),
        },
        OneToOneField::DiffYx => Some(y? - x?).map(|v| v.to_string()),
    }
}

fn literal(value: &Option<String>) -> String {
    match value {
        Some(v) => format!("'{}'", v.replace('\'', "''")),
        None => "null".to_string(),
    }
}

fn op_sql(op: &Op) -> String {
    let ident = |name: &str| format!("\"{name}\"");
    match op {
        Op::Insert { table, row, .. } => format!(
            "insert into {} ({}) values ({})",
            ident(table),
            row.iter()
                .map(|(c, _)| ident(c))
                .collect::<Vec<_>>()
                .join(", "),
            row.iter()
                .map(|(_, v)| literal(v))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Op::Update {
            table, pk, changes, ..
        } => format!(
            "update {} set {} where id = {pk}",
            ident(table),
            changes
                .iter()
                .map(|(c, v)| format!("{} = {}", ident(c), literal(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Op::Delete { table, pk, .. } => format!("delete from {} where id = {pk}", ident(table)),
        Op::Truncate { table, .. } => format!("truncate {}", ident(table)),
        other => panic!("{other:?}"),
    }
}

fn ddl(table: &Table) -> String {
    let columns: Vec<String> = table
        .columns
        .iter()
        .map(|c| {
            let ty = match c.value_type {
                ValueType::Integer(width) => width.pg_name(),
                ValueType::Text => "text",
                other => panic!("the star tier has no {other:?} column"),
            };
            let constraint = if c.name == table.pk_col {
                " primary key"
            } else if table.unique_cols.contains(&c.name) {
                " unique"
            } else {
                ""
            };
            format!("\"{}\" {ty}{constraint}", c.name)
        })
        .chain(
            table
                .unique_sets
                .iter()
                .map(|set| format!("unique ({})", set.join(", "))),
        )
        .collect();
    format!("create table \"{}\" ({})", table.name, columns.join(", "))
}

/// The tables of `program`, written by its ops, in a fresh database.
async fn written_database(
    cluster: &TestCluster,
    program: &Program,
) -> (testkit::TestDatabase, tokio_postgres::Client) {
    let db = cluster.create_isolated_database().await;
    let (client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(connection);
    for table in &program.tables {
        client.batch_execute(&ddl(table)).await.expect("create");
    }
    for op in &program.ops {
        client
            .execute(op_sql(op).as_str(), &[])
            .await
            .unwrap_or_else(|e| panic!("{op:?}: {e}"));
    }
    (db, client)
}

/// `def`'s oracle rows, by `ja`'s primary key.
async fn oracle_rows(
    client: &tokio_postgres::Client,
    def: &TransformDef,
) -> BTreeMap<String, Vec<Option<String>>> {
    let sql = generative::oracle::render_join_one_to_one_select(def, "id");
    let mut out = BTreeMap::new();
    for row in client
        .query(sql.as_str(), &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
    {
        let id: String = row.get(0);
        let values = (1..row.len()).map(|i| row.get(i)).collect();
        assert!(out.insert(id, values).is_none(), "{sql}: a repeated row");
    }
    out
}

/// Checks each spec's oracle over `program`'s written tables against the
/// model, in the definition's `JOIN` order and in `orders`; returns how many
/// rows matched at every position, and how many kept an unmatched `LEFT`
/// position.
async fn check_oracle(
    client: &tokio_postgres::Client,
    program: &Program,
    specs: &[OneToOneSpec],
    orders: &[Vec<u8>],
) -> (usize, usize) {
    let mut model = Model::new(&program.tables);
    for op in &program.ops {
        model.apply(op);
    }
    let (mut full, mut partial) = (0, 0);
    for spec in specs {
        let want = model.rows(spec);
        let identity: Vec<u8> = (1..spec.tables() as u8).collect();
        for order in std::iter::once(&identity).chain(orders) {
            let text = star_definition_text("j", spec, order);
            let def = trellis::dev::defs::parse(&text).expect("parses");
            assert_eq!(oracle_rows(client, &def).await, want, "{text}");
        }
        for values in want.values() {
            // A `BId` field is the first select item at position 1.
            if spec.fields.first() == Some(&OneToOneField::BId) {
                if values[0].is_some() {
                    full += 1;
                } else {
                    partial += 1;
                }
            }
        }
    }
    (full, partial)
}

/// Every order of `1..tables` for a small star, the reversed and a rotated
/// one past it.
fn orders(tables: usize) -> Vec<Vec<u8>> {
    let positions: Vec<u8> = (1..tables as u8).collect();
    let mut reversed = positions.clone();
    reversed.reverse();
    let mut rotated = positions.clone();
    rotated.rotate_left(1);
    vec![reversed, rotated]
}

/// The kind tuples of `trellis/tests/support/star.rs` (`KINDS_2` to
/// `KINDS_4`): all `LEFT`, then one `LEFT` among `INNER`s per position, and
/// all `INNER`.
fn kind_tuples(positions: usize) -> Vec<Vec<bool>> {
    let mut tuples = vec![vec![true; positions], vec![false; positions]];
    for p in 0..positions {
        let mut tuple = vec![false; positions];
        tuple[p] = true;
        tuples.push(tuple);
    }
    tuples
}

/// A hand-built star state at `tables` tables: every position written
/// over six keys, with the root's own keys on the same key, another, or
/// `NULL`; rows inserted, rekeyed, updated and deleted.
fn hand_built(tables: usize) -> (Program, Vec<OneToOneSpec>) {
    let mut draws = Vec::new();
    for i in 0..120u8 {
        draws.push(OneToOneOpDraw {
            position: i % tables as u8,
            kind: match i % 7 {
                0..=3 => JoinOpKind::Insert,
                4 | 5 => JoinOpKind::Update,
                _ => JoinOpKind::Delete,
            },
            pick: usize::from(i) * 5,
            // Raw draws over the hot keys and a few others.
            key: [0, 1, 2, 100, 101, 102][usize::from(i.wrapping_mul(7)) % 6],
            id: i.wrapping_mul(3),
            t: i % 3,
            half_null: i % 11 == 5,
            value: (i % 13 != 0).then_some(i as i8 % 50 - 10),
            columns: [1, 8, 9, 3, 4][usize::from(i) % 5],
            own_keys: [
                i.wrapping_mul(7) % 3,
                100 + i % 3,
                if i % 9 == 0 { 252 } else { 1 },
            ],
            on_key: i % 4 == 0,
        });
    }
    let positions = tables - 1;
    let specs: Vec<OneToOneSpec> = kind_tuples(positions)
        .into_iter()
        .enumerate()
        .map(|(n, kinds)| OneToOneSpec {
            left: kinds[0],
            on: OnShape::ALL[n % OnShape::ALL.len()],
            fields: vec![
                OneToOneField::BId,
                OneToOneField::AX,
                OneToOneField::CoalesceXy,
                OneToOneField::CaseW,
            ],
            more: (2..tables)
                .map(|p| StarPosition {
                    left: kinds[p - 1],
                    on: OnShape::ALL[(n + p) % OnShape::ALL.len()],
                    own_key: (n + p) % 2 == 0,
                    fields: vec![
                        OneToOneField::BId,
                        OneToOneField::SumXy,
                        OneToOneField::CaseW,
                    ],
                })
                .collect(),
        })
        .collect();
    let program = build_star_program(&[], tables, KeyShape::Star, &draws, &[]);
    (program, specs)
}

/// The oracle is the star's multi-join on hand-built states at 3, 4 and 5
/// tables, for every kind tuple the pins run, in any `JOIN` order.
#[tokio::test]
async fn the_star_oracle_is_the_multi_join_at_three_to_five_tables() {
    let cluster = TestCluster::start();
    for tables in 3..=MAX_TABLES {
        let (program, specs) = hand_built(tables);
        let (_db, client) = written_database(&cluster, &program).await;
        let (full, partial) = check_oracle(&client, &program, &specs, &orders(tables)).await;
        assert!(
            full > 0 && partial > 0,
            "{tables} tables: {full} full, {partial} partial"
        );
    }
}

/// The same over drawn star cases of three or more tables.
#[tokio::test]
async fn the_star_oracle_is_the_multi_join_over_drawn_cases() {
    let cluster = TestCluster::start();
    let strategy = star_case_strategy().boxed();
    let cases: Vec<StarCase> = generative::plant_sweep::seeded_cases(&strategy, 1, 16)
        .into_iter()
        .filter(|c| c.tables >= 3)
        .take(8)
        .collect();
    assert!(cases.len() >= 6, "{}", cases.len());
    for case in &cases {
        let (_db, client) = written_database(&cluster, &case.case.program).await;
        check_oracle(
            &client,
            &case.case.program,
            &case.specs,
            std::slice::from_ref(&case.join_order),
        )
        .await;
    }
}

/// With the knob on, define admits every drawn star definition: each
/// position's `ON` holds its table's unique key, and no `ON` reads two
/// joined tables (#1772's refusals). Only define runs: a star's build halts
/// until its apply is built, and nothing here waits for it.
#[tokio::test]
async fn define_admits_every_drawn_star_definition() {
    use generative::backend::{Backend, ManualBackend};
    trellis::dev::defs::set_star_join(true);
    let cluster = TestCluster::start();
    let strategy = star_case_strategy().boxed();
    let cases: Vec<StarCase> = generative::plant_sweep::seeded_cases(&strategy, 2, 24)
        .into_iter()
        .filter(|c| c.tables >= 3)
        .take(8)
        .collect();
    assert!(cases.len() >= 6, "{}", cases.len());
    for case in &cases {
        let db = cluster.create_isolated_database().await;
        let mut backend = ManualBackend::connect(db.dsn())
            .await
            .expect("connect the backend");
        let program = Program {
            ops: Vec::new(),
            def_install_after_op: vec![0; case.case.program.defs.len()],
            ..case.case.program.clone()
        };
        backend
            .install(&program)
            .await
            .unwrap_or_else(|e| panic!("define refused {:#?}: {e:?}", program.defs));
        let statuses = backend.definition_statuses().await.expect("statuses");
        assert_eq!(statuses.len(), program.defs.len(), "{statuses:?}");
    }
}
