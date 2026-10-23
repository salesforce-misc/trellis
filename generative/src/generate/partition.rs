//! ADR-0017: the partitioned tier's model of a partitioned source, and the
//! partition maintenance, partition-key updates and leaf-named writes it
//! draws (issue #1320).
//!
//! [`super::partition_table`] makes a program's table a partitioned source
//! keyed by `(pk, grain)` and routed by the grain. Once rows can move between
//! partitions and leaves can leave and rejoin the tree, whether an op
//! succeeds depends on which leaf each row lives in, so the expectations
//! [`super::render_mutate`] set from plain pk liveness no longer hold. A
//! [`PartitionSim`] replays the table's ops the way Postgres applies them
//! (rows keyed by `(pk, grain)`, routed to the leaf whose bound holds the
//! grain, or to the `DEFAULT`, or nowhere) and sets every op's
//! [`Op::expect`] from that.
//!
//! The model routes grains `0..=`[`PARTITION_GRAIN_MAX`]. `HASH` routing is
//! Postgres's own hash of a `bigint`, which [`HASH_REMAINDERS`] tabulates;
//! a database test checks the table against `satisfies_hash_partition`.

use std::collections::BTreeSet;

use crate::model::{Op, OpOutcome, PartitionAction, PartitionStrategy, Program, Table};

/// The largest grain a partitioned source's ops write: [`super::GRAIN_MAX`]
/// for seeds, one more for a partition-key update (a value that routes to no
/// `LIST` leaf), and up to this for a new leaf's rows, so every `HASH`
/// remainder has a value.
pub const PARTITION_GRAIN_MAX: i64 = 7;

/// The reconcile interval a run over a partitioned source sets
/// (`ManualBackend::set_reconcile_interval`): each check after partition
/// maintenance waits for the pass that records it, which the engine's
/// default interval would make the bulk of the run.
pub const RECONCILE_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);

/// `HASH_REMAINDERS[modulus - 1][grain]`: the remainder of the `HASH` leaf a
/// `bigint` grain routes to, for each modulus [`super::strategy`] draws
/// (`1..=4`). Postgres's partition hash is seeded with a constant, so it is
/// the same on every server and version.
pub const HASH_REMAINDERS: [[u8; PARTITION_GRAIN_MAX as usize + 1]; 4] = [
    [0, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 1, 1, 1, 1, 1],
    [2, 2, 0, 1, 0, 2, 0, 1],
    [0, 0, 2, 1, 3, 1, 3, 3],
];

/// One partition-maintenance draw, resolved against the modelled tree when
/// it is placed (see [`add_partition_maintenance`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceKind {
    /// Detach a leaf of the tree; the table keeps its rows. `CONCURRENTLY`
    /// only while the tree has no `DEFAULT` (Postgres refuses it then).
    Detach { concurrently: bool },
    /// Attach a detached table again under its own bound, with its rows.
    Attach,
    /// Drop a leaf of the tree.
    Drop,
    /// `TRUNCATE` a leaf of the tree, naming the leaf.
    TruncateLeaf,
    /// A new leaf for a bound no leaf holds, with `rows` rows written into
    /// it directly; when `standalone`, created outside the tree and
    /// attached with them. When every bound is held, a leaf is dropped
    /// first and the new one takes its bound.
    Create { rows: u8, standalone: bool },
}

/// One draw [`add_partition_maintenance`] layers on a program's partitioned
/// table. Plain numbers, so a counterexample shrinks; each is resolved
/// against the modelled tree where it lands, and one that has nothing to act
/// on there (a detach with no leaf left, say) is dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionDraw {
    /// Partition maintenance before the program's op `at` (modulo one more
    /// than the op count, so it can also come last). `pick` chooses among
    /// the leaves or bounds the kind can act on.
    Maintenance {
        at: usize,
        pick: usize,
        kind: MaintenanceKind,
    },
    /// Before op `at`, an `UPDATE` of row `pk`'s partition key to `grain`,
    /// which moves it to another partition, named on the leaf it lives in
    /// when `via_leaf` (where a value outside the leaf's bound is refused).
    MoveRow {
        at: usize,
        pk: i64,
        grain: i64,
        via_leaf: bool,
    },
    /// Issue op `at`, if it writes the partitioned table, naming the leaf
    /// its row lives in (or routes to) instead of the root; with `detached`,
    /// a detached table holding the row instead, if there is one.
    ViaLeaf { at: usize, detached: bool },
}

/// How a [`PartitionStrategy`] lays out its bounds: one slot per bound, the
/// `DEFAULT` last. Slot `i` is the `i`-th leaf `partitioned_source_ddl`
/// creates, so the leaf it starts with is `<root>_p<i>` (`<root>_pd` for the
/// `DEFAULT`).
#[derive(Debug, Clone, Copy)]
struct Layout(PartitionStrategy);

impl Layout {
    fn slots(self) -> usize {
        match self.0 {
            PartitionStrategy::Hash { modulus } => modulus as usize,
            PartitionStrategy::List { values, default } => values as usize + usize::from(default),
            PartitionStrategy::Range { bounds } => bounds as usize,
        }
    }

    fn default_slot(self) -> Option<usize> {
        match self.0 {
            PartitionStrategy::List {
                values,
                default: true,
            } => Some(values as usize),
            _ => None,
        }
    }

    /// The non-`DEFAULT` slot whose bound holds `grain`, if any.
    fn slot_of(self, grain: i64) -> Option<usize> {
        match self.0 {
            PartitionStrategy::Hash { modulus } => {
                let grain = usize::try_from(grain)
                    .ok()
                    .filter(|&g| g <= PARTITION_GRAIN_MAX as usize)
                    .unwrap_or_else(|| {
                        panic!("the partition model routes grains 0..={PARTITION_GRAIN_MAX}, not {grain}")
                    });
                Some(HASH_REMAINDERS[modulus as usize - 1][grain] as usize)
            }
            PartitionStrategy::List { values, .. } => (0..i64::from(values))
                .contains(&grain)
                .then_some(grain as usize),
            PartitionStrategy::Range { bounds } => {
                Some(grain.clamp(0, i64::from(bounds) - 1) as usize)
            }
        }
    }

    /// Slot `slot`'s bound clause, as `partitioned_source_ddl` writes it.
    fn bound(self, slot: usize) -> String {
        match self.0 {
            PartitionStrategy::Hash { modulus } => {
                format!("for values with (modulus {modulus}, remainder {slot})")
            }
            PartitionStrategy::List { .. } if Some(slot) == self.default_slot() => {
                "default".to_string()
            }
            PartitionStrategy::List { .. } => format!("for values in ({slot})"),
            PartitionStrategy::Range { bounds } => {
                let from = if slot == 0 {
                    "minvalue".to_string()
                } else {
                    slot.to_string()
                };
                let to = if slot + 1 == bounds as usize {
                    "maxvalue".to_string()
                } else {
                    (slot + 1).to_string()
                };
                format!("for values from ({from}) to ({to})")
            }
        }
    }

    fn slot_of_bound(self, bound: &str) -> usize {
        (0..self.slots())
            .find(|&s| self.bound(s) == bound)
            .unwrap_or_else(|| panic!("no slot has the bound {bound:?} — a generator bug"))
    }
}

/// One table that is, or was, a leaf: its bound's slot, whether it is in
/// the tree, and its rows' `(pk, grain)` keys.
#[derive(Debug, Clone)]
struct SimTable {
    name: String,
    slot: usize,
    attached: bool,
    /// Whether the table carries its bound as a `CHECK` constraint, which a
    /// `DETACH ... CONCURRENTLY` of a `LIST` or `RANGE` leaf adds and
    /// nothing here removes.
    checked: bool,
    rows: BTreeSet<(i64, i64)>,
}

/// A partitioned source as Postgres holds it, for working out each op's
/// outcome. Only the primary key `(pk, grain)` matters to whether an op
/// succeeds, so that is all a row keeps.
#[derive(Debug, Clone)]
pub struct PartitionSim {
    layout: Layout,
    root: String,
    pk_col: String,
    grain_col: String,
    /// Every table that is a leaf or was detached from the tree (a dropped
    /// one is gone).
    tables: Vec<SimTable>,
    /// How many new leaves have been named.
    created: usize,
}

impl PartitionSim {
    /// The partitioned `table` as `partitioned_source_ddl` creates it: a leaf
    /// per bound, all empty.
    ///
    /// # Panics
    ///
    /// If `table` isn't partitioned.
    pub fn new(table: &Table) -> Self {
        let partitioning = table
            .partitioning
            .as_ref()
            .expect("PartitionSim models a partitioned table");
        let layout = Layout(partitioning.strategy);
        let tables = (0..layout.slots())
            .map(|slot| SimTable {
                name: if Some(slot) == layout.default_slot() {
                    format!("{}_pd", table.name)
                } else {
                    format!("{}_p{slot}", table.name)
                },
                slot,
                attached: true,
                checked: false,
                rows: BTreeSet::new(),
            })
            .collect();
        PartitionSim {
            layout,
            root: table.name.clone(),
            pk_col: table.pk_col.clone(),
            grain_col: partitioning.column.clone(),
            tables,
            created: 0,
        }
    }

    fn occupant(&self, slot: usize) -> Option<usize> {
        self.tables
            .iter()
            .position(|t| t.attached && t.slot == slot)
    }

    /// The leaf a row with `grain` routes to: its bound's, else the
    /// `DEFAULT`, else none.
    fn route(&self, grain: i64) -> Option<usize> {
        self.layout
            .slot_of(grain)
            .and_then(|s| self.occupant(s))
            .or_else(|| self.layout.default_slot().and_then(|s| self.occupant(s)))
    }

    /// Whether a row with `grain` may live in table `t`: in the tree, one
    /// that routes there; outside it, any row, unless the table checks its
    /// bound.
    fn holds(&self, t: usize, grain: i64) -> bool {
        let table = &self.tables[t];
        if table.attached {
            self.route(grain) == Some(t)
        } else {
            !table.checked || self.layout.slot_of(grain) == Some(table.slot)
        }
    }

    /// Whether table `t`'s rows all fit slot `slot`'s bound in the tree as it
    /// is, and the `DEFAULT` holds none that would (the checks Postgres makes
    /// to attach or create a leaf there).
    fn may_take_slot(&self, slot: usize, rows: &BTreeSet<(i64, i64)>) -> bool {
        let fits = |grain: i64| match self.layout.slot_of(grain) {
            Some(s) => s == slot,
            None => false,
        };
        if Some(slot) == self.layout.default_slot() {
            return rows.iter().all(|&(_, g)| {
                self.layout
                    .slot_of(g)
                    .is_none_or(|s| self.occupant(s).is_none())
            });
        }
        let default_clear = self
            .layout
            .default_slot()
            .and_then(|s| self.occupant(s))
            .is_none_or(|d| self.tables[d].rows.iter().all(|&(_, g)| !fits(g)));
        default_clear && rows.iter().all(|&(_, g)| fits(g))
    }

    fn table_named(&self, name: &str) -> usize {
        self.tables
            .iter()
            .position(|t| t.name == name)
            .unwrap_or_else(|| panic!("no modelled table is named {name:?} — a generator bug"))
    }

    /// The `(pk, grain)` of a row an insert writes.
    fn key(&self, row: &[(String, Option<String>)]) -> (i64, i64) {
        let get = |column: &str| -> i64 {
            row.iter()
                .find(|(c, _)| c == column)
                .and_then(|(_, v)| v.as_deref())
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| {
                    panic!(
                        "an insert into {} carries no integer {column} — a generator bug",
                        self.root
                    )
                })
        };
        (get(&self.pk_col), get(&self.grain_col))
    }

    /// Inserts `row` into table `t` (any leaf when `None`: the one it
    /// routes to), as Postgres would.
    fn insert(&mut self, t: Option<usize>, row: &[(String, Option<String>)]) -> OpOutcome {
        let (pk, grain) = self.key(row);
        let t = match t {
            Some(t) if self.holds(t, grain) => t,
            Some(_) => return OpOutcome::Fails,
            None => match self.route(grain) {
                Some(t) => t,
                None => return OpOutcome::Fails,
            },
        };
        if self.tables[t].rows.insert((pk, grain)) {
            OpOutcome::Succeeds
        } else {
            OpOutcome::Fails
        }
    }

    /// Updates the rows with primary-key column `pk` in `within` (every leaf
    /// when `None`, as through the root), setting the grain to `grain` when
    /// given.
    fn update(&mut self, within: Option<usize>, pk: i64, grain: Option<i64>) -> OpOutcome {
        let scope: Vec<usize> = match within {
            Some(t) => vec![t],
            None => (0..self.tables.len())
                .filter(|&t| self.tables[t].attached)
                .collect(),
        };
        let matched: Vec<(usize, i64)> = scope
            .iter()
            .flat_map(|&t| {
                self.tables[t]
                    .rows
                    .iter()
                    .filter(move |&&(p, _)| p == pk)
                    .map(move |&(_, g)| (t, g))
            })
            .collect();
        let Some(new) = grain else {
            return if matched.is_empty() {
                OpOutcome::AffectsNoRows
            } else {
                OpOutcome::Succeeds
            };
        };
        match matched.as_slice() {
            [] => OpOutcome::AffectsNoRows,
            // Every matched row takes the same key: a unique violation.
            [_, _, ..] => OpOutcome::Fails,
            [(_, old)] if *old == new => OpOutcome::Succeeds,
            &[(t, old)] => {
                // Through the root, a row whose new key routes elsewhere
                // moves there; named on a leaf, it must stay.
                let dest = match within {
                    None => match self.route(new) {
                        Some(dest) => dest,
                        None => return OpOutcome::Fails,
                    },
                    Some(_) if self.holds(t, new) => t,
                    Some(_) => return OpOutcome::Fails,
                };
                if self.tables[dest].rows.contains(&(pk, new)) {
                    return OpOutcome::Fails;
                }
                self.tables[t].rows.remove(&(pk, old));
                self.tables[dest].rows.insert((pk, new));
                OpOutcome::Succeeds
            }
        }
    }

    fn delete(&mut self, within: Option<usize>, pk: i64) -> OpOutcome {
        let mut removed = false;
        for (t, table) in self.tables.iter_mut().enumerate() {
            if within.map_or(table.attached, |w| w == t) {
                let before = table.rows.len();
                table.rows.retain(|&(p, _)| p != pk);
                removed |= table.rows.len() != before;
            }
        }
        if removed {
            OpOutcome::Succeeds
        } else {
            OpOutcome::AffectsNoRows
        }
    }

    fn truncate(&mut self, within: Option<usize>) -> OpOutcome {
        let mut removed = false;
        for (t, table) in self.tables.iter_mut().enumerate() {
            if within.map_or(table.attached, |w| w == t) {
                removed |= !table.rows.is_empty();
                table.rows.clear();
            }
        }
        if removed {
            OpOutcome::Succeeds
        } else {
            OpOutcome::AffectsNoRows
        }
    }

    /// Applies a write on the root, or on table `within`, and returns its
    /// outcome.
    fn write(&mut self, within: Option<usize>, op: &Op) -> OpOutcome {
        match op {
            Op::Insert { row, .. } => self.insert(within, row),
            Op::BulkInsert { rows, .. } => {
                let before = self.clone();
                for row in rows {
                    if self.insert(within, row) == OpOutcome::Fails {
                        *self = before;
                        return OpOutcome::Fails;
                    }
                }
                OpOutcome::Succeeds
            }
            Op::Update { pk, changes, .. } => {
                let grain = changes
                    .iter()
                    .find(|(c, _)| *c == self.grain_col)
                    .map(|(_, v)| {
                        v.as_deref()
                            .and_then(|v| v.parse().ok())
                            .expect("a partition-key update writes an integer — a generator bug")
                    });
                self.update(within, parse_pk(pk), grain)
            }
            Op::Delete { pk, .. } => self.delete(within, parse_pk(pk)),
            Op::Truncate { .. } => self.truncate(within),
            Op::Leaf { .. } | Op::Partition { .. } => {
                panic!("PartitionSim::write takes a single write — a generator bug")
            }
        }
    }

    /// Applies `op`, which must name this table, and returns the outcome
    /// Postgres gives it.
    pub fn apply(&mut self, op: &Op) -> OpOutcome {
        assert_eq!(
            op.table(),
            self.root,
            "PartitionSim::apply: an op on another table"
        );
        match op {
            Op::Leaf { leaf, op } => {
                let t = self.table_named(leaf);
                self.write(Some(t), op)
            }
            Op::Partition { action, .. } => self.maintain(action),
            _ => self.write(None, op),
        }
    }

    fn maintain(&mut self, action: &PartitionAction) -> OpOutcome {
        match action {
            PartitionAction::Detach { leaf, concurrently } => {
                let t = self.table_named(leaf);
                assert!(
                    self.tables[t].attached,
                    "detach of a table outside the tree"
                );
                self.tables[t].attached = false;
                // Postgres adds no such constraint for a `HASH` bound.
                self.tables[t].checked |=
                    *concurrently && !matches!(self.layout.0, PartitionStrategy::Hash { .. });
                OpOutcome::Succeeds
            }
            PartitionAction::Attach { leaf, .. } => {
                let t = self.table_named(leaf);
                let slot = self.tables[t].slot;
                assert!(
                    !self.tables[t].attached && self.occupant(slot).is_none(),
                    "attach of {leaf} to a bound a leaf holds — a generator bug"
                );
                if !self.may_take_slot(slot, &self.tables[t].rows) {
                    return OpOutcome::Fails;
                }
                self.tables[t].attached = true;
                OpOutcome::Succeeds
            }
            PartitionAction::Drop { leaf } => {
                let t = self.table_named(leaf);
                self.tables.remove(t);
                OpOutcome::Succeeds
            }
            PartitionAction::Create {
                leaf, bound, rows, ..
            } => {
                let slot = self.layout.slot_of_bound(bound);
                assert!(
                    self.occupant(slot).is_none(),
                    "a new leaf for a bound a leaf holds — a generator bug"
                );
                let mut keys = BTreeSet::new();
                for row in rows {
                    if !keys.insert(self.key(row)) {
                        return OpOutcome::Fails;
                    }
                }
                if !self.may_take_slot(slot, &keys) {
                    return OpOutcome::Fails;
                }
                self.tables.push(SimTable {
                    name: leaf.clone(),
                    slot,
                    attached: true,
                    checked: false,
                    rows: keys,
                });
                OpOutcome::Succeeds
            }
        }
    }
}

fn parse_pk(pk: &str) -> i64 {
    pk.parse()
        .unwrap_or_else(|_| panic!("a partitioned table's pk is an integer, not {pk:?}"))
}

/// Sets the expectation of every op on `program.tables[table]`, a
/// partitioned source, from a [`PartitionSim`] replay of them in order.
pub fn resimulate(program: &mut Program, table: usize) {
    let mut sim = PartitionSim::new(&program.tables[table]);
    let name = program.tables[table].name.clone();
    for op in program.ops.iter_mut().filter(|op| op.table() == name) {
        let outcome = sim.apply(op);
        op.set_expect(outcome);
    }
}

/// Layers `draws` on `program`, whose table `table` is a partitioned source
/// ([`super::partition_table`]): partition maintenance and partition-key
/// updates inserted into its op stream, and some of its writes named on a
/// leaf. Each draw is resolved against a [`PartitionSim`] replay at the
/// point it lands, and every op on the table gets its expectation from the
/// same replay.
///
/// A new leaf's rows have primary keys `1..=rows` (so a key can live in
/// more than one partition at once, under different grains) and the
/// smallest grain its bound holds.
pub fn add_partition_maintenance(
    mut program: Program,
    table: usize,
    draws: &[PartitionDraw],
) -> Program {
    let spec = program.tables[table].clone();
    let root = spec.name.clone();
    let grain_col = spec
        .partitioning
        .as_ref()
        .expect("add_partition_maintenance: the table is partitioned")
        .column
        .clone();
    let ops = std::mem::take(&mut program.ops);
    let count = ops.len();
    let slots = count + 1;
    let mut sim = PartitionSim::new(&spec);
    let mut out = Vec::with_capacity(count + draws.len());
    let emit = |sim: &mut PartitionSim, mut op: Op, out: &mut Vec<Op>| {
        let outcome = sim.apply(&op);
        op.set_expect(outcome);
        out.push(op);
    };
    for (index, op) in ops.into_iter().map(Some).chain([None]).enumerate() {
        for draw in draws {
            match *draw {
                PartitionDraw::Maintenance { at, pick, kind } if at % slots == index => {
                    // A new leaf needs a bound no leaf holds: with none free,
                    // it replaces a leaf, dropped first.
                    if matches!(kind, MaintenanceKind::Create { .. })
                        && (0..sim.layout.slots()).all(|s| sim.occupant(s).is_some())
                        && let Some(op) =
                            maintenance_op(&mut sim, &spec, pick, MaintenanceKind::Drop)
                    {
                        emit(&mut sim, op, &mut out);
                    }
                    if let Some(op) = maintenance_op(&mut sim, &spec, pick, kind) {
                        emit(&mut sim, op, &mut out);
                    }
                }
                PartitionDraw::MoveRow {
                    at,
                    pk,
                    grain,
                    via_leaf,
                } if at % slots == index => {
                    let update = Op::Update {
                        table: root.clone(),
                        pk: pk.to_string(),
                        changes: vec![(grain_col.clone(), Some(grain.to_string()))],
                        expect: OpOutcome::Succeeds,
                    };
                    let op = if via_leaf {
                        on_leaf(&sim, update, false)
                    } else {
                        update
                    };
                    emit(&mut sim, op, &mut out);
                }
                _ => {}
            }
        }
        let Some(op) = op else { break };
        if op.table() != root {
            out.push(op);
            continue;
        }
        let via = draws.iter().find_map(|d| match *d {
            PartitionDraw::ViaLeaf { at, detached } if at % count.max(1) == index => Some(detached),
            _ => None,
        });
        let op = match via {
            Some(detached) => on_leaf(&sim, op, detached),
            None => op,
        };
        emit(&mut sim, op, &mut out);
    }
    program.ops = out;
    program
}

/// `op`, a write through the root, named on the table its row lives in (a
/// detached one, if `detached` and one holds it), or for an insert, the leaf
/// it routes to. Unchanged when there is no such table (an insert that
/// routes nowhere), or the op is a whole-table one.
fn on_leaf(sim: &PartitionSim, op: Op, detached: bool) -> Op {
    let leaf = match &op {
        Op::Insert { row, .. } => {
            let (pk, grain) = sim.key(row);
            let outside = detached
                .then(|| {
                    sim.tables.iter().position(|t| {
                        !t.attached && !t.rows.iter().any(|&(p, g)| p == pk && g == grain)
                    })
                })
                .flatten();
            outside.or_else(|| sim.route(grain))
        }
        Op::Update { pk, .. } | Op::Delete { pk, .. } => {
            let pk = parse_pk(pk);
            let holds = |t: &SimTable| t.rows.iter().any(|&(p, _)| p == pk);
            let outside = detached
                .then(|| sim.tables.iter().position(|t| !t.attached && holds(t)))
                .flatten();
            outside
                .or_else(|| sim.tables.iter().position(|t| t.attached && holds(t)))
                .or_else(|| sim.tables.iter().position(|t| t.attached))
        }
        _ => None,
    };
    match leaf {
        Some(t) => Op::Leaf {
            leaf: sim.tables[t].name.clone(),
            op: Box::new(op),
        },
        None => op,
    }
}

/// The op a [`PartitionDraw::Maintenance`] becomes in the tree `sim` models,
/// or `None` if the kind has nothing to act on.
fn maintenance_op(
    sim: &mut PartitionSim,
    table: &Table,
    pick: usize,
    kind: MaintenanceKind,
) -> Option<Op> {
    let choose = |candidates: Vec<usize>| -> Option<usize> {
        (!candidates.is_empty()).then(|| candidates[pick % candidates.len()])
    };
    let attached: Vec<usize> = (0..sim.tables.len())
        .filter(|&t| sim.tables[t].attached)
        .collect();
    let partition = |action: PartitionAction| Op::Partition {
        table: table.name.clone(),
        action,
        expect: OpOutcome::Succeeds,
    };
    let op = match kind {
        MaintenanceKind::Detach { concurrently } => {
            let t = choose(attached)?;
            let has_default = sim
                .layout
                .default_slot()
                .and_then(|s| sim.occupant(s))
                .is_some();
            partition(PartitionAction::Detach {
                leaf: sim.tables[t].name.clone(),
                concurrently: concurrently && !has_default,
            })
        }
        MaintenanceKind::Drop => {
            let t = choose(attached)?;
            partition(PartitionAction::Drop {
                leaf: sim.tables[t].name.clone(),
            })
        }
        MaintenanceKind::TruncateLeaf => {
            let t = choose(attached)?;
            Op::Leaf {
                leaf: sim.tables[t].name.clone(),
                op: Box::new(Op::Truncate {
                    table: table.name.clone(),
                    expect: OpOutcome::Succeeds,
                }),
            }
        }
        MaintenanceKind::Attach => {
            let t = choose(
                (0..sim.tables.len())
                    .filter(|&t| {
                        !sim.tables[t].attached && sim.occupant(sim.tables[t].slot).is_none()
                    })
                    .collect(),
            )?;
            partition(PartitionAction::Attach {
                leaf: sim.tables[t].name.clone(),
                bound: sim.layout.bound(sim.tables[t].slot),
            })
        }
        MaintenanceKind::Create { rows, standalone } => {
            let slot = choose(
                (0..sim.layout.slots())
                    .filter(|&s| sim.occupant(s).is_none())
                    .collect(),
            )?;
            let grain = (0..=PARTITION_GRAIN_MAX).find(|&g| match sim.layout.slot_of(g) {
                Some(s) if Some(slot) != sim.layout.default_slot() => s == slot,
                Some(s) => sim.occupant(s).is_none(),
                None => Some(slot) == sim.layout.default_slot(),
            })?;
            sim.created += 1;
            let leaf = format!("{}_n{}", table.name, sim.created);
            let grain_col = &table.partitioning.as_ref().expect("partitioned").column;
            let rows = (1..=i64::from(rows))
                .map(|pk| {
                    vec![
                        (table.pk_col.clone(), Some(pk.to_string())),
                        (table.columns[1].name.clone(), Some(pk.to_string())),
                        (grain_col.clone(), Some(grain.to_string())),
                    ]
                })
                .collect();
            partition(PartitionAction::Create {
                leaf,
                bound: sim.layout.bound(slot),
                rows,
                standalone,
            })
        }
    };
    Some(op)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generate::{
        AggregateFn, DefShape, Mutate, TableSpec, build_program_multi_with_shapes, partition_table,
    };

    /// A one-table program partitioned by `strategy`, seeding `grains` (pk
    /// `i + 1` at `grains[i]`), then `mutates`.
    fn program(strategy: PartitionStrategy, grains: &[i64], mutates: Vec<Mutate>) -> Program {
        let mut spec = TableSpec::numeric_only(vec![(Some(1), None); grains.len()], mutates);
        spec.grain_values = grains.iter().map(|g| Some(g.to_string())).collect();
        partition_table(
            build_program_multi_with_shapes(
                &[spec],
                &[(
                    0,
                    DefShape::Aggregate {
                        functions: vec![AggregateFn::Count],
                    },
                )],
            ),
            0,
            strategy,
        )
    }

    fn expects(program: &Program) -> Vec<OpOutcome> {
        program.ops.iter().map(|op| op.expect().clone()).collect()
    }

    fn describe(op: &Op) -> String {
        match op {
            Op::Leaf { leaf, op } => format!("{} on {leaf}", describe(op)),
            Op::Partition { action, .. } => format!("{action:?}"),
            Op::Insert { .. } => "insert".into(),
            Op::Update { .. } => "update".into(),
            Op::Delete { .. } => "delete".into(),
            Op::Truncate { .. } => "truncate".into(),
            Op::BulkInsert { .. } => "bulk insert".into(),
        }
    }

    /// Issue #1320: a revival of a row moved to another partition no longer
    /// collides with it (the primary key holds the grain), and a second
    /// partition-key update of that pk then hits both rows and fails.
    #[test]
    fn a_revival_after_a_partition_key_update_follows_the_rows_current_grain() {
        let program = program(
            PartitionStrategy::List {
                values: 3,
                default: false,
            },
            &[0],
            vec![
                Mutate::MoveGroup {
                    pk: 1,
                    grain: Some(2),
                },
                Mutate::DuplicateInsert {
                    pk: 1,
                    c1: None,
                    c2: None,
                },
                Mutate::MoveGroup {
                    pk: 1,
                    grain: Some(1),
                },
                Mutate::Delete { pk: 1 },
                Mutate::MoveGroup {
                    pk: 1,
                    grain: Some(1),
                },
            ],
        );
        assert_eq!(
            expects(&program),
            vec![
                OpOutcome::Succeeds,
                OpOutcome::Succeeds,
                OpOutcome::Succeeds,
                // Two rows of pk 1 (grains 2 and 0), both set to 1.
                OpOutcome::Fails,
                OpOutcome::Succeeds,
                OpOutcome::AffectsNoRows,
            ]
        );
    }

    /// A partition-key update to a value no leaf holds fails without a
    /// `DEFAULT`, and lands in the `DEFAULT` with one.
    #[test]
    fn a_partition_key_update_to_an_unrouted_value_fails_without_a_default() {
        let moves = vec![Mutate::MoveGroup {
            pk: 1,
            grain: Some(3),
        }];
        let without = program(
            PartitionStrategy::List {
                values: 3,
                default: false,
            },
            &[0],
            moves.clone(),
        );
        assert_eq!(expects(&without)[1], OpOutcome::Fails);
        let with = program(
            PartitionStrategy::List {
                values: 2,
                default: true,
            },
            &[0],
            moves,
        );
        assert_eq!(expects(&with)[1], OpOutcome::Succeeds);
    }

    /// Detaching a leaf takes its rows out of the root (a delete through the
    /// root then misses them, and a write that routes to its bound fails),
    /// attaching it again brings them back, and a leaf-named write reaches
    /// only that leaf.
    #[test]
    fn maintenance_follows_the_rows_each_leaf_holds() {
        let base = program(
            PartitionStrategy::Range { bounds: 2 },
            &[0, 1],
            vec![
                Mutate::Delete { pk: 1 },
                Mutate::DuplicateInsert {
                    pk: 1,
                    c1: None,
                    c2: None,
                },
                Mutate::Delete { pk: 2 },
            ],
        );
        // ops: insert pk1 (g0), insert pk2 (g1), delete 1, revive 1, delete 2.
        let program = add_partition_maintenance(
            base,
            0,
            &[
                // Detach the leaf holding grain 0 before `delete 1`.
                PartitionDraw::Maintenance {
                    at: 2,
                    pick: 0,
                    kind: MaintenanceKind::Detach { concurrently: true },
                },
                // Attach it again before `delete 2`.
                PartitionDraw::Maintenance {
                    at: 4,
                    pick: 0,
                    kind: MaintenanceKind::Attach,
                },
                // `delete 2` named on its leaf.
                PartitionDraw::ViaLeaf {
                    at: 4,
                    detached: false,
                },
                // Truncate the leaf of grain 0 at the end.
                PartitionDraw::Maintenance {
                    at: 5,
                    pick: 0,
                    kind: MaintenanceKind::TruncateLeaf,
                },
            ],
        );
        let shape: Vec<(String, OpOutcome)> = program
            .ops
            .iter()
            .map(|op| (describe(op), op.expect().clone()))
            .collect();
        let detach = format!(
            "{:?}",
            PartitionAction::Detach {
                leaf: "t0_p0".into(),
                concurrently: true
            }
        );
        let attach = format!(
            "{:?}",
            PartitionAction::Attach {
                leaf: "t0_p0".into(),
                bound: "for values from (minvalue) to (1)".into()
            }
        );
        assert_eq!(
            shape,
            vec![
                ("insert".into(), OpOutcome::Succeeds),
                ("insert".into(), OpOutcome::Succeeds),
                (detach, OpOutcome::Succeeds),
                // pk 1 left with the detached leaf.
                ("delete".into(), OpOutcome::AffectsNoRows),
                // Grain 0 routes to no leaf.
                ("insert".into(), OpOutcome::Fails),
                (attach, OpOutcome::Succeeds),
                ("delete on t0_p1".into(), OpOutcome::Succeeds),
                ("truncate on t0_p0".into(), OpOutcome::Succeeds),
            ]
        );
    }

    /// A new leaf beside a `DEFAULT` holding rows of its bound is refused,
    /// and a leaf-named partition-key update to a value outside the leaf's
    /// bound fails.
    #[test]
    fn a_default_holding_a_bounds_rows_refuses_a_leaf_for_it() {
        let base = program(
            PartitionStrategy::List {
                values: 2,
                default: true,
            },
            &[1, 0],
            vec![Mutate::Update {
                pk: 1,
                c1: None,
                c2: None,
            }],
        );
        // ops: insert pk1 (g1), insert pk2 (g0), update 1.
        let program = add_partition_maintenance(
            base,
            0,
            &[
                // Drop `t0_p1` (pick 1 of p0, p1, pd), with pk 1.
                PartitionDraw::Maintenance {
                    at: 2,
                    pick: 1,
                    kind: MaintenanceKind::Drop,
                },
                // Grain 1 now routes to the DEFAULT.
                PartitionDraw::MoveRow {
                    at: 2,
                    pk: 2,
                    grain: 1,
                    via_leaf: false,
                },
                // A new leaf for grain 1 (the only free bound).
                PartitionDraw::Maintenance {
                    at: 3,
                    pick: 0,
                    kind: MaintenanceKind::Create {
                        rows: 1,
                        standalone: false,
                    },
                },
                // Named on the DEFAULT, where grain 0 doesn't belong.
                PartitionDraw::MoveRow {
                    at: 3,
                    pk: 2,
                    grain: 0,
                    via_leaf: true,
                },
            ],
        );
        let shape: Vec<(String, OpOutcome)> = program
            .ops
            .iter()
            .map(|op| (describe(op), op.expect().clone()))
            .collect();
        let drop = format!(
            "{:?}",
            PartitionAction::Drop {
                leaf: "t0_p1".into()
            }
        );
        let create = format!(
            "{:?}",
            PartitionAction::Create {
                leaf: "t0_n1".into(),
                bound: "for values in (1)".into(),
                rows: vec![vec![
                    ("c0".into(), Some("1".into())),
                    ("c1".into(), Some("1".into())),
                    ("c6".into(), Some("1".into())),
                ]],
                standalone: false,
            }
        );
        assert_eq!(
            shape,
            vec![
                ("insert".into(), OpOutcome::Succeeds),
                ("insert".into(), OpOutcome::Succeeds),
                (drop, OpOutcome::Succeeds),
                ("update".into(), OpOutcome::Succeeds),
                ("update".into(), OpOutcome::AffectsNoRows),
                (create, OpOutcome::Fails),
                ("update on t0_pd".into(), OpOutcome::Fails),
            ]
        );
    }
}
