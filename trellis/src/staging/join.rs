//! A `JOIN` transform's apply step (#1108 J3, ADR-0019).
//!
//! # When it runs
//!
//! A `JOIN` definition ([`applies`]) is built by its plan
//! job (a fresh build or a `RESUME`) or its sweep job (a rebuild in place) in
//! one transaction ([`build`], "A build" below), and applies every captured
//! change from the build's start on.
//!
//! Either table may be another definition's target (#1096 decision 5,
//! ADR-0019). A target is never captured: its writers stage each changed key
//! through the target-mutation seam as exact old and new images ordered by
//! the write token (`super::target_mutations`, "The image feed"), and the side
//! consumes them as it consumes captured images. A target is never truncated
//! as a ring operation (a source `TRUNCATE` upstream clears the target key by
//! key), so such a side's truncate floor never rises. Its fresh build or
//! `RESUME` waits for the seam fence first (`super::build`'s "Seam-fed
//! sources"). An upstream build that writes its target outside the seam
//! rebuilds the `JOIN` (`intake::markers::park_target_catchup_if_read`).
//!
//! Either table may be a partitioned root (#1111, ADR-0019 "Either side may
//! be partitioned"). A node operation (`DROP`, `DETACH`, `TRUNCATE`,
//! `ATTACH`, a new leaf, a `DEFAULT` whose siblings changed, the complement
//! sweep's hit) is corrected by `super::partition_correction`, which
//! recomputes that side's cells that route to the node through
//! [`recompute_routed`]: steps 2 to 5 below, so the cells' change reaches
//! `T` under their `J` locks.
//!
//! # One page ([`apply_join_target`])
//!
//! A page carries changes of either table, or both. In its one transaction,
//! under `READ COMMITTED` so every statement after a lock sees each writer
//! the lock waited for (ADR-0019's JR2):
//!
//! 1. **Stage.** Each side's ring rows are split into sides and summed per
//!    (cell, transaction, statement) into a session table, before any lock,
//!    by #1100's writer (`super::group_basis`): `P`'s cells are the `FROM`
//!    side's `GROUP BY` columns and join key, `M`'s the joined side's. A
//!    joined row with a `NULL` in any join-key column joins nothing and is
//!    never staged. An image-less change (a `Recompute`: a released key, a
//!    Re-derive, a catch-up) has no sides: it names the cells to recompute,
//!    its key's current row's and its prior-image hint's, read before any
//!    lock as a plain aggregate's do (`super::group_basis`).
//! 2. **Lock (JR1).** The pair's lock row: `FOR SHARE`, or `FOR UPDATE` on
//!    a page that carries a `TRUNCATE` of either side (below). Then `J`:
//!    one sorted `insert … on conflict do nothing` of every non-null key
//!    the page names, then one sorted `select … for update` of the same
//!    keys. `J` rows are never deleted (ADR-0019), which is what makes that
//!    pair of statements exact. A key with a `NULL` part takes no `J` row
//!    and no lock. Then the `P` cells and the `M` cells, each in one sorted
//!    statement. `FOR UPDATE` on both sides makes every key's lock mode the
//!    page's one mode, so a mixed page never upgrades.
//! 3. **Apply the sides.** #1100's writer applies the staged sides to `P`
//!    and `M` (each cell's basis and its side's truncate floor filter them),
//!    and records the change it applied per cell. Then each cell to
//!    recompute is rewritten from a `GROUP BY` of its source rows, with the
//!    statement's snapshot as its basis (ADR-0002's R1, R5, R6), and its
//!    change recorded the same way.
//! 4. **The product (JR3).** One statement computes the change to `T` from
//!    the cells' changes and the partials the page holds locked:
//!    `ΔP ⊗ M_new + P_old ⊗ ΔM`, and for `LEFT` the unmatched terms: a `P`
//!    cell with a `NULL` key part always lands in the `NULL` joined group,
//!    and every `P` cell of a key moves in or out of it as the key's
//!    `J.matches` crosses zero. The same statement adds `ΣΔM.n` to each
//!    key's `J.matches`.
//! 5. **`T`.** The changed `T` groups are created and locked in one sorted
//!    statement (a zero group the tombstone pass collected is created
//!    afresh: `super::group_basis`'s "Zero groups"), then #1100's writer
//!    adds the deltas to `T`'s hidden partials and writes each visible row
//!    whose value changed (only while its row count is above 0), through
//!    the target-mutation seam.
//!
//! The lock order is ADR-0019's: lock row, `J` sorted, `P` then `M` cells
//! sorted, `T` groups sorted. A page applies its `JOIN` targets in target
//! order, after its other aggregate targets.
//!
//! # `TRUNCATE` ([`JoinTargetPlan::truncate`])
//!
//! A source `TRUNCATE` drains on a page of its own segment, which the drain
//! barrier orders after every page of earlier segments and before every
//! page of later ones, of either table (`super::apply::next_claimable_segments`).
//! That is ADR-0019's two-sided barrier, mixed pages included. Holding the
//! lock row `FOR UPDATE`, before its `J` locks, the page clears what the
//! truncate empties: for the `FROM` side, `P` and `T`; for the joined side,
//! `M`, every `J.matches` (to 0) and `T`, then refills `T` from `P` (each
//! cell's unmatched term for `LEFT`, nothing for `INNER`). Each side raises
//! its own truncate floor, so a change from before the truncate that still
//! reaches a page is refused. The visible target is cleared through the
//! seam. Then the page applies its other changes as any page does. The
//! hidden tables are emptied by `DELETE`, not `TRUNCATE`: another writer
//! (a release) may hold a hidden table's `ACCESS SHARE` from its working
//! tables' `create … as select` while it queues on the lock row.
//!
//! # A build ([`build`])
//!
//! With both sides unpartitioned (or empty), a fresh build, a `RESUME`
//! (which recreates the hidden tables and keeps the visible target) and a
//! rebuild in place of a live `JOIN` (a
//! `request_backfill` of either table, a re-installed capture) are one
//! `READ COMMITTED` transaction over both structures, #1097's S1T: a page
//! whose two sides both recompute every cell they enumerate from one `GROUP
//! BY` of the whole table, through the same writer ([`write_locked`]).
//!
//! 1. The lock row `FOR SHARE`.
//! 2. **Enumerate** each side's cells (ADR-0002's R8): its hidden table's,
//!    its table's, and those of the old images in the ring, by a `create
//!    temp table … as select`, which Postgres can run in parallel.
//! 3. `J` `FOR UPDATE` on the union of both sides' keys, in one sorted
//!    statement, then the `P` cells with a `NULL` key part, which take no `J`
//!    row (#1097's rule 6: `J` holds every other cell). Pages of keys the
//!    build holds wait for its commit; the other keys' pages run through.
//! 4. Each side's snapshot statement, one `GROUP BY` of its table into a
//!    temp table (parallel where Postgres can), whose snapshot is each cell's
//!    basis, taken after every lock statement; then its write of the cells it
//!    enumerated, each written once: an update of a cell the hidden table
//!    holds, an insert of one it doesn't. A cell first seen after the
//!    enumeration is left to its pages. Then the product of both sides'
//!    changes into `T`. The working tables are analyzed as they fill, so
//!    these statements are planned for millions of cells.
//! 5. `T`: its groups are locked **after** the product, so the set is exactly
//!    the groups the product reads under `J`, with every group of the visible
//!    target and of `T`'s hidden table added (R8 one level up). `T` changes
//!    only as the product's delta; each locked group's visible row is then
//!    compared with the row the target holds (prior images are the held
//!    rows), which also removes the row of a group no source row backs.
//!
//! Readers see the old `T` and the old visible rows until the commit. A
//! `TRUNCATE` page waits for the commit on the lock row and then clears what
//! the build wrote; the build holds `ACCESS SHARE` of both tables, so a
//! `TRUNCATE` statement queues behind it. The build's hold on both
//! structures, the xmin horizon and the ring's retirement lasts the whole
//! transaction (`docs/recommendations.md`).
//!
//! A rebuild in place repairs what capture missed and visible rows that
//! disagree with `T`, not a hand edit of a hidden table, which a `RESUME`
//! recreates.
//!
//! # A build with a partitioned side
//!
//! A side that is a partitioned root with rows (#1286) is built per side and
//! per leaf, in transactions of their own ([`BuildWork`]): every unpartitioned
//! side whole, then each partitioned side's leaves in oid order and its
//! remainder, each step the shape of [`recompute_routed`] (steps 1 to 4 above
//! over the region's cells, `T` written as a delta from the partials), then a
//! final step that locks every group of the visible target and of `T` and
//! compares each visible row with the row the target holds. The job's cursor
//! names the last step committed (`super::build::run_join_steps`). Readers
//! see progressive values until the final step commits.
//!
//! A `RESUME` build parks its readers' catch-up when it commits (#1287): its
//! pages computed prior images from empty partials while the target kept the
//! earlier rows, so a reader holds rows those images don't match, and its
//! rebuild overwrites them. The park is in the last transaction, once.
//!
//! # Release ([`release`])
//!
//! A quarantine release, and the automatic retry, of a key held for a
//! `JOIN` recomputes every cell of the key's side it could owe (ADR-0002's
//! R8): those its parked changes named (`poison_held.agg_groups`) and its
//! current row's, through steps 2 to 5 above, in the release's own
//! transaction.

use std::collections::HashMap;

use tokio_postgres::Transaction;
use tokio_postgres::types::PgLsn;

use crate::defs::ast::{Expr, KeySpace, TransformDef, ValueType};
use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::defs::join::{self as spec, HiddenTable, Side};
use crate::defs::model::Definition;
use crate::defs::oracle::render_qualified_expr_sql;
use crate::pool::quote_ident;

use super::apply::ApplyError;
use super::fold::{FoldedChange, earliest_origin};
use super::group_basis::{self, Arg, GroupShape, GroupTargetPlan, Writer};
use super::target_mutations::TargetMutations;

/// Whether `def` is a `JOIN` that applies: always, unless a test forced the
/// group-basis knob off (`super::group_basis::enabled`), when it stays
/// `waiting_to_backfill`.
pub(crate) fn applies(def: &TransformDef) -> bool {
    matches!(def.key_space, KeySpace::Join(_)) && group_basis::enabled()
}

/// One side's part of a [`JoinTargetPlan`].
#[derive(Debug, Clone)]
struct SidePlan {
    /// `P` or `M`, through #1100's writer.
    plan: GroupTargetPlan,
    /// The side's join-key columns, in `ON` order.
    join_cols: Vec<String>,
    /// The deltas table its apply fills, quoted and `pg_temp.`-qualified.
    deltas: String,
    /// Whether the page (or the release) recomputes cells of this side.
    recompute: bool,
    /// The partition a partition correction reads its cells' rows from, in
    /// one `GROUP BY` of the node (quoted and qualified), instead of through
    /// the root ([`recompute_routed`]).
    node: Option<String>,
    /// A build's per-transaction tables, quoted and `pg_temp.`-qualified:
    /// its enumerated cells ([`group_basis::enumerate_cells`]) and its
    /// snapshot's `GROUP BY` and each cell's match to its hidden row and to
    /// that `GROUP BY` ([`group_basis::write_join_build_cells`]).
    cells: String,
    grouped: String,
    matched: String,
}

impl SidePlan {
    /// Whether this side writes its cells: it has sides to apply or cells
    /// to recompute.
    fn writes(&self) -> bool {
        !self.plan.sides.is_empty() || self.recompute
    }

    /// The table or leaf a build (`bulk`) reads this side's cells from in
    /// one `GROUP BY`, when it does: then it writes each cell once
    /// ([`group_basis::write_join_build_cells`]). A build step that probes
    /// each cell's rows through the root recomputes them as a page does.
    fn bulk_node(&self, bulk: bool) -> Option<&str> {
        if bulk && self.recompute && self.plan.sides.is_empty() {
            self.node.as_deref()
        } else {
            None
        }
    }

    /// ` where …`: the cell has a `NULL` part in its join key, so it takes no
    /// `J` row and no `J` lock. A joined-side cell never has one.
    fn null_key_filter(&self) -> String {
        let null: Vec<String> = quoted(&self.join_cols)
            .iter()
            .map(|c| format!("{c} is null"))
            .collect();
        format!(" where {}", null.join(" or "))
    }
}

/// A page's `TRUNCATE` of one side or both ([`JoinTargetPlan::truncate`]).
#[derive(Debug, Clone, Default)]
struct Truncate {
    /// Each side's truncate's `lsn`: the floor it raises.
    from: Option<PgLsn>,
    joined: Option<PgLsn>,
    hop_gen: i32,
    src_changed: Option<std::time::SystemTime>,
    origin_lsn: Option<PgLsn>,
}

/// One page's work on one `JOIN` target ([`apply_join_target`]).
#[derive(Debug, Clone)]
pub(crate) struct JoinTargetPlan {
    /// The target, qualified and unquoted.
    pub(crate) target: String,
    /// The target's group identity, for a truncate's clear.
    identity: Vec<PrimaryKeyColumn>,
    truncate: Option<Truncate>,
    left: bool,
    lock_ident: String,
    j_ident: String,
    /// `J`'s key columns: the `FROM` side's join key.
    j_cols: Vec<String>,
    from: SidePlan,
    joined: SidePlan,
    /// `T`'s group columns, each with the side it is read from.
    t_groups: Vec<(Side, String)>,
    /// `T`, through #1100's writer.
    t: GroupTargetPlan,
}

/// `expr` with each reference to another value field replaced by that
/// field's expression, side-qualified columns kept (a `GROUP BY` key's
/// field stays a reference, which reads the target group's column). `None`
/// past a depth no accepted definition reaches (define refuses a cycle).
fn substitute(def: &TransformDef, expr: &Expr, depth: usize) -> Option<Expr> {
    if depth > def.fields.len() {
        return None;
    }
    let is_key = |name: &str| {
        spec::spec(def).is_some_and(|join| join.group_by.iter().any(|key| key.column == name))
    };
    Some(match expr {
        Expr::Column(name) if !is_key(name) => match def.fields.iter().find(|f| f.name == *name) {
            Some(field) => substitute(def, &field.expr, depth + 1)?,
            None => expr.clone(),
        },
        Expr::FunctionCall { name, args } => Expr::FunctionCall {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| substitute(def, a, depth))
                .collect::<Option<Vec<_>>>()?,
        },
        Expr::BinaryOp { op, lhs, rhs } => Expr::BinaryOp {
            op: *op,
            lhs: Box::new(substitute(def, lhs, depth)?),
            rhs: Box::new(substitute(def, rhs, depth)?),
        },
        other => other.clone(),
    })
}

/// The name of a `JOIN` working table (a session temp table, bare):
/// `prefix`, the definition id, and a hash of the columns of the hidden
/// table it copies (`hidden_columns`, from [`hidden_table_columns`]). The
/// pool keeps its connections, and a connection keeps the temp table it
/// first created under a name, so the name follows the hidden table's
/// column types and collations: a definition whose key column was re-typed
/// or re-collated under the same id gets a new table, and the old one is
/// never touched again. Within Postgres's 63 bytes for any `i64` id.
fn working_table_name(prefix: &str, id: i64, hidden_columns: &str) -> String {
    format!(
        "trellis_join_{prefix}_{id}_{:016x}",
        group_basis::shape_hash(hidden_columns)
    )
}

/// The qualified hidden table `table` of definition `id`, quoted.
fn hidden_ident(schema: &str, id: i64, table: HiddenTable) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(&table.name(id)))
}

fn quoted(columns: &[String]) -> Vec<String> {
    columns.iter().map(|c| quote_ident(c)).collect()
}

impl JoinTargetPlan {
    /// The plan of `definition`, a `JOIN`. `joined` is its joined table
    /// (qualified) and that table's column types, `field_types` the visible
    /// target's column types (`format_type`), `hidden_columns` its hidden
    /// tables' column lists ([`hidden_table_columns`]), `identity` the
    /// target's group identity, and `source_pks` each side's row identity
    /// (`FROM`, joined), which an image-less change's key names.
    pub(crate) fn new(
        catalog_schema: &str,
        definition: &Definition,
        joined: &(String, HashMap<String, ValueType>),
        field_types: &HashMap<String, String>,
        hidden_columns: &HashMap<String, String>,
        identity: Vec<PrimaryKeyColumn>,
        source_pks: [Vec<PrimaryKeyColumn>; 2],
    ) -> Result<Self, ApplyError> {
        let [from_pk, joined_pk] = source_pks;
        let def = &definition.def;
        let id = definition.id;
        let working = |prefix: &str, table: HiddenTable| {
            let columns = hidden_columns
                .get(&table.name(id))
                .map_or("", String::as_str);
            working_table_name(prefix, id, columns)
        };
        let target = definition.target_table.as_str();
        let unsupported = |what: &'static str| ApplyError::GroupBasisUnsupported {
            target: target.to_string(),
            what,
        };
        let Some(join) = spec::spec(def) else {
            return Err(unsupported("a definition that isn't a JOIN"));
        };
        let left = join.kind == crate::defs::ast::JoinKind::Left;
        let arguments = |side| spec::side_arguments(def, side);
        let (from_args, joined_args) = (arguments(Side::From), arguments(Side::Joined));
        let types_of = |side| match side {
            Side::From => &definition.source_columns,
            Side::Joined => &joined.1,
        };
        // The side an aggregate's argument reads (define refuses one that
        // reads both, or neither).
        let side_of = |arg: &Expr| {
            if from_args.contains(arg) {
                Some(Side::From)
            } else if joined_args.contains(arg) {
                Some(Side::Joined)
            } else {
                None
            }
        };
        // `T`'s arguments: the `FROM` side's, then the joined side's.
        let all_args: Vec<Arg> = from_args
            .iter()
            .chain(&joined_args)
            .map(|arg| Arg {
                sql: render_qualified_expr_sql(arg, "s"),
                expr: arg.clone(),
                summed: false,
            })
            .collect();
        let t_groups: Vec<(Side, String)> = join
            .group_by
            .iter()
            .map(|key| {
                let side = if key.table == def.source {
                    Side::From
                } else {
                    Side::Joined
                };
                (side, key.column.clone())
            })
            .collect();
        let group_cols: Vec<String> = t_groups.iter().map(|(_, c)| c.clone()).collect();
        let mut t_shape = GroupShape {
            group_types: vec![String::new(); group_cols.len()],
            group_cols,
            args: all_args,
            aggs: Vec::new(),
            fields: Vec::new(),
            numeric_args: Vec::new(),
        };
        let type_of = |arg: &Expr| {
            let side = side_of(arg)?;
            crate::defs::validate::infer_expr_type(arg, types_of(side)).ok()
        };
        let arg_of = |arg: &Expr| match side_of(arg)? {
            Side::From => from_args.iter().position(|a| a == arg),
            Side::Joined => joined_args
                .iter()
                .position(|a| a == arg)
                .map(|i| i + from_args.len()),
        };
        for field in &def.fields {
            if join.group_by.iter().any(|key| key.column == field.name) {
                continue;
            }
            let expr = substitute(def, &field.expr, 0)
                .and_then(|expr| group_basis::rewrite(&expr, &type_of, &arg_of, &mut t_shape))
                .ok_or_else(|| unsupported("a field the JOIN apply can't compute from partials"))?;
            let pg_type = field_types
                .get(&field.name)
                .cloned()
                .ok_or_else(|| unsupported("a field its target has no column for"))?;
            t_shape.fields.push((field.name.clone(), pg_type, expr));
        }
        let side_plan =
            |side: Side, table: HiddenTable, source: &str, pk: Vec<PrimaryKeyColumn>, tag: &str| {
                let args: Vec<Arg> = match side {
                    Side::From => t_shape.args[..from_args.len()].to_vec(),
                    Side::Joined => t_shape.args[from_args.len()..].to_vec(),
                };
                let key_cols = spec::side_key_columns(def, side);
                let join_cols = spec::join_key(def, side);
                // A joined row with a `NULL` key part joins nothing: no `M` cell.
                let stage_filter = match side {
                    Side::From => String::new(),
                    Side::Joined => join_cols
                        .iter()
                        .map(|c| format!(" and s.{} is not null", quote_ident(c)))
                        .collect(),
                };
                let hidden = hidden_ident(catalog_schema, id, table);
                let deltas = format!(
                    "pg_temp.{}",
                    quote_ident(&working(&format!("d{tag}"), table))
                );
                SidePlan {
                    plan: GroupTargetPlan::for_join(
                        target,
                        hidden.clone(),
                        &working(tag, table),
                        Some((source, &working(&format!("r{tag}"), table), pk)),
                        Vec::new(),
                        GroupShape {
                            group_types: vec![String::new(); key_cols.len()],
                            group_cols: key_cols,
                            args,
                            aggs: Vec::new(),
                            fields: Vec::new(),
                            numeric_args: Vec::new(),
                        },
                        Writer::JoinPartials {
                            deltas: deltas.clone(),
                        },
                        // Each side's own truncate floor (ADR-0019).
                        Some(hidden),
                        stage_filter,
                    )
                    .with_ring(catalog_schema),
                    join_cols,
                    deltas,
                    recompute: false,
                    node: None,
                    cells: format!(
                        "pg_temp.{}",
                        quote_ident(&working(&format!("e{tag}"), table))
                    ),
                    grouped: format!(
                        "pg_temp.{}",
                        quote_ident(&working(&format!("g{tag}"), table))
                    ),
                    matched: format!(
                        "pg_temp.{}",
                        quote_ident(&working(&format!("w{tag}"), table))
                    ),
                }
            };
        let from = side_plan(
            Side::From,
            HiddenTable::FromPartials,
            &definition.source_table,
            from_pk,
            "p",
        );
        let joined_plan = side_plan(
            Side::Joined,
            HiddenTable::JoinedPartials,
            &joined.0,
            joined_pk,
            "m",
        );
        let t = GroupTargetPlan::for_join(
            target,
            hidden_ident(catalog_schema, id, HiddenTable::TargetPartials),
            &working("t", HiddenTable::TargetPartials),
            None,
            identity.clone(),
            t_shape,
            Writer::JoinTarget,
            None,
            String::new(),
        );
        Ok(Self {
            target: target.to_string(),
            identity,
            truncate: None,
            left,
            lock_ident: hidden_ident(catalog_schema, id, HiddenTable::TruncateLock),
            j_ident: hidden_ident(catalog_schema, id, HiddenTable::Matches),
            j_cols: spec::join_key(def, Side::From),
            from,
            joined: joined_plan,
            t_groups,
            t,
        })
    }

    fn side_mut(&mut self, side: Side) -> &mut SidePlan {
        match side {
            Side::From => &mut self.from,
            Side::Joined => &mut self.joined,
        }
    }

    /// Adds one folded change of `side`'s table. An image-less change names
    /// cells to recompute (see the module doc).
    pub(crate) fn push(&mut self, side: Side, change: &FoldedChange) -> Result<(), ApplyError> {
        let plan = self.side_mut(side);
        plan.plan.push(change)?;
        plan.recompute |= change.has_recompute;
        self.t.note(change);
        Ok(())
    }

    /// Adds a `TRUNCATE` of `side`'s table, `change`, whose `lsn` is `lsn`:
    /// the page clears what it empties before its other changes (see the
    /// module doc).
    pub(crate) fn truncate(&mut self, side: Side, lsn: PgLsn, change: &FoldedChange) {
        let truncate = self.truncate.get_or_insert_with(|| Truncate {
            hop_gen: change.hop_gen,
            src_changed: change.src_changed,
            origin_lsn: change.origin_lsn,
            ..Truncate::default()
        });
        truncate.hop_gen = truncate.hop_gen.max(change.hop_gen);
        truncate.src_changed =
            super::apply::earliest_src_changed(truncate.src_changed, change.src_changed);
        truncate.origin_lsn = earliest_origin(truncate.origin_lsn, change.origin_lsn);
        let floor = match side {
            Side::From => &mut truncate.from,
            Side::Joined => &mut truncate.joined,
        };
        *floor = Some(floor.map_or(lsn, |f| f.max(lsn)));
        self.t.note(change);
    }

    /// The keys the page's cells name, staged or to recompute, as `J`'s
    /// columns, with no `NULL` part: `select …` over both sides' working
    /// tables.
    fn staged_keys_sql(&self) -> String {
        let as_j = |cols: &[String]| -> String {
            cols.iter()
                .zip(&self.j_cols)
                .map(|(c, j)| format!("{} as {}", quote_ident(c), quote_ident(j)))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let not_null: Vec<String> = quoted(&self.j_cols)
            .iter()
            .map(|c| format!("{c} is not null"))
            .collect();
        let selects: Vec<String> = [&self.from, &self.joined]
            .into_iter()
            .flat_map(|side| {
                let columns = as_j(&side.join_cols);
                [side.plan.staged(), side.plan.recomputed()]
                    .map(|table| format!("select {columns} from {table}"))
            })
            .collect();
        format!(
            "select * from ({}) k where {}",
            selects.join(" union "),
            not_null.join(" and ")
        )
    }

    /// A `TRUNCATE` of the joined side's refill of `T` (see the module doc):
    /// stages, into `T`'s staging table, each `P` cell's unmatched term `P ⊗
    /// U` per `T` group. Run after `T` is emptied and before the page's own
    /// sides apply, so it reads `P` as it was before them.
    fn refill_sql(&self) -> String {
        let p_args = &self.from.plan.shape.args;
        let m_args = &self.joined.plan.shape.args;
        // The `FROM` side's `T` group columns, which `P` holds; the joined
        // side's are `NULL` in every unmatched term.
        let from_groups: Vec<String> = self
            .t_groups
            .iter()
            .filter(|(side, _)| *side == Side::From)
            .map(|(_, c)| quote_ident(c))
            .collect();
        let mut select: Vec<String> = self
            .t_groups
            .iter()
            .map(|(side, c)| match side {
                Side::From => format!("q.{}", quote_ident(c)),
                Side::Joined => "null".to_string(),
            })
            .collect();
        let mut insert_cols = quoted(&self.t.shape.group_cols);
        insert_cols.extend(["__txid", "__lsn", "__n"].map(String::from));
        select.extend([
            "pg_current_xact_id()".to_string(),
            "'0/0'::pg_lsn".to_string(),
            "q.__n".to_string(),
        ]);
        let mut sums = from_groups.clone();
        sums.push("sum(__n)::bigint as __n".to_string());
        let mut nonzero = vec!["sum(__n) <> 0".to_string()];
        for (i, arg) in p_args.iter().chain(m_args).enumerate() {
            let from = i < p_args.len();
            let i = i + 1;
            insert_cols.push(format!("__c{i}"));
            if from {
                sums.push(format!("sum(__count_{i})::bigint as __c{i}"));
                nonzero.push(format!("sum(__count_{i}) <> 0"));
                select.push(format!("q.__c{i}"));
            } else {
                select.push("0".to_string());
            }
            if arg.summed {
                insert_cols.push(format!("__s{i}"));
                if from {
                    sums.push(format!("sum(__sum_{i}) as __s{i}"));
                    nonzero.push(format!("sum(__sum_{i}) <> 0"));
                    select.push(format!("q.__s{i}"));
                } else {
                    select.push("0".to_string());
                }
            }
        }
        let group_by = if from_groups.is_empty() {
            String::new()
        } else {
            format!(" group by {}", from_groups.join(", "))
        };
        format!(
            "insert into {staged} ({cols}) select {select} from ( \
                 select {sums} from {p}{group_by} having {nonzero} \
             ) q",
            staged = self.t.staged(),
            cols = insert_cols.join(", "),
            select = select.join(", "),
            sums = sums.join(", "),
            p = self.from.plan.hidden(),
            nonzero = nonzero.join(" or "),
        )
    }

    /// Step 2's `J` insert: every key the page names that has no row.
    fn j_insert_sql(&self) -> String {
        let cols = quoted(&self.j_cols).join(", ");
        format!(
            "insert into {j} ({cols}) select {cols} from ({keys}) k order by {cols} \
             on conflict do nothing",
            j = self.j_ident,
            keys = self.staged_keys_sql(),
        )
    }

    /// Step 2's `J` lock: the same keys, sorted, `FOR UPDATE`.
    fn j_lock_sql(&self) -> String {
        let cols = quoted(&self.j_cols);
        let j_cols: Vec<String> = cols.iter().map(|c| format!("j.{c}")).collect();
        format!(
            "select 1 from {j} j where ({jc}) in (select {cols} from ({keys}) k) \
             order by {jc} for update of j",
            j = self.j_ident,
            jc = j_cols.join(", "),
            cols = cols.join(", "),
            keys = self.staged_keys_sql(),
        )
    }

    /// Step 4's statement (see the module doc): stages the product's change
    /// per `T` group into `T`'s staging table and adds each key's `ΣΔM.n`
    /// to `J.matches`. Every read is of rows the page holds locked.
    fn product_sql(&self) -> String {
        let p = self.from.plan.hidden();
        let m = self.joined.plan.hidden();
        // Each side's change per cell: its apply's and its recompute's, for
        // a cell both wrote, summed.
        let (dp, dm) = ("__dp", "__dm");
        let summed = |name: &str, side: &SidePlan| {
            let keys = quoted(&side.plan.shape.group_cols).join(", ");
            let mut columns = vec![keys.clone(), "sum(__dn)::bigint as __dn".to_string()];
            for (i, arg) in side.plan.shape.args.iter().enumerate() {
                let i = i + 1;
                columns.push(format!("sum(__dc{i})::bigint as __dc{i}"));
                if arg.summed {
                    columns.push(format!("sum(__ds{i}) as __ds{i}"));
                }
            }
            format!(
                "{name} as materialized (select {} from {} group by {keys})",
                columns.join(", "),
                side.deltas
            )
        };
        let delta_ctes = vec![summed(dp, &self.from), summed(dm, &self.joined)];
        let p_args = self.from.plan.shape.args.clone();
        let m_args = self.joined.plan.shape.args.clone();
        let fa = p_args.len();
        let pj = quoted(&self.from.join_cols);
        let mj = quoted(&self.joined.join_cols);
        let jc = quoted(&self.j_cols);
        // `on <a>.<pj> = <b>.<mj>` for each key column pair.
        let key_eq = |a: &str, a_cols: &[String], b: &str, b_cols: &[String]| -> String {
            a_cols
                .iter()
                .zip(b_cols)
                .map(|(x, y)| format!("{a}.{x} = {b}.{y}"))
                .collect::<Vec<_>>()
                .join(" and ")
        };
        // A `P` cell `p`'s delta row `d`, matched on every key column,
        // `NULL` matching `NULL`.
        let cell_eq = self.from.plan.group_match("d", "p");
        // One term's select list: `T`'s group columns (`P`'s from `a`, `M`'s
        // from `b`, or `NULL` when `b` is `None`), then `__n`, then per
        // argument its count and, when summed, its sum.
        let term = |a: &str, b: Option<&str>, n: String, counts: Vec<String>, sums: Vec<String>| {
            let mut columns: Vec<String> = self
                .t_groups
                .iter()
                .map(|(side, c)| {
                    let c = quote_ident(c);
                    match (side, b) {
                        (Side::From, _) => format!("{a}.{c} as {c}"),
                        (Side::Joined, Some(b)) => format!("{b}.{c} as {c}"),
                        (Side::Joined, None) => format!("null as {c}"),
                    }
                })
                .collect();
            columns.push(format!("({n})::bigint as __n"));
            for (i, arg) in p_args.iter().chain(&m_args).enumerate() {
                columns.push(format!("({})::bigint as __c{}", counts[i], i + 1));
                if arg.summed {
                    columns.push(format!("({})::numeric as __s{}", sums[i], i + 1));
                }
            }
            columns.join(", ")
        };
        // `P_old` of cell `p`, given its delta row `d` (left joined).
        let p_old_n = "(p.__n - coalesce(d.__dn, 0))".to_string();
        let p_old_c = |i: usize| format!("(p.__count_{i} - coalesce(d.__dc{i}, 0))");
        let p_old_s = |i: usize| {
            if p_args[i - 1].summed {
                format!("(p.__sum_{i} - coalesce(d.__ds{i}, 0))")
            } else {
                "0".to_string()
            }
        };
        let d_sum = |alias: &str, args: &[Arg], i: usize| {
            if args[i - 1].summed {
                format!("{alias}.__ds{i}")
            } else {
                "0".to_string()
            }
        };
        let mut terms = Vec::new();
        // ΔP ⊗ M_new.
        {
            let mut counts = Vec::new();
            let mut sums = Vec::new();
            for i in 1..=fa {
                counts.push(format!("d.__dc{i} * m.__n"));
                sums.push(format!("{} * m.__n", d_sum("d", &p_args, i)));
            }
            for j in 1..=m_args.len() {
                counts.push(format!("d.__dn * m.__count_{j}"));
                sums.push(format!("d.__dn * m.__sum_{j}"));
            }
            terms.push(format!(
                "select {} from {dp} d join {m} m on {}",
                term("d", Some("m"), "d.__dn * m.__n".to_string(), counts, sums),
                key_eq("m", &mj, "d", &pj),
            ));
        }
        // P_old ⊗ ΔM.
        {
            let mut counts = Vec::new();
            let mut sums = Vec::new();
            for i in 1..=fa {
                counts.push(format!("{} * e.__dn", p_old_c(i)));
                sums.push(format!("{} * e.__dn", p_old_s(i)));
            }
            for j in 1..=m_args.len() {
                counts.push(format!("{p_old_n} * e.__dc{j}"));
                sums.push(format!("{p_old_n} * {}", d_sum("e", &m_args, j)));
            }
            terms.push(format!(
                "select {} from {dm} e join {p} p on {} left join {dp} d on {cell_eq}",
                term("p", Some("e"), format!("{p_old_n} * e.__dn"), counts, sums),
                key_eq("p", &pj, "e", &mj),
            ));
        }
        let mut ctes = delta_ctes;
        if self.left {
            let zero_b = |counts: &mut Vec<String>, sums: &mut Vec<String>| {
                for _ in 0..m_args.len() {
                    counts.push("0".to_string());
                    sums.push("0".to_string());
                }
            };
            // A `P` cell with a `NULL` key part: always unmatched.
            {
                let mut counts: Vec<String> = (1..=fa).map(|i| format!("d.__dc{i}")).collect();
                let mut sums: Vec<String> = (1..=fa).map(|i| d_sum("d", &p_args, i)).collect();
                zero_b(&mut counts, &mut sums);
                let null_key: Vec<String> = pj.iter().map(|c| format!("d.{c} is null")).collect();
                terms.push(format!(
                    "select {} from {dp} d where {}",
                    term("d", None, "d.__dn".to_string(), counts, sums),
                    null_key.join(" or ")
                ));
            }
            // Every `P` cell of a key the page changed: `[matches_new = 0] P_new
            // - [matches_old = 0] P_old` (a flip, or a change while unmatched).
            {
                let mut counts: Vec<String> = (1..=fa)
                    .map(|i| format!("jd.__new * p.__count_{i} - jd.__old * {}", p_old_c(i)))
                    .collect();
                let mut sums: Vec<String> = (1..=fa)
                    .map(|i| {
                        if p_args[i - 1].summed {
                            format!("jd.__new * p.__sum_{i} - jd.__old * {}", p_old_s(i))
                        } else {
                            "0".to_string()
                        }
                    })
                    .collect();
                zero_b(&mut counts, &mut sums);
                // With `matches_new > 0` the term is `-P_old`, which adds
                // nothing when every partial of `P_old` is 0: a cell a build
                // inserted, on a key its `M` matched.
                let mut p_old: Vec<String> = vec![p_old_n.clone()];
                for i in 1..=fa {
                    p_old.push(p_old_c(i));
                    if p_args[i - 1].summed {
                        p_old.push(p_old_s(i));
                    }
                }
                let p_old_nonzero: Vec<String> =
                    p_old.iter().map(|v| format!("{v} <> 0")).collect();
                terms.push(format!(
                    "select {} from jd join {p} p on {} left join {dp} d on {cell_eq} \
                     where jd.__new <> 0 or (jd.__old <> 0 and ({}))",
                    term(
                        "p",
                        None,
                        format!("jd.__new * p.__n - jd.__old * {p_old_n}"),
                        counts,
                        sums
                    ),
                    key_eq("p", &pj, "jd", &jc),
                    p_old_nonzero.join(" or "),
                ));
            }
            let pj_as: Vec<String> = pj
                .iter()
                .zip(&jc)
                .map(|(c, j)| format!("{c} as {j}"))
                .collect();
            let mj_as: Vec<String> = mj
                .iter()
                .zip(&jc)
                .map(|(c, j)| format!("{c} as {j}"))
                .collect();
            let pj_not_null: Vec<String> = pj.iter().map(|c| format!("{c} is not null")).collect();
            let jk: Vec<String> = jc.iter().map(|c| format!("k.{c}")).collect();
            #[cfg_attr(not(any(test, feature = "test-util")), allow(unused_mut))]
            let mut matches_new = "j.__matches + coalesce(x.__d, 0)";
            // Planted bug (#1114): a key's matches never cross zero. See
            // `crate::plant`.
            #[cfg(any(test, feature = "test-util"))]
            if crate::plant::armed() == Some(crate::plant::Plant::JoinDropFlip) {
                matches_new = "j.__matches";
            }
            ctes.push(format!(
                "jd as ( \
                     select {jk}, ({matches_new} = 0)::int as __new, \
                            (j.__matches = 0)::int as __old \
                     from (select {pj_as} from {dp} where {pj_nn} \
                           union select {mj_as} from {dm}) k \
                     join {j} j on {k_j} \
                     left join (select {mj_as}, sum(__dn) as __d from {dm} group by {mj}) x \
                         on {k_x} \
                 )",
                jk = jk.join(", "),
                pj_as = pj_as.join(", "),
                pj_nn = pj_not_null.join(" and "),
                mj_as = mj_as.join(", "),
                mj = mj.join(", "),
                j = self.j_ident,
                k_j = key_eq("k", &jc, "j", &jc),
                k_x = key_eq("k", &jc, "x", &jc),
            ));
        }
        let t_cols = quoted(&self.t.shape.group_cols);
        let mut insert_cols = t_cols.clone();
        insert_cols.extend(["__txid", "__lsn", "__n"].map(String::from));
        let mut sums = vec!["sum(__n)::bigint".to_string()];
        let mut nonzero = vec!["sum(__n) <> 0".to_string()];
        for (i, arg) in p_args.iter().chain(&m_args).enumerate() {
            let i = i + 1;
            insert_cols.push(format!("__c{i}"));
            sums.push(format!("sum(__c{i})::bigint"));
            nonzero.push(format!("sum(__c{i}) <> 0"));
            if arg.summed {
                insert_cols.push(format!("__s{i}"));
                sums.push(format!("sum(__s{i})"));
                nonzero.push(format!("sum(__s{i}) <> 0"));
            }
        }
        ctes.push(format!("terms as ({})", terms.join(" union all ")));
        ctes.push(format!(
            "staged as ( \
                 insert into {t_staged} ({insert_cols}) \
                 select {t_cols}, pg_current_xact_id(), '0/0'::pg_lsn, {sums} from terms \
                 group by {t_cols} having {nonzero} \
             )",
            t_staged = self.t.staged(),
            insert_cols = insert_cols.join(", "),
            t_cols = t_cols.join(", "),
            sums = sums.join(", "),
            nonzero = nonzero.join(" or "),
        ));
        let mj_as: Vec<String> = mj
            .iter()
            .zip(&jc)
            .map(|(c, j)| format!("{c} as {j}"))
            .collect();
        ctes.push(format!(
            "matched as ( \
                 update {j} j set __matches = j.__matches + x.__d \
                 from (select {mj_as}, sum(__dn) as __d from {dm} group by {mj}) x \
                 where {j_x} and x.__d <> 0 \
             )",
            j = self.j_ident,
            mj_as = mj_as.join(", "),
            mj = mj.join(", "),
            j_x = key_eq("j", &jc, "x", &jc),
        ));
        format!("with {} select 1", ctes.join(", "))
    }
}

impl JoinTargetPlan {
    /// Planted bug `join_no_j_lock` (`crate::plant`): [`Self::j_insert_sql`]
    /// for the keys with no `J` row only, so a key that has one is never
    /// touched, and so never waited on.
    #[cfg(any(test, feature = "test-util"))]
    fn planted_j_insert_missing_sql(&self) -> String {
        let cols = quoted(&self.j_cols);
        let matched: Vec<String> = cols.iter().map(|c| format!("j.{c} = k.{c}")).collect();
        format!(
            "insert into {j} ({cols}) select {cols} from ({keys}) k \
             where not exists (select 1 from {j} j where {matched}) \
             order by {cols} on conflict do nothing",
            j = self.j_ident,
            cols = cols.join(", "),
            keys = self.staged_keys_sql(),
            matched = matched.join(" and "),
        )
    }

    /// Planted bug `join_other_side_before_j` (`crate::plant`): copies
    /// each side's hidden partials into [`PLANTED_BEFORE_J`].
    #[cfg(any(test, feature = "test-util"))]
    fn planted_copy_before_j_sql(&self) -> String {
        [&self.from, &self.joined]
            .iter()
            .zip(PLANTED_BEFORE_J)
            .map(|(side, copy)| {
                format!(
                    "drop table if exists {copy}; \
                     create temporary table {copy} on commit drop as select {} from {}",
                    Self::planted_partials(side, false).join(", "),
                    side.plan.hidden()
                )
            })
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// The group columns and partials of `side`'s hidden table, or of its
    /// deltas table (`deltas`), named as the hidden table's.
    #[cfg(any(test, feature = "test-util"))]
    fn planted_partials(side: &SidePlan, deltas: bool) -> Vec<String> {
        let mut columns = quoted(&side.plan.shape.group_cols);
        let named = |delta: String, hidden: String| {
            if deltas {
                format!("{delta} as {hidden}")
            } else {
                hidden
            }
        };
        columns.push(named("__dn".into(), "__n".into()));
        for (i, arg) in side.plan.shape.args.iter().enumerate() {
            let i = i + 1;
            columns.push(named(format!("__dc{i}"), format!("__count_{i}")));
            if arg.summed {
                columns.push(named(format!("__ds{i}"), format!("__sum_{i}")));
            }
        }
        columns
    }

    /// Planted bug `join_other_side_before_j`: builds [`PLANTED_STALE`],
    /// each side's copy from before the `J` locks plus the page's own
    /// changes to it; and the query of how many cells differ from the hidden
    /// tables now (those another page changed in between).
    #[cfg(any(test, feature = "test-util"))]
    fn planted_stale_sides_sql(&self) -> (String, String) {
        let mut statements = Vec::new();
        let mut differ = Vec::new();
        for ((side, copy), stale) in [&self.from, &self.joined]
            .iter()
            .zip(PLANTED_BEFORE_J)
            .zip(PLANTED_STALE)
        {
            let cols = quoted(&side.plan.shape.group_cols).join(", ");
            let partials = Self::planted_partials(side, false);
            let sums: Vec<String> = partials[side.plan.shape.group_cols.len()..]
                .iter()
                .map(|c| format!("sum({c}) as {c}"))
                .collect();
            statements.push(format!(
                "drop table if exists {stale}; \
                 create temporary table {stale} on commit drop as \
                 select {cols}, {sums} from ( \
                     select {all} from {copy} \
                     union all select {deltas} from {table} \
                 ) as x group by {cols}",
                sums = sums.join(", "),
                all = partials.join(", "),
                deltas = Self::planted_partials(side, true).join(", "),
                table = side.deltas,
            ));
            differ.push(format!(
                "(select count(*) from ((select {all} from {stale} except select {all} from {h}) \
                  union all (select {all} from {h} except select {all} from {stale})) as d)",
                all = partials.join(", "),
                h = side.plan.hidden(),
            ));
        }
        (
            statements.join("; "),
            format!("select ({})::bigint", differ.join(" + ")),
        )
    }

    /// Planted bug `join_drop_flip` (`crate::plant`): how many keys the
    /// page's joined-side changes move across zero matches, read before the
    /// product adds them to `J.matches`.
    #[cfg(any(test, feature = "test-util"))]
    fn planted_flips_sql(&self) -> String {
        let jc = quoted(&self.j_cols);
        let mj = quoted(&self.joined.join_cols);
        let mj_as: Vec<String> = mj
            .iter()
            .zip(&jc)
            .map(|(c, j)| format!("{c} as {j}"))
            .collect();
        let j_x: Vec<String> = jc.iter().map(|c| format!("j.{c} = x.{c}")).collect();
        format!(
            "select count(*) from (select {mj_as}, sum(__dn) as __d from {deltas} \
                                   group by {mj}) x \
             join {j} j on {j_x} \
             where x.__d <> 0 and (j.__matches = 0) <> (j.__matches + x.__d = 0)",
            mj_as = mj_as.join(", "),
            deltas = self.joined.deltas,
            mj = mj.join(", "),
            j = self.j_ident,
            j_x = j_x.join(" and "),
        )
    }
}

/// Planted bug `join_other_side_before_j`: the copies of `P` and `M` taken
/// before a page's `J` locks, and the same with the page's own changes added.
#[cfg(any(test, feature = "test-util"))]
const PLANTED_BEFORE_J: [&str; 2] = [
    "pg_temp.trellis_planted_p_before_j",
    "pg_temp.trellis_planted_m_before_j",
];
#[cfg(any(test, feature = "test-util"))]
const PLANTED_STALE: [&str; 2] = [
    "pg_temp.trellis_planted_p_stale",
    "pg_temp.trellis_planted_m_stale",
];

/// Runs `point` for the plan's target (test builds only).
macro_rules! pause {
    ($txn:expr, $point:ident, $target:expr) => {
        #[cfg(any(test, feature = "test-util"))]
        super::interleave::pause_at($txn, super::interleave::PausePoint::$point, $target).await?;
    };
}

/// One page's work on one `JOIN` target (see the module doc). Returns the
/// visible rows written and deleted.
#[tracing::instrument(
    name = "staging.apply_join_target",
    skip_all,
    fields(
        target = %plan.target,
        from_sides = plan.from.plan.sides.len(),
        joined_sides = plan.joined.plan.sides.len(),
        truncate = plan.truncate.is_some(),
    )
)]
pub(crate) async fn apply_join_target(
    txn: &Transaction<'_>,
    plan: &JoinTargetPlan,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    // 1. The working tables (once a session), then each side's staging and
    // the cells its image-less changes name.
    create_working_tables(txn, plan).await?;
    for side in [&plan.from, &plan.joined] {
        if !side.plan.sides.is_empty() {
            group_basis::stage_sides(txn, &side.plan).await?;
        }
        if !side.plan.recompute_keys().is_empty() {
            group_basis::name_groups(txn, &side.plan, &[], side.plan.recompute_keys()).await?;
        }
    }
    write_locked(txn, plan, mutations, false, false).await
}

/// Creates every working table of `plan`'s writers, once a session.
async fn create_working_tables(
    txn: &Transaction<'_>,
    plan: &JoinTargetPlan,
) -> Result<(), ApplyError> {
    let mut ddl = Vec::with_capacity(5);
    for side in [&plan.from, &plan.joined] {
        ddl.push(side.plan.working_ddl());
        ddl.extend(side.plan.deltas_ddl());
    }
    ddl.push(plan.t.staged_ddl());
    txn.batch_execute(&ddl.join(";\n")).await?;
    Ok(())
}

/// Steps 2 to 5 of a page (see the module doc), over the sides already
/// staged and the cells already named: the lock row and a truncate's clear,
/// `J`, the cells, the cells' changes, the product and `T`.
///
/// With `build`, this is the transaction of [`build`]: `T`'s write is the
/// build's (every group of the visible target and of `T`'s hidden table is
/// locked, and every visible row compared with the row the target holds).
/// With `bulk`, a transaction of [`build`] whatever its work: a side that
/// reads its cells from one `GROUP BY` of its table or leaf locks only its
/// `NULL`-key cells and writes each cell once, and the working tables are
/// analyzed after each statement that fills them.
async fn write_locked(
    txn: &Transaction<'_>,
    plan: &JoinTargetPlan,
    mutations: &mut TargetMutations,
    build: bool,
    bulk: bool,
) -> Result<(usize, usize), ApplyError> {
    // 2. The pair's lock row, a truncate's clear, `J`, then the cells.
    let mode = if plan.truncate.is_some() {
        "update"
    } else {
        "share"
    };
    txn.execute(
        &format!("select 1 from {} for {mode}", plan.lock_ident),
        &[],
    )
    .await?;
    let mut deleted = 0;
    if let Some(truncate) = &plan.truncate {
        deleted += clear(txn, plan, truncate, mutations).await?;
    }
    // Planted bug (#1114): read both sides' partials before the `J` locks.
    // See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let before_j =
        crate::plant::armed() == Some(crate::plant::Plant::JoinOtherSideBeforeJ) && !build;
    #[cfg(any(test, feature = "test-util"))]
    if before_j {
        txn.batch_execute(&plan.planted_copy_before_j_sql()).await?;
    }
    pause!(txn, BeforeJoinLock, &plan.target);
    // Planted bug (#1114): insert the missing `J` rows and lock none. See
    // `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let skip_j_lock = crate::plant::fires(crate::plant::Plant::JoinNoJLock, !build);
    #[cfg(not(any(test, feature = "test-util")))]
    let skip_j_lock = false;
    #[cfg(any(test, feature = "test-util"))]
    if skip_j_lock {
        txn.execute(&plan.planted_j_insert_missing_sql(), &[])
            .await?;
    }
    if !skip_j_lock {
        txn.execute(&plan.j_insert_sql(), &[]).await?;
    }
    pause!(txn, AfterJoinKeyInsert, &plan.target);
    let waiting_since = std::time::Instant::now();
    if !skip_j_lock {
        txn.execute(&plan.j_lock_sql(), &[]).await?;
    }
    // A build's wait for the keys its pages hold is not a page's contention.
    if !build {
        crate::metrics::record_join_lock_wait(&plan.target, waiting_since.elapsed());
    }
    pause!(txn, AfterJoinLock, &plan.target);
    let sides = [&plan.from, &plan.joined];
    for side in sides {
        if side.writes() {
            let lock = match side.bulk_node(bulk) {
                // A build's keyed cells are held by `J`; only those with a
                // `NULL` key part, which take no `J` row, are locked as
                // cells (#1097's rule 6).
                Some(_) => side.plan.lock_sql_where(&side.null_key_filter()),
                None => side.plan.lock_sql(),
            };
            txn.execute(&lock, &[]).await?;
        }
    }
    pause!(txn, AfterEntryLock, &plan.target);
    // 3. Each side's cells, recording the change applied per cell, then
    // the cells it recomputes: a build's in one write per cell.
    for side in sides {
        if !side.plan.sides.is_empty() {
            group_basis::apply_locked_groups(txn, &side.plan, mutations).await?;
        }
        if side.recompute {
            match side.bulk_node(bulk) {
                Some(node) => {
                    group_basis::write_join_build_cells(
                        txn,
                        &side.plan,
                        node,
                        &side.grouped,
                        &side.matched,
                    )
                    .await?;
                }
                None => {
                    group_basis::recompute_locked_groups_from(
                        txn,
                        &side.plan,
                        side.node.as_deref(),
                        mutations,
                    )
                    .await?;
                }
            }
        }
        pause!(txn, BetweenJoinSideWrites, &plan.target);
    }
    if bulk {
        group_basis::analyze(txn, &[&plan.from.deltas, &plan.joined.deltas]).await?;
    }
    // 4. The product into `T`'s staging, and `J.matches`. With nested
    // loops left on: its joins look up the other side's cells of a key
    // through the cell identity's index.
    let product = plan.product_sql();
    // The planted bug's partials from before the `J` locks, with the page's
    // own changes added, read in place of each side's.
    #[cfg(any(test, feature = "test-util"))]
    let product = if before_j {
        let (build_stale, count_changed) = plan.planted_stale_sides_sql();
        txn.batch_execute(&build_stale).await?;
        let changed: i64 = txn.query_one(&count_changed, &[]).await?.get(0);
        crate::plant::fires(crate::plant::Plant::JoinOtherSideBeforeJ, changed > 0);
        product
            .replace(plan.from.plan.hidden(), PLANTED_STALE[0])
            .replace(plan.joined.plan.hidden(), PLANTED_STALE[1])
    } else {
        product
    };
    // Planted bug (#1114): count the pages whose product the dropped flip
    // changes. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    if plan.left && crate::plant::armed() == Some(crate::plant::Plant::JoinDropFlip) {
        let flips: i64 = txn.query_one(&plan.planted_flips_sql(), &[]).await?.get(0);
        crate::plant::fires(crate::plant::Plant::JoinDropFlip, flips > 0);
    }
    if bulk {
        txn.batch_execute(group_basis::BULK_PLAN_SETTINGS).await?;
    }
    txn.execute(&product, &[]).await?;
    if bulk {
        txn.batch_execute(group_basis::BULK_PLAN_RESET).await?;
    }
    // 5. `T`: lock the changed groups, sorted, then write them. A build
    // locks every group of the visible target and of `T` as well, and
    // compares each visible row with the one the target holds.
    if build {
        group_basis::enumerate_target_groups(txn, &plan.t).await?;
    }
    if bulk {
        group_basis::analyze(txn, &[plan.t.staged()]).await?;
    }
    pause!(txn, BeforeJoinTargetLock, &plan.target);
    // Planted bug (#1114): insert the missing `T` groups and lock none. See
    // `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let t_lock = if crate::plant::fires(crate::plant::Plant::JoinTSkipLock, !build) {
        plan.t.planted_insert_missing_sql()
    } else {
        plan.t.lock_sql()
    };
    #[cfg(not(any(test, feature = "test-util")))]
    let t_lock = plan.t.lock_sql();
    txn.execute(&t_lock, &[]).await?;
    pause!(txn, AfterJoinTargetLock, &plan.target);
    let (written, removed) = if build {
        group_basis::apply_join_build_groups(txn, &plan.t, mutations).await?
    } else {
        let (_, written, removed) =
            group_basis::apply_locked_groups(txn, &plan.t, mutations).await?;
        (written, removed)
    };
    pause!(txn, AfterGroupUpsert, &plan.target);
    Ok((written, deleted + removed))
}

/// A page's `TRUNCATE` clear (see the module doc), under the lock row held
/// `FOR UPDATE`: empties the truncated sides' cells and raises their
/// floors, zeroes every `J.matches` for the joined side, empties `T` and
/// the visible target (through the seam), and for the joined side refills
/// `T`'s staging from `P`. Returns how many visible rows it deleted.
async fn clear(
    txn: &Transaction<'_>,
    plan: &JoinTargetPlan,
    truncate: &Truncate,
    mutations: &mut TargetMutations,
) -> Result<usize, ApplyError> {
    for (side, lsn) in [(&plan.from, truncate.from), (&plan.joined, truncate.joined)] {
        if let Some(lsn) = lsn {
            let hidden = side.plan.hidden();
            txn.batch_execute(&format!("delete from {hidden}")).await?;
            group_basis::raise_truncate_floor(txn, hidden, lsn).await?;
        }
    }
    if truncate.joined.is_some() {
        txn.batch_execute(&format!(
            "update {} set __matches = 0 where __matches <> 0",
            plan.j_ident
        ))
        .await?;
    }
    txn.batch_execute(&format!("delete from {}", plan.t.hidden()))
        .await?;
    let deleted = super::apply::clear_target(
        txn,
        &plan.target,
        &plan.identity,
        truncate.hop_gen,
        truncate.src_changed,
        truncate.origin_lsn,
        mutations,
    )
    .await?;
    if truncate.joined.is_some() && plan.left {
        txn.execute(&plan.refill_sql(), &[]).await?;
    }
    Ok(deleted)
}

/// A `JOIN`'s plan for definition `definition`, with every catalog read it
/// needs ([`JoinTargetPlan::new`]).
pub(crate) async fn plan_for(
    client: &impl tokio_postgres::GenericClient,
    catalog_schema: &str,
    definition: &Definition,
) -> Result<JoinTargetPlan, ApplyError> {
    let identity = ddl::identity_key_columns(client, &definition.target_table).await?;
    let joined = crate::defs::catalog::join_side_of(client, definition.id)
        .await?
        .ok_or_else(|| ApplyError::GroupBasisUnsupported {
            target: definition.target_table.clone(),
            what: "a JOIN definition with no joined table recorded",
        })?;
    let field_types = target_column_types(client, &definition.target_table).await?;
    let hidden_columns = hidden_table_columns(client, catalog_schema, definition.id).await?;
    let source_pks = [
        ddl::identity_key_columns(client, &definition.source_table).await?,
        ddl::identity_key_columns(client, &joined.0).await?,
    ];
    JoinTargetPlan::new(
        catalog_schema,
        definition,
        &joined,
        &field_types,
        &hidden_columns,
        identity,
        source_pks,
    )
}

/// A quarantine release's (or the automatic retry's) work on `definition`, a
/// `JOIN` that applies, for `key` of `src_table` (see the module doc):
/// recomputes every cell of the key's side named by `cells` (the held row's
/// `agg_groups`: objects of the side's cell columns) and the cell of the
/// key's current row, carries the change to `T` under the `J` locks, and
/// stages the seam's rows for every visible row that changed. The caller
/// holds the version fence of `src_table`, so no page of it runs meanwhile.
/// Returns whether `definition` is such a `JOIN`; when it isn't, does
/// nothing.
pub(crate) async fn release(
    txn: &Transaction<'_>,
    catalog_schema: &str,
    definition: &Definition,
    src_table: &str,
    cells: &[String],
    key: &str,
) -> Result<bool, ApplyError> {
    if !applies(&definition.def) {
        return Ok(false);
    }
    let Some(side) = spec::side_of_table(&definition.def, src_table) else {
        return Ok(false);
    };
    let mut plan = plan_for(txn, catalog_schema, definition).await?;
    plan.side_mut(side).recompute = true;
    create_working_tables(txn, &plan).await?;
    let side_plan = match side {
        Side::From => &plan.from,
        Side::Joined => &plan.joined,
    };
    group_basis::name_groups(txn, &side_plan.plan, cells, &[key.to_string()]).await?;
    let mut mutations = TargetMutations::new();
    write_locked(txn, &plan, &mut mutations, false, false).await?;
    let propagation = mutations.into_staged(txn).await?;
    super::append::append(txn, &propagation.changes).await?;
    Ok(true)
}

/// A partition correction's recompute on `definition`, a `JOIN` that
/// applies (#1111, ADR-0019 "Either side may be partitioned";
/// `super::partition_correction`): every cell of `side` named by `cells`
/// (objects of the side's cell columns: the cells that route to the node,
/// which the correction enumerates) is locked and rewritten from a
/// `GROUP BY` of its rows, with the statement's snapshot as its basis,
/// through steps 2 to 5 of a page, so each cell's change reaches `T` under
/// its `J` lock in this transaction. The rows are read through the root,
/// or, when `node` (quoted and qualified) is given, by one `GROUP BY` of
/// that partition, whose caller has established that it holds every row of
/// every named cell. Then the seam's rows for every visible row that
/// changed. A change the node's operation removed that is still in the
/// ring has a transaction the basis sees, so its page refuses it.
pub(crate) async fn recompute_routed(
    txn: &Transaction<'_>,
    catalog_schema: &str,
    definition: &Definition,
    side: Side,
    cells: &[String],
    node: Option<&str>,
) -> Result<(), ApplyError> {
    let mut plan = plan_for(txn, catalog_schema, definition).await?;
    let side_plan = plan.side_mut(side);
    side_plan.recompute = true;
    side_plan.node = node.map(str::to_string);
    create_working_tables(txn, &plan).await?;
    let side_plan = match side {
        Side::From => &plan.from,
        Side::Joined => &plan.joined,
    };
    group_basis::name_groups(txn, &side_plan.plan, cells, &[]).await?;
    let mut mutations = TargetMutations::new();
    write_locked(txn, &plan, &mut mutations, false, false).await?;
    let propagation = mutations.into_staged(txn).await?;
    super::append::append(txn, &propagation.changes).await?;
    Ok(())
}

/// One transaction of a `JOIN`'s fresh build, `RESUME` or rebuild in place
/// (#1109, #1097's decision), in the caller's `READ COMMITTED` transaction,
/// which commits it. `work` is what it covers ([`BuildWork`]).
///
/// [`BuildWork::Single`], both sides unpartitioned (or empty): both
/// structures, `P` and `M`, and `T`, so readers see the old `T` and the old
/// visible target until the commit.
///
/// 1. The lock row `FOR SHARE`, before the enumeration, so a `TRUNCATE` page
///    (the row `FOR UPDATE`) waits for the commit and clears what it empties
///    after; a `TRUNCATE` statement queues on the `ACCESS SHARE` the
///    enumeration takes on each table.
/// 2. **Enumerate** each side's cells (R8, [`group_basis::enumerate_cells`]):
///    its hidden table's, its table's, and those of the ring's old images.
/// 3. Steps 2 to 5 of a page ([`write_locked`]) with every enumerated cell
///    of both sides recomputed by one `GROUP BY` of its whole table: `J` on
///    the union of both sides' keys in one sorted statement, the `NULL`-key
///    cells, `P`'s and `M`'s snapshot statements and absolute writes (each
///    side's `GROUP BY`, after every lock statement, holds the snapshot that
///    is its cells' basis; each cell is written once), then the product of
///    the cells' changes into `T`.
///    A cell first seen after the enumeration is not locked, so it is not
///    written: its pages apply it.
/// 4. `T`: its groups are locked after the product, so the set is the
///    product's groups, read from rows held under `J`, plus every group of the
///    visible target and of `T`'s hidden table (R8 one level up). `T` changes
///    only as the product's delta; every locked group's visible row is then
///    compared with the row the target holds, which removes the row of a
///    group no source row backs and repairs a row that disagrees with `T`'s
///    partials.
///
/// [`BuildWork::Side`], one step of a build with a partitioned side: steps 1
/// to 3 over that side's cells in the region alone, `J` on their keys, `T`
/// locked and written as a page writes it (only the product's groups, as a
/// delta from the partials). [`BuildWork::Final`]: the lock row, then step 4's
/// lock of every group of the visible target and of `T`'s hidden table, and
/// its compare, with no product.
///
/// A hand edit of `T` or of `J.matches` is not repaired: they change only by
/// delta (a `RESUME` recreates them). Returns the visible rows written and
/// deleted.
pub(crate) async fn build(
    txn: &Transaction<'_>,
    catalog_schema: &str,
    definition: &Definition,
    work: &BuildWork<'_>,
) -> Result<(usize, usize), ApplyError> {
    let mut plan = plan_for(txn, catalog_schema, definition).await?;
    pause!(txn, BeforeJoinBuild, &plan.target);
    create_working_tables(txn, &plan).await?;
    let mut mutations = TargetMutations::new();
    let counts = match work {
        BuildWork::Single => {
            for side in [&mut plan.from, &mut plan.joined] {
                side.recompute = true;
                side.node = Some(side.plan.source_row().to_string());
            }
            txn.execute(&format!("select 1 from {} for share", plan.lock_ident), &[])
                .await?;
            for side in [&mut plan.from, &mut plan.joined] {
                group_basis::enumerate_cells(txn, &mut side.plan, None, &side.cells).await?;
            }
            write_locked(txn, &plan, &mut mutations, true, true).await?
        }
        BuildWork::Side { side, scope } => {
            let side_plan = plan.side_mut(*side);
            side_plan.recompute = true;
            // The region's rows come from one `GROUP BY` of the table (a whole
            // side) or of the leaf, if the tree is as recorded; otherwise
            // each cell's rows are probed through the root.
            side_plan.node = match scope {
                None => Some(side_plan.plan.source_row().to_string()),
                Some(scope) if scope.read_node => scope.node.clone(),
                Some(_) => None,
            };
            txn.execute(&format!("select 1 from {} for share", plan.lock_ident), &[])
                .await?;
            let side_plan = plan.side_mut(*side);
            group_basis::enumerate_cells(txn, &mut side_plan.plan, *scope, &side_plan.cells)
                .await?;
            write_locked(txn, &plan, &mut mutations, false, true).await?
        }
        BuildWork::Final => {
            txn.execute(&format!("select 1 from {} for share", plan.lock_ident), &[])
                .await?;
            group_basis::enumerate_target_groups(txn, &plan.t).await?;
            pause!(txn, BeforeJoinTargetLock, &plan.target);
            txn.execute(&plan.t.lock_sql(), &[]).await?;
            pause!(txn, AfterJoinTargetLock, &plan.target);
            group_basis::apply_join_build_groups(txn, &plan.t, &mut mutations).await?
        }
    };
    let propagation = mutations.into_staged(txn).await?;
    super::append::append(txn, &propagation.changes).await?;
    Ok(counts)
}

/// What one transaction of a `JOIN`'s build does ([`build`]).
#[derive(Clone, Copy)]
pub(crate) enum BuildWork<'a> {
    /// The whole build: both sides unpartitioned (or empty), S1T.
    Single,
    /// One side's cells in a region, a step of the per-side, per-leaf build
    /// of a `JOIN` with a partitioned side that has rows (#1286): the whole
    /// side (`scope` none), or the cells a leaf or the remainder of its
    /// recorded tree covers.
    Side {
        side: Side,
        scope: Option<&'a group_basis::RebuildScope>,
    },
    /// The last step of that build: every group of `T` and of the visible
    /// target compared.
    Final,
}

/// The column list of each of definition `id`'s hidden tables in
/// `catalog_schema`, by bare name, for [`JoinTargetPlan::new`]'s working
/// table names ([`working_table_name`]): each column's name, exact type
/// (`format_type`) and collation, in column order.
pub(crate) async fn hidden_table_columns(
    client: &impl tokio_postgres::GenericClient,
    catalog_schema: &str,
    id: i64,
) -> Result<HashMap<String, String>, tokio_postgres::Error> {
    let names: Vec<String> = HiddenTable::ALL.iter().map(|t| t.name(id)).collect();
    Ok(client
        .query(
            &format!(
                "select c.relname::text, \
                        string_agg(pg_catalog.format('%I %s %s', a.attname, {}, \
                                       coalesce({}, '')), \
                                   ',' order by a.attnum) \
                 from pg_catalog.pg_class c \
                 join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
                 join pg_catalog.pg_attribute a on a.attrelid = c.oid \
                 where n.nspname = $1 and c.relname = any($2) \
                   and a.attnum > 0 and not a.attisdropped \
                 group by c.relname",
                crate::defs::copy_type::type_sql("a.atttypid"),
                crate::defs::copy_type::collation_sql("a.attcollation")
            ),
            &[&catalog_schema, &names],
        )
        .await?
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect())
}

/// The visible target's column types (`format_type`), by name, for
/// [`JoinTargetPlan::new`].
pub(crate) async fn target_column_types(
    client: &impl tokio_postgres::GenericClient,
    target: &str,
) -> Result<HashMap<String, String>, tokio_postgres::Error> {
    Ok(client
        .query(
            &format!(
                "select attname::text, {} \
                 from pg_catalog.pg_attribute \
                 where attrelid = pg_catalog.to_regclass($1) and attnum > 0 and not attisdropped",
                crate::defs::copy_type::type_sql("atttypid")
            ),
            &[&crate::defs::ddl::regclass_arg(target)],
        )
        .await?
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The name of every `JOIN` working table follows its hidden table's
    /// column list, and fits a Postgres identifier for any id.
    #[test]
    fn a_working_table_name_follows_its_hidden_columns() {
        let int4 = "g integer ,k text ";
        assert_eq!(
            working_table_name("dp", 7, int4),
            working_table_name("dp", 7, int4)
        );
        assert_ne!(
            working_table_name("dp", 7, int4),
            working_table_name("dp", 7, "g bigint ,k text ")
        );
        assert_ne!(
            working_table_name("dp", 7, int4),
            working_table_name("dp", 7, "g integer ,k text C")
        );
        assert!(working_table_name("dp", 7, int4).starts_with("trellis_join_dp_7_"));
        let longest = working_table_name("dp", i64::MIN, int4);
        assert!(longest.len() <= 63, "{longest} fits a Postgres identifier");
    }

    /// A re-typed or re-collated key column of a hidden table under the
    /// same definition id (as after a resume, which keeps the id and
    /// recreates the hidden tables) gives its working tables a new name, so
    /// a pooled connection never reuses a temp table of the old shape (#1142).
    #[tokio::test]
    async fn a_retyped_or_recollated_hidden_column_renames_the_working_tables() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(
                "create schema if not exists trellis_t; \
                 create table trellis_t.join_7_p (g integer, k text, __n bigint); \
                 create table trellis_t.join_7_t (g integer, __n bigint)",
            )
            .await
            .expect("hidden tables");
        let names = |columns: &HashMap<String, String>| {
            (
                working_table_name("p", 7, columns.get("join_7_p").map_or("", String::as_str)),
                working_table_name("t", 7, columns.get("join_7_t").map_or("", String::as_str)),
            )
        };
        let first = hidden_table_columns(&client, "trellis_t", 7)
            .await
            .expect("columns");
        assert_eq!(first.len(), 2, "{first:?}");
        client
            .batch_execute("alter table trellis_t.join_7_p alter column g type bigint")
            .await
            .expect("re-type");
        let retyped = hidden_table_columns(&client, "trellis_t", 7)
            .await
            .expect("columns");
        assert_ne!(names(&first).0, names(&retyped).0);
        assert_eq!(names(&first).1, names(&retyped).1, "T is unchanged");
        client
            .batch_execute("alter table trellis_t.join_7_p alter column k type text collate \"C\"")
            .await
            .expect("re-collate");
        let recollated = hidden_table_columns(&client, "trellis_t", 7)
            .await
            .expect("columns");
        assert_ne!(names(&retyped).0, names(&recollated).0);
    }
}
