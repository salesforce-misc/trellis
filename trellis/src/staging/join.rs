//! A `JOIN` transform's apply step (#1108 J3, ADR-0019).
//!
//! # When it runs
//!
//! Only behind the group-basis dev knob (`super::group_basis::enabled`):
//! nothing builds a `JOIN` yet (J4, #1109), and with the knob off one stays
//! `waiting_to_backfill` and never applies. With the knob on, a `JOIN`
//! definition ([`applies`]) is built like a knob-on aggregate: its plan job
//! only checks that both tables are empty, unpartitioned and no other
//! definition's target (`super::build`), so it goes `live` with empty hidden
//! tables and applies every captured change from then on.
//! A `TRUNCATE` of either side, an image-less change (a Re-derive, a
//! released key, a catch-up) and a rebuild halt it: they are J5's (#1110)
//! and J4's. A knob-on aggregate takes the first two (#1102), but a `JOIN`'s
//! own truncate and recompute need its `J` and pair locks.
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
//!    never staged.
//! 2. **Lock (JR1).** The pair's lock row `FOR SHARE`. Then `J`: one sorted
//!    `insert … on conflict do nothing` of every non-null key the page
//!    names, then one sorted `select … for update` of the same keys. `J`
//!    rows are never deleted (ADR-0019), which is what makes that pair of
//!    statements exact. A key with a `NULL` part takes no `J` row and no
//!    lock. Then the `P` cells and the `M` cells, each in one sorted
//!    statement. `FOR UPDATE` on both sides makes every key's lock mode the
//!    page's one mode, so a mixed page never upgrades.
//! 3. **Apply the sides.** #1100's writer applies the staged sides to `P`
//!    and `M` (each cell's basis and its side's truncate floor filter them),
//!    and records the change it applied per cell.
//! 4. **The product (JR3).** One statement computes the change to `T` from
//!    the cells' changes and the partials the page holds locked:
//!    `ΔP ⊗ M_new + P_old ⊗ ΔM`, and for `LEFT` the unmatched terms: a `P`
//!    cell with a `NULL` key part always lands in the `NULL` joined group,
//!    and every `P` cell of a key moves in or out of it as the key's
//!    `J.matches` crosses zero. The same statement adds `ΣΔM.n` to each
//!    key's `J.matches`.
//! 5. **`T`.** The changed `T` groups are created and locked in one sorted
//!    statement, then #1100's writer adds the deltas to `T`'s hidden
//!    partials and writes each visible row whose value changed (only while
//!    its row count is above 0), through the target-mutation seam.
//!
//! The lock order is ADR-0019's: lock row, `J` sorted, `P` then `M` cells
//! sorted, `T` groups sorted. A page applies its `JOIN` targets in target
//! order, after its other aggregate targets.

use std::collections::HashMap;

use tokio_postgres::Transaction;

use crate::defs::ast::{Expr, KeySpace, TransformDef, ValueType};
use crate::defs::ddl::PrimaryKeyColumn;
use crate::defs::join::{self as spec, HiddenTable, Side};
use crate::defs::model::Definition;
use crate::defs::oracle::render_to_one_rel_expr_sql;
use crate::pool::quote_ident;

use super::apply::ApplyError;
use super::fold::FoldedChange;
use super::group_basis::{self, Arg, GroupShape, GroupTargetPlan, Writer};
use super::target_mutations::TargetMutations;

/// Whether `def` is a `JOIN` that applies: only behind the group-basis knob
/// (see the module doc).
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
}

/// One page's work on one `JOIN` target ([`apply_join_target`]).
#[derive(Debug, Clone)]
pub(crate) struct JoinTargetPlan {
    /// The target, qualified and unquoted.
    pub(crate) target: String,
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
    /// tables' column lists ([`hidden_table_columns`]), and `identity` the
    /// target's group identity.
    pub(crate) fn new(
        catalog_schema: &str,
        definition: &Definition,
        joined: &(String, HashMap<String, ValueType>),
        field_types: &HashMap<String, String>,
        hidden_columns: &HashMap<String, String>,
        identity: Vec<PrimaryKeyColumn>,
    ) -> Result<Self, ApplyError> {
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
                sql: render_to_one_rel_expr_sql(arg, "s"),
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
            crate::defs::validate::infer_expr_type(arg, types_of(side), &HashMap::new()).ok()
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
        let side_plan = |side: Side, table: HiddenTable, source: &str, tag: &str| {
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
                    Some(source),
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
                ),
                join_cols,
                deltas,
            }
        };
        let from = side_plan(
            Side::From,
            HiddenTable::FromPartials,
            &definition.source_table,
            "p",
        );
        let joined_plan = side_plan(Side::Joined, HiddenTable::JoinedPartials, &joined.0, "m");
        let t = GroupTargetPlan::for_join(
            target,
            hidden_ident(catalog_schema, id, HiddenTable::TargetPartials),
            &working("t", HiddenTable::TargetPartials),
            None,
            identity,
            t_shape,
            Writer::JoinTarget,
            None,
            String::new(),
        );
        Ok(Self {
            target: target.to_string(),
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

    /// Adds one folded change of `side`'s table. An image-less change can't
    /// be applied as deltas (see the module doc).
    pub(crate) fn push(&mut self, side: Side, change: &FoldedChange) -> Result<(), ApplyError> {
        match side {
            Side::From => self.from.plan.push(change)?,
            Side::Joined => self.joined.plan.push(change)?,
        }
        self.t.note(change);
        Ok(())
    }

    /// The keys the page's staged cells name, as `J`'s columns, with no
    /// `NULL` part: `select …` over both staging tables.
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
        format!(
            "select * from (select {} from {} union select {} from {}) k where {}",
            as_j(&self.from.join_cols),
            self.from.plan.staged(),
            as_j(&self.joined.join_cols),
            self.joined.plan.staged(),
            not_null.join(" and ")
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
        let (dp, dm) = (&self.from.deltas, &self.joined.deltas);
        let p_args = self.from.plan.shape.args.clone();
        let m_args = self.joined.plan.shape.args.clone();
        let fa = p_args.len();
        let p_keys = quoted(&self.from.plan.shape.group_cols);
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
        // `NULL` matching `NULL` (the array form hashes).
        let cell_eq: String = p_keys
            .iter()
            .map(|c| format!("array[d.{c}] = array[p.{c}]"))
            .collect::<Vec<_>>()
            .join(" and ");
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
        let mut ctes = Vec::new();
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
                terms.push(format!(
                    "select {} from jd join {p} p on {} left join {dp} d on {cell_eq} \
                     where jd.__new <> 0 or jd.__old <> 0",
                    term(
                        "p",
                        None,
                        format!("jd.__new * p.__n - jd.__old * {p_old_n}"),
                        counts,
                        sums
                    ),
                    key_eq("p", &pj, "jd", &jc),
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
            ctes.push(format!(
                "jd as ( \
                     select {jk}, (j.__matches + coalesce(x.__d, 0) = 0)::int as __new, \
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
    )
)]
pub(crate) async fn apply_join_target(
    txn: &Transaction<'_>,
    plan: &JoinTargetPlan,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    let sides = [&plan.from, &plan.joined];
    // 1. The working tables (once a session), then each side's staging.
    let mut ddl = Vec::with_capacity(5);
    for side in sides {
        ddl.push(side.plan.staged_ddl());
        ddl.extend(side.plan.deltas_ddl());
    }
    ddl.push(plan.t.staged_ddl());
    txn.batch_execute(&ddl.join(";\n")).await?;
    for side in sides {
        if !side.plan.sides.is_empty() {
            group_basis::stage_sides(txn, &side.plan).await?;
        }
    }
    // 2. The pair's lock row, `J`, then the cells.
    txn.execute(&format!("select 1 from {} for share", plan.lock_ident), &[])
        .await?;
    pause!(txn, BeforeJoinLock, &plan.target);
    txn.execute(&plan.j_insert_sql(), &[]).await?;
    pause!(txn, AfterJoinKeyInsert, &plan.target);
    txn.execute(&plan.j_lock_sql(), &[]).await?;
    pause!(txn, AfterJoinLock, &plan.target);
    for side in sides {
        if !side.plan.sides.is_empty() {
            txn.execute(&side.plan.lock_sql(), &[]).await?;
        }
    }
    pause!(txn, AfterEntryLock, &plan.target);
    // 3. Each side's cells, recording the change applied per cell.
    for side in sides {
        if !side.plan.sides.is_empty() {
            group_basis::apply_locked_groups(txn, &side.plan, mutations).await?;
        }
    }
    // 4. The product into `T`'s staging, and `J.matches`. With nested
    // loops left on: its joins look up the other side's cells of a key
    // through the cell identity's index.
    txn.execute(&plan.product_sql(), &[]).await?;
    // 5. `T`: lock the changed groups, sorted, then write them.
    txn.execute(&plan.t.lock_sql(), &[]).await?;
    pause!(txn, AfterJoinTargetLock, &plan.target);
    let (_, written, deleted) = group_basis::apply_locked_groups(txn, &plan.t, mutations).await?;
    pause!(txn, AfterGroupUpsert, &plan.target);
    Ok((written, deleted))
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
            "select c.relname::text, \
                    string_agg(pg_catalog.format('%I %s %s', a.attname, \
                                   pg_catalog.format_type(a.atttypid, a.atttypmod), \
                                   coalesce(co.collname::text, '')), \
                               ',' order by a.attnum) \
             from pg_catalog.pg_class c \
             join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
             join pg_catalog.pg_attribute a on a.attrelid = c.oid \
             left join pg_catalog.pg_collation co on co.oid = a.attcollation \
             where n.nspname = $1 and c.relname = any($2) \
               and a.attnum > 0 and not a.attisdropped \
             group by c.relname",
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
            "select attname::text, pg_catalog.format_type(atttypid, atttypmod) \
             from pg_catalog.pg_attribute \
             where attrelid = pg_catalog.to_regclass($1) and attnum > 0 and not attisdropped",
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
