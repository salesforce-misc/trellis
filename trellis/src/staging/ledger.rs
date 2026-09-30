//! Plain aggregate targets on the ledger (#623 part D3, first half; epic
//! #556, ADR-0002 invariants I1, I2, I3 and I5).
//!
//! # Which targets
//!
//! [`route`] sends an aggregate target here iff every field is `SUM(x)` or
//! `AVG(x)` over a plain source column `x` of an exact numeric type, or
//! `COUNT(*)`, or `COUNT(x)` over a plain source column of any type but
//! `json`/`jsonb`; every `GROUP BY` key is a plain source column; and it
//! reads no relationship. (The grammar has no definition filter yet.) Every
//! other aggregate target (`MIN`/`MAX` and the other recompute-only
//! aggregates, a composed field, an argument that is an expression, a float
//! `SUM`/`AVG`, a relationship) stays on `super::apply_aggregate`'s path,
//! with its pre-lock, probe and recompute horizons, until #623 D4/D5 move
//! it. A target is entirely on one path or the other.
//!
//! # The ledger
//!
//! `<target>__ledger` ([`crate::defs::ledger`], D2) holds one entry per source
//! key: the key's group, whether it is a live member, one contribution per
//! aggregate argument, and the ordering state (`applied_lsn`, `applied_seg`,
//! `basis`, `tombstone`). A group row is the sum of its live members'
//! entries, plus the hidden member count `__trellis_members`, and is deleted
//! when that count reaches 0 (I3). The one-pass build writes both.
//!
//! # One page, one target ([`apply_ledger_target`])
//!
//! 1. **Lock** (I1, I5): insert a non-member placeholder for every key of the
//!    page that has no entry, then `select … for update` every entry, sorted
//!    by key, in one statement.
//! 2. **Re-derive read**: a record staged as a `recompute` (or folded with
//!    one) is a Re-derive, and so is one with no change identity. One
//!    statement reads those keys' source rows *and* `pg_current_snapshot()`,
//!    after the lock. A key with no row is a tombstone.
//! 3. **One statement** updates the entries, aggregates the moves between
//!    each updated entry's old and new state into per-group increments, and
//!    upserts them in group order (I5), returning each written group:
//!    - a Re-derive writes the entry from its read and sets `basis` to the
//!      read's snapshot, leaving `applied_lsn` alone (the D split's Q1);
//!    - an Apply writes the entry from the change's new image (a delete
//!      makes a tombstone) and sets `applied_lsn`, but only if the change is
//!      not visible in the entry's `basis`, is newer than its `applied_lsn`,
//!      and is above the target's truncate floor (I2, Q6).
//! 4. Groups whose member count reached 0 are deleted.
//!
//! Every written or deleted group reaches the target-mutation seam with its
//! prior image, rebuilt from the upsert's result minus the increments (PG 17
//! has no `OLD` in `RETURNING`), or none for a group the upsert created.
//!
//! Contributions and group keys are cast from an image's text by
//! `jsonb_populate_record` over the ledger's own row type, the same text
//! every other image holds, so the ledger's types decide the casts.

use std::collections::HashMap;
use std::time::SystemTime;

use tokio_postgres::Transaction;
use tokio_postgres::types::PgLsn;

use crate::defs::ast::{
    Expr, FieldDef, GroupByKey, KeySpace, TransformDef, ValueType, group_by_contains,
};
use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::defs::eval;
use crate::defs::ledger as schema;
use crate::defs::pg_type::PgType;
use crate::pool::{quote_ident, quote_literal};

use super::apply::ApplyError;
use super::fold::{FoldedChange, earliest_origin};
use super::target_mutations::TargetMutations;

/// One target column the ledger path maintains, besides its `GROUP BY`
/// columns and `__trellis_members`.
#[derive(Debug, Clone, PartialEq)]
enum LedgerField {
    /// `SUM(x)`: `column` holds the sum of the non-null contributions of
    /// argument `arg`, and is `NULL` when its hidden count `count` is 0.
    Sum {
        column: String,
        count: String,
        arg: usize,
    },
    /// `COUNT(*)`: the group's member count.
    CountStar { column: String },
    /// `COUNT(x)`: the count of argument `arg`'s non-null contributions.
    CountArg { column: String, arg: usize },
    /// `AVG(x)`: `column` is the hidden running sum `sum` over the hidden
    /// count `count` (shared with a `SUM(x)`, issue #48) as `numeric`, which
    /// is Postgres's own `avg()` over an exact numeric argument, and `NULL`
    /// when `count` is 0.
    Avg {
        column: String,
        sum: String,
        count: String,
        arg: usize,
    },
}

/// One contribution column of a ledger-routed target.
#[derive(Debug, Clone, PartialEq)]
struct Contrib {
    /// The ledger column (`__arg<n>`).
    column: String,
    /// The source column it holds.
    source: String,
    /// Whether a `SUM` or `AVG` sums it. A `COUNT(x)`-only argument needs
    /// only its NULL-ness, so it may be of any type and gets no sum
    /// increment.
    summed: bool,
}

/// What a ledger-routed definition's target looks like ([`route`]).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LedgerShape {
    /// The `GROUP BY` columns: each is a source column of the same name, and
    /// the target's and the ledger's column of that name.
    group_cols: Vec<String>,
    /// The contribution columns, in [`schema::contributions`]' order.
    contribs: Vec<Contrib>,
    fields: Vec<LedgerField>,
}

/// Whether `def` is a target the ledger path maintains (see the module doc),
/// and its shape if so.
pub(crate) fn route(
    def: &TransformDef,
    source_columns: &HashMap<String, ValueType>,
) -> Option<LedgerShape> {
    let KeySpace::Aggregate { group_by } = &def.key_space else {
        return None;
    };
    if !eval::relationship_references(def).is_empty() {
        return None;
    }
    let mut group_cols = Vec::with_capacity(group_by.len());
    for key in group_by {
        let GroupByKey::Column(column) = key else {
            return None;
        };
        if !source_columns.contains_key(column) {
            return None;
        }
        group_cols.push(column.clone());
    }
    let substituted = crate::defs::backfill::substituted_field_exprs(def).ok()?;
    let column = |expr: &Expr| -> Option<String> {
        match expr {
            Expr::Column(column) if source_columns.contains_key(column) => Some(column.clone()),
            _ => None,
        }
    };
    let exact = |column: &str| source_columns[column].is_exact_numeric_family();
    // Every field must be a bare `SUM`/`AVG`/`COUNT` call before anything is
    // derived from the fields.
    let value_fields: Vec<&FieldDef> = def
        .fields
        .iter()
        .filter(|f| !group_by_contains(group_by, &f.name))
        .collect();
    for field in &value_fields {
        match &substituted[&field.name] {
            Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {}
            Expr::FunctionCall { name, args }
                if (name == "SUM" || name == "AVG") && args.len() == 1 =>
            {
                if !exact(&column(&args[0])?) {
                    return None;
                }
            }
            Expr::FunctionCall { name, args } if name == "COUNT" && args.len() == 1 => {
                // An image holds a column's output text, which
                // `jsonb_populate_record` would store in a `json`/`jsonb`
                // contribution as a JSON string, not the value the build
                // writes. Every other type goes through its input function.
                if matches!(
                    source_columns[&column(&args[0])?],
                    ValueType::Other(PgType::Json | PgType::Jsonb)
                ) {
                    return None;
                }
            }
            _ => return None,
        }
    }
    let contributions = schema::contributions(&def.fields, group_by, &substituted);
    let summed = |arg: &Expr| {
        value_fields.iter().any(|f| {
            matches!(&substituted[&f.name], Expr::FunctionCall { name, args }
                if (name == "SUM" || name == "AVG") && args.first() == Some(arg))
        })
    };
    let mut contribs = Vec::with_capacity(contributions.len());
    for contribution in &contributions {
        contribs.push(Contrib {
            column: contribution.column.clone(),
            source: column(&contribution.arg)?,
            summed: summed(&contribution.arg),
        });
    }
    let arg_of = |arg: &Expr| -> usize {
        contributions
            .iter()
            .position(|c| c.arg == *arg)
            .expect("every aggregate argument has a contribution column")
    };
    let count_cols = ddl::count_column_names(&def.fields, &substituted);
    let fields = value_fields
        .iter()
        .map(|field| match &substituted[&field.name] {
            Expr::FunctionCall { name, args } if name == "SUM" => LedgerField::Sum {
                column: field.name.clone(),
                count: count_cols[&field.name].clone(),
                arg: arg_of(&args[0]),
            },
            Expr::FunctionCall { name, args } if name == "AVG" => LedgerField::Avg {
                column: field.name.clone(),
                sum: ddl::avg_sum_column(&field.name),
                count: count_cols[&field.name].clone(),
                arg: arg_of(&args[0]),
            },
            Expr::FunctionCall { args, .. } if args.is_empty() => LedgerField::CountStar {
                column: field.name.clone(),
            },
            Expr::FunctionCall { args, .. } => LedgerField::CountArg {
                column: field.name.clone(),
                arg: arg_of(&args[0]),
            },
            _ => unreachable!("checked above"),
        })
        .collect();
    Some(LedgerShape {
        group_cols,
        contribs,
        fields,
    })
}

/// One folded record for a ledger target.
#[derive(Debug, Clone)]
struct LedgerRecord {
    key: String,
    /// `None`: a Re-derive. `Some`: an Apply of the record's last change,
    /// with its `lsn`, `row_txid` and new image (`None` for a delete).
    apply: Option<(PgLsn, String, Option<String>)>,
    hop_gen: i32,
    src_changed: Option<SystemTime>,
    origin_lsn: Option<PgLsn>,
}

/// One page's work on one ledger target ([`apply_ledger_target`]).
#[derive(Debug, Clone)]
pub(crate) struct LedgerTargetPlan {
    /// The target's qualified identity (`transform_definitions.target_table`).
    pub(crate) target: String,
    target_ident: String,
    ledger_ident: String,
    /// The source's qualified identity, for the Re-derive read.
    source_table: String,
    source_pk: Vec<PrimaryKeyColumn>,
    /// The target's row identity (its `GROUP BY` columns), which renders a
    /// group's key for the seam exactly as a chained reader keys it.
    identity: Vec<PrimaryKeyColumn>,
    shape: LedgerShape,
    records: Vec<LedgerRecord>,
}

impl LedgerTargetPlan {
    pub(crate) fn new(
        target: &str,
        source_table: &str,
        source_pk: Vec<PrimaryKeyColumn>,
        identity: Vec<PrimaryKeyColumn>,
        shape: LedgerShape,
    ) -> Self {
        Self {
            target: target.to_string(),
            target_ident: ddl::qualified_target_table_ident(target),
            ledger_ident: ddl::qualified_target_table_ident(&schema::ledger_table_name(target)),
            source_table: source_table.to_string(),
            source_pk,
            identity,
            shape,
            records: Vec::new(),
        }
    }

    /// Adds a Re-derive of source key `key`, with no provenance (the orphan
    /// sweep's, `intake::resume_orphans`).
    pub(crate) fn push_rederive(&mut self, key: String) {
        self.records.push(LedgerRecord {
            key,
            apply: None,
            hop_gen: 0,
            src_changed: None,
            origin_lsn: None,
        });
    }

    /// Adds one folded record: an Apply of its last change, or a Re-derive
    /// when it was staged as (or folded with) a `recompute`, or has no change
    /// to apply.
    pub(crate) fn push(&mut self, change: &FoldedChange) {
        let apply = match &change.last_change {
            Some(last) if !change.has_recompute => {
                Some((last.lsn, last.row_txid.clone(), change.new_image.clone()))
            }
            _ => None,
        };
        self.records.push(LedgerRecord {
            key: change.key.clone(),
            apply,
            hop_gen: change.hop_gen,
            src_changed: change.src_changed,
            origin_lsn: change.origin_lsn,
        });
    }
}

/// ADR-0002 I2 as SQL over the ledger entry `l`, the batch row `v` (its
/// `lsn` and `txid`) and the target's truncate floor `fl`: whether an Apply
/// changes the entry. `visibility` false renders the `lsn_only_skip` plant.
fn apply_predicate(visibility: bool) -> String {
    let basis = quote_ident(schema::BASIS_COLUMN);
    let applied = quote_ident(schema::APPLIED_LSN_COLUMN);
    let seen = if visibility {
        format!("(l.{basis} is null or not pg_visible_in_snapshot(v.__txid, l.{basis})) and ")
    } else {
        String::new()
    };
    format!(
        "{seen}(l.{applied} is null or v.__lsn > l.{applied}) \
         and not exists (select 1 from fl where v.__lsn <= fl.floor)"
    )
}

/// The page's one ledger-and-groups statement (see the module doc, step 3).
/// Binds `$1` keys, `$2` Re-derive flags, `$3` `lsn`s, `$4` `row_txid`s,
/// `$5` images (all `text[]`, in key order), `$6` the Re-derive read's
/// snapshot, `$7` the page's segment and `$8` the target's identity.
/// `image_columns` are the seam's prior-image columns (`None`: no reader).
fn ledger_statement(
    plan: &LedgerTargetPlan,
    image_columns: Option<&[String]>,
    visibility: bool,
    drop_racing: bool,
) -> String {
    let shape = &plan.shape;
    let ledger = &plan.ledger_ident;
    let target = &plan.target_ident;
    let q = |c: &str| quote_ident(c);
    let groups: Vec<String> = shape.group_cols.iter().map(|c| q(c)).collect();
    let args: Vec<String> = shape.contribs.iter().map(|c| q(&c.column)).collect();
    let values: Vec<&String> = groups.iter().chain(&args).collect();
    let cols = |alias: &str| -> String {
        values
            .iter()
            .map(|c| format!("{alias}.{c}"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let member = q(schema::MEMBER_COLUMN);
    let tombstone = q(schema::TOMBSTONE_COLUMN);
    let key = q(schema::KEY_COLUMN);
    let basis = q(schema::BASIS_COLUMN);
    let applied = q(schema::APPLIED_LSN_COLUMN);
    let seg = q(schema::APPLIED_SEG_COLUMN);
    let members = q(ddl::MEMBERS_COLUMN);

    // An image's values keyed by ledger column, for `jsonb_populate_record`.
    let doc: Vec<String> = shape
        .group_cols
        .iter()
        .map(|c| (c, c))
        .chain(shape.contribs.iter().map(|c| (&c.column, &c.source)))
        .map(|(ledger_col, source_col)| {
            format!(
                "{}, b.__img -> {}",
                quote_literal(ledger_col),
                quote_literal(source_col)
            )
        })
        .collect();
    let set_values: Vec<String> = values.iter().map(|c| format!("{c} = v.{c}")).collect();

    // Per-group increments: `dm` members, and per argument `dc<i>` (its
    // non-null count) and, for a summed argument, `ds<i>` (its sum).
    let mut deltas = vec!["sum(m.__sign) as __dm".to_string()];
    let mut nonzero = vec!["sum(m.__sign) <> 0".to_string()];
    for (i, a) in args.iter().enumerate() {
        deltas.push(format!(
            "sum(case when m.{a} is null then 0 else m.__sign end) as __dc{i}"
        ));
        nonzero.push(format!(
            "sum(case when m.{a} is null then 0 else m.__sign end) <> 0"
        ));
        if !shape.contribs[i].summed {
            continue;
        }
        // Summed per sign and then subtracted, not `sum(__sign * a)`: the
        // product is computed in the argument's own type, so negating an
        // `integer`'s -2147483648 (or a `bigint`'s minimum) overflows it.
        // `sum` widens first (`integer` to `bigint`, `bigint` to `numeric`).
        let ds = format!(
            "(coalesce(sum(m.{a}) filter (where m.__sign > 0), 0) \
             - coalesce(sum(m.{a}) filter (where m.__sign < 0), 0))"
        );
        deltas.push(format!("{ds} as __ds{i}"));
        nonzero.push(format!("{ds} <> 0"));
    }
    let group_list = |alias: &str| -> String {
        groups
            .iter()
            .map(|c| format!("{alias}.{c}"))
            .collect::<Vec<_>>()
            .join(", ")
    };

    // The upsert's columns, inserted values, and conflict updates; and each
    // maintained column's prior value over the upsert's result `up` and the
    // increments `d`.
    let mut insert_cols = groups.clone();
    let mut insert_vals: Vec<String> = groups.iter().map(|c| format!("d.{c}")).collect();
    let mut updates = Vec::new();
    let mut priors: HashMap<String, String> = HashMap::new();
    for field in &shape.fields {
        match field {
            LedgerField::Sum { column, count, arg } => {
                let (f, c) = (q(column), q(count));
                insert_cols.push(f.clone());
                insert_vals.push(format!(
                    "case when d.__dc{arg} = 0 and d.__ds{arg} = 0 then null else d.__ds{arg} end"
                ));
                updates.push(format!(
                    "{f} = case when t.{c} + excluded.{c} = 0 then null \
                     else coalesce(t.{f}, 0) + coalesce(excluded.{f}, 0) end"
                ));
                priors.insert(
                    column.clone(),
                    format!(
                        "case when up.{c} - d.__dc{arg} = 0 then null \
                         else coalesce(up.{f}, 0) - d.__ds{arg} end"
                    ),
                );
            }
            LedgerField::Avg {
                column,
                sum,
                count,
                arg,
            } => {
                // The running sum as `SUM`'s, and the visible column
                // `sum / count::numeric`: Postgres's `avg()` over an exact
                // numeric argument is `numeric_div` of the same two, and the
                // one-pass build writes the same expression.
                let (f, s, c) = (q(column), q(sum), q(count));
                let new_sum = format!("coalesce(t.{s}, 0) + coalesce(excluded.{s}, 0)");
                let new_count = format!("t.{c} + excluded.{c}");
                insert_cols.push(s.clone());
                insert_vals.push(format!(
                    "case when d.__dc{arg} = 0 and d.__ds{arg} = 0 then null else d.__ds{arg} end"
                ));
                insert_cols.push(f.clone());
                insert_vals.push(format!(
                    "case when d.__dc{arg} = 0 then null \
                     else d.__ds{arg} / d.__dc{arg}::numeric end"
                ));
                updates.push(format!(
                    "{s} = case when {new_count} = 0 then null else {new_sum} end"
                ));
                updates.push(format!(
                    "{f} = case when {new_count} = 0 then null \
                     else ({new_sum}) / ({new_count})::numeric end"
                ));
                let (old_sum, old_count) = (
                    format!("coalesce(up.{s}, 0) - d.__ds{arg}"),
                    format!("up.{c} - d.__dc{arg}"),
                );
                priors.insert(
                    sum.clone(),
                    format!("case when {old_count} = 0 then null else {old_sum} end"),
                );
                priors.insert(
                    column.clone(),
                    format!(
                        "case when {old_count} = 0 then null \
                         else ({old_sum}) / ({old_count})::numeric end"
                    ),
                );
            }
            LedgerField::CountStar { column } => {
                let f = q(column);
                insert_cols.push(f.clone());
                insert_vals.push("d.__dm".to_string());
                updates.push(format!("{f} = t.{f} + excluded.{f}"));
                priors.insert(column.clone(), format!("up.{f} - d.__dm"));
            }
            LedgerField::CountArg { column, arg } => {
                let f = q(column);
                insert_cols.push(f.clone());
                insert_vals.push(format!("d.__dc{arg}"));
                updates.push(format!("{f} = t.{f} + excluded.{f}"));
                priors.insert(column.clone(), format!("up.{f} - d.__dc{arg}"));
            }
        }
    }
    // A `SUM`'s or `AVG`'s hidden count, once per shared column (issue #48).
    let mut counts_emitted: Vec<&str> = Vec::new();
    for field in &shape.fields {
        let (LedgerField::Sum { count, arg, .. } | LedgerField::Avg { count, arg, .. }) = field
        else {
            continue;
        };
        if counts_emitted.contains(&count.as_str()) {
            continue;
        }
        counts_emitted.push(count);
        let c = q(count);
        insert_cols.push(c.clone());
        insert_vals.push(format!("d.__dc{arg}"));
        updates.push(format!("{c} = t.{c} + excluded.{c}"));
        priors.insert(count.clone(), format!("up.{c} - d.__dc{arg}"));
    }
    insert_cols.push(members.clone());
    insert_vals.push("d.__dm".to_string());
    updates.push(format!("{members} = t.{members} + excluded.{members}"));
    priors.insert(
        ddl::MEMBERS_COLUMN.to_string(),
        format!("up.{members} - d.__dm"),
    );

    let prior_image = match image_columns {
        Some(columns) => {
            let pairs: Vec<String> = columns
                .iter()
                .map(|c| {
                    let value = priors
                        .get(c)
                        .cloned()
                        .unwrap_or_else(|| format!("up.{}", q(c)));
                    format!("{}, ({value})::text", quote_literal(c))
                })
                .collect();
            format!("jsonb_build_object({})::text", pairs.join(", "))
        }
        None => "null::text".to_string(),
    };
    // Planted bug (#557): drop the increments of a group another apply
    // transaction holds. See `crate::plant`.
    let racing = if drop_racing {
        " where pg_try_advisory_xact_lock(hashtext($8), hashtext(d.__gk))"
    } else {
        ""
    };

    format!(
        "with b as ( \
             select u.__k, u.__rederive, u.__lsn::pg_lsn as __lsn, u.__txid::xid8 as __txid, \
                    u.__img::jsonb as __img \
             from unnest($1::text[], $2::bool[], $3::text[], $4::text[], $5::text[]) \
                  as u(__k, __rederive, __lsn, __txid, __img) \
         ), \
         v as ( \
             select b.__k, b.__rederive, b.__lsn, b.__txid, b.__img is not null as __present, {r_cols} \
             from b cross join lateral \
                  jsonb_populate_record(null::{ledger}, jsonb_build_object({doc})) r \
         ), \
         old as ( \
             select l.{key} as __k, {l_cols}, l.{member} and not l.{tombstone} as __live \
             from {ledger} l where l.{key} = any($1::text[]) \
         ), \
         fl as (select floor from ledger_truncate_floor where target_table = $8), \
         upd as ( \
             update {ledger} l set {set_values}, \
                 {member} = v.__present, {tombstone} = not v.__present, \
                 {basis} = case when v.__rederive then $6::text::pg_snapshot else l.{basis} end, \
                 {applied} = case when v.__rederive then l.{applied} else v.__lsn end, \
                 {seg} = greatest(l.{seg}, $7::bigint) \
             from v \
             where l.{key} = v.__k and (v.__rederive or ({predicate})) \
             returning l.{key} as __k, {l_cols}, l.{member} and not l.{tombstone} as __live \
         ), \
         moves as ( \
             select {u_cols}, 1 as __sign, u.__k from upd u where u.__live \
             union all \
             select {o_cols}, -1 as __sign, o.__k from old o join upd u on u.__k = o.__k where o.__live \
         ), \
         d as ( \
             select {m_groups}, {m_gk} as __gk, {deltas}, array_agg(m.__k) as __ks \
             from moves m group by {m_groups} \
             having {nonzero} \
         ), \
         up as ( \
             insert into {target} as t ({insert_cols}) \
             select {insert_vals} from d{racing} order by {d_groups} \
             on conflict ({group_cols}) do update set {updates} \
             returning t.*, {t_gk} as __trellis_gk, (t.xmax = 0) as __trellis_inserted, \
                       t.ctid::text as __trellis_ctid \
         ) \
         select up.__trellis_gk, up.__trellis_inserted, up.__trellis_ctid, \
                up.{members}, d.__ks, {prior_image} \
         from up join d on {up_d}",
        r_cols = cols("r"),
        l_cols = cols("l"),
        u_cols = cols("u"),
        o_cols = cols("o"),
        doc = doc.join(", "),
        set_values = set_values.join(", "),
        predicate = apply_predicate(visibility),
        m_groups = group_list("m"),
        m_gk = ddl::pk_key_sql_expr(&plan.identity, Some("m")),
        deltas = deltas.join(", "),
        nonzero = nonzero.join(" or "),
        insert_cols = insert_cols.join(", "),
        insert_vals = insert_vals.join(", "),
        d_groups = group_list("d"),
        group_cols = groups.join(", "),
        updates = updates.join(", "),
        t_gk = ddl::pk_key_sql_expr(&plan.identity, Some("t")),
        // Each upserted group back to its increments by the group columns'
        // own equality, not by their text: equal values can render
        // differently (`numeric` `1.5` and `1.50`, `float` `0` and `-0`), and
        // the target row keeps whichever spelling created it. A one-element
        // array compares `NULL` equal to `NULL` (the `nulls not distinct`
        // key) and still hashes.
        up_d = groups
            .iter()
            .map(|c| format!("array[up.{c}] = array[d.{c}]"))
            .collect::<Vec<_>>()
            .join(" and "),
    )
}

/// One group the page wrote, from [`ledger_statement`]'s result.
struct WrittenGroup {
    key: String,
    inserted: bool,
    ctid: String,
    members: i64,
    keys: Vec<String>,
    prior: Option<String>,
}

/// Applies one page's records to one ledger target, in the page's
/// transaction. See the module doc. Returns the groups written and deleted.
#[tracing::instrument(
    name = "staging.apply_ledger_target",
    skip(txn, plan, mutations),
    fields(transform = %plan.target, records = plan.records.len())
)]
pub(crate) async fn apply_ledger_target(
    txn: &Transaction<'_>,
    plan: &LedgerTargetPlan,
    seg_seq: i64,
    mutations: &mut TargetMutations,
) -> Result<(usize, usize), ApplyError> {
    if plan.records.is_empty() {
        return Ok((0, 0));
    }
    let mut records: Vec<&LedgerRecord> = plan.records.iter().collect();
    records.sort_by(|a, b| a.key.cmp(&b.key));
    let keys: Vec<&str> = records.iter().map(|r| r.key.as_str()).collect();
    let ledger = &plan.ledger_ident;
    let key_col = quote_ident(schema::KEY_COLUMN);

    // 1. The entry lock (I1, I5): placeholders for new keys, then every
    // entry, sorted, in one statement.
    txn.execute(
        &format!(
            "insert into {ledger} ({key_col}, {}) \
             select k, false from unnest($1::text[]) as k order by k \
             on conflict do nothing",
            quote_ident(schema::MEMBER_COLUMN)
        ),
        &[&keys],
    )
    .await?;
    // Planted bug (#557): read without the entry lock. See `crate::plant`.
    #[cfg(any(test, feature = "test-util"))]
    let skip_lock = crate::plant::fires(crate::plant::Plant::SkipLedgerLock, true);
    #[cfg(not(any(test, feature = "test-util")))]
    let skip_lock = false;
    if !skip_lock {
        txn.execute(
            &format!(
                "select 1 from {ledger} where {key_col} = any($1::text[]) \
                 order by {key_col} for update"
            ),
            &[&keys],
        )
        .await?;
    }
    // Test-only pause point (#623 D1). See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterEntryLock,
        &plan.target,
    )
    .await?;

    // 2. The Re-derive read: its rows and its snapshot in one statement.
    let rederive: Vec<&str> = records
        .iter()
        .filter(|r| r.apply.is_none())
        .map(|r| r.key.as_str())
        .collect();
    let mut read_images: HashMap<String, String> = HashMap::new();
    let mut snapshot: Option<String> = None;
    if !rederive.is_empty() {
        let mut columns: Vec<String> = plan.shape.group_cols.clone();
        for contrib in &plan.shape.contribs {
            if !columns.contains(&contrib.source) {
                columns.push(contrib.source.clone());
            }
        }
        let query = super::apply::live_rows_query(
            &plan.source_table,
            &plan.source_pk,
            &columns,
            &rederive,
        )?;
        let sql = format!(
            "select null::text, pg_catalog.pg_current_snapshot()::text \
             union all select m.k, m.doc::text from ({}) m",
            query.docs
        );
        for row in txn.query(&sql, &query.params()).await? {
            let key: Option<String> = row.get(0);
            let value: String = row.get(1);
            match key {
                None => snapshot = Some(value),
                Some(key) => {
                    read_images.insert(key, value);
                }
            }
        }
        // Test-only pause point (#623 D1), directly after the one
        // read-and-snapshot statement: the chunked-read test depends on
        // nothing committing between the two. See `super::interleave`.
        #[cfg(any(test, feature = "test-util"))]
        super::interleave::pause_at(
            txn,
            super::interleave::PausePoint::AfterRederiveRead,
            &plan.target,
        )
        .await?;
    }

    // 3. The entries and the groups, in one statement.
    let mut flags = Vec::with_capacity(records.len());
    let mut lsns: Vec<Option<String>> = Vec::with_capacity(records.len());
    let mut txids: Vec<Option<&str>> = Vec::with_capacity(records.len());
    let mut images: Vec<Option<&str>> = Vec::with_capacity(records.len());
    for record in &records {
        match &record.apply {
            Some((lsn, txid, image)) => {
                flags.push(false);
                lsns.push(Some(lsn.to_string()));
                txids.push(Some(txid.as_str()));
                images.push(image.as_deref());
            }
            None => {
                flags.push(true);
                lsns.push(None);
                txids.push(None);
                images.push(read_images.get(&record.key).map(String::as_str));
            }
        }
    }
    let image_columns = mutations.image_columns(txn, &plan.target).await?;
    #[cfg(any(test, feature = "test-util"))]
    let (visibility, drop_racing) = (
        !crate::plant::fires(crate::plant::Plant::LsnOnlySkip, !flags.iter().all(|f| *f)),
        crate::plant::fires(crate::plant::Plant::DropRacingGroupDelta, true),
    );
    #[cfg(not(any(test, feature = "test-util")))]
    let (visibility, drop_racing) = (true, false);
    let sql = ledger_statement(plan, image_columns.as_deref(), visibility, drop_racing);
    let rows = txn
        .query(
            &sql,
            &[
                &keys,
                &flags,
                &lsns,
                &txids,
                &images,
                &snapshot,
                &seg_seq,
                &plan.target,
            ],
        )
        .await?;
    let groups: Vec<WrittenGroup> = rows
        .iter()
        .map(|row| WrittenGroup {
            key: row.get(0),
            inserted: row.get(1),
            ctid: row.get(2),
            members: row.get(3),
            keys: row.get(4),
            prior: row.get(5),
        })
        .collect();
    // Test-only pause point (#623 D1). See `super::interleave`.
    #[cfg(any(test, feature = "test-util"))]
    super::interleave::pause_at(
        txn,
        super::interleave::PausePoint::AfterGroupUpsert,
        &plan.target,
    )
    .await?;

    // 4. Emptied groups go. Every one is locked by the upsert already.
    let emptied: Vec<&str> = groups
        .iter()
        .filter(|g| g.members <= 0)
        .map(|g| g.ctid.as_str())
        .collect();
    if !emptied.is_empty() {
        txn.execute(
            &format!(
                "delete from {} where ctid = any($1::text[]::tid[])",
                plan.target_ident
            ),
            &[&emptied],
        )
        .await?;
    }

    // The seam: each group with the provenance of the records that moved it.
    let by_key: HashMap<&str, &LedgerRecord> =
        records.iter().map(|r| (r.key.as_str(), *r)).collect();
    let (mut written, mut deleted) = (0, 0);
    for group in groups {
        let deleting = group.members <= 0;
        if deleting && group.inserted {
            continue;
        }
        if deleting {
            deleted += 1;
        } else {
            written += 1;
        }
        let mut hop_gen = 0;
        let mut src_changed: Option<SystemTime> = None;
        let mut origin: Option<Option<PgLsn>> = None;
        for key in &group.keys {
            let record = by_key[key.as_str()];
            hop_gen = hop_gen.max(record.hop_gen);
            src_changed = super::apply::earliest_src_changed(src_changed, record.src_changed);
            origin = Some(match origin {
                None => record.origin_lsn,
                Some(o) => earliest_origin(o, record.origin_lsn),
            });
        }
        let prior = if group.inserted { None } else { group.prior };
        mutations.record(
            &plan.target,
            group.key,
            prior,
            hop_gen,
            src_changed,
            origin.flatten(),
        );
    }
    Ok((written, deleted))
}

/// Empties a ledger target for a source `TRUNCATE` (the D split's Q6) and
/// raises its truncate floor to `lsn`, the truncate's ring `lsn`. The caller
/// clears the group rows.
pub(super) async fn truncate_ledger(
    txn: &Transaction<'_>,
    target: &str,
    lsn: Option<PgLsn>,
) -> Result<(), ApplyError> {
    txn.batch_execute(&format!(
        "truncate {}",
        ddl::qualified_target_table_ident(&schema::ledger_table_name(target))
    ))
    .await?;
    if let Some(lsn) = lsn {
        txn.execute(
            "insert into ledger_truncate_floor (target_table, floor) values ($1, $2) \
             on conflict (target_table) do update \
             set floor = greatest(ledger_truncate_floor.floor, excluded.floor)",
            &[&target, &lsn],
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defs::parse;

    fn shape(text: &str, columns: &[(&str, ValueType)]) -> Option<LedgerShape> {
        let def = parse(text).expect("parse");
        let source_columns: HashMap<String, ValueType> = columns
            .iter()
            .map(|(c, t)| ((*c).to_string(), *t))
            .collect();
        route(&def, &source_columns)
    }

    const INT: ValueType = ValueType::Integer(crate::integer::IntWidth::Int4);

    #[test]
    fn plain_sum_and_count_targets_route_to_the_ledger() {
        let s = shape(
            "TRANSFORM t FROM s GROUP BY g SELECT g AS g, SUM(v) AS total, COUNT(*) AS n, \
             COUNT(v) AS nv",
            &[("g", ValueType::Text), ("v", ValueType::Numeric)],
        )
        .expect("routed");
        assert_eq!(s.group_cols, vec!["g"]);
        assert_eq!(
            s.contribs,
            vec![Contrib {
                column: "__arg0".to_string(),
                source: "v".to_string(),
                summed: true
            }]
        );
        assert_eq!(
            s.fields,
            vec![
                LedgerField::Sum {
                    column: "total".to_string(),
                    count: "__total_count".to_string(),
                    arg: 0
                },
                LedgerField::CountStar {
                    column: "n".to_string()
                },
                LedgerField::CountArg {
                    column: "nv".to_string(),
                    arg: 0
                },
            ]
        );
        assert!(
            shape(
                "TRANSFORM t FROM s GROUP BY g SELECT SUM(v) AS x",
                &[("g", INT), ("v", INT)]
            )
            .is_some()
        );
    }

    /// #623 D3b: `AVG` over an exact numeric column shares `SUM`'s hidden
    /// count, and `COUNT(x)` takes a column of any type but `json`/`jsonb`,
    /// whose contribution gets no sum.
    #[test]
    fn avg_and_count_of_any_column_route_to_the_ledger() {
        let s = shape(
            "TRANSFORM t FROM s GROUP BY g SELECT SUM(v) AS total, AVG(v) AS mean, \
             COUNT(name) AS named",
            &[
                ("g", INT),
                ("v", ValueType::Integer(crate::integer::IntWidth::Int8)),
                ("name", ValueType::Text),
            ],
        )
        .expect("routed");
        assert_eq!(
            s.contribs,
            vec![
                Contrib {
                    column: "__arg0".to_string(),
                    source: "v".to_string(),
                    summed: true
                },
                Contrib {
                    column: "__arg1".to_string(),
                    source: "name".to_string(),
                    summed: false
                },
            ]
        );
        assert_eq!(
            s.fields[1],
            LedgerField::Avg {
                column: "mean".to_string(),
                sum: "__mean_sum".to_string(),
                count: "__total_count".to_string(),
                arg: 0
            }
        );
        assert_eq!(
            s.fields[2],
            LedgerField::CountArg {
                column: "named".to_string(),
                arg: 1
            }
        );
    }

    #[test]
    fn every_other_aggregate_stays_on_the_old_path() {
        let cols = &[
            ("g", INT),
            ("v", ValueType::Numeric),
            ("f", ValueType::Float(crate::float::FloatWidth::Float8)),
            ("name", ValueType::Text),
            ("doc", ValueType::Other(PgType::Jsonb)),
        ];
        for text in [
            "TRANSFORM t FROM s GROUP BY g SELECT MIN(v) AS x",
            "TRANSFORM t FROM s GROUP BY g SELECT SUM(f) AS x",
            "TRANSFORM t FROM s GROUP BY g SELECT AVG(f) AS x",
            "TRANSFORM t FROM s GROUP BY g SELECT SUM(v) AS x, SUM(name) AS y",
            "TRANSFORM t FROM s GROUP BY g SELECT COUNT(doc) AS x",
            "TRANSFORM t FROM s GROUP BY g SELECT AVG(v + 1) AS x",
            "TRANSFORM t FROM s GROUP BY g SELECT COUNT(v + 1) AS x",
            "TRANSFORM t FROM s GROUP BY g SELECT SUM(v) AS t, COUNT(*) AS n, t + n AS x",
            "TRANSFORM t FROM s GROUP BY g SELECT SUM(v + 1) AS x",
            "TRANSFORM t FROM s GROUP BY g SELECT SUM(v) + COUNT(*) AS x",
            "TRANSFORM t FROM s SELECT v AS x",
        ] {
            assert_eq!(shape(text, cols), None, "{text}");
        }
    }

    /// ADR-0002 I2 evaluated by Postgres: whether an Apply of a change
    /// (`txid`, `lsn`) changes an entry (`basis`, `applied_lsn`) under the
    /// truncate `floor`.
    async fn applies(
        client: &tokio_postgres::Client,
        basis: Option<&str>,
        applied_lsn: Option<&str>,
        (txid, lsn): (&str, &str),
        floor: Option<&str>,
        visibility: bool,
    ) -> bool {
        client
            .query_one(
                &format!(
                    "with l as (select $1::text::pg_snapshot as \"__basis\", \
                                       $2::text::pg_lsn as \"__applied_lsn\"), \
                          v as (select $3::text::xid8 as __txid, $4::text::pg_lsn as __lsn), \
                          fl as (select $5::text::pg_lsn as floor where $5::text is not null) \
                     select {} from l, v",
                    apply_predicate(visibility)
                ),
                &[&basis, &applied_lsn, &txid, &lsn, &floor],
            )
            .await
            .expect("evaluate the apply predicate")
            .get(0)
    }

    #[tokio::test]
    async fn the_apply_predicate_skips_what_the_basis_saw_older_lsns_and_the_truncated() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(connection);
        let c = &client;
        // A basis taken with 102 in flight: 99 and 103 had committed, 106
        // hadn't started.
        let basis = Some("100:105:102");
        let change = |txid| (txid, "0/200");

        // A never-applied entry (a placeholder) takes any change.
        assert!(applies(c, None, None, change("99"), None, true).await);
        // Visible in the basis: the read already counted it.
        assert!(!applies(c, basis, None, change("99"), None, true).await);
        assert!(!applies(c, basis, None, change("103"), None, true).await);
        // In flight when the basis was taken (`xip`), or after it: applied.
        assert!(applies(c, basis, None, change("102"), None, true).await);
        assert!(applies(c, basis, None, change("106"), None, true).await);
        // `applied_lsn`: only a strictly newer change applies.
        assert!(!applies(c, None, Some("0/200"), change("106"), None, true).await);
        assert!(!applies(c, None, Some("0/300"), change("106"), None, true).await);
        assert!(applies(c, None, Some("0/100"), change("106"), None, true).await);
        // The truncate floor: a change at or below it predates the truncate.
        assert!(!applies(c, None, None, change("106"), Some("0/200"), true).await);
        assert!(applies(c, None, None, change("106"), Some("0/1FF"), true).await);
        // The `lsn_only_skip` plant drops the visibility term.
        assert!(applies(c, basis, None, change("99"), None, false).await);
    }

    #[test]
    fn the_apply_predicate_is_visibility_then_lsn_then_the_floor() {
        assert_eq!(
            apply_predicate(true),
            r#"(l."__basis" is null or not pg_visible_in_snapshot(v.__txid, l."__basis")) and (l."__applied_lsn" is null or v.__lsn > l."__applied_lsn") and not exists (select 1 from fl where v.__lsn <= fl.floor)"#
        );
        assert!(!apply_predicate(false).contains("pg_visible_in_snapshot"));
    }
}
