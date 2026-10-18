//! `self_check` of an aggregate target on the group basis (#1104, ADR-0013
//! "Scope", `super::group_basis`).
//!
//! One page is a keyset page of groups, by the group key text
//! (`ddl::pk_key_sql_expr` over the `GROUP BY` columns, under byte order).
//! One statement ([`page_sql`]) reads, under one snapshot, four sides of the
//! page:
//!
//! 1. the source's partials: a `GROUP BY` of the source, a group's row
//!    count, each argument's non-null count and, for a summed argument, its
//!    sum;
//! 2. the hidden table's partials, the same values;
//! 3. the visible row each hidden group's partials imply (a row exactly
//!    while `__n > 0`, each field the function of the partials the
//!    definition names);
//! 4. the visible target's rows.
//!
//! [`diff_groups`] compares 1 against 2 and 3 against 4, and reports a group
//! 1 has and 4 lacks, or the reverse, as a missing or extra row. It reports, never
//! repairs: the remedy is a `request_backfill` of the source or a
//! `PAUSE`/`RESUME` (ADR-0013). A group's partials are compared as values (a
//! sum's trailing zeros are not part of it), the visible row byte for byte.
//!
//! Like the 1-1 comparison, the page is read only after the convergence wait
//! `super::self_check::audit` makes, and a divergence is reported only if it
//! reproduces after a second one, so a group a page is still applying is
//! never reported. The hidden basis and stamp are not read: after the wait
//! every ring row the target's pages hold is applied, whichever basis a
//! group's row carries.
//!
//! The SQL here is rendered from the definition separately from the apply
//! path's (`group_basis::GroupTargetPlan`), so a rendering bug there
//! disagrees with this oracle instead of agreeing with itself.
//!
//! Cost: the source side is a scan of the source per page, since the group
//! key is an expression no index orders. Its `GROUP BY` takes only the
//! groups up to the hidden side's last key, so it aggregates about a page
//! of groups, not every group past the cursor. A walk of G groups of a
//! source of N rows, L groups a page, reads about G / L times N source rows,
//! as a 1-1 walk over an integer key reads N / L times N.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use tokio_postgres::types::PgLsn;

use crate::defs::ast::{Expr, Predicate};
use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::defs::model::Definition;
use crate::pool::{Pool, quote_ident};

use super::group_basis::{self, Agg, GroupShape};
use super::self_check::{
    BYTE_ORDER, ComparePass, Divergence, SelfCheckError, SelfCheckScope, render_leaf,
};

/// Refuses a definition with an argument or field [`render_leaf`] can't
/// render.
pub(super) fn check_renders(def: &Definition, shape: &GroupShape) -> Result<(), SelfCheckError> {
    let exprs = shape
        .args
        .iter()
        .map(|a| &a.expr)
        .chain(shape.fields.iter().map(|(_, _, e)| e));
    for expr in exprs {
        render_leaf(expr).map_err(|detail| SelfCheckError::UnsupportedExpr {
            target: def.def.target.clone(),
            detail,
        })?;
    }
    Ok(())
}

/// The partials' columns in the order every side lists them: the row count,
/// then per argument its count and, if summed, its sum. Names are the
/// hidden table's.
fn partial_columns(shape: &GroupShape) -> Vec<String> {
    let mut names = vec!["__n".to_string()];
    for (i, arg) in shape.args.iter().enumerate() {
        names.push(format!("__count_{}", i + 1));
        if arg.summed {
            names.push(format!("__sum_{}", i + 1));
        }
    }
    names
}

/// A partial as the text both sides compare: counts as they are, a sum
/// without trailing zeros.
pub(super) fn partial_text(column: &str, expr: &str) -> String {
    if column.starts_with("__sum_") {
        format!("pg_catalog.trim_scale(({expr}))::text")
    } else {
        format!("({expr})::text")
    }
}

/// The aggregate `agg` of a group over its hidden partials' columns.
fn agg_sql(agg: &Agg) -> String {
    match agg {
        Agg::CountStar => "__n".to_string(),
        Agg::Count(i) => format!("__count_{}", i + 1),
        Agg::Sum(i, cast) => format!(
            "case when __count_{n} = 0 then null else (__sum_{n})::{cast} end",
            n = i + 1
        ),
        Agg::Avg(i) => format!(
            "case when __count_{n} = 0 then null else (__sum_{n}) / (__count_{n})::numeric end",
            n = i + 1
        ),
    }
}

/// The statement of one page ([`compare`]). `$1` is the cursor, `$2` the
/// limit; each row is `(side, page_end, key, values)` for the sides in the
/// module doc, numbered from 1, and `page_end` is the page's bound, the
/// lowest last key of the sides that filled the limit (as the 1-1 page's).
fn page_sql(
    shape: &GroupShape,
    identity: &[PrimaryKeyColumn],
    source: &str,
    hidden: &str,
    target: &str,
) -> Result<String, SelfCheckError> {
    let render = |expr: &Expr| {
        render_leaf(expr).map_err(|detail| SelfCheckError::UnsupportedExpr {
            target: target.to_string(),
            detail,
        })
    };
    let key = format!(
        "({}) collate {BYTE_ORDER}",
        ddl::pk_key_sql_expr(identity, None)
    );
    let after = format!("($1::text is null or {key} > $1)");
    let group_cols = shape
        .group_cols
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");

    // Side 1: the source's partials per group.
    let mut src_parts = vec![partial_text("__n", "count(*)")];
    for arg in &shape.args {
        let rendered = render(&arg.expr)?;
        src_parts.push(partial_text("__count", &format!("count({rendered})")));
        if arg.summed {
            src_parts.push(partial_text(
                "__sum_",
                &format!("coalesce(sum(coalesce({rendered}, 0)::numeric), 0)"),
            ));
        }
    }
    // Only the groups up to the hidden side's last key, when it filled the
    // limit: the page ends there or earlier, so a group past it would be
    // trimmed anyway, and this way the `GROUP BY` holds a page of groups
    // rather than every group past the cursor.
    let src = format!(
        "select {key} as __k, array[{}]::text[] as __v from {source} \
         where {after} and ((select e from hid_end) is null or {key} <= (select e from hid_end)) \
         group by {group_cols} order by __k limit $2",
        src_parts.join(", ")
    );

    // Sides 2 and 3: the hidden table's partials, and the visible row they
    // imply.
    let columns = partial_columns(shape);
    let hid_parts: Vec<String> = columns
        .iter()
        .map(|c| partial_text(c, &quote_ident(c)))
        .collect();
    let aggs: Vec<String> = shape
        .aggs
        .iter()
        .enumerate()
        .map(|(k, agg)| {
            format!(
                "{} as {}",
                agg_sql(agg),
                quote_ident(&group_basis::placeholder(k))
            )
        })
        .collect();
    let mut implied = Vec::with_capacity(shape.fields.len());
    for (_, pg_type, expr) in &shape.fields {
        implied.push(format!("(({})::{pg_type})::text", render(expr)?));
    }
    let lateral = if aggs.is_empty() {
        String::new()
    } else {
        format!("cross join lateral (select {}) as __a", aggs.join(", "))
    };
    let hid = format!(
        "select {key} as __k, array[{}]::text[] as __v, \
                array[{}]::text[] as __f, __n > 0 as __visible \
         from {hidden} {lateral} where {after} order by __k limit $2",
        hid_parts.join(", "),
        implied.join(", ")
    );

    // Side 4: the visible rows.
    let visible: Vec<String> = shape
        .fields
        .iter()
        .map(|(name, _, _)| format!("{}::text", quote_ident(name)))
        .collect();
    let vis = format!(
        "select {key} as __k, array[{}]::text[] as __v from {target} \
         where {after} order by __k limit $2",
        visible.join(", ")
    );

    Ok(format!(
        "with hid as materialized ({hid}), \
              hid_end as (select max(__k) as e from hid having count(*) >= $2), \
              src as materialized ({src}), \
              vis as materialized ({vis}), \
              page as ( \
                select least( \
                  (select max(__k) from src having count(*) >= $2), \
                  (select e from hid_end), \
                  (select max(__k) from vis having count(*) >= $2)) as page_end) \
         select 1, page.page_end, src.__k, src.__v from src, page \
          where page.page_end is null or src.__k <= page.page_end \
         union all \
         select 2, page.page_end, hid.__k, hid.__v from hid, page \
          where page.page_end is null or hid.__k <= page.page_end \
         union all \
         select 3, page.page_end, hid.__k, hid.__f from hid, page \
          where hid.__visible and (page.page_end is null or hid.__k <= page.page_end) \
         union all \
         select 4, page.page_end, vis.__k, vis.__v from vis, page \
          where page.page_end is null or vis.__k <= page.page_end"
    ))
}

pub(super) type Side = BTreeMap<String, Vec<Option<String>>>;

/// One page of the comparison of a group-basis target: [`page_sql`]'s rows,
/// diffed.
pub(super) async fn compare(
    pool: &Pool,
    def: &Definition,
    shape: &GroupShape,
    identity: &[PrimaryKeyColumn],
    scope: &SelfCheckScope,
    paused: &HashSet<String>,
    checked_through: PgLsn,
) -> Result<ComparePass, SelfCheckError> {
    // The source's `GROUP BY` has no `WHERE` for the definition's predicate,
    // sound only while `Predicate` has one variant (as the 1-1 comparison's).
    match def.def.predicate {
        Predicate::True => {}
    }
    let hidden = group_basis::hidden_table_ident(pool.schema(), def.id);
    let sql = page_sql(
        shape,
        identity,
        &ddl::qualified_source_table(&def.source_table),
        &hidden,
        &ddl::qualified_target_table_ident(&def.target_table),
    )?;
    let client = pool.get().await?;
    let after: Option<&str> = scope.after.as_deref();
    let rows = client.query(sql.as_str(), &[&after, &scope.limit]).await?;

    let mut sides: [Side; 4] = Default::default();
    let mut page_end: Option<String> = None;
    for row in &rows {
        let side: i32 = row.get(0);
        page_end = row.get(1);
        sides[(side - 1) as usize].insert(row.get(2), row.get(3));
    }
    let [src, hid, implied, vis] = sides;
    let hidden_name = group_basis::hidden_table_name(def.id);
    let partials: Vec<String> = partial_columns(shape)
        .into_iter()
        .map(|c| format!("{hidden_name}.{c}"))
        .collect();
    let fields: Vec<&str> = shape.fields.iter().map(|(n, _, _)| n.as_str()).collect();
    let diff = diff_groups(&partials, &fields, paused, src, hid, implied, vis, page_end);
    Ok(ComparePass {
        checked_through,
        rows_compared: diff.0,
        next_after: diff.1,
        divergences: diff.2,
    })
}

/// The pure half of [`compare`]: `(rows compared, next cursor, divergences)`.
/// `partials` and `fields` name the columns of the sides' value lists.
#[allow(clippy::too_many_arguments)]
fn diff_groups(
    partials: &[String],
    fields: &[&str],
    paused: &HashSet<String>,
    src: Side,
    hid: Side,
    implied: Side,
    vis: Side,
    page_end: Option<String>,
) -> (i64, Option<String>, Vec<Divergence>) {
    let mut divergences = Vec::new();
    let zeros: Vec<Option<String>> = partials.iter().map(|_| Some("0".to_string())).collect();

    // The hidden partials against the source's. A group on one side only is
    // all zeros on the other: the hidden table keeps a zero group's row until
    // the zero-group GC (`retire::collect_zero_groups`) deletes it.
    let keys: BTreeSet<&String> = src.keys().chain(hid.keys()).collect();
    for key in keys {
        let expected = src.get(key).unwrap_or(&zeros);
        let actual = hid.get(key).unwrap_or(&zeros);
        for (column, (r, p)) in partials.iter().zip(expected.iter().zip(actual)) {
            if r != p {
                divergences.push(Divergence::Cell {
                    key: key.clone(),
                    column: column.clone(),
                    persisted: p.clone(),
                    recomputed: r.clone(),
                });
            }
        }
    }

    // The visible rows against the partials.
    for (key, expected) in &implied {
        match vis.get(key) {
            None => divergences.push(Divergence::MissingRow { key: key.clone() }),
            Some(actual) => {
                for (column, (r, p)) in fields.iter().zip(expected.iter().zip(actual)) {
                    if r != p && !paused.contains(*column) {
                        divergences.push(Divergence::Cell {
                            key: key.clone(),
                            column: column.to_string(),
                            persisted: p.clone(),
                            recomputed: r.clone(),
                        });
                    }
                }
            }
        }
    }
    for key in vis.keys() {
        if !implied.contains_key(key) {
            divergences.push(Divergence::ExtraRow { key: key.clone() });
        }
    }

    // The visible rows against the source's groups (ADR-0013: a group the
    // source has and the target lacks, or the reverse, is a missing or extra
    // row), where the partials agree with the target and so didn't say it.
    for key in src.keys() {
        if !vis.contains_key(key) && !implied.contains_key(key) {
            divergences.push(Divergence::MissingRow { key: key.clone() });
        }
    }
    for key in vis.keys() {
        if !src.contains_key(key) && implied.contains_key(key) {
            divergences.push(Divergence::ExtraRow { key: key.clone() });
        }
    }

    let all: BTreeSet<&String> = src
        .keys()
        .chain(hid.keys())
        .chain(implied.keys())
        .chain(vis.keys())
        .collect();
    (all.len() as i64, page_end, divergences)
}
