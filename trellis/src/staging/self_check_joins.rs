//! `self_check` of a `JOIN` target (ADR-0013 "Scope", ADR-0019).
//!
//! A `JOIN` target is audited in four parts, walked in this order, each a
//! keyset walk of its own keys:
//!
//! 1. **`T` and the visible target**, a page of target groups, by the target's
//!    identity columns ([`super::self_check_keyset`]), named in a report by
//!    their key text (`ddl::pk_key_sql_expr`). One
//!    statement reads, under one snapshot, the definition's own `JOIN …
//!    GROUP BY` over the two tables as the target has applied them (each group's row count, each
//!    argument's non-null count and, for a summed argument, its sum, and
//!    each visible field as the definition writes it), `T`'s partials, and
//!    the visible rows. `T` is compared against the query's partials, which
//!    are `Σ_k P ⊗ M` over the sides as they are (the cross terms of
//!    ADR-0019, `LEFT`'s unmatched rows included), and the visible rows
//!    against the query's rows: a group the query has and the target lacks,
//!    or the reverse, is a missing or extra row.
//! 2. **`P`**, a page of `FROM`-side cells (its join key, then its `GROUP BY`
//!    columns), against a `GROUP BY` of the `FROM` table over those columns.
//! 3. **`M`**, the same for the joined table, over its rows with no `NULL`
//!    join-key part (the only ones that can join anything).
//! 4. **`J`**, a page of join keys: each key's `__matches` against the
//!    joined table's row count for that key.
//!
//! Each hidden table is compared against what the source tables say it
//! holds, not against the other hidden tables, so a damaged `P`, `M`, `J` or
//! `T` is reported on that table alone: a divergence of a hidden table is a
//! [`Divergence::Cell`] whose column is `join_<id>_<part>.<column>`, and whose
//! key is the cell's, the join key's or the group's key text. A cell or key
//! on one side only is all zeros on the other: the hidden tables keep a cell
//! that emptied, and `J` keeps every key a page ever named.
//! Keys, cells and visible fields compare as values, as an aggregate's do
//! (`super::self_check_groups`): a key by its columns' equality, a field
//! whose text differs by its type's `=`.
//!
//! The walk's cursor ([`super::self_check::SelfCheckScope::after`]) carries
//! the part: `t:<key>`, `p:<key>`, `m:<key>` or `j:<key>` continues a part
//! after a key, and a bare `p`, `m` or `j` starts one. The first page of a
//! walk (no cursor) starts the target's part, and the last part's end ends
//! the walk.
//!
//! It reports, never repairs (ADR-0013): the remedy is a
//! `request_backfill` or a `PAUSE`/`RESUME`. Like every page, it is read only
//! after `super::self_check::audit`'s convergence wait. Each part reads its
//! tables minus the ring rows not applied yet ([`super::self_check_ring`]):
//! every row weighted `+1`, each unapplied change's old image `+1` and new
//! image `-1` (left out where its page would refuse it, by its `P` or `M`
//! cell's basis or its side's truncate floor). `P`, `M` and `J` sum their
//! side's weights; the target's part runs the definition's `JOIN … GROUP BY`
//! over both tables' weighted rows, each pair weighing the product of its
//! rows' weights, which is `Σ_k (P − ΔP) ⊗ (M − ΔM)` without the partial
//! algebra. So a divergence is reported from one pass. An unapplied change
//! with no usable image names the cells of its side it may touch as
//! uncertain, and every group of the target's part, which any pair may hold;
//! a divergence there is reported only if it reproduces after a second wait.
//!
//! The SQL here is rendered from the definition separately from the apply
//! path's (`super::join`): the query is the definition's own `JOIN …
//! GROUP BY`, so a rendering bug in the product the apply computes
//! disagrees with it instead of agreeing with itself.
//!
//! Cost: a page of `P` or `M` is keyed in the hidden table's cell identity
//! (`spec::side_key_columns`): the join key, then the side's group columns not
//! among them. The hidden table's identity index has that order, so the hidden side
//! is a range scan of it. The source side is a range scan of an index leading
//! with the side's join columns (`a`'s for `P`, `b`'s for `M`), the group
//! columns sorted within each join key, and the walk of the part reads its
//! table about once. `J` is the same with `b`'s join key. Without such an
//! index the part's source side scans its table per page, aggregating only the
//! keys up to the hidden side's last key. The target's part reads the join of
//! both tables, which no index orders, so each of its pages scans them.

use std::collections::{BTreeSet, HashMap, HashSet};

use tokio_postgres::types::PgLsn;

use crate::defs::ast::{Expr, JoinKind, KeySpace, Predicate, TransformDef};
use crate::defs::catalog;
use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::defs::join::{self as spec, HiddenTable, Side as JoinSide};
use crate::defs::model::Definition;
use crate::defs::registry;
use crate::pool::{Pool, quote_ident};

use super::self_check::{
    ComparePass, Divergence, SelfCheckError, SelfCheckScope, render_call_arg, render_leaf,
};
use super::self_check_groups::{
    Side, Uncertain, bound, drop_equal_values, partial_text, weighted_partials,
};
use super::self_check_keyset::{BYTE_ORDER, Keyset};
use super::self_check_ring::{Lands, Ring, WEIGHT};

/// One part of the walk (the module doc), in walk order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Part {
    /// `T` and the visible target.
    Target,
    /// `P`.
    From,
    /// `M`.
    Joined,
    /// `J`.
    Matches,
}

impl Part {
    fn tag(self) -> &'static str {
        match self {
            Part::Target => "t",
            Part::From => "p",
            Part::Joined => "m",
            Part::Matches => "j",
        }
    }

    fn of_tag(tag: &str) -> Option<Part> {
        [Part::Target, Part::From, Part::Joined, Part::Matches]
            .into_iter()
            .find(|part| part.tag() == tag)
    }

    fn next(self) -> Option<Part> {
        match self {
            Part::Target => Some(Part::From),
            Part::From => Some(Part::Joined),
            Part::Joined => Some(Part::Matches),
            Part::Matches => None,
        }
    }

    /// The hidden table this part compares.
    fn hidden(self) -> HiddenTable {
        match self {
            Part::Target => HiddenTable::TargetPartials,
            Part::From => HiddenTable::FromPartials,
            Part::Joined => HiddenTable::JoinedPartials,
            Part::Matches => HiddenTable::Matches,
        }
    }
}

/// The part and the key within it a cursor continues after (the module
/// doc). A cursor of no part's form continues the target's part after it.
fn parse_cursor(after: Option<&str>) -> (Part, Option<String>) {
    let Some(after) = after else {
        return (Part::Target, None);
    };
    match after.split_once(':') {
        Some((tag, key)) => match Part::of_tag(tag) {
            Some(part) => (part, Some(key.to_string())),
            None => (Part::Target, Some(after.to_string())),
        },
        None => match Part::of_tag(after) {
            Some(part) => (part, None),
            None => (Part::Target, Some(after.to_string())),
        },
    }
}

/// The cursor that follows a page of `part` that ended at `page_end`
/// (`None`: the part's end).
fn next_cursor(part: Part, page_end: Option<String>) -> Option<String> {
    match page_end {
        Some(key) => Some(format!("{}:{key}", part.tag())),
        None => part.next().map(|next| next.tag().to_string()),
    }
}

/// `def`'s `JOIN`. Only called on a `JOIN` definition.
fn join_of(def: &TransformDef) -> &crate::defs::ast::JoinSpec {
    match &def.key_space {
        KeySpace::Join(join) => join,
        _ => unreachable!("self_check_joins is only reached for a JOIN definition"),
    }
}

/// The side `table`, a bare table name as written in the definition, is.
fn side_of(def: &TransformDef, table: &str) -> Option<JoinSide> {
    if table == def.source {
        Some(JoinSide::From)
    } else if table == join_of(def).right {
        Some(JoinSide::Joined)
    } else {
        None
    }
}

/// Renders a `JOIN` field expression, or an aggregate's argument, as SQL
/// with every column read through its side's alias (`from` or `joined`).
/// [`render_leaf`]'s twin for two tables: written fresh here, not shared
/// with the apply path. `Err` for what a `JOIN` definition can't hold.
fn render(def: &TransformDef, expr: &Expr, from: &str, joined: &str) -> Result<String, String> {
    Ok(match expr {
        Expr::SourceColumn { table, column, .. } => {
            let alias = match side_of(def, table) {
                Some(JoinSide::From) => from,
                Some(JoinSide::Joined) => joined,
                None => return Err(format!("reads '{table}.{column}', which is neither table")),
            };
            format!("{alias}.{}", quote_ident(column))
        }
        Expr::Column(name) => {
            return Err(format!(
                "reads the unqualified column '{name}', which a JOIN definition can't hold"
            ));
        }
        Expr::BinaryOp { op, lhs, rhs } => {
            let symbol = registry::operator_spec(*op).symbol;
            format!(
                "({} {symbol} {})",
                render(def, lhs, from, joined)?,
                render(def, rhs, from, joined)?
            )
        }
        Expr::Negate(operand) => format!("(-({}))", render(def, operand, from, joined)?),
        Expr::Cast { expr, target } => {
            format!("cast({} as {target})", render(def, expr, from, joined)?)
        }
        Expr::Not(operand) => format!("(not {})", render(def, operand, from, joined)?),
        Expr::IsNull { expr, negated } => format!(
            "({} {})",
            render(def, expr, from, joined)?,
            if *negated { "is not null" } else { "is null" }
        ),
        Expr::Case {
            branches,
            otherwise,
        } => case_sql(branches, otherwise.as_deref(), |e| {
            render(def, e, from, joined)
        })?,
        Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
            "count(*)".to_string()
        }
        Expr::FunctionCall { name, args } => {
            let mut rendered = Vec::with_capacity(args.len());
            for (i, arg) in args.iter().enumerate() {
                rendered.push(match arg {
                    Expr::NumberLiteral(_) => render_call_arg(name, i, arg, render_leaf),
                    _ => render(def, arg, from, joined)?,
                });
            }
            format!("{}({})", name.to_lowercase(), rendered.join(", "))
        }
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::TypedLiteral { .. } => {
            render_leaf(expr)
        }
    })
}

/// A searched `CASE` as SQL text, each condition and result rendered by
/// `render`.
fn case_sql(
    branches: &[(Expr, Expr)],
    otherwise: Option<&Expr>,
    mut render: impl FnMut(&Expr) -> Result<String, String>,
) -> Result<String, String> {
    let mut sql = String::from("(case");
    for (condition, result) in branches {
        sql.push_str(&format!(
            " when {} then {}",
            render(condition)?,
            render(result)?
        ));
    }
    if let Some(otherwise) = otherwise {
        sql.push_str(&format!(" else {}", render(otherwise)?));
    }
    sql.push_str(" end)");
    Ok(sql)
}

/// The fields of the visible target that aren't a `GROUP BY` key, in
/// definition order.
fn value_fields(def: &TransformDef) -> Vec<&crate::defs::ast::FieldDef> {
    let join = join_of(def);
    def.fields
        .iter()
        .filter(|f| !join.group_by.iter().any(|key| key.column == f.name))
        .collect()
}

/// Refuses a `JOIN` definition with a field or argument [`render`] can't
/// render.
pub(super) fn check_renders(def: &Definition) -> Result<(), SelfCheckError> {
    let args: Vec<Expr> = spec::side_arguments(&def.def, JoinSide::From)
        .into_iter()
        .chain(spec::side_arguments(&def.def, JoinSide::Joined))
        .collect();
    for field in value_fields(&def.def) {
        render(&def.def, &field.expr, "a", "b")
            .and_then(|_| render_signed(&def.def, &field.expr, &args))
            .map_err(|detail| SelfCheckError::UnsupportedExpr {
                target: def.def.target.clone(),
                detail,
            })?;
    }
    Ok(())
}

/// Whether each of `args` is summed: some field takes its `SUM` or `AVG`,
/// so its hidden tables keep its sum.
fn summed(def: &TransformDef, args: &[Expr]) -> Vec<bool> {
    fn walk(expr: &Expr, args: &[Expr], out: &mut [bool]) {
        match expr {
            Expr::FunctionCall { name, args: called } => {
                let upper = name.to_ascii_uppercase();
                if let ("SUM" | "AVG", [arg]) = (upper.as_str(), called.as_slice())
                    && let Some(i) = args.iter().position(|a| a == arg)
                {
                    out[i] = true;
                }
                for arg in called {
                    walk(arg, args, out);
                }
            }
            Expr::BinaryOp { lhs, rhs, .. } => {
                walk(lhs, args, out);
                walk(rhs, args, out);
            }
            Expr::Negate(operand) | Expr::Cast { expr: operand, .. } => walk(operand, args, out),
            Expr::Not(_) | Expr::IsNull { .. } | Expr::Case { .. } => {
                for child in expr.logic_children() {
                    walk(child, args, out);
                }
            }
            _ => {}
        }
    }
    let mut out = vec![false; args.len()];
    for field in &def.fields {
        walk(&field.expr, args, &mut out);
    }
    out
}

/// One table's partial columns, as named in its hidden table: the row
/// count, then per argument its non-null count and, if summed, its sum.
fn partial_columns(summed: &[bool]) -> Vec<String> {
    let mut names = vec!["__n".to_string()];
    for (i, summed) in summed.iter().enumerate() {
        names.push(format!("__count_{}", i + 1));
        if *summed {
            names.push(format!("__sum_{}", i + 1));
        }
    }
    names
}

/// The key text of `key` read through `alias`, under byte order.
fn key_sql(key: &[PrimaryKeyColumn], alias: &str) -> String {
    format!(
        "({}) collate {BYTE_ORDER}",
        ddl::pk_key_sql_expr(key, Some(alias))
    )
}

/// What a page reads of the ring besides its own sides
/// (`super::self_check_ring`): the CTEs it needs first (each ending with
/// `, `), a query of the key text (`__k`) and columns (`__c0`, …) of every
/// key an unsubtractable ring row may touch, and the condition under which
/// every key may.
struct RingReads {
    ctes: String,
    uncertain: String,
    global: String,
}

/// The statement of a page of `P`, `M` or `J`: `$1` is the limit, and the
/// cursor is in the statement ([`Keyset::set_cursor`]); each row is `(side, page_end, key, values)`, side 1 the source's and 2 the
/// hidden table's, as `super::self_check_groups`' page, and sides 5 and 6 its
/// uncertain keys and its "every key" row. `source_from` names the weighted
/// source rows as `__s`, carrying `keyset`'s columns, `source_where`
/// (`and …`, or empty) filters them, and `having` keeps a key whose partials
/// aren't all zero.
#[allow(clippy::too_many_arguments)]
fn partials_page_sql(
    keyset: &Keyset,
    hidden: &str,
    hidden_values: &[String],
    source_from: &str,
    source_where: &str,
    source_values: &[String],
    having: &str,
    ring: &RingReads,
    bounded: bool,
) -> String {
    let key = keyset.columns();
    let hk = ddl::pk_key_sql_expr(key, Some("__h"));
    let sk = ddl::pk_key_sql_expr(key, Some("__s"));
    let h_refs = keyset.refs(Some("__h"));
    let s_refs = keyset.refs(Some("__s"));
    let h_carried = keyset.select_carried(&h_refs);
    let s_carried = keyset.select_carried(&s_refs);
    let hid_end: Vec<String> = keyset
        .carried(None)
        .iter()
        .map(|c| format!("(select {c} from he)"))
        .collect();
    let within_hid = bound(keyset, &s_refs, &hid_end, bounded);
    let group: Vec<String> = s_refs.clone();
    let h_order = keyset.order_by(&h_refs);
    let s_order = keyset.order_by(&s_refs);
    let hv = hidden_values.join(", ");
    let sv = source_values.join(", ");
    let hid = keyset.side(&h_refs, |cond| {
        format!(
            "select {hk} as __k{h_carried}, array[{hv}]::text[] as __v from {hidden} __h \
             where {cond} order by {h_order} limit $1"
        )
    });
    let src = keyset.side(&s_refs, |cond| {
        format!(
            "select {sk} as __k{s_carried}, array[{sv}]::text[] as __v from {source_from} \
             where {cond} and {within_hid} {source_where} \
             group by {group} having {having} order by {s_order} limit $1",
            group = group.join(", "),
        )
    });
    let ctes = format!(
        "{ctes}\
         hid0 as materialized ({hid}), \
         hid as materialized ( \
             select hid0.*, row_number() over (order by {c_order}) as __rn from hid0), \
         he as (select {c_cols} from hid where __rn = $1), \
         src as materialized ({src}), \
         unc as ({uncertain}), ",
        ctes = ring.ctes,
        c_order = keyset.order_by(&keyset.carried(None)),
        c_cols = keyset.carried(None).join(", "),
        uncertain = ring.uncertain,
    );
    // An uncertain image's key is the page's equal key's text
    // ([`Keyset::named_key`]).
    let extra = format!(
        " union all select 5, page.page_end, {unc_named}, null::text[] from unc, page \
          union all select 6, page.page_end, '', null::text[] from page where {global}",
        unc_named = keyset.named_key("unc", "unc.__k"),
        global = ring.global,
    );
    keyset.ranked_by_value(&ctes, &[(1, "src"), (2, "hid")], &[1, 2], &extra)
}

/// Everything a page of a `JOIN` target needs, read from the definition and
/// the catalog.
struct Audit<'a> {
    def: &'a TransformDef,
    id: i64,
    schema: String,
    /// The two tables, quoted.
    from_table: String,
    joined_table: String,
    /// The two tables as the ring names them, and their row identities.
    from_src: &'a str,
    joined_src: &'a str,
    from_pk: Vec<PrimaryKeyColumn>,
    joined_pk: Vec<PrimaryKeyColumn>,
    target: String,
    from_args: Vec<Expr>,
    joined_args: Vec<Expr>,
    from_summed: Vec<bool>,
    joined_summed: Vec<bool>,
}

impl Audit<'_> {
    fn hidden(&self, part: Part) -> String {
        format!(
            "{}.{}",
            quote_ident(&self.schema),
            quote_ident(&part.hidden().name(self.id))
        )
    }

    /// The hidden table's columns `columns`, named as a divergence names
    /// them.
    fn named(&self, part: Part, columns: &[String]) -> Vec<String> {
        let table = part.hidden().name(self.id);
        columns.iter().map(|c| format!("{table}.{c}")).collect()
    }

    fn hidden_values(columns: &[String]) -> Vec<String> {
        columns
            .iter()
            .map(|c| partial_text(c, &quote_ident(c)))
            .collect()
    }

    fn unsupported(&self, detail: String) -> SelfCheckError {
        SelfCheckError::UnsupportedExpr {
            target: self.def.target.clone(),
            detail,
        }
    }

    fn render_args(&self, args: &[Expr], alias: &str) -> Result<Vec<String>, SelfCheckError> {
        args.iter()
            .map(|arg| render(self.def, arg, alias, alias).map_err(|d| self.unsupported(d)))
            .collect()
    }

    /// `side`'s table's unapplied ring rows (`super::self_check_ring`).
    fn ring(&self, side: JoinSide) -> Ring<'_> {
        match side {
            JoinSide::From => Ring {
                src_table: self.from_src,
                source_row: self.from_table.clone(),
                source_pk: &self.from_pk,
                transform_id: self.id,
                tag: "a",
            },
            JoinSide::Joined => Ring {
                src_table: self.joined_src,
                source_row: self.joined_table.clone(),
                source_pk: &self.joined_pk,
                transform_id: self.id,
                tag: "b",
            },
        }
    }

    /// `ring`'s CTEs and its weighted rows, its changes landing where a page
    /// applies `side`'s: in `P` or `M`, above that table's truncate floor and
    /// unseen by its cell's basis. A joined row with a `NULL` join-key part
    /// lands nowhere. `late_probe` as [`Lands::late_probe`].
    fn ring_reads(&self, side: JoinSide, ring: &Ring<'_>, late_probe: bool) -> (String, String) {
        let hidden = self.hidden(match side {
            JoinSide::From => Part::From,
            JoinSide::Joined => Part::Joined,
        });
        let cells = spec::side_key_columns(self.def, side);
        let filter: String = match side {
            JoinSide::From => String::new(),
            JoinSide::Joined => spec::join_key(self.def, side)
                .iter()
                .map(|c| format!(" and r.{} is not null", quote_ident(c)))
                .collect(),
        };
        let lands = Lands {
            floor_key: &hidden,
            basis: Some((&hidden, &cells)),
            filter,
            late_probe,
        };
        (ring.ctes(&lands), ring.weighted_sql(&lands))
    }

    /// The key text (`__k`) over `key`'s columns, and the columns (`__c0`,
    /// …), of each image of `ring`'s uncertain rows, populated as `__s`, after `rename` (a
    /// lateral `select` over `__r`, the populated row, naming `key`'s
    /// columns as `__s`'s; `None`: `__s` is the populated row itself).
    fn uncertain_keys(ring: &Ring<'_>, key: &[PrimaryKeyColumn], rename: Option<&str>) -> String {
        let populated = match rename {
            None => format!(
                "cross join lateral jsonb_populate_record(null::{}, u.__i) as __s",
                ring.source_row
            ),
            Some(rename) => format!(
                "cross join lateral jsonb_populate_record(null::{}, u.__i) as __r \
                 cross join lateral (select {rename}) as __s",
                ring.source_row
            ),
        };
        let carried: String = key
            .iter()
            .enumerate()
            .map(|(i, c)| format!(", __s.{} as __c{i}", quote_ident(&c.name)))
            .collect();
        format!(
            "select distinct {} as __k{carried} from ({}) u {populated}",
            key_sql(key, "__s"),
            ring.uncertain_images_sql()
        )
    }

    /// `P` or `M`'s page statement, and its partial columns: the side's
    /// cells against a `GROUP BY` of its table's weighted rows.
    fn side_page(
        &self,
        side: JoinSide,
        keyset: &Keyset,
        steer: bool,
    ) -> Result<(String, Vec<String>), SelfCheckError> {
        let (part, args, summed) = match side {
            JoinSide::From => (Part::From, &self.from_args, &self.from_summed),
            JoinSide::Joined => (Part::Joined, &self.joined_args, &self.joined_summed),
        };
        let key = keyset.columns();
        // A joined row with a `NULL` join-key part joins nothing, and has no
        // `M` cell.
        let source_where: String = match side {
            JoinSide::From => String::new(),
            JoinSide::Joined => spec::join_key(self.def, side)
                .iter()
                .map(|c| format!(" and __s.{} is not null", quote_ident(c)))
                .collect(),
        };
        let columns = partial_columns(summed);
        let rendered: Vec<(String, bool)> = self
            .render_args(args, "__s")?
            .into_iter()
            .zip(summed.iter().copied())
            .collect();
        let (values, nonzero) = weighted_partials(&rendered);
        let ring = self.ring(side);
        let (ctes, weighted) = self.ring_reads(side, &ring, true);
        let reads = RingReads {
            ctes,
            uncertain: Self::uncertain_keys(&ring, key, None),
            global: ring.global_sql(),
        };
        let sql = partials_page_sql(
            keyset,
            &self.hidden(part),
            &Self::hidden_values(&columns),
            &format!("{weighted} __s"),
            &source_where,
            &values,
            &nonzero,
            &reads,
            !steer,
        );
        Ok((sql, columns))
    }

    /// `J`'s page statement: each join key's `__matches` against the joined
    /// table's weighted row count for it, read under `J`'s column names.
    fn matches_page(&self, keyset: &Keyset, steer: bool) -> String {
        let j_cols = spec::join_key(self.def, JoinSide::From);
        let joined_cols = spec::join_key(self.def, JoinSide::Joined);
        let renamed = |alias: &str| {
            joined_cols
                .iter()
                .zip(&j_cols)
                .map(|(b, j)| format!("{alias}.{} as {}", quote_ident(b), quote_ident(j)))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let not_null: String = j_cols
            .iter()
            .map(|c| format!(" and __s.{} is not null", quote_ident(c)))
            .collect();
        let key = keyset.columns();
        let ring = self.ring(JoinSide::Joined);
        let (ctes, weighted) = self.ring_reads(JoinSide::Joined, &ring, true);
        let reads = RingReads {
            ctes,
            uncertain: Self::uncertain_keys(&ring, key, Some(&renamed("__r"))),
            global: ring.global_sql(),
        };
        partials_page_sql(
            keyset,
            &self.hidden(Part::Matches),
            &["__matches::text".to_string()],
            &format!(
                "{weighted} __b cross join lateral (select {}, __b.{WEIGHT} as {WEIGHT}) __s",
                renamed("__b"),
            ),
            &not_null,
            &[format!("sum(__s.{WEIGHT})::text")],
            &format!("sum(__s.{WEIGHT}) <> 0"),
            &reads,
            !steer,
        )
    }

    /// The target part's statement (the module doc): each row is `(side,
    /// page_end, key, values)`, side 1 the query's partials, 2 `T`'s, 3 the
    /// query's visible rows and 4 the target's, and side 6 says every group
    /// may still differ. `identity` is the target's group key;
    /// `field_types` the visible fields' column types.
    ///
    /// The query is the definition's `JOIN … GROUP BY` over both tables'
    /// weighted rows (`super::self_check_ring`): each pair's weight is the
    /// product of its rows', and each partial and field sums weights, so it
    /// reads both tables as the target has applied them. A `LEFT JOIN`'s
    /// row is unmatched while its key's joined rows weigh 0 in all.
    fn target_page(
        &self,
        keyset: &Keyset,
        fields: &[&crate::defs::ast::FieldDef],
        field_types: &HashMap<String, String>,
    ) -> Result<(String, Vec<String>), SelfCheckError> {
        let join = join_of(self.def);
        let alias = |table: &str| match side_of(self.def, table) {
            Some(JoinSide::Joined) => "__b",
            _ => "__a",
        };
        let identity = keyset.columns();
        let hk = ddl::pk_key_sql_expr(identity, Some("__h"));
        let gk = ddl::pk_key_sql_expr(identity, Some("__g"));
        let vk = ddl::pk_key_sql_expr(identity, Some("__t"));
        let (h_refs, g_refs, v_refs) = (
            keyset.refs(Some("__h")),
            keyset.refs(Some("__g")),
            keyset.refs(Some("__t")),
        );
        let (h_carried, g_carried, v_carried) = (
            keyset.select_carried(&h_refs),
            keyset.select_carried(&g_refs),
            keyset.select_carried(&v_refs),
        );
        let hid_end: Vec<String> = keyset
            .carried(None)
            .iter()
            .map(|c| format!("(select {c} from he)"))
            .collect();
        let within_hid = format!(
            "(not exists (select 1 from he) or {})",
            keyset.at_most(&g_refs, &hid_end)
        );

        // The query's partials: the `FROM` side's arguments, then the joined
        // side's, as `T` numbers them, each a column of the pairs.
        let all_args: Vec<Expr> = self
            .from_args
            .iter()
            .chain(&self.joined_args)
            .cloned()
            .collect();
        let mut xs = String::new();
        for (i, arg) in all_args.iter().enumerate() {
            let rendered = render(self.def, arg, "__a", "__b").map_err(|d| self.unsupported(d))?;
            xs.push_str(&format!(", {rendered} as __x{}", i + 1));
        }
        let summed: Vec<bool> = self
            .from_summed
            .iter()
            .chain(&self.joined_summed)
            .copied()
            .collect();
        let columns = partial_columns(&summed);
        let x_columns: Vec<(String, bool)> = summed
            .iter()
            .enumerate()
            .map(|(i, s)| (format!("__x{}", i + 1), *s))
            .collect();
        let (sv, nonzero) = weighted_partials(&x_columns);
        let mut oracle_fields = Vec::with_capacity(fields.len());
        for field in fields {
            let pg_type = field_types.get(&field.name).map_or("text", String::as_str);
            oracle_fields.push(format!(
                "(({})::{pg_type})::text",
                render_signed(self.def, &field.expr, &all_args).map_err(|d| self.unsupported(d))?
            ));
        }
        let on: Vec<String> = join
            .on
            .iter()
            .map(|term| {
                format!(
                    "{}.{} = {}.{}",
                    alias(&term.lhs.table),
                    quote_ident(&term.lhs.column),
                    alias(&term.rhs.table),
                    quote_ident(&term.rhs.column)
                )
            })
            .collect();
        let groups: Vec<String> = join
            .group_by
            .iter()
            .map(|key| {
                format!(
                    "{}.{} as {}",
                    alias(&key.table),
                    quote_ident(&key.column),
                    quote_ident(&key.column)
                )
            })
            .collect();
        let group_by: Vec<String> = join
            .group_by
            .iter()
            .map(|key| format!("__g.{}", quote_ident(&key.column)))
            .collect();
        let (ra, rb) = (self.ring(JoinSide::From), self.ring(JoinSide::Joined));
        // Every weighted row is read, some twice, so each image is probed
        // once, in the CTEs.
        let (mut ctes, wa) = self.ring_reads(JoinSide::From, &ra, false);
        let (ctes_b, wb) = self.ring_reads(JoinSide::Joined, &rb, false);
        ctes.push_str(&ctes_b);
        let mut pairs = format!(
            "select __g.*, __a.{WEIGHT} * __b.{WEIGHT} as {WEIGHT}{xs} \
             from {wa} __a join {wb} __b on {on} \
             cross join lateral (select {groups}) __g",
            on = on.join(" and "),
            groups = groups.join(", "),
        );
        if join.kind == JoinKind::Left {
            // Each joined key's rows' weight in all: a `FROM` row whose key's
            // is 0 (or that has a `NULL` key part) is unmatched.
            let a_cols = spec::join_key(self.def, JoinSide::From);
            let b_cols = spec::join_key(self.def, JoinSide::Joined);
            let b_list: Vec<String> = b_cols
                .iter()
                .map(|c| format!("__b.{}", quote_ident(c)))
                .collect();
            let b_not_null: Vec<String> = b_cols
                .iter()
                .map(|c| format!("__b.{} is not null", quote_ident(c)))
                .collect();
            let bn_on: Vec<String> = a_cols
                .iter()
                .zip(&b_cols)
                .map(|(a, b)| format!("__bn.{} = __a.{}", quote_ident(b), quote_ident(a)))
                .collect();
            ctes.push_str(&format!(
                "__bn as materialized ( \
                     select {b_list}, sum(__b.{WEIGHT}) as {WEIGHT} from {wb} __b \
                     where {b_not_null} group by {b_list}), ",
                b_list = b_list.join(", "),
                b_not_null = b_not_null.join(" and "),
            ));
            pairs.push_str(&format!(
                " union all \
                 select __g.*, __a.{WEIGHT}{xs} \
                 from {wa} __a \
                 cross join lateral (select (null::{b}).*) __b \
                 cross join lateral (select {groups}) __g \
                 where not exists (select 1 from __bn where {bn_on} and __bn.{WEIGHT} <> 0)",
                b = self.joined_table,
                groups = groups.join(", "),
                bn_on = bn_on.join(" and "),
            ));
        }
        let visible: Vec<String> = fields
            .iter()
            .map(|f| format!("{}::text", quote_ident(&f.name)))
            .collect();
        let hv = Self::hidden_values(&columns).join(", ");
        let (h_order, g_order, v_order) = (
            keyset.order_by(&h_refs),
            keyset.order_by(&g_refs),
            keyset.order_by(&v_refs),
        );
        let t = self.hidden(Part::Target);
        let sv = sv.join(", ");
        let sf = oracle_fields.join(", ");
        let group_by = group_by.join(", ");
        let vv = visible.join(", ");
        let target = &self.target;
        let hid = keyset.side(&h_refs, |cond| {
            format!(
                "select {hk} as __k{h_carried}, array[{hv}]::text[] as __v from {t} __h \
                 where {cond} order by {h_order} limit $1"
            )
        });
        let src = keyset.side(&g_refs, |cond| {
            format!(
                "select {gk} as __k{g_carried}, array[{sv}]::text[] as __v, \
                        array[{sf}]::text[] as __f, sum({WEIGHT}) > 0 as __pos \
                 from ({pairs}) __g \
                 where {cond} and {within_hid} \
                 group by {group_by} having {nonzero} order by {g_order} limit $1"
            )
        });
        let vis = keyset.side(&v_refs, |cond| {
            format!(
                "select {vk} as __k{v_carried}, array[{vv}]::text[] as __v from {target} __t \
                 where {cond} order by {v_order} limit $1"
            )
        });
        let ctes = format!(
            "{ctes}\
             hid0 as materialized ({hid}), \
             hid as materialized ( \
                 select hid0.*, row_number() over (order by {c_order}) as __rn from hid0), \
             he as (select {c_cols} from hid where __rn = $1), \
             src as materialized ({src}), \
             pos as (select __k, {c_cols}, __f as __v from src where __pos), \
             vis as materialized ({vis}), ",
            c_order = keyset.order_by(&keyset.carried(None)),
            c_cols = keyset.carried(None).join(", "),
        );
        let extra = format!(
            " union all select 6, page.page_end, '', null::text[] from page \
              where ({any_a}) or ({any_b})",
            any_a = ra.any_uncertain_sql(),
            any_b = rb.any_uncertain_sql(),
        );
        let sql = keyset.ranked_by_value(
            &ctes,
            &[(1, "src"), (2, "hid"), (3, "pos"), (4, "vis")],
            &[1, 2, 4],
            &extra,
        );
        Ok((sql, columns))
    }
}

/// Renders a visible field of a `JOIN` over the weighted pairs of
/// [`Audit::target_page`] (aliased `__g`): each aggregate sums the pairs'
/// weights, `COUNT(*)` their weights, `COUNT(x)` those where `x` isn't
/// `NULL`, `SUM(x)` the positive pairs' `x` less the negative ones' (`NULL`
/// while that count is 0, as `sum()` is) and `AVG(x)` that sum over that
/// count, as Postgres's `avg()` divides. `args` are the pairs' `__x<i>`
/// columns, in order. [`render`]'s twin for weighted rows, written fresh
/// here, not shared with the apply path.
fn render_signed(def: &TransformDef, expr: &Expr, args: &[Expr]) -> Result<String, String> {
    let w = WEIGHT;
    Ok(match expr {
        Expr::FunctionCall { name, args: called } => {
            let upper = name.to_ascii_uppercase();
            match (upper.as_str(), called.as_slice()) {
                ("COUNT", []) => format!("sum({w})"),
                (aggregate @ ("COUNT" | "SUM" | "AVG"), [arg]) => {
                    let i = args
                        .iter()
                        .position(|a| a == arg)
                        .ok_or_else(|| format!("{aggregate} of an expression it doesn't read"))?;
                    let x = format!("__x{}", i + 1);
                    let count = format!("sum({w} * ({x} is not null)::int)");
                    let sum = format!(
                        "(coalesce(sum({x}) filter (where {w} > 0), 0) \
                          - coalesce(sum({x}) filter (where {w} < 0), 0))"
                    );
                    match aggregate {
                        "COUNT" => count,
                        "SUM" => format!("(case when {count} = 0 then null else {sum} end)"),
                        _ => format!(
                            "(case when {count} = 0 then null \
                              else ({sum})::numeric / ({count})::numeric end)"
                        ),
                    }
                }
                _ => {
                    let mut rendered = Vec::with_capacity(called.len());
                    for (i, arg) in called.iter().enumerate() {
                        rendered.push(match arg {
                            Expr::NumberLiteral(_) => render_call_arg(name, i, arg, render_leaf),
                            _ => render_signed(def, arg, args)?,
                        });
                    }
                    format!("{}({})", name.to_lowercase(), rendered.join(", "))
                }
            }
        }
        Expr::BinaryOp { op, lhs, rhs } => {
            let symbol = registry::operator_spec(*op).symbol;
            format!(
                "({} {symbol} {})",
                render_signed(def, lhs, args)?,
                render_signed(def, rhs, args)?
            )
        }
        Expr::Negate(operand) => format!("(-({}))", render_signed(def, operand, args)?),
        Expr::Cast { expr, target } => {
            format!("cast({} as {target})", render_signed(def, expr, args)?)
        }
        Expr::Not(operand) => format!("(not {})", render_signed(def, operand, args)?),
        Expr::IsNull { expr, negated } => format!(
            "({} {})",
            render_signed(def, expr, args)?,
            if *negated { "is not null" } else { "is null" }
        ),
        Expr::Case {
            branches,
            otherwise,
        } => case_sql(branches, otherwise.as_deref(), |e| {
            render_signed(def, e, args)
        })?,
        Expr::SourceColumn { table, column, .. }
            if join_of(def)
                .group_by
                .iter()
                .any(|key| &key.table == table && &key.column == column) =>
        {
            format!("__g.{}", quote_ident(column))
        }
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::TypedLiteral { .. } => {
            render_leaf(expr)
        }
        _ => {
            return Err(format!(
                "a field reading {expr:?} outside an aggregate, which a JOIN definition can't \
                 hold"
            ));
        }
    })
}

/// The joined table of `def`, as the catalog records it.
async fn joined_table(
    client: &tokio_postgres::Client,
    def: &Definition,
) -> Result<String, SelfCheckError> {
    let (joined, _) = catalog::join_side_of(client, def.id)
        .await?
        .ok_or_else(|| SelfCheckError::UnsupportedExpr {
            target: def.def.target.clone(),
            detail: "a JOIN definition with no joined table recorded".to_string(),
        })?;
    Ok(joined)
}

/// `def`'s audit over its tables (`joined` from [`joined_table`]).
async fn build_audit<'a>(
    pool: &Pool,
    def: &'a Definition,
    joined: &'a String,
) -> Result<Audit<'a>, SelfCheckError> {
    let from_args = spec::side_arguments(&def.def, JoinSide::From);
    let joined_args = spec::side_arguments(&def.def, JoinSide::Joined);
    let from_pk = ddl::source_primary_key(pool, &def.source_table).await?;
    let joined_pk = ddl::source_primary_key(pool, joined).await?;
    Ok(Audit {
        def: &def.def,
        id: def.id,
        schema: pool.schema().to_string(),
        from_table: ddl::qualified_source_table(&def.source_table),
        joined_table: ddl::qualified_source_table(joined),
        from_src: &def.source_table,
        joined_src: joined,
        from_pk,
        joined_pk,
        target: ddl::qualified_target_table_ident(&def.target_table),
        from_summed: summed(&def.def, &from_args),
        joined_summed: summed(&def.def, &joined_args),
        from_args,
        joined_args,
    })
}

impl Audit<'_> {
    /// `part`'s page statement after `after`, its partial columns, and
    /// whether the page steers by the source's index ([`Keyset::run`]).
    async fn page_statement(
        &self,
        client: &tokio_postgres::Client,
        def: &Definition,
        identity: &[PrimaryKeyColumn],
        part: Part,
        after: Option<&str>,
    ) -> Result<(String, Vec<String>, bool), SelfCheckError> {
        let audit = self;
        let fields = value_fields(&def.def);
        let field_types = if part == Part::Target {
            super::join::target_column_types(client, &def.target_table).await?
        } else {
            HashMap::new()
        };
        // A part's key columns are typed and collated as its hidden table's,
        // which define made from the source's and a change between
        // deterministic collations of the source leaves alone: a walk then
        // pages in one order from its first page to its last, as an
        // aggregate's does by its target's columns. A side's key is its
        // hidden identity's order, the join key first, so the hidden table's
        // index serves the hidden side and an index on the source's join
        // columns serves the source side.
        let mut keyset = match part {
            Part::Target => Keyset::resolve(client, identity).await?,
            Part::From | Part::Joined | Part::Matches => {
                let names = match part {
                    Part::From => spec::side_key_columns(&def.def, JoinSide::From),
                    Part::Joined => spec::side_key_columns(&def.def, JoinSide::Joined),
                    _ => spec::join_key(&def.def, JoinSide::From),
                };
                Keyset::resolve_columns(client, &audit.hidden(part), &names).await?
            }
        };
        // The single-table parts are index-ordered when their table has the
        // index define requires, leading with its join key
        // ([`crate::defs::source_index`], [`Keyset::run`]): the page's later
        // columns, a side's group columns, are sorted within each join key.
        let steer = match part {
            Part::Target => false,
            Part::From => {
                let key = spec::join_key(&def.def, JoinSide::From);
                crate::defs::source_index::has_index(client, audit.from_src, &key).await?
            }
            Part::Joined | Part::Matches => {
                let key = spec::join_key(&def.def, JoinSide::Joined);
                crate::defs::source_index::has_index(client, audit.joined_src, &key).await?
            }
        };
        keyset.set_cursor(after)?;
        let (sql, columns) = match part {
            Part::Target => audit.target_page(&keyset, &fields, &field_types)?,
            Part::From => audit.side_page(JoinSide::From, &keyset, steer)?,
            Part::Joined => audit.side_page(JoinSide::Joined, &keyset, steer)?,
            Part::Matches => (
                audit.matches_page(&keyset, steer),
                vec!["__matches".to_string()],
            ),
        };
        Ok((sql, columns, steer))
    }
}

/// The plan of `part`'s page statement of `def` after `after`, with
/// sequential scans disabled so that a table still read by one is a table
/// no index can serve ([`super::self_check::explain_page`]). `part` is a
/// cursor's part (`p`, `m`, `j`, `t`).
#[cfg(any(test, feature = "internals"))]
pub(super) async fn explain(
    pool: &Pool,
    def: &Definition,
    identity: &[PrimaryKeyColumn],
    part: &str,
    after: Option<&str>,
    limit: i64,
    executed: bool,
) -> Result<String, SelfCheckError> {
    let mut client = pool.get().await?;
    let joined = joined_table(&client, def).await?;
    let audit = build_audit(pool, def, &joined).await?;
    let (part, _) = parse_cursor(Some(part));
    let (sql, _, steer) = audit
        .page_statement(&client, def, identity, part, after)
        .await?;
    let txn = client.transaction().await?;
    txn.batch_execute("set local enable_seqscan to off").await?;
    if steer {
        txn.batch_execute("set local enable_hashagg to off").await?;
    }
    let rows = txn
        .query(
            format!("{} {sql}", super::self_check::explain_verb(executed)).as_str(),
            &[&limit],
        )
        .await?;
    txn.rollback().await?;
    Ok(rows
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>()
        .join("\n"))
}

/// One page of the comparison of a `JOIN` target: the part the cursor names
/// ([`parse_cursor`]), diffed.
pub(super) async fn compare(
    pool: &Pool,
    def: &Definition,
    identity: &[PrimaryKeyColumn],
    scope: &SelfCheckScope,
    paused: &HashSet<String>,
    checked_through: PgLsn,
) -> Result<ComparePass, SelfCheckError> {
    // The query has no `WHERE` for the definition's predicate, sound only
    // while `Predicate` has one variant (as the 1-1 comparison's).
    match def.def.predicate {
        Predicate::True => {}
    }
    let mut client = pool.get().await?;
    let joined = joined_table(&client, def).await?;
    let audit = build_audit(pool, def, &joined).await?;
    let (part, after) = parse_cursor(scope.after.as_deref());
    let fields = value_fields(&def.def);
    let (sql, columns, steer) = audit
        .page_statement(&client, def, identity, part, after.as_deref())
        .await?;
    let rows = Keyset::run(&mut client, sql.as_str(), scope.limit, steer).await?;

    let mut sides: [Side; 4] = Default::default();
    let mut uncertain = Uncertain::default();
    let mut page_end: Option<String> = None;
    for row in &rows {
        let side: i32 = row.get(0);
        page_end = row.get(1);
        match side {
            5 => {
                uncertain.keys.insert(row.get(2));
            }
            6 => uncertain.all = true,
            _ => {
                sides[(side - 1) as usize].insert(row.get(2), row.get(3));
            }
        }
    }
    let [src, hid, oracle, vis] = sides;
    let mut divergences = Vec::new();
    let keys: BTreeSet<String> = src
        .keys()
        .chain(hid.keys())
        .chain(oracle.keys())
        .chain(vis.keys())
        .cloned()
        .collect();
    diff_partials(&audit.named(part, &columns), &src, &hid, &mut divergences);
    if part == Part::Target {
        let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
        diff_rows(&names, paused, &oracle, &vis, &mut divergences);
        if !divergences.is_empty() {
            let field_types =
                super::join::target_column_types(&**client, &def.target_table).await?;
            let types: HashMap<&str, &str> = names
                .iter()
                .map(|n| (*n, field_types.get(*n).map_or("text", String::as_str)))
                .collect();
            drop_equal_values(&client, &types, &mut divergences).await?;
        }
    }
    Ok(ComparePass {
        checked_through,
        rows_compared: keys.len() as i64,
        next_after: next_cursor(part, page_end),
        divergences,
        uncertain,
    })
}

/// A hidden table's partials (`hid`) against the source's (`src`), each
/// list's values named by `columns`. A key on one side only is all zeros on
/// the other.
fn diff_partials(columns: &[String], src: &Side, hid: &Side, out: &mut Vec<Divergence>) {
    let zeros: Vec<Option<String>> = columns.iter().map(|_| Some("0".to_string())).collect();
    let keys: BTreeSet<&String> = src.keys().chain(hid.keys()).collect();
    for key in keys {
        let expected = src.get(key).unwrap_or(&zeros);
        let actual = hid.get(key).unwrap_or(&zeros);
        for (column, (r, p)) in columns.iter().zip(expected.iter().zip(actual)) {
            if r != p {
                out.push(Divergence::Cell {
                    key: key.clone(),
                    column: column.clone(),
                    persisted: p.clone(),
                    recomputed: r.clone(),
                });
            }
        }
    }
}

/// The visible rows (`vis`) against the query's (`oracle`), each list's
/// values named by `fields`; a paused field isn't compared.
fn diff_rows(
    fields: &[&str],
    paused: &HashSet<String>,
    oracle: &Side,
    vis: &Side,
    out: &mut Vec<Divergence>,
) {
    for (key, expected) in oracle {
        match vis.get(key) {
            None => out.push(Divergence::MissingRow { key: key.clone() }),
            Some(actual) => {
                for (field, (r, p)) in fields.iter().zip(expected.iter().zip(actual)) {
                    if r != p && !paused.contains(*field) {
                        out.push(Divergence::Cell {
                            key: key.clone(),
                            column: field.to_string(),
                            persisted: p.clone(),
                            recomputed: r.clone(),
                        });
                    }
                }
            }
        }
    }
    for key in vis.keys() {
        if !oracle.contains_key(key) {
            out.push(Divergence::ExtraRow { key: key.clone() });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn side(rows: &[(&str, &[Option<&str>])]) -> Side {
        rows.iter()
            .map(|(k, v)| {
                (
                    k.to_string(),
                    v.iter().map(|x| x.map(str::to_string)).collect(),
                )
            })
            .collect()
    }

    fn cell(
        key: &str,
        column: &str,
        persisted: Option<&str>,
        recomputed: Option<&str>,
    ) -> Divergence {
        Divergence::Cell {
            key: key.to_string(),
            column: column.to_string(),
            persisted: persisted.map(str::to_string),
            recomputed: recomputed.map(str::to_string),
        }
    }

    #[test]
    fn a_cursor_names_its_part_and_key() {
        assert_eq!(parse_cursor(None), (Part::Target, None));
        assert_eq!(parse_cursor(Some("p")), (Part::From, None));
        assert_eq!(
            parse_cursor(Some("m:1:2")),
            (Part::Joined, Some("1:2".to_string()))
        );
        assert_eq!(
            parse_cursor(Some("j:")),
            (Part::Matches, Some(String::new()))
        );
        assert_eq!(
            parse_cursor(Some("x:5")),
            (Part::Target, Some("x:5".to_string()))
        );
    }

    #[test]
    fn the_walk_runs_every_part_in_order_and_ends_after_j() {
        let mut cursor = None;
        let mut parts = Vec::new();
        loop {
            let (part, _) = parse_cursor(cursor.as_deref());
            parts.push(part);
            // One page a part, reaching its end; a page that ended at a key
            // stays in its part.
            assert_eq!(
                next_cursor(part, Some("k".into())),
                Some(format!("{}:k", part.tag()))
            );
            cursor = next_cursor(part, None);
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(
            parts,
            [Part::Target, Part::From, Part::Joined, Part::Matches]
        );
    }

    #[test]
    fn partials_differ_per_cell_and_a_one_sided_key_is_zeros() {
        let columns = ["p.__n".to_string(), "p.__count_1".to_string()];
        let mut out = Vec::new();
        diff_partials(
            &columns,
            &side(&[
                ("1", &[Some("2"), Some("1")]),
                ("2", &[Some("1"), Some("1")]),
            ]),
            &side(&[
                ("1", &[Some("2"), Some("2")]),
                ("3", &[Some("0"), Some("4")]),
            ]),
            &mut out,
        );
        assert_eq!(
            out,
            [
                cell("1", "p.__count_1", Some("2"), Some("1")),
                cell("2", "p.__n", Some("0"), Some("1")),
                cell("2", "p.__count_1", Some("0"), Some("1")),
                cell("3", "p.__count_1", Some("4"), Some("0")),
            ]
        );
    }

    #[test]
    fn rows_differ_by_cell_missing_or_extra_and_a_paused_field_is_skipped() {
        let mut out = Vec::new();
        let paused = HashSet::from(["q".to_string()]);
        diff_rows(
            &["n", "q"],
            &paused,
            &side(&[("1", &[Some("2"), Some("5")]), ("2", &[Some("1"), None])]),
            &side(&[("1", &[Some("3"), Some("6")]), ("9", &[Some("1"), None])]),
            &mut out,
        );
        assert_eq!(
            out,
            [
                cell("1", "n", Some("3"), Some("2")),
                Divergence::MissingRow { key: "2".into() },
                Divergence::ExtraRow { key: "9".into() },
            ]
        );
    }

    #[test]
    fn only_a_sum_or_average_argument_keeps_a_sum() {
        let def = crate::defs::parse(
            "TRANSFORM t FROM a JOIN b ON a.k = b.k GROUP BY a.g \
             SELECT a.g AS g, COUNT(a.x) AS cx, AVG(a.y) AS ay, COALESCE(SUM(b.z), 0) AS sz, \
                    COUNT(b.w) AS cw",
        )
        .unwrap();
        let from = spec::side_arguments(&def, JoinSide::From);
        let joined = spec::side_arguments(&def, JoinSide::Joined);
        assert_eq!(summed(&def, &from), [false, true]);
        assert_eq!(summed(&def, &joined), [true, false]);
        assert_eq!(
            partial_columns(&[false, true]),
            ["__n", "__count_1", "__count_2", "__sum_2"]
        );
    }
}
