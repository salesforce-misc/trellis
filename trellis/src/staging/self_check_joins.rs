//! `self_check` of a `JOIN` target (#1112, ADR-0013 "Scope", ADR-0019).
//!
//! A `JOIN` target is audited in four parts, walked in this order, each a
//! keyset walk of its own keys:
//!
//! 1. **`T` and the visible target**, a page of target groups, by the group
//!    key text (`ddl::pk_key_sql_expr` over the target's identity). One
//!    statement reads, under one snapshot, the definition's own `JOIN …
//!    GROUP BY` over the two tables (each group's row count, each
//!    argument's non-null count and, for a summed argument, its sum, and
//!    each visible field as the definition writes it), `T`'s partials, and
//!    the visible rows. `T` is compared against the query's partials, which
//!    are `Σ_k P ⊗ M` over the sides as they are (the cross terms of
//!    ADR-0019, `LEFT`'s unmatched rows included), and the visible rows
//!    against the query's rows: a group the query has and the target lacks,
//!    or the reverse, is a missing or extra row.
//! 2. **`P`**, a page of `FROM`-side cells (its `GROUP BY` columns and join
//!    key), against a `GROUP BY` of the `FROM` table over those columns.
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
//!
//! The walk's cursor ([`super::self_check::SelfCheckScope::after`]) carries
//! the part: `t:<key>`, `p:<key>`, `m:<key>` or `j:<key>` continues a part
//! after a key, and a bare `p`, `m` or `j` starts one. The first page of a
//! walk (no cursor) starts the target's part, and the last part's end ends
//! the walk.
//!
//! It reports, never repairs (ADR-0013, #1096 decision 3): the remedy is a
//! `request_backfill` or a `PAUSE`/`RESUME`. Like every page, it is read only
//! after `super::self_check::audit`'s convergence wait, and under
//! `SelfCheckMode::Standard` a divergence is reported only if it reproduces
//! after a second one. That re-check matches a divergence by its key and
//! column, so a hot group whose value differs on both passes is reported,
//! as on any other target (#1140). The hidden basis and stamp aren't read.
//!
//! The SQL here is rendered from the definition separately from the apply
//! path's (`super::join`): the query is the definition's own `JOIN …
//! GROUP BY`, so a rendering bug in the product the apply computes
//! disagrees with it instead of agreeing with itself.
//!
//! Cost: each part's source side scans its table (the target's part, the
//! join of both) per page, aggregating only the keys up to the hidden
//! side's last key, so about a page of groups.

use std::collections::{BTreeSet, HashMap, HashSet};

use tokio_postgres::types::PgLsn;

use crate::defs::ast::{Expr, JoinKind, KeySpace, Operator, Predicate, TransformDef};
use crate::defs::catalog;
use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::defs::join::{self as spec, HiddenTable, Side as JoinSide};
use crate::defs::model::Definition;
use crate::pool::{Pool, quote_ident};

use super::self_check::{
    BYTE_ORDER, ComparePass, Divergence, SelfCheckError, SelfCheckScope, render_leaf,
};
use super::self_check_groups::{Side, partial_text};

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
        Expr::RelationshipPath { rel, column } => {
            return Err(format!(
                "reads the relationship path '{rel}.{column}', which a JOIN definition can't hold"
            ));
        }
        Expr::BinaryOp { op, lhs, rhs } => {
            let symbol = match op {
                Operator::Add => "+",
                Operator::GreaterThan => ">",
            };
            format!(
                "({} {symbol} {})",
                render(def, lhs, from, joined)?,
                render(def, rhs, from, joined)?
            )
        }
        Expr::FunctionCall { name, args } if name == "COUNT" && args.is_empty() => {
            "count(*)".to_string()
        }
        Expr::FunctionCall { name, args } => {
            let mut rendered = Vec::with_capacity(args.len());
            for arg in args {
                rendered.push(render(def, arg, from, joined)?);
            }
            format!("{}({})", name.to_lowercase(), rendered.join(", "))
        }
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::TypedLiteral { .. } => {
            render_leaf(expr)?
        }
    })
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
    for field in value_fields(&def.def) {
        render(&def.def, &field.expr, "a", "b").map_err(|detail| {
            SelfCheckError::UnsupportedExpr {
                target: def.def.target.clone(),
                detail,
            }
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

/// The source side's partials over `args` (rendered), in
/// [`partial_columns`]' order, as text.
fn source_partials(args: &[String], summed: &[bool]) -> Vec<String> {
    let mut parts = vec![partial_text("__n", "count(*)")];
    for (i, (arg, summed)) in args.iter().zip(summed).enumerate() {
        parts.push(partial_text(
            &format!("__count_{}", i + 1),
            &format!("count({arg})"),
        ));
        if *summed {
            parts.push(partial_text(
                &format!("__sum_{}", i + 1),
                &format!("coalesce(sum(coalesce({arg}, 0)::numeric), 0)"),
            ));
        }
    }
    parts
}

/// Key columns `names`, for [`ddl::pk_key_sql_expr`]: nullable, so a `NULL`
/// part encodes as the ring's key text does.
fn key_columns(names: &[String]) -> Vec<PrimaryKeyColumn> {
    names
        .iter()
        .map(|name| PrimaryKeyColumn {
            name: name.clone(),
            data_type: String::new(),
            nullable: true,
            collation: None,
        })
        .collect()
}

/// The key text of `key` read through `alias`, under byte order.
fn key_sql(key: &[PrimaryKeyColumn], alias: &str) -> String {
    format!(
        "({}) collate {BYTE_ORDER}",
        ddl::pk_key_sql_expr(key, Some(alias))
    )
}

/// `$1`, the cursor's key, bounds `key` from below.
fn after_sql(key: &str) -> String {
    format!("($1::text is null or {key} > $1)")
}

/// The statement of a page of `P`, `M` or `J`: `$1` is the cursor's key,
/// `$2` the limit; each row is `(side, page_end, key, values)`, side 1 the
/// source's and 2 the hidden table's, as `super::self_check_groups`' page.
/// `source_from` names the source rows as `__s`, carrying `key`'s columns,
/// and `source_where` (`and …`, or empty) filters them.
fn partials_page_sql(
    key: &[PrimaryKeyColumn],
    hidden: &str,
    hidden_values: &[String],
    source_from: &str,
    source_where: &str,
    source_values: &[String],
) -> String {
    let hk = key_sql(key, "__h");
    let sk = key_sql(key, "__s");
    let group: Vec<String> = key
        .iter()
        .map(|c| format!("__s.{}", quote_ident(&c.name)))
        .collect();
    format!(
        "with hid as materialized ( \
             select {hk} as __k, array[{hv}]::text[] as __v from {hidden} __h \
             where {h_after} order by __k limit $2), \
         hid_end as (select max(__k) as e from hid having count(*) >= $2), \
         src as materialized ( \
             select {sk} as __k, array[{sv}]::text[] as __v from {source_from} \
             where {s_after} and ((select e from hid_end) is null or {sk} <= (select e from hid_end)) \
                   {source_where} \
             group by {group} order by __k limit $2), \
         page as ( \
             select least((select max(__k) from src having count(*) >= $2), \
                          (select e from hid_end)) as page_end) \
         select 1, page.page_end, src.__k, src.__v from src, page \
          where page.page_end is null or src.__k <= page.page_end \
         union all \
         select 2, page.page_end, hid.__k, hid.__v from hid, page \
          where page.page_end is null or hid.__k <= page.page_end",
        hv = hidden_values.join(", "),
        h_after = after_sql(&hk),
        sv = source_values.join(", "),
        s_after = after_sql(&sk),
        group = group.join(", "),
    )
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

    fn render_args(&self, args: &[Expr], alias: &str) -> Result<Vec<String>, SelfCheckError> {
        args.iter()
            .map(|arg| {
                render(self.def, arg, alias, alias).map_err(|detail| {
                    SelfCheckError::UnsupportedExpr {
                        target: self.def.target.clone(),
                        detail,
                    }
                })
            })
            .collect()
    }

    /// `P` or `M`'s page statement, and its partial columns.
    fn side_page(&self, side: JoinSide) -> Result<(String, Vec<String>), SelfCheckError> {
        let (part, table, args, summed) = match side {
            JoinSide::From => (
                Part::From,
                &self.from_table,
                &self.from_args,
                &self.from_summed,
            ),
            JoinSide::Joined => (
                Part::Joined,
                &self.joined_table,
                &self.joined_args,
                &self.joined_summed,
            ),
        };
        let key = key_columns(&spec::side_key_columns(self.def, side));
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
        let sql = partials_page_sql(
            &key,
            &self.hidden(part),
            &Self::hidden_values(&columns),
            &format!("{table} __s"),
            &source_where,
            &source_partials(&self.render_args(args, "__s")?, summed),
        );
        Ok((sql, columns))
    }

    /// `J`'s page statement: each join key's `__matches` against the joined
    /// table's rows for it, read under `J`'s column names.
    fn matches_page(&self) -> String {
        let j_cols = spec::join_key(self.def, JoinSide::From);
        let joined_cols = spec::join_key(self.def, JoinSide::Joined);
        let renamed: Vec<String> = joined_cols
            .iter()
            .zip(&j_cols)
            .map(|(b, j)| format!("__b.{} as {}", quote_ident(b), quote_ident(j)))
            .collect();
        let not_null: String = j_cols
            .iter()
            .map(|c| format!(" and __s.{} is not null", quote_ident(c)))
            .collect();
        partials_page_sql(
            &key_columns(&j_cols),
            &self.hidden(Part::Matches),
            &["__matches::text".to_string()],
            &format!(
                "{} __b cross join lateral (select {}) __s",
                self.joined_table,
                renamed.join(", ")
            ),
            &not_null,
            &["count(*)::text".to_string()],
        )
    }

    /// The target part's statement (the module doc): each row is `(side,
    /// page_end, key, values)`, side 1 the query's partials, 2 `T`'s, 3 the
    /// query's visible rows and 4 the target's. `identity` is the target's
    /// group key; `field_types` the visible fields' column types.
    fn target_page(
        &self,
        identity: &[PrimaryKeyColumn],
        fields: &[&crate::defs::ast::FieldDef],
        field_types: &HashMap<String, String>,
    ) -> Result<(String, Vec<String>), SelfCheckError> {
        let join = join_of(self.def);
        let unsupported = |detail: String| SelfCheckError::UnsupportedExpr {
            target: self.def.target.clone(),
            detail,
        };
        let alias = |table: &str| match side_of(self.def, table) {
            Some(JoinSide::Joined) => "__b",
            _ => "__a",
        };
        let hk = key_sql(identity, "__h");
        let gk = key_sql(identity, "__g");
        let vk = key_sql(identity, "__t");

        // The query's partials: the `FROM` side's arguments, then the joined
        // side's, as `T` numbers them.
        let mut args = Vec::with_capacity(self.from_args.len() + self.joined_args.len());
        for arg in self.from_args.iter().chain(&self.joined_args) {
            args.push(render(self.def, arg, "__a", "__b").map_err(unsupported)?);
        }
        let summed: Vec<bool> = self
            .from_summed
            .iter()
            .chain(&self.joined_summed)
            .copied()
            .collect();
        let columns = partial_columns(&summed);
        let mut oracle_fields = Vec::with_capacity(fields.len());
        for field in fields {
            let pg_type = field_types.get(&field.name).map_or("text", String::as_str);
            oracle_fields.push(format!(
                "(({})::{pg_type})::text",
                render(self.def, &field.expr, "__a", "__b").map_err(unsupported)?
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
        let kind = match join.kind {
            JoinKind::Inner => "join",
            JoinKind::Left => "left join",
        };
        let visible: Vec<String> = fields
            .iter()
            .map(|f| format!("{}::text", quote_ident(&f.name)))
            .collect();
        let sql = format!(
            "with hid as materialized ( \
                 select {hk} as __k, array[{hv}]::text[] as __v from {t} __h \
                 where {h_after} order by __k limit $2), \
             hid_end as (select max(__k) as e from hid having count(*) >= $2), \
             src as materialized ( \
                 select {gk} as __k, array[{sv}]::text[] as __v, array[{sf}]::text[] as __f \
                 from {a} __a {kind} {b} __b on {on} \
                 cross join lateral (select {groups}) __g \
                 where {g_after} \
                   and ((select e from hid_end) is null or {gk} <= (select e from hid_end)) \
                 group by {group_by} order by __k limit $2), \
             vis as materialized ( \
                 select {vk} as __k, array[{vv}]::text[] as __v from {target} __t \
                 where {v_after} order by __k limit $2), \
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
             select 3, page.page_end, src.__k, src.__f from src, page \
              where page.page_end is null or src.__k <= page.page_end \
             union all \
             select 4, page.page_end, vis.__k, vis.__v from vis, page \
              where page.page_end is null or vis.__k <= page.page_end",
            hv = Self::hidden_values(&columns).join(", "),
            t = self.hidden(Part::Target),
            h_after = after_sql(&hk),
            sv = source_partials(&args, &summed).join(", "),
            sf = oracle_fields.join(", "),
            a = self.from_table,
            b = self.joined_table,
            on = on.join(" and "),
            groups = groups.join(", "),
            g_after = after_sql(&gk),
            group_by = group_by.join(", "),
            vv = visible.join(", "),
            target = self.target,
            v_after = after_sql(&vk),
        );
        Ok((sql, columns))
    }
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
    let client = pool.get().await?;
    let (joined, _) = catalog::join_side_of(&**client, def.id)
        .await?
        .ok_or_else(|| SelfCheckError::UnsupportedExpr {
            target: def.def.target.clone(),
            detail: "a JOIN definition with no joined table recorded".to_string(),
        })?;
    let from_args = spec::side_arguments(&def.def, JoinSide::From);
    let joined_args = spec::side_arguments(&def.def, JoinSide::Joined);
    let audit = Audit {
        def: &def.def,
        id: def.id,
        schema: pool.schema().to_string(),
        from_table: ddl::qualified_source_table(&def.source_table),
        joined_table: ddl::qualified_source_table(&joined),
        target: ddl::qualified_target_table_ident(&def.target_table),
        from_summed: summed(&def.def, &from_args),
        joined_summed: summed(&def.def, &joined_args),
        from_args,
        joined_args,
    };

    let (part, after) = parse_cursor(scope.after.as_deref());
    let fields = value_fields(&def.def);
    let field_types = if part == Part::Target {
        super::join::target_column_types(&**client, &def.target_table).await?
    } else {
        HashMap::new()
    };
    let (sql, columns) = match part {
        Part::Target => audit.target_page(identity, &fields, &field_types)?,
        Part::From => audit.side_page(JoinSide::From)?,
        Part::Joined => audit.side_page(JoinSide::Joined)?,
        Part::Matches => (audit.matches_page(), vec!["__matches".to_string()]),
    };
    let after: Option<&str> = after.as_deref();
    let rows = client.query(sql.as_str(), &[&after, &scope.limit]).await?;

    let mut sides: [Side; 4] = Default::default();
    let mut page_end: Option<String> = None;
    for row in &rows {
        let side: i32 = row.get(0);
        page_end = row.get(1);
        sides[(side - 1) as usize].insert(row.get(2), row.get(3));
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
    }
    Ok(ComparePass {
        checked_through,
        rows_compared: keys.len() as i64,
        next_after: next_cursor(part, page_end),
        divergences,
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
