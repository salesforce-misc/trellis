//! Typed keyset paging for a `self_check` walk (#1178, ADR-0013 "How a walk
//! pages").
//!
//! A page selects its keys by a comparison on the key's raw columns,
//! `(k1, …, kn) > (c1, …, cn) order by k1, …, kn limit L`, on both sides of
//! the comparison, so an index that leads with those columns serves the page
//! with a range scan, and a walk reads each table about once, not once per
//! page. The encoded key text ([`ddl::pk_key_sql_expr`]) stays the identity a
//! divergence is reported by and the walk cursor's persisted form
//! ([`SelfCheckScope::after`](super::self_check::SelfCheckScope)); it no longer
//! orders or bounds a page. [`Keyset::params`] decodes a cursor back into its
//! columns' values.
//!
//! **Order.** Both sides order by the same expressions: each column
//! ascending, `NULL` last (the default a btree index holds), and each
//! collatable column under one collation named in the statement, so a source
//! and a target whose columns are collated differently still page in one
//! order. The collation is the key index's own when that is deterministic
//! (so its index serves the page) and `"C"` when it is not: under a
//! nondeterministic one two distinct keys could compare equal and a page's
//! `limit` could cut between them.
//!
//! **Trimming.** Each side is `limit`ed separately, so whenever the sides'
//! key sets differ their pages end at different keys. A page ends at the
//! lowest last key of the sides that filled the limit, and keys past it are
//! left for the next page. The sides' keys are ranked together
//! ([`Keyset::ranked`]): a dense rank over the typed order, in the one
//! statement, so "past the end" is an integer comparison however the key
//! sorts, and the end's key text is read off the row that ranks last.

use std::fmt::Write as _;

use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::pool::{quote_ident, quote_literal};

use super::self_check::SelfCheckError;

/// `"C"`, byte order.
pub(super) const BYTE_ORDER: &str = r#"pg_catalog."C""#;

/// A key's columns, in the order a page sorts them.
#[derive(Debug, Clone)]
pub(super) struct Keyset {
    /// The key as the ring encodes it, to decode a cursor.
    pk: Vec<PrimaryKeyColumn>,
    /// Each column's collation in a page's statement, `None` for a type with
    /// none.
    collations: Vec<Option<String>>,
    /// The cursor's columns' values, `None` for the first page. A page's
    /// statement is built for its cursor.
    cursor: Option<Vec<Option<String>>>,
}

impl Keyset {
    /// `pk`'s columns, each collation resolved against the catalog
    /// ([`Keyset`]'s "Order").
    pub(super) async fn resolve(
        client: &tokio_postgres::Client,
        pk: &[PrimaryKeyColumn],
    ) -> Result<Keyset, SelfCheckError> {
        let mut collations = Vec::with_capacity(pk.len());
        for column in pk {
            let Some(collation) = column.collation.as_deref() else {
                collations.push(None);
                continue;
            };
            let deterministic: Option<bool> = client
                .query_opt(
                    "select co.collisdeterministic from pg_catalog.pg_collation co \
                     where co.oid = pg_catalog.to_regcollation($1)",
                    &[&collation],
                )
                .await?
                .map(|row| row.get(0));
            collations.push(Some(match deterministic {
                Some(true) => collation.to_string(),
                _ => BYTE_ORDER.to_string(),
            }));
        }
        Ok(Keyset {
            pk: pk.to_vec(),
            collations,
            cursor: None,
        })
    }

    /// The keyset over `names`, the columns of `table` (a quoted, qualified
    /// name), nullable: the types and collations read from the catalog. For a
    /// key that is no table's index, such as a `JOIN` part's cell or group.
    pub(super) async fn resolve_columns(
        client: &tokio_postgres::Client,
        table: &str,
        names: &[String],
    ) -> Result<Keyset, SelfCheckError> {
        let rows = client
            .query(
                "select u.name, pg_catalog.format_type(a.atttypid, a.atttypmod), \
                        (select pg_catalog.format('%I.%I', n.nspname, co.collname) \
                         from pg_catalog.pg_collation co \
                         join pg_catalog.pg_namespace n on n.oid = co.collnamespace \
                         where co.oid = a.attcollation) \
                 from unnest($2::text[]) with ordinality as u(name, ord) \
                 join pg_catalog.pg_attribute a \
                   on a.attrelid = pg_catalog.to_regclass($1) and a.attname = u.name \
                  and not a.attisdropped \
                 order by u.ord",
                &[&table, &names],
            )
            .await?;
        let pk: Vec<PrimaryKeyColumn> = rows
            .iter()
            .map(|row| PrimaryKeyColumn {
                name: row.get(0),
                data_type: row.get(1),
                nullable: true,
                collation: row.get(2),
            })
            .collect();
        Self::resolve(client, &pk).await
    }

    /// A keyset over `pk` with each column's collation as given. For tests of
    /// the statements, which need no catalog.
    #[cfg(test)]
    pub(super) fn with_collations(
        pk: &[PrimaryKeyColumn],
        collations: Vec<Option<String>>,
    ) -> Self {
        Keyset {
            pk: pk.to_vec(),
            collations,
            cursor: None,
        }
    }

    /// The key's columns.
    pub(super) fn columns(&self) -> &[PrimaryKeyColumn] {
        &self.pk
    }

    /// The columns read through `alias` (or bare), quoted.
    pub(super) fn refs(&self, alias: Option<&str>) -> Vec<String> {
        self.pk
            .iter()
            .map(|c| match alias {
                Some(a) => format!("{a}.{}", quote_ident(&c.name)),
                None => quote_ident(&c.name),
            })
            .collect()
    }

    /// The columns of a CTE that carries them as `__c0`, `__c1`, …, read
    /// through `alias` (or bare).
    pub(super) fn carried(&self, alias: Option<&str>) -> Vec<String> {
        (0..self.pk.len())
            .map(|i| match alias {
                Some(a) => format!("{a}.__c{i}"),
                None => format!("__c{i}"),
            })
            .collect()
    }

    /// `refs` as the select-list columns `__c0`, `__c1`, …, each led by a
    /// comma: `, "id" as __c0`.
    pub(super) fn select_carried(&self, refs: &[String]) -> String {
        refs.iter()
            .enumerate()
            .map(|(i, r)| format!(", {r} as __c{i}"))
            .collect()
    }

    /// One column of a comparison: `expr` under its collation.
    fn collated(&self, i: usize, expr: &str) -> String {
        match &self.collations[i] {
            Some(collation) => format!("({expr} collate {collation})"),
            None => expr.to_string(),
        }
    }

    /// The `order by` list for `exprs` (one per column).
    pub(super) fn order_by(&self, exprs: &[String]) -> String {
        exprs
            .iter()
            .enumerate()
            .map(|(i, e)| match &self.collations[i] {
                Some(collation) => format!("{e} collate {collation}"),
                None => e.clone(),
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Whether no column can be `NULL`, so a row comparison orders the key.
    fn all_not_null(&self) -> bool {
        self.pk.iter().all(|c| !c.nullable)
    }

    /// `lhs > rhs` in the page's order (`NULL` last), as a boolean that is
    /// never `NULL`. `rhs` are values of the columns' types; one may be
    /// `NULL` only for a nullable column.
    pub(super) fn greater(&self, lhs: &[String], rhs: &[String]) -> String {
        if self.all_not_null() {
            let l: Vec<String> = lhs
                .iter()
                .enumerate()
                .map(|(i, e)| self.collated(i, e))
                .collect();
            return format!("(({}) > ({}))", l.join(", "), rhs.join(", "));
        }
        let n = lhs.len();
        let mut out = String::new();
        let mut closers = 0;
        for i in 0..n {
            let l = self.collated(i, &lhs[i]);
            let r = &rhs[i];
            // NULL sorts last: nothing is after a NULL cursor part, and a NULL
            // value is after every other.
            let gt = format!(
                "(case when {r} is null then false else ({l} > {r} or {} is null) end)",
                lhs[i]
            );
            if i + 1 == n {
                let _ = write!(out, "{gt}");
            } else {
                let _ = write!(out, "({gt} or ({l} is not distinct from {r} and ");
                closers += 1;
            }
        }
        out.push_str(&")".repeat(closers * 2));
        format!("({out})")
    }

    /// `lhs <= rhs` in the page's order, never `NULL`.
    pub(super) fn at_most(&self, lhs: &[String], rhs: &[String]) -> String {
        format!("(not {})", self.greater(lhs, rhs))
    }

    /// Whether `table` (a quoted, qualified name) has a valid, complete
    /// btree index whose first column is `column`, which can order a page of
    /// that key.
    pub(super) async fn index_leads_with(
        client: &tokio_postgres::Client,
        table: &str,
        column: &str,
    ) -> Result<bool, SelfCheckError> {
        Ok(client
            .query_one(
                "select exists ( \
                   select 1 from pg_catalog.pg_index i \
                   join pg_catalog.pg_class c on c.oid = i.indexrelid \
                   join pg_catalog.pg_am am on am.oid = c.relam and am.amname = 'btree' \
                   join pg_catalog.pg_attribute a \
                     on a.attrelid = i.indrelid and a.attnum = i.indkey[0] \
                  where i.indrelid = pg_catalog.to_regclass($1) \
                    and i.indisvalid and i.indpred is null and a.attname = $2)",
                &[&table, &column],
            )
            .await?
            .get(0))
    }

    /// Runs a page's statement `sql` with `limit` for `$1`. `steer` says an
    /// index orders the source's page: the planner then costs a grouped page
    /// by the groups it counts in the weighted union of the source and its
    /// ring rows, which it can't estimate (a default 200), and picks a hash
    /// aggregate of the whole source over the index scan that ends after a
    /// page of groups. So the statement runs without hash aggregation, which
    /// leaves the ordered `GroupAggregate`.
    pub(super) async fn run(
        client: &mut tokio_postgres::Client,
        sql: &str,
        limit: i64,
        steer: bool,
    ) -> Result<Vec<tokio_postgres::Row>, SelfCheckError> {
        if !steer {
            return Ok(client.query(sql, &[&limit]).await?);
        }
        let txn = client.transaction().await?;
        txn.batch_execute("set local enable_hashagg to off").await?;
        let rows = txn.query(sql, &[&limit]).await?;
        txn.commit().await?;
        Ok(rows)
    }

    /// Sets the cursor the page continues after: its key text, decoded into
    /// the columns' values.
    pub(super) fn set_cursor(&mut self, after: Option<&str>) -> Result<(), SelfCheckError> {
        self.cursor = match after {
            None => None,
            Some(after) => {
                let parts = ddl::split_pk_key(&self.pk, "self_check cursor", after)?;
                Some(
                    parts
                        .into_iter()
                        .map(|p| p.map(|p| p.into_owned()))
                        .collect(),
                )
            }
        };
        Ok(())
    }

    /// The conditions, one per branch, that together say a row of `refs` is
    /// past the cursor ([`Keyset::side`] unions a branch each).
    ///
    /// A key none of whose columns can be `NULL` is one row comparison. With
    /// a nullable column the rows past the cursor are no single range: `NULL`
    /// sorts last, so `k > c` leaves out the rows with `k` `NULL` that come
    /// after it. They are a branch of their own, which is still a range of an
    /// index on the key, whereas one condition covering both
    /// (`k > c or k is null`) is none. The branches are disjoint, one for
    /// each way a row can first differ from the cursor, in a column, and a
    /// page unions them and orders the union.
    fn branches(&self, refs: &[String]) -> Vec<String> {
        let Some(cursor) = &self.cursor else {
            return vec!["true".to_string()];
        };
        let literal = |i: usize| {
            format!(
                "{}::{}",
                quote_literal(cursor[i].as_deref().expect("a non-null part")),
                self.pk[i].data_type
            )
        };
        let mut out = Vec::new();
        let row_comparison = cursor.iter().all(Option::is_some);
        if row_comparison {
            let lhs: Vec<String> = refs
                .iter()
                .enumerate()
                .map(|(i, r)| self.collated(i, r))
                .collect();
            let rhs: Vec<String> = (0..refs.len()).map(literal).collect();
            out.push(format!("(({}) > ({}))", lhs.join(", "), rhs.join(", ")));
        }
        for i in 0..refs.len() {
            if cursor[i].is_none() {
                continue;
            }
            let mut prefix: Vec<String> = (0..i)
                .map(|t| match &cursor[t] {
                    Some(_) => format!("{} = {}", self.collated(t, &refs[t]), literal(t)),
                    None => format!("{} is null", refs[t]),
                })
                .collect();
            if !row_comparison {
                let mut greater = prefix.clone();
                greater.push(format!("{} > {}", self.collated(i, &refs[i]), literal(i)));
                out.push(format!("({})", greater.join(" and ")));
            }
            if self.pk[i].nullable {
                prefix.push(format!("{} is null", refs[i]));
                out.push(format!("({})", prefix.join(" and ")));
            }
        }
        if out.is_empty() {
            out.push("false".to_string());
        }
        out
    }

    /// One side of a page: `select` is the side's `select … where {cond}
    /// order by … limit $1`, built for a condition. The side is the first
    /// `limit` rows past the cursor in the key's order, each branch of the
    /// cursor's conditions read on its own and the union cut again.
    pub(super) fn side(&self, refs: &[String], select: impl Fn(&str) -> String) -> String {
        let branches = self.branches(refs);
        if let [only] = branches.as_slice() {
            return select(only);
        }
        let union = branches
            .iter()
            .map(|cond| format!("({})", select(cond)))
            .collect::<Vec<_>>()
            .join(" union all ");
        format!(
            "select * from ({union}) u order by {} limit $1",
            self.order_by(&self.carried(None))
        )
    }

    /// The tail of a page's statement: ranks the keys of every `side` in
    /// the page's order, finds where the page ends, and selects the rows up
    /// to there.
    ///
    /// Each `side` is the name of a CTE with the columns `__k` (the key
    /// text), `__c0…` ([`Keyset::select_carried`]) and `__v` (`text[]`, the
    /// row's payload), and the number it reports its rows as; `bounds` are
    /// the sides whose last key can end the page, if they filled the limit
    /// (`$1`). Appends the CTEs `allk`, `ranked` and `page` to `ctes` (which
    /// ends `, `) and returns the statement over them: each row is `(side,
    /// page_end, key, payload)`, `page_end` the key text where the page
    /// ends, or `NULL` when no bounding side filled the limit. `extra` is
    /// more `union all` branches (each beginning ` union all `) over `page`,
    /// whose `page.page_end` they may read.
    pub(super) fn ranked(
        &self,
        ctes: &str,
        sides: &[(i32, &str)],
        bounds: &[i32],
        extra: &str,
    ) -> String {
        let all: Vec<String> = sides
            .iter()
            .map(|(n, cte)| {
                let carried: String = (0..self.pk.len()).map(|i| format!(", __c{i}")).collect();
                format!("select {n}::int as __side, __k{carried}, __v from {cte}")
            })
            .collect();
        let order = self.order_by(&self.carried(None));
        let ends: Vec<String> = bounds
            .iter()
            .map(|n| {
                format!("(select max(__r) from ranked where __side = {n} having count(*) >= $1)")
            })
            .collect();
        format!(
            "with {ctes}\
             allk as ({all}), \
             ranked as (select a.*, dense_rank() over (order by {order}) as __r from allk a), \
             page as ( \
                select e.pe_r, (select __k from ranked where __r = e.pe_r limit 1) as page_end \
                from (select least({ends}) as pe_r) e) \
             select r.__side, page.page_end, r.__k, r.__v from ranked r, page \
              where page.pe_r is null or r.__r <= page.pe_r{extra}",
            all = all.join(" union all "),
            ends = ends.join(", "),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, ty: &str, nullable: bool) -> PrimaryKeyColumn {
        PrimaryKeyColumn {
            name: name.to_string(),
            data_type: ty.to_string(),
            nullable,
            collation: None,
        }
    }

    fn keyset(
        pk: &[PrimaryKeyColumn],
        collations: Vec<Option<String>>,
        after: Option<&str>,
    ) -> Keyset {
        let mut ks = Keyset::with_collations(pk, collations);
        ks.set_cursor(after).expect("a cursor");
        ks
    }

    #[test]
    fn a_not_null_key_pages_by_a_row_comparison() {
        let ks = keyset(
            &[col("a", "bigint", false), col("b", "text", false)],
            vec![None, Some(r#"pg_catalog."C""#.to_string())],
            Some("5\u{1f}x"),
        );
        assert_eq!(
            ks.branches(&ks.refs(None)),
            vec![r#"(("a", ("b" collate pg_catalog."C")) > ('5'::bigint, 'x'::text))"#.to_string()]
        );
        assert_eq!(
            ks.order_by(&ks.refs(None)),
            r#""a", "b" collate pg_catalog."C""#
        );
    }

    #[test]
    fn a_nullable_column_adds_a_branch_for_the_nulls_after_the_cursor() {
        let pk = [col("a", "integer", true)];
        let after = keyset(&pk, vec![None], Some("5"));
        assert_eq!(
            after.branches(&after.refs(None)),
            vec![
                "((\"a\") > ('5'::integer))".to_string(),
                "(\"a\" is null)".to_string()
            ]
        );
        // After the null group there is nothing.
        let end = keyset(&pk, vec![None], Some("\u{1}"));
        assert_eq!(end.branches(&end.refs(None)), vec!["false".to_string()]);
        let first = keyset(&pk, vec![None], None);
        assert_eq!(first.branches(&first.refs(None)), vec!["true".to_string()]);
    }

    #[test]
    fn a_composite_cursor_with_a_null_part_continues_in_the_null_region() {
        let pk = [col("a", "integer", true), col("b", "integer", true)];
        // (5, NULL): after it come the rows with a > 5 and the a IS NULL ones
        // (NULL sorts last in both columns).
        let ks = keyset(&pk, vec![None, None], Some("5\u{1f}\u{1}"));
        let branches = ks.branches(&ks.refs(None));
        assert_eq!(
            branches,
            vec![
                "(\"a\" > '5'::integer)".to_string(),
                "(\"a\" is null)".to_string()
            ]
        );
    }
}
