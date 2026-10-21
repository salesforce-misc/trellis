//! Typed keyset paging for a `self_check` walk (#1178, ADR-0013 "How a walk
//! pages").
//!
//! A page selects its keys by a comparison on the key's raw columns,
//! `(k1, …, kn) > (c1, …, cn) order by k1, …, kn limit L`, on both sides of
//! the comparison, so an index that leads with those columns serves the page
//! with a range scan, and a walk reads each table about once, not once per
//! page. The encoded key text ([`ddl::pk_key_sql_expr`]) stays the identity a
//! divergence is reported by and the walk cursor's persisted form
//! ([`SelfCheckScope::after`](super::self_check::SelfCheckScope)); it orders
//! and bounds no page. [`Keyset::set_cursor`] decodes a cursor back into
//! its columns' values.
//!
//! **Order.** Both sides order by the same expressions: each column
//! ascending, `NULL` last (the default a btree index holds), and each
//! collatable column under one collation named in the statement, so a source
//! and a target whose columns are collated differently still page in one
//! order. The collation is the key column's own when that is deterministic
//! (so its index serves the page) and `"C"` when it is not: under a
//! nondeterministic one two distinct keys could compare equal and a page's
//! `limit` could cut between them. The column is the source key's for a 1-1
//! target, the target's for an aggregate and a `JOIN`'s target part, and the
//! hidden table's for `P`, `M` and `J`. The last two keep the collation define
//! made them with when the source is re-collated, so a walk that straddles that
//! pages in one order throughout.
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
                data_type: crate::defs::ddl::key_cast_type(&row.get::<_, String>(1)),
                nullable: true,
                collation: row.get(2),
            })
            .collect();
        // A column gone from the table would otherwise shrink the key, and
        // the page would compare keys of another arity than the ring's.
        if pk.len() != names.len() {
            return Err(SelfCheckError::UnsupportedExpr {
                target: table.to_string(),
                detail: format!("the key columns {names:?} are not all columns of {table}"),
            });
        }
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

/// A walk paged by [`Keyset`] against the unpaged comparison, on tables of
/// random keys: nullable and composite, `NULL`-heavy, of several types, the
/// two sides' text columns under different collations, at tiny page sizes.
#[cfg(test)]
mod walk_tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;

    /// A deterministic xorshift, so a failure reproduces.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn pick<'a>(&mut self, of: &[&'a str]) -> &'a str {
            of[(self.next() % of.len() as u64) as usize]
        }

        fn chance(&mut self, percent: u64) -> bool {
            self.next() % 100 < percent
        }
    }

    /// One key column: its type in each table, and the literals a value is
    /// drawn from (`null` among them, often).
    struct Column {
        name: &'static str,
        src_type: String,
        tgt_type: String,
        nullable: bool,
        values: Vec<&'static str>,
    }

    async fn connect(dsn: &str) -> tokio_postgres::Client {
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }

    /// A row of literals for `columns`.
    fn row(rng: &mut Rng, columns: &[Column]) -> String {
        let parts: Vec<String> = columns
            .iter()
            .map(|c| {
                let v = rng.pick(&c.values);
                if v == "null" {
                    "null".to_string()
                } else {
                    format!("{}::{}", quote_literal(v), c.src_type)
                }
            })
            .collect();
        format!("({})", parts.join(", "))
    }

    /// Each side's key texts, read whole.
    async fn keys(client: &tokio_postgres::Client, ks: &Keyset, table: &str) -> BTreeSet<String> {
        let key = ddl::pk_key_sql_expr(ks.columns(), None);
        client
            .query(&format!("select {key} from {table}"), &[])
            .await
            .expect("read the keys")
            .iter()
            .map(|r| r.get(0))
            .collect()
    }

    /// Walks `src` against `tgt`, `limit` keys a page: for each side, the
    /// page each key was read on.
    async fn walk(
        client: &mut tokio_postgres::Client,
        template: &Keyset,
        limit: i64,
    ) -> [BTreeMap<String, usize>; 2] {
        let mut seen: [BTreeMap<String, usize>; 2] = Default::default();
        let mut after: Option<String> = None;
        for page in 0..10_000 {
            let mut ks = template.clone();
            ks.set_cursor(after.as_deref()).expect("a cursor");
            let refs = ks.refs(None);
            let carried = ks.select_carried(&refs);
            let order = ks.order_by(&refs);
            let key = ddl::pk_key_sql_expr(ks.columns(), None);
            let side = |table: &str| {
                ks.side(&refs, |cond| {
                    format!(
                        "select {key} as __k{carried}, array[]::text[] as __v from {table} \
                         where {cond} order by {order} limit $1"
                    )
                })
            };
            let sql = ks.ranked(
                &format!(
                    "s as materialized ({}), t as materialized ({}), ",
                    side("src"),
                    side("tgt")
                ),
                &[(1, "s"), (2, "t")],
                &[1, 2],
                "",
            );
            let rows = Keyset::run(client, &sql, limit, false)
                .await
                .unwrap_or_else(|e| panic!("{e:?}\n{sql}"));
            let mut end: Option<String> = None;
            for r in &rows {
                let side: i32 = r.get(0);
                end = r.get(1);
                let key: String = r.get(2);
                let previous = seen[(side - 1) as usize].insert(key.clone(), page);
                assert_eq!(previous, None, "side {side} read {key:?} twice");
            }
            match end {
                None => return seen,
                Some(end) => {
                    assert_ne!(after.as_deref(), Some(end.as_str()), "no progress");
                    after = Some(end);
                }
            }
        }
        panic!("the walk did not end");
    }

    /// Builds `src` and `tgt` over `columns`, the target `src`'s keys with
    /// some dropped and some extra, and checks the walk at every page size
    /// against the unpaged sets: each key read once on each side it is in,
    /// on the same page on both sides.
    async fn check(dsn: &str, columns: &[Column], seed: u64) {
        let mut client = connect(dsn).await;
        let ddl_of = |tgt: bool| {
            columns
                .iter()
                .map(|c| {
                    let ty = if tgt { &c.tgt_type } else { &c.src_type };
                    let null = if c.nullable { "" } else { " not null" };
                    format!("{} {ty}{null}", c.name)
                })
                .collect::<Vec<_>>()
                .join(", ")
        };
        let names = columns
            .iter()
            .map(|c| c.name)
            .collect::<Vec<_>>()
            .join(", ");
        client
            .batch_execute(&format!(
                "drop table if exists src, tgt; \
                 create table src ({}); create table tgt ({}); \
                 create unique index on src ({names}) nulls not distinct; \
                 create unique index on tgt ({names}) nulls not distinct",
                ddl_of(false),
                ddl_of(true)
            ))
            .await
            .expect("create the tables");
        let mut rng = Rng(seed);
        let mut src = BTreeSet::new();
        for _ in 0..60 {
            src.insert(row(&mut rng, columns));
        }
        // The target misses some of the source's rows and has some of its
        // own.
        let mut tgt: BTreeSet<String> = src.iter().filter(|_| !rng.chance(15)).cloned().collect();
        if tgt.len() == src.len() {
            let middle = src.iter().nth(src.len() / 2).expect("a row").clone();
            tgt.remove(&middle);
        }
        for _ in 0..10 {
            let r = row(&mut rng, columns);
            if !src.contains(&r) {
                tgt.insert(r);
            }
        }
        for (table, rows) in [("src", &src), ("tgt", &tgt)] {
            for r in rows {
                client
                    .batch_execute(&format!(
                        "insert into {table} values {r} on conflict do nothing"
                    ))
                    .await
                    .expect("insert a row");
            }
        }
        let names: Vec<String> = columns.iter().map(|c| c.name.to_string()).collect();
        let mut template = Keyset::resolve_columns(&client, "public.src", &names)
            .await
            .expect("resolve the keyset");
        for (pk, c) in template.pk.iter_mut().zip(columns) {
            pk.nullable = c.nullable;
        }
        let full = [
            keys(&client, &template, "src").await,
            keys(&client, &template, "tgt").await,
        ];
        assert!(
            full[0] != full[1],
            "seed {seed} plants no divergence: {src:?} {tgt:?}"
        );
        for limit in [1, 2, 3, 4, 7, 1000] {
            let seen = walk(&mut client, &template, limit).await;
            for side in 0..2 {
                let read: BTreeSet<String> = seen[side].keys().cloned().collect();
                assert_eq!(
                    read, full[side],
                    "side {side} at limit {limit}, seed {seed}"
                );
            }
            for (key, page) in &seen[0] {
                if let Some(other) = seen[1].get(key) {
                    assert_eq!(
                        page, other,
                        "{key:?} read on two pages at limit {limit}, seed {seed}"
                    );
                }
            }
        }
    }

    fn column(
        name: &'static str,
        ty: &str,
        tgt_ty: &str,
        nullable: bool,
        values: &[&'static str],
    ) -> Column {
        Column {
            name,
            src_type: ty.to_string(),
            tgt_type: tgt_ty.to_string(),
            nullable,
            values: values.to_vec(),
        }
    }

    const TEXTS: &[&str] = &[
        "null",
        "null",
        "",
        "a",
        "A",
        "b",
        "B",
        "é",
        "E",
        "z",
        "10",
        "9",
        "a\u{1f}b",
        "\u{1}",
        "\u{1}\u{1}",
        "\u{1e}",
    ];

    #[tokio::test]
    async fn a_paged_walk_reads_what_an_unpaged_comparison_does() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_empty_database().await;
        let client = connect(db.dsn()).await;
        // A deterministic collation that orders unlike byte order, and a
        // nondeterministic one, where ICU is built in.
        let icu: bool = client
            .query_one(
                "select exists (select 1 from pg_catalog.pg_collation \
                 where collname = 'und-x-icu')",
                &[],
            )
            .await
            .expect("read the collations")
            .get(0);
        let ordered = if icu {
            client
                .batch_execute(
                    "create collation ci (provider = icu, locale = 'und-u-ks-level2', \
                     deterministic = false)",
                )
                .await
                .expect("create a nondeterministic collation");
            r#"text collate "und-x-icu""#
        } else {
            "text"
        };
        let c_text = r#"text collate "C""#;
        let ints: &[&'static str] = &["null", "null", "1", "2", "-3", "10", "9"];
        let mut shapes: Vec<Vec<Column>> = vec![
            // Nullable integer and text, the target's text under "C".
            vec![
                column("a", "integer", "integer", true, ints),
                column("b", ordered, c_text, true, TEXTS),
            ],
            // Three nullable columns over tiny domains: NULL in every
            // position of the cursor.
            vec![
                column("a", "integer", "integer", true, &["null", "1", "2"]),
                column("b", "integer", "bigint", true, &["null", "1", "2"]),
                column("c", "smallint", "smallint", true, &["null", "1", "2"]),
            ],
            // A not-null composite: one row comparison.
            vec![
                column("a", "bigint", "bigint", false, &["1", "2", "-3", "10", "9"]),
                column("b", ordered, c_text, false, &TEXTS[2..]),
            ],
            // numeric (1.0 and 1.00 are one key), timestamptz, uuid.
            vec![
                column(
                    "n",
                    "numeric",
                    "numeric",
                    true,
                    &[
                        "null", "1.0", "1.00", "-0", "0.001", "10", "9", "NaN", "Infinity",
                    ],
                ),
                column(
                    "t",
                    "timestamptz",
                    "timestamptz",
                    true,
                    &[
                        "null",
                        "2024-01-01 00:00:00+00",
                        "2024-01-01 00:00:00.000001+00",
                        "2023-12-31 23:00:00-02",
                        "infinity",
                        "-infinity",
                    ],
                ),
                column(
                    "u",
                    "uuid",
                    "uuid",
                    true,
                    &[
                        "null",
                        "00000000-0000-0000-0000-000000000000",
                        "ffffffff-ffff-ffff-ffff-ffffffffffff",
                        "7a9f0000-0000-0000-0000-000000000001",
                    ],
                ),
            ],
            // float8: NaN last but before NULL, -0 and 0 one key.
            vec![column(
                "f",
                "double precision",
                "double precision",
                true,
                &[
                    "null",
                    "NaN",
                    "Infinity",
                    "-Infinity",
                    "-0",
                    "0",
                    "0.1",
                    "1e300",
                    "-1.5",
                ],
            )],
        ];
        if icu {
            // A nondeterministic source collation (a key re-collated after
            // define, which refuses one): the walk falls back to byte order
            // on both sides. A single not-null column, since the key text of
            // any other (`replace`) can't be computed under one.
            shapes.push(vec![column(
                "t",
                r#"text collate ci"#,
                c_text,
                false,
                &TEXTS[2..],
            )]);
        }
        for (i, shape) in shapes.iter().enumerate() {
            for seed in 1..=8u64 {
                check(db.dsn(), shape, seed * 7919 + i as u64).await;
            }
        }
        // A key column the table no longer has is refused, not dropped from
        // the key.
        let gone = Keyset::resolve_columns(
            &client,
            "public.src",
            &["t".to_string(), "gone".to_string()],
        )
        .await;
        assert!(
            matches!(gone, Err(SelfCheckError::UnsupportedExpr { .. })),
            "{gone:?}"
        );
    }

    /// The hash aggregation [`Keyset::run`] turns off for a statement is on
    /// again for the connection's next one, whether the statement succeeded
    /// or failed.
    #[tokio::test]
    async fn run_leaves_hash_aggregation_as_it_found_it() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_empty_database().await;
        let mut client = connect(db.dsn()).await;
        async fn show(c: &tokio_postgres::Client) -> String {
            c.query_one("show enable_hashagg", &[])
                .await
                .expect("show")
                .get(0)
        }
        let rows = Keyset::run(
            &mut client,
            "select current_setting('enable_hashagg') from generate_series(1, $1::bigint) g",
            1,
            true,
        )
        .await
        .expect("run");
        assert_eq!(
            rows[0].get::<_, String>(0),
            "off",
            "off inside the statement"
        );
        assert_eq!(show(&client).await, "on", "on after a statement");
        let failed = Keyset::run(&mut client, "select 1 / ($1::bigint - 1)", 1, true).await;
        assert!(failed.is_err());
        assert_eq!(show(&client).await, "on", "on after a failed statement");
    }
}
