//! The orphan walk of a rebuild sweep: the target rows no live ledger entry
//! backs, a page of target rows at a time.
//!
//! A sweep's orphan pass reads the whole target once. Each batch reads the
//! next `scan` target rows in the target's own key order, from a typed
//! cursor past the last row the batch before read ([`OrphanCursor::Keyed`]),
//! so the target's key index serves the read and the anti-join probes the
//! ledger by key, whatever the key's columns and types: a key's text
//! order is the index's only for a single text column. The walk compares
//! under the collations the target's key index orders by (the #1203 lesson:
//! a source key re-collated since define orders differently from the
//! target's).
//!
//! A key with a `NULL` part (a target keyed `unique nulls not distinct` over
//! a nullable source key) is in no row comparison's range, so those rows get
//! a stage of their own once the keyed rows are read
//! ([`OrphanCursor::Nulls`]), paged by their key text. They are few: the
//! target's rows whose grouping columns are `NULL`.
//!
//! A batch only names its orphans. Its caller locks each one's entry and
//! re-derives it from the source, as a page would, and never deletes one by
//! statement: a row re-inserted and applied between the read and the delete
//! would be lost (#1481 H3).

use std::time::Instant;

use tokio_postgres::Transaction;
use tokio_postgres::types::ToSql;

use crate::defs::ddl::{self, PrimaryKeyColumn};
use crate::metrics::{self, BuildStatement};
use crate::pool::quote_ident;

use super::super::apply::ApplyError;

/// What an orphan walk reads: a target and the ledger whose live entries
/// back its rows.
pub(crate) struct OrphanWalk<'a> {
    /// The target's qualified identity (`transform_definitions.target_table`).
    pub target: &'a str,
    /// The target's own key columns, with the collations its key index
    /// orders them by (`ddl::source_primary_key` of the target).
    pub key: &'a [PrimaryKeyColumn],
    /// The ledger (`R` for a 1-1 `JOIN`), qualified and quoted.
    pub ledger: &'a str,
    /// The source table a quarantined key of the target is recorded under
    /// (`poison.src_table`).
    pub source_table: &'a str,
}

/// Where an orphan walk stands: in the keyed rows after a key (`None`: from
/// the start), or in the rows with a `NULL` key part after a key text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OrphanCursor {
    Keyed(Option<String>),
    Nulls(Option<String>),
}

/// One batch of an orphan walk ([`orphan_batch`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OrphanBatch {
    /// The orphans among the rows read, in key order.
    pub orphans: Vec<String>,
    /// The target rows the batch read.
    pub scanned: i64,
    /// Where the next batch starts, or `None` when the walk is done.
    pub next: Option<OrphanCursor>,
}

/// Reads up to `scan` target rows from `cursor` and returns those that have
/// no live entry on the ledger (none, or a tombstone) and aren't quarantined
/// for the target. See the module doc.
pub(crate) async fn orphan_batch(
    txn: &Transaction<'_>,
    walk: &OrphanWalk<'_>,
    cursor: &OrphanCursor,
    scan: i64,
) -> Result<OrphanBatch, ApplyError> {
    let statement = Statement::of(walk, cursor)?;
    let started = Instant::now();
    let row = txn
        .query_one(&statement.sql, &statement.params(walk, &scan))
        .await?;
    metrics::record_build_statement(BuildStatement::ChunkKeys, started.elapsed());
    let scanned: i64 = row.get(0);
    let last: Option<String> = row.get(1);
    let orphans: Vec<String> = row.get(2);
    let next = if scanned >= scan {
        Some(match cursor {
            OrphanCursor::Keyed(_) => OrphanCursor::Keyed(last),
            OrphanCursor::Nulls(_) => OrphanCursor::Nulls(last),
        })
    } else {
        match cursor {
            OrphanCursor::Keyed(_) if walk.key.iter().any(|c| c.nullable) => {
                Some(OrphanCursor::Nulls(None))
            }
            _ => None,
        }
    };
    Ok(OrphanBatch {
        orphans,
        scanned,
        next,
    })
}

/// One batch's statement and the cursor's parameters: `$1` the batch size,
/// `$2` and `$3` the source table and target (for `poison`), then either
/// the typed cursor's parts or the `NULL` stage's key text.
struct Statement {
    sql: String,
    /// The typed cursor's parts, in the keyed stage.
    parts: Vec<String>,
    /// In the `NULL` stage, the key text it starts after (`None` from the
    /// start).
    nulls_after: Option<Option<String>>,
}

impl Statement {
    fn of(walk: &OrphanWalk<'_>, cursor: &OrphanCursor) -> Result<Self, ApplyError> {
        let target = ddl::qualified_target_table_ident(walk.target);
        let k_tgt = ddl::pk_key_sql_expr(walk.key, Some("t"));
        let idents: Vec<String> = walk
            .key
            .iter()
            .map(|c| format!("t.{}", quote_ident(&c.name)))
            .collect();
        let any_null: Vec<String> = idents.iter().map(|c| format!("{c} is null")).collect();
        let mut parts: Vec<String> = Vec::new();
        let mut nulls_after: Option<Option<String>> = None;
        let (filter, order) = match cursor {
            OrphanCursor::Keyed(after) => {
                let mut filter = format!("not ({})", any_null.join(" or "));
                if let Some(after) = after {
                    parts = ddl::split_pk_key(walk.key, walk.target, after)?
                        .into_iter()
                        .map(|part| part.map(|c| c.into_owned()).unwrap_or_default())
                        .collect();
                    filter = format!(
                        "{filter} and {}",
                        crate::defs::backfill::pk_row_cmp(&idents, walk.key, ">", 3)
                    );
                }
                let order: Vec<String> = idents
                    .iter()
                    .zip(walk.key)
                    .map(|(ident, c)| c.ordered(ident))
                    .collect();
                (filter, order.join(", "))
            }
            OrphanCursor::Nulls(after) => {
                nulls_after = Some(after.clone());
                (
                    format!(
                        "({}) and ($4::text is null or ({k_tgt}) collate \"C\" > $4)",
                        any_null.join(" or ")
                    ),
                    format!("({k_tgt}) collate \"C\""),
                )
            }
        };
        let sql = format!(
            "with w as ( \
                 select ({k_tgt}) as __k, row_number() over (order by {order}) as __n \
                 from {target} t where {filter} order by {order} limit $1 \
             ) \
             select (select count(*) from w), \
                    (select w.__k from w order by w.__n desc limit 1), \
                    array(select w.__k from w \
                          where not exists (select 1 from {ledger} l \
                                            where l.{key} = w.__k and not l.{tombstone}) \
                            and not exists (select 1 from poison p \
                                            join transform_definitions d \
                                              on d.id = p.transform_id \
                                            where p.src_table = $2 and p.key = w.__k \
                                              and d.target_table = $3) \
                          order by w.__n)",
            ledger = walk.ledger,
            key = quote_ident(crate::defs::ledger::KEY_COLUMN),
            tombstone = quote_ident(crate::defs::ledger::TOMBSTONE_COLUMN),
        );
        Ok(Self {
            sql,
            parts,
            nulls_after,
        })
    }

    fn params<'a>(
        &'a self,
        walk: &'a OrphanWalk<'_>,
        scan: &'a i64,
    ) -> Vec<&'a (dyn ToSql + Sync)> {
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![scan, &walk.source_table, &walk.target];
        params.extend(self.parts.iter().map(|v| v as &(dyn ToSql + Sync)));
        if let Some(after) = &self.nulls_after {
            params.push(after);
        }
        params
    }
}

#[cfg(test)]
mod tests {
    use super::{OrphanCursor, OrphanWalk, Statement, orphan_batch};
    use crate::defs::ddl::PrimaryKeyColumn;
    use tokio_postgres::NoTls;

    async fn connect(dsn: &str) -> tokio_postgres::Client {
        let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!(
                "set search_path to {}, public",
                crate::config::DEFAULT_SCHEMA
            ))
            .await
            .expect("set search_path");
        client
    }

    fn column(name: &str, data_type: &str, nullable: bool) -> PrimaryKeyColumn {
        PrimaryKeyColumn {
            name: name.to_string(),
            data_type: data_type.to_string(),
            nullable,
            collation: None,
        }
    }

    /// A walk from a typed cursor reads the target through its key index
    /// from the cursor and probes the ledger by key (#1490), for an integer
    /// key and a composite one: no batch reads the whole target or the whole
    /// ledger, as a cursor on the key's text did for any key but a single
    /// text column.
    #[tokio::test]
    async fn a_batch_reads_the_target_by_its_key_index_from_the_cursor() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let client = connect(db.dsn()).await;
        client
            .batch_execute(
                "create table public.ti (id integer primary key); \
                 insert into public.ti select g from generate_series(1, 100000) g; \
                 create table public.tc (g integer not null, h text not null, \
                     primary key (g, h)); \
                 insert into public.tc select g / 10, 'h' || g from generate_series(1, 100000) g; \
                 create table public.l (__from_key text collate \"C\" primary key, \
                     __tombstone boolean not null default false); \
                 insert into public.l select g::text from generate_series(1, 80000) g; \
                 analyze public.ti, public.tc, public.l",
            )
            .await
            .expect("seed");
        for (target, key, at) in [
            (
                "public.ti",
                vec![column("id", "integer", false)],
                "id = 50000",
            ),
            (
                "public.tc",
                vec![column("g", "integer", false), column("h", "text", false)],
                "g = 5000 and h = 'h50000'",
            ),
        ] {
            let after: String = client
                .query_one(
                    &format!(
                        "select ({}) from {target} t where {at}",
                        crate::defs::ddl::pk_key_sql_expr(&key, Some("t"))
                    ),
                    &[],
                )
                .await
                .expect("the cursor's key")
                .get(0);
            let walk = OrphanWalk {
                target,
                key: &key,
                ledger: "public.l",
                source_table: "public.src",
            };
            let statement =
                Statement::of(&walk, &OrphanCursor::Keyed(Some(after))).expect("a statement");
            let scan = 100_i64;
            let plan: Vec<String> = client
                .query(
                    &format!("explain (costs off) {}", statement.sql),
                    &statement.params(&walk, &scan),
                )
                .await
                .expect("explain")
                .into_iter()
                .map(|row| row.get(0))
                .collect();
            let plan = plan.join("\n");
            let table = target.trim_start_matches("public.");
            assert!(
                plan.contains(&format!("Index Only Scan using {table}_pkey on {table} t"))
                    || plan.contains(&format!("Index Scan using {table}_pkey on {table} t")),
                "{target} is read through its key index:\n{plan}"
            );
            assert!(
                !plan.contains("Seq Scan"),
                "{target}: no batch reads a whole table:\n{plan}"
            );
        }
    }

    /// The walk visits every target row once, in key order, with a `NULL`
    /// key part's rows last: every orphan (no ledger entry, or a tombstone)
    /// comes back exactly once, and no backed row does.
    #[tokio::test]
    async fn the_walk_names_every_orphan_once_null_parts_included() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = connect(db.dsn()).await;
        client
            .batch_execute(
                "create table public.t (g integer, h text, unique nulls not distinct (g, h)); \
                 insert into public.t select g % 7, 'h' || g from generate_series(1, 40) g; \
                 insert into public.t values (null, 'x'), (1, null), (null, null), \
                     (null, 'y'); \
                 create table public.l (__from_key text collate \"C\" primary key, \
                     __tombstone boolean not null default false)",
            )
            .await
            .expect("seed");
        let key = vec![column("g", "integer", true), column("h", "text", true)];
        let k = crate::defs::ddl::pk_key_sql_expr(&key, Some("t"));
        // Every third row is backed, every fifth a tombstone, and so is one
        // `NULL`-part row.
        client
            .batch_execute(&format!(
                "insert into public.l \
                     select k, n % 5 = 0 from ( \
                         select ({k}) as k, row_number() over (order by g, h) as n \
                         from public.t t) s \
                     where n % 3 = 0 or n % 5 = 0; \
                 insert into public.l select ({k}) from public.t t \
                     where g is null and h = 'x' on conflict do nothing"
            ))
            .await
            .expect("the ledger");
        let mut expected: Vec<String> = client
            .query(
                &format!(
                    "select ({k}) from public.t t where not exists \
                     (select 1 from public.l l where l.__from_key = ({k}) and not l.__tombstone)"
                ),
                &[],
            )
            .await
            .expect("the orphans")
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        let walk = OrphanWalk {
            target: "public.t",
            key: &key,
            ledger: "public.l",
            source_table: "public.src",
        };
        let mut cursor = OrphanCursor::Keyed(None);
        let mut found: Vec<String> = Vec::new();
        let mut scanned = 0;
        for _ in 0..100 {
            let txn = client.transaction().await.expect("begin");
            let batch = orphan_batch(&txn, &walk, &cursor, 3)
                .await
                .expect("a batch");
            txn.commit().await.expect("commit");
            scanned += batch.scanned;
            found.extend(batch.orphans);
            match batch.next {
                Some(next) => cursor = next,
                None => break,
            }
        }
        assert_eq!(scanned, 44, "the walk read every target row once");
        let mut sorted = found.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), found.len(), "no orphan twice: {found:?}");
        expected.sort();
        assert_eq!(sorted, expected);
    }

    /// A text key whose index collation orders it otherwise than its bytes
    /// (the cluster's default, ICU `en-US`), the same key under `"C"`, and an
    /// `(integer, text)` key: with no ledger entry every row is an orphan, so
    /// a walk in batches of every size from 1 to 4 must name each row once,
    /// in the key index's order. Each batch runs on a fresh connection from
    /// the cursor's stored text, as a sweep resumed by another worker reads
    /// it. A walk that compares or orders under another collation than the
    /// index's, drops a key part, or repeats the row at a page boundary
    /// names a row twice, skips one, or reads them out of order.
    #[tokio::test]
    async fn the_walk_pages_a_key_in_its_index_order_from_a_stored_cursor() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let client = connect(db.dsn()).await;
        // Byte order puts `-x`, `A`..`Z`, `_x` and `a`..`z` apart; `en-US`
        // interleaves the cases and puts the punctuation first.
        let words = "('a'), ('B'), ('b'), ('A'), ('_x'), ('Z'), ('-x'), ('z'), ('ab'), ('aB'), \
                     ('Ab'), ('AB'), ('é'), ('e'), ('E'), ('f')";
        client
            .batch_execute(&format!(
                "create table public.td (id text primary key); \
                 insert into public.td values {words}; \
                 create table public.tc (id text collate \"C\" primary key); \
                 insert into public.tc values {words}; \
                 create table public.tm (g integer, h text, primary key (g, h)); \
                 insert into public.tm select n, td.id from public.td, generate_series(0, 2) n; \
                 create table public.l (__from_key text collate \"C\" primary key, \
                     __tombstone boolean not null default false)"
            ))
            .await
            .expect("seed");
        for (target, order) in [
            ("public.td", "id"),
            ("public.tc", "id"),
            ("public.tm", "g, h"),
        ] {
            let key = crate::defs::ddl::source_primary_key_in_txn(&client, target)
                .await
                .expect("the target's key");
            let k = crate::defs::ddl::pk_key_sql_expr(&key, Some("t"));
            let expected: Vec<String> = client
                .query(
                    &format!("select ({k}) from {target} t order by {order}"),
                    &[],
                )
                .await
                .expect("every row in index order")
                .into_iter()
                .map(|row| row.get(0))
                .collect();
            let walk = OrphanWalk {
                target,
                key: &key,
                ledger: "public.l",
                source_table: "public.src",
            };
            for scan in 1..=4_i64 {
                let mut stored: Option<String> = None;
                let mut found: Vec<String> = Vec::new();
                let mut scanned = 0;
                for _ in 0..200 {
                    let mut fresh = connect(db.dsn()).await;
                    let txn = fresh.transaction().await.expect("begin");
                    let batch = orphan_batch(&txn, &walk, &OrphanCursor::Keyed(stored), scan)
                        .await
                        .expect("a batch");
                    txn.commit().await.expect("commit");
                    scanned += batch.scanned;
                    found.extend(batch.orphans);
                    match batch.next {
                        Some(OrphanCursor::Keyed(next)) => stored = next,
                        Some(OrphanCursor::Nulls(_)) => {
                            panic!("{target}: no key part is nullable")
                        }
                        None => break,
                    }
                    assert!(stored.is_some(), "a full batch's cursor has a key");
                }
                assert_eq!(found, expected, "{target}, batches of {scan}");
                assert_eq!(
                    scanned,
                    expected.len() as i64,
                    "{target}, batches of {scan}"
                );
            }
        }
    }
}
