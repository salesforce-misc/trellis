//! Makes every test server version attributes the way PostgreSQL's 2026-11
//! minors do (15.20, 16.16, 17.12, 18.7), so one set of expectations holds on
//! all of them.
//!
//! Those minors' `ALTER COLUMN c TYPE` stamps `pg_attver=<WAL insert LSN>`
//! into the `attoptions` of `c` and of every later attribute, dropped ones
//! included (`SetAttributeVersions`). Older minors write nothing. Capture
//! leaves the version out of its column comparison, so a later column the
//! server re-stamped reads as a same-type rewrite whenever the table's file
//! node also moved. This hook gives an older minor the same stamps.
//!
//! It writes `pg_attribute` as the cluster's superuser. That is acceptable
//! only because the testkit's clusters are throwaway; nothing outside the
//! tests does it. An older minor then refuses `ALTER COLUMN ... SET (...)`
//! on a stamped column (`unrecognized parameter "pg_attver"`), which the
//! 2026-11 minors accept.

use tokio_postgres::Client;

/// Whether the server stamps versions itself, probed once per client.
pub struct Attver {
    server_stamps: bool,
}

impl Attver {
    /// Probes the server with a retype on a temporary table.
    pub async fn probe(client: &Client) -> Self {
        client
            .batch_execute(
                "create temp table attver_probe (c int); \
                 alter table attver_probe alter column c type bigint",
            )
            .await
            .expect("probe a retype");
        let server_stamps = client
            .query_one(
                "select coalesce(array_to_string(attoptions, ','), '') like '%pg_attver=%' \
                 from pg_catalog.pg_attribute \
                 where attrelid = 'pg_temp.attver_probe'::regclass and attname = 'c'",
                &[],
            )
            .await
            .expect("read the probe")
            .get(0);
        client
            .batch_execute("drop table attver_probe")
            .await
            .expect("drop the probe");
        Self { server_stamps }
    }

    /// Call after running `statement`: if it is `alter table T alter column
    /// C type ...` and the server left no stamp of its own, stamps `C` and
    /// every later attribute of `T` with one WAL insert LSN, in the
    /// statement's transaction. `SET DATA TYPE` and a retype in a list of
    /// subcommands panic, as they are not simulated; nor are the children
    /// of a partitioned or inherited `T`, which the server also stamps.
    pub async fn after(&self, client: &Client, statement: &str) {
        if self.server_stamps {
            return;
        }
        let lower = statement.to_lowercase();
        assert!(
            !lower.contains(" set data type") && !lower.contains(", alter"),
            "the attver hook does not simulate {statement}"
        );
        let words: Vec<&str> = lower.split_whitespace().collect();
        let retypes = words
            .windows(6)
            .filter(|w| w[0] == "table" && w[2] == "alter" && w[3] == "column" && w[5] == "type");
        for w in retypes {
            client
                .execute(
                    "update pg_catalog.pg_attribute set attoptions = \
                       coalesce((select array_agg(o order by i) \
                                 from unnest(attoptions) with ordinality u(o, i) \
                                 where o not like 'pg_attver=%'), '{}'::text[]) \
                       || ('pg_attver=' || \
                           (select pg_current_wal_insert_lsn() - '0/0'::pg_lsn)::text) \
                     where attrelid = $1::text::regclass and attnum >= \
                       (select attnum from pg_catalog.pg_attribute \
                        where attrelid = $1::text::regclass and attname = $2)",
                    &[&w[1], &w[4]],
                )
                .await
                .unwrap_or_else(|e| panic!("stamp {} after {statement}: {e}", w[4]));
        }
    }
}
