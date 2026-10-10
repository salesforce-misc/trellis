//! Applies Trellis's ordered SQL migrations via `refinery`.
//!
//! Migration files live in `trellis/migrations/` and are embedded into the
//! binary at compile time by [`refinery::embed_migrations!`] — nothing is
//! read from disk at runtime, so a deployed binary carries its own
//! migrations. Applied versions are tracked in refinery's own
//! `refinery_schema_history` table, created (like everything else Trellis
//! owns) in the schema configured on [`Config`] via `search_path`
//! (see [`crate::pool`]).
//!
//! Each migration runs in its own transaction (refinery's behavior, not
//! something built here): a failing migration rolls back cleanly and is
//! not recorded as applied, so the ledger never reflects a partial
//! migration.
//!
//! Before the runner ever touches the schema, [`crate::identity`] decides
//! whether attaching to it is safe at all — see that module for what
//! "safe" means.

mod embedded {
    #![allow(clippy::needless_raw_string_hashes)]
    refinery::embed_migrations!("migrations");
}

use crate::config::Config;
use crate::error::Error;
use crate::identity;
use crate::pool::Pool;

/// Ensures `config.schema()` exists and is safe to attach to (see
/// [`crate::identity`]), then applies any migrations not yet recorded as
/// applied. A no-op if everything is already up to date.
///
/// Attaches to one database run one at a time (see
/// [`identity::AttachLock`]), and a catalog schema that belongs to another
/// instance sharing the database is refused (issue #877).
///
/// Deliberately public — a tier-2 composable primitive under ADR-0012, not a
/// leaked internal. [`crate::Trellis::migrate`] and
/// [`crate::BlockingTrellis::migrate`] delegate to it, and an embedder (or a
/// shared test bootstrap such as `testkit`'s) may call it directly to bring a
/// schema up to date without standing up a facade.
pub async fn migrate(pool: &Pool, config: &Config) -> Result<(), Error> {
    // One attach at a time per database: the instance-conflict check reads
    // the markers other attaches write. The attach runs on the lock's own
    // session, so the lock can't end before it. See `identity::AttachLock`.
    let mut lock = identity::AttachLock::acquire(pool).await?;
    let attached = attach(lock.client(), config).await;
    lock.release().await;
    attached
}

async fn attach(pg_client: &mut tokio_postgres::Client, config: &Config) -> Result<(), Error> {
    identity::prepare_attach(pg_client, config).await?;

    run_migrations(pg_client).await?;

    // Always seed/reconcile the marker, not just on a "fresh attach" — see
    // `identity::seed_marker` for why: it's an idempotent upsert, so a
    // clean re-attach, a resumed crash between the runner creating
    // `trellis_instance` and this committing, and a changed target schema
    // are all handled by one statement rather than separate states to track.
    identity::seed_marker(pg_client, config).await?;

    Ok(())
}

/// Runs the pending migrations. Inside a public call (issue #599) it runs
/// them one `run_async` per migration, so that each migration's transaction
/// can be given the budget remaining when it starts rather than what was left
/// when the connection was opened: a migration that doesn't fit is stopped by
/// the server and rolled back, and the ones before it stay applied, which is
/// refinery's migration-at-a-time model anyway. Outside a call it is one
/// `run_async`, as before.
async fn run_migrations(pg_client: &mut tokio_postgres::Client) -> Result<(), Error> {
    if crate::deadline::current().is_none() {
        embedded::migrations::runner().run_async(pg_client).await?;
        return Ok(());
    }
    let mut versions: Vec<i32> = embedded::migrations::runner()
        .get_migrations()
        .iter()
        .map(|migration| migration.version())
        .collect();
    versions.sort_unstable();
    let Some(&first) = versions.first() else {
        return Ok(());
    };
    // The first pass creates refinery's history table, which the query for
    // what is applied needs.
    run_up_to(pg_client, first).await?;
    let applied: std::collections::HashSet<i32> = embedded::migrations::runner()
        .get_applied_migrations_async(pg_client)
        .await?
        .iter()
        .map(|migration| migration.version())
        .collect();
    for version in versions.into_iter().filter(|v| !applied.contains(v)) {
        run_up_to(pg_client, version).await?;
    }
    // Nothing is pending now; this last pass is refinery's divergence and
    // missing-migration checks over the whole set, as an unbounded run does.
    run_up_to_latest(pg_client).await
}

/// The budget the running call has left, as this session's
/// `statement_timeout`.
async fn refresh_call_budget(pg_client: &tokio_postgres::Client) -> Result<(), Error> {
    if let Some(deadline) = crate::deadline::current() {
        pg_client
            .simple_query(&deadline.set_statement_timeout_sql(false))
            .await?;
    }
    Ok(())
}

async fn run_up_to(pg_client: &mut tokio_postgres::Client, version: i32) -> Result<(), Error> {
    refresh_call_budget(pg_client).await?;
    embedded::migrations::runner()
        .set_target(refinery::Target::Version(version))
        .run_async(pg_client)
        .await?;
    Ok(())
}

async fn run_up_to_latest(pg_client: &mut tokio_postgres::Client) -> Result<(), Error> {
    refresh_call_budget(pg_client).await?;
    embedded::migrations::runner().run_async(pg_client).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    /// Issue #599: inside a call, each migration pass runs under the budget
    /// left when the pass starts, not the one the session was opened with.
    /// The session's `statement_timeout` after the run is the budget the
    /// last pass was given.
    #[tokio::test]
    async fn each_migration_pass_gets_the_budget_left_when_it_starts() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let config = Config::from_dsn(db.dsn().to_string()).expect("dsn");
        let pool = Pool::new(&config).expect("pool");
        let setting = "select setting::bigint from pg_settings where name = 'statement_timeout'";
        let (opened, last) =
            crate::deadline::bounded(Instant::now(), Duration::from_secs(20), async {
                let attach = async {
                    let mut client = pool.connect_unpooled().await?;
                    let opened: i64 = client.query_one(setting, &[]).await?.get(0);
                    identity::prepare_attach(&mut client, &config).await?;
                    // Stands for the time the earlier migrations took.
                    tokio::time::sleep(Duration::from_millis(1500)).await;
                    run_migrations(&mut client).await?;
                    let last: i64 = client.query_one(setting, &[]).await?.get(0);
                    Ok::<_, Error>((opened, last))
                };
                attach.await.map_err(crate::TrellisError::Engine)
            })
            .await
            .expect("migrate inside a call");
        assert!((19_000..=20_000).contains(&opened), "when opened: {opened}");
        assert!((1..=18_500).contains(&last), "for the last pass: {last}");
    }
}
