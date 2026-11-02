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
//!
//! # Numbering and editing
//!
//! Versions are contiguous from 1 (a test checks the ledger against the files).
//! A migration that has shipped is never edited; a schema change is a new file
//! at the next version. Before the first release the files were compacted into
//! topic migrations, so a catalog schema created earlier is recreated rather
//! than migrated.
//!
//! # Rebuilding a catalog table
//!
//! A migration must never rebuild `partition_sources`, `capture_baselines` or
//! `capture_triggers`
//! by create-copy-rename; use `ALTER`. Rows copied into a new table look
//! exactly like a dump and restore: `partition_sources` and
//! `capture_triggers` record the oid of the table that holds each row, and a
//! stored oid that no longer names it pauses every definition
//! (`capture::restore`). Any `ALTER`, rewriting or not, keeps the oid.

mod embedded {
    #![allow(clippy::needless_raw_string_hashes)]
    refinery::embed_migrations!("migrations");
}

use crate::config::Config;
use crate::error::Error;
use crate::handover;
use crate::identity;
use crate::pool::{Pool, quote_ident};

/// Whether some migration this binary carries hasn't run on `schema`'s
/// instance: refinery's history lacks its version. A schema with no history
/// has never been migrated, so no process of any version can be running on
/// it, and reads as nothing pending: `migrate` creates it, and a background
/// process started before that fails as it reads the catalog. A version the
/// history has and this binary lacks doesn't count either: refinery refuses
/// that database when it runs.
pub(crate) async fn migrations_pending(
    client: &impl tokio_postgres::GenericClient,
    schema: &str,
) -> Result<bool, tokio_postgres::Error> {
    let history = format!("{}.refinery_schema_history", quote_ident(schema));
    let carried: Vec<i32> = embedded::migrations::runner()
        .get_migrations()
        .iter()
        .map(|migration| migration.version())
        .collect();
    if !has_history(client, schema).await? {
        return Ok(false);
    }
    let applied: std::collections::HashSet<i32> = client
        .query(&format!("select version from {history}"), &[])
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();
    Ok(carried.iter().any(|version| !applied.contains(version)))
}

/// Whether `schema` has refinery's history table: it has been migrated.
async fn has_history(
    client: &impl tokio_postgres::GenericClient,
    schema: &str,
) -> Result<bool, tokio_postgres::Error> {
    let history = format!("{}.refinery_schema_history", quote_ident(schema));
    Ok(client
        .query_one("select to_regclass($1) is not null", &[&history])
        .await?
        .get(0))
}

/// Ensures `config.schema()` exists and is safe to attach to (see
/// `crate::identity`), then applies any migrations not yet recorded as
/// applied. A no-op if everything is already up to date.
///
/// Attaches to one database run one at a time (see
/// `identity::AttachLock`), and a catalog schema that belongs to another
/// instance sharing the database is refused.
///
/// Deliberately public — a tier-2 composable primitive under ADR-0012, not a
/// leaked internal. [`crate::Trellis::migrate`] and
/// [`crate::BlockingTrellis::migrate`] do the same on their own pool, and an
/// embedder (or a shared test bootstrap such as `testkit`'s) may call it
/// directly to bring a schema up to date without standing up a facade. It
/// opens the one connection it needs from `config` and closes it before
/// returning.
pub async fn migrate(config: &Config) -> Result<(), Error> {
    migrate_with_pool(&Pool::new(config)?, config).await
}

/// [`migrate`] on `pool`'s connection settings rather than a pool of its own
/// built from `config`.
pub async fn migrate_with_pool(pool: &Pool, config: &Config) -> Result<(), Error> {
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

    // ADR-0023: the handover lock for the rest of the attach, so no background
    // process registers between the check below and the migrations it allows.
    // Session-level on the attach's own session, which closes with it.
    let handover_key = handover::handover_lock_key(config.schema());
    pg_client
        .execute("select pg_advisory_lock($1)", &[&handover_key])
        .await?;
    let attached = attach_under_handover(pg_client, config).await;
    let _ = pg_client
        .execute("select pg_advisory_unlock($1)", &[&handover_key])
        .await;
    attached
}

/// [`attach`] once the handover lock is held. Migrations never run while a
/// Trellis process is live (ADR-0023, rule 3): one is of an older version
/// than the migrations. While another version is live and nothing is
/// pending, the attach changes nothing: it runs its checks (refinery's among
/// them), and refuses if the grammar check would pause a definition or the
/// marker would change.
async fn attach_under_handover(
    pg_client: &mut tokio_postgres::Client,
    config: &Config,
) -> Result<(), Error> {
    let observed = handover::observe(&*pg_client, config.schema(), true).await?;
    let other_version =
        match handover::decide(config.binary_version(), &observed, handover::Asker::Migrate) {
            handover::Verdict::TakeOver => None,
            handover::Verdict::Refuse(seen) if seen.migrations_pending => {
                return Err(Error::OtherVersionLive(seen));
            }
            handover::Verdict::Refuse(seen) => Some(seen),
            handover::Verdict::StandBy(_) => unreachable!("only a background process stands by"),
        };
    if let Some(seen) = other_version {
        // Refinery's divergence and missing-migration checks still refuse a
        // database a newer binary migrated (rule 8). Nothing is pending, so
        // the run applies and writes nothing; a schema without a history has
        // nothing to check, and the run would create it.
        if has_history(&*pg_client, config.schema()).await? {
            embedded::migrations::runner().run_async(pg_client).await?;
        }
        let would_pause = crate::defs::grammar::check_stored_versions_dry_run(pg_client).await?;
        if would_pause > 0 || identity::marker_would_change(pg_client, config).await? {
            return Err(Error::OtherVersionLive(seen));
        }
        return Ok(());
    }

    run_migrations(pg_client).await?;

    // ADR-0018: before anything reads a stored definition, refuse an attach
    // over definitions this binary can't read and pause the ones outside the
    // support window.
    crate::defs::grammar::check_stored_versions(pg_client).await?;

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
