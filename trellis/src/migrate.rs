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
//! "safe" means — and [`crate::catalog_floor`] whether this binary can read
//! the catalog: its version meets the catalog floor, and the applied history
//! fits its migrations. Applied migrations newer than any this binary carries
//! are a newer release's, which the floor says this one can read, so the
//! runner runs with refinery's `abort_missing` off, and only after that check
//! has classified the history.
//!
//! # Numbering and editing
//!
//! Versions are contiguous from 1 (a test checks the ledger against the files).
//! A migration that has shipped is never edited; a schema change is a new file
//! at the next version. Before the first release the files were compacted into
//! topic migrations, so a catalog schema created earlier is recreated rather
//! than migrated.
//!
//! # Raising the catalog floor
//!
//! Each migration declares the oldest release that can read the catalog after
//! it (ADR-0024). One the previous releases can read declares nothing: it adds
//! a table, or a column that is nullable or has a default, and the code that
//! reads it takes a `NULL` (or the default) as a row an older release wrote,
//! since an older release at or above the floor runs on the catalog and writes
//! rows without it. One they can't read, and every change to the shape of the
//! capture objects on source tables, raises the floor in its own SQL, to the
//! greater of the stored floor and its own:
//!
//! ```sql
//! update catalog_floor set version = '0.3.0'
//!  where string_to_array(version, '.')::int[] < '{0,3,0}';
//! ```
//!
//! and [`crate::catalog_floor::CATALOG_FLOOR`] moves with it.
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

use crate::catalog_floor::{self, MigrationRecord};
use crate::config::Config;
use crate::error::Error;
use crate::handover;
use crate::identity;
use crate::pool::{Pool, quote_ident};

/// The migrations a binary carries, and the catalog floor they leave.
pub(crate) struct Migrations {
    list: Vec<refinery::Migration>,
    floor: String,
}

impl Migrations {
    /// This binary's own: the files in `trellis/migrations/`.
    pub(crate) fn embedded() -> Self {
        Migrations {
            list: embedded::migrations::runner().get_migrations().clone(),
            floor: catalog_floor::CATALOG_FLOOR.to_string(),
        }
    }

    /// The runner over this set. `abort_missing` is off: an applied migration
    /// this set lacks is a newer release's, and [`catalog_floor::check`] has
    /// refused every other case before a runner runs.
    fn runner(&self) -> refinery::Runner {
        refinery::Runner::new(&self.list).set_abort_missing(false)
    }

    fn records(&self) -> Vec<MigrationRecord> {
        self.list
            .iter()
            .map(|migration| MigrationRecord {
                version: migration.version(),
                name: migration.name().to_string(),
                checksum: migration.checksum().to_string(),
            })
            .collect()
    }
}

/// Whether a binary of `version`, carrying this binary's migrations, is below
/// `schema`'s catalog floor ([`catalog_floor::check_floor`]): what every
/// background process checks before it stages, drains or builds, and every
/// state-changing call before it changes anything (ADR-0024, rule 2).
pub(crate) async fn below_catalog_floor(
    client: &impl tokio_postgres::GenericClient,
    schema: &str,
    version: &str,
) -> Result<Option<catalog_floor::FloorRefused>, tokio_postgres::Error> {
    let migrations = Migrations::embedded();
    catalog_floor::check_floor(
        client,
        schema,
        version,
        &migrations.floor,
        &migrations.records(),
    )
    .await
}

/// Whether some migration this binary carries hasn't run on `schema`'s
/// instance: refinery's history lacks its version. A schema with no history
/// has never been migrated, so no process of any version can be running on
/// it, and reads as nothing pending: `migrate` creates it, and a background
/// process started before that fails as it reads the catalog. A version the
/// history has and this binary lacks doesn't count either: it is a newer
/// release's, or a gap the attach refuses ([`catalog_floor::decide`]).
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
    migrate_carrying(pool, config, &Migrations::embedded()).await
}

/// [`migrate_with_pool`] for a binary carrying `migrations`: a test stands
/// in for another release with its own set.
async fn migrate_carrying(
    pool: &Pool,
    config: &Config,
    migrations: &Migrations,
) -> Result<(), Error> {
    // One attach at a time per database: the instance-conflict check reads
    // the markers other attaches write. The attach runs on the lock's own
    // session, so the lock can't end before it. See `identity::AttachLock`.
    let mut lock = identity::AttachLock::acquire(pool).await?;
    let attached = attach(lock.client(), config, migrations).await;
    lock.release().await;
    attached
}

async fn attach(
    pg_client: &mut tokio_postgres::Client,
    config: &Config,
    migrations: &Migrations,
) -> Result<(), Error> {
    identity::prepare_attach(pg_client, config).await?;

    // ADR-0024: refuse a catalog this binary can't read before anything
    // runs on it. A handle that connects without migrating runs the floor
    // half of this check ([`below_catalog_floor`]).
    catalog_floor::check(
        &*pg_client,
        config.schema(),
        config.binary_version(),
        &migrations.floor,
        &migrations.records(),
    )
    .await?;

    // ADR-0023: the handover lock for the rest of the attach, so no background
    // process registers between the check below and the migrations it allows.
    // Session-level on the attach's own session, which closes with it.
    let handover_key = handover::handover_lock_key(config.schema());
    pg_client
        .execute("select pg_advisory_lock($1)", &[&handover_key])
        .await?;
    let attached = attach_under_handover(pg_client, config, migrations).await;
    let _ = pg_client
        .execute("select pg_advisory_unlock($1)", &[&handover_key])
        .await;
    attached
}

/// [`attach`] once the handover lock is held. Migrations never run while a
/// Trellis process is live (ADR-0023, rule 3): one is of an older version
/// than the migrations. While another version is live and nothing is
/// pending, the attach changes nothing: it has run its checks (the catalog
/// floor and history among them), and refuses if the grammar check would
/// pause a definition or the marker would change.
async fn attach_under_handover(
    pg_client: &mut tokio_postgres::Client,
    config: &Config,
    migrations: &Migrations,
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
        let would_pause =
            crate::defs::grammar::check_stored_versions_dry_run(pg_client, &config.floors())
                .await?;
        if would_pause > 0 || identity::marker_would_change(pg_client, config).await? {
            return Err(Error::OtherVersionLive(seen));
        }
        return Ok(());
    }

    run_migrations(pg_client, migrations).await?;

    // ADR-0018, ADR-0024 rule 3: before anything reads a stored definition,
    // pause each one this binary can't run (its floor or its grammar is
    // newer) or read (outside the support window, or text it refuses), each
    // on its own, so the rest of the instance runs beside it.
    crate::defs::grammar::check_stored_versions(pg_client, &config.floors()).await?;

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
async fn run_migrations(
    pg_client: &mut tokio_postgres::Client,
    migrations: &Migrations,
) -> Result<(), Error> {
    if crate::deadline::current().is_none() {
        migrations.runner().run_async(pg_client).await?;
        return Ok(());
    }
    let mut versions: Vec<i32> = migrations
        .list
        .iter()
        .map(|migration| migration.version())
        .collect();
    versions.sort_unstable();
    let Some(&first) = versions.first() else {
        return Ok(());
    };
    // The first pass creates refinery's history table, which the query for
    // what is applied needs.
    run_up_to(pg_client, migrations, first).await?;
    let applied: std::collections::HashSet<i32> = migrations
        .runner()
        .get_applied_migrations_async(pg_client)
        .await?
        .iter()
        .map(|migration| migration.version())
        .collect();
    for version in versions.into_iter().filter(|v| !applied.contains(v)) {
        run_up_to(pg_client, migrations, version).await?;
    }
    // Nothing is pending now; this last pass is refinery's divergence check
    // over the whole set, as an unbounded run does.
    run_up_to_latest(pg_client, migrations).await
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

async fn run_up_to(
    pg_client: &mut tokio_postgres::Client,
    migrations: &Migrations,
    version: i32,
) -> Result<(), Error> {
    refresh_call_budget(pg_client).await?;
    migrations
        .runner()
        .set_target(refinery::Target::Version(version))
        .run_async(pg_client)
        .await?;
    Ok(())
}

async fn run_up_to_latest(
    pg_client: &mut tokio_postgres::Client,
    migrations: &Migrations,
) -> Result<(), Error> {
    refresh_call_budget(pg_client).await?;
    migrations.runner().run_async(pg_client).await?;
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
                    run_migrations(&mut client, &Migrations::embedded()).await?;
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

    /// This binary's migrations plus `extra` (`(file name, sql)`), leaving
    /// the floor `floor`: a newer release, in one test.
    fn newer_release(extra: &[(&str, &str)], floor: &str) -> Migrations {
        let mut migrations = Migrations::embedded();
        for (name, sql) in extra {
            migrations
                .list
                .push(refinery::Migration::unapplied(name, sql).expect("migration"));
        }
        migrations.floor = floor.to_string();
        migrations
    }

    /// An additive migration of a newer release: a nullable column on a
    /// table this binary writes.
    const ADDITIVE: (&str, &str) = (
        "V9001__newer_additive",
        "alter table transform_definitions add column newer_note text",
    );

    /// A newer release's migration this binary can't read: it raises the
    /// floor, as ADR-0024 has every such migration do.
    const RAISES_FLOOR: (&str, &str) = (
        "V9002__newer_raises_floor",
        "update catalog_floor set version = '0.3.0' \
         where string_to_array(version, '.')::int[] < '{0,3,0}'",
    );

    async fn history(client: &tokio_postgres::Client) -> Vec<i32> {
        client
            .query(
                "select version from trellis.refinery_schema_history order by version",
                &[],
            )
            .await
            .expect("history")
            .iter()
            .map(|row| row.get(0))
            .collect()
    }

    /// ADR-0024 rules 2, 3 and 5: a binary at or above the catalog floor
    /// attaches to a catalog a newer release migrated, tolerating the
    /// migration it doesn't carry and applying nothing, and writes rows the
    /// newer release's additive column leaves `NULL`. Once the newer release
    /// raises the floor above it, it refuses, naming both versions, and
    /// changes nothing.
    #[tokio::test]
    async fn an_older_binary_attaches_above_the_floor_and_refuses_below_it() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let base = Config::from_dsn(db.dsn().to_string()).expect("dsn");
        let current = base.clone().with_binary_version("0.2.0");
        let newer = base.clone().with_binary_version("0.3.0");
        let pool = Pool::new(&base).expect("pool");
        let raw = pool.connect_unpooled().await.expect("connect");
        raw.batch_execute("create table public.src (id bigint primary key, v int)")
            .await
            .expect("source");

        migrate_carrying(&pool, &newer, &newer_release(&[ADDITIVE], "0.2.0"))
            .await
            .expect("the newer release migrates");
        let migrated = history(&raw).await;
        assert_eq!(migrated.last(), Some(&9001));

        migrate_carrying(&pool, &current, &Migrations::embedded())
            .await
            .expect("this release attaches at the floor and tolerates V9001");
        assert_eq!(history(&raw).await, migrated, "nothing applied or re-run");

        // It runs: a define writes a definition row without the newer column.
        let handle = crate::Trellis::connect(current.clone(), crate::TrellisOptions::default())
            .await
            .expect("connect");
        handle
            .apply("TRANSFORM src_copy FROM src SELECT v AS v")
            .await
            .expect("define");
        handle.shutdown().await.expect("shutdown");
        let note: Option<String> = raw
            .query_one(
                "select newer_note from trellis.transform_definitions \
                 where target_table like '%src_copy'",
                &[],
            )
            .await
            .expect("the definition row")
            .get(0);
        assert_eq!(note, None, "a row an older release wrote");

        migrate_carrying(
            &pool,
            &newer,
            &newer_release(&[ADDITIVE, RAISES_FLOOR], "0.3.0"),
        )
        .await
        .expect("the newer release raises the floor");
        let raised = history(&raw).await;

        let err = migrate_carrying(&pool, &current, &Migrations::embedded())
            .await
            .expect_err("below the floor");
        assert_eq!(err.code(), crate::ErrorCode::VersionFloor, "{err}");
        let message = err.to_string();
        assert!(
            message.contains("floor is 0.3.0") && message.contains("version 0.2.0"),
            "{message}"
        );
        assert_eq!(history(&raw).await, raised, "the refusal changed nothing");

        migrate_carrying(
            &pool,
            &newer,
            &newer_release(&[ADDITIVE, RAISES_FLOOR], "0.3.0"),
        )
        .await
        .expect("the newer release still attaches");
    }

    /// The floor a fresh migrate records is the one this binary's
    /// migrations declare ([`catalog_floor::CATALOG_FLOOR`]), and the marker's
    /// retired format version is 2, which releases before 0.2.0 refuse.
    #[tokio::test]
    async fn a_fresh_migrate_records_the_floor_this_binary_declares() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let config = Config::from_dsn(db.dsn().to_string()).expect("dsn");
        migrate(&config).await.expect("migrate");
        let raw = Pool::new(&config)
            .expect("pool")
            .connect_unpooled()
            .await
            .expect("connect");
        assert_eq!(
            catalog_floor::read_floor(&raw, config.schema())
                .await
                .expect("floor")
                .as_deref(),
            Some(catalog_floor::CATALOG_FLOOR)
        );
        let format: i32 = raw
            .query_one(
                "select instance_format_version from trellis.trellis_instance",
                &[],
            )
            .await
            .expect("marker")
            .get(0);
        assert_eq!(format, 2);
    }

    /// ADR-0024: the migration that adds definition floors gives every
    /// definition already stored floor 0.2.0, the baseline.
    #[tokio::test]
    async fn the_floor_migration_gives_every_existing_definition_the_baseline() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_empty_database().await;
        let config = Config::from_dsn(db.dsn().to_string()).expect("dsn");
        let pool = Pool::new(&config).expect("pool");
        let mut client = pool.connect_unpooled().await.expect("connect");
        identity::prepare_attach(&mut client, &config)
            .await
            .expect("prepare");
        let migrations = Migrations::embedded();
        let floor_migration = migrations
            .list
            .iter()
            .find(|migration| migration.name() == "definition_floor")
            .expect("the floor migration")
            .version();
        let before = migrations
            .list
            .iter()
            .map(|migration| migration.version())
            .filter(|&version| version < floor_migration)
            .max()
            .expect("a migration before it");
        run_up_to(&mut client, &migrations, before)
            .await
            .expect("migrate to before");
        client
            .batch_execute(
                "insert into source_table_versions (source_table, version) \
                 values ('public.s', 1), ('public.u', 1); \
                 insert into transform_definitions \
                 (target_table, source_table, source_version, definition_text, status) \
                 values ('public.t', 'public.s', 1, 'TRANSFORM t FROM s SELECT a AS a', 'live'), \
                        ('public.g', 'public.u', 1, \
                         'TRANSFORM g FROM u GROUP BY k SELECT k AS k, COUNT(*) AS n', 'paused')",
            )
            .await
            .expect("definitions stored before floors");

        run_up_to_latest(&mut client, &migrations)
            .await
            .expect("migrate");

        let floors: Vec<(String, String)> = client
            .query(
                "select target_table, floor_version from transform_definitions order by id",
                &[],
            )
            .await
            .expect("read the floors")
            .iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        assert_eq!(
            floors,
            [
                ("public.t".to_string(), "0.2.0".to_string()),
                ("public.g".to_string(), "0.2.0".to_string()),
            ]
        );
        let refused = client
            .execute(
                "update transform_definitions set floor_version = '0.2.0-rc.1'",
                &[],
            )
            .await;
        assert!(refused.is_err(), "a floor is a release: {refused:?}");
    }
}
