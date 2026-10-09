//! Instance identity: deciding, before any migration touches a schema,
//! whether attaching to it is safe.
//!
//! Because several independent Trellis instances can share one database
//! cluster — even one database — differing only in `config.schema()` (see
//! `docs/instance-identity.md`), attaching to a schema needs to distinguish
//! three cases:
//!
//! 1. **Fresh (or mid-migration on our own behalf).** The schema doesn't
//!    exist yet, exists but is empty, or exists with Trellis's own
//!    migration ledger (`refinery_schema_history`) but no marker yet — the
//!    last of these is a crash between the runner creating V9's table and
//!    [`seed_marker`] committing, and must be exactly as attachable as a
//!    fresh schema, or a crashed first-ever migrate would permanently lock
//!    itself out. Ours to take/resume.
//! 2. **Same instance.** The schema already carries our marker
//!    (`trellis_instance`) recording this exact schema name and a format
//!    version we understand — a clean, idempotent re-attach.
//! 3. **Someone else's.** The marker records a *different* schema name (a
//!    marker was copied/restored under the wrong name), the marker records
//!    a format version newer than this build understands, or the schema
//!    pre-existed with foreign objects and neither a marker nor a Trellis
//!    migration ledger (not ours, don't clobber it). All three are refused
//!    with [`Error::IncompatibleInstance`].
//!
//! [`prepare_attach`] runs this decision *before* [`crate::migrate::migrate`]
//! hands off to the refinery runner, and [`seed_marker`] runs after the
//! runner *unconditionally* (an idempotent upsert, not gated on a "fresh"
//! flag) — see both functions' docs for why that matters for crash recovery
//! and for two instances racing a first-ever attach.
//!
//! Before any of that, [`prepare_attach`] also refuses a catalog schema that
//! belongs to, or collides with, *another* instance sharing the database
//! (issue #877, epic #806) — see [`prepare_attach`] for the four rules. A
//! second client of the *same* instance is not "another instance": its
//! marker matches, so it attaches as always. The whole attach (check, runner,
//! marker write) runs under one database-wide [`AttachLock`], so the check
//! never reads a marker another attach is about to write.

use crate::config::Config;
use crate::error::Error;
use crate::pool::quote_ident;

/// The on-disk shape/meaning of the `trellis_instance` marker. Bump this
/// when a future change to the marker (or to what attaching means) isn't
/// safely interpretable by old code — a schema whose marker records a
/// version newer than this constant is refused rather than guessed at. Only
/// one version has ever existed, so there's no migration path defined yet;
/// add one alongside the first version bump.
pub const INSTANCE_FORMAT_VERSION: i32 = 1;

/// A resolved instance identity, for reporting (see [`Config`]'s
/// [`std::fmt::Display`] impl for the short form; this is the structured
/// version).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// The schema this instance operates in.
    pub schema: String,
    /// The instance-identity marker format this build writes/expects. See
    /// [`INSTANCE_FORMAT_VERSION`].
    pub format_version: i32,
}

impl Identity {
    /// The identity this build of Trellis would attach to `config` as.
    /// Doesn't touch the database — see [`prepare_attach`] for the
    /// database-backed check.
    pub fn resolved(config: &Config) -> Self {
        Self {
            schema: config.schema().to_string(),
            format_version: INSTANCE_FORMAT_VERSION,
        }
    }
}

impl std::fmt::Display for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "schema {:?}, instance format version {}",
            self.schema, self.format_version
        )
    }
}

/// Ensures `config.schema()` exists and is safe to attach to, refusing it
/// otherwise (see the module docs for the three cases).
///
/// Deliberately does *not* return a "fresh vs. re-attach" flag: an earlier
/// version of this function did, and gated [`seed_marker`] on it, which
/// meant a process death between the runner creating `trellis_instance` and
/// the (then-conditional) seed insert committing left a schema with our
/// ledger and tables but no marker — a state this same function would then
/// refuse to touch again, permanently. Marker seeding is unconditional now
/// (see [`seed_marker`]), so no flag is needed here.
///
/// Before anything is created, four rules keep one instance's catalog away
/// from another's (issue #877). Each refusal is an
/// [`Error::IncompatibleInstance`] naming both schemas:
///
/// 1. The catalog schema may not be `public`.
/// 2. It may not be this instance's own target schema.
/// 3. The target schema may not hold another instance's catalog (a
///    `trellis_instance` table, V9).
/// 4. The catalog schema may not be another instance's target schema (its
///    marker's `target_schema`, V83).
///
/// Rules 1 and 2 read only the [`Config`]; 3 and 4 scan the database's
/// markers, so the caller holds the [`AttachLock`] while it runs. A marker
/// this role cannot read is not seen by rule 4, and a marker written before
/// V83 has no target schema; both are blind spots, not refusals.
pub(crate) async fn prepare_attach(
    client: &mut tokio_postgres::Client,
    config: &Config,
) -> Result<(), Error> {
    let schema = config.schema();
    check_catalog_schema(config)?;
    refuse_conflicting_instances(client, config).await?;
    let existed_before = schema_exists(client, schema).await?;

    // Fully qualified (not relying on `search_path`) because the schema
    // itself may not exist yet on a brand-new database.
    client
        .batch_execute(&format!(
            "create schema if not exists {}",
            quote_ident(schema)
        ))
        .await?;

    match read_marker(client, schema).await? {
        Some(marker) => {
            if marker.schema_name != schema {
                return Err(Error::IncompatibleInstance(format!(
                    "schema {schema:?} is already claimed by a different Trellis instance \
                     (its marker records schema {:?}); refusing to attach",
                    marker.schema_name
                )));
            }
            if marker.instance_format_version > INSTANCE_FORMAT_VERSION {
                return Err(Error::IncompatibleInstance(format!(
                    "schema {schema:?} was written by a newer Trellis (instance format \
                     version {}); this build only understands up to version {INSTANCE_FORMAT_VERSION}",
                    marker.instance_format_version
                )));
            }
            // Same schema, a format version we understand: a clean re-attach.
            Ok(())
        }
        None => {
            // No marker yet doesn't necessarily mean "foreign": it's also
            // the state of a schema that's ours but mid-migration (crashed
            // between the runner creating V9's table and `seed_marker`
            // committing — or, before V9 even ran, between an earlier
            // migration committing and the crash). `refinery_schema_history`
            // is refinery's own ledger table, created by the very first
            // migration it applies, so its presence is a reliable signal
            // that this schema is (or was becoming) a Trellis instance
            // rather than something foreign. Only refuse when the schema
            // pre-existed, has at least one table, AND has neither marker
            // nor ledger — genuinely foreign, nothing of ours to resume.
            if existed_before
                && schema_has_any_tables(client, schema).await?
                && !schema_has_refinery_ledger(client, schema).await?
            {
                return Err(Error::IncompatibleInstance(format!(
                    "schema {schema:?} already exists with objects that aren't a Trellis \
                     instance (no identity marker or migration ledger found); refusing to \
                     take it over"
                )));
            }
            // Brand new, pre-existing but empty, or ours mid-migration:
            // safe to (re)take. `seed_marker` will (re)seed the marker
            // after the runner has ensured `trellis_instance` exists.
            Ok(())
        }
    }
}

/// Seeds the identity marker, unconditionally, after the migration runner
/// has ensured `trellis_instance` exists, and records the instance's target
/// schema on it. `on conflict (singleton) do update` rather than a plain
/// insert, for three reasons:
///
/// - **Crash recovery.** If a prior attach crashed after the runner created
///   `trellis_instance` but before this insert committed, the table exists
///   but is empty; this call fills it in, exactly as if this were the first
///   successful attach. Running it on every `migrate()` call (not just
///   "fresh" ones, per the old, now-removed flag) means there's no separate
///   "did we already seed this" state to get wrong.
/// - **Concurrent first attach.** Two processes racing to migrate the same
///   brand-new schema both reach this point; `singleton`'s primary key
///   would turn a second plain insert into a `Error::Connect` (a unique
///   violation), when the right outcome is a no-op for the loser. Its row
///   already matches (same schema, same version) since they're attaching
///   to the same config. ([`AttachLock`] serializes the attaches now, so the
///   loser finds the row there already; the upsert stays the one statement
///   that covers every case.)
/// - **A changed target schema.** The target schema is configuration that can
///   change between deploys, so the marker's `target_schema` follows the
///   config; it is the only column the update touches, and it is skipped
///   when the value already matches.
pub(crate) async fn seed_marker(
    client: &mut tokio_postgres::Client,
    config: &Config,
) -> Result<(), Error> {
    let schema = config.schema();
    let target_schema = config.target_schema();
    client
        .execute(
            "insert into trellis_instance (schema_name, instance_format_version, target_schema) \
             values ($1, $2, $3) \
             on conflict (singleton) do update set target_schema = excluded.target_schema \
             where trellis_instance.target_schema is distinct from excluded.target_schema",
            &[&schema, &INSTANCE_FORMAT_VERSION, &target_schema],
        )
        .await?;
    Ok(())
}

/// Rules 1 and 2 of [`prepare_attach`], which read only `config`.
fn check_catalog_schema(config: &Config) -> Result<(), Error> {
    let schema = config.schema();
    if schema == "public" {
        return Err(Error::IncompatibleInstance(format!(
            "catalog schema {schema:?} is not allowed: `public` is the application's \
             schema, not a Trellis instance's; give the instance a schema of its own \
             (`TRELLIS_SCHEMA`); refusing to attach"
        )));
    }
    if schema == config.target_schema() {
        return Err(Error::IncompatibleInstance(format!(
            "catalog schema {schema:?} is also this instance's target schema; the catalog \
             and the transform targets need different schemas (`TRELLIS_SCHEMA`, \
             `TRELLIS_TARGET_SCHEMA`); refusing to attach"
        )));
    }
    Ok(())
}

/// Rules 3 and 4 of [`prepare_attach`]: another instance's catalog schema
/// must not be this instance's target schema, and another instance's target
/// schema must not be this instance's catalog schema.
async fn refuse_conflicting_instances(
    client: &tokio_postgres::Client,
    config: &Config,
) -> Result<(), Error> {
    let schema = config.schema();
    let target_schema = config.target_schema();
    for other in other_instances(client, schema).await? {
        if other.catalog_schema == target_schema {
            return Err(Error::IncompatibleInstance(format!(
                "target schema {target_schema:?} holds another Trellis instance's catalog \
                 (it has a `trellis_instance` marker, so {:?} is that instance's catalog \
                 schema); this instance's catalog schema is {schema:?}; refusing to attach",
                other.catalog_schema
            )));
        }
        if other.target_schema.as_deref() == Some(schema) {
            return Err(Error::IncompatibleInstance(format!(
                "catalog schema {schema:?} is the target schema of another Trellis \
                 instance, whose catalog schema is {:?}; refusing to attach",
                other.catalog_schema
            )));
        }
    }
    Ok(())
}

/// Another instance in this database, as its `trellis_instance` table shows.
struct OtherInstance {
    /// The schema holding its `trellis_instance` table: its catalog schema.
    catalog_schema: String,
    /// The target schema its marker records. `None` if the marker predates
    /// V83, is not written yet, or this role cannot read it.
    target_schema: Option<String>,
}

/// Every `trellis_instance` table in the database outside `schema`.
async fn other_instances(
    client: &tokio_postgres::Client,
    schema: &str,
) -> Result<Vec<OtherInstance>, Error> {
    // From `pg_class`, not `information_schema`, which hides a table the role
    // has no privilege on: rule 3 needs to see it exist, and only reading it
    // needs the privilege.
    let tables = client
        .query(
            "select n.nspname, \
                    exists(select 1 from pg_attribute a \
                           where a.attrelid = c.oid and a.attname = 'target_schema' \
                             and a.attnum > 0 and not a.attisdropped), \
                    has_schema_privilege(n.oid, 'usage') \
                        and has_table_privilege(c.oid, 'select') \
             from pg_class c join pg_namespace n on n.oid = c.relnamespace \
             where c.relname = 'trellis_instance' and c.relkind in ('r', 'p') \
               and n.nspname <> $1 \
             order by n.nspname",
            &[&schema],
        )
        .await?;
    let mut others = Vec::with_capacity(tables.len());
    for table in &tables {
        let catalog_schema: String = table.get(0);
        let has_target_schema: bool = table.get(1);
        let readable: bool = table.get(2);
        let target_schema = if has_target_schema && readable {
            client
                .query_opt(
                    &format!(
                        "select target_schema from {}.trellis_instance",
                        quote_ident(&catalog_schema)
                    ),
                    &[],
                )
                .await?
                .and_then(|row| row.get(0))
        } else {
            None
        };
        others.push(OtherInstance {
            catalog_schema,
            target_schema,
        });
    }
    Ok(others)
}

/// Serializes attaches across a database: held, on a connection of its own
/// that the attach then runs on, from before [`prepare_attach`] until after
/// [`seed_marker`].
///
/// Rules 3 and 4 compare this instance with the markers the others have
/// written, and a marker is written at the end of an attach. Unserialized,
/// two instances whose schemas conflict, attaching at once, would each scan
/// before the other wrote its marker and both pass; and two clients of one
/// instance would both run the migration runner on one schema. Under the lock
/// the second attach sees everything the first committed: a conflicting
/// instance is refused, a client of the same instance finds the schema
/// migrated and its marker matching, and attaches as a no-op.
///
/// A session-level advisory lock on a dedicated unpooled connection, so no
/// path (an error, a dropped future) can hand a pooled connection that still
/// holds it to the next borrower: dropping the connection releases it. The
/// attach itself runs on that same connection ([`AttachLock::client`]), so
/// the lock can't end before the attach does: a session that dies (a
/// network drop, an operator's `pg_terminate_backend`, `idle_session_timeout`
/// on a lock session left idle) takes its half-done attach with it, and the
/// next attach never runs beside one.
pub(crate) struct AttachLock {
    client: tokio_postgres::Client,
}

/// The longest an attach waits for another attach to finish: a first-ever
/// attach runs every migration, and a client starting at the same moment
/// waits for it. The session's usual `lock_timeout`
/// ([`crate::locks::LOCK_TIMEOUT`]) guards transactions that hold a snapshot;
/// this wait holds none. It applies to the advisory lock's wait only: the
/// attach that follows on the same session runs its migrations under the
/// usual cap, so a migration's DDL never queues the instance's running
/// workers behind it for longer than that.
const ATTACH_LOCK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

/// The advisory-lock key of [`AttachLock`], one per database.
const ATTACH_LOCK_KEY: i64 = 0x5452_4c53_4154_5448;

impl AttachLock {
    pub(crate) async fn acquire(pool: &crate::pool::Pool) -> Result<Self, Error> {
        let mut client = pool.connect_unpooled().await?;
        // `set local`, so the long wait ends with this transaction. A
        // session-level advisory lock outlives the transaction it was taken
        // in.
        let txn = client.transaction().await?;
        crate::locks::set_local_lock_timeout(&txn, ATTACH_LOCK_TIMEOUT).await?;
        txn.execute("select pg_advisory_lock($1)", &[&ATTACH_LOCK_KEY])
            .await?;
        txn.commit().await?;
        Ok(Self { client })
    }

    /// The lock's own session, which the attach runs on.
    pub(crate) fn client(&mut self) -> &mut tokio_postgres::Client {
        &mut self.client
    }

    /// Releases the lock. Best effort: the connection closes with `self`
    /// either way, and the server releases a closed session's locks.
    pub(crate) async fn release(self) {
        let _ = self
            .client
            .execute("select pg_advisory_unlock($1)", &[&ATTACH_LOCK_KEY])
            .await;
    }
}

struct Marker {
    schema_name: String,
    instance_format_version: i32,
}

/// Whether `schema` exists at all, independent of `search_path` (this must
/// work before the schema is created, and information_schema views aren't
/// scoped by `search_path` anyway).
async fn schema_exists(client: &tokio_postgres::Client, schema: &str) -> Result<bool, Error> {
    let row = client
        .query_one(
            "select exists(select 1 from pg_namespace where nspname = $1)",
            &[&schema],
        )
        .await?;
    Ok(row.get(0))
}

/// Whether `schema` contains any tables at all (used only to help
/// distinguish an empty pre-existing schema, which is safe to take over,
/// from one with foreign objects, which isn't — see
/// [`schema_has_refinery_ledger`] for the other half of that distinction).
async fn schema_has_any_tables(
    client: &tokio_postgres::Client,
    schema: &str,
) -> Result<bool, Error> {
    let row = client
        .query_one(
            "select exists(select 1 from information_schema.tables where table_schema = $1)",
            &[&schema],
        )
        .await?;
    Ok(row.get(0))
}

/// Whether `schema` contains refinery's own migration ledger
/// (`refinery_schema_history`). That table is created by the very first
/// migration refinery ever applies in a schema, so finding it — even
/// without a `trellis_instance` marker — means this schema is a Trellis
/// instance that crashed partway through its first-ever migrate, not a
/// foreign schema. Schema-qualified explicitly rather than relying on
/// `search_path`, matching [`read_marker`].
async fn schema_has_refinery_ledger(
    client: &tokio_postgres::Client,
    schema: &str,
) -> Result<bool, Error> {
    let row = client
        .query_one(
            "select exists(select 1 from information_schema.tables \
             where table_schema = $1 and table_name = 'refinery_schema_history')",
            &[&schema],
        )
        .await?;
    Ok(row.get(0))
}

/// Reads the identity marker from `schema`, if its `trellis_instance` table
/// exists yet *and has a row*. Both "table doesn't exist" (every fresh
/// attach) and "table exists but is empty" (a crash between the runner
/// creating it and [`seed_marker`] committing) read as `None` — a schema
/// we're safe to (re)seed, not an error — so this uses `query_opt`, not
/// `query_one`, on both the existence probe's absence and the row lookup:
/// a `query_one` against a zero-row `trellis_instance` would surface as an
/// opaque `Error::Connect` rather than the "no marker yet" case it actually
/// is.
async fn read_marker(
    client: &tokio_postgres::Client,
    schema: &str,
) -> Result<Option<Marker>, Error> {
    let exists_row = client
        .query_one(
            "select exists(select 1 from information_schema.tables \
             where table_schema = $1 and table_name = 'trellis_instance')",
            &[&schema],
        )
        .await?;
    let table_exists: bool = exists_row.get(0);
    if !table_exists {
        return Ok(None);
    }

    // Qualified with the schema explicitly rather than relying on
    // `search_path`: this runs on the pool's shared client, whose
    // `search_path` is pinned to `config.schema()` already in the normal
    // case, but being explicit here keeps this function correct regardless.
    let row = client
        .query_opt(
            &format!(
                "select schema_name, instance_format_version from {}.trellis_instance",
                quote_ident(schema)
            ),
            &[],
        )
        .await?;
    Ok(row.map(|row| Marker {
        schema_name: row.get(0),
        instance_format_version: row.get(1),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_resolved_matches_config_schema() {
        let config = Config::from_dsn("postgresql://example/db").expect("valid default schema");
        let identity = Identity::resolved(&config);
        assert_eq!(identity.schema, config.schema());
        assert_eq!(identity.format_version, INSTANCE_FORMAT_VERSION);
    }

    fn config_with_target(schema: &str, target: &str) -> Config {
        Config::with_schema("postgresql://example/db", schema)
            .and_then(|config| config.with_target_schema(target))
            .expect("valid schemas")
    }

    #[test]
    fn a_catalog_schema_of_public_or_its_own_target_schema_is_refused() {
        let public = check_catalog_schema(&config_with_target("public", "app"));
        assert!(
            matches!(&public, Err(Error::IncompatibleInstance(m)) if m.contains("\"public\"")),
            "{public:?}"
        );
        let own_target = check_catalog_schema(&config_with_target("app", "app"));
        assert!(
            matches!(&own_target, Err(Error::IncompatibleInstance(m)) if m.contains("\"app\"")),
            "{own_target:?}"
        );
        check_catalog_schema(&config_with_target("trellis", "public")).expect("the defaults");
    }

    /// An attach holds the database-wide lock from its check to its marker
    /// write: while another holder has it, a second attach waits, and it
    /// proceeds only once the holder releases. Two attaches never interleave
    /// their check and their marker write, so a check can't read a marker
    /// that another attach is about to change.
    #[tokio::test]
    async fn an_attach_waits_for_the_attach_lock() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_empty_database().await;
        let config = Config::with_schema(db.dsn(), "instance_waits").expect("config");
        let pool = std::sync::Arc::new(crate::pool::Pool::new(&config).expect("pool"));

        let holder = AttachLock::acquire(&pool).await.expect("take the lock");
        let attach = tokio::spawn({
            let pool = std::sync::Arc::clone(&pool);
            let config = config.clone();
            async move { crate::migrate::migrate(&pool, &config).await }
        });

        // Wait until the attach is queued on the lock, then show it has not
        // gone past it: nothing of the instance exists.
        let observer = db.pool.get().await.expect("observer");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let waiting: i64 = observer
                .query_one(
                    "select count(*) from pg_locks where locktype = 'advisory' and not granted",
                    &[],
                )
                .await
                .expect("look for the waiting attach")
                .get(0);
            if waiting == 1 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the attach never queued on the lock"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(!attach.is_finished(), "the attach ran past a held lock");
        assert!(
            !schema_exists(&observer, "instance_waits")
                .await
                .expect("look up the schema"),
            "the attach created its schema while another held the lock"
        );

        holder.release().await;
        attach
            .await
            .expect("attach task")
            .expect("attach once the lock is free");
    }

    /// The attach runs on the lock's own session, and only the lock's wait
    /// gets [`ATTACH_LOCK_TIMEOUT`]: once the lock is held, the session is
    /// back under the usual cap, so the migrations that follow can't queue
    /// the instance's running workers behind a DDL lock for five minutes.
    #[tokio::test]
    async fn the_attach_runs_under_the_usual_lock_timeout_once_the_lock_is_held() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_empty_database().await;
        let config = Config::with_schema(db.dsn(), "instance_cap").expect("config");
        let pool = crate::pool::Pool::new(&config).expect("pool");

        let mut lock = AttachLock::acquire(&pool).await.expect("take the lock");
        let row = lock
            .client()
            .query_one(
                "select current_setting('lock_timeout'), \
                        exists(select 1 from pg_locks where locktype = 'advisory' \
                               and pid = pg_backend_pid() and granted)",
                &[],
            )
            .await
            .expect("read the session");
        let (lock_timeout, held): (String, bool) = (row.get(0), row.get(1));
        assert!(held, "the lock is held on the session the attach runs on");
        assert_eq!(
            lock_timeout,
            format!("{}s", crate::locks::LOCK_TIMEOUT.as_secs())
        );
        lock.release().await;
    }

    #[test]
    fn identity_display_is_human_readable() {
        let identity = Identity {
            schema: "trellis".to_string(),
            format_version: 1,
        };
        assert_eq!(
            identity.to_string(),
            "schema \"trellis\", instance format version 1"
        );
    }
}
