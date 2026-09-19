//! The subprocess-supervised backend (issue #166): [`ManualBackend`]'s
//! sibling `Backend` impl, differing *only* in how the engine that drains
//! the ring is started, stopped, and restarted. Every DDL/DML/read-back
//! concern (installing tables/relationships/definitions, applying raw
//! source DML, polling watermarks, reading back a [`Snapshot`]) is shared
//! with `ManualBackend` via [`super::sql`] — see that module's doc comment.
//!
//! **Why this exists**: `ManualBackend` runs the engine's `trellis::Client`
//! in-process (see its own module doc comment) — real, but not the shape a
//! genuine crash test needs. [`Backend::restart`]'s own doc comment already
//! flags this: `ManualBackend::restart` is a faithful *simulation* of a
//! crash (drop the in-process client, start a fresh one), not an actual
//! `SIGKILL`. `testkit::CrashGuard` — a real subprocess-`SIGKILL` primitive
//! — has sat unused since it was written for exactly this reason: nothing
//! in the suite ran the engine as a real OS process for it to supervise.
//! `SubprocessBackend` is that: [`SubprocessBackend::install`] spawns
//! `engine_subprocess` (a tiny bin, `generative/src/bin/engine_subprocess.rs`,
//! that starts a real `trellis::Client` and blocks) as a genuine child
//! process via [`CrashGuard::spawn`], and [`SubprocessBackend::restart`]
//! really does `SIGKILL` it and spawn a fresh one — see that method's doc
//! comment for how a caller can confirm the kill was real (not a clean
//! exit).
//!
//! **The deterministic pause point (issue #166, item 2)**: a blind
//! "apply an op, sleep a bit, `SIGKILL`" test would only *sometimes* land
//! inside Phase 3's open transaction — this suite's programs are tiny, and a
//! drain of them can complete in well under a millisecond. Instead,
//! [`SubprocessBackend::arm_pause_before_commit`]/
//! [`SubprocessBackend::wait_for_pause`] drive `trellis::staging::apply`'s
//! `pause_before_commit_for_tests` test-only hook (gated behind the
//! `trellis` crate's `test-util` feature, which this crate's `Cargo.toml`
//! enables): arm it, apply the op(s) meant to be interrupted, wait for the
//! hook's own marker file to appear (proof the engine is sitting inside an
//! *uncommitted* Phase 3 transaction), then `SIGKILL` with certainty about
//! exactly what got interrupted. See `generative/tests/subprocess_crash.rs`
//! for the end-to-end regression test this exists for.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use testkit::crash::CrashGuard;
use tokio_postgres::NoTls;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::TransformDef;
use trellis::defs::{CatalogError, DdlError, create_relationship, install_definition};
use trellis::staging::{StagingError, await_converged, watermark_token};
use trellis::{Config, Pool};

use super::Snapshot;
use super::sql;
use crate::model::{Op, Program, Table};

/// How long [`SubprocessBackend::quiesce`] waits for ring convergence and
/// definition-settle before giving up — same value and rationale as
/// `ManualBackend::QUIESCE_TIMEOUT`.
const QUIESCE_TIMEOUT: Duration = Duration::from_secs(30);

/// How long [`SubprocessBackend`] waits for a freshly spawned
/// `engine_subprocess` to write its readiness marker (i.e. for
/// `trellis::Client::start` to finish setup — publication/slot/snapshot
/// handshake) before giving up. Generous for the same reason
/// `ManualBackend::QUIESCE_TIMEOUT` is: a genuinely stuck startup should
/// time out loudly, not hang the suite.
const READY_TIMEOUT: Duration = Duration::from_secs(30);

/// Failure modes across the subprocess backend's lifecycle. Mirrors
/// `ManualBackendError`'s shape (same variant names wherever the condition
/// is the same), plus a couple of subprocess-specific ones
/// ([`SubprocessBackendError::Spawn`]/[`SubprocessBackendError::EngineNotReady`]).
#[derive(Debug)]
pub enum SubprocessBackendError {
    /// An op named a table [`SubprocessBackend::install`] was never given.
    UnknownTable {
        table: String,
    },
    /// [`SubprocessBackend::connect`] was given no explicitly-named
    /// connection target — see `ManualBackendError::UnnamedTarget`'s doc
    /// comment for the rationale (design doc §6).
    UnnamedTarget,
    /// [`SubprocessBackend::restart`]/[`SubprocessBackend::scale_out`] was
    /// called before [`SubprocessBackend::install`] ever spawned a primary
    /// engine subprocess.
    NoClientStarted,
    /// Spawning `engine_subprocess` itself failed (e.g. the binary path
    /// [`SubprocessBackend::connect`] was given doesn't exist).
    Spawn(std::io::Error),
    /// A freshly spawned `engine_subprocess` never wrote its readiness
    /// marker within [`READY_TIMEOUT`] — either it's stuck in
    /// `trellis::Client::start`'s setup, or it exited before ever reaching
    /// it (check its inherited stderr, which
    /// [`SubprocessBackend::spawn_engine`] leaves attached to this test
    /// process's own for exactly this kind of diagnosis).
    EngineNotReady {
        waited: Duration,
    },
    /// [`SubprocessBackend::quiesce`] waited [`QUIESCE_TIMEOUT`] for every
    /// installed definition to reach a terminal backfill outcome and at
    /// least one never did — see `ManualBackendError::DefinitionSettleTimeout`'s
    /// doc comment.
    DefinitionSettleTimeout {
        unsettled: Vec<String>,
        waited: Duration,
    },
    Config(trellis::Error),
    Catalog(CatalogError),
    Ddl(DdlError),
    Staging(StagingError),
    Db(tokio_postgres::Error),
}

impl From<trellis::Error> for SubprocessBackendError {
    fn from(err: trellis::Error) -> Self {
        SubprocessBackendError::Config(err)
    }
}

impl From<CatalogError> for SubprocessBackendError {
    fn from(err: CatalogError) -> Self {
        SubprocessBackendError::Catalog(err)
    }
}

impl From<DdlError> for SubprocessBackendError {
    fn from(err: DdlError) -> Self {
        SubprocessBackendError::Ddl(err)
    }
}

impl From<StagingError> for SubprocessBackendError {
    fn from(err: StagingError) -> Self {
        SubprocessBackendError::Staging(err)
    }
}

impl From<tokio_postgres::Error> for SubprocessBackendError {
    fn from(err: tokio_postgres::Error) -> Self {
        SubprocessBackendError::Db(err)
    }
}

impl From<sql::ApplyOpError> for SubprocessBackendError {
    fn from(err: sql::ApplyOpError) -> Self {
        match err {
            sql::ApplyOpError::UnknownTable(table) => {
                SubprocessBackendError::UnknownTable { table }
            }
            sql::ApplyOpError::Db(err) => SubprocessBackendError::Db(err),
        }
    }
}

static SCRATCH_COUNTER: AtomicU64 = AtomicU64::new(0);

/// The subprocess-supervised backend. Owns a raw connection (DDL, DML,
/// watermark reads, snapshot reads — identical role to `ManualBackend::raw`)
/// and, once [`SubprocessBackend::install`] has run, a real child process
/// (via [`CrashGuard`]) running `engine_subprocess` against the same
/// database.
pub struct SubprocessBackend {
    dsn: String,
    pool: Pool,
    raw: tokio_postgres::Client,
    /// Path to the `engine_subprocess` binary this backend spawns — see
    /// [`SubprocessBackend::connect`]'s doc comment for how a caller
    /// resolves this (`env!("CARGO_BIN_EXE_engine_subprocess")` from an
    /// integration test).
    engine_bin: PathBuf,
    tables: HashMap<String, Table>,
    defs: Vec<TransformDef>,
    slot: String,
    publication: String,
    application_threads: usize,
    maintenance_interval: Duration,
    /// Remembered from the first successful [`SubprocessBackend::install`]
    /// (which is the only call that ever computes it from a [`Program`]) so
    /// [`SubprocessBackend::restart`] can respawn against the exact same
    /// source-table set without the caller handing it back in — the
    /// subprocess analog of `ManualBackend::client_options`.
    source_tables: Vec<String>,
    /// The primary engine subprocess, once spawned. `None` before the first
    /// [`SubprocessBackend::install`] call that has at least one table.
    child: Option<CrashGuard>,
    /// Additional application-worker-only subprocesses started by
    /// [`SubprocessBackend::scale_out`], kept alive for this backend's own
    /// lifetime — same role as `ManualBackend::scale_out_clients`.
    scale_out_children: Vec<CrashGuard>,
    /// The exit status [`SubprocessBackend::restart`] observed the last time
    /// it killed a running child, if any — exposed via
    /// [`SubprocessBackend::last_kill_status`] so a crash test can confirm
    /// the process really was `SIGKILL`ed (`ExitStatusExt::signal() ==
    /// Some(9)`) rather than having exited cleanly on its own beforehand.
    last_kill_status: Option<ExitStatus>,
    /// A scratch directory (removed on `Drop`) holding this backend's
    /// readiness/pause-hook marker files — see
    /// [`SubprocessBackend::arm_pause_before_commit`]'s doc comment for the
    /// pause-hook files specifically.
    scratch_dir: PathBuf,
    ready_marker_path: PathBuf,
    pause_trigger_path: PathBuf,
    pause_marker_path: PathBuf,
}

impl SubprocessBackend {
    /// Connects to `dsn` — an already-migrated Trellis database (see
    /// `testkit::TestCluster::create_isolated_database`) — but installs
    /// nothing yet and spawns no subprocess. `engine_bin` is the path to the
    /// `engine_subprocess` binary this backend will `Command::spawn()` once
    /// [`SubprocessBackend::install`] has at least one source table; callers
    /// (integration tests, which Cargo builds this crate's `src/bin/*.rs`
    /// targets alongside) resolve it via
    /// `env!("CARGO_BIN_EXE_engine_subprocess")` — that env var is only
    /// populated for test/bench/example compilation units, which is why this
    /// takes the path as a parameter rather than resolving it itself (this
    /// module compiles as part of the plain library target, which never
    /// gets it).
    ///
    /// One application worker, the engine's default maintenance cadence,
    /// the shared default `"trellis_slot"`/`"trellis_pub"` names — see
    /// [`SubprocessBackend::connect_with_options`]/
    /// [`SubprocessBackend::set_slot_and_publication`] to override any of
    /// those (same rationale as `ManualBackend`'s equivalents — issue #188).
    ///
    /// `dsn` must be given explicitly (design doc §6: refuse an unnamed
    /// target), matching `ManualBackend::connect`.
    pub async fn connect(
        dsn: impl Into<String>,
        engine_bin: impl Into<PathBuf>,
    ) -> Result<Self, SubprocessBackendError> {
        Self::connect_with_options(dsn, engine_bin, 1, None).await
    }

    /// [`SubprocessBackend::connect`]'s general form — `application_threads`
    /// app-worker tasks inside the spawned subprocess, and (when `Some`) the
    /// subprocess's maintenance-loop cadence, mirroring
    /// `ManualBackend::connect_with_options`.
    pub async fn connect_with_options(
        dsn: impl Into<String>,
        engine_bin: impl Into<PathBuf>,
        application_threads: usize,
        maintenance_interval: Option<Duration>,
    ) -> Result<Self, SubprocessBackendError> {
        let dsn = dsn.into();
        if dsn.trim().is_empty() {
            return Err(SubprocessBackendError::UnnamedTarget);
        }
        println!(
            "generative: connecting SubprocessBackend to {dsn} ({application_threads} \
             application worker(s))"
        );
        let config = Config::from_dsn(dsn.clone())?;
        let pool = Pool::new(&config)?;

        let (raw, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        raw.batch_execute(&format!("set search_path to {}, public", config.schema()))
            .await?;

        let scratch_dir = std::env::temp_dir().join(format!(
            "trellis-subprocess-backend-{}-{}",
            std::process::id(),
            SCRATCH_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&scratch_dir).map_err(SubprocessBackendError::Spawn)?;
        let ready_marker_path = scratch_dir.join("ready");
        let pause_trigger_path = scratch_dir.join("pause-trigger");
        let pause_marker_path = scratch_dir.join("pause-marker");

        Ok(Self {
            dsn,
            pool,
            raw,
            engine_bin: engine_bin.into(),
            tables: HashMap::new(),
            defs: Vec::new(),
            slot: "trellis_slot".to_string(),
            publication: "trellis_pub".to_string(),
            application_threads,
            maintenance_interval: maintenance_interval
                .unwrap_or_else(|| trellis::ClientOptions::default().maintenance_interval),
            source_tables: Vec::new(),
            child: None,
            scale_out_children: Vec::new(),
            last_kill_status: None,
            scratch_dir,
            ready_marker_path,
            pause_trigger_path,
            pause_marker_path,
        })
    }

    /// Overrides the slot/publication names the primary `engine_subprocess`
    /// is spawned with, instead of the shared `"trellis_slot"`/
    /// `"trellis_pub"` defaults — same issue #188 rationale as
    /// `ManualBackend::set_slot_and_publication`: a logical replication slot
    /// name is unique cluster-wide, so two backends sharing one Postgres
    /// cluster (even across separate isolated databases) need distinct
    /// names. Must be called before the first [`SubprocessBackend::install`]
    /// (which is the only call that spawns the primary subprocess); a
    /// subprocess already spawned ignores a later call.
    pub fn set_slot_and_publication(
        &mut self,
        slot: impl Into<String>,
        publication: impl Into<String>,
    ) {
        self.slot = slot.into();
        self.publication = publication.into();
    }

    /// Arms `trellis::staging::apply`'s test-only pre-commit pause hook
    /// (issue #166; see that module's `pause_before_commit_for_tests` doc
    /// comment for the full mechanism): creates the trigger file the hook
    /// polls for, so the *next* Phase 3 commit the primary engine
    /// subprocess attempts pauses indefinitely, right after every write and
    /// right before `COMMIT`, instead of proceeding normally.
    ///
    /// Callers must pair this with [`SubprocessBackend::wait_for_pause`] (to
    /// confirm the pause actually engaged before sending a kill) and
    /// [`SubprocessBackend::disarm_pause_before_commit`] (before restarting,
    /// so the *redrive* isn't paused too — see that method's doc comment).
    pub fn arm_pause_before_commit(&self) -> std::io::Result<()> {
        // Clear any marker a *previous* arm/kill cycle left behind first, so
        // `wait_for_pause` can only ever observe a marker this arming
        // produced. Without this, a second arm in the same test would return
        // `true` instantly off the stale file and the `SIGKILL` would land
        // nowhere near an open transaction — a silently vacuous crash test.
        let _ = std::fs::remove_file(&self.pause_marker_path);
        std::fs::write(&self.pause_trigger_path, b"armed")
    }

    /// Removes the pause hook's trigger file, so any *subsequent* Phase 3
    /// commit proceeds normally. Must be called before
    /// [`SubprocessBackend::restart`] in a SIGKILL-mid-drain test: the
    /// respawned subprocess inherits the same `TRELLIS_TEST_PAUSE_TRIGGER`
    /// path, and would otherwise immediately re-pause on its very first
    /// commit while redraining the segment the killed process never
    /// finished — hanging the test instead of proving redrive completes.
    pub fn disarm_pause_before_commit(&self) {
        let _ = std::fs::remove_file(&self.pause_trigger_path);
    }

    /// Polls (bounded by `timeout`) for the pause hook's marker file — the
    /// signal that the primary engine subprocess is now actually parked
    /// inside an *uncommitted* Phase 3 transaction, not just that
    /// [`SubprocessBackend::arm_pause_before_commit`] was called. Only once
    /// this returns `true` is it safe to assume a `SIGKILL` will land inside
    /// that exact window rather than before Phase 3 even started or after it
    /// already committed.
    pub async fn wait_for_pause(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.pause_marker_path.exists() {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// The exit status [`SubprocessBackend::restart`] most recently observed
    /// after killing a running primary child, if any. A SIGKILL-mid-drain
    /// test uses this to confirm the process it killed really was
    /// terminated by `SIGKILL` (`std::os::unix::process::ExitStatusExt::signal()
    /// == Some(9)`), not that it happened to exit cleanly on its own just
    /// before `restart` reached it.
    pub fn last_kill_status(&self) -> Option<ExitStatus> {
        self.last_kill_status
    }

    async fn install_definition(
        &mut self,
        def: &TransformDef,
    ) -> Result<(), SubprocessBackendError> {
        let source_table = self
            .tables
            .get(&def.source)
            .ok_or_else(|| SubprocessBackendError::UnknownTable {
                table: def.source.clone(),
            })?
            .clone();
        let source_columns = sql::source_columns(&source_table);
        let text = sql::render_definition(def);
        // Same front door real callers and `ManualBackend` use — see
        // `ManualBackend::install_definition`'s doc comment (issue #63 C1).
        install_definition(&self.pool, &text, &source_columns, "public").await?;
        Ok(())
    }

    /// Spawns `engine_subprocess` as a real child process (via
    /// [`CrashGuard::spawn`]), configured entirely through environment
    /// variables (the pattern `testkit::crash::CrashGuard`'s own doc comment
    /// calls for: "a binary... re-invoked with an env var... telling it
    /// which operation to run"), and blocks (bounded by [`READY_TIMEOUT`])
    /// until it signals readiness by writing [`Self::ready_marker_path`].
    ///
    /// stdout/stderr are inherited (not piped/discarded): a subprocess that
    /// fails to start, or panics, prints straight into this test process's
    /// own output — the same visibility a real deployment's logs would give
    /// an operator, and essential for diagnosing an
    /// [`SubprocessBackendError::EngineNotReady`] timeout.
    async fn spawn_engine(&mut self, staging_worker: bool) -> Result<(), SubprocessBackendError> {
        let _ = std::fs::remove_file(&self.ready_marker_path);

        let mut command = Command::new(&self.engine_bin);
        command
            .env("TRELLIS_DSN", &self.dsn)
            .env("TRELLIS_STAGING_WORKER", staging_worker.to_string())
            .env(
                "TRELLIS_APPLICATION_THREADS",
                if staging_worker {
                    self.application_threads.to_string()
                } else {
                    // An application-worker-only subprocess (`scale_out`)
                    // always runs exactly one drain worker — mirrors
                    // `ManualBackend::scale_out`'s hardcoded `application_threads: 1`.
                    "1".to_string()
                },
            )
            .env("TRELLIS_SOURCE_TABLES", self.source_tables.join(","))
            .env("TRELLIS_SLOT", &self.slot)
            .env("TRELLIS_PUBLICATION", &self.publication)
            .env(
                "TRELLIS_MAINTENANCE_INTERVAL_MS",
                self.maintenance_interval.as_millis().to_string(),
            )
            .env("TRELLIS_READY_MARKER", &self.ready_marker_path)
            // Issue #166's pause hook — see this module's doc comment. Set
            // unconditionally (every spawned subprocess, primary or
            // scale-out): a no-op unless `arm_pause_before_commit` later
            // creates the trigger file, per `pause_before_commit_for_tests`'s
            // own doc comment in `trellis::staging::apply`.
            .env("TRELLIS_TEST_PAUSE_TRIGGER", &self.pause_trigger_path)
            .env("TRELLIS_TEST_PAUSE_MARKER", &self.pause_marker_path)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());

        let guard = CrashGuard::spawn(&mut command).map_err(SubprocessBackendError::Spawn)?;
        if staging_worker {
            self.child = Some(guard);
        } else {
            self.scale_out_children.push(guard);
        }

        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            if self.ready_marker_path.exists() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(SubprocessBackendError::EngineNotReady {
                    waited: READY_TIMEOUT,
                });
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

impl Drop for SubprocessBackend {
    fn drop(&mut self) {
        // `self.child`/`self.scale_out_children` drop right after this body
        // (struct-field drop order), which is what actually SIGKILLs any
        // still-running subprocess (`CrashGuard::drop`) — this only cleans
        // up the scratch directory those processes' marker files lived in.
        let _ = std::fs::remove_dir_all(&self.scratch_dir);
    }
}

impl super::Backend for SubprocessBackend {
    type Error = SubprocessBackendError;

    async fn install(&mut self, program: &Program) -> Result<(), SubprocessBackendError> {
        for table in &program.tables {
            sql::create_source_table(&self.raw, table).await?;
            self.tables.insert(table.name.clone(), table.clone());
        }
        // Issue #34: relationships before definitions — see
        // `ManualBackend::install`'s identical comment for why this
        // ordering is always safe.
        for rel in &program.relationships {
            create_relationship(&self.pool, &sql::render_relationship(rel)).await?;
        }
        for def in &program.defs {
            self.install_definition(def).await?;
            self.defs.push(def.clone());
        }

        let source_tables: Vec<String> = program
            .tables
            .iter()
            .map(|t| format!("{DEFAULT_SCHEMA}.{}", t.name))
            .collect();
        if !source_tables.is_empty() && self.child.is_none() {
            self.source_tables = source_tables;
            self.spawn_engine(true).await?;
        }
        Ok(())
    }

    async fn apply(&mut self, op: &Op) -> Result<u64, SubprocessBackendError> {
        sql::apply_op(&mut self.raw, &self.tables, op)
            .await
            .map_err(SubprocessBackendError::from)
    }

    async fn quiesce(&mut self) -> Result<(), SubprocessBackendError> {
        let token = watermark_token(&self.raw).await?;
        await_converged(&self.raw, token, QUIESCE_TIMEOUT).await?;

        // Same gap `ManualBackend::quiesce` closes (public-api-design
        // review) and for the same reason: a direct-build 1-1 definition's
        // backfill runs entirely outside the ring
        // (docs/decisions/0007's amendment), invisible to `await_converged`.
        sql::await_definitions_settled(&self.raw, &self.defs, QUIESCE_TIMEOUT)
            .await
            .map_err(|err| match err {
                sql::DefinitionSettleError::Db(err) => SubprocessBackendError::Db(err),
                sql::DefinitionSettleError::Timeout { unsettled, waited } => {
                    SubprocessBackendError::DefinitionSettleTimeout { unsettled, waited }
                }
            })
    }

    async fn snapshot(&mut self) -> Result<Snapshot, SubprocessBackendError> {
        use std::collections::BTreeMap;
        use trellis::defs::ast::{KeySpace, ValueType};
        use trellis::defs::{qualified_target_table, require_single_column_pk, source_primary_key};

        let mut snapshot: Snapshot = BTreeMap::new();

        for table in self.tables.values() {
            let rows = sql::read_table(
                &self.raw,
                &sql::quote_ident(&table.name),
                &table.pk_col,
                &table.columns,
            )
            .await?;
            snapshot.insert(table.name.clone(), rows);
        }

        for def in &self.defs {
            let qualified = qualified_target_table("public", def);
            let rows = match &def.key_space {
                KeySpace::OneToOne => {
                    let pk = require_single_column_pk(
                        source_primary_key(&self.pool, &def.source).await?,
                        &def.source,
                    )?;
                    let target_columns: Vec<crate::model::Column> =
                        std::iter::once(crate::model::Column {
                            name: pk.name.clone(),
                            value_type: ValueType::Numeric,
                        })
                        .chain(def.fields.iter().map(|f| crate::model::Column {
                            name: f.name.clone(),
                            value_type: ValueType::Text,
                        }))
                        .collect();
                    sql::read_table(&self.raw, &qualified, &pk.name, &target_columns).await?
                }
                KeySpace::Aggregate { group_by } => {
                    let group_by_names: Vec<String> = group_by
                        .iter()
                        .map(|k| k.target_column_name().to_string())
                        .collect();
                    sql::read_aggregate_table(&self.raw, &qualified, &group_by_names, &def.fields)
                        .await?
                }
            };
            snapshot.insert(def.target.clone(), rows);
        }

        Ok(snapshot)
    }

    /// The real thing (issue #166), unlike `ManualBackend::restart`'s
    /// in-process simulation: `SIGKILL`s the currently running primary
    /// subprocess (via [`CrashGuard::kill`]), waits for it to actually exit
    /// (recording the exit status — see [`SubprocessBackend::last_kill_status`]),
    /// then spawns a fresh one against the exact same dsn/slot/publication/
    /// source-table set `install` originally used. The ring is durable
    /// Postgres state untouched by any of this, so the fresh subprocess is
    /// expected to redrive exactly whatever the killed one left mid-flight —
    /// including, if [`SubprocessBackend::arm_pause_before_commit`] paused it
    /// there, an entire uncommitted Phase 3 batch, which Postgres's own
    /// transaction rollback (triggered by the killed process's connection
    /// dropping) already guarantees was never partially applied.
    async fn restart(&mut self) -> Result<(), SubprocessBackendError> {
        let mut guard = self
            .child
            .take()
            .ok_or(SubprocessBackendError::NoClientStarted)?;
        // `kill` (SIGKILL) then `wait`, not just letting `guard` drop:
        // `CrashGuard::drop` performs the same two calls best-effort, but
        // doing it explicitly here means this method can surface the real
        // exit status via `last_kill_status` rather than discarding it.
        let _ = guard.kill();
        let status = guard.wait();
        drop(guard);
        self.last_kill_status = status.ok();

        self.spawn_engine(true).await
    }

    /// Starts an additional, application-worker-only (`staging_worker:
    /// false`) `engine_subprocess`, alongside whatever primary subprocess
    /// `install` already started — the subprocess analog of
    /// `ManualBackend::scale_out` (improvement-plan task E3).
    async fn scale_out(&mut self) -> Result<(), SubprocessBackendError> {
        self.spawn_engine(false).await
    }
}
