//! An ephemeral, disposable Postgres instance for integration tests, plus
//! per-scenario database isolation on top of one running instance.
//!
//! [`TestCluster`] owns one `postgres` server process listening only on a
//! unix socket in a throwaway temp directory; both are cleaned up when it's
//! dropped. It's started with `wal_level=logical` (and matching
//! `max_replication_slots`/`max_wal_senders`) so intake's replication-slot
//! machinery can be exercised against it.
//! [`TestCluster::create_isolated_database`] then hands out a fresh,
//! migrated database per logical test scenario, so several scenarios can
//! share one running instance — avoiding a repeated `initdb` per test —
//! without bleeding state into each other.
//!
//! Each test function is expected to own its own [`TestCluster`] as a local
//! variable. That keeps teardown a plain, guaranteed `Drop` at the end of
//! the test — including on panic/unwind — rather than relying on a shared
//! global that would risk leaking a `postgres` process, since Rust doesn't
//! run statics' destructors on normal process exit.
//!
//! `Drop` only covers a *normal unwind*, though. A signal-kill of the test
//! binary (Ctrl-C, an editor/CI cancel, SIGKILL) runs no destructor, so every
//! live cluster leaks its temp dir *and* the SysV shared-memory segment
//! Postgres holds as a startup interlock. Because that segment table is tiny
//! on macOS (`kern.sysv.shmmni` defaults to 32), a few killed runs exhaust it
//! and `initdb` starts failing for everyone (see issue #43). Since no handler
//! can trap SIGKILL, teardown can't be the only defense: [`TestCluster::start`]
//! first runs a self-healing reaper ([`reap_orphans_in`]) that sweeps orphaned
//! `trellis-testkit-*` dirs from dead prior runs, freeing each one's leaked
//! segment before allocating a new one.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Condvar, Mutex, Once};
use std::time::{Duration, Instant};
use trellis::{Config, Pool, migrate};

static COUNTER: AtomicU32 = AtomicU32::new(0);

fn unique_suffix() -> String {
    format!(
        "{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// Cap on concurrently-live clusters within a test process. Each running
/// `postgres` holds one SysV shared-memory segment as a startup interlock,
/// and macOS's `kern.sysv.shmseg` defaults to 8; without a cap, libtest
/// fanning tests across many cores starts enough clusters at once to exhaust
/// the segment table and fail `initdb`. 4 stays well under the limit with
/// headroom for a dropping cluster whose segment isn't freed until its
/// `pg_ctl stop` returns.
const MAX_LIVE_CLUSTERS: usize = 4;
static LIVE_CLUSTERS: (Mutex<usize>, Condvar) = (Mutex::new(0), Condvar::new());

/// RAII permit for one live cluster. Acquired before `initdb` and released
/// only when dropped — which, for a permit held in [`TestCluster`], happens
/// after the server has been stopped and its segment freed. Held as a local
/// first in `start()` so a panic during setup releases it rather than
/// deadlocking later tests.
struct ClusterPermit;

impl ClusterPermit {
    fn acquire() -> Self {
        let (lock, cvar) = &LIVE_CLUSTERS;
        let mut live = lock.lock().expect("cluster permit lock");
        while *live >= MAX_LIVE_CLUSTERS {
            live = cvar.wait(live).expect("cluster permit wait");
        }
        *live += 1;
        ClusterPermit
    }
}

impl Drop for ClusterPermit {
    fn drop(&mut self) {
        let (lock, cvar) = &LIVE_CLUSTERS;
        let mut live = lock.lock().expect("cluster permit lock");
        *live -= 1;
        cvar.notify_one();
    }
}

/// One ephemeral Postgres server, reachable only over a unix socket,
/// configured for logical replication.
pub struct TestCluster {
    root: PathBuf,
    data_dir: PathBuf,
    socket_dir: PathBuf,
    port: u16,
    // Behind a `Mutex` so [`TestCluster::restart`] can replace the process
    // through a shared reference: the generative suite keeps its cluster in
    // a `thread_local` and only ever hands out `&TestCluster`.
    server: Mutex<Child>,
    // Released after `Drop` stops the server (fields drop after the explicit
    // `Drop::drop` body), so a freed permit means a freed shmem segment.
    _permit: ClusterPermit,
}

impl TestCluster {
    /// Initializes and starts a fresh, empty Postgres instance with
    /// `wal_level=logical` (and matching replication slot/sender limits),
    /// waiting until it accepts connections. Panics on any setup failure —
    /// this is test-only scaffolding, not a place to build resilient error
    /// handling.
    pub fn start() -> Self {
        // Reclaim segments/dirs leaked by prior runs that were killed before
        // `Drop` could stop their server. Once per process is enough: our own
        // live clusters are protected by the liveness check, and `Drop` frees
        // this process's segments directly, so nothing new to reap accrues
        // from us mid-run.
        reap_orphans_once();

        // Gate concurrent clusters before any `initdb`; a local so setup
        // panics release it, then moved into the returned cluster.
        let permit = ClusterPermit::acquire();

        let root = std::env::temp_dir().join(format!("trellis-testkit-{}", unique_suffix()));
        let data_dir = root.join("data");
        let socket_dir = root.join("sock");
        fs::create_dir_all(&socket_dir).expect("create socket dir");

        let port: u16 = 5432;

        // `initdb`'s bootstrap backend transiently allocates a SysV segment,
        // so on a machine whose small system-wide table (macOS `shmmni`
        // defaults to 32) is under pressure from other Postgres instances it
        // can fail spuriously. Retry a few times with backoff before giving
        // up — the pressure is transient as other segments are freed.
        run_with_retries(
            || {
                // A failed `initdb` can leave a partial data dir it would then
                // refuse to reuse, so clear it before each attempt.
                let _ = fs::remove_dir_all(&data_dir);
                let mut cmd = Command::new("initdb");
                cmd.arg("-D")
                    .arg(&data_dir)
                    .arg("-U")
                    .arg("postgres")
                    .arg("--auth=trust")
                    .arg("--no-sync");
                cmd
            },
            "initdb",
        );

        let log_path = root.join("postgres.log");
        let server = spawn_server(&data_dir, &socket_dir, port, &log_path);

        let cluster = Self {
            root,
            data_dir,
            socket_dir,
            port,
            server: Mutex::new(server),
            _permit: permit,
        };
        cluster.wait_ready(&log_path);
        cluster
    }

    fn wait_ready(&self, log_path: &Path) {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let status = Command::new("pg_isready")
                .arg("-h")
                .arg(&self.socket_dir)
                .arg("-p")
                .arg(self.port.to_string())
                .arg("-U")
                .arg("postgres")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();

            if matches!(status, Ok(s) if s.success()) {
                return;
            }

            if Instant::now() >= deadline {
                let log = fs::read_to_string(log_path).unwrap_or_default();
                panic!("postgres did not become ready in time; log:\n{log}");
            }

            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// The temp directory backing this instance (data dir, socket dir,
    /// log). Exposed mainly so tests of the harness itself can assert it's
    /// gone after teardown.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The OS process id of the `postgres` server. Exposed mainly so tests
    /// of the harness itself can assert the process has exited after
    /// teardown.
    pub fn server_pid(&self) -> u32 {
        self.server.lock().expect("server lock").id()
    }

    /// Stops the server and starts it again on the same data directory,
    /// socket and port, waiting until it accepts connections. Every open
    /// connection is severed, the way a real Postgres restart severs them;
    /// data, replication slots and configuration survive.
    ///
    /// [`StopMode::Fast`] is an orderly shutdown (a shutdown checkpoint, then
    /// exit). [`StopMode::Immediate`] skips the checkpoint, so the next start
    /// runs crash recovery from WAL — the same path a power loss takes.
    ///
    /// Panics if the server doesn't stop or doesn't come back, like the rest
    /// of this module.
    pub fn restart(&self, mode: StopMode) {
        self.while_stopped(mode, || {});
    }

    /// Takes a cold, file-level backup: stops the server with a
    /// [`StopMode::Fast`] shutdown (so the copy is of a cleanly shut-down
    /// cluster, shutdown checkpoint and all), copies the whole data
    /// directory, and starts the server again on the original. Every open
    /// connection is severed, as with [`TestCluster::restart`].
    ///
    /// The copy carries everything in the data directory, replication slots
    /// (`pg_replslot/`) included, so a cluster restored from it with
    /// [`TestCluster::from_backup`] has each slot exactly where it stood at
    /// the shutdown. That is the difference from a `pg_basebackup`, which
    /// leaves `pg_replslot/` out.
    ///
    /// The backup lives in its own temp directory until the returned
    /// [`ClusterBackup`] is dropped. It is a full copy of the cluster, WAL
    /// included, so drop it as soon as it has been restored from.
    pub fn cold_backup(&self) -> ClusterBackup {
        let root = std::env::temp_dir().join(format!("trellis-testkit-{}", unique_suffix()));
        let backup = ClusterBackup { root };
        fs::create_dir_all(&backup.root).expect("create backup dir");
        self.while_stopped(StopMode::Fast, || {
            copy_data_dir(&self.data_dir, &backup.data_dir());
        });
        backup
    }

    /// Starts a new, independent cluster on a copy of `backup`'s data
    /// directory, in its own temp directory and on its own socket, and waits
    /// until it accepts connections. The server comes up the way a restored
    /// production server would: from the backed-up files, with no `initdb`
    /// and no migration. Every database the backed-up cluster had is there
    /// under the same name; [`TestCluster::database_dsn`] reaches one.
    ///
    /// `backup` is copied, not consumed, so it can be restored from more
    /// than once. Torn down on drop like any other [`TestCluster`].
    pub fn from_backup(backup: &ClusterBackup) -> Self {
        reap_orphans_once();
        let permit = ClusterPermit::acquire();

        let root = std::env::temp_dir().join(format!("trellis-testkit-{}", unique_suffix()));
        let data_dir = root.join("data");
        let socket_dir = root.join("sock");
        fs::create_dir_all(&socket_dir).expect("create socket dir");
        copy_data_dir(&backup.data_dir(), &data_dir);

        let port: u16 = 5432;
        let log_path = root.join("postgres.log");
        let server = spawn_server(&data_dir, &socket_dir, port, &log_path);

        let cluster = Self {
            root,
            data_dir,
            socket_dir,
            port,
            server: Mutex::new(server),
            _permit: permit,
        };
        cluster.wait_ready(&log_path);
        cluster
    }

    /// The libpq keyword/value connection string for database `name` on
    /// this cluster. Mostly for reaching a database on a cluster started
    /// [`TestCluster::from_backup`], which has no [`TestDatabase`] handle
    /// for it.
    pub fn database_dsn(&self, name: &str) -> String {
        format!(
            "host={} port={} user=postgres dbname={}",
            self.socket_dir.display(),
            self.port,
            name
        )
    }

    /// Stops the server in `mode`, runs `while_down` with it stopped, then
    /// starts it again on the same data directory, socket and port and
    /// waits until it accepts connections.
    fn while_stopped(&self, mode: StopMode, while_down: impl FnOnce()) {
        let mut server = self.server.lock().expect("server lock");
        run_to_completion(
            Command::new("pg_ctl")
                .arg("stop")
                .arg("-D")
                .arg(&self.data_dir)
                .arg("-m")
                .arg(mode.as_pg_ctl_arg())
                .arg("-w")
                .arg("-t")
                .arg("30"),
            "pg_ctl stop",
        );
        let _ = server.wait();
        while_down();
        let log_path = self.root.join("postgres.log");
        *server = spawn_server(&self.data_dir, &self.socket_dir, self.port, &log_path);
        drop(server);
        self.wait_ready(&log_path);
    }

    /// Creates a fresh, uniquely-named, otherwise-empty database on this
    /// instance with its own connection pool. No migrations are applied —
    /// useful for tests that exercise [`trellis::migrate`] itself. Most
    /// callers want [`TestCluster::create_isolated_database`] instead.
    pub async fn create_empty_database(&self) -> TestDatabase {
        let name = format!("trellis_test_{}", unique_suffix());

        run_to_completion(
            Command::new("createdb")
                .arg("-h")
                .arg(&self.socket_dir)
                .arg("-p")
                .arg(self.port.to_string())
                .arg("-U")
                .arg("postgres")
                .arg(&name),
            "createdb",
        );

        let dsn = self.database_dsn(&name);

        let config = Config::from_dsn(dsn.clone()).expect("valid schema");
        let pool = Pool::new(&config).expect("build pool for isolated database");

        TestDatabase {
            socket_dir: self.socket_dir.clone(),
            port: self.port,
            name,
            dsn,
            pool,
        }
    }

    /// Creates a fresh, uniquely-named database on this instance, applies
    /// the engine's migrations to it, and returns a handle with its own
    /// connection pool. The database is dropped when the returned
    /// [`TestDatabase`] is dropped, so concurrent callers against the same
    /// [`TestCluster`] never see each other's tables or data.
    pub async fn create_isolated_database(&self) -> TestDatabase {
        let db = self.create_empty_database().await;
        let config = Config::from_dsn(db.dsn().to_string()).expect("valid schema");
        migrate(&db.pool, &config)
            .await
            .expect("apply migrations to isolated database");
        db
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        // Postgres can't trap SIGKILL, so a straight `Child::kill()` would
        // skip shutdown and leak the SysV shared memory segment it allocates
        // as a startup interlock — one per teardown, quickly exhausting the
        // (often tiny, e.g. macOS's 32-segment) system table and breaking
        // every subsequent `initdb`. Stop through `pg_ctl` first (the
        // postmaster frees its segment on the way out), falling back to
        // SIGKILL only on timeout.
        //
        // `immediate`, not `fast`: the data directory is deleted right after
        // this, so the shutdown checkpoint `fast` writes buys nothing. And
        // `fast` waits for every walsender to finish streaming and for its
        // client to confirm it. A test that leaves a replication consumer
        // attached at teardown (an `Intake` spawned onto the runtime, say)
        // never sends that confirmation, so `fast` sat out the whole `-t`
        // timeout and then SIGKILLed anyway, about 10s per test. `immediate`
        // still goes through the postmaster, which releases the SysV segment
        // before it exits.
        let stopped_via_pg_ctl = Command::new("pg_ctl")
            .arg("stop")
            .arg("-D")
            .arg(&self.data_dir)
            .arg("-m")
            .arg("immediate")
            .arg("-w")
            .arg("-t")
            .arg("10")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false);

        let server = self.server.get_mut().unwrap_or_else(|e| e.into_inner());
        if !stopped_via_pg_ctl {
            // `pg_ctl stop` failed or timed out; SIGKILL can't clean up, so
            // free the segment ourselves from the pidfile before it's lost.
            // Log it: a fallback here is the one leak path teardown *can*
            // see, and a silent one turns into flaky `initdb` failures later.
            eprintln!(
                "testkit: `pg_ctl stop` did not stop postgres cleanly (pid {}); \
                 falling back to SIGKILL and reaping its shmem segment",
                server.id()
            );
            let _ = server.kill();
            let _ = server.wait();
            reap_shmem_segment(&self.data_dir);
        } else {
            let _ = server.wait();
        }
        let _ = fs::remove_dir_all(&self.root);
    }
}

/// A cold, file-level copy of a stopped [`TestCluster`]'s data directory,
/// from [`TestCluster::cold_backup`]. Restore it with
/// [`TestCluster::from_backup`]. The copy is deleted when this is dropped.
///
/// It sits in a `trellis-testkit-<pid>-<n>` temp directory like a cluster's
/// own, so if the test process is killed before `Drop` runs, the next
/// [`TestCluster::start`]'s orphan reaper removes it (it has no
/// `postmaster.pid`, and its owning process is dead).
pub struct ClusterBackup {
    root: PathBuf,
}

impl ClusterBackup {
    fn data_dir(&self) -> PathBuf {
        self.root.join("data")
    }

    /// The temp directory holding the copy. Exposed so tests of the harness
    /// itself can assert it's gone after drop.
    pub fn root(&self) -> &Path {
        &self.root
    }
}

impl Drop for ClusterBackup {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

/// Copies a stopped cluster's data directory `from` to `to` (which must not
/// exist yet), keeping permissions: Postgres refuses to start on a data
/// directory anyone but its owner can read.
fn copy_data_dir(from: &Path, to: &Path) {
    run_to_completion(
        Command::new("cp").arg("-Rp").arg(from).arg(to),
        "copy data directory",
    );
}

/// A uniquely-named database on a [`TestCluster`], with its own connection
/// pool. Dropped (including the underlying database) when this handle goes
/// out of scope.
pub struct TestDatabase {
    socket_dir: PathBuf,
    port: u16,
    name: String,
    dsn: String,
    pub pool: Pool,
}

impl TestDatabase {
    /// The libpq keyword/value connection string for this database.
    pub fn dsn(&self) -> &str {
        &self.dsn
    }

    /// The database's name, e.g. for building a second connection outside
    /// the pool (as [`crate::crash::OpenTransaction`] does).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The cluster's Unix socket directory, for callers that need to build
    /// a connection [`Self::dsn`] doesn't cover — e.g. intake's replication
    /// transport, which speaks a different wire protocol
    /// (`pgwire_replication::ReplicationConfig::unix` wants the socket
    /// directory and port separately, not a libpq keyword/value string).
    pub fn socket_dir(&self) -> &Path {
        &self.socket_dir
    }

    /// The cluster's port. See [`Self::socket_dir`].
    pub fn port(&self) -> u16 {
        self.port
    }
}

/// How many times [`TestDatabase`]'s `Drop` tries `dropdb` before giving up,
/// [`DROP_RETRY_DELAY`] apart.
const DROP_ATTEMPTS: u32 = 50;
const DROP_RETRY_DELAY: Duration = Duration::from_millis(100);

impl Drop for TestDatabase {
    fn drop(&mut self) {
        // `--force` (PG 13+) disconnects any backends still attached before
        // dropping, so teardown doesn't race connections just released to
        // `pool`. It does not cover a logical slot that is still *active*:
        // `dropdb` refuses that outright, before `--force` terminates
        // anything. A `trellis::Client` whose best-effort shutdown hasn't
        // finished leaves exactly that. Were the failure ignored, the
        // database and its slot would leak, and the slot, inactive a moment
        // later, would pin the cluster's WAL from then on. A deep nightly
        // run puts hundreds of cases on one cluster, so that retained WAL
        // grew to gigabytes and filled the tmpfs quota. So end the slot's
        // walsender and retry; once the slot is inactive, `dropdb` drops it
        // along with the database.
        for _ in 0..DROP_ATTEMPTS {
            let dropped = Command::new("dropdb")
                .arg("-h")
                .arg(&self.socket_dir)
                .arg("-p")
                .arg(self.port.to_string())
                .arg("-U")
                .arg("postgres")
                .arg("--force")
                .arg("--if-exists")
                .arg(&self.name)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|status| status.success());
            if dropped {
                return;
            }
            let _ = Command::new("psql")
                .arg("-h")
                .arg(&self.socket_dir)
                .arg("-p")
                .arg(self.port.to_string())
                .arg("-U")
                .arg("postgres")
                .arg("-d")
                .arg("postgres")
                .arg("-c")
                .arg(format!(
                    "select pg_terminate_backend(active_pid) from pg_replication_slots \
                     where database = '{}' and active_pid is not null",
                    self.name
                ))
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
            std::thread::sleep(DROP_RETRY_DELAY);
        }
        eprintln!(
            "testkit: could not drop database {} after {DROP_ATTEMPTS} attempts; \
             it and any replication slot on it are leaked",
            self.name
        );
    }
}

/// How [`TestCluster::restart`] stops the server (`pg_ctl stop -m <mode>`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopMode {
    /// Roll back open transactions, disconnect clients, write a shutdown
    /// checkpoint, exit.
    Fast,
    /// Exit without a shutdown checkpoint. The next start replays WAL from
    /// the last checkpoint, as after a crash.
    Immediate,
}

impl StopMode {
    fn as_pg_ctl_arg(self) -> &'static str {
        match self {
            StopMode::Fast => "fast",
            StopMode::Immediate => "immediate",
        }
    }
}

/// Starts `postgres` on `data_dir`, appending its output to `log_path` (so a
/// [`TestCluster::restart`] keeps the log from before the restart).
fn spawn_server(data_dir: &Path, socket_dir: &Path, port: u16, log_path: &Path) -> Child {
    let log_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .expect("create postgres log file");
    Command::new("postgres")
        .arg("-D")
        .arg(data_dir)
        .arg("-h")
        .arg("") // no TCP listener; unix socket only
        .arg("-k")
        .arg(socket_dir)
        .arg("-p")
        .arg(port.to_string())
        .arg("-c")
        .arg("wal_level=logical")
        // Headroom, not a tuning knob: issue #188 gives every
        // shared-cluster generative case its own slot *name*, so any slot
        // a case does leave behind (a `TestDatabase` drop that gave up, see
        // its `Drop`) accumulates rather than reusing one name, and a deep
        // nightly run puts hundreds of cases on one cluster. 10 left only
        // a handful of leaks' worth of room before
        // `pg_create_logical_replication_slot` would start failing with
        // "all replication slots are in use"; 50 is still trivial shared
        // memory (a slot is a small fixed struct) and takes that off the
        // table.
        .arg("-c")
        .arg("max_replication_slots=50")
        .arg("-c")
        .arg("max_wal_senders=50")
        // The default (`posix`) puts dynamic shared memory segments in
        // `/dev/shm`, outside the cluster's temp dir. A postgres that is
        // SIGKILLed (the teardown fallback, or a killed test binary) never
        // unlinks them, and neither `Drop` nor the orphan reaper knows they
        // exist, so they pile up until `/dev/shm`'s quota is gone. `mmap`
        // keeps them in `$PGDATA/pg_dynshmem`, which both already delete.
        .arg("-c")
        .arg("dynamic_shared_memory_type=mmap")
        // Tests that assert on what the server logged (`log_statement =
        // 'all'`, see `trellis/tests/apply.rs`) read the `postgres.log`
        // file the two `Stdio::from` handles below point at. That only
        // works while the server writes to its inherited stderr: with the
        // logging collector on, the postmaster hands logging to a
        // collector subprocess that writes its own rotated files under
        // `$PGDATA/log` instead, and everything those tests want to scrape
        // lands there rather than in the file they read.
        //
        // `logging_collector` defaults to `off` in vanilla Postgres, but
        // some distributions ship a `postgresql.conf.sample` that turns it
        // on (Fedora's does), and `initdb` copies that sample verbatim into
        // the cluster it creates — so whether those tests pass depended on
        // the packaging of whichever Postgres happened to be on `PATH`.
        // Pinning it here makes the cluster's logging behaviour a property
        // of the harness rather than of the host. It is a postmaster-level
        // setting, so passing it on the command line also outranks both
        // `postgresql.conf` and any later `ALTER SYSTEM`.
        .arg("-c")
        .arg("logging_collector=off")
        .stdout(Stdio::from(log_file.try_clone().expect("clone log handle")))
        .stderr(Stdio::from(log_file))
        .spawn()
        .expect("spawn postgres")
}

/// Runs [`reap_orphans_in`] against the system temp dir exactly once per
/// process, on the first [`TestCluster::start`].
fn reap_orphans_once() {
    static REAPED: Once = Once::new();
    REAPED.call_once(|| reap_orphans_in(&std::env::temp_dir()));
}

/// Sweeps `trellis-testkit-*` directories left in `tmp` by dead prior runs,
/// freeing each one's leaked SysV shared-memory segment (via its
/// `postmaster.pid`) and removing the directory. A dir whose recorded
/// postmaster is still alive belongs to a running test process and is left
/// untouched no matter what; one whose postmaster is dead (or has no
/// `postmaster.pid` at all) is only a genuine orphan if its *owning* process
/// (see [`owning_pid`]) is also confirmed dead — a live owner can be
/// mid-`initdb`/mid-startup (no pidfile yet) or, per #206, mid-teardown of a
/// postgres it just had to SIGKILL and may still be depending on that
/// directory for (e.g. a retry) — so a live owner is left alone either way.
///
/// Everything here is best-effort: this reclaims resources that already leaked,
/// so any failure (a segment already gone, a dir we can't read) is not worth
/// failing a test over.
fn reap_orphans_in(tmp: &Path) {
    let Ok(entries) = fs::read_dir(tmp) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("trellis-testkit-") {
            continue;
        }
        let dir = entry.path();
        let data_dir = dir.join("data");
        match fs::read_to_string(data_dir.join("postmaster.pid")) {
            // First line of `postmaster.pid` is the postmaster PID. If it's
            // still alive this cluster is in use by a running test process
            // (possibly a parallel test binary) — leave it entirely alone,
            // regardless of the owning process (below): never reap a
            // directory backing a postgres that's actually running.
            Ok(contents) => {
                let postgres_alive = contents
                    .lines()
                    .next()
                    .and_then(|line| line.trim().parse::<i32>().ok())
                    .is_some_and(process_alive);
                if postgres_alive {
                    continue;
                }
                // Postgres itself is confirmed dead, but a dead postmaster
                // doesn't make the *directory* an orphan: testkit's own
                // teardown (`TestCluster::drop`) SIGKILLs a wedged postgres
                // as a fallback while its owning harness process is still
                // alive and using this directory (#206) — so, exactly like
                // the no-pidfile case below, only reap once the owning
                // process is confirmed dead too. A name that doesn't match
                // the `<owning-pid>-<counter>` shape (so `owning_pid`
                // returns `None`) falls back to the pre-#206 behavior of
                // reaping on the postmaster PID alone, since there's no
                // owner to check.
                match owning_pid(&name) {
                    Some(pid) if process_alive(pid) => continue,
                    Some(_) | None => {
                        reap_shmem_segment(&data_dir);
                        let _ = fs::remove_dir_all(&dir);
                    }
                }
            }
            // No `postmaster.pid`: either a run was killed mid-`initdb` (no
            // server, no segment — safe to drop) or a cluster in *another*,
            // still-live process whose postgres hasn't written its pidfile
            // yet (still mid-`initdb`, possibly for a while under load — see
            // #198). The directory name itself encodes the answer: it's
            // `trellis-testkit-<owning-pid>-<counter>` (see [`unique_suffix`]),
            // so check that PID directly rather than guessing from age. A
            // dir whose owning process is still alive is left alone no
            // matter how long setup takes; one whose owner is confirmed dead
            // is a genuine orphan and reaped immediately.
            Err(_) => match owning_pid(&name) {
                Some(pid) if process_alive(pid) => {}
                Some(_) => {
                    let _ = fs::remove_dir_all(&dir);
                }
                // Name doesn't match the `<owning-pid>-<counter>` shape we
                // generate (e.g. some future/foreign layout) — fall back to
                // the conservative age guard rather than guessing wrong.
                None => {
                    if older_than(&dir, Duration::from_secs(60)) {
                        let _ = fs::remove_dir_all(&dir);
                    }
                }
            },
        }
    }
}

/// Extracts the owning process's PID from a `trellis-testkit-<pid>-<counter>`
/// directory name, as produced by [`unique_suffix`]. Returns `None` if the
/// name doesn't have that shape (e.g. it's not one this build of testkit
/// created).
fn owning_pid(dir_name: &str) -> Option<i32> {
    dir_name
        .strip_prefix("trellis-testkit-")?
        .split('-')
        .next()?
        .parse::<i32>()
        .ok()
}

/// Whether `path`'s last modification was more than `age` ago. Conservative on
/// any uncertainty (unreadable mtime, a clock that won't subtract): returns
/// `false` so the caller leaves the path alone.
fn older_than(path: &Path, age: Duration) -> bool {
    fs::metadata(path)
        .and_then(|meta| meta.modified())
        .ok()
        .and_then(|modified| modified.elapsed().ok())
        .is_some_and(|elapsed| elapsed > age)
}

/// Frees the SysV shared-memory segment Postgres recorded in
/// `<data_dir>/postmaster.pid`. Postgres writes the segment's key and id on
/// line 7 (`LOCK_FILE_LINE_SHMEM_KEY`) as `<key> <id>`, and reclaims stale
/// segments this same way on its own restart. A clean `pg_ctl stop` frees the
/// segment and deletes the file, so this only does anything after a server was
/// killed without cleanup. Best-effort and silent: the id feeds a plain
/// `ipcrm -m`, and a missing file / already-freed segment is exactly the state
/// we want anyway.
fn reap_shmem_segment(data_dir: &Path) {
    let Ok(contents) = fs::read_to_string(data_dir.join("postmaster.pid")) else {
        return;
    };
    let shmid = contents
        .lines()
        .nth(6)
        .and_then(|line| line.split_whitespace().nth(1))
        .filter(|id| id.parse::<u64>().is_ok());
    if let Some(shmid) = shmid {
        let _ = Command::new("ipcrm")
            .arg("-m")
            .arg(shmid)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// Whether a process with `pid` currently exists. `kill -0` sends no signal —
/// it just probes existence, exiting non-zero (ESRCH) when there's no such
/// process. Dependency-free, matching how the harness self-tests check it. A
/// reused PID makes a dead postmaster look alive, which only makes reaping more
/// conservative (we skip it), never destructive.
fn process_alive(pid: i32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

/// Like [`run_to_completion`] but retries on failure with linear backoff,
/// re-building the command each attempt (so a per-attempt reset — e.g.
/// clearing a partial data dir — can live in the closure). Panics with the
/// last attempt's output once retries are exhausted.
fn run_with_retries(mut make_command: impl FnMut() -> Command, label: &str) {
    const MAX_ATTEMPTS: u32 = 5;
    for attempt in 1..=MAX_ATTEMPTS {
        let output = make_command()
            .output()
            .unwrap_or_else(|e| panic!("failed to run {label}: {e}"));
        if output.status.success() {
            return;
        }
        if attempt == MAX_ATTEMPTS {
            panic!(
                "{label} failed after {attempt} attempts: {}\nstdout:\n{}\nstderr:\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
        }
        std::thread::sleep(Duration::from_millis(300 * attempt as u64));
    }
}

fn run_to_completion(command: &mut Command, label: &str) {
    let output = command
        .output()
        .unwrap_or_else(|e| panic!("failed to run {label}: {e}"));
    if !output.status.success() {
        panic!(
            "{label} failed: {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // No real postmaster runs here, so the shmem id on line 7 is a bogus one
    // that can't match a live segment; `reap_shmem_segment`'s `ipcrm` on it is
    // a harmless no-op. What we're exercising is the dir-sweep and liveness
    // gating, not the actual segment free.
    fn write_fake_cluster(sandbox: &Path, suffix: &str, postmaster_pid: Option<i32>) -> PathBuf {
        let dir = sandbox.join(format!("trellis-testkit-{suffix}"));
        let data_dir = dir.join("data");
        fs::create_dir_all(&data_dir).expect("create fake data dir");
        if let Some(pid) = postmaster_pid {
            // Mimic Postgres's pidfile: line 1 is the PID, line 7 is
            // `<shmKey> <shmId>`.
            let contents = format!(
                "{pid}\n{}\n0\n5432\n\n\n123 987654321\nready\n",
                dir.display()
            );
            fs::write(data_dir.join("postmaster.pid"), contents).expect("write fake pidfile");
        }
        dir
    }

    // Like `write_fake_cluster`, but names the directory the way
    // `unique_suffix` actually does (`<owning-pid>-<counter>`), so
    // `owning_pid` can parse a PID back out of it. Never writes a
    // `postmaster.pid` — these are for exercising the no-pidfile branch's
    // owning-process check specifically.
    fn write_fake_cluster_owned_by(sandbox: &Path, owning_pid: i32, counter: u32) -> PathBuf {
        let dir = sandbox.join(format!("trellis-testkit-{owning_pid}-{counter}"));
        fs::create_dir_all(dir.join("data")).expect("create fake data dir");
        dir
    }

    // Std has no set-mtime API, so shell out to `touch -t` to push a dir's
    // modification time far enough back to clear `older_than`'s guard.
    fn backdate(path: &Path) {
        let status = Command::new("touch")
            .arg("-t")
            .arg("202001010000")
            .arg(path)
            .status()
            .expect("run touch");
        assert!(status.success(), "touch should backdate the dir");
    }

    fn fresh_sandbox(tag: &str) -> PathBuf {
        let sandbox =
            std::env::temp_dir().join(format!("trellis-reap-test-{}-{tag}", unique_suffix()));
        fs::create_dir_all(&sandbox).expect("create sandbox");
        sandbox
    }

    #[test]
    fn reaps_dir_whose_postmaster_is_dead() {
        let sandbox = fresh_sandbox("dead");
        // i32::MAX is above macOS's PID ceiling, so it's reliably not a live
        // process.
        let dir = write_fake_cluster(&sandbox, "dead", Some(i32::MAX));

        reap_orphans_in(&sandbox);

        assert!(
            !dir.exists(),
            "orphan of a dead postmaster should be removed"
        );
        let _ = fs::remove_dir_all(&sandbox);
    }

    #[test]
    fn keeps_dir_whose_postmaster_is_alive() {
        let sandbox = fresh_sandbox("alive");
        // Our own process is unquestionably alive, standing in for a running
        // test binary's postmaster.
        let dir = write_fake_cluster(&sandbox, "alive", Some(std::process::id() as i32));

        reap_orphans_in(&sandbox);

        assert!(
            dir.exists(),
            "a cluster whose postmaster is still alive must not be reaped"
        );
        let _ = fs::remove_dir_all(&sandbox);
    }

    #[test]
    fn reaps_stale_dir_with_no_pidfile_and_unparseable_name() {
        // A name that doesn't match `<owning-pid>-<counter>` falls back to
        // the age guard rather than the owning-pid check exercised below.
        let sandbox = fresh_sandbox("nopid-stale");
        let dir = write_fake_cluster(&sandbox, "nopid-stale", None);
        // Backdate it well past the age guard so it reads as a genuine
        // leftover, not an in-flight setup in another process.
        backdate(&dir);

        reap_orphans_in(&sandbox);

        assert!(
            !dir.exists(),
            "a stale leftover with no postmaster.pid has no segment to free and should be removed"
        );
        let _ = fs::remove_dir_all(&sandbox);
    }

    #[test]
    fn keeps_fresh_dir_with_no_pidfile_and_unparseable_name() {
        // Same fallback path as above, but fresh: must not be swept.
        let sandbox = fresh_sandbox("nopid-fresh");
        let dir = write_fake_cluster(&sandbox, "nopid-fresh", None);

        reap_orphans_in(&sandbox);

        assert!(
            dir.exists(),
            "a just-created pidfile-less dir may be mid-setup elsewhere and must be left alone"
        );
        let _ = fs::remove_dir_all(&sandbox);
    }

    #[test]
    fn keeps_dir_with_no_pidfile_whose_owning_process_is_alive() {
        // This is the #198 regression case: a cluster whose owning process
        // is still alive and mid-`initdb` (no postmaster.pid yet) must never
        // be reaped, no matter how long setup takes. Backdate it well past
        // the old 60s age guard to prove it's the owning-pid check — not
        // age — that's protecting it now.
        let sandbox = fresh_sandbox("owner-alive");
        let dir = write_fake_cluster_owned_by(&sandbox, std::process::id() as i32, 0);
        backdate(&dir);

        reap_orphans_in(&sandbox);

        assert!(
            dir.exists(),
            "a pidfile-less dir whose owning process is still alive must never be reaped, \
             regardless of age"
        );
        let _ = fs::remove_dir_all(&sandbox);
    }

    #[test]
    fn reaps_dir_with_no_pidfile_whose_owning_process_is_dead() {
        // The flip side: once the owning process is confirmed dead, the
        // orphan is real and gets reaped immediately — no need to wait out
        // an age guard.
        let sandbox = fresh_sandbox("owner-dead");
        // i32::MAX is above macOS's PID ceiling, so it's reliably not a live
        // process.
        let dir = write_fake_cluster_owned_by(&sandbox, i32::MAX, 0);

        reap_orphans_in(&sandbox);

        assert!(
            !dir.exists(),
            "a pidfile-less dir whose owning process is confirmed dead is a genuine orphan \
             and should be removed immediately"
        );
        let _ = fs::remove_dir_all(&sandbox);
    }

    /// Regression test for #198: two independent OS processes concurrently
    /// running `reap_orphans_in` against the same shared temp root must
    /// never reap a directory that belongs to the *other*, still-live
    /// process — even though that directory has no `postmaster.pid` yet
    /// (still mid-`initdb`) and even though its age alone would look old
    /// enough to sweep under the old heuristic.
    ///
    /// A real child process stands in for "the other process": its PID is
    /// embedded in the directory name exactly as `unique_suffix` would, so
    /// this exercises the actual mechanism (`owning_pid` + `process_alive`)
    /// rather than merely simulating it.
    #[test]
    fn does_not_reap_concurrent_processes_in_flight_cluster() {
        let sandbox = fresh_sandbox("race");

        // Stand-in for another live `TestCluster::start()` in a different
        // process: something that reliably stays alive until we kill it.
        let mut other_process = Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("spawn stand-in process");
        let other_pid = other_process.id() as i32;

        let dir = write_fake_cluster_owned_by(&sandbox, other_pid, 0);
        // Backdate past the old 60s guard: under load, real setup
        // (`initdb` retries, contention) can plausibly take this long, and
        // the fix must not depend on it staying "fresh".
        backdate(&dir);

        // A concurrent process's reap pass must leave the other process's
        // in-flight directory alone while it's still alive.
        reap_orphans_in(&sandbox);
        assert!(
            dir.exists(),
            "must not reap another live process's in-flight cluster directory"
        );

        // Once that process actually exits, its directory is a genuine
        // orphan and the next reap pass must clean it up.
        other_process.kill().expect("kill stand-in process");
        other_process.wait().expect("reap stand-in process");

        reap_orphans_in(&sandbox);
        assert!(
            !dir.exists(),
            "must reap the directory once its owning process has actually exited"
        );

        let _ = fs::remove_dir_all(&sandbox);
    }

    /// Regression test for #206: the `Ok(contents)` (pidfile-present) branch
    /// must respect the same owning-process liveness check #198 gave the
    /// `Err(_)` (no-pidfile) branch. A `postmaster.pid` naming a dead
    /// postgres PID does not by itself mean the *directory* is an orphan —
    /// its owning process (the harness that called `TestCluster::start`,
    /// per the directory name) can still be alive and depending on that
    /// directory, e.g. immediately after SIGKILLing a wedged postgres as
    /// part of its own teardown. As in #198's own regression test, a real
    /// child process stands in for that independent owner so this exercises
    /// `owning_pid` + `process_alive` for real, not a simulation that would
    /// all share this process's own PID.
    #[test]
    fn does_not_reap_live_owners_dir_with_stale_pidfile() {
        let sandbox = fresh_sandbox("stale-pidfile-owner-alive");

        // Stand-in for another live `TestCluster` owner in a different
        // process, analogous to #198's `does_not_reap_concurrent_processes_in_flight_cluster`.
        let mut other_process = Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("spawn stand-in process");
        let other_pid = other_process.id() as i32;

        // Named the way `unique_suffix` does (`<owning-pid>-<counter>`), so
        // `owning_pid` parses that live process back out of it. i32::MAX is
        // above macOS's PID ceiling, so it's reliably not a live process:
        // this postmaster.pid is stale, naming a dead postgres PID, even
        // though the *owning* process is still alive.
        let dir = write_fake_cluster(&sandbox, &format!("{other_pid}-0"), Some(i32::MAX));

        reap_orphans_in(&sandbox);
        assert!(
            dir.exists(),
            "must not reap a directory whose owning process is alive, even if its \
             postmaster.pid names a dead PID"
        );

        // Once the owning process actually exits, the directory (stale
        // pidfile and all) is a genuine orphan and must be reaped.
        other_process.kill().expect("kill stand-in process");
        other_process.wait().expect("reap stand-in process");

        reap_orphans_in(&sandbox);
        assert!(
            !dir.exists(),
            "must reap the directory once its owning process has actually exited"
        );

        let _ = fs::remove_dir_all(&sandbox);
    }

    #[test]
    fn ignores_unrelated_directories() {
        let sandbox = fresh_sandbox("unrelated");
        let dir = sandbox.join("some-other-tool-cache");
        fs::create_dir_all(&dir).expect("create unrelated dir");

        reap_orphans_in(&sandbox);

        assert!(
            dir.exists(),
            "directories without the trellis-testkit- prefix must be left alone"
        );
        let _ = fs::remove_dir_all(&sandbox);
    }
}
