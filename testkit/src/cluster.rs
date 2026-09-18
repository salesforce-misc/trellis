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
    server: Child,
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
        let log_file = fs::File::create(&log_path).expect("create postgres log file");
        let server = Command::new("postgres")
            .arg("-D")
            .arg(&data_dir)
            .arg("-h")
            .arg("") // no TCP listener; unix socket only
            .arg("-k")
            .arg(&socket_dir)
            .arg("-p")
            .arg(port.to_string())
            .arg("-c")
            .arg("wal_level=logical")
            // Headroom, not a tuning knob: a `TestDatabase`'s `dropdb
            // --force` fails silently while that database still has an
            // *active* logical slot (see `TestDatabase::drop`), so a case
            // whose `trellis::Client` hasn't finished its best-effort
            // shutdown leaves both behind. Issue #188 gives every
            // shared-cluster generative case its own slot *name*, which
            // removes the cross-case collision but means such leaks
            // accumulate rather than reusing one name — and a deep nightly
            // run puts hundreds of cases on one cluster. 10 left only a
            // handful of leaks' worth of room before
            // `pg_create_logical_replication_slot` would start failing with
            // "all replication slots are in use"; 50 is still trivial
            // shared memory (a slot is a small fixed struct) and takes that
            // off the table.
            .arg("-c")
            .arg("max_replication_slots=50")
            .arg("-c")
            .arg("max_wal_senders=50")
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
            .expect("spawn postgres");

        let cluster = Self {
            root,
            data_dir,
            socket_dir,
            port,
            server,
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
        self.server.id()
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

        let dsn = format!(
            "host={} port={} user=postgres dbname={}",
            self.socket_dir.display(),
            self.port,
            name
        );

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
        // every subsequent `initdb`. Stop gracefully first (`pg_ctl stop`
        // cleans up shared memory), falling back to SIGKILL only on timeout.
        let stopped_gracefully = Command::new("pg_ctl")
            .arg("stop")
            .arg("-D")
            .arg(&self.data_dir)
            .arg("-m")
            .arg("fast")
            .arg("-w")
            .arg("-t")
            .arg("10")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false);

        if !stopped_gracefully {
            // The graceful stop timed out; SIGKILL can't clean up, so free the
            // segment ourselves from the pidfile before it's lost. Log it: a
            // fallback here is the one leak path teardown *can* see, and a
            // silent one turns into flaky `initdb` failures later.
            eprintln!(
                "testkit: `pg_ctl stop` did not stop postgres cleanly (pid {}); \
                 falling back to SIGKILL and reaping its shmem segment",
                self.server.id()
            );
            let _ = self.server.kill();
            let _ = self.server.wait();
            reap_shmem_segment(&self.data_dir);
        } else {
            let _ = self.server.wait();
        }
        let _ = fs::remove_dir_all(&self.root);
    }
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

impl Drop for TestDatabase {
    fn drop(&mut self) {
        // `--force` (PG 13+) disconnects any backends still attached before
        // dropping, so teardown doesn't race connections just released to
        // `pool`.
        let _ = Command::new("dropdb")
            .arg("-h")
            .arg(&self.socket_dir)
            .arg("-p")
            .arg(self.port.to_string())
            .arg("-U")
            .arg("postgres")
            .arg("--force")
            .arg(&self.name)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
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
/// untouched; a dir with no `postmaster.pid` never got a server (or it already
/// stopped), so there's no segment to free — just remove it.
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
        if !name.to_string_lossy().starts_with("trellis-testkit-") {
            continue;
        }
        let dir = entry.path();
        let data_dir = dir.join("data");
        match fs::read_to_string(data_dir.join("postmaster.pid")) {
            // First line of `postmaster.pid` is the postmaster PID. If it's
            // still alive this cluster is in use by a running test process
            // (possibly a parallel test binary) — leave it entirely alone.
            Ok(contents) => {
                let alive = contents
                    .lines()
                    .next()
                    .and_then(|line| line.trim().parse::<i32>().ok())
                    .is_some_and(process_alive);
                if alive {
                    continue;
                }
                reap_shmem_segment(&data_dir);
                let _ = fs::remove_dir_all(&dir);
            }
            // No `postmaster.pid`: either a run killed mid-`initdb` (no server,
            // no segment — safe to drop) or a cluster in *another* process
            // whose postgres hasn't written its pidfile yet. Can't tell those
            // apart directly, so only sweep dirs old enough that no in-flight
            // setup could still be using them; a fresh one is left for its
            // owner and reaped on a later run.
            Err(_) => {
                if older_than(&dir, Duration::from_secs(60)) {
                    let _ = fs::remove_dir_all(&dir);
                }
            }
        }
    }
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
    fn reaps_stale_dir_with_no_pidfile() {
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
    fn keeps_fresh_dir_with_no_pidfile() {
        let sandbox = fresh_sandbox("nopid-fresh");
        // Freshly created: stands in for a cluster in another process whose
        // postgres hasn't written its pidfile yet. Must not be swept.
        let dir = write_fake_cluster(&sandbox, "nopid-fresh", None);

        reap_orphans_in(&sandbox);

        assert!(
            dir.exists(),
            "a just-created pidfile-less dir may be mid-setup elsewhere and must be left alone"
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
