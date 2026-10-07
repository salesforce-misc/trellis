//! Seams for durability tests that need to kill a process mid-operation
//! (the generative suite's subprocess engine) or hold a transaction open across a
//! concurrent operation (the "straddler"/phase-gap tests). This module
//! provides the mechanism; the actual crash points and assertions belong to
//! those test suites.

use std::io;
use std::process::{Child, Command};
use std::time::{Duration, Instant};
use tokio_postgres::{Client, NoTls};

/// Spawns `command` and gives back a handle that can SIGKILL it on demand.
///
/// The intended pattern: `command` is some binary — often the test binary
/// itself, re-invoked with an env var or argument telling it which
/// operation to run — that signals readiness once it has reached the
/// intended crash point (e.g. by writing a marker file, or by a row
/// becoming visible in the database). Poll for that with [`wait_until`],
/// then call [`CrashGuard::kill`].
pub struct CrashGuard {
    child: Child,
}

impl CrashGuard {
    /// Spawns `command`. The caller is responsible for making the child
    /// signal when it has reached the point that should be killed (see
    /// module docs).
    pub fn spawn(command: &mut Command) -> io::Result<Self> {
        Ok(Self {
            child: command.spawn()?,
        })
    }

    /// Sends SIGKILL to the child immediately — [`Child::kill`]'s
    /// documented behavior on Unix — simulating a hard crash (`kill -9`)
    /// rather than a graceful shutdown.
    pub fn kill(&mut self) -> io::Result<()> {
        self.child.kill()
    }

    /// Waits for the child to exit after [`kill`](Self::kill), returning
    /// its exit status.
    pub fn wait(&mut self) -> io::Result<std::process::ExitStatus> {
        self.child.wait()
    }

    /// The child's exit status if it has already exited, without blocking:
    /// lets a caller waiting for the child's readiness signal notice that it
    /// died first.
    pub fn try_wait(&mut self) -> io::Result<Option<std::process::ExitStatus>> {
        self.child.try_wait()
    }
}

impl Drop for CrashGuard {
    fn drop(&mut self) {
        // Best-effort: if the test panicked between `kill()` and `wait()`
        // (or never called either), don't leave the child running as an
        // orphan/zombie.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Polls `condition` until it returns `true` or `timeout` elapses,
/// returning whether it succeeded. Used to wait for a child process to
/// reach a crash point (e.g. a marker file appearing, or a row becoming
/// visible) before calling [`CrashGuard::kill`].
pub fn wait_until(mut condition: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if condition() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// A transaction held open on its own dedicated connection (outside any
/// pool), for tests that need to straddle a phase gap: begin a
/// transaction, do some setup work inside it, run some *other* operation
/// concurrently, then commit or roll back and observe the result.
///
/// Deliberately not built on `deadpool_postgres`'s transaction type: that
/// borrows its client, which makes it awkward to hold across `.await`
/// points spanning unrelated operations. This instead owns a plain
/// `tokio_postgres::Client` connected directly (bypassing the pool), so it
/// can be held open for as long as the test needs.
pub struct OpenTransaction {
    client: Client,
    _connection: tokio::task::JoinHandle<()>,
}

impl OpenTransaction {
    /// Connects directly to `dsn` (not through a pool) and issues `BEGIN`.
    pub async fn begin(dsn: &str) -> Self {
        let (client, connection) = tokio_postgres::connect(dsn, NoTls)
            .await
            .expect("connect for held transaction");
        let handle = tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute("begin")
            .await
            .expect("begin transaction");
        Self {
            client,
            _connection: handle,
        }
    }

    /// Runs a statement inside the held-open transaction.
    pub async fn execute(&self, sql: &str) {
        self.client
            .batch_execute(sql)
            .await
            .expect("execute inside held transaction");
    }

    /// Commits the transaction, ending the straddle.
    pub async fn commit(self) {
        self.client
            .batch_execute("commit")
            .await
            .expect("commit held transaction");
    }

    /// Rolls back the transaction, ending the straddle.
    pub async fn rollback(self) {
        self.client
            .batch_execute("rollback")
            .await
            .expect("rollback held transaction");
    }
}
