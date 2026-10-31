//! A hand-driven drain for the interleaving tests.
//!
//! Source writes go through trigger capture, installed by hand with
//! `capture::install`, since the client doesn't install it yet
//! (C5). The driver seals both phases itself, and drains one batch at a time
//! with `drain_once` on a worker name the test picks, optionally frozen at a
//! [`PausePoint`] (`trellis::staging::interleave`). Nothing here waits for
//! convergence. The waits are [`Driver::wait_blocked_behind`], for a backend
//! to queue on a lock a frozen worker holds, which the test has forced, and
//! [`Driver::seal`], for the seal gate's backpressure to clear; neither is a
//! race.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

use testkit::{TestCluster, TestDatabase};
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls};
use trellis::capture::columns::{capture_spec, load_catalog};
use trellis::capture::install::{self, Progress};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, install_definition};
use trellis::intake::harness;

use trellis::staging::interleave::{PausePoint, PauseScope, Reached, with_scope};
use trellis::staging::{
    ApplyError, ApplyOutcome, StagedChange, append, apply, collect_tombstones, has_pending,
    retire_drained_segments, seal,
};

pub const WAKE: &str = "interleave_wake";

/// How long [`Driver::seal`] waits for the seal gate: long enough for a
/// server process stalled on a loaded box, short enough that a transaction
/// the test itself left open fails the test in one timeout.
pub const SEAL_GATE_WAIT: Duration = Duration::from_secs(60);

/// Splits `ddl` into its schema statements and its `insert into …` seed rows.
/// A group-basis aggregate over a partitioned source is defined while the
/// source is empty, and seeded once capture has recorded the tree.
pub fn split_seed(ddl: &str) -> (String, String) {
    let (mut schema, mut seed) = (String::new(), String::new());
    for statement in ddl.split(';').filter(|s| !s.trim().is_empty()) {
        let target = if statement.trim_start().starts_with("insert into") {
            &mut seed
        } else {
            &mut schema
        };
        target.push_str(statement);
        target.push(';');
    }
    (schema, seed)
}

/// Opens a plain connection to `dsn` with the engine's schema on the path.
pub async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public"))
        .await
        .expect("set search_path");
    client
}

/// Fields drop in declaration order: the clients first, then the database,
/// then the cluster, so `TestDatabase::drop`'s `dropdb` runs against a live
/// server instead of retrying against a stopped one and leaking.
pub struct Driver {
    /// Seals, stages recomputes and reads the oracle. Never frozen and never
    /// holds a pause lock.
    pub ctl: Client,
    /// Holds the session advisory locks frozen workers wait on, and nothing
    /// else, so it can never be part of a deadlock cycle.
    gate: Client,
    next_lock: AtomicI64,
    /// How long each phase of [`Self::start`] took: the source DDL, the
    /// definitions' install (define, build and backfill), the capture
    /// install, and the settle to quiescence. For measurements.
    pub phases: Vec<(&'static str, Duration)>,
    pub db: TestDatabase,
    pub cluster: TestCluster,
}

impl Driver {
    /// A fresh database with `source_ddl` run, `definitions` installed and
    /// live, trigger capture installed for `captured`, and the pipeline
    /// drained to quiescence.
    pub async fn start(
        source_ddl: &str,
        columns: &[(&str, ValueType)],
        definitions: &[&str],
        captured: &[&str],
    ) -> Self {
        Self::start_with_database_defaults(&[], source_ddl, columns, definitions, captured).await
    }

    /// [`Self::start`] on a database whose defaults
    /// (`alter database ... set name to value`) include `defaults`. The
    /// engine's pool only opens connections once they are set, so every
    /// session Trellis opens starts from them. `ctl` and `gate` connect
    /// before them and keep the server's defaults: they stand in for the
    /// test, not for the engine. A [`Self::user`] connection starts from
    /// them too, as an application's would.
    pub async fn start_with_database_defaults(
        defaults: &[(&str, &str)],
        source_ddl: &str,
        columns: &[(&str, ValueType)],
        definitions: &[&str],
        captured: &[&str],
    ) -> Self {
        Self::start_inner(defaults, source_ddl, columns, definitions, captured, true).await
    }

    /// [`Self::start`] for definitions over tables that hold rows: the
    /// builds are started (`backfilling`, their jobs unclaimed) and left for
    /// the test to run, frozen where the scenario needs it, instead of run to
    /// `live`.
    pub async fn start_unbuilt(
        defaults: &[(&str, &str)],
        source_ddl: &str,
        columns: &[(&str, ValueType)],
        definitions: &[&str],
        captured: &[&str],
    ) -> Self {
        Self::start_inner(defaults, source_ddl, columns, definitions, captured, false).await
    }

    async fn start_inner(
        defaults: &[(&str, &str)],
        source_ddl: &str,
        columns: &[(&str, ValueType)],
        definitions: &[&str],
        captured: &[&str],
        built: bool,
    ) -> Self {
        let cluster = TestCluster::start();
        let mut db = cluster.create_isolated_database().await;
        let mut ctl = connect(db.dsn()).await;
        let gate = connect(db.dsn()).await;
        if !defaults.is_empty() {
            for (name, value) in defaults {
                ctl.batch_execute(&format!(
                    "alter database \"{}\" set {name} to '{value}'",
                    db.name().replace('"', "\"\"")
                ))
                .await
                .expect("set a database default");
            }
            // Migrating the database opened the pool's connections before
            // the defaults existed; a fresh pool opens its own after them.
            let config =
                trellis::config::Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
            db.pool = trellis::Pool::new(&config).expect("a fresh pool");
        }
        let mut phases = Vec::new();
        let started = Instant::now();
        ctl.batch_execute(source_ddl).await.expect("source DDL");
        phases.push(("source_ddl", started.elapsed()));
        let columns: HashMap<String, ValueType> = columns
            .iter()
            .map(|(name, ty)| (name.to_string(), *ty))
            .collect();
        let started = Instant::now();
        for definition in definitions {
            install_definition(&db.pool, definition, &columns, "public")
                .await
                .expect("install definition");
        }
        phases.push(("define", started.elapsed()));
        let started = Instant::now();
        let catalog = load_catalog(&ctl).await.expect("load the catalog");
        for table in captured {
            let spec = capture_spec(&ctl, &catalog, table)
                .await
                .expect("capture spec");
            match install::reconcile(&mut ctl, DEFAULT_SCHEMA, &spec, None)
                .await
                .expect("install capture")
            {
                Progress::Done(_) => {}
                Progress::Waiting(wait) => panic!("an unbounded install waited: {wait}"),
            }
        }
        phases.push(("capture_install", started.elapsed()));
        let started = Instant::now();
        if built {
            harness::settle_builds(&db.pool).await;
        } else {
            harness::start_registered_builds(&db.pool)
                .await
                .expect("start the builds");
        }
        let mut driver = Driver {
            cluster,
            db,
            ctl,
            gate,
            next_lock: AtomicI64::new(623_000),
            phases,
        };
        if !built {
            return driver;
        }
        driver.settle().await;
        let settled = started.elapsed();
        driver.phases.push(("settle", settled));
        let not_live: i64 = driver
            .ctl
            .query_one(
                "select count(*) from transform_definitions where status <> 'live'",
                &[],
            )
            .await
            .expect("read statuses")
            .get(0);
        assert_eq!(not_live, 0, "every definition is live before the scenario");
        driver
    }

    pub fn pool(&self) -> &trellis::Pool {
        &self.db.pool
    }

    /// A new application connection.
    pub async fn user(&self) -> Client {
        connect(self.db.dsn()).await
    }

    /// Seals both phases and returns the sealed batch. Retires every drained
    /// segment it can first, so a test never runs the four-slot ring full.
    ///
    /// The seal gate holds while a transaction that was in flight at the
    /// previous batch's fence is still open. That is backpressure, which the
    /// engine's own sealer retries; here the likely holder is a server
    /// process finishing on a loaded box (autovacuum's analyze, say, which
    /// takes an xid for its statistics writes). So this waits up to
    /// [`SEAL_GATE_WAIT`] for it. A holder the test itself leaves open never
    /// goes away, and the panic names every open xid-holding session.
    pub async fn seal(&mut self) -> i64 {
        self.seal_within(SEAL_GATE_WAIT)
            .await
            .unwrap_or_else(|held| panic!("seal phase 1: {held}"))
    }

    /// [`Self::seal`], waiting at most `wait` for the seal gate; `Err` names
    /// the sessions holding an xid open when it ran out.
    pub async fn seal_within(&mut self, wait: Duration) -> Result<i64, String> {
        let deadline = Instant::now() + wait;
        loop {
            if let Some(batch) = self.try_seal().await {
                return Ok(batch);
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "the seal gate stayed blocked for {wait:?} ({}); transactions holding an xid: {}",
                    self.gate_state().await,
                    self.xid_holders().await
                ));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// The two sides of the seal gate's comparison: the current snapshot's
    /// `xmin` (the oldest xid running now) and the predecessor fence's
    /// `xmax`, or that the predecessor has no fence, which holds the gate
    /// whatever is running. A holder blocks only if its xid is below that
    /// `xmax`.
    async fn gate_state(&self) -> String {
        let row = self
            .ctl
            .query_one(
                "select pg_snapshot_xmin(pg_current_snapshot())::text, \
                        (select coalesce(pg_snapshot_xmax(s.fence_snapshot)::text, 'unfenced') \
                         from segments s, segment_pointer p \
                         where s.seg_seq = p.active_seq - 1)",
                &[],
            )
            .await
            .expect("read the seal gate's state");
        format!(
            "snapshot xmin {}, predecessor fence xmax {}",
            row.get::<_, String>(0),
            row.get::<_, Option<String>>(1)
                .unwrap_or_else(|| "none (no predecessor)".to_string())
        )
    }

    /// Every session (and prepared transaction) holding an xid open, one per
    /// line: what a blocked seal gate is waiting on.
    async fn xid_holders(&self) -> String {
        let sessions = self
            .ctl
            .query(
                "select pid, state, backend_xid::text, backend_type, \
                        coalesce(left(query, 120), '') \
                 from pg_stat_activity \
                 where backend_xid is not null and pid <> pg_backend_pid()",
                &[],
            )
            .await
            .expect("read pg_stat_activity");
        let prepared = self
            .ctl
            .query("select gid, transaction::text from pg_prepared_xacts", &[])
            .await
            .expect("read pg_prepared_xacts");
        let mut lines: Vec<String> = sessions
            .iter()
            .map(|row| {
                format!(
                    "pid {} ({}, {}) xid {}: {}",
                    row.get::<_, i32>(0),
                    row.get::<_, String>(3),
                    row.get::<_, Option<String>>(1).unwrap_or_default(),
                    row.get::<_, String>(2),
                    row.get::<_, String>(4)
                )
            })
            .collect();
        lines.extend(prepared.iter().map(|row| {
            format!(
                "prepared {} xid {}",
                row.get::<_, String>(0),
                row.get::<_, String>(1)
            )
        }));
        if lines.is_empty() {
            "none".to_string()
        } else {
            lines.join("; ")
        }
    }

    /// [`Self::seal`], or `None` when the seal gate refuses it: a
    /// transaction that may still write into the active segment's
    /// predecessor is open (backpressure, which a test running other
    /// transactions concurrently can meet).
    pub async fn try_seal(&mut self) -> Option<i64> {
        self.retire().await;
        let outcome = match seal::seal_phase1(&mut self.ctl).await {
            Ok(outcome) => outcome,
            Err(trellis::staging::StagingError::SealGateBlocked) => return None,
            Err(err) => panic!("seal phase 1: {err}"),
        };
        seal::seal_phase2(&self.ctl, outcome.sealed_seg_seq, WAKE)
            .await
            .expect("seal phase 2");
        Some(outcome.sealed_seg_seq)
    }

    /// Stages an image-less `Recompute` (today's Re-derive request) for each
    /// of `keys` of `src_table` into the active segment.
    pub async fn stage_recomputes(&self, src_table: &str, keys: &[&str]) {
        let keys: Vec<(&str, Option<String>)> = keys.iter().map(|key| (*key, None)).collect();
        self.stage_recomputes_with_prior(src_table, &keys).await;
    }

    /// As [`Self::stage_recomputes`], each carrying a prior image (a CDC
    /// text-format row image).
    pub async fn stage_recomputes_with_prior(
        &self,
        src_table: &str,
        keys: &[(&str, Option<String>)],
    ) {
        let changes: Vec<StagedChange> = keys
            .iter()
            .map(|(key, prior_image)| StagedChange::Recompute {
                src_table: src_table.to_string(),
                key: key.to_string(),
                hop_gen: 0,
                src_changed: None,
                prior_image: prior_image.clone(),
                origin_lsn: None,
            })
            .collect();
        let mut client = self.db.pool.get().await.expect("pool");
        let txn = client.transaction().await.expect("begin");
        append(&txn, &changes).await.expect("stage recomputes");
        txn.commit().await.expect("commit recomputes");
    }

    /// Drains `batch` to completion on worker `worker`, with nothing armed.
    pub async fn drain(&self, batch: i64, worker: &str) -> Option<ApplyOutcome> {
        self.drain_frozen(batch, worker, &[]).await.finish().await
    }

    /// Starts draining `batch` on worker `worker` in its own task, frozen at
    /// each of `points` (a `(point, qualified target)` pair) once reached.
    /// The caller holds each point's lock until [`RunningDrain::release`].
    pub async fn drain_frozen(
        &self,
        batch: i64,
        worker: &str,
        points: &[(PausePoint, &str)],
    ) -> RunningDrain {
        self.spawn_drain(batch, worker, 1, points).await
    }

    /// Starts draining this worker's share of `batch` in its own task, as one
    /// of `live_workers` workers (a batch of at least `MIN_ROWS_TO_SPLIT`
    /// rows is split into buckets, and each worker claims its share).
    pub async fn drain_share(&self, batch: i64, worker: &str, live_workers: i64) -> RunningDrain {
        self.spawn_drain(batch, worker, live_workers, &[]).await
    }

    /// A [`PauseScope`] with each of `points` armed, each on a pause lock the
    /// gate takes now.
    async fn arm(&self, points: &[(PausePoint, &str)]) -> (Arc<PauseScope>, Vec<Frozen>) {
        let scope = PauseScope::new();
        let mut frozen = Vec::with_capacity(points.len());
        for &(point, target) in points {
            let lock_key = self.next_lock.fetch_add(1, Ordering::Relaxed);
            self.gate
                .execute("select pg_advisory_lock($1)", &[&lock_key])
                .await
                .expect("take the pause lock");
            frozen.push(Frozen {
                point,
                lock_key,
                reached: Some(scope.arm(point, target, lock_key)),
            });
        }
        (scope, frozen)
    }

    async fn spawn_drain(
        &self,
        batch: i64,
        worker: &str,
        live_workers: i64,
        points: &[(PausePoint, &str)],
    ) -> RunningDrain {
        let (scope, frozen) = self.arm(points).await;
        let pool = self.db.pool.clone();
        let worker = worker.to_string();
        // Claims again until the claim wins nothing, so a worker whose peers
        // finished first picks up any bucket left over.
        let handle = tokio::spawn(with_scope(scope, async move {
            let mut last = None;
            while let Some(outcome) =
                apply::drain_once(&pool, batch, &worker, live_workers, WAKE).await?
            {
                last = Some(outcome);
            }
            Ok(last)
        }));
        RunningDrain {
            handle: Some(handle),
            frozen,
            ended: None,
        }
    }

    /// Starts `work` (given the pool) in its own task, frozen at each of
    /// `points` once reached, as [`Self::drain_frozen`] does for a drain: for
    /// a writer outside the drain, a column resume or an `ALTER`'s backfill.
    pub async fn run_frozen<T, F, Fut>(&self, points: &[(PausePoint, &str)], work: F) -> Running<T>
    where
        T: Send + 'static,
        F: FnOnce(trellis::Pool) -> Fut,
        Fut: Future<Output = Result<T, ApplyError>> + Send + 'static,
    {
        let (scope, frozen) = self.arm(points).await;
        let handle = tokio::spawn(with_scope(scope, work(self.db.pool.clone())));
        Running {
            handle: Some(handle),
            frozen,
            ended: None,
        }
    }

    /// Releases a frozen worker's pause lock.
    async fn unlock(&self, lock_key: i64) {
        let released: bool = self
            .gate
            .query_one("select pg_advisory_unlock($1)", &[&lock_key])
            .await
            .expect("release the pause lock")
            .get(0);
        assert!(released, "the gate held pause lock {lock_key}");
    }

    /// Waits until some backend is queued on a lock `pid` holds. Used after
    /// starting a second drain whose page must block behind a frozen one, so
    /// the test knows it has reached that lock before letting the first go.
    pub async fn wait_blocked_behind(&self, pid: i32) {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let blocked: bool = self
                .ctl
                .query_one(
                    "select exists (select 1 from pg_stat_activity \
                     where $1 = any(pg_blocking_pids(pid)))",
                    &[&pid],
                )
                .await
                .expect("read pg_stat_activity")
                .get(0);
            if blocked {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "no backend queued behind the frozen worker (pid {pid}) within 60 s"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Waits until at least `n` backends are queued on a lock some other
    /// backend holds. Like [`Self::wait_blocked_behind`], for when the test
    /// has forced `n` waiters but not which holder each queues behind.
    pub async fn wait_blocked(&self, n: i64) {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let blocked: i64 = self
                .ctl
                .query_one(
                    "select count(*) from pg_stat_activity \
                     where cardinality(pg_blocking_pids(pid)) > 0",
                    &[],
                )
                .await
                .expect("read pg_stat_activity")
                .get(0);
            if blocked >= n {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "{blocked} of {n} backends queued on a lock within 60 s"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Seals and drains until nothing is pending anywhere in the ring. A
    /// bounded deterministic loop, not a wait for convergence.
    pub async fn settle(&mut self) {
        for _ in 0..16 {
            let batch = self.seal().await;
            self.drain(batch, "settle").await;
            retire_drained_segments(&mut self.ctl)
                .await
                .expect("retire drained segments");
            self.collect_tombstones().await;
            if !has_pending(&self.ctl).await.expect("has_pending") {
                return;
            }
        }
        panic!("pipeline did not reach quiescence within 16 seal/drain rounds");
    }

    /// Frees the ring slots of drained segments.
    pub async fn retire(&mut self) {
        retire_drained_segments(&mut self.ctl)
            .await
            .expect("retire drained segments");
    }

    /// Collects the tombstones the drained prefix no longer needs,
    /// returning how many went.
    pub async fn collect_tombstones(&mut self) -> u64 {
        collect_tombstones(&mut self.ctl)
            .await
            .expect("collect tombstones")
    }

    /// `sql`'s rows, each rendered as a row literal, `(1,25,2)`.
    pub async fn rows(&self, sql: &str) -> Vec<String> {
        self.ctl
            .query(&format!("select r::text from ({sql}) r"), &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .into_iter()
            .map(|row| row.get(0))
            .collect()
    }

    /// Every `deadlock detected` error the server has logged so far, each
    /// with the lines after it (the `DETAIL` naming the processes and their
    /// statements, the `CONTEXT`, the `STATEMENT`), so a failing assertion
    /// shows which statements formed the cycle.
    pub fn deadlocks_logged(&self) -> Vec<String> {
        const REPORT_LINES: usize = 10;
        let log =
            std::fs::read_to_string(self.cluster.root().join("postgres.log")).unwrap_or_default();
        let lines: Vec<&str> = log.lines().collect();
        lines
            .iter()
            .enumerate()
            .filter(|(_, line)| line.contains("deadlock detected"))
            .map(|(at, _)| lines[at..(at + REPORT_LINES).min(lines.len())].join("\n"))
            .collect()
    }

    /// Releases `drain`'s pause at `point` once it has been reached.
    pub async fn release<T>(&self, drain: &mut Running<T>, point: PausePoint) {
        let at = drain
            .frozen
            .iter()
            .position(|f| f.point == point)
            .unwrap_or_else(|| panic!("{point:?} was not armed"));
        let frozen = drain.frozen.remove(at);
        self.unlock(frozen.lock_key).await;
    }
}

struct Frozen {
    point: PausePoint,
    lock_key: i64,
    reached: Option<tokio::sync::oneshot::Receiver<Reached>>,
}

/// A drain running in its own task. See [`Driver::drain_frozen`].
pub type RunningDrain = Running<Option<ApplyOutcome>>;

/// A drain or a build chunk running in its own task.
pub struct Running<T> {
    handle: Option<JoinHandle<Result<T, ApplyError>>>,
    frozen: Vec<Frozen>,
    /// What the task returned, once [`Running::reached_or_finished`] saw it
    /// end.
    ended: Option<Result<T, ApplyError>>,
}

impl<T> Running<T> {
    /// Waits until the worker is frozen at `point`. Panics if the drain
    /// finishes (or fails) without reaching it.
    pub async fn reached(&mut self, point: PausePoint) -> Reached {
        let frozen = self
            .frozen
            .iter_mut()
            .find(|f| f.point == point)
            .unwrap_or_else(|| panic!("{point:?} was not armed"));
        let rx = frozen.reached.take().expect("reached() once per point");
        let handle = self.handle.as_mut().expect("drain still running");
        tokio::select! {
            reached = rx => match reached {
                Ok(reached) => reached,
                // The worker dropped its pause scope: it finished, or failed,
                // without reaching `point`. Say how.
                Err(_) => panic!(
                    "the task finished without reaching {point:?}: {:?}",
                    handle.await.expect("task").map(|_| ())
                ),
            },
            finished = &mut *handle => panic!(
                "the task finished without reaching {point:?}: {:?}",
                finished.expect("task").map(|_| ())
            ),
        }
    }

    /// Waits until the worker is frozen at `point` (`Some`), or until it
    /// finishes, or fails, without reaching it (`None`): a worker that rolls
    /// back and gives up before `point`. [`Self::finish_result`] then returns
    /// what it returned.
    #[allow(dead_code)]
    pub async fn reached_or_finished(&mut self, point: PausePoint) -> Option<Reached> {
        let frozen = self
            .frozen
            .iter_mut()
            .find(|f| f.point == point)
            .unwrap_or_else(|| panic!("{point:?} was not armed"));
        let rx = frozen.reached.take().expect("reached() once per point");
        let handle = self.handle.as_mut().expect("drain still running");
        let finished = tokio::select! {
            reached = rx => match reached {
                Ok(reached) => return Some(reached),
                Err(_) => handle.await,
            },
            finished = &mut *handle => finished,
        };
        self.handle = None;
        self.ended = Some(finished.expect("task"));
        None
    }

    /// Whether the task has finished, without waiting for it.
    pub fn is_finished(&self) -> bool {
        self.handle
            .as_ref()
            .is_none_or(|handle| handle.is_finished())
    }

    /// Kills the task where it is, as a worker that dies: nothing it had not
    /// committed survives, and its claims are left for the test to release.
    /// The armed points stay held, so nothing runs on past one.
    pub async fn kill(mut self) {
        let handle = self.handle.take().expect("task");
        handle.abort();
        let _ = handle.await;
    }

    /// Waits for the task to finish, after every armed point was released.
    pub async fn finish(self) -> T {
        self.finish_result().await.expect("the task failed")
    }

    /// [`Self::finish`], returning the task's error instead of panicking.
    pub async fn finish_result(mut self) -> Result<T, ApplyError> {
        assert!(
            self.frozen.is_empty(),
            "release every armed point before finishing the task"
        );
        if let Some(ended) = self.ended.take() {
            return ended;
        }
        self.handle.take().expect("task").await.expect("task")
    }
}
