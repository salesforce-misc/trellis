//! A hand-driven drain for the interleaving tests (#623 D1).
//!
//! Source writes go through trigger capture, installed by hand with
//! `capture::install` (#622 C3), since the client doesn't install it yet
//! (C5). The driver seals both phases itself, and drains one batch at a time
//! with `drain_once` on a worker name the test picks, optionally frozen at a
//! [`PausePoint`] (`trellis::staging::interleave`). Nothing here waits for
//! convergence (#297). The one wait, [`Driver::wait_blocked_behind`], waits
//! for a backend to queue on a lock a frozen worker holds, which the test has
//! forced; it is not a race.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

use testkit::{TestCluster, TestDatabase};
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls};
use trellis::capture::columns::{capture_spec, load_catalog};
use trellis::capture::install::{self, Progress};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, install_definition};
use trellis::intake::markers;
use trellis::staging::interleave::{PausePoint, PauseScope, Reached, with_scope};
use trellis::staging::{
    ApplyError, ApplyOutcome, StagedChange, StagedWatermark, append, apply, collect_tombstones,
    has_pending, retire_drained_segments, seal,
};

pub const WAKE: &str = "interleave_wake";

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

pub struct Driver {
    pub cluster: TestCluster,
    pub db: TestDatabase,
    /// Seals, stages recomputes and reads the oracle. Never frozen and never
    /// holds a pause lock.
    pub ctl: Client,
    /// Holds the session advisory locks frozen workers wait on, and nothing
    /// else, so it can never be part of a deadlock cycle.
    gate: Client,
    next_lock: AtomicI64,
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
        let cluster = TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut ctl = connect(db.dsn()).await;
        let gate = connect(db.dsn()).await;
        ctl.batch_execute(source_ddl).await.expect("source DDL");
        let columns: HashMap<String, ValueType> = columns
            .iter()
            .map(|(name, ty)| (name.to_string(), *ty))
            .collect();
        for definition in definitions {
            install_definition(&db.pool, definition, &columns, "public")
                .await
                .expect("install definition");
        }
        let catalog = load_catalog(&ctl, DEFAULT_SCHEMA)
            .await
            .expect("load the catalog");
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
        markers::settle_registrations(&db.pool).await;
        let mut driver = Driver {
            cluster,
            db,
            ctl,
            gate,
            next_lock: AtomicI64::new(623_000),
        };
        driver.settle().await;
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
    pub async fn seal(&mut self) -> i64 {
        self.retire().await;
        let outcome = seal::seal_phase1(&mut self.ctl)
            .await
            .expect("seal phase 1");
        seal::seal_phase2(&self.ctl, outcome.sealed_seg_seq, WAKE)
            .await
            .expect("seal phase 2");
        outcome.sealed_seg_seq
    }

    /// Stages an image-less `Recompute` (today's Re-derive request) for each
    /// of `keys` of `src_table` into the active segment.
    pub async fn stage_recomputes(&self, src_table: &str, keys: &[&str]) {
        let keys: Vec<(&str, Option<String>)> = keys.iter().map(|key| (*key, None)).collect();
        self.stage_recomputes_with_prior(src_table, &keys).await;
    }

    /// As [`Self::stage_recomputes`], each carrying a prior image (the
    /// CDC text-format row image the go-live enumeration attaches, #392).
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
                group_key: None,
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

    async fn spawn_drain(
        &self,
        batch: i64,
        worker: &str,
        live_workers: i64,
        points: &[(PausePoint, &str)],
    ) -> RunningDrain {
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
        let pool = self.db.pool.clone();
        let worker = worker.to_string();
        // Claims again until the claim wins nothing, so a worker whose peers
        // finished first picks up any bucket left over.
        let handle = tokio::spawn(with_scope(scope, async move {
            let mut last = None;
            while let Some(outcome) = apply::drain_once(
                &pool,
                batch,
                &worker,
                live_workers,
                WAKE,
                &StagedWatermark::saturated(),
            )
            .await?
            {
                last = Some(outcome);
            }
            Ok(last)
        }));
        RunningDrain {
            handle: Some(handle),
            frozen,
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

    /// Collects the tombstones the drained prefix no longer needs (#623 D7),
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
    pub async fn release(&self, drain: &mut RunningDrain, point: PausePoint) {
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
pub struct RunningDrain {
    handle: Option<JoinHandle<Result<Option<ApplyOutcome>, ApplyError>>>,
    frozen: Vec<Frozen>,
}

impl RunningDrain {
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
            reached = rx => reached.expect("pause scope dropped"),
            finished = handle => panic!(
                "the drain finished without reaching {point:?}: {:?}",
                finished.expect("drain task").map(|o| o.map(|o| (o.keys_written, o.keys_deleted)))
            ),
        }
    }

    /// Waits for the drain to finish, after every armed point was released.
    pub async fn finish(mut self) -> Option<ApplyOutcome> {
        assert!(
            self.frozen.is_empty(),
            "release every armed point before finishing the drain"
        );
        self.handle
            .take()
            .expect("drain task")
            .await
            .expect("drain task")
            .expect("drain_once")
    }
}
