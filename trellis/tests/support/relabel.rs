//! A harness for the enum relabel tests (#1241): a database with
//! `public.tone` (`'brisk'`, `'calm'`), definitions brought live, and each
//! step of the staging worker driven by hand: a capture pass, build chunks,
//! seal and drain. Nothing polls for convergence (#297).

#![allow(dead_code)]

use std::time::{Duration, Instant};

use testkit::{TestCluster, TestDatabase};
use tokio_postgres::{Client, NoTls};
use trellis::capture::reconcile;
use trellis::defs::TransformStatus;
use trellis::staging::self_check::CheckPageExt;
use trellis::{Config, SelfCheckMode, Trellis, TrellisOptions};

pub const SCHEMA: &str = "trellis";

pub const RELABEL: &str = "alter type public.tone rename value 'brisk' to 'zippy'";

/// The reason a relabel pauses a reader with.
pub const REASON: &str = "enum value 'brisk' of type public.tone was renamed to 'zippy'; \
     Trellis's staged changes and keys still name the old label; resume to rebuild the target \
     from the source";

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {SCHEMA}, public"))
        .await
        .expect("set search_path");
    client
}

/// Fields drop in order: clients, facade, database, cluster.
pub struct H {
    pub raw: Client,
    pub trellis: Trellis,
    pub db: TestDatabase,
    _cluster: TestCluster,
    pub targets: Vec<String>,
}

impl H {
    /// `ddl` run after the enum is created, `definitions` defined and live.
    pub async fn new(ddl: &str, definitions: &[&str]) -> H {
        let cluster = TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let raw = connect(db.dsn()).await;
        raw.batch_execute(&format!(
            "create type public.tone as enum ('brisk', 'calm'); {ddl}"
        ))
        .await
        .expect("source DDL");
        let trellis = Trellis::connect(
            Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
            TrellisOptions::default(),
        )
        .await
        .expect("connect a definer");
        let mut targets = Vec::new();
        for text in definitions {
            trellis.apply(text).await.expect(text);
            let name = text.split_whitespace().nth(1).expect("a target name");
            targets.push(name.to_string());
        }
        let mut h = H {
            raw,
            trellis,
            db,
            _cluster: cluster,
            targets,
        };
        h.converge("bring live").await;
        h
    }

    pub fn pool(&self) -> &trellis::Pool {
        &self.db.pool
    }

    pub async fn write(&self, sql: &str) {
        self.raw.batch_execute(sql).await.expect(sql);
    }

    /// One whole staging-worker pass.
    pub async fn pass(&mut self) {
        let pool = self.db.pool.clone();
        trellis::client::reconcile_pass(
            &mut self.raw,
            &pool,
            SCHEMA,
            "relabel_wake",
            Duration::from_secs(2),
        )
        .await
        .expect("reconcile pass");
    }

    /// The capture half of one pass only. Asserts no table failed.
    pub async fn capture_pass(&mut self) {
        let desired = trellis::defs::tables_to_capture(&self.db.pool)
            .await
            .expect("read the tables to capture");
        let outcome = reconcile::reconcile(
            &mut self.raw,
            SCHEMA,
            &desired,
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .expect("capture pass");
        assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
    }

    /// Claims, runs and finishes every pending build chunk, then settles the
    /// builds.
    pub async fn build(&self) {
        use trellis::defs::chunk_queue;
        const CLAIMED_BY: &str = "relabel_backfill";
        loop {
            let client = self.db.pool.get().await.expect("acquire connection");
            let claimed = chunk_queue::claim_chunks(&**client, CLAIMED_BY, 1000)
                .await
                .expect("claim_chunks");
            drop(client);
            if claimed.is_empty() {
                trellis::staging::build::settle_builds(&self.db.pool).await;
                return;
            }
            for chunk in &claimed {
                chunk_queue::run_claimed_chunk(
                    &self.db.pool,
                    chunk,
                    CLAIMED_BY,
                    Duration::from_secs(5),
                    Duration::from_secs(60),
                )
                .await
                .expect("run_claimed_chunk");
                chunk_queue::finish_chunk(&self.db.pool, chunk, CLAIMED_BY)
                    .await
                    .expect("finish_chunk");
            }
        }
    }

    /// Seals and drains until nothing is pending. A failing page fails the
    /// test.
    pub async fn drain(&mut self) {
        use trellis::staging::{
            StagedWatermark, apply, has_pending, retire_drained_segments, seal,
        };
        let watermark = StagedWatermark::saturated();
        for _ in 0..16 {
            let outcome = seal::seal_phase1(&mut self.raw)
                .await
                .expect("seal phase 1");
            seal::seal_phase2(&self.raw, outcome.sealed_seg_seq, "relabel_wake")
                .await
                .expect("seal phase 2");
            while apply::drain_once(
                &self.db.pool,
                outcome.sealed_seg_seq,
                "relabel_test",
                1,
                "trellis_relabel_test",
                &watermark,
            )
            .await
            .expect("drain_once")
            .is_some()
            {}
            retire_drained_segments(&mut self.raw)
                .await
                .expect("retire drained segments");
            if !has_pending(&self.raw).await.expect("has_pending") {
                return;
            }
        }
        panic!("the ring did not reach quiescence within 16 seal/drain rounds");
    }

    /// Drains the active segment, whose rows include keys whose write fails:
    /// each failed drain charges the key a death, until it is held and the
    /// drain goes past it. Bounded attempts, not a timed wait.
    pub async fn drain_past_failures(&mut self) {
        use trellis::staging::{StagedWatermark, apply, retire_drained_segments, seal};
        let watermark = StagedWatermark::saturated();
        let outcome = seal::seal_phase1(&mut self.raw)
            .await
            .expect("seal phase 1");
        seal::seal_phase2(&self.raw, outcome.sealed_seg_seq, "relabel_wake")
            .await
            .expect("seal phase 2");
        let mut drained = false;
        for _ in 0..40 {
            match apply::drain_once(
                &self.db.pool,
                outcome.sealed_seg_seq,
                "relabel_test",
                1,
                "trellis_relabel_test",
                &watermark,
            )
            .await
            {
                Ok(Some(_)) | Err(_) => {}
                Ok(None) => {
                    drained = true;
                    break;
                }
            }
        }
        assert!(drained, "the segment did not drain within 40 attempts");
        retire_drained_segments(&mut self.raw)
            .await
            .expect("retire drained segments");
    }

    pub async fn status(&self, target: &str) -> TransformStatus {
        let text: String = self
            .raw
            .query_one(
                "select status from transform_definitions \
                 where split_part(target_table, '.', 2) = $1",
                &[&target],
            )
            .await
            .expect("read status")
            .get(0);
        TransformStatus::from_persisted(&text).expect("known status")
    }

    /// What `status()` reports as the definition's `capture_failure`.
    pub async fn capture_failure(&self, target: &str) -> Option<String> {
        self.trellis
            .status(target)
            .await
            .expect("status")
            .expect(target)
            .capture_failure
            .map(|failure| failure.error)
    }

    /// The keys held for any definition.
    pub async fn held(&self) -> i64 {
        self.raw
            .query_one(
                "select (select count(*) from poison) + (select count(*) from poison_held)",
                &[],
            )
            .await
            .expect("count held keys")
            .get(0)
    }

    /// Rounds of a pass, the builds it asks for, and a drain, until every
    /// target is live: a bounded number of explicit steps, not a timed wait.
    pub async fn converge(&mut self, label: &str) {
        for _ in 0..8 {
            self.pass().await;
            self.build().await;
            self.drain().await;
            let mut all_live = true;
            for target in self.targets.clone() {
                all_live &= self.status(&target).await == TransformStatus::Live;
            }
            if all_live {
                return;
            }
        }
        for target in self.targets.clone() {
            eprintln!(
                "{target}: {:?}",
                self.trellis.status(&target).await.expect("status")
            );
        }
        for sql in [
            "select to_jsonb(f) from capture_failures f",
            "select to_jsonb(r) from resume_requests r",
            "select to_jsonb(p) from poison p",
            "select to_jsonb(d) - 'source_columns' - 'definition' from transform_definitions d",
        ] {
            eprintln!("{sql}: {:?}", self.rows(sql).await);
        }
        panic!("{label}: not every definition went live within 8 rounds");
    }

    pub async fn rows(&self, sql: &str) -> Vec<String> {
        self.raw
            .query(&format!("select r::text from ({sql}) r order by 1"), &[])
            .await
            .expect(sql)
            .into_iter()
            .map(|row| row.get(0))
            .collect()
    }

    /// `self_check`'s verdict, nothing draining in the background.
    pub async fn self_check(&self, target: &str) -> String {
        match self
            .trellis
            .self_check_to_end(target, SelfCheckMode::Strict, Duration::from_secs(3))
            .await
        {
            Ok(report) => format!("{:?}", report.outcome),
            Err(err) => format!("error: {err}"),
        }
    }

    /// `target` (`target_sql` over it) equals its oracle, is live, and holds
    /// no key.
    pub async fn assert_exact_unaudited(&self, target: &str, target_sql: &str, oracle_sql: &str) {
        assert_eq!(
            self.status(target).await,
            TransformStatus::Live,
            "{target}: {:?}",
            self.capture_failure(target).await
        );
        assert_eq!(
            self.rows(target_sql).await,
            self.rows(oracle_sql).await,
            "{target} against its oracle"
        );
        assert_eq!(self.held().await, 0, "{target}: no key is held");
    }

    /// [`Self::assert_exact_unaudited`], and `self_check` agrees.
    pub async fn assert_exact(&self, target: &str, target_sql: &str, oracle_sql: &str) {
        self.assert_exact_unaudited(target, target_sql, oracle_sql)
            .await;
        assert_eq!(self.self_check(target).await, "Converged", "{target}");
    }

    /// Every target in `paused` is paused with the relabel's reason, and
    /// every other target is live with no reason.
    pub async fn assert_paused_by_the_relabel(&self, paused: &[&str]) {
        for target in self.targets.clone() {
            if paused.contains(&target.as_str()) {
                assert_eq!(
                    self.status(&target).await,
                    TransformStatus::Paused,
                    "{target}"
                );
                let reason = self.capture_failure(&target).await.unwrap_or_default();
                assert!(reason.starts_with(REASON), "{target}: {reason}");
            } else {
                assert_eq!(
                    (
                        self.status(&target).await,
                        self.capture_failure(&target).await
                    ),
                    (TransformStatus::Live, None),
                    "{target}"
                );
            }
        }
    }

    pub async fn resume(&self, targets: &[&str]) {
        for target in targets {
            let text = format!("RESUME TRANSFORM {target}");
            self.trellis.apply(&text).await.expect(&text);
        }
    }

    pub async fn count(&self, sql: &str) -> i64 {
        self.raw.query_one(sql, &[]).await.expect(sql).get(0)
    }
}
