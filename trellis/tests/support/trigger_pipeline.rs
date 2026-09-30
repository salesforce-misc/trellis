//! A deterministic stand-in for a live pipeline, shared by the chained-hop
//! tests (`intermediate_hop_cdc.rs`, `chained_live_hops.rs`) and the
//! mixed-case and relationship-endpoint tests.
//!
//! [`Pipeline::attach`] runs the staging worker's reconcile pass by hand
//! (`trellis::client::reconcile_pass`), which installs the real capture
//! triggers on every table [`trellis::defs::publication_tables`] names and
//! discharges their join markers. From then on the application's own writes
//! stage their ring rows, and [`Pipeline::drain`] seals and drains the ring
//! by hand until nothing is pending. So a source row reaches the ring spelled
//! exactly the way capture spells it (the half of issue #267 a test that
//! hand-stages the row would write in itself), with no background worker, no
//! seal timer and no waiting for convergence (#297). Each seal/drain round is
//! its own batch, so a hop's own downstream staging always lands in a later
//! batch than the write that produced it.
//!
//! The captured set is exactly what [`trellis::defs::publication_tables`]
//! asks for, and [`Pipeline::attach`] asserts it up front: since issue #315
//! no Trellis-owned target is ever captured, and since #375 not even one
//! that is a relationship endpoint.

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use testkit::{TestCluster, TestDatabase};
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::staging::{StagedWatermark, apply, has_pending, retire_drained_segments};

const WAKE: &str = "hop_wake";
/// How many seal/drain rounds [`Pipeline::drain`] allows before declaring
/// the ring stuck. A chain needs one round per hop.
const MAX_ROUNDS: usize = 16;
/// How many reconcile passes [`Pipeline::attach`] allows for every join
/// marker to discharge. Nothing holds a table's lock here, so one pass
/// installs the capture and parks the markers, and the next discharges any
/// go-live catch-up the first dispatched.
const MAX_PASSES: usize = 5;

/// Starts a fresh cluster and isolated database, and returns a raw client
/// on it with `search_path` pinned. Create source tables and install
/// definitions on it, then hand all three to [`Pipeline::attach`].
pub async fn database() -> (TestCluster, TestDatabase, Client) {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    (cluster, db, raw)
}

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'"
        ))
        .await
        .expect("set search_path");
    client
}

/// Renders every non-retired segment plus its ring rows, so a ring that
/// never quiesces names the stuck `(src_table, key)` pairs directly.
async fn dump_ring(raw: &Client) -> String {
    let mut out = String::from("segments and ring contents:\n");
    let segments = raw
        .query(
            "select seg_seq, ring_slot, state from segments order by seg_seq",
            &[],
        )
        .await
        .expect("read segments");
    for seg in &segments {
        let seq: i64 = seg.get(0);
        let ring_slot: i16 = seg.get(1);
        let state: String = seg.get(2);
        out.push_str(&format!(
            "  seg_seq={seq} ring_slot={ring_slot} state={state}\n"
        ));
        let rows = raw
            .query(
                &format!(
                    "select src_table, key, op from seg_{ring_slot} \
                     order by src_table, key, change_id"
                ),
                &[],
            )
            .await
            .expect("read ring slot");
        for row in rows {
            let src_table: String = row.get(0);
            let key: String = row.get(1);
            let op: String = row.get(2);
            out.push_str(&format!("    {src_table:?} key={key:?} op={op}\n"));
        }
    }
    out
}

/// The capture triggers over the tables [`trellis::defs::publication_tables`]
/// names, with the ring sealed and drained by hand.
pub struct Pipeline {
    pub raw: Client,
    pub db: TestDatabase,
    _cluster: TestCluster,
}

impl Pipeline {
    /// Asserts [`trellis::defs::publication_tables`] is exactly
    /// `expected_captured`, then runs reconcile passes until each of those
    /// tables' capture is installed and every join marker has discharged,
    /// and drains what that staged. Call it after every definition is
    /// installed and before the first write the test wants captured.
    pub async fn attach(
        cluster: TestCluster,
        db: TestDatabase,
        mut raw: Client,
        expected_captured: &[&str],
    ) -> Self {
        let desired = trellis::defs::publication_tables(&db.pool)
            .await
            .expect("publication_tables");
        assert_eq!(
            desired, expected_captured,
            "only tables this instance does not own are captured (issues #315, #375)"
        );
        let mut markers = -1;
        for _ in 0..MAX_PASSES {
            trellis::client::reconcile_pass(
                &mut raw,
                &db.pool,
                DEFAULT_SCHEMA,
                WAKE,
                Duration::from_secs(5),
            )
            .await
            .expect("reconcile pass");
            markers = raw
                .query_one("select count(*) from pending_backfill", &[])
                .await
                .expect("count pending markers")
                .get::<_, i64>(0);
            if markers == 0 {
                break;
            }
        }
        assert_eq!(
            markers, 0,
            "every join marker must discharge within {MAX_PASSES} reconcile passes"
        );
        let installed = trellis::capture::reconcile::installed_tables(&raw, DEFAULT_SCHEMA)
            .await
            .expect("read the installed capture");
        let expected: BTreeSet<String> = expected_captured.iter().map(|t| t.to_string()).collect();
        assert_eq!(
            installed, expected,
            "the pass captures exactly those tables"
        );
        let mut pipeline = Self {
            raw,
            db,
            _cluster: cluster,
        };
        pipeline.drain().await;
        pipeline
    }

    /// Seals and drains until nothing is pending. A drain that fails panics
    /// with its error rather than being retried forever (issue #267's
    /// live-lock).
    pub async fn drain(&mut self) {
        let watermark = StagedWatermark::saturated();
        for _ in 0..MAX_ROUNDS {
            trellis::staging::seal_if_active_nonempty(&mut self.raw, WAKE)
                .await
                .expect("seal");
            while let Some(seg) = apply::next_claimable_segment(&self.raw)
                .await
                .expect("next claimable segment")
            {
                apply::drain_once(&self.db.pool, seg, "hop_test", 1, WAKE, &watermark)
                    .await
                    .expect("drain_once");
            }
            retire_drained_segments(&mut self.raw)
                .await
                .expect("retire drained segments");
            if !has_pending(&self.raw).await.expect("has_pending") {
                return;
            }
        }
        panic!(
            "the ring did not quiesce within {MAX_ROUNDS} seal/drain rounds\n{}",
            dump_ring(&self.raw).await
        );
    }

    /// Runs `sql`, which must select two columns, and returns them as a
    /// `first -> second` map of their text forms.
    pub async fn rows(&self, sql: &str) -> HashMap<String, Option<String>> {
        self.raw
            .query(sql, &[])
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"))
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect()
    }
}
