//! A deterministic stand-in for a live pipeline, shared by the chained-hop
//! tests (`intermediate_hop_cdc.rs`, `chained_live_hops.rs`).
//!
//! It reads real `pgoutput` bytes off a second logical slot with
//! `pg_logical_slot_get_binary_changes` and feeds them to a real
//! [`intake::Intake`] through `handle_event`, then seals and drains the ring
//! by hand until nothing is pending. So a source row reaches the ring
//! spelled exactly the way intake spells it (the half of issue #267 a test
//! that hand-stages the CDC row would write in itself), with no background
//! worker, no seal timer and no waiting for convergence (#297). Each
//! seal/drain round is its own batch, so a hop's own downstream staging
//! always lands in a later batch than the write that produced it.
//!
//! The publication is exactly what [`trellis::defs::publication_tables`]
//! asks for, and [`Pipeline::attach`] asserts that set up front: since issue
//! #315 no Trellis-owned target is ever published, and since #375 not even
//! one that is a relationship endpoint.

use std::collections::HashMap;

use bytes::Bytes;
use pgwire_replication::{Lsn, ReplicationEvent};
use testkit::{TestCluster, TestDatabase};
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::intake::{self, spill};
use trellis::staging::{StagedWatermark, apply, has_pending, retire_drained_segments};

const PUBLICATION: &str = "hop_pub";
/// The slot [`intake::Intake::connect`] requires to exist and be healthy.
/// Nothing reads its stream: the pipeline feeds intake from [`BYTES_SLOT`].
const INTAKE_SLOT: &str = "hop_intake";
/// The slot the pipeline reads `pgoutput` bytes from.
const BYTES_SLOT: &str = "hop_bytes";
const WAKE: &str = "hop_wake";
/// How many seal/drain rounds [`Pipeline::feed_and_drain`] allows before
/// declaring the ring stuck. A chain needs one round per hop.
const MAX_ROUNDS: usize = 16;

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

fn be_u64(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes[..8].try_into().expect("8 bytes"))
}

/// Turns one `pgoutput` protocol-v1 message into the event the replication
/// transport would have handed intake. Begin, Commit and logical-decoding
/// messages are peeled off by the transport, so they are decoded here;
/// everything else goes through as raw `XLogData`, exactly as intake sees it.
fn to_event(data: &[u8]) -> ReplicationEvent {
    match data[0] {
        b'B' => ReplicationEvent::Begin {
            final_lsn: Lsn::from(be_u64(&data[1..])),
            commit_time_micros: be_u64(&data[9..]) as i64,
            xid: u32::from_be_bytes(data[17..21].try_into().expect("4 bytes")),
        },
        b'C' => ReplicationEvent::Commit {
            lsn: Lsn::from(be_u64(&data[2..])),
            end_lsn: Lsn::from(be_u64(&data[10..])),
            commit_time_micros: be_u64(&data[18..]) as i64,
        },
        b'M' => {
            let transactional = data[1] & 1 == 1;
            let lsn = Lsn::from(be_u64(&data[2..]));
            let rest = &data[10..];
            let nul = rest
                .iter()
                .position(|b| *b == 0)
                .expect("prefix terminator");
            let prefix = String::from_utf8(rest[..nul].to_vec()).expect("utf-8 prefix");
            let body = &rest[nul + 1..];
            let len = u32::from_be_bytes(body[..4].try_into().expect("4 bytes")) as usize;
            ReplicationEvent::Message {
                transactional,
                lsn,
                prefix,
                content: Bytes::copy_from_slice(&body[4..4 + len]),
            }
        }
        _ => ReplicationEvent::XLogData {
            wal_start: Lsn::from(0),
            wal_end: Lsn::from(0),
            server_time_micros: 0,
            data: Bytes::copy_from_slice(data),
        },
    }
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
        let slot: i16 = seg.get(1);
        let state: String = seg.get(2);
        out.push_str(&format!("  seg_seq={seq} slot={slot} state={state}\n"));
        let rows = raw
            .query(
                &format!(
                    "select src_table, key, op from seg_{slot} order by src_table, key, change_id"
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

/// A real [`intake::Intake`] over the tables [`trellis::defs::publication_tables`]
/// names, fed and drained by hand.
pub struct Pipeline {
    pub raw: Client,
    pub db: TestDatabase,
    intake: intake::Intake,
    _cluster: TestCluster,
}

impl Pipeline {
    /// Asserts [`trellis::defs::publication_tables`] is exactly
    /// `expected_published`, publishes those tables, and connects intake.
    /// Call it after every definition is installed and before the first
    /// write the test wants intake to decode.
    pub async fn attach(
        cluster: TestCluster,
        db: TestDatabase,
        raw: Client,
        expected_published: &[&str],
    ) -> Self {
        let published = trellis::defs::publication_tables(&db.pool)
            .await
            .expect("publication_tables");
        assert_eq!(
            published, expected_published,
            "only tables this instance does not own are published (issues #315, #375)"
        );
        // Each `schema.table` component quoted, so a mixed-case table is
        // published as itself rather than a case-folded name (issue #561).
        let quoted: Vec<String> = published
            .iter()
            .map(|name| {
                let (schema, table) = name.split_once('.').expect("a qualified name");
                format!("\"{schema}\".\"{table}\"")
            })
            .collect();
        raw.batch_execute(&format!(
            "create publication {PUBLICATION} for table {}",
            quoted.join(", ")
        ))
        .await
        .expect("create publication");
        for slot in [INTAKE_SLOT, BYTES_SLOT] {
            raw.query_one(
                "select slot_name from pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&slot],
            )
            .await
            .expect("create slot");
        }
        raw.execute(
            "insert into replication_progress (slot_name, confirmed_lsn) \
             select slot_name, confirmed_flush_lsn from pg_replication_slots \
             where slot_name = $1 and database = current_database()",
            &[&INTAKE_SLOT],
        )
        .await
        .expect("seed replication_progress");

        let config = intake::IntakeConfig {
            dsn: db.dsn().to_string(),
            schema: DEFAULT_SCHEMA.to_string(),
            host: db.socket_dir().display().to_string(),
            port: db.port(),
            user: "postgres".to_string(),
            password: String::new(),
            database: db.name().to_string(),
            slot: INTAKE_SLOT.to_string(),
            publication: PUBLICATION.to_string(),
            wake_channel: WAKE.to_string(),
            spill_threshold: spill::DEFAULT_SPILL_THRESHOLD,
            hard_cap: spill::DEFAULT_HARD_CAP,
            group_commit: None,
        };
        let intake = intake::Intake::connect(&config, StagedWatermark::new(), db.pool.clone())
            .await
            .expect("connect intake");

        Self {
            raw,
            db,
            intake,
            _cluster: cluster,
        }
    }

    /// Feeds intake everything committed since the last call, then seals and
    /// drains until nothing is pending. A drain that fails panics with its
    /// error rather than being retried forever (issue #267's live-lock).
    pub async fn feed_and_drain(&mut self) {
        self.feed_intake().await;
        self.drain_to_quiescence().await;
    }

    /// [`Self::feed_and_drain`] twice: the second round has intake decode
    /// every transaction the first round's drains committed, so nothing a
    /// drain wrote can still be on its way back into the ring.
    // `intermediate_hop_cdc.rs` steps through the two rounds itself.
    #[allow(dead_code)]
    pub async fn settle(&mut self) {
        self.feed_and_drain().await;
        self.feed_and_drain().await;
    }

    /// Consumes everything committed on [`BYTES_SLOT`] since the last call
    /// and feeds it to intake, which stages it into the ring. Nothing is
    /// sealed or drained, so a test can inspect what intake staged.
    pub async fn feed_intake(&mut self) {
        let rows = self
            .raw
            .query(
                "select data from pg_logical_slot_get_binary_changes($1, null, null, \
                 'proto_version', '1', 'publication_names', $2, 'messages', 'true')",
                &[&BYTES_SLOT, &PUBLICATION],
            )
            .await
            .expect("read pgoutput bytes");
        for row in rows {
            let data: Vec<u8> = row.get(0);
            self.intake
                .handle_event(to_event(&data))
                .await
                .expect("intake handles the event");
        }
    }

    async fn drain_to_quiescence(&mut self) {
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

    pub async fn finish(self) {
        // Intake's own stream was never read, so its walsender would hold up
        // cluster shutdown waiting for a flush confirmation that never comes.
        self.raw
            .execute(
                "select pg_terminate_backend(active_pid) from pg_replication_slots \
                 where slot_name = $1 and active_pid is not null",
                &[&INTAKE_SLOT],
            )
            .await
            .expect("terminate intake's walsender");
    }
}
