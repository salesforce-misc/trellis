//! Issues #312 and #315: a write to a chain's intermediate hop must reach a
//! downstream aggregate exactly once, and must leave both the group a row
//! left and the group it joined correct.
//!
//! `h1` below is the target of `src -> h1` and the source of the aggregate
//! `h1 -> h3`. Since issue #315 a target table is never published: the drain
//! that writes `h1` stages its own downstream `Recompute` for `h3` inside the
//! writing transaction (`staging::target_mutations`), and that is the only
//! copy. Before, `h1` was published too, and intake decoded the same write a
//! second time; when the two copies landed in different batches the
//! aggregate re-derived the group from live state (which already held the
//! write) and then added the CDC delta on top of it.
//!
//! The test drives intake and the drain by hand so every hop lands in its own
//! batch, rather than relying on seal timing: it reads real `pgoutput` bytes
//! off a second logical slot with `pg_logical_slot_get_binary_changes` and
//! feeds them to a real [`intake::Intake`] through `handle_event`. The
//! publication is exactly what [`trellis::defs::publication_tables`] asks for.

use std::collections::HashMap;

use bytes::Bytes;
use pgwire_replication::{Lsn, ReplicationEvent};
use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::install_definition;
use trellis::intake::{self, spill};
use trellis::staging::{StagedWatermark, apply, has_pending, retire_drained_segments};

const PUBLICATION: &str = "hop_pub";
/// The slot [`intake::Intake::connect`] requires to exist and be healthy.
/// Nothing reads its stream: the test feeds intake from [`BYTES_SLOT`].
const INTAKE_SLOT: &str = "hop_intake";
/// The slot the test reads `pgoutput` bytes from.
const BYTES_SLOT: &str = "hop_bytes";
const WAKE: &str = "hop_wake";

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

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
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

/// Consumes everything committed on [`BYTES_SLOT`] since the last call and
/// feeds it to `intake`, which stages it into the ring.
async fn feed_intake(raw: &Client, intake: &mut intake::Intake) {
    let rows = raw
        .query(
            "select data from pg_logical_slot_get_binary_changes($1, null, null, \
             'proto_version', '1', 'publication_names', $2, 'messages', 'true')",
            &[&BYTES_SLOT, &PUBLICATION],
        )
        .await
        .expect("read pgoutput bytes");
    for row in rows {
        let data: Vec<u8> = row.get(0);
        intake
            .handle_event(to_event(&data))
            .await
            .expect("intake handles the event");
    }
}

/// Seals and drains until nothing is pending. Each round is its own batch, so
/// a hop's own downstream staging always lands in a later batch than the
/// write that produced it.
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    let watermark = StagedWatermark::saturated();
    for _ in 0..16 {
        trellis::staging::seal_if_active_nonempty(client, WAKE)
            .await
            .expect("seal");
        while let Some(seg) = apply::next_claimable_segment(&*client)
            .await
            .expect("next claimable segment")
        {
            apply::drain_once(pool, seg, "hop_test", 1, WAKE, &watermark)
                .await
                .expect("drain_once");
        }
        retire_drained_segments(client)
            .await
            .expect("retire drained segments");
        if !has_pending(client).await.expect("has_pending") {
            return;
        }
    }
    panic!("pipeline did not reach quiescence within 16 seal/drain rounds");
}

async fn h3_groups(raw: &Client) -> HashMap<String, String> {
    raw.query("select val::text, n::text from h3", &[])
        .await
        .expect("read h3")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

/// `src -> h1 -> h3` with `h1` published, and a real [`intake::Intake`] fed
/// by hand from [`BYTES_SLOT`].
struct Hop {
    raw: Client,
    intake: intake::Intake,
    db: testkit::TestDatabase,
    _cluster: TestCluster,
}

impl Hop {
    async fn start() -> Self {
        let cluster = TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let raw = connect_raw(db.dsn()).await;

        raw.batch_execute("create table public.src (id integer primary key, val numeric)")
            .await
            .expect("create src");
        let columns = numeric_columns(&["id", "val"]);
        install_definition(
            &db.pool,
            "TRANSFORM h1 FROM public.src SELECT val AS val",
            &columns,
            "public",
        )
        .await
        .expect("install h1");
        // No `REPLICA IDENTITY FULL` on `h1`: an aggregate over a target
        // never reads its CDC, so it has no old-image requirement (#315).
        install_definition(
            &db.pool,
            "TRANSFORM h3 FROM public.h1 GROUP BY val SELECT COUNT(*) AS n",
            &columns,
            "public",
        )
        .await
        .expect("install h3");

        let published = trellis::defs::publication_tables(&db.pool)
            .await
            .expect("publication_tables");
        assert_eq!(
            published,
            vec!["public.src".to_string()],
            "a chain's intermediate hop is a target, so it is never published"
        );
        raw.batch_execute(&format!(
            "create publication {PUBLICATION} for table {}",
            published.join(", ")
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
            "insert into replication_progress (slot_name, confirmed_lsn) values ($1, $2)",
            &[&INTAKE_SLOT, &PgLsn::from(0)],
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
            intake,
            db,
            _cluster: cluster,
        }
    }

    /// Feeds intake everything committed since the last call, then seals and
    /// drains until nothing is pending.
    async fn feed_and_drain(&mut self) {
        feed_intake(&self.raw, &mut self.intake).await;
        drain_to_quiescence(&self.db.pool, &mut self.raw).await;
    }

    async fn finish(self) {
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

#[tokio::test]
async fn an_aggregate_counts_an_intermediate_hops_write_once() {
    let mut hop = Hop::start().await;

    hop.raw
        .execute("insert into public.src (id, val) values (1, 7)", &[])
        .await
        .expect("insert into src");

    // The `src` insert reaches the ring, and draining it writes `h1`, whose
    // own downstream `Recompute` then drains into `h3` in a later batch.
    hop.feed_and_drain().await;
    assert_eq!(
        h3_groups(&hop.raw).await,
        HashMap::from([("7".to_string(), "1".to_string())]),
        "the in-transaction propagation alone must count the row once"
    );

    // Intake now decodes the transaction that wrote `h1`. `h1` isn't in the
    // publication, so nothing of that write reaches the ring a second time.
    hop.feed_and_drain().await;
    assert_eq!(
        h3_groups(&hop.raw).await,
        HashMap::from([("7".to_string(), "1".to_string())]),
        "an intermediate hop's write must never be counted twice"
    );

    hop.finish().await;
}

/// Issue #315's grain migration: `h3` groups by `h1.val`, a non-key column
/// of an upstream target. Moving a row from group 7 to group 8 must leave
/// group 7 correct as well as group 8. The recompute `h1`'s write stages is
/// image-less (re-read live), so on its own it only names group 8; the prior
/// image it carries is what tells `h3` to re-derive group 7 too. A delete is
/// the same: only the prior image names the group the row left.
#[tokio::test]
async fn an_aggregate_over_a_hop_follows_a_row_that_moves_groups_and_then_leaves() {
    let mut hop = Hop::start().await;

    hop.raw
        .batch_execute("insert into public.src (id, val) values (1, 7), (2, 7), (3, 9)")
        .await
        .expect("seed src");
    hop.feed_and_drain().await;
    assert_eq!(
        h3_groups(&hop.raw).await,
        HashMap::from([
            ("7".to_string(), "2".to_string()),
            ("9".to_string(), "1".to_string()),
        ]),
    );

    hop.raw
        .execute("update public.src set val = 8 where id = 1", &[])
        .await
        .expect("move row 1 from group 7 to group 8");
    hop.feed_and_drain().await;
    assert_eq!(
        h3_groups(&hop.raw).await,
        HashMap::from([
            ("7".to_string(), "1".to_string()),
            ("8".to_string(), "1".to_string()),
            ("9".to_string(), "1".to_string()),
        ]),
        "the row's old group must lose it, not only its new group gain it"
    );

    hop.raw
        .execute("update public.src set val = 9 where id = 2", &[])
        .await
        .expect("move group 7's last row into group 9");
    hop.raw
        .execute("delete from public.src where id = 1", &[])
        .await
        .expect("delete group 8's only row");
    hop.feed_and_drain().await;
    assert_eq!(
        h3_groups(&hop.raw).await,
        HashMap::from([("9".to_string(), "2".to_string())]),
        "a group every row left, by moving or by deletion, must be removed"
    );

    hop.finish().await;
}
