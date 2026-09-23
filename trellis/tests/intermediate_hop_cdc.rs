//! Issue #312: a write to a chain's intermediate hop must reach a downstream
//! aggregate once, even when the hop is in the publication.
//!
//! `h1` below is the target of `src -> h1` and the source of the aggregate
//! `h1 -> h3`, so it is published. The drain that writes `h1` stages its own
//! downstream `Recompute` for `h3` inside the writing transaction, and intake
//! would decode that same write again as CDC. When the two copies land in
//! different batches, the aggregate re-derives the group from live state
//! (which already holds the write) and then adds the CDC delta on top of it.
//!
//! The test drives intake and the drain by hand so the two copies are forced
//! into separate batches every time, rather than relying on seal timing: it
//! reads real `pgoutput` bytes off a second logical slot with
//! `pg_logical_slot_get_binary_changes` and feeds them to a real
//! [`intake::Intake`] through `handle_event`.

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

#[tokio::test]
async fn an_aggregate_counts_a_published_intermediate_hops_write_once() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;

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
    // An aggregate over `h1` needs `h1`'s full old image.
    raw.batch_execute("alter table public.h1 replica identity full")
        .await
        .expect("widen h1's replica identity");
    install_definition(
        &db.pool,
        "TRANSFORM h3 FROM public.h1 GROUP BY val SELECT COUNT(*) AS n",
        &columns,
        "public",
    )
    .await
    .expect("install h3");

    raw.batch_execute(&format!(
        "create publication {PUBLICATION} for table public.src, public.h1"
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
    let mut intake = intake::Intake::connect(&config, StagedWatermark::new(), db.pool.clone())
        .await
        .expect("connect intake");

    raw.execute("insert into public.src (id, val) values (1, 7)", &[])
        .await
        .expect("insert into src");

    // The `src` insert reaches the ring, and draining it writes `h1`, whose
    // own downstream `Recompute` then drains into `h3` in a later batch.
    feed_intake(&raw, &mut intake).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        h3_groups(&raw).await,
        HashMap::from([("7".to_string(), "1".to_string())]),
        "the in-transaction propagation alone must count the row once"
    );

    // Intake now decodes the transaction that wrote `h1`. Its CDC copy of
    // that write is the second producer: staged, it would drain as a delta
    // on top of the group the `Recompute` already re-derived.
    feed_intake(&raw, &mut intake).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        h3_groups(&raw).await,
        HashMap::from([("7".to_string(), "1".to_string())]),
        "the CDC copy of an already-propagated hop write must not be counted again"
    );

    // Intake's own stream was never read, so its walsender would hold up
    // cluster shutdown waiting for a flush confirmation that never comes.
    raw.execute(
        "select pg_terminate_backend(active_pid) from pg_replication_slots \
         where slot_name = $1 and active_pid is not null",
        &[&INTAKE_SLOT],
    )
    .await
    .expect("terminate intake's walsender");
}
