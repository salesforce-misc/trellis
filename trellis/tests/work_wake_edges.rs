//! The edges that make build work claimable announce themselves (issue #273):
//! an idle drain worker backs off its wake, so each change that turns the
//! build probe from "nothing" into "something" must `NOTIFY` the work channel
//! (migration V86's triggers) or, for a retry coming due, be scheduled by the
//! worker from the probe's `next_due`.
//!
//! Every test acts and then reads what the listening session was told. A
//! notification is delivered before the listener's next command completes, so
//! `Listener::heard` runs `select 1` and then reads what arrived: no sleeping,
//! no polling until something converges (#297).

use std::time::Duration;

use testkit::{TestCluster, TestDatabase};
use tokio_postgres::{AsyncMessage, Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, chunk_queue};
use trellis::staging::build;

const AGG: &str =
    "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX SELECT SUM(v) AS total";

/// A session `LISTEN`ing on the work channel.
struct Listener {
    client: Client,
    rx: tokio::sync::mpsc::UnboundedReceiver<()>,
}

impl Listener {
    async fn open(dsn: &str, channel: &str) -> Self {
        let (client, mut connection) = tokio_postgres::connect(dsn, NoTls)
            .await
            .expect("connect listener");
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                match std::future::poll_fn(|cx| connection.poll_message(cx)).await {
                    Some(Ok(AsyncMessage::Notification(_))) => {
                        let _ = tx.send(());
                    }
                    Some(Ok(_)) => continue,
                    _ => break,
                }
            }
        });
        client
            .batch_execute(&format!("listen \"{channel}\""))
            .await
            .expect("listen");
        Self { client, rx }
    }

    /// How many notifications have reached this session since the last call.
    async fn heard(&mut self) -> usize {
        self.client.simple_query("select 1").await.expect("flush");
        let mut n = 0;
        while self.rx.try_recv().is_ok() {
            n += 1;
        }
        n
    }
}

struct Fixture {
    db: TestDatabase,
    raw: Client,
    listener: Listener,
    _cluster: TestCluster,
}

impl Fixture {
    async fn new() -> Self {
        let cluster = TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (raw, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        raw.batch_execute(&format!(
            "set search_path to {DEFAULT_SCHEMA}, public; \
             create table public.src (id integer primary key, g integer, v bigint); \
             insert into public.src select i, i % 7, i from generate_series(1, 50) i"
        ))
        .await
        .expect("seed");
        let channel: String = raw
            .query_one("select work_wake_channel()", &[])
            .await
            .expect("channel")
            .get(0);
        assert_eq!(channel, format!("{DEFAULT_SCHEMA}_work"));
        let mut listener = Listener::open(db.dsn(), &channel).await;
        let columns = [
            (
                "id".to_string(),
                ValueType::Integer(trellis::IntWidth::Int4),
            ),
            ("g".to_string(), ValueType::Integer(trellis::IntWidth::Int4)),
            ("v".to_string(), ValueType::Integer(trellis::IntWidth::Int8)),
        ]
        .into_iter()
        .collect();
        trellis::defs::install_definition(&db.pool, AGG, &columns, "public")
            .await
            .expect("register");
        listener.heard().await;
        // Freeze the definition so only the chunks' own edges are in play.
        raw.batch_execute("update transform_definitions set status = 'paused'")
            .await
            .expect("pause");
        assert_eq!(listener.heard().await, 0, "pausing is not an edge");
        Self {
            db,
            raw,
            listener,
            _cluster: cluster,
        }
    }

    /// One unclaimed, undone chunk for the definition.
    async fn add_chunk(&self) -> i64 {
        self.raw
            .query_one(
                "insert into backfill_chunks (definition_id, kind, lo, hi, fuse_rearmed_at) \
                 select id, 'plan', null, null, fuse_rearmed_at from transform_definitions \
                 returning id",
                &[],
            )
            .await
            .expect("insert chunk")
            .get(0)
    }

    async fn sql(&self, sql: &str) {
        self.raw.batch_execute(sql).await.expect(sql);
    }
}

#[tokio::test]
async fn a_new_unclaimed_chunk_announces_itself() {
    let mut f = Fixture::new().await;
    f.add_chunk().await;
    assert_eq!(f.listener.heard().await, 1);
}

#[tokio::test]
async fn a_claim_released_or_reclaimed_announces_itself() {
    let mut f = Fixture::new().await;
    f.sql("update transform_definitions set status = 'backfilling', build = null")
        .await;
    f.add_chunk().await;
    f.listener.heard().await;

    // Claiming it, and a worker refreshing its claim, are not edges.
    let claimed = {
        let client = f.db.pool.get().await.expect("pool");
        chunk_queue::claim_chunks_of(&**client, "w1", 1, &[chunk_queue::KIND_PLAN])
            .await
            .expect("claim")
    };
    assert_eq!(claimed.len(), 1);
    f.sql("update backfill_chunks set claimed_at = now()").await;
    assert_eq!(f.listener.heard().await, 0, "claim and refresh");

    // The worker gives it back.
    let mut raw = f.db.pool.get().await.expect("pool");
    let released = chunk_queue::release_chunk(&mut **raw, claimed[0].id, "w1")
        .await
        .expect("release");
    assert_eq!(released, 1);
    assert_eq!(f.listener.heard().await, 1, "release");

    // A claim nobody refreshes is swept free.
    {
        let client = f.db.pool.get().await.expect("pool");
        chunk_queue::claim_chunks_of(&**client, "w2", 1, &[chunk_queue::KIND_PLAN])
            .await
            .expect("claim");
    }
    f.listener.heard().await;
    let swept = chunk_queue::reclaim_stale_chunks(&mut **raw, Duration::ZERO)
        .await
        .expect("sweep");
    assert_eq!(swept, 1);
    assert_eq!(f.listener.heard().await, 1, "reclaim");
}

#[tokio::test]
async fn a_retry_cleared_or_a_chunk_filled_in_announces_itself() {
    let mut f = Fixture::new().await;
    f.add_chunk().await;
    f.sql("update backfill_chunks set next_attempt_at = now() + interval '1 hour'")
        .await;
    f.listener.heard().await;

    // Backing off further, or off again, is not an edge to work; the
    // backoff ending is, here by the retry-now update the builds use.
    f.sql("update backfill_chunks set next_attempt_at = null")
        .await;
    assert_eq!(f.listener.heard().await, 1, "backoff cleared");

    f.sql("update backfill_chunks set fields = array['total'], kind = 'plan'")
        .await;
    assert_eq!(f.listener.heard().await, 1, "fields filled in");

    // Finishing a chunk is not an edge.
    f.sql("update backfill_chunks set done = true").await;
    assert_eq!(f.listener.heard().await, 0, "done");
}

#[tokio::test]
async fn a_definition_entering_a_build_state_announces_itself() {
    let mut f = Fixture::new().await;
    f.sql("update transform_definitions set status = 'backfilling'")
        .await;
    assert_eq!(f.listener.heard().await, 1, "paused -> backfilling");
    f.sql("update transform_definitions set status = 'catching_up'")
        .await;
    assert_eq!(f.listener.heard().await, 1, "backfilling -> catching_up");
    f.sql("update transform_definitions set build = 'rederive'")
        .await;
    assert_eq!(f.listener.heard().await, 1, "build changed");
    // Every status a chunk is handed out in announces entering it (a resume
    // or an unfreeze makes the definition's existing chunks claimable
    // without touching a chunk row, #1047); a frozen status is no edge.
    for status in trellis::defs::TransformStatus::ALL {
        f.sql("update transform_definitions set status = 'paused'")
            .await;
        f.listener.heard().await;
        f.sql(&format!(
            "update transform_definitions set status = '{}'",
            status.as_str()
        ))
        .await;
        let dispatchable =
            trellis::defs::TransformStatus::dispatchable().contains(&status.as_str());
        assert_eq!(
            f.listener.heard().await,
            usize::from(dispatchable),
            "paused -> {}",
            status.as_str()
        );
    }
    f.sql("update transform_definitions set status = 'live'")
        .await;
    f.listener.heard().await;
    f.sql("update transform_definitions set status = 'live'")
        .await;
    assert_eq!(f.listener.heard().await, 0, "unchanged");
}

/// Resuming a paused definition holding an undone chunk: the probe is false
/// while paused and true after, and the resume sent the wake (#1047).
#[tokio::test]
async fn resuming_a_paused_definition_with_a_chunk_wakes_workers() {
    let mut f = Fixture::new().await;
    f.add_chunk().await;
    f.listener.heard().await;
    let probe = || async {
        build::work_probe(&f.db.pool)
            .await
            .expect("probe")
            .may_exist
    };
    assert!(!probe().await, "paused");
    f.sql("update transform_definitions set status = 'waiting_to_backfill'")
        .await;
    assert_eq!(f.listener.heard().await, 1, "resume woke the workers");
    assert!(probe().await, "resumed");
}

/// The one edge nothing announces: a backing-off chunk coming due. The probe
/// reports when, so the idle worker wakes for it itself.
#[tokio::test]
async fn the_probe_reports_when_a_backing_off_chunk_comes_due() {
    let f = Fixture::new().await;
    // A definition chunks are handed out in (the fixture pauses it).
    f.sql("update transform_definitions set status = 'backfilling', build = null")
        .await;
    let probe = || async { build::work_probe(&f.db.pool).await.expect("probe") };
    assert_eq!(probe().await.next_due, None, "no chunk");

    f.add_chunk().await;
    let ready = probe().await;
    assert!(ready.may_exist, "an unclaimed chunk");
    assert_eq!(ready.next_due, None, "claimable now, nothing to wait for");

    f.sql(
        "update backfill_chunks set next_attempt_at = now() + interval '1 hour', last_error = 'x'",
    )
    .await;
    let backing_off = probe().await;
    assert!(!backing_off.may_exist);
    let due = backing_off.next_due.expect("a retry is coming");
    assert!(
        due > Duration::from_secs(3500) && due <= Duration::from_secs(3600),
        "{due:?}"
    );

    // A second chunk sooner than the first sets the time.
    f.add_chunk().await;
    f.sql(
        "update backfill_chunks set next_attempt_at = now() + interval '30 seconds' \
           where id = (select max(id) from backfill_chunks)",
    )
    .await;
    let sooner = probe().await.next_due.expect("due");
    assert!(sooner <= Duration::from_secs(30), "{sooner:?}");

    // Claimed and done chunks come due for nobody.
    f.sql(
        "update backfill_chunks set claimed_by = 'peer', claimed_at = now() \
           where next_attempt_at is not null",
    )
    .await;
    assert_eq!(probe().await.next_due, None);
}

/// The work channel is cut to Postgres's 63-byte limit on a character
/// boundary, and the triggers' baked-in name is the one `work_wake_channel()`
/// hands the workers: for a schema whose name fills the limit, a multi-byte
/// one that has to be cut mid-name, and one that needs quoting.
#[tokio::test]
async fn a_long_or_quoted_schemas_triggers_notify_the_channel_its_workers_listen_on() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    for schema in ["s".repeat(63), "é".repeat(31), "Inst \"B\" x".to_string()] {
        let config =
            trellis::Config::with_schema(db.dsn().to_string(), schema.as_str()).expect("schema");
        let handle = trellis::Trellis::connect(config, trellis::TrellisOptions::default())
            .await
            .expect("connect");
        handle.migrate().await.expect("migrate");
        handle.shutdown().await.expect("shutdown");

        let (raw, connection) = tokio_postgres::connect(db.dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let quoted = format!("\"{}\"", schema.replace('"', "\"\""));
        let channel: String = raw
            .query_one(&format!("select {quoted}.work_wake_channel()"), &[])
            .await
            .expect("channel")
            .get(0);
        assert!(channel.len() <= 63, "{channel:?}");
        assert!(channel.ends_with("_work"), "{channel:?}");
        let cut = channel.strip_suffix("_work").expect("suffix");
        assert!(
            schema.starts_with(cut),
            "{channel:?} is cut from {schema:?}"
        );
        assert!(
            schema.len() + "_work".len() <= 63 || channel.len() > 63 - "é".len(),
            "{channel:?} is cut no shorter than it has to be"
        );

        let mut listener = Listener::open(db.dsn(), &channel.replace('"', "\"\"")).await;
        raw.batch_execute(&format!(
            "create temp table t (x integer); \
             create trigger chunk after insert on t \
                 for each row execute function {quoted}.announce_chunk_work(); \
             create trigger definition after update on t \
                 for each row execute function {quoted}.announce_definition_work(); \
             insert into t values (1)"
        ))
        .await
        .expect("fire the chunk trigger");
        assert_eq!(listener.heard().await, 1, "chunk trigger in {schema:?}");
        raw.batch_execute("update t set x = 2")
            .await
            .expect("fire the definition trigger");
        assert_eq!(
            listener.heard().await,
            1,
            "definition trigger in {schema:?}"
        );
    }
}
