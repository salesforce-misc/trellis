//! Issue #267: multi-hop live transform chains driven through a real
//! [`trellis::Client`], rather than the direct/ring-bypassing backfill path
//! (`backfill_definition`, ADR-0007) every other chained-transform test in
//! this suite uses.
//!
//! The distinction is the whole point. A chain's intermediate hop (`h1` in
//! `src -> h1 -> h2`) is two things at once: the *target* of the upstream
//! definition — so a write to it gets a downstream `Recompute` staged
//! directly, in the applying transaction, by `apply.rs`'s "4. Downstream
//! propagation" step — and a *source* of the downstream definition, so
//! `client::reconcile_source_tables` eventually adds it to the CDC
//! publication, after which ordinary intake independently decodes and stages
//! that very same write again.
//!
//! Before issue #267's fix those two stagings disagreed on how to spell the
//! one logical table: propagation used the bare `TransformDef::target`
//! (`"h1"`), intake used `publication::qualify`'s persisted identity
//! (`"public.h1"`). `staging::fold` groups by `(src_table, key)` in SQL, on
//! the raw strings, so the two never coalesced — they rode into `h2`'s apply
//! as two independent changes for one key, and the batched `INSERT ... ON
//! CONFLICT DO UPDATE` was handed the same conflict key twice:
//!
//! ```text
//! ERROR: ON CONFLICT DO UPDATE command cannot affect row a second time
//! ```
//!
//! which is a *permanent* live-lock, not a flake: every retry re-derives the
//! identical pair from the identical still-present ring rows, so the segment
//! stays `state = 'draining'` forever. These tests therefore assert real
//! convergence with a generous-but-bounded timeout, and additionally that no
//! segment is left stuck mid-drain.

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::install_definition;
use trellis::{Client as TrellisClient, ClientOptions};

/// Connects directly to `dsn` (bypassing `trellis::Pool`) and pins
/// `search_path`, matching `client_e2e.rs`'s helper of the same name.
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

/// Polls `predicate` until it returns `true`, or panics with `message` and a
/// dump of every non-drained segment plus its ring rows once `timeout`
/// elapses — the diagnostic the issue's own investigation had to assemble by
/// hand, so a future regression reports the stuck `(src_table, key)` pair
/// directly instead of just "timed out".
async fn poll_until<F>(raw: &Client, timeout: Duration, message: &str, mut predicate: F)
where
    F: AsyncFnMut() -> bool,
{
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if predicate().await {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "timed out after {timeout:?}: {message}\n{}",
                dump(raw).await
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Renders every segment's slot/state/seq plus that slot's ring rows, for
/// [`poll_until`]'s panic message.
async fn dump(raw: &Client) -> String {
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

/// Asserts no segment is parked mid-drain — the observable signature of
/// issue #267's live-lock, distinct from "slow": a `draining` segment that
/// never reaches `drained` means the apply transaction is failing
/// deterministically and being retried forever.
async fn assert_no_stuck_segment(raw: &Client) {
    let stuck: i64 = raw
        .query_one(
            "select count(*) from segments where state = 'draining'",
            &[],
        )
        .await
        .expect("count draining segments")
        .get(0);
    assert_eq!(
        stuck,
        0,
        "a segment is stuck mid-drain — issue #267's duplicate-`src_table` live-lock:\n{}",
        dump(raw).await
    );
}

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

/// Options every test here shares: a full client (staging + application
/// workers) with a reconcile cadence short enough that an intermediate hop
/// joins the publication within the test, rather than after it — the
/// precondition issue #267's workaround deliberately avoided by setting this
/// interval long.
fn live_options() -> ClientOptions {
    ClientOptions {
        staging_worker: true,
        application_threads: 4,
        source_tables: vec!["public.src".to_string()],
        reconcile_interval: Duration::from_millis(200),
        ..Default::default()
    }
}

/// Waits until `table` has actually joined the CDC publication. Every test
/// here waits for this *before* writing, so the duplicate-staging window is
/// guaranteed open rather than raced into: it is exactly the moment the
/// propagation path and the intake path both start observing the same write.
async fn wait_for_publication(raw: &Client, table: &str) {
    poll_until(
        raw,
        Duration::from_secs(30),
        &format!("{table} never joined the CDC publication"),
        async || {
            let published: i64 = raw
                .query_one(
                    "select count(*) from pg_publication_tables where tablename = $1",
                    &[&table],
                )
                .await
                .expect("read pg_publication_tables")
                .get(0);
            published > 0
        },
    )
    .await;
}

/// Reads `table` as an `id -> value` map, for the convergence assertions.
async fn snapshot(raw: &Client, table: &str, value_col: &str) -> HashMap<String, Option<String>> {
    raw.query(
        &format!("select id::text, {value_col}::text from {table}"),
        &[],
    )
    .await
    .unwrap_or_else(|e| panic!("read {table}: {e}"))
    .into_iter()
    .map(|row| (row.get(0), row.get(1)))
    .collect()
}

/// The issue's own repro, verbatim in shape: a 2-hop 1-1 passthrough chain,
/// one insert, driven entirely through a live `Client`.
#[tokio::test]
async fn a_two_hop_one_to_one_chain_converges_once_the_middle_hop_joins_the_publication() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    raw.batch_execute("create table public.src (id integer primary key, val numeric)")
        .await
        .expect("create src");

    let client = TrellisClient::start(db.dsn(), live_options()).expect("client start");

    let columns = numeric_columns(&["id", "val"]);
    install_definition(
        &db.pool,
        "TRANSFORM h1 FROM public.src SELECT val AS val",
        &columns,
        "public",
    )
    .await
    .expect("install h1");
    install_definition(
        &db.pool,
        "TRANSFORM h2 FROM public.h1 SELECT val AS val",
        &columns,
        "public",
    )
    .await
    .expect("install h2");

    // `h1` is an intermediate hop, so `reconcile_source_tables` derives it
    // into the publication from `h2`'s `source_table`. Only after that is
    // the bug's precondition met.
    wait_for_publication(&raw, "h1").await;

    raw.execute("insert into public.src (id, val) values (1, 1)", &[])
        .await
        .expect("insert into src");

    poll_until(
        &raw,
        Duration::from_secs(45),
        "h2 never converged through the 2-hop live chain (issue #267)",
        async || {
            snapshot(&raw, "h2", "val").await
                == HashMap::from([("1".to_string(), Some("1".to_string()))])
        },
    )
    .await;

    assert_no_stuck_segment(&raw).await;
    client.shutdown().await.expect("clean shutdown");
}

/// Issue #267 follow-up (the issue's repro is 1-1 only, 2 hops only): the
/// same double-staging shape is not special to depth 2 or to passthrough
/// hops. `h2` here is a *deeper* intermediate hop, and `h3` an aggregate
/// reading it — so the duplicate lands on an aggregate's source (whose Phase
/// 3 write shape is `apply_aggregate`'s sequential per-group upserts, not
/// `apply_target`'s batched `ON CONFLICT`), and `h1`/`h2` both sit in the
/// publication as intermediate hops simultaneously.
#[tokio::test]
async fn a_three_hop_chain_ending_in_an_aggregate_converges_with_every_hop_published() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    raw.batch_execute("create table public.src (id integer primary key, val numeric)")
        .await
        .expect("create src");

    let client = TrellisClient::start(db.dsn(), live_options()).expect("client start");

    let columns = numeric_columns(&["id", "val"]);
    for text in [
        "TRANSFORM h1 FROM public.src SELECT val AS val",
        "TRANSFORM h2 FROM public.h1 SELECT val AS val",
    ] {
        install_definition(&db.pool, text, &columns, "public")
            .await
            .unwrap_or_else(|e| panic!("install {text:?}: {e}"));
    }

    // A chained aggregate needs its source's old image to subtract a changed
    // row's previous contribution, so the intermediate hop it reads must carry
    // `REPLICA IDENTITY FULL` before `h3` will install at all — the same
    // widening `defs_aggregate_group_by_relationship.rs` does for its own
    // chained aggregate. Orthogonal to issue #267, just a precondition of the
    // shape. (Issue #56 makes this safe for the key: `extract_key` reads the
    // real primary key rather than trusting `pgoutput`'s `is_key` flags, which
    // `FULL` sets on every column — so intake's key for an `h2` row is still
    // its `id`, the same key the propagation path stages.)
    raw.batch_execute("alter table public.h2 replica identity full")
        .await
        .expect("widen h2's replica identity");
    install_definition(
        &db.pool,
        "TRANSFORM h3 FROM public.h2 GROUP BY val SELECT COUNT(*) AS n",
        &columns,
        "public",
    )
    .await
    .expect("install h3");

    wait_for_publication(&raw, "h1").await;
    wait_for_publication(&raw, "h2").await;

    raw.execute(
        "insert into public.src (id, val) values (1, 7), (2, 7)",
        &[],
    )
    .await
    .expect("insert into src");

    poll_until(
        &raw,
        Duration::from_secs(45),
        "h3 never converged through the 3-hop live chain ending in an aggregate (issue #267)",
        async || {
            let groups: HashMap<String, Option<String>> = raw
                .query("select val::text, n::text from h3", &[])
                .await
                .expect("read h3")
                .into_iter()
                .map(|row| (row.get(0), row.get(1)))
                .collect();
            groups == HashMap::from([("7".to_string(), Some("2".to_string()))])
        },
    )
    .await;

    assert_no_stuck_segment(&raw).await;
    client.shutdown().await.expect("clean shutdown");
}
