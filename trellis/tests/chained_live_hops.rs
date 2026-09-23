//! Multi-hop live transform chains driven through a real [`trellis::Client`],
//! rather than the direct/ring-bypassing backfill path (`backfill_definition`,
//! ADR-0007) or hand-run drains the other chained-transform tests use.
//!
//! A chain's intermediate hop (`h1` in `src -> h1 -> h2`) is two things at
//! once: the *target* of the upstream definition and a *source* of the
//! downstream one. Issue #267 found that, while `h1` sat in the CDC
//! publication, a write to it was staged twice under two spellings of the
//! table — once by the applying transaction's own downstream propagation and
//! once by intake — which live-locked the downstream apply. Issue #315 then
//! took intermediate hops out of the publication altogether: every write to
//! a target reaches its readers only through the target-mutation seam
//! (`staging::target_mutations`), inside the writing transaction, so there is
//! one staging and one spelling by construction. That also fixed an
//! aggregate target feeding another transform, whose CDC could not be
//! decoded at all (issue #315's original report: intake died on the first
//! change).
//!
//! These tests assert real convergence with a generous-but-bounded timeout,
//! that no segment is left stuck mid-drain, and that no hop is ever
//! published.

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

/// Waits until `table`'s definition is `live`: a plain 1-1 target builds
/// through the chunk queue, and nothing may chain off it before it is live
/// (issue #315).
async fn wait_for_live(raw: &Client, table: &str) {
    poll_until(
        raw,
        Duration::from_secs(30),
        &format!("{table} never went live"),
        async || {
            raw.query_opt(
                "select 1 from transform_definitions \
                 where split_part(target_table, '.', 2) = $1 and status = 'live'",
                &[&table],
            )
            .await
            .expect("read transform_definitions")
            .is_some()
        },
    )
    .await;
}

/// Waits until the reconcile loop has published `public.src`, then asserts
/// none of `hops` is published: a target's readers hear about it through
/// the target-mutation seam, never CDC (issue #315).
async fn assert_only_src_is_published(raw: &Client, hops: &[&str]) {
    poll_until(
        raw,
        Duration::from_secs(30),
        "src never joined the CDC publication",
        async || {
            raw.query_one(
                "select count(*) from pg_publication_tables where tablename = 'src'",
                &[],
            )
            .await
            .expect("read pg_publication_tables")
            .get::<_, i64>(0)
                > 0
        },
    )
    .await;
    for hop in hops {
        let published: i64 = raw
            .query_one(
                "select count(*) from pg_publication_tables where tablename = $1",
                &[hop],
            )
            .await
            .expect("read pg_publication_tables")
            .get(0);
        assert_eq!(
            published, 0,
            "{hop} is a target and must never be published"
        );
    }
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

/// Issue #267's repro, verbatim in shape: a 2-hop 1-1 passthrough chain, one
/// insert, driven entirely through a live `Client`.
#[tokio::test]
async fn a_two_hop_one_to_one_chain_converges_without_publishing_the_middle_hop() {
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
    wait_for_live(&raw, "h1").await;
    install_definition(
        &db.pool,
        "TRANSFORM h2 FROM public.h1 SELECT val AS val",
        &columns,
        "public",
    )
    .await
    .expect("install h2");
    wait_for_live(&raw, "h2").await;
    assert_only_src_is_published(&raw, &["h1", "h2"]).await;

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

/// Issue #267 follow-up (the issue's repro is 1-1 only, 2 hops only): `h2`
/// here is a *deeper* intermediate hop, and `h3` an aggregate reading it.
#[tokio::test]
async fn a_three_hop_chain_ending_in_an_aggregate_converges() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    raw.batch_execute("create table public.src (id integer primary key, val numeric)")
        .await
        .expect("create src");

    let client = TrellisClient::start(db.dsn(), live_options()).expect("client start");

    let columns = numeric_columns(&["id", "val"]);
    for (hop, text) in [
        ("h1", "TRANSFORM h1 FROM public.src SELECT val AS val"),
        ("h2", "TRANSFORM h2 FROM public.h1 SELECT val AS val"),
    ] {
        install_definition(&db.pool, text, &columns, "public")
            .await
            .unwrap_or_else(|e| panic!("install {text:?}: {e}"));
        wait_for_live(&raw, hop).await;
    }

    // No `REPLICA IDENTITY FULL` on `h2`: an aggregate over a target never
    // reads that target's CDC (issue #315).
    install_definition(
        &db.pool,
        "TRANSFORM h3 FROM public.h2 GROUP BY val SELECT COUNT(*) AS n",
        &columns,
        "public",
    )
    .await
    .expect("install h3");
    assert_only_src_is_published(&raw, &["h1", "h2", "h3"]).await;

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

/// Issue #315's original report: an aggregate target feeding another
/// transform. While the aggregate target was published, its first CDC change
/// killed intake (no primary key to decode a key from), so nothing
/// downstream ever converged again. `hist` counts `agg`'s groups by their
/// size, so moving a source row between `agg` groups also moves `agg` rows
/// between `hist` groups — the prior image each `agg` write carries is what
/// lets `hist` fix the group a row left.
#[tokio::test]
async fn an_aggregate_chained_off_an_aggregate_target_converges_and_follows_group_moves() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;

    // A text group key: a transform chained off an aggregate target reads
    // its group column as a primary key, which can't be `numeric`.
    raw.batch_execute(
        "create table public.src (id integer primary key, val text); \
         alter table public.src replica identity full",
    )
    .await
    .expect("create src");

    let client = TrellisClient::start(db.dsn(), live_options()).expect("client start");

    install_definition(
        &db.pool,
        "TRANSFORM agg FROM public.src GROUP BY val SELECT COUNT(*) AS n",
        &HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("val".to_string(), ValueType::Text),
        ]),
        "public",
    )
    .await
    .expect("install agg");
    wait_for_live(&raw, "agg").await;
    install_definition(
        &db.pool,
        "TRANSFORM hist FROM public.agg GROUP BY n SELECT COUNT(*) AS groups",
        &HashMap::from([
            ("val".to_string(), ValueType::Text),
            ("n".to_string(), ValueType::Numeric),
        ]),
        "public",
    )
    .await
    .expect("install hist");
    assert_only_src_is_published(&raw, &["agg", "hist"]).await;

    let hist = async || -> HashMap<String, Option<String>> {
        raw.query("select n::text, groups::text from hist", &[])
            .await
            .expect("read hist")
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect()
    };

    // agg: {a: 2 rows, b: 1 row} -> hist: {2: 1 group, 1: 1 group}.
    raw.execute(
        "insert into public.src (id, val) values (1, 'a'), (2, 'a'), (3, 'b')",
        &[],
    )
    .await
    .expect("insert into src");
    poll_until(
        &raw,
        Duration::from_secs(45),
        "hist never converged through the aggregate chain",
        async || {
            hist().await
                == HashMap::from([
                    ("2".to_string(), Some("1".to_string())),
                    ("1".to_string(), Some("1".to_string())),
                ])
        },
    )
    .await;

    // Move row 3 into group a: agg {a: 3 rows} (group b gone) -> hist
    // {3: 1 group}. agg's group a leaves hist group 2 for hist group 3, and
    // agg's group b leaves hist group 1 by going extinct: both old hist
    // groups are only reachable through each agg write's prior image.
    raw.execute("update public.src set val = 'a' where id = 3", &[])
        .await
        .expect("move row 3");
    poll_until(
        &raw,
        Duration::from_secs(45),
        "hist never followed agg's rows out of their old groups",
        async || hist().await == HashMap::from([("3".to_string(), Some("1".to_string()))]),
    )
    .await;

    // Delete a row: agg {a: 2 rows} -> hist {2: 1 group}.
    raw.execute("delete from public.src where id = 1", &[])
        .await
        .expect("delete row 1");
    poll_until(
        &raw,
        Duration::from_secs(45),
        "hist never followed agg's shrunken group",
        async || hist().await == HashMap::from([("2".to_string(), Some("1".to_string()))]),
    )
    .await;

    assert_no_stuck_segment(&raw).await;
    client.shutdown().await.expect("clean shutdown");
}
