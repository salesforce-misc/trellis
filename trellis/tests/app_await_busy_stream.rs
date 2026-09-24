//! Issue #469: `Trellis::await_converged` must converge while unrelated
//! writers keep the pipeline busy.
//!
//! `converged_through` gates a token on ring rows whose `origin_lsn` is at or
//! below it, and treats a missing origin as older than any token. Intake used
//! to stage every row without an origin, and the rows a drain derives for
//! downstream transforms carried none either, so under a steady stream of
//! unrelated writes every token waited on work committed after it and the
//! wait never returned. Rows now carry the commit they trace back to, hop by
//! hop.
//!
//! The busy chain here covers both propagation paths a derived row can take:
//! `noise -> noise_prices` (1-1) `-> noise_totals` (aggregate) `->
//! noise_tally` (1-1 over the aggregate's target). Each wait gets a 10s
//! budget; it converges in well under a second, and without the fix it never
//! converges at all while the writer runs.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use testkit::TestCluster;
use trellis::defs::TransformStatus;
use trellis::{Config, Trellis, TrellisOptions};

const BUDGET: Duration = Duration::from_secs(10);

async fn wait_live(trellis: &Trellis, target: &str) {
    for _ in 0..1_500 {
        let status = trellis.status(target).await.expect("status");
        if status.is_some_and(|s| s.status == TransformStatus::Live) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("{target} never went live");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wait_converges_while_an_unrelated_chain_stays_busy() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let config = Config::with_schema(db.dsn().to_string(), "trellis").expect("valid schema");
    let pool = trellis::Pool::new(&config).expect("pool");
    trellis::migrate(&pool, &config).await.expect("migrate");
    let conn = pool.get().await.expect("connection");
    conn.batch_execute(
        "create table widgets (id integer primary key, price integer); \
         create table noise (id serial primary key, price integer)",
    )
    .await
    .expect("create tables");

    let definer = Trellis::connect(config.clone(), TrellisOptions::default())
        .await
        .expect("connect definer");
    definer
        .apply("TRANSFORM widget_prices FROM widgets SELECT price AS price")
        .await
        .expect("define widget_prices");
    definer
        .apply("TRANSFORM noise_prices FROM noise SELECT price AS price")
        .await
        .expect("define noise_prices");
    definer.shutdown().await.expect("shutdown definer");

    let trellis = Trellis::connect(
        config,
        TrellisOptions {
            staging: true,
            drain_threads: 2,
            ..Default::default()
        },
    )
    .await
    .expect("connect staging instance");
    wait_live(&trellis, "widget_prices").await;
    wait_live(&trellis, "noise_prices").await;
    // Chained definitions need their upstream live before they register.
    trellis
        .apply("TRANSFORM noise_totals FROM noise_prices GROUP BY price SELECT count(*) AS n")
        .await
        .expect("define noise_totals");
    wait_live(&trellis, "noise_totals").await;
    trellis
        .apply("TRANSFORM noise_tally FROM noise_totals SELECT n AS n")
        .await
        .expect("define noise_tally");
    wait_live(&trellis, "noise_tally").await;

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let stop = stop.clone();
        let pool = pool.clone();
        tokio::spawn(async move {
            let conn = pool.get().await.expect("writer connection");
            while !stop.load(Ordering::Relaxed) {
                conn.execute("insert into noise (price) values (1)", &[])
                    .await
                    .expect("insert noise");
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
    };
    // Let the busy chain get going before the first wait.
    tokio::time::sleep(Duration::from_millis(500)).await;

    for id in 1..=3 {
        conn.execute("insert into widgets values ($1, $1)", &[&id])
            .await
            .expect("insert widget");
        let token = trellis.watermark_token().await.expect("watermark_token");
        let converged = trellis.await_converged(token, BUDGET).await;
        if converged.is_err() {
            stop.store(true, Ordering::Relaxed);
        }
        converged.expect("converges while the unrelated chain stays busy");
        let price: Option<i32> = conn
            .query_opt(
                "select price from public.widget_prices where id = $1",
                &[&id],
            )
            .await
            .expect("read target")
            .map(|row| row.get(0));
        assert_eq!(price, Some(id));
    }

    stop.store(true, Ordering::Relaxed);
    writer.await.expect("writer task");
    trellis.shutdown().await.expect("shutdown");
}
