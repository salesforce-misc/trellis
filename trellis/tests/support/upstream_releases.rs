//! The scenarios of `upstream_build_releases.rs`: a reader of a
//! Trellis target holding a key, and the upstream build whose completion
//! releases it.

#![allow(dead_code)]

use crate::drain_driver::{self, Driver};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;
use trellis::staging::apply;
use trellis::{Config, Trellis, TrellisOptions};

pub async fn pass(d: &mut Driver) {
    let pool = d.pool().clone();
    trellis::client::reconcile_pass(&mut d.ctl, &pool, DEFAULT_SCHEMA)
        .await
        .expect("reconcile pass");
}

pub async fn facade(dsn: &str) -> Trellis {
    let config = Config::with_schema(dsn.to_string(), DEFAULT_SCHEMA).expect("config");
    Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect")
}

pub async fn status_of(trellis: &Trellis, target: &str) -> String {
    let reported = trellis
        .status(target)
        .await
        .expect("status")
        .expect("the definition");
    let held = match &reported.held_keys {
        Some(held) => format!(" (held keys: {})", held.count),
        None => String::new(),
    };
    match reported.capture_failure {
        Some(failure) => format!("{:?}: {}{held}", reported.status, failure.error),
        None => format!("{:?}{held}", reported.status),
    }
}

/// Seals and drains eight times, tolerating a drain that fails on a held
/// key's value: each failure charges the key a death, so the page commits
/// once the key is held. A fixed number of rounds, not a wait.
/// Returns how many attempts failed.
pub async fn settle_tolerant(d: &mut Driver) -> usize {
    let mut failures = 0;
    for _ in 0..8 {
        let batch = d.seal().await;
        loop {
            match apply::drain_once(d.pool(), batch, "worker", 1, drain_driver::WAKE).await {
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(err) => {
                    failures += 1;
                    assert!(failures <= 40, "the batch never drained: {err}");
                }
            }
        }
        d.retire().await;
    }
    failures
}

/// Claims, runs and finishes every pending backfill chunk, then settles the
/// builds.
pub async fn run_backfill_chunks(pool: &trellis::Pool) {
    const CLAIMED_BY: &str = "upstream_release_backfill";
    trellis::staging::build::settle_builds(pool).await;
}

pub async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

pub fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

/// `up` (a 1-1 copy of `t`) and `r`, a 1-1 copy of `up` whose target
/// refuses a `g` of 100 or more, with key 2 of `up` held for `r` by a
/// failure no build of `up` repairs.
pub async fn persistently_held() -> (Driver, Trellis) {
    let mut d = Driver::start(
        "create table public.t (id integer primary key, g integer)",
        &[("id", int4()), ("g", int4())],
        &["TRANSFORM up FROM public.t SELECT g AS g"],
        &["public.t"],
    )
    .await;
    let trellis = facade(d.db.dsn()).await;
    trellis
        .apply("TRANSFORM r FROM public.up SELECT g AS g")
        .await
        .expect("define r");
    for _ in 0..3 {
        pass(&mut d).await;
        run_backfill_chunks(d.pool()).await;
        settle_tolerant(&mut d).await;
    }
    d.ctl
        .batch_execute("alter table public.r add constraint small check (g < 100)")
        .await
        .expect("constrain r");
    write(&d, "insert into public.t values (1, 1), (2, 200)").await;
    settle_tolerant(&mut d).await;
    assert_eq!(status_of(&trellis, "r").await, "Live (held keys: 1)");
    assert_eq!(r_retry_attempts(&d).await, vec!["(0)".to_string()]);
    (d, trellis)
}

/// The `retry_attempts` of each key `r` holds.
pub async fn r_retry_attempts(d: &Driver) -> Vec<String> {
    d.rows(
        "select p.retry_attempts::text from poison p \
         join transform_definitions d on d.id = p.transform_id \
         where d.target_table like '%.r' order by p.key",
    )
    .await
}

/// Rounds of (pass, builds, drain). A fixed number, not a wait.
pub async fn rounds(d: &mut Driver, n: usize) {
    for _ in 0..n {
        pass(d).await;
        run_backfill_chunks(d.pool()).await;
        settle_tolerant(d).await;
    }
}

/// A persistent failure of `r`, a 1-1 copy of an aggregate upstream, is
/// retried once by the build a `RESUME` of the upstream runs.
pub async fn persistent_failure_retried_once_per_aggregate_upstream_resume() {
    let mut d = Driver::start(
        "create table public.t (id integer primary key, g integer, x integer)",
        &[("id", int4()), ("g", int4()), ("x", int4())],
        &["TRANSFORM up FROM public.t GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, SUM(x) AS s"],
        &["public.t"],
    )
    .await;
    let trellis = facade(d.db.dsn()).await;
    trellis
        .apply("TRANSFORM r FROM public.up SELECT s AS s")
        .await
        .expect("define r");
    rounds(&mut d, 3).await;
    d.ctl
        .batch_execute("alter table public.r add constraint small check (s < 100)")
        .await
        .expect("constrain r");
    write(&d, "insert into public.t values (1, 1, 1), (2, 2, 200)").await;
    settle_tolerant(&mut d).await;
    assert_eq!(status_of(&trellis, "r").await, "Live (held keys: 1)");
    assert_eq!(r_retry_attempts(&d).await, vec!["(0)".to_string()]);

    trellis.apply("PAUSE TRANSFORM up").await.expect("pause up");
    trellis
        .apply("RESUME TRANSFORM up")
        .await
        .expect("resume up");
    rounds(&mut d, 4).await;
    assert_eq!(status_of(&trellis, "up").await, "Live");
    assert_eq!(status_of(&trellis, "r").await, "Live (held keys: 1)");
    assert_eq!(
        r_retry_attempts(&d).await,
        vec!["(1)".to_string()],
        "the resume's build retried r's key once"
    );
    trellis.shutdown().await.expect("shutdown");
}
