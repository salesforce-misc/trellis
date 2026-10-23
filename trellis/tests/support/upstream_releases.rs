//! The scenarios of `upstream_build_releases.rs` and
//! `upstream_build_releases_ledger.rs` (#1306): a reader of a Trellis
//! target holding a key, and the upstream build whose completion releases
//! it. The group-basis knob is process-wide, so each test binary runs with
//! one setting only.

#![allow(dead_code)]

use std::time::Duration;

use crate::drain_driver::{self, Driver};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{PgType, ValueType, chunk_queue};
use trellis::integer::IntWidth;
use trellis::staging::{StagedWatermark, apply};
use trellis::{Config, Trellis, TrellisOptions};

pub async fn pass(d: &mut Driver) {
    let pool = d.pool().clone();
    trellis::client::reconcile_pass(
        &mut d.ctl,
        &pool,
        DEFAULT_SCHEMA,
        "upstream_release_wake",
        Duration::from_secs(2),
    )
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
/// once the key is held. A fixed number of rounds, not a wait (#297).
/// Returns how many attempts failed.
pub async fn settle_tolerant(d: &mut Driver) -> usize {
    let mut failures = 0;
    for _ in 0..8 {
        let batch = d.seal().await;
        loop {
            match apply::drain_once(
                d.pool(),
                batch,
                "worker",
                1,
                drain_driver::WAKE,
                &StagedWatermark::saturated(),
            )
            .await
            {
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
    loop {
        let client = pool.get().await.expect("acquire connection");
        let claimed = chunk_queue::claim_chunks(&**client, CLAIMED_BY, 1000)
            .await
            .expect("claim_chunks");
        drop(client);
        if claimed.is_empty() {
            trellis::staging::build::settle_builds(pool).await;
            return;
        }
        for chunk in &claimed {
            chunk_queue::run_claimed_chunk(
                pool,
                chunk,
                CLAIMED_BY,
                Duration::from_secs(5),
                Duration::from_secs(60),
            )
            .await
            .expect("run_claimed_chunk");
            chunk_queue::finish_chunk(pool, chunk, CLAIMED_BY)
                .await
                .expect("finish_chunk");
        }
    }
}

pub async fn write(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

pub fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

pub const AGG: &str = "select g::text, a::text, o::text from public.agg";
pub const ORACLE: &str =
    "select g::text, bit_and(v)::text, bit_or(v)::text from public.t group by g";

pub async fn diverged(d: &Driver) -> Vec<String> {
    d.rows(&format!(
        "(({AGG}) except ({ORACLE})) union all (({ORACLE}) except ({AGG}))"
    ))
    .await
}

/// `up` (a 1-1 copy of `t`'s `bit(5)`) and `agg`, `BIT_AND`/`BIT_OR` over
/// `up`, seeded and settled.
pub async fn chained_bit_aggregates(group_basis: bool) -> (Driver, Trellis) {
    trellis::dev::staging::set_group_basis(group_basis);
    let mut d = Driver::start(
        "create table public.t (id integer primary key, g integer, v bit(5))",
        &[
            ("id", int4()),
            ("g", int4()),
            ("v", ValueType::Other(PgType::Bit)),
        ],
        &["TRANSFORM up FROM public.t SELECT g AS g, v AS v"],
        &["public.t"],
    )
    .await;
    let trellis = facade(d.db.dsn()).await;
    trellis
        .apply(
            "TRANSFORM agg FROM public.up GROUP BY g WITHOUT SOURCE INDEX \
             SELECT g AS g, BIT_AND(v) AS a, BIT_OR(v) AS o",
        )
        .await
        .expect("define agg");
    for _ in 0..3 {
        pass(&mut d).await;
        run_backfill_chunks(d.pool()).await;
        settle_tolerant(&mut d).await;
    }
    write(
        &d,
        "insert into public.t values (1, 1, '10101'), (2, 1, '11100'), (3, 2, '00011')",
    )
    .await;
    settle_tolerant(&mut d).await;
    assert_eq!(status_of(&trellis, "agg").await, "Live");
    assert_eq!(diverged(&d).await, Vec::<String>::new(), "after the seed");
    (d, trellis)
}

/// Rewrites `t.v` to `bit(8)` and writes a row the rewrite's old width
/// can't AND with, then settles *before* the pass sees the rewrite, the
/// order production runs in: `up` then holds 5- and 8-bit values together,
/// and `agg`'s group 1 fails until its key is held.
pub async fn rewrite_then_settle(d: &mut Driver, trellis: &Trellis) {
    d.ctl
        .batch_execute("alter table public.t alter column v type bit(8) using v::bit(8)")
        .await
        .expect("alter");
    write(d, "insert into public.t values (4, 1, '11111111')").await;
    let failures = settle_tolerant(d).await;
    assert!(failures > 0, "agg's drain never failed on mixed widths");
    assert_eq!(status_of(trellis, "agg").await, "Live (held keys: 1)");
}

/// Rounds of (pass, builds, drain), resuming `up` if it pauses with a
/// reason. A fixed number of rounds, not a wait.
pub async fn rebuild_upstream(d: &mut Driver, trellis: &Trellis) {
    for _ in 0..8 {
        pass(d).await;
        run_backfill_chunks(d.pool()).await;
        settle_tolerant(d).await;
        let status = status_of(trellis, "up").await;
        if status
            .strip_prefix("Paused: ")
            .is_some_and(|why| !why.starts_with("resuming:"))
        {
            trellis
                .apply("RESUME TRANSFORM up")
                .await
                .expect("resume up");
        }
    }
}

pub async fn chained_bit_aggregates_converge_after_upstream_rebuild(group_basis: bool) {
    let (mut d, trellis) = chained_bit_aggregates(group_basis).await;
    rewrite_then_settle(&mut d, &trellis).await;
    rebuild_upstream(&mut d, &trellis).await;
    assert_eq!(status_of(&trellis, "up").await, "Live");
    assert_eq!(
        status_of(&trellis, "agg").await,
        "Live",
        "group basis {group_basis}: agg still holds a key after up's rebuild"
    );
    assert_eq!(
        diverged(&d).await,
        Vec::<String>::new(),
        "group basis {group_basis}"
    );
    trellis.shutdown().await.expect("shutdown");
}

/// `up` (a 1-1 copy of `t`) and `r`, a 1-1 copy of `up` whose target
/// refuses a `g` of 100 or more, with key 2 of `up` held for `r` by a
/// failure no build of `up` repairs.
pub async fn persistently_held(group_basis: bool) -> (Driver, Trellis) {
    trellis::dev::staging::set_group_basis(group_basis);
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
pub async fn persistent_failure_retried_once_per_aggregate_upstream_resume(group_basis: bool) {
    trellis::dev::staging::set_group_basis(group_basis);
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
