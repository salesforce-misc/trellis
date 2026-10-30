//! A row wider than 50 columns: Postgres caps a function call at 100
//! arguments (SQLSTATE 54023), so a row image built by one
//! `jsonb_build_object('<col>', <col>::text, ...)` call fails for a source or
//! a target past 50 columns (#1493). Every image builder joins chunks of at
//! most 50 pairs instead.
//!
//! 120 columns crosses a second chunk boundary (50 + 50 + 20). Every test
//! drives the pipeline by hand through `support/drain_driver.rs`; nothing
//! waits for convergence.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use std::collections::HashMap;
use std::time::Duration;

use drain_driver::Driver;
use trellis::config::Config;
use trellis::defs::chunk_queue;
use trellis::defs::{ValueType, install_definition};
use trellis::integer::IntWidth;
use trellis::staging::build::{self, WorkerOptions};
use trellis::staging::self_check::{
    SelfCheckMode, SelfCheckOutcome, SelfCheckScope, check_page, compare_page,
};
use trellis::{Trellis, TrellisOptions};

const WIDTH: usize = 120;

const OPTIONS: WorkerOptions = WorkerOptions {
    chunk_rows: 1_000,
    drain_batch_cap: usize::MAX,
    heartbeat_interval: Duration::from_secs(5),
    reclaim_ttl: Duration::from_secs(60),
};

/// A rebuild of `src`'s targets, run by hand.
async fn rebuild(d: &Driver, src: &str) {
    let trellis = Trellis::connect(
        Config::from_dsn(d.db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a Trellis");
    trellis
        .request_backfill(src)
        .await
        .expect("request a rebuild");
    d.run_frozen(&[], |pool| async move {
        let claimed = chunk_queue::claim_chunks_of(
            &**pool.get().await.expect("pool"),
            "rebuild",
            1,
            &[chunk_queue::KIND_SWEEP],
        )
        .await
        .expect("claim the rebuild");
        assert_eq!(claimed.len(), 1, "one rebuild job to claim");
        build::run_claimed(&pool, &claimed[0], "rebuild", &OPTIONS).await;
        Ok(())
    })
    .await
    .finish()
    .await;
}

/// The aggregate's `self_check`, one page.
async fn assert_aggregate_self_check_clean(d: &Driver, target: &str) {
    let page = check_page(
        d.pool(),
        target,
        SelfCheckScope {
            after: None,
            limit: 100,
        },
        SelfCheckMode::Standard,
        Duration::from_secs(30),
    )
    .await
    .expect("a page of the check");
    assert!(
        matches!(page.outcome, SelfCheckOutcome::Converged),
        "self_check: {:?}",
        page.outcome
    );
}

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

fn cols() -> Vec<String> {
    (0..WIDTH).map(|i| format!("c{i}")).collect()
}

fn columns() -> Vec<(String, ValueType)> {
    let mut columns = vec![("id".to_string(), int4())];
    columns.extend(cols().into_iter().map(|c| (c, int4())));
    // The aggregate's target columns, which a downstream transform reads.
    columns.extend((0..60).map(|i| (format!("s{i}"), ValueType::Integer(IntWidth::Int8))));
    columns
}

/// A row `(id, id*1000 + 0, id*1000 + 1, ...)` plus `bump` on every value.
fn row(id: i32, bump: i32) -> String {
    let values: Vec<String> = (0..WIDTH as i32)
        .map(|i| (id * 1000 + i + bump).to_string())
        .collect();
    format!("({id}, {})", values.join(", "))
}

fn select_all() -> String {
    cols()
        .iter()
        .map(|c| format!("{c} AS {c}"))
        .collect::<Vec<_>>()
        .join(", ")
}

async fn start(transforms: &[String], seed: &str) -> Driver {
    let ddl = format!(
        "create table public.w (id integer primary key, {}){seed}",
        cols()
            .iter()
            .map(|c| format!("{c} integer"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let columns = columns();
    let columns: Vec<(&str, ValueType)> = columns.iter().map(|(n, t)| (n.as_str(), *t)).collect();
    let transforms: Vec<&str> = transforms.iter().map(String::as_str).collect();
    Driver::start(&ddl, &columns, &transforms, &["public.w"]).await
}

async fn step(d: &mut Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
    let batch = d.seal().await;
    d.drain(batch, "step").await;
}

/// A Re-derive (an image-less change) of each of `keys`, drained.
async fn re_derive(d: &mut Driver, keys: &[&str]) {
    d.stage_recomputes("public.w", keys).await;
    let batch = d.seal().await;
    d.drain(batch, "re-derive").await;
}

async fn assert_self_check_clean(d: &Driver, target: &str) {
    let page = compare_page(
        d.pool(),
        target,
        SelfCheckScope {
            after: None,
            limit: 100,
        },
    )
    .await
    .expect("a comparison");
    assert_eq!(page.divergences, [], "{target}");
    assert!(
        page.uncertain_keys.is_empty() && !page.uncertain_all,
        "{target}"
    );
}

/// A 1-1 transform over a source of 120 columns, once narrow and once as wide
/// as its source: inserts, updates and deletes apply, a Re-derive of a live
/// key and of a deleted key (the live re-read), and the target images the
/// writes read back (`target_mutations`, `pool.rs`) all build their objects
/// from chunks.
#[tokio::test]
async fn a_one_to_one_over_a_source_and_target_wider_than_50_columns_applies_and_re_derives() {
    let mut d = start(
        &[
            "TRANSFORM narrow FROM public.w SELECT c0 AS c0, c119 AS c119".to_string(),
            format!("TRANSFORM wide FROM public.w SELECT {}", select_all()),
        ],
        "",
    )
    .await;
    let wide_oracle = format!("select id, {} from public.w order by id", cols().join(", "));
    let wide_target = format!(
        "select id, {} from public.wide order by id",
        cols().join(", ")
    );
    let narrow_oracle = "select id, c0, c119 from public.w order by id";
    let narrow_target = "select id, c0, c119 from public.narrow order by id";

    step(
        &mut d,
        &format!("insert into public.w values {}, {}", row(1, 0), row(2, 0)),
    )
    .await;
    assert_eq!(d.rows(&wide_target).await, d.rows(&wide_oracle).await);
    assert_eq!(d.rows(narrow_target).await, d.rows(narrow_oracle).await);

    step(
        &mut d,
        "update public.w set c0 = -1, c60 = -2, c119 = -3 where id = 1",
    )
    .await;
    assert_eq!(d.rows(&wide_target).await, d.rows(&wide_oracle).await);
    assert_eq!(d.rows(narrow_target).await, d.rows(narrow_oracle).await);

    // The Re-derive re-reads the live row: every source column, past the
    // first chunk. A key that is gone re-derives to a delete.
    re_derive(&mut d, &["1", "2"]).await;
    step(&mut d, "delete from public.w where id = 2").await;
    re_derive(&mut d, &["2"]).await;
    assert_eq!(d.rows(&wide_target).await, d.rows(&wide_oracle).await);
    assert_eq!(d.rows(narrow_target).await, d.rows(narrow_oracle).await);
    assert_eq!(d.rows(&wide_target).await.len(), 1);

    // An update whose Re-derive has to change the target: the source row
    // changes under capture, then a Re-derive of the key lands the live row.
    step(&mut d, "update public.w set c100 = 7 where id = 1").await;
    re_derive(&mut d, &["1"]).await;
    assert_eq!(d.rows(&wide_target).await, d.rows(&wide_oracle).await);

    assert_self_check_clean(&d, "wide").await;
    assert_self_check_clean(&d, "narrow").await;
}

/// An aggregate with 60 fields (a target past 50 columns), over a source of
/// 120 columns, builds, applies and re-derives. A 1-1 transform reads the
/// aggregate's target, so the aggregate's writes image its prior rows
/// (`group_basis`' `prior_image`) over all 61 columns.
#[tokio::test]
async fn an_aggregate_with_a_target_wider_than_50_columns_applies_and_re_derives() {
    let fields: Vec<String> = (0..60).map(|i| format!("SUM(c{i}) AS s{i}")).collect();
    let copied: Vec<String> = (1..60).map(|i| format!("s{i} AS s{i}")).collect();
    let mut d = start(
        &[format!(
            "TRANSFORM agg FROM public.w GROUP BY c119 WITHOUT SOURCE INDEX \
             SELECT c119 AS c119, {}",
            fields.join(", ")
        )],
        &format!(
            "; insert into public.w values {}, {}, {}",
            row(1, 0),
            row(2, 0),
            row(3, 0)
        ),
    )
    .await;
    // The aggregate is live before a transform can read its target.
    let columns: HashMap<String, ValueType> = columns().into_iter().collect();
    install_definition(
        d.pool(),
        &format!(
            "TRANSFORM downstream FROM public.agg SELECT s0 AS s0, {}",
            copied.join(", ")
        ),
        &columns,
        "public",
    )
    .await
    .expect("install the downstream transform");
    trellis::intake::harness::settle_builds(d.pool()).await;
    let sums: Vec<String> = (0..60).map(|i| format!("sum(c{i})")).collect();
    let names: Vec<String> = (0..60).map(|i| format!("s{i}")).collect();
    let oracle = format!(
        "select c119, {} from public.w group by c119 order by c119",
        sums.join(", ")
    );
    let target = format!(
        "select c119, {} from public.agg order by c119",
        names.join(", ")
    );

    assert_eq!(d.rows(&target).await, d.rows(&oracle).await);
    step(&mut d, "update public.w set c119 = 5000 where id = 1").await;
    step(&mut d, "update public.w set c3 = c3 + 11 where id = 2").await;
    step(&mut d, "delete from public.w where id = 3").await;
    re_derive(&mut d, &["1", "2", "3"]).await;
    assert_eq!(d.rows(&target).await, d.rows(&oracle).await);
    let downstream = format!(
        "select c119, {} from public.downstream order by c119",
        names.join(", ")
    );
    assert_eq!(d.rows(&downstream).await, d.rows(&oracle).await);
    assert_aggregate_self_check_clean(&d, "agg").await;
    rebuild(&d, "w").await;
    assert_eq!(d.rows(&target).await, d.rows(&oracle).await);
    assert_eq!(d.rows(&downstream).await, d.rows(&oracle).await);
}
