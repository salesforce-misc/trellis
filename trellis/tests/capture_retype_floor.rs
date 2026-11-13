//! A retype of a column a definition reads re-validates it over the new
//! types (ADR-0024, "A retype re-stamps or pauses"): a definition the
//! validator still admits stays live with its floor stamped again, up or
//! down; one it refuses pauses, naming the refusal and the remedies, and
//! `RESUME` refuses until one of them holds.
//!
//! The cases are rows of #1640's false-positive study. A test stands in for
//! a later release by injecting the binary version and the release of the
//! identity integer cast (`Config::with_binary_version`,
//! `Config::with_feature_since`), so a re-stamp shows. Every pass is a call;
//! nothing waits for convergence.

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::defs::TransformStatus;
use trellis::{Config, Trellis, TrellisOptions};

#[path = "support/attver.rs"]
mod attver;

const SCHEMA: &str = "trellis";

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {SCHEMA}, public"))
        .await
        .expect("set search_path");
    client
}

/// A handle running as 0.3.0, in which the identity integer cast arrived.
async fn later_release(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string())
            .expect("valid dsn")
            .with_binary_version("0.3.0")
            .with_feature_since("TypeRule(Cast(IntegerIdentity))", "0.3.0"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect")
}

/// One staging-worker pass, with the handle's floors.
async fn pass(raw: &mut Client, trellis: &Trellis) {
    trellis::client::reconcile_pass(raw, trellis.pool(), SCHEMA)
        .await
        .expect("reconcile pass");
}

/// Runs every queued build to completion by hand, so the definitions are
/// live.
async fn build_everything(raw: &mut Client, trellis: &Trellis) {
    pass(raw, trellis).await;
    trellis::intake::harness::start_registered_builds(trellis.pool())
        .await
        .expect("dispatch registered builds");
    trellis::staging::build::settle_builds(trellis.pool()).await;
    pass(raw, trellis).await;
}

/// `target`'s status, stored floor and capture failure, if any.
async fn state(raw: &Client, target: &str) -> (TransformStatus, String, Option<String>) {
    let row = raw
        .query_one(
            "select d.status, d.floor_version, \
                    (select f.error from capture_failures f where f.transform_id = d.id) \
             from transform_definitions d where split_part(d.target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .expect("read the definition");
    (
        trellis::defs::StoredStatus::from_row(row.get(0))
            .expect("a status this release knows")
            .into(),
        row.get(1),
        row.get(2),
    )
}

/// i2, i18 and i21: a retype the validator still admits leaves the
/// definition live and stamps its floor again over the new type, which
/// moves it up (`CAST(c AS bigint)` over a `bigint` is the identity cast,
/// 0.3.0 here) or down (back over an `integer`, a widening cast again).
#[tokio::test]
async fn a_valid_retype_stays_live_and_restamps_the_floor() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.i2 (id int primary key, c int); \
         insert into public.i2 select i, i from generate_series(1, 10) i; \
         create table public.i18 (id int primary key, g int not null, c int); \
         insert into public.i18 select i, i % 3, i from generate_series(1, 10) i; \
         create table public.i21 (id int primary key, c bigint); \
         insert into public.i21 select i, i from generate_series(1, 10) i;",
    )
    .await
    .expect("sources");
    let trellis = later_release(db.dsn()).await;
    for text in [
        "TRANSFORM r_i2 FROM public.i2 SELECT CAST(c AS bigint) AS x",
        "TRANSFORM r_i18 FROM public.i18 GROUP BY g WITHOUT SOURCE INDEX \
         SELECT g AS g, SUM(CAST(c AS bigint)) AS total",
        "TRANSFORM r_i21 FROM public.i21 SELECT CAST(c AS bigint) AS x",
    ] {
        trellis.apply(text).await.expect(text);
    }
    build_everything(&mut raw, &trellis).await;
    for (target, floor) in [("r_i2", "0.2.0"), ("r_i18", "0.2.0"), ("r_i21", "0.3.0")] {
        assert_eq!(
            state(&raw, target).await,
            (TransformStatus::Live, floor.to_string(), None),
            "{target} before"
        );
    }

    raw.batch_execute(
        "alter table public.i2 alter column c type bigint; \
         alter table public.i18 alter column c type bigint; \
         alter table public.i21 alter column c type integer;",
    )
    .await
    .expect("retype");
    pass(&mut raw, &trellis).await;
    pass(&mut raw, &trellis).await;

    for (target, floor) in [("r_i2", "0.3.0"), ("r_i18", "0.3.0")] {
        assert_eq!(
            state(&raw, target).await,
            (TransformStatus::Live, floor.to_string(), None),
            "{target} after the pure widening"
        );
    }
    // `bigint` to `integer` is no pure widening: the reader rebuilds, and
    // its floor comes down.
    let (status, floor, failure) = state(&raw, "r_i21").await;
    assert_eq!((floor.as_str(), failure), ("0.2.0", None));
    assert_ne!(status, TransformStatus::Paused);
}

/// i4 and i7: a pure widening the validator no longer admits (a narrowing
/// `CAST` now, `date + bigint`) pauses the definition, naming the column,
/// both types, the refusal and the remedies; `RESUME` refuses until the
/// column is changed back.
#[tokio::test]
async fn an_invalid_retype_pauses_with_the_refusal_until_fixed() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.i4 (id int primary key, c smallint); \
         insert into public.i4 select i, i from generate_series(1, 10) i; \
         create table public.i7 (id int primary key, c int, d date); \
         insert into public.i7 select i, i, date '2024-01-01' from generate_series(1, 10) i;",
    )
    .await
    .expect("sources");
    let trellis = later_release(db.dsn()).await;
    for text in [
        "TRANSFORM r_i4 FROM public.i4 SELECT CAST(c AS integer) AS x",
        "TRANSFORM r_i7 FROM public.i7 SELECT d + c AS x",
    ] {
        trellis.apply(text).await.expect(text);
    }
    build_everything(&mut raw, &trellis).await;

    raw.batch_execute(
        "alter table public.i4 alter column c type bigint; \
         alter table public.i7 alter column c type bigint;",
    )
    .await
    .expect("retype");
    pass(&mut raw, &trellis).await;
    pass(&mut raw, &trellis).await;

    for (target, refusal, from) in [
        ("r_i4", "casts a bigint value to integer", "smallint"),
        (
            "r_i7",
            "date arithmetic takes a smallint or integer",
            "integer",
        ),
    ] {
        let (status, floor, failure) = state(&raw, target).await;
        assert_eq!((status, floor.as_str()), (TransformStatus::Paused, "0.2.0"));
        let failure = failure.expect("a capture failure");
        for part in [
            &format!("column \"c\" of public.{}", &target[2..]),
            &format!("changed type from {from} to bigint"),
            refusal,
            "ALTER TRANSFORM it to a definition that validates, or change the column back",
        ] {
            assert!(failure.contains(part), "{target}: {part:?} in {failure}");
        }
        let resumed = trellis.apply(&format!("RESUME TRANSFORM {target}")).await;
        assert!(resumed.is_err(), "{target}: resume refused while invalid");
    }

    // Changed back, the resume re-validates and rebuilds.
    raw.batch_execute("alter table public.i7 alter column c type integer")
        .await
        .expect("change it back");
    trellis
        .apply("RESUME TRANSFORM r_i7")
        .await
        .expect("resume once valid");
    assert_ne!(state(&raw, "r_i7").await.0, TransformStatus::Paused);
}

/// p5: a 1-1 key widened past what an expression reading it admits pauses
/// with the validator's refusal, not the copy check's promise that a resume
/// rebuilds it (a resume would refuse it).
#[tokio::test]
async fn a_key_retype_the_validator_refuses_names_the_refusal() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.p5 (id smallint primary key, d date); \
         insert into public.p5 select i, date '2024-01-01' from generate_series(1, 10) i;",
    )
    .await
    .expect("source");
    let trellis = later_release(db.dsn()).await;
    trellis
        .apply("TRANSFORM r_p5 FROM public.p5 SELECT d + id AS x")
        .await
        .expect("define");
    build_everything(&mut raw, &trellis).await;

    raw.batch_execute("alter table public.p5 alter column id type bigint")
        .await
        .expect("retype the key");
    pass(&mut raw, &trellis).await;
    pass(&mut raw, &trellis).await;

    let (status, _, failure) = state(&raw, "r_p5").await;
    assert_eq!(status, TransformStatus::Paused);
    let failure = failure.expect("a capture failure");
    for part in [
        "column \"id\" of public.p5 changed type from smallint to bigint",
        "date arithmetic takes a smallint or integer",
        "refuses until that is fixed",
    ] {
        assert!(failure.contains(part), "{part:?} in {failure}");
    }
    assert!(
        !failure.contains("the resume brings Trellis's copies to the new types"),
        "{failure}"
    );
    assert!(trellis.apply("RESUME TRANSFORM r_p5").await.is_err());
}

/// A handle running as 0.3.0 in which `bigint` arrived as a key type (the
/// join-key allowlist's entry, which also admits a primary key).
async fn bigint_key_release(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string())
            .expect("valid dsn")
            .with_binary_version("0.3.0")
            .with_feature_since(r#"JoinKey(Listed("bigint"))"#, "0.3.0"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect")
}

/// A retype of a source's primary-key column re-stamps the floor with the
/// entry that admits the new key type (#1660): an aggregate's source key
/// widened from `integer` to `bigint`, which no copy holds, doesn't pause,
/// and its floor rises to `bigint`'s release. (It rebuilds: the widening
/// rewrites the table, and PostgreSQL's 2026-11 minors re-version `g`, read
/// after `id`; `support/attver.rs`.)
#[tokio::test]
async fn a_primary_key_retype_restamps_the_floor_with_the_new_key_type() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.k1 (id int primary key, g int not null, c int); \
         insert into public.k1 select i, i % 3, i from generate_series(1, 10) i;",
    )
    .await
    .expect("source");
    let trellis = bigint_key_release(db.dsn()).await;
    trellis
        .apply(
            "TRANSFORM r_k1 FROM public.k1 GROUP BY g WITHOUT SOURCE INDEX \
             SELECT g AS g, COUNT(*) AS n",
        )
        .await
        .expect("define");
    build_everything(&mut raw, &trellis).await;
    assert_eq!(
        state(&raw, "r_k1").await,
        (TransformStatus::Live, "0.2.0".to_string(), None)
    );

    let retype = "alter table public.k1 alter column id type bigint";
    raw.batch_execute(retype).await.expect("retype the key");
    attver::Attver::probe(&raw).await.after(&raw, retype).await;
    pass(&mut raw, &trellis).await;
    pass(&mut raw, &trellis).await;
    assert_eq!(
        state(&raw, "r_k1").await,
        (TransformStatus::Backfilling, "0.3.0".to_string(), None)
    );
}

/// A retype of a 1-1 source's primary-key column to a type define refuses
/// as a key (`numeric`) pauses the definition through the key check, naming
/// the column and the new type, and leaves its floor as it was: the floor's
/// re-stamp over live key types never runs for it.
#[tokio::test]
async fn a_primary_key_retype_to_a_refused_type_pauses_and_keeps_the_floor() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.k2 (id int primary key, v text); \
         insert into public.k2 select i, i::text from generate_series(1, 10) i;",
    )
    .await
    .expect("source");
    let trellis = bigint_key_release(db.dsn()).await;
    trellis
        .apply("TRANSFORM r_k2 FROM public.k2 SELECT v AS v")
        .await
        .expect("define");
    build_everything(&mut raw, &trellis).await;

    raw.batch_execute("alter table public.k2 alter column id type numeric")
        .await
        .expect("retype the key");
    pass(&mut raw, &trellis).await;
    pass(&mut raw, &trellis).await;

    let (status, floor, failure) = state(&raw, "r_k2").await;
    assert_eq!((status, floor.as_str()), (TransformStatus::Paused, "0.2.0"));
    let failure = failure.expect("a capture failure");
    for part in [
        "column \"id\" of public.k2",
        "is now numeric, a type Trellis refuses as such a key at define",
    ] {
        assert!(failure.contains(part), "{part:?} in {failure}");
    }
    assert!(trellis.apply("RESUME TRANSFORM r_k2").await.is_err());
}
