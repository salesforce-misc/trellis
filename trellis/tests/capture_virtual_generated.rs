//! Virtual generated columns (`GENERATED ALWAYS AS (...) VIRTUAL`, new in
//! PostgreSQL 18). A statement trigger's transition table holds no value
//! for one, so a capture function imaging it fails every write to the
//! table. Define refuses a definition that reads one, and a resume refuses
//! while the column is still virtual. A read column dropped and added again
//! as virtual, or another column renamed onto its name, pauses its readers
//! on the staging worker's next capture pass, which then stops imaging the
//! column so the application's writes succeed again. A definition on the
//! same table that doesn't read the column is unaffected throughout.
//!
//! Each test returns without running on a server older than 18, where no
//! column is virtual; the refusal and the pause are unit-tested over
//! catalog rows too (`defs::catalog`, `staging::schema_change`). Every pass,
//! build and drain is driven by hand; nothing polls or sleeps.

use std::time::{Duration, Instant};

use testkit::{TestCluster, TestDatabase};
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::TransformStatus;
use trellis::defs::join_one_to_one::set_enabled;
use trellis::staging::apply::ApplyError;
use trellis::staging::self_check::CheckPageExt;
use trellis::{
    CatalogError, Config, SelfCheckMode, SelfCheckOutcome, Trellis, TrellisError, TrellisOptions,
};

const WAKE: &str = "capture_virtual_generated_wake";

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public"))
        .await
        .expect("set search_path");
    client
}

struct Instance {
    db: TestDatabase,
    raw: Client,
    /// The application's connection.
    app: Client,
    trellis: Trellis,
}

/// A database with `public.bv` (`g` generated from `img`, `stored` or
/// `virtual` per `kind`) and `public.a`, which joins `bv` on `k`; `None`
/// when the server is older than PostgreSQL 18.
async fn instance(cluster: &TestCluster, kind: &str) -> Option<Instance> {
    let db = cluster.create_isolated_database().await;
    let raw = connect(db.dsn()).await;
    let version: i32 = raw
        .query_one("select current_setting('server_version_num')::int", &[])
        .await
        .expect("read the server version")
        .get(0);
    if version < 180_000 {
        eprintln!("skipped: virtual generated columns need PostgreSQL 18, the server is {version}");
        return None;
    }
    raw.batch_execute(&format!(
        "create table public.bv (bid int primary key, k int not null unique, grp int not null, \
             img int, g int generated always as (img * 2) {kind}); \
         create table public.a (id int primary key, k int, grp int not null, r int); \
         insert into public.bv (bid, k, grp, img) \
           select i, i, i % 2, i * 100 from generate_series(1, 6) i; \
         insert into public.a (id, k, grp, r) \
           select i, i, i % 3, i * 10 from generate_series(1, 6) i;"
    ))
    .await
    .expect("create the sources");
    let trellis = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a definer");
    let app = connect(db.dsn()).await;
    Some(Instance {
        db,
        raw,
        app,
        trellis,
    })
}

/// The capture half of one staging-worker pass, alone.
async fn capture_pass(raw: &mut Client, pool: &trellis::Pool) {
    let desired = trellis::defs::tables_to_capture(pool)
        .await
        .expect("read the tables to capture");
    let outcome = trellis::capture::reconcile::reconcile(
        raw,
        DEFAULT_SCHEMA,
        &desired,
        Instant::now() + Duration::from_secs(2),
    )
    .await
    .expect("capture pass");
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
}

async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    use trellis::staging::{apply, has_pending, retire_drained_segments, seal};
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, WAKE)
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "capture_virtual_generated_test",
            1,
            WAKE,
        )
        .await
        .expect("drain_once")
        .is_some()
        {}
        retire_drained_segments(client)
            .await
            .expect("retire drained segments");
        if !has_pending(client).await.expect("has_pending") {
            return;
        }
    }
    panic!("the ring did not reach quiescence within 16 seal/drain rounds");
}

/// `n` rounds of a whole staging-worker pass (capture, then the start of
/// ready builds), the builds run, and the ring drained.
async fn work(it: &mut Instance, n: usize) {
    for _ in 0..n {
        trellis::client::reconcile_pass(&mut it.raw, &it.db.pool, DEFAULT_SCHEMA)
            .await
            .expect("reconcile pass");
        trellis::staging::build::settle_builds(&it.db.pool).await;
        drain_to_quiescence(&it.db.pool, &mut it.raw).await;
    }
}

async fn status(trellis: &Trellis, target: &str) -> trellis::DefinitionStatus {
    trellis
        .status(target)
        .await
        .expect("status")
        .unwrap_or_else(|| panic!("{target} is registered"))
}

/// Asserts `target` is live, with no capture failure, and that `self_check`
/// finds it converged.
async fn assert_converged(trellis: &Trellis, target: &str) {
    let reported = status(trellis, target).await;
    assert_eq!(reported.status, TransformStatus::Live, "{target}");
    assert_eq!(reported.capture_failure, None, "{target}");
    let report = trellis
        .self_check_to_end(target, SelfCheckMode::Strict, Duration::from_secs(30))
        .await
        .unwrap_or_else(|e| panic!("self_check {target}: {e}"));
    assert!(
        matches!(report.outcome, SelfCheckOutcome::Converged),
        "{target}: {:?}",
        report.outcome
    );
}

/// Asserts `target` is paused for `bv.g` being virtual, with the remedy.
async fn assert_paused_for_virtual_g(trellis: &Trellis, target: &str) {
    let reported = status(trellis, target).await;
    assert_eq!(reported.status, TransformStatus::Paused, "{target}");
    let failure = reported
        .capture_failure
        .unwrap_or_else(|| panic!("{target}'s reason is reported"));
    assert_eq!(failure.source_table, "public.bv", "{failure:?}");
    assert_eq!(failure.columns, ["g"], "{failure:?}");
    assert!(
        failure.error.starts_with(
            "column \"g\" of public.bv is a virtual generated column (GENERATED ALWAYS AS \
             (img * 2) VIRTUAL)"
        ) && failure.error.contains("Make the column STORED and resume"),
        "{failure:?}"
    );
}

/// Asserts `err` is define's refusal of `public.bv.g`.
fn assert_refused_for_virtual_g(err: &CatalogError, what: &str) {
    assert!(
        matches!(
            err,
            CatalogError::VirtualGeneratedColumn { table, column, expression }
                if table == "public.bv" && column == "g" && expression == "(img * 2)"
        ),
        "{what}: {err:?}"
    );
}

/// The application writes to `bv` one row in, one changed and one out.
async fn app_writes(app: &Client, bid: i32) -> Result<(), tokio_postgres::Error> {
    app.batch_execute(&format!(
        "insert into public.bv (bid, k, grp, img) values ({bid}, {bid}, 1, {bid}00); \
         update public.bv set img = img + 1 where bid = 1; \
         delete from public.bv where bid = 2"
    ))
    .await
}

/// Define refuses a definition reading a virtual column wherever it reads
/// it: a 1-1 field, a `GROUP BY` key, an aggregate's argument, a `JOIN`'s
/// `ON` column, and a field over the joined table, in an aggregate and in
/// a 1-1 `JOIN`. Nothing is registered.
#[tokio::test]
async fn define_refuses_a_definition_reading_a_virtual_column() {
    set_enabled(true);
    let cluster = TestCluster::start();
    let Some(it) = instance(&cluster, "virtual").await else {
        return;
    };
    for text in [
        "TRANSFORM p FROM public.bv SELECT g AS g",
        "TRANSFORM sg FROM public.bv GROUP BY g WITHOUT SOURCE INDEX \
         SELECT g AS g, COUNT(*) AS c",
        "TRANSFORM s FROM public.bv GROUP BY grp WITHOUT SOURCE INDEX \
         SELECT grp AS grp, SUM(g) AS total",
        "TRANSFORM jon FROM public.a JOIN public.bv ON a.k = bv.g GROUP BY a.grp \
         WITHOUT SOURCE INDEX SELECT a.grp AS grp, COUNT(*) AS c",
        "TRANSFORM jf FROM public.a JOIN public.bv ON a.k = bv.k GROUP BY a.grp \
         WITHOUT SOURCE INDEX SELECT a.grp AS grp, SUM(bv.g) AS total",
        "TRANSFORM jb FROM public.a LEFT JOIN public.bv ON a.k = bv.k \
         SELECT a.r AS r, bv.g AS bg",
        "TRANSFORM jbf FROM public.bv JOIN public.a ON bv.g = a.k GROUP BY bv.grp \
         WITHOUT SOURCE INDEX SELECT bv.grp AS grp, COUNT(*) AS c",
    ] {
        let err = it.trellis.apply(text).await.expect_err(text);
        let TrellisError::Catalog(err) = &err else {
            panic!("{text}: {err:?}");
        };
        assert_refused_for_virtual_g(err, text);
    }
    let registered: i64 = it
        .raw
        .query_one("select count(*) from transform_definitions", &[])
        .await
        .expect("count definitions")
        .get(0);
    assert_eq!(registered, 0, "no refused definition is registered");
}

/// Definitions over the same table that don't read the virtual column
/// define, build, capture the application's writes and stay converged.
#[tokio::test]
async fn a_definition_not_reading_a_virtual_column_stays_converged() {
    set_enabled(true);
    let cluster = TestCluster::start();
    let Some(mut it) = instance(&cluster, "virtual").await else {
        return;
    };
    for text in [
        "TRANSFORM q FROM public.bv SELECT img AS img",
        "TRANSFORM t FROM public.bv GROUP BY grp WITHOUT SOURCE INDEX \
         SELECT grp AS grp, SUM(img) AS total",
        "TRANSFORM jq FROM public.a LEFT JOIN public.bv ON a.k = bv.k \
         SELECT a.r AS r, bv.img AS img",
    ] {
        it.trellis.apply(text).await.expect(text);
    }
    work(&mut it, 3).await;
    for bid in [7, 8] {
        app_writes(&it.app, bid)
            .await
            .expect("the application writes to bv");
        it.app
            .batch_execute("update public.bv set img = img * 2 where bid = 3")
            .await
            .expect("the application updates bv");
        work(&mut it, 1).await;
    }
    for target in ["q", "t", "jq"] {
        assert_converged(&it.trellis, target).await;
    }
}

/// A read column made virtual after define, by `ddl`: the installed capture
/// fails the application's writes until the next capture pass, which pauses
/// the column's readers with the reason. The pass after it stops imaging the
/// column, so the writes succeed again, and the definition that doesn't read
/// it stays converged throughout. A resume is refused while the column is
/// virtual, and rebuilds once it is stored again.
async fn a_read_column_made_virtual_pauses_its_readers(ddl: &str) {
    set_enabled(true);
    let cluster = TestCluster::start();
    let Some(mut it) = instance(&cluster, "stored").await else {
        return;
    };
    for text in [
        "TRANSFORM p FROM public.bv SELECT g AS g",
        "TRANSFORM s FROM public.bv GROUP BY grp WITHOUT SOURCE INDEX \
         SELECT grp AS grp, SUM(g) AS total",
        "TRANSFORM jb FROM public.a LEFT JOIN public.bv ON a.k = bv.k \
         SELECT a.r AS r, bv.g AS bg",
        "TRANSFORM q FROM public.bv SELECT img AS img",
    ] {
        it.trellis.apply(text).await.expect(text);
    }
    work(&mut it, 3).await;
    for target in ["p", "s", "jb", "q"] {
        assert_converged(&it.trellis, target).await;
    }

    it.app.batch_execute(ddl).await.expect(ddl);
    // The capture installed for `g` stored can't image it now.
    let err = app_writes(&it.app, 7)
        .await
        .expect_err("a write before the pass fails");
    assert!(
        err.as_db_error()
            .is_some_and(|e| e.message() == "unexpected virtual generated column reference"),
        "{err:?}"
    );

    capture_pass(&mut it.raw, &it.db.pool).await;
    for target in ["p", "s", "jb"] {
        assert_paused_for_virtual_g(&it.trellis, target).await;
    }
    let reported = status(&it.trellis, "q").await;
    assert_eq!(reported.status, TransformStatus::Live);
    assert_eq!(reported.capture_failure, None);

    // The next pass regenerates `bv`'s capture over `q`'s columns alone.
    capture_pass(&mut it.raw, &it.db.pool).await;
    app_writes(&it.app, 7)
        .await
        .expect("the application's writes succeed after the pause");
    work(&mut it, 1).await;
    assert_converged(&it.trellis, "q").await;
    for target in ["p", "s", "jb"] {
        assert_eq!(
            status(&it.trellis, target).await.status,
            TransformStatus::Paused,
            "a later pass leaves {target} paused"
        );
    }

    let err = it
        .trellis
        .apply("RESUME TRANSFORM p")
        .await
        .expect_err("a resume while g is virtual is refused");
    assert!(
        matches!(
            &err,
            TrellisError::Apply(ApplyError::ResumeRefused { reason, .. })
                if matches!(**reason, CatalogError::VirtualGeneratedColumn { .. })
        ),
        "{err:?}"
    );
    if let TrellisError::Apply(ApplyError::ResumeRefused { reason, .. }) = &err {
        assert_refused_for_virtual_g(reason, "RESUME TRANSFORM p");
    }

    // Stored again, every reader resumes to a rebuild from the source.
    it.app
        .batch_execute(
            "alter table public.bv drop column g; \
             alter table public.bv add column g int generated always as (img * 2) stored",
        )
        .await
        .expect("make g stored again");
    for target in ["p", "s", "jb"] {
        it.trellis
            .apply(&format!("RESUME TRANSFORM {target}"))
            .await
            .unwrap_or_else(|e| panic!("resume {target}: {e}"));
    }
    work(&mut it, 3).await;
    app_writes(&it.app, 8)
        .await
        .expect("the application writes to bv");
    work(&mut it, 1).await;
    for target in ["p", "s", "jb", "q"] {
        assert_converged(&it.trellis, target).await;
    }
}

#[tokio::test]
async fn a_read_column_dropped_and_added_again_as_virtual_pauses_its_readers() {
    a_read_column_made_virtual_pauses_its_readers(
        "alter table public.bv drop column g; \
         alter table public.bv add column g int generated always as (img * 2) virtual",
    )
    .await;
}

#[tokio::test]
async fn a_virtual_column_renamed_onto_a_read_column_pauses_its_readers() {
    a_read_column_made_virtual_pauses_its_readers(
        "alter table public.bv add column v int generated always as (img * 2) virtual; \
         alter table public.bv rename column g to g_stored; \
         alter table public.bv rename column v to g",
    )
    .await;
}
