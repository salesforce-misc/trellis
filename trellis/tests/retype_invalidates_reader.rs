//! A re-type of a 1-1 target's column that invalidates a definition reading
//! it (#1188): `ALTER TRANSFORM` refuses up front, and a `RESUME` that
//! re-types the column pauses the reader define would now refuse.
//!
//! The readers are group-basis aggregates and a `JOIN` (the group-basis dev
//! knob is on for this binary), whose `SUM`/`AVG` define refuses over a
//! numeric with no declared scale (#1096 decision 2), and a `JOIN` whose
//! key pair a re-type breaks. Every step is a bounded, explicit pass or
//! drain; nothing polls for convergence (#297). The one wait, in the
//! lock-order test, is for a backend to block on a lock.

use std::collections::HashMap;
use std::time::Duration;

use drain_driver::Driver;
use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::defs::{TransformStatus, ValueType};
use trellis::integer::IntWidth;
use trellis::{Config, Trellis, TrellisError, TrellisOptions};

#[path = "support/drain_driver.rs"]
mod drain_driver;

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

async fn definer(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a definer")
}

/// One whole staging-worker pass: capture, markers, discharge.
async fn full_pass(raw: &mut Client, pool: &trellis::Pool) {
    trellis::client::reconcile_pass(
        raw,
        pool,
        SCHEMA,
        "retype_invalidates_reader_wake",
        Duration::from_secs(2),
    )
    .await
    .expect("reconcile pass");
}

async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    use trellis::staging::{StagedWatermark, apply, has_pending, retire_drained_segments, seal};
    let watermark = StagedWatermark::saturated();
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(
            client,
            outcome.sealed_seg_seq,
            "retype_invalidates_reader_wake",
        )
        .await
        .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "retype_invalidates_reader_test",
            1,
            "trellis_retype_invalidates_reader_test",
            &watermark,
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

/// Claims, runs and finishes every pending backfill chunk.
async fn run_backfill_chunks(pool: &trellis::Pool) {
    use trellis::defs::chunk_queue;
    const CLAIMED_BY: &str = "retype_invalidates_reader_backfill";
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

async fn status(raw: &Client, target: &str) -> TransformStatus {
    let text: String = raw
        .query_one(
            "select status from transform_definitions where split_part(target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .expect("read status")
        .get(0);
    TransformStatus::from_persisted(&text).expect("known status")
}

/// Alternates staging-worker passes and drains until every one of
/// `targets` is live: a bounded number of explicit steps, not a timed wait.
async fn bring_live(raw: &mut Client, pool: &trellis::Pool, targets: &[&str]) {
    for _ in 0..8 {
        full_pass(raw, pool).await;
        run_backfill_chunks(pool).await;
        drain_to_quiescence(pool, raw).await;
        let mut all_live = true;
        for target in targets {
            all_live &= status(raw, target).await == TransformStatus::Live;
        }
        if all_live {
            return;
        }
    }
    for target in targets {
        eprintln!("{target}: {:?}", status(raw, target).await);
    }
    panic!("{targets:?} did not all go live within 8 pass/drain rounds");
}

/// `public.src (id, g, price numeric(10,2))`, empty, with `t` a passthrough of `price` as `p` and these readers of
/// `t.p`, all live:
///
/// - `roll`: `SUM(p)` per `g` (refused over a numeric with no scale);
/// - `cnt`: `COUNT(p)` per `g` (valid over any type);
/// - `c`: a 1-1 passthrough of `p` (valid; #828's copies are its business).
async fn setup(dsn: &str, raw: &mut Client, pool: &trellis::Pool) -> Trellis {
    trellis::dev::staging::set_group_basis(true);
    raw.batch_execute(
        "create table public.src (id int primary key, g int, price numeric(10,2)); \
        ",
    )
    .await
    .expect("tables");
    let trellis = definer(dsn).await;
    trellis
        .apply("TRANSFORM t FROM public.src SELECT g AS g, price AS p")
        .await
        .expect("define t");
    bring_live(raw, pool, &["t"]).await;
    for text in [
        "TRANSFORM roll FROM public.t GROUP BY g WITHOUT SOURCE INDEX SELECT SUM(p) AS total, COUNT(*) AS n",
        "TRANSFORM cnt FROM public.t GROUP BY g WITHOUT SOURCE INDEX SELECT COUNT(p) AS c",
        "TRANSFORM c FROM public.t SELECT p AS q",
    ] {
        trellis.apply(text).await.expect(text);
    }
    bring_live(raw, pool, &["t", "roll", "cnt", "c"]).await;
    trellis
}

async fn column_type(raw: &Client, table: &str, column: &str) -> String {
    raw.query_one(
        "select pg_catalog.format_type(atttypid, atttypmod) from pg_catalog.pg_attribute \
         where attrelid = pg_catalog.to_regclass($1) and attname = $2",
        &[&table, &column],
    )
    .await
    .unwrap_or_else(|err| panic!("{table}.{column}: {err}"))
    .get(0)
}

async fn definition_texts(raw: &Client) -> Vec<String> {
    raw.query(
        "select target_table || ' ' || definition_version || ' ' || definition_text \
         from transform_definitions order by id",
        &[],
    )
    .await
    .expect("read definitions")
    .into_iter()
    .map(|r| r.get(0))
    .collect()
}

async fn statuses(raw: &Client, targets: &[&str]) -> Vec<TransformStatus> {
    let mut all = Vec::new();
    for target in targets {
        all.push(status(raw, target).await);
    }
    all
}

/// Rule 1: the alter that would re-type `t.p` to `numeric` is refused,
/// naming each reader define would refuse, and changes and pauses nothing.
/// A reader that stays valid (`cnt`, `c`) isn't named. Without the check the
/// alter commits and `roll` stays live over a column define refuses.
#[tokio::test]
async fn an_alter_that_retypes_a_column_a_reader_cannot_read_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let before = definition_texts(&raw).await;

    let err = trellis
        .apply("ALTER TRANSFORM t ALTER p AS COALESCE(price, 0.5)")
        .await
        .expect_err("the alter is refused");
    assert!(
        matches!(
            err,
            TrellisError::Catalog(trellis::defs::CatalogError::UnsupportedAlter(_))
        ),
        "{err:?}"
    );
    let message = err.to_string();
    assert!(message.contains("'roll'"), "{message}");
    assert!(
        message.contains("no declared scale"),
        "the refusal is define's: {message}"
    );
    assert!(
        !message.contains("'cnt'") && !message.contains("'c'"),
        "{message}"
    );

    assert_eq!(column_type(&raw, "public.t", "p").await, "numeric(10,2)");
    assert_eq!(definition_texts(&raw).await, before, "nothing changed");
    assert_eq!(
        statuses(&raw, &["t", "roll", "cnt", "c"]).await,
        [TransformStatus::Live; 4],
        "nothing paused"
    );
    trellis.shutdown().await.expect("shutdown");
}

/// An alter whose readers all stay valid is accepted (rule 1 refuses only
/// what define would refuse).
#[tokio::test]
async fn an_alter_every_reader_survives_is_accepted() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    // Only the valid readers remain.
    for target in ["roll"] {
        raw.batch_execute(&format!(
            "update transform_definitions set status = 'paused' \
             where split_part(target_table, '.', 2) = '{target}'"
        ))
        .await
        .expect("pause");
        trellis
            .apply(&format!("DROP TRANSFORM {target}"))
            .await
            .expect("drop");
    }
    trellis
        .apply("ALTER TRANSFORM t ALTER p AS COALESCE(price, 0.5)")
        .await
        .expect("accepted");
    assert_eq!(column_type(&raw, "public.t", "p").await, "numeric");
    trellis.shutdown().await.expect("shutdown");
}

/// Rule 2: a `RESUME` that re-types `t.p` pauses `roll`, with
/// define's refusal as the reason naming the re-type, and its own
/// `RESUME` refuses. `cnt` and `c` stay valid and are not paused by it.
#[tokio::test]
async fn a_resume_that_retypes_a_column_pauses_the_readers_define_would_refuse() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;

    raw.batch_execute("alter table public.src alter column price type numeric")
        .await
        .expect("widen the source");
    full_pass(&mut raw, &db.pool).await;
    assert_eq!(status(&raw, "t").await, TransformStatus::Paused);
    trellis.apply("RESUME TRANSFORM t").await.expect("resume t");
    // `t` is rebuilt and live again before a reader's resume is judged.
    bring_live(&mut raw, &db.pool, &["t"]).await;

    assert_eq!(column_type(&raw, "public.t", "p").await, "numeric");
    for reader in ["roll"] {
        let reported = trellis.status(reader).await.expect("status").expect(reader);
        assert_eq!(reported.status, TransformStatus::Paused, "{reader}");
        let error = reported.capture_failure.expect("a reason").error;
        assert!(error.contains("no declared scale"), "{reader}: {error}");
        assert!(
            error.contains("from numeric(10,2) to numeric"),
            "{reader} names the upstream re-type: {error}"
        );
        match trellis.apply(&format!("RESUME TRANSFORM {reader}")).await {
            Err(TrellisError::Apply(trellis::staging::apply::ApplyError::ResumeRefused {
                ..
            })) => {}
            other => panic!("the resume of {reader} should be refused: {other:?}"),
        }
    }
    for reader in ["cnt", "c"] {
        let reported = trellis.status(reader).await.expect("status").expect(reader);
        assert!(
            reported
                .capture_failure
                .as_ref()
                .is_none_or(|f| !f.error.contains("no declared scale")),
            "{reader} isn't paused for a refusal: {reported:?}"
        );
    }
    trellis.shutdown().await.expect("shutdown");
}

/// Rule 1 for a `JOIN` reader: the alter is refused, naming the `JOIN`,
/// and the column keeps its type. Driven through the staging driver, which
/// stands a `JOIN` over a 1-1 target up without a staging worker.
#[tokio::test]
async fn an_alter_that_retypes_a_column_a_join_cannot_read_is_refused() {
    trellis::dev::staging::set_group_basis(true);
    let int4 = ValueType::Integer(IntWidth::Int4);
    let d = Driver::start(
        "create table public.src (id int primary key, g int, price numeric(10,2)); \
         create table public.o (id int primary key, g int)",
        &[("id", int4), ("g", int4), ("price", ValueType::Numeric)],
        &["TRANSFORM t FROM public.src SELECT g AS g, price AS p"],
        &["public.src", "public.o"],
    )
    .await;
    let columns: HashMap<String, ValueType> = [
        ("id".to_string(), int4),
        ("g".to_string(), int4),
        ("p".to_string(), ValueType::Numeric),
    ]
    .into();
    trellis::defs::install_definition(
        d.pool(),
        "TRANSFORM j FROM public.t JOIN public.o ON t.g = o.g GROUP BY t.g \
         WITHOUT SOURCE INDEX SELECT t.g AS g, SUM(t.p) AS s",
        &columns,
        "public",
    )
    .await
    .expect("define the JOIN");
    // The `JOIN` is registered, not built: a refusal doesn't depend on
    // its status.
    let trellis::defs::Statement::AlterTransform(alter) =
        trellis::defs::parse_statement("ALTER TRANSFORM t ALTER p AS COALESCE(price, 0.5)")
            .expect("parses")
    else {
        panic!("not an ALTER");
    };
    let err = trellis::defs::alter_transform(d.pool(), &alter)
        .await
        .expect_err("the alter is refused");
    let message = err.to_string();
    assert!(message.contains("'j'"), "{message}");
    assert!(message.contains("no declared scale"), "{message}");
    assert_eq!(
        d.rows(
            "select format_type(atttypid, atttypmod) from pg_attribute \
             where attrelid = 'public.t'::regclass and attname = 'p'"
        )
        .await,
        ["(\"numeric(10,2)\")"]
    );
}

/// Polls (bounded) until some backend waits on a lock `pid` holds: the
/// interleaving test's one wait for a lock-wait state, not for convergence.
async fn wait_blocked_behind(raw: &Client, pid: i32) {
    for _ in 0..600 {
        let blocked: bool = raw
            .query_one(
                "select exists (select 1 from pg_catalog.pg_stat_activity \
                 where $1 = any(pg_catalog.pg_blocking_pids(pid)))",
                &[&pid],
            )
            .await
            .expect("read lock waits")
            .get(0);
        if blocked {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("nothing blocked behind backend {pid} within 30s");
}

/// Rule 2's lock order: the re-type takes the readers' definition rows
/// before the target's `ACCESS EXCLUSIVE` lock. A transaction holding the
/// reader's row (`roll`'s, as its build or an `ALTER TRANSFORM roll` would)
/// and then reading the target `t` is not deadlocked by the resume's
/// re-type: the re-type waits on the row holding nothing on `t`, the
/// holder reads `t` and commits, and the same pass then re-types `t.p` and
/// pauses `roll`. Taking the rows after the re-type (holding `t`) deadlocks
/// here, and Postgres aborts one side.
#[tokio::test]
async fn a_resume_retype_takes_reader_rows_before_the_target_lock() {
    resume_retype_against_a_reader_row_holder(Duration::ZERO, false).await;
}

/// The same interleaving with the holder stalled past the re-type's
/// `lock_timeout` while the pass waits on `roll`'s row (what a box under load
/// does to the test above, #1281): the pass gives the re-type up for its next
/// one, and that one re-types `t.p`, pausing `roll`.
#[tokio::test]
async fn a_resume_retype_whose_row_wait_times_out_is_retried_by_the_next_pass() {
    // `staging::quarantine::RETYPE_LOCK_TIMEOUT` is 2 s. The test checks
    // that the first pass did time out, so a longer timeout fails it here
    // rather than leaving it to pass without a retry.
    resume_retype_against_a_reader_row_holder(Duration::from_millis(2500), true).await;
}

/// The last re-type failure recorded on `t` (empty if none).
async fn t_failure(raw: &Client) -> String {
    raw.query_one(
        "select coalesce(string_agg(f.error, ' | '), '') \
         from capture_failures f join transform_definitions d on d.id = f.transform_id \
         where split_part(d.target_table, '.', 2) = 't'",
        &[],
    )
    .await
    .expect("read t's capture_failure")
    .get(0)
}

async fn resume_retype_against_a_reader_row_holder(stall: Duration, expect_retry: bool) {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    raw.batch_execute("alter table public.src alter column price type numeric")
        .await
        .expect("widen the source");
    full_pass(&mut raw, &db.pool).await;
    assert_eq!(status(&raw, "t").await, TransformStatus::Paused);
    trellis.apply("RESUME TRANSFORM t").await.expect("resume t");

    let holder = connect(db.dsn()).await;
    let holder_pid: i32 = holder
        .query_one("select pg_catalog.pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0);
    holder
        .batch_execute(
            "begin; set local lock_timeout = '10s'; \
             select 1 from transform_definitions \
             where split_part(target_table, '.', 2) = 'roll' for update",
        )
        .await
        .expect("hold roll's definition row");

    let pool = db.pool.clone();
    let dsn = db.dsn().to_string();
    let pass = tokio::spawn(async move {
        let mut pass_client = connect(&dsn).await;
        full_pass(&mut pass_client, &pool).await;
    });
    // The pass's re-type of `t.p` now waits on `roll`'s row.
    wait_blocked_behind(&raw, holder_pid).await;
    tokio::time::sleep(stall).await;
    holder
        .batch_execute("select count(*) from public.t")
        .await
        .expect("the holder reads the target without a deadlock");
    holder.batch_execute("commit").await.expect("commit");
    pass.await.expect("the pass");
    if expect_retry {
        assert_eq!(
            column_type(&raw, "public.t", "p").await,
            "numeric(10,2)",
            "the stalled holder outlasted the re-type's lock_timeout: the first pass left it"
        );
        let failure = t_failure(&raw).await;
        assert!(failure.contains("lock timeout"), "{failure}");
    }

    // The re-type's wait on `roll`'s row is capped by its 2 s lock_timeout
    // (`RETYPE_LOCK_TIMEOUT`); a box so loaded that this task stalls past it
    // between the pass blocking and the holder committing makes the pass
    // leave the re-type for its next one (#1281), which is the engine's
    // contract, not the ordering bug. What the ordering bug produces is a
    // deadlock, which Postgres breaks by aborting a side: the holder (its
    // read above would have failed) or the pass, whose recorded last error
    // then says so. So: a bounded number of explicit further passes, each
    // only after a failure that was the lock timeout (a deadlock, or any
    // other failure, fails the test).
    for _ in 0..4 {
        if column_type(&raw, "public.t", "p").await == "numeric" {
            break;
        }
        let failure = t_failure(&raw).await;
        assert!(
            failure.contains("lock timeout") && !failure.contains("deadlock"),
            "the re-type failed other than by its lock_timeout (a deadlock is the ordering \
             bug): {failure}"
        );
        full_pass(&mut raw, &db.pool).await;
    }
    assert_eq!(
        column_type(&raw, "public.t", "p").await,
        "numeric",
        "t.p was re-typed within a few passes"
    );
    assert_eq!(status(&raw, "roll").await, TransformStatus::Paused);
    trellis.shutdown().await.expect("shutdown");
}

/// Rule 2 down a chain `t -> c -> roll2`: `t`'s resume re-types `t.p`,
/// which pauses `c` (its copy of `p` is outgrown, #828); `c`'s chained
/// resume re-types `c.q`, which pauses `roll2`, a `SUM(q)` define would now
/// refuse. `c` goes live again.
#[tokio::test]
async fn a_chained_resume_retype_pauses_the_reader_define_would_refuse() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    trellis::dev::staging::set_group_basis(true);
    raw.batch_execute("create table public.src (id int primary key, g int, price numeric(10,2))")
        .await
        .expect("tables");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply("TRANSFORM t FROM public.src SELECT g AS g, price AS p")
        .await
        .expect("define t");
    bring_live(&mut raw, &db.pool, &["t"]).await;
    trellis
        .apply("TRANSFORM c FROM public.t SELECT g AS g, p AS q")
        .await
        .expect("define c");
    bring_live(&mut raw, &db.pool, &["t", "c"]).await;
    trellis
        .apply(
            "TRANSFORM roll2 FROM public.c GROUP BY g WITHOUT SOURCE INDEX \
             SELECT SUM(q) AS total",
        )
        .await
        .expect("define roll2");
    bring_live(&mut raw, &db.pool, &["t", "c", "roll2"]).await;

    raw.batch_execute("alter table public.src alter column price type numeric")
        .await
        .expect("widen the source");
    full_pass(&mut raw, &db.pool).await;
    assert_eq!(status(&raw, "t").await, TransformStatus::Paused);
    trellis.apply("RESUME TRANSFORM t").await.expect("resume t");
    bring_live(&mut raw, &db.pool, &["t", "c"]).await;

    assert_eq!(column_type(&raw, "public.c", "q").await, "numeric");
    let reported = trellis
        .status("roll2")
        .await
        .expect("status")
        .expect("roll2");
    assert_eq!(reported.status, TransformStatus::Paused);
    let error = reported.capture_failure.expect("a reason").error;
    assert!(error.contains("no declared scale"), "{error}");
    assert!(
        error.contains("public.c.q from numeric(10,2) to numeric"),
        "{error}"
    );
    trellis.shutdown().await.expect("shutdown");
}

/// The captured-source path: a source column a group-basis `SUM` and a
/// `JOIN`'s `SUM` read is re-typed to an unconstrained `numeric`, and the
/// capture pass's re-validation (#703 R1) pauses both with define's refusal
/// (#1096 decision 2). For the plain aggregate only `revalidate`'s
/// group-basis check refuses.
#[tokio::test]
async fn a_source_retype_to_unscaled_numeric_pauses_its_sum_readers() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    trellis::dev::staging::set_group_basis(true);
    raw.batch_execute(
        "create table public.src (id int primary key, g int, price numeric(10,2)); \
         create table public.o (id int primary key, g int)",
    )
    .await
    .expect("tables");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply(
            "TRANSFORM agg FROM public.src GROUP BY g WITHOUT SOURCE INDEX \
             SELECT SUM(price) AS total",
        )
        .await
        .expect("define agg");
    trellis
        .apply(
            "TRANSFORM j FROM public.src JOIN public.o ON src.g = o.g GROUP BY src.g \
             WITHOUT SOURCE INDEX SELECT src.g AS g, SUM(src.price) AS s",
        )
        .await
        .expect("define j");
    bring_live(&mut raw, &db.pool, &["agg", "j"]).await;

    raw.batch_execute("alter table public.src alter column price type numeric")
        .await
        .expect("widen the source");
    full_pass(&mut raw, &db.pool).await;

    for reader in ["agg", "j"] {
        let reported = trellis.status(reader).await.expect("status").expect(reader);
        assert_eq!(reported.status, TransformStatus::Paused, "{reported:?}");
        let error = reported.capture_failure.expect("a reason").error;
        assert!(error.contains("no declared scale"), "{reader}: {error}");
    }
    trellis.shutdown().await.expect("shutdown");
}

/// Rules 1 and 4: an alter that changes a column's type within the string
/// family leaves a `JOIN` key pair `t.k = o.k` valid (Trellis's copy of a
/// `varchar(n)` column is unbounded, #1207, and `varchar` and `text` pair),
/// so the alter is accepted and the `JOIN` is untouched.
#[tokio::test]
async fn an_alter_that_keeps_a_join_key_pair_valid_is_accepted() {
    trellis::dev::staging::set_group_basis(true);
    let int4 = ValueType::Integer(IntWidth::Int4);
    let d = Driver::start(
        "create table public.src (id int primary key, g int, name varchar(10)); \
         create table public.o (id int primary key, k varchar)",
        &[("id", int4), ("g", int4), ("name", ValueType::Text)],
        &["TRANSFORM t FROM public.src SELECT g AS g, name AS k"],
        &["public.src", "public.o"],
    )
    .await;
    let columns: HashMap<String, ValueType> = [
        ("id".to_string(), int4),
        ("g".to_string(), int4),
        ("k".to_string(), ValueType::Text),
    ]
    .into();
    trellis::defs::install_definition(
        d.pool(),
        "TRANSFORM j FROM public.t JOIN public.o ON t.k = o.k GROUP BY t.g \
         WITHOUT SOURCE INDEX SELECT t.g AS g, COUNT(*) AS n",
        &columns,
        "public",
    )
    .await
    .expect("define the JOIN");
    let trellis::defs::Statement::AlterTransform(alter) =
        trellis::defs::parse_statement("ALTER TRANSFORM t ALTER k AS COALESCE(name, 'x')")
            .expect("parses")
    else {
        panic!("not an ALTER");
    };
    trellis::defs::alter_transform(d.pool(), &alter)
        .await
        .expect("the alter keeps the key pair valid");
    assert_eq!(
        d.rows("select status from transform_definitions where target_table = 'public.j'")
            .await,
        ["(waiting_to_backfill)"],
        "the JOIN is untouched"
    );
}
