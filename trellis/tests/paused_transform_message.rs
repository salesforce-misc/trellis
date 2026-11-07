//! A paused definition's message: every reason it is paused for, and what
//! `RESUME` would do now, kept current by the staging worker's capture
//! pass when a relation it reads changes (`staging::pause_message`).
//!
//! Every test is stepped by hand: schema changes from a separate session,
//! then explicit capture passes. Nothing polls for convergence.

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::capture::reconcile;
use trellis::defs::TransformStatus;
use trellis::{CaptureFailure, Config, Trellis, TrellisOptions};

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

async fn full_pass(raw: &mut Client, pool: &trellis::pool::Pool) {
    trellis::client::reconcile_pass(raw, pool, SCHEMA)
        .await
        .expect("reconcile pass");
}

/// The capture half of one pass only, which pauses and refreshes.
async fn capture_pass(raw: &mut Client, pool: &trellis::pool::Pool) {
    let desired = trellis::defs::tables_to_capture(pool)
        .await
        .expect("read the tables to capture");
    let outcome = reconcile::reconcile(
        raw,
        SCHEMA,
        &desired,
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .expect("capture pass");
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
}

async fn drain_to_quiescence(pool: &trellis::pool::Pool, client: &mut Client) {
    use trellis::staging::{apply, has_pending, retire_drained_segments, seal};
    for _ in 0..16 {
        let outcome = seal::seal_phase1_waiting(client).await;
        seal::seal_phase2(client, outcome.sealed_seg_seq, "paused_message_wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "paused_message_test",
            1,
            "trellis_paused_message_test",
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

async fn stored_status(raw: &Client, target: &str) -> TransformStatus {
    let text: String = raw
        .query_one(
            "select status from transform_definitions where split_part(target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .expect("read status")
        .get(0);
    trellis::defs::StoredStatus::from_persisted(&text)
        .expect("a stored status")
        .into()
}

/// Alternates staging-worker passes, builds and drains until `target` is
/// live: a bounded number of explicit steps, not a timed wait.
async fn bring_live(raw: &mut Client, pool: &trellis::pool::Pool, target: &str) {
    for _ in 0..8 {
        full_pass(raw, pool).await;
        trellis::staging::build::settle_builds(pool).await;
        drain_to_quiescence(pool, raw).await;
        if stored_status(raw, target).await == TransformStatus::Live {
            return;
        }
    }
    panic!("{target} did not go live within 8 pass/drain rounds");
}

/// `target`'s reported status and its `capture_failure`.
async fn paused(trellis: &Trellis, target: &str) -> CaptureFailure {
    let reported = trellis.status(target).await.expect("status").expect(target);
    assert_eq!(reported.status, TransformStatus::Paused, "{target}");
    reported
        .capture_failure
        .unwrap_or_else(|| panic!("{target} reports why it is paused"))
}

async fn id_of(raw: &Client, target: &str) -> i64 {
    raw.query_one(
        "select id from transform_definitions where split_part(target_table, '.', 2) = $1",
        &[&target],
    )
    .await
    .expect("read id")
    .get(0)
}

async fn database(raw: &Client) -> String {
    raw.query_one("select current_database()::text", &[])
        .await
        .expect("current database")
        .get(0)
}

/// `public.u` with one 1-1 reader `t` (`a`, `b`) and one aggregate `s`
/// (`SUM(b)` by `g`), both live.
async fn setup(dsn: &str, raw: &mut Client, pool: &trellis::pool::Pool) -> Trellis {
    raw.batch_execute(
        "create table public.u (id int primary key, g int not null, a int, b int); \
         insert into public.u select i, i % 2, i, 10 * i from generate_series(1, 4) i;",
    )
    .await
    .expect("seed");
    let trellis = definer(dsn).await;
    for text in [
        "TRANSFORM t FROM public.u SELECT a AS a, b AS b",
        "TRANSFORM s FROM public.u GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, SUM(b) AS total",
    ] {
        trellis.apply(text).await.expect(text);
    }
    bring_live(raw, pool, "t").await;
    bring_live(raw, pool, "s").await;
    trellis
}

/// A restore pauses `t`, and a column it reads is renamed while it stays
/// paused: the next pass adds that reason, and both show, the newest
/// first, with what `RESUME` would do. With the column back, the restore's
/// reason is the one left, and `RESUME` resets the ordering state the
/// restore recorded before it rebuilds.
#[tokio::test]
async fn two_reasons_show_and_resume_honours_the_restore_that_came_first() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;

    // A dump and restore, as `capture_disruption.rs` shows one.
    app.batch_execute(
        "update capture_triggers set catalog_oid = (catalog_oid::bigint + 1000)::oid \
         where table_name = 'public.u'",
    )
    .await
    .expect("look restored");
    capture_pass(&mut raw, &db.pool).await;
    let restored = paused(&trellis, "t").await;
    assert_eq!(restored.reasons.len(), 1, "{restored:?}");
    assert!(
        restored.error.contains("restored from a logical dump"),
        "{restored:?}"
    );

    app.batch_execute("alter table public.u rename column b to b2")
        .await
        .expect("rename");
    capture_pass(&mut raw, &db.pool).await;
    let both = paused(&trellis, "t").await;
    assert_eq!(both.reasons.len(), 2, "{both:?}");
    assert!(both.reasons[0].contains("\"b\"") && both.reasons[0].contains("renamed or dropped"));
    assert!(both.reasons[1].contains("restored from a logical dump"));
    assert!(
        both.error.starts_with(&both.reasons[0])
            && both
                .error
                .contains(" It was already paused: the database was restored"),
        "{both:?}"
    );
    let outcome = both
        .resume_outcome
        .clone()
        .expect("the pass worked out the outcome");
    assert!(outcome.starts_with("RESUME would refuse: "), "{outcome}");
    assert!(both.error.ends_with(&outcome), "{both:?}");

    app.batch_execute("alter table public.u rename column b2 to b")
        .await
        .expect("rename back");
    capture_pass(&mut raw, &db.pool).await;
    let left = paused(&trellis, "t").await;
    assert_eq!(left.reasons.len(), 1, "{left:?}");
    assert!(left.reasons[0].contains("restored from a logical dump"));
    let outcome = left.resume_outcome.expect("outcome");
    assert!(
        outcome.contains("reset the ordering state") && outcome.contains("rebuild public.t"),
        "{outcome}"
    );
    let restored_flag: bool = raw
        .query_one(
            "select restored from capture_failures where transform_id = $1",
            &[&id_of(&raw, "t").await],
        )
        .await
        .expect("read the record")
        .get(0);
    assert!(restored_flag, "the second reason kept the restore's state");

    trellis.apply("RESUME TRANSFORM t").await.expect("resume");
    bring_live(&mut raw, &db.pool, "t").await;
    let rows: Vec<(i32, i32)> = raw
        .query("select a, b from public.t order by a", &[])
        .await
        .expect("read t")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(rows, vec![(1, 10), (2, 20), (3, 30), (4, 40)]);
}

/// The source's primary key is redefined, pausing its readers, then
/// redefined again while they stay paused: the next pass rewrites the
/// reason to name the key as it is now. Put back, the reason leaves, the
/// message says the cause is gone, and the definition stays paused.
#[tokio::test]
async fn a_key_changed_again_while_paused_rewrites_the_message() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;

    app.batch_execute("alter table public.u drop constraint u_pkey, add primary key (id, g)")
        .await
        .expect("redefine the key");
    capture_pass(&mut raw, &db.pool).await;
    let first = paused(&trellis, "s").await;
    assert!(first.error.contains("to (\"id\", \"g\")"), "{first:?}");
    assert_eq!(first.resume_outcome, None, "worked out by the next pass");
    capture_pass(&mut raw, &db.pool).await;
    let evaluated = paused(&trellis, "s").await;
    assert!(evaluated.resume_outcome.is_some(), "{evaluated:?}");
    assert!(
        evaluated.error.contains("to (\"id\", \"g\")"),
        "{evaluated:?}"
    );

    app.batch_execute("alter table public.u drop constraint u_pkey, add primary key (g, id)")
        .await
        .expect("redefine the key again");
    capture_pass(&mut raw, &db.pool).await;
    let again = paused(&trellis, "s").await;
    assert_eq!(again.reasons.len(), 1, "{again:?}");
    assert!(
        again
            .error
            .contains("changed from (\"id\") to (\"g\", \"id\")"),
        "{again:?}"
    );
    assert_eq!(again.columns, vec!["id".to_string()]);

    app.batch_execute("alter table public.u drop constraint u_pkey, add primary key (id)")
        .await
        .expect("put the key back");
    capture_pass(&mut raw, &db.pool).await;
    let gone = paused(&trellis, "s").await;
    assert!(gone.reasons.is_empty(), "{gone:?}");
    assert!(
        gone.error
            .starts_with("the cause it was paused for is gone. RESUME would "),
        "{gone:?}"
    );
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(stored_status(&raw, "s").await, TransformStatus::Paused);
    assert_eq!(stored_status(&raw, "t").await, TransformStatus::Paused);
}

/// A read column renamed pauses its reader; renamed back while it stays
/// paused, the reason leaves the message, which says `RESUME` would
/// rebuild it, and it stays paused until `RESUME`.
#[tokio::test]
async fn a_cause_fixed_while_paused_leaves_the_message_and_the_pause() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;

    app.batch_execute("alter table public.u rename column a to a2")
        .await
        .expect("rename");
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    let missing = paused(&trellis, "t").await;
    assert_eq!(missing.columns, vec!["a".to_string()], "{missing:?}");
    assert!(
        missing
            .resume_outcome
            .as_deref()
            .is_some_and(|o| o.starts_with("RESUME would refuse: ")),
        "{missing:?}"
    );

    app.batch_execute("alter table public.u rename column a2 to a")
        .await
        .expect("rename back");
    capture_pass(&mut raw, &db.pool).await;
    let fixed = paused(&trellis, "t").await;
    assert!(fixed.reasons.is_empty(), "{fixed:?}");
    assert!(fixed.columns.is_empty(), "{fixed:?}");
    assert_eq!(
        fixed.error,
        "the cause it was paused for is gone. RESUME would rebuild public.t from public.u"
    );
    for _ in 0..2 {
        full_pass(&mut raw, &db.pool).await;
    }
    assert_eq!(stored_status(&raw, "t").await, TransformStatus::Paused);

    trellis.apply("RESUME TRANSFORM t").await.expect("resume");
    bring_live(&mut raw, &db.pool, "t").await;
}

/// A paused definition whose relations don't change is evaluated once,
/// by the first pass after its pause, and never again; a change to a
/// relation it reads has the next pass evaluate it again, and a change to
/// one it doesn't read doesn't.
#[tokio::test]
async fn an_unchanged_paused_definition_is_not_evaluated_again() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;
    app.batch_execute("create table public.other (id int primary key)")
        .await
        .expect("an unrelated table");
    let id = id_of(&raw, "t").await;
    let database = database(&raw).await;
    let refreshes = || trellis::staging::pause_message_refreshes(&database, SCHEMA, id);

    app.batch_execute("alter table public.u rename column a to a2")
        .await
        .expect("rename");
    capture_pass(&mut raw, &db.pool).await;
    paused(&trellis, "t").await;
    let before = refreshes();
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        refreshes(),
        before + 1,
        "the first pass after the pause evaluates it"
    );
    for _ in 0..3 {
        capture_pass(&mut raw, &db.pool).await;
    }
    assert_eq!(refreshes(), before + 1, "nothing it reads changed");

    app.batch_execute("alter table public.other add column x int")
        .await
        .expect("change another table");
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(refreshes(), before + 1, "a table it doesn't read changed");

    app.batch_execute("alter table public.u add column spare int")
        .await
        .expect("change its source");
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(refreshes(), before + 2, "its source changed");
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(refreshes(), before + 2);
    assert!(paused(&trellis, "t").await.resume_outcome.is_some());
}

/// What the message says `RESUME` would do is what it does, for three kinds
/// of pause: a re-type (the copies are re-typed, then the target is
/// rebuilt), a primary key redefined (the 1-1 reader's `RESUME` re-keys its
/// target in place, adding the new key column, and the aggregate's
/// re-records the key; both rebuild), and a column dropped (refused, naming
/// the column).
#[tokio::test]
async fn the_outcome_is_what_resume_does() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;
    let outcome_of = async |target: &str| {
        paused(&trellis, target)
            .await
            .resume_outcome
            .unwrap_or_else(|| panic!("{target}'s outcome"))
    };
    let column_type = async |table: &str, column: &str| -> String {
        app.query_one(
            "select format_type(atttypid, atttypmod) from pg_attribute \
             where attrelid = $1::text::regclass and attname = $2",
            &[&table, &column],
        )
        .await
        .expect("read the column type")
        .get(0)
    };

    // A re-type: the message names the copy and its types, and RESUME
    // re-types it before the rebuild.
    app.batch_execute("alter table public.u alter column a type bigint")
        .await
        .expect("re-type");
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    let retype = outcome_of("t").await;
    assert!(
        retype.starts_with("RESUME would re-type the columns Trellis created")
            && retype.contains("from integer to bigint")
            && retype.ends_with(", then rebuild public.t from public.u"),
        "{retype}"
    );
    assert_eq!(column_type("public.t", "a").await, "integer");
    trellis.apply("RESUME TRANSFORM t").await.expect("resume t");
    bring_live(&mut raw, &db.pool, "t").await;
    assert_eq!(column_type("public.t", "a").await, "bigint");

    // A key redefined: the 1-1 reader's RESUME re-keys its target as its
    // message says; the aggregate's re-records the source's key as its new
    // primary key before it rebuilds, so the outcome names that step first.
    app.batch_execute("alter table public.u drop constraint u_pkey, add primary key (id, g)")
        .await
        .expect("redefine the key");
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        outcome_of("t").await,
        format!("RESUME would {WIDEN_T}, then rebuild public.t from public.u")
    );
    assert_eq!(
        outcome_of("s").await,
        "RESUME would re-record public.u's key as (id, g), then rebuild public.s from public.u"
    );
    trellis.apply("RESUME TRANSFORM t").await.expect("resume t");
    assert_eq!(
        key_and_columns(&app, "public.t").await,
        ("PRIMARY KEY (id, g)".into(), "{id,a,b,g}".into())
    );
    bring_live(&mut raw, &db.pool, "t").await;
    trellis.apply("RESUME TRANSFORM s").await.expect("resume s");
    bring_live(&mut raw, &db.pool, "s").await;

    // A column dropped: the refusal names it, and RESUME refuses.
    app.batch_execute("alter table public.u drop column b")
        .await
        .expect("drop a column");
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    let dropped = outcome_of("s").await;
    assert!(
        dropped.starts_with("RESUME would refuse: ") && dropped.contains("'b'"),
        "{dropped}"
    );
    let err = trellis
        .apply("RESUME TRANSFORM s")
        .await
        .expect_err("the resume refuses while the column is gone");
    let named = dropped
        .strip_prefix(
            "RESUME would refuse: define would refuse the definition as the schema stands now: ",
        )
        .unwrap_or_else(|| panic!("{dropped}"));
    assert!(err.to_string().contains(named), "{err} vs {dropped}");
    assert_eq!(stored_status(&raw, "s").await, TransformStatus::Paused);
}

/// `public.u`'s recorded key, as `definition_keys` holds it.
async fn recorded_key(raw: &Client) -> String {
    raw.query_one(
        "select key_columns::text from definition_keys where table_name = 'public.u'",
        &[],
    )
    .await
    .expect("read the record")
    .get(0)
}

/// The outcome's re-keying for each thing a resume's re-keying decides
/// (`defs::recorded_keys::decide`): a moved primary key is re-recorded for
/// the aggregate, which then rebuilds; nothing the outcome works out is
/// kept, so the record changes only with the RESUME.
#[tokio::test]
async fn a_moved_key_outcome_re_records_it_then_rebuilds() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;

    app.batch_execute("alter table public.u drop constraint u_pkey, add primary key (id, g)")
        .await
        .expect("redefine the key");
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        paused(&trellis, "s").await.resume_outcome.as_deref(),
        Some(
            "RESUME would re-record public.u's key as (id, g), then rebuild public.s from public.u"
        )
    );
    assert_eq!(
        recorded_key(&raw).await,
        "{id}",
        "the outcome keeps nothing"
    );
    trellis.apply("RESUME TRANSFORM s").await.expect("resume s");
    assert_eq!(
        recorded_key(&raw).await,
        "{id,g}",
        "the resume re-recorded it"
    );
    bring_live(&mut raw, &db.pool, "s").await;
}

/// A key the outcome would re-record is locked by another transaction (a
/// concurrent resume or define): the pass doesn't wait for it or fail, and
/// writes no outcome; the next pass after it commits does.
#[tokio::test]
async fn a_locked_key_record_defers_the_outcome_to_a_later_pass() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;
    let mut holder = connect(db.dsn()).await;
    let (database, id) = (database(&raw).await, id_of(&raw, "s").await);
    let refreshes = || trellis::staging::pause_message_refreshes(&database, SCHEMA, id);
    let before = refreshes();

    let lock = holder.transaction().await.expect("begin");
    lock.batch_execute("select 1 from definition_keys where table_name = 'public.u' for update")
        .await
        .expect("lock the record");
    app.batch_execute("alter table public.u drop constraint u_pkey, add primary key (id, g)")
        .await
        .expect("redefine the key");
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(paused(&trellis, "s").await.resume_outcome, None);
    // The refresh returned, rather than failing the rest of the pass.
    assert!(refreshes() > before, "s was evaluated");
    lock.commit().await.expect("release the record");
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        paused(&trellis, "s").await.resume_outcome.as_deref(),
        Some(
            "RESUME would re-record public.u's key as (id, g), then rebuild public.s from public.u"
        )
    );
}

/// What a 1-1 `t`'s RESUME does once `public.u`'s key is `(id, g)`: `g`,
/// which `t` doesn't select, is added to its key.
const WIDEN_T: &str = "re-key public.t by (id, g) in place, adding the key column g, which \
     empties it until the rebuild, then re-record public.u's key as (id, g), then re-record \
     public.t's key as (id, g)";

/// `table`'s key constraint and its columns, in order.
async fn key_and_columns(client: &Client, table: &str) -> (String, String) {
    let row = client
        .query_one(
            "select (select pg_get_constraintdef(oid) from pg_constraint \
                     where conrelid = $1::text::regclass and contype in ('p', 'u')), \
                    array(select attname::text from pg_attribute \
                          where attrelid = $1::text::regclass and attnum > 0 \
                            and not attisdropped order by attnum)::text",
            &[&table],
        )
        .await
        .expect("read the key and columns");
    (row.get(0), row.get(1))
}

/// `target`'s outcome is the refusal its RESUME makes, and the RESUME
/// leaves it paused.
async fn assert_outcome_is_the_refusal(
    trellis: &Trellis,
    raw: &Client,
    pool: &trellis::pool::Pool,
    target: &str,
    names: &str,
) {
    let outcome = paused(trellis, target)
        .await
        .resume_outcome
        .expect("outcome");
    let err = trellis::staging::quarantine::resume_transform(pool, target)
        .await
        .expect_err("the resume refuses");
    let reason = std::error::Error::source(&err)
        .expect("the refusal's reason")
        .to_string();
    assert!(reason.contains(names), "{reason}");
    assert_eq!(outcome, format!("RESUME would refuse: {reason}"));
    assert_eq!(stored_status(raw, target).await, TransformStatus::Paused);
}

/// A 1-1 transform's source whose primary key moved: what its outcome says
/// the re-keying of its target does is what the RESUME does, for each kind:
/// a key column added (the key widened to `(id, g)`), a former key column
/// dropped (narrowed back to `(id)`), and the two refusals, a user's view
/// over the column a narrowing drops and a column of the new key column's
/// name the user added to the target, each the very refusal the RESUME
/// makes.
#[tokio::test]
async fn a_re_keying_outcome_of_a_one_to_one_is_what_resume_does() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;
    let outcome = async |target: &str| {
        paused(&trellis, target)
            .await
            .resume_outcome
            .unwrap_or_else(|| panic!("{target}'s outcome"))
    };

    // Widened: g is added to the key.
    app.batch_execute("alter table public.u drop constraint u_pkey, add primary key (id, g)")
        .await
        .expect("widen the key");
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        outcome("t").await,
        format!("RESUME would {WIDEN_T}, then rebuild public.t from public.u")
    );
    trellis.apply("RESUME TRANSFORM t").await.expect("resume t");
    assert_eq!(
        key_and_columns(&app, "public.t").await,
        ("PRIMARY KEY (id, g)".into(), "{id,a,b,g}".into())
    );
    bring_live(&mut raw, &db.pool, "t").await;

    // Narrowed with a view over g: refused, naming the view.
    app.batch_execute(
        "create view public.t_g as select g from public.t; \
         alter table public.u drop constraint u_pkey, add primary key (id)",
    )
    .await
    .expect("narrow the key");
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    assert_outcome_is_the_refusal(&trellis, &raw, &db.pool, "t", "view public.t_g").await;

    // Without the view: g is dropped.
    app.batch_execute("drop view public.t_g; alter table public.u alter column a set not null")
        .await
        .expect("drop the view");
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        outcome("t").await,
        "RESUME would re-key public.t by (id) in place, dropping the former key column g \
         (ALTER TRANSFORM ... ADD g AS g once it is live adds it back as a field), then \
         re-record public.u's key as (id), then re-record public.t's key as (id), then \
         rebuild public.t from public.u"
    );
    trellis.apply("RESUME TRANSFORM t").await.expect("resume t");
    assert_eq!(
        key_and_columns(&app, "public.t").await,
        ("PRIMARY KEY (id)".into(), "{id,a,b}".into())
    );
    bring_live(&mut raw, &db.pool, "t").await;

    // Widened again, over a column g the user added to the target: refused,
    // naming it.
    app.batch_execute(
        "alter table public.t add column g int; \
         alter table public.u drop constraint u_pkey, add primary key (id, g)",
    )
    .await
    .expect("widen the key");
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    assert_outcome_is_the_refusal(&trellis, &raw, &db.pool, "t", "Trellis didn't create").await;
    assert_eq!(recorded_key(&raw).await, "{id}");
}

/// A source created again with no primary key, only a unique index: the
/// aggregate's outcome refuses as its RESUME does, since no other index is
/// taken as the key.
#[tokio::test]
async fn a_dropped_key_outcome_is_the_resume_refusal() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;

    app.batch_execute(
        "drop table public.u; \
         create table public.u (id int not null, g int not null, a int, b int); \
         create unique index u_id_g on public.u (id, g)",
    )
    .await
    .expect("create the source again");
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    let outcome = paused(&trellis, "s").await.resume_outcome.expect("outcome");
    let err = trellis::staging::quarantine::resume_transform(&db.pool, "s")
        .await
        .expect_err("the resume refuses");
    let reason = std::error::Error::source(&err)
        .expect("the refusal's reason")
        .to_string();
    assert!(reason.contains("u_id_g"), "{reason}");
    assert_eq!(outcome, format!("RESUME would refuse: {reason}"));
    assert_eq!(recorded_key(&raw).await, "{id}");
}

/// A definition already paused with a record is paused again by another
/// writer (a restore after a key change): the second reason is added in
/// front of the first, which stays with its own state, and `RESUME`
/// honours both.
#[tokio::test]
async fn a_second_pause_adds_its_reason_and_keeps_the_first() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;

    app.batch_execute("alter table public.u drop constraint u_pkey, add primary key (id, g)")
        .await
        .expect("redefine the key");
    capture_pass(&mut raw, &db.pool).await;
    let keyed = paused(&trellis, "s").await;
    assert_eq!(keyed.reasons.len(), 1, "{keyed:?}");

    app.batch_execute(
        "update capture_triggers set catalog_oid = (catalog_oid::bigint + 1000)::oid \
         where table_name = 'public.u'",
    )
    .await
    .expect("look restored");
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    let both = paused(&trellis, "s").await;
    assert_eq!(both.reasons.len(), 2, "{both:?}");
    assert!(
        both.reasons[0].contains("restored from a logical dump"),
        "{both:?}"
    );
    assert_eq!(both.reasons[1], keyed.reasons[0], "{both:?}");
    assert_eq!(
        both.columns, keyed.columns,
        "the record keeps the first reason's columns"
    );
    // Both reasons' resume steps, in the order the resume takes them: the
    // key change's re-record (before the re-validation), the restore's
    // reset, then the rebuild.
    assert_eq!(
        both.resume_outcome.as_deref(),
        Some(
            "RESUME would re-record public.u's key as (id, g), then reset the ordering state \
             the dump brought from the original database, then rebuild public.s from public.u"
        )
    );
    trellis.apply("RESUME TRANSFORM s").await.expect("resume s");
    bring_live(&mut raw, &db.pool, "s").await;
    let totals: Vec<(i32, i64)> = raw
        .query("select g, total::bigint from public.s order by g", &[])
        .await
        .expect("read s")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(totals, vec![(0, 60), (1, 40)]);
}

/// An operator's `PAUSE` that withdraws a pending resume, while the pass's
/// refresh is writing the same definition's message: the `PAUSE` waits for
/// the record's row lock, then rewrites the reasons as the refresh left
/// them, rather than writing over reasons it read before the refresh
/// committed (which collided with the refresh's rows).
#[tokio::test]
async fn a_pause_that_withdraws_a_resume_waits_for_a_refresh_writing_the_message() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;

    app.batch_execute("alter table public.u rename column a to a2")
        .await
        .expect("rename");
    capture_pass(&mut raw, &db.pool).await;
    paused(&trellis, "t").await;
    let id = id_of(&raw, "t").await;
    raw.execute(
        "insert into resume_requests (transform_id) values ($1)",
        &[&id],
    )
    .await
    .expect("a pending resume");

    // A refresh's write, held open under the record's row lock.
    let mut refresher = connect(db.dsn()).await;
    let refresh = refresher.transaction().await.expect("begin");
    refresh
        .query_one(
            "select error from capture_failures where transform_id = $1 for update",
            &[&id],
        )
        .await
        .expect("the record's row lock");
    refresh
        .execute(
            "delete from capture_failure_reasons where transform_id = $1",
            &[&id],
        )
        .await
        .expect("delete the reasons");
    refresh
        .execute(
            "insert into capture_failure_reasons \
                 (transform_id, position, source_table, columns, reason) \
             values ($1, 1, 'public.u', '{}', 'refreshed')",
            &[&id],
        )
        .await
        .expect("write the refreshed reason");
    refresh
        .execute(
            "update capture_failures set error = 'refreshed' where transform_id = $1",
            &[&id],
        )
        .await
        .expect("write the refreshed message");

    let pausing = {
        let dsn = db.dsn().to_string();
        tokio::spawn(async move { definer(&dsn).await.apply("PAUSE TRANSFORM t").await })
    };
    // The PAUSE is blocked on a lock the refresh holds.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let blocked: bool = app
            .query_one(
                "select exists (select 1 from pg_stat_activity \
                 where wait_event_type = 'Lock' and datname = current_database())",
                &[],
            )
            .await
            .expect("read pg_stat_activity")
            .get(0);
        if blocked {
            break;
        }
        assert!(Instant::now() < deadline, "the PAUSE never waited");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    refresh.commit().await.expect("commit the refresh");
    pausing
        .await
        .expect("the PAUSE task")
        .expect("the PAUSE withdraws the resume");
    let withdrawn = paused(&trellis, "t").await;
    assert_eq!(withdrawn.reasons.len(), 1, "{withdrawn:?}");
    assert!(
        withdrawn.error.starts_with("paused by the operator"),
        "{withdrawn:?}"
    );
}
