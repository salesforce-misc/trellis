//! A schema change never fails the application's write.
//!
//! Renaming or dropping a column a capture function images used to make
//! every write to the table fail, because the function's static inserts no
//! longer planned. Now the function counts its columns first. On a miss it
//! writes a `schema_changed` marker and images the columns that are left, so
//! the write succeeds. The drain pauses every definition that reads a
//! missing column, with the reason on `Trellis::status`, and the others on
//! the table keep applying. The staging worker's next reconcile regenerates
//! the functions over the columns still read, and a resumed definition
//! (after the column is back) widens capture again and rebuilds.
//!
//! Every test is stepped by hand: writes from a separate session, then
//! explicit seal/drain rounds and staging-worker passes. Nothing polls for
//! convergence.

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::capture::install::{Installed, installed};
use trellis::capture::reconcile;
use trellis::defs::TransformStatus;
use trellis::staging::apply::ApplyError;
use trellis::{CaptureFailureKind, Config, Trellis, TrellisError, TrellisOptions};

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

/// One whole staging-worker pass: capture, then the start of ready builds.
async fn full_pass(raw: &mut Client, pool: &trellis::Pool) {
    trellis::client::reconcile_pass(raw, pool, SCHEMA)
        .await
        .expect("reconcile pass");
}

/// The capture half of one pass only.
async fn capture_pass(raw: &mut Client, pool: &trellis::Pool) -> reconcile::PassOutcome {
    let desired = trellis::defs::tables_to_capture(pool)
        .await
        .expect("read the tables to capture");
    reconcile::reconcile(
        raw,
        SCHEMA,
        &desired,
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .expect("capture pass")
}

/// Seals and drains through the engine's own apply path until nothing is
/// pending anywhere in the ring (as in `capture_join.rs`).
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    use trellis::staging::{apply, has_pending, retire_drained_segments, seal};
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, "capture_schema_change_wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "capture_schema_change_test",
            1,
            "trellis_capture_schema_change_test",
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

async fn status(raw: &Client, target: &str) -> TransformStatus {
    let text: String = raw
        .query_one(
            "select status from transform_definitions where split_part(target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .expect("read status")
        .get(0);
    trellis::defs::StoredStatus::from_row(&text).into()
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

/// Claims, runs and finishes every pending backfill chunk: the hand-driven
/// stand-in for a drain worker's build half (as in `alter_transform.rs`).
async fn run_backfill_chunks(pool: &trellis::Pool) {
    trellis::staging::build::settle_builds(pool).await;
}

/// The key and image columns `table`'s installed capture images.
async fn captured(raw: &Client, table: &str) -> (Vec<String>, Vec<String>) {
    match installed(raw, SCHEMA, table)
        .await
        .expect("read the capture")
    {
        Installed::Complete { spec, current } => {
            assert!(current, "a pass installs current functions");
            (spec.key().to_vec(), spec.columns().to_vec())
        }
        other => panic!("{table} should be captured: {other:?}"),
    }
}

/// Every ring row, in any slot, as `select <columns> from ring where <filter>`.
fn ring(columns: &str, filter: &str) -> String {
    let arms: Vec<String> = (0..4)
        .map(|slot| format!("select {columns} from {SCHEMA}.seg_{slot} where {filter}"))
        .collect();
    arms.join(" union all ")
}

/// The highest `change_id` in the ring: rows a later write stages are above
/// it, whatever drained segments still hold.
async fn high_water(raw: &Client) -> i64 {
    raw.query_one(
        &format!(
            "select coalesce(max(change_id), 0) from ({}) r",
            ring("change_id", "true")
        ),
        &[],
    )
    .await
    .expect("read the high water")
    .get(0)
}

/// Every `schema_changed` marker staged after `since`, as `(src_table,
/// new_image)`.
async fn markers(raw: &Client, since: i64) -> Vec<(String, String)> {
    raw.query(
        &format!(
            "select src_table, new_image from ({}) m where change_id > $1 order by change_id",
            ring(
                "src_table, new_image::text, change_id",
                "op = 'schema_changed'"
            )
        ),
        &[&since],
    )
    .await
    .expect("read markers")
    .into_iter()
    .map(|row| (row.get(0), row.get(1)))
    .collect()
}

/// The `key` of every non-marker row staged after `since`.
async fn imaged_keys(raw: &Client, since: i64) -> Vec<String> {
    raw.query(
        &format!(
            "select key from ({}) r where change_id > $1 order by change_id",
            ring("key, change_id", "op <> 'schema_changed'")
        ),
        &[&since],
    )
    .await
    .expect("read keys")
    .into_iter()
    .map(|row| row.get(0))
    .collect()
}

/// `select <columns> from <table> order by 1` as text rows, for comparing a
/// target with what it should hold.
async fn rows(raw: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
    raw.query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| (0..row.len()).map(|i| row.get(i)).collect())
        .collect()
}

/// `public.u` with three columns, one definition per column plus a ledger
/// aggregate over `b`, all live.
///
/// - `ta` reads `a`, `tb` reads `b`, `tc` reads `c`;
/// - `tsum` sums `b` per `g`.
async fn setup(dsn: &str, raw: &mut Client, pool: &trellis::Pool) -> Trellis {
    raw.batch_execute(
        "create table public.u (id int primary key, g int, a int, b int, c int, spare int); \
         insert into public.u select i, i % 2, i, 10 * i, 100 * i, 0 \
           from generate_series(1, 4) i;",
    )
    .await
    .expect("seed");
    let trellis = definer(dsn).await;
    for text in [
        "TRANSFORM ta FROM public.u SELECT a AS a",
        "TRANSFORM tb FROM public.u SELECT b AS b",
        "TRANSFORM tc FROM public.u SELECT c AS c",
        "TRANSFORM tsum FROM public.u GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, SUM(b) AS total",
    ] {
        trellis.apply(text).await.expect(text);
    }
    bring_live(raw, pool, &["ta", "tb", "tc", "tsum"]).await;
    trellis
}

/// `tb` and `tsum` hold exactly what `public.u` says they should.
async fn b_readers_converged(raw: &Client) {
    assert_eq!(
        rows(raw, "select id::text, b::text from public.tb order by id").await,
        rows(raw, "select id::text, b::text from public.u order by id").await,
        "tb converged"
    );
    assert_eq!(
        rows(
            raw,
            "select g::text, total::text from public.tsum where total <> 0 order by g"
        )
        .await,
        rows(
            raw,
            "select g::text, sum(b)::text from public.u group by g order by g"
        )
        .await,
        "tsum converged"
    );
}

/// Renaming a read column while writers run: every write succeeds, before
/// and after the rename, including one transaction that writes, renames and
/// writes again. The marker is in the ring. The drain pauses the one reader
/// of the column with the reason on `Trellis::status`, while the definitions
/// that don't read it (one of them an aggregate) keep converging over
/// the partial images. The next pass narrows capture to the columns still
/// read, and the markers stop. Once the column is back, resuming the reader
/// widens capture again and rebuilds it.
#[tokio::test]
async fn renaming_a_read_column_pauses_its_reader_and_never_fails_a_write() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;
    let since = high_water(&raw).await;

    // A writer with a statement in flight when the rename is issued: the
    // rename waits for it, and nothing fails either side.
    app.batch_execute("begin; update public.u set b = b + 1 where id = 1")
        .await
        .expect("a write before the rename");
    let rename = {
        let dsn = db.dsn().to_string();
        tokio::spawn(async move {
            connect(&dsn)
                .await
                .batch_execute("alter table public.u rename column a to a2")
                .await
        })
    };
    app.batch_execute("insert into public.u values (5, 1, 5, 50, 500, 0); commit")
        .await
        .expect("the open writer commits");
    rename.await.expect("join").expect("the rename lands");

    // Every kind of write after it succeeds.
    app.batch_execute(
        "insert into public.u values (6, 0, 6, 60, 600, 0); \
         update public.u set b = b + 1, a2 = a2 + 1 where id in (2, 6); \
         update public.u set id = 7 where id = 3; \
         delete from public.u where id = 4; \
         begin; \
         insert into public.u values (8, 0, 8, 80, 800, 0); \
         alter table public.u rename column c to c2; \
         update public.u set b = b + 1 where id = 8; \
         alter table public.u rename column c2 to c; \
         commit;",
    )
    .await
    .expect("no write fails after the rename");
    let marked = markers(&raw, since).await;
    assert!(!marked.is_empty(), "the capture marked the schema change");
    assert!(
        marked.iter().all(|(table, image)| table == "public.u"
            && image.contains("\"missing\": [\"a\"")
            && image.contains("\"key_missing\": false")),
        "{marked:?}"
    );

    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(status(&raw, "ta").await, TransformStatus::Paused);
    let reported = trellis
        .status("ta")
        .await
        .expect("status")
        .expect("ta is registered");
    assert_eq!(reported.status, TransformStatus::Paused);
    let failure = reported.capture_failure.expect("the reason is reported");
    assert_eq!(failure.kind, CaptureFailureKind::Capture, "not a halt");
    assert_eq!(failure.source_table, "public.u");
    assert_eq!(failure.columns, vec!["a".to_string()]);
    assert!(
        failure.error.contains("\"a\"") && failure.error.contains("public.u"),
        "{}",
        failure.error
    );
    for other in ["tb", "tsum"] {
        assert_eq!(status(&raw, other).await, TransformStatus::Live, "{other}");
        let reported = trellis.status(other).await.expect("status").expect(other);
        assert_eq!(reported.capture_failure, None, "{other}");
    }
    // `tc` read `c` only while it was renamed inside one transaction, so its
    // own statements imaged it under the other name: the marker named `c`
    // too, and it pauses as well.
    assert_eq!(status(&raw, "tc").await, TransformStatus::Paused);
    b_readers_converged(&raw).await;

    // The next pass regenerates capture over what the live readers need.
    let outcome = capture_pass(&mut raw, &db.pool).await;
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
    assert_eq!(
        captured(&raw, "public.u").await.1,
        vec!["b", "g", "id"],
        "capture no longer images the paused readers' columns"
    );
    let since = high_water(&raw).await;
    app.batch_execute("update public.u set b = b + 1")
        .await
        .expect("a write after the regeneration");
    assert!(
        markers(&raw, since).await.is_empty(),
        "the regenerated functions no longer miss a column"
    );
    drain_to_quiescence(&db.pool, &mut raw).await;
    b_readers_converged(&raw).await;

    // Put the column back and resume the reader: capture widens again and
    // the rebuild converges.
    raw.batch_execute("alter table public.u rename column a2 to a")
        .await
        .expect("rename back");
    for target in ["ta", "tc"] {
        trellis
            .apply(&format!("RESUME TRANSFORM {target}"))
            .await
            .expect("resume");
        assert_eq!(
            trellis
                .status(target)
                .await
                .expect("status")
                .expect(target)
                .capture_failure,
            None,
            "resuming clears the reason"
        );
    }
    bring_live(&mut raw, &db.pool, &["ta", "tb", "tc", "tsum"]).await;
    assert_eq!(
        captured(&raw, "public.u").await.1,
        vec!["a", "b", "c", "g", "id"]
    );
    app.batch_execute("update public.u set a = a + 1, c = c + 1")
        .await
        .expect("a write after the widen");
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        rows(&raw, "select id::text, a::text from public.ta order by id").await,
        rows(&raw, "select id::text, a::text from public.u order by id").await,
        "the resumed reader rebuilt and applies again"
    );
    assert_eq!(
        rows(&raw, "select id::text, c::text from public.tc order by id").await,
        rows(&raw, "select id::text, c::text from public.u order by id").await,
    );
    b_readers_converged(&raw).await;
}

/// Dropping a read column: the writes succeed, its reader pauses and the
/// others converge. Dropping the paused reader leaves capture narrowed.
#[tokio::test]
async fn dropping_a_read_column_pauses_its_reader_and_never_fails_a_write() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;
    let since = high_water(&raw).await;

    app.batch_execute(
        "alter table public.u drop column a; \
         insert into public.u values (5, 1, 50, 500, 0); \
         update public.u set b = b + 1; \
         delete from public.u where id = 1;",
    )
    .await
    .expect("no write fails after the drop");
    let marked = markers(&raw, since).await;
    assert_eq!(marked.len(), 3, "one marker per statement: {marked:?}");
    assert!(
        marked
            .iter()
            .all(|(_, image)| image.contains("\"missing\": [\"a\"]")),
        "{marked:?}"
    );
    drain_to_quiescence(&db.pool, &mut raw).await;

    let failure = trellis
        .status("ta")
        .await
        .expect("status")
        .expect("ta")
        .capture_failure
        .expect("ta paused by the drop");
    assert_eq!(failure.columns, vec!["a".to_string()]);
    assert_eq!(status(&raw, "ta").await, TransformStatus::Paused);
    for other in ["tb", "tc", "tsum"] {
        assert_eq!(status(&raw, other).await, TransformStatus::Live, "{other}");
    }
    b_readers_converged(&raw).await;

    for statement in ["DROP TRANSFORM ta"] {
        trellis.apply(statement).await.expect(statement);
    }
    let outcome = capture_pass(&mut raw, &db.pool).await;
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
    assert_eq!(
        captured(&raw, "public.u").await.1,
        vec!["b", "c", "g", "id"]
    );
}

/// Renaming a column no definition reads changes nothing: no marker, no
/// pause.
#[tokio::test]
async fn renaming_an_unread_column_marks_and_pauses_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;
    let since = high_water(&raw).await;

    app.batch_execute(
        "alter table public.u rename column spare to spare2; \
         insert into public.u values (5, 1, 5, 50, 500, 0); \
         update public.u set b = b + 1;",
    )
    .await
    .expect("writes after the rename");
    assert!(markers(&raw, since).await.is_empty());
    drain_to_quiescence(&db.pool, &mut raw).await;
    for target in ["ta", "tb", "tc", "tsum"] {
        assert_eq!(
            status(&raw, target).await,
            TransformStatus::Live,
            "{target}"
        );
        let reported = trellis.status(target).await.expect("status").expect(target);
        assert_eq!(reported.capture_failure, None);
    }
    b_readers_converged(&raw).await;
}

/// Renaming a primary-key column: the writes succeed, the function writes
/// the marker only (every image would be keyed by a column it can't name),
/// and every definition on the table pauses. The next pass regenerates
/// capture keyed by the new name, and writes stage images again.
#[tokio::test]
async fn renaming_a_key_column_pauses_every_reader_and_never_fails_a_write() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;
    let since = high_water(&raw).await;

    app.batch_execute(
        "alter table public.u rename column id to uid; \
         insert into public.u values (5, 1, 5, 50, 500, 0); \
         update public.u set b = b + 1; \
         delete from public.u where uid = 1;",
    )
    .await
    .expect("no write fails after the key rename");
    let marked = markers(&raw, since).await;
    assert_eq!(marked.len(), 3, "one marker per statement: {marked:?}");
    assert!(
        marked
            .iter()
            .all(|(_, image)| image.contains("\"key_missing\": true")),
        "{marked:?}"
    );
    assert_eq!(
        imaged_keys(&raw, since).await,
        Vec::<String>::new(),
        "without its key the function images nothing"
    );

    drain_to_quiescence(&db.pool, &mut raw).await;
    for target in ["ta", "tb", "tc", "tsum"] {
        assert_eq!(
            status(&raw, target).await,
            TransformStatus::Paused,
            "{target}"
        );
        let failure = trellis
            .status(target)
            .await
            .expect("status")
            .expect(target)
            .capture_failure
            .expect("every reader reports the key change");
        assert!(failure.columns.contains(&"id".to_string()), "{failure:?}");
    }

    // Every reader is paused, so the next pass regenerates capture over the
    // new key alone, and a write stages keyed rows again.
    let outcome = capture_pass(&mut raw, &db.pool).await;
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
    assert_eq!(
        captured(&raw, "public.u").await,
        (vec!["uid".to_string()], vec!["uid".to_string()])
    );
    let since = high_water(&raw).await;
    // A delete: capture now images `uid` alone, and an update of an unimaged
    // column stages nothing.
    app.batch_execute("delete from public.u where uid = 2")
        .await
        .expect("a write after the regeneration");
    assert!(markers(&raw, since).await.is_empty());
    assert_eq!(imaged_keys(&raw, since).await, vec!["2".to_string()]);
}

/// A ring row's key, op and images, as text.
type ImageRow = (String, String, Option<String>, Option<String>);

/// The images a function writes on its schema-changed branch, built at run
/// time and run with `EXECUTE`, are byte-identical to the static inserts'
/// over the columns that are left, from an application session whose output
/// settings differ from Trellis's.
#[tokio::test]
async fn the_partial_images_match_the_static_ones_for_the_columns_left() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.p (id int primary key, t text, n numeric, f float8, \
           ts timestamptz, iv interval, b bytea, arr int[], j jsonb, flag bool, \
           nothing text, gone int)",
    )
    .await
    .expect("seed");
    // Capture every column by hand: no definition can read them all (an
    // array, say), and the images are what's compared.
    let _migrated = definer(db.dsn()).await;
    let columns = [
        "t", "n", "f", "ts", "iv", "b", "arr", "j", "flag", "nothing", "gone",
    ];
    let spec = trellis::capture::sql::CaptureSpec::new(
        "public.p",
        vec!["id".to_string()],
        columns.iter().map(|c| c.to_string()),
    )
    .expect("a valid spec");
    trellis::capture::install::install(&mut raw, SCHEMA, &spec, None)
        .await
        .expect("install")
        .done()
        .expect("nothing holds the table");

    let app = connect(db.dsn()).await;
    app.batch_execute(
        "set datestyle to 'SQL, DMY'; set timezone to 'America/St_Johns'; \
         set bytea_output to 'escape'; set intervalstyle to 'sql_standard'; \
         set extra_float_digits to 0",
    )
    .await
    .expect("an application session with its own output settings");
    let values = "'it''s a \\ test', 12345.678900, 0.1, '2024-02-29 13:14:15.123456+05:30', \
                  '1 year 2 mons 3 days 04:05:06.7', '\\x00ff'::bytea, '{1,NULL,3}', \
                  '{\"k\": [1, 2]}', true, null";
    let since = high_water(&raw).await;
    app.batch_execute(&format!(
        "insert into public.p values (1, {values}, 7); \
         update public.p set n = n + 1 where id = 1; \
         alter table public.p drop column gone; \
         insert into public.p values (2, {values}); \
         update public.p set n = n + 1 where id = 2;"
    ))
    .await
    .expect("no write fails after the drop");

    let images: Vec<ImageRow> = raw
        .query(
            &format!(
                "select key, op, (old_image - 'gone' - 'id')::text, \
                        (new_image - 'gone' - 'id')::text \
                 from ({}) r where change_id > $1 order by change_id",
                ring(
                    "key, op, old_image, new_image, change_id",
                    "op <> 'schema_changed'"
                )
            ),
            &[&since],
        )
        .await
        .expect("read images")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
        .collect();
    assert_eq!(images.len(), 4, "{images:?}");
    let (before, after) = images.split_at(2);
    for ((k1, op1, old1, new1), (k2, op2, old2, new2)) in before.iter().zip(after) {
        assert_eq!((k1.as_str(), k2.as_str()), ("1", "2"));
        assert_eq!(op1, op2);
        assert_eq!(old1, old2, "old images match");
        assert_eq!(new1, new2, "new images match");
    }
    let static_new = before[0].3.as_deref().expect("an insert has a new image");
    assert!(
        static_new.contains("\"ts\": \"2024-02-29 07:44:15.123456+00\""),
        "rendered under Trellis's pinned settings: {static_new}"
    );
    let partial: String = raw
        .query_one(
            &format!(
                "select new_image::text from ({}) r where change_id > $1 and key = '2' \
                 and op = 'insert'",
                ring("key, op, new_image, change_id", "true")
            ),
            &[&since],
        )
        .await
        .expect("the partial insert")
        .get(0);
    assert!(!partial.contains("\"gone\""), "{partial}");
}

/// A key column renamed while the table is quiet, so the staging worker's
/// capture pass sees the rename before any write marks it. The pass pauses
/// every reader instead of regenerating the functions keyed by the new name
/// (after which no write would ever mark the change, and every drain would
/// fail applying rows keyed by a column the definitions don't know).
#[tokio::test]
async fn a_key_rename_the_capture_pass_sees_first_pauses_every_reader() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;

    app.batch_execute("alter table public.u rename column id to uid")
        .await
        .expect("rename the key");
    let outcome = capture_pass(&mut raw, &db.pool).await;
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
    for target in ["ta", "tb", "tc", "tsum"] {
        assert_eq!(
            status(&raw, target).await,
            TransformStatus::Paused,
            "{target}"
        );
        let failure = trellis
            .status(target)
            .await
            .expect("status")
            .expect(target)
            .capture_failure
            .expect("the reason is reported");
        assert_eq!(failure.columns, vec!["id".to_string()]);
    }
    assert_eq!(
        captured(&raw, "public.u").await.0,
        vec!["id".to_string()],
        "the pass that pauses leaves the functions for the next one"
    );

    app.batch_execute(
        "insert into public.u values (5, 1, 5, 50, 500, 0); \
         update public.u set b = b + 1; \
         delete from public.u where uid = 1;",
    )
    .await
    .expect("no write fails");
    drain_to_quiescence(&db.pool, &mut raw).await;
    let outcome = capture_pass(&mut raw, &db.pool).await;
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
    assert_eq!(
        captured(&raw, "public.u").await,
        (vec!["uid".to_string()], vec!["uid".to_string()])
    );
}

/// A primary key redefined without a rename, its old column still there.
/// The installed functions go on imaging the old key, so no write
/// marks the change, and without a check the capture pass would regenerate
/// them keyed by the new one, silently. The pass pauses every reader
/// instead, with the old and new key on its status, and leaves the functions
/// for the next pass. A row the old functions staged then drains without
/// reaching a paused reader, and no write fails. (A drain page that runs
/// before the pass pauses them itself: see the false-positive rows below.)
#[tokio::test]
async fn a_primary_key_redefined_without_a_rename_pauses_every_reader() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;

    app.batch_execute(
        "alter table public.u drop constraint u_pkey, add primary key (id, g); \
         update public.u set b = b + 1 where id = 1;",
    )
    .await
    .expect("redefine the key, and write");

    let outcome = capture_pass(&mut raw, &db.pool).await;
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
    for target in ["ta", "tb", "tc", "tsum"] {
        let reported = trellis.status(target).await.expect("status").expect(target);
        assert_eq!(reported.status, TransformStatus::Paused, "{target}");
        let failure = reported.capture_failure.expect("the reason is reported");
        assert_eq!(failure.source_table, "public.u");
        assert_eq!(failure.columns, vec!["id".to_string()]);
        assert!(
            failure.error.contains("primary key") && failure.error.contains("\"g\""),
            "{failure:?}"
        );
    }
    assert_eq!(
        captured(&raw, "public.u").await.0,
        vec!["id".to_string()],
        "the pass that pauses leaves the functions for the next one"
    );

    drain_to_quiescence(&db.pool, &mut raw).await;
    app.batch_execute("insert into public.u values (5, 1, 5, 50, 500, 0)")
        .await
        .expect("no write fails");
    drain_to_quiescence(&db.pool, &mut raw).await;
    let outcome = capture_pass(&mut raw, &db.pool).await;
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
    assert_eq!(
        captured(&raw, "public.u").await.0,
        vec!["id".to_string(), "g".to_string()],
        "with every reader paused, the next pass keys capture by the new key"
    );
}

/// A drain page that runs between a primary key's redefinition and
/// the capture pass. The old functions still stage rows keyed by the old
/// key, which the page would read by the new one. The page compares the
/// installed capture's key with the table's before it applies anything,
/// pauses every reader of the table as the pass would, and charges nothing to
/// quarantine. `alter` redefines the key; `new_key` is how the reason names
/// the key it leaves.
async fn a_page_before_the_pass_pauses_on_a_redefined_key(alter: &str, new_key: &str) {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;

    app.batch_execute(&format!(
        "{alter}; update public.u set b = b + 1 where id = 1;"
    ))
    .await
    .expect("redefine the key, and write");

    drain_to_quiescence(&db.pool, &mut raw).await;
    for target in ["ta", "tb", "tc", "tsum"] {
        let reported = trellis.status(target).await.expect("status").expect(target);
        assert_eq!(reported.status, TransformStatus::Paused, "{target}");
        let failure = reported.capture_failure.expect("the reason is reported");
        assert_eq!(failure.source_table, "public.u");
        assert_eq!(failure.columns, vec!["id".to_string()]);
        assert!(
            failure.error.contains("primary key") && failure.error.contains(new_key),
            "{failure:?}"
        );
    }
    assert_eq!(
        rows(
            &raw,
            "select (select count(*) from poison)::text, \
                    (select count(*) from poison_held)::text, \
                    (select count(*) from key_deaths)::text"
        )
        .await,
        vec![vec![
            Some("0".to_string()),
            Some("0".to_string()),
            Some("0".to_string())
        ]],
        "nothing is charged to quarantine"
    );

    let outcome = capture_pass(&mut raw, &db.pool).await;
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
    app.batch_execute("update public.u set b = b + 1")
        .await
        .expect("no write fails");
    drain_to_quiescence(&db.pool, &mut raw).await;
}

#[tokio::test]
async fn a_drain_page_before_the_pass_pauses_on_a_key_whose_arity_changed() {
    a_page_before_the_pass_pauses_on_a_redefined_key(
        "alter table public.u drop constraint u_pkey, add primary key (id, g)",
        "(\"id\", \"g\")",
    )
    .await;
}

#[tokio::test]
async fn a_drain_page_before_the_pass_pauses_on_a_key_swapped_for_another_column() {
    a_page_before_the_pass_pauses_on_a_redefined_key(
        "alter table public.u drop constraint u_pkey, add primary key (c)",
        "(\"c\")",
    )
    .await;
}

/// The false-positive rows: a key rebuilt on the same columns, by
/// `ADD PRIMARY KEY USING INDEX`, renamed, or joined by new indexes and
/// constraints, pauses nothing when a page drains before the pass.
#[tokio::test]
async fn a_drain_page_pauses_nothing_for_a_key_rebuilt_on_the_same_columns() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let _trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;

    for alter in [
        // X6
        "alter table public.u drop constraint u_pkey, add constraint u_pkey primary key (id)",
        // D20
        "create unique index u_pk2 on public.u (id); \
         alter table public.u drop constraint u_pkey, \
             add constraint u_pkey primary key using index u_pk2",
        // X7
        "alter table public.u rename constraint u_pkey to u_pk_renamed; \
         alter table public.u rename constraint u_pk_renamed to u_pkey",
        // D16, D18, D19
        "create index u_a_idx on public.u (a); \
         alter table public.u add constraint ck check (a > 0); \
         alter table public.u add constraint uq unique (c)",
    ] {
        app.batch_execute(&format!(
            "{alter}; update public.u set b = b + 1 where id = 1;"
        ))
        .await
        .expect(alter);
        drain_to_quiescence(&db.pool, &mut raw).await;
        for target in ["ta", "tb", "tc", "tsum"] {
            assert_eq!(
                status(&raw, target).await,
                TransformStatus::Live,
                "{alter}: {target}"
            );
        }
        b_readers_converged(&raw).await;
    }
}

/// Resuming a definition while the column it reads is still missing: the
/// resume re-validates it as define would and refuses, so it stays
/// paused with its reason, rather than waiting to backfill with the
/// table's capture failing every pass. The other readers of the table keep
/// applying.
#[tokio::test]
async fn resuming_while_the_column_is_still_missing_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;
    app.batch_execute("alter table public.u rename column a to a2; update public.u set b = b + 1")
        .await
        .expect("writes after the rename");
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(status(&raw, "ta").await, TransformStatus::Paused);
    let outcome = capture_pass(&mut raw, &db.pool).await;
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);

    match trellis.apply("RESUME TRANSFORM ta").await {
        Err(TrellisError::Apply(ApplyError::ResumeRefused { reason, .. })) => {
            assert!(reason.to_string().contains("'a'"), "{reason}");
        }
        other => panic!("expected the resume to be refused, got {other:?}"),
    }
    let outcome = capture_pass(&mut raw, &db.pool).await;
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
    let reported = trellis.status("ta").await.expect("status").expect("ta");
    assert_eq!(reported.status, TransformStatus::Paused);
    assert_eq!(
        reported.capture_failure.expect("the reason").columns,
        vec!["a".to_string()]
    );

    let outcome = capture_pass(&mut raw, &db.pool).await;
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
    app.batch_execute("update public.u set b = b + 1")
        .await
        .expect("a write");
    drain_to_quiescence(&db.pool, &mut raw).await;
    b_readers_converged(&raw).await;
}

/// The schema-changed branch's `format()` template and fragments keep odd
/// identifiers intact: a schema, table, composite key and columns with
/// mixed case, spaces, quotes, `%` and `%1$s`. The partial images, keys
/// (a key move included) and group keys match the static ones.
#[tokio::test]
async fn the_partial_images_keep_odd_identifiers() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        r#"create schema "Sch%e ma";
           create table "Sch%e ma"."Od""d T%able" ("K%1" int, "k 2" text, "Mixed" text,
               "a%s" int, "q""uote" text, "%1$s" text, gone int, primary key ("K%1", "k 2"));"#,
    )
    .await
    .expect("seed");
    let _migrated = definer(db.dsn()).await;
    let spec = trellis::capture::sql::CaptureSpec::new(
        "Sch%e ma.Od\"d T%able",
        vec!["K%1".to_string(), "k 2".to_string()],
        ["Mixed", "a%s", "q\"uote", "%1$s", "gone"]
            .iter()
            .map(|c| c.to_string()),
    )
    .expect("a valid spec");
    trellis::capture::install::install(&mut raw, SCHEMA, &spec, None)
        .await
        .expect("install")
        .done()
        .expect("nothing holds the table");

    let app = connect(db.dsn()).await;
    let t = r#""Sch%e ma"."Od""d T%able""#;
    let writes = format!(
        "insert into {t} values (1, 'x' || chr(31) || 'y%s', 'M''x', 5, 'q\"', '%s%%'{{gone}}); \
         update {t} set \"a%s\" = 6, \"K%1\" = 2 where \"K%1\" = 1; \
         delete from {t};"
    );
    let since = high_water(&raw).await;
    app.batch_execute(&writes.replace("{gone}", ", 7"))
        .await
        .expect("writes before the drop");
    app.batch_execute(&format!("alter table {t} drop column gone"))
        .await
        .expect("drop");
    app.batch_execute(&writes.replace("{gone}", ""))
        .await
        .expect("no write fails after the drop");

    let images: Vec<ImageRow> = raw
        .query(
            &format!(
                "select key, op, (old_image - 'gone')::text, (new_image - 'gone')::text \
                 from ({}) r where change_id > $1 order by change_id",
                ring(
                    "key, op, old_image, new_image, change_id",
                    "op <> 'schema_changed'"
                )
            ),
            &[&since],
        )
        .await
        .expect("read images")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
        .collect();
    assert_eq!(images.len(), 8, "{images:#?}");
    let (before, after) = images.split_at(4);
    assert_eq!(before, after, "partial rows match the static ones");
    assert!(
        before[0]
            .3
            .as_deref()
            .is_some_and(|i| i.contains("\"%1$s\": \"%s%%\"")),
        "{before:?}"
    );
    assert_eq!(markers(&raw, since).await.len(), 3, "one per statement");
}

/// A definition paused for a missing column holds up no other definition on
/// its table. `ta` is edited to read `spare`, a column nothing imaged
/// yet, so its new field waits for a capture imaging `spare`
/// (`column_status.awaiting_capture`) and its field build waits until one
/// does. Before any pass widens capture, `a` (which only `ta`
/// reads) is renamed, so the drain pauses `ta` with a capture failure and
/// capture stops imaging for it: `a` is gone and `spare` is read by no one
/// else, so capture can never image them. `tb`'s own edit then starts a
/// field build on the same table, which must not wait on the paused `ta`'s
/// columns, so `tb` builds its field and goes live while `ta` stays paused.
/// `ta`'s status names only its capture failure, not a capture wait on the
/// table. Once the column is back, resuming `ta` rebuilds it: the rebuild's
/// start waits for a capture imaging `spare`, releases `spare2`, and the
/// rebuild covers every write made meanwhile.
#[tokio::test]
async fn a_definition_paused_for_a_missing_column_does_not_hold_the_tables_catch_up() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;

    trellis
        .apply("ALTER TRANSFORM ta ADD spare AS spare2")
        .await
        .expect("alter ta");
    let awaiting: bool = raw
        .query_one(
            "select exists (select 1 from column_status \
             where transform_table = 'ta' and column_name = 'spare2' and awaiting_capture)",
            &[],
        )
        .await
        .expect("read column_status")
        .get(0);
    assert!(awaiting, "spare2 waits for a capture imaging spare");

    raw.batch_execute("alter table public.u rename column a to a2; update public.u set b = b + 1")
        .await
        .expect("writes after the rename");
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(status(&raw, "ta").await, TransformStatus::Paused);

    trellis
        .apply("ALTER TRANSFORM tb ADD b AS b3")
        .await
        .expect("alter tb");
    assert_eq!(status(&raw, "tb").await, TransformStatus::Backfilling);

    bring_live(&mut raw, &db.pool, &["tb"]).await;
    assert_eq!(
        rows(&raw, "select id::text, b3::text from public.tb order by id").await,
        rows(&raw, "select id::text, b::text from public.u order by id").await,
        "tb's new field is built"
    );
    // A lock wait on the table's capture, as a pass records one, is not
    // what the paused `ta` waits on, though its `spare2` still awaits
    // capture: only the resume its capture failure asks for gets it going.
    raw.batch_execute(
        "insert into capture_holdups \
             (table_name, since, operation, lock_mode, observed_at, blockers) \
         values ('public.u', now(), 'widen', 'ShareRowExclusiveLock', now(), '{}')",
    )
    .await
    .expect("record a lock wait");
    let reported = trellis.status("ta").await.expect("status").expect("ta");
    assert_eq!(reported.status, TransformStatus::Paused);
    assert_eq!(reported.capture_wait, None, "{reported:?}");
    assert_eq!(
        reported.capture_failure.expect("the reason").columns,
        vec!["a".to_string()]
    );
    raw.batch_execute("delete from capture_holdups")
        .await
        .expect("clear the lock wait");

    // Once the column is back, resuming `ta` counts it again: the widen
    // images `a` and `spare`, `spare2` is released, and the rebuild covers
    // every write made while `ta` was left out.
    raw.batch_execute(
        "alter table public.u rename column a2 to a; \
         update public.u set a = a + 5, spare = id * 7",
    )
    .await
    .expect("rename back and write");
    trellis.apply("RESUME TRANSFORM ta").await.expect("resume");
    bring_live(&mut raw, &db.pool, &["ta", "tb"]).await;
    assert_eq!(
        rows(
            &raw,
            "select id::text, a::text, spare2::text from public.ta order by id"
        )
        .await,
        rows(
            &raw,
            "select id::text, a::text, spare::text from public.u order by id"
        )
        .await,
        "the resumed ta rebuilt with its edited field"
    );
    let left: i64 = raw
        .query_one(
            "select count(*) from column_status where transform_table = 'ta'",
            &[],
        )
        .await
        .expect("read column_status")
        .get(0);
    assert_eq!(left, 0, "spare2 no longer waits");
}

/// An edit adding a field that reads, by alias, a field still
/// awaiting its capture. Apply holds the reader out with that field, and
/// the reader's own field build runs (and skips it) before the capture is
/// ready, so the build that releases the awaited field must write the
/// reader too, though the reader didn't exist when that build was
/// registered. Each step is driven by hand, in the order that leaves it to
/// the release: the reader's build first, then the capture pass and its
/// drain, then the release.
#[tokio::test]
async fn the_release_of_a_field_awaiting_capture_builds_a_reader_added_after_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;

    trellis
        .apply("ALTER TRANSFORM ta ADD spare AS spare2")
        .await
        .expect("add a field awaiting its capture");
    trellis
        .apply("ALTER TRANSFORM ta ADD spare2 + 1 AS spare3")
        .await
        .expect("add a reader of it");
    run_backfill_chunks(&db.pool).await;
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    run_backfill_chunks(&db.pool).await;
    bring_live(&mut raw, &db.pool, &["ta"]).await;
    assert_eq!(
        rows(
            &raw,
            "select id::text, spare2::text, spare3::text from public.ta order by id"
        )
        .await,
        rows(
            &raw,
            "select id::text, spare::text, (spare + 1)::text from public.u order by id"
        )
        .await,
        "the release's build writes the reader added after it was registered"
    );
}

/// `public.users`, keyed by `handle`; `users_copy` reads it directly,
/// `posts_named` reads it as the joined side of a `JOIN` from `posts` on
/// `posts.author = users.handle`, and `named_copy` is a downstream hop target
/// reading `users_copy`: the closure a halt on `users` reaches.
/// `posts_plain` reads only `posts`, and `public.o` is an unrelated table
/// whose `o_copy` shares the drain's pages with them. All live.
async fn halt_closure_setup(dsn: &str, raw: &mut Client, pool: &trellis::Pool) -> Trellis {
    raw.batch_execute(
        "create table public.users (handle varchar(16) primary key, name text); \
         create table public.posts (id int primary key, author varchar(16)); \
         create table public.o (id int primary key, v int); \
         insert into public.users values ('ann', 'Ann'), ('bob', 'Bob'); \
         insert into public.posts values (1, 'ann'), (2, 'bob'); \
         insert into public.o values (1, 10);",
    )
    .await
    .expect("seed");
    let trellis = definer(dsn).await;
    for text in [
        "TRANSFORM posts_named FROM public.posts JOIN public.users \
         ON posts.author = users.handle GROUP BY users.name WITHOUT SOURCE INDEX \
         SELECT users.name AS name, COUNT(*) AS n",
        "TRANSFORM users_copy FROM public.users SELECT name AS name",
        "TRANSFORM posts_plain FROM public.posts SELECT author AS author",
        "TRANSFORM o_copy FROM public.o SELECT v AS v",
    ] {
        trellis.apply(text).await.expect(text);
    }
    bring_live(
        raw,
        pool,
        &["posts_named", "users_copy", "posts_plain", "o_copy"],
    )
    .await;
    // A downstream hop is defined once its upstream is live.
    let text = "TRANSFORM named_copy FROM users_copy SELECT name AS name";
    trellis.apply(text).await.expect(text);
    bring_live(raw, pool, &HALT_CLOSURE_ALL).await;
    trellis
}

/// What a halt on `users` pauses in [`halt_closure_setup`].
const HALT_CLOSURE: [&str; 3] = ["posts_named", "users_copy", "named_copy"];

/// Every definition [`halt_closure_setup`] registers.
const HALT_CLOSURE_ALL: [&str; 5] = [
    "posts_named",
    "users_copy",
    "named_copy",
    "posts_plain",
    "o_copy",
];

/// The `kind` and `error` of `target`'s `capture_failures` record, if any.
async fn halt_record(raw: &Client, target: &str) -> Option<(String, String)> {
    raw.query_opt(
        "select f.kind, f.error from capture_failures f \
         join transform_definitions d on d.id = f.transform_id \
         where split_part(d.target_table, '.', 2) = $1",
        &[&target],
    )
    .await
    .expect("read the capture failure")
    .map(|row| (row.get(0), row.get(1)))
}

/// The targets [`Trellis::definitions`] reports a halt on, each
/// checked to be a halt, in id order.
async fn halted_targets(trellis: &Trellis) -> Vec<String> {
    let mut halted = Vec::new();
    for summary in trellis.definitions().await.expect("definitions") {
        if let Some(halt) = summary.halt {
            assert_eq!(
                halt.kind,
                CaptureFailureKind::Halt,
                "{}",
                summary.target_table
            );
            halted.push(summary.target_table);
        }
    }
    halted
}

async fn halting_stops(pool: &trellis::Pool) -> i64 {
    trellis::staging::halting_stop_stats(pool)
        .await
        .expect("halting_stop_stats")
        .stop_count
}

/// Counts the `ERROR` events logged on this thread while its guard lives,
/// keeping each one's fields for the failure message.
mod error_log {
    use std::sync::{Arc, LazyLock, Mutex};

    use tracing::field::{Field, Visit};
    use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

    #[derive(Clone, Default)]
    pub struct Errors(pub Arc<Mutex<Vec<String>>>);

    struct Fields(String);

    impl Visit for Fields {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0.push_str(&format!("{}={value:?} ", field.name()));
        }
    }

    struct ErrorLayer(Errors);

    impl<S: tracing::Subscriber> Layer<S> for ErrorLayer {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            if *event.metadata().level() == tracing::Level::ERROR {
                let mut fields = Fields(String::new());
                event.record(&mut fields);
                self.0.0.lock().unwrap().push(fields.0);
            }
        }
    }

    /// As `client::log_capture::install_capture`: a permanently live no-op
    /// dispatcher keeps `tracing`'s cached callsite interest computed over
    /// every live dispatcher, so another test's thread can't cache a shared
    /// callsite as "never" for this one.
    pub fn install() -> (tracing::subscriber::DefaultGuard, Errors) {
        static KEEP_INTEREST_GLOBAL: LazyLock<tracing::Dispatch> =
            LazyLock::new(|| tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default()));
        LazyLock::force(&KEEP_INTEREST_GLOBAL);
        let errors = Errors::default();
        let subscriber = tracing_subscriber::registry().with(ErrorLayer(errors.clone()));
        (tracing::subscriber::set_default(subscriber), errors)
    }
}

/// A page holding a write to a table whose key the drain can't
/// use, beside an unrelated table's write. The drain used to fail the page
/// every poll, forever, logging and counting a stop each time. Now it
/// pauses the halt's closure (`users_copy` reads `users`, `posts_named`
/// joins it, `named_copy` reads `users_copy`), records
/// why, and commits the rest of the page. Over many more polls with more
/// writes to `users`, the stop is counted and logged once. Once the key is
/// fixed, resuming each one rebuilds it to what a fresh build derives.
#[tokio::test]
async fn a_halting_key_pauses_its_closure_once_and_the_rest_of_the_page_drains() {
    use trellis::staging::{apply, has_pending, seal};
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = halt_closure_setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;
    let stops = halting_stops(&db.pool).await;
    let (_guard, errors) = error_log::install();

    app.batch_execute(
        "alter table public.users alter column handle type character(8); \
         update public.users set name = 'Annie' where handle = 'ann'; \
         update public.o set v = 11 where id = 1;",
    )
    .await
    .expect("retype the key, and write it and the unrelated table");
    let sealed = seal::seal_phase1(&mut raw).await.expect("seal phase 1");
    seal::seal_phase2(&raw, sealed.sealed_seg_seq, "capture_schema_change_wake")
        .await
        .expect("seal phase 2");
    let outcome = apply::drain_once(
        &db.pool,
        sealed.sealed_seg_seq,
        "capture_schema_change_test",
        1,
        "trellis_capture_schema_change_test",
    )
    .await
    .expect("the halt pauses its closure and the page commits");
    assert!(outcome.is_some_and(|o| o.batch_drained), "the page drains");
    assert_eq!(
        rows(&raw, "select id::text, v::text from public.o_copy").await,
        vec![vec![Some("1".to_string()), Some("11".to_string())]],
        "the unrelated definition's write committed with the page"
    );
    for target in HALT_CLOSURE {
        assert_eq!(
            status(&raw, target).await,
            TransformStatus::Paused,
            "{target}"
        );
        let (kind, error) = halt_record(&raw, target)
            .await
            .unwrap_or_else(|| panic!("{target} has no halt record"));
        assert_eq!(kind, "halt", "{target}");
        assert!(error.contains("character"), "{target}: {error}");
        let reported = trellis.status(target).await.expect("status").expect(target);
        assert_eq!(reported.status, TransformStatus::Paused, "{target}");
        let failure = reported.capture_failure.expect(target);
        assert_eq!(failure.kind, CaptureFailureKind::Halt, "{target}");
        assert_eq!(failure.source_table, "public.users", "{target}");
        assert_eq!(failure.error, error, "{target}");
    }
    for target in ["posts_plain", "o_copy"] {
        assert_eq!(
            status(&raw, target).await,
            TransformStatus::Live,
            "{target}"
        );
        assert_eq!(halt_record(&raw, target).await, None, "{target}");
    }
    assert_eq!(halting_stops(&db.pool).await, stops + 1);
    // The health read: one listing names every halted definition and why.
    assert_eq!(
        halted_targets(&trellis).await,
        HALT_CLOSURE
            .iter()
            .map(|t| format!("public.{t}"))
            .collect::<Vec<_>>(),
        "definitions() reports the halt on exactly the closure"
    );

    for poll in 0..20 {
        app.batch_execute(&format!(
            "update public.users set name = 'Ann {poll}' where handle = 'ann'; \
             update public.o set v = {poll} where id = 1;"
        ))
        .await
        .expect("write the halted table and the unrelated one");
        drain_to_quiescence(&db.pool, &mut raw).await;
    }
    assert!(
        !has_pending(&raw).await.expect("has_pending"),
        "every segment retired"
    );
    assert_eq!(
        rows(&raw, "select v::text from public.o_copy").await,
        vec![vec![Some("19".to_string())]],
        "the unrelated definition kept applying"
    );
    assert_eq!(
        halting_stops(&db.pool).await,
        stops + 1,
        "one stop per halt"
    );
    let logged = errors.0.lock().unwrap().clone();
    assert_eq!(logged.len(), 1, "one error line per halt: {logged:#?}");
    assert!(
        HALT_CLOSURE.iter().all(|t| logged[0].contains(t)),
        "the line names the closure: {}",
        logged[0]
    );

    app.batch_execute("alter table public.users alter column handle type varchar(16)")
        .await
        .expect("fix the key");
    for target in HALT_CLOSURE {
        trellis
            .apply(&format!("RESUME TRANSFORM {target}"))
            .await
            .unwrap_or_else(|e| panic!("resume {target}: {e}"));
    }
    bring_live(&mut raw, &db.pool, &HALT_CLOSURE_ALL).await;
    assert!(
        halted_targets(&trellis).await.is_empty(),
        "a resume ends the halt"
    );
    for target in HALT_CLOSURE {
        let reported = trellis.status(target).await.expect("status").expect(target);
        assert_eq!(reported.capture_failure, None, "{target}");
    }
    assert_eq!(
        rows(&raw, "select name from public.users_copy order by name").await,
        rows(&raw, "select name from public.users order by name").await,
        "users_copy rebuilt"
    );
    assert_eq!(
        rows(
            &raw,
            "select name, n::text from public.posts_named order by name"
        )
        .await,
        rows(
            &raw,
            "select u.name, count(*)::text from public.posts p \
             join public.users u on u.handle = p.author group by u.name order by u.name"
        )
        .await,
        "posts_named rebuilt"
    );
    assert_eq!(
        rows(
            &raw,
            "select handle::text, name from public.named_copy order by handle"
        )
        .await,
        rows(
            &raw,
            "select handle::text, name from public.users_copy order by handle"
        )
        .await,
        "named_copy rebuilt"
    );
}

/// A member of a halted closure that reads the key
/// define would refuse can't be resumed before the key is fixed. The resume
/// re-validates it as define would and refuses, naming the type, and it
/// stays paused with its halt record: the halt and the resume's validation
/// compose, the first record standing. A member that doesn't read the key
/// (downstream of one that does) passes the validation. Once the key is
/// fixed, every member resumes, and the same cause after that halts the
/// closure again as a new episode, with its own stop.
#[tokio::test]
async fn resuming_a_halted_closure_before_the_key_is_fixed_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = halt_closure_setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;
    let stops = halting_stops(&db.pool).await;

    app.batch_execute(
        "alter table public.users alter column handle type character(8); \
         update public.users set name = 'Annie' where handle = 'ann';",
    )
    .await
    .expect("retype the key and write it");
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(halting_stops(&db.pool).await, stops + 1);

    for target in ["posts_named", "users_copy"] {
        match trellis.apply(&format!("RESUME TRANSFORM {target}")).await {
            Err(TrellisError::Apply(ApplyError::ResumeRefused { reason, .. })) => {
                assert!(
                    reason.to_string().contains("character(8)"),
                    "{target}: {reason}"
                );
            }
            other => panic!("expected the resume of {target} to be refused, got {other:?}"),
        }
        let reported = trellis.status(target).await.expect("status").expect(target);
        assert_eq!(reported.status, TransformStatus::Paused, "{target}");
        assert_eq!(
            reported.capture_failure.expect("the halt record").kind,
            CaptureFailureKind::Halt,
            "{target}"
        );
    }
    trellis
        .apply("RESUME TRANSFORM named_copy")
        .await
        .expect("named_copy reads no key define would refuse");
    assert_eq!(halting_stops(&db.pool).await, stops + 1);

    // Once the key is fixed the members resume, and the same cause after
    // that is a new episode, with its own stop.
    app.batch_execute("alter table public.users alter column handle type varchar(16)")
        .await
        .expect("fix the key");
    for target in ["posts_named", "users_copy"] {
        trellis
            .apply(&format!("RESUME TRANSFORM {target}"))
            .await
            .unwrap_or_else(|err| panic!("resume {target}: {err}"));
    }
    bring_live(&mut raw, &db.pool, &HALT_CLOSURE).await;
    app.batch_execute(
        "alter table public.users alter column handle type character(8); \
         update public.users set name = 'Ann again' where handle = 'ann';",
    )
    .await
    .expect("retype the key again and write it");
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        halting_stops(&db.pool).await,
        stops + 2,
        "the second halt is a new episode"
    );
    for target in HALT_CLOSURE {
        assert_eq!(
            status(&raw, target).await,
            TransformStatus::Paused,
            "{target}"
        );
    }
}

/// Two workers draining different buckets of one page both meet
/// the halt. Whichever pauses the closure first records the stop; the other
/// finds it frozen, records none, and drains its share too. And a halt on a
/// closure already paused pauses and records nothing.
#[tokio::test]
async fn peers_halting_on_one_closure_record_one_stop() {
    use trellis::staging::apply::{self, ApplyError};
    use trellis::staging::{MIN_ROWS_TO_SPLIT, halt_closure, seal};
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let _trellis = halt_closure_setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;
    let stops = halting_stops(&db.pool).await;

    app.batch_execute(&format!(
        "alter table public.users alter column handle type character(8); \
         insert into public.users select 'u' || n, 'U' || n \
         from generate_series(1, {}) n;",
        MIN_ROWS_TO_SPLIT * 2
    ))
    .await
    .expect("retype the key, and write enough rows to split the page");
    let sealed = seal::seal_phase1(&mut raw).await.expect("seal phase 1");
    seal::seal_phase2(&raw, sealed.sealed_seg_seq, "capture_schema_change_wake")
        .await
        .expect("seal phase 2");
    let drain = |claimed_by: &'static str| {
        let pool = db.pool.clone();
        async move {
            let mut shares = 0;
            while apply::drain_once(
                &pool,
                sealed.sealed_seg_seq,
                claimed_by,
                2,
                "trellis_capture_schema_change_test",
            )
            .await?
            .is_some()
            {
                shares += 1;
            }
            Ok::<_, ApplyError>(shares)
        }
    };
    let (a, b) = tokio::join!(drain("worker-a"), drain("worker-b"));
    let (a, b) = (a.expect("worker-a drains"), b.expect("worker-b drains"));
    assert!(a > 0 && b > 0, "each worker drained a share: {a}, {b}");
    let seg = raw
        .query_one(
            "select state from segments where seg_seq = $1",
            &[&sealed.sealed_seg_seq],
        )
        .await
        .expect("read the segment");
    let state: String = seg.get(0);
    assert_eq!(state, "drained", "both shares drained");
    for target in HALT_CLOSURE {
        assert_eq!(
            status(&raw, target).await,
            TransformStatus::Paused,
            "{target}"
        );
    }
    assert_eq!(
        halting_stops(&db.pool).await,
        stops + 1,
        "one stop for both"
    );

    let again = ApplyError::from(trellis::defs::ddl::DdlError::UnsupportedPrimaryKeyType {
        source_table: "public.users".to_string(),
        column: "handle".to_string(),
        pg_type: "character".to_string(),
    });
    let paused = halt_closure(&db.pool, &again).await.expect("halt again");
    assert!(
        paused.is_empty(),
        "the closure is already paused: {paused:?}"
    );
    assert_eq!(halting_stops(&db.pool).await, stops + 1, "no stop for it");
}
