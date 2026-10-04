//! A schema change never fails the application's write (#622 C6, acceptance
//! A4).
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
//! convergence (#297).

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::capture::install::{Installed, installed};
use trellis::capture::reconcile;
use trellis::defs::TransformStatus;
use trellis::{Config, Trellis, TrellisOptions};

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
        "capture_schema_change_wake",
        Duration::from_secs(2),
    )
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
    use trellis::staging::{StagedWatermark, apply, has_pending, retire_drained_segments, seal};
    let watermark = StagedWatermark::saturated();
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

/// Claims, runs and finishes every pending backfill chunk: the hand-driven
/// stand-in for a drain worker's build half (as in `alter_transform.rs`).
async fn run_backfill_chunks(pool: &trellis::Pool) {
    use trellis::defs::chunk_queue;
    const CLAIMED_BY: &str = "capture_schema_change_backfill";
    loop {
        let client = pool.get().await.expect("acquire connection");
        let claimed = chunk_queue::claim_chunks(&**client, CLAIMED_BY, 1000)
            .await
            .expect("claim_chunks");
        drop(client);
        if claimed.is_empty() {
            // A plain aggregate is the Re-derive build's (#625 F3, F5).
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
/// - `tsum` sums `b` per `g` (the ledger path, #623 D3a).
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
        "TRANSFORM tsum FROM public.u GROUP BY g SELECT g AS g, SUM(b) AS total",
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
/// that don't read it (one of them on the ledger path) keep converging over
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
    // column stages nothing (#623 D8a).
    app.batch_execute("delete from public.u where uid = 2")
        .await
        .expect("a write after the regeneration");
    assert!(markers(&raw, since).await.is_empty());
    assert_eq!(imaged_keys(&raw, since).await, vec!["2".to_string()]);
}

/// A ring row's key, op, images and group key, as text.
type ImageRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// The images a function writes on its schema-changed branch, built at run
/// time and run with `EXECUTE`, are byte-identical to the static inserts'
/// over the columns that are left, from an application session whose output
/// settings differ from Trellis's (#622 A1's harness, in miniature).
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
        // Group-key values in physical column order, one of them dropped.
        vec!["t".to_string(), "n".to_string(), "gone".to_string()],
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

    // The static rows' `gone` is 7, a value no other column holds, so
    // dropping it from their group keys leaves what the partial rows hold.
    let images: Vec<ImageRow> = raw
        .query(
            &format!(
                "select key, op, (old_image - 'gone' - 'id')::text, \
                        (new_image - 'gone' - 'id')::text, \
                        array_remove(group_key, '7')::text \
                 from ({}) r where change_id > $1 order by change_id",
                ring(
                    "key, op, old_image, new_image, group_key, change_id",
                    "op <> 'schema_changed'"
                )
            ),
            &[&since],
        )
        .await
        .expect("read images")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4)))
        .collect();
    assert_eq!(images.len(), 4, "{images:?}");
    let (before, after) = images.split_at(2);
    for ((k1, op1, old1, new1, gk1), (k2, op2, old2, new2, gk2)) in before.iter().zip(after) {
        assert_eq!((k1.as_str(), k2.as_str()), ("1", "2"));
        assert_eq!(op1, op2);
        assert_eq!(old1, old2, "old images match");
        assert_eq!(new1, new2, "new images match");
        assert_eq!(gk1, gk2, "group keys match");
        assert!(
            gk2.as_deref().is_some_and(|g| g.contains("12345")),
            "{gk2:?}"
        );
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

/// Renaming a to-side column a definition reads through a relationship: the
/// to-side's writes succeed, that reader pauses, and the definitions reading
/// its other column through the same relationship (one 1-1, one grouped by
/// it) keep converging with nothing poisoned. Once the column is back, the
/// resumed reader rebuilds with the to-side rows written meanwhile, whose
/// settled projection the partial images left without the column.
#[tokio::test]
async fn renaming_a_to_side_column_pauses_only_its_readers_through_the_relationship() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.p (id int primary key, name text, tier text); \
         create table public.c (id int primary key, pid int, amount int); \
         insert into public.p select i, 'n' || i, 't' || i from generate_series(1, 3) i; \
         insert into public.c select i, 1 + i % 3, i from generate_series(1, 6) i;",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    for text in [
        "RELATIONSHIP parent FROM c.pid TO p.id",
        "TRANSFORM c_named FROM public.c SELECT amount AS amount, parent.name AS name",
        "TRANSFORM c_tiered FROM public.c SELECT amount AS amount, parent.tier AS tier",
        "TRANSFORM by_tier FROM public.c GROUP BY parent.tier \
         SELECT parent.tier AS tier, COUNT(*) AS n",
    ] {
        trellis.apply(text).await.expect(text);
    }
    let targets = ["c_named", "c_tiered", "by_tier"];
    bring_live(&mut raw, &db.pool, &targets).await;
    let joined = |column: &str| {
        format!(
            "select c.id::text, p.{column} from public.c c \
             left join public.p p on p.id = c.pid order by c.id"
        )
    };
    let app = connect(db.dsn()).await;

    app.batch_execute(
        "alter table public.p rename column name to name2; \
         update public.p set name2 = name2 || 'x', tier = tier || 'y' where id = 1; \
         insert into public.c values (7, 1, 7);",
    )
    .await
    .expect("no write fails after the rename");
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(status(&raw, "c_named").await, TransformStatus::Paused);
    for other in ["c_tiered", "by_tier"] {
        assert_eq!(status(&raw, other).await, TransformStatus::Live, "{other}");
    }
    let poisoned: i64 = raw
        .query_one("select count(*) from poison", &[])
        .await
        .expect("read poison")
        .get(0);
    assert_eq!(
        poisoned, 0,
        "no partial image reached a reader of the column"
    );
    assert_eq!(
        rows(
            &raw,
            "select id::text, tier from public.c_tiered order by id"
        )
        .await,
        rows(&raw, &joined("tier")).await,
    );

    // Narrowed, then written while the column is still gone.
    let outcome = capture_pass(&mut raw, &db.pool).await;
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
    app.batch_execute("update public.p set name2 = name2 || 'z', tier = tier || 'w' where id = 2")
        .await
        .expect("a write after the narrow");
    drain_to_quiescence(&db.pool, &mut raw).await;

    raw.batch_execute("alter table public.p rename column name2 to name")
        .await
        .expect("rename back");
    trellis
        .apply("RESUME TRANSFORM c_named")
        .await
        .expect("resume");
    bring_live(&mut raw, &db.pool, &targets).await;
    app.batch_execute("insert into public.c values (8, 2, 8)")
        .await
        .expect("a write after the resume");
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        rows(
            &raw,
            "select id::text, name from public.c_named order by id"
        )
        .await,
        rows(&raw, &joined("name")).await,
        "the rebuilt reader sees the names written while the column was gone"
    );
    assert_eq!(
        rows(
            &raw,
            "select id::text, tier from public.c_tiered order by id"
        )
        .await,
        rows(&raw, &joined("tier")).await,
    );
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

/// A primary key redefined without a rename, its old column still there
/// (#687). The installed functions go on imaging the old key, so no write
/// marks the change, and without a check the capture pass would regenerate
/// them keyed by the new one, silently. The pass pauses every reader
/// instead, with the old and new key on its status, and leaves the functions
/// for the next pass. A row the old functions staged then drains without
/// reaching a paused reader, and no write fails.
///
/// (A drain that runs before the pass still meets that row while its
/// readers are live, and fails on its key: #703.)
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

/// Resuming a definition while the column it reads is still missing: the
/// next capture pass pauses it again, with the reason on its status, rather
/// than leaving it waiting to backfill and the table's capture failing every
/// pass. The other readers of the table keep applying.
#[tokio::test]
async fn resuming_while_the_column_is_still_missing_pauses_again() {
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

    trellis.apply("RESUME TRANSFORM ta").await.expect("resume");
    assert_eq!(status(&raw, "ta").await, TransformStatus::WaitingToBackfill);
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
        vec!["Mixed".to_string(), "a%s".to_string(), "gone".to_string()],
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
                "select key, op, (old_image - 'gone')::text, (new_image - 'gone')::text, \
                        array_remove(group_key, '7')::text \
                 from ({}) r where change_id > $1 order by change_id",
                ring(
                    "key, op, old_image, new_image, group_key, change_id",
                    "op <> 'schema_changed'"
                )
            ),
            &[&since],
        )
        .await
        .expect("read images")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4)))
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
/// its table (#705). `ta` is edited to read `spare`, a column nothing imaged
/// yet, so its new field waits for a capture imaging `spare`
/// (`column_status.awaiting_capture`) and its field build waits until one
/// does (#625 F8b). Before any pass widens capture, `a` (which only `ta`
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

/// Issue #748: an edit adding a field that reads, by alias, a field still
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

/// `public.users`, keyed by `handle`, is the to-side of `posts.author`;
/// `posts_named` reads a user's name through it, `posts_plain` reads only
/// `posts`, and `public.o` is an unrelated table whose `o_copy` shares the
/// drain's pages with them (#768). All live.
async fn retyped_to_side_setup(dsn: &str, raw: &mut Client, pool: &trellis::Pool) -> Trellis {
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
        "RELATIONSHIP author FROM posts.author TO users.handle",
        "TRANSFORM posts_named FROM public.posts SELECT author.name AS name",
        "TRANSFORM posts_plain FROM public.posts SELECT author AS author",
        "TRANSFORM o_copy FROM public.o SELECT v AS v",
    ] {
        trellis.apply(text).await.expect(text);
    }
    bring_live(raw, pool, &["posts_named", "posts_plain", "o_copy"]).await;
    trellis
}

/// Issue #768: a to-side's primary key retyped off the key allowlist
/// (`character(8)`, whose `::text` drops the padding an image keeps) once
/// every definition reading the table is paused. The drain reads the key of
/// every staged table, and used to fail every page holding a write to this
/// one with `UnsupportedPrimaryKeyType`, though no reader would apply the
/// row: the to-side stays captured while its from-side has any definition
/// (`posts_plain` here, which doesn't read it). Now such a table's rows are
/// dropped like any paused reader's share, and the unrelated table on the
/// same pages keeps converging, through a row write and a `TRUNCATE` alike.
#[tokio::test]
async fn a_to_side_key_retyped_off_the_allowlist_does_not_halt_the_drain_once_its_readers_are_paused()
 {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = retyped_to_side_setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;

    trellis
        .apply("PAUSE TRANSFORM posts_named")
        .await
        .expect("pause the reader");
    app.batch_execute("alter table public.users alter column handle type character(8)")
        .await
        .expect("retype the key");
    let outcome = capture_pass(&mut raw, &db.pool).await;
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);

    app.batch_execute(
        "update public.users set name = 'Annie' where handle = 'ann'; \
         insert into public.users values ('cy', 'Cy'); \
         update public.o set v = 11 where id = 1;",
    )
    .await
    .expect("write the to-side and the unrelated table");
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        rows(&raw, "select id::text, v::text from public.o_copy").await,
        vec![vec![Some("1".to_string()), Some("11".to_string())]],
        "the unrelated table's write drained past the retyped to-side's"
    );

    app.batch_execute("truncate public.users; insert into public.o values (2, 20)")
        .await
        .expect("truncate the to-side");
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        rows(
            &raw,
            "select id::text, v::text from public.o_copy order by id"
        )
        .await,
        vec![
            vec![Some("1".to_string()), Some("11".to_string())],
            vec![Some("2".to_string()), Some("20".to_string())],
        ],
        "a truncate of the retyped to-side drains too"
    );
    assert_eq!(status(&raw, "posts_named").await, TransformStatus::Paused);
    assert_eq!(status(&raw, "posts_plain").await, TransformStatus::Live);
}

/// Issue #768's other half: while a definition still applies a to-side's
/// rows, a primary key retyped off the allowlist halts the drain as before
/// (#703 R2 would pause it in the drain instead). Nothing is dropped for a
/// reader that would apply it.
#[tokio::test]
async fn a_to_side_key_retyped_off_the_allowlist_still_halts_the_drain_while_a_reader_applies() {
    use trellis::staging::{StagedWatermark, apply, seal};
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let _trellis = retyped_to_side_setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;

    app.batch_execute(
        "alter table public.users alter column handle type character(8); \
         update public.users set name = 'Annie' where handle = 'ann';",
    )
    .await
    .expect("retype the key, and write");
    let outcome = seal::seal_phase1(&mut raw).await.expect("seal phase 1");
    seal::seal_phase2(&raw, outcome.sealed_seg_seq, "capture_schema_change_wake")
        .await
        .expect("seal phase 2");
    let err = apply::drain_once(
        &db.pool,
        outcome.sealed_seg_seq,
        "capture_schema_change_test",
        1,
        "trellis_capture_schema_change_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect_err("the drain halts while posts_named applies the to-side");
    assert!(
        err.to_string().contains("character"),
        "the halt names the unsupported key type: {err}"
    );
}

/// Issue #768: the to-side rows skipped while every reader was paused never
/// reached the relationship's settled projection either. Resuming a reader
/// once the key is back on the allowlist refreshes the projection, so rows
/// the resumed reader derives later read the names written meanwhile, not
/// the ones the projection held at the pause.
#[tokio::test]
async fn resuming_a_reader_refreshes_the_projection_its_skipped_to_side_rows_missed() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = retyped_to_side_setup(db.dsn(), &mut raw, &db.pool).await;
    let app = connect(db.dsn()).await;

    trellis
        .apply("PAUSE TRANSFORM posts_named")
        .await
        .expect("pause the reader");
    app.batch_execute(
        "alter table public.users alter column handle type character(8); \
         update public.users set name = 'Annie' where handle = 'ann'; \
         insert into public.users values ('cy', 'Cy'); \
         delete from public.users where handle = 'bob';",
    )
    .await
    .expect("retype the key, and write the to-side");
    drain_to_quiescence(&db.pool, &mut raw).await;

    app.batch_execute("alter table public.users alter column handle type varchar(16)")
        .await
        .expect("retype the key back");
    trellis
        .apply("RESUME TRANSFORM posts_named")
        .await
        .expect("resume");
    bring_live(
        &mut raw,
        &db.pool,
        &["posts_named", "posts_plain", "o_copy"],
    )
    .await;
    app.batch_execute("insert into public.posts values (3, 'ann'), (4, 'cy'), (5, 'bob')")
        .await
        .expect("posts after the resume");
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        rows(
            &raw,
            "select id::text, name from public.posts_named order by id"
        )
        .await,
        rows(
            &raw,
            "select p.id::text, u.name from public.posts p \
             left join public.users u on u.handle = p.author order by p.id"
        )
        .await,
        "the resumed reader reads the to-side as it is now"
    );
}
