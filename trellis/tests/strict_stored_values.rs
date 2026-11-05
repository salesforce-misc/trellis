//! Strict reads of enum-like stored values (ADR-0024 rule 6): a value a
//! newer Trellis release wrote, which this release doesn't know, pauses the
//! definition it belongs to with a reason naming it, never reads as a value
//! this release knows and never panics. Every other definition keeps
//! running.
//!
//! A test stands in for the newer release: it relaxes the check constraints
//! as that release's migration would, and writes the values it would write.
//!
//! Migration V15 clears the one kind of value an earlier release stored that
//! would read as a tag this release writes: a 1-1 sweep's bare cursor of the
//! tagged shape.

use testkit::TestCluster;
use tokio_postgres::Client;
use trellis::{CaptureFailureKind, Config, ErrorCode, TransformStatus, Trellis, TrellisOptions};

async fn raw(db: &testkit::TestDatabase) -> Client {
    let (client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "set search_path to {}, public",
            trellis::config::DEFAULT_SCHEMA
        ))
        .await
        .expect("search_path");
    client
}

async fn handle(db: &testkit::TestDatabase) -> Trellis {
    Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("config"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect")
}

/// The definitions the newer release wrote a value for, the column it
/// wrote, and the SQL that writes it.
const NEWER: [(&str, &str, &str); 5] = [
    (
        "t_status",
        "transform_definitions.status 'merging'",
        "update transform_definitions set status = 'merging' where id = $1",
    ),
    (
        "t_build",
        "transform_definitions.build 'merging'",
        "update transform_definitions set build = 'merging' where id = $1",
    ),
    (
        "t_chunk",
        "backfill_chunks.kind 'merging'",
        "insert into backfill_chunks (definition_id, kind) values ($1, 'merging')",
    ),
    (
        "t_phase",
        "partition_correction_progress.phase 'ledger'",
        "insert into partition_correction_progress (correction_id, definition_id, phase) \
         values ((select max(id) from partition_corrections), $1, 'ledger')",
    ),
    (
        "t_kind",
        "capture_failures.kind 'relabel'",
        "with paused as (update transform_definitions set status = 'paused' where id = $1) \
         insert into capture_failures (transform_id, source_table, columns, error, kind) \
         values ($1, 'public.s', '{}', 'relabelled by the newer release', 'relabel')",
    ),
];

/// Each value a newer release wrote pauses its own definition at attach,
/// naming the value; `status()` reports it (a status it can't read is
/// refused, a capture failure kind it can't read is `unknown`) and never
/// panics; `RESUME` refuses with `version_floor` while the value is stored;
/// and the definition beside them keeps running.
#[tokio::test]
async fn a_value_a_newer_release_wrote_pauses_its_definition_and_the_rest_keep_running() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    raw.batch_execute(
        "create table public.s (id integer primary key, a integer); \
         insert into public.s values (1, 1), (2, 2)",
    )
    .await
    .expect("source");
    let trellis = handle(&db).await;
    let targets: Vec<&str> = NEWER
        .iter()
        .map(|&(target, ..)| target)
        .chain(["ok"])
        .collect();
    for target in &targets {
        let text = format!("TRANSFORM {target} FROM s SELECT a AS a");
        trellis.apply(&text).await.expect(&text);
    }
    trellis::intake::harness::start_registered_builds(trellis.pool())
        .await
        .expect("dispatch the builds");
    trellis::staging::build::settle_builds(trellis.pool()).await;

    // The newer release's migration relaxes the checks, and it writes the
    // values.
    raw.batch_execute(
        "alter table transform_definitions drop constraint transform_definitions_status_check; \
         alter table transform_definitions drop constraint transform_definitions_build_check; \
         alter table backfill_chunks drop constraint backfill_chunks_kind_check; \
         alter table capture_failures drop constraint capture_failures_kind_check; \
         insert into partition_corrections (table_name, kind, node_oid, node_name, cause) \
         values ('public.s', 'new_leaf', 1, 'public.s_1', 'created')",
    )
    .await
    .expect("relax the checks");
    for &(target, _, write) in &NEWER {
        let id: i64 = raw
            .query_one(
                "select id from transform_definitions \
                 where split_part(target_table, '.', 2) = $1",
                &[&target],
            )
            .await
            .expect("id")
            .get(0);
        raw.execute(write, &[&id]).await.expect(write);
    }

    // Before anything pauses them, a read neither panics nor reads the
    // value as one this release knows.
    let err = trellis
        .status("t_status")
        .await
        .expect_err("a status this release can't read");
    assert_eq!(err.code(), ErrorCode::VersionFloor, "{err}");
    assert!(
        err.to_string()
            .contains("transform_definitions.status 'merging' was written by a newer Trellis"),
        "{err}"
    );
    let kind = trellis
        .status("t_kind")
        .await
        .expect("status")
        .expect("defined")
        .capture_failure
        .expect("its failure")
        .kind;
    assert_eq!(kind, CaptureFailureKind::Unknown);

    // Attaching pauses each, naming the value, and nothing else.
    trellis::migrate::migrate_with_pool(&db.pool, &Config::from_dsn(db.dsn().to_string()).unwrap())
        .await
        .expect("a value a newer release wrote pauses; it doesn't refuse the attach");
    for &(target, value, _) in &NEWER {
        let status = trellis
            .status(target)
            .await
            .expect("status")
            .expect("defined");
        assert_eq!(status.status, TransformStatus::Paused, "{target}");
        let failure = status.capture_failure.expect("a reason");
        assert_eq!(failure.kind, CaptureFailureKind::Capture, "{target}");
        assert!(
            failure.error.starts_with(&format!(
                "this release can't run transform public.{target}: its {value} was written by \
                 a newer Trellis release"
            )),
            "{target}: {}",
            failure.error
        );
    }
    let kept = trellis
        .status("t_kind")
        .await
        .expect("status")
        .expect("defined")
        .capture_failure
        .expect("a reason")
        .error;
    assert!(
        kept.ends_with("It was already paused: relabelled by the newer release"),
        "{kept}"
    );
    assert_eq!(
        trellis
            .status("ok")
            .await
            .expect("status")
            .expect("defined")
            .status,
        TransformStatus::Live
    );

    // RESUME refuses while the value is stored. The pause itself stored
    // `paused` over a status, and `capture` over a kind, so those two have
    // nothing left to refuse on.
    for target in ["t_build", "t_chunk", "t_phase"] {
        let err = trellis
            .apply(&format!("RESUME TRANSFORM {target}"))
            .await
            .expect_err(target);
        assert_eq!(err.code(), ErrorCode::VersionFloor, "{target}: {err}");
        assert!(
            err.to_string().contains("was written by a newer Trellis"),
            "{err}"
        );
    }

    // A second attach finds them recorded and changes nothing.
    let before: Vec<String> = raw
        .query(
            "select transform_id || ' ' || error || ' ' || detected_at::text \
             from capture_failures order by transform_id",
            &[],
        )
        .await
        .expect("failures")
        .iter()
        .map(|row| row.get(0))
        .collect();
    trellis::migrate::migrate_with_pool(&db.pool, &Config::from_dsn(db.dsn().to_string()).unwrap())
        .await
        .expect("attach again");
    let after: Vec<String> = raw
        .query(
            "select transform_id || ' ' || error || ' ' || detected_at::text \
             from capture_failures order by transform_id",
            &[],
        )
        .await
        .expect("failures")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(before, after);
}

/// The definitions a newer release wrote a job cursor for, the tag it
/// carries, and the job's kind and cursor (`backfill_chunks.lo`).
const NEWER_CURSORS: [(&str, &str, &str, &str); 3] = [
    // A 1-1 sweep's tagged cursor.
    (
        "t_sweep",
        "stale",
        "sweep",
        "chr(1) || 'stale' || chr(1) || '7'",
    ),
    // A 1-1 `JOIN`'s plan job and sweep job.
    ("t_join_plan", "z", "plan", "chr(1) || 'z:7'"),
    ("t_join_sweep", "q", "sweep", "chr(1) || 'q'"),
];

/// A job cursor whose tag a newer release wrote pauses its definition at
/// attach, naming the tag, and `RESUME` refuses while it is stored. An
/// untagged cursor is the format every release reads, and pauses nothing: a
/// 1-1 sweep's ledger key that starts with U+0001 but isn't shaped as a tag
/// (as a release before the tags wrote every ledger key), and a plain plan
/// job's boundary, which is never tagged.
#[tokio::test]
async fn a_cursor_tag_a_newer_release_wrote_pauses_its_definition() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    raw.batch_execute(
        "create table public.s (id integer primary key, a integer); \
         insert into public.s values (1, 1), (2, 2); \
         create table public.b (id integer primary key, k integer, img integer); \
         create unique index on public.b (k); \
         insert into public.b values (1, 1, 10)",
    )
    .await
    .expect("sources");
    let trellis = handle(&db).await;
    for text in [
        "TRANSFORM t_sweep FROM s SELECT a AS a",
        "TRANSFORM t_legacy FROM s SELECT a AS a",
        "TRANSFORM t_join_plan FROM public.s LEFT JOIN public.b ON s.a = b.k \
         SELECT s.a AS a, b.img AS img",
        "TRANSFORM t_join_sweep FROM public.s LEFT JOIN public.b ON s.a = b.k \
         SELECT s.a AS a, b.img AS img",
    ] {
        trellis.apply(text).await.expect(text);
    }
    trellis::intake::harness::start_registered_builds(trellis.pool())
        .await
        .expect("dispatch the builds");
    trellis::staging::build::settle_builds(trellis.pool()).await;

    let insert = |kind: &str, cursor: &str| {
        format!(
            "insert into backfill_chunks (definition_id, kind, lo) \
             select id, '{kind}', {cursor} from transform_definitions \
             where split_part(target_table, '.', 2) = $1"
        )
    };
    for &(target, _, kind, cursor) in &NEWER_CURSORS {
        raw.execute(&insert(kind, cursor), &[&target])
            .await
            .expect(target);
    }
    for (kind, cursor) in [
        ("sweep", "chr(1) || '7'"),
        ("sweep", "chr(1) || 'Stale' || chr(1) || '7'"),
        ("plan", "chr(1) || 'z:7'"),
    ] {
        raw.execute(&insert(kind, cursor), &[&"t_legacy"])
            .await
            .expect("an untagged cursor");
    }

    trellis::migrate::migrate_with_pool(&db.pool, &Config::from_dsn(db.dsn().to_string()).unwrap())
        .await
        .expect("a cursor a newer release wrote pauses; it doesn't refuse the attach");
    for &(target, tag, ..) in &NEWER_CURSORS {
        let status = trellis
            .status(target)
            .await
            .expect("status")
            .expect("defined");
        assert_eq!(status.status, TransformStatus::Paused, "{target}");
        let failure = status.capture_failure.expect("a reason");
        assert!(
            failure.error.starts_with(&format!(
                "this release can't run transform public.{target}: its backfill_chunks.lo \
                 cursor tag '{tag}' was written by a newer Trellis release"
            )),
            "{target}: {}",
            failure.error
        );
        let err = trellis
            .apply(&format!("RESUME TRANSFORM {target}"))
            .await
            .expect_err(target);
        assert_eq!(err.code(), ErrorCode::VersionFloor, "{target}: {err}");
    }
    let legacy = trellis
        .status("t_legacy")
        .await
        .expect("status")
        .expect("defined");
    assert_eq!(legacy.status, TransformStatus::Live);
    assert!(legacy.capture_failure.is_none());
}

/// Migration V15 clears the cursor of every unfinished 1-1 sweep that starts
/// with U+0001: a release before the tagged sweep cursors stored a ledger key
/// bare, so one of the tagged shape, a tag this release knows or not, would
/// read as a tag. Every other job keeps its cursor: a sweep's bare key, a
/// finished sweep, another kind of job, and a 1-1 `JOIN`'s sweep, whose
/// cursors are always tagged. The database is put back to before V15, with
/// the jobs an earlier release left, and `migrate` runs it.
#[tokio::test]
async fn migration_v15_restarts_the_sweeps_whose_cursor_reads_as_a_tag() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = raw(&db).await;
    raw.batch_execute(
        "create table public.s (id integer primary key, a integer); \
         insert into public.s values (1, 1), (2, 2); \
         create table public.b (id integer primary key, k integer, img integer); \
         create unique index on public.b (k); \
         insert into public.b values (1, 1, 10)",
    )
    .await
    .expect("sources");
    let trellis = handle(&db).await;
    for text in [
        "TRANSFORM t_plain FROM s SELECT a AS a",
        "TRANSFORM t_join FROM public.s LEFT JOIN public.b ON s.a = b.k \
         SELECT s.a AS a, b.img AS img",
    ] {
        trellis.apply(text).await.expect(text);
    }
    trellis::intake::harness::start_registered_builds(trellis.pool())
        .await
        .expect("dispatch the builds");
    trellis::staging::build::settle_builds(trellis.pool()).await;

    // (definition, kind, cursor, done, whether V15 clears it)
    let jobs = [
        (
            "t_plain",
            "sweep",
            "chr(1) || 'foo' || chr(1) || 'X'",
            false,
            true,
        ),
        (
            "t_plain",
            "sweep",
            "chr(1) || 'ledger' || chr(1) || 'X'",
            false,
            true,
        ),
        (
            "t_plain",
            "sweep",
            "chr(1) || 'orphans' || chr(1) || 'X'",
            false,
            true,
        ),
        (
            "t_plain",
            "sweep",
            "chr(1) || 'orphan-nulls' || chr(1)",
            false,
            true,
        ),
        ("t_plain", "sweep", "chr(1) || '7'", false, true),
        ("t_plain", "sweep", "'X'", false, false),
        (
            "t_plain",
            "sweep",
            "chr(1) || 'ledger' || chr(1) || 'X'",
            true,
            false,
        ),
        (
            "t_plain",
            "plan",
            "chr(1) || 'ledger' || chr(1) || 'X'",
            false,
            false,
        ),
        ("t_join", "sweep", "chr(1) || 'r:X'", false, false),
        ("t_join", "plan", "chr(1) || 'a:X'", false, false),
    ];
    let mut seeded = Vec::new();
    for &(target, kind, cursor, done, cleared) in &jobs {
        let id: i64 = raw
            .query_one(
                &format!(
                    "insert into backfill_chunks (definition_id, kind, lo, done) \
                     select id, '{kind}', {cursor}, $2 from transform_definitions \
                     where split_part(target_table, '.', 2) = $1 returning id"
                ),
                &[&target, &done],
            )
            .await
            .expect(cursor)
            .get(0);
        let stored: String = raw
            .query_one("select lo from backfill_chunks where id = $1", &[&id])
            .await
            .expect("the stored cursor")
            .get(0);
        seeded.push((id, stored, target, kind, done, cleared));
    }
    raw.batch_execute("delete from refinery_schema_history where version = 15")
        .await
        .expect("a database before V15");

    trellis::migrate::migrate_with_pool(&db.pool, &Config::from_dsn(db.dsn().to_string()).unwrap())
        .await
        .expect("migrate runs V15");
    for (id, stored, target, kind, done, cleared) in seeded {
        let lo: Option<String> = raw
            .query_one("select lo from backfill_chunks where id = $1", &[&id])
            .await
            .expect("the job")
            .get(0);
        let expected = (!cleared).then_some(stored.clone());
        assert_eq!(lo, expected, "{target} {kind} done={done} {stored:?}");
    }
    let plain = trellis
        .status("t_plain")
        .await
        .expect("status")
        .expect("defined");
    assert!(
        plain.capture_failure.is_none(),
        "an earlier release's bare key no longer reads as an unknown tag: {:?}",
        plain.capture_failure
    );
}
