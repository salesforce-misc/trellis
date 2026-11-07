//! A resume of an aggregate whose source's key changed while it was paused
//! re-records the key for the table, for every reader of it, and rebuilds.
//! The record is the table's and is tied to the relation it was taken from:
//! a table dropped and created again has no record until a define or a
//! resume takes its key, whatever definitions of the old table remain.
//!
//! The source recreated with another key, for each kind of reader, is in
//! `resume_after_source_key_change.rs`. Every pass, build and drain is
//! stepped by hand. Nothing polls for convergence.

#[path = "support/resume_passes.rs"]
mod resume_passes;

use resume_passes::{
    RECREATED, SOURCE, bring_live, capture_pass, drain_to_quiescence, full_pass, rows, setup,
    status,
};
use testkit::TestCluster;
use trellis::defs::TransformStatus;

const SUMS: &str =
    "TRANSFORM c_sums FROM public.c GROUP BY pid WITHOUT SOURCE INDEX SELECT SUM(amount) AS total";
const SUMS_ACTUAL: &str = "select pid::text, total::text from public.c_sums order by 1";
const SUMS_ORACLE: &str =
    "select pid::text, sum(amount)::text from public.c group by pid order by 1";

const COUNTS: &str =
    "TRANSFORM c_counts FROM public.c GROUP BY pid WITHOUT SOURCE INDEX SELECT COUNT(*) AS n";
const COUNTS_ACTUAL: &str = "select pid::text, n::text from public.c_counts order by 1";
const COUNTS_ORACLE: &str =
    "select pid::text, count(*)::text from public.c group by pid order by 1";

/// `public.c`'s recorded key.
async fn recorded(raw: &tokio_postgres::Client) -> Vec<Vec<Option<String>>> {
    rows(
        raw,
        "select key_columns::text from trellis.definition_keys where table_name = 'public.c'",
    )
    .await
}

/// The source's primary key redefined in place, with a change staged under
/// the old key: the table keeps its oid, and its record has no index. The
/// resume takes the new primary key and rebuilds.
#[tokio::test]
async fn an_aggregate_resumes_onto_a_primary_key_redefined_in_place() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, SOURCE, &[SUMS]).await;
    raw.batch_execute("insert into public.c values (100, 1, 5000)")
        .await
        .unwrap();
    raw.batch_execute(
        "alter table public.c drop constraint c_pkey, add primary key (id, pid); \
         insert into public.c values (100, 2, 7)",
    )
    .await
    .unwrap();
    full_pass(&mut raw, &db.pool).await;
    assert_eq!(
        status(&trellis, "c_sums").await.status,
        TransformStatus::Paused
    );
    trellis.apply("RESUME TRANSFORM c_sums").await.unwrap();
    assert_eq!(recorded(&raw).await, [[Some("{id,pid}".to_string())]]);
    bring_live(&mut raw, &db.pool, &trellis, &["c_sums"]).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
    raw.batch_execute(
        "insert into public.c values (100, 0, 3); update public.c set amount = 9 \
         where id = 100 and pid = 1; delete from public.c where id = 2",
    )
    .await
    .unwrap();
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
}

/// Two aggregates over one source, which is created again with another key:
/// the one defined second resumes first, and runs on the new key, while the
/// other is still paused on the old table's. That one then resumes onto the
/// key already recorded.
#[tokio::test]
async fn one_reader_s_resume_re_records_the_key_for_every_reader() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, SOURCE, &[SUMS, COUNTS]).await;
    raw.batch_execute(RECREATED).await.unwrap();
    capture_pass(&mut raw, &db.pool).await;
    for target in ["c_sums", "c_counts"] {
        assert_eq!(
            status(&trellis, target).await.status,
            TransformStatus::Paused
        );
    }
    trellis.apply("RESUME TRANSFORM c_counts").await.unwrap();
    assert_eq!(recorded(&raw).await, [[Some("{pid,id}".to_string())]]);
    bring_live(&mut raw, &db.pool, &trellis, &["c_counts"]).await;
    assert_eq!(
        rows(&raw, COUNTS_ACTUAL).await,
        rows(&raw, COUNTS_ORACLE).await
    );
    raw.batch_execute("insert into public.c values (4, 8, 1); delete from public.c where id = 1")
        .await
        .unwrap();
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        rows(&raw, COUNTS_ACTUAL).await,
        rows(&raw, COUNTS_ORACLE).await
    );
    assert_eq!(
        status(&trellis, "c_sums").await.status,
        TransformStatus::Paused
    );

    trellis.apply("RESUME TRANSFORM c_sums").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["c_sums", "c_counts"]).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
    raw.batch_execute("insert into public.c values (5, 7, 2)")
        .await
        .unwrap();
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
    assert_eq!(
        rows(&raw, COUNTS_ACTUAL).await,
        rows(&raw, COUNTS_ORACLE).await
    );
}

/// A define over a table dropped and created again, while a definition of
/// the old one is still paused: the new table's key, not the old table's
/// record. The paused definition then resumes onto it.
#[tokio::test]
async fn a_define_over_a_recreated_table_takes_the_new_table_s_key() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, SOURCE, &[SUMS]).await;
    raw.batch_execute(RECREATED).await.unwrap();
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        status(&trellis, "c_sums").await.status,
        TransformStatus::Paused
    );
    trellis
        .apply("TRANSFORM c_copy FROM public.c SELECT amount AS amount")
        .await
        .expect("a define over the new table");
    assert_eq!(recorded(&raw).await, [[Some("{pid,id}".to_string())]]);
    assert_eq!(
        rows(
            &raw,
            "select pg_catalog.pg_get_constraintdef(oid) from pg_catalog.pg_constraint \
             where conrelid = 'public.c_copy'::regclass and contype = 'p'"
        )
        .await,
        [[Some("PRIMARY KEY (pid, id)".to_string())]]
    );
    // The new reader is paused with the old one for the capture the drop
    // removed, and resumes with it.
    full_pass(&mut raw, &db.pool).await;
    trellis.apply("RESUME TRANSFORM c_copy").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["c_copy"]).await;
    let copy_oracle = "select pid::text, id::text, amount::text from public.c order by 1, 2";
    let copy_actual = "select pid::text, id::text, amount::text from public.c_copy order by 1, 2";
    assert_eq!(rows(&raw, copy_actual).await, rows(&raw, copy_oracle).await);

    trellis.apply("RESUME TRANSFORM c_sums").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["c_sums", "c_copy"]).await;
    raw.batch_execute("insert into public.c values (4, 8, 1); delete from public.c where id = 1")
        .await
        .unwrap();
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
    assert_eq!(rows(&raw, copy_actual).await, rows(&raw, copy_oracle).await);
}

/// A source created again with no primary key, only a unique index: the
/// resume doesn't take the index as the key, and refuses, naming the
/// remedy. With a primary key added, it resumes.
#[tokio::test]
async fn a_resume_takes_no_unique_index_as_a_changed_key() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, SOURCE, &[SUMS]).await;
    raw.batch_execute(
        "drop table public.c; \
         create table public.c (id int not null, pid int not null, amount int, unique (pid, id)); \
         create index on public.c (pid); \
         insert into public.c values (1, 7, 40), (2, 7, 60), (3, 8, 1000)",
    )
    .await
    .unwrap();
    full_pass(&mut raw, &db.pool).await;
    assert_eq!(
        status(&trellis, "c_sums").await.status,
        TransformStatus::Paused
    );
    let refusal = trellis
        .apply("RESUME TRANSFORM c_sums")
        .await
        .expect_err("no primary key to take")
        .to_string();
    for part in [
        "the key of table 'public.c' changed since Trellis recorded it (id)",
        "c_pid_id_key (pid, id)",
        "Add a primary key",
    ] {
        assert!(refusal.contains(part), "{part:?} in {refusal}");
    }
    assert_eq!(
        status(&trellis, "c_sums").await.status,
        TransformStatus::Paused
    );
    assert_eq!(recorded(&raw).await, [[Some("{id}".to_string())]]);

    raw.batch_execute("alter table public.c add primary key (id)")
        .await
        .unwrap();
    trellis.apply("RESUME TRANSFORM c_sums").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["c_sums"]).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
}

const COPY: &str = "TRANSFORM c_copy FROM public.c SELECT amount AS amount";

/// The primary key redefined in place while one reader is paused for
/// another reason (an operator's `PAUSE`) and the others run, and that
/// reader resumed before any pass sees the change: its resume re-records
/// the key for the table, and pauses the readers still running, which are
/// keyed by the old one. Otherwise the pass, which takes a recorded primary
/// key as every reader's, regenerates capture over the new key under them.
#[tokio::test]
async fn a_re_record_pauses_the_readers_still_running_on_the_old_key() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, SOURCE, &[SUMS, COPY, COUNTS]).await;
    trellis.apply("PAUSE TRANSFORM c_counts").await.unwrap();
    raw.batch_execute("alter table public.c drop constraint c_pkey, add primary key (id, pid)")
        .await
        .unwrap();
    trellis.apply("RESUME TRANSFORM c_counts").await.unwrap();
    assert_eq!(recorded(&raw).await, [[Some("{id,pid}".to_string())]]);
    for target in ["c_sums", "c_copy"] {
        let status = status(&trellis, target).await;
        assert_eq!(status.status, TransformStatus::Paused, "{target}");
        let reason = status.capture_failure.expect("a reason").error;
        assert!(
            reason.contains("changed from (\"id\") to (\"id\", \"pid\")"),
            "{reason}"
        );
    }
    bring_live(&mut raw, &db.pool, &trellis, &["c_counts"]).await;
    raw.batch_execute("insert into public.c values (100, 2, 7); delete from public.c where id = 1")
        .await
        .unwrap();
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        rows(&raw, COUNTS_ACTUAL).await,
        rows(&raw, COUNTS_ORACLE).await
    );

    // The aggregate resumes onto the recorded key; the 1-1 too, its target
    // re-keyed in place to it, `pid` added as a key column.
    trellis.apply("RESUME TRANSFORM c_sums").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["c_sums", "c_counts"]).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
    trellis.apply("RESUME TRANSFORM c_copy").await.unwrap();
    bring_live(
        &mut raw,
        &db.pool,
        &trellis,
        &["c_copy", "c_sums", "c_counts"],
    )
    .await;
    assert_eq!(
        rows(
            &raw,
            "select id::text, pid::text, amount::text from public.c_copy order by 1, 2"
        )
        .await,
        rows(
            &raw,
            "select id::text, pid::text, amount::text from public.c order by 1, 2"
        )
        .await
    );
}

/// The primary key moved in place to other columns while a unique index
/// over the old key's columns is left: capture keys the table by its primary
/// key, so the resume takes it rather than keep the record because an index
/// still covers it, and the next pass doesn't pause the aggregate again.
#[tokio::test]
async fn an_aggregate_resumes_onto_a_primary_key_moved_beside_the_old_key_s_index() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, SOURCE, &[SUMS]).await;
    raw.batch_execute(
        "alter table public.c add column u int; update public.c set u = id * 10; \
         alter table public.c alter column u set not null; \
         create unique index c_id_key on public.c (id); \
         alter table public.c drop constraint c_pkey, add primary key (u)",
    )
    .await
    .unwrap();
    full_pass(&mut raw, &db.pool).await;
    assert_eq!(
        status(&trellis, "c_sums").await.status,
        TransformStatus::Paused
    );
    trellis.apply("RESUME TRANSFORM c_sums").await.unwrap();
    assert_eq!(recorded(&raw).await, [[Some("{u}".to_string())]]);
    bring_live(&mut raw, &db.pool, &trellis, &["c_sums"]).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
    raw.batch_execute(
        "insert into public.c values (100, 2, 7, 1000); delete from public.c where id = 1; \
         update public.c set amount = 50 where id = 2",
    )
    .await
    .unwrap();
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        status(&trellis, "c_sums").await.status,
        TransformStatus::Live
    );
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
}

/// A resume the re-validation refuses after the key was re-recorded (the
/// recreated source lacks a column the aggregate reads) keeps none of the
/// re-keying: the record is still the old table's.
#[tokio::test]
async fn a_refused_resume_keeps_the_old_record() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, SOURCE, &[SUMS]).await;
    raw.batch_execute(
        "drop table public.c; \
         create table public.c (id int not null, pid int not null, primary key (pid, id)); \
         insert into public.c values (1, 7)",
    )
    .await
    .unwrap();
    full_pass(&mut raw, &db.pool).await;
    trellis
        .apply("RESUME TRANSFORM c_sums")
        .await
        .expect_err("amount is gone");
    assert_eq!(recorded(&raw).await, [[Some("{id}".to_string())]]);
    assert_eq!(
        status(&trellis, "c_sums").await.status,
        TransformStatus::Paused
    );
}
