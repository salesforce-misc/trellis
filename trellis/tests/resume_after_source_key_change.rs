//! A source whose primary key changed while its readers were paused:
//! dropped and created again with another key. The keys Trellis recorded or
//! staged under the old key are discarded by the resume, never decoded
//! against the new one, and the rebuild reads the table now at the name.
//!
//! The aggregate and the 1-1 target, which the resume re-keys in place to
//! the new key, and the transforms chained off it; a `JOIN` is in
//! `group_basis_resume_after_source_key_change.rs`. Every pass, build and
//! drain is stepped by hand.
//! Nothing polls for convergence.

#[path = "support/resume_passes.rs"]
mod resume_passes;

use resume_passes::{
    RECREATED, SOURCE, assert_staged, bring_live, capture_pass, drain_to_quiescence, rows, setup,
    status,
};
use testkit::TestCluster;
use trellis::defs::TransformStatus;

const SUMS: &str =
    "TRANSFORM c_sums FROM public.c GROUP BY pid WITHOUT SOURCE INDEX SELECT SUM(amount) AS total";
const SUMS_ACTUAL: &str = "select pid::text, total::text from public.c_sums order by 1";
const SUMS_ORACLE: &str =
    "select pid::text, sum(amount)::text from public.c group by pid order by 1";

/// An aggregate's source dropped and created again with a
/// two-column key, with a change staged under the old key and not yet
/// drained. The resume rebuilds the target from the new table, and the new
/// table's changes apply after it.
#[tokio::test]
async fn an_aggregate_resumes_onto_a_source_recreated_with_a_wider_key() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, SOURCE, &[SUMS]).await;
    // Staged under the old key, and left in the ring.
    raw.batch_execute("insert into public.c values (100, 1, 5000)")
        .await
        .unwrap();
    raw.batch_execute(RECREATED).await.unwrap();
    capture_pass(&mut raw, &db.pool).await;
    assert_staged(&raw).await;
    assert_eq!(
        status(&trellis, "c_sums").await.status,
        TransformStatus::Paused
    );
    trellis.apply("RESUME TRANSFORM c_sums").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["c_sums"]).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
    raw.batch_execute("insert into public.c values (4, 8, 1); delete from public.c where id = 1")
        .await
        .unwrap();
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
}

/// The reverse: a two-column key narrowed to one. A key staged under the
/// old key splits into no part of the new one, and would cast as the whole
/// key text, so nothing may decode it.
#[tokio::test]
async fn an_aggregate_resumes_onto_a_source_recreated_with_a_narrower_key() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(
        &cluster,
        "create table public.c (id int not null, pid int not null, amount int, \
             primary key (pid, id)); \
         create index on public.c (pid); \
         insert into public.c select i, i % 3, i from generate_series(1, 6) i",
        &[SUMS],
    )
    .await;
    raw.batch_execute("insert into public.c values (100, 1, 5000)")
        .await
        .unwrap();
    raw.batch_execute(
        "drop table public.c; \
         create table public.c (id int primary key, pid int not null, amount int); \
         create index on public.c (pid); \
         insert into public.c values (1, 7, 40), (2, 7, 60), (3, 8, 1000)",
    )
    .await
    .unwrap();
    capture_pass(&mut raw, &db.pool).await;
    assert_staged(&raw).await;
    trellis.apply("RESUME TRANSFORM c_sums").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["c_sums"]).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
}

/// The same columns in the other order: every old key still splits into as
/// many parts as the new key has, and its parts would bind to the wrong
/// columns (`'a'` as an `integer`).
#[tokio::test]
async fn an_aggregate_resumes_onto_a_source_whose_key_columns_were_reordered() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(
        &cluster,
        "create table public.c (id int not null, tag text not null, pid int not null, \
             amount int, primary key (id, tag)); \
         create index on public.c (pid); \
         insert into public.c values (1, 'a', 7, 1), (2, 'b', 1, 2), (3, 'c', 8, 3)",
        &[SUMS],
    )
    .await;
    raw.batch_execute("insert into public.c values (4, 'd', 2, 5000)")
        .await
        .unwrap();
    raw.batch_execute(
        "drop table public.c; \
         create table public.c (id int not null, tag text not null, pid int not null, \
             amount int, primary key (tag, id)); \
         create index on public.c (pid); \
         insert into public.c values (1, 'a', 1, 40), (5, 'e', 7, 60), (3, 'c', 8, 1000)",
    )
    .await
    .unwrap();
    capture_pass(&mut raw, &db.pool).await;
    assert_staged(&raw).await;
    trellis.apply("RESUME TRANSFORM c_sums").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["c_sums"]).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
}

const COPY: &str = "TRANSFORM c_copy FROM public.c SELECT amount AS amount";
const COPY_ACTUAL: &str =
    "select pid::text, id::text, amount::text from public.c_copy order by 1, 2";
const COPY_ORACLE: &str = "select pid::text, id::text, amount::text from public.c order by 1, 2";

/// `table`'s key constraint, as Postgres prints it.
async fn key_of(raw: &tokio_postgres::Client, table: &str) -> Vec<Vec<Option<String>>> {
    rows(
        raw,
        &format!(
            "select pg_get_constraintdef(oid) from pg_constraint \
             where conrelid = '{table}'::regclass and contype in ('p', 'u')"
        ),
    )
    .await
}

/// `table`'s columns in order, each with whether it is `NOT NULL`.
async fn columns_of(raw: &tokio_postgres::Client, table: &str) -> Vec<Vec<Option<String>>> {
    rows(
        raw,
        &format!(
            "select attname::text, attnotnull::text from pg_attribute \
             where attrelid = '{table}'::regclass and attnum > 0 and not attisdropped \
             order by attnum"
        ),
    )
    .await
}

/// A 1-1 target is keyed by its source's key. Its source created again with
/// the wider key `(pid, id)`, `pid` a column the definition doesn't select,
/// with a change staged under the old key: the resume adds `pid` to the
/// target as a key column, replaces the primary key in place (the table,
/// the user's index and grant stay), and rebuilds, so the target converges
/// on the new table and its later changes apply. Before the fix the resume
/// refused with `SourceKeyChanged`.
#[tokio::test]
async fn a_one_to_one_resume_adds_a_new_key_column_it_does_not_select() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, SOURCE, &[COPY]).await;
    raw.batch_execute(
        "create index c_copy_amount on public.c_copy (amount); \
         grant select on public.c_copy to public",
    )
    .await
    .unwrap();
    let oid = rows(&raw, "select 'public.c_copy'::regclass::oid::text").await;
    raw.batch_execute("insert into public.c values (100, 1, 5000)")
        .await
        .unwrap();
    raw.batch_execute(RECREATED).await.unwrap();
    capture_pass(&mut raw, &db.pool).await;
    assert_staged(&raw).await;
    assert_eq!(
        status(&trellis, "c_copy").await.status,
        TransformStatus::Paused
    );
    trellis.apply("RESUME TRANSFORM c_copy").await.unwrap();
    assert_eq!(
        key_of(&raw, "public.c_copy").await,
        [[Some("PRIMARY KEY (pid, id)".into())]]
    );
    bring_live(&mut raw, &db.pool, &trellis, &["c_copy"]).await;
    assert_eq!(rows(&raw, COPY_ACTUAL).await, rows(&raw, COPY_ORACLE).await);
    raw.batch_execute(
        "insert into public.c values (4, 8, 1), (1, 8, 2); delete from public.c where id = 2; \
         update public.c set amount = 41 where id = 1 and pid = 7",
    )
    .await
    .unwrap();
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(rows(&raw, COPY_ACTUAL).await, rows(&raw, COPY_ORACLE).await);
    assert_eq!(
        rows(&raw, "select 'public.c_copy'::regclass::oid::text").await,
        oid,
        "the same table"
    );
    assert_eq!(
        rows(
            &raw,
            "select indexrelid::regclass::text from pg_index \
             where indrelid = 'public.c_copy'::regclass and not indisprimary"
        )
        .await,
        [[Some("c_copy_amount".into())]],
        "the user's index stays"
    );
    assert_eq!(
        rows(
            &raw,
            "select has_table_privilege('public', 'public.c_copy', 'select')::text"
        )
        .await,
        [[Some("true".into())]],
        "the grant stays"
    );
}

/// The source key narrowed from `(pid, id)` to `(id)`: the former key column
/// `pid`, which the definition doesn't create, is dropped from the target,
/// and the target converges, though it held two rows of `id` 1 under the
/// old key (the source's other one deleted while paused, so that `(id)` can
/// be its key). A field reading `pid` under another name stays.
/// `ALTER TRANSFORM … ADD pid AS pid` once it is live, as the pause message
/// says, brings the column back as a field holding the source's values.
#[tokio::test]
async fn a_one_to_one_resume_drops_a_former_key_column_it_does_not_create() {
    let cluster = TestCluster::start();
    for (definition, columns) in [
        (COPY, &["id", "amount"][..]),
        (
            "TRANSFORM c_copy FROM public.c SELECT amount AS amount, pid AS p",
            &["id", "amount", "p"][..],
        ),
    ] {
        let (db, mut raw, trellis) = setup(
            &cluster,
            "create table public.c (id int not null, pid int not null, amount int, \
                 primary key (pid, id)); \
             insert into public.c select i, i % 3, i from generate_series(1, 6) i; \
             insert into public.c values (1, 5, 50)",
            &[definition],
        )
        .await;
        trellis.apply("PAUSE TRANSFORM c_copy").await.unwrap();
        raw.batch_execute(
            "delete from public.c where id = 1 and pid = 1; \
             alter table public.c drop constraint c_pkey, add primary key (id)",
        )
        .await
        .unwrap();
        assert_eq!(
            rows(
                &raw,
                "select count(*)::text from public.c_copy where id = 1"
            )
            .await,
            [[Some("2".into())]],
            "the target holds both rows of id 1"
        );
        capture_pass(&mut raw, &db.pool).await;
        assert_eq!(
            status(&trellis, "c_copy").await.status,
            TransformStatus::Paused
        );
        trellis.apply("RESUME TRANSFORM c_copy").await.unwrap();
        assert_eq!(
            key_of(&raw, "public.c_copy").await,
            [[Some("PRIMARY KEY (id)".into())]]
        );
        let names: Vec<Option<String>> = columns_of(&raw, "public.c_copy")
            .await
            .into_iter()
            .map(|row| row[0].clone())
            .collect();
        assert_eq!(
            names,
            columns
                .iter()
                .map(|c| Some(c.to_string()))
                .collect::<Vec<_>>(),
            "{definition}"
        );
        bring_live(&mut raw, &db.pool, &trellis, &["c_copy"]).await;
        raw.batch_execute(
            "insert into public.c values (100, 2, 7); delete from public.c where id = 1; \
             update public.c set pid = 9 where id = 2",
        )
        .await
        .unwrap();
        drain_to_quiescence(&db.pool, &mut raw).await;
        let read = |names: &[&str]| names.join("::text, ") + "::text";
        let source: Vec<&str> = columns
            .iter()
            .map(|c| if *c == "p" { "pid" } else { c })
            .collect();
        assert_eq!(
            rows(
                &raw,
                &format!("select {} from public.c_copy order by 1", read(columns))
            )
            .await,
            rows(
                &raw,
                &format!("select {} from public.c order by 1", read(&source))
            )
            .await,
            "{definition}"
        );
        drop((db, raw, trellis));
    }

    let (db, mut raw, trellis) = setup(
        &cluster,
        "create table public.c (id int not null, pid int not null, amount int, \
             primary key (pid, id)); \
         insert into public.c select i, i % 3, i from generate_series(1, 6) i",
        &[COPY],
    )
    .await;
    raw.batch_execute("alter table public.c drop constraint c_pkey, add primary key (id)")
        .await
        .unwrap();
    capture_pass(&mut raw, &db.pool).await;
    trellis.apply("RESUME TRANSFORM c_copy").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["c_copy"]).await;
    trellis
        .apply("ALTER TRANSFORM c_copy ADD pid AS pid")
        .await
        .unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["c_copy"]).await;
    assert_eq!(
        rows(&raw, COPY_ACTUAL).await,
        rows(&raw, COPY_ORACLE).await,
        "pid is back, as a field"
    );
}

/// The resume refuses, naming the conflict, and leaves the target as it
/// was: a new key column's name taken by a column of the target that isn't
/// a copy of it (the definition's field of that name, or a column the user
/// added), a user's view over a former key column it would drop, and a
/// user's foreign key referencing the key constraint it replaces. Dropping
/// the object lets the resume go on.
#[tokio::test]
async fn a_one_to_one_resume_refuses_what_re_keying_would_take() {
    // A field named like the new key column.
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(
        &cluster,
        "create table public.c (id int primary key, amount int); \
         insert into public.c select i, i from generate_series(1, 6) i",
        &["TRANSFORM c_copy FROM public.c SELECT amount + 1 AS pid"],
    )
    .await;
    raw.batch_execute(RECREATED).await.unwrap();
    capture_pass(&mut raw, &db.pool).await;
    let err = trellis
        .apply("RESUME TRANSFORM c_copy")
        .await
        .expect_err("the field has the key column's name")
        .to_string();
    assert!(
        err.contains("adds its key column \"pid\"") && err.contains("field \"pid\""),
        "{err}"
    );
    assert_eq!(
        key_of(&raw, "public.c_copy").await,
        [[Some("PRIMARY KEY (id)".into())]]
    );
    drop((db, raw, trellis));

    // A column the user added.
    let (db, mut raw, trellis) = setup(&cluster, SOURCE, &[COPY]).await;
    raw.batch_execute("alter table public.c_copy add column pid int")
        .await
        .unwrap();
    raw.batch_execute(RECREATED).await.unwrap();
    capture_pass(&mut raw, &db.pool).await;
    let err = trellis
        .apply("RESUME TRANSFORM c_copy")
        .await
        .expect_err("the user's column has the key column's name")
        .to_string();
    assert!(
        err.contains("adds its key column \"pid\"") && err.contains("Trellis didn't create"),
        "{err}"
    );
    raw.batch_execute("alter table public.c_copy drop column pid")
        .await
        .unwrap();
    trellis.apply("RESUME TRANSFORM c_copy").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["c_copy"]).await;
    assert_eq!(rows(&raw, COPY_ACTUAL).await, rows(&raw, COPY_ORACLE).await);
    drop((db, raw, trellis));

    // A view over the former key column the narrowed key drops.
    let (db, mut raw, trellis) = setup(
        &cluster,
        "create table public.c (id int not null, pid int not null, amount int, \
             primary key (pid, id)); \
         insert into public.c select i, i % 3, i from generate_series(1, 6) i",
        &[COPY],
    )
    .await;
    raw.batch_execute(
        "create view public.c_copy_pids as select pid from public.c_copy; \
         alter table public.c drop constraint c_pkey, add primary key (id)",
    )
    .await
    .unwrap();
    capture_pass(&mut raw, &db.pool).await;
    let err = trellis
        .apply("RESUME TRANSFORM c_copy")
        .await
        .expect_err("a view depends on pid")
        .to_string();
    assert!(
        err.contains("\"pid\"") && err.contains("view public.c_copy_pids"),
        "{err}"
    );
    assert_eq!(
        key_of(&raw, "public.c_copy").await,
        [[Some("PRIMARY KEY (pid, id)".into())]]
    );
    raw.batch_execute("drop view public.c_copy_pids")
        .await
        .unwrap();
    trellis.apply("RESUME TRANSFORM c_copy").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["c_copy"]).await;
    assert_eq!(
        rows(
            &raw,
            "select id::text, amount::text from public.c_copy order by 1"
        )
        .await,
        rows(
            &raw,
            "select id::text, amount::text from public.c order by 1"
        )
        .await
    );
    drop((db, raw, trellis));

    // A user's foreign key referencing the key constraint the re-keying
    // replaces.
    let (db, mut raw, trellis) = setup(&cluster, SOURCE, &[COPY]).await;
    raw.batch_execute(
        "create table public.refs (id int references public.c_copy (id)); \
         insert into public.refs values (1)",
    )
    .await
    .unwrap();
    raw.batch_execute(RECREATED).await.unwrap();
    capture_pass(&mut raw, &db.pool).await;
    let err = trellis
        .apply("RESUME TRANSFORM c_copy")
        .await
        .expect_err("a foreign key references the key")
        .to_string();
    assert!(
        err.contains("key constraint \"c_copy_pkey\"") && err.contains("refs_id_fkey"),
        "{err}"
    );
    assert_eq!(
        key_of(&raw, "public.c_copy").await,
        [[Some("PRIMARY KEY (id)".into())]]
    );
    raw.batch_execute("drop table public.refs").await.unwrap();
    trellis.apply("RESUME TRANSFORM c_copy").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["c_copy"]).await;
    assert_eq!(rows(&raw, COPY_ACTUAL).await, rows(&raw, COPY_ORACLE).await);
}

/// A plain source converted to a partitioned one (the routine case): a
/// partitioned table's key must hold the partition key, so the conversion
/// widens it. The resume rebuilds onto the tree, and capture covers the
/// leaves.
const PARTITIONED: &str = "drop table public.c; \
     create table public.c (id int not null, pid int not null, amount int, \
         primary key (pid, id)) partition by list (pid); \
     create table public.c_7 partition of public.c for values in (7); \
     create table public.c_8 partition of public.c for values in (8); \
     create index on public.c (pid); \
     insert into public.c values (1, 7, 40), (2, 7, 60), (3, 8, 1000)";

#[tokio::test]
async fn an_aggregate_resumes_onto_its_source_converted_to_a_partitioned_table() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, SOURCE, &[SUMS]).await;
    raw.batch_execute("insert into public.c values (100, 1, 5000)")
        .await
        .unwrap();
    raw.batch_execute(PARTITIONED).await.unwrap();
    capture_pass(&mut raw, &db.pool).await;
    assert_staged(&raw).await;
    assert_eq!(
        status(&trellis, "c_sums").await.status,
        TransformStatus::Paused
    );
    trellis.apply("RESUME TRANSFORM c_sums").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["c_sums"]).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
    raw.batch_execute(
        "insert into public.c values (4, 8, 1), (5, 7, 2); delete from public.c where id = 1",
    )
    .await
    .unwrap();
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
}

/// An aggregate whose target holds 500 groups over 25,001 source rows when
/// its source is dropped and created again with two groups: the resume's
/// rebuild removes every group the new table lacks, so no old group survives
/// into the rebuilt target.
#[tokio::test]
async fn an_aggregate_of_many_groups_resumes_onto_a_wider_key() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(
        &cluster,
        "create table public.c (id int primary key, pid int not null, amount int); \
         create index on public.c (pid); \
         insert into public.c select i, i % 500, i from generate_series(1, 25001) i",
        &[SUMS],
    )
    .await;
    raw.batch_execute(RECREATED).await.unwrap();
    capture_pass(&mut raw, &db.pool).await;
    trellis.apply("RESUME TRANSFORM c_sums").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["c_sums"]).await;
    assert_eq!(rows(&raw, SUMS_ACTUAL).await, rows(&raw, SUMS_ORACLE).await);
}

/// The routine case, a source converted to a partitioned table, for a 1-1
/// transform: define refuses a partitioned table as a 1-1 transform's
/// source, so its resume refuses as define would, naming the table, and
/// leaves the target and its key as they were, rather than re-key it.
#[tokio::test]
async fn a_one_to_one_resume_onto_its_source_converted_to_a_partitioned_table_refuses() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, SOURCE, &[COPY]).await;
    let before = rows(
        &raw,
        "select id::text, amount::text from public.c_copy order by 1",
    )
    .await;
    raw.batch_execute(PARTITIONED).await.unwrap();
    capture_pass(&mut raw, &db.pool).await;
    let err = trellis
        .apply("RESUME TRANSFORM c_copy")
        .await
        .expect_err("define refuses a partitioned 1-1 source")
        .to_string();
    assert!(
        err.contains("partitioned table public.c can't be the source of a 1-1 transform"),
        "{err}"
    );
    assert_eq!(
        key_of(&raw, "public.c_copy").await,
        [[Some("PRIMARY KEY (id)".into())]]
    );
    assert_eq!(
        rows(
            &raw,
            "select id::text, amount::text from public.c_copy order by 1"
        )
        .await,
        before
    );
    assert_eq!(
        status(&trellis, "c_copy").await.status,
        TransformStatus::Paused
    );
}

/// Transforms chained off a re-keyed 1-1 target, an aggregate and another
/// 1-1: the 1-1's resume re-records its target's key, which pauses them
/// (their source's key changed), and each one's own resume follows the same
/// rules over it: the aggregate rebuilds, the chained 1-1 adds `pid` to its
/// own target's key. Both converge, and so do later changes.
#[tokio::test]
async fn transforms_chained_off_a_re_keyed_one_to_one_target_resume_after_it() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, SOURCE, &[COPY]).await;
    for text in [
        "TRANSFORM c_tot FROM public.c_copy GROUP BY amount WITHOUT SOURCE INDEX \
         SELECT SUM(id) AS ids",
        "TRANSFORM c_copy2 FROM public.c_copy SELECT amount AS amount",
    ] {
        trellis.apply(text).await.expect(text);
    }
    bring_live(&mut raw, &db.pool, &trellis, &["c_tot", "c_copy2"]).await;
    raw.batch_execute(RECREATED).await.unwrap();
    capture_pass(&mut raw, &db.pool).await;
    trellis.apply("RESUME TRANSFORM c_copy").await.unwrap();
    for target in ["c_tot", "c_copy2"] {
        let reported = status(&trellis, target).await;
        assert_eq!(reported.status, TransformStatus::Paused, "{target}");
        let reason = reported.capture_failure.expect("a reason").error;
        assert!(
            reason.contains("public.c_copy changed from (\"id\") to (\"pid\", \"id\")"),
            "{target}: {reason}"
        );
    }
    bring_live(&mut raw, &db.pool, &trellis, &["c_copy"]).await;
    trellis.apply("RESUME TRANSFORM c_tot").await.unwrap();
    trellis.apply("RESUME TRANSFORM c_copy2").await.unwrap();
    assert_eq!(
        key_of(&raw, "public.c_copy2").await,
        [[Some("PRIMARY KEY (pid, id)".into())]]
    );
    let tot_actual = "select amount::text, ids::text from public.c_tot order by 1";
    let tot_oracle = "select amount::text, sum(id)::text from public.c group by amount order by 1";
    let copy2_actual = "select pid::text, id::text, amount::text from public.c_copy2 order by 1, 2";
    bring_live(
        &mut raw,
        &db.pool,
        &trellis,
        &["c_copy", "c_tot", "c_copy2"],
    )
    .await;
    assert_eq!(rows(&raw, tot_actual).await, rows(&raw, tot_oracle).await);
    assert_eq!(
        rows(&raw, copy2_actual).await,
        rows(&raw, COPY_ORACLE).await
    );
    raw.batch_execute(
        "insert into public.c values (4, 8, 40), (1, 8, 2); delete from public.c where id = 2; \
         update public.c set amount = 1000 where id = 1 and pid = 7",
    )
    .await
    .unwrap();
    for _ in 0..3 {
        drain_to_quiescence(&db.pool, &mut raw).await;
    }
    assert_eq!(rows(&raw, COPY_ACTUAL).await, rows(&raw, COPY_ORACLE).await);
    assert_eq!(rows(&raw, tot_actual).await, rows(&raw, tot_oracle).await);
    assert_eq!(
        rows(&raw, copy2_actual).await,
        rows(&raw, COPY_ORACLE).await
    );
}

/// A 1-1 `JOIN`'s `FROM` side recreated with the wider key `(pid, id)`:
/// its resume re-keys the target the same way, adding `pid`, which no field
/// selects, and the rebuild and later changes of either table converge.
#[tokio::test]
async fn a_one_to_one_join_resume_adds_a_new_key_column_of_its_from_side() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(
        &cluster,
        &format!(
            "{SOURCE}; create table public.n (pid int primary key, label text); \
             insert into public.n values (0, 'zero'), (1, 'one'), (7, 'seven'), (8, 'eight')"
        ),
        &[
            "TRANSFORM c_named FROM public.c LEFT JOIN public.n ON c.pid = n.pid \
           SELECT c.amount AS amount, n.label AS label",
        ],
    )
    .await;
    raw.batch_execute(RECREATED).await.unwrap();
    capture_pass(&mut raw, &db.pool).await;
    trellis.apply("RESUME TRANSFORM c_named").await.unwrap();
    assert_eq!(
        key_of(&raw, "public.c_named").await,
        [[Some("PRIMARY KEY (pid, id)".into())]]
    );
    let actual = "select pid::text, id::text, amount::text, label from public.c_named \
                  order by 1, 2";
    let oracle = "select c.pid::text, c.id::text, c.amount::text, n.label from public.c c \
                  left join public.n n on c.pid = n.pid order by 1, 2";
    bring_live(&mut raw, &db.pool, &trellis, &["c_named"]).await;
    assert_eq!(rows(&raw, actual).await, rows(&raw, oracle).await);
    raw.batch_execute(
        "insert into public.c values (4, 8, 1), (1, 8, 2); delete from public.c where id = 2; \
         update public.n set label = 'SEVEN' where pid = 7",
    )
    .await
    .unwrap();
    for _ in 0..3 {
        drain_to_quiescence(&db.pool, &mut raw).await;
    }
    assert_eq!(rows(&raw, actual).await, rows(&raw, oracle).await);
}
