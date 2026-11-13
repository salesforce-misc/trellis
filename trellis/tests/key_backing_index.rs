//! The capture pass's check of the unique index backing a declared
//! `KEY (...)` (#1708 rule 3, ADR-0026): with no valid index backing the key,
//! every reader of the table pauses on the next pass, with no grace and
//! whether or not a write arrived, and the message names the remedy. The
//! match is by shape (#1744's study), so routine index maintenance that keeps
//! an equivalent valid index at every committed instant pauses nothing.
//!
//! Every pass and drain is stepped by hand. Nothing polls for convergence.

#[path = "support/attver.rs"]
mod attver;
#[path = "support/resume_passes.rs"]
mod resume_passes;

use resume_passes::{
    SCHEMA, bring_live, connect, drain_to_quiescence, full_pass, rows, setup, status,
};
use testkit::TestCluster;
use trellis::defs::TransformStatus;

/// A source with no primary key, keyed by `KEY (k1, k2)` through `s_key`,
/// an index written in the other order.
const KEYLESS: &str = "create table public.s (k1 int not null, k2 int not null, g int, v int); \
     create unique index s_key on public.s (k2, k1); \
     insert into public.s select i, i * 10, i % 2, i from generate_series(1, 6) i";

const ONE: &str = "TRANSFORM s_one FROM public.s KEY (k1, k2) SELECT v AS v";
const AGG: &str = "TRANSFORM s_sums FROM public.s KEY (k1, k2) GROUP BY g WITHOUT SOURCE INDEX \
     SELECT COUNT(*) AS n, SUM(v) AS total";

async fn converged(raw: &tokio_postgres::Client) {
    assert_eq!(
        rows(
            raw,
            "select k1::text, k2::text, v::text from public.s_one order by k1, k2"
        )
        .await,
        rows(
            raw,
            "select k1::text, k2::text, v::text from public.s order by k1, k2"
        )
        .await
    );
    assert_eq!(
        rows(
            raw,
            "select g::text, n::text, total::text from public.s_sums order by g"
        )
        .await,
        rows(
            raw,
            "select g::text, count(*)::text, sum(v)::text from public.s group by g order by g"
        )
        .await
    );
}

async fn assert_live(trellis: &trellis::Trellis, targets: &[&str], step: &str) {
    for target in targets {
        let reported = status(trellis, target).await;
        assert_eq!(reported.capture_failure, None, "{target} after {step}");
        assert_eq!(
            reported.status,
            TransformStatus::Live,
            "{target} after {step}"
        );
    }
}

/// The message both readers carry once `s_key` is gone.
const UNBACKED: &str = "no valid unique index backs KEY (k1, k2) on public.s any more, so \
     public.s can take two rows with one key, which this definition's rows are keyed by. Create \
     one with `create unique index concurrently on \"public\".\"s\" (\"k1\", \"k2\")`, then resume the definition \
     to rebuild it. To replace the index backing a KEY, build the replacement before dropping the \
     old index";

/// The backing index dropped, with no write after it: the next pass pauses
/// every reader with the message and its `create unique index`. `RESUME`
/// refuses while none backs the key; once one does, the pass's refresh says
/// the cause is gone and what `RESUME` will do, and `RESUME` rebuilds.
#[tokio::test]
async fn a_dropped_key_index_pauses_every_reader_on_the_next_pass() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, KEYLESS, &[ONE, AGG]).await;
    raw.batch_execute("drop index public.s_key").await.unwrap();
    full_pass(&mut raw, &db.pool).await;
    for target in ["s_one", "s_sums"] {
        let reported = status(&trellis, target).await;
        assert_eq!(reported.status, TransformStatus::Paused, "{target}");
        let failure = reported.capture_failure.expect("a capture failure");
        assert_eq!(failure.source_table, "public.s");
        assert_eq!(failure.columns, ["k1", "k2"]);
        assert_eq!(failure.reasons, [UNBACKED], "{target}");
    }
    let refusal = trellis
        .apply("RESUME TRANSFORM s_sums")
        .await
        .expect_err("no index backs the key")
        .to_string();
    assert!(
        refusal.contains("no unique index backs KEY (k1, k2) on 'public.s'"),
        "{refusal}"
    );

    raw.batch_execute("create unique index s_key on public.s (k1, k2)")
        .await
        .unwrap();
    full_pass(&mut raw, &db.pool).await;
    let failure = status(&trellis, "s_sums")
        .await
        .capture_failure
        .expect("still paused");
    assert_eq!(failure.reasons, Vec::<String>::new());
    assert_eq!(
        failure.error,
        "the cause it was paused for is gone. RESUME would rebuild public.s_sums from public.s"
    );
    for target in ["s_one", "s_sums"] {
        trellis
            .apply(&format!("RESUME TRANSFORM {target}"))
            .await
            .unwrap();
    }
    bring_live(&mut raw, &db.pool, &trellis, &["s_one", "s_sums"]).await;
    converged(&raw).await;
}

/// #1744's matrix rows that keep a valid equivalent index at every
/// committed instant: each pauses nothing, and both readers keep
/// converging through writes after it.
#[tokio::test]
async fn routine_maintenance_of_the_key_index_pauses_nothing() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, KEYLESS, &[ONE, AGG]).await;
    for (n, step) in [
        // Build the replacement, then drop the old one, then rename.
        &[
            "create unique index concurrently s_key_new on public.s (k1, k2)",
            "drop index concurrently public.s_key",
            "alter index public.s_key_new rename to s_key",
        ][..],
        &["reindex index concurrently public.s_key"],
        &["reindex table concurrently public.s"],
        &["alter index public.s_key rename to s_key_renamed"],
        // Drop and create in one transaction.
        &["begin; drop index public.s_key_renamed; \
           create unique index s_key on public.s (k2, k1); commit"],
        // pg_repack's index mode: a copy built concurrently beside the
        // original, then dropped; the original's oid stays valid.
        &[
            "create unique index concurrently index_repack on public.s (k1, k2)",
            "drop index concurrently public.index_repack",
        ],
        // A replacement build that fails leaves an invalid index; the old
        // one still backs the key.
        &["create unique index concurrently s_bad on public.s (g)"],
        &["drop index public.s_bad"],
        // The key's index becomes the primary key.
        &[
            "create unique index concurrently s_pk on public.s (k1, k2)",
            "alter table public.s add primary key using index s_pk",
            "drop index public.s_key",
        ],
    ]
    .into_iter()
    .enumerate()
    {
        let next = 100 + n;
        for statement in step {
            let outcome = raw.batch_execute(statement).await;
            if statement.contains("s_bad") && statement.starts_with("create") {
                outcome.expect_err("g has duplicates");
            } else {
                outcome.unwrap_or_else(|e| panic!("{statement}: {e}"));
            }
            full_pass(&mut raw, &db.pool).await;
            assert_live(&trellis, &["s_one", "s_sums"], statement).await;
        }
        raw.batch_execute(&format!(
            "insert into public.s values ({next}, {next}, 1, {next}); \
             update public.s set v = v + 1 where k1 = 2"
        ))
        .await
        .unwrap();
        drain_to_quiescence(&db.pool, &mut raw).await;
        full_pass(&mut raw, &db.pool).await;
        assert_live(&trellis, &["s_one", "s_sums"], &step.join("; ")).await;
        converged(&raw).await;
    }
}

/// More of #1744's matrix rows, on the real pass: a replacement build of the
/// key's own shape left invalid beside the valid index (a cancelled
/// `CREATE UNIQUE INDEX CONCURRENTLY`), and rewrites, moves and primary-key
/// operations that keep the backing index valid at every committed instant,
/// including a key column retyped (its index rebuilt in the `ALTER`). Each
/// pauses nothing, and the reader keeps converging through writes. The
/// retype rewrites the table, and PostgreSQL's 2026-11 minors re-version the
/// columns after `k1`, so the reader rebuilds (`support/attver.rs`).
#[tokio::test]
async fn an_invalid_copy_beside_the_index_and_other_routine_operations_pause_nothing() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, KEYLESS, &[AGG]).await;
    // An open snapshot holds the build in its last wait, where the timeout
    // cancels it and leaves its index invalid.
    let holder = connect(db.dsn()).await;
    holder
        .batch_execute("begin isolation level repeatable read; select count(*) from public.s")
        .await
        .unwrap();
    raw.batch_execute("set statement_timeout = '500ms'")
        .await
        .unwrap();
    raw.batch_execute("create unique index concurrently s_copy on public.s (k1, k2)")
        .await
        .expect_err("cancelled while the snapshot is open");
    raw.batch_execute("reset statement_timeout").await.unwrap();
    holder.batch_execute("commit").await.unwrap();
    assert_eq!(
        rows(
            &raw,
            "select indisvalid::text from pg_catalog.pg_index \
             where indexrelid = 'public.s_copy'::regclass"
        )
        .await,
        [[Some("false".to_string())]]
    );
    let attver = attver::Attver::probe(&raw).await;
    for (n, statement) in [
        "select 1",
        "vacuum full public.s",
        "cluster public.s using s_key",
        "alter index public.s_key set tablespace pg_default",
        "reindex table public.s",
        "reindex index concurrently public.s_copy",
        "drop index public.s_copy",
        // A primary key over other columns, added through an index, then
        // dropped, then added again in place: the key is the `KEY`'s.
        "create unique index concurrently s_k2 on public.s (k2)",
        "alter table public.s add primary key using index s_k2",
        "alter table public.s drop constraint s_k2",
        "alter table public.s add primary key (k1)",
        "alter table public.s alter column k1 type bigint",
    ]
    .into_iter()
    .enumerate()
    {
        raw.batch_execute(statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
        attver.after(&raw, statement).await;
        full_pass(&mut raw, &db.pool).await;
        if statement.contains(" type ") {
            let reported = status(&trellis, "s_sums").await;
            assert_eq!(reported.capture_failure, None, "{statement}");
            assert_eq!(reported.status, TransformStatus::Backfilling, "{statement}");
            bring_live(&mut raw, &db.pool, &trellis, &["s_sums"]).await;
        }
        assert_live(&trellis, &["s_sums"], statement).await;
        let next = 100 + n;
        raw.batch_execute(&format!(
            "insert into public.s values ({next}, {next}, 1, {next}); \
             update public.s set v = v + 1 where k1 = 2"
        ))
        .await
        .unwrap();
        drain_to_quiescence(&db.pool, &mut raw).await;
        full_pass(&mut raw, &db.pool).await;
        assert_live(&trellis, &["s_sums"], statement).await;
    }
    assert_eq!(
        rows(
            &raw,
            "select g::text, n::text, total::text from public.s_sums order by g"
        )
        .await,
        rows(
            &raw,
            "select g::text, count(*)::text, sum(v)::text from public.s group by g order by g"
        )
        .await
    );
}

/// A text key's column moved to another collation: the `ALTER` rebuilds the
/// index under the column's new collation in the same transaction, so the
/// index keeps backing the key and the `KEY` check pauses nothing.
#[tokio::test]
async fn a_key_column_recollated_keeps_its_index_backing_the_key() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(
        &cluster,
        "create table public.c (k text not null, g int); \
         create unique index c_key on public.c (k); \
         insert into public.c select 'k' || i, i % 2 from generate_series(1, 6) i",
        &[
            "TRANSFORM c_sums FROM public.c KEY (k) GROUP BY g WITHOUT SOURCE INDEX \
           SELECT COUNT(*) AS n",
        ],
    )
    .await;
    raw.batch_execute("alter table public.c alter column k type text collate \"C\"")
        .await
        .unwrap();
    full_pass(&mut raw, &db.pool).await;
    let reasons = status(&trellis, "c_sums")
        .await
        .capture_failure
        .map(|failure| failure.reasons)
        .unwrap_or_default();
    assert!(
        !reasons.iter().any(|r| r.contains("backs KEY")),
        "{reasons:?}"
    );
}

/// A key column made nullable under an index that is not `NULLS NOT
/// DISTINCT` no longer backs the key: two rows can share it through a
/// `NULL`. The column's nullability is in the table's fingerprint, so the
/// next pass checks and pauses, and the message's index is `NULLS NOT
/// DISTINCT`.
#[tokio::test]
async fn a_key_column_made_nullable_pauses() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, KEYLESS, &[AGG]).await;
    full_pass(&mut raw, &db.pool).await;
    raw.batch_execute("alter table public.s alter column k2 drop not null")
        .await
        .unwrap();
    full_pass(&mut raw, &db.pool).await;
    let reported = status(&trellis, "s_sums").await;
    assert_eq!(reported.status, TransformStatus::Paused);
    let reasons = reported.capture_failure.expect("why").reasons;
    assert_eq!(reasons.len(), 1, "{reasons:?}");
    assert!(
        reasons[0].contains(
            "`create unique index concurrently on \"public\".\"s\" (\"k1\", \"k2\") \
             nulls not distinct`"
        ),
        "{reasons:?}"
    );
}

/// A drop-first swap whose build fails (`CREATE UNIQUE INDEX CONCURRENTLY`
/// meeting a duplicate): the invalid index it leaves has the key's shape but
/// backs nothing, so the pass pauses every reader.
#[tokio::test]
async fn a_failed_build_after_the_drop_pauses() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, KEYLESS, &[AGG]).await;
    raw.batch_execute("drop index public.s_key; insert into public.s values (1, 10, 0, 99)")
        .await
        .unwrap();
    raw.batch_execute("create unique index concurrently s_key on public.s (k1, k2)")
        .await
        .expect_err("the duplicate fails the build");
    assert_eq!(
        rows(
            &raw,
            "select indisvalid::text from pg_catalog.pg_index \
             where indexrelid = 'public.s_key'::regclass"
        )
        .await,
        [[Some("false".to_string())]]
    );
    full_pass(&mut raw, &db.pool).await;
    let reported = status(&trellis, "s_sums").await;
    assert_eq!(reported.status, TransformStatus::Paused);
    assert_eq!(reported.capture_failure.expect("why").reasons, [UNBACKED]);
}

/// The check reads the catalog only after an index change on the table:
/// passes over an unchanged table skip it, and a new index, even one on
/// another column, has the next pass check again.
#[tokio::test]
async fn steady_state_adds_no_check() {
    let cluster = TestCluster::start();
    let (db, mut raw, _trellis) = setup(&cluster, KEYLESS, &[AGG]).await;
    let database: String = raw
        .query_one("select current_database()::text", &[])
        .await
        .unwrap()
        .get(0);
    let checks = || trellis::staging::key_backing_checks(&database, SCHEMA, "public.s");
    full_pass(&mut raw, &db.pool).await;
    let settled = checks();
    assert!(settled >= 1, "the key has been checked");
    full_pass(&mut raw, &db.pool).await;
    full_pass(&mut raw, &db.pool).await;
    assert_eq!(checks(), settled, "an unchanged table is not checked again");
    raw.batch_execute("create index s_v on public.s (v)")
        .await
        .unwrap();
    full_pass(&mut raw, &db.pool).await;
    assert_eq!(checks(), settled + 1, "an index change is checked");
    full_pass(&mut raw, &db.pool).await;
    assert_eq!(checks(), settled + 1);
}

/// A table keyed by its primary key has no `KEY` check: a unique index on
/// it dropped pauses nothing and costs no check.
#[tokio::test]
async fn a_table_keyed_by_its_primary_key_is_not_checked() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(
        &cluster,
        "create table public.p (id int primary key, k int not null unique, g int); \
         insert into public.p select i, i, i % 2 from generate_series(1, 4) i",
        &["TRANSFORM p_sums FROM public.p GROUP BY g WITHOUT SOURCE INDEX SELECT COUNT(*) AS n"],
    )
    .await;
    raw.batch_execute("alter table public.p drop constraint p_k_key")
        .await
        .unwrap();
    full_pass(&mut raw, &db.pool).await;
    assert_live(&trellis, &["p_sums"], "drop the unique constraint").await;
    let database: String = raw
        .query_one("select current_database()::text", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        trellis::staging::key_backing_checks(&database, SCHEMA, "public.p"),
        0
    );
}

/// A partitioned source keyed by `KEY (g, id)`. Its root index is dropped and
/// rebuilt the lock-light way (`ON ONLY` the root, then each leaf's index
/// attached), but one leaf's build fails on a duplicate and its invalid index
/// is attached anyway. Repairing that leaf's index leaves the root invalid,
/// so the pass pauses the reader with the `ATTACH PARTITION` advice; once the
/// leaf's index is attached again the root is valid and the cause is gone.
#[tokio::test]
async fn a_partitioned_root_left_invalid_after_a_leaf_repair_pauses() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(
        &cluster,
        "create table public.pt (id int not null, g int not null, v int) \
             partition by range (g); \
         create table public.pt_0 partition of public.pt for values from (0) to (10); \
         create table public.pt_1 partition of public.pt for values from (10) to (20); \
         create unique index pt_key on public.pt (g, id); \
         insert into public.pt select i, i % 20, i from generate_series(1, 8) i",
        &[
            "TRANSFORM pt_sums FROM public.pt KEY (g, id) GROUP BY g WITHOUT SOURCE INDEX \
           SELECT COUNT(*) AS n",
        ],
    )
    .await;
    raw.batch_execute(
        "drop index public.pt_key; \
         insert into public.pt values (1, 11, 0), (1, 11, 0); \
         create unique index pt_key_new on only public.pt (g, id); \
         create unique index pt_0_key on public.pt_0 (g, id); \
         alter index public.pt_key_new attach partition public.pt_0_key",
    )
    .await
    .unwrap();
    raw.batch_execute("create unique index concurrently pt_1_key on public.pt_1 (g, id)")
        .await
        .expect_err("the duplicate fails the leaf's build");
    raw.batch_execute(
        "alter index public.pt_key_new attach partition public.pt_1_key; \
         delete from public.pt where id = 1 and g = 11 and v = 0; \
         reindex index public.pt_1_key",
    )
    .await
    .unwrap();
    assert_eq!(
        rows(
            &raw,
            "select (select indisvalid from pg_catalog.pg_index \
                     where indexrelid = 'public.pt_1_key'::regclass)::text, \
                    (select indisvalid from pg_catalog.pg_index \
                     where indexrelid = 'public.pt_key_new'::regclass)::text"
        )
        .await,
        [[Some("true".to_string()), Some("false".to_string())]],
        "the leaf is repaired and the root is still invalid"
    );
    full_pass(&mut raw, &db.pool).await;
    let reported = status(&trellis, "pt_sums").await;
    assert_eq!(reported.status, TransformStatus::Paused);
    assert_eq!(
        reported.capture_failure.expect("why").reasons,
        [
            "no valid unique index backs KEY (g, id) on public.pt any more, so public.pt can take \
          two rows with one key, which this definition's rows are keyed by. Create one with \
          `create unique index on \"public\".\"pt\" (\"g\", \"id\")`, then resume the definition to rebuild it. \
          To replace the index backing a KEY, build the replacement before dropping the old \
          index. On a partitioned table, the root's index stays invalid after a leaf's index is \
          repaired: re-run ALTER INDEX ... ATTACH PARTITION for the repaired leaf's index"
        ]
    );

    raw.batch_execute("alter index public.pt_key_new attach partition public.pt_1_key")
        .await
        .unwrap();
    full_pass(&mut raw, &db.pool).await;
    let failure = status(&trellis, "pt_sums")
        .await
        .capture_failure
        .expect("still paused");
    assert!(
        failure
            .error
            .starts_with("the cause it was paused for is gone. RESUME would rebuild"),
        "{}",
        failure.error
    );
}
