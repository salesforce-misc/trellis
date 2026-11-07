//! `KEY (...)` after a `FROM` or `JOIN` table (ADR-0026, #1745): a source
//! with no primary key is keyed by the key its definition names, backed by
//! a unique index over exactly those columns, matched by shape; with neither
//! a primary key nor a `KEY`, define refuses. A table is keyed one way for
//! every transform that reads it, and a primary key change on a table its
//! readers key by `KEY` pauses nothing.
//!
//! Every pass, build and drain is stepped by hand. Nothing polls for
//! convergence.

#[path = "support/resume_passes.rs"]
mod resume_passes;

use resume_passes::{bring_live, drain_to_quiescence, full_pass, rows, setup, status};
use testkit::TestCluster;
use trellis::defs::TransformStatus;

/// A source with no primary key: its key is `(k1, k2)`, backed by an index
/// written in the other order.
const KEYLESS: &str = "create table public.s (k1 int not null, k2 int not null, g int, v int); \
     create unique index s_key on public.s (k2, k1); \
     insert into public.s select i, i * 10, i % 2, i from generate_series(1, 6) i";

const ONE: &str = "TRANSFORM s_one FROM public.s KEY (k1, k2) SELECT v AS v";
const ONE_ACTUAL: &str = "select k1::text, k2::text, v::text from public.s_one order by k1, k2";
const ONE_ORACLE: &str = "select k1::text, k2::text, v::text from public.s order by k1, k2";

const AGG: &str = "TRANSFORM s_sums FROM public.s KEY (k1, k2) GROUP BY g WITHOUT SOURCE INDEX \
     SELECT COUNT(*) AS n, SUM(v) AS total";
const AGG_ACTUAL: &str = "select g::text, n::text, total::text from public.s_sums order by g";
const AGG_ORACLE: &str =
    "select g::text, count(*)::text, sum(v)::text from public.s group by g order by g";

async fn converged(raw: &tokio_postgres::Client) {
    assert_eq!(rows(raw, ONE_ACTUAL).await, rows(raw, ONE_ORACLE).await);
    assert_eq!(rows(raw, AGG_ACTUAL).await, rows(raw, AGG_ORACLE).await);
}

async fn recorded(raw: &tokio_postgres::Client, table: &str) -> Vec<Vec<Option<String>>> {
    rows(
        raw,
        &format!(
            "select key_columns::text from trellis.definition_keys where table_name = '{table}'"
        ),
    )
    .await
}

/// The key the installed capture functions image `table` by, as their
/// comment records it.
async fn capture_key(raw: &tokio_postgres::Client, table: &str) -> String {
    let spec = match trellis::capture::install::installed(raw, "trellis", table)
        .await
        .expect("read the installed capture")
    {
        trellis::capture::install::Installed::Complete { spec, .. } => spec,
        other => panic!("{table}'s capture isn't installed: {other:?}"),
    };
    assert!(
        spec.declared_key(),
        "{table}'s capture key isn't the declared key"
    );
    spec.key().join(",")
}

/// A table with no primary key is keyed by its `KEY`, in the order written,
/// through an index over the same columns in another order: its 1-1 target's
/// primary key, its record and its capture key are `(k1, k2)`, and both
/// transforms converge through inserts, updates and deletes. A replacement
/// index built before the old one is dropped changes nothing, and `RESUME`
/// accepts it too.
#[tokio::test]
async fn a_table_with_no_primary_key_is_keyed_by_its_key() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, KEYLESS, &[ONE, AGG]).await;
    converged(&raw).await;
    assert_eq!(
        rows(
            &raw,
            "select pg_catalog.pg_get_constraintdef(oid) from pg_catalog.pg_constraint \
             where conrelid = 'public.s_one'::regclass and contype = 'p'"
        )
        .await,
        [[Some("PRIMARY KEY (k1, k2)".to_string())]]
    );
    assert_eq!(
        recorded(&raw, "public.s").await,
        [[Some("{k1,k2}".to_string())]]
    );
    assert_eq!(capture_key(&raw, "public.s").await, "k1,k2");

    raw.batch_execute(
        "insert into public.s values (7, 70, 1, 7); update public.s set v = 100 where k1 = 2; \
         update public.s set g = 0 where k1 = 3; delete from public.s where k1 = 4",
    )
    .await
    .unwrap();
    drain_to_quiescence(&db.pool, &mut raw).await;
    converged(&raw).await;

    // Build the replacement, then drop the old index: nothing changes.
    raw.batch_execute(
        "create unique index s_key_new on public.s (k1, k2); drop index public.s_key; \
         alter index public.s_key_new rename to s_key; \
         insert into public.s values (8, 80, 0, 8); delete from public.s where k1 = 1",
    )
    .await
    .unwrap();
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    for target in ["s_one", "s_sums"] {
        assert_eq!(status(&trellis, target).await.status, TransformStatus::Live);
    }
    converged(&raw).await;

    trellis.apply("PAUSE TRANSFORM s_sums").await.unwrap();
    trellis.apply("RESUME TRANSFORM s_sums").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["s_one", "s_sums"]).await;
    converged(&raw).await;
}

/// With the index backing a `KEY` dropped, the reader pauses, and `RESUME`
/// re-validates the `KEY` against the table now: with no index backing it,
/// it refuses by name, and once one is built again it rebuilds.
#[tokio::test]
async fn resume_needs_an_index_backing_the_key_again() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, KEYLESS, &[AGG]).await;
    raw.batch_execute("drop index public.s_key; insert into public.s values (9, 90, 1, 9)")
        .await
        .unwrap();
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        status(&trellis, "s_sums").await.status,
        TransformStatus::Paused
    );
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
    trellis.apply("RESUME TRANSFORM s_sums").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["s_sums"]).await;
    assert_eq!(rows(&raw, AGG_ACTUAL).await, rows(&raw, AGG_ORACLE).await);
}

/// A paused reader of a table keyed by `KEY` whose backing index is gone:
/// the outcome the pass works out is the very refusal its `RESUME` makes,
/// never a re-key to the table's primary key, which a `KEY` table isn't
/// keyed by even once it has one.
#[tokio::test]
async fn an_unbacked_key_outcome_is_the_resume_refusal() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, KEYLESS, &[AGG]).await;
    raw.batch_execute("drop index public.s_key; insert into public.s values (9, 90, 1, 9)")
        .await
        .unwrap();
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        status(&trellis, "s_sums").await.status,
        TransformStatus::Paused
    );
    // A primary key over other columns gives the resume nothing to key by.
    raw.batch_execute("alter table public.s add primary key (k2)")
        .await
        .unwrap();
    full_pass(&mut raw, &db.pool).await;
    full_pass(&mut raw, &db.pool).await;
    let outcome = status(&trellis, "s_sums")
        .await
        .capture_failure
        .expect("s_sums reports why it is paused")
        .resume_outcome
        .expect("the pass worked out the outcome");
    let err = trellis::staging::quarantine::resume_transform(&db.pool, "s_sums")
        .await
        .expect_err("no index backs the key");
    let reason = std::error::Error::source(&err)
        .expect("the refusal's reason")
        .to_string();
    assert!(
        reason.contains("no unique index backs KEY (k1, k2) on 'public.s'"),
        "{reason}"
    );
    // Define refuses an unbacked `KEY` too, so the outcome says so.
    assert_eq!(
        outcome,
        format!(
            "RESUME would refuse: define would refuse the definition as the schema stands \
             now: {reason}"
        )
    );
    assert_eq!(
        recorded(&raw, "public.s").await,
        [[Some("{k1,k2}".to_string())]]
    );
}

/// A paused reader of a table keyed by `KEY`, created again at its name
/// with the key's index and a primary key over another column: the outcome
/// the pass works out re-records the declared key for the new table, never
/// its primary key, and `RESUME` does just that and rebuilds.
#[tokio::test]
async fn a_key_table_created_again_outcome_re_records_the_declared_key() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(&cluster, KEYLESS, &[AGG]).await;
    raw.batch_execute(
        "drop table public.s; \
         create table public.s (k1 int not null, k2 int not null, g int, v int primary key); \
         create unique index s_key on public.s (k2, k1); \
         insert into public.s select i, i * 10, i % 2, i from generate_series(1, 4) i",
    )
    .await
    .unwrap();
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    full_pass(&mut raw, &db.pool).await;
    full_pass(&mut raw, &db.pool).await;
    let reported = status(&trellis, "s_sums").await;
    assert_eq!(reported.status, TransformStatus::Paused);
    let outcome = reported
        .capture_failure
        .expect("s_sums reports why it is paused")
        .resume_outcome
        .expect("the pass worked out the outcome");
    assert_eq!(
        outcome,
        "RESUME would re-record public.s's key as (k1, k2), then rebuild public.s_sums \
         from public.s"
    );
    trellis.apply("RESUME TRANSFORM s_sums").await.unwrap();
    assert_eq!(
        rows(
            &raw,
            "select key_columns::text, (relation_oid = 'public.s'::regclass::oid)::text \
             from trellis.definition_keys where table_name = 'public.s'"
        )
        .await,
        [[Some("{k1,k2}".to_string()), Some("true".to_string())]],
        "the declared key, tied to the new table"
    );
    bring_live(&mut raw, &db.pool, &trellis, &["s_sums"]).await;
    assert_eq!(rows(&raw, AGG_ACTUAL).await, rows(&raw, AGG_ORACLE).await);
    assert_eq!(capture_key(&raw, "public.s").await, "k1,k2");
}

/// The readers of a table keyed by `KEY (k)` are keyed by it whatever its
/// primary key does (#1744's finding: the primary-key change detectors are
/// scoped to readers keyed by the primary key). The primary key redefined
/// with another column, and then dropped, pauses neither transform, and both
/// keep converging.
#[tokio::test]
async fn a_primary_key_change_on_a_table_keyed_by_key_pauses_nothing() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(
        &cluster,
        "create table public.p (id int primary key, k int not null unique, g int not null, \
             v int); \
         insert into public.p select i, 100 + i, i % 2, i from generate_series(1, 6) i",
        &[
            "TRANSFORM p_one FROM public.p KEY (k) SELECT v AS v",
            "TRANSFORM p_sums FROM public.p KEY (k) GROUP BY g WITHOUT SOURCE INDEX \
             SELECT SUM(v) AS total",
        ],
    )
    .await;
    assert_eq!(capture_key(&raw, "public.p").await, "k");
    let one = (
        "select k::text, v::text from public.p_one order by k",
        "select k::text, v::text from public.p order by k",
    );
    let sums = (
        "select g::text, total::text from public.p_sums order by g",
        "select g::text, sum(v)::text from public.p group by g order by g",
    );
    for step in [
        "alter table public.p drop constraint p_pkey, add primary key (id, g); \
         insert into public.p values (7, 107, 1, 7); update public.p set v = 50 where k = 102",
        "alter table public.p drop constraint p_pkey; \
         delete from public.p where k = 103; insert into public.p values (8, 108, 0, 8)",
    ] {
        raw.batch_execute(step).await.unwrap();
        full_pass(&mut raw, &db.pool).await;
        drain_to_quiescence(&db.pool, &mut raw).await;
        full_pass(&mut raw, &db.pool).await;
        for target in ["p_one", "p_sums"] {
            let reported = status(&trellis, target).await;
            assert_eq!(
                reported.status,
                TransformStatus::Live,
                "{target} after {step}"
            );
            assert_eq!(reported.capture_failure, None, "{target} after {step}");
        }
        assert_eq!(rows(&raw, one.0).await, rows(&raw, one.1).await, "{step}");
        assert_eq!(rows(&raw, sums.0).await, rows(&raw, sums.1).await, "{step}");
        assert_eq!(capture_key(&raw, "public.p").await, "k");
    }
}

/// Every define refusal of a key: none at all, a `KEY` no index backs (none
/// over its columns, a partial, expression, nullable or deferrable one, one
/// under a non-default operator class or another collation than its
/// column's), a
/// deferrable primary key named as a `KEY`, a `KEY` on another transform's target,
/// and a key other than the one the table's other transforms use.
#[tokio::test]
async fn define_refuses_a_key_it_cannot_key_by() {
    let cluster = TestCluster::start();
    let (_db, raw, trellis) = setup(
        &cluster,
        "create table public.base (id int primary key, g int); \
         insert into public.base values (1, 1)",
        &[
            "TRANSFORM base_sums FROM public.base GROUP BY g WITHOUT SOURCE INDEX \
           SELECT COUNT(*) AS n",
        ],
    )
    .await;
    raw.batch_execute(
        "create table public.bare (a int not null, t text); \
         create table public.part (a int not null, live bool); \
         create unique index part_a on public.part (a) where live; \
         create table public.expr (a int not null); \
         create unique index expr_a on public.expr ((a + 0)); \
         create table public.nullable (a int unique); \
         create table public.nnd (a int unique nulls not distinct, v int); \
         create table public.deferred (a int not null unique deferrable, v int); \
         create table public.deferred_pk (id int primary key deferrable, v int); \
         create table public.both (id int primary key, k int not null unique, \
             j int not null unique, v int); \
         create table public.other (id int primary key, bk int); \
         create table public.opclass (a text not null, v int); \
         create unique index opclass_a on public.opclass (a text_pattern_ops); \
         create table public.collated (a text not null, v int); \
         create unique index collated_a on public.collated (a collate \"C\")",
    )
    .await
    .unwrap();
    let refused = |text: &'static str| {
        let trellis = &trellis;
        async move {
            trellis
                .apply(text)
                .await
                .map(|_| ())
                .expect_err(text)
                .to_string()
        }
    };

    let message = refused("TRANSFORM bare_copy FROM public.bare SELECT t AS t").await;
    assert!(
        message.contains("'public.bare' has no primary key")
            && message.contains("KEY (column, ...)")
            && message.contains("ADD PRIMARY KEY"),
        "{message}"
    );
    let message = refused("TRANSFORM bare_copy FROM public.bare KEY (a) SELECT t AS t").await;
    assert!(
        message.contains("no unique index backs KEY (a) on 'public.bare'")
            && message.contains("it has no unique index"),
        "{message}"
    );
    for (text, why) in [
        (
            "TRANSFORM part_copy FROM public.part KEY (a) SELECT live AS live",
            "partial",
        ),
        (
            "TRANSFORM expr_copy FROM public.expr KEY (a) SELECT a AS b",
            "an expression",
        ),
        (
            "TRANSFORM null_copy FROM public.nullable KEY (a) SELECT a AS b",
            "over a nullable column and not NULLS NOT DISTINCT",
        ),
        (
            "TRANSFORM def_copy FROM public.deferred KEY (a) SELECT v AS v",
            "deferrable",
        ),
        (
            "TRANSFORM opc_copy FROM public.opclass KEY (a) SELECT v AS v",
            "under a non-default operator class",
        ),
        (
            "TRANSFORM coll_copy FROM public.collated KEY (a) SELECT v AS v",
            "under a collation other than its column's",
        ),
    ] {
        let message = refused(text).await;
        assert!(
            message.contains("no unique index backs KEY (a)") && message.contains(why),
            "{text}: {message}"
        );
    }
    // A deferrable primary key backs no KEY (#1742 is about the default).
    let message =
        refused("TRANSFORM dpk_copy FROM public.deferred_pk KEY (id) SELECT v AS v").await;
    assert!(
        message.contains("no unique index backs KEY (id)") && message.contains("deferrable"),
        "{message}"
    );
    let message = refused("TRANSFORM chained FROM public.base_sums KEY (g) SELECT n AS n").await;
    assert!(
        message.contains("KEY (g) names a key for 'public.base_sums'")
            && message.contains("another transform's target"),
        "{message}"
    );

    // A nullable key works through KEY on a NULLS NOT DISTINCT index.
    trellis
        .apply("TRANSFORM nnd_copy FROM public.nnd KEY (a) SELECT v AS v")
        .await
        .expect("a NULLS NOT DISTINCT index backs a nullable key");

    // One key per table: the second definition must write the first's.
    trellis
        .apply("TRANSFORM both_k FROM public.both KEY (k) SELECT v AS v")
        .await
        .expect("the first key of public.both");
    for (text, write) in [
        (
            "TRANSFORM both_pk FROM public.both SELECT v AS v",
            "write KEY (k) after it here, not its primary key, with no KEY",
        ),
        (
            "TRANSFORM both_j FROM public.both KEY (j) SELECT v AS v",
            "write KEY (k) after it here, not KEY (j)",
        ),
        (
            "TRANSFORM both_join FROM public.other JOIN public.both KEY (j) ON other.bk = both.k \
             GROUP BY other.id WITHOUT SOURCE INDEX SELECT COUNT(*) AS n",
            "write KEY (k) after it here, not KEY (j)",
        ),
    ] {
        let message = refused(text).await;
        assert!(
            message.contains(
                "'public.both' is keyed by KEY (k) for its other transforms \
                              (public.both_k)"
            ) && message.contains(write),
            "{text}: {message}"
        );
    }
    let message = refused("TRANSFORM base_k FROM public.base KEY (id) SELECT g AS g").await;
    assert!(
        message.contains("'public.base' is keyed by its primary key, with no KEY")
            && message.contains("write no KEY after it here, not KEY (id)"),
        "{message}"
    );
    trellis
        .apply(
            "TRANSFORM both_join FROM public.other JOIN public.both KEY (k) ON other.bk = both.k \
             GROUP BY other.id WITHOUT SOURCE INDEX SELECT COUNT(*) AS n",
        )
        .await
        .expect("the same key on a JOIN side");
}
