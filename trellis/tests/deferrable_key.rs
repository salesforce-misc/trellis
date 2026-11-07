//! A deferrable primary key is no key (ADR-0026, #1742): one can hold
//! duplicate keys inside a transaction, so define and `RESUME` refuse a
//! source or joined table whose primary key is deferrable (initially
//! immediate or deferred), naming the remedy. A table whose readers key it
//! by a `KEY (...)` over a unique index that isn't deferrable is keyed by
//! that, whatever its primary key is.
//!
//! Every pass, build and drain is stepped by hand. Nothing polls for
//! convergence.

#[path = "support/resume_passes.rs"]
mod resume_passes;

use resume_passes::{bring_live, drain_to_quiescence, full_pass, rows, setup, status};
use testkit::TestCluster;
use trellis::defs::TransformStatus;

/// Define refuses a deferrable primary key in both modes, as a plain
/// source, as an aggregate's, as a `JOIN`'s `FROM` table and as its joined
/// table, plain or 1-1. A 1-1 `JOIN`'s `ON`-uniqueness check accepts the
/// deferrable key on `b` (a separate check); `b`'s row identity refuses it.
#[tokio::test]
async fn define_refuses_a_deferrable_primary_key() {
    let cluster = TestCluster::start();
    let (_db, raw, trellis) = setup(
        &cluster,
        "create table public.base (id int primary key, bk int, g int); \
         insert into public.base values (1, 1, 1)",
        &[],
    )
    .await;
    raw.batch_execute(
        "create table public.imm (id int primary key deferrable initially immediate, \
             g int, v int); \
         create table public.def (id int, g int, v int, \
             constraint def_key primary key (id) deferrable initially deferred)",
    )
    .await
    .unwrap();
    for (table, constraint) in [("imm", "imm_pkey"), ("def", "def_key")] {
        let texts = [
            format!("TRANSFORM {table}_one FROM public.{table} SELECT v AS v"),
            format!(
                "TRANSFORM {table}_sums FROM public.{table} GROUP BY g WITHOUT SOURCE INDEX \
                 SELECT COUNT(*) AS n"
            ),
            format!(
                "TRANSFORM {table}_from FROM public.{table} JOIN public.base \
                 ON {table}.id = base.bk GROUP BY {table}.g WITHOUT SOURCE INDEX \
                 SELECT COUNT(*) AS n"
            ),
            format!(
                "TRANSFORM {table}_joined FROM public.base JOIN public.{table} \
                 ON base.bk = {table}.id GROUP BY base.g WITHOUT SOURCE INDEX \
                 SELECT COUNT(*) AS n"
            ),
            format!(
                "TRANSFORM {table}_one_join FROM public.base LEFT JOIN public.{table} \
                 ON base.bk = {table}.id SELECT {table}.v AS v"
            ),
        ];
        for text in texts {
            let message = trellis
                .apply(&text)
                .await
                .map(|_| ())
                .expect_err(&text)
                .to_string();
            assert!(
                message.contains(&format!(
                    "the primary key of 'public.{table}' (id) is deferrable"
                )) && message.contains(&format!(
                    "ALTER TABLE public.{table} DROP CONSTRAINT {constraint}, ADD PRIMARY KEY (id)"
                )) && message.contains("KEY (column, ...)"),
                "{text}: {message}"
            );
        }
    }
    assert_eq!(
        rows(
            &raw,
            "select count(*)::text from trellis.transform_definitions"
        )
        .await,
        [[Some("0".to_string())]]
    );
}

/// The primary key isn't the key of a table whose readers name a `KEY
/// (...)` over a unique index that isn't deferrable: define accepts it,
/// whatever the primary key is, and both transforms converge.
#[tokio::test]
async fn a_key_beside_a_deferrable_primary_key_is_the_key() {
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(
        &cluster,
        "create table public.p (id int primary key deferrable initially deferred, \
             k int not null unique, g int, v int); \
         insert into public.p select i, 100 + i, i % 2, i from generate_series(1, 6) i",
        &[
            "TRANSFORM p_one FROM public.p KEY (k) SELECT v AS v",
            "TRANSFORM p_sums FROM public.p KEY (k) GROUP BY g WITHOUT SOURCE INDEX \
             SELECT SUM(v) AS total",
        ],
    )
    .await;
    let one = (
        "select k::text, v::text from public.p_one order by k",
        "select k::text, v::text from public.p order by k",
    );
    let sums = (
        "select g::text, total::text from public.p_sums order by g",
        "select g::text, sum(v)::text from public.p group by g order by g",
    );
    raw.batch_execute(
        "insert into public.p values (7, 107, 1, 7); update public.p set v = 50 where k = 102; \
         delete from public.p where k = 103",
    )
    .await
    .unwrap();
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    for target in ["p_one", "p_sums"] {
        assert_eq!(status(&trellis, target).await.status, TransformStatus::Live);
    }
    assert_eq!(rows(&raw, one.0).await, rows(&raw, one.1).await);
    assert_eq!(rows(&raw, sums.0).await, rows(&raw, sums.1).await);
}

/// A primary key dropped and added again deferrable, over the same
/// columns: `RESUME` re-validates as define does and refuses it by name;
/// the key recreated not deferrable, it rebuilds.
#[tokio::test]
async fn resume_refuses_a_primary_key_made_deferrable() {
    const SUMS: (&str, &str) = (
        "select g::text, n::text from public.s_sums order by g",
        "select g::text, count(*)::text from public.s group by g order by g",
    );
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(
        &cluster,
        "create table public.s (id int primary key, g int); \
         insert into public.s select i, i % 2 from generate_series(1, 6) i",
        &[
            "TRANSFORM s_sums FROM public.s GROUP BY g WITHOUT SOURCE INDEX \
           SELECT COUNT(*) AS n",
        ],
    )
    .await;
    trellis.apply("PAUSE TRANSFORM s_sums").await.unwrap();
    raw.batch_execute(
        "alter table public.s drop constraint s_pkey, add primary key (id) deferrable; \
         insert into public.s values (7, 1)",
    )
    .await
    .unwrap();
    full_pass(&mut raw, &db.pool).await;
    let refusal = trellis
        .apply("RESUME TRANSFORM s_sums")
        .await
        .expect_err("the primary key is deferrable")
        .to_string();
    assert!(
        refusal.contains("the primary key of 'public.s' (id) is deferrable")
            && refusal
                .contains("ALTER TABLE public.s DROP CONSTRAINT s_pkey, ADD PRIMARY KEY (id)"),
        "{refusal}"
    );
    assert_eq!(
        status(&trellis, "s_sums").await.status,
        TransformStatus::Paused
    );

    raw.batch_execute("alter table public.s drop constraint s_pkey, add primary key (id)")
        .await
        .unwrap();
    trellis.apply("RESUME TRANSFORM s_sums").await.unwrap();
    bring_live(&mut raw, &db.pool, &trellis, &["s_sums"]).await;
    assert_eq!(rows(&raw, SUMS.0).await, rows(&raw, SUMS.1).await);
}

/// A primary key made deferrable in place, over the same columns, while
/// its readers run: the next drain finds no key it can key the table by and
/// halts every reader, naming the remedy, and `RESUME` refuses the same
/// way until the key is recreated not deferrable. Then both rebuild.
#[tokio::test]
async fn a_primary_key_made_deferrable_halts_its_readers() {
    const ONE: (&str, &str) = (
        "select id::text, g::text from public.s_one order by id",
        "select id::text, g::text from public.s order by id",
    );
    const SUMS: (&str, &str) = (
        "select g::text, n::text from public.s_sums order by g",
        "select g::text, count(*)::text from public.s group by g order by g",
    );
    let cluster = TestCluster::start();
    let (db, mut raw, trellis) = setup(
        &cluster,
        "create table public.s (id int primary key, g int); \
         insert into public.s select i, i % 2 from generate_series(1, 6) i",
        &[
            "TRANSFORM s_one FROM public.s SELECT g AS g",
            "TRANSFORM s_sums FROM public.s GROUP BY g WITHOUT SOURCE INDEX \
             SELECT COUNT(*) AS n",
        ],
    )
    .await;
    raw.batch_execute(
        "alter table public.s drop constraint s_pkey, \
             add primary key (id) deferrable initially deferred; \
         insert into public.s values (7, 1)",
    )
    .await
    .unwrap();
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    for target in ["s_one", "s_sums"] {
        let reported = status(&trellis, target).await;
        assert_eq!(reported.status, TransformStatus::Paused, "{target}");
        let failure = reported.capture_failure.expect("the halt's reason");
        assert!(
            failure
                .error
                .contains("the primary key of 'public.s' (id) is deferrable")
                && failure
                    .error
                    .contains("ALTER TABLE public.s DROP CONSTRAINT s_pkey, ADD PRIMARY KEY (id)"),
            "{target}: {}",
            failure.error
        );
        let refusal = trellis
            .apply(&format!("RESUME TRANSFORM {target}"))
            .await
            .expect_err("the primary key is deferrable")
            .to_string();
        assert!(
            refusal.contains("the primary key of 'public.s' (id) is deferrable"),
            "{target}: {refusal}"
        );
    }

    raw.batch_execute("alter table public.s drop constraint s_pkey, add primary key (id)")
        .await
        .unwrap();
    for target in ["s_one", "s_sums"] {
        trellis
            .apply(&format!("RESUME TRANSFORM {target}"))
            .await
            .expect(target);
    }
    bring_live(&mut raw, &db.pool, &trellis, &["s_one", "s_sums"]).await;
    assert_eq!(rows(&raw, ONE.0).await, rows(&raw, ONE.1).await);
    assert_eq!(rows(&raw, SUMS.0).await, rows(&raw, SUMS.1).await);
}
