//! A released definition over a table with no primary key (#1745 rule 8,
//! ADR-0026). An earlier release keyed such a table by a unique index it
//! chose (its oldest qualifying one); this release keys a table by its
//! primary key or a `KEY (...)` the definition names. At attach, a
//! definition whose table's chosen index backs a key now gets it written
//! into its stored text as an explicit `KEY` and keeps running; one whose
//! table has no index that does is paused, naming the table and the
//! remedies. A 1-1 definition is converted only when the index is over the
//! key its target was built on, the primary key its source had at define.
//! A definition over a table whose primary key is deferrable pauses too
//! (#1742): no key is taken for it.
//!
//! The test builds that state as an earlier release left it: definitions
//! over tables whose primary key was then dropped, with the catalog's key
//! records deleted, as before the migration that added them. Every step
//! drives the pipeline by hand through `support/drain_driver.rs`. None
//! sleeps or polls for convergence.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;
use trellis::staging::build;

const ONE: &str = "TRANSFORM s_one FROM public.s SELECT v AS v";
const ONE_ACTUAL: &str = "select id, v from public.s_one order by id";
const ONE_EXPECTED: &str = "select id, v from public.s order by id";

const SUMS: &str = "TRANSFORM u_sums FROM public.u GROUP BY g WITHOUT SOURCE INDEX \
                    SELECT COUNT(*) AS n, SUM(v) AS total";
const SUMS_ACTUAL: &str = "select g, n, total from public.u_sums order by g";
const SUMS_EXPECTED: &str = "select g, count(*), sum(v) from public.u group by g order by g";

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

async fn exec(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

async fn settle(d: &mut Driver) {
    let pool = d.pool().clone();
    for _ in 0..2 {
        trellis::client::reconcile_pass(&mut d.ctl, &pool, DEFAULT_SCHEMA)
            .await
            .expect("reconcile pass");
        build::settle_builds(d.pool()).await;
        d.settle().await;
    }
}

async fn statuses(d: &Driver) -> Vec<String> {
    d.rows("select target_table, status from transform_definitions order by id")
        .await
}

#[tokio::test]
async fn a_released_definition_over_a_table_with_no_primary_key_takes_its_index_as_key() {
    let mut d = Driver::start(
        "create table public.s (id integer not null, v integer, primary key (id)); \
         create unique index s_id on public.s (id); \
         create table public.u (id integer not null, n integer, g integer, v integer, \
             primary key (id)); \
         create unique index u_n on public.u (n)",
        &[("id", int4()), ("n", int4()), ("g", int4()), ("v", int4())],
        &[ONE, SUMS],
        &["public.s", "public.u"],
    )
    .await;
    exec(
        &d,
        "insert into public.s values (1, 10), (2, 20); \
         insert into public.u values (1, 1, 1, 5), (2, 2, 1, 6), (3, null, 2, 7)",
    )
    .await;
    d.settle().await;
    assert_eq!(d.rows(ONE_ACTUAL).await, d.rows(ONE_EXPECTED).await);
    assert_eq!(d.rows(SUMS_ACTUAL).await, d.rows(SUMS_EXPECTED).await);

    // What an earlier release left: both tables lost their primary key,
    // and the catalog has no key records.
    exec(
        &d,
        "alter table public.s drop constraint s_pkey; \
         alter table public.u drop constraint u_pkey",
    )
    .await;
    d.ctl
        .batch_execute("delete from definition_keys")
        .await
        .expect("a catalog from before the record");
    let config = trellis::Config::from_dsn(d.db.dsn().to_string()).expect("config");
    trellis::migrate::migrate_with_pool(d.pool(), &config)
        .await
        .expect("attach");

    // `s`'s unique index backs a key: it is the definition's KEY now.
    assert_eq!(
        d.rows("select definition_text from transform_definitions order by id")
            .await,
        [
            "(\"TRANSFORM s_one FROM public.s KEY (id) SELECT v AS v\")".to_string(),
            format!("(\"{SUMS}\")"),
        ]
    );
    assert_eq!(
        d.rows("select key_columns::text from definition_keys where table_name = 'public.s'")
            .await,
        ["({id})"]
    );
    // `u`'s only unique index is over a nullable column: no key.
    assert_eq!(
        statuses(&d).await,
        ["(public.s_one,live)", "(public.u_sums,paused)"]
    );
    let reasons = d
        .rows("select error from capture_failures order by transform_id")
        .await;
    assert_eq!(reasons.len(), 1, "{reasons:?}");
    assert!(
        reasons[0].contains("transform public.u_sums reads public.u, which has no primary key")
            && reasons[0].contains("u_n (n), over a nullable column and not NULLS NOT DISTINCT")
            && reasons[0].contains("ADD PRIMARY KEY")
            && reasons[0].contains("RESUME")
            && reasons[0].contains("DROP TRANSFORM and define it again with a valid KEY"),
        "{}",
        reasons[0]
    );

    exec(
        &d,
        "insert into public.s values (3, 30); update public.s set v = 21 where id = 2; \
         delete from public.s where id = 1",
    )
    .await;
    settle(&mut d).await;
    assert_eq!(statuses(&d).await[0], "(public.s_one,live)");
    assert_eq!(d.rows(ONE_ACTUAL).await, d.rows(ONE_EXPECTED).await);

    // The remedy: a primary key, then RESUME.
    exec(&d, "alter table public.u add primary key (id)").await;
    trellis::staging::quarantine::resume_transform(d.pool(), "u_sums")
        .await
        .expect("resume");
    settle(&mut d).await;
    assert_eq!(
        statuses(&d).await,
        ["(public.s_one,live)", "(public.u_sums,live)"]
    );
    assert_eq!(d.rows(SUMS_ACTUAL).await, d.rows(SUMS_EXPECTED).await);
}

/// The primary key a released 1-1 definition's target was built on was
/// dropped, and the index the earlier release's rule chooses is over other
/// columns: taking it as the `KEY` would key the source by columns the
/// target and its ledger aren't keyed by, so the definition pauses, naming
/// the key to add back. An aggregate over such a table is keyed by its
/// group columns, takes the index as its `KEY` and keeps running.
#[tokio::test]
async fn a_released_one_to_one_whose_dropped_key_differs_from_the_chosen_index_pauses() {
    const BY_N: &str = "TRANSFORM s_one FROM public.s SELECT v AS v";
    const BY_N_ACTUAL: &str = "select id, v from public.s_one order by id";
    const BY_N_EXPECTED: &str = "select id, v from public.s order by id";
    const AGG: &str = "TRANSFORM u_sums FROM public.u GROUP BY g WITHOUT SOURCE INDEX \
                       SELECT COUNT(*) AS n, SUM(v) AS total";
    let mut d = Driver::start(
        "create table public.s (id integer not null, n integer not null, v integer, \
             primary key (id)); \
         create unique index s_n on public.s (n); \
         create table public.u (id integer not null, n integer not null, g integer, \
             v integer, primary key (id)); \
         create unique index u_n on public.u (n)",
        &[("id", int4()), ("n", int4()), ("g", int4()), ("v", int4())],
        &[BY_N, AGG],
        &["public.s", "public.u"],
    )
    .await;
    exec(
        &d,
        "insert into public.s values (1, 10, 100), (2, 20, 200); \
         insert into public.u values (1, 10, 1, 5), (2, 20, 1, 6), (3, 30, 2, 7)",
    )
    .await;
    d.settle().await;
    exec(
        &d,
        "alter table public.s drop constraint s_pkey; \
         alter table public.u drop constraint u_pkey",
    )
    .await;
    d.ctl
        .batch_execute("delete from definition_keys")
        .await
        .expect("a catalog from before the record");
    let config = trellis::Config::from_dsn(d.db.dsn().to_string()).expect("config");
    trellis::migrate::migrate_with_pool(d.pool(), &config)
        .await
        .expect("attach");

    assert_eq!(
        d.rows("select definition_text from transform_definitions order by id")
            .await,
        [
            format!("(\"{BY_N}\")"),
            "(\"TRANSFORM u_sums FROM public.u KEY (n) GROUP BY g WITHOUT SOURCE INDEX \
             SELECT COUNT(*) AS n, SUM(v) AS total\")"
                .to_string(),
        ]
    );
    assert_eq!(
        statuses(&d).await,
        ["(public.s_one,paused)", "(public.u_sums,live)"]
    );
    let reasons = d
        .rows("select error from capture_failures order by transform_id")
        .await;
    assert_eq!(reasons.len(), 1, "{reasons:?}");
    assert!(
        reasons[0].contains("transform public.s_one reads public.s, which has no primary key")
            && reasons[0].contains("Its target is keyed by (id)")
            && reasons[0].contains("is over (n), another key")
            && reasons[0].contains("ALTER TABLE public.s ADD PRIMARY KEY (id)"),
        "{}",
        reasons[0]
    );

    exec(
        &d,
        "update public.s set v = 101 where id = 1; update public.s set n = 99 where id = 2; \
         insert into public.s values (3, 30, 300); \
         update public.u set g = 2 where id = 1; update public.u set n = 99 where id = 2; \
         delete from public.u where id = 3",
    )
    .await;
    settle(&mut d).await;
    assert_eq!(d.rows(SUMS_ACTUAL).await, d.rows(SUMS_EXPECTED).await);

    // The remedy the message names.
    exec(&d, "alter table public.s add primary key (id)").await;
    trellis::staging::quarantine::resume_transform(d.pool(), "s_one")
        .await
        .expect("resume");
    settle(&mut d).await;
    assert_eq!(
        statuses(&d).await,
        ["(public.s_one,live)", "(public.u_sums,live)"]
    );
    assert_eq!(d.rows(BY_N_ACTUAL).await, d.rows(BY_N_EXPECTED).await);
    exec(&d, "update public.s set v = 102 where id = 1").await;
    settle(&mut d).await;
    assert_eq!(d.rows(BY_N_ACTUAL).await, d.rows(BY_N_EXPECTED).await);
}

/// A released definition over a table whose primary key is deferrable
/// (#1742): an earlier release keyed the table by it, and Trellis no longer
/// keys a table by a deferrable key. At attach each definition pauses,
/// naming the key and the remedy; nothing is recorded for the table, and a
/// unique index over the same columns that isn't deferrable never stands in
/// for the key. `RESUME` refuses until the key is recreated not deferrable,
/// then rebuilds.
#[tokio::test]
async fn a_released_definition_over_a_deferrable_primary_key_pauses() {
    let mut d = Driver::start(
        "create table public.s (id integer not null, v integer, primary key (id)); \
         create unique index s_id on public.s (id); \
         create table public.u (id integer not null, n integer, g integer, v integer, \
             primary key (id))",
        &[("id", int4()), ("n", int4()), ("g", int4()), ("v", int4())],
        &[ONE, SUMS],
        &["public.s", "public.u"],
    )
    .await;
    exec(
        &d,
        "insert into public.s values (1, 10), (2, 20); \
         insert into public.u values (1, 1, 1, 5), (2, 2, 1, 6), (3, null, 2, 7)",
    )
    .await;
    d.settle().await;

    // What an earlier release left: definitions over deferrable primary
    // keys (in both modes), and no key records.
    exec(
        &d,
        "alter table public.s drop constraint s_pkey, \
             add primary key (id) deferrable initially immediate; \
         alter table public.u drop constraint u_pkey, \
             add primary key (id) deferrable initially deferred",
    )
    .await;
    d.ctl
        .batch_execute("delete from definition_keys")
        .await
        .expect("a catalog from before the record");
    let config = trellis::Config::from_dsn(d.db.dsn().to_string()).expect("config");
    trellis::migrate::migrate_with_pool(d.pool(), &config)
        .await
        .expect("attach");

    assert_eq!(
        statuses(&d).await,
        ["(public.s_one,paused)", "(public.u_sums,paused)"]
    );
    assert_eq!(
        d.rows(
            "select table_name from definition_keys \
             where table_name in ('public.s', 'public.u')"
        )
        .await,
        Vec::<String>::new(),
        "no key is recorded for a table with a deferrable primary key"
    );
    let reasons = d
        .rows("select error from capture_failures order by transform_id")
        .await;
    assert_eq!(reasons.len(), 2, "{reasons:?}");
    for (reason, (target, table)) in reasons
        .iter()
        .zip([("public.s_one", "public.s"), ("public.u_sums", "public.u")])
    {
        assert!(
            reason.contains(&format!(
                "transform {target} reads {table}, whose primary key (id) is deferrable"
            )) && reason.contains(&format!(
                "ALTER TABLE {table} DROP CONSTRAINT {}_pkey, ADD PRIMARY KEY (id)",
                &table["public.".len()..]
            )) && reason.contains("RESUME this transform")
                && reason.contains("DROP TRANSFORM and define it again with KEY (...)"),
            "{reason}"
        );
    }

    let err = trellis::staging::quarantine::resume_transform(d.pool(), "s_one")
        .await
        .expect_err("the primary key is still deferrable");
    let reason = std::error::Error::source(&err)
        .expect("the refusal's reason")
        .to_string();
    assert!(
        reason.contains("the primary key of 'public.s' (id) is deferrable"),
        "{reason}"
    );

    // The remedy: recreate each key not deferrable, then RESUME.
    exec(
        &d,
        "alter table public.s drop constraint s_pkey, add primary key (id); \
         alter table public.u drop constraint u_pkey, add primary key (id); \
         insert into public.s values (3, 30); update public.u set v = 60 where id = 2",
    )
    .await;
    for target in ["s_one", "u_sums"] {
        trellis::staging::quarantine::resume_transform(d.pool(), target)
            .await
            .expect(target);
    }
    settle(&mut d).await;
    assert_eq!(
        statuses(&d).await,
        ["(public.s_one,live)", "(public.u_sums,live)"]
    );
    assert_eq!(d.rows(ONE_ACTUAL).await, d.rows(ONE_EXPECTED).await);
    assert_eq!(d.rows(SUMS_ACTUAL).await, d.rows(SUMS_EXPECTED).await);
}
