//! A source key with `INCLUDE` columns (#1722): its `INCLUDE` columns are not
//! key columns. A release before the fix keyed such a source by its
//! `INCLUDE` columns too, so a definition it built has a 1-1 target keyed
//! `(id, x)`, capture keys and ledger keys over both columns.
//!
//! The test builds that state as an earlier release left it: definitions
//! over a source keyed `(id, x)`, whose key then becomes `(id) include (x)`,
//! with the catalog's key records deleted, as before the migration that
//! added them. Attaching pauses each definition, naming the `INCLUDE`
//! column; `RESUME` re-keys the 1-1 target in place and rebuilds both, and
//! refuses while a user's view or index depends on the column it drops.
//!
//! Every step drives the pipeline by hand through `support/drain_driver.rs`.
//! None sleeps or polls for convergence.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;
use trellis::staging::build;

const ONE: &str = "TRANSFORM one FROM public.inc SELECT ga AS ga, y AS y";
const ONE_ACTUAL: &str = "select id, ga, y from public.one order by id";
const ONE_EXPECTED: &str = "select id, ga, y from public.inc order by id";

const AGG: &str = "TRANSFORM g FROM public.inc GROUP BY ga WITHOUT SOURCE INDEX \
                   SELECT COUNT(*) AS n, SUM(y) AS sy";
const AGG_ACTUAL: &str = "select ga, n, sy from public.g order by ga";
const AGG_EXPECTED: &str = "select ga, count(*), sum(y) from public.inc group by ga order by ga";

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

async fn assert_converged(d: &Driver) {
    assert_eq!(
        statuses(d).await,
        ["(public.one,live)", "(public.g,live)"],
        "{:?}",
        d.rows("select transform_id, error from capture_failures")
            .await
    );
    assert_eq!(d.rows(ONE_ACTUAL).await, d.rows(ONE_EXPECTED).await);
    assert_eq!(d.rows(AGG_ACTUAL).await, d.rows(AGG_EXPECTED).await);
}

#[tokio::test]
async fn a_definition_built_over_an_include_column_pauses_and_resume_re_keys_it() {
    let mut d = Driver::start(
        "create table public.inc (id integer not null, ga integer, x integer not null, \
             y integer, primary key (id, x))",
        &[("id", int4()), ("ga", int4()), ("x", int4()), ("y", int4())],
        &[ONE, AGG],
        &["public.inc"],
    )
    .await;
    exec(
        &d,
        "insert into public.inc values (1, 1, 10, 1), (2, 1, 20, 2), (3, 2, 30, 3)",
    )
    .await;
    d.settle().await;
    assert_converged(&d).await;
    exec(&d, "create index one_ga on public.one (ga)").await;
    let one_acl = "select relacl::text from pg_class where oid = 'public.one'::regclass";
    exec(&d, "grant select on public.one to public").await;
    let acl = d.rows(one_acl).await;

    // What an earlier release left: the key is `(id) include (x)` and was
    // read as `(id, x)`; the catalog has no key records.
    exec(
        &d,
        "alter table public.inc drop constraint inc_pkey, \
             add primary key (id) include (x)",
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
        ["(public.one,paused)", "(public.g,paused)"]
    );
    let reasons = d
        .rows("select error from capture_failures order by transform_id")
        .await;
    assert_eq!(reasons.len(), 2, "{reasons:?}");
    for reason in &reasons {
        assert!(
            reason.contains("public.inc's key with its INCLUDE columns (x)")
                && reason.contains("RESUME")
                && reason.contains("drop the transform"),
            "{reason}"
        );
    }
    assert_eq!(
        d.rows("select table_name, key_columns::text from definition_keys order by 1")
            .await,
        ["(public.g,{ga})", "(public.inc,{id})", "(public.one,{id})"],
        "the corrected key is recorded"
    );
    // A second attach changes nothing.
    trellis::migrate::migrate_with_pool(d.pool(), &config)
        .await
        .expect("attach again");
    assert_eq!(
        d.rows("select error from capture_failures order by transform_id")
            .await,
        reasons
    );

    // A write the capture an earlier release installed stages, keyed by
    // `(id, x)`; then the staging worker's passes regenerate the source's
    // capture over the corrected key, and keep the reason.
    exec(&d, "update public.inc set y = 7 where id = 3").await;
    settle(&mut d).await;
    assert_eq!(
        d.rows("select error from capture_failures order by transform_id")
            .await,
        reasons
    );

    // The source changes while the definitions are paused.
    exec(
        &d,
        "update public.inc set y = 9 where id = 1; delete from public.inc where id = 2; \
         insert into public.inc values (4, 2, 40, 4)",
    )
    .await;

    // A user's view and index on the column the re-keying drops hold it.
    exec(
        &d,
        "create view public.one_x as select id, x from public.one; \
         create index one_x_ix on public.one (x)",
    )
    .await;
    for blocker in ["view public.one_x", "index public.one_x_ix"] {
        let refusal = trellis::staging::quarantine::resume_transform(d.pool(), "one")
            .await
            .expect_err("the re-keying is refused")
            .to_string();
        assert!(
            refusal.contains("public.one")
                && refusal.contains("\"x\"")
                && refusal.contains(blocker),
            "{blocker:?} in {refusal}"
        );
        let drop = if blocker.starts_with("view") {
            "drop view public.one_x"
        } else {
            "drop index public.one_x_ix"
        };
        exec(&d, drop).await;
    }
    for name in ["one", "g"] {
        trellis::staging::quarantine::resume_transform(d.pool(), name)
            .await
            .expect("resume");
    }
    settle(&mut d).await;
    assert_converged(&d).await;
    assert_eq!(
        d.rows(
            "select pg_get_constraintdef(oid) from pg_constraint \
             where conrelid = 'public.one'::regclass and contype in ('p', 'u')"
        )
        .await,
        ["(\"PRIMARY KEY (id)\")"],
        "re-keyed in place"
    );
    assert_eq!(
        d.rows(
            "select attname from pg_attribute where attrelid = 'public.one'::regclass \
             and attnum > 0 and not attisdropped order by attnum"
        )
        .await,
        ["(id)", "(ga)", "(y)"],
        "x, which the transform no longer creates, is dropped"
    );
    assert_eq!(
        d.rows(
            "select indexrelid::regclass::text from pg_index \
             where indrelid = 'public.one'::regclass and not indisprimary"
        )
        .await,
        ["(one_ga)"],
        "the user's index stays"
    );
    assert_eq!(d.rows(one_acl).await, acl, "the grant stays");

    // The live path keys by `(id)` now: an update of the INCLUDE column,
    // and a NULL in it, are updates of the same row.
    exec(
        &d,
        "alter table public.inc alter column x drop not null; \
         update public.inc set x = x + 1, y = y + 1 where id = 1; \
         update public.inc set x = null where id = 3; \
         insert into public.inc values (5, null, null, 5)",
    )
    .await;
    settle(&mut d).await;
    assert_converged(&d).await;
}

/// Only the index an earlier release keyed the source by counts: a source
/// whose primary key has no `INCLUDE` columns, with a covering index (unique
/// or not) over the key's columns that has one, was never built over it, so
/// attaching pauses nothing.
#[tokio::test]
async fn a_covering_index_beside_the_key_pauses_nothing_at_attach() {
    let mut d = Driver::start(
        "create table public.inc (id integer not null, ga integer, x integer, \
             y integer, primary key (id))",
        &[("id", int4()), ("ga", int4()), ("x", int4()), ("y", int4())],
        &[ONE, AGG],
        &["public.inc"],
    )
    .await;
    exec(
        &d,
        "insert into public.inc values (1, 1, 10, 1), (2, 1, 20, 2); \
         create index inc_cover on public.inc (id) include (x); \
         create unique index inc_cover_u on public.inc (id) include (y)",
    )
    .await;
    d.settle().await;
    assert_converged(&d).await;
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
        ["(public.one,live)", "(public.g,live)"],
        "{:?}",
        d.rows("select transform_id, error from capture_failures")
            .await
    );
}

/// A 1-1 transform reading the 1-1 target an earlier release keyed by
/// `(id, x)` was keyed by `(id, x)` too. Attaching pauses it as well, naming
/// the upstream. Its `RESUME` waits for the upstream's, then re-keys it the
/// same way.
#[tokio::test]
async fn a_one_to_one_reader_of_a_mis_keyed_target_pauses_and_resume_re_keys_it() {
    const TWO: &str = "TRANSFORM two FROM public.one SELECT ga AS ga, y AS y";
    const TWO_ACTUAL: &str = "select id, ga, y from public.two order by id";
    let mut d = Driver::start(
        "create table public.inc (id integer not null, ga integer, x integer not null, \
             y integer, primary key (id, x))",
        &[("id", int4()), ("ga", int4()), ("x", int4()), ("y", int4())],
        &[ONE],
        &["public.inc"],
    )
    .await;
    exec(
        &d,
        "insert into public.inc values (1, 1, 10, 1), (2, 1, 20, 2)",
    )
    .await;
    d.settle().await;
    let columns = std::collections::HashMap::from([
        ("id".to_string(), int4()),
        ("x".to_string(), int4()),
        ("ga".to_string(), int4()),
        ("y".to_string(), int4()),
    ]);
    trellis::defs::install_definition(d.pool(), TWO, &columns, "public")
        .await
        .expect("install two");
    trellis::intake::harness::settle_builds(d.pool()).await;
    settle(&mut d).await;
    assert_eq!(
        statuses(&d).await,
        ["(public.one,live)", "(public.two,live)"]
    );

    exec(
        &d,
        "alter table public.inc drop constraint inc_pkey, add primary key (id) include (x)",
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
        ["(public.one,paused)", "(public.two,paused)"]
    );
    let reason = d
        .rows("select error from capture_failures order by transform_id offset 1")
        .await;
    assert!(
        reason[0].contains("transform public.two reads public.one")
            && reason[0].contains("public.inc's key with its INCLUDE columns (x)")
            && reason[0].contains("RESUME public.one first"),
        "{reason:?}"
    );
    assert_eq!(
        d.rows(
            "select table_name, key_columns::text from definition_keys \
             where table_name in ('public.one', 'public.two') order by 1"
        )
        .await,
        ["(public.one,{id})", "(public.two,{id})"],
        "the reader's corrected key is recorded"
    );

    exec(&d, "update public.inc set y = 70 where id = 1").await;
    settle(&mut d).await;
    // The reader's resume waits for its upstream to be live, and re-keys
    // it then.
    for name in ["two", "one"] {
        trellis::staging::quarantine::resume_transform(d.pool(), name)
            .await
            .expect("resume");
    }
    settle(&mut d).await;
    settle(&mut d).await;
    exec(
        &d,
        "update public.inc set x = x + 1, y = 80 where id = 2; \
         insert into public.inc values (3, 2, 30, 3)",
    )
    .await;
    settle(&mut d).await;
    assert_eq!(
        statuses(&d).await,
        ["(public.one,live)", "(public.two,live)"],
        "{:?}",
        d.rows("select transform_id, error from capture_failures")
            .await
    );
    assert_eq!(d.rows(TWO_ACTUAL).await, d.rows(ONE_EXPECTED).await);
    assert_eq!(
        d.rows(
            "select pg_get_constraintdef(oid) from pg_constraint \
             where conrelid = 'public.two'::regclass and contype in ('p', 'u')"
        )
        .await,
        ["(\"PRIMARY KEY (id)\")"],
        "re-keyed in place"
    );
}
