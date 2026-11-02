//! A user's function or operator in `public` can't change what a definition
//! computes (#1477).
//!
//! Postgres searches `pg_catalog` first when `search_path` doesn't list it,
//! but a better argument-type match beats schema order: with `public` on the
//! path, `public.strpos(varchar, text)` wins over `pg_catalog.strpos(text,
//! text)` for a `varchar` column, `public.+(numeric, integer)` over the
//! built-in `numeric + numeric` for `n + 1`, and `public.=(varchar, varchar)`
//! over `text = text` for `v IS DISTINCT FROM w`, which looks `=` up through
//! the path and can't be qualified. Every Trellis session has Trellis's own
//! schema alone on its path, so none of these is visible to its SQL.
//!
//! Each test creates all three shadows first, then a 1-1 target, an
//! aggregate or a `JOIN` over `varchar`/`numeric` columns that uses them in
//! its fields (and, for the aggregate and the `JOIN`, a `varchar` key, which
//! Trellis's own SQL compares with `=`), and checks the target against the
//! same expressions computed with the built-ins after each way a target is
//! written: its first build (for a 1-1 target, the chunked Re-derive build),
//! the drain of changes made while it's live, and the rebuild a `RESUME`
//! runs. `self_check` must report `Converged` after each.

#[path = "support/wait_live.rs"]
mod wait_live;

use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::staging::self_check::{CheckPageExt, SelfCheckScope};
use trellis::{Config, SelfCheckMode, SelfCheckOutcome, Trellis, TrellisOptions};

/// The shadows. Each returns a value no built-in would, so a target written
/// through one differs from the oracle's built-in result.
const SHADOWS: &str = "\
    create function public.strpos(varchar, text) returns integer \
      language sql immutable as 'select 999'; \
    create function public.shadow_plus(numeric, integer) returns numeric \
      language sql immutable as 'select 777::numeric'; \
    create operator public.+ (leftarg = numeric, rightarg = integer, \
      function = public.shadow_plus); \
    create function public.shadow_eq(varchar, varchar) returns boolean \
      language sql immutable as 'select false'; \
    create operator public.= (leftarg = varchar, rightarg = varchar, \
      function = public.shadow_eq)";

/// A connection for the test's own writes, with the database's default
/// `search_path` (`"$user", public`): the shadows are visible on it, as on
/// any application connection.
async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// A connection with `pg_catalog` alone on its path, for the oracle: it
/// computes every expression with the built-ins, and names tables qualified.
async fn connect_oracle(dsn: &str) -> Client {
    let client = connect_raw(dsn).await;
    client
        .batch_execute("set search_path to pg_catalog")
        .await
        .expect("set the oracle's search_path");
    client
}

/// Sets up the source tables and the shadows, and confirms the shadows do
/// win on a path that lists `public`, so the test proves something.
async fn set_up(raw: &Client, tables: &str) {
    raw.batch_execute(tables).await.expect("source tables");
    raw.batch_execute(SHADOWS)
        .await
        .expect("create the shadows");
    let row = raw
        .query_one(
            "select strpos('hello'::varchar, 'l'::text), (1.5::numeric + 1::integer)::text, \
                    'a'::varchar is distinct from 'a'::varchar",
            &[],
        )
        .await
        .expect("probe the shadows");
    assert_eq!(row.get::<_, i32>(0), 999, "public.strpos wins on this path");
    assert_eq!(row.get::<_, &str>(1), "777", "public.+ wins on this path");
    assert!(row.get::<_, bool>(2), "public.= wins on this path");
}

async fn define(dsn: &str, statement: &str) {
    let definer = Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect definer");
    definer
        .apply(statement)
        .await
        .unwrap_or_else(|e| panic!("{statement}: {e}"));
    definer.shutdown().await.expect("shut the definer down");
}

async fn run(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions {
            staging: true,
            drain_threads: 2,
            ..Default::default()
        },
    )
    .await
    .expect("connect the running trellis")
}

async fn converge(trellis: &Trellis) {
    let token = trellis.watermark_token().await.expect("watermark_token");
    trellis
        .await_converged(token, Duration::from_secs(60))
        .await
        .expect("the pipeline converges");
}

async fn write(raw: &Client, statements: &[&str]) {
    for sql in statements {
        raw.batch_execute(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

/// Checks `target` against `oracle` (both read on the oracle's connection,
/// with the built-ins), and `self_check`, after `phase`.
async fn check(trellis: &Trellis, oracle_db: &Client, target: &str, oracle: &str, phase: &str) {
    let actual = format!("select t::text from public.{target} t");
    let differences: i64 = oracle_db
        .query_one(
            &format!(
                "with expected as (select e::text from ({oracle}) e), \
                      actual as ({actual}) \
                 select (select count(*) from (table expected except table actual) missing) \
                      + (select count(*) from (table actual except table expected) extra)"
            ),
            &[],
        )
        .await
        .unwrap_or_else(|e| panic!("{phase}: difference query for {target}: {e}"))
        .get(0);
    if differences != 0 {
        let rows: Vec<String> = oracle_db
            .query(&format!("{actual} order by 1"), &[])
            .await
            .expect("read the target")
            .iter()
            .map(|row| row.get(0))
            .collect();
        panic!("{phase}: {target} differs from the built-ins' result: {rows:?}");
    }
    let page = trellis
        .self_check_page(
            target,
            SelfCheckScope {
                after: None,
                limit: 1000,
            },
            SelfCheckMode::Strict,
            Duration::from_secs(60),
        )
        .await
        .unwrap_or_else(|e| panic!("{phase}: self_check {target}: {e}"));
    assert!(
        matches!(page.outcome, SelfCheckOutcome::Converged),
        "{phase}: self_check {target}: {:?}",
        page.outcome
    );
}

/// Pauses and resumes `target`, changing the source while it's paused so
/// the rebuild has new values to compute, and waits for it to be live again.
async fn pause_change_resume(trellis: &Trellis, raw: &Client, target: &str, changes: &[&str]) {
    trellis
        .apply(&format!("PAUSE TRANSFORM {target}"))
        .await
        .expect("pause");
    write(raw, changes).await;
    trellis
        .apply(&format!("RESUME TRANSFORM {target}"))
        .await
        .expect("resume");
    wait_live::wait_for_live(trellis, target).await;
    converge(trellis).await;
}

const SOURCE: &str = "create table public.s ( \
     id bigint primary key, g varchar not null, v varchar, w varchar, n numeric(10, 2))";

const SEED: &str = "insert into public.s values \
     (1, 'a', 'hello', 'hello', 1.5), (2, 'a', 'world', 'hello', 2.25), \
     (3, 'b', 'lol', null, null), (4, 'b', null, 'x', -4)";

const LIVE_CHANGES: &[&str] = &[
    "insert into public.s values (5, 'c', 'plain', 'plain', 10)",
    "update public.s set v = 'hell', n = n * 2 where id = 1",
    "update public.s set w = 'world' where id = 2",
    "delete from public.s where id = 4",
];

const PAUSED_CHANGES: &[&str] = &[
    "insert into public.s values (6, 'a', 'all', 'al', 0.5)",
    "update public.s set v = 'lull', g = 'c' where id = 3",
];

/// A 1-1 target's fields that call the shadowed names.
#[tokio::test]
async fn a_one_to_one_target_computes_with_the_builtins() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    let oracle_db = connect_oracle(db.dsn()).await;
    set_up(&raw, SOURCE).await;
    write(&raw, &[SEED]).await;
    define(
        db.dsn(),
        "TRANSFORM one FROM s SELECT g AS g, STRPOS(v, 'l') AS p, n + 1 AS n1, \
         v IS DISTINCT FROM w AS d, CASE WHEN v IS NOT DISTINCT FROM w THEN n + 2 END AS c",
    )
    .await;
    let oracle = "select id, g, strpos(v, 'l'), n + 1, v is distinct from w, \
                  case when v is not distinct from w then n + 2 end from public.s";
    let trellis = run(db.dsn()).await;
    wait_live::wait_for_live(&trellis, "one").await;
    check(&trellis, &oracle_db, "one", oracle, "build").await;

    write(&raw, LIVE_CHANGES).await;
    converge(&trellis).await;
    check(&trellis, &oracle_db, "one", oracle, "drain").await;

    pause_change_resume(&trellis, &raw, "one", PAUSED_CHANGES).await;
    check(&trellis, &oracle_db, "one", oracle, "RESUME").await;
    trellis.shutdown().await.expect("shutdown");
}

/// An aggregate grouped by a `varchar` key, with the shadowed names in its
/// arguments and around its aggregates.
#[tokio::test]
async fn an_aggregate_computes_with_the_builtins() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    let oracle_db = connect_oracle(db.dsn()).await;
    set_up(&raw, SOURCE).await;
    write(&raw, &[SEED]).await;
    define(
        db.dsn(),
        "TRANSFORM agg FROM s GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, \
         SUM(STRPOS(v, 'l')) AS sp, SUM(CASE WHEN n + 1 > 3 THEN 1 ELSE 0 END) AS sn, SUM(n) + 1 AS composed, \
         SUM(CASE WHEN v IS DISTINCT FROM w THEN 1 ELSE 0 END) AS distinct_pairs, \
         COUNT(*) AS c",
    )
    .await;
    let oracle = "select g, sum(strpos(v, 'l')), sum(case when n + 1 > 3 then 1 else 0 end), sum(n) + 1, \
                  sum(case when v is distinct from w then 1 else 0 end), count(*) \
                  from public.s group by g";
    let trellis = run(db.dsn()).await;
    wait_live::wait_for_live(&trellis, "agg").await;
    check(&trellis, &oracle_db, "agg", oracle, "build").await;

    write(&raw, LIVE_CHANGES).await;
    converge(&trellis).await;
    check(&trellis, &oracle_db, "agg", oracle, "drain").await;

    pause_change_resume(&trellis, &raw, "agg", PAUSED_CHANGES).await;
    check(&trellis, &oracle_db, "agg", oracle, "RESUME").await;
    trellis.shutdown().await.expect("shutdown");
}

/// A `JOIN` on a `varchar` key, with the shadowed names in each side's
/// aggregate arguments and around its aggregates.
#[tokio::test]
async fn a_join_computes_with_the_builtins() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    let oracle_db = connect_oracle(db.dsn()).await;
    set_up(
        &raw,
        "create table public.fa (id integer primary key, k varchar, v varchar, n numeric(10, 2)); \
         create table public.db (id integer primary key, k varchar, w varchar); \
         create index on public.db (k)",
    )
    .await;
    write(
        &raw,
        &[
            "insert into public.fa values (1, 'k1', 'hello', 1.5), (2, 'k1', 'lol', 2), \
             (3, 'k2', 'world', null), (4, 'k3', null, 7)",
            "insert into public.db values (1, 'k1', 'hello'), (2, 'k1', 'k1'), \
             (3, 'k2', 'world'), (4, 'k9', 'y')",
        ],
    )
    .await;
    define(
        db.dsn(),
        "TRANSFORM j FROM public.fa JOIN public.db ON fa.k = db.k GROUP BY fa.k \
         WITHOUT SOURCE INDEX SELECT fa.k AS k, SUM(STRPOS(fa.v, 'l')) AS sp, \
         SUM(CASE WHEN fa.n + 1 > 3 THEN 1 ELSE 0 END) AS sn, SUM(fa.n) + 1 AS composed, \
         SUM(CASE WHEN db.w IS DISTINCT FROM db.k THEN 1 ELSE 0 END) AS distinct_pairs, \
         COUNT(*) AS c",
    )
    .await;
    let oracle = "select fa.k, sum(strpos(fa.v, 'l')), sum(case when fa.n + 1 > 3 then 1 else 0 end), sum(fa.n) + 1, \
                  sum(case when db.w is distinct from db.k then 1 else 0 end), count(*) \
                  from public.fa join public.db on fa.k = db.k group by fa.k";
    let trellis = run(db.dsn()).await;
    wait_live::wait_for_live(&trellis, "j").await;
    check(&trellis, &oracle_db, "j", oracle, "build").await;

    write(
        &raw,
        &[
            "insert into public.db values (5, 'k3', 'z')",
            "update public.fa set v = 'hell', n = n * 2 where id = 1",
            "update public.db set w = 'lol' where id = 2",
            "delete from public.fa where id = 3",
            "insert into public.fa values (5, 'k9', 'all', 0.5)",
        ],
    )
    .await;
    converge(&trellis).await;
    check(&trellis, &oracle_db, "j", oracle, "drain").await;

    pause_change_resume(
        &trellis,
        &raw,
        "j",
        &[
            "update public.fa set k = 'k2' where id = 2",
            "insert into public.db values (6, 'k2', 'lol')",
        ],
    )
    .await;
    check(&trellis, &oracle_db, "j", oracle, "RESUME").await;
    trellis.shutdown().await.expect("shutdown");
}
