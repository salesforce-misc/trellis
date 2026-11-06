//! A target's row identity is the key define created for it, whatever
//! unique indexes or constraints a user adds to the target and whatever
//! order their OIDs end up in (#1719). `REINDEX INDEX CONCURRENTLY` of the
//! key gives its index a new OID, newer than a unique index the user added
//! earlier; the `RESUME`'s rebuild that follows must still key the target by
//! the definition's key.
//!
//! An expression unique index is never a key. The tests of a user's
//! `unique nulls not distinct` constraint older than the key are ignored
//! until #1719's design decision picks how a target's key is told apart
//! from one.
//!
//! Every test drives the pipeline by hand through `support/drain_driver.rs`.
//! None sleeps or polls for convergence.

#[path = "support/drain_driver.rs"]
mod drain_driver;

use drain_driver::Driver;
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ValueType;
use trellis::integer::IntWidth;
use trellis::staging::build;

const DDL: &str = "create table public.a (id integer primary key, ga integer, k integer, \
                       x integer); \
                   create table public.b (id integer primary key, k integer, gb integer, \
                       y integer); \
                   create index on public.a (k); create index on public.b (k)";

const AGG: &str = "TRANSFORM agg FROM public.a GROUP BY ga WITHOUT SOURCE INDEX \
                   SELECT COUNT(*) AS n, SUM(x) AS sx";
const AGG_ACTUAL: &str = "select ga, n, sx from public.agg order by ga";
const AGG_EXPECTED: &str = "select ga, count(*), sum(x) from public.a group by ga order by ga";

const JOINED: &str = "TRANSFORM j FROM public.a JOIN public.b ON a.k = b.k GROUP BY a.ga \
                      WITHOUT SOURCE INDEX SELECT a.ga AS ga, COUNT(*) AS n, SUM(b.y) AS sy";
const JOINED_ACTUAL: &str = "select ga, n, sy from public.j order by ga";
const JOINED_EXPECTED: &str = "select a.ga, count(*), sum(b.y) from public.a \
                               join public.b on a.k = b.k group by a.ga order by a.ga";

const ONE: &str = "TRANSFORM one FROM public.a SELECT ga AS ga, x AS x";
const ONE_ACTUAL: &str = "select id, ga, x from public.one order by id";
const ONE_EXPECTED: &str = "select id, ga, x from public.a order by id";

/// A 1-1 target over the aggregate: keyed by the aggregate's nullable
/// group key, so by a `unique nulls not distinct` key, not a primary key.
const MIRROR: &str = "TRANSFORM mirror FROM public.agg SELECT n AS n, sx AS sx";
const MIRROR_ACTUAL: &str = "select ga, n, sx from public.mirror order by ga";

const SEED: &str = "insert into public.a values (1, 1, 1, 10), (2, 1, 2, 5), (3, 2, 1, 7), \
                        (4, null, 2, 3); \
                    insert into public.b values (1, 1, 100, 1), (2, 2, 200, 2)";

fn int4() -> ValueType {
    ValueType::Integer(IntWidth::Int4)
}

fn int8() -> ValueType {
    ValueType::Integer(IntWidth::Int8)
}

async fn start(definitions: &[&str]) -> Driver {
    let mut d = Driver::start(
        DDL,
        &[("id", int4()), ("ga", int4()), ("k", int4()), ("x", int4())],
        definitions,
        &["public.a", "public.b"],
    )
    .await;
    exec(&d, SEED).await;
    d.settle().await;
    d
}

/// [`start`] with [`AGG`], then [`MIRROR`] defined over its live target.
async fn start_chain() -> Driver {
    let mut d = start(&[AGG]).await;
    let columns = std::collections::HashMap::from([
        ("ga".to_string(), int4()),
        ("n".to_string(), int8()),
        ("sx".to_string(), int8()),
    ]);
    trellis::defs::install_definition(d.pool(), MIRROR, &columns, "public")
        .await
        .expect("install the mirror");
    trellis::intake::harness::settle_builds(d.pool()).await;
    d.settle().await;
    build::settle_builds(d.pool()).await;
    d.settle().await;
    assert_live(&d).await;
    assert_eq!(d.rows(MIRROR_ACTUAL).await, d.rows(AGG_ACTUAL).await);
    d
}

async fn exec(d: &Driver, sql: &str) {
    d.user().await.batch_execute(sql).await.expect(sql);
}

/// The index backing `target`'s key, as define created it: its only unique
/// index before the test adds one.
async fn key_index(d: &Driver, target: &str) -> String {
    let rows = d
        .rows(&format!(
            "select indexrelid::regclass::text from pg_index \
             where indrelid = '{target}'::regclass and indisunique"
        ))
        .await;
    assert_eq!(rows.len(), 1, "{target} has one unique index: {rows:?}");
    rows[0].trim_matches(|c| c == '(' || c == ')').to_string()
}

/// Adds the user's unique index or constraint (`user_sql`) to `target`,
/// then rebuilds `target`'s key concurrently, so the key's index is newer
/// than the user's.
async fn reindex_key_after(d: &Driver, target: &str, user_sql: &str) {
    let key = key_index(d, target).await;
    exec(d, user_sql).await;
    exec(d, &format!("reindex index concurrently {key}")).await;
}

/// Pauses `name` by hand, the way a capture disruption does, resumes it, and
/// runs its rebuild and everything downstream to quiescence.
async fn pause_and_resume(d: &mut Driver, name: &str) {
    d.ctl
        .execute(
            "update transform_definitions set status = 'paused' \
             where target_table = 'public.' || $1",
            &[&name],
        )
        .await
        .expect("pause");
    trellis::staging::quarantine::resume_transform(d.pool(), name)
        .await
        .expect("resume");
    let pool = d.pool().clone();
    trellis::client::reconcile_pass(&mut d.ctl, &pool, DEFAULT_SCHEMA)
        .await
        .expect("reconcile pass");
    build::settle_builds(d.pool()).await;
    d.settle().await;
    build::settle_builds(d.pool()).await;
    d.settle().await;
}

async fn assert_live(d: &Driver) {
    assert_eq!(
        d.rows(
            "select d.target_table, d.status, \
                    (select string_agg(c.last_error, '; ') from backfill_chunks c \
                     where c.definition_id = d.id) \
             from transform_definitions d where d.status <> 'live' order by d.id"
        )
        .await,
        Vec::<String>::new(),
        "every definition is live"
    );
}

/// Writes after the resume, so the live path keys its writes too.
async fn write_more(d: &mut Driver) {
    exec(
        d,
        "insert into public.a values (5, 3, 1, 1), (6, null, 2, 2); \
         update public.a set x = x + 1 where id = 1; delete from public.a where id = 3",
    )
    .await;
    d.settle().await;
}

async fn aggregate_case(user_sql: &str) {
    let mut d = start(&[AGG]).await;
    reindex_key_after(&d, "public.agg", user_sql).await;
    pause_and_resume(&mut d, "agg").await;
    assert_live(&d).await;
    assert_eq!(d.rows(AGG_ACTUAL).await, d.rows(AGG_EXPECTED).await);
    write_more(&mut d).await;
    assert_live(&d).await;
    assert_eq!(d.rows(AGG_ACTUAL).await, d.rows(AGG_EXPECTED).await);
}

async fn join_case(user_sql: &str) {
    let mut d = start(&[JOINED]).await;
    reindex_key_after(&d, "public.j", user_sql).await;
    pause_and_resume(&mut d, "j").await;
    assert_live(&d).await;
    assert_eq!(d.rows(JOINED_ACTUAL).await, d.rows(JOINED_EXPECTED).await);
    write_more(&mut d).await;
    assert_live(&d).await;
    assert_eq!(d.rows(JOINED_ACTUAL).await, d.rows(JOINED_EXPECTED).await);
}

/// An expression unique index has no plain column to key by.
#[tokio::test]
async fn an_aggregate_keeps_its_group_key_beside_an_older_expression_unique_index() {
    aggregate_case("create unique index agg_expr on public.agg ((ga * 2))").await;
}

/// A user's `unique nulls not distinct` constraint over other columns.
#[tokio::test]
#[ignore = "#1719: a unique constraint older than the key awaits its design decision"]
async fn an_aggregate_keeps_its_group_key_beside_an_older_unique_constraint() {
    aggregate_case(
        "alter table public.agg add constraint agg_user unique nulls not distinct (n, ga)",
    )
    .await;
}

#[tokio::test]
async fn a_join_keeps_its_group_key_beside_an_older_expression_unique_index() {
    join_case("create unique index j_expr on public.j ((ga * 2))").await;
}

#[tokio::test]
#[ignore = "#1719: a unique constraint older than the key awaits its design decision"]
async fn a_join_keeps_its_group_key_beside_an_older_unique_constraint() {
    join_case("alter table public.j add constraint j_user unique nulls not distinct (n, ga)").await;
}

/// A 1-1 target over an aggregate is keyed by the aggregate's nullable
/// group key, with a `unique nulls not distinct` key of its own.
#[tokio::test]
#[ignore = "#1719: a unique constraint older than the key awaits its design decision"]
async fn a_one_to_one_target_without_a_primary_key_keeps_its_key() {
    let mut d = start_chain().await;
    reindex_key_after(
        &d,
        "public.mirror",
        "alter table public.mirror add constraint mirror_user unique nulls not distinct (n, ga)",
    )
    .await;
    pause_and_resume(&mut d, "mirror").await;
    assert_live(&d).await;
    assert_eq!(d.rows(MIRROR_ACTUAL).await, d.rows(AGG_ACTUAL).await);
    write_more(&mut d).await;
    assert_live(&d).await;
    assert_eq!(d.rows(MIRROR_ACTUAL).await, d.rows(AGG_ACTUAL).await);
}

/// A 1-1 target keyed by a primary key is keyed by it, whatever unique
/// index the user adds.
#[tokio::test]
async fn a_one_to_one_target_keeps_its_primary_key() {
    let mut d = start(&[ONE]).await;
    reindex_key_after(
        &d,
        "public.one",
        "create unique index one_expr on public.one ((id * 2)); \
         alter table public.one add constraint one_user unique nulls not distinct (x, id)",
    )
    .await;
    pause_and_resume(&mut d, "one").await;
    assert_live(&d).await;
    assert_eq!(d.rows(ONE_ACTUAL).await, d.rows(ONE_EXPECTED).await);
    write_more(&mut d).await;
    assert_live(&d).await;
    assert_eq!(d.rows(ONE_ACTUAL).await, d.rows(ONE_EXPECTED).await);
}

/// A definition reading the aggregate's target keys its source by the same
/// key the aggregate writes by: the group key, not the user's constraint.
#[tokio::test]
#[ignore = "#1719: a unique constraint older than the key awaits its design decision"]
async fn a_reader_of_an_aggregate_target_keys_it_by_the_group_key() {
    let mut d = start_chain().await;
    reindex_key_after(
        &d,
        "public.agg",
        "alter table public.agg add constraint agg_user unique nulls not distinct (n, ga)",
    )
    .await;
    pause_and_resume(&mut d, "agg").await;
    assert_live(&d).await;
    assert_eq!(d.rows(AGG_ACTUAL).await, d.rows(AGG_EXPECTED).await);
    assert_eq!(d.rows(MIRROR_ACTUAL).await, d.rows(AGG_ACTUAL).await);
    write_more(&mut d).await;
    assert_live(&d).await;
    assert_eq!(d.rows(AGG_ACTUAL).await, d.rows(AGG_EXPECTED).await);
    assert_eq!(d.rows(MIRROR_ACTUAL).await, d.rows(AGG_ACTUAL).await);
}
