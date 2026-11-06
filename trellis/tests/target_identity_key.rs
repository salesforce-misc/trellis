//! A target's row identity is the key define created for it, whatever
//! unique indexes or constraints a user adds to the target and whatever
//! order their OIDs end up in (#1719). `REINDEX INDEX CONCURRENTLY` of the
//! key gives its index a new OID, newer than a unique index the user added
//! earlier; the `RESUME`'s rebuild that follows must still key the target by
//! the definition's key.
//!
//! An expression unique index is never a key, and a user's `unique nulls
//! not distinct` constraint older than the key isn't either: define records
//! the key it chose for each table (`definition_keys`), and the identity is
//! the index over exactly those columns.
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
async fn a_join_keeps_its_group_key_beside_an_older_unique_constraint() {
    join_case("alter table public.j add constraint j_user unique nulls not distinct (n, ga)").await;
}

/// A 1-1 target over an aggregate is keyed by the aggregate's nullable
/// group key, with a `unique nulls not distinct` key of its own.
#[tokio::test]
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

/// The source columns of `public.a`, for a define the test runs itself.
fn a_columns() -> std::collections::HashMap<String, ValueType> {
    std::collections::HashMap::from([
        ("id".to_string(), int4()),
        ("ga".to_string(), int4()),
        ("k".to_string(), int4()),
        ("x".to_string(), int4()),
    ])
}

/// An adopted target whose user created a `unique nulls not distinct`
/// constraint before the group key: the target is keyed by the group key.
#[tokio::test]
async fn an_adopted_aggregate_keeps_its_group_key_beside_an_older_user_constraint() {
    let mut d = start(&[]).await;
    exec(
        &d,
        "create table public.agg (ga integer, n bigint, sx bigint, \
             constraint agg_user unique nulls not distinct (n, ga)); \
         alter table public.agg add constraint agg_key unique nulls not distinct (ga)",
    )
    .await;
    trellis::defs::install_definition(
        d.pool(),
        "TRANSFORM agg ADOPT EXISTING TABLE FROM public.a GROUP BY ga WITHOUT SOURCE INDEX \
         SELECT COUNT(*) AS n, SUM(x) AS sx",
        &a_columns(),
        "public",
    )
    .await
    .expect("adopt");
    let pool = d.pool().clone();
    trellis::client::reconcile_pass(&mut d.ctl, &pool, DEFAULT_SCHEMA)
        .await
        .expect("reconcile pass");
    build::settle_builds(d.pool()).await;
    d.settle().await;
    assert_live(&d).await;
    assert_eq!(d.rows(AGG_ACTUAL).await, d.rows(AGG_EXPECTED).await);
    write_more(&mut d).await;
    assert_live(&d).await;
    assert_eq!(d.rows(AGG_ACTUAL).await, d.rows(AGG_EXPECTED).await);
}

const RECORDED: &str = "select d.target_table, k.table_name, k.key_columns::text \
                        from definition_keys k join transform_definitions d \
                          on d.id = k.transform_id order by 1, 2";

/// Define records the key of every table a definition keys by, and a
/// definition stored before the record existed gets the same record when a
/// binary carrying it attaches, from its parsed text: its target's group
/// key even where the oldest unique index is a user's, and the source key
/// of a reader of that target from the aggregate's record.
#[tokio::test]
async fn an_existing_definition_gets_its_key_record_at_attach() {
    let d = start_chain().await;
    let defined = d.rows(RECORDED).await;
    assert_eq!(
        defined,
        [
            "(public.agg,public.a,{id})",
            "(public.agg,public.agg,{ga})",
            "(public.mirror,public.agg,{ga})",
            "(public.mirror,public.mirror,{ga})",
        ]
    );
    reindex_key_after(
        &d,
        "public.agg",
        "alter table public.agg add constraint agg_user unique nulls not distinct (n, ga)",
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
    assert_eq!(d.rows(RECORDED).await, defined);
    assert_live(&d).await;
}

/// The recorded key's index dropped: `RESUME` refuses, naming the table,
/// the recorded key and the index left, rather than key the target by the
/// user's constraint. Restored, it resumes; dropped again before the
/// rebuild starts, the start pauses the definition with the same reason.
#[tokio::test]
async fn a_resume_and_a_build_refuse_when_the_recorded_key_has_no_index() {
    let mut d = start(&[AGG]).await;
    let key = key_index(&d, "public.agg").await;
    exec(
        &d,
        &format!(
            "alter table public.agg add constraint agg_user unique nulls not distinct (n, ga); \
             alter table public.agg drop constraint {key}"
        ),
    )
    .await;
    d.ctl
        .execute(
            "update transform_definitions set status = 'paused' where target_table = 'public.agg'",
            &[],
        )
        .await
        .expect("pause");
    let refusal = trellis::staging::quarantine::resume_transform(d.pool(), "agg")
        .await
        .expect_err("the resume is refused")
        .to_string();
    for part in [
        "table 'public.agg'",
        "at define (ga)",
        "its unique indexes are agg_user (n, ga)",
    ] {
        assert!(refusal.contains(part), "{part:?} in {refusal}");
    }

    exec(
        &d,
        "alter table public.agg add constraint agg_ga_key unique nulls not distinct (ga)",
    )
    .await;
    trellis::staging::quarantine::resume_transform(d.pool(), "agg")
        .await
        .expect("resumes with the key back");
    exec(&d, "alter table public.agg drop constraint agg_ga_key").await;
    let pool = d.pool().clone();
    trellis::client::reconcile_pass(&mut d.ctl, &pool, DEFAULT_SCHEMA)
        .await
        .expect("reconcile pass");
    let paused = d
        .rows(
            "select d.status, cf.error from transform_definitions d \
             join capture_failures cf on cf.transform_id = d.id \
             where d.target_table = 'public.agg'",
        )
        .await;
    assert_eq!(paused.len(), 1, "{paused:?}");
    assert!(
        paused[0].starts_with("(paused,") && paused[0].contains("at define (ga)"),
        "the start paused it by name: {paused:?}"
    );
}
