//! A key column re-typed after define, or a column Trellis keeps a typed
//! copy of widened, pauses the definitions it concerns, and a resume
//! re-validates and rebuilds them (issues #760, #767, #708).
//!
//! Define refuses a key, join or `GROUP BY` column whose type or collation
//! Trellis can't match by. An `ALTER COLUMN ... TYPE` or `COLLATE` after
//! define can give one such a type, break a relationship's same-type
//! pairing, render the keys Trellis already stored differently, or outgrow
//! a copy Trellis keeps of the column. The staging worker's capture pass
//! pauses the definitions concerned, with the reason on `Trellis::status`.
//! A resume refuses while define would; otherwise it re-types Trellis's
//! copies (through the staging worker) and rebuilds.
//!
//! Every test is stepped by hand: writes, then explicit capture passes,
//! drains and backfill runs. Nothing polls for convergence (#297).

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::capture::reconcile;
use trellis::defs::TransformStatus;
use trellis::{Config, Trellis, TrellisError, TrellisOptions};

const SCHEMA: &str = "trellis";

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {SCHEMA}, public"))
        .await
        .expect("set search_path");
    client
}

async fn definer(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a definer")
}

/// One whole staging-worker pass: capture, markers, discharge.
async fn full_pass(raw: &mut Client, pool: &trellis::Pool) {
    trellis::client::reconcile_pass(
        raw,
        pool,
        SCHEMA,
        "capture_key_retype_wake",
        Duration::from_secs(2),
    )
    .await
    .expect("reconcile pass");
}

/// The capture half of one pass only. Asserts no table failed.
async fn capture_pass(raw: &mut Client, pool: &trellis::Pool) {
    let desired = trellis::defs::tables_to_capture(pool)
        .await
        .expect("read the tables to capture");
    let outcome = reconcile::reconcile(
        raw,
        SCHEMA,
        &desired,
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .expect("capture pass");
    assert!(outcome.failed.is_empty(), "{:?}", outcome.failed);
}

async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    use trellis::staging::{StagedWatermark, apply, has_pending, retire_drained_segments, seal};
    let watermark = StagedWatermark::saturated();
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, "capture_key_retype_wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "capture_key_retype_test",
            1,
            "trellis_capture_key_retype_test",
            &watermark,
        )
        .await
        .expect("drain_once")
        .is_some()
        {}
        retire_drained_segments(client)
            .await
            .expect("retire drained segments");
        if !has_pending(client).await.expect("has_pending") {
            return;
        }
    }
    panic!("the ring did not reach quiescence within 16 seal/drain rounds");
}

/// Drains the active segment, whose rows include a key whose write fails:
/// each failed drain charges the key a death, until it is quarantined and
/// the drain goes past it. Bounded attempts, not a timed wait. The
/// quarantined key's held rows keep the ring from quiescence.
async fn drain_past_failures(pool: &trellis::Pool, client: &mut Client) {
    use trellis::staging::{StagedWatermark, apply, retire_drained_segments, seal};
    let watermark = StagedWatermark::saturated();
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq, "capture_key_retype_wake")
        .await
        .expect("seal phase 2");
    let mut drained = false;
    for _ in 0..16 {
        match apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "capture_key_retype_test",
            1,
            "trellis_capture_key_retype_test",
            &watermark,
        )
        .await
        {
            Ok(Some(_)) | Err(_) => {}
            Ok(None) => {
                drained = true;
                break;
            }
        }
    }
    assert!(drained, "the segment did not drain within 16 attempts");
    retire_drained_segments(client)
        .await
        .expect("retire drained segments");
}

/// Claims, runs and finishes every pending backfill chunk.
async fn run_backfill_chunks(pool: &trellis::Pool) {
    use trellis::defs::chunk_queue;
    const CLAIMED_BY: &str = "capture_key_retype_backfill";
    loop {
        let client = pool.get().await.expect("acquire connection");
        let claimed = chunk_queue::claim_chunks(&**client, CLAIMED_BY, 1000)
            .await
            .expect("claim_chunks");
        drop(client);
        if claimed.is_empty() {
            trellis::staging::build::settle_builds(pool).await;
            return;
        }
        for chunk in &claimed {
            chunk_queue::run_claimed_chunk(
                pool,
                chunk,
                CLAIMED_BY,
                Duration::from_secs(5),
                Duration::from_secs(60),
            )
            .await
            .expect("run_claimed_chunk");
            chunk_queue::finish_chunk(pool, chunk, CLAIMED_BY)
                .await
                .expect("finish_chunk");
        }
    }
}

async fn status(raw: &Client, target: &str) -> TransformStatus {
    let text: String = raw
        .query_one(
            "select status from transform_definitions where split_part(target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .expect("read status")
        .get(0);
    TransformStatus::from_persisted(&text).expect("known status")
}

/// Alternates staging-worker passes and drains until every one of
/// `targets` is live: a bounded number of explicit steps, not a timed wait.
async fn bring_live(raw: &mut Client, pool: &trellis::Pool, targets: &[&str]) {
    for _ in 0..8 {
        full_pass(raw, pool).await;
        run_backfill_chunks(pool).await;
        drain_to_quiescence(pool, raw).await;
        let mut all_live = true;
        for target in targets {
            all_live &= status(raw, target).await == TransformStatus::Live;
        }
        if all_live {
            return;
        }
    }
    for target in targets {
        eprintln!("{target}: {:?}", status(raw, target).await);
    }
    panic!("{targets:?} did not all go live within 8 pass/drain rounds");
}

async fn rows(raw: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
    raw.query(sql, &[])
        .await
        .expect(sql)
        .into_iter()
        .map(|row| (0..row.len()).map(|i| row.get(i)).collect())
        .collect()
}

/// The reason `target` is paused with, asserting it's paused for a capture
/// failure on `table` about `columns`.
async fn paused_for(trellis: &Trellis, target: &str, table: &str, columns: &[&str]) -> String {
    let reported = trellis.status(target).await.expect("status").expect(target);
    assert_eq!(reported.status, TransformStatus::Paused, "{target}");
    let failure = reported
        .capture_failure
        .unwrap_or_else(|| panic!("{target}'s reason is reported"));
    assert_eq!(failure.source_table, table, "{target}");
    assert_eq!(
        failure.columns,
        columns.iter().map(|c| c.to_string()).collect::<Vec<_>>(),
        "{target}"
    );
    failure.error
}

/// `RESUME TRANSFORM target`, which must succeed.
async fn resume(trellis: &Trellis, target: &str) {
    trellis
        .apply(&format!("RESUME TRANSFORM {target}"))
        .await
        .unwrap_or_else(|err| panic!("resume {target}: {err}"));
}

/// `RESUME TRANSFORM target`, which must be refused: returns the message.
async fn resume_refused(trellis: &Trellis, target: &str) -> String {
    match trellis.apply(&format!("RESUME TRANSFORM {target}")).await {
        Err(
            err @ TrellisError::Apply(trellis::staging::apply::ApplyError::ResumeRefused { .. }),
        ) => err.to_string(),
        other => panic!("expected the resume of {target} to be refused, got {other:?}"),
    }
}

/// `table.column`'s type, as `format_type` renders it.
async fn column_type(raw: &Client, table: &str, column: &str) -> String {
    raw.query_one(
        "select pg_catalog.format_type(atttypid, atttypmod) from pg_catalog.pg_attribute \
         where attrelid = pg_catalog.to_regclass($1) and attname = $2",
        &[&table, &column],
    )
    .await
    .unwrap_or_else(|err| panic!("{table}.{column}: {err}"))
    .get(0)
}

/// Whether `target`'s resume waits on the staging worker to re-type its
/// copies: it is paused, its reason says so, and its request is recorded.
async fn assert_retyping(trellis: &Trellis, raw: &Client, target: &str) -> String {
    let reported = trellis.status(target).await.expect("status").expect(target);
    assert_eq!(reported.status, TransformStatus::Paused, "{target}");
    let error = reported.capture_failure.expect("reason").error;
    assert!(error.starts_with("resuming:"), "{error}");
    let requested: i64 = raw
        .query_one(
            "select count(*) from resume_requests r join transform_definitions d \
             on d.id = r.transform_id where split_part(d.target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .expect("read requests")
        .get(0);
    assert_eq!(requested, 1, "{target}");
    error
}

/// `public.users` and `public.posts`, related by `posts.author` to
/// `users.handle`, with three definitions, all live:
///
/// - `post_authors` reads `author.name` through the relationship;
/// - `post_titles` reads only `posts`;
/// - `per_kind` counts `posts` per `kind`.
async fn setup(dsn: &str, raw: &mut Client, pool: &trellis::Pool) -> Trellis {
    raw.batch_execute(
        "create table public.users (handle varchar(8) primary key, name text); \
         create table public.posts (id int primary key, author varchar(8), title text, \
                                    kind int, price numeric(10,2)); \
         insert into public.users values ('ann', 'Ann'), ('bob', 'Bob'); \
         insert into public.posts values (1, 'ann', 'a', 1, 1.50), (2, 'bob', 'b', 2, 2.50), \
                                         (3, 'ann', 'c', 1, 1.50);",
    )
    .await
    .expect("seed");
    let trellis = definer(dsn).await;
    for text in [
        "RELATIONSHIP author FROM posts.author TO users.handle",
        "TRANSFORM post_authors FROM public.posts SELECT author.name AS author_name",
        "TRANSFORM post_titles FROM public.posts SELECT title AS title",
        "TRANSFORM per_kind FROM public.posts GROUP BY kind SELECT kind AS kind, COUNT(*) AS n",
    ] {
        trellis.apply(text).await.expect(text);
    }
    bring_live(raw, pool, &["post_authors", "post_titles", "per_kind"]).await;
    trellis
}

/// The issue's example: a join column altered to `character(n)` after the
/// relationship exists. `character(n)`'s `::text` drops the padding its
/// captured rendering keeps, so its keys would never match, and define
/// refuses it. The capture pass pauses only the definition that reads
/// through the relationship. A resume re-validates it as define would and
/// refuses, leaving it paused, while the column is `character(n)`; once the
/// column is back, a resume rebuilds it.
#[tokio::test]
async fn a_join_column_altered_to_character_n_pauses_its_readers_and_resume_refuses_until_it_is_changed_back()
 {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;

    raw.batch_execute("alter table public.users alter column handle type character(8)")
        .await
        .expect("re-type the to-side join column");
    capture_pass(&mut raw, &db.pool).await;

    let error = paused_for(&trellis, "post_authors", "public.users", &["handle"]).await;
    assert!(
        error.contains("character(8)")
            && error.contains("relationship 'author'")
            && error.contains("refuses until that is fixed"),
        "{error}"
    );
    assert_eq!(status(&raw, "post_titles").await, TransformStatus::Live);
    assert_eq!(status(&raw, "per_kind").await, TransformStatus::Live);

    let refused = resume_refused(&trellis, "post_authors").await;
    assert!(
        refused.contains("users.handle") && refused.contains("character(8)"),
        "{refused}"
    );
    paused_for(&trellis, "post_authors", "public.users", &["handle"]).await;

    raw.batch_execute("alter table public.users alter column handle type varchar(8)")
        .await
        .expect("change the column back");
    resume(&trellis, "post_authors").await;
    assert_eq!(
        status(&raw, "post_authors").await,
        TransformStatus::WaitingToBackfill
    );
    bring_live(&mut raw, &db.pool, &["post_authors"]).await;
    raw.batch_execute("update public.users set name = 'Annie' where handle = 'ann'")
        .await
        .expect("a write");
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        rows(
            &raw,
            "select id::text, author_name from public.post_authors order by id"
        )
        .await,
        rows(
            &raw,
            "select p.id::text, u.name from public.posts p \
             join public.users u on u.handle = p.author order by p.id"
        )
        .await,
    );
}

/// The changes no key rendering, typed copy or join pairing depends on
/// pause nothing: widening an aggregate's source key (its ledger keys it by
/// text), a `GROUP BY` key's `varchar` widening or move to `text` (its copy
/// is `text`), a wider timestamp precision or numeric precision and scale
/// on a `GROUP BY` key (its copy is the type's full precision, or
/// unconstrained `numeric`), any change to a column no definition keys by,
/// and a change between deterministic collations. Every definition stays
/// live and keeps converging.
#[tokio::test]
async fn changes_no_key_copy_or_pairing_depends_on_pause_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.events (id int primary key, kind varchar(20), at timestamp(3), \
                                     price numeric(10,2), note varchar(10)); \
         insert into public.events values (1, 'a', '2024-01-01 10:00:00.123', 1.50, 'x'), \
                                          (2, 'b', '2024-01-01 10:00:00.123', 2.50, 'y');",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    for text in [
        "TRANSFORM per_kind FROM public.events GROUP BY kind SELECT kind AS kind, COUNT(*) AS n",
        "TRANSFORM per_at FROM public.events GROUP BY at SELECT at AS at, COUNT(*) AS n",
        "TRANSFORM per_price FROM public.events GROUP BY price \
         SELECT price AS price, COUNT(*) AS n",
    ] {
        trellis.apply(text).await.expect(text);
    }
    let targets = ["per_kind", "per_at", "per_price"];
    bring_live(&mut raw, &db.pool, &targets).await;

    for alter in [
        "alter table public.events alter column id type bigint",
        "alter table public.events alter column kind type varchar(40)",
        "alter table public.events alter column kind type text",
        "alter table public.events alter column at type timestamp(6)",
        "alter table public.events alter column price type numeric(12,3)",
        "alter table public.events alter column note type varchar(5)",
        "alter table public.events alter column note type integer using 0",
        "alter table public.events alter column kind type text collate \"C\"",
    ] {
        raw.batch_execute(alter).await.expect(alter);
        capture_pass(&mut raw, &db.pool).await;
        for target in targets {
            assert_eq!(status(&raw, target).await, TransformStatus::Live, "{alter}");
        }
    }

    raw.batch_execute(
        "insert into public.events values (3000000000, 'a', '2024-01-01 10:00:00.123456', \
                                           1.505, 0); \
         update public.events set kind = 'b' where id = 1;",
    )
    .await
    .expect("writes");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    // `per_price` keeps the rendering each group was first written with
    // (#110), so its groups are compared as numbers.
    for (target, column) in [
        ("per_kind", "kind"),
        ("per_at", "at"),
        ("per_price", "(price * 1000)::bigint"),
    ] {
        assert_eq!(
            status(&raw, target).await,
            TransformStatus::Live,
            "{target}"
        );
        assert_eq!(
            rows(
                &raw,
                &format!(
                    "select {column}::text, n::text from public.{target} where n <> 0 \
                     order by 1"
                )
            )
            .await,
            rows(
                &raw,
                &format!(
                    "select {column}::text, count(*)::text from public.events \
                     group by 1 order by 1"
                )
            )
            .await,
            "{target}"
        );
    }
}

/// `public.items`, keyed by an `integer`, with a 1-1 definition whose
/// target copies the key and passes `name` (`varchar(10)`) through, live.
async fn items(dsn: &str, raw: &mut Client, pool: &trellis::Pool) -> Trellis {
    raw.batch_execute(
        "create table public.items (id int primary key, name varchar(10), qty int); \
         insert into public.items values (1, 'one', 1), (2, 'two', 2);",
    )
    .await
    .expect("seed");
    let trellis = definer(dsn).await;
    trellis
        .apply("TRANSFORM item_names FROM public.items SELECT name AS name, qty + 1 AS next")
        .await
        .expect("define item_names");
    bring_live(raw, pool, &["item_names"]).await;
    trellis
}

/// `public.item_names` against its oracle.
async fn assert_item_names_match(raw: &Client) {
    assert_eq!(
        rows(
            raw,
            "select id::text, name, next::text from public.item_names order by id"
        )
        .await,
        rows(
            raw,
            "select id::text, name::text, (qty + 1)::text from public.items order by id"
        )
        .await,
    );
}

/// Widening a 1-1 definition's source key (`integer` to `bigint`) outgrows
/// its target's key, a copy of it: the capture pass pauses the definition,
/// naming the copy and the remedy. A resume returns at once, the
/// definition still paused with its request; the staging worker's next
/// pass re-types the copy and completes the resume, and the rebuild then
/// takes a key above 2^31.
#[tokio::test]
async fn widening_a_one_to_one_key_pauses_and_resume_widens_the_target_key() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = items(db.dsn(), &mut raw, &db.pool).await;

    raw.batch_execute("alter table public.items alter column id type bigint")
        .await
        .expect("widen the key");
    capture_pass(&mut raw, &db.pool).await;
    let error = paused_for(&trellis, "item_names", "public.items", &["id"]).await;
    assert!(
        error.contains("widened to bigint")
            && error.contains("public.item_names.id (integer)")
            && error.contains("Resume the definition"),
        "{error}"
    );

    resume(&trellis, "item_names").await;
    let error = assert_retyping(&trellis, &raw, "item_names").await;
    assert!(
        error.contains("public.item_names.id from integer to bigint"),
        "{error}"
    );
    assert_eq!(
        column_type(&raw, "public.item_names", "id").await,
        "integer"
    );

    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(column_type(&raw, "public.item_names", "id").await, "bigint");
    assert_eq!(
        status(&raw, "item_names").await,
        TransformStatus::WaitingToBackfill
    );
    bring_live(&mut raw, &db.pool, &["item_names"]).await;
    raw.batch_execute("insert into public.items values (3000000000, 'big', 3)")
        .await
        .expect("a key above 2^31");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(status(&raw, "item_names").await, TransformStatus::Live);
    assert_item_names_match(&raw).await;

    // The recorded key type moved with the resume: nothing pauses again.
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(status(&raw, "item_names").await, TransformStatus::Live);
}

/// Widening a column a 1-1 target passes through (`varchar(10)` to
/// `varchar(40)`) outgrows its copy: the pass pauses the definition, and a
/// resume widens the copy and rebuilds, so a longer value lands. A
/// narrowing of a passthrough pauses nothing: every value still fits.
#[tokio::test]
async fn widening_a_passthrough_pauses_and_resume_widens_its_copy() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = items(db.dsn(), &mut raw, &db.pool).await;

    raw.batch_execute("alter table public.items alter column name type varchar(8)")
        .await
        .expect("narrow the passthrough");
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(status(&raw, "item_names").await, TransformStatus::Live);

    raw.batch_execute("alter table public.items alter column name type varchar(40)")
        .await
        .expect("widen the passthrough");
    capture_pass(&mut raw, &db.pool).await;
    let error = paused_for(&trellis, "item_names", "public.items", &["name"]).await;
    assert!(
        error.contains("widened to character varying(40)")
            && error.contains("public.item_names.name (character varying(10))"),
        "{error}"
    );

    resume(&trellis, "item_names").await;
    assert_retyping(&trellis, &raw, "item_names").await;
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        column_type(&raw, "public.item_names", "name").await,
        "character varying(40)"
    );
    bring_live(&mut raw, &db.pool, &["item_names"]).await;
    raw.batch_execute("insert into public.items values (3, 'a much longer name', 3)")
        .await
        .expect("a longer value");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(status(&raw, "item_names").await, TransformStatus::Live);
    assert_item_names_match(&raw).await;
}

/// Widening an aggregate's `GROUP BY` key (`integer` to `bigint`) outgrows
/// its copies in the target, the ledger and the group-delta table: the pass
/// pauses the aggregate, naming each, and a resume re-types all three
/// (generating the delta table's partition column again) and rebuilds.
#[tokio::test]
async fn widening_a_group_by_key_pauses_and_resume_widens_the_target_and_ledger_copies() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.orders (id int primary key, shop int, amount int); \
         insert into public.orders values (1, 1, 10), (2, 1, 20), (3, 2, 5);",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply(
            "TRANSFORM per_shop FROM public.orders GROUP BY shop \
             SELECT shop AS shop, SUM(amount) AS total, COUNT(*) AS n",
        )
        .await
        .expect("define per_shop");
    bring_live(&mut raw, &db.pool, &["per_shop"]).await;

    raw.batch_execute("alter table public.orders alter column shop type bigint")
        .await
        .expect("widen the GROUP BY key");
    capture_pass(&mut raw, &db.pool).await;
    let error = paused_for(&trellis, "per_shop", "public.orders", &["shop"]).await;
    for copy in [
        "public.per_shop.shop (integer)",
        "public.per_shop__ledger.shop (integer)",
    ] {
        assert!(error.contains(copy), "{copy}: {error}");
    }

    resume(&trellis, "per_shop").await;
    assert_retyping(&trellis, &raw, "per_shop").await;
    capture_pass(&mut raw, &db.pool).await;
    for table in ["public.per_shop", "public.per_shop__ledger"] {
        assert_eq!(column_type(&raw, table, "shop").await, "bigint", "{table}");
    }
    bring_live(&mut raw, &db.pool, &["per_shop"]).await;
    raw.batch_execute(
        "insert into public.orders values (4, 3000000000, 7), (5, 3000000000, 8); \
         update public.orders set shop = 3000000000 where id = 1;",
    )
    .await
    .expect("groups above 2^31");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(status(&raw, "per_shop").await, TransformStatus::Live);
    assert_eq!(
        rows(
            &raw,
            "select shop::text, total::text, n::text from public.per_shop where n <> 0 \
             order by shop"
        )
        .await,
        rows(
            &raw,
            "select shop::text, sum(amount)::text, count(*)::text from public.orders \
             group by shop order by shop"
        )
        .await,
    );
}

/// `public.users` and `public.posts`, related to-one by `posts.author_id`
/// to `users.id`, both `integer`, with a 1-1 definition reading
/// `author.name` through it, live.
async fn post_authors(dsn: &str, raw: &mut Client, pool: &trellis::Pool) -> Trellis {
    raw.batch_execute(
        "create table public.users (id int primary key, name text); \
         create table public.posts (id int primary key, author_id int, title text); \
         insert into public.users values (1, 'Ann'), (2, 'Bob'); \
         insert into public.posts values (1, 1, 'a'), (2, 2, 'b'), (3, 1, 'c');",
    )
    .await
    .expect("seed");
    let trellis = definer(dsn).await;
    for text in [
        "RELATIONSHIP author FROM posts.author_id TO users.id",
        "TRANSFORM post_authors FROM public.posts SELECT author.name AS author_name",
    ] {
        trellis.apply(text).await.expect(text);
    }
    bring_live(raw, pool, &["post_authors"]).await;
    trellis
}

/// Widening one side of a join (`users.id` to `bigint` while
/// `posts.author_id` stays `integer`) breaks #590's same-type pairing and
/// outgrows the relationship projection's key, a copy of `users.id`: the
/// pass pauses the reader for both. A resume refuses while the pair
/// differs, naming both columns and both types. Once the other side is
/// widened too, a resume re-types the projection's key and rebuilds.
#[tokio::test]
async fn a_one_sided_join_widening_pauses_and_resume_refuses_until_the_other_side_matches() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = post_authors(db.dsn(), &mut raw, &db.pool).await;

    raw.batch_execute("alter table public.users alter column id type bigint")
        .await
        .expect("widen the to-side join column");
    capture_pass(&mut raw, &db.pool).await;
    // The pass reaches `posts` first, where the pairing breaks.
    let error = paused_for(&trellis, "post_authors", "public.posts", &["author_id"]).await;
    assert!(
        error.contains("posts.author_id (integer)")
            && error.contains("users.id (bigint)")
            && error.contains("Alter one column to match the other")
            && error.contains("refuses until that is fixed"),
        "{error}"
    );

    let refused = resume_refused(&trellis, "post_authors").await;
    assert!(
        refused.contains("posts.author_id (integer)") && refused.contains("users.id (bigint)"),
        "{refused}"
    );
    paused_for(&trellis, "post_authors", "public.posts", &["author_id"]).await;

    raw.batch_execute("alter table public.posts alter column author_id type bigint")
        .await
        .expect("widen the from-side to match");
    capture_pass(&mut raw, &db.pool).await;
    resume(&trellis, "post_authors").await;
    let error = assert_retyping(&trellis, &raw, "post_authors").await;
    let projection: String = raw
        .query_one("select projection_table from relationship_projections", &[])
        .await
        .expect("the relationship's projection")
        .get(0);
    assert!(
        error.contains(&format!("trellis.{projection}.id from integer to bigint")),
        "{error}"
    );
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        column_type(&raw, &format!("trellis.{projection}"), "id").await,
        "bigint"
    );
    bring_live(&mut raw, &db.pool, &["post_authors"]).await;
    raw.batch_execute(
        "insert into public.users values (3000000000, 'Big'); \
         insert into public.posts values (4, 3000000000, 'd');",
    )
    .await
    .expect("a join key above 2^31");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(status(&raw, "post_authors").await, TransformStatus::Live);
    assert_eq!(
        rows(
            &raw,
            "select id::text, author_name from public.post_authors order by id"
        )
        .await,
        rows(
            &raw,
            "select p.id::text, u.name from public.posts p \
             left join public.users u on u.id = p.author_id order by p.id"
        )
        .await,
    );
}

/// A key widened before the capture pass sees it, and a value above 2^31
/// drained meanwhile: the target's copy can't hold it, so the key is
/// quarantined. The pass then pauses the definition. The resume re-types
/// the copy, releases the key and rebuilds, so the row lands.
#[tokio::test]
async fn resume_after_a_quarantine_releases_the_key_and_rebuilds_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = items(db.dsn(), &mut raw, &db.pool).await;

    raw.batch_execute(
        "alter table public.items alter column id type bigint; \
         insert into public.items values (3000000000, 'big', 3);",
    )
    .await
    .expect("widen the key and write past the copy");
    drain_past_failures(&db.pool, &mut raw).await;
    let poisoned: i64 = raw
        .query_one("select count(*) from poison", &[])
        .await
        .expect("read poison")
        .get(0);
    assert_eq!(poisoned, 1, "the key the copy can't hold is quarantined");

    capture_pass(&mut raw, &db.pool).await;
    paused_for(&trellis, "item_names", "public.items", &["id"]).await;
    resume(&trellis, "item_names").await;
    bring_live(&mut raw, &db.pool, &["item_names"]).await;
    let poisoned: i64 = raw
        .query_one("select count(*) from poison", &[])
        .await
        .expect("read poison")
        .get(0);
    assert_eq!(poisoned, 0);
    assert_item_names_match(&raw).await;
}

/// The staging worker re-types a resumed definition's copies and then
/// completes the resume in its own transaction. A crash between the two
/// (here the re-type done by hand, the completion never run) leaves the
/// definition paused with its request. The next resume, or the next pass,
/// finishes the work: there is nothing left to re-type.
#[tokio::test]
async fn a_crash_between_the_re_type_and_the_rebuild_leaves_the_resume_to_finish() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = items(db.dsn(), &mut raw, &db.pool).await;

    raw.batch_execute("alter table public.items alter column id type bigint")
        .await
        .expect("widen the key");
    capture_pass(&mut raw, &db.pool).await;
    resume(&trellis, "item_names").await;
    raw.batch_execute("alter table public.item_names alter column id type bigint")
        .await
        .expect("the worker's re-type, then a crash");
    assert_retyping(&trellis, &raw, "item_names").await;

    resume(&trellis, "item_names").await;
    assert_eq!(
        status(&raw, "item_names").await,
        TransformStatus::WaitingToBackfill
    );
    let requests: i64 = raw
        .query_one("select count(*) from resume_requests", &[])
        .await
        .expect("read requests")
        .get(0);
    assert_eq!(requests, 0);
    bring_live(&mut raw, &db.pool, &["item_names"]).await;
    raw.batch_execute("insert into public.items values (3000000000, 'big', 3)")
        .await
        .expect("a key above 2^31");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_item_names_match(&raw).await;
}

/// The staging worker's own completion of a requested resume after a
/// crash: the request alone is enough, without a second `RESUME`.
#[tokio::test]
async fn the_next_pass_finishes_a_resume_whose_copies_were_re_typed() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = items(db.dsn(), &mut raw, &db.pool).await;

    raw.batch_execute("alter table public.items alter column id type bigint")
        .await
        .expect("widen the key");
    capture_pass(&mut raw, &db.pool).await;
    resume(&trellis, "item_names").await;
    raw.batch_execute("alter table public.item_names alter column id type bigint")
        .await
        .expect("the worker's re-type, then a crash");
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        status(&raw, "item_names").await,
        TransformStatus::WaitingToBackfill
    );
    bring_live(&mut raw, &db.pool, &["item_names"]).await;
    assert_item_names_match(&raw).await;
}

/// Narrowing a `varchar(n)` key strips trailing spaces past the new length
/// in the table rewrite, with no trigger, so the stored key no longer
/// matches: the pass pauses the definition, and a resume rebuilds it from
/// the narrowed keys.
#[tokio::test]
async fn narrowing_a_varchar_key_pauses_and_resume_rebuilds_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.codes (code varchar(10) primary key, label text); \
         insert into public.codes values ('abc   ', 'padded'), ('xyz', 'plain');",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply("TRANSFORM code_labels FROM public.codes SELECT label AS label")
        .await
        .expect("define code_labels");
    bring_live(&mut raw, &db.pool, &["code_labels"]).await;

    raw.batch_execute("alter table public.codes alter column code type varchar(3)")
        .await
        .expect("narrow the key");
    capture_pass(&mut raw, &db.pool).await;
    let error = paused_for(&trellis, "code_labels", "public.codes", &["code"]).await;
    assert!(
        error.contains("from character varying(10) to character varying(3)"),
        "{error}"
    );

    resume(&trellis, "code_labels").await;
    capture_pass(&mut raw, &db.pool).await;
    bring_live(&mut raw, &db.pool, &["code_labels"]).await;
    assert_eq!(
        rows(
            &raw,
            "select code::text, label from public.code_labels order by code"
        )
        .await,
        rows(
            &raw,
            "select code::text, label from public.codes order by code"
        )
        .await,
    );
}

/// A change that renders the existing keys differently pauses the
/// definitions keyed by them, though define would accept the new type:
/// `timestamp` to `timestamptz` (an `ALTER` in a New York session moves
/// every instant, and the rendering gains `+00`). The definitions that don't
/// key by the column keep applying. A resume re-types the aggregate's
/// copies, records the new type and rebuilds, and its target then matches.
#[tokio::test]
async fn a_type_change_that_re_renders_the_keys_pauses_and_resume_rebuilds_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.readings (taken timestamp primary key, at_day timestamp, v int); \
         insert into public.readings values \
           ('2024-01-01 10:00', '2024-01-01', 1), ('2024-01-02 10:00', '2024-01-01', 2), \
           ('2024-01-03 10:00', '2024-01-02', 4);",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    for text in [
        "TRANSFORM by_taken FROM public.readings SELECT v AS v",
        "TRANSFORM per_day FROM public.readings GROUP BY at_day SELECT at_day AS at_day, \
         SUM(v) AS total",
    ] {
        trellis.apply(text).await.expect(text);
    }
    bring_live(&mut raw, &db.pool, &["by_taken", "per_day"]).await;

    raw.batch_execute(
        "begin; set local timezone = 'America/New_York'; \
         alter table public.readings alter column at_day type timestamptz; commit;",
    )
    .await
    .expect("re-type the GROUP BY column");
    capture_pass(&mut raw, &db.pool).await;
    let error = paused_for(&trellis, "per_day", "public.readings", &["at_day"]).await;
    assert!(
        error.contains("from timestamp without time zone to timestamp with time zone")
            && error.contains("GROUP BY")
            && error.contains("Resume the definition"),
        "{error}"
    );
    assert_eq!(status(&raw, "by_taken").await, TransformStatus::Live);

    resume(&trellis, "per_day").await;
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        column_type(&raw, "public.per_day", "at_day").await,
        "timestamp with time zone"
    );
    bring_live(&mut raw, &db.pool, &["per_day"]).await;
    raw.batch_execute("insert into public.readings values ('2024-01-04 10:00', '2024-01-02', 8)")
        .await
        .expect("a write");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        rows(
            &raw,
            "select at_day::text, total::text from public.per_day where total is not null \
             order by at_day"
        )
        .await,
        rows(
            &raw,
            "select at_day::text, sum(v)::text from public.readings group by at_day \
             order by at_day"
        )
        .await,
    );
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(status(&raw, "per_day").await, TransformStatus::Live);

    // The source key: a 1-1 target's key column is a copy of it. `date`
    // renders it differently; the resume re-types the copy and rebuilds.
    raw.batch_execute("alter table public.readings alter column taken type date")
        .await
        .expect("re-type the source key");
    capture_pass(&mut raw, &db.pool).await;
    let error = paused_for(&trellis, "by_taken", "public.readings", &["taken"]).await;
    assert!(
        error.contains("source key") && error.contains("date"),
        "{error}"
    );
    resume(&trellis, "by_taken").await;
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(column_type(&raw, "public.by_taken", "taken").await, "date");
    bring_live(&mut raw, &db.pool, &["by_taken"]).await;
    assert_eq!(
        rows(
            &raw,
            "select taken::text, v::text from public.by_taken order by taken"
        )
        .await,
        rows(
            &raw,
            "select taken::text, v::text from public.readings order by taken"
        )
        .await,
    );
}
/// A nondeterministic collation given to a `GROUP BY` column after define,
/// which define refuses (#638), pauses the aggregate. The relationship's
/// reader, which doesn't key by the column, keeps applying.
///
/// Only ICU provides nondeterministic collations; every test cluster's
/// default collation is ICU (#665).
#[tokio::test]
async fn a_nondeterministic_collation_on_a_group_by_column_pauses_its_reader() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    trellis
        .apply("TRANSFORM per_title FROM public.posts GROUP BY title SELECT COUNT(*) AS n")
        .await
        .expect("define per_title");
    bring_live(&mut raw, &db.pool, &["per_title"]).await;

    raw.batch_execute(
        "create collation public.case_insensitive \
           (provider = icu, locale = 'und-u-ks-level2', deterministic = false); \
         alter table public.posts alter column title type text collate public.case_insensitive",
    )
    .await
    .expect("give the GROUP BY column a nondeterministic collation");
    capture_pass(&mut raw, &db.pool).await;

    let error = paused_for(&trellis, "per_title", "public.posts", &["title"]).await;
    assert!(
        error.contains("nondeterministic") && error.contains("case_insensitive"),
        "{error}"
    );
    for target in ["post_authors", "post_titles", "per_kind"] {
        assert_eq!(
            status(&raw, target).await,
            TransformStatus::Live,
            "{target}"
        );
    }
}

/// A wider `numeric` scale on a `GROUP BY` key re-renders every stored
/// value (`1.50` reads back as `1.500`) and fires no capture trigger, yet
/// pauses nothing: numeric groups are matched as numbers, so a delete or
/// update of a row the rewrite re-rendered still retracts from the group it
/// was counted in, and a new row joins it. A scale narrower than the one
/// recorded at define does pause: it rounds the stored values, so two
/// groups (`1.555`, `1.556`) can become one (`1.56`) with no trigger firing.
/// A resume rebuilds the groups from the rounded values.
#[tokio::test]
async fn a_wider_numeric_scale_on_a_group_by_key_keeps_its_groups_and_a_narrower_one_pauses() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = setup(db.dsn(), &mut raw, &db.pool).await;
    trellis
        .apply(
            "TRANSFORM per_price FROM public.posts GROUP BY price \
             SELECT price AS price, COUNT(*) AS n, SUM(kind) AS kinds",
        )
        .await
        .expect("define per_price");
    bring_live(&mut raw, &db.pool, &["per_price"]).await;

    raw.batch_execute("alter table public.posts alter column price type numeric(10,3)")
        .await
        .expect("change the GROUP BY key's scale");
    capture_pass(&mut raw, &db.pool).await;
    for target in ["post_authors", "post_titles", "per_kind", "per_price"] {
        assert_eq!(
            status(&raw, target).await,
            TransformStatus::Live,
            "{target}"
        );
    }

    raw.batch_execute(
        "delete from public.posts where id = 1; \
         update public.posts set price = 1.5 where id = 2; \
         insert into public.posts values (4, 'ann', 'd', 5, 2.5), (5, 'bob', 'e', 7, 1.5);",
    )
    .await
    .expect("writes to rows the rewrite re-rendered");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(status(&raw, "per_price").await, TransformStatus::Live);
    // Compared as numbers: the target's rendering of a group is the one it
    // was first written with.
    assert_eq!(
        rows(
            &raw,
            "select (price * 1000)::bigint::text, n::text, kinds::text from public.per_price \
             where n <> 0 order by price"
        )
        .await,
        rows(
            &raw,
            "select (price * 1000)::bigint::text, count(*)::text, sum(kind)::text \
             from public.posts group by price order by price"
        )
        .await,
    );

    // Back to the recorded scale: nothing the target holds changes.
    raw.batch_execute("alter table public.posts alter column price type numeric(10,2)")
        .await
        .expect("narrow back to the recorded scale");
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(status(&raw, "per_price").await, TransformStatus::Live);

    raw.batch_execute("alter table public.posts alter column price type numeric(10,1)")
        .await
        .expect("narrow below the recorded scale");
    capture_pass(&mut raw, &db.pool).await;
    let error = paused_for(&trellis, "per_price", "public.posts", &["price"]).await;
    assert!(
        error.contains("from numeric(10,2) to numeric(10,1)") && error.contains("GROUP BY"),
        "{error}"
    );
    for target in ["post_authors", "post_titles", "per_kind"] {
        assert_eq!(
            status(&raw, target).await,
            TransformStatus::Live,
            "{target}"
        );
    }

    // The resume rebuilds the groups from the rounded values.
    resume(&trellis, "per_price").await;
    bring_live(&mut raw, &db.pool, &["per_price"]).await;
    assert_eq!(
        rows(
            &raw,
            "select (price * 1000)::bigint::text, n::text, kinds::text from public.per_price \
             where n <> 0 order by price"
        )
        .await,
        rows(
            &raw,
            "select (price * 1000)::bigint::text, count(*)::text, sum(kind)::text \
             from public.posts group by price order by price"
        )
        .await,
    );
}

/// #708: a resume re-runs define-time validation against the live schema,
/// whatever paused the definition. Paused by its operator, then a column
/// one of its fields sums made `text`, the definition is one define would
/// refuse (`SUM` over text): the resume refuses with define's error, and
/// leaves it paused. With the column back, the resume rebuilds.
#[tokio::test]
async fn resume_refuses_a_definition_define_would_refuse_now() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = items(db.dsn(), &mut raw, &db.pool).await;

    trellis
        .apply("PAUSE TRANSFORM item_names")
        .await
        .expect("pause");
    raw.batch_execute("alter table public.items alter column qty type text")
        .await
        .expect("re-type a column a field computes over");
    capture_pass(&mut raw, &db.pool).await;
    let refused = resume_refused(&trellis, "item_names").await;
    assert!(
        refused.contains("calculated field 'next'") && refused.contains("found text"),
        "{refused}"
    );
    assert_eq!(status(&raw, "item_names").await, TransformStatus::Paused);

    raw.batch_execute("alter table public.items alter column qty type int using qty::int")
        .await
        .expect("change it back");
    resume(&trellis, "item_names").await;
    bring_live(&mut raw, &db.pool, &["item_names"]).await;
    assert_item_names_match(&raw).await;
}

/// #708: a 1-1 definition whose source key was redefined is paused by the
/// capture pass (#687), and its resume refuses with the remedy: the target
/// is keyed by the old key, so only restoring it, or a drop and redefine,
/// gets it going again.
#[tokio::test]
async fn resume_refuses_a_redefined_source_key() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = items(db.dsn(), &mut raw, &db.pool).await;

    raw.batch_execute(
        "alter table public.items drop constraint items_pkey; \
         alter table public.items alter column name set not null; \
         alter table public.items add primary key (name);",
    )
    .await
    .expect("redefine the key");
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(status(&raw, "item_names").await, TransformStatus::Paused);
    let refused = resume_refused(&trellis, "item_names").await;
    assert!(
        refused.contains("primary key of public.items is now (name)")
            && refused.contains("keyed by (id)")
            && refused.contains("drop the definition and define it again"),
        "{refused}"
    );
    assert_eq!(status(&raw, "item_names").await, TransformStatus::Paused);
}

/// #708: a field resume re-validates its definition too, and refuses while
/// define would refuse it.
#[tokio::test]
async fn a_field_resume_refuses_a_definition_define_would_refuse_now() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = items(db.dsn(), &mut raw, &db.pool).await;

    trellis
        .apply("PAUSE TRANSFORM item_names.next")
        .await
        .expect("pause the field");
    raw.batch_execute("alter table public.items alter column qty type text")
        .await
        .expect("re-type the column the field computes over");
    match trellis.apply("RESUME TRANSFORM item_names.next").await {
        Err(TrellisError::Apply(trellis::staging::apply::ApplyError::ResumeRefused {
            reason,
            ..
        })) => assert!(reason.to_string().contains("'next'"), "{reason}"),
        other => panic!("expected the field resume to be refused, got {other:?}"),
    }
    let still_paused: i64 = raw
        .query_one(
            "select count(*) from column_status \
             where transform_table = 'item_names' and column_name = 'next'",
            &[],
        )
        .await
        .expect("read column_status")
        .get(0);
    assert_eq!(still_paused, 1);
}

/// An aggregate's `GROUP BY` key read through a to-one relationship
/// (`GROUP BY author.country`) is read from the relationship's projection,
/// whose column for it copies the to-side column's type. Widening the
/// to-side column (`varchar(2)` to `varchar(20)`) outgrows that copy: the
/// pass pauses the aggregate, naming it, and a resume re-types it and
/// rebuilds, so a longer value groups.
#[tokio::test]
async fn widening_a_group_by_key_read_through_a_relationship_pauses_and_resume_widens_its_projection_column()
 {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.users (id int primary key, country varchar(2)); \
         create table public.posts (id int primary key, author_id int); \
         insert into public.users values (1, 'US'), (2, 'FR'); \
         insert into public.posts values (1, 1), (2, 2), (3, 1);",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    for text in [
        "RELATIONSHIP author FROM posts.author_id TO users.id",
        "TRANSFORM per_country FROM public.posts GROUP BY author.country \
         SELECT author.country AS country, COUNT(*) AS n",
    ] {
        trellis.apply(text).await.expect(text);
    }
    bring_live(&mut raw, &db.pool, &["per_country"]).await;
    let projection: String = raw
        .query_one("select projection_table from relationship_projections", &[])
        .await
        .expect("the relationship's projection")
        .get(0);
    let projection = format!("trellis.{projection}");

    raw.batch_execute("alter table public.users alter column country type varchar(20)")
        .await
        .expect("widen the GROUP BY key's to-side column");
    capture_pass(&mut raw, &db.pool).await;
    let error = paused_for(&trellis, "per_country", "public.users", &["country"]).await;
    assert!(
        error.contains(&format!("{projection}.country (character varying(2))")),
        "{error}"
    );

    resume(&trellis, "per_country").await;
    let error = assert_retyping(&trellis, &raw, "per_country").await;
    assert!(
        error.contains(&format!(
            "{projection}.country from character varying(2) to character varying(20)"
        )),
        "{error}"
    );
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        column_type(&raw, &projection, "country").await,
        "character varying(20)"
    );
    bring_live(&mut raw, &db.pool, &["per_country"]).await;
    raw.batch_execute("update public.users set country = 'United States' where id = 1")
        .await
        .expect("a longer value");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(status(&raw, "per_country").await, TransformStatus::Live);
    assert_eq!(
        rows(
            &raw,
            "select country::text, n::text from public.per_country where n <> 0 order by 1"
        )
        .await,
        rows(
            &raw,
            "select u.country::text, count(*)::text from public.posts p \
             join public.users u on u.id = p.author_id group by 1 order by 1"
        )
        .await,
    );
}

/// #760 rule 8: re-typing a copy in place can fail. A 1-1 key re-typed
/// `text` to `uuid` with a `USING` that isn't a plain cast leaves the
/// target's copy holding text no `uuid` cast accepts. The resume's re-type
/// fails: the request ends, the definition stays paused with the Postgres
/// error on its `capture_failure`, and the target keeps its type and rows.
/// A second resume tries again and ends the same way.
#[tokio::test]
async fn a_re_type_that_fails_leaves_the_definition_paused_with_the_error_and_its_target_as_it_was()
{
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.tags (code text primary key, label text); \
         insert into public.tags values ('red', 'Red'), ('blue', 'Blue');",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply("TRANSFORM tag_labels FROM public.tags SELECT label AS label")
        .await
        .expect("define tag_labels");
    bring_live(&mut raw, &db.pool, &["tag_labels"]).await;
    let before = rows(
        &raw,
        "select code, label from public.tag_labels order by code",
    )
    .await;

    raw.batch_execute("alter table public.tags alter column code type uuid using md5(code)::uuid")
        .await
        .expect("re-type the key with a USING that isn't a cast");
    capture_pass(&mut raw, &db.pool).await;
    paused_for(&trellis, "tag_labels", "public.tags", &["code"]).await;

    for _ in 0..2 {
        resume(&trellis, "tag_labels").await;
        assert_retyping(&trellis, &raw, "tag_labels").await;
        capture_pass(&mut raw, &db.pool).await;
        let reported = trellis
            .status("tag_labels")
            .await
            .expect("status")
            .expect("tag_labels");
        assert_eq!(reported.status, TransformStatus::Paused);
        let error = reported.capture_failure.expect("reason").error;
        assert!(
            error.contains("couldn't re-type Trellis's columns public.tag_labels.code")
                && error.contains("invalid input syntax for type uuid")
                && error.contains("drop the definition and define it again"),
            "{error}"
        );
        let requests: i64 = raw
            .query_one("select count(*) from resume_requests", &[])
            .await
            .expect("read requests")
            .get(0);
        assert_eq!(requests, 0);
        assert_eq!(column_type(&raw, "public.tag_labels", "code").await, "text");
        assert_eq!(
            rows(
                &raw,
                "select code, label from public.tag_labels order by code"
            )
            .await,
            before
        );
    }
}

/// The staging worker re-types each table's copies in a transaction of its
/// own, so a crash can fall between two tables: here the ledger's copy is
/// re-typed (by hand) and the target's and the group-delta table's are not.
/// The definition is still paused with its request, and the next pass
/// re-types what's left and completes the resume.
#[tokio::test]
async fn a_crash_between_two_tables_re_types_leaves_the_rest_to_the_next_pass() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.orders (id int primary key, shop int, amount int); \
         insert into public.orders values (1, 1, 10), (2, 1, 20), (3, 2, 5);",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply(
            "TRANSFORM per_shop FROM public.orders GROUP BY shop \
             SELECT shop AS shop, SUM(amount) AS total",
        )
        .await
        .expect("define per_shop");
    bring_live(&mut raw, &db.pool, &["per_shop"]).await;

    raw.batch_execute("alter table public.orders alter column shop type bigint")
        .await
        .expect("widen the GROUP BY key");
    capture_pass(&mut raw, &db.pool).await;
    paused_for(&trellis, "per_shop", "public.orders", &["shop"]).await;
    resume(&trellis, "per_shop").await;
    raw.batch_execute("alter table public.per_shop__ledger alter column shop type bigint")
        .await
        .expect("one table's re-type, then a crash");
    assert_retyping(&trellis, &raw, "per_shop").await;
    assert_eq!(
        column_type(&raw, "public.per_shop", "shop").await,
        "integer"
    );

    capture_pass(&mut raw, &db.pool).await;
    for table in [
        "public.per_shop",
        "public.per_shop__ledger",
        "public.per_shop__deltas",
    ] {
        assert_eq!(column_type(&raw, table, "shop").await, "bigint", "{table}");
    }
    assert_eq!(
        status(&raw, "per_shop").await,
        TransformStatus::WaitingToBackfill
    );
    bring_live(&mut raw, &db.pool, &["per_shop"]).await;
    raw.batch_execute("insert into public.orders values (4, 3000000000, 7)")
        .await
        .expect("a group above 2^31");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        rows(
            &raw,
            "select shop::text, total::text from public.per_shop where total is not null \
             order by shop"
        )
        .await,
        rows(
            &raw,
            "select shop::text, sum(amount)::text from public.orders group by shop order by shop"
        )
        .await,
    );
}

/// #824: widening a column an aggregate sums and takes the minimum of
/// (`integer` to `bigint`) outgrows the ledger's contribution column, typed
/// as the argument, and the target's `MIN` column. The pass pauses the
/// aggregate, naming each. A resume re-types them, and the `SUM` column,
/// which define now gives `numeric` (`sum(bigint)`'s type) rather than
/// `bigint`, and nothing else, then rebuilds, and a value above 2^31 lands.
#[tokio::test]
async fn widening_a_summed_column_pauses_and_resume_re_types_its_contribution_and_target_columns() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.orders (id int primary key, shop int, amount int, note text); \
         insert into public.orders values (1, 1, 10, 'a'), (2, 1, 20, 'b'), (3, 2, 5, 'c');",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply(
            "TRANSFORM per_shop FROM public.orders GROUP BY shop \
             SELECT shop AS shop, SUM(amount) AS total, MIN(amount) AS least, \
                    MAX(note) AS last_note, COUNT(*) AS n",
        )
        .await
        .expect("define per_shop");
    bring_live(&mut raw, &db.pool, &["per_shop"]).await;
    let types = async |raw: &Client| {
        let mut types = Vec::new();
        for (table, column) in [
            ("public.per_shop", "shop"),
            ("public.per_shop", "total"),
            ("public.per_shop", "least"),
            ("public.per_shop", "last_note"),
            ("public.per_shop", "n"),
            ("public.per_shop__ledger", "shop"),
            ("public.per_shop__ledger", "__arg0"),
            ("public.per_shop__ledger", "__arg1"),
        ] {
            types.push(column_type(raw, table, column).await);
        }
        types
    };
    assert_eq!(
        types(&raw).await,
        [
            "integer", "bigint", "integer", "text", "bigint", "integer", "integer", "text"
        ]
    );

    raw.batch_execute("alter table public.orders alter column amount type bigint")
        .await
        .expect("widen the summed column");
    capture_pass(&mut raw, &db.pool).await;
    let error = paused_for(&trellis, "per_shop", "public.orders", &["amount"]).await;
    for column in [
        "public.per_shop.least (integer, now bigint)",
        "public.per_shop__ledger.__arg0 (integer, now bigint)",
    ] {
        assert!(error.contains(column), "{column}: {error}");
    }
    for untouched in ["per_shop.shop", "last_note", "per_shop.n ", "__arg1"] {
        assert!(!error.contains(untouched), "{untouched}: {error}");
    }

    resume(&trellis, "per_shop").await;
    let error = assert_retyping(&trellis, &raw, "per_shop").await;
    assert!(
        error.contains("public.per_shop__ledger.__arg0 from integer to bigint")
            && error.contains("public.per_shop.total from bigint to numeric"),
        "{error}"
    );
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        types(&raw).await,
        [
            "integer", "numeric", "bigint", "text", "bigint", "integer", "bigint", "text"
        ]
    );
    assert_eq!(
        status(&raw, "per_shop").await,
        TransformStatus::WaitingToBackfill
    );
    bring_live(&mut raw, &db.pool, &["per_shop"]).await;
    raw.batch_execute(
        "insert into public.orders values (4, 1, 3000000000, 'd'), (5, 3, 9000000000000000000, 'e')",
    )
    .await
    .expect("values above 2^31");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(status(&raw, "per_shop").await, TransformStatus::Live);
    assert_eq!(
        rows(
            &raw,
            "select shop::text, total::text, least::text, last_note, n::text \
             from public.per_shop where n <> 0 order by shop"
        )
        .await,
        rows(
            &raw,
            "select shop::text, sum(amount)::text, min(amount)::text, max(note), \
                    count(*)::text \
             from public.orders group by shop order by shop"
        )
        .await,
    );

    // The types moved with the resume: nothing pauses again.
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(status(&raw, "per_shop").await, TransformStatus::Live);
}

/// #824: widening a column a 1-1 calculated field reads (`qty + 1`, with
/// `qty` `integer` to `bigint`) outgrows the field's column: the pass
/// pauses the definition, and a resume re-types that column, leaving the
/// key and the passthrough as they are, then rebuilds.
#[tokio::test]
async fn widening_a_column_a_calculated_field_reads_pauses_and_resume_re_types_the_field() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = items(db.dsn(), &mut raw, &db.pool).await;
    assert_eq!(
        column_type(&raw, "public.item_names", "next").await,
        "integer"
    );

    raw.batch_execute("alter table public.items alter column qty type bigint")
        .await
        .expect("widen the calculated field's column");
    capture_pass(&mut raw, &db.pool).await;
    let error = paused_for(&trellis, "item_names", "public.items", &["qty"]).await;
    assert!(
        error.contains("column \"qty\" of public.items widened to bigint")
            && error.contains("public.item_names.next (integer, now bigint)")
            && error.contains("Resume the definition"),
        "{error}"
    );
    assert!(!error.contains("item_names.name"), "{error}");

    resume(&trellis, "item_names").await;
    let error = assert_retyping(&trellis, &raw, "item_names").await;
    assert!(
        error.contains("public.item_names.next from integer to bigint"),
        "{error}"
    );
    capture_pass(&mut raw, &db.pool).await;
    for (column, ty) in [
        ("id", "integer"),
        ("name", "character varying(10)"),
        ("next", "bigint"),
    ] {
        assert_eq!(
            column_type(&raw, "public.item_names", column).await,
            ty,
            "{column}"
        );
    }
    bring_live(&mut raw, &db.pool, &["item_names"]).await;
    raw.batch_execute("insert into public.items values (3, 'three', 3000000000)")
        .await
        .expect("a value above 2^31");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(status(&raw, "item_names").await, TransformStatus::Live);
    assert_item_names_match(&raw).await;
}

/// #824: an integer column moved to `numeric` outgrows every integer
/// column Trellis created from it. Here it happens to a chained definition:
/// the resume that re-types an upstream aggregate's `SUM` column from
/// `bigint` to `numeric` (`sum(bigint)`'s type) moves the column the
/// downstream aggregate reads, and the pass pauses the downstream, whose
/// `MIN` column and ledger contribution are `bigint`. Its resume re-types
/// them, and a minimum above 2^63 lands.
#[tokio::test]
async fn an_upstream_sum_re_typed_to_numeric_pauses_and_resume_re_types_its_downstream_reader() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.sales (id int primary key, shop int, amount int); \
         insert into public.sales values (1, 1, 10), (2, 1, 20), (3, 2, 5);",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply(
            "TRANSFORM shop_totals FROM public.sales GROUP BY shop \
             SELECT shop AS shop, SUM(amount) AS total",
        )
        .await
        .expect("define shop_totals");
    bring_live(&mut raw, &db.pool, &["shop_totals"]).await;
    trellis
        .apply(
            "TRANSFORM smallest FROM public.shop_totals GROUP BY shop \
             SELECT shop AS shop, MIN(total) AS least",
        )
        .await
        .expect("define smallest");
    bring_live(&mut raw, &db.pool, &["shop_totals", "smallest"]).await;
    assert_eq!(
        column_type(&raw, "public.smallest", "least").await,
        "bigint"
    );

    raw.batch_execute("alter table public.sales alter column amount type bigint")
        .await
        .expect("widen the summed column");
    capture_pass(&mut raw, &db.pool).await;
    paused_for(&trellis, "shop_totals", "public.sales", &["amount"]).await;
    resume(&trellis, "shop_totals").await;
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        column_type(&raw, "public.shop_totals", "total").await,
        "numeric"
    );
    let error = paused_for(&trellis, "smallest", "public.shop_totals", &["total"]).await;
    for column in [
        "public.smallest.least (bigint, now numeric)",
        "public.smallest__ledger.__arg0 (bigint, now numeric)",
    ] {
        assert!(error.contains(column), "{column}: {error}");
    }

    resume(&trellis, "smallest").await;
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        column_type(&raw, "public.smallest", "least").await,
        "numeric"
    );
    assert_eq!(
        column_type(&raw, "public.smallest__ledger", "__arg0").await,
        "numeric"
    );
    bring_live(&mut raw, &db.pool, &["shop_totals", "smallest"]).await;
    raw.batch_execute(
        "insert into public.sales values (4, 3, 9000000000000000000), \
                                         (5, 3, 9000000000000000000)",
    )
    .await
    .expect("a sum above 2^63");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    for target in ["shop_totals", "smallest"] {
        assert_eq!(
            status(&raw, target).await,
            TransformStatus::Live,
            "{target}"
        );
    }
    assert_eq!(
        rows(
            &raw,
            "select shop::text, least::text from public.smallest order by shop"
        )
        .await,
        rows(
            &raw,
            "select shop::text, sum(amount)::text from public.sales group by shop order by shop"
        )
        .await,
    );
}

/// #824: widening a to-side column a 1-1 definition reads through a to-one
/// relationship (`author.name`, `varchar(10)` to `varchar(40)`) outgrows the
/// relationship projection's column for it. The target's column is `text`,
/// which holds every value already, and isn't re-typed. The pass pauses
/// the definition, and a resume widens the projection's column and
/// rebuilds, so a longer name lands.
#[tokio::test]
async fn widening_a_column_read_through_a_relationship_pauses_and_resume_widens_its_projection_column()
 {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.users (id int primary key, name varchar(10)); \
         create table public.posts (id int primary key, author_id int); \
         insert into public.users values (1, 'Ann'), (2, 'Bob'); \
         insert into public.posts values (1, 1), (2, 2), (3, 1);",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    for text in [
        "RELATIONSHIP author FROM posts.author_id TO users.id",
        "TRANSFORM post_authors FROM public.posts SELECT author.name AS author_name",
    ] {
        trellis.apply(text).await.expect(text);
    }
    bring_live(&mut raw, &db.pool, &["post_authors"]).await;
    let projection: String = raw
        .query_one("select projection_table from relationship_projections", &[])
        .await
        .expect("the relationship's projection")
        .get(0);
    let projection = format!("trellis.{projection}");
    assert_eq!(
        column_type(&raw, &projection, "name").await,
        "character varying(10)"
    );

    raw.batch_execute("alter table public.users alter column name type varchar(40)")
        .await
        .expect("widen the to-side column");
    capture_pass(&mut raw, &db.pool).await;
    let error = paused_for(&trellis, "post_authors", "public.users", &["name"]).await;
    assert!(
        error.contains(&format!("{projection}.name (character varying(10))")),
        "{error}"
    );
    assert!(!error.contains("post_authors.author_name"), "{error}");

    resume(&trellis, "post_authors").await;
    let error = assert_retyping(&trellis, &raw, "post_authors").await;
    assert!(
        error.contains(&format!(
            "{projection}.name from character varying(10) to character varying(40)"
        )) && !error.contains("author_name"),
        "{error}"
    );
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        column_type(&raw, &projection, "name").await,
        "character varying(40)"
    );
    assert_eq!(
        column_type(&raw, "public.post_authors", "author_name").await,
        "text"
    );
    bring_live(&mut raw, &db.pool, &["post_authors"]).await;
    raw.batch_execute("update public.users set name = 'Ann With A Long Name' where id = 1")
        .await
        .expect("a longer value");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(status(&raw, "post_authors").await, TransformStatus::Live);
    assert_eq!(
        rows(
            &raw,
            "select id::text, author_name from public.post_authors order by id"
        )
        .await,
        rows(
            &raw,
            "select p.id::text, u.name::text from public.posts p \
             join public.users u on u.id = p.author_id order by p.id"
        )
        .await,
    );
}

/// #824: a source change whose columns Trellis created from it hold every
/// value of the types define would give them now pauses nothing: a
/// widening under an expression typed by its family (`CHAR_LENGTH(name)`,
/// `qty + 2.5`, `MAX(name)`, `SUM(price)` over a `numeric(10,2)`), and a
/// narrowing under a `SUM`, a `MIN` and a calculated field.
#[tokio::test]
async fn changes_no_created_column_outgrows_pause_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.lines (id int primary key, name varchar(10), qty bigint, \
                                    price numeric(10,2), kind int); \
         insert into public.lines values (1, 'a', 1, 1.50, 1), (2, 'bb', 2, 2.50, 1), \
                                         (3, 'ccc', 3, 3.50, 2);",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    for text in [
        "TRANSFORM line_sizes FROM public.lines \
         SELECT CHAR_LENGTH(name) AS len, qty + 2.5 AS scaled, qty + 1 AS next",
        "TRANSFORM per_kind FROM public.lines GROUP BY kind \
         SELECT kind AS kind, MAX(name) AS top, SUM(price) AS total, SUM(qty) AS qty_total, \
                MIN(qty) AS least",
    ] {
        trellis.apply(text).await.expect(text);
    }
    let targets = ["line_sizes", "per_kind"];
    bring_live(&mut raw, &db.pool, &targets).await;
    let snapshot = async |raw: &Client| {
        rows(
            raw,
            "select c.relname::text, a.attname::text, \
                    pg_catalog.format_type(a.atttypid, a.atttypmod) \
             from pg_catalog.pg_attribute a join pg_catalog.pg_class c on c.oid = a.attrelid \
             where c.relname in ('line_sizes', 'per_kind', 'per_kind__ledger') \
               and a.attnum > 0 and not a.attisdropped order by 1, 2",
        )
        .await
    };
    let before = snapshot(&raw).await;

    for alter in [
        "alter table public.lines alter column name type varchar(40)",
        "alter table public.lines alter column name type text",
        "alter table public.lines alter column price type numeric(12,2)",
        "alter table public.lines alter column qty type integer",
    ] {
        raw.batch_execute(alter).await.expect(alter);
        capture_pass(&mut raw, &db.pool).await;
        for target in targets {
            assert_eq!(status(&raw, target).await, TransformStatus::Live, "{alter}");
        }
    }
    assert_eq!(snapshot(&raw).await, before);

    raw.batch_execute(
        "insert into public.lines values (4, 'a much longer name', 4, 1234567890.25, 2)",
    )
    .await
    .expect("writes");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    for target in targets {
        assert_eq!(
            status(&raw, target).await,
            TransformStatus::Live,
            "{target}"
        );
    }
    assert_eq!(
        rows(
            &raw,
            "select id::text, len::text, scaled::text, next::text from public.line_sizes \
             order by id"
        )
        .await,
        rows(
            &raw,
            "select id::text, char_length(name)::text, (qty + 2.5)::text, (qty + 1)::text \
             from public.lines order by id"
        )
        .await,
    );
    assert_eq!(
        rows(
            &raw,
            "select kind::text, top, total::text, qty_total::text, least::text \
             from public.per_kind order by kind"
        )
        .await,
        rows(
            &raw,
            "select kind::text, max(name), sum(price)::text, sum(qty)::text, min(qty)::text \
             from public.lines group by kind order by kind"
        )
        .await,
    );
}
