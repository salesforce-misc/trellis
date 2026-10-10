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

/// `table`'s `relfilenode`: a rewrite gives the table a new one.
async fn relfilenode(raw: &Client, table: &str) -> u32 {
    raw.query_one(
        "select relfilenode from pg_catalog.pg_class where oid = pg_catalog.to_regclass($1)",
        &[&table],
    )
    .await
    .unwrap_or_else(|err| panic!("{table}: {err}"))
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

/// A table where one column widens without a rewrite (a passthrough's
/// `varchar(10)` to `varchar(40)`) and another with one (the key's
/// `integer` to `bigint`) isn't re-typed in place (#824): the pass pauses
/// the definition, naming both, and a resume re-types both and rebuilds, so
/// a longer value lands. A narrowing of a passthrough pauses nothing and
/// re-types nothing: every value still fits.
#[tokio::test]
async fn a_table_that_also_needs_a_rewrite_pauses_and_resume_re_types_all_of_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = items(db.dsn(), &mut raw, &db.pool).await;

    raw.batch_execute("alter table public.items alter column name type varchar(8)")
        .await
        .expect("narrow the passthrough");
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(status(&raw, "item_names").await, TransformStatus::Live);
    assert_eq!(
        column_type(&raw, "public.item_names", "name").await,
        "character varying(10)"
    );

    raw.batch_execute(
        "alter table public.items alter column name type varchar(40), \
                                  alter column id type bigint",
    )
    .await
    .expect("widen the passthrough and the key");
    capture_pass(&mut raw, &db.pool).await;
    let error = paused_for(&trellis, "item_names", "public.items", &["id", "name"]).await;
    assert!(
        error.contains("widened to character varying(40)")
            && error.contains("public.item_names.name (character varying(10))")
            && error.contains("widened to bigint"),
        "{error}"
    );
    assert_eq!(
        column_type(&raw, "public.item_names", "name").await,
        "character varying(10)"
    );

    resume(&trellis, "item_names").await;
    assert_retyping(&trellis, &raw, "item_names").await;
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        column_type(&raw, "public.item_names", "name").await,
        "character varying(40)"
    );
    assert_eq!(column_type(&raw, "public.item_names", "id").await, "bigint");
    bring_live(&mut raw, &db.pool, &["item_names"]).await;
    raw.batch_execute("insert into public.items values (3000000000, 'a much longer name', 3)")
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
/// was counted in, and a new row joins it. The pass records the wider
/// scale, so narrowing back to the scale define saw pauses: it rounds the
/// values stored under the wider one, so two groups (`1.555`, `1.556`)
/// become one (`1.56`) with no trigger firing. A resume rebuilds the groups
/// from the rounded values.
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
    let groups_match = async |raw: &Client| {
        // Compared as numbers: the target's rendering of a group is the one
        // it was first written with.
        assert_eq!(
            rows(
                raw,
                "select (price * 1000)::bigint::text, n::text, kinds::text from public.per_price \
                 where n <> 0 order by price"
            )
            .await,
            rows(
                raw,
                "select (price * 1000)::bigint::text, count(*)::text, sum(kind)::text \
                 from public.posts group by price order by price"
            )
            .await,
        );
    };

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
         insert into public.posts values (4, 'ann', 'd', 5, 2.5), (5, 'bob', 'e', 7, 1.5), \
                                         (6, 'ann', 'f', 1, 1.555), (7, 'bob', 'g', 2, 1.556);",
    )
    .await
    .expect("writes to rows the rewrite re-rendered, and at the wider scale");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(status(&raw, "per_price").await, TransformStatus::Live);
    groups_match(&raw).await;

    // Back to the scale define saw: it rounds the keys stored at the wider
    // one, merging 1.555 and 1.556.
    raw.batch_execute("alter table public.posts alter column price type numeric(10,2)")
        .await
        .expect("narrow back to the scale define saw");
    capture_pass(&mut raw, &db.pool).await;
    let error = paused_for(&trellis, "per_price", "public.posts", &["price"]).await;
    assert!(
        error.contains("from numeric(10,3) to numeric(10,2)") && error.contains("GROUP BY"),
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
    groups_match(&raw).await;
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
/// to-side column (`varchar(2)` to `varchar(20)`) outgrows that copy, but
/// without a rewrite: one pass re-types the projection's column in place
/// (#824), and the aggregate stays live, so a longer value groups.
#[tokio::test]
async fn widening_a_group_by_key_read_through_a_relationship_re_types_its_projection_column_in_place()
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

    let before = relfilenode(&raw, &projection).await;
    raw.batch_execute("alter table public.users alter column country type varchar(20)")
        .await
        .expect("widen the GROUP BY key's to-side column");
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(status(&raw, "per_country").await, TransformStatus::Live);
    assert_eq!(
        column_type(&raw, &projection, "country").await,
        "character varying(20)"
    );
    assert_eq!(relfilenode(&raw, &projection).await, before, "no rewrite");
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
        // #828: it names the repair, and why Trellis doesn't make it.
        assert!(
            error.contains("couldn't re-type Trellis's columns public.tag_labels.code")
                && error.contains("invalid input syntax for type uuid")
                && error.contains("the target keeps its rows")
                && error.contains(
                    "DROP TRANSFORM tag_labels and define it again, which builds the target \
                     from empty: Trellis doesn't empty a target on its own, since the \
                     application reads it"
                ),
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

/// #857: a `SUM` whose argument moved from `numeric` to a float type is
/// recompute-only now, but its group-delta table keeps the running-sum column
/// define gave the old `SUM`. A resume refuses rather than rebuild into it,
/// names `DROP TRANSFORM`, and leaves the definition paused and the table as
/// it was.
#[tokio::test]
async fn a_resume_refuses_an_aggregate_whose_sum_argument_moved_between_numeric_and_float() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.orders (id int primary key, shop varchar(10), amount numeric); \
         insert into public.orders values (1, 'a', 10.5), (2, 'a', 20), (3, 'b', 5);",
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
    let delta_columns = async |raw: &Client| -> Vec<String> {
        raw.query(
            "select attname::text from pg_attribute \
             where attrelid = 'public.per_shop__deltas'::regclass \
               and attnum > 0 and not attisdropped and attname like '\\_\\_d%' \
             order by attnum",
            &[],
        )
        .await
        .expect("read the delta columns")
        .into_iter()
        .map(|row| row.get(0))
        .collect()
    };
    let before = delta_columns(&raw).await;
    assert!(before.iter().any(|c| c == "__ds0"), "{before:?}");

    trellis
        .apply("PAUSE TRANSFORM per_shop")
        .await
        .expect("pause per_shop");
    raw.batch_execute("alter table public.orders alter column amount type double precision")
        .await
        .expect("move the summed column to a float");
    let message = resume_refused(&trellis, "per_shop").await;
    assert!(
        message.contains("DROP TRANSFORM") && message.contains("__ds0"),
        "{message}"
    );
    // Define would accept this definition, so the message doesn't say it
    // would refuse it.
    assert!(!message.contains("define would refuse"), "{message}");
    assert_eq!(status(&raw, "per_shop").await, TransformStatus::Paused);
    assert_eq!(delta_columns(&raw).await, before);
}

/// #857, the other direction: a `SUM` argument that moved from a float type
/// to `numeric` gains the running-sum column define would give it now, and
/// loses the recompute flag. The resume refuses that too.
#[tokio::test]
async fn a_resume_refuses_an_aggregate_whose_sum_argument_moved_from_float_to_numeric() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.orders (id int primary key, shop varchar(10), amount double precision); \
         insert into public.orders values (1, 'a', 10.5), (2, 'a', 20), (3, 'b', 5);",
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

    trellis
        .apply("PAUSE TRANSFORM per_shop")
        .await
        .expect("pause per_shop");
    raw.batch_execute("alter table public.orders alter column amount type numeric")
        .await
        .expect("move the summed column to numeric");
    let message = resume_refused(&trellis, "per_shop").await;
    assert!(
        message.contains("DROP TRANSFORM")
            && message.contains("__out")
            && message.contains("__ds0"),
        "{message}"
    );
    assert_eq!(status(&raw, "per_shop").await, TransformStatus::Paused);
}

/// #857: the staging worker makes the same check before it re-types
/// anything. The operator's resume is accepted (a widened `GROUP BY` key is
/// waiting to be re-typed), then the summed column moves to a float: the
/// worker's pass refuses the resume, leaves the key's column as it was, and
/// reports `DROP TRANSFORM`.
#[tokio::test]
async fn the_worker_refuses_a_requested_resume_whose_delta_table_s_shape_changed_first() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.orders (id int primary key, shop int, amount numeric); \
         insert into public.orders values (1, 1, 10.5), (2, 1, 20), (3, 2, 5);",
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
        .expect("widen the key");
    capture_pass(&mut raw, &db.pool).await;
    resume(&trellis, "per_shop").await;
    assert_retyping(&trellis, &raw, "per_shop").await;
    raw.batch_execute("alter table public.orders alter column amount type double precision")
        .await
        .expect("move the summed column to a float");
    capture_pass(&mut raw, &db.pool).await;

    let reported = trellis
        .status("per_shop")
        .await
        .expect("status")
        .expect("per_shop");
    assert_eq!(reported.status, TransformStatus::Paused);
    let error = reported.capture_failure.expect("reason").error;
    assert!(
        error.starts_with("the resume was refused:") && error.contains("DROP TRANSFORM"),
        "{error}"
    );
    assert!(!error.contains("define would refuse"), "{error}");
    assert_eq!(
        column_type(&raw, "public.per_shop", "shop").await,
        "integer"
    );
}

/// #857 negative control: a retype that keeps every field's classification
/// (`SUM` over `integer` moved to `bigint`) leaves the group-delta table's
/// shape as define would create it, so the resume goes ahead.
#[tokio::test]
async fn a_resume_goes_ahead_when_a_sum_argument_retype_keeps_the_delta_table_s_shape() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.orders (id int primary key, shop varchar(10), amount int); \
         insert into public.orders values (1, 'a', 10), (2, 'a', 20), (3, 'b', 5);",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply(
            "TRANSFORM per_shop FROM public.orders GROUP BY shop \
             SELECT shop AS shop, SUM(amount) AS total, MIN(amount) AS least, COUNT(*) AS n",
        )
        .await
        .expect("define per_shop");
    bring_live(&mut raw, &db.pool, &["per_shop"]).await;

    trellis
        .apply("PAUSE TRANSFORM per_shop")
        .await
        .expect("pause per_shop");
    raw.batch_execute("alter table public.orders alter column amount type bigint")
        .await
        .expect("widen the summed column");
    trellis
        .apply("RESUME TRANSFORM per_shop")
        .await
        .expect("the resume isn't refused");
}

/// #857: a column resume of an aggregate field builds nothing and never
/// reads the group-delta table, so it isn't refused for the table's shape.
#[tokio::test]
async fn a_column_resume_of_an_aggregate_field_ignores_the_delta_table_s_shape() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.orders (id int primary key, shop varchar(10), amount numeric); \
         insert into public.orders values (1, 'a', 10.5), (2, 'a', 20), (3, 'b', 5);",
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

    trellis
        .apply("PAUSE TRANSFORM per_shop.total")
        .await
        .expect("pause the field");
    raw.batch_execute("alter table public.orders alter column amount type double precision")
        .await
        .expect("move the summed column to a float");
    trellis
        .apply("RESUME TRANSFORM per_shop.total")
        .await
        .expect("the column resume isn't refused for the delta table's shape");
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
/// them, and a minimum above 2^63 lands. The downstream's pause names the
/// upstream resume as its cause (#828).
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
    // #828: the pause names the upstream resume that caused it.
    assert!(
        error.starts_with(
            "the resume of transform shop_totals re-typed public.shop_totals.total from bigint \
             to numeric"
        ),
        "{error}"
    );
    assert_eq!(
        caused_by(&raw, "smallest").await.as_deref(),
        Some("shop_totals")
    );
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

/// The upstream definition (its bare target) whose resume `target`'s pause
/// records as its cause (#828), if any. `target` must have a pause record.
async fn caused_by(raw: &Client, target: &str) -> Option<String> {
    raw.query_one(
        "select split_part(u.target_table, '.', 2) from capture_failures f \
         join transform_definitions d on d.id = f.transform_id \
         left join transform_definitions u on u.id = f.caused_by \
         where split_part(d.target_table, '.', 2) = $1",
        &[&target],
    )
    .await
    .unwrap_or_else(|err| panic!("{target}'s pause record: {err}"))
    .get(0)
}

/// When `target`'s pause record was last written.
async fn failure_detected_at(raw: &Client, target: &str) -> String {
    raw.query_one(
        "select f.detected_at::text from capture_failures f \
         join transform_definitions d on d.id = f.transform_id \
         where split_part(d.target_table, '.', 2) = $1",
        &[&target],
    )
    .await
    .unwrap_or_else(|err| panic!("{target}'s pause record: {err}"))
    .get(0)
}

/// Whether `target` isn't paused and has no pause record. A reader of a
/// target being rebuilt is `catching_up` (#476).
async fn assert_unpaused(trellis: &Trellis, target: &str) {
    let reported = trellis.status(target).await.expect("status").expect(target);
    assert!(!reported.status.is_frozen(), "{target}: {reported:?}");
    assert!(reported.capture_failure.is_none(), "{target}: {reported:?}");
}

/// Asserts `target` waits on `upstream`'s rebuild (#828, #970): it is
/// paused with `upstream` as its cause, has no resume request, and its
/// rebuild hasn't started (no backfill chunk, no build).
async fn assert_waiting_on(trellis: &Trellis, raw: &Client, target: &str, upstream: &str) {
    let reported = trellis.status(target).await.expect("status").expect(target);
    assert_eq!(reported.status, TransformStatus::Paused, "{target}");
    assert_eq!(
        caused_by(raw, target).await.as_deref(),
        Some(upstream),
        "{target}"
    );
    let started: i64 = raw
        .query_one(
            "select (select count(*) from resume_requests r where r.transform_id = d.id) \
                  + (select count(*) from backfill_chunks c where c.definition_id = d.id and not c.done) \
                  + (case when d.build is null then 0 else 1 end) \
             from transform_definitions d where split_part(d.target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .expect("read what started")
        .get(0);
    assert_eq!(started, 0, "{target} has a request, chunks or a build");
}

/// Steps the staging worker by hand (a pass, the backfill chunks, a drain)
/// until `upstream` is live, asserting after every step that `waiting` is
/// still waiting on it. Bounded, not a timed wait.
async fn step_until_live(
    trellis: &Trellis,
    raw: &mut Client,
    pool: &trellis::Pool,
    upstream: &str,
    waiting: &[&str],
) {
    for _ in 0..12 {
        for step in 0..3 {
            if status(raw, upstream).await == TransformStatus::Live {
                return;
            }
            match step {
                0 => full_pass(raw, pool).await,
                1 => run_backfill_chunks(pool).await,
                _ => drain_to_quiescence(pool, raw).await,
            }
            if status(raw, upstream).await != TransformStatus::Live {
                for target in waiting {
                    assert_waiting_on(trellis, raw, target, upstream).await;
                }
            }
        }
    }
    panic!("{upstream} did not go live within 12 rounds of steps");
}

/// `public.items` and the chain `item_names` (A) -> `b_names` (B) ->
/// `c_names` (C), all live.
async fn chain(dsn: &str, raw: &mut Client, pool: &trellis::Pool) -> Trellis {
    let trellis = items(dsn, raw, pool).await;
    trellis
        .apply("TRANSFORM b_names FROM public.item_names SELECT name AS name")
        .await
        .expect("define b_names");
    bring_live(raw, pool, &["item_names", "b_names"]).await;
    trellis
        .apply("TRANSFORM c_names FROM public.b_names SELECT name AS name")
        .await
        .expect("define c_names");
    bring_live(raw, pool, &["item_names", "b_names", "c_names"]).await;
    trellis
}

/// Widens `public.items.id`, and has the operator resume A (`item_names`):
/// the pass re-types its target and pauses B with A as its cause.
async fn resume_head_of_chain(trellis: &Trellis, raw: &mut Client, pool: &trellis::Pool) {
    raw.batch_execute("alter table public.items alter column id type bigint")
        .await
        .expect("widen the key");
    capture_pass(raw, pool).await;
    paused_for(trellis, "item_names", "public.items", &["id"]).await;
    assert_eq!(caused_by(raw, "item_names").await, None);
    assert_unpaused(trellis, "b_names").await;

    resume(trellis, "item_names").await;
    capture_pass(raw, pool).await;
    assert_eq!(column_type(raw, "public.item_names", "id").await, "bigint");
    assert_eq!(
        status(raw, "item_names").await,
        TransformStatus::WaitingToBackfill
    );
}

/// `c_names` against its oracle, with a key above 2^31 written first.
async fn assert_chain_converges(trellis: &Trellis, raw: &mut Client, pool: &trellis::Pool) {
    bring_live(raw, pool, &["item_names", "b_names", "c_names"]).await;
    raw.batch_execute("insert into public.items values (3000000000, 'big', 3)")
        .await
        .expect("a key above 2^31");
    full_pass(raw, pool).await;
    drain_to_quiescence(pool, raw).await;
    for target in ["item_names", "b_names", "c_names"] {
        assert_unpaused(trellis, target).await;
        assert_eq!(status(raw, target).await, TransformStatus::Live, "{target}");
        assert_eq!(
            column_type(raw, &format!("public.{target}"), "id").await,
            "bigint"
        );
    }
    assert_eq!(
        rows(raw, "select id::text, name from public.c_names order by id").await,
        rows(
            raw,
            "select id::text, name::text from public.items order by id"
        )
        .await,
    );
}

/// #828, #970: a resume that re-types a target's key (`integer` to
/// `bigint`) pauses the definition chained off that target, whose own key
/// copy is still `integer`. Its pause records the upstream resume as its
/// cause and its message names that resume, not a column the operator
/// altered. The capture pass then resumes it once the upstream is live, so
/// the operator's one `RESUME` of A carries A -> B -> C:
///
/// - while A rebuilds, B stays paused with no request and no build;
/// - the first pass after A is live re-types B's target and pauses C with
///   B as its cause;
/// - C resumes the same way once B is live.
#[tokio::test]
async fn one_resume_carries_a_chain_of_definitions_each_rebuilt_after_its_upstream() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = chain(db.dsn(), &mut raw, &db.pool).await;

    resume_head_of_chain(&trellis, &mut raw, &db.pool).await;
    let error = paused_for(&trellis, "b_names", "public.item_names", &["id"]).await;
    assert_eq!(
        error,
        "the resume of transform item_names re-typed public.item_names.id from integer to \
         bigint, which this definition reads, so the columns Trellis created for this \
         definition from it can't hold every value of the types define would give them now: \
         public.b_names.id (integer, now bigint). This definition resumes on its own once \
         item_names is live again, bringing its columns to the new types and rebuilding it. \
         Or drop the definition and define it again"
    );
    assert_waiting_on(&trellis, &raw, "b_names", "item_names").await;
    assert_unpaused(&trellis, "c_names").await;

    // Passes while A is waiting to backfill, building and catching up leave
    // B alone.
    capture_pass(&mut raw, &db.pool).await;
    assert_waiting_on(&trellis, &raw, "b_names", "item_names").await;
    step_until_live(&trellis, &mut raw, &db.pool, "item_names", &["b_names"]).await;
    assert_unpaused(&trellis, "c_names").await;

    // The first pass after A is live re-types B's target, resumes B, and
    // pauses C with B as its cause.
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(column_type(&raw, "public.b_names", "id").await, "bigint");
    assert_eq!(
        status(&raw, "b_names").await,
        TransformStatus::WaitingToBackfill
    );
    assert_unpaused(&trellis, "b_names").await;
    let error = paused_for(&trellis, "c_names", "public.b_names", &["id"]).await;
    assert!(
        error.starts_with(
            "the resume of transform b_names re-typed public.b_names.id from integer to bigint"
        ) && error.contains("resumes on its own once b_names is live again"),
        "{error}"
    );
    assert_waiting_on(&trellis, &raw, "c_names", "b_names").await;

    step_until_live(&trellis, &mut raw, &db.pool, "b_names", &["c_names"]).await;
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(column_type(&raw, "public.c_names", "id").await, "bigint");
    assert_eq!(
        status(&raw, "c_names").await,
        TransformStatus::WaitingToBackfill
    );
    assert_chain_converges(&trellis, &mut raw, &db.pool).await;
}

/// #970: the durable queue is `caused_by` plus `resume_requests`: dropping
/// every handle after A's rebuild, and running the next pass from fresh
/// ones, resumes B, and then C.
#[tokio::test]
async fn a_restart_after_the_upstream_rebuild_resumes_the_chain_from_where_it_stopped() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = chain(db.dsn(), &mut raw, &db.pool).await;

    resume_head_of_chain(&trellis, &mut raw, &db.pool).await;
    step_until_live(&trellis, &mut raw, &db.pool, "item_names", &["b_names"]).await;
    drop(trellis);
    drop(raw);

    let mut raw = connect(db.dsn()).await;
    let trellis = definer(db.dsn()).await;
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(column_type(&raw, "public.b_names", "id").await, "bigint");
    assert_eq!(
        status(&raw, "b_names").await,
        TransformStatus::WaitingToBackfill
    );
    assert_waiting_on(&trellis, &raw, "c_names", "b_names").await;
    step_until_live(&trellis, &mut raw, &db.pool, "b_names", &["c_names"]).await;
    drop(trellis);
    drop(raw);

    let mut raw = connect(db.dsn()).await;
    let trellis = definer(db.dsn()).await;
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(column_type(&raw, "public.c_names", "id").await, "bigint");
    assert_eq!(
        status(&raw, "c_names").await,
        TransformStatus::WaitingToBackfill
    );
    assert_chain_converges(&trellis, &mut raw, &db.pool).await;
}

/// #970: the automatic resume of B comes back with copies to re-type, so
/// it takes the request path an operator's resume does: the same pass
/// records the request, re-types and completes it, and the cause is gone
/// once the request is recorded. A crash between the request and the
/// re-type (the request recorded by hand, the re-type never run) leaves the
/// next pass to finish it.
#[tokio::test]
async fn a_crash_after_the_automatic_resume_requests_the_re_type_leaves_the_request_to_finish() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = chain(db.dsn(), &mut raw, &db.pool).await;

    resume_head_of_chain(&trellis, &mut raw, &db.pool).await;
    step_until_live(&trellis, &mut raw, &db.pool, "item_names", &["b_names"]).await;
    // The automatic resume's transaction alone: the request replaces the
    // cause (`request_retype`), as it does in the pass.
    raw.batch_execute(
        "insert into resume_requests (transform_id) \
           select id from transform_definitions where target_table = 'public.b_names'; \
         update capture_failures set caused_by = null, error = 'resuming: re-typing' \
          where transform_id = (select id from transform_definitions \
                                 where target_table = 'public.b_names')",
    )
    .await
    .expect("the request, then a crash");
    assert_eq!(column_type(&raw, "public.b_names", "id").await, "integer");

    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(column_type(&raw, "public.b_names", "id").await, "bigint");
    assert_eq!(
        status(&raw, "b_names").await,
        TransformStatus::WaitingToBackfill
    );
    assert_waiting_on(&trellis, &raw, "c_names", "b_names").await;
    step_until_live(&trellis, &mut raw, &db.pool, "b_names", &["c_names"]).await;
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(column_type(&raw, "public.c_names", "id").await, "bigint");
    assert_chain_converges(&trellis, &mut raw, &db.pool).await;
}

/// #970: B is refused at its automatic resume: the schema changed after
/// the pause, so define would refuse it. The refusal is recorded on its
/// `capture_failure`, the cause is cleared, and the next pass doesn't try
/// again (its `detected_at` stands). C is untouched: B's target was never
/// re-typed, so C stays live, reading it.
#[tokio::test]
async fn a_definition_refused_at_its_automatic_resume_stays_paused_with_the_refusal() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = items(db.dsn(), &mut raw, &db.pool).await;
    raw.batch_execute(
        "create table public.owners (code varchar(10) primary key, label text); \
         insert into public.owners values ('one', 'First'), ('two', 'Second');",
    )
    .await
    .expect("seed owners");
    for text in [
        "RELATIONSHIP owner FROM item_names.name TO owners.code",
        "TRANSFORM b_owners FROM public.item_names SELECT owner.label AS owner_label",
    ] {
        trellis.apply(text).await.expect(text);
    }
    bring_live(&mut raw, &db.pool, &["item_names", "b_owners"]).await;
    trellis
        .apply("TRANSFORM c_owners FROM public.b_owners SELECT owner_label AS owner_label")
        .await
        .expect("define c_owners");
    bring_live(&mut raw, &db.pool, &["item_names", "b_owners", "c_owners"]).await;

    raw.batch_execute("alter table public.items alter column id type bigint")
        .await
        .expect("widen the key");
    capture_pass(&mut raw, &db.pool).await;
    resume(&trellis, "item_names").await;
    capture_pass(&mut raw, &db.pool).await;
    assert_waiting_on(&trellis, &raw, "b_owners", "item_names").await;

    // The schema changes after the pause: define refuses a join column of
    // type character(n).
    raw.batch_execute("alter table public.owners alter column code type character(10)")
        .await
        .expect("re-type the to-side join column");
    step_until_live(&trellis, &mut raw, &db.pool, "item_names", &["b_owners"]).await;

    capture_pass(&mut raw, &db.pool).await;
    let error = paused_for(&trellis, "b_owners", "public.item_names", &[]).await;
    assert!(
        error.starts_with("the resume was refused: ") && error.contains("character(10)"),
        "{error}"
    );
    assert_eq!(caused_by(&raw, "b_owners").await, None);
    assert_eq!(column_type(&raw, "public.b_owners", "id").await, "integer");
    let detected_at = failure_detected_at(&raw, "b_owners").await;

    // The next pass leaves the record as it is.
    capture_pass(&mut raw, &db.pool).await;
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        paused_for(&trellis, "b_owners", "public.item_names", &[]).await,
        error
    );
    assert_eq!(detected_at, failure_detected_at(&raw, "b_owners").await);
    // C is untouched: never paused, reading B's paused target.
    assert_unpaused(&trellis, "c_owners").await;
    assert_eq!(status(&raw, "c_owners").await, TransformStatus::Live);
}

/// #970: a `RESUME` by hand of a definition waiting on its upstream resumes
/// it at once, though the upstream isn't live yet: the request replaces the
/// cause, and the next pass re-types its target and pauses the definition
/// below it with it as the cause.
#[tokio::test]
async fn a_resume_by_hand_of_a_waiting_definition_resumes_it_before_its_upstream_is_live() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = chain(db.dsn(), &mut raw, &db.pool).await;

    resume_head_of_chain(&trellis, &mut raw, &db.pool).await;
    assert_waiting_on(&trellis, &raw, "b_names", "item_names").await;
    resume(&trellis, "b_names").await;
    assert_eq!(caused_by(&raw, "b_names").await, None);

    capture_pass(&mut raw, &db.pool).await;
    assert_ne!(status(&raw, "item_names").await, TransformStatus::Live);
    assert_eq!(column_type(&raw, "public.b_names", "id").await, "bigint");
    assert_eq!(
        status(&raw, "b_names").await,
        TransformStatus::WaitingToBackfill
    );
    assert_waiting_on(&trellis, &raw, "c_names", "b_names").await;
    assert_chain_converges(&trellis, &mut raw, &db.pool).await;
}

/// #828: a definition the operator paused before the upstream's resume
/// re-typed the target it reads was paused for another reason first. It
/// gets the pause record, as before, but no cause: its message is the
/// ordinary one. A sibling the resume did pause records it.
#[tokio::test]
async fn a_definition_paused_before_the_upstream_resume_records_no_cause() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = items(db.dsn(), &mut raw, &db.pool).await;
    for text in [
        "TRANSFORM b_names FROM public.item_names SELECT name AS name",
        "TRANSFORM b_next FROM public.item_names SELECT next AS next",
    ] {
        trellis.apply(text).await.expect(text);
    }
    bring_live(&mut raw, &db.pool, &["item_names", "b_names", "b_next"]).await;
    trellis
        .apply("PAUSE TRANSFORM b_next")
        .await
        .expect("pause b_next");

    raw.batch_execute("alter table public.items alter column id type bigint")
        .await
        .expect("widen the key");
    capture_pass(&mut raw, &db.pool).await;
    resume(&trellis, "item_names").await;
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(column_type(&raw, "public.item_names", "id").await, "bigint");

    paused_for(&trellis, "b_names", "public.item_names", &["id"]).await;
    assert_eq!(
        caused_by(&raw, "b_names").await.as_deref(),
        Some("item_names")
    );
    let error = paused_for(&trellis, "b_next", "public.item_names", &["id"]).await;
    assert!(
        !error.contains("the resume of") && error.contains("Resume the definition"),
        "{error}"
    );
    assert_eq!(caused_by(&raw, "b_next").await, None);
}

/// #828: a definition the upstream's re-type pauses that also fails its own
/// re-validation keeps its own error, with no cause: here the re-typed key
/// is a relationship's to-side, whose join columns no longer match, which a
/// resume refuses until the other side matches.
#[tokio::test]
async fn a_definition_the_re_type_leaves_refused_keeps_its_own_error() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = items(db.dsn(), &mut raw, &db.pool).await;
    raw.batch_execute(
        "create table public.notes (id int primary key, item int, body text); \
         insert into public.notes values (1, 1, 'x'), (2, 2, 'y');",
    )
    .await
    .expect("seed notes");
    for text in [
        "RELATIONSHIP item FROM notes.item TO item_names.id",
        "TRANSFORM note_items FROM public.notes SELECT item.name AS item_name",
    ] {
        trellis.apply(text).await.expect(text);
    }
    bring_live(&mut raw, &db.pool, &["item_names", "note_items"]).await;

    raw.batch_execute("alter table public.items alter column id type bigint")
        .await
        .expect("widen the key");
    capture_pass(&mut raw, &db.pool).await;
    resume(&trellis, "item_names").await;
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(column_type(&raw, "public.item_names", "id").await, "bigint");

    // The pass checks the relationship's from-side first, and pauses it
    // there.
    let error = paused_for(&trellis, "note_items", "public.notes", &["item"]).await;
    assert!(
        !error.contains("the resume of") && error.contains("refuses until that is fixed"),
        "{error}"
    );
    assert_eq!(caused_by(&raw, "note_items").await, None);
    resume_refused(&trellis, "note_items").await;
}

/// #828: [`a_definition_the_re_type_leaves_refused_keeps_its_own_error`]
/// with the refusal found on the re-typed target itself. The relationship's
/// from-side is another definition's target (`z_notes`), which the pass
/// checks after `item_names`, so the pairing that no longer matches is
/// found while checking the table the upstream resume re-typed, alongside
/// a projection column (`next`) that outgrew only because that resume
/// re-typed it. The refusal still stands, with no cause, while a sibling
/// the same check pauses for the re-type alone records it.
#[tokio::test]
async fn a_definition_refused_on_the_re_typed_target_keeps_its_own_error() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = items(db.dsn(), &mut raw, &db.pool).await;
    raw.batch_execute(
        "create table public.notes (id int primary key, item int, body text); \
         insert into public.notes values (1, 1, 'x'), (2, 2, 'y');",
    )
    .await
    .expect("seed notes");
    for text in [
        "TRANSFORM z_notes FROM public.notes SELECT item AS item",
        "TRANSFORM b_names FROM public.item_names SELECT name AS name",
    ] {
        trellis.apply(text).await.expect(text);
    }
    bring_live(&mut raw, &db.pool, &["item_names", "z_notes", "b_names"]).await;
    for text in [
        "RELATIONSHIP item FROM z_notes.item TO item_names.id",
        "TRANSFORM note_items FROM public.z_notes \
         SELECT item.name AS item_name, item.next AS item_next",
    ] {
        trellis.apply(text).await.expect(text);
    }
    bring_live(
        &mut raw,
        &db.pool,
        &["item_names", "z_notes", "b_names", "note_items"],
    )
    .await;

    raw.batch_execute(
        "alter table public.items alter column id type bigint, alter column qty type bigint",
    )
    .await
    .expect("widen the key and qty");
    capture_pass(&mut raw, &db.pool).await;
    resume(&trellis, "item_names").await;
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(column_type(&raw, "public.item_names", "id").await, "bigint");

    let error = paused_for(&trellis, "note_items", "public.item_names", &["id", "next"]).await;
    assert!(
        !error.contains("the resume of") && error.contains("refuses until that is fixed"),
        "{error}"
    );
    assert_eq!(caused_by(&raw, "note_items").await, None);
    resume_refused(&trellis, "note_items").await;
    // The sibling the same check pauses for the re-type alone records it.
    paused_for(&trellis, "b_names", "public.item_names", &["id"]).await;
    assert_eq!(
        caused_by(&raw, "b_names").await.as_deref(),
        Some("item_names")
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

/// The columns #824's in-place re-type reaches, over `public.users` and
/// `public.posts` (`posts.author` related to `users.handle`):
///
/// - `user_labels`, a 1-1 definition: its target's key copies
///   `users.handle`, and its passthrough `users.label`;
/// - `post_labels` reads `author.label`: the relationship's projection
///   copies `users.handle` as its key and `users.label` as a data column;
/// - `per_author` takes `MAX(amount)` per `author`: its ledger's
///   contribution column is typed as `posts.amount`'s family.
///
/// `key` is the type of `users.handle` and `posts.author`, `value` of
/// `users.label` and `posts.amount`. Returns the projection, qualified.
async fn widening_fixture(
    dsn: &str,
    raw: &mut Client,
    pool: &trellis::Pool,
    key: &str,
    value: &str,
) -> String {
    raw.batch_execute(&format!(
        "create table public.users (handle {key} primary key, label {value}); \
         create table public.posts (id int primary key, author {key}, amount {value}); \
         insert into public.users values ('ann', '1.50'), ('bob', '2.25'); \
         insert into public.posts values (1, 'ann', '1.50'), (2, 'bob', '2.25'), \
                                         (3, 'ann', '3.75');"
    ))
    .await
    .expect("seed");
    let trellis = definer(dsn).await;
    for text in [
        "RELATIONSHIP author FROM posts.author TO users.handle",
        "TRANSFORM user_labels FROM public.users SELECT label AS label",
        "TRANSFORM post_labels FROM public.posts SELECT author.label AS author_label",
        "TRANSFORM per_author FROM public.posts GROUP BY author \
         SELECT author AS author, MAX(amount) AS top",
    ] {
        trellis.apply(text).await.expect(text);
    }
    bring_live(raw, pool, &WIDENING_TARGETS).await;
    let projection: String = raw
        .query_one("select projection_table from relationship_projections", &[])
        .await
        .expect("the relationship's projection")
        .get(0);
    format!("trellis.{projection}")
}

const WIDENING_TARGETS: [&str; 3] = ["user_labels", "post_labels", "per_author"];

/// Each of [`widening_fixture`]'s targets against its oracle.
async fn assert_widening_targets_match(raw: &Client) {
    for (target, oracle) in [
        (
            "select handle::text, label::text from public.user_labels order by 1",
            "select handle::text, label::text from public.users order by 1",
        ),
        (
            "select id::text, author_label::text from public.post_labels order by 1",
            "select p.id::text, u.label::text from public.posts p \
             left join public.users u on u.handle = p.author order by 1",
        ),
        (
            "select author::text, top::text from public.per_author order by 1",
            "select author::text, max(amount)::text from public.posts group by 1 order by 1",
        ),
    ] {
        assert_eq!(rows(raw, target).await, rows(raw, oracle).await, "{target}");
    }
}

/// #824 rule 2: one catalog-only widening. The source's key columns are
/// re-typed `key_to` (or kept, when `None`) and its value columns
/// `value_to`; one staging-worker pass re-types every column Trellis copied
/// from them in place, to `key_display` and `value_display`, without a
/// rewrite, a pause or a rebuild: every definition stays live. A key and a
/// value the old types couldn't hold then apply. The contribution column is
/// typed by the value's family, so it holds them already and is left alone.
async fn a_catalog_only_widening_re_types_in_place(
    key: &str,
    key_to: Option<(&str, &str)>,
    value: &str,
    (value_to, value_display): (&str, &str),
    (long_key, long_value): (&str, &str),
) {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let projection = widening_fixture(db.dsn(), &mut raw, &db.pool, key, value).await;
    let contribution = column_type(&raw, "public.per_author__ledger", "__arg0").await;
    let target_file = relfilenode(&raw, "public.user_labels").await;
    let projection_file = relfilenode(&raw, &projection).await;

    let mut alter = format!(
        "alter table public.users alter column label type {value_to}; \
         alter table public.posts alter column amount type {value_to};"
    );
    if let Some((key_to, _)) = key_to {
        alter.push_str(&format!(
            "alter table public.users alter column handle type {key_to}; \
             alter table public.posts alter column author type {key_to};"
        ));
    }
    raw.batch_execute(&alter).await.expect("widen the source");
    full_pass(&mut raw, &db.pool).await;

    for target in WIDENING_TARGETS {
        assert_eq!(
            status(&raw, target).await,
            TransformStatus::Live,
            "{target}"
        );
    }
    assert_eq!(
        column_type(&raw, "public.user_labels", "label").await,
        value_display
    );
    assert_eq!(column_type(&raw, &projection, "label").await, value_display);
    if let Some((_, key_display)) = key_to {
        assert_eq!(
            column_type(&raw, "public.user_labels", "handle").await,
            key_display
        );
        assert_eq!(column_type(&raw, &projection, "handle").await, key_display);
    }
    assert_eq!(
        column_type(&raw, "public.per_author__ledger", "__arg0").await,
        contribution
    );
    assert_eq!(
        relfilenode(&raw, "public.user_labels").await,
        target_file,
        "no rewrite"
    );
    assert_eq!(
        relfilenode(&raw, &projection).await,
        projection_file,
        "no rewrite"
    );

    raw.execute(
        &format!("insert into public.users values ($1, $2::text::{value_to})"),
        &[&long_key, &long_value],
    )
    .await
    .expect("a key and a value the old types can't hold");
    raw.execute(
        &format!("insert into public.posts values (4, $1, $2::text::{value_to})"),
        &[&long_key, &long_value],
    )
    .await
    .expect("a post by it");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    for target in WIDENING_TARGETS {
        assert_eq!(
            status(&raw, target).await,
            TransformStatus::Live,
            "{target}"
        );
    }
    let held: i64 = raw
        .query_one("select count(*) from poison", &[])
        .await
        .expect("read poison")
        .get(0);
    assert_eq!(held, 0);
    assert_widening_targets_match(&raw).await;
}

#[tokio::test]
async fn a_longer_varchar_is_re_typed_in_place() {
    a_catalog_only_widening_re_types_in_place(
        "varchar(10)",
        Some(("varchar(40)", "character varying(40)")),
        "varchar(10)",
        ("varchar(40)", "character varying(40)"),
        ("a handle past ten", "a label past ten characters"),
    )
    .await;
}

#[tokio::test]
async fn a_bounded_varchar_to_text_is_re_typed_in_place() {
    a_catalog_only_widening_re_types_in_place(
        "varchar(10)",
        Some(("text", "text")),
        "varchar(10)",
        ("text", "text"),
        ("a handle past ten", "a label past ten characters"),
    )
    .await;
}

#[tokio::test]
async fn an_unbounded_varchar_to_text_is_re_typed_in_place() {
    a_catalog_only_widening_re_types_in_place(
        "varchar",
        Some(("text", "text")),
        "varchar",
        ("text", "text"),
        ("a handle past ten", "a label past ten characters"),
    )
    .await;
}

#[tokio::test]
async fn removing_a_varchar_length_is_re_typed_in_place() {
    a_catalog_only_widening_re_types_in_place(
        "varchar(10)",
        Some(("varchar", "character varying")),
        "varchar(10)",
        ("varchar", "character varying"),
        ("a handle past ten", "a label past ten characters"),
    )
    .await;
}

/// `numeric` can't be a key, so only the value columns widen.
#[tokio::test]
async fn more_numeric_precision_at_the_same_scale_is_re_typed_in_place() {
    a_catalog_only_widening_re_types_in_place(
        "varchar(10)",
        None,
        "numeric(5,2)",
        ("numeric(9,2)", "numeric(9,2)"),
        ("dan", "12345.67"),
    )
    .await;
}

/// #824 rule 3: values written after the source widened and before the
/// pass re-types Trellis's columns fail their writes, `22001` for a
/// `varchar` and `22003` for a `numeric`, and their keys are held: a 1-1
/// target's passthroughs, and a to-one projection's data column, whose
/// failed write holds the to-side key for the definition reading through
/// it. One pass re-types the columns in place and releases those keys, and
/// the next drain applies them. A key held for another reason (a check
/// constraint on the target, `23514`) stays held.
#[tokio::test]
async fn keys_held_for_a_value_the_old_type_couldnt_hold_are_released_after_the_re_type() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.goods (id int primary key, name varchar(10), price numeric(5,2), \
                                    qty int); \
         insert into public.goods values (1, 'one', 1.00, 1), (2, 'two', 2.00, 2); \
         create table public.users (id int primary key, name varchar(10)); \
         create table public.posts (id int primary key, author_id int); \
         insert into public.users values (1, 'Ann'), (2, 'Bob'); \
         insert into public.posts values (1, 1), (2, 2), (3, 1);",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    for text in [
        "TRANSFORM good_names FROM public.goods SELECT name AS name, price AS price, qty AS qty",
        "RELATIONSHIP author FROM posts.author_id TO users.id",
        "TRANSFORM post_authors FROM public.posts SELECT author.name AS author_name",
    ] {
        trellis.apply(text).await.expect(text);
    }
    let targets = ["good_names", "post_authors"];
    bring_live(&mut raw, &db.pool, &targets).await;
    raw.batch_execute("alter table public.good_names add constraint few check (qty < 100)")
        .await
        .expect("a check on the target");

    raw.batch_execute(
        "alter table public.goods alter column name type varchar(40), \
                                  alter column price type numeric(9,2); \
         alter table public.users alter column name type varchar(40); \
         insert into public.goods values (3, 'a name past ten characters', 3.00, 3), \
                                         (4, 'four', 12345.67, 4), (5, 'five', 5.00, 500); \
         update public.users set name = 'Ann With A Long Name' where id = 1;",
    )
    .await
    .expect("widen, then write values the copies can't hold");
    drain_past_failures(&db.pool, &mut raw).await;
    let held = async |raw: &Client| {
        rows(
            raw,
            "select split_part(d.target_table, '.', 2), p.src_table, p.key, p.sqlstate \
             from poison p join transform_definitions d on d.id = p.transform_id order by 1, 3",
        )
        .await
    };
    let row = |target: &str, table: &str, key: &str, sqlstate: &str| {
        [target, table, key, sqlstate]
            .into_iter()
            .map(|v| Some(v.to_string()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        held(&raw).await,
        vec![
            row("good_names", "public.goods", "3", "22001"),
            row("good_names", "public.goods", "4", "22003"),
            row("good_names", "public.goods", "5", "23514"),
            row("post_authors", "public.users", "1", "22001"),
        ]
    );

    full_pass(&mut raw, &db.pool).await;
    for target in targets {
        assert_eq!(
            status(&raw, target).await,
            TransformStatus::Live,
            "{target}"
        );
    }
    assert_eq!(
        column_type(&raw, "public.good_names", "name").await,
        "character varying(40)"
    );
    assert_eq!(
        column_type(&raw, "public.good_names", "price").await,
        "numeric(9,2)"
    );
    assert_eq!(
        held(&raw).await,
        vec![row("good_names", "public.goods", "5", "23514")]
    );
    let requests = async |raw: &Client| -> i64 {
        raw.query_one("select count(*) from retype_releases", &[])
            .await
            .expect("read retype_releases")
            .get(0)
    };
    assert_eq!(
        requests(&raw).await,
        3,
        "each request is kept for its window: 22001 and 22003 for good_names, 22001 for \
         post_authors"
    );

    // The goods apply in the first drain. The released `users` key's
    // re-derive reaches `post_authors`' rows through the reverse records it
    // stages, which the second drain applies (#754). Key 5's held rows keep
    // the ring from quiescence, so each drain is one bounded round.
    drain_past_failures(&db.pool, &mut raw).await;
    drain_past_failures(&db.pool, &mut raw).await;
    assert_eq!(
        rows(
            &raw,
            "select id::text, name, price::text, qty::text from public.good_names order by id"
        )
        .await,
        rows(
            &raw,
            "select id::text, name::text, price::text, qty::text from public.goods \
             where id <> 5 order by id"
        )
        .await,
    );
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

    // A drain that reproduced a key's failure before the re-type can commit
    // its eviction after the pass's release read the keys held. The next
    // pass within the request's window releases it.
    raw.execute(
        "insert into poison (transform_id, src_table, key, last_error, sqlstate) \
         select id, 'public.goods', '2', 'value too long for type character varying(10)', \
                '22001' \
         from transform_definitions where target_table = 'public.good_names'",
        &[],
    )
    .await
    .expect("a key evicted after the release");
    full_pass(&mut raw, &db.pool).await;
    assert_eq!(
        held(&raw).await,
        vec![row("good_names", "public.goods", "5", "23514")]
    );
    // Past the window, a pass that finds nothing more to release consumes
    // the requests.
    raw.batch_execute("update retype_releases set requested_at = requested_at - interval '1 hour'")
        .await
        .expect("age the requests past their window");
    full_pass(&mut raw, &db.pool).await;
    assert_eq!(requests(&raw).await, 0, "every requested release was made");
}

/// #824: only the named widenings are re-typed in place. A `character(n)`
/// length change (its values are blank-padded to the length) and a `text`
/// column made `varchar(n)` (binary-coercible, but a narrowing) are left
/// as they were, and, as before, pause nothing: every value still fits a
/// passthrough of either.
#[tokio::test]
async fn character_and_text_to_varchar_changes_are_not_re_typed_in_place() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.tags (id int primary key, code char(5), note text); \
         insert into public.tags values (1, 'ab', 'x'), (2, 'cd', 'y');",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply("TRANSFORM tag_codes FROM public.tags SELECT code AS code, note AS note")
        .await
        .expect("define tag_codes");
    bring_live(&mut raw, &db.pool, &["tag_codes"]).await;

    raw.batch_execute(
        "alter table public.tags alter column code type char(9), \
                                 alter column note type varchar(20)",
    )
    .await
    .expect("change both columns");
    full_pass(&mut raw, &db.pool).await;
    assert_eq!(status(&raw, "tag_codes").await, TransformStatus::Live);
    assert_eq!(
        column_type(&raw, "public.tag_codes", "code").await,
        "character(5)"
    );
    assert_eq!(column_type(&raw, "public.tag_codes", "note").await, "text");
}

/// #824: the in-place re-type waits for its table's lock at most
/// `RETYPE_LOCK_TIMEOUT`. While another session holds a lock on the
/// target, the pass changes nothing and pauses nothing; the next pass,
/// with the lock gone, re-types it.
#[tokio::test]
async fn a_re_type_that_cant_lock_its_table_changes_nothing_and_pauses_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    items(db.dsn(), &mut raw, &db.pool).await;

    raw.batch_execute("alter table public.items alter column name type varchar(40)")
        .await
        .expect("widen the passthrough");
    let locker = connect(db.dsn()).await;
    locker
        .batch_execute("begin; lock table public.item_names in access share mode")
        .await
        .expect("hold a lock on the target");
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(status(&raw, "item_names").await, TransformStatus::Live);
    assert_eq!(
        column_type(&raw, "public.item_names", "name").await,
        "character varying(10)"
    );
    let requests: i64 = raw
        .query_one("select count(*) from retype_releases", &[])
        .await
        .expect("read retype_releases")
        .get(0);
    assert_eq!(requests, 0);

    locker.batch_execute("rollback").await.expect("let go");
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(status(&raw, "item_names").await, TransformStatus::Live);
    assert_eq!(
        column_type(&raw, "public.item_names", "name").await,
        "character varying(40)"
    );
}

/// #824: an in-place re-type that fails for another reason than its lock
/// (a view on the target's column) leaves the table as it was, asks for no
/// release, and pauses the definition as a widening that needs a rewrite
/// does, naming the column, for its resume to re-type.
#[tokio::test]
async fn a_re_type_in_place_that_fails_pauses_its_definition_instead() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = items(db.dsn(), &mut raw, &db.pool).await;

    raw.batch_execute(
        "create view public.item_name_list as select name from public.item_names; \
         alter table public.items alter column name type varchar(40)",
    )
    .await
    .expect("a view on the target's column, then widen the passthrough");
    capture_pass(&mut raw, &db.pool).await;
    let error = paused_for(&trellis, "item_names", "public.items", &["name"]).await;
    assert!(error.contains("character varying(40)"), "{error}");
    assert_eq!(
        column_type(&raw, "public.item_names", "name").await,
        "character varying(10)"
    );
    let requests: i64 = raw
        .query_one("select count(*) from retype_releases", &[])
        .await
        .expect("read retype_releases")
        .get(0);
    assert_eq!(requests, 0);
}

/// #824: a failed in-place re-type is remembered for the definitions that
/// owned the table, not for the table's name. A definition dropped and
/// defined again under the same target, with no pass in between to see the
/// old one gone, tries the same re-type afresh: it doesn't pause for the
/// old definition's failure.
#[tokio::test]
async fn a_failed_re_type_in_place_is_not_held_against_a_definition_defined_again() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = items(db.dsn(), &mut raw, &db.pool).await;

    raw.batch_execute(
        "create view public.item_name_list as select name from public.item_names; \
         alter table public.items alter column name type varchar(40)",
    )
    .await
    .expect("a view on the target's column, then widen the passthrough");
    capture_pass(&mut raw, &db.pool).await;
    paused_for(&trellis, "item_names", "public.items", &["name"]).await;

    raw.batch_execute(
        "drop view public.item_name_list; \
         alter table public.items alter column name type varchar(10)",
    )
    .await
    .expect("drop the view, and narrow the passthrough back");
    trellis
        .apply("DROP TRANSFORM item_names")
        .await
        .expect("drop item_names");
    trellis
        .apply("TRANSFORM item_names FROM public.items SELECT name AS name, qty + 1 AS next")
        .await
        .expect("define item_names again");
    assert_eq!(
        column_type(&raw, "public.item_names", "name").await,
        "character varying(10)"
    );
    raw.batch_execute("alter table public.items alter column name type varchar(40)")
        .await
        .expect("widen the passthrough again");
    capture_pass(&mut raw, &db.pool).await;
    assert_ne!(status(&raw, "item_names").await, TransformStatus::Paused);
    assert_eq!(
        column_type(&raw, "public.item_names", "name").await,
        "character varying(40)"
    );
    bring_live(&mut raw, &db.pool, &["item_names"]).await;
}

/// Counts each in-place re-type of `public.item_names` the server starts,
/// in a sequence a rolled-back `ALTER` doesn't undo, and fails it with the
/// SQLSTATE in `public.retype_injection`, if any. An event trigger at
/// `ddl_command_start` runs before the `ALTER` takes its lock.
async fn watch_item_names_retypes(raw: &Client) {
    raw.batch_execute(
        "create table public.retype_injection (code text); \
         create sequence public.retype_attempts; \
         create function public.watch_retype() returns event_trigger language plpgsql as $$ \
         declare injected text; \
         begin \
           if pg_catalog.current_query() ilike 'alter table %item_names%' then \
             perform pg_catalog.nextval('public.retype_attempts'); \
             select code into injected from public.retype_injection limit 1; \
             if injected is not null then \
               raise exception 'injected %', injected using errcode = injected; \
             end if; \
           end if; \
         end $$; \
         create event trigger watch_retype on ddl_command_start when tag in ('ALTER TABLE') \
           execute function public.watch_retype();",
    )
    .await
    .expect("watch item_names's re-types");
}

/// How many in-place re-types of `public.item_names` the server started
/// ([`watch_item_names_retypes`]).
async fn retype_attempts(raw: &Client) -> i64 {
    raw.query_one(
        "select case when is_called then last_value else 0 end from public.retype_attempts",
        &[],
    )
    .await
    .expect("read retype_attempts")
    .get(0)
}

/// #824: an in-place re-type that fails transiently (a deadlock, a
/// statement or lock timeout's cancel) changes nothing and pauses nothing,
/// as one that can't get its lock doesn't: the next pass tries again.
#[tokio::test]
async fn a_re_type_in_place_that_fails_transiently_pauses_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    items(db.dsn(), &mut raw, &db.pool).await;
    watch_item_names_retypes(&raw).await;

    raw.batch_execute("alter table public.items alter column name type varchar(40)")
        .await
        .expect("widen the passthrough");
    for (attempt, code) in ["40P01", "57014"].into_iter().enumerate() {
        raw.execute("insert into public.retype_injection values ($1)", &[&code])
            .await
            .expect("inject the error");
        capture_pass(&mut raw, &db.pool).await;
        assert_eq!(retype_attempts(&raw).await, attempt as i64 + 1, "{code}");
        assert_eq!(
            status(&raw, "item_names").await,
            TransformStatus::Live,
            "{code}"
        );
        assert_eq!(
            column_type(&raw, "public.item_names", "name").await,
            "character varying(10)",
            "{code}"
        );
        raw.batch_execute("delete from public.retype_injection")
            .await
            .expect("stop injecting");
    }
    let requests: i64 = raw
        .query_one("select count(*) from retype_releases", &[])
        .await
        .expect("read retype_releases")
        .get(0);
    assert_eq!(requests, 0);

    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(retype_attempts(&raw).await, 3);
    assert_eq!(status(&raw, "item_names").await, TransformStatus::Live);
    assert_eq!(
        column_type(&raw, "public.item_names", "name").await,
        "character varying(40)"
    );
}

/// #828: a resume's re-type that fails transiently (a deadlock, a statement
/// timeout's cancel) leaves the request queued, as one that can't get its
/// lock does: the definition stays paused waiting on the re-type, with no
/// failed-conversion message and no `DROP TRANSFORM` advice, and the next
/// clean pass completes the resume.
#[tokio::test]
async fn a_resume_re_type_that_fails_transiently_keeps_the_request() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = items(db.dsn(), &mut raw, &db.pool).await;

    raw.batch_execute("alter table public.items alter column id type bigint")
        .await
        .expect("widen the key");
    capture_pass(&mut raw, &db.pool).await;
    paused_for(&trellis, "item_names", "public.items", &["id"]).await;
    resume(&trellis, "item_names").await;
    watch_item_names_retypes(&raw).await;

    for (attempt, code) in ["40P01", "57014"].into_iter().enumerate() {
        raw.execute("insert into public.retype_injection values ($1)", &[&code])
            .await
            .expect("inject the error");
        capture_pass(&mut raw, &db.pool).await;
        assert_eq!(retype_attempts(&raw).await, attempt as i64 + 1, "{code}");
        let error = assert_retyping(&trellis, &raw, "item_names").await;
        assert!(
            !error.contains("couldn't re-type") && !error.contains("DROP TRANSFORM"),
            "{code}: {error}"
        );
        assert_eq!(
            column_type(&raw, "public.item_names", "id").await,
            "integer",
            "{code}"
        );
        raw.batch_execute("delete from public.retype_injection")
            .await
            .expect("stop injecting");
    }

    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(retype_attempts(&raw).await, 3);
    assert_eq!(column_type(&raw, "public.item_names", "id").await, "bigint");
    assert_eq!(
        status(&raw, "item_names").await,
        TransformStatus::WaitingToBackfill
    );
    bring_live(&mut raw, &db.pool, &["item_names"]).await;
    assert_item_names_match(&raw).await;
}

/// #824: an in-place re-type that fails for good on a column that isn't
/// outgrown (an unbounded `varchar` to `text`, with a view on the target's
/// column) pauses nothing, since the column still holds every value. The
/// next pass doesn't try it again, so it doesn't take the target's lock
/// every pass: only once the source's type changes again, here after the
/// view is gone.
#[tokio::test]
async fn a_re_type_in_place_that_fails_on_a_column_not_outgrown_is_not_retried() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    items(db.dsn(), &mut raw, &db.pool).await;
    raw.batch_execute("alter table public.items alter column name type varchar")
        .await
        .expect("drop the passthrough's length");
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(
        column_type(&raw, "public.item_names", "name").await,
        "character varying"
    );
    watch_item_names_retypes(&raw).await;

    raw.batch_execute(
        "create view public.item_name_list as select name from public.item_names; \
         alter table public.items alter column name type text",
    )
    .await
    .expect("a view on the target's column, then move the passthrough to text");
    for pass in 0..2 {
        capture_pass(&mut raw, &db.pool).await;
        assert_eq!(retype_attempts(&raw).await, 1, "pass {pass}");
        assert_eq!(
            status(&raw, "item_names").await,
            TransformStatus::Live,
            "pass {pass}"
        );
        assert_eq!(
            column_type(&raw, "public.item_names", "name").await,
            "character varying",
            "pass {pass}"
        );
    }

    raw.batch_execute(
        "drop view public.item_name_list; \
         alter table public.items alter column name type varchar",
    )
    .await
    .expect("drop the view, and move the passthrough back");
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(retype_attempts(&raw).await, 1);
    raw.batch_execute("alter table public.items alter column name type text")
        .await
        .expect("move the passthrough to text again");
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(retype_attempts(&raw).await, 2);
    assert_eq!(status(&raw, "item_names").await, TransformStatus::Live);
    assert_eq!(column_type(&raw, "public.item_names", "name").await, "text");
}

/// #824: a 1-1 key re-typed in place (`varchar(10)` to `varchar(40)`)
/// keeps the collation it copied from the source key, and its index. It is
/// recorded at its new type, as a resume records it. Its target stores
/// keys of that type from then on, so a later narrowing (`varchar(20)`,
/// truncating a key past it) is measured from it, and pauses the
/// definition: the stored key no longer matches the source's.
#[tokio::test]
async fn a_key_re_typed_in_place_is_measured_from_its_new_type() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.codes (code varchar(10) collate \"C\" primary key, n int); \
         insert into public.codes values ('a', 1), ('b', 2);",
    )
    .await
    .expect("seed");
    let trellis = definer(db.dsn()).await;
    trellis
        .apply("TRANSFORM code_counts FROM public.codes SELECT n AS n")
        .await
        .expect("define code_counts");
    bring_live(&mut raw, &db.pool, &["code_counts"]).await;

    let key_index = async |raw: &Client| {
        rows(
            raw,
            "select i.indexrelid::regclass::text, c.relfilenode::text, \
                    (select co.collname::text from pg_catalog.pg_attribute a \
                     join pg_catalog.pg_collation co on co.oid = a.attcollation \
                     where a.attrelid = i.indrelid and a.attname = 'code') \
             from pg_catalog.pg_index i join pg_catalog.pg_class c on c.oid = i.indexrelid \
             where i.indrelid = 'public.code_counts'::regclass and i.indisprimary",
        )
        .await
    };
    let before = key_index(&raw).await;
    assert_eq!(before[0][2].as_deref(), Some("C"));
    raw.batch_execute("alter table public.codes alter column code type varchar(40) collate \"C\"")
        .await
        .expect("widen the key");
    full_pass(&mut raw, &db.pool).await;
    assert_eq!(status(&raw, "code_counts").await, TransformStatus::Live);
    assert_eq!(
        column_type(&raw, "public.code_counts", "code").await,
        "character varying(40)"
    );
    assert_eq!(
        key_index(&raw).await,
        before,
        "the same collation and index"
    );
    raw.batch_execute("insert into public.codes values ('a code past twenty chars', 3)")
        .await
        .expect("a key the old type couldn't hold");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;

    raw.batch_execute(
        "alter table public.codes alter column code type varchar(20) collate \"C\" \
         using code::varchar(20)",
    )
    .await
    .expect("narrow the key, truncating one");
    capture_pass(&mut raw, &db.pool).await;
    let error = paused_for(&trellis, "code_counts", "public.codes", &["code"]).await;
    assert!(
        error.contains("from character varying(40) to character varying(20)"),
        "{error}"
    );
}

// ---- #894: a re-type cancelled by a statement_timeout ends the request ----

/// Gives `public.item_names` an expression index whose function reads
/// `public.retype_gate`: while its `mode` is `cancel`, rebuilding the index
/// (which the `integer` to `bigint` rewrite of `id` does) cancels the
/// session's own statement (`57014`, "user request"); while `sleep`, it
/// sleeps for 30 s, which a `statement_timeout` cancels. No `mode` row, no
/// effect. The cancel is as deterministic as a lock held by hand: no timer.
async fn gate_item_names_retype(raw: &Client) {
    raw.batch_execute(
        "create table public.retype_gate (mode text); \
         create function public.retype_gate_fn(v bigint) returns bigint \
             language plpgsql immutable as $$ \
         declare m text; \
         begin \
             select mode into m from public.retype_gate limit 1; \
             if m = 'cancel' then \
                 perform pg_cancel_backend(pg_backend_pid()); \
                 perform pg_sleep(30); \
             elsif m = 'sleep' then \
                 perform pg_sleep(30); \
             end if; \
             return v; \
         end $$; \
         create index item_names_gate on public.item_names (public.retype_gate_fn(id));",
    )
    .await
    .expect("gate the re-type");
}

async fn set_gate(raw: &Client, mode: Option<&str>) {
    raw.batch_execute("delete from public.retype_gate")
        .await
        .expect("clear the gate");
    if let Some(mode) = mode {
        raw.execute("insert into public.retype_gate values ($1)", &[&mode])
            .await
            .expect("arm the gate");
    }
}

/// `item_names`'s request count, or `None` once the request has ended.
async fn timeout_cancels(raw: &Client, target: &str) -> Option<i32> {
    raw.query_opt(
        "select r.timeout_cancels from resume_requests r join transform_definitions d \
         on d.id = r.transform_id where split_part(d.target_table, '.', 2) = $1",
        &[&target],
    )
    .await
    .expect("read the count")
    .map(|row| row.get(0))
}

/// `item_names` paused for a widened key, its re-type gated, and resumed:
/// the request waits for the staging worker's next pass.
async fn gated_resume(dsn: &str, raw: &mut Client, pool: &trellis::Pool) -> Trellis {
    let trellis = items(dsn, raw, pool).await;
    gate_item_names_retype(raw).await;
    raw.batch_execute("alter table public.items alter column id type bigint")
        .await
        .expect("widen the key");
    capture_pass(raw, pool).await;
    paused_for(&trellis, "item_names", "public.items", &["id"]).await;
    resume(&trellis, "item_names").await;
    assert_retyping(&trellis, raw, "item_names").await;
    trellis
}

/// #894: a re-type cancelled by a `statement_timeout` is retried on the
/// next pass, each time telling the operator why in the `resuming:` message;
/// the third cancellation ends the request, naming the timeout and both
/// remedies. The definition stays paused, the copy keeps its type and the
/// target its rows. A `RESUME` while the request is still there starts the
/// count over; one after it ended starts at 0 and, with the cause gone,
/// finishes the re-type.
#[tokio::test]
async fn a_re_type_cancelled_three_times_ends_the_request_and_a_resume_once_the_cause_is_gone_finishes()
 {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = gated_resume(db.dsn(), &mut raw, &db.pool).await;
    let before = rows(
        &raw,
        "select id::text, name from public.item_names order by id",
    )
    .await;
    set_gate(&raw, Some("cancel")).await;

    for attempt in 1..=2 {
        capture_pass(&mut raw, &db.pool).await;
        let error = assert_retyping(&trellis, &raw, "item_names").await;
        assert!(
            error.contains("public.item_names.id from integer to bigint")
                && error.contains(&format!(
                    "attempt {attempt} of 3, last error: canceling statement due to user request"
                )),
            "{error}"
        );
        assert_eq!(timeout_cancels(&raw, "item_names").await, Some(attempt));
    }

    // A new RESUME while the request waits starts the count over.
    resume(&trellis, "item_names").await;
    assert_eq!(timeout_cancels(&raw, "item_names").await, Some(0));
    let error = assert_retyping(&trellis, &raw, "item_names").await;
    assert!(!error.contains("attempt"), "{error}");

    for attempt in 1..=2 {
        capture_pass(&mut raw, &db.pool).await;
        assert_retyping(&trellis, &raw, "item_names").await;
        assert_eq!(timeout_cancels(&raw, "item_names").await, Some(attempt));
    }
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(timeout_cancels(&raw, "item_names").await, None, "ended");
    let reported = trellis
        .status("item_names")
        .await
        .expect("status")
        .expect("item_names");
    assert_eq!(reported.status, TransformStatus::Paused);
    let error = reported.capture_failure.expect("reason").error;
    assert!(
        error.contains("public.item_names.id")
            && error.contains("target table item_names")
            && error.contains("re-type was cancelled 3 times, by a statement_timeout")
            && error.contains("(last: canceling statement due to user request)")
            && error.contains("Raise statement_timeout for Trellis's role or database")
            && error.contains("restart Trellis")
            && error.contains("resume the definition again")
            && error.contains("DROP TRANSFORM item_names and define it again")
            && !error.starts_with("resuming:"),
        "{error}"
    );
    assert_eq!(
        column_type(&raw, "public.item_names", "id").await,
        "integer"
    );
    assert_eq!(
        rows(
            &raw,
            "select id::text, name from public.item_names order by id"
        )
        .await,
        before
    );
    // Ended, so a further pass leaves it alone.
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(timeout_cancels(&raw, "item_names").await, None);
    assert_eq!(status(&raw, "item_names").await, TransformStatus::Paused);

    // The remedy: with the cause gone, a new RESUME starts at 0 and finishes.
    set_gate(&raw, None).await;
    resume(&trellis, "item_names").await;
    assert_eq!(timeout_cancels(&raw, "item_names").await, Some(0));
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(column_type(&raw, "public.item_names", "id").await, "bigint");
    assert_eq!(
        status(&raw, "item_names").await,
        TransformStatus::WaitingToBackfill
    );
    assert_eq!(timeout_cancels(&raw, "item_names").await, None);
}

/// #894: the other remedy the ending message names. `DROP TRANSFORM` and
/// define again works after a request ended this way, and builds the target
/// with the widened key.
#[tokio::test]
async fn drop_transform_and_define_again_works_after_a_re_type_request_ended_on_timeouts() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = gated_resume(db.dsn(), &mut raw, &db.pool).await;
    set_gate(&raw, Some("cancel")).await;
    for _ in 0..3 {
        capture_pass(&mut raw, &db.pool).await;
    }
    assert_eq!(timeout_cancels(&raw, "item_names").await, None, "ended");
    assert_eq!(status(&raw, "item_names").await, TransformStatus::Paused);

    set_gate(&raw, None).await;
    trellis
        .apply("DROP TRANSFORM item_names")
        .await
        .expect("drop");
    trellis
        .apply("TRANSFORM item_names FROM public.items SELECT name AS name, qty + 1 AS next")
        .await
        .expect("define again");
    bring_live(&mut raw, &db.pool, &["item_names"]).await;
    assert_eq!(column_type(&raw, "public.item_names", "id").await, "bigint");
    raw.batch_execute("insert into public.items values (3000000000, 'big', 3)")
        .await
        .expect("a key above 2^31");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(status(&raw, "item_names").await, TransformStatus::Live);
    assert_item_names_match(&raw).await;
}

/// #894 rule 2: a lock timeout never held the lock, so it isn't counted: the
/// request stays however many passes fail on it, and the message carries the
/// last error without an attempt number. Here a reader of the target holds
/// `ACCESS SHARE`, so the re-type's `ACCESS EXCLUSIVE` times out after
/// `RETYPE_LOCK_TIMEOUT`; once it lets go the next pass re-types.
#[tokio::test]
async fn a_lock_timeout_on_the_re_type_is_retried_without_counting() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = gated_resume(db.dsn(), &mut raw, &db.pool).await;

    let mut reader = connect(db.dsn()).await;
    let held = reader.transaction().await.expect("begin");
    held.batch_execute("lock table public.item_names in access share mode")
        .await
        .expect("hold a read lock on the target");
    capture_pass(&mut raw, &db.pool).await;
    let error = assert_retyping(&trellis, &raw, "item_names").await;
    assert!(
        error.contains("last error: canceling statement due to lock timeout")
            && !error.contains(" of 3"),
        "{error}"
    );
    assert_eq!(timeout_cancels(&raw, "item_names").await, Some(0));
    held.rollback().await.expect("release");

    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(column_type(&raw, "public.item_names", "id").await, "bigint");
    assert_eq!(
        status(&raw, "item_names").await,
        TransformStatus::WaitingToBackfill
    );
}

/// #894: the cancel is the operator's own `statement_timeout`, not only a
/// cancel by request: a re-type that outlasts it is counted the same way.
#[tokio::test]
async fn a_re_type_outlasting_the_statement_timeout_is_counted() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let trellis = gated_resume(db.dsn(), &mut raw, &db.pool).await;
    set_gate(&raw, Some("sleep")).await;

    // The timeout bounds every statement of the pass, not only the re-type,
    // so it is set well above what any of the others takes on a loaded box;
    // the gate's 30 s sleep is still ten times over it.
    raw.batch_execute("set statement_timeout = '3s'")
        .await
        .expect("set the timeout");
    capture_pass(&mut raw, &db.pool).await;
    raw.batch_execute("reset statement_timeout")
        .await
        .expect("reset the timeout");
    let error = assert_retyping(&trellis, &raw, "item_names").await;
    assert!(
        error.contains("attempt 1 of 3, last error: canceling statement due to statement timeout"),
        "{error}"
    );
    assert_eq!(timeout_cancels(&raw, "item_names").await, Some(1));
}

// ---------------------------------------------------------------------
// #858: the capture pass skips a definition's typed-copy comparison while
// nothing it reads has changed (`staging::drift_memo`).
// ---------------------------------------------------------------------

/// Every comparison of typed copies the capture pass has run for the
/// definitions of `raw`'s database, by target.
async fn copy_checks(raw: &Client) -> std::collections::BTreeMap<String, u64> {
    let database: String = raw
        .query_one("select pg_catalog.current_database()::text", &[])
        .await
        .expect("database")
        .get(0);
    raw.query(
        "select id, split_part(target_table, '.', 2) from transform_definitions order by id",
        &[],
    )
    .await
    .expect("definitions")
    .into_iter()
    .map(|row| {
        let id: i64 = row.get(0);
        (
            row.get::<_, String>(1),
            trellis::staging::drift_checks(&database, SCHEMA, id),
        )
    })
    .collect()
}

/// `setup`'s three live definitions after one capture pass has compared
/// each and recorded it, with the checks run so far.
async fn settled(
    db: &testkit::TestDatabase,
    raw: &mut Client,
) -> (Trellis, std::collections::BTreeMap<String, u64>) {
    let trellis = setup(db.dsn(), raw, &db.pool).await;
    capture_pass(raw, &db.pool).await;
    let checks = copy_checks(raw).await;
    assert!(checks.values().all(|n| *n > 0), "{checks:?}");
    (trellis, checks)
}

/// The skip itself: passes over an unchanged schema compare no definition's
/// copies, however many definitions read the table.
#[tokio::test]
async fn passes_over_an_unchanged_schema_compare_no_copies() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let (_trellis, settled) = settled(&db, &mut raw).await;

    for _ in 0..3 {
        capture_pass(&mut raw, &db.pool).await;
    }
    full_pass(&mut raw, &db.pool).await;
    assert_eq!(copy_checks(&raw).await, settled);

    // Writes to the source change no column.
    raw.batch_execute("insert into public.posts values (4, 'bob', 'd', 2, 2.50)")
        .await
        .expect("write");
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(copy_checks(&raw).await, settled);
}

/// A widening after clean passes is found by the next pass, and only the
/// definitions on the changed tables are compared again.
#[tokio::test]
async fn a_widening_after_clean_passes_is_caught_on_the_next_pass() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let (trellis, settled) = settled(&db, &mut raw).await;
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(copy_checks(&raw).await, settled);

    raw.batch_execute("alter table public.posts alter column kind type bigint")
        .await
        .expect("widen");
    capture_pass(&mut raw, &db.pool).await;

    assert_eq!(status(&raw, "per_kind").await, TransformStatus::Paused);
    let error = paused_for(&trellis, "per_kind", "public.posts", &["kind"]).await;
    assert!(
        error.contains("integer") && error.contains("bigint"),
        "{error}"
    );
    let after = copy_checks(&raw).await;
    assert!(after["per_kind"] > settled["per_kind"], "{after:?}");
}

/// A catalog-only widening of a column read through a relationship, after
/// clean passes, is re-typed in place by the next pass, both sides alike.
#[tokio::test]
async fn a_catalog_only_widening_after_clean_passes_is_re_typed_on_the_next_pass() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let (_trellis, settled) = settled(&db, &mut raw).await;
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(copy_checks(&raw).await, settled);

    raw.batch_execute(
        "alter table public.users alter column handle type varchar(40); \
         alter table public.posts alter column author type varchar(40);",
    )
    .await
    .expect("widen");
    capture_pass(&mut raw, &db.pool).await;

    for target in ["post_authors", "post_titles", "per_kind"] {
        assert_eq!(
            status(&raw, target).await,
            TransformStatus::Live,
            "{target}"
        );
    }
    let projection: String = raw
        .query_one("select projection_table from relationship_projections", &[])
        .await
        .expect("projection")
        .get(0);
    assert_eq!(
        column_type(&raw, &format!("trellis.{projection}"), "handle").await,
        "character varying(40)"
    );
    let after = copy_checks(&raw).await;
    assert!(after["post_authors"] > settled["post_authors"], "{after:?}");
}

/// A column re-typed and re-typed back between two passes ends where it was,
/// but is not the column the last comparison saw: the next pass compares
/// again. The fingerprint carries each row's `xmin` for this.
#[tokio::test]
async fn a_column_re_typed_and_back_between_passes_is_compared_again() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let (_trellis, settled) = settled(&db, &mut raw).await;

    raw.batch_execute(
        "alter table public.posts alter column kind type bigint; \
         alter table public.posts alter column kind type integer;",
    )
    .await
    .expect("there and back");
    capture_pass(&mut raw, &db.pool).await;

    for target in ["post_authors", "post_titles", "per_kind"] {
        assert_eq!(
            status(&raw, target).await,
            TransformStatus::Live,
            "{target}"
        );
    }
    let after = copy_checks(&raw).await;
    assert!(after["per_kind"] > settled["per_kind"], "{after:?}");
    // And settled again.
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(copy_checks(&raw).await, after);
}

/// A column Trellis created, re-typed by hand away from what define would
/// give it, is found though the source never changed: the target's own
/// columns are fingerprinted too.
#[tokio::test]
async fn a_copy_re_typed_by_hand_is_compared_again() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let (_trellis, settled) = settled(&db, &mut raw).await;

    raw.batch_execute("alter table public.post_titles alter column title type varchar(100)")
        .await
        .expect("narrow the copy by hand");
    capture_pass(&mut raw, &db.pool).await;
    let after = copy_checks(&raw).await;
    assert!(after["post_titles"] > settled["post_titles"], "{after:?}");
}

/// A definition paused for a widening and resumed is compared afresh, finds
/// its copies current, and then settles: nothing recorded before the pause
/// stands in for the comparison after the resume.
#[tokio::test]
async fn a_resumed_definition_is_compared_again_and_then_skipped() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let (trellis, settled) = settled(&db, &mut raw).await;

    raw.batch_execute("alter table public.posts alter column kind type bigint")
        .await
        .expect("widen");
    capture_pass(&mut raw, &db.pool).await;
    assert_eq!(status(&raw, "per_kind").await, TransformStatus::Paused);
    resume(&trellis, "per_kind").await;
    bring_live(&mut raw, &db.pool, &["per_kind"]).await;
    assert_eq!(column_type(&raw, "public.per_kind", "kind").await, "bigint");
    capture_pass(&mut raw, &db.pool).await;
    let after = copy_checks(&raw).await;
    assert!(after["per_kind"] > settled["per_kind"], "{after:?}");
    for _ in 0..2 {
        capture_pass(&mut raw, &db.pool).await;
    }
    assert_eq!(copy_checks(&raw).await, after);
    assert_eq!(status(&raw, "per_kind").await, TransformStatus::Live);
}

/// The aggregate's ledger and the relationship's projection are read by the
/// comparison, so a change to either alone, with the source untouched,
/// brings it back.
#[tokio::test]
async fn a_change_to_a_ledger_or_projection_column_alone_is_compared_again() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let (_trellis, mut last) = settled(&db, &mut raw).await;
    let projection: String = raw
        .query_one("select projection_table from relationship_projections", &[])
        .await
        .expect("projection")
        .get(0);

    for (table, alter, target) in [
        (
            "public.per_kind__ledger".to_string(),
            "alter column kind type bigint",
            "per_kind",
        ),
        (
            format!("trellis.{projection}"),
            "alter column name type varchar(5)",
            "post_authors",
        ),
    ] {
        raw.batch_execute(&format!("alter table {table} {alter}"))
            .await
            .unwrap_or_else(|err| panic!("{table}: {err}"));
        capture_pass(&mut raw, &db.pool).await;
        let after = copy_checks(&raw).await;
        assert!(after[target] > last[target], "{table}: {after:?}");
        last = after;
    }
}

/// A copy found different from what define would give it, though the source
/// is not past it (here the source narrowed), is compared on every pass.
#[tokio::test]
async fn a_drifted_copy_is_never_skipped() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let (_trellis, settled) = settled(&db, &mut raw).await;

    raw.batch_execute("alter table public.posts alter column title type varchar(5)")
        .await
        .expect("narrow the source");
    capture_pass(&mut raw, &db.pool).await;
    let first = copy_checks(&raw).await;
    assert!(first["post_titles"] > settled["post_titles"], "{first:?}");
    capture_pass(&mut raw, &db.pool).await;
    let second = copy_checks(&raw).await;
    assert!(second["post_titles"] > first["post_titles"], "{second:?}");
    assert_eq!(status(&raw, "post_titles").await, TransformStatus::Live);
}

/// `ALTER TRANSFORM` rewrites a definition in place, giving it a column
/// Trellis copies from the source. The column joins the comparison at once:
/// the definition's recorded stamp names the definition it was taken from.
#[tokio::test]
async fn a_column_added_by_alter_transform_is_compared_after_clean_passes() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let (trellis, settled) = settled(&db, &mut raw).await;

    trellis
        .apply("ALTER TRANSFORM post_titles ADD kind AS k")
        .await
        .expect("add a passthrough");
    bring_live(&mut raw, &db.pool, &["post_titles"]).await;
    capture_pass(&mut raw, &db.pool).await;
    let added = copy_checks(&raw).await;
    assert!(added["post_titles"] > settled["post_titles"], "{added:?}");
    assert_eq!(
        column_type(&raw, "public.post_titles", "k").await,
        "integer"
    );

    raw.batch_execute("alter table public.posts alter column kind type bigint")
        .await
        .expect("widen");
    capture_pass(&mut raw, &db.pool).await;
    // The new copy is among those found outgrown, with the group-by key.
    let error = paused_for(&trellis, "post_titles", "public.posts", &["kind"]).await;
    assert!(error.contains("public.post_titles.k"), "{error}");
}
