//! A key column re-typed after define is re-validated (issue #760).
//!
//! Define refuses a key, join or `GROUP BY` column whose type or collation
//! Trellis can't match by. An `ALTER COLUMN ... TYPE` or `COLLATE` after
//! define can give one such a type, or render the keys Trellis already
//! stored differently. The staging worker's capture pass pauses the
//! definitions that use the column for either, with the reason on
//! `Trellis::status`, and pauses a resumed one again while the column is
//! still changed. A routine widening pauses nothing.
//!
//! Every test is stepped by hand: writes, then explicit capture passes,
//! drains and backfill runs. Nothing polls for convergence (#297).

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::capture::reconcile;
use trellis::defs::TransformStatus;
use trellis::{Config, Trellis, TrellisOptions};

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
/// captured rendering keeps, so its keys would never match. The capture pass
/// pauses only the definition that reads through the relationship. Resuming
/// it while the column is still `character(n)` pauses it again, before its
/// rebuild is dispatched; once the column is back, a resume rebuilds it.
#[tokio::test]
async fn a_join_column_altered_to_character_n_pauses_its_readers_until_it_is_changed_back() {
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
        error.contains("character(8)") && error.contains("relationship 'author'"),
        "{error}"
    );
    assert_eq!(status(&raw, "post_titles").await, TransformStatus::Live);
    assert_eq!(status(&raw, "per_kind").await, TransformStatus::Live);

    trellis
        .apply("RESUME TRANSFORM post_authors")
        .await
        .expect("resume");
    assert_eq!(
        status(&raw, "post_authors").await,
        TransformStatus::WaitingToBackfill
    );
    capture_pass(&mut raw, &db.pool).await;
    paused_for(&trellis, "post_authors", "public.users", &["handle"]).await;

    raw.batch_execute("alter table public.users alter column handle type varchar(8)")
        .await
        .expect("change the column back");
    trellis
        .apply("RESUME TRANSFORM post_authors")
        .await
        .expect("resume");
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

/// The routine changes the issue names pause nothing: widening an integer
/// key, widening both join columns one `ALTER` at a time (a capture pass
/// runs while they differ), `varchar(n)` to `text`, a `numeric` precision
/// change, and a change between deterministic collations. Every definition
/// stays live and keeps converging.
#[tokio::test]
async fn routine_widenings_pause_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let _trellis = setup(db.dsn(), &mut raw, &db.pool).await;

    let recorded = rows(
        &raw,
        "select table_name, column_name, type_name from definition_key_types \
         order by transform_id, table_name, column_name",
    )
    .await;
    assert!(
        recorded.contains(&vec![
            Some("public.posts".to_string()),
            Some("kind".to_string()),
            Some("pg_catalog.int4".to_string()),
        ]),
        "define records the GROUP BY key's type: {recorded:?}"
    );

    for alter in [
        "alter table public.posts alter column id type bigint",
        "alter table public.posts alter column kind type bigint",
        "alter table public.users alter column handle type varchar(32)",
        "alter table public.posts alter column author type varchar(32)",
        "alter table public.posts alter column author type text",
        "alter table public.users alter column handle type text",
        "alter table public.posts alter column price type numeric(12,2)",
        "alter table public.users alter column handle type text collate \"C\"",
        "alter table public.posts alter column author type text collate \"C\"",
    ] {
        raw.batch_execute(alter).await.expect(alter);
        capture_pass(&mut raw, &db.pool).await;
        for target in ["post_authors", "post_titles", "per_kind"] {
            assert_eq!(status(&raw, target).await, TransformStatus::Live, "{alter}");
        }
    }

    raw.batch_execute(
        "insert into public.users values ('cy', 'Cy'); \
         insert into public.posts values (4, 'cy', 'd', 2, 3.50);",
    )
    .await
    .expect("writes");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        rows(
            &raw,
            "select kind::text, n::text from public.per_kind where n <> 0 order by kind"
        )
        .await,
        rows(
            &raw,
            "select kind::text, count(*)::text from public.posts group by kind order by kind"
        )
        .await,
    );
}

/// A change that renders the existing keys differently pauses the
/// definitions keyed by them, though define would accept the new type:
/// `timestamp` to `timestamptz` (an `ALTER` in a New York session moves
/// every instant, and the rendering gains `+00`). The definitions that don't
/// key by the column keep applying. A resume pauses again, because the
/// target's key column still has the old type.
#[tokio::test]
async fn a_type_change_that_re_renders_the_keys_pauses_until_it_is_changed_back() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    raw.batch_execute(
        "create table public.readings (taken timestamp primary key, at_day timestamp, v int); \
         insert into public.readings values \
           ('2024-01-01 10:00', '2024-01-01', 1), ('2024-01-02 10:00', '2024-01-01', 2);",
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
        error.contains("from timestamp without time zone to timestamp with time zone"),
        "{error}"
    );
    assert!(error.contains("GROUP BY"), "{error}");
    assert_eq!(status(&raw, "by_taken").await, TransformStatus::Live);

    trellis
        .apply("RESUME TRANSFORM per_day")
        .await
        .expect("resume");
    capture_pass(&mut raw, &db.pool).await;
    paused_for(&trellis, "per_day", "public.readings", &["at_day"]).await;

    // The source key: a 1-1 target's key column is a copy of it.
    raw.batch_execute("alter table public.readings alter column taken type date")
        .await
        .expect("re-type the source key");
    capture_pass(&mut raw, &db.pool).await;
    let error = paused_for(&trellis, "by_taken", "public.readings", &["taken"]).await;
    assert!(
        error.contains("source key") && error.contains("date"),
        "{error}"
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
}

/// After only the from-side join column is widened (`varchar(8)` to
/// `varchar(32)`), which the pass deliberately doesn't refuse, a lookup casts
/// a from-side key to the to-side's `varchar(8)`, which truncates: the
/// dangling key `'annabelle9'` finds the to-row `'annabell'`. The match is
/// still dropped, because every lookup returns rows keyed by their own text
/// and the evaluator looks up the from-side key's. So it joins nothing, as
/// Postgres's own `=` would.
#[tokio::test]
async fn a_one_sided_join_widening_joins_no_key_by_its_prefix() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect(db.dsn()).await;
    let _trellis = setup(db.dsn(), &mut raw, &db.pool).await;

    raw.batch_execute(
        "alter table public.posts alter column author type varchar(32); \
         insert into public.users values ('annabell', 'Annabell');",
    )
    .await
    .expect("widen the from-side join column only");
    capture_pass(&mut raw, &db.pool).await;
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    raw.batch_execute(
        "insert into public.posts values (4, 'annabelle9', 'd', 1, 1.00), \
                                         (5, 'annabell', 'e', 1, 1.00);",
    )
    .await
    .expect("a dangling key whose prefix is a to-side key, and a real one");
    full_pass(&mut raw, &db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    raw.batch_execute("update public.users set name = 'Belle' where handle = 'annabell'")
        .await
        .expect("a to-side write");
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
             left join public.users u on u.handle = p.author order by p.id"
        )
        .await,
    );
}
