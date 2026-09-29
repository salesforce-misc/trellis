//! Issue #677's review: an `UPDATE` that leaves an out-of-line TOASTed
//! column unchanged, through real intake.
//!
//! `pgoutput` doesn't resend such a column in the new tuple. Before intake
//! filled it in from the old tuple, the new image simply lacked it, so:
//! - a 1:1 field reading a parent column through a to-one relationship went
//!   silently stale (the reverse path read the absent column as `NULL`);
//! - once #677 made relationship reads strict, an aggregate over the same
//!   relationship, which the live-row check used to repair, and a to-many
//!   reverse recompute keyed by the TOASTed `to_col`, both failed the key
//!   with `MissingColumn` instead.
//!
//! Every table here is `REPLICA IDENTITY FULL`, which relationships and
//! aggregates require, so the old tuple always has the value.

use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::{Config, Trellis, TrellisOptions};

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("set search_path to public")
        .await
        .expect("set search_path");
    client
}

/// A 2240-byte text value unique to `n`. Its column is `STORAGE EXTERNAL`,
/// so it is stored out of line, uncompressed.
fn big(n: i32) -> String {
    format!("(select string_agg(md5('{n}' || g::text), '') from generate_series(1, 70) g)")
}

/// `target` and `oracle` as the count of rows in one and not the other.
async fn symmetric_difference(raw: &Client, target: &str, oracle: &str) -> i64 {
    raw.query_one(
        &format!(
            "with expected as ({oracle}), actual as ({target}) \
             select (select count(*) from (table expected except table actual) m) \
                  + (select count(*) from (table actual except table expected) x)"
        ),
        &[],
    )
    .await
    .unwrap_or_else(|e| panic!("difference query for {target}: {e}"))
    .get(0)
}

#[tokio::test]
async fn an_unchanged_toasted_column_is_read_from_the_old_image() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(
        "create table posts (id bigint primary key, author text, word_count int); \
         alter table posts alter column author set storage external; \
         alter table posts replica identity full; \
         create table post_tags (id bigint primary key, post_id bigint, tag text, weight int); \
         alter table post_tags replica identity full; \
         create table articles (id bigint primary key, ref text unique, title text); \
         alter table articles alter column ref set storage external; \
         alter table articles replica identity full; \
         create table comments (id bigint primary key, article_ref text, word_count int); \
         alter table comments alter column article_ref set storage external; \
         alter table comments replica identity full",
    )
    .await
    .expect("source tables");
    raw.batch_execute(&format!(
        "insert into posts values (1, {a}, 100), (2, 'bob', 200); \
         insert into post_tags values (10, 1, 'x', 1), (11, 1, 'y', 2), (12, 2, 'x', 3); \
         insert into articles values (1, {r}, 'a1'), (2, 'short', 'a2'); \
         insert into comments values (100, {r}, 5), (101, {r}, 6), (102, 'short', 7)",
        a = big(1),
        r = big(2),
    ))
    .await
    .expect("seed");

    let trellis = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions {
            staging: true,
            drain_threads: 2,
            ..Default::default()
        },
    )
    .await
    .expect("connect running trellis");
    for statement in [
        "RELATIONSHIP post FROM post_tags.post_id TO posts.id",
        "TRANSFORM by_author FROM post_tags GROUP BY post.author SELECT COUNT(*) AS n, SUM(weight) AS w",
        "TRANSFORM by_tag FROM post_tags GROUP BY tag SELECT tag AS tag, SUM(post.word_count) AS words",
        "TRANSFORM tag_view FROM post_tags SELECT post.author AS author, post.word_count AS wc",
        "RELATIONSHIP comments FROM articles.ref TO comments.article_ref",
        "TRANSFORM article_words FROM articles SELECT sum(comments.word_count) AS total_words",
    ] {
        trellis
            .apply(statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let not_live: i64 = raw
            .query_one(
                "select count(*) from trellis.transform_definitions where status <> 'live'",
                &[],
            )
            .await
            .expect("read transform status")
            .get(0);
        if not_live == 0 {
            break;
        }
        assert!(Instant::now() < deadline, "the definitions never went live");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Each UPDATE leaves the TOASTed column (`posts.author`, and the to-many
    // join key `comments.article_ref`) unchanged.
    raw.batch_execute(
        "update posts set word_count = word_count + 7 where id = 1; \
         update comments set word_count = word_count + 10 where id = 100",
    )
    .await
    .expect("updates");
    let token = trellis.watermark_token().await.expect("watermark_token");
    trellis
        .await_converged(token, Duration::from_secs(60))
        .await
        .expect("the updates must converge, not poison their keys");

    let poisoned: Vec<String> = raw
        .query(
            "select src_table || ': ' || last_error from trellis.poison",
            &[],
        )
        .await
        .expect("read poison")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert!(poisoned.is_empty(), "{poisoned:?}");

    for (target, oracle) in [
        (
            "select md5(author), n::bigint, w::numeric from by_author",
            "select md5(p.author), count(*), sum(t.weight)::numeric \
             from post_tags t left join posts p on p.id = t.post_id group by p.author",
        ),
        (
            "select tag, words::numeric from by_tag",
            "select t.tag, sum(p.word_count)::numeric \
             from post_tags t left join posts p on p.id = t.post_id group by t.tag",
        ),
        (
            "select id, md5(author), wc::numeric from tag_view",
            "select t.id, md5(p.author), p.word_count::numeric \
             from post_tags t left join posts p on p.id = t.post_id",
        ),
        (
            "select id, total_words::numeric from article_words",
            "select a.id, (select sum(c.word_count) from comments c \
                           where c.article_ref = a.ref)::numeric \
             from articles a",
        ),
    ] {
        assert_eq!(
            symmetric_difference(&raw, target, oracle).await,
            0,
            "{target} must equal its SQL oracle"
        );
    }
    trellis.shutdown().await.expect("shutdown");
}
