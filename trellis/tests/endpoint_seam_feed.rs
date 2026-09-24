//! Issues #402/#403 (#375's direction 1): the target-mutation seam is the
//! only change feed for a target that is a relationship endpoint, standing in
//! for the CDC such a target no longer gets (endpoint targets are never
//! published). No test here runs intake at all: the seam alone has to drive
//! the relationship machinery.
//!
//! Drains run by hand (seal, drain, retire), so nothing waits on convergence
//! timing (#297).

use std::collections::{BTreeMap, HashMap};

use testkit::{TestCluster, TestDatabase};
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::{
    RelationshipDefinition, create_definition, create_relationship, install_definition,
    relationship_projection,
};
use trellis::integer::IntWidth;
use trellis::staging::{
    CdcOp, StagedChange, StagedWatermark, TargetMutations, append, apply, has_pending,
    retire_drained_segments,
};

const WAKE: &str = "endpoint_seam_wake";

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'"
        ))
        .await
        .expect("set search_path");
    client
}

async fn setup() -> (TestCluster, TestDatabase, Client) {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    (cluster, db, raw)
}

fn int_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Integer(IntWidth::Int4)))
        .collect()
}

/// Seals the active segment and drains everything claimable, once.
async fn drain_round(pool: &trellis::Pool, client: &mut Client) {
    let watermark = StagedWatermark::saturated();
    trellis::staging::seal_if_active_nonempty(client, WAKE)
        .await
        .expect("seal");
    while let Some(seg) = apply::next_claimable_segment(&*client)
        .await
        .expect("next claimable segment")
    {
        apply::drain_once(pool, seg, "endpoint_seam_test", 1, WAKE, &watermark)
            .await
            .expect("drain_once");
    }
    retire_drained_segments(client)
        .await
        .expect("retire drained segments");
}

/// Seals and drains until nothing is pending, one batch per round.
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    for _ in 0..16 {
        drain_round(pool, client).await;
        if !has_pending(client).await.expect("has_pending") {
            return;
        }
    }
    panic!("pipeline did not reach quiescence within 16 seal/drain rounds");
}

/// One ring row as staged, for the assertions on the seam's row shape.
#[derive(Debug)]
struct RingRow {
    op: String,
    lsn: Option<PgLsn>,
    old_image: Option<String>,
    new_image: Option<String>,
    group_key: Option<Vec<String>>,
}

/// Every row staged for `src_table` in the active (not yet sealed) segment,
/// in append order.
async fn staged_rows(raw: &Client, src_table: &str) -> Vec<RingRow> {
    let slot: i16 = raw
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment_pointer")
        .get(0);
    raw.query(
        &format!(
            "select op, lsn, old_image::text, new_image::text, group_key \
             from seg_{slot} where src_table = $1 order by change_id"
        ),
        &[&src_table],
    )
    .await
    .expect("read the ring")
    .into_iter()
    .map(|row| RingRow {
        op: row.get(0),
        lsn: row.get(1),
        old_image: row.get(2),
        new_image: row.get(3),
        group_key: row.get(4),
    })
    .collect()
}

async fn rows(raw: &Client, sql: &str) -> BTreeMap<String, String> {
    raw.query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

async fn projection_table(pool: &trellis::Pool, relationship: &RelationshipDefinition) -> String {
    relationship_projection(pool, relationship.id)
        .await
        .expect("read the projection catalog row")
        .expect("a to-one relationship has a projection")
        .projection_table
}

/// A hand-staged source change, standing in for intake.
async fn stage_source_update(raw: &mut Client, src_table: &str, key: &str, old: &str, new: &str) {
    let txn = raw.transaction().await.expect("begin");
    append(
        &txn,
        &[StagedChange::Cdc {
            src_table: src_table.to_string(),
            key: key.to_string(),
            op: CdcOp::Update,
            lsn: Some(PgLsn::from(1)),
            old_image: Some(old.to_string()),
            new_image: Some(new.to_string()),
            origin_lsn: None,
            src_changed: None,
            hop_gen: 0,
            group_key: None,
        }],
    )
    .await
    .expect("stage the source change");
    txn.commit().await.expect("commit");
}

/// A 1-1 target that is a to-one relationship's to-side: a write the drain
/// makes to it reaches the ring as an image-bearing update carrying the
/// writer's token as its `lsn`, and that row alone advances the settled
/// parent projection (to that token) and the definition reading through the
/// relationship.
#[tokio::test]
async fn a_seam_row_on_a_target_to_side_advances_its_to_one_projection() {
    let (_cluster, db, mut raw) = setup().await;
    raw.batch_execute(
        "create table public.src (id integer primary key, v integer); \
         create table public.t (id integer primary key, doubled integer); \
         create table public.report_view (id integer primary key, doubled integer); \
         insert into public.src values (1, 5), (2, 7); \
         create table public.reports (id integer primary key, oid integer); \
         insert into public.reports values (10, 1), (11, 2); \
         alter table public.src replica identity full; \
         alter table public.reports replica identity full",
    )
    .await
    .expect("create sources");
    create_definition(
        &db.pool,
        "TRANSFORM public.t FROM public.src SELECT v + v AS doubled",
        &int_columns(&["id", "v"]),
    )
    .await
    .expect("install t");
    drain_to_quiescence(&db.pool, &mut raw).await;
    let relationship =
        create_relationship(&db.pool, "RELATIONSHIP rollup FROM reports.oid TO t.id")
            .await
            .expect("a relationship whose to-side is a target");
    create_definition(
        &db.pool,
        "TRANSFORM public.report_view FROM public.reports SELECT rollup.doubled AS doubled",
        &int_columns(&["id", "oid"]),
    )
    .await
    .expect("install a reader through the relationship");
    drain_to_quiescence(&db.pool, &mut raw).await;
    let projection = projection_table(&db.pool, &relationship).await;

    raw.execute("update public.src set v = 50 where id = 1", &[])
        .await
        .expect("update the source");
    stage_source_update(
        &mut raw,
        "public.src",
        "1",
        r#"{"id":"1","v":"5"}"#,
        r#"{"id":"1","v":"50"}"#,
    )
    .await;
    drain_round(&db.pool, &mut raw).await;

    let staged = staged_rows(&raw, "public.t").await;
    assert_eq!(
        staged.len(),
        1,
        "one seam row for t's one changed key: {staged:?}"
    );
    let row = &staged[0];
    assert_eq!(
        row.op, "update",
        "an endpoint target's seam row is CDC-shaped"
    );
    assert_eq!(
        row.old_image.as_deref(),
        Some(r#"{"id": "1", "doubled": "10"}"#)
    );
    assert_eq!(
        row.new_image.as_deref(),
        Some(r#"{"id": "1", "doubled": "100"}"#)
    );
    assert_eq!(row.group_key, None, "t is no relationship's from-side");
    let token = row
        .lsn
        .expect("an image-bearing seam row carries the write token");

    drain_to_quiescence(&db.pool, &mut raw).await;
    let (doubled, lsn): (String, Option<PgLsn>) = {
        let row = raw
            .query_one(
                &format!("select doubled::text, __trellis_lsn from {projection} where id = 1"),
                &[],
            )
            .await
            .expect("read parent 1's projection row");
        (row.get(0), row.get(1))
    };
    assert_eq!(
        (doubled, lsn),
        ("100".to_string(), Some(token)),
        "the seam row alone advanced parent 1's projection row, to its token"
    );
    assert_eq!(
        rows(
            &raw,
            "select id::text, doubled::text from public.report_view"
        )
        .await,
        BTreeMap::from([
            ("10".to_string(), "100".to_string()),
            ("11".to_string(), "14".to_string()),
        ]),
    );
}

/// An aggregate reading a to-one relationship whose to-side is a 1-1 target:
/// a write to that target reaches the aggregate as a reverse *delta*, the way
/// the to-side's CDC would, not as a forced re-derive. The group is offset by
/// hand first, and a delta keeps the offset where a re-derive would erase it.
#[tokio::test]
async fn a_seam_row_on_a_target_to_side_drives_an_aggregates_reverse_delta() {
    let (_cluster, db, mut raw) = setup().await;
    raw.batch_execute(
        "create table public.posts_src (id integer primary key, word_count integer); \
         create table public.posts (id integer primary key, word_count integer); \
         insert into public.posts_src values (1, 100), (2, 250); \
         create table public.post_tags (id integer primary key, post integer, tag text); \
         create index on public.post_tags (post); \
         insert into public.post_tags values (10, 1, 'rust'), (11, 2, 'rust'), (12, 1, 'db'); \
         alter table public.posts_src replica identity full; \
         alter table public.post_tags replica identity full",
    )
    .await
    .expect("create sources");
    create_definition(
        &db.pool,
        "TRANSFORM public.posts FROM public.posts_src SELECT word_count AS word_count",
        &int_columns(&["id", "word_count"]),
    )
    .await
    .expect("install posts");
    drain_to_quiescence(&db.pool, &mut raw).await;
    create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("a relationship whose to-side is a target");
    let mut tag_columns = int_columns(&["id", "post"]);
    tag_columns.insert("tag".to_string(), ValueType::Text);
    install_definition(
        &db.pool,
        "TRANSFORM public.tag_totals FROM public.post_tags GROUP BY tag \
         SELECT SUM(post.word_count) AS total_words",
        &tag_columns,
        "public",
    )
    .await
    .expect("install tag_totals");
    trellis::intake::publication::settle_registrations(&db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        rows(&raw, "select tag, total_words::text from public.tag_totals").await,
        BTreeMap::from([
            ("db".to_string(), "100".to_string()),
            ("rust".to_string(), "350".to_string()),
        ]),
    );

    raw.execute(
        "update public.tag_totals set total_words = total_words + 1000",
        &[],
    )
    .await
    .expect("offset every group");
    raw.execute(
        "update public.posts_src set word_count = 400 where id = 1",
        &[],
    )
    .await
    .expect("update the source");
    stage_source_update(
        &mut raw,
        "public.posts_src",
        "1",
        r#"{"id":"1","word_count":"100"}"#,
        r#"{"id":"1","word_count":"400"}"#,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut raw).await;

    assert_eq!(
        rows(&raw, "select tag, total_words::text from public.tag_totals").await,
        BTreeMap::from([
            ("db".to_string(), "1400".to_string()),
            ("rust".to_string(), "1650".to_string()),
        ]),
        "posts' seam row applied +300 to both of post 1's groups as a delta"
    );
}

/// A 1-1 target that is a to-one relationship's from-side: its seam rows
/// carry `group_key`, the join-key values their images touched, so a child
/// inserted pointing at parent 3 and re-pointed to parent 2 in the same
/// batch still bumps parent 3's projection `gen` (issue #133), although the
/// folded images name only parent 2. Also pins the other two ops' shapes: a
/// delete stages its prior image alone, and a key born and deleted in one
/// transaction stages nothing.
#[tokio::test]
async fn a_from_side_targets_seam_group_key_bumps_an_erased_parents_gen() {
    let (_cluster, db, mut raw) = setup().await;
    raw.batch_execute(
        "create table public.children_src (id integer primary key, parent_id integer); \
         create table public.children (id integer primary key, parent_id integer); \
         create table public.child_view (id integer primary key, pname text); \
         insert into public.children_src values (100, 1); \
         create table public.parents (id integer primary key, name text); \
         insert into public.parents values (1, 'A'), (2, 'B'), (3, 'C'); \
         alter table public.children_src replica identity full; \
         alter table public.parents replica identity full",
    )
    .await
    .expect("create sources");
    create_definition(
        &db.pool,
        "TRANSFORM public.children FROM public.children_src SELECT parent_id AS parent_id",
        &int_columns(&["id", "parent_id"]),
    )
    .await
    .expect("install children");
    drain_to_quiescence(&db.pool, &mut raw).await;
    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP parent FROM children.parent_id TO parents.id",
    )
    .await
    .expect("a relationship whose from-side is a target");
    create_definition(
        &db.pool,
        "TRANSFORM public.child_view FROM public.children SELECT parent.name AS pname",
        &int_columns(&["id", "parent_id"]),
    )
    .await
    .expect("install a reader through the relationship");
    drain_to_quiescence(&db.pool, &mut raw).await;
    let projection = projection_table(&db.pool, &relationship).await;
    let gens = format!("select id::text, __trellis_gen::text from {projection}");
    let before = rows(&raw, &gens).await;

    // Two writer transactions through the seam, standing in for two drains
    // that each wrote `children`: insert child 200 under parent 3, then
    // re-point it to parent 2. Both rows land in the one active segment.
    seam_write(
        &mut raw,
        "200",
        false,
        &["insert into public.children values (200, 3)"],
    )
    .await;
    seam_write(
        &mut raw,
        "200",
        true,
        &["update public.children set parent_id = 2 where id = 200"],
    )
    .await;

    let staged = staged_rows(&raw, "public.children").await;
    let shape: Vec<(&str, Option<&[String]>)> = staged
        .iter()
        .map(|r| (r.op.as_str(), r.group_key.as_deref()))
        .collect();
    assert_eq!(
        shape,
        vec![
            ("insert", Some(&["3".to_string()][..])),
            ("update", Some(&["2".to_string(), "3".to_string()][..])),
        ],
        "each seam row's group_key is the parent ids its images touched"
    );
    assert!(
        staged[0].lsn.expect("token") < staged[1].lsn.expect("token"),
        "the second writer's token orders after the first's: {staged:?}"
    );

    drain_round(&db.pool, &mut raw).await;
    let after = rows(&raw, &gens).await;
    let bumped = |id: &str| after[id].parse::<i64>().unwrap() - before[id].parse::<i64>().unwrap();
    assert_eq!(
        (bumped("1"), bumped("2"), bumped("3")),
        (0, 1, 1),
        "parent 3 is named only by the seam rows' group_key, and its gen still bumps"
    );
    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        rows(&raw, "select id::text, pname from public.child_view").await,
        BTreeMap::from([
            ("100".to_string(), "A".to_string()),
            ("200".to_string(), "B".to_string()),
        ]),
    );

    // A delete stages the prior image alone, its group_key from that image.
    // A key created and deleted in one transaction changed nothing anyone
    // saw, and stages nothing.
    seam_write(
        &mut raw,
        "200",
        true,
        &["delete from public.children where id = 200"],
    )
    .await;
    seam_write(
        &mut raw,
        "300",
        false,
        &[
            "insert into public.children values (300, 1)",
            "delete from public.children where id = 300",
        ],
    )
    .await;
    let staged = staged_rows(&raw, "public.children").await;
    let shape: Vec<(&str, bool, bool, Option<&[String]>)> = staged
        .iter()
        .map(|r| {
            (
                r.op.as_str(),
                r.old_image.is_some(),
                r.new_image.is_some(),
                r.group_key.as_deref(),
            )
        })
        .collect();
    assert_eq!(
        shape,
        vec![("delete", true, false, Some(&["2".to_string()][..]))],
        "{staged:?}"
    );
}

/// One writer transaction on `public.children` through the seam, standing in
/// for a drain that wrote it: optionally pre-locks `key` and captures its
/// prior image (as every real writer does for a row it is about to change),
/// runs `writes`, and records `key` once.
async fn seam_write(raw: &mut Client, key: &str, lock_prior: bool, writes: &[&str]) {
    let txn = raw.transaction().await.expect("begin a writer");
    let mut mutations = TargetMutations::new();
    let image = mutations
        .image_sql(&txn, "public.children", "t")
        .await
        .expect("image_sql")
        .expect("the seam feeds an endpoint target, so it captures images");
    let prior_image = if lock_prior {
        Some(
            txn.query_one(
                &format!(
                    "select ({image})::text from public.children t \
                     where id = {key}::integer for update"
                ),
                &[],
            )
            .await
            .expect("pre-lock the key")
            .get::<_, String>(0),
        )
    } else {
        None
    };
    for write in writes {
        txn.execute(*write, &[]).await.expect("write children");
    }
    mutations.record("public.children", key.to_string(), prior_image, 0, None);
    mutations.flush(&txn).await.expect("flush the seam");
    txn.commit().await.expect("commit the writer");
}

/// A hand-staged source insert, standing in for intake.
async fn stage_source_insert(raw: &mut Client, src_table: &str, key: &str, new: &str) {
    let txn = raw.transaction().await.expect("begin");
    append(
        &txn,
        &[StagedChange::Cdc {
            src_table: src_table.to_string(),
            key: key.to_string(),
            op: CdcOp::Insert,
            lsn: Some(PgLsn::from(1)),
            old_image: None,
            new_image: Some(new.to_string()),
            origin_lsn: None,
            src_changed: None,
            hop_gen: 0,
            group_key: None,
        }],
    )
    .await
    .expect("stage the source change");
    txn.commit().await.expect("commit");
}

/// An aggregate target as a to-one relationship's to-side (issue #403 lifted
/// #400's refusal): its seam rows are CDC-shaped like a 1-1 target's, the
/// NULL group included, whose row the seam has to re-read NULL-safely (a
/// plain `=` would find no row and stage the update as a delete). Those rows
/// alone carry a new group and a changed one through the projection to a
/// definition reading through the relationship.
#[tokio::test]
async fn an_aggregate_target_endpoint_is_fed_by_the_seam_null_group_included() {
    let (_cluster, db, mut raw) = setup().await;
    raw.batch_execute(
        "create table public.sales (id integer primary key, region integer, amount integer); \
         insert into public.sales values (1, 1, 10), (2, null, 5); \
         create table public.stores (id integer primary key, region integer); \
         create table public.store_view (id integer primary key, total integer); \
         insert into public.stores values (100, 1), (101, null), (102, 2); \
         alter table public.sales replica identity full; \
         alter table public.stores replica identity full",
    )
    .await
    .expect("create sources");
    install_definition(
        &db.pool,
        "TRANSFORM public.region_totals FROM public.sales GROUP BY region \
         SELECT SUM(amount) AS total",
        &int_columns(&["id", "region", "amount"]),
        "public",
    )
    .await
    .expect("install region_totals");
    trellis::intake::publication::settle_registrations(&db.pool).await;
    drain_to_quiescence(&db.pool, &mut raw).await;
    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP totals FROM stores.region TO region_totals.region",
    )
    .await
    .expect("a relationship whose to-side is an aggregate target");
    create_definition(
        &db.pool,
        "TRANSFORM public.store_view FROM public.stores SELECT totals.total AS total",
        &int_columns(&["id", "region"]),
    )
    .await
    .expect("install a reader through the relationship");
    drain_to_quiescence(&db.pool, &mut raw).await;
    projection_table(&db.pool, &relationship).await;
    let view = "select id::text, coalesce(total::text, 'null') from public.store_view";
    assert_eq!(
        rows(&raw, view).await,
        BTreeMap::from([
            ("100".to_string(), "10".to_string()),
            ("101".to_string(), "null".to_string()),
            ("102".to_string(), "null".to_string()),
        ]),
    );

    // A new group 2, and a change to the NULL group.
    raw.batch_execute(
        "insert into public.sales values (3, 2, 7); \
         update public.sales set amount = 8 where id = 2",
    )
    .await
    .expect("write the source");
    stage_source_insert(
        &mut raw,
        "public.sales",
        "3",
        r#"{"id":"3","region":"2","amount":"7"}"#,
    )
    .await;
    stage_source_update(
        &mut raw,
        "public.sales",
        "2",
        r#"{"id":"2","region":null,"amount":"5"}"#,
        r#"{"id":"2","region":null,"amount":"8"}"#,
    )
    .await;
    drain_round(&db.pool, &mut raw).await;

    let staged = staged_rows(&raw, "public.region_totals").await;
    // The hidden recompute horizon is a WAL position the build and the
    // forced re-derivations stamp (#321, #419), not part of what this pins.
    let horizon =
        regex::Regex::new(r#""__trellis_recompute_lsn": ("[^"]*"|null)"#).expect("valid regex");
    let image = |image: &Option<String>| {
        image.as_deref().map(|image| {
            horizon
                .replace(image, r#""__trellis_recompute_lsn": <lsn>"#)
                .into_owned()
        })
    };
    let mut shape: Vec<(&str, Option<String>, Option<String>)> = staged
        .iter()
        .map(|r| (r.op.as_str(), image(&r.old_image), image(&r.new_image)))
        .collect();
    shape.sort();
    let expected = |text: &str| Some(text.to_string());
    assert_eq!(
        shape,
        vec![
            (
                "insert",
                None,
                expected(
                    r#"{"total": "7", "region": "2", "__total_count": "1", "__trellis_recompute_lsn": <lsn>}"#
                )
            ),
            (
                "update",
                expected(
                    r#"{"total": "5", "region": null, "__total_count": "1", "__trellis_recompute_lsn": <lsn>}"#
                ),
                expected(
                    r#"{"total": "8", "region": null, "__total_count": "1", "__trellis_recompute_lsn": <lsn>}"#
                )
            ),
        ],
        "{staged:?}"
    );
    assert!(
        staged.iter().all(|r| r.lsn.is_some()),
        "every image-bearing seam row carries the token: {staged:?}"
    );

    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        rows(&raw, view).await,
        BTreeMap::from([
            ("100".to_string(), "10".to_string()),
            ("101".to_string(), "null".to_string()),
            ("102".to_string(), "7".to_string()),
        ]),
        "a NULL join key never matches, and group 2's insert reached store 102"
    );
}

/// A source `TRUNCATE` clears a 1-1 target that is a to-one relationship's
/// to-side. The clear goes through the seam like any other write, so the
/// endpoint's readers see no key-less sentinel for it: each cleared key is a
/// CDC-shaped delete carrying its prior image, and the reader through the
/// relationship loses every enrichment.
#[tokio::test]
async fn a_truncate_clear_of_an_endpoint_target_stages_per_key_deletes() {
    let (_cluster, db, mut raw) = setup().await;
    raw.batch_execute(
        "create table public.src (id integer primary key, v integer); \
         create table public.t (id integer primary key, doubled integer); \
         create table public.report_view (id integer primary key, doubled integer); \
         insert into public.src values (1, 5), (2, 7); \
         create table public.reports (id integer primary key, oid integer); \
         insert into public.reports values (10, 1), (11, 2); \
         alter table public.src replica identity full; \
         alter table public.reports replica identity full",
    )
    .await
    .expect("create sources");
    create_definition(
        &db.pool,
        "TRANSFORM public.t FROM public.src SELECT v + v AS doubled",
        &int_columns(&["id", "v"]),
    )
    .await
    .expect("install t");
    drain_to_quiescence(&db.pool, &mut raw).await;
    create_relationship(&db.pool, "RELATIONSHIP rollup FROM reports.oid TO t.id")
        .await
        .expect("a relationship whose to-side is a target");
    create_definition(
        &db.pool,
        "TRANSFORM public.report_view FROM public.reports SELECT rollup.doubled AS doubled",
        &int_columns(&["id", "oid"]),
    )
    .await
    .expect("install a reader through the relationship");
    drain_to_quiescence(&db.pool, &mut raw).await;

    raw.execute("truncate public.src", &[])
        .await
        .expect("truncate the source");
    let txn = raw.transaction().await.expect("begin");
    append(
        &txn,
        &[StagedChange::Truncate {
            src_table: "public.src".to_string(),
            lsn: Some(PgLsn::from(1)),
            origin_lsn: None,
            src_changed: None,
        }],
    )
    .await
    .expect("stage the truncate");
    txn.commit().await.expect("commit");
    drain_round(&db.pool, &mut raw).await;

    let staged = staged_rows(&raw, "public.t").await;
    let shape: Vec<(&str, Option<&str>, bool)> = staged
        .iter()
        .map(|r| (r.op.as_str(), r.old_image.as_deref(), r.new_image.is_some()))
        .collect();
    assert_eq!(
        shape,
        vec![
            ("delete", Some(r#"{"id": "1", "doubled": "10"}"#), false),
            ("delete", Some(r#"{"id": "2", "doubled": "14"}"#), false),
        ],
        "{staged:?}"
    );

    drain_to_quiescence(&db.pool, &mut raw).await;
    assert_eq!(
        rows(
            &raw,
            "select id::text, coalesce(doubled::text, 'null') from public.report_view"
        )
        .await,
        BTreeMap::from([
            ("10".to_string(), "null".to_string()),
            ("11".to_string(), "null".to_string()),
        ]),
    );
}

/// A 1-1 target that is the from-side of two relationships: each seam row's
/// `group_key` unions both relationships' `from_col` values, across both
/// images, as intake's `touched_group_key` does for a decoded change.
#[tokio::test]
async fn a_from_side_target_of_two_relationships_unions_both_join_keys() {
    let (_cluster, db, mut raw) = setup().await;
    raw.batch_execute(
        "create table public.items_src (id integer primary key, parent_id integer, \
                                        owner_id integer); \
         create table public.items (id integer primary key, parent_id integer, \
                                    owner_id integer); \
         insert into public.items_src values (1, 10, 20); \
         create table public.parents (id integer primary key); \
         create table public.owners (id integer primary key); \
         alter table public.items_src replica identity full; \
         alter table public.parents replica identity full; \
         alter table public.owners replica identity full",
    )
    .await
    .expect("create sources");
    create_definition(
        &db.pool,
        "TRANSFORM public.items FROM public.items_src \
         SELECT parent_id AS parent_id, owner_id AS owner_id",
        &int_columns(&["id", "parent_id", "owner_id"]),
    )
    .await
    .expect("install items");
    drain_to_quiescence(&db.pool, &mut raw).await;
    for text in [
        "RELATIONSHIP parent FROM items.parent_id TO parents.id",
        "RELATIONSHIP owner FROM items.owner_id TO owners.id",
    ] {
        create_relationship(&db.pool, text)
            .await
            .unwrap_or_else(|e| panic!("{text}: {e}"));
    }

    raw.execute(
        "update public.items_src set parent_id = 11, owner_id = 21 where id = 1",
        &[],
    )
    .await
    .expect("update the source");
    stage_source_update(
        &mut raw,
        "public.items_src",
        "1",
        r#"{"id":"1","parent_id":"10","owner_id":"20"}"#,
        r#"{"id":"1","parent_id":"11","owner_id":"21"}"#,
    )
    .await;
    drain_round(&db.pool, &mut raw).await;

    let staged = staged_rows(&raw, "public.items").await;
    assert_eq!(staged.len(), 1, "{staged:?}");
    assert_eq!(
        staged[0].group_key.as_deref(),
        Some(
            &[
                "10".to_string(),
                "11".to_string(),
                "20".to_string(),
                "21".to_string()
            ][..]
        ),
        "{staged:?}"
    );
}
