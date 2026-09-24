//! Regression tests for issue #126: a source table whose primary key spans
//! more than one column ("composite") must be able to drain through the
//! from-side relationship path, and through the general (non-relationship)
//! live-CDC apply path — both previously hard-rejected by
//! `defs::ddl::source_primary_key` with `DdlError::CompositePrimaryKeyUnsupported`
//! the moment such a table appeared as *any* live drain batch's source,
//! relationship or not.
//!
//! This file reuses issue #94's exact running example
//! (`defs_aggregate_relationship.rs`'s `post_tags`/`posts` schema and
//! `TAG_TOTALS` definition: `TRANSFORM tag_totals FROM post_tags GROUP BY tag
//! SELECT COUNT(*) AS post_count, SUM(post.word_count) AS total_words`), with
//! one change: `post_tags` here has **no surrogate `id` column at all** — its
//! primary key is the composite `(post, tag)` pair the issue's own report
//! names as the motivating shape, so every helper below (`stage_cdc`'s `key`
//! argument, the from-side relationship reverse path) has to build/consume a
//! genuine two-column key rather than a single-column stand-in.
//!
//! Four things this file proves that no other test file does:
//!
//! 1. `aggregate_over_a_to_one_relationship_with_composite_pk_backfills_to_the_oracle`
//!    — a definition install that falls back to the ring's enumeration path
//!    (unsupported by the direct/set-based builder because it reads a
//!    relationship) stages every pre-existing composite-PK row as an
//!    image-less `Recompute`, which the live drain must re-fetch in one
//!    *batched* query keyed by the composite identity
//!    (`staging::apply::read_live_rows_batch`) — this is the "outside a
//!    relationship" general apply path issue #126 flags as previously
//!    untested even for a plain source, now exercised end-to-end for a
//!    source that's *also* a relationship's from-side.
//! 2. `forward_insert_of_a_composite_pk_from_side_row_updates_its_group_total`
//!    — an ordinary forward CDC insert into the composite-PK from-side table
//!    drains correctly (mirrors `defs_aggregate_relationship.rs`'s
//!    `inserting_a_from_side_row_updates_its_groups_total`).
//! 3. `reverse_update_of_the_to_side_row_updates_every_dependent_group` — the
//!    headline case: changing the *related* `posts` row drains through
//!    `staging::apply::build_reverse_relationship_shape`'s settled-parent-
//!    projection reverse-delta path (epic #127, issues #130-#132), which
//!    introspects `post_tags`' own (composite) primary key to build the
//!    `Recompute`/live-refetch machinery that walks back from the changed
//!    parent to every matching from-side row — the literal "from-side
//!    relationship path" issue #126 is titled after.
//! 4. `plain_aggregate_over_a_composite_pk_source_drains_with_no_relationship_at_all`
//!    — the same composite-PK source, droppped into an aggregate definition
//!    that reads no relationship whatsoever, still drains a plain forward
//!    insert/update/delete sequence correctly — `staging::apply::compute`
//!    calls `ddl::source_primary_key` unconditionally for every source in a
//!    drain batch, relationship or not, so this combination needed its own
//!    coverage per issue #126's own report.
//!
//! The staging harness (connect, stage a CDC row by hand, seal/drain to
//! quiescence) mirrors `defs_aggregate_relationship.rs`/
//! `apply_relationship_reverse.rs`; see those files for the ring/seal
//! mechanics this one doesn't re-explain.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::{create_relationship, install_definition};
use trellis::staging::apply;
use trellis::staging::{has_pending, retire_drained_segments};

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'; set bytea_output to 'hex'"))
        .await
        .expect("set search_path");
    client
}

async fn seal_active_segment(client: &mut Client) -> i64 {
    use trellis::staging::seal;
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

async fn active_seg_table(client: &Client) -> String {
    let ring_slot: i16 = client
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment pointer")
        .get(0);
    format!("seg_{ring_slot}")
}

fn qualify_fixture_table(name: &str) -> String {
    if name.contains('.') {
        name.to_string()
    } else {
        format!("{DEFAULT_SCHEMA}.{name}")
    }
}

/// A composite primary-key identity string, U+001F-joined in the key's own
/// declared column order — the same encoding `intake::extract_key` (real
/// CDC) and `intake::publication::enumerate_and_append` (ring-based
/// backfill) both already produce, and the one `ddl::pk_key_sql_expr`/
/// `ddl::split_pk_key` on the apply side now agree with (issue #126). Every
/// `post_tags` fixture below declares its primary key `(post, tag)` in that
/// order, so this always joins `post` then `tag`.
fn composite_key(parts: &[&str]) -> String {
    parts.join("\u{1f}")
}

/// Stages one image-bearing (CDC-shaped) change into the active ring
/// segment — mirrors `defs_aggregate_relationship.rs`'s helper of the same
/// name.
async fn stage_cdc(
    client: &Client,
    src_table: &str,
    key: &str,
    op: &str,
    old_image: Option<&str>,
    new_image: Option<&str>,
) {
    let src_table = qualify_fixture_table(src_table);
    let table = active_seg_table(client).await;
    let lsn = PgLsn::from(1u64);
    client
        .execute(
            &format!(
                "insert into {table} (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                 values ($1, $2, $3, $4, $5::text::jsonb, $6::text::jsonb, 0)"
            ),
            &[&src_table, &key, &op, &lsn, &old_image, &new_image],
        )
        .await
        .unwrap_or_else(|e| panic!("stage cdc {key:?} into {table} failed: {e}"));
}

/// Seals and drains repeatedly until nothing is pending anywhere in the ring
/// — mirrors `defs_aggregate_relationship.rs`'s helper of the same name.
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    let watermark = trellis::staging::StagedWatermark::saturated();
    let watermark = &watermark;
    for _ in 0..16 {
        let seg = seal_active_segment(client).await;
        while apply::drain_once(
            pool,
            seg,
            "composite_pk_test",
            1,
            "trellis_composite_pk_test",
            watermark,
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
    panic!("pipeline did not reach quiescence within 16 seal/drain rounds");
}

fn columns(pairs: &[(&str, ValueType)]) -> HashMap<String, ValueType> {
    pairs
        .iter()
        .map(|(name, ty)| (name.to_string(), *ty))
        .collect()
}

fn post_tags_columns() -> HashMap<String, ValueType> {
    columns(&[("post", ValueType::Numeric), ("tag", ValueType::Text)])
}

const TAG_TOTALS: &str = "TRANSFORM tag_totals FROM post_tags GROUP BY tag \
     SELECT COUNT(*) AS post_count, SUM(post.word_count) AS total_words";

/// Issue #94's schema, minus its surrogate `id` column: `post_tags`' own
/// primary key is the composite `(post, tag)` pair — the shape issue #126's
/// report names explicitly (`post_tags (post, tag)`). `REPLICA IDENTITY
/// FULL` on both tables for the same reasons as
/// `defs_aggregate_relationship.rs`'s `create_schema`: `post_tags` is an
/// aggregate source (the delta/recompute path needs the old image to locate
/// a changed row's leaving group), and `posts` is the to-side of a to-one
/// relationship whose settled parent projection requires it unconditionally.
async fn create_schema(client: &Client) {
    client
        .batch_execute(
            "create table posts (id integer primary key, word_count integer); \
             create table post_tags (post integer, tag text, primary key (post, tag)); \
             alter table post_tags replica identity full; \
             alter table posts replica identity full; \
             insert into posts (id, word_count) values (1, 100), (2, 250), (3, null); \
             insert into post_tags (post, tag) values \
               (1, 'rust'), (2, 'rust'), (1, 'db'), \
               (999, 'rust'), (3, 'db')",
        )
        .await
        .expect("create + seed the composite-pk variant of issue #94's schema");
}

type Totals = HashMap<String, (Option<String>, Option<String>)>;

async fn target_totals(client: &Client) -> Totals {
    client
        .query(
            "select tag, post_count::text, total_words::text from tag_totals",
            &[],
        )
        .await
        .expect("read tag_totals")
        .into_iter()
        .map(|r| (r.get(0), (r.get(1), r.get(2))))
        .collect()
}

// ---------------------------------------------------------------------
// 1. Backfill over pre-existing composite-PK rows (the general apply path).
// ---------------------------------------------------------------------

/// Issue #94's headline case, replayed over a composite-PK `post_tags`: a
/// to-one relationship path folded by `SUM` in a `GROUP BY` definition
/// installs, and its backfill over pre-existing data (which falls back to
/// the ring's enumeration + live-refetch path, since the relationship read
/// makes the direct/set-based aggregate builder bail with
/// `BackfillError::Unsupported`) converges correctly — proving
/// `staging::apply::read_live_rows_batch`'s batched live re-fetch, and
/// `defs::ddl::source_primary_key`, both now handle a two-column key.
#[tokio::test]
async fn aggregate_over_a_to_one_relationship_with_composite_pk_backfills_to_the_oracle() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");

    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition against a composite-pk source");
    trellis::intake::publication::settle_registrations(&db.pool).await;

    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = target_totals(&client).await;
    // `rust` = 100 (post 1) + 250 (post 2) + NULL (post 999 doesn't exist);
    // `db` = 100 (post 1) + NULL (post 3's NULL word_count) — identical
    // arithmetic to `defs_aggregate_relationship.rs`'s surrogate-`id` version,
    // confirming the composite key changes nothing about the computed value.
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("3".to_string()), Some("350".to_string())))
    );
    assert_eq!(
        totals.get("db"),
        Some(&(Some("2".to_string()), Some("100".to_string())))
    );
}

// ---------------------------------------------------------------------
// 2. Forward propagation.
// ---------------------------------------------------------------------

/// Forward propagation: a new `post_tags` row — identified only by its
/// composite `(post, tag)` key, no surrogate column at all — folds its
/// related post's `word_count` into the right group.
#[tokio::test]
async fn forward_insert_of_a_composite_pk_from_side_row_updates_its_group_total() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    trellis::intake::publication::settle_registrations(&db.pool).await;
    drain_to_quiescence(&db.pool, &mut client).await;

    client
        .execute("insert into post_tags (post, tag) values (2, 'db')", &[])
        .await
        .expect("insert a new post_tag");
    stage_cdc(
        &client,
        "post_tags",
        &composite_key(&["2", "db"]),
        "insert",
        None,
        Some("{\"post\":2,\"tag\":\"db\"}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = target_totals(&client).await;
    // `db` gains post 2's 250 words and one more row: (2, 100+250=350).
    assert_eq!(
        totals.get("db"),
        Some(&(Some("3".to_string()), Some("350".to_string())))
    );
}

// ---------------------------------------------------------------------
// 3. Reverse propagation: the literal "from-side relationship path".
// ---------------------------------------------------------------------

/// Reverse propagation (issue #30's mechanism, epic #127's settled-parent-
/// projection delta rewrite): changing the *related* `posts` row's
/// `word_count` must re-derive every group whose members read it — post 1 is
/// referenced by both the `rust` and `db` groups (via `post_tags` rows keyed
/// only by their composite `(post, tag)` identity), so both move.
///
/// This is the scenario issue #126 is titled after:
/// `staging::apply::build_reverse_relationship_shape` introspects
/// `post_tags.from_table`'s own primary key (now a `Vec<PrimaryKeyColumn>` of
/// length 2, not the single `PrimaryKeyColumn` every other test file's
/// surrogate-PK `post_tags` produces) to build the reverse-delta machinery
/// that walks from the changed `posts` row back to every matching
/// `post_tags` row — before issue #126 this introspection itself failed
/// outright with `DdlError::CompositePrimaryKeyUnsupported` the moment
/// `post_tags` had no single-column key, independent of whether the reverse
/// delta's *fast* (aggregate) path or its image-less fallback ended up
/// handling any individual record.
#[tokio::test]
async fn reverse_update_of_the_to_side_row_updates_every_dependent_group() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    trellis::intake::publication::settle_registrations(&db.pool).await;
    drain_to_quiescence(&db.pool, &mut client).await;

    client
        .execute("update posts set word_count = 400 where id = 1", &[])
        .await
        .expect("update the related post");
    stage_cdc(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = target_totals(&client).await;
    // `rust`: (1,'rust') now contributes 400 instead of 100 -> 400+250=650.
    // `db`: (1,'db') now contributes 400 instead of 100 -> 400.
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("3".to_string()), Some("650".to_string())))
    );
    assert_eq!(
        totals.get("db"),
        Some(&(Some("2".to_string()), Some("400".to_string())))
    );
}

// ---------------------------------------------------------------------
// 4. A composite-PK source with no relationship at all.
// ---------------------------------------------------------------------

/// Issue #126's own report flags this combination as previously untested:
/// `staging::apply::compute` calls `ddl::source_primary_key` unconditionally
/// for *every* source table a drain batch touches, not only ones a
/// relationship's from-side path reads — so a composite-PK source feeding a
/// perfectly ordinary, relationship-free `KeySpace::Aggregate` definition
/// must also keep draining. `order_lines (order_id, line_no)` is composite
/// and carries no relationship of any kind.
#[tokio::test]
async fn plain_aggregate_over_a_composite_pk_source_drains_with_no_relationship_at_all() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_lines ( \
                 order_id integer, line_no integer, category text, amount numeric, \
                 primary key (order_id, line_no) \
             ); \
             alter table order_lines replica identity full; \
             insert into order_lines (order_id, line_no, category, amount) values \
               (1, 1, 'books', 10), (1, 2, 'books', 5), (2, 1, 'toys', 20)",
        )
        .await
        .expect("create + seed a composite-pk, relationship-free source");

    let source_columns = columns(&[
        ("order_id", ValueType::Numeric),
        ("line_no", ValueType::Numeric),
        ("category", ValueType::Text),
        ("amount", ValueType::Numeric),
    ]);
    install_definition(
        &db.pool,
        "TRANSFORM category_totals FROM order_lines GROUP BY category SELECT SUM(amount) AS total",
        &source_columns,
        "public",
    )
    .await
    .expect("install a plain aggregate over a composite-pk source");
    trellis::intake::publication::settle_registrations(&db.pool).await;
    drain_to_quiescence(&db.pool, &mut client).await;

    let read_totals = async |client: &Client| -> Totals {
        client
            .query(
                "select category, null::text, total::text from category_totals",
                &[],
            )
            .await
            .expect("read category_totals")
            .into_iter()
            .map(|r| (r.get(0), (r.get(1), r.get(2))))
            .collect()
    };

    let totals = read_totals(&client).await;
    assert_eq!(
        totals.get("books"),
        Some(&(None, Some("15".to_string()))),
        "backfill over pre-existing composite-pk rows"
    );
    assert_eq!(totals.get("toys"), Some(&(None, Some("20".to_string()))));

    // A forward insert (a genuinely new composite key, `(2, 2)`).
    client
        .execute(
            "insert into order_lines (order_id, line_no, category, amount) values (2, 2, 'toys', 7)",
            &[],
        )
        .await
        .expect("insert a new order line");
    stage_cdc(
        &client,
        "order_lines",
        &composite_key(&["2", "2"]),
        "insert",
        None,
        Some("{\"order_id\":2,\"line_no\":2,\"category\":\"toys\",\"amount\":7}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        read_totals(&client).await.get("toys"),
        Some(&(None, Some("27".to_string()))),
        "after a forward insert"
    );

    // An update that migrates a row from one group to another (`(1, 2)`
    // re-categorized from `books` to `toys`) — exercises the old/new-image
    // decode path (`compute`'s `needs_old_rows`), still keyed by the same
    // composite identity throughout.
    client
        .execute(
            "update order_lines set category = 'toys' where order_id = 1 and line_no = 2",
            &[],
        )
        .await
        .expect("re-categorize an order line");
    stage_cdc(
        &client,
        "order_lines",
        &composite_key(&["1", "2"]),
        "update",
        Some("{\"order_id\":1,\"line_no\":2,\"category\":\"books\",\"amount\":5}"),
        Some("{\"order_id\":1,\"line_no\":2,\"category\":\"toys\",\"amount\":5}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    let totals = read_totals(&client).await;
    assert_eq!(
        totals.get("books"),
        Some(&(None, Some("10".to_string()))),
        "after migrating a row out of books"
    );
    assert_eq!(
        totals.get("toys"),
        Some(&(None, Some("32".to_string()))),
        "after migrating a row into toys (20 + 7 + 5)"
    );

    // A delete (exercises the composite-pk delete/old-image path too).
    client
        .execute(
            "delete from order_lines where order_id = 2 and line_no = 1",
            &[],
        )
        .await
        .expect("delete an order line");
    stage_cdc(
        &client,
        "order_lines",
        &composite_key(&["2", "1"]),
        "delete",
        Some("{\"order_id\":2,\"line_no\":1,\"category\":\"toys\",\"amount\":20}"),
        None,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        read_totals(&client).await.get("toys"),
        Some(&(None, Some("12".to_string()))),
        "after deleting a row (32 - 20)"
    );
}
