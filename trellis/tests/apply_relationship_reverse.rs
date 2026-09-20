//! Integration tests for issue #131 (epic #127): the to-one relationship
//! reverse **delta** — a parent-keyed record carrying the parent's old/new
//! image and a `(prev_lsn, lsn)` chain, applied as paired subtract-old/
//! add-new over the parent's from-side rows, replacing the pre-#131
//! image-less from-side `Recompute` for to-one relationships specifically.
//! See `trellis/tests/spikes/issue-102-PLAN-DRAFT.md` §2 and §7 Phase 1
//! steps 4-5, and `staging::apply`'s own module doc comment (the "Issue
//! #131, epic #127" section) for the mechanism.
//!
//! Reuses issue #94's `defs_aggregate_relationship.rs` schema/fixture
//! (`post_tags` from-side, `posts` to-side, `SUM`/`COUNT` grouped by `tag`)
//! since it's already exactly the fully-invertible, single-relationship
//! shape this issue's fast path targets, and its dangling reference (a
//! `post_tags` row pointing at `post = 999`, which doesn't exist in the seed
//! data) doubles as a ready-made parent-insert fixture.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::{Expr, FieldDef, KeySpace, Predicate, TransformDef, ValueType};
use trellis::defs::{
    create_definition, create_relationship, create_target_table, install_definition,
    relationship_projection, require_single_column_pk, source_primary_key,
};
use trellis::staging::apply::{self, ApplyPlan};
use trellis::staging::{StagedWatermark, claim, fold, has_pending, retire_drained_segments};

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
    seal::seal_phase2(client, outcome.sealed_seg_seq)
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

/// Stages one image-bearing (CDC-shaped) change into the active ring
/// segment, at an explicit `lsn` — unlike most of this crate's other test
/// files' `stage_cdc` helpers, which pin every row to `lsn = 1`, several
/// tests here need distinct, ordered LSNs (the fold's "latest wins" rule,
/// and the reverse record's own `lsn`/`prev_lsn` chain, both key off it).
async fn stage_cdc_at_lsn(
    client: &Client,
    src_table: &str,
    key: &str,
    op: &str,
    old_image: Option<&str>,
    new_image: Option<&str>,
    lsn: u64,
) {
    let src_table = qualify_fixture_table(src_table);
    let table = active_seg_table(client).await;
    let lsn = PgLsn::from(lsn);
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

/// Issue #133's own staging helper: [`stage_cdc_at_lsn`] plus an explicit
/// `group_key` — standing in for what real intake's `cdc_change`/
/// `touched_group_key` would have populated from the row's own old/new
/// images for its outbound relationship's `from_col`, since these tests
/// stage directly into the ring rather than running a real replication
/// stream.
// 8 args, all independently meaningful test-staging inputs (mirrors
// `stage_cdc_at_lsn`'s own shape plus `group_key`) — a struct wrapper would
// just move the same fields into another type call sites still have to fill
// in field-by-field, for no real clarity gain in a test helper.
#[allow(clippy::too_many_arguments)]
async fn stage_cdc_with_group_key_at_lsn(
    client: &Client,
    src_table: &str,
    key: &str,
    op: &str,
    old_image: Option<&str>,
    new_image: Option<&str>,
    lsn: u64,
    group_key: &[&str],
) {
    let src_table = qualify_fixture_table(src_table);
    let table = active_seg_table(client).await;
    let lsn = PgLsn::from(lsn);
    let group_key: Vec<&str> = group_key.to_vec();
    client
        .execute(
            &format!(
                "insert into {table} \
                 (src_table, key, op, lsn, old_image, new_image, hop_gen, group_key) \
                 values ($1, $2, $3, $4, $5::text::jsonb, $6::text::jsonb, 0, $7)"
            ),
            &[
                &src_table, &key, &op, &lsn, &old_image, &new_image, &group_key,
            ],
        )
        .await
        .unwrap_or_else(|e| panic!("stage cdc {key:?} into {table} failed: {e}"));
}

async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    // Issue #132: a throwaway, always-caught-up watermark for the tests
    // that just want the pipeline to converge — the guard (a)-specific
    // tests below build their own `StagedWatermark` by hand instead of
    // using this helper.
    let watermark = StagedWatermark::saturated();
    for _ in 0..16 {
        let seg = seal_active_segment(client).await;
        while apply::drain_once(
            pool,
            seg,
            "reverse_test",
            1,
            "trellis_reverse_test",
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
    panic!("pipeline did not reach quiescence within 16 seal/drain rounds");
}

fn columns(pairs: &[(&str, ValueType)]) -> HashMap<String, ValueType> {
    pairs
        .iter()
        .map(|(name, ty)| (name.to_string(), *ty))
        .collect()
}

fn post_tags_columns() -> HashMap<String, ValueType> {
    columns(&[
        ("id", ValueType::Numeric),
        ("post", ValueType::Numeric),
        ("tag", ValueType::Text),
    ])
}

const TAG_TOTALS: &str = "TRANSFORM tag_totals FROM post_tags GROUP BY tag \
     SELECT COUNT(*) AS post_count, SUM(post.word_count) AS total_words";

/// Issue #94's exact schema (see `defs_aggregate_relationship.rs`), reused
/// verbatim: `post_tags` row 13 (`post = 999`, tag `rust`) is a dangling
/// reference to a post that doesn't exist yet — this file's parent-insert
/// test inserts it.
async fn create_schema(client: &Client) {
    client
        .batch_execute(
            "create table posts (id integer primary key, word_count integer); \
             create table post_tags (id integer primary key, post integer, tag text); \
             alter table post_tags replica identity full; \
             alter table posts replica identity full; \
             create index on post_tags (post); \
             insert into posts (id, word_count) values (1, 100), (2, 250), (3, null); \
             insert into post_tags (id, post, tag) values \
               (10, 1, 'rust'), (11, 2, 'rust'), (12, 1, 'db'), \
               (13, 999, 'rust'), (14, 3, 'db')",
        )
        .await
        .expect("create + seed issue #94's schema");
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

async fn projection_table_for(pool: &trellis::Pool, relationship_id: i64) -> String {
    relationship_projection(pool, relationship_id)
        .await
        .expect("read projection catalog row")
        .expect("to-one relationship has a projection")
        .projection_table
}

async fn projection_lsn(client: &Client, projection_table: &str, id: i32) -> Option<PgLsn> {
    client
        .query_one(
            &format!("select __trellis_lsn from {projection_table} where id = $1"),
            &[&id],
        )
        .await
        .unwrap_or_else(|e| panic!("read {projection_table}'s lsn for id {id}: {e}"))
        .get(0)
}

/// Drives `claim`/`fold`/`compute` by hand for one segment, stopping short
/// of applying it — the building block both the guard-(d) stopgap tests in
/// this file share, so two (or more) segments' Phase 2 can be forced to run
/// before either's Phase 3 commits (the out-of-order-drain race guard (d)
/// exists for; `drain_once` always applies a segment immediately after
/// computing it, so it can't construct this scenario on its own).
async fn claim_fold_compute(pool: &trellis::Pool, seg_seq: i64, claimed_by: &str) -> ApplyPlan {
    let mut phase1 = pool.get().await.expect("connection");
    let txn = phase1.transaction().await.expect("begin phase 1");
    claim::claim(&*txn, seg_seq, claimed_by, 1)
        .await
        .expect("claim");
    let filter = claim::owned_bucket_filter(&*txn, seg_seq, claimed_by)
        .await
        .expect("owned_bucket_filter");
    let folded = fold::fold(&txn, seg_seq, filter).await.expect("fold");
    txn.commit().await.expect("commit phase 1");
    apply::compute(pool, &folded).await.expect("compute")
}

async fn projection_row_exists(client: &Client, projection_table: &str, id: i32) -> bool {
    client
        .query_one(
            &format!("select exists (select 1 from {projection_table} where id = $1)"),
            &[&id],
        )
        .await
        .unwrap_or_else(|e| panic!("probe {projection_table} for id {id}: {e}"))
        .get(0)
}

/// Issue #132 guard (b)'s own column — read the same way [`projection_lsn`]
/// reads guard (d)'s.
async fn projection_gen(client: &Client, projection_table: &str, id: i32) -> i64 {
    client
        .query_one(
            &format!("select __trellis_gen from {projection_table} where id = $1"),
            &[&id],
        )
        .await
        .unwrap_or_else(|e| panic!("read {projection_table}'s gen for id {id}: {e}"))
        .get(0)
}

/// The `key`s of every image-less `recompute` row currently staged for
/// `src_table` in the *active* ring segment — the guard-rejection stopgap's
/// own signal (see the guard-(d) tests above for the same pattern), reused
/// by issue #132's guard (a)/(b)/(c) tests below.
async fn staged_recompute_keys(client: &Client, src_table: &str) -> Vec<String> {
    let table = active_seg_table(client).await;
    let src_table = qualify_fixture_table(src_table);
    client
        .query(
            &format!(
                "select key from {table} where src_table = $1 and op = 'recompute' order by key"
            ),
            &[&src_table],
        )
        .await
        .expect("read staged recompute rows")
        .into_iter()
        .map(|r| r.get(0))
        .collect()
}

/// Issue #134: every `rel_reverse_deferred` row currently staged for
/// `relationship_id` in the *active* ring segment — `(key, retry_count,
/// hop_gen)`, ordered by key. `hop_gen` rides along so tests can assert the
/// hop-bound-exemption property directly off the persisted row, not just
/// off `MAX_HOP_GEN` never tripping.
async fn staged_deferred_reverses(
    client: &Client,
    relationship_id: i64,
) -> Vec<(String, i32, i32)> {
    let table = active_seg_table(client).await;
    client
        .query(
            &format!(
                "select key, retry_count, hop_gen from {table} \
                 where op = 'rel_reverse_deferred' and relationship_id = $1 order by key"
            ),
            &[&relationship_id],
        )
        .await
        .expect("read staged rel_reverse_deferred rows")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect()
}

/// Issue #134: same shape as [`staged_deferred_reverses`], but scoped to
/// one *specific* already-sealed `seg_seq`'s own physical ring table,
/// rather than whatever is currently active — how the
/// "re-stage into the active batch, never the one being drained" tests
/// below prove a deferred row is *absent* from the segment its own
/// rejection ran against.
async fn deferred_reverses_in_segment(
    client: &Client,
    seg_seq: i64,
    relationship_id: i64,
) -> Vec<(String, i32, i32)> {
    let ring_slot: i16 = client
        .query_one(
            "select ring_slot from segments where seg_seq = $1",
            &[&seg_seq],
        )
        .await
        .expect("read segment's ring_slot")
        .get(0);
    let table = format!("seg_{ring_slot}");
    client
        .query(
            &format!(
                "select key, retry_count, hop_gen from {table} \
                 where op = 'rel_reverse_deferred' and relationship_id = $1 order by key"
            ),
            &[&relationship_id],
        )
        .await
        .expect("read staged rel_reverse_deferred rows")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect()
}

/// The current value of one label-series of a counter, scraped out of
/// [`trellis::metrics::Metrics::render_prometheus`]'s text exposition —
/// mirrors `end_to_end_latency.rs`'s own `bucket_count`/
/// `metric_mentions_transform` helpers for that file's histograms. `0` when
/// the series doesn't appear in the render at all (a label combination
/// that's never been incremented yet doesn't emit a line), matching a
/// counter's own natural starting value — never a panic, since a "before"
/// snapshot legitimately hits this case for a guard this test hasn't
/// triggered yet.
fn counter_value(rendered: &str, metric: &str, label_name: &str, label_value: &str) -> u64 {
    rendered
        .lines()
        .find(|line| {
            line.starts_with(metric) && line.contains(&format!("{label_name}=\"{label_value}\""))
        })
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

// ---------------------------------------------------------------------
// 1. A to-side update is a real delta, not a live recompute.
// ---------------------------------------------------------------------

/// A to-side (parent) update that changes a `SUM`-read column advances the
/// settled parent projection's `__trellis_lsn` to the change's own `lsn` —
/// a signature only issue #131's reverse-delta apply produces. The pre-#131
/// mechanism (an image-less from-side `Recompute`) never wrote to the
/// projection at all, so this is a direct, positive proof the new path ran,
/// not just an end-state convergence check (which an eventually-correct
/// live recompute would also pass).
#[tokio::test]
async fn to_side_update_advances_the_projection_lsn_via_the_delta_path() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    let projection_table = projection_table_for(&db.pool, relationship.id).await;

    client
        .execute("update posts set word_count = 400 where id = 1", &[])
        .await
        .expect("update the related post");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
        100,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        projection_lsn(&client, &projection_table, 1).await,
        Some(PgLsn::from(100)),
        "the reverse-delta apply must advance the projection's own lsn chain \
         to the applied record's lsn"
    );
    let totals = target_totals(&client).await;
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
// 2. N parent changes to the same key in one batch fold to one record.
// ---------------------------------------------------------------------

/// Two updates to the same to-side row, staged into the *same* segment
/// before it seals, fold (via the ordinary, pre-existing CDC fold — see
/// `RelationshipReverseRecord`'s doc comment on why no new ring plumbing was
/// needed) to one reverse record: `old_image` from the earliest raw change,
/// `new_image`/`lsn` from the latest. The target must reflect only the
/// *net* effect (100 -> 500, never a transient 100 -> 400 -> 500 that a
/// naive per-raw-row apply would double-count), and the projection's lsn
/// must land on the *latest* staged lsn, not the first.
#[tokio::test]
async fn two_parent_changes_in_one_batch_fold_to_one_record() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    let projection_table = projection_table_for(&db.pool, relationship.id).await;

    client
        .execute("update posts set word_count = 500 where id = 1", &[])
        .await
        .expect("update the related post to its final value");
    // Two raw CDC rows for the same key, same batch: 100->400, then
    // 400->500. The pre-existing fold collapses them to old=100, new=500.
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
        100,
    )
    .await;
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":400}"),
        Some("{\"id\":1,\"word_count\":500}"),
        200,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        projection_lsn(&client, &projection_table, 1).await,
        Some(PgLsn::from(200)),
        "the folded record's lsn must be the latest of the two raw changes"
    );
    let totals = target_totals(&client).await;
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("3".to_string()), Some("750".to_string()))),
        "net effect only: 250 (post 2) + 500 (post 1, its *final* value) + null"
    );
}

// ---------------------------------------------------------------------
// 3. Parent insert and parent delete.
// ---------------------------------------------------------------------

/// A parent INSERT (`old_image` absent): `post_tags` row 13's dangling
/// reference to `post = 999` starts contributing once that post exists,
/// and the projection gets a brand-new row (not an update to one that
/// never existed).
#[tokio::test]
async fn parent_insert_is_picked_up_by_the_reverse_path() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    let projection_table = projection_table_for(&db.pool, relationship.id).await;
    assert!(
        !projection_row_exists(&client, &projection_table, 999).await,
        "post 999 doesn't exist yet, so the projection must not have a row for it"
    );

    client
        .execute("insert into posts (id, word_count) values (999, 999)", &[])
        .await
        .expect("insert the previously-dangling post");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "999",
        "insert",
        None,
        Some("{\"id\":999,\"word_count\":999}"),
        50,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert!(
        projection_row_exists(&client, &projection_table, 999).await,
        "the reverse path must insert a projection row for a brand-new parent"
    );
    assert_eq!(
        projection_lsn(&client, &projection_table, 999).await,
        Some(PgLsn::from(50))
    );
    let totals = target_totals(&client).await;
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("3".to_string()), Some("1349".to_string()))),
        "100 (post 1) + 250 (post 2) + 999 (the newly-inserted post 999)"
    );
}

/// A parent DELETE (`new_image` absent): deleting post 2 removes its
/// contribution to `rust`'s total and removes its projection row outright.
#[tokio::test]
async fn parent_delete_is_picked_up_by_the_reverse_path() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    let projection_table = projection_table_for(&db.pool, relationship.id).await;
    assert!(projection_row_exists(&client, &projection_table, 2).await);

    client
        .execute("delete from posts where id = 2", &[])
        .await
        .expect("delete the related post");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "2",
        "delete",
        Some("{\"id\":2,\"word_count\":250}"),
        None,
        75,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert!(
        !projection_row_exists(&client, &projection_table, 2).await,
        "the reverse path must delete the projection row for a deleted parent"
    );
    let totals = target_totals(&client).await;
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("3".to_string()), Some("100".to_string()))),
        "only post 1's 100 remains; post 2 is gone and post 999 never existed"
    );
}

// ---------------------------------------------------------------------
// 4. The ordering stopgap (guard (d), pre-#132) actually rejects a stale
//    prev_lsn, and the pipeline still converges afterward.
// ---------------------------------------------------------------------

/// Simulates two reverse records for the *same* parent key both captured
/// (Phase 2) before either applied (Phase 3) — the scenario the plan doc's
/// guard (d) exists for (§2: "two parent changes in different segments can
/// drain out of order"). Driven by hand at the `compute`/
/// `apply_and_mark_drained` level (rather than `drain_once`, which always
/// applies a segment immediately after computing it) so both computes can
/// be forced to run before either apply commits.
///
/// The first apply succeeds and advances the projection's `lsn`. The
/// second's `prev_lsn` (captured before the first applied) no longer
/// matches, so it must **not** apply its own delta directly — asserted by
/// checking the projection's `lsn` is still the first record's, not the
/// second's, immediately after the second apply commits. The stopgap then
/// re-stages the second record's from-side rows as an image-less
/// `Recompute`, so a further drain to quiescence must still converge to the
/// correct final total (500's contribution, not 400's or double-counted).
#[tokio::test]
async fn a_stale_prev_lsn_is_rejected_and_the_pipeline_still_converges() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    let projection_table = projection_table_for(&db.pool, relationship.id).await;

    // First parent change: 100 -> 400, its own segment.
    client
        .execute("update posts set word_count = 400 where id = 1", &[])
        .await
        .expect("first update");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
        100,
    )
    .await;
    let seg_a = seal_active_segment(&mut client).await;

    // Phase 2 for segment A — captures `prev_lsn` against the pre-update
    // projection state.
    let plan_a = claim_fold_compute(&db.pool, seg_a, "worker_a").await;

    // Second parent change: 400 -> 500 (live value, matching what A's own
    // apply hasn't landed yet), its own later segment — captured *before*
    // A's Phase 3 runs, so it reads the exact same stale `prev_lsn`.
    client
        .execute("update posts set word_count = 500 where id = 1", &[])
        .await
        .expect("second update");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":400}"),
        Some("{\"id\":1,\"word_count\":500}"),
        200,
    )
    .await;
    let seg_b = seal_active_segment(&mut client).await;
    let plan_b = claim_fold_compute(&db.pool, seg_b, "worker_b").await;

    // Apply A: matches, advances the projection to lsn 100.
    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3 (a)");
    apply::apply_and_mark_drained(
        &txn,
        seg_a,
        "worker_a",
        &plan_a,
        "trellis_reverse_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("apply A");
    txn.commit().await.expect("commit A");
    assert_eq!(
        projection_lsn(&client, &projection_table, 1).await,
        Some(PgLsn::from(100))
    );

    // Apply B: its `prev_lsn` (captured before A applied) no longer matches
    // the projection's current lsn (100, not the pre-update seed) — the
    // ordering stopgap must reject B's own delta.
    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3 (b)");
    apply::apply_and_mark_drained(
        &txn,
        seg_b,
        "worker_b",
        &plan_b,
        "trellis_reverse_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("apply B (rejected internally, but the drain call itself still succeeds)");
    txn.commit().await.expect("commit B");

    assert_eq!(
        projection_lsn(&client, &projection_table, 1).await,
        Some(PgLsn::from(100)),
        "B's stale prev_lsn must be rejected — the projection stays at A's lsn, \
         not advanced to B's"
    );

    // The stopgap re-staged B's from-side rows as an image-less recompute;
    // draining that to quiescence must still converge on the *true* final
    // value (500, the live value by now), not 400 (A's value, stale) and
    // not double-counted.
    retire_drained_segments(&mut client)
        .await
        .expect("retire drained segments");
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = target_totals(&client).await;
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("3".to_string()), Some("750".to_string()))),
        "250 (post 2) + 500 (post 1's true final value, recovered via the stopgap \
         fallback) + null (post 999)"
    );
}

/// Issue #134 (originally a review follow-up to issue #131, about the
/// guard-(d) stopgap's *from-side Recompute* fallback loop double-staging
/// when `old_key == new_key`): that fallback no longer exists — a guard
/// rejection now re-stages the record itself as one `rel_reverse_deferred`
/// row, not a per-from-side-row `Recompute` fan-out, so the double-staging
/// this test originally pinned is structurally impossible today (there is
/// nothing left to enumerate twice). Repurposed to pin the replacement
/// invariant that matters now: an ordinary same-key parent update
/// (`old_key == new_key`, ids equal) whose guard (d) rejects stages
/// *exactly one* `rel_reverse_deferred` row — not two, one per key side —
/// carrying `retry_count = 1` and `hop_gen = 0` (issue #134's exemption).
///
/// Same two-segment setup as `a_stale_prev_lsn_is_rejected_and_the_pipeline_still_converges`,
/// but both parent changes touch the same `id = 1` (an ordinary attribute
/// update on both sides, not a repoint) — and instead of draining to
/// quiescence, this inspects the ring directly right after B's rejected
/// apply commits.
#[tokio::test]
async fn a_same_key_guard_rejection_stages_exactly_one_deferred_reverse_row() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    // First parent change: 100 -> 400, its own segment.
    client
        .execute("update posts set word_count = 400 where id = 1", &[])
        .await
        .expect("first update");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
        100,
    )
    .await;
    let seg_a = seal_active_segment(&mut client).await;
    let plan_a = claim_fold_compute(&db.pool, seg_a, "worker_a").await;

    // Second parent change: same key (id = 1), 400 -> 500 — old_key ==
    // new_key, the common case the bug affected. Captured before A applies,
    // so it reads the same stale prev_lsn A is about to consume.
    client
        .execute("update posts set word_count = 500 where id = 1", &[])
        .await
        .expect("second update");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":400}"),
        Some("{\"id\":1,\"word_count\":500}"),
        200,
    )
    .await;
    let seg_b = seal_active_segment(&mut client).await;
    let plan_b = claim_fold_compute(&db.pool, seg_b, "worker_b").await;

    // Apply A: matches, advances the projection to lsn 100.
    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3 (a)");
    apply::apply_and_mark_drained(
        &txn,
        seg_a,
        "worker_a",
        &plan_a,
        "trellis_reverse_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("apply A");
    txn.commit().await.expect("commit A");

    // Apply B: prev_lsn is stale — guard (d) rejects and B is re-staged as
    // one `rel_reverse_deferred` row (issue #134), not a from-side
    // Recompute fan-out. Neither A's fast-path delta (no downstream readers
    // of tag_totals) nor B's own deferral branch (it `continue`s past the
    // `needs_recompute_fallback`/projection-advance code) stages anything
    // else, so whatever lands in the now-active ring segment is exactly
    // this test's signal.
    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3 (b)");
    apply::apply_and_mark_drained(
        &txn,
        seg_b,
        "worker_b",
        &plan_b,
        "trellis_reverse_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("apply B");
    txn.commit().await.expect("commit B");

    let deferred = staged_deferred_reverses(&client, relationship.id).await;
    assert_eq!(
        deferred,
        vec![("1".to_string(), 1, 0)],
        "exactly one rel_reverse_deferred row for post 1 (old_key == new_key, \
         not one per side), retry_count 1 on its first rejection, hop_gen \
         untouched at 0"
    );
}

// ---------------------------------------------------------------------
// 6. Issue #132's guards (a), (b), (c) — guard (d) is #4 above (issue
//    #131's original stopgap, formalized as one of these four guards in
//    `apply.rs`'s `check_reverse_guards`).
//
// Each test drives `compute`/`apply_and_mark_drained` by hand (the same
// `claim_fold_compute` building block guard (d)'s tests use above) so the
// specific precondition each guard checks can be violated deliberately,
// independent of the other three, mirroring
// `a_stale_prev_lsn_is_rejected_and_the_pipeline_still_converges`'s own
// pattern: assert the guard rejected (the projection did not advance, and
// an image-less fallback recompute was staged for the parent's from-side
// rows), then drive the pipeline to quiescence and assert it still
// converges on the true final value.
// ---------------------------------------------------------------------

/// Guard (a) (plan doc §2; ablation 125/3000, and the precondition that
/// makes guard (c) trustworthy at all): a reverse record whose captured
/// watermark `X` (the source's write frontier as of Phase 2) is still ahead
/// of what intake has *staged* must not apply — even though nothing else
/// about the record looks wrong (no concurrent forward apply, no sibling
/// reverse, no in-flight child). Driven with a [`StagedWatermark`]
/// constructed fresh (starts at LSN 0 — see its own doc comment) and never
/// advanced: no live `intake::Intake` runs in this test at all, so a
/// watermark that never moves is exactly "intake hasn't caught up yet."
#[tokio::test]
async fn guard_a_watermark_barrier_rejects_and_the_pipeline_still_converges() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    let projection_table = projection_table_for(&db.pool, relationship.id).await;
    let baseline_lsn = projection_lsn(&client, &projection_table, 1).await;

    client
        .execute("update posts set word_count = 400 where id = 1", &[])
        .await
        .expect("update the related post");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
        100,
    )
    .await;
    let seg = seal_active_segment(&mut client).await;
    let plan = claim_fold_compute(&db.pool, seg, "worker_a").await;

    // Guard (a): this database has already generated plenty of real WAL
    // (schema DDL, seed inserts, the drain-to-quiescence above) by the time
    // `claim_fold_compute` captured its own `X` a moment ago, so a
    // watermark that starts at (and stays at) LSN 0 is certain to be
    // behind it.
    let unstaged_watermark = StagedWatermark::new();
    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3");
    apply::apply_and_mark_drained(
        &txn,
        seg,
        "worker_a",
        &plan,
        "trellis_reverse_test",
        &unstaged_watermark,
    )
    .await
    .expect("apply (rejected internally by guard (a), but the drain call itself still succeeds)");
    txn.commit().await.expect("commit");

    assert_eq!(
        projection_lsn(&client, &projection_table, 1).await,
        baseline_lsn,
        "guard (a) must reject before ever touching the projection"
    );
    assert_eq!(
        staged_deferred_reverses(&client, relationship.id).await,
        vec![("1".to_string(), 1, 0)],
        "guard (a) must defer and re-stage the record itself (issue #134) — \
         one rel_reverse_deferred row for post 1, retry_count 1, hop_gen \
         untouched at 0 — not an image-less recompute of its from-side rows"
    );
    assert!(
        staged_recompute_keys(&client, "post_tags").await.is_empty(),
        "issue #134 replaces the from-side Recompute fallback entirely; \
         nothing should be staged for post_tags directly"
    );

    retire_drained_segments(&mut client)
        .await
        .expect("retire drained segments");
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = target_totals(&client).await;
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("3".to_string()), Some("650".to_string()))),
        "the pipeline must still converge on post 1's true value (400) via \
         a retried delta, even though guard (a) rejected the first attempt — \
         an aborting reverse defers and retries, it does not stall the batch"
    );
    assert_eq!(
        projection_lsn(&client, &projection_table, 1).await,
        Some(PgLsn::from(100)),
        "the retried delta must eventually advance the projection to the \
         deferred record's own lsn, proving it applied as a real delta on \
         retry rather than merely converging via some other path"
    );
}

/// Guard (b) (plan doc §2; ablation 82/3000): a reverse record whose
/// captured `prev_gen` no longer matches the projection row's current
/// `__trellis_gen`, re-read under `FOR UPDATE` in Phase 3, must not apply —
/// a forward apply resolved this same parent through the projection
/// between Phase 2's capture and Phase 3's lock. Simulated by bumping
/// `__trellis_gen` directly (reaching past the mechanism, matching this
/// suite's own convention for standing in for a piece another issue builds
/// — see e.g. `insert_poison_held` in `quarantine.rs`): building a *real*
/// concurrent forward apply here would need a second, independent
/// from-side change unrelated to this scenario, which is exactly what
/// `apply_and_mark_drained_many`'s "3c" step's gen-bump already is regardless
/// of what triggered it.
#[tokio::test]
async fn guard_b_generation_check_rejects_and_the_pipeline_still_converges() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    let projection_table = projection_table_for(&db.pool, relationship.id).await;
    let baseline_lsn = projection_lsn(&client, &projection_table, 1).await;
    let baseline_gen = projection_gen(&client, &projection_table, 1).await;

    client
        .execute("update posts set word_count = 400 where id = 1", &[])
        .await
        .expect("update the related post");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
        100,
    )
    .await;
    let seg = seal_active_segment(&mut client).await;
    let plan = claim_fold_compute(&db.pool, seg, "worker_a").await;

    // Simulate "a forward apply landed between Phase 2's capture and Phase
    // 3's lock" by bumping the projection row's gen directly — exactly the
    // effect `apply_and_mark_drained_many`'s "3c" step has, regardless of
    // what change actually triggered it.
    client
        .execute(
            &format!(
                "update {projection_table} set __trellis_gen = __trellis_gen + 1 where id = 1"
            ),
            &[],
        )
        .await
        .expect("bump the projection's gen, simulating a concurrent forward apply");

    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3");
    apply::apply_and_mark_drained(
        &txn,
        seg,
        "worker_a",
        &plan,
        "trellis_reverse_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("apply (rejected internally by guard (b), but the drain call itself still succeeds)");
    txn.commit().await.expect("commit");

    assert_eq!(
        projection_lsn(&client, &projection_table, 1).await,
        baseline_lsn,
        "guard (b) must reject before ever touching the projection's lsn"
    );
    assert_eq!(
        projection_gen(&client, &projection_table, 1).await,
        baseline_gen + 1,
        "the projection's gen must stay exactly at the simulated forward \
         apply's bump — guard (b) must not advance it further"
    );
    assert_eq!(
        staged_deferred_reverses(&client, relationship.id).await,
        vec![("1".to_string(), 1, 0)],
        "guard (b) must defer and re-stage the record itself (issue #134) — \
         one rel_reverse_deferred row for post 1, retry_count 1, hop_gen \
         untouched at 0 — not an image-less recompute of its from-side rows"
    );

    retire_drained_segments(&mut client)
        .await
        .expect("retire drained segments");
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = target_totals(&client).await;
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("3".to_string()), Some("650".to_string()))),
        "the pipeline must still converge on post 1's true value (400) via \
         a retried delta, even though guard (b) rejected the first attempt — \
         the retry re-derives prev_gen fresh (now the simulated forward \
         apply's bumped value), so it correctly passes once retried"
    );
    assert_eq!(
        projection_lsn(&client, &projection_table, 1).await,
        Some(PgLsn::from(100)),
        "the retried delta must eventually advance the projection to the \
         deferred record's own lsn"
    );
}

/// Issue #133's own regression pin — the exact trace from the issue body
/// (plan doc §3.1): a from-side row (`articles`) is inserted pointing at
/// category 3, then re-pointed to category 2, **within the same batch**.
/// The fold collapses this to `old_image = NULL` (born in the batch),
/// `new_image` naming category 2 — category 3 appears in *neither* folded
/// endpoint. Before #133, nothing signalled that category 3 was touched at
/// all, so its projection row's `gen` never bumped, and a reverse for
/// category 3 holding an enumeration captured before the re-point would
/// wrongly pass guard (b) and move a row (`articles` id 100) that never
/// left. This test proves both halves directly: (1) the erasing batch's own
/// Phase 3 still bumps category 3's `gen` (a positive, direct signal —
/// before #133 this assertion alone fails, since nothing in the folded
/// old/new image ever named category 3), and (2) a stale reverse captured
/// before that bump is then correctly rejected by guard (b) rather than
/// silently applying against a from-side set that already moved on.
///
/// Deliberately uses the `articles`/`categories` `KeySpace::OneToOne`
/// fixture (`article_cat_def`, below), **not** this file's usual
/// `post_tags`/`posts` aggregate fixture: at the time this test was written,
/// an aggregate definition's forward path went through `apply_aggregate`'s
/// `rel_joins`/`force_every_group` mechanism (out of scope for this issue —
/// see the plan doc §7 step 8 and this crate's own review notes), which
/// never called `build_relationship_context` and so never reached the #133
/// gen-bump code at all — a `KeySpace::OneToOne` definition reading a
/// relationship was the only shape that exercised it on the forward path.
/// Issue #136 (epic #127) later deleted `force_every_group` and wired the
/// aggregate forward path through `build_relationship_context` too (see
/// `apply_aggregate`'s module doc comment), so this distinction no longer
/// holds — but this test's own fixture choice is left as-is rather than
/// migrated, since it already covers the `KeySpace::OneToOne` shape
/// correctly and `defs_aggregate_relationship.rs`/`apply_relationship_forward.rs`
/// cover the aggregate shape.
#[tokio::test]
async fn issue_133_a_within_batch_repoint_still_bumps_the_erased_intermediate_parents_gen() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table categories (id integer primary key, name text); \
             alter table categories replica identity full; \
             create table articles (id integer primary key, category_id integer); \
             alter table articles replica identity full; \
             insert into categories (id, name) values (1, 'A'), (2, 'B'), (3, 'C')",
        )
        .await
        .expect("create + seed tables");

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_id TO categories.id",
    )
    .await
    .expect("create to-one relationship");
    create_definition(
        &db.pool,
        "TRANSFORM article_cat FROM articles SELECT category.name AS category_name",
        &articles_columns(),
    )
    .await
    .expect("create to-one enrichment definition");
    let pk = require_single_column_pk(
        source_primary_key(&db.pool, "articles")
            .await
            .expect("introspect articles pk"),
        "articles",
    )
    .expect("single-column pk");
    create_target_table(
        &db.pool,
        &article_cat_def(),
        "public",
        &pk,
        &articles_columns(),
        &article_cat_def().source,
    )
    .await
    .expect("create target table");

    // The pre-existing child: article 100, already pointing at category 3
    // before either of this test's two batches runs — the row guard (b)'s
    // fallback must (correctly) still move, and must be the *only* one it
    // moves. Both the live row (guard (b)'s fallback live-enumerates the
    // *current* table, not the ring) and its staged CDC record (so the
    // forward path picks it up too) are needed.
    client
        .execute(
            "insert into articles (id, category_id) values (100, 3)",
            &[],
        )
        .await
        .expect("insert article 100 pointing at category 3");
    stage_cdc_at_lsn(
        &client,
        "articles",
        "100",
        "insert",
        None,
        Some("{\"id\":100,\"category_id\":3}"),
        1,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    let projection_table = projection_table_for(&db.pool, relationship.id).await;
    let baseline_lsn_3 = projection_lsn(&client, &projection_table, 3).await;
    let baseline_gen_3 = projection_gen(&client, &projection_table, 3).await;

    // The reverse candidate: a genuine parent change to category 3 (rename
    // it), Phase 2-captured *before* the erasing batch below ever runs — so
    // its `prev_gen` is `baseline_gen_3`, about to go stale.
    client
        .execute("update categories set name = 'C-renamed' where id = 3", &[])
        .await
        .expect("rename category 3");
    stage_cdc_at_lsn(
        &client,
        "categories",
        "3",
        "update",
        Some("{\"id\":3,\"name\":\"C\"}"),
        Some("{\"id\":3,\"name\":\"C-renamed\"}"),
        100,
    )
    .await;
    let seg_r = seal_active_segment(&mut client).await;
    let plan_r = claim_fold_compute(&db.pool, seg_r, "worker_r").await;

    // The erasing batch: `articles` row 200 inserted pointing at category
    // 3, then re-pointed to category 2 — both within the *same*,
    // still-active segment. `group_key` is set explicitly on each raw row
    // (standing in for what real intake's `touched_group_key` would have
    // read off these same old/new images), exactly as issue #133 populates
    // it: the insert's own touched value (3), then the update's own
    // touched values (3 and 2).
    client
        .execute(
            "insert into articles (id, category_id) values (200, 3)",
            &[],
        )
        .await
        .expect("insert an article pointing at category 3");
    stage_cdc_with_group_key_at_lsn(
        &client,
        "articles",
        "200",
        "insert",
        None,
        Some("{\"id\":200,\"category_id\":3}"),
        200,
        &["3"],
    )
    .await;
    client
        .execute("update articles set category_id = 2 where id = 200", &[])
        .await
        .expect("re-point article 200 to category 2");
    stage_cdc_with_group_key_at_lsn(
        &client,
        "articles",
        "200",
        "update",
        Some("{\"id\":200,\"category_id\":3}"),
        Some("{\"id\":200,\"category_id\":2}"),
        300,
        &["3", "2"],
    )
    .await;

    // Fully drain the erasing batch on its own — its Phase 3 is what must
    // bump category 3's gen via the #133 `group_key` signal.
    let seg_e = seal_active_segment(&mut client).await;
    let watermark = StagedWatermark::saturated();
    while apply::drain_once(
        &db.pool,
        seg_e,
        "worker_e",
        1,
        "trellis_reverse_test",
        &watermark,
    )
    .await
    .expect("drain_once seg_e")
    .is_some()
    {}
    retire_drained_segments(&mut client)
        .await
        .expect("retire drained segments");

    // (1) The positive, direct signal: category 3's projection row's gen
    // must have bumped, even though category 3 is invisible in the erasing
    // batch's own folded old/new images — this is exactly the assertion
    // that fails without #133 (the folded-endpoint-only signal never names
    // category 3 at all: old_image is NULL, born in the batch; new_image
    // names category 2).
    assert_eq!(
        projection_gen(&client, &projection_table, 3).await,
        baseline_gen_3 + 1,
        "the erased intermediate parent (category 3) must still get its gen \
         bumped — the folded old/new images alone never name it"
    );

    // (2) Guard (b): the reverse captured *before* that bump must now be
    // rejected, not silently applied against a from-side set that already
    // moved on (article 200 no longer points at category 3).
    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3");
    apply::apply_and_mark_drained(
        &txn,
        seg_r,
        "worker_r",
        &plan_r,
        "trellis_reverse_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("apply (rejected internally by guard (b), but the drain call itself still succeeds)");
    txn.commit().await.expect("commit");

    assert_eq!(
        projection_lsn(&client, &projection_table, 3).await,
        baseline_lsn_3,
        "guard (b) must reject before ever touching the projection's lsn"
    );
    assert_eq!(
        projection_gen(&client, &projection_table, 3).await,
        baseline_gen_3 + 1,
        "guard (b) must not advance gen any further than the erasing batch's own bump"
    );
    assert_eq!(
        staged_deferred_reverses(&client, relationship.id).await,
        vec![("3".to_string(), 1, 0)],
        "guard (b) must defer and re-stage the record itself (issue #134) — \
         one rel_reverse_deferred row for category 3, retry_count 1, hop_gen \
         untouched at 0 — not an image-less recompute of its from-side rows"
    );
    assert!(
        staged_recompute_keys(&client, "articles").await.is_empty(),
        "issue #134 replaces the from-side Recompute fallback entirely; \
         nothing should be staged for articles directly by the rejection"
    );

    retire_drained_segments(&mut client)
        .await
        .expect("retire drained segments");
    drain_to_quiescence(&db.pool, &mut client).await;

    // The mechanism-level assertions above are this test's actual #133
    // proof (the erased parent's gen bumped; guard (b) then correctly
    // rejected the stale reverse rather than silently applying it). With
    // issue #134's deferral plumbing now built, that rejected reverse is
    // re-derived and retried on the next drain: `prev_gen` is re-captured
    // fresh (now the erasing batch's own bump, `baseline_gen_3 + 1`) rather
    // than replayed stale, so the retry's guard (b) check correctly passes
    // this time (nothing bumped the gen again in between), the delta
    // applies, and the projection advances to "C-renamed" — which the
    // `needs_recompute_fallback` path's own re-staged `Recompute` for
    // article 100 (still, correctly, the only from-side row this reverse
    // touches — article 200 re-pointed away and is untouched by it) then
    // picks up. Article 100 ends up on the *new* value, not stuck on the
    // stale one — the entire point of #134 is that a deferred reverse
    // converges, not that it never applies.
    assert_eq!(
        target_category_name(&client, 100).await,
        Some("C-renamed".to_string()),
        "article 100 must eventually reflect category 3's renamed value \
         once the deferred reverse is retried and succeeds (issue #134)"
    );
    assert_eq!(
        target_category_name(&client, 200).await,
        Some("B".to_string()),
        "article 200 must reflect category 2 (where it actually re-pointed to), \
         not category 3 (where guard (b) correctly refused to move it)"
    );
}

/// Guard (c) (plan doc §2; ablation 1174/3000 — the guard doing most of the
/// correctness work): a reverse record must not apply while any staged
/// from-side change for its parent's join key, committed at or before `X`,
/// is still undrained — otherwise the paired subtract-old/add-new delta
/// would be computed against a from-side row set that doesn't yet equal
/// what the target actually reflects. Built by staging a brand-new
/// `post_tags` row pointing at post 1 into the *new* active segment (left
/// deliberately unsealed, hence undrained) before computing/applying the
/// *earlier*, already-sealed segment holding post 1's own parent change —
/// guard (a) and guard (d) both pass here (a saturated watermark and an
/// unmoved `prev_lsn`), isolating guard (c) as the one guard that catches
/// this.
#[tokio::test]
async fn guard_c_in_flight_check_rejects_and_the_pipeline_still_converges() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    let projection_table = projection_table_for(&db.pool, relationship.id).await;
    let baseline_lsn = projection_lsn(&client, &projection_table, 1).await;
    let baseline_gen = projection_gen(&client, &projection_table, 1).await;

    // The parent change, sealed alone into its own segment.
    client
        .execute("update posts set word_count = 400 where id = 1", &[])
        .await
        .expect("update the related post");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
        100,
    )
    .await;
    let seg = seal_active_segment(&mut client).await;

    // A brand-new from-side row pointing at the same parent, staged into
    // the (new) active segment right after — never sealed, so it stays
    // undrained for the rest of this test.
    client
        .execute(
            "insert into post_tags (id, post, tag) values (20, 1, 'rust')",
            &[],
        )
        .await
        .expect("insert a new post_tags row pointing at post 1");
    stage_cdc_at_lsn(
        &client,
        "post_tags",
        "20",
        "insert",
        None,
        Some("{\"id\":20,\"post\":1,\"tag\":\"rust\"}"),
        50,
    )
    .await;

    // Phase 2 for the parent's segment — captured *after* the from-side
    // insert above already committed, so `X` (guard (a)'s watermark) is
    // certain to cover it.
    let plan = claim_fold_compute(&db.pool, seg, "worker_a").await;

    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3");
    apply::apply_and_mark_drained(
        &txn,
        seg,
        "worker_a",
        &plan,
        "trellis_reverse_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("apply (rejected internally by guard (c), but the drain call itself still succeeds)");
    txn.commit().await.expect("commit");

    assert_eq!(
        projection_lsn(&client, &projection_table, 1).await,
        baseline_lsn,
        "guard (c) must reject before ever touching the projection"
    );
    assert_eq!(
        projection_gen(&client, &projection_table, 1).await,
        baseline_gen,
        "guard (c) must reject before ever touching the projection"
    );
    // Issue #134: guard (c) defers and re-stages the record itself, rather
    // than falling back to a live from-side enumeration — the retry's own
    // fast-path delta (once row 20's segment has drained and the in-flight
    // condition clears) is what ends up picking up row 20 live, via the
    // same `from_side_rows_for_trigger_txn` read the pre-#134 fallback used.
    assert_eq!(
        staged_deferred_reverses(&client, relationship.id).await,
        vec![("1".to_string(), 1, 0)],
        "guard (c) must defer and re-stage the record itself (issue #134) — \
         one rel_reverse_deferred row for post 1, retry_count 1, hop_gen \
         untouched at 0 — not an image-less recompute of its from-side rows"
    );
    assert!(
        staged_recompute_keys(&client, "post_tags").await.is_empty(),
        "issue #134 replaces the from-side Recompute fallback entirely; \
         nothing should be staged for post_tags directly by the rejection"
    );

    retire_drained_segments(&mut client)
        .await
        .expect("retire drained segments");
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = target_totals(&client).await;
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("4".to_string()), Some("1050".to_string()))),
        "the pipeline must still converge on post 1's true value (400), \
         counting the new row 20 exactly once: 400 (post 1, via row 10) + \
         250 (post 2) + null (post 999) + 400 (post 1, via the new row 20) \
         — via a retried delta, once row 20's segment drains and guard (c) \
         no longer sees anything in flight"
    );
    assert_eq!(
        projection_lsn(&client, &projection_table, 1).await,
        Some(PgLsn::from(100)),
        "the retried delta must eventually advance the projection to the \
         deferred record's own lsn"
    );
}

/// Positive case: an ordinary parent update where all four guards
/// genuinely pass — including guard (a), proven with a real
/// [`StagedWatermark`] advanced to (not merely defaulted past) the current
/// `pg_current_wal_lsn()`, not [`StagedWatermark::saturated`]. A direct
/// positive signal that the true-delta path ran (the projection's lsn
/// advances to this record's own lsn) rather than an indirect "the final
/// total happens to be right" check, and that *no* fallback recompute was
/// staged — the fast path fully covered this record.
#[tokio::test]
async fn all_four_guards_pass_and_the_delta_applies_in_the_ordinary_case() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    let projection_table = projection_table_for(&db.pool, relationship.id).await;

    client
        .execute("update posts set word_count = 400 where id = 1", &[])
        .await
        .expect("update the related post");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
        100,
    )
    .await;
    let seg = seal_active_segment(&mut client).await;
    let plan = claim_fold_compute(&db.pool, seg, "worker_a").await;

    // Guard (a) genuinely passes here: seeded at LSN 0, then advanced to
    // the real current `pg_current_wal_lsn()` — strictly past whatever `X`
    // Phase 2 captured a moment ago, since nothing else has committed on
    // this connection since.
    let watermark = StagedWatermark::new();
    let now: PgLsn = client
        .query_one("select pg_current_wal_lsn()", &[])
        .await
        .expect("read the current wal lsn")
        .get(0);
    watermark.advance(now);

    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3");
    apply::apply_and_mark_drained(
        &txn,
        seg,
        "worker_a",
        &plan,
        "trellis_reverse_test",
        &watermark,
    )
    .await
    .expect("apply");
    txn.commit().await.expect("commit");

    assert_eq!(
        projection_lsn(&client, &projection_table, 1).await,
        Some(PgLsn::from(100)),
        "all four guards passed, so the true-delta path must have advanced \
         the projection to this record's own lsn"
    );
    assert_eq!(
        staged_recompute_keys(&client, "post_tags").await,
        Vec::<String>::new(),
        "all four guards passed, so no fallback recompute should have been \
         staged for post 1's children"
    );
    let totals = target_totals(&client).await;
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("3".to_string()), Some("650".to_string())))
    );
}

/// A combined, hand-interleaved scenario loosely modeling the plan doc's
/// own generative ablation campaign (§4): two segments draining
/// out-of-order (guard (d)'s own scenario) *and* a from-side child staged
/// in between, still undrained when the pipeline finally gets back around
/// to it. Not a substitute for that 3,000-run campaign (explicitly out of
/// this issue's scope — see #138), just a hand-built check that the four
/// guards still compose correctly (each one's rejection doesn't corrupt or
/// skip what the others are responsible for) under more than one hazard at
/// once.
#[tokio::test]
async fn out_of_order_segments_plus_an_in_flight_child_still_converges() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;
    let projection_table = projection_table_for(&db.pool, relationship.id).await;

    // Segment A: 100 -> 400.
    client
        .execute("update posts set word_count = 400 where id = 1", &[])
        .await
        .expect("first update");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
        100,
    )
    .await;
    let seg_a = seal_active_segment(&mut client).await;
    let plan_a = claim_fold_compute(&db.pool, seg_a, "worker_a").await;

    // Segment B: 400 -> 500 (the live value), captured before A applies —
    // the same stale-`prev_lsn` setup as guard (d)'s own test.
    client
        .execute("update posts set word_count = 500 where id = 1", &[])
        .await
        .expect("second update");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":400}"),
        Some("{\"id\":1,\"word_count\":500}"),
        200,
    )
    .await;
    let seg_b = seal_active_segment(&mut client).await;
    let plan_b = claim_fold_compute(&db.pool, seg_b, "worker_b").await;

    // A applies cleanly.
    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3 (a)");
    apply::apply_and_mark_drained(
        &txn,
        seg_a,
        "worker_a",
        &plan_a,
        "trellis_reverse_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("apply A");
    txn.commit().await.expect("commit A");

    // A brand-new from-side row, staged into the current active segment
    // (never sealed here) — still undrained by the time B's rejected apply
    // runs below.
    client
        .execute(
            "insert into post_tags (id, post, tag) values (21, 1, 'rust')",
            &[],
        )
        .await
        .expect("insert a new post_tags row pointing at post 1");
    stage_cdc_at_lsn(
        &client,
        "post_tags",
        "21",
        "insert",
        None,
        Some("{\"id\":21,\"post\":1,\"tag\":\"rust\"}"),
        150,
    )
    .await;

    // B's `prev_lsn` (captured before A applied) is already stale, so
    // guard (d) rejects it regardless of the in-flight child above — this
    // exercises both hazards landing on the same key in the same window,
    // not just one at a time.
    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3 (b)");
    apply::apply_and_mark_drained(
        &txn,
        seg_b,
        "worker_b",
        &plan_b,
        "trellis_reverse_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("apply B (rejected internally, but the drain call itself still succeeds)");
    txn.commit().await.expect("commit B");

    retire_drained_segments(&mut client)
        .await
        .expect("retire drained segments");
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = target_totals(&client).await;
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("4".to_string()), Some("1250".to_string()))),
        "500 (post 1's true final value, via row 10) + 250 (post 2) + null \
         (post 999) + 500 (post 1's true final value, via the new row 21) \
         — recovered correctly despite the out-of-order segments and the \
         in-flight child both landing on the same key. Takes more than one \
         retry round: guard (c) defers again on the very first retry\
         attempt, since row 21's own CDC shares the deferred record's \
         segment and so still reads as undrained mid-transaction; the \
         second retry (once that segment has actually committed) passes. \
         See issue #134's `retry_count == 0` fast-path gate (this module's \
         own comment on it) for why this doesn't corrupt the total despite \
         row 21's own forward apply having already folded its contribution \
         into the group by the time the delta finally applies."
    );
    assert_eq!(
        projection_lsn(&client, &projection_table, 1).await,
        Some(PgLsn::from(200)),
        "the retried delta must eventually advance the projection to B's \
         own lsn"
    );
}

// ---------------------------------------------------------------------
// 5. To-many relationships are unaffected.
// ---------------------------------------------------------------------

fn articles_columns() -> HashMap<String, ValueType> {
    columns(&[
        ("id", ValueType::Numeric),
        ("category_id", ValueType::Numeric),
    ])
}

fn article_cat_def() -> TransformDef {
    TransformDef {
        target: "article_cat".to_string(),
        source: "articles".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "category_name".to_string(),
            expr: Expr::RelationshipPath {
                rel: "category".to_string(),
                column: "name".to_string(),
            },
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    }
}

async fn target_category_name(client: &Client, id: i32) -> Option<String> {
    client
        .query_one(
            "select category_name from article_cat where id = $1",
            &[&id],
        )
        .await
        .unwrap_or_else(|e| panic!("read article_cat id {id}: {e}"))
        .get(0)
}

/// A `KeySpace::OneToOne` target reading a to-one relationship is design
/// fork 1 in `build_reverse_relationship_shape`'s doc comment — it has no
/// additive semantics to delta, so it stays on the pre-#131 image-less
/// `Recompute` mechanism (`ReverseRelationshipShape::needs_recompute_fallback`)
/// rather than being folded into the new fast path. This just proves that
/// fallback still converges correctly post-#131 — a regression check for
/// the exact scenario `apply_relationship_forward.rs`'s fixture uses.
#[tokio::test]
async fn a_one_to_one_target_still_converges_via_the_fallback_mechanism() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table categories (id integer primary key, name text); \
             alter table categories replica identity full; \
             insert into categories (id, name) values (10, 'Tech'); \
             create table articles (id integer primary key, category_id integer); \
             alter table articles replica identity full; \
             insert into articles (id, category_id) values (1, 10)",
        )
        .await
        .expect("create + seed tables");

    create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_id TO categories.id",
    )
    .await
    .expect("create to-one relationship");
    create_definition(
        &db.pool,
        "TRANSFORM article_cat FROM articles SELECT category.name AS category_name",
        &articles_columns(),
    )
    .await
    .expect("create to-one enrichment definition");
    let pk = require_single_column_pk(
        source_primary_key(&db.pool, "articles")
            .await
            .expect("introspect articles pk"),
        "articles",
    )
    .expect("single-column pk");
    create_target_table(
        &db.pool,
        &article_cat_def(),
        "public",
        &pk,
        &articles_columns(),
        &article_cat_def().source,
    )
    .await
    .expect("create target table");
    stage_cdc_at_lsn(
        &client,
        "articles",
        "1",
        "insert",
        None,
        Some("{\"id\":1,\"category_id\":10}"),
        1,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        target_category_name(&client, 1).await,
        Some("Tech".to_string())
    );

    client
        .execute("update categories set name = 'Renamed' where id = 10", &[])
        .await
        .expect("rename the category");
    stage_cdc_at_lsn(
        &client,
        "categories",
        "10",
        "update",
        Some("{\"id\":10,\"name\":\"Tech\"}"),
        Some("{\"id\":10,\"name\":\"Renamed\"}"),
        1,
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        target_category_name(&client, 1).await,
        Some("Renamed".to_string()),
        "a 1-1 target reading the relationship must still re-derive via the \
         pre-#131 fallback, unchanged"
    );
}

// ---------------------------------------------------------------------
// 6. Issue #134: the deferral plumbing itself — its own staged kind, the
//    retry counter's hop_gen exemption, re-staging into the active batch,
//    and the four per-guard metrics.
// ---------------------------------------------------------------------

/// Two things at once, both load-bearing: (1) issue #134's own exemption —
/// a reverse that keeps getting deferred must never touch `hop_gen`, so it
/// can never trip [`apply::MAX_HOP_GEN`] (32) — and (2) issue #135's
/// fairness bound on top of it — deferral must not continue forever in the
/// first place. Before #135, this test drove a watermark that never
/// advances (guard (a) rejects deterministically, every single round,
/// independent of any other guard's state) for 40 rounds and asserted
/// `retry_count` climbed monotonically the whole way, precisely because
/// nothing stopped it. That is no longer the correct expectation: guard (a)
/// is not guard (d), so once `retry_count` reaches
/// [`apply::RELATIONSHIP_REVERSE_FAIRNESS_THRESHOLD`], the next rejection
/// must escalate instead of deferring a `THRESHOLD + 1`-th time — see
/// `staging::apply`'s own "Issue #135, epic #127: fairness escalation"
/// design section for why escalating past guard (a)/(b)/(c) specifically
/// (never (d)) is sound.
#[tokio::test]
async fn deferring_past_the_fairness_threshold_escalates_instead_of_spinning_forever() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    let projection_table = projection_table_for(&db.pool, relationship.id).await;

    client
        .execute("update posts set word_count = 400 where id = 1", &[])
        .await
        .expect("update the related post");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
        100,
    )
    .await;

    // Never advanced — guard (a) rejects on every single round, so nothing
    // about *this specific* rejection ever resolves on its own. Before the
    // threshold, this must look exactly like the pre-#135 behavior:
    // `retry_count` climbing by one per round, `hop_gen` pinned at 0.
    let unstaged_watermark = StagedWatermark::new();
    let threshold = apply::RELATIONSHIP_REVERSE_FAIRNESS_THRESHOLD;
    for round in 1..threshold {
        let seg = seal_active_segment(&mut client).await;
        let plan = claim_fold_compute(&db.pool, seg, "worker_hopgen").await;
        let mut phase3 = db.pool.get().await.expect("connection");
        let txn = phase3.transaction().await.expect("begin phase 3");
        let outcome = apply::apply_and_mark_drained(
            &txn,
            seg,
            "worker_hopgen",
            &plan,
            "trellis_reverse_test",
            &unstaged_watermark,
        )
        .await
        .unwrap_or_else(|e| {
            panic!("round {round}: apply must never fail with HopBoundExceeded (or any other error): {e}")
        });
        txn.commit().await.expect("commit");
        retire_drained_segments(&mut client)
            .await
            .expect("retire drained segments");

        assert_eq!(
            outcome.fairness_escalations, 0,
            "round {round}: below the fairness threshold ({threshold}), so this must \
             be an ordinary deferral, not an escalation"
        );
        let deferred = staged_deferred_reverses(&client, relationship.id).await;
        assert_eq!(
            deferred,
            vec![("1".to_string(), round, 0)],
            "round {round}: retry_count must climb to exactly {round}, and \
             hop_gen must stay at 0 — issue #134's whole point is that this \
             counter is independent of, and never burns, hop_gen"
        );
    }

    // The threshold-th rejection: `retry_count` going in is `threshold - 1`,
    // so this attempt's `retry_count + 1 == threshold` — the escalation
    // branch fires instead of yet another deferral.
    let seg = seal_active_segment(&mut client).await;
    let plan = claim_fold_compute(&db.pool, seg, "worker_hopgen").await;
    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3");
    let outcome = apply::apply_and_mark_drained(
        &txn,
        seg,
        "worker_hopgen",
        &plan,
        "trellis_reverse_test",
        &unstaged_watermark,
    )
    .await
    .expect("apply must succeed — escalation, not an error, is how this resolves");
    txn.commit().await.expect("commit");
    retire_drained_segments(&mut client)
        .await
        .expect("retire drained segments");

    assert_eq!(
        outcome.fairness_escalations, 1,
        "round {threshold}: exactly one reverse transition must escalate — guard (a) \
         is still failing (the watermark never advanced), so this can only be the \
         fairness mechanism, not a natural pass"
    );
    assert!(
        staged_deferred_reverses(&client, relationship.id)
            .await
            .is_empty(),
        "escalation must consume the deferred row — nothing left to retry"
    );
    assert_eq!(
        staged_recompute_keys(&client, "post_tags").await,
        vec!["10".to_string(), "12".to_string()],
        "escalation must stage the pre-#131 fallback recompute for every from-side \
         row currently under post 1 (post_tags 10 and 12), the same always-correct \
         mechanism a guard rejection used before #134 introduced deferral"
    );
    assert_eq!(
        projection_lsn(&client, &projection_table, 1).await,
        Some(PgLsn::from(100)),
        "escalation must advance the settled parent projection to this transition's \
         own lsn immediately, even though guard (a) — which does not gate the \
         projection write, only the fast-path delta — is still failing"
    );

    retire_drained_segments(&mut client)
        .await
        .expect("retire drained segments");
    drain_to_quiescence(&db.pool, &mut client).await;

    let totals = target_totals(&client).await;
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("3".to_string()), Some("650".to_string()))),
        "the pipeline must still converge on post 1's true value (400) via the \
         escalated recompute, even though guard (a) never once passed"
    );
}

/// Issue #135's own crux: a **liveness** model, not just a correctness one.
/// The existing generative/ablation campaign (`issue-102-PLAN-DRAFT.md` §7
/// step 7, §10) only ever measures state *after* a workload stops, so it
/// cannot observe starvation by construction. This test simulates a hot
/// parent (`posts` row 1) whose own value changes exactly once, while its
/// children (`post_tags`) churn continuously — a brand-new child lands in
/// *every single drain round*, sustained for longer than the fairness
/// threshold, with no quiet round ever engineered in on purpose.
///
/// Before issue #135, this is exactly the starvation shape: every round's
/// own freshly-staged, freshly-forward-evaluated child bumps the parent's
/// projection `gen` inside the *same* transaction as the reverse's guard
/// (b) re-check (see `staging::apply`'s "Issue #135" design section for
/// why that race is structural, not probabilistic, here — nothing about it
/// depends on real concurrency or timing luck), so guard (b) fails
/// deterministically, round after round, for as long as churn continues.
/// A `retry_count`-only mechanism with no bound would defer forever under
/// this exact load.
///
/// Asserts liveness with an explicit, checked upper bound — the reverse
/// must escalate at or before round
/// [`apply::RELATIONSHIP_REVERSE_FAIRNESS_THRESHOLD`], not merely
/// "eventually" — while churn continues both during and after that round,
/// and the parent's own transition still converges to the fully correct
/// final state once every staged recompute drains.
#[tokio::test]
async fn a_hot_parent_under_sustained_child_churn_still_resolves_within_the_fairness_bound() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    let projection_table = projection_table_for(&db.pool, relationship.id).await;

    // The parent's own single, real edit — the transition the reverse must
    // eventually apply, despite everything below.
    client
        .execute("update posts set word_count = 400 where id = 1", &[])
        .await
        .expect("update the related post");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
        100,
    )
    .await;

    // Guard (a) is saturated on purpose — this test isolates the churn-driven
    // failure modes (guards (b)/(c)), not the watermark barrier, which the
    // threshold test above already covers in isolation.
    let watermark = StagedWatermark::saturated();
    let threshold = apply::RELATIONSHIP_REVERSE_FAIRNESS_THRESHOLD;
    let mut next_child_id = 40i32;
    let mut resolved_at_round: Option<i32> = None;

    async fn insert_churn_child(client: &Client, id: i32) {
        client
            .execute(
                &format!("insert into post_tags (id, post, tag) values ({id}, 1, 'churn')"),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("insert churn child {id}: {e}"));
        stage_cdc_at_lsn(
            client,
            "post_tags",
            &id.to_string(),
            "insert",
            None,
            Some(&format!("{{\"id\":{id},\"post\":1,\"tag\":\"churn\"}}")),
            200 + u64::try_from(id).expect("id is non-negative"),
        )
        .await;
    }

    // Sustained churn: continue for a few rounds *past* the threshold too,
    // so the test would visibly fail (never finding `fairness_escalations >
    // 0`, then panicking below) if the mechanism only happened to work for
    // a lucky handful of rounds rather than actually bounding the wait.
    for round in 1..=(threshold + 3) {
        insert_churn_child(&client, next_child_id).await;
        next_child_id += 1;

        let seg = seal_active_segment(&mut client).await;
        let plan = claim_fold_compute(&db.pool, seg, "worker_churn").await;
        let mut phase3 = db.pool.get().await.expect("connection");
        let txn = phase3.transaction().await.expect("begin phase 3");
        let outcome = apply::apply_and_mark_drained(
            &txn,
            seg,
            "worker_churn",
            &plan,
            "trellis_reverse_test",
            &watermark,
        )
        .await
        .unwrap_or_else(|e| panic!("round {round}: apply must not fail: {e}"));
        txn.commit().await.expect("commit");
        retire_drained_segments(&mut client)
            .await
            .expect("retire drained segments");

        if outcome.fairness_escalations > 0 {
            resolved_at_round = Some(round);
            break;
        }

        // Still pending, still churning — the property that would be false
        // without issue #135's fix: under #134 alone this could spin past
        // `threshold` indefinitely, since every round supplies its own
        // fresh guard (b) failure.
        assert!(
            !staged_deferred_reverses(&client, relationship.id)
                .await
                .is_empty(),
            "round {round}: the reverse must still be pending (deferred), not \
             silently dropped"
        );
        assert!(
            round < threshold,
            "round {round}: exceeded the fairness threshold ({threshold}) without \
             escalating — this is exactly the starvation issue #135 exists to rule out"
        );
    }

    let resolved_at_round = resolved_at_round.unwrap_or_else(|| {
        panic!(
            "the reverse never escalated within {} rounds under sustained churn — \
             starvation freedom is broken",
            threshold + 3
        )
    });
    assert!(
        resolved_at_round <= threshold,
        "escalation must happen at or before round {threshold} (the fairness \
         threshold), not round {resolved_at_round}"
    );
    assert!(
        staged_deferred_reverses(&client, relationship.id)
            .await
            .is_empty(),
        "escalation must consume the deferred row"
    );
    assert_eq!(
        projection_lsn(&client, &projection_table, 1).await,
        Some(PgLsn::from(100)),
        "escalation must advance the projection to the parent's true final value, \
         even though it took several rounds of sustained child churn to get there"
    );

    // A few more churn rounds *after* resolution — proving the system stays
    // live afterward too, not merely that it eventually gives up once.
    for _ in 0..2 {
        insert_churn_child(&client, next_child_id).await;
        next_child_id += 1;
    }

    drain_to_quiescence(&db.pool, &mut client).await;

    // Final correctness: the original transition (post 1's rust/db tags)
    // converged exactly as it would with no churn at all —
    let totals = target_totals(&client).await;
    assert_eq!(
        totals.get("rust"),
        Some(&(Some("3".to_string()), Some("650".to_string()))),
        "post 1's real transition must still converge correctly (400, not a stale \
         100) despite continuous, unrelated child churn throughout"
    );

    // — and every churn child (whether it landed before or after escalation)
    // is counted exactly once, against the parent's *true* final value: proof
    // the fallback recompute this mechanism triggers is not merely
    // non-corrupting but actually self-correcting for churn that arrived
    // while the parent's own projection was still stale.
    let churn_count = i64::from(next_child_id - 40);
    let (churn_post_count, churn_total_words) =
        totals.get("churn").expect("the churn tag group exists");
    assert_eq!(
        churn_post_count.as_deref(),
        Some(churn_count.to_string().as_str()),
        "every churn child must be counted exactly once, regardless of whether it \
         raced the parent's own pending transition"
    );
    assert_eq!(
        churn_total_words.as_deref(),
        Some((400 * churn_count).to_string().as_str()),
        "every churn child's contribution must reflect post 1's true final \
         word_count (400), including the ones evaluated before escalation against \
         the still-stale projection value — proof the fallback recompute this \
         mechanism relies on is self-correcting, not merely non-corrupting"
    );
}

/// Issue #135's own observability requirement (the plan doc's "the deferral
/// counters should become engine metrics so it is observable in
/// production" — already true for `d5_block_*` since #134; this is that
/// same requirement for the fairness mechanism itself): a fairness
/// escalation must be independently visible on
/// `trellis_relationship_reverse_fairness_escalated_total`, a separate
/// series from `trellis_relationship_reverse_deferred_total` (see
/// `metrics.rs`'s own doc comment on why the two are not one metric with an
/// extra label value). Drives the same stuck-guard-(a) scenario as
/// `deferring_past_the_fairness_threshold_escalates_instead_of_spinning_forever`,
/// but checks the metric directly rather than `ApplyOutcome`'s own field
/// (already covered there) — this test's job is the registry plumbing, not
/// the escalation decision itself.
#[tokio::test]
async fn fairness_escalation_increments_its_own_metric() {
    const METRIC: &str = "trellis_relationship_reverse_fairness_escalated_total";

    fn metric_value(rendered: &str) -> u64 {
        rendered
            .lines()
            .find(|line| line.starts_with(METRIC))
            .and_then(|line| line.rsplit(' ').next())
            .and_then(|v| v.parse().ok())
            .unwrap_or(0)
    }

    let before = metric_value(&trellis::metrics::Metrics::new().render_prometheus());

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
    drain_to_quiescence(&db.pool, &mut client).await;
    client
        .execute("update posts set word_count = 400 where id = 1", &[])
        .await
        .expect("update");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
        100,
    )
    .await;

    let unstaged_watermark = StagedWatermark::new();
    let threshold = apply::RELATIONSHIP_REVERSE_FAIRNESS_THRESHOLD;
    let mut escalated = false;
    for _ in 1..=threshold {
        let seg = seal_active_segment(&mut client).await;
        let plan = claim_fold_compute(&db.pool, seg, "worker_metric_fair").await;
        let mut phase3 = db.pool.get().await.expect("connection");
        let txn = phase3.transaction().await.expect("begin phase 3");
        let outcome = apply::apply_and_mark_drained(
            &txn,
            seg,
            "worker_metric_fair",
            &plan,
            "trellis_reverse_test",
            &unstaged_watermark,
        )
        .await
        .expect("apply");
        txn.commit().await.expect("commit");
        retire_drained_segments(&mut client)
            .await
            .expect("retire drained segments");

        // Mirrors `staging::apply::flush_relationship_reverse_fairness_escalation_metric`
        // (private to that crate) by hand — the same caller responsibility
        // `each_guard_increments_its_own_deferral_metric` discharges for
        // `deferral_counts`.
        for _ in 0..outcome.fairness_escalations {
            trellis::metrics::increment_relationship_reverse_fairness_escalated();
        }
        if outcome.fairness_escalations > 0 {
            escalated = true;
            break;
        }
    }
    assert!(
        escalated,
        "the stuck-guard-(a) scenario must escalate within {threshold} rounds"
    );

    let after = metric_value(&trellis::metrics::Metrics::new().render_prometheus());
    assert!(
        after > before,
        "the fairness-escalation metric must have advanced by at least one \
         (before={before}, after={after}) — it may be more than one if a \
         concurrently-running sibling test also escalated, which is expected \
         and not itself a failure"
    );
}

/// Doc 05's property 1 ("the claimed batch is immutable... every producer
/// writes to the *active* batch, including a worker doing downstream
/// propagation") applied to issue #134's new producer: a guard rejection's
/// re-staged `rel_reverse_deferred` row must land in whichever segment is
/// *currently accepting new appends* — never retroactively inserted into
/// the segment that's mid-drain (the one whose guard just rejected it).
/// Proven directly against the ring's physical tables, not inferred from
/// convergence: the deferred row is present in the segment that was
/// *active* at the moment of rejection, and absent from the segment that
/// was actually being drained.
#[tokio::test]
async fn a_deferred_reverse_lands_in_the_active_segment_never_the_draining_one() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    client
        .execute("update posts set word_count = 400 where id = 1", &[])
        .await
        .expect("update the related post");
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
        100,
    )
    .await;
    let seg = seal_active_segment(&mut client).await;
    let plan = claim_fold_compute(&db.pool, seg, "worker_active_batch").await;

    // `seg` is now sealed (about to be claimed/drained below); whatever
    // ring slot is active *now* is a different, later segment — captured
    // before the apply so this test can independently verify against both
    // physical tables afterward, not just infer it from one lookup.
    let active_table_before = active_seg_table(&client).await;
    let seg_ring_slot: i16 = client
        .query_one("select ring_slot from segments where seg_seq = $1", &[&seg])
        .await
        .expect("read seg's own ring_slot")
        .get(0);
    let seg_table = format!("seg_{seg_ring_slot}");
    assert_ne!(
        active_table_before, seg_table,
        "sanity: the sealed segment being drained and the currently-active \
         one must be different physical tables"
    );

    let unstaged_watermark = StagedWatermark::new();
    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3");
    apply::apply_and_mark_drained(
        &txn,
        seg,
        "worker_active_batch",
        &plan,
        "trellis_reverse_test",
        &unstaged_watermark,
    )
    .await
    .expect("apply (rejected internally by guard (a))");
    txn.commit().await.expect("commit");

    assert_eq!(
        staged_deferred_reverses(&client, relationship.id).await,
        vec![("1".to_string(), 1, 0)],
        "the deferred row must be present in the currently-active segment"
    );
    assert_eq!(
        deferred_reverses_in_segment(&client, seg, relationship.id).await,
        Vec::<(String, i32, i32)>::new(),
        "the deferred row must be absent from the segment whose guard \
         rejection produced it — re-staging into the batch being drained \
         (rather than the active one) would violate doc 05's property 1"
    );
}

/// The plan doc's `d5_block_*` counters (issue #134's own §7 step 6/step 7
/// naming), one dedicated scenario per guard. Two layers, deliberately:
///
/// 1. **`ApplyOutcome::deferral_counts` itself** — a plain struct field,
///    populated by Phase 3 and returned from `apply_and_mark_drained`
///    regardless of what its caller does next. Asserting on this directly
///    is deterministic and has zero shared/concurrent state to race
///    against — this is the primary, always-reliable check that Phase 3
///    attributed the rejection to the right guard.
/// 2. **The metrics registry itself** — `apply_and_mark_drained`'s own
///    callers are responsible for flushing `deferral_counts` into
///    `trellis::metrics::increment_relationship_reverse_deferred` only
///    *after* their own commit succeeds (`drain_once`/`drain_many` do this
///    internally via `flush_relationship_reverse_deferral_metrics`, a
///    private helper — this test, calling `apply_and_mark_drained`
///    directly the same way every other scenario in this file does,
///    reproduces that exact same post-commit flush call by hand). This is
///    what actually proves the wiring reaches Prometheus, not just that
///    Phase 3's own bookkeeping is correct.
///
/// The registry is process-wide (`metrics.rs`'s own module doc comment) —
/// shared with every other test in this binary, run in parallel by
/// default, including several *other* tests in this same file that also
/// trigger a guard rejection (and so also bump one of these same four
/// labels). Review follow-up to this issue confirmed an earlier version of
/// layer 2 here — asserting the *other three* labels stayed at an exact
/// prior value across the one label under test — flaky under that
/// concurrency: a sibling test's own rejection landing inside this test's
/// narrow before/after window is indistinguishable, on a shared global
/// counter, from a real bug. Narrowed to what a shared, concurrently-written
/// counter can actually support without a redesign of the (deliberately
/// process-wide, ADR-0009) metrics registry or serializing this file's
/// tests against each other: layer 2 asserts only that *its own* label's
/// delta is **at least** one (never exactly one, and never that the other
/// three stayed put) — robust to any amount of concurrent noise from
/// sibling tests, and, since a delta can only ever move *up*, never a
/// false negative. `metric_label`'s own per-variant mapping is separately
/// unit-tested, with zero concurrency exposure at all, in `staging::apply`'s
/// own test module
/// (`reverse_guard_failure_metric_labels_match_the_plan_docs_own_d5_block_names`).
#[tokio::test]
async fn each_guard_increments_its_own_deferral_metric() {
    const METRIC: &str = "trellis_relationship_reverse_deferred_total";

    async fn value_for(label: &str) -> u64 {
        let rendered = trellis::metrics::Metrics::new().render_prometheus();
        counter_value(&rendered, METRIC, "guard", label)
    }

    /// Mirrors `staging::apply::flush_relationship_reverse_deferral_metrics`
    /// (private to that crate) by hand — any direct caller of
    /// `apply_and_mark_drained`/`_many` (this test included) owns this same
    /// post-commit-only responsibility `drain_once`/`drain_many` discharge
    /// internally.
    fn flush(deferral_counts: &std::collections::HashMap<&'static str, u64>) {
        for (guard, count) in deferral_counts {
            for _ in 0..*count {
                trellis::metrics::increment_relationship_reverse_deferred(guard);
            }
        }
    }

    async fn assert_label_advanced_by_at_least_one(before: u64, label: &str) {
        let after = value_for(label).await;
        assert!(
            after > before,
            "label {label:?} must have advanced by at least one \
             (before={before}, after={after}) — it may be more than one if \
             a concurrently-running sibling test also rejected on this \
             same guard, which is expected and not itself a failure"
        );
    }

    // Guard (a): watermark barrier.
    {
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
        drain_to_quiescence(&db.pool, &mut client).await;
        client
            .execute("update posts set word_count = 400 where id = 1", &[])
            .await
            .expect("update");
        stage_cdc_at_lsn(
            &client,
            "posts",
            "1",
            "update",
            Some("{\"id\":1,\"word_count\":100}"),
            Some("{\"id\":1,\"word_count\":400}"),
            100,
        )
        .await;
        let seg = seal_active_segment(&mut client).await;
        let plan = claim_fold_compute(&db.pool, seg, "worker_metric_a").await;

        let before = value_for("d5_block_barrier").await;
        let mut phase3 = db.pool.get().await.expect("connection");
        let txn = phase3.transaction().await.expect("begin phase 3");
        let outcome = apply::apply_and_mark_drained(
            &txn,
            seg,
            "worker_metric_a",
            &plan,
            "trellis_reverse_test",
            &StagedWatermark::new(),
        )
        .await
        .expect("apply (rejected by guard a)");
        txn.commit().await.expect("commit");
        assert_eq!(
            outcome.deferral_counts.get("d5_block_barrier"),
            Some(&1),
            "Phase 3 must attribute this rejection to guard (a)'s own label"
        );
        flush(&outcome.deferral_counts);
        assert_label_advanced_by_at_least_one(before, "d5_block_barrier").await;
    }

    // Guard (b): generation check.
    {
        let cluster = TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let mut client = connect_raw(db.dsn()).await;
        create_schema(&client).await;
        let relationship = create_relationship(
            &db.pool,
            "RELATIONSHIP post FROM post_tags.post TO posts.id",
        )
        .await
        .expect("create to-one relationship");
        install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
            .await
            .expect("install the aggregate-over-to-one definition");
        drain_to_quiescence(&db.pool, &mut client).await;
        let projection_table = projection_table_for(&db.pool, relationship.id).await;
        client
            .execute("update posts set word_count = 400 where id = 1", &[])
            .await
            .expect("update");
        stage_cdc_at_lsn(
            &client,
            "posts",
            "1",
            "update",
            Some("{\"id\":1,\"word_count\":100}"),
            Some("{\"id\":1,\"word_count\":400}"),
            100,
        )
        .await;
        let seg = seal_active_segment(&mut client).await;
        let plan = claim_fold_compute(&db.pool, seg, "worker_metric_b").await;
        client
            .execute(
                &format!(
                    "update {projection_table} set __trellis_gen = __trellis_gen + 1 where id = 1"
                ),
                &[],
            )
            .await
            .expect("simulate a concurrent forward apply");

        let before = value_for("d5_block_gen").await;
        let mut phase3 = db.pool.get().await.expect("connection");
        let txn = phase3.transaction().await.expect("begin phase 3");
        let outcome = apply::apply_and_mark_drained(
            &txn,
            seg,
            "worker_metric_b",
            &plan,
            "trellis_reverse_test",
            &StagedWatermark::saturated(),
        )
        .await
        .expect("apply (rejected by guard b)");
        txn.commit().await.expect("commit");
        assert_eq!(
            outcome.deferral_counts.get("d5_block_gen"),
            Some(&1),
            "Phase 3 must attribute this rejection to guard (b)'s own label"
        );
        flush(&outcome.deferral_counts);
        assert_label_advanced_by_at_least_one(before, "d5_block_gen").await;
    }

    // Guard (c): in-flight check.
    {
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
        drain_to_quiescence(&db.pool, &mut client).await;
        client
            .execute("update posts set word_count = 400 where id = 1", &[])
            .await
            .expect("update");
        stage_cdc_at_lsn(
            &client,
            "posts",
            "1",
            "update",
            Some("{\"id\":1,\"word_count\":100}"),
            Some("{\"id\":1,\"word_count\":400}"),
            100,
        )
        .await;
        let seg = seal_active_segment(&mut client).await;
        client
            .execute(
                "insert into post_tags (id, post, tag) values (30, 1, 'rust')",
                &[],
            )
            .await
            .expect("insert a new post_tags row pointing at post 1");
        stage_cdc_at_lsn(
            &client,
            "post_tags",
            "30",
            "insert",
            None,
            Some("{\"id\":30,\"post\":1,\"tag\":\"rust\"}"),
            50,
        )
        .await;
        let plan = claim_fold_compute(&db.pool, seg, "worker_metric_c").await;

        let before = value_for("d5_block_inflight").await;
        let mut phase3 = db.pool.get().await.expect("connection");
        let txn = phase3.transaction().await.expect("begin phase 3");
        let outcome = apply::apply_and_mark_drained(
            &txn,
            seg,
            "worker_metric_c",
            &plan,
            "trellis_reverse_test",
            &StagedWatermark::saturated(),
        )
        .await
        .expect("apply (rejected by guard c)");
        txn.commit().await.expect("commit");
        assert_eq!(
            outcome.deferral_counts.get("d5_block_inflight"),
            Some(&1),
            "Phase 3 must attribute this rejection to guard (c)'s own label"
        );
        flush(&outcome.deferral_counts);
        assert_label_advanced_by_at_least_one(before, "d5_block_inflight").await;
    }

    // Guard (d): per-parent ordering (the stale-prev_lsn scenario).
    {
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
        drain_to_quiescence(&db.pool, &mut client).await;

        client
            .execute("update posts set word_count = 400 where id = 1", &[])
            .await
            .expect("first update");
        stage_cdc_at_lsn(
            &client,
            "posts",
            "1",
            "update",
            Some("{\"id\":1,\"word_count\":100}"),
            Some("{\"id\":1,\"word_count\":400}"),
            100,
        )
        .await;
        let seg_a = seal_active_segment(&mut client).await;
        let plan_a = claim_fold_compute(&db.pool, seg_a, "worker_metric_d_a").await;

        client
            .execute("update posts set word_count = 500 where id = 1", &[])
            .await
            .expect("second update");
        stage_cdc_at_lsn(
            &client,
            "posts",
            "1",
            "update",
            Some("{\"id\":1,\"word_count\":400}"),
            Some("{\"id\":1,\"word_count\":500}"),
            200,
        )
        .await;
        let seg_b = seal_active_segment(&mut client).await;
        let plan_b = claim_fold_compute(&db.pool, seg_b, "worker_metric_d_b").await;

        let mut phase3 = db.pool.get().await.expect("connection");
        let txn = phase3.transaction().await.expect("begin phase 3 (a)");
        apply::apply_and_mark_drained(
            &txn,
            seg_a,
            "worker_metric_d_a",
            &plan_a,
            "trellis_reverse_test",
            &StagedWatermark::saturated(),
        )
        .await
        .expect("apply A");
        txn.commit().await.expect("commit A");

        let before = value_for("d5_block_order").await;
        let mut phase3 = db.pool.get().await.expect("connection");
        let txn = phase3.transaction().await.expect("begin phase 3 (b)");
        let outcome = apply::apply_and_mark_drained(
            &txn,
            seg_b,
            "worker_metric_d_b",
            &plan_b,
            "trellis_reverse_test",
            &StagedWatermark::saturated(),
        )
        .await
        .expect("apply B (rejected by guard d)");
        txn.commit().await.expect("commit B");
        assert_eq!(
            outcome.deferral_counts.get("d5_block_order"),
            Some(&1),
            "Phase 3 must attribute this rejection to guard (d)'s own label"
        );
        flush(&outcome.deferral_counts);
        assert_label_advanced_by_at_least_one(before, "d5_block_order").await;
    }
}

/// Review follow-up to issue #134: the `retry_count > 0` restriction alone
/// (this module's first attempt at closing the old `force_every_group`-vs-
/// `diff_pass` staleness hazard) was too narrow — it only ever protected a
/// record that was *itself* previously guard-rejected. This test originally
/// reproduced that hazard directly: `force_every_group` (removed by issue
/// #136, epic #127) used to make an *ordinary* sibling insert's own forward
/// evaluation do a **live** SQL join straight back to the to-side table, so
/// a sibling that drained between the parent's live write and the parent's
/// own CDC being staged would settle the whole group to the *already
/// updated* live total — an "old" era `diff_pass` could then wrongly
/// re-add a delta on top of.
///
/// **Post-#136**, an ordinary from-side insert to a relationship-reading
/// aggregate never live-reads the to-side table at all: it resolves the
/// relationship the same way a `KeySpace::OneToOne` target already did
/// (issue #130's settled parent projection), so the sibling below now
/// settles 'rust' using the *old*, still-unbumped projection value (100),
/// never the live-updated one (400) — the specific staleness hazard this
/// test used to reproduce is no longer reachable through this path at all
/// (see `apply_aggregate`'s module doc comment's "Relationship-reading
/// aggregates" section). What survives, and what this test now proves
/// instead: `relationship_fast_path_precondition_holds` (issue #134's own
/// review follow-up) does not know *why* a sibling row touched this parent
/// key — it conservatively treats any matching CDC row in the LSN window as
/// disqualifying, whether or not that row could actually have raced this
/// reverse — so it still routes this record to the pre-#131 image-less
/// fallback exactly as before, and that fallback must still converge to the
/// true total. This is deliberately left as-is (more conservative than
/// strictly necessary post-#136), not tightened here: doing so would need
/// `relationship_fast_path_precondition_holds` to distinguish "a sibling
/// that resolved via the safe settled-projection path" from "one that could
/// have live-read a mid-flight value," which no longer exists as a
/// distinction to draw now that no forward path does the latter — a
/// possible follow-up simplification, not a correctness gap (the fallback
/// this routes to is always correct, just more conservative than it now
/// strictly needs to be).
///
/// `relationship_fast_path_precondition_holds` scans every physical ring
/// row — not just still-undrained ones — for a sibling touching this key,
/// so the *already-drained-but-not-yet-retired* sibling here still leaves a
/// trace it can find (this test deliberately never calls
/// `retire_drained_segments`, so that trace survives — see that function's
/// own doc comment for the residual gap once retirement *does* run).
#[tokio::test]
async fn a_sibling_that_already_drained_before_the_parents_own_cdc_is_staged_does_not_corrupt_a_first_attempt()
 {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema(&client).await;

    let relationship = create_relationship(
        &db.pool,
        "RELATIONSHIP post FROM post_tags.post TO posts.id",
    )
    .await
    .expect("create to-one relationship");
    install_definition(&db.pool, TAG_TOTALS, &post_tags_columns(), "public")
        .await
        .expect("install the aggregate-over-to-one definition");
    drain_to_quiescence(&db.pool, &mut client).await;

    // This test's own lsns must be real, monotonically-increasing WAL
    // positions, not this file's usual small hand-picked placeholders
    // (`100`, `200`, ...): `record.prev_lsn` (the lower bound
    // `relationship_fast_path_precondition_holds` checks the sibling's own
    // lsn against) comes from the settled parent projection's
    // `__trellis_lsn`, which backfill stamps from a *real*
    // `pg_current_wal_lsn()` at relationship-creation time — a large value
    // already, by the time this test's own staging starts, from all the
    // real WAL activity `create_schema`/`create_relationship`/
    // `drain_to_quiescence` above generate. A hand-picked small lsn for the
    // sibling below would (harmlessly, but misleadingly) sit *below* that
    // baseline and never satisfy the "committed after the projection's own
    // last-known-good position" test this function's own doc comment
    // describes — masking the very hazard this test exists to prove closed.
    let base: u64 = u64::from(
        client
            .query_one("select pg_current_wal_lsn()", &[])
            .await
            .expect("read the current wal lsn")
            .get::<_, PgLsn>(0),
    );

    // The parent's *live* row is updated for real, but its CDC is not
    // staged yet — mirroring "the reverse hasn't even been enumerated" at
    // this point.
    client
        .execute("update posts set word_count = 400 where id = 1", &[])
        .await
        .expect("live update the related post");

    // A sibling from-side row, inserted and *fully drained* before the
    // parent's own CDC ever lands: post-#136, its own forward evaluation
    // resolves post 1's word_count from the settled parent projection (still
    // 100 — nothing has bumped it yet), never a live read of the
    // already-updated `posts` row (400).
    client
        .execute(
            "insert into post_tags (id, post, tag) values (40, 1, 'rust')",
            &[],
        )
        .await
        .expect("insert a new post_tags row pointing at post 1");
    stage_cdc_at_lsn(
        &client,
        "post_tags",
        "40",
        "insert",
        None,
        Some("{\"id\":40,\"post\":1,\"tag\":\"rust\"}"),
        base + 50,
    )
    .await;
    let sibling_seg = seal_active_segment(&mut client).await;
    let watermark = StagedWatermark::saturated();
    while apply::drain_once(
        &db.pool,
        sibling_seg,
        "worker_sibling",
        1,
        "trellis_reverse_test",
        &watermark,
    )
    .await
    .expect("drain_once sibling_seg")
    .is_some()
    {}
    // Deliberately *not* calling `retire_drained_segments` here: this test
    // is the case `relationship_fast_path_precondition_holds` is meant to
    // catch (the sibling's ring row is still physically present, just
    // marked drained) — see that function's own doc comment for why an
    // already-*retired* sibling is a different, still-open story.

    let totals_before_parent_cdc = target_totals(&client).await;
    assert_eq!(
        totals_before_parent_cdc.get("rust"),
        Some(&(Some("4".to_string()), Some("450".to_string()))),
        "sanity: the sibling's own forward delta must have resolved post 1's \
         relationship value from the settled projection (still 100, via row \
         10 + the new row 40) + 250 via post 2's row 11 + null via post \
         999's row 13 = 450 — *not* the live-updated 400 — before the \
         parent's own CDC is ever staged"
    );

    // *Now* stage the parent's own CDC — first attempt, never rejected.
    stage_cdc_at_lsn(
        &client,
        "posts",
        "1",
        "update",
        Some("{\"id\":1,\"word_count\":100}"),
        Some("{\"id\":1,\"word_count\":400}"),
        base + 100,
    )
    .await;
    let seg = seal_active_segment(&mut client).await;
    let plan = claim_fold_compute(&db.pool, seg, "worker_parent").await;

    let mut phase3 = db.pool.get().await.expect("connection");
    let txn = phase3.transaction().await.expect("begin phase 3");
    apply::apply_and_mark_drained(
        &txn,
        seg,
        "worker_parent",
        &plan,
        "trellis_reverse_test",
        &watermark,
    )
    .await
    .expect("apply the parent's reverse");
    txn.commit().await.expect("commit");

    // Guards (a)/(b)/(c)/(d) all genuinely pass here (nothing rejects the
    // record outright — the sibling is *not* in flight by guard (c)'s own,
    // unchanged "still undrained" definition), so this never becomes a
    // `rel_reverse_deferred` row at all. It's the fast-path-vs-fallback
    // decision, one level deeper, that must route to the fallback: the
    // whole `aggregate_shapes` loop is skipped for this record (not just
    // the touched 'rust' group), so *every* from-side row matching post 1
    // — rows 10 and 12 ('rust' and 'db' respectively) and the new row 40 —
    // gets an image-less `Recompute`, the same shape
    // `needs_recompute_fallback` already uses.
    let mut recompute_keys = staged_recompute_keys(&client, "post_tags").await;
    recompute_keys.sort();
    assert_eq!(
        recompute_keys,
        vec!["10".to_string(), "12".to_string(), "40".to_string()],
        "the fast path's own extra precondition must have found the \
         already-drained sibling and routed this first attempt to the \
         fallback instead of trusting a stale diff"
    );
    assert!(
        staged_deferred_reverses(&client, relationship.id)
            .await
            .is_empty(),
        "no guard actually rejected this record — only the fast-path's own \
         extra precondition did — so nothing should be staged as \
         rel_reverse_deferred here"
    );

    // The reverse's own delta write was routed to the fallback (not
    // `diff_pass`), so nothing has corrected 'rust' yet at this point —
    // it's still the pre-fallback, sibling-only total from above, not yet
    // double-corrected *or* under-corrected. The re-staged `Recompute`s
    // (rows 10/12/40, asserted above) are what bring it to the true final
    // value once they drain, below.
    let totals_after = target_totals(&client).await;
    assert_eq!(
        totals_after.get("rust"),
        Some(&(Some("4".to_string()), Some("450".to_string()))),
        "the fallback's own image-less Recomputes haven't drained yet (that \
         happens below), so the group must still read exactly the same as \
         it did right after the sibling's own forward apply — no partial or \
         double correction from this transaction's own reverse write, which \
         was routed entirely to the fallback"
    );
    let projection_table = projection_table_for(&db.pool, relationship.id).await;
    assert_eq!(
        projection_lsn(&client, &projection_table, 1).await,
        Some(PgLsn::from(base + 100)),
        "guards all genuinely passed, so the projection must still advance \
         to this record's own lsn — only the *fast-path write* was routed \
         to the fallback, not the whole record"
    );

    // The re-staged `Recompute`s for rows 10/12/40 must still drain
    // cleanly — idempotent for 'rust' (already correct) and the actual
    // correction for 'db' (row 12, untouched by anything until now).
    retire_drained_segments(&mut client)
        .await
        .expect("retire drained segments");
    drain_to_quiescence(&db.pool, &mut client).await;
    let totals_final = target_totals(&client).await;
    assert_eq!(
        totals_final.get("rust"),
        Some(&(Some("4".to_string()), Some("1050".to_string()))),
        "still correct after the fallback's own idempotent recomputes drain"
    );
    assert_eq!(
        totals_final.get("db"),
        Some(&(Some("2".to_string()), Some("400".to_string()))),
        "'db' (row 12's group, never touched by the sibling's own forward \
         apply) must converge too: 400 (post 1, its true final value) + \
         null (post 3)"
    );
}

// ---------------------------------------------------------------------
// Issue #244: an image-less `Recompute` on the *to-side* table is not a
// parent state transition.
// ---------------------------------------------------------------------

/// Stages one image-less `recompute` trigger into the active ring segment
/// — the exact row shape `intake::publication::enumerate_and_append` (a
/// definition's ring backfill), forward propagation's chained-target hop,
/// and the reverse/TRUNCATE-clear fallbacks all append: no images, no
/// `lsn`, only "this key exists as of now".
async fn stage_recompute(client: &Client, src_table: &str, key: &str) {
    let src_table = qualify_fixture_table(src_table);
    let table = active_seg_table(client).await;
    client
        .execute(
            &format!(
                "insert into {table} (src_table, key, op, hop_gen) values ($1, $2, 'recompute', 0)"
            ),
            &[&src_table, &key],
        )
        .await
        .unwrap_or_else(|e| panic!("stage recompute {key:?} into {table} failed: {e}"));
}

/// The oracle: an independently-authored SQL `GROUP BY` over the live base
/// tables, never the engine's own evaluator (ADR-0013). Deliberately
/// written as the plain left-join aggregate a human would write by hand for
/// [`TAG_TOTALS`], so agreeing with it is real evidence rather than a
/// tautology.
async fn tag_totals_oracle(client: &Client) -> Totals {
    client
        .query(
            "select pt.tag, count(*)::text, sum(p.word_count)::text \
             from post_tags pt left join posts p on p.id = pt.post \
             group by pt.tag",
            &[],
        )
        .await
        .expect("read the hand-written GROUP BY oracle")
        .into_iter()
        .map(|r| (r.get(0), (r.get(1), r.get(2))))
        .collect()
}

/// **Issue #244.** An image-less `StagedChange::Recompute` staged against a
/// relationship's *to-side* table asserts nothing about that row's state —
/// it only says "this key exists as of now" (see
/// `intake::publication::enumerate_and_append`'s doc comment). It is what a
/// definition's ring backfill enumeration stages for its own source table,
/// what forward propagation stages for a chained target, and what
/// TRUNCATE-clear/reverse-fallback stage for a from-side row — and that
/// from-side row's table is very often *also* some other relationship's
/// to-side, which is how the generative suite hit this without any
/// definition being anchored on the parent at all.
///
/// Before this issue's fix, `compute`'s to-one reverse loop built a
/// [`trellis::staging::apply::ApplyPlan`] record for such a change with
/// `old_row = None` (no pre-image to decode) and `new_row = Some(..)` — the
/// live re-read `compute` does for every image-less trigger — which is
/// byte-for-byte the shape of a genuine **parent INSERT**. The reverse
/// delta path then added the parent's contribution to every matching
/// from-side row's group a second time, on top of the contribution already
/// folded in when the parent was first seen: a silent 2x `SUM`.
///
/// The `both images absent` skip that was supposed to catch this tested
/// the *decoded rows*, not the change's own images, so it only ever fired
/// when the live re-read also came back empty (a row that no longer
/// exists) — never for the overwhelmingly common case of a row that is
/// still there.
#[tokio::test]
async fn an_image_less_recompute_on_the_to_side_table_is_not_a_parent_insert() {
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
    drain_to_quiescence(&db.pool, &mut client).await;

    let settled = target_totals(&client).await;
    assert_eq!(
        settled,
        tag_totals_oracle(&client).await,
        "sanity check: the freshly built target already matches the \
         hand-written GROUP BY before anything else is staged"
    );

    // No write to `posts` at all — the base data is untouched. Only the
    // image-less trigger the backfill/propagation paths stage is added.
    stage_recompute(&client, "posts", "1").await;
    stage_recompute(&client, "posts", "2").await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        target_totals(&client).await,
        tag_totals_oracle(&client).await,
        "an image-less recompute on the to-side table must be idempotent: \
         the parent's contribution was already folded in, so re-asserting \
         that the parent exists must not add it a second time"
    );
    assert_eq!(
        target_totals(&client).await,
        settled,
        "and, specifically, nothing about the target may have moved at all"
    );
}
