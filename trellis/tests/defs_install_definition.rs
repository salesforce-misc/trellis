//! Integration tests for `defs::catalog::install_definition` (issue #63 C1):
//! the front door that creates a definition's target table once, then either
//! records it for the backfill discharge (ADR-0016, #418: a plain 1-1
//! definition's chunks, or the ring enumeration for a shape the direct build
//! can't render) or, for an aggregate or relationship-enriched 1-1 definition,
//! still builds it in-call via the fast set-based backfill
//! (`backfill::backfill_definition`) until issue #419.
//!
//! Each branch leaves a distinct, checkable signature in the ring: a direct or
//! chunked build stages no enumeration `Recompute` rows, while the ring
//! fallback enumerates every existing source row into the active segment.
//! Tests assert on that signature directly, rather than only on the
//! end-to-end target contents, to confirm each branch actually ran the code
//! path it claims to. `discharge_registrations` stands in for the staging
//! worker's discharge.
//!
//! The staging harness (connect, stage a CDC row, seal/drain to quiescence)
//! mirrors `apply_relationships.rs`/`defs_relationship_frontdoor.rs`; see
//! those files for the ring/seal mechanics.

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::{DEFAULT_SCHEMA, DEFAULT_TARGET_SCHEMA};
use trellis::defs::ast::{Expr, FieldDef, KeySpace, Predicate, RelationshipDef, TransformDef};
use trellis::defs::{
    TransformStatus, ValueType, chunk_queue, create_relationship, install_definition,
    render_relationship_select_sql,
};
use trellis::staging::apply;
use trellis::staging::{has_pending, retire_drained_segments};

/// Claims and executes every pending direct-build backfill chunk
/// (`trellis::defs::chunk_queue`, docs/decisions/0007's amendment) until none
/// remain — the test-harness stand-in for a running `application_threads`
/// drain worker, since `install_definition` no longer runs a plain
/// (non-relationship) 1-1 definition's backfill in-call: it now returns as
/// soon as the chunk work is enumerated and persisted, `Backfilling` until a
/// drain worker actually claims and finishes each chunk. Panics (via
/// `expect`) rather than swallowing an error, matching this file's other
/// harness helpers (`drain_to_quiescence`) — a chunk-execution failure here
/// means the test itself is broken, not something to retry past.
async fn drain_backfill_chunks(pool: &trellis::Pool) {
    // ADR-0016 (#418): registration only records a definition; the backfill
    // discharge dispatches its chunks.
    trellis::intake::publication::discharge_registrations(pool)
        .await
        .expect("dispatch registered definitions' builds");
    const CLAIMED_BY: &str = "install_def_test_backfill_worker";
    loop {
        let client = pool.get().await.expect("acquire connection");
        let claimed = chunk_queue::claim_chunks(&**client, CLAIMED_BY, 1000)
            .await
            .expect("claim_chunks");
        drop(client);
        if claimed.is_empty() {
            return;
        }
        for chunk in &claimed {
            chunk_queue::run_claimed_chunk(pool, chunk, CLAIMED_BY, Duration::from_secs(5))
                .await
                .expect("run_claimed_chunk");
            chunk_queue::finish_chunk(pool, chunk, CLAIMED_BY)
                .await
                .expect("finish_chunk");
        }
    }
}

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

/// The ring table backing the currently-active segment.
async fn active_seg_table(client: &Client) -> String {
    let ring_slot: i16 = client
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment pointer")
        .get(0);
    format!("seg_{ring_slot}")
}

/// How many rows are currently staged in the active segment for `src_table` —
/// the fast path's/ring fallback's distinguishing signature (see module doc).
async fn staged_count_for_source(client: &Client, src_table: &str) -> i64 {
    let seg = active_seg_table(client).await;
    let sql = format!("select count(*) from {seg} where src_table = $1");
    client
        .query_one(&sql, &[&src_table])
        .await
        .expect("count staged rows")
        .get(0)
}

/// Bare (no `.`) `name` qualified under [`DEFAULT_SCHEMA`] — where every bare
/// `create table` in this file's own fixtures actually lands, since
/// `connect_raw` pins `search_path` to `{DEFAULT_SCHEMA}, public` and never
/// qualifies its own DDL. Mirrors `apply.rs`'s `qualify_fixture_table`: a
/// real CDC producer always stages a fully-qualified `src_table` (issue
/// #76), and `compute`'s forward-propagation lookup
/// (`catalog::transforms_for_source`) now requires that exact qualified
/// identity to match `schema_nodes`/`schema_edges` (issue #74, ADR-0007).
/// Already-qualified input (containing a `.`) passes through unchanged.
fn qualify_fixture_table(name: &str) -> String {
    if name.contains('.') {
        name.to_string()
    } else {
        format!("{DEFAULT_SCHEMA}.{name}")
    }
}

/// Stages one image-bearing (CDC-shaped) change into the active ring segment.
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

/// Seals and drains repeatedly until nothing is pending anywhere in the ring.
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    // Issue #132: a throwaway, always-caught-up watermark — this helper
    // has no live `Intake` running (these tests stage CDC rows by hand),
    // and none of this file's tests exercise guard (a) specifically, so a
    // real watermark would only ever make guard (a) reject spuriously.
    let watermark = trellis::staging::StagedWatermark::saturated();
    let watermark = &watermark;
    for _ in 0..16 {
        let seg = seal_active_segment(client).await;
        while apply::drain_once(
            pool,
            seg,
            "install_def_test",
            1,
            "trellis_install_def_test",
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

fn numeric(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

fn columns(pairs: &[(&str, ValueType)]) -> HashMap<String, ValueType> {
    pairs
        .iter()
        .map(|(name, ty)| (name.to_string(), *ty))
        .collect()
}

// ---------------------------------------------------------------------
// Fast-path success branch: a plain (non-relationship) 1-1 definition.
// ---------------------------------------------------------------------

#[tokio::test]
async fn install_definition_fast_path_builds_target_without_staging_the_ring() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table s (id bigint primary key, a numeric); \
             insert into s (id, a) select g, g from generate_series(1, 500) g",
        )
        .await
        .expect("seed source");

    let cols = numeric(&["a"]);
    install_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT a + a AS x",
        &cols,
        "public",
    )
    .await
    .expect("install_definition via the fast path");

    // The direct build's chunk work is now enumerated and persisted, not
    // executed in-call (docs/decisions/0007's amendment) — drive it to
    // completion the way a running `application_threads` drain worker would
    // before asserting on the target's contents.
    drain_backfill_chunks(&db.pool).await;

    let mismatches: i64 = client
        .query_one(
            "select count(*) from s left join t on t.id = s.id \
             where t.id is null or t.x is distinct from s.a + s.a",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        mismatches, 0,
        "target built directly and matches every source row"
    );

    // The fast path persists via `create_definition_without_backfill`, which
    // stages no ring-enumeration `Recompute` rows — the opposite of the ring
    // fallback exercised below. Zero staged rows for `s` is the positive
    // signal that the direct build actually ran, not a silent fallback.
    assert_eq!(
        staged_count_for_source(&client, &format!("{DEFAULT_SCHEMA}.s")).await,
        0,
        "fast path must not enumerate the source into the ring"
    );
}

/// Issue #121, regression guard, fast/unit-style form (issue #297): the direct
/// build's chunk-boundary discovery (`defs::backfill::discover_pk_ranges`,
/// exercised here through `install_definition`'s real `plan_one_to_one_chunks`
/// -> persisted-`backfill_chunks` path) once had a live bug where an
/// unqualified `order by <col>` resolved to the *output* list's same-named
/// `<col>::text` cast instead of the input column, sorting lexicographically
/// (`'9999' > '50000'`) instead of numerically — silently over-chunking a
/// composite-keyed source into extra, overlapping chunks invisible to a
/// final-state count/value check alone, since the direct build's
/// overwrite-upsert is idempotent; only the *chunk count* itself exposes it.
///
/// This used to be a `client_e2e.rs` test that started a real `TrellisClient`
/// and polled up to 20s for a live multi-threaded drain to converge, purely to
/// get to a point where `backfill_chunks` could be counted — flaky under CI
/// runner load (#297) despite the chunk count itself being knowable
/// synchronously, right after `install_definition` returns, with no client
/// and no polling at all. This test asserts exactly that, then drains the
/// persisted chunks the same deterministic way
/// `install_definition_fast_path_builds_target_without_staging_the_ring`
/// above does, to also keep the final-built-value correctness check that a
/// chunk-count-only assertion can't provide on its own.
///
/// 99999 rows, one key column (`b`) cycling `1..=3` per value of the other
/// (`a`), forces the 50k-row chunk boundary to land *inside* an `a`-group
/// rather than on a clean one — the shape that would silently duplicate work
/// across chunks under the bug. See also
/// `defs_backfill_direct.rs`'s `one_to_one_build_with_a_composite_key_is_exhaustive_across_a_boundary_inside_a_group`,
/// which proves the same boundary shape is built exhaustively and correctly
/// through the *other* (in-call, non-durable-queue) direct-build entry point,
/// `backfill_definition` — that test doesn't assert chunk count (it can't:
/// `backfill_definition` never persists to `backfill_chunks`), so it alone
/// would not have caught the #121 over-chunking bug; this test is the one
/// that does, for the durable-queue path `install_definition` actually uses.
#[tokio::test]
async fn install_definition_chunks_a_composite_key_boundary_inside_a_group_into_exactly_two_chunks()
{
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table widgets (a bigint, b bigint, primary key (a, b)); \
             insert into widgets (a, b) \
             select (g - 1) / 3 + 1, (g - 1) % 3 + 1 from generate_series(1, 99999) g",
        )
        .await
        .expect("seed widgets with a composite primary key");

    let cols = numeric(&["a", "b"]);
    let def = install_definition(
        &db.pool,
        "TRANSFORM widgets_calc FROM widgets SELECT a + b AS total",
        &cols,
        "public",
    )
    .await
    .expect("install_definition records the definition and returns");
    assert_eq!(def.status, TransformStatus::WaitingToBackfill);
    // ADR-0016 (#418): the discharge plans and enqueues the chunks.
    trellis::intake::publication::discharge_registrations(&db.pool)
        .await
        .expect("dispatch the build");

    let chunk_count: i64 = client
        .query_one(
            "select count(*) from backfill_chunks where definition_id = $1",
            &[&def.id],
        )
        .await
        .expect("count persisted chunks")
        .get(0);
    assert_eq!(
        chunk_count, 2,
        "99999 rows at 50k rows/chunk must be exactly two chunks, \
         not silently over-chunked by a boundary-discovery bug"
    );

    drain_backfill_chunks(&db.pool).await;

    let mismatches: i64 = client
        .query_one(
            "select count(*) from widgets \
             left join widgets_calc on widgets_calc.a = widgets.a and widgets_calc.b = widgets.b \
             where widgets_calc.a is null \
                or widgets_calc.total is distinct from (widgets.a + widgets.b)::numeric",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        mismatches, 0,
        "every composite-keyed row must be built exactly once with the right value"
    );

    let status: String = client
        .query_one(
            &format!(
                "select status from transform_definitions where target_table = '{DEFAULT_TARGET_SCHEMA}.widgets_calc'"
            ),
            &[],
        )
        .await
        .expect("read back status")
        .get(0);
    assert_eq!(
        status, "live",
        "draining both persisted chunks must flip the definition to live"
    );
}

/// A reviewer's high-severity follow-up to issue #76's own grammar work: the
/// catalog correctly persists an explicitly-qualified source's fully-qualified
/// identity (`defs_catalog.rs`'s
/// `an_explicitly_qualified_source_resolves_to_that_exact_relation_not_search_path`
/// already covers that), but every physical SQL builder that actually reads
/// the *live* source table at backfill time used to still emit a bare,
/// unqualified `def.source`, relying on this pool's own pinned `search_path`
/// (`Config::schema`, `Config::target_schema`, `"public"` —
/// `pool::session_bootstrap`) to resolve it. That's silently wrong the moment
/// a same-named table sits in one of those pinned schemas while the
/// definition explicitly named a *different* one — exactly the setup below:
/// `public.orders` is a decoy (`public` is pinned, via this call's own
/// `target_schema` argument), `custom.orders` is the real, explicitly-named
/// source, and the two hold different row counts/values so a wrong-table read
/// is unmistakable in the target's contents, not just in a persisted string.
///
/// Drives the definition all the way through the fast (non-relationship 1-1)
/// path's durable chunk queue (the backfill discharge ->
/// `backfill::plan_one_to_one_chunks` -> `chunk_queue::dispatch_one_to_one`,
/// claimed and executed by [`drain_backfill_chunks`] via
/// `backfill::execute_one_to_one_chunk`) — the read-back leg that
/// reconstructs a [`trellis::defs::model::Definition`] fresh via
/// `catalog::definition_by_id` for every claimed chunk, so this also confirms
/// that reconstruction actually carries the persisted qualified source
/// through rather than re-deriving a bare one from re-parsed
/// `definition_text`.
#[tokio::test]
async fn install_definition_fast_path_reads_the_explicitly_qualified_source_not_a_same_named_decoy()
{
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table public.orders (id bigint primary key, price numeric); \
             insert into public.orders (id, price) values (1, 999), (2, 888); \
             create schema custom; \
             create table custom.orders (id bigint primary key, price numeric); \
             insert into custom.orders (id, price) values (1, 10), (2, 20), (3, 30);",
        )
        .await
        .expect("seed the public.orders decoy and the real custom.orders");

    let cols = numeric(&["price"]);
    install_definition(
        &db.pool,
        "TRANSFORM order_totals FROM custom.orders SELECT price AS total",
        &cols,
        "public",
    )
    .await
    .expect("install_definition against the explicitly-qualified source");

    // The fast path's chunk work is enumerated, not executed in-call
    // (docs/decisions/0007's amendment) — drive it to completion the way a
    // real drain worker would.
    drain_backfill_chunks(&db.pool).await;

    let row_count: i64 = client
        .query_one("select count(*) from order_totals", &[])
        .await
        .expect("count order_totals rows")
        .get(0);
    assert_eq!(
        row_count, 3,
        "custom.orders has 3 rows; a count of 2 would mean the public.orders \
         decoy was read instead"
    );

    let mismatches: i64 = client
        .query_one(
            "select count(*) from custom.orders left join order_totals \
                 on order_totals.id = custom.orders.id \
             where order_totals.id is null \
                or order_totals.total is distinct from custom.orders.price",
            &[],
        )
        .await
        .expect("compare the target against custom.orders")
        .get(0);
    assert_eq!(
        mismatches, 0,
        "order_totals must be built from custom.orders's prices, not the \
         same-named public.orders decoy sitting on this pool's own pinned \
         search_path"
    );
}

/// Issue #76 / ADR-0007 grammar clause 4, through the real front door: an
/// explicitly-qualified `TRANSFORM <schema>.<target>` must override the
/// `target_schema` argument this function is called with, for *every* step —
/// the physical `CREATE TABLE` DDL, the direct-build `INSERT`s, and the
/// persisted qualified identity all need to agree on `custom`, not the
/// `"public"` this call still passes as its own `target_schema` argument
/// (mirroring a real caller who never changed `Config::target_schema` but
/// wants to redirect just this one definition). This is the regression this
/// module's own bug would have reintroduced: an earlier draft of issue #76's
/// change checked the target's existence *before* this function's DDL step
/// ran, which would reject every legitimate explicit-target install outright
/// (the table doesn't exist yet — DDL is what's about to create it) —
/// exercising the fast path (not just `create_definition`'s ring path, which
/// `defs_catalog.rs`'s own issue #76 tests already cover) is what catches
/// that class of ordering bug.
#[tokio::test]
async fn install_definition_honors_an_explicitly_qualified_target_schema() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create schema custom; \
             create table s (id bigint primary key, a numeric); \
             insert into s (id, a) select g, g from generate_series(1, 50) g",
        )
        .await
        .expect("seed source and create the custom schema");

    let cols = numeric(&["a"]);
    let def = install_definition(
        &db.pool,
        "TRANSFORM custom.t FROM s SELECT a + a AS x",
        &cols,
        // Deliberately still "public": the explicit `custom.t` spelling must
        // win over this argument, not merely happen to agree with it.
        "public",
    )
    .await
    .expect("install_definition should honor the explicit target schema");

    drain_backfill_chunks(&db.pool).await;

    let target_table: String = client
        .query_one(
            "select target_table from transform_definitions where id = $1",
            &[&def.id],
        )
        .await
        .expect("read back the persisted definition")
        .get(0);
    assert_eq!(target_table, "custom.t");

    let mismatches: i64 = client
        .query_one(
            "select count(*) from s left join custom.t on custom.t.id = s.id \
             where custom.t.id is null or custom.t.x is distinct from s.a + s.a",
            &[],
        )
        .await
        .expect("the target was actually built under the named schema")
        .get(0);
    assert_eq!(
        mismatches, 0,
        "target built directly under the explicit schema and matches every source row"
    );

    let public_t_exists: bool = client
        .query_one(
            "select exists (select 1 from information_schema.tables \
             where table_schema = 'public' and table_name = 't')",
            &[],
        )
        .await
        .expect("check public.t")
        .get(0);
    assert!(
        !public_t_exists,
        "the explicit schema must fully override the passed-in target_schema argument, \
         not just add to it"
    );
}

// ---------------------------------------------------------------------
// Status lifecycle (issue #55): a definition that completes its backfill via
// `install_definition` must come back — and be persisted — as `Live`, not
// left sitting in the speculative `Backfilling` row the direct-build path
// inserts ahead of the build (see `catalog::install_definition`'s doc
// comment). Covers both the fast direct-build path and the ring-fallback
// path, since each takes a different route to `Live` (an in-place status
// flip vs. a fresh row from `create_definition` after the speculative row is
// discarded).
// ---------------------------------------------------------------------

#[tokio::test]
async fn install_definition_fast_path_ends_up_live() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table s (id bigint primary key, a numeric); \
             insert into s (id, a) select g, g from generate_series(1, 50) g",
        )
        .await
        .expect("seed source");

    let cols = numeric(&["a"]);
    let def = install_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT a + a AS x",
        &cols,
        "public",
    )
    .await
    .expect("install_definition via the fast path");

    // ADR-0016 (#418): `install_definition` returns once the definition is
    // recorded, before its build is even dispatched — so the definition it
    // hands back (and the persisted row) must still be `WaitingToBackfill`
    // right here.
    assert_eq!(
        def.status,
        TransformStatus::WaitingToBackfill,
        "install_definition must return before the build is dispatched"
    );
    let rows = client
        .query(
            &format!(
                "select status from transform_definitions where target_table = '{DEFAULT_TARGET_SCHEMA}.t'"
            ),
            &[],
        )
        .await
        .expect("read back persisted status");
    assert_eq!(
        rows.len(),
        1,
        "exactly one row for `t` — no leftover speculative row"
    );
    let persisted_status: String = rows[0].get(0);
    assert_eq!(
        persisted_status, "waiting_to_backfill",
        "the persisted row waits for the discharge to dispatch its chunks"
    );

    // Driving the chunk queue to completion (the `application_threads` drain
    // worker's job in a real fleet) must flip it the rest of the way to live.
    drain_backfill_chunks(&db.pool).await;
    let rows = client
        .query(
            &format!(
                "select status from transform_definitions where target_table = '{DEFAULT_TARGET_SCHEMA}.t'"
            ),
            &[],
        )
        .await
        .expect("read back persisted status");
    assert_eq!(rows.len(), 1);
    let persisted_status: String = rows[0].get(0);
    assert_eq!(
        persisted_status, "live",
        "the persisted row must have been flipped to live once every chunk finished"
    );
}

#[tokio::test]
async fn install_definition_ring_fallback_ends_up_live() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table categories (id integer primary key, name text); \
             create table articles (id integer primary key, category_id integer, title text); \
             alter table categories replica identity full; \
             alter table articles replica identity full; \
             insert into categories (id, name) values (10, 'Tech'); \
             insert into articles (id, category_id, title) values (1, 10, 'a1')",
        )
        .await
        .expect("create + seed tables");

    create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_id TO categories.id",
    )
    .await
    .expect("create to-one relationship");

    let source_columns = columns(&[
        ("id", ValueType::Numeric),
        ("category_id", ValueType::Numeric),
        ("title", ValueType::Text),
    ]);

    // Same `Unsupported` shape as the fallback test below: routes through
    // the speculative-insert-then-delete-then-ring-recreate path in
    // `install_definition`.
    let def = install_definition(
        &db.pool,
        "TRANSFORM article_cat FROM articles SELECT category.name AS category_name",
        &source_columns,
        "public",
    )
    .await
    .expect("install_definition falls back to the ring path");

    assert_eq!(
        def.status,
        TransformStatus::WaitingToBackfill,
        "the ring-fallback path waits for the discharge too (ADR-0016, #418)"
    );
    trellis::intake::publication::discharge_registrations(&db.pool)
        .await
        .expect("the discharge enumerates the source and takes it live");

    let rows = client
        .query(
            &format!(
                "select status from transform_definitions where target_table = '{DEFAULT_TARGET_SCHEMA}.article_cat'"
            ),
            &[],
        )
        .await
        .expect("read back persisted status");
    assert_eq!(
        rows.len(),
        1,
        "the speculative row must have been deleted, leaving exactly the ring path's own row"
    );
    let persisted_status: String = rows[0].get(0);
    assert_eq!(persisted_status, "live");
}

// ---------------------------------------------------------------------
// Fast-path success branch: a plain 1-1 field that references another
// calculated field's alias (issue #83 — `double_price + tax AS total` where
// `double_price = price + price`). WI1 made this shape fall back to the ring
// (safe, but slow); the direct build now inlines the alias chain
// (`substitute_all_fields`) and builds it set-based, so the source is never
// enumerated into the ring. (Supersedes WI1's ring-fallback assertion for
// this shape — expected and correct.)
// ---------------------------------------------------------------------

#[tokio::test]
async fn install_definition_fast_path_builds_a_plain_cross_field_alias_chain() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table s (id bigint primary key, price numeric, tax numeric); \
             insert into s (id, price, tax) values (1, 10, 1), (2, 20, 2), (3, 30, 3)",
        )
        .await
        .expect("seed source");

    let cols = numeric(&["price", "tax"]);
    // `total` references `double_price`, itself a calculated field
    // (`price + price`). Substitution inlines it to `(price + price) + tax`,
    // so the direct build renders self-contained source SQL.
    install_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT price + price AS double_price, \
         double_price + tax AS total",
        &cols,
        "public",
    )
    .await
    .expect("install_definition builds the alias chain directly");

    // The direct build's chunk work is enumerated/persisted, not executed
    // in-call — drive it to completion before reading the target.
    drain_backfill_chunks(&db.pool).await;

    // The direct build populates the target and stages nothing in the ring
    // — the fast-path signature (see the sibling fast-path test).
    let mut rows: Vec<(i64, String, String)> = client
        .query(
            "select id, double_price::text, total::text from t order by id",
            &[],
        )
        .await
        .expect("read t")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    rows.sort_by_key(|(id, ..)| *id);
    assert_eq!(
        rows,
        vec![
            (1, "20".to_string(), "21".to_string()),
            (2, "40".to_string(), "42".to_string()),
            (3, "60".to_string(), "63".to_string()),
        ],
        "direct build computes the alias chain correctly for every row"
    );
    assert_eq!(
        staged_count_for_source(&client, &format!("{DEFAULT_SCHEMA}.s")).await,
        0,
        "fast path must not enumerate the source into the ring"
    );
}

// ---------------------------------------------------------------------
// Fast-path success branch: an Aggregate (`GROUP BY`) definition whose
// `GROUP BY` field references another calculated field's alias (the
// Aggregate-key-space counterpart of the plain cross-field-alias test above
// — `total + total AS double_total` where `total = SUM(amount)`). Before this
// fix, `backfill_aggregate`'s `classify_field`/`render_expr_sql` rendered
// `double_total`'s raw, un-substituted `Expr::Column("total")` as a bare SQL
// identifier, which Postgres rejects (`total` names neither a source column
// nor a same-SELECT-list-visible name) — the direct build now inlines the
// alias first (`substituted_field_exprs`), same as the 1-1 path.
// ---------------------------------------------------------------------

#[tokio::test]
async fn install_definition_fast_path_builds_an_aggregate_cross_field_alias_chain() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_items \
             (id bigint primary key, order_id bigint, amount numeric); \
             alter table order_items replica identity full; \
             insert into order_items (id, order_id, amount) values \
             (1, 10, 5), (2, 10, 7), (3, 20, 3)",
        )
        .await
        .expect("seed source");

    let cols = columns(&[
        ("order_id", ValueType::Numeric),
        ("amount", ValueType::Numeric),
    ]);
    // `double_total` references `total`, itself a calculated field
    // (`SUM(amount)`). Substitution inlines it to `sum(amount) + sum(amount)`
    // before any SQL is rendered.
    install_definition(
        &db.pool,
        "TRANSFORM order_summary FROM order_items GROUP BY order_id \
         SELECT order_id AS order_id, SUM(amount) AS total, \
         total + total AS double_total",
        &cols,
        "public",
    )
    .await
    .expect("install_definition builds the aggregate alias chain directly");

    // The direct build populates the target synchronously and stages nothing
    // in the ring — the fast-path signature (see the sibling fast-path tests).
    let mut rows: Vec<(String, String, String)> = client
        .query(
            "select order_id::text, total::text, double_total::text \
             from order_summary order by order_id",
            &[],
        )
        .await
        .expect("read order_summary")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            ("10".to_string(), "12".to_string(), "24".to_string()),
            ("20".to_string(), "3".to_string(), "6".to_string()),
        ],
        "direct build computes the aggregate alias chain correctly for every group \
         (double_total = 2 * sum(amount))"
    );
    assert_eq!(
        staged_count_for_source(&client, &format!("{DEFAULT_SCHEMA}.order_items")).await,
        0,
        "fast path must not enumerate the source into the ring"
    );
}

/// A bare (not `BinaryOp`-wrapped) alias of a `SUM` field, declared *before*
/// the field it aliases — `SELECT ..., total AS grand_total, SUM(amount) AS
/// total`. Forward references are legal (the validator's cycle check and the
/// evaluator's `fields_by_name` lookup are both declaration-order-agnostic),
/// so `grand_total`'s classification (does it need a hidden running-count
/// column, and under what name?) can only be answered from its *substituted*
/// form (`SUM(amount)`), not its raw `Column("total")` shape. This pins the
/// declaration-order-dependent regression the `double_total = total + total`
/// case above doesn't cover: that shape is a `BinaryOp`, which never
/// classifies as a bare `SUM`/`AVG` field either way, so it never exercised
/// `ddl::create_aggregate_target_table`'s own (substitution-aware)
/// `is_sum_field`/`count_column_names` classification — only a bare-alias
/// field does.
#[tokio::test]
async fn install_definition_builds_a_bare_alias_of_a_sum_field_declared_before_it() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_items \
             (id bigint primary key, order_id bigint, amount numeric); \
             alter table order_items replica identity full; \
             insert into order_items (id, order_id, amount) values \
             (1, 10, 5), (2, 10, 7), (3, 20, 3)",
        )
        .await
        .expect("seed source");

    let cols = columns(&[
        ("order_id", ValueType::Numeric),
        ("amount", ValueType::Numeric),
    ]);
    install_definition(
        &db.pool,
        "TRANSFORM order_summary FROM order_items GROUP BY order_id \
         SELECT order_id AS order_id, total AS grand_total, SUM(amount) AS total",
        &cols,
        "public",
    )
    .await
    .expect("install_definition builds a bare alias of a SUM field declared before it");

    let mut rows: Vec<(String, String, String)> = client
        .query(
            "select order_id::text, total::text, grand_total::text \
             from order_summary order by order_id",
            &[],
        )
        .await
        .expect("read order_summary")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            ("10".to_string(), "12".to_string(), "12".to_string()),
            ("20".to_string(), "3".to_string(), "3".to_string()),
        ],
        "grand_total must equal total in every group, regardless of declaration order"
    );
}

/// Several bare aliases of the same `SUM` field, declared out of order and
/// on both sides of it (`grand_total`/`super_total` alias `total`, which
/// itself is declared between them). Their *substituted* forms are all
/// structurally identical (`SUM(amount)`), so `count_column_names_from`'s
/// same-argument dedup (issue #48) must fold all three onto the one hidden
/// running-count column `total` itself would get — exercising that the
/// dedup, `ddl::create_aggregate_target_table`'s column creation, and
/// `backfill_aggregate`'s writes all agree on *which* name that is,
/// regardless of which of the three fields is first in declaration order.
/// A group with an all-`NULL` argument additionally pins that the shared
/// count still distinguishes "sum of nothing" (`NULL`) from "sum that nets
/// to zero" for every alias, not just the field that directly wraps `SUM`.
#[tokio::test]
async fn install_definition_shares_one_count_column_across_several_aliases_of_a_sum_field() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_items \
             (id bigint primary key, order_id bigint, amount numeric); \
             alter table order_items replica identity full; \
             insert into order_items (id, order_id, amount) values \
             (1, 10, 5), (2, 10, 7), (3, 20, null)",
        )
        .await
        .expect("seed source");

    let cols = columns(&[
        ("order_id", ValueType::Numeric),
        ("amount", ValueType::Numeric),
    ]);
    install_definition(
        &db.pool,
        "TRANSFORM order_summary FROM order_items GROUP BY order_id \
         SELECT order_id AS order_id, grand_total AS super_total, \
         total AS grand_total, SUM(amount) AS total",
        &cols,
        "public",
    )
    .await
    .expect("install_definition shares one count column across several SUM aliases");

    // (order_id, total, grand_total, super_total)
    type Row = (String, Option<String>, Option<String>, Option<String>);
    let mut rows: Vec<Row> = client
        .query(
            "select order_id::text, total::text, grand_total::text, super_total::text \
             from order_summary order by order_id",
            &[],
        )
        .await
        .expect("read order_summary")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            (
                "10".to_string(),
                Some("12".to_string()),
                Some("12".to_string()),
                Some("12".to_string())
            ),
            ("20".to_string(), None, None, None),
        ],
        "every alias of `total` must equal `total` in every group, including NULL \
         (sum of an all-NULL group), regardless of declaration order"
    );
}

// ---------------------------------------------------------------------
// Unsupported/ring-fallback branch: a bare to-one relationship lookup
// (no aggregate wrapper) — a shape `backfill_relationship_one_to_one`
// explicitly rejects, since it only renders to-many aggregates.
// ---------------------------------------------------------------------

fn to_one_def() -> TransformDef {
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

fn category_rel() -> HashMap<String, RelationshipDef> {
    HashMap::from([(
        "category".to_string(),
        RelationshipDef {
            name: "category".to_string(),
            from_table: "articles".to_string(),
            from_col: "category_id".to_string(),
            to_table: "categories".to_string(),
            to_col: "id".to_string(),
        },
    )])
}

/// Oracle counterpart of [`to_one_def`], projecting the source PK `id` too so
/// the outer wrapper can key on it — the target table carries `id` as its own
/// PK column, added by `install_definition`'s DDL step.
fn to_one_oracle_def() -> TransformDef {
    let mut def = to_one_def();
    def.fields.insert(
        0,
        FieldDef {
            name: "id".to_string(),
            expr: Expr::Column("id".to_string()),
        },
    );
    def
}

async fn oracle_to_one(client: &Client) -> HashMap<String, Option<String>> {
    let base = render_relationship_select_sql(&to_one_oracle_def(), &category_rel());
    let sql = format!("select id::text, category_name::text from ({base}) t");
    client
        .query(sql.as_str(), &[])
        .await
        .expect("query to-one oracle")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

async fn target_to_one(client: &Client) -> HashMap<String, Option<String>> {
    client
        .query("select id::text, category_name::text from article_cat", &[])
        .await
        .expect("read article_cat")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

#[tokio::test]
async fn install_definition_falls_back_to_ring_for_relationship_enriched_definition() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table categories (id integer primary key, name text); \
             create table articles (id integer primary key, category_id integer, title text); \
             alter table categories replica identity full; \
             alter table articles replica identity full; \
             insert into categories (id, name) values (10, 'Tech'), (20, 'News'); \
             insert into articles (id, category_id, title) values \
             (1, 10, 'a1'), (2, 20, 'a2'), (3, 99, 'a3')",
        )
        .await
        .expect("create + seed tables");

    create_relationship(
        &db.pool,
        "RELATIONSHIP category FROM articles.category_id TO categories.id",
    )
    .await
    .expect("create to-one relationship");

    let source_columns = columns(&[
        ("id", ValueType::Numeric),
        ("category_id", ValueType::Numeric),
        ("title", ValueType::Text),
    ]);

    // `uses_relationships` routes this to `backfill_relationship_one_to_one`,
    // but a bare to-one lookup (`category.name`, no aggregate) is a shape
    // `collect_agg_leaves` still rejects. `install_definition` must catch the
    // resulting `BackfillError::Unsupported` and fall back to the ring-based
    // `create_definition`, having already created the target table itself
    // (a second `create_target_table` call in the fallback would have errored
    // on the already-existing relation, which never happens here).
    install_definition(
        &db.pool,
        "TRANSFORM article_cat FROM articles SELECT category.name AS category_name",
        &source_columns,
        "public",
    )
    .await
    .expect("install_definition falls back to the ring path for a relationship-enriched shape");
    trellis::intake::publication::discharge_registrations(&db.pool)
        .await
        .expect("the discharge enumerates the source");

    // The ring fallback enumerates every existing source row into the active
    // segment — the opposite signal from the fast-path sibling test above —
    // and the target starts empty, since the ring build only stages
    // `Recompute` rows.
    assert_eq!(
        staged_count_for_source(&client, &format!("{DEFAULT_SCHEMA}.articles")).await,
        3,
        "ring fallback enumerated every existing source row"
    );
    let empty: i64 = client
        .query_one("select count(*) from article_cat", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(empty, 0, "ring fallback does not build rows synchronously");

    // Draining the ring builds the target from the enumerated backfill,
    // converging to the LEFT JOIN oracle (article 3 -> NULL: category 99
    // doesn't exist) — proof the fallback left the definition and target in a
    // valid, usable state.
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        target_to_one(&client).await,
        oracle_to_one(&client).await,
        "after draining the ring-fallback backfill"
    );

    // The ring path is live going forward: a fresh CDC insert on the source
    // drains through to a correct row too.
    client
        .execute(
            "insert into articles (id, category_id, title) values (4, 10, 'a4')",
            &[],
        )
        .await
        .expect("insert article 4");
    stage_cdc(
        &client,
        "articles",
        "4",
        "insert",
        None,
        Some("{\"id\":4,\"category_id\":10,\"title\":\"a4\"}"),
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        target_to_one(&client).await,
        oracle_to_one(&client).await,
        "after a live CDC insert following the fallback"
    );
}

// ---------------------------------------------------------------------
// Fast-path success branch: a relationship-enriched 1-1 field that references
// other (to-many-aggregate) fields' aliases (issue #83 — the original
// report: `count(posts.id), count(comments.id), post_count + comment_count
// AS total`, which used to HARD-CRASH). Substitution inlines `total` to
// `count(posts.id) + count(comments.id)`; the two shared `count` leaves are
// deduped into one staged column each, and the whole tree renders against
// them — so the definition builds directly, no ring enumeration.
// (Supersedes WI1's ring-fallback assertion for this shape.)
// ---------------------------------------------------------------------

#[tokio::test]
async fn install_definition_fast_path_builds_a_relationship_cross_field_alias_chain() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table authors (id integer primary key, name text); \
             create table posts (id integer primary key, author_id integer); \
             create table comments (id integer primary key, author_id integer); \
             alter table posts replica identity full; \
             alter table comments replica identity full; \
             insert into authors (id, name) values (1, 'a'), (2, 'b'); \
             insert into posts (id, author_id) values (100, 1), (101, 1); \
             insert into comments (id, author_id) values (200, 1), (201, 1), (202, 1)",
        )
        .await
        .expect("create + seed tables");

    create_relationship(
        &db.pool,
        "RELATIONSHIP posts FROM authors.id TO posts.author_id",
    )
    .await
    .expect("create posts relationship");
    create_relationship(
        &db.pool,
        "RELATIONSHIP comments FROM authors.id TO comments.author_id",
    )
    .await
    .expect("create comments relationship");

    let source_columns: HashMap<String, ValueType> =
        columns(&[("id", ValueType::Numeric), ("name", ValueType::Text)]);

    install_definition(
        &db.pool,
        "TRANSFORM author_totals FROM authors SELECT \
         COUNT(posts.id) AS post_count, \
         COUNT(comments.id) AS comment_count, \
         post_count + comment_count AS total",
        &source_columns,
        "public",
    )
    .await
    .expect("install_definition builds the relationship alias chain directly");

    // Direct build: target populated synchronously, nothing staged in the ring.
    let mut rows: Vec<(String, String, String, String)> = client
        .query(
            "select id::text, post_count::text, comment_count::text, total::text \
             from author_totals order by id",
            &[],
        )
        .await
        .expect("read author_totals")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            (
                "1".to_string(),
                "2".to_string(),
                "3".to_string(),
                "5".to_string()
            ),
            (
                "2".to_string(),
                "0".to_string(),
                "0".to_string(),
                "0".to_string()
            ),
        ],
        "direct build computes the relationship alias chain correctly"
    );
    assert_eq!(
        staged_count_for_source(&client, &format!("{DEFAULT_SCHEMA}.authors")).await,
        0,
        "fast path must not enumerate the source into the ring"
    );
}

// ---------------------------------------------------------------------
// Fast-path success branch: a coalesce-wrapped to-many aggregate
// (`coalesce(sum(posts.word_count), 0)`, issue #83) — the normal way to write
// a nullable aggregate. The aggregate leaf nested inside `coalesce` is now
// recognized, staged, and rendered as `coalesce(<staged-ref>, 0)`, so the
// definition builds directly instead of falling back to the ring.
// ---------------------------------------------------------------------

#[tokio::test]
async fn install_definition_fast_path_builds_a_coalesce_wrapped_aggregate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table authors (id integer primary key, name text); \
             create table posts (id integer primary key, author_id integer, word_count integer); \
             alter table posts replica identity full; \
             insert into authors (id, name) values (1, 'a'), (2, 'b'); \
             insert into posts (id, author_id, word_count) values \
             (100, 1, 10), (101, 1, 20)",
        )
        .await
        .expect("create + seed tables");

    create_relationship(
        &db.pool,
        "RELATIONSHIP posts FROM authors.id TO posts.author_id",
    )
    .await
    .expect("create posts relationship");

    let source_columns: HashMap<String, ValueType> =
        columns(&[("id", ValueType::Numeric), ("name", ValueType::Text)]);

    install_definition(
        &db.pool,
        "TRANSFORM author_totals FROM authors SELECT \
         coalesce(sum(posts.word_count), 0) AS total_words",
        &source_columns,
        "public",
    )
    .await
    .expect("install_definition builds a coalesce-wrapped aggregate directly");

    let mut rows: Vec<(String, String)> = client
        .query(
            "select id::text, total_words::text from author_totals order by id",
            &[],
        )
        .await
        .expect("read author_totals")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            ("1".to_string(), "30".to_string()),
            // No posts -> coalesce(NULL, 0) = 0, not NULL.
            ("2".to_string(), "0".to_string()),
        ],
        "coalesce(sum(...), 0) builds directly with the empty set coalesced to 0"
    );
    assert_eq!(
        staged_count_for_source(&client, &format!("{DEFAULT_SCHEMA}.authors")).await,
        0,
        "fast path must not enumerate the source into the ring"
    );
}

// ---------------------------------------------------------------------
// Fast-path success branch: the deepest nesting issue #83's repro implies —
// an alias-derived field summing two coalesce-wrapped aggregates over
// *different* relationships (`total_words = total_posted_words +
// total_commented_words`, each itself `coalesce(sum(...), 0)`). Substitution
// inlines both, giving `coalesce(sum(posts.word_count), 0) +
// coalesce(sum(comments.word_count), 0)`; the two distinct SUM leaves are
// staged (one per relationship) and the tree renders against them.
// ---------------------------------------------------------------------

#[tokio::test]
async fn install_definition_fast_path_builds_nested_coalesce_alias_chain() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table authors (id integer primary key, name text); \
             create table posts (id integer primary key, author_id integer, word_count integer); \
             create table comments (id integer primary key, author_id integer, word_count integer); \
             alter table posts replica identity full; \
             alter table comments replica identity full; \
             insert into authors (id, name) values (1, 'a'), (2, 'b'); \
             insert into posts (id, author_id, word_count) values (100, 1, 10), (101, 1, 20); \
             insert into comments (id, author_id, word_count) values (200, 1, 3), (201, 1, 4)",
        )
        .await
        .expect("create + seed tables");

    create_relationship(
        &db.pool,
        "RELATIONSHIP posts FROM authors.id TO posts.author_id",
    )
    .await
    .expect("create posts relationship");
    create_relationship(
        &db.pool,
        "RELATIONSHIP comments FROM authors.id TO comments.author_id",
    )
    .await
    .expect("create comments relationship");

    let source_columns: HashMap<String, ValueType> =
        columns(&[("id", ValueType::Numeric), ("name", ValueType::Text)]);

    install_definition(
        &db.pool,
        "TRANSFORM author_totals FROM authors SELECT \
         coalesce(sum(posts.word_count), 0) AS total_posted_words, \
         coalesce(sum(comments.word_count), 0) AS total_commented_words, \
         total_posted_words + total_commented_words AS total_words",
        &source_columns,
        "public",
    )
    .await
    .expect("install_definition builds the nested coalesce alias chain directly");

    let mut rows: Vec<(String, String, String, String)> = client
        .query(
            "select id::text, total_posted_words::text, total_commented_words::text, \
             total_words::text from author_totals order by id",
            &[],
        )
        .await
        .expect("read author_totals")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            // posts 10+20=30, comments 3+4=7, total 37.
            (
                "1".to_string(),
                "30".to_string(),
                "7".to_string(),
                "37".to_string()
            ),
            // No related rows -> every coalesce(NULL, 0) = 0.
            (
                "2".to_string(),
                "0".to_string(),
                "0".to_string(),
                "0".to_string()
            ),
        ],
        "nested coalesce+alias chain builds directly with correct sums"
    );
    assert_eq!(
        staged_count_for_source(&client, &format!("{DEFAULT_SCHEMA}.authors")).await,
        0,
        "fast path must not enumerate the source into the ring"
    );
}

// ---------------------------------------------------------------------
// Reviewer follow-up to issue #74 (epic #78's own whole-branch review):
// `install_definition`'s own source resolution (`resolve_source_for_install`,
// `plan_direct_backfill_coverage`) walked bare `def.source` only through
// `search_path` (`resolve_source_schema`/`resolve_source_schema_in_txn`),
// never through `resolve_graph_identity`/`resolve_graph_identity_in_txn`'s
// bare-target-suffix fallback the way `create_definition_inner`'s own
// resolution of the identical bare source already does (issue #74). Since
// `install_definition` runs its own DDL/backfill *before*
// `create_definition_inner` is ever reached — a distinct fast path, not a
// thin wrapper around the ring — a second definition's bare `FROM t` failed
// to find a first definition's target explicitly qualified into a schema
// outside the fixed `search_path` `pool::session_bootstrap` pins, even
// though the ring path (`create_definition`) already resolved the identical
// chain correctly.
// ---------------------------------------------------------------------

/// Plain (non-relationship) 1-1 repro: exercises `resolve_source_for_install`
/// specifically, since a plain 1-1 definition's DDL step
/// (`ddl::source_primary_key(pool, &qualified_source)`, run directly inside
/// `install_definition` before it registers the definition for the discharge)
/// is the very first place this gap could fail — before `plan_direct_backfill_coverage`
/// even runs (that function only runs for the relationship-enriched/aggregate
/// branch, see the sibling test below).
#[tokio::test]
async fn install_definition_fast_path_resolves_a_bare_from_chained_off_a_non_default_schema_target()
{
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create schema custom; \
             create table s (id bigint primary key, a numeric); \
             insert into s (id, a) select g, g from generate_series(1, 50) g",
        )
        .await
        .expect("seed source and create the custom schema");

    // Def A: installs with an explicit non-default target schema, exactly
    // like `install_definition_honors_an_explicitly_qualified_target_schema`'s
    // own setup — `custom` is nowhere on this pool's pinned `search_path`
    // (`Config::schema`/`Config::target_schema`/`public`).
    install_definition(
        &db.pool,
        "TRANSFORM custom.t FROM s SELECT a + a AS x",
        &numeric(&["a"]),
        "public",
    )
    .await
    .expect("def A installs with an explicit non-default target schema");
    drain_backfill_chunks(&db.pool).await;

    // Def B: a bare `FROM t` must still resolve to def A's `custom.t` — a
    // plain `search_path` walk alone would report "t" not found, even though
    // `custom.t` is live.
    let def_b = install_definition(
        &db.pool,
        "TRANSFORM u FROM t SELECT x + x AS y",
        &numeric(&["x"]),
        "public",
    )
    .await
    .expect(
        "def B's bare FROM must resolve to def A's explicitly-qualified custom.t \
         target, not fail as 'not found on the search path'",
    );
    drain_backfill_chunks(&db.pool).await;

    let source_table: String = client
        .query_one(
            "select source_table from transform_definitions where id = $1",
            &[&def_b.id],
        )
        .await
        .expect("read back def B's persisted definition")
        .get(0);
    assert_eq!(
        source_table, "custom.t",
        "def B must resolve its bare FROM to custom.t, def A's actual target"
    );

    let mismatches: i64 = client
        .query_one(
            "select count(*) from custom.t left join u on u.id = custom.t.id \
             where u.id is null or u.y is distinct from custom.t.x + custom.t.x",
            &[],
        )
        .await
        .expect("compare the target against custom.t")
        .get(0);
    assert_eq!(
        mismatches, 0,
        "u must be built from custom.t's rows, not fail before ever reaching them"
    );
}

/// Relationship-enriched-1-1 repro: exercises `plan_direct_backfill_coverage`'s
/// own per-table resolution specifically. A relationship-enriched 1-1
/// definition never takes the plain-1-1 short-circuit (registering for the
/// discharge) — `backfill::uses_relationships` routes it
/// through `plan_direct_backfill_coverage` instead, same as an Aggregate
/// would, but without also exercising `create_definition_inner`'s separate
/// `assert_replica_identity_supports_aggregate` check (irrelevant to a
/// to-one-enriched 1-1, and out of this fix's scope). `def C`'s own source
/// is the chained, non-default-schema target — `tagrel`'s to-side
/// (`tags`) is an ordinary, already-on-`search_path` table, so only the
/// `bare_table == def.source` branch this fix touches is under test here.
///
/// `plan_direct_backfill_coverage` runs unconditionally, before
/// `backfill::backfill_definition` ever classifies this shape (a bare to-one
/// passthrough enrichment, `tagrel.label` with no aggregate) as
/// `BackfillError::Unsupported` and falls back to the ring — mirroring
/// `install_definition_falls_back_to_ring_for_relationship_enriched_definition`
/// above. So this still proves the fix: before it, resolution failed inside
/// `plan_direct_backfill_coverage` itself, before the fallback was ever
/// reached.
#[tokio::test]
async fn install_definition_relationship_enriched_path_resolves_a_bare_from_chained_off_a_non_default_schema_target()
 {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create schema custom; \
             create table s (id bigint primary key, a numeric); \
             insert into s (id, a) select g, g from generate_series(1, 50) g; \
             create table tags (id serial primary key, label text); \
             alter table tags replica identity full; \
             insert into tags (id, label) select g, 'tagged' from generate_series(1, 50) g",
        )
        .await
        .expect("seed source, tags, and create the custom schema");

    install_definition(
        &db.pool,
        "TRANSFORM custom.t FROM s SELECT a + a AS x",
        &numeric(&["a"]),
        "public",
    )
    .await
    .expect("def A installs with an explicit non-default target schema");
    drain_backfill_chunks(&db.pool).await;

    // `t.id` (def A's target's own PK) is a real, integer-family column —
    // this relationship's `from_table` is itself the chained, non-default-
    // schema target under test, so declaring it also exercises this same
    // fix's `create_relationship`-side gap (see `defs_relationship_catalog.rs`'s
    // own regression test). Issue #158: the from-side needs `REPLICA IDENTITY
    // FULL` unconditionally too, same as the to-side, regardless of whether
    // its join column happens to be the primary key.
    db.pool
        .get()
        .await
        .expect("get connection")
        .batch_execute("alter table custom.t replica identity full")
        .await
        .expect("set replica identity full on custom.t");
    create_relationship(&db.pool, "RELATIONSHIP tagrel FROM t.id TO tags.id")
        .await
        .expect("relationship's bare FROM must resolve to custom.t");

    // Def C: a relationship-enriched 1-1 whose bare `FROM t` must resolve to
    // def A's `custom.t` through `plan_direct_backfill_coverage`, not fail
    // before the direct build ever runs.
    install_definition(
        &db.pool,
        "TRANSFORM u FROM t SELECT x AS y, tagrel.label AS tag_label",
        &numeric(&["x"]),
        "public",
    )
    .await
    .expect(
        "def C's bare FROM must resolve to def A's explicitly-qualified custom.t \
         target through plan_direct_backfill_coverage, not fail as 'not found on \
         the search path'",
    );

    // This shape is `Unsupported` by the direct build (see this test's own
    // doc comment), so `install_definition` falls back to the ring — the
    // target starts empty and only converges once the discharge has
    // enumerated the source and the ring is drained, exactly like
    // `install_definition_falls_back_to_ring_for_relationship_enriched_definition`.
    trellis::intake::publication::discharge_registrations(&db.pool)
        .await
        .expect("the discharge enumerates the source");
    drain_to_quiescence(&db.pool, &mut client).await;

    let row_count: i64 = client
        .query_one("select count(*) from u where tag_label = 'tagged'", &[])
        .await
        .expect("count u rows")
        .get(0);
    assert_eq!(
        row_count, 50,
        "u must be built from custom.t's 50 rows, each enriched via tagrel"
    );
}

/// ADR-0016 (#418): registration reads no source rows. It succeeds while
/// another session holds the source `ACCESS EXCLUSIVE` (which blocks every
/// read of it) — for a plain 1-1 definition, and for a shape the direct build
/// can't render, whose ring fallback used to enumerate the source inside
/// registration — and both go live once the lock is released and the
/// discharge runs. The timeout only turns a registration stuck on the lock
/// into a failure instead of a hang.
#[tokio::test]
async fn registration_reads_no_source_rows() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table s (id bigint primary key, a numeric); \
             insert into s (id, a) select g, g from generate_series(1, 50) g",
        )
        .await
        .expect("seed source");

    let locker = connect_raw(db.dsn()).await;
    locker
        .batch_execute("begin; lock table s in access exclusive mode")
        .await
        .expect("lock the source against reads");

    let register = |text: String| {
        let pool = db.pool.clone();
        async move {
            tokio::time::timeout(
                Duration::from_secs(20),
                install_definition(&pool, &text, &numeric(&["a"]), "public"),
            )
            .await
            .unwrap_or_else(|_| panic!("registering {text:?} waited on the source's lock"))
            .unwrap_or_else(|e| panic!("register {text:?}: {e}"))
        }
    };
    let plain = register("TRANSFORM t FROM s SELECT a + a AS x".to_string()).await;
    assert_eq!(plain.status, TransformStatus::WaitingToBackfill);
    // A doubling alias chain (`f<k> = f<k-1> + f<k-1>`) whose inlined form
    // outgrows the direct build's substitution budget is `Unsupported`, so
    // it falls back to the ring.
    let chain: Vec<String> = std::iter::once("a + a AS f0".to_string())
        .chain((1..=17).map(|k| format!("f{} + f{} AS f{k}", k - 1, k - 1)))
        .collect();
    let ring = register(format!("TRANSFORM u FROM s SELECT {}", chain.join(", "))).await;
    assert_eq!(ring.status, TransformStatus::WaitingToBackfill);

    locker
        .batch_execute("commit")
        .await
        .expect("release the lock");
    trellis::intake::publication::discharge_registrations(&db.pool)
        .await
        .expect("discharge the registrations");
    let ring_rows: i64 = client
        .query_one(
            &format!("select count(*) from {}", active_seg_table(&client).await),
            &[],
        )
        .await
        .expect("count staged ring rows")
        .get(0);
    assert_eq!(
        ring_rows, 50,
        "only the ring fallback enumerates the source"
    );
    drain_backfill_chunks(&db.pool).await;
    drain_to_quiescence(&db.pool, &mut client).await;

    let statuses: Vec<String> = client
        .query("select status from transform_definitions order by id", &[])
        .await
        .expect("read statuses")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(statuses, ["live", "live"]);
    let mismatches: i64 = client
        .query_one(
            "select count(*) from s \
             left join t on t.id = s.id left join u on u.id = s.id \
             where t.x is distinct from s.a + s.a or u.f17 is distinct from s.a * 262144",
            &[],
        )
        .await
        .expect("compare the targets to the source")
        .get(0);
    assert_eq!(
        mismatches, 0,
        "both targets are built from every source row"
    );
}
