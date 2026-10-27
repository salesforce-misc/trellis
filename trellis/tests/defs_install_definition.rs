//! Integration tests for `defs::catalog::install_definition`:
//! the front door that creates a definition's target table once, then
//! records it `waiting_to_backfill` (ADR-0002) for the Re-derive
//! build (`staging::build`): a plain 1-1 definition's `rederive` chunks, or an
//! aggregate's one absolute write in its plan job.
//!
//! The build reads the source itself and stages nothing in the ring, which
//! some tests assert directly (no `Recompute` rows for the source in the
//! active segment) beside the end-to-end target contents.
//! `trellis::intake::harness::start_registered_builds` and `settle_builds`
//! stand in for the staging worker's start of the build and the drain
//! workers that run it.
//!
//! The staging harness (connect, stage a CDC row, seal/drain to quiescence)
//! is this file's own.

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::CatalogError;
use trellis::config::{DEFAULT_SCHEMA, DEFAULT_TARGET_SCHEMA};
use trellis::defs::{TransformStatus, ValueType, install_definition};
use trellis::staging::apply;
use trellis::staging::{has_pending, retire_drained_segments};

/// Starts the Re-derive build of every registered definition and runs each
/// to completion, so each is `live` when this returns — the test-harness
/// stand-in for the staging worker and a running `application_threads`
/// drain worker, since `install_definition` only records a definition
/// `WaitingToBackfill` (ADR-0002). Panics (via `expect`) rather than
/// swallowing an error, matching this file's other harness helpers
/// (`drain_to_quiescence`) — a build failure here means the test itself is
/// broken, not something to retry past.
async fn drain_backfill_chunks(pool: &trellis::Pool) {
    // ADR-0002: registration only records a definition; the staging
    // worker starts its build.
    trellis::intake::harness::start_registered_builds(pool)
        .await
        .expect("dispatch registered definitions' builds");
    trellis::staging::build::settle_builds(pool).await;
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

/// How many rows are currently staged in the active segment for `src_table`:
/// zero after a build, which reads the source itself (see module doc).
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
/// real CDC producer always stages a fully-qualified `src_table`,
/// and `compute`'s forward-propagation lookup
/// (`catalog::transforms_for_source`) requires that exact qualified
/// identity to match `schema_nodes`/`schema_edges` (ADR-0011).
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
    let lsn = testkit::wal_insert_lsn(client).await;
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

/// A composite primary-key identity string, U+001F-joined in the key's own
/// declared column order: the encoding the capture trigger produces, and the one
/// `ddl::pk_key_sql_expr`/`ddl::split_pk_key` on the apply side agree with.
fn composite_key(parts: &[&str]) -> String {
    parts.join("\u{1f}")
}

/// Seals and drains repeatedly until nothing is pending anywhere in the ring.
async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    // A throwaway, always-caught-up watermark — this helper
    // has no live capture running, and none of this file's tests exercise guard (a) specifically, so a
    // real watermark would only ever make guard (a) reject spuriously.
    for _ in 0..16 {
        let seg = seal_active_segment(client).await;
        while apply::drain_once(pool, seg, "install_def_test", 1, "trellis_install_def_test")
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
// Fast-path success branch: a plain 1-1 definition.
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

    // Registration only records the definition (ADR-0002) — run its build
    // to completion the way the staging worker and a running
    // `application_threads` drain worker would before asserting on the
    // target's contents.
    trellis::intake::harness::settle_builds(&db.pool).await;

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

    // The build reads the source itself and stages no `Recompute` rows in
    // the ring: zero staged rows for `s`.
    assert_eq!(
        staged_count_for_source(&client, &format!("{DEFAULT_SCHEMA}.s")).await,
        0,
        "the build must not stage the source into the ring"
    );
}

/// The Re-derive build of a plain 1-1 over an explicitly-qualified
/// source: its plan and chunk SQL must read `custom.orders`, not the
/// `public.orders` decoy on the pool's pinned `search_path`.
#[tokio::test]
async fn rederive_build_reads_the_explicitly_qualified_source_not_a_same_named_decoy() {
    explicitly_qualified_source_is_read_not_a_same_named_decoy(false).await;
}

/// The decoy test's body: `seam_fed` makes `custom.orders` read as another
/// definition's target before the definition is registered.
async fn explicitly_qualified_source_is_read_not_a_same_named_decoy(seam_fed: bool) {
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
    if seam_fed {
        trellis::intake::harness::feed_from_a_test_definition(&client, "custom.orders")
            .await
            .expect("make custom.orders read as a seam-fed source");
    }

    let cols = numeric(&["price"]);
    install_definition(
        &db.pool,
        "TRANSFORM order_totals FROM custom.orders SELECT price AS total",
        &cols,
        "public",
    )
    .await
    .expect("install_definition against the explicitly-qualified source");

    // The start records the Re-derive build (`build = 'rederive'`).
    trellis::intake::harness::start_registered_builds(&db.pool)
        .await
        .expect("dispatch the build");
    let build: Option<String> = client
        .query_one(
            "select build from transform_definitions where target_table = 'public.order_totals'",
            &[],
        )
        .await
        .expect("read which build runs")
        .get(0);
    assert_eq!(
        build.as_deref() == Some("rederive"),
        !seam_fed,
        "the shape picks the build under test: {build:?}"
    );

    // Run the build to completion the way a real drain worker would.
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

/// ADR-0011, through the real front door: an
/// explicitly-qualified `TRANSFORM <schema>.<target>` must override the
/// `target_schema` argument this function is called with, for *every* step —
/// the physical `CREATE TABLE` DDL, the build's writes, and the
/// persisted qualified identity all need to agree on `custom`, not the
/// `"public"` this call still passes as its own `target_schema` argument
/// (mirroring a real caller who never changed `Config::target_schema` but
/// wants to redirect just this one definition). This is the regression this
/// module's own bug would have reintroduced: checking the target's existence
/// *before* this function's DDL step
/// ran would reject every legitimate explicit-target install outright
/// (the table doesn't exist yet — DDL is what's about to create it) —
/// exercising the fast path (not just `create_definition`'s ring path, which
/// `defs_catalog.rs`'s own tests already cover) is what catches
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
// Status lifecycle: `install_definition` persists exactly one
// `WaitingToBackfill` row, which its build flips in place to `Live`.
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

    // ADR-0002: `install_definition` returns once the definition is
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
        "the persisted row waits for the staging worker to start its build"
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

/// A doubling alias chain (`f<k> = f<k-1> + f<k-1>`) whose inlined form
/// outgrows the substitution budget every build needs is refused at define.
#[tokio::test]
async fn install_definition_refuses_fields_that_inline_past_the_budget() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table s (id bigint primary key, a numeric); \
             insert into s (id, a) values (1, 1)",
        )
        .await
        .expect("create + seed the source");

    let chain: Vec<String> = std::iter::once("a + a AS f0".to_string())
        .chain((1..=17).map(|k| format!("f{} + f{} AS f{k}", k - 1, k - 1)))
        .collect();
    let err = install_definition(
        &db.pool,
        &format!("TRANSFORM chain FROM s SELECT {}", chain.join(", ")),
        &numeric(&["a"]),
        "public",
    )
    .await
    .expect_err("every build inlines the chain into one expression");
    assert!(
        matches!(
            err,
            trellis::defs::CatalogError::Validate(
                trellis::defs::ValidationError::FieldExpansionTooLarge
            )
        ),
        "{err:?}"
    );
    let left_behind: i64 = client
        .query_one(
            "select count(*) from information_schema.tables where table_name = 'chain'",
            &[],
        )
        .await
        .expect("look for the target")
        .get(0);
    assert_eq!(left_behind, 0, "a refused definition leaves no target");
}

// ---------------------------------------------------------------------
// Fast-path success branch: a plain 1-1 field that references another
// calculated field's alias (`double_price + tax AS total` where
// `double_price = price + price`). The build inlines the alias chain
// (`substitute_all_fields`) and builds it set-based, so the source is never
// staged into the ring.
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
    // so the build renders self-contained source SQL.
    install_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT price + price AS double_price, \
         double_price + tax AS total",
        &cols,
        "public",
    )
    .await
    .expect("install_definition builds the alias chain directly");

    // Run the build to completion before reading the target.
    trellis::intake::harness::settle_builds(&db.pool).await;

    // The build populates the target and stages nothing in the ring (see
    // the sibling fast-path test).
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
        "the build computes the alias chain correctly for every row"
    );
    assert_eq!(
        staged_count_for_source(&client, &format!("{DEFAULT_SCHEMA}.s")).await,
        0,
        "the build must not stage the source into the ring"
    );
}

// ---------------------------------------------------------------------
// Fast-path success branch: an Aggregate (`GROUP BY`) definition whose
// `GROUP BY` field references another calculated field's alias (the
// Aggregate-key-space counterpart of the plain cross-field-alias test above
// — `total + total AS double_total` where `total = SUM(amount)`). The build
// inlines the alias first (`substituted_field_exprs`), same as the 1-1 path,
// rather than rendering `double_total`'s raw `Expr::Column("total")` as a
// bare SQL identifier, which Postgres rejects (`total` names neither a
// source column nor a same-SELECT-list-visible name).
// ---------------------------------------------------------------------

#[tokio::test]
async fn install_definition_fast_path_builds_an_aggregate_cross_field_alias_chain() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_items \
             (id bigint primary key, order_id bigint, amount numeric(12,0)); \
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
        WITHOUT SOURCE INDEX SELECT order_id AS order_id, SUM(amount) AS total, \
         total + total AS double_total",
        &cols,
        "public",
    )
    .await
    .expect("install_definition builds the aggregate alias chain directly");
    // ADR-0002: the build runs as a background job the staging
    // worker starts; run it before reading the target.
    trellis::intake::harness::settle_builds(&db.pool).await;

    // The build's plan job writes the target in one absolute write and
    // stages nothing in the ring (see the sibling fast-path tests).
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
        "the build computes the aggregate alias chain correctly for every group \
         (double_total = 2 * sum(amount))"
    );
    assert_eq!(
        staged_count_for_source(&client, &format!("{DEFAULT_SCHEMA}.order_items")).await,
        0,
        "the build must not stage the source into the ring"
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
             (id bigint primary key, order_id bigint, amount numeric(12,0)); \
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
        WITHOUT SOURCE INDEX SELECT order_id AS order_id, total AS grand_total, SUM(amount) AS total",
        &cols,
        "public",
    )
    .await
    .expect("install_definition builds a bare alias of a SUM field declared before it");
    // ADR-0002: the build runs as a background job the staging
    // worker starts; run it before reading the target.
    trellis::intake::harness::settle_builds(&db.pool).await;

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
/// structurally identical (`SUM(amount)`), so the group basis keeps one
/// partial for the argument in its hidden table and every alias reads it,
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
             (id bigint primary key, order_id bigint, amount numeric(12,0)); \
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
        WITHOUT SOURCE INDEX SELECT order_id AS order_id, grand_total AS super_total, \
         total AS grand_total, SUM(amount) AS total",
        &cols,
        "public",
    )
    .await
    .expect("install_definition shares one count column across several SUM aliases");
    // ADR-0002: the build runs as a background job the staging
    // worker starts; run it before reading the target.
    trellis::intake::harness::settle_builds(&db.pool).await;

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
// A coalesce-wrapped aggregate (`coalesce(sum(word_count), 0)`),
// the normal way to write a nullable aggregate: the aggregate nested inside
// `coalesce` builds with an all-`NULL` group coalesced to 0.
// ---------------------------------------------------------------------

#[tokio::test]
async fn install_definition_fast_path_builds_a_coalesce_wrapped_aggregate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table posts \
             (id bigint primary key, author_id bigint, word_count numeric(12,0)); \
             insert into posts (id, author_id, word_count) values \
             (100, 1, 10), (101, 1, 20), (102, 2, null)",
        )
        .await
        .expect("create + seed tables");

    let source_columns = columns(&[
        ("author_id", ValueType::Numeric),
        ("word_count", ValueType::Numeric),
    ]);

    install_definition(
        &db.pool,
        "TRANSFORM author_totals FROM posts GROUP BY author_id WITHOUT SOURCE INDEX \
         SELECT author_id AS author_id, coalesce(sum(word_count), 0) AS total_words",
        &source_columns,
        "public",
    )
    .await
    .expect("install_definition builds a coalesce-wrapped aggregate");
    trellis::intake::harness::settle_builds(&db.pool).await;

    let mut rows: Vec<(String, String)> = client
        .query(
            "select author_id::text, total_words::text from author_totals order by author_id",
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
            // Only NULL word counts -> coalesce(NULL, 0) = 0, not NULL.
            ("2".to_string(), "0".to_string()),
        ],
        "coalesce(sum(...), 0) builds with the all-NULL group coalesced to 0"
    );
    assert_eq!(
        staged_count_for_source(&client, &format!("{DEFAULT_SCHEMA}.posts")).await,
        0,
        "the build must not stage the source into the ring"
    );
}

// ---------------------------------------------------------------------
// The deepest nesting: an alias-derived field
// summing two coalesce-wrapped aggregates (`total_words = total_posted_words
// + total_commented_words`, each itself `coalesce(sum(...), 0)`).
// Substitution inlines both, giving `coalesce(sum(posted), 0) +
// coalesce(sum(commented), 0)`.
// ---------------------------------------------------------------------

#[tokio::test]
async fn install_definition_fast_path_builds_nested_coalesce_alias_chain() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table posts (id bigint primary key, author_id bigint, \
             posted numeric(12,0), commented numeric(12,0)); \
             insert into posts (id, author_id, posted, commented) values \
             (100, 1, 10, 3), (101, 1, 20, 4), (102, 2, null, null)",
        )
        .await
        .expect("create + seed tables");

    let source_columns = columns(&[
        ("author_id", ValueType::Numeric),
        ("posted", ValueType::Numeric),
        ("commented", ValueType::Numeric),
    ]);

    install_definition(
        &db.pool,
        "TRANSFORM author_totals FROM posts GROUP BY author_id WITHOUT SOURCE INDEX SELECT \
         author_id AS author_id, \
         coalesce(sum(posted), 0) AS total_posted_words, \
         coalesce(sum(commented), 0) AS total_commented_words, \
         total_posted_words + total_commented_words AS total_words",
        &source_columns,
        "public",
    )
    .await
    .expect("install_definition builds the nested coalesce alias chain");
    trellis::intake::harness::settle_builds(&db.pool).await;

    let mut rows: Vec<(String, String, String, String)> = client
        .query(
            "select author_id::text, total_posted_words::text, \
             total_commented_words::text, total_words::text \
             from author_totals order by author_id",
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
            // posted 10+20=30, commented 3+4=7, total 37.
            (
                "1".to_string(),
                "30".to_string(),
                "7".to_string(),
                "37".to_string()
            ),
            // Only NULLs -> every coalesce(NULL, 0) = 0.
            (
                "2".to_string(),
                "0".to_string(),
                "0".to_string(),
                "0".to_string()
            ),
        ],
        "nested coalesce+alias chain builds with correct sums"
    );
    assert_eq!(
        staged_count_for_source(&client, &format!("{DEFAULT_SCHEMA}.posts")).await,
        0,
        "the build must not stage the source into the ring"
    );
}

// ---------------------------------------------------------------------
// `install_definition`'s own source resolution (`resolve_source_for_install`,
// and the since-retired coverage-fence planning that ran beside it) walked
// bare `def.source` only through
// `search_path` (`resolve_source_schema`/`resolve_source_schema_in_txn`),
// never through `resolve_graph_identity`/`resolve_graph_identity_in_txn`'s
// bare-target-suffix fallback the way `create_definition_inner`'s own
// resolution of the identical bare source already does. Since
// `install_definition` runs its own DDL *before*
// `create_definition_inner` is ever reached — a distinct fast path, not a
// thin wrapper around the ring — a second definition's bare `FROM t` failed
// to find a first definition's target explicitly qualified into a schema
// outside the fixed `search_path` `pool::session_bootstrap` pins, even
// though the ring path (`create_definition`) already resolved the identical
// chain correctly.
// ---------------------------------------------------------------------

/// Plain 1-1 repro: exercises `resolve_source_for_install`
/// specifically, since a plain 1-1 definition's DDL step
/// (`ddl::source_primary_key(pool, &qualified_source)`, run directly inside
/// `install_definition` before it registers the definition for its build)
/// is the very first place this gap could fail.
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

/// ADR-0002: registration reads no source rows. It succeeds while
/// another session holds the source `ACCESS EXCLUSIVE` (which blocks every
/// read of it) — and goes live once the lock is released and its build runs. The timeout only turns a registration stuck on the lock
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
    locker
        .batch_execute("commit")
        .await
        .expect("release the lock");
    drain_backfill_chunks(&db.pool).await;
    drain_to_quiescence(&db.pool, &mut client).await;

    let status: String = client
        .query_one("select status from transform_definitions", &[])
        .await
        .expect("read the status")
        .get(0);
    assert_eq!(status, "live");
    let mismatches: i64 = client
        .query_one(
            "select count(*) from s left join t on t.id = s.id \
             where t.x is distinct from s.a + s.a",
            &[],
        )
        .await
        .expect("compare the target to the source")
        .get(0);
    assert_eq!(mismatches, 0, "the target is built from every source row");
}

// ---------------------------------------------------------------------
// Registration never adopts a relation it didn't create, and a
// failed registration leaves nothing behind that would block a retry.
// ---------------------------------------------------------------------

async fn relation_exists(client: &Client, schema: &str, name: &str) -> bool {
    client
        .query_one(
            "select exists (select 1 from pg_class c \
             join pg_namespace n on n.oid = c.relnamespace \
             where n.nspname = $1 and c.relname = $2)",
            &[&schema, &name],
        )
        .await
        .expect("check pg_class")
        .get(0)
}

async fn definition_count(client: &Client) -> i64 {
    client
        .query_one("select count(*) from transform_definitions", &[])
        .await
        .expect("count definitions")
        .get(0)
}

/// A user table, a view, or another definition's target under the target's
/// qualified name is refused with `TargetTableExists`, and the existing
/// relation (and its rows) is left exactly as it was.
#[tokio::test]
async fn install_definition_refuses_a_target_name_that_already_exists() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table public.s (id bigint primary key, a numeric); \
             insert into public.s values (1, 1), (2, 2); \
             create table public.users_table (note text); \
             insert into public.users_table values ('keep me'), ('and me'); \
             create view public.users_view as select 1 as one",
        )
        .await
        .expect("seed the source and the user's relations");
    let cols = numeric(&["a"]);

    for existing in ["users_table", "users_view"] {
        let err = install_definition(
            &db.pool,
            &format!("TRANSFORM {existing} FROM s SELECT a + a AS x"),
            &cols,
            "public",
        )
        .await
        .expect_err("a pre-existing relation must be refused, not adopted");
        match &err {
            CatalogError::TargetTableExists { table } => {
                assert_eq!(table, &format!("public.{existing}"));
            }
            other => panic!("expected TargetTableExists for {existing}, got {other:?}"),
        }
        assert!(
            err.to_string()
                .contains(&format!("\"public.{existing}\" already exists")),
            "the message names the table and says it exists: {err}"
        );
    }

    // The user's table is untouched: same shape, same rows.
    let notes: Vec<String> = client
        .query("select note from public.users_table order by note", &[])
        .await
        .expect("the user's table still reads with its own shape")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(notes, vec!["and me".to_string(), "keep me".to_string()]);
    assert_eq!(definition_count(&client).await, 0, "nothing was registered");

    // Another definition's target is refused the same way.
    install_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT a + a AS x",
        &cols,
        "public",
    )
    .await
    .expect("the first registration creates public.t");
    let err = install_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT a AS y",
        &cols,
        "public",
    )
    .await
    .expect_err("a second registration on the same target is refused");
    assert!(
        matches!(&err, CatalogError::TargetTableExists { table } if table == "public.t"),
        "expected TargetTableExists, got {err:?}"
    );
    assert_eq!(definition_count(&client).await, 1);
}

/// A registration that fails after its target's `create table` has run rolls
/// the table back with the catalog row, so the same registration succeeds on
/// retry. The failure is injected on the catalog insert, the last write
/// registration makes.
#[tokio::test]
async fn a_registration_that_fails_partway_leaves_no_target_and_can_be_retried() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(&format!(
            "create table public.s (id bigint primary key, a numeric); \
             insert into public.s values (1, 1), (2, 2); \
             create function public.fail_registration() returns trigger \
             language plpgsql as $$ begin raise exception 'injected registration failure'; end $$; \
             create trigger fail_registration before insert on {DEFAULT_SCHEMA}.transform_definitions \
             for each row execute function public.fail_registration()"
        ))
        .await
        .expect("seed the source and the failing trigger");
    let cols = numeric(&["a"]);
    let text = "TRANSFORM t FROM s SELECT a + a AS x";

    let err = install_definition(&db.pool, text, &cols, "public")
        .await
        .expect_err("the injected failure fails registration");
    assert!(
        err.to_string().contains("injected registration failure"),
        "the injected failure is what failed it: {err}"
    );
    assert!(
        !relation_exists(&client, "public", "t").await,
        "a failed registration must not leave its target table behind"
    );
    assert_eq!(definition_count(&client).await, 0);

    client
        .batch_execute(&format!(
            "drop trigger fail_registration on {DEFAULT_SCHEMA}.transform_definitions"
        ))
        .await
        .expect("clear the injected failure");

    let def = install_definition(&db.pool, text, &cols, "public")
        .await
        .expect("the retry registers under the same target name");
    assert_eq!(def.target_table, "public.t");
    assert_eq!(def.status, TransformStatus::WaitingToBackfill);
    assert!(relation_exists(&client, "public", "t").await);
}

/// A registration racing a concurrent creator of the same target name is
/// refused with `TargetTableExists`, not a raw database error. Made
/// deterministic by holding the rival's `create table` open in its own
/// transaction until registration is blocked behind it (it passed the
/// existence check, since the rival's table isn't visible yet), then
/// committing the rival.
#[tokio::test]
async fn a_registration_that_loses_a_create_race_is_refused_as_already_existing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut rival = connect_raw(db.dsn()).await;
    let observer = connect_raw(db.dsn()).await;

    rival
        .batch_execute(
            "create table public.s (id bigint primary key, a numeric); \
             insert into public.s values (1, 1)",
        )
        .await
        .expect("seed the source");
    let rival_txn = rival.transaction().await.expect("begin the rival");
    rival_txn
        .batch_execute("create table public.t (id bigint primary key)")
        .await
        .expect("the rival creates the target name, uncommitted");

    let pool = db.pool.clone();
    let registration = tokio::spawn(async move {
        install_definition(
            &pool,
            "TRANSFORM t FROM s SELECT a + a AS x",
            &numeric(&["a"]),
            "public",
        )
        .await
    });

    // Wait until registration's `create table` is blocked behind the rival.
    let mut blocked = false;
    for _ in 0..500 {
        let waiting: i64 = observer
            .query_one(
                "select count(*) from pg_stat_activity \
                 where datname = current_database() and wait_event_type = 'Lock' \
                 and query ilike 'create table%'",
                &[],
            )
            .await
            .expect("poll pg_stat_activity")
            .get(0);
        if waiting > 0 {
            blocked = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        blocked,
        "registration never blocked behind the rival's create"
    );
    rival_txn.commit().await.expect("commit the rival");

    let err = registration
        .await
        .expect("registration task")
        .expect_err("the rival's table must be refused, not adopted");
    assert!(
        matches!(&err, CatalogError::TargetTableExists { table } if table == "public.t"),
        "expected TargetTableExists, got {err:?}"
    );
    assert_eq!(definition_count(&observer).await, 0);
}

/// A definition that closes a table cycle is refused as a cycle, not as a
/// taken target name, even though a cycle-closing target always already
/// exists: the target is only created after the definition's own checks
/// pass, so the error is about what's wrong with the definition.
#[tokio::test]
async fn a_cycle_closing_registration_reports_the_cycle_not_the_existing_target() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute("create table public.s (id bigint primary key, a numeric)")
        .await
        .expect("seed the source");

    install_definition(
        &db.pool,
        "TRANSFORM b FROM s SELECT a + a AS x",
        &numeric(&["a"]),
        "public",
    )
    .await
    .expect("s -> b registers");
    // Stand in for b's build finishing, so b is a valid upstream.
    client
        .execute(
            "update transform_definitions set status = 'live' where target_table = 'public.b'",
            &[],
        )
        .await
        .expect("flip b live");

    let err = install_definition(
        &db.pool,
        "TRANSFORM s FROM b SELECT x AS a",
        &numeric(&["x"]),
        "public",
    )
    .await
    .expect_err("b -> s closes a cycle with s -> b");
    assert!(
        matches!(
            &err,
            CatalogError::Validate(trellis::defs::ValidationError::TableCycle { .. })
        ),
        "expected TableCycle, got {err:?}"
    );
    assert_eq!(
        definition_count(&client).await,
        1,
        "only s -> b is registered"
    );
}

// ---------------------------------------------------------------------
// A field named after a 1-1 source's primary key column.
// ---------------------------------------------------------------------

async fn table_exists(client: &Client, qualified: &str) -> bool {
    client
        .query_one("select to_regclass($1) is not null", &[&qualified])
        .await
        .unwrap()
        .get(0)
}

/// The target already carries the source's key column, so a field of the same
/// name, even the passthrough `id AS id`, would declare it twice. It used to
/// fail install with a raw `42701 column "id" specified more than once`. Now it
/// is refused with a clear error before any DDL, and the refusal names the key
/// even for a non-passthrough (which `validate` alone would report as
/// `CalculatedFieldShadowsSourceColumn`, advising a bare passthrough instead).
#[tokio::test]
async fn install_definition_rejects_a_field_named_after_a_primary_key_column() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    client
        .batch_execute("create table s (id bigint primary key, a numeric)")
        .await
        .expect("seed source");

    for text in [
        "TRANSFORM t FROM s SELECT id AS id, a AS a",
        "TRANSFORM t FROM s SELECT a AS id",
    ] {
        let err = install_definition(&db.pool, text, &numeric(&["id", "a"]), "public")
            .await
            .expect_err("a field named after the key column is refused");
        assert!(
            matches!(
                &err,
                CatalogError::Validate(
                    trellis::defs::ValidationError::FieldNamedAfterPrimaryKey { field }
                ) if field == "id"
            ),
            "{text}: {err:?}"
        );
        assert!(
            !table_exists(&client, "public.t").await,
            "{text}: no target table is left behind"
        );
    }
}

/// A key column selected under another name is an ordinary column that
/// copies the key; nothing collides, and it installs and builds as one.
#[tokio::test]
async fn install_definition_accepts_a_primary_key_column_under_another_name() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table s (id bigint primary key, a numeric); \
             insert into s (id, a) select g, g from generate_series(1, 20) g",
        )
        .await
        .expect("seed source");

    install_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT id AS order_id, a AS a",
        &numeric(&["id", "a"]),
        "public",
    )
    .await
    .expect("a renamed primary key column installs");
    trellis::intake::harness::settle_builds(&db.pool).await;
    let mismatches: i64 = client
        .query_one(
            "select count(*) from s full join t on t.id = s.id \
             where t.id is null or s.id is null or t.order_id is distinct from s.id",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(mismatches, 0);
}

/// `ALTER TRANSFORM ... ADD id AS id` gets the same clear refusal as a first
/// `TRANSFORM`, rather than the raw 42701 its rewrite's insert used to hit.
#[tokio::test]
async fn alter_transform_add_of_a_field_named_after_a_primary_key_column_is_rejected() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table s (id bigint primary key, a numeric); \
             insert into s (id, a) select g, g from generate_series(1, 20) g",
        )
        .await
        .expect("seed source");
    install_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT a AS a",
        &numeric(&["id", "a"]),
        "public",
    )
    .await
    .expect("install");
    drain_backfill_chunks(&db.pool).await;

    for text in [
        "ALTER TRANSFORM t ADD id AS id",
        "ALTER TRANSFORM t ADD a + 1 AS id",
    ] {
        let trellis::defs::Statement::AlterTransform(alter) =
            trellis::defs::parse_statement(text).expect("parse")
        else {
            panic!("an ALTER TRANSFORM statement");
        };
        let err = trellis::defs::alter_transform(&db.pool, &alter)
            .await
            .expect_err("adding a field named after the key column is refused");
        assert!(
            matches!(
                &err,
                CatalogError::Validate(
                    trellis::defs::ValidationError::FieldNamedAfterPrimaryKey { field }
                ) if field == "id"
            ),
            "{text}: {err:?}"
        );
    }
}

/// The key read runs before `validate` for a 1-1 definition with a
/// field named like a source column (`a AS a`). With an explicit `FROM
/// <schema>.<table>` naming a table that doesn't exist, that read would find
/// no key and report `NoPrimaryKey`. The explicit-schema existence check runs
/// first, so the missing table is reported as missing.
#[tokio::test]
async fn an_explicit_source_without_its_table_is_reported_missing_before_the_key_is_read() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    client
        .batch_execute("create schema custom; create table s (id bigint primary key, a numeric)")
        .await
        .expect("seed a same-named table outside the named schema");

    let err = install_definition(
        &db.pool,
        "TRANSFORM t FROM custom.s SELECT a AS a",
        &numeric(&["id", "a"]),
        "public",
    )
    .await
    .expect_err("custom.s doesn't exist");
    assert!(
        matches!(
            &err,
            CatalogError::Validate(
                trellis::defs::ValidationError::QualifiedSourceTableNotFound { schema, table }
            ) if schema == "custom" && table == "s"
        ),
        "{err:?}"
    );
}

/// Regression for DDL-before-validate ordering: if `install_definition` emitted the
/// target table's DDL *before* running `validate()`, a definition rejected for any
/// reason would still leave an orphan table behind. The failure here is plain
/// `UngroupedColumnReference`, because the fix is about ordering for *every*
/// key-space.
#[tokio::test]
async fn an_invalid_aggregate_definition_leaves_no_target_table() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    client
        .batch_execute("create table post_tags (id integer primary key, post integer, tag text)")
        .await
        .expect("create the source");

    let err = install_definition(
        &db.pool,
        "TRANSFORM tag_totals FROM post_tags GROUP BY tag WITHOUT SOURCE INDEX SELECT post AS p",
        &columns(&[
            ("id", ValueType::Numeric),
            ("post", ValueType::Numeric),
            ("tag", ValueType::Text),
        ]),
        "public",
    )
    .await
    .unwrap_err();
    match err {
        CatalogError::Validate(trellis::defs::ValidationError::UngroupedColumnReference {
            field,
            column,
        }) => {
            assert_eq!(field, "p");
            assert_eq!(column, "post");
        }
        other => panic!("expected UngroupedColumnReference, got {other:?}"),
    }
    assert!(
        !table_exists(&client, "public.tag_totals").await,
        "validation must run before any DDL is issued"
    );
}

/// `staging::apply::compute` calls `ddl::source_primary_key` for
/// every source table a drain batch touches, so a composite-PK source
/// feeding an ordinary `GROUP BY` definition must build and then drain a
/// forward insert, a group-migrating update and a delete, each keyed by the
/// composite identity. `order_lines (order_id, line_no)` is composite.
#[tokio::test]
async fn an_aggregate_over_a_composite_pk_source_builds_and_drains() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table order_lines ( \
                 order_id integer, line_no integer, category text, amount numeric(12,0), \
                 primary key (order_id, line_no) \
             ); \
             insert into order_lines (order_id, line_no, category, amount) values \
               (1, 1, 'books', 10), (1, 2, 'books', 5), (2, 1, 'toys', 20)",
        )
        .await
        .expect("create + seed a composite-pk source");

    let source_columns = columns(&[
        ("order_id", ValueType::Numeric),
        ("line_no", ValueType::Numeric),
        ("category", ValueType::Text),
        ("amount", ValueType::Numeric),
    ]);
    install_definition(
        &db.pool,
        "TRANSFORM category_totals FROM order_lines GROUP BY category WITHOUT SOURCE INDEX \
         SELECT SUM(amount) AS total",
        &source_columns,
        "public",
    )
    .await
    .expect("install an aggregate over a composite-pk source");
    trellis::intake::harness::settle_builds(&db.pool).await;
    drain_to_quiescence(&db.pool, &mut client).await;

    let read_totals = async |client: &Client| -> HashMap<String, Option<String>> {
        client
            .query("select category, total::text from category_totals", &[])
            .await
            .expect("read category_totals")
            .into_iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect()
    };

    let totals = read_totals(&client).await;
    assert_eq!(
        totals.get("books"),
        Some(&Some("15".to_string())),
        "the build over pre-existing composite-pk rows"
    );
    assert_eq!(totals.get("toys"), Some(&Some("20".to_string())));

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
        Some(&Some("27".to_string())),
        "after a forward insert"
    );

    // An update that migrates a row from one group to another (`(1, 2)`
    // re-categorized from `books` to `toys`): the old/new-image decode path,
    // keyed by the same composite identity throughout.
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
        Some(&Some("10".to_string())),
        "after migrating a row out of books"
    );
    assert_eq!(
        totals.get("toys"),
        Some(&Some("32".to_string())),
        "after migrating a row into toys (20 + 7 + 5)"
    );

    // A delete (the composite-pk delete/old-image path).
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
        Some(&Some("12".to_string())),
        "after deleting a row (32 - 20)"
    );
}
