//! Integration tests for apply ∪ mark-drained (issue #11, stage 05),
//! 1-1/scalar subset only, run against a real, ephemeral Postgres instance
//! via the shared harness (`testkit::TestCluster`).
//!
//! See docs/staging-and-claiming/05-apply-and-exactly-once-deltas.md for the
//! design these tests hold the implementation to. Every test builds its own
//! source table, definition, and target table by hand (mirroring
//! `defs_ddl.rs`/`defs_oracle.rs`'s convention) and stages changes directly
//! into the ring (mirroring `claims.rs`/`fold.rs`'s convention), rather than
//! going through CDC intake — intake is out of scope here.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use trellis::defs::{
    PgType, create_definition, create_target_table, recompute, source_primary_key,
};
use trellis::staging::apply::{self, ApplyError};
use trellis::staging::{SegmentState, StagedWatermark, TRUNCATE_SENTINEL_KEY, claim, fold};

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
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

/// Connects directly to `dsn` (bypassing `trellis::Pool`), matching
/// `claims.rs`/`fold.rs`'s convention.
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

async fn segment_state(client: &Client, seg_seq: i64) -> SegmentState {
    let state: String = client
        .query_one("select state from segments where seg_seq = $1", &[&seg_seq])
        .await
        .expect("read segment state")
        .get(0);
    SegmentState::from_sql(&state).unwrap_or_else(|| panic!("unrecognized state {state:?}"))
}

async fn drained_mask(client: &Client, seg_seq: i64) -> i64 {
    client
        .query_one(
            "select drained_mask from segments where seg_seq = $1",
            &[&seg_seq],
        )
        .await
        .expect("read drained_mask")
        .get(0)
}

/// Bare (no `.`) `name` qualified under [`DEFAULT_SCHEMA`] — where every
/// bare `create table` in this file's own fixtures actually lands, since
/// `connect_raw` pins `search_path` to `{DEFAULT_SCHEMA}, public` and never
/// qualifies its own DDL. Used by [`insert_cdc_row`]/[`insert_truncate_row`]
/// so a hand-staged ring row's `src_table` matches what a real CDC producer
/// would actually stage (issue #76: always fully-qualified) and, as of
/// issue #74, what `schema_nodes`/`schema_edges` now key on — before #74
/// neither mattered, since `schema_nodes` was itself bare-keyed and the
/// physical read paths (`ddl::source_primary_key`/`read_live_rows_batch`)
/// tolerate a bare name fine via their own connection's `search_path`
/// (Postgres resolves it live), so this file's fixtures got away with
/// staging a bare `src_table` even after #76. Already-qualified input
/// (containing a `.`) passes through unchanged — e.g. a target-table src
/// under `public` some test builds by hand instead of through this helper.
fn qualify_fixture_table(name: &str) -> String {
    if name.contains('.') {
        name.to_string()
    } else {
        format!("{DEFAULT_SCHEMA}.{name}")
    }
}

/// Stages one image-bearing (CDC-shaped) change directly into `table`.
/// `src_table` is qualified via [`qualify_fixture_table`] if it isn't
/// already.
async fn insert_cdc_row(
    client: &Client,
    table: &str,
    src_table: &str,
    key: &str,
    op: &str,
    old_image: Option<&str>,
    new_image: Option<&str>,
) {
    let src_table = qualify_fixture_table(src_table);
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
        .unwrap_or_else(|e| panic!("insert cdc row {key:?} into {table} failed: {e}"));
}

/// Stages a truncate sentinel directly into `table`, for `src_table`.
async fn insert_truncate_row(client: &Client, table: &str, src_table: &str) {
    let src_table = qualify_fixture_table(src_table);
    client
        .execute(
            &format!(
                "insert into {table} (src_table, key, op, hop_gen) \
                 values ($1, $2, 'truncate', 0)"
            ),
            &[&src_table, &TRUNCATE_SENTINEL_KEY],
        )
        .await
        .unwrap_or_else(|e| panic!("insert truncate row into {table} failed: {e}"));
}

fn order_totals_def() -> TransformDef {
    TransformDef {
        target: "order_totals".to_string(),
        source: "orders".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "total".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column("price".to_string())),
                rhs: Box::new(Expr::Column("tax".to_string())),
            },
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    }
}

/// Runs one full drain attempt against `seg_seq` the same way [`drain_once`]
/// does, and panics with the underlying error on failure — this test file's
/// standard "drain and expect it to succeed" step.
async fn drain(pool: &trellis::Pool, seg_seq: i64, claimed_by: &str) -> apply::ApplyOutcome {
    // Issue #132: a throwaway, always-caught-up watermark — no live
    // `Intake` runs in this test file, and it isn't exercising guard (a).
    apply::drain_once(
        pool,
        seg_seq,
        claimed_by,
        1,
        "trellis_apply_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("drain_once")
    .expect("drain_once must claim and drain something")
}

#[tokio::test]
async fn drain_matches_the_oracle_across_an_insert_update_and_delete() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // `orders` starts empty — its rows arrive below, after the definition
    // exists, purely as this batch's staged CDC events. That keeps the
    // definition's own initial backfill (which enumerates whatever `orders`
    // holds at definition time) from separately re-discovering and writing
    // the same rows this test's hand-staged changes are about to describe,
    // which would collide with them in the same segment.
    client
        .batch_execute("create table orders (id integer primary key, price numeric, tax numeric)")
        .await
        .expect("seed source table");

    let def = order_totals_def();
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("create target table");

    // The live source table's *final* state: order 3 has already been
    // deleted (a real CDC delete would have removed it from `orders` too;
    // this test's "live" table always reflects that end state), 1 and 2
    // are present at the values their staged changes carry.
    client
        .execute(
            "insert into orders (id, price, tax) values (1, 10.00, 1.50), (2, 20.00, 2.00)",
            &[],
        )
        .await
        .expect("seed source rows after the definition exists");

    // Pre-populate a target row for order 3, standing in for data an
    // earlier drain wrote before this batch's delete arrives.
    client
        .execute("insert into order_totals (id, total) values (3, 999)", &[])
        .await
        .expect("pre-populate target row for the key this batch deletes");

    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "2",
        "update",
        Some(r#"{"price":"15.00","tax":"1.00"}"#),
        Some(r#"{"price":"20.00","tax":"2.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "3",
        "delete",
        Some(r#"{"price":"5.00","tax":"0.50"}"#),
        None,
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert_eq!(outcome.keys_written, 2, "orders 1 and 2 must be written");
    assert_eq!(outcome.keys_deleted, 1, "order 3 must be deleted");

    let oracle = recompute(&db.pool, &def, &pk[0].name, &source_columns)
        .await
        .expect("oracle recompute");
    let target_rows = client
        .query("select id::text, total::text from order_totals", &[])
        .await
        .expect("read target table");
    assert_eq!(target_rows.len(), oracle.len());
    for row in target_rows {
        let id: String = row.get(0);
        let total: Option<String> = row.get(1);
        let expected = &oracle[&id];
        assert_eq!(
            total,
            expected["total"].as_ref().map(|n| n.to_string()),
            "total mismatch for id {id}"
        );
    }
}

/// A reviewer's high-severity follow-up to issue #76's grammar work, at the
/// CDC-apply layer this time (`defs_install_definition.rs`'s
/// `install_definition_fast_path_reads_the_explicitly_qualified_source_not_a_same_named_decoy`
/// covers the direct-build/backfill layer): a definition created with an
/// explicit `FROM custom.orders` must have its *live* source reads —
/// [`read_live_rows_batch`], reached whenever a folded change carries no
/// image, e.g. every `Recompute` marker `create_definition`'s own initial
/// ring-backfill enumeration stages for a pre-existing row — actually read
/// `custom.orders`, not a same-named `orders` sitting in one of this pool's
/// own pinned `search_path` schemas (`Config::schema`, `Config::target_schema`,
/// `"public"` — `pool::session_bootstrap`). `public.orders` below is exactly
/// that decoy, holding different rows than the real, explicitly-named
/// `custom.orders`, so a wrong-table live read is unmistakable in the
/// drained target's contents.
#[tokio::test]
async fn explicitly_qualified_source_reads_the_right_table_on_a_live_refetch() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table public.orders (id integer primary key, price numeric); \
             insert into public.orders (id, price) values (1, 999), (2, 888); \
             create schema custom; \
             create table custom.orders (id integer primary key, price numeric); \
             insert into custom.orders (id, price) values (1, 10), (2, 20), (3, 30);",
        )
        .await
        .expect("seed the public.orders decoy and the real custom.orders");

    let source_columns = numeric_columns(&["price"]);
    let pk = source_primary_key(&db.pool, "custom.orders")
        .await
        .expect("introspect custom.orders' primary key");
    let def =
        trellis::defs::parse("TRANSFORM order_totals FROM custom.orders SELECT price AS total")
            .expect("parse the explicitly-qualified definition");
    create_target_table(
        &db.pool,
        &def,
        "public",
        &pk,
        &source_columns,
        "custom.orders",
    )
    .await
    .expect("materialize order_totals ahead of create_definition");

    // `create_definition` enumerates every one of `custom.orders`'s 3
    // pre-existing rows into the active segment as image-less `Recompute`
    // markers — exactly the shape that forces `compute`'s live-refetch path
    // (`read_live_rows_batch`) once drained below.
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM custom.orders SELECT price AS total",
        &source_columns,
    )
    .await
    .expect("create definition against the explicitly-qualified source");

    let seg_seq = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert_eq!(
        outcome.keys_written, 3,
        "all 3 of custom.orders's rows must be written"
    );

    let target_rows: Vec<(i32, Option<String>)> = client
        .query("select id, total::text from order_totals", &[])
        .await
        .expect("read target table")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        target_rows.len(),
        3,
        "custom.orders has 3 rows; a count of 2 would mean the public.orders \
         decoy was read instead"
    );
    let expected: HashMap<i32, &str> = HashMap::from([(1, "10"), (2, "20"), (3, "30")]);
    for (id, total) in target_rows {
        assert_eq!(
            total.as_deref(),
            expected.get(&id).copied(),
            "order_totals must reflect custom.orders's prices for id {id}, \
             not the same-named public.orders decoy"
        );
    }
}

/// The target-side twin of
/// [`explicitly_qualified_source_reads_the_right_table_on_a_live_refetch`]
/// above, and the one this round of issue #78's own fix-sweep exists for: a
/// definition installed with an explicit non-default *target* schema (issue
/// #76's `TRANSFORM custom.<target> FROM ...` grammar) backfills fine (its
/// ring-enumeration and direct-build paths already read
/// `Definition::target_table`/`resolve_source_for_install` correctly), but a
/// subsequent *live* CDC write against its source used to fail outright:
/// `apply_target` (`staging::apply`'s Phase 3 DML-emission function) bound
/// `def.def.target` — always bare, even here — straight into
/// `quote_ident`, so its `INSERT`/`UPDATE`/`DELETE` tried to write
/// `"order_totals"` unqualified, which isn't on this connection's pinned
/// `search_path` (`{DEFAULT_SCHEMA}, "public"}` — see `qualify_fixture_table`'s
/// own doc comment) and so does not exist from its point of view, even
/// though `custom.order_totals` (the real, already-backfilled target) does.
/// Confirmed empirically before this fix: this exact repro raised a bare
/// `relation "order_totals" does not exist`.
///
/// Drains an insert, an update, and a delete against `orders` — the same
/// three-op shape [`drain_matches_the_oracle_across_an_insert_update_and_delete`]
/// covers for the unqualified-target case — and checks `custom.order_totals`
/// (not a same-named `public.order_totals`/`{DEFAULT_SCHEMA}.order_totals`
/// decoy) reflects every one of them.
#[tokio::test]
async fn explicitly_qualified_target_receives_a_live_cdc_write() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create schema custom; \
             create table orders (id integer primary key, price numeric, tax numeric)",
        )
        .await
        .expect("seed source table and the custom schema");

    let source_columns = numeric_columns(&["id", "price", "tax"]);
    let def_text = "TRANSFORM custom.order_totals FROM orders SELECT price + tax AS total";
    let def = trellis::defs::parse(def_text).expect("parse the explicitly-qualified target");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "custom", &pk, &source_columns, &def.source)
        .await
        .expect("materialize custom.order_totals ahead of create_definition");

    // `orders` starts empty (mirroring this file's other tests' convention)
    // so `create_definition`'s own ring-enumeration backfill enumerates
    // nothing, and every row below arrives purely as this batch's staged CDC
    // events instead.
    create_definition(&db.pool, def_text, &source_columns)
        .await
        .expect("create definition against the explicitly-qualified target");

    client
        .execute(
            "insert into orders (id, price, tax) values (1, 10.00, 1.50), (2, 20.00, 2.00)",
            &[],
        )
        .await
        .expect("seed source rows after the definition exists");

    // Pre-populate a target row for order 3, standing in for data an earlier
    // drain wrote before this batch's delete arrives.
    client
        .execute(
            "insert into custom.order_totals (id, total) values (3, 999)",
            &[],
        )
        .await
        .expect("pre-populate target row for the key this batch deletes");

    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "2",
        "update",
        Some(r#"{"price":"15.00","tax":"1.00"}"#),
        Some(r#"{"price":"20.00","tax":"2.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "3",
        "delete",
        Some(r#"{"price":"5.00","tax":"0.50"}"#),
        None,
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert_eq!(outcome.keys_written, 2, "orders 1 and 2 must be written");
    assert_eq!(outcome.keys_deleted, 1, "order 3 must be deleted");

    let target_rows: Vec<(i32, Option<String>)> = client
        .query("select id, total::text from custom.order_totals", &[])
        .await
        .expect(
            "custom.order_totals must exist and be readable — a bare, unqualified \
             write would have raised relation \"order_totals\" does not exist instead",
        )
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    let expected: HashMap<i32, &str> = HashMap::from([(1, "11.50"), (2, "22.00")]);
    assert_eq!(
        target_rows.len(),
        2,
        "order 3 must be gone and only orders 1 and 2 remain"
    );
    for (id, total) in target_rows {
        assert_eq!(
            total.as_deref(),
            expected.get(&id).copied(),
            "custom.order_totals.total mismatch for id {id}"
        );
    }

    let bare_decoy_exists: bool = client
        .query_one(
            "select exists (select 1 from information_schema.tables \
             where table_name = 'order_totals' and table_schema <> 'custom')",
            &[],
        )
        .await
        .expect("check for a same-named decoy outside the custom schema")
        .get(0);
    assert!(
        !bare_decoy_exists,
        "the live write must land in custom.order_totals, never create/touch a \
         same-named table in some other schema on the connection's search_path"
    );
}

/// Issue #380: two definitions over same-named source tables in different
/// schemas (`blog.posts`, `shop.posts`) must each drain against their own
/// source. The drain's version fence used to look `source_table_versions` up
/// by the bare table-name suffix, which matched both rows and failed every
/// drain touching either source with `RowCount`. `compute` also bucketed
/// changes by that bare suffix, so both tables' changes shared one bucket
/// and were evaluated against whichever table staged first.
///
/// One batch carries a change to each table under the same key, with
/// different values, so each target must end up holding its own table's
/// value.
#[tokio::test]
async fn same_named_sources_in_different_schemas_each_drain_against_their_own_table() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create schema blog; create schema shop; \
             create table blog.posts (id integer primary key, score numeric); \
             create table shop.posts (id integer primary key, score numeric);",
        )
        .await
        .expect("seed blog.posts and shop.posts");

    let source_columns = numeric_columns(&["id", "score"]);
    for (schema, target) in [("blog", "blog_feed"), ("shop", "shop_feed")] {
        let def_text = format!("TRANSFORM {target} FROM {schema}.posts SELECT score AS s");
        let def = trellis::defs::parse(&def_text).expect("parse the definition");
        let source = format!("{schema}.posts");
        let pk = source_primary_key(&db.pool, &source)
            .await
            .expect("introspect the source's primary key");
        create_target_table(&db.pool, &def, "public", &pk, &source_columns, &source)
            .await
            .expect("materialize the target ahead of create_definition");
        create_definition(&db.pool, &def_text, &source_columns)
            .await
            .expect("create definition");
    }

    client
        .batch_execute(
            "insert into blog.posts (id, score) values (1, 10); \
             insert into shop.posts (id, score) values (1, 20);",
        )
        .await
        .expect("seed source rows after the definitions exist");
    insert_cdc_row(
        &client,
        "seg_0",
        "blog.posts",
        "1",
        "insert",
        None,
        Some(r#"{"id":1,"score":"10"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "shop.posts",
        "1",
        "insert",
        None,
        Some(r#"{"id":1,"score":"20"}"#),
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert_eq!(outcome.keys_written, 2, "one write per target");

    for (target, expected) in [("blog_feed", "10"), ("shop_feed", "20")] {
        let rows: Vec<(i32, Option<String>)> = client
            .query(&format!("select id, s::text from {target}"), &[])
            .await
            .expect("read target")
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
        assert_eq!(
            rows,
            vec![(1, Some(expected.to_string()))],
            "{target} must hold its own source's value"
        );
    }
}

#[tokio::test]
async fn a_fully_drained_single_bucket_batch_flips_the_segment_to_drained() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // `orders` starts empty so the definition's own initial backfill (below)
    // enumerates nothing; the row arrives afterward, purely as this batch's
    // staged CDC event, so it doesn't collide with a backfill-staged
    // recompute for the same key in the same segment.
    client
        .batch_execute("create table orders (id integer primary key, price numeric, tax numeric)")
        .await
        .expect("seed source table");

    let def = order_totals_def();
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("create target table");

    client
        .execute(
            "insert into orders (id, price, tax) values (1, 10.00, 1.50)",
            &[],
        )
        .await
        .expect("seed source rows after the definition exists");

    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    assert_eq!(segment_state(&client, seg_seq).await, SegmentState::Sealed);

    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert!(
        outcome.batch_drained,
        "a lone worker's one claim over a single-bucket batch must drain it fully"
    );
    assert_eq!(segment_state(&client, seg_seq).await, SegmentState::Drained);
    assert_eq!(drained_mask(&client, seg_seq).await, 1);
}

#[tokio::test]
async fn a_claim_lost_mid_drain_rolls_back_and_applies_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // `orders` starts empty so the definition's own initial backfill enumerates
    // nothing; the row arrives afterward as this batch's staged CDC event.
    client
        .batch_execute("create table orders (id integer primary key, price numeric, tax numeric)")
        .await
        .expect("seed source table");

    let def = order_totals_def();
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("create target table");

    client
        .execute(
            "insert into orders (id, price, tax) values (1, 10.00, 1.50)",
            &[],
        )
        .await
        .expect("seed source rows after the definition exists");

    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;

    // Phase 1 by hand: claim, own, fold, commit — exactly what
    // `drain_once`'s opening block does.
    let mut phase1_client = db.pool.get().await.expect("connection");
    let txn = phase1_client.transaction().await.expect("begin phase 1");
    claim::claim(&*txn, seg_seq, "worker", 1)
        .await
        .expect("claim");
    let filter = claim::owned_bucket_filter(&*txn, seg_seq, "worker")
        .await
        .expect("owned_bucket_filter");
    let folded = fold::fold(&txn, seg_seq, filter).await.expect("fold");
    txn.commit().await.expect("commit phase 1");
    assert!(!folded.is_empty());

    // Simulate the claim being reclaimed out from under this worker in the
    // gap between phase 1 and phase 3 (e.g. a TTL sweep).
    client
        .execute(
            "delete from seg_claims where seg_seq = $1 and claimed_by = 'worker'",
            &[&seg_seq],
        )
        .await
        .expect("simulate a reclaim");

    let plan = apply::compute(&db.pool, &folded).await.expect("compute");
    let mut phase3_client = db.pool.get().await.expect("connection");
    let txn = phase3_client.transaction().await.expect("begin phase 3");
    let err = apply::apply_and_mark_drained(
        &txn,
        seg_seq,
        "worker",
        &plan,
        "trellis_apply_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect_err("the claim is gone; completion must fail");
    assert!(matches!(err, ApplyError::ClaimLost), "got {err:?}");
    txn.rollback().await.expect("rollback phase 3");

    // Nothing was applied: the insert never got past the rolled-back
    // transaction.
    let count: i64 = client
        .query_one("select count(*) from order_totals", &[])
        .await
        .expect("count target rows")
        .get(0);
    assert_eq!(
        count, 0,
        "the rolled-back apply must not have written anything"
    );
    assert_eq!(
        drained_mask(&client, seg_seq).await,
        0,
        "the rolled-back completion must not have marked any bucket drained"
    );
}

#[tokio::test]
async fn a_definition_change_on_a_touched_source_trips_the_version_fence() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table orders (id integer primary key, price numeric, tax numeric); \
             insert into orders (id, price, tax) values (1, 10.00, 1.50)",
        )
        .await
        .expect("seed source table");

    let def = order_totals_def();
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("create target table");

    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;

    let mut phase1_client = db.pool.get().await.expect("connection");
    let txn = phase1_client.transaction().await.expect("begin phase 1");
    claim::claim(&*txn, seg_seq, "worker", 1)
        .await
        .expect("claim");
    let filter = claim::owned_bucket_filter(&*txn, seg_seq, "worker")
        .await
        .expect("owned_bucket_filter");
    let folded = fold::fold(&txn, seg_seq, filter).await.expect("fold");
    txn.commit().await.expect("commit phase 1");

    let plan = apply::compute(&db.pool, &folded).await.expect("compute");

    // A second definition landing against `orders` after phase 2 loaded its
    // version — a real concurrent `create_definition` call would do this;
    // bumping the row directly is equivalent and avoids re-parsing a second
    // definition text just to move the counter.
    client
        .execute(
            // Issue #72: `source_table` is now persisted fully-qualified
            // (`{schema}.orders`, not bare `orders`) — `orders` was created
            // bare above, so it landed in the pool's default first
            // search-path schema, `DEFAULT_SCHEMA`.
            &format!(
                "update source_table_versions set version = version + 1 \
                 where source_table = '{DEFAULT_SCHEMA}.orders'"
            ),
            &[],
        )
        .await
        .expect("bump orders' version, simulating a concurrent definition change");

    let mut phase3_client = db.pool.get().await.expect("connection");
    let txn = phase3_client.transaction().await.expect("begin phase 3");
    let err = apply::apply_and_mark_drained(
        &txn,
        seg_seq,
        "worker",
        &plan,
        "trellis_apply_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect_err("orders' version moved since compute; the fence must trip");
    match &err {
        ApplyError::VersionFenceMiss { src_table } => {
            assert_eq!(src_table, &format!("{DEFAULT_SCHEMA}.orders"))
        }
        other => panic!("expected VersionFenceMiss, got {other:?}"),
    }
    txn.rollback().await.expect("rollback phase 3");

    let count: i64 = client
        .query_one("select count(*) from order_totals", &[])
        .await
        .expect("count target rows")
        .get(0);
    assert_eq!(count, 0, "a fence miss must not have written anything");
}

#[tokio::test]
async fn a_definition_change_on_an_unrelated_source_does_not_trip_the_fence() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // `orders` starts empty so order_totals' own initial backfill enumerates
    // nothing; its row arrives afterward as this batch's staged CDC event.
    client
        .batch_execute(
            "create table orders (id integer primary key, price numeric, tax numeric); \
             create table widgets (id integer primary key, cost numeric)",
        )
        .await
        .expect("seed source tables");

    let def = order_totals_def();
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create order_totals definition");
    // `widgets` is never touched by this batch — its own version bump below
    // must not be in this batch's fence set at all.
    let widget_columns = numeric_columns(&["id", "cost"]);
    create_definition(
        &db.pool,
        "TRANSFORM widget_costs FROM widgets SELECT cost + cost AS doubled",
        &widget_columns,
    )
    .await
    .expect("create widget_costs definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("create target table");

    client
        .execute(
            "insert into orders (id, price, tax) values (1, 10.00, 1.50)",
            &[],
        )
        .await
        .expect("seed source rows after the definitions exist");

    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;

    let mut phase1_client = db.pool.get().await.expect("connection");
    let txn = phase1_client.transaction().await.expect("begin phase 1");
    claim::claim(&*txn, seg_seq, "worker", 1)
        .await
        .expect("claim");
    let filter = claim::owned_bucket_filter(&*txn, seg_seq, "worker")
        .await
        .expect("owned_bucket_filter");
    let folded = fold::fold(&txn, seg_seq, filter).await.expect("fold");
    txn.commit().await.expect("commit phase 1");

    let plan = apply::compute(&db.pool, &folded).await.expect("compute");

    // Change `widgets`, not `orders` — this batch never evaluated against
    // `widgets`, so it must not be in the fence set.
    client
        .execute(
            // Issue #72: `source_table` is persisted fully-qualified now.
            &format!(
                "update source_table_versions set version = version + 1 \
                 where source_table = '{DEFAULT_SCHEMA}.widgets'"
            ),
            &[],
        )
        .await
        .expect("bump widgets' version");

    let mut phase3_client = db.pool.get().await.expect("connection");
    let txn = phase3_client.transaction().await.expect("begin phase 3");
    let outcome = apply::apply_and_mark_drained(
        &txn,
        seg_seq,
        "worker",
        &plan,
        "trellis_apply_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("an unrelated source's version change must not trip this batch's fence");
    txn.commit().await.expect("commit phase 3");
    assert_eq!(outcome.keys_written, 1);
}

#[tokio::test]
async fn a_write_that_changes_nothing_is_suppressed_as_a_no_op() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table orders (id integer primary key, price numeric, tax numeric); \
             insert into orders (id, price, tax) values (1, 10.00, 1.50)",
        )
        .await
        .expect("seed source table");

    let def = order_totals_def();
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("create target table");

    // Pre-populate the target with the exact value the staged update would
    // also produce (10.00 + 1.50 = 11.50) — the write this batch stages
    // physically changes nothing.
    client
        .execute(
            "insert into order_totals (id, total) values (1, 11.50)",
            &[],
        )
        .await
        .expect("pre-populate target row with the same value");

    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "update",
        Some(r#"{"price":"5.00","tax":"1.50"}"#),
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert_eq!(
        outcome.keys_written, 0,
        "a write that changes nothing must be suppressed, not counted as written"
    );

    let total: String = client
        .query_one("select total::text from order_totals where id = 1", &[])
        .await
        .expect("read target row")
        .get(0);
    assert_eq!(total, "11.50");
}

/// Issue #392's check on the 1-1 path: a `recompute` folded with the key's
/// CDC update leaves a record that looks like a plain update, but a 1-1 write
/// is the whole row evaluated from its image, never a delta on the stored
/// value. So a stale target row (999 here) is still repaired, with no need
/// for the aggregate path's `has_recompute` handling.
#[tokio::test]
async fn a_recompute_folded_with_a_cdc_update_still_repairs_a_stale_one_to_one_row() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table orders (id integer primary key, price numeric, tax numeric); \
             insert into orders (id, price, tax) values (1, 20.00, 1.50)",
        )
        .await
        .expect("seed source table");
    let def = order_totals_def();
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("create target table");
    client
        .execute("insert into order_totals (id, total) values (1, 999)", &[])
        .await
        .expect("a stale target row");

    insert_cdc_row(&client, "seg_0", "orders", "1", "recompute", None, None).await;
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "update",
        Some(r#"{"id":"1","price":"10.00","tax":"1.50"}"#),
        Some(r#"{"id":"1","price":"20.00","tax":"1.50"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;
    drain(&db.pool, seg_seq, "worker").await;

    let total: String = client
        .query_one("select total::text from order_totals where id = 1", &[])
        .await
        .expect("read target row")
        .get(0);
    assert_eq!(total, "21.50");
}

#[tokio::test]
async fn a_truncate_clears_every_target_row_but_a_same_batch_post_truncate_insert_survives() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // `orders` starts empty so order_totals' own initial backfill enumerates
    // nothing; row 4 arrives afterward, purely as this batch's staged CDC
    // event, so it doesn't collide with a backfill-staged recompute for the
    // same key in the same segment.
    client
        .batch_execute("create table orders (id integer primary key, price numeric, tax numeric)")
        .await
        .expect("seed source table");

    let def = order_totals_def();
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create order_totals definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("create order_totals table");

    // A downstream reader of order_totals, to check that keys the truncate
    // physically clears also propagate downstream like any other
    // physically-changed key.
    let order_totals_columns = numeric_columns(&["id", "total"]);
    let summary_def = create_definition(
        &db.pool,
        "TRANSFORM order_summary FROM order_totals SELECT total + total AS grand_total",
        &order_totals_columns,
    )
    .await
    .expect("create order_summary definition");
    create_target_table(
        &db.pool,
        &summary_def.def,
        "public",
        &pk,
        &order_totals_columns,
        &summary_def.def.source,
    )
    .await
    .expect("create order_summary table");

    client
        .execute(
            "insert into orders (id, price, tax) values (4, 40.00, 4.00)",
            &[],
        )
        .await
        .expect("seed source rows after both definitions exist");

    // Rows an earlier drain left behind — the truncate must clear every one
    // of them, and its clear must propagate downstream to order_summary's
    // own already-derived rows for the same keys.
    client
        .execute(
            "insert into order_totals (id, total) values (1, 100), (2, 200), (3, 300)",
            &[],
        )
        .await
        .expect("pre-populate target rows the truncate must clear");
    client
        .execute(
            "insert into order_summary (id, grand_total) values (1, 200), (2, 400), (3, 600)",
            &[],
        )
        .await
        .expect("pre-populate order_summary's own already-derived rows");

    // The truncate sentinel, followed by a post-truncate insert for a new
    // key — both land in `seg_0`, so both are in the one single-bucket batch
    // `seal::seal_phase1` forces whenever `has_truncate` is true.
    insert_truncate_row(&client, "seg_0", "orders").await;
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "4",
        "insert",
        None,
        Some(r#"{"price":"40.00","tax":"4.00"}"#),
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert_eq!(
        outcome.keys_deleted, 3,
        "the truncate must clear all 3 pre-existing rows"
    );
    assert_eq!(
        outcome.keys_written, 1,
        "the post-truncate insert must still apply in the same batch"
    );

    let remaining: Vec<(String, String)> = client
        .query(
            "select id::text, total::text from order_totals order by id",
            &[],
        )
        .await
        .expect("read target table")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        remaining,
        vec![("4".to_string(), "44.00".to_string())],
        "only the post-truncate row must remain"
    );

    // Downstream propagation: the cleared keys 1-3 and the written key 4
    // must all have staged a recompute trigger for order_summary.
    let seg2 = seal_active_segment(&mut client).await;
    let outcome2 = drain(&db.pool, seg2, "worker").await;
    assert_eq!(
        outcome2.keys_deleted, 3,
        "order_summary must have its own rows for 1-3 deleted, driven by the \
         cleared order_totals keys' recompute triggers"
    );
    assert_eq!(
        outcome2.keys_written, 1,
        "order_summary must have written the surviving key 4"
    );
    let summary_remaining: Vec<String> = client
        .query("select id::text from order_summary", &[])
        .await
        .expect("read order_summary")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        summary_remaining,
        vec!["4".to_string()],
        "downstream propagation of the truncate's clear must reach order_summary too"
    );
}

#[tokio::test]
async fn next_claimable_segment_never_hands_out_a_segment_past_an_undrained_truncate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // Segment 1: ordinary, no truncate.
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "a",
        "insert",
        None,
        Some(r#"{"v":"a"}"#),
    )
    .await;
    let seg1 = seal_active_segment(&mut client).await;

    // Segment 2: bears a truncate — forces bucket_count = 1.
    insert_truncate_row(&client, "seg_1", "orders").await;
    let seg2 = seal_active_segment(&mut client).await;

    // Segment 3: ordinary again, sealed after the truncate.
    insert_cdc_row(
        &client,
        "seg_2",
        "orders",
        "b",
        "insert",
        None,
        Some(r#"{"v":"b"}"#),
    )
    .await;
    let seg3 = seal_active_segment(&mut client).await;

    // All three sealed and undrained: the barrier must return the lowest,
    // seg1 — never skipping ahead to the truncate or past it.
    let next = apply::next_claimable_segment(&client)
        .await
        .expect("query barrier");
    assert_eq!(next, Some(seg1));

    // Mark seg1 fully drained directly (isolating the barrier query itself
    // from the claim/apply machinery, which is exercised elsewhere).
    client
        .execute(
            "update segments \
             set state = 'drained', drained_mask = (1::bigint << bucket_count) - 1 \
             where seg_seq = $1",
            &[&seg1],
        )
        .await
        .expect("mark seg1 drained");

    // seg2 (the truncate) is now the lowest undrained segment, and also `B`
    // itself — the barrier must hand it out, not skip past it to seg3.
    let next = apply::next_claimable_segment(&client)
        .await
        .expect("query barrier");
    assert_eq!(
        next,
        Some(seg2),
        "the barrier must not skip the undrained truncate segment"
    );

    // Mark seg2 fully drained too.
    client
        .execute(
            "update segments \
             set state = 'drained', drained_mask = (1::bigint << bucket_count) - 1 \
             where seg_seq = $1",
            &[&seg2],
        )
        .await
        .expect("mark seg2 drained");

    // Only now, with the truncate itself drained, does seg3 become
    // claimable.
    let next = apply::next_claimable_segment(&client)
        .await
        .expect("query barrier");
    assert_eq!(
        next,
        Some(seg3),
        "seg3 must become claimable only once the truncate segment below it has drained"
    );
}

#[tokio::test]
async fn a_change_propagates_two_hops_downstream_then_stops() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // `orders` starts empty so order_totals' own initial backfill enumerates
    // nothing; row 1 arrives afterward, purely as this batch's staged CDC
    // event, so it doesn't collide with a backfill-staged recompute for the
    // same key in the same segment.
    client
        .batch_execute("create table orders (id integer primary key, price numeric, tax numeric)")
        .await
        .expect("seed source table");

    let def = order_totals_def();
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create order_totals definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("create order_totals table");

    // A second definition reading `order_totals` itself — the downstream
    // hop this test exercises. `order_summary` has no downstream reader of
    // its own, so propagation must stop after this hop.
    let order_totals_columns = numeric_columns(&["id", "total"]);
    let summary_def = create_definition(
        &db.pool,
        "TRANSFORM order_summary FROM order_totals SELECT total + total AS grand_total",
        &order_totals_columns,
    )
    .await
    .expect("create order_summary definition");
    create_target_table(
        &db.pool,
        &summary_def.def,
        "public",
        &pk,
        &order_totals_columns,
        &summary_def.def.source,
    )
    .await
    .expect("create order_summary table");

    client
        .execute(
            "insert into orders (id, price, tax) values (1, 10.00, 1.50)",
            &[],
        )
        .await
        .expect("seed source rows after both definitions exist");

    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
    )
    .await;

    // Hop 0: orders -> order_totals, direct apply.
    let seg1 = seal_active_segment(&mut client).await;
    let outcome1 = drain(&db.pool, seg1, "worker").await;
    assert_eq!(outcome1.keys_written, 1);
    let total: String = client
        .query_one("select total::text from order_totals where id = 1", &[])
        .await
        .expect("read order_totals")
        .get(0);
    assert_eq!(total, "11.50");

    // The apply must have staged a Recompute trigger for order_totals's own
    // downstream reader, landing in the ring's active segment — under
    // `order_totals`'s *qualified* identity (issue #267: propagation used to
    // stage the bare `def.target`, which the fold could never coalesce with
    // the qualified spelling CDC intake stages for the very same table once
    // an intermediate hop joins the publication).
    let staged: i64 = client
        .query_one(
            "select count(*) from (
                 select src_table, key from seg_0
                 union all select src_table, key from seg_1
                 union all select src_table, key from seg_2
                 union all select src_table, key from seg_3
             ) rows where src_table = 'public.order_totals' and key = '1'",
            &[],
        )
        .await
        .expect("count staged recompute rows")
        .get(0);
    assert_eq!(
        staged, 1,
        "order_totals must have staged exactly one recompute trigger for order_summary"
    );

    // Hop 1: order_totals -> order_summary, driven by the recompute
    // trigger, which carries no image and must re-read order_totals live.
    let seg2 = seal_active_segment(&mut client).await;
    let outcome2 = drain(&db.pool, seg2, "worker").await;
    assert_eq!(outcome2.keys_written, 1);
    let grand_total: String = client
        .query_one(
            "select grand_total::text from order_summary where id = 1",
            &[],
        )
        .await
        .expect("read order_summary")
        .get(0);
    assert_eq!(grand_total, "23.00", "11.50 + 11.50");

    // No further propagation: order_summary has no downstream reader, so
    // nothing new should have been staged for it.
    let further_staged: i64 = client
        .query_one(
            "select count(*) from (
                 select src_table, key from seg_0
                 union all select src_table, key from seg_1
                 union all select src_table, key from seg_2
                 union all select src_table, key from seg_3
             ) rows where src_table = 'public.order_summary'",
            &[],
        )
        .await
        .expect("count staged rows for order_summary")
        .get(0);
    assert_eq!(
        further_staged, 0,
        "propagation must stop once a target has no downstream reader"
    );
}

/// Issue #63's write-path gap: a text-column passthrough must round-trip
/// through `compute()`/apply with the persisted `source_columns` type map
/// (`catalog::create_definition`), not default every column to Numeric and
/// fail to parse. Also exercises a numeric-*looking* text value ("007") to
/// prove it isn't misparsed as a number and corrupted.
#[tokio::test]
async fn a_text_column_passthrough_round_trips_through_compute() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // `items` starts empty so the definition's own initial backfill
    // enumerates nothing; its rows arrive afterward as this batch's staged
    // CDC events, so they don't collide with a backfill-staged recompute for
    // the same keys in the same segment.
    client
        .batch_execute("create table items (id integer primary key, label text)")
        .await
        .expect("seed source table");

    let def = TransformDef {
        target: "labels".to_string(),
        source: "items".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "out".to_string(),
            expr: Expr::Column("label".to_string()),
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };
    let source_columns = columns(&[("id", ValueType::Numeric), ("label", ValueType::Text)]);
    create_definition(
        &db.pool,
        "TRANSFORM labels FROM items SELECT label AS out",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("create target table");

    client
        .execute(
            "insert into items (id, label) values (1, 'hello world'), (2, '007')",
            &[],
        )
        .await
        .expect("seed source rows after the definition exists");

    insert_cdc_row(
        &client,
        "seg_0",
        "items",
        "1",
        "insert",
        None,
        Some(r#"{"label":"hello world"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "items",
        "2",
        "insert",
        None,
        Some(r#"{"label":"007"}"#),
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert_eq!(outcome.keys_written, 2);

    let rows = client
        .query("select id, out from labels order by id", &[])
        .await
        .expect("read target table");
    let out1: String = rows[0].get(1);
    let out2: String = rows[1].get(1);
    assert_eq!(out1, "hello world");
    assert_eq!(
        out2, "007",
        "a numeric-looking text value must not be misparsed as a number"
    );
}

/// A string-literal calculated field (`SELECT 'hi' AS out`) must write its
/// literal text, not silently collapse to NULL (the pre-fix write-path
/// extraction only unwrapped `Value::Numeric`).
#[tokio::test]
async fn a_string_literal_field_writes_its_value_through_compute() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // `items` starts empty so the definition's own initial backfill
    // enumerates nothing; its row arrives afterward as this batch's staged
    // CDC event.
    client
        .batch_execute("create table items (id integer primary key)")
        .await
        .expect("seed source table");

    let def = TransformDef {
        target: "literals".to_string(),
        source: "items".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "out".to_string(),
            expr: Expr::StringLiteral("hi".to_string()),
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };
    let source_columns = numeric_columns(&["id"]);
    create_definition(
        &db.pool,
        "TRANSFORM literals FROM items SELECT 'hi' AS out",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("create target table");

    client
        .execute("insert into items (id) values (1)", &[])
        .await
        .expect("seed source rows after the definition exists");

    insert_cdc_row(&client, "seg_0", "items", "1", "insert", None, Some("{}")).await;

    let seg_seq = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert_eq!(outcome.keys_written, 1);

    let out: String = client
        .query_one("select out from literals where id = 1", &[])
        .await
        .expect("read target row")
        .get(0);
    assert_eq!(out, "hi");
}

/// A boolean-column passthrough must round-trip through `compute()`/apply as
/// a real `boolean` column, not collapse to NULL.
#[tokio::test]
async fn a_boolean_column_passthrough_round_trips_through_compute() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // `items` starts empty so the definition's own initial backfill
    // enumerates nothing; its rows arrive afterward as this batch's staged
    // CDC events.
    client
        .batch_execute("create table items (id integer primary key, flag boolean)")
        .await
        .expect("seed source table");

    let def = TransformDef {
        target: "flags".to_string(),
        source: "items".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "out".to_string(),
            expr: Expr::Column("flag".to_string()),
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };
    let source_columns = columns(&[("id", ValueType::Numeric), ("flag", ValueType::Boolean)]);
    create_definition(
        &db.pool,
        "TRANSFORM flags FROM items SELECT flag AS out",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("create target table");

    client
        .execute(
            "insert into items (id, flag) values (1, true), (2, false)",
            &[],
        )
        .await
        .expect("seed source rows after the definition exists");

    insert_cdc_row(
        &client,
        "seg_0",
        "items",
        "1",
        "insert",
        None,
        Some(r#"{"flag":"t"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "items",
        "2",
        "insert",
        None,
        Some(r#"{"flag":"f"}"#),
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert_eq!(outcome.keys_written, 2);

    let rows = client
        .query("select id, out from flags order by id", &[])
        .await
        .expect("read target table");
    let out1: bool = rows[0].get(1);
    let out2: bool = rows[1].get(1);
    assert!(out1);
    assert!(!out2);
}

/// Issue #108's "typed CDC round-trip": a column whose Postgres type has no
/// first-class [`ValueType`] variant of its own — here `jsonb`, classified
/// as [`ValueType::Other`] via the PG-OID registry — still round-trips
/// byte-exact through the real `compute()` write path: `parse_value` tags
/// the CDC-decoded text with its [`trellis::defs::PgType`] family
/// ([`Value::Other`]), and the write plan's `field_pg_types` (`ddl::pg_type_name`)
/// casts it back to a genuine `jsonb` target column — not the `text` column
/// a pre-#108 build would have silently created and wired here.
#[tokio::test]
async fn a_jsonb_column_passthrough_round_trips_through_compute() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // `items` starts empty so the definition's own initial backfill
    // enumerates nothing; its rows arrive afterward as this batch's staged
    // CDC events.
    client
        .batch_execute("create table items (id integer primary key, payload jsonb)")
        .await
        .expect("seed source table");

    let def = TransformDef {
        target: "payloads".to_string(),
        source: "items".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "out".to_string(),
            expr: Expr::Column("payload".to_string()),
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };
    let source_columns = columns(&[
        ("id", ValueType::Numeric),
        ("payload", ValueType::Other(PgType::Jsonb)),
    ]);
    create_definition(
        &db.pool,
        "TRANSFORM payloads FROM items SELECT payload AS out",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("create target table");

    client
        .execute(
            "insert into items (id, payload) values \
             (1, '{\"a\": 1, \"b\": [1, 2, 3]}'), (2, '\"just a string\"')",
            &[],
        )
        .await
        .expect("seed source rows after the definition exists");

    // The exact CDC-decoded text a real logical-decoding stream would send
    // for each row is jsonb's own canonical text rendering — captured live
    // rather than hand-guessed, so this test doesn't depend on Postgres's
    // exact jsonb whitespace-normalization rules.
    let seeded = client
        .query("select id, payload::text from items order by id", &[])
        .await
        .expect("read back seeded jsonb text");
    let payload1: String = seeded[0].get(1);
    let payload2: String = seeded[1].get(1);

    let escape = |s: &str| s.replace('\\', "\\\\").replace('"', "\\\"");
    insert_cdc_row(
        &client,
        "seg_0",
        "items",
        "1",
        "insert",
        None,
        Some(&format!(r#"{{"payload":"{}"}}"#, escape(&payload1))),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "items",
        "2",
        "insert",
        None,
        Some(&format!(r#"{{"payload":"{}"}}"#, escape(&payload2))),
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert_eq!(outcome.keys_written, 2);

    let target_columns = client
        .query(
            "select column_name, data_type from information_schema.columns \
             where table_name = 'payloads' and column_name = 'out'",
            &[],
        )
        .await
        .expect("introspect target column type");
    assert_eq!(target_columns[0].get::<_, String>(1), "jsonb");

    let rows = client
        .query("select id, out::text from payloads order by id", &[])
        .await
        .expect("read target table");
    let out1: String = rows[0].get(1);
    let out2: String = rows[1].get(1);
    assert_eq!(out1, payload1, "jsonb object value must round-trip exactly");
    assert_eq!(
        out2, payload2,
        "jsonb string scalar must round-trip exactly"
    );
}

/// `SELECT strpos(name, 'foo') > 0 AS has_foo` (issue #65's composed
/// function-call-plus-comparison example) must round-trip through
/// `compute()`/apply into a real `boolean` target column, for both the
/// keyword-present and keyword-absent cases.
#[tokio::test]
async fn a_function_call_composed_with_greater_than_round_trips_through_compute() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // `items` starts empty so the definition's own initial backfill
    // enumerates nothing; its rows arrive afterward as this batch's staged
    // CDC events.
    client
        .batch_execute("create table items (id integer primary key, name text)")
        .await
        .expect("seed source table");

    let def = TransformDef {
        target: "keyword_flags".to_string(),
        source: "items".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "has_foo".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::GreaterThan,
                lhs: Box::new(Expr::FunctionCall {
                    name: "STRPOS".to_string(),
                    args: vec![
                        Expr::Column("name".to_string()),
                        Expr::StringLiteral("foo".to_string()),
                    ],
                }),
                rhs: Box::new(Expr::NumberLiteral("0".to_string())),
            },
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };
    let source_columns = columns(&[("id", ValueType::Numeric), ("name", ValueType::Text)]);
    create_definition(
        &db.pool,
        "TRANSFORM keyword_flags FROM items SELECT strpos(name, 'foo') > 0 AS has_foo",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("create target table");

    client
        .execute(
            "insert into items (id, name) values (1, 'has foo in it'), (2, 'no match here')",
            &[],
        )
        .await
        .expect("seed source rows after the definition exists");

    insert_cdc_row(
        &client,
        "seg_0",
        "items",
        "1",
        "insert",
        None,
        Some(r#"{"name":"has foo in it"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "items",
        "2",
        "insert",
        None,
        Some(r#"{"name":"no match here"}"#),
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert_eq!(outcome.keys_written, 2);

    let rows = client
        .query("select id, has_foo from keyword_flags order by id", &[])
        .await
        .expect("read target table");
    let present: bool = rows[0].get(1);
    let absent: bool = rows[1].get(1);
    assert!(present, "row with 'foo' in name should have has_foo = true");
    assert!(
        !absent,
        "row without 'foo' in name should have has_foo = false"
    );
}

/// Issue #13: a backfill's initial enumeration stages every pre-existing
/// source row as a bare `Recompute` trigger with no image, which
/// `compute()` must re-read live from the source table. Before the fix,
/// that re-read was one `select ... where id = $1` round trip per key; this
/// asserts it is now exactly one batched `where id = any($1)` round trip
/// for the whole bucket, regardless of key count, by turning on Postgres
/// statement logging and counting matches in the server log — and that the
/// result is still correct, matching the oracle.
#[tokio::test]
async fn a_backfill_style_batch_of_bare_recompute_triggers_refetches_in_one_batched_query() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    const N: usize = 25;

    let mut seed_sql = "create table orders (id integer primary key, price numeric, tax numeric); \
         insert into orders (id, price, tax) values "
        .to_string();
    let rows: Vec<String> = (1..=N).map(|i| format!("({i}, {i}.00, 1.00)")).collect();
    seed_sql.push_str(&rows.join(", "));
    client
        .batch_execute(&seed_sql)
        .await
        .expect("seed source table");

    let def = order_totals_def();
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("create target table");

    // Every row staged as a bare recompute trigger — no old/new image —
    // exactly the shape a backfill's enumeration produces.
    for i in 1..=N {
        client
            .execute(
                "insert into seg_0 (src_table, key, op, hop_gen) \
                 values ('trellis.orders', $1, 'recompute', 0)",
                &[&i.to_string()],
            )
            .await
            .unwrap_or_else(|e| panic!("insert recompute row {i} failed: {e}"));
    }

    // Turn on statement logging only for the drain below, so the log
    // reflects just the queries this batch's live refetch issues.
    client
        .execute("alter system set log_statement = 'all'", &[])
        .await
        .expect("enable statement logging");
    client
        .execute("select pg_reload_conf()", &[])
        .await
        .expect("reload config");

    let seg_seq = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert_eq!(outcome.keys_written, N);

    client
        .execute("alter system reset log_statement", &[])
        .await
        .expect("disable statement logging");
    client
        .execute("select pg_reload_conf()", &[])
        .await
        .expect("reload config");

    let log =
        std::fs::read_to_string(cluster.root().join("postgres.log")).expect("read postgres log");
    let refetch_queries: Vec<&str> = log
        .lines()
        // `join unnest(` singles out Phase 2's refetch: Phase 3's issue #344
        // check also reads `orders` by key, but from its own `unnest(...)`.
        .filter(|line| {
            line.contains("from \"trellis\".\"orders\" t")
                && line.contains("\"id\" =")
                && line.contains("join unnest(")
        })
        .collect();
    assert_eq!(
        refetch_queries.len(),
        1,
        "the live refetch for this bucket's {N} keys must be exactly one query, not one per key:\n{log}"
    );
    assert!(
        refetch_queries[0].contains("::text[]::"),
        "the one refetch query must batch every key via one bound array parameter \
         (`read_live_rows_batch`'s `join unnest($1::text[]::<type>[])`, issue #126 — no \
         longer literally `= any($1)` once the live refetch had to generalize to an \
         arbitrary-arity primary key), not a single-key `= $1`:\n{}",
        refetch_queries[0]
    );

    let oracle = recompute(&db.pool, &def, &pk[0].name, &source_columns)
        .await
        .expect("oracle recompute");
    let target_rows = client
        .query("select id::text, total::text from order_totals", &[])
        .await
        .expect("read target table");
    assert_eq!(target_rows.len(), N);
    for row in target_rows {
        let id: String = row.get(0);
        let total: Option<String> = row.get(1);
        let expected = &oracle[&id];
        assert_eq!(
            total,
            expected["total"].as_ref().map(|n| n.to_string()),
            "total mismatch for id {id}"
        );
    }
}

/// Regression test for the bind-parameter cap (`apply_target`'s
/// `MAX_WRITE_PARAMS_PER_STATEMENT`): a single target's write batch large
/// enough that one unchunked `INSERT ... VALUES` would need more than
/// Postgres's 65535-bind-parameter-per-statement limit. `order_totals` has
/// 2 columns per row (`id`, `total`), so 33,000 rows needs 66,000 params —
/// over the cap, and enough to force the chunking loop to split across two
/// chunks (30,000 + 3,000) rather than just brush the boundary. Before the
/// fix, this batch would fail every attempt with a `Kind::Parse` "invalid
/// message length: parameters is not drained" error and retry forever
/// (issue: apply_target never completing on a >~16k-row backfill).
#[tokio::test]
async fn a_write_batch_past_the_bind_parameter_cap_chunks_and_still_drains() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    const N: i64 = 33_000;

    client
        .batch_execute(
            "create table orders (id integer primary key, price numeric, tax numeric); \
             insert into orders (id, price, tax) \
             select i, i::numeric, 1.00 from generate_series(1, 33000) as i",
        )
        .await
        .expect("seed source table");

    let def = order_totals_def();
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("create target table");

    // Every row staged as a bare recompute trigger, same shape a backfill's
    // enumeration produces — one `INSERT ... SELECT` rather than N round
    // trips, since staging this many rows isn't itself what's under test.
    client
        .execute(
            "insert into seg_0 (src_table, key, op, hop_gen) \
             select 'trellis.orders', i::text, 'recompute', 0 from generate_series(1, $1::bigint) as i",
            &[&N],
        )
        .await
        .expect("stage recompute rows");

    let seg_seq = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert_eq!(outcome.keys_written, N as usize);

    let target_count: i64 = client
        .query_one("select count(*) from order_totals", &[])
        .await
        .expect("count target table")
        .get(0);
    assert_eq!(target_count, N);

    let oracle = recompute(&db.pool, &def, &pk[0].name, &source_columns)
        .await
        .expect("oracle recompute");
    let target_rows = client
        .query("select id::text, total::text from order_totals", &[])
        .await
        .expect("read target table");
    assert_eq!(target_rows.len() as i64, N);
    for row in target_rows {
        let id: String = row.get(0);
        let total: Option<String> = row.get(1);
        let expected = &oracle[&id];
        assert_eq!(
            total,
            expected["total"].as_ref().map(|n| n.to_string()),
            "total mismatch for id {id}"
        );
    }
}

/// A single bucket mixing all three shapes `compute()` dispatches on (see
/// its own doc comment): a staged `new_image` (decoded inline, no refetch),
/// a genuine CDC delete (`old_image` only, no refetch either), and bare
/// recompute triggers (the only shape needing [`read_live_rows_batch`]).
/// Guards against a fix that only handles a homogeneous, all-recompute
/// bucket: the batched refetch must cover exactly the recompute keys, the
/// other two shapes must still resolve correctly without ever touching it,
/// and all three shapes' outcomes must be correct in the same drain.
#[tokio::test]
async fn a_mixed_bucket_of_all_three_change_shapes_drains_correctly_in_one_batch() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // The live table's *final* state (the oracle recomputes straight from
    // it, matching this file's existing convention): 1, 2, and 5 survive —
    // 1 and 5 via their bare recompute triggers, 2 via its staged
    // new_image, which must match the row here — while 3 and 4 are already
    // gone, one via a bare recompute trigger resolving to a delete, the
    // other via a genuine staged CDC delete.
    //
    // `orders` starts empty so the definition's own initial backfill
    // enumerates nothing; rows 1, 2, and 5 arrive afterward, purely as this
    // batch's staged changes below, so they don't collide with a
    // backfill-staged recompute for the same keys in the same segment.
    client
        .batch_execute("create table orders (id integer primary key, price numeric, tax numeric)")
        .await
        .expect("seed source table");

    let def = order_totals_def();
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("create target table");

    client
        .execute(
            "insert into orders (id, price, tax) values \
             (1, 10.00, 1.50), (2, 20.00, 2.00), (5, 50.00, 5.00)",
            &[],
        )
        .await
        .expect("seed source rows after the definition exists");

    // Pre-populate target rows for 3 and 4, standing in for data an earlier
    // drain wrote before this batch's deletes arrive.
    client
        .execute(
            "insert into order_totals (id, total) values (3, 999), (4, 999)",
            &[],
        )
        .await
        .expect("pre-populate target rows this batch deletes");

    // Bare recompute triggers (need the batched refetch): 1 (row present,
    // write) and 3 (row absent, delete).
    for key in ["1", "3"] {
        client
            .execute(
                "insert into seg_0 (src_table, key, op, hop_gen) \
                 values ('trellis.orders', $1, 'recompute', 0)",
                &[&key],
            )
            .await
            .unwrap_or_else(|e| panic!("insert recompute row {key} failed: {e}"));
    }
    // A staged new_image (decoded inline, no refetch): 2, written from its
    // own image regardless of what (if anything) is live in `orders`.
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "2",
        "insert",
        None,
        Some(r#"{"price":"20.00","tax":"2.00"}"#),
    )
    .await;
    // A genuine CDC delete (old_image only, no refetch): 4.
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "4",
        "delete",
        Some(r#"{"price":"40.00","tax":"4.00"}"#),
        None,
    )
    .await;
    // A second bare recompute trigger (row present, write): 5 — so the
    // batched refetch covers more than one key, not just the one that
    // happens to resolve to a delete.
    client
        .execute(
            "insert into seg_0 (src_table, key, op, hop_gen) \
             values ('trellis.orders', '5', 'recompute', 0)",
            &[],
        )
        .await
        .expect("insert recompute row 5");

    client
        .execute("alter system set log_statement = 'all'", &[])
        .await
        .expect("enable statement logging");
    client
        .execute("select pg_reload_conf()", &[])
        .await
        .expect("reload config");

    let seg_seq = seal_active_segment(&mut client).await;
    let outcome = drain(&db.pool, seg_seq, "worker").await;
    assert_eq!(outcome.keys_written, 3, "1, 2, and 5 must be written");
    assert_eq!(outcome.keys_deleted, 2, "3 and 4 must be deleted");

    client
        .execute("alter system reset log_statement", &[])
        .await
        .expect("disable statement logging");
    client
        .execute("select pg_reload_conf()", &[])
        .await
        .expect("reload config");

    let log =
        std::fs::read_to_string(cluster.root().join("postgres.log")).expect("read postgres log");
    let refetch_queries: Vec<&str> = log
        .lines()
        // `join unnest(` singles out Phase 2's refetch: Phase 3's issue #344
        // check also reads `orders` by key, but from its own `unnest(...)`.
        .filter(|line| {
            line.contains("from \"trellis\".\"orders\" t")
                && line.contains("\"id\" =")
                && line.contains("join unnest(")
        })
        .collect();
    assert_eq!(
        refetch_queries.len(),
        1,
        "the bare-recompute subset (1, 3, 5) must still cost exactly one batched refetch:\n{log}"
    );
    assert!(
        refetch_queries[0].contains("::text[]::"),
        "the one refetch query must batch its keys via one bound array parameter \
         (`read_live_rows_batch`'s `join unnest($1::text[]::<type>[])`, issue #126):\n{}",
        refetch_queries[0]
    );

    let oracle = recompute(&db.pool, &def, &pk[0].name, &source_columns)
        .await
        .expect("oracle recompute");
    let target_rows = client
        .query("select id::text, total::text from order_totals", &[])
        .await
        .expect("read target table");
    assert_eq!(target_rows.len(), 3, "only 1, 2, and 5 must remain");
    for row in target_rows {
        let id: String = row.get(0);
        let total: Option<String> = row.get(1);
        let expected = &oracle[&id];
        assert_eq!(
            total,
            expected["total"].as_ref().map(|n| n.to_string()),
            "total mismatch for id {id}"
        );
    }
}

#[tokio::test]
async fn drain_many_coalesces_two_sealed_segments_into_one_apply_pass() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute("create table orders (id integer primary key, price numeric, tax numeric)")
        .await
        .expect("seed source table");

    let def = order_totals_def();
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("create target table");

    client
        .execute(
            "insert into orders (id, price, tax) values \
             (1, 10.00, 1.50), (2, 30.00, 3.00), (3, 1.00, 0.10)",
            &[],
        )
        .await
        .expect("seed source rows after the definition exists");

    // Segment 1: order 1 arrives, and order 2's first (interim) update.
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "2",
        "update",
        Some(r#"{"price":"5.00","tax":"0.50"}"#),
        Some(r#"{"price":"20.00","tax":"2.00"}"#),
    )
    .await;
    let seg1 = seal_active_segment(&mut client).await;

    // Segment 2: order 2's second update (same key as segment 1, so the
    // merge must collapse both into one write reflecting only the final
    // image) plus a brand-new order 3.
    insert_cdc_row(
        &client,
        "seg_1",
        "orders",
        "2",
        "update",
        Some(r#"{"price":"20.00","tax":"2.00"}"#),
        Some(r#"{"price":"30.00","tax":"3.00"}"#),
    )
    .await;
    insert_cdc_row(
        &client,
        "seg_1",
        "orders",
        "3",
        "insert",
        None,
        Some(r#"{"price":"1.00","tax":"0.10"}"#),
    )
    .await;
    let seg2 = seal_active_segment(&mut client).await;

    let outcome = apply::drain_many(
        &db.pool,
        &[seg1, seg2],
        "worker",
        1,
        "trellis_apply_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("drain_many")
    .expect("drain_many must claim and drain something");

    assert_eq!(
        outcome.keys_written, 3,
        "orders 1, 2, and 3 must each be written exactly once, \
         even though order 2 was touched by both segments"
    );
    assert_eq!(outcome.keys_deleted, 0);
    assert_eq!(
        outcome.segments_drained.len(),
        2,
        "both coalesced segments must report a drain outcome"
    );
    for &(seg_seq, fully_drained) in &outcome.segments_drained {
        assert!(
            fully_drained,
            "segment {seg_seq} must be fully drained by the single coalesced apply pass"
        );
        assert_eq!(segment_state(&client, seg_seq).await, SegmentState::Drained);
    }

    let oracle = recompute(&db.pool, &def, &pk[0].name, &source_columns)
        .await
        .expect("oracle recompute");
    let target_rows = client
        .query("select id::text, total::text from order_totals", &[])
        .await
        .expect("read target table");
    assert_eq!(target_rows.len(), oracle.len());
    for row in target_rows {
        let id: String = row.get(0);
        let total: Option<String> = row.get(1);
        let expected = &oracle[&id];
        assert_eq!(
            total,
            expected["total"].as_ref().map(|n| n.to_string()),
            "total mismatch for id {id}"
        );
    }
}

#[tokio::test]
async fn next_claimable_segments_stops_at_the_first_undrained_truncate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    // Segment 1: ordinary, no truncate.
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "a",
        "insert",
        None,
        Some(r#"{"v":"a"}"#),
    )
    .await;
    let seg1 = seal_active_segment(&mut client).await;

    // Segment 2: bears a truncate — a two-directional barrier that must
    // never be coalesced with anything else.
    insert_truncate_row(&client, "seg_1", "orders").await;
    let seg2 = seal_active_segment(&mut client).await;

    // Segment 3: ordinary again, sealed after the truncate.
    insert_cdc_row(
        &client,
        "seg_2",
        "orders",
        "b",
        "insert",
        None,
        Some(r#"{"v":"b"}"#),
    )
    .await;
    let seg3 = seal_active_segment(&mut client).await;

    // All three sealed and undrained: a wide batch request must stop at
    // seg1, excluding the truncate-bearing seg2 and everything past it —
    // never coalescing an ordinary segment with a truncate.
    let batch = apply::next_claimable_segments(&client, 10)
        .await
        .expect("query barrier");
    assert_eq!(batch, vec![seg1]);

    client
        .execute(
            "update segments \
             set state = 'drained', drained_mask = (1::bigint << bucket_count) - 1 \
             where seg_seq = $1",
            &[&seg1],
        )
        .await
        .expect("mark seg1 drained");

    // seg2 (the truncate) is now the lowest undrained segment: it must be
    // handed out alone, never bundled with seg3.
    let batch = apply::next_claimable_segments(&client, 10)
        .await
        .expect("query barrier");
    assert_eq!(
        batch,
        vec![seg2],
        "the truncate segment must be handed out alone, not coalesced with seg3"
    );

    client
        .execute(
            "update segments \
             set state = 'drained', drained_mask = (1::bigint << bucket_count) - 1 \
             where seg_seq = $1",
            &[&seg2],
        )
        .await
        .expect("mark seg2 drained");

    // Only now, with the truncate itself drained, does seg3 become
    // claimable.
    let batch = apply::next_claimable_segments(&client, 10)
        .await
        .expect("query barrier");
    assert_eq!(batch, vec![seg3]);
}

// ---------------------------------------------------------------------------
// Issue #344: two batches for the same key, applied out of order.
//
// Segments drain in any order and nothing orders one batch's Phase 3 against
// another's for the same key, so a slow worker can commit a value computed
// from an older source state *after* a newer batch has already drained. The
// tests below run that interleaving by hand: batch 1 is claimed, folded and
// computed, then the source row changes and batch 2 drains completely, and
// only then does batch 1's Phase 3 run. The target must end on the value the
// source's current state implies, whatever batch 1 computed.
// ---------------------------------------------------------------------------

/// A source `orders` table, an `order_totals` (`price + tax`) definition over
/// it and the target table, with `seed` inserted into `orders` *before* the
/// definition exists when given (so the definition's own backfill enumeration
/// also stages it).
async fn out_of_order_fixture(seed: Option<&str>) -> (TestCluster, testkit::TestDatabase, Client) {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute("create table orders (id integer primary key, price numeric, tax numeric)")
        .await
        .expect("create source table");
    if let Some(seed) = seed {
        client
            .batch_execute(&format!(
                "insert into orders (id, price, tax) values {seed}"
            ))
            .await
            .expect("seed source table");
    }

    let def = order_totals_def();
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM order_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("create target table");

    (cluster, db, client)
}

/// Phases 1 and 2 of a drain of `seg_seq` as `worker`: claims every bucket,
/// folds, commits the claim, and computes the plan, stopping short of
/// Phase 3 so a test can run other batches before this one applies.
async fn claim_and_compute(pool: &trellis::Pool, seg_seq: i64, worker: &str) -> apply::ApplyPlan {
    let mut phase1_client = pool.get().await.expect("connection");
    let txn = phase1_client.transaction().await.expect("begin phase 1");
    claim::claim(&*txn, seg_seq, worker, 1)
        .await
        .expect("claim");
    let filter = claim::owned_bucket_filter(&*txn, seg_seq, worker)
        .await
        .expect("owned_bucket_filter");
    let folded = fold::fold(&txn, seg_seq, filter).await.expect("fold");
    txn.commit().await.expect("commit phase 1");
    apply::compute(pool, &folded).await.expect("compute")
}

/// Phase 3 of a drain [`claim_and_compute`] started.
async fn apply_computed(
    pool: &trellis::Pool,
    seg_seq: i64,
    worker: &str,
    plan: &apply::ApplyPlan,
) -> apply::ApplyOutcome {
    let mut phase3_client = pool.get().await.expect("connection");
    let txn = phase3_client.transaction().await.expect("begin phase 3");
    let outcome = apply::apply_and_mark_drained(
        &txn,
        seg_seq,
        worker,
        plan,
        "trellis_apply_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("apply");
    txn.commit().await.expect("commit phase 3");
    outcome
}

async fn order_total(client: &Client, id: i32) -> Option<String> {
    client
        .query_opt(
            "select total::text from public.order_totals where id = $1",
            &[&id],
        )
        .await
        .expect("read target")
        .map(|row| row.get(0))
}

/// Drains every sealed segment still claimable, so a test's final assertion
/// sees a settled target.
async fn drain_everything_left(pool: &trellis::Pool, client: &Client) {
    while let Some(&seg) = apply::next_claimable_segments(client, 1)
        .await
        .expect("next claimable")
        .first()
    {
        drain(pool, seg, "sweeper").await;
    }
}

/// The repro from issue #344: batch 1 is a bare recompute trigger (the shape
/// a catch-up enumeration stages), so its Phase 2 reads the live source row.
#[tokio::test]
async fn a_recompute_read_before_a_newer_batch_drains_does_not_overwrite_it() {
    let (_cluster, db, mut client) = out_of_order_fixture(Some("(1, 10.00, 1.00)")).await;

    // The definition's own enumeration already staged a recompute for id 1;
    // batch 1 is that segment. Its Phase 2 reads price = 10.00.
    let seg1 = seal_active_segment(&mut client).await;
    let stale_plan = claim_and_compute(&db.pool, seg1, "slow_worker").await;

    client
        .execute("update orders set price = 20.00 where id = 1", &[])
        .await
        .expect("update source row");
    insert_cdc_row(
        &client,
        "seg_1",
        "orders",
        "1",
        "update",
        Some(r#"{"price":"10.00","tax":"1.00"}"#),
        Some(r#"{"price":"20.00","tax":"1.00"}"#),
    )
    .await;
    let seg2 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg2, "fast_worker").await;
    assert_eq!(order_total(&client, 1).await.as_deref(), Some("21.00"));

    apply_computed(&db.pool, seg1, "slow_worker", &stale_plan).await;
    drain_everything_left(&db.pool, &client).await;
    assert_eq!(
        order_total(&client, 1).await.as_deref(),
        Some("21.00"),
        "a recompute read before a newer batch drained must not overwrite that batch's value"
    );
}

/// The same race with two image-bearing CDC updates: batch 1's image is
/// older than batch 2's, and batch 2 drains first.
#[tokio::test]
async fn an_older_cdc_image_applied_after_a_newer_one_does_not_overwrite_it() {
    let (_cluster, db, mut client) = out_of_order_fixture(None).await;

    client
        .execute(
            "insert into orders (id, price, tax) values (1, 15.00, 1.00)",
            &[],
        )
        .await
        .expect("insert source row");
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"id":"1","price":"15.00","tax":"1.00"}"#),
    )
    .await;
    let seg1 = seal_active_segment(&mut client).await;
    let stale_plan = claim_and_compute(&db.pool, seg1, "slow_worker").await;

    client
        .execute("update orders set price = 20.00 where id = 1", &[])
        .await
        .expect("update source row");
    insert_cdc_row(
        &client,
        "seg_1",
        "orders",
        "1",
        "update",
        Some(r#"{"id":"1","price":"15.00","tax":"1.00"}"#),
        Some(r#"{"id":"1","price":"20.00","tax":"1.00"}"#),
    )
    .await;
    let seg2 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg2, "fast_worker").await;
    assert_eq!(order_total(&client, 1).await.as_deref(), Some("21.00"));

    apply_computed(&db.pool, seg1, "slow_worker", &stale_plan).await;
    drain_everything_left(&db.pool, &client).await;
    assert_eq!(
        order_total(&client, 1).await.as_deref(),
        Some("21.00"),
        "an older image applied late must not overwrite a newer one"
    );
}

/// A stale write must not resurrect a row a newer batch deleted.
#[tokio::test]
async fn a_stale_write_applied_after_a_newer_delete_does_not_resurrect_the_row() {
    let (_cluster, db, mut client) = out_of_order_fixture(None).await;

    client
        .execute(
            "insert into orders (id, price, tax) values (1, 10.00, 1.00)",
            &[],
        )
        .await
        .expect("insert source row");
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"id":"1","price":"10.00","tax":"1.00"}"#),
    )
    .await;
    let seg1 = seal_active_segment(&mut client).await;
    let stale_plan = claim_and_compute(&db.pool, seg1, "slow_worker").await;

    client
        .execute("delete from orders where id = 1", &[])
        .await
        .expect("delete source row");
    insert_cdc_row(
        &client,
        "seg_1",
        "orders",
        "1",
        "delete",
        Some(r#"{"id":"1","price":"10.00","tax":"1.00"}"#),
        None,
    )
    .await;
    let seg2 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg2, "fast_worker").await;
    assert_eq!(order_total(&client, 1).await, None);

    apply_computed(&db.pool, seg1, "slow_worker", &stale_plan).await;
    drain_everything_left(&db.pool, &client).await;
    assert_eq!(
        order_total(&client, 1).await,
        None,
        "a stale write must not resurrect a row a newer batch deleted"
    );
}

/// The mirror image: a stale delete must not remove a row the source got
/// back and a newer batch already wrote.
#[tokio::test]
async fn a_stale_delete_applied_after_a_newer_insert_does_not_remove_the_row() {
    let (_cluster, db, mut client) = out_of_order_fixture(Some("(1, 10.00, 1.00)")).await;
    // Settle the definition's own enumeration first.
    seal_active_segment(&mut client).await;
    drain_everything_left(&db.pool, &client).await;
    assert_eq!(order_total(&client, 1).await.as_deref(), Some("11.00"));

    client
        .execute("delete from orders where id = 1", &[])
        .await
        .expect("delete source row");
    insert_cdc_row(
        &client,
        "seg_1",
        "orders",
        "1",
        "delete",
        Some(r#"{"id":"1","price":"10.00","tax":"1.00"}"#),
        None,
    )
    .await;
    let seg1 = seal_active_segment(&mut client).await;
    let stale_plan = claim_and_compute(&db.pool, seg1, "slow_worker").await;

    client
        .execute(
            "insert into orders (id, price, tax) values (1, 30.00, 1.00)",
            &[],
        )
        .await
        .expect("re-insert source row");
    insert_cdc_row(
        &client,
        "seg_2",
        "orders",
        "1",
        "insert",
        None,
        Some(r#"{"id":"1","price":"30.00","tax":"1.00"}"#),
    )
    .await;
    let seg2 = seal_active_segment(&mut client).await;
    drain(&db.pool, seg2, "fast_worker").await;
    assert_eq!(order_total(&client, 1).await.as_deref(), Some("31.00"));

    apply_computed(&db.pool, seg1, "slow_worker", &stale_plan).await;
    drain_everything_left(&db.pool, &client).await;
    assert_eq!(
        order_total(&client, 1).await.as_deref(),
        Some("31.00"),
        "a stale delete must not remove a row a newer batch wrote"
    );
}
