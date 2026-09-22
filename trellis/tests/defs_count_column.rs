//! End-to-end tests for issue #120's `COUNT(<column>)` in a `GROUP BY`
//! definition — counting non-null occurrences of a specific column, a
//! different Postgres semantic from `COUNT(*)`'s unconditional row count.
//!
//! # The real gap, found by reading the code first
//!
//! `defs::invertibility` already modelled `CountArg::Column` (issue #75's
//! own groundwork) as invertible with no hidden partials, identically to
//! `CountArg::Star` — the delta model needed no new machinery. The real gap
//! was three narrower things:
//!
//! 1. **Grammar**: `super::parser`'s aggregate-`COUNT` branch accepted only
//!    the literal `*`, rejecting any real argument with
//!    `ParseError::UnsupportedAggregateFunction` (now removed — that
//!    variant's only purpose was rejecting exactly this shape).
//! 2. **A real validator gap, not just a missing feature**: `AGGREGATE_FUNCTION_SPECS`'s
//!    `COUNT` row declares `arg_types: &[]` (arity handled by the parser,
//!    not this table), so `super::validate::infer_expr`'s generic
//!    `args.iter().zip(spec.arg_types)` loop would `zip` a real one-element
//!    `args` against an empty `arg_types` and produce *zero* iterations —
//!    meaning `COUNT(<expr>)`'s argument would never be recursively
//!    type-checked at all once the grammar accepted it. Fixed with a
//!    `COUNT`-specific early return in `infer_expr` that validates the
//!    argument unconditionally (Postgres's own `count(x)` accepts any type).
//! 3. **Two silently-wrong SQL renderers**: `staging::apply_aggregate`'s
//!    forced-full-recompute paths (`upsert_group`'s single-group probe and
//!    `apply_forced_groups_bulk`'s bulk `INSERT ... SELECT`) both hardcoded
//!    `count(*)` for every [`AggFieldKind::Count`] field — correct for
//!    `COUNT(*)`, silently wrong for `COUNT(<column>)` (it would have
//!    written the group's *row count*, not the column's non-null count, any
//!    time an image-less recompute trigger forced that field's group onto
//!    the full-recompute path). `count_column_forced_full_recompute_...`
//!    below exercises exactly that path.
//!
//! Bundled in, since `docs/type-support.md` (#111's own "exact integer
//! semantics" note) explicitly earmarks it for this issue: `COUNT` is now
//! declared `bigint` (`Integer(Int8)`), matching Postgres's own
//! `pg_typeof(count(*))`/`pg_typeof(count(x))`, rather than the `numeric`
//! every aggregate used to collapse into before issue #111 had anywhere to
//! put an integer.
//!
//! Harness conventions follow `defs_enum.rs`/`defs_min_max_text_and_uuid.rs`.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::{create_aggregate_target_table, create_definition, parse, validate};
use trellis::integer::IntWidth;
use trellis::staging::{StagedWatermark, apply, retire_drained_segments, seal};

// ---------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public"))
        .await
        .expect("session bootstrap");
    client
}

/// The physical ring table this test should stage into *right now* —
/// `seal::seal_phase1` rotates which ring slot is active every time it's
/// called (`RING_SIZE = 4`), so a test that seals/drains more than once (as
/// several below do, to exercise the delta path across several rounds)
/// cannot hardcode `seg_0` past its first round: the fold's own "phase gap"
/// mechanism (`seal::fenced_window`'s predecessor-half `UNION ALL`, `docs/
/// staging-and-claiming/04-claiming-and-the-fold.md`) only ever looks back
/// *one* generation, so a stale-slot write survives exactly one round past
/// sealing and then is silently never read by anything — matches
/// `defs_aggregate_relationship.rs`'s own `active_seg_table` helper.
async fn active_seg_table(client: &Client) -> String {
    let ring_slot: i16 = client
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment pointer")
        .get(0);
    format!("seg_{ring_slot}")
}

/// A monotonically increasing LSN, shared by every staged row across every
/// test in this file — no two tests share a database, and every test only
/// needs its own rows internally ordered, not globally unique.
fn next_lsn() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

async fn stage_insert(client: &Client, key: &str, src_table: &str, new_image: &str) {
    let table = active_seg_table(client).await;
    let src_table = format!("{DEFAULT_SCHEMA}.{src_table}");
    client
        .execute(
            &format!(
                "insert into {table} \
                 (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                 values ($1, $2, 'insert', $3, null, $4::text::jsonb, 0)"
            ),
            &[&src_table, &key, &PgLsn::from(next_lsn()), &new_image],
        )
        .await
        .unwrap_or_else(|e| panic!("stage insert {key} into {table}: {e}"));
}

async fn stage_update(
    client: &Client,
    key: &str,
    src_table: &str,
    old_image: &str,
    new_image: &str,
) {
    let table = active_seg_table(client).await;
    let src_table = format!("{DEFAULT_SCHEMA}.{src_table}");
    client
        .execute(
            &format!(
                "insert into {table} \
                 (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                 values ($1, $2, 'update', $3, $4::text::jsonb, $5::text::jsonb, 0)"
            ),
            &[
                &src_table,
                &key,
                &PgLsn::from(next_lsn()),
                &old_image,
                &new_image,
            ],
        )
        .await
        .unwrap_or_else(|e| panic!("stage update {key} into {table}: {e}"));
}

async fn stage_delete(client: &Client, key: &str, src_table: &str, old_image: &str) {
    let table = active_seg_table(client).await;
    let src_table = format!("{DEFAULT_SCHEMA}.{src_table}");
    client
        .execute(
            &format!(
                "insert into {table} \
                 (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                 values ($1, $2, 'delete', $3, $4::text::jsonb, null, 0)"
            ),
            &[&src_table, &key, &PgLsn::from(next_lsn()), &old_image],
        )
        .await
        .unwrap_or_else(|e| panic!("stage delete {key} into {table}: {e}"));
}

/// A bare recompute trigger — no image at all — which
/// `staging::apply_aggregate::accumulate_changes` cannot fold as a delta
/// (no prior state to diff against), so it forces the touched group onto
/// [`GroupPlan::force_full_recompute`] — the path this issue's `probe_count`
/// fix targets.
async fn stage_bare_recompute(client: &Client, key: &str, src_table: &str) {
    let table = active_seg_table(client).await;
    let src_table = format!("{DEFAULT_SCHEMA}.{src_table}");
    client
        .execute(
            &format!(
                "insert into {table} \
                 (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                 values ($1, $2, 'recompute', null, null, null, 0)"
            ),
            &[&src_table, &key],
        )
        .await
        .unwrap_or_else(|e| panic!("stage bare recompute {key} into {table}: {e}"));
}

async fn drain_sealed(client: &mut Client, pool: &trellis::Pool, worker: &str) {
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq)
        .await
        .expect("seal phase 2");
    apply::drain_once(
        pool,
        outcome.sealed_seg_seq,
        worker,
        1,
        "trellis_defs_count_column",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("drain_once")
    .expect("drain_once must claim and drain something");
    // Frees this round's now-fully-drained ring slot so a test with more
    // rounds than `RING_SIZE` doesn't hit `StagingError::RingFull` once the
    // ring wraps back around.
    retire_drained_segments(client)
        .await
        .expect("retire drained segments");
}

async fn read_totals(client: &Client) -> Vec<(i32, i64, i64)> {
    let mut got: Vec<(i32, i64, i64)> = client
        .query(
            "select grp, total_rows, non_null_amounts from totals order by grp",
            &[],
        )
        .await
        .expect("read totals")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect();
    got.sort_by_key(|(grp, ..)| *grp);
    got
}

async fn read_expected(client: &Client) -> Vec<(i32, i64, i64)> {
    let mut expected: Vec<(i32, i64, i64)> = client
        .query(
            "select grp, count(*), count(amount) from events group by grp order by grp",
            &[],
        )
        .await
        .expect("server-side recompute")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect();
    expected.sort_by_key(|(grp, ..)| *grp);
    expected
}

const DEF_SQL: &str = "TRANSFORM totals FROM events GROUP BY grp \
     SELECT grp AS grp, COUNT(*) AS total_rows, COUNT(amount) AS non_null_amounts";

fn columns() -> HashMap<String, ValueType> {
    HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("grp".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("amount".to_string(), ValueType::Numeric),
    ])
}

async fn create_schema_and_definition(pool: &trellis::Pool, client: &Client) {
    client
        .batch_execute(
            "create table events (id integer primary key, grp integer, amount numeric); \
             alter table events replica identity full",
        )
        .await
        .expect("create source table");

    let def = parse(DEF_SQL).expect("parse");
    validate(&def, &columns(), &HashMap::new()).expect("validate");
    create_definition(pool, DEF_SQL, &columns())
        .await
        .expect("create definition");
    create_aggregate_target_table(pool, &def, "public", &columns())
        .await
        .expect("create aggregate target table");
}

// ---------------------------------------------------------------------
// 1. Grammar + type: parses, validates, declared bigint
// ---------------------------------------------------------------------

#[test]
fn count_of_a_column_parses_and_validates() {
    let def = parse(DEF_SQL).expect("COUNT(<column>) must parse in a GROUP BY definition");
    validate(&def, &columns(), &HashMap::new())
        .expect("COUNT(<column>) must validate as a GROUP BY field");
    // The declared type itself (must be `bigint`, matching Postgres's own
    // `pg_typeof(count(x))`) is pinned by the crate-internal unit test
    // `defs::validate::tests::count_of_a_column_in_a_group_by_is_accepted_and_typed_bigint`
    // (this external test binary has no access to `infer_field_types`,
    // `pub(crate)`) and, end-to-end, by `count_column_target_type_is_bigint_live`
    // below.
}

#[tokio::test]
async fn count_column_target_type_is_bigint_live() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    create_schema_and_definition(&db.pool, &client).await;

    let row = client
        .query_one(
            "select data_type from information_schema.columns \
             where table_name = 'totals' and column_name = 'non_null_amounts'",
            &[],
        )
        .await
        .expect("introspect totals.non_null_amounts");
    let data_type: String = row.get(0);
    assert_eq!(data_type, "bigint");
}

// ---------------------------------------------------------------------
// 2. Correctness: COUNT(<column>) excludes NULLs, COUNT(*) doesn't
// ---------------------------------------------------------------------

#[tokio::test]
async fn count_column_matches_a_server_side_recompute_excluding_nulls() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema_and_definition(&db.pool, &client).await;

    // grp 1: three rows, one NULL amount => total_rows=3, non_null_amounts=2.
    // grp 2: two rows, both NULL amount => total_rows=2, non_null_amounts=0.
    let seed: Vec<(i32, i32, Option<&str>)> = vec![
        (1, 1, Some("10")),
        (2, 1, Some("20")),
        (3, 1, None),
        (4, 2, None),
        (5, 2, None),
    ];
    for (id, grp, amount) in &seed {
        let amount_sql = amount
            .map(|a| a.to_string())
            .unwrap_or_else(|| "null".to_string());
        client
            .execute(
                &format!("insert into events (id, grp, amount) values ({id}, {grp}, {amount_sql})"),
                &[],
            )
            .await
            .expect("seed source row");
        let amount_json = amount
            .map(|a| format!("\"{a}\""))
            .unwrap_or_else(|| "null".to_string());
        stage_insert(
            &client,
            &id.to_string(),
            "events",
            &format!(r#"{{"grp":"{grp}","amount":{amount_json}}}"#),
        )
        .await;
    }
    drain_sealed(&mut client, &db.pool, "count_worker_v1").await;

    let got = read_totals(&client).await;
    let expected = read_expected(&client).await;
    assert_eq!(got, expected);
    assert_eq!(
        got,
        vec![(1, 3, 2), (2, 2, 0)],
        "COUNT(*) counts every row; COUNT(amount) excludes NULLs — a different \
         answer for the same group"
    );
}

// ---------------------------------------------------------------------
// 3. Delta path: NULL <-> non-NULL transitions and deletes
// ---------------------------------------------------------------------

#[tokio::test]
async fn count_column_delta_path_tracks_null_transitions_and_deletes() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema_and_definition(&db.pool, &client).await;

    client
        .batch_execute(
            "insert into events (id, grp, amount) values \
               (1, 1, 10), (2, 1, null), (3, 1, 30)",
        )
        .await
        .expect("seed source rows");
    stage_insert(&client, "1", "events", r#"{"grp":"1","amount":"10"}"#).await;
    stage_insert(&client, "2", "events", r#"{"grp":"1","amount":null}"#).await;
    stage_insert(&client, "3", "events", r#"{"grp":"1","amount":"30"}"#).await;
    drain_sealed(&mut client, &db.pool, "count_worker_v2").await;

    assert_eq!(read_totals(&client).await, vec![(1, 3, 2)]);

    // NULL -> non-NULL: row 2's amount becomes 20. non_null_amounts must
    // increment even though total_rows (COUNT(*)) is unaffected.
    client
        .execute("update events set amount = 20 where id = 2", &[])
        .await
        .expect("null -> non-null");
    stage_update(
        &client,
        "2",
        "events",
        r#"{"grp":"1","amount":null}"#,
        r#"{"grp":"1","amount":"20"}"#,
    )
    .await;
    drain_sealed(&mut client, &db.pool, "count_worker_v3").await;
    assert_eq!(
        read_totals(&client).await,
        vec![(1, 3, 3)],
        "a NULL -> non-NULL transition must increment COUNT(<column>) with no \
         change to COUNT(*)"
    );
    assert_eq!(read_totals(&client).await, read_expected(&client).await);

    // non-NULL -> NULL: row 1's amount becomes NULL.
    client
        .execute("update events set amount = null where id = 1", &[])
        .await
        .expect("non-null -> null");
    stage_update(
        &client,
        "1",
        "events",
        r#"{"grp":"1","amount":"10"}"#,
        r#"{"grp":"1","amount":null}"#,
    )
    .await;
    drain_sealed(&mut client, &db.pool, "count_worker_v4").await;
    assert_eq!(read_totals(&client).await, vec![(1, 3, 2)]);
    assert_eq!(read_totals(&client).await, read_expected(&client).await);

    // Delete a row holding a non-null value: the group survives (2 rows
    // remain — `COUNT(<column>)`'s own zero never implies group extinction,
    // unlike `COUNT(*)`), and both counts decrement by one.
    client
        .execute("delete from events where id = 3", &[])
        .await
        .expect("delete a non-null-amount row");
    stage_delete(&client, "3", "events", r#"{"grp":"1","amount":"30"}"#).await;
    drain_sealed(&mut client, &db.pool, "count_worker_v5").await;
    assert_eq!(
        read_totals(&client).await,
        vec![(1, 2, 1)],
        "deleting a non-null-amount row decrements both counts; the group \
         itself stays alive since other rows remain"
    );
    assert_eq!(read_totals(&client).await, read_expected(&client).await);
}

// ---------------------------------------------------------------------
// 4. Forced full recompute: an image-less recompute trigger
// ---------------------------------------------------------------------

/// Targets the bug this issue's review found: `staging::apply_aggregate`'s
/// forced-full-recompute paths used to hardcode `count(*)` for *every*
/// [`AggFieldKind::Count`] field — correct for `COUNT(*)`, silently wrong
/// for `COUNT(<column>)` (it would have written the row count instead of
/// the non-null count). A bare recompute trigger (no image) forces its
/// group onto exactly that path.
#[tokio::test]
async fn count_column_forced_full_recompute_still_excludes_nulls() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema_and_definition(&db.pool, &client).await;

    client
        .batch_execute(
            "insert into events (id, grp, amount) values \
               (1, 1, 10), (2, 1, null), (3, 1, null), (4, 1, 40)",
        )
        .await
        .expect("seed source rows");

    // A bare recompute trigger carries no image at all, so
    // `accumulate_changes` cannot fold it as a delta and forces the whole
    // group onto `GroupPlan::force_full_recompute` — exercising
    // `upsert_group`'s `probe_count` (single-group probe path).
    stage_bare_recompute(&client, "1", "events").await;
    drain_sealed(&mut client, &db.pool, "count_worker_forced_v1").await;

    let got = read_totals(&client).await;
    let expected = read_expected(&client).await;
    assert_eq!(got, expected);
    assert_eq!(
        got,
        vec![(1, 4, 2)],
        "a forced full recompute must still count only the non-NULL amounts, \
         not the group's raw row count"
    );
}

/// The bulk counterpart of the previous test — many groups forced onto
/// [`GroupPlan::force_full_recompute`] in the same batch, exercising
/// `apply_forced_groups_bulk`'s own `count(<expr>)` rendering rather than
/// `upsert_group`'s per-group probe.
#[tokio::test]
async fn count_column_bulk_forced_full_recompute_still_excludes_nulls() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    create_schema_and_definition(&db.pool, &client).await;

    client
        .batch_execute(
            "insert into events (id, grp, amount) values \
               (1, 1, 10), (2, 1, null), \
               (3, 2, null), (4, 2, null), (5, 2, 50), \
               (6, 3, 60)",
        )
        .await
        .expect("seed source rows");

    for id in ["1", "3", "6"] {
        stage_bare_recompute(&client, id, "events").await;
    }
    drain_sealed(&mut client, &db.pool, "count_worker_forced_bulk_v1").await;

    let got = read_totals(&client).await;
    let expected = read_expected(&client).await;
    assert_eq!(got, expected);
    assert_eq!(got, vec![(1, 2, 1), (2, 3, 1), (3, 1, 1)]);
}
