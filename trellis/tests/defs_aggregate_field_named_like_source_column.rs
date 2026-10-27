//! A field named after a source column (`SUM(val) AS val`) used to
//! be substituted into a later field's aggregate argument, so
//! `MIN(val) AS lo` read as `MIN(SUM(val))`. Define accepted it and every
//! build attempt failed. An aggregate's argument now reads the source column,
//! as in SQL, and a name that is no source column and names another aggregate
//! field is refused at define.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::validate::ValidationError;
use trellis::defs::{CatalogError, ValueType, install_definition};
use trellis::intake::harness;
use trellis::integer::IntWidth;
use trellis::staging::{has_pending, retire_drained_segments};

const TEST_NAME: &str = "defs_aggregate_field_named_like_source_column_test";

/// The definition from the issue.
const AGG_TOTALS: &str = "TRANSFORM agg_totals FROM public.agg_src GROUP BY grp \
   WITHOUT SOURCE INDEX SELECT grp AS grp, SUM(val) AS val, COUNT(*) AS row_count, COUNT(val) AS lo, SUM(val + 1) AS hi";

fn source_columns() -> HashMap<String, ValueType> {
    HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("grp".to_string(), ValueType::Text),
        ("val".to_string(), ValueType::Integer(IntWidth::Int4)),
    ])
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

async fn drain_backfill_chunks(pool: &trellis::Pool) {
    harness::start_registered_builds(pool)
        .await
        .expect("dispatch registered definitions' builds");
    trellis::staging::build::settle_builds(pool).await;
}

async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    use trellis::staging::{apply, seal};
    for _ in 0..16 {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
            .await
            .expect("seal phase 2");
        while apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            TEST_NAME,
            1,
            "trellis_defs_aggregate_field_named_like_source_column_test",
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

/// `grp -> (val, row_count, lo, hi)` as text.
async fn target_rows(client: &Client) -> Vec<(String, String, String, String, String)> {
    client
        .query(
            "select grp, val::text, row_count::text, lo::text, hi::text \
             from agg_totals order by grp",
            &[],
        )
        .await
        .expect("read agg_totals")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4)))
        .collect()
}

fn row(
    grp: &str,
    val: &str,
    n: &str,
    lo: &str,
    hi: &str,
) -> (String, String, String, String, String) {
    (
        grp.to_string(),
        val.to_string(),
        n.to_string(),
        lo.to_string(),
        hi.to_string(),
    )
}

/// The issue's example (its MIN and MAX, deferred past 0.1.0 by #1117, as a
/// COUNT and a SUM over the same column) builds the values SQL gives.
#[tokio::test]
async fn an_aggregate_field_named_like_a_source_column_reads_the_source_column() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table public.agg_src (id integer primary key, grp text, val integer); \
             insert into public.agg_src (id, grp, val) values \
               (1, 'a', 5), (2, 'a', 7), (3, 'a', 2), (4, 'b', 9)",
        )
        .await
        .expect("create + seed agg_src");

    install_definition(&db.pool, AGG_TOTALS, &source_columns(), "public")
        .await
        .expect("define accepts the issue's definition");
    drain_backfill_chunks(&db.pool).await;
    harness::settle_builds(&db.pool).await;
    drain_to_quiescence(&db.pool, &mut client).await;

    assert_eq!(
        target_rows(&client).await,
        vec![
            row("a", "14", "3", "3", "17"),
            row("b", "9", "1", "1", "10"),
        ],
        "val is SUM(val), lo and hi are COUNT(val) and SUM(val + 1) of the source column"
    );
}

/// A name inside an aggregate's argument that is no source column and names
/// another aggregate field would nest one aggregate in another; define refuses
/// it instead of accepting a definition that can never build.
#[tokio::test]
async fn an_aggregate_of_another_aggregate_field_is_refused_at_define() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table public.agg_src (id integer primary key, grp text, val integer)",
        )
        .await
        .expect("create agg_src");

    let err = install_definition(
        &db.pool,
        "TRANSFORM nested FROM public.agg_src GROUP BY grp \
        WITHOUT SOURCE INDEX SELECT grp AS grp, SUM(val) AS total, SUM(total) AS biggest",
        &source_columns(),
        "public",
    )
    .await
    .expect_err("SUM(total) would nest SUM inside SUM");
    let message = err.to_string();
    assert!(
        message.contains("biggest") && message.contains("total"),
        "the refusal names the field and what it aggregates: {message}"
    );
}

/// A plain aggregate outside the admitted set is refused at
/// define by family, with nothing left behind: no target, no hidden table, no
/// definition row. The float and unconstrained-`numeric` refusals read the
/// live column, so they come from define's transaction, not the text.
#[tokio::test]
async fn a_plain_aggregate_outside_the_admitted_set_is_refused_at_define_by_family() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table public.agg_src (id integer primary key, grp text, val integer, \
             f double precision, n numeric, ok boolean)",
        )
        .await
        .expect("create agg_src");
    let mut columns = source_columns();
    columns.insert(
        "f".to_string(),
        ValueType::Float(trellis::float::FloatWidth::Float8),
    );
    columns.insert("n".to_string(), ValueType::Numeric);
    columns.insert("ok".to_string(), ValueType::Boolean);

    for (field, want) in [
        ("MIN(val)", Some("MIN/MAX")),
        ("MAX(grp)", Some("MIN/MAX")),
        ("BOOL_AND(ok)", Some("bool_and/bool_or/bit_and/bit_or")),
        ("ARRAY_AGG(val)", Some("array_agg/string_agg/jsonb_agg")),
        ("SUM(f)", Some("float and interval SUM/AVG")),
        ("AVG(f)", Some("float and interval SUM/AVG")),
        ("SUM(n)", None),
        ("SUM(val) + MAX(val)", Some("MIN/MAX")),
    ] {
        let err = install_definition(
            &db.pool,
            &format!(
                "TRANSFORM refused FROM public.agg_src GROUP BY grp \
                 WITHOUT SOURCE INDEX SELECT grp AS grp, {field} AS x"
            ),
            &columns,
            "public",
        )
        .await
        .expect_err(field);
        match want {
            Some(family) => assert!(
                matches!(
                    &err,
                    CatalogError::Validate(ValidationError::DeferredAggregate { family: f, .. })
                        if *f == family
                ),
                "{field}: {err:?}"
            ),
            None => assert!(
                matches!(
                    &err,
                    CatalogError::Validate(ValidationError::UnconstrainedNumericAggregate { .. })
                ),
                "{field}: {err:?}"
            ),
        }
        let left: (bool, i64, i64) = {
            let row = client
                .query_one(
                    &format!(
                        "select to_regclass('public.refused') is not null, \
                         (select count(*) from {DEFAULT_SCHEMA}.transform_definitions), \
                         (select count(*) from pg_catalog.pg_class c \
                          join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
                          where n.nspname = '{DEFAULT_SCHEMA}' and c.relname like 'agg\\_%')"
                    ),
                    &[],
                )
                .await
                .expect("look for leftovers");
            (row.get(0), row.get(1), row.get(2))
        };
        assert_eq!(
            left,
            (false, 0, 0),
            "{field}: a refusal leaves nothing behind"
        );
    }
}
