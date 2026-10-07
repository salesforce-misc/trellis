//! Issue #830: a bare name that is both a source column and a field whose
//! expression is something else is refused at define, naming both meanings
//! and both qualified spellings. A qualified `<table>.<column>` reads the
//! source column.

use std::collections::HashMap;
use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{ValueType, chunk_queue, install_definition};
use trellis::intake::markers;
use trellis::integer::IntWidth;
use trellis::staging::{CdcOp, StagedChange, has_pending, retire_drained_segments};

const TEST_NAME: &str = "defs_ambiguous_bare_name_test";

fn source_columns() -> HashMap<String, ValueType> {
    HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("grp".to_string(), ValueType::Integer(IntWidth::Int4)),
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
    markers::discharge_registrations(pool)
        .await
        .expect("dispatch registered definitions' builds");
    loop {
        let client = pool.get().await.expect("acquire connection");
        let claimed = chunk_queue::claim_chunks(&**client, TEST_NAME, 1000)
            .await
            .expect("claim_chunks");
        drop(client);
        if claimed.is_empty() {
            return;
        }
        for chunk in &claimed {
            chunk_queue::run_claimed_chunk(
                pool,
                chunk,
                TEST_NAME,
                Duration::from_secs(5),
                Duration::from_secs(60),
            )
            .await
            .expect("the build must not fail");
            chunk_queue::finish_chunk(pool, chunk, TEST_NAME)
                .await
                .expect("finish_chunk");
        }
    }
}

async fn drain_to_quiescence(pool: &trellis::Pool, client: &mut Client) {
    use trellis::staging::{apply, seal};
    let watermark = trellis::staging::StagedWatermark::saturated();
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
            "trellis_defs_ambiguous_bare_name_test",
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

/// The issue's example: `val` is a source column and a field, and `SUM(val)`
/// used to read the field. Define refuses it and creates nothing; the message
/// spells the schema the bare `FROM` resolved to.
#[tokio::test]
async fn an_ambiguous_bare_name_is_refused_at_define() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table public.amb_src (id integer primary key, grp integer, val integer)",
        )
        .await
        .expect("create amb_src");

    let err = install_definition(
        &db.pool,
        "TRANSFORM amb_totals FROM amb_src GROUP BY grp \
         SELECT grp AS grp, (grp + 1) AS val, SUM(val) AS total",
        &source_columns(),
        "public",
    )
    .await
    .expect_err("SUM(val) could mean the source column or the field");
    assert_eq!(
        err.to_string(),
        "definition failed validation: calculated field 'total' reads 'val', which is ambiguous: it names both the source \
         column 'val' of 'amb_src' and the calculated field 'val' (`(grp + 1)`). Write \
         `amb_src.val` or `public.amb_src.val` to read the source column, or rename the field \
         'val' and read it by its new name"
    );
    let target_exists: bool = client
        .query_one("select to_regclass('public.amb_totals') is not null", &[])
        .await
        .expect("look up the target")
        .get(0);
    assert!(
        !target_exists,
        "a refused definition leaves no target behind"
    );
}

/// The qualified spelling defines, builds and applies `SUM` and `MAX` over the
/// source column, beside the field that shadows it.
#[tokio::test]
async fn a_qualified_name_aggregates_the_source_column() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table public.amb_src (id integer primary key, grp integer, val integer); \
             insert into public.amb_src (id, grp, val) values (1, 1, 5), (2, 1, 7), (3, 2, 9)",
        )
        .await
        .expect("create + seed amb_src");

    install_definition(
        &db.pool,
        "TRANSFORM amb_totals FROM amb_src GROUP BY grp \
         SELECT grp AS grp, (grp + 1) AS val, SUM(amb_src.val) AS total, \
         MAX(public.amb_src.val) AS biggest",
        &source_columns(),
        "public",
    )
    .await
    .expect("the qualified names are not ambiguous");
    drain_backfill_chunks(&db.pool).await;
    markers::settle_registrations(&db.pool).await;
    markers::run_pending_backfills(
        &mut client,
        "wake",
        &trellis::staging::StagedWatermark::saturated(),
        Duration::ZERO,
    )
    .await
    .expect("discharge the go-live catch-up");
    drain_to_quiescence(&db.pool, &mut client).await;

    let row = |grp: i32, val: &str, total: &str, biggest: &str| {
        (grp, val.to_string(), total.to_string(), biggest.to_string())
    };
    assert_eq!(
        read_totals(&client).await,
        vec![row(1, "2", "12", "7"), row(2, "3", "9", "9")]
    );

    // Live Apply folds the source column too, not the field: an insert and an
    // update, staged as capture would stage them.
    commit_and_stage(
        &mut client,
        "insert into public.amb_src (id, grp, val) values (4, 1, 100)",
        |lsn| {
            cdc(
                CdcOp::Insert,
                "4",
                lsn,
                None,
                Some(r#"{"id":"4","grp":"1","val":"100"}"#),
            )
        },
    )
    .await;
    commit_and_stage(
        &mut client,
        "update public.amb_src set val = 1 where id = 3",
        |lsn| {
            cdc(
                CdcOp::Update,
                "3",
                lsn,
                Some(r#"{"id":"3","grp":"2","val":"9"}"#),
                Some(r#"{"id":"3","grp":"2","val":"1"}"#),
            )
        },
    )
    .await;
    drain_to_quiescence(&db.pool, &mut client).await;
    assert_eq!(
        read_totals(&client).await,
        vec![row(1, "2", "112", "100"), row(2, "3", "1", "1")]
    );
}

/// Commits `sql` and stages `change` (built from the write's LSN) in one
/// transaction, as capture would stage it.
async fn commit_and_stage(client: &mut Client, sql: &str, change: impl Fn(PgLsn) -> StagedChange) {
    let txn = client.transaction().await.expect("begin source write");
    txn.batch_execute(sql).await.expect("source write");
    let lsn: PgLsn = txn
        .query_one("select pg_current_wal_insert_lsn()", &[])
        .await
        .expect("read the write's lsn")
        .get(0);
    trellis::staging::append(&txn, &[change(lsn)])
        .await
        .expect("stage the change's CDC row");
    txn.commit().await.expect("commit source write");
}

fn cdc(op: CdcOp, key: &str, lsn: PgLsn, old: Option<&str>, new: Option<&str>) -> StagedChange {
    StagedChange::Cdc {
        src_table: "public.amb_src".to_string(),
        key: key.to_string(),
        op,
        lsn: Some(lsn),
        old_image: old.map(str::to_string),
        new_image: new.map(str::to_string),
        origin_lsn: None,
        src_changed: None,
        hop_gen: 0,
        group_key: None,
    }
}

async fn read_totals(client: &Client) -> Vec<(i32, String, String, String)> {
    client
        .query(
            "select grp, val::text, total::text, biggest::text from amb_totals order by grp",
            &[],
        )
        .await
        .expect("read amb_totals")
        .into_iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect()
}

/// An `ALTER TRANSFORM` clause is parsed without its source, so the catalog
/// reads `<FROM table>.<column>` in it as the source column against the
/// stored definition. Unqualified, it would be a relationship path, which an
/// ALTER refuses. Re-adding the same field is a no-op, and a three-part name
/// must name the schema the bare `FROM` resolved to.
#[tokio::test]
async fn an_alter_reads_a_qualified_name_as_the_source_column() {
    use trellis::defs::ast::Expr;
    use trellis::defs::{Statement, alter_transform, parse_statement};

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table public.amb_src (id integer primary key, grp integer, val integer)",
        )
        .await
        .expect("create amb_src");
    install_definition(
        &db.pool,
        "TRANSFORM amb_calc FROM amb_src SELECT val AS val",
        &source_columns(),
        "public",
    )
    .await
    .expect("define");
    markers::discharge_registrations(&db.pool)
        .await
        .expect("dispatch the build");

    let alter = |text: &str| match parse_statement(text).expect("parse") {
        Statement::AlterTransform(alter) => alter,
        other => panic!("not an ALTER: {other:?}"),
    };
    let add = alter("ALTER TRANSFORM amb_calc ADD amb_src.val + 1 AS bumped");
    let outcome = alter_transform(&db.pool, &add).await.expect("add");
    assert_eq!(outcome.added, vec!["bumped".to_string()]);
    let bumped = outcome
        .definition
        .def
        .fields
        .iter()
        .find(|f| f.name == "bumped")
        .expect("the added field");
    assert!(
        matches!(&bumped.expr, Expr::BinaryOp { lhs, .. }
            if matches!(lhs.as_ref(), Expr::SourceColumn { table, column, .. }
                if table == "amb_src" && column == "val")),
        "{:?}",
        bumped.expr
    );

    let again = alter_transform(&db.pool, &add)
        .await
        .expect("re-adding the same field is a no-op");
    assert!(again.added.is_empty(), "{:?}", again.added);

    let err = alter_transform(
        &db.pool,
        &alter("ALTER TRANSFORM amb_calc ADD other.amb_src.val AS wrong"),
    )
    .await
    .expect_err("schema `other` is not the source's");
    assert!(
        err.to_string()
            .contains("must name this transform's FROM table ('public.amb_src')"),
        "{err}"
    );
}

/// A source column added after define under the name of a field another
/// field reads (`adj`) leaves the transform reading the field until a resume,
/// whose re-validation refuses the now-ambiguous name as define would.
#[tokio::test]
async fn a_resume_refuses_a_name_a_later_source_column_made_ambiguous() {
    use trellis::{ApplyError, Config, Trellis, TrellisError, TrellisOptions};

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table public.amb_src (id integer primary key, grp integer, val integer)",
        )
        .await
        .expect("create amb_src");
    let trellis = Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a define-only Trellis");
    trellis
        .apply(
            "TRANSFORM amb_totals FROM amb_src GROUP BY grp \
             SELECT grp AS grp, (grp + 1) AS adj, SUM(adj) AS total",
        )
        .await
        .expect("no source column is named adj yet");
    trellis
        .apply("PAUSE TRANSFORM amb_totals")
        .await
        .expect("pause");
    client
        .batch_execute("alter table public.amb_src add column adj integer")
        .await
        .expect("add a column named like the field");

    match trellis.apply("RESUME TRANSFORM amb_totals").await {
        Err(TrellisError::Apply(ApplyError::ResumeRefused { reason, .. })) => {
            let reason = reason.to_string();
            assert!(
                reason.contains("calculated field 'total' reads 'adj', which is ambiguous")
                    && reason.contains("`amb_src.adj` or `public.amb_src.adj`"),
                "{reason}"
            );
        }
        other => panic!("expected the resume to be refused, got {other:?}"),
    }
    trellis.shutdown().await.expect("shutdown");
}
