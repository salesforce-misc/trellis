//! `<table>.c AS c` and `<schema>.<table>.c AS c` are the passthrough of `c`
//! wherever a bare `c AS c` is (ADR-0004, ADR-0022). A column Postgres
//! reserves can only be written qualified, so before this a 1-1 transform had
//! no way to copy `order` to a target column of the same name.
//!
//! Capture is the real triggers, and the ring is drained by hand
//! (`support/trigger_pipeline.rs`), so nothing here polls for convergence.

use std::collections::HashMap;

use trellis::defs::ast::ValueType;
use trellis::defs::install_definition;
use trellis::integer::IntWidth;

#[path = "support/trigger_pipeline.rs"]
mod trigger_pipeline;

use trigger_pipeline::Pipeline;

const SOURCE_DDL: &str = "create table public.qp_src (id bigint primary key, grp integer, \
     val integer, \"order\" integer, \"group\" integer, \"Mixed\" integer, amt numeric(10,2)); \
     insert into public.qp_src values (1, 1, 5, 10, 30, 100, 1.25), (2, 1, 7, 11, 31, 101, 2.50), \
     (3, 2, 9, 12, 32, 102, 3.75)";

fn source_columns() -> HashMap<String, ValueType> {
    let int4 = ValueType::Integer(IntWidth::Int4);
    HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
        ("grp".to_string(), int4),
        ("val".to_string(), int4),
        ("order".to_string(), int4),
        ("group".to_string(), int4),
        ("Mixed".to_string(), int4),
        ("amt".to_string(), ValueType::Numeric),
    ])
}

async fn install(db: &testkit::TestDatabase, text: &str) {
    install_definition(&db.pool, text, &source_columns(), "public")
        .await
        .unwrap_or_else(|e| panic!("install {text:?}: {e}"));
    trellis::intake::harness::settle_builds(&db.pool).await;
}

async fn install_err(db: &testkit::TestDatabase, text: &str) -> String {
    install_definition(&db.pool, text, &source_columns(), "public")
        .await
        .expect_err(text)
        .to_string()
}

fn rows(pairs: &[(&str, &str)]) -> HashMap<String, Option<String>> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), Some(v.to_string())))
        .collect()
}

async fn column_type(pipeline: &Pipeline, table: &str, column: &str) -> String {
    pipeline
        .raw
        .query_one(
            "select format_type(a.atttypid, a.atttypmod) from pg_attribute a \
             where a.attrelid = ('public.' || $1)::regclass and a.attname = $2 \
             and not a.attisdropped",
            &[&table, &column],
        )
        .await
        .unwrap_or_else(|e| panic!("{table}.{column}: {e}"))
        .get(0)
}

/// A 1-1 transform copies reserved-word and mixed-case columns under their
/// own names through a qualified passthrough, with the source's type and
/// modifier, a backfill, and CDC.
#[tokio::test]
async fn a_qualified_passthrough_copies_a_reserved_word_column_in_a_one_to_one_transform() {
    let (cluster, db, raw) = trigger_pipeline::database().await;
    raw.batch_execute(SOURCE_DDL).await.expect("seed");
    install(
        &db,
        "TRANSFORM qp_copy FROM qp_src SELECT qp_src.order AS order, \
         public.qp_src.group AS group, qp_src.Mixed AS Mixed, qp_src.amt AS amt, \
         qp_src.val AS val",
    )
    .await;
    // The bare spelling of the same copy, for the target's column types.
    install(
        &db,
        "TRANSFORM qp_bare FROM qp_src SELECT amt AS amt, val AS val",
    )
    .await;

    let mut pipeline = Pipeline::attach(cluster, db, raw, &["public.qp_src"]).await;
    const COPY: &str = "select id::text, concat_ws(',', \"order\", \"group\", \"Mixed\", amt, val) \
                        from public.qp_copy";
    assert_eq!(
        pipeline.rows(COPY).await,
        rows(&[
            ("1", "10,30,100,1.25,5"),
            ("2", "11,31,101,2.50,7"),
            ("3", "12,32,102,3.75,9")
        ]),
        "the backfill copied every column"
    );
    // A copy carries the type, not its modifier (ADR-0020).
    assert_eq!(column_type(&pipeline, "qp_copy", "amt").await, "numeric");
    assert_eq!(
        column_type(&pipeline, "qp_copy", "amt").await,
        column_type(&pipeline, "qp_bare", "amt").await,
        "the qualified passthrough carries the type as the bare one does"
    );

    pipeline
        .raw
        .batch_execute(
            "update public.qp_src set \"order\" = 99, \"Mixed\" = 7, amt = 9.99 where id = 2; \
             insert into public.qp_src values (4, 2, 1, 13, 33, 103, 4.00); \
             delete from public.qp_src where id = 1",
        )
        .await
        .expect("write the source");
    pipeline.drain().await;
    assert_eq!(
        pipeline.rows(COPY).await,
        rows(&[
            ("2", "99,31,7,9.99,7"),
            ("3", "12,32,102,3.75,9"),
            ("4", "13,33,103,4.00,1")
        ]),
        "CDC reads the reserved-word and mixed-case columns"
    );
}

/// In an aggregate the qualified passthrough of a `GROUP BY` key is accepted
/// beside it, bare or qualified in the clause, for a reserved-word key too.
#[tokio::test]
async fn a_qualified_passthrough_of_a_group_by_key_is_accepted() {
    let (cluster, db, raw) = trigger_pipeline::database().await;
    raw.batch_execute(SOURCE_DDL).await.expect("seed");
    install(
        &db,
        "TRANSFORM qp_by_grp FROM qp_src GROUP BY grp WITHOUT SOURCE INDEX \
         SELECT qp_src.grp AS grp, SUM(val) AS total",
    )
    .await;
    install(
        &db,
        "TRANSFORM qp_by_group FROM qp_src GROUP BY qp_src.group WITHOUT SOURCE INDEX \
         SELECT public.qp_src.group AS group, SUM(val) AS total",
    )
    .await;

    // The qualified passthrough shadows nothing, so another field reads the
    // bare name without ambiguity.
    install(
        &db,
        "TRANSFORM qp_next FROM qp_src GROUP BY grp WITHOUT SOURCE INDEX \
         SELECT qp_src.grp AS grp, grp + 1 AS next_grp, SUM(val) AS total",
    )
    .await;

    let mut pipeline = Pipeline::attach(cluster, db, raw, &["public.qp_src"]).await;
    assert_eq!(
        pipeline
            .rows("select grp::text, next_grp::text from public.qp_next")
            .await,
        rows(&[("1", "2"), ("2", "3")])
    );
    const BY_GRP: &str = "select grp::text, total::text from public.qp_by_grp";
    const BY_GROUP: &str = "select \"group\"::text, total::text from public.qp_by_group";
    assert_eq!(
        pipeline.rows(BY_GRP).await,
        rows(&[("1", "12"), ("2", "9")])
    );
    assert_eq!(
        pipeline.rows(BY_GROUP).await,
        rows(&[("30", "5"), ("31", "7"), ("32", "9")])
    );

    pipeline
        .raw
        .batch_execute("update public.qp_src set val = 20 where id = 1")
        .await
        .expect("write the source");
    pipeline.drain().await;
    assert_eq!(
        pipeline.rows(BY_GRP).await,
        rows(&[("1", "27"), ("2", "9")])
    );
    assert_eq!(
        pipeline.rows(BY_GROUP).await,
        rows(&[("30", "20"), ("31", "7"), ("32", "9")])
    );
}

/// Only a read of the field's own column, through the `FROM` table, is a
/// passthrough: every other shape that takes a source column's name is still
/// refused, and so is a qualifier naming another table.
#[tokio::test]
async fn a_qualified_read_that_is_not_the_passthrough_is_still_refused() {
    let (cluster, db, raw) = trigger_pipeline::database().await;
    raw.batch_execute(SOURCE_DDL).await.expect("seed");
    let _cluster = cluster;

    // A different column under the name of a source column.
    let err = install_err(&db, "TRANSFORM qp_bad FROM qp_src SELECT qp_src.grp AS val").await;
    assert!(
        err.contains("shares its name with a source column"),
        "{err}"
    );
    // An expression over the column.
    let err = install_err(
        &db,
        "TRANSFORM qp_bad FROM qp_src SELECT (qp_src.val + 1) AS val",
    )
    .await;
    assert!(
        err.contains("shares its name with a source column"),
        "{err}"
    );
    // Another table's name as the qualifier.
    let err = install_err(&db, "TRANSFORM qp_bad FROM qp_src SELECT other.val AS val").await;
    assert!(err.contains("other.val"), "{err}");
    // Another schema's.
    let err = install_err(
        &db,
        "TRANSFORM qp_bad FROM qp_src SELECT elsewhere.qp_src.val AS val",
    )
    .await;
    assert!(err.contains("elsewhere.qp_src.val"), "{err}");
    // A qualified column that isn't on the source.
    let err = install_err(
        &db,
        "TRANSFORM qp_bad FROM qp_src SELECT qp_src.nope AS nope",
    )
    .await;
    assert!(err.contains("qp_src.nope"), "{err}");
    // In an aggregate: a key's field is the key, not another column or an
    // aggregate under its name.
    let err = install_err(
        &db,
        "TRANSFORM qp_bad FROM qp_src GROUP BY grp WITHOUT SOURCE INDEX \
         SELECT qp_src.val AS grp",
    )
    .await;
    assert!(err.contains("GROUP BY column"), "{err}");
    let err = install_err(
        &db,
        "TRANSFORM qp_bad FROM qp_src GROUP BY grp WITHOUT SOURCE INDEX \
         SELECT SUM(qp_src.val) AS grp",
    )
    .await;
    assert!(err.contains("GROUP BY column"), "{err}");
}
