//! Issue #979: a schema-qualified `TRANSFORM <schema>.<target>` may not name
//! a Trellis catalog schema, this instance's own or another instance's (a
//! schema holding a `trellis_instance` table). Define refuses it before it
//! creates anything, and a `RESUME` of a definition whose target schema has
//! since become one refuses it the same way. Direct, with no polling.

use testkit::{TestCluster, TestDatabase};
use trellis::{
    ApplyError, CatalogError, Config, Pool, Trellis, TrellisError, TrellisOptions, config, migrate,
};

const SOURCE: &str = "create table public.cat_src (id integer primary key, v integer)";

async fn raw(db: &TestDatabase, sql: &str) {
    db.pool
        .get()
        .await
        .expect("connect")
        .batch_execute(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn relation_exists(db: &TestDatabase, qualified: &str) -> bool {
    db.pool
        .get()
        .await
        .expect("connect")
        .query_one("select to_regclass($1) is not null", &[&qualified])
        .await
        .expect("look up the relation")
        .get(0)
}

async fn connect(db: &TestDatabase) -> Trellis {
    Trellis::connect(
        Config::from_dsn(db.dsn().to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a define-only Trellis")
}

/// The refusal define made, as `(target, schema, own, message)`.
fn refusal(result: Result<impl std::fmt::Debug, TrellisError>) -> (String, String, bool, String) {
    match result {
        Err(TrellisError::Catalog(
            ref err @ CatalogError::TargetInCatalogSchema {
                ref target,
                ref schema,
                own,
            },
        )) => {
            assert_eq!(err.code(), trellis::ErrorCode::Validation);
            (target.clone(), schema.clone(), own, err.to_string())
        }
        other => panic!("expected TargetInCatalogSchema, got {other:?}"),
    }
}

#[tokio::test]
async fn a_target_qualified_into_this_instances_catalog_schema_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    raw(&db, SOURCE).await;
    let trellis = connect(&db).await;

    let (target, schema, own, message) = refusal(
        trellis
            .apply(&format!(
                "TRANSFORM {}.cat_out FROM cat_src SELECT v AS v",
                config::DEFAULT_SCHEMA
            ))
            .await,
    );

    assert_eq!(schema, config::DEFAULT_SCHEMA);
    assert_eq!(target, format!("{}.cat_out", config::DEFAULT_SCHEMA));
    assert!(own);
    assert!(
        message.contains("this instance's catalog schema")
            && message.contains("TRANSFORM <schema>.<target>"),
        "names the schema and the remedy: {message}"
    );
    assert!(
        !relation_exists(&db, &format!("{}.cat_out", config::DEFAULT_SCHEMA)).await,
        "a refused define creates no table"
    );
    trellis.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_target_qualified_into_another_instances_catalog_schema_is_refused() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    raw(&db, SOURCE).await;
    // Another instance attaches, with `instance_b` as its catalog schema.
    let other = Config::with_schema(db.dsn(), "instance_b").expect("valid schema");
    migrate(&Pool::new(&other).expect("pool"), &other)
        .await
        .expect("attach the other instance");
    let trellis = connect(&db).await;

    // Both an aggregate and a 1-1 target create a ledger beside the target.
    for (name, body) in [
        ("cat_one", "SELECT v AS v"),
        ("cat_agg", "GROUP BY v SELECT v AS v, COUNT(*) AS n"),
    ] {
        let (_, schema, own, message) = refusal(
            trellis
                .apply(&format!("TRANSFORM instance_b.{name} FROM cat_src {body}"))
                .await,
        );
        assert_eq!(schema, "instance_b");
        assert!(!own);
        assert!(
            message.contains("another Trellis instance's catalog")
                && message.contains("\"instance_b\""),
            "names the schema: {message}"
        );
        assert!(
            !relation_exists(&db, &format!("instance_b.{name}")).await,
            "a refused define creates no table"
        );
    }
    trellis.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_target_qualified_into_an_ordinary_schema_is_accepted() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    raw(&db, SOURCE).await;
    raw(&db, "create schema reporting").await;
    let trellis = connect(&db).await;

    trellis
        .apply("TRANSFORM reporting.cat_out FROM cat_src SELECT v AS v")
        .await
        .expect("an ordinary schema is a fine target");

    assert!(relation_exists(&db, "reporting.cat_out").await);
    trellis.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_resume_refuses_a_target_schema_that_became_another_instances_catalog() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    raw(&db, SOURCE).await;
    raw(&db, "create schema late").await;
    let trellis = connect(&db).await;
    trellis
        .apply("TRANSFORM late.cat_out FROM cat_src SELECT v AS v")
        .await
        .expect("`late` is an ordinary schema when the transform is defined");
    trellis
        .apply("PAUSE TRANSFORM cat_out")
        .await
        .expect("pause");
    // `late` now holds a `trellis_instance` table: another instance's
    // catalog, as far as anything can tell.
    raw(&db, "create table late.trellis_instance (schema_name text)").await;

    match trellis.apply("RESUME TRANSFORM cat_out").await {
        Err(TrellisError::Apply(ApplyError::ResumeRefused { reason, .. })) => {
            assert!(
                matches!(
                    *reason,
                    CatalogError::TargetInCatalogSchema { own: false, ref schema, .. }
                        if schema == "late"
                ),
                "{reason:?}"
            );
        }
        other => panic!("expected the resume to be refused, got {other:?}"),
    }
    trellis.shutdown().await.expect("shutdown");
}
