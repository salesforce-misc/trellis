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
        for relation in [
            format!("instance_b.{name}"),
            format!("instance_b.{name}__ledger"),
            format!("instance_b.{name}__deltas"),
        ] {
            assert!(
                !relation_exists(&db, &relation).await,
                "a refused define creates no table: {relation}"
            );
        }
    }
    assert_eq!(
        definition_count(&db, config::DEFAULT_SCHEMA).await,
        0,
        "a refused define persists no definition"
    );
    trellis.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_target_qualified_into_an_ordinary_schema_is_accepted() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    raw(&db, SOURCE).await;
    // Other tables, and a relation named `trellis_instance` that isn't a
    // table (the attach scan matches tables only, too), don't make a
    // schema a catalog.
    raw(
        &db,
        "create schema reporting; \
         create table reporting.unrelated (id integer); \
         create view reporting.trellis_instance as select 1 as one",
    )
    .await;
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
        Err(TrellisError::Apply(err @ ApplyError::ResumeRefused { .. })) => {
            let ApplyError::ResumeRefused { reason, .. } = &err else {
                unreachable!()
            };
            assert!(
                matches!(
                    **reason,
                    CatalogError::TargetInCatalogSchema { own: false, ref schema, .. }
                        if schema == "late"
                ),
                "{reason:?}"
            );
            // Removing the other catalog from `late` would take the target
            // with it, so the message doesn't offer "fix the schema and
            // resume": it names the drop and redefine.
            let message = err.to_string();
            assert!(
                !message.contains("define would refuse")
                    && message.contains("`DROP TRANSFORM` it and define it again"),
                "{message}"
            );
        }
        other => panic!("expected the resume to be refused, got {other:?}"),
    }
    let status: String = db
        .pool
        .get()
        .await
        .expect("connect")
        .query_one(
            &format!(
                "select status from {}.transform_definitions",
                config::DEFAULT_SCHEMA
            ),
            &[],
        )
        .await
        .expect("read the status")
        .get(0);
    assert_eq!(status, "paused", "a refused resume leaves it paused");
    trellis.shutdown().await.expect("shutdown");
}

async fn definition_count(db: &TestDatabase, catalog_schema: &str) -> i64 {
    db.pool
        .get()
        .await
        .expect("connect")
        .query_one(
            &format!("select count(*) from \"{catalog_schema}\".transform_definitions"),
            &[],
        )
        .await
        .expect("count the definitions")
        .get(0)
}

/// Schema names compare exactly, as Postgres stores them: the grammar keeps
/// an identifier's case, and the DDL quotes it, so `Cat_Mixed` and
/// `cat_mixed` are two schemas, only the first of them a catalog.
#[tokio::test]
async fn a_catalog_schema_is_matched_by_its_exact_mixed_case_name() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    raw(&db, SOURCE).await;
    raw(&db, "create schema cat_mixed; create schema other_mixed").await;
    let other = Config::with_schema(db.dsn(), "Other_Mixed").expect("valid schema");
    migrate(&Pool::new(&other).expect("pool"), &other)
        .await
        .expect("attach the other instance");
    let config = Config::with_schema(db.dsn(), "Cat_Mixed").expect("valid schema");
    migrate(&Pool::new(&config).expect("pool"), &config)
        .await
        .expect("attach this instance");
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect a define-only Trellis");

    let (_, schema, own, _) = refusal(
        trellis
            .apply("TRANSFORM Cat_Mixed.mc_own FROM cat_src SELECT v AS v")
            .await,
    );
    assert_eq!((schema.as_str(), own), ("Cat_Mixed", true));
    let (_, schema, own, _) = refusal(
        trellis
            .apply("TRANSFORM Other_Mixed.mc_other FROM cat_src SELECT v AS v")
            .await,
    );
    assert_eq!((schema.as_str(), own), ("Other_Mixed", false));

    // The lower-case namesakes are ordinary schemas.
    for statement in [
        "TRANSFORM cat_mixed.mc_own FROM cat_src SELECT v AS v",
        "TRANSFORM other_mixed.mc_other FROM cat_src SELECT v AS v",
    ] {
        trellis
            .apply(statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    assert!(relation_exists(&db, "cat_mixed.mc_own").await);
    assert!(relation_exists(&db, "other_mixed.mc_other").await);
    assert!(!relation_exists(&db, "\"Cat_Mixed\".mc_own").await);
    assert!(!relation_exists(&db, "\"Other_Mixed\".mc_other").await);
    trellis.shutdown().await.expect("shutdown");
}

/// The probe reads `pg_class`, which every role can read, so a
/// `trellis_instance` table the role holds no privilege on (which
/// `information_schema` hides), or sits in a schema the role lacks `USAGE`
/// on, is still found.
#[tokio::test]
async fn a_trellis_instance_table_the_role_cannot_read_is_still_found() {
    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let database = db.name();
    raw(
        &db,
        &format!(
            "create role trellis login; \
             grant create, connect, temporary on database \"{database}\" to trellis; \
             grant usage on schema public to trellis; \
             {SOURCE}; \
             alter table public.cat_src owner to trellis; \
             create schema hidden; \
             grant usage, create on schema hidden to trellis; \
             create table hidden.trellis_instance (schema_name text); \
             create schema nousage; \
             grant create on schema nousage to trellis; \
             create table nousage.trellis_instance (schema_name text);"
        ),
    )
    .await;
    assert!(db.dsn().contains("user=postgres"), "{}", db.dsn());
    let config =
        Config::from_dsn(db.dsn().replace("user=postgres", "user=trellis")).expect("valid dsn");
    migrate(&Pool::new(&config).expect("pool"), &config)
        .await
        .expect("attach as the Trellis role");
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect as the Trellis role");

    for schema_name in ["hidden", "nousage"] {
        let (_, schema, own, _) = refusal(
            trellis
                .apply(&format!(
                    "TRANSFORM {schema_name}.cat_out FROM cat_src SELECT v AS v"
                ))
                .await,
        );
        assert_eq!((schema.as_str(), own), (schema_name, false));
        assert!(!relation_exists(&db, &format!("{schema_name}.cat_out")).await);
    }
    trellis.shutdown().await.expect("shutdown");
}

/// A define refused in a target schema that became a catalog after an
/// earlier define there, of a new target or of the existing one's own
/// name, creates nothing and leaves the existing definition as it was.
#[tokio::test]
async fn a_refused_define_beside_an_existing_transform_changes_nothing() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    raw(&db, SOURCE).await;
    raw(&db, "create schema late").await;
    let trellis = connect(&db).await;
    trellis
        .apply("TRANSFORM late.cat_out FROM cat_src SELECT v AS v")
        .await
        .expect("`late` is an ordinary schema when the transform is defined");
    raw(&db, "create table late.trellis_instance (schema_name text)").await;

    for statement in [
        "TRANSFORM late.cat_out FROM cat_src SELECT v AS v",
        "TRANSFORM late.cat_agg FROM cat_src GROUP BY v SELECT v AS v, COUNT(*) AS n",
    ] {
        refusal(trellis.apply(statement).await);
    }

    assert_eq!(definition_count(&db, config::DEFAULT_SCHEMA).await, 1);
    for relation in [
        "late.cat_agg",
        "late.cat_agg__ledger",
        "late.cat_agg__deltas",
    ] {
        assert!(!relation_exists(&db, relation).await, "{relation}");
    }
    assert!(relation_exists(&db, "late.cat_out").await);
    trellis.shutdown().await.expect("shutdown");
}
