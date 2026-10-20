//! Issue #1168: every call that names a transform takes either spelling of
//! its target name, the schema-qualified one `apply` returns
//! (`<target schema>.<table>`) or the bare table name its text declared. A
//! qualified name with the wrong schema names nothing; there is no fallback to
//! the bare suffix.
//!
//! No test runs a pipeline: each asserts what a lookup finds or refuses.

use std::time::Duration;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::{DEFAULT_SCHEMA, DEFAULT_TARGET_SCHEMA};
use trellis::{
    ApplyError, CatalogError, Config, SelfCheckError, SelfCheckMode, Trellis, TrellisError,
    TrellisOptions,
};

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public"))
        .await
        .expect("set search_path");
    client
}

async fn define_only(dsn: &str) -> Trellis {
    Trellis::connect(
        Config::from_dsn(dsn.to_string()).expect("valid dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect a define-only Trellis")
}

async fn persisted_status(raw: &Client) -> Option<String> {
    raw.query_opt(
        "select status from transform_definitions where split_part(target_table, '.', 2) = 'rollup'",
        &[],
    )
    .await
    .expect("read status")
    .map(|row| row.get(0))
}

/// A registered aggregate `rollup`, and the qualified name `apply` returned.
async fn setup() -> (
    Trellis,
    Client,
    String,
    (TestCluster, testkit::TestDatabase),
) {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(
        "create table orders (id bigint primary key, g bigint, a numeric); \
         insert into orders select s, s % 2, s from generate_series(1, 4) s;",
    )
    .await
    .expect("seed");
    let trellis = define_only(db.dsn()).await;
    let qualified = trellis
        .apply("TRANSFORM rollup FROM orders GROUP BY g SELECT sum(a) AS total")
        .await
        .expect("define")
        .into_transform()
        .expect("a transform");
    assert_eq!(qualified, format!("{DEFAULT_TARGET_SCHEMA}.rollup"));
    (trellis, raw, qualified, (cluster, db))
}

fn wrong_schema() -> String {
    "no_such_schema.rollup".to_string()
}

#[tokio::test]
async fn the_read_methods_take_either_spelling() {
    let (trellis, _raw, qualified, _db) = setup().await;

    // status(define(..)) round-trips.
    for name in [qualified.as_str(), "rollup"] {
        assert!(
            trellis.status(name).await.expect("status").is_some(),
            "status({name})"
        );
        trellis
            .quarantine_status(name)
            .await
            .unwrap_or_else(|e| panic!("quarantine_status({name}): {e}"));
        trellis
            .sample_quarantined(name, None, 10)
            .await
            .unwrap_or_else(|e| panic!("sample_quarantined({name}): {e}"));
    }
    let entry = trellis.quarantine_status(&qualified).await.unwrap();
    assert_eq!(
        entry.target,
        trellis::QuarantineTarget::Transform("rollup".to_string()),
        "the entry names the transform, not a column"
    );

    // A wrong schema matches nothing, and does not fall back to the suffix.
    let wrong = wrong_schema();
    assert!(trellis.status(&wrong).await.unwrap().is_none());
    assert!(matches!(
        trellis.quarantine_status(&wrong).await,
        Err(TrellisError::TransformNotFound(_))
    ));
    // Read as a `transform.column` address of no such transform: no rows.
    assert!(
        trellis
            .sample_quarantined(&wrong, None, 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn release_key_and_self_check_take_either_spelling() {
    let (trellis, _raw, qualified, _db) = setup().await;

    // The lookup resolves, so what is refused is the key, not the transform.
    for name in [qualified.as_str(), "rollup"] {
        let err = trellis
            .release_key(name, "orders", "1")
            .await
            .expect_err("holds no such key");
        assert!(
            matches!(err, TrellisError::Apply(ApplyError::KeyNotHeld { .. })),
            "release_key({name}): {err:?}"
        );
    }
    assert!(matches!(
        trellis.release_key(&wrong_schema(), "orders", "1").await,
        Err(TrellisError::TransformNotFound(_))
    ));

    let check = |name: String| {
        let trellis = &trellis;
        async move {
            trellis
                .self_check(&name, SelfCheckMode::Strict, Duration::from_secs(1))
                .await
        }
    };
    for name in [qualified.clone(), "rollup".to_string()] {
        let found = !matches!(
            check(name.clone()).await,
            Err(TrellisError::SelfCheck(SelfCheckError::TargetNotFound(_)))
        );
        assert!(found, "self_check({name}) did not find the transform");
    }
    assert!(matches!(
        check(wrong_schema()).await,
        Err(TrellisError::SelfCheck(SelfCheckError::TargetNotFound(_)))
    ));
}

#[tokio::test]
async fn pause_resume_alter_and_drop_take_either_spelling() {
    let (trellis, raw, qualified, _db) = setup().await;

    // A wrong-schema qualified name names nothing, in every statement.
    let wrong = wrong_schema();
    for verb in ["PAUSE", "RESUME"] {
        assert!(
            trellis
                .apply(&format!("{verb} TRANSFORM {wrong}"))
                .await
                .is_err(),
            "{verb} {wrong}"
        );
    }
    assert!(matches!(
        trellis
            .apply(&format!("ALTER TRANSFORM {wrong} ADD total + 1 AS t2"))
            .await,
        Err(TrellisError::Catalog(
            CatalogError::TransformNotFound { .. }
        ))
    ));
    trellis
        .apply(&format!("DROP TRANSFORM {wrong}"))
        .await
        .expect("an absent drop is a no-op");
    assert!(
        persisted_status(&raw).await.is_some(),
        "nothing was dropped"
    );
    assert_eq!(
        persisted_status(&raw).await.as_deref(),
        Some("waiting_to_backfill")
    );

    // PAUSE / RESUME with the qualified name act on the whole transform.
    trellis
        .apply(&format!("PAUSE TRANSFORM {qualified}"))
        .await
        .expect("pause by qualified name");
    assert_eq!(persisted_status(&raw).await.as_deref(), Some("paused"));
    trellis
        .apply("RESUME TRANSFORM rollup")
        .await
        .expect("resume by bare name");
    assert_ne!(persisted_status(&raw).await.as_deref(), Some("paused"));
    trellis
        .apply("PAUSE TRANSFORM rollup")
        .await
        .expect("pause by bare name");
    trellis
        .apply(&format!("RESUME TRANSFORM {qualified}"))
        .await
        .expect("resume by qualified name");
    assert_ne!(persisted_status(&raw).await.as_deref(), Some("paused"));

    // ALTER finds the transform by the qualified name (an aggregate is then
    // refused for its shape, not for being unknown).
    let err = trellis
        .apply(&format!("ALTER TRANSFORM {qualified} ADD total + 1 AS t2"))
        .await
        .expect_err("not an editable shape");
    assert!(
        !matches!(
            err,
            TrellisError::Catalog(CatalogError::TransformNotFound { .. })
        ),
        "{err:?}"
    );

    // DROP by the qualified name.
    trellis
        .apply(&format!("PAUSE TRANSFORM {qualified}"))
        .await
        .unwrap();
    trellis
        .apply(&format!("DROP TRANSFORM {qualified}"))
        .await
        .expect("drop by qualified name");
    assert!(persisted_status(&raw).await.is_none(), "dropped");
}

/// `rollup.total` is both column `total` of transform `rollup` and the
/// qualified name of a transform `total` in schema `rollup`. Column addressing
/// wins (ADR-0003): the column has no other spelling, while the transform is
/// still named by its bare `total`.
#[tokio::test]
async fn a_column_address_wins_over_the_qualified_name_it_spells() {
    let (trellis, raw, _qualified, _db) = setup().await;
    raw.batch_execute("create schema rollup")
        .await
        .expect("create schema");
    let other = trellis
        .apply("TRANSFORM rollup.total FROM orders GROUP BY g SELECT count(*) AS n")
        .await
        .expect("define rollup.total")
        .into_transform()
        .expect("a transform");
    assert_eq!(other, "rollup.total");

    assert_eq!(
        trellis
            .quarantine_status("rollup.total")
            .await
            .unwrap()
            .target,
        trellis::QuarantineTarget::Column("rollup".to_string(), "total".to_string()),
    );
    assert_eq!(
        trellis.quarantine_status("total").await.unwrap().target,
        trellis::QuarantineTarget::Transform("total".to_string()),
    );
    // `status` takes no column, so there the address is the qualified name.
    assert!(trellis.status("rollup.total").await.unwrap().is_some());

    // PAUSE reads it as the column too; whatever it does to the column, the
    // transform `total` is not paused.
    let _ = trellis.apply("PAUSE TRANSFORM rollup.total").await;
    let status: String = raw
        .query_one(
            "select status from transform_definitions where target_table = 'rollup.total'",
            &[],
        )
        .await
        .expect("read status")
        .get(0);
    assert_ne!(status, "paused");
}

/// `DROP TRANSFORM <transform>.<column>` is refused, changing nothing, rather
/// than reported as a drop of a transform named `<transform>.<column>`.
#[tokio::test]
async fn drop_refuses_a_column_address_of_a_registered_transform() {
    let (trellis, raw, _qualified, _db) = setup().await;
    let err = trellis
        .apply("DROP TRANSFORM rollup.total")
        .await
        .expect_err("a column address is not a DROP target");
    assert!(
        matches!(
            &err,
            TrellisError::Parse(trellis::ParseError::MalformedDefinitionAddress { .. })
        ),
        "{err:?}"
    );
    assert!(
        err.to_string()
            .contains("ALTER TRANSFORM rollup DROP total"),
        "{err}"
    );
    assert_eq!(
        persisted_status(&raw).await.as_deref(),
        Some("waiting_to_backfill")
    );
}
