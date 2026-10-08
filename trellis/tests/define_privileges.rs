//! Define refuses a table the Trellis role can't see by naming the missing
//! privilege (issue #933), not by blaming the table's shape or calling it
//! missing.
//!
//! Bare names resolve through `information_schema.tables` and the
//! connection's `search_path`, which hide a table the role holds no
//! privilege on and skip a schema it lacks `USAGE` on. Before #933, a
//! `RELATIONSHIP` to such a table was refused as "must be a plain table ...
//! with a primary key" or as a column that "does not exist", and a
//! `TRANSFORM` from it as "not found". Each case here covers a define path
//! that resolves a name: a `TRANSFORM`'s bare and qualified `FROM`, and a
//! `RELATIONSHIP`'s endpoints.
//!
//! The roles follow `docs/recommendations.md` ("One Trellis role"): the
//! `trellis` login role is a member of `app_owner`, which owns the sources,
//! and holds the documented grants, less the one each test withholds.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::defs::ValueType;
use trellis::{CatalogError, Config, ErrorCode, Trellis, TrellisOptions};

const SCHEMA: &str = trellis::config::DEFAULT_SCHEMA;
const TARGETS: &str = "trellis_targets";

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

fn as_role(dsn: &str, role: &str) -> String {
    assert!(dsn.contains("user=postgres"), "{dsn}");
    dsn.replace("user=postgres", &format!("user={role}"))
}

/// A database with the documented roles and grants, `public.orders` owned by
/// `app_owner`, and the instance migrated as `trellis`. `public.customers`
/// and `app.items` are owned by `other_owner`, which `trellis` isn't a member
/// of, and `trellis` has no `USAGE` on schema `app`.
async fn set_up(cluster: &TestCluster) -> (testkit::TestDatabase, Client, trellis::Pool, Trellis) {
    let db = cluster.create_empty_database().await;
    let admin = connect(db.dsn()).await;
    let database = db.name();
    admin
        .batch_execute(&format!(
            "create role app_owner nologin; \
             create role other_owner nologin; \
             create role trellis login; \
             revoke all on database \"{database}\" from public; \
             revoke all on schema public from public; \
             create schema {TARGETS}; \
             create schema app; \
             create table public.orders (id int primary key, customer_id int, amount int); \
             create table public.customers (id int primary key, name text); \
             create table app.items (id int primary key, name text); \
             alter table public.orders owner to app_owner; \
             alter table public.customers owner to other_owner; \
             alter table app.items owner to other_owner; \
             alter schema app owner to other_owner; \
             grant create, connect, temporary on database \"{database}\" to trellis; \
             grant app_owner to trellis; \
             grant create, usage on schema {TARGETS} to trellis; \
             grant usage on schema public to trellis;"
        ))
        .await
        .expect("the roles and sources");

    let config = Config::with_schema(as_role(db.dsn(), "trellis"), SCHEMA)
        .expect("valid config")
        .with_target_schema(TARGETS)
        .expect("valid target schema");
    let pool = trellis::Pool::new(&config).expect("pool");
    trellis::migrate(&pool, &config)
        .await
        .expect("migrate as the Trellis role");
    let trellis = Trellis::connect(config, TrellisOptions::default())
        .await
        .expect("connect as the Trellis role");
    (db, admin, pool, trellis)
}

/// `statement` is refused with #933's message: it names `table`, says what
/// the role lacks (`lacks`), and points at the doc section that lists the
/// grants.
async fn assert_refused(trellis: &Trellis, statement: &str, table: &str, lacks: &str) {
    let err = trellis
        .apply(statement)
        .await
        .err()
        .unwrap_or_else(|| panic!("{statement}: accepted"));
    let message = err.to_string();
    for wanted in [
        format!("\"{table}\""),
        "role \"trellis\"".to_string(),
        lacks.to_string(),
        "\"One Trellis role\" in docs/recommendations.md".to_string(),
    ] {
        assert!(
            message.contains(&wanted),
            "{statement}: expected {wanted:?} in: {message}"
        );
    }
    for misleading in ["not found", "does not exist", "plain table", "primary key"] {
        assert!(
            !message.contains(misleading),
            "{statement}: misleading {misleading:?} in: {message}"
        );
    }
    assert_eq!(err.code(), ErrorCode::Validation, "{statement}: {message}");
}

#[tokio::test]
async fn a_table_the_role_holds_no_privilege_on_is_refused_naming_its_owner() {
    let cluster = TestCluster::start();
    let (_db, _admin, _pool, trellis) = set_up(&cluster).await;
    let lacks = "isn't a member of \"other_owner\", the table's owner";

    // The to-side: `orders` is visible, `customers` isn't.
    assert_refused(
        &trellis,
        "RELATIONSHIP customer FROM orders.customer_id TO customers.id",
        "public.customers",
        lacks,
    )
    .await;
    // The from-side.
    assert_refused(
        &trellis,
        "RELATIONSHIP order_of FROM customers.id TO orders.id",
        "public.customers",
        lacks,
    )
    .await;
    assert_refused(
        &trellis,
        "TRANSFORM customer_copy FROM customers SELECT name AS name",
        "public.customers",
        lacks,
    )
    .await;
    assert_refused(
        &trellis,
        "TRANSFORM customer_copy FROM public.customers SELECT name AS name",
        "public.customers",
        lacks,
    )
    .await;

    // A table the role can see still defines.
    trellis
        .apply("TRANSFORM order_copy FROM orders SELECT amount AS amount")
        .await
        .expect("a source the role owns through app_owner");
}

#[tokio::test]
async fn a_table_in_a_schema_the_role_lacks_usage_on_is_refused_naming_the_schema() {
    let cluster = TestCluster::start();
    let (_db, admin, _pool, trellis) = set_up(&cluster).await;
    admin
        .batch_execute("revoke usage on schema public from trellis")
        .await
        .expect("revoke USAGE on public");

    let lacks = "lacks USAGE on schema \"public\"";
    assert_refused(
        &trellis,
        "RELATIONSHIP customer FROM orders.customer_id TO customers.id",
        "public.orders",
        lacks,
    )
    .await;
    assert_refused(
        &trellis,
        "TRANSFORM order_copy FROM orders SELECT amount AS amount",
        "public.orders",
        lacks,
    )
    .await;
    assert_refused(
        &trellis,
        "TRANSFORM order_copy FROM public.orders SELECT amount AS amount",
        "public.orders",
        lacks,
    )
    .await;
    // A schema off the search path, which only a qualified name reaches. The
    // role lacks both privileges on it, and the message names both.
    assert_refused(
        &trellis,
        "TRANSFORM item_copy FROM app.items SELECT name AS name",
        "app.items",
        "lacks USAGE on schema \"app\"",
    )
    .await;
    assert_refused(
        &trellis,
        "TRANSFORM item_copy FROM app.items SELECT name AS name",
        "app.items",
        "isn't a member of \"other_owner\", the table's owner",
    )
    .await;
}

#[tokio::test]
async fn a_table_that_does_not_exist_is_still_not_found() {
    let cluster = TestCluster::start();
    let (_db, _admin, _pool, trellis) = set_up(&cluster).await;
    for statement in [
        "TRANSFORM nothing_copy FROM nothing SELECT name AS name",
        "TRANSFORM nothing_copy FROM public.nothing SELECT name AS name",
    ] {
        let err = trellis
            .apply(statement)
            .await
            .err()
            .unwrap_or_else(|| panic!("{statement}: accepted"));
        assert_eq!(err.code(), ErrorCode::NotFound, "{statement}: {err}");
        assert!(
            !err.to_string().contains("One Trellis role"),
            "{statement}: {err}"
        );
    }
}

/// The catalog entry points refuse it too, not only `Trellis::apply`, which
/// introspects the source's columns before reaching them.
#[tokio::test]
async fn the_catalog_entry_points_refuse_it_too() {
    let cluster = TestCluster::start();
    let (_db, _admin, pool, _trellis) = set_up(&cluster).await;
    let columns: HashMap<String, ValueType> = [
        ("id".to_string(), ValueType::Numeric),
        ("name".to_string(), ValueType::Text),
    ]
    .into();
    for statement in [
        "TRANSFORM customer_copy FROM customers SELECT name AS name",
        "TRANSFORM customer_copy FROM public.customers SELECT name AS name",
    ] {
        let installed = trellis::defs::install_definition(&pool, statement, &columns, TARGETS)
            .await
            .err();
        let created = trellis::defs::create_definition(&pool, statement, &columns)
            .await
            .err();
        for (entry, err) in [
            ("install_definition", installed),
            ("create_definition", created),
        ] {
            assert!(
                matches!(
                    &err,
                    Some(CatalogError::TableNotAccessible { table, lacks_table_privilege: true, .. })
                        if table == "public.customers"
                ),
                "{entry} {statement}: {err:?}"
            );
        }
    }
}

/// `request_backfill` resolves a bare name the way define does, and refuses a
/// table the role can't use the same way, rather than as not found.
#[tokio::test]
async fn request_backfill_names_the_missing_privilege_too() {
    let cluster = TestCluster::start();
    let (_db, _admin, _pool, trellis) = set_up(&cluster).await;
    let err = trellis
        .request_backfill("customers")
        .await
        .expect_err("a table the role holds no privilege on");
    assert!(
        matches!(
            &err,
            trellis::TrellisError::Catalog(CatalogError::TableNotAccessible {
                table,
                lacks_table_privilege: true,
                lacks_schema_usage: false,
                ..
            }) if table == "public.customers"
        ),
        "{err:?}"
    );
    assert_eq!(err.code(), ErrorCode::Validation, "{err}");

    let err = trellis
        .request_backfill("nothing")
        .await
        .expect_err("a table that doesn't exist");
    assert!(
        matches!(&err, trellis::TrellisError::SourceTableNotFound(table) if table == "nothing"),
        "{err:?}"
    );
}
