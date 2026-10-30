//! Define of a 1-1 JOIN (`FROM a [INNER|LEFT] JOIN b ON ...` with no
//! `GROUP BY`, ADR-0021, epic #1392) behind the dev knob
//! `TRELLIS_DEV_ONE_TO_ONE_JOIN`: the accept/refuse matrix, the catalog state
//! and hidden tables define creates, the lifecycle (pause, resume, drop), and
//! one test per site that would otherwise treat the new key space as a plain
//! 1-1 or an aggregate JOIN without saying so.
//!
//! The `FROM` side's page and the minimal build run (`join_one_to_one_interleavings`
//! covers them); a change of the joined side and a `TRUNCATE` halt the
//! definition rather than run wrong. Every test is stepped by hand: no staging
//! worker runs, and nothing polls for convergence.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio::sync::{Mutex, MutexGuard};
use tokio_postgres::{Client, NoTls};
use trellis::capture::install::{Installed, installed};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::join_one_to_one::set_enabled;
use trellis::defs::{CatalogError, ValidationError, ValueType, install_definition};
use trellis::{Applied, Config, TransformStatus, Trellis, TrellisOptions};

/// The knob is process-wide and these tests run on threads of one process:
/// each takes this for its whole body and sets the state it needs.
static KNOB: Mutex<()> = Mutex::const_new(());

async fn with_knob(on: bool) -> MutexGuard<'static, ()> {
    let guard = KNOB.lock().await;
    set_enabled(on);
    guard
}

async fn batch(pool: &trellis::pool::Pool, sql: &str) {
    pool.get()
        .await
        .expect("get connection")
        .batch_execute(sql)
        .await
        .expect("set up tables");
}

async fn define_err(pool: &trellis::pool::Pool, text: &str) -> CatalogError {
    let source_columns: HashMap<String, ValueType> = HashMap::new();
    install_definition(pool, text, &source_columns, "public")
        .await
        .expect_err("this 1-1 JOIN definition is refused")
}

async fn assert_nothing_registered(pool: &trellis::pool::Pool, target: &str) {
    let client = pool.get().await.expect("get connection");
    let exists: bool = client
        .query_one(
            "select pg_catalog.to_regclass($1) is not null",
            &[&format!("public.{target}")],
        )
        .await
        .expect("probe target")
        .get(0);
    assert!(!exists, "a refused definition must not leave its target");
    let definitions: i64 = client
        .query_one(
            "select count(*) from transform_definitions where split_part(target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .expect("count definitions")
        .get(0);
    assert_eq!(definitions, 0, "a refused definition must not be stored");
}

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

async fn status(trellis: &Trellis, target: &str) -> Option<TransformStatus> {
    trellis
        .status(target)
        .await
        .expect("status")
        .map(|status| status.status)
}

async fn definition_id(raw: &Client, target: &str) -> i64 {
    raw.query_one(
        "select id from transform_definitions where split_part(target_table, '.', 2) = $1",
        &[&target],
    )
    .await
    .expect("read the definition's id")
    .get(0)
}

/// The definition's 1-1 JOIN hidden tables in the catalog schema, by name.
async fn hidden_tables(raw: &Client, id: i64) -> Vec<String> {
    raw.query(
        "select c.relname::text from pg_catalog.pg_class c \
         join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
         where n.nspname = $1 and c.relname like $2 and c.relkind = 'r' order by 1",
        &[&DEFAULT_SCHEMA, &format!("join1\\_{id}\\_%")],
    )
    .await
    .expect("list hidden tables")
    .into_iter()
    .map(|row| row.get(0))
    .collect()
}

async fn count(raw: &Client, sql: &str) -> i64 {
    raw.query_one(sql, &[]).await.expect("count").get(0)
}

async fn pass(raw: &mut Client, pool: &trellis::Pool) {
    trellis::client::reconcile_pass(raw, pool, DEFAULT_SCHEMA)
        .await
        .expect("reconcile pass");
}

const TABLES: &str = "set search_path to public;
     create table orders (id integer primary key, region text, total numeric(10,2),
                          cust integer, code text);
     create table customers (id integer primary key, name text, tier text, ext integer,
                             a integer, b integer);
     create table loose (id integer primary key, cust integer, name text);
     create unique index customers_ext on customers (ext) include (name);
     create unique index customers_ab on customers (a, b);
     create index on loose (cust);
     insert into customers values (1, 'ann', 'gold', 10, 1, 1), (2, 'bob', 'tin', 20, 1, 2);
     insert into orders values (1, 'emea', 10.00, 1, 'x'), (2, 'apac', 5.50, 2, 'y'),
                               (3, 'emea', 1.25, null, 'z');
     set search_path to trellis, public;";

const LEFT_JOIN: &str = "TRANSFORM order_view FROM orders LEFT JOIN customers \
     ON orders.cust = customers.id \
     SELECT customers.name AS who, orders.total * 2 AS doubled, \
            COALESCE(customers.tier, 'none') AS tier";

#[tokio::test]
async fn the_knob_off_refuses_a_one_to_one_join_at_define_and_leaves_nothing() {
    let _knob = with_knob(false).await;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    batch(&db.pool, TABLES).await;

    let err = define_err(&db.pool, LEFT_JOIN).await;
    assert!(
        matches!(
            err,
            CatalogError::Validate(ValidationError::OneToOneJoinDisabled)
        ),
        "{err:?}"
    );
    assert!(err.to_string().contains("needs a GROUP BY"), "{err}");
    assert_nothing_registered(&db.pool, "order_view").await;

    // The gate is at define, not at parse: the text still parses and loads
    // under the current grammar, so a definition stored while the knob was on
    // does not fail `grammar::load` in a process that has it off.
    let def = trellis::defs::parse(LEFT_JOIN).expect("a 1-1 JOIN parses with the knob off");
    assert!(matches!(
        def.key_space,
        trellis::defs::ast::KeySpace::JoinOneToOne(_)
    ));

    // An aggregate JOIN is unaffected.
    install_definition(
        &db.pool,
        "TRANSFORM by_region FROM orders JOIN customers ON orders.cust = customers.id \
         GROUP BY orders.region WITHOUT SOURCE INDEX SELECT orders.region AS region, \
         COUNT(*) AS n",
        &HashMap::new(),
        "public",
    )
    .await
    .expect("an aggregate JOIN still defines");
}

#[tokio::test]
async fn the_accept_and_refuse_matrix() {
    let _knob = with_knob(true).await;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    batch(&db.pool, TABLES).await;

    // Accepted: `b`'s ON columns are its primary key, a unique index with an
    // INCLUDE column, a composite unique key, or a superset of a unique key;
    // INNER and LEFT; any expression over qualified columns of either side.
    for (target, text) in [
        ("order_view", LEFT_JOIN),
        (
            "on_inner",
            "TRANSFORM on_inner FROM orders INNER JOIN customers ON orders.cust = customers.id \
             SELECT customers.name AS who",
        ),
        (
            "on_bare_join",
            "TRANSFORM on_bare_join FROM orders JOIN customers ON orders.cust = customers.id \
             SELECT customers.name AS who",
        ),
        (
            "on_unique_index",
            "TRANSFORM on_unique_index FROM orders LEFT JOIN customers \
             ON orders.cust = customers.ext SELECT customers.name AS who",
        ),
        (
            "on_composite",
            "TRANSFORM on_composite FROM orders LEFT JOIN customers \
             ON orders.cust = customers.a AND orders.id = customers.b \
             SELECT customers.name AS who",
        ),
        (
            "on_superset",
            "TRANSFORM on_superset FROM orders LEFT JOIN customers \
             ON orders.cust = customers.id AND orders.code = customers.name \
             SELECT customers.tier AS tier",
        ),
        (
            "mixed_fields",
            "TRANSFORM mixed_fields FROM orders LEFT JOIN customers \
             ON orders.cust = customers.id \
             SELECT CASE WHEN customers.tier = 'gold' THEN orders.total ELSE 0 END AS gold_total, \
                    orders.region AS region, COALESCE(customers.name, 'nobody') AS shout",
        ),
    ] {
        let definition = install_definition(&db.pool, text, &HashMap::new(), "public")
            .await
            .unwrap_or_else(|err| panic!("{text}: {err:?}"));
        assert_eq!(definition.target_table, format!("public.{target}"));
        assert_eq!(definition.status, TransformStatus::WaitingToBackfill);
    }

    let unique = |err: &CatalogError| {
        matches!(
            err,
            CatalogError::Validate(ValidationError::OneToOneJoinKeyNotUnique { .. })
        )
    };
    let on = |on: &str| {
        format!(
            "TRANSFORM refused FROM orders LEFT JOIN customers ON {on} \
             SELECT customers.name AS who"
        )
    };

    // `b`'s ON columns are not unique: a plain column, one column of a
    // composite unique key (also under an invalid unique index), and a
    // partial or expression unique index.
    batch(
        &db.pool,
        "create unique index customers_partial on customers (tier) where tier = 'gold';
         create unique index customers_expr on customers (lower(name));",
    )
    .await;
    // An invalid unique index (a concurrent build that failed on the
    // duplicates of `a`) guarantees nothing either.
    db.pool
        .get()
        .await
        .expect("get connection")
        .batch_execute("create unique index concurrently customers_a_invalid on customers (a)")
        .await
        .expect_err("customers.a holds duplicates");
    for text in [
        on("orders.region = customers.tier"),
        on("orders.cust = customers.a"),
        on("orders.region = customers.name"),
    ] {
        let err = define_err(&db.pool, &text).await;
        assert!(unique(&err), "{text}: {err:?}");
        assert!(err.to_string().contains("unique"), "{err}");
    }
    assert_nothing_registered(&db.pool, "refused").await;

    // Fields: aggregates (including the deferred families, which parse so
    // the validator can name them), unqualified and unknown columns.
    let field = |select: &str| {
        format!(
            "TRANSFORM refused FROM orders LEFT JOIN customers ON orders.cust = customers.id \
             SELECT {select}"
        )
    };
    // An ordinary aggregate is refused at parse, as it is in any key space
    // without a GROUP BY.
    let err = define_err(
        &db.pool,
        &field("customers.name AS who, SUM(orders.total) AS s"),
    )
    .await;
    assert!(
        matches!(&err, CatalogError::Parse(parse) if parse.to_string().contains("GROUP BY")),
        "{err:?}"
    );
    let err = define_err(&db.pool, &field("MAX(customers.ext) AS m")).await;
    assert!(
        matches!(
            err,
            CatalogError::Validate(ValidationError::OneToOneJoinAggregate { .. })
        ),
        "{err:?}"
    );
    let err = define_err(&db.pool, &field("total AS t")).await;
    assert!(
        matches!(
            err,
            CatalogError::Validate(ValidationError::JoinUnqualifiedColumn { .. })
        ),
        "{err:?}"
    );
    let err = define_err(&db.pool, &field("elsewhere.x AS t")).await;
    assert!(
        matches!(
            err,
            CatalogError::Validate(ValidationError::JoinUnknownTable { .. })
        ),
        "{err:?}"
    );
    let err = define_err(&db.pool, &field("customers.nope AS t")).await;
    assert!(
        matches!(
            err,
            CatalogError::Validate(ValidationError::JoinUnresolvedColumn { .. })
        ),
        "{err:?}"
    );
    // A field named after a primary key column of `a` would duplicate the
    // target's key column.
    let err = define_err(&db.pool, &field("customers.name AS id")).await;
    assert!(
        matches!(
            err,
            CatalogError::Validate(ValidationError::FieldNamedAfterPrimaryKey { .. })
        ),
        "{err:?}"
    );
    // A column named by two ON terms: the joined side's (`id`), the FROM
    // side's (`cust`), each a typed refusal naming the column and not the
    // raw error of the hidden-table DDL.
    for (text_on, column) in [
        (
            "orders.cust = customers.id AND orders.id = customers.id",
            "customers.id",
        ),
        (
            "orders.cust = customers.id AND orders.cust = customers.ext",
            "orders.cust",
        ),
    ] {
        let err = define_err(&db.pool, &on(text_on)).await;
        assert!(
            matches!(
                &err,
                CatalogError::Validate(ValidationError::JoinOnRepeatedColumn { column: named, .. })
                    if named == column
            ),
            "{text_on}: {err:?}"
        );
        assert!(err.to_string().contains(column), "{err}");
    }
    // A repeated pair keeps its own refusal.
    let err = define_err(
        &db.pool,
        &on("orders.cust = customers.id AND customers.id = orders.cust"),
    )
    .await;
    assert!(
        matches!(
            err,
            CatalogError::Validate(ValidationError::JoinOnRepeatedPair { .. })
        ),
        "{err:?}"
    );
    // The ON clause follows the aggregate JOIN's rules: a type mismatch.
    let err = define_err(&db.pool, &on("orders.code = customers.id")).await;
    assert!(
        matches!(
            err,
            CatalogError::Validate(ValidationError::JoinKeyTypeMismatch { .. })
        ),
        "{err:?}"
    );
    // `WITHOUT SOURCE INDEX` names an index a 1-1 JOIN never needs.
    let err = define_err(
        &db.pool,
        &format!("{} WITHOUT SOURCE INDEX", on("orders.cust = customers.id")).replace(
            "SELECT customers.name AS who WITHOUT SOURCE INDEX",
            "WITHOUT SOURCE INDEX SELECT customers.name AS who",
        ),
    )
    .await;
    assert!(matches!(err, CatalogError::Parse(_)), "{err:?}");
    assert_nothing_registered(&db.pool, "refused").await;
}

#[tokio::test]
async fn a_partitioned_side_is_refused() {
    let _knob = with_knob(true).await;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    batch(
        &db.pool,
        "create table facts (id integer, day date, cust integer, primary key (id, day))
             partition by range (day);
         create table facts_1 partition of facts for values from ('2026-01-01') to ('2027-01-01');
         create table dims (id integer primary key, d date, name text);
         create table plain_dims (id integer primary key, name text);",
    )
    .await;
    // `a` partitioned, and `b` partitioned.
    for text in [
        "TRANSFORM v FROM facts LEFT JOIN dims ON facts.cust = dims.id SELECT dims.name AS n",
        "TRANSFORM v FROM dims LEFT JOIN facts ON dims.id = facts.id AND dims.d = facts.day \
         SELECT facts.cust AS c",
    ] {
        let err = define_err(&db.pool, text).await;
        assert!(
            matches!(
                err,
                CatalogError::PartitionedSource(
                    trellis::defs::partition_tree::PartitionRefusal::OneToOneJoinSide { .. }
                )
            ),
            "{text}: {err:?}"
        );
        assert!(err.to_string().contains("1-1 JOIN"), "{err}");
    }
    assert_nothing_registered(&db.pool, "v").await;
}

/// Marks `target` live without a build, the way the other define tests do for
/// a table a later definition reads.
async fn make_live(raw: &Client, target: &str) {
    raw.execute(
        "update transform_definitions set status = 'live' \
         where split_part(target_table, '.', 2) = $1",
        &[&target],
    )
    .await
    .expect("mark live");
}

#[tokio::test]
async fn a_one_to_one_target_side_needs_its_primary_key_and_no_transform_reads_a_one_to_one_join_target()
 {
    let _knob = with_knob(true).await;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(TABLES).await.expect("seed");

    // A plain 1-1 target of `customers`, with a unique key that is not its
    // primary key.
    let copy = install_definition(
        &db.pool,
        "TRANSFORM cust_copy FROM customers SELECT ext AS ext, name AS name",
        &HashMap::from([
            (
                "ext".to_string(),
                ValueType::Integer(trellis::dev::defs::IntWidth::Int4),
            ),
            ("name".to_string(), ValueType::Text),
        ]),
        "public",
    )
    .await
    .expect("define a plain 1-1 target");
    make_live(&raw, "cust_copy").await;
    raw.batch_execute(&format!(
        "create unique index on {} (ext)",
        copy.target_table
    ))
    .await
    .expect("unique index on the copy");

    // Joined on its primary key `id`: accepted. On another unique column: the
    // 1-1 target is a `b` side joined off its primary key, which is refused.
    install_definition(
        &db.pool,
        "TRANSFORM by_pk FROM orders LEFT JOIN cust_copy ON orders.cust = cust_copy.id \
         SELECT cust_copy.name AS who",
        &HashMap::new(),
        "public",
    )
    .await
    .expect("a 1-1 target joined on its primary key");
    let err = define_err(
        &db.pool,
        "TRANSFORM by_ext FROM orders LEFT JOIN cust_copy ON orders.cust = cust_copy.ext \
         SELECT cust_copy.name AS who",
    )
    .await;
    assert!(
        matches!(
            err,
            CatalogError::Validate(ValidationError::OneToOneJoinUnsupported { .. })
        ),
        "{err:?}"
    );
    assert!(err.to_string().contains("primary key"), "{err}");

    // Nothing reads a 1-1 JOIN's target, whatever the reader's key space.
    make_live(&raw, "by_pk").await;
    let columns = HashMap::from([
        (
            "id".to_string(),
            ValueType::Integer(trellis::dev::defs::IntWidth::Int4),
        ),
        ("who".to_string(), ValueType::Text),
    ]);
    for text in [
        "TRANSFORM rd1 FROM by_pk SELECT who AS w",
        "TRANSFORM rd2 FROM by_pk GROUP BY who SELECT who AS who, COUNT(*) AS n",
        "TRANSFORM rd3 FROM customers JOIN by_pk ON customers.id = by_pk.id \
         GROUP BY customers.tier SELECT customers.tier AS tier, COUNT(*) AS n",
        "TRANSFORM rd4 FROM customers LEFT JOIN by_pk ON customers.id = by_pk.id \
         SELECT by_pk.who AS w",
    ] {
        let err = install_definition(&db.pool, text, &columns, "public")
            .await
            .expect_err("a reader of a 1-1 JOIN target is refused");
        assert!(
            matches!(
                err,
                CatalogError::Validate(ValidationError::OneToOneJoinTargetRead { .. })
            ),
            "{text}: {err:?}"
        );
        assert!(err.to_string().contains("1-1 JOIN"), "{err}");
    }
}

/// A chained `a` side may be joined on any columns, since its `ON` columns
/// carry no uniqueness; a chained `b` side only on columns that include every
/// primary-key column of that table, however many unique indexes it has.
#[tokio::test]
async fn a_one_to_one_target_is_an_a_side_on_any_columns_and_a_b_side_on_columns_containing_its_primary_key()
 {
    let _knob = with_knob(true).await;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(TABLES).await.expect("seed");
    raw.batch_execute(
        "set search_path to public;
         create table pairs (x integer, y integer, v text, primary key (x, y));
         set search_path to trellis, public;",
    )
    .await
    .expect("composite-key table");

    let copy = install_definition(
        &db.pool,
        "TRANSFORM cust_copy FROM customers SELECT ext AS ext, name AS name",
        &HashMap::from([
            (
                "ext".to_string(),
                ValueType::Integer(trellis::dev::defs::IntWidth::Int4),
            ),
            ("name".to_string(), ValueType::Text),
        ]),
        "public",
    )
    .await
    .expect("define a plain 1-1 target");
    make_live(&raw, "cust_copy").await;
    let pairs_copy = install_definition(
        &db.pool,
        "TRANSFORM pairs_copy FROM pairs SELECT v AS v",
        &HashMap::from([("v".to_string(), ValueType::Text)]),
        "public",
    )
    .await
    .expect("define a plain 1-1 target with a composite key");
    make_live(&raw, "pairs_copy").await;
    // Unique on columns that are not (all of) the primary key: they pass the
    // uniqueness rule, so only the primary-key rule decides.
    raw.batch_execute(&format!(
        "create unique index on {} (ext);
         create unique index on {} (x)",
        copy.target_table, pairs_copy.target_table
    ))
    .await
    .expect("user-added unique indexes");

    // The `a` side: no rule on its `ON` columns, so a non-key one is fine.
    install_definition(
        &db.pool,
        "TRANSFORM a_side FROM cust_copy LEFT JOIN customers ON cust_copy.ext = customers.ext \
         SELECT customers.tier AS tier",
        &HashMap::new(),
        "public",
    )
    .await
    .expect("a 1-1 target as the a side, joined on a non-key column");

    // The `b` side: its primary key plus another column is accepted.
    install_definition(
        &db.pool,
        "TRANSFORM b_superset FROM orders LEFT JOIN cust_copy \
         ON orders.cust = cust_copy.id AND orders.code = cust_copy.name \
         SELECT cust_copy.name AS who",
        &HashMap::new(),
        "public",
    )
    .await
    .expect("a 1-1 target as the b side, joined on its primary key and another column");
    install_definition(
        &db.pool,
        "TRANSFORM b_composite FROM orders LEFT JOIN pairs_copy \
         ON orders.id = pairs_copy.y AND orders.cust = pairs_copy.x \
         SELECT pairs_copy.v AS v",
        &HashMap::new(),
        "public",
    )
    .await
    .expect("a 1-1 target as the b side, joined on its whole composite key in another order");

    // Not on a column that is unique only through an index a user added, and
    // not on part of a composite primary key.
    for text in [
        "TRANSFORM b_uidx FROM orders LEFT JOIN cust_copy ON orders.cust = cust_copy.ext \
         SELECT cust_copy.name AS who",
        "TRANSFORM b_subset FROM orders LEFT JOIN pairs_copy ON orders.cust = pairs_copy.x \
         SELECT pairs_copy.v AS v",
    ] {
        let err = define_err(&db.pool, text).await;
        assert!(
            matches!(
                err,
                CatalogError::Validate(ValidationError::OneToOneJoinUnsupported { .. })
            ),
            "{text}: {err:?}"
        );
        assert!(err.to_string().contains("primary key"), "{err}");
    }
    assert_nothing_registered(&db.pool, "b_uidx").await;
    assert_nothing_registered(&db.pool, "b_subset").await;
}

#[tokio::test]
async fn define_registers_both_tables_the_target_and_the_hidden_tables() {
    let _knob = with_knob(true).await;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;
    raw.batch_execute(TABLES).await.expect("seed");
    let trellis = define_only(db.dsn()).await;

    let applied = trellis.apply(LEFT_JOIN).await.expect("define the 1-1 JOIN");
    assert!(
        applied
            .into_transform()
            .expect("a transform is defined")
            .ends_with(".order_view")
    );
    assert_eq!(
        status(&trellis, "order_view").await,
        Some(TransformStatus::WaitingToBackfill)
    );
    // Not an aggregate JOIN: its status carries no hidden-table sizes.
    let reported = trellis
        .status("order_view")
        .await
        .expect("status")
        .expect("defined");
    assert!(reported.join.is_none(), "{:?}", reported.join);

    // The catalog row names both tables, and the edges and fences cover both.
    let row = raw
        .query_one(
            "select source_table, join_table, join_columns ->> 'name' from transform_definitions",
            &[],
        )
        .await
        .expect("read the definition");
    assert_eq!(row.get::<_, String>(0), "public.orders");
    assert_eq!(row.get::<_, String>(1), "public.customers");
    assert_eq!(row.get::<_, Option<String>>(2).as_deref(), Some("text"));
    let edges: Vec<(String, String)> = raw
        .query(
            "select f.table_name, e.kind from schema_edges e \
             join schema_nodes f on f.id = e.from_node_id order by e.kind",
            &[],
        )
        .await
        .expect("read edges")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        edges,
        [
            ("public.customers".to_string(), "join".to_string()),
            ("public.orders".to_string(), "source".to_string()),
        ]
    );
    assert_eq!(
        count(
            &raw,
            "select count(*) from source_table_versions \
             where source_table in ('public.orders', 'public.customers') and version = 1"
        )
        .await,
        2
    );

    // The target is keyed by `a`'s primary key, holds the fields in the types
    // a copy and an expression have, and has no ledger of its own.
    let columns: Vec<(String, String)> = raw
        .query(
            "select attname::text, pg_catalog.format_type(atttypid, atttypmod) \
             from pg_catalog.pg_attribute \
             where attrelid = 'public.order_view'::regclass and attnum > 0 order by attnum",
            &[],
        )
        .await
        .expect("read the target's columns")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        columns,
        [
            ("id", "integer"),
            ("who", "text"),
            ("doubled", "numeric"),
            ("tier", "text"),
        ]
        .map(|(c, t)| (c.to_string(), t.to_string()))
    );
    assert_eq!(
        count(
            &raw,
            "select count(*) from pg_index where indrelid = 'public.order_view'::regclass \
             and indisprimary"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &raw,
            "select count(*) from pg_class where relname = 'order_view__ledger'"
        )
        .await,
        0
    );

    // The hidden tables sit in the catalog schema, named by the definition's
    // id, and are empty.
    let id = definition_id(&raw, "order_view").await;
    assert_eq!(
        hidden_tables(&raw, id).await,
        [
            format!("join1_{id}_j"),
            format!("join1_{id}_l"),
            format!("join1_{id}_m"),
            format!("join1_{id}_r"),
        ]
    );
    for part in ["r", "m"] {
        let table = format!("{DEFAULT_SCHEMA}.join1_{id}_{part}");
        let columns: Vec<String> = raw
            .query(
                "select attname::text from pg_catalog.pg_attribute \
                 where attrelid = $1::text::regclass and attnum > 0 order by attnum",
                &[&table],
            )
            .await
            .expect("read the ledger columns")
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        // The ledger columns, then the join key, then the columns the fields
        // read.
        assert_eq!(
            &columns[..5],
            [
                "__from_key",
                "__applied_lsn",
                "__applied_seg",
                "__basis",
                "__tombstone"
            ]
        );
        let expected: &[&str] = if part == "r" {
            &["cust", "total"]
        } else {
            &["id", "name", "tier"]
        };
        assert_eq!(&columns[5..], expected, "{table}");
        // The join key index, and the partial tombstone index the collection
        // walks (without it the collection silently skips the table).
        let indexes: Vec<String> = raw
            .query(
                "select pg_get_indexdef(indexrelid) from pg_index \
                 where indrelid = $1::text::regclass order by 1",
                &[&table],
            )
            .await
            .expect("read indexes")
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        assert!(
            indexes
                .iter()
                .any(|i| i.contains("__applied_seg") && i.contains("WHERE __tombstone")),
            "{table}: {indexes:?}"
        );
        let key = if part == "r" { "(cust)" } else { "(id)" };
        assert!(
            indexes.iter().any(|i| i.ends_with(key)),
            "{table}: {indexes:?}"
        );
    }
    assert_eq!(
        count(
            &raw,
            &format!("select count(*) from {DEFAULT_SCHEMA}.join1_{id}_l")
        )
        .await,
        1,
        "the pair's truncate lock row"
    );

    // Define hands capture both tables with the columns the fields read.
    pass(&mut raw, &db.pool).await;
    for (table, columns) in [
        ("public.orders", vec!["id", "cust", "total"]),
        ("public.customers", vec!["id", "name", "tier"]),
    ] {
        match installed(&raw, DEFAULT_SCHEMA, table)
            .await
            .expect("read the capture")
        {
            Installed::Complete { spec, .. } => {
                for column in columns {
                    assert!(
                        spec.columns().iter().any(|c| c == column)
                            || spec.key().iter().any(|c| c == column),
                        "{table} images {column}: {spec:?}"
                    );
                }
            }
            other => panic!("{table} should be captured: {other:?}"),
        }
    }
}

#[tokio::test]
async fn a_build_start_runs_the_minimal_build_of_a_one_to_one_join() {
    let _knob = with_knob(true).await;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;
    raw.batch_execute(TABLES).await.expect("seed");
    let trellis = define_only(db.dsn()).await;
    trellis.apply(LEFT_JOIN).await.expect("define the 1-1 JOIN");

    // The first pass installs both captures; the second finds the build ready
    // and starts it.
    pass(&mut raw, &db.pool).await;
    pass(&mut raw, &db.pool).await;
    assert_eq!(
        status(&trellis, "order_view").await,
        Some(TransformStatus::Backfilling)
    );
    // The job fills the hidden tables and the target in one transaction and
    // the definition goes live.
    trellis::staging::build::settle_builds(&db.pool).await;
    assert_eq!(
        status(&trellis, "order_view").await,
        Some(TransformStatus::Live)
    );
    assert_eq!(
        raw.query(
            "select id, who, doubled::text, tier from public.order_view order by id",
            &[]
        )
        .await
        .expect("the target")
        .iter()
        .map(|row| (
            row.get::<_, i32>(0),
            row.get::<_, Option<String>>(1),
            row.get::<_, String>(2),
            row.get::<_, String>(3)
        ))
        .collect::<Vec<_>>(),
        vec![
            (
                1,
                Some("ann".to_string()),
                "20.00".to_string(),
                "gold".to_string()
            ),
            (
                2,
                Some("bob".to_string()),
                "11.00".to_string(),
                "tin".to_string()
            ),
            (3, None, "2.50".to_string(), "none".to_string()),
        ],
        "a LEFT JOIN keeps the order with no customer"
    );

    // `self_check` doesn't compare a 1-1 JOIN yet, and says so.
    let err = trellis
        .self_check(
            "order_view",
            trellis::SelfCheckMode::Standard,
            std::time::Duration::from_secs(5),
        )
        .await
        .expect_err("a 1-1 JOIN isn't compared yet");
    assert!(err.to_string().contains("1-1 JOIN"), "{err}");
}

#[tokio::test]
async fn a_one_to_one_join_pauses_resumes_revalidates_and_drops_with_its_hidden_tables() {
    let _knob = with_knob(true).await;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(TABLES).await.expect("seed");
    let trellis = define_only(db.dsn()).await;
    trellis
        .apply(
            "TRANSFORM order_view FROM orders LEFT JOIN customers ON orders.cust = customers.ext \
             SELECT customers.name AS who",
        )
        .await
        .expect("define the 1-1 JOIN");
    let id = definition_id(&raw, "order_view").await;

    assert!(matches!(
        trellis
            .apply("PAUSE TRANSFORM order_view")
            .await
            .expect("pause"),
        Applied::Paused
    ));

    // A resume drops and recreates the hidden tables: whatever was in them
    // is gone.
    raw.batch_execute(&format!(
        "insert into {DEFAULT_SCHEMA}.join1_{id}_j (cust) values (1);
         insert into {DEFAULT_SCHEMA}.join1_{id}_r (__from_key, cust) values ('1', 1)"
    ))
    .await
    .expect("write hidden rows");
    trellis
        .apply("RESUME TRANSFORM order_view")
        .await
        .expect("resume");
    assert_eq!(
        status(&trellis, "order_view").await,
        Some(TransformStatus::WaitingToBackfill)
    );
    for part in ["j", "r"] {
        assert_eq!(
            count(
                &raw,
                &format!("select count(*) from {DEFAULT_SCHEMA}.join1_{id}_{part}")
            )
            .await,
            0
        );
    }
    assert_eq!(hidden_tables(&raw, id).await.len(), 4);

    // A resume revalidates `b`'s uniqueness: lose the unique index the ON
    // column rested on and the resume refuses, naming the table.
    trellis
        .apply("PAUSE TRANSFORM order_view")
        .await
        .expect("pause");
    raw.batch_execute("drop index public.customers_ext")
        .await
        .expect("lose uniqueness");
    let err = trellis
        .apply("RESUME TRANSFORM order_view")
        .await
        .expect_err("the resume revalidates");
    assert!(err.to_string().contains("unique"), "{err}");
    assert!(err.to_string().contains("customers"), "{err}");
    raw.batch_execute("create unique index customers_ext on public.customers (ext)")
        .await
        .expect("restore uniqueness");
    trellis
        .apply("RESUME TRANSFORM order_view")
        .await
        .expect("resume once unique again");
    trellis
        .apply("PAUSE TRANSFORM order_view")
        .await
        .expect("pause");

    // The target is keyed by `a`'s key as define found it: a redefined key
    // refuses the resume, as for a plain 1-1 target.
    raw.batch_execute(
        "alter table public.orders drop constraint orders_pkey, add primary key (id, region)",
    )
    .await
    .expect("redefine a's key");
    let err = trellis
        .apply("RESUME TRANSFORM order_view")
        .await
        .expect_err("the resume checks a's key");
    assert!(err.to_string().contains("orders"), "{err}");
    assert!(err.to_string().contains("region"), "{err}");
    raw.batch_execute(
        "alter table public.orders drop constraint orders_pkey, add primary key (id)",
    )
    .await
    .expect("restore a's key");

    // DROP takes the target, the hidden tables, the floors of `R` and `M`,
    // the row and its edges.
    raw.batch_execute(&format!(
        "insert into ledger_truncate_floor (target_table, floor) values
            ('\"{DEFAULT_SCHEMA}\".\"join1_{id}_r\"', '0/10'),
            ('\"{DEFAULT_SCHEMA}\".\"join1_{id}_m\"', '0/10')"
    ))
    .await
    .expect("write floors");
    assert!(matches!(
        trellis
            .apply("DROP TRANSFORM order_view")
            .await
            .expect("drop"),
        Applied::Dropped
    ));
    assert_eq!(status(&trellis, "order_view").await, None);
    assert!(hidden_tables(&raw, id).await.is_empty());
    assert_eq!(
        count(&raw, "select count(*) from ledger_truncate_floor").await,
        0
    );
    assert_eq!(
        count(
            &raw,
            "select count(*) from pg_catalog.pg_class where relname = 'order_view'"
        )
        .await,
        0
    );
    assert_eq!(count(&raw, "select count(*) from schema_edges").await, 0);
    assert_eq!(
        count(&raw, "select count(*) from transform_definitions").await,
        0
    );
}

/// Seals the ring and drains every claimable segment by hand, until nothing
/// is pending.
async fn drain(raw: &mut Client, pool: &trellis::Pool) {
    const WAKE: &str = "one_to_one_join_wake";
    for _ in 0..16 {
        trellis::staging::seal_if_active_nonempty(raw, WAKE)
            .await
            .expect("seal");
        while let Some(seg) = trellis::staging::apply::next_claimable_segment(raw)
            .await
            .expect("next claimable segment")
        {
            trellis::staging::apply::drain_once(pool, seg, "drainer", 1, WAKE)
                .await
                .expect("drain_once");
        }
        trellis::staging::retire_drained_segments(raw)
            .await
            .expect("retire drained segments");
        if !trellis::staging::has_pending(raw)
            .await
            .expect("has_pending")
        {
            return;
        }
    }
    panic!("the ring did not quiesce within 16 seal/drain rounds");
}

/// A change to `a` is applied by the 1-1 JOIN's page, and a change to `b` by
/// its fan-out, rather than either applying as a plain 1-1 change (a target
/// row computed from the one table alone). A `TRUNCATE` of either side halts
/// it with the reason rather than clearing nothing: that is a later
/// milestone's.
#[tokio::test]
async fn a_change_of_either_side_applies_and_a_truncate_halts_a_one_to_one_join() {
    let _knob = with_knob(true).await;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;
    raw.batch_execute(TABLES).await.expect("seed");
    let trellis = define_only(db.dsn()).await;
    trellis.apply(LEFT_JOIN).await.expect("define the 1-1 JOIN");

    // The passes install both captures and start the build, which fills the
    // target.
    pass(&mut raw, &db.pool).await;
    pass(&mut raw, &db.pool).await;
    trellis::staging::build::settle_builds(&db.pool).await;
    assert_eq!(
        status(&trellis, "order_view").await,
        Some(TransformStatus::Live)
    );

    // A change of `a` applies.
    raw.batch_execute("insert into public.orders values (4, 'emea', 3.00, 1, 'w')")
        .await
        .expect("change a");
    drain(&mut raw, &db.pool).await;
    assert_eq!(
        status(&trellis, "order_view").await,
        Some(TransformStatus::Live)
    );
    assert_eq!(
        count(&raw, "select count(*) from public.order_view").await,
        4,
        "the new order reached the target"
    );
    // So does a change of `b`, which fans out to its orders.
    raw.batch_execute("update public.customers set name = 'bea' where id = 2")
        .await
        .expect("change b");
    drain(&mut raw, &db.pool).await;
    assert_eq!(
        status(&trellis, "order_view").await,
        Some(TransformStatus::Live)
    );
    assert_eq!(
        count(
            &raw,
            "select count(*) from public.order_view where id = 2 and who = 'bea'"
        )
        .await,
        1,
        "the customer's new name reached its order"
    );
    let diverging = trellis::staging::one_to_one_join_diverging_keys(&db.pool, "public.order_view")
        .await
        .expect("self_check");
    assert!(diverging.is_empty(), "{diverging:?}");
    for (change, reason) in [("truncate public.customers", "a TRUNCATE of a 1-1 JOIN side")] {
        raw.batch_execute(change).await.expect("change a side");
        drain(&mut raw, &db.pool).await;
        let reported = trellis
            .status("order_view")
            .await
            .expect("status")
            .expect("defined");
        assert_eq!(reported.status, TransformStatus::Paused, "{change}");
        let failure = reported.capture_failure.expect("a reason for the pause");
        assert!(failure.error.contains(reason), "{change}: {failure:?}");
        assert_eq!(
            count(&raw, "select count(*) from public.order_view").await,
            4,
            "nothing was written to the target"
        );
        make_live(&raw, "order_view").await;
        raw.execute("delete from capture_failures", &[])
            .await
            .expect("clear the pause record");
    }
}

/// A restored 1-1 JOIN's `RESUME` recreates `R` and `M` (empty, no basis)
/// and `J`, keeps the target's rows, and drops the truncate floors the dump
/// carried. The reset of the ledgers' ordering columns is not needed on top
/// of the recreate.
#[tokio::test]
async fn a_restored_one_to_one_joins_resume_recreates_its_ledgers_and_keeps_the_target() {
    let _knob = with_knob(true).await;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(TABLES).await.expect("seed");
    let trellis = define_only(db.dsn()).await;
    trellis.apply(LEFT_JOIN).await.expect("define the 1-1 JOIN");
    let id = definition_id(&raw, "order_view").await;
    trellis
        .apply("PAUSE TRANSFORM order_view")
        .await
        .expect("pause");
    // The dump's state: ledgers holding the original database's positions,
    // floors for the target and the hidden tables, rows in the target.
    let schema = DEFAULT_SCHEMA;
    for side in ["r", "m"] {
        raw.batch_execute(&format!(
            "insert into {schema}.join1_{id}_{side} (__from_key, __applied_lsn, __basis) \
                 values ('1', '0/10', '10:20:');
             insert into ledger_truncate_floor values \
                 ('\"{schema}\".\"join1_{id}_{side}\"', '0/10')"
        ))
        .await
        .expect("restored hidden ledger");
    }
    raw.batch_execute(
        "insert into ledger_truncate_floor values ('public.order_view', '0/10'), \
             ('public.other', '0/10');
         insert into public.order_view (id) values (1)",
    )
    .await
    .expect("a target row");
    raw.batch_execute(
        "insert into capture_failures (transform_id, source_table, columns, error, kind, restored) \
         select id, 'public.orders', '{}', 'restored', 'capture', true \
         from transform_definitions where split_part(target_table, '.', 2) = 'order_view' \
         on conflict (transform_id) do update set restored = true",
    )
    .await
    .expect("mark restored");
    assert_eq!(
        count(&raw, "select count(*) from public.order_view").await,
        1
    );

    trellis
        .apply("RESUME TRANSFORM order_view")
        .await
        .expect("resume");

    for side in ["r", "m"] {
        assert_eq!(
            count(
                &raw,
                &format!("select count(*) from {schema}.join1_{id}_{side}")
            )
            .await,
            0,
            "{side} is recreated empty"
        );
    }
    assert_eq!(
        count(&raw, "select count(*) from public.order_view").await,
        1,
        "the target keeps its rows"
    );
    let floors: Vec<String> = raw
        .query("select target_table from ledger_truncate_floor", &[])
        .await
        .expect("floors")
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(floors, ["public.other"], "the restored floors are dropped");
}

/// A `RESUME` of a 1-1 JOIN waits out a chunk that still holds its claim row
/// before it recreates the hidden tables (as for a group-basis aggregate's
/// partials), so it never holds `ACCESS EXCLUSIVE` on them while it waits
/// with the definition row: a chunk writing them, which holds the claim row
/// `for key share`, would otherwise queue behind the resume, which queues
/// behind the chunk. Here the chunk's session takes a lock on `R` while the
/// resume is queued on the claim row: it gets it only if the resume hasn't
/// yet dropped `R`.
#[tokio::test]
async fn a_resume_waits_out_an_in_flight_chunk_before_recreating_the_hidden_tables() {
    let _knob = with_knob(true).await;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(TABLES).await.expect("seed");
    let trellis = define_only(db.dsn()).await;
    trellis.apply(LEFT_JOIN).await.expect("define the 1-1 JOIN");
    let id = definition_id(&raw, "order_view").await;
    trellis
        .apply("PAUSE TRANSFORM order_view")
        .await
        .expect("pause");
    let chunk: i64 = raw
        .query_one(
            "insert into backfill_chunks (definition_id, kind, lo, hi, claimed_by, claimed_at, \
                                          fuse_rearmed_at) \
             select id, 'rederive', null, '9', 'held', now(), fuse_rearmed_at \
             from transform_definitions where id = $1 returning id",
            &[&id],
        )
        .await
        .expect("a claimed chunk")
        .get(0);
    // The chunk's session, mid-write: its claim row held `for key share`.
    let mut chunk_session = connect_raw(db.dsn()).await;
    let pid: i32 = chunk_session
        .query_one("select pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0);
    let txn = chunk_session.transaction().await.expect("begin");
    txn.execute(
        "select 1 from backfill_chunks where id = $1 for key share",
        &[&chunk],
    )
    .await
    .expect("hold the claim row");

    let resume = tokio::spawn({
        let pool = db.pool.clone();
        async move { trellis::staging::quarantine::resume_transform(&pool, "order_view").await }
    });
    // The resume is queued on the claim row. It reaches the wait in a few
    // statements; the bound only turns a resume that never gets there into
    // a failure rather than a hang.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let queued: bool = raw
            .query_one(
                "select exists (select 1 from pg_stat_activity \
                 where $1 = any(pg_blocking_pids(pid)))",
                &[&pid],
            )
            .await
            .expect("read pg_stat_activity")
            .get(0);
        if queued {
            break;
        }
        assert!(
            !resume.is_finished(),
            "the resume ended without waiting for the chunk: {:?}",
            resume.await
        );
        assert!(
            std::time::Instant::now() < deadline,
            "the resume never queued on the claim row"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    // It has not yet taken `ACCESS EXCLUSIVE` on the hidden ledger.
    txn.batch_execute(&format!(
        "set local lock_timeout = '2s'; \
         lock table {DEFAULT_SCHEMA}.join1_{id}_r in access share mode"
    ))
    .await
    .expect("the resume holds no lock on R while it waits for the chunk");
    txn.commit().await.expect("the chunk's write commits");
    resume.await.expect("task").expect("resume");
    assert_eq!(
        status(&trellis, "order_view").await,
        Some(TransformStatus::WaitingToBackfill)
    );
}

/// No transform reads a 1-1 JOIN target, however it came to: a reader whose
/// source was dropped while it was paused, and whose source's name a 1-1
/// JOIN's target then took, is refused at `RESUME` as at define.
#[tokio::test]
async fn a_resume_refuses_a_reader_whose_source_became_a_one_to_one_join_target() {
    let _knob = with_knob(true).await;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(TABLES).await.expect("seed");
    raw.batch_execute("create table public.shadow (id integer primary key, who text)")
        .await
        .expect("the reader's source");
    let trellis = define_only(db.dsn()).await;
    trellis
        .apply("TRANSFORM shadow_copy FROM shadow SELECT who AS who")
        .await
        .expect("define the reader");
    trellis
        .apply("PAUSE TRANSFORM shadow_copy")
        .await
        .expect("pause the reader");
    raw.batch_execute("drop table public.shadow")
        .await
        .expect("drop the reader's source");
    trellis
        .apply(
            "TRANSFORM shadow FROM orders LEFT JOIN customers ON orders.cust = customers.id \
             SELECT customers.name AS who",
        )
        .await
        .expect("a 1-1 JOIN whose target takes the dropped source's name");
    // Live, as a later milestone's build would leave it; a resume otherwise
    // waits for its upstream to go live, and revalidates then.
    make_live(&raw, "shadow").await;

    let err = trellis
        .apply("RESUME TRANSFORM shadow_copy")
        .await
        .expect_err("the resumed reader would read a 1-1 JOIN target");
    assert!(err.to_string().contains("1-1 JOIN"), "{err}");
    assert_eq!(
        status(&trellis, "shadow_copy").await,
        Some(TransformStatus::Paused)
    );
}

/// A rebuild of a live 1-1 JOIN in place (`request_backfill`, which also
/// starts a rebuild after an uncaptured change) re-derives every entry and
/// turns the ones whose rows are gone into tombstones: the target ends equal
/// to the live join, and the definition goes live again with no sweep job.
#[tokio::test]
async fn a_rebuild_in_place_drops_the_entries_whose_rows_are_gone() {
    let _knob = with_knob(true).await;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;
    raw.batch_execute(TABLES).await.expect("seed");
    let trellis = define_only(db.dsn()).await;
    trellis.apply(LEFT_JOIN).await.expect("define the 1-1 JOIN");
    pass(&mut raw, &db.pool).await;
    pass(&mut raw, &db.pool).await;
    trellis::staging::build::settle_builds(&db.pool).await;
    // Changes capture never saw, on both sides.
    let toggle = |action: &str| {
        format!(
            "do $$ declare t record; begin \
                 for t in select tgrelid::regclass::text as tab, tgname from pg_trigger \
                          where tgrelid in ('public.orders'::regclass, \
                                            'public.customers'::regclass) \
                            and not tgisinternal loop \
                     execute format('alter table %s {action} trigger %I', t.tab, t.tgname); \
                 end loop; \
             end $$;"
        )
    };
    raw.batch_execute(&format!(
        "begin; {} \
         delete from public.orders where id = 3; \
         update public.customers set name = 'zed' where id = 1; \
         delete from public.customers where id = 2; \
         {} commit;",
        toggle("disable"),
        toggle("enable always")
    ))
    .await
    .expect("write without capture");
    trellis
        .request_backfill("public.orders")
        .await
        .expect("request a rebuild");
    assert_eq!(
        status(&trellis, "order_view").await,
        Some(TransformStatus::Backfilling)
    );
    trellis::staging::build::settle_builds(&db.pool).await;
    drain(&mut raw, &db.pool).await;
    assert_eq!(
        status(&trellis, "order_view").await,
        Some(TransformStatus::Live)
    );
    assert_eq!(
        raw.query(
            "select id::text || ' ' || coalesce(who, '-') || ' ' || tier \
             from public.order_view order by id",
            &[]
        )
        .await
        .expect("the target")
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>(),
        ["1 zed gold", "2 - none"],
    );
    let diverging = trellis::staging::one_to_one_join_diverging_keys(&db.pool, "public.order_view")
        .await
        .expect("compare with the live join");
    assert!(diverging.is_empty(), "{diverging:?}");
}
