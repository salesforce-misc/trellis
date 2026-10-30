//! Postgres caps an index at 32 columns (`INDEX_MAX_KEYS`), and Trellis keys
//! its target or hidden tables on a definition's `GROUP BY` columns and on a
//! `JOIN` side's `ON` columns. A definition whose key is wider used to reach
//! `DefineIndex` and fail with Postgres's own `54011`; it is now refused at
//! validation, naming the clause and the limit (#1499).
//!
//! One test per shape of key, each pinning the boundary (32 columns admitted,
//! 33 refused) against a real Postgres, and that a refusal leaves no target
//! and no catalog row behind. The width that counts is the widest index
//! Trellis builds, not the user-visible clause: a `JOIN` side's hidden table is
//! keyed by its `ON` columns plus its `GROUP BY` columns not already among
//! them.
//!
//! Every test is stepped by hand: no staging worker runs.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio::sync::{Mutex, MutexGuard};
use trellis::defs::join_one_to_one::set_enabled;
use trellis::defs::{CatalogError, ValidationError, ValueType, install_definition};
use trellis::integer::IntWidth;

/// The one-to-one `JOIN` knob is process-wide; the test that turns it on holds
/// this for its whole body.
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

/// `prefix0, prefix1, ...` up to (excluding) `n`.
fn names(prefix: &str, n: usize) -> Vec<String> {
    (0..n).map(|i| format!("{prefix}{i}")).collect()
}

/// `table.prefix0, table.prefix1, ...`
fn qualified(table: &str, prefix: &str, n: usize) -> Vec<String> {
    names(prefix, n)
        .into_iter()
        .map(|c| format!("{table}.{c}"))
        .collect()
}

/// `create table <name> (id integer primary key, <prefix>0 integer, ...)`.
fn table_sql(name: &str, extra: &[&str], prefix: &str, n: usize) -> String {
    let mut columns = vec!["id integer primary key".to_string()];
    columns.extend(extra.iter().map(|c| format!("{c} integer")));
    columns.extend(names(prefix, n).into_iter().map(|c| format!("{c} integer")));
    format!("create table public.{name} ({});", columns.join(", "))
}

async fn define(
    pool: &trellis::pool::Pool,
    text: &str,
    source_columns: &HashMap<String, ValueType>,
) -> Result<(), CatalogError> {
    install_definition(pool, text, source_columns, "public")
        .await
        .map(|_| ())
}

async fn nothing_registered(pool: &trellis::pool::Pool, target: &str) {
    let client = pool.get().await.expect("get connection");
    let exists: bool = client
        .query_one(
            "select pg_catalog.to_regclass($1) is not null",
            &[&format!("public.{target}")],
        )
        .await
        .expect("probe target")
        .get(0);
    assert!(
        !exists,
        "{target}: a refused definition must not leave its target"
    );
    let definitions: i64 = client
        .query_one(
            "select count(*) from transform_definitions where split_part(target_table, '.', 2) = $1",
            &[&target],
        )
        .await
        .expect("count definitions")
        .get(0);
    assert_eq!(
        definitions, 0,
        "{target}: a refused definition must not be stored"
    );
}

/// The hidden tables of every registered definition, to prove a refusal added
/// none.
async fn hidden_table_count(pool: &trellis::pool::Pool) -> i64 {
    pool.get()
        .await
        .expect("get connection")
        .query_one(
            "select count(*) from pg_catalog.pg_class c \
             join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
             where n.nspname = 'trellis' and c.relname ~ '^(join1?_[0-9]+_|agg_[0-9]+$)'",
            &[],
        )
        .await
        .expect("count hidden tables")
        .get(0)
}

/// Asserts `result` is the typed refusal for exactly `clause` with `columns`
/// columns, and that its message names the clause and the limit.
fn assert_too_wide<T: std::fmt::Debug>(
    result: Result<T, CatalogError>,
    clause: &str,
    columns: usize,
) {
    let err = result.expect_err("a key wider than an index is refused");
    match &err {
        CatalogError::Validate(ValidationError::IndexKeyTooWide {
            clause: named,
            columns: count,
            limit,
        }) => {
            assert_eq!(named, clause, "{err}");
            assert_eq!((*count, *limit), (columns, 32), "{err}");
        }
        other => panic!("expected IndexKeyTooWide for {clause}, got {other:?}"),
    }
    let message = err.to_string();
    assert!(message.contains(clause), "{message}");
    assert!(message.contains("32"), "{message}");
    assert!(!message.contains("54011"), "{message}");
}

#[tokio::test]
async fn an_aggregate_group_by_wider_than_an_index_is_refused_at_validation() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    batch(&db.pool, &table_sql("w", &[], "c", 40)).await;
    let mut source_columns: HashMap<String, ValueType> = names("c", 40)
        .into_iter()
        .map(|c| (c, ValueType::Integer(IntWidth::Int4)))
        .collect();
    source_columns.insert("id".to_string(), ValueType::Integer(IntWidth::Int4));

    let text = |target: &str, n: usize| {
        let keys = names("c", n);
        let selected: Vec<String> = keys.iter().map(|c| format!("{c} AS {c}")).collect();
        format!(
            "TRANSFORM {target} FROM public.w GROUP BY {} WITHOUT SOURCE INDEX \
             SELECT {}, SUM(id) AS s",
            keys.join(", "),
            selected.join(", ")
        )
    };

    // 32 is the most an index takes.
    define(&db.pool, &text("g32", 32), &source_columns)
        .await
        .expect("a 32-column group key is admitted");
    let hidden = hidden_table_count(&db.pool).await;
    assert_eq!(hidden, 1, "the admitted aggregate's hidden table");

    assert_too_wide(
        define(&db.pool, &text("g33", 33), &source_columns).await,
        "GROUP BY",
        33,
    );
    nothing_registered(&db.pool, "g33").await;
    assert_too_wide(
        define(&db.pool, &text("g40", 40), &source_columns).await,
        "GROUP BY",
        40,
    );
    nothing_registered(&db.pool, "g40").await;
    assert_eq!(hidden_table_count(&db.pool).await, hidden);
}

/// The `ON` and `GROUP BY` of an aggregate `JOIN` over `orders` (`a*`
/// columns) and `items` (`b*` columns, `order_id`).
fn aggregate_join(target: &str, on: &[String], group_by: &[String]) -> String {
    let selected: Vec<String> = group_by
        .iter()
        .map(|key| {
            let column = key.split_once('.').expect("qualified key").1;
            format!("{key} AS {column}")
        })
        .collect();
    format!(
        "TRANSFORM {target} FROM public.orders JOIN public.items ON {} \
         GROUP BY {} WITHOUT SOURCE INDEX SELECT {}, COUNT(*) AS n",
        on.join(" AND "),
        group_by.join(", "),
        selected.join(", ")
    )
}

#[tokio::test]
async fn an_aggregate_join_key_wider_than_an_index_is_refused_at_validation() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    batch(
        &db.pool,
        &format!(
            "{} {}",
            table_sql("orders", &[], "a", 40),
            table_sql("items", &["order_id"], "b", 40)
        ),
    )
    .await;
    let join_on = |n: usize| -> Vec<String> {
        (0..n)
            .map(|i| format!("orders.a{i} = items.b{i}"))
            .collect()
    };
    let key = "orders.id = items.order_id".to_string();

    // GROUP BY: 32 admitted (the FROM side's key is the ON column, which is
    // also a group column, so it counts once); 33 refused whichever sides the
    // columns come from.
    let mut group = qualified("orders", "a", 32);
    let on_a0 = vec!["orders.a0 = items.order_id".to_string()];
    define(
        &db.pool,
        &aggregate_join("jg32", &on_a0, &group),
        &HashMap::new(),
    )
    .await
    .expect("a 32-column group key is admitted");
    let hidden = hidden_table_count(&db.pool).await;

    group.extend(qualified("items", "b", 1));
    assert_too_wide(
        define(
            &db.pool,
            &aggregate_join("jg33", &on_a0, &group),
            &HashMap::new(),
        )
        .await,
        "GROUP BY",
        33,
    );
    nothing_registered(&db.pool, "jg33").await;
    // The same 33 columns split over both sides: each side's hidden table is
    // narrow, the target and `T` are not.
    let mut split = qualified("orders", "a", 17);
    split.extend(qualified("items", "b", 16));
    assert_too_wide(
        define(
            &db.pool,
            &aggregate_join("jg_split", &on_a0, &split),
            &HashMap::new(),
        )
        .await,
        "GROUP BY",
        33,
    );
    nothing_registered(&db.pool, "jg_split").await;

    // A side's hidden table is keyed by its ON columns and its GROUP BY
    // columns together: 32 group columns beside an ON column that isn't one
    // of them is 33, though neither clause is over the limit.
    let a32 = qualified("orders", "a", 32);
    assert_too_wide(
        define(
            &db.pool,
            &aggregate_join("js_from", std::slice::from_ref(&key), &a32),
            &HashMap::new(),
        )
        .await,
        "orders's key, its ON and GROUP BY columns together,",
        33,
    );
    nothing_registered(&db.pool, "js_from").await;
    let b32 = qualified("items", "b", 32);
    assert_too_wide(
        define(
            &db.pool,
            &aggregate_join("js_joined", std::slice::from_ref(&key), &b32),
            &HashMap::new(),
        )
        .await,
        "items's key, its ON and GROUP BY columns together,",
        33,
    );
    nothing_registered(&db.pool, "js_joined").await;
    // The same group columns with the ON column among them: 32, admitted.
    define(
        &db.pool,
        &aggregate_join("js_joined_ok", &["orders.id = items.b0".to_string()], &b32),
        &HashMap::new(),
    )
    .await
    .expect("32 columns in a hidden table's key are admitted");

    // ON: 32 pairs admitted (grouping by one of the ON columns keeps the key
    // at 32); a 33rd pair, or a group column outside the ON columns, is 33.
    define(
        &db.pool,
        &aggregate_join("jo32", &join_on(32), &["orders.a0".to_string()]),
        &HashMap::new(),
    )
    .await
    .expect("a 32-column ON is admitted");
    assert_too_wide(
        define(
            &db.pool,
            &aggregate_join("jo33", &join_on(33), &["orders.a0".to_string()]),
            &HashMap::new(),
        )
        .await,
        "each side of ON",
        33,
    );
    nothing_registered(&db.pool, "jo33").await;
    assert_too_wide(
        define(
            &db.pool,
            &aggregate_join("jo32_g", &join_on(32), &["orders.a39".to_string()]),
            &HashMap::new(),
        )
        .await,
        "orders's key, its ON and GROUP BY columns together,",
        33,
    );
    nothing_registered(&db.pool, "jo32_g").await;

    // Every refusal above left the hidden tables of the admitted
    // definitions alone and added none. (`jg32`, `js_joined_ok` and `jo32`
    // are the three that were admitted; the count was taken after `jg32`.)
    let after = hidden_table_count(&db.pool).await;
    let per_definition = hidden;
    assert_eq!(after, per_definition * 3, "hidden tables of 3 definitions");
}

#[tokio::test]
async fn a_one_to_one_join_key_wider_than_an_index_is_refused_at_validation() {
    let _knob = with_knob(true).await;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    // `fb` is keyed by 32 columns (the most Postgres takes) and has more
    // columns beyond them.
    let pk = names("p", 32);
    batch(
        &db.pool,
        &format!(
            "{} create table public.fb ({}, q0 integer, q1 integer, primary key ({}));",
            table_sql("fa", &[], "f", 40),
            pk.iter()
                .map(|c| format!("{c} integer not null"))
                .collect::<Vec<_>>()
                .join(", "),
            pk.join(", ")
        ),
    )
    .await;
    let text = |target: &str, n: usize| {
        let terms: Vec<String> = (0..n)
            .map(|i| {
                let joined = if i < 32 {
                    format!("p{i}")
                } else {
                    "q0".to_string()
                };
                format!("fa.f{i} = fb.{joined}")
            })
            .collect();
        format!(
            "TRANSFORM {target} FROM public.fa JOIN public.fb ON {} SELECT fb.q1 AS q",
            terms.join(" AND ")
        )
    };

    define(&db.pool, &text("o32", 32), &HashMap::new())
        .await
        .expect("a 32-column ON is admitted");
    let hidden = hidden_table_count(&db.pool).await;
    assert_too_wide(
        define(&db.pool, &text("o33", 33), &HashMap::new()).await,
        "each side of ON",
        33,
    );
    nothing_registered(&db.pool, "o33").await;
    assert_eq!(hidden_table_count(&db.pool).await, hidden);
    set_enabled(false);
}

#[tokio::test]
async fn a_one_to_one_target_key_cannot_exceed_an_index_because_its_source_key_cannot() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    // A source can't have a wider primary key than an index takes, so a 1-1
    // target (keyed as its source is) never does. Postgres refuses the source.
    let wide = names("k", 33);
    let err = db
        .pool
        .get()
        .await
        .expect("get connection")
        .batch_execute(&format!(
            "create table public.too_wide ({}, primary key ({}))",
            wide.iter()
                .map(|c| format!("{c} integer"))
                .collect::<Vec<_>>()
                .join(", "),
            wide.join(", ")
        ))
        .await
        .expect_err("Postgres refuses a 33-column primary key");
    assert_eq!(err.code().map(|c| c.code()), Some("54011"), "{err:?}");

    // A 32-column key is the most there is, and a 1-1 transform keys its
    // target on all 32.
    let pk = names("k", 32);
    batch(
        &db.pool,
        &format!(
            "create table public.wide ({}, v integer, primary key ({}))",
            pk.iter()
                .map(|c| format!("{c} integer"))
                .collect::<Vec<_>>()
                .join(", "),
            pk.join(", ")
        ),
    )
    .await;
    let mut source_columns: HashMap<String, ValueType> = pk
        .iter()
        .map(|c| (c.clone(), ValueType::Integer(IntWidth::Int4)))
        .collect();
    source_columns.insert("v".to_string(), ValueType::Integer(IntWidth::Int4));
    define(
        &db.pool,
        "TRANSFORM wide_copy FROM public.wide SELECT v AS v",
        &source_columns,
    )
    .await
    .expect("a 1-1 transform of a 32-column primary key is admitted");
}
