//! Define of a star 1-1 JOIN (more than one joined table, ADR-0021, #1460
//! sub-step 3.6), behind the development knob `TRELLIS_DEV_STAR_JOIN`: what
//! define creates and `DROP` takes for a star, the refusals by name (the cap
//! of 5 tables, a table read twice, a snowflake, a partitioned table, a
//! position whose `ON` columns aren't unique, an aggregate JOIN of more than
//! one joined table), and the knob's gate at define and `RESUME`.
//!
//! The knob is process-wide, so every test here takes [`serial`] and sets it.
//! No staging worker runs: a star's apply is not built yet.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::{CatalogError, ValidationError, install_definition};
use trellis::dev::defs::set_star_join;
use trellis::{Applied, Config, Trellis, TrellisOptions};

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Takes the binary's knob lock and sets the knob to `on`.
async fn serial(on: bool) -> tokio::sync::MutexGuard<'static, ()> {
    let guard = SERIAL.lock().await;
    set_star_join(on);
    guard
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

async fn define_err(pool: &trellis::pool::Pool, text: &str) -> CatalogError {
    install_definition(pool, text, &HashMap::new(), "public")
        .await
        .expect_err("this star definition is refused")
}

async fn count(raw: &Client, sql: &str) -> i64 {
    raw.query_one(sql, &[]).await.expect("count").get(0)
}

async fn definitions(raw: &Client) -> i64 {
    count(raw, "select count(*) from transform_definitions").await
}

/// Asserts a refused define left nothing: no definition, no joined-table
/// row, no hidden table, no target.
async fn assert_nothing_left(raw: &Client, text: &str) {
    assert_eq!(definitions(raw).await, 0, "{text} left a definition");
    assert_eq!(
        count(raw, "select count(*) from definition_join_tables").await,
        0,
        "{text} left a joined-table row"
    );
    assert_eq!(
        count(
            raw,
            &format!(
                "select count(*) from pg_catalog.pg_class c \
                 join pg_catalog.pg_namespace n on n.oid = c.relnamespace \
                 where (n.nspname = '{DEFAULT_SCHEMA}' and c.relname ~ '^join1_[0-9]+_') \
                    or (n.nspname = 'public' and c.relname in ('v', 'six'))"
            )
        )
        .await,
        0,
        "{text} left a hidden table or a target"
    );
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

/// `a` and five tables to join to it, each unique on `id`, and `n`, unique
/// on nothing but its primary key `id`.
const TABLES: &str = "set search_path to public;
     create table a (id integer primary key, k1 integer, k2 integer, k3 integer, k4 integer,
                     k5 integer, d date, note text);
     create table b1 (id integer primary key, x text);
     create table b2 (id integer primary key, x text);
     create table b3 (id integer primary key, x text);
     create table b4 (id integer primary key, x text);
     create table b5 (id integer primary key, x text);
     create table n (id integer primary key, k integer, x text);
     insert into a values (1, 1, 1, 1, 1, 1, '2026-02-01', 'one');
     insert into b1 values (1, 'b1'); insert into b2 values (1, 'b2');
     insert into b3 values (1, 'b3'); insert into b4 values (1, 'b4');
     set search_path to trellis, public;";

/// A star of `1 + joined` tables: `a` and `b1..b<joined>`, each joined on its
/// own column of `a`, the even positions under an alias.
fn star(target: &str, joined: usize) -> String {
    let joins: Vec<String> = (1..=joined)
        .map(|i| {
            let kind = if i % 2 == 0 {
                "INNER JOIN"
            } else {
                "LEFT JOIN"
            };
            if i % 2 == 0 {
                format!("{kind} b{i} AS t{i} ON a.k{i} = t{i}.id")
            } else {
                format!("{kind} b{i} ON a.k{i} = b{i}.id")
            }
        })
        .collect();
    let fields: Vec<String> = (1..=joined)
        .map(|i| {
            if i % 2 == 0 {
                format!("t{i}.x AS x{i}")
            } else {
                format!("b{i}.x AS x{i}")
            }
        })
        .collect();
    format!(
        "TRANSFORM {target} FROM a {} SELECT a.note AS note, {}",
        joins.join(" "),
        fields.join(", ")
    )
}

/// U2 (#1464), and define → `DROP`: a star of 5 tables defines, creating
/// `R`, every position's `M`, `J` and the lock row, and recording every joined
/// table; one of 6 is refused by name and leaves nothing. `DROP` takes every
/// hidden table and the joined tables' rows.
#[tokio::test]
async fn u2_a_star_of_five_defines_and_drops_every_hidden_table_and_six_are_refused() {
    let _knob = serial(true).await;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(TABLES).await.expect("seed");
    let trellis = define_only(db.dsn()).await;

    let err = define_err(&db.pool, &star("six", 5)).await;
    assert!(
        matches!(
            err,
            CatalogError::Validate(ValidationError::StarJoinTooManyTables { tables: 6 })
        ),
        "{err:?}"
    );
    assert!(err.to_string().contains("at most 5"), "{err}");
    assert_nothing_left(&raw, "six").await;

    trellis
        .apply(&star("five", 4))
        .await
        .expect("a star of five tables defines");
    let id = definition_id(&raw, "five").await;
    let suffixes: Vec<String> = hidden_tables(&raw, id)
        .await
        .into_iter()
        .map(|name| name.trim_start_matches(&format!("join1_{id}_")).to_string())
        .collect();
    assert_eq!(suffixes, ["j", "l", "m", "m2", "m3", "m4", "r"]);
    assert_eq!(
        count(
            &raw,
            &format!(
                "select count(*) from definition_join_tables where transform_id = {id} \
                 and position between 2 and 4"
            )
        )
        .await,
        3,
        "positions 2 to 4 are recorded beside join_table"
    );
    // The stored text reads back as written, aliases and kinds included.
    let stored: String = raw
        .query_one(
            "select definition_text from transform_definitions where id = $1",
            &[&id],
        )
        .await
        .expect("stored text")
        .get(0);
    for piece in ["LEFT JOIN b3", "INNER JOIN b4 AS t4 ON a.k4 = t4.id"] {
        assert!(stored.contains(piece), "{piece} in {stored}");
    }

    trellis.apply("PAUSE TRANSFORM five").await.expect("pause");
    assert!(matches!(
        trellis.apply("DROP TRANSFORM five").await.expect("drop"),
        Applied::Dropped
    ));
    assert!(hidden_tables(&raw, id).await.is_empty());
    assert_eq!(
        count(&raw, "select count(*) from definition_join_tables").await,
        0
    );
    assert_eq!(definitions(&raw).await, 0);
}

/// Every refusal of a star's shape, by name, each leaving nothing behind.
#[tokio::test]
async fn a_stars_shape_refusals_are_named() {
    let _knob = serial(true).await;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(TABLES).await.expect("seed");
    raw.batch_execute(
        "create table public.facts (id integer, day date, primary key (id, day))
             partition by range (day);
         create table public.facts_1 partition of public.facts
             for values from ('2026-01-01') to ('2027-01-01');",
    )
    .await
    .expect("a partitioned table");

    let refused = |words: &'static str| move |err: &CatalogError| err.to_string().contains(words);
    type Check = Box<dyn Fn(&CatalogError) -> bool>;
    let cases: Vec<(&str, Check)> = vec![
        // Snowflake: b2 joins b1, not `a`.
        (
            "TRANSFORM v FROM a JOIN b1 ON a.k1 = b1.id JOIN b2 ON b1.id = b2.id \
             SELECT b2.x AS x",
            Box::new(|err| {
                matches!(
                    err,
                    CatalogError::Validate(ValidationError::JoinOnReadsAnotherJoinedTable {
                        other,
                        ..
                    }) if other == "b1"
                )
            }),
        ),
        // The root and two joined tables in one ON.
        (
            "TRANSFORM v FROM a JOIN b1 ON a.k1 = b1.id \
             JOIN b2 ON a.k2 = b2.id AND b1.id = b2.id SELECT b2.x AS x",
            Box::new(|err| {
                matches!(
                    err,
                    CatalogError::Validate(ValidationError::JoinOnReadsAnotherJoinedTable { .. })
                )
            }),
        ),
        // Position 1's ON reads a later joined table.
        (
            "TRANSFORM v FROM a JOIN b1 ON a.k1 = b2.id JOIN b2 ON a.k2 = b2.id \
             SELECT b1.x AS x",
            Box::new(|err| {
                matches!(
                    err,
                    CatalogError::Validate(ValidationError::JoinOnReadsAnotherJoinedTable {
                        table,
                        other,
                        ..
                    }) if table == "b1" && other == "b2"
                )
            }),
        ),
        // USING, a comma join and LATERAL at a later position.
        (
            "TRANSFORM v FROM a JOIN b1 ON a.k1 = b1.id JOIN b2 USING (id) SELECT b1.x AS x",
            Box::new(refused("JOIN USING")),
        ),
        (
            "TRANSFORM v FROM a JOIN b1 ON a.k1 = b1.id, b2 SELECT b1.x AS x",
            Box::new(refused("comma join")),
        ),
        (
            "TRANSFORM v FROM a JOIN b1 ON a.k1 = b1.id JOIN LATERAL b2 ON a.k2 = b2.id \
             SELECT b1.x AS x",
            Box::new(refused("JOIN LATERAL")),
        ),
        // A table joined twice, under another alias.
        (
            "TRANSFORM v FROM a JOIN b1 ON a.k1 = b1.id JOIN b1 AS again ON a.k2 = again.id \
             SELECT b1.x AS x",
            Box::new(refused("joined twice")),
        ),
        // The FROM table joined again at a later position.
        (
            "TRANSFORM v FROM a JOIN b1 ON a.k1 = b1.id JOIN a AS a2 ON a.k2 = a2.id \
             SELECT b1.x AS x",
            Box::new(refused("self-join")),
        ),
        // RIGHT, FULL and CROSS at a later position.
        (
            "TRANSFORM v FROM a JOIN b1 ON a.k1 = b1.id RIGHT JOIN b2 ON a.k2 = b2.id \
             SELECT b1.x AS x",
            Box::new(refused("only INNER and LEFT")),
        ),
        (
            "TRANSFORM v FROM a JOIN b1 ON a.k1 = b1.id FULL JOIN b2 ON a.k2 = b2.id \
             SELECT b1.x AS x",
            Box::new(refused("only INNER and LEFT")),
        ),
        (
            "TRANSFORM v FROM a JOIN b1 ON a.k1 = b1.id CROSS JOIN b2 SELECT b1.x AS x",
            Box::new(refused("only INNER and LEFT")),
        ),
        (
            "TRANSFORM v FROM a JOIN b1 ON a.k1 = b1.id NATURAL JOIN b2 SELECT b1.x AS x",
            Box::new(refused("only INNER and LEFT")),
        ),
        // The target named as a later joined table.
        (
            "TRANSFORM b2 FROM a JOIN b1 ON a.k1 = b1.id JOIN b2 ON a.k2 = b2.id \
             SELECT b1.x AS x",
            Box::new(|err| {
                matches!(
                    err,
                    CatalogError::Validate(ValidationError::TargetEqualsSource { table })
                        if table == "b2"
                )
            }),
        ),
        // A partitioned table at position 2.
        (
            "TRANSFORM v FROM a JOIN b1 ON a.k1 = b1.id \
             LEFT JOIN facts ON a.k2 = facts.id AND a.d = facts.day SELECT b1.x AS x",
            Box::new(|err| {
                matches!(
                    err,
                    CatalogError::PartitionedSource(
                        trellis::defs::partition_tree::PartitionRefusal::OneToOneJoinSide {
                            table
                        }
                    ) if table.contains("facts")
                )
            }),
        ),
        // Position 3's ON columns aren't unique: the refusal names its table.
        (
            "TRANSFORM v FROM a JOIN b1 ON a.k1 = b1.id JOIN b2 ON a.k2 = b2.id \
             LEFT JOIN n ON a.k3 = n.k SELECT n.x AS x",
            Box::new(|err| {
                matches!(
                    err,
                    CatalogError::Validate(ValidationError::OneToOneJoinKeyNotUnique {
                        table,
                        columns,
                    }) if table == "public.n" && columns == &["k".to_string()]
                )
            }),
        ),
        // An aggregate JOIN of more than one joined table.
        (
            "TRANSFORM v FROM a JOIN b1 ON a.k1 = b1.id JOIN b2 ON a.k2 = b2.id \
             GROUP BY a.note SELECT a.note AS note, COUNT(*) AS c",
            Box::new(|err| {
                matches!(
                    err,
                    CatalogError::Validate(ValidationError::JoinTooManyTables)
                )
            }),
        ),
    ];
    for (text, check) in cases {
        let err = define_err(&db.pool, text).await;
        assert!(check(&err), "{text}: {err:?} / {err}");
        assert_nothing_left(&raw, text).await;
    }
}

/// The knob gates at define and at `RESUME`, never at load: with it off a
/// star is refused by name, and a star defined while it was on is refused at
/// its `RESUME` once it is off, and resumes again once it is back on.
#[tokio::test]
async fn the_knob_gates_a_star_at_define_and_resume() {
    let _knob = serial(false).await;
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let raw = connect_raw(db.dsn()).await;
    raw.batch_execute(TABLES).await.expect("seed");
    let trellis = define_only(db.dsn()).await;

    let err = define_err(&db.pool, &star("v", 2)).await;
    assert!(
        matches!(
            err,
            CatalogError::Validate(ValidationError::StarJoinDisabled { tables: 3 })
        ),
        "{err:?}"
    );
    assert_nothing_left(&raw, "the star with the knob off").await;
    // The two-table JOIN doesn't need it.
    trellis
        .apply(&star("two", 1))
        .await
        .expect("a two-table JOIN defines with the knob off");

    set_star_join(true);
    trellis.apply(&star("v", 2)).await.expect("define the star");
    trellis.apply("PAUSE TRANSFORM v").await.expect("pause");
    set_star_join(false);
    // The stored star still loads: status reads it.
    assert!(trellis.status("v").await.expect("status").is_some());
    let err = trellis
        .apply("RESUME TRANSFORM v")
        .await
        .expect_err("the resume revalidates with the knob off");
    assert!(err.to_string().contains("not available yet"), "{err}");
    set_star_join(true);
    trellis
        .apply("RESUME TRANSFORM v")
        .await
        .expect("resumes with the knob on");
    set_star_join(false);
}
