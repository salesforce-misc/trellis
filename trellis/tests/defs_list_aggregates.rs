//! Bare `array_agg`/`string_agg` (issue #293): recompute-only aggregates over
//! `text`, like `jsonb_agg`, with no `ORDER BY`. Element order is the one
//! documented caveat (`docs/type-support.md`, `array_agg` and `string_agg`), so the
//! pipeline test compares element sets and the evaluator test feeds both
//! sides the same row order.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::eval::{RegexCache, Row, Value, evaluate_aggregate};
use trellis::defs::invertibility::{AggregateArg, Invertibility, classify};
use trellis::defs::pg_type::PgType;
use trellis::defs::validate::ValidationError;
use trellis::defs::{
    backfill_definition, create_aggregate_target_table, create_definition,
    create_definition_without_backfill, parse, registry, validate,
};
use trellis::integer::IntWidth;
use trellis::staging::{StagedWatermark, apply, seal};

fn columns() -> HashMap<String, ValueType> {
    HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("grp".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("tag".to_string(), ValueType::Text),
        ("n".to_string(), ValueType::Numeric),
    ])
}

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public"))
        .await
        .expect("session bootstrap");
    client
}

#[test]
fn result_types_follow_postgres() {
    assert_eq!(
        registry::aggregate_result_type("ARRAY_AGG", ValueType::Text),
        Some(ValueType::Other(PgType::TextArray))
    );
    assert_eq!(
        registry::aggregate_result_type("STRING_AGG", ValueType::Text),
        Some(ValueType::Text)
    );
    for name in ["ARRAY_AGG", "STRING_AGG"] {
        assert_eq!(
            registry::aggregate_result_type(name, ValueType::Numeric),
            None
        );
        assert_eq!(
            registry::aggregate_result_type(name, ValueType::Other(PgType::Jsonb)),
            None
        );
    }
}

#[test]
fn both_are_classified_recompute_only() {
    for name in ["ARRAY_AGG", "STRING_AGG"] {
        let verdict = classify(name, AggregateArg::Column(ValueType::Text))
            .unwrap_or_else(|| panic!("{name} must classify"));
        assert_eq!(verdict.invertibility, Invertibility::RecomputeOnly);
    }
}

fn validate_sql(sql: &str) -> Result<(), ValidationError> {
    validate(&parse(sql).expect("parses"), &columns(), &HashMap::new())
}

#[test]
fn definitions_validate_over_text_only() {
    validate_sql(
        "TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, ARRAY_AGG(tag) AS tags, \
         STRING_AGG(tag, ', ') AS names",
    )
    .expect("text arguments validate");

    for sql in [
        "TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, ARRAY_AGG(n) AS a",
        "TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, STRING_AGG(n, ',') AS a",
    ] {
        assert!(
            matches!(
                validate_sql(sql),
                Err(ValidationError::FunctionArgTypeMismatch { .. })
            ),
            "{sql}"
        );
    }
}

#[test]
fn string_agg_separator_must_be_a_literal() {
    assert_eq!(
        validate_sql(
            "TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, STRING_AGG(tag, tag) AS a"
        ),
        Err(ValidationError::StringAggSeparatorNotLiteral {
            field: "a".to_string()
        })
    );
}

#[test]
fn arity_is_enforced_by_the_parser() {
    assert!(
        parse("TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, STRING_AGG(tag) AS a").is_err()
    );
    assert!(
        parse("TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, ARRAY_AGG(tag, tag) AS a")
            .is_err()
    );
}

/// The evaluator's fold renders exactly what a server-side aggregate does,
/// including `array_out`'s quoting and `NULL` handling, over groups that
/// both sides read in the same order.
#[tokio::test]
async fn evaluator_fold_matches_a_server_side_aggregate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let groups: &[&[Option<&str>]] = &[
        &[Some("a"), Some("b"), Some("c")],
        &[
            Some("has space"),
            Some("com,ma"),
            Some("q\"uote"),
            Some("back\\slash"),
        ],
        &[Some(""), Some("NULL"), Some("null"), Some("{x}")],
        &[Some("a"), None, Some("b")],
        &[None, None],
        &[Some("only")],
        &[
            Some(" lead"),
            Some("tab\tx"),
            Some("v\u{0b}t"),
            Some("nb\u{a0}sp"),
            Some("em\u{2003}sp"),
            Some("NuLl"),
            Some("a\"b\\c{d}e,f"),
            Some("é"),
        ],
    ];
    let def = parse(
        "TRANSFORM t FROM s GROUP BY grp SELECT grp AS grp, ARRAY_AGG(tag) AS tags, \
         STRING_AGG(tag, ', ') AS names, STRING_AGG(tag, '') AS joined, \
         STRING_AGG(tag, 'it''s') AS quoted",
    )
    .expect("parses");
    let columns = columns();
    validate(&def, &columns, &HashMap::new()).expect("validate");

    for group in groups {
        let values_sql = group
            .iter()
            .map(|v| match v {
                Some(s) => format!("('{}'::text)", s.replace('\'', "''")),
                None => "(null::text)".to_string(),
            })
            .collect::<Vec<_>>()
            .join(",");
        let server = client
            .query_one(
                &format!(
                    "select array_agg(v)::text, string_agg(v, ', '), string_agg(v, ''), \
                     string_agg(v, 'it''s') \
                     from (values {values_sql}) t(v)"
                ),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("server aggregate over {group:?}: {e}"));
        let server_tags: Option<String> = server.get(0);
        let server_names: Option<String> = server.get(1);
        let server_joined: Option<String> = server.get(2);
        let server_quoted: Option<String> = server.get(3);

        let rows: Vec<Row> = group
            .iter()
            .map(|v| {
                Row::from([
                    ("grp".to_string(), Some("1".to_string())),
                    ("tag".to_string(), v.map(str::to_string)),
                ])
            })
            .collect();
        let result = evaluate_aggregate(&def, &rows, &columns, &mut RegexCache::default())
            .unwrap_or_else(|e| panic!("fold over {group:?}: {e}"));

        assert_eq!(
            result["tags"],
            server_tags.map(|t| Value::Other(PgType::TextArray, t)),
            "ARRAY_AGG over {group:?}"
        );
        assert_eq!(
            result["names"],
            server_names.map(Value::Text),
            "STRING_AGG over {group:?}"
        );
        assert_eq!(
            result["joined"],
            server_joined.map(Value::Text),
            "STRING_AGG with an empty separator over {group:?}"
        );
        assert_eq!(
            result["quoted"],
            server_quoted.map(Value::Text),
            "STRING_AGG with a quoted separator over {group:?}"
        );
    }
}

/// parse -> create -> stage CDC -> apply (the ledger's live recompute) ->
/// read, compared as element sets against live aggregates over the source.
#[tokio::test]
async fn a_cdc_apply_produces_array_and_string_aggregates_matching_the_source_elements() {
    const DEF_SQL: &str = "TRANSFORM totals FROM events GROUP BY grp SELECT grp AS grp, \
                           ARRAY_AGG(tag) AS tags, STRING_AGG(tag, '|') AS names";

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute("create table events (id integer primary key, grp integer, tag text)")
        .await
        .expect("create source table");

    let columns = columns();
    let columns: HashMap<String, ValueType> = columns
        .into_iter()
        .filter(|(name, _)| name != "n")
        .collect();
    let def = parse(DEF_SQL).expect("parse");
    validate(&def, &columns, &HashMap::new()).expect("validate");
    create_definition(&db.pool, DEF_SQL, &columns)
        .await
        .expect("create definition");
    create_aggregate_target_table(&db.pool, &def, "public", &columns)
        .await
        .expect("create aggregate target table");

    client
        .batch_execute(
            r#"insert into events (id, grp, tag) values
                 (1, 1, 'red'), (2, 1, 'with space'), (3, 1, null), (4, 2, 'solo')"#,
        )
        .await
        .expect("seed source rows");

    let images = [
        ("1", r#"{"id":"1","grp":"1","tag":"red"}"#),
        ("2", r#"{"id":"2","grp":"1","tag":"with space"}"#),
        ("3", r#"{"id":"3","grp":"1","tag":null}"#),
        ("4", r#"{"id":"4","grp":"2","tag":"solo"}"#),
    ];
    for (key, image) in images {
        let src_table = format!("{DEFAULT_SCHEMA}.events");
        client
            .execute(
                "insert into seg_0 (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                 values ($1, $2, 'insert', $3, null, $4::text::jsonb, 0)",
                &[
                    &src_table,
                    &key,
                    &testkit::wal_insert_lsn(&client).await,
                    &image,
                ],
            )
            .await
            .unwrap_or_else(|e| panic!("stage {key}: {e}"));
    }

    let outcome = seal::seal_phase1(&mut client).await.expect("seal phase 1");
    seal::seal_phase2(&client, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");
    apply::drain_once(
        &db.pool,
        outcome.sealed_seg_seq,
        "list_agg_worker",
        1,
        "trellis_defs_list_aggregates_issue_293",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("drain_once")
    .expect("drain_once must claim and drain something");

    for grp in [1i32, 2] {
        let sorted = |expr: &str, table: &str| {
            format!(
                "select array(select e from unnest({expr}) e order by e nulls last) \
                 from {table} where grp = $1"
            )
        };
        let actual_tags: Vec<Option<String>> = client
            .query_one(&sorted("tags", "totals"), &[&grp])
            .await
            .unwrap_or_else(|e| panic!("read tags for grp {grp}: {e}"))
            .get(0);
        let expected_tags: Vec<Option<String>> = client
            .query_one(
                "select array(select e from unnest(array_agg(tag)) e order by e nulls last) \
                 from events where grp = $1",
                &[&grp],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            actual_tags, expected_tags,
            "grp {grp}: ARRAY_AGG element set"
        );

        let actual_names: String = client
            .query_one(
                "select array_to_string(array(select e from unnest(string_to_array(names, '|')) e \
                 order by e), '|') from totals where grp = $1",
                &[&grp],
            )
            .await
            .unwrap()
            .get(0);
        let expected_names: String = client
            .query_one(
                "select string_agg(tag, '|' order by tag) from events where grp = $1",
                &[&grp],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            actual_names, expected_names,
            "grp {grp}: STRING_AGG element set"
        );
    }
}

/// The direct build of an existing source folds both aggregates server-side.
#[tokio::test]
async fn a_direct_build_folds_both_aggregates() {
    const DEF_SQL: &str = "TRANSFORM totals FROM events GROUP BY grp SELECT grp AS grp, \
                           ARRAY_AGG(tag) AS tags, STRING_AGG(tag, '|') AS names";

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    client
        .batch_execute(
            "create table events (id integer primary key, grp integer, tag text); \
             insert into events values (1, 1, 'a'), (2, 1, 'b'), (3, 1, null), (4, 2, null)",
        )
        .await
        .expect("seed");

    let columns: HashMap<String, ValueType> = columns()
        .into_iter()
        .filter(|(name, _)| name != "n")
        .collect();
    let def = parse(DEF_SQL).expect("parse");
    validate(&def, &columns, &HashMap::new()).expect("validate");
    create_definition_without_backfill(&db.pool, DEF_SQL, &columns)
        .await
        .expect("create definition");
    create_aggregate_target_table(&db.pool, &def, "public", &columns)
        .await
        .expect("create aggregate target table");
    backfill_definition(&db.pool, &def, "public", &def.source, &columns)
        .await
        .expect("backfill");

    let row = client
        .query_one(
            "select (select array_agg(e order by e) from unnest(tags) e)::text, \
                    (select string_agg(e, '|' order by e) from unnest(string_to_array(names, '|')) e) \
             from totals where grp = 1",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "{a,b,NULL}");
    assert_eq!(row.get::<_, String>(1), "a|b");

    let row = client
        .query_one("select tags::text, names from totals where grp = 2", &[])
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "{NULL}");
    assert_eq!(row.get::<_, Option<String>>(1), None);
}
