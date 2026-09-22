//! End-to-end tests for issue #115's `jsonb` support: the passthrough gap
//! check, the join/primary/`GROUP BY` key refusal, the typed-literal
//! ("computed 1-1 target") role (covered by `defs_typed_literals.rs`'s
//! shared `CASES` table, which this issue added a `JSONB` row to), and the
//! `jsonb_agg` aggregate.
//!
//! # Why these run against a live server
//!
//! Per #111-#119's playbook: check `pg_cast` rather than assume `::text` is
//! a type's own output function, check `pg_proc`/`pg_aggregate` rather than
//! assume an aggregate exists or is safe, and check equality/rendering
//! agreement live rather than reason about it from the type's name. Three
//! findings this issue's investigation produced, none of them the shape the
//! epic's own framing predicted:
//!
//! 1. **`jsonb` has no second `::text` renderer** (unlike `boolean`/`inet`),
//!    and `jsonb_out` genuinely canonicalizes object key order. It is
//!    *still* refused as a text-matched key, because an embedded JSON
//!    *number* is not renormalized — `'{"a":1}'::jsonb` and
//!    `'{"a":1.0}'::jsonb` are `=` but `::text`-distinct — the same
//!    equivalence-class hazard `real`/`interval` have, discovered one type
//!    deeper. See `crate::jsonb`'s module doc for the full account.
//! 2. **`jsonb_agg`'s `STABLE` marking is not about ordering.**
//!    `array_agg`/`string_agg` are equally order-sensitive and are
//!    `IMMUTABLE`. `jsonb_agg` alone is `STABLE` because it is polymorphic
//!    and, for some argument types (`timestamptz`, `money`), its row-to-
//!    `jsonb` conversion reads a session GUC. Restricting `JSONB_AGG`'s
//!    argument to `jsonb` itself excludes that hazard entirely.
//! 3. **`jsonb_agg` is the one aggregate in the registry whose transition
//!    function is not `STRICT`** — a `NULL` row becomes a JSON `null`
//!    element rather than being skipped, unlike every other aggregate here.
//!
//! Per ADR-0013 every comparison is against independently-authored SQL —
//! plain `pg_cast`, plain `pg_proc`, plain `jsonb_agg`, plain `=` — never
//! against `defs::oracle::recompute`, which would be the engine's own
//! renderer grading itself. Harness conventions follow
//! `defs_bit.rs`/`defs_boolean.rs`.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::eval::{RegexCache, Row, Value, evaluate_aggregate};
use trellis::defs::invertibility::{AggregateArg, Invertibility, classify};
use trellis::defs::pg_type::PgType;
use trellis::defs::{
    create_aggregate_target_table, create_definition, create_relationship, parse, registry,
    source_primary_key, validate,
};
use trellis::integer::IntWidth;
use trellis::staging::{StagedWatermark, apply, seal};

// ---------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------

fn source_columns() -> HashMap<String, ValueType> {
    HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
        ("j".to_string(), ValueType::Other(PgType::Jsonb)),
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

// ---------------------------------------------------------------------
// 1. Rendering: no second renderer, unlike boolean/inet
// ---------------------------------------------------------------------

/// `jsonb`'s `::text` is `jsonb_out` directly — no `pg_cast` override, the
/// same "easy" shape `bit`/`bit varying` (#118) had, contrasted with
/// `boolean`, which does have one.
#[tokio::test]
async fn jsonb_has_no_dedicated_text_cast_function() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let n: i64 = client
        .query_one(
            "select count(*) from pg_cast \
             where castsource = 'jsonb'::regtype and casttarget = 'text'::regtype",
            &[],
        )
        .await
        .expect("pg_cast probe")
        .get(0);
    assert_eq!(n, 0, "jsonb must have no dedicated ::text cast function");

    // Control: `boolean` does have one, so this probe is discriminating.
    let n: i64 = client
        .query_one(
            "select count(*) from pg_cast \
             where castsource = 'boolean'::regtype and casttarget = 'text'::regtype",
            &[],
        )
        .await
        .expect("pg_cast probe")
        .get(0);
    assert_eq!(n, 1, "boolean must have one, as the discriminating control");
}

/// `jsonb_in`/`jsonb_out` are `IMMUTABLE` — the volatility bar the
/// typed-literal role needs (`super::defs::typed_literal`'s doc comment).
#[tokio::test]
async fn jsonb_in_and_out_are_immutable() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let rows = client
        .query(
            "select proname, provolatile from pg_proc \
             where proname in ('jsonb_in', 'jsonb_out') and pronamespace = 'pg_catalog'::regnamespace",
            &[],
        )
        .await
        .expect("pg_proc probe");
    assert_eq!(rows.len(), 2, "both jsonb_in and jsonb_out must exist");
    for row in rows {
        let name: String = row.get(0);
        let volatility: i8 = row.get::<_, i8>(1);
        assert_eq!(
            volatility as u8 as char, 'i',
            "{name} must be IMMUTABLE, not {volatility}"
        );
    }
}

/// `to_jsonb`'s special datetime/`bytea` renderer never applies to a
/// `jsonb`-typed column — a `jsonb` value's `to_jsonb` conversion is the
/// identity — so `staging::apply::row_as_text_jsonb_sql`'s live-row reads
/// (`<col>::text`) and a bare `to_jsonb(t.*)` would agree even if anything
/// in this engine still called the latter (issue #248 already replaced
/// every such call site). Checked per #113/#116's playbook rather than
/// assumed.
#[tokio::test]
async fn to_jsonb_and_text_agree_for_jsonb() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(r#"create table t (j jsonb); insert into t values ('{"b": 2, "a": 1}')"#)
        .await
        .expect("seed");
    let row = client
        .query_one("select j::text, to_jsonb(t.*)->>'j' from t", &[])
        .await
        .expect("compare renderers");
    let (via_text, via_tojsonb): (String, String) = (row.get(0), row.get(1));
    assert_eq!(via_text, via_tojsonb);
}

// ---------------------------------------------------------------------
// 2. Key roles: refused, and why — a fresh hazard shape
// ---------------------------------------------------------------------

/// The core live finding: `jsonb_out` canonicalizes object key order (so a
/// text-matched key is *not* split by two spellings of the same key set),
/// but an embedded JSON number preserves its input scale, so two spellings
/// of the *same number* remain `jsonb`-equal while diverging under
/// `::text` — the reason `jsonb` is not on
/// `catalog::TEXT_STABLE_JOIN_KEY_TYPES` despite clearing the key-order bar.
#[tokio::test]
async fn key_order_is_canonicalized_but_embedded_number_scale_is_not() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    // Key order: two spellings of one key set converge to one `::text`.
    let row = client
        .query_one(
            r#"select ('{"b": 2, "a": 1}'::jsonb)::text = ('{"a": 1, "b": 2}'::jsonb)::text as
               same_text"#,
            &[],
        )
        .await
        .expect("key order probe");
    assert!(
        row.get::<_, bool>(0),
        "differing key order must converge under ::text"
    );

    // Number scale: `=` holds, `::text` does not — the residual hazard.
    let row = client
        .query_one(
            r#"select '{"a": 1}'::jsonb = '{"a": 1.0}'::jsonb as eq,
                      ('{"a": 1}'::jsonb)::text = ('{"a": 1.0}'::jsonb)::text as same_text"#,
            &[],
        )
        .await
        .expect("number scale probe");
    let (eq, same_text): (bool, bool) = (row.get(0), row.get(1));
    assert!(
        eq,
        "'{{\"a\": 1}}' and '{{\"a\": 1.0}}' must be jsonb-equal"
    );
    assert!(
        !same_text,
        "...but must NOT converge under ::text — this is why jsonb is refused as a text-matched key"
    );
}

/// A `jsonb` column is refused as a relationship join key and as a 1-1
/// primary key — the live-facing regression pin for
/// `catalog::TEXT_STABLE_JOIN_KEY_TYPES`'s deliberate `jsonb` absence.
#[tokio::test]
async fn jsonb_is_refused_as_a_relationship_join_key_and_primary_key() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table parent (k jsonb primary key); \
             create table child (id bigint primary key, k jsonb); \
             alter table child replica identity full; \
             alter table parent replica identity full",
        )
        .await
        .expect("create relationship tables");

    create_relationship(&db.pool, "RELATIONSHIP r FROM child.k TO parent.k")
        .await
        .expect_err("a jsonb column must be refused as a join key");

    source_primary_key(&db.pool, "parent")
        .await
        .expect_err("a jsonb primary key must be refused");
}

/// The `GROUP BY` key gate refuses `jsonb` too — a no-DB regression pin
/// alongside `validate`'s own unit test
/// (`an_other_typed_column_is_rejected_as_an_aggregate_group_by_key`).
#[test]
fn jsonb_group_by_key_is_refused() {
    let def =
        parse("TRANSFORM t FROM s GROUP BY j SELECT j AS k, SUM(n) AS total").expect("parses");
    validate(&def, &source_columns(), &HashMap::new())
        .expect_err("jsonb must still be refused as a GROUP BY key");
}

// ---------------------------------------------------------------------
// 3. `MIN`/`MAX(jsonb)` — not attempted (out of scope; spot-checked only)
// ---------------------------------------------------------------------

#[tokio::test]
async fn jsonb_min_max_is_not_wired_up() {
    for name in ["MIN", "MAX"] {
        assert_eq!(
            registry::aggregate_result_type(name, ValueType::Other(PgType::Jsonb)),
            None,
            "{name}(jsonb) is out of this issue's scope — see crate::jsonb's module doc"
        );
    }
}

// ---------------------------------------------------------------------
// 4. `jsonb_agg`'s STABLE marking: root-caused live
// ---------------------------------------------------------------------

/// The load-bearing investigation result: `array_agg`/`string_agg` are
/// `IMMUTABLE` despite being just as order-sensitive as `jsonb_agg`, which
/// alone is `STABLE` — so order-sensitivity is not the reason.
#[tokio::test]
async fn jsonb_agg_is_stable_while_array_agg_and_string_agg_are_immutable() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let rows = client
        .query(
            "select proname, provolatile from pg_proc \
             where proname in ('array_agg', 'string_agg', 'jsonb_agg') \
               and pronamespace = 'pg_catalog'::regnamespace",
            &[],
        )
        .await
        .expect("pg_proc probe");
    assert!(!rows.is_empty());
    for row in rows {
        let name: String = row.get(0);
        let volatility = row.get::<_, i8>(1) as u8 as char;
        if name == "jsonb_agg" {
            assert_eq!(volatility, 's', "jsonb_agg must be STABLE");
        } else {
            assert_eq!(volatility, 'i', "{name} must be IMMUTABLE, not STABLE");
        }
    }
}

/// The real reason `jsonb_agg` is `STABLE`: it is polymorphic, and for a
/// `timestamptz` argument its row-to-`jsonb` conversion reads the session
/// `TimeZone` GUC. Restricting `JSONB_AGG`'s argument to `jsonb` itself
/// (this crate's own scope decision) sidesteps this — demonstrated by the
/// second half of this test, which shows a `jsonb` argument is unaffected.
#[tokio::test]
async fn jsonb_agg_over_timestamptz_is_guc_dependent_but_over_jsonb_is_not() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table tstz (v timestamptz); \
             insert into tstz values ('2024-01-01 12:00:00+00'); \
             create table j (v jsonb); \
             insert into j values ('\"2024-01-01T12:00:00+00:00\"')",
        )
        .await
        .expect("seed");

    client.batch_execute("set timezone = 'UTC'").await.unwrap();
    let utc_tstz: String = client
        .query_one("select jsonb_agg(v)::text from tstz", &[])
        .await
        .unwrap()
        .get(0);
    let utc_j: String = client
        .query_one("select jsonb_agg(v)::text from j", &[])
        .await
        .unwrap()
        .get(0);

    client
        .batch_execute("set timezone = 'America/New_York'")
        .await
        .unwrap();
    let ny_tstz: String = client
        .query_one("select jsonb_agg(v)::text from tstz", &[])
        .await
        .unwrap()
        .get(0);
    let ny_j: String = client
        .query_one("select jsonb_agg(v)::text from j", &[])
        .await
        .unwrap()
        .get(0);

    assert_ne!(
        utc_tstz, ny_tstz,
        "jsonb_agg(timestamptz) must be TimeZone-dependent — the hazard being excluded"
    );
    assert_eq!(
        utc_j, ny_j,
        "jsonb_agg(jsonb) must be unaffected by TimeZone — the exclusion actually works"
    );
}

/// `jsonb_agg`'s transition function is not `STRICT`: a `NULL` row becomes
/// a JSON `null` element, unlike every other aggregate `AGGREGATE_FUNCTION_SPECS`
/// admits (all of which skip a `NULL` row entirely). Only a genuinely
/// *empty* row set folds to SQL `NULL`.
#[tokio::test]
async fn jsonb_agg_includes_null_rows_unlike_every_other_aggregate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            r#"create table t (g int, v jsonb);
               insert into t values (1, '"a"'), (1, NULL), (1, '"b"'), (2, NULL)"#,
        )
        .await
        .expect("seed");

    let group1: String = client
        .query_one("select jsonb_agg(v)::text from t where g = 1", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(group1, r#"["a", null, "b"]"#);

    let all_null_group: String = client
        .query_one("select jsonb_agg(v)::text from t where g = 2", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(all_null_group, "[null]");

    let empty_group: Option<String> = client
        .query_one("select jsonb_agg(v)::text from t where g = 99", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(empty_group, None, "a genuinely empty group is SQL NULL");

    // The Rust evaluator must reproduce this exactly (`fold_jsonb_agg`).
    let def = parse("TRANSFORM out FROM t GROUP BY g SELECT g AS g, JSONB_AGG(v) AS items")
        .expect("parses");
    let columns = HashMap::from([
        ("g".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("v".to_string(), ValueType::Other(PgType::Jsonb)),
    ]);
    validate(&def, &columns, &HashMap::new()).expect("validate");

    let rows: Vec<Row> = vec![
        Row::from([
            ("g".to_string(), Some("1".to_string())),
            ("v".to_string(), Some("\"a\"".to_string())),
        ]),
        Row::from([
            ("g".to_string(), Some("1".to_string())),
            ("v".to_string(), None),
        ]),
        Row::from([
            ("g".to_string(), Some("1".to_string())),
            ("v".to_string(), Some("\"b\"".to_string())),
        ]),
    ];
    let result = evaluate_aggregate(&def, &rows, &columns, &mut RegexCache::default())
        .expect("evaluate_aggregate");
    assert_eq!(result["items"], Some(Value::Other(PgType::Jsonb, group1)));
}

/// `JSONB_AGG`'s result type is `jsonb`, matching a live `pg_typeof`.
#[tokio::test]
async fn jsonb_agg_result_type_matches_pg_typeof() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let pg_type: String = client
        .query_one(
            "select pg_typeof(jsonb_agg(v))::text from (values ('1'::jsonb)) t(v)",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(pg_type, "jsonb");

    assert_eq!(
        registry::aggregate_result_type("JSONB_AGG", ValueType::Other(PgType::Jsonb)),
        Some(ValueType::Other(PgType::Jsonb))
    );
}

/// `JSONB_AGG` is classified `RecomputeOnly` — a live-facing regression
/// guard alongside `defs::invertibility`'s own pure-code unit tests.
#[test]
fn jsonb_agg_is_classified_recompute_only() {
    let verdict = classify(
        "JSONB_AGG",
        AggregateArg::Column(ValueType::Other(PgType::Jsonb)),
    )
    .expect("JSONB_AGG must classify");
    assert_eq!(verdict.invertibility, Invertibility::RecomputeOnly);
}

/// The Rust evaluator's fold matches a server-side `jsonb_agg` byte-for-byte
/// over a handful of small groups — the same "cross-checked against an
/// independently-authored SQL aggregate" shape
/// `bit_and_or_fold_matches_a_server_side_aggregate` (#118) pins for its own
/// family. Element order is not independently varied here (both sides read
/// the same in-memory row order), which is deliberate — see `crate::jsonb`'s
/// module doc for why cross-recompute order agreement is a documented
/// caveat, not a guarantee, and out of scope to force via grammar in this
/// issue.
#[tokio::test]
async fn jsonb_agg_fold_matches_a_server_side_aggregate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let groups: &[&[Option<&str>]] = &[
        &[Some("1"), Some("2"), Some("3")],
        &[Some(r#"{"a": 1}"#), Some(r#"[1, 2]"#)],
        &[Some("\"x\""), None, Some("\"y\"")],
        &[None, None],
        &[Some("true"), Some("false"), Some("null")],
    ];

    for group in groups {
        let values_sql = group
            .iter()
            .map(|v| match v {
                Some(j) => format!("('{j}'::jsonb)"),
                None => "(null::jsonb)".to_string(),
            })
            .collect::<Vec<_>>()
            .join(",");
        let server: String = client
            .query_one(
                &format!("select (jsonb_agg(v))::text from (values {values_sql}) t(v)"),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("server jsonb_agg over {group:?}: {e}"))
            .get(0);

        let def = parse("TRANSFORM t FROM s GROUP BY id SELECT id AS k, JSONB_AGG(j) AS items")
            .expect("parses");
        let columns = source_columns();
        validate(&def, &columns, &HashMap::new()).expect("validate");

        let rows: Vec<Row> = group
            .iter()
            .map(|v| {
                Row::from([
                    ("id".to_string(), Some("1".to_string())),
                    ("j".to_string(), v.map(|s| s.to_string())),
                ])
            })
            .collect();
        let result = evaluate_aggregate(&def, &rows, &columns, &mut RegexCache::default())
            .unwrap_or_else(|e| panic!("jsonb_agg fold over {group:?}: {e}"));

        assert_eq!(
            result["items"],
            Some(Value::Other(PgType::Jsonb, server.clone())),
            "JSONB_AGG over {group:?}: evaluator disagreed with server ({server})"
        );
    }
}

// ---------------------------------------------------------------------
// 5. The full pipeline: install -> CDC -> apply -> read
// ---------------------------------------------------------------------

/// parse -> create -> stage CDC -> **apply** (`RecomputeOnly`, so this
/// exercises `staging::apply_aggregate`'s `probe_recompute_fields_bulk`
/// path, which asks Postgres directly rather than the Rust evaluator) ->
/// read, cross-checked against the *set* of elements a live, independent
/// `jsonb_agg` produces over the same rows — set, not sequence, because
/// element order is this aggregate's one documented, non-corrupting
/// caveat (`crate::jsonb`'s module doc).
#[tokio::test]
async fn a_cdc_apply_produces_a_jsonb_agg_matching_the_live_elements() {
    const DEF_SQL: &str =
        "TRANSFORM totals FROM events GROUP BY grp SELECT grp AS grp, JSONB_AGG(payload) AS items";

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table events (id integer primary key, grp integer, payload jsonb); \
             alter table events replica identity full",
        )
        .await
        .expect("create source table");

    let columns = HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("grp".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("payload".to_string(), ValueType::Other(PgType::Jsonb)),
    ]);
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
            r#"insert into events (id, grp, payload) values
                 (1, 1, '{"n": 1}'),
                 (2, 1, '{"n": 2}'),
                 (3, 2, 'null')"#,
        )
        .await
        .expect("seed source rows");

    async fn stage_image(client: &Client, segment: &str, key: &str, new_image: &str) {
        let src_table = format!("{DEFAULT_SCHEMA}.events");
        client
            .execute(
                &format!(
                    "insert into {segment} \
                     (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                     values ($1, $2, 'insert', $3, null, $4::text::jsonb, 0)"
                ),
                &[&src_table, &key, &PgLsn::from(1u64), &new_image],
            )
            .await
            .unwrap_or_else(|e| panic!("stage {key}: {e}"));
    }

    stage_image(
        &client,
        "seg_0",
        "1",
        r#"{"id":"1","grp":"1","payload":"{\"n\": 1}"}"#,
    )
    .await;
    stage_image(
        &client,
        "seg_0",
        "2",
        r#"{"id":"2","grp":"1","payload":"{\"n\": 2}"}"#,
    )
    .await;
    stage_image(
        &client,
        "seg_0",
        "3",
        r#"{"id":"3","grp":"2","payload":"null"}"#,
    )
    .await;

    let outcome = seal::seal_phase1(&mut client).await.expect("seal phase 1");
    seal::seal_phase2(&client, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");
    apply::drain_once(
        &db.pool,
        outcome.sealed_seg_seq,
        "jsonb_worker",
        1,
        "trellis_defs_jsonb_issue_115",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("drain_once")
    .expect("drain_once must claim and drain something");

    for grp in [1i32, 2] {
        let items_text: String = client
            .query_one("select items::text from totals where grp = $1", &[&grp])
            .await
            .unwrap_or_else(|e| panic!("read totals for grp {grp}: {e}"))
            .get(0);
        let expected_elements: Vec<String> = client
            .query_one(
                "select array_agg(payload::text order by payload::text) from events where grp = $1",
                &[&grp],
            )
            .await
            .unwrap()
            .get::<_, Vec<String>>(0);

        let mut actual_elements: Vec<String> = serde_json_array_elements(&items_text);
        actual_elements.sort();
        assert_eq!(
            actual_elements, expected_elements,
            "grp {grp}: JSONB_AGG's element set must match the source rows exactly"
        );
    }
}

/// A tiny, purpose-built splitter for a canonical top-level jsonb *array*
/// literal (`[elem, elem, ...]`, `jsonb_out`'s own `", "` separator, no
/// nested top-level commas outside balanced brackets/strings) — just enough
/// to pull `items::text` apart into per-element text for the set comparison
/// above, without reaching for a full JSON parsing dependency in a test.
fn serde_json_array_elements(array_text: &str) -> Vec<String> {
    let inner = array_text
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .expect("must be a top-level jsonb array");
    if inner.is_empty() {
        return Vec::new();
    }
    let mut elements = Vec::new();
    let mut depth = 0i32;
    let mut in_string = false;
    let mut start = 0usize;
    let bytes = inner.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'"' if !in_string => in_string = true,
            b'"' if in_string => {
                // A canonical jsonb string never ends in an odd number of
                // trailing backslashes right before this quote in a way
                // that would need lookback here, since escapes are always
                // two-character pairs consumed together below.
                in_string = false;
            }
            b'\\' if in_string => i += 1, // skip the escaped character
            b'{' | b'[' if !in_string => depth += 1,
            b'}' | b']' if !in_string => depth -= 1,
            b',' if !in_string && depth == 0 => {
                elements.push(inner[start..i].to_string());
                start = i + 2; // skip ", "
                i += 1;
            }
            _ => {}
        }
        i += 1;
    }
    elements.push(inner[start..].to_string());
    elements
}

#[test]
fn serde_json_array_elements_splits_a_canonical_jsonb_array() {
    assert_eq!(serde_json_array_elements("[]"), Vec::<String>::new());
    assert_eq!(
        serde_json_array_elements(r#"[1, "a, b", {"x": [1, 2]}, null]"#),
        vec!["1", "\"a, b\"", "{\"x\": [1, 2]}", "null"]
    );
}
