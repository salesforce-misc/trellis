//! End-to-end tests for issue #115's `jsonb` support: the passthrough gap
//! check, the join/primary/`GROUP BY` key refusal, the typed-literal
//! ("computed 1-1 target") role (covered by `defs_typed_literals.rs`'s
//! shared `CASES` table, which this issue added a `JSONB` row to). The
//! `jsonb_agg` aggregate is refused at define
//! (`ValidationError::DeferredAggregate`).
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
//! plain `pg_cast`, plain `pg_proc`, plain `=` — never
//! against `defs::oracle::recompute`, which would be the engine's own
//! renderer grading itself. Harness conventions follow
//! `defs_bit.rs`/`defs_boolean.rs`.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::pg_type::PgType;
use trellis::defs::{
    CatalogError, ValidationError, install_definition, parse, registry, source_primary_key,
    validate,
};
use trellis::integer::IntWidth;

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

/// Defines a `JOIN` whose one `ON` term pairs `left.lcol` with
/// `right.rcol`. `JOIN` key admission reads the same allowlist
/// (`join_key_types::is_text_stable_join_key_type`) a 1-1 primary key does.
async fn define_join_on(
    pool: &trellis::pool::Pool,
    target: &str,
    (left, lcol): (&str, &str),
    (right, rcol): (&str, &str),
) -> Result<(), trellis::defs::CatalogError> {
    let text = format!(
        "TRANSFORM {target} FROM {left} JOIN {right} ON {left}.{lcol} = {right}.{rcol} \
         GROUP BY {left}.id WITHOUT SOURCE INDEX SELECT COUNT(*) AS n"
    );
    install_definition(pool, &text, &HashMap::new(), "public")
        .await
        .map(|_| ())
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
/// `join_key_types::TEXT_STABLE_JOIN_KEY_TYPES` despite clearing the key-order bar.
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

/// A `jsonb` column is refused as a `JOIN` `ON` key and as a 1-1
/// primary key — the live-facing regression pin for
/// `join_key_types::TEXT_STABLE_JOIN_KEY_TYPES`'s deliberate `jsonb` absence.
#[tokio::test]
async fn jsonb_is_refused_as_a_join_key_and_primary_key() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table parent (k jsonb primary key); \
             create table child (id bigint primary key, k jsonb)",
        )
        .await
        .expect("create join tables");

    let err = define_join_on(&db.pool, "j", ("child", "k"), ("parent", "k"))
        .await
        .expect_err("a jsonb column must be refused as a join key");
    assert!(
        matches!(
            &err,
            CatalogError::Validate(ValidationError::JoinUnsupportedKeyType { pg_type, .. })
                if pg_type == "jsonb"
        ),
        "{err:?}"
    );

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
        parse("TRANSFORM t FROM s GROUP BY j WITHOUT SOURCE INDEX SELECT j AS k, SUM(n) AS total")
            .expect("parses");
    validate(&def, &source_columns()).expect_err("jsonb must still be refused as a GROUP BY key");
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
