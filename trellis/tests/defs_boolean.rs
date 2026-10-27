//! End-to-end tests for `boolean` support as a join/`GROUP BY`
//! key. The `bool_and`/`bool_or` aggregates are refused at define
//! (`ValidationError::DeferredAggregate`).
//!
//! # Why these run against a live server
//!
//! `boolean` looks like the obvious easy case:
//! `boolout` renders exactly two values (`'t'`/`'f'`), a textbook
//! bijection, so on the pattern the other type families follow ("text-stability
//! is a property of the rendering, not the operator set") it reads like a
//! straight allowlist addition, the simplest of the type families.
//! Asked of a real server rather than assumed, that turns out to be
//! wrong in a genuinely new way — not "no typed index needed" and not
//! "needs one", but a *third* shape none
//! of the other type families had reason to find:
//!
//! 1. **`boolean` has two independent, disagreeing output paths**, and nothing
//!    about the value's own equality is at issue. `boolout` (what a capture
//!    trigger images and what a raw fetch renders) spells a boolean
//!    `'t'`/`'f'`. `<col>::text` does **not** call `boolout` — Postgres ships
//!    a second, dedicated `pg_cast` row (`pg_catalog.text(boolean)`) that
//!    overrides the cast to spell it `'true'`/`'false'` instead. Every other
//!    type `join_key_types::TEXT_STABLE_JOIN_KEY_TYPES` admits has *no* such
//!    override — its `::text` cast just calls its own output function — so
//!    `boolean` is the only type on the epic's board with a second renderer
//!    at all.
//! 2. **That divergence is real, not theoretical, for the join/primary-key
//!    role**: capture stores a key's `boolout`
//!    spelling verbatim, and several of `staging::apply`'s scalar key
//!    lookups compare it against a live column's `{col}::text` rendering —
//!    two different strings for one value, silently never matching. This is
//!    why `boolean` is *not* added to `join_key_types::TEXT_STABLE_JOIN_KEY_TYPES`
//!    here — seeing this test file, this is a deliberate, documented
//!    non-change, not an oversight. See `catalog.rs`'s own doc comment on
//!    that constant for the full account, including which call sites are
//!    and are not exposed (`key_array_filter` already sidesteps
//!    it for its own bulk lookups).
//! 3. **The `GROUP BY` key role's *final SQL* half was already safe despite
//!    the same divergence**, because the old aggregate path's keyset
//!    match bound the group key as a native-typed array
//!    (`$1::text[]::boolean[]`), which parses *either* spelling back to the
//!    same value via `boolin` — a permissive input function — before ever
//!    comparing. That is why `validate::reject_unsupported_group_by_key_type`
//!    already admitted `ValueType::Boolean` before this issue.
//! 4. **...but that in-memory-only left a second, genuinely live bug this
//!    issue found and fixed**: the old path bucketed one drain
//!    batch's touched rows by the group key's own `text` — a bare Rust `HashMap` key, compared byte-for-byte with
//!    no database (and so no `boolin`) anywhere in the loop. A row that
//!    arrived with the `'t'` spelling and one that arrived with the
//!    `'true'` spelling used to land in *two* separate groups that
//!    both independently wrote to the one row the SQL layer correctly
//!    resolved them to — silently corrupting its value rather than visibly
//!    splitting it into two rows the way a `timestamp` key once did.
//!    Canonicalizing a `Boolean` key part closed that gap;
//!    `a_boolean_group_key_seeded_by_cdc_and_by_live_read_is_one_group_not_two`
//!    below reproduces the corruption end-to-end and pins the fix.
//!
//! Per ADR-0013 every comparison is against independently-authored SQL —
//! plain `pg_cast`, plain `boolout`, plain `group by` — never against
//! `defs::oracle::recompute`, which would be the engine's own renderer
//! grading itself. Harness conventions follow
//! `defs_bytea.rs`.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::{
    CatalogError, ValidationError, create_aggregate_target_table, create_definition,
    install_definition, parse, source_primary_key, validate,
};
use trellis::integer::IntWidth;
use trellis::staging::{apply, seal};

// ---------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------

/// The column typing the no-DB validator tests below check against.
fn source_columns() -> HashMap<String, ValueType> {
    HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
        ("flag".to_string(), ValueType::Boolean),
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
) -> Result<(), CatalogError> {
    let text = format!(
        "TRANSFORM {target} FROM {left} JOIN {right} ON {left}.{lcol} = {right}.{rcol} \
         GROUP BY {left}.id WITHOUT SOURCE INDEX SELECT COUNT(*) AS n"
    );
    install_definition(pool, &text, &HashMap::new(), "public")
        .await
        .map(|_| ())
}

// ---------------------------------------------------------------------
// 1. The renderer divergence itself
// ---------------------------------------------------------------------

/// `boolean` is the only type this epic has admitted anywhere whose
/// `::text` cast is not its own output function — verified directly off
/// `pg_cast` rather than inferred, and contrasted with a spread of types
/// already on `join_key_types::TEXT_STABLE_JOIN_KEY_TYPES` to confirm none of them
/// share the hazard.
#[tokio::test]
async fn boolean_is_the_only_admitted_family_with_a_dedicated_text_cast() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let row = client
        .query_one(
            "select castfunc::regproc::text from pg_cast \
             where castsource = 'boolean'::regtype and casttarget = 'text'::regtype",
            &[],
        )
        .await
        .expect("boolean must have its own text() cast function");
    let castfunc: String = row.get(0);
    assert_eq!(
        castfunc, "pg_catalog.text",
        "boolean's ::text cast must be pg_catalog.text(boolean), not its output function"
    );

    for pg_type in [
        "smallint", "integer", "bigint", "oid", "uuid", "text", "date", "bytea",
    ] {
        let row = client
            .query_one(
                &format!(
                    "select count(*) from pg_cast \
                     where castsource = '{pg_type}'::regtype and casttarget = 'text'::regtype"
                ),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("pg_cast probe for {pg_type}: {e}"));
        let n: i64 = row.get(0);
        assert_eq!(
            n, 0,
            "{pg_type} must have no dedicated ::text cast function (its ::text must be its \
             own output function), unlike boolean"
        );
    }
}

/// The concrete consequence of the cast override: `boolout` (what CDC
/// decodes) and `<col>::text` (what Trellis's own renderers use) spell the
/// same two values two different ways. Demonstrated over a real table
/// column, not a bare literal, since it is a live column's cast Trellis's
/// SQL actually runs.
#[tokio::test]
async fn boolout_and_the_text_cast_disagree_on_a_live_column() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute("create table t (v boolean); insert into t values (true), (false)")
        .await
        .expect("seed table");

    let rows = client
        .query(
            "select boolout(v)::text as via_output_func, v::text as via_cast from t order by v",
            &[],
        )
        .await
        .expect("sweep t");
    assert_eq!(rows.len(), 2);
    let mut seen = Vec::new();
    for row in rows {
        let (via_output_func, via_cast): (String, String) = (row.get(0), row.get(1));
        assert_ne!(
            via_output_func, via_cast,
            "boolout and ::text must disagree on every boolean value"
        );
        seen.push((via_output_func, via_cast));
    }
    assert_eq!(
        seen,
        vec![
            ("f".to_string(), "false".to_string()),
            ("t".to_string(), "true".to_string()),
        ]
    );
}

// ---------------------------------------------------------------------
// 2. Join key / primary key role: still refused, deliberately
// ---------------------------------------------------------------------

/// `boolean` must be refused as a `JOIN` `ON` key and as a 1-1
/// primary key — the join/PK-key roles gate on the same
/// `join_key_types::TEXT_STABLE_JOIN_KEY_TYPES` allowlist, and `boolean` is not on
/// it (see `catalog.rs`'s doc comment and this file's module doc for why).
/// A regression pin: if a future edit adds `"boolean"` to that list without
/// also fixing the scalar key-lookup call sites in `staging::apply` that
/// still do raw `{col}::text = $1` matching, this is the test that should
/// catch it turning `Ok`.
#[tokio::test]
async fn boolean_is_refused_as_a_join_key_and_primary_key() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table parent (k boolean primary key); \
             create table child (id bigint primary key, k boolean)",
        )
        .await
        .expect("create join tables");

    let err = define_join_on(&db.pool, "j", ("child", "k"), ("parent", "k"))
        .await
        .expect_err("a boolean column must still be refused as a join key");
    assert!(
        matches!(
            &err,
            CatalogError::Validate(ValidationError::JoinUnsupportedKeyType { pg_type, .. })
                if pg_type == "boolean"
        ),
        "{err:?}"
    );

    let err = source_primary_key(&db.pool, "parent")
        .await
        .expect_err("a single-column boolean primary key must still be refused");
    let _ = err;
}

/// The `GROUP BY` key gate (`validate::reject_unsupported_group_by_key_type`)
/// admits `boolean` — long-standing behavior,
/// pinned here so a future edit can't accidentally tighten it without a
/// test noticing, the same convention `defs_bytea.rs`'s
/// `bytea_group_by_key_is_admitted` and `defs_temporal.rs`'s
/// `the_group_by_key_gate_follows_the_same_split` follow.
#[test]
fn boolean_group_by_key_is_admitted() {
    let def = parse(
        "TRANSFORM t FROM s GROUP BY flag WITHOUT SOURCE INDEX SELECT flag AS k, SUM(n) AS total",
    )
    .expect("parses");
    validate(&def, &source_columns())
        .unwrap_or_else(|e| panic!("boolean must be accepted as a GROUP BY key: {e}"));
}

// ---------------------------------------------------------------------
// 3. The two-spellings regression shape, reproduced for a boolean GROUP BY key
// ---------------------------------------------------------------------

/// A `boolean` `GROUP BY` group touched once through an ordinary
/// image-bearing CDC change (whose staged image text carries `boolout`'s
/// `'t'`/`'f'` spelling, exactly as a real capture trigger renders it)
/// and once through a bare, image-less live refetch (whose text comes from
/// `staging::apply::row_as_text_jsonb_sql`'s `<col>::text` cast, `'true'`/
/// `'false'`) must still land as **one** target row, not two — proving the
/// `GROUP BY` key role really is safe despite the renderer divergence this
/// file's other tests establish.
///
/// This is the positive counterpart to
/// `boolean_is_refused_as_a_join_key_and_primary_key`: the
/// same underlying two-renderer fact, safe here and unsafe there, for a
/// mechanical reason pinned by both tests together.
#[tokio::test]
async fn a_boolean_group_key_seeded_by_cdc_and_by_live_read_is_one_group_not_two() {
    const DEF_SQL: &str = "TRANSFORM totals FROM events GROUP BY grp WITHOUT SOURCE INDEX SELECT grp AS grp, SUM(amount) AS total";

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table events ( \
               id integer primary key, grp boolean, amount numeric(12,0))",
        )
        .await
        .expect("create source table");

    let columns = HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("grp".to_string(), ValueType::Boolean),
        ("amount".to_string(), ValueType::Numeric),
    ]);
    let def = parse(DEF_SQL).expect("parse");
    validate(&def, &columns).expect("validate");
    create_definition(&db.pool, DEF_SQL, &columns)
        .await
        .expect("create definition");
    create_aggregate_target_table(&db.pool, &def, "public", &columns)
        .await
        .expect("create aggregate target table");

    client
        .batch_execute(
            "insert into events (id, grp, amount) values \
               (1, true, 10), \
               (2, true, 5)",
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
                &[
                    &src_table,
                    &key,
                    &testkit::wal_insert_lsn(client).await,
                    &new_image,
                ],
            )
            .await
            .unwrap_or_else(|e| panic!("stage image-bearing {key}: {e}"));
    }

    async fn stage_bare_recompute(client: &Client, segment: &str, key: &str) {
        let src_table = format!("{DEFAULT_SCHEMA}.events");
        client
            .execute(
                &format!(
                    "insert into {segment} \
                     (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                     values ($1, $2, 'recompute', null, null, null, 0)"
                ),
                &[&src_table, &key],
            )
            .await
            .unwrap_or_else(|e| panic!("stage bare recompute {key}: {e}"));
    }

    async fn drain_sealed(client: &mut Client, pool: &trellis::Pool) {
        let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
        seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
            .await
            .expect("seal phase 2");
        apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "boolean_worker",
            1,
            "trellis_defs_boolean_issue_119_regression",
        )
        .await
        .expect("drain_once")
        .expect("drain_once must claim and drain something");
    }

    // Row 1: an ordinary image-bearing insert, staged with the literal
    // spelling a capture trigger images — `boolout`'s `'t'`,
    // not the SQL cast's `'true'`.
    stage_image(&client, "seg_0", "1", r#"{"grp":"t","amount":"10"}"#).await;
    // Row 2: a bare recompute trigger — no image at all — forcing
    // `live_rows_query`'s live refetch to decode `grp` via
    // `row_as_text_jsonb_sql`'s `<col>::text` cast, which spells the very
    // same value `'true'`.
    stage_bare_recompute(&client, "seg_0", "2").await;

    drain_sealed(&mut client, &db.pool).await;

    let rows = client
        .query("select grp, total::text from totals", &[])
        .await
        .expect("read totals");
    assert_eq!(
        rows.len(),
        1,
        "one Postgres GROUP BY group must land as one target row, not two; got {rows:?}",
        rows = rows
            .iter()
            .map(|r| (r.get::<_, bool>(0), r.get::<_, String>(1)))
            .collect::<Vec<_>>()
    );
    let (got_grp, got_total): (bool, String) = (rows[0].get(0), rows[0].get(1));
    assert!(got_grp, "the group key must be true");
    assert_eq!(got_total, "15", "the group's total must be 10 + 5");

    // Cross-check against an independently-authored recompute (ADR-0013).
    let expected: Vec<(bool, String)> = client
        .query(
            "select grp, sum(amount)::text from events group by grp",
            &[],
        )
        .await
        .expect("hand-written recompute")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        expected.len(),
        1,
        "the source itself has exactly one GROUP BY group"
    );
    assert_eq!(expected[0], (got_grp, got_total));
}
