//! End-to-end tests for issue #119's `boolean` support — as a join/`GROUP
//! BY` key, and the two new aggregates `bool_and`/`bool_or`.
//!
//! # Why these run against a live server
//!
//! Issue #119's own scope note treats `boolean` as the obvious easy case:
//! `boolout` renders exactly two values (`'t'`/`'f'`), a textbook
//! bijection, so on the pattern #111/#113/#114 established ("text-stability
//! is a property of the rendering, not the operator set") it reads like a
//! straight allowlist addition, the simplest of the epic's type families so
//! far. Asked of a real server rather than assumed, that turns out to be
//! wrong in a genuinely new way — not "no typed index needed" (#111/#113/
//! #114's finding) and not "needs one" (#112's), but a *third* shape none
//! of the previous four type-family issues had reason to find:
//!
//! 1. **`boolean` has two independent, disagreeing output paths**, and nothing
//!    about the value's own equality is at issue. `boolout` (what CDC/
//!    `pgoutput` decodes and what a raw fetch renders) spells a boolean
//!    `'t'`/`'f'`. `<col>::text` does **not** call `boolout` — Postgres ships
//!    a second, dedicated `pg_cast` row (`pg_catalog.text(boolean)`) that
//!    overrides the cast to spell it `'true'`/`'false'` instead. Every other
//!    type `catalog::TEXT_STABLE_JOIN_KEY_TYPES` admits has *no* such
//!    override — its `::text` cast just calls its own output function — so
//!    `boolean` is the only type on the epic's board with a second renderer
//!    at all.
//! 2. **That divergence is real, not theoretical, for the join/primary-key
//!    role**: `intake::extract_key` stores a CDC-decoded key's `boolout`
//!    spelling verbatim, and several of `staging::apply`'s scalar key
//!    lookups compare it against a live column's `{col}::text` rendering —
//!    two different strings for one value, silently never matching. This is
//!    why `boolean` is *not* added to `catalog::TEXT_STABLE_JOIN_KEY_TYPES`
//!    here — seeing this test file, this is a deliberate, documented
//!    non-change, not an oversight. See `catalog.rs`'s own doc comment on
//!    that constant for the full account, including which call sites are
//!    and are not exposed (issue #125's `key_array_filter` already sidesteps
//!    it for its own bulk lookups).
//! 3. **The `GROUP BY` key role's *final SQL* half was already safe despite
//!    the same divergence**, because `staging::apply_aggregate`'s keyset
//!    match always binds the group key as a native-typed array
//!    (`$1::text[]::boolean[]`), which parses *either* spelling back to the
//!    same value via `boolin` — a permissive input function — before ever
//!    comparing. That is why `validate::reject_unsupported_group_by_key_type`
//!    already admitted `ValueType::Boolean` before this issue.
//! 4. **...but that in-memory-only left a second, genuinely live bug this
//!    issue found and fixed**: `accumulate_changes` buckets one drain
//!    batch's touched rows into `GroupPlan`s keyed by `derive_group_key`'s
//!    own `text` — a bare Rust `HashMap` key, compared byte-for-byte with
//!    no database (and so no `boolin`) anywhere in the loop. A row that
//!    arrived with the `'t'` spelling and one that arrived with the
//!    `'true'` spelling used to land in *two* separate `GroupPlan`s that
//!    both independently wrote to the one row the SQL layer correctly
//!    resolved them to — silently corrupting its value rather than visibly
//!    splitting it into two rows the way #248's pre-fix `timestamp` did.
//!    `apply_aggregate::canonicalize_group_key_part` (a no-op for every
//!    type but `Boolean`) closes that gap;
//!    `a_boolean_group_key_seeded_by_cdc_and_by_live_read_is_one_group_not_two`
//!    below reproduces the corruption end-to-end and pins the fix.
//!
//! `bool_and`/`bool_or` themselves are total, commutative, associative
//! folds over `{true, false}` with no overflow/rounding hazard — but they
//! are still **recompute-only**, on `MIN`/`MAX`'s reasoning rather than
//! `SUM`'s: the aggregate's only maintained state is its own current
//! one-bit value, which is not enough to invert a delete (deleting *some*
//! `false` row from a `bool_and = false` group might flip it to `true`, or
//! might not, depending on which row). See
//! `defs::invertibility::classify`'s `BOOL_AND`/`BOOL_OR` arm for the full
//! reasoning; `bool_and_or_deletion_cannot_be_inverted_from_the_aggregate_alone`
//! below demonstrates it live.
//!
//! Per ADR-0013 every comparison is against independently-authored SQL —
//! plain `pg_cast`, plain `boolout`, plain `bool_and`/`bool_or`, plain
//! `group by` — never against `defs::oracle::recompute`, which would be the
//! engine's own renderer grading itself. Harness conventions follow
//! `defs_bytea.rs`.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::eval::{RegexCache, Row, Value, evaluate_aggregate};
use trellis::defs::invertibility::{AggregateArg, Invertibility, classify};
use trellis::defs::{
    create_aggregate_target_table, create_definition, create_relationship, parse, registry,
    source_primary_key, validate,
};
use trellis::integer::IntWidth;
use trellis::staging::{StagedWatermark, apply, seal};

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

// ---------------------------------------------------------------------
// 1. The renderer divergence itself
// ---------------------------------------------------------------------

/// `boolean` is the only type this epic has admitted anywhere whose
/// `::text` cast is not its own output function — verified directly off
/// `pg_cast` rather than inferred, and contrasted with a spread of types
/// already on `catalog::TEXT_STABLE_JOIN_KEY_TYPES` to confirm none of them
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

/// `boolean` must be refused as a relationship join key and as a 1-1
/// primary key — the join/PK-key roles gate on the same
/// `catalog::TEXT_STABLE_JOIN_KEY_TYPES` allowlist, and `boolean` is not on
/// it (see `catalog.rs`'s doc comment and this file's module doc for why).
/// A regression pin: if a future edit adds `"boolean"` to that list without
/// also fixing the scalar key-lookup call sites in `staging::apply` that
/// still do raw `{col}::text = $1` matching, this is the test that should
/// catch it turning `Ok`.
#[tokio::test]
async fn boolean_is_refused_as_a_relationship_join_key_and_primary_key() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table parent (k boolean primary key); \
             create table child (id bigint primary key, k boolean); \
             alter table child replica identity full; \
             alter table parent replica identity full",
        )
        .await
        .expect("create relationship tables");

    let err = create_relationship(&db.pool, "RELATIONSHIP r FROM child.k TO parent.k")
        .await
        .expect_err("a boolean column must still be refused as a join key");
    let _ = err; // the specific CatalogError variant isn't load-bearing here

    let err = source_primary_key(&db.pool, "parent")
        .await
        .expect_err("a single-column boolean primary key must still be refused");
    let _ = err;
}

/// The `GROUP BY` key gate (`validate::reject_unsupported_group_by_key_type`)
/// admits `boolean` — pre-existing behavior (this arm predates issue #119),
/// pinned here so a future edit can't accidentally tighten it without a
/// test noticing, the same convention `defs_bytea.rs`'s
/// `bytea_group_by_key_is_admitted` and `defs_temporal.rs`'s
/// `the_group_by_key_gate_follows_the_same_split` follow.
#[test]
fn boolean_group_by_key_is_admitted() {
    let def = parse("TRANSFORM t FROM s GROUP BY flag SELECT flag AS k, SUM(n) AS total")
        .expect("parses");
    validate(&def, &source_columns(), &HashMap::new())
        .unwrap_or_else(|e| panic!("boolean must be accepted as a GROUP BY key: {e}"));
}

// ---------------------------------------------------------------------
// 3. bool_and/bool_or: result types, folding, invertibility
// ---------------------------------------------------------------------

/// `bool_and`/`bool_or` both return `boolean` — checked against
/// `pg_typeof`, not assumed, per ADR-0013.
#[tokio::test]
async fn bool_and_or_result_types_match_pg_typeof() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    for agg in ["bool_and", "bool_or"] {
        let row = client
            .query_one(
                &format!("select pg_typeof({agg}(v))::text from (values (true), (false)) t(v)"),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("{agg} pg_typeof probe: {e}"));
        let ty: String = row.get(0);
        assert_eq!(ty, "boolean");
    }

    for name in ["BOOL_AND", "BOOL_OR"] {
        assert_eq!(
            registry::aggregate_result_type(name, ValueType::Boolean),
            Some(ValueType::Boolean),
            "{name}(boolean) must resolve to Boolean"
        );
    }
}

/// The evaluator's fold must match a server-side `bool_and`/`bool_or` over
/// every NULL-handling shape Postgres distinguishes: all-true, all-false,
/// mixed, a NULL mixed in (skipped, matching every other aggregate's
/// "aggregate of non-NULL values" rule), and an all-NULL group (`NULL`
/// result, not `false`/`true`).
#[tokio::test]
async fn bool_and_or_fold_matches_a_server_side_aggregate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let groups: &[&[Option<bool>]] = &[
        &[Some(true), Some(true), Some(true)],
        &[Some(false), Some(false)],
        &[Some(true), Some(false), Some(true)],
        &[Some(true), None, Some(false)],
        &[None, None],
    ];

    for group in groups {
        let values_sql = group
            .iter()
            .map(|v| match v {
                Some(true) => "(true)".to_string(),
                Some(false) => "(false)".to_string(),
                None => "(null::boolean)".to_string(),
            })
            .collect::<Vec<_>>()
            .join(",");
        let row = client
            .query_one(
                &format!("select bool_and(v), bool_or(v) from (values {values_sql}) t(v)"),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("server bool_and/bool_or over {group:?}: {e}"));
        let (server_and, server_or): (Option<bool>, Option<bool>) = (row.get(0), row.get(1));

        let def = parse(
            "TRANSFORM t FROM s GROUP BY id SELECT id AS k, \
             BOOL_AND(flag) AS all_true, BOOL_OR(flag) AS any_true",
        )
        .expect("parses");
        let columns = HashMap::from([
            ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
            ("flag".to_string(), ValueType::Boolean),
        ]);
        validate(&def, &columns, &HashMap::new()).expect("validate");

        let rows: Vec<Row> = group
            .iter()
            .map(|v| {
                Row::from([
                    ("id".to_string(), Some("1".to_string())),
                    ("flag".to_string(), v.map(|b| b.to_string())),
                ])
            })
            .collect();
        let result = evaluate_aggregate(&def, &rows, &columns, &mut RegexCache::default())
            .unwrap_or_else(|e| panic!("bool_and/bool_or fold over {group:?}: {e}"));

        assert_eq!(
            result["all_true"],
            server_and.map(Value::Boolean),
            "BOOL_AND over {group:?}"
        );
        assert_eq!(
            result["any_true"],
            server_or.map(Value::Boolean),
            "BOOL_OR over {group:?}"
        );
    }
}

/// `bool_and`/`bool_or` are classified `RecomputeOnly` — pinned here as a
/// live-facing regression guard (the pure-code classification is already
/// pinned in `defs::invertibility`'s own unit tests).
#[test]
fn bool_and_or_are_classified_recompute_only() {
    for name in ["BOOL_AND", "BOOL_OR"] {
        let verdict = classify(name, AggregateArg::Column(ValueType::Boolean))
            .unwrap_or_else(|| panic!("{name} must classify"));
        assert_eq!(verdict.invertibility, Invertibility::RecomputeOnly);
    }
}

/// The concrete reason `bool_and`/`bool_or` cannot be delta-maintained from
/// their own visible value alone (`defs::invertibility`'s classification):
/// two different deletions from the *same* starting group, both leaving a
/// `bool_and = false` state with two rows remaining, land at two different
/// true answers after the delete — so "the old aggregate was `false`" is not
/// enough information to invert. A one-bit delta model literally cannot
/// distinguish the two cases; only a full recompute (or hidden per-value
/// counts this engine does not currently maintain — see
/// `defs::invertibility`'s doc comment) can.
#[tokio::test]
async fn bool_and_or_deletion_cannot_be_inverted_from_the_aggregate_alone() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let row = client
        .query_one(
            "select bool_and(v) from (values (false), (true), (true)) t(v)",
            &[],
        )
        .await
        .expect("starting bool_and");
    let starting: bool = row.get(0);
    assert!(!starting, "the starting group must fold to false");

    // Delete the `false` row: the remaining two are both `true`.
    let row = client
        .query_one("select bool_and(v) from (values (true), (true)) t(v)", &[])
        .await
        .expect("bool_and after deleting the false row");
    let after_deleting_false: bool = row.get(0);

    // Instead delete one of the *other* (`true`) rows from the same
    // starting group: the `false` row survives, so the fold is still
    // `false`.
    let row = client
        .query_one("select bool_and(v) from (values (false), (true)) t(v)", &[])
        .await
        .expect("bool_and after deleting a true row");
    let after_deleting_true: bool = row.get(0);

    assert_ne!(
        after_deleting_false, after_deleting_true,
        "two different deletions from a bool_and = false group of 3 must not converge to the \
         same post-delete answer — this is exactly why a one-bit delta can't invert a delete"
    );
}

// ---------------------------------------------------------------------
// 4. The #113/#248 regression shape, reproduced for a boolean GROUP BY key
// ---------------------------------------------------------------------

/// A `boolean` `GROUP BY` group touched once through an ordinary
/// image-bearing CDC change (whose staged image text carries `boolout`'s
/// `'t'`/`'f'` spelling, exactly as real logical decoding would produce)
/// and once through a bare, image-less live refetch (whose text comes from
/// `staging::apply::row_as_text_jsonb_sql`'s `<col>::text` cast, `'true'`/
/// `'false'`) must still land as **one** target row, not two — proving the
/// `GROUP BY` key role really is safe despite the renderer divergence this
/// file's other tests establish, because `staging::apply_aggregate`'s
/// keyset match re-parses both spellings through `boolin` before comparing
/// natively rather than matching either spelling as raw text.
///
/// This is the positive counterpart to
/// `boolean_is_refused_as_a_relationship_join_key_and_primary_key`: the
/// same underlying two-renderer fact, safe here and unsafe there, for a
/// mechanical reason pinned by both tests together.
#[tokio::test]
async fn a_boolean_group_key_seeded_by_cdc_and_by_live_read_is_one_group_not_two() {
    const DEF_SQL: &str =
        "TRANSFORM totals FROM events GROUP BY grp SELECT grp AS grp, SUM(amount) AS total";

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table events ( \
               id integer primary key, grp boolean, amount numeric); \
             alter table events replica identity full",
        )
        .await
        .expect("create source table");

    let columns = HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("grp".to_string(), ValueType::Boolean),
        ("amount".to_string(), ValueType::Numeric),
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
            &StagedWatermark::saturated(),
        )
        .await
        .expect("drain_once")
        .expect("drain_once must claim and drain something");
    }

    // Row 1: an ordinary image-bearing insert, staged with the literal
    // spelling real CDC/`pgoutput` decoding produces — `boolout`'s `'t'`,
    // not the SQL cast's `'true'`.
    stage_image(&client, "seg_0", "1", r#"{"grp":"t","amount":"10"}"#).await;
    // Row 2: a bare recompute trigger — no image at all — forcing
    // `read_live_rows_batch`'s live refetch to decode `grp` via
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
