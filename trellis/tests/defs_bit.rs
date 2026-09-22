//! End-to-end tests for issue #118's `bit`/`bit varying` support — as
//! join/primary-key and `GROUP BY` key roles, and the two new aggregates
//! `bit_and`/`bit_or`. The typed-literal ("computed 1-1 target") role is
//! covered by `defs_typed_literals.rs`'s shared `CASES` table, which this
//! issue added a `VARBIT` row to.
//!
//! # Why these run against a live server
//!
//! The issue's own framing treats `bit`/`bit varying` as one homogeneous
//! family sharing one verdict per role. Asked of a real server, per
//! #111-#114/#119's playbook rather than assumed, that turns out to be
//! wrong in two independent ways:
//!
//! 1. **Unlike `boolean` (#119), neither bit-string type has a second,
//!    disagreeing `::text` renderer.** `select castfunc from pg_cast where
//!    castsource in ('bit'::regtype, 'varbit'::regtype) and casttarget =
//!    'text'::regtype` returns **no rows** — `::text` is `bit_out`/
//!    `varbit_out` directly, exactly like `oid`/`bytea`/most of the
//!    temporal families. So both types are safe, text-stable join/primary
//!    keys.
//! 2. **The two types split anyway, on a completely different axis: DDL
//!    typmod, not rendering.** `ValueType`/`PgType::Bit`/`PgType::VarBit`
//!    carry no length, and Postgres's *default* typmod for a bare
//!    **fixed-length** `bit` column or cast (no explicit length given) is
//!    `bit(1)` — a genuinely *narrowing* default unlike every other
//!    admitted family's unconstrained bare default (`numeric`, `text`,
//!    bare `bit varying` itself). Any role that needs Trellis to declare a
//!    **brand-new** column or cast from bare `ValueType` alone (a `GROUP
//!    BY` key column, an aggregate's result column, a typed literal) hits
//!    silent truncation or an outright write failure for fixed-length
//!    `bit`, verified live (`'101'::bit` truncates to `'1'`;
//!    `create table t(x bit); insert into t values ('101')` raises `bit
//!    string length 3 does not match type bit(1)`). A role that instead
//!    reuses an *already-existing* column's own concrete introspected type
//!    (a relationship join key's bound-array cast, a 1-1 primary key's
//!    `format_type`-sourced declaration) never hits this at all. `bit
//!    varying`'s bare default is genuinely unconstrained and has no such
//!    trap.
//!
//! `bit_and`/`bit_or` land on the safe side of split 2 by construction:
//! `registry::aggregate_result_type` always widens their result to
//! `Other(PgType::VarBit)`, never mirroring a `bit`-typed argument's own
//! family the way `MIN`/`MAX`/`SUM(interval)` mirror theirs elsewhere in the
//! same function — see that arm's own doc comment. They are still
//! **recompute-only**, on `bool_and`/`bool_or`'s reasoning generalized past
//! two possible values per column to `2^n`.
//!
//! Per ADR-0013 every comparison is against independently-authored SQL —
//! plain `pg_cast`, plain `bit_and`/`bit_or`, plain `to_jsonb`, plain `group
//! by` — never against `defs::oracle::recompute`, which would be the
//! engine's own renderer grading itself. Harness conventions follow
//! `defs_boolean.rs`/`defs_bytea.rs`.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::eval::{EvalError, RegexCache, Row, Value, evaluate_aggregate};
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

/// The column typing the no-DB validator tests below check against.
fn source_columns() -> HashMap<String, ValueType> {
    HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
        ("b".to_string(), ValueType::Other(PgType::Bit)),
        ("vb".to_string(), ValueType::Other(PgType::VarBit)),
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
// 1. Text-stability: no second renderer, unlike boolean
// ---------------------------------------------------------------------

/// Neither `bit` nor `bit varying` has a dedicated `::text` cast function —
/// unlike `boolean` (#119), `::text` is each type's own output function
/// (`bit_out`/`varbit_out`) directly. Contrasted with a spread of types
/// already admitted (and `boolean`, which is not) to confirm the probe
/// itself is discriminating.
#[tokio::test]
async fn bit_and_varbit_have_no_dedicated_text_cast_function() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    for pg_type in [
        "bit", "varbit", "smallint", "integer", "oid", "uuid", "bytea",
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
            "{pg_type} must have no dedicated ::text cast function"
        );
    }

    // `boolean` is the control: it *does* have one, so this probe would have
    // caught a false "no divergence" verdict for bit/varbit had one existed.
    let row = client
        .query_one(
            "select count(*) from pg_cast \
             where castsource = 'boolean'::regtype and casttarget = 'text'::regtype",
            &[],
        )
        .await
        .expect("pg_cast probe for boolean");
    let n: i64 = row.get(0);
    assert_eq!(n, 1, "boolean must still have its own ::text cast (#119)");
}

/// `to_jsonb` and `::text` must agree for both types — the render-agreement
/// half of the #113/#248 defect shape. Neither type is a datetime family
/// (the only families `to_jsonb` special-cases), so this is expected to
/// hold, but it is checked live rather than assumed.
#[tokio::test]
async fn to_jsonb_and_text_agree_for_bit_and_varbit() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table renderers (b bit(5), vb bit varying); \
             insert into renderers values ('10100'::bit(5), '0110'::varbit), \
                                           (null, null)",
        )
        .await
        .expect("seed renderers");

    let rows = client
        .query(
            "select b::text, (to_jsonb(renderers.*) ->> 'b'), \
                    vb::text, (to_jsonb(renderers.*) ->> 'vb') \
             from renderers",
            &[],
        )
        .await
        .expect("sweep renderers");
    assert_eq!(rows.len(), 2);
    for row in rows {
        let (b_text, b_jsonb): (Option<String>, Option<String>) = (row.get(0), row.get(1));
        let (vb_text, vb_jsonb): (Option<String>, Option<String>) = (row.get(2), row.get(3));
        assert_eq!(b_text, b_jsonb, "::text and to_jsonb must agree on bit");
        assert_eq!(
            vb_text, vb_jsonb,
            "::text and to_jsonb must agree on bit varying"
        );
    }
}

// ---------------------------------------------------------------------
// 2. Key roles: split by DDL typmod, not by rendering
// ---------------------------------------------------------------------

/// Both `bit(n)` and `bit varying` are accepted as a relationship join key
/// and, because both roles gate on the same
/// `catalog::TEXT_STABLE_JOIN_KEY_TYPES` allowlist, as a 1-1 primary key
/// too — neither role ever asks a bare `ValueType` to declare a new column
/// (a join-key lookup casts the bound array to the column's own concrete
/// introspected type; a 1-1 primary key copies that concrete type
/// verbatim), so the DDL-typmod hazard that keeps fixed-length `bit` off
/// the `GROUP BY` key role (below) never applies here.
#[tokio::test]
async fn bit_and_varbit_join_keys_and_primary_keys_are_admitted() {
    for (pg_decl, expected_pk_type) in [("bit(4)", "bit(4)"), ("bit varying", "bit varying")] {
        let cluster = TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let client = connect_raw(db.dsn()).await;

        client
            .batch_execute(&format!(
                "create table parent (k {pg_decl} primary key); \
                 create table child (id bigint primary key, k {pg_decl}); \
                 alter table child replica identity full; \
                 alter table parent replica identity full"
            ))
            .await
            .expect("create relationship tables");

        create_relationship(&db.pool, "RELATIONSHIP r FROM child.k TO parent.k")
            .await
            .unwrap_or_else(|e| panic!("a {pg_decl} column must be accepted as a join key: {e}"));

        let pk = source_primary_key(&db.pool, "parent")
            .await
            .unwrap_or_else(|e| {
                panic!("a single-column {pg_decl} primary key must be accepted: {e}")
            });
        assert_eq!(pk.len(), 1);
        assert_eq!(pk[0].data_type, expected_pk_type);
    }
}

/// The `GROUP BY` key gate admits `bit varying` — its bare default typmod
/// is genuinely unconstrained, so `ddl::create_aggregate_target_table`
/// declaring the key column from bare `ValueType` alone is safe.
#[test]
fn varbit_group_by_key_is_admitted() {
    let def =
        parse("TRANSFORM t FROM s GROUP BY vb SELECT vb AS k, SUM(n) AS total").expect("parses");
    validate(&def, &source_columns(), &HashMap::new())
        .unwrap_or_else(|e| panic!("bit varying must be accepted as a GROUP BY key: {e}"));
}

/// Fixed-length `bit` is refused as a `GROUP BY` key — a regression pin for
/// the DDL-typmod-loss reason (not a text-rendering one), distinct from
/// every other refusal on this gate. If a future edit widens the `VarBit`
/// arm to also cover `Bit` without fixing the underlying bare-`bit`-
/// defaults-to-`bit(1)` DDL trap, this is the test that should catch it.
#[test]
fn fixed_length_bit_group_by_key_is_refused() {
    let def =
        parse("TRANSFORM t FROM s GROUP BY b SELECT b AS k, SUM(n) AS total").expect("parses");
    let err = validate(&def, &source_columns(), &HashMap::new())
        .expect_err("fixed-length bit must still be refused as a GROUP BY key");
    let _ = err; // the specific ValidationError variant isn't load-bearing here
}

/// The concrete DDL trap the `Bit`/`VarBit` split above exists to avoid:
/// Postgres's own default typmod for a bare fixed-length `bit` cast is
/// `bit(1)`, so a wider literal is silently truncated rather than
/// rejected — the reason `render_sql`'s bare `'<text>'::bit` can never be
/// safe for a computed target, and the reason
/// `ddl::create_aggregate_target_table`'s bare-`ValueType` column
/// declaration can't be either. `bit varying` has no such trap.
#[tokio::test]
async fn fixed_length_bit_truncates_under_a_bare_cast_but_bit_varying_does_not() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let row = client
        .query_one("select ('101'::bit)::text, length('101'::bit)", &[])
        .await
        .expect("bare bit cast");
    let (truncated, len): (String, i32) = (row.get(0), row.get(1));
    assert_eq!(truncated, "1", "a bare ::bit cast must truncate to bit(1)");
    assert_eq!(len, 1);

    let row = client
        .query_one(
            "select ('101'::bit varying)::text, length('101'::bit varying)",
            &[],
        )
        .await
        .expect("bare bit varying cast");
    let (preserved, len): (String, i32) = (row.get(0), row.get(1));
    assert_eq!(
        preserved, "101",
        "a bare ::bit varying cast must not truncate"
    );
    assert_eq!(len, 3);
}

// ---------------------------------------------------------------------
// 3. MIN/MAX: no such Postgres aggregate, same shape as bytea
// ---------------------------------------------------------------------

/// Postgres has no `min(bit)`/`max(bit)`/`min(bit varying)`/
/// `max(bit varying)` aggregate, despite both types having a full,
/// `IMMUTABLE` btree opclass — the same shape #114 found for `bytea`, not a
/// rendering hazard.
#[tokio::test]
async fn postgres_has_no_min_max_aggregate_for_bit_or_varbit() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table t (b bit(3), vb bit varying); \
             insert into t values ('101'::bit(3), '101'::varbit)",
        )
        .await
        .expect("seed probe table");
    for (agg, col) in [("min", "b"), ("max", "b"), ("min", "vb"), ("max", "vb")] {
        let err = client
            .query_one(&format!("select {agg}({col}) from t"), &[])
            .await
            .expect_err(&format!("{agg}({col}) must not exist in Postgres"));
        let db_error = err
            .as_db_error()
            .unwrap_or_else(|| panic!("{agg}({col}) must fail as a DbError: {err}"));
        assert!(
            db_error.message().contains("does not exist"),
            "{agg}({col}): {db_error}"
        );
    }

    for name in ["MIN", "MAX"] {
        for pg_type in [PgType::Bit, PgType::VarBit] {
            assert!(
                registry::aggregate_result_type(name, ValueType::Other(pg_type)).is_none(),
                "{name}({pg_type}) must have no result type"
            );
        }
    }
}

// ---------------------------------------------------------------------
// 4. bit_and/bit_or: result types, folding, invertibility
// ---------------------------------------------------------------------

/// `bit_and`/`bit_or` both return `bit`, per `pg_typeof` — but Trellis's own
/// registry deliberately widens the *declared* result to `bit varying`
/// (checked against the registry, not `pg_typeof`, since that widening is a
/// Trellis-side DDL-safety decision `pg_typeof` cannot see: Postgres itself
/// only ever calls `bit_and(bit)`/`bit_or(bit)`, confirmed here too).
#[tokio::test]
async fn bit_and_or_result_types_match_pg_typeof_and_are_widened_to_varbit() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    for (agg, arg_sql) in [
        ("bit_and", "'101'::bit(3)"),
        ("bit_or", "'101'::bit(3)"),
        ("bit_and", "'101'::varbit"),
        ("bit_or", "'101'::varbit"),
    ] {
        let row = client
            .query_one(
                &format!(
                    "select format_type(pg_typeof({agg}(v)), null) from (values ({arg_sql})) t(v)"
                ),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("{agg}({arg_sql}) pg_typeof probe: {e}"));
        let ty: String = row.get(0);
        assert_eq!(
            ty, "bit",
            "postgres's own {agg} always returns the fixed-length bit family"
        );
    }

    for name in ["BIT_AND", "BIT_OR"] {
        for pg_type in [PgType::Bit, PgType::VarBit] {
            assert_eq!(
                registry::aggregate_result_type(name, ValueType::Other(pg_type)),
                Some(ValueType::Other(PgType::VarBit)),
                "{name}({pg_type}) must resolve to Other(VarBit), widened away from bare bit's \
                 bit(1) DDL trap"
            );
        }
    }
}

/// The evaluator's fold must match a server-side `bit_and`/`bit_or` over
/// every NULL-handling shape Postgres distinguishes: all-set, all-clear,
/// mixed, a NULL mixed in (skipped), and an all-NULL group (`NULL` result).
#[tokio::test]
async fn bit_and_or_fold_matches_a_server_side_aggregate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let groups: &[&[Option<&str>]] = &[
        &[Some("111"), Some("111"), Some("111")],
        &[Some("000"), Some("000")],
        &[Some("101"), Some("011"), Some("111")],
        &[Some("101"), None, Some("011")],
        &[None, None],
    ];

    for group in groups {
        let values_sql = group
            .iter()
            .map(|v| match v {
                Some(bits) => format!("('{bits}'::bit(3))"),
                None => "(null::bit(3))".to_string(),
            })
            .collect::<Vec<_>>()
            .join(",");
        let row = client
            .query_one(
                &format!(
                    "select bit_and(v)::text, bit_or(v)::text from (values {values_sql}) t(v)"
                ),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("server bit_and/bit_or over {group:?}: {e}"));
        let (server_and, server_or): (Option<String>, Option<String>) = (row.get(0), row.get(1));

        let def = parse(
            "TRANSFORM t FROM s GROUP BY id SELECT id AS k, \
             BIT_AND(b) AS all_bits, BIT_OR(b) AS any_bits",
        )
        .expect("parses");
        let columns = HashMap::from([
            ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
            ("b".to_string(), ValueType::Other(PgType::Bit)),
        ]);
        validate(&def, &columns, &HashMap::new()).expect("validate");

        let rows: Vec<Row> = group
            .iter()
            .map(|v| {
                Row::from([
                    ("id".to_string(), Some("1".to_string())),
                    ("b".to_string(), v.map(|s| s.to_string())),
                ])
            })
            .collect();
        let result = evaluate_aggregate(&def, &rows, &columns, &mut RegexCache::default())
            .unwrap_or_else(|e| panic!("bit_and/bit_or fold over {group:?}: {e}"));

        assert_eq!(
            result["all_bits"],
            server_and.map(|s| Value::Other(PgType::VarBit, s)),
            "BIT_AND over {group:?}"
        );
        assert_eq!(
            result["any_bits"],
            server_or.map(|s| Value::Other(PgType::VarBit, s)),
            "BIT_OR over {group:?}"
        );
    }
}

/// Postgres itself refuses to combine two differently-sized bit strings
/// (`cannot AND bit strings of different sizes`) rather than
/// padding/truncating either one — a hazard only an unconstrained `bit
/// varying` argument can trigger (every row of a fixed-length `bit(n)`
/// column shares the column's one width). The evaluator must reproduce the
/// same refusal, not silently pick a length — checked here by calling
/// `evaluate_aggregate` directly over a two-row group, the same
/// multi-row shape `defs::oracle::recompute_aggregate`'s test-only
/// cross-check uses. `EvalError::BitStringLengthMismatch`'s own doc
/// comment is the authoritative account of why this is *not* the shape a
/// live apply ever calls the evaluator with: `staging::apply_aggregate`'s
/// production caller (`row_contribution`) only ever passes one row at a
/// time, so a real mismatched-length group surfaces there as Postgres's
/// own native error (`ApplyError::Db`) instead, never this Rust variant.
#[tokio::test]
async fn bit_and_or_reject_mismatched_length_bit_varying_the_same_way_postgres_does() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let err = client
        .query_one(
            "select bit_and(v) from (values ('1'::bit varying), ('01'::bit varying)) t(v)",
            &[],
        )
        .await
        .expect_err("postgres must refuse differently-sized bit varying arguments");
    let db_error = err
        .as_db_error()
        .unwrap_or_else(|| panic!("must fail as a DbError: {err}"));
    assert!(db_error.message().contains("different sizes"), "{db_error}");

    let def = parse("TRANSFORM t FROM s GROUP BY id SELECT id AS k, BIT_AND(vb) AS all_bits")
        .expect("parses");
    let columns = HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
        ("vb".to_string(), ValueType::Other(PgType::VarBit)),
    ]);
    validate(&def, &columns, &HashMap::new()).expect("validate");
    let rows: Vec<Row> = vec![
        Row::from([
            ("id".to_string(), Some("1".to_string())),
            ("vb".to_string(), Some("1".to_string())),
        ]),
        Row::from([
            ("id".to_string(), Some("1".to_string())),
            ("vb".to_string(), Some("01".to_string())),
        ]),
    ];
    let err = evaluate_aggregate(&def, &rows, &columns, &mut RegexCache::default())
        .expect_err("the evaluator must refuse the mismatched lengths too");
    assert!(matches!(err, EvalError::BitStringLengthMismatch { .. }));
}

/// `bit_and`/`bit_or` are classified `RecomputeOnly` — pinned here as a
/// live-facing regression guard (the pure-code classification is already
/// pinned in `defs::invertibility`'s own unit tests).
#[test]
fn bit_and_or_are_classified_recompute_only() {
    for name in ["BIT_AND", "BIT_OR"] {
        let verdict = classify(name, AggregateArg::Column(ValueType::Other(PgType::Bit)))
            .unwrap_or_else(|| panic!("{name} must classify"));
        assert_eq!(verdict.invertibility, Invertibility::RecomputeOnly);
    }
}

/// The concrete reason `bit_and`/`bit_or` cannot be delta-maintained from
/// their own visible value alone: two different deletions from the *same*
/// starting group land at two different true answers, so "the old aggregate
/// was X" is not enough information to invert — `bool_and`/`bool_or`'s
/// `{false, true, true}` argument (#119), generalized from 2 possible
/// per-column values to `2^n`.
#[tokio::test]
async fn bit_and_or_deletion_cannot_be_inverted_from_the_aggregate_alone() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let row = client
        .query_one(
            "select bit_and(v)::text from \
             (values ('01'::bit(2)), ('10'::bit(2)), ('11'::bit(2))) t(v)",
            &[],
        )
        .await
        .expect("starting bit_and");
    let starting: String = row.get(0);
    assert_eq!(starting, "00", "the starting group must fold to 00");

    // Delete the `01` row: the remaining two fold to `10`.
    let row = client
        .query_one(
            "select bit_and(v)::text from (values ('10'::bit(2)), ('11'::bit(2))) t(v)",
            &[],
        )
        .await
        .expect("bit_and after deleting the 01 row");
    let after_deleting_01: String = row.get(0);

    // Instead delete the `10` row from the same starting group: the
    // remaining two fold to `01`.
    let row = client
        .query_one(
            "select bit_and(v)::text from (values ('01'::bit(2)), ('11'::bit(2))) t(v)",
            &[],
        )
        .await
        .expect("bit_and after deleting the 10 row");
    let after_deleting_10: String = row.get(0);

    assert_ne!(
        after_deleting_01, after_deleting_10,
        "two different deletions from a bit_and = 00 group of 3 must not converge to the \
         same post-delete answer — this is exactly why a running fold can't invert a delete"
    );
}

// ---------------------------------------------------------------------
// 5. Regression guard: a bit varying GROUP BY key seeded two ways
// ---------------------------------------------------------------------

/// A `bit varying` `GROUP BY` group touched once through an ordinary
/// image-bearing CDC change and once through a bare, image-less live
/// refetch must still land as **one** target row, not two — the #113/#248
/// defect shape. `bit varying` was never exposed to that defect (see
/// `to_jsonb_and_text_agree_for_bit_and_varbit` above), so this is a
/// regression guard rather than a reproduction of a live bug, the same
/// framing `defs_bytea.rs`'s equivalent test uses.
#[tokio::test]
async fn a_varbit_group_key_seeded_by_cdc_and_by_live_read_is_one_group_not_two() {
    const DEF_SQL: &str =
        "TRANSFORM totals FROM events GROUP BY grp SELECT grp AS grp, SUM(amount) AS total";

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table events ( \
               id integer primary key, grp bit varying, amount numeric); \
             alter table events replica identity full",
        )
        .await
        .expect("create source table");

    let columns = HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("grp".to_string(), ValueType::Other(PgType::VarBit)),
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
               (1, '101', 10), \
               (2, '101', 5)",
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
            "bit_worker",
            1,
            "trellis_defs_bit_issue_118_regression",
            &StagedWatermark::saturated(),
        )
        .await
        .expect("drain_once")
        .expect("drain_once must claim and drain something");
    }

    // Row 1: an ordinary image-bearing insert, the shape real CDC produces.
    stage_image(&client, "seg_0", "1", r#"{"grp":"101","amount":"10"}"#).await;
    // Row 2: a bare recompute trigger — no image at all — forcing
    // `read_live_rows_batch`'s live refetch to decode `grp` via
    // `row_as_text_jsonb_sql`'s `<col>::text` cast.
    stage_bare_recompute(&client, "seg_0", "2").await;

    drain_sealed(&mut client, &db.pool).await;

    let rows = client
        .query("select grp::text, total::text from totals", &[])
        .await
        .expect("read totals");
    assert_eq!(
        rows.len(),
        1,
        "one Postgres GROUP BY group must land as one target row, not two; got {rows:?}",
        rows = rows
            .iter()
            .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
            .collect::<Vec<_>>()
    );
    let (got_grp, got_total): (String, String) = (rows[0].get(0), rows[0].get(1));
    assert_eq!(got_grp, "101");
    assert_eq!(got_total, "15", "the group's total must be 10 + 5");

    // Cross-check against an independently-authored recompute (ADR-0013).
    let expected: Vec<(String, String)> = client
        .query(
            "select grp::text, sum(amount)::text from events group by grp",
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
