//! End-to-end tests for `bit`/`bit varying` support as
//! join/primary-key and `GROUP BY` key roles. The `bit_and`/`bit_or`
//! aggregates are refused at define (`ValidationError::DeferredAggregate`).
//! The typed-literal ("computed 1-1 target") role is
//! covered by `defs_typed_literals.rs`'s shared `CASES` table, which has a
//! `VARBIT` row.
//!
//! # Why these run against a live server
//!
//! It is tempting to treat `bit`/`bit varying` as one homogeneous
//! family sharing one verdict per role. Asked of a real server rather than
//! assumed, that turns out to be
//! wrong in two independent ways:
//!
//! 1. **Unlike `boolean`, neither bit-string type has a second,
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
//!    (a `JOIN` key's bound-array cast, a 1-1 primary key's
//!    `format_type`-sourced declaration) never hits this at all. `bit
//!    varying`'s bare default is genuinely unconstrained and has no such
//!    trap.
//!
//! Per ADR-0013 every comparison is against independently-authored SQL —
//! plain `pg_cast`, plain `to_jsonb`, plain `group by` — never against
//! the engine's own renderer. Harness conventions follow
//! `defs_boolean.rs`/`defs_bytea.rs`.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::pg_type::PgType;
use trellis::defs::{
    create_aggregate_target_table, create_definition, install_definition, parse,
    source_primary_key, validate,
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
// 1. Text-stability: no second renderer, unlike boolean
// ---------------------------------------------------------------------

/// Neither `bit` nor `bit varying` has a dedicated `::text` cast function —
/// unlike `boolean`, `::text` is each type's own output function
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
    assert_eq!(n, 1, "boolean must still have its own ::text cast");
}

/// `to_jsonb` and `::text` must agree for both types — the render-agreement
/// half of the datetime defect shape. Neither type is a datetime family
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

/// Both `bit(n)` and `bit varying` are accepted as a `JOIN` `ON` key
/// and, because both roles gate on the same
/// `join_key_types::TEXT_STABLE_JOIN_KEY_TYPES` allowlist, as a 1-1 primary key
/// too — neither role ever asks a bare `ValueType` to declare a new column
/// (a join-key lookup casts the bound array to the column's own concrete
/// introspected type; a 1-1 primary key copies that concrete type
/// verbatim), so the DDL-typmod hazard that keeps fixed-length `bit` off
/// the `GROUP BY` key role (below) never applies here.
#[tokio::test]
async fn bit_and_varbit_join_keys_and_primary_keys_are_admitted() {
    // A copy has no modifier, so `bit(4)` is the unlimited `"bit"` (ADR-0020).
    for (pg_decl, expected_pk_type) in [("bit(4)", "\"bit\""), ("bit varying", "bit varying")] {
        let cluster = TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let client = connect_raw(db.dsn()).await;

        client
            .batch_execute(&format!(
                "create table parent (k {pg_decl} primary key); \
                 create table child (id bigint primary key, k {pg_decl})"
            ))
            .await
            .expect("create join tables");

        define_join_on(&db.pool, "j", ("child", "k"), ("parent", "k"))
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
    let def = parse(
        "TRANSFORM t FROM s GROUP BY vb WITHOUT SOURCE INDEX SELECT vb AS k, SUM(n) AS total",
    )
    .expect("parses");
    validate(&def, &source_columns())
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
        parse("TRANSFORM t FROM s GROUP BY b WITHOUT SOURCE INDEX SELECT b AS k, SUM(n) AS total")
            .expect("parses");
    let err = validate(&def, &source_columns())
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
// 3. Regression guard: a bit varying GROUP BY key seeded two ways
// ---------------------------------------------------------------------

/// A `bit varying` `GROUP BY` group touched once through an ordinary
/// image-bearing CDC change and once through a bare, image-less live
/// refetch must still land as **one** target row, not two — the datetime
/// defect shape. `bit varying` was never exposed to that defect (see
/// `to_jsonb_and_text_agree_for_bit_and_varbit` above), so this is a
/// regression guard rather than a reproduction of a live bug, the same
/// framing `defs_bytea.rs`'s equivalent test uses.
#[tokio::test]
async fn a_varbit_group_key_seeded_by_cdc_and_by_live_read_is_one_group_not_two() {
    const DEF_SQL: &str = "TRANSFORM totals FROM events GROUP BY grp WITHOUT SOURCE INDEX SELECT grp AS grp, SUM(amount) AS total";

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table events ( \
               id integer primary key, grp bit varying, amount numeric(12,0))",
        )
        .await
        .expect("create source table");

    let columns = HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("grp".to_string(), ValueType::Other(PgType::VarBit)),
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
            "bit_worker",
            1,
            "trellis_defs_bit_issue_118_regression",
        )
        .await
        .expect("drain_once")
        .expect("drain_once must claim and drain something");
    }

    // Row 1: an ordinary image-bearing insert, the shape real CDC produces.
    stage_image(&client, "seg_0", "1", r#"{"grp":"101","amount":"10"}"#).await;
    // Row 2: a bare recompute trigger — no image at all — forcing
    // `live_rows_query`'s live refetch to decode `grp` via
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
