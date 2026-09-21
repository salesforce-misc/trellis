//! End-to-end tests for issue #114's `bytea` support — as a computed/
//! passthrough target (already landed: ingest since #108, typed literal
//! since #109) and, new here, via the join/primary-key and `GROUP BY` key
//! roles, plus a clear verdict on the `MIN`/`MAX` aggregate role.
//!
//! # Why these run against a live server
//!
//! Issue #114's own scope note predicted the same shape #110's typed key
//! index exists for: "text/wire rendering depends on the `bytea_output` GUC
//! -> key role needs decoded comparison." #111 found that kind of premise
//! wrong for `oid`, #113 found it wrong for four of the six temporal
//! families, and #112 found it *right* for floats — so, per that playbook,
//! this issue treats it as a question to put to a real Postgres rather than
//! infer from the type's name. Every claim below is grounded that way:
//!
//! 1. **Text stability.** `byteaout` under the pinned `bytea_output = 'hex'`
//!    must be a bijection — equal values render identically, distinct
//!    values render distinctly — over a grid that includes the empty value,
//!    embedded `NUL` bytes and the full byte range.
//! 2. **Ordering.** Since `bytea` gets no typed key index, if it were ever
//!    to need one this crate must know raw hex-text order already agrees
//!    with `bytea_cmp` — checked directly.
//! 3. **Render consistency.** `to_jsonb` and `::text` must agree on
//!    `bytea`, both as bare Postgres functions and as the actual SQL
//!    `staging::apply::row_as_text_jsonb_sql` builds (issue #248's fix).
//! 4. **Key roles.** Relationship join keys, 1-1 primary keys and `GROUP BY`
//!    keys must all accept a `bytea` column.
//! 5. **The `MIN`/`MAX` verdict.** Demonstrated, not asserted: Postgres
//!    itself has no `min(bytea)`/`max(bytea)` aggregate despite `bytea`
//!    having a full, `IMMUTABLE` btree opclass, so this crate can never
//!    support the role — not a rendering hazard, a missing server-side
//!    construct ADR-0004 has nothing to be a subset of.
//! 6. **The #113/#248 regression shape, reproduced for `bytea`.** A
//!    `GROUP BY` group touched once through an image-bearing change and once
//!    through a bare, image-less live refetch must still land as one target
//!    row, not two — the exact defect issue #248 fixed for `timestamp`.
//!
//! Per ADR-0013 every comparison is against independently-authored SQL —
//! plain `count(distinct ...)`, plain `to_jsonb`, plain `::text` — never
//! against `defs::oracle::recompute`, which would be the engine's own
//! renderer grading itself. Harness conventions follow `defs_temporal.rs`.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
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

/// The column typing `bytea_group_by_key_is_admitted` and
/// `postgres_has_no_min_max_aggregate_for_bytea` validate against — no live
/// table needed, since `validate` only consults this map, never the
/// database.
fn source_columns() -> HashMap<String, ValueType> {
    HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
        ("b".to_string(), ValueType::Other(PgType::Bytea)),
        ("n".to_string(), ValueType::Numeric),
    ])
}

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "set search_path to {DEFAULT_SCHEMA}, public; set bytea_output to 'hex'"
        ))
        .await
        .expect("session bootstrap");
    client
}

/// A spread of `bytea` values chosen to hit the structural edges: the empty
/// value (distinct from `NULL`), a single `NUL` byte, `NUL` embedded
/// mid-string, a prefix/extension pair (`\x00` vs `\x0000`, which would
/// misorder under a naive comparison that ignored length), a couple of
/// individual byte values at the low/high/boundary ends of the range, and
/// one longer mixed value. The *exhaustive* single-byte sweep (every value
/// `0x00..=0xff`) is generated separately in
/// `bytea_text_rendering_is_a_bijection_under_hex_output` rather than
/// hand-listed here.
const GRID: &[&str] = &[
    "\\x",
    "\\x00",
    "\\x0000",
    "\\x0001",
    "\\x0a",
    "\\x0b",
    "\\x7f",
    "\\x80",
    "\\xff",
    "\\xdead00beef",
    "\\xdeadbeef",
];

// ---------------------------------------------------------------------
// 1. Text stability
// ---------------------------------------------------------------------

/// `byteaout` under the pinned `bytea_output = 'hex'` must be a bijection:
/// as many distinct `::text` groups as distinct values, for every value in
/// the structural-edge-case grid (the empty value, embedded `NUL` bytes, a
/// prefix/extension pair) **and** for every single byte value from `0x00` to
/// `0xff` — the full range, generated rather than hand-listed, so the claim
/// in `catalog::TEXT_STABLE_JOIN_KEY_TYPES`'s doc comment ("every byte value
/// from `0x00` to `0xff`") is checked exactly as stated rather than
/// approximated by a handful of representatives.
///
/// This is the load-bearing fact behind admitting `bytea` straight onto
/// `catalog::TEXT_STABLE_JOIN_KEY_TYPES` with no typed key index — the
/// same measurement `defs_temporal.rs`'s
/// `the_text_stability_verdict_is_the_server_s_not_this_crate_s` makes for
/// the temporal families.
#[tokio::test]
async fn bytea_text_rendering_is_a_bijection_under_hex_output() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let grid_rows = GRID
        .iter()
        .map(|v| format!("('{v}'::bytea)"))
        .collect::<Vec<_>>()
        .join(",");
    let row = client
        .query_one(
            &format!(
                "select count(distinct v)::bigint, count(distinct v::text)::bigint \
                 from (values {grid_rows}) t(v)"
            ),
            &[],
        )
        .await
        .expect("bytea grid stability probe");
    let (by_value, by_text): (i64, i64) = (row.get(0), row.get(1));
    assert_eq!(
        by_value, by_text,
        "Postgres makes {by_value} value groups but ::text makes {by_text} text groups"
    );
    assert_eq!(
        by_value,
        GRID.len() as i64,
        "every grid value must be pairwise distinct to begin with"
    );

    // The full single-byte range, generated: 256 distinct one-byte values,
    // built as `set_byte('\x00', 0, i)` for `i` in `0..=255` rather than
    // hand-listed.
    let row = client
        .query_one(
            "select count(distinct v)::bigint, count(distinct v::text)::bigint \
             from (select set_byte('\\x00'::bytea, 0, i) as v \
                   from generate_series(0, 255) i) t",
            &[],
        )
        .await
        .expect("bytea full-byte-range stability probe");
    let (by_value, by_text): (i64, i64) = (row.get(0), row.get(1));
    assert_eq!(
        by_value, 256,
        "generate_series(0, 255) must yield 256 distinct bytes"
    );
    assert_eq!(
        by_value, by_text,
        "every single byte value 0x00..=0xff must render as a distinct, bijective ::text"
    );
}

// ---------------------------------------------------------------------
// 2. Ordering
// ---------------------------------------------------------------------

/// Raw hex-text order already agrees with `bytea_cmp`: ASCII orders
/// `'0'..'9' < 'a'..'f'` in exactly the order those characters' nibble
/// values need, and the fixed two-hex-digit-per-byte encoding means a
/// shorter value's hex text is always a genuine *prefix* of a longer one
/// that starts the same way (never, say, a longer string that happens to
/// sort first) — so lexicographic text comparison reproduces `bytea`'s own
/// unsigned byte-by-byte order, prefix-is-smaller included.
///
/// This crate does not currently need this fact for anything (`bytea` holds
/// no `MIN`/`MAX` role — see below — and no other code path orders `bytea`
/// values), but it is the fact that would make a future `MIN`/`MAX(bytea)`
/// role safe if Postgres ever gained the aggregate, so it is pinned here
/// rather than left as an unchecked assumption in a doc comment.
#[tokio::test]
async fn bytea_hex_text_order_matches_native_bytea_cmp() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute("create table ord (v bytea)")
        .await
        .expect("create ord");
    for v in GRID {
        client
            .execute(&format!("insert into ord values ('{v}'::bytea)"), &[])
            .await
            .unwrap_or_else(|e| panic!("insert {v}: {e}"));
    }

    let by_value: Vec<String> = client
        .query("select v::text from ord order by v", &[])
        .await
        .expect("order by v")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    let by_text: Vec<String> = client
        .query("select v::text from ord order by v::text", &[])
        .await
        .expect("order by v::text")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        by_value, by_text,
        "native bytea order and hex-text order must be identical"
    );
}

// ---------------------------------------------------------------------
// 3. Render consistency
// ---------------------------------------------------------------------

/// Bare Postgres `to_jsonb` and `::text` must agree on `bytea` — unlike
/// `timestamp`/`timestamptz`, which #248 had to reconcile, `bytea` was never
/// routed through `to_jsonb`'s special datetime writer, so this should hold
/// even for a value predating #114's own changes.
#[tokio::test]
async fn to_jsonb_and_text_agree_for_bytea() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute("create table renderers (v bytea)")
        .await
        .expect("create renderers");
    for v in GRID {
        client
            .execute(&format!("insert into renderers values ('{v}'::bytea)"), &[])
            .await
            .unwrap_or_else(|e| panic!("insert {v}: {e}"));
    }

    let rows = client
        .query(
            "select v::text, (to_jsonb(renderers.*) ->> 'v') from renderers",
            &[],
        )
        .await
        .expect("sweep renderers");
    assert_eq!(rows.len(), GRID.len());
    for row in rows {
        let (via_text, via_jsonb): (String, String) = (row.get(0), row.get(1));
        assert_eq!(
            via_text, via_jsonb,
            "::text and to_jsonb must agree on every bytea value"
        );
    }
}

/// The actual SQL `staging::apply::row_as_text_jsonb_sql` builds (issue
/// #248's replacement for bare `to_jsonb(t.*)`) must agree with `::text` for
/// `bytea` — the direct counterpart to
/// `defs_temporal.rs`'s `the_engine_s_own_row_renderer_agrees_with_text_for_every_temporal_family`.
#[tokio::test]
async fn the_engine_s_own_row_renderer_agrees_with_text_for_bytea() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute("create table engine_renderer_sweep (v bytea)")
        .await
        .expect("create sweep table");
    for v in GRID {
        client
            .execute(
                &format!("insert into engine_renderer_sweep values ('{v}'::bytea)"),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("insert {v}: {e}"));
    }

    let row_columns = apply::live_row_columns(&client, "engine_renderer_sweep")
        .await
        .expect("introspect columns");
    let doc_expr = apply::row_as_text_jsonb_sql("t", &row_columns);
    let via_engine: Vec<String> = client
        .query(
            &format!("select {doc_expr} ->> 'v' from engine_renderer_sweep t"),
            &[],
        )
        .await
        .expect("engine renderer sweep")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    let via_text: Vec<String> = client
        .query("select v::text from engine_renderer_sweep", &[])
        .await
        .expect("::text sweep")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        via_engine, via_text,
        "the engine's own row renderer must agree with ::text for bytea"
    );
}

// ---------------------------------------------------------------------
// 4. Key roles
// ---------------------------------------------------------------------

/// `bytea` is accepted as a relationship join key and, because both roles
/// gate on the same `catalog::TEXT_STABLE_JOIN_KEY_TYPES` allowlist, as a
/// 1-1 primary key too.
#[tokio::test]
async fn bytea_join_keys_and_primary_keys_are_admitted() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table parent (k bytea primary key); \
             create table child (id bigint primary key, k bytea); \
             alter table child replica identity full; \
             alter table parent replica identity full",
        )
        .await
        .expect("create relationship tables");

    create_relationship(&db.pool, "RELATIONSHIP r FROM child.k TO parent.k")
        .await
        .unwrap_or_else(|e| panic!("a bytea column must be accepted as a join key: {e}"));

    let pk = source_primary_key(&db.pool, "parent")
        .await
        .unwrap_or_else(|e| panic!("a single-column bytea primary key must be accepted: {e}"));
    assert_eq!(pk.len(), 1);
    assert_eq!(pk[0].data_type, "bytea");
}

/// The `GROUP BY` key gate (`validate::reject_unsupported_group_by_key_type`)
/// admits `bytea`, the same allowlist-widening #111 did for `oid` and #113
/// did for four of the six temporal families.
#[test]
fn bytea_group_by_key_is_admitted() {
    let def =
        parse("TRANSFORM t FROM s GROUP BY b SELECT b AS k, SUM(n) AS total").expect("parses");
    validate(&def, &source_columns(), &HashMap::new())
        .unwrap_or_else(|e| panic!("bytea must be accepted as a GROUP BY key: {e}"));
}

// ---------------------------------------------------------------------
// 5. The MIN/MAX verdict
// ---------------------------------------------------------------------

/// Postgres itself has no `min(bytea)`/`max(bytea)` aggregate, despite
/// `bytea` having a full, `IMMUTABLE` btree opclass (`<`/`>`/`=`/`ORDER BY`
/// all work — see `bytea_hex_text_order_matches_native_bytea_cmp` above).
/// This is not a rendering hazard `#110`'s typed key index could fix; it is
/// a server-side construct that does not exist for this type, so
/// `MIN`/`MAX(bytea)` has nothing for ADR-0004's "immutable subset of
/// Postgres" to be a subset *of*.
#[tokio::test]
async fn postgres_has_no_min_max_aggregate_for_bytea() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute("create table t (v bytea); insert into t values ('\\x00')")
        .await
        .expect("seed probe table");
    for agg in ["min", "max"] {
        let err = client
            .query_one(&format!("select {agg}(v) from t"), &[])
            .await
            .expect_err(&format!("{agg}(bytea) must not exist in Postgres"));
        // `tokio_postgres::Error`'s own `Display` is deliberately terse
        // ("db error") — the server's actual message lives on the wrapped
        // `DbError`.
        let db_error = err
            .as_db_error()
            .unwrap_or_else(|| panic!("{agg}(bytea) must fail as a DbError: {err}"));
        assert!(
            db_error.message().contains("does not exist"),
            "{agg}(bytea): {db_error}"
        );
    }

    // Trellis's own gate agrees, at both the layer that decides an
    // aggregate's result type and the layer that reports the error to a
    // definition author.
    for name in ["MIN", "MAX"] {
        assert!(
            registry::aggregate_result_type(name, ValueType::Other(PgType::Bytea)).is_none(),
            "{name}(bytea) must have no result type"
        );

        let def = parse(&format!(
            "TRANSFORM t FROM s GROUP BY id SELECT id AS k, {name}(b) AS m"
        ))
        .expect("parses");
        let err = validate(&def, &source_columns(), &HashMap::new())
            .expect_err(&format!("{name}(b) over a bytea column must be rejected"));
        let _ = err; // the specific ValidationError variant isn't load-bearing here
    }
}

// ---------------------------------------------------------------------
// 6. The #113/#248 regression shape, reproduced for bytea
// ---------------------------------------------------------------------

/// A `bytea` `GROUP BY` group touched once through an ordinary image-bearing
/// change and once through a bare, image-less live refetch must still land
/// as **one** target row, not two — the same shape `defs_temporal.rs`'s
/// `a_timestamp_group_key_seeded_by_backfill_and_by_live_read_is_one_group_not_two`
/// pins for `timestamp`. `bytea` was never exposed to the defect #248 fixed
/// (see `to_jsonb_and_text_agree_for_bytea`), so this is a regression guard
/// rather than a reproduction of a live bug — if a future change ever routed
/// a `bytea` row-read back through raw `to_jsonb(t.*)`, or introduced any
/// other renderer disagreement, this test would be the one to catch it.
///
/// The shared group key is `\x00` — a single embedded `NUL` byte — to keep
/// the low-level-string-handling edge case the task explicitly flagged
/// (NUL-byte handling) inside the one test that exercises the full staging
/// pipeline, not just an isolated SQL probe.
#[tokio::test]
async fn a_bytea_group_key_seeded_by_backfill_and_by_live_read_is_one_group_not_two() {
    const DEF_SQL: &str =
        "TRANSFORM totals FROM events GROUP BY grp SELECT grp AS grp, SUM(amount) AS total";

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table events ( \
               id integer primary key, grp bytea, amount numeric); \
             alter table events replica identity full",
        )
        .await
        .expect("create source table");

    let columns = HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("grp".to_string(), ValueType::Other(PgType::Bytea)),
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
               (1, '\\x00', 10), \
               (2, '\\x00', 5)",
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
        seal::seal_phase2(client, outcome.sealed_seg_seq)
            .await
            .expect("seal phase 2");
        apply::drain_once(
            pool,
            outcome.sealed_seg_seq,
            "bytea_worker",
            1,
            "trellis_defs_bytea_issue_114_regression",
            &StagedWatermark::saturated(),
        )
        .await
        .expect("drain_once")
        .expect("drain_once must claim and drain something");
    }

    // Row 1: an ordinary image-bearing insert, the shape real CDC produces —
    // canonical hex text, exactly what a walsender emits under the pinned
    // `bytea_output = 'hex'`.
    stage_image(&client, "seg_0", "1", r#"{"grp":"\\x00","amount":"10"}"#).await;
    // Row 2: a bare recompute trigger — no image at all — forcing
    // `read_live_rows_batch`'s live refetch to decode `grp` straight off
    // Postgres via `row_as_text_jsonb_sql`.
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
    assert_eq!(got_grp, "\\x00");
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
