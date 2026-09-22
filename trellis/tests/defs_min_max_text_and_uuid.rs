//! End-to-end tests for issue #120's `MIN`/`MAX` generalization — `text`/
//! `varchar` (which lands) and `uuid` (checked live, and found *not* to
//! land, despite `docs/type-support.md` marking it only `⚠️` rather than
//! `❌` before this issue).
//!
//! # Why these run against a live server
//!
//! Per #111-#119's playbook: check that `min`/`max` actually exist for a
//! type and keep its own type, rather than assume from "has a full btree
//! opclass" — checked here for *both* `text` and `uuid`, and the two land
//! completely differently. Then check whether a `GROUP BY` definition's
//! `MIN`/`MAX` actually recomputes correctly on delete of the extremum (the
//! issue's own framing: "recompute-on-delete of extremum") against an
//! independently-authored server-side recompute.
//!
//! **The real gap for `text` turned out to be narrower than "wire up
//! admission."** `defs::invertibility::classify`'s `MIN`/`MAX` arm was
//! already type-agnostic (`("MIN" | "MAX", AggregateArg::Column(_)) =>
//! RecomputeOnly`, unconditional on type, since before this issue), and a
//! `KeySpace::Aggregate` field's `MIN`/`MAX` was already resolved entirely
//! by a server-side `min()`/`max()` push-down
//! (`staging::apply_aggregate::probe_recompute_fields_bulk`/`probe_field_value`),
//! never a Rust-side fold. The actual gap was purely at the *admission*
//! layer: `registry::aggregate_result_type` fell through to `None` for
//! `Text` because it wasn't in the numeric family and had no early-return
//! arm of its own the way `Boolean`/the `Other` families did.
//!
//! **`text` is nonetheless the one family this issue could not fully
//! close.** Postgres `text` ordering is a property of the *column's own
//! collation* (`pg_attribute.attcollation`), which this crate tracks
//! nowhere — so while the `GROUP BY` `MIN`/`MAX(text)` role above is
//! unconditionally correct (Postgres resolves the real collation itself),
//! the one shape with no live-SQL fallback (a `KeySpace::OneToOne` field's
//! `MIN`/`MAX` wrapping a to-many relationship path) is refused outright at
//! validation time (`ValidationError::TextAggregateOverToManyRelationshipUnsupported`)
//! rather than risk a silent wrong-order fold under a non-byte-order
//! collation — see that variant's own doc comment, and
//! `trellis/src/defs/validate.rs`'s `min_max_text_over_a_to_many_relationship_is_refused`
//! unit test (no live server needed for that one: it's a pure
//! grammar/type-shape check, exactly like `defs_enum.rs`'s equivalent case
//! is tested at the unit level too).
//!
//! **`uuid` does not land at all, and this was found, not assumed.** `uuid`
//! has a full, `IMMUTABLE` btree opclass (`uuid_ops`) — `ORDER BY`/`<`/`>`
//! all work — which is exactly the shape that made `text`'s admission look
//! straightforward. But `select min(v) from (values ('...'::uuid)) t(v)` is
//! `ERROR: function min(uuid) does not exist` on a live Postgres 17.11, and
//! no `pg_proc`/`pg_aggregate` row names `min`/`max` over a lone `uuid`
//! argument — the identical "opclass but no aggregate" finding issue #114
//! made for `bytea` and issue #116 made for `macaddr`/`macaddr8`. `uuid`
//! stays `❌` for this role; `registry::aggregate_result_type` returns
//! `None` for it, unconditionally, and the validator reports the ordinary
//! `FunctionArgTypeMismatch` any aggregate/type mismatch gets.
//!
//! Per ADR-0013 every comparison here is against independently-authored SQL
//! — plain `min`/`max`, plain `group by`, plain `pg_proc`/`pg_aggregate` —
//! never against `defs::oracle::recompute`. Harness conventions follow
//! `defs_enum.rs`/`defs_netaddr.rs`.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::{create_aggregate_target_table, create_definition, parse, registry, validate};
use trellis::integer::IntWidth;
use trellis::staging::{StagedWatermark, apply, seal};

// ---------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------

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

/// The physical ring table this test should stage into *right now* — see
/// `defs_count_column.rs`'s identically-named helper for why hardcoding
/// `seg_0` past a test's first seal/drain round is unsafe (`seal::seal_phase1`
/// rotates the active ring slot every call; the fold's phase-gap mechanism
/// only tolerates one stale generation).
async fn active_seg_table(client: &Client) -> String {
    let ring_slot: i16 = client
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment pointer")
        .get(0);
    format!("seg_{ring_slot}")
}

fn next_lsn() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

async fn stage_image(client: &Client, key: &str, src_table: &str, new_image: &str) {
    let table = active_seg_table(client).await;
    let src_table = format!("{DEFAULT_SCHEMA}.{src_table}");
    client
        .execute(
            &format!(
                "insert into {table} \
                 (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                 values ($1, $2, 'insert', $3, null, $4::text::jsonb, 0)"
            ),
            &[&src_table, &key, &PgLsn::from(next_lsn()), &new_image],
        )
        .await
        .unwrap_or_else(|e| panic!("stage image-bearing {key} into {table}: {e}"));
}

async fn stage_delete(client: &Client, key: &str, src_table: &str, old_image: &str) {
    let table = active_seg_table(client).await;
    let src_table = format!("{DEFAULT_SCHEMA}.{src_table}");
    client
        .execute(
            &format!(
                "insert into {table} \
                 (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                 values ($1, $2, 'delete', $3, $4::text::jsonb, null, 0)"
            ),
            &[&src_table, &key, &PgLsn::from(next_lsn()), &old_image],
        )
        .await
        .unwrap_or_else(|e| panic!("stage delete {key} into {table}: {e}"));
}

async fn drain_sealed(client: &mut Client, pool: &trellis::Pool, worker: &str) {
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq)
        .await
        .expect("seal phase 2");
    apply::drain_once(
        pool,
        outcome.sealed_seg_seq,
        worker,
        1,
        "trellis_defs_min_max_text_uuid",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("drain_once")
    .expect("drain_once must claim and drain something");
}

// ---------------------------------------------------------------------
// 1. Admission: registry::aggregate_result_type mirrors the argument type
// ---------------------------------------------------------------------

#[test]
fn min_max_admit_text_mirroring_the_argument_type() {
    assert_eq!(
        registry::aggregate_result_type("MIN", ValueType::Text),
        Some(ValueType::Text),
        "MIN(text) must resolve to its own argument's type"
    );
    assert_eq!(
        registry::aggregate_result_type("MAX", ValueType::Text),
        Some(ValueType::Text),
        "MAX(text) must resolve to its own argument's type"
    );
    // SUM/AVG still have no such Postgres aggregate for text.
    assert_eq!(
        registry::aggregate_result_type("SUM", ValueType::Text),
        None
    );
    assert_eq!(
        registry::aggregate_result_type("AVG", ValueType::Text),
        None
    );
}

/// `uuid` admits none of the four — checked, not assumed (see this file's
/// module doc comment for the live `pg_proc`/`pg_aggregate` evidence).
#[test]
fn uuid_admits_no_min_max_sum_avg_aggregate() {
    for name in ["MIN", "MAX", "SUM", "AVG"] {
        assert_eq!(
            registry::aggregate_result_type(name, ValueType::Uuid),
            None,
            "{name}(uuid) must not be admitted — Postgres has no such aggregate"
        );
    }
}

// ---------------------------------------------------------------------
// 2. text: GROUP BY MIN/MAX, live, including recompute-on-delete
// ---------------------------------------------------------------------

#[tokio::test]
async fn text_group_by_min_max_matches_a_server_side_recompute() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table events (id integer primary key, grp integer, label text); \
             alter table events replica identity full",
        )
        .await
        .expect("create source table");

    let def_sql = "TRANSFORM totals FROM events GROUP BY grp \
                   SELECT grp AS grp, MIN(label) AS lo, MAX(label) AS hi";
    let columns = HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("grp".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("label".to_string(), ValueType::Text),
    ]);
    let def = parse(def_sql).expect("parse");
    validate(&def, &columns, &HashMap::new()).expect("validate");
    create_definition(&db.pool, def_sql, &columns)
        .await
        .expect("create definition");
    create_aggregate_target_table(&db.pool, &def, "public", &columns)
        .await
        .expect("create aggregate target table");

    let rows: Vec<(i32, i32, &str)> = vec![
        (1, 1, "banana"),
        (2, 1, "apple"),
        (3, 1, "cherry"),
        (4, 2, "zebra"),
        (5, 2, "zebra"),
        (6, 3, "mango"),
    ];
    client
        .batch_execute(
            "insert into events (id, grp, label) values \
               (1, 1, 'banana'), (2, 1, 'apple'), (3, 1, 'cherry'), \
               (4, 2, 'zebra'), (5, 2, 'zebra'), (6, 3, 'mango')",
        )
        .await
        .expect("seed source rows");

    for (id, grp, label) in &rows {
        stage_image(
            &client,
            &id.to_string(),
            "events",
            &format!(r#"{{"grp":"{grp}","label":"{label}"}}"#),
        )
        .await;
    }
    drain_sealed(&mut client, &db.pool, "text_worker_v1").await;

    async fn read_totals(client: &Client) -> Vec<(i32, String, String)> {
        let mut got: Vec<(i32, String, String)> = client
            .query(
                "select grp, lo::text, hi::text from totals order by grp",
                &[],
            )
            .await
            .expect("read totals")
            .into_iter()
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .collect();
        got.sort_by_key(|(grp, ..)| *grp);
        got
    }

    async fn read_expected(client: &Client) -> Vec<(i32, String, String)> {
        let mut expected: Vec<(i32, String, String)> = client
            .query(
                "select grp, min(label)::text, max(label)::text from events \
                 group by grp order by grp",
                &[],
            )
            .await
            .expect("server-side recompute")
            .into_iter()
            .map(|row| (row.get(0), row.get(1), row.get(2)))
            .collect();
        expected.sort_by_key(|(grp, ..)| *grp);
        expected
    }

    let got = read_totals(&client).await;
    let expected = read_expected(&client).await;
    assert_eq!(got, expected);
    assert_eq!(
        got,
        vec![
            (1, "apple".to_string(), "cherry".to_string()),
            (2, "zebra".to_string(), "zebra".to_string()),
            (3, "mango".to_string(), "mango".to_string()),
        ]
    );

    // Recompute-on-delete of the extremum (the issue's own framing): delete
    // group 1's current minimum ('apple') and its current maximum
    // ('cherry') is untouched, so only `lo` should move.
    client
        .execute("delete from events where id = 2", &[])
        .await
        .expect("delete the current minimum");
    stage_delete(&client, "2", "events", r#"{"grp":"1","label":"apple"}"#).await;
    drain_sealed(&mut client, &db.pool, "text_worker_v2").await;

    let got = read_totals(&client).await;
    let expected = read_expected(&client).await;
    assert_eq!(got, expected);
    assert_eq!(
        got[0],
        (1, "banana".to_string(), "cherry".to_string()),
        "deleting the extremum must recompute to the next-lowest value, matching Postgres"
    );
}

// ---------------------------------------------------------------------
// 3. uuid: no min(uuid)/max(uuid) aggregate exists, live — checked, not
//    assumed from "has a full btree opclass"
// ---------------------------------------------------------------------

/// The exact live-server check that overturned this file's original
/// assumption (a positional, fixed-width-per-byte hex encoding *would* let a
/// pure-Rust fold reproduce `uuid_cmp` correctly — the same reasoning that
/// holds for `bytea`'s hex text — but that reasoning only matters if
/// Postgres actually wires a `min`/`max` aggregate to `uuid_ops` in the
/// first place, and it does not).
#[tokio::test]
async fn postgres_has_no_min_max_uuid_aggregate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let err = client
        .query_one(
            "select min(v) from (values ('aaaaaaaa-0000-0000-0000-000000000000'::uuid)) t(v)",
            &[],
        )
        .await
        .expect_err("Postgres must not have a min(uuid) aggregate");
    let message = err
        .as_db_error()
        .map(|e| e.message().to_string())
        .unwrap_or_default();
    assert!(
        message.contains("does not exist"),
        "expected an undefined-function error, got: {err:?}"
    );

    let row = client
        .query_one(
            "select count(*) from pg_proc p \
             join pg_aggregate a on a.aggfnoid = p.oid \
             where p.proname in ('min', 'max') \
               and pg_get_function_arguments(p.oid) ilike '%uuid%'",
            &[],
        )
        .await
        .expect("pg_proc/pg_aggregate probe");
    let n: i64 = row.get(0);
    assert_eq!(
        n, 0,
        "no pg_proc row should name min/max over a lone uuid argument"
    );

    // `uuid` nonetheless has a full, IMMUTABLE btree opclass — the same
    // "opclass but no aggregate" shape as bytea/macaddr, not a genuine
    // ordering hazard.
    let row = client
        .query_one(
            "select count(*) from pg_opclass where opcname = 'uuid_ops'",
            &[],
        )
        .await
        .expect("pg_opclass probe");
    let n: i64 = row.get(0);
    assert!(n > 0, "uuid must still have a real btree opclass");
}

/// `MIN`/`MAX(<uuid column>)` in a `GROUP BY` definition is refused at
/// validate time — the front-door counterpart of
/// `defs::validate::tests::min_max_uuid_is_refused_unconditionally`.
#[test]
fn min_max_uuid_group_by_definition_is_rejected() {
    let def_sql = "TRANSFORM totals FROM events GROUP BY grp \
                   SELECT grp AS grp, MAX(token) AS hi";
    let columns = HashMap::from([
        ("grp".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("token".to_string(), ValueType::Uuid),
    ]);
    let def = parse(def_sql).expect("parse");
    let err = validate(&def, &columns, &HashMap::new()).expect_err("MAX(uuid) must not validate");
    match err {
        trellis::defs::ValidationError::FunctionArgTypeMismatch { found, .. } => {
            assert_eq!(found, ValueType::Uuid)
        }
        other => panic!("expected FunctionArgTypeMismatch, got {other:?}"),
    }
}
