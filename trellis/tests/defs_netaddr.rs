//! End-to-end tests for issue #116's network-address types (`inet`, `cidr`,
//! `macaddr`, `macaddr8`) — as join/primary-key and `GROUP BY` key roles,
//! typed literals, and the `MIN`/`MAX` aggregate role.
//!
//! # Why these run against a live server
//!
//! Issue #116's own scope note treats all four families as one group and
//! marks `MIN`/`MAX` `⚠️` across the board. Per #119's playbook — check
//! `pg_cast` for a second renderer before assuming `::text` is a type's own
//! output function, and check `pg_proc`/`pg_aggregate` for `MIN`/`MAX`
//! before assuming it exists — asked of a real server, the four turn out
//! **not** to land the same way at all:
//!
//! 1. **`pg_cast`.** `inet` and `cidr` share one `text()` cast row
//!    (`pg_catalog.text(inet)`, `prosrc = network_show`); `macaddr`/
//!    `macaddr8` have none. Whether that shared cast row actually diverges
//!    from the type's own output function is a *per-type* fact, not a
//!    per-row one: it does for `inet` (real, live, load-bearing — the exact
//!    `boolean` shape) and does not for `cidr` (verified never to differ).
//! 2. **Key roles.** `cidr`/`macaddr`/`macaddr8` are admitted as
//!    relationship/primary keys and `GROUP BY` keys. `inet` is refused as a
//!    relationship/primary key (same reason `boolean` is) but *admitted* as
//!    a `GROUP BY` key, the same split `boolean` got in #119, because
//!    `staging::apply_aggregate`'s keyset match never does raw-text
//!    comparison and `inet_in` reconciles both spellings — demonstrated here
//!    with the same end-to-end CDC-vs-live-read regression shape
//!    `defs_boolean.rs`/`defs_temporal.rs` each pin for their own family.
//! 3. **`MIN`/`MAX`.** `inet` has a real, own-type-preserving aggregate,
//!    cross-checked against a server-side `min`/`max` byte-for-byte
//!    (ADR-0013). `cidr`'s only reachable `min`/`max` is Postgres's own
//!    implicit upcast to `inet` — demonstrated live to return a value typed
//!    `inet`, not `cidr` — so this crate refuses it rather than silently
//!    changing a computed field's type. `macaddr`/`macaddr8` have no such
//!    aggregate at all (the exact `bytea` finding from #114), demonstrated
//!    the same way `defs_bytea.rs` demonstrates it for `bytea`.
//! 4. **`to_jsonb`.** Checked live for all four; `inet` is the one case
//!    where it disagrees with `::text` (it agrees with `inet_out` instead) —
//!    not a live hazard, since issue #248 already replaced every bare
//!    `to_jsonb(t.*)` row-body read in this engine with an explicit
//!    `<col>::text`, so nothing here actually calls `to_jsonb` that way.
//!
//! Per ADR-0013 every comparison is against independently-authored SQL —
//! plain `pg_cast`, plain `network_cmp`/`<`, plain `group by`, plain
//! `min`/`max` — never against `defs::oracle::recompute`, which would be the
//! engine's own renderer grading itself. Harness conventions follow
//! `defs_boolean.rs`/`defs_bytea.rs`.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::eval::{RegexCache, Row, Value, evaluate_aggregate};
use trellis::defs::pg_type::PgType;
use trellis::defs::{
    create_aggregate_target_table, create_definition, create_relationship, parse, registry,
    source_primary_key, validate,
};
use trellis::integer::IntWidth;
use trellis::netaddr;
use trellis::staging::{StagedWatermark, apply, seal};

// ---------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------

fn source_columns() -> HashMap<String, ValueType> {
    HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
        ("i".to_string(), ValueType::Other(PgType::Inet)),
        ("c".to_string(), ValueType::Other(PgType::Cidr)),
        ("m".to_string(), ValueType::Other(PgType::MacAddr)),
        ("m8".to_string(), ValueType::Other(PgType::MacAddr8)),
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
// 1. The pg_cast findings
// ---------------------------------------------------------------------

/// `inet`/`cidr` share one dedicated `text()` cast row; `macaddr`/`macaddr8`
/// have none — the load-bearing check per #119's playbook, done directly
/// against `pg_cast` rather than assumed from the types' names.
#[tokio::test]
async fn inet_and_cidr_share_a_text_cast_row_macaddr_and_macaddr8_have_none() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    for pg_type in ["inet", "cidr"] {
        let row = client
            .query_one(
                &format!(
                    "select castfunc::regproc::text from pg_cast \
                     where castsource = '{pg_type}'::regtype and casttarget = 'text'::regtype"
                ),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("{pg_type} must have a dedicated text() cast: {e}"));
        let castfunc: String = row.get(0);
        assert_eq!(castfunc, "pg_catalog.text");
    }

    for pg_type in ["macaddr", "macaddr8"] {
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
}

/// The concrete consequence for `inet`: `inet_out` (what CDC decodes) and
/// `<col>::text` (`network_show`, the cast) disagree on every bare-host
/// address and agree on every value with an explicit sub-maximal netmask —
/// demonstrated over a real table column, the same shape
/// `defs_boolean.rs`'s `boolout_and_the_text_cast_disagree_on_a_live_column`
/// takes.
#[tokio::test]
async fn inet_out_and_the_text_cast_disagree_on_bare_host_addresses() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table t (v inet); \
             insert into t values ('192.168.1.5'), ('::1'), ('192.168.1.0/24'), ('10.0.0.0/8')",
        )
        .await
        .expect("seed table");

    let rows = client
        .query(
            "select inet_out(v)::text as via_output_func, v::text as via_cast from t order by v",
            &[],
        )
        .await
        .expect("sweep t");
    assert_eq!(rows.len(), 4);
    let mut bare_host_disagreed = false;
    for row in rows {
        let (via_output_func, via_cast): (String, String) = (row.get(0), row.get(1));
        if !via_output_func.contains('/') {
            // A bare host address: `inet_out` elided the netmask,
            // `network_show` never does. The default width depends on the
            // address family (`/32` for v4, `/128` for v6).
            assert_ne!(via_output_func, via_cast);
            let default_suffix = if via_output_func.contains(':') {
                "/128"
            } else {
                "/32"
            };
            assert_eq!(via_cast, format!("{via_output_func}{default_suffix}"));
            bare_host_disagreed = true;
        } else {
            // An explicit sub-maximal netmask: both renderers agree.
            assert_eq!(via_output_func, via_cast);
        }
    }
    assert!(
        bare_host_disagreed,
        "the grid must include at least one bare host address to exercise the divergence"
    );
}

/// `cidr`'s cast never diverges from `cidr_out` — unlike `inet`, a `cidr`
/// value's whole point is that the netmask is significant, so both
/// renderers always print it.
#[tokio::test]
async fn cidr_out_and_the_text_cast_always_agree() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table t (v cidr); \
             insert into t values ('192.168.1.0/24'), ('10.0.0.0/8'), ('0.0.0.0/0'), \
             ('192.168.1.5/32'), ('2001:db8::/32'), ('::/0')",
        )
        .await
        .expect("seed table");

    let rows = client
        .query("select cidr_out(v)::text, v::text from t", &[])
        .await
        .expect("sweep t");
    assert_eq!(rows.len(), 6);
    for row in rows {
        let (via_output_func, via_cast): (String, String) = (row.get(0), row.get(1));
        assert_eq!(via_output_func, via_cast);
    }
}

// ---------------------------------------------------------------------
// 2. Key roles
// ---------------------------------------------------------------------

/// `cidr`/`macaddr`/`macaddr8` are accepted as relationship join keys and,
/// because both roles gate on the same `catalog::TEXT_STABLE_JOIN_KEY_TYPES`
/// allowlist, as 1-1 primary keys too.
#[tokio::test]
async fn cidr_macaddr_macaddr8_join_keys_and_primary_keys_are_admitted() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    for (idx, pg_type) in ["cidr", "macaddr", "macaddr8"].into_iter().enumerate() {
        let parent = format!("parent_{idx}");
        let child = format!("child_{idx}");
        client
            .batch_execute(&format!(
                "create table {parent} (k {pg_type} primary key); \
                 create table {child} (id bigint primary key, k {pg_type}); \
                 alter table {child} replica identity full; \
                 alter table {parent} replica identity full"
            ))
            .await
            .unwrap_or_else(|e| panic!("create relationship tables for {pg_type}: {e}"));

        create_relationship(
            &db.pool,
            &format!("RELATIONSHIP r_{idx} FROM {child}.k TO {parent}.k"),
        )
        .await
        .unwrap_or_else(|e| panic!("a {pg_type} column must be accepted as a join key: {e}"));

        let pk = source_primary_key(&db.pool, &parent)
            .await
            .unwrap_or_else(|e| {
                panic!("a single-column {pg_type} primary key must be accepted: {e}")
            });
        assert_eq!(pk.len(), 1);
        assert_eq!(pk[0].data_type, pg_type);
    }
}

/// `inet` must be refused as a relationship join key and as a 1-1 primary
/// key — the same shape `defs_boolean.rs`'s
/// `boolean_is_refused_as_a_relationship_join_key_and_primary_key` pins.
#[tokio::test]
async fn inet_is_refused_as_a_relationship_join_key_and_primary_key() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table parent (k inet primary key); \
             create table child (id bigint primary key, k inet); \
             alter table child replica identity full; \
             alter table parent replica identity full",
        )
        .await
        .expect("create relationship tables");

    let err = create_relationship(&db.pool, "RELATIONSHIP r FROM child.k TO parent.k")
        .await
        .expect_err("an inet column must be refused as a join key");
    let _ = err;

    let err = source_primary_key(&db.pool, "parent")
        .await
        .expect_err("a single-column inet primary key must be refused");
    let _ = err;
}

/// The `GROUP BY` key gate admits all four families — `inet` included,
/// despite its refusal above, the same split `boolean` got.
#[test]
fn all_four_families_are_admitted_as_group_by_keys() {
    for (column, target) in [("i", "gi"), ("c", "gc"), ("m", "gm"), ("m8", "gm8")] {
        let def = parse(&format!(
            "TRANSFORM t FROM s GROUP BY {column} SELECT {column} AS {target}, SUM(n) AS total"
        ))
        .expect("parses");
        validate(&def, &source_columns(), &HashMap::new())
            .unwrap_or_else(|e| panic!("{column} must be accepted as a GROUP BY key: {e}"));
    }
}

// ---------------------------------------------------------------------
// 3. The MIN/MAX verdict
// ---------------------------------------------------------------------

/// `inet` has a real `min`/`max` that keeps its own argument type — checked
/// against `pg_typeof`, not assumed — and the Rust evaluator's fold matches
/// a server-side `min`/`max` byte-for-byte over a grid that exercises every
/// tier of `network_cmp`'s tie-break (cross-family, shared-prefix-at-
/// different-netmask-lengths, same-network-different-host-bits).
#[tokio::test]
async fn inet_min_max_keeps_its_type_and_the_fold_matches_a_server_side_aggregate() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let row = client
        .query_one(
            "select pg_typeof(min(v))::text, pg_typeof(max(v))::text \
             from (values ('10.0.0.1'::inet), ('192.168.1.5'::inet)) t(v)",
            &[],
        )
        .await
        .expect("pg_typeof probe");
    let (min_ty, max_ty): (String, String) = (row.get(0), row.get(1));
    assert_eq!(min_ty, "inet");
    assert_eq!(max_ty, "inet");

    for name in ["MIN", "MAX"] {
        assert_eq!(
            registry::aggregate_result_type(name, ValueType::Other(PgType::Inet)),
            Some(ValueType::Other(PgType::Inet)),
            "{name}(inet) must resolve to Other(Inet)"
        );
    }

    const GROUPS: &[&[&str]] = &[
        &["10.0.0.1", "192.168.1.5", "172.16.0.1"],
        &["::1", "::", "fe80::1"],
        &["1.2.3.4", "::1"], // cross-family
        &["10.1.0.0/16", "10.1.2.0/24", "10.2.0.0/16"],
        &["10.1.2.3/24", "10.1.2.99/24", "10.1.2.1/24"],
    ];

    for group in GROUPS {
        let values_sql = group
            .iter()
            .map(|v| format!("('{v}'::inet)"))
            .collect::<Vec<_>>()
            .join(",");
        let row = client
            .query_one(
                &format!("select min(v)::text, max(v)::text from (values {values_sql}) t(v)"),
                &[],
            )
            .await
            .unwrap_or_else(|e| panic!("server min/max over {group:?}: {e}"));
        let (server_min, server_max): (String, String) = (row.get(0), row.get(1));

        // Feed the evaluator the engine's own canonical rendering
        // (`::text`, matching `network_show`), the text it would actually
        // hold internally.
        let mut canonical = Vec::new();
        for value in *group {
            let row = client
                .query_one(&format!("select '{value}'::inet::text"), &[])
                .await
                .expect("canonicalize");
            canonical.push(row.get::<_, String>(0));
        }

        for (aggregate, expected) in [("MIN", &server_min), ("MAX", &server_max)] {
            let def = parse(&format!(
                "TRANSFORM t FROM s GROUP BY id SELECT id AS k, {aggregate}(i) AS out"
            ))
            .expect("parses");
            let columns = HashMap::from([
                ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
                ("i".to_string(), ValueType::Other(PgType::Inet)),
            ]);
            validate(&def, &columns, &HashMap::new())
                .unwrap_or_else(|e| panic!("{aggregate}(inet) must validate: {e}"));

            let rows: Vec<Row> = canonical
                .iter()
                .map(|text| {
                    Row::from([
                        ("id".to_string(), Some("1".to_string())),
                        ("i".to_string(), Some(text.clone())),
                    ])
                })
                .collect();
            let out = evaluate_aggregate(&def, &rows, &columns, &mut RegexCache::default())
                .unwrap_or_else(|e| panic!("{aggregate}(inet) fold over {group:?}: {e}"))
                .remove("out")
                .expect("the aggregate field")
                .expect("a non-empty group folds to a value");
            assert_eq!(
                out,
                Value::Other(PgType::Inet, expected.clone()),
                "{aggregate}(inet) over {group:?} must match a server-side aggregate"
            );
        }
    }
}

/// `cidr`'s only reachable `min`/`max` silently changes the result's type to
/// `inet` (Postgres's own implicit upcast) — demonstrated, not assumed —
/// which is why this crate refuses the role rather than surface a
/// type-changing aggregate.
#[tokio::test]
async fn postgres_only_reaches_min_max_cidr_via_an_implicit_upcast_to_inet() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table t (v cidr); \
             insert into t values ('192.168.1.0/24'), ('10.0.0.0/8')",
        )
        .await
        .expect("seed table");

    let row = client
        .query_one(
            "select pg_typeof(min(v))::text, pg_typeof(max(v))::text from t",
            &[],
        )
        .await
        .expect("pg_typeof probe");
    let (min_ty, max_ty): (String, String) = (row.get(0), row.get(1));
    assert_eq!(
        min_ty, "inet",
        "Postgres's own min(cidr) resolves through an implicit upcast to inet"
    );
    assert_eq!(max_ty, "inet");

    // Trellis's own gate agrees: no result type, so a MIN/MAX(cidr) field is
    // rejected at define time rather than silently declared `inet`.
    for name in ["MIN", "MAX"] {
        assert!(
            registry::aggregate_result_type(name, ValueType::Other(PgType::Cidr)).is_none(),
            "{name}(cidr) must have no result type"
        );
    }
}

/// `macaddr`/`macaddr8` have no `min`/`max` aggregate in Postgres at all —
/// the exact `bytea` finding from #114, demonstrated the same way
/// `defs_bytea.rs`'s `postgres_has_no_min_max_aggregate_for_bytea` does.
#[tokio::test]
async fn postgres_has_no_min_max_aggregate_for_macaddr_or_macaddr8() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    for (pg_type, sample, value_type) in [
        (
            "macaddr",
            "08:00:2b:01:02:03",
            ValueType::Other(PgType::MacAddr),
        ),
        (
            "macaddr8",
            "08:00:2b:01:02:03:04:05",
            ValueType::Other(PgType::MacAddr8),
        ),
    ] {
        client
            .batch_execute(&format!(
                "create table t_{pg_type} (v {pg_type}); \
                 insert into t_{pg_type} values ('{sample}')"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed probe table for {pg_type}: {e}"));

        for agg in ["min", "max"] {
            let err = client
                .query_one(&format!("select {agg}(v) from t_{pg_type}"), &[])
                .await
                .expect_err(&format!("{agg}({pg_type}) must not exist in Postgres"));
            let db_error = err
                .as_db_error()
                .unwrap_or_else(|| panic!("{agg}({pg_type}) must fail as a DbError: {err}"));
            assert!(
                db_error.message().contains("does not exist"),
                "{agg}({pg_type}): {db_error}"
            );
        }

        for name in ["MIN", "MAX"] {
            assert!(
                registry::aggregate_result_type(name, value_type).is_none(),
                "{name}({pg_type}) must have no result type"
            );
        }
    }
}

// ---------------------------------------------------------------------
// 4. to_jsonb agreement
// ---------------------------------------------------------------------

/// `to_jsonb` agrees with `::text` for `cidr`/`macaddr`/`macaddr8`, and
/// disagrees for `inet` (it agrees with `inet_out` instead) — checked live,
/// not assumed, matching every other family's `to_jsonb`-vs-`::text` sweep
/// in this epic. Not a live hazard: issue #248 already replaced every bare
/// `to_jsonb(t.*)` row-body read with an explicit `<col>::text`, so nothing
/// in the engine calls `to_jsonb` the way this test's bare Postgres call
/// does.
#[tokio::test]
async fn to_jsonb_agrees_with_text_except_for_inet() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table t (i inet, c cidr, m macaddr, m8 macaddr8); \
             insert into t values \
               ('192.168.1.5', '192.168.1.0/24', '08:00:2b:01:02:03', '08:00:2b:01:02:03:04:05')",
        )
        .await
        .expect("seed table");

    let row = client
        .query_one(
            "select i::text, (to_jsonb(t.*)->>'i'), \
                    c::text, (to_jsonb(t.*)->>'c'), \
                    m::text, (to_jsonb(t.*)->>'m'), \
                    m8::text, (to_jsonb(t.*)->>'m8') \
             from t",
            &[],
        )
        .await
        .expect("sweep t");

    let i_text: String = row.get(0);
    let i_jsonb: String = row.get(1);
    assert_ne!(i_text, i_jsonb, "inet is the one family where these differ");
    assert_eq!(i_text, format!("{i_jsonb}/32"));

    for (text_idx, jsonb_idx) in [(2, 3), (4, 5), (6, 7)] {
        let via_text: String = row.get(text_idx);
        let via_jsonb: String = row.get(jsonb_idx);
        assert_eq!(via_text, via_jsonb);
    }
}

// ---------------------------------------------------------------------
// 5. The #119-shaped regression, reproduced for inet's GROUP BY key
// ---------------------------------------------------------------------

/// An `inet` `GROUP BY` group touched once through an ordinary image-bearing
/// CDC change (staged with `inet_out`'s host-elided spelling, exactly what
/// real logical decoding produces) and once through a bare, image-less live
/// refetch (`row_as_text_jsonb_sql`'s `<col>::text` cast, `network_show`'s
/// always-explicit spelling) must still land as **one** target row, not
/// two — the same shape `defs_boolean.rs`'s
/// `a_boolean_group_key_seeded_by_cdc_and_by_live_read_is_one_group_not_two`
/// pins, here for the family issue #116 found the analogous divergence in.
#[tokio::test]
async fn an_inet_group_key_seeded_by_cdc_and_by_live_read_is_one_group_not_two() {
    const DEF_SQL: &str =
        "TRANSFORM totals FROM events GROUP BY grp SELECT grp AS grp, SUM(amount) AS total";

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create table events ( \
               id integer primary key, grp inet, amount numeric); \
             alter table events replica identity full",
        )
        .await
        .expect("create source table");

    let columns = HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int4)),
        ("grp".to_string(), ValueType::Other(PgType::Inet)),
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
               (1, '192.168.1.5', 10), \
               (2, '192.168.1.5', 5)",
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
            "inet_worker",
            1,
            "trellis_defs_netaddr_issue_116_regression",
            &StagedWatermark::saturated(),
        )
        .await
        .expect("drain_once")
        .expect("drain_once must claim and drain something");
    }

    // Row 1: an ordinary image-bearing insert, staged with `inet_out`'s
    // host-elided spelling — exactly what real CDC/`pgoutput` decoding
    // produces for a bare host address.
    stage_image(
        &client,
        "seg_0",
        "1",
        r#"{"grp":"192.168.1.5","amount":"10"}"#,
    )
    .await;
    // Row 2: a bare recompute trigger — no image at all — forcing
    // `read_live_rows_batch`'s live refetch to decode `grp` via
    // `row_as_text_jsonb_sql`'s `<col>::text` cast, which spells the very
    // same value `192.168.1.5/32`.
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
    assert_eq!(got_grp, "192.168.1.5/32");
    assert_eq!(got_total, "15", "the group's total must be 10 + 5");

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
    assert_eq!(expected.len(), 1);
    assert_eq!(expected[0], (got_grp, got_total));
}

// ---------------------------------------------------------------------
// 6. Typed literals
// ---------------------------------------------------------------------

/// Each family's typed literal round-trips through a real cast, and the
/// literal text is checked to already be in the exact canonical spelling
/// Postgres's own renderer (`network_show` for `inet`/`cidr`,
/// `macaddr_out`/`macaddr8_out` for the other two) would echo back —
/// `defs::typed_literal`'s round-trip-identity bar.
#[tokio::test]
async fn typed_literals_round_trip_through_a_real_cast() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    let cases: &[(&str, &str)] = &[
        ("inet", "192.168.1.5/32"),
        ("inet", "2001:db8::1/128"),
        ("cidr", "192.168.1.0/24"),
        ("cidr", "2001:db8::/32"),
        ("macaddr", "08:00:2b:01:02:03"),
        ("macaddr8", "08:00:2b:01:02:03:04:05"),
    ];
    for (pg_type, text) in cases {
        let row = client
            .query_one(&format!("select '{text}'::{pg_type}::text"), &[])
            .await
            .unwrap_or_else(|e| panic!("{pg_type} literal {text}: {e}"));
        let round_tripped: String = row.get(0);
        assert_eq!(
            &round_tripped, text,
            "{pg_type} literal must already be canonical"
        );
    }
}

/// Non-canonical spellings that `_in` would still accept must be rejected
/// by the literal checkers — the round-trip-identity bar, not merely
/// "Postgres would parse it".
#[test]
fn typed_literal_checkers_reject_non_canonical_spellings() {
    assert!(netaddr::canonical_inet("192.168.1.5/32").is_ok());
    assert!(
        netaddr::canonical_inet("192.168.1.5").is_err(),
        "inet_out's elided form is not network_show's canonical text"
    );
    assert!(netaddr::canonical_cidr("192.168.1.0/24").is_ok());
    assert!(
        netaddr::canonical_cidr("192.168.1.5/24").is_err(),
        "cidr_in accepts non-zero host bits under a smaller netmask, but cidr_out never would"
    );
    assert!(netaddr::canonical_macaddr("08:00:2b:01:02:03").is_ok());
    assert!(
        netaddr::canonical_macaddr("08-00-2b-01-02-03").is_err(),
        "macaddr_in accepts hyphens, but macaddr_out never emits them"
    );
    assert!(
        netaddr::canonical_macaddr("08:00:2B:01:02:03").is_err(),
        "macaddr_out is always lowercase"
    );
    assert!(netaddr::canonical_macaddr8("08:00:2b:01:02:03:04:05").is_ok());
}
