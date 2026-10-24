//! End-to-end tests for issue #117's user-defined enum type support — as
//! join/primary-key and `GROUP BY` key roles, and the `MIN`/`MAX` result
//! type, honoring the type's *creation-order* comparison rather than an
//! alphabetical one.
//!
//! # Why these run against a live server
//!
//! Per #111-#119's playbook: check `pg_cast` for a second `::text` renderer
//! rather than assume `enumout` is the whole story, check `to_jsonb`
//! agreement, check that `min`/`max` actually exist and keep their argument's
//! own concrete type.
//!
//! **Text-stability.** `select castfunc from pg_cast where castsource =
//! 'an enum type'::regtype and casttarget = 'text'::regtype` returns no rows
//! on a live Postgres 17 (verified below) — unlike `boolean` (#119), an enum
//! type has no second, dedicated `::text` cast function. `enumout` is what
//! `<col>::text` calls, and it renders each value as its own label text
//! verbatim: an enum value's identity *is* that label (a `pg_enum` row keyed
//! by OID), so there is no deeper representation for two distinct values to
//! collide under — a bijection by construction, the same conclusion `bit`/
//! `bit varying` reached (#118) for a structurally different reason (a
//! fixed alphabet rather than a fixed label set).
//!
//! **Key roles.** Both land cleanly: the `JOIN` key/primary-key role gains
//! a *dynamic* counterpart to the static `catalog::TEXT_STABLE_JOIN_KEY_TYPES`
//! allowlist (a live catalog probe of the column's type, since
//! there is no way to enumerate "every enum type" ahead of time the way a
//! fixed builtin family can be), and the `GROUP BY` key gate admits
//! `Other(PgType::Enum(_))` outright — no DDL-typmod trap the way
//! fixed-length `bit` has, since an enum type's bare name already names its
//! complete, exact value domain.
//!
//! **`MIN`/`MAX`.** `anyenum` has a full btree opclass ordered by
//! `pg_enum.enumsortorder` (creation position, not alphabetical), and
//! `pg_typeof(min(v))` keeps the argument's own concrete enum type — the same
//! "own family, own terms" shape `inet`'s `MIN`/`MAX` landed under (#116).
//!
//! The one place this type family is *not* like every earlier one: it is the
//! first whose ordering is not a fixed, universal property of the family
//! itself but a live, per-type, schema-defined fact — see
//! `trellis::defs::eval::EvalError::EnumOrderingUnavailable`'s own doc
//! comment (pinned by unit tests in `defs::eval`'s own test module, not
//! here) for the one evaluator-internal consequence of that: the pure,
//! DB-less Rust fold this issue's `MIN`/`MAX` support shares with every
//! other family cannot always answer a multi-distinct-value group's
//! ordering honestly, and refuses rather than guess.
//!
//! Per ADR-0013 every comparison is against independently-authored SQL —
//! plain `pg_cast`, plain `to_jsonb`, plain `min`/`max`, plain `group by` —
//! never against `defs::oracle::recompute`, which would be the engine's own
//! renderer grading itself. Harness conventions follow `defs_netaddr.rs`/
//! `defs_bit.rs`.

use std::collections::HashMap;

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::ValueType;
use trellis::defs::pg_type::PgType;
use trellis::defs::{
    CatalogError, ValidationError, create_aggregate_target_table, create_definition,
    install_definition, parse, registry, source_primary_key, validate,
};
use trellis::integer::IntWidth;
use trellis::staging::{apply, seal};

// ---------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------

/// The column typing the no-DB validator tests below check against. A
/// hand-built `PgType::Enum` literal needs no live classification — see
/// `defs::pg_type::PgType::Enum`'s own doc comment on why its payload is
/// just an interned `&'static str` token.
fn source_columns() -> HashMap<String, ValueType> {
    HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int8)),
        (
            "p".to_string(),
            ValueType::Other(PgType::Enum("enum:public.priority_enum")),
        ),
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
/// (`catalog::is_text_stable_join_key_type`) a 1-1 primary key does.
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
// 1. Text-stability: no second renderer
// ---------------------------------------------------------------------

/// An enum type has no dedicated `::text` cast function — unlike `boolean`
/// (#119), `::text` is `enumout` directly. Contrasted with `boolean` as the
/// control, so the probe itself is confirmed discriminating.
#[tokio::test]
async fn enum_has_no_dedicated_text_cast_function() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute("create type priority_enum as enum ('low', 'medium', 'high')")
        .await
        .expect("create enum type");

    let row = client
        .query_one(
            "select count(*) from pg_cast \
             where castsource = 'priority_enum'::regtype and casttarget = 'text'::regtype",
            &[],
        )
        .await
        .expect("pg_cast probe for priority_enum");
    let n: i64 = row.get(0);
    assert_eq!(n, 0, "an enum type must have no dedicated ::text cast");

    // `boolean` is the control: it *does* have one.
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

/// `to_jsonb` and `::text` must agree — the render-agreement half of the
/// #113/#248 defect shape. An enum is not a datetime family (the only
/// families `to_jsonb` special-cases), so this is expected to hold, but is
/// checked live rather than assumed.
#[tokio::test]
async fn to_jsonb_and_text_agree_for_enum() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create type priority_enum as enum ('low', 'medium', 'high'); \
             create table renderers (p priority_enum); \
             insert into renderers values ('medium'::priority_enum), (null)",
        )
        .await
        .expect("seed renderers");

    let rows = client
        .query(
            "select p::text, (to_jsonb(renderers.*) ->> 'p') from renderers",
            &[],
        )
        .await
        .expect("sweep renderers");
    assert_eq!(rows.len(), 2);
    for row in rows {
        let (p_text, p_jsonb): (Option<String>, Option<String>) = (row.get(0), row.get(1));
        assert_eq!(p_text, p_jsonb, "::text and to_jsonb must agree on enum");
    }
}

// ---------------------------------------------------------------------
// 2. Key roles
// ---------------------------------------------------------------------

/// An enum column is admitted as both a `JOIN` `ON` key and a 1-1
/// primary key — the dynamic, catalog-probed counterpart to the static
/// `catalog::TEXT_STABLE_JOIN_KEY_TYPES` allowlist every other admitted
/// family reaches through.
#[tokio::test]
async fn enum_join_key_and_primary_key_are_admitted() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create type priority_enum as enum ('low', 'medium', 'high'); \
             create table parent (k priority_enum primary key); \
             create table child (id bigint primary key, k priority_enum)",
        )
        .await
        .expect("create join tables");

    define_join_on(&db.pool, "j", ("child", "k"), ("parent", "k"))
        .await
        .unwrap_or_else(|e| panic!("an enum column must be accepted as a join key: {e}"));

    let pk = source_primary_key(&db.pool, "parent")
        .await
        .unwrap_or_else(|e| panic!("a single-column enum primary key must be accepted: {e}"));
    assert_eq!(pk.len(), 1);
    // Rendered qualified, whatever the search path (ADR-0020).
    assert_eq!(pk[0].data_type, "trellis.priority_enum");
}

/// A hand-built enum type must never be conflated with a different one, even
/// when both are otherwise perfectly good join key types: a `JOIN`'s `ON`
/// type-check still rejects joining two distinct enum types against each
/// other exactly as it would `uuid` against `bigint`, since
/// [`registry::PgType::Enum`]'s whole point is carrying each type's own
/// identity rather than conflating "enum-ness".
#[tokio::test]
async fn two_distinct_enum_types_are_not_comparable_as_a_join_key() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create type priority_enum as enum ('low', 'medium', 'high'); \
             create type status_enum as enum ('open', 'closed'); \
             create table parent (k status_enum primary key); \
             create table child (id bigint primary key, k priority_enum)",
        )
        .await
        .expect("create join tables");

    let err = define_join_on(&db.pool, "j", ("child", "k"), ("parent", "k"))
        .await
        .expect_err("two distinct enum types must not be treated as comparable");
    assert!(
        matches!(
            &err,
            CatalogError::Validate(ValidationError::JoinKeyTypeMismatch { lhs_type, rhs_type, .. })
                if lhs_type.ends_with("priority_enum") && rhs_type.ends_with("status_enum")
        ),
        "{err:?}"
    );
}

/// The `GROUP BY` key gate admits an enum type outright: unlike fixed-length
/// `bit`'s `bit(1)` narrowing default, an enum type's bare name already
/// names its complete, exact value domain, so
/// `ddl::create_aggregate_target_table` declaring a `GROUP BY` key's own
/// column from bare `ValueType` alone can never lose precision the way a
/// bare `bit` column declaration can.
#[test]
fn enum_group_by_key_is_admitted() {
    let def =
        parse("TRANSFORM t FROM s GROUP BY p WITHOUT SOURCE INDEX SELECT p AS k, SUM(n) AS total")
            .expect("parses");
    validate(&def, &source_columns(), &HashMap::new())
        .unwrap_or_else(|e| panic!("an enum column must be accepted as a GROUP BY key: {e}"));
}

/// Verified live, not assumed from "no second renderer": an enum `GROUP BY`
/// key seeded once through an image-bearing CDC insert (`enumout`'s
/// spelling, verbatim) and once through a bare-recompute live read
/// (`<col>::text`'s spelling) must land as *one* Postgres-equivalent group,
/// not two — the same regression shape `defs_boolean.rs`/`defs_netaddr.rs`
/// each pin for `boolean`/`inet`, whose two renderers genuinely disagree.
/// This is the belt-and-suspenders check that enum's single-renderer
/// argument (section 1, above) is actually true end-to-end, including
/// any in-memory grouping that compares text byte-for-byte — exactly where
/// `boolean`/`inet`'s hazard lived on the old aggregate path.
#[tokio::test]
async fn an_enum_group_key_seeded_by_cdc_and_by_live_read_is_one_group_not_two() {
    const DEF_SQL: &str = "TRANSFORM totals FROM events GROUP BY grp WITHOUT SOURCE INDEX SELECT grp AS grp, SUM(amount) AS total";

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute(
            "create type public.priority_enum as enum ('low', 'medium', 'high'); \
             create table events ( \
               id integer primary key, grp priority_enum, amount numeric(12,0))",
        )
        .await
        .expect("create source table");

    let columns = HashMap::from([
        ("id".to_string(), ValueType::Integer(IntWidth::Int4)),
        (
            "grp".to_string(),
            ValueType::Other(PgType::Enum("enum:public.priority_enum")),
        ),
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
            "insert into events (id, grp, amount) values (1, 'medium', 10), (2, 'medium', 5)",
        )
        .await
        .expect("seed source rows");

    async fn stage_image(client: &Client, key: &str, new_image: &str) {
        let src_table = format!("{DEFAULT_SCHEMA}.events");
        client
            .execute(
                "insert into seg_0 \
                 (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                 values ($1, $2, 'insert', $3, null, $4::text::jsonb, 0)",
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

    async fn stage_bare_recompute(client: &Client, key: &str) {
        let src_table = format!("{DEFAULT_SCHEMA}.events");
        client
            .execute(
                "insert into seg_0 \
                 (src_table, key, op, lsn, old_image, new_image, hop_gen) \
                 values ($1, $2, 'recompute', null, null, null, 0)",
                &[&src_table, &key],
            )
            .await
            .unwrap_or_else(|e| panic!("stage bare recompute {key}: {e}"));
    }

    // Row 1: an ordinary image-bearing insert, staged with `enumout`'s
    // spelling — exactly what a capture trigger images.
    stage_image(&client, "1", r#"{"grp":"medium","amount":"10"}"#).await;
    // Row 2: a bare recompute trigger — no image at all — forcing a live
    // refetch that decodes `grp` via `row_as_text_jsonb_sql`'s `<col>::text`
    // cast instead.
    stage_bare_recompute(&client, "2").await;

    let outcome = seal::seal_phase1(&mut client).await.expect("seal phase 1");
    seal::seal_phase2(&client, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");
    apply::drain_once(
        &db.pool,
        outcome.sealed_seg_seq,
        "enum_group_key_worker",
        1,
        "trellis_defs_enum_issue_117_group_key",
    )
    .await
    .expect("drain_once")
    .expect("drain_once must claim and drain something");

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
    assert_eq!(got_grp, "medium");
    assert_eq!(got_total, "15", "the group's total must be 10 + 5");
}

// ---------------------------------------------------------------------
// 3. MIN/MAX: creation-order, not alphabetical
// ---------------------------------------------------------------------

/// `min(enum)`/`max(enum)` are real Postgres aggregates that keep their
/// argument's own concrete enum type (`pg_typeof`, not assumed), and their
/// comparison is the type's *creation-order* one — demonstrated by choosing
/// label spellings where the two orders disagree outright, so a passing
/// assertion here could not be explained by an accidental alphabetical
/// fallback anywhere in the stack.
#[tokio::test]
async fn enum_min_max_keeps_its_type_and_honors_creation_order_not_alphabetical() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;

    // Creation order: bravo (1) < alpha (2) — the *opposite* of alphabetical
    // (alpha < bravo). A naive lexicographic MIN/MAX would get this backwards.
    client
        .batch_execute("create type public.flip_enum as enum ('bravo', 'alpha')")
        .await
        .expect("create enum type");

    let row = client
        .query_one(
            "select pg_typeof(min(v))::text, pg_typeof(max(v))::text \
             from (values ('bravo'::flip_enum), ('alpha'::flip_enum)) t(v)",
            &[],
        )
        .await
        .expect("pg_typeof probe");
    let (min_ty, max_ty): (String, String) = (row.get(0), row.get(1));
    assert_eq!(min_ty, "flip_enum");
    assert_eq!(max_ty, "flip_enum");

    let row = client
        .query_one(
            "select min(v)::text, max(v)::text \
             from (values ('bravo'::flip_enum), ('alpha'::flip_enum)) t(v)",
            &[],
        )
        .await
        .expect("min/max probe");
    let (min_v, max_v): (String, String) = (row.get(0), row.get(1));
    assert_eq!(min_v, "bravo", "creation order, not alphabetical, must win");
    assert_eq!(max_v, "alpha", "creation order, not alphabetical, must win");

    let pg_type = registry::aggregate_result_type(
        "MIN",
        ValueType::Other(PgType::Enum("enum:public.flip_enum")),
    );
    assert_eq!(
        pg_type,
        Some(ValueType::Other(PgType::Enum("enum:public.flip_enum"))),
        "MIN(enum) must resolve to Other(Enum), mirroring its argument's own type"
    );
    assert_eq!(
        registry::aggregate_result_type(
            "MAX",
            ValueType::Other(PgType::Enum("enum:public.flip_enum"))
        ),
        pg_type
    );
}
