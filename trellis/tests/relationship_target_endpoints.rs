//! Issue #375's direction 1 (#403): a relationship endpoint that is one of
//! this instance's own targets is never captured. The target-mutation seam
//! is its only change feed, staging each write CDC-shaped (prior and new
//! image, write token as `lsn`), so `create_relationship` asks nothing of the
//! target's key, and an aggregate target may be an endpoint like a 1-1 one.
//!
//! This replaces #375's interim guards (#400), which kept the endpoint in
//! the change stream and refused an aggregate target as an endpoint.
//! The permanent half of that guard, for an endpoint this instance doesn't
//! own, lives in `cross_instance_target_source.rs`.
//!
//! Everything here is driven by hand; nothing waits for convergence (#297).

use std::collections::HashMap;

use trellis::defs::ast::ValueType;
use trellis::defs::{
    CatalogError, RelationshipSide, create_relationship, install_definition, tables_to_capture,
};
use trellis::integer::IntWidth;

#[path = "support/trigger_pipeline.rs"]
mod trigger_pipeline;

use trigger_pipeline::Pipeline;

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

fn rows(pairs: &[(&str, &str)]) -> HashMap<String, Option<String>> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), Some(v.to_string())))
        .collect()
}

/// A target is accepted as a to-one and a to-many to-side, and it stays
/// uncaptured: `tables_to_capture` doesn't list it.
#[tokio::test]
async fn a_target_endpoint_stays_uncaptured() {
    let (_cluster, db, raw) = trigger_pipeline::database().await;
    raw.batch_execute(
        "create table public.src (id integer primary key, val numeric, grp integer); \
         create table public.reports (id integer primary key, oid integer)",
    )
    .await
    .expect("create sources");
    install_definition(
        &db.pool,
        "TRANSFORM h1 FROM public.src SELECT val AS val, grp AS grp",
        &HashMap::from([
            ("id".to_string(), ValueType::Integer(IntWidth::Int4)),
            ("val".to_string(), ValueType::Numeric),
            ("grp".to_string(), ValueType::Integer(IntWidth::Int4)),
        ]),
        "public",
    )
    .await
    .expect("install h1");
    trellis::intake::markers::settle_registrations(&db.pool).await;

    create_relationship(&db.pool, "RELATIONSHIP rollup FROM reports.oid TO h1.id")
        .await
        .expect("a to-one relationship onto a target");
    create_relationship(&db.pool, "RELATIONSHIP by_grp FROM reports.oid TO h1.grp")
        .await
        .expect("a to-many relationship onto a target, whose to_col is no part of its key");
    let captured = tables_to_capture(&db.pool)
        .await
        .expect("tables_to_capture");
    assert!(
        !captured.contains(&"public.h1".to_string()),
        "an endpoint target is not captured: {captured:?}"
    );
}

/// Issue #375 point (c), the case guard 1 existed for: a 1-1 target read by
/// an aggregate becomes a to-many relationship's from-side. It used to join
/// the change stream then, so an update could reach the aggregate as CDC
/// with no old image, counted as an insert into the new
/// group without leaving the old one. Now it stays uncaptured (asserted by
/// [`Pipeline::attach`]), and the aggregate sees each write once, through the
/// seam, with the prior image the seam captured under its row lock.
#[tokio::test]
async fn a_target_that_becomes_a_to_many_from_side_reaches_its_aggregate_through_the_seam_alone() {
    let (cluster, db, raw) = trigger_pipeline::database().await;
    raw.batch_execute(
        "create table public.src (id integer primary key, val numeric); \
         create table public.categories (id integer primary key, hid integer)",
    )
    .await
    .expect("create sources");
    for text in [
        "TRANSFORM h1 FROM public.src SELECT val AS val",
        "TRANSFORM agg FROM public.h1 GROUP BY val SELECT COUNT(*) AS n",
    ] {
        install_definition(&db.pool, text, &numeric_columns(&["id", "val"]), "public")
            .await
            .unwrap_or_else(|e| panic!("install {text:?}: {e}"));
        trellis::intake::markers::settle_registrations(&db.pool).await;
    }
    create_relationship(&db.pool, "RELATIONSHIP cats FROM h1.id TO categories.hid")
        .await
        .expect("a to-many relationship whose from-side is a target");

    let mut chain = Pipeline::attach(cluster, db, raw, &["public.src", "public.categories"]).await;
    chain
        .raw
        .execute(
            "insert into public.src (id, val) values (1, 10), (2, 10)",
            &[],
        )
        .await
        .expect("insert into src");
    chain.drain().await;
    chain
        .raw
        .execute("update public.src set val = 20 where id = 1", &[])
        .await
        .expect("update src");
    chain.drain().await;

    assert_eq!(
        chain
            .rows("select trim_scale(val)::text, n::text from public.agg")
            .await,
        rows(&[("10", "1"), ("20", "1")]),
        "row 1 left group 10 and joined group 20, counted once"
    );
}

/// Guard 2's same-instance half is gone: an aggregate target is accepted as
/// either endpoint, though it has no primary key. The join key here is an integer grouping column, so nothing else
/// would reject it. The seam feeding such an endpoint is pinned in
/// `endpoint_seam_feed.rs`.
#[tokio::test]
async fn an_aggregate_target_is_accepted_as_a_relationship_endpoint() {
    let (_cluster, db, raw) = trigger_pipeline::database().await;
    raw.batch_execute(
        "create table public.sales (id integer primary key, region integer, amount integer); \
         create table public.stores (id integer primary key, region integer); \
         create table public.regions (id integer primary key, name text)",
    )
    .await
    .expect("create sources");
    install_definition(
        &db.pool,
        "TRANSFORM region_totals FROM sales GROUP BY region SELECT SUM(amount) AS total",
        &HashMap::from([
            ("id".to_string(), ValueType::Integer(IntWidth::Int4)),
            ("region".to_string(), ValueType::Integer(IntWidth::Int4)),
            ("amount".to_string(), ValueType::Integer(IntWidth::Int4)),
        ]),
        "public",
    )
    .await
    .expect("install the aggregate");
    trellis::intake::markers::settle_registrations(&db.pool).await;

    for text in [
        "RELATIONSHIP totals FROM stores.region TO region_totals.region",
        "RELATIONSHIP home FROM region_totals.region TO regions.id",
    ] {
        create_relationship(&db.pool, text)
            .await
            .unwrap_or_else(|e| panic!("{text}: {e}"));
    }
    let captured = tables_to_capture(&db.pool)
        .await
        .expect("tables_to_capture");
    assert!(
        !captured.contains(&"public.region_totals".to_string()),
        "an aggregate endpoint target is not captured: {captured:?}"
    );
}

/// A target still `backfilling` can't become an endpoint yet: its initial
/// build (here the chunk queue the discharge dispatched, with no drain worker
/// to run it) writes it outside the seam, which is now the endpoint's only
/// feed, so the relationship would never hear about the rows the rest of the
/// build writes. The same rule a transform chaining off the target follows.
#[tokio::test]
async fn a_target_still_backfilling_is_refused_as_an_endpoint() {
    let (_cluster, db, raw) = trigger_pipeline::database().await;
    raw.batch_execute(
        "create table public.src (id integer primary key, val numeric); \
         insert into public.src values (1, 1), (2, 2); \
         create table public.reports (id integer primary key, oid integer)",
    )
    .await
    .expect("create sources");
    let h1 = install_definition(
        &db.pool,
        "TRANSFORM h1 FROM public.src SELECT val AS val",
        &numeric_columns(&["id", "val"]),
        "public",
    )
    .await
    .expect("install h1");
    assert_eq!(h1.status.as_str(), "waiting_to_backfill");
    trellis::intake::markers::discharge_registrations(&db.pool)
        .await
        .expect("dispatch h1's chunks");

    // Both sides (issue #429): the rule is about the endpoint, not its role.
    for text in [
        "RELATIONSHIP rollup FROM reports.oid TO h1.id",
        "RELATIONSHIP report FROM h1.id TO reports.id",
    ] {
        match create_relationship(&db.pool, text).await {
            Err(CatalogError::TransformNotLive { transform, status }) => {
                assert_eq!(transform, "h1", "{text}");
                assert_eq!(status.as_str(), "backfilling", "{text}");
            }
            other => panic!("expected TransformNotLive for {text}, got {other:?}"),
        }
    }
}

/// Issue #429: an own target is exempt from the capture-keying check, but
/// not from the key-type check. An aggregate
/// target's key is its `GROUP BY` columns, and apply keys the endpoint's
/// changes by it through the same type-gated lookup as any other endpoint's,
/// so a `numeric` grouping column would halt the instance on the first
/// change the relationship propagates. Rejected on either side, even though
/// the join key itself is an integer.
#[tokio::test]
async fn an_aggregate_target_keyed_on_an_unsupported_type_is_refused_as_an_endpoint() {
    let (_cluster, db, raw) = trigger_pipeline::database().await;
    raw.batch_execute(
        "create table public.sales (id integer primary key, region integer, tier numeric, \
                                    amount integer); \
         create table public.stores (id integer primary key, region integer); \
         create table public.regions (id integer primary key, name text)",
    )
    .await
    .expect("create sources");
    install_definition(
        &db.pool,
        "TRANSFORM tier_totals FROM sales GROUP BY region, tier SELECT SUM(amount) AS total",
        &HashMap::from([
            ("id".to_string(), ValueType::Integer(IntWidth::Int4)),
            ("region".to_string(), ValueType::Integer(IntWidth::Int4)),
            ("tier".to_string(), ValueType::Numeric),
            ("amount".to_string(), ValueType::Integer(IntWidth::Int4)),
        ]),
        "public",
    )
    .await
    .expect("install the aggregate");
    trellis::intake::markers::settle_registrations(&db.pool).await;

    for (text, side) in [
        (
            "RELATIONSHIP home FROM tier_totals.region TO regions.id",
            RelationshipSide::From,
        ),
        (
            "RELATIONSHIP totals FROM stores.region TO tier_totals.region",
            RelationshipSide::To,
        ),
    ] {
        match create_relationship(&db.pool, text).await {
            Err(CatalogError::RelationshipEndpointUnsupportedKey {
                side: got_side,
                endpoint,
                column,
                pg_type,
                ..
            }) => {
                assert_eq!(got_side, side, "{text}");
                assert_eq!(endpoint, "public.tier_totals", "{text}");
                assert_eq!(column, "tier", "{text}");
                assert_eq!(pg_type, "numeric", "{text}");
            }
            other => {
                panic!("expected RelationshipEndpointUnsupportedKey for {text}, got {other:?}")
            }
        }
    }
}
