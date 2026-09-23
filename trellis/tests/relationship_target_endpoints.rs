//! Issue #375's direction 1 (#403): a relationship endpoint that is one of
//! this instance's own targets is never published. The target-mutation seam
//! is its only change feed, staging each write CDC-shaped (prior and new
//! image, write token as `lsn`), so `create_relationship` asks nothing of the
//! target's replica identity or key, and an aggregate target may be an
//! endpoint like a 1-1 one.
//!
//! This replaces #375's interim guards (#400), which kept the endpoint
//! published: the target was put on `REPLICA IDENTITY FULL` by the
//! relationship itself, and an aggregate target was refused as an endpoint.
//! The permanent half of that guard, for an endpoint this instance doesn't
//! own, lives in `cross_instance_target_source.rs`.
//!
//! Everything here is driven by hand; nothing waits for convergence (#297).

use std::collections::HashMap;

use tokio_postgres::Client;
use trellis::defs::ast::ValueType;
use trellis::defs::{CatalogError, create_relationship, install_definition, publication_tables};
use trellis::integer::IntWidth;

#[path = "support/pgoutput_intake.rs"]
mod pgoutput_intake;

use pgoutput_intake::Pipeline;

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

async fn replica_identity(raw: &Client, qualified: &str) -> String {
    raw.query_one(
        "select relreplident::text from pg_class where oid = to_regclass($1)",
        &[&qualified],
    )
    .await
    .expect("read relreplident")
    .get(0)
}

/// A target is accepted as a to-one and a to-many to-side on its default
/// replica identity, which the relationship leaves alone, and it stays out of
/// the publication. A plain source endpoint is still held to the identity its
/// CDC needs (ADR-0005: checked, never altered).
#[tokio::test]
async fn a_target_endpoint_keeps_its_replica_identity_and_stays_unpublished() {
    let (_cluster, db, raw) = pgoutput_intake::database().await;
    raw.batch_execute(
        "create table public.src (id integer primary key, val numeric, grp integer); \
         create table public.reports (id integer primary key, oid integer); \
         alter table public.reports replica identity full; \
         create table public.notes (id integer primary key, oid integer)",
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
    assert_eq!(replica_identity(&raw, "public.h1").await, "d");

    let err = create_relationship(&db.pool, "RELATIONSHIP noted FROM notes.oid TO h1.id")
        .await
        .expect_err("a plain from-side without FULL is still rejected");
    assert!(
        err.to_string().contains("notes"),
        "the rejection names the plain source, not the target: {err}"
    );
    assert_eq!(replica_identity(&raw, "public.notes").await, "d");

    create_relationship(&db.pool, "RELATIONSHIP rollup FROM reports.oid TO h1.id")
        .await
        .expect("a to-one relationship onto a target");
    create_relationship(&db.pool, "RELATIONSHIP by_grp FROM reports.oid TO h1.grp")
        .await
        .expect("a to-many relationship onto a target, whose to_col is no part of its key");
    assert_eq!(
        replica_identity(&raw, "public.h1").await,
        "d",
        "the relationship leaves the target's replica identity alone"
    );
    let published = publication_tables(&db.pool)
        .await
        .expect("publication_tables");
    assert!(
        !published.contains(&"public.h1".to_string()),
        "an endpoint target is not published: {published:?}"
    );
}

/// Issue #375 point (c), the case guard 1 existed for: a 1-1 target read by
/// an aggregate becomes a to-many relationship's from-side. It used to be
/// published then, on whatever identity it had, so an update reached the
/// aggregate as CDC with no old image, counted as an insert into the new
/// group without leaving the old one. Now it stays unpublished (asserted by
/// [`Pipeline::attach`]), and the aggregate sees each write once, through the
/// seam, with the prior image the seam captured under its row lock.
#[tokio::test]
async fn a_target_that_becomes_a_to_many_from_side_reaches_its_aggregate_through_the_seam_alone() {
    let (cluster, db, raw) = pgoutput_intake::database().await;
    raw.batch_execute(
        "create table public.src (id integer primary key, val numeric); \
         alter table public.src replica identity full; \
         create table public.categories (id integer primary key, hid integer); \
         alter table public.categories replica identity full",
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
    }
    create_relationship(&db.pool, "RELATIONSHIP cats FROM h1.id TO categories.hid")
        .await
        .expect("a to-many relationship whose from-side is a target");
    assert_eq!(replica_identity(&raw, "public.h1").await, "d");

    let mut chain = Pipeline::attach(cluster, db, raw, &["public.src", "public.categories"]).await;
    chain
        .raw
        .execute(
            "insert into public.src (id, val) values (1, 10), (2, 10)",
            &[],
        )
        .await
        .expect("insert into src");
    chain.settle().await;
    chain
        .raw
        .execute("update public.src set val = 20 where id = 1", &[])
        .await
        .expect("update src");
    chain.settle().await;

    assert_eq!(
        chain
            .rows("select trim_scale(val)::text, n::text from public.agg")
            .await,
        rows(&[("10", "1"), ("20", "1")]),
        "row 1 left group 10 and joined group 20, counted once"
    );

    chain.finish().await;
}

/// Guard 2's same-instance half is gone: an aggregate target is accepted as
/// either endpoint, on its default identity (it has no primary key for one to
/// name). The join key here is an integer grouping column, so nothing else
/// would reject it. The seam feeding such an endpoint is pinned in
/// `endpoint_seam_feed.rs`.
#[tokio::test]
async fn an_aggregate_target_is_accepted_as_a_relationship_endpoint() {
    let (_cluster, db, raw) = pgoutput_intake::database().await;
    raw.batch_execute(
        "create table public.sales (id integer primary key, region integer, amount integer); \
         alter table public.sales replica identity full; \
         create table public.stores (id integer primary key, region integer); \
         alter table public.stores replica identity full; \
         create table public.regions (id integer primary key, name text); \
         alter table public.regions replica identity full",
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

    for text in [
        "RELATIONSHIP totals FROM stores.region TO region_totals.region",
        "RELATIONSHIP home FROM region_totals.region TO regions.id",
    ] {
        create_relationship(&db.pool, text)
            .await
            .unwrap_or_else(|e| panic!("{text}: {e}"));
    }
    assert_eq!(replica_identity(&raw, "public.region_totals").await, "d");
    let published = publication_tables(&db.pool)
        .await
        .expect("publication_tables");
    assert!(
        !published.contains(&"public.region_totals".to_string()),
        "an aggregate endpoint target is not published: {published:?}"
    );
}

/// A target still `backfilling` can't become an endpoint yet: its initial
/// build (here the chunk queue, with no drain worker to run it) writes it
/// outside the seam, which is now the endpoint's only feed, so the
/// relationship would never hear about the rows the rest of the build
/// writes. The same rule a transform chaining off the target follows.
#[tokio::test]
async fn a_target_still_backfilling_is_refused_as_an_endpoint() {
    let (_cluster, db, raw) = pgoutput_intake::database().await;
    raw.batch_execute(
        "create table public.src (id integer primary key, val numeric); \
         insert into public.src values (1, 1), (2, 2); \
         create table public.reports (id integer primary key, oid integer); \
         alter table public.reports replica identity full",
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
    assert_eq!(h1.status.as_str(), "backfilling");

    match create_relationship(&db.pool, "RELATIONSHIP rollup FROM reports.oid TO h1.id").await {
        Err(CatalogError::TransformNotLive { transform, status }) => {
            assert_eq!(transform, "h1");
            assert_eq!(status.as_str(), "backfilling");
        }
        other => panic!("expected TransformNotLive, got {other:?}"),
    }
}
