//! Issue #375's interim guards on a relationship endpoint that is one of this
//! instance's own targets.
//!
//! Since issue #315 every target reaches its readers through the
//! target-mutation seam and leaves the CDC publication, except a target that
//! is a relationship endpoint: that one stays published, because the settled
//! parent projection and the from-side `group_key` still need image-bearing
//! CDC. So an endpoint target has two change feeds until #375's direction 1
//! lands, and `create_relationship` guards the two ways that goes wrong:
//!
//! 1. An endpoint target gets `REPLICA IDENTITY FULL` in the relationship's
//!    own transaction. An aggregate over a seam-only target never needed it,
//!    so without this a relationship created later publishes the target with
//!    no old image, and the aggregate counts each CDC update as an insert.
//! 2. An aggregate target can't be an endpoint at all. Published, it is a CDC
//!    source, and it has no primary key to key its changes by.
//!
//! The cross-instance half of guard 2 lives in `cross_instance_target_source.rs`.
//! Everything here is driven by hand; nothing waits for convergence (#297).

use std::collections::HashMap;

use tokio_postgres::Client;
use trellis::defs::ast::ValueType;
use trellis::defs::{CatalogError, create_relationship, install_definition};
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

/// Guard 1, to-one: the target to-side gets `FULL` from the relationship
/// itself, where it used to be rejected until the operator ran the `ALTER`.
/// A plain source endpoint is still only checked, never altered (ADR-0005).
#[tokio::test]
async fn a_to_one_relationship_puts_its_target_to_side_on_replica_identity_full() {
    let (_cluster, db, raw) = pgoutput_intake::database().await;
    raw.batch_execute(
        "create table public.src (id integer primary key, val numeric); \
         create table public.reports (id integer primary key, oid integer); \
         alter table public.reports replica identity full; \
         create table public.notes (id integer primary key, oid integer)",
    )
    .await
    .expect("create sources");
    install_definition(
        &db.pool,
        "TRANSFORM h1 FROM public.src SELECT val AS val",
        &numeric_columns(&["id", "val"]),
        "public",
    )
    .await
    .expect("install h1");
    assert_eq!(replica_identity(&raw, "public.h1").await, "d");

    // `notes` keeps its default identity, so this is rejected, and the whole
    // transaction with it: the target's identity must not change either.
    let err = create_relationship(&db.pool, "RELATIONSHIP noted FROM notes.oid TO h1.id")
        .await
        .expect_err("a plain from-side without FULL is still rejected");
    assert!(
        err.to_string().contains("notes"),
        "the rejection names the plain source, not the target: {err}"
    );
    assert_eq!(replica_identity(&raw, "public.notes").await, "d");
    assert_eq!(replica_identity(&raw, "public.h1").await, "d");

    create_relationship(&db.pool, "RELATIONSHIP rollup FROM reports.oid TO h1.id")
        .await
        .expect("a to-one relationship onto a target");
    assert_eq!(replica_identity(&raw, "public.h1").await, "f");
}

/// Guard 1, issue #375 point (c): an aggregate over a 1-1 target needs no
/// `REPLICA IDENTITY FULL` while the target is seam-only. A to-many
/// relationship created later, with the target as its from-side (the one
/// endpoint `create_relationship` otherwise lets through on the default
/// identity), publishes it. Without `FULL`, intake stages the target's CDC
/// update with no old image, and the aggregate applies it as an insert into
/// the new group without taking the row out of the old one.
///
/// This pins what intake stages for the aggregate rather than the
/// aggregate's totals: the endpoint's second feed (the seam's recompute plus
/// its own CDC) still double-counts every write until #321's recompute
/// horizon lands, which is #375 point (b), not this guard's.
#[tokio::test]
async fn a_target_that_later_becomes_a_to_many_from_side_publishes_updates_with_old_images() {
    let (cluster, db, raw) = pgoutput_intake::database().await;
    raw.batch_execute(
        "create table public.src (id integer primary key, val numeric); \
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
    assert_eq!(replica_identity(&raw, "public.h1").await, "d");

    create_relationship(&db.pool, "RELATIONSHIP cats FROM h1.id TO categories.hid")
        .await
        .expect("a to-many relationship whose from-side is a target");
    assert_eq!(
        replica_identity(&raw, "public.h1").await,
        "f",
        "the relationship publishes h1, so it must carry old images"
    );

    let mut chain = Pipeline::attach(
        cluster,
        db,
        raw,
        &["public.src", "public.h1", "public.categories"],
    )
    .await;
    chain
        .raw
        .execute(
            "insert into public.src (id, val) values (1, 10), (2, 10)",
            &[],
        )
        .await
        .expect("insert into src");
    chain.settle().await;

    // The first round decodes the source update and drains it, which writes
    // h1. The second feed decodes h1's own update and stages it, undrained.
    chain
        .raw
        .execute("update public.src set val = 20 where id = 1", &[])
        .await
        .expect("update src");
    chain.feed_and_drain().await;
    chain.feed_intake().await;

    let slot: i16 = chain
        .raw
        .query_one("select ring_slot from segment_pointer", &[])
        .await
        .expect("read segment_pointer")
        .get(0);
    let staged: HashMap<String, Option<String>> = chain
        .rows(&format!(
            "select key, old_image->>'val' from seg_{slot} \
             where src_table = 'public.h1' and op = 'update'"
        ))
        .await;
    assert_eq!(
        staged,
        rows(&[("1", "10")]),
        "the aggregate needs h1's old image to take the row out of group 10"
    );

    chain.finish().await;
}

/// Guard 2, same instance, issue #375 point (a): an aggregate target is
/// refused as either endpoint of a relationship, since a published endpoint
/// is a CDC source and an aggregate target has no primary key. The join key
/// here is an integer grouping column, so nothing else would reject it.
#[tokio::test]
async fn an_aggregate_target_is_refused_as_a_relationship_endpoint() {
    let (_cluster, db, raw) = pgoutput_intake::database().await;
    raw.batch_execute(
        "create table public.sales (id integer primary key, region integer, amount integer); \
         alter table public.sales replica identity full; \
         create table public.stores (id integer primary key, region integer); \
         alter table public.stores replica identity full",
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
    // Whatever FULL would buy, it can't give the target a key.
    raw.batch_execute("alter table public.region_totals replica identity full")
        .await
        .expect("widen region_totals's replica identity");

    for text in [
        "RELATIONSHIP totals FROM stores.region TO region_totals.region",
        "RELATIONSHIP stores FROM region_totals.region TO stores.region",
    ] {
        match create_relationship(&db.pool, text).await {
            Err(CatalogError::RelationshipEndpointIsAggregateTarget { endpoint }) => {
                assert_eq!(endpoint, "public.region_totals", "{text}");
            }
            other => {
                panic!("expected RelationshipEndpointIsAggregateTarget for {text}, got {other:?}")
            }
        }
    }
}
