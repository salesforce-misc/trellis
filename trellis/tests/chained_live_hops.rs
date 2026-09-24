//! Multi-hop transform chains fed by real CDC for their root source, rather
//! than the direct/ring-bypassing backfill path (`backfill_definition`,
//! ADR-0007) or a hand-staged CDC row.
//!
//! A chain's intermediate hop (`h1` in `src -> h1 -> h2`) is two things at
//! once: the *target* of the upstream definition and a *source* of the
//! downstream one. Issue #267 found that, while `h1` sat in the CDC
//! publication, a write to it was staged twice under two spellings of the
//! table — once by the applying transaction's own downstream propagation and
//! once by intake — which live-locked the downstream apply. Issue #315 then
//! took intermediate hops out of the publication altogether: every write to
//! a target reaches its readers only through the target-mutation seam
//! (`staging::target_mutations`), inside the writing transaction, so there is
//! one staging and one spelling by construction. That also fixed an
//! aggregate target feeding another transform, whose CDC could not be
//! decoded at all (issue #315's original report: intake died on the first
//! change).
//!
//! Intake's spelling of the root source is still real here: the tests read
//! `pgoutput` off a second replication slot and feed it to a real
//! `Intake::handle_event`, then seal and drain by hand
//! (`support/pgoutput_intake.rs`). Hand-staging the root's CDC row instead
//! would write intake's half of #267's spelling disagreement into the test
//! (#369). Driving every step by hand makes each test deterministic: no
//! background worker, no reconcile loop, no polling for convergence (#297).
//! A drain that fails panics with its error, and a ring that never quiesces
//! panics with its stuck `(src_table, key)` pairs, where #267 would once
//! have retried a `draining` segment forever.

use std::collections::HashMap;

use trellis::defs::ast::ValueType;
use trellis::defs::install_definition;

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

/// Installs each `TRANSFORM` in order against `columns`, taking each live
/// (the discharge, then its chunks) before the next: that is what lets the
/// next hop chain off it at all (`reject_non_live_upstream`, issue #315).
async fn install_chain(
    db: &testkit::TestDatabase,
    texts: &[&str],
    columns: &HashMap<String, ValueType>,
) {
    for text in texts {
        install_definition(&db.pool, text, columns, "public")
            .await
            .unwrap_or_else(|e| panic!("install {text:?}: {e}"));
        trellis::intake::publication::settle_registrations(&db.pool).await;
    }
}

/// Issue #267's repro, in shape: a 2-hop 1-1 passthrough chain and one
/// insert, then an update and a delete of the same row, so every CDC op for
/// the root reaches `h2` through the middle hop.
#[tokio::test]
async fn a_two_hop_one_to_one_chain_converges_without_publishing_the_middle_hop() {
    let (cluster, db, raw) = pgoutput_intake::database().await;
    raw.batch_execute("create table public.src (id integer primary key, val numeric)")
        .await
        .expect("create src");
    install_chain(
        &db,
        &[
            "TRANSFORM h1 FROM public.src SELECT val AS val",
            "TRANSFORM h2 FROM public.h1 SELECT val AS val",
        ],
        &numeric_columns(&["id", "val"]),
    )
    .await;
    let mut chain = Pipeline::attach(cluster, db, raw, &["public.src"]).await;
    const H2: &str = "select id::text, val::text from h2";

    chain
        .raw
        .execute("insert into public.src (id, val) values (1, 1)", &[])
        .await
        .expect("insert into src");
    chain.settle().await;
    assert_eq!(
        chain.rows(H2).await,
        rows(&[("1", "1")]),
        "h2 must converge through the 2-hop chain (issue #267)"
    );

    chain
        .raw
        .execute("update public.src set val = 2 where id = 1", &[])
        .await
        .expect("update src");
    chain.settle().await;
    assert_eq!(chain.rows(H2).await, rows(&[("1", "2")]));

    chain
        .raw
        .execute("delete from public.src where id = 1", &[])
        .await
        .expect("delete from src");
    chain.settle().await;
    assert_eq!(chain.rows(H2).await, rows(&[]));

    chain.finish().await;
}

/// Issue #267 follow-up (the issue's repro is 1-1 only, 2 hops only): `h2`
/// here is a *deeper* intermediate hop, and `h3` an aggregate reading it.
#[tokio::test]
async fn a_three_hop_chain_ending_in_an_aggregate_converges() {
    let (cluster, db, raw) = pgoutput_intake::database().await;
    raw.batch_execute("create table public.src (id integer primary key, val numeric)")
        .await
        .expect("create src");
    // No `REPLICA IDENTITY FULL` on `h2`: an aggregate over a target never
    // reads that target's CDC (issue #315).
    install_chain(
        &db,
        &[
            "TRANSFORM h1 FROM public.src SELECT val AS val",
            "TRANSFORM h2 FROM public.h1 SELECT val AS val",
            "TRANSFORM h3 FROM public.h2 GROUP BY val SELECT COUNT(*) AS n",
        ],
        &numeric_columns(&["id", "val"]),
    )
    .await;
    let mut chain = Pipeline::attach(cluster, db, raw, &["public.src"]).await;
    const H3: &str = "select val::text, n::text from h3";

    chain
        .raw
        .execute(
            "insert into public.src (id, val) values (1, 7), (2, 7)",
            &[],
        )
        .await
        .expect("insert into src");
    chain.settle().await;
    assert_eq!(
        chain.rows(H3).await,
        rows(&[("7", "2")]),
        "h3 must converge through the 3-hop chain ending in an aggregate (issue #267)"
    );

    // A group move two hops down: only `h2`'s prior image names group 7.
    chain
        .raw
        .execute("update public.src set val = 8 where id = 1", &[])
        .await
        .expect("move row 1 to group 8");
    chain.settle().await;
    assert_eq!(chain.rows(H3).await, rows(&[("7", "1"), ("8", "1")]));

    chain.finish().await;
}

/// Issue #315's original report: an aggregate target feeding another
/// transform. While the aggregate target was published, its first CDC change
/// killed intake (no primary key to decode a key from), so nothing
/// downstream ever converged again. `hist` counts `agg`'s groups by their
/// size, so moving a source row between `agg` groups also moves `agg` rows
/// between `hist` groups — the prior image each `agg` write carries is what
/// lets `hist` fix the group a row left.
#[tokio::test]
async fn an_aggregate_chained_off_an_aggregate_target_converges_and_follows_group_moves() {
    let (cluster, db, raw) = pgoutput_intake::database().await;
    // A text group key: a transform chained off an aggregate target reads
    // its group column as a primary key, which can't be `numeric`.
    raw.batch_execute(
        "create table public.src (id integer primary key, val text); \
         alter table public.src replica identity full",
    )
    .await
    .expect("create src");
    install_chain(
        &db,
        &["TRANSFORM agg FROM public.src GROUP BY val SELECT COUNT(*) AS n"],
        &HashMap::from([
            ("id".to_string(), ValueType::Numeric),
            ("val".to_string(), ValueType::Text),
        ]),
    )
    .await;
    install_chain(
        &db,
        &["TRANSFORM hist FROM public.agg GROUP BY n SELECT COUNT(*) AS groups"],
        &HashMap::from([
            ("val".to_string(), ValueType::Text),
            ("n".to_string(), ValueType::Numeric),
        ]),
    )
    .await;
    let mut chain = Pipeline::attach(cluster, db, raw, &["public.src"]).await;
    const HIST: &str = "select n::text, groups::text from hist";

    // agg: {a: 2 rows, b: 1 row} -> hist: {2: 1 group, 1: 1 group}.
    chain
        .raw
        .execute(
            "insert into public.src (id, val) values (1, 'a'), (2, 'a'), (3, 'b')",
            &[],
        )
        .await
        .expect("insert into src");
    chain.settle().await;
    assert_eq!(
        chain.rows(HIST).await,
        rows(&[("2", "1"), ("1", "1")]),
        "hist must converge through the aggregate chain"
    );

    // Move row 3 into group a: agg {a: 3 rows} (group b gone) -> hist
    // {3: 1 group}. agg's group a leaves hist group 2 for hist group 3, and
    // agg's group b leaves hist group 1 by going extinct: both old hist
    // groups are only reachable through each agg write's prior image.
    chain
        .raw
        .execute("update public.src set val = 'a' where id = 3", &[])
        .await
        .expect("move row 3");
    chain.settle().await;
    assert_eq!(
        chain.rows(HIST).await,
        rows(&[("3", "1")]),
        "hist must follow agg's rows out of their old groups"
    );

    // Delete a row: agg {a: 2 rows} -> hist {2: 1 group}.
    chain
        .raw
        .execute("delete from public.src where id = 1", &[])
        .await
        .expect("delete row 1");
    chain.settle().await;
    assert_eq!(
        chain.rows(HIST).await,
        rows(&[("2", "1")]),
        "hist must follow agg's shrunken group"
    );

    chain.finish().await;
}
