//! Issues #312 and #315: a write to a chain's intermediate hop must reach a
//! downstream aggregate exactly once, and must leave both the group a row
//! left and the group it joined correct.
//!
//! `h1` below is the target of `src -> h1` and the source of the aggregate
//! `h1 -> h3`. Since issue #315 a target table is never published: the drain
//! that writes `h1` stages its own downstream `Recompute` for `h3` inside the
//! writing transaction (`staging::target_mutations`), and that is the only
//! copy. Before, `h1` was published too, and intake decoded the same write a
//! second time; when the two copies landed in different batches the
//! aggregate re-derived the group from live state (which already held the
//! write) and then added the CDC delta on top of it.
//!
//! The tests drive intake and the drain by hand (`support/pgoutput_intake.rs`)
//! so every hop lands in its own batch, rather than relying on seal timing.

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

fn groups(pairs: &[(&str, &str)]) -> HashMap<String, Option<String>> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), Some(v.to_string())))
        .collect()
}

/// `src -> h1 -> h3`, with only `src` published.
async fn start() -> Pipeline {
    let (cluster, db, raw) = pgoutput_intake::database().await;
    raw.batch_execute("create table public.src (id integer primary key, val numeric)")
        .await
        .expect("create src");
    let columns = numeric_columns(&["id", "val"]);
    install_definition(
        &db.pool,
        "TRANSFORM h1 FROM public.src SELECT val AS val",
        &columns,
        "public",
    )
    .await
    .expect("install h1");
    trellis::intake::publication::settle_registrations(&db.pool).await;
    // No `REPLICA IDENTITY FULL` on `h1`: an aggregate over a target
    // never reads its CDC, so it has no old-image requirement (#315).
    install_definition(
        &db.pool,
        "TRANSFORM h3 FROM public.h1 GROUP BY val SELECT COUNT(*) AS n",
        &columns,
        "public",
    )
    .await
    .expect("install h3");
    trellis::intake::publication::settle_registrations(&db.pool).await;
    Pipeline::attach(cluster, db, raw, &["public.src"]).await
}

const H3: &str = "select val::text, n::text from h3";

#[tokio::test]
async fn an_aggregate_counts_an_intermediate_hops_write_once() {
    let mut hop = start().await;

    hop.raw
        .execute("insert into public.src (id, val) values (1, 7)", &[])
        .await
        .expect("insert into src");

    // The `src` insert reaches the ring, and draining it writes `h1`, whose
    // own downstream `Recompute` then drains into `h3` in a later batch.
    hop.feed_and_drain().await;
    assert_eq!(
        hop.rows(H3).await,
        groups(&[("7", "1")]),
        "the in-transaction propagation alone must count the row once"
    );

    // Intake now decodes the transaction that wrote `h1`. `h1` isn't in the
    // publication, so nothing of that write reaches the ring a second time.
    hop.feed_and_drain().await;
    assert_eq!(
        hop.rows(H3).await,
        groups(&[("7", "1")]),
        "an intermediate hop's write must never be counted twice"
    );

    hop.finish().await;
}

/// Issue #315's grain migration: `h3` groups by `h1.val`, a non-key column
/// of an upstream target. Moving a row from group 7 to group 8 must leave
/// group 7 correct as well as group 8. The recompute `h1`'s write stages is
/// image-less (re-read live), so on its own it only names group 8; the prior
/// image it carries is what tells `h3` to re-derive group 7 too. A delete is
/// the same: only the prior image names the group the row left.
#[tokio::test]
async fn an_aggregate_over_a_hop_follows_a_row_that_moves_groups_and_then_leaves() {
    let mut hop = start().await;

    hop.raw
        .batch_execute("insert into public.src (id, val) values (1, 7), (2, 7), (3, 9)")
        .await
        .expect("seed src");
    hop.feed_and_drain().await;
    assert_eq!(hop.rows(H3).await, groups(&[("7", "2"), ("9", "1")]),);

    hop.raw
        .execute("update public.src set val = 8 where id = 1", &[])
        .await
        .expect("move row 1 from group 7 to group 8");
    hop.feed_and_drain().await;
    assert_eq!(
        hop.rows(H3).await,
        groups(&[("7", "1"), ("8", "1"), ("9", "1")]),
        "the row's old group must lose it, not only its new group gain it"
    );

    hop.raw
        .execute("update public.src set val = 9 where id = 2", &[])
        .await
        .expect("move group 7's last row into group 9");
    hop.raw
        .execute("delete from public.src where id = 1", &[])
        .await
        .expect("delete group 8's only row");
    hop.feed_and_drain().await;
    assert_eq!(
        hop.rows(H3).await,
        groups(&[("9", "2")]),
        "a group every row left, by moving or by deletion, must be removed"
    );

    hop.finish().await;
}
