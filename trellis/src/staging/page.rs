//! Bounded drain paging (issue #620, epic #556 milestone A2). See
//! docs/staging-and-claiming/04-claiming-and-the-fold.md, "Paging a share
//! larger than the cap".
//!
//! A drain holds at most `ClientOptions::drain_batch_cap` folded records at
//! once. A share that fits is folded directly, as before; a share that
//! doesn't is walked in pages, keyset-ordered on [`PageKey`], each page its
//! own compute-and-apply transaction that advances the bucket's
//! `drain_cursor` row. This module is where pages come from
//! ([`MaterializedPages`]) and where the cursor is read back
//! ([`read_cursors`]); the page's apply, claim check and cursor write live in
//! `super::apply`.

use std::collections::HashMap;

use tokio_postgres::GenericClient;

use crate::pool::{Pool, quote_literal};

use super::apply::{ApplyError, SegmentStep};
use super::fold::{self, BucketFilter, FoldedChange, PageKey};

/// One page of a share: at most the cap's worth of folded records, plus where
/// the next page starts.
#[derive(Debug)]
pub(crate) struct Page {
    pub records: Vec<FoldedChange>,
    /// The last page key this page covers when more pages follow (what the
    /// page's transaction writes to `drain_cursor`), or `None` when this page
    /// runs to the end of the share and its transaction completes the claim.
    pub next: Option<PageKey>,
}

/// Where an oversized share's pages come from (issue #620 A2b): the share is
/// folded once into a session `TEMP` table ([`fold::materialize_share`]),
/// then read back a page at a time by keyset ([`fold::read_page`]). Linear in
/// the share, where A2a's rescan pager re-scanned the fenced window for every
/// page and was quadratic.
///
/// A page never splits a key and never holds more than `cap` records;
/// successive pages, each starting after the previous page's
/// [`Page::next`], cover the materialized share exactly once.
///
/// **The session is this struct's own.** A `TEMP` table lives in one backend,
/// so the table and every page read of it share one connection, opened by
/// [`Pool::connect_unpooled`] rather than borrowed from the pool, and owned
/// here. Dropping this struct closes the connection, and Postgres drops the
/// table with the session. That holds however the drain call ends: finished,
/// failed on a page, or its future dropped mid-page. A pooled connection
/// never carries the table, so no later borrower can inherit a stale one;
/// and the page loop never waits on the pool for the session, so a paged
/// drain still runs on a one-connection pool. A reclaimed share's next
/// claimant has its own session and materializes again from the bucket's
/// cursor.
pub(crate) struct MaterializedPages {
    client: tokio_postgres::Client,
    seg_seq: i64,
}

impl MaterializedPages {
    /// Opens the session a paged drain of `seg_seq` materializes into.
    pub(crate) async fn open(pool: &Pool, seg_seq: i64) -> Result<Self, ApplyError> {
        Ok(Self {
            client: pool.connect_unpooled().await?,
            seg_seq,
        })
    }

    /// Folds `filter`'s share from strictly after `after` (a resumed
    /// cursor, or the start) into the session's table, replacing the table
    /// any earlier cursor group left. One transaction, committed before any
    /// page applies. Returns the folded record count.
    pub(crate) async fn materialize(
        &mut self,
        filter: &BucketFilter,
        after: Option<&PageKey>,
    ) -> Result<u64, ApplyError> {
        let txn = self.client.transaction().await?;
        let records = fold::materialize_share(&txn, self.seg_seq, filter, after).await?;
        txn.commit().await?;
        Ok(records)
    }

    /// The next page of the last [`Self::materialize`]d share after `after`.
    /// Its caller passes the previous page's [`Page::next`], starting from
    /// the `after` it materialized from.
    pub(crate) async fn next_page(
        &self,
        after: Option<&PageKey>,
        cap: usize,
    ) -> Result<Page, ApplyError> {
        let (records, next) = fold::read_page(&self.client, after, cap).await?;
        Ok(Page { records, next })
    }
}

/// The committed `drain_cursor` rows for `buckets` of `seg_seq`: bucket to
/// the last page key a committed page covered. A bucket with no row has
/// applied nothing of this segment yet.
pub(crate) async fn read_cursors(
    client: &impl GenericClient,
    seg_seq: i64,
    buckets: &[i16],
) -> Result<HashMap<i16, PageKey>, ApplyError> {
    let rows = client
        .query(
            "select bucket, after_route, after_src_table, after_key from drain_cursor \
             where seg_seq = $1 and bucket = any($2::smallint[])",
            &[&seg_seq, &buckets],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            (
                row.get(0),
                PageKey {
                    route: row.get(1),
                    src_table: row.get(2),
                    key: row.get(3),
                },
            )
        })
        .collect())
}

/// The ring rows a Phase 3 transaction applies itself (issue #762): every
/// row of its claimed buckets up to the page it is applying. They are
/// committed with the transaction's own drain-state update, so to anyone
/// reading after it they are applied, and to the transaction itself they
/// are its own page: a record it already applied wrote what that row
/// names, and one it hasn't yet will.
pub(crate) struct ClaimScope<'a> {
    pub(crate) steps: &'a [SegmentStep],
    pub(crate) claimed_by: &'a str,
}

impl ClaimScope<'_> {
    /// SQL, over ring row `r`, true when `r` is in this claim, given its
    /// owning segment `owner` and its bucket there, `bucket` (both SQL).
    fn sql(&self, owner: &str, bucket: &str) -> String {
        let key = "(r.route, r.src_table collate \"C\", r.key collate \"C\")";
        let arms: Vec<String> = self
            .steps
            .iter()
            .map(|step| {
                let seg_seq = step.seg_seq;
                match &step.page {
                    // The pre-paging contract: every bucket `claimed_by`
                    // holds, whole.
                    None => format!(
                        "({owner}.seg_seq = {seg_seq} and exists (select 1 from seg_claims k \
                             where k.seg_seq = {seg_seq} and k.bucket = {bucket} \
                               and k.claimed_by = {claimed_by}))",
                        claimed_by = quote_literal(self.claimed_by),
                    ),
                    // The page's buckets, from their cursor (rows at or
                    // below it are applied already) through `next`, or to
                    // the end on a final page. A bucket held but paged
                    // separately is not in this transaction.
                    Some(page) => {
                        let buckets = page
                            .buckets
                            .iter()
                            .map(i16::to_string)
                            .collect::<Vec<_>>()
                            .join(", ");
                        let through = page.next.as_ref().map_or(String::new(), |next| {
                            format!(
                                " and {key} <= ({}::bigint, {} collate \"C\", {} collate \"C\")",
                                next.route,
                                quote_literal(&next.src_table),
                                quote_literal(&next.key),
                            )
                        });
                        format!(
                            "({owner}.seg_seq = {seg_seq} \
                              and {bucket} = any(array[{buckets}]::int[]){through})"
                        )
                    }
                }
            })
            .collect();
        if arms.is_empty() {
            "false".to_string()
        } else {
            arms.join(" or ")
        }
    }
}

/// SQL, over ring row `r` of ring slot `slot` (issue #762): true while `r`'s
/// change is still pending, that is, no committed drain has applied it, and
/// `claim`, when given, is not applying it now.
///
/// A change is applied once its bucket has, which drain state alone says:
/// its owning segment has drained (`state = 'drained'`), its bucket's bit is
/// in that segment's `drained_mask`, or its bucket's `drain_cursor` is at or
/// past its page key (`staging::fold::PageKey`, `(route, src_table, key)`
/// byte-wise: every ring row of a key has the same one, and a committed
/// cursor means every key through it applied). Each is written in the
/// transaction that applied the row, so it is monotonic: once a reader sees
/// a change applied, every later reader does, whatever has been written to
/// a projection since. A change parked as a poisoned key's
/// (`quarantine::park_batch_contribution`) is applied in this sense: its
/// page committed without it, and it never writes anything again.
///
/// The owning segment is the fenced window's (`seal::fenced_window`): the
/// segment in `slot` (a ring slot holds one live segment) when the row is in
/// its fence, or the next segment when it isn't (a late writer's row,
/// which that segment's window takes). A row whose owner has no fence yet
/// (the active segment, or a seal's crash window) is pending, as is one
/// whose owner can't be found.
pub(crate) fn ring_row_pending_sql(slot: i16, claim: Option<&ClaimScope<'_>>) -> String {
    let applied = |owner: &str| {
        let bucket = format!("(r.route % {owner}.bucket_count)::int");
        let in_claim = claim.map_or(String::new(), |claim| {
            format!(" or {}", claim.sql(owner, &bucket))
        });
        format!(
            "({owner}.state = 'drained' \
              or {owner}.drained_mask & (1::bigint << {bucket}) <> 0 \
              or exists (select 1 from drain_cursor c \
                         where c.seg_seq = {owner}.seg_seq and c.bucket = {bucket} \
                           and (r.route, r.src_table collate \"C\", r.key collate \"C\") \
                               <= (c.after_route, c.after_src_table collate \"C\", \
                                   c.after_key collate \"C\")){in_claim})"
        )
    };
    format!(
        "exists (select 1 from segments s where s.ring_slot = {slot} and \
             case when s.fence_snapshot is null then true \
                  when pg_visible_in_snapshot(r.row_txid, s.fence_snapshot) then not {own} \
                  else not exists (select 1 from segments n \
                                   where n.seg_seq = s.seg_seq + 1 \
                                     and n.fence_snapshot is not null \
                                     and pg_visible_in_snapshot(r.row_txid, n.fence_snapshot) \
                                     and {successor}) end)",
        own = applied("s"),
        successor = applied("n"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DEFAULT_SCHEMA;
    use crate::staging::apply::PageClaim;
    use crate::staging::seal;

    const WAKE: &str = "page_pending_test_wake";

    /// Stages one `orders` update for `key` into ring slot 0.
    async fn stage(client: &tokio_postgres::Client, key: &str) {
        client
            .execute(
                "insert into seg_0 (src_table, key, op, lsn, old_image, new_image, \
                                    origin_lsn, src_changed, hop_gen) \
                 values ('orders', $1, 'update', pg_current_wal_insert_lsn(), \
                         '{\"v\": 0}', '{\"v\": 1}', pg_current_wal_insert_lsn(), now(), 0)",
                &[&key],
            )
            .await
            .expect("stage update");
    }

    /// Whether each of `keys`' ring rows in slot 0 is pending, under `claim`.
    async fn pending(
        client: &tokio_postgres::Client,
        keys: &[&str],
        claim: Option<&ClaimScope<'_>>,
    ) -> Vec<bool> {
        let mut out = Vec::new();
        for key in keys {
            out.push(
                client
                    .query_one(
                        &format!(
                            "select {} from seg_0 r where r.key = $1",
                            ring_row_pending_sql(0, claim)
                        ),
                        &[key],
                    )
                    .await
                    .expect("evaluate pending")
                    .get(0),
            );
        }
        out
    }

    /// `key`'s page key in slot 0.
    async fn page_key(client: &tokio_postgres::Client, key: &str) -> fold::PageKey {
        let row = client
            .query_one(
                "select route, src_table, key from seg_0 where key = $1",
                &[&key],
            )
            .await
            .expect("read the page key");
        fold::PageKey {
            route: row.get(0),
            src_table: row.get(1),
            key: row.get(2),
        }
    }

    /// Issue #762: a ring row is pending exactly until drain state says its
    /// bucket applied it: its segment's `drained_mask` bit, or its bucket's
    /// cursor at or past its page key. A late writer's row belongs to the
    /// next segment, and the transaction applying a row counts it applied.
    #[tokio::test]
    async fn a_ring_row_is_pending_until_its_bucket_applies_it() {
        let cluster = testkit::TestCluster::start();
        let db = cluster.create_isolated_database().await;
        let (mut client, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public"))
            .await
            .expect("set search_path");

        stage(&client, "a").await;
        assert_eq!(
            pending(&client, &["a"], None).await,
            [true],
            "the active segment's row"
        );
        let sealed = seal::seal_phase1(&mut client).await.expect("seal phase 1");
        seal::seal_phase2(&client, sealed.sealed_seg_seq, WAKE)
            .await
            .expect("seal phase 2");
        let seg = sealed.sealed_seg_seq;
        // A late writer's row in the sealed segment's slot: the next
        // segment's.
        stage(&client, "late").await;
        let a = page_key(&client, "a").await;
        let bucket = (a.route % 2) as i16;
        client
            .execute(
                "update segments set state = 'draining', bucket_count = 2 where seg_seq = $1",
                &[&seg],
            )
            .await
            .expect("split the sealed segment in two buckets");
        assert_eq!(pending(&client, &["a", "late"], None).await, [true, true]);

        // The other bucket drains: still pending.
        client
            .execute(
                "update segments set drained_mask = 1::bigint << (1 - $2::int) \
                 where seg_seq = $1",
                &[&seg, &(bucket as i32)],
            )
            .await
            .expect("drain the other bucket");
        assert_eq!(pending(&client, &["a"], None).await, [true]);

        // A cursor short of the key: pending. At the key: applied.
        client
            .execute(
                "insert into drain_cursor \
                     (seg_seq, bucket, after_route, after_src_table, after_key) \
                 values ($1, $2, $3, $4, '')",
                &[&seg, &bucket, &a.route, &a.src_table],
            )
            .await
            .expect("write a cursor short of the key");
        assert_eq!(pending(&client, &["a"], None).await, [true]);
        client
            .execute(
                "update drain_cursor set after_key = $3 where seg_seq = $1 and bucket = $2",
                &[&seg, &bucket, &a.key],
            )
            .await
            .expect("move the cursor to the key");
        assert_eq!(pending(&client, &["a"], None).await, [false]);
        client
            .execute("delete from drain_cursor where seg_seq = $1", &[&seg])
            .await
            .expect("drop the cursor");

        // A page this transaction applies counts it applied when it covers
        // the key, and not when it stops short of it or names another
        // bucket.
        let through = |next: Option<fold::PageKey>, buckets: Vec<i16>| SegmentStep {
            seg_seq: seg,
            page: Some(PageClaim {
                held: buckets.clone(),
                buckets,
                next,
            }),
        };
        let short = fold::PageKey {
            key: String::new(),
            ..a.clone()
        };
        for (step, expected) in [
            (through(Some(a.clone()), vec![bucket]), false),
            (through(None, vec![bucket]), false),
            (through(Some(short), vec![bucket]), true),
            (through(None, vec![1 - bucket]), true),
        ] {
            let steps = [step];
            let claim = ClaimScope {
                steps: &steps,
                claimed_by: "me",
            };
            assert_eq!(
                pending(&client, &["a"], Some(&claim)).await,
                [expected],
                "{:?}",
                steps[0].page
            );
        }
        // The pre-paging contract: every bucket `claimed_by` holds.
        client
            .execute(
                "insert into seg_claims (seg_seq, bucket, claimed_by) values ($1, $2, 'me')",
                &[&seg, &bucket],
            )
            .await
            .expect("claim the key's bucket");
        for (claimed_by, expected) in [("me", false), ("someone else", true)] {
            let steps = [SegmentStep {
                seg_seq: seg,
                page: None,
            }];
            let claim = ClaimScope {
                steps: &steps,
                claimed_by,
            };
            assert_eq!(
                pending(&client, &["a"], Some(&claim)).await,
                [expected],
                "{claimed_by}"
            );
        }

        // The key's bucket drains: applied. The late row is still pending,
        // in the next segment, which isn't sealed.
        client
            .execute(
                "update segments set drained_mask = 3 where seg_seq = $1",
                &[&seg],
            )
            .await
            .expect("drain the key's bucket");
        assert_eq!(pending(&client, &["a", "late"], None).await, [false, true]);

        // The next segment seals with the late row in its window, and drains.
        let next = seal::seal_phase1(&mut client).await.expect("seal phase 1");
        seal::seal_phase2(&client, next.sealed_seg_seq, WAKE)
            .await
            .expect("seal phase 2");
        assert_eq!(pending(&client, &["late"], None).await, [true]);
        client
            .execute(
                "update segments set state = 'drained', drained_mask = 1 where seg_seq = $1",
                &[&next.sealed_seg_seq],
            )
            .await
            .expect("drain the next segment");
        assert_eq!(pending(&client, &["late"], None).await, [false]);
    }
}
