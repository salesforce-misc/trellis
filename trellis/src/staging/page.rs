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

use crate::pool::Pool;

use super::apply::ApplyError;
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
