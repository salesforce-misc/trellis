//! Bounded drain paging (issue #620, epic #556 milestone A2a). See
//! docs/staging-and-claiming/04-claiming-and-the-fold.md, "Paging a share
//! larger than the cap".
//!
//! A drain holds at most `ClientOptions::drain_batch_cap` folded records at
//! once. A share that fits is folded directly, as before; a share that
//! doesn't is walked in pages, keyset-ordered on [`PageKey`], each page its
//! own compute-and-apply transaction that advances the bucket's
//! `drain_cursor` row. This module is where pages come from ([`PageSource`])
//! and where the cursor is read back ([`read_cursors`]); the page's apply,
//! claim check and cursor write live in `super::apply`.

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

/// Where an oversized share's pages come from. A page never splits a key and
/// never holds more than `cap` records; successive calls, each starting after
/// the previous page's [`Page::next`], cover the share exactly once.
///
/// A2a's source is [`RescanPages`]: each page re-scans the fenced window, which
/// is quadratic in the share's size. A2b replaces it with a once-per-claim
/// materialize into a session `TEMP` table, read back by keyset, behind this
/// same interface. The caller resumes from a `drain_cursor` row by passing it
/// as `after`, so a source must not assume it started at the beginning.
pub(crate) trait PageSource {
    async fn next_page(&mut self, after: Option<&PageKey>, cap: usize) -> Result<Page, ApplyError>;
}

/// The rescan pager: each page is two statements over the fenced window, in
/// one read transaction. [`fold::page_boundary`] finds the page's last key
/// (a top-K over the distinct keys after `after`), then [`fold::fold_page`]
/// folds that key range.
pub(crate) struct RescanPages<'a> {
    pool: &'a Pool,
    seg_seq: i64,
    filter: BucketFilter,
}

impl<'a> RescanPages<'a> {
    pub(crate) fn new(pool: &'a Pool, seg_seq: i64, filter: BucketFilter) -> Self {
        Self {
            pool,
            seg_seq,
            filter,
        }
    }
}

impl PageSource for RescanPages<'_> {
    async fn next_page(&mut self, after: Option<&PageKey>, cap: usize) -> Result<Page, ApplyError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let next = fold::page_boundary(&txn, self.seg_seq, &self.filter, after, cap).await?;
        let records =
            fold::fold_page(&txn, self.seg_seq, &self.filter, after, next.as_ref()).await?;
        txn.commit().await?;
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
