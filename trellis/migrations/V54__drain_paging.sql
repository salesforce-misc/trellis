-- Issue #620 (epic #556, milestone A2a): bounded drain paging. See
-- docs/staging-and-claiming/04-claiming-and-the-fold.md, "Paging a share
-- larger than the cap", and 05-apply-and-exactly-once-deltas.md, "A page is
-- its own transaction".

-- The ring-row count the seal already takes to size `bucket_count`, kept
-- rather than thrown away: a drain estimates its share from it (rows over
-- buckets, times the buckets it holds) to pick the direct fold when the share
-- fits `ClientOptions::drain_batch_cap` and paging when it doesn't, and
-- `staging::apply::next_claimable_segments` coalesces segments only while
-- their summed counts fit. An estimate, not a bound: the fenced window can
-- hold a predecessor's late rows this count never saw, which is why the direct
-- fold also carries a `limit cap + 1` guard.
alter table segments add column row_count bigint not null default 0;

-- Where each bucket's paging stands: the last `(route, src_table, key)` page
-- key a committed page covered, in the keyset order
-- `staging::fold::PageKey` defines (`route` is `-1` for a truncate
-- sentinel, so the clear is always page 1). Written in each non-final page's
-- own transaction, behind the claim check (`update seg_claims ... returning
-- bucket` must return every bucket the worker holds, or the page rolls back
-- as `ClaimLost`), so a committed row here always means "every key up to
-- this one is applied, exactly once".
--
-- A separate table, not a `seg_claims` column: reclaim deletes the claim row
-- (that delete is the claim epoch), and a cursor that died with it would make
-- the next claimant re-apply every committed page. Not a `segments` column
-- either: every page commit would then lock the segment's hot row.
--
-- A bucket with a cursor and no claim is free under the unchanged
-- `CLAIM_SQL`; its next claimant resumes after the cursor. `on delete
-- cascade`: retirement deletes a drained segment's row and this goes with it.
create table drain_cursor (
    seg_seq bigint not null references segments (seg_seq) on delete cascade,
    bucket smallint not null,
    after_route bigint not null,
    after_src_table text not null,
    after_key text not null,
    primary key (seg_seq, bucket)
);
