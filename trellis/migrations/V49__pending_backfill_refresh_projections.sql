-- Issue #507: a `pending_backfill` marker parked because a definition's
-- target was rewritten outside the target-mutation seam (a resumed
-- definition's rebuild going live) also has its discharge refresh every
-- to-one relationship projection on that target from the target's rows
-- (`catalog::refresh_relationship_projections_in_txn`). The refresh diffs
-- the whole target against each projection, so only those markers ask for
-- it: every other write to a target reaches its projections through the
-- seam. A re-park of the table keeps the flag, and the marker's delete
-- clears it.
alter table pending_backfill
    add column refresh_projections boolean not null default false;
