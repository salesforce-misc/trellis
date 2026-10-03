-- #733: the segment that was active when a definition's latest Re-derive
-- build started (`staging::build`'s start), or null for a definition no
-- Re-derive build has started.
--
-- Every change committed before the start is in a batch at or below it, so
-- a page that drains such a batch for the definition re-derives the batch's
-- keys instead of applying their changes (`staging::ledger`): a later change
-- to the same key may have drained before the start and never reached the
-- definition, so the older change's image can be stale.
alter table transform_definitions add column build_seg bigint;
