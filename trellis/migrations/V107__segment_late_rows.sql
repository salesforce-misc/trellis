-- #1219: the rows of a segment's ring slot that its own fence does not show,
-- the late writers' rows (docs/staging-and-claiming/03-sealing-and-the-fence.md):
-- the segment's successor takes them. Counted once, by the retire pass, after
-- every transaction in the fence has finished (so no more can land), and only
-- when a reader needs it (the fence had a transaction in flight and the
-- successor hasn't drained); NULL otherwise. `self_check` skips a drained slot
-- whose count is 0.
alter table segments add column late_rows bigint;
