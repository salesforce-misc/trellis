-- A stored generated column's expression in each capture baseline row
-- (`capture::baseline`): whether the column is a stored generated column,
-- and a hash of its `pg_attrdef` expression with the `:location` fields left
-- out. The capture pass rebuilds a column's readers when a stored generated
-- column's expression changed as its table was rewritten
-- (`ALTER COLUMN ... SET EXPRESSION`).
--
-- Declares no catalog floor (ADR-0024): both columns are nullable with no
-- default, and the pass reads a `NULL` as a row an older release wrote: it
-- writes the row back without acting, as it does a newly recorded one. An
-- older release at or above the floor inserts rows without them, and updates
-- the rest of a row in place, leaving the pair as a newer release last wrote
-- it. The pass acts on the pair only together with a file node that moved
-- since the row was written, so a pair left stale by an expression change
-- the older release missed rebuilds the readers at the next rewrite of the
-- table instead of never.
alter table capture_baselines
    add column generated_stored boolean,
    add column expr_hash text;
