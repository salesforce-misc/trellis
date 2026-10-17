-- #1099 (G1 of #1091): every transition row counts once.
--
-- A capture that re-reads the live row (`capture::sql`, "The image is the
-- live row") stages the live row as `new_image`, which a nested write to the
-- same key may have changed after the statement's own change, and stages a
-- vanished insert as a `delete` whose `old_image` is its new row. A reader
-- that sums `f(new) − f(old)` per ring row (the group basis, #1043 R3) then
-- counts a change twice. `transition_images` holds the statement's own
-- transition rows for such a ring row, as `{"old": <image or null>, "new":
-- <image or null>}`, so the ring rows of a statement sum to
-- `Σ f(NEW TABLE) − Σ f(OLD TABLE)`.
--
-- It is set only by a capture that re-reads, and only on a row whose
-- `old_image`/`new_image` aren't its transition rows. NULL means they are.
-- Every other ring writer leaves it NULL, and 1-1 readers ignore it.
--
-- This migration `ALTER`s every `seg_N`, which takes `ACCESS EXCLUSIVE` and
-- so stalls every captured application writer while it runs. Adding a
-- nullable column without a default rewrites nothing.

alter table seg_0 add column transition_images jsonb;
alter table seg_1 add column transition_images jsonb;
alter table seg_2 add column transition_images jsonb;
alter table seg_3 add column transition_images jsonb;
