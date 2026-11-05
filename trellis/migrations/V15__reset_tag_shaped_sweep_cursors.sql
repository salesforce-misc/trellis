-- Restarts every unfinished 1-1 rebuild sweep whose cursor starts with
-- U+0001 (`staging::build::one_to_one::SweepCursor`).
--
-- A release before the tagged sweep cursors stored the sweep's last ledger
-- key bare, whatever its text. From this release on, a cursor that starts
-- with U+0001 is tagged (`\x01ledger\x01<key>`, `\x01orphans\x01<key>`,
-- `\x01orphan-nulls\x01...`), so a bare key an older release stored with the
-- tagged shape would read as a tag: a known one resumes the sweep from the
-- wrong place, past keys it never re-derived; an unknown one pauses the
-- definition for good. A `NULL` cursor is the start of the sweep, which
-- every release reads the same way, and a sweep re-derives idempotently, so
-- each such sweep walks again from the start. Tagged cursors a development
-- build of this release already wrote are reset too, which only repeats
-- their walk.
--
-- Only a definition with no `JOIN` has a 1-1 sweep; a 1-1 `JOIN`'s sweep
-- and plan jobs always tagged their cursors, so theirs are left alone.
-- `ascii` reads the first character without a collation or a pattern.
--
-- Declares no catalog floor: it writes only `NULL` cursors, which every
-- release reads as the start of the sweep.
update backfill_chunks bc
   set lo = null
  from transform_definitions d
 where d.id = bc.definition_id
   and d.join_table is null
   and bc.kind = 'sweep'
   and not bc.done
   and ascii(bc.lo) = 1;
