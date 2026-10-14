-- A definition becoming dispatchable announces itself (#1047).
--
-- The build probe (`staging::build::work_probe`) now counts a chunk only
-- while its definition is in a status the chunk claim hands work in
-- (`TransformStatus::dispatchable()`), so a paused or quarantined definition's
-- undone chunks no longer keep every idle worker awake. The other side of
-- that gate is the wake: a definition moving into a dispatchable status (a
-- resume, an unfreeze) makes its existing chunks claimable without touching
-- a chunk row, so V86's definition trigger, which announced only
-- `backfilling` and `catching_up`, must announce `waiting_to_backfill` and
-- `live` too.
drop trigger announce_definition_insert on transform_definitions;
drop trigger announce_definition_update on transform_definitions;

create trigger announce_definition_insert
    after insert on transform_definitions
    for each row
    when (new.status in ('waiting_to_backfill', 'backfilling', 'catching_up', 'live'))
    execute function announce_definition_work();

create trigger announce_definition_update
    after update on transform_definitions
    for each row
    when (new.status in ('waiting_to_backfill', 'backfilling', 'catching_up', 'live')
          and (old.status is distinct from new.status or old.build is distinct from new.build))
    execute function announce_definition_work();
