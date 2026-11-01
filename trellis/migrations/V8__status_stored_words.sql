-- `catching_up` is a reported status (a `live` definition whose upstream is
-- behind), never a stored one: the stored words are the five below
-- (`StoredStatus`). The CHECK and the two definition triggers stop naming it.
alter table transform_definitions drop constraint transform_definitions_status_check;
alter table transform_definitions add constraint transform_definitions_status_check
    check (status in ('waiting_to_backfill', 'backfilling', 'live', 'quarantined', 'paused'));

drop trigger announce_definition_insert on transform_definitions;
create trigger announce_definition_insert
    after insert on transform_definitions
    for each row
    when (new.status in ('waiting_to_backfill', 'backfilling', 'live'))
    execute function announce_definition_work();

drop trigger announce_definition_update on transform_definitions;
create trigger announce_definition_update
    after update on transform_definitions
    for each row
    when (new.status in ('waiting_to_backfill', 'backfilling', 'live')
          and (old.status is distinct from new.status or old.build is distinct from new.build))
    execute function announce_definition_work();
