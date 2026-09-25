-- Issue #476: `catching_up`, a status apply maintains exactly as it does
-- `live`, for a definition whose go-live catch-up hasn't been discharged
-- yet. A finished chunked or direct build lands here instead of `live`, and
-- so does a `live` definition that gets a catch-up of its own (a column
-- resume, an `ALTER TRANSFORM` that added columns). The discharge that runs
-- the catch-up flips it to `live`, so `live` means the target is in its
-- steady state (ADR-0016, "What `live` promises").
--
-- Same drop-and-re-add idiom as V32__transform_paused_status.sql.
alter table transform_definitions
    drop constraint transform_definitions_status_check;

alter table transform_definitions
    add constraint transform_definitions_status_check
        check (status in (
            'waiting_to_backfill', 'backfilling', 'catching_up', 'live', 'quarantined', 'paused'
        ));
