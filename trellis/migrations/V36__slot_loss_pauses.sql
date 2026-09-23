-- Issue #310: which transforms Trellis paused on its own because the
-- replication slot feeding them was lost (missing, or invalidated with
-- `wal_status = 'lost'`), so the staging worker can keep reminding the
-- operator about them until each is resumed.
--
-- The pause itself is the ordinary `transform_definitions.status = 'paused'`
-- (ADR-0014); this table only records the reason, which the status column
-- has no room for. A row whose transform is no longer frozen is stale and is
-- pruned by `intake::slot_loss::slot_loss_paused_transforms`; `RESUME
-- TRANSFORM` also deletes it directly. `on delete cascade` takes the row with
-- a dropped definition.
create table if not exists slot_loss_pauses (
    transform_id bigint primary key
        references transform_definitions (id) on delete cascade,
    slot_name text not null,
    lost_confirmed_lsn pg_lsn not null,
    paused_at timestamptz not null default now()
);
