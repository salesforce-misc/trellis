-- #622 C8: trigger capture replaced the logical-replication slot and its
-- publication (C5), and C8 deletes the intake that read them. Two tables
-- only intake used go with it:
--
-- - `replication_progress` (V4): the slot's last confirmed LSN, which intake
--   advanced with every staged transaction.
-- - `slot_loss_pauses` (V36): which transforms a lost slot paused, for their
--   status. A trigger can't be lost the way a slot can.
--
-- A database used before C5 may still hold a `trellis_slot` replication slot
-- and a `trellis_pub` publication. Nothing reads the slot, so it pins WAL
-- until the disk fills. Trellis never drops a slot on its own; drop them by
-- hand: `select pg_drop_replication_slot('trellis_slot')` and
-- `drop publication trellis_pub`.

drop table slot_loss_pauses;
drop table replication_progress;
