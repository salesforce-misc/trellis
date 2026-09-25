-- Issue #420: nothing reads `pending_backfill.added_at`. The discharge reads
-- markers in park order (`generation`), and `Trellis::status` reports a
-- marker's retry state, not when it was parked.
alter table pending_backfill drop column added_at;
