-- Issue #407 (ADR-0016): a `pending_backfill` marker whose discharge fails
-- carries its retry state, so one broken marker neither starves the markers
-- behind it nor gets retried on every maintenance pass.
--
-- `attempts` counts the discharges of this park that failed, `last_error` is
-- the latest one's error text (read back through `Trellis::status`), and
-- `next_attempt_at` is when the discharge next tries it, backed off
-- exponentially up to a cap. A marker with no `next_attempt_at` is due now.
--
-- Discharge records a failure against the generation it read, and every new
-- park of the table (`park_marker`) resets all three, so a fresh park is
-- retried at once.
alter table pending_backfill
    add column if not exists attempts integer not null default 0,
    add column if not exists last_error text,
    add column if not exists next_attempt_at timestamptz;
