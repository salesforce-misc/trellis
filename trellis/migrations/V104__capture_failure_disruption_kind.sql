-- #1181: a third `capture_failures.kind`, `disruption`, for a pause that
-- Trellis's capture was changed outside Trellis (#707, #999): a trigger or
-- capture function altered, disabled or recreated, or a read table gone.
-- Those are the pauses a fixture load or bulk loader leaves (`ENABLE
-- TRIGGER ALL` puts the capture triggers in ORIGIN mode), and a test
-- helper resumes exactly them, so they need a stable discriminator that
-- the sentence in `error` isn't.
alter table capture_failures drop constraint capture_failures_kind_check;
alter table capture_failures add constraint capture_failures_kind_check
    check (kind in ('capture', 'halt', 'disruption'));
