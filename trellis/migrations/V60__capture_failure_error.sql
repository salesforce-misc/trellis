-- #687: a capture failure records its own sentence.
--
-- A definition can now be paused for two schema changes (#622 C6): a
-- column it reads was renamed or dropped, or its source's primary key was
-- redefined without a rename. The pause records the sentence
-- `Trellis::status` reports as `capture_failure.error`, written where the
-- cause is known, rather than `status` rebuilding it from `columns`.
alter table capture_failures add column error text not null default '';
alter table capture_failures alter column error drop default;
