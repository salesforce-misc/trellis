-- `capture_failure_reasons.recheck` takes two more words, each a reason whose
-- cause is gone once an index is back (`staging::pause_message`): `backing`,
-- no unique index backs the table's declared `KEY (columns)`; and `index`, no
-- valid index leads with `columns`, which a `MIN`/`MAX` probe reads.
--
-- Declares no catalog floor: an older release reads a word it doesn't know
-- as a reason that stands until a resume (`Recheck::from_stored`), which is
-- what a pause before this release was, and writes only the words the check
-- admitted before.
alter table capture_failure_reasons
    drop constraint if exists capture_failure_reasons_recheck_check,
    add constraint capture_failure_reasons_recheck_check
        check (recheck in ('columns', 'key', 'backing', 'index'));
