-- `partition_corrections.cause` takes only the words a release writes, as
-- `kind` already does: what changed about a node (`capture::partitions`'s
-- `Cause`), and `complement` for the keys outside a node's tree, whose
-- correction is queued with its kind as its cause. A word an older release
-- doesn't know can then only be written by a migration that relaxes this
-- check, and that migration raises the catalog floor (ADR-0024 rule 6).
--
-- Declares no catalog floor: every release writes only these words (0.1.0
-- included), so the check holds on every existing row, and an older release
-- at or above the floor writes nothing it refuses.
alter table partition_corrections
    add constraint partition_corrections_cause_check check (cause in (
        'created', 'reattached', 'default_siblings', 'renamed', 'gone', 'truncated',
        'complement'));
