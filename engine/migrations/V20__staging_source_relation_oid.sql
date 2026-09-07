-- ADR-0007: preserve the physical source relation identity through the
-- staging ring. This remains nullable because sealed/live rows written before
-- this migration have only source presentation text and cannot be safely
-- resolved after a rename or replacement.
alter table seg_0 add column if not exists source_relation_oid oid;
alter table seg_1 add column if not exists source_relation_oid oid;
alter table seg_2 add column if not exists source_relation_oid oid;
alter table seg_3 add column if not exists source_relation_oid oid;

-- Parked folded rows must retain the same identity while quarantine support
-- is migrated separately.
alter table poison_held add column if not exists source_relation_oid oid;

-- Do not replace the generated route in this additive migration. Existing
-- rows retain a route derived from source presentation text; changing the
-- expression would require dropping/rebuilding the generated column and
-- would make a sealed batch's persisted bucket assignment inconsistent.
