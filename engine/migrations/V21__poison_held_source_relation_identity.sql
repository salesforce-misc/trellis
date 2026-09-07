-- ADR-0007: `(src_table, key, seg_seq)` cannot distinguish a dropped and
-- recreated source relation that reused its presentation name. Preserve every
-- existing row behind a surrogate primary key, then enforce identity-specific
-- uniqueness for OID-bearing rows while retaining the legacy null-OID shape.
alter table poison_held
    add column if not exists id bigint generated always as identity;

alter table poison_held
    drop constraint if exists poison_held_pkey;

alter table poison_held
    add constraint poison_held_pkey primary key (id);

create unique index if not exists poison_held_source_relation_oid_key_seg_seq_key
    on poison_held (source_relation_oid, key, seg_seq)
    where source_relation_oid is not null;

create unique index if not exists poison_held_legacy_src_table_key_seg_seq_key
    on poison_held (src_table, key, seg_seq)
    where source_relation_oid is null;
