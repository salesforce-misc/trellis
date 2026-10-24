-- One `poison_held` row per held key (issue #803).
--
-- `poison_held` was keyed `(transform_id, src_table, key, seg_seq)`, one row
-- with full images for every segment that changed a held key, so a key held
-- for good gained rows without limit. `staging::quarantine::release_key`
-- never replays them: it folds them into one `Recompute`. So the table now
-- keeps that fold, one row per `(transform_id, src_table, key)`, and every
-- park merges into it (`staging::quarantine::HeldKey`, whose `merge` is the
-- Rust twin of the park's `on conflict do update`):
--
--   * `seg_seq`, `old_image`: the earliest parked change's segment and its
--     pre-image (a recompute's prior-image hint), the state readers last
--     saw, which the release hands on as its prior image;
--   * `origin_lsn`: the earliest, and null (unknown) once any parked change's
--     is, which `converge`'s condition 4 reads as older than any token;
--   * `src_changed`: the earliest known; a key held for any source change
--     has one, so it also says whether the release is a source change;
--   * `hop_gen`: the deepest.
--
-- `op`, `new_image` and `held_seq` are gone: nothing reads `op`, and the
-- release no longer needs an order among one key's rows.
--
-- Trellis is unreleased, so the table is recreated empty rather than folded.

drop table poison_held;
create table poison_held (
    transform_id bigint not null references transform_definitions (id) on delete cascade,
    src_table text not null,
    key text not null,
    seg_seq bigint not null,
    old_image jsonb,
    origin_lsn pg_lsn,
    src_changed timestamptz,
    hop_gen integer not null default 0,
    primary key (transform_id, src_table, key)
);
