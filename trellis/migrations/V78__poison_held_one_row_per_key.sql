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
--   * `hop_gen`: the deepest;
--   * `group_key`: the sorted, deduplicated union;
--   * `join_values`: one `{to_col: value}` object per value the parked
--     changes held in some relationship's `to_col`, deduplicated: every raw
--     row's new image (`FoldedChange::to_col_values`, #785) and every
--     park's pre-image (batches drain out of order, so a later park's
--     pre-image can name a value no parked new image did), which the
--     release's to-one projection rewrite (#754) reads;
--   * `lsn`: the greatest, the release's `parked_through` (#754).
--
-- `op`, `new_image` and `held_seq` are gone: nothing reads `op`, the release
-- no longer needs an order among one key's rows, and `join_values` names
-- every to-side key the parked images did.
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
    group_key text[],
    join_values jsonb[] not null default '{}',
    lsn pg_lsn,
    primary key (transform_id, src_table, key)
);
