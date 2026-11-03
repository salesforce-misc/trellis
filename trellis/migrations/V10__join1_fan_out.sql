-- A 1-1 JOIN's pending fan-outs (ADR-0021): one row per join key whose `b`
-- change has target rows of `a` left to recompute. The `b` page writes it in
-- its own transaction, for the key it stops in and every key after it, and
-- each chunk of the key recomputes the next `R` entries after `cursor` and
-- advances it, or deletes the row, in one transaction under the key's `J`
-- lock. So a chunk that dies leaves the row as its last commit did, and the
-- next one carries on from there.
--
-- `join_key` is the key's columns as text, in `ON` order, as the definition's
-- `R` and `J` render them. `cursor` is the `R` entry key (`a`'s primary key
-- text, under "C") of the last row recomputed; `NULL` is the key's start, which
-- a later `b` change on the key sets again. `gen` counts the pages that wrote
-- the row. `origin_lsn`, `src_changed` and `hop_gen` are the earliest origin
-- (and source-change time, and deepest hop) of the `b` changes the row's
-- remaining rows don't reflect yet, merged across pages: convergence waits on
-- them as on a parked row, and a row a chunk holds in quarantine parks with
-- them. `seg_seq` is the segment of the last page that wrote the row, which a
-- held row records.
create table join1_fan_out (
    transform_id bigint not null references transform_definitions (id) on delete cascade,
    join_key text[] not null,
    gen bigint not null default 1,
    cursor text collate "C",
    seg_seq bigint not null,
    origin_lsn pg_lsn,
    src_changed timestamptz,
    hop_gen integer not null default 0,
    primary key (transform_id, join_key)
);

-- Convergence asks whether any row's origin is at or below a token.
create index on join1_fan_out (origin_lsn);

-- A new pending fan-out is work for an idle drain worker.
create trigger announce_join1_fan_out_insert
    after insert on join1_fan_out
    for each row
    execute function announce_chunk_work();
