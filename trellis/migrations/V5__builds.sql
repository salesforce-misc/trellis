-- Build work: the chunks a build is claimed and run in.
--
-- `kind` is the chunk's job: `plan` (enumerates a build), `rederive` (a
-- primary-key range of the source to re-derive; `lo` and `hi` bound it),
-- `sweep` (the post-build sweep), `correction` (a partitioned source's
-- correction, named by `correction_id`, which carries no range).
-- `fields` names the fields of an `ALTER TRANSFORM` field build (null: all).
-- `key_collations` are the key columns' collations the range was paged
-- under. `start_xid` and `fence_xid` are the snapshot and the fence the build
-- reads against. `attempts` and `charged` count the tries and the ones
-- charged against the retry budget, and `next_attempt_at` is the backoff
-- deadline. `fuse_rearmed_at` is the definition's fuse re-arm the chunk ran
-- under.
create table backfill_chunks (
    id bigserial primary key,
    definition_id bigint not null references transform_definitions (id) on delete cascade,
    lo text,
    hi text,
    done boolean not null default false,
    claimed_by text,
    claimed_at timestamptz,
    created_at timestamptz not null default now(),
    fuse_rearmed_at timestamptz,
    attempts integer not null default 0,
    charged integer not null default 0,
    last_error text,
    next_attempt_at timestamptz,
    kind text not null,
    start_xid xid8,
    fields text[],
    key_collations text[],
    fence_xid xid8,
    correction_id bigint references partition_corrections (id) on delete cascade,
    constraint backfill_chunks_kind_check
        check (kind in ('plan', 'rederive', 'sweep', 'correction')),
    constraint backfill_chunks_kind_bounds check (
        case kind
            when 'rederive' then hi is not null
            when 'correction' then hi is null and lo is null and correction_id is not null
            else hi is null
        end),
    constraint backfill_chunks_fields_kind check (
        fields is null or kind in ('plan', 'rederive')),
    constraint backfill_chunks_correction_kind check (
        (kind = 'correction') = (correction_id is not null))
);

create index backfill_chunks_definition_idx on backfill_chunks (definition_id);

create index backfill_chunks_claimable_idx on backfill_chunks (id)
    where not done and claimed_by is null;

create index backfill_chunks_correction on backfill_chunks (correction_id)
    where correction_id is not null;
