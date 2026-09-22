-- Issue #283: fold pre-existing *bare* quarantine rows into their qualified
-- counterpart, so every counter/marker table keys on one canonical identity
-- per logical source table.
--
-- `staging/quarantine.rs` stored and matched whatever `src_table` spelling the
-- ring row it was diagnosing happened to carry. A single logical source staged
-- under two spellings — bare `orders` and qualified `public.orders`, a real and
-- durable duality (issue #267 canonicalized *emission* going forward, but ring,
-- `poison`, `poison_held` and `key_deaths` rows staged before it are durable,
-- and this crate's own integration fixtures still stage bare by hand) —
-- therefore accumulated two entirely independent sets of quarantine state that
-- never combined:
--
--   * `trip_transform_fuse_if_crossed`'s `count(*) from poison` (both the
--     unwindowed guard and issue #160's per-definition windowed count) saw one
--     spelling only, so two half-threshold budgets never tripped the
--     whole-transform fuse even though the same source had killed twice the
--     threshold's worth of rows.
--   * `transform_fuse_gate` — issue #159's serialization row lock — had one row
--     per *spelling*, so two workers evicting the same logical source under
--     different spellings queued on different locks and serialized against
--     nothing, which is the very race that table exists to close.
--   * `key_deaths` maintained two row-level death counters for one physical
--     row, so the row-level fuse took up to twice as many real failures to fire.
--   * `poison`'s own fold-exclusion read (`poisoned_keys_among`) missed a key
--     already evicted under the other spelling and re-evaluated it.
--   * `column_failures`' primary key `(transform_table, column_name, src_table,
--     key)` split its idempotency dedup — the one case that *over*-counted: one
--     stubborn row could charge `column_deaths` twice.
--
-- The code half of the fix resolves `src_table` once per source table (through
-- `quarantine::qualified_src_table`, issue #281's resolution) and writes *and*
-- reads every one of these tables under that canonical identity. Writes and
-- reads had to move together — qualifying only the counts while writes stayed
-- raw would make a bare-poisoned source count zero and never trip at all, which
-- is exactly why #281 deliberately left these tables alone. This migration is
-- the third part: rows already on disk under a bare spelling are invisible to
-- the now-canonical reads, so they are folded into the qualified row and
-- deleted, rather than stranded.
--
-- **Resolution is from Trellis's own catalog, never from `search_path`.**
-- `V24__schema_nodes_qualified_identity.sql` set this epic's precedent and its
-- reasoning holds here: `search_path`-aware, per-row physical resolution is Rust
-- logic against a live connection, and a one-shot SQL migration attempting it
-- would be guessing at the right `search_path` for a row that records no such
-- context. So this fold consults only the qualified identities Trellis itself
-- has already recorded — `schema_nodes.table_name` (qualified since V24),
-- `transform_definitions.source_table` (#72) and `.target_table` (#73) — and
-- folds a bare row only when **exactly one** of those resolves its bare suffix.
-- That is sufficient rather than merely convenient: a quarantine row for a
-- source no definition was ever evaluating cannot exist in the first place. It
-- is also strictly safe in the two cases it declines:
--
--   * Two schemas share a bare suffix (`public.orders` and `archive.orders`):
--     ambiguous, so the bare row is left exactly as it is. Nothing is merged
--     into the wrong physical table — the outcome #74/ADR-0007 exists to
--     prevent — and nothing is deleted. Note this one case does *not* line up
--     with the code side: `qualified_src_table` resolves an ambiguous bare name
--     anyway, via `catalog::resolve_source_schema`'s `search_path` walk, so new
--     writes land on the `search_path` winner while the un-folded bare rows stay
--     where they are. That residual split is accepted: folding into a guess at
--     the `search_path` a row never recorded is the one outcome worse than
--     leaving it, and an ambiguous bare suffix means the code's own answer is
--     already session-dependent.

--   * Nothing resolves it at all (a since-dropped source, or the U+001F
--     `RelationshipReverseDeferred` sentinel, which names no table at all):
--     left alone too, which matches the code side exactly — those are the same
--     spellings `qualified_src_table` deliberately hands back unchanged, so raw
--     *is* their canonical key and the two halves stay consistent.
--
-- Unlike V24 this migration deletes nothing it has not first folded, so it does
-- not lean on "there is no production deployment yet" for its safety the way
-- that truncate did.
--
-- `column_deaths` is deliberately **not** adjusted. It is keyed
-- `(transform_table, column_name)` with no `src_table` at all, so its budget
-- always did aggregate correctly across spellings; the split was in
-- `column_failures`' dedup in front of it. Any historical double charge is not
-- reconstructible from the surviving rows (the two `column_failures` rows record
-- that the pair was charged, not how many times), and a *guessed* decrement
-- could silently un-pause a genuinely fused column. Folding the dedup key stops
-- the over-count from here on; `resume_column` remains the operator's way to
-- clear a counter that already over-counted.

-- One resolution per bare suffix, or no row at all where it is ambiguous.
create temporary table trellis_v33_canonical_src_table as
with qualified_identities as (
    select table_name as qualified from schema_nodes
    where table_name like '%.%'
    union
    select source_table from transform_definitions
    where source_table like '%.%'
    union
    select target_table from transform_definitions
    where target_table like '%.%'
)
select split_part(qualified, '.', 2) as bare, min(qualified) as qualified
from qualified_identities
where split_part(qualified, '.', 2) <> ''
group by split_part(qualified, '.', 2)
having count(*) = 1;

create unique index trellis_v33_canonical_src_table_bare
    on trellis_v33_canonical_src_table (bare);

-- `poison`: a marker, so the qualified row wins outright where both spellings
-- already hold the same key (`do nothing`) — a key is either evicted or not, and
-- `poisoned_at`/`last_error` are diagnostic detail, not a quantity to combine.
-- Where only the bare row exists it becomes the qualified row.
insert into poison (src_table, key, poisoned_at, last_error)
select c.qualified, p.key, p.poisoned_at, p.last_error
from poison p
join trellis_v33_canonical_src_table c on p.src_table = c.bare
on conflict (src_table, key) do nothing;

delete from poison p
using trellis_v33_canonical_src_table c
where p.src_table = c.bare;

-- `poison_held`: likewise a marker, keyed `(src_table, key, seg_seq)` and
-- already idempotent on that triple by `park_batch_contribution`'s own
-- `on conflict do nothing`, so this fold uses the same rule. `held_seq` is
-- deliberately not carried across: it is a `bigserial` tie-breaker *within* one
-- `(src_table, key, seg_seq)` (which holds at most one row), and re-issuing it
-- from the sequence keeps `release_key`'s `seg_seq asc, held_seq asc` replay
-- order — batch order first — intact.
--
-- **The one place `do nothing` can drop real work, called out rather than
-- hidden:** unlike the other markers, a `poison_held` row carries a *payload*
-- (`old_image`/`new_image`/`op`), so where both spellings parked a row for the
-- same `(key, seg_seq)` the bare row's payload is discarded and the qualified
-- one survives. That needs one batch to have folded two separate contributions
-- for one logical key under two spellings — precisely the fold-coalescing gap
-- issue #267 closed, so it can only come from a pre-#267 ring — and the code
-- side lands in the same place from here on (`park_batch_contribution`'s own
-- `on conflict do nothing` now sees both spellings collapse onto one canonical
-- key). Retaining both is not expressible under this table's primary key, and
-- re-keying a parked row onto a `seg_seq` it never came from would corrupt
-- `release_key`'s replay order, which is load-bearing for aggregate deltas.
insert into poison_held
    (src_table, key, seg_seq, op, lsn, old_image, new_image, origin_lsn,
     src_changed, hop_gen, group_key)
select c.qualified, h.key, h.seg_seq, h.op, h.lsn, h.old_image, h.new_image,
       h.origin_lsn, h.src_changed, h.hop_gen, h.group_key
from poison_held h
join trellis_v33_canonical_src_table c on h.src_table = c.bare
on conflict (src_table, key, seg_seq) do nothing;

delete from poison_held h
using trellis_v33_canonical_src_table c
where h.src_table = c.bare;

-- `key_deaths`: a *counter*, so the two rows' `deaths` are summed — that sum is
-- the honest answer to "how many real, observed attempts have failed for this
-- physical row", which is what the row-level fuse spends. The surviving
-- `last_error` is whichever row died more recently (ties resolve to the folded
-- bare row, arbitrary and harmless: it is diagnostic text either way).
insert into key_deaths (src_table, key, deaths, last_error, last_death_at)
select c.qualified, k.key, k.deaths, k.last_error, k.last_death_at
from key_deaths k
join trellis_v33_canonical_src_table c on k.src_table = c.bare
on conflict (src_table, key) do update set
    deaths = key_deaths.deaths + excluded.deaths,
    last_error = case
        when excluded.last_death_at is null then key_deaths.last_error
        when key_deaths.last_death_at is null then excluded.last_error
        when excluded.last_death_at >= key_deaths.last_death_at
            then excluded.last_error
        else key_deaths.last_error
    end,
    last_death_at = greatest(key_deaths.last_death_at, excluded.last_death_at);

delete from key_deaths k
using trellis_v33_canonical_src_table c
where k.src_table = c.bare;

-- `column_failures`: a marker (the dedup key in front of `column_deaths`), so
-- `do nothing` again — the qualified row already records that this
-- `(transform, column, row)` was charged, which is the entire fact this table
-- carries. Folding the duplicate is what stops the same physical row being
-- charged a second time from here on.
insert into column_failures
    (transform_table, column_name, src_table, key, error, failed_at)
select f.transform_table, f.column_name, c.qualified, f.key, f.error, f.failed_at
from column_failures f
join trellis_v33_canonical_src_table c on f.src_table = c.bare
on conflict (transform_table, column_name, src_table, key) do nothing;

delete from column_failures f
using trellis_v33_canonical_src_table c
where f.src_table = c.bare;

-- `transform_fuse_gate`: one lock row per source. `checks`/`last_checked_at`
-- are operator-facing bookkeeping (V30: "the lock, not the value, is the
-- point"), so they combine the obvious way — total checks, most recent check —
-- and the *row* becoming single-keyed is the actual fix: from here on both
-- spellings' evictions queue on one lock.
insert into transform_fuse_gate (src_table, checks, last_checked_at)
select c.qualified, g.checks, g.last_checked_at
from transform_fuse_gate g
join trellis_v33_canonical_src_table c on g.src_table = c.bare
on conflict (src_table) do update set
    checks = transform_fuse_gate.checks + excluded.checks,
    last_checked_at = greatest(
        transform_fuse_gate.last_checked_at, excluded.last_checked_at);

delete from transform_fuse_gate g
using trellis_v33_canonical_src_table c
where g.src_table = c.bare;

drop table trellis_v33_canonical_src_table;
