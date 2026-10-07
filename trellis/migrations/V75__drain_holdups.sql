-- #817: a drain page that keeps failing with nothing charged or paused.
--
-- When a drain surfaces a page's error without charging a key or pausing a
-- definition (a refused read or write the catalog can't pin on a table,
-- isolation that reproduced nothing or hit its probe limit, isolate-eligible
-- retries exhausted), it records the page here, in a short transaction of
-- its own after the page's transaction rolled back (`staging::holdup`).
-- `Trellis::status` reports it as `drain_failure` on every unfrozen
-- definition that reads a table on the page, and `self_check` reports every
-- row. It charges and pauses nothing. A transient failure is never recorded:
-- retrying it is the expected behaviour.
--
-- One row per segment. `buckets` are the segment's buckets the failing
-- pages covered, as far as the failing worker still claimed them when it
-- recorded (it locks those claims while it does): several drain workers can hold different buckets of one
-- segment, so a page committing clears only its own buckets, in the
-- transaction that commits it, and the row goes once none is left. The
-- segment's own deletion (its retirement, or any other discard) takes the
-- row with it, so a holdup never outlives its page.
--
-- `tables` are the canonical source tables the failing pages held. `error`
-- and `sqlstate` are the latest failure's (`sqlstate` is null for one that
-- didn't come from Postgres). `since` is the first failure, `last_seen` the
-- latest, and `attempts` counts the failed drain passes in between.
create table drain_holdups (
    seg_seq bigint primary key references segments (seg_seq) on delete cascade,
    buckets smallint[] not null check (cardinality(buckets) > 0),
    tables text[] not null,
    error text not null,
    sqlstate text,
    since timestamptz not null,
    last_seen timestamptz not null,
    attempts integer not null check (attempts > 0)
);
