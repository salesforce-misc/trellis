-- #625 F4 (#616): a backfill chunk that fails says so, and stops retrying
-- blindly.
--
-- A failed chunk used to be released with no record of the failure, so a
-- chunk that failed on its data was claimed and failed again forever while
-- the definition sat in `backfilling` and `Trellis::status` reported
-- nothing. `defs::chunk_queue::fail_chunk` now records each failure on the
-- chunk's row and decides what happens next:
--
-- * `attempts`: how many times this chunk (or the chunk it was split from)
--   has failed. It sets the backoff and is what `Trellis::status` reports.
-- * `charged`: the failures that count toward pausing the definition. A
--   transient failure (a lost connection, a lock or serialization conflict)
--   isn't charged, and neither is a failure the chunk narrows to a key by
--   splitting itself in two.
-- * `last_error`: the latest failure's error.
-- * `next_attempt_at`: the earliest time the chunk may be claimed again.
--   `null` is "now"; `chunk_queue::claim_chunks` skips a row that isn't due.
alter table backfill_chunks
    add column attempts integer not null default 0,
    add column charged integer not null default 0,
    add column last_error text,
    add column next_attempt_at timestamptz;
