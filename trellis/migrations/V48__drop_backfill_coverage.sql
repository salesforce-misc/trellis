-- Issues #468, #485: retire the direct-build coverage record (V18).
--
-- A row here let a direct build's go-live catch-up skip re-reading a table
-- that looked unchanged since the build read it: the same row count, and no
-- row with an `xmin` newer than the build's fence. A row inserted after the
-- fence, read by the build and deleted again nets out on both, so the skip
-- kept a row the source no longer had (#468). The go-live catch-up now always
-- re-reads the table, and deletes the target rows no source row backs
-- (#485), so nothing reads this table any more.
drop table if exists backfill_coverage;
