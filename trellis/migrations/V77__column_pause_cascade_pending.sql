-- Issue #912: a column pause whose cascade hasn't finished yet.
--
-- A column pause (`PAUSE TRANSFORM t.c`, or the column fuse tripping) commits
-- its own `column_status` row first, then walks its dependents
-- (`staging::quarantine::cascade_pause`), each pair in a transaction of its
-- own that can wait out the lock timeout behind a writer in flight. The pause
-- records here, in the transaction that writes its row, that the walk is
-- still owed; the walk clears it once it reaches every dependent. The
-- staging worker's capture pass finishes the walk of any pause still marked,
-- so a cascade that failed part-way, or whose process died, completes without
-- an operator noticing.
alter table column_status add column cascade_pending boolean not null default false;

create index column_status_cascade_pending on column_status (transform_table, column_name)
    where cascade_pending;
