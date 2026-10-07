-- #824: the staging worker re-types a column Trellis created when the
-- source widened it without a rewrite (a longer `varchar`, `text`, a
-- `numeric` with more precision), and then releases the keys a definition
-- held because a value didn't fit the old type in the meantime.
--
-- `poison.sqlstate` is the SQLSTATE of the failure that held the key, so
-- the release can tell a value too long (`22001`) or a numeric overflow
-- (`22003`) from every other failure, whatever language the server words
-- its messages in. `NULL` where the failure had none.
alter table poison add column sqlstate text;

-- One row per definition and SQLSTATE whose held keys a re-type released
-- the cause of, written in the re-type's own transaction, so a crash
-- between the two doesn't lose the release. The staging worker releases
-- the keys (`staging::quarantine::release_retyped_keys`) and then deletes
-- the row. It has no foreign key on purpose: the re-type's transaction
-- holds `ACCESS EXCLUSIVE` on a table a drop takes after the definition's
-- row, so it takes no lock on that row. The release deletes a row whose
-- definition is gone.
create table retype_releases (
    transform_id bigint not null,
    sqlstate text not null,
    requested_at timestamptz not null default now(),
    primary key (transform_id, sqlstate)
);
