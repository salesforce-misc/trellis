-- #1306: a build of a Trellis target that completes asks for the release of
-- every key each direct reader of that target holds (a definition whose
-- source, or `JOIN` side, is the target): a reader can hold a key for a
-- failure the upstream caused while it was being rebuilt, which the
-- rebuild has repaired.
--
-- One row per reader, written in the transaction that completes the
-- upstream's build, so the release reads the upstream's final rows and a
-- worker stop between the two doesn't lose it. The staging worker releases
-- the keys (`staging::quarantine::release_upstream_rebuilt_keys`) after each
-- capture pass, with the automatic retry's semantics, and deletes the row
-- once they are released and its window has passed. `first_read_at` is when
-- the first release after `requested_at` read the keys held: a later
-- release of the same request takes only keys held since before then, so a
-- key that fails again after its retry is not retried again. It has no
-- foreign key, as `retype_releases` has none (V74): the release deletes a
-- row whose definition is gone.
create table upstream_releases (
    transform_id bigint primary key,
    requested_at timestamptz not null default now(),
    first_read_at timestamptz
);
