-- Automatic retry of held keys (issue #861).
--
-- `poison.retry_attempts` counts how many times the staging worker has
-- released the key on its own (`staging::quarantine::retry_held_keys`); a key
-- poisoned for the first time has 0. The key's next retry is due
-- `least(initial * 2^retry_attempts, cap)` after `poisoned_at`, so the
-- schedule is derived from the retry option rather than stored, and a change
-- to the option applies to keys already held.
--
-- `key_deaths.retry_attempts` carries the count across the release: the
-- retry's release writes the key's death counter one below the death
-- threshold with the incremented count, so a retried key that fails again is
-- poisoned on its first failure instead of a full threshold of rounds, and
-- the eviction copies the count onto the new `poison` row. A clean apply of
-- the key deletes the counter row (`clear_key_deaths`), which is what ends a
-- successful retry's state; an operator's release, a resume and a drop
-- delete it too.
alter table poison add column retry_attempts integer not null default 0;
alter table key_deaths add column retry_attempts integer not null default 0;
