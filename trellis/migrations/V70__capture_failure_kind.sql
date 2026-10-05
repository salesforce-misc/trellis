-- #663: what paused a definition recorded in `capture_failures`.
--
-- A drain whose page fails with a halting error (a key the drain can't use,
-- a runaway propagation wave, an aggregate off the ledger) used to retry the
-- same page every poll, forever. Now it pauses every definition that reads
-- the halting source, through a relationship too, and everything downstream
-- of them (`staging::halt`), recording the reason here like a capture
-- failure does, so the rest of the page commits and its segments retire.
-- Resuming the definition is the same rebuild, and deletes the row.
--
-- `kind` tells the two apart: `capture` for a capture failure (#622 C6,
-- #687, #745, #751, #765), `halt` for a drain's halt. Every existing row is
-- a capture failure.
alter table capture_failures add column kind text not null default 'capture'
    check (kind in ('capture', 'halt'));
