-- #623 D5: every aggregate target is on the ledger, so nothing reads or
-- raises an extinct horizon (V40) any more. A change for a group with no row
-- applies to its ledger entry, whose basis already says whether a live read
-- counted it.
--
-- Aggregate targets created before this keep their `__trellis_recompute_lsn`
-- column. Nothing writes or reads it.
drop table aggregate_extinct_horizon;
