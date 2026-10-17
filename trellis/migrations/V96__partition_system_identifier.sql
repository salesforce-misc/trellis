-- ADR-0017 P8a (#1067): the cluster's `system_identifier` as of the pass that
-- recorded each partitioned source. A changed one makes the pass compare the
-- recorded oids and bounds with the live tree: all present is `pg_upgrade`,
-- which resets every `xmin`, so the pass records them again and corrects
-- nothing.
alter table partition_sources add column system_identifier text;
