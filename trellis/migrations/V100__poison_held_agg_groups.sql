-- #1102: a held key's groups on a group-basis aggregate.
--
-- While a key is held, a group-basis aggregate skips its changes, so the
-- groups they feed drift until the release. The release recomputes every
-- group the key could owe (ADR-0002, "a release on an aggregate"): the
-- groups named by the old and new images of every change parked for the
-- key, the evicting change's among them, plus its current row's group. The
-- parked changes themselves are discarded, so the held row keeps those
-- groups, and no image: `agg_groups` holds one object of the definition's
-- `GROUP BY` columns per group, as text, deduplicated. Every park merges
-- into it. It stays NULL for a definition that isn't on the group basis.

alter table poison_held add column agg_groups text[];
