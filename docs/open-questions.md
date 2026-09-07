# Open Questions

A running list of decisions not yet made and investigations still open. When one
is resolved it should graduate into the relevant doc or an [ADR](decisions/) and
be removed from here.

## Quarantine policy details

[ADR-0003](decisions/0003-quarantine-storage-and-api.md) fixed quarantine
storage and the read APIs but left the policy knobs open:

* The fuse threshold (fixed count vs. percentage) and whether it's
  configurable per transform.
* Whether tripping the fuse should pause transforms chained onto the fused
  one, or let them keep reading its last-written data.
* Retry-with-backoff vs. immediate quarantine, and whether a dead-letter area
  is needed.

Issue #16 (per-key isolate/evict/park/release, `engine/src/staging/quarantine.rs`)
resolved two of these knobs for its own poison/poison_held/key_deaths track —
a distinct mechanism from ADR-0003's transform-wide fuse, but facing the same
open questions:

* **Fuse threshold**: fixed count, not a percentage — `DEFAULT_DEATH_THRESHOLD
  = 5` (a key evicts once its `key_deaths.deaths` count reaches 5; `0`
  disables eviction, deferring to the halting-schema-error instance-stop path
  instead). Not yet configurable per source table or transform — this crate
  has no per-instance config surface today, so every call site uses the
  constant directly.
* **Retry-with-backoff vs. immediate quarantine**: neither, exactly —
  transient failures always retry (no quarantine); a version fence miss
  retries with `FenceMissBackoff`'s existing consecutive-miss backoff; only a
  non-transient, non-halting failure goes through per-key isolation, and even
  then a key isn't quarantined until it *repeatedly* isolates past the fuse
  threshold above. There is no separate dead-letter area: `poison_held` (keyed
  `(src_table, key, seg_seq)`) is itself the parked/dead-letter storage,
  replayed back onto the active batch by an operator-driven release.

## Transform redefinition (post-v1)

v1 transforms are **immutable**: to change one you define a new transform and cut
over. The long-run model allows *some* edits — column-definition changes applied
via backfill, but **not** granularity changes (1-1 / aggregate / cross-join is
fixed at creation). The versioning scheme for the Postgres-stored schema and the
migration path for an in-place column edit are not yet designed.

[ADR-0007](decisions/0007-source-object-identity.md) settles the identity rule:
a definition remains bound to the table and columns resolved when it was accepted;
a drop-and-recreate at the same name never transfers that binding. The concrete API
for repairing, rebinding, or replacing an invalidated definition remains part of
this redefinition work.

One target is settled independent of the API surface: a definition's calculated
fields (and the relationships they reference) are chosen and applied **together,
as one atomic unit that backfills in a single pass** — defining a table with an
initial set of columns enumerates the source once, not once per column. Growing a
table must never require a separate backfill job per added column; a batch of
column edits should likewise backfill in one pass, with single-column incremental
backfill available only as an optimization. The concrete redefinition API is
still to be designed.

## Transform grammar — concrete syntax

[ADR-0004](decisions/0004-transform-definition-grammar.md) settled the approach
(a minimal, purpose-built SQL-flavored grammar over our own execution layer)
and, for the 1-1/`+`-only slice (issue #22), the outer statement shape
(`TRANSFORM ... FROM ... SELECT ... AS ... [WHERE ...]`). Still open:

* Cross-join side-qualification notation (general and aggregate function-call
  spelling are now settled — see ADR-0004).
* Whether the grammar and its stored schema are versioned independently of the
  transform-redefinition scheme (above).

Relationship grammar and reference syntax are no longer open — the standalone
`RELATIONSHIP ... FROM ... TO ...` declaration, the relationship-name-headed path
(`relationship.column`), and the to-one/to-many cardinality rules are settled in
[0006-relationships](decisions/0006-relationships.md).
