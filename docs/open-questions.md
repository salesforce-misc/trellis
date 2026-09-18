# Open Questions

A running list of decisions not yet made and investigations still open. When one
is resolved it should graduate into the relevant doc or an [ADR](decisions/) and
be removed from here.

## Quarantine policy details

[ADR-0003](decisions/0003-quarantine-storage-and-api.md) settles quarantine
storage, the read APIs, the per-column and transform-wide fuse tiers, the fixed
(non-configurable) fuse threshold, and fuse re-arm on resume. One policy knob
remains open:

* **Retry-with-backoff vs. immediate quarantine, and whether a dead-letter area
  is needed.** Today (issue #16's per-key track, `trellis/src/staging/quarantine.rs`)
  neither, exactly: transient failures always retry (a version-fence miss backs
  off via `FenceMissBackoff`), and only a non-transient, non-halting failure
  goes through per-key isolation — and even then a key isn't quarantined until
  it *repeatedly* isolates past the fuse threshold. There is no separate
  dead-letter area: `poison_held` (keyed `(src_table, key, seg_seq)`) is itself
  the parked storage, replayed onto the active batch by an operator-driven
  release. Whether an explicit backoff/dead-letter policy should replace this is
  the open question.

## Transform redefinition (post-v1)

v1 transforms are **immutable**: to change one you define a new transform and cut
over. The long-run model allows *some* edits — column-definition changes applied
via backfill, but **not** granularity changes (1-1 / aggregate / cross-join is
fixed at creation). The versioning scheme for the Postgres-stored schema and the
migration path for an in-place column edit are not yet designed.

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
