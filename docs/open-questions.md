# Open Questions

Decisions not yet made and investigations still open. When one resolves it
graduates into the relevant doc or an [ADR](decisions/) and leaves this list.

## Quarantine: backoff vs. dead-letter

[ADR-0003](decisions/0003-quarantine-storage-and-api.md) settles quarantine
storage, read APIs, fuse tiers, threshold, and re-arm. One knob stays open:
**should an explicit retry-backoff/dead-letter policy replace today's behavior?**

Today (issue #16, `trellis/src/staging/quarantine.rs`): transient failures always
retry (version-fence misses back off via `FenceMissBackoff`); a non-transient,
non-halting failure goes through per-key isolation, and a key is quarantined only
after it *repeatedly* isolates past the fuse threshold. There is no separate
dead-letter area — `poison_held` (keyed `(src_table, key, seg_seq)`) is the parked
storage, replayed onto the active batch by an operator release.

## Transform redefinition (post-v1)

v1 transforms are **immutable**: to change one, define a new transform and cut
over. The long-run model allows column-definition edits via backfill, but **not**
granularity changes (1-1 / aggregate / cross-join is fixed at creation). Still to
design: the Postgres schema versioning scheme and the in-place column-edit
migration path.

One target is settled regardless of API: a definition's calculated fields (and the
relationships they reference) apply **together, backfilling in a single pass** —
enumerating the source once, not once per column. Growing a table must never spawn
a backfill job per added column; single-column incremental backfill is only an
optimization.

## Transform grammar — concrete syntax

[ADR-0004](decisions/0004-transform-definition-grammar.md) settles the approach (a
minimal, purpose-built SQL-flavored grammar over our own execution layer) and the
1-1/`+`-only outer shape (`TRANSFORM ... FROM ... SELECT ... AS ... [WHERE ...]`).
Still open:

* Cross-join side-qualification notation (function-call spelling is settled in ADR-0004).
* Whether the grammar and its stored schema version independently of the
  redefinition scheme above.

Relationship grammar is settled in
[0006-relationships](decisions/0006-relationships.md): the standalone
`RELATIONSHIP ... FROM ... TO ...` declaration, the name-headed path
(`relationship.column`), and the to-one/to-many cardinality rules.
