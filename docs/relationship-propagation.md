# Relationship Propagation: The Obligation Table

Trellis has six independent code paths that can turn a change into a write
against a relationship-enriched target. Each was added at a different time, by a
different issue, and each had to independently relearn the same handful of edge
inputs — a `NULL` join key, a parent that doesn't exist yet, a source with the
wrong `REPLICA IDENTITY`. [Issue #173](https://github.com/salesforce-misc/trellis/issues/173)
notes that **11 of this repo's 27 `bug`-labeled issues are on the relationship
path**, and that the TRUNCATE-on-relationship-to-side case has been fixed and
re-broken *twice* (fixed for #98, regressed by epic #127 and re-filed as #165,
re-filed again as #168 once #167 un-masked it). The pattern is always the same:
a new path is added and doesn't handle an edge input a *previous* path had
already learned about.

This document is that checklist, in one place instead of scattered across doc
comments in `apply.rs`. Rows are the recurring edge inputs; columns are the
paths that must handle them (or provably don't). Each cell names the code site
that discharges the obligation and the test that pins it — or says **GAP**,
because finding the gaps is the point, not filling in a complete-looking table.

This is Phase 1 of #173's five-phase plan. Phase 2 (a derived replica-identity
precondition surface), Phase 3 (a shared edge-input enumeration replacing this
table with a compiler-enforced `match`), Phase 4 (turning this table into a real
test matrix), and Phase 5 (the encoded-key contract, #103/#107/#110/#125/#163/#171)
are out of scope.

## The six propagation paths

| Path | What it does | Entry point |
|---|---|---|
| **Forward read** | A `OneToOne`/`Aggregate` target re-evaluates a row that reads a relationship, resolving a to-one value from the settled parent projection or a to-many value from a live join. | `build_relationship_context` (`apply.rs:791`), from the by-source loop in `compute` (`apply.rs:3583`); to-one value via `fetch_relationship_projection_rows` (`apply.rs:2531`). |
| **Reverse delta (parent-keyed)** | A to-side (parent) row changes; every `Aggregate` target whose fields are all invertible (`Sum`/`Avg`/`Count`) gets a true per-row delta against the matching from-side rows, without re-deriving the group. | `build_reverse_relationship_shape` (`apply.rs:1315`), gated by `check_reverse_guards` (`apply.rs:1963`); applied via `apply_projection_advance` (`apply.rs:2386`) and `apply_aggregate::apply_delta_groups_bulk` (`apply_aggregate.rs:2636`). |
| **Reverse fallback / deferral** | Everything the delta path excludes: every `OneToOne` target reading a to-one relationship, any `Aggregate` with a non-invertible field, a definition reading more than one relationship, and — unconditionally — every to-many relationship. Stages an image-less `Recompute` per affected from-side row. | `stage_reverse_recompute_fallback` (`apply.rs:2266`), from `check_reverse_guards`'s rejection arms; the to-many case is a separate always-taken inline block (`apply.rs:3745-3808`), pre-dating epic #127 and never migrated onto the delta path. |
| **TRUNCATE clear** | A source `TRUNCATE` clears every direct target and reverse-recomputes every from-side row reading the truncated table *through* a relationship (a `TRUNCATE` stages one key-less sentinel, not per-row images). | The `truncated` loop in `compute` (`apply.rs:4341`), inbound-relationship handling at `apply.rs:4436-4478`. |
| **Aggregate incremental** | The per-batch `GROUP BY` delta engine: folds row-level contributions (including ones read through a relationship) into per-group deltas, forcing a full group recompute for anything image-less. | `accumulate_changes` (`apply_aggregate.rs:917`), forcing full recompute via `GroupPlan::force_full_recompute` (`apply_aggregate.rs:968`) and `apply_forced_groups_bulk` (`apply_aggregate.rs:1932`). |
| **Backfill** | The direct set-based initial build at `install_definition`/`create_relationship` time — one SQL pass, no ring, only for recognized shapes; anything else falls back to the ring. | `backfill_definition` (`backfill.rs:191`); `backfill_relationship_one_to_one` (`backfill.rs:1418`) for a to-many aggregate reading a to-one's own fields, `backfill_aggregate` (`backfill.rs:875`) for a `GROUP BY` target, `resolve_to_one_joins` (`backfill.rs:811`) for the to-one endpoints an aggregate leaf resolves through. |

Two structural facts shape several cells:

- **A `OneToOne` target never uses the delta fast path** — only `Aggregate`
  targets qualify (a 1-1 target has no additive semantics; it's re-derived
  outright). For a bare to-one enrichment, "Reverse delta" is always N/A and
  "Reverse fallback" is always in play.
- **The direct backfill build never handles a bare to-one passthrough field.**
  `backfill_relationship_one_to_one` bails to the ring on any unsupported shape
  (a bare to-one path, a nested relationship, a cyclic alias chain)
  (`backfill.rs:1449-1452`). `TRANSFORM t FROM a SELECT category.name AS x` is
  installed, backfilled through the *ring* (a `Recompute` per source row,
  discharged by Forward read), and never touches `backfill.rs` — confirmed by
  `install_definition_falls_back_to_ring_for_relationship_enriched_definition`
  (`defs_install_definition.rs:950`). Backfill's fast path is real only for a
  to-many aggregate over a to-one's own field, not for a bare to-one value.

## The obligation table

Legend: a function + test means **handled and pinned**. **N/A** means this path
structurally never sees this input (explained inline). **GAP** means a real,
unaddressed hole — see "Known gaps".

| Edge input | Forward read | Reverse delta | Reverse fallback | TRUNCATE clear | Aggregate incremental | Backfill |
|---|---|---|---|---|---|---|
| **TRUNCATE's key-less sentinel** (#98, #165, #168) | N/A — never sees a `TRUNCATE`; consumes what TRUNCATE-clear leaves behind. | N/A — a `TRUNCATE` carries no image, so no `RelationshipReverseRecord` can be built (`apply.rs:3572-3579`). | Reused — TRUNCATE clear feeds this path's `reverse_recomputes` accumulator directly. | **Handled.** `from_side_keys_with_non_null_join` (`apply.rs:695`) + `relationship_projection_clears` (`apply.rs:4462-4471`, the #168 fix). Pinned: `truncating_a_relationship_to_side_table_leaves_a_stale_enrichment` (`convergence.rs:1191`). | **Handled** for a table truncated directly (`AggregateClearPlan`, `apply.rs:4378-4388`) and reached *through* a relationship (forces `force_full_recompute` via the reused image-less `Recompute`). Pinned: `truncating_a_relationship_to_side_table_leaves_a_stale_aggregate_enrichment` (`convergence.rs:1099`). | N/A — backfill runs once against a live snapshot; `TRUNCATE` is a live CDC event only the ring sees. |
| **A `NULL` join key** (a literally-`NULL` FK; #128, ADR-0006/#33 for the nullability rule) | **Handled.** `build_relationship_context`'s join-key collection (`apply.rs:838-847`) skips `None`; a `NULL`/non-matching `from_col` resolves to `NULL` by left-join. Pinned: `a_to_one_relationship_aggregated_inside_a_group_by_converges_end_to_end` (`convergence.rs:998`), whose from-side draw holds both a literal `None` key and an unmatched non-`NULL` `"k9"`. | **Handled by construction** — a `NULL` `from_col` can never be a parent-change key (there's no parent). Pinned: `repoint_to_null_parent_converges_in_the_same_intake_window`/`_across_a_seal_boundary` (`relationship_interleaving.rs:254,287`). | **Handled by construction** — `WHERE col = ANY($1)` can never match a `NULL` (SQL three-valued logic). Covered incidentally whenever the generative suite draws a `NULL` FK. | **Handled**; deliberately excludes already-`NULL` rows for efficiency (`from_side_keys_with_non_null_join` doc comment, `apply.rs:684-694`). No test asserts the exclusion itself — see gap 2. | **Handled** — `augment_row_with_forward_relationships`'s `.and_then` chain (`apply_aggregate.rs:864-870`) yields `None` for a `NULL`/unmatched `from_col`. Pinned by the same `converges_end_to_end` test. | N/A for a bare to-one passthrough (ring; pinned by `frontdoor_to_one_enrichment_converges_to_oracle`, a Forward-read test). **Handled** for a to-many aggregate: `relationship_build_matches_oracle_including_no_match_and_multi_child` (`defs_backfill_relationship.rs:181`). |
| **A composite source primary key** (#126, #163) | **N/A in the common case, halts in the ring-bypass case.** A `OneToOne` target can only install against a single-column source PK (`ddl::require_single_column_pk`, `ddl.rs:458`) — so through the front door this can't occur. Reachable only by bypassing that gate via raw `create_definition`; then `apply.rs:3952` surfaces `DdlError::CompositePrimaryKeyUnsupported` and **halts the instance** rather than mis-deriving. Pinned: `a_composite_primary_key_source_is_never_quarantined_and_stops_the_instance` (`quarantine.rs:654`). #163's `extract_key`/`pk_key_sql_expr` ordering risk is now fixed — `extract_key` normalizes to the primary key's declared order, the order every consumer decodes with. | **Handled.** `reverse_update_of_the_to_side_row_updates_every_dependent_group` (`defs_relationship_composite_pk.rs:341`) drives a to-side update against a composite-PK (`post`, `tag`) from-side. | Same code path as Reverse delta. | N/A in the common case, same halt as Forward read for a `OneToOne` source; `Aggregate` targets need no PK narrowing (`apply.rs:4378-4388`) and truncate-clear correctly. | **Handled.** `plain_aggregate_over_a_composite_pk_source_drains_with_no_relationship_at_all` (`:398`) and `forward_insert_of_a_composite_pk_from_side_row_updates_its_group_total` (`:279`). | **Handled.** `aggregate_over_a_to_one_relationship_with_composite_pk_backfills_to_the_oracle` (`:237`), via `read_live_rows_batch`'s batched composite-key re-fetch. |
| **A missing/nonexistent parent, and an FK re-point to one** (#138, PR #169) | **Handled** — same "unmatched key resolves to `NULL`" mechanism as the `NULL`-key row. Pinned by `to_one_enrichment_nulls_out_when_the_related_row_appears_then_disappears` (`defs_relationship_nullability.rs:213`, the full no-match → appears → disappears arc), the `converges_end_to_end` unmatched `"k9"`, and `frontdoor_to_one_enrichment_converges_to_oracle` (`defs_relationship_frontdoor.rs:205`, category `99` missing). | **Handled** — the row #138 was written to stress: a parent `INSERT` turning a dangling reference live. Pinned: `parent_insert_is_picked_up_by_the_reverse_path` (`apply_relationship_reverse.rs:532`), `parent_delete_is_picked_up_by_the_reverse_path` (`:590`), and the timing matrix in `repoint_to_nonexistent_parent_converges_in_the_same_intake_window`/`_across_a_seal_boundary` (`relationship_interleaving.rs:249,278`). | Shares `stage_reverse_recompute_fallback`; a from-side row pointing at a since-appeared/vanished parent is enumerated by `from_side_rows_for_join_txn`'s live re-read regardless. No pin distinct from the Reverse-delta ones. | N/A — a `TRUNCATE` has no per-row key to re-point. | **Handled, dedicated pins.** `accumulate_changes` resolves the relationship read *once per side* (`old_row` and `new_row` independently), so a re-point subtracts the old parent and adds the new in one delta. Pinned by `a_from_side_re_point_within_one_update_diffs_old_and_new_parent_contributions` (`defs_aggregate_relationship.rs:537`), `updating_a_to_side_row_updates_every_dependent_group` (`:470`), `inserting_a_from_side_row_resolves_from_the_projection_not_live_parent_state` (`:389`), `avg_over_a_relationship_read_column_maintains_through_backfill_and_forward_insert` (`:614`). Uncovered sliver: no test drives a delta-path re-point whose old/new side is the nonexistent parent — gap 1. | N/A for a bare to-one passthrough; **handled** for a to-many aggregate's missing-children case via `relationship_build_matches_oracle_including_no_match_and_multi_child` (see the `NULL`-key row). |
| **A source lacking the needed `REPLICA IDENTITY`** (#41, #47, #158) | N/A at apply time — rejected earlier at `create_relationship`. | N/A at apply time. | N/A at apply time. | N/A — a `TRUNCATE` carries no image, so replica identity is irrelevant. | N/A at apply time — rejected by `assert_replica_identity_supports_aggregate` (`catalog.rs:2603`) → `intake::require_replica_identity_full` (`replica_identity.rs:46`). This gate sits in `create_definition_inner` (`catalog.rs:1026`), so it holds on the raw `create_definition` path too. Pinned: `an_aggregate_transform_against_default_replica_identity_is_rejected` / `..._replica_identity_full_is_accepted` (`defs_catalog.rs:1444,1498`). | N/A — backfill reads a live full-table snapshot, not a CDC image; the later incremental drain is what the declare-time gate protects. |
| **A shared from-table reachable via two relationships** (#79's double-count) | **Handled.** `build_relationship_context` resolves each relationship independently, and `forward_relationship_synthetic_column` (`apply_aggregate.rs:655`) namespaces the synthetic column by **both** relationship name and column, so two relationships reading a same-named to-side column can't collide. Pinned: `two_relationships_sharing_a_to_side_column_name_resolve_independently` (`defs_aggregate_relationship.rs:745`). | N/A — any definition referencing more than one *distinct* relationship sets `needs_recompute_fallback` and never reaches the delta path (`apply.rs:1372-1376`). | **Handled — where #79 actually lived.** The `reverse_recomputes` accumulator is one `HashMap` keyed `(from_table, from_key)` shared across every inbound relationship, so two relationships resolving to the same key collapse to one `Recompute` (`hop_gen` merged by `max`, `src_changed` by earliest). Pinned: `reverse_recompute_dedupes_across_relationships_sharing_from_table` (`apply_relationships.rs:718`, counts staged rows *without* `distinct`) and `reverse_recompute_fan_in_keeps_the_earliest_src_changed` (`:843`). | **Handled by reuse.** The `truncated` loop pushes into that same deduped accumulator; `relationship_projection_clears` is a `HashSet` of qualified table names. No test combines a `TRUNCATE` with two shared-from-table relationships; the dedupe is structural (the container types). | **Handled** — same synthetic-column namespacing as Forward read; the pinning test above is an aggregate definition. | **Handled** — #79's repro is a `backfill_relationship_one_to_one` build of a 1-1 target fed by two to-many relationships, which built correctly (the bug was the ring backlog behind it). Covered by the backfill half of the two-relationship test above. |

## Known gaps

Three items fell out of filling in this table — the point of the exercise, not
a completeness failure:

1. **No test drives an aggregate delta-path re-point across the
   *nonexistent*-parent boundary.** The missing-parent and re-point handling is
   each well pinned, but no fixture combines them: an unmatched from-side row is
   unmatched from backfill onward and stays that way, so "old side resolved to a
   real parent, new side resolves to nothing" is only exercised through the
   *reverse* path, never through `accumulate_changes`'s own old/new resolution.
   The code reads correct (both sides go through the same `.and_then` chain),
   but the assertion is missing. Cheap to add: one `UPDATE post_tags SET post = 999`
   on `defs_aggregate_relationship.rs`'s existing fixture.
2. **`from_side_keys_with_non_null_join`'s `NULL`-exclusion is argued in a doc
   comment, not asserted in a test.** Nothing breaks today if it's wrong (a
   `NULL`-FK row just recomputes to the same `NULL`), but a future change that
   made the exclusion *incorrect* — e.g. relying on it to skip *newly*-`NULL`
   rows that still need clearing — would have no test to catch it.
3. **The composite-PK/`OneToOne`-target interaction halts rather than rejects
   when reached off the front door.** `install_definition`'s
   `require_single_column_pk` gate means this can't happen through ordinary use,
   but `create_definition` has no equivalent gate, so a caller skipping
   `install_definition` reaches the halting `CompositePrimaryKeyUnsupported`
   path. Safe (no corruption) but still an availability bug. The aggregate
   replica-identity gate *is* enforced in `create_definition_inner`
   (`catalog.rs:1026`) and covers both entry points — this gate could move the
   same way. Filed as #177. (#163 — composite-key column ordering — is now
   fixed and unrelated to #177, which is about entry-point enforcement, not
   key encoding.)

No cell above is a *silent* correctness gap — every "N/A" is backed by a
structural reason (a declare-time gate, or an input shape that provably can't
reach that path). That is the table's value: check the next new path against
every row here before it ships, not after.

## Related: the validation functions Phase 2 will consolidate

Three functions currently enforce the replica-identity row, one per feature:

- `intake::replica_identity::require_replica_identity_full` (`replica_identity.rs:46`) — #7's scaffolding, the shared rejection every caller below delegates to.
- `defs::catalog::assert_replica_identity_supports_aggregate` (`catalog.rs:2603`) — #47, gates a `GROUP BY` source at `install_definition` time.
- `defs::catalog::assert_replica_identity_supports_projection` (`catalog.rs:2712`) — #129/epic #127, extended to the from-side by #158, gates a to-one relationship's to-side and from-side at `create_relationship` time.

(A fourth, `assert_replica_identity_supports_to_many` (`catalog.rs:2536`), gates
a to-many's to-side — #41 — and accepts a narrower covering
`REPLICA IDENTITY USING INDEX`, since a to-many join needs only one non-PK
column's old value.)

Per #173 Phase 2, these should collapse into one function that derives the
source-table guarantees a resolved plan needs from the plan itself, so adding a
new plan shape that needs a new guarantee is a compile-or-test failure, not a
silent gap the next #41/#47/#158 rediscovers.
