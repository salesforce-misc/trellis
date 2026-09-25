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

This is Phase 1 of #173's five-phase plan. Phase 5 (the encoded-key contract,
#103/#107/#110/#125/#163/#171) remains out of scope for this document.

**Phase 4** (turning this table into a real test matrix) has since landed:
`generative/tests/relationship_interleaving.rs`'s `obligation_matrix` module
is this same table as code — a closed `Path` enum, a closed `EdgeInput` enum,
and an exhaustive `cell()` match with no wildcard arm, so a missing
classification is a compiler error rather than an empty-looking cell. Every
`Handled` cell there names the real test(s) that pin it (re-homed into that
file when the scenario is a `ManualBackend`-driven interleaving case,
referenced by `file::test_name` otherwise); every `NotApplicable` cell names
the structural reason. Filling that matrix in found two cells this document
had under-verified — see "Known gaps" below, both now closed by test-only
changes.

**Phase 2** (a derived replica-identity precondition surface) and **Phase 3**
(a shared edge-input enumeration) have since landed. Phase 3, specifically,
gives the TRUNCATE row below a compiler-enforced home: every from-side
enumeration site in `apply.rs` now builds a `ReverseTrigger` (`Keys(&[String])`
or `WholeKeyspace`, `apply.rs:700`) and dispatches off an exhaustive `match`
with no wildcard arm — `from_side_keys` (`apply.rs:745`, replacing the former
`from_side_keys_for_join`/`from_side_keys_with_non_null_join`) for the
pool-based lookup the to-many reverse and TRUNCATE-clear paths share, and
`from_side_rows_for_trigger_txn` (`apply.rs:1739`, replacing
`from_side_rows_for_join_txn`) for the transactional full-row lookup the
reverse-delta fast path and reverse fallback share. Adding a third variant to
`ReverseTrigger` is now a compile error at both match sites, not a case a new
path can silently skip the way TRUNCATE's key-less sentinel was forgotten
three times (#98, #165, #168). This table's own line-number citations below
are updated to match; the table's *shape* (which path handles which edge
input) is unchanged by Phase 3 — it was a refactor of *how* the decision is
made, not a change to what any path does.

## The propagation paths

| Path | What it does | Entry point |
|---|---|---|
| **Forward read** | A `OneToOne`/`Aggregate` target re-evaluates a row that reads a relationship, resolving a to-one value from the settled parent projection or a to-many value from a live join. | `build_relationship_context` (`apply.rs:870`), from the by-source loop in `compute` (`apply.rs:3928`); to-one value via `fetch_relationship_projection_rows` (`apply.rs:2637`). |
| **Reverse delta (parent-keyed)** | A to-side (parent) row changes; every `Aggregate` target whose fields are all invertible (`Sum`/`Avg`/`Count`) gets a true per-row delta against the matching from-side rows, without re-deriving the group. | `build_reverse_relationship_shape` (`apply.rs:1394`), gated by `check_reverse_guards` (`apply.rs:2071`); applied via `apply_projection_advance` (`apply.rs:2492`) and `apply_aggregate::apply_delta_groups_bulk` (`apply_aggregate.rs:2636`). Its from-side enumeration is a `ReverseTrigger::Keys` dispatch through `from_side_rows_for_trigger_txn` (`apply.rs:1739`) — see the Phase 3 note above. |
| **Reverse fallback / deferral** | Everything the delta path excludes: every `OneToOne` target reading a to-one relationship, any `Aggregate` with a non-invertible field, a definition reading more than one relationship, and — unconditionally — every to-many relationship. Stages an image-less `Recompute` per affected from-side row. | `stage_reverse_recompute_fallback` (`apply.rs:2374`), from `check_reverse_guards`'s rejection arms; the to-many case is a separate always-taken inline block (`apply.rs:4096-4157`), pre-dating epic #127 and never migrated onto the delta path. Both now enumerate from-side rows through the same `ReverseTrigger`-driven functions the other paths use (Phase 3). |
| **TRUNCATE clear** | A source `TRUNCATE` clears every direct target and reverse-recomputes every from-side row reading the truncated table *through* a relationship (a `TRUNCATE` stages one key-less sentinel, not per-row images). | The `truncated` loop in `compute` (`apply.rs:4695`), inbound-relationship handling at `apply.rs:4790-4844`. Its from-side enumeration is a `ReverseTrigger::WholeKeyspace` dispatch through `from_side_keys` (`apply.rs:745`) — the same function, and the same enum, the to-many reverse path's `ReverseTrigger::Keys` dispatch below uses. |
| **Aggregate incremental** | The per-batch `GROUP BY` delta engine: folds row-level contributions (including ones read through a relationship) into per-group deltas, forcing a full group recompute for anything image-less. | `accumulate_changes` (`apply_aggregate.rs:917`), forcing full recompute via `GroupPlan::force_full_recompute` (`apply_aggregate.rs:968`) and `apply_forced_groups_bulk` (`apply_aggregate.rs:1932`). |
| **Backfill** | The direct set-based initial build, dispatched by the backfill discharge as one background job a drain thread runs (ADR-0016) — one SQL pass, no ring, only for recognized shapes; anything else falls back to the ring. | `backfill_definition` (`backfill.rs:191`); `backfill_relationship_one_to_one` (`backfill.rs:1418`) for a to-many aggregate reading a to-one's own fields, `backfill_aggregate` (`backfill.rs:875`) for a `GROUP BY` target, `resolve_to_one_joins` (`backfill.rs:811`) for the to-one endpoints an aggregate leaf resolves through. |
| **Seam-fed endpoint** | A relationship endpoint that is one of this instance's own targets (issue #375, #403). It is never published: the target-mutation seam stages each write to it as a CDC-shaped ring row (prior and new image, a write token as `lsn`, `group_key` for a from-side), so every path above consumes it exactly as it would a source's CDC. | `staging::target_mutations` (`TargetMutations::into_staged`, `read_new_images`). |

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
| **TRUNCATE's key-less sentinel** (#98, #165, #168) | N/A — never sees a `TRUNCATE`; consumes what TRUNCATE-clear leaves behind. | N/A — a `TRUNCATE` carries no image, so no `RelationshipReverseRecord` can be built (the both-images-absent skip, `apply.rs:4174-4179`); reflected in the type since Phase 3 too — `from_side_rows_for_trigger_txn`'s `ReverseTrigger::WholeKeyspace` arm returns a typed `ApplyError::ReverseTriggerNotResolvable` for exactly this reason (`apply.rs:1739`), a typed error rather than a panic per that module's convention for invariants a function cannot enforce against a future caller. Pinned: `reverse_trigger_whole_keyspace_is_a_typed_error_not_a_panic_in_the_txn_lookup`. | Reused — TRUNCATE clear feeds this path's `reverse_recomputes` accumulator directly. | **Handled.** `ReverseTrigger::WholeKeyspace` via `from_side_keys` (`apply.rs:745`, its `WholeKeyspace` arm) + `relationship_projection_clears` (`apply.rs:4815-4824`, the #168 fix). Pinned: `truncating_a_relationship_to_side_table_leaves_a_stale_enrichment` (re-homed by #173 phase 4 to `relationship_interleaving.rs`, alongside its DELETE control `deleting_a_relationship_to_side_row_does_clear_the_enrichment`). | **Handled** for a table truncated directly (`AggregateClearPlan`, `apply.rs:4732-4741`) and reached *through* a relationship (forces `force_full_recompute` via the reused image-less `Recompute`). Pinned: `truncating_a_relationship_to_side_table_leaves_a_stale_aggregate_enrichment` (re-homed by #173 phase 4 to `relationship_interleaving.rs`). | N/A — backfill runs once against a live snapshot; `TRUNCATE` is a live CDC event only the ring sees. |
| **A `NULL` join key** (a literally-`NULL` FK; #128, ADR-0006/#33 for the nullability rule) | **Handled.** `build_relationship_context`'s join-key collection (`apply.rs:917-925`) skips `None`; a `NULL`/non-matching `from_col` resolves to `NULL` by left-join. Pinned: `a_to_one_relationship_aggregated_inside_a_group_by_converges_end_to_end` (`convergence.rs:998`), whose from-side draw holds both a literal `None` key and an unmatched non-`NULL` `"k9"`. | **Handled by construction** — a `NULL` `from_col` can never be a parent-change key (there's no parent). Pinned: `repoint_to_null_parent_converges_in_the_same_intake_window`/`_across_a_seal_boundary` (`relationship_interleaving.rs:254,287`). | **Handled by construction** — `WHERE col = ANY($1)` can never match a `NULL` (SQL three-valued logic). Covered incidentally whenever the generative suite draws a `NULL` FK. | **Handled**; deliberately excludes already-`NULL` rows for efficiency (`ReverseTrigger::WholeKeyspace`'s doc comment, `apply.rs:708-719`). Pinned since #173 phase 3: `reverse_trigger_whole_keyspace_matches_every_non_null_row_via_from_side_keys` (gap 2, now closed). | **Handled** — `augment_row_with_forward_relationships`'s `.and_then` chain (`apply_aggregate.rs:864-870`) yields `None` for a `NULL`/unmatched `from_col`. Pinned by the same `converges_end_to_end` test. | N/A for a bare to-one passthrough (ring; pinned by `frontdoor_to_one_enrichment_converges_to_oracle`, a Forward-read test). **Handled** for a to-many aggregate: `relationship_build_matches_oracle_including_no_match_and_multi_child` (`defs_backfill_relationship.rs:181`). |
| **A composite source primary key** (#126, #163, #177) | **N/A — rejected at declare time on every entry point** (was halt-on-bypass before #177). A `OneToOne` target can only be created against a single-column source PK: `ddl::require_single_column_pk` (`ddl.rs:458`) gates `install_definition`'s DDL step *and*, since #177, `create_definition_inner` itself (`catalog.rs:1305`), so `create_definition`/`create_definition_without_backfill` reject it too, with a typed error, before any row is persisted. Pinned: `a_one_to_one_transform_against_a_composite_primary_key_source_is_rejected` (`defs_catalog.rs:1770`) and `a_composite_primary_key_source_is_rejected_at_create_time_not_quarantined_or_halted` (`quarantine.rs:667`). `apply.rs:4306`'s narrowing remains as a backstop for a source whose PK *becomes* composite after its definition exists — still a typed halt there, by design. #163's `extract_key`/`pk_key_sql_expr` ordering risk is fixed too — `extract_key` normalizes to the primary key's declared order, the order every consumer decodes with. | **Handled.** `reverse_update_of_the_to_side_row_updates_every_dependent_group` (`defs_relationship_composite_pk.rs:341`) drives a to-side update against a composite-PK (`post`, `tag`) from-side. | Same code path as Reverse delta. | **N/A — same declare-time rejection as Forward read** for a `OneToOne` source's own PK (and the same halting backstop for a PK that turns composite later); `Aggregate` targets need no PK narrowing (`apply.rs:4732-4741`) and truncate-clear correctly. | **Handled.** `plain_aggregate_over_a_composite_pk_source_drains_with_no_relationship_at_all` (`:398`) and `forward_insert_of_a_composite_pk_from_side_row_updates_its_group_total` (`:279`). | **Handled.** `aggregate_over_a_to_one_relationship_with_composite_pk_backfills_to_the_oracle` (`:237`), via `read_live_rows_batch`'s batched composite-key re-fetch. |
| **A missing/nonexistent parent, and an FK re-point to one** (#138, PR #169) | **Handled** — same "unmatched key resolves to `NULL`" mechanism as the `NULL`-key row. Pinned by `to_one_enrichment_nulls_out_when_the_related_row_appears_then_disappears` (`defs_relationship_nullability.rs:213`, the full no-match → appears → disappears arc), the `converges_end_to_end` unmatched `"k9"`, and `frontdoor_to_one_enrichment_converges_to_oracle` (`defs_relationship_frontdoor.rs:205`, category `99` missing). | **Handled** — the row #138 was written to stress: a parent `INSERT` turning a dangling reference live. Pinned: `parent_insert_is_picked_up_by_the_reverse_path` (`apply_relationship_reverse.rs:532`), `parent_delete_is_picked_up_by_the_reverse_path` (`:590`), and the timing matrix in `repoint_to_nonexistent_parent_converges_in_the_same_intake_window`/`_across_a_seal_boundary` (`relationship_interleaving.rs:249,278`). | Shares `stage_reverse_recompute_fallback`; a from-side row pointing at a since-appeared/vanished parent is enumerated by `from_side_rows_for_trigger_txn`'s live re-read regardless. No pin distinct from the Reverse-delta ones. | N/A — a `TRUNCATE` has no per-row key to re-point. | **Handled, dedicated pins.** `accumulate_changes` resolves the relationship read *once per side* (`old_row` and `new_row` independently), so a re-point subtracts the old parent and adds the new in one delta. Pinned by `a_from_side_re_point_within_one_update_diffs_old_and_new_parent_contributions` (`defs_aggregate_relationship.rs:537`), `updating_a_to_side_row_updates_every_dependent_group` (`:470`), `inserting_a_from_side_row_resolves_from_the_projection_not_live_parent_state` (`:389`), `avg_over_a_relationship_read_column_maintains_through_backfill_and_forward_insert` (`:614`). Closed by #173 phase 4 (Known Gap 1): `a_from_side_re_point_to_a_nonexistent_parent_subtracts_the_old_contribution` (`defs_aggregate_relationship.rs`) drives the delta path's own old/new resolution across the nonexistent-parent boundary directly. | N/A for a bare to-one passthrough; **handled** for a to-many aggregate's missing-children case via `relationship_build_matches_oracle_including_no_match_and_multi_child` (see the `NULL`-key row). |
| **A source lacking the needed `REPLICA IDENTITY`** (#41, #47, #158) | N/A at apply time — rejected earlier at `create_relationship`. | N/A at apply time. | N/A at apply time. | N/A — a `TRUNCATE` carries no image, so replica identity is irrelevant. | N/A at apply time — rejected by `assert_replica_identity_supports_aggregate` (`catalog.rs:2603`) → `intake::require_replica_identity_full` (`replica_identity.rs:46`). This gate sits in `create_definition_inner` (`catalog.rs:1026`), so it holds on the raw `create_definition` path too — exactly where the composite-PK/`OneToOne` gate above was moved as well by #177. Pinned: `an_aggregate_transform_against_default_replica_identity_is_rejected` / `..._replica_identity_full_is_accepted` (`defs_catalog.rs:1444,1498`). | N/A — backfill reads a live full-table snapshot, not a CDC image; the later incremental drain is what the declare-time gate protects. |
| **A shared from-table reachable via two relationships** (#79's double-count) | **Handled.** `build_relationship_context` resolves each relationship independently, and `forward_relationship_synthetic_column` (`apply_aggregate.rs:655`) namespaces the synthetic column by **both** relationship name and column, so two relationships reading a same-named to-side column can't collide. Pinned: `two_relationships_sharing_a_to_side_column_name_resolve_independently` (`defs_aggregate_relationship.rs:745`). | N/A — any definition referencing more than one *distinct* relationship sets `needs_recompute_fallback` and never reaches the delta path (`apply.rs:1451-1453`, "Design fork 3"). | **Handled — where #79 actually lived.** The `reverse_recomputes` accumulator is one `HashMap` keyed `(from_table, from_key)` shared across every inbound relationship, so two relationships resolving to the same key collapse to one `Recompute` (`hop_gen` merged by `max`, `src_changed` by earliest). Pinned: `reverse_recompute_dedupes_across_relationships_sharing_from_table` (`apply_relationships.rs:718`, counts staged rows *without* `distinct`) and `reverse_recompute_fan_in_keeps_the_earliest_src_changed` (`:843`). | **Handled.** The `truncated` loop pushes into that same deduped accumulator; `relationship_projection_clears` is a `HashSet` of qualified table names. Pinned since #173 phase 4 (Known Gap 4, now closed): `reverse_recompute_dedupes_a_truncate_against_a_relationship_sharing_the_same_from_table` (`apply_relationships.rs`). | **Handled** — same synthetic-column namespacing as Forward read; the pinning test above is an aggregate definition. | **Handled** — #79's repro is a `backfill_relationship_one_to_one` build of a 1-1 target fed by two to-many relationships, which built correctly (the bug was the ring backlog behind it). Covered by the backfill half of the two-relationship test above. |

### The seam-fed endpoint column

A seventh column, kept out of the table above for width. It covers only how
an endpoint target's changes *reach* the other six paths; what each path then
does with them is the row above.

| Edge input | Seam-fed endpoint |
|---|---|
| **TRUNCATE's key-less sentinel** | **Handled.** A target is cleared through the seam (`apply::clear_target`), so its readers get a CDC-shaped delete per key, never the sentinel. Pinned: `a_truncate_clear_of_an_endpoint_target_stages_per_key_deletes` (`endpoint_seam_feed.rs`). |
| **A `NULL` join key** | **Handled.** An aggregate target's `NULL` group is re-read `NULL`-safely, and a `NULL` `to_col` never becomes a projection row (`apply_projection_advance`). Pinned: `an_aggregate_target_endpoint_is_fed_by_the_seam_null_group_included` (`endpoint_seam_feed.rs`), `read_new_images_matches_null_grouping_components` (`target_mutations.rs`). |
| **A composite source primary key** | **Handled.** Pinned: `read_new_images_matches_composite_mixed_type_keys` (`target_mutations.rs`). |
| **A missing/nonexistent parent, and an FK re-point to one** | **Handled.** The seam row's `group_key` names every parent either image touched. Pinned: `a_from_side_targets_seam_group_key_bumps_an_erased_parents_gen` (`endpoint_seam_feed.rs`). |
| **A source lacking the needed `REPLICA IDENTITY`** | **Handled by construction.** The endpoint is never published, so its identity is never read; the seam captures prior images under its own row lock. Pinned: `relationship_target_endpoints.rs`. |
| **A shared from-table reachable via two relationships** | **Handled.** `group_key` unions every outbound relationship's `from_col`. Pinned: `a_from_side_target_of_two_relationships_unions_both_join_keys` (`endpoint_seam_feed.rs`). |

One write to an endpoint target bypasses the seam: a resumed definition's
rebuild. Its catch-up (`intake::publication::park_target_catchup_if_read`,
#507) covers it instead. The discharge refreshes every to-one projection on
the target from the rebuilt rows (`catalog::refresh_relationship_projections_in_txn`)
and enumerates the target, so reverse propagation re-derives each consumer.
Pinned: `a_resumed_targets_rebuild_reaches_a_relationship_consumer` and
`a_resumed_targets_rebuild_refreshes_a_to_one_projection`
(`target_mutation_seam.rs`).

## Known gaps

Four items fell out of filling in this table — the point of the exercise, not
a completeness failure. All four have since been closed; they are kept here,
struck through, so the table's own history stays readable:

1. ~~**No test drives an aggregate delta-path re-point across the
   *nonexistent*-parent boundary.**~~ **Closed by #173 phase 4.** The
   missing-parent and re-point handling were each well pinned, but no fixture
   combined them: an unmatched from-side row is unmatched from backfill
   onward and stays that way, so "old side resolved to a real parent, new
   side resolves to nothing" was only exercised through the *reverse* path,
   never through `accumulate_changes`'s own old/new resolution. Closed
   exactly as scoped — one `UPDATE post_tags SET post = 999` on
   `defs_aggregate_relationship.rs`'s existing fixture, pinned by
   `a_from_side_re_point_to_a_nonexistent_parent_subtracts_the_old_contribution`.
2. ~~**`from_side_keys_with_non_null_join`'s `NULL`-exclusion is argued in a
   doc comment, not asserted in a test.**~~ **Closed by #173 phase 3.** The
   exclusion is now pinned directly by
   `reverse_trigger_whole_keyspace_matches_every_non_null_row_via_from_side_keys`
   (`apply.rs`), whose fixture carries a `NULL`-keyed row specifically to
   assert it does *not* come back from `ReverseTrigger::WholeKeyspace` via
   `from_side_keys`. The residual risk the gap named — a future change relying
   on the exclusion to skip *newly*-`NULL` rows that still need clearing —
   would now fail that test rather than pass silently.
3. ~~**The composite-PK/`OneToOne`-target interaction halts rather than rejects
   when reached off the front door.**~~ **Closed by #177.** `install_definition`'s
   `require_single_column_pk` gate meant this couldn't happen through ordinary
   use, but `create_definition`/`create_definition_without_backfill` had no
   equivalent gate, so a caller skipping `install_definition` reached the
   halting `CompositePrimaryKeyUnsupported` path. Safe (no corruption) but an
   availability bug. `create_definition_inner` now runs the same check itself
   (`catalog.rs:1305`), exactly the way the aggregate replica-identity gate
   beside it (`catalog.rs:1026`) always has — both entry points reject cleanly
   and up front. The same fix incidentally closed the identical gap for
   `DdlError::UnsupportedPrimaryKeyType` (issue #107's check, bundled into the
   same `ddl::source_primary_key` call). Two residual, non-correctness notes:
   an `Aggregate` definition still runs no `source_primary_key` check at
   declare time (it needs no PK narrowing), so an aggregate over a source with
   an unsupported PK type still surfaces as an apply-time halt; and a source
   whose PK *becomes* composite/unsupported after its definition exists still
   halts, by design.

   (#163 — composite-key column ordering — is now fixed too, alongside #171's
   group-key twin of the same encoding contract; both are unrelated to #177,
   which is about entry-point enforcement, not key encoding.)
4. ~~**No test combines a `TRUNCATE` with two shared-from-table
   relationships.**~~ **Closed by #173 phase 4.** The "shared from-table"
   row's `TruncateClear` cell below was classified "Handled by reuse" on
   code-reading alone — the `truncated` loop feeds the very same
   `reverse_recomputes` accumulator the row-driven dedupe test already pins
   — but nothing had actually driven a `TRUNCATE` sentinel through it
   alongside an ordinary keyed reverse trigger sharing the same from-table.
   Pinned by
   `reverse_recompute_dedupes_a_truncate_against_a_relationship_sharing_the_same_from_table`
   (`apply_relationships.rs`), which combines a `TRUNCATE` on `comments`
   (`ReverseTrigger::WholeKeyspace`) with a fresh `likes` insert
   (`ReverseTrigger::Keys`) in the same batch, both resolving to the same
   from-side row — the dedupe held, confirming the code-reading was right.

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
