# Relationship Propagation: The Obligation Table

Trellis has six independent code paths that can turn a change into a write
against a relationship-enriched target: a forward read, two flavors of reverse
recompute, a `TRUNCATE` clear, an aggregate's incremental delta, and the direct
backfill build. Each was added at a different time, by a different issue, and
each had to independently relearn the same handful of edge-case inputs — a
`NULL` join key, a parent that doesn't exist yet, a source with the wrong
`REPLICA IDENTITY`, and so on. [Issue #173](https://github.com/salesforce-misc/trellis/issues/173)
observes that **11 of this repo's 27 `bug`-labeled issues are on the
relationship path**, and that one of them — the TRUNCATE-on-relationship-to-side
case — has been fixed and re-broken *twice*: fixed for issue #98, regressed by
epic #127's new reverse mechanism and re-filed as #165, then re-filed again as
#168 once #167 un-masked it. The pattern every time is the same: a new
propagation path is added, and it doesn't handle an edge input a *previous*
path had already learned about.

This document is that checklist, written down once, in one place, instead of
scattered across doc comments on individual functions in `apply.rs`. Rows are
the recurring edge inputs that have historically caused bugs; columns are the
propagation paths that must handle them (or provably don't need to). Each cell
names the code site that discharges the obligation and the test that pins it —
or says plainly that it doesn't, because **finding the gaps is the point of
this exercise**, not filling in a complete-looking table. See the "Known gaps"
section below for what fell out.

This is Phase 1 of #173's five-phase plan. Phase 2 (one derived
replica-identity precondition surface), Phase 3 (one shared edge-input
enumeration replacing this document's own hand-audited table with a
compiler-enforced `match`), Phase 4 (turning this table into a real test
matrix), and Phase 5 (the encoded-key contract, #103/#107/#110/#125/#163/#171)
are out of scope here.

## The six propagation paths

| Path | What it does | Entry point (verified against current `main`) |
|---|---|---|
| **Forward read** | A `OneToOne`/`Aggregate` target re-evaluates a row that reads a relationship, resolving a to-one value from the settled parent projection or a to-many value from a live join. | `build_relationship_context` (`trellis/src/staging/apply.rs:791`), called from the by-source loop in `compute` (`apply.rs:3583`); a to-one value is resolved via `fetch_relationship_projection_rows` (`apply.rs:2531`). |
| **Reverse delta (parent-keyed)** | A to-side (parent) row changes; every `Aggregate` target whose fields are all invertible (`Sum`/`Avg`/`Count`) gets a true per-row delta applied against the matching from-side rows, without re-deriving the whole group. | `build_reverse_relationship_shape` (`apply.rs:1315`), gated by `check_reverse_guards` (`apply.rs:1963`); applied via `apply_projection_advance` (`apply.rs:2386`) and `apply_aggregate::apply_delta_groups_bulk` (`apply_aggregate.rs:2636`). |
| **Reverse fallback / deferral** | Everything the delta fast path excludes: every `OneToOne` target reading a to-one relationship (1-1 has no additive semantics to delta), any `Aggregate` target with a non-invertible field, a definition reading more than one relationship, and — unconditionally, Phase 1 of epic #127 never touched this — every to-many relationship. Stages an image-less `Recompute` per affected from-side row instead. | `stage_reverse_recompute_fallback` (`apply.rs:2266`), used by `check_reverse_guards`'s rejection arms; the to-many case is a separate, always-taken inline block in the same by-source loop (`apply.rs:3745-3808`), pre-dating epic #127 and never migrated onto the delta path. |
| **TRUNCATE clear** | A source `TRUNCATE` clears every direct target (`transforms_for_source`) and, separately, reverse-recomputes every from-side row that reads the truncated table *through* a relationship, since a `TRUNCATE` stages one key-less sentinel row, not per-row CDC images. | The `truncated` loop in `compute` (`apply.rs:4341`), specifically the inbound-relationship handling at `apply.rs:4436-4478`. |
| **Aggregate incremental** | The per-batch `GROUP BY` delta engine: folds row-level contributions (including ones read through a relationship) into per-group deltas, and forces a full group recompute for anything image-less. | `accumulate_changes` (`apply_aggregate.rs:917`), forcing full recompute via `GroupPlan::force_full_recompute` (`apply_aggregate.rs:968`) and `apply_forced_groups_bulk` (`apply_aggregate.rs:1932`). |
| **Backfill** | The direct, set-based initial build at `install_definition`/`create_relationship` time — one SQL pass over existing data, no ring involvement, only for the shapes it recognizes; anything else falls back to the ring (which then goes through the other five paths as ordinary `Recompute`s). | `backfill_definition` (`trellis/src/defs/backfill.rs:191`); `backfill_relationship_one_to_one` (`backfill.rs:1418`) for a to-many aggregate reading a to-one relationship's own fields, `backfill_aggregate` (`backfill.rs:875`) for a `GROUP BY` target, `resolve_to_one_joins` (`backfill.rs:811`) for the to-one endpoints an aggregate leaf resolves through. |

Two structural notes that shape several cells below:

- **A `OneToOne` target never uses the delta fast path.** Only `Aggregate`
  targets qualify (`build_reverse_relationship_shape`'s own doc comment, design
  fork 1: a 1-1 target has no additive semantics to delta — it's just
  re-derived outright, already O(one row)). So for a bare to-one enrichment
  (`category.name AS category_name`), "Reverse delta" is always N/A and
  "Reverse fallback" is always the mechanism in play.
- **The direct backfill build never handles a bare to-one passthrough field at
  all.** `backfill_relationship_one_to_one`'s own doc comment says so
  explicitly: substituting fields and collecting aggregate leaves "bail[s] to
  the ring on any unsupported shape (a bare to-one path, a nested relationship
  reference, or a cyclic alias chain)" (`backfill.rs:1449-1452`). A definition
  with `TRANSFORM t FROM a SELECT category.name AS x` is installed, backfilled
  through the *ring* (an initial `Recompute` per source row, discharged by the
  Forward read path), and never touches `backfill.rs`'s SQL-generation code at
  all — confirmed by `install_definition_falls_back_to_ring_for_relationship_enriched_definition`
  (`trellis/tests/defs_install_definition.rs:950`). Backfill's fast path is
  real only for a to-many aggregate over a to-one relationship's own field
  (`backfill_aggregate`), not for a bare to-one value.

## The obligation table

Legend: a named function + test means **handled and pinned**. **N/A** means
this path structurally never sees this input (explained inline). **GAP** means
a real, unaddressed hole — see "Known gaps" below for the full list.

| Edge input | Forward read | Reverse delta | Reverse fallback | TRUNCATE clear | Aggregate incremental | Backfill |
|---|---|---|---|---|---|---|
| **TRUNCATE's key-less sentinel** (#98, #165, #168) | N/A — never sees a `TRUNCATE`; consumes whatever the TRUNCATE-clear column leaves behind. | N/A — a `TRUNCATE` carries no old/new image, so no `RelationshipReverseRecord` can be built (`apply.rs:3572-3579`). | Reused, not reimplemented — TRUNCATE clear feeds this path's `reverse_recomputes` accumulator directly. | **Handled.** `from_side_keys_with_non_null_join` (`apply.rs:695`) + `relationship_projection_clears` (`apply.rs:4462-4471`, the #168 fix). Pinned: `truncating_a_relationship_to_side_table_leaves_a_stale_enrichment` (`generative/tests/convergence.rs:1191`, #98/#168 regression pin). | **Handled** for a table truncated directly (own `AggregateClearPlan`, `apply.rs:4378-4388`) and for one reached *through* a relationship (forces `GroupPlan::force_full_recompute` via the reused image-less `Recompute`). Pinned: `truncating_a_relationship_to_side_table_leaves_a_stale_aggregate_enrichment` (`convergence.rs:1099`, #98's aggregate case). | N/A — backfill runs once against a live snapshot at install time; `TRUNCATE` is a live CDC event the ring, not `backfill.rs`, ever sees. |
| **A `NULL` join key** (a literally-`NULL` FK; issue #128, and ADR-0006/issue #33 for the nullability rule. The *unmatched non-`NULL`* FK is the next row down) | **Handled.** `build_relationship_context`'s join-key collection (`apply.rs:838-847`) skips `None` outright; a `NULL`/non-matching `from_col` resolves to `NULL` by left-join semantics. Pinned: `a_to_one_relationship_aggregated_inside_a_group_by_converges_end_to_end` (`convergence.rs:998`), whose from-side draw deliberately contains both a literal `None` join key and an unmatched non-`NULL` one (`"k9"`) in the same run. (`to_one_enrichment_nulls_out_when_the_related_row_appears_then_disappears`, `trellis/tests/defs_relationship_nullability.rs:213`, is often cited here but is *not* a `NULL`-FK test — its article points at a non-`NULL`, nonexistent `category_id = 10`; it pins the missing-parent row below.) | **Handled by construction** — a from-side row with a `NULL` `from_col` can never be `old_key`/`new_key` for a parent change (there's no parent to change). Pinned: `repoint_to_null_parent_converges_in_the_same_intake_window`/`_across_a_seal_boundary` (`generative/tests/relationship_interleaving.rs:254,287`). | **Handled by construction** — `from_side_keys_for_join`/`from_side_rows_for_join_txn`'s `WHERE col = ANY($1)` can never match a `NULL` column value (SQL three-valued logic). No dedicated unit test; covered incidentally whenever the generative suite draws a `NULL` FK. | **Handled**, and deliberately excludes already-`NULL` rows for efficiency, not correctness — see `from_side_keys_with_non_null_join`'s own doc comment (`apply.rs:684-694`). **No test asserts the exclusion itself** (only that the end state converges), a minor documentation-only gap. | **Handled** — `augment_row_with_forward_relationships`'s `.and_then` chain (`apply_aggregate.rs:864-870`) yields `None` for a `NULL`/unmatched `from_col`. Pinned by the same `a_to_one_relationship_aggregated_inside_a_group_by_converges_end_to_end` test above. | N/A for a bare to-one passthrough (falls back to the ring — see the structural note above; the ring case is pinned by `frontdoor_to_one_enrichment_converges_to_oracle`, which is Forward read, not Backfill). **Handled** for a to-many aggregate: `relationship_build_matches_oracle_including_no_match_and_multi_child` (`trellis/tests/defs_backfill_relationship.rs:181`) pins the empty-related-set case. |
| **A composite source primary key** (#126, #163) | **N/A in the common case, GAP-adjacent in the ring-bypass case.** A `OneToOne` target can only ever be installed against a single-column source PK (`ddl::require_single_column_pk`, `trellis/src/defs/ddl.rs:458`, enforced at `install_definition` time) — so through the front door this combination cannot occur. Reachable only by bypassing that gate (a raw `create_definition` call, skipping `install_definition`'s DDL check); when it is, `apply.rs:3952`'s narrowing surfaces a typed `DdlError::CompositePrimaryKeyUnsupported` that **halts the instance** rather than mis-deriving. Pinned (as a halt, not a silent failure): `a_composite_primary_key_source_is_never_quarantined_and_stops_the_instance` (`trellis/tests/quarantine.rs:654`). Issue #163 flags the underlying `extract_key`/`pk_key_sql_expr` ordering-convention risk as still latent (Phase 5 territory). | **Handled.** `reverse_update_of_the_to_side_row_updates_every_dependent_group` (`trellis/tests/defs_relationship_composite_pk.rs:341`) drives a to-side update through `build_reverse_relationship_shape` against a composite-PK (`post`, `tag`) from-side table. | Same code path as Reverse delta for this input — see that cell. | **N/A in the common case, same halting behavior as Forward read** for a `OneToOne` target's own source; `Aggregate` targets need no PK narrowing at all (`apply.rs:4378-4388`) and truncate-clear correctly. | **Handled.** `plain_aggregate_over_a_composite_pk_source_drains_with_no_relationship_at_all` (`defs_relationship_composite_pk.rs:398`) and `forward_insert_of_a_composite_pk_from_side_row_updates_its_group_total` (`defs_relationship_composite_pk.rs:279`). | **Handled.** `aggregate_over_a_to_one_relationship_with_composite_pk_backfills_to_the_oracle` (`defs_relationship_composite_pk.rs:237`), via `read_live_rows_batch`'s batched composite-key re-fetch. |
| **A missing/nonexistent parent row, and an FK re-point to one** (issue #138, landed via PR #169) | **Handled** — same "unmatched join key resolves to `NULL`" mechanism as the `NULL`-join-key row; a nonexistent parent and a `NULL` FK converge to the same code path (`build_relationship_context`, see above). Pinned by `to_one_enrichment_nulls_out_when_the_related_row_appears_then_disappears` (`defs_relationship_nullability.rs:213` — the full no-match → appears → disappears arc through the front door), the same `a_to_one_relationship_aggregated_inside_a_group_by_converges_end_to_end` (unmatched, non-`NULL` key `"k9"`) and `frontdoor_to_one_enrichment_converges_to_oracle` (`trellis/tests/defs_relationship_frontdoor.rs:205`, category `99` doesn't exist). | **Handled**, and this is the row #138 was actually written to stress: a parent `INSERT` turning a previously-dangling reference live. Pinned: `parent_insert_is_picked_up_by_the_reverse_path` (`trellis/tests/apply_relationship_reverse.rs:532`) and `parent_delete_is_picked_up_by_the_reverse_path` (`:590`); the full timing matrix (same intake window / across a seal boundary, repoint-to-nonexistent-parent variant) in `repoint_to_nonexistent_parent_converges_in_the_same_intake_window`/`_across_a_seal_boundary` (`relationship_interleaving.rs:249,278`). | Shares the mechanism `stage_reverse_recompute_fallback` provides for any disqualified `Aggregate`/every `OneToOne` target — a from-side row pointing at a since-appeared or since-vanished parent is enumerated by `from_side_rows_for_join_txn`'s live re-read regardless of whether the parent currently exists. No dedicated pin distinct from the Reverse-delta ones above. | N/A — a `TRUNCATE` has no per-row key to re-point; see the TRUNCATE row above. | **Handled, with real dedicated pins.** `accumulate_changes` resolves a row's relationship read *once per side* (`old_row`'s join key and `new_row`'s independently), so a re-point subtracts the old parent's contribution and adds the new one in one delta. Isolated directly — not incidentally — by `a_from_side_re_point_within_one_update_diffs_old_and_new_parent_contributions` (`trellis/tests/defs_aggregate_relationship.rs:537`, which asserts the exact delta value a single-sided resolution would get wrong, over a fixture that also holds an unmatched `post = 999` row), `updating_a_to_side_row_updates_every_dependent_group` (`:470`), `inserting_a_from_side_row_resolves_from_the_projection_not_live_parent_state` (`:389`), and `avg_over_a_relationship_read_column_maintains_through_backfill_and_forward_insert` (`:614`, which pins `post = 999`'s no-match exclusion from `AVG`). The one uncovered sliver: every unmatched row in these fixtures is unmatched *statically*, so no test drives a **delta-path re-point whose old or new side is the nonexistent parent** — see known gap 1. | N/A for a bare to-one passthrough (see structural note); **handled** for a to-many aggregate's own missing-children case via `relationship_build_matches_oracle_including_no_match_and_multi_child` (see the `NULL`-join-key row). |
| **A source lacking the `REPLICA IDENTITY` the path needs** (#41, #47, #158) | N/A at apply time — rejected earlier, at `create_relationship`, so this table never reaches Forward read in a broken state. | N/A at apply time, same reason. | N/A at apply time, same reason. | N/A — a `TRUNCATE` carries no image at all, so replica identity is irrelevant to it. | N/A at apply time — rejected by `assert_replica_identity_supports_aggregate` (`trellis/src/defs/catalog.rs:2603`), delegating to `intake::require_replica_identity_full` (`trellis/src/intake/replica_identity.rs:46`). Note this gate sits in `create_definition_inner` (`catalog.rs:1026`), *not* in `install_definition` — so unlike the composite-PK/`OneToOne` gate below, it holds on the raw `create_definition` path too. Pinned: `an_aggregate_transform_against_default_replica_identity_is_rejected` and `an_aggregate_transform_against_replica_identity_full_is_accepted` (`trellis/tests/defs_catalog.rs:1444,1498`, #47 — both call `create_definition` directly, which is what proves where the gate lives). | N/A — backfill reads a live full-table snapshot, not a CDC image, so it needs no replica identity at all; the *later* incremental drain of the same definition is what the declare-time gate protects. |
| **A shared from-table reachable via two relationships** (#79's double-count; #173's sixth enumerated input) | **Handled.** `build_relationship_context` resolves each relationship independently into its own entry, and `apply_aggregate::forward_relationship_synthetic_column` (`apply_aggregate.rs:655`) namespaces the augmented row's synthetic column by **both** relationship name and column, so two relationships reading a same-named to-side column can't overwrite each other. Pinned: `two_relationships_sharing_a_to_side_column_name_resolve_independently` (`trellis/tests/defs_aggregate_relationship.rs:745`), which is explicitly the test written because code-reading alone had been the only evidence. | N/A — "design fork 3" (`apply.rs:1372-1376`): any definition whose fields and `GROUP BY` together reference more than one *distinct* relationship sets `needs_recompute_fallback` and never reaches the delta path at all. | **Handled, and this is where #79 actually lived.** The `reverse_recomputes` accumulator is one `HashMap` keyed `(from_table, from_key)` shared across *every* inbound relationship in the batch, so two relationships resolving to the same from-side key collapse to one staged `Recompute` (`hop_gen` merged by `max`, `src_changed` by `earliest_src_changed` — deliberately opposite directions). Pinned: `reverse_recompute_dedupes_across_relationships_sharing_from_table` (`trellis/tests/apply_relationships.rs:718`, which counts staged rows *without* `distinct` precisely so a regression can't hide) and `reverse_recompute_fan_in_keeps_the_earliest_src_changed` (`:843`). | **Handled by reuse, not separately pinned.** The `truncated` loop pushes into that same deduped accumulator, and `relationship_projection_clears` is a `HashSet` of qualified table names, so N inbound relationships onto one from-table yield one recompute per from-side key and one clear per projection. No test combines a `TRUNCATE` with two relationships sharing a from-table; the dedupe is structural (the container types), which is why this is "handled by reuse" rather than a gap. | **Handled** — same synthetic-column namespacing as Forward read; the pinning test above *is* an aggregate definition, so its forward-delta half exercises this path directly. | **Handled** — #79's own repro is a `backfill_relationship_one_to_one` fast build of a 1-1 target fed by two to-many relationships, which produced correct values (the bug was the ring backlog behind it, not the build). The backfill half of `two_relationships_sharing_a_to_side_column_name_resolve_independently` covers the two-relationship build. |

The replica-identity row is the one case where "N/A" across the board is the
right answer for the *apply-time* paths specifically because a **declare-time**
gate (not part of any of the six paths above) already prevents the bad state
from occurring. That gate is exactly the three functions called out below.

## Known gaps

One real availability gap, one narrow untested transition, and one
documentation-only thinness fell out of filling in this table — genuinely the
point of the exercise, not a completeness failure of the audit:

1. **No test drives an aggregate delta-path re-point across the
   *nonexistent*-parent boundary.** The aggregate incremental path's
   missing-parent and re-point handling is itself well pinned
   (`a_from_side_re_point_within_one_update_diffs_old_and_new_parent_contributions`
   asserts the exact two-sided delta value; `avg_over_a_relationship_read_column...`
   pins an unmatched `post = 999`'s exclusion). What no test does is combine
   them: in every fixture, an unmatched from-side row is unmatched from
   backfill onward and stays that way, so the delta arithmetic for "this row's
   old side resolved to a real parent and its new side resolves to nothing"
   (or the reverse) is only exercised through the *reverse* path's
   `repoint_to_nonexistent_parent_*` tests, never through
   `accumulate_changes`'s own old/new resolution. Narrow, and the code reads
   correct (both sides go through the same `.and_then` chain that already
   yields `None` for the static case), but it is the one assertion this row is
   missing. Cheap to add: one `UPDATE post_tags SET post = 999` step on
   `defs_aggregate_relationship.rs`'s existing fixture.
2. **`from_side_keys_with_non_null_join`'s `NULL`-exclusion optimization is
   argued in a doc comment, not asserted in a test.** Nothing would break
   today if this were wrong (a `NULL`-FK row simply gets recomputed to the
   same `NULL` it already had), but a future change that made the exclusion
   *incorrect* — e.g. relying on it to also skip *newly*-`NULL` rows that
   still need clearing — would have no test to catch it.
3. **The composite-PK/`OneToOne`-target interaction is a halt, not a
   rejection, when reached off the normal front door.** `install_definition`'s
   `ddl::require_single_column_pk` gate means this can't happen through
   ordinary use, but `create_definition` (used directly by a few tests and,
   per its own doc comment, available to any future internal caller) has no
   equivalent gate — so a caller that skips `install_definition` can still
   reach the halting `CompositePrimaryKeyUnsupported` path. The instance halts
   safely rather than corrupting data, but "safely halts" is still an
   availability bug waiting for the next caller who doesn't know to route
   through `install_definition`. Note the contrast with the aggregate
   replica-identity gate, which *is* enforced in `create_definition_inner`
   (`catalog.rs:1026`) and so covers both entry points — this gate could be
   moved the same way.

   **This is not currently filed anywhere.** Issue #163 is adjacent but
   different: it is about composite-key *column ordering*
   (`intake::extract_key`'s physical-column order vs. `ddl::pk_key_sql_expr`'s
   PK-declared order agreeing only by fixture coincidence), not about which
   entry points enforce the single-column-PK precondition. Treat this bullet
   as an unfiled finding of this audit.

No cell in the table above is a *silent* correctness gap — every "N/A" is
backed by a structural reason (a declare-time gate, or an input shape that
provably can't reach that path), not an assumption. That is the table's whole
value: the next new propagation path can be checked against every row here
before it ships, instead of after.

## Related: the validation functions Phase 2 will consolidate

Three independent functions currently enforce the replica-identity row above,
one per feature, added one at a time as each issue rediscovered the
requirement:

- `intake::replica_identity::require_replica_identity_full` (`trellis/src/intake/replica_identity.rs:46`) — issue #7's original scaffolding, the shared rejection (exact `ALTER TABLE ... REPLICA IDENTITY FULL;` text) every caller below delegates to.
- `defs::catalog::assert_replica_identity_supports_aggregate` (`trellis/src/defs/catalog.rs:2603`) — issue #47, gates a `GROUP BY` definition's source at `install_definition` time.
- `defs::catalog::assert_replica_identity_supports_projection` (`trellis/src/defs/catalog.rs:2712`) — issue #129/epic #127, extended to the from-side by issue #158, gates a to-one relationship's to-side *and* from-side at `create_relationship` time.

(A fourth, `assert_replica_identity_supports_to_many` at `catalog.rs:2536`,
gates a to-many relationship's to-side — issue #41 — and is the odd one out:
it accepts a narrower covering `REPLICA IDENTITY USING INDEX`, not just `FULL`,
since a to-many join only ever needs one non-PK column's old value, not the
whole row.)

Per #173's Phase 2, these three (four) should collapse into one function that
derives the complete set of source-table guarantees a resolved plan needs from
the plan itself, rather than one hand-written assertion per feature — so that
adding a new plan shape that needs a new guarantee is a compile-or-test
failure, not a silent gap the next `#41`/`#47`/`#158` rediscovers. That
consolidation, and the encoded-key contract it sits beside (#103/#107/#110/#125/#163/#171),
are out of scope for this document; this table is what Phase 2's derivation
needs to already be correct against.
