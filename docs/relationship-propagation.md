# Relationship Propagation: The Obligation Table

Six code paths can turn a change into a write against a relationship-enriched
target, and a seventh feeds them when an endpoint is one of the instance's own
targets. Each has to handle the same handful of edge inputs: a `NULL` join key,
a parent that doesn't exist yet, a composite primary key, a `TRUNCATE`. A path
that forgets one leaves a target stale. This document is the checklist: rows
are the edge inputs, columns are the paths, and each cell names the code that
discharges the obligation and the test that pins it, or says why the path
never sees the input.

`generative/tests/relationship_interleaving.rs`'s `obligation_matrix` module is
this same table as code: a closed `Path` enum, a closed `EdgeInput` enum and an
exhaustive `cell()` match with no wildcard arm, so an unclassified cell is a
compiler error. Every handled cell there names the tests that pin it, and every
not-applicable cell names the structural reason. The from-side enumeration has
the same shape: every site builds a `ReverseTrigger` (`Keys` or
`WholeKeyspace`) and dispatches off an exhaustive `match`, so a new variant is a
compile error at both match sites (`from_side_keys`,
`from_side_rows_for_trigger_txn`) rather than a case a new path can skip.

The ways a user or operator can still leave a relationship-fed target stale are
listed in [known correctness gaps](known-correctness-gaps.md); this table is
about the engine's own paths. The encoded-key contract (how keys are rendered
and compared) is out of scope here; see [type support](type-support.md).

## The propagation paths

| Path | What it does | Where |
|---|---|---|
| **Forward read** | A `OneToOne`/`Aggregate` target re-evaluates a row that reads a relationship, resolving a to-one value from the settled parent projection or a to-many value from a live join. | `build_relationship_context`, called from the by-source loop in `compute`; the to-one value comes from `fetch_relationship_projection_rows`. A column resume's field build over a relationship-enriched 1-1 target (`DirectRederive`) runs the same reads (`RelationshipReads`) in its chunk's transaction, after the entry lock (#832). |
| **Reverse delta (parent-keyed)** | A to-side (parent) row changes. The guards (watermark, generation, ordering, in-flight) order the change against the parent's settled projection, and the projection advances. The from-side rows then take the image-less `Recompute` the fallback stages: a 1-1 target re-reads the row, and an aggregate re-derives its ledger entry, reading the parent live. No target applies a per-row delta from a parent change. | `build_reverse_relationship_shape`, gated by `check_reverse_guards`; the projection advance is `apply_projection_advance`. The from-side enumeration is a `ReverseTrigger::Keys` dispatch through `from_side_rows_for_trigger_txn`. |
| **Reverse fallback / deferral** | Every target reading a to-one relationship, a definition reading more than one relationship, and every to-many relationship. Stages an image-less `Recompute` per affected from-side row. | `stage_reverse_recompute_fallback`, from `check_reverse_guards`'s rejection arms; the to-many case is a separate inline block in `compute`. Both enumerate from-side rows through the `ReverseTrigger`-driven functions. |
| **TRUNCATE clear** | A source `TRUNCATE` clears every direct target and re-derives every from-side row reading the truncated table *through* a relationship. A `TRUNCATE` stages one key-less sentinel, not per-row images. | The `truncated` loop in `compute`, including its inbound-relationship handling. The from-side enumeration is a `ReverseTrigger::WholeKeyspace` dispatch through `from_side_keys`. |
| **Aggregate incremental** | The per-key ledger: every `Aggregate` target keeps one ledger entry per source key, recording its group, its contributions and a basis snapshot. An Apply folds a change's delta into its entry and the group's increments. A Recompute re-derives the entry outright, reading any to-one relationship's parent live in the same statement as the child's read. Fields increments can't maintain (`MIN`/`MAX`, `BOOL_AND`/`BOOL_OR`, a float `SUM`/`AVG`) are rewritten from the group's live entries after the upsert. | `route`/`route_definition` in `staging::ledger` decide which targets land here; `apply_ledger_target` applies one page; `recompute_written` and `finish_groups` handle the recomputed fields and empty-group deletion. |
| **Build** | How the target is first filled. A plain aggregate or plain 1-1 definition on a captured table takes the Re-derive build, which has no relationship in any field. A relationship-enriched 1-1 definition, and an aggregate reading a relationship, take the direct set-based build (one `INSERT … SELECT` pass run as a background job), or the ring enumeration when the direct build can't render the shape. See [data flow](data-flow.md#capturing-a-tables-existing-rows). | `defs::backfill`: `check_direct_build` decides, `backfill_relationship_one_to_one` builds a 1-1 target whose fields aggregate a to-many relationship, `backfill_aggregate` a `GROUP BY` target, `resolve_to_one_joins` the to-one endpoints an aggregate leaf resolves through. |
| **Seam-fed endpoint** | A relationship endpoint that is one of this instance's own targets. Triggers never capture it: the target-mutation seam stages each write to it as a CDC-shaped ring row (prior and new image, a write token as `lsn`, `group_key` for a from-side), so every path above consumes it as it would a source's CDC. | `staging::target_mutations` (`into_staged`, `read_new_images`). |

Two structural facts shape several cells:

- **A parent change always falls through to an image-less `Recompute`.** A
  1-1 target has no additive semantics, so it is re-derived outright, and an
  aggregate re-derives its ledger entry.
- **The direct build never handles a bare to-one passthrough field.**
  `backfill_relationship_one_to_one` returns `BackfillError::Unsupported` on any
  shape it can't render (a bare to-one path, a nested relationship, a cyclic
  alias chain), and the discharge checks that first (`check_direct_build`).
  `TRANSFORM t FROM a SELECT category.name AS x` is built through the ring (a
  `Recompute` per source row, discharged by Forward read) and never touches
  `backfill.rs`, as
  `install_definition_falls_back_to_ring_for_relationship_enriched_definition`
  (`defs_install_definition.rs`) pins. The direct build is real for a 1-1 target
  aggregating a to-many relationship, not for a bare to-one value.

## Endpoint requirements

A relationship `CREATE RELATIONSHIP` accepts must never halt its definitions later
over something that was knowable when it was declared (issue #429). So every
requirement the paths above place on a relationship's two endpoint tables and
its join columns is checked at create time, in one place:
`defs::catalog::validate_relationship`, which runs before any catalog write.
Each check calls the same function, or reads the same allowlist, as the path
that would otherwise fail, so the create-time gate and the runtime can't drift
apart. Each rejection names the side (from or to), the table and, where there
is one, the column.

| Requirement | Applies to | Why (the path that needs it) | Check |
|---|---|---|---|
| Both join columns exist | from, to | Every path reads them. | `join_column_in_txn` |
| Each join column's type is on the join-key allowlist | from, to | The engine matches join keys as text; see [type-support.md](type-support.md)'s join-key role. | `assert_join_key_type_supported` |
| The join columns have the same type, type modifier and collation | the pair | Trellis never casts a join key (#590). The key lookups cast one side's keys to the other side's type, so an `integer`/`bigint` pair raises 22003 on a key above 2^31, a modifier mismatch (`varchar(50)`/`varchar(255)`, `timestamp(3)`/`timestamp(6)`) truncates or rounds a key onto another row, and two different non-default collations make the oracle's and backfill's `a.x = b.y` raise 42P22. | `assert_joinable_as_is` |
| A join column's collation, if any, is deterministic | the pair | A nondeterministic collation's `=` matches strings that differ (by case, say), while the engine matches join keys by their exact text (#590). | `assert_deterministic_join_collation` |
| Capture can key the endpoint's changes: it is a plain table with a primary key, outside any partition or inheritance hierarchy | from, to; not this instance's own targets | An endpoint the instance doesn't own is captured by triggers, which key each change by its primary key (#375). | `reject_unkeyed_relationship_endpoint` → `catalog::change_keyed` |
| The endpoint's key (its primary key, or the unique index standing in for one) has only types on the key allowlist | from, to; own targets included | Apply keys every change the relationship propagates by it: the to-side's own staged changes (`apply::compute`'s per-source lookup and its `TRUNCATE` loop), and the from-side rows a to-side change re-derives (`accumulate_from_side_recomputes`, `build_reverse_relationship_shape`, the `TRUNCATE` loop's from-side walk). That lookup rejects an unsupported type, and the rejection halts every definition reading the endpoint, and everything downstream of them (#429, #663). An aggregate target's key is its `GROUP BY` columns, so owning the endpoint doesn't exempt it. | `ddl::source_primary_key_in_txn`, the runtime's own lookup |
| The endpoint's key columns have deterministic collations | from, to; own targets included | The same key, matched by its exact text: a nondeterministic collation's `=` and unique index treat strings that differ (by case, say) as one key (#638). An own target's key takes the database default, which is always deterministic. | `assert_deterministic_key_collation_in_txn` |
| An endpoint that is one of this instance's targets is `live` | from, to | The seam is such an endpoint's only change feed, and a build's writes land outside it (#403). | `reject_non_live_upstream` |
| The name is unique on the qualified from-table | from | A relationship's identity is `(from_schema, from_table, name)` (#288). | inline query |
| The new edge closes no table cycle | the pair | Propagation must terminate. | `reject_if_table_cycle` |

A join column with no usable index on the from-side draws a warning
(`RelationshipWarning::MissingFkIndex`), not a rejection. Columns a transform
reads *through* a relationship (`rel.column`) are checked when that transform
is defined, not here.

Out of scope: the source schema changing after the relationship is created,
such as a key's type changing or its primary key being dropped. The source
schema belongs to the user
([ADR-0005](decisions/0005-source-schema-is-user-owned.md)), and such drift
can still halt, at apply time, the definitions that read the table and those
downstream of them: each is left `paused` with a `halt` capture failure until
resumed (#663). Drift Trellis doesn't detect is in
[known correctness gaps](known-correctness-gaps.md).

## The obligation table

Legend: a function + test means **handled and pinned**. **N/A** means this path
structurally never sees this input (explained inline). Gaps that need an
operator are in [known correctness gaps](known-correctness-gaps.md).

| Edge input | Forward read | Reverse delta | Reverse fallback | TRUNCATE clear | Aggregate incremental | Backfill |
|---|---|---|---|---|---|---|
| **TRUNCATE's key-less sentinel** (#98, #165, #168) | N/A — never sees a `TRUNCATE`; consumes what TRUNCATE-clear leaves behind. | N/A — a `TRUNCATE` carries no image, so no `RelationshipReverseRecord` can be built (the both-images-absent skip, the code); the type says so too: `from_side_rows_for_trigger_txn`'s `ReverseTrigger::WholeKeyspace` arm returns a typed `ApplyError::ReverseTriggerNotResolvable` for exactly this reason, a typed error rather than a panic per that module's convention for invariants a function cannot enforce against a future caller. Pinned: `reverse_trigger_whole_keyspace_is_a_typed_error_not_a_panic_in_the_txn_lookup`. | Reused — TRUNCATE clear feeds this path's `reverse_recomputes` accumulator directly. | **Handled.** `ReverseTrigger::WholeKeyspace` via `from_side_keys` + `relationship_projection_clears`. Pinned: `truncating_a_relationship_to_side_table_leaves_a_stale_enrichment` (`relationship_interleaving.rs`, alongside its DELETE control `deleting_a_relationship_to_side_row_does_clear_the_enrichment`). | **Handled** for a table truncated directly (`AggregateClearPlan`) and reached *through* a relationship: the reused image-less `Recompute` re-derives the row's ledger entry, which reads the (now-gone) parent live and gets `NULL` by the same left join every other `Recompute` uses. Pinned: `truncating_a_relationship_to_side_table_leaves_a_stale_aggregate_enrichment` (`relationship_interleaving.rs`). | N/A — backfill runs once against a live snapshot; `TRUNCATE` is a live CDC event only the ring sees. |
| **A `NULL` join key** (a literally-`NULL` FK; #128, ADR-0006/#33 for the nullability rule) | **Handled.** `build_relationship_context`'s join-key collection skips `None`; a `NULL`/non-matching `from_col` resolves to `NULL` by left-join. Pinned: `a_to_one_relationship_aggregated_inside_a_group_by_converges_end_to_end`, whose from-side draw holds both a literal `None` key and an unmatched non-`NULL` `"k9"`. | **Handled by construction** — a `NULL` `from_col` can never be a parent-change key (there's no parent). Pinned: `repoint_to_null_parent_converges_back_to_back`/`_across_a_seal_boundary`. | **Handled by construction** — `WHERE col = ANY($1)` can never match a `NULL` (SQL three-valued logic). Covered incidentally whenever the generative suite draws a `NULL` FK. | **Handled**; deliberately excludes already-`NULL` rows for efficiency (`ReverseTrigger::WholeKeyspace`'s doc comment, the code). Pinned: `reverse_trigger_whole_keyspace_matches_every_non_null_row_via_from_side_keys` (gap 2, now closed). | **Handled by construction** — `route`'s left join to the relationship's to-side yields `NULL` for a `NULL`/unmatched `from_col`, same as any left join. Pinned by the same `converges_end_to_end` test. | N/A for a bare to-one passthrough (ring; pinned by `frontdoor_to_one_enrichment_converges_to_oracle`, a Forward-read test). **Handled** for a to-many aggregate: `relationship_build_matches_oracle_including_no_match_and_multi_child`. |
| **A composite source primary key** (#126, #163) | **Handled.** A `OneToOne` target's primary key mirrors the source's in full, at whatever arity. A relationship's join columns (`from_col`, `to_col`) are single columns, but an endpoint table's primary key may be composite: `CREATE RELATIONSHIP` checks every one of its key columns against the key allowlist and requires a deterministic collation. A capture trigger keys a row by its primary key in declared order, the order `pk_key_sql_expr` and every consumer use. A source whose key later becomes composite or unsupported is paused by the capture pass or the drain's halt, not applied. Pinned: `a_one_to_one_transform_against_a_composite_primary_key_source_is_accepted` (`defs_catalog.rs`), `a_composite_primary_key_source_drains_cleanly_with_no_quarantine_or_halt` (`quarantine.rs`). | **Handled.** `reverse_update_of_the_to_side_row_updates_every_dependent_group` drives a to-side update against a composite-PK (`post`, `tag`) from-side. | Same code path as Reverse delta. | **Handled.** `Aggregate` targets need no key narrowing and truncate-clear correctly. | **Handled.** `plain_aggregate_over_a_composite_pk_source_drains_with_no_relationship_at_all` and `forward_insert_of_a_composite_pk_from_side_row_updates_its_group_total`. | **Handled.** `aggregate_over_a_to_one_relationship_with_composite_pk_backfills_to_the_oracle`, via `read_live_rows_batch`'s batched composite-key re-fetch. |
| **A missing/nonexistent parent, and an FK re-point to one** (#138, PR #169) | **Handled** — same "unmatched key resolves to `NULL`" mechanism as the `NULL`-key row. Pinned by `to_one_enrichment_nulls_out_when_the_related_row_appears_then_disappears` (`defs_relationship_nullability.rs`, the full no-match → appears → disappears arc), the `converges_end_to_end` unmatched `"k9"`, and `frontdoor_to_one_enrichment_converges_to_oracle` (`defs_relationship_frontdoor.rs`, category `99` missing). | **Handled** — the row #138 was written to stress: a parent `INSERT` turning a dangling reference live. Pinned: `parent_insert_is_picked_up_by_the_reverse_path`, `parent_delete_is_picked_up_by_the_reverse_path` , and the timing matrix in `repoint_to_nonexistent_parent_converges_back_to_back`/`_across_a_seal_boundary`. | Shares `stage_reverse_recompute_fallback`; a from-side row pointing at a since-appeared/vanished parent is enumerated by `from_side_rows_for_trigger_txn`'s live re-read regardless. No pin distinct from the Reverse-delta ones. | N/A — a `TRUNCATE` has no per-row key to re-point. | **Handled, dedicated pins.** On the ledger, a from-side change is an Apply that writes the entry from its new image (new group only) plus, where the parent change itself reaches other children, a `Recompute` of each — there is no "old parent" to resolve, since the entry's own prior group is what the upsert subtracts from. Pinned by `a_from_side_re_point_within_one_update_diffs_old_and_new_parent_contributions`, `updating_a_to_side_row_updates_every_dependent_group` , `inserting_a_from_side_row_resolves_from_the_projection_not_live_parent_state` , `avg_over_a_relationship_read_column_maintains_through_backfill_and_forward_insert` . Also `a_from_side_re_point_to_a_nonexistent_parent_subtracts_the_old_contribution` (`defs_aggregate_relationship.rs`). | N/A for a bare to-one passthrough; **handled** for a to-many aggregate's missing-children case via `relationship_build_matches_oracle_including_no_match_and_multi_child` (see the `NULL`-key row). |
| **A parent value the fold erases** (#784): a parent born and deleted inside one batch, or re-keyed through a join value and on, or deferred reverses of one key that fold the same way | Not this case: a 1-1 target reads the settled projection, which only reverse records advance, not the live parent. | N/A: no record names the erased value. | **Handled.** A `to_col` that is the to-side's whole primary key has one value per ring key, the key itself. For any other `to_col`, the fold reads its values out of every raw row's new image, labelled by column (`FoldedChange::to_col_values`, #785), which survives the fold like `group_key` does. A deferred reverse carries its own old and new keys. `compute` re-derives the from-side rows of every touched value neither folded image names (`touched_join_values`), including a deferred retry that folds to no image, and a change the drain parks because its key is poisoned, whose children are re-derived when it parks rather than left stale while the key is held. Pinned: `a_parent_born_and_deleted_in_one_batch_after_a_child_read_it_*`, `a_parent_born_and_deleted_across_deferred_reverses_*`, `a_parent_rekeyed_through_a_childs_key_inside_one_batch`, `a_parent_that_is_also_a_from_side_*`, `a_to_sides_stale_group_key_of_another_type_still_drains`, `a_parent_born_and_deleted_on_a_to_side_joined_by_two_non_key_columns`, `a_parent_born_and_deleted_on_a_to_side_that_is_also_a_from_side`, `a_parent_rekeyed_through_a_childs_key_on_a_to_side_joined_by_two_non_key_columns`, `a_parked_parent_born_and_deleted_on_a_non_key_to_col` (`ledger_interleavings.rs`); the fold's half by `to_col_values_union_every_raw_rows_new_image_by_column` (`fold.rs`). A nested statement that re-keys the value again is a [known gap](known-correctness-gaps.md#9-an-application-trigger-re-keying-a-parents-join-column-within-the-statement). | N/A | **The case that needs it:** a ledger write reads the parent live, so a child can count a parent value no reverse record names. | N/A |
| **A shared from-table reachable via two relationships** (#79's double-count) | **Handled.** `build_relationship_context` resolves each relationship independently, keyed by relationship name, so two relationships reading a same-named to-side column can't collide by construction. The ledger's own left-join construction (`route`, the code) namespaces each relationship's join by name too (`join_alias`, the code). Pinned: `two_relationships_sharing_a_to_side_column_name_resolve_independently`. | N/A — no target applies a parent-change delta, so this edge input doesn't distinguish this path from Reverse fallback. | **Handled — where #79 actually lived.** The `reverse_recomputes` accumulator is one `HashMap` keyed `(from_table, from_key)` shared across every inbound relationship, so two relationships resolving to the same key collapse to one `Recompute` (`hop_gen` merged by `max`, `src_changed` by earliest). Pinned: `reverse_recompute_dedupes_across_relationships_sharing_from_table` (`apply_relationships.rs`, counts staged rows *without* `distinct`) and `reverse_recompute_fan_in_keeps_the_earliest_src_changed` . | **Handled.** The `truncated` loop pushes into that same deduped accumulator; `relationship_projection_clears` is a `HashSet` of qualified table names. Pinned: `reverse_recompute_dedupes_a_truncate_against_a_relationship_sharing_the_same_from_table` (`apply_relationships.rs`). | **Handled** — same name-keyed namespacing as Forward read; the pinning test above is an aggregate definition. | **Handled** — #79's repro is a `backfill_relationship_one_to_one` build of a 1-1 target fed by two to-many relationships, which built correctly (the bug was the ring backlog behind it). Covered by the backfill half of the two-relationship test above. |

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
| **A shared from-table reachable via two relationships** | **Handled.** `group_key` unions every outbound relationship's `from_col`. Pinned: `a_from_side_target_of_two_relationships_unions_both_join_keys` (`endpoint_seam_feed.rs`). |

One write to an endpoint target bypasses the seam: a resumed definition's
rebuild. Its catch-up (`intake::markers::park_target_catchup_if_read`,
#507) covers it instead. The discharge refreshes every to-one projection on
the target from the rebuilt rows (`catalog::refresh_relationship_projections_in_txn`)
and enumerates the target, so reverse propagation re-derives each consumer.
A seam row staged before the rebuild and drained after the refresh carries a
pre-rebuild image, so apply checks a target to-side's reverse records against
the live row and follows the live row when they disagree
(`apply::to_side_superseded`).
Pinned: `a_resumed_targets_rebuild_reaches_a_relationship_consumer`,
`a_resumed_targets_rebuild_refreshes_a_to_one_projection`, the four
`stale_seam_rows_*` tests and `only_a_rewrites_catch_up_refreshes_the_projection`
(`target_mutation_seam.rs`).

No cell above is a gap in the engine's own paths: every N/A is backed by a
structural reason (a declare-time gate, or an input shape that provably can't
reach that path). The known exceptions, which need an operator, are in
[known correctness gaps](known-correctness-gaps.md). Check a new path against
every row here before it ships.
