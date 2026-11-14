# Architecture: module map

Which module owns which concept and catalog table. How a change flows is
[data-flow](data-flow.md); the machinery, stage by stage, is
[staging-and-claiming](staging-and-claiming/README.md); the words are in the
[glossary](glossary.md). The crate's tiers and public API are in
[ADR-0012](decisions/0012-curate-public-api-demote-engine-modules.md).

The map names modules and tables only, so it doesn't rot; each module's doc
comment has the rest. `check-module-map.py` fails CI when a file of
`trellis/src/staging` or `trellis/src/defs` is missing here, or a `staging::` or
`defs::` name here is not a module. A table column lists the catalog tables the
module mainly writes. Hidden per-definition tables are in the
[glossary](glossary.md#hidden-tables).

## The crate

| Module | Concept |
|---|---|
| `defs` | Parse, validate and catalog definitions; pause and drop. |
| `capture` | Capture triggers and the reconcile pass. Tables: `capture_triggers`, `capture_gates`, `capture_baselines`, `capture_holdups`, `partition_sources`, `partition_corrections`. |
| `intake` | The rebuild of a table's readers (`park_table_catch_ups`). |
| `staging` | The ring, sealing, claiming, apply, builds, failures, `self_check`. |
| `app`, `blocking`, `client`, `deadline` | The `Trellis` facade, its blocking wrapper, and the `Client` runtime with the maintenance loop. |
| `config`, `pool`, `tls`, `migrate`, `identity`, `instance_log` | Connections, migrations, instance identity. Tables: `trellis_instance`, `catalog_floor`. |
| `handover`, `catalog_floor`, `release_version`, `locks` | Version handover, the catalog floor's checks, lock discipline. |
| `dump` | Definitions as SQL for a schema dump. |
| `metrics`, `otel`, `error`, `error_code` | Observability and errors. |
| `plant`, `dev` | Planted bugs and the dev door for the generative suite. |
| `float`, `integer`, `jsonb`, `netaddr`, `temporal` | Per-family type rules shared by define and render. |

## `defs`

| Module | Concept | Tables |
|---|---|---|
| `defs::lexer`, `defs::parser`, `defs::ast`, `defs::error` | The statement and expression grammar. | |
| `defs::grammar`, `defs::registry`, `defs::typed_literal`, `defs::case_hint` | Grammar versions, the function and operator registry, typed literals, the case hint. | |
| `defs::validate`, `defs::validate_join_one_to_one` | Define-time validation of aggregates, 1-1 and `JOIN` definitions. | |
| `defs::oracle`, `defs::alias_readers`, `defs::backfill` | Rendering fields to SQL, the fields a field reads, field inlining. | |
| `defs::model`, `defs::catalog` | The stored definition and the catalog that holds it. | `transform_definitions`, `schema_nodes`, `schema_edges`, `definition_join_tables`, `source_table_versions` |
| `defs::lifecycle` | `PAUSE` and `DROP`, and what a drop removes. | |
| `defs::ddl`, `defs::adopt` | The target table's DDL, and `ADOPT EXISTING TABLE`. | |
| `defs::ledger` | The 1-1 ledger's DDL, `<target>__ledger`. | |
| `defs::join`, `defs::join_one_to_one` | The tables Trellis keeps for a `JOIN` and a 1-1 `JOIN`. | |
| `defs::chunk_queue` | The build's claimable chunk queue and the claim fence. | `backfill_chunks` |
| `defs::pg_type`, `defs::copy_type`, `defs::copies` | Type identity and the columns Trellis creates with a source type. | |
| `defs::join_key_types`, `defs::key_types` | The join-key type allowlist, and key re-validation after define. | `definition_key_types`, `definition_types`, `definition_enum_values` |
| `defs::source_key`, `defs::recorded_keys`, `defs::source_index` | The key a table is read by, its record, the source index a `GROUP BY` needs. | `definition_keys` |
| `defs::live_table`, `defs::hierarchy`, `defs::row_security`, `defs::subscription`, `defs::partition_tree` | Checks of a source table: its columns, hierarchy, row-level security, subscription, partition tree. | |
| `defs::floor`, `defs::floor_manifest`, `defs::numeric_sums` | Definition floors, the feature-floor manifest, numeric partials for older definitions. | |
| `defs::refusal_tables` | The test that every refusal is in a [refusal table](transforms.md#refusals). | |
| `defs::arithmetic_tests`, `defs::date_tests`, `defs::identifier_tests`, `defs::literal_tests`, `defs::logic_tests`, `defs::table_alias_tests`, `defs::table_key_tests` | Grammar tests by feature. | |

## `staging`

[Stage 02](staging-and-claiming/02-the-staging-ring.md) and
[03](staging-and-claiming/03-sealing-and-the-fence.md): the ring and its batches.

| Module | Concept | Tables |
|---|---|---|
| `staging::append` | The blind append every Rust-side producer uses. | |
| `staging::session` | The one staging worker per instance. | |
| `staging::state`, `staging::seal` | The segment state graph; sealing and the seal fence. | `segment_pointer`, `segments` |
| `staging::ring_gauges` | The ring's size and age gauges. | |

[Stage 04](staging-and-claiming/04-claiming-and-the-fold.md): claims and the fold.

| Module | Concept | Tables |
|---|---|---|
| `staging::claim` | Buckets, shares and the claim. | `seg_claims`, `drainers` |
| `staging::liveness` | Heartbeats, reclaim, the fence-miss backoff. | |
| `staging::fold` | The claim-time fold. | |
| `staging::page`, `staging::keyset` | Paging a share; matching keys through the key index. | `drain_cursor` |

[Stage 05](staging-and-claiming/05-apply-and-exactly-once-deltas.md): apply.

| Module | Concept | Tables |
|---|---|---|
| `staging::apply` | The drain: claim, fold, compute, apply and mark drained. | |
| `staging::one_to_one_ledger`, `staging::one_to_one_render`, `staging::ledger` | 1-1 apply: the ledger, the fields as SQL, statements they share. | `ledger_truncate_floor` |
| `staging::group_basis` | Aggregate apply on the [group basis](staging-and-claiming/group-basis.md). | |
| `staging::join`, `staging::join_one_to_one` | `JOIN` and 1-1 `JOIN` apply. | `join1_fan_out` |
| `staging::target_mutations` | The seam: every write to a target, fed to its readers. | |

[Builds](staging-and-claiming/builds.md) and corrections.

| Module | Concept | Tables |
|---|---|---|
| `staging::build`, `staging::build::one_to_one`, `staging::build::join_one_to_one`, `staging::build::orphans` | The plan job, chunks and structure writes; the build fence. | |
| `staging::join_one_to_one::build`, `staging::join_one_to_one::fan_out`, `staging::join_one_to_one::held` | A 1-1 `JOIN`'s build, `b`-change fan-out, and held rows. | |
| `staging::partition_correction`, `staging::partition_correction::group` | The correction engine for a partitioned source. | `partition_correction_progress` |

[Stage 06](staging-and-claiming/06-cleanup-and-reclaim.md) and
[07](staging-and-claiming/07-convergence-and-await.md): cleanup, failures, `await`.

| Module | Concept | Tables |
|---|---|---|
| `staging::retire` | Retiring drained segments; tombstone and zero-group collection. | |
| `staging::quarantine` | Failure classes, key and column fuses, eviction, parking, release. | `poison`, `poison_held`, `key_deaths`, `column_status`, `column_failures`, `column_deaths`, `column_pause_cascades`, `transform_fuse_gate`, `upstream_releases`, `retype_releases`, `resume_requests`, `halting_stops`, `capture_failures`, `retype_causes` |
| `staging::halt` | What a halting failure pauses. | |
| `staging::holdup` | Drain holdups. | `drain_holdups` |
| `staging::schema_change`, `staging::drift_memo`, `staging::pause_message` | Pausing readers of a schema change; the pass's drift memo; the pause message. | `capture_failure_reasons` |
| `staging::converge` | The read-your-writes predicate. | |
| `staging::worker_registry` | The live-worker registry. | `worker_registry` |

`self_check` ([ADR-0013](decisions/0013-self-check-production-recompute-audit.md)) and status.

| Module | Concept | Tables |
|---|---|---|
| `staging::self_check`, `staging::self_check_job` | The audit and its background job. | `self_check_jobs` |
| `staging::self_check_groups`, `staging::self_check_joins`, `staging::self_check_join_one_to_one` | The audit of each target shape. | |
| `staging::self_check_keyset`, `staging::self_check_ring`, `staging::capture_audit` | Keyset paging, the unapplied ring rows, the capture audit. | |
| `staging::status_gauges`, `staging::join_status`, `staging::join_one_to_one_status` | Status and liveness gauges. | |
| `staging::error` | The module's error type. | |
| `staging::golden_sql`, `staging::keyset_golden`, `staging::interleave` | Test support: golden SQL and pause points. | |
