---
status: accepted
date: 2026-09-15
deciders: Michael Ries (riesmmm@gmail.com)
consulted: 
informed:
---

# Observability Decisions

[docs/observability.md](../observability.md) laid out a first design pass for
metrics, logs/traces, and transform-status visibility (epic #49) and left a set
of questions open. This ADR settles them so issues #51–#56 build against a
fixed design; `docs/observability.md` is amended to point here. This changes
none of that doc's goals, non-goals, or its metrics-vs-logs/traces split — it
fills in the specifics.

## Decisions

### 1. Dependencies: `metrics` facade, not `prometheus` directly

**Decision:** `metrics` + `metrics-exporter-prometheus` for the metrics
registry and Prometheus text encoding; `tracing` + `tracing-opentelemetry` +
`opentelemetry-otlp` for logs/traces.

Both are pure in-process registry/encoder libraries — neither adds an HTTP
server to the core `trellis` crate. `metrics-exporter-prometheus`'s optional
Hyper-listener feature is **not** enabled, preserving `docs/observability.md`'s
"mountable handler, not a bound port" design (`render_prometheus()` stays a
plain function the operator serves from their own HTTP stack — see
`cli/src/commands/run.rs`'s `--prometheus-bind` flag).

The facade decouples recording an observation (via the `Recorder` trait,
implemented once by `metrics-exporter-prometheus`) from reading the registry
back out. That keeps any consumer — the `render_prometheus()` exposition path
(#53) or a future reader — off a second recording path and avoids hardwiring
Prometheus's concrete `Registry`/`HistogramVec` types into this crate's call
sites. The one alternative weighed was depending on the `prometheus` crate
directly (the option `docs/observability.md` listed); rejected for exactly that
hardwiring, at the cost of one extra crate in the graph.

### 2. End-to-end latency keying: terminal transform only

**Decision:** one histogram per **terminal (sink) transform**, summed across
every source feeding it — not per source→sink pair.

Keying by pair multiplies cardinality by fan-in for no benefit: "how stale is
this transform's output" is the question operators ask, answered by the
terminal transform alone. Per-source breakdowns, if ever needed, are better
served by traces (decision 3) than a permanent high-cardinality series.

### 3. Traces vs. flat logs: spans are first-class

**Decision:** adopt `tracing` spans as a first-class signal for propagation. A
change moving source → hop → hop → apply is a span tree, and
per-hop/per-transform latency is *derived* from span durations rather than
timed a second time.

Two downstream effects:

* Gates #56: a span-based instrumentation of `trellis/src`'s propagation path
  (source intake, staging, fold, apply), not flat-log-plus-metrics. There is no
  existing `log`/`tracing` usage in the crate today — greenfield, not a
  conversion.
* Clarifies #51: the per-transform latency histogram is populated from
  span/timing data captured during fold, not a second independent measurement.
  Fold already holds the origin timestamp (decision 5); a span from that origin
  to the applied output gives the histogram its `.observe()` value directly, so
  traces and the latency metric stay consistent by construction.

### 4. Transform status storage: no new field

**Decision:** transform lifecycle status stays in the existing
`transform_definitions.status` column — the `TransformStatus` enum
(`WaitingToBackfill | Backfilling | Live | Quarantined`,
`trellis/src/defs/model.rs`), persisted as text and check-constrained in
`V19__transform_status.sql`.

`docs/observability.md` asked whether lifecycle status should live alongside
[ADR-0003](0003-quarantine-storage-and-api.md)'s quarantine model or in its own
row. It's already the same field: `Quarantined` is a `TransformStatus` value
reserved for ADR-0003's whole-transform fuse, so quarantine and the backfill
lifecycle are two arcs of one state machine on one field. ADR-0003's
*column*-level tier (`column_status`/`column_failures`) is a separate,
finer-grained mechanism — a `live` transform can carry paused columns without
its `status` moving — so the two tiers don't conflict.

Issue #55 wires the `waiting_to_backfill`/`backfilling`/`live` transitions on
this field — the `xmin`-fence wait
(`trellis/src/intake/publication.rs`'s `Snapshot::settled_since`) and both
backfill-enumeration paths — plus a `quarantined → waiting_to_backfill` resume
(`staging::quarantine::resume_transform`). The whole-transform fuse-trip that
would *write* `Quarantined` is designed for but not yet implemented (follow-up,
epic #49), so `resume_transform` is correct but not yet reachable. The per-key
(`poison`) and per-column tiers have writers today.

### 5. Staging-ring metrics: drop the per-transform depth gauge, add a cheap segment-state gauge

**Decision:** no per-transform `staging_ring_depth{transform}` gauge. Two
pieces cover the need:

* **Per-transform latency histogram** (#51/#52), computed at apply completion.
  The origin timestamp already rides on every staged row — `src_changed:
  Option<SystemTime>` on `StagedChange::Cdc`/`Truncate`
  (`trellis/src/staging/append.rs`), from the `Commit` event's
  `commit_time_micros` (`trellis/src/intake/mod.rs`) — and apply already groups
  folded changes by consuming transform (`compute()`'s `by_source` map in
  `trellis/src/staging/apply.rs`). One `.observe()` per transform-group there
  is effectively free: no new I/O, no new join.
* **`staging_segments{state}` gauge** (new, system-level): a count of segments
  by `SegmentState` (`Active`/`Sealed`/`Draining`/`Drained`,
  `trellis/src/staging/state.rs` and the `segments` table), read on-demand from
  segment metadata, not incremented on the hot path.

The rejected gauge needed hot-path bookkeeping (increment on stage, decrement
on fold) — a throughput risk for a "supporting" series. It's also the wrong
shape: the ring partitions by source-table/key-hash bucket
(`trellis/migrations/V3__staging_ring.sql`), not by transform, so a live
per-transform depth would need a join the layout doesn't offer.
`staging_segments{state}` gives the same "is the ring backing up" signal at
near-zero cost.

**Throughput counter unit (#409):** `trellis_changes_applied_total{transform}`
counts staged ring rows a transform folded and applied, not folded changes. The
fold collapses a fenced window's rows to one change per `(src_table, key)`, so
counting folded changes measured neither transactions nor rows. It measured a
batching artifact: the same 30 inserts could report 30, 3 or 1 depending on
when a segment sealed. A staged row matches the operator's model (rows written
to the source, comparable to the application's write rate and to
`pg_stat_user_tables`). It also composes across hops with no extra plumbing,
because a propagated change is itself a ring row: each target writer reports
every physically changed key into `TargetMutations`, which stages one row per
key for the next hop. The fold carries each group's `count(*)` as
`FoldedChange::row_count`, and a truncate sentinel counts as one row. The
latency histograms still observe once per folded change, because a folded
change carries one origin timestamp and nobody asks for a row-weighted
quantile. The counter is therefore no longer the histograms' throughput
denominator. The histograms' own `_count` series is, minus bare recompute
triggers, which carry no origin timestamp and aren't observed.

### 6. Histogram bucket boundaries

**Decision:** exponential buckets, ~10ms–60s (matching the stated
"sub-second to tens-of-seconds propagation range"), one global default for
every histogram:

```
[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 60.0]  // seconds
```

Not per-transform configurable in this pass — a single default keeps #51/#52
simple and every histogram comparable. Per-transform boundaries are a targeted
follow-up once real data justifies them, not a speculative knob.

### 7. Metric retention: left to Prometheus, not Trellis

**Decision:** Trellis retains no metric history. Retention, rollup, and
cross-instance aggregation are what an operator's Prometheus/VictoriaMetrics/
Thanos stack already does; duplicating a pruned history table inside Trellis
would add write load, a schema object, and a prune job for no gap Trellis needs
to fill. Operators configure their scraper's retention against
`render_prometheus()` like any other target.

A pruned Postgres rollup table (`trellis.metric_rollup`, #54) was built
(`V25__metric_rollup.sql`, `trellis::rollup`) then removed in favor of this —
see `docs/observability.md`'s
["Retention"](../observability.md#retention-left-to-prometheus-not-trellis).

## Related

* [ADR-0003](0003-quarantine-storage-and-api.md) — quarantine/fuse model that
  decision 4 reuses `transform_definitions.status` alongside.
* [docs/observability.md](../observability.md) — the design doc this ADR
  settles, amended in place to link back here.
* Issues #49 (epic), #50 (this ADR), #51/#52 (per-transform and end-to-end
  latency), #53 (Prometheus exposition), #54 (rollup job), #55 (backfill status
  wiring), #56 (span-based tracing instrumentation).
