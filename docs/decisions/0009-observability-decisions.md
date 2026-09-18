---
status: accepted
date: 2026-09-15
deciders: Michael Ries (riesmmm@gmail.com)
consulted: 
informed:
---

# Observability Decisions

[docs/observability.md](../observability.md) laid out a first design pass for
metrics, logs/traces, and transform-status visibility (epic #49), and
collected a set of open questions it deliberately left unresolved. This ADR
settles those questions so the follow-on implementation issues (#51-#56) have
a fixed design to build against, and amends `docs/observability.md` in place
to mark them settled and point back here.

## Summary

The decisions settled here, each discussed below with its rationale:

1. Dependencies: `metrics` + `metrics-exporter-prometheus`, and `tracing` +
   `tracing-opentelemetry` + `opentelemetry-otlp`.
2. End-to-end latency is keyed by terminal transform only.
3. Traces/spans are adopted as a first-class signal; per-hop and per-transform
   latency derive from span durations rather than a separate measurement.
4. Transform status reuses the existing `transform_definitions.status` field;
   no new schema.
5. The staging ring gets a cheap `staging_segments{state}` gauge rather than a
   per-transform `staging_ring_depth{transform}` gauge.
6. Histogram buckets are exponential, ~10ms-60s, one global default for every
   histogram.
7. Trellis retains no metric history of its own; retention, rollup, and
   cross-instance aggregation are left to the operator's external Prometheus/
   TSDB stack.

None of this changes
`docs/observability.md`'s goals/non-goals or its two-subsystem split
(metrics vs. logs/traces) — it fills in the specifics that section left
undecided.

## Decisions

### 1. Dependencies: `metrics` facade, not `prometheus` directly

**Decision:** `metrics` + `metrics-exporter-prometheus` for the in-process
metrics registry and Prometheus text encoding; `tracing` +
`tracing-opentelemetry` + `opentelemetry-otlp` for logs/traces.

Both are pure in-process registry/encoder libraries — neither adds an HTTP
server to the core `trellis` crate.  `metrics-exporter-prometheus` has an
optional Cargo feature that bundles a Hyper listener; that feature is **not**
enabled here, preserving `docs/observability.md`'s "mountable handler, not a
bound port" design (`render_prometheus()` stays a plain function the operator
serves from their own HTTP stack — see `cli/src/commands/run.rs`'s
`--prometheus-bind` flag for a worked example).

The facade was chosen over depending on the `prometheus` crate directly for
the decoupling it provides: `metrics` separates "recording an observation"
(via its `Recorder` trait, implemented once by `metrics-exporter-prometheus`)
from "reading the registry back out." That keeps any registry consumer — the
`render_prometheus()` exposition path (issue #53), or any future reader — off a
second, parallel recording path, and avoids hardwiring Prometheus's own
concrete `Registry`/`HistogramVec` types into this crate's recording call
sites.

### 2. End-to-end latency keying: terminal transform only

**Decision:** end-to-end latency is one histogram per **terminal (sink)
transform**, summed across every source feeding it — not a histogram per
source→sink pair.

`docs/observability.md`'s "What 'latency' means" section left this as an
open cardinality question. A DAG can have several sources feeding one sink;
keying by pair multiplies the metric's cardinality by fan-in for no operator
benefit most of the time — "how stale is this transform's output" is the
question an operator actually asks, and that's answered by the terminal
transform alone. Per-source breakdowns, if ever needed, are a debugging tool
better served by traces (see decision 3) than by a permanently-exported
high-cardinality metric series.

### 3. Traces vs. flat logs: spans are first-class

**Decision:** adopt `tracing` spans as a first-class signal for propagation.
A change moving source → hop → hop → apply is modeled as a span tree, and
per-hop/per-transform latency is *derived* from span durations rather than
instrumented a second time with independent timers.

This resolves the framing question `docs/observability.md`'s "Logs and
traces" section left open, and has two concrete downstream effects:

* It gates issue #56's design: #56 is a span-based instrumentation of
  `trellis/src`'s propagation path (source intake, staging, fold, apply),
  not a flat-log-plus-metrics design. As noted during research, there is no
  existing `log`/`tracing` usage anywhere in the `trellis` crate today, so
  this is greenfield instrumentation, not a conversion.
* It clarifies #51: the per-transform latency histogram is **populated from
  span/timing data captured during fold**, not a second, independently-timed
  measurement. Fold already has the origin timestamp in hand (see decision 5
  below) — a span covering a staged row's journey from that origin to its
  applied output gives the histogram its `.observe()` value directly from the
  span's duration, so the two signals (traces and the per-transform latency
  metric) stay consistent by construction instead of by convention.

### 4. Transform status storage: no new field

**Decision:** transform lifecycle status continues to live in the single
existing `transform_definitions.status` column — the `TransformStatus` enum
(`WaitingToBackfill | Backfilling | Live | Quarantined`,
`trellis/src/defs/model.rs`), persisted as text and check-constrained in
`V19__transform_status.sql`. No new schema is introduced for status.

This was an open question in `docs/observability.md`'s "Where transform
status is stored and read" bullet: whether the lifecycle status should live
alongside [ADR-0003](0003-quarantine-storage-and-api.md)'s quarantine model
or in its own row. It's already the same field: `Quarantined` is a value the
`TransformStatus` enum reserves for ADR-0003's whole-transform fuse tier, so
quarantine and the backfill lifecycle are designed as two arcs of one state
machine sharing one field, exactly as `docs/observability.md`'s "Transform
status lifecycle" diagram depicts. ADR-0003's *column*-level quarantine tier
(`column_status`/`column_failures`) is intentionally a separate, finer-grained
mechanism that does not touch this field — a `live` transform can carry
individually paused columns without its overall `status` moving, so there is
no conflict between the two tiers sharing this column's semantics.

Issue #55 wires the `waiting_to_backfill`/`backfilling`/`live` transitions on
this field — the `xmin`-fence wait (`trellis/src/intake/publication.rs`'s
`Snapshot::settled_since`) and both backfill-enumeration paths — plus a
`quarantined → waiting_to_backfill` resume function
(`staging::quarantine::resume_transform`), rather than introducing a parallel
status source. The whole-transform fuse-trip condition that would *write*
`Quarantined` to this field is still unwritten — designed for but not yet
implemented, tracked as a follow-up (see epic #49) — so `resume_transform` is
correct but not yet reachable in production. The per-key (`poison`) and
per-column (`column_status`) tiers have their own writers today.

### 5. Staging-ring metrics: drop the per-transform depth gauge, add a cheap segment-state gauge

**Decision:** the staging ring is not measured by a per-transform
`staging_ring_depth{transform}` gauge. Two independent pieces cover the need
instead:

* **Per-transform latency histogram** (#51/#52), computed at **apply
  completion**, not on a separate live-counter path. The origin timestamp
  already rides on every staged row — `src_changed: Option<SystemTime>` on
  `StagedChange::Cdc`/`Truncate` (`trellis/src/staging/append.rs`), sourced
  from the replication `Commit` event's `commit_time_micros`
  (`trellis/src/intake/mod.rs`) — and the apply path already groups folded
  changes by which transform(s) consume them (`compute()`'s `by_source` map
  in `trellis/src/staging/apply.rs`, downstream of fold). Tagging one
  histogram `.observe()` call per transform-group there is effectively free:
  it reuses data and grouping apply already computes, with no new I/O and no
  new join.
* **`staging_segments{state}` gauge** (new, cheap, system-level): a count of
  segments by `SegmentState` (`Active`/`Sealed`/`Draining`/`Drained`, from
  `trellis/src/staging/state.rs` and the `segments` registry table), read
  on-demand from existing segment metadata rather than incremented on the
  hot append/fold path.

The rejected gauge would have needed bookkeeping on the hot append/fold
path (increment on stage, decrement on fold) to stay live and per-transform —
a throughput risk not worth taking for what `docs/observability.md` itself
called a "supporting" series. It's also the wrong shape for how the ring
actually partitions: the staging ring is partitioned by source-table/key-hash
bucket (`trellis/migrations/V3__staging_ring.sql`,
`trellis/src/staging/state.rs`), not by transform, so a genuinely live
per-transform depth reading would need a join/aggregation the ring's own
layout doesn't offer for free. `staging_segments{state}` gives an operator
the same "is the ring backing up" signal at effectively zero cost, using data
that's already tracked for segment lifecycle management.

### 6. Histogram bucket boundaries

**Decision:** exponential buckets spanning roughly 10ms to 60s — matching
`docs/observability.md`'s stated "sub-second to tens-of-seconds propagation
range" — as a single global default applied to every histogram (per-transform
and end-to-end alike):

```
[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 60.0]  // seconds
```

Not configurable per transform in this pass. A single default keeps the
initial implementation (#51/#52) simple and every histogram directly
comparable; if a specific transform's latency profile turns out to need
different boundaries (much faster or slower than this range), that's a
targeted follow-up once real data justifies it, not a speculative knob added
up front.

### 7. Metric retention: left to Prometheus, not Trellis

**Decision:** Trellis retains no metric history of its own. Retention, rollup,
and cross-instance aggregation are exactly what an operator's existing
Prometheus/VictoriaMetrics/Thanos stack already does well; duplicating a pruned
history table inside Trellis would add write load, a schema object, and a prune
job for a capability mature external tooling already covers, rather than a gap
Trellis itself needs to fill. Operators who want history configure their
scraper's own retention against the `render_prometheus()` endpoint like any
other Prometheus target.

A pruned Postgres rollup table (`trellis.metric_rollup`, issue #54) was built
(`V25__metric_rollup.sql`, `trellis::rollup`) and then removed in favor of this
approach — see `docs/observability.md`'s
["Retention"](../observability.md#retention-left-to-prometheus-not-trellis)
section.

## Options considered

Dependency choice (decision 1) was the one item here with a real alternative
weighed against it:

* **`prometheus` crate directly.** The obvious default for Prometheus text
  exposition, and what `docs/observability.md`'s "Proposed dependencies"
  section listed as an explicit alternative. Rejected because it hardwires
  Prometheus's own concrete registry/collector types into the recording call
  sites, so any consumer that wants to read the registry back (the exposition
  path, or a future reader) is coupled to those internal type shapes.
* **`metrics` + `metrics-exporter-prometheus` (chosen).** A thin recording
  facade in front of a Prometheus-flavored exporter. Costs one extra crate in
  the dependency graph relative to using `prometheus` directly, in exchange for
  a `Recorder`/inspection-trait seam that separates recording an observation
  from reading the registry back out.

The other six decisions were framing/design questions
`docs/observability.md` posed explicitly as open (end-to-end keying, traces
vs. flat logs, status storage location, staging-ring metrics shape, bucket
boundaries, metric retention); each is a single
settled choice rather than a field of alternatives, so they're recorded above
under "Decisions" with their rationale rather than re-listed here.

## Related

* [ADR-0003](0003-quarantine-storage-and-api.md) — the quarantine storage and
  fuse model that decision 4 reuses `transform_definitions.status` alongside.
* [docs/observability.md](../observability.md) — the design doc this ADR
  settles the open questions from; amended in place to link back here.
* Issues #49 (epic), #50 (this ADR), #51/#52 (per-transform and end-to-end
  latency), #53 (Prometheus exposition), #54 (rollup job), #55 (backfill
  status wiring), #56 (span-based tracing instrumentation).
