# Trellis documentation

Start with the reader you are. Terms are in the [glossary](glossary.md).

## I write definitions

* [Transforms](transforms.md): the statements, the four shapes (1-1, aggregate, `JOIN`, 1-1 `JOIN`), and what sources and targets are supported.
* [Expressions](expressions.md): what a calculated field may contain.
* [Type support](type-support.md): which Postgres types work, in which role.
* [Known correctness gaps](known-correctness-gaps.md): the ways a target can go stale or wrong without Trellis repairing it.

## I run Trellis in an application

* [Embedding](embedding.md): running it in-process from Rust, Ruby or Elixir, and polling to `live`.
* [Instance identity](instance-identity.md): the schema an instance owns.
* [Upgrading](upgrading.md): upgrade and roll back.

## I operate it

* [Recommendations](recommendations.md): WAL and checkpoint tuning, ring sizing, roles and permissions.
* [Operations](operations.md): what to do when a definition stops, and how each repair behaves.
* [Status reference](status-reference.md): each status word and field.
* [Observability](observability.md): metrics, logs, traces, and the stops and waits behind some status fields.

## I change Trellis

* [CONTRIBUTING](../CONTRIBUTING.md): process and checklist.
* [Architecture](architecture.md): which module owns which concept and catalog table.
* [Data flow](data-flow.md): how a change travels, logically; [staging and claiming](staging-and-claiming/README.md) is the machinery, stage by stage.
* [Decisions](decisions/): the ADRs, which every change must agree with.
* [Testing strategy](testing-strategy.md) and the [generative test suite](generative-test-suite.md): how correctness is proven.
* [Releasing](releasing.md): how a release is cut and what it ships.
