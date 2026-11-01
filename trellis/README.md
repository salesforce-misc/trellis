# trellis-pg

> **Version 0.1:** the API may change between 0.x minor releases, before 1.0. An upgrade never re-interprets a stored definition: it keeps its meaning, or is paused with the reason; see [grammar versions](https://github.com/salesforce-misc/trellis/blob/main/docs/transforms.md#grammar-versions).

The engine behind [Trellis](https://github.com/salesforce-misc/trellis): streaming SQL transforms
(aggregates, `JOIN`s and copies) that are maintained incrementally inside your own PostgreSQL
database. A target is an ordinary table that stays exactly equal to the query over its source as
the source changes.

The crate is published as `trellis-pg`; its library is named `trellis`:

```toml
[dependencies]
trellis-pg = "0.1"
```

```rust
use trellis::Client;
```

Trellis is also available as a command-line tool and as Elixir and Ruby bindings; the
[README](https://github.com/salesforce-misc/trellis#install) lists them all.

## Documentation

* [Embedding Trellis in an application](https://github.com/salesforce-misc/trellis/blob/main/docs/embedding.md)
* [Transform grammar and supported sources and targets](https://github.com/salesforce-misc/trellis/blob/main/docs/transforms.md)
* [Running Trellis well](https://github.com/salesforce-misc/trellis/blob/main/docs/recommendations.md)
* [Known correctness gaps](https://github.com/salesforce-misc/trellis/blob/main/docs/known-correctness-gaps.md)
* [API documentation](https://docs.rs/trellis-pg)

Licensed under the [Apache License, Version 2.0](https://github.com/salesforce-misc/trellis/blob/main/LICENSE.txt).
