//! Dev-only, uncommitted surface — ADR-0012's one sanctioned exception.
//!
//! **This is not API.** Nothing here carries a public-API or semver
//! commitment; anything reached through it may change without notice. It is
//! compiled only under `cfg(test)` or the off-by-default `test-util` cargo
//! feature, which no production dependency edge may ever enable — see
//! `Cargo.toml`'s `test-util` comment for the workspace feature-unification
//! rule and the `strings` check that enforces it.
//!
//! ADR-0012 puts `defs`, `staging`, and `intake` behind `pub(crate)`: the
//! sibling crates drive the system through the facade and verify against
//! Postgres, not by calling engine functions. The exception the ADR sanctions
//! is the generative fuzz suite's *independent, evaluator-driven cross-check
//! leg* — a second recompute whose only job is to catch divergence between
//! the engine's Rust evaluator and Postgres semantics. Because it must stay
//! independent of both the shipped `self_check` audit (ADR-0013; not yet
//! implemented) and the suite's own SQL oracle, it cannot become a public
//! method. This module is that leg's reach into the
//! engine, plus the small set of type and registry lookups its
//! correctness assertions need. `benchmark`'s scenario oracle, which renders
//! the engine's own `SELECT` for a definition and compares a persisted target
//! against it, is the same shape of consumer.
//!
//! The paths here mirror the engine's own, so a consumer writes
//! `trellis::dev::defs::ast::TransformDef` where it used to write
//! `trellis::defs::ast::TransformDef`.
//!
//! Adding to this module is a deliberate, visible act. If you are reaching
//! for something here in order to *drive the engine into a state*, that is
//! the thing ADR-0012 says to do through `Trellis`/`Client` instead.
//!
//! This is **not** the door this crate's own `tests/*.rs` use — those are
//! separate compilation units exercising the engine directly by design, and
//! they see `defs`/`staging`/`intake` through the `internals` feature
//! instead. See `Cargo.toml`.

/// The engine items `generative` and `benchmark` reach for under `defs`.
pub mod defs {
    /// The parsed definition AST. The fuzz generator emits definition *text*
    /// and installs it through the public path (ADR-0012), but its oracle and
    /// coverage tracker still need to name the shapes it generated.
    pub use crate::defs::ast;
    /// `SUM`/`COUNT`/`AVG`/`MIN`/`MAX`'s invertibility classification — the
    /// suite asserts against the engine's own verdict rather than a second,
    /// drifting copy of the table.
    pub use crate::defs::invertibility;
    /// The evaluator-driven cross-check leg itself, plus the SQL renderers
    /// `benchmark`'s scenario oracle compares a persisted target against.
    pub use crate::defs::oracle;
    /// Function/operator/aggregate registries: the source of truth the oracle
    /// consults instead of hardcoding the engine's accepted spellings.
    pub use crate::defs::registry;
    /// Issue #109's typed-literal allowlist and its shared SQL renderer. The
    /// suite's own `render_expr` has to spell an `Expr::TypedLiteral` back
    /// out — both as definition source text and as oracle SQL — and must do
    /// it from the engine's table rather than a second, drifting copy, for
    /// the same reason `registry` is reachable here.
    pub use crate::defs::typed_literal;

    pub use crate::defs::{
        CatalogError, DdlError, PgType, TransformStatus, ValueType, backfill_definition,
        create_aggregate_target_table, create_definition_without_backfill, create_relationship,
        create_target_table, install_definition, parse, qualified_target_table,
        require_single_column_pk, source_primary_key,
    };
}

/// The engine items `generative`'s backend drivers reach for under `staging`.
pub mod staging {
    /// `seal_phase1`/`seal_phase2` force a seal boundary at a deterministic
    /// point so two changes land in distinct segments instead of depending on
    /// a maintenance tick's timing. There is no facade equivalent — "seal
    /// now" is an internal cadence concern, not a user capability — so this
    /// stays a gated reach rather than a new public method.
    pub use crate::staging::{
        StagingError, await_converged, has_pending, seal_phase1, seal_phase2, watermark_token,
    };
}
