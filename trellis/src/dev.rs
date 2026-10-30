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
//! is the generative fuzz suite's SQL oracle, which renders a definition back to
//! a `SELECT` and must stay independent of the shipped `self_check` audit
//! (ADR-0013), so it cannot become a public method. This module is the
//! oracle's reach into the engine: the small set of type and registry lookups
//! its correctness assertions need. `benchmark`'s scenario oracle, which renders
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

/// The default instance schema name, for harnesses that build a `Config` or
/// name Trellis's own tables without resolving one.
pub mod config {
    pub use crate::config::DEFAULT_SCHEMA;
}

/// The latency histogram boundaries and build-statement classes, for
/// `benchmark`'s scrape of the engine's metrics.
pub mod metrics {
    pub use crate::metrics::{BuildStatement, LATENCY_BUCKETS, increment_changes_applied};
}

/// The engine items `generative` and `benchmark` reach for under `defs`.
pub mod defs {
    /// The parsed definition AST. The fuzz generator emits definition *text*
    /// and installs it through the public path (ADR-0012), but its oracle and
    /// coverage tracker still need to name the shapes it generated.
    pub use crate::defs::ast;
    /// The SQL renderers `benchmark`'s scenario oracle compares a persisted
    /// target against.
    pub use crate::defs::oracle;
    /// Function/operator/aggregate registries: the source of truth the oracle
    /// consults instead of hardcoding the engine's accepted spellings.
    pub use crate::defs::registry;
    /// The typed-literal allowlist and its shared SQL renderer. The
    /// suite's own `render_expr` has to spell an `Expr::TypedLiteral` back
    /// out — both as definition source text and as oracle SQL — and must do
    /// it from the engine's table rather than a second, drifting copy, for
    /// the same reason `registry` is reachable here.
    pub use crate::defs::typed_literal;

    /// The 1-1 JOIN dev knob (ADR-0021, epic #1392): tests and `benchmark`
    /// turn the grammar on, once, before a client starts. It is removed with
    /// the epic's last milestone.
    pub use crate::defs::join_one_to_one::{
        KNOB_ENV as ONE_TO_ONE_JOIN_ENV, MAX_FANOUT_ENV as ONE_TO_ONE_JOIN_MAX_FANOUT_ENV,
        enabled as one_to_one_join_enabled, set_enabled as set_one_to_one_join,
        set_max_fanout as set_one_to_one_join_max_fanout,
    };
    pub use crate::staging::join_one_to_one_status::{
        FAN_OUT_WARNING_ENV as ONE_TO_ONE_JOIN_FAN_OUT_WARNING_ENV,
        fan_out_warning_rows as one_to_one_join_fan_out_warning_rows,
        set_fan_out_warning_rows as set_one_to_one_join_fan_out_warning_rows,
    };

    pub use crate::float::FloatWidth;
    pub use crate::integer::IntWidth;

    pub use crate::defs::{
        CatalogError, DdlError, PgType, TransformStatus, ValueType, create_aggregate_target_table,
        create_definition_without_backfill, create_target_table, install_definition, parse,
        qualified_target_table, source_primary_key,
    };

    /// The define-time checks, for the expression conformance harness: it
    /// runs `validate` on a bare definition and names the refusal it gets.
    pub use crate::defs::validate::{ValidationError, validate};

    /// `def` rendered back to the definition text the catalog stores, which
    /// `parse` reads back to an equal definition (the round-trip property of
    /// the expression conformance harness).
    pub fn render_definition_text(def: &crate::defs::ast::TransformDef) -> String {
        crate::defs::ast::render_definition_text(def)
    }

    /// `expr` rendered as the SQL a 1-1 apply and a `JOIN` write embed, each
    /// column read through `source_sql`. The expression conformance harness
    /// checks it against the user's text beside `oracle::render_expr_sql`
    /// (the build's renderer), since the two are separate code.
    pub fn render_qualified_expr_sql(expr: &crate::defs::ast::Expr, source_sql: &str) -> String {
        crate::defs::oracle::render_qualified_expr_sql(expr, source_sql)
    }
}

/// The engine items `generative`'s backend drivers reach for under `staging`.
pub mod staging {
    /// Read, never set: `benchmark`'s contention sampler tells a Re-derive
    /// build's statements from a drain page's by the `application_name`
    /// their transactions set.
    pub use crate::staging::build::CHUNK_APPLICATION_NAME;
    /// Read, never set: the concurrent tier's coverage report
    /// counts bursts large enough to seal into a split batch, and must follow
    /// the threshold if it moves.
    pub use crate::staging::claim::MIN_ROWS_TO_SPLIT;
    /// `seal_phase1`/`seal_phase2` force a seal boundary at a deterministic
    /// point so two changes land in distinct segments instead of depending on
    /// a maintenance tick's timing. There is no facade equivalent — "seal
    /// now" is an internal cadence concern, not a user capability — so this
    /// stays a gated reach rather than a new public method.
    /// `retire_drained_segments` rides along because a forced seal must
    /// answer `RingFull` the same way the engine's own seal-on-demand does:
    /// retire what's retirable, then retry.
    /// `seal_if_active_nonempty` is the maintenance tick's own seal step, for
    /// a harness that runs with that tick disabled and seals on demand
    /// instead: it seals only when there is something to seal.
    /// `converged_through` is the single, non-waiting check behind
    /// `await_converged`, for a harness that must re-check the ring once
    /// without starting another wait.
    pub use crate::staging::converge::converged_through;
    /// The plan of a group-basis page's apply statement, for plan-shape
    /// tests.
    #[cfg(any(test, feature = "internals"))]
    pub use crate::staging::group_basis::explain_apply as explain_group_apply;
    /// The plans of a group-basis target's whole-source reads (its node-form
    /// recompute and its rebuild), for plan-shape tests.
    #[cfg(any(test, feature = "internals"))]
    pub use crate::staging::group_basis::explain_whole_source_reads;
    /// One page of a `self_check` walk, for `benchmark`'s `self-check-walk`:
    /// it walks a JOIN target's `P` and `M` parts alone, from the
    /// cursor `p`, which the job (it walks `T` first) can't.
    pub use crate::staging::self_check::{SelfCheckScope, check_page as self_check_page};
    /// One pass of the `self_check` worker task, for a harness that runs no
    /// drain workers (so no such task) and hand-drives the job it started
    /// through `Trellis::self_check`, the way `ManualBackend` drains
    /// by hand.
    pub use crate::staging::self_check_job::{
        DEFAULT_PAGE_KEYS, WorkerOptions as SelfCheckWorkerOptions,
        work_once as work_self_check_once,
    };
    /// The staging-worker singleton's advisory-lock key, for a crash test
    /// that holds the singleton itself to stand in for a killed engine's
    /// backend that hasn't freed it yet.
    pub use crate::staging::session::producer_singleton_lock_key;
    pub use crate::staging::{
        StagingError, await_converged, has_pending, retire_drained_segments,
        seal_if_active_nonempty, seal_phase1, seal_phase2, watermark_token,
    };
}

/// Trigger capture, for `benchmark`'s `write-tax` and
/// `capture-ceiling`: the write-path benchmark installs the capture
/// triggers through the real installer, with nothing draining, before C5
/// wires the installer into the staging worker.
pub mod capture {
    pub use crate::capture::CaptureError;
    pub use crate::capture::columns::{capture_spec, load_catalog};
    pub use crate::capture::install::install;
    pub use crate::capture::sql::CaptureSpec;
}

/// Test-only pause points inside a drain page's transaction, for
/// a harness that freezes one worker between two steps. See
/// `crate::staging::interleave`'s module doc.
pub mod interleave {
    pub use crate::staging::interleave::{
        PausePoint, PauseScope, Reached, force_join_key_moves, join_key_moves, set_commit_stall,
        set_stall, with_scope,
    };
}

/// The Re-derive build's lock timeout, for a harness that drives a build by
/// hand. See `crate::staging::build`'s module doc.
pub mod build {
    pub use crate::staging::build::CHUNK_LOCK_TIMEOUT;
}

/// Planted ordering bugs. A harness reads which plant
/// this process runs with and how often it fired; it never arms one, since
/// only [`plant::PLANT_ENV`] can. See `crate::plant`'s module doc.
pub mod plant {
    pub use crate::plant::{PLANT_ENV, Plant, armed, fired};
}

/// Sample values of the public status and report structs, which are
/// `#[non_exhaustive]` and so cannot be built with a struct literal outside
/// this crate. A harness test builds one with [`fixture!`](crate::fixture):
/// the sample, with the named fields overwritten.
pub mod fixtures {
    use std::time::SystemTime;

    use crate::{
        BackfillFailure, CaptureFailure, CaptureFailureKind, CaptureWait, CorrectingNode,
        DefinitionStatus, DefinitionSummary, DrainFailure, FanOutWarning, HeldKeys, IndexRole,
        JoinStatus, PoisonEntry, PoisonSample, QuarantineEntry, QuarantineState, QuarantineTarget,
        SelfCheckJob, SelfCheckJobState, SelfCheckMode, SelfCheckOutcome, SelfCheckReport,
        SourceIndexGap, TransformStatus,
    };

    #[allow(non_snake_case)]
    pub fn BackfillFailure() -> BackfillFailure {
        BackfillFailure {
            source_table: String::new(),
            attempts: 0,
            last_error: String::new(),
            next_attempt_at: SystemTime::UNIX_EPOCH,
        }
    }

    #[allow(non_snake_case)]
    pub fn CaptureFailure() -> CaptureFailure {
        CaptureFailure {
            kind: CaptureFailureKind::ALL[0],
            source_table: String::new(),
            columns: Vec::new(),
            error: String::new(),
            detected_at: SystemTime::UNIX_EPOCH,
        }
    }

    #[allow(non_snake_case)]
    pub fn CaptureWait() -> CaptureWait {
        CaptureWait {
            table: String::new(),
            operation: String::new(),
            lock_mode: String::new(),
            waiting_since: SystemTime::UNIX_EPOCH,
            observed_at: SystemTime::UNIX_EPOCH,
            blockers: Vec::new(),
        }
    }

    #[allow(non_snake_case)]
    pub fn CorrectingNode() -> CorrectingNode {
        CorrectingNode {
            node: String::new(),
            since: SystemTime::UNIX_EPOCH,
            keys_remaining: None,
        }
    }

    #[allow(non_snake_case)]
    pub fn DefinitionStatus() -> DefinitionStatus {
        DefinitionStatus {
            status: TransformStatus::Live,
            backfill_failure: None,
            capture_wait: None,
            capture_failure: None,
            held_keys: None,
            drain_failure: None,
            source_index_gaps: Vec::new(),
            join: None,
            correcting: Vec::new(),
            paused_upstream_columns: Vec::new(),
            build_wait: None,
        }
    }

    #[allow(non_snake_case)]
    pub fn DefinitionSummary() -> DefinitionSummary {
        DefinitionSummary {
            target_table: String::new(),
            status: TransformStatus::Live,
            created_at: SystemTime::UNIX_EPOCH,
            backfill_failure: None,
            halt: None,
        }
    }

    #[allow(non_snake_case)]
    pub fn DrainFailure() -> DrainFailure {
        DrainFailure {
            seg_seq: 0,
            tables: Vec::new(),
            error: String::new(),
            sqlstate: None,
            since: SystemTime::UNIX_EPOCH,
            last_seen: SystemTime::UNIX_EPOCH,
            attempts: 0,
        }
    }

    #[allow(non_snake_case)]
    pub fn HeldKeys() -> HeldKeys {
        HeldKeys {
            count: 0,
            oldest_poisoned_at: SystemTime::UNIX_EPOCH,
            max_retry_attempts: 0,
            next_retry_at: None,
        }
    }

    #[allow(non_snake_case)]
    pub fn FanOutWarning() -> FanOutWarning {
        FanOutWarning {
            key: String::new(),
            rows: 0,
            threshold: 0,
        }
    }

    #[allow(non_snake_case)]
    pub fn JoinStatus() -> JoinStatus {
        JoinStatus {
            hidden_tables: Vec::new(),
            partials_warning: None,
            fan_out_warning: None,
            building: None,
        }
    }

    #[allow(non_snake_case)]
    pub fn PoisonEntry() -> PoisonEntry {
        PoisonEntry {
            transform: String::new(),
            src_table: String::new(),
            key: String::new(),
            last_error: String::new(),
            poisoned_at: SystemTime::UNIX_EPOCH,
        }
    }

    #[allow(non_snake_case)]
    pub fn PoisonSample() -> PoisonSample {
        PoisonSample {
            src_table: String::new(),
            key: String::new(),
            error_message: String::new(),
            retry_attempts: 0,
            next_retry_at: None,
        }
    }

    #[allow(non_snake_case)]
    pub fn QuarantineEntry() -> QuarantineEntry {
        QuarantineEntry {
            target: QuarantineTarget::Transform(String::new()),
            state: QuarantineState::ALL[0],
            paused_at: None,
            last_error: None,
        }
    }

    #[allow(non_snake_case)]
    pub fn SelfCheckJob() -> SelfCheckJob {
        SelfCheckJob {
            id: 0,
            target: String::new(),
            mode: SelfCheckMode::Strict,
            state: SelfCheckJobState::Cancelled,
            rows_compared: 0,
            report: None,
            error: None,
        }
    }

    #[allow(non_snake_case)]
    pub fn SelfCheckReport() -> SelfCheckReport {
        SelfCheckReport {
            target: String::new(),
            checked_through: 0.into(),
            rows_compared: 0,
            truncated: false,
            outcome: SelfCheckOutcome::Converged,
            held_keys: None,
            drain_failures: Vec::new(),
        }
    }

    #[allow(non_snake_case)]
    pub fn SourceIndexGap() -> SourceIndexGap {
        SourceIndexGap {
            table: String::new(),
            columns: Vec::new(),
            role: IndexRole::GroupBy,
            opted_out: false,
        }
    }
}

/// Builds a `#[non_exhaustive]` status or report struct for a harness test:
/// its [`dev::fixtures`](crate::dev::fixtures) sample with the named fields
/// overwritten, so it reads like the struct literal it replaces.
#[macro_export]
#[doc(hidden)]
macro_rules! fixture {
    (@value $field:ident) => { $field };
    (@value $field:ident $value:expr) => { $value };
    ($ty:ident { $($field:ident $(: $value:expr)?),* $(,)? }) => {{
        #[allow(unused_mut)]
        let mut sample = $crate::dev::fixtures::$ty();
        $(sample.$field = $crate::fixture!(@value $field $($value)?);)*
        sample
    }};
}
