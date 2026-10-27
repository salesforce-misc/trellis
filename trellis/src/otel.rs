//! Optional OTLP export layer (ADR-0009 decisions 1 and 3).
//!
//! Compiled **only** when the `otlp` Cargo feature is enabled — off by
//! default. With the feature off, none of this module's four dependencies
//! (`opentelemetry`, `opentelemetry_sdk`, `opentelemetry-otlp`,
//! `tracing-opentelemetry`) are even compiled in, let alone linked; see
//! `Cargo.toml`'s `[features]` section, and this crate's `cargo build
//! -p trellis` (no flags) vs. `cargo build -p trellis --features otlp`,
//! both of which succeed independently.
//!
//! **This module never installs a global subscriber.** [`layer`] hands back
//! a plain `tracing_subscriber::Layer` for the *embedder* to compose into
//! their own subscriber (`tracing_subscriber::registry().with(...)`,
//! optionally alongside `tracing_subscriber::fmt::layer()` for local
//! structured logs) and call `tracing::subscriber::set_global_default` on
//! themselves — matching `src/metrics.rs`'s "library, not a binary"
//! discipline for the `metrics` facade (that module never binds a socket;
//! this one never claims the process's one global subscriber slot). Every
//! span/event this crate emits (see `docs/observability.md`'s "Logs and
//! traces" section and `docs/decisions/0009-observability-decisions.md`
//! decision 3) flows through the ordinary `tracing` facade regardless of
//! whether this module — or the `otlp` feature at all — is ever used: with
//! no subscriber installed, or a subscriber built with no OTLP layer,
//! spans/events cost only the `tracing` crate's own near-zero
//! no-subscriber overhead ("local structured logs unaffected without a
//! collector").
//!
//! # Usage
//!
//! ```no_run
//! # #[cfg(feature = "otlp")]
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use tracing_subscriber::layer::SubscriberExt;
//!
//! let provider = trellis::otel::tracer_provider("http://localhost:4317", "trellis")?;
//! let otlp_layer = trellis::otel::layer(&provider);
//! tracing::subscriber::set_global_default(tracing_subscriber::registry().with(otlp_layer))?;
//!
//! // ... run the embedder's application ...
//!
//! // On shutdown: flush any spans still buffered in the batch exporter.
//! provider.shutdown()?;
//! # Ok(())
//! # }
//! ```
//!
//! [`tracer_provider`] and [`layer`] are deliberately two separate
//! functions, not one that installs everything: the embedder needs to keep
//! the [`opentelemetry_sdk::trace::SdkTracerProvider`] handle around to call
//! `.shutdown()` on process exit (flushing the batch exporter's buffer) —
//! something a single all-in-one `install()` function couldn't hand back
//! without inventing its own shutdown-guard type.

use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::Layer;
use tracing_subscriber::registry::LookupSpan;

/// Re-exported rather than wrapped: [`opentelemetry_otlp::ExporterBuildError`]
/// already implements `std::error::Error`/`Display` (via `thiserror`), so a
/// bespoke wrapper here would add nothing but boilerplate.
pub use opentelemetry_otlp::ExporterBuildError as OtelError;

/// Builds a [`SdkTracerProvider`] whose batch span processor exports to
/// `endpoint` over OTLP/gRPC (`opentelemetry-otlp`'s Tonic transport — the
/// exact dependency ADR-0009 decision 1 approved; this module's `Cargo.toml`
/// entry deliberately disables the crate's `http-proto`/`reqwest` default
/// features to keep the `otlp` feature's dependency footprint to just the
/// gRPC path). `endpoint` is a plain `http://host:port` URL (e.g.
/// `http://localhost:4317` for a collector's default gRPC port) — not
/// validated until [`opentelemetry_otlp::SpanExporter::builder`]'s `build()`
/// call actually parses it.
///
/// `service_name` becomes the exported `service.name` resource attribute —
/// the one label every OTLP backend expects to group spans by; callers
/// typically pass their own binary's name (`env!("CARGO_PKG_NAME")` or
/// similar), not necessarily `"trellis"`, since the embedder's process may
/// emit spans from more than one crate.
///
/// The returned provider is *not* installed anywhere — [`layer`] derives a
/// `tracing_subscriber::Layer` from it, and the caller is responsible for
/// composing that layer into their own subscriber and for calling
/// `.shutdown()` on the provider before the process exits, so the batch
/// exporter's buffer flushes rather than dropping in-flight spans silently.
pub fn tracer_provider(endpoint: &str, service_name: &str) -> Result<SdkTracerProvider, OtelError> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;
    let resource = Resource::builder()
        .with_service_name(service_name.to_string())
        .build();
    Ok(SdkTracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(exporter)
        .build())
}

/// A `tracing_subscriber::Layer` that exports every span it sees to
/// `provider`'s configured OTLP endpoint — thin plumbing over
/// `tracing_opentelemetry::layer().with_tracer(...)`. Compose it into an
/// embedder's own subscriber (see this module's doc comment for a full
/// example); this function never installs anything globally itself.
///
/// Takes `&SdkTracerProvider` rather than consuming it: the caller keeps
/// the provider to call `.shutdown()` on later (see [`tracer_provider`]'s
/// doc comment). `provider.tracer(...)` clones the provider's internal
/// handle into the returned `Tracer`, so the caller dropping their own
/// `SdkTracerProvider` value after this call would still leave the export
/// pipeline running — but doing so removes the caller's only handle for a
/// clean shutdown flush, so callers should hold onto it regardless.
pub fn layer<S>(provider: &SdkTracerProvider) -> impl Layer<S> + Send + Sync + 'static
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync,
{
    let tracer = opentelemetry::trace::TracerProvider::tracer(provider, "trellis");
    tracing_opentelemetry::layer().with_tracer(tracer)
}
