//! Integration tests for issue #56 (epic #49): the `tracing` spans/events
//! added along the propagation path (`staging::apply::compute`,
//! `staging::apply::apply_target`/`apply_and_mark_drained`/`drain_once`,
//! quarantine trips, backfill status transitions) actually fire, with the
//! field conventions ADR-0009 decision 3 calls for.
//!
//! `trellis/src` never installs a global `tracing` subscriber itself (the
//! embedder's job — see `src/otel.rs`'s module doc comment) — these tests
//! install one of their own, scoped to each test via
//! `tracing::subscriber::set_default`'s thread-local guard, using
//! `tracing_subscriber`'s `Registry` + a small capturing `Layer` (the same
//! `Registry`/`Layer` combinator shape an embedder composes their own
//! subscriber from, per `docs/observability.md`'s "Logs and traces"
//! section) — not a bespoke, unrealistic test-only subscriber shape.
//!
//! Every test here runs on `#[tokio::test]`'s default (current-thread)
//! runtime, matching the rest of this crate's integration tests: the
//! capturing layer is installed via a thread-local guard, which stays valid
//! across every `.await` point in these tests only because nothing here
//! ever hops to a different OS thread mid-poll.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::SystemTime;

use testkit::TestCluster;
use testkit::crash::OpenTransaction;
use tokio_postgres::{Client, NoTls};
use tracing::field::{Field, Visit};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

use trellis::config::DEFAULT_SCHEMA;
use trellis::defs::ast::{Expr, FieldDef, KeySpace, Operator, Predicate, TransformDef, ValueType};
use trellis::defs::{TransformStatus, create_definition, create_target_table, install_definition};
use trellis::staging::apply;
use trellis::staging::{FoldedChange, IsolationOutcome, StagedWatermark, isolate_and_evict};

// ---------------------------------------------------------------------
// A minimal capturing `tracing_subscriber::Layer`
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
struct CapturedSpan {
    name: &'static str,
    fields: HashMap<String, String>,
}

#[derive(Debug, Clone)]
struct CapturedEvent {
    level: tracing::Level,
    fields: HashMap<String, String>,
}

impl CapturedEvent {
    fn message(&self) -> &str {
        self.fields.get("message").map(String::as_str).unwrap_or("")
    }
}

#[derive(Clone, Default)]
struct Captured {
    spans: Arc<Mutex<HashMap<u64, CapturedSpan>>>,
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

impl Captured {
    fn spans(&self) -> Vec<CapturedSpan> {
        self.spans.lock().unwrap().values().cloned().collect()
    }

    fn events(&self) -> Vec<CapturedEvent> {
        self.events.lock().unwrap().clone()
    }

    /// The one captured span named `name` — panics (with every captured
    /// span's name, for an easy diff) if there isn't exactly one, since
    /// every test below stages exactly one batch through each span.
    fn span_named(&self, name: &str) -> CapturedSpan {
        let spans = self.spans();
        let matches: Vec<&CapturedSpan> = spans.iter().filter(|s| s.name == name).collect();
        match matches.as_slice() {
            [one] => (*one).clone(),
            _ => panic!(
                "expected exactly one span named {name:?}, found {}: {spans:#?}",
                matches.len()
            ),
        }
    }
}

/// Records every field (however it's recorded — `record_str`, `record_i64`,
/// `record_debug`, ...) as its `Debug`-formatted text. A `%field` value
/// (`tracing`'s "Display" field wrapper) formats through `record_debug`
/// with a `Debug` impl that forwards to the original `Display` impl, so a
/// `%`-recorded string field (every string field this crate's own spans/
/// events use — see `src/staging/apply.rs`) comes
/// through here unquoted, exactly as it displays.
struct FieldVisitor(HashMap<String, String>);

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }
}

struct CaptureLayer(Captured);

impl<S> Layer<S> for CaptureLayer
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        _ctx: Context<'_, S>,
    ) {
        let mut visitor = FieldVisitor(HashMap::new());
        attrs.record(&mut visitor);
        self.0.spans.lock().unwrap().insert(
            id.into_u64(),
            CapturedSpan {
                name: attrs.metadata().name(),
                fields: visitor.0,
            },
        );
    }

    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        _ctx: Context<'_, S>,
    ) {
        let mut visitor = FieldVisitor(HashMap::new());
        values.record(&mut visitor);
        if let Some(span) = self.0.spans.lock().unwrap().get_mut(&id.into_u64()) {
            span.fields.extend(visitor.0);
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = FieldVisitor(HashMap::new());
        event.record(&mut visitor);
        self.0.events.lock().unwrap().push(CapturedEvent {
            level: *event.metadata().level(),
            fields: visitor.0,
        });
    }
}

/// Installs a fresh [`Captured`]/[`CaptureLayer`] pair as this thread's
/// default `tracing` subscriber, returning the guard (drop it to restore
/// whatever was installed before) and the handle to read captured
/// spans/events back out of.
///
/// `tracing` caches each callsite's interest globally, and while only one
/// scoped dispatcher is live it takes that interest from the default of
/// whichever thread registers the callsite. A test that logs before
/// installing its capture could then cache a shared callsite as "never"
/// while another test's capture was the only one live, silently dropping
/// that test's events. A permanently live no-op dispatcher keeps two
/// registered, so interest is always computed over every live dispatcher.
fn install_capture() -> (tracing::subscriber::DefaultGuard, Captured) {
    static KEEP_INTEREST_GLOBAL: LazyLock<tracing::Dispatch> =
        LazyLock::new(|| tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default()));
    LazyLock::force(&KEEP_INTEREST_GLOBAL);
    let captured = Captured::default();
    let subscriber = tracing_subscriber::registry().with(CaptureLayer(captured.clone()));
    let guard = tracing::subscriber::set_default(subscriber);
    (guard, captured)
}

// ---------------------------------------------------------------------
// Shared scaffolding (mirrors apply.rs/end_to_end_latency.rs/quarantine.rs)
// ---------------------------------------------------------------------

async fn connect_raw(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("set search_path to {DEFAULT_SCHEMA}, public; set datestyle to 'ISO, YMD'; set bytea_output to 'hex'"))
        .await
        .expect("set search_path");
    client
}

async fn seal_active_segment(client: &mut Client) -> i64 {
    use trellis::staging::seal;
    let outcome = seal::seal_phase1(client).await.expect("seal phase 1");
    seal::seal_phase2(client, outcome.sealed_seg_seq, "wake")
        .await
        .expect("seal phase 2");
    outcome.sealed_seg_seq
}

fn numeric_columns(names: &[&str]) -> HashMap<String, ValueType> {
    names
        .iter()
        .map(|n| (n.to_string(), ValueType::Numeric))
        .collect()
}

async fn insert_cdc_row(
    client: &Client,
    table: &str,
    src_table: &str,
    key: &str,
    op: &str,
    new_image: Option<&str>,
) {
    let lsn = testkit::wal_insert_lsn(client).await;
    client
        .execute(
            &format!(
                "insert into {table} (src_table, key, op, lsn, new_image, hop_gen) \
                 values ($1, $2, $3, $4, $5::text::jsonb, 0)"
            ),
            &[&src_table, &key, &op, &lsn, &new_image],
        )
        .await
        .unwrap_or_else(|e| panic!("insert cdc row {key:?} into {table} failed: {e}"));
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

/// Issue #56's apply-phase span tree: `staging.drain_once` parents
/// `staging.compute` and `staging.apply_and_mark_drained`, the latter
/// parenting one `staging.apply_target` per consuming transform — exercised
/// via the same direct-staging harness `apply.rs`/`end_to_end_latency.rs`
/// use (stage a change directly into the ring, no live CDC), draining one
/// one-hop batch: `orders` -> `spans_totals`.
#[tokio::test]
async fn compute_and_apply_spans_fire_with_batch_and_transform_fields() {
    let (_guard, captured) = install_capture();

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;

    client
        .batch_execute("create table orders (id integer primary key, price numeric, tax numeric)")
        .await
        .expect("seed source table");

    let def = TransformDef {
        target: "spans_totals".to_string(),
        source: "orders".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "total".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column("price".to_string())),
                rhs: Box::new(Expr::Column("tax".to_string())),
            },
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        "TRANSFORM spans_totals FROM orders SELECT price + tax AS total",
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = trellis::defs::source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("create target table");

    client
        .execute(
            "insert into orders (id, price, tax) values (1, 10.00, 1.50)",
            &[],
        )
        .await
        .expect("seed source row");
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        Some(r#"{"price":"10.00","tax":"1.50"}"#),
    )
    .await;

    let seg_seq = seal_active_segment(&mut client).await;
    let outcome = apply::drain_once(
        &db.pool,
        seg_seq,
        "span_test_worker",
        1,
        "trellis_span_test",
        &StagedWatermark::saturated(),
    )
    .await
    .expect("drain_once")
    .expect("drain_once must claim and drain something");
    assert_eq!(outcome.keys_written, 1);

    let drain_span = captured.span_named("staging.drain_once");
    assert_eq!(
        drain_span.fields.get("claimed_by").map(String::as_str),
        Some("span_test_worker")
    );
    assert_eq!(
        drain_span.fields.get("attempt").map(String::as_str),
        Some("1"),
        "the winning attempt must be recorded: {drain_span:#?}"
    );

    let compute_span = captured.span_named("staging.compute");
    assert_eq!(
        compute_span.fields.get("folded").map(String::as_str),
        Some("1")
    );
    assert_eq!(
        compute_span.fields.get("sources").map(String::as_str),
        Some("1")
    );

    let apply_span = captured.span_named("staging.apply_and_mark_drained");
    assert_eq!(
        apply_span.fields.get("targets").map(String::as_str),
        Some("1")
    );
    assert_eq!(
        apply_span.fields.get("keys_written").map(String::as_str),
        Some("1")
    );

    let target_span = captured.span_named("staging.apply_target");
    assert_eq!(
        target_span.fields.get("transform").map(String::as_str),
        Some("spans_totals"),
        "the per-transform span's `transform` field must match the metrics facade's own \
         `transform` label convention: {target_span:#?}"
    );
    assert_eq!(
        target_span.fields.get("written").map(String::as_str),
        Some("1")
    );

    // A per-source `tracing::debug!` event, not a nested span (see
    // `compute`'s own doc comment on why) — still visible as a captured
    // event with the source table's qualified name (issue #380 keys
    // `compute`'s per-source loop on it).
    let events = captured.events();
    let qualified_orders = format!("{DEFAULT_SCHEMA}.orders");
    assert!(
        events
            .iter()
            .any(|e| e.message().contains("evaluating a source table")
                && e.fields.get("src_table").map(String::as_str)
                    == Some(qualified_orders.as_str())),
        "expected a per-source debug event naming orders: {events:#?}"
    );
}

/// Creates `orders (id, price, tax)` with one healthy row (id 1) and a
/// one-to-one `target` transform computing `price + tax`, for the quarantine
/// tests below to feed a malformed image for key 1 into.
async fn seed_orders_totals(db: &testkit::TestDatabase, client: &Client, target: &str) {
    let def = TransformDef {
        target: target.to_string(),
        source: "orders".to_string(),
        key_space: KeySpace::OneToOne,
        fields: vec![FieldDef {
            name: "total".to_string(),
            expr: Expr::BinaryOp {
                op: Operator::Add,
                lhs: Box::new(Expr::Column("price".to_string())),
                rhs: Box::new(Expr::Column("tax".to_string())),
            },
        }],
        predicate: Predicate::True,
        explicit_source_schema: None,
        explicit_target_schema: None,
    };
    client
        .batch_execute("create table orders (id integer primary key, price numeric, tax numeric)")
        .await
        .expect("seed source table");
    let source_columns = numeric_columns(&["id", "price", "tax"]);
    create_definition(
        &db.pool,
        &format!("TRANSFORM {target} FROM orders SELECT price + tax AS total"),
        &source_columns,
    )
    .await
    .expect("create definition");
    let pk = trellis::defs::source_primary_key(&db.pool, &def.source)
        .await
        .expect("introspect source primary key");
    create_target_table(&db.pool, &def, "public", &pk, &source_columns, &def.source)
        .await
        .expect("create target table");
    client
        .execute(
            "insert into orders (id, price, tax) values (1, 10.00, 1.50)",
            &[],
        )
        .await
        .expect("seed source row");
}

/// Issue #56's quarantine event: isolating and evicting a poisoned key
/// (`quarantine::evict_key`, called from `isolate_and_evict`) emits a
/// `WARN` event naming the key — mirroring
/// `tests/quarantine.rs`'s `zero_threshold_disables_eviction_even_past_the_default_threshold`'s
/// convention of calling `isolate_and_evict` directly with a hand-built
/// `FoldedChange`, here with `threshold: 1` so the single failing key
/// evicts on its first attempt.
#[tokio::test]
async fn isolating_and_evicting_a_poisoned_key_emits_a_warning_event() {
    let (_guard, captured) = install_capture();

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = connect_raw(db.dsn()).await;
    seed_orders_totals(&db, &client, "spans_evict_totals").await;

    let folded = vec![FoldedChange {
        src_table: "orders".to_string(),
        key: "1".to_string(),
        new_image: Some(r#"{"price":"not-a-number","tax":"1.50"}"#.to_string()),
        old_image: None,
        src_changed: None,
        origin_lsn: None,
        lsn: None,
        min_image_lsn: None,
        hop_gen: 0,
        first_seen: SystemTime::now(),
        group_key: None,
        is_truncate: false,
        relationship_reverse_deferred: None,
        retry_count: 0,
        prior_image: None,
        row_count: 1,
        has_recompute: false,
        vanished_images: Vec::new(),
        ends_in_delete: false,
        last_change: None,
    }];

    let result = isolate_and_evict(
        &db.pool,
        1,
        "span_test_worker",
        "trellis_span_test",
        &folded,
        1,
    )
    .await
    .expect("isolate_and_evict");
    assert!(
        matches!(result, IsolationOutcome::Evicted { .. }),
        "the failing key must be evicted at threshold 1"
    );

    let events = captured.events();
    let evicted = events.iter().find(|e| {
        e.level == tracing::Level::WARN
            && e.message().contains("evicting a key to the poison table")
    });
    assert!(
        evicted.is_some(),
        "expected a WARN event for evicting the poisoned key: {events:#?}"
    );
    let evicted = evicted.unwrap();
    // The *canonical* identity, not the bare `src_table` the folded change above
    // carries (issue #283): every quarantine table is keyed on it, so the
    // eviction warning names the row it actually wrote. An operator reading this
    // line and then grepping `poison` for the name in it only gets a hit if
    // those two agree, which is exactly what that issue restored.
    let canonical = format!("{DEFAULT_SCHEMA}.orders");
    assert_eq!(
        evicted.fields.get("src_table").map(String::as_str),
        Some(canonical.as_str())
    );
    assert_eq!(evicted.fields.get("key").map(String::as_str), Some("1"));

    // Issue #670: isolation logs its start and its end, the end naming the
    // outcome, the keys it pinned, the probes it used and how long it took.
    let field = |event: &CapturedEvent, name: &str| {
        event
            .fields
            .get(name)
            .map(|v| v.trim_matches('"').to_string())
            .unwrap_or_else(|| panic!("no `{name}` on {event:?}"))
    };
    let started = events
        .iter()
        .find(|e| e.message().starts_with("isolating a failed batch"))
        .unwrap_or_else(|| panic!("no isolation start event: {events:#?}"));
    assert_eq!(started.level, tracing::Level::INFO);
    assert_eq!(field(started, "records"), "1");
    let finished = events
        .iter()
        .find(|e| e.message() == "isolation finished")
        .unwrap_or_else(|| panic!("no isolation end event: {events:#?}"));
    assert_eq!(finished.level, tracing::Level::INFO);
    assert_eq!(field(finished, "outcome"), "Evicted");
    assert_eq!(field(finished, "pinned"), "1");
    assert_eq!(field(finished, "probes"), "1");
    assert_eq!(field(finished, "records"), "1");
    assert_eq!(field(finished, "search"), "complete");
    field(finished, "elapsed_ms")
        .parse::<u64>()
        .expect("elapsed_ms is a number");
}

/// Issue #614: when isolation pins a failure on a key that is still below the
/// death threshold, `classify_and_retry` must say so at `WARN`, naming the key
/// and its death count, not log the same "isolation reproduced nothing" line a
/// batch with no attributable key gets. One `drain_once` call over a batch
/// whose only key is malformed charges it one death (of the default 5) and
/// surfaces the failure, so there is nothing to poll for.
#[tokio::test]
async fn a_below_threshold_charge_is_logged_as_a_warning_naming_the_key() {
    let (_guard, captured) = install_capture();

    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut client = connect_raw(db.dsn()).await;
    seed_orders_totals(&db, &client, "spans_charge_totals").await;
    insert_cdc_row(
        &client,
        "seg_0",
        "orders",
        "1",
        "insert",
        Some(r#"{"price":"not-a-number","tax":"1.50"}"#),
    )
    .await;
    let seg_seq = seal_active_segment(&mut client).await;

    let result = apply::drain_once(
        &db.pool,
        seg_seq,
        "span_test_worker",
        1,
        "trellis_span_test",
        &StagedWatermark::saturated(),
    )
    .await;
    assert!(
        result.is_err(),
        "a below-threshold charge surfaces the original failure, got {result:?}"
    );

    let events = captured.events();
    assert!(
        !events
            .iter()
            .any(|e| e.message().contains("isolation reproduced nothing")),
        "isolation did pin key 1, so it must not be reported as reproducing nothing: \
         {events:#?}"
    );
    let charged = events
        .iter()
        .find(|e| {
            e.level == tracing::Level::WARN
                && e.message().contains("still below the death threshold")
        })
        .unwrap_or_else(|| panic!("expected a WARN event naming the charged key: {events:#?}"));
    let expected = format!("{DEFAULT_SCHEMA}.orders key=1 (1/5 deaths)");
    assert_eq!(
        charged.fields.get("charged").map(String::as_str),
        Some(expected.as_str())
    );
    assert!(
        charged
            .fields
            .get("error")
            .is_some_and(|e| e.contains("not-a-number")),
        "the original failure rides along on the same line: {charged:#?}"
    );
}

/// Issue #56's backfill status transition events (#55's lifecycle):
/// `waiting_to_backfill` -> `backfilling` from the staging worker's
/// reconcile pass (`trellis::client::reconcile_pass`) dispatching a
/// plain 1-1 definition's chunks (ADR-0016), then `backfilling` ->
/// `catching_up` from its last chunk finishing and `catching_up` -> `live`
/// from its go-live catch-up's discharge (issue #476) — mirrors `transform_status_lifecycle.rs`'s
/// `a_fresh_transform_waits_on_the_xmin_fence_then_reaches_live`'s straggler
/// setup, trimmed to just the settle-and-discharge half this test cares
/// about (that file already covers the full status/data correctness story;
/// this one only adds the tracing assertion).
#[tokio::test]
async fn backfill_status_transitions_emit_info_events() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let mut raw = connect_raw(db.dsn()).await;

    raw.batch_execute(
        "create table s (id bigint primary key, a numeric); \
         insert into s (id, a) select g, g from generate_series(1, 5) g",
    )
    .await
    .expect("seed source table");

    let straggler = OpenTransaction::begin(db.dsn()).await;
    straggler.execute("select txid_current()").await;

    let cols = numeric_columns(&["a"]);
    let def = install_definition(
        &db.pool,
        "TRANSFORM t FROM s SELECT a + a AS x",
        &cols,
        "public",
    )
    .await
    .expect("install_definition defers instead of racing the fence");
    assert_eq!(def.status, TransformStatus::WaitingToBackfill);

    straggler.commit().await;

    // Only install the tracing capture around the settled pass:
    // `install_definition` above only registers, so there is nothing to
    // capture yet.
    let (_guard, captured) = install_capture();
    trellis::client::reconcile_pass(
        &mut raw,
        &db.pool,
        DEFAULT_SCHEMA,
        "wake",
        std::time::Duration::from_secs(5),
    )
    .await
    .expect("reconcile pass (settled)");
    trellis::intake::publication::settle_registrations(&db.pool).await;

    let events = captured.events();
    let to_backfilling = events.iter().find(|e| {
        e.level == tracing::Level::INFO
            && e.message().contains("transform status transition")
            && e.fields.get("to").map(String::as_str) == Some("backfilling")
    });
    assert!(
        to_backfilling.is_some(),
        "expected an info event for waiting_to_backfill -> backfilling: {events:#?}"
    );
    assert_eq!(
        to_backfilling
            .unwrap()
            .fields
            .get("from")
            .map(String::as_str),
        Some("waiting_to_backfill")
    );

    // Issue #476: the build's completion leaves it `catching_up`, and the
    // discharge of its go-live catch-up takes it `live`.
    let to_catching_up = events.iter().find(|e| {
        e.level == tracing::Level::INFO
            && e.message().contains("transform status transition")
            && e.fields.get("to").map(String::as_str) == Some("catching_up")
    });
    assert!(
        to_catching_up.is_some(),
        "expected an info event for backfilling -> catching_up: {events:#?}"
    );
    assert_eq!(
        to_catching_up
            .unwrap()
            .fields
            .get("from")
            .map(String::as_str),
        Some("backfilling")
    );

    let to_live = events.iter().find(|e| {
        e.level == tracing::Level::INFO
            && e.message().contains("transform status transition")
            && e.fields.get("to").map(String::as_str) == Some("live")
    });
    assert!(
        to_live.is_some(),
        "expected an info event for catching_up -> live: {events:#?}"
    );
    assert_eq!(
        to_live.unwrap().fields.get("from").map(String::as_str),
        Some("catching_up")
    );
}
