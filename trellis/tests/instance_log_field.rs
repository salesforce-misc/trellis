//! Issue #874 (epic #806): every engine log event names the instance that
//! emitted it, in a `trellis_instance` field.
//!
//! Two instances share one process and one database, each with its own
//! catalog schema, so each logs as `<database>/<schema>`. One runs as an async [`Trellis`] on the test's runtime,
//! the other as a [`BlockingTrellis`] with a runtime of its own, and both run
//! a staging worker. A single global subscriber captures what the whole
//! process logs, and the assertion is about *every* event from the engine,
//! not a sampled few: none lacks the field, none names an instance but its
//! own.
//!
//! The events come from the paths that differ in how they reach the name:
//! the staging worker's first capture pass (a task on the client's runtime,
//! finished before `connect` returns), and `apply` (a call on the host's
//! runtime for the async handle, and on the blocking handle's runtime for the
//! other). Nothing here waits for convergence.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use testkit::TestCluster;
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use trellis::config::DEFAULT_SCHEMA;
use trellis::{BlockingTrellis, Config, Trellis, TrellisOptions};

const INSTANCE_B: &str = "instance_b";

#[derive(Debug, Clone)]
struct Event {
    target: String,
    fields: HashMap<String, String>,
}

impl Event {
    fn message(&self) -> &str {
        self.fields.get("message").map_or("", String::as_str)
    }

    fn instance(&self) -> Option<&str> {
        self.fields.get("trellis_instance").map(String::as_str)
    }
}

struct Fields(HashMap<String, String>);

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<Event>>>);

impl<S: tracing::Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut fields = Fields(HashMap::new());
        event.record(&mut fields);
        self.0.lock().unwrap().push(Event {
            target: event.metadata().target().to_string(),
            fields: fields.0,
        });
    }
}

impl Capture {
    /// What the engine logged: its own crate's events, not `tokio_postgres`'s.
    fn engine_events(&self) -> Vec<Event> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.target.starts_with("trellis::"))
            .cloned()
            .collect()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_engine_event_names_its_own_instance() {
    let capture = Capture::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(capture.clone()))
        .expect("this binary's one test installs the global subscriber");

    let cluster = TestCluster::start();
    let db = cluster.create_empty_database().await;
    let config_a = Config::with_schema(db.dsn().to_string(), DEFAULT_SCHEMA).expect("schema A");
    let config_b = Config::with_schema(db.dsn().to_string(), INSTANCE_B).expect("schema B");

    let raw = db.pool.get().await.expect("connection");
    raw.batch_execute(
        "create table a_src (id bigint primary key, price numeric); \
         create table b_src (id bigint primary key, price numeric)",
    )
    .await
    .expect("create the sources");
    // Each instance logs as its database and catalog schema.
    let database: String = raw
        .query_one("select current_database()::text", &[])
        .await
        .expect("database name")
        .get(0);
    let name_a = format!("{database}/{DEFAULT_SCHEMA}");
    let name_b = format!("{database}/{INSTANCE_B}");

    // Define one transform per instance, each reading its own source, with no
    // background work yet.
    for (config, source) in [(&config_a, "a_src"), (&config_b, "b_src")] {
        let definer = Trellis::connect(config.clone(), TrellisOptions::default())
            .await
            .expect("connect a define-only handle");
        definer.migrate().await.expect("migrate");
        definer
            .apply(&format!(
                "TRANSFORM {source}_doubled FROM {source} SELECT price + price AS total"
            ))
            .await
            .expect("define");
        definer.shutdown().await.expect("shutdown definer");
    }

    // The staging workers' first capture pass installs capture on each
    // instance's source before `connect` returns.
    let staging = TrellisOptions {
        staging: true,
        ..TrellisOptions::default()
    };
    let a = Trellis::connect(config_a, staging.clone())
        .await
        .expect("connect A");
    // A thread with no runtime of its own, as `BlockingTrellis` requires.
    let b = std::thread::spawn(move || BlockingTrellis::connect(config_b, staging))
        .join()
        .expect("join")
        .expect("connect B");

    // A PAUSE and a repeated PAUSE: a status transition and a no-op, each
    // logged by the call itself.
    a.apply("PAUSE TRANSFORM a_src_doubled").await.expect("A");
    a.apply("PAUSE TRANSFORM a_src_doubled").await.expect("A");
    std::thread::spawn(move || {
        b.apply("PAUSE TRANSFORM b_src_doubled").expect("B");
        b.apply("PAUSE TRANSFORM b_src_doubled").expect("B");
        b.shutdown().expect("shutdown B");
    })
    .join()
    .expect("join");
    a.shutdown().await.expect("shutdown A");

    let events = capture.engine_events();
    assert!(!events.is_empty(), "the engine logged nothing");

    let nameless: Vec<_> = events
        .iter()
        .filter(|event| event.instance().is_none_or(|name| name == "unknown"))
        .map(|event| format!("{} {:?}", event.target, event.message()))
        .collect();
    assert!(
        nameless.is_empty(),
        "events without an instance:\n{}",
        nameless.join("\n")
    );

    // Each event is about its own instance's table or transform, and says so.
    let mut seen: HashMap<&str, Vec<&str>> = HashMap::new();
    for event in &events {
        let instance = event.instance().expect("checked above");
        let about_a = ["a_src", "a_src_doubled"];
        let about_b = ["b_src", "b_src_doubled"];
        for field in ["table", "transform"] {
            let Some(subject) = event.fields.get(field) else {
                continue;
            };
            let subject = subject.trim_matches('"');
            let subject = subject.strip_prefix("public.").unwrap_or(subject);
            let expected = if about_a.contains(&subject) {
                name_a.as_str()
            } else if about_b.contains(&subject) {
                name_b.as_str()
            } else {
                continue;
            };
            assert_eq!(
                instance,
                expected,
                "{field}={subject} logged under the wrong instance: {} {:?}",
                event.target,
                event.message()
            );
            seen.entry(expected).or_default().push(field);
        }
    }

    // Both instances logged from both kinds of path: the capture pass logs a
    // `table`, `apply` a `transform`.
    for instance in [name_a.as_str(), name_b.as_str()] {
        let fields = seen.get(instance).map(Vec::as_slice).unwrap_or(&[]);
        assert!(
            fields.contains(&"table"),
            "no capture-pass event for {instance}: {events:#?}"
        );
        assert!(
            fields.contains(&"transform"),
            "no apply event for {instance}: {events:#?}"
        );
    }
}
