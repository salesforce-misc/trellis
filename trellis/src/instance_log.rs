//! The instance every engine log event names (issue #874, epic #806).
//!
//! Several handles can share one process and one database, each with its own
//! catalog schema, and they all log through the one `tracing` facade. Every
//! event the engine emits therefore carries a `trellis_instance` field: the
//! instance's database and catalog schema, as `mydb/trellis` ([`name_of`]).
//! A host that routes, filters or counts log lines can tell which handle a
//! line came from.
//!
//! # How the field is attached
//!
//! The engine's event sites sit deep inside functions that take only a
//! database client, with no handle or schema in reach, so the name is not
//! passed down as an argument. Each handle instead *scopes* the code it
//! runs, and the event macros in this module read the scope:
//!
//! - **Threads the handle owns.** [`Client`](crate::client::Client)'s runtime
//!   threads (the thread that calls `block_on`, the workers and the blocking
//!   pool) carry the name for their whole life, so every task spawned onto
//!   that runtime logs under it.
//! - **Calls on the host's runtime.** Each [`Trellis`](crate::app::Trellis)
//!   method runs inside [`scoped`], which sets the name around every poll of
//!   its future, so it holds wherever the host's scheduler moves the task.
//!   [`BlockingTrellis`](crate::blocking::BlockingTrellis) runs its calls
//!   through those methods. A task such a method spawns is wrapped with
//!   [`in_current_instance`], or names its instance itself.
//!
//! The scope is a thread-local rather than a `tracing` span on purpose. A
//! span would sit open for the handle's whole life, and an OpenTelemetry
//! layer would then put every span the handle ever opens into one trace. It
//! would also need the embed log bridge, which records events only, to read
//! spans. An event field reaches every subscriber as it is.
//!
//! # The macros
//!
//! [`error!`], [`warn!`], [`info!`], [`debug!`] and [`trace!`] are the
//! `tracing` macros of the same names with `trellis_instance` put first, and
//! take the same arguments. Engine code uses them in place of `tracing`'s own
//! (`crate::instance_log::warn!(...)`), and the `no_event_bypasses_the_instance_field`
//! test fails on a `tracing` event macro anywhere else in this crate, so a new
//! event cannot go out without the field. An event emitted outside any scope
//! (a test calling an engine function directly) reads [`UNSCOPED`].

use std::cell::RefCell;
use std::fmt;
use std::future::Future;
use std::sync::Arc;

use crate::config::Config;

/// What [`Current`] prints on a thread no handle has scoped.
pub(crate) const UNSCOPED: &str = "unknown";

thread_local! {
    static CURRENT: RefCell<Option<Arc<str>>> = const { RefCell::new(None) };
}

/// The name an instance logs under: its database and catalog schema, as
/// `<database>/<schema>` (`mydb/trellis`). Two instances in one process can
/// share a schema name in different databases, so the schema alone does not
/// tell them apart.
pub(crate) fn name_of(config: &Config) -> Arc<str> {
    Arc::from(format!("{}/{}", database_of(config.dsn()), config.schema()))
}

/// The database `dsn` connects to, as `tokio_postgres` resolves it: its
/// `dbname`, else the user name (Postgres's default database), else the OS
/// user `tokio_postgres` connects as. A DSN that does not parse never
/// connects, and reads [`UNSCOPED`].
fn database_of(dsn: &str) -> String {
    let Ok(parsed) = crate::config::parse_dsn(dsn) else {
        return UNSCOPED.to_string();
    };
    match parsed.get_dbname().or(parsed.get_user()) {
        Some(database) => database.to_string(),
        None => whoami::username().unwrap_or_else(|_| UNSCOPED.to_string()),
    }
}

/// The current thread's instance name, as a `tracing` field value. The
/// macros evaluate it only for an event some subscriber wants.
pub(crate) struct Current;

impl fmt::Display for Current {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `try_with`: an event during thread teardown must not panic.
        CURRENT
            .try_with(|current| match &*current.borrow() {
                Some(name) => f.write_str(name),
                None => f.write_str(UNSCOPED),
            })
            .unwrap_or_else(|_| f.write_str(UNSCOPED))
    }
}

/// Puts `name` on the current thread for the rest of its life. For the
/// threads a handle owns: its runtime's workers and blocking pool, and the
/// thread that drives the runtime.
pub(crate) fn enter_thread(name: &Arc<str>) {
    let _ = CURRENT.try_with(|current| *current.borrow_mut() = Some(Arc::clone(name)));
}

/// The name set on the current thread, to carry onto another one.
fn current_name() -> Option<Arc<str>> {
    CURRENT
        .try_with(|current| current.borrow().clone())
        .ok()
        .flatten()
}

/// Restores the previous name when dropped, also on a panic.
struct Restore(Option<Arc<str>>);

impl Restore {
    fn set(name: &Arc<str>) -> Restore {
        let previous = CURRENT
            .try_with(|current| current.borrow_mut().replace(Arc::clone(name)))
            .ok()
            .flatten();
        Restore(previous)
    }
}

impl Drop for Restore {
    fn drop(&mut self) {
        let previous = self.0.take();
        let _ = CURRENT.try_with(|current| *current.borrow_mut() = previous);
    }
}

/// Runs `future` with `name` as the instance on every poll, so the name
/// follows the task across `.await`s and across the threads a multi-thread
/// scheduler moves it between.
pub(crate) async fn scoped<F: Future>(name: Arc<str>, future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(move |cx| {
        let _restore = Restore::set(&name);
        future.as_mut().poll(cx)
    })
    .await
}

/// `future`, carrying the instance of the thread that calls this, for a
/// `tokio::spawn` that may run on a thread of the host's runtime. Unchanged
/// when this thread has no instance.
pub(crate) fn in_current_instance<F: Future>(future: F) -> impl Future<Output = F::Output> {
    let name = current_name();
    async move {
        match name {
            Some(name) => scoped(name, future).await,
            None => future.await,
        }
    }
}

macro_rules! error_event {
    ($($arg:tt)*) => {
        ::tracing::error!(trellis_instance = %$crate::instance_log::Current, $($arg)*)
    };
}
macro_rules! warn_event {
    ($($arg:tt)*) => {
        ::tracing::warn!(trellis_instance = %$crate::instance_log::Current, $($arg)*)
    };
}
macro_rules! info_event {
    ($($arg:tt)*) => {
        ::tracing::info!(trellis_instance = %$crate::instance_log::Current, $($arg)*)
    };
}
macro_rules! debug_event {
    ($($arg:tt)*) => {
        ::tracing::debug!(trellis_instance = %$crate::instance_log::Current, $($arg)*)
    };
}
#[allow(unused_macros)]
macro_rules! trace_event {
    ($($arg:tt)*) => {
        ::tracing::trace!(trellis_instance = %$crate::instance_log::Current, $($arg)*)
    };
}

#[allow(unused_imports)]
pub(crate) use debug_event as debug;
pub(crate) use error_event as error;
pub(crate) use info_event as info;
#[allow(unused_imports)]
pub(crate) use trace_event as trace;
// Aliased: a macro named `warn` is ambiguous with the built-in attribute.
pub(crate) use warn_event as warn;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use tracing::field::{Field, Visit};
    use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

    use super::*;

    /// The `trellis_instance` of every event, in order.
    #[derive(Clone, Default)]
    struct Instances(Arc<Mutex<Vec<String>>>);

    struct FieldVisitor(HashMap<String, String>);

    impl Visit for FieldVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            self.0
                .insert(field.name().to_string(), format!("{value:?}"));
        }
    }

    impl<S: tracing::Subscriber> Layer<S> for Instances {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            let mut visitor = FieldVisitor(HashMap::new());
            event.record(&mut visitor);
            self.0.lock().unwrap().push(
                visitor
                    .0
                    .remove("trellis_instance")
                    .unwrap_or_else(|| "MISSING".to_string()),
            );
        }
    }

    fn capture<R>(run: impl FnOnce() -> R) -> (R, Vec<String>) {
        let instances = Instances::default();
        let subscriber = tracing_subscriber::registry().with(instances.clone());
        let result = tracing::subscriber::with_default(subscriber, run);
        let seen = instances.0.lock().unwrap().clone();
        (result, seen)
    }

    #[test]
    fn an_event_carries_the_threads_instance_or_unknown() {
        let (_, seen) = capture(|| {
            warn!("outside any scope");
            std::thread::spawn(|| {
                // The `with_default` above is this thread's alone.
            })
            .join()
            .unwrap();
            let name: Arc<str> = Arc::from("tenant_a");
            let _restore = Restore::set(&name);
            info!(table = "public.t", "inside");
        });
        assert_eq!(seen, [UNSCOPED, "tenant_a"]);
    }

    /// The name follows a task across awaits and across the threads of a
    /// multi-thread runtime, and does not leak onto a task that shares one of
    /// those threads.
    #[test]
    fn a_scoped_future_keeps_its_name_across_awaits_and_threads() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut tasks = Vec::new();
            for name in ["a", "b", "c", "d"] {
                tasks.push(tokio::spawn(scoped(Arc::from(name), async move {
                    for _ in 0..20 {
                        tokio::task::yield_now().await;
                        let seen = Current.to_string();
                        assert_eq!(seen, name);
                    }
                })));
            }
            for task in tasks {
                task.await.unwrap();
            }
            // A plain task on the same workers has no instance.
            tokio::spawn(async {
                assert_eq!(Current.to_string(), UNSCOPED);
            })
            .await
            .unwrap();
        });
    }

    #[test]
    fn in_current_instance_carries_the_name_onto_a_spawned_task() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .unwrap();
        runtime.block_on(async {
            let seen = scoped(Arc::from("tenant_a"), async {
                // Called inside the scope, as a method's spawn is.
                tokio::spawn(in_current_instance(async { Current.to_string() })).await
            })
            .await
            .unwrap();
            assert_eq!(seen, "tenant_a");
            let seen = tokio::spawn(in_current_instance(async { Current.to_string() }))
                .await
                .unwrap();
            assert_eq!(seen, UNSCOPED);
        });
    }

    #[test]
    fn a_panic_inside_a_scope_restores_the_previous_name() {
        let outer: Arc<str> = Arc::from("outer");
        let _outer = Restore::set(&outer);
        let result = std::panic::catch_unwind(|| {
            let inner: Arc<str> = Arc::from("inner");
            let _inner = Restore::set(&inner);
            panic!("unwinding");
        });
        assert!(result.is_err());
        assert_eq!(Current.to_string(), "outer");
    }

    /// The name is the database and the schema, with the database resolved
    /// as `tokio_postgres` connects: two instances that share a schema name
    /// in different databases log apart.
    #[test]
    fn the_name_is_the_database_and_the_catalog_schema() {
        let name = |dsn: &str, schema: &str| {
            name_of(&Config::with_schema(dsn, schema).expect("config")).to_string()
        };
        assert_eq!(
            name("postgres://app@db.local/tenant_a", "trellis"),
            "tenant_a/trellis"
        );
        assert_eq!(
            name("postgres://app@db.local/tenant_b", "trellis"),
            "tenant_b/trellis"
        );
        assert_eq!(
            name("host=db.local user=app dbname='my db'", "ops"),
            "my db/ops"
        );
        // No dbname: Postgres connects to the user's database.
        assert_eq!(name("host=db.local user=app", "trellis"), "app/trellis");
        // No user either: `tokio_postgres` connects as the OS user.
        assert_eq!(
            name("host=db.local", "trellis"),
            format!("{}/trellis", whoami::username().expect("OS user"))
        );
    }

    fn rust_files(dir: &Path, into: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                rust_files(&path, into);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                into.push(path);
            }
        }
    }

    /// Every event macro of the engine is this module's. A `tracing::warn!`
    /// (or `use tracing::warn`) would put a line out without the instance.
    #[test]
    fn no_event_bypasses_the_instance_field() {
        const LEVELS: [&str; 6] = ["error", "warn", "info", "debug", "trace", "event"];
        let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = Vec::new();
        rust_files(&src, &mut files);
        let this = src.join("instance_log.rs");
        let mut offenders = Vec::new();
        for file in files.iter().filter(|file| **file != this) {
            let text = std::fs::read_to_string(file).unwrap();
            for (n, line) in text.lines().enumerate() {
                let code = line.split("//").next().unwrap_or("");
                let direct = LEVELS
                    .iter()
                    .any(|level| code.contains(&format!("tracing::{level}!")));
                let imported = code.contains("use tracing::")
                    && code
                        .split(|c: char| !c.is_alphanumeric() && c != '_')
                        .any(|word| LEVELS.contains(&word));
                // `use tracing as t;` would let `t::warn!` through.
                let renamed = code.contains("tracing as ");
                if direct || imported || renamed {
                    offenders.push(format!("{}:{}: {}", file.display(), n + 1, line.trim()));
                }
            }
        }
        assert!(
            offenders.is_empty(),
            "emit through crate::instance_log::{{error, warn, info, debug, trace}} so the event \
             carries `trellis_instance`:\n{}",
            offenders.join("\n")
        );
    }
}
