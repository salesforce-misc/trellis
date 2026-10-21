//! The migrated template data directory every ordinary [`TestCluster`] starts
//! from (#1246).
//!
//! `initdb` (~220 ms) and the migrations (~70 ms) run once, not once per
//! test: a throwaway cluster is initialised and migrated into a database
//! called [`TEMPLATE_DB`], stopped cleanly, and its data directory is kept
//! as the template. Each test's cluster is a copy of that directory (~30 ms)
//! with its own postmaster, and [`TestCluster::create_isolated_database`] is
//! `createdb -T trellis_template` inside the copy.
//!
//! The template is cached across processes in the system temp dir as
//! `trellis-template-<key>`, so one `cargo test` run builds it once for all
//! its test binaries. The key hashes the engine's sources and migrations
//! (`build.rs`), the Postgres version and the `initdb` arguments, so a stale
//! template is never reused; a builder holds an exclusive lock on
//! `trellis-template-<key>.lock` and publishes the directory with a rename, so
//! a reader sees it whole or not at all. Templates for keys no run has used
//! for a day are deleted by the next builder.
//!
//! The reaper ([`reap_orphans_in`]) only looks at `trellis-testkit-*`, so it
//! never sweeps a template.

use super::*;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::OnceLock;

/// The migrated database inside the template, cloned for each isolated one.
pub(super) const TEMPLATE_DB: &str = "trellis_template";

const TEMPLATE_PREFIX: &str = "trellis-template-";
const STALE_AFTER: Duration = Duration::from_secs(24 * 60 * 60);

/// The template data directory, building it first if no run has yet. Takes
/// no cluster permit, so it is safe to call before acquiring one.
pub(super) fn data_dir() -> &'static Path {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| ensure(&std::env::temp_dir()))
}

fn key() -> String {
    let postgres = Command::new("postgres")
        .arg("--version")
        .output()
        .map(|out| out.stdout)
        .unwrap_or_default();
    let mut hasher = DefaultHasher::new();
    postgres.hash(&mut hasher);
    for arg in initdb_command(Path::new("DATA")).get_args() {
        arg.hash(&mut hasher);
    }
    format!(
        "{}-{:016x}",
        env!("TRELLIS_SOURCE_FINGERPRINT"),
        hasher.finish()
    )
}

/// The template under `tmp`, built if absent.
fn ensure(tmp: &Path) -> PathBuf {
    let name = format!("{TEMPLATE_PREFIX}{}", key());
    let dir = tmp.join(&name);
    if !dir.exists() {
        // Held until the end of the build; another process wanting the same
        // template waits here and finds it done.
        let lock = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(tmp.join(format!("{name}.lock")))
            .expect("create template lock file");
        lock.lock().expect("lock the template");
        if !dir.exists() {
            build(tmp, &name, &dir);
        }
    }
    mark_used(&dir);
    dir
}

fn build(tmp: &Path, name: &str, dir: &Path) {
    delete_stale(tmp);
    // A build killed after the copy and before the rename.
    let staging = tmp.join(format!("{name}.tmp-{}", std::process::id()));
    let _ = fs::remove_dir_all(&staging);

    // The build cluster is transient, so it lives in the system temp dir
    // whatever `tmp` is: its socket path must stay short.
    let cluster =
        TestCluster::start_listening_in(&std::env::temp_dir(), false, DataDirSource::Initdb);
    cluster.create_database(TEMPLATE_DB);
    migrate(&cluster.database_dsn(TEMPLATE_DB));
    // A clean shutdown first, so the copy needs no recovery on first start.
    cluster.while_stopped(StopMode::Fast, || {
        copy_data_dir(&cluster.data_dir, &staging)
    });
    drop(cluster);
    fs::rename(&staging, dir).expect("publish the template");
}

fn migrate(dsn: &str) {
    // Own thread and runtime: the caller may be inside a tokio runtime.
    std::thread::scope(|scope| {
        scope.spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build runtime")
                .block_on(async {
                    let config = Config::from_dsn(dsn.to_string()).expect("valid dsn");
                    let pool = Pool::new(&config).expect("pool");
                    super::migrate(&pool, &config)
                        .await
                        .expect("migrate the template");
                });
        });
    });
}

/// Records that a run is using the template now, so [`delete_stale`] spares it.
fn mark_used(dir: &Path) {
    let _ = fs::File::open(dir).and_then(|dir| dir.set_modified(std::time::SystemTime::now()));
}

/// Removes templates (and their lock and staging files) that no run has used
/// for [`STALE_AFTER`], so keys from old engine builds do not pile up.
fn delete_stale(tmp: &Path) {
    let Ok(entries) = fs::read_dir(tmp) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        if !name.to_string_lossy().starts_with(TEMPLATE_PREFIX) {
            continue;
        }
        let path = entry.path();
        if older_than(&path, STALE_AFTER) {
            let _ = fs::remove_dir_all(&path).or_else(|_| fs::remove_file(&path));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_key_is_stable_within_a_build() {
        assert_eq!(key(), key());
    }

    #[test]
    fn a_template_is_built_once_and_a_second_caller_reuses_it() {
        let sandbox = fresh_sandbox_dir("template");
        let first = ensure(&sandbox);
        let built = fs::metadata(&first).and_then(|m| m.modified()).unwrap();
        std::thread::sleep(Duration::from_millis(20));
        let second = ensure(&sandbox);
        assert_eq!(first, second);
        // A reuse touches the directory but builds no other.
        let templates = fs::read_dir(&sandbox)
            .unwrap()
            .flatten()
            .filter(|e| e.path().is_dir())
            .count();
        assert_eq!(templates, 1, "no staging or build directory left behind");
        assert!(fs::metadata(&second).and_then(|m| m.modified()).unwrap() >= built);
        assert!(second.join("PG_VERSION").exists());
        let _ = fs::remove_dir_all(&sandbox);
    }

    #[test]
    fn the_reaper_leaves_a_template_alone() {
        let sandbox = fresh_sandbox_dir("reaper");
        let template = sandbox.join(format!("{TEMPLATE_PREFIX}abc-def"));
        fs::create_dir_all(template.join("PG_VERSION_DIR")).unwrap();
        reap_orphans_in(&sandbox);
        assert!(template.exists());
        let _ = fs::remove_dir_all(&sandbox);
    }

    #[test]
    fn only_templates_unused_for_a_day_are_deleted() {
        let sandbox = fresh_sandbox_dir("stale");
        let old = sandbox.join(format!("{TEMPLATE_PREFIX}old"));
        let fresh = sandbox.join(format!("{TEMPLATE_PREFIX}fresh"));
        let other = sandbox.join("unrelated");
        for dir in [&old, &fresh, &other] {
            fs::create_dir_all(dir).unwrap();
        }
        let status = Command::new("touch")
            .args(["-t", "202001010000"])
            .arg(&old)
            .status()
            .unwrap();
        assert!(status.success());
        delete_stale(&sandbox);
        assert!(!old.exists());
        assert!(fresh.exists());
        assert!(other.exists());
        let _ = fs::remove_dir_all(&sandbox);
    }

    fn fresh_sandbox_dir(tag: &str) -> PathBuf {
        let sandbox =
            std::env::temp_dir().join(format!("trellis-template-test-{}-{tag}", unique_suffix()));
        fs::create_dir_all(&sandbox).unwrap();
        sandbox
    }
}
