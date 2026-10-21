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
//! (`build.rs`), the Postgres binary and the `initdb` arguments, so a stale
//! template is never reused, and the day, so none outlives a temp-dir cleaner
//! (see [`key`]). A builder holds an exclusive lock on
//! `trellis-template-<key>.lock` and publishes the directory with a rename, so
//! a reader sees it whole or not at all. Every process that uses a template
//! holds a shared lock on that file for as long as it runs, and a builder
//! deletes the other templates that no process holds and none has used for
//! an hour, so templates of earlier engine builds do not fill the temp dir
//! (each is ~50 MB, and every edit to the engine makes a new one).
//!
//! The reaper ([`reap_orphans_in`]) only looks at `trellis-testkit-*`, so it
//! never sweeps a template.

use super::*;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::OnceLock;

/// The migrated database inside the template, cloned for each isolated one.
pub(super) const TEMPLATE_DB: &str = "trellis_template";

const TEMPLATE_PREFIX: &str = "trellis-template-";
/// How long a template no process holds must go unused before a builder
/// deletes it. Only the gap between test binaries of one run needs covering.
const STALE_AFTER: Duration = Duration::from_secs(60 * 60);
/// How old a template's lock file must be before a builder deletes it: older
/// than any key a process could still compute (see [`key`]'s day), so no
/// process can be about to open it.
const LOCK_STALE_AFTER: Duration = Duration::from_secs(3 * 24 * 60 * 60);

/// The template data directory, building it first if no run has yet. Takes
/// no cluster permit, so it is safe to call before acquiring one.
pub(super) fn data_dir() -> &'static Path {
    // The lock file is kept open, its shared lock held, until the process
    // exits.
    static TEMPLATE: OnceLock<(PathBuf, fs::File)> = OnceLock::new();
    &TEMPLATE.get_or_init(|| ensure(&std::env::temp_dir())).0
}

/// The cache key: the engine build (`build.rs`), the Postgres binaries, the
/// `initdb` arguments, the user and the day.
///
/// The `postgres` binary's path, size and mtime are in it as well as its
/// version, so a rebuild of the same version (against a new ICU, say) or a
/// different installation on `PATH` gets its own template. The user keeps
/// one user's template (mode 0700) from blocking another's on a shared temp
/// dir. The day bounds a template's age to two days, so a temp-dir cleaner
/// that deletes files by age (systemd-tmpfiles, macOS's) never finds one old
/// enough to empty while it is still in use.
fn key() -> String {
    let postgres = Command::new("postgres")
        .arg("--version")
        .output()
        .map(|out| out.stdout)
        .unwrap_or_default();
    let mut hasher = DefaultHasher::new();
    postgres.hash(&mut hasher);
    if let Some(binary) = on_path("postgres") {
        binary.hash(&mut hasher);
        if let Ok(meta) = fs::metadata(&binary) {
            meta.len().hash(&mut hasher);
            meta.modified().ok().hash(&mut hasher);
        }
    }
    for arg in initdb_command(Path::new("DATA")).get_args() {
        arg.hash(&mut hasher);
    }
    std::env::var_os("USER").hash(&mut hasher);
    let day = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_secs() / (24 * 60 * 60));
    format!(
        "{}-{:016x}-{day}",
        env!("TRELLIS_SOURCE_FINGERPRINT"),
        hasher.finish()
    )
}

/// The first `program` on `PATH`, as a spawn would find it.
fn on_path(program: &str) -> Option<PathBuf> {
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|dir| dir.join(program))
        .find(|path| path.is_file())
}

/// The template under `tmp`, built if absent, with its lock file, on which
/// this process now holds a shared lock: while it does, no builder deletes
/// the template.
fn ensure(tmp: &Path) -> (PathBuf, fs::File) {
    let name = format!("{TEMPLATE_PREFIX}{}", key());
    let dir = tmp.join(&name);
    let lock = open_lock(tmp, &name);
    // Waits out a build in progress.
    lock.lock_shared().expect("lock the template shared");
    if !dir.exists() {
        // Exclusive for the build; another process wanting the same template
        // waits for it and finds it done.
        lock.unlock().expect("unlock the template");
        lock.lock().expect("lock the template");
        if !dir.exists() {
            build(tmp, &name, &dir);
        }
        lock.lock_shared().expect("lock the template shared");
    }
    mark_used(&dir);
    (dir, lock)
}

/// The lock file for template `name`, created if missing. Never created
/// exclusively, so every opener shares one file.
fn open_lock(tmp: &Path, name: &str) -> fs::File {
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(tmp.join(format!("{name}.lock")))
        .expect("create template lock file")
}

fn build(tmp: &Path, name: &str, dir: &Path) {
    delete_stale(tmp, name);
    // A build killed after the copy and before the rename leaves its staging
    // directory. Under the lock no other build of this key is running, so
    // every one there is such a leftover.
    let staging_prefix = format!("{name}.tmp-");
    if let Ok(entries) = fs::read_dir(tmp) {
        for entry in entries.flatten() {
            if entry
                .file_name()
                .to_string_lossy()
                .starts_with(&staging_prefix)
            {
                let _ = fs::remove_dir_all(entry.path());
            }
        }
    }
    let staging = tmp.join(format!("{staging_prefix}{}", std::process::id()));

    // The build cluster is transient, so it lives in the system temp dir
    // whatever `tmp` is: its socket path must stay short.
    let cluster = TestCluster::start_listening_in(
        &std::env::temp_dir(),
        false,
        DataDirSource::Initdb,
        Vec::new(),
    );
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
/// Called on every copy, not only when a process first finds the template,
/// so the age [`delete_stale`] goes by is the time since the last copy.
pub(super) fn mark_used(dir: &Path) {
    let _ = fs::File::open(dir).and_then(|dir| dir.set_modified(std::time::SystemTime::now()));
}

/// Removes the templates other than `own` that no process holds and none has
/// used for [`STALE_AFTER`], staging directories of builds killed that long
/// ago, and lock files older than [`LOCK_STALE_AFTER`] that nothing holds.
fn delete_stale(tmp: &Path, own: &str) {
    let Ok(entries) = fs::read_dir(tmp) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with(TEMPLATE_PREFIX) || name.starts_with(own) {
            continue;
        }
        let path = entry.path();
        if let Some(template) = name.strip_suffix(".lock") {
            if older_than(&path, LOCK_STALE_AFTER) && !held(tmp, template) {
                let _ = fs::remove_file(&path);
            }
        } else if name.contains(".tmp-") {
            if older_than(&path, STALE_AFTER) {
                let _ = fs::remove_dir_all(&path);
            }
        } else if older_than(&path, STALE_AFTER) && !held(tmp, &name) {
            let _ = fs::remove_dir_all(&path);
        }
    }
}

/// Whether some process holds template `name`'s lock. Taking it exclusively
/// and letting it go at once tells, since every user holds it shared.
fn held(tmp: &Path, name: &str) -> bool {
    open_lock(tmp, name).try_lock().is_err()
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
        let (first, _first_lock) = ensure(&sandbox);
        let built = fs::metadata(&first).and_then(|m| m.modified()).unwrap();
        std::thread::sleep(Duration::from_millis(20));
        let (second, _second_lock) = ensure(&sandbox);
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
    fn a_build_removes_the_staging_directory_a_killed_build_left() {
        let sandbox = fresh_sandbox_dir("staging");
        let leftover = sandbox.join(format!("{TEMPLATE_PREFIX}{}.tmp-999999999", key()));
        fs::create_dir_all(leftover.join("base")).unwrap();
        let (template, _lock) = ensure(&sandbox);
        assert!(!leftover.exists());
        assert!(template.join("PG_VERSION").exists());
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
    fn a_builder_deletes_only_old_templates_no_process_holds() {
        let sandbox = fresh_sandbox_dir("stale");
        let path = |name: &str| sandbox.join(format!("{TEMPLATE_PREFIX}{name}"));
        let (old, held_old, fresh, staging, other) = (
            path("old"),
            path("held"),
            path("fresh"),
            path("x.tmp-1"),
            sandbox.join("unrelated"),
        );
        for dir in [&old, &held_old, &fresh, &staging, &other] {
            fs::create_dir_all(dir).unwrap();
        }
        let holder = open_lock(&sandbox, &format!("{TEMPLATE_PREFIX}held"));
        holder.lock_shared().unwrap();
        let old_lock = sandbox.join(format!("{TEMPLATE_PREFIX}gone.lock"));
        fs::File::create(&old_lock).unwrap();
        for stale in [&old, &held_old, &staging, &other, &old_lock] {
            let status = Command::new("touch")
                .args(["-t", "202001010000"])
                .arg(stale)
                .status()
                .unwrap();
            assert!(status.success());
        }
        delete_stale(&sandbox, &format!("{TEMPLATE_PREFIX}own"));
        assert!(!old.exists(), "an old template nobody holds is deleted");
        assert!(held_old.exists(), "a template a process holds is kept");
        assert!(fresh.exists(), "a recently used template is kept");
        assert!(!staging.exists(), "an old staging directory is deleted");
        assert!(!old_lock.exists(), "an old lock nobody holds is deleted");
        assert!(other.exists(), "nothing else is touched");
        drop(holder);
        let _ = fs::remove_dir_all(&sandbox);
    }

    fn fresh_sandbox_dir(tag: &str) -> PathBuf {
        let sandbox =
            std::env::temp_dir().join(format!("trellis-template-test-{}-{tag}", unique_suffix()));
        fs::create_dir_all(&sandbox).unwrap();
        sandbox
    }
}
