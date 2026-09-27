//! Issue #600: a forked child that connects its own handle must not hang on a
//! process-global lock some parent thread held at the moment of the fork.
//!
//! `fork()` copies only the calling thread, so a lock another thread held is
//! copied held, with nobody left to release it. The crate's globals on a
//! fresh handle's path (the enum type-name interner and the metrics
//! registry) are rebuilt in a forked child instead of inherited; see
//! `src/fork_local.rs`.
//!
//! This file forks, so it runs as a plain `main` (`harness = false` in
//! `Cargo.toml`) rather than inside libtest, whose own threads a child would
//! inherit the locks of too. Each scenario forks one child and waits for it
//! with a deadline; a child that hangs is killed and reported as a failure,
//! so a regression fails this binary instead of hanging the test run.
//!
//! The interner scenarios hold the interner's real lock from a parent thread
//! across the fork, deterministically, through
//! `defs::pg_type::hold_enum_interner_lock`. The metrics registry's locks
//! live inside `metrics-util` and can't be held from outside, so the
//! metrics scenario checks the property that rules the hang out: the
//! child's registry is not the parent's.

#[cfg(unix)]
fn main() {
    unix::main();
}

#[cfg(not(unix))]
fn main() {}

#[cfg(unix)]
mod unix {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    use testkit::TestCluster;
    use trellis::defs::pg_type::{PgType, hold_enum_interner_lock};
    use trellis::{BlockingTrellis, Config, Metrics, TrellisOptions};

    /// Deadline for a child that needs no database.
    const QUICK_CHILD: Duration = Duration::from_secs(20);
    /// Deadline for a child that connects, defines and reads over Postgres.
    /// Generous because `verify` runs this alongside the whole suite.
    const CONNECTING_CHILD: Duration = Duration::from_secs(120);

    type Scenario = fn() -> Result<(), String>;

    pub fn main() {
        let scenarios: [(&str, Scenario); 3] = [
            (
                "interning an enum type while a parent thread holds the interner lock",
                interner_lock_held_at_fork,
            ),
            (
                "a forked child records into its own metrics registry",
                child_gets_its_own_metrics_registry,
            ),
            (
                "a forked child connects and defines over an enum column while a live \
                 parent engine thread holds the interner lock",
                child_connects_while_parent_engine_is_live,
            ),
        ];
        let mut failed = 0;
        for (name, scenario) in scenarios {
            match scenario() {
                Ok(()) => println!("test {name} ... ok"),
                Err(why) => {
                    failed += 1;
                    println!("test {name} ... FAILED\n    {why}");
                }
            }
        }
        if failed > 0 {
            println!("\n{failed} fork scenario(s) failed");
            std::process::exit(1);
        }
    }

    /// Holds the enum interner's lock on a parent thread for as long as the
    /// returned value lives.
    struct HeldInternerLock {
        release: mpsc::Sender<()>,
        holder: thread::JoinHandle<()>,
    }

    impl HeldInternerLock {
        fn hold() -> Self {
            let (held_tx, held_rx) = mpsc::channel();
            let (release, release_rx) = mpsc::channel::<()>();
            let holder = thread::spawn(move || {
                let guard = hold_enum_interner_lock();
                held_tx.send(()).expect("signal the lock is held");
                let _ = release_rx.recv();
                drop(guard);
            });
            held_rx.recv().expect("the holder thread took the lock");
            Self { release, holder }
        }

        fn release(self) {
            let _ = self.release.send(());
            self.holder.join().expect("holder thread");
        }
    }

    /// Forks, runs `body` in the child, and waits up to `deadline` for it.
    /// The child never returns into this process's own code: it `_exit`s
    /// with 0 when `body` returns `Ok`, and 1 otherwise, without running
    /// destructors for anything it inherited (the parent's cluster, pool
    /// connections, runtime).
    fn in_forked_child(
        deadline: Duration,
        body: impl FnOnce() -> Result<(), String>,
    ) -> Result<(), String> {
        // SAFETY: the child runs only `body` and then `_exit`s.
        let pid = unsafe { libc::fork() };
        if pid < 0 {
            return Err(format!("fork failed: {}", std::io::Error::last_os_error()));
        }
        if pid == 0 {
            let code = match catch_unwind(AssertUnwindSafe(body)) {
                Ok(Ok(())) => 0,
                Ok(Err(why)) => {
                    eprintln!("    child: {why}");
                    1
                }
                Err(_) => 1,
            };
            // SAFETY: ends the child without unwinding into the parent's
            // frames or running its inherited destructors.
            unsafe { libc::_exit(code) };
        }

        let started = Instant::now();
        loop {
            let mut status = 0;
            // SAFETY: `pid` is our own child; `status` is a valid out pointer.
            let reaped = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
            if reaped == pid {
                return if libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0 {
                    Ok(())
                } else {
                    Err(format!("child exited abnormally (wait status {status})"))
                };
            }
            if reaped < 0 {
                return Err(format!(
                    "waitpid failed: {}",
                    std::io::Error::last_os_error()
                ));
            }
            if started.elapsed() > deadline {
                // SAFETY: `pid` is our own, still-unreaped child.
                unsafe {
                    libc::kill(pid, libc::SIGKILL);
                    libc::waitpid(pid, &mut status, 0);
                }
                return Err(format!(
                    "child still running after {deadline:?}: it hung on a lock inherited from \
                     the parent, and was killed"
                ));
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn interner_lock_held_at_fork() -> Result<(), String> {
        // Build the parent's interner before the fork, as a running engine
        // would have.
        PgType::from_name("enum:public.parent_status").expect("enum token decodes");

        let held = HeldInternerLock::hold();
        let outcome = in_forked_child(QUICK_CHILD, || {
            let decoded =
                PgType::from_name("enum:public.child_status").ok_or("enum token did not decode")?;
            if decoded.enum_qualified_name() != Some("public.child_status") {
                return Err(format!("decoded the wrong type: {decoded:?}"));
            }
            Ok(())
        });
        held.release();
        outcome
    }

    fn child_gets_its_own_metrics_registry() -> Result<(), String> {
        trellis::metrics::increment_intake_restarts("fork_parent_only");
        let parent = Metrics::new().render_prometheus();
        assert!(parent.contains("fork_parent_only"), "{parent}");

        in_forked_child(QUICK_CHILD, || {
            trellis::metrics::increment_intake_restarts("fork_child_only");
            let child = Metrics::new().render_prometheus();
            if !child.contains("fork_child_only") {
                return Err(format!("the child's own series is missing:\n{child}"));
            }
            if !child.contains("# HELP trellis_intake_restarts_total ") {
                return Err(format!(
                    "the child's registry has no descriptions:\n{child}"
                ));
            }
            if child.contains("fork_parent_only") {
                return Err(format!(
                    "the child is recording into the parent's registry, whose locks a parent \
                     thread may have held at the fork:\n{child}"
                ));
            }
            Ok(())
        })
    }

    fn child_connects_while_parent_engine_is_live() -> Result<(), String> {
        let cluster = TestCluster::start();
        let setup = tokio::runtime::Runtime::new().expect("setup runtime");
        let db = setup.block_on(cluster.create_empty_database());
        setup.block_on(async {
            let client = db.pool.get().await.expect("get connection");
            client
                .batch_execute(
                    "create type public.ticket_status as enum ('open', 'closed'); \
                     create table tickets (id bigint primary key, status ticket_status); \
                     create table orders (id bigint primary key, status ticket_status)",
                )
                .await
                .expect("create source tables");
        });
        drop(setup);

        let config = Config::from_dsn(db.dsn().to_string()).expect("valid dsn");
        // The parent's own handle, left running across the fork: its engine
        // threads are live, as in the case #151's `before_fork` advice
        // doesn't cover.
        let parent = BlockingTrellis::connect(config.clone(), TrellisOptions::default())
            .expect("parent connects");
        parent.migrate().expect("migrate");
        parent
            .apply("TRANSFORM ticket_view FROM tickets SELECT status AS status")
            .expect("parent defines over the enum column");

        let held = HeldInternerLock::hold();
        let outcome = in_forked_child(CONNECTING_CHILD, move || {
            let child = BlockingTrellis::connect(config, TrellisOptions::default())
                .map_err(|e| format!("child connect: {e}"))?;
            // Reads the parent's definition back, decoding its persisted
            // enum type token through the interner.
            let defs = child
                .definitions()
                .map_err(|e| format!("child definitions: {e}"))?;
            if defs.is_empty() {
                return Err("the child sees no definitions".to_string());
            }
            // Classifies a live enum column, interning its type name.
            child
                .apply("TRANSFORM order_view FROM orders SELECT status AS status")
                .map_err(|e| format!("child define: {e}"))?;
            child
                .status("order_view")
                .map_err(|e| format!("child status: {e}"))?
                .ok_or("the child's definition has no status")?;
            Ok(())
        });
        held.release();
        parent.shutdown().expect("parent shuts down");
        outcome
    }
}
