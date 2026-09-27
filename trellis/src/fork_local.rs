//! Process-global state that a forked child rebuilds instead of inheriting
//! (issue #600).
//!
//! `fork()` copies the whole address space but only the calling thread. Any
//! lock another parent thread held at that instant is copied *held*, and the
//! child has no thread that will ever release it. So a child that connects
//! its own [`crate::Trellis`] (the Ruby binding's `Process.fork` case) hangs
//! the first time it touches a process-wide lock that some parent engine
//! thread happened to hold at the fork. This crate has two such globals on
//! the paths a fresh handle takes: the enum type-name interner
//! (`defs::pg_type`) and the metrics recorder (`metrics`), whose registry
//! is a set of `RwLock`-guarded shards inside `metrics-util` that every
//! `counter!`/`gauge!`/`histogram!` call takes.
//!
//! [`ForkLocal`] holds such a global. It is built lazily, like a `OnceLock`,
//! but it also remembers which *fork generation* built it. A
//! `pthread_atfork` child handler bumps the generation in every forked
//! child, so the child's first [`ForkLocal::get`] sees a stale value and
//! builds a fresh one rather than reaching into the parent's copy and its
//! possibly-held locks. The stale copy is leaked on purpose: it may be
//! locked forever, and freeing a lock another (now nonexistent) thread holds
//! is not something to attempt. A leak happens at most once per global per
//! fork, and only in the child.
//!
//! Nothing here blocks. Two threads that race to build the first value both
//! build one and the loser drops its own, so there is no "initialization in
//! progress" state a fork could freeze mid-way the way it can freeze a
//! `OnceLock` or `std::sync::Once`.
//!
//! This covers only this crate's own globals. A lock held inside libc,
//! `tokio`, `tracing` or another dependency's own statics at the moment of
//! the fork is out of its reach; see `docs/embedding.md`'s "Forking" section.

use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};

/// Bumped by [`bump_generation`] in every forked child.
static GENERATION: AtomicU64 = AtomicU64::new(0);

/// Whether this process has registered [`bump_generation`] yet. A plain
/// flag, not a `Once`: two threads that race here may both register, which
/// only means a child bumps the generation twice. That's harmless, whereas
/// a `Once` caught mid-call by a fork would leave the child waiting on it
/// forever.
static HANDLER_REGISTERED: AtomicBool = AtomicBool::new(false);

#[cfg(unix)]
extern "C" fn bump_generation() {
    // Runs in the child, in the forking thread, before `fork()` returns
    // there. An atomic add is async-signal-safe, which is all this context
    // allows.
    GENERATION.fetch_add(1, Ordering::Relaxed);
}

/// The current fork generation, registering the `pthread_atfork` child
/// handler first if this process hasn't yet.
///
/// Registering before reading is what makes staleness detection sound: any
/// value built from the returned generation was built after the handler was
/// in place, so every later fork's child is guaranteed a different
/// generation.
fn current_generation() -> u64 {
    if !HANDLER_REGISTERED.load(Ordering::Acquire) {
        register_handler();
        HANDLER_REGISTERED.store(true, Ordering::Release);
    }
    GENERATION.load(Ordering::Acquire)
}

#[cfg(unix)]
fn register_handler() {
    // SAFETY: `bump_generation` is a plain `extern "C" fn()` that only
    // touches an atomic. The two null handlers mean "nothing to do" before
    // the fork and in the parent.
    let rc = unsafe { libc::pthread_atfork(None, None, Some(bump_generation)) };
    // Only fails with ENOMEM. Without the handler a forked child would
    // silently share this process's globals again, so fail loudly instead.
    assert_eq!(rc, 0, "pthread_atfork failed to register (errno {rc})");
}

#[cfg(not(unix))]
fn register_handler() {
    // No `fork()` here, so the generation never changes.
}

struct Slot<T> {
    generation: u64,
    value: T,
}

/// A lazily built process-global value that a forked child rebuilds rather
/// than inherits. See the module doc comment.
pub(crate) struct ForkLocal<T> {
    slot: AtomicPtr<Slot<T>>,
    init: fn() -> T,
}

impl<T: Send + Sync + 'static> ForkLocal<T> {
    pub(crate) const fn new(init: fn() -> T) -> Self {
        Self {
            slot: AtomicPtr::new(ptr::null_mut()),
            init,
        }
    }

    /// This process's value, building it on first use in this process
    /// (including the first use in a forked child).
    pub(crate) fn get(&'static self) -> &'static T {
        self.get_at(current_generation())
    }

    /// [`Self::get`] for an explicit generation, so the unit test below can
    /// play parent and child without bumping the process-wide generation
    /// every other global in the test binary reads.
    fn get_at(&'static self, generation: u64) -> &'static T {
        let mut seen = self.slot.load(Ordering::Acquire);
        loop {
            // SAFETY: every non-null pointer ever stored in `slot` came from
            // `Box::into_raw` below and is never freed once published (a
            // replaced slot is leaked), so it is valid for `'static`.
            if let Some(slot) = unsafe { seen.as_ref() }
                && slot.generation == generation
            {
                return &slot.value;
            }
            let fresh = Box::into_raw(Box::new(Slot {
                generation,
                value: (self.init)(),
            }));
            match self
                .slot
                .compare_exchange(seen, fresh, Ordering::AcqRel, Ordering::Acquire)
            {
                // `seen`, if non-null, belonged to the parent process and is
                // leaked: see the module doc comment.
                // SAFETY: `fresh` is now published and, like every published
                // slot, never freed.
                Ok(_) => return unsafe { &(*fresh).value },
                Err(current) => {
                    // SAFETY: the exchange failed, so `fresh` was never
                    // published and this thread still owns it.
                    drop(unsafe { Box::from_raw(fresh) });
                    seen = current;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;

    use super::*;

    static BUILDS: AtomicUsize = AtomicUsize::new(0);

    fn build() -> Mutex<u32> {
        BUILDS.fetch_add(1, Ordering::SeqCst);
        Mutex::new(0)
    }

    static COUNTER: ForkLocal<Mutex<u32>> = ForkLocal::new(build);

    /// `tests/fork_child_globals.rs` covers a real `fork()`; this pins the
    /// generation bookkeeping on its own.
    #[test]
    fn value_is_shared_within_a_generation_and_rebuilt_in_the_next() {
        let first: *const Mutex<u32> = COUNTER.get_at(7);
        *COUNTER.get_at(7).lock().unwrap() += 1;
        assert!(
            ptr::eq(first, COUNTER.get_at(7)),
            "same process, same value"
        );
        assert_eq!(BUILDS.load(Ordering::SeqCst), 1);

        // Hold the parent's lock across the "fork", the way an engine thread
        // might have held it when the real fork happened.
        let held = COUNTER.get_at(7).lock().unwrap();
        let child = COUNTER.get_at(8);
        assert!(
            !ptr::eq(first, child),
            "a forked child builds its own value"
        );
        assert_eq!(
            *child
                .try_lock()
                .expect("the child's value is not the held one"),
            0
        );
        assert_eq!(BUILDS.load(Ordering::SeqCst), 2);
        drop(held);
    }
}
