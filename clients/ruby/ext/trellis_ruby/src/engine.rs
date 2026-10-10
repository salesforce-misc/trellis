//! Knowing whether a forked child may connect (issue #600).
//!
//! `fork()` copies the whole address space but only the calling thread. The
//! engine keeps process-wide state behind locks (the enum type-name
//! interner, the metrics registry, and others inside its dependencies:
//! `tracing`, `quanta`, Rust's stdout), and its threads take them while they
//! work. A lock one of them held at the moment of a fork is copied held, and
//! no thread in the child will ever release it, so a child that connects an
//! engine of its own can wait on it forever. Nothing the child does after
//! the fork can undo that. The only sound rule is the one this module
//! enforces: a child may connect only if its parent had no engine thread
//! running when it forked.
//!
//! [`ENGINES`] counts this process's engines from before an engine's first
//! thread starts until after its last one has exited. A forked child
//! inherits the count along with the rest of memory, so the child's first
//! `connect` can tell, from the count alone, whether its parent had an
//! engine running at the fork, whatever did the forking (`Kernel#fork`, a C
//! extension's `fork()`, a server's worker spawner). It needs no fork hook.
//!
//! "Running" is deliberately broader than "held by `Trellis`": an engine
//! still connecting on another thread, or still winding down after an
//! interrupted `shutdown` or a dropped handle, has threads too, and counts.

use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, ready};

use trellis::{BlockingTrellis, Config, Shutdown, TrellisError, TrellisOptions};

/// The process whose engines are counted (high 32 bits: its pid) and how many
/// are running (low 32 bits). One word, so a count and its owner change
/// together. A count whose owner isn't this process was inherited through
/// `fork`, and can never drop: the threads it counts don't exist here.
static ENGINES: AtomicU64 = AtomicU64::new(0);

fn pack(pid: u32, count: u32) -> u64 {
    (u64::from(pid) << 32) | u64::from(count)
}

fn unpack(word: u64) -> (u32, u32) {
    ((word >> 32) as u32, word as u32)
}

/// Why this process can't connect: it was forked from `parent` while
/// `parent` had an engine running.
pub(crate) struct ForkedWhileRunning {
    pub(crate) parent: u32,
}

/// Whether this process may start an engine. Exact, not a snapshot that can
/// go stale: a process forked while its parent's engine ran stays that way
/// for good, since nothing here ever lowers an inherited count.
pub(crate) fn check() -> Result<(), ForkedWhileRunning> {
    let (owner, count) = unpack(ENGINES.load(Ordering::SeqCst));
    if owner != std::process::id() && count > 0 {
        return Err(ForkedWhileRunning { parent: owner });
    }
    Ok(())
}

/// One count in [`ENGINES`]. There is no `Drop`: a lease that is lost rather
/// than [released](Lease::release) keeps its count for good, which only
/// ever makes a later child refuse to connect, never lets one connect when
/// it shouldn't.
struct Lease(());

impl Lease {
    fn take() -> Result<Lease, ForkedWhileRunning> {
        let me = std::process::id();
        let mut word = ENGINES.load(Ordering::SeqCst);
        loop {
            let (owner, count) = unpack(word);
            let next = if owner == me {
                count + 1
            } else if count == 0 {
                1
            } else {
                return Err(ForkedWhileRunning { parent: owner });
            };
            match ENGINES.compare_exchange(word, pack(me, next), Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return Ok(Lease(())),
                Err(seen) => word = seen,
            }
        }
    }

    /// Only once every thread the engine started has exited.
    fn release(self) {
        ENGINES.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A [`BlockingTrellis`] counted in [`ENGINES`] for as long as it may have
/// threads.
pub(crate) struct Engine {
    /// `None` only once `shutdown` or `drop` has taken it.
    running: Option<(BlockingTrellis, Lease)>,
}

impl Engine {
    /// Connects, counting the engine before its first thread starts. The
    /// outer `Err` is [`check`]'s refusal, before anything has started.
    pub(crate) fn connect(
        config: Config,
        options: TrellisOptions,
    ) -> Result<Result<Engine, TrellisError>, ForkedWhileRunning> {
        let lease = Lease::take()?;
        Ok(match BlockingTrellis::connect(config, options) {
            Ok(trellis) => Ok(Engine {
                running: Some((trellis, lease)),
            }),
            Err(err) => {
                // A failed connect has joined every thread it started.
                lease.release();
                Err(err)
            }
        })
    }

    /// Starts stopping the engine, returning the wait for its threads to end.
    /// The wait is a future, so the caller can wait interruptibly; if it is
    /// dropped before it finishes, the shutdown is finished on a thread of
    /// its own (see [`Stopping`]).
    pub(crate) fn start_shutdown(mut self) -> Stopping {
        let (trellis, lease) = self
            .running
            .take()
            .expect("an engine is shut down only once");
        Stopping {
            running: Some((trellis.start_shutdown(), lease)),
        }
    }
}

/// An engine's shutdown under way, ready once its threads have exited and
/// its count is given back.
pub(crate) struct Stopping {
    /// `None` once the shutdown has finished.
    running: Option<(Shutdown, Lease)>,
}

/// Gives `lease` back once `result` shows the engine's threads are gone.
/// `BlockingThreadGone` means its background thread panicked, which drops the
/// engine without joining the threads it started, so that count is kept.
fn finish(result: &Result<(), TrellisError>, lease: Lease) {
    if !matches!(result, Err(TrellisError::BlockingThreadGone)) {
        lease.release();
    }
}

impl Future for Stopping {
    type Output = Result<(), TrellisError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some((shutdown, _)) = self.running.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        let result = ready!(Pin::new(shutdown).poll(cx));
        if let Some((_, lease)) = self.running.take() {
            finish(&result, lease);
        }
        Poll::Ready(result)
    }
}

impl Drop for Stopping {
    /// A shutdown abandoned before it finished (its waiter was interrupted)
    /// still runs to its end: a thread of its own waits for it, so the engine
    /// is joined and its count given back once its threads are gone. This is
    /// the one thread the binding starts on a caller's behalf, and only for
    /// an interrupted shutdown, never for a call.
    fn drop(&mut self) {
        if let Some((shutdown, lease)) = self.running.take() {
            // If the thread can't start, the closure (and the lease in it) is
            // dropped unreleased: the engine winds down unjoined, and its
            // count stays.
            let _ = std::thread::Builder::new()
                .name("trellis-ruby-stop".to_string())
                .spawn(move || finish(&shutdown.wait(), lease));
        }
    }
}

/// Shuts `trellis` down and gives its count back once its threads are gone.
fn stop(trellis: BlockingTrellis, lease: Lease) -> Result<(), TrellisError> {
    let result = trellis.shutdown();
    finish(&result, lease);
    result
}

impl Deref for Engine {
    type Target = BlockingTrellis;

    fn deref(&self) -> &BlockingTrellis {
        &self.running.as_ref().expect("a live engine").0
    }
}

impl Drop for Engine {
    /// An engine dropped without `shutdown` (a handle garbage-collected while
    /// connected, or a `connect` whose caller was interrupted and gave up on
    /// it) is shut down on a thread of its own, so the thread dropping it
    /// (Ruby's GC, say) doesn't wait, and its count is still given back only
    /// once its threads are gone. Never reached in a forked child: `Handle`
    /// leaks an inherited engine rather than dropping it.
    fn drop(&mut self) {
        if let Some((trellis, lease)) = self.running.take() {
            // If the thread can't start, the closure (and the lease in it) is
            // dropped unreleased: the engine winds down unjoined, and its
            // count stays.
            let _ = std::thread::Builder::new()
                .name("trellis-ruby-stop".to_string())
                .spawn(move || {
                    let _ = stop(trellis, lease);
                });
        }
    }
}
