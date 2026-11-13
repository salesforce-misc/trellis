//! `connect`'s `metrics_bind`: this process's metrics registry served on a
//! port, by `trellis-embed`'s [`MetricsListener`] (the CLI's
//! `--prometheus-bind` listener, on a thread of its own).
//!
//! The registry is process-wide, so a process has one listener, shared by
//! every handle that asked for it. The first such handle binds it; a later
//! one asking for the same address (as given, or as bound) shares it, and one
//! asking for another address is refused. It stops, closing its socket, when
//! the last handle that asked for it shuts down or is dropped.
//!
//! The listener's thread renders the registry, taking its lock, so it holds
//! an engine [`Lease`] like an engine's threads: a child forked while it runs
//! can't connect, and so can't serve or scrape a registry whose lock it may
//! have inherited held. A child forked while it runs also inherits its
//! socket, which then stays bound until that child exits too: one more reason
//! to shut down before forking.

use std::io;
use std::net::SocketAddr;
use std::sync::{Mutex, PoisonError};

use trellis_embed::metrics_http::MetricsListener;

use crate::engine::Lease;

/// The process's listener, while some handle uses it.
struct Served {
    /// The address the first handle asked for (port 0 included).
    requested: SocketAddr,
    listener: MetricsListener,
    lease: Lease,
    /// How many [`MetricsUse`]s are live.
    users: usize,
}

static SERVED: Mutex<Option<Served>> = Mutex::new(None);

/// One handle's share of the process's listener. Dropping it gives the share
/// back, stopping the listener with the last one.
pub(crate) struct MetricsUse {
    local_addr: SocketAddr,
    /// The process that took it: a copy a forked child inherited is never
    /// given back, as the listener it counts isn't the child's.
    pid: u32,
}

impl MetricsUse {
    /// Serves this process's registry on `requested`, or shares the listener
    /// already serving it. The error is the refusal's message.
    pub(crate) fn acquire(requested: SocketAddr) -> Result<MetricsUse, String> {
        let mut served = SERVED.lock().unwrap_or_else(PoisonError::into_inner);
        let local_addr = match served.as_mut() {
            Some(served) => {
                let bound = served.listener.local_addr();
                if requested != served.requested && requested != bound {
                    return Err(format!(
                        "metrics_bind {requested}: this process already serves its metrics on \
                         {bound}, and a process has one metrics listener, shared by its handles: \
                         pass the same metrics_bind, or none"
                    ));
                }
                served.users += 1;
                bound
            }
            None => {
                let lease = Lease::take().map_err(|err| {
                    format!(
                        "metrics_bind {requested}: this process was forked from process {} while \
                         that process had a Trellis engine running",
                        err.parent
                    )
                })?;
                let listener = match MetricsListener::bind(requested) {
                    Ok(listener) => listener,
                    Err(err) => {
                        lease.release();
                        return Err(bind_failure(requested, &err));
                    }
                };
                let bound = listener.local_addr();
                *served = Some(Served {
                    requested,
                    listener,
                    lease,
                    users: 1,
                });
                bound
            }
        };
        Ok(MetricsUse {
            local_addr,
            pid: std::process::id(),
        })
    }

    /// The address the listener is bound to.
    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

fn bind_failure(requested: SocketAddr, err: &io::Error) -> String {
    let hint = if err.kind() == io::ErrorKind::AddrInUse {
        ": another process holds it (a sibling worker, or the parent of a forked one, may be \
         serving its own metrics there); pass metrics_bind only in the one process that serves \
         them, or a different port in each"
    } else {
        ""
    };
    format!("metrics_bind {requested}: can't listen there: {err}{hint}")
}

impl Drop for MetricsUse {
    fn drop(&mut self) {
        if self.pid != std::process::id() {
            return;
        }
        let mut served = SERVED.lock().unwrap_or_else(PoisonError::into_inner);
        match served.as_mut() {
            Some(current) if current.users > 1 => current.users -= 1,
            _ => {
                // Under the lock, so a handle connecting meanwhile binds the
                // address only once this socket is closed.
                if let Some(Served {
                    mut listener,
                    lease,
                    ..
                }) = served.take()
                {
                    listener.stop();
                    lease.release();
                }
            }
        }
    }
}
