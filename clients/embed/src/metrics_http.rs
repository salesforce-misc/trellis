//! The metrics scrape listener `trellis run --prometheus-bind` and the Ruby
//! binding's `metrics_bind` share: a small TCP listener answering every
//! request with this process's registry, rendered fresh by
//! [`Metrics::render_prometheus`]. The registry is process-wide, so it serves
//! whatever the process's pipeline records, never another process's.
//!
//! There is deliberately no HTTP parsing beyond the method. A scraper (or
//! `curl`) sends a bare `GET` with a few headers and nothing worth reading, so
//! a connection drains a bounded amount of request ([`MAX_REQUEST_BYTES`],
//! [`IO_TIMEOUT`]) and is answered and closed, whatever its path: `GET` gets
//! a `200 OK` with the body, `HEAD` the same headers alone, and anything else
//! (a request that never names its method included) a `405`, as
//! `Trellis::Metrics::RackApp` answers.
//!
//! [`serve`] is the loop, for a caller already on a `tokio` runtime (the
//! CLI's). [`MetricsListener`] runs it on a thread of its own with its own
//! single-threaded runtime, for a host with no runtime to lend (a binding,
//! whose engines' runtimes each belong to one handle while the listener
//! belongs to the process).

use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::thread::JoinHandle;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use trellis::Metrics;

/// The most request bytes a connection may send before it is answered
/// anyway. A real request is a few hundred bytes; this only bounds a client
/// that never sends the blank line ending its headers.
pub const MAX_REQUEST_BYTES: usize = 8 * 1024;

/// How long a connection may take to send its request, and to take the
/// response, before it is answered anyway or dropped: a slow or silent client
/// can't hold its task open.
pub const IO_TIMEOUT: Duration = Duration::from_secs(5);

/// The `Content-Type` of Prometheus's text exposition format.
pub const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Accepts connections on `listener` forever, answering each in a task of
/// its own. A failed accept or connection is passed to `report` and the loop
/// goes on: one bad client never takes the listener down. Stop it by
/// dropping the future (a `select!` against the caller's own shutdown).
pub async fn serve(listener: TcpListener, report: fn(&str)) -> Infallible {
    loop {
        match listener.accept().await {
            Ok((stream, _peer)) => {
                tokio::spawn(async move {
                    if let Err(err) = answer(stream).await {
                        report(&format!("metrics connection error: {err}"));
                    }
                });
            }
            Err(err) => report(&format!("metrics accept error: {err}")),
        }
    }
}

/// Drains the request, then writes the response for its method (see the
/// module doc) and closes the connection.
async fn answer(mut stream: TcpStream) -> Result<(), String> {
    let request = drain_request(&mut stream).await?;
    let response = respond(method(&request));
    tokio::time::timeout(IO_TIMEOUT, stream.write_all(response.as_bytes()))
        .await
        .map_err(|_| "timed out writing response".to_string())?
        .map_err(|err| format!("failed to write response: {err}"))?;
    stream
        .shutdown()
        .await
        .map_err(|err| format!("failed to close connection: {err}"))
}

/// The request's method: its bytes up to the first space.
fn method(request: &[u8]) -> &[u8] {
    request
        .split(|&byte| byte == b' ')
        .next()
        .unwrap_or_default()
}

/// The whole response to a request with `method`.
fn respond(method: &[u8]) -> String {
    let (head_only, status) = match method {
        b"GET" => (false, "200 OK"),
        b"HEAD" => (true, "200 OK"),
        _ => {
            return "HTTP/1.1 405 Method Not Allowed\r\n\
                    Allow: GET, HEAD\r\n\
                    Content-Length: 0\r\n\
                    Connection: close\r\n\
                    \r\n"
                .to_string();
        }
    };
    let body = Metrics::new().render_prometheus();
    format!(
        "HTTP/1.1 {status}\r\n\
         Content-Type: {PROMETHEUS_CONTENT_TYPE}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {}",
        body.len(),
        if head_only { "" } else { &body },
    )
}

/// Reads until the headers look complete (`\r\n\r\n`), the client stops
/// sending, or [`MAX_REQUEST_BYTES`] or [`IO_TIMEOUT`] is reached, and returns
/// what it read.
async fn drain_request(stream: &mut TcpStream) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    let drain = async {
        let mut chunk = [0u8; 512];
        while buf.len() < MAX_REQUEST_BYTES {
            let n = stream
                .read(&mut chunk)
                .await
                .map_err(|err| format!("failed to read request: {err}"))?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            if buf.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        Ok::<(), String>(())
    };
    // A client still sending at the timeout is answered anyway.
    tokio::time::timeout(IO_TIMEOUT, drain)
        .await
        .unwrap_or(Ok(()))?;
    Ok(buf)
}

/// [`serve`] on a thread of its own, from [`bind`](MetricsListener::bind)
/// until [`stop`](MetricsListener::stop) or drop, which close the socket
/// before returning, so the address can be bound again at once.
///
/// Connection errors go to `tracing` at `warn`, where a binding's log bridge
/// picks them up. The thread only touches the registry and its socket; it
/// never calls into a host VM.
pub struct MetricsListener {
    local_addr: SocketAddr,
    /// `None` once stopped.
    running: Option<(oneshot::Sender<()>, JoinHandle<()>)>,
}

impl MetricsListener {
    /// Binds `addr` (port 0 picks a free port; see
    /// [`local_addr`](Self::local_addr)) and starts serving. The bind happens
    /// before this returns, so its failure (the address in use, not local, or
    /// not permitted) is this call's error.
    pub fn bind(addr: SocketAddr) -> io::Result<MetricsListener> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        // Bound through tokio, as the CLI binds, for its socket options
        // (`SO_REUSEADDR`, so a stopped listener's address rebinds at once).
        let listener = runtime.block_on(TcpListener::bind(addr))?;
        let local_addr = listener.local_addr()?;
        let (stop, stopped) = oneshot::channel::<()>();
        let thread = std::thread::Builder::new()
            .name("trellis-metrics".to_string())
            .spawn(move || {
                runtime.block_on(async move {
                    tokio::select! {
                        never = serve(listener, |message| tracing::warn!("{message}")) => match never {},
                        _ = stopped => {}
                    }
                });
                // Dropping the runtime here drops the listener and every
                // connection still open.
            })?;
        tracing::info!("serving metrics on http://{local_addr}");
        Ok(MetricsListener {
            local_addr,
            running: Some((stop, thread)),
        })
    }

    /// The address it listens on: the port the OS picked for port 0.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stops serving and waits for the thread, so the socket is closed when
    /// this returns. Idempotent.
    pub fn stop(&mut self) {
        if let Some((stop, thread)) = self.running.take() {
            let _ = stop.send(());
            let _ = thread.join();
        }
    }
}

impl Drop for MetricsListener {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use super::*;

    /// One request over a plain blocking socket, as a scraper sends it.
    fn request(addr: SocketAddr, text: &str) -> String {
        let mut stream = std::net::TcpStream::connect(addr).expect("connect to the listener");
        stream
            .write_all(text.as_bytes())
            .expect("write the request");
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .expect("read until the listener closes the connection");
        response
    }

    #[test]
    fn serves_the_rendered_registry_as_a_200_with_the_prometheus_content_type() {
        trellis::dev::metrics::increment_changes_applied("metrics_http_test_target", 1);
        let listener = MetricsListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();

        let response = request(
            listener.local_addr(),
            "GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        );

        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
        assert!(
            response.contains(&format!("Content-Type: {PROMETHEUS_CONTENT_TYPE}\r\n")),
            "{response}"
        );
        let (head, body) = response.split_once("\r\n\r\n").unwrap();
        assert!(
            head.contains(&format!("Content-Length: {}\r\n", body.len())),
            "{response}"
        );
        assert!(
            body.contains("trellis_changes_applied_total")
                && body.contains("metrics_http_test_target"),
            "{response}"
        );
    }

    /// `HEAD` gets `GET`'s headers alone; every path is answered alike.
    #[test]
    fn head_gets_the_headers_alone() {
        trellis::dev::metrics::increment_changes_applied("metrics_http_head_target", 1);
        let listener = MetricsListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();

        let get = request(listener.local_addr(), "GET /elsewhere HTTP/1.1\r\n\r\n");
        let head = request(listener.local_addr(), "HEAD / HTTP/1.1\r\n\r\n");

        let (get_head, get_body) = get.split_once("\r\n\r\n").unwrap();
        assert!(get_head.starts_with("HTTP/1.1 200 OK\r\n"), "{get}");
        assert!(!get_body.is_empty(), "{get}");
        assert!(head.starts_with("HTTP/1.1 200 OK\r\n"), "{head}");
        assert!(head.ends_with("\r\n\r\n"), "a body after HEAD: {head}");
        assert!(
            head.contains(&format!("Content-Type: {PROMETHEUS_CONTENT_TYPE}\r\n")),
            "{head}"
        );
    }

    /// Any other method, or none, is refused as `Trellis::Metrics::RackApp`
    /// refuses it.
    #[test]
    fn other_methods_get_a_405() {
        let listener = MetricsListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let refusal = "HTTP/1.1 405 Method Not Allowed\r\n\
                       Allow: GET, HEAD\r\n\
                       Content-Length: 0\r\n\
                       Connection: close\r\n\r\n";

        for text in ["POST /metrics HTTP/1.1\r\n\r\n", "get / HTTP/1.1\r\n\r\n"] {
            assert_eq!(request(listener.local_addr(), text), refusal, "{text}");
        }

        // A client that closes its side without a request is answered too.
        let mut stream = std::net::TcpStream::connect(listener.local_addr()).unwrap();
        stream.shutdown(std::net::Shutdown::Write).unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        assert_eq!(response, refusal);
    }

    /// A client that connects and sends nothing doesn't hold `stop` up for
    /// its read timeout: its connection is dropped with the runtime.
    #[test]
    fn stopping_does_not_wait_for_a_silent_client() {
        let mut listener = MetricsListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let _silent = std::net::TcpStream::connect(listener.local_addr()).unwrap();
        // Accepted in order, so once this is answered the silent one has been
        // accepted and its task is waiting on it.
        request(listener.local_addr(), "GET / HTTP/1.1\r\n\r\n");

        let started = std::time::Instant::now();
        listener.stop();

        assert!(started.elapsed() < IO_TIMEOUT, "{:?}", started.elapsed());
    }

    #[test]
    fn binding_an_address_in_use_fails() {
        let first = MetricsListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();

        let err = MetricsListener::bind(first.local_addr())
            .err()
            .expect("the second bind fails");

        assert_eq!(err.kind(), io::ErrorKind::AddrInUse);
    }

    /// After a scrape, so the server side of a closed connection is in
    /// TIME_WAIT on the port, as in production.
    #[test]
    fn stopping_releases_the_address_at_once() {
        let mut listener = MetricsListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr();
        request(addr, "GET / HTTP/1.1\r\n\r\n");

        listener.stop();

        assert!(std::net::TcpStream::connect(addr).is_err());
        MetricsListener::bind(addr).expect("rebinding a stopped listener's address");
    }
}
