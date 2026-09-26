//! Tests of the `trellis-testkit` binary (issue #143), driven the way a host
//! test suite drives it: spawn it, read its JSON line, connect, then end it
//! and check it took the cluster with it.

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};
use tokio_postgres::NoTls;

/// A running `trellis-testkit` and what it printed.
struct Spawned {
    child: Child,
    // `None` once closed.
    stdin: Option<ChildStdin>,
    info: serde_json::Value,
}

impl Spawned {
    fn start(args: &[&str]) -> Self {
        Self::start_with_stderr(args, Stdio::inherit())
    }

    fn start_with_stderr(args: &[&str], stderr: Stdio) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_trellis-testkit"))
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(stderr)
            .spawn()
            .expect("spawn trellis-testkit");
        let stdin = child.stdin.take().expect("piped stdin");
        let mut line = String::new();
        BufReader::new(child.stdout.take().expect("piped stdout"))
            .read_line(&mut line)
            .expect("read the connection-info line");
        let info = serde_json::from_str(&line)
            .unwrap_or_else(|e| panic!("stdout's first line is not JSON ({e}): {line:?}"));
        Spawned {
            child,
            stdin: Some(stdin),
            info,
        }
    }

    fn str_field(&self, key: &str) -> &str {
        self.info[key]
            .as_str()
            .unwrap_or_else(|| panic!("`{key}` should be a string in {}", self.info))
    }

    fn data_dir(&self) -> PathBuf {
        PathBuf::from(self.str_field("data_dir"))
    }

    fn postmaster_pid(&self) -> u64 {
        self.info["pid"].as_u64().expect("`pid` should be a number")
    }

    fn wait_for_exit(&mut self) -> ExitStatus {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(status) = self.child.try_wait().expect("poll trellis-testkit") {
                return status;
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                panic!("trellis-testkit did not exit within 60s");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

/// Connects with the printed `dsn`, and again from the separate
/// `host`/`port`/`user`/`dbname` fields (how Postgrex is configured), and
/// checks both land in the named database on a logical-replication cluster.
async fn connect_and_check(spawned: &Spawned) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(spawned.str_field("dsn"), NoTls)
        .await
        .expect("connect with the printed dsn");
    tokio::spawn(connection);
    let row = client
        .query_one(
            "select current_database(), current_setting('wal_level')",
            &[],
        )
        .await
        .expect("query the database");
    assert_eq!(row.get::<_, String>(0), spawned.str_field("dbname"));
    assert_eq!(row.get::<_, String>(1), "logical");

    let mut config = tokio_postgres::Config::new();
    config
        .host_path(spawned.str_field("host"))
        .port(
            spawned.info["port"]
                .as_u64()
                .expect("`port` should be a number") as u16,
        )
        .user(spawned.str_field("user"))
        .dbname(spawned.str_field("dbname"));
    let (by_parts, connection) = config
        .connect(NoTls)
        .await
        .expect("connect with the separate fields");
    tokio::spawn(connection);
    by_parts
        .query_one("select 1", &[])
        .await
        .expect("query over the by-parts connection");

    client
}

fn process_alive(pid: u64) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn assert_torn_down(spawned: &mut Spawned, data_dir: &std::path::Path, pid: u64) {
    let status = spawned.wait_for_exit();
    assert!(status.success(), "trellis-testkit exited with {status}");
    assert!(
        !data_dir.exists(),
        "data dir {} should be removed on teardown",
        data_dir.display()
    );
    assert!(
        !data_dir.parent().expect("data dir has a parent").exists(),
        "the cluster's temp dir should be removed on teardown"
    );
    assert!(!process_alive(pid), "postmaster {pid} should have exited");
}

#[tokio::test]
async fn closing_stdin_tears_the_cluster_down() {
    let mut spawned = Spawned::start(&["--dbname", "host_suite"]);
    assert_eq!(spawned.str_field("dbname"), "host_suite");
    let data_dir = spawned.data_dir();
    let pid = spawned.postmaster_pid();
    assert!(data_dir.exists(), "data dir should exist while held");
    assert!(
        process_alive(pid),
        "postmaster should be running while held"
    );

    let client = connect_and_check(&spawned).await;
    drop(client);

    // What happens when a host test runner exits, however it exits.
    drop(spawned.stdin.take());

    assert_torn_down(&mut spawned, &data_dir, pid);
}

#[tokio::test]
async fn sigterm_tears_the_cluster_down_with_a_client_still_connected() {
    let mut spawned = Spawned::start(&[]);
    assert_eq!(spawned.str_field("dbname"), "trellis_test");
    let data_dir = spawned.data_dir();
    let pid = spawned.postmaster_pid();

    // Stay connected through teardown, as a runner that signals before
    // closing its pool would.
    let _client = connect_and_check(&spawned).await;

    let status = Command::new("kill")
        .arg("-TERM")
        .arg(spawned.child.id().to_string())
        .status()
        .expect("run kill");
    assert!(status.success(), "kill -TERM failed");

    // stdin is still open here, so it was the signal that did it.
    assert_torn_down(&mut spawned, &data_dir, pid);
}

#[test]
fn a_host_that_captured_stderr_and_died_still_gets_a_clean_exit() {
    // A host that spawned with stderr on a pipe too (Ruby's `Open3.popen3`,
    // say) and then died: every pipe it held closes at once. Writing the
    // teardown note into the dead stderr must not panic the binary.
    let mut spawned = Spawned::start_with_stderr(&[], Stdio::piped());
    let data_dir = spawned.data_dir();
    let pid = spawned.postmaster_pid();

    drop(spawned.child.stderr.take());
    drop(spawned.stdin.take());

    assert_torn_down(&mut spawned, &data_dir, pid);
}
