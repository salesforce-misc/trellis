//! `trellis dump` and `trellis restore`, driven as an operator drives them:
//! the binary spawned against a database on a [`TestCluster`].
//!
//! - a dump of a chain and an aggregate leaves Trellis's schema, the
//!   ledgers and the capture triggers out, and restoring it into an empty
//!   database restores both definitions as recorded, the reader after its
//!   upstream, with a user's index on a target
//!   (`a_dump_restores_a_chain_and_an_aggregate_into_an_empty_database`)
//! - two dumps of the same definitions are the same bytes
//!   (`two_dumps_of_the_same_definitions_are_the_same`)
//! - `--definitions-only` writes just the appendix, without pg_dump
//!   (`definitions_only_writes_the_appendix_without_pg_dump`)
//! - a missing or too old pg_dump refuses, writing nothing
//!   (`a_missing_or_old_pg_dump_refuses_and_writes_nothing`)
//! - a file that can't be read or doesn't load leaves the database as it
//!   was (`a_bad_file_loads_nothing`)
//! - a database that isn't empty is refused and left as it was
//!   (`a_database_that_is_not_empty_is_refused`)
//! - a define that commits while pg_dump runs redoes the dump, and one
//!   during each of three attempts fails it
//!   (`a_define_racing_pg_dump_redoes_the_dump`)

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant};

use testkit::TestCluster;
use tokio_postgres::{Client, NoTls};
use trellis::{Config, TransformStatus, Trellis, TrellisOptions};

/// The source tables, and a chain (`o1` reads `orders`, `tot` reads `o1`)
/// defined reader first in the restore order's terms: `tot` is defined
/// after `o1`, so the dump has to put `o1` first anyway.
const SOURCE_DDL: &str = "\
    create table public.orders (id bigint primary key, g bigint, a bigint); \
    insert into public.orders values (1, 1, 10), (2, 1, 20), (3, 2, 30)";

const DEFINITIONS: [&str; 2] = [
    "TRANSFORM o1 FROM orders SELECT g AS g, a + 1 AS b",
    "TRANSFORM tot FROM o1 GROUP BY g WITHOUT SOURCE INDEX SELECT g AS g, SUM(b) AS total",
];

const RECORDED: &str = "\
    select jsonb_build_array(target_table, source_table, join_table, definition_text, \
           grammar_version, floor_version)::text \
    from trellis.transform_definitions order by target_table";

async fn connect(dsn: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn strings(client: &Client, sql: &str) -> Vec<String> {
    client
        .query(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| row.get(0))
        .collect()
}

async fn count(client: &Client, sql: &str) -> i64 {
    client
        .query_one(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .get(0)
}

/// What a database holds besides an empty `public`: the check `restore`
/// makes, as a count.
async fn things_in(client: &Client) -> i64 {
    count(
        client,
        "select (select count(*) from pg_namespace \
                 where nspname not in ('pg_catalog', 'information_schema', 'public') \
                   and nspname !~ '^pg_(toast|temp_|toast_temp_)') \
              + (select count(*) from pg_class c join pg_namespace n on n.oid = c.relnamespace \
                 where n.nspname = 'public')",
    )
    .await
}

/// A database with the source tables and both definitions, built by the
/// running engine (a chain's reader is defined over a `live` upstream), and
/// a user's index on a target.
async fn source_database(cluster: &TestCluster) -> (testkit::TestDatabase, Client) {
    let source = cluster.create_isolated_database().await;
    let raw = connect(source.dsn()).await;
    raw.batch_execute(SOURCE_DDL).await.expect("source ddl");
    let trellis = Trellis::connect(
        Config::from_dsn(source.dsn()).expect("dsn"),
        TrellisOptions {
            staging: true,
            drain_threads: 1,
            ..Default::default()
        },
    )
    .await
    .expect("start the engine");
    for statement in DEFINITIONS {
        let applied = trellis.apply(statement).await.expect(statement);
        let target = applied.into_transform().expect("a define names its target");
        wait_for_live(&trellis, &target).await;
    }
    trellis.shutdown().await.expect("shutdown");
    raw.batch_execute("create index o1_by_b on public.o1 (b)")
        .await
        .expect("a user's index on a target");
    (source, raw)
}

/// Polls `status` until `target` is `live`, as an operator does.
async fn wait_for_live(trellis: &Trellis, target: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let status = trellis
            .status(target)
            .await
            .expect("status")
            .expect("registered")
            .status;
        if status == TransformStatus::Live {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{target} never reported live (last: {status:?})"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Runs the CLI with `args` against `dsn`, with `path` as PATH when given.
fn trellis_cli(dsn: &str, args: &[&str], path: Option<&Path>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_trellis"));
    command.args(args).args(["--database-url", dsn]);
    if let Some(path) = path {
        command.env("PATH", path);
    }
    command.output().expect("spawn trellis")
}

fn succeeded(output: &Output, what: &str) -> String {
    assert!(
        output.status.success(),
        "{what} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout.clone()).expect("utf8 stdout")
}

fn failed(output: &Output, what: &str) -> String {
    assert!(
        !output.status.success(),
        "{what} should fail; stdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// A directory of this test's own, for dump files and fake programs,
/// removed when the test ends.
struct Scratch(PathBuf);

impl std::ops::Deref for Scratch {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.0
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn scratch(name: &str) -> Scratch {
    let dir = std::env::temp_dir().join(format!(
        "trellis_cli_dump_restore_{}_{name}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    Scratch(dir)
}

#[tokio::test]
async fn a_dump_restores_a_chain_and_an_aggregate_into_an_empty_database() {
    let cluster = TestCluster::start();
    let (source, raw) = source_database(&cluster).await;
    let recorded = strings(&raw, RECORDED).await;
    assert_eq!(recorded.len(), 2);
    assert!(
        count(
            &raw,
            "select count(*) from pg_trigger where tgname like 'trellis_capture_%'"
        )
        .await
            > 0,
        "the source table has capture triggers for the dump to leave out"
    );

    let dir = scratch("round_trip");
    let file = dir.join("dump.sql");
    let file_arg = file.to_str().expect("utf8 path");
    let stdout = succeeded(
        &trellis_cli(source.dsn(), &["dump", "--out", file_arg], None),
        "dump",
    );
    assert!(stdout.contains("2 definition(s)"), "{stdout}");
    let dump = std::fs::read_to_string(&file).expect("the dump file");
    assert!(dump.contains("CREATE TABLE public.orders"), "{dump}");
    assert!(dump.contains("CREATE TABLE public.tot"), "{dump}");
    assert!(
        dump.contains("o1_by_b"),
        "the user's index is dumped: {dump}"
    );
    assert!(!dump.contains("trellis_capture_"), "{dump}");
    assert!(!dump.contains("__ledger"), "{dump}");
    assert!(!dump.contains("CREATE SCHEMA trellis"), "{dump}");
    let o1 = dump.find("'public.o1', null,").expect("o1's row");
    let tot = dump.find("'public.tot', null,").expect("tot's row");
    assert!(o1 < tot, "the upstream is queued first: {dump}");

    let loaded = cluster.create_empty_database().await;
    let stdout = succeeded(
        &trellis_cli(loaded.dsn(), &["restore", file_arg], None),
        "restore",
    );
    assert!(stdout.contains("restored 2 definition(s)"), "{stdout}");
    let after = connect(loaded.dsn()).await;
    assert_eq!(
        strings(&after, RECORDED).await,
        recorded,
        "text, identities, grammar and floor as recorded"
    );
    assert_eq!(
        strings(
            &after,
            "select status from trellis.transform_definitions order by id"
        )
        .await,
        ["waiting_to_backfill", "waiting_to_backfill"]
    );
    assert_eq!(
        count(
            &after,
            "select (to_regclass('trellis.restore_queue') is null)::int::bigint"
        )
        .await,
        1,
        "the queue is dropped"
    );
    assert_eq!(
        count(
            &after,
            "select count(*) from pg_indexes where indexname = 'o1_by_b'"
        )
        .await,
        1,
        "the user's index on the adopted target survives"
    );
    // The restored database dumps the same definitions.
    let again = succeeded(
        &trellis_cli(loaded.dsn(), &["dump", "--definitions-only"], None),
        "dump of the restored database",
    );
    assert!(dump.ends_with(&again), "{again}");
}

/// `dump` without pg_dump's `\restrict` lines, whose key `pg_dump` 15.14,
/// 16.10, 17.6 and later pick at random on every run.
fn without_restrict_keys(dump: &str) -> String {
    dump.lines()
        .filter(|line| !line.starts_with("\\restrict ") && !line.starts_with("\\unrestrict "))
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test]
async fn two_dumps_of_the_same_definitions_are_the_same() {
    let cluster = TestCluster::start();
    let (source, _raw) = source_database(&cluster).await;
    let dump = || succeeded(&trellis_cli(source.dsn(), &["dump"], None), "dump");
    let first = dump();
    assert!(first.contains("CREATE TABLE public.o1"), "{first}");
    assert_eq!(
        without_restrict_keys(&dump()),
        without_restrict_keys(&first)
    );
    let definitions = || {
        succeeded(
            &trellis_cli(source.dsn(), &["dump", "--definitions-only"], None),
            "dump --definitions-only",
        )
    };
    assert_eq!(definitions(), definitions());
}

#[tokio::test]
async fn definitions_only_writes_the_appendix_without_pg_dump() {
    let cluster = TestCluster::start();
    let (source, _raw) = source_database(&cluster).await;
    let trellis = Trellis::connect(
        Config::from_dsn(source.dsn()).expect("dsn"),
        TrellisOptions::default(),
    )
    .await
    .expect("connect");
    let appendix = trellis.schema_dump().await.expect("schema dump").appendix;
    trellis.shutdown().await.expect("shutdown");

    // No pg_dump on PATH: it isn't run.
    let empty = scratch("definitions_only");
    let stdout = succeeded(
        &trellis_cli(source.dsn(), &["dump", "--definitions-only"], Some(&empty)),
        "dump --definitions-only",
    );
    assert_eq!(stdout, appendix);
    assert_eq!(
        stdout
            .lines()
            .filter(|line| line.starts_with("insert into "))
            .count(),
        2,
        "{stdout}"
    );
    assert!(!stdout.contains("CREATE TABLE"), "{stdout}");
}

#[tokio::test]
async fn a_missing_or_old_pg_dump_refuses_and_writes_nothing() {
    let cluster = TestCluster::start();
    let (source, _raw) = source_database(&cluster).await;
    let dir = scratch("missing_pg_dump");
    let file = dir.join("dump.sql");
    let file_arg = file.to_str().expect("utf8 path");
    let bin = dir.join("bin");
    std::fs::create_dir_all(&bin).expect("bin dir");

    let stderr = failed(
        &trellis_cli(source.dsn(), &["dump", "-o", file_arg], Some(&bin)),
        "dump without pg_dump",
    );
    assert!(stderr.contains("pg_dump is not on PATH"), "{stderr}");
    assert!(stderr.contains("--definitions-only"), "{stderr}");
    assert!(!file.exists(), "nothing is written");

    // A pg_dump older than 15, which the trigger filter isn't tested against.
    let fake = bin.join("pg_dump");
    std::fs::write(
        &fake,
        "#!/bin/sh\necho 'pg_dump (PostgreSQL) 14.9'\necho 'CREATE TABLE t (id int);'\n",
    )
    .expect("fake pg_dump");
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    }
    let stderr = failed(
        &trellis_cli(source.dsn(), &["dump", "-o", file_arg], Some(&bin)),
        "dump with pg_dump 14",
    );
    assert!(stderr.contains("too old"), "{stderr}");
    assert!(stderr.contains("pg_dump 15 or later"), "{stderr}");
    assert!(!file.exists(), "nothing is written");
}

#[tokio::test]
async fn a_bad_file_loads_nothing() {
    let cluster = TestCluster::start();
    let loaded = cluster.create_empty_database().await;
    let client = connect(loaded.dsn()).await;
    let dir = scratch("bad_file");

    let missing = dir.join("missing.sql");
    let stderr = failed(
        &trellis_cli(
            loaded.dsn(),
            &["restore", missing.to_str().expect("utf8")],
            None,
        ),
        "restore of a missing file",
    );
    assert!(stderr.contains("could not read"), "{stderr}");

    // The first statement loads, the second doesn't: the load is one
    // transaction, so neither stays, and nothing migrates.
    let bad = dir.join("bad.sql");
    std::fs::write(
        &bad,
        "CREATE TABLE public.half (id int);\nthis is not a dump;\n",
    )
    .expect("bad file");
    let stderr = failed(
        &trellis_cli(
            loaded.dsn(),
            &["restore", bad.to_str().expect("utf8")],
            None,
        ),
        "restore of a file that doesn't load",
    );
    assert!(stderr.contains("psql failed"), "{stderr}");
    assert!(stderr.contains("nothing was loaded"), "{stderr}");
    assert_eq!(things_in(&client).await, 0, "the database is still empty");
}

#[tokio::test]
async fn a_database_that_is_not_empty_is_refused() {
    let cluster = TestCluster::start();
    let (source, raw) = source_database(&cluster).await;
    let dir = scratch("not_empty");
    let file = dir.join("dump.sql");
    let file_arg = file.to_str().expect("utf8 path");
    succeeded(
        &trellis_cli(source.dsn(), &["dump", "-o", file_arg], None),
        "dump",
    );

    // A database with one table of its own.
    let other = cluster.create_empty_database().await;
    let client = connect(other.dsn()).await;
    client
        .batch_execute("create table public.mine (id int)")
        .await
        .expect("a table");
    let stderr = failed(
        &trellis_cli(other.dsn(), &["restore", file_arg], None),
        "restore into a database with a table",
    );
    assert!(stderr.contains("only into an empty database"), "{stderr}");
    assert_eq!(things_in(&client).await, 1, "only the table it had");

    // The database the dump came from.
    let before = things_in(&raw).await;
    let stderr = failed(
        &trellis_cli(source.dsn(), &["restore", file_arg], None),
        "restore into the dumped database",
    );
    assert!(stderr.contains("only into an empty database"), "{stderr}");
    assert_eq!(things_in(&raw).await, before);
}

/// The `pg_dump` the test would run: the first on PATH.
fn real_pg_dump() -> PathBuf {
    std::env::split_paths(&std::env::var_os("PATH").expect("PATH"))
        .map(|dir| dir.join("pg_dump"))
        .find(|candidate| candidate.is_file())
        .expect("pg_dump on PATH")
}

/// A `pg_dump` in `bin` that, on its first `defines` dumping runs, defines
/// a new transform `<name><run>` over `orders` through the CLI before running the real
/// `pg_dump`: a define that commits between the dump's two reads of the
/// definitions.
fn racing_pg_dump(bin: &Path, dsn: &str, name: &str, defines: usize) {
    let quote = |text: &str| format!("'{}'", text.replace('\'', r"'\''"));
    let counter = bin.join("runs");
    let script = format!(
        "#!/bin/sh\n\
         if [ \"$1\" != --version ]; then\n\
           n=$(( $(cat {counter} 2>/dev/null || echo 0) + 1 ))\n\
           echo $n > {counter}\n\
           if [ $n -le {defines} ]; then\n\
             {trellis} --database-url {dsn} apply \"TRANSFORM {name}$n FROM orders SELECT a AS a$n\" \
               >/dev/null || exit 70\n\
           fi\n\
         fi\n\
         exec {pg_dump} \"$@\"\n",
        counter = quote(counter.to_str().expect("utf8")),
        trellis = quote(env!("CARGO_BIN_EXE_trellis")),
        dsn = quote(dsn),
        pg_dump = quote(real_pg_dump().to_str().expect("utf8")),
    );
    let fake = bin.join("pg_dump");
    std::fs::write(&fake, script).expect("fake pg_dump");
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    let _ = std::fs::remove_file(&counter);
}

#[tokio::test]
async fn a_define_racing_pg_dump_redoes_the_dump() {
    let cluster = TestCluster::start();
    let (source, _raw) = source_database(&cluster).await;
    let dir = scratch("race");
    let bin = dir.join("bin");
    std::fs::create_dir_all(&bin).expect("bin dir");
    let path = std::env::join_paths(std::iter::once(bin.clone()).chain(std::env::split_paths(
        &std::env::var_os("PATH").expect("PATH"),
    )))
    .expect("PATH");

    // The first attempt's pg_dump sees raced1, its first read of the
    // definitions doesn't: the second attempt has both.
    racing_pg_dump(&bin, source.dsn(), "raced", 1);
    let dump = succeeded(
        &trellis_cli(source.dsn(), &["dump"], Some(Path::new(&path))),
        "dump",
    );
    assert!(dump.contains("CREATE TABLE public.raced1"), "{dump}");
    assert!(dump.contains("'public.raced1', null,"), "{dump}");
    assert_eq!(
        std::fs::read_to_string(bin.join("runs"))
            .expect("runs")
            .trim(),
        "2",
        "the dump was taken twice"
    );

    // A define during each of three attempts.
    racing_pg_dump(&bin, source.dsn(), "again", 3);
    let out = dir.join("dump.sql");
    let stderr = failed(
        &trellis_cli(
            source.dsn(),
            &["dump", "-o", out.to_str().expect("utf8")],
            Some(Path::new(&path)),
        ),
        "dump racing three defines",
    );
    assert!(
        stderr.contains("changed during each of 3 dumps"),
        "{stderr}"
    );
    assert!(!out.exists(), "nothing is written");
}
