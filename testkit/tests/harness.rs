//! Tests of the harness itself (issue #21): that it spins up and tears down
//! cleanly, that isolated databases don't bleed into each other, that a
//! restart keeps the data while severing every connection, and that a cold
//! backup restores into an independent cluster (issue #236).

use std::process::{Command, Stdio};
use testkit::{StopMode, TestCluster, fixtures};

#[tokio::test]
async fn harness_starts_runs_a_query_and_tears_down_cleanly() {
    let cluster = TestCluster::start();
    let root = cluster.root().to_path_buf();
    let pid = cluster.server_pid();

    let db = cluster.create_empty_database().await;
    let row = db
        .pool
        .get()
        .await
        .expect("acquire connection")
        .query_one("select 1", &[])
        .await
        .expect("select 1");
    let value: i32 = row.get(0);
    assert_eq!(value, 1);

    // Line 7 of `postmaster.pid` is the SysV segment's "key id" pair. Read
    // it before teardown deletes the file. The shm checks are Linux-only
    // because they read `/proc/sysvipc/shm`.
    #[cfg(target_os = "linux")]
    let shmid: String = std::fs::read_to_string(root.join("data").join("postmaster.pid"))
        .expect("read postmaster.pid")
        .lines()
        .nth(6)
        .and_then(|line| line.split_whitespace().nth(1))
        .expect("shmem key/id line in postmaster.pid")
        .to_string();
    // Guard the post-teardown check below against passing vacuously (a
    // misparsed id, or a Postgres that stopped using a SysV segment).
    #[cfg(target_os = "linux")]
    assert!(
        sysv_segment_live(&shmid),
        "shared memory segment {shmid} from postmaster.pid should be live before teardown"
    );

    // Dynamic shared memory lives inside the temp dir (where teardown and
    // the orphan reaper delete it), not in `/dev/shm`, where a SIGKILLed
    // server would leak it. The control segment exists from startup.
    let dynshmem = root.join("data").join("pg_dynshmem");
    assert!(
        std::fs::read_dir(&dynshmem)
            .expect("read pg_dynshmem")
            .next()
            .is_some(),
        "dynamic shared memory segments should be in {}",
        dynshmem.display()
    );

    drop(db);
    drop(cluster);

    assert!(!root.exists(), "temp dir should be removed on teardown");

    // Teardown stops the server with `pg_ctl stop -m immediate`, which must
    // still free the SysV segment (issue #43: leaked segments break every
    // later `initdb`).
    #[cfg(target_os = "linux")]
    assert!(
        !sysv_segment_live(&shmid),
        "shared memory segment {shmid} should be freed on teardown"
    );

    // `kill -0` sends no signal; it just checks whether the process still
    // exists. A non-zero exit means it doesn't (ESRCH), proving the server
    // was actually terminated rather than merely disconnected from.
    let still_alive = Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false);
    assert!(
        !still_alive,
        "postgres process {pid} should have exited on teardown"
    );
}

/// Whether SysV shared memory segment `shmid` currently exists.
/// `/proc/sysvipc/shm` lists live segments with the id in its second column.
#[cfg(target_os = "linux")]
fn sysv_segment_live(shmid: &str) -> bool {
    std::fs::read_to_string("/proc/sysvipc/shm")
        .expect("read /proc/sysvipc/shm")
        .lines()
        .skip(1)
        .any(|line| line.split_whitespace().nth(1) == Some(shmid))
}

#[tokio::test]
async fn isolated_databases_do_not_bleed_into_each_other() {
    let cluster = TestCluster::start();
    let db_a = cluster.create_isolated_database().await;
    let db_b = cluster.create_isolated_database().await;

    assert_ne!(db_a.name(), db_b.name(), "each database gets a unique name");

    fixtures::create_source_table(&db_a.pool, "widgets").await;
    fixtures::create_source_table(&db_b.pool, "widgets").await;

    // Same table name, same id, different content, run concurrently: if
    // isolation didn't hold (e.g. both landed in the same database) this
    // would either collide on the primary key or read back the wrong
    // payload.
    let (rows_a, rows_b) = tokio::join!(
        async {
            fixtures::insert_row(&db_a.pool, "widgets", 1, "from-a").await;
            fixtures::read_rows(&db_a.pool, "widgets").await
        },
        async {
            fixtures::insert_row(&db_b.pool, "widgets", 1, "from-b").await;
            fixtures::read_rows(&db_b.pool, "widgets").await
        },
    );

    assert_eq!(rows_a, vec![(1, "from-a".to_string())]);
    assert_eq!(rows_b, vec![(1, "from-b".to_string())]);
}

/// Issue #665: every test database collates the same on every machine,
/// whatever `LANG` the test runs under. Unpinned, this box's `en_US.UTF-8`
/// leaked in while CI got `C.UTF-8`, and a test's `ORDER BY` could pass here
/// and fail on CI. The default is ICU `en-US`, so a query that needs bytewise
/// order has to say `collate "C"` everywhere, not just on CI.
#[tokio::test]
async fn test_databases_default_to_the_icu_en_us_collation() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    let client = db.pool.get().await.expect("get connection");
    let row = client
        .query_one(
            // PG 17 renamed `daticulocale` to `datlocale`; CI runs 16.
            "select datlocprovider::text, \
                    coalesce(to_jsonb(d) ->> 'datlocale', to_jsonb(d) ->> 'daticulocale'), \
                    datcollate, datctype, pg_encoding_to_char(encoding) \
             from pg_database d where datname = current_database()",
            &[],
        )
        .await
        .expect("read the database's locale");
    let locale: (String, Option<String>, String, String, String) =
        (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4));
    assert_eq!(
        locale,
        (
            "i".to_string(),
            Some("en-US".to_string()),
            "C.UTF-8".to_string(),
            "C.UTF-8".to_string(),
            "UTF8".to_string(),
        ),
        "(provider, ICU locale, lc_collate, lc_ctype, encoding)"
    );

    // The point of the pin: the default collation is linguistic, so 'a'
    // sorts before 'B', where bytewise ("C") order would put 'B' first.
    let order: Vec<String> = client
        .query("select v from (values ('B'), ('a')) t(v) order by v", &[])
        .await
        .expect("order under the default collation")
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(order, ["a", "B"]);
}

/// Issue #236: `restart` in either mode brings the same cluster back — a new
/// server process, every existing connection severed, and the data intact.
///
/// Multi-threaded so each pooled connection's driver task observes the
/// server closing it while `restart` blocks this thread: a pooled
/// connection whose close nobody has observed yet looks healthy to the
/// pool's recycle check and fails its first query.
#[tokio::test(flavor = "multi_thread")]
async fn restart_severs_connections_and_keeps_data() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    fixtures::create_source_table(&db.pool, "widgets").await;
    fixtures::insert_row(&db.pool, "widgets", 1, "before").await;

    let (raw, connection) = tokio_postgres::connect(db.dsn(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    raw.query_one("select 1", &[])
        .await
        .expect("the connection works before the restart");

    for mode in [StopMode::Fast, StopMode::Immediate] {
        let pid = cluster.server_pid();
        cluster.restart(mode);
        assert_ne!(cluster.server_pid(), pid, "{mode:?}: a new server process");
        fixtures::insert_row(&db.pool, "widgets", 2 + mode as i64, "after").await;
    }

    assert!(
        raw.query_one("select 1", &[]).await.is_err(),
        "a connection opened before the restart must be severed"
    );
    let rows = fixtures::read_rows(&db.pool, "widgets").await;
    assert_eq!(
        rows,
        vec![
            (1, "before".to_string()),
            (2, "after".to_string()),
            (3, "after".to_string()),
        ]
    );
}

/// Issue #236: a cold backup restores into an independent cluster holding
/// the data exactly as it was at the backup,
/// and later writes to either cluster don't reach the other. Both the
/// backup's and the restored cluster's temp directories go on drop.
#[tokio::test(flavor = "multi_thread")]
async fn a_cold_backup_restores_data_as_of_the_backup() {
    let cluster = TestCluster::start();
    let db = cluster.create_isolated_database().await;
    fixtures::create_source_table(&db.pool, "widgets").await;
    fixtures::insert_row(&db.pool, "widgets", 1, "before").await;

    let backup = cluster.cold_backup();
    let backup_root = backup.root().to_path_buf();
    assert!(backup_root.exists());

    // The original comes back up, and moves on past the backup.
    fixtures::insert_row(&db.pool, "widgets", 2, "after").await;

    let restored = TestCluster::from_backup(&backup);
    drop(backup);
    assert!(
        !backup_root.exists(),
        "the backup's copy is deleted on drop"
    );
    let restored_root = restored.root().to_path_buf();
    assert_ne!(restored_root, cluster.root());

    let config =
        trellis::Config::from_dsn(restored.database_dsn(db.name())).expect("restored config");
    let restored_pool = trellis::Pool::new(&config).expect("restored pool");
    assert_eq!(
        fixtures::read_rows(&restored_pool, "widgets").await,
        vec![(1, "before".to_string())],
        "the restore holds exactly what the backup did"
    );
    fixtures::insert_row(&restored_pool, "widgets", 3, "restored").await;
    assert_eq!(
        fixtures::read_rows(&db.pool, "widgets").await,
        vec![(1, "before".to_string()), (2, "after".to_string())],
        "a write to the restore doesn't reach the original"
    );

    drop(restored_pool);
    drop(restored);
    assert!(
        !restored_root.exists(),
        "the restored cluster's dir is deleted on drop"
    );
}
