//! TLS between Trellis and Postgres, against a cluster with `ssl = on`
//! ([`TestCluster::start_with_tls`]): each `sslmode`, the certificate checks,
//! client-certificate authentication, and the refusals.
//!
//! `pg_stat_ssl` is the proof that a session is encrypted: the server reports
//! it for the backend itself, so a client that only claimed TLS would show
//! `false`.

use testkit::TestCluster;
use tokio_postgres::GenericClient;
use trellis::config::DEFAULT_SCHEMA;
use trellis::staging::ProducerSession;
use trellis::{Config, Error, Pool};

const PASSWORD: &str = "pw-must-not-leak";

/// Whether the session behind `client` is encrypted, as its backend reports.
async fn is_encrypted(client: &impl GenericClient) -> bool {
    client
        .query_one(
            "select ssl from pg_stat_ssl where pid = pg_backend_pid()",
            &[],
        )
        .await
        .expect("read pg_stat_ssl")
        .get(0)
}

fn dsn(cluster: &TestCluster, database: &str, host: &str, options: &str) -> String {
    format!(
        "host={host} port={} user=postgres password={PASSWORD} dbname={database} {options}",
        cluster.port()
    )
}

fn path(cluster: &TestCluster, file: &str) -> String {
    format!("'{}'", cluster.tls_dir().join(file).display())
}

async fn pool_is_encrypted(dsn: String) -> Result<bool, Error> {
    let pool = Pool::new(&Config::from_dsn(dsn)?)?;
    let client = pool.get().await?;
    Ok(is_encrypted(&**client).await)
}

/// Asserts `err` carries no secret and (its `Debug`, which holds the cause)
/// names `reason`.
fn assert_refused_because(err: &Error, reason: &str) {
    let text = format!("{err} {err:?}");
    assert!(!text.contains(PASSWORD), "password in error: {text}");
    assert!(text.contains(reason), "expected {reason:?} in: {text}");
}

#[tokio::test]
async fn every_encrypting_mode_encrypts_the_pool_and_a_dedicated_session() {
    let cluster = TestCluster::start_with_tls();
    let db = cluster.create_isolated_database().await;
    let ca = path(&cluster, "ca.crt");
    for host in ["127.0.0.1", "localhost"] {
        for options in [
            String::new(),
            "sslmode=prefer".to_string(),
            "sslmode=require".to_string(),
            format!("sslmode=verify-ca sslrootcert={ca}"),
            format!("sslmode=verify-full sslrootcert={ca}"),
        ] {
            let dsn = dsn(&cluster, db.name(), host, &options);
            assert!(
                pool_is_encrypted(dsn.clone()).await.expect("pooled"),
                "pool not encrypted: {host} {options}"
            );
            let session = ProducerSession::connect(&dsn, DEFAULT_SCHEMA)
                .await
                .expect("dedicated session");
            assert!(
                is_encrypted(session.client()).await,
                "session not encrypted: {host} {options}"
            );
        }
    }
}

#[tokio::test]
async fn a_server_certificate_from_another_ca_is_refused() {
    let cluster = TestCluster::start_with_tls();
    let db = cluster.create_isolated_database().await;
    let other = path(&cluster, "other-ca.crt");
    for mode in ["verify-ca", "verify-full"] {
        let dsn = dsn(
            &cluster,
            db.name(),
            "127.0.0.1",
            &format!("sslmode={mode} sslrootcert={other}"),
        );
        let err = pool_is_encrypted(dsn.clone()).await.expect_err("refused");
        assert_refused_because(&err, "UnknownIssuer");
        let err = ProducerSession::connect(&dsn, DEFAULT_SCHEMA)
            .await
            .map(|_| ())
            .expect_err("refused");
        let text = format!("{err} {err:?}");
        assert!(!text.contains(PASSWORD), "{text}");
    }
}

#[tokio::test]
async fn a_certificate_for_another_name_fails_verify_full_but_passes_verify_ca() {
    let cluster = TestCluster::start_with_tls();
    let db = cluster.create_isolated_database().await;
    let ca = path(&cluster, "ca.crt");
    // `hostaddr` reaches the server while `host` is the name to verify.
    let wrong_name = |mode: &str| {
        dsn(
            &cluster,
            db.name(),
            "wrong.example",
            &format!("hostaddr=127.0.0.1 sslmode={mode} sslrootcert={ca}"),
        )
    };
    let err = pool_is_encrypted(wrong_name("verify-full"))
        .await
        .expect_err("name mismatch");
    assert_refused_because(&err, "NotValidForName");
    assert!(pool_is_encrypted(wrong_name("verify-ca")).await.unwrap());
}

#[tokio::test]
async fn require_fails_on_a_server_without_tls_and_prefer_falls_back_to_plaintext() {
    let cluster = TestCluster::start_with_tcp();
    let db = cluster.create_isolated_database().await;
    let plain = |options: &str| dsn(&cluster, db.name(), "127.0.0.1", options);

    let err = pool_is_encrypted(plain("sslmode=require"))
        .await
        .expect_err("no TLS on the server");
    assert_refused_because(&err, "server does not support TLS");
    assert!(
        !pool_is_encrypted(plain("")).await.expect("default prefer"),
        "the default must fall back to plaintext on a server without TLS"
    );
    assert!(!pool_is_encrypted(plain("sslmode=disable")).await.unwrap());
}

#[tokio::test]
async fn disable_is_refused_by_a_server_that_only_accepts_tls() {
    let cluster = TestCluster::start_with_tls();
    let db = cluster.create_isolated_database().await;
    let err = pool_is_encrypted(dsn(&cluster, db.name(), "127.0.0.1", "sslmode=disable"))
        .await
        .expect_err("hostssl only");
    assert_refused_because(&err, "no pg_hba.conf entry");
}

#[tokio::test]
async fn a_client_certificate_authenticates_where_the_server_demands_one() {
    let cluster = TestCluster::start_with_tls();
    cluster.create_database("certdb");
    let ca = path(&cluster, "ca.crt");
    let base = format!("sslmode=verify-full sslrootcert={ca}");

    let err = pool_is_encrypted(dsn(&cluster, "certdb", "127.0.0.1", &base))
        .await
        .expect_err("no client certificate");
    assert_refused_because(&err, "valid client certificate");

    let with_cert = format!(
        "{base} sslcert={} sslkey={}",
        path(&cluster, "client.crt"),
        path(&cluster, "client.key")
    );
    assert!(
        pool_is_encrypted(dsn(&cluster, "certdb", "127.0.0.1", &with_cert))
            .await
            .expect("client certificate accepted")
    );
    // A certificate that no trusted CA signed is not accepted either.
    let err = pool_is_encrypted(dsn(
        &cluster,
        "certdb",
        "127.0.0.1",
        &format!(
            "{base} sslcert={} sslkey={}",
            path(&cluster, "other-ca.crt"),
            path(&cluster, "client.key")
        ),
    ))
    .await
    .expect_err("mismatched certificate and key");
    assert_refused_because(&err, "do not form a usable pair");
}

#[tokio::test]
async fn a_mode_that_needs_encryption_is_a_config_error_on_a_unix_socket_host() {
    let cluster = TestCluster::start_with_tls();
    let db = cluster.create_isolated_database().await;
    let socket = cluster.socket_dir().display().to_string();
    for mode in ["require", "verify-full"] {
        let dsn = format!(
            "host={socket} port={} user=postgres dbname={} sslmode={mode} sslrootcert={}",
            cluster.port(),
            db.name(),
            path(&cluster, "ca.crt")
        );
        let err = Pool::new(&Config::from_dsn(dsn.clone()).unwrap()).expect_err("unix socket");
        assert!(matches!(err, Error::Config(_)), "{err}");
        assert!(err.to_string().contains("unix-socket"), "{err}");
        let err = ProducerSession::connect(&dsn, DEFAULT_SCHEMA)
            .await
            .map(|_| ())
            .expect_err("unix socket");
        assert!(format!("{err}").contains("unix-socket"), "{err}");
    }
    // The socket itself works with the modes that allow plaintext.
    let plain = format!(
        "host={socket} port={} user=postgres dbname={}",
        cluster.port(),
        db.name()
    );
    assert!(!pool_is_encrypted(plain).await.unwrap());
}

/// `prefer` falls back to plaintext only when the server answers that it
/// has no TLS. A server that offers TLS but whose handshake fails (here: it
/// speaks only protocol versions rustls refuses) is an error, never a
/// silent plaintext session. The server's `hostssl`-only `pg_hba.conf`
/// would refuse a plaintext session too, so the error must be the
/// handshake's, not that refusal.
#[tokio::test]
async fn a_failed_handshake_is_an_error_under_prefer_not_a_plaintext_retry() {
    let cluster = TestCluster::start_with_tls_settings(&[
        "ssl_min_protocol_version=TLSv1",
        "ssl_max_protocol_version=TLSv1.1",
    ]);
    let db = cluster.create_isolated_database().await;
    for options in ["", "sslmode=prefer", "sslmode=require"] {
        let dsn = dsn(&cluster, db.name(), "127.0.0.1", options);
        let err = pool_is_encrypted(dsn.clone())
            .await
            .expect_err("the handshake fails");
        let text = format!("{err} {err:?}");
        assert!(!text.contains(PASSWORD), "{text}");
        assert!(
            !text.contains("pg_hba.conf"),
            "fell back to plaintext: {text}"
        );
        assert!(text.contains("TLS handshake"), "{options:?}: {text}");
    }
}

/// A DSN that names only `hostaddr` has no host name for the handshake.
/// The modes that check no name still encrypt.
#[tokio::test]
async fn a_hostaddr_without_a_host_encrypts_in_the_unverified_modes() {
    let cluster = TestCluster::start_with_tls();
    let db = cluster.create_isolated_database().await;
    for options in ["", "sslmode=prefer", "sslmode=require"] {
        let dsn = format!(
            "hostaddr=127.0.0.1 port={} user=postgres dbname={} {options}",
            cluster.port(),
            db.name()
        );
        assert!(
            pool_is_encrypted(dsn.clone()).await.expect("pooled"),
            "{options:?}"
        );
        let session = ProducerSession::connect(&dsn, DEFAULT_SCHEMA)
            .await
            .expect("dedicated session");
        assert!(is_encrypted(session.client()).await, "{options:?}");
    }
}

/// libpq's rule: `sslmode=require` with an `sslrootcert` verifies the chain
/// as `verify-ca` does (the name is not checked); without one it checks
/// nothing, so it connects although no certificate is trusted at all.
#[tokio::test]
async fn require_with_a_root_cert_verifies_the_chain_and_without_one_checks_nothing() {
    let cluster = TestCluster::start_with_tls();
    let db = cluster.create_isolated_database().await;
    // `hostaddr` reaches the server; `host` is a name the certificate lacks.
    let require = |root: &str| {
        let root = match root {
            "" => String::new(),
            file => format!("sslrootcert={}", path(&cluster, file)),
        };
        dsn(
            &cluster,
            db.name(),
            "wrong.example",
            &format!("hostaddr=127.0.0.1 sslmode=require {root}"),
        )
    };
    assert!(
        pool_is_encrypted(require("ca.crt"))
            .await
            .expect("right CA")
    );
    let session = ProducerSession::connect(&require("ca.crt"), DEFAULT_SCHEMA)
        .await
        .expect("dedicated session, right CA");
    assert!(is_encrypted(session.client()).await);

    let err = pool_is_encrypted(require("other-ca.crt"))
        .await
        .expect_err("another CA");
    assert_refused_because(&err, "UnknownIssuer");
    let err = ProducerSession::connect(&require("other-ca.crt"), DEFAULT_SCHEMA)
        .await
        .map(|_| ())
        .expect_err("another CA");
    assert!(format!("{err:?}").contains("UnknownIssuer"), "{err:?}");

    assert!(pool_is_encrypted(require("")).await.expect("no root cert"));
}
