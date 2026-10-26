//! Throwaway certificates for [`super::TestCluster::start_with_tls`], made
//! with the `openssl` command line (a test host that runs Postgres has it).

use std::fs;
use std::path::Path;
use std::process::Command;

// Every certificate carries explicit extensions. Without any, `openssl x509
// -req` writes an X.509 version 1 certificate on OpenSSL 3.0 (rustls rejects
// those), while newer releases add extensions on their own; naming them makes
// the output the same on every version.

/// A CA: may sign, and nothing else.
const CA_EXTENSIONS: [&str; 2] = [
    "basicConstraints=critical,CA:TRUE",
    "keyUsage=critical,keyCertSign,cRLSign",
];

/// The server's host names: `localhost` and loopback, so `verify-full`
/// passes against either spelling.
const SERVER_EXTENSIONS: [&str; 3] = [
    "basicConstraints=critical,CA:FALSE",
    "extendedKeyUsage=serverAuth",
    "subjectAltName=DNS:localhost,IP:127.0.0.1",
];

const CLIENT_EXTENSIONS: [&str; 2] = [
    "basicConstraints=critical,CA:FALSE",
    "extendedKeyUsage=clientAuth",
];

fn openssl(dir: &Path, args: &[&str]) {
    let output = Command::new("openssl")
        .args(args)
        .current_dir(dir)
        .output()
        .expect("run openssl (the TLS test cluster needs it on PATH)");
    assert!(
        output.status.success(),
        "openssl {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn self_signed_ca(dir: &Path, name: &str) {
    let (key, cert, subject) = (
        format!("{name}.key"),
        format!("{name}.crt"),
        format!("/CN=trellis-test-{name}"),
    );
    let mut args = vec![
        "req",
        "-x509",
        "-newkey",
        "ec",
        "-pkeyopt",
        "ec_paramgen_curve:prime256v1",
        "-nodes",
        "-keyout",
        &key,
        "-out",
        &cert,
        "-subj",
        &subject,
        "-days",
        "2",
    ];
    for extension in CA_EXTENSIONS {
        args.extend(["-addext", extension]);
    }
    openssl(dir, &args);
}

fn signed_by_ca(dir: &Path, name: &str, common_name: &str, extensions: &[&str]) {
    openssl(
        dir,
        &[
            "req",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:prime256v1",
            "-nodes",
            "-keyout",
            &format!("{name}.key"),
            "-out",
            &format!("{name}.csr"),
            "-subj",
            &format!("/CN={common_name}"),
        ],
    );
    let ext_file = format!("{name}.ext");
    let mut args: Vec<String> = [
        "x509",
        "-req",
        "-in",
        &format!("{name}.csr"),
        "-CA",
        "ca.crt",
        "-CAkey",
        "ca.key",
        "-CAcreateserial",
        "-out",
        &format!("{name}.crt"),
        "-days",
        "2",
    ]
    .map(String::from)
    .to_vec();
    fs::write(dir.join(&ext_file), extensions.join("\n") + "\n").expect("write extensions");
    args.extend(["-extfile".to_string(), ext_file]);
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    openssl(dir, &args);
}

/// Writes into `dir`: `ca.crt` (signs the rest), `server.crt`/`server.key`
/// (named `localhost`/`127.0.0.1`), `client.crt`/`client.key` (common name
/// `postgres`, the test user), `other-ca.crt` (a CA that signed nothing), and
/// `pg_hba.conf`. The hba file accepts a TLS session on loopback, and on
/// the database `certdb` only a client certificate. Plain TCP is refused.
pub fn write_material(dir: &Path) {
    fs::create_dir_all(dir).expect("create tls dir");
    self_signed_ca(dir, "ca");
    self_signed_ca(dir, "other-ca");
    signed_by_ca(dir, "server", "localhost", &SERVER_EXTENSIONS);
    signed_by_ca(dir, "client", "postgres", &CLIENT_EXTENSIONS);
    // Postgres refuses a key file that others can read.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(dir.join("server.key"), fs::Permissions::from_mode(0o600))
            .expect("restrict server.key");
    }
    fs::write(
        dir.join("pg_hba.conf"),
        "local all all trust\n\
         hostssl certdb all 127.0.0.1/32 cert\n\
         hostssl all all 127.0.0.1/32 trust\n",
    )
    .expect("write pg_hba.conf");
}

/// The `-c` settings that turn TLS on, with the files in `dir`.
pub(super) fn settings(dir: &Path) -> Vec<String> {
    let file = |name: &str| dir.join(name).display().to_string();
    vec![
        "ssl=on".to_string(),
        format!("ssl_cert_file={}", file("server.crt")),
        format!("ssl_key_file={}", file("server.key")),
        format!("ssl_ca_file={}", file("ca.crt")),
        format!("hba_file={}", file("pg_hba.conf")),
    ]
}
