//! TLS for the connections Trellis opens to Postgres.
//!
//! `tokio_postgres`'s own DSN parser knows only `sslmode` =
//! `disable | prefer | require` and rejects every other `ssl*` key. This
//! module reads the TLS options out of the DSN first ([`split_dsn`]), hands
//! the rest to that parser, and builds a rustls connector from them
//! ([`TlsSpec::connector`]). The options and their rules are documented for
//! users in `docs/embedding.md` (the "TLS" section).
//!
//! Nothing here puts a secret in an error: the messages are fixed text plus
//! file paths, never a DSN fragment or file contents.

use crate::error::Error;
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::WebPkiSupportedAlgorithms;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, Error as TlsError, RootCertStore,
    SignatureScheme,
};
use std::fmt;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres_rustls::MakeRustlsConnect;

/// The `sslmode` values Trellis accepts, with libpq's meanings. `allow` is
/// not among them: it is "try plaintext first", which has no use here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SslMode {
    /// Never use TLS.
    Disable,
    /// Use TLS when the server offers it, without checking its certificate;
    /// plaintext only when the server answers that it has no TLS. The default.
    Prefer,
    /// Require TLS, without checking the server's certificate.
    Require,
    /// Require TLS and a server certificate that chains to `sslrootcert`.
    VerifyCa,
    /// [`SslMode::VerifyCa`], plus the certificate must name the host.
    VerifyFull,
}

impl SslMode {
    fn parse(value: &str) -> Result<Self, Error> {
        match value {
            "disable" => Ok(Self::Disable),
            "prefer" => Ok(Self::Prefer),
            "require" => Ok(Self::Require),
            "verify-ca" => Ok(Self::VerifyCa),
            "verify-full" => Ok(Self::VerifyFull),
            _ => Err(config_err(
                "sslmode must be one of disable, prefer, require, verify-ca or verify-full \
                 (allow is not supported)",
            )),
        }
    }

    fn verifies(self) -> bool {
        matches!(self, Self::VerifyCa | Self::VerifyFull)
    }

    /// The mode `tokio_postgres` gets: it only decides whether to offer TLS
    /// and whether a refusal is fatal.
    fn for_tokio(self) -> tokio_postgres::config::SslMode {
        match self {
            Self::Disable => tokio_postgres::config::SslMode::Disable,
            Self::Prefer => tokio_postgres::config::SslMode::Prefer,
            Self::Require | Self::VerifyCa | Self::VerifyFull => {
                tokio_postgres::config::SslMode::Require
            }
        }
    }
}

/// The TLS options of one DSN.
#[derive(Debug, Clone)]
pub(crate) struct TlsSpec {
    mode: SslMode,
    root_cert: Option<PathBuf>,
    client_cert: Option<PathBuf>,
    client_key: Option<PathBuf>,
}

impl Default for TlsSpec {
    fn default() -> Self {
        Self {
            mode: SslMode::Prefer,
            root_cert: None,
            client_cert: None,
            client_key: None,
        }
    }
}

/// libpq `ssl*` options Trellis does not support, named in the error so the
/// user knows it was seen and refused. Any other unknown key is left to
/// `tokio_postgres`, whose rejection is replaced by the fixed parse error.
const UNSUPPORTED_KEYS: &[&str] = &[
    "sslpassword",
    "sslcrl",
    "sslcrldir",
    "sslsni",
    "sslcompression",
    "sslcertmode",
    "sslkeylogfile",
    "sslnegotiation",
    "ssl_min_protocol_version",
    "ssl_max_protocol_version",
];

fn config_err(message: &str) -> Error {
    Error::Config(format!(
        "{message} (see the TLS section of docs/embedding.md)"
    ))
}

impl TlsSpec {
    fn from_params(params: &[(String, String)]) -> Result<Self, Error> {
        let mut spec = Self::default();
        let mut certs = (None, None);
        for (key, value) in params {
            let path = || (!value.is_empty()).then(|| PathBuf::from(value));
            match key.as_str() {
                "sslmode" => spec.mode = SslMode::parse(value)?,
                "sslrootcert" => spec.root_cert = path(),
                "sslcert" => certs.0 = path(),
                "sslkey" => certs.1 = path(),
                other => {
                    return Err(Error::Config(format!(
                        "the connection option {other} is not supported; the TLS options are \
                         sslmode, sslrootcert, sslcert and sslkey (see the TLS section of \
                         docs/embedding.md)"
                    )));
                }
            }
        }
        match certs {
            (Some(cert), Some(key)) => {
                spec.client_cert = Some(cert);
                spec.client_key = Some(key);
            }
            (None, None) => {}
            _ => return Err(config_err("sslcert and sslkey must be given together")),
        }
        if spec.mode.verifies() && spec.root_cert.is_none() {
            return Err(config_err(
                "sslmode=verify-ca and verify-full need sslrootcert, a PEM file of the CA \
                 certificate(s) that signed the server's certificate",
            ));
        }
        Ok(spec)
    }

    /// Whether this spec forbids a plaintext session, so a unix-socket host
    /// (where `tokio_postgres` never uses TLS) would silently break it.
    fn demands_encryption(&self) -> bool {
        self.mode != SslMode::Disable && self.mode != SslMode::Prefer
    }

    /// Applies the mode to `config` and refuses a unix-socket host when the
    /// mode demands encryption.
    pub(crate) fn apply(&self, config: &mut tokio_postgres::Config) -> Result<(), Error> {
        #[cfg(unix)]
        if self.demands_encryption()
            && config
                .get_hosts()
                .iter()
                .any(|host| matches!(host, tokio_postgres::config::Host::Unix(_)))
        {
            return Err(config_err(
                "sslmode=require, verify-ca and verify-full cannot be used with a unix-socket \
                 host, which is never encrypted; use a TCP host, or sslmode=disable or prefer \
                 for the socket",
            ));
        }
        config.ssl_mode(self.mode.for_tokio());
        Ok(())
    }

    /// Builds the rustls connector, reading the certificate and key files.
    pub(crate) fn connector(&self) -> Result<Tls, Error> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier: Arc<dyn ServerCertVerifier> = match (self.mode, &self.root_cert) {
            (SslMode::VerifyFull | SslMode::VerifyCa, Some(root)) => {
                let mut roots = RootCertStore::empty();
                let certs = read_certs(root, "sslrootcert")?;
                for cert in certs {
                    roots.add(cert).map_err(|e| {
                        Error::Config(format!(
                            "sslrootcert {}: not a usable CA certificate: {e}",
                            root.display()
                        ))
                    })?;
                }
                let inner =
                    WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider.clone())
                        .build()
                        .map_err(|e| Error::Config(format!("sslrootcert is not usable: {e}")))?;
                if self.mode == SslMode::VerifyCa {
                    Arc::new(IgnoreServerName { inner })
                } else {
                    inner
                }
            }
            _ => Arc::new(AcceptAnyCert {
                algorithms: provider.signature_verification_algorithms,
            }),
        };
        let builder = ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(|e| Error::Config(format!("TLS setup failed: {e}")))?
            .dangerous()
            .with_custom_certificate_verifier(verifier);
        let config = match (&self.client_cert, &self.client_key) {
            (Some(cert), Some(key)) if self.mode != SslMode::Disable => {
                let chain = read_certs(cert, "sslcert")?;
                let key = read_key(key)?;
                builder.with_client_auth_cert(chain, key).map_err(|e| {
                    Error::Config(format!("sslcert and sslkey do not form a usable pair: {e}"))
                })?
            }
            _ => builder.with_no_client_auth(),
        };
        Ok(Tls {
            inner: MakeRustlsConnect::new(config),
            verifies: self.mode.verifies(),
        })
    }
}

/// The connector `tokio_postgres` connects with. Cheap to clone.
///
/// `tokio_postgres` asks for a connector for every host, with an empty name
/// for a unix socket, and `MakeRustlsConnect` refuses a name that is not a
/// valid DNS name or IP address. A name nothing checks (a socket path, or any
/// host in a mode that does not verify) is swapped for a placeholder instead,
/// so such a DSN connects as it would with libpq.
#[derive(Clone)]
pub(crate) struct Tls {
    inner: MakeRustlsConnect,
    verifies: bool,
}

impl fmt::Debug for Tls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Tls")
    }
}

impl<S> MakeTlsConnect<S> for Tls
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = <MakeRustlsConnect as MakeTlsConnect<S>>::Stream;
    type TlsConnect = <MakeRustlsConnect as MakeTlsConnect<S>>::TlsConnect;
    type Error = <MakeRustlsConnect as MakeTlsConnect<S>>::Error;

    fn make_tls_connect(&mut self, hostname: &str) -> Result<Self::TlsConnect, Self::Error> {
        match MakeTlsConnect::<S>::make_tls_connect(&mut self.inner, hostname) {
            Err(_) if !self.verifies => {
                MakeTlsConnect::<S>::make_tls_connect(&mut self.inner, UNCHECKED_NAME)
            }
            other => other,
        }
    }
}

/// The server name given to a connector when no name will be checked.
const UNCHECKED_NAME: &str = "localhost";

fn open(path: &Path, option: &str) -> Result<BufReader<File>, Error> {
    File::open(path).map(BufReader::new).map_err(|e| {
        Error::Config(format!(
            "{option} {} could not be read: {}",
            path.display(),
            e.kind()
        ))
    })
}

fn read_certs(path: &Path, option: &str) -> Result<Vec<CertificateDer<'static>>, Error> {
    let certs = rustls_pemfile::certs(&mut open(path, option)?)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            Error::Config(format!(
                "{option} {} is not valid PEM: {}",
                path.display(),
                e.kind()
            ))
        })?;
    if certs.is_empty() {
        return Err(Error::Config(format!(
            "{option} {} holds no PEM certificate",
            path.display()
        )));
    }
    Ok(certs)
}

fn read_key(path: &Path) -> Result<rustls::pki_types::PrivateKeyDer<'static>, Error> {
    rustls_pemfile::private_key(&mut open(path, "sslkey")?)
        .map_err(|e| {
            Error::Config(format!(
                "sslkey {} is not valid PEM: {}",
                path.display(),
                e.kind()
            ))
        })?
        .ok_or_else(|| {
            Error::Config(format!(
                "sslkey {} holds no PEM private key (an encrypted key is not supported)",
                path.display()
            ))
        })
}

/// `require` and `prefer`: encrypts, and trusts whatever certificate the
/// server presents. Handshake signatures are still checked, so the session
/// is a real TLS session with the peer that holds the certificate's key.
#[derive(Debug)]
struct AcceptAnyCert {
    algorithms: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for AcceptAnyCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

/// `verify-ca`: the chain must verify against `sslrootcert`, but the
/// certificate need not name the host. The name is checked last, so a
/// not-valid-for-name failure means the chain was good.
#[derive(Debug)]
struct IgnoreServerName {
    inner: Arc<WebPkiServerVerifier>,
}

impl ServerCertVerifier for IgnoreServerName {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        match self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Err(TlsError::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// Splits the TLS options out of `dsn`: returns the DSN without them, for
/// `tokio_postgres`'s parser, and the options.
///
/// A DSN this cannot scan comes back unchanged with the default options, so
/// `tokio_postgres` rejects it and the caller reports the fixed parse error.
pub(crate) fn split_dsn(dsn: &str) -> Result<(String, TlsSpec), Error> {
    let scanned = if dsn.starts_with("postgres://") || dsn.starts_with("postgresql://") {
        split_url(dsn)
    } else {
        split_key_value(dsn)
    };
    let Some((rest, params)) = scanned else {
        return Ok((dsn.to_string(), TlsSpec::default()));
    };
    Ok((rest, TlsSpec::from_params(&params)?))
}

fn is_tls_key(key: &str) -> bool {
    matches!(key, "sslmode" | "sslrootcert" | "sslcert" | "sslkey")
        || UNSUPPORTED_KEYS.contains(&key)
}

/// `key = value` pairs, mirroring `tokio_postgres`'s grammar: whitespace
/// around `=`, a value either single-quoted or ending at whitespace, and a
/// backslash escaping the next character in both.
fn split_key_value(dsn: &str) -> Option<(String, Vec<(String, String)>)> {
    let mut rest = String::new();
    let mut params = Vec::new();
    let mut chars = dsn.char_indices().peekable();
    let mut copied_to = 0;
    loop {
        while chars.next_if(|(_, c)| c.is_whitespace()).is_some() {}
        let Some(&(start, _)) = chars.peek() else {
            break;
        };
        let mut key = String::new();
        while let Some((_, c)) = chars.next_if(|(_, c)| !c.is_whitespace() && *c != '=') {
            key.push(c);
        }
        while chars.next_if(|(_, c)| c.is_whitespace()).is_some() {}
        chars.next_if(|(_, c)| *c == '=')?;
        while chars.next_if(|(_, c)| c.is_whitespace()).is_some() {}
        let mut value = String::new();
        if chars.next_if(|(_, c)| *c == '\'').is_some() {
            loop {
                match chars.next()?.1 {
                    '\'' => break,
                    '\\' => value.push(chars.next()?.1),
                    c => value.push(c),
                }
            }
        } else {
            while let Some((_, c)) = chars.next_if(|(_, c)| !c.is_whitespace()) {
                if c == '\\' {
                    if let Some((_, c2)) = chars.next() {
                        value.push(c2);
                    }
                } else {
                    value.push(c);
                }
            }
        }
        let end = chars.peek().map_or(dsn.len(), |&(i, _)| i);
        if is_tls_key(&key) {
            rest.push_str(&dsn[copied_to..start]);
            copied_to = end;
            params.push((key, value));
        }
    }
    rest.push_str(&dsn[copied_to..]);
    Some((rest, params))
}

fn percent_decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = s.get(i + 1..i + 3)?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// The `?key=value&...` query of a `postgres://` URL, percent-decoded.
fn split_url(dsn: &str) -> Option<(String, Vec<(String, String)>)> {
    // `tokio_postgres` ends the credentials at the first `@`, then the
    // authority and path at the first `?` after that.
    let from = dsn.find('@').unwrap_or(0);
    let Some(question) = dsn[from..].find('?').map(|i| i + from) else {
        return Some((dsn.to_string(), Vec::new()));
    };
    let mut kept = Vec::new();
    let mut params = Vec::new();
    for segment in dsn[question + 1..].split('&') {
        if segment.is_empty() {
            continue;
        }
        let (key, value) = segment.split_once('=')?;
        let key = percent_decode(key)?;
        if is_tls_key(&key) {
            params.push((key, percent_decode(value)?));
        } else {
            kept.push(segment);
        }
    }
    let mut rest = dsn[..question].to_string();
    if !kept.is_empty() {
        rest.push('?');
        rest.push_str(&kept.join("&"));
    }
    Some((rest, params))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(dsn: &str) -> Result<(String, TlsSpec), Error> {
        split_dsn(dsn)
    }

    #[test]
    fn a_dsn_without_tls_options_is_untouched_and_prefers_tls() {
        for dsn in [
            "host=db user=alice",
            "postgres://alice@db/app",
            "postgres://alice@db/app?application_name=x",
        ] {
            let (rest, spec) = spec(dsn).unwrap();
            assert_eq!(rest, dsn);
            assert_eq!(spec.mode, SslMode::Prefer);
            assert!(spec.root_cert.is_none() && spec.client_cert.is_none());
        }
    }

    #[test]
    fn key_value_options_are_extracted_with_quoting_and_escapes() {
        let (rest, spec) = spec(
            "host=db sslmode = verify-full  sslrootcert='/etc/my certs/ca.pem' user=alice \
             sslcert=/c/it\\ is.pem sslkey='/k/it\\'s.pem'",
        )
        .unwrap();
        assert_eq!(spec.mode, SslMode::VerifyFull);
        assert_eq!(spec.root_cert.unwrap(), Path::new("/etc/my certs/ca.pem"));
        assert_eq!(spec.client_cert.unwrap(), Path::new("/c/it is.pem"));
        assert_eq!(spec.client_key.unwrap(), Path::new("/k/it's.pem"));
        let parsed: tokio_postgres::Config = rest.parse().expect("the rest parses");
        assert_eq!(parsed.get_user(), Some("alice"));
    }

    #[test]
    fn a_password_that_looks_like_an_option_is_left_alone() {
        let (rest, spec) = spec("host=db password='sslmode=disable' user=a").unwrap();
        assert_eq!(spec.mode, SslMode::Prefer);
        assert_eq!(rest, "host=db password='sslmode=disable' user=a");
    }

    #[test]
    fn url_options_are_extracted_and_percent_decoded() {
        let (rest, spec) = spec(
            "postgres://alice:p%40ss@db:5433/app?sslmode=verify-ca&application_name=x\
             &sslrootcert=%2Fetc%2Fmy%20certs%2Fca.pem",
        )
        .unwrap();
        assert_eq!(spec.mode, SslMode::VerifyCa);
        assert_eq!(spec.root_cert.unwrap(), Path::new("/etc/my certs/ca.pem"));
        assert_eq!(
            rest,
            "postgres://alice:p%40ss@db:5433/app?application_name=x"
        );
        let (rest, _) = self::spec("postgres://db/app?sslmode=require").unwrap();
        assert_eq!(rest, "postgres://db/app");
    }

    #[test]
    fn every_documented_mode_parses() {
        for (text, mode) in [
            ("disable", SslMode::Disable),
            ("prefer", SslMode::Prefer),
            ("require", SslMode::Require),
        ] {
            let (_, s) = spec(&format!("host=db sslmode={text}")).unwrap();
            assert_eq!(s.mode, mode);
        }
    }

    #[test]
    fn allow_and_unknown_modes_are_rejected() {
        for mode in ["allow", "verify", "REQUIRE", ""] {
            let err = spec(&format!("host=db sslmode='{mode}'")).unwrap_err();
            assert!(matches!(err, Error::Config(_)), "{mode}: {err}");
            assert!(err.to_string().contains("sslmode must be one of"));
        }
    }

    #[test]
    fn unsupported_ssl_options_are_rejected_by_name() {
        for key in ["sslpassword", "sslcrl", "sslnegotiation"] {
            let err = spec(&format!("host=db {key}=x")).unwrap_err();
            assert!(err.to_string().contains(key), "{err}");
        }
        let err = spec("postgres://db/app?sslpassword=hunter2").unwrap_err();
        assert!(!err.to_string().contains("hunter2"), "{err}");
    }

    #[test]
    fn a_client_cert_needs_its_key_and_the_reverse() {
        for dsn in ["host=db sslcert=/c.pem", "host=db sslkey=/k.pem"] {
            let err = spec(dsn).unwrap_err();
            assert!(err.to_string().contains("together"), "{err}");
        }
        assert!(spec("host=db sslcert=/c.pem sslkey=/k.pem").is_ok());
    }

    #[test]
    fn verifying_modes_need_a_root_cert() {
        for mode in ["verify-ca", "verify-full"] {
            let err = spec(&format!("host=db sslmode={mode}")).unwrap_err();
            assert!(err.to_string().contains("sslrootcert"), "{err}");
            assert!(spec(&format!("host=db sslmode={mode} sslrootcert=/ca.pem")).is_ok());
        }
        assert!(spec("host=db sslmode=require").is_ok());
    }

    #[test]
    fn an_unscannable_dsn_passes_through_for_the_fixed_parse_error() {
        for dsn in ["host=db user", "host='db", "postgres://db/app?sslmode"] {
            let (rest, spec) = split_dsn(dsn).unwrap();
            assert_eq!(rest, dsn);
            assert_eq!(spec.mode, SslMode::Prefer);
        }
    }

    #[test]
    fn a_demanding_mode_on_a_unix_socket_host_is_a_config_error() {
        for mode in ["require", "verify-ca", "verify-full"] {
            let (rest, spec) = split_dsn(&format!(
                "host=/var/run/postgresql sslmode={mode} sslrootcert=/ca.pem"
            ))
            .unwrap();
            let mut config: tokio_postgres::Config = rest.parse().unwrap();
            let err = spec.apply(&mut config).unwrap_err();
            assert!(matches!(err, Error::Config(_)));
            assert!(err.to_string().contains("unix-socket"), "{err}");
        }
        for mode in ["disable", "prefer"] {
            let (rest, spec) =
                split_dsn(&format!("host=/var/run/postgresql sslmode={mode}")).unwrap();
            let mut config: tokio_postgres::Config = rest.parse().unwrap();
            spec.apply(&mut config).unwrap();
        }
        let (rest, spec) = split_dsn("host=db sslmode=require").unwrap();
        let mut config: tokio_postgres::Config = rest.parse().unwrap();
        spec.apply(&mut config).unwrap();
        assert_eq!(
            config.get_ssl_mode(),
            tokio_postgres::config::SslMode::Require
        );
    }

    #[test]
    fn file_errors_name_the_path_and_never_the_contents() {
        let dir = std::env::temp_dir().join(format!("trellis-tls-unit-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let junk = dir.join("junk.pem");
        std::fs::write(&junk, "SECRET-KEY-MATERIAL not pem").unwrap();
        let missing = dir.join("missing.pem");
        for (option, path) in [("sslrootcert", &junk), ("sslrootcert", &missing)] {
            let (_, spec) = split_dsn(&format!(
                "host=db sslmode=verify-full {option}='{}'",
                path.display()
            ))
            .unwrap();
            let err = spec.connector().unwrap_err().to_string();
            assert!(err.contains(&path.display().to_string()), "{err}");
            assert!(!err.contains("SECRET"), "{err}");
        }
        let (_, spec) = split_dsn(&format!(
            "host=db sslmode=require sslcert='{0}' sslkey='{0}'",
            junk.display()
        ))
        .unwrap();
        let err = spec.connector().unwrap_err().to_string();
        assert!(!err.contains("SECRET"), "{err}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_mode_that_never_verifies_builds_without_any_file() {
        for mode in ["disable", "prefer", "require"] {
            let (_, spec) = split_dsn(&format!("host=db sslmode={mode}")).unwrap();
            spec.connector().expect("connector");
        }
    }
}
