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
use rustls::pki_types::pem::{self, PemObject};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, Error as TlsError, RootCertStore,
    SignatureScheme,
};
use std::fmt;
use std::fs::File;
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
    /// Require TLS, without checking the server's certificate unless
    /// `sslrootcert` is given, which makes it [`SslMode::VerifyCa`].
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
        // libpq's rule: `require` with a root certificate verifies the
        // chain, as `verify-ca` does.
        if spec.mode == SslMode::Require && spec.root_cert.is_some() {
            spec.mode = SslMode::VerifyCa;
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
        // `tokio_postgres` takes the handshake's server name from `host`
        // and refuses TLS without one, so a DSN naming only `hostaddr`
        // would fail against every server that offers TLS. The address
        // stands in for the name, as if it had been given as `host`.
        if config.get_hosts().is_empty() {
            for addr in config.get_hostaddrs().to_vec() {
                config.host(addr.to_string());
            }
        }
        config.ssl_mode(self.mode.for_tokio());
        Ok(())
    }

    /// Builds the rustls connector, reading the certificate and key files.
    pub(crate) fn connector(&self) -> Result<Tls, Error> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = self.verifier(&provider)?;
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
            checks_name: self.mode == SslMode::VerifyFull,
        })
    }

    /// The server-certificate check for the mode, reading `sslrootcert`.
    fn verifier(
        &self,
        provider: &Arc<rustls::crypto::CryptoProvider>,
    ) -> Result<Arc<dyn ServerCertVerifier>, Error> {
        Ok(match (self.mode, &self.root_cert) {
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
        })
    }
}

/// The connector `tokio_postgres` connects with. Cheap to clone.
///
/// `tokio_postgres` asks for a connector for every host, with an empty name
/// for a unix socket, and `MakeRustlsConnect` refuses a name that is not a
/// valid DNS name or IP address. A name nothing checks (a socket path, or any
/// host in a mode other than `verify-full`) is swapped for a placeholder
/// instead, so such a DSN connects as it would with libpq.
#[derive(Clone)]
pub(crate) struct Tls {
    inner: MakeRustlsConnect,
    checks_name: bool,
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
            Err(_) if !self.checks_name => {
                MakeTlsConnect::<S>::make_tls_connect(&mut self.inner, UNCHECKED_NAME)
            }
            other => other,
        }
    }
}

/// The server name given to a connector when no name will be checked.
const UNCHECKED_NAME: &str = "localhost";

fn open(path: &Path, option: &str) -> Result<File, Error> {
    File::open(path).map_err(|e| {
        Error::Config(format!(
            "{option} {} could not be read: {}",
            path.display(),
            e.kind()
        ))
    })
}

/// A PEM error, by kind only: the decoder's own messages can quote a line
/// of the file, which for `sslkey` is key material.
fn pem_err(option: &str, path: &Path, e: &pem::Error) -> Error {
    let what = match e {
        pem::Error::Io(e) => {
            return Error::Config(format!(
                "{option} {} could not be read: {}",
                path.display(),
                e.kind()
            ));
        }
        pem::Error::NoItemsFound => "holds no PEM item of the expected kind",
        _ => "is not valid PEM",
    };
    Error::Config(format!("{option} {} {what}", path.display()))
}

fn read_certs(path: &Path, option: &str) -> Result<Vec<CertificateDer<'static>>, Error> {
    let certs = CertificateDer::pem_reader_iter(open(path, option)?)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| pem_err(option, path, &e))?;
    if certs.is_empty() {
        return Err(Error::Config(format!(
            "{option} {} holds no PEM certificate",
            path.display()
        )));
    }
    Ok(certs)
}

fn read_key(path: &Path) -> Result<PrivateKeyDer<'static>, Error> {
    PrivateKeyDer::from_pem_reader(open(path, "sslkey")?).map_err(|e| match e {
        pem::Error::NoItemsFound => Error::Config(format!(
            "sslkey {} holds no PEM private key (an encrypted key is not supported)",
            path.display()
        )),
        e => pem_err("sslkey", path, &e),
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
/// `None` for a DSN this cannot scan; the caller reports the fixed parse
/// error for it.
pub(crate) fn split_dsn(dsn: &str) -> Result<Option<(String, TlsSpec)>, Error> {
    let scanned = if dsn.starts_with("postgres://") || dsn.starts_with("postgresql://") {
        split_url(dsn)
    } else {
        split_key_value(dsn)
    };
    let Some((rest, params)) = scanned else {
        return Ok(None);
    };
    Ok(Some((rest, TlsSpec::from_params(&params)?)))
}

fn is_tls_key(key: &str) -> bool {
    matches!(key, "sslmode" | "sslrootcert" | "sslcert" | "sslkey")
        || UNSUPPORTED_KEYS.contains(&key)
}

/// `key = value` pairs, mirroring `tokio_postgres`'s grammar: whitespace
/// around `=`, a value either single-quoted or ending at whitespace, and a
/// backslash escaping the next character in both. An empty key (a `=`
/// where a key should start) is unscannable: `tokio_postgres` stops parsing
/// there and ignores the rest, `sslmode` included.
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
        if key.is_empty() {
            return None;
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
        split_dsn(dsn).map(|scanned| scanned.expect("the DSN scans"))
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
    fn require_with_a_root_cert_is_verify_ca_as_in_libpq() {
        let (_, s) = spec("host=db sslmode=require sslrootcert=/ca.pem").unwrap();
        assert_eq!(s.mode, SslMode::VerifyCa);
        let (_, s) = spec("postgres://db/app?sslrootcert=%2Fca.pem&sslmode=require").unwrap();
        assert_eq!(s.mode, SslMode::VerifyCa);
        let (_, s) = spec("host=db sslmode=require").unwrap();
        assert_eq!(s.mode, SslMode::Require);
        // Only `require`: `prefer` with a root certificate still does not
        // verify, as in libpq.
        let (_, s) = spec("host=db sslrootcert=/ca.pem").unwrap();
        assert_eq!(s.mode, SslMode::Prefer);
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
    fn an_unscannable_dsn_is_the_fixed_parse_error() {
        // The last: `tokio_postgres` stops at the `=` where a key should
        // start and would connect with `sslmode=require` overridden by the
        // default, had the DSN reached it.
        for dsn in [
            "host=db user",
            "host='db",
            "postgres://db/app?sslmode",
            "host=db sslmode=require =x y",
        ] {
            assert!(split_dsn(dsn).unwrap().is_none(), "{dsn}");
            let err = crate::config::parse_dsn(dsn).unwrap_err().to_string();
            assert!(err.contains("does not parse"), "{dsn}: {err}");
        }
    }

    #[test]
    fn a_demanding_mode_on_a_unix_socket_host_is_a_config_error() {
        for mode in ["require", "verify-ca", "verify-full"] {
            let (rest, spec) = spec(&format!(
                "host=/var/run/postgresql sslmode={mode} sslrootcert=/ca.pem"
            ))
            .unwrap();
            let mut config: tokio_postgres::Config = rest.parse().unwrap();
            let err = spec.apply(&mut config).unwrap_err();
            assert!(matches!(err, Error::Config(_)));
            assert!(err.to_string().contains("unix-socket"), "{err}");
        }
        for mode in ["disable", "prefer"] {
            let (rest, spec) = spec(&format!("host=/var/run/postgresql sslmode={mode}")).unwrap();
            let mut config: tokio_postgres::Config = rest.parse().unwrap();
            spec.apply(&mut config).unwrap();
        }
        let (rest, spec) = spec("host=db sslmode=require").unwrap();
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
            let (_, spec) = spec(&format!(
                "host=db sslmode=verify-full {option}='{}'",
                path.display()
            ))
            .unwrap();
            let err = spec.connector().unwrap_err().to_string();
            assert!(err.contains(&path.display().to_string()), "{err}");
            assert!(!err.contains("SECRET"), "{err}");
        }
        let (_, spec) = spec(&format!(
            "host=db sslmode=require sslcert='{0}' sslkey='{0}'",
            junk.display()
        ))
        .unwrap();
        let err = spec.connector().unwrap_err().to_string();
        assert!(!err.contains("SECRET"), "{err}");
        // A PEM block whose body does not decode: the decoder's message
        // would quote the offending bytes.
        let broken = dir.join("broken.key");
        std::fs::write(
            &broken,
            "-----BEGIN PRIVATE KEY-----\nSECRET!*#\n-----END PRIVATE KEY-----\n",
        )
        .unwrap();
        let err = read_key(&broken).unwrap_err().to_string();
        assert!(err.contains("is not valid PEM"), "{err}");
        assert!(!err.contains("SECRET") && !err.contains('!'), "{err}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// The verifiers themselves, against throwaway certificates: `verify-ca`
    /// forgives only a certificate that names another host; an expired or
    /// not-yet-valid certificate, or one from another CA, fails in both
    /// verifying modes. The unverified modes check handshake signatures
    /// with the same algorithms as the verifying ones.
    #[test]
    fn the_verifiers_forgive_only_a_name_mismatch_and_only_in_verify_ca() {
        let dir = std::env::temp_dir().join(format!("trellis-tls-verify-{}", std::process::id()));
        testkit::cluster::tls::write_material(&dir);
        let server = read_certs(&dir.join("server.crt"), "test")
            .unwrap()
            .remove(0);
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = |mode: &str, root: &str| {
            spec(&format!(
                "host=db sslmode={mode} sslrootcert='{}'",
                dir.join(root).display()
            ))
            .unwrap()
            .1
            .verifier(&provider)
            .unwrap()
        };
        let named = ServerName::try_from("localhost").unwrap();
        let other = ServerName::try_from("other.example").unwrap();
        let now = UnixTime::now();
        let day = std::time::Duration::from_secs(86_400);
        let later =
            UnixTime::since_unix_epoch(std::time::Duration::from_secs(now.as_secs()) + 30 * day);
        let earlier =
            UnixTime::since_unix_epoch(std::time::Duration::from_secs(now.as_secs()) - 30 * day);
        let check = |v: &Arc<dyn ServerCertVerifier>, name: &ServerName<'_>, at: UnixTime| {
            v.verify_server_cert(&server, &[], name, &[], at)
                .map(|_| ())
        };
        let full = verifier("verify-full", "ca.crt");
        let ca = verifier("verify-ca", "ca.crt");
        check(&full, &named, now).expect("verify-full, right name");
        check(&ca, &named, now).expect("verify-ca, right name");
        check(&ca, &other, now).expect("verify-ca ignores the name");
        let err = check(&full, &other, now).unwrap_err();
        assert!(
            matches!(
                err,
                TlsError::InvalidCertificate(CertificateError::NotValidForNameContext { .. })
            ),
            "{err:?}"
        );
        for v in [&full, &ca] {
            for name in [&named, &other] {
                let err = check(v, name, later).unwrap_err();
                assert!(
                    matches!(
                        err,
                        TlsError::InvalidCertificate(CertificateError::ExpiredContext { .. })
                    ),
                    "{err:?}"
                );
                let err = check(v, name, earlier).unwrap_err();
                assert!(
                    matches!(
                        err,
                        TlsError::InvalidCertificate(CertificateError::NotValidYetContext { .. })
                    ),
                    "{err:?}"
                );
            }
        }
        for mode in ["verify-full", "verify-ca"] {
            let v = verifier(mode, "other-ca.crt");
            for name in [&named, &other] {
                let err = check(&v, name, now).unwrap_err();
                assert!(
                    matches!(
                        err,
                        TlsError::InvalidCertificate(CertificateError::UnknownIssuer)
                    ),
                    "{mode}: {err:?}"
                );
            }
        }
        let unverified = spec("host=db sslmode=require")
            .unwrap()
            .1
            .verifier(&provider)
            .unwrap();
        assert_eq!(
            unverified.supported_verify_schemes(),
            full.supported_verify_schemes()
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// The PEM shapes `openssl` writes: a CA bundle with CRLF line ends
    /// whose second certificate is the one that matters, and private keys
    /// in PKCS#8, PKCS#1 (RSA) and SEC1 (EC). An empty file and an encrypted
    /// key are refused by path.
    #[test]
    fn pem_files_in_the_usual_shapes_are_read() {
        let dir = std::env::temp_dir().join(format!("trellis-tls-pem-{}", std::process::id()));
        testkit::cluster::tls::write_material(&dir);
        let openssl = |args: &[&str]| {
            let status = std::process::Command::new("openssl")
                .args(args)
                .current_dir(&dir)
                .output()
                .expect("openssl")
                .status;
            assert!(status.success(), "openssl {args:?}");
        };
        let text = |name: &str| std::fs::read_to_string(dir.join(name)).unwrap();
        let bundle = format!("{}{}", text("other-ca.crt"), text("ca.crt")).replace('\n', "\r\n");
        std::fs::write(dir.join("bundle.crt"), bundle).unwrap();
        let roots = read_certs(&dir.join("bundle.crt"), "sslrootcert").unwrap();
        assert_eq!(roots.len(), 2);
        let server = read_certs(&dir.join("server.crt"), "test")
            .unwrap()
            .remove(0);
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let (_, full) = spec(&format!(
            "host=db sslmode=verify-full sslrootcert='{}'",
            dir.join("bundle.crt").display()
        ))
        .unwrap();
        full.verifier(&provider)
            .unwrap()
            .verify_server_cert(
                &server,
                &[],
                &ServerName::try_from("localhost").unwrap(),
                &[],
                UnixTime::now(),
            )
            .expect("the bundle's second CA signed the server certificate");

        openssl(&["genrsa", "-traditional", "-out", "rsa.key", "2048"]);
        openssl(&[
            "ecparam",
            "-name",
            "prime256v1",
            "-genkey",
            "-noout",
            "-out",
            "ec.key",
        ]);
        openssl(&[
            "genpkey",
            "-algorithm",
            "EC",
            "-pkeyopt",
            "ec_paramgen_curve:P-256",
            "-aes256",
            "-pass",
            "pass:x",
            "-out",
            "encrypted.key",
        ]);
        assert!(text("rsa.key").contains("BEGIN RSA PRIVATE KEY"));
        assert!(text("ec.key").contains("BEGIN EC PRIVATE KEY"));
        assert!(text("client.key").contains("BEGIN PRIVATE KEY"));
        assert!(matches!(
            read_key(&dir.join("rsa.key")).unwrap(),
            PrivateKeyDer::Pkcs1(_)
        ));
        assert!(matches!(
            read_key(&dir.join("ec.key")).unwrap(),
            PrivateKeyDer::Sec1(_)
        ));
        assert!(matches!(
            read_key(&dir.join("client.key")).unwrap(),
            PrivateKeyDer::Pkcs8(_)
        ));
        let err = read_key(&dir.join("encrypted.key"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("encrypted key is not supported"), "{err}");

        std::fs::write(dir.join("empty.pem"), "").unwrap();
        let err = read_certs(&dir.join("empty.pem"), "sslrootcert")
            .unwrap_err()
            .to_string();
        assert!(err.contains("empty.pem holds no PEM certificate"), "{err}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_mode_that_never_verifies_builds_without_any_file() {
        for mode in ["disable", "prefer", "require"] {
            let (_, spec) = spec(&format!("host=db sslmode={mode}")).unwrap();
            spec.connector().expect("connector");
        }
    }
}

/// The pre-pass against `tokio_postgres`'s own parser, on generated DSNs
/// that hold only options both understand (`sslmode` limited to `disable`,
/// `prefer` and `require`): wherever `tokio_postgres` accepts a DSN, the
/// pre-pass must lift out the same `sslmode` and leave a rest that parses to
/// the same everything else. Deterministic: a fixed-seed xorshift.
#[cfg(test)]
mod differential {
    use super::*;

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn pick<'a>(&mut self, xs: &[&'a str]) -> &'a str {
            xs[(self.next() % xs.len() as u64) as usize]
        }
    }

    /// Whether both parsers accepted `dsn` (and agreed: a disagreement
    /// panics). `may_refuse`: the pre-pass may refuse a DSN that
    /// `tokio_postgres` accepts, which it does for an empty key, where
    /// `tokio_postgres` silently stops parsing.
    fn agree(dsn: &str, may_refuse: bool) -> bool {
        let theirs = dsn.parse::<tokio_postgres::Config>();
        let ours = split_dsn(dsn).ok().flatten().and_then(|(rest, spec)| {
            let config = rest.parse::<tokio_postgres::Config>().ok()?;
            Some((config, spec))
        });
        match (theirs, ours) {
            (Ok(theirs), Some((mut ours, spec))) => {
                assert_eq!(spec.mode.for_tokio(), theirs.get_ssl_mode(), "{dsn:?}");
                ours.ssl_mode(theirs.get_ssl_mode());
                assert_eq!(format!("{theirs:?}"), format!("{ours:?}"), "{dsn:?}");
                assert_eq!(theirs.get_password(), ours.get_password(), "{dsn:?}");
                true
            }
            (Ok(_), None) => {
                assert!(
                    may_refuse,
                    "tokio_postgres accepts, the pre-pass refuses: {dsn:?}"
                );
                false
            }
            // The pre-pass is more lenient only about empty `&` segments in
            // a URL query, which it skips.
            (Err(_), _) => false,
        }
    }

    #[test]
    fn token_soup_key_value() {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        let tokens = [
            "sslmode",
            "host",
            "password",
            "user",
            "=",
            " ",
            "  ",
            "'",
            "\\",
            "require",
            "disable",
            "prefer",
            "x",
            "a b",
            "\t",
            "sslmode=require",
            "\\'",
        ];
        let agreed = (0..100_000)
            .filter(|_| {
                let n = 1 + rng.next() % 9;
                let dsn: String = (0..n).map(|_| rng.pick(&tokens)).collect();
                agree(&dsn, true)
            })
            .count();
        assert!(agreed > 1_000, "{agreed}");
    }

    #[test]
    fn token_soup_url() {
        let mut rng = Rng(0x2545_f491_4f6c_dd1d);
        let tokens = [
            "u",
            ":",
            "p",
            "@",
            "db",
            "/",
            "app",
            "?",
            "&",
            "=",
            "sslmode",
            "require",
            "disable",
            "%20",
            "%3D",
            "%26",
            "%zz",
            "%",
            "application_name",
            "ssl%6dode",
            "[::1]",
            ",",
            "5433",
            "%40",
            "%3F",
            "host=%2Ftmp",
        ];
        let agreed = (0..100_000)
            .filter(|_| {
                let n = 1 + rng.next() % 12;
                let dsn: String = std::iter::once("postgres://")
                    .chain((0..n).map(|_| rng.pick(&tokens)))
                    .collect();
                agree(&dsn, false)
            })
            .count();
        assert!(agreed > 10_000, "{agreed}");
    }

    /// Well-formed `key=value` DSNs whose values hold quotes, backslashes,
    /// whitespace and option-like text, each quoted or escaped as the
    /// grammar allows.
    #[test]
    fn well_formed_key_value() {
        let mut rng = Rng(0xdead_beef_cafe_f00d);
        let keys = [
            "host",
            "password",
            "user",
            "dbname",
            "application_name",
            "sslmode",
        ];
        let pieces = [
            "a",
            " ",
            "'",
            "\\",
            "=",
            "sslmode=disable",
            "%",
            "@",
            "?",
            "\t",
        ];
        for _ in 0..100_000 {
            let mut dsn = String::new();
            for _ in 0..(1 + rng.next() % 5) {
                let key = rng.pick(&keys);
                dsn.push_str(rng.pick(&["", " ", "  "]));
                dsn.push_str(key);
                dsn.push_str(rng.pick(&["", " "]));
                dsn.push('=');
                dsn.push_str(rng.pick(&["", " "]));
                let raw: String = if key == "sslmode" {
                    rng.pick(&["disable", "prefer", "require"]).to_string()
                } else {
                    (0..rng.next() % 6).map(|_| rng.pick(&pieces)).collect()
                };
                let quoted = raw.is_empty() || rng.next().is_multiple_of(2);
                if quoted {
                    dsn.push('\'');
                }
                for c in raw.chars() {
                    if c == '\\' || c == '\'' || (!quoted && c.is_whitespace()) {
                        dsn.push('\\');
                    }
                    dsn.push(c);
                }
                if quoted {
                    dsn.push('\'');
                }
                dsn.push(' ');
            }
            assert!(agree(&dsn, false), "{dsn:?}");
        }
    }
}
