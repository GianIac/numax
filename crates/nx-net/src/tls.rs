//! TLS/mTLS support for nx-net.
//!
//! Provides secure, mutually-authenticated transport between peers.
//!
//! ## Security features
//! - TLS 1.3 with forward secrecy
//! - Mutual TLS (mTLS): both peers authenticate
//! - NodeId derived from public key: `NodeId = SHA256(pubkey)[0..16]`
//! - Optional allowlist for permissioned networks
//!
//! ## Usage
//! - Production: provide cert/key/ca files via CLI
//! - Development: use `generate_self_signed()` for testing

use std::collections::HashSet;
use std::fs;
use std::io::BufReader;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::{NetError, NetResult};

pub type ClientTlsStream = tokio_rustls::client::TlsStream<TcpStream>;
pub type ServerTlsStream = tokio_rustls::server::TlsStream<TcpStream>;

/// 16-byte NodeId derived from certificate public key.
pub type NodeId = [u8; 16];

/// Network stream used by nx-net: plain TCP or TLS (client/server side).
pub enum NetStream {
    Plain(TcpStream),
    TlsClient(ClientTlsStream),
    TlsServer(ServerTlsStream),
}

impl NetStream {
    /// Returns the peer leaf certificate in DER form (owned), if this is a TLS stream.
    ///
    /// - Plain TCP streams return `None`.
    /// - For secure TLS/mTLS connections, a peer certificate is expected to be present.
    pub fn peer_cert_der(&self) -> Option<CertificateDer<'static>> {
        let cert = match self {
            NetStream::Plain(_) => return None,
            NetStream::TlsClient(tls) => tls.get_ref().1.peer_certificates()?.first()?.clone(),
            NetStream::TlsServer(tls) => tls.get_ref().1.peer_certificates()?.first()?.clone(),
        };

        Some(CertificateDer::from(cert.as_ref().to_vec()))
    }
}

impl AsyncRead for NetStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            NetStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            NetStream::TlsClient(s) => Pin::new(s).poll_read(cx, buf),
            NetStream::TlsServer(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for NetStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            NetStream::Plain(s) => Pin::new(s).poll_write(cx, data),
            NetStream::TlsClient(s) => Pin::new(s).poll_write(cx, data),
            NetStream::TlsServer(s) => Pin::new(s).poll_write(cx, data),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            NetStream::Plain(s) => Pin::new(s).poll_flush(cx),
            NetStream::TlsClient(s) => Pin::new(s).poll_flush(cx),
            NetStream::TlsServer(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            NetStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            NetStream::TlsClient(s) => Pin::new(s).poll_shutdown(cx),
            NetStream::TlsServer(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

impl TlsConfig {
    /// Server-side: wrap a TCP stream into TLS if TLS is enabled.
    pub async fn accept_stream(&self, tcp: TcpStream) -> NetResult<NetStream> {
        if !self.is_enabled() {
            return Ok(NetStream::Plain(tcp));
        }

        let acceptor = self.build_acceptor()?;
        let tls = acceptor
            .accept(tcp)
            .await
            .map_err(|e| NetError::TlsError(format!("tls accept failed: {}", e)))?;

        Ok(NetStream::TlsServer(tls))
    }

    /// Client-side: wrap a TCP stream into TLS if TLS is enabled.
    pub async fn connect_stream(
        &self,
        tcp: TcpStream,
        server_name: rustls::pki_types::ServerName<'static>,
    ) -> NetResult<NetStream> {
        if !self.is_enabled() && !self.insecure {
            return Ok(NetStream::Plain(tcp));
        }

        let connector = self.build_connector()?;
        let tls = connector
            .connect(server_name, tcp)
            .await
            .map_err(|e| NetError::TlsError(format!("tls connect failed: {}", e)))?;

        Ok(NetStream::TlsClient(tls))
    }
}

/// TLS configuration for a node.
#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    /// Path to this node's certificate (PEM)
    pub cert_path: Option<String>,
    /// Path to this node's private key (PEM)
    pub key_path: Option<String>,
    /// Path to CA certificate for verifying peers (PEM)
    pub ca_path: Option<String>,
    /// Allowed peer NodeIds (if Some, only these can connect)
    pub allowed_peers: Option<HashSet<String>>,
    /// Skip certificate verification (development only!)
    pub insecure: bool,
}

impl TlsConfig {
    /// Create TLS config with certificate, key, and CA paths.
    pub fn new(
        cert_path: impl Into<String>,
        key_path: impl Into<String>,
        ca_path: impl Into<String>,
    ) -> Self {
        Self {
            cert_path: Some(cert_path.into()),
            key_path: Some(key_path.into()),
            ca_path: Some(ca_path.into()),
            allowed_peers: None,
            insecure: false,
        }
    }

    /// Create config with allowlist of peer NodeIds.
    pub fn with_allowed_peers(mut self, peers: HashSet<String>) -> Self {
        self.allowed_peers = Some(peers);
        self
    }

    /// Create insecure config for development (skips cert verification).
    ///
    /// # Warning
    /// Never use in production!
    pub fn insecure_dev() -> Self {
        tracing::warn!("TLS insecure mode enabled - DO NOT USE IN PRODUCTION");
        Self {
            cert_path: None,
            key_path: None,
            ca_path: None,
            allowed_peers: None,
            insecure: true,
        }
    }

    /// Check if TLS is enabled (has cert and key).
    pub fn is_enabled(&self) -> bool {
        self.cert_path.is_some() && self.key_path.is_some()
    }

    /// Check if mTLS is enabled (has CA for peer verification).
    pub fn is_mtls_enabled(&self) -> bool {
        self.is_enabled() && self.ca_path.is_some()
    }

    /// Check if a NodeId is allowed to connect.
    pub fn is_peer_allowed(&self, node_id: &str) -> bool {
        match &self.allowed_peers {
            Some(allowed) => allowed.contains(node_id),
            None => true, // No allowlist = all peers allowed
        }
    }

    /// Load certificates from PEM file.
    pub fn load_certs(path: &Path) -> NetResult<Vec<CertificateDer<'static>>> {
        let file = fs::File::open(path)
            .map_err(|e| NetError::TlsError(format!("failed to open cert file: {}", e)))?;
        let mut reader = BufReader::new(file);

        let certs = rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| NetError::TlsError(format!("failed to parse certs: {}", e)))?;

        if certs.is_empty() {
            return Err(NetError::TlsError("no certificates found in file".into()));
        }

        Ok(certs)
    }

    /// Load private key from PEM file.
    pub fn load_key(path: &Path) -> NetResult<PrivateKeyDer<'static>> {
        let file = fs::File::open(path)
            .map_err(|e| NetError::TlsError(format!("failed to open key file: {}", e)))?;
        let mut reader = BufReader::new(file);

        rustls_pemfile::private_key(&mut reader)
            .map_err(|e| NetError::TlsError(format!("failed to parse key: {}", e)))?
            .ok_or_else(|| NetError::TlsError("no private key found in file".into()))
    }

    /// Load CA certificates into a RootCertStore.
    pub fn load_ca_store(path: &Path) -> NetResult<RootCertStore> {
        let certs = Self::load_certs(path)?;
        let mut store = RootCertStore::empty();

        for cert in certs {
            store
                .add(cert)
                .map_err(|e| NetError::TlsError(format!("failed to add CA cert: {}", e)))?;
        }

        Ok(store)
    }

    /// Build TLS acceptor for server-side connections (with mTLS).
    pub fn build_acceptor(&self) -> NetResult<TlsAcceptor> {
        let cert_path = self
            .cert_path
            .as_ref()
            .ok_or_else(|| NetError::TlsError("cert_path required for acceptor".into()))?;
        let key_path = self
            .key_path
            .as_ref()
            .ok_or_else(|| NetError::TlsError("key_path required for acceptor".into()))?;

        let certs = Self::load_certs(Path::new(cert_path))?;
        let key = Self::load_key(Path::new(key_path))?;

        let config = if let Some(ca_path) = &self.ca_path {
            // mTLS: require and verify client certificate
            let ca_store = Self::load_ca_store(Path::new(ca_path))?;
            let client_verifier = WebPkiClientVerifier::builder(Arc::new(ca_store))
                .build()
                .map_err(|e| {
                    NetError::TlsError(format!("failed to build client verifier: {}", e))
                })?;

            ServerConfig::builder()
                .with_client_cert_verifier(client_verifier)
                .with_single_cert(certs, key)
                .map_err(|e| NetError::TlsError(format!("failed to build server config: {}", e)))?
        } else {
            // TLS only: no client auth
            ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|e| NetError::TlsError(format!("failed to build server config: {}", e)))?
        };

        Ok(TlsAcceptor::from(Arc::new(config)))
    }

    /// Build TLS connector for client-side connections (with mTLS).
    pub fn build_connector(&self) -> NetResult<TlsConnector> {
        let config = if self.insecure {
            // Development only: skip certificate verification
            tracing::warn!("TLS certificate verification disabled - DO NOT USE IN PRODUCTION");

            ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(InsecureVerifier))
                .with_no_client_auth()
        } else {
            // Load CA for server verification
            let ca_store = if let Some(ca_path) = &self.ca_path {
                Self::load_ca_store(Path::new(ca_path))?
            } else {
                return Err(NetError::TlsError(
                    "ca_path required for secure connections".into(),
                ));
            };

            // mTLS: provide client certificate if available
            if let (Some(cert_path), Some(key_path)) = (&self.cert_path, &self.key_path) {
                let certs = Self::load_certs(Path::new(cert_path))?;
                let key = Self::load_key(Path::new(key_path))?;

                ClientConfig::builder()
                    .with_root_certificates(ca_store)
                    .with_client_auth_cert(certs, key)
                    .map_err(|e| {
                        NetError::TlsError(format!("failed to build client config: {}", e))
                    })?
            } else {
                ClientConfig::builder()
                    .with_root_certificates(ca_store)
                    .with_no_client_auth()
            }
        };

        Ok(TlsConnector::from(Arc::new(config)))
    }
}

// --- Identity (NodeId) helpers ---

/// Derive the canonical NodeId from the peer certificate public key.
///
/// Spec:
/// - Compute SHA-256 over the DER-encoded SubjectPublicKeyInfo (SPKI)
/// - NodeId = first 16 bytes of that hash
pub fn derive_node_id_from_cert(cert_der: &CertificateDer<'_>) -> NetResult<NodeId> {
    let hash = pubkey_spki_sha256(cert_der)?;
    let mut node_id = [0u8; 16];
    node_id.copy_from_slice(&hash[..16]);
    Ok(node_id)
}

/// Derive the protocol NodeId (nx_sync::NodeId) from the peer certificate public key.
///
/// Wire format choice:
/// - protocol NodeId is the lowercase hex string of the 16-byte identity
pub fn derive_protocol_node_id_from_cert(
    cert_der: &CertificateDer<'_>,
) -> NetResult<nx_sync::NodeId> {
    let node_id16 = derive_node_id_from_cert(cert_der)?;
    let hex = node_id_to_hex(&node_id16);
    Ok(nx_sync::NodeId::new(hex))
}

/// Compute a debug fingerprint for a certificate public key.
///
/// This is NOT used for protocol identity; it's meant for logs and diagnostics.
/// Returns a 64-char lowercase hex string (full SHA-256 of SPKI).
pub fn cert_fingerprint_hex(cert_der: &CertificateDer<'_>) -> NetResult<String> {
    let hash = pubkey_spki_sha256(cert_der)?;
    Ok(hex::encode(hash))
}

/// Backward-compatible helper kept for existing call sites/tests.
///
/// Prefer `derive_node_id_from_cert`.
pub fn derive_node_id(cert_der: &CertificateDer<'_>) -> NodeId {
    derive_node_id_from_cert(cert_der).expect("derive_node_id_from_cert")
}

/// Hash the certificate public key (SPKI) with SHA-256.
///
/// Internal helper used by both NodeId derivation and debug fingerprint.
fn pubkey_spki_sha256(cert_der: &CertificateDer<'_>) -> NetResult<[u8; 32]> {
    let (_, cert) = x509_parser::parse_x509_certificate(cert_der.as_ref())
        .map_err(|e| NetError::TlsError(format!("failed to parse x509 cert: {}", e)))?;

    let spki_der = cert.tbs_certificate.subject_pki.raw;

    let mut hasher = Sha256::new();
    hasher.update(spki_der);
    let out = hasher.finalize();

    let mut hash = [0u8; 32];
    hash.copy_from_slice(&out);
    Ok(hash)
}

/// Convert NodeId to hex string for display/comparison.
pub fn node_id_to_hex(node_id: &NodeId) -> String {
    hex::encode(node_id)
}

/// Parse NodeId from hex string.
pub fn node_id_from_hex(s: &str) -> NetResult<NodeId> {
    let bytes =
        hex::decode(s).map_err(|e| NetError::TlsError(format!("invalid node_id hex: {}", e)))?;

    if bytes.len() != 16 {
        return Err(NetError::TlsError(format!(
            "node_id must be 16 bytes, got {}",
            bytes.len()
        )));
    }

    let mut node_id = [0u8; 16];
    node_id.copy_from_slice(&bytes);
    Ok(node_id)
}

/// Generate self-signed certificate for development/testing.
///
/// Returns (cert_pem, key_pem) as strings.
pub fn generate_self_signed(common_name: &str) -> NetResult<(String, String)> {
    use rcgen::{CertificateParams, KeyPair};

    let mut params = CertificateParams::new(vec![common_name.to_string()])
        .map_err(|e| NetError::TlsError(format!("failed to create cert params: {}", e)))?;

    params.distinguished_name.push(
        rcgen::DnType::CommonName,
        rcgen::DnValue::Utf8String(common_name.to_string()),
    );

    let key_pair = KeyPair::generate()
        .map_err(|e| NetError::TlsError(format!("failed to generate key pair: {}", e)))?;

    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| NetError::TlsError(format!("failed to generate self-signed cert: {}", e)))?;

    Ok((cert.pem(), key_pair.serialize_pem()))
}

/// Generate CA certificate for signing other certs.
///
/// Returns (ca_cert_pem, ca_key_pem).
pub fn generate_ca(common_name: &str) -> NetResult<(String, String)> {
    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
        KeyUsagePurpose,
    };

    let mut params = CertificateParams::new(Vec::<String>::new())
        .map_err(|e| NetError::TlsError(format!("failed to create CA params: {}", e)))?;

    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);

    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];

    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];

    params.distinguished_name.push(
        rcgen::DnType::CommonName,
        rcgen::DnValue::Utf8String(common_name.to_string()),
    );

    let key_pair = KeyPair::generate()
        .map_err(|e| NetError::TlsError(format!("failed to generate CA key pair: {}", e)))?;

    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| NetError::TlsError(format!("failed to generate CA cert: {}", e)))?;

    Ok((cert.pem(), key_pair.serialize_pem()))
}

/// Generate a certificate signed by a CA.
///
/// # Arguments
/// * `_ca_cert_pem` - CA certificate in PEM format (kept for API consistency)
/// * `ca_key_pem` - CA private key PEM format
/// * `common_name` - CN for the new certificate
///
/// # Returns
/// (cert_pem, key_pem) for the new certificate
pub fn generate_signed(
    ca_cert_pem: &str,
    ca_key_pem: &str,
    common_name: &str,
) -> NetResult<(String, String)> {
    use rcgen::{CertificateParams, Issuer, KeyPair};

    // Parse CA key
    let ca_key = KeyPair::from_pem(ca_key_pem)
        .map_err(|e| NetError::TlsError(format!("failed to parse CA key: {}", e)))?;

    // Build Issuer from existing CA cert + CA key
    let issuer = Issuer::from_ca_cert_pem(ca_cert_pem, &ca_key)
        .map_err(|e| NetError::TlsError(format!("failed to parse CA cert: {}", e)))?;

    // Create certificate params for the node
    let mut params = CertificateParams::new(vec![common_name.to_string()])
        .map_err(|e| NetError::TlsError(format!("failed to create cert params: {}", e)))?;

    params.distinguished_name.push(
        rcgen::DnType::CommonName,
        rcgen::DnValue::Utf8String(common_name.to_string()),
    );

    // Generate key for new cert
    let key_pair = KeyPair::generate()
        .map_err(|e| NetError::TlsError(format!("failed to generate key pair: {}", e)))?;

    // Sign with CA (rcgen 0.14.x)
    let cert = params
        .signed_by(&key_pair, &issuer)
        .map_err(|e| NetError::TlsError(format!("failed to sign certificate: {}", e)))?;

    Ok((cert.pem(), key_pair.serialize_pem()))
}

/// Write certificate and key to files.
///
/// Useful for test setup and development.
pub fn write_cert_files(
    cert_pem: &str,
    key_pem: &str,
    cert_path: &Path,
    key_path: &Path,
) -> NetResult<()> {
    fs::write(cert_path, cert_pem)
        .map_err(|e| NetError::TlsError(format!("failed to write cert file: {}", e)))?;
    fs::write(key_path, key_pem)
        .map_err(|e| NetError::TlsError(format!("failed to write key file: {}", e)))?;
    Ok(())
}

/// Complete test PKI (CA + node certs) for integration tests.
///
/// Creates a temp directory with:
/// - ca.pem, ca-key.pem
/// - node1.pem, node1-key.pem
/// - node2.pem, node2-key.pem
///
/// Auto-cleanup on drop.
pub struct TestPki {
    /// Temp directory containing all cert files
    dir: tempfile::TempDir,
    /// CA certificate PEM
    pub ca_cert: String,
    /// CA private key PEM
    pub ca_key: String,
    /// Node 1 certificate PEM
    pub node1_cert: String,
    /// Node 1 private key PEM
    pub node1_key: String,
    /// Node 2 certificate PEM
    pub node2_cert: String,
    /// Node 2 private key PEM
    pub node2_key: String,
}

impl TestPki {
    // dir_path is needed for tests that want to load certs directly from files instead of using TlsConfig
    pub fn dir_path(&self) -> &Path {
        self.dir.path()
    }

    /// Generate a complete test PKI with CA and two node certs.
    pub fn generate() -> NetResult<Self> {
        // Generate CA
        let (ca_cert, ca_key) = generate_ca("test-ca")?;

        // Generate node certs signed by CA
        let (node1_cert, node1_key) = generate_signed(&ca_cert, &ca_key, "localhost")?;
        let (node2_cert, node2_key) = generate_signed(&ca_cert, &ca_key, "node-2")?;

        // Create temp directory
        // Create a unique temp directory (safe with parallel tests)
        let dir = tempfile::tempdir()
            .map_err(|e| NetError::TlsError(format!("failed to create temp dir: {}", e)))?;

        // Write files
        write_cert_files(
            &ca_cert,
            &ca_key,
            &dir.path().join("ca.pem"),
            &dir.path().join("ca-key.pem"),
        )?;
        write_cert_files(
            &node1_cert,
            &node1_key,
            &dir.path().join("node1.pem"),
            &dir.path().join("node1-key.pem"),
        )?;
        write_cert_files(
            &node2_cert,
            &node2_key,
            &dir.path().join("node2.pem"),
            &dir.path().join("node2-key.pem"),
        )?;

        Ok(Self {
            dir,
            ca_cert,
            ca_key,
            node1_cert,
            node1_key,
            node2_cert,
            node2_key,
        })
    }

    /// Get TlsConfig for node 1.
    pub fn node1_config(&self) -> TlsConfig {
        TlsConfig::new(
            self.dir.path().join("node1.pem").to_string_lossy(),
            self.dir.path().join("node1-key.pem").to_string_lossy(),
            self.dir.path().join("ca.pem").to_string_lossy(),
        )
    }

    /// Get TlsConfig for node 2.
    pub fn node2_config(&self) -> TlsConfig {
        TlsConfig::new(
            self.dir.path().join("node2.pem").to_string_lossy(),
            self.dir.path().join("node2-key.pem").to_string_lossy(),
            self.dir.path().join("ca.pem").to_string_lossy(),
        )
    }
}

/// Insecure certificate verifier for development.
/// Accepts any certificate without validation.
///
/// # Warning
/// NEVER use in production!
#[derive(Debug)]
struct InsecureVerifier;

impl rustls::client::danger::ServerCertVerifier for InsecureVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_self_signed() {
        let (cert_pem, key_pem) = generate_self_signed("test-node").unwrap();

        assert!(cert_pem.contains("-----BEGIN CERTIFICATE-----"));
        assert!(cert_pem.contains("-----END CERTIFICATE-----"));
        assert!(key_pem.contains("-----BEGIN PRIVATE KEY-----"));
        assert!(key_pem.contains("-----END PRIVATE KEY-----"));
    }

    #[test]
    fn test_generate_ca() {
        let (ca_pem, key_pem) = generate_ca("test-ca").unwrap();

        assert!(ca_pem.contains("-----BEGIN CERTIFICATE-----"));
        assert!(key_pem.contains("-----BEGIN PRIVATE KEY-----"));
    }

    #[test]
    fn test_generate_signed_by_ca() {
        let (ca_cert, ca_key) = generate_ca("my-ca").unwrap();
        let (node_cert, node_key) = generate_signed(&ca_cert, &ca_key, "my-node").unwrap();

        assert!(node_cert.contains("-----BEGIN CERTIFICATE-----"));
        assert!(node_key.contains("-----BEGIN PRIVATE KEY-----"));
    }

    #[test]
    fn test_different_nodes_different_certs() {
        let (ca_cert, ca_key) = generate_ca("ca").unwrap();

        let (cert1, _) = generate_signed(&ca_cert, &ca_key, "node-1").unwrap();
        let (cert2, _) = generate_signed(&ca_cert, &ca_key, "node-2").unwrap();

        assert_ne!(cert1, cert2);
    }

    #[test]
    fn test_node_id_deterministic() {
        let (cert_pem, _) = generate_self_signed("test").unwrap();

        let mut reader = std::io::BufReader::new(cert_pem.as_bytes());
        let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let node_id_1 = derive_node_id(&certs[0]);
        let node_id_2 = derive_node_id(&certs[0]);

        assert_eq!(node_id_1, node_id_2);
    }

    #[test]
    fn test_different_certs_different_node_ids() {
        let (cert1_pem, _) = generate_self_signed("node-1").unwrap();
        let (cert2_pem, _) = generate_self_signed("node-2").unwrap();

        let mut reader1 = std::io::BufReader::new(cert1_pem.as_bytes());
        let certs1: Vec<_> = rustls_pemfile::certs(&mut reader1)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let mut reader2 = std::io::BufReader::new(cert2_pem.as_bytes());
        let certs2: Vec<_> = rustls_pemfile::certs(&mut reader2)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        let node_id_1 = derive_node_id(&certs1[0]);
        let node_id_2 = derive_node_id(&certs2[0]);

        assert_ne!(node_id_1, node_id_2);
    }

    #[test]
    fn test_node_id_hex_roundtrip() {
        let node_id: NodeId = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let hex = node_id_to_hex(&node_id);
        let parsed = node_id_from_hex(&hex).unwrap();

        assert_eq!(node_id, parsed);
        assert_eq!(hex, "0102030405060708090a0b0c0d0e0f10");
    }

    #[test]
    fn test_node_id_from_hex_invalid() {
        assert!(node_id_from_hex("0102030405").is_err()); // too short
        assert!(node_id_from_hex("not-hex").is_err()); // invalid
        assert!(node_id_from_hex("0102030405060708090a0b0c0d0e0f101112").is_err()); // too long
    }

    #[test]
    fn test_tls_config_default() {
        let config = TlsConfig::default();

        assert!(!config.is_enabled());
        assert!(!config.is_mtls_enabled());
        assert!(!config.insecure);
    }

    #[test]
    fn test_tls_config_with_paths() {
        let config = TlsConfig::new("cert.pem", "key.pem", "ca.pem");

        assert!(config.is_enabled());
        assert!(config.is_mtls_enabled());
    }

    #[test]
    fn test_tls_config_insecure() {
        let config = TlsConfig::insecure_dev();

        assert!(!config.is_enabled());
        assert!(config.insecure);
    }

    #[test]
    fn test_allowlist_check() {
        let mut allowed = HashSet::new();
        allowed.insert("abc123".to_string());
        allowed.insert("def456".to_string());

        let config = TlsConfig::default().with_allowed_peers(allowed);

        assert!(config.is_peer_allowed("abc123"));
        assert!(config.is_peer_allowed("def456"));
        assert!(!config.is_peer_allowed("xyz789"));
    }

    #[test]
    fn test_no_allowlist_allows_all() {
        let config = TlsConfig::default();

        assert!(config.is_peer_allowed("any-peer"));
    }

    #[test]
    fn test_test_pki_generation() {
        let pki = TestPki::generate().unwrap();

        assert!(pki.dir_path().join("ca.pem").exists());
        assert!(pki.dir_path().join("ca-key.pem").exists());
        assert!(pki.dir_path().join("node1.pem").exists());
        assert!(pki.dir_path().join("node1-key.pem").exists());
        assert!(pki.dir_path().join("node2.pem").exists());
        assert!(pki.dir_path().join("node2-key.pem").exists());

        let config1 = pki.node1_config();
        let config2 = pki.node2_config();

        assert!(config1.is_enabled());
        assert!(config1.is_mtls_enabled());
        assert!(config2.is_enabled());
        assert!(config2.is_mtls_enabled());
    }

    #[test]
    fn test_load_certs_from_generated_pki() {
        let pki = TestPki::generate().unwrap();

        let certs = TlsConfig::load_certs(&pki.dir_path().join("node1.pem")).unwrap();
        assert_eq!(certs.len(), 1);

        let _key = TlsConfig::load_key(&pki.dir_path().join("node1-key.pem")).unwrap();

        let ca_store = TlsConfig::load_ca_store(&pki.dir_path().join("ca.pem")).unwrap();
        assert!(!ca_store.is_empty());
    }

    #[test]
    fn test_build_acceptor_with_pki() {
        let pki = TestPki::generate().unwrap();
        let config = pki.node1_config();

        let acceptor = config.build_acceptor();
        assert!(
            acceptor.is_ok(),
            "build_acceptor failed: {:?}",
            acceptor.err()
        );
    }

    #[test]
    fn test_build_connector_with_pki() {
        let pki = TestPki::generate().unwrap();
        let config = pki.node1_config();

        let connector = config.build_connector();
        assert!(
            connector.is_ok(),
            "build_connector failed: {:?}",
            connector.err()
        );
    }

    #[test]
    fn test_build_acceptor_without_cert_fails() {
        let config = TlsConfig::default();

        let result = config.build_acceptor();
        assert!(result.is_err());
    }

    #[test]
    fn test_build_connector_without_ca_fails() {
        let config = TlsConfig {
            cert_path: Some("cert.pem".into()),
            key_path: Some("key.pem".into()),
            ca_path: None,
            allowed_peers: None,
            insecure: false,
        };

        let result = config.build_connector();
        assert!(result.is_err());
    }
}
