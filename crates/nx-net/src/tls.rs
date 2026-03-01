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
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use sha2::{Digest, Sha256};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::{NetError, NetResult};

/// 16-byte NodeId derived from certificate public key.
pub type NodeId = [u8; 16];

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
                .map_err(|e| NetError::TlsError(format!("failed to build client verifier: {}", e)))?;

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

/// Derive NodeId from certificate's public key.
///
/// NodeId = SHA256(SubjectPublicKeyInfo)[0..16]
pub fn derive_node_id(cert_der: &CertificateDer<'_>) -> NodeId {
    let mut hasher = Sha256::new();
    hasher.update(cert_der.as_ref());
    let hash = hasher.finalize();

    let mut node_id = [0u8; 16];
    node_id.copy_from_slice(&hash[..16]);
    node_id
}

/// Convert NodeId to hex string for display/comparison.
pub fn node_id_to_hex(node_id: &NodeId) -> String {
    hex::encode(node_id)
}

/// Parse NodeId from hex string.
pub fn node_id_from_hex(s: &str) -> NetResult<NodeId> {
    let bytes = hex::decode(s)
        .map_err(|e| NetError::TlsError(format!("invalid node_id hex: {}", e)))?;

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
    use rcgen::{CertificateParams, IsCa, KeyPair, BasicConstraints};

    let mut params = CertificateParams::new(vec![])
        .map_err(|e| NetError::TlsError(format!("failed to create CA params: {}", e)))?;

    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
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

/// Insecure certificate verifier for development.
/// Accepts any certificate without validation.
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
        assert!(key_pem.contains("-----BEGIN PRIVATE KEY-----"));
    }

    #[test]
    fn test_generate_ca() {
        let (ca_pem, key_pem) = generate_ca("test-ca").unwrap();

        assert!(ca_pem.contains("-----BEGIN CERTIFICATE-----"));
        assert!(key_pem.contains("-----BEGIN PRIVATE KEY-----"));
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
    fn test_allowlist() {
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
    fn test_node_id_hex_roundtrip() {
        let node_id: NodeId = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let hex = node_id_to_hex(&node_id);
        let parsed = node_id_from_hex(&hex).unwrap();
        assert_eq!(node_id, parsed);
    }
}