use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nx_sync::NodeId;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::message::{Message, MessageKind, PROTOCOL_VERSION};
use crate::node::{
    connect_transport, read_message, supported_formats_for, verify_peer_identity, write_message,
};
use crate::{NetError, NetResult, SerializationFormat, TlsConfig};

/// Default maximum number of endpoint suggestions retained by a bootstrap seed.
pub const DEFAULT_BOOTSTRAP_CACHE_CAPACITY: usize = 1_024;
/// Default maximum number of endpoint suggestions returned by one bootstrap query.
pub const DEFAULT_BOOTSTRAP_RESPONSE_CAPACITY: usize = 128;
/// Hard upper bound for candidates requested or returned in one bootstrap query.
pub const MAX_BOOTSTRAP_RESPONSE_CAPACITY: usize = 4_096;
/// Default lifetime of an endpoint suggestion learned by a bootstrap seed.
pub const DEFAULT_BOOTSTRAP_CANDIDATE_TTL: Duration = Duration::from_secs(60);
/// Hard upper bound for a bootstrap candidate lease.
pub const MAX_BOOTSTRAP_CANDIDATE_TTL: Duration = Duration::from_secs(300);
/// Default maximum number of concurrent one-shot bootstrap queries per client.
pub const DEFAULT_MAX_CONCURRENT_BOOTSTRAP_QUERIES: usize = 1;

pub(crate) const MAX_CLUSTER_ID_LEN: usize = 255;
pub(crate) const MAX_ENDPOINT_LEN: usize = 512;

/// Server-side policy for the authenticated bootstrap exchange.
#[derive(Debug, Clone)]
pub struct BootstrapServerConfig {
    cluster_id: String,
    advertised_endpoint: Option<String>,
    max_cached_candidates: usize,
    max_response_candidates: usize,
    candidate_ttl: Duration,
}

impl BootstrapServerConfig {
    pub fn new(cluster_id: impl Into<String>) -> NetResult<Self> {
        let config = Self {
            cluster_id: cluster_id.into(),
            advertised_endpoint: None,
            max_cached_candidates: DEFAULT_BOOTSTRAP_CACHE_CAPACITY,
            max_response_candidates: DEFAULT_BOOTSTRAP_RESPONSE_CAPACITY,
            candidate_ttl: DEFAULT_BOOTSTRAP_CANDIDATE_TTL,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn with_advertised_endpoint(mut self, endpoint: impl Into<String>) -> NetResult<Self> {
        let endpoint = endpoint.into();
        self.advertised_endpoint = Some(canonicalize_advertised_endpoint(&endpoint)?);
        Ok(self)
    }

    pub fn with_max_cached_candidates(mut self, limit: usize) -> NetResult<Self> {
        if limit == 0 {
            return Err(NetError::InvalidMessage(
                "bootstrap cache capacity must be greater than zero".into(),
            ));
        }
        self.max_cached_candidates = limit;
        Ok(self)
    }

    pub fn with_max_response_candidates(mut self, limit: usize) -> NetResult<Self> {
        validate_response_capacity(limit)?;
        self.max_response_candidates = limit;
        Ok(self)
    }

    pub fn with_candidate_ttl(mut self, ttl: Duration) -> NetResult<Self> {
        validate_candidate_ttl(ttl)?;
        self.candidate_ttl = ttl;
        Ok(self)
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub fn advertised_endpoint(&self) -> Option<&str> {
        self.advertised_endpoint.as_deref()
    }

    pub fn max_cached_candidates(&self) -> usize {
        self.max_cached_candidates
    }

    pub fn max_response_candidates(&self) -> usize {
        self.max_response_candidates
    }

    pub fn candidate_ttl(&self) -> Duration {
        self.candidate_ttl
    }

    pub(crate) fn validate(&self) -> NetResult<()> {
        validate_cluster_id(&self.cluster_id)?;
        if let Some(endpoint) = &self.advertised_endpoint {
            validate_advertised_endpoint(endpoint)?;
        }
        if self.max_cached_candidates == 0 {
            return Err(NetError::InvalidMessage(
                "bootstrap cache capacity must be greater than zero".into(),
            ));
        }
        validate_response_capacity(self.max_response_candidates)?;
        validate_candidate_ttl(self.candidate_ttl)
    }
}

/// Client-side identity, transport, and bounds for one-shot bootstrap queries.
#[derive(Debug, Clone)]
pub struct BootstrapClientConfig {
    pub node_id: NodeId,
    pub tls: Option<TlsConfig>,
    pub max_message_size: usize,
    pub socket_timeout: Duration,
    pub serialization_format: SerializationFormat,
    pub max_response_candidates: usize,
    pub max_candidate_ttl: Duration,
    pub max_concurrent_queries: usize,
}

impl BootstrapClientConfig {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            tls: None,
            max_message_size: crate::DEFAULT_MAX_MESSAGE_SIZE,
            socket_timeout: crate::DEFAULT_SOCKET_TIMEOUT,
            serialization_format: SerializationFormat::Bincode,
            max_response_candidates: DEFAULT_BOOTSTRAP_RESPONSE_CAPACITY,
            max_candidate_ttl: MAX_BOOTSTRAP_CANDIDATE_TTL,
            max_concurrent_queries: DEFAULT_MAX_CONCURRENT_BOOTSTRAP_QUERIES,
        }
    }

    pub(crate) fn validate(&self) -> NetResult<()> {
        if self.max_message_size == 0 {
            return Err(NetError::InvalidMessage(
                "bootstrap maximum message size must be greater than zero".into(),
            ));
        }
        if self.socket_timeout.is_zero() {
            return Err(NetError::InvalidMessage(
                "bootstrap socket timeout must be greater than zero".into(),
            ));
        }
        validate_response_capacity(self.max_response_candidates)?;
        validate_candidate_ttl(self.max_candidate_ttl)?;
        if self.max_concurrent_queries == 0 {
            return Err(NetError::InvalidMessage(
                "bootstrap concurrent query limit must be greater than zero".into(),
            ));
        }
        Ok(())
    }
}

/// A bounded request for candidate endpoints in one cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapRequest {
    pub cluster_id: String,
    pub advertised_endpoint: Option<String>,
    pub max_results: usize,
}

impl BootstrapRequest {
    pub fn new(cluster_id: impl Into<String>, max_results: usize) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            advertised_endpoint: None,
            max_results,
        }
    }

    pub fn with_advertised_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.advertised_endpoint = Some(endpoint.into());
        self
    }
}

/// Result of an authenticated one-shot bootstrap exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapResponse {
    /// Identity authenticated by the transport policy (certificate-bound with secure TLS).
    pub seed_node_id: NodeId,
    /// Advertised endpoints. These remain untrusted connection candidates.
    pub endpoints: Vec<String>,
    /// Maximum time for which the returned snapshot may be retained without refresh.
    pub candidate_ttl: Duration,
}

/// Clonable client for bounded one-shot bootstrap exchanges.
#[derive(Debug, Clone)]
pub struct BootstrapClient {
    pub(crate) config: Arc<BootstrapClientConfig>,
    query_slots: Arc<Semaphore>,
}

impl BootstrapClient {
    pub fn new(config: BootstrapClientConfig) -> NetResult<Self> {
        config.validate()?;
        let max_concurrent_queries = config.max_concurrent_queries;
        Ok(Self {
            config: Arc::new(config),
            query_slots: Arc::new(Semaphore::new(max_concurrent_queries)),
        })
    }

    pub(crate) fn acquire_query_slot(&self) -> NetResult<OwnedSemaphorePermit> {
        Arc::clone(&self.query_slots)
            .try_acquire_owned()
            .map_err(|_| {
                NetError::ConnectionAttemptLimitReached(self.config.max_concurrent_queries)
            })
    }

    /// Contact one seed and return its authenticated, bounded endpoint suggestions.
    ///
    /// Only the seed identity is authenticated here. Every returned endpoint
    /// remains a candidate and must pass the normal peer handshake independently.
    pub async fn query(
        &self,
        seed: &str,
        request: BootstrapRequest,
    ) -> NetResult<BootstrapResponse> {
        validate_cluster_id(&request.cluster_id)?;
        if request.max_results == 0
            || request.max_results > self.config.max_response_candidates
            || u32::try_from(request.max_results).is_err()
        {
            return Err(NetError::InvalidMessage(format!(
                "bootstrap max_results must be in 1..={}",
                self.config.max_response_candidates
            )));
        }
        if let Some(endpoint) = &request.advertised_endpoint {
            validate_advertised_endpoint(endpoint)?;
        }

        let _query_slot = self.acquire_query_slot()?;
        let (stream, _transport_addr) =
            connect_transport(seed, self.config.tls.as_ref(), self.config.socket_timeout).await?;
        let peer_cert = stream.peer_cert_der();
        // A bootstrap request may disclose our advertised endpoint. Apply the
        // certificate allowlist before sending it, then bind the server's
        // claimed NodeId to the same certificate after the response.
        if self
            .config
            .tls
            .as_ref()
            .is_some_and(|configuration| !configuration.insecure)
        {
            let certificate = peer_cert.as_ref().ok_or_else(|| {
                NetError::TlsError("missing peer certificate in TLS session".into())
            })?;
            let expected_seed_id = crate::tls::derive_protocol_node_id_from_cert(certificate)?;
            verify_peer_identity(
                &self.config.node_id,
                &expected_seed_id,
                Some(certificate),
                self.config.tls.as_ref(),
            )?;
        }
        let (mut reader, mut writer) = tokio::io::split(stream);
        let supported_formats = supported_formats_for(self.config.serialization_format);
        let hello = Message::bootstrap_hello(
            self.config.node_id.clone(),
            supported_formats.clone(),
            self.config.serialization_format,
            request.cluster_id.clone(),
            request.advertised_endpoint,
            request.max_results as u32,
        );
        write_message(
            &mut writer,
            &hello,
            self.config.serialization_format,
            self.config.socket_timeout,
        )
        .await?;

        let response = read_message(
            &mut reader,
            self.config.max_message_size,
            self.config.socket_timeout,
        )
        .await?;
        let (seed_node_id, selected_format, candidates, candidate_ttl_ms) = match response.kind {
            MessageKind::BootstrapAck {
                node_id,
                protocol_version,
                selected_format,
                cluster_id,
                candidates,
                candidate_ttl_ms,
            } => {
                if protocol_version != PROTOCOL_VERSION {
                    return Err(NetError::Wire(crate::WireError::protocol_mismatch(
                        protocol_version,
                    )));
                }
                if cluster_id != request.cluster_id {
                    return Err(NetError::InvalidMessage(
                        "bootstrap response cluster ID does not match the request".into(),
                    ));
                }
                (node_id, selected_format, candidates, candidate_ttl_ms)
            }
            MessageKind::Error { error } => return Err(NetError::Wire(error)),
            _ => {
                return Err(NetError::InvalidMessage(
                    "expected BootstrapAck from bootstrap seed".into(),
                ));
            }
        };

        if !supported_formats.contains(&selected_format) {
            return Err(NetError::InvalidMessage(format!(
                "bootstrap seed selected unsupported serialization format: {selected_format:?}"
            )));
        }
        verify_peer_identity(
            &self.config.node_id,
            &seed_node_id,
            peer_cert.as_ref(),
            self.config.tls.as_ref(),
        )?;
        if candidates.len() > request.max_results
            || candidates.len() > self.config.max_response_candidates
        {
            return Err(NetError::InvalidMessage(
                "bootstrap response exceeds the negotiated candidate limit".into(),
            ));
        }
        let candidate_ttl = Duration::from_millis(candidate_ttl_ms);
        if candidate_ttl.is_zero() || candidate_ttl > self.config.max_candidate_ttl {
            return Err(NetError::InvalidMessage(
                "bootstrap response contains an invalid candidate TTL".into(),
            ));
        }
        let mut seen = HashSet::new();
        let mut normalized_candidates = Vec::with_capacity(candidates.len());
        for endpoint in candidates {
            let endpoint = canonicalize_advertised_endpoint(&endpoint)?;
            if !seen.insert(endpoint.clone()) {
                return Err(NetError::InvalidMessage(
                    "bootstrap response contains duplicate endpoints".into(),
                ));
            }
            normalized_candidates.push(endpoint);
        }

        Ok(BootstrapResponse {
            seed_node_id,
            endpoints: normalized_candidates,
            candidate_ttl,
        })
    }
}

#[derive(Debug, Clone)]
struct CachedCandidate {
    endpoint: String,
    expires_at: Instant,
}

#[derive(Debug)]
struct BootstrapCache {
    by_node: HashMap<NodeId, CachedCandidate>,
    order: Vec<NodeId>,
}

#[derive(Debug)]
pub(crate) struct BootstrapServer {
    config: BootstrapServerConfig,
    advertised_endpoint: Mutex<Option<String>>,
    cache: Mutex<BootstrapCache>,
}

impl BootstrapServer {
    pub(crate) fn new(config: BootstrapServerConfig) -> Self {
        Self {
            advertised_endpoint: Mutex::new(config.advertised_endpoint.clone()),
            config,
            cache: Mutex::new(BootstrapCache {
                by_node: HashMap::new(),
                order: Vec::new(),
            }),
        }
    }

    pub(crate) fn cluster_id(&self) -> &str {
        self.config.cluster_id()
    }

    pub(crate) fn candidate_ttl(&self) -> Duration {
        self.config.candidate_ttl()
    }

    pub(crate) fn announce(&self, endpoint: String) -> NetResult<()> {
        let endpoint = canonicalize_advertised_endpoint(&endpoint)?;
        *self.advertised_endpoint.lock().map_err(cache_poisoned)? = Some(endpoint);
        Ok(())
    }

    pub(crate) fn withdraw(&self) -> NetResult<()> {
        *self.advertised_endpoint.lock().map_err(cache_poisoned)? = None;
        Ok(())
    }

    pub(crate) fn clear(&self) {
        if let Ok(mut advertised) = self.advertised_endpoint.lock() {
            *advertised = None;
        }
        if let Ok(mut cache) = self.cache.lock() {
            cache.by_node.clear();
            cache.order.clear();
        }
    }

    pub(crate) fn exchange(
        &self,
        requester: &NodeId,
        requester_endpoint: Option<String>,
        requested_results: usize,
    ) -> NetResult<Vec<String>> {
        let now = Instant::now();
        let mut cache = self.cache.lock().map_err(cache_poisoned)?;
        prune_expired(&mut cache, now);

        match requester_endpoint {
            Some(endpoint) => {
                let endpoint = canonicalize_advertised_endpoint(&endpoint)?;
                if let Some(entry) = cache.by_node.get_mut(requester) {
                    entry.endpoint = endpoint;
                    entry.expires_at = now + self.config.candidate_ttl;
                } else if cache.by_node.len() < self.config.max_cached_candidates {
                    cache.order.push(requester.clone());
                    cache.by_node.insert(
                        requester.clone(),
                        CachedCandidate {
                            endpoint,
                            expires_at: now + self.config.candidate_ttl,
                        },
                    );
                }
            }
            None => {
                cache.by_node.remove(requester);
                cache.order.retain(|node_id| node_id != requester);
            }
        }

        let limit = requested_results
            .min(self.config.max_response_candidates)
            .min(MAX_BOOTSTRAP_RESPONSE_CAPACITY);
        let advertised = self
            .advertised_endpoint
            .lock()
            .map_err(cache_poisoned)?
            .clone();
        // Reserve for locally available entries, never for a remote request's capacity.
        let available = cache.by_node.len() - usize::from(cache.by_node.contains_key(requester))
            + usize::from(advertised.is_some());
        let mut endpoints = Vec::with_capacity(limit.min(available));
        let mut seen = HashSet::new();
        if limit > 0
            && let Some(endpoint) = advertised
            && seen.insert(endpoint.clone())
        {
            endpoints.push(endpoint);
        }
        for node_id in &cache.order {
            if endpoints.len() >= limit {
                break;
            }
            if node_id == requester {
                continue;
            }
            let Some(candidate) = cache.by_node.get(node_id) else {
                continue;
            };
            if seen.insert(candidate.endpoint.clone()) {
                endpoints.push(candidate.endpoint.clone());
            }
        }
        endpoints.truncate(limit);
        Ok(endpoints)
    }
}

fn prune_expired(cache: &mut BootstrapCache, now: Instant) {
    cache
        .by_node
        .retain(|_, candidate| candidate.expires_at > now);
    cache
        .order
        .retain(|node_id| cache.by_node.contains_key(node_id));
}

fn cache_poisoned<T>(_: std::sync::PoisonError<T>) -> NetError {
    NetError::ConnectionFailed("bootstrap cache is poisoned".into())
}

pub(crate) fn validate_cluster_id(cluster_id: &str) -> NetResult<()> {
    if cluster_id.is_empty() || cluster_id.len() > MAX_CLUSTER_ID_LEN {
        return Err(NetError::InvalidMessage(format!(
            "bootstrap cluster ID length must be in 1..={MAX_CLUSTER_ID_LEN}"
        )));
    }
    if cluster_id.chars().any(char::is_control) {
        return Err(NetError::InvalidMessage(
            "bootstrap cluster ID must not contain control characters".into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_advertised_endpoint(endpoint: &str) -> NetResult<()> {
    canonicalize_advertised_endpoint(endpoint).map(|_| ())
}

fn canonicalize_advertised_endpoint(endpoint: &str) -> NetResult<String> {
    if endpoint.is_empty() || endpoint.len() > MAX_ENDPOINT_LEN || endpoint.trim() != endpoint {
        return Err(NetError::InvalidMessage(format!(
            "advertised endpoint must be non-empty, trimmed, and at most {MAX_ENDPOINT_LEN} bytes"
        )));
    }

    let (host, port) = split_endpoint(endpoint)?;
    if port == 0 {
        return Err(NetError::InvalidMessage(
            "advertised endpoint must not use port zero".into(),
        ));
    }
    let canonical_host = if let Ok(ip) = host.parse::<IpAddr>() {
        let undialable = ip.is_unspecified()
            || ip.is_multicast()
            || matches!(ip, IpAddr::V4(address) if address.is_broadcast())
            || matches!(ip, IpAddr::V6(address) if address.is_unicast_link_local());
        if undialable {
            return Err(NetError::InvalidMessage(
                "advertised endpoint must use a dialable unicast IP address".into(),
            ));
        }
        ip.to_string()
    } else if !valid_dns_name(host) {
        return Err(NetError::InvalidMessage(
            "advertised endpoint host must be a valid DNS name or IP address".into(),
        ));
    } else {
        host.strip_suffix('.').unwrap_or(host).to_ascii_lowercase()
    };
    if canonical_host.contains(':') {
        Ok(format!("[{canonical_host}]:{port}"))
    } else {
        Ok(format!("{canonical_host}:{port}"))
    }
}

fn split_endpoint(endpoint: &str) -> NetResult<(&str, u16)> {
    let (host, port) = if endpoint.starts_with('[') {
        let closing = endpoint.find(']').ok_or_else(|| {
            NetError::InvalidMessage("invalid bracketed advertised endpoint".into())
        })?;
        if endpoint.as_bytes().get(closing + 1) != Some(&b':') {
            return Err(NetError::InvalidMessage(
                "advertised endpoint must include a port".into(),
            ));
        }
        (&endpoint[1..closing], &endpoint[closing + 2..])
    } else {
        let (host, port) = endpoint.rsplit_once(':').ok_or_else(|| {
            NetError::InvalidMessage("advertised endpoint must be host:port".into())
        })?;
        if host.contains(':') {
            return Err(NetError::InvalidMessage(
                "IPv6 advertised endpoints must use brackets".into(),
            ));
        }
        (host, port)
    };
    let port = port.parse::<u16>().map_err(|_| {
        NetError::InvalidMessage("advertised endpoint port must be a valid u16".into())
    })?;
    Ok((host, port))
}

fn valid_dns_name(host: &str) -> bool {
    let host = host.strip_suffix('.').unwrap_or(host);
    !host.is_empty()
        && host.len() <= 253
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                && label
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_alphanumeric)
                && label
                    .as_bytes()
                    .last()
                    .is_some_and(u8::is_ascii_alphanumeric)
        })
}

fn validate_response_capacity(limit: usize) -> NetResult<()> {
    if !(1..=MAX_BOOTSTRAP_RESPONSE_CAPACITY).contains(&limit) {
        return Err(NetError::InvalidMessage(format!(
            "bootstrap response capacity must be in 1..={MAX_BOOTSTRAP_RESPONSE_CAPACITY}"
        )));
    }
    Ok(())
}

fn validate_candidate_ttl(ttl: Duration) -> NetResult<()> {
    if ttl.is_zero() || ttl > MAX_BOOTSTRAP_CANDIDATE_TTL {
        return Err(NetError::InvalidMessage(format!(
            "bootstrap candidate TTL must be in 1ms..={}s",
            MAX_BOOTSTRAP_CANDIDATE_TTL.as_secs()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Node, NodeConfig, TestPki};

    fn certificate_node_id(path: &std::path::Path) -> NodeId {
        let certificate = crate::TlsConfig::load_certs(path).unwrap().remove(0);
        crate::tls::derive_protocol_node_id_from_cert(&certificate).unwrap()
    }

    #[test]
    fn response_capacity_is_validated_by_both_configurations_and_client_constructor() {
        for limit in [
            0,
            MAX_BOOTSTRAP_RESPONSE_CAPACITY + 1,
            u32::MAX as usize,
            usize::MAX,
        ] {
            assert!(
                BootstrapServerConfig::new("cluster-a")
                    .unwrap()
                    .with_max_response_candidates(limit)
                    .is_err()
            );
            let mut server_config = BootstrapServerConfig::new("cluster-a").unwrap();
            server_config.max_response_candidates = limit;
            assert!(server_config.validate().is_err());
            let mut client_config = BootstrapClientConfig::new(NodeId::new("client"));
            client_config.max_response_candidates = limit;
            assert!(client_config.validate().is_err());
            assert!(BootstrapClient::new(client_config).is_err());
        }
        for limit in [1, MAX_BOOTSTRAP_RESPONSE_CAPACITY] {
            BootstrapServerConfig::new("cluster-a")
                .unwrap()
                .with_max_response_candidates(limit)
                .unwrap()
                .validate()
                .unwrap();
            let mut config = BootstrapClientConfig::new(NodeId::new("client"));
            config.max_response_candidates = limit;
            BootstrapClient::new(config).unwrap();
        }
    }

    #[test]
    fn huge_request_reserves_only_available_candidates() {
        let server = BootstrapServer::new(
            BootstrapServerConfig::new("cluster-a")
                .unwrap()
                .with_max_response_candidates(MAX_BOOTSTRAP_RESPONSE_CAPACITY)
                .unwrap(),
        );
        let requester = NodeId::new("client");
        let empty = server
            .exchange(&requester, None, u32::MAX as usize)
            .unwrap();
        assert_eq!(empty.capacity(), 0);
        server.announce("seed.example:9000".into()).unwrap();
        server
            .exchange(&NodeId::new("peer"), Some("peer.example:9000".into()), 1)
            .unwrap();
        let response = server
            .exchange(
                &requester,
                Some("client.example:9000".into()),
                u32::MAX as usize,
            )
            .unwrap();
        assert_eq!(response, ["seed.example:9000", "peer.example:9000"]);
        assert_eq!(response.capacity(), 2);
        let zero = server.exchange(&requester, None, 0).unwrap();
        assert_eq!(zero.capacity(), 0);
        assert!(zero.is_empty());
    }

    #[tokio::test]
    async fn remote_u32_max_request_returns_a_bounded_v5_response() {
        for format in [SerializationFormat::Bincode, SerializationFormat::Json] {
            let node = Node::new(
                NodeConfig::new(NodeId::new("seed"), "127.0.0.1:0").with_bootstrap_server(
                    BootstrapServerConfig::new("cluster-a")
                        .unwrap()
                        .with_max_response_candidates(MAX_BOOTSTRAP_RESPONSE_CAPACITY)
                        .unwrap(),
                ),
            );
            let bound = node.start_listener().await.unwrap();
            node.announce_bootstrap_endpoint(bound.to_string()).unwrap();
            let mut stream = tokio::net::TcpStream::connect(bound).await.unwrap();
            let request = Message::bootstrap_hello(
                NodeId::new("client"),
                vec![format],
                format,
                "cluster-a".into(),
                None,
                u32::MAX,
            );
            write_message(&mut stream, &request, format, Duration::from_secs(1))
                .await
                .unwrap();
            let response = read_message(&mut stream, 1024, Duration::from_secs(1))
                .await
                .unwrap();
            match response.kind {
                MessageKind::BootstrapAck {
                    protocol_version,
                    candidates,
                    ..
                } => {
                    assert_eq!(protocol_version, 5);
                    assert_eq!(candidates, [bound.to_string()]);
                }
                other => panic!("unexpected bootstrap reply: {other:?}"),
            }
            assert_eq!(node.connected_peer_count().await, 0);
            node.shutdown().await;
        }
    }

    #[test]
    fn endpoint_validation_rejects_wildcard_and_dynamic_port() {
        assert!(validate_advertised_endpoint("0.0.0.0:9000").is_err());
        assert!(validate_advertised_endpoint("[::]:9000").is_err());
        assert!(validate_advertised_endpoint("[::0]:9000").is_err());
        assert!(validate_advertised_endpoint("[0:0:0:0:0:0:0:0]:9000").is_err());
        assert!(validate_advertised_endpoint("[fe80::1]:9000").is_err());
        assert!(validate_advertised_endpoint("*:9000").is_err());
        assert!(validate_advertised_endpoint("bad_name:9000").is_err());
        assert!(validate_advertised_endpoint("::1:9000").is_err());
        assert!(validate_advertised_endpoint("localhost:0").is_err());
        assert!(validate_advertised_endpoint("node.example:9000").is_ok());
        assert!(validate_advertised_endpoint("[2001:db8::1]:9000").is_ok());
    }

    #[test]
    fn cache_is_bounded_deduplicated_and_excludes_requester() {
        let server = BootstrapServer::new(
            BootstrapServerConfig::new("cluster-a")
                .unwrap()
                .with_advertised_endpoint("seed.example:9000")
                .unwrap()
                .with_max_cached_candidates(2)
                .unwrap(),
        );

        server
            .exchange(&NodeId::new("node-a"), Some("a.example:9000".into()), 10)
            .unwrap();
        server
            .exchange(&NodeId::new("node-b"), Some("b.example:9000".into()), 10)
            .unwrap();
        server
            .exchange(&NodeId::new("node-c"), Some("c.example:9000".into()), 10)
            .unwrap();

        let response = server.exchange(&NodeId::new("node-a"), None, 2).unwrap();
        assert_eq!(response, vec!["seed.example:9000", "b.example:9000"]);
    }

    #[tokio::test]
    async fn one_shot_query_returns_candidates_without_registering_a_peer() {
        let server_config = BootstrapServerConfig::new("cluster-a")
            .unwrap()
            .with_max_response_candidates(4)
            .unwrap();
        let mut node = Node::new(
            NodeConfig::new(NodeId::new("seed"), "127.0.0.1:0")
                .with_bootstrap_server(server_config),
        );
        let mut events = node.take_event_receiver().unwrap();
        let bound = node.start_listener().await.unwrap();
        node.announce_bootstrap_endpoint(bound.to_string()).unwrap();

        let client =
            BootstrapClient::new(BootstrapClientConfig::new(NodeId::new("client"))).unwrap();
        let response = client
            .query(
                &bound.to_string(),
                BootstrapRequest::new("cluster-a", 4).with_advertised_endpoint("127.0.0.1:43111"),
            )
            .await
            .unwrap();

        assert_eq!(response.seed_node_id, NodeId::new("seed"));
        assert_eq!(response.endpoints, [bound.to_string()]);
        assert_eq!(node.connected_peer_count().await, 0);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), events.recv())
                .await
                .is_err()
        );
        node.shutdown().await;
    }

    #[tokio::test]
    async fn one_shot_query_supports_json_negotiation() {
        let server_config = BootstrapServerConfig::new("cluster-a").unwrap();
        let node = Node::new(
            NodeConfig::new(NodeId::new("seed"), "127.0.0.1:0")
                .with_serialization_format(SerializationFormat::Json)
                .with_bootstrap_server(server_config),
        );
        let bound = node.start_listener().await.unwrap();
        node.announce_bootstrap_endpoint(bound.to_string()).unwrap();
        let mut client_config = BootstrapClientConfig::new(NodeId::new("client"));
        client_config.serialization_format = SerializationFormat::Json;
        let client = BootstrapClient::new(client_config).unwrap();

        let response = client
            .query(&bound.to_string(), BootstrapRequest::new("cluster-a", 1))
            .await
            .unwrap();

        assert_eq!(response.endpoints, [bound.to_string()]);
        assert_eq!(node.connected_peer_count().await, 0);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn cluster_mismatch_is_rejected_without_populating_the_cache() {
        let server_config = BootstrapServerConfig::new("cluster-a").unwrap();
        let node = Node::new(
            NodeConfig::new(NodeId::new("seed"), "127.0.0.1:0")
                .with_bootstrap_server(server_config),
        );
        let bound = node.start_listener().await.unwrap();
        let client =
            BootstrapClient::new(BootstrapClientConfig::new(NodeId::new("client"))).unwrap();

        let error = client
            .query(
                &bound.to_string(),
                BootstrapRequest::new("cluster-b", 1).with_advertised_endpoint("127.0.0.1:43111"),
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            NetError::Wire(crate::WireError::BootstrapRejected { .. })
        ));
        assert_eq!(node.connected_peer_count().await, 0);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn bootstrap_seed_allowlist_is_checked_before_announcement_disclosure() {
        let pki = TestPki::generate().unwrap();
        let seed_id = certificate_node_id(&pki.dir_path().join("node1.pem"));
        let client_id = certificate_node_id(&pki.dir_path().join("node2.pem"));
        let seed = Node::new(
            NodeConfig::new(seed_id.clone(), "127.0.0.1:0")
                .with_tls(pki.node1_config())
                .with_bootstrap_server(BootstrapServerConfig::new("cluster-a").unwrap()),
        );
        let bound = seed.start_listener().await.unwrap();
        seed.announce_bootstrap_endpoint(bound.to_string()).unwrap();
        let seed_endpoint = format!("localhost:{}", bound.port());

        let denied_tls = pki
            .node2_config()
            .with_allowed_peers(HashSet::from(["00000000000000000000000000000000".into()]));
        let mut denied_config = BootstrapClientConfig::new(client_id.clone());
        denied_config.tls = Some(denied_tls);
        let denied = BootstrapClient::new(denied_config).unwrap();
        let error = denied
            .query(
                &seed_endpoint,
                BootstrapRequest::new("cluster-a", 4).with_advertised_endpoint("127.0.0.1:43111"),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, NetError::TlsError(_)));

        let mut allowed_config = BootstrapClientConfig::new(client_id);
        allowed_config.tls = Some(pki.node2_config());
        let allowed = BootstrapClient::new(allowed_config).unwrap();
        let response = allowed
            .query(&seed_endpoint, BootstrapRequest::new("cluster-a", 4))
            .await
            .unwrap();
        assert_eq!(response.seed_node_id, seed_id);
        assert_eq!(response.endpoints, [bound.to_string()]);
        seed.shutdown().await;
    }
}
