use nx_net::TlsConfig;

/// Default maximum number of simultaneously connected peers.
pub const DEFAULT_MAX_PEERS: usize = nx_net::DEFAULT_MAX_PEERS;

/// Default maximum number of locally-produced ops waiting for broadcast.
pub const DEFAULT_QUEUED_OPS_LIMIT: usize = 10_000;

/// Sync configuration for the runtime.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Initial peer addresses (e.g. ["127.0.0.1:9001"]).
    pub peers: Vec<String>,

    /// Address to listen on (e.g. "0.0.0.0:9000").
    pub listen_addr: Option<String>,

    /// Optional TLS/mTLS configuration for peer connections.
    pub tls: Option<TlsConfig>,

    /// Maximum number of simultaneously connected peers.
    pub max_peers: usize,

    /// Maximum number of locally-produced ops waiting for broadcast.
    pub queued_ops_limit: usize,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            peers: Vec::new(),
            listen_addr: None,
            tls: None,
            max_peers: DEFAULT_MAX_PEERS,
            queued_ops_limit: DEFAULT_QUEUED_OPS_LIMIT,
        }
    }
}

impl SyncConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_peer(mut self, addr: impl Into<String>) -> Self {
        self.peers.push(addr.into());
        self
    }

    pub fn with_listen_addr(mut self, addr: impl Into<String>) -> Self {
        self.listen_addr = Some(addr.into());
        self
    }

    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    pub fn with_max_peers(mut self, max_peers: usize) -> Self {
        self.max_peers = max_peers;
        self
    }

    pub fn with_queued_ops_limit(mut self, queued_ops_limit: usize) -> Self {
        self.queued_ops_limit = queued_ops_limit;
        self
    }

    /// Sync is enabled iff we have a bound listen address.
    pub fn is_enabled(&self) -> bool {
        self.listen_addr.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_enabled_requires_listen() {
        let cfg = SyncConfig::new();
        assert!(!cfg.is_enabled());
        assert_eq!(cfg.max_peers, DEFAULT_MAX_PEERS);
        assert_eq!(cfg.queued_ops_limit, DEFAULT_QUEUED_OPS_LIMIT);

        let cfg = SyncConfig::new().with_listen_addr("0.0.0.0:9000");
        assert!(cfg.is_enabled());
    }

    #[test]
    fn test_peers_alone_do_not_enable() {
        let cfg = SyncConfig::new().with_peer("127.0.0.1:9000");
        assert!(!cfg.is_enabled());
    }

    #[test]
    fn test_with_tls() {
        let cfg = SyncConfig::new().with_tls(TlsConfig::insecure_dev());
        assert!(cfg.tls.is_some());
        assert!(cfg.tls.as_ref().unwrap().insecure);
    }

    #[test]
    fn test_limits_are_configurable() {
        let cfg = SyncConfig::new()
            .with_max_peers(8)
            .with_queued_ops_limit(256);

        assert_eq!(cfg.max_peers, 8);
        assert_eq!(cfg.queued_ops_limit, 256);
    }
}
