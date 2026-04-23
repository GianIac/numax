use nx_net::TlsConfig;

/// Sync configuration for the runtime.
#[derive(Debug, Clone, Default)]
pub struct SyncConfig {
    /// Initial peer addresses (e.g. ["127.0.0.1:9001"]).
    pub peers: Vec<String>,

    /// Address to listen on (e.g. "0.0.0.0:9000").
    pub listen_addr: Option<String>,

    /// Optional TLS/mTLS configuration for peer connections.
    pub tls: Option<TlsConfig>,
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
}