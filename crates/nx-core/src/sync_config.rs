use nx_net::TlsConfig;
use std::time::Duration;

/// Default maximum number of simultaneously connected peers.
pub const DEFAULT_MAX_PEERS: usize = nx_net::DEFAULT_MAX_PEERS;

/// Default maximum number of locally-produced ops waiting for broadcast.
pub const DEFAULT_QUEUED_OPS_LIMIT: usize = 10_000;

/// Default maximum accepted wire message size.
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = nx_net::DEFAULT_MAX_MESSAGE_SIZE;

/// Default timeout for socket reads and writes.
pub const DEFAULT_SOCKET_TIMEOUT: Duration = nx_net::DEFAULT_SOCKET_TIMEOUT;

/// Default first delay before retrying a failed configured peer connection.
pub const DEFAULT_RECONNECT_INITIAL_DELAY: Duration = Duration::from_millis(500);

/// Default maximum delay between reconnect attempts for a configured peer.
pub const DEFAULT_RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);

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

    /// Maximum accepted wire message size.
    pub max_message_size: usize,

    /// Timeout for socket reads and writes.
    pub socket_timeout: Duration,

    /// Initial delay for automatic reconnect attempts.
    pub reconnect_initial_delay: Duration,

    /// Maximum delay for automatic reconnect attempts.
    pub reconnect_max_delay: Duration,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            peers: Vec::new(),
            listen_addr: None,
            tls: None,
            max_peers: DEFAULT_MAX_PEERS,
            queued_ops_limit: DEFAULT_QUEUED_OPS_LIMIT,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            socket_timeout: DEFAULT_SOCKET_TIMEOUT,
            reconnect_initial_delay: DEFAULT_RECONNECT_INITIAL_DELAY,
            reconnect_max_delay: DEFAULT_RECONNECT_MAX_DELAY,
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

    pub fn with_max_message_size(mut self, max_message_size: usize) -> Self {
        self.max_message_size = max_message_size;
        self
    }

    pub fn with_socket_timeout(mut self, socket_timeout: Duration) -> Self {
        self.socket_timeout = socket_timeout;
        self
    }

    pub fn with_reconnect_backoff(mut self, initial_delay: Duration, max_delay: Duration) -> Self {
        self.reconnect_initial_delay = initial_delay;
        self.reconnect_max_delay = max_delay;
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
        assert_eq!(cfg.max_message_size, DEFAULT_MAX_MESSAGE_SIZE);
        assert_eq!(cfg.socket_timeout, DEFAULT_SOCKET_TIMEOUT);
        assert_eq!(cfg.reconnect_initial_delay, DEFAULT_RECONNECT_INITIAL_DELAY);
        assert_eq!(cfg.reconnect_max_delay, DEFAULT_RECONNECT_MAX_DELAY);

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
            .with_queued_ops_limit(256)
            .with_max_message_size(1024)
            .with_socket_timeout(Duration::from_secs(5))
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_secs(2));

        assert_eq!(cfg.max_peers, 8);
        assert_eq!(cfg.queued_ops_limit, 256);
        assert_eq!(cfg.max_message_size, 1024);
        assert_eq!(cfg.socket_timeout, Duration::from_secs(5));
        assert_eq!(cfg.reconnect_initial_delay, Duration::from_millis(10));
        assert_eq!(cfg.reconnect_max_delay, Duration::from_secs(2));
    }
}
