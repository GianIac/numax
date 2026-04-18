/// config sync fo the runtime.
#[derive(Debug, Clone, Default)]
pub struct SyncConfig {
    /// Key prefixes to replicate (es. ["counter:", "state:"]).
    pub replicated_prefixes: Vec<String>,

    /// Initial peer addresses (es. ["127.0.0.1:9001"]).
    pub peers: Vec<String>,

    /// Address to listen on (es. "0.0.0.0:9000").
    pub listen_addr: Option<String>,
}

impl SyncConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.replicated_prefixes.push(prefix.into());
        self
    }

    pub fn with_peer(mut self, addr: impl Into<String>) -> Self {
        self.peers.push(addr.into());
        self
    }

    pub fn with_listen_addr(mut self, addr: impl Into<String>) -> Self {
        self.listen_addr = Some(addr.into());
        self
    }

    /// Check if a key belongs to a replicated prefix
    pub fn is_replicated(&self, key: &str) -> bool {
        self.replicated_prefixes.iter().any(|p| key.starts_with(p))
    }

    /// Sync is enabled if there is at least one prefix and one listen_addr.
    pub fn is_enabled(&self) -> bool {
        !self.replicated_prefixes.is_empty() && self.listen_addr.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_replicated() {
        let config = SyncConfig::new()
            .with_prefix("counter:")
            .with_prefix("state:");

        assert!(config.is_replicated("counter:visits"));
        assert!(config.is_replicated("state:user:123"));
        assert!(!config.is_replicated("local:temp"));
        assert!(!config.is_replicated("other"));
    }

    #[test]
    fn test_is_enabled() {
        let config = SyncConfig::new();
        assert!(!config.is_enabled());

        let config = SyncConfig::new()
            .with_prefix("counter:")
            .with_listen_addr("0.0.0.0:9000");
        assert!(config.is_enabled());
    }
}
