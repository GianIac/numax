use nx_sync::NodeId;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// Identifier of a peer (based on NodeId).
pub type PeerId = NodeId;

#[allow(dead_code)]
/// Connection state of a peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerState {
    /// Not yet connected.
    Disconnected,
    /// Connection in progress.
    Connecting,
    /// Connected, handshake in progress.
    Handshaking,
    /// Connected and operational.
    Connected,
    /// Error, waiting for retry.
    Failed,
}

/// Information about a peer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    /// Address of the peer.
    pub addr: String,

    /// NodeId (known after handshake).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<NodeId>,
}

impl PeerInfo {
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            node_id: None,
        }
    }

    pub fn with_node_id(mut self, node_id: NodeId) -> Self {
        self.node_id = Some(node_id);
        self
    }

    /// Parses the address as SocketAddr.
    pub fn socket_addr(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        self.addr.parse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_info_new() {
        let peer = PeerInfo::new("127.0.0.1:9000");
        assert_eq!(peer.addr, "127.0.0.1:9000");
        assert!(peer.node_id.is_none());
    }

    #[test]
    fn test_peer_info_with_node_id() {
        let peer = PeerInfo::new("127.0.0.1:9000").with_node_id(NodeId::new("node-1"));
        assert!(peer.node_id.is_some());
    }

    #[test]
    fn test_peer_socket_addr() {
        let peer = PeerInfo::new("127.0.0.1:9000");
        let addr = peer.socket_addr().unwrap();
        assert_eq!(addr.port(), 9000);
    }
}
