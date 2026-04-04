use std::collections::HashSet;
use std::time::Duration;

use nx_net::{Node, NodeConfig, TestPki, TlsConfig};
use tokio::time::timeout;

#[tokio::test]
async fn allowlist_rejects_peer_not_in_list() {
    let pki = TestPki::generate().expect("generate test PKI");

    // Server TLS config with allowlist that does NOT include the real client identity.
    let mut allowed = HashSet::new();
    allowed.insert("deadbeefdeadbeefdeadbeefdeadbeef".to_string()); // bogus id

    let server_tls: TlsConfig = pki.node1_config().with_allowed_peers(allowed);

    let server_cfg = NodeConfig::new(nx_sync::NodeId::new("server"), "127.0.0.1:0").with_tls(server_tls);

    let mut server = Node::new(server_cfg);
    let mut events = server.take_event_receiver().expect("event receiver");

    let addr = server.start_listener().await.expect("start listener");

    // Client tries to connect with valid TLS/mTLS, but its NodeId won't be allowlisted.
    let client_tls: TlsConfig = pki.node2_config();
    let client_cfg = NodeConfig::new(nx_sync::NodeId::new("client"), "127.0.0.1:0").with_tls(client_tls);
    let client = Node::new(client_cfg);

    let _ = client.connect_to_peer(&addr.to_string()).await;

    // Server must NOT emit PeerConnected.
    let res = timeout(Duration::from_millis(300), events.recv()).await;
    assert!(res.is_err(), "expected no PeerConnected event (peer rejected by allowlist)");
}