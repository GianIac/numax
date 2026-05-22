use std::time::Duration;

use nx_net::{Message, MessageKind, Node, NodeConfig, SerializationFormat, TestPki};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::timeout;

#[tokio::test]
async fn node_id_mismatch_disconnects_immediately() {
    let pki = TestPki::generate().expect("generate test PKI");

    // Server node with TLS enabled.
    let server_cfg =
        NodeConfig::new(nx_sync::NodeId::new("server"), "127.0.0.1:0").with_tls(pki.node1_config());

    let mut server = Node::new(server_cfg);
    let mut events = server.take_event_receiver().expect("event receiver");

    let addr = server.start_listener().await.expect("start listener");

    // Client establishes TLS correctly, but lies in HELLO.node_id.
    let client_task = tokio::spawn(async move {
        let tcp = TcpStream::connect(addr).await.expect("connect tcp");

        let tls_cfg = pki.node2_config();
        let connector = tls_cfg.build_connector().expect("build_connector");

        let server_name =
            rustls::pki_types::ServerName::try_from("localhost").expect("valid server name");

        let tls = connector
            .connect(server_name, tcp)
            .await
            .expect("tls connect");

        let mut stream = nx_net::NetStream::TlsClient(tls);

        // Forge a NodeId that cannot match the cert-derived identity.
        let forged = nx_sync::NodeId::new("00000000000000000000000000000000");
        let hello = Message {
            kind: MessageKind::Hello {
                node_id: forged,
                version: 1,
                supported_formats: vec![SerializationFormat::Bincode, SerializationFormat::Json],
                preferred_format: SerializationFormat::Bincode,
            },
        };

        let bytes = hello.to_bytes().expect("serialize hello");
        stream.write_all(&bytes).await.expect("write hello");
        let _ = stream.flush().await;
    });

    // Server must NOT emit PeerConnected for the forged client.
    let server_wait = tokio::spawn(async move {
        let res = timeout(Duration::from_millis(300), events.recv()).await;
        assert!(res.is_err(), "expected no events (peer should be rejected)");
    });

    timeout(Duration::from_secs(5), client_task)
        .await
        .expect("client timeout")
        .expect("client join");

    timeout(Duration::from_secs(5), server_wait)
        .await
        .expect("server timeout")
        .expect("server join");
}
