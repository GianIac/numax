use std::collections::HashMap;
use std::sync::Arc;

use nx_sync::{NodeId, Op};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, mpsc, watch};
use tracing::{debug, error, info, warn};

use crate::error::{NetError, NetResult};
use crate::message::{Message, MessageKind, PROTOCOL_VERSION};
use crate::peer::{PeerInfo, PeerState};
use crate::tls::{NetStream, TlsConfig};

/// Node configuration.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// NodeId of this node.
    pub node_id: NodeId,

    /// Address to listen on (e.g. "0.0.0.0:9000").
    pub listen_addr: String,

    /// Initial peers to connect to.
    pub initial_peers: Vec<String>,

    /// Optional TLS/mTLS configuration.
    pub tls: Option<TlsConfig>,
}

impl NodeConfig {
    pub fn new(node_id: NodeId, listen_addr: impl Into<String>) -> Self {
        Self {
            node_id,
            listen_addr: listen_addr.into(),
            initial_peers: Vec::new(),
            tls: None,
        }
    }

    pub fn with_peers(mut self, peers: Vec<String>) -> Self {
        self.initial_peers = peers;
        self
    }

    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }
}

/// Node exit event (for runtime).
#[derive(Debug, Clone)]
pub enum NodeEvent {
    /// received new operations from a peer.
    OpsReceived { from: NodeId, ops: Vec<Op> },

    /// Peer connected.
    PeerConnected { node_id: NodeId },

    /// Peer disconnected.
    PeerDisconnected { node_id: NodeId },
}

#[allow(dead_code)]
/// internal state for each peer connection.
struct PeerConnection {
    info: PeerInfo,
    state: PeerState,
    writer: Option<tokio::io::WriteHalf<crate::tls::NetStream>>,
}

/// node
pub struct Node {
    config: NodeConfig,
    peers: Arc<RwLock<HashMap<String, PeerConnection>>>,
    event_tx: mpsc::Sender<NodeEvent>,
    event_rx: Option<mpsc::Receiver<NodeEvent>>,
    shutdown_tx: watch::Sender<bool>,
}

impl Node {
    /// crate new node
    pub fn new(config: NodeConfig) -> Self {
        let (event_tx, event_rx) = mpsc::channel(100);
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);

        Self {
            config,
            peers: Arc::new(RwLock::new(HashMap::new())),
            event_tx,
            event_rx: Some(event_rx),
            shutdown_tx,
        }
    }

    /// Gets the event receiver (can only be called once).
    pub fn take_event_receiver(&mut self) -> Option<mpsc::Receiver<NodeEvent>> {
        self.event_rx.take()
    }

    /// Start listener TCP.
    ///
    /// Returns the actual bound address (useful when binding to port 0 in tests).
    pub async fn start_listener(&self) -> NetResult<std::net::SocketAddr> {
        let listener = TcpListener::bind(&self.config.listen_addr).await?;
        let bound_addr = listener.local_addr()?;

        info!(addr = %bound_addr, "listening");

        let peers = Arc::clone(&self.peers);
        let node_id = self.config.node_id.clone();
        let event_tx = self.event_tx.clone();
        let tls = self.config.tls.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            debug!("listener shutdown requested");
                            break;
                        }
                    }
                    accepted = listener.accept() => {
                        match accepted {
                            Ok((stream, addr)) => {
                                info!(%addr, "incoming connection");
                                let peers = Arc::clone(&peers);
                                let node_id = node_id.clone();
                                let event_tx = event_tx.clone();
                                let tls = tls.clone();

                                tokio::spawn(async move {
                                    if let Err(e) = handle_incoming(
                                        stream,
                                        addr.to_string(),
                                        tls,
                                        node_id,
                                        peers,
                                        event_tx,
                                    )
                                    .await
                                    {
                                        error!(%addr, error = %e, "connection error");
                                    }
                                });
                            }
                            Err(e) => {
                                error!(error = %e, "accept error");
                            }
                        }
                    }
                }
            }
        });

        Ok(bound_addr)
    }

    /// Conncet to a peer
    pub async fn connect_to_peer(&self, addr: &str) -> NetResult<()> {
        let tcp = TcpStream::connect(addr)
            .await
            .map_err(|e| NetError::ConnectionFailed(format!("{}: {}", addr, e)))?;

        let stream: NetStream = if let Some(tls_cfg) = &self.config.tls {
            // Extract host from "host:port"
            let host = addr.rsplit_once(':').map(|(h, _)| h).unwrap_or(addr);

            // rustls verifies the presented certificate against this name
            // ServerName returned above is not 'static; turn it into owned 'static
            let server_name = rustls::pki_types::ServerName::try_from(host)
                .or_else(|_| rustls::pki_types::ServerName::try_from("localhost"))
                .map_err(|e| {
                    NetError::TlsError(format!("invalid server name '{}': {}", host, e))
                })?;

            let server_name = server_name.to_owned();

            tls_cfg.connect_stream(tcp, server_name).await?
        } else {
            NetStream::Plain(tcp)
        };

        // Capture the peer certificate (owned) before moving the stream into split().
        let peer_cert = stream.peer_cert_der();

        let (mut reader, mut writer) = tokio::io::split(stream);

        // Send HELLO
        let hello = Message::hello(self.config.node_id.clone());
        write_message(&mut writer, &hello).await?;

        // Wait HELLO_ACK
        let response = read_message(&mut reader).await?;
        let peer_node_id = match response.kind {
            MessageKind::HelloAck { node_id, version } => {
                if version != PROTOCOL_VERSION {
                    warn!(
                        their_version = version,
                        our_version = PROTOCOL_VERSION,
                        "protocol version mismatch"
                    );
                }
                info!(peer = %node_id, "handshake complete");
                node_id
            }
            _ => {
                return Err(NetError::InvalidMessage("expected HelloAck".into()));
            }
        };

        // TLS identity binding: claimed NodeId must match the peer certificate public key.
        if let Some(tls_cfg) = &self.config.tls
            && !tls_cfg.insecure
        {
            let peer_cert = peer_cert.ok_or_else(|| {
                NetError::TlsError("missing peer certificate in TLS session".into())
            })?;

            let expected = crate::tls::derive_protocol_node_id_from_cert(&peer_cert)?;

            if peer_node_id != expected {
                let fingerprint = crate::tls::cert_fingerprint_hex(&peer_cert)
                    .unwrap_or_else(|_| "<unavailable>".into());

                return Err(NetError::TlsError(format!(
                    "node_id mismatch (claimed={:?}, expected={:?}, fingerprint={})",
                    peer_node_id, expected, fingerprint
                )));
            }

            // Optional allowlist enforcement (permissioned network).
            if let Some(_allowed) = &tls_cfg.allowed_peers {
                // Peer NodeId on the wire is nx_sync::NodeId; allowlist stores strings.
                let peer_id_str = peer_node_id.to_string();
                if !tls_cfg.is_peer_allowed(&peer_id_str) {
                    return Err(NetError::TlsError(format!(
                        "peer node_id not in allowlist: {:?}",
                        peer_node_id
                    )));
                }
            }
        }

        // Save connection
        {
            let mut peers = self.peers.write().await;
            peers.insert(
                addr.to_string(),
                PeerConnection {
                    info: PeerInfo::new(addr).with_node_id(peer_node_id.clone()),
                    state: PeerState::Connected,
                    writer: Some(writer),
                },
            );
        }

        // Notify Event
        let _ = self
            .event_tx
            .send(NodeEvent::PeerConnected {
                node_id: peer_node_id.clone(),
            })
            .await;

        // Start read loop
        let peers = Arc::clone(&self.peers);
        let event_tx = self.event_tx.clone();
        let addr_owned = addr.to_string();

        tokio::spawn(async move {
            if let Err(e) = read_loop(reader, peer_node_id.clone(), event_tx.clone()).await {
                debug!(peer = %peer_node_id, error = %e, "read loop ended");
            }

            // Cleanup
            let mut peers = peers.write().await;
            peers.remove(&addr_owned);

            let _ = event_tx
                .send(NodeEvent::PeerDisconnected {
                    node_id: peer_node_id,
                })
                .await;
        });

        Ok(())
    }

    /// Send ops to all connected peers.
    pub async fn broadcast_ops(&self, ops: Vec<Op>) -> NetResult<()> {
        let msg = Message::push_ops(ops);
        let bytes = msg.to_bytes()?;

        let mut peers = self.peers.write().await;

        for (addr, conn) in peers.iter_mut() {
            if conn.state == PeerState::Connected
                && let Some(ref mut writer) = conn.writer
                && let Err(e) = writer.write_all(&bytes).await
            {
                warn!(%addr, error = %e, "failed to send ops");
                conn.state = PeerState::Failed;
            }
        }

        Ok(())
    }

    /// Returns the number of currently connected peers.
    pub async fn connected_peer_count(&self) -> usize {
        let peers = self.peers.read().await;
        peers
            .values()
            .filter(|c| c.state == PeerState::Connected)
            .count()
    }

    /// Close outbound peer connections by dropping their writers.
    pub async fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
        let mut peers = self.peers.write().await;
        let count = peers.len();
        peers.clear();
        debug!(count, "node peer connections closed");
    }
}

/// Manage an incoming connection from a peer (handshake + read loop).
async fn handle_incoming(
    stream: TcpStream,
    addr: String,
    tls: Option<TlsConfig>,
    our_node_id: NodeId,
    peers: Arc<RwLock<HashMap<String, PeerConnection>>>,
    event_tx: mpsc::Sender<NodeEvent>,
) -> NetResult<()> {
    let stream: NetStream = match tls {
        Some(ref tls_cfg) => tls_cfg.accept_stream(stream).await?,
        None => NetStream::Plain(stream),
    };

    // Capture the peer certificate (owned) before moving the stream into split().
    let peer_cert = stream.peer_cert_der();

    let (mut reader, mut writer) = tokio::io::split(stream);

    // Wait for HELLO
    let msg = read_message(&mut reader).await?;
    let peer_node_id = match msg.kind {
        MessageKind::Hello { node_id, version } => {
            if version != PROTOCOL_VERSION {
                warn!(their_version = version, "protocol mismatch");
            }
            node_id
        }
        _ => {
            return Err(NetError::InvalidMessage("expected Hello".into()));
        }
    };

    // TLS identity binding: claimed NodeId must match the peer certificate public key.
    if let Some(tls_cfg) = &tls
        && !tls_cfg.insecure
    {
        let peer_cert = peer_cert
            .ok_or_else(|| NetError::TlsError("missing peer certificate in TLS session".into()))?;

        let expected = crate::tls::derive_protocol_node_id_from_cert(&peer_cert)?;

        if peer_node_id != expected {
            let fingerprint = crate::tls::cert_fingerprint_hex(&peer_cert)
                .unwrap_or_else(|_| "<unavailable>".into());

            return Err(NetError::TlsError(format!(
                "node_id mismatch (claimed={:?}, expected={:?}, fingerprint={})",
                peer_node_id, expected, fingerprint
            )));
        }

        // Optional allowlist enforcement (permissioned network).
        if let Some(_allowed) = &tls_cfg.allowed_peers {
            let peer_id_str = peer_node_id.to_string();
            if !tls_cfg.is_peer_allowed(&peer_id_str) {
                return Err(NetError::TlsError(format!(
                    "peer node_id not in allowlist: {:?}",
                    peer_node_id
                )));
            }
        }
    }

    // Send HELLO_ACK
    let ack = Message::hello_ack(our_node_id);
    write_message(&mut writer, &ack).await?;

    {
        let mut peers = peers.write().await;
        peers.insert(
            addr.clone(),
            PeerConnection {
                info: PeerInfo::new(&addr).with_node_id(peer_node_id.clone()),
                state: PeerState::Connected,
                writer: Some(writer),
            },
        );
    }

    info!(peer = %peer_node_id, "incoming peer connected");

    // Notify Event
    let _ = event_tx
        .send(NodeEvent::PeerConnected {
            node_id: peer_node_id.clone(),
        })
        .await;

    // Read Loop
    read_loop(reader, peer_node_id.clone(), event_tx.clone()).await?;

    {
        let mut peers = peers.write().await;
        peers.remove(&addr);
    }

    let _ = event_tx
        .send(NodeEvent::PeerDisconnected {
            node_id: peer_node_id,
        })
        .await;

    Ok(())
}

/// Loop for reading messages from a peer until disconnection
async fn read_loop(
    mut reader: tokio::io::ReadHalf<NetStream>,
    peer_node_id: NodeId,
    event_tx: mpsc::Sender<NodeEvent>,
) -> NetResult<()> {
    loop {
        let msg = match read_message(&mut reader).await {
            Ok(m) => m,
            Err(NetError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                debug!(peer = %peer_node_id, "peer disconnected");
                break;
            }
            Err(e) => return Err(e),
        };

        match msg.kind {
            MessageKind::PushOps { ops } => {
                debug!(peer = %peer_node_id, count = ops.len(), "received ops");
                let _ = event_tx
                    .send(NodeEvent::OpsReceived {
                        from: peer_node_id.clone(),
                        ops,
                    })
                    .await;
            }
            MessageKind::Ping => {
                debug!(peer = %peer_node_id, "received ping");
                // TODO: send Pong (serve writer)
            }
            _ => {
                debug!(peer = %peer_node_id, kind = ?msg.kind, "unhandled message");
            }
        }
    }

    Ok(())
}

/// Writes a message to a stream.
async fn write_message<W: AsyncWriteExt + Unpin>(writer: &mut W, msg: &Message) -> NetResult<()> {
    let bytes = msg.to_bytes()?;
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

/// Reads a message from a stream.
async fn read_message<R: AsyncReadExt + Unpin>(reader: &mut R) -> NetResult<Message> {
    // Read length (4 bytes)
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    // Sanity check
    if len > 10 * 1024 * 1024 {
        return Err(NetError::InvalidMessage("message too large".into()));
    }

    // Read payload
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf).await?;

    Message::from_bytes(&buf).map_err(|e| e.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_node_config() {
        let config = NodeConfig::new(NodeId::new("test"), "127.0.0.1:9000")
            .with_peers(vec!["127.0.0.1:9001".into()]);

        assert_eq!(config.listen_addr, "127.0.0.1:9000");
        assert_eq!(config.initial_peers.len(), 1);
    }
}
