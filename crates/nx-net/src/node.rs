use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use nx_sync::{NodeId, Op};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, OwnedSemaphorePermit, RwLock, Semaphore, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use crate::error::{NetError, NetResult};
use crate::message::{Message, MessageKind, PROTOCOL_VERSION};
use crate::peer::{PeerInfo, PeerState};
use crate::tls::{NetStream, TlsConfig};

/// Default maximum number of simultaneously connected peers.
pub const DEFAULT_MAX_PEERS: usize = 64;

/// Default maximum accepted wire message size.
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Default timeout for socket reads and writes.
pub const DEFAULT_SOCKET_TIMEOUT: Duration = Duration::from_secs(30);

/// Time allowed for network tasks to finish cooperatively after shutdown.
const TASK_SHUTDOWN_GRACE: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, Copy)]
struct NodeLimits {
    max_peers: usize,
    max_message_size: usize,
    socket_timeout: Duration,
}

struct IncomingContext {
    tls: Option<TlsConfig>,
    our_node_id: NodeId,
    peers: Arc<RwLock<HashMap<String, PeerConnection>>>,
    event_tx: mpsc::Sender<NodeEvent>,
    limits: NodeLimits,
    slot: OwnedSemaphorePermit,
    shutdown_rx: watch::Receiver<bool>,
}

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

    /// Maximum number of simultaneously connected peers.
    pub max_peers: usize,

    /// Maximum accepted wire message size.
    pub max_message_size: usize,

    /// Timeout for socket reads and writes.
    pub socket_timeout: Duration,
}

impl NodeConfig {
    pub fn new(node_id: NodeId, listen_addr: impl Into<String>) -> Self {
        Self {
            node_id,
            listen_addr: listen_addr.into(),
            initial_peers: Vec::new(),
            tls: None,
            max_peers: DEFAULT_MAX_PEERS,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            socket_timeout: DEFAULT_SOCKET_TIMEOUT,
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

    pub fn with_max_peers(mut self, max_peers: usize) -> Self {
        self.max_peers = max_peers;
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
}

/// Node exit event (for runtime).
#[derive(Debug, Clone)]
pub enum NodeEvent {
    /// received new operations from a peer.
    OpsReceived { from: NodeId, ops: Vec<Op> },

    /// Peer connected.
    PeerConnected {
        node_id: NodeId,
        peers_connected: usize,
    },

    /// Peer disconnected.
    PeerDisconnected {
        node_id: NodeId,
        peers_connected: usize,
    },
}

#[allow(dead_code)]
/// internal state for each peer connection.
struct PeerConnection {
    info: PeerInfo,
    state: PeerState,
    writer: Option<Arc<tokio::sync::Mutex<tokio::io::WriteHalf<crate::tls::NetStream>>>>,
    _slot: OwnedSemaphorePermit,
}

/// node
pub struct Node {
    config: NodeConfig,
    peers: Arc<RwLock<HashMap<String, PeerConnection>>>,
    event_tx: mpsc::Sender<NodeEvent>,
    event_rx: Option<mpsc::Receiver<NodeEvent>>,
    shutdown_tx: watch::Sender<bool>,
    connection_slots: Arc<Semaphore>,
    tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl Node {
    /// crate new node
    pub fn new(config: NodeConfig) -> Self {
        let (event_tx, event_rx) = mpsc::channel(100);
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let max_peers = config.max_peers;

        Self {
            config,
            peers: Arc::new(RwLock::new(HashMap::new())),
            event_tx,
            event_rx: Some(event_rx),
            shutdown_tx,
            connection_slots: Arc::new(Semaphore::new(max_peers)),
            tasks: Arc::new(Mutex::new(Vec::new())),
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
        let connection_slots = Arc::clone(&self.connection_slots);
        let tasks = Arc::clone(&self.tasks);
        let limits = NodeLimits {
            max_peers: self.config.max_peers,
            max_message_size: self.config.max_message_size,
            socket_timeout: self.config.socket_timeout,
        };
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let shutdown_tx = self.shutdown_tx.clone();

        let listener_task = tokio::spawn(async move {
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
                                let slot = match Arc::clone(&connection_slots).try_acquire_owned() {
                                    Ok(slot) => slot,
                                    Err(_) => {
                                        warn!(%addr, limit = limits.max_peers, "rejecting incoming connection: peer limit reached");
                                        continue;
                                    }
                                };
                                let peers = Arc::clone(&peers);
                                let node_id = node_id.clone();
                                let event_tx = event_tx.clone();
                                let tls = tls.clone();
                                let limits = limits;
                                let shutdown_rx = shutdown_tx.subscribe();

                                let task = tokio::spawn(async move {
                                    let context = IncomingContext {
                                        tls,
                                        our_node_id: node_id,
                                        peers,
                                        event_tx,
                                        limits,
                                        slot,
                                        shutdown_rx,
                                    };

                                    if let Err(e) =
                                        handle_incoming(stream, addr.to_string(), context).await
                                    {
                                        error!(%addr, error = %e, "connection error");
                                    }
                                });
                                tasks.lock().await.push(task);
                            }
                            Err(e) => {
                                error!(error = %e, "accept error");
                            }
                        }
                    }
                }
            }
        });
        self.tasks.lock().await.push(listener_task);

        Ok(bound_addr)
    }

    /// Conncet to a peer
    pub async fn connect_to_peer(&self, addr: &str) -> NetResult<()> {
        self.ensure_peer_slot_available().await?;
        let slot = Arc::clone(&self.connection_slots)
            .try_acquire_owned()
            .map_err(|_| NetError::PeerLimitReached(self.config.max_peers))?;

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
        write_message(&mut writer, &hello, self.config.socket_timeout).await?;

        // Wait HELLO_ACK
        let response = read_message(
            &mut reader,
            self.config.max_message_size,
            self.config.socket_timeout,
        )
        .await?;
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
        let peers_connected = {
            let mut peers = self.peers.write().await;
            ensure_peer_slot_available(&peers, self.config.max_peers, Some(addr))?;
            peers.insert(
                addr.to_string(),
                PeerConnection {
                    info: PeerInfo::new(addr).with_node_id(peer_node_id.clone()),
                    state: PeerState::Connected,
                    writer: Some(Arc::new(tokio::sync::Mutex::new(writer))),
                    _slot: slot,
                },
            );
            connected_peer_count(&peers)
        };

        // Notify Event
        let _ = self
            .event_tx
            .send(NodeEvent::PeerConnected {
                node_id: peer_node_id.clone(),
                peers_connected,
            })
            .await;

        // Start read loop
        let peers = Arc::clone(&self.peers);
        let event_tx = self.event_tx.clone();
        let addr_owned = addr.to_string();
        let max_message_size = self.config.max_message_size;
        let socket_timeout = self.config.socket_timeout;
        let shutdown_rx = self.shutdown_tx.subscribe();

        let task = tokio::spawn(async move {
            if let Err(e) = read_loop(
                reader,
                peer_node_id.clone(),
                event_tx.clone(),
                max_message_size,
                socket_timeout,
                shutdown_rx,
            )
            .await
            {
                debug!(peer = %peer_node_id, error = %e, "read loop ended");
            }

            // Cleanup
            let disconnected = {
                let mut peers = peers.write().await;
                peers.remove(&addr_owned).and_then(|removed| {
                    (removed.state == PeerState::Connected)
                        .then(|| (peer_node_id.clone(), connected_peer_count(&peers)))
                })
            };

            if let Some((node_id, peers_connected)) = disconnected {
                let _ = event_tx
                    .send(NodeEvent::PeerDisconnected {
                        node_id,
                        peers_connected,
                    })
                    .await;
            }
        });
        self.tasks.lock().await.push(task);

        Ok(())
    }

    /// Send ops to all connected peers.
    pub async fn broadcast_ops(&self, ops: Vec<Op>) -> NetResult<()> {
        let msg = Message::push_ops(ops);
        let bytes = msg.to_bytes()?;

        let writers = {
            let peers = self.peers.read().await;
            peers
                .iter()
                .filter_map(|(addr, conn)| {
                    (conn.state == PeerState::Connected)
                        .then(|| {
                            conn.writer
                                .as_ref()
                                .map(|writer| (addr.clone(), Arc::clone(writer)))
                        })
                        .flatten()
                })
                .collect::<Vec<_>>()
        };

        let mut failed = Vec::new();
        for (addr, writer) in writers {
            let mut writer = writer.lock().await;
            if let Err(e) = write_bytes(&mut *writer, &bytes, self.config.socket_timeout).await {
                warn!(%addr, error = %e, "failed to send ops");
                failed.push(addr.clone());
                if let Some((node_id, peers_connected)) = self.mark_peer_failed(&addr).await {
                    let _ = self
                        .event_tx
                        .send(NodeEvent::PeerDisconnected {
                            node_id,
                            peers_connected,
                        })
                        .await;
                }
            }
        }

        if !failed.is_empty() {
            return Err(NetError::PeerDisconnected(failed.join(",")));
        }

        Ok(())
    }

    /// Returns the number of currently connected peers.
    pub async fn connected_peer_count(&self) -> usize {
        let peers = self.peers.read().await;
        connected_peer_count(&peers)
    }

    async fn ensure_peer_slot_available(&self) -> NetResult<()> {
        let peers = self.peers.read().await;
        ensure_peer_slot_available(&peers, self.config.max_peers, None)
    }

    async fn mark_peer_failed(&self, addr: &str) -> Option<(NodeId, usize)> {
        let mut peers = self.peers.write().await;
        let node_id = {
            let conn = peers.get_mut(addr)?;
            conn.state = PeerState::Failed;
            conn.info.node_id.clone()?
        };
        Some((node_id, connected_peer_count(&peers)))
    }

    /// Close outbound peer connections by dropping their writers.
    pub async fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);

        let mut tasks = {
            let mut tasks = self.tasks.lock().await;
            std::mem::take(&mut *tasks)
        };

        for mut task in tasks.drain(..) {
            match timeout(TASK_SHUTDOWN_GRACE, &mut task).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    debug!(error = %e, "network task ended during shutdown");
                }
                Err(_) => {
                    warn!("network task did not finish cooperatively; aborting");
                    task.abort();
                    let _ = task.await;
                }
            }
        }

        let count = {
            let mut peers = self.peers.write().await;
            let count = peers.len();
            peers.clear();
            count
        };
        debug!(count, "node peer connections closed");
    }
}

/// Manage an incoming connection from a peer (handshake + read loop).
async fn handle_incoming(
    stream: TcpStream,
    addr: String,
    context: IncomingContext,
) -> NetResult<()> {
    let IncomingContext {
        tls,
        our_node_id,
        peers,
        event_tx,
        limits,
        slot,
        shutdown_rx,
    } = context;

    let stream: NetStream = match tls {
        Some(ref tls_cfg) => tls_cfg.accept_stream(stream).await?,
        None => NetStream::Plain(stream),
    };

    // Capture the peer certificate (owned) before moving the stream into split().
    let peer_cert = stream.peer_cert_der();

    let (mut reader, mut writer) = tokio::io::split(stream);

    // Wait for HELLO
    let msg = read_message(&mut reader, limits.max_message_size, limits.socket_timeout).await?;
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

    {
        let peers = peers.read().await;
        ensure_peer_slot_available(&peers, limits.max_peers, Some(&addr))?;
    }

    let ack = Message::hello_ack(our_node_id);
    write_message(&mut writer, &ack, limits.socket_timeout).await?;

    let peers_connected = {
        let mut peers = peers.write().await;
        ensure_peer_slot_available(&peers, limits.max_peers, Some(&addr))?;
        peers.insert(
            addr.clone(),
            PeerConnection {
                info: PeerInfo::new(&addr).with_node_id(peer_node_id.clone()),
                state: PeerState::Connected,
                writer: Some(Arc::new(tokio::sync::Mutex::new(writer))),
                _slot: slot,
            },
        );
        connected_peer_count(&peers)
    };

    info!(peer = %peer_node_id, "incoming peer connected");

    // Notify Event
    let _ = event_tx
        .send(NodeEvent::PeerConnected {
            node_id: peer_node_id.clone(),
            peers_connected,
        })
        .await;

    // Read Loop
    let read_result = read_loop(
        reader,
        peer_node_id.clone(),
        event_tx.clone(),
        limits.max_message_size,
        limits.socket_timeout,
        shutdown_rx,
    )
    .await;

    let disconnected = {
        let mut peers = peers.write().await;
        let Some(removed) = peers.remove(&addr) else {
            return read_result;
        };
        (removed.state == PeerState::Connected)
            .then(|| (peer_node_id.clone(), connected_peer_count(&peers)))
    };

    if let Some((node_id, peers_connected)) = disconnected {
        let _ = event_tx
            .send(NodeEvent::PeerDisconnected {
                node_id,
                peers_connected,
            })
            .await;
    }

    read_result
}

fn connected_peer_count(peers: &HashMap<String, PeerConnection>) -> usize {
    peers
        .values()
        .filter(|c| c.state == PeerState::Connected)
        .count()
}

fn ensure_peer_slot_available(
    peers: &HashMap<String, PeerConnection>,
    max_peers: usize,
    replacing_addr: Option<&str>,
) -> NetResult<()> {
    if replacing_addr.is_some_and(|addr| peers.contains_key(addr)) {
        return Ok(());
    }

    let count = connected_peer_count(peers);
    if count >= max_peers {
        return Err(NetError::PeerLimitReached(max_peers));
    }

    Ok(())
}

/// Loop for reading messages from a peer until disconnection
async fn read_loop(
    mut reader: tokio::io::ReadHalf<NetStream>,
    peer_node_id: NodeId,
    event_tx: mpsc::Sender<NodeEvent>,
    max_message_size: usize,
    socket_timeout: Duration,
    mut shutdown_rx: watch::Receiver<bool>,
) -> NetResult<()> {
    loop {
        let msg = tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    debug!(peer = %peer_node_id, "read loop shutdown requested");
                    break;
                }
                continue;
            }

            msg = read_message(&mut reader, max_message_size, socket_timeout) => {
                match msg {
                    Ok(m) => m,
                    Err(NetError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        debug!(peer = %peer_node_id, "peer disconnected");
                        break;
                    }
                    Err(e) => return Err(e),
                }
            }
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
async fn write_message<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    msg: &Message,
    socket_timeout: Duration,
) -> NetResult<()> {
    let bytes = msg.to_bytes()?;
    write_bytes(writer, &bytes, socket_timeout).await?;
    Ok(())
}

async fn write_bytes<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    bytes: &[u8],
    socket_timeout: Duration,
) -> NetResult<()> {
    timeout(socket_timeout, writer.write_all(bytes))
        .await
        .map_err(|_| NetError::Timeout)??;
    timeout(socket_timeout, writer.flush())
        .await
        .map_err(|_| NetError::Timeout)??;
    Ok(())
}

/// Reads a message from a stream.
async fn read_message<R: AsyncReadExt + Unpin>(
    reader: &mut R,
    max_message_size: usize,
    socket_timeout: Duration,
) -> NetResult<Message> {
    // Read length (4 bytes)
    let mut len_buf = [0u8; 4];
    timeout(socket_timeout, reader.read_exact(&mut len_buf))
        .await
        .map_err(|_| NetError::Timeout)??;
    let len = u32::from_be_bytes(len_buf) as usize;

    // Sanity check
    if len > max_message_size {
        return Err(NetError::MessageTooLarge {
            len,
            limit: max_message_size,
        });
    }

    // Read payload
    let mut buf = vec![0u8; len];
    timeout(socket_timeout, reader.read_exact(&mut buf))
        .await
        .map_err(|_| NetError::Timeout)??;

    Message::from_bytes(&buf).map_err(|e| e.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_slot() -> OwnedSemaphorePermit {
        Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap()
    }

    #[tokio::test]
    async fn test_node_config() {
        let config = NodeConfig::new(NodeId::new("test"), "127.0.0.1:9000")
            .with_peers(vec!["127.0.0.1:9001".into()]);

        assert_eq!(config.listen_addr, "127.0.0.1:9000");
        assert_eq!(config.initial_peers.len(), 1);
        assert_eq!(config.max_peers, DEFAULT_MAX_PEERS);
        assert_eq!(config.max_message_size, DEFAULT_MAX_MESSAGE_SIZE);
        assert_eq!(config.socket_timeout, DEFAULT_SOCKET_TIMEOUT);
    }

    #[test]
    fn node_config_allows_custom_peer_limit() {
        let config = NodeConfig::new(NodeId::new("test"), "127.0.0.1:9000").with_max_peers(2);

        assert_eq!(config.max_peers, 2);
    }

    #[test]
    fn peer_slot_limit_rejects_new_peer_when_full() {
        let mut peers = HashMap::new();
        peers.insert(
            "127.0.0.1:9001".to_string(),
            PeerConnection {
                info: PeerInfo::new("127.0.0.1:9001"),
                state: PeerState::Connected,
                writer: None,
                _slot: test_slot(),
            },
        );

        let err = ensure_peer_slot_available(&peers, 1, None).unwrap_err();

        assert!(matches!(err, NetError::PeerLimitReached(1)));
    }

    #[test]
    fn peer_slot_limit_allows_replacing_same_addr() {
        let mut peers = HashMap::new();
        peers.insert(
            "127.0.0.1:9001".to_string(),
            PeerConnection {
                info: PeerInfo::new("127.0.0.1:9001"),
                state: PeerState::Connected,
                writer: None,
                _slot: test_slot(),
            },
        );

        ensure_peer_slot_available(&peers, 1, Some("127.0.0.1:9001")).unwrap();
    }

    #[tokio::test]
    async fn mark_peer_failed_returns_updated_connected_count() {
        let node = Node::new(NodeConfig::new(NodeId::new("test"), "127.0.0.1:9000"));
        {
            let mut peers = node.peers.write().await;
            peers.insert(
                "127.0.0.1:9001".to_string(),
                PeerConnection {
                    info: PeerInfo::new("127.0.0.1:9001").with_node_id(NodeId::new("peer-a")),
                    state: PeerState::Connected,
                    writer: None,
                    _slot: test_slot(),
                },
            );
            peers.insert(
                "127.0.0.1:9002".to_string(),
                PeerConnection {
                    info: PeerInfo::new("127.0.0.1:9002").with_node_id(NodeId::new("peer-b")),
                    state: PeerState::Connected,
                    writer: None,
                    _slot: test_slot(),
                },
            );
        }

        let (node_id, connected) = node.mark_peer_failed("127.0.0.1:9001").await.unwrap();

        assert_eq!(node_id, NodeId::new("peer-a"));
        assert_eq!(connected, 1);
    }

    #[test]
    fn node_config_allows_custom_wire_limits() {
        let config = NodeConfig::new(NodeId::new("test"), "127.0.0.1:9000")
            .with_max_message_size(1024)
            .with_socket_timeout(Duration::from_secs(3));

        assert_eq!(config.max_message_size, 1024);
        assert_eq!(config.socket_timeout, Duration::from_secs(3));
    }

    #[tokio::test]
    async fn read_message_rejects_payload_over_configured_limit() {
        let (mut client, mut server) = tokio::io::duplex(64);
        client.write_all(&8u32.to_be_bytes()).await.unwrap();

        let err = read_message(&mut server, 4, DEFAULT_SOCKET_TIMEOUT)
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            NetError::MessageTooLarge { len: 8, limit: 4 }
        ));
    }

    #[tokio::test]
    async fn read_message_times_out_waiting_for_length() {
        let (_client, mut server) = tokio::io::duplex(64);

        let err = read_message(
            &mut server,
            DEFAULT_MAX_MESSAGE_SIZE,
            Duration::from_millis(1),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, NetError::Timeout));
    }

    #[tokio::test]
    async fn incoming_idle_handshake_consumes_peer_slot() {
        let config = NodeConfig::new(NodeId::new("test"), "127.0.0.1:0")
            .with_max_peers(1)
            .with_socket_timeout(Duration::from_secs(5));
        let node = Node::new(config);
        let addr = node.start_listener().await.unwrap();

        let first = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(25)).await;

        assert_eq!(node.connection_slots.available_permits(), 0);
        assert_eq!(node.connected_peer_count().await, 0);

        let second = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(25)).await;

        assert_eq!(node.connection_slots.available_permits(), 0);
        assert_eq!(node.connected_peer_count().await, 0);

        drop(second);
        drop(first);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn active_peer_shutdown_does_not_wait_for_socket_timeout() {
        let node_a = Node::new(
            NodeConfig::new(NodeId::new("node-a"), "127.0.0.1:0")
                .with_socket_timeout(Duration::from_secs(5)),
        );
        let addr_a = node_a.start_listener().await.unwrap();

        let mut node_b = Node::new(
            NodeConfig::new(NodeId::new("node-b"), "127.0.0.1:0")
                .with_socket_timeout(Duration::from_secs(5)),
        );
        let mut events_b = node_b.take_event_receiver().unwrap();
        node_b.connect_to_peer(&addr_a.to_string()).await.unwrap();

        let connected = timeout(Duration::from_secs(1), async {
            loop {
                if let Some(NodeEvent::PeerConnected { .. }) = events_b.recv().await {
                    break;
                }
            }
        })
        .await;
        assert!(connected.is_ok(), "peer did not connect");

        let started = std::time::Instant::now();
        node_b.shutdown().await;

        assert!(
            started.elapsed() < Duration::from_secs(1),
            "shutdown waited for socket timeout instead of cooperative signal"
        );

        let disconnected = timeout(Duration::from_secs(1), async {
            loop {
                if let Some(NodeEvent::PeerDisconnected { .. }) = events_b.recv().await {
                    break;
                }
            }
        })
        .await;
        assert!(
            disconnected.is_ok(),
            "peer disconnect event was not emitted"
        );

        node_a.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "stress test: opens 1000 local TCP connections"]
    async fn thousand_simultaneous_connections_do_not_crash() {
        let config = NodeConfig::new(NodeId::new("stress-node"), "127.0.0.1:0")
            .with_max_peers(DEFAULT_MAX_PEERS)
            .with_socket_timeout(Duration::from_millis(100));
        let node = Node::new(config);
        let addr = node.start_listener().await.unwrap();

        let mut tasks = Vec::with_capacity(1000);
        for _ in 0..1000 {
            tasks.push(tokio::spawn(async move { TcpStream::connect(addr).await }));
        }

        let mut connected = 0usize;
        for task in tasks {
            if task.await.unwrap().is_ok() {
                connected += 1;
            }
        }

        assert!(connected > 0, "stress test did not open any connection");
        node.shutdown().await;
    }
}
