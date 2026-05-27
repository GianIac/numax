use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use nx_sync::{NodeId, Op};
use tokio::io::{AsyncReadExt, AsyncWriteExt, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, OwnedSemaphorePermit, RwLock, Semaphore, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use crate::error::{NetError, NetResult};
use crate::message::{
    DEFAULT_SUPPORTED_FORMATS, Message, MessageKind, PROTOCOL_VERSION, SerializationFormat,
};
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

type PeerWriter = Arc<Mutex<WriteHalf<NetStream>>>;

#[derive(Debug, Clone, Copy)]
struct NodeLimits {
    max_peers: usize,
    max_message_size: usize,
    socket_timeout: Duration,
    serialization_format: SerializationFormat,
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

struct ReadLoopContext {
    peer_node_id: NodeId,
    addr: String,
    event_tx: mpsc::Sender<NodeEvent>,
    writer: PeerWriter,
    serialization_format: SerializationFormat,
    max_message_size: usize,
    socket_timeout: Duration,
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

    /// Serialization format used for outgoing messages.
    pub serialization_format: SerializationFormat,
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
            serialization_format: SerializationFormat::Bincode,
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

    pub fn with_serialization_format(mut self, serialization_format: SerializationFormat) -> Self {
        self.serialization_format = serialization_format;
        self
    }
}

/// Node exit event (for runtime).
#[derive(Debug, Clone)]
pub enum NodeEvent {
    /// received new operations from a peer.
    OpsReceived { from: NodeId, ops: Vec<Op> },

    /// Peer requested operations from a known point.
    PullRequested {
        from: NodeId,
        addr: String,
        since_op_id: Option<String>,
    },

    /// Peer connected.
    PeerConnected {
        node_id: NodeId,
        addr: String,
        peers_connected: usize,
    },

    /// Peer disconnected.
    PeerDisconnected {
        node_id: NodeId,
        addr: String,
        peers_connected: usize,
    },
}

#[allow(dead_code)]
/// internal state for each peer connection.
struct PeerConnection {
    info: PeerInfo,
    state: PeerState,
    serialization_format: SerializationFormat,
    writer: Option<PeerWriter>,
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
            serialization_format: self.config.serialization_format,
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

        let tcp = timeout(self.config.socket_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| NetError::Timeout)?
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
        let supported_formats = supported_formats_for(self.config.serialization_format);
        let hello = Message::hello_with_formats(
            self.config.node_id.clone(),
            supported_formats,
            self.config.serialization_format,
        );
        write_message(
            &mut writer,
            &hello,
            self.config.serialization_format,
            self.config.socket_timeout,
        )
        .await?;

        // Wait HELLO_ACK
        let response = read_message(
            &mut reader,
            self.config.max_message_size,
            self.config.socket_timeout,
        )
        .await?;
        let (peer_node_id, negotiated_format) = match response.kind {
            MessageKind::HelloAck {
                node_id,
                version,
                selected_format,
            } => {
                if version != PROTOCOL_VERSION {
                    return Err(protocol_version_mismatch(version));
                }
                if !supported_formats_for(self.config.serialization_format)
                    .contains(&selected_format)
                {
                    return Err(NetError::InvalidMessage(format!(
                        "peer selected unsupported serialization format: {selected_format:?}"
                    )));
                }
                info!(peer = %node_id, serialization_format = ?selected_format, "handshake complete");
                (node_id, selected_format)
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
        let writer = Arc::new(Mutex::new(writer));
        let peers_connected = {
            let mut peers = self.peers.write().await;
            ensure_peer_slot_available(&peers, self.config.max_peers, Some(addr))?;
            peers.insert(
                addr.to_string(),
                PeerConnection {
                    info: PeerInfo::new(addr).with_node_id(peer_node_id.clone()),
                    state: PeerState::Connected,
                    serialization_format: negotiated_format,
                    writer: Some(Arc::clone(&writer)),
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
                addr: addr.to_string(),
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
                ReadLoopContext {
                    peer_node_id: peer_node_id.clone(),
                    addr: addr_owned.clone(),
                    event_tx: event_tx.clone(),
                    writer,
                    serialization_format: negotiated_format,
                    max_message_size,
                    socket_timeout,
                    shutdown_rx,
                },
            )
            .await
            {
                debug!(peer = %peer_node_id, error = %e, "read loop ended");
            }

            // Cleanup
            let disconnected = {
                let mut peers = peers.write().await;
                peers.remove(&addr_owned).and_then(|removed| {
                    (removed.state == PeerState::Connected).then(|| {
                        (
                            peer_node_id.clone(),
                            addr_owned.clone(),
                            connected_peer_count(&peers),
                        )
                    })
                })
            };

            if let Some((node_id, addr, peers_connected)) = disconnected {
                let _ = event_tx
                    .send(NodeEvent::PeerDisconnected {
                        node_id,
                        addr,
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
        self.broadcast_message(Message::push_ops(ops)).await
    }

    /// Send ops to a specific connected peer.
    pub async fn send_ops_to_addr(&self, addr: &str, ops: Vec<Op>) -> NetResult<()> {
        self.send_message_to_addr(addr, Message::push_ops(ops))
            .await
    }

    /// Request ops from a specific connected peer.
    pub async fn send_pull_since_to_addr(
        &self,
        addr: &str,
        since_op_id: Option<String>,
    ) -> NetResult<()> {
        self.send_message_to_addr(addr, Message::pull_since(since_op_id))
            .await
    }

    async fn broadcast_message(&self, msg: Message) -> NetResult<()> {
        let writers = {
            let peers = self.peers.read().await;
            peers
                .iter()
                .filter_map(|(addr, conn)| {
                    (conn.state == PeerState::Connected)
                        .then(|| {
                            conn.writer.as_ref().map(|writer| {
                                (addr.clone(), Arc::clone(writer), conn.serialization_format)
                            })
                        })
                        .flatten()
                })
                .collect::<Vec<_>>()
        };

        let mut failed = Vec::new();
        for (addr, writer, serialization_format) in writers {
            let bytes = msg.to_bytes_with_format(serialization_format)?;
            let mut writer = writer.lock().await;
            if let Err(e) = write_bytes(&mut *writer, &bytes, self.config.socket_timeout).await {
                warn!(%addr, error = %e, "failed to send ops");
                failed.push(addr.clone());
                if let Some((node_id, peers_connected)) = self.mark_peer_failed(&addr).await {
                    let _ = self
                        .event_tx
                        .send(NodeEvent::PeerDisconnected {
                            node_id,
                            addr: addr.clone(),
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

    async fn send_message_to_addr(&self, addr: &str, msg: Message) -> NetResult<()> {
        let peer_writer = {
            let peers = self.peers.read().await;
            peers.get(addr).and_then(|conn| {
                (conn.state == PeerState::Connected)
                    .then(|| {
                        conn.writer
                            .as_ref()
                            .map(|writer| (Arc::clone(writer), conn.serialization_format))
                    })
                    .flatten()
            })
        };

        let Some((writer, serialization_format)) = peer_writer else {
            return Err(NetError::PeerDisconnected(addr.to_string()));
        };

        let bytes = msg.to_bytes_with_format(serialization_format)?;
        let mut writer = writer.lock().await;
        if let Err(e) = write_bytes(&mut *writer, &bytes, self.config.socket_timeout).await {
            warn!(%addr, error = %e, "failed to send message to peer");
            if let Some((node_id, peers_connected)) = self.mark_peer_failed(addr).await {
                let _ = self
                    .event_tx
                    .send(NodeEvent::PeerDisconnected {
                        node_id,
                        addr: addr.to_string(),
                        peers_connected,
                    })
                    .await;
            }
            return Err(e);
        }

        Ok(())
    }

    /// Returns the number of currently connected peers.
    pub async fn connected_peer_count(&self) -> usize {
        let peers = self.peers.read().await;
        connected_peer_count(&peers)
    }

    /// Returns true when the configured peer address currently has an active connection.
    pub async fn is_connected_addr(&self, addr: &str) -> bool {
        let peers = self.peers.read().await;
        peers
            .get(addr)
            .is_some_and(|conn| conn.state == PeerState::Connected)
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
    let (peer_node_id, negotiated_format) = match msg.kind {
        MessageKind::Hello {
            node_id,
            version,
            supported_formats,
            preferred_format,
        } => {
            if version != PROTOCOL_VERSION {
                return Err(protocol_version_mismatch(version));
            }
            let negotiated_format =
                negotiate_serialization_format(limits.serialization_format, &supported_formats)
                    .ok_or_else(|| {
                        NetError::InvalidMessage(
                            "no mutually supported serialization format".to_string(),
                        )
                    })?;
            if negotiated_format != preferred_format {
                debug!(
                    peer = %node_id,
                    peer_preferred_format = ?preferred_format,
                    selected_format = ?negotiated_format,
                    "selected alternate serialization format"
                );
            }
            (node_id, negotiated_format)
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

    let ack = Message::hello_ack_with_format(our_node_id, negotiated_format);
    write_message(&mut writer, &ack, negotiated_format, limits.socket_timeout).await?;

    let writer = Arc::new(Mutex::new(writer));
    let peers_connected = {
        let mut peers = peers.write().await;
        ensure_peer_slot_available(&peers, limits.max_peers, Some(&addr))?;
        peers.insert(
            addr.clone(),
            PeerConnection {
                info: PeerInfo::new(&addr).with_node_id(peer_node_id.clone()),
                state: PeerState::Connected,
                serialization_format: negotiated_format,
                writer: Some(Arc::clone(&writer)),
                _slot: slot,
            },
        );
        connected_peer_count(&peers)
    };

    info!(peer = %peer_node_id, serialization_format = ?negotiated_format, "incoming peer connected");

    // Notify Event
    let _ = event_tx
        .send(NodeEvent::PeerConnected {
            node_id: peer_node_id.clone(),
            addr: addr.clone(),
            peers_connected,
        })
        .await;

    // Read Loop
    let read_result = read_loop(
        reader,
        ReadLoopContext {
            peer_node_id: peer_node_id.clone(),
            addr: addr.clone(),
            event_tx: event_tx.clone(),
            writer,
            serialization_format: negotiated_format,
            max_message_size: limits.max_message_size,
            socket_timeout: limits.socket_timeout,
            shutdown_rx,
        },
    )
    .await;

    let disconnected = {
        let mut peers = peers.write().await;
        let Some(removed) = peers.remove(&addr) else {
            return read_result;
        };
        (removed.state == PeerState::Connected).then(|| {
            (
                peer_node_id.clone(),
                addr.clone(),
                connected_peer_count(&peers),
            )
        })
    };

    if let Some((node_id, addr, peers_connected)) = disconnected {
        let _ = event_tx
            .send(NodeEvent::PeerDisconnected {
                node_id,
                addr,
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

fn supported_formats_for(preferred: SerializationFormat) -> Vec<SerializationFormat> {
    match preferred {
        SerializationFormat::Json => vec![SerializationFormat::Json],
        SerializationFormat::Bincode => DEFAULT_SUPPORTED_FORMATS.to_vec(),
    }
}

fn negotiate_serialization_format(
    preferred: SerializationFormat,
    peer_supported: &[SerializationFormat],
) -> Option<SerializationFormat> {
    let local_supported = supported_formats_for(preferred);
    if peer_supported.contains(&preferred) && local_supported.contains(&preferred) {
        return Some(preferred);
    }
    local_supported
        .into_iter()
        .find(|format| peer_supported.contains(format))
}

fn protocol_version_mismatch(their_version: u32) -> NetError {
    NetError::InvalidMessage(format!(
        "protocol version mismatch: expected {PROTOCOL_VERSION}, got {their_version}"
    ))
}

/// Loop for reading messages from a peer until disconnection
async fn read_loop(
    mut reader: tokio::io::ReadHalf<NetStream>,
    context: ReadLoopContext,
) -> NetResult<()> {
    let ReadLoopContext {
        peer_node_id,
        addr,
        event_tx,
        writer,
        serialization_format,
        max_message_size,
        socket_timeout,
        mut shutdown_rx,
    } = context;

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
            MessageKind::PullSince { since_op_id } => {
                debug!(peer = %peer_node_id, addr = %addr, ?since_op_id, "received pull request");
                let _ = event_tx
                    .send(NodeEvent::PullRequested {
                        from: peer_node_id.clone(),
                        addr: addr.clone(),
                        since_op_id,
                    })
                    .await;
            }
            MessageKind::Ping => {
                debug!(peer = %peer_node_id, "received ping");
                let mut writer = writer.lock().await;
                write_message(
                    &mut *writer,
                    &Message::pong(),
                    serialization_format,
                    socket_timeout,
                )
                .await?;
            }
            MessageKind::Pong => {
                debug!(peer = %peer_node_id, "received pong");
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
    serialization_format: SerializationFormat,
    socket_timeout: Duration,
) -> NetResult<()> {
    let bytes = msg.to_bytes_with_format(serialization_format)?;
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

    Message::from_bytes(&buf)
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
    fn negotiation_prefers_local_format_when_peer_supports_it() {
        let selected = negotiate_serialization_format(
            SerializationFormat::Bincode,
            &[SerializationFormat::Json, SerializationFormat::Bincode],
        );

        assert_eq!(selected, Some(SerializationFormat::Bincode));
    }

    #[test]
    fn negotiation_falls_back_to_json_for_debug_peer() {
        let selected = negotiate_serialization_format(
            SerializationFormat::Bincode,
            &[SerializationFormat::Json],
        );

        assert_eq!(selected, Some(SerializationFormat::Json));
    }

    #[test]
    fn negotiation_rejects_empty_peer_formats() {
        let selected = negotiate_serialization_format(SerializationFormat::Bincode, &[]);

        assert_eq!(selected, None);
    }

    #[test]
    fn peer_slot_limit_rejects_new_peer_when_full() {
        let mut peers = HashMap::new();
        peers.insert(
            "127.0.0.1:9001".to_string(),
            PeerConnection {
                info: PeerInfo::new("127.0.0.1:9001"),
                state: PeerState::Connected,
                serialization_format: SerializationFormat::Bincode,
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
                serialization_format: SerializationFormat::Bincode,
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
                    serialization_format: SerializationFormat::Bincode,
                    writer: None,
                    _slot: test_slot(),
                },
            );
            peers.insert(
                "127.0.0.1:9002".to_string(),
                PeerConnection {
                    info: PeerInfo::new("127.0.0.1:9002").with_node_id(NodeId::new("peer-b")),
                    state: PeerState::Connected,
                    serialization_format: SerializationFormat::Bincode,
                    writer: None,
                    _slot: test_slot(),
                },
            );
        }

        let (node_id, connected) = node.mark_peer_failed("127.0.0.1:9001").await.unwrap();

        assert_eq!(node_id, NodeId::new("peer-a"));
        assert_eq!(connected, 1);
    }

    #[tokio::test]
    async fn is_connected_addr_tracks_connected_state() {
        let node = Node::new(NodeConfig::new(NodeId::new("test"), "127.0.0.1:9000"));
        {
            let mut peers = node.peers.write().await;
            peers.insert(
                "127.0.0.1:9001".to_string(),
                PeerConnection {
                    info: PeerInfo::new("127.0.0.1:9001").with_node_id(NodeId::new("peer-a")),
                    state: PeerState::Connected,
                    serialization_format: SerializationFormat::Bincode,
                    writer: None,
                    _slot: test_slot(),
                },
            );
            peers.insert(
                "127.0.0.1:9002".to_string(),
                PeerConnection {
                    info: PeerInfo::new("127.0.0.1:9002").with_node_id(NodeId::new("peer-b")),
                    state: PeerState::Failed,
                    serialization_format: SerializationFormat::Bincode,
                    writer: None,
                    _slot: test_slot(),
                },
            );
        }

        assert!(node.is_connected_addr("127.0.0.1:9001").await);
        assert!(!node.is_connected_addr("127.0.0.1:9002").await);
        assert!(!node.is_connected_addr("127.0.0.1:9003").await);
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
    async fn connect_to_peer_times_out_during_handshake() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept_task = tokio::spawn(async move {
            let (_stream, _addr) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let node = Node::new(
            NodeConfig::new(NodeId::new("test"), "127.0.0.1:0")
                .with_socket_timeout(Duration::from_millis(10)),
        );

        let err = node.connect_to_peer(&addr.to_string()).await.unwrap_err();

        assert!(matches!(err, NetError::Timeout));
        accept_task.await.unwrap();
    }

    #[tokio::test]
    async fn connect_to_peer_rejects_protocol_version_mismatch() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let accept_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = NetStream::Plain(stream);
            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).await.unwrap();
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            stream.read_exact(&mut buf).await.unwrap();

            let ack = Message {
                kind: MessageKind::HelloAck {
                    node_id: NodeId::new("old-peer"),
                    version: PROTOCOL_VERSION - 1,
                    selected_format: SerializationFormat::Bincode,
                },
            };
            let bytes = ack.to_bytes().unwrap();
            stream.write_all(&bytes).await.unwrap();
            stream.flush().await.unwrap();
        });

        let node = Node::new(
            NodeConfig::new(NodeId::new("test"), "127.0.0.1:0")
                .with_socket_timeout(Duration::from_secs(1)),
        );

        let err = node.connect_to_peer(&addr.to_string()).await.unwrap_err();

        assert!(
            matches!(err, NetError::InvalidMessage(msg) if msg.contains("protocol version mismatch"))
        );
        accept_task.await.unwrap();
    }

    #[tokio::test]
    async fn incoming_rejects_protocol_version_mismatch() {
        let mut node = Node::new(
            NodeConfig::new(NodeId::new("node-a"), "127.0.0.1:0")
                .with_socket_timeout(Duration::from_secs(1)),
        );
        let mut events = node.take_event_receiver().unwrap();
        let addr = node.start_listener().await.unwrap();

        let mut stream = TcpStream::connect(addr).await.unwrap();
        let hello = Message {
            kind: MessageKind::Hello {
                node_id: NodeId::new("old-peer"),
                version: PROTOCOL_VERSION - 1,
                supported_formats: vec![SerializationFormat::Bincode, SerializationFormat::Json],
                preferred_format: SerializationFormat::Bincode,
            },
        };
        let bytes = hello.to_bytes().unwrap();
        stream.write_all(&bytes).await.unwrap();
        stream.flush().await.unwrap();

        let event = timeout(Duration::from_millis(200), events.recv()).await;
        assert!(event.is_err(), "old protocol peer must not connect");

        node.shutdown().await;
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

    #[tokio::test]
    async fn incoming_ping_gets_pong_response() {
        let node = Node::new(
            NodeConfig::new(NodeId::new("node-a"), "127.0.0.1:0")
                .with_socket_timeout(Duration::from_secs(1)),
        );
        let addr = node.start_listener().await.unwrap();

        let mut stream = NetStream::Plain(TcpStream::connect(addr).await.unwrap());
        let hello = Message::hello(NodeId::new("peer-a"));
        let bytes = hello.to_bytes().unwrap();
        stream.write_all(&bytes).await.unwrap();
        stream.flush().await.unwrap();

        let ack = read_message(
            &mut stream,
            DEFAULT_MAX_MESSAGE_SIZE,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        let negotiated_format = match ack.kind {
            MessageKind::HelloAck {
                selected_format, ..
            } => selected_format,
            other => panic!("expected HelloAck, got {other:?}"),
        };

        let ping = Message::ping()
            .to_bytes_with_format(negotiated_format)
            .unwrap();
        stream.write_all(&ping).await.unwrap();
        stream.flush().await.unwrap();

        let pong = read_message(
            &mut stream,
            DEFAULT_MAX_MESSAGE_SIZE,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert!(matches!(pong.kind, MessageKind::Pong));

        node.shutdown().await;
    }

    #[tokio::test]
    async fn bincode_node_negotiates_json_with_debug_peer() {
        let node_a = Node::new(NodeConfig::new(NodeId::new("node-a"), "127.0.0.1:0"));
        node_a.start_listener().await.unwrap();

        let mut node_b = Node::new(
            NodeConfig::new(NodeId::new("node-b"), "127.0.0.1:0")
                .with_serialization_format(SerializationFormat::Json),
        );
        let mut events_b = node_b.take_event_receiver().unwrap();
        let addr_b = node_b.start_listener().await.unwrap();

        node_a.connect_to_peer(&addr_b.to_string()).await.unwrap();

        timeout(Duration::from_secs(1), async {
            loop {
                if matches!(events_b.recv().await, Some(NodeEvent::PeerConnected { .. })) {
                    break;
                }
            }
        })
        .await
        .unwrap();

        let peers = node_a.peers.read().await;
        let peer = peers.get(&addr_b.to_string()).expect("connected peer");
        assert_eq!(peer.serialization_format, SerializationFormat::Json);

        drop(peers);
        node_a.shutdown().await;
        node_b.shutdown().await;
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
