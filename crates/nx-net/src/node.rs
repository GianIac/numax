use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::Duration;

use nx_sync::{NodeId, Op};
use tokio::io::{AsyncReadExt, AsyncWriteExt, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, OwnedSemaphorePermit, RwLock, Semaphore, mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use crate::bootstrap::{BootstrapServer, BootstrapServerConfig};
use crate::error::{BootstrapResult, NetError, NetResult, NodeConfigError};
use crate::message::{
    DEFAULT_SUPPORTED_FORMATS, Message, MessageKind, PROTOCOL_VERSION, ProtocolWireError,
    SerializationFormat, WireError, WireMessage, WireMessageKind, validate_payload_len,
};
use crate::peer::{
    ConnectionDirection, PeerConnectionInfo, PeerIdentity, PeerIdentityVerification, PeerInfo,
    PeerState,
};
use crate::tls::{NetStream, TlsConfig};

/// Default maximum number of simultaneously connected peers.
pub const DEFAULT_MAX_PEERS: usize = 64;

/// Default maximum accepted wire message size.
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Default timeout for socket reads and writes.
pub const DEFAULT_SOCKET_TIMEOUT: Duration = Duration::from_secs(30);

/// Default number of node events buffered for the runtime event loop.
pub const DEFAULT_EVENT_CHANNEL_CAPACITY: usize = 1024;

/// Time allowed for network tasks to finish cooperatively after shutdown.
const TASK_SHUTDOWN_GRACE: Duration = Duration::from_secs(3);
const MAX_CONCURRENT_OUTBOUND_ATTEMPTS: usize = 1;

type PeerWriter = Arc<Mutex<WriteHalf<NetStream>>>;

/// Generation identity and cancellation shared by the reader and write snapshots.
struct ConnectionInstance {
    created_at: std::time::Instant,
    closed_tx: watch::Sender<bool>,
}

/// Opaque identity of one data connection. Retaining it does not retain its socket.
#[derive(Clone)]
pub struct ConnectionGeneration(Arc<ConnectionInstance>);

impl PartialEq for ConnectionGeneration {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for ConnectionGeneration {}
impl ConnectionGeneration {
    pub fn created_at(&self) -> std::time::Instant {
        self.0.created_at
    }
    pub fn is_closed(&self) -> bool {
        *self.0.closed_tx.borrow()
    }
}

/// One encoded History frame tied to its writer generation, ready for rate admission.
pub struct PreparedHistory {
    owner: Weak<StdMutex<TaskRegistry>>,
    addr: String,
    peer_id: NodeId,
    writer: Weak<Mutex<WriteHalf<NetStream>>>,
    generation: ConnectionGeneration,
    bytes: Vec<u8>,
}
impl PreparedHistory {
    pub fn encoded_len(&self) -> usize {
        self.bytes.len()
    }
    pub fn generation(&self) -> ConnectionGeneration {
        self.generation.clone()
    }
    pub fn peer_id(&self) -> &NodeId {
        &self.peer_id
    }
}

/// Correlated control request/response time, excluding connect and TLS setup.
/// Includes request writing, remote service and response decoding; not TCP RTT.
#[derive(Debug, Clone, Copy)]
pub struct ControlTiming {
    pub started_at: std::time::Instant,
    pub round_trip: Duration,
    pub observed_at: std::time::Instant,
}

impl ConnectionInstance {
    fn new() -> Self {
        Self {
            created_at: std::time::Instant::now(),
            closed_tx: watch::channel(false).0,
        }
    }
}

async fn wait_for_stop(mut receiver: watch::Receiver<bool>) {
    // Do not let a watch guard escape into select! branch outputs (it is not Send).
    let _ = receiver.wait_for(|stopped| *stopped).await;
}

async fn while_connection_open<T>(
    closed_rx: &watch::Receiver<bool>,
    shutdown_rx: &watch::Receiver<bool>,
    work: impl Future<Output = T>,
) -> Option<T> {
    tokio::select! {
        biased;
        _ = wait_for_stop(closed_rx.clone()) => None,
        _ = wait_for_stop(shutdown_rx.clone()) => None,
        result = work => Some(result),
    }
}

#[derive(Default)]
struct TaskRegistry {
    closed: bool,
    tasks: JoinSet<()>,
}

impl TaskRegistry {
    fn spawn(&mut self, task: impl Future<Output = ()> + Send + 'static) -> NetResult<()> {
        if self.closed {
            return Err(NetError::ConnectionFailed("node is shut down".into()));
        }
        while self.tasks.try_join_next().is_some() {}
        // Admission and spawn share the shutdown lock: no untracked task can escape.
        self.tasks.spawn(task);
        Ok(())
    }
}

fn spawn_task(
    registry: &StdMutex<TaskRegistry>,
    task: impl Future<Output = ()> + Send + 'static,
) -> NetResult<()> {
    registry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .spawn(task)
}

/// Owns the drained tasks across awaits. Cancellation aborts them without detaching
/// their handles, so a subsequent shutdown can still join every owned task.
struct ShutdownTasks<'a> {
    registry: &'a StdMutex<TaskRegistry>,
    tasks: JoinSet<()>,
}

impl<'a> ShutdownTasks<'a> {
    fn close(registry: &'a StdMutex<TaskRegistry>) -> Self {
        let mut state = registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.closed = true;
        Self {
            registry,
            tasks: std::mem::take(&mut state.tasks),
        }
    }

    async fn join(&mut self, grace: Duration) {
        if timeout(grace, async {
            while let Some(result) = self.tasks.join_next().await {
                if let Err(error) = result {
                    debug!(%error, "network task ended during shutdown");
                }
            }
        })
        .await
        .is_err()
        {
            warn!("network tasks did not finish cooperatively; aborting");
            self.tasks.abort_all();
            while self.tasks.join_next().await.is_some() {}
        }
    }
}

impl Drop for ShutdownTasks<'_> {
    fn drop(&mut self) {
        self.tasks.abort_all();
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Shutdown callers are serialized and a closed registry rejects all spawns.
        registry.tasks = std::mem::take(&mut self.tasks);
    }
}

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
    connection_slots: Arc<Semaphore>,
    control: Arc<crate::control::ControlService>,
    shutdown_rx: watch::Receiver<bool>,
    bootstrap_server: Option<Arc<BootstrapServer>>,
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
    closed_rx: watch::Receiver<bool>,
}

enum IncomingHandshake {
    Peer {
        node_id: NodeId,
        format: SerializationFormat,
    },
    Bootstrap {
        node_id: NodeId,
        format: SerializationFormat,
        cluster_id: String,
        advertised_endpoint: Option<String>,
        max_results: usize,
    },
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

    /// Number of node events buffered for the runtime event loop.
    pub event_channel_capacity: usize,
}

impl NodeConfig {
    /// Check limits before allocating channels or starting network tasks.
    /// Zero peers disables admission; event capacity and socket timeout must be positive.
    pub fn validate(&self) -> Result<(), NodeConfigError> {
        if self.max_peers > Semaphore::MAX_PERMITS {
            return Err(NodeConfigError::MaxPeersTooLarge {
                limit: Semaphore::MAX_PERMITS,
            });
        }
        if self.event_channel_capacity == 0 || self.event_channel_capacity > Semaphore::MAX_PERMITS
        {
            return Err(NodeConfigError::InvalidEventChannelCapacity {
                limit: Semaphore::MAX_PERMITS,
            });
        }
        if self.socket_timeout.is_zero()
            || std::time::Instant::now()
                .checked_add(self.socket_timeout)
                .is_none()
        {
            return Err(NodeConfigError::InvalidSocketTimeout);
        }
        Ok(())
    }

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
            event_channel_capacity: DEFAULT_EVENT_CHANNEL_CAPACITY,
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

    pub fn with_event_channel_capacity(mut self, event_channel_capacity: usize) -> Self {
        self.event_channel_capacity = event_channel_capacity;
        self
    }
}

/// Node exit event (for runtime).
#[derive(Debug, Clone)]
pub enum NodeEvent {
    /// Protocol-7 sequenced replication and history exchange.
    HistoryReceived {
        from: NodeId,
        addr: String,
        message: nx_sync::HistoryMessage,
    },
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

/// Internal state for each peer connection.
///
/// `_slot` intentionally holds the semaphore permit for the lifetime of the
/// connection; dropping the connection releases capacity.
struct PeerConnection {
    info: PeerInfo,
    connection_info: Option<PeerConnectionInfo>,
    instance: Arc<ConnectionInstance>,
    state: PeerState,
    serialization_format: SerializationFormat,
    writer: Option<PeerWriter>,
    _slot: OwnedSemaphorePermit,
}

impl Drop for PeerConnection {
    fn drop(&mut self) {
        // Removal/replacement cancels every user of this generation, even if the
        // reader is sending an event or waiting for the writer mutex.
        self.instance.closed_tx.send_replace(true);
    }
}

struct ConnectionAttemptGuard {
    endpoint: String,
    attempts: Arc<StdMutex<HashSet<String>>>,
}

impl ConnectionAttemptGuard {
    fn acquire(endpoint: &str, attempts: Arc<StdMutex<HashSet<String>>>) -> NetResult<Self> {
        let mut active = attempts.lock().map_err(|_| {
            NetError::ConnectionFailed("outbound attempt registry is poisoned".to_string())
        })?;
        if !active.insert(endpoint.to_string()) {
            return Err(NetError::ConnectionFailed(format!(
                "connection attempt already in progress for peer: {endpoint}"
            )));
        }
        drop(active);
        Ok(Self {
            endpoint: endpoint.to_string(),
            attempts,
        })
    }
}

impl Drop for ConnectionAttemptGuard {
    fn drop(&mut self) {
        if let Ok(mut active) = self.attempts.lock() {
            active.remove(&self.endpoint);
        }
    }
}

/// node
pub struct Node {
    config: NodeConfig,
    peers: Arc<RwLock<HashMap<String, PeerConnection>>>,
    event_tx: mpsc::Sender<NodeEvent>,
    event_rx: Option<mpsc::Receiver<NodeEvent>>,
    shutdown_tx: watch::Sender<bool>,
    connection_slots: Arc<Semaphore>,
    outbound_attempt_slots: Arc<Semaphore>,
    outbound_attempts: Arc<StdMutex<HashSet<String>>>,
    tasks: Arc<StdMutex<TaskRegistry>>,
    shutdown_lock: Mutex<()>,
    bootstrap_server: Option<Arc<BootstrapServer>>,
    control: Arc<crate::control::ControlService>,
    control_rx: Option<mpsc::Receiver<crate::ControlIncoming>>,
    handshake_slots: Arc<Semaphore>,
}

impl Node {
    /// Create a node using the legacy infallible constructor.
    pub fn new(config: NodeConfig) -> Self {
        let event_channel_capacity = config.event_channel_capacity.max(1);
        let (event_tx, event_rx) = mpsc::channel(event_channel_capacity);
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let max_peers = config.max_peers;
        let (control, control_rx) = crate::control::ControlService::new(max_peers);

        Self {
            config,
            peers: Arc::new(RwLock::new(HashMap::new())),
            event_tx,
            event_rx: Some(event_rx),
            shutdown_tx,
            connection_slots: Arc::new(Semaphore::new(max_peers)),
            outbound_attempt_slots: Arc::new(Semaphore::new(MAX_CONCURRENT_OUTBOUND_ATTEMPTS)),
            outbound_attempts: Arc::new(StdMutex::new(HashSet::new())),
            tasks: Arc::new(StdMutex::new(TaskRegistry::default())),
            shutdown_lock: Mutex::new(()),
            bootstrap_server: None,
            control: Arc::new(control),
            control_rx: Some(control_rx),
            handshake_slots: Arc::new(Semaphore::new(crate::MAX_CONTROL_REQUESTS)),
        }
    }

    /// Validate configuration and create a node, without binding any sockets.
    pub fn try_new(config: NodeConfig) -> Result<Self, NodeConfigError> {
        config.validate()?;
        Ok(Self::new(config))
    }

    /// Validate node and bootstrap policy, then create a bootstrap-capable node.
    pub fn try_new_with_bootstrap_server(
        config: NodeConfig,
        bootstrap_server: BootstrapServerConfig,
    ) -> BootstrapResult<Self> {
        config.validate()?;
        bootstrap_server.validate()?;
        let mut node = Self::new(config);
        node.bootstrap_server = Some(Arc::new(BootstrapServer::new(bootstrap_server)));
        Ok(node)
    }

    /// Gets the event receiver (can only be called once).
    pub fn take_event_receiver(&mut self) -> Option<mpsc::Receiver<NodeEvent>> {
        self.event_rx.take()
    }

    pub fn take_control_receiver(&mut self) -> Option<mpsc::Receiver<crate::ControlIncoming>> {
        self.control_rx.take()
    }

    pub async fn control_request(
        &self,
        addr: &str,
        request: crate::ControlRequest,
        budget: Duration,
    ) -> NetResult<(NodeId, crate::ControlReply)> {
        self.control_request_tracked(addr, request, budget, None)
            .await
    }

    pub async fn control_request_tracked(
        &self,
        addr: &str,
        request: crate::ControlRequest,
        budget: Duration,
        dispatched: Option<tokio::sync::oneshot::Sender<()>>,
    ) -> NetResult<(NodeId, crate::ControlReply)> {
        self.control_request_timed_tracked(addr, request, budget, dispatched)
            .await
            .map(|(peer, reply, _)| (peer, reply))
    }

    pub async fn control_request_timed_tracked(
        &self,
        addr: &str,
        request: crate::ControlRequest,
        budget: Duration,
        dispatched: Option<tokio::sync::oneshot::Sender<()>>,
    ) -> NetResult<(NodeId, crate::ControlReply, ControlTiming)> {
        let _slot = Arc::clone(&self.control.outgoing)
            .try_acquire_owned()
            .map_err(|_| NetError::PeerLimitReached(crate::MAX_CONTROL_REQUESTS))?;
        let request_id = request.request_id;
        if request.owner.member != self.config.node_id.to_string() || budget.is_zero() {
            return Err(NetError::InvalidMessage(
                "control owner/budget mismatch".into(),
            ));
        }
        let work = async {
            if let Some(dispatched) = dispatched {
                let _ = dispatched.send(());
            }
            let (stream, _) = connect_transport(addr, self.config.tls.as_ref(), budget).await?;
            let certificate = stream.peer_cert_der();
            if self.config.tls.as_ref().is_some_and(|tls| !tls.insecure) {
                let certificate = certificate
                    .as_ref()
                    .ok_or_else(|| NetError::TlsError("missing peer certificate".into()))?;
                let expected = crate::tls::derive_protocol_node_id_from_cert(certificate)?;
                verify_peer_identity(
                    &self.config.node_id,
                    &expected,
                    Some(certificate),
                    self.config.tls.as_ref(),
                )?;
            }
            let (mut reader, mut writer) = tokio::io::split(stream);
            let formats = supported_formats_for(self.config.serialization_format);
            let hello = WireMessage {
                kind: WireMessageKind::ControlHello {
                    node_id: self.config.node_id.clone(),
                    protocol_version: PROTOCOL_VERSION,
                    supported_formats: formats.clone(),
                    preferred_format: self.config.serialization_format,
                    request,
                },
            };
            let started = std::time::Instant::now();
            write_message(
                &mut writer,
                &hello,
                self.config.serialization_format,
                budget,
            )
            .await?;
            let (format, response) = read_wire_message_with_format(
                &mut reader,
                crate::MAX_CONTROL_FRAME.min(self.config.max_message_size),
                budget,
            )
            .await?;
            let observed_at = std::time::Instant::now();
            match response.kind {
                WireMessageKind::ControlAck {
                    node_id,
                    protocol_version,
                    selected_format,
                    reply,
                } => {
                    if !is_protocol_version_compatible(protocol_version) {
                        return Err(protocol_version_mismatch(protocol_version));
                    }
                    verify_peer_identity(
                        &self.config.node_id,
                        &node_id,
                        certificate.as_ref(),
                        self.config.tls.as_ref(),
                    )?;
                    if !formats.contains(&selected_format)
                        || format != selected_format
                        || reply.request_id != request_id
                        || reply.owner.member != node_id.to_string()
                    {
                        return Err(NetError::InvalidMessage(
                            "uncorrelated control response".into(),
                        ));
                    }
                    Ok((
                        node_id,
                        reply,
                        ControlTiming {
                            started_at: started,
                            round_trip: observed_at.saturating_duration_since(started),
                            observed_at,
                        },
                    ))
                }
                WireMessageKind::Error { error } => Err(NetError::Wire(error.try_into()?)),
                _ => Err(NetError::InvalidMessage("expected ControlAck".into())),
            }
        };
        tokio::select! {
            _ = wait_for_stop(self.shutdown_tx.subscribe()) => Err(NetError::ConnectionFailed("node is shut down".into())),
            result = timeout(budget.min(self.config.socket_timeout), work) => result.map_err(|_| NetError::Timeout)?,
        }
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
        let handshake_slots = Arc::clone(&self.handshake_slots);
        let control = Arc::clone(&self.control);
        let tasks = Arc::clone(&self.tasks);
        let limits = NodeLimits {
            max_peers: self.config.max_peers,
            max_message_size: self.config.max_message_size,
            socket_timeout: self.config.socket_timeout,
            serialization_format: self.config.serialization_format,
        };
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let shutdown_tx = self.shutdown_tx.clone();
        let bootstrap_server = self.bootstrap_server.clone();

        spawn_task(&self.tasks, async move {
            loop {
                if *shutdown_rx.borrow() {
                    break;
                }
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
                                let slot = match Arc::clone(&handshake_slots).try_acquire_owned() {
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
                                let bootstrap_server = bootstrap_server.clone();
                                let connection_slots = Arc::clone(&connection_slots);
                                let control = Arc::clone(&control);

                                let admitted = spawn_task(&tasks, async move {
                                    let context = IncomingContext {
                                        tls,
                                        our_node_id: node_id,
                                        peers,
                                        event_tx,
                                        limits,
                                        slot,
                                        connection_slots,
                                        control,
                                        shutdown_rx,
                                        bootstrap_server,
                                    };

                                    if let Err(e) =
                                        handle_incoming(stream, addr.to_string(), context).await
                                    {
                                        error!(%addr, error = %e, "connection error");
                                    }
                                });
                                if admitted.is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                error!(error = %e, "accept error");
                            }
                        }
                    }
                }
            }
        })?;

        Ok(bound_addr)
    }

    /// Conncet to a peer
    pub async fn connect_to_peer(&self, addr: &str) -> NetResult<()> {
        if *self.shutdown_tx.borrow() {
            return Err(NetError::ConnectionFailed("node is shut down".into()));
        }
        if self.is_connected_addr(addr).await {
            return Ok(());
        }
        let _attempt = ConnectionAttemptGuard::acquire(addr, Arc::clone(&self.outbound_attempts))?;
        let _attempt_slot = Arc::clone(&self.outbound_attempt_slots)
            .try_acquire_owned()
            .map_err(|_| {
                NetError::ConnectionFailed(format!(
                    "outbound connection attempt limit reached: {MAX_CONCURRENT_OUTBOUND_ATTEMPTS}"
                ))
            })?;
        let slot = Arc::clone(&self.connection_slots)
            .try_acquire_owned()
            .map_err(|_| NetError::PeerLimitReached(self.config.max_peers))?;

        let (stream, transport_addr) =
            connect_transport(addr, self.config.tls.as_ref(), self.config.socket_timeout).await?;

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
                protocol_version,
                selected_format,
            } => {
                if !is_protocol_version_compatible(protocol_version) {
                    return Err(protocol_version_mismatch(protocol_version));
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
            MessageKind::Error { error } => {
                return Err(NetError::Wire(error));
            }
            _ => {
                return Err(NetError::InvalidMessage("expected HelloAck".into()));
            }
        };

        verify_peer_identity(
            &self.config.node_id,
            &peer_node_id,
            peer_cert.as_ref(),
            self.config.tls.as_ref(),
        )?;

        // Save connection
        let writer = Arc::new(Mutex::new(writer));
        let connection_instance = Arc::new(ConnectionInstance::new());
        let mut peer_connections = self.peers.write().await;
        let peers_connected = {
            let peers = &mut *peer_connections;
            if peers
                .get(addr)
                .is_some_and(|connection| connection.state == PeerState::Connected)
            {
                return Ok(());
            }
            ensure_peer_slot_available(peers, self.config.max_peers, Some(addr))?;
            peers.insert(
                addr.to_string(),
                PeerConnection {
                    info: PeerInfo::new(addr).with_node_id(peer_node_id.clone()),
                    connection_info: Some(PeerConnectionInfo {
                        transport_addr,
                        dialed_endpoint: Some(addr.to_string()),
                        direction: ConnectionDirection::Outbound,
                        identity: PeerIdentity {
                            node_id: peer_node_id.clone(),
                            verification: identity_verification(self.config.tls.as_ref()),
                        },
                    }),
                    instance: Arc::clone(&connection_instance),
                    state: PeerState::Connected,
                    serialization_format: negotiated_format,
                    writer: Some(Arc::clone(&writer)),
                    _slot: slot,
                },
            );
            connected_peer_count(peers)
        };

        // No await between inserting the connection and registering its owner.
        // Keep the peer lock until rejected admission has rolled the insertion back.
        let peers = Arc::clone(&self.peers);
        let event_tx = self.event_tx.clone();
        let addr_owned = addr.to_string();
        let max_message_size = self.config.max_message_size;
        let socket_timeout = self.config.socket_timeout;
        let shutdown_rx = self.shutdown_tx.subscribe();
        let shutdown_for_events = shutdown_rx.clone();
        let task_instance = Arc::clone(&connection_instance);
        let (connected_tx, connected_rx) = tokio::sync::oneshot::channel();

        let admitted = spawn_task(&self.tasks, async move {
            let closed_rx = task_instance.closed_tx.subscribe();
            while_connection_open(
                &closed_rx,
                &shutdown_for_events,
                send_node_event(
                    &event_tx,
                    &shutdown_for_events,
                    "PeerConnected",
                    NodeEvent::PeerConnected {
                        node_id: peer_node_id.clone(),
                        addr: addr_owned.clone(),
                        peers_connected,
                    },
                ),
            )
            .await;
            let _ = connected_tx.send(());

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
                    closed_rx,
                },
            )
            .await
            {
                debug!(peer = %peer_node_id, error = %e, "read loop ended");
            }

            // Cleanup
            let disconnected = {
                let mut peers = peers.write().await;
                remove_connection_if_current(&mut peers, &addr_owned, &task_instance).and_then(
                    |removed| {
                        (removed.state == PeerState::Connected).then(|| {
                            (
                                peer_node_id.clone(),
                                addr_owned.clone(),
                                connected_peer_count(&peers),
                            )
                        })
                    },
                )
            };

            if let Some((node_id, addr, peers_connected)) = disconnected {
                send_node_event(
                    &event_tx,
                    &shutdown_for_events,
                    "PeerDisconnected",
                    NodeEvent::PeerDisconnected {
                        node_id,
                        addr,
                        peers_connected,
                    },
                )
                .await;
            }
        });
        if admitted.is_err() {
            remove_connection_if_current(&mut peer_connections, addr, &connection_instance);
        }
        drop(peer_connections);
        admitted?;
        // Preserve event delivery before returning, but keep the read task owned
        // even if the caller cancels while the bounded event queue is full.
        connected_rx.await.map_err(|_| {
            NetError::ConnectionFailed("connection task stopped before announcing the peer".into())
        })
    }

    /// Send protocol-7 history data without changing the legacy public message codec.
    pub async fn broadcast_history(&self, message: nx_sync::HistoryMessage) -> NetResult<()> {
        self.broadcast_message(WireMessage {
            kind: WireMessageKind::History { message },
        })
        .await
    }

    pub async fn send_history_to_addr(
        &self,
        addr: &str,
        message: nx_sync::HistoryMessage,
    ) -> NetResult<()> {
        self.send_message_to_addr(
            addr,
            WireMessage {
                kind: WireMessageKind::History { message },
            },
        )
        .await
    }

    /// Encode once using the active connection's negotiated codec. The size
    /// includes framing, and an oversized frame is refused before rate charging.
    pub async fn prepare_history_to_addr(
        &self,
        addr: &str,
        message: nx_sync::HistoryMessage,
    ) -> NetResult<PreparedHistory> {
        let (writer, serialization_format, generation, peer_id) = {
            let peers = self.peers.read().await;
            let peer = peers
                .get(addr)
                .filter(|peer| peer.state == PeerState::Connected)
                .ok_or_else(|| NetError::PeerDisconnected(addr.into()))?;
            let writer = peer
                .writer
                .as_ref()
                .ok_or_else(|| NetError::PeerDisconnected(addr.into()))?;
            let peer_id = peer
                .info
                .node_id
                .clone()
                .ok_or_else(|| NetError::PeerDisconnected(addr.into()))?;
            (
                Arc::downgrade(writer),
                peer.serialization_format,
                ConnectionGeneration(Arc::clone(&peer.instance)),
                peer_id,
            )
        };
        let bytes = WireMessage {
            kind: WireMessageKind::History { message },
        }
        .to_bytes_with_format(serialization_format)?;
        validate_payload_len(bytes.len().saturating_sub(4), self.config.max_message_size)?;
        Ok(PreparedHistory {
            owner: Arc::downgrade(&self.tasks),
            addr: addr.into(),
            peer_id,
            writer,
            generation,
            bytes,
        })
    }

    /// Send only on the generation used for encoding and rate admission.
    /// Duration is local writer/transport service time, never a round-trip sample.
    pub async fn send_prepared_history(&self, prepared: PreparedHistory) -> NetResult<Duration> {
        if !Weak::ptr_eq(&prepared.owner, &Arc::downgrade(&self.tasks)) {
            return Err(NetError::InvalidMessage(
                "prepared frame belongs to another node".into(),
            ));
        }
        let started = std::time::Instant::now();
        let PreparedHistory {
            addr,
            writer,
            generation,
            bytes,
            ..
        } = prepared;
        if let Err(error) = self
            .write_to_connection(&addr, writer, &generation.0, &bytes)
            .await
        {
            if let Some((node_id, peers_connected)) =
                self.mark_peer_failed(&addr, &generation.0).await
            {
                send_node_event(
                    &self.event_tx,
                    &self.shutdown_tx.subscribe(),
                    "PeerDisconnected",
                    NodeEvent::PeerDisconnected {
                        node_id,
                        addr,
                        peers_connected,
                    },
                )
                .await;
            }
            return Err(error);
        }
        Ok(started.elapsed())
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

    async fn broadcast_message(&self, msg: impl Into<WireMessage>) -> NetResult<()> {
        let msg = msg.into();
        let writers = {
            let peers = self.peers.read().await;
            peers
                .iter()
                .filter_map(|(addr, conn)| {
                    (conn.state == PeerState::Connected)
                        .then(|| {
                            conn.writer.as_ref().map(|writer| {
                                (
                                    addr.clone(),
                                    Arc::downgrade(writer),
                                    conn.serialization_format,
                                    Arc::clone(&conn.instance),
                                )
                            })
                        })
                        .flatten()
                })
                .collect::<Vec<_>>()
        };

        let mut failed = Vec::new();
        for (addr, writer, serialization_format, instance) in writers {
            let bytes = msg.to_bytes_with_format(serialization_format)?;
            if let Err(e) = self
                .write_to_connection(&addr, writer, &instance, &bytes)
                .await
            {
                warn!(%addr, error = %e, "failed to send ops");
                failed.push(addr.clone());
                if let Some((node_id, peers_connected)) =
                    self.mark_peer_failed(&addr, &instance).await
                {
                    let shutdown_for_events = self.shutdown_tx.subscribe();
                    send_node_event(
                        &self.event_tx,
                        &shutdown_for_events,
                        "PeerDisconnected",
                        NodeEvent::PeerDisconnected {
                            node_id,
                            addr: addr.clone(),
                            peers_connected,
                        },
                    )
                    .await;
                }
            }
        }

        if !failed.is_empty() {
            return Err(NetError::PeerDisconnected(failed.join(",")));
        }

        Ok(())
    }

    async fn send_message_to_addr(&self, addr: &str, msg: impl Into<WireMessage>) -> NetResult<()> {
        let msg = msg.into();
        let peer_writer = {
            let peers = self.peers.read().await;
            peers.get(addr).and_then(|conn| {
                (conn.state == PeerState::Connected)
                    .then(|| {
                        conn.writer.as_ref().map(|writer| {
                            (
                                Arc::downgrade(writer),
                                conn.serialization_format,
                                Arc::clone(&conn.instance),
                            )
                        })
                    })
                    .flatten()
            })
        };

        let Some((writer, serialization_format, instance)) = peer_writer else {
            return Err(NetError::PeerDisconnected(addr.to_string()));
        };

        let bytes = msg.to_bytes_with_format(serialization_format)?;
        if let Err(e) = self
            .write_to_connection(addr, writer, &instance, &bytes)
            .await
        {
            warn!(%addr, error = %e, "failed to send message to peer");
            if let Some((node_id, peers_connected)) = self.mark_peer_failed(addr, &instance).await {
                let shutdown_for_events = self.shutdown_tx.subscribe();
                send_node_event(
                    &self.event_tx,
                    &shutdown_for_events,
                    "PeerDisconnected",
                    NodeEvent::PeerDisconnected {
                        node_id,
                        addr: addr.to_string(),
                        peers_connected,
                    },
                )
                .await;
            }
            return Err(e);
        }

        Ok(())
    }

    async fn write_to_connection(
        &self,
        addr: &str,
        writer: Weak<Mutex<WriteHalf<NetStream>>>,
        instance: &ConnectionInstance,
        bytes: &[u8],
    ) -> NetResult<()> {
        let closed_rx = instance.closed_tx.subscribe();
        let shutdown_rx = self.shutdown_tx.subscribe();
        while_connection_open(&closed_rx, &shutdown_rx, async move {
            let writer = writer
                .upgrade()
                .ok_or_else(|| NetError::PeerDisconnected(addr.into()))?;
            let mut writer = writer.lock().await;
            write_bytes(&mut *writer, bytes, self.config.socket_timeout).await
        })
        .await
        .unwrap_or_else(|| Err(NetError::PeerDisconnected(addr.into())))
    }

    /// Returns the number of currently connected peers.
    pub async fn connected_peer_count(&self) -> usize {
        let peers = self.peers.read().await;
        connected_peer_count(&peers)
    }

    /// Snapshot of active peers as `(connection address, handshake NodeId)` pairs.
    /// Sorted by address; identities are certificate-bound only with secure TLS.
    pub async fn connected_peers(&self) -> Vec<(String, NodeId)> {
        let peers = self.peers.read().await;
        let mut connected = peers
            .iter()
            .filter(|(_, connection)| connection.state == PeerState::Connected)
            .filter_map(|(addr, connection)| {
                connection
                    .info
                    .node_id
                    .clone()
                    .map(|node_id| (addr.clone(), node_id))
            })
            .collect::<Vec<_>>();
        connected.sort_by(|(left, _), (right, _)| left.cmp(right));
        connected
    }

    /// Release a topology slot only while the address still belongs to the
    /// expected peer. Removal cancels the reader/writer generation before event
    /// delivery, so a saturated event queue cannot retain the admission permit.
    pub async fn disconnect_peer(&self, addr: &str, expected: &NodeId) -> bool {
        let peers_connected = {
            let mut peers = self.peers.write().await;
            if !peers.get(addr).is_some_and(|peer| {
                peer.state == PeerState::Connected && peer.info.node_id.as_ref() == Some(expected)
            }) {
                return false;
            }
            drop(peers.remove(addr));
            connected_peer_count(&peers)
        };
        send_node_event(
            &self.event_tx,
            &self.shutdown_tx.subscribe(),
            "PeerDisconnected",
            NodeEvent::PeerDisconnected {
                node_id: expected.clone(),
                addr: addr.to_string(),
                peers_connected,
            },
        )
        .await;
        true
    }

    /// Returns true when the configured peer address currently has an active connection.
    pub async fn is_connected_addr(&self, addr: &str) -> bool {
        let peers = self.peers.read().await;
        peers
            .get(addr)
            .is_some_and(|conn| conn.state == PeerState::Connected)
    }

    /// Returns authenticated/claimed identity and transport facts for an active connection.
    pub async fn connection_info(&self, addr: &str) -> Option<PeerConnectionInfo> {
        let peers = self.peers.read().await;
        peers.get(addr).and_then(|connection| {
            (connection.state == PeerState::Connected)
                .then(|| connection.connection_info.clone())
                .flatten()
        })
    }

    /// Publish the endpoint returned by this node's bootstrap service.
    pub fn announce_bootstrap_endpoint(&self, endpoint: impl Into<String>) -> NetResult<()> {
        let server = self
            .bootstrap_server
            .as_ref()
            .ok_or_else(|| NetError::InvalidMessage("bootstrap server is not configured".into()))?;
        server.announce(endpoint.into())
    }

    /// Withdraw the endpoint returned by this node's bootstrap service.
    pub fn withdraw_bootstrap_endpoint(&self) -> NetResult<()> {
        let server = self
            .bootstrap_server
            .as_ref()
            .ok_or_else(|| NetError::InvalidMessage("bootstrap server is not configured".into()))?;
        server.withdraw()
    }

    async fn mark_peer_failed(
        &self,
        addr: &str,
        instance: &Arc<ConnectionInstance>,
    ) -> Option<(NodeId, usize)> {
        let mut peers = self.peers.write().await;
        let removed = remove_connection_if_current(&mut peers, addr, instance)?;
        let node_id = (removed.state == PeerState::Connected)
            .then(|| removed.info.node_id.clone())
            .flatten();
        // Release admission and signal cancellation before any event queue await.
        drop(removed);
        node_id.map(|node_id| (node_id, connected_peer_count(&peers)))
    }

    /// Stop admissions, join owned network tasks within one grace period, then
    /// abort/join any stragglers and close peer writers. Safe to retry if cancelled.
    pub async fn shutdown(&self) {
        let _shutdown = self.shutdown_lock.lock().await;
        let mut tasks = ShutdownTasks::close(&self.tasks);
        self.shutdown_tx.send_replace(true);
        self.connection_slots.close();
        self.outbound_attempt_slots.close();
        if let Some(server) = &self.bootstrap_server {
            server.clear();
        }

        tasks.join(TASK_SHUTDOWN_GRACE).await;

        let count = {
            let mut peers = self.peers.write().await;
            let count = peers.len();
            peers.clear();
            count
        };
        debug!(count, "node peer connections closed");
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let mut registry = self
            .tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        registry.closed = true;
        registry.tasks.abort_all();
        self.shutdown_tx.send_replace(true);
        self.connection_slots.close();
        self.outbound_attempt_slots.close();
        if let Some(server) = &self.bootstrap_server {
            server.clear();
        }
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
        slot: handshake_slot,
        connection_slots,
        control,
        shutdown_rx,
        bootstrap_server,
    } = context;

    let stream: NetStream = match tls {
        Some(ref tls_cfg) => timeout(limits.socket_timeout, tls_cfg.accept_stream(stream))
            .await
            .map_err(|_| NetError::Timeout)??,
        None => NetStream::Plain(stream),
    };

    // Capture the peer certificate (owned) before moving the stream into split().
    let peer_cert = stream.peer_cert_der();

    let (mut reader, mut writer) = tokio::io::split(stream);

    // Wait for HELLO
    let (hello_format, msg) = read_wire_message_with_format(
        &mut reader,
        limits.max_message_size.min(crate::MAX_CONTROL_FRAME),
        limits.socket_timeout,
    )
    .await?;
    let msg = match msg {
        WireMessage {
            kind:
                WireMessageKind::ControlHello {
                    node_id,
                    protocol_version,
                    supported_formats,
                    preferred_format: _,
                    request,
                },
        } => {
            if !is_protocol_version_compatible(protocol_version) {
                let _ = write_message(
                    &mut writer,
                    &WireMessage::wire_error(WireError::protocol_mismatch(protocol_version).into()),
                    hello_format,
                    limits.socket_timeout,
                )
                .await;
                return Err(protocol_version_mismatch(protocol_version));
            }
            verify_peer_identity(&our_node_id, &node_id, peer_cert.as_ref(), tls.as_ref())?;
            if request.owner.member != node_id.to_string() {
                return Err(NetError::InvalidMessage(
                    "control owner is not the admitted sender".into(),
                ));
            }
            let format =
                negotiate_serialization_format(limits.serialization_format, &supported_formats)
                    .ok_or_else(|| NetError::InvalidMessage("no control codec in common".into()))?;
            let _slot = match Arc::clone(&control.incoming).try_acquire_owned() {
                Ok(slot) => slot,
                Err(_) => {
                    return write_message(
                        &mut writer,
                        &WireMessage::wire_error(ProtocolWireError::RateLimited {
                            retry_after_ms: Some(100),
                        }),
                        format,
                        limits.socket_timeout,
                    )
                    .await;
                }
            };
            drop(handshake_slot);
            let request_id = request.request_id;
            let (respond_to, response) = tokio::sync::oneshot::channel();
            if control
                .events
                .try_send(crate::ControlIncoming {
                    from: node_id,
                    verification: identity_verification(tls.as_ref()),
                    request,
                    respond_to,
                })
                .is_err()
            {
                return write_message(
                    &mut writer,
                    &WireMessage::wire_error(ProtocolWireError::RateLimited {
                        retry_after_ms: Some(100),
                    }),
                    format,
                    limits.socket_timeout,
                )
                .await;
            }
            let reply = tokio::select! {
                _ = wait_for_stop(shutdown_rx.clone()) => return Ok(()),
                reply = timeout(limits.socket_timeout.min(Duration::from_secs(2)), response) =>
                    reply.map_err(|_| NetError::Timeout)?.map_err(|_| NetError::ConnectionFailed("control service unavailable".into()))?,
            };
            if reply.request_id != request_id || reply.owner.member != our_node_id.to_string() {
                return Err(NetError::InvalidMessage(
                    "invalid local control response".into(),
                ));
            }
            let ack = WireMessage {
                kind: WireMessageKind::ControlAck {
                    node_id: our_node_id,
                    protocol_version: PROTOCOL_VERSION,
                    selected_format: format,
                    reply,
                },
            };
            write_message(&mut writer, &ack, format, limits.socket_timeout).await?;
            return Ok(());
        }
        other => other,
    };
    let slot = connection_slots
        .try_acquire_owned()
        .map_err(|_| NetError::PeerLimitReached(limits.max_peers))?;
    drop(handshake_slot);
    let handshake = match msg.kind {
        WireMessageKind::Hello {
            node_id,
            protocol_version,
            supported_formats,
            preferred_format,
        } => {
            if !is_protocol_version_compatible(protocol_version) {
                let error = WireError::protocol_mismatch(protocol_version);
                let _ = write_message(
                    &mut writer,
                    &WireMessage::wire_error(error.into()),
                    hello_format,
                    limits.socket_timeout,
                )
                .await;
                return Err(protocol_version_mismatch(protocol_version));
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
            IncomingHandshake::Peer {
                node_id,
                format: negotiated_format,
            }
        }
        WireMessageKind::BootstrapHello {
            node_id,
            protocol_version,
            supported_formats,
            preferred_format,
            cluster_id,
            advertised_endpoint,
            max_results,
        } => {
            if !is_protocol_version_compatible(protocol_version) {
                let error = WireError::protocol_mismatch(protocol_version);
                let _ = write_message(
                    &mut writer,
                    &WireMessage::wire_error(error.into()),
                    hello_format,
                    limits.socket_timeout,
                )
                .await;
                return Err(protocol_version_mismatch(protocol_version));
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
                    "selected alternate bootstrap serialization format"
                );
            }
            IncomingHandshake::Bootstrap {
                node_id,
                format: negotiated_format,
                cluster_id,
                advertised_endpoint,
                max_results: max_results as usize,
            }
        }
        WireMessageKind::Error { error } => {
            return Err(NetError::Wire(error.try_into()?));
        }
        _ => {
            return Err(NetError::InvalidMessage(
                "expected Hello or BootstrapHello".into(),
            ));
        }
    };

    let peer_node_id = match &handshake {
        IncomingHandshake::Peer { node_id, .. } | IncomingHandshake::Bootstrap { node_id, .. } => {
            node_id
        }
    };
    verify_peer_identity(&our_node_id, peer_node_id, peer_cert.as_ref(), tls.as_ref())?;

    if let IncomingHandshake::Bootstrap {
        node_id,
        format,
        cluster_id,
        advertised_endpoint,
        max_results,
    } = handshake
    {
        let server = match bootstrap_server {
            Some(server) => server,
            None => {
                let error = ProtocolWireError::BootstrapRejected {
                    reason: "bootstrap service is disabled".into(),
                };
                let _ = write_message(
                    &mut writer,
                    &WireMessage::wire_error(error.clone()),
                    format,
                    limits.socket_timeout,
                )
                .await;
                return Err(NetError::InvalidMessage(error.to_string()));
            }
        };
        if cluster_id != server.cluster_id() {
            let error = ProtocolWireError::BootstrapRejected {
                reason: "cluster ID does not match this bootstrap seed".into(),
            };
            let _ = write_message(
                &mut writer,
                &WireMessage::wire_error(error.clone()),
                format,
                limits.socket_timeout,
            )
            .await;
            return Err(NetError::InvalidMessage(error.to_string()));
        }
        if max_results == 0 {
            let error = ProtocolWireError::BootstrapRejected {
                reason: "max_results must be greater than zero".into(),
            };
            let _ = write_message(
                &mut writer,
                &WireMessage::wire_error(error.clone()),
                format,
                limits.socket_timeout,
            )
            .await;
            return Err(NetError::InvalidMessage(error.to_string()));
        }
        let candidates = match server.exchange(&node_id, advertised_endpoint, max_results) {
            Ok(candidates) => candidates,
            Err(error) => {
                let wire_error = ProtocolWireError::BootstrapRejected {
                    reason: error.to_string(),
                };
                let _ = write_message(
                    &mut writer,
                    &WireMessage::wire_error(wire_error.clone()),
                    format,
                    limits.socket_timeout,
                )
                .await;
                return Err(NetError::InvalidMessage(wire_error.to_string()));
            }
        };
        let candidate_ttl_ms =
            u64::try_from(server.candidate_ttl().as_millis()).unwrap_or(u64::MAX);
        let ack = WireMessage::bootstrap_ack(
            our_node_id,
            format,
            cluster_id,
            candidates,
            candidate_ttl_ms,
        );
        write_message(&mut writer, &ack, format, limits.socket_timeout).await?;
        return Ok(());
    }

    let IncomingHandshake::Peer {
        node_id: peer_node_id,
        format: negotiated_format,
    } = handshake
    else {
        unreachable!("bootstrap handshakes return before peer admission")
    };

    {
        let peers = peers.read().await;
        ensure_peer_slot_available(&peers, limits.max_peers, Some(&addr))?;
    }

    let ack = Message::hello_ack_with_format(our_node_id, negotiated_format);
    write_message(&mut writer, &ack, negotiated_format, limits.socket_timeout).await?;

    let writer = Arc::new(Mutex::new(writer));
    let connection_instance = Arc::new(ConnectionInstance::new());
    let peers_connected = {
        let mut peers = peers.write().await;
        ensure_peer_slot_available(&peers, limits.max_peers, Some(&addr))?;
        peers.insert(
            addr.clone(),
            PeerConnection {
                info: PeerInfo::new(&addr).with_node_id(peer_node_id.clone()),
                connection_info: Some(PeerConnectionInfo {
                    transport_addr: addr.clone(),
                    dialed_endpoint: None,
                    direction: ConnectionDirection::Inbound,
                    identity: PeerIdentity {
                        node_id: peer_node_id.clone(),
                        verification: identity_verification(tls.as_ref()),
                    },
                }),
                instance: Arc::clone(&connection_instance),
                state: PeerState::Connected,
                serialization_format: negotiated_format,
                writer: Some(Arc::clone(&writer)),
                _slot: slot,
            },
        );
        connected_peer_count(&peers)
    };

    info!(peer = %peer_node_id, serialization_format = ?negotiated_format, "incoming peer connected");

    let shutdown_for_events = shutdown_rx.clone();
    let closed_rx = connection_instance.closed_tx.subscribe();
    while_connection_open(
        &closed_rx,
        &shutdown_for_events,
        send_node_event(
            &event_tx,
            &shutdown_for_events,
            "PeerConnected",
            NodeEvent::PeerConnected {
                node_id: peer_node_id.clone(),
                addr: addr.clone(),
                peers_connected,
            },
        ),
    )
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
            closed_rx,
        },
    )
    .await;

    let disconnected = {
        let mut peers = peers.write().await;
        let Some(removed) = remove_connection_if_current(&mut peers, &addr, &connection_instance)
        else {
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
        send_node_event(
            &event_tx,
            &shutdown_for_events,
            "PeerDisconnected",
            NodeEvent::PeerDisconnected {
                node_id,
                addr,
                peers_connected,
            },
        )
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

fn remove_connection_if_current(
    peers: &mut HashMap<String, PeerConnection>,
    addr: &str,
    instance: &Arc<ConnectionInstance>,
) -> Option<PeerConnection> {
    peers
        .get(addr)
        .is_some_and(|connection| Arc::ptr_eq(&connection.instance, instance))
        .then(|| peers.remove(addr))
        .flatten()
}

pub(crate) async fn connect_transport(
    addr: &str,
    tls: Option<&TlsConfig>,
    socket_timeout: Duration,
) -> NetResult<(NetStream, String)> {
    let tcp = timeout(socket_timeout, TcpStream::connect(addr))
        .await
        .map_err(|_| NetError::Timeout)?
        .map_err(|error| NetError::ConnectionFailed(format!("{addr}: {error}")))?;
    let transport_addr = tcp.peer_addr()?.to_string();
    let stream = if let Some(tls_config) = tls {
        let host = endpoint_host(addr)?;
        let server_name =
            rustls::pki_types::ServerName::try_from(host.to_string()).map_err(|error| {
                NetError::TlsError(format!("invalid server name '{host}': {error}"))
            })?;
        timeout(socket_timeout, tls_config.connect_stream(tcp, server_name))
            .await
            .map_err(|_| NetError::Timeout)??
    } else {
        NetStream::Plain(tcp)
    };
    Ok((stream, transport_addr))
}

fn endpoint_host(endpoint: &str) -> NetResult<&str> {
    if endpoint.starts_with('[') {
        let closing = endpoint
            .find(']')
            .ok_or_else(|| NetError::InvalidMessage("invalid bracketed peer endpoint".into()))?;
        return Ok(&endpoint[1..closing]);
    }
    endpoint
        .rsplit_once(':')
        .map(|(host, _)| host)
        .filter(|host| !host.is_empty())
        .ok_or_else(|| NetError::InvalidMessage("peer endpoint must be host:port".into()))
}

pub(crate) fn verify_peer_identity(
    our_node_id: &NodeId,
    peer_node_id: &NodeId,
    peer_cert: Option<&rustls::pki_types::CertificateDer<'static>>,
    tls: Option<&TlsConfig>,
) -> NetResult<()> {
    if peer_node_id == our_node_id {
        return Err(NetError::ConnectionFailed(format!(
            "refusing connection to local node ID: {peer_node_id}"
        )));
    }

    if let Some(tls_config) = tls
        && !tls_config.insecure
    {
        let peer_cert = peer_cert
            .ok_or_else(|| NetError::TlsError("missing peer certificate in TLS session".into()))?;
        let expected = crate::tls::derive_protocol_node_id_from_cert(peer_cert)?;
        if peer_node_id != &expected {
            let fingerprint = crate::tls::cert_fingerprint_hex(peer_cert)
                .unwrap_or_else(|_| "<unavailable>".into());
            return Err(NetError::TlsError(format!(
                "node_id mismatch (claimed={peer_node_id:?}, expected={expected:?}, fingerprint={fingerprint})"
            )));
        }
        if !tls_config.is_peer_allowed(&peer_node_id.to_string()) {
            return Err(NetError::TlsError(format!(
                "peer node_id not in allowlist: {peer_node_id:?}"
            )));
        }
    }
    Ok(())
}

fn identity_verification(tls: Option<&TlsConfig>) -> PeerIdentityVerification {
    if tls.is_some_and(|config| !config.insecure) {
        PeerIdentityVerification::CertificateBound
    } else {
        PeerIdentityVerification::Unverified
    }
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

pub(crate) fn supported_formats_for(preferred: SerializationFormat) -> Vec<SerializationFormat> {
    match preferred {
        SerializationFormat::Json => vec![SerializationFormat::Json],
        SerializationFormat::Bincode => DEFAULT_SUPPORTED_FORMATS.to_vec(),
    }
}

pub(crate) fn negotiate_serialization_format(
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
    NetError::Wire(WireError::protocol_mismatch(their_version))
}

fn is_protocol_version_compatible(peer_version: u32) -> bool {
    peer_version == PROTOCOL_VERSION
}

async fn send_node_event(
    event_tx: &mpsc::Sender<NodeEvent>,
    shutdown_rx: &watch::Receiver<bool>,
    event_name: &'static str,
    event: NodeEvent,
) {
    // Keep immediately available shutdown notifications, but never wait for a
    // full queue during shutdown. Disconnect callers release peer resources first.
    let result = match event_tx.try_send(event) {
        Ok(()) => return,
        Err(mpsc::error::TrySendError::Closed(event)) => Err(mpsc::error::SendError(event)),
        Err(mpsc::error::TrySendError::Full(event)) => {
            tokio::select! {
                biased;
                _ = wait_for_stop(shutdown_rx.clone()) => return,
                result = event_tx.send(event) => result,
            }
        }
    };
    if let Err(e) = result {
        if *shutdown_rx.borrow() {
            debug!(
                event = event_name,
                error = %e,
                "node event receiver already closed during shutdown"
            );
        } else {
            warn!(event = event_name, error = %e, "failed to send node event; receiver closed");
        }
    }
}

/// Loop for reading messages from a peer until disconnection
async fn read_loop(
    reader: tokio::io::ReadHalf<NetStream>,
    context: ReadLoopContext,
) -> NetResult<()> {
    let closed_rx = context.closed_rx.clone();
    let shutdown_rx = context.shutdown_rx.clone();
    // Cover the whole loop, including event backpressure and Ping writer locks.
    while_connection_open(&closed_rx, &shutdown_rx, read_messages(reader, context))
        .await
        .unwrap_or(Ok(()))
}

async fn read_messages(
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
        closed_rx: _,
    } = context;
    let shutdown_for_events = shutdown_rx.clone();

    loop {
        if *shutdown_rx.borrow() {
            break;
        }
        let msg = tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    debug!(peer = %peer_node_id, "read loop shutdown requested");
                    break;
                }
                continue;
            }

            msg = async { read_wire_message_with_format(&mut reader, max_message_size, socket_timeout).await.map(|(_, message)| message) } => {
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

        let msg = match msg {
            WireMessage {
                kind: WireMessageKind::History { message },
            } => {
                send_node_event(
                    &event_tx,
                    &shutdown_for_events,
                    "HistoryReceived",
                    NodeEvent::HistoryReceived {
                        from: peer_node_id.clone(),
                        addr: addr.clone(),
                        message,
                    },
                )
                .await;
                continue;
            }
            other => Message::try_from(other)?,
        };
        match msg.kind {
            MessageKind::PushOps { ops } => {
                debug!(peer = %peer_node_id, count = ops.len(), "received ops");
                send_node_event(
                    &event_tx,
                    &shutdown_for_events,
                    "OpsReceived",
                    NodeEvent::OpsReceived {
                        from: peer_node_id.clone(),
                        ops,
                    },
                )
                .await;
            }
            MessageKind::PullSince { since_op_id } => {
                debug!(peer = %peer_node_id, addr = %addr, ?since_op_id, "received pull request");
                send_node_event(
                    &event_tx,
                    &shutdown_for_events,
                    "PullRequested",
                    NodeEvent::PullRequested {
                        from: peer_node_id.clone(),
                        addr: addr.clone(),
                        since_op_id,
                    },
                )
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
            MessageKind::Error { error } => {
                return Err(NetError::Wire(error));
            }
            _ => {
                debug!(peer = %peer_node_id, kind = ?msg.kind, "unhandled message");
            }
        }
    }

    Ok(())
}

/// Writes a message to a stream.
pub(crate) trait WireEncode {
    fn encode_wire(&self, format: SerializationFormat) -> NetResult<Vec<u8>>;
}

impl WireEncode for Message {
    fn encode_wire(&self, format: SerializationFormat) -> NetResult<Vec<u8>> {
        self.to_bytes_with_format(format)
    }
}

impl WireEncode for WireMessage {
    fn encode_wire(&self, format: SerializationFormat) -> NetResult<Vec<u8>> {
        self.to_bytes_with_format(format)
    }
}

pub(crate) async fn write_message<W, M>(
    writer: &mut W,
    msg: &M,
    serialization_format: SerializationFormat,
    socket_timeout: Duration,
) -> NetResult<()>
where
    W: AsyncWriteExt + Unpin,
    M: WireEncode,
{
    let bytes = msg.encode_wire(serialization_format)?;
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
pub(crate) async fn read_message<R: AsyncReadExt + Unpin>(
    reader: &mut R,
    max_message_size: usize,
    socket_timeout: Duration,
) -> NetResult<Message> {
    let (_, msg) = read_message_with_format(reader, max_message_size, socket_timeout).await?;
    Ok(msg)
}

pub(crate) async fn read_message_with_format<R: AsyncReadExt + Unpin>(
    reader: &mut R,
    max_message_size: usize,
    socket_timeout: Duration,
) -> NetResult<(SerializationFormat, Message)> {
    let (format, message) =
        read_wire_message_with_format(reader, max_message_size, socket_timeout).await?;
    Ok((format, message.try_into()?))
}

pub(crate) async fn read_wire_message_with_format<R: AsyncReadExt + Unpin>(
    reader: &mut R,
    max_message_size: usize,
    socket_timeout: Duration,
) -> NetResult<(SerializationFormat, WireMessage)> {
    // Read length (4 bytes)
    let mut len_buf = [0u8; 4];
    timeout(socket_timeout, reader.read_exact(&mut len_buf))
        .await
        .map_err(|_| NetError::Timeout)??;
    let len = u32::from_be_bytes(len_buf) as usize;

    validate_payload_len(len, max_message_size)?;

    // Read payload
    let mut buf = vec![0u8; len];
    timeout(socket_timeout, reader.read_exact(&mut buf))
        .await
        .map_err(|_| NetError::Timeout)??;

    WireMessage::from_bytes_with_format(&buf)
}

/// Fuzzing-only entry point for the production stream framing path.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub async fn read_wire_message_for_fuzzing(
    bytes: &[u8],
    max_message_size: usize,
) -> NetResult<(SerializationFormat, Message)> {
    let mut reader = bytes;
    read_message_with_format(&mut reader, max_message_size, DEFAULT_SOCKET_TIMEOUT).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tls::TestPki;

    fn test_slot() -> OwnedSemaphorePermit {
        Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap()
    }

    fn owner(member: &NodeId) -> crate::MembershipRecord {
        crate::MembershipRecord {
            member: member.to_string(),
            reporter: member.to_string(),
            version: crate::MembershipVersion {
                epoch: 1,
                incarnation: 0,
            },
            state: crate::MembershipState::Alive,
            endpoint: None,
        }
    }
    #[tokio::test]
    async fn timed_control_excludes_tls_setup_and_rejects_uncorrelated_replies() {
        use rustls::pki_types::{CertificateDer, pem::PemObject};
        let pki = TestPki::generate().unwrap();
        for format in [SerializationFormat::Json, SerializationFormat::Bincode] {
            for corrupt in [false, true] {
                let client_id = crate::tls::derive_protocol_node_id_from_cert(
                    &CertificateDer::from_pem_slice(pki.node2_cert.as_bytes()).unwrap(),
                )
                .unwrap();
                let server_id = crate::tls::derive_protocol_node_id_from_cert(
                    &CertificateDer::from_pem_slice(pki.node1_cert.as_bytes()).unwrap(),
                )
                .unwrap();
                let client = Node::new(
                    NodeConfig::new(client_id.clone(), "127.0.0.1:0")
                        .with_tls(pki.node2_config())
                        .with_serialization_format(format),
                );
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let endpoint = format!("localhost:{}", listener.local_addr().unwrap().port());
                let tls_delay = Duration::from_millis(50);
                let service_delay = Duration::from_millis(15);
                let server = async {
                    let (tcp, _) = listener.accept().await.unwrap();
                    // Deliberate TLS service delay is the injected fault, not polling.
                    tokio::time::sleep(tls_delay).await;
                    let mut stream = pki.node1_config().accept_stream(tcp).await.unwrap();
                    let (_, request) = read_wire_message_with_format(
                        &mut stream,
                        crate::MAX_CONTROL_FRAME,
                        Duration::from_secs(2),
                    )
                    .await
                    .unwrap();
                    let WireMessageKind::ControlHello { request, .. } = request.kind else {
                        panic!("missing control request");
                    };
                    tokio::time::sleep(service_delay).await;
                    write_message(
                        &mut stream,
                        &WireMessage {
                            kind: WireMessageKind::ControlAck {
                                node_id: server_id.clone(),
                                protocol_version: PROTOCOL_VERSION,
                                selected_format: format,
                                reply: crate::ControlReply {
                                    request_id: request.request_id + u64::from(corrupt),
                                    owner: owner(&server_id),
                                    ack: true,
                                    nack: false,
                                    records: vec![],
                                    next: None,
                                },
                            },
                        },
                        format,
                        Duration::from_secs(2),
                    )
                    .await
                    .unwrap();
                };
                let started = std::time::Instant::now();
                let query = client.control_request_timed_tracked(
                    &endpoint,
                    crate::ControlRequest {
                        request_id: 1,
                        owner: owner(&client_id),
                        updates: vec![],
                        kind: crate::ControlKind::Exchange { after: None },
                    },
                    Duration::from_secs(2),
                    None,
                );
                let (result, ()) = timeout(Duration::from_secs(5), async {
                    tokio::join!(query, server)
                })
                .await
                .unwrap();
                if corrupt {
                    assert!(matches!(result, Err(NetError::InvalidMessage(_))));
                } else {
                    let (_, _, timing) = result.unwrap();
                    assert!(timing.round_trip >= service_delay);
                    assert!(started.elapsed().saturating_sub(timing.round_trip) >= tls_delay);
                    assert!(timing.observed_at >= started);
                }
                client.shutdown().await;
            }
        }
    }

    #[tokio::test]
    async fn prepared_history_accounts_framing_and_cannot_cross_connections_or_nodes() {
        for format in [SerializationFormat::Json, SerializationFormat::Bincode] {
            let client = Node::new(
                NodeConfig::new(NodeId::new("client"), "127.0.0.1:0")
                    .with_serialization_format(format)
                    .with_max_message_size(1024),
            );
            let server = Node::new(
                NodeConfig::new(NodeId::new("server"), "127.0.0.1:0")
                    .with_serialization_format(format),
            );
            let address = server.start_listener().await.unwrap().to_string();
            client.connect_to_peer(&address).await.unwrap();
            let message = nx_sync::HistoryMessage::SummaryRequest { after_origin: None };
            let prepared = client
                .prepare_history_to_addr(&address, message.clone())
                .await
                .unwrap();
            let encoded = WireMessage {
                kind: WireMessageKind::History { message },
            }
            .to_bytes_with_format(format)
            .unwrap();
            assert_eq!(prepared.encoded_len(), encoded.len());
            let generation = prepared.generation();
            assert!(!generation.is_closed());
            client
                .disconnect_peer(&address, &NodeId::new("server"))
                .await;
            client.connect_to_peer(&address).await.unwrap();
            assert!(generation.is_closed());
            assert!(matches!(
                client.send_prepared_history(prepared).await,
                Err(NetError::PeerDisconnected(_))
            ));
            assert!(client.is_connected_addr(&address).await);
            let newer = client
                .prepare_history_to_addr(
                    &address,
                    nx_sync::HistoryMessage::SummaryRequest { after_origin: None },
                )
                .await
                .unwrap();
            assert!(generation != newer.generation());
            assert!(matches!(
                server.send_prepared_history(newer).await,
                Err(NetError::InvalidMessage(_))
            ));
            let oversized = nx_sync::HistoryMessage::SummaryRequest {
                after_origin: Some("x".repeat(4096)),
            };
            assert!(matches!(
                client.prepare_history_to_addr(&address, oversized).await,
                Err(NetError::MessageTooLarge { .. })
            ));
            client.shutdown().await;
            server.shutdown().await;
        }
    }

    #[tokio::test]
    async fn control_rpc_meets_local_budget_while_maximum_data_frame_is_blocked() {
        use rustls::pki_types::{CertificateDer, pem::PemObject};
        let pki = TestPki::generate().unwrap();
        for format in [SerializationFormat::Json, SerializationFormat::Bincode] {
            for secure in [false, true] {
                let mut client_config = NodeConfig::new(NodeId::new("client"), "127.0.0.1:0")
                    .with_max_peers(1)
                    .with_event_channel_capacity(1)
                    .with_serialization_format(format);
                let mut server_config = NodeConfig::new(NodeId::new("server"), "127.0.0.1:0")
                    .with_max_peers(1)
                    .with_event_channel_capacity(1)
                    .with_serialization_format(format);
                if secure {
                    client_config.node_id = crate::tls::derive_protocol_node_id_from_cert(
                        &CertificateDer::from_pem_slice(pki.node2_cert.as_bytes()).unwrap(),
                    )
                    .unwrap();
                    server_config.node_id = crate::tls::derive_protocol_node_id_from_cert(
                        &CertificateDer::from_pem_slice(pki.node1_cert.as_bytes()).unwrap(),
                    )
                    .unwrap();
                    client_config.tls = Some(pki.node2_config());
                    server_config.tls = Some(pki.node1_config());
                }
                let client_owner = owner(&client_config.node_id);
                let server_owner = owner(&server_config.node_id);
                let client = Node::new(client_config);
                let mut server = Node::new(server_config);
                let mut incoming = server.take_control_receiver().unwrap();
                let address = server.start_listener().await.unwrap();
                let endpoint = if secure {
                    format!("localhost:{}", address.port())
                } else {
                    address.to_string()
                };
                client.connect_to_peer(&endpoint).await.unwrap();
                assert_eq!(client.connection_slots.available_permits(), 0);
                assert_eq!(server.connection_slots.available_permits(), 0);
                let (peer_addr, _) = server.connected_peers().await.remove(0);
                server
                    .send_message_to_addr(
                        &peer_addr,
                        Message {
                            kind: MessageKind::PushOps { ops: vec![] },
                        },
                    )
                    .await
                    .unwrap();
                let empty = Message {
                    kind: MessageKind::Error {
                        error: WireError::Internal {
                            reason: String::new(),
                        },
                    },
                };
                let overhead = empty.to_bytes_with_format(format).unwrap().len() - 4;
                let maximum = Message {
                    kind: MessageKind::Error {
                        error: WireError::Internal {
                            reason: "x".repeat(DEFAULT_MAX_MESSAGE_SIZE - overhead),
                        },
                    },
                };
                let bytes = maximum.to_bytes_with_format(format).unwrap();
                assert_eq!(bytes.len() - 4, DEFAULT_MAX_MESSAGE_SIZE);
                let writer = Arc::clone(
                    server.peers.read().await[&peer_addr]
                        .writer
                        .as_ref()
                        .unwrap(),
                );
                let data_writer = Arc::clone(&writer);
                let mut blocked = tokio_test::task::spawn(async move {
                    write_bytes(
                        &mut *data_writer.lock().await,
                        &bytes,
                        Duration::from_secs(5),
                    )
                    .await
                });
                assert!(blocked.poll().is_pending());
                assert!(writer.try_lock().is_err());
                assert_eq!(client.event_tx.capacity(), 0);
                let started = std::time::Instant::now();
                let request = crate::ControlRequest {
                    request_id: 81,
                    owner: client_owner,
                    updates: vec![],
                    kind: crate::ControlKind::Exchange { after: None },
                };
                let serve = async {
                    let event = incoming.recv().await.unwrap();
                    event
                        .respond_to
                        .send(crate::ControlReply {
                            request_id: 81,
                            owner: server_owner,
                            ack: false,
                            nack: false,
                            records: vec![],
                            next: None,
                        })
                        .unwrap();
                };
                let budget = Duration::from_millis(500);
                let (reply, ()) = timeout(Duration::from_secs(2), async {
                    tokio::join!(client.control_request(&endpoint, request, budget), serve)
                })
                .await
                .unwrap();
                reply.unwrap();
                let elapsed = started.elapsed();
                assert!(elapsed <= budget, "{format:?}, TLS={secure}: {elapsed:?}");
                assert!(blocked.poll().is_pending());
                assert!(writer.try_lock().is_err());
                eprintln!(
                    "control-maximum-frame {}",
                    serde_json::json!({"format": format!("{format:?}"), "tls": secure,
                    "data_payload_bytes": DEFAULT_MAX_MESSAGE_SIZE, "data_slots_available": 0,
                    "data_event_capacity": 1, "control_budget_ms": budget.as_millis(), "control_elapsed_us": elapsed.as_micros()})
                );
                drop(blocked);
                assert!(writer.try_lock().is_ok());
                client.shutdown().await;
                server.shutdown().await;
            }
        }
    }

    #[test]
    fn node_config_validates_semaphore_boundaries_without_allocating_them() {
        let config = NodeConfig::new(NodeId::new("test"), "127.0.0.1:0");
        config.clone().with_max_peers(0).validate().unwrap();
        config
            .clone()
            .with_max_peers(Semaphore::MAX_PERMITS)
            .validate()
            .unwrap();
        config
            .clone()
            .with_event_channel_capacity(Semaphore::MAX_PERMITS)
            .validate()
            .unwrap();
        for limit in [Semaphore::MAX_PERMITS + 1, usize::MAX] {
            assert!(matches!(
                config.clone().with_max_peers(limit).validate(),
                Err(NodeConfigError::MaxPeersTooLarge { .. })
            ));
            assert!(matches!(
                config.clone().with_event_channel_capacity(limit).validate(),
                Err(NodeConfigError::InvalidEventChannelCapacity { .. })
            ));
        }
        let node = Node::try_new(config).unwrap();
        assert_eq!(node.connection_slots.available_permits(), DEFAULT_MAX_PEERS);
        assert_eq!(node.event_tx.max_capacity(), DEFAULT_EVENT_CHANNEL_CAPACITY);
    }

    #[tokio::test]
    async fn strict_constructor_rejects_invalid_node_configs() {
        let config = NodeConfig::new(NodeId::new("test"), "invalid listen address");
        for invalid in [
            config.clone().with_max_peers(Semaphore::MAX_PERMITS + 1),
            config.clone().with_max_peers(usize::MAX),
            config
                .clone()
                .with_event_channel_capacity(Semaphore::MAX_PERMITS + 1),
            config.clone().with_event_channel_capacity(usize::MAX),
            config.clone().with_event_channel_capacity(0),
            config.clone().with_socket_timeout(Duration::MAX),
            config.clone().with_socket_timeout(Duration::ZERO),
        ] {
            assert!(Node::try_new(invalid).is_err());
        }

        // Preserve the 0.1.4 constructor behavior for existing callers: a zero
        // event capacity is clamped to one by the infallible legacy path.
        let node = Node::new(config.with_event_channel_capacity(0));
        assert_eq!(node.event_tx.max_capacity(), 1);
        assert_eq!(node.connection_slots.available_permits(), DEFAULT_MAX_PEERS);
        node.shutdown().await;
    }

    async fn open_raw_peer(
        node: &Node,
        listener: &TcpListener,
        incoming_addr: Option<std::net::SocketAddr>,
    ) -> TcpStream {
        timeout(Duration::from_secs(3), async {
            if let Some(addr) = incoming_addr {
                let mut stream = TcpStream::connect(addr).await.unwrap();
                write_message(
                    &mut stream,
                    &Message::hello(NodeId::new("raw-peer")),
                    SerializationFormat::Bincode,
                    Duration::from_secs(1),
                )
                .await
                .unwrap();
                let ack = read_message(
                    &mut stream,
                    DEFAULT_MAX_MESSAGE_SIZE,
                    Duration::from_secs(1),
                )
                .await
                .unwrap();
                assert!(matches!(ack.kind, MessageKind::HelloAck { .. }));
                stream
            } else {
                let addr = listener.local_addr().unwrap().to_string();
                let (connected, stream) = tokio::join!(node.connect_to_peer(&addr), async {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let hello = read_message(
                        &mut stream,
                        DEFAULT_MAX_MESSAGE_SIZE,
                        Duration::from_secs(1),
                    )
                    .await
                    .unwrap();
                    assert!(matches!(hello.kind, MessageKind::Hello { .. }));
                    write_message(
                        &mut stream,
                        &Message::hello_ack_with_format(
                            NodeId::new("raw-peer"),
                            SerializationFormat::Bincode,
                        ),
                        SerializationFormat::Bincode,
                        Duration::from_secs(1),
                    )
                    .await
                    .unwrap();
                    stream
                });
                connected.unwrap();
                stream
            }
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn topology_disconnect_checks_identity_and_releases_slot_before_event_delivery() {
        let mut node = Node::try_new(
            NodeConfig::new(NodeId::new("test"), "127.0.0.1:0")
                .with_max_peers(1)
                .with_event_channel_capacity(1),
        )
        .unwrap();
        let mut events = node.take_event_receiver().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let _remote = open_raw_peer(&node, &listener, None).await;
        let Some(NodeEvent::PeerConnected { addr, .. }) = events.recv().await else {
            panic!("missing connection event");
        };
        assert!(
            !node
                .disconnect_peer(&addr, &NodeId::new("wrong-owner"))
                .await
        );
        assert_eq!(node.connected_peer_count().await, 1);
        node.event_tx
            .send(NodeEvent::PeerDisconnected {
                node_id: NodeId::new("queue-filler"),
                addr: "unused".into(),
                peers_connected: 1,
            })
            .await
            .unwrap();
        let expected = NodeId::new("raw-peer");
        let disconnect = node.disconnect_peer(&addr, &expected);
        tokio::pin!(disconnect);
        timeout(Duration::from_secs(2), async {
        tokio::select! {
            _ = &mut disconnect => panic!("full event queue must block delivery"),
            _ = async {
                while node.connection_slots.available_permits() == 0 { tokio::task::yield_now().await; }
            } => {}
        }
        }).await.unwrap();
        assert_eq!(node.connected_peer_count().await, 0);
        events.recv().await.unwrap();
        assert!(disconnect.await);
        assert!(
            matches!(events.recv().await, Some(NodeEvent::PeerDisconnected { node_id, .. }) if node_id == NodeId::new("raw-peer"))
        );
        assert!(!node.disconnect_peer(&addr, &NodeId::new("raw-peer")).await);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn write_failure_releases_live_reader_and_slot_before_disconnect_event_delivery() {
        for incoming in [false, true] {
            for broadcast in [false, true] {
                let mut node = Node::try_new(
                    NodeConfig::new(NodeId::new("test"), "127.0.0.1:0")
                        .with_max_peers(1)
                        .with_event_channel_capacity(1)
                        .with_socket_timeout(Duration::from_secs(60)),
                )
                .unwrap();
                let mut events = node.take_event_receiver().unwrap();
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let incoming_addr = if incoming {
                    Some(node.start_listener().await.unwrap())
                } else {
                    None
                };
                let mut remote = open_raw_peer(&node, &listener, incoming_addr).await;
                let Some(NodeEvent::PeerConnected { addr, .. }) = events.recv().await else {
                    panic!("missing connection event")
                };
                let (writer, instance) = {
                    let peers = node.peers.read().await;
                    let peer = &peers[&addr];
                    (
                        Arc::downgrade(peer.writer.as_ref().unwrap()),
                        Arc::clone(&peer.instance),
                    )
                };
                assert_eq!(node.connection_slots.available_permits(), 0);

                // Inject a deterministic write-side failure without closing the reader.
                // The remote sends a request and never reads again after the handshake.
                writer
                    .upgrade()
                    .unwrap()
                    .lock()
                    .await
                    .shutdown()
                    .await
                    .unwrap();
                write_message(
                    &mut remote,
                    &Message::pull_since(None),
                    SerializationFormat::Bincode,
                    Duration::from_secs(1),
                )
                .await
                .unwrap();
                assert!(matches!(
                    timeout(Duration::from_secs(1), events.recv())
                        .await
                        .unwrap(),
                    Some(NodeEvent::PullRequested { .. })
                ));
                node.event_tx
                    .try_send(NodeEvent::OpsReceived {
                        from: NodeId::new("queued"),
                        ops: vec![],
                    })
                    .unwrap();

                let mut send = tokio_test::task::spawn(async {
                    if broadcast {
                        node.broadcast_message(Message::ping()).await
                    } else {
                        node.send_message_to_addr(&addr, Message::ping()).await
                    }
                });
                // The write has failed and only the bounded disconnect event is blocked.
                assert!(send.poll().is_pending());
                assert_eq!(node.connection_slots.available_permits(), 1);
                assert!(!node.is_connected_addr(&addr).await);
                assert!(*instance.closed_tx.borrow());
                timeout(Duration::from_secs(1), async {
                    while writer.upgrade().is_some() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("reader or failed send retained the socket writer");
                assert!(matches!(
                    events.recv().await,
                    Some(NodeEvent::OpsReceived { .. })
                ));
                assert!(send.await.is_err());
                assert!(matches!(
                    events.recv().await,
                    Some(NodeEvent::PeerDisconnected {
                        peers_connected: 0,
                        ..
                    })
                ));
                assert!(events.try_recv().is_err(), "duplicate disconnect event");

                // This must actually acquire the released permit and complete a handshake.
                let _replacement = open_raw_peer(&node, &listener, incoming_addr).await;
                assert!(matches!(
                    events.recv().await,
                    Some(NodeEvent::PeerConnected {
                        peers_connected: 1,
                        ..
                    })
                ));
                assert_eq!(node.connection_slots.available_permits(), 0);
                assert_eq!(node.connected_peer_count().await, 1);
                assert!(node.mark_peer_failed(&addr, &instance).await.is_none());
                assert!(events.try_recv().is_err());
                node.shutdown().await;
            }
        }
    }

    #[tokio::test]
    async fn read_loop_cancels_event_backpressure_and_ping_writer_lock() {
        for blocked_on_ping in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let stream = TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (mut remote, _) = listener.accept().await.unwrap();
            let (reader, writer) = tokio::io::split(NetStream::Plain(stream));
            let writer = Arc::new(Mutex::new(writer));
            let held_writer = writer.lock().await;
            let instance = ConnectionInstance::new();
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let (event_tx, mut events) = mpsc::channel(1);
            // The first event is an observable barrier; the next frame blocks in
            // either event delivery or Ping's writer lock, not in socket reading.
            let mut bytes = Message::pull_since(None).to_bytes().unwrap();
            bytes.extend(
                if blocked_on_ping {
                    Message::ping()
                } else {
                    Message::pull_since(Some("blocked".into()))
                }
                .to_bytes()
                .unwrap(),
            );
            remote.write_all(&bytes).await.unwrap();
            let mut read = tokio_test::task::spawn(read_loop(
                reader,
                ReadLoopContext {
                    peer_node_id: NodeId::new("peer"),
                    addr: "peer".into(),
                    event_tx,
                    writer: Arc::clone(&writer),
                    serialization_format: SerializationFormat::Bincode,
                    max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
                    socket_timeout: Duration::from_secs(60),
                    shutdown_rx,
                    closed_rx: instance.closed_tx.subscribe(),
                },
            ));
            timeout(
                Duration::from_secs(1),
                std::future::poll_fn(|cx| {
                    assert!(read.poll().is_pending());
                    if events.len() == 1 {
                        std::task::Poll::Ready(())
                    } else {
                        cx.waker().wake_by_ref();
                        std::task::Poll::Pending
                    }
                }),
            )
            .await
            .unwrap();
            instance.closed_tx.send_replace(true);
            timeout(Duration::from_secs(1), read)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(Arc::strong_count(&writer), 1, "reader retained its writer");
            assert!(matches!(
                events.try_recv(),
                Ok(NodeEvent::PullRequested {
                    since_op_id: None,
                    ..
                })
            ));
            assert!(events.try_recv().is_err());
            drop(held_writer);
            drop(shutdown_tx);
        }
    }

    struct ControlTestPath {
        remote: TcpStream,
        reader: tokio::io::ReadHalf<NetStream>,
        context: ReadLoopContext,
        events: mpsc::Receiver<NodeEvent>,
        instance: ConnectionInstance,
        shutdown: watch::Sender<bool>,
        frame_bytes: usize,
        send_started: std::time::Instant,
        send_finished: std::time::Instant,
    }

    async fn control_test_path(
        format: SerializationFormat,
        messages: Vec<Message>,
    ) -> ControlTestPath {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stream = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut remote, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        for message in messages {
            bytes.extend(message.to_bytes_with_format(format).unwrap());
        }
        let send_started = std::time::Instant::now();
        remote.write_all(&bytes).await.unwrap();
        let send_finished = std::time::Instant::now();
        let mut peek = vec![0; bytes.len()];
        timeout(Duration::from_secs(5), async {
            while stream.peek(&mut peek).await.unwrap() < bytes.len() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let (reader, writer) = tokio::io::split(NetStream::Plain(stream));
        let (event_tx, events) = mpsc::channel(1);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let instance = ConnectionInstance::new();
        let closed_rx = instance.closed_tx.subscribe();
        ControlTestPath {
            remote,
            reader,
            events,
            instance,
            shutdown,
            frame_bytes: bytes.len(),
            send_started,
            send_finished,
            context: ReadLoopContext {
                peer_node_id: NodeId::new("control-experiment-peer"),
                addr: "control-experiment-peer".into(),
                event_tx,
                writer: Arc::new(Mutex::new(writer)),
                serialization_format: format,
                max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
                socket_timeout: Duration::from_secs(5),
                shutdown_rx,
                closed_rx,
            },
        }
    }

    #[tokio::test]
    async fn control_progress_isolated_from_shared_writer_and_event_backpressure() {
        for format in [SerializationFormat::Json, SerializationFormat::Bincode] {
            for blocked_on_writer in [false, true] {
                let data_message = || {
                    Message::push_ops(vec![Op::gcounter_increment(
                        NodeId::new("control-experiment-peer"),
                        "control-experiment-counter",
                        1,
                    )])
                };
                let mut messages = vec![data_message()];
                if !blocked_on_writer {
                    messages.push(data_message());
                }
                messages.push(Message::ping());
                let mut shared = control_test_path(format, messages).await;
                let mut independent = control_test_path(format, vec![Message::ping()]).await;
                let shared_writer = Arc::clone(&shared.context.writer);
                let independent_writer = Arc::clone(&independent.context.writer);
                let held_writer = if blocked_on_writer {
                    Some(shared_writer.lock().await)
                } else {
                    None
                };
                let mut shared_read =
                    tokio_test::task::spawn(read_loop(shared.reader, shared.context));
                let mut independent_read =
                    tokio_test::task::spawn(read_loop(independent.reader, independent.context));
                let (received_format, pong) = timeout(Duration::from_secs(5), async {
                    tokio::select! {
                        biased;
                        result = &mut shared_read => panic!("shared reader exited before release: {result:?}"),
                        result = &mut independent_read => panic!("independent reader exited: {result:?}"),
                        reply = read_message_with_format(&mut independent.remote, DEFAULT_MAX_MESSAGE_SIZE, Duration::from_secs(5)) => reply,
                    }
                }).await.unwrap().unwrap();
                assert_eq!(received_format, format);
                assert_eq!(pong, Message::pong());
                assert_eq!(shared.events.len(), 1);
                let independent_observed = independent.send_started.elapsed();
                let mut shared_reply = tokio_test::task::spawn(read_message_with_format(
                    &mut shared.remote,
                    DEFAULT_MAX_MESSAGE_SIZE,
                    Duration::from_secs(5),
                ));
                assert!(shared_reply.poll().is_pending());
                let resumed_at = std::time::Instant::now();
                if blocked_on_writer {
                    drop(held_writer);
                } else {
                    assert!(matches!(
                        shared.events.try_recv(),
                        Ok(NodeEvent::OpsReceived { .. })
                    ));
                }
                let (received_format, pong) = timeout(Duration::from_secs(5), async {
                    tokio::select! {
                        biased;
                        result = &mut shared_read => panic!("shared reader exited before Pong: {result:?}"),
                        reply = &mut shared_reply => reply,
                    }
                }).await.unwrap().unwrap();
                assert_eq!(received_format, format);
                assert_eq!(pong, Message::pong());
                eprintln!(
                    "control-isolation {}",
                    serde_json::json!({
                        "format": format!("{format:?}"),
                        "blocked_on": if blocked_on_writer { "writer" } else { "event_queue" },
                        "event_capacity": 1, "shared_frame_bytes": shared.frame_bytes,
                        "independent_frame_bytes": independent.frame_bytes,
                        "shared_write_us": shared.send_finished.duration_since(shared.send_started).as_micros(),
                        "independent_write_us": independent.send_finished.duration_since(independent.send_started).as_micros(),
                        "independent_write_start_to_pong_us": independent_observed.as_micros(),
                        "shared_write_end_to_release_us": resumed_at.duration_since(shared.send_finished).as_micros(),
                        "shared_release_to_pong_us": resumed_at.elapsed().as_micros(),
                    })
                );
                shared.instance.closed_tx.send_replace(true);
                independent.instance.closed_tx.send_replace(true);
                shared.shutdown.send_replace(true);
                independent.shutdown.send_replace(true);
                timeout(Duration::from_secs(5), shared_read)
                    .await
                    .unwrap()
                    .unwrap();
                timeout(Duration::from_secs(5), independent_read)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(Arc::strong_count(&shared_writer), 1);
                assert_eq!(Arc::strong_count(&independent_writer), 1);
            }
        }
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
        assert_eq!(
            config.event_channel_capacity,
            DEFAULT_EVENT_CHANNEL_CAPACITY
        );
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
                connection_info: None,
                instance: Arc::new(ConnectionInstance::new()),
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
                connection_info: None,
                instance: Arc::new(ConnectionInstance::new()),
                state: PeerState::Connected,
                serialization_format: SerializationFormat::Bincode,
                writer: None,
                _slot: test_slot(),
            },
        );

        ensure_peer_slot_available(&peers, 1, Some("127.0.0.1:9001")).unwrap();
    }

    #[test]
    fn stale_connection_cleanup_cannot_remove_a_replacement() {
        let addr = "127.0.0.1:9001";
        let current = Arc::new(ConnectionInstance::new());
        let stale = Arc::new(ConnectionInstance::new());
        let mut peers = HashMap::from([(
            addr.to_string(),
            PeerConnection {
                info: PeerInfo::new(addr),
                connection_info: None,
                instance: Arc::clone(&current),
                state: PeerState::Connected,
                serialization_format: SerializationFormat::Bincode,
                writer: None,
                _slot: test_slot(),
            },
        )]);

        assert!(remove_connection_if_current(&mut peers, addr, &stale).is_none());
        assert!(peers.contains_key(addr));
        assert!(remove_connection_if_current(&mut peers, addr, &current).is_some());
        assert!(!peers.contains_key(addr));
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
                    connection_info: None,
                    instance: Arc::new(ConnectionInstance::new()),
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
                    connection_info: None,
                    instance: Arc::new(ConnectionInstance::new()),
                    state: PeerState::Connected,
                    serialization_format: SerializationFormat::Bincode,
                    writer: None,
                    _slot: test_slot(),
                },
            );
        }

        let instance = Arc::clone(&node.peers.read().await["127.0.0.1:9001"].instance);
        let (node_id, connected) = node
            .mark_peer_failed("127.0.0.1:9001", &instance)
            .await
            .unwrap();

        assert_eq!(node_id, NodeId::new("peer-a"));
        assert_eq!(connected, 1);
        assert!(
            node.mark_peer_failed("127.0.0.1:9001", &instance)
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn registry_prunes_finished_tasks_before_admission() {
        let tasks = StdMutex::new(TaskRegistry::default());
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        spawn_task(&tasks, async move {
            done_tx.send(()).unwrap();
        })
        .unwrap();
        done_rx.await.unwrap();
        spawn_task(&tasks, std::future::pending()).unwrap();
        assert_eq!(tasks.lock().unwrap().tasks.len(), 1);
        ShutdownTasks::close(&tasks).join(Duration::ZERO).await;
    }

    #[tokio::test]
    async fn registry_rejects_late_admission_and_drops_the_unspawned_future() {
        let tasks = StdMutex::new(TaskRegistry::default());
        let mut shutdown = ShutdownTasks::close(&tasks);
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel::<()>();
        assert!(
            spawn_task(&tasks, async move {
                let _sender = dropped_tx;
                panic!("late task must never run");
            })
            .is_err()
        );
        assert!(dropped_rx.await.is_err());
        shutdown.join(Duration::ZERO).await;
        assert!(shutdown.tasks.is_empty());
    }

    #[tokio::test]
    async fn concurrent_registration_is_joined_or_rejected() {
        let node = Node::new(NodeConfig::new(NodeId::new("test"), "127.0.0.1:0"));
        let registry = Arc::clone(&node.tasks);
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (admitted_tx, admitted_rx) = tokio::sync::oneshot::channel();
        spawn_task(&node.tasks, async move {
            entered_tx.send(()).unwrap();
            release_rx.await.unwrap();
            let admitted = spawn_task(&registry, async {});
            admitted_tx.send(admitted.is_ok()).unwrap();
        })
        .unwrap();
        entered_rx.await.unwrap();
        let mut shutdown = tokio_test::task::spawn(node.shutdown());
        assert!(shutdown.poll().is_pending());
        release_tx.send(()).unwrap();
        assert!(!admitted_rx.await.unwrap());
        shutdown.await;
        let registry = node.tasks.lock().unwrap();
        assert!(registry.closed);
        assert!(registry.tasks.is_empty());
    }

    #[tokio::test]
    async fn cancelled_shutdown_aborts_tasks_and_retains_handles_for_retry() {
        let node = Node::new(NodeConfig::new(NodeId::new("test"), "127.0.0.1:0"));
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel::<()>();
        spawn_task(&node.tasks, async move {
            let _sender = dropped_tx;
            std::future::pending::<()>().await;
        })
        .unwrap();
        let mut shutdown = tokio_test::task::spawn(node.shutdown());
        assert!(shutdown.poll().is_pending());
        drop(shutdown);
        assert!(
            dropped_rx.await.is_err(),
            "cancelled shutdown detached a task"
        );
        assert_eq!(node.tasks.lock().unwrap().tasks.len(), 1);
        assert!(node.start_listener().await.is_err());
        node.shutdown().await;
        assert!(node.tasks.lock().unwrap().tasks.is_empty());
        assert_eq!(node.connected_peer_count().await, 0);
    }

    #[tokio::test]
    async fn outbound_handshake_finishing_after_shutdown_cannot_register_a_peer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let node = Arc::new(Node::new(NodeConfig::new(
            NodeId::new("test"),
            "127.0.0.1:0",
        )));
        let client = Arc::clone(&node);
        let connect = tokio::spawn(async move { client.connect_to_peer(&addr).await });
        let (mut stream, _) = listener.accept().await.unwrap();
        read_message(
            &mut stream,
            DEFAULT_MAX_MESSAGE_SIZE,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        node.shutdown().await;
        write_message(
            &mut stream,
            &Message::hello_ack_with_format(NodeId::new("peer"), SerializationFormat::Bincode),
            SerializationFormat::Bincode,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert!(connect.await.unwrap().is_err());
        assert!(node.connected_peers().await.is_empty());
        assert!(node.tasks.lock().unwrap().tasks.is_empty());
        assert_eq!(node.connection_slots.available_permits(), DEFAULT_MAX_PEERS);
    }

    #[tokio::test]
    async fn cancelling_connect_during_event_backpressure_keeps_read_task_owned() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let mut node = Node::new(
            NodeConfig::new(NodeId::new("test"), "127.0.0.1:0").with_event_channel_capacity(1),
        );
        let mut events = node.take_event_receiver().unwrap();
        node.event_tx
            .try_send(NodeEvent::OpsReceived {
                from: NodeId::new("queued"),
                ops: vec![],
            })
            .unwrap();
        let node = Arc::new(node);
        let client = Arc::clone(&node);
        let endpoint = addr.clone();
        let connect = tokio::spawn(async move { client.connect_to_peer(&endpoint).await });
        let (mut stream, _) = listener.accept().await.unwrap();
        read_message(
            &mut stream,
            DEFAULT_MAX_MESSAGE_SIZE,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        write_message(
            &mut stream,
            &Message::hello_ack_with_format(NodeId::new("peer"), SerializationFormat::Bincode),
            SerializationFormat::Bincode,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        timeout(Duration::from_secs(1), async {
            while !node.is_connected_addr(&addr).await {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!connect.is_finished());
        connect.abort();
        assert!(connect.await.unwrap_err().is_cancelled());
        assert_eq!(node.tasks.lock().unwrap().tasks.len(), 1);
        assert!(matches!(
            events.recv().await,
            Some(NodeEvent::OpsReceived { .. })
        ));
        assert!(matches!(
            events.recv().await,
            Some(NodeEvent::PeerConnected { .. })
        ));
        node.shutdown().await;
        assert!(node.tasks.lock().unwrap().tasks.is_empty());
        assert!(node.connected_peers().await.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_uses_one_total_grace_for_all_tasks() {
        let node = Node::new(NodeConfig::new(NodeId::new("test"), "127.0.0.1:0"));
        let mut dropped = Vec::new();
        for _ in 0..8 {
            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            dropped.push(rx);
            spawn_task(&node.tasks, async move {
                let _sender = tx;
                std::future::pending::<()>().await;
            })
            .unwrap();
        }
        let started = tokio::time::Instant::now();
        node.shutdown().await;
        assert_eq!(started.elapsed(), TASK_SHUTDOWN_GRACE);
        for task in dropped {
            assert!(task.await.is_err());
        }
        assert!(node.tasks.lock().unwrap().tasks.is_empty());
    }

    #[tokio::test]
    async fn dropping_node_aborts_owned_tasks_even_when_registry_is_shared() {
        let node = Node::new(NodeConfig::new(NodeId::new("test"), "127.0.0.1:0"));
        let registry = Arc::clone(&node.tasks);
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let task_registry = Arc::clone(&registry);
        spawn_task(&registry, async move {
            let _registry = task_registry;
            let _sender = tx;
            std::future::pending::<()>().await;
        })
        .unwrap();
        drop(node);
        assert!(rx.await.is_err());
        assert!(registry.lock().unwrap().closed);
        ShutdownTasks::close(&registry).join(Duration::ZERO).await;
    }

    #[tokio::test]
    async fn stale_writer_failure_does_not_fail_or_emit_disconnect_for_replacement() {
        for broadcast in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let stream = TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (_remote, _) = listener.accept().await.unwrap();
            let (_reader, writer) = tokio::io::split(NetStream::Plain(stream));
            let writer = Arc::new(Mutex::new(writer));
            let mut held_writer = writer.lock().await;
            held_writer.shutdown().await.unwrap();
            let mut node = Node::new(NodeConfig::new(NodeId::new("test"), "127.0.0.1:0"));
            let mut events = node.take_event_receiver().unwrap();
            let addr = "127.0.0.1:9001";
            node.peers.write().await.insert(
                addr.into(),
                PeerConnection {
                    info: PeerInfo::new(addr).with_node_id(NodeId::new("old")),
                    connection_info: None,
                    instance: Arc::new(ConnectionInstance::new()),
                    state: PeerState::Connected,
                    serialization_format: SerializationFormat::Bincode,
                    writer: Some(Arc::clone(&writer)),
                    _slot: test_slot(),
                },
            );
            let mut send = tokio_test::task::spawn(async {
                if broadcast {
                    node.broadcast_message(Message::ping()).await
                } else {
                    node.send_message_to_addr(addr, Message::ping()).await
                }
            });
            // The snapshot has been taken, but writing is blocked on our writer lock.
            assert!(send.poll().is_pending());
            let stale = Arc::clone(&node.peers.read().await[addr].instance);
            let replacement = Arc::new(ConnectionInstance::new());
            node.peers.write().await.insert(
                addr.into(),
                PeerConnection {
                    info: PeerInfo::new(addr).with_node_id(NodeId::new("replacement")),
                    connection_info: None,
                    instance: Arc::clone(&replacement),
                    state: PeerState::Connected,
                    serialization_format: SerializationFormat::Bincode,
                    writer: None,
                    _slot: test_slot(),
                },
            );
            // Cancellation must release the snapshot without acquiring this lock.
            assert!(
                timeout(Duration::from_secs(1), send)
                    .await
                    .unwrap()
                    .is_err()
            );
            assert!(node.mark_peer_failed(addr, &stale).await.is_none());
            drop(held_writer);
            assert!(node.is_connected_addr(addr).await);
            assert_eq!(
                node.connected_peers().await,
                [(addr.into(), NodeId::new("replacement"))]
            );
            assert!(events.try_recv().is_err());
            assert!(Arc::ptr_eq(
                &node.peers.read().await[addr].instance,
                &replacement
            ));
            node.shutdown().await;
        }
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
                    connection_info: None,
                    instance: Arc::new(ConnectionInstance::new()),
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
                    connection_info: None,
                    instance: Arc::new(ConnectionInstance::new()),
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

    #[tokio::test]
    async fn connected_peers_is_sorted_and_excludes_failed_connections() {
        let node = Node::new(NodeConfig::new(NodeId::new("test"), "127.0.0.1:0"));
        for (addr, state) in [
            ("z.example:9000", PeerState::Connected),
            ("a.example:9000", PeerState::Connected),
            ("failed.example:9000", PeerState::Failed),
        ] {
            node.peers.write().await.insert(
                addr.into(),
                PeerConnection {
                    info: PeerInfo::new(addr).with_node_id(NodeId::new(addr)),
                    connection_info: None,
                    instance: Arc::new(ConnectionInstance::new()),
                    state,
                    serialization_format: SerializationFormat::Bincode,
                    writer: None,
                    _slot: test_slot(),
                },
            );
        }
        assert_eq!(
            node.connected_peers().await,
            [
                ("a.example:9000".into(), NodeId::new("a.example:9000")),
                ("z.example:9000".into(), NodeId::new("z.example:9000")),
            ]
        );
        node.shutdown().await;
        assert!(node.connected_peers().await.is_empty());
    }

    #[test]
    fn node_config_allows_custom_wire_limits() {
        let config = NodeConfig::new(NodeId::new("test"), "127.0.0.1:9000")
            .with_max_message_size(1024)
            .with_socket_timeout(Duration::from_secs(3))
            .with_event_channel_capacity(4096);

        assert_eq!(config.max_message_size, 1024);
        assert_eq!(config.socket_timeout, Duration::from_secs(3));
        assert_eq!(config.event_channel_capacity, 4096);
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
    async fn connect_to_peer_times_out_during_tls_handshake() {
        let pki = TestPki::generate().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept_task = tokio::spawn(async move {
            let (_stream, _addr) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let node = Node::new(
            NodeConfig::new(NodeId::new("test"), "127.0.0.1:0")
                .with_tls(pki.node1_config())
                .with_socket_timeout(Duration::from_millis(10)),
        );

        let err = node.connect_to_peer(&addr.to_string()).await.unwrap_err();

        assert!(matches!(err, NetError::Timeout));
        accept_task.await.unwrap();
    }

    #[tokio::test]
    async fn connect_to_peer_rejects_protocol_version_mismatch() {
        for version in [5, PROTOCOL_VERSION - 1, PROTOCOL_VERSION + 1] {
            for format in [SerializationFormat::Json, SerializationFormat::Bincode] {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let accept_task = tokio::spawn(async move {
                    let (stream, _) = listener.accept().await.unwrap();
                    let mut stream = NetStream::Plain(stream);
                    let hello = read_message(
                        &mut stream,
                        DEFAULT_MAX_MESSAGE_SIZE,
                        Duration::from_secs(1),
                    )
                    .await
                    .unwrap();
                    assert!(matches!(hello.kind, MessageKind::Hello { .. }));
                    let ack = Message {
                        kind: MessageKind::HelloAck {
                            node_id: NodeId::new("incompatible-peer"),
                            protocol_version: version,
                            selected_format: format,
                        },
                    };
                    stream
                        .write_all(&ack.to_bytes_with_format(format).unwrap())
                        .await
                        .unwrap();
                    stream.flush().await.unwrap();
                    // Rejection must close the socket before any CRDT frame is sent.
                    let mut byte = [0u8; 1];
                    assert_eq!(
                        timeout(Duration::from_secs(1), stream.read(&mut byte))
                            .await
                            .unwrap()
                            .unwrap(),
                        0
                    );
                });
                let mut node = Node::new(
                    NodeConfig::new(NodeId::new("test"), "127.0.0.1:0")
                        .with_socket_timeout(Duration::from_secs(1)),
                );
                let mut events = node.take_event_receiver().unwrap();
                let err = node.connect_to_peer(&addr.to_string()).await.unwrap_err();
                assert!(matches!(
                    err,
                    NetError::Wire(WireError::ProtocolMismatch { expected: PROTOCOL_VERSION, got }) if got == version
                ));
                accept_task.await.unwrap();
                assert_eq!(node.connected_peer_count().await, 0);
                assert!(matches!(
                    events.try_recv(),
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty)
                ));
                node.shutdown().await;
            }
        }
    }

    #[test]
    fn protocol_compatibility_requires_an_exact_version_match() {
        assert!(is_protocol_version_compatible(PROTOCOL_VERSION));
        assert!(!is_protocol_version_compatible(PROTOCOL_VERSION - 1));
        assert!(!is_protocol_version_compatible(PROTOCOL_VERSION + 1));
    }

    #[tokio::test]
    async fn incoming_rejects_protocol_version_mismatch() {
        for version in [5, PROTOCOL_VERSION - 1, PROTOCOL_VERSION + 1] {
            for format in [SerializationFormat::Json, SerializationFormat::Bincode] {
                let mut node = Node::new(
                    NodeConfig::new(NodeId::new("node-a"), "127.0.0.1:0")
                        .with_socket_timeout(Duration::from_secs(1)),
                );
                let mut events = node.take_event_receiver().unwrap();
                let addr = node.start_listener().await.unwrap();
                let mut stream = TcpStream::connect(addr).await.unwrap();
                let hello = Message {
                    kind: MessageKind::Hello {
                        node_id: NodeId::new("incompatible-peer"),
                        protocol_version: version,
                        supported_formats: vec![format],
                        preferred_format: format,
                    },
                };
                stream
                    .write_all(&hello.to_bytes_with_format(format).unwrap())
                    .await
                    .unwrap();
                stream.flush().await.unwrap();
                let reply = read_message(
                    &mut stream,
                    DEFAULT_MAX_MESSAGE_SIZE,
                    Duration::from_secs(1),
                )
                .await
                .unwrap();
                assert!(matches!(
                    reply.kind,
                    MessageKind::Error { error: WireError::ProtocolMismatch { expected: PROTOCOL_VERSION, got } } if got == version
                ));
                let mut byte = [0u8; 1];
                assert_eq!(
                    timeout(Duration::from_secs(1), stream.read(&mut byte))
                        .await
                        .unwrap()
                        .unwrap(),
                    0
                );
                assert_eq!(node.connected_peer_count().await, 0);
                assert!(matches!(
                    events.try_recv(),
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty)
                ));
                node.shutdown().await;
            }
        }
    }

    #[tokio::test]
    async fn incoming_idle_handshake_consumes_bounded_handshake_not_peer_slot() {
        let config = NodeConfig::new(NodeId::new("test"), "127.0.0.1:0")
            .with_max_peers(1)
            .with_socket_timeout(Duration::from_secs(5));
        let mut node = Node::new(config);
        node.handshake_slots = Arc::new(Semaphore::new(1));
        let addr = node.start_listener().await.unwrap();

        let first = TcpStream::connect(addr).await.unwrap();
        timeout(Duration::from_secs(1), async {
            while node.handshake_slots.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        assert_eq!(node.connection_slots.available_permits(), 1);
        assert_eq!(node.connected_peer_count().await, 0);

        let mut second = TcpStream::connect(addr).await.unwrap();
        let mut byte = [0];
        assert_eq!(
            timeout(Duration::from_secs(1), second.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );

        assert_eq!(node.handshake_slots.available_permits(), 0);
        assert_eq!(node.connection_slots.available_permits(), 1);
        assert_eq!(node.connected_peer_count().await, 0);

        drop(second);
        drop(first);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn incoming_idle_tls_handshake_releases_peer_slot_after_timeout() {
        let pki = TestPki::generate().unwrap();
        let config = NodeConfig::new(NodeId::new("test"), "127.0.0.1:0")
            .with_tls(pki.node1_config())
            .with_max_peers(1)
            .with_socket_timeout(Duration::from_millis(10));
        let node = Node::new(config);
        let addr = node.start_listener().await.unwrap();

        let stream = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(node.connection_slots.available_permits(), 1);
        assert_eq!(node.connected_peer_count().await, 0);

        drop(stream);
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
        let connection = node_a
            .connection_info(&addr_b.to_string())
            .await
            .expect("active connection metadata");
        assert_eq!(connection.transport_addr, addr_b.to_string());
        assert_eq!(connection.dialed_endpoint, Some(addr_b.to_string()));
        assert_eq!(connection.direction, ConnectionDirection::Outbound);
        assert_eq!(connection.identity.node_id, NodeId::new("node-b"));
        assert_eq!(
            connection.identity.verification,
            PeerIdentityVerification::Unverified
        );
        node_a.shutdown().await;
        node_b.shutdown().await;
    }

    #[tokio::test]
    async fn self_node_id_is_rejected_after_handshake() {
        let node = Node::new(
            NodeConfig::new(NodeId::new("same-node"), "127.0.0.1:0")
                .with_socket_timeout(Duration::from_secs(1)),
        );
        let addr = node.start_listener().await.unwrap();

        assert!(node.connect_to_peer(&addr.to_string()).await.is_err());
        assert_eq!(node.connected_peer_count().await, 0);

        node.shutdown().await;
    }

    #[tokio::test]
    async fn duplicate_outbound_attempt_is_rejected_while_handshake_is_pending() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = accepted_tx.send(());
            let _ = release_rx.await;
            drop(stream);
        });
        let node = Arc::new(Node::new(
            NodeConfig::new(NodeId::new("client"), "127.0.0.1:0")
                .with_socket_timeout(Duration::from_secs(2)),
        ));
        let first_node = Arc::clone(&node);
        let first_addr = addr.clone();
        let first = tokio::spawn(async move { first_node.connect_to_peer(&first_addr).await });
        accepted_rx.await.unwrap();

        assert!(matches!(
            node.connect_to_peer(&addr).await,
            Err(NetError::ConnectionFailed(message))
                if message.contains("connection attempt already in progress")
                    && message.contains(&addr)
        ));

        let _ = release_tx.send(());
        assert!(first.await.unwrap().is_err());
        server.await.unwrap();
        node.shutdown().await;
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
