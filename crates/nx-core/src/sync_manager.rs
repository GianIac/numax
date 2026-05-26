use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant as StdInstant};

use nx_net::{NetError, Node, NodeConfig, NodeEvent};
use nx_store::Store as NxStore;
use nx_sync::{GCounter, LwwRegister, NodeId, Op, OpKind, PNCounter};
use tokio::sync::{RwLock, mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::observability::RuntimeMetrics;
use crate::sync_config::SyncConfig;

/// Upper bound on how many ops we coalesce into a single PushOps message.
const BROADCAST_BATCH_MAX: usize = 1024;
const BROADCAST_COALESCE_DELAY: Duration = Duration::from_millis(1);
const GCOUNTER_STORE_PREFIX: &str = "__nx/crdt/gcounter/";
const GCOUNTER_STATE_STORE_PREFIX: &str = "__nx/crdt/state/gcounter/";
const PNCOUNTER_STORE_PREFIX: &str = "__nx/crdt/pncounter/";
const PNCOUNTER_STATE_STORE_PREFIX: &str = "__nx/crdt/state/pncounter/";
const LWW_REGISTER_STORE_PREFIX: &str = "__nx/crdt/lww-register/";
const LWW_REGISTER_STATE_STORE_PREFIX: &str = "__nx/crdt/state/lww-register/";
const SEEN_OP_STORE_PREFIX: &str = "__nx/crdt/seen-op/";
const OP_LOG_STORE_PREFIX: &str = "__nx/crdt/op-log/";

#[derive(Debug, Clone, Copy)]
enum CrdtStoreNamespace {
    Materialized,
    State,
}

#[derive(Debug, Clone, Copy)]
enum CrdtKind {
    GCounter,
    PNCounter,
    LwwRegister,
}

impl CrdtKind {
    fn label(self) -> &'static str {
        match self {
            Self::GCounter => "GCounter",
            Self::PNCounter => "PNCounter",
            Self::LwwRegister => "LWW-Register",
        }
    }

    fn prefix(self, namespace: CrdtStoreNamespace) -> &'static str {
        match (self, namespace) {
            (Self::GCounter, CrdtStoreNamespace::Materialized) => GCOUNTER_STORE_PREFIX,
            (Self::GCounter, CrdtStoreNamespace::State) => GCOUNTER_STATE_STORE_PREFIX,
            (Self::PNCounter, CrdtStoreNamespace::Materialized) => PNCOUNTER_STORE_PREFIX,
            (Self::PNCounter, CrdtStoreNamespace::State) => PNCOUNTER_STATE_STORE_PREFIX,
            (Self::LwwRegister, CrdtStoreNamespace::Materialized) => LWW_REGISTER_STORE_PREFIX,
            (Self::LwwRegister, CrdtStoreNamespace::State) => LWW_REGISTER_STATE_STORE_PREFIX,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum SeenOpsInsert {
    AlreadySeen,
    Inserted { evicted: Vec<String> },
}

/// Health state for a configured peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerHealthState {
    Healthy,
    Suspect,
    Dead,
}

#[derive(Debug, Clone, Copy)]
struct PeerHealth {
    state: PeerHealthState,
    consecutive_failures: u32,
}

impl Default for PeerHealth {
    fn default() -> Self {
        Self {
            state: PeerHealthState::Suspect,
            consecutive_failures: 0,
        }
    }
}

#[derive(Debug, Clone)]
struct SeenOps {
    ids: HashSet<String>,
    order: VecDeque<String>,
    limit: usize,
}

impl SeenOps {
    fn new(limit: usize) -> Self {
        Self {
            ids: HashSet::new(),
            order: VecDeque::new(),
            limit: normalize_seen_ops_limit(limit),
        }
    }

    #[cfg(test)]
    fn contains(&self, op_id: &str) -> bool {
        self.ids.contains(op_id)
    }

    fn insert(&mut self, op_id: impl Into<String>) -> SeenOpsInsert {
        let op_id = op_id.into();
        if !self.ids.insert(op_id.clone()) {
            return SeenOpsInsert::AlreadySeen;
        }

        self.order.push_back(op_id);
        let mut evicted_ids = Vec::new();
        while self.order.len() > self.limit {
            if let Some(evicted) = self.order.pop_front() {
                self.ids.remove(&evicted);
                evicted_ids.push(evicted);
            }
        }
        SeenOpsInsert::Inserted {
            evicted: evicted_ids,
        }
    }

    fn len(&self) -> usize {
        self.ids.len()
    }
}

struct ReconnectLoopContext {
    node: Arc<Node>,
    peers: Vec<String>,
    max_peers: usize,
    initial_delay: Duration,
    max_delay: Duration,
    peer_dead_after_failures: u32,
    shutdown_rx: watch::Receiver<bool>,
    metrics: Arc<RuntimeMetrics>,
    peer_health: Arc<RwLock<HashMap<String, PeerHealth>>>,
}

struct AntiEntropyLoopContext {
    node: Arc<Node>,
    peers: Vec<String>,
    interval: Duration,
    shutdown_rx: watch::Receiver<bool>,
    metrics: Arc<RuntimeMetrics>,
    peer_node_ids: Arc<RwLock<HashMap<String, NodeId>>>,
    anti_entropy_watermarks: Arc<RwLock<HashMap<NodeId, String>>>,
}

struct BroadcastLoopContext {
    node: Arc<Node>,
    seen_ops: Arc<RwLock<SeenOps>>,
    seen_ops_next_sequence: Arc<AtomicU64>,
    op_log: Arc<RwLock<Vec<Op>>>,
    op_log_next_sequence: Arc<AtomicU64>,
    op_log_limit: usize,
    store: Arc<NxStore>,
    metrics: Arc<RuntimeMetrics>,
}

struct RemoteOpApplyContext<'a> {
    counters: &'a Arc<RwLock<HashMap<String, GCounter>>>,
    pncounters: &'a Arc<RwLock<HashMap<String, PNCounter>>>,
    lww_registers: &'a Arc<RwLock<HashMap<String, LwwRegister>>>,
    seen_ops: &'a Arc<RwLock<SeenOps>>,
    seen_ops_next_sequence: &'a Arc<AtomicU64>,
    op_log: &'a Arc<RwLock<Vec<Op>>>,
    op_log_next_sequence: &'a Arc<AtomicU64>,
    op_log_limit: usize,
    store: &'a Arc<NxStore>,
    metrics: &'a Arc<RuntimeMetrics>,
}

struct OpPersistencePlan {
    op: Op,
    seen_sequence: u64,
    op_log_sequence: u64,
    seen_evicted: Vec<String>,
    op_log_evicted: Vec<Op>,
}

#[derive(Clone)]
struct NodeEventContext {
    counters: Arc<RwLock<HashMap<String, GCounter>>>,
    pncounters: Arc<RwLock<HashMap<String, PNCounter>>>,
    lww_registers: Arc<RwLock<HashMap<String, LwwRegister>>>,
    seen_ops: Arc<RwLock<SeenOps>>,
    seen_ops_next_sequence: Arc<AtomicU64>,
    op_log: Arc<RwLock<Vec<Op>>>,
    op_log_next_sequence: Arc<AtomicU64>,
    op_log_limit: usize,
    store: Arc<NxStore>,
    metrics: Arc<RuntimeMetrics>,
    node: Arc<Node>,
    peer_health: Arc<RwLock<HashMap<String, PeerHealth>>>,
    peer_node_ids: Arc<RwLock<HashMap<String, NodeId>>>,
    anti_entropy_watermarks: Arc<RwLock<HashMap<NodeId, String>>>,
    peer_dead_after_failures: u32,
}

struct PeerReconnectState {
    addr: String,
    delay: Duration,
    next_attempt_at: StdInstant,
}

impl PeerReconnectState {
    fn new(addr: String, initial_delay: Duration, now: StdInstant) -> Self {
        Self {
            addr,
            delay: initial_delay,
            next_attempt_at: now,
        }
    }

    fn reset(&mut self, initial_delay: Duration, now: StdInstant) {
        self.delay = initial_delay;
        self.next_attempt_at = now;
    }

    fn record_failure(&mut self, max_delay: Duration, now: StdInstant) -> Duration {
        let attempt_delay = self.delay;
        self.next_attempt_at = now + attempt_delay;
        self.delay = next_reconnect_delay(attempt_delay, max_delay);
        attempt_delay
    }
}

struct ConfiguredPeerConnectContext<'a> {
    node: &'a Node,
    max_peers: usize,
    peer_dead_after_failures: u32,
    metrics: &'a Arc<RuntimeMetrics>,
    peer_health: &'a Arc<RwLock<HashMap<String, PeerHealth>>>,
}

enum ConfiguredPeerConnectOutcome {
    Connected,
    AlreadyConnected,
    SlotLimitReached,
    Failed,
}

/// Clonable, cheap handle to the SyncManager exposed to host calls.
#[derive(Clone)]
pub struct SyncHandle {
    node_id: NodeId,
    op_tx: mpsc::Sender<Op>,
    counters: Arc<RwLock<HashMap<String, GCounter>>>,
    pncounters: Arc<RwLock<HashMap<String, PNCounter>>>,
    lww_registers: Arc<RwLock<HashMap<String, LwwRegister>>>,
    store: Arc<NxStore>,
    metrics: Arc<RuntimeMetrics>,
    peer_node_ids: Arc<RwLock<HashMap<String, NodeId>>>,
}

impl SyncHandle {
    /// NodeId of the local node (used to stamp locally-produced Ops).
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Sender to enqueue Ops for broadcast. Backpressure is bounded by the underlying channel capacity.
    pub fn op_sender(&self) -> mpsc::Sender<Op> {
        self.op_tx.clone()
    }

    /// Read-side handle over the counter registry.
    pub fn counters(&self) -> Arc<RwLock<HashMap<String, GCounter>>> {
        Arc::clone(&self.counters)
    }

    /// Read-side handle over the PNCounter registry.
    pub fn pncounters(&self) -> Arc<RwLock<HashMap<String, PNCounter>>> {
        Arc::clone(&self.pncounters)
    }

    /// Read-side handle over the LWW-Register registry.
    pub fn lww_registers(&self) -> Arc<RwLock<HashMap<String, LwwRegister>>> {
        Arc::clone(&self.lww_registers)
    }

    /// Shared datastore used to materialize CRDT values.
    pub fn store(&self) -> Arc<NxStore> {
        Arc::clone(&self.store)
    }

    /// Shared runtime metrics.
    pub fn metrics(&self) -> Arc<RuntimeMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Connected peers known to the sync manager, as `(addr, node_id)` pairs.
    pub async fn connected_peers(&self) -> Vec<(String, NodeId)> {
        let peers = self.peer_node_ids.read().await;
        let mut peers = peers
            .iter()
            .map(|(addr, node_id)| (addr.clone(), node_id.clone()))
            .collect::<Vec<_>>();
        peers.sort_by(|(addr_a, _), (addr_b, _)| addr_a.cmp(addr_b));
        peers
    }
}

pub struct SyncManager {
    /// NodeId.
    node_id: NodeId,

    /// SyncConfig
    config: SyncConfig,

    /// Network node. Wrapped in `Arc` so the broadcast drain task spawned
    /// by `start` can share ownership with the manager.
    node: Option<Arc<Node>>,

    /// GCounter
    counters: Arc<RwLock<HashMap<String, GCounter>>>,

    /// PNCounter
    pncounters: Arc<RwLock<HashMap<String, PNCounter>>>,

    /// LWW-Register
    lww_registers: Arc<RwLock<HashMap<String, LwwRegister>>>,

    /// Store used to materialize replicated values.
    store: Arc<NxStore>,

    /// Shared runtime metrics.
    metrics: Arc<RuntimeMetrics>,

    /// OpId
    seen_ops: Arc<RwLock<SeenOps>>,

    /// Monotonic sequence used to retain recent durable dedup metadata.
    seen_ops_next_sequence: Arc<AtomicU64>,

    /// In-memory operation log used to answer anti-entropy pull requests.
    op_log: Arc<RwLock<Vec<Op>>>,

    /// Monotonic sequence used to retain recent durable operation-log entries.
    op_log_next_sequence: Arc<AtomicU64>,

    /// Health state for configured peers, keyed by configured address.
    peer_health: Arc<RwLock<HashMap<String, PeerHealth>>>,

    /// Connected configured peer NodeIds, keyed by configured address.
    peer_node_ids: Arc<RwLock<HashMap<String, NodeId>>>,

    /// Last received OpId per peer NodeId, used for incremental anti-entropy pulls.
    anti_entropy_watermarks: Arc<RwLock<HashMap<NodeId, String>>>,

    /// Channel to send Ops to broadcast.
    op_tx: mpsc::Sender<Op>,

    /// Receiver drained by `start` into the broadcast task.
    op_rx: Option<mpsc::Receiver<Op>>,

    /// Shutdown signal shared with background tasks.
    shutdown_tx: watch::Sender<bool>,

    /// Inbound event loop task.
    event_task: Option<JoinHandle<()>>,

    /// Outbound broadcast drain task.
    broadcast_task: Option<JoinHandle<()>>,

    /// Configured peer reconnect task.
    reconnect_task: Option<JoinHandle<()>>,

    /// Periodic anti-entropy pull task.
    anti_entropy_task: Option<JoinHandle<()>>,
}

impl SyncManager {
    /// new SyncManager.
    pub fn new(
        node_id: NodeId,
        config: SyncConfig,
        store: Arc<NxStore>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        let (op_tx, op_rx) = mpsc::channel(config.queued_ops_limit.max(1));
        let op_log_limit = normalize_op_log_limit(config.op_log_limit);
        let seen_ops_limit = normalize_seen_ops_limit(config.seen_ops_limit);
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let mut counters = HashMap::new();
        let mut pncounters = HashMap::new();
        let mut lww_registers = HashMap::new();
        let (op_log, op_log_next_sequence) = match hydrate_op_log(&store, op_log_limit) {
            Ok(hydrated) => hydrated,
            Err(e) => {
                warn!(error = %e, "failed to hydrate operation log");
                (Vec::with_capacity(op_log_limit.min(1024)), 0)
            }
        };
        let peer_health = config
            .peers
            .iter()
            .map(|peer| (peer.clone(), PeerHealth::default()))
            .collect::<HashMap<_, _>>();

        let (seen_ops, seen_ops_next_sequence) = match hydrate_seen_ops(&store, seen_ops_limit) {
            Ok(hydrated) => hydrated,
            Err(e) => {
                warn!(error = %e, "failed to hydrate seen OpIds");
                (SeenOps::new(seen_ops_limit), 0)
            }
        };

        if let Err(e) = hydrate_gcounter_registry(&store, &node_id, &mut counters) {
            warn!(error = %e, "failed to hydrate GCounter registry");
        }
        if let Err(e) = hydrate_pncounter_registry(&store, &node_id, &mut pncounters) {
            warn!(error = %e, "failed to hydrate PNCounter registry");
        }
        if let Err(e) = hydrate_lww_register_registry(&store, &mut lww_registers) {
            warn!(error = %e, "failed to hydrate LWW-Register registry");
        }
        let counters = Arc::new(RwLock::new(counters));
        let pncounters = Arc::new(RwLock::new(pncounters));
        let lww_registers = Arc::new(RwLock::new(lww_registers));

        Self {
            node_id,
            config,
            node: None,
            counters,
            pncounters,
            lww_registers,
            store,
            metrics,
            seen_ops: Arc::new(RwLock::new(seen_ops)),
            seen_ops_next_sequence: Arc::new(AtomicU64::new(seen_ops_next_sequence)),
            op_log: Arc::new(RwLock::new(op_log)),
            op_log_next_sequence: Arc::new(AtomicU64::new(op_log_next_sequence)),
            peer_health: Arc::new(RwLock::new(peer_health)),
            peer_node_ids: Arc::new(RwLock::new(HashMap::new())),
            anti_entropy_watermarks: Arc::new(RwLock::new(HashMap::new())),
            op_tx,
            op_rx: Some(op_rx),
            shutdown_tx,
            event_task: None,
            broadcast_task: None,
            reconnect_task: None,
            anti_entropy_task: None,
        }
    }

    /// Return  NodeId.
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Return sender
    pub fn op_sender(&self) -> mpsc::Sender<Op> {
        self.op_tx.clone()
    }

    /// Take the receiver of operations
    pub fn take_op_receiver(&mut self) -> Option<mpsc::Receiver<Op>> {
        self.op_rx.take()
    }

    /// Build a clonable handle exposing the op channel and the counter registry.
    pub fn handle(&self) -> SyncHandle {
        SyncHandle {
            node_id: self.node_id.clone(),
            op_tx: self.op_tx.clone(),
            counters: Arc::clone(&self.counters),
            pncounters: Arc::clone(&self.pncounters),
            lww_registers: Arc::clone(&self.lww_registers),
            store: Arc::clone(&self.store),
            metrics: Arc::clone(&self.metrics),
            peer_node_ids: Arc::clone(&self.peer_node_ids),
        }
    }

    /// Start networking: bind the listener, dial initial peers, spawn the inbound event loop and the outbound broadcast drain loop.
    pub async fn start(&mut self) -> anyhow::Result<()> {
        let listen_addr = match &self.config.listen_addr {
            Some(addr) => addr.clone(),
            None => {
                info!("sync disabled: no listen_addr");
                return Ok(());
            }
        };

        // Build the network node.
        let mut node_config = NodeConfig::new(self.node_id.clone(), &listen_addr)
            .with_peers(self.config.peers.clone())
            .with_max_peers(self.config.max_peers)
            .with_max_message_size(self.config.max_message_size)
            .with_socket_timeout(self.config.socket_timeout)
            .with_serialization_format(self.config.serialization_format);

        if let Some(tls) = self.config.tls.clone() {
            node_config = node_config.with_tls(tls);
        }

        let mut node = Node::new(node_config);
        let mut event_rx = node.take_event_receiver().unwrap();

        node.start_listener().await?;

        // Connect to initial peers.
        let peer_dead_after_failures =
            normalize_peer_dead_after_failures(self.config.peer_dead_after_failures);
        let connect_context = ConfiguredPeerConnectContext {
            node: &node,
            max_peers: self.config.max_peers,
            peer_dead_after_failures,
            metrics: &self.metrics,
            peer_health: &self.peer_health,
        };
        for peer_addr in &self.config.peers {
            if matches!(
                try_connect_configured_peer(&connect_context, peer_addr).await,
                ConfiguredPeerConnectOutcome::SlotLimitReached
            ) {
                break;
            }
        }

        // Move the node into an Arc so it can be shared between the manager and the broadcast drain task.
        let node = Arc::new(node);
        self.node = Some(Arc::clone(&node));

        // Inbound loop: apply remote ops into the counter registry.
        let event_context = NodeEventContext {
            counters: Arc::clone(&self.counters),
            pncounters: Arc::clone(&self.pncounters),
            lww_registers: Arc::clone(&self.lww_registers),
            seen_ops: Arc::clone(&self.seen_ops),
            seen_ops_next_sequence: Arc::clone(&self.seen_ops_next_sequence),
            op_log: Arc::clone(&self.op_log),
            op_log_next_sequence: Arc::clone(&self.op_log_next_sequence),
            op_log_limit: normalize_op_log_limit(self.config.op_log_limit),
            store: Arc::clone(&self.store),
            metrics: Arc::clone(&self.metrics),
            node: Arc::clone(&node),
            peer_health: Arc::clone(&self.peer_health),
            peer_node_ids: Arc::clone(&self.peer_node_ids),
            anti_entropy_watermarks: Arc::clone(&self.anti_entropy_watermarks),
            peer_dead_after_failures: normalize_peer_dead_after_failures(
                self.config.peer_dead_after_failures,
            ),
        };
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        self.event_task = Some(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            debug!("event loop shutdown requested");
                            drain_node_events(&mut event_rx, &event_context).await;
                            break;
                        }
                    }
                    event = event_rx.recv() => {
                        let Some(event) = event else {
                            break;
                        };
                        handle_node_event(event, &event_context).await;
                    }
                }
            }
            debug!("event loop terminated");
        }));

        // Outbound loop: drain locally-produced ops into the network.
        let op_rx = self
            .op_rx
            .take()
            .expect("op_rx already taken: SyncManager::start called twice?");
        self.broadcast_task = Some(spawn_broadcast_loop(
            BroadcastLoopContext {
                node: Arc::clone(&node),
                seen_ops: Arc::clone(&self.seen_ops),
                seen_ops_next_sequence: Arc::clone(&self.seen_ops_next_sequence),
                op_log: Arc::clone(&self.op_log),
                op_log_next_sequence: Arc::clone(&self.op_log_next_sequence),
                op_log_limit: normalize_op_log_limit(self.config.op_log_limit),
                store: Arc::clone(&self.store),
                metrics: Arc::clone(&self.metrics),
            },
            op_rx,
            self.shutdown_tx.subscribe(),
        ));

        self.reconnect_task = spawn_reconnect_loop(ReconnectLoopContext {
            node: Arc::clone(&node),
            peers: self.config.peers.clone(),
            max_peers: self.config.max_peers,
            initial_delay: self.config.reconnect_initial_delay,
            max_delay: self.config.reconnect_max_delay,
            peer_dead_after_failures: self.config.peer_dead_after_failures,
            shutdown_rx: self.shutdown_tx.subscribe(),
            metrics: Arc::clone(&self.metrics),
            peer_health: Arc::clone(&self.peer_health),
        });

        self.anti_entropy_task = spawn_anti_entropy_loop(AntiEntropyLoopContext {
            node: Arc::clone(&node),
            peers: self.config.peers.clone(),
            interval: self.config.anti_entropy_interval,
            shutdown_rx: self.shutdown_tx.subscribe(),
            metrics: Arc::clone(&self.metrics),
            peer_node_ids: Arc::clone(&self.peer_node_ids),
            anti_entropy_watermarks: Arc::clone(&self.anti_entropy_watermarks),
        });

        Ok(())
    }

    /// Connect to a peer after the manager has started.
    pub async fn connect_to_peer(&self, addr: &str) -> anyhow::Result<()> {
        let Some(node) = self.node.as_ref() else {
            anyhow::bail!("sync manager is not started");
        };
        node.connect_to_peer(addr).await?;
        Ok(())
    }

    /// Retry connecting to the peers configured at startup.
    pub async fn reconnect_configured_peers(&self) {
        let Some(node) = self.node.as_ref() else {
            return;
        };
        let peer_dead_after_failures =
            normalize_peer_dead_after_failures(self.config.peer_dead_after_failures);
        let connect_context = ConfiguredPeerConnectContext {
            node,
            max_peers: self.config.max_peers,
            peer_dead_after_failures,
            metrics: &self.metrics,
            peer_health: &self.peer_health,
        };
        for peer_addr in &self.config.peers {
            if matches!(
                try_connect_configured_peer(&connect_context, peer_addr).await,
                ConfiguredPeerConnectOutcome::SlotLimitReached
            ) {
                break;
            }
        }
    }

    /// Returns the current health state of a configured peer.
    pub async fn peer_health_state(&self, addr: &str) -> Option<PeerHealthState> {
        let peer_health = self.peer_health.read().await;
        peer_health.get(addr).map(|health| health.state)
    }

    /// Returns the number of connected peers, or zero before networking starts.
    pub async fn connected_peer_count(&self) -> usize {
        let Some(node) = self.node.as_ref() else {
            return 0;
        };
        node.connected_peer_count().await
    }

    /// Returns the current value of a GCounter.
    pub async fn get_counter_value(&self, key: &str) -> u64 {
        let counters = self.counters.read().await;
        counters.get(key).map(|c| c.value()).unwrap_or(0)
    }

    /// Returns the current value of a PNCounter.
    pub async fn get_pncounter_value(&self, key: &str) -> i64 {
        let pncounters = self.pncounters.read().await;
        pncounters.get(key).map(|c| c.value()).unwrap_or(0)
    }

    /// Returns the current value of an LWW-Register.
    pub async fn get_lww_register_value(&self, key: &str) -> Option<Vec<u8>> {
        let registers = self.lww_registers.read().await;
        registers.get(key).map(|register| register.value_bytes())
    }

    /// Gracefully stop sync tasks and close network connections.
    pub async fn shutdown(&mut self) -> anyhow::Result<()> {
        let _ = self.shutdown_tx.send(true);

        if let Some(task) = self.broadcast_task.take()
            && let Err(e) = task.await
        {
            warn!(error = %e, "broadcast task failed during shutdown");
        }

        if let Some(task) = self.reconnect_task.take()
            && let Err(e) = task.await
        {
            warn!(error = %e, "reconnect task failed during shutdown");
        }

        if let Some(task) = self.anti_entropy_task.take()
            && let Err(e) = task.await
        {
            warn!(error = %e, "anti-entropy task failed during shutdown");
        }

        if let Some(node) = self.node.as_ref() {
            node.shutdown().await;
        }
        self.metrics.set_peers_connected(0);

        if let Some(task) = self.event_task.take()
            && let Err(e) = task.await
        {
            warn!(error = %e, "event task failed during shutdown");
        }

        info!("sync manager shut down");
        Ok(())
    }
}

/// Spawn the outbound broadcast drain loop.
fn spawn_broadcast_loop(
    context: BroadcastLoopContext,
    mut op_rx: mpsc::Receiver<Op>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        debug!("broadcast loop shutdown requested");
                        drain_broadcast_queue(
                            &context,
                            &mut op_rx,
                        ).await;
                        break;
                    }
                }
                op = op_rx.recv() => {
                    let Some(first) = op else {
                        break;
                    };
                    broadcast_batch(
                        &context,
                        &mut op_rx,
                        first,
                    ).await;
                }
            }
        }
        debug!("broadcast loop terminated");
    })
}

async fn try_connect_configured_peer(
    context: &ConfiguredPeerConnectContext<'_>,
    peer_addr: &str,
) -> ConfiguredPeerConnectOutcome {
    if context.node.is_connected_addr(peer_addr).await {
        mark_peer_success(context.peer_health, peer_addr).await;
        return ConfiguredPeerConnectOutcome::AlreadyConnected;
    }

    if context.node.connected_peer_count().await >= context.max_peers {
        debug!(
            peer = %peer_addr,
            limit = context.max_peers,
            "skipping configured peer connect: peer slot limit reached"
        );
        return ConfiguredPeerConnectOutcome::SlotLimitReached;
    }

    match context.node.connect_to_peer(peer_addr).await {
        Ok(()) => {
            mark_peer_success(context.peer_health, peer_addr).await;
            ConfiguredPeerConnectOutcome::Connected
        }
        Err(NetError::PeerLimitReached(limit)) => {
            debug!(
                peer = %peer_addr,
                limit,
                "skipping configured peer connect: peer slot limit reached"
            );
            ConfiguredPeerConnectOutcome::SlotLimitReached
        }
        Err(e) => {
            context.metrics.record_sync_error();
            mark_peer_failure(
                context.peer_health,
                peer_addr,
                context.peer_dead_after_failures,
            )
            .await;
            debug!(peer = %peer_addr, error = %e, "configured peer connect failed");
            ConfiguredPeerConnectOutcome::Failed
        }
    }
}

fn spawn_reconnect_loop(context: ReconnectLoopContext) -> Option<JoinHandle<()>> {
    let ReconnectLoopContext {
        node,
        peers,
        max_peers,
        initial_delay,
        max_delay,
        peer_dead_after_failures,
        mut shutdown_rx,
        metrics,
        peer_health,
    } = context;

    if peers.is_empty() {
        return None;
    }

    Some(tokio::spawn(async move {
        let initial_delay = normalize_reconnect_delay(initial_delay);
        let max_delay = max_delay.max(initial_delay);
        let peer_dead_after_failures = normalize_peer_dead_after_failures(peer_dead_after_failures);
        let now = StdInstant::now();
        let mut state = peers
            .into_iter()
            .map(|addr| PeerReconnectState::new(addr, initial_delay, now))
            .collect::<Vec<_>>();

        loop {
            let mut sleep_for: Option<Duration> = None;
            let now = StdInstant::now();
            let connect_context = ConfiguredPeerConnectContext {
                node: node.as_ref(),
                max_peers,
                peer_dead_after_failures,
                metrics: &metrics,
                peer_health: &peer_health,
            };

            for peer in state.iter_mut() {
                let peer_addr = peer.addr.as_str();

                if node.is_connected_addr(peer_addr).await {
                    mark_peer_success(&peer_health, peer_addr).await;
                    peer.reset(initial_delay, StdInstant::now());
                    continue;
                }

                if let Some(wait) = peer.next_attempt_at.checked_duration_since(now)
                    && !wait.is_zero()
                {
                    sleep_for = Some(sleep_for.map_or(wait, |current| current.min(wait)));
                    continue;
                }

                match try_connect_configured_peer(&connect_context, peer_addr).await {
                    ConfiguredPeerConnectOutcome::Connected => {
                        info!(peer = %peer_addr, "reconnected configured peer");
                        peer.reset(initial_delay, StdInstant::now());
                    }
                    ConfiguredPeerConnectOutcome::AlreadyConnected => {
                        peer.reset(initial_delay, StdInstant::now());
                    }
                    ConfiguredPeerConnectOutcome::SlotLimitReached => {
                        break;
                    }
                    ConfiguredPeerConnectOutcome::Failed => {
                        let attempt_delay = peer.record_failure(max_delay, StdInstant::now());
                        sleep_for = Some(
                            sleep_for.map_or(attempt_delay, |current| current.min(attempt_delay)),
                        );
                    }
                }
            }
            let sleep_for = sleep_for.unwrap_or(initial_delay);

            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        debug!("reconnect loop shutdown requested");
                        break;
                    }
                }
                _ = tokio::time::sleep(sleep_for) => {}
            }
        }
        debug!("reconnect loop terminated");
    }))
}

fn spawn_anti_entropy_loop(context: AntiEntropyLoopContext) -> Option<JoinHandle<()>> {
    let AntiEntropyLoopContext {
        node,
        peers,
        interval,
        mut shutdown_rx,
        metrics,
        peer_node_ids,
        anti_entropy_watermarks,
    } = context;

    if peers.is_empty() {
        return None;
    }

    Some(tokio::spawn(async move {
        let interval = normalize_anti_entropy_interval(interval);
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        debug!("anti-entropy loop shutdown requested");
                        break;
                    }
                }
                _ = tokio::time::sleep(interval) => {
                    for peer in &peers {
                        if !node.is_connected_addr(peer).await {
                            continue;
                        }

                        let _last_seen_op_id = {
                            let peer_node_ids = peer_node_ids.read().await;
                            let Some(node_id) = peer_node_ids.get(peer) else {
                                continue;
                            };
                            anti_entropy_watermarks.read().await.get(node_id).cloned()
                        };

                        if let Err(e) = node.send_pull_since_to_addr(peer, None).await {
                            metrics.record_sync_error();
                            debug!(peer = %peer, error = %e, "anti-entropy pull failed");
                        } else {
                            debug!(peer = %peer, "anti-entropy pull requested");
                        }
                    }
                }
            }
        }
        debug!("anti-entropy loop terminated");
    }))
}

fn normalize_reconnect_delay(delay: Duration) -> Duration {
    delay.max(Duration::from_millis(1))
}

fn normalize_anti_entropy_interval(interval: Duration) -> Duration {
    interval.max(Duration::from_millis(1))
}

fn normalize_op_log_limit(limit: usize) -> usize {
    limit.max(1)
}

fn normalize_seen_ops_limit(limit: usize) -> usize {
    limit.max(1)
}

fn next_reconnect_delay(current: Duration, max_delay: Duration) -> Duration {
    current.saturating_mul(2).min(max_delay.max(current))
}

fn normalize_peer_dead_after_failures(failures: u32) -> u32 {
    failures.max(1)
}

fn record_peer_success(peer_health: &mut HashMap<String, PeerHealth>, peer: &str) {
    let health = peer_health.entry(peer.to_string()).or_default();
    health.state = PeerHealthState::Healthy;
    health.consecutive_failures = 0;
}

fn record_peer_failure(
    peer_health: &mut HashMap<String, PeerHealth>,
    peer: &str,
    dead_after_failures: u32,
) {
    let dead_after_failures = normalize_peer_dead_after_failures(dead_after_failures);
    let health = peer_health.entry(peer.to_string()).or_default();
    health.consecutive_failures = health.consecutive_failures.saturating_add(1);
    health.state = if health.consecutive_failures >= dead_after_failures {
        PeerHealthState::Dead
    } else {
        PeerHealthState::Suspect
    };
}

async fn mark_peer_success(peer_health: &Arc<RwLock<HashMap<String, PeerHealth>>>, peer: &str) {
    let mut peer_health = peer_health.write().await;
    record_peer_success(&mut peer_health, peer);
}

async fn mark_peer_failure(
    peer_health: &Arc<RwLock<HashMap<String, PeerHealth>>>,
    peer: &str,
    dead_after_failures: u32,
) {
    let mut peer_health = peer_health.write().await;
    record_peer_failure(&mut peer_health, peer, dead_after_failures);
}

async fn mark_known_peer_success(
    peer_health: &Arc<RwLock<HashMap<String, PeerHealth>>>,
    peer: &str,
) {
    let mut peer_health = peer_health.write().await;
    if peer_health.contains_key(peer) {
        record_peer_success(&mut peer_health, peer);
    }
}

async fn mark_known_peer_failure(
    peer_health: &Arc<RwLock<HashMap<String, PeerHealth>>>,
    peer: &str,
    dead_after_failures: u32,
) {
    let mut peer_health = peer_health.write().await;
    if peer_health.contains_key(peer) {
        record_peer_failure(&mut peer_health, peer, dead_after_failures);
    }
}

async fn broadcast_batch(
    context: &BroadcastLoopContext,
    op_rx: &mut mpsc::Receiver<Op>,
    first: Op,
) {
    let mut batch = Vec::with_capacity(BROADCAST_BATCH_MAX);
    batch.push(first);

    // Give producers a tiny window to fill the channel so sustained load is
    // sent in larger PushOps messages instead of many tiny network writes.
    tokio::time::sleep(BROADCAST_COALESCE_DELAY).await;

    while batch.len() < BROADCAST_BATCH_MAX {
        match op_rx.try_recv() {
            Ok(op) => batch.push(op),
            Err(_) => break,
        }
    }

    let count = batch.len();
    if let Err(e) = remember_ops(
        &context.seen_ops,
        &context.seen_ops_next_sequence,
        &context.op_log,
        &context.op_log_next_sequence,
        context.op_log_limit,
        &context.store,
        &batch,
    )
    .await
    {
        context.metrics.record_sync_error();
        warn!(error = %e, "failed to persist local op dedup metadata");
        return;
    }
    let started = std::time::Instant::now();
    if let Err(e) = context.node.broadcast_ops(batch).await {
        context.metrics.record_sync_error();
        warn!(error = %e, count, "broadcast partially failed; ops dropped for failed peers");
    } else {
        context.metrics.record_broadcast_batch(count);
        context.metrics.record_sync_latency(started.elapsed());
        debug!(count, "broadcast batch sent");
    }
}

async fn drain_broadcast_queue(context: &BroadcastLoopContext, op_rx: &mut mpsc::Receiver<Op>) {
    while let Ok(first) = op_rx.try_recv() {
        broadcast_batch(context, op_rx, first).await;
    }
}

async fn remember_ops(
    seen_ops: &Arc<RwLock<SeenOps>>,
    seen_ops_next_sequence: &Arc<AtomicU64>,
    op_log: &Arc<RwLock<Vec<Op>>>,
    op_log_next_sequence: &Arc<AtomicU64>,
    op_log_limit: usize,
    store: &Arc<NxStore>,
    ops: &[Op],
) -> anyhow::Result<()> {
    let mut seen = seen_ops.write().await;
    let mut log = op_log.write().await;
    let mut next_seen_sequence = seen_ops_next_sequence.load(Ordering::Relaxed);
    let mut next_op_log_sequence = op_log_next_sequence.load(Ordering::Relaxed);
    let mut inserted_ops = Vec::new();
    let mut inserted_ids = Vec::new();
    let mut inserts = Vec::new();
    for op in ops {
        let op_id = op.id.as_str();
        if !seen.ids.contains(op_id) && !inserted_ids.iter().any(|id| id == op_id) {
            inserted_ids.push(op_id.to_string());
            inserted_ops.push(op.clone());
        }
    }

    let seen_evicted = plan_seen_evictions(&seen, &inserted_ids);
    let op_log_evicted = plan_op_log_evictions(&log, inserted_ops.len(), op_log_limit);
    let last_insert_index = inserted_ops.len().saturating_sub(1);

    for (index, op) in inserted_ops.iter().enumerate() {
        inserts.push(OpPersistencePlan {
            op: op.clone(),
            seen_sequence: next_seen_sequence,
            op_log_sequence: next_op_log_sequence,
            seen_evicted: if index == last_insert_index {
                seen_evicted.clone()
            } else {
                Vec::new()
            },
            op_log_evicted: if index == last_insert_index {
                op_log_evicted.clone()
            } else {
                Vec::new()
            },
        });
        next_seen_sequence = next_seen_sequence.saturating_add(1);
        next_op_log_sequence = next_op_log_sequence.saturating_add(1);
    }

    persist_local_ops_batch(store, &inserts)?;
    apply_seen_insertions(&mut seen, &inserted_ids, &seen_evicted);
    log.extend(inserted_ops);
    prune_op_log_and_return_evicted(&mut log, op_log_limit);
    seen_ops_next_sequence.store(next_seen_sequence, Ordering::Relaxed);
    op_log_next_sequence.store(next_op_log_sequence, Ordering::Relaxed);
    Ok(())
}

fn plan_seen_evictions(seen: &SeenOps, inserted_ids: &[String]) -> Vec<String> {
    let total_len = seen.order.len().saturating_add(inserted_ids.len());
    let remove_count = total_len.saturating_sub(seen.limit);
    if remove_count == 0 {
        return Vec::new();
    }

    let mut evicted = seen
        .order
        .iter()
        .take(remove_count)
        .cloned()
        .collect::<Vec<_>>();
    if remove_count > seen.order.len() {
        evicted.extend(
            inserted_ids
                .iter()
                .take(remove_count - seen.order.len())
                .cloned(),
        );
    }
    evicted
}

fn apply_seen_insertions(seen: &mut SeenOps, inserted_ids: &[String], evicted: &[String]) {
    for op_id in inserted_ids {
        seen.ids.insert(op_id.clone());
        seen.order.push_back(op_id.clone());
    }
    for op_id in evicted {
        seen.ids.remove(op_id);
        let _ = seen.order.pop_front();
    }
}

fn plan_op_log_evictions(log: &[Op], inserted_count: usize, limit: usize) -> Vec<Op> {
    let limit = normalize_op_log_limit(limit);
    let total_len = log.len().saturating_add(inserted_count);
    let remove_count = total_len.saturating_sub(limit);
    log.iter().take(remove_count).cloned().collect()
}

fn prune_op_log_and_return_evicted(log: &mut Vec<Op>, limit: usize) -> Vec<Op> {
    let limit = normalize_op_log_limit(limit);
    if log.len() > limit {
        let remove_count = log.len() - limit;
        log.drain(..remove_count).collect()
    } else {
        Vec::new()
    }
}

async fn op_log_since(op_log: &Arc<RwLock<Vec<Op>>>, since_op_id: Option<&str>) -> Vec<Op> {
    let log = op_log.read().await;
    match since_op_id {
        Some(op_id) => log
            .iter()
            .position(|op| op.id.as_str() == op_id)
            .map_or_else(|| log.clone(), |index| log[index + 1..].to_vec()),
        None => log.clone(),
    }
}

async fn drain_node_events(event_rx: &mut mpsc::Receiver<NodeEvent>, context: &NodeEventContext) {
    while let Ok(event) = event_rx.try_recv() {
        handle_node_event(event, context).await;
    }
}

async fn handle_node_event(event: NodeEvent, context: &NodeEventContext) {
    match event {
        NodeEvent::OpsReceived { from, ops } => {
            debug!(from = %from, count = ops.len(), "received ops from peer");
            let last_received_op_id = ops.last().map(|op| op.id.as_str().to_string());
            let apply_context = RemoteOpApplyContext {
                counters: &context.counters,
                pncounters: &context.pncounters,
                lww_registers: &context.lww_registers,
                seen_ops: &context.seen_ops,
                seen_ops_next_sequence: &context.seen_ops_next_sequence,
                op_log: &context.op_log,
                op_log_next_sequence: &context.op_log_next_sequence,
                op_log_limit: context.op_log_limit,
                store: &context.store,
                metrics: &context.metrics,
            };
            if let Err(e) = apply_remote_ops(&ops, &apply_context).await {
                context.metrics.record_sync_error();
                error!(error = %e, "failed to apply remote ops batch");
                return;
            }
            if let Some(op_id) = last_received_op_id {
                context
                    .anti_entropy_watermarks
                    .write()
                    .await
                    .insert(from, op_id);
            }
        }
        NodeEvent::PullRequested {
            from,
            addr,
            since_op_id,
        } => {
            let ops = op_log_since(&context.op_log, since_op_id.as_deref()).await;
            let count = ops.len();
            if let Err(e) = context.node.send_ops_to_addr(&addr, ops).await {
                context.metrics.record_sync_error();
                warn!(peer = %from, addr = %addr, error = %e, "failed to answer pull request");
            } else {
                debug!(peer = %from, addr = %addr, count, "answered pull request");
            }
        }
        NodeEvent::PeerConnected {
            node_id,
            addr,
            peers_connected,
        } => {
            mark_known_peer_success(&context.peer_health, &addr).await;
            context
                .peer_node_ids
                .write()
                .await
                .insert(addr.clone(), node_id.clone());
            context.metrics.record_peer_connect();
            context.metrics.set_peers_connected(peers_connected);
            info!(peer = %node_id, addr = %addr, "peer connected");
        }
        NodeEvent::PeerDisconnected {
            node_id,
            addr,
            peers_connected,
        } => {
            mark_known_peer_failure(
                &context.peer_health,
                &addr,
                context.peer_dead_after_failures,
            )
            .await;
            context.peer_node_ids.write().await.remove(&addr);
            context.metrics.record_peer_disconnect();
            context.metrics.set_peers_connected(peers_connected);
            info!(peer = %node_id, addr = %addr, "peer disconnected");
        }
    }
}

pub(crate) fn materialized_gcounter_key(key: &str) -> Vec<u8> {
    crdt_store_key(CrdtKind::GCounter, CrdtStoreNamespace::Materialized, key)
}

fn durable_gcounter_state_key(key: &str) -> Vec<u8> {
    crdt_store_key(CrdtKind::GCounter, CrdtStoreNamespace::State, key)
}

pub(crate) fn materialized_pncounter_key(key: &str) -> Vec<u8> {
    crdt_store_key(CrdtKind::PNCounter, CrdtStoreNamespace::Materialized, key)
}

fn durable_pncounter_state_key(key: &str) -> Vec<u8> {
    crdt_store_key(CrdtKind::PNCounter, CrdtStoreNamespace::State, key)
}

pub(crate) fn materialized_lww_register_key(key: &str) -> Vec<u8> {
    crdt_store_key(CrdtKind::LwwRegister, CrdtStoreNamespace::Materialized, key)
}

fn durable_lww_register_state_key(key: &str) -> Vec<u8> {
    crdt_store_key(CrdtKind::LwwRegister, CrdtStoreNamespace::State, key)
}

fn seen_op_store_key(op_id: &str) -> Vec<u8> {
    prefixed_store_key(SEEN_OP_STORE_PREFIX, op_id)
}

fn op_log_store_key(op_id: &str) -> Vec<u8> {
    prefixed_store_key(OP_LOG_STORE_PREFIX, op_id)
}

fn crdt_store_key(kind: CrdtKind, namespace: CrdtStoreNamespace, key: &str) -> Vec<u8> {
    prefixed_store_key(kind.prefix(namespace), key)
}

fn prefixed_store_key(prefix: &str, key: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(prefix.len() + key.len());
    out.extend_from_slice(prefix.as_bytes());
    out.extend_from_slice(key.as_bytes());
    out
}

fn logical_gcounter_key(store_key: &[u8]) -> anyhow::Result<String> {
    logical_crdt_key(
        store_key,
        CrdtKind::GCounter,
        CrdtStoreNamespace::Materialized,
    )
}

fn logical_gcounter_state_key(store_key: &[u8]) -> anyhow::Result<String> {
    logical_crdt_key(store_key, CrdtKind::GCounter, CrdtStoreNamespace::State)
}

fn logical_pncounter_key(store_key: &[u8]) -> anyhow::Result<String> {
    logical_crdt_key(
        store_key,
        CrdtKind::PNCounter,
        CrdtStoreNamespace::Materialized,
    )
}

fn logical_pncounter_state_key(store_key: &[u8]) -> anyhow::Result<String> {
    logical_crdt_key(store_key, CrdtKind::PNCounter, CrdtStoreNamespace::State)
}

fn logical_lww_register_state_key(store_key: &[u8]) -> anyhow::Result<String> {
    logical_crdt_key(store_key, CrdtKind::LwwRegister, CrdtStoreNamespace::State)
}

fn logical_seen_op_id(store_key: &[u8]) -> anyhow::Result<String> {
    logical_key_for_prefix(store_key, SEEN_OP_STORE_PREFIX, "seen OpId")
}

fn logical_op_log_id(store_key: &[u8]) -> anyhow::Result<String> {
    logical_key_for_prefix(store_key, OP_LOG_STORE_PREFIX, "op log OpId")
}

fn logical_crdt_key(
    store_key: &[u8],
    kind: CrdtKind,
    namespace: CrdtStoreNamespace,
) -> anyhow::Result<String> {
    let namespace_label = match namespace {
        CrdtStoreNamespace::Materialized => "materialized",
        CrdtStoreNamespace::State => "durable state",
    };
    logical_key_for_prefix(
        store_key,
        kind.prefix(namespace),
        &format!("{namespace_label} {}", kind.label()),
    )
}

fn logical_key_for_prefix(store_key: &[u8], prefix: &str, kind: &str) -> anyhow::Result<String> {
    let key = store_key
        .strip_prefix(prefix.as_bytes())
        .ok_or_else(|| anyhow::anyhow!("invalid {kind} key"))?;

    String::from_utf8(key.to_vec()).map_err(|e| anyhow::anyhow!("invalid UTF-8 in {kind} key: {e}"))
}

fn parse_seen_op_sequence(bytes: &[u8]) -> anyhow::Result<u64> {
    let buf: [u8; 8] = bytes.try_into().map_err(|_| {
        anyhow::anyhow!(
            "invalid seen OpId sequence length: expected 8, got {}",
            bytes.len()
        )
    })?;

    Ok(u64::from_be_bytes(buf))
}

fn parse_materialized_gcounter_value(bytes: &[u8]) -> anyhow::Result<u64> {
    let buf: [u8; 8] = bytes.try_into().map_err(|_| {
        anyhow::anyhow!(
            "invalid materialized GCounter value length: expected 8, got {}",
            bytes.len()
        )
    })?;

    Ok(u64::from_le_bytes(buf))
}

fn parse_materialized_pncounter_value(bytes: &[u8]) -> anyhow::Result<i64> {
    let buf: [u8; 8] = bytes.try_into().map_err(|_| {
        anyhow::anyhow!(
            "invalid materialized PNCounter value length: expected 8, got {}",
            bytes.len()
        )
    })?;

    Ok(i64::from_le_bytes(buf))
}

fn encode_durable_op_log_value(sequence: u64, op: &Op) -> anyhow::Result<Vec<u8>> {
    let op_bytes = op.to_bytes()?;
    let mut out = Vec::with_capacity(8 + op_bytes.len());
    out.extend_from_slice(&sequence.to_be_bytes());
    out.extend_from_slice(&op_bytes);
    Ok(out)
}

fn parse_durable_op_log_value(bytes: &[u8]) -> anyhow::Result<(u64, Op)> {
    if bytes.len() < 8 {
        anyhow::bail!(
            "invalid op log value length: expected at least 8, got {}",
            bytes.len()
        );
    }
    let mut seq = [0u8; 8];
    seq.copy_from_slice(&bytes[..8]);
    let op = Op::from_bytes(&bytes[8..])
        .map_err(|e| anyhow::anyhow!("invalid durable op log JSON: {e}"))?;
    Ok((u64::from_be_bytes(seq), op))
}

fn hydrate_gcounter_registry(
    store: &NxStore,
    node_id: &NodeId,
    counters: &mut HashMap<String, GCounter>,
) -> anyhow::Result<usize> {
    let durable_count = hydrate_durable_gcounter_state(store, counters)?;
    let materialized_count = hydrate_materialized_gcounter_values(store, node_id, counters)?;

    debug!(
        durable_count,
        materialized_count,
        total = counters.len(),
        "hydrated GCounter registry from sled"
    );
    Ok(counters.len())
}

fn hydrate_pncounter_registry(
    store: &NxStore,
    node_id: &NodeId,
    counters: &mut HashMap<String, PNCounter>,
) -> anyhow::Result<usize> {
    let durable_count = hydrate_durable_pncounter_state(store, counters)?;
    let materialized_count = hydrate_materialized_pncounter_values(store, node_id, counters)?;

    debug!(
        durable_count,
        materialized_count,
        total = counters.len(),
        "hydrated PNCounter registry from sled"
    );
    Ok(counters.len())
}

fn hydrate_lww_register_registry(
    store: &NxStore,
    registers: &mut HashMap<String, LwwRegister>,
) -> anyhow::Result<usize> {
    let durable_count = hydrate_durable_lww_register_state(store, registers)?;

    debug!(
        durable_count,
        total = registers.len(),
        "hydrated LWW-Register registry from sled"
    );
    Ok(registers.len())
}

fn hydrate_seen_ops(store: &NxStore, limit: usize) -> anyhow::Result<(SeenOps, u64)> {
    let limit = normalize_seen_ops_limit(limit);
    let entries = store.scan_prefix(SEEN_OP_STORE_PREFIX.as_bytes())?;
    let mut ordered = Vec::with_capacity(entries.len());
    let mut next_sequence = 0;

    for (store_key, value_bytes) in entries {
        let op_id = match logical_seen_op_id(&store_key) {
            Ok(op_id) => op_id,
            Err(e) => {
                warn!(error = %e, "skipping invalid seen OpId key");
                continue;
            }
        };
        let sequence = match parse_seen_op_sequence(&value_bytes) {
            Ok(sequence) => sequence,
            Err(e) => {
                warn!(op_id = %op_id, error = %e, "skipping invalid seen OpId metadata");
                continue;
            }
        };

        next_sequence = next_sequence.max(sequence.saturating_add(1));
        ordered.push((sequence, op_id));
    }

    ordered.sort_by_key(|(sequence, _)| *sequence);
    let mut seen = SeenOps::new(limit);
    let mut evicted = Vec::new();
    for (_, op_id) in ordered {
        if let SeenOpsInsert::Inserted {
            evicted: evicted_ids,
        } = seen.insert(op_id)
        {
            evicted.extend(evicted_ids);
        }
    }
    persist_seen_op_evictions(store, &evicted)?;

    debug!(
        count = seen.len(),
        next_sequence, "hydrated seen OpId metadata from sled"
    );
    Ok((seen, next_sequence))
}

fn hydrate_op_log(store: &NxStore, limit: usize) -> anyhow::Result<(Vec<Op>, u64)> {
    let limit = normalize_op_log_limit(limit);
    let entries = store.scan_prefix(OP_LOG_STORE_PREFIX.as_bytes())?;
    let mut ordered = Vec::with_capacity(entries.len());
    let mut next_sequence = 0;

    for (store_key, value_bytes) in entries {
        let op_id = match logical_op_log_id(&store_key) {
            Ok(op_id) => op_id,
            Err(e) => {
                warn!(error = %e, "skipping invalid durable op log key");
                continue;
            }
        };
        let (sequence, op) = match parse_durable_op_log_value(&value_bytes) {
            Ok(value) => value,
            Err(e) => {
                warn!(op_id = %op_id, error = %e, "skipping invalid durable op log entry");
                continue;
            }
        };
        if op.id.as_str() != op_id {
            warn!(
                key_op_id = %op_id,
                value_op_id = %op.id,
                "skipping durable op log entry with mismatched OpId"
            );
            continue;
        }

        next_sequence = next_sequence.max(sequence.saturating_add(1));
        ordered.push((sequence, op));
    }

    ordered.sort_by_key(|(sequence, _)| *sequence);
    let mut op_log = ordered.into_iter().map(|(_, op)| op).collect::<Vec<_>>();
    let evicted = prune_op_log_and_return_evicted(&mut op_log, limit);
    persist_op_log_evictions(store, &evicted)?;

    debug!(
        count = op_log.len(),
        next_sequence, "hydrated durable op log from sled"
    );
    Ok((op_log, next_sequence))
}

fn hydrate_durable_gcounter_state(
    store: &NxStore,
    counters: &mut HashMap<String, GCounter>,
) -> anyhow::Result<usize> {
    let entries = store.scan_prefix(GCOUNTER_STATE_STORE_PREFIX.as_bytes())?;
    let mut hydrated = 0;

    for (store_key, value_bytes) in entries {
        let key = match logical_gcounter_state_key(&store_key) {
            Ok(key) => key,
            Err(e) => {
                warn!(error = %e, "skipping invalid durable GCounter state key");
                continue;
            }
        };
        let counter = match parse_durable_gcounter_state(&value_bytes) {
            Ok(counter) => counter,
            Err(e) => {
                warn!(key = %key, error = %e, "skipping invalid durable GCounter state");
                continue;
            }
        };

        materialize_gcounter_value(store, &key, counter.value())?;
        counters.insert(key, counter);
        hydrated += 1;
    }

    Ok(hydrated)
}

fn hydrate_durable_pncounter_state(
    store: &NxStore,
    counters: &mut HashMap<String, PNCounter>,
) -> anyhow::Result<usize> {
    let entries = store.scan_prefix(PNCOUNTER_STATE_STORE_PREFIX.as_bytes())?;
    let mut hydrated = 0;

    for (store_key, value_bytes) in entries {
        let key = match logical_pncounter_state_key(&store_key) {
            Ok(key) => key,
            Err(e) => {
                warn!(error = %e, "skipping invalid durable PNCounter state key");
                continue;
            }
        };
        let counter = match parse_durable_pncounter_state(&value_bytes) {
            Ok(counter) => counter,
            Err(e) => {
                warn!(key = %key, error = %e, "skipping invalid durable PNCounter state");
                continue;
            }
        };

        materialize_pncounter_value(store, &key, counter.value())?;
        counters.insert(key, counter);
        hydrated += 1;
    }

    Ok(hydrated)
}

fn hydrate_durable_lww_register_state(
    store: &NxStore,
    registers: &mut HashMap<String, LwwRegister>,
) -> anyhow::Result<usize> {
    let entries = store.scan_prefix(LWW_REGISTER_STATE_STORE_PREFIX.as_bytes())?;
    let mut hydrated = 0;

    for (store_key, value_bytes) in entries {
        let key = match logical_lww_register_state_key(&store_key) {
            Ok(key) => key,
            Err(e) => {
                warn!(error = %e, "skipping invalid durable LWW-Register state key");
                continue;
            }
        };
        let register = match parse_durable_lww_register_state(&value_bytes) {
            Ok(register) => register,
            Err(e) => {
                warn!(key = %key, error = %e, "skipping invalid durable LWW-Register state");
                continue;
            }
        };

        materialize_lww_register_value(store, &key, register.value())?;
        registers.insert(key, register);
        hydrated += 1;
    }

    Ok(hydrated)
}

fn hydrate_materialized_gcounter_values(
    store: &NxStore,
    node_id: &NodeId,
    counters: &mut HashMap<String, GCounter>,
) -> anyhow::Result<usize> {
    let entries = store.scan_prefix(GCOUNTER_STORE_PREFIX.as_bytes())?;
    let mut hydrated = 0;

    for (store_key, value_bytes) in entries {
        let key = match logical_gcounter_key(&store_key) {
            Ok(key) => key,
            Err(e) => {
                warn!(error = %e, "skipping invalid materialized GCounter key");
                continue;
            }
        };
        let value = match parse_materialized_gcounter_value(&value_bytes) {
            Ok(value) => value,
            Err(e) => {
                warn!(key = %key, error = %e, "skipping invalid materialized GCounter value");
                continue;
            }
        };
        if counters.contains_key(&key) {
            continue;
        }

        let mut counter = GCounter::new();
        counter.increment(node_id, value);
        counters.insert(key, counter);
        hydrated += 1;
    }

    Ok(hydrated)
}

fn hydrate_materialized_pncounter_values(
    store: &NxStore,
    node_id: &NodeId,
    counters: &mut HashMap<String, PNCounter>,
) -> anyhow::Result<usize> {
    let entries = store.scan_prefix(PNCOUNTER_STORE_PREFIX.as_bytes())?;
    let mut hydrated = 0;

    for (store_key, value_bytes) in entries {
        let key = match logical_pncounter_key(&store_key) {
            Ok(key) => key,
            Err(e) => {
                warn!(error = %e, "skipping invalid materialized PNCounter key");
                continue;
            }
        };
        let value = match parse_materialized_pncounter_value(&value_bytes) {
            Ok(value) => value,
            Err(e) => {
                warn!(key = %key, error = %e, "skipping invalid materialized PNCounter value");
                continue;
            }
        };
        if counters.contains_key(&key) {
            continue;
        }

        let mut counter = PNCounter::new();
        if value >= 0 {
            counter.increment(node_id, value as u64);
        } else {
            counter.decrement(node_id, value.unsigned_abs());
        }
        counters.insert(key, counter);
        hydrated += 1;
    }

    Ok(hydrated)
}

pub(crate) fn materialize_gcounter_value(
    store: &NxStore,
    key: &str,
    value: u64,
) -> anyhow::Result<()> {
    let store_key = materialized_gcounter_key(key);
    store.set(&store_key, &value.to_le_bytes())?;
    Ok(())
}

pub(crate) fn materialize_pncounter_value(
    store: &NxStore,
    key: &str,
    value: i64,
) -> anyhow::Result<()> {
    let store_key = materialized_pncounter_key(key);
    store.set(&store_key, &value.to_le_bytes())?;
    Ok(())
}

pub(crate) fn materialize_lww_register_value(
    store: &NxStore,
    key: &str,
    value: &[u8],
) -> anyhow::Result<()> {
    let store_key = materialized_lww_register_key(key);
    store.set(&store_key, value)?;
    Ok(())
}

fn parse_durable_gcounter_state(bytes: &[u8]) -> anyhow::Result<GCounter> {
    let json = std::str::from_utf8(bytes)
        .map_err(|e| anyhow::anyhow!("invalid UTF-8 in durable GCounter state: {e}"))?;
    GCounter::from_json(json).map_err(|e| anyhow::anyhow!("invalid durable GCounter JSON: {e}"))
}

fn parse_durable_pncounter_state(bytes: &[u8]) -> anyhow::Result<PNCounter> {
    let json = std::str::from_utf8(bytes)
        .map_err(|e| anyhow::anyhow!("invalid UTF-8 in durable PNCounter state: {e}"))?;
    PNCounter::from_json(json).map_err(|e| anyhow::anyhow!("invalid durable PNCounter JSON: {e}"))
}

fn parse_durable_lww_register_state(bytes: &[u8]) -> anyhow::Result<LwwRegister> {
    let json = std::str::from_utf8(bytes)
        .map_err(|e| anyhow::anyhow!("invalid UTF-8 in durable LWW-Register state: {e}"))?;
    LwwRegister::from_json(json)
        .map_err(|e| anyhow::anyhow!("invalid durable LWW-Register JSON: {e}"))
}

pub(crate) fn persist_gcounter_state(
    store: &NxStore,
    key: &str,
    counter: &GCounter,
) -> anyhow::Result<()> {
    let store_key = durable_gcounter_state_key(key);
    let state_json = counter.to_json()?;
    store.set(&store_key, state_json.as_bytes())?;
    materialize_gcounter_value(store, key, counter.value())?;
    Ok(())
}

pub(crate) fn persist_pncounter_state(
    store: &NxStore,
    key: &str,
    counter: &PNCounter,
) -> anyhow::Result<()> {
    let store_key = durable_pncounter_state_key(key);
    let state_json = counter.to_json()?;
    store.set(&store_key, state_json.as_bytes())?;
    materialize_pncounter_value(store, key, counter.value())?;
    Ok(())
}

pub(crate) fn persist_lww_register_state(
    store: &NxStore,
    key: &str,
    register: &LwwRegister,
) -> anyhow::Result<()> {
    let store_key = durable_lww_register_state_key(key);
    let state_json = register.to_json()?;
    store.set(&store_key, state_json.as_bytes())?;
    materialize_lww_register_value(store, key, register.value())?;
    Ok(())
}

#[cfg(test)]
fn persist_seen_op_batch(
    store: &NxStore,
    op_id: &str,
    sequence: u64,
    evicted: &[String],
) -> anyhow::Result<()> {
    let seen_key = seen_op_store_key(op_id);
    let seen_value = sequence.to_be_bytes();
    let delete_keys = evicted
        .iter()
        .map(|op_id| seen_op_store_key(op_id))
        .collect::<Vec<_>>();
    let delete_refs = delete_keys
        .iter()
        .map(|key| key.as_slice())
        .collect::<Vec<_>>();

    store.apply_batch(
        &[(seen_key.as_slice(), seen_value.as_slice())],
        &delete_refs,
    )?;
    Ok(())
}

fn collect_seen_delete_keys(evicted: &[String]) -> Vec<Vec<u8>> {
    evicted
        .iter()
        .map(|op_id| seen_op_store_key(op_id))
        .collect()
}

fn collect_op_log_delete_keys(evicted: &[Op]) -> Vec<Vec<u8>> {
    evicted
        .iter()
        .map(|op| op_log_store_key(op.id.as_str()))
        .collect()
}

fn persist_seen_op_evictions(store: &NxStore, evicted: &[String]) -> anyhow::Result<()> {
    if evicted.is_empty() {
        return Ok(());
    }

    let delete_keys = evicted
        .iter()
        .map(|op_id| seen_op_store_key(op_id))
        .collect::<Vec<_>>();
    let delete_refs = delete_keys
        .iter()
        .map(|key| key.as_slice())
        .collect::<Vec<_>>();
    store.apply_batch(&[], &delete_refs)?;
    Ok(())
}

fn persist_op_log_evictions(store: &NxStore, evicted: &[Op]) -> anyhow::Result<()> {
    if evicted.is_empty() {
        return Ok(());
    }

    let delete_keys = collect_op_log_delete_keys(evicted);
    let delete_refs = delete_keys
        .iter()
        .map(|key| key.as_slice())
        .collect::<Vec<_>>();
    store.apply_batch(&[], &delete_refs)?;
    Ok(())
}

fn persist_local_ops_batch(store: &NxStore, plans: &[OpPersistencePlan]) -> anyhow::Result<()> {
    if plans.is_empty() {
        return Ok(());
    }

    let mut set_keys = Vec::new();
    let mut set_values = Vec::new();
    let mut delete_keys = Vec::new();

    for plan in plans {
        set_keys.push(seen_op_store_key(plan.op.id.as_str()));
        set_values.push(plan.seen_sequence.to_be_bytes().to_vec());
        set_keys.push(op_log_store_key(plan.op.id.as_str()));
        set_values.push(encode_durable_op_log_value(plan.op_log_sequence, &plan.op)?);
        delete_keys.extend(collect_seen_delete_keys(&plan.seen_evicted));
        delete_keys.extend(collect_op_log_delete_keys(&plan.op_log_evicted));
    }

    let sets = set_keys
        .iter()
        .zip(set_values.iter())
        .map(|(key, value)| (key.as_slice(), value.as_slice()))
        .collect::<Vec<_>>();
    let deletes = delete_keys
        .iter()
        .map(|key| key.as_slice())
        .collect::<Vec<_>>();

    store.apply_batch(&sets, &deletes)?;
    Ok(())
}

fn persist_remote_ops_batch(
    store: &NxStore,
    plans: &[OpPersistencePlan],
    counter_updates: &HashMap<String, GCounter>,
    pncounter_updates: &HashMap<String, PNCounter>,
    lww_register_updates: &HashMap<String, LwwRegister>,
) -> anyhow::Result<()> {
    if plans.is_empty() {
        return Ok(());
    }

    let mut set_keys = Vec::new();
    let mut set_values = Vec::new();
    let mut delete_keys = Vec::new();
    let mut changed_counter_keys = counter_updates.keys().collect::<Vec<_>>();
    changed_counter_keys.sort();

    for key in changed_counter_keys {
        let Some(counter) = counter_updates.get(key.as_str()) else {
            continue;
        };
        set_keys.push(durable_gcounter_state_key(key));
        set_values.push(counter.to_json()?.into_bytes());
        set_keys.push(materialized_gcounter_key(key));
        set_values.push(counter.value().to_le_bytes().to_vec());
    }

    let mut changed_pncounter_keys = pncounter_updates.keys().collect::<Vec<_>>();
    changed_pncounter_keys.sort();

    for key in changed_pncounter_keys {
        let Some(counter) = pncounter_updates.get(key.as_str()) else {
            continue;
        };
        set_keys.push(durable_pncounter_state_key(key));
        set_values.push(counter.to_json()?.into_bytes());
        set_keys.push(materialized_pncounter_key(key));
        set_values.push(counter.value().to_le_bytes().to_vec());
    }

    let mut changed_lww_register_keys = lww_register_updates.keys().collect::<Vec<_>>();
    changed_lww_register_keys.sort();

    for key in changed_lww_register_keys {
        let Some(register) = lww_register_updates.get(key.as_str()) else {
            continue;
        };
        set_keys.push(durable_lww_register_state_key(key));
        set_values.push(register.to_json()?.into_bytes());
        set_keys.push(materialized_lww_register_key(key));
        set_values.push(register.value_bytes());
    }

    for plan in plans {
        set_keys.push(seen_op_store_key(plan.op.id.as_str()));
        set_values.push(plan.seen_sequence.to_be_bytes().to_vec());
        set_keys.push(op_log_store_key(plan.op.id.as_str()));
        set_values.push(encode_durable_op_log_value(plan.op_log_sequence, &plan.op)?);
        delete_keys.extend(collect_seen_delete_keys(&plan.seen_evicted));
        delete_keys.extend(collect_op_log_delete_keys(&plan.op_log_evicted));
    }

    let sets = set_keys
        .iter()
        .zip(set_values.iter())
        .map(|(key, value)| (key.as_slice(), value.as_slice()))
        .collect::<Vec<_>>();
    let deletes = delete_keys
        .iter()
        .map(|key| key.as_slice())
        .collect::<Vec<_>>();

    store.apply_batch(&sets, &deletes)?;
    Ok(())
}

/// Apply operations received from a remote peer as a single commit-aware batch.
async fn apply_remote_ops(ops: &[Op], context: &RemoteOpApplyContext<'_>) -> anyhow::Result<()> {
    if ops.is_empty() {
        return Ok(());
    }

    let mut seen = context.seen_ops.write().await;
    let mut log = context.op_log.write().await;
    let mut counters = context.counters.write().await;
    let mut pncounters = context.pncounters.write().await;
    let mut lww_registers = context.lww_registers.write().await;
    let mut next_seen_sequence = context.seen_ops_next_sequence.load(Ordering::Relaxed);
    let mut next_op_log_sequence = context.op_log_next_sequence.load(Ordering::Relaxed);
    let mut inserted_ids = Vec::new();
    let mut inserted_ops = Vec::new();
    let mut plans = Vec::new();
    let mut counter_updates = HashMap::new();
    let mut pncounter_updates = HashMap::new();
    let mut lww_register_updates = HashMap::new();

    for op in ops {
        let op_id = op.id.as_str();
        if seen.ids.contains(op_id) || inserted_ids.iter().any(|id| id == op_id) {
            debug!(op_id = %op.id, "skipping duplicate op");
            continue;
        }

        apply_remote_op_to_counter_updates(
            op,
            &counters,
            &mut counter_updates,
            &pncounters,
            &mut pncounter_updates,
            &lww_registers,
            &mut lww_register_updates,
        );

        inserted_ids.push(op_id.to_string());
        inserted_ops.push(op.clone());
    }

    let seen_evicted = plan_seen_evictions(&seen, &inserted_ids);
    let op_log_evicted = plan_op_log_evictions(&log, inserted_ops.len(), context.op_log_limit);
    let last_insert_index = inserted_ops.len().saturating_sub(1);

    for (index, op) in inserted_ops.iter().enumerate() {
        plans.push(OpPersistencePlan {
            op: op.clone(),
            seen_sequence: next_seen_sequence,
            op_log_sequence: next_op_log_sequence,
            seen_evicted: if index == last_insert_index {
                seen_evicted.clone()
            } else {
                Vec::new()
            },
            op_log_evicted: if index == last_insert_index {
                op_log_evicted.clone()
            } else {
                Vec::new()
            },
        });
        next_seen_sequence = next_seen_sequence.saturating_add(1);
        next_op_log_sequence = next_op_log_sequence.saturating_add(1);
    }

    persist_remote_ops_batch(
        context.store,
        &plans,
        &counter_updates,
        &pncounter_updates,
        &lww_register_updates,
    )?;

    let applied_count = plans.len() as u64;
    apply_seen_insertions(&mut seen, &inserted_ids, &seen_evicted);
    log.extend(inserted_ops);
    prune_op_log_and_return_evicted(&mut log, context.op_log_limit);
    for (key, counter) in counter_updates {
        counters.insert(key, counter);
    }
    for (key, counter) in pncounter_updates {
        pncounters.insert(key, counter);
    }
    for (key, register) in lww_register_updates {
        lww_registers.insert(key, register);
    }
    context
        .seen_ops_next_sequence
        .store(next_seen_sequence, Ordering::Relaxed);
    context
        .op_log_next_sequence
        .store(next_op_log_sequence, Ordering::Relaxed);

    if applied_count > 0 {
        context.metrics.record_ops(applied_count);
        debug!(count = applied_count, "applied remote ops batch");
    }

    Ok(())
}

fn apply_remote_op_to_counter_updates(
    op: &Op,
    counters: &HashMap<String, GCounter>,
    counter_updates: &mut HashMap<String, GCounter>,
    pncounters: &HashMap<String, PNCounter>,
    pncounter_updates: &mut HashMap<String, PNCounter>,
    lww_registers: &HashMap<String, LwwRegister>,
    lww_register_updates: &mut HashMap<String, LwwRegister>,
) {
    match &op.kind {
        OpKind::GCounterIncrement { key, increment } => {
            apply_remote_gcounter_increment(op, key, *increment, counters, counter_updates);
        }
        OpKind::PNCounterIncrement { key, .. } | OpKind::PNCounterDecrement { key, .. } => {
            apply_remote_pncounter_op(op, key, pncounters, pncounter_updates);
        }
        OpKind::LwwRegisterSet {
            key,
            value,
            timestamp_ms,
        } => {
            apply_remote_lww_register_set(
                op,
                key,
                value,
                *timestamp_ms,
                lww_registers,
                lww_register_updates,
            );
        }
    }
}

fn apply_remote_gcounter_increment(
    op: &Op,
    key: &str,
    increment: u64,
    counters: &HashMap<String, GCounter>,
    counter_updates: &mut HashMap<String, GCounter>,
) {
    let counter = counter_updates
        .entry(key.to_string())
        .or_insert_with(|| counters.get(key).cloned().unwrap_or_else(GCounter::new));
    counter.increment(&op.origin, increment);
}

fn apply_remote_pncounter_op(
    op: &Op,
    key: &str,
    counters: &HashMap<String, PNCounter>,
    counter_updates: &mut HashMap<String, PNCounter>,
) {
    let counter = counter_updates
        .entry(key.to_string())
        .or_insert_with(|| counters.get(key).cloned().unwrap_or_else(PNCounter::new));
    let _ = counter.apply_op(op);
}

fn apply_remote_lww_register_set(
    op: &Op,
    key: &str,
    value: &[u8],
    timestamp_ms: u64,
    registers: &HashMap<String, LwwRegister>,
    register_updates: &mut HashMap<String, LwwRegister>,
) {
    if let Some(register) = register_updates.get_mut(key) {
        register.assign(value.to_vec(), timestamp_ms, op.origin.clone());
        return;
    }

    let Some(existing) = registers.get(key) else {
        register_updates.insert(
            key.to_string(),
            LwwRegister::new(value.to_vec(), timestamp_ms, op.origin.clone()),
        );
        return;
    };

    let mut register = existing.clone();
    if register.assign(value.to_vec(), timestamp_ms, op.origin.clone()) {
        register_updates.insert(key.to_string(), register);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{Runtime, RuntimeConfig};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::time::{Duration, Instant, sleep};

    fn temp_store() -> Arc<NxStore> {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        path.push(format!("numax-core-sync-test-{nanos}"));
        Arc::new(NxStore::open(path).unwrap())
    }

    fn metrics() -> Arc<RuntimeMetrics> {
        Arc::new(RuntimeMetrics::default())
    }

    fn free_addr() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        addr.to_string()
    }

    fn read_materialized(store: &NxStore, key: &str) -> u64 {
        let key = materialized_gcounter_key(key);
        let bytes = store.get(&key).unwrap().unwrap();
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&bytes);
        u64::from_le_bytes(buf)
    }

    fn read_materialized_pncounter(store: &NxStore, key: &str) -> i64 {
        let key = materialized_pncounter_key(key);
        let bytes = store.get(&key).unwrap().unwrap();
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&bytes);
        i64::from_le_bytes(buf)
    }

    fn read_durable_gcounter_state(store: &NxStore, key: &str) -> GCounter {
        let key = durable_gcounter_state_key(key);
        let bytes = store.get(&key).unwrap().unwrap();
        parse_durable_gcounter_state(&bytes).unwrap()
    }

    fn read_durable_pncounter_state(store: &NxStore, key: &str) -> PNCounter {
        let key = durable_pncounter_state_key(key);
        let bytes = store.get(&key).unwrap().unwrap();
        parse_durable_pncounter_state(&bytes).unwrap()
    }

    fn read_materialized_lww_register(store: &NxStore, key: &str) -> Vec<u8> {
        let key = materialized_lww_register_key(key);
        store.get(&key).unwrap().unwrap()
    }

    fn read_durable_lww_register_state(store: &NxStore, key: &str) -> LwwRegister {
        let key = durable_lww_register_state_key(key);
        let bytes = store.get(&key).unwrap().unwrap();
        parse_durable_lww_register_state(&bytes).unwrap()
    }

    fn seen_op_exists(store: &NxStore, op_id: &str) -> bool {
        store.get(&seen_op_store_key(op_id)).unwrap().is_some()
    }

    fn write_durable_op_log_entry(store: &NxStore, key_op_id: &str, sequence: u64, op: &Op) {
        let key = op_log_store_key(key_op_id);
        let value = encode_durable_op_log_value(sequence, op).unwrap();
        store.set(&key, &value).unwrap();
    }

    fn test_event_context(
        counters: Arc<RwLock<HashMap<String, GCounter>>>,
        seen_ops: Arc<RwLock<SeenOps>>,
        op_log: Arc<RwLock<Vec<Op>>>,
        store: Arc<NxStore>,
        metrics: Arc<RuntimeMetrics>,
        peer_health: Arc<RwLock<HashMap<String, PeerHealth>>>,
    ) -> NodeEventContext {
        NodeEventContext {
            counters,
            pncounters: Arc::new(RwLock::new(HashMap::new())),
            lww_registers: Arc::new(RwLock::new(HashMap::new())),
            seen_ops,
            seen_ops_next_sequence: Arc::new(AtomicU64::new(0)),
            op_log,
            op_log_next_sequence: Arc::new(AtomicU64::new(0)),
            op_log_limit: 1024,
            store,
            metrics,
            node: Arc::new(Node::new(NodeConfig::new(
                NodeId::generate(),
                "127.0.0.1:0",
            ))),
            peer_health,
            peer_node_ids: Arc::new(RwLock::new(HashMap::new())),
            anti_entropy_watermarks: Arc::new(RwLock::new(HashMap::new())),
            peer_dead_after_failures: 2,
        }
    }

    async fn apply_remote_op_for_test(
        op: &Op,
        counters: &Arc<RwLock<HashMap<String, GCounter>>>,
        seen_ops: &Arc<RwLock<SeenOps>>,
        seen_ops_next_sequence: &Arc<AtomicU64>,
        op_log: &Arc<RwLock<Vec<Op>>>,
        op_log_next_sequence: &Arc<AtomicU64>,
        store: &Arc<NxStore>,
    ) {
        let pncounters = Arc::new(RwLock::new(HashMap::new()));
        let lww_registers = Arc::new(RwLock::new(HashMap::new()));
        let metrics = metrics();
        let context = RemoteOpApplyContext {
            counters,
            pncounters: &pncounters,
            lww_registers: &lww_registers,
            seen_ops,
            seen_ops_next_sequence,
            op_log,
            op_log_next_sequence,
            op_log_limit: 1024,
            store,
            metrics: &metrics,
        };
        apply_remote_ops(std::slice::from_ref(op), &context)
            .await
            .unwrap();
    }

    async fn apply_remote_pncounter_op_for_test(
        op: &Op,
        pncounters: &Arc<RwLock<HashMap<String, PNCounter>>>,
        seen_ops: &Arc<RwLock<SeenOps>>,
        seen_ops_next_sequence: &Arc<AtomicU64>,
        op_log: &Arc<RwLock<Vec<Op>>>,
        op_log_next_sequence: &Arc<AtomicU64>,
        store: &Arc<NxStore>,
    ) {
        let counters = Arc::new(RwLock::new(HashMap::new()));
        let lww_registers = Arc::new(RwLock::new(HashMap::new()));
        let metrics = metrics();
        let context = RemoteOpApplyContext {
            counters: &counters,
            pncounters,
            lww_registers: &lww_registers,
            seen_ops,
            seen_ops_next_sequence,
            op_log,
            op_log_next_sequence,
            op_log_limit: 1024,
            store,
            metrics: &metrics,
        };
        apply_remote_ops(std::slice::from_ref(op), &context)
            .await
            .unwrap();
    }

    async fn apply_remote_lww_register_op_for_test(
        op: &Op,
        registers: &Arc<RwLock<HashMap<String, LwwRegister>>>,
        seen_ops: &Arc<RwLock<SeenOps>>,
        seen_ops_next_sequence: &Arc<AtomicU64>,
        op_log: &Arc<RwLock<Vec<Op>>>,
        op_log_next_sequence: &Arc<AtomicU64>,
        store: &Arc<NxStore>,
    ) {
        let counters = Arc::new(RwLock::new(HashMap::new()));
        let pncounters = Arc::new(RwLock::new(HashMap::new()));
        let metrics = metrics();
        let context = RemoteOpApplyContext {
            counters: &counters,
            pncounters: &pncounters,
            lww_registers: registers,
            seen_ops,
            seen_ops_next_sequence,
            op_log,
            op_log_next_sequence,
            op_log_limit: 1024,
            store,
            metrics: &metrics,
        };
        apply_remote_ops(std::slice::from_ref(op), &context)
            .await
            .unwrap();
    }

    async fn wait_for_counter(manager: &SyncManager, key: &str, expected: u64) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if manager.get_counter_value(key).await == expected {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "counter {key} did not reach {expected}"
            );
            sleep(Duration::from_millis(25)).await;
        }
    }

    async fn wait_for_pncounter(manager: &SyncManager, key: &str, expected: i64) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if manager.get_pncounter_value(key).await == expected {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "pncounter {key} did not reach {expected}"
            );
            sleep(Duration::from_millis(25)).await;
        }
    }

    async fn wait_for_lww_register(manager: &SyncManager, key: &str, expected: &[u8]) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if manager.get_lww_register_value(key).await.as_deref() == Some(expected) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "lww-register {key} did not reach expected value"
            );
            sleep(Duration::from_millis(25)).await;
        }
    }

    async fn wait_for_connected_peer(manager: &SyncManager) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if manager.connected_peer_count().await > 0 {
                return;
            }
            assert!(Instant::now() < deadline, "manager did not connect to peer");
            sleep(Duration::from_millis(25)).await;
        }
    }

    async fn wait_for_peer_health(manager: &SyncManager, peer: &str, expected: PeerHealthState) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if manager.peer_health_state(peer).await == Some(expected) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "peer {peer} did not reach health state {expected:?}"
            );
            sleep(Duration::from_millis(25)).await;
        }
    }

    async fn local_increment(handle: &SyncHandle, key: &str, delta: u64) {
        let op = apply_local_increment(handle, key, delta).await;
        handle.op_sender().send(op).await.unwrap();
    }

    async fn local_pncounter_inc(handle: &SyncHandle, key: &str, delta: u64) {
        let op = apply_local_pncounter_inc(handle, key, delta).await;
        handle.op_sender().send(op).await.unwrap();
    }

    async fn local_pncounter_dec(handle: &SyncHandle, key: &str, delta: u64) {
        let op = apply_local_pncounter_dec(handle, key, delta).await;
        handle.op_sender().send(op).await.unwrap();
    }

    async fn local_lww_register_set(
        handle: &SyncHandle,
        key: &str,
        value: &[u8],
        timestamp_ms: u64,
    ) {
        let op = apply_local_lww_register_set(handle, key, value, timestamp_ms).await;
        handle.op_sender().send(op).await.unwrap();
    }

    async fn dropped_local_increment(
        manager: &SyncManager,
        handle: &SyncHandle,
        key: &str,
        delta: u64,
    ) {
        let op = apply_local_increment(handle, key, delta).await;
        remember_ops(
            &manager.seen_ops,
            &manager.seen_ops_next_sequence,
            &manager.op_log,
            &manager.op_log_next_sequence,
            manager.config.op_log_limit,
            &manager.store,
            &[op],
        )
        .await
        .unwrap();
    }

    async fn apply_local_increment(handle: &SyncHandle, key: &str, delta: u64) -> Op {
        {
            let counters_arc = handle.counters();
            let mut counters = counters_arc.write().await;
            let mut counter = counters.get(key).cloned().unwrap_or_else(GCounter::new);
            counter.increment(handle.node_id(), delta);

            persist_gcounter_state(&handle.store(), key, &counter).unwrap();
            counters.insert(key.to_string(), counter);
        }

        Op::gcounter_increment(handle.node_id().clone(), key, delta)
    }

    async fn apply_local_pncounter_inc(handle: &SyncHandle, key: &str, delta: u64) -> Op {
        apply_local_pncounter_change(handle, key, delta, PNCounterChangeForTest::Increment).await
    }

    async fn apply_local_pncounter_dec(handle: &SyncHandle, key: &str, delta: u64) -> Op {
        apply_local_pncounter_change(handle, key, delta, PNCounterChangeForTest::Decrement).await
    }

    #[derive(Debug, Clone, Copy)]
    enum PNCounterChangeForTest {
        Increment,
        Decrement,
    }

    async fn apply_local_pncounter_change(
        handle: &SyncHandle,
        key: &str,
        delta: u64,
        change: PNCounterChangeForTest,
    ) -> Op {
        {
            let counters_arc = handle.pncounters();
            let mut counters = counters_arc.write().await;
            let mut counter = counters.get(key).cloned().unwrap_or_else(PNCounter::new);
            match change {
                PNCounterChangeForTest::Increment => counter.increment(handle.node_id(), delta),
                PNCounterChangeForTest::Decrement => counter.decrement(handle.node_id(), delta),
            }

            persist_pncounter_state(&handle.store(), key, &counter).unwrap();
            counters.insert(key.to_string(), counter);
        }

        match change {
            PNCounterChangeForTest::Increment => {
                Op::pncounter_increment(handle.node_id().clone(), key, delta)
            }
            PNCounterChangeForTest::Decrement => {
                Op::pncounter_decrement(handle.node_id().clone(), key, delta)
            }
        }
    }

    async fn apply_local_lww_register_set(
        handle: &SyncHandle,
        key: &str,
        value: &[u8],
        timestamp_ms: u64,
    ) -> Op {
        {
            let registers_arc = handle.lww_registers();
            let mut registers = registers_arc.write().await;
            let candidate =
                LwwRegister::new(value.to_vec(), timestamp_ms, handle.node_id().clone());
            let register = registers
                .entry(key.to_string())
                .or_insert_with(|| candidate.clone());
            register.merge(&candidate);

            persist_lww_register_state(&handle.store(), key, register).unwrap();
        }

        Op::lww_register_set(handle.node_id().clone(), key, value.to_vec(), timestamp_ms)
    }

    async fn started_manager(addr: String) -> (SyncManager, SyncHandle, Arc<NxStore>) {
        let store = temp_store();
        let config = SyncConfig::new().with_listen_addr(addr);
        let mut manager =
            SyncManager::new(NodeId::generate(), config, Arc::clone(&store), metrics());
        let handle = manager.handle();
        manager.start().await.unwrap();
        (manager, handle, store)
    }

    async fn started_manager_with_store(
        node_id: NodeId,
        config: SyncConfig,
        store: Arc<NxStore>,
    ) -> (SyncManager, SyncHandle) {
        let mut manager = SyncManager::new(node_id, config, store, metrics());
        let handle = manager.handle();
        manager.start().await.unwrap();
        (manager, handle)
    }

    async fn started_manager_with_config(
        config: SyncConfig,
    ) -> (SyncManager, SyncHandle, Arc<NxStore>) {
        let store = temp_store();
        let mut manager =
            SyncManager::new(NodeId::generate(), config, Arc::clone(&store), metrics());
        let handle = manager.handle();
        manager.start().await.unwrap();
        (manager, handle, store)
    }

    #[test]
    fn crdt_store_keys_roundtrip_through_generic_namespace_helpers() {
        let materialized = crdt_store_key(
            CrdtKind::GCounter,
            CrdtStoreNamespace::Materialized,
            "counter:visits",
        );
        let durable_state = crdt_store_key(
            CrdtKind::GCounter,
            CrdtStoreNamespace::State,
            "counter:visits",
        );

        assert_eq!(materialized, materialized_gcounter_key("counter:visits"));
        assert_eq!(durable_state, durable_gcounter_state_key("counter:visits"));
        assert_eq!(
            logical_crdt_key(
                &materialized,
                CrdtKind::GCounter,
                CrdtStoreNamespace::Materialized,
            )
            .unwrap(),
            "counter:visits"
        );
        assert_eq!(
            logical_crdt_key(
                &durable_state,
                CrdtKind::GCounter,
                CrdtStoreNamespace::State
            )
            .unwrap(),
            "counter:visits"
        );

        let pn_materialized = crdt_store_key(
            CrdtKind::PNCounter,
            CrdtStoreNamespace::Materialized,
            "stock:sku-1",
        );
        let pn_durable_state = crdt_store_key(
            CrdtKind::PNCounter,
            CrdtStoreNamespace::State,
            "stock:sku-1",
        );

        assert_eq!(pn_materialized, materialized_pncounter_key("stock:sku-1"));
        assert_eq!(pn_durable_state, durable_pncounter_state_key("stock:sku-1"));
        assert_eq!(
            logical_crdt_key(
                &pn_materialized,
                CrdtKind::PNCounter,
                CrdtStoreNamespace::Materialized,
            )
            .unwrap(),
            "stock:sku-1"
        );
        assert_eq!(
            logical_crdt_key(
                &pn_durable_state,
                CrdtKind::PNCounter,
                CrdtStoreNamespace::State
            )
            .unwrap(),
            "stock:sku-1"
        );

        let lww_materialized = crdt_store_key(
            CrdtKind::LwwRegister,
            CrdtStoreNamespace::Materialized,
            "status:user-1",
        );
        let lww_durable_state = crdt_store_key(
            CrdtKind::LwwRegister,
            CrdtStoreNamespace::State,
            "status:user-1",
        );

        assert_eq!(
            lww_materialized,
            materialized_lww_register_key("status:user-1")
        );
        assert_eq!(
            lww_durable_state,
            durable_lww_register_state_key("status:user-1")
        );
        assert_eq!(
            logical_crdt_key(
                &lww_materialized,
                CrdtKind::LwwRegister,
                CrdtStoreNamespace::Materialized,
            )
            .unwrap(),
            "status:user-1"
        );
        assert_eq!(
            logical_crdt_key(
                &lww_durable_state,
                CrdtKind::LwwRegister,
                CrdtStoreNamespace::State
            )
            .unwrap(),
            "status:user-1"
        );
    }

    #[test]
    fn reconnect_backoff_doubles_until_max() {
        let max = Duration::from_secs(5);

        assert_eq!(
            next_reconnect_delay(Duration::from_millis(500), max),
            Duration::from_secs(1)
        );
        assert_eq!(
            next_reconnect_delay(Duration::from_secs(4), max),
            Duration::from_secs(5)
        );
        assert_eq!(
            next_reconnect_delay(Duration::from_secs(5), max),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn reconnect_delay_is_never_zero() {
        assert_eq!(
            normalize_reconnect_delay(Duration::ZERO),
            Duration::from_millis(1)
        );
    }

    #[test]
    fn peer_reconnect_state_tracks_next_attempt_per_peer() {
        let now = StdInstant::now();
        let mut state =
            PeerReconnectState::new("peer-a".to_string(), Duration::from_millis(500), now);

        let first_delay = state.record_failure(Duration::from_secs(5), now);
        assert_eq!(first_delay, Duration::from_millis(500));
        assert_eq!(state.delay, Duration::from_secs(1));
        assert_eq!(state.next_attempt_at, now + Duration::from_millis(500));

        state.reset(Duration::from_millis(500), now);
        assert_eq!(state.delay, Duration::from_millis(500));
        assert_eq!(state.next_attempt_at, now);
    }

    #[test]
    fn peer_reconnect_state_schedules_backoff_from_failure_time() {
        let started_at = StdInstant::now();
        let failed_at = started_at + Duration::from_secs(3);
        let mut state =
            PeerReconnectState::new("peer-a".to_string(), Duration::from_millis(500), started_at);

        state.record_failure(Duration::from_secs(5), failed_at);

        assert_eq!(
            state.next_attempt_at,
            failed_at + Duration::from_millis(500)
        );
    }

    #[tokio::test]
    async fn op_log_since_returns_ops_after_known_id() {
        let op_a = Op::gcounter_increment(NodeId::new("node-a"), "counter:visits", 1);
        let op_b = Op::gcounter_increment(NodeId::new("node-b"), "counter:visits", 2);
        let op_c = Op::gcounter_increment(NodeId::new("node-c"), "counter:visits", 3);
        let op_log = Arc::new(RwLock::new(vec![op_a.clone(), op_b.clone(), op_c.clone()]));

        assert_eq!(
            op_log_since(&op_log, None).await,
            vec![op_a, op_b.clone(), op_c.clone()]
        );
        assert_eq!(
            op_log_since(&op_log, Some(op_b.id.as_str())).await,
            vec![op_c]
        );
    }

    #[tokio::test]
    async fn remember_ops_prunes_op_log_to_configured_limit() {
        let store = temp_store();
        let seen_ops = Arc::new(RwLock::new(SeenOps::new(10)));
        let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
        let op_log_next_sequence = Arc::new(AtomicU64::new(0));
        let op_log = Arc::new(RwLock::new(Vec::new()));
        let op_a = Op::gcounter_increment(NodeId::new("node-a"), "counter:visits", 1);
        let op_b = Op::gcounter_increment(NodeId::new("node-b"), "counter:visits", 2);
        let op_c = Op::gcounter_increment(NodeId::new("node-c"), "counter:visits", 3);

        remember_ops(
            &seen_ops,
            &seen_ops_next_sequence,
            &op_log,
            &op_log_next_sequence,
            2,
            &store,
            &[op_a.clone(), op_b.clone(), op_c.clone()],
        )
        .await
        .unwrap();

        assert_eq!(op_log.read().await.as_slice(), &[op_b, op_c]);
        assert!(seen_ops.read().await.contains(op_a.id.as_str()));
    }

    #[test]
    fn op_log_limit_is_never_zero() {
        assert_eq!(normalize_op_log_limit(0), 1);
    }

    #[test]
    fn seen_ops_limit_is_never_zero() {
        assert_eq!(normalize_seen_ops_limit(0), 1);
    }

    #[test]
    fn seen_ops_eviction_keeps_recent_ids() {
        let mut seen = SeenOps::new(2);

        assert!(matches!(
            seen.insert("op-a"),
            SeenOpsInsert::Inserted { .. }
        ));
        assert!(matches!(
            seen.insert("op-b"),
            SeenOpsInsert::Inserted { .. }
        ));
        assert_eq!(
            seen.insert("op-c"),
            SeenOpsInsert::Inserted {
                evicted: vec!["op-a".to_string()]
            }
        );

        assert!(!seen.contains("op-a"));
        assert!(seen.contains("op-b"));
        assert!(seen.contains("op-c"));
        assert_eq!(seen.len(), 2);
    }

    #[test]
    fn seen_ops_duplicate_does_not_refresh_position() {
        let mut seen = SeenOps::new(2);

        assert!(matches!(
            seen.insert("op-a"),
            SeenOpsInsert::Inserted { .. }
        ));
        assert!(matches!(
            seen.insert("op-b"),
            SeenOpsInsert::Inserted { .. }
        ));
        assert_eq!(seen.insert("op-a"), SeenOpsInsert::AlreadySeen);
        assert_eq!(
            seen.insert("op-c"),
            SeenOpsInsert::Inserted {
                evicted: vec!["op-a".to_string()]
            }
        );

        assert!(!seen.contains("op-a"));
        assert!(seen.contains("op-b"));
        assert!(seen.contains("op-c"));
    }

    #[test]
    fn hydrate_seen_ops_restores_recent_ids_and_next_sequence() {
        let store = temp_store();

        persist_seen_op_batch(&store, "op-a", 7, &[]).unwrap();
        persist_seen_op_batch(&store, "op-b", 8, &[]).unwrap();

        let (seen, next_sequence) = hydrate_seen_ops(&store, 10).unwrap();

        assert!(seen.contains("op-a"));
        assert!(seen.contains("op-b"));
        assert_eq!(seen.len(), 2);
        assert_eq!(next_sequence, 9);
    }

    #[test]
    fn hydrate_seen_ops_prunes_old_metadata_to_limit() {
        let store = temp_store();

        persist_seen_op_batch(&store, "op-a", 1, &[]).unwrap();
        persist_seen_op_batch(&store, "op-b", 2, &[]).unwrap();
        persist_seen_op_batch(&store, "op-c", 3, &[]).unwrap();

        let (seen, next_sequence) = hydrate_seen_ops(&store, 2).unwrap();

        assert!(!seen.contains("op-a"));
        assert!(seen.contains("op-b"));
        assert!(seen.contains("op-c"));
        assert_eq!(next_sequence, 4);
        assert!(!seen_op_exists(&store, "op-a"));
        assert!(seen_op_exists(&store, "op-b"));
        assert!(seen_op_exists(&store, "op-c"));
    }

    #[test]
    fn hydrate_op_log_skips_key_value_op_id_mismatch() {
        let store = temp_store();
        let op_a = Op::gcounter_increment(NodeId::new("node-a"), "counter:visits", 1);
        let op_b = Op::gcounter_increment(NodeId::new("node-b"), "counter:visits", 1);

        write_durable_op_log_entry(&store, op_a.id.as_str(), 1, &op_a);
        write_durable_op_log_entry(&store, "different-op-id", 2, &op_b);

        let (op_log, next_sequence) = hydrate_op_log(&store, 10).unwrap();

        assert_eq!(op_log, vec![op_a]);
        assert_eq!(next_sequence, 2);
    }

    #[test]
    fn peer_health_marks_suspect_then_dead_after_failures() {
        let mut peer_health = HashMap::new();

        record_peer_failure(&mut peer_health, "peer-a", 2);
        let health = peer_health.get("peer-a").unwrap();
        assert_eq!(health.state, PeerHealthState::Suspect);
        assert_eq!(health.consecutive_failures, 1);

        record_peer_failure(&mut peer_health, "peer-a", 2);
        let health = peer_health.get("peer-a").unwrap();
        assert_eq!(health.state, PeerHealthState::Dead);
        assert_eq!(health.consecutive_failures, 2);
    }

    #[test]
    fn peer_health_resets_after_success() {
        let mut peer_health = HashMap::new();

        record_peer_failure(&mut peer_health, "peer-a", 1);
        assert_eq!(
            peer_health.get("peer-a").unwrap().state,
            PeerHealthState::Dead
        );

        record_peer_success(&mut peer_health, "peer-a");
        let health = peer_health.get("peer-a").unwrap();
        assert_eq!(health.state, PeerHealthState::Healthy);
        assert_eq!(health.consecutive_failures, 0);
    }

    #[test]
    fn peer_dead_after_failures_is_never_zero() {
        assert_eq!(normalize_peer_dead_after_failures(0), 1);
    }

    #[tokio::test]
    async fn peer_events_update_known_peer_health() {
        let peer = "127.0.0.1:42000";
        let peer_health = Arc::new(RwLock::new(HashMap::from([(
            peer.to_string(),
            PeerHealth::default(),
        )])));
        let counters = Arc::new(RwLock::new(HashMap::new()));
        let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
        let op_log = Arc::new(RwLock::new(Vec::new()));
        let store = temp_store();
        let metrics = metrics();
        let context = test_event_context(
            Arc::clone(&counters),
            Arc::clone(&seen_ops),
            Arc::clone(&op_log),
            Arc::clone(&store),
            Arc::clone(&metrics),
            Arc::clone(&peer_health),
        );
        let node_id = NodeId::generate();

        handle_node_event(
            NodeEvent::PeerConnected {
                node_id: node_id.clone(),
                addr: peer.to_string(),
                peers_connected: 1,
            },
            &context,
        )
        .await;
        assert_eq!(
            peer_health.read().await.get(peer).unwrap().state,
            PeerHealthState::Healthy
        );

        handle_node_event(
            NodeEvent::PeerDisconnected {
                node_id,
                addr: peer.to_string(),
                peers_connected: 0,
            },
            &context,
        )
        .await;
        let health = *peer_health.read().await.get(peer).unwrap();
        assert_eq!(health.state, PeerHealthState::Suspect);
        assert_eq!(health.consecutive_failures, 1);
    }

    #[tokio::test]
    async fn peer_events_ignore_unknown_peer_health() {
        let peer_health = Arc::new(RwLock::new(HashMap::new()));
        let counters = Arc::new(RwLock::new(HashMap::new()));
        let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
        let op_log = Arc::new(RwLock::new(Vec::new()));
        let store = temp_store();
        let metrics = metrics();
        let context = test_event_context(
            Arc::clone(&counters),
            Arc::clone(&seen_ops),
            Arc::clone(&op_log),
            Arc::clone(&store),
            Arc::clone(&metrics),
            Arc::clone(&peer_health),
        );

        handle_node_event(
            NodeEvent::PeerConnected {
                node_id: NodeId::generate(),
                addr: "127.0.0.1:42001".to_string(),
                peers_connected: 1,
            },
            &context,
        )
        .await;

        assert!(peer_health.read().await.is_empty());
    }

    #[tokio::test]
    async fn ops_received_updates_anti_entropy_watermark() {
        let peer = NodeId::new("peer-a");
        let peer_health = Arc::new(RwLock::new(HashMap::new()));
        let counters = Arc::new(RwLock::new(HashMap::new()));
        let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
        let op_log = Arc::new(RwLock::new(Vec::new()));
        let store = temp_store();
        let metrics = metrics();
        let context = test_event_context(
            Arc::clone(&counters),
            Arc::clone(&seen_ops),
            Arc::clone(&op_log),
            Arc::clone(&store),
            Arc::clone(&metrics),
            Arc::clone(&peer_health),
        );
        let op_a = Op::gcounter_increment(peer.clone(), "counter:visits", 1);
        let op_b = Op::gcounter_increment(peer.clone(), "counter:visits", 1);
        let expected = op_b.id.as_str().to_string();

        handle_node_event(
            NodeEvent::OpsReceived {
                from: peer.clone(),
                ops: vec![op_a, op_b],
            },
            &context,
        )
        .await;

        assert_eq!(
            context
                .anti_entropy_watermarks
                .read()
                .await
                .get(&peer)
                .cloned(),
            Some(expected)
        );
    }

    #[tokio::test]
    async fn apply_remote_op_materializes_counter_value() {
        let store = temp_store();
        let counters = Arc::new(RwLock::new(HashMap::new()));
        let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
        let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
        let op_log = Arc::new(RwLock::new(Vec::new()));
        let op_log_next_sequence = Arc::new(AtomicU64::new(0));
        let origin = NodeId::new("remote-a");
        let op = Op::gcounter_increment(origin, "counter:visits", 3);

        apply_remote_op_for_test(
            &op,
            &counters,
            &seen_ops,
            &seen_ops_next_sequence,
            &op_log,
            &op_log_next_sequence,
            &store,
        )
        .await;

        let key = materialized_gcounter_key("counter:visits");
        let bytes = store.get(&key).unwrap().unwrap();
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&bytes);

        assert_eq!(u64::from_le_bytes(buf), 3);
        let state = read_durable_gcounter_state(&store, "counter:visits");
        assert_eq!(state.value_for(&NodeId::new("remote-a")), 3);
    }

    #[tokio::test]
    async fn apply_remote_op_duplicate_does_not_double_materialize() {
        let store = temp_store();
        let counters = Arc::new(RwLock::new(HashMap::new()));
        let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
        let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
        let op_log = Arc::new(RwLock::new(Vec::new()));
        let op_log_next_sequence = Arc::new(AtomicU64::new(0));
        let origin = NodeId::new("remote-a");
        let op = Op::gcounter_increment(origin, "counter:visits", 3);

        apply_remote_op_for_test(
            &op,
            &counters,
            &seen_ops,
            &seen_ops_next_sequence,
            &op_log,
            &op_log_next_sequence,
            &store,
        )
        .await;
        apply_remote_op_for_test(
            &op,
            &counters,
            &seen_ops,
            &seen_ops_next_sequence,
            &op_log,
            &op_log_next_sequence,
            &store,
        )
        .await;

        let key = materialized_gcounter_key("counter:visits");
        let bytes = store.get(&key).unwrap().unwrap();
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&bytes);

        assert_eq!(u64::from_le_bytes(buf), 3);
    }

    #[tokio::test]
    async fn apply_remote_pncounter_ops_materialize_signed_value() {
        let store = temp_store();
        let pncounters = Arc::new(RwLock::new(HashMap::new()));
        let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
        let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
        let op_log = Arc::new(RwLock::new(Vec::new()));
        let op_log_next_sequence = Arc::new(AtomicU64::new(0));
        let origin = NodeId::new("remote-a");
        let increment = Op::pncounter_increment(origin.clone(), "stock:sku-1", 10);
        let decrement = Op::pncounter_decrement(origin.clone(), "stock:sku-1", 3);

        apply_remote_pncounter_op_for_test(
            &increment,
            &pncounters,
            &seen_ops,
            &seen_ops_next_sequence,
            &op_log,
            &op_log_next_sequence,
            &store,
        )
        .await;
        apply_remote_pncounter_op_for_test(
            &decrement,
            &pncounters,
            &seen_ops,
            &seen_ops_next_sequence,
            &op_log,
            &op_log_next_sequence,
            &store,
        )
        .await;

        assert_eq!(read_materialized_pncounter(&store, "stock:sku-1"), 7);
        let state = read_durable_pncounter_state(&store, "stock:sku-1");
        assert_eq!(state.positive_for(&origin), 10);
        assert_eq!(state.negative_for(&origin), 3);
        assert_eq!(state.value(), 7);
    }

    #[tokio::test]
    async fn apply_remote_pncounter_duplicate_does_not_double_materialize() {
        let store = temp_store();
        let pncounters = Arc::new(RwLock::new(HashMap::new()));
        let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
        let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
        let op_log = Arc::new(RwLock::new(Vec::new()));
        let op_log_next_sequence = Arc::new(AtomicU64::new(0));
        let op = Op::pncounter_decrement(NodeId::new("remote-a"), "stock:sku-1", 3);

        apply_remote_pncounter_op_for_test(
            &op,
            &pncounters,
            &seen_ops,
            &seen_ops_next_sequence,
            &op_log,
            &op_log_next_sequence,
            &store,
        )
        .await;
        apply_remote_pncounter_op_for_test(
            &op,
            &pncounters,
            &seen_ops,
            &seen_ops_next_sequence,
            &op_log,
            &op_log_next_sequence,
            &store,
        )
        .await;

        assert_eq!(read_materialized_pncounter(&store, "stock:sku-1"), -3);
    }

    #[tokio::test]
    async fn apply_remote_lww_register_set_materializes_value() {
        let store = temp_store();
        let registers = Arc::new(RwLock::new(HashMap::new()));
        let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
        let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
        let op_log = Arc::new(RwLock::new(Vec::new()));
        let op_log_next_sequence = Arc::new(AtomicU64::new(0));
        let origin = NodeId::new("remote-a");
        let op = Op::lww_register_set(origin.clone(), "status:user-1", b"online".to_vec(), 100);

        apply_remote_lww_register_op_for_test(
            &op,
            &registers,
            &seen_ops,
            &seen_ops_next_sequence,
            &op_log,
            &op_log_next_sequence,
            &store,
        )
        .await;

        assert_eq!(
            read_materialized_lww_register(&store, "status:user-1"),
            b"online".to_vec()
        );
        let state = read_durable_lww_register_state(&store, "status:user-1");
        assert_eq!(state.value(), b"online");
        assert_eq!(state.timestamp_ms(), 100);
        assert_eq!(state.writer(), &origin);
    }

    #[tokio::test]
    async fn apply_remote_lww_register_older_set_does_not_overwrite() {
        let store = temp_store();
        let registers = Arc::new(RwLock::new(HashMap::new()));
        let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
        let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
        let op_log = Arc::new(RwLock::new(Vec::new()));
        let op_log_next_sequence = Arc::new(AtomicU64::new(0));
        let newer = Op::lww_register_set(
            NodeId::new("remote-a"),
            "status:user-1",
            b"online".to_vec(),
            200,
        );
        let older = Op::lww_register_set(
            NodeId::new("remote-b"),
            "status:user-1",
            b"away".to_vec(),
            100,
        );

        apply_remote_lww_register_op_for_test(
            &newer,
            &registers,
            &seen_ops,
            &seen_ops_next_sequence,
            &op_log,
            &op_log_next_sequence,
            &store,
        )
        .await;
        apply_remote_lww_register_op_for_test(
            &older,
            &registers,
            &seen_ops,
            &seen_ops_next_sequence,
            &op_log,
            &op_log_next_sequence,
            &store,
        )
        .await;

        assert_eq!(
            read_materialized_lww_register(&store, "status:user-1"),
            b"online".to_vec()
        );
        let state = read_durable_lww_register_state(&store, "status:user-1");
        assert_eq!(state.value(), b"online");
        assert_eq!(state.timestamp_ms(), 200);
    }

    #[tokio::test]
    async fn apply_remote_lww_register_duplicate_does_not_double_apply() {
        let store = temp_store();
        let registers = Arc::new(RwLock::new(HashMap::new()));
        let seen_ops = Arc::new(RwLock::new(SeenOps::new(1024)));
        let seen_ops_next_sequence = Arc::new(AtomicU64::new(0));
        let op_log = Arc::new(RwLock::new(Vec::new()));
        let op_log_next_sequence = Arc::new(AtomicU64::new(0));
        let op = Op::lww_register_set(
            NodeId::new("remote-a"),
            "status:user-1",
            b"online".to_vec(),
            100,
        );

        apply_remote_lww_register_op_for_test(
            &op,
            &registers,
            &seen_ops,
            &seen_ops_next_sequence,
            &op_log,
            &op_log_next_sequence,
            &store,
        )
        .await;
        apply_remote_lww_register_op_for_test(
            &op,
            &registers,
            &seen_ops,
            &seen_ops_next_sequence,
            &op_log,
            &op_log_next_sequence,
            &store,
        )
        .await;

        assert_eq!(op_log.read().await.len(), 1);
        assert_eq!(
            read_materialized_lww_register(&store, "status:user-1"),
            b"online".to_vec()
        );
    }

    #[tokio::test]
    async fn duplicate_remote_op_after_restart_does_not_double_count() {
        let store = temp_store();
        let origin = NodeId::new("remote-a");
        let op = Op::gcounter_increment(origin, "counter:visits", 3);

        let first_manager = SyncManager::new(
            NodeId::new("local-node"),
            SyncConfig::new(),
            Arc::clone(&store),
            metrics(),
        );
        apply_remote_op_for_test(
            &op,
            &first_manager.counters,
            &first_manager.seen_ops,
            &first_manager.seen_ops_next_sequence,
            &first_manager.op_log,
            &first_manager.op_log_next_sequence,
            &store,
        )
        .await;

        let restarted_manager = SyncManager::new(
            NodeId::new("local-node"),
            SyncConfig::new(),
            Arc::clone(&store),
            metrics(),
        );
        apply_remote_op_for_test(
            &op,
            &restarted_manager.counters,
            &restarted_manager.seen_ops,
            &restarted_manager.seen_ops_next_sequence,
            &restarted_manager.op_log,
            &restarted_manager.op_log_next_sequence,
            &store,
        )
        .await;

        assert_eq!(
            restarted_manager.get_counter_value("counter:visits").await,
            3
        );
        assert_eq!(read_materialized(&store, "counter:visits"), 3);
    }

    #[tokio::test]
    async fn manager_hydrates_gcounter_registry_from_materialized_values() {
        let store = temp_store();
        materialize_gcounter_value(&store, "counter:visits", 42).unwrap();

        let node_id = NodeId::new("local-node");
        let config = SyncConfig::new();
        let manager = SyncManager::new(node_id.clone(), config, Arc::clone(&store), metrics());

        assert_eq!(manager.get_counter_value("counter:visits").await, 42);

        let counters = manager.counters.read().await;
        let counter = counters.get("counter:visits").unwrap();
        assert_eq!(counter.value_for(&node_id), 42);
    }

    #[tokio::test]
    async fn manager_hydrates_gcounter_registry_from_durable_state() {
        let store = temp_store();
        let node_a = NodeId::new("node-a");
        let node_b = NodeId::new("node-b");
        let mut counter = GCounter::new();
        counter.increment(&node_a, 5);
        counter.increment(&node_b, 7);

        persist_gcounter_state(&store, "counter:visits", &counter).unwrap();

        let manager = SyncManager::new(
            NodeId::new("local-node"),
            SyncConfig::new(),
            Arc::clone(&store),
            metrics(),
        );

        assert_eq!(manager.get_counter_value("counter:visits").await, 12);

        let counters = manager.counters.read().await;
        let hydrated = counters.get("counter:visits").unwrap();
        assert_eq!(hydrated.value_for(&node_a), 5);
        assert_eq!(hydrated.value_for(&node_b), 7);
    }

    #[tokio::test]
    async fn manager_hydrates_pncounter_registry_from_durable_state() {
        let store = temp_store();
        let node_a = NodeId::new("node-a");
        let node_b = NodeId::new("node-b");
        let mut counter = PNCounter::new();
        counter.increment(&node_a, 8);
        counter.decrement(&node_b, 11);

        persist_pncounter_state(&store, "stock:sku-1", &counter).unwrap();

        let manager = SyncManager::new(
            NodeId::new("local-node"),
            SyncConfig::new(),
            Arc::clone(&store),
            metrics(),
        );

        assert_eq!(manager.get_pncounter_value("stock:sku-1").await, -3);

        let pncounters = manager.pncounters.read().await;
        let hydrated = pncounters.get("stock:sku-1").unwrap();
        assert_eq!(hydrated.positive_for(&node_a), 8);
        assert_eq!(hydrated.negative_for(&node_b), 11);
    }

    #[tokio::test]
    async fn manager_hydrates_lww_register_registry_from_durable_state() {
        let store = temp_store();
        let writer = NodeId::new("node-a");
        let register = LwwRegister::new(b"online".to_vec(), 123, writer.clone());

        persist_lww_register_state(&store, "status:user-1", &register).unwrap();

        let manager = SyncManager::new(
            NodeId::new("local-node"),
            SyncConfig::new(),
            Arc::clone(&store),
            metrics(),
        );

        assert_eq!(
            manager
                .get_lww_register_value("status:user-1")
                .await
                .unwrap(),
            b"online".to_vec()
        );

        let registers = manager.lww_registers.read().await;
        let hydrated = registers.get("status:user-1").unwrap();
        assert_eq!(hydrated.value(), b"online");
        assert_eq!(hydrated.timestamp_ms(), 123);
        assert_eq!(hydrated.writer(), &writer);
    }

    #[tokio::test]
    async fn durable_lww_register_state_takes_precedence_over_materialized_value() {
        let store = temp_store();
        let register = LwwRegister::new(b"online".to_vec(), 123, NodeId::new("node-a"));

        persist_lww_register_state(&store, "status:user-1", &register).unwrap();
        materialize_lww_register_value(&store, "status:user-1", b"stale").unwrap();

        let manager = SyncManager::new(
            NodeId::new("local-node"),
            SyncConfig::new(),
            Arc::clone(&store),
            metrics(),
        );

        assert_eq!(
            manager
                .get_lww_register_value("status:user-1")
                .await
                .unwrap(),
            b"online".to_vec()
        );
        assert_eq!(
            read_materialized_lww_register(&store, "status:user-1"),
            b"online".to_vec()
        );
    }

    #[tokio::test]
    async fn durable_pncounter_state_takes_precedence_over_materialized_total() {
        let store = temp_store();
        let node_a = NodeId::new("node-a");
        let node_b = NodeId::new("node-b");
        let mut counter = PNCounter::new();
        counter.increment(&node_a, 5);
        counter.decrement(&node_b, 2);

        persist_pncounter_state(&store, "stock:sku-1", &counter).unwrap();
        materialize_pncounter_value(&store, "stock:sku-1", -99).unwrap();

        let manager = SyncManager::new(
            NodeId::new("local-node"),
            SyncConfig::new(),
            Arc::clone(&store),
            metrics(),
        );

        assert_eq!(manager.get_pncounter_value("stock:sku-1").await, 3);
        assert_eq!(read_materialized_pncounter(&store, "stock:sku-1"), 3);
    }

    #[tokio::test]
    async fn durable_gcounter_state_takes_precedence_over_materialized_total() {
        let store = temp_store();
        let node_a = NodeId::new("node-a");
        let node_b = NodeId::new("node-b");
        let mut counter = GCounter::new();
        counter.increment(&node_a, 2);
        counter.increment(&node_b, 3);

        persist_gcounter_state(&store, "counter:visits", &counter).unwrap();
        materialize_gcounter_value(&store, "counter:visits", 99).unwrap();

        let manager = SyncManager::new(
            NodeId::new("local-node"),
            SyncConfig::new(),
            Arc::clone(&store),
            metrics(),
        );

        assert_eq!(manager.get_counter_value("counter:visits").await, 5);
        assert_eq!(read_materialized(&store, "counter:visits"), 5);
    }

    #[tokio::test]
    async fn e2e_two_nodes_push_ops_materializes_on_peer() {
        let key = "visits";
        let addr_a = free_addr();
        let addr_b = free_addr();
        let (manager_a, handle_a, _store_a) = started_manager(addr_a).await;
        let (manager_b, _handle_b, store_b) = started_manager(addr_b.clone()).await;

        manager_a.connect_to_peer(&addr_b).await.unwrap();
        local_increment(&handle_a, key, 1).await;

        wait_for_counter(&manager_b, key, 1).await;
        assert_eq!(read_materialized(&store_b, key), 1);
    }

    #[tokio::test]
    async fn e2e_two_nodes_parallel_increments_converge() {
        let key = "visits";
        let addr_a = free_addr();
        let addr_b = free_addr();
        let (manager_a, handle_a, store_a) = started_manager(addr_a.clone()).await;
        let (manager_b, handle_b, store_b) = started_manager(addr_b.clone()).await;

        manager_a.connect_to_peer(&addr_b).await.unwrap();
        manager_b.connect_to_peer(&addr_a).await.unwrap();

        tokio::join!(
            local_increment(&handle_a, key, 1),
            local_increment(&handle_b, key, 1),
        );

        wait_for_counter(&manager_a, key, 2).await;
        wait_for_counter(&manager_b, key, 2).await;
        assert_eq!(read_materialized(&store_a, key), 2);
        assert_eq!(read_materialized(&store_b, key), 2);
    }

    #[tokio::test]
    async fn e2e_two_nodes_pncounter_inc_dec_converge() {
        let key = "inventory:sku-1";
        let addr_a = free_addr();
        let addr_b = free_addr();
        let (manager_a, handle_a, store_a) = started_manager(addr_a.clone()).await;
        let (manager_b, handle_b, store_b) = started_manager(addr_b.clone()).await;

        manager_a.connect_to_peer(&addr_b).await.unwrap();
        manager_b.connect_to_peer(&addr_a).await.unwrap();

        tokio::join!(
            local_pncounter_inc(&handle_a, key, 10),
            local_pncounter_dec(&handle_b, key, 4),
        );

        wait_for_pncounter(&manager_a, key, 6).await;
        wait_for_pncounter(&manager_b, key, 6).await;
        assert_eq!(read_materialized_pncounter(&store_a, key), 6);
        assert_eq!(read_materialized_pncounter(&store_b, key), 6);
    }

    #[tokio::test]
    async fn e2e_two_nodes_lww_register_sets_converge_to_latest_value() {
        let key = "status:user-1";
        let addr_a = free_addr();
        let addr_b = free_addr();
        let (manager_a, handle_a, store_a) = started_manager(addr_a.clone()).await;
        let (manager_b, handle_b, store_b) = started_manager(addr_b.clone()).await;

        manager_a.connect_to_peer(&addr_b).await.unwrap();
        manager_b.connect_to_peer(&addr_a).await.unwrap();

        tokio::join!(
            local_lww_register_set(&handle_a, key, b"online", 100),
            local_lww_register_set(&handle_b, key, b"away", 200),
        );

        wait_for_lww_register(&manager_a, key, b"away").await;
        wait_for_lww_register(&manager_b, key, b"away").await;
        assert_eq!(
            read_materialized_lww_register(&store_a, key),
            b"away".to_vec()
        );
        assert_eq!(
            read_materialized_lww_register(&store_b, key),
            b"away".to_vec()
        );

        let state_a = read_durable_lww_register_state(&store_a, key);
        let state_b = read_durable_lww_register_state(&store_b, key);
        assert_eq!(state_a.value(), b"away");
        assert_eq!(state_b.value(), b"away");
        assert_eq!(state_a.timestamp_ms(), 200);
        assert_eq!(state_b.timestamp_ms(), 200);
        assert_eq!(state_a.writer(), handle_b.node_id());
        assert_eq!(state_b.writer(), handle_b.node_id());
    }

    #[tokio::test]
    async fn reconnect_loop_connects_configured_peer_that_starts_later() {
        let key = "visits";
        let addr_a = free_addr();
        let addr_b = free_addr();

        let config_a = SyncConfig::new()
            .with_listen_addr(addr_a)
            .with_peer(addr_b.clone())
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(50));
        let (manager_a, handle_a, _store_a) = started_manager_with_config(config_a).await;

        let config_b = SyncConfig::new().with_listen_addr(addr_b);
        let (manager_b, _handle_b, store_b) = started_manager_with_config(config_b).await;

        wait_for_connected_peer(&manager_a).await;
        local_increment(&handle_a, key, 1).await;

        wait_for_counter(&manager_b, key, 1).await;
        assert_eq!(read_materialized(&store_b, key), 1);
    }

    #[tokio::test]
    async fn anti_entropy_pull_converges_peer_that_missed_broadcast() {
        let key = "visits";
        let addr_a = free_addr();
        let addr_b = free_addr();

        let config_a = SyncConfig::new().with_listen_addr(addr_a.clone());
        let (manager_a, handle_a, _store_a) = started_manager_with_config(config_a).await;

        local_increment(&handle_a, key, 1).await;

        let config_b = SyncConfig::new()
            .with_listen_addr(addr_b)
            .with_peer(addr_a)
            .with_anti_entropy_interval(Duration::from_millis(10));
        let (manager_b, _handle_b, store_b) = started_manager_with_config(config_b).await;

        wait_for_connected_peer(&manager_b).await;
        wait_for_counter(&manager_b, key, 1).await;
        assert_eq!(read_materialized(&store_b, key), 1);
        assert_eq!(manager_a.get_counter_value(key).await, 1);
    }

    #[tokio::test]
    async fn intermittent_network_with_ten_percent_lost_pushes_converges() {
        let key = "visits";
        let addr_a = free_addr();
        let addr_b = free_addr();

        let config_a = SyncConfig::new()
            .with_listen_addr(addr_a.clone())
            .with_peer(addr_b.clone())
            .with_anti_entropy_interval(Duration::from_millis(10));
        let (manager_a, handle_a, store_a) = started_manager_with_config(config_a).await;

        let config_b = SyncConfig::new()
            .with_listen_addr(addr_b)
            .with_peer(addr_a)
            .with_anti_entropy_interval(Duration::from_millis(10));
        let (manager_b, _handle_b, store_b) = started_manager_with_config(config_b).await;

        wait_for_connected_peer(&manager_a).await;
        wait_for_connected_peer(&manager_b).await;

        for i in 0..100 {
            if i % 10 == 0 {
                dropped_local_increment(&manager_a, &handle_a, key, 1).await;
            } else {
                local_increment(&handle_a, key, 1).await;
            }
        }

        wait_for_counter(&manager_b, key, 100).await;
        assert_eq!(manager_a.get_counter_value(key).await, 100);
        assert_eq!(read_materialized(&store_a, key), 100);
        assert_eq!(read_materialized(&store_b, key), 100);
    }

    #[tokio::test]
    async fn node_restart_reconnects_and_converges_from_durable_state() {
        let key = "visits";
        let addr_a = free_addr();
        let addr_b = free_addr();
        let store_b = temp_store();
        let node_b_id = NodeId::new("node-b");

        let config_a = SyncConfig::new()
            .with_listen_addr(addr_a.clone())
            .with_peer(addr_b.clone())
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(50))
            .with_anti_entropy_interval(Duration::from_millis(10));
        let (manager_a, handle_a, store_a) = started_manager_with_config(config_a).await;

        let config_b = SyncConfig::new()
            .with_listen_addr(addr_b.clone())
            .with_peer(addr_a.clone())
            .with_anti_entropy_interval(Duration::from_millis(10));
        let (mut manager_b, _handle_b) =
            started_manager_with_store(node_b_id.clone(), config_b, Arc::clone(&store_b)).await;

        wait_for_connected_peer(&manager_a).await;
        local_increment(&handle_a, key, 1).await;
        wait_for_counter(&manager_b, key, 1).await;

        manager_b.shutdown().await.unwrap();
        local_increment(&handle_a, key, 1).await;

        let config_b_restart = SyncConfig::new()
            .with_listen_addr(addr_b)
            .with_peer(addr_a)
            .with_anti_entropy_interval(Duration::from_millis(10));
        let (manager_b_restarted, _handle_b_restarted) =
            started_manager_with_store(node_b_id, config_b_restart, Arc::clone(&store_b)).await;

        wait_for_connected_peer(&manager_a).await;
        wait_for_connected_peer(&manager_b_restarted).await;
        wait_for_counter(&manager_b_restarted, key, 2).await;

        assert_eq!(manager_a.get_counter_value(key).await, 2);
        assert_eq!(read_materialized(&store_a, key), 2);
        assert_eq!(read_materialized(&store_b, key), 2);
    }

    #[tokio::test]
    #[ignore = "chaos smoke: repeatedly restarts a node and uses local TCP timing"]
    async fn chaos_node_restart_loop_converges() {
        let key = "visits";
        let addr_a = free_addr();
        let addr_b = free_addr();
        let addr_c = free_addr();
        let store_b = temp_store();
        let node_b_id = NodeId::new("node-b");

        let config_a = SyncConfig::new()
            .with_listen_addr(addr_a.clone())
            .with_peer(addr_b.clone())
            .with_peer(addr_c.clone())
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(50))
            .with_anti_entropy_interval(Duration::from_millis(10));
        let (manager_a, handle_a, store_a) = started_manager_with_config(config_a).await;

        let config_b = SyncConfig::new()
            .with_listen_addr(addr_b.clone())
            .with_peer(addr_a.clone())
            .with_peer(addr_c.clone())
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(50))
            .with_anti_entropy_interval(Duration::from_millis(10));
        let (mut manager_b, _handle_b) =
            started_manager_with_store(node_b_id.clone(), config_b.clone(), Arc::clone(&store_b))
                .await;

        let config_c = SyncConfig::new()
            .with_listen_addr(addr_c)
            .with_peer(addr_a.clone())
            .with_peer(addr_b.clone())
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(50))
            .with_anti_entropy_interval(Duration::from_millis(10));
        let (manager_c, handle_c, store_c) = started_manager_with_config(config_c).await;

        let mut expected = 0;
        for _ in 0..3 {
            local_increment(&handle_a, key, 1).await;
            local_increment(&handle_c, key, 1).await;
            expected += 2;
            wait_for_counter(&manager_b, key, expected).await;

            manager_b.shutdown().await.unwrap();

            local_increment(&handle_a, key, 1).await;
            local_increment(&handle_c, key, 1).await;
            expected += 2;

            let restarted = started_manager_with_store(
                node_b_id.clone(),
                config_b.clone(),
                Arc::clone(&store_b),
            )
            .await;
            manager_b = restarted.0;

            wait_for_connected_peer(&manager_b).await;
            wait_for_counter(&manager_b, key, expected).await;
        }

        wait_for_counter(&manager_a, key, expected).await;
        wait_for_counter(&manager_c, key, expected).await;
        assert_eq!(read_materialized(&store_a, key), expected);
        assert_eq!(read_materialized(&store_b, key), expected);
        assert_eq!(read_materialized(&store_c, key), expected);
    }

    #[tokio::test]
    async fn source_restart_before_anti_entropy_still_serves_missed_ops() {
        let key = "visits";
        let addr_a = free_addr();
        let addr_b = free_addr();
        let store_a = temp_store();
        let node_a_id = NodeId::new("node-a");

        let config_a = SyncConfig::new()
            .with_listen_addr(addr_a.clone())
            .with_peer(addr_b.clone())
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(50))
            .with_anti_entropy_interval(Duration::from_millis(10));
        let (mut manager_a, handle_a) =
            started_manager_with_store(node_a_id.clone(), config_a, Arc::clone(&store_a)).await;

        let config_b = SyncConfig::new()
            .with_listen_addr(addr_b)
            .with_peer(addr_a.clone())
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(50))
            .with_anti_entropy_interval(Duration::from_millis(10));
        let (manager_b, _handle_b, store_b) = started_manager_with_config(config_b).await;

        wait_for_connected_peer(&manager_a).await;
        wait_for_connected_peer(&manager_b).await;

        dropped_local_increment(&manager_a, &handle_a, key, 1).await;
        manager_a.shutdown().await.unwrap();

        let config_a_restart = SyncConfig::new()
            .with_listen_addr(addr_a)
            .with_peer(manager_b.config.listen_addr.clone().unwrap())
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(50))
            .with_anti_entropy_interval(Duration::from_millis(10));
        let (manager_a_restarted, _handle_a_restarted) =
            started_manager_with_store(node_a_id, config_a_restart, Arc::clone(&store_a)).await;

        wait_for_connected_peer(&manager_a_restarted).await;
        wait_for_connected_peer(&manager_b).await;
        wait_for_counter(&manager_b, key, 1).await;

        assert_eq!(read_materialized(&store_a, key), 1);
        assert_eq!(read_materialized(&store_b, key), 1);
    }

    #[tokio::test]
    async fn peer_health_marks_dead_then_healthy_when_peer_recovers() {
        let addr_a = free_addr();
        let addr_b = free_addr();

        let config_a = SyncConfig::new()
            .with_listen_addr(addr_a)
            .with_peer(addr_b.clone())
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20))
            .with_peer_dead_after_failures(2);
        let (manager_a, _handle_a, _store_a) = started_manager_with_config(config_a).await;

        wait_for_peer_health(&manager_a, &addr_b, PeerHealthState::Dead).await;

        let config_b = SyncConfig::new().with_listen_addr(addr_b.clone());
        let (_manager_b, _handle_b, _store_b) = started_manager_with_config(config_b).await;

        wait_for_connected_peer(&manager_a).await;
        wait_for_peer_health(&manager_a, &addr_b, PeerHealthState::Healthy).await;
    }

    #[tokio::test]
    async fn peer_rotation_replaces_dead_peer_with_available_configured_peer() {
        let addr_a = free_addr();
        let addr_b = free_addr();
        let addr_c = free_addr();

        let (mut manager_b, _handle_b, _store_b) = started_manager(addr_b.clone()).await;
        let (_manager_c, _handle_c, _store_c) = started_manager(addr_c.clone()).await;

        let config_a = SyncConfig::new()
            .with_listen_addr(addr_a)
            .with_peer(addr_b.clone())
            .with_peer(addr_c.clone())
            .with_max_peers(1)
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(20))
            .with_peer_dead_after_failures(1);
        let (manager_a, _handle_a, _store_a) = started_manager_with_config(config_a).await;

        wait_for_peer_health(&manager_a, &addr_b, PeerHealthState::Healthy).await;
        sleep(Duration::from_millis(50)).await;
        assert_eq!(
            manager_a.peer_health_state(&addr_c).await,
            Some(PeerHealthState::Suspect),
            "peer C should not be marked failed while peer slots are full"
        );

        manager_b.shutdown().await.unwrap();

        wait_for_peer_health(&manager_a, &addr_b, PeerHealthState::Dead).await;
        wait_for_peer_health(&manager_a, &addr_c, PeerHealthState::Healthy).await;
        assert_eq!(manager_a.connected_peer_count().await, 1);
    }

    #[tokio::test]
    async fn shutdown_drains_queued_ops_before_closing_connections() {
        let key = "visits";
        let addr_a = free_addr();
        let addr_b = free_addr();
        let (mut manager_a, handle_a, _store_a) = started_manager(addr_a).await;
        let (manager_b, _handle_b, store_b) = started_manager(addr_b.clone()).await;

        manager_a.connect_to_peer(&addr_b).await.unwrap();

        let op = Op::gcounter_increment(handle_a.node_id().clone(), key, 1);
        handle_a.op_sender().send(op).await.unwrap();
        manager_a.shutdown().await.unwrap();

        wait_for_counter(&manager_b, key, 1).await;
        assert_eq!(read_materialized(&store_b, key), 1);
    }

    #[tokio::test]
    async fn manager_uses_configured_queued_ops_limit() {
        let store = temp_store();
        let config = SyncConfig::new().with_queued_ops_limit(256);
        let manager = SyncManager::new(NodeId::generate(), config, Arc::clone(&store), metrics());

        assert_eq!(manager.op_sender().max_capacity(), 256);
    }

    #[tokio::test]
    async fn manager_normalizes_empty_queued_ops_limit() {
        let store = temp_store();
        let config = SyncConfig::new().with_queued_ops_limit(0);
        let manager = SyncManager::new(NodeId::generate(), config, Arc::clone(&store), metrics());

        assert_eq!(manager.op_sender().max_capacity(), 1);
    }

    #[test]
    fn sync_disabled_runtime_has_no_sync_handle() {
        let config = RuntimeConfig {
            datastore_path: std::env::temp_dir().join(format!(
                "numax-core-nosync-test-{}",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            )),
            ..RuntimeConfig::default()
        };

        let runtime = Runtime::new(config).unwrap();
        assert!(runtime.sync_handle().is_none());
    }
}
