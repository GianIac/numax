use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant as StdInstant};

use nx_net::{NetError, Node, NodeConfig, NodeEvent};
use nx_store::Store as NxStore;
use nx_sync::{GCounter, NodeId, Op, OpKind};
use tokio::sync::{RwLock, mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::observability::RuntimeMetrics;
use crate::sync_config::SyncConfig;

/// Upper bound on how many ops we coalesce into a single PushOps message.
const BROADCAST_BATCH_MAX: usize = 64;
const GCOUNTER_STORE_PREFIX: &str = "__nx/crdt/gcounter/";

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
    store: Arc<NxStore>,
    metrics: Arc<RuntimeMetrics>,
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

    /// Shared datastore used to materialize CRDT values.
    pub fn store(&self) -> Arc<NxStore> {
        Arc::clone(&self.store)
    }

    /// Shared runtime metrics.
    pub fn metrics(&self) -> Arc<RuntimeMetrics> {
        Arc::clone(&self.metrics)
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

    /// Store used to materialize replicated values.
    store: Arc<NxStore>,

    /// Shared runtime metrics.
    metrics: Arc<RuntimeMetrics>,

    /// OpId
    seen_ops: Arc<RwLock<HashSet<String>>>,

    /// Health state for configured peers, keyed by configured address.
    peer_health: Arc<RwLock<HashMap<String, PeerHealth>>>,

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
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let mut counters = HashMap::new();
        let peer_health = config
            .peers
            .iter()
            .map(|peer| (peer.clone(), PeerHealth::default()))
            .collect::<HashMap<_, _>>();

        if let Err(e) = hydrate_gcounter_registry(&store, &node_id, &mut counters) {
            warn!(error = %e, "failed to hydrate GCounter registry");
        }
        let counters = Arc::new(RwLock::new(counters));

        Self {
            node_id,
            config,
            node: None,
            counters,
            store,
            metrics,
            seen_ops: Arc::new(RwLock::new(HashSet::new())),
            peer_health: Arc::new(RwLock::new(peer_health)),
            op_tx,
            op_rx: Some(op_rx),
            shutdown_tx,
            event_task: None,
            broadcast_task: None,
            reconnect_task: None,
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
            store: Arc::clone(&self.store),
            metrics: Arc::clone(&self.metrics),
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
            .with_socket_timeout(self.config.socket_timeout);

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
        let counters = Arc::clone(&self.counters);
        let seen_ops = Arc::clone(&self.seen_ops);
        let store = Arc::clone(&self.store);
        let metrics = Arc::clone(&self.metrics);
        let peer_health = Arc::clone(&self.peer_health);
        let peer_dead_after_failures =
            normalize_peer_dead_after_failures(self.config.peer_dead_after_failures);
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        self.event_task = Some(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            debug!("event loop shutdown requested");
                            drain_node_events(
                                &mut event_rx,
                                &counters,
                                &seen_ops,
                                &store,
                                &metrics,
                                &peer_health,
                                peer_dead_after_failures,
                            ).await;
                            break;
                        }
                    }
                    event = event_rx.recv() => {
                        let Some(event) = event else {
                            break;
                        };
                        handle_node_event(
                            event,
                            &counters,
                            &seen_ops,
                            &store,
                            &metrics,
                            &peer_health,
                            peer_dead_after_failures,
                        ).await;
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
            Arc::clone(&node),
            op_rx,
            self.shutdown_tx.subscribe(),
            Arc::clone(&self.metrics),
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
    node: Arc<Node>,
    mut op_rx: mpsc::Receiver<Op>,
    mut shutdown_rx: watch::Receiver<bool>,
    metrics: Arc<RuntimeMetrics>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        debug!("broadcast loop shutdown requested");
                        drain_broadcast_queue(&node, &mut op_rx, &metrics).await;
                        break;
                    }
                }
                op = op_rx.recv() => {
                    let Some(first) = op else {
                        break;
                    };
                    broadcast_batch(&node, &mut op_rx, first, &metrics).await;
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
                    peer.reset(initial_delay, now);
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
                        peer.reset(initial_delay, now);
                    }
                    ConfiguredPeerConnectOutcome::AlreadyConnected => {
                        peer.reset(initial_delay, now);
                    }
                    ConfiguredPeerConnectOutcome::SlotLimitReached => {
                        break;
                    }
                    ConfiguredPeerConnectOutcome::Failed => {
                        let attempt_delay = peer.record_failure(max_delay, now);
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

fn normalize_reconnect_delay(delay: Duration) -> Duration {
    delay.max(Duration::from_millis(1))
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
    node: &Arc<Node>,
    op_rx: &mut mpsc::Receiver<Op>,
    first: Op,
    metrics: &Arc<RuntimeMetrics>,
) {
    let mut batch = Vec::with_capacity(BROADCAST_BATCH_MAX);
    batch.push(first);

    // Coalesce any ops already queued, without yielding.
    while batch.len() < BROADCAST_BATCH_MAX {
        match op_rx.try_recv() {
            Ok(op) => batch.push(op),
            Err(_) => break,
        }
    }

    let count = batch.len();
    let started = std::time::Instant::now();
    if let Err(e) = node.broadcast_ops(batch).await {
        metrics.record_sync_error();
        warn!(error = %e, count, "broadcast partially failed; ops dropped for failed peers");
    } else {
        metrics.record_broadcast_batch(count);
        metrics.record_sync_latency(started.elapsed());
        debug!(count, "broadcast batch sent");
    }
}

async fn drain_broadcast_queue(
    node: &Arc<Node>,
    op_rx: &mut mpsc::Receiver<Op>,
    metrics: &Arc<RuntimeMetrics>,
) {
    while let Ok(first) = op_rx.try_recv() {
        broadcast_batch(node, op_rx, first, metrics).await;
    }
}

async fn drain_node_events(
    event_rx: &mut mpsc::Receiver<NodeEvent>,
    counters: &Arc<RwLock<HashMap<String, GCounter>>>,
    seen_ops: &Arc<RwLock<HashSet<String>>>,
    store: &Arc<NxStore>,
    metrics: &Arc<RuntimeMetrics>,
    peer_health: &Arc<RwLock<HashMap<String, PeerHealth>>>,
    peer_dead_after_failures: u32,
) {
    while let Ok(event) = event_rx.try_recv() {
        handle_node_event(
            event,
            counters,
            seen_ops,
            store,
            metrics,
            peer_health,
            peer_dead_after_failures,
        )
        .await;
    }
}

async fn handle_node_event(
    event: NodeEvent,
    counters: &Arc<RwLock<HashMap<String, GCounter>>>,
    seen_ops: &Arc<RwLock<HashSet<String>>>,
    store: &Arc<NxStore>,
    metrics: &Arc<RuntimeMetrics>,
    peer_health: &Arc<RwLock<HashMap<String, PeerHealth>>>,
    peer_dead_after_failures: u32,
) {
    match event {
        NodeEvent::OpsReceived { from, ops } => {
            debug!(from = %from, count = ops.len(), "received ops from peer");
            for op in ops {
                if let Err(e) = apply_remote_op(&op, counters, seen_ops, store, metrics).await {
                    metrics.record_sync_error();
                    error!(error = %e, "failed to apply remote op");
                }
            }
        }
        NodeEvent::PeerConnected {
            node_id,
            addr,
            peers_connected,
        } => {
            mark_known_peer_success(peer_health, &addr).await;
            metrics.record_peer_connect();
            metrics.set_peers_connected(peers_connected);
            info!(peer = %node_id, addr = %addr, "peer connected");
        }
        NodeEvent::PeerDisconnected {
            node_id,
            addr,
            peers_connected,
        } => {
            mark_known_peer_failure(peer_health, &addr, peer_dead_after_failures).await;
            metrics.record_peer_disconnect();
            metrics.set_peers_connected(peers_connected);
            info!(peer = %node_id, addr = %addr, "peer disconnected");
        }
    }
}

pub(crate) fn materialized_gcounter_key(key: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(GCOUNTER_STORE_PREFIX.len() + key.len());
    out.extend_from_slice(GCOUNTER_STORE_PREFIX.as_bytes());
    out.extend_from_slice(key.as_bytes());
    out
}

fn logical_gcounter_key(store_key: &[u8]) -> anyhow::Result<String> {
    let key = store_key
        .strip_prefix(GCOUNTER_STORE_PREFIX.as_bytes())
        .ok_or_else(|| anyhow::anyhow!("invalid GCounter materialized key"))?;

    String::from_utf8(key.to_vec())
        .map_err(|e| anyhow::anyhow!("invalid UTF-8 in GCounter materialized key: {e}"))
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

fn hydrate_gcounter_registry(
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

        let mut counter = GCounter::new();
        counter.increment(node_id, value);
        counters.insert(key, counter);
        hydrated += 1;
    }

    debug!(count = hydrated, "hydrated GCounter registry from sled");
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

/// Apply an operation received from a remote peer.
async fn apply_remote_op(
    op: &Op,
    counters: &Arc<RwLock<HashMap<String, GCounter>>>,
    seen_ops: &Arc<RwLock<HashSet<String>>>,
    store: &Arc<NxStore>,
    metrics: &Arc<RuntimeMetrics>,
) -> anyhow::Result<()> {
    // Deduplication
    {
        let seen = seen_ops.read().await;
        if seen.contains(op.id.as_str()) {
            debug!(op_id = %op.id, "skipping duplicate op");
            return Ok(());
        }
    }

    // Apply according to type
    match &op.kind {
        OpKind::GCounterIncrement { key, increment } => {
            let mut counters = counters.write().await;
            let mut counter = counters.get(key).cloned().unwrap_or_else(GCounter::new);
            counter.increment(&op.origin, *increment);
            let total = counter.value();

            materialize_gcounter_value(store, key, total)?;
            counters.insert(key.clone(), counter);

            metrics.record_ops(1);
            debug!(op_id = %op.id, key = %key, from = %op.origin, increment = %increment, total, "applied remote increment");
        }
    }

    {
        let mut seen = seen_ops.write().await;
        seen.insert(op.id.as_str().to_string());
    }

    Ok(())
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
        {
            let counters_arc = handle.counters();
            let mut counters = counters_arc.write().await;
            let mut counter = counters.get(key).cloned().unwrap_or_else(GCounter::new);
            counter.increment(handle.node_id(), delta);
            let total = counter.value();

            materialize_gcounter_value(&handle.store(), key, total).unwrap();
            counters.insert(key.to_string(), counter);
        }

        let op = Op::gcounter_increment(handle.node_id().clone(), key, delta);
        handle.op_sender().send(op).await.unwrap();
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
        let seen_ops = Arc::new(RwLock::new(HashSet::new()));
        let store = temp_store();
        let metrics = metrics();
        let node_id = NodeId::generate();

        handle_node_event(
            NodeEvent::PeerConnected {
                node_id: node_id.clone(),
                addr: peer.to_string(),
                peers_connected: 1,
            },
            &counters,
            &seen_ops,
            &store,
            &metrics,
            &peer_health,
            2,
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
            &counters,
            &seen_ops,
            &store,
            &metrics,
            &peer_health,
            2,
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
        let seen_ops = Arc::new(RwLock::new(HashSet::new()));
        let store = temp_store();
        let metrics = metrics();

        handle_node_event(
            NodeEvent::PeerConnected {
                node_id: NodeId::generate(),
                addr: "127.0.0.1:42001".to_string(),
                peers_connected: 1,
            },
            &counters,
            &seen_ops,
            &store,
            &metrics,
            &peer_health,
            1,
        )
        .await;

        assert!(peer_health.read().await.is_empty());
    }

    #[tokio::test]
    async fn apply_remote_op_materializes_counter_value() {
        let store = temp_store();
        let counters = Arc::new(RwLock::new(HashMap::new()));
        let seen_ops = Arc::new(RwLock::new(HashSet::new()));
        let origin = NodeId::new("remote-a");
        let op = Op::gcounter_increment(origin, "counter:visits", 3);

        apply_remote_op(&op, &counters, &seen_ops, &store, &metrics())
            .await
            .unwrap();

        let key = materialized_gcounter_key("counter:visits");
        let bytes = store.get(&key).unwrap().unwrap();
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&bytes);

        assert_eq!(u64::from_le_bytes(buf), 3);
    }

    #[tokio::test]
    async fn apply_remote_op_duplicate_does_not_double_materialize() {
        let store = temp_store();
        let counters = Arc::new(RwLock::new(HashMap::new()));
        let seen_ops = Arc::new(RwLock::new(HashSet::new()));
        let origin = NodeId::new("remote-a");
        let op = Op::gcounter_increment(origin, "counter:visits", 3);

        apply_remote_op(&op, &counters, &seen_ops, &store, &metrics())
            .await
            .unwrap();
        apply_remote_op(&op, &counters, &seen_ops, &store, &metrics())
            .await
            .unwrap();

        let key = materialized_gcounter_key("counter:visits");
        let bytes = store.get(&key).unwrap().unwrap();
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&bytes);

        assert_eq!(u64::from_le_bytes(buf), 3);
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
