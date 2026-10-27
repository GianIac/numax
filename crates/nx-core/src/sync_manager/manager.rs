use std::collections::HashMap;
use std::sync::{Arc, atomic::AtomicU64};

use nx_net::{BootstrapServerConfig, Node, NodeConfig, PeerConnectionInfo};
use nx_store::Store as NxStore;
use nx_sync::{GCounter, LwwMap, LwwRegister, NodeId, ORSet, Op, PNCounter, Rga};
use tokio::sync::{RwLock, mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::observability::RuntimeMetrics;
use crate::sync_config::SyncConfig;
use crate::{
    DEFAULT_MAX_PEER_CANDIDATES, DiscoveryProvider, DiscoveryRuntimeConfig, StaticDiscovery,
};

use super::candidates::DiscoveryCoordinator;
use super::peer::{
    ConfiguredPeerConnectContext, ConfiguredPeerConnectOutcome, PeerHealth, PeerHealthState,
    normalize_peer_dead_after_failures,
};
use super::replication::{
    drain_node_events, handle_node_event, normalize_op_log_limit, normalize_seen_ops_limit,
    spawn_anti_entropy_loop, spawn_broadcast_loop, spawn_reconnect_loop,
    try_connect_configured_peer,
};
use super::schema::ensure_sync_schema;
use super::storage::{
    hydrate_gcounter_registry, hydrate_lww_map_registry, hydrate_lww_register_registry,
    hydrate_op_log, hydrate_orset_registry, hydrate_pncounter_registry, hydrate_rga_registry,
    hydrate_seen_ops,
};
use super::types::*;

/// Clonable, cheap handle to the SyncManager exposed to host calls.
#[derive(Clone)]
pub struct SyncHandle {
    node_id: NodeId,
    op_tx: mpsc::Sender<Op>,
    counters: Arc<RwLock<HashMap<String, GCounter>>>,
    pncounters: Arc<RwLock<HashMap<String, PNCounter>>>,
    lww_registers: Arc<RwLock<HashMap<String, LwwRegister>>>,
    lww_maps: Arc<RwLock<HashMap<String, LwwMap>>>,
    orsets: Arc<RwLock<HashMap<String, ORSet>>>,
    rgas: Arc<RwLock<HashMap<String, Rga>>>,
    store: Arc<NxStore>,
    metrics: Arc<RuntimeMetrics>,
    active_connections: Arc<RwLock<HashMap<String, PeerConnectionInfo>>>,
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

    /// Read-side handle over the LWW-Map registry.
    pub fn lww_maps(&self) -> Arc<RwLock<HashMap<String, LwwMap>>> {
        Arc::clone(&self.lww_maps)
    }

    /// Read-side handle over the ORSet registry.
    pub fn orsets(&self) -> Arc<RwLock<HashMap<String, ORSet>>> {
        Arc::clone(&self.orsets)
    }

    /// Read-side handle over the RGA registry.
    pub fn rgas(&self) -> Arc<RwLock<HashMap<String, Rga>>> {
        Arc::clone(&self.rgas)
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
        let peers = self.active_connections.read().await;
        let mut peers = peers
            .iter()
            .map(|(addr, connection)| (addr.clone(), connection.identity.node_id.clone()))
            .collect::<Vec<_>>();
        peers.sort_by(|(addr_a, _), (addr_b, _)| addr_a.cmp(addr_b));
        peers
    }

    /// Active transport connections with handshake identity verification details.
    pub async fn active_connections(&self) -> Vec<PeerConnectionInfo> {
        let connections = self.active_connections.read().await;
        let mut connections = connections.values().cloned().collect::<Vec<_>>();
        connections.sort_by(|left, right| {
            left.transport_addr
                .cmp(&right.transport_addr)
                .then_with(|| left.dialed_endpoint.cmp(&right.dialed_endpoint))
        });
        connections
    }
}

pub struct SyncManager {
    /// NodeId.
    node_id: NodeId,

    /// SyncConfig
    config: SyncConfig,

    /// Discovery sources whose contributions feed the shared candidate set.
    discovery_providers: Vec<DiscoveryProvider>,

    /// Discovery coordination, validation, and bound policy.
    discovery_config: DiscoveryRuntimeConfig,

    /// Owner of provider watches and the live peer candidate registry.
    discovery_coordinator: Option<DiscoveryCoordinator>,

    /// Network node. Wrapped in `Arc` so the broadcast drain task spawned
    /// by `start` can share ownership with the manager.
    node: Option<Arc<Node>>,

    /// GCounter
    counters: Arc<RwLock<HashMap<String, GCounter>>>,

    /// PNCounter
    pncounters: Arc<RwLock<HashMap<String, PNCounter>>>,

    /// LWW-Register
    lww_registers: Arc<RwLock<HashMap<String, LwwRegister>>>,

    /// LWW-Map
    lww_maps: Arc<RwLock<HashMap<String, LwwMap>>>,

    /// ORSet
    orsets: Arc<RwLock<HashMap<String, ORSet>>>,

    /// RGA
    rgas: Arc<RwLock<HashMap<String, Rga>>>,

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

    /// Health state for current discovery candidates, keyed by dial endpoint.
    peer_health: Arc<RwLock<HashMap<String, PeerHealth>>>,

    /// Active connections, keyed by the address used by the network node.
    active_connections: Arc<RwLock<HashMap<String, PeerConnectionInfo>>>,

    /// Last received OpId per peer NodeId, for observation only, not a causal frontier.
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
    /// Create a SyncManager, panicking if configuration or persistence is invalid.
    ///
    /// Runtime integrations should prefer [`Self::try_new`] so configuration and
    /// schema errors can be reported without terminating the process.
    pub fn new(
        node_id: NodeId,
        config: SyncConfig,
        store: Arc<NxStore>,
        metrics: Arc<RuntimeMetrics>,
    ) -> Self {
        Self::try_new(node_id, config, store, metrics).expect("failed to initialize SyncManager")
    }

    /// Create a SyncManager after validating configuration and managed persistence schemas.
    pub fn try_new(
        node_id: NodeId,
        config: SyncConfig,
        store: Arc<NxStore>,
        metrics: Arc<RuntimeMetrics>,
    ) -> anyhow::Result<Self> {
        let static_discovery = Arc::new(StaticDiscovery::new(config.peers.clone()));
        // Explicit peers were accepted as a finite caller-owned list before the
        // discovery coordinator existed. Keep that compatibility while retaining
        // the configured bound for every dynamic-discovery construction path.
        let discovery_config = DiscoveryRuntimeConfig::default()
            .with_max_candidates(DEFAULT_MAX_PEER_CANDIDATES.max(config.peers.len()));
        Self::try_new_with_discovery(
            node_id,
            config,
            store,
            metrics,
            vec![DiscoveryProvider::new("static", static_discovery)],
            discovery_config,
        )
    }

    /// Create a manager with explicitly owned discovery sources and policy.
    pub fn try_new_with_discovery(
        node_id: NodeId,
        config: SyncConfig,
        store: Arc<NxStore>,
        metrics: Arc<RuntimeMetrics>,
        discovery_providers: Vec<DiscoveryProvider>,
        discovery_config: DiscoveryRuntimeConfig,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        ensure_sync_schema(&store)?;

        let (op_tx, op_rx) = mpsc::channel(config.queued_ops_limit.max(1));
        let op_log_limit = normalize_op_log_limit(config.op_log_limit);
        let seen_ops_limit = normalize_seen_ops_limit(config.seen_ops_limit);
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let mut counters = HashMap::new();
        let mut pncounters = HashMap::new();
        let mut lww_registers = HashMap::new();
        let mut lww_maps = HashMap::new();
        let mut orsets = HashMap::new();
        let mut rgas = HashMap::new();
        let (op_log, op_log_next_sequence) = hydrate_op_log(&store, op_log_limit)?;
        let peer_health = config
            .peers
            .iter()
            .map(|peer| (peer.clone(), PeerHealth::default()))
            .collect::<HashMap<_, _>>();

        let (seen_ops, seen_ops_next_sequence) = hydrate_seen_ops(&store, seen_ops_limit)?;

        hydrate_gcounter_registry(&store, &node_id, &mut counters)?;
        hydrate_pncounter_registry(&store, &node_id, &mut pncounters)?;
        hydrate_lww_register_registry(&store, &mut lww_registers)?;
        hydrate_lww_map_registry(&store, &mut lww_maps)?;
        hydrate_orset_registry(&store, &mut orsets)?;
        hydrate_rga_registry(&store, &mut rgas)?;
        let counters = Arc::new(RwLock::new(counters));
        let pncounters = Arc::new(RwLock::new(pncounters));
        let lww_registers = Arc::new(RwLock::new(lww_registers));
        let lww_maps = Arc::new(RwLock::new(lww_maps));
        let orsets = Arc::new(RwLock::new(orsets));
        let rgas = Arc::new(RwLock::new(rgas));

        Ok(Self {
            node_id,
            config,
            discovery_providers,
            discovery_config,
            discovery_coordinator: None,
            node: None,
            counters,
            pncounters,
            lww_registers,
            lww_maps,
            orsets,
            rgas,
            store,
            metrics,
            seen_ops: Arc::new(RwLock::new(seen_ops)),
            seen_ops_next_sequence: Arc::new(AtomicU64::new(seen_ops_next_sequence)),
            op_log: Arc::new(RwLock::new(op_log)),
            op_log_next_sequence: Arc::new(AtomicU64::new(op_log_next_sequence)),
            peer_health: Arc::new(RwLock::new(peer_health)),
            active_connections: Arc::new(RwLock::new(HashMap::new())),
            anti_entropy_watermarks: Arc::new(RwLock::new(HashMap::new())),
            op_tx,
            op_rx: Some(op_rx),
            shutdown_tx,
            event_task: None,
            broadcast_task: None,
            reconnect_task: None,
            anti_entropy_task: None,
        })
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
            lww_maps: Arc::clone(&self.lww_maps),
            orsets: Arc::clone(&self.orsets),
            rgas: Arc::clone(&self.rgas),
            store: Arc::clone(&self.store),
            metrics: Arc::clone(&self.metrics),
            active_connections: Arc::clone(&self.active_connections),
        }
    }

    /// Bind the listener and start discovery, replication, and reconnect tasks.
    ///
    /// Initial peers are dialed in the background. Success means local services
    /// are started, not that a peer is connected or replication has settled.
    pub async fn start(&mut self) -> anyhow::Result<()> {
        self.config.validate()?;
        let listen_addr = match &self.config.listen_addr {
            Some(addr) => addr.clone(),
            None => {
                info!("sync disabled: no listen_addr");
                return Ok(());
            }
        };

        if self.node.is_some() || self.op_rx.is_none() {
            anyhow::bail!("sync manager is already started");
        }

        // Reject local configuration before acquiring provider watches or tasks.
        // The reconnect loop consumes live candidates, not NodeConfig::initial_peers.
        let mut node_config = NodeConfig::new(self.node_id.clone(), &listen_addr)
            .with_max_peers(self.config.max_peers)
            .with_max_message_size(self.config.max_message_size)
            .with_socket_timeout(self.config.socket_timeout)
            .with_serialization_format(self.config.serialization_format)
            .with_event_channel_capacity(self.config.queued_ops_limit.max(1));
        let bootstrap_server = BootstrapServerConfig::new(self.discovery_config.cluster_id())?
            .with_max_cached_candidates(self.discovery_config.max_candidates())?
            .with_max_response_candidates(
                self.discovery_config
                    .max_candidates()
                    .min(nx_net::MAX_BOOTSTRAP_RESPONSE_CAPACITY),
            )?;
        if let Some(tls) = self.config.tls.clone() {
            node_config = node_config.with_tls(tls);
        }

        let mut node = Node::try_new_with_bootstrap_server(node_config, bootstrap_server)?;
        let Some(mut event_rx) = node.take_event_receiver() else {
            anyhow::bail!("network event receiver is unavailable");
        };

        // Provider watches are acquired before binding so discovery startup is
        // atomic with respect to network resources. All later failures roll back.
        let mut discovery_coordinator = DiscoveryCoordinator::start(
            self.discovery_providers.clone(),
            self.discovery_config.clone(),
        )
        .await?;
        let candidates_rx = discovery_coordinator.candidates();

        let bound_addr = match node.start_listener().await {
            Ok(bound_addr) => bound_addr,
            Err(error) => {
                rollback_discovery(&mut discovery_coordinator).await;
                return Err(error.into());
            }
        };
        let advertised_endpoint = match discovery_coordinator
            .configure_local_endpoint(bound_addr)
            .await
        {
            Ok(endpoint) => endpoint,
            Err(error) => {
                node.shutdown().await;
                rollback_discovery(&mut discovery_coordinator).await;
                return Err(error.into());
            }
        };
        if let Some(endpoint) = &advertised_endpoint
            && let Err(error) = node.announce_bootstrap_endpoint(endpoint.clone())
        {
            node.shutdown().await;
            rollback_discovery(&mut discovery_coordinator).await;
            return Err(error.into());
        }
        if let Err(error) = discovery_coordinator
            .announce(advertised_endpoint.as_deref())
            .await
        {
            node.shutdown().await;
            rollback_discovery(&mut discovery_coordinator).await;
            return Err(error.into());
        }

        let initial_candidates = Arc::clone(&candidates_rx.borrow());
        *self.peer_health.write().await = initial_candidates
            .iter()
            .map(|peer| (peer.clone(), PeerHealth::default()))
            .collect();

        let Some(op_rx) = self.op_rx.take() else {
            node.shutdown().await;
            rollback_discovery(&mut discovery_coordinator).await;
            anyhow::bail!("sync manager is already started");
        };

        // Move the node into an Arc so it can be shared between the manager and the broadcast drain task.
        let node = Arc::new(node);
        self.node = Some(Arc::clone(&node));

        // Inbound loop: apply remote ops into the counter registry.
        let event_context = NodeEventContext {
            counters: Arc::clone(&self.counters),
            pncounters: Arc::clone(&self.pncounters),
            lww_registers: Arc::clone(&self.lww_registers),
            lww_maps: Arc::clone(&self.lww_maps),
            orsets: Arc::clone(&self.orsets),
            rgas: Arc::clone(&self.rgas),
            seen_ops: Arc::clone(&self.seen_ops),
            seen_ops_next_sequence: Arc::clone(&self.seen_ops_next_sequence),
            op_log: Arc::clone(&self.op_log),
            op_log_next_sequence: Arc::clone(&self.op_log_next_sequence),
            op_log_limit: normalize_op_log_limit(self.config.op_log_limit),
            store: Arc::clone(&self.store),
            metrics: Arc::clone(&self.metrics),
            node: Arc::clone(&node),
            peer_health: Arc::clone(&self.peer_health),
            active_connections: Arc::clone(&self.active_connections),
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
            candidates_rx,
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
            interval: self.config.anti_entropy_interval,
            shutdown_rx: self.shutdown_tx.subscribe(),
            metrics: Arc::clone(&self.metrics),
        });

        self.discovery_coordinator = Some(discovery_coordinator);

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

    /// Retry connecting to the current discovery candidates.
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
        let Some(coordinator) = self.discovery_coordinator.as_ref() else {
            return;
        };
        let candidates_rx = coordinator.candidates();
        let candidates = Arc::clone(&candidates_rx.borrow());
        for peer_addr in candidates.iter() {
            if matches!(
                try_connect_configured_peer(&connect_context, peer_addr).await,
                ConfiguredPeerConnectOutcome::SlotLimitReached
            ) {
                break;
            }
        }
    }

    /// Returns the current health state of a discovery candidate.
    pub async fn peer_health_state(&self, addr: &str) -> Option<PeerHealthState> {
        let peer_health = self.peer_health.read().await;
        peer_health.get(addr).map(|health| health.state)
    }

    /// Returns the current ordered, deduplicated discovery candidate snapshot.
    pub fn peer_candidates(&self) -> Vec<String> {
        let Some(coordinator) = self.discovery_coordinator.as_ref() else {
            return Vec::new();
        };
        let candidates = coordinator.candidates();
        candidates.borrow().as_ref().clone()
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

    /// Returns the visible entries of an LWW-Map.
    pub async fn get_lww_map_entries(&self, key: &str) -> Vec<(String, Vec<u8>)> {
        let maps = self.lww_maps.read().await;
        maps.get(key).map(|map| map.entries()).unwrap_or_default()
    }

    /// Returns the visible elements of an ORSet.
    pub async fn get_orset_elements(&self, key: &str) -> Vec<String> {
        let sets = self.orsets.read().await;
        sets.get(key).map(|set| set.elements()).unwrap_or_default()
    }

    /// Returns the visible values of an RGA.
    pub async fn get_rga_values(&self, key: &str) -> Vec<Vec<u8>> {
        let rgas = self.rgas.read().await;
        rgas.get(key).map(|rga| rga.values()).unwrap_or_default()
    }

    /// Gracefully stop sync tasks and close network connections.
    pub async fn shutdown(&mut self) -> anyhow::Result<()> {
        let _ = self.shutdown_tx.send(true);
        let mut discovery_coordinator = self.discovery_coordinator.take();
        if let Some(coordinator) = discovery_coordinator.as_ref() {
            coordinator.request_shutdown();
        }
        let mut discovery_error = None;

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

        if let Some(mut coordinator) = discovery_coordinator.take()
            && let Err(error) = coordinator.shutdown().await
        {
            warn!(error = %error, "discovery shutdown failed");
            discovery_error = Some(error);
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
        discovery_error.map_or(Ok(()), |error| Err(error.into()))
    }
}

async fn rollback_discovery(coordinator: &mut DiscoveryCoordinator) {
    if let Err(error) = coordinator.shutdown().await {
        warn!(error = %error, "discovery rollback failed");
    }
}

// End of main code. Test below:

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;
