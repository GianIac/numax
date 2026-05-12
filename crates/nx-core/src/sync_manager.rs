use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use nx_net::{Node, NodeConfig, NodeEvent};
use nx_store::Store as NxStore;
use nx_sync::{GCounter, NodeId, Op, OpKind};
use tokio::sync::{RwLock, mpsc};
use tracing::{debug, error, info, warn};

use crate::sync_config::SyncConfig;

/// Upper bound on how many ops we coalesce into a single PushOps message.
const BROADCAST_BATCH_MAX: usize = 64;
const GCOUNTER_STORE_PREFIX: &str = "__nx/crdt/gcounter/";

/// Clonable, cheap handle to the SyncManager exposed to host calls.
#[derive(Clone)]
pub struct SyncHandle {
    node_id: NodeId,
    op_tx: mpsc::Sender<Op>,
    counters: Arc<RwLock<HashMap<String, GCounter>>>,
    store: Arc<NxStore>,
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

    /// OpId
    seen_ops: Arc<RwLock<HashSet<String>>>,

    /// Channel to send Ops to broadcast.
    op_tx: mpsc::Sender<Op>,

    /// Receiver drained by `start` into the broadcast task.
    op_rx: Option<mpsc::Receiver<Op>>,
}

impl SyncManager {
    /// new SyncManager.
    pub fn new(node_id: NodeId, config: SyncConfig, store: Arc<NxStore>) -> Self {
        let (op_tx, op_rx) = mpsc::channel(100);

        Self {
            node_id,
            config,
            node: None,
            counters: Arc::new(RwLock::new(HashMap::new())),
            store,
            seen_ops: Arc::new(RwLock::new(HashSet::new())),
            op_tx,
            op_rx: Some(op_rx),
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
            .with_peers(self.config.peers.clone());

        if let Some(tls) = self.config.tls.clone() {
            node_config = node_config.with_tls(tls);
        }

        let mut node = Node::new(node_config);
        let mut event_rx = node.take_event_receiver().unwrap();

        node.start_listener().await?;

        // Connect to initial peers.
        for peer_addr in &self.config.peers {
            if let Err(e) = node.connect_to_peer(peer_addr).await {
                warn!(peer = %peer_addr, error = %e, "failed to connect to peer");
            }
        }

        // Move the node into an Arc so it can be shared between the manager and the broadcast drain task.
        let node = Arc::new(node);
        self.node = Some(Arc::clone(&node));

        // Inbound loop: apply remote ops into the counter registry.
        let counters = Arc::clone(&self.counters);
        let seen_ops = Arc::clone(&self.seen_ops);
        let store = Arc::clone(&self.store);
        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                match event {
                    NodeEvent::OpsReceived { from, ops } => {
                        debug!(from = %from, count = ops.len(), "received ops from peer");
                        for op in ops {
                            if let Err(e) = apply_remote_op(&op, &counters, &seen_ops, &store).await
                            {
                                error!(error = %e, "failed to apply remote op");
                            }
                        }
                    }
                    NodeEvent::PeerConnected { node_id } => {
                        info!(peer = %node_id, "peer connected");
                    }
                    NodeEvent::PeerDisconnected { node_id } => {
                        info!(peer = %node_id, "peer disconnected");
                    }
                }
            }
            debug!("event loop terminated");
        });

        // Outbound loop: drain locally-produced ops into the network.
        let op_rx = self
            .op_rx
            .take()
            .expect("op_rx already taken: SyncManager::start called twice?");
        spawn_broadcast_loop(Arc::clone(&node), op_rx);

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

    /// Broadcast of pending operations, this method can be used for forced flush
    pub async fn broadcast_pending(&self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Returns the current value of a GCounter.
    pub async fn get_counter_value(&self, key: &str) -> u64 {
        let counters = self.counters.read().await;
        counters.get(key).map(|c| c.value()).unwrap_or(0)
    }
}

/// Spawn the outbound broadcast drain loop.
fn spawn_broadcast_loop(node: Arc<Node>, mut op_rx: mpsc::Receiver<Op>) {
    tokio::spawn(async move {
        while let Some(first) = op_rx.recv().await {
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
            if let Err(e) = node.broadcast_ops(batch).await {
                warn!(error = %e, count, "broadcast failed; ops dropped for this round");
            } else {
                debug!(count, "broadcast batch sent");
            }
        }
        debug!("broadcast loop terminated (op channel closed)");
    });
}

pub(crate) fn materialized_gcounter_key(key: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(GCOUNTER_STORE_PREFIX.len() + key.len());
    out.extend_from_slice(GCOUNTER_STORE_PREFIX.as_bytes());
    out.extend_from_slice(key.as_bytes());
    out
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

            debug!(key = %key, from = %op.origin, increment = %increment, total, "applied remote increment");
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
        let mut manager = SyncManager::new(NodeId::generate(), config, Arc::clone(&store));
        let handle = manager.handle();
        manager.start().await.unwrap();
        (manager, handle, store)
    }

    #[tokio::test]
    async fn apply_remote_op_materializes_counter_value() {
        let store = temp_store();
        let counters = Arc::new(RwLock::new(HashMap::new()));
        let seen_ops = Arc::new(RwLock::new(HashSet::new()));
        let origin = NodeId::new("remote-a");
        let op = Op::gcounter_increment(origin, "counter:visits", 3);

        apply_remote_op(&op, &counters, &seen_ops, &store)
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

        apply_remote_op(&op, &counters, &seen_ops, &store)
            .await
            .unwrap();
        apply_remote_op(&op, &counters, &seen_ops, &store)
            .await
            .unwrap();

        let key = materialized_gcounter_key("counter:visits");
        let bytes = store.get(&key).unwrap().unwrap();
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&bytes);

        assert_eq!(u64::from_le_bytes(buf), 3);
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
