use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use nx_net::{Node, NodeConfig, NodeEvent};
use nx_sync::{GCounter, NodeId, Op, OpKind};
use tokio::sync::{RwLock, mpsc};
use tracing::{debug, error, info, warn};

use crate::sync_config::SyncConfig;

/// Upper bound on how many ops we coalesce into a single PushOps message.
const BROADCAST_BATCH_MAX: usize = 64;

/// Clonable, cheap handle to the SyncManager exposed to host calls.
#[derive(Clone)]
pub struct SyncHandle {
    node_id: NodeId,
    op_tx: mpsc::Sender<Op>,
    counters: Arc<RwLock<HashMap<String, GCounter>>>,
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

    /// OpId
    seen_ops: Arc<RwLock<HashSet<String>>>,

    /// Channel to send Ops to broadcast.
    op_tx: mpsc::Sender<Op>,

    /// Receiver drained by `start` into the broadcast task.
    op_rx: Option<mpsc::Receiver<Op>>,
}

impl SyncManager {
    /// new SyncManager.
    pub fn new(node_id: NodeId, config: SyncConfig) -> Self {
        let (op_tx, op_rx) = mpsc::channel(100);

        Self {
            node_id,
            config,
            node: None,
            counters: Arc::new(RwLock::new(HashMap::new())),
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
        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                match event {
                    NodeEvent::OpsReceived { from, ops } => {
                        debug!(from = %from, count = ops.len(), "received ops from peer");
                        for op in ops {
                            if let Err(e) = apply_remote_op(&op, &counters, &seen_ops).await {
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

/// Apply an operation received from a remote peer.
async fn apply_remote_op(
    op: &Op,
    counters: &Arc<RwLock<HashMap<String, GCounter>>>,
    seen_ops: &Arc<RwLock<HashSet<String>>>,
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
            let counter = counters.entry(key.clone()).or_insert_with(GCounter::new);
            counter.increment(&op.origin, *increment);

            debug!(key = %key, from = %op.origin, increment = %increment, total = counter.value(), "applied remote increment");
        }
    }

    {
        let mut seen = seen_ops.write().await;
        seen.insert(op.id.as_str().to_string());
    }

    Ok(())
}