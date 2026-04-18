use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use nx_net::{Node, NodeConfig, NodeEvent};
use nx_sync::{GCounter, NodeId, Op, OpKind};
use tokio::sync::{RwLock, mpsc};
use tracing::{debug, error, info, warn};

use crate::sync_config::SyncConfig;

pub struct SyncManager {
    /// NodeId.
    node_id: NodeId,

    /// SyncConfig
    config: SyncConfig,

    /// Node
    node: Option<Node>,

    /// GCounter
    counters: Arc<RwLock<HashMap<String, GCounter>>>,

    /// OpId
    seen_ops: Arc<RwLock<HashSet<String>>>,

    /// Channel to send Ops to broadcast.
    op_tx: mpsc::Sender<Op>,

    /// Receiver
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

    /// start networking
    pub async fn start(&mut self) -> anyhow::Result<()> {
        let listen_addr = match &self.config.listen_addr {
            Some(addr) => addr.clone(),
            None => {
                info!("sync disabled: no listen_addr");
                return Ok(());
            }
        };

        // crate and start the node
        let node_config = NodeConfig::new(self.node_id.clone(), &listen_addr)
            .with_peers(self.config.peers.clone());

        let mut node = Node::new(node_config);
        let mut event_rx = node.take_event_receiver().unwrap();

        node.start_listener().await?;

        // Connect to initial peers
        for peer_addr in &self.config.peers {
            if let Err(e) = node.connect_to_peer(peer_addr).await {
                warn!(peer = %peer_addr, error = %e, "failed to connect to peer");
            }
        }

        self.node = Some(node);

        // Start event loop in background
        let counters = Arc::clone(&self.counters);
        let seen_ops = Arc::clone(&self.seen_ops);
        let config = self.config.clone();

        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                match event {
                    NodeEvent::OpsReceived { from, ops } => {
                        debug!(from = %from, count = ops.len(), "received ops from peer");
                        for op in ops {
                            if let Err(e) =
                                apply_remote_op(&op, &counters, &seen_ops, &config).await
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
        });

        Ok(())
    }

    pub async fn on_local_write(&self, key: &str, _value: &[u8]) -> anyhow::Result<()> {
        if !self.config.is_replicated(key) {
            return Ok(());
        }

        let op = Op::gcounter_increment(self.node_id.clone(), key, 1);

        // apply localy
        {
            let mut counters = self.counters.write().await;
            let counter = counters
                .entry(key.to_string())
                .or_insert_with(GCounter::new);
            counter.increment(&self.node_id, 1);
            debug!(key = %key, value = counter.value(), "local counter updated");
        }

        // Brand as seen
        {
            let mut seen = self.seen_ops.write().await;
            seen.insert(op.id.as_str().to_string());
        }

        // send for broadcast
        if let Err(e) = self.op_tx.send(op).await {
            error!(error = %e, "failed to queue op for broadcast");
        }

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

/// Apply an operation received from a remote peer.
async fn apply_remote_op(
    op: &Op,
    counters: &Arc<RwLock<HashMap<String, GCounter>>>,
    seen_ops: &Arc<RwLock<HashSet<String>>>,
    config: &SyncConfig,
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
            if !config.is_replicated(key) {
                warn!(key = %key, "received op for non-replicated key");
                return Ok(());
            }

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
