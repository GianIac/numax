use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant as StdInstant};

use nx_net::{NetError, NodeEvent, WireRetryPolicy};
use nx_store::Store as NxStore;
use nx_sync::Op;
use tokio::sync::{RwLock, mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use super::apply::apply_remote_ops;
use super::peer::{
    ConfiguredPeerConnectContext, ConfiguredPeerConnectOutcome, PeerReconnectState,
    mark_known_peer_failure, mark_known_peer_success, mark_peer_failure, mark_peer_success,
    normalize_peer_dead_after_failures, normalize_reconnect_delay,
};
use super::storage::persist_local_ops_batch;
use super::types::*;

pub(super) fn spawn_broadcast_loop(
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

pub(super) async fn try_connect_configured_peer(
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

            let outcome = connect_failure_outcome(&e);
            match outcome {
                ConfiguredPeerConnectOutcome::Fatal => {
                    debug!(
                        peer = %peer_addr,
                        error = %e,
                        "configured peer connect failed with fatal wire error"
                    );
                }
                ConfiguredPeerConnectOutcome::RetryAfter(delay) => {
                    debug!(
                        peer = %peer_addr,
                        error = %e,
                        retry_after_ms = delay.as_millis(),
                        "configured peer connect rate limited"
                    );
                }
                ConfiguredPeerConnectOutcome::Failed => {
                    debug!(peer = %peer_addr, error = %e, "configured peer connect failed");
                }
                ConfiguredPeerConnectOutcome::Connected
                | ConfiguredPeerConnectOutcome::AlreadyConnected
                | ConfiguredPeerConnectOutcome::SlotLimitReached => {}
            }
            outcome
        }
    }
}

fn connect_failure_outcome(error: &NetError) -> ConfiguredPeerConnectOutcome {
    match error {
        NetError::Wire(wire_error) => match wire_error.retry_policy() {
            WireRetryPolicy::Fatal => ConfiguredPeerConnectOutcome::Fatal,
            WireRetryPolicy::RetryAfter(delay) => ConfiguredPeerConnectOutcome::RetryAfter(delay),
            WireRetryPolicy::Retry | WireRetryPolicy::RequestFatal => {
                ConfiguredPeerConnectOutcome::Failed
            }
            _ => ConfiguredPeerConnectOutcome::Failed,
        },
        _ => ConfiguredPeerConnectOutcome::Failed,
    }
}

fn bounded_retry_after(delay: Duration, max_delay: Duration) -> Duration {
    normalize_reconnect_delay(delay).min(max_delay)
}

pub(super) fn spawn_reconnect_loop(context: ReconnectLoopContext) -> Option<JoinHandle<()>> {
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

                if peer.stopped {
                    continue;
                }

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
                    ConfiguredPeerConnectOutcome::RetryAfter(delay) => {
                        let retry_after = peer.record_retry_after(
                            bounded_retry_after(delay, max_delay),
                            StdInstant::now(),
                        );
                        sleep_for =
                            Some(sleep_for.map_or(retry_after, |current| current.min(retry_after)));
                    }
                    ConfiguredPeerConnectOutcome::Fatal => {
                        warn!(
                            peer = %peer_addr,
                            "stopping automatic reconnect for configured peer after fatal wire error"
                        );
                        peer.stop();
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

pub(super) fn spawn_anti_entropy_loop(context: AntiEntropyLoopContext) -> Option<JoinHandle<()>> {
    let AntiEntropyLoopContext {
        node,
        peers,
        interval,
        mut shutdown_rx,
        metrics,
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

                        // A single "last seen OpId" is not a safe causal frontier: a peer can
                        // receive a newer op while an older broadcast was dropped. Until the
                        // protocol has contiguous/causal metadata, anti-entropy pulls the bounded
                        // op-log and relies on OpId deduplication on the receiver.
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

pub(super) fn normalize_anti_entropy_interval(interval: Duration) -> Duration {
    interval.max(Duration::from_millis(1))
}

pub(super) fn normalize_op_log_limit(limit: usize) -> usize {
    limit.max(1)
}

pub(super) fn normalize_seen_ops_limit(limit: usize) -> usize {
    limit.max(1)
}

pub(super) async fn broadcast_batch(
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

pub(super) async fn drain_broadcast_queue(
    context: &BroadcastLoopContext,
    op_rx: &mut mpsc::Receiver<Op>,
) {
    while let Ok(first) = op_rx.try_recv() {
        broadcast_batch(context, op_rx, first).await;
    }
}

pub(super) async fn remember_ops(
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

pub(super) fn plan_seen_evictions(seen: &SeenOps, inserted_ids: &[String]) -> Vec<String> {
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

pub(super) fn apply_seen_insertions(
    seen: &mut SeenOps,
    inserted_ids: &[String],
    evicted: &[String],
) {
    for op_id in inserted_ids {
        seen.ids.insert(op_id.clone());
        seen.order.push_back(op_id.clone());
    }
    for op_id in evicted {
        seen.ids.remove(op_id);
        let _ = seen.order.pop_front();
    }
}

pub(super) fn plan_op_log_evictions(log: &[Op], inserted_count: usize, limit: usize) -> Vec<Op> {
    let limit = normalize_op_log_limit(limit);
    let total_len = log.len().saturating_add(inserted_count);
    let remove_count = total_len.saturating_sub(limit);
    log.iter().take(remove_count).cloned().collect()
}

pub(super) fn prune_op_log_and_return_evicted(log: &mut Vec<Op>, limit: usize) -> Vec<Op> {
    let limit = normalize_op_log_limit(limit);
    if log.len() > limit {
        let remove_count = log.len() - limit;
        log.drain(..remove_count).collect()
    } else {
        Vec::new()
    }
}

pub(super) async fn op_log_since(
    op_log: &Arc<RwLock<Vec<Op>>>,
    since_op_id: Option<&str>,
) -> Vec<Op> {
    let log = op_log.read().await;
    match since_op_id {
        Some(op_id) => log
            .iter()
            .position(|op| op.id.as_str() == op_id)
            .map_or_else(|| log.clone(), |index| log[index + 1..].to_vec()),
        None => log.clone(),
    }
}

pub(super) async fn drain_node_events(
    event_rx: &mut mpsc::Receiver<NodeEvent>,
    context: &NodeEventContext,
) {
    while let Ok(event) = event_rx.try_recv() {
        handle_node_event(event, context).await;
    }
}

pub(super) async fn handle_node_event(event: NodeEvent, context: &NodeEventContext) {
    match event {
        NodeEvent::OpsReceived { from, ops } => {
            debug!(from = %from, count = ops.len(), "received ops from peer");
            let last_received_op_id = ops.last().map(|op| op.id.as_str().to_string());
            let apply_context = RemoteOpApplyContext {
                counters: &context.counters,
                pncounters: &context.pncounters,
                lww_registers: &context.lww_registers,
                lww_maps: &context.lww_maps,
                orsets: &context.orsets,
                rgas: &context.rgas,
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

// End of main code. Test below:
#[cfg(test)]
mod tests {
    use super::*;
    use crate::observability::RuntimeMetrics;
    use crate::sync_manager::peer::{PeerHealth, PeerHealthState};
    use crate::sync_manager::storage::materialized_gcounter_key;
    use nx_net::{Node, NodeConfig, PROTOCOL_VERSION, WireError};
    use nx_sync::{GCounter, NodeId};
    use std::collections::HashMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_store() -> Arc<NxStore> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        path.push(format!("numax-core-sync-replication-test-{nanos}-{seq}"));
        Arc::new(NxStore::open(path).unwrap())
    }

    fn metrics() -> Arc<RuntimeMetrics> {
        Arc::new(RuntimeMetrics::default())
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
            lww_maps: Arc::new(RwLock::new(HashMap::new())),
            orsets: Arc::new(RwLock::new(HashMap::new())),
            rgas: Arc::new(RwLock::new(HashMap::new())),
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
    fn retry_after_is_bounded_by_configured_max_delay() {
        assert_eq!(
            bounded_retry_after(Duration::from_secs(60), Duration::from_secs(5)),
            Duration::from_secs(5)
        );
        assert_eq!(
            bounded_retry_after(Duration::ZERO, Duration::from_secs(5)),
            Duration::from_millis(1)
        );
    }

    #[test]
    fn connect_failure_outcome_follows_wire_retry_policy() {
        assert_eq!(
            connect_failure_outcome(&NetError::Wire(WireError::ProtocolMismatch {
                expected: PROTOCOL_VERSION,
                got: PROTOCOL_VERSION - 1,
            })),
            ConfiguredPeerConnectOutcome::Fatal
        );
        assert_eq!(
            connect_failure_outcome(&NetError::Wire(WireError::RateLimited {
                retry_after_ms: Some(250),
            })),
            ConfiguredPeerConnectOutcome::RetryAfter(Duration::from_millis(250))
        );
        assert_eq!(
            connect_failure_outcome(&NetError::Wire(WireError::Internal {
                reason: "temporary".into(),
            })),
            ConfiguredPeerConnectOutcome::Failed
        );
        assert_eq!(
            connect_failure_outcome(&NetError::Wire(WireError::OpRejected {
                reason: "bad op".into(),
            })),
            ConfiguredPeerConnectOutcome::Failed
        );
        assert_eq!(
            connect_failure_outcome(&NetError::Timeout),
            ConfiguredPeerConnectOutcome::Failed
        );
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
        let key = materialized_gcounter_key("counter:visits");
        assert!(store.get(&key).unwrap().is_some());
    }
}
