use std::collections::{HashMap, HashSet};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant as StdInstant};

use nx_net::{NetError, NodeEvent, WireRetryPolicy};
#[cfg(test)]
use nx_store::Store as NxStore;
use nx_sync::Op;
use tokio::sync::{RwLock, mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use super::apply::apply_sequenced_ops;
use super::manager::AcceptedLocalOp;
use super::peer::{
    ConfiguredPeerConnectContext, ConfiguredPeerConnectOutcome, PeerReconnectState,
    mark_known_peer_failure, mark_known_peer_success, mark_peer_failure, mark_peer_success,
    normalize_peer_dead_after_failures, normalize_reconnect_delay,
};
use super::sequence::{REPLAY_BYTES, REPLAY_PAGE, SUMMARY_PAGE};
use super::types::*;
use nx_sync::HistoryMessage;

pub(super) fn spawn_broadcast_loop(
    context: BroadcastLoopContext,
    mut op_rx: mpsc::Receiver<AcceptedLocalOp>,
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

async fn wait_for_shutdown(shutdown_rx: &mut watch::Receiver<bool>) {
    // Do not return watch::Ref from a select branch: its non-Send guard can
    // otherwise be retained across an await in another branch's handler.
    let _ = shutdown_rx.wait_for(|shutdown| *shutdown).await;
}

pub(super) fn spawn_reconnect_loop(context: ReconnectLoopContext) -> Option<JoinHandle<()>> {
    let ReconnectLoopContext {
        node,
        mut candidates_rx,
        max_peers,
        initial_delay,
        max_delay,
        peer_dead_after_failures,
        mut shutdown_rx,
        metrics,
        peer_health,
    } = context;

    Some(tokio::spawn(async move {
        let initial_delay = normalize_reconnect_delay(initial_delay);
        let max_delay = max_delay.max(initial_delay);
        let peer_dead_after_failures = normalize_peer_dead_after_failures(peer_dead_after_failures);
        let mut state = Vec::new();
        let initial_candidates = Arc::clone(&candidates_rx.borrow());
        reconcile_reconnect_candidates(
            &mut state,
            initial_candidates.as_ref(),
            initial_delay,
            &peer_health,
        )
        .await;

        'reconnect: loop {
            if *shutdown_rx.borrow() {
                break;
            }
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

                // The task owns the dial future: shutdown drops an in-flight
                // handshake instead of waiting for every candidate's timeout.
                let outcome = tokio::select! {
                    biased;
                    _ = wait_for_shutdown(&mut shutdown_rx) => break 'reconnect,
                    outcome = try_connect_configured_peer(&connect_context, peer_addr) => outcome,
                };
                match outcome {
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
                        let Some(attempt_delay) = peer.record_failure(max_delay, StdInstant::now())
                        else {
                            metrics.record_sync_error();
                            warn!(peer = %peer.addr, "stopping reconnect: backoff deadline overflow");
                            continue;
                        };
                        sleep_for = Some(
                            sleep_for.map_or(attempt_delay, |current| current.min(attempt_delay)),
                        );
                    }
                    ConfiguredPeerConnectOutcome::RetryAfter(delay) => {
                        let Some(retry_after) = peer.record_retry_after(
                            bounded_retry_after(delay, max_delay),
                            StdInstant::now(),
                        ) else {
                            metrics.record_sync_error();
                            warn!(peer = %peer.addr, "stopping reconnect: retry-after deadline overflow");
                            continue;
                        };
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
            let Some(deadline) = tokio::time::Instant::now().checked_add(sleep_for) else {
                metrics.record_sync_error();
                warn!("stopping reconnect loop: sleep deadline overflow");
                break;
            };

            tokio::select! {
                _ = wait_for_shutdown(&mut shutdown_rx) => {
                    debug!("reconnect loop shutdown requested");
                    break;
                }
                changed = candidates_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    let candidates = Arc::clone(&candidates_rx.borrow_and_update());
                    reconcile_reconnect_candidates(
                        &mut state,
                        candidates.as_ref(),
                        initial_delay,
                        &peer_health,
                    ).await;
                }
                _ = tokio::time::sleep_until(deadline) => {}
            }
        }
        debug!("reconnect loop terminated");
    }))
}

pub(super) fn spawn_anti_entropy_loop(context: AntiEntropyLoopContext) -> Option<JoinHandle<()>> {
    let AntiEntropyLoopContext {
        history_cursors,
        request_timeout,
        node,
        interval,
        mut shutdown_rx,
        metrics,
    } = context;

    Some(tokio::spawn(async move {
        let interval = normalize_anti_entropy_interval(interval);
        // Keep the cadence independent of discovery churn and skip missed ticks
        // rather than issuing bursts after a slow transport write. Recheck every
        // deadline: construction-time validation cannot guarantee future additions.
        let mut previous = tokio::time::Instant::now();
        loop {
            let Some(deadline) =
                checked_anti_entropy_deadline(previous, interval, tokio::time::Instant::now())
            else {
                metrics.record_sync_error();
                warn!("stopping anti-entropy loop: cadence deadline overflow");
                break;
            };
            previous = deadline;
            tokio::select! {
                biased;
                _ = wait_for_shutdown(&mut shutdown_rx) => {
                    debug!("anti-entropy loop shutdown requested");
                    break;
                }
                _ = async {
                    tokio::time::sleep_until(deadline).await;
                    let peers = node.connected_peers().await;
                    for (peer, _) in peers {
                        let after_origin = {
                            let mut cursors = history_cursors.write().await;
                            let cursor = cursors.entry(peer.clone()).or_default();
                            let now = StdInstant::now();
                            if cursor.request_deadline.is_some_and(|deadline| deadline > now) {
                                continue;
                            }
                            let Some(deadline) = now.checked_add(request_timeout) else {
                                metrics.record_sync_error();
                                warn!("stopping anti-entropy loop: request deadline overflow");
                                return;
                            };
                            cursor.request_deadline = Some(deadline);
                            cursor.after_origin.clone()
                        };
                        if let Err(error) = node.send_history_to_addr(&peer, HistoryMessage::SummaryRequest { after_origin }).await {
                            if let Some(cursor) = history_cursors.write().await.get_mut(&peer) {
                                cursor.request_deadline = None;
                            }
                            metrics.record_sync_error();
                            debug!(%peer, %error, "history summary request failed");
                        }
                    }
                } => {}
            }
        }
        debug!("anti-entropy loop terminated");
    }))
}

fn checked_anti_entropy_deadline(
    previous: tokio::time::Instant,
    interval: Duration,
    now: tokio::time::Instant,
) -> Option<tokio::time::Instant> {
    let interval = normalize_anti_entropy_interval(interval);
    let next = previous.checked_add(interval)?;
    if next > now {
        return Some(next);
    }
    // Preserve the original phase while skipping ticks missed during transport I/O.
    let remainder = now.duration_since(previous).as_nanos() % interval.as_nanos();
    let remainder = Duration::new(
        (remainder / 1_000_000_000) as u64,
        (remainder % 1_000_000_000) as u32,
    );
    now.checked_add(interval - remainder)
}

async fn reconcile_reconnect_candidates(
    state: &mut Vec<PeerReconnectState>,
    candidates: &[String],
    initial_delay: Duration,
    peer_health: &Arc<RwLock<std::collections::HashMap<String, super::peer::PeerHealth>>>,
) {
    let retained = candidates.iter().collect::<HashSet<_>>();
    let mut existing = state
        .drain(..)
        .map(|peer| (peer.addr.clone(), peer))
        .collect::<HashMap<_, _>>();
    let now = StdInstant::now();
    state.extend(candidates.iter().map(|addr| {
        existing
            .remove(addr)
            .unwrap_or_else(|| PeerReconnectState::new(addr.clone(), initial_delay, now))
    }));

    let mut health = peer_health.write().await;
    health.retain(|addr, _| retained.contains(addr));
    for candidate in candidates {
        health.entry(candidate.clone()).or_default();
    }
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
    op_rx: &mut mpsc::Receiver<AcceptedLocalOp>,
    first: AcceptedLocalOp,
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

    let batch = batch
        .into_iter()
        .map(AcceptedLocalOp::into_sequenced)
        .collect::<Vec<_>>();
    let count = batch.len();
    let started = std::time::Instant::now();
    if let Err(e) = context
        .node
        .broadcast_history(HistoryMessage::Push { ops: batch })
        .await
    {
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
    op_rx: &mut mpsc::Receiver<AcceptedLocalOp>,
) {
    while let Ok(first) = op_rx.try_recv() {
        broadcast_batch(context, op_rx, first).await;
    }
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

pub(super) fn plan_op_log_evictions(log: &[Op], inserted_ops: &[Op], limit: usize) -> Vec<Op> {
    let limit = normalize_op_log_limit(limit);
    let total_len = log.len().saturating_add(inserted_ops.len());
    let remove_count = total_len.saturating_sub(limit);
    log.iter()
        .chain(inserted_ops)
        .take(remove_count)
        .cloned()
        .collect()
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

#[cfg(test)]
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
        NodeEvent::HistoryReceived {
            from,
            addr,
            message,
        } => {
            let observed_id = match &message {
                HistoryMessage::Push { ops } | HistoryMessage::Replay { ops, .. } => {
                    ops.last().map(|entry| entry.op.id.as_str().to_owned())
                }
                _ => None,
            };
            if let Err(error) = handle_history_message(&addr, message, context).await {
                context.metrics.record_sync_error();
                warn!(%from, %error, "replication history exchange failed");
            } else if let Some(id) = observed_id {
                context
                    .anti_entropy_watermarks
                    .write()
                    .await
                    .insert(from, id);
            }
        }
        NodeEvent::OpsReceived { from, .. } => {
            context.metrics.record_sync_error();
            warn!(%from, "unsequenced legacy replay rejected; state baseline recovery required");
        }
        NodeEvent::PullRequested { from, .. } => {
            context.metrics.record_sync_error();
            warn!(%from, "legacy replay request rejected; sequenced history request required");
        }
        NodeEvent::PeerConnected {
            node_id,
            addr,
            peers_connected,
        } => {
            mark_known_peer_success(&context.peer_health, &addr).await;
            if let Some(connection) = context.node.connection_info(&addr).await {
                if connection.identity.node_id == node_id {
                    context
                        .active_connections
                        .write()
                        .await
                        .insert(addr.clone(), connection);
                } else {
                    warn!(peer = %node_id, addr = %addr, "ignored inconsistent connection identity");
                }
            }
            context.metrics.record_peer_connect();
            context.metrics.set_peers_connected(peers_connected);
            info!(peer = %node_id, addr = %addr, "peer connected");
        }
        NodeEvent::PeerDisconnected {
            node_id,
            addr,
            peers_connected,
        } => {
            context.history_cursors.write().await.remove(&addr);
            context.history_requests.write().await.remove(&addr);
            mark_known_peer_failure(
                &context.peer_health,
                &addr,
                context.peer_dead_after_failures,
            )
            .await;
            context.active_connections.write().await.remove(&addr);
            context.metrics.record_peer_disconnect();
            context.metrics.set_peers_connected(peers_connected);
            info!(peer = %node_id, addr = %addr, "peer disconnected");
        }
    }
}

fn history_apply_context(context: &NodeEventContext) -> RemoteOpApplyContext<'_> {
    RemoteOpApplyContext {
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
    }
}

fn validate_range(start: u64, end: u64) -> anyhow::Result<()> {
    anyhow::ensure!(
        start > 0 && end >= start && end - start < REPLAY_PAGE,
        "invalid history request interval"
    );
    Ok(())
}

async fn request_history_range(
    addr: &str,
    origin: nx_sync::NodeId,
    start: u64,
    end: u64,
    context: &NodeEventContext,
) -> anyhow::Result<()> {
    request_history_or_baseline(addr, origin, start, end, context).await
}

async fn request_baseline(
    addr: &str,
    origin: nx_sync::NodeId,
    context: &NodeEventContext,
) -> anyhow::Result<()> {
    // Zero is not a valid replay sequence; it identifies the bounded baseline request.
    request_history_or_baseline(addr, origin, 0, 0, context).await
}

async fn request_history_or_baseline(
    addr: &str,
    origin: nx_sync::NodeId,
    start: u64,
    end: u64,
    context: &NodeEventContext,
) -> anyhow::Result<()> {
    let request_id = nx_sync::OpId::generate();
    {
        let mut requests = context.history_requests.write().await;
        let pending = requests.entry(addr.to_owned()).or_default();
        let now = StdInstant::now();
        pending.retain(|_, request| request.deadline > now);
        if pending.contains_key(origin.as_str()) || pending.len() >= SUMMARY_PAGE {
            debug!(%addr, %origin, "deferring history range: request pending or peer budget full");
            return Ok(());
        }
        let deadline = now
            .checked_add(context.history_request_timeout)
            .ok_or_else(|| anyhow::anyhow!("history request deadline overflow"))?;
        pending.insert(
            origin.as_str().to_owned(),
            PendingHistoryRequest {
                request_id: request_id.clone(),
                start,
                end,
                deadline,
            },
        );
    }
    if let Err(error) = context
        .node
        .send_history_to_addr(
            addr,
            if start == 0 {
                HistoryMessage::BaselineRequest {
                    request_id: request_id.clone(),
                    origin: origin.clone(),
                    version: super::baseline::VERSION,
                }
            } else {
                HistoryMessage::Request {
                    request_id: request_id.clone(),
                    origin: origin.clone(),
                    start,
                    end,
                }
            },
        )
        .await
    {
        complete_history_request(addr, &origin, &request_id, start, end, context).await;
        return Err(error.into());
    }
    Ok(())
}

async fn complete_history_request(
    addr: &str,
    origin: &nx_sync::NodeId,
    request_id: &nx_sync::OpId,
    start: u64,
    end: u64,
    context: &NodeEventContext,
) -> bool {
    let mut requests = context.history_requests.write().await;
    if let Some(pending) = requests.get_mut(addr)
        && let Some(request) = pending.get(origin.as_str())
        && request.request_id == *request_id
        && request.deadline > StdInstant::now()
        && request.start == start
        && end >= start
        && end <= request.end
    {
        pending.remove(origin.as_str());
        return true;
    }
    false
}

async fn handle_history_message(
    addr: &str,
    message: HistoryMessage,
    context: &NodeEventContext,
) -> anyhow::Result<()> {
    match message {
        HistoryMessage::Push { ops } => {
            anyhow::ensure!(
                ops.len() <= 1024,
                "sequenced batch exceeds operation budget"
            );
            let result = apply_sequenced_ops(&ops, &history_apply_context(context)).await;
            if let Err(error) = &result
                && error
                    .downcast_ref::<super::sequence::TrackingLimit>()
                    .is_none()
            {
                return result;
            }
            let mut origins = std::collections::BTreeMap::<String, u64>::new();
            for entry in &ops {
                anyhow::ensure!(
                    entry.position.generation > 0 && entry.position.sequence > 0,
                    "invalid pushed position"
                );
                origins
                    .entry(entry.op.origin.as_str().to_owned())
                    .and_modify(|high| *high = (*high).max(entry.position.sequence))
                    .or_insert(entry.position.sequence);
            }
            // A far-ahead rejection must still permit earliest-range recovery.
            for (origin, high_sequence) in origins.into_iter().take(SUMMARY_PAGE) {
                let summary = nx_sync::HistorySummary {
                    baseline_required: false,
                    origin: nx_sync::NodeId::new(origin),
                    high_sequence,
                };
                if let Some((start, end)) =
                    super::storage::record_summary(&context.store, &summary)?
                {
                    request_history_range(addr, summary.origin, start, end, context).await?;
                }
            }
            result?;
        }
        HistoryMessage::SummaryRequest { after_origin } => {
            let cursor = super::storage::summary_cursor(after_origin.as_deref())?;
            let (entries, _) = super::storage::summary_page(&context.store, cursor.as_deref())?;
            context
                .node
                .send_history_to_addr(addr, HistoryMessage::Summary { entries })
                .await?;
        }
        HistoryMessage::Summary { entries } => {
            anyhow::ensure!(
                entries.len() <= SUMMARY_PAGE,
                "history summary page exceeds budget"
            );
            anyhow::ensure!(
                entries
                    .windows(2)
                    .all(|pair| pair[0].origin.as_str() < pair[1].origin.as_str()),
                "unordered history summary page"
            );
            let next = if entries.len() == SUMMARY_PAGE {
                entries.last().map(|entry| entry.origin.as_str().to_owned())
            } else {
                None
            };
            {
                let mut cursors = context.history_cursors.write().await;
                let cursor = cursors.entry(addr.to_owned()).or_default();
                cursor.after_origin = next;
                cursor.request_deadline = None;
            }
            for entry in entries {
                let missing = super::storage::record_summary(&context.store, &entry)?;
                if super::storage::needs_baseline(&context.store, entry.origin.as_str())?
                    || context.store.exists(super::storage::BASELINE_UNCONFIRMED)?
                {
                    request_baseline(addr, entry.origin, context).await?;
                } else if let Some((start, end)) = missing {
                    request_history_range(addr, entry.origin, start, end, context).await?;
                }
            }
        }
        HistoryMessage::Request {
            request_id,
            origin,
            start,
            end,
        } => {
            validate_range(start, end)?;
            anyhow::ensure!(
                !request_id.as_str().is_empty() && request_id.as_str().len() <= 128,
                "invalid history request identity"
            );
            let response = {
                let log = context.op_log.read().await;
                let mut retained = Vec::new();
                for op in log.iter().filter(|op| op.origin == origin) {
                    if let Some(entry) = super::storage::sequenced_op(&context.store, op)?
                        && entry.position.sequence >= start
                        && entry.position.sequence <= end
                    {
                        retained.push(entry);
                    }
                }
                retained.sort_by_key(|entry| entry.position.sequence);
                let mut page = Vec::new();
                let mut bytes = 2;
                for (expected, entry) in (start..=end).zip(retained) {
                    let size = serde_json::to_vec(&entry)?.len() + 1;
                    if entry.position.sequence != expected || bytes + size > REPLAY_BYTES {
                        break;
                    }
                    bytes += size;
                    page.push(entry);
                    if expected == end {
                        break;
                    }
                }
                if let Some(last) = page.last() {
                    HistoryMessage::Replay {
                        request_id,
                        origin,
                        start,
                        end: last.position.sequence,
                        ops: page,
                    }
                } else {
                    HistoryMessage::Unavailable {
                        request_id,
                        origin,
                        start,
                        end,
                    }
                }
            };
            context.node.send_history_to_addr(addr, response).await?;
        }
        HistoryMessage::Replay {
            request_id,
            origin,
            start,
            end,
            ops,
        } => {
            validate_range(start, end)?;
            anyhow::ensure!(
                ops.len() as u64 == end - start + 1,
                "partial replay interval"
            );
            anyhow::ensure!(
                serde_json::to_vec(&ops)?.len() <= REPLAY_BYTES,
                "replay exceeds byte budget"
            );
            for (index, entry) in ops.iter().enumerate() {
                anyhow::ensure!(
                    entry.op.origin == origin && entry.position.sequence == start + index as u64,
                    "replay position mismatch"
                );
            }
            if !complete_history_request(addr, &origin, &request_id, start, end, context).await {
                debug!(%addr, %origin, %request_id, "ignoring stale or unsolicited history replay");
                return Ok(());
            }
            apply_sequenced_ops(&ops, &history_apply_context(context)).await?;
            if let Some((start, end)) = super::storage::missing_range(&context.store, &origin)? {
                request_history_range(addr, origin, start, end, context).await?;
            }
        }
        HistoryMessage::Unavailable {
            request_id,
            origin,
            start,
            end,
        } => {
            validate_range(start, end)?;
            if !complete_history_request(addr, &origin, &request_id, start, end, context).await {
                debug!(%addr, %origin, %request_id, "ignoring stale or unsolicited unavailable history");
                return Ok(());
            }
            super::storage::record_unavailable(
                &context.store,
                &origin,
                super::sequence::UnavailableHistory {
                    source: addr.to_owned(),
                    start,
                    end,
                    request_id,
                },
            )?;
            warn!(%origin, start, end, "source cannot supply missing history; another source or state baseline is required");
            if let Err(error) = request_baseline(addr, origin, context).await {
                context.metrics.record_sync_error();
                debug!(%addr, %error, "baseline request deferred after unavailable replay; periodic repair will retry");
            }
        }
        HistoryMessage::BaselineRequest {
            request_id,
            origin,
            version,
        } => {
            anyhow::ensure!(
                !request_id.as_str().is_empty() && request_id.as_str().len() <= 128,
                "invalid baseline request identity"
            );
            super::storage::summary_cursor(Some(origin.as_str()))?;
            let result = if version != super::baseline::VERSION {
                Err(super::baseline::Rejected(nx_sync::BaselineFailure::UnsupportedVersion).into())
            } else {
                super::baseline::export(&history_apply_context(context)).await
            };
            let response = match result {
                Ok(payload) => HistoryMessage::Baseline {
                    request_id,
                    origin,
                    version,
                    digest: *blake3::hash(&payload).as_bytes(),
                    payload,
                },
                Err(error) => HistoryMessage::BaselineUnavailable {
                    request_id,
                    origin,
                    reason: error
                        .downcast_ref::<super::baseline::Rejected>()
                        .map_or(nx_sync::BaselineFailure::SourceIncomplete, |r| r.0),
                },
            };
            context.node.send_history_to_addr(addr, response).await?;
        }
        HistoryMessage::Baseline {
            request_id,
            origin,
            version,
            payload,
            digest,
        } => {
            if !complete_history_request(addr, &origin, &request_id, 0, 0, context).await {
                debug!(%addr, "ignoring stale or unsolicited baseline");
                return Ok(());
            }
            if let Err(error) = super::baseline::import(
                version,
                &payload,
                &digest,
                &origin,
                &history_apply_context(context),
            )
            .await
            {
                let reason = error
                    .downcast_ref::<super::baseline::Rejected>()
                    .map_or(nx_sync::BaselineFailure::InvalidState, |r| r.0);
                super::storage::record_recovery_failure(&context.store, &origin, addr, reason)?;
                return Err(error);
            }
        }
        HistoryMessage::BaselineUnavailable {
            request_id,
            origin,
            reason,
        } => {
            if !complete_history_request(addr, &origin, &request_id, 0, 0, context).await {
                debug!(%addr, "ignoring stale or unsolicited baseline refusal");
                return Ok(());
            }
            super::storage::record_recovery_failure(&context.store, &origin, addr, reason)?;
            context.metrics.record_sync_error();
            warn!(%origin, ?reason, "gap unrecoverable from this source under the bounded baseline contract; other sources may still recover it");
        }
    }
    Ok(())
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

    #[test]
    fn anti_entropy_deadlines_are_checked_and_skip_missed_ticks_without_bursts() {
        let now = tokio::time::Instant::now();
        let interval = Duration::from_millis(10);
        assert_eq!(checked_anti_entropy_deadline(now, Duration::MAX, now), None);
        assert_eq!(
            checked_anti_entropy_deadline(now, interval, now),
            now.checked_add(interval)
        );
        assert_eq!(
            checked_anti_entropy_deadline(now, interval, now + Duration::from_millis(35)),
            now.checked_add(Duration::from_millis(40)),
        );
        assert_eq!(
            checked_anti_entropy_deadline(now, Duration::ZERO, now),
            now.checked_add(Duration::from_millis(1)),
        );
    }

    #[tokio::test]
    async fn anti_entropy_task_exits_without_panicking_on_deadline_overflow() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = spawn_anti_entropy_loop(AntiEntropyLoopContext {
            history_cursors: Arc::new(RwLock::new(HashMap::new())),
            request_timeout: Duration::from_secs(30),
            node: Arc::new(
                Node::try_new(NodeConfig::new(NodeId::generate(), "127.0.0.1:0")).unwrap(),
            ),
            interval: Duration::MAX,
            shutdown_rx,
            metrics: metrics(),
        })
        .unwrap();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn reconnect_task_exits_without_panicking_on_sleep_deadline_overflow() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (_candidates_tx, candidates_rx) = watch::channel(Arc::new(Vec::new()));
        let task = spawn_reconnect_loop(ReconnectLoopContext {
            node: Arc::new(
                Node::try_new(NodeConfig::new(NodeId::generate(), "127.0.0.1:0")).unwrap(),
            ),
            candidates_rx,
            max_peers: 0,
            initial_delay: Duration::MAX,
            max_delay: Duration::MAX,
            peer_dead_after_failures: 1,
            shutdown_rx,
            metrics: metrics(),
            peer_health: Arc::new(RwLock::new(HashMap::new())),
        })
        .unwrap();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn removed_candidates_are_deleted_from_reconnect_state_and_health() {
        let now = StdInstant::now();
        let mut state = vec![PeerReconnectState::new(
            "peer.example:9000".to_string(),
            Duration::from_millis(10),
            now,
        )];
        let peer_health = Arc::new(RwLock::new(HashMap::from([(
            "peer.example:9000".to_string(),
            PeerHealth::default(),
        )])));

        reconcile_reconnect_candidates(&mut state, &[], Duration::from_millis(10), &peer_health)
            .await;

        assert!(state.is_empty());
        assert!(peer_health.read().await.is_empty());
    }

    #[tokio::test]
    async fn candidate_reordering_preserves_backoff_state() {
        let now = StdInstant::now();
        let mut first = PeerReconnectState::new(
            "one.example:9000".to_string(),
            Duration::from_millis(10),
            now,
        );
        first.record_failure(Duration::from_secs(1), now).unwrap();
        let first_deadline = first.next_attempt_at;
        let first_delay = first.delay;
        let mut state = vec![
            first,
            PeerReconnectState::new(
                "two.example:9000".to_string(),
                Duration::from_millis(10),
                now,
            ),
        ];
        let peer_health = Arc::new(RwLock::new(HashMap::new()));

        reconcile_reconnect_candidates(
            &mut state,
            &["two.example:9000".into(), "one.example:9000".into()],
            Duration::from_millis(10),
            &peer_health,
        )
        .await;

        assert_eq!(state[0].addr, "two.example:9000");
        assert_eq!(state[1].addr, "one.example:9000");
        assert_eq!(state[1].next_attempt_at, first_deadline);
        assert_eq!(state[1].delay, first_delay);
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
            history_cursors: Arc::new(RwLock::new(HashMap::new())),
            history_requests: Arc::new(RwLock::new(HashMap::new())),
            history_request_timeout: Duration::from_secs(30),
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
            active_connections: Arc::new(RwLock::new(HashMap::new())),
            anti_entropy_watermarks: Arc::new(RwLock::new(HashMap::new())),
            peer_dead_after_failures: 2,
        }
    }

    #[tokio::test]
    async fn baseline_stale_unsolicited_and_expired_responses_cannot_change_recovery_state() {
        use nx_sync::{BaselineFailure, HistorySummary, OpId};
        let store = temp_store();
        let context = test_event_context(
            Arc::new(RwLock::new(HashMap::new())),
            Arc::new(RwLock::new(SeenOps::new(1))),
            Arc::new(RwLock::new(Vec::new())),
            Arc::clone(&store),
            metrics(),
            Arc::new(RwLock::new(HashMap::new())),
        );
        let origin = NodeId::new("origin");
        super::super::storage::record_summary(
            &store,
            &HistorySummary {
                origin: origin.clone(),
                high_sequence: 1,
                baseline_required: false,
            },
        )
        .unwrap();
        let id = OpId::generate();
        let addr = "source-a:1234";
        let before = store.scan_prefix(b"__nx/").unwrap();
        for (source, request_id, deadline) in [
            (addr, id.clone(), StdInstant::now()),
            (
                "other:1234",
                id.clone(),
                StdInstant::now() + Duration::from_secs(30),
            ),
            (
                addr,
                OpId::generate(),
                StdInstant::now() + Duration::from_secs(30),
            ),
        ] {
            context.history_requests.write().await.insert(
                addr.into(),
                HashMap::from([(
                    origin.as_str().to_owned(),
                    PendingHistoryRequest {
                        request_id: id.clone(),
                        start: 0,
                        end: 0,
                        deadline,
                    },
                )]),
            );
            handle_history_message(
                source,
                HistoryMessage::Baseline {
                    request_id: request_id.clone(),
                    origin: origin.clone(),
                    version: 0,
                    payload: vec![1],
                    digest: [0; 32],
                },
                &context,
            )
            .await
            .unwrap();
            handle_history_message(
                source,
                HistoryMessage::BaselineUnavailable {
                    request_id,
                    origin: origin.clone(),
                    reason: BaselineFailure::BudgetExceeded,
                },
                &context,
            )
            .await
            .unwrap();
            assert_eq!(before, store.scan_prefix(b"__nx/").unwrap());
        }
        context.history_requests.write().await.insert(
            addr.into(),
            HashMap::from([(
                origin.as_str().to_owned(),
                PendingHistoryRequest {
                    request_id: id.clone(),
                    start: 0,
                    end: 0,
                    deadline: StdInstant::now() + Duration::from_secs(30),
                },
            )]),
        );
        handle_history_message(
            addr,
            HistoryMessage::BaselineUnavailable {
                request_id: id,
                origin: origin.clone(),
                reason: BaselineFailure::BudgetExceeded,
            },
            &context,
        )
        .await
        .unwrap();
        let failure = super::super::storage::replication_progress(&store).unwrap()[0]
            .recovery_failure
            .clone()
            .unwrap();
        assert_eq!(failure.source, addr);
        assert_eq!(failure.reason, BaselineFailure::BudgetExceeded);
        // Actual replay from another source clears this failure without inventing a baseline.
        apply_sequenced_ops(
            &[nx_sync::SequencedOp {
                op: Op::gcounter_increment(origin.clone(), "visits", 1),
                position: nx_sync::OpPosition {
                    generation: 1,
                    sequence: 1,
                },
            }],
            &history_apply_context(&context),
        )
        .await
        .unwrap();
        super::super::storage::record_summary(
            &store,
            &HistorySummary {
                origin,
                high_sequence: 2,
                baseline_required: false,
            },
        )
        .unwrap();
        assert!(
            super::super::storage::replication_progress(&store).unwrap()[0]
                .recovery_failure
                .is_none()
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
            NodeEvent::HistoryReceived {
                from: peer.clone(),
                addr: "unused".into(),
                message: nx_sync::HistoryMessage::Push {
                    ops: vec![op_a, op_b]
                        .into_iter()
                        .enumerate()
                        .map(|(index, op)| nx_sync::SequencedOp {
                            op,
                            position: nx_sync::OpPosition {
                                generation: 1,
                                sequence: index as u64 + 1,
                            },
                        })
                        .collect(),
                },
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

    #[tokio::test]
    async fn sequenced_invalid_pushes_do_not_persist_recovery_targets() {
        use nx_sync::{OpId, OpPosition, SequencedOp};
        let store = temp_store();
        let context = test_event_context(
            Arc::new(RwLock::new(HashMap::new())),
            Arc::new(RwLock::new(SeenOps::new(1))),
            Arc::new(RwLock::new(Vec::new())),
            Arc::clone(&store),
            metrics(),
            Arc::new(RwLock::new(HashMap::new())),
        );
        let valid = SequencedOp {
            op: Op::gcounter_increment(NodeId::new("origin"), "visits", 1),
            position: OpPosition {
                generation: 1,
                sequence: 100,
            },
        };
        let mut long_id = valid.clone();
        long_id.op.id = OpId::new("x".repeat(129));
        let mut zero_generation = valid.clone();
        zero_generation.position.generation = 0;
        let mut conflict = valid.clone();
        conflict.op.id = OpId::new("different");
        let mut far = valid.clone();
        far.position.sequence = super::super::sequence::MAX_GAP + 1;
        let before = store.scan_prefix(b"__nx/").unwrap();
        for ops in [
            vec![long_id.clone()],
            vec![zero_generation],
            vec![valid.clone(), conflict],
            vec![far.clone(), long_id],
        ] {
            assert!(
                handle_history_message("unused", HistoryMessage::Push { ops }, &context)
                    .await
                    .is_err()
            );
            assert_eq!(store.scan_prefix(b"__nx/").unwrap(), before);
            assert!(context.counters.read().await.is_empty());
            assert!(context.history_requests.read().await.is_empty());
        }
        // A validated far-ahead rejection still records the target even if transport is absent.
        assert!(
            handle_history_message("unused", HistoryMessage::Push { ops: vec![far] }, &context)
                .await
                .is_err()
        );
        let progress = super::super::storage::replication_progress(&store).unwrap();
        assert_eq!(progress[0].contiguous_sequence, 0);
        assert_eq!(
            progress[0].known_high_sequence,
            super::super::sequence::MAX_GAP + 1
        );
        assert!(context.counters.read().await.is_empty());
    }

    #[tokio::test]
    async fn sequenced_stale_unavailable_cannot_describe_a_new_gap_or_retry() {
        use nx_sync::{HistorySummary, OpId, OpPosition, SequencedOp};
        let store = temp_store();
        let context = test_event_context(
            Arc::new(RwLock::new(HashMap::new())),
            Arc::new(RwLock::new(SeenOps::new(1))),
            Arc::new(RwLock::new(Vec::new())),
            Arc::clone(&store),
            metrics(),
            Arc::new(RwLock::new(HashMap::new())),
        );
        let origin = NodeId::new("origin");
        let addr = "source-a:1234";
        let old_id = OpId::generate();
        let new_id = OpId::generate();
        super::super::storage::record_summary(
            &store,
            &HistorySummary {
                origin: origin.clone(),
                high_sequence: 2,
                baseline_required: false,
            },
        )
        .unwrap();
        apply_sequenced_ops(
            &[SequencedOp {
                op: Op::gcounter_increment(origin.clone(), "visits", 1),
                position: OpPosition {
                    generation: 1,
                    sequence: 1,
                },
            }],
            &history_apply_context(&context),
        )
        .await
        .unwrap();
        context.history_requests.write().await.insert(
            addr.to_owned(),
            HashMap::from([(
                origin.as_str().to_owned(),
                PendingHistoryRequest {
                    request_id: old_id.clone(),
                    start: 2,
                    end: 2,
                    deadline: StdInstant::now(),
                },
            )]),
        );
        let before_expired = store.scan_prefix(b"__nx/").unwrap();
        handle_history_message(
            addr,
            HistoryMessage::Unavailable {
                request_id: old_id.clone(),
                origin: origin.clone(),
                start: 2,
                end: 2,
            },
            &context,
        )
        .await
        .unwrap();
        assert_eq!(store.scan_prefix(b"__nx/").unwrap(), before_expired);
        context.history_requests.write().await.insert(
            addr.to_owned(),
            HashMap::from([(
                origin.as_str().to_owned(),
                PendingHistoryRequest {
                    request_id: new_id.clone(),
                    start: 2,
                    end: 2,
                    deadline: StdInstant::now() + Duration::from_secs(30),
                },
            )]),
        );
        let before = store.scan_prefix(b"__nx/").unwrap();
        for (source, id, start, end) in [
            (addr, old_id.clone(), 1, 1),
            (addr, old_id, 2, 2),
            ("source-b:1234", new_id.clone(), 2, 2),
        ] {
            handle_history_message(
                source,
                HistoryMessage::Unavailable {
                    request_id: id,
                    origin: origin.clone(),
                    start,
                    end,
                },
                &context,
            )
            .await
            .unwrap();
            assert_eq!(store.scan_prefix(b"__nx/").unwrap(), before);
            assert_eq!(context.history_requests.read().await[addr].len(), 1);
        }
        handle_history_message(
            addr,
            HistoryMessage::Unavailable {
                request_id: new_id.clone(),
                origin: origin.clone(),
                start: 2,
                end: 2,
            },
            &context,
        )
        .await
        .unwrap();
        let progress = super::super::storage::replication_progress(&store).unwrap();
        assert!(progress[0].source_unavailable);
        let evidence = progress[0].unavailable_evidence.as_ref().unwrap();
        assert_eq!(evidence.source, addr);
        assert_eq!(evidence.request_id, new_id);
        assert_eq!((evidence.start, evidence.end), (2, 2));
        store.flush().unwrap();
        assert!(super::super::storage::replication_progress(&store).unwrap()[0].source_unavailable);
        // Another source closes that interval; a later suffix must not inherit the evidence.
        apply_sequenced_ops(
            &[SequencedOp {
                op: Op::gcounter_increment(origin.clone(), "visits", 1),
                position: OpPosition {
                    generation: 1,
                    sequence: 2,
                },
            }],
            &history_apply_context(&context),
        )
        .await
        .unwrap();
        super::super::storage::record_summary(
            &store,
            &HistorySummary {
                origin: origin.clone(),
                high_sequence: 3,
                baseline_required: false,
            },
        )
        .unwrap();
        handle_history_message(
            addr,
            HistoryMessage::Unavailable {
                request_id: new_id,
                origin,
                start: 2,
                end: 2,
            },
            &context,
        )
        .await
        .unwrap();
        let progress = super::super::storage::replication_progress(&store).unwrap();
        assert_eq!(progress[0].contiguous_sequence, 2);
        assert_eq!(progress[0].known_high_sequence, 3);
        assert!(!progress[0].source_unavailable);
        assert!(progress[0].unavailable_evidence.is_none());
    }

    #[tokio::test]
    async fn sequenced_partial_and_mismatched_replays_do_not_advance_receipts() {
        use nx_sync::{HistorySummary, NodeId, OpPosition, SequencedOp};

        let store = temp_store();
        let origin = NodeId::new("origin");
        let context = test_event_context(
            Arc::new(RwLock::new(HashMap::new())),
            Arc::new(RwLock::new(SeenOps::new(1))),
            Arc::new(RwLock::new(Vec::new())),
            Arc::clone(&store),
            metrics(),
            Arc::new(RwLock::new(HashMap::new())),
        );
        super::super::storage::record_summary(
            &store,
            &HistorySummary {
                origin: origin.clone(),
                high_sequence: 2,
                baseline_required: false,
            },
        )
        .unwrap();
        let operation = SequencedOp {
            op: Op::gcounter_increment(origin.clone(), "visits", 1),
            position: OpPosition {
                generation: 1,
                sequence: 1,
            },
        };
        let before = store.scan_prefix(b"__nx/").unwrap();
        for (start, end, ops) in [
            (1, 2, vec![operation.clone()]),
            (2, 2, vec![operation.clone()]),
            (1, 1, Vec::new()),
            (0, 0, vec![operation.clone()]),
            (1, REPLAY_PAGE + 1, vec![operation.clone()]),
        ] {
            assert!(
                handle_history_message(
                    "unused",
                    HistoryMessage::Replay {
                        request_id: nx_sync::OpId::new("test-request"),
                        origin: origin.clone(),
                        start,
                        end,
                        ops,
                    },
                    &context,
                )
                .await
                .is_err()
            );
            assert_eq!(store.scan_prefix(b"__nx/").unwrap(), before);
            assert!(context.counters.read().await.is_empty());
        }
        context.history_requests.write().await.insert(
            "unused".to_owned(),
            HashMap::from([(
                origin.as_str().to_owned(),
                PendingHistoryRequest {
                    request_id: nx_sync::OpId::new("test-request"),
                    start: 1,
                    end: 2,
                    deadline: StdInstant::now() + Duration::from_secs(30),
                },
            )]),
        );
        handle_history_message(
            "unused",
            HistoryMessage::Replay {
                request_id: nx_sync::OpId::new("test-request"),
                origin: origin.clone(),
                start: 1,
                end: 2,
                ops: vec![
                    operation,
                    SequencedOp {
                        op: Op::gcounter_increment(origin, "visits", 1),
                        position: OpPosition {
                            generation: 1,
                            sequence: 2,
                        },
                    },
                ],
            },
            &context,
        )
        .await
        .unwrap();
        let progress = super::super::storage::replication_progress(&store).unwrap();
        assert_eq!(progress[0].contiguous_sequence, 2);
        assert_eq!(progress[0].known_high_sequence, 2);
        assert_eq!(context.counters.read().await["visits"].value(), 2);
    }

    #[tokio::test]
    async fn sequenced_repeated_summaries_bound_inflight_range_requests() {
        let mut source = Node::new(NodeConfig::new(NodeId::new("source"), "127.0.0.1:0"));
        let mut events = source.take_event_receiver().unwrap();
        let addr = source.start_listener().await.unwrap().to_string();
        let context = test_event_context(
            Arc::new(RwLock::new(HashMap::new())),
            Arc::new(RwLock::new(SeenOps::new(1))),
            Arc::new(RwLock::new(Vec::new())),
            temp_store(),
            metrics(),
            Arc::new(RwLock::new(HashMap::new())),
        );
        context.node.connect_to_peer(&addr).await.unwrap();
        for _ in 0..3 {
            handle_history_message(
                &addr,
                HistoryMessage::Summary {
                    entries: vec![nx_sync::HistorySummary {
                        origin: NodeId::new("offline-origin"),
                        high_sequence: 3,
                        baseline_required: false,
                    }],
                },
                &context,
            )
            .await
            .unwrap();
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if matches!(
                    events.recv().await.unwrap(),
                    NodeEvent::HistoryReceived {
                        message: HistoryMessage::Request {
                            start: 1,
                            end: 3,
                            ..
                        },
                        ..
                    }
                ) {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), events.recv())
                .await
                .is_err(),
            "repeated summaries duplicated an outstanding range request"
        );
        let origin = NodeId::new("offline-origin");
        let old_id = context.history_requests.read().await[&addr][origin.as_str()]
            .request_id
            .clone();
        complete_history_request(&addr, &origin, &old_id, 2, 3, &context).await;
        assert_eq!(context.history_requests.read().await[&addr].len(), 1);
        context
            .history_requests
            .write()
            .await
            .get_mut(&addr)
            .unwrap()
            .get_mut(origin.as_str())
            .unwrap()
            .deadline = StdInstant::now();
        request_history_range(&addr, origin.clone(), 1, 3, &context)
            .await
            .unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), events.recv())
                .await
                .unwrap()
                .unwrap(),
            NodeEvent::HistoryReceived {
                message: HistoryMessage::Request {
                    start: 1,
                    end: 3,
                    ..
                },
                ..
            }
        ));
        let request_id = context.history_requests.read().await[&addr][origin.as_str()]
            .request_id
            .clone();
        handle_history_message(
            &addr,
            HistoryMessage::Unavailable {
                request_id,
                origin: origin.clone(),
                start: 1,
                end: 3,
            },
            &context,
        )
        .await
        .unwrap();
        let pending = context.history_requests.read().await;
        let baseline_request = &pending[&addr][origin.as_str()];
        assert_eq!((baseline_request.start, baseline_request.end), (0, 0));
        drop(pending);
        assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .unwrap()
            .unwrap(),
        NodeEvent::HistoryReceived {
            message: HistoryMessage::BaselineRequest {
                origin: requested_origin,
                version: super::super::baseline::VERSION,
                ..
            },
            ..
        } if requested_origin == origin
        ));
        assert!(
            super::super::storage::replication_progress(&context.store).unwrap()[0]
                .source_unavailable
        );
        context.node.shutdown().await;
        source.shutdown().await;
    }

    #[tokio::test]
    async fn sequenced_summary_polling_waits_for_response_or_retry_deadline() {
        let mut source = Node::new(NodeConfig::new(NodeId::new("source"), "127.0.0.1:0"));
        let mut events = source.take_event_receiver().unwrap();
        let addr = source.start_listener().await.unwrap().to_string();
        let node = Arc::new(Node::new(NodeConfig::new(
            NodeId::new("target"),
            "127.0.0.1:0",
        )));
        node.connect_to_peer(&addr).await.unwrap();
        let cursors = Arc::new(RwLock::new(HashMap::new()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = spawn_anti_entropy_loop(AntiEntropyLoopContext {
            history_cursors: Arc::clone(&cursors),
            request_timeout: Duration::from_secs(30),
            node: Arc::clone(&node),
            interval: Duration::from_millis(1),
            shutdown_rx,
            metrics: metrics(),
        })
        .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if matches!(
                    events.recv().await.unwrap(),
                    NodeEvent::HistoryReceived {
                        message: HistoryMessage::SummaryRequest { .. },
                        ..
                    }
                ) {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), events.recv())
                .await
                .is_err(),
            "polling duplicated an outstanding summary request"
        );
        cursors
            .write()
            .await
            .get_mut(&addr)
            .unwrap()
            .request_deadline = Some(StdInstant::now());
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), events.recv())
                .await
                .unwrap()
                .unwrap(),
            NodeEvent::HistoryReceived {
                message: HistoryMessage::SummaryRequest { .. },
                ..
            }
        ));
        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        node.shutdown().await;
        source.shutdown().await;
    }
}
