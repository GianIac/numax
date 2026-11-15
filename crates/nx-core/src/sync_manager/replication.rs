use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant as StdInstant};

use nx_net::{NetError, NodeEvent, WireRetryPolicy};
#[cfg(test)]
use nx_store::Store as NxStore;
use nx_sync::Op;
use tokio::sync::{RwLock, mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use super::apply::apply_sequenced_ops_and_collect;
use super::manager::AcceptedLocalOp;
use super::peer::{
    ConfiguredPeerConnectContext, ConfiguredPeerConnectOutcome, PeerReconnectState,
    mark_known_peer_failure, mark_known_peer_success, mark_peer_failure, mark_peer_success,
    normalize_peer_dead_after_failures, normalize_reconnect_delay,
};
use super::sequence::{REPLAY_BYTES, REPLAY_PAGE, SUMMARY_PAGE};
use super::types::*;
use crate::observability::RuntimeMetrics;
use nx_sync::{HistoryMessage, NodeId};

const MAX_RECONNECT_ATTEMPTS_PER_TURN: usize = 8;

pub(super) fn spawn_broadcast_loop(
    context: BroadcastLoopContext,
    op_rx: super::delivery::Receiver<AcceptedLocalOp>,
    shutdown_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(broadcast_loop(context, op_rx, shutdown_rx))
}

pub(super) async fn broadcast_loop(
    context: BroadcastLoopContext,
    mut op_rx: super::delivery::Receiver<AcceptedLocalOp>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut pending = None;
    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    debug!("broadcast loop shutdown requested");
                    drain_broadcast_queue(
                        &context,
                        &mut op_rx,
                        pending.take(),
                    ).await;
                    break;
                }
            }
            op = async { match pending.take() { Some(op) => Some(op), None => op_rx.recv().await } } => {
                let Some(first) = op else {
                    break;
                };
                broadcast_batch(
                    &context,
                    &mut op_rx,
                    first,
                    &mut pending,
                ).await;
            }
        }
    }
    debug!("broadcast loop terminated");
}

pub(super) fn spawn_forward_loop(
    context: ForwardLoopContext,
    forwarded_rx: super::delivery::Receiver<ForwardedOp>,
    shutdown_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(forward_loop(context, forwarded_rx, shutdown_rx))
}

pub(super) async fn forward_loop(
    context: ForwardLoopContext,
    mut forwarded_rx: super::delivery::Receiver<ForwardedOp>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut pending = None;
    loop {
        let first = tokio::select! {
            biased;
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    debug!("forward loop shutdown requested; durable history remains available for repair");
                    break;
                }
                continue;
            }
            forwarded = async {
                match pending.take() {
                    Some(forwarded) => Some(forwarded),
                    None => forwarded_rx.recv().await,
                }
            } => forwarded,
        };
        let Some(first) = first else {
            break;
        };
        forward_batch(&context, &mut forwarded_rx, first, &mut pending).await;
    }
    debug!("forward loop terminated");
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
            context
                .metrics
                .peers
                .increment(crate::peer_metrics::Counter::DialConnected);
            mark_peer_success(context.peer_health, peer_addr).await;
            ConfiguredPeerConnectOutcome::Connected
        }
        Err(NetError::PeerLimitReached(limit)) => {
            debug!(
                peer = %peer_addr,
                limit,
                error_class = "peer_limit_reached",
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
            context.metrics.peers.increment(match outcome {
                ConfiguredPeerConnectOutcome::Fatal => crate::peer_metrics::Counter::DialFatal,
                ConfiguredPeerConnectOutcome::RetryAfter(_) => {
                    crate::peer_metrics::Counter::DialLimited
                }
                _ => crate::peer_metrics::Counter::DialFailed,
            });
            match outcome {
                ConfiguredPeerConnectOutcome::Fatal => {
                    debug!(
                        peer = %peer_addr,
                        error = %e,
                        error_class = connection_error_class(&e),
                        "configured peer connect failed with fatal wire error"
                    );
                }
                ConfiguredPeerConnectOutcome::RetryAfter(delay) => {
                    debug!(
                        peer = %peer_addr,
                        error = %e,
                        error_class = connection_error_class(&e),
                        retry_after_ms = delay.as_millis(),
                        "configured peer connect rate limited"
                    );
                }
                ConfiguredPeerConnectOutcome::Failed => {
                    debug!(
                        peer = %peer_addr,
                        error = %e,
                        error_class = connection_error_class(&e),
                        "configured peer connect failed"
                    );
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

fn connection_error_class(error: &NetError) -> &'static str {
    match error {
        NetError::Io(error) => match error.kind() {
            std::io::ErrorKind::ConnectionRefused => "io_connection_refused",
            std::io::ErrorKind::ConnectionReset => "io_connection_reset",
            std::io::ErrorKind::ConnectionAborted => "io_connection_aborted",
            std::io::ErrorKind::BrokenPipe => "io_broken_pipe",
            std::io::ErrorKind::UnexpectedEof => "io_unexpected_eof",
            std::io::ErrorKind::TimedOut => "io_timed_out",
            std::io::ErrorKind::WouldBlock => "io_would_block",
            _ => "io_other",
        },
        NetError::ConnectionFailed(message)
            if message.starts_with("outbound connection attempt limit reached:") =>
        {
            "outbound_attempt_limit"
        }
        NetError::ConnectionFailed(_) => "connection_failed",
        NetError::PeerDisconnected(_) => "peer_disconnected",
        NetError::InvalidMessage(_) => "invalid_message",
        NetError::Wire(error) => match error.retry_policy() {
            WireRetryPolicy::Fatal => "wire_fatal",
            WireRetryPolicy::RetryAfter(_) => "wire_retry_after",
            WireRetryPolicy::Retry | WireRetryPolicy::RequestFatal => "wire_retry",
            _ => "wire_other",
        },
        NetError::MessageTooLarge { .. } => "message_too_large",
        NetError::Timeout => "timeout",
        NetError::ChannelClosed => "channel_closed",
        NetError::TlsError(_) => "tls_error",
        NetError::PeerNotAllowed(_) => "peer_not_allowed",
        NetError::PeerLimitReached(_) => "peer_limit_reached",
        NetError::NodeIdMismatch { .. } => "node_id_mismatch",
        NetError::Serialization(_)
        | NetError::BinarySerialization(_)
        | NetError::BinaryDeserialization(_) => "serialization",
    }
}

#[cfg(test)]
fn trace_recovery_event(event: &'static str, items: u64) {
    debug!(
        target: "nx_core::sync_manager::replication",
        recovery_event = event,
        recovery_items = items,
        "bounded recovery diagnostic"
    );
}

#[cfg(test)]
fn trace_recovery_send_failure(kind: &'static str, error: &NetError) {
    let class = match connection_error_class(error) {
        "io_unexpected_eof" => "eof",
        "io_connection_reset" | "io_connection_aborted" | "io_broken_pipe" => "reset",
        "io_connection_refused" => "refused",
        "io_timed_out" | "timeout" => "timeout",
        "io_would_block" => "budget",
        "peer_limit_reached" => "peer_limit",
        "connection_failed" | "outbound_attempt_limit" => "connection",
        "peer_disconnected" => "peer_disconnected",
        "channel_closed" => "channel_closed",
        "wire_fatal" | "wire_retry_after" | "wire_retry" | "wire_other" => "wire",
        "serialization" => "serialization",
        "invalid_message" | "message_too_large" | "tls_error" | "peer_not_allowed"
        | "node_id_mismatch" => "protocol",
        "io_other" => "io",
        _ => "other",
    };
    let event = match (kind, class) {
        ("summary", "eof") => "summary_send_failed_eof",
        ("summary", "reset") => "summary_send_failed_reset",
        ("summary", "refused") => "summary_send_failed_refused",
        ("summary", "timeout") => "summary_send_failed_timeout",
        ("summary", "peer_limit") => "summary_send_failed_peer_limit",
        ("summary", "budget") => "summary_send_failed_budget",
        ("summary", "connection") => "summary_send_failed_connection",
        ("summary", "peer_disconnected") => "summary_send_failed_peer_disconnected",
        ("summary", "channel_closed") => "summary_send_failed_channel_closed",
        ("summary", "wire") => "summary_send_failed_wire",
        ("summary", "serialization") => "summary_send_failed_serialization",
        ("summary", "protocol") => "summary_send_failed_protocol",
        ("summary", "io") => "summary_send_failed_io",
        ("range", "eof") => "range_send_failed_eof",
        ("range", "reset") => "range_send_failed_reset",
        ("range", "refused") => "range_send_failed_refused",
        ("range", "timeout") => "range_send_failed_timeout",
        ("range", "peer_limit") => "range_send_failed_peer_limit",
        ("range", "budget") => "range_send_failed_budget",
        ("range", "connection") => "range_send_failed_connection",
        ("range", "peer_disconnected") => "range_send_failed_peer_disconnected",
        ("range", "channel_closed") => "range_send_failed_channel_closed",
        ("range", "wire") => "range_send_failed_wire",
        ("range", "serialization") => "range_send_failed_serialization",
        ("range", "protocol") => "range_send_failed_protocol",
        ("range", "io") => "range_send_failed_io",
        ("baseline", "eof") => "baseline_send_failed_eof",
        ("baseline", "reset") => "baseline_send_failed_reset",
        ("baseline", "refused") => "baseline_send_failed_refused",
        ("baseline", "timeout") => "baseline_send_failed_timeout",
        ("baseline", "peer_limit") => "baseline_send_failed_peer_limit",
        ("baseline", "budget") => "baseline_send_failed_budget",
        ("baseline", "connection") => "baseline_send_failed_connection",
        ("baseline", "peer_disconnected") => "baseline_send_failed_peer_disconnected",
        ("baseline", "channel_closed") => "baseline_send_failed_channel_closed",
        ("baseline", "wire") => "baseline_send_failed_wire",
        ("baseline", "serialization") => "baseline_send_failed_serialization",
        ("baseline", "protocol") => "baseline_send_failed_protocol",
        ("baseline", "io") => "baseline_send_failed_io",
        ("summary", _) => "summary_send_failed_other",
        ("range", _) => "range_send_failed_other",
        ("baseline", _) => "baseline_send_failed_other",
        _ => "recovery_send_failed_other",
    };
    trace_recovery_event(event, 0);
}

#[cfg(test)]
pub(super) fn trace_recovery_latency(event: &'static str, elapsed: Duration, items: u64) {
    let bucket = if elapsed < Duration::from_millis(100) {
        0
    } else if elapsed < Duration::from_millis(500) {
        1
    } else if elapsed < Duration::from_secs(2) {
        2
    } else {
        3
    };
    let event = match (event, bucket) {
        ("summary", 0) => "summary_response_lt_100ms",
        ("summary", 1) => "summary_response_lt_500ms",
        ("summary", 2) => "summary_response_lt_2s",
        ("summary", _) => "summary_response_gte_2s",
        ("summary_record", 0) => "summary_record_lt_100ms",
        ("summary_record", 1) => "summary_record_lt_500ms",
        ("summary_record", 2) => "summary_record_lt_2s",
        ("summary_record", _) => "summary_record_gte_2s",
        ("summary_gap_send", 0) => "summary_gap_send_lt_100ms",
        ("summary_gap_send", 1) => "summary_gap_send_lt_500ms",
        ("summary_gap_send", 2) => "summary_gap_send_lt_2s",
        ("summary_gap_send", _) => "summary_gap_send_gte_2s",
        ("summary_gap_dispatch", 0) => "summary_gap_dispatch_lt_100ms",
        ("summary_gap_dispatch", 1) => "summary_gap_dispatch_lt_500ms",
        ("summary_gap_dispatch", 2) => "summary_gap_dispatch_lt_2s",
        ("summary_gap_dispatch", _) => "summary_gap_dispatch_gte_2s",
        ("range_replay", 0) => "range_replay_lt_100ms",
        ("range_replay", 1) => "range_replay_lt_500ms",
        ("range_replay", 2) => "range_replay_lt_2s",
        ("range_replay", _) => "range_replay_gte_2s",
        ("range_unavailable", 0) => "range_unavailable_lt_100ms",
        ("range_unavailable", 1) => "range_unavailable_lt_500ms",
        ("range_unavailable", 2) => "range_unavailable_lt_2s",
        ("range_unavailable", _) => "range_unavailable_gte_2s",
        ("baseline_accepted", 0) => "baseline_accepted_lt_100ms",
        ("baseline_accepted", 1) => "baseline_accepted_lt_500ms",
        ("baseline_accepted", 2) => "baseline_accepted_lt_2s",
        ("baseline_accepted", _) => "baseline_accepted_gte_2s",
        ("baseline_unavailable", 0) => "baseline_unavailable_lt_100ms",
        ("baseline_unavailable", 1) => "baseline_unavailable_lt_500ms",
        ("baseline_unavailable", 2) => "baseline_unavailable_lt_2s",
        ("baseline_unavailable", _) => "baseline_unavailable_gte_2s",
        _ => "other_recovery_response",
    };
    trace_recovery_event(event, items);
}

#[cfg(test)]
fn request_age(now: StdInstant, deadline: StdInstant, timeout: Duration) -> Duration {
    timeout.saturating_sub(deadline.saturating_duration_since(now))
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
    Some(tokio::spawn(reconnect_loop(context)))
}

pub(super) async fn reconnect_loop(context: ReconnectLoopContext) {
    let ReconnectLoopContext {
        local_id,
        mut membership,
        rotation_interval,
        mut recovery_rotation,
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

    let initial_delay = normalize_reconnect_delay(initial_delay);
    let max_delay = max_delay.max(initial_delay);
    let peer_dead_after_failures = normalize_peer_dead_after_failures(peer_dead_after_failures);
    let mut state = Vec::new();
    let mut neighbors = super::neighbors::Neighbors::new(node.now());
    let mut victim_cursor = 0usize;
    let mut jitter = super::delivery::Jitter::new(super::delivery::seed(local_id.as_str()));
    let mut seen_alive = HashMap::new();
    let mut revival_resets: HashMap<String, StdInstant> = HashMap::new();
    let mut membership_open = true;
    let mut recovery_rotation_generation = RecoveryRotation::default().generation;
    let mut recovery_rotation_pending = false;
    let mut preferred_rotation_peer: Option<String> = None;

    'reconnect: loop {
        if *shutdown_rx.borrow() {
            break;
        }
        let now = node.now();
        let periodic_rotation_due = neighbors.rotate(now, rotation_interval);
        let mut rotation_due = periodic_rotation_due;
        let mut recovery_rotation_wait = None;
        if recovery_rotation_pending {
            if periodic_rotation_due {
                recovery_rotation_pending = false;
            } else if neighbors.rotate(now, super::neighbors::RECOVERY_ROTATION_COOLDOWN) {
                rotation_due = true;
                recovery_rotation_pending = false;
            } else {
                recovery_rotation_wait = Some(
                    neighbors.rotation_wait(now, super::neighbors::RECOVERY_ROTATION_COOLDOWN),
                );
            }
        }
        let discovery = Arc::clone(&candidates_rx.borrow_and_update());
        let records = membership.borrow_and_update().records.clone();
        let alive = alive_endpoints(&records);
        let revived: HashSet<_> = revived_endpoints(&alive, &seen_alive).collect();
        seen_alive = alive;
        let was_suppressed = neighbors.recovery_suppressed();
        let candidates = neighbors.candidates(&discovery, &records, local_id.as_str());
        if !was_suppressed && neighbors.recovery_suppressed() {
            metrics.record_sync_error();
            warn!(
                "withdrawal budget exhausted; membership-only data recovery disabled until restart; live discovery remains usable"
            );
        }
        reconcile_reconnect_candidates(
            &mut state,
            &candidates,
            initial_delay,
            &peer_health,
            node.now(),
        )
        .await;
        // A member back in Alive with a newer version may sit behind a backoff that
        // grew while it was unreachable; dial it promptly, once per rotation window.
        let now = node.now();
        revival_resets.retain(|addr, _| state.iter().any(|peer| &peer.addr == addr));
        for peer in state.iter_mut() {
            if !peer.stopped
                && revived.contains(&peer.addr)
                && revival_resets
                    .get(&peer.addr)
                    .is_none_or(|at| now.saturating_duration_since(*at) >= rotation_interval)
            {
                peer.reset(initial_delay, now);
                revival_resets.insert(peer.addr.clone(), now);
            }
        }
        neighbors.advance();
        let removed_duplicates = node.deduplicate_peer_connections().await;
        if removed_duplicates > 0 {
            debug!(
                removed_duplicates,
                "deduplicated active connections by peer identity"
            );
        }
        let connected = node.connected_peers().await;
        let connected_ids: HashSet<_> = connected.iter().map(|(_, id)| id.as_str()).collect();
        let mut connected_endpoints: HashSet<_> =
            connected.iter().map(|(addr, _)| addr.clone()).collect();
        let candidate_set: HashSet<_> = candidates.iter().map(String::as_str).collect();
        let mut replaceable_ids = HashSet::new();
        for record in &records {
            if let Some(endpoint) = record
                .endpoint
                .as_ref()
                .and_then(|endpoint| super::canonicalize_endpoint(endpoint).ok())
            {
                if connected_ids.contains(record.member.as_str()) {
                    connected_endpoints.insert(endpoint.clone());
                }
                if candidate_set.contains(endpoint.as_str()) {
                    replaceable_ids.insert(record.member.as_str());
                }
            }
        }
        let mut attempts = 0usize;
        let mut sleep_for = recovery_rotation_wait;
        let now = node.now();
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

            if connected_endpoints.contains(peer_addr) || node.is_connected_addr(peer_addr).await {
                mark_peer_success(&peer_health, peer_addr).await;
                peer.reset(initial_delay, node.now());
                continue;
            }

            if let Some(wait) = peer.next_attempt_at.checked_duration_since(now)
                && !wait.is_zero()
            {
                sleep_for = Some(sleep_for.map_or(wait, |current| current.min(wait)));
                continue;
            }

            // Bound dialing work per turn. Advancing the first candidate on
            // every turn prevents a slow prefix or full slots from starving bridges.
            if attempts == MAX_RECONNECT_ATTEMPTS_PER_TURN {
                break;
            }
            attempts += 1;
            if node.connected_peer_count().await >= max_peers {
                let replaceable: Vec<_> = connected
                    .iter()
                    .filter(|(addr, id)| {
                        candidate_set.contains(addr.as_str())
                            || replaceable_ids.contains(id.as_str())
                            || preferred_rotation_peer.as_deref() == Some(addr.as_str())
                    })
                    .collect();
                if !replaceable.is_empty() && rotation_due {
                    rotation_due = false;
                    let selected = preferred_rotation_peer
                        .as_ref()
                        .and_then(|preferred| {
                            replaceable
                                .iter()
                                .copied()
                                .find(|(addr, _)| addr == preferred)
                        })
                        .unwrap_or_else(|| replaceable[victim_cursor % replaceable.len()]);
                    let (addr, id) = selected;
                    victim_cursor = victim_cursor.wrapping_add(1);
                    tokio::select! {
                        biased;
                        _ = wait_for_shutdown(&mut shutdown_rx) => break 'reconnect,
                        _ = node.disconnect_peer(addr, id) => {},
                    }
                    preferred_rotation_peer = None;
                }
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
                    if preferred_rotation_peer
                        .as_deref()
                        .is_some_and(|preferred| preferred != peer_addr)
                    {
                        preferred_rotation_peer = None;
                    }
                    info!(peer = %peer_addr, "reconnected configured peer");
                    peer.reset(initial_delay, node.now());
                }
                ConfiguredPeerConnectOutcome::AlreadyConnected => {
                    if preferred_rotation_peer
                        .as_deref()
                        .is_some_and(|preferred| preferred != peer_addr)
                    {
                        preferred_rotation_peer = None;
                    }
                    peer.reset(initial_delay, node.now());
                }
                ConfiguredPeerConnectOutcome::SlotLimitReached => {
                    break;
                }
                ConfiguredPeerConnectOutcome::Failed => {
                    let Some(attempt_delay) = peer.record_failure(max_delay, node.now()) else {
                        metrics.record_sync_error();
                        warn!(peer = %peer.addr, "stopping reconnect: backoff deadline overflow");
                        continue;
                    };
                    sleep_for =
                        Some(sleep_for.map_or(attempt_delay, |current| current.min(attempt_delay)));
                    metrics
                        .peers
                        .increment(crate::peer_metrics::Counter::RetryFailure);
                }
                ConfiguredPeerConnectOutcome::RetryAfter(delay) => {
                    let Some(retry_after) =
                        peer.record_retry_after(bounded_retry_after(delay, max_delay), node.now())
                    else {
                        metrics.record_sync_error();
                        warn!(peer = %peer.addr, "stopping reconnect: retry-after deadline overflow");
                        continue;
                    };
                    sleep_for =
                        Some(sleep_for.map_or(retry_after, |current| current.min(retry_after)));
                    metrics
                        .peers
                        .increment(crate::peer_metrics::Counter::RetryLimited);
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
        let sleep_for = sleep_for
            .saturating_add(jitter.delay(sleep_for / 8))
            .min(max_delay);
        let Some(deadline) = node.now().checked_add(sleep_for) else {
            metrics.record_sync_error();
            warn!("stopping reconnect loop: sleep deadline overflow");
            break;
        };

        loop {
            tokio::select! {
                biased;
                _ = wait_for_shutdown(&mut shutdown_rx) => {
                    debug!("reconnect loop shutdown requested");
                    break 'reconnect;
                }
                changed = candidates_rx.changed() => {
                    if changed.is_err() {
                        break 'reconnect;
                    }
                    break;
                }
                changed = membership.changed(), if membership_open => {
                    if changed.is_err() {
                        membership_open = false;
                        continue;
                    }
                    let alive = alive_endpoints(&membership.borrow().records);
                    if revived_endpoints(&alive, &seen_alive).next().is_some() {
                        break;
                    }
                }
                changed = async {
                    match recovery_rotation.as_mut() {
                        Some(receiver) => receiver.changed().await,
                        None => std::future::pending().await,
                    }
                } => {
                    let Some(receiver) = recovery_rotation.as_mut() else { continue };
                    if changed.is_err() {
                        recovery_rotation = None;
                        continue;
                    }
                    let request = receiver.borrow_and_update().clone();
                    if request.generation != recovery_rotation_generation {
                        recovery_rotation_generation = request.generation;
                        recovery_rotation_pending = request.source_addr.is_some();
                        preferred_rotation_peer = request.source_addr;
                        break;
                    }
                }
                _ = node.sleep_until(deadline) => break,
            }
        }
    }
    debug!("reconnect loop terminated");
}

/// Canonical endpoints of Alive members with their membership version.
fn alive_endpoints(
    records: &[nx_net::MembershipRecord],
) -> HashMap<String, nx_net::MembershipVersion> {
    records
        .iter()
        .filter(|record| record.state == nx_net::MembershipState::Alive)
        .filter_map(|record| {
            let endpoint = record.endpoint.as_ref()?;
            let endpoint = super::canonicalize_endpoint(endpoint).ok()?;
            Some((endpoint, record.version))
        })
        .collect()
}

/// Endpoints that became Alive, or Alive at a newer version, since `seen`.
fn revived_endpoints<'a>(
    alive: &'a HashMap<String, nx_net::MembershipVersion>,
    seen: &'a HashMap<String, nx_net::MembershipVersion>,
) -> impl Iterator<Item = String> + 'a {
    alive
        .iter()
        .filter(|(endpoint, version)| seen.get(*endpoint) != Some(*version))
        .map(|(endpoint, _)| endpoint.clone())
}

pub(super) fn spawn_anti_entropy_loop(context: AntiEntropyLoopContext) -> Option<JoinHandle<()>> {
    Some(tokio::spawn(anti_entropy_loop(context)))
}

pub(super) async fn anti_entropy_loop(context: AntiEntropyLoopContext) {
    let mut shutdown_rx = context.shutdown_rx.clone();
    let interval = normalize_anti_entropy_interval(context.interval);
    let mut jitter = super::delivery::Jitter::new(context.jitter_seed);
    let mut peer_cursor = 0usize;
    let mut previous = context.node.now();
    let mut membership = context.membership.clone();
    let mut seen_alive = membership
        .as_mut()
        .map(|membership| alive_members(&membership.borrow_and_update().records))
        .unwrap_or_default();
    'entropy: loop {
        let period =
            interval.saturating_sub(jitter.delay((interval / 8).min(Duration::from_secs(1))));
        let Some(deadline) = checked_anti_entropy_deadline(previous, period, context.node.now())
        else {
            context.metrics.record_sync_error();
            warn!("stopping anti-entropy loop: cadence deadline overflow");
            break;
        };
        previous = deadline;
        // Between periodic turns, service catch-up sessions: start them for members
        // that came back Alive and retry summary pages that went unanswered. The
        // retry deadline is fixed per round so frequent membership updates cannot
        // postpone it indefinitely.
        let mut retry_at = context.node.now().checked_add(SUMMARY_REQUEST_TIMEOUT);
        loop {
            let poll = retry_at.unwrap_or(deadline).min(deadline);
            tokio::select! {
                biased;
                _ = wait_for_shutdown(&mut shutdown_rx) => break 'entropy,
                changed = async {
                    match membership.as_mut() {
                        Some(membership) => membership.changed().await,
                        None => std::future::pending().await,
                    }
                } => {
                    let Some(receiver) = membership.as_mut() else { continue };
                    if changed.is_err() {
                        membership = None;
                        continue;
                    }
                    let alive = alive_members(&receiver.borrow_and_update().records);
                    let revived: HashSet<_> = alive
                        .iter()
                        .filter(|(member, version)| seen_alive.get(*member) != Some(*version))
                        .map(|(member, _)| member.clone())
                        .collect();
                    seen_alive = alive;
                    if !revived.is_empty() {
                        tokio::select! {
                            biased;
                            _ = wait_for_shutdown(&mut shutdown_rx) => break 'entropy,
                            _ = start_catch_up(&context, &revived) => {}
                        }
                    }
                }
                _ = context.node.sleep_until(poll) => {
                    if poll >= deadline {
                        tokio::select! {
                            biased;
                            _ = wait_for_shutdown(&mut shutdown_rx) => break 'entropy,
                            _ = anti_entropy_turn(&context, &mut peer_cursor) => {}
                        }
                        break;
                    }
                    tokio::select! {
                        biased;
                        _ = wait_for_shutdown(&mut shutdown_rx) => break 'entropy,
                        _ = catch_up_turn(&context) => {}
                    }
                    retry_at = context.node.now().checked_add(SUMMARY_REQUEST_TIMEOUT);
                }
            }
        }
    }
    debug!("anti-entropy loop terminated");
}

/// Alive members by identity with their membership version.
fn alive_members(
    records: &[nx_net::MembershipRecord],
) -> HashMap<String, nx_net::MembershipVersion> {
    records
        .iter()
        .filter(|record| record.state == nx_net::MembershipState::Alive)
        .map(|record| (record.member.clone(), record.version))
        .collect()
}

/// Starts a catch-up session on the data connections of revived members: a
/// partition can heal without the transport ever dropping those sessions.
async fn start_catch_up(context: &AntiEntropyLoopContext, revived: &HashSet<String>) {
    let peers: Vec<_> = context
        .node
        .connected_peers()
        .await
        .into_iter()
        .filter(|(_, id)| revived.contains(id.as_str()))
        .map(|(addr, _)| addr)
        .collect();
    if peers.is_empty() {
        return;
    }
    {
        let mut cursors = context.history_cursors.write().await;
        for addr in &peers {
            let cursor = cursors.entry(addr.clone()).or_default();
            cursor.catch_up = true;
            cursor.after_origin = None;
            cursor.walk_gap = false;
            cursor.rewalks = 0;
        }
    }
    let mut turn = super::delivery::Turn::new();
    for addr in peers {
        if let Err(error) = request_summary(
            &context.node,
            &context.repair_budget,
            &context.history_cursors,
            &addr,
            context.request_timeout,
            Some(&mut turn),
        )
        .await
        {
            debug!(peer = %addr, %error, "catch-up summary request deferred");
        }
    }
}

/// Re-requests the next summary page of catch-up sessions whose previous request
/// went unanswered; bounded by the per-turn message and byte budget.
async fn catch_up_turn(context: &AntiEntropyLoopContext) {
    let now = context.node.now();
    let peers: Vec<_> = {
        let connected = context.node.connected_peers().await;
        let cursors = context.history_cursors.read().await;
        connected
            .into_iter()
            .map(|(addr, _)| addr)
            .filter(|addr| {
                cursors.get(addr).is_some_and(|cursor| {
                    cursor.catch_up
                        && cursor
                            .request_deadline
                            .is_none_or(|deadline| deadline <= now)
                })
            })
            .collect()
    };
    let mut turn = super::delivery::Turn::new();
    for addr in peers.into_iter().take(super::delivery::TURN_MESSAGES) {
        if let Err(error) = request_summary(
            &context.node,
            &context.repair_budget,
            &context.history_cursors,
            &addr,
            context.request_timeout,
            Some(&mut turn),
        )
        .await
        {
            debug!(peer = %addr, %error, "catch-up summary retry deferred");
        }
    }
}

/// One bounded protocol turn; both adapters execute this function.
pub(super) async fn anti_entropy_turn(context: &AntiEntropyLoopContext, peer_cursor: &mut usize) {
    context
        .node
        .refresh_recovery(&context.metrics, Arc::clone(&context.store))
        .await;
    let mut peers = context.node.connected_peers().await;
    peers.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.as_str().cmp(b.1.as_str())));
    context
        .history_cursors
        .write()
        .await
        .retain(|addr, _| peers.iter().any(|(peer, _)| peer == addr));
    if !peers.is_empty() {
        let len = peers.len();
        peers.rotate_left(*peer_cursor % len);
        *peer_cursor = peer_cursor.wrapping_add(1);
    }
    let mut turn = super::delivery::Turn::new();
    for (peer, _) in peers.into_iter().take(super::delivery::TURN_MESSAGES) {
        // An aligned prefix does not prove that later summary pages have no gaps.
        context
            .history_cursors
            .write()
            .await
            .entry(peer.clone())
            .or_default()
            .catch_up = true;
        if let Err(error) = request_summary(
            &context.node,
            &context.repair_budget,
            &context.history_cursors,
            &peer,
            context.request_timeout,
            Some(&mut turn),
        )
        .await
        {
            context.metrics.record_sync_error();
            debug!(%peer, %error, "history summary request failed");
        }
    }
}

/// Summary pages are small and idempotent, so a lost request is retried quickly
/// instead of blocking that peer's walk for the whole socket timeout.
pub(super) const SUMMARY_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
/// An origin already requested from another peer is not requested again until
/// this much of that request's lifetime has elapsed; later requests hedge loss.
pub(super) const REPAIR_HEDGE_AFTER: Duration = Duration::from_secs(2);
/// Repeated walks per catch-up session while gaps remain (2, 4, 8 and 16 s apart);
/// afterwards the periodic anti-entropy cadence takes over.
pub(super) const CATCH_UP_REWALKS: u32 = 4;

/// Claims the per-peer summary slot and requests the cursor's next page.
/// Returns `false` when a summary request to that peer is still outstanding.
async fn request_summary(
    node: &Arc<dyn crate::peer_io::PeerIo>,
    budget: &std::sync::Mutex<super::delivery::RepairBudget>,
    cursors: &RwLock<HashMap<String, HistoryCursor>>,
    peer: &str,
    timeout: Duration,
    turn: Option<&mut super::delivery::Turn>,
) -> anyhow::Result<bool> {
    let after_origin = {
        let mut cursors = cursors.write().await;
        let cursor = cursors.entry(peer.to_owned()).or_default();
        let now = node.now();
        if cursor
            .request_deadline
            .is_some_and(|deadline| deadline > now)
        {
            #[cfg(test)]
            trace_recovery_event("summary_request_deferred_pending", 0);
            return Ok(false);
        }
        #[cfg(test)]
        if cursor
            .request_deadline
            .is_some_and(|deadline| deadline <= now)
        {
            trace_recovery_event("summary_request_expired", 0);
        }
        let deadline = now
            .checked_add(timeout.min(SUMMARY_REQUEST_TIMEOUT))
            .ok_or_else(|| anyhow::anyhow!("history summary deadline overflow"))?;
        cursor.request_deadline = Some(deadline);
        cursor.after_origin.clone()
    };
    if let Err(error) = send_repair_history(
        node,
        budget,
        peer,
        HistoryMessage::SummaryRequest { after_origin },
        turn,
    )
    .await
    {
        #[cfg(test)]
        trace_recovery_send_failure("summary", &error);
        // Keep the claimed deadline: a refused or failed send is retried after it,
        // never in a tight loop.
        return Err(error.into());
    }
    #[cfg(test)]
    trace_recovery_event("summary_request_sent", 0);
    Ok(true)
}

fn checked_anti_entropy_deadline(
    previous: StdInstant,
    interval: Duration,
    now: StdInstant,
) -> Option<StdInstant> {
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
    now: StdInstant,
) {
    let retained = candidates.iter().collect::<HashSet<_>>();
    let mut existing = state
        .drain(..)
        .map(|peer| (peer.addr.clone(), peer))
        .collect::<HashMap<_, _>>();

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

pub(super) fn estimate_gossip_size(
    local_id: &NodeId,
    records: &[nx_net::MembershipRecord],
    peers: &[(String, NodeId)],
) -> usize {
    let mut identities = HashSet::new();
    identities.insert(local_id.clone());
    identities.extend(
        records
            .iter()
            .filter(|record| {
                matches!(
                    record.state,
                    nx_net::MembershipState::Alive | nx_net::MembershipState::Suspect
                )
            })
            .map(|record| NodeId::new(record.member.clone())),
    );
    identities.extend(peers.iter().map(|(_, identity)| identity.clone()));
    identities.len()
}

pub(super) fn select_gossip_peers(
    local_id: &NodeId,
    peers: &[(String, NodeId)],
    cluster_size: usize,
    excluded_addr: Option<&str>,
    fanout_constant: usize,
    max_fanout: usize,
    cursor: &AtomicUsize,
) -> Vec<String> {
    let logarithmic_fanout = if cluster_size <= 1 {
        0
    } else {
        (usize::BITS - (cluster_size - 1).leading_zeros()) as usize
    };
    let requested = logarithmic_fanout
        .saturating_add(fanout_constant)
        .min(max_fanout);
    let mut eligible = peers
        .iter()
        .filter(|(addr, _)| excluded_addr != Some(addr.as_str()))
        .map(|(addr, _)| addr.clone())
        .collect::<Vec<_>>();
    eligible.sort_by_cached_key(|addr| {
        let digest = blake3::hash(format!("numax-gossip/{local_id}/{addr}").as_bytes());
        (*digest.as_bytes(), addr.clone())
    });

    let count = requested.min(eligible.len());
    if count == 0 {
        return Vec::new();
    }

    let start = cursor.fetch_add(count, Ordering::Relaxed) % eligible.len();
    (0..count)
        .map(|offset| eligible[(start + offset) % eligible.len()].clone())
        .collect()
}

/// The mutex covers only pure pacing decisions, never encoding or I/O. Denial
/// skips a fast-path attempt; durable history remains eligible for anti-entropy.
async fn send_repair_history(
    node: &Arc<dyn crate::peer_io::PeerIo>,
    budget: &std::sync::Mutex<super::delivery::RepairBudget>,
    addr: &str,
    message: HistoryMessage,
    turn: Option<&mut super::delivery::Turn>,
) -> nx_net::NetResult<()> {
    #[cfg(test)]
    let trace = nx_net::HistoryTrace::from_message(addr, &message);
    #[cfg(test)]
    if let Some(trace) = &trace {
        trace.emit("send_begin");
    }
    #[cfg(test)]
    let budget_event = match &message {
        HistoryMessage::SummaryRequest { .. } => "summary_send_budget_deferred",
        HistoryMessage::Request { .. } => "range_send_budget_deferred",
        HistoryMessage::BaselineRequest { .. } => "baseline_send_budget_deferred",
        HistoryMessage::Push { .. } => "push_send_budget_deferred",
        HistoryMessage::Summary { .. }
        | HistoryMessage::Replay { .. }
        | HistoryMessage::Unavailable { .. }
        | HistoryMessage::Baseline { .. }
        | HistoryMessage::BaselineUnavailable { .. } => "response_send_budget_deferred",
    };
    let preparation = node.clone().prepare_history_to_addr(addr, message).await;
    #[cfg(test)]
    if let Some(trace) = &trace {
        trace.emit(if preparation.is_ok() {
            "send_prepared"
        } else {
            "send_prepare_failed"
        });
    }
    let prepared = preparation?;
    let bytes = prepared.encoded_len();
    if turn.is_some_and(|turn| !turn.admit(bytes))
        || !budget
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .admit(bytes, node.now())
    {
        #[cfg(test)]
        trace_recovery_event(budget_event, 0);
        #[cfg(test)]
        if let Some(trace) = &trace {
            trace.emit("send_budget_denied");
        }
        return Err(NetError::Io(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "recovery budget exhausted; periodic repair will retry",
        )));
    }
    let result = node
        .send_prepared_history_with_timeout(prepared, super::delivery::SEND_TIMEOUT)
        .await
        .map(|_| ());
    #[cfg(test)]
    if let Some(trace) = &trace {
        trace.emit(if result.is_ok() {
            "send_completed"
        } else {
            "send_failed"
        });
    }
    result
}

async fn send_paced_history(
    node: &Arc<dyn crate::peer_io::PeerIo>,
    metrics: &RuntimeMetrics,
    pacing: &std::sync::Mutex<super::pacing::Pacer>,
    membership: &watch::Receiver<crate::membership::runtime::Snapshot>,
    addr: &str,
    message: HistoryMessage,
    budget: (super::pacing::Load, &mut super::delivery::Turn),
) -> nx_net::NetResult<bool> {
    let prepared = node.clone().prepare_history_to_addr(addr, message).await?;
    if !budget.1.admit(prepared.encoded_len()) {
        metrics.record_gossip_pacing(false, prepared.encoded_len(), 0, None);
        return Ok(false);
    }
    let sample = membership
        .borrow()
        .probe_timings
        .iter()
        .find(|sample| sample.member == prepared.peer_id().as_str())
        .map(|sample| sample.timing);
    let generation = prepared.generation();
    let bytes = prepared.encoded_len();
    let decision = pacing
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .admit(
            addr,
            generation.clone(),
            bytes,
            node.now(),
            budget.0,
            sample,
        );
    if !decision.allowed && decision.bytes_per_second == 0 {
        return Err(NetError::PeerDisconnected(addr.to_owned()));
    }
    metrics.record_gossip_pacing(
        decision.allowed,
        bytes,
        decision.bytes_per_second,
        decision.round_trip,
    );
    if !decision.allowed {
        return Ok(false);
    }
    let result = node
        .send_prepared_history_with_timeout(prepared, super::delivery::SEND_TIMEOUT)
        .await;
    pacing
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .sent(addr, &generation, node.now(), result.as_ref().ok().copied());
    result.map(|_| true)
}

async fn forward_batch(
    context: &ForwardLoopContext,
    forwarded_rx: &mut super::delivery::Receiver<ForwardedOp>,
    first: ForwardedOp,
    pending: &mut Option<ForwardedOp>,
) {
    let load = super::pacing::Load {
        queued: forwarded_rx.len().saturating_add(1),
        capacity: forwarded_rx.max_capacity(),
    };
    let source_addr = first.source_addr;
    let mut entries = vec![first.entry];
    let byte_budget = context.max_message_size;
    let mut sizes = PushFrameSize::default();
    if !sizes.include(&entries[0], byte_budget) {
        context.metrics.record_sync_error();
        warn!(%source_addr, byte_budget, "forwarded operation exceeds gossip byte budget; durable history remains available for repair");
        return;
    }

    let seed = super::delivery::seed(entries[0].op.id.as_str())
        ^ super::delivery::seed(context.local_id.as_str());
    context
        .node
        .sleep_until(
            context.node.now()
                + super::delivery::Jitter::new(seed)
                    .delay(BROADCAST_COALESCE_DELAY.saturating_mul(2)),
        )
        .await;
    for _ in 1..BROADCAST_BATCH_MAX {
        match forwarded_rx.try_recv() {
            Ok(forwarded) if forwarded.source_addr == source_addr => {
                if !sizes.include(&forwarded.entry, byte_budget) {
                    *pending = Some(forwarded);
                    break;
                }
                entries.push(forwarded.entry);
            }
            Ok(forwarded) => {
                *pending = Some(forwarded);
                break;
            }
            Err(_) => break,
        }
    }

    let peers = context.node.connected_peers().await;
    let cluster_size = estimate_gossip_size(
        &context.local_id,
        &context.membership.borrow().records,
        &peers,
    );
    let recipients = select_gossip_peers(
        &context.local_id,
        &peers,
        cluster_size,
        Some(&source_addr),
        context.fanout_constant,
        context.max_fanout,
        &context.fanout_cursor,
    );
    context.metrics.peers.fanout(true, false, recipients.len());
    if recipients.is_empty() {
        context.metrics.peers.fanout(true, true, 0);
        return;
    }

    let count = entries.len();
    let started = context.node.now();
    let mut sent = 0;
    let mut failed = 0;
    let mut turn = super::delivery::Turn::new();
    for peer in recipients.into_iter().take(super::delivery::TURN_MESSAGES) {
        match send_paced_history(
            &context.node,
            &context.metrics,
            &context.pacing,
            &context.membership,
            &peer,
            HistoryMessage::Push {
                ops: entries.clone(),
            },
            (load, &mut turn),
        )
        .await
        {
            Ok(true) => sent += 1,
            Ok(false) => {}
            Err(error) => {
                failed += 1;
                debug!(%peer, %error, count, "forward send failed; durable history remains available for repair");
            }
        }
    }
    if sent > 0 {
        context.metrics.record_broadcast_batch(count);
        context
            .metrics
            .record_sync_latency(context.node.now().saturating_duration_since(started));
    }
    context.metrics.peers.fanout(true, true, sent + failed);
    if failed > 0 {
        context.metrics.record_sync_error();
        warn!(
            count,
            sent,
            failed,
            "forward batch partially failed; durable history remains available for repair"
        );
    }
}

fn enqueue_forwarded_ops(
    addr: &str,
    entries: Vec<nx_sync::SequencedOp>,
    context: &NodeEventContext,
) {
    let Some(forward_tx) = context.forward_tx.as_ref() else {
        return;
    };
    for entry in entries {
        if let Err(error) = forward_tx.try_send(ForwardedOp {
            source_addr: addr.to_owned(),
            entry,
        }) {
            context.metrics.record_sync_error();
            warn!(%addr, %error, "forward queue is full or shutting down; durable history remains available for repair");
        }
    }
}

#[derive(Default)]
struct PushFrameSize {
    bytes: [usize; 2],
    count: usize,
}

impl PushFrameSize {
    fn include(&mut self, entry: &nx_sync::SequencedOp, max_message_size: usize) -> bool {
        let mut next = self.bytes;
        for (index, format) in [
            nx_net::SerializationFormat::Json,
            nx_net::SerializationFormat::Bincode,
        ]
        .into_iter()
        .enumerate()
        {
            let Ok(single) = nx_net::history_frame_len(
                &HistoryMessage::Push {
                    ops: vec![entry.clone()],
                },
                format,
            ) else {
                return false;
            };
            next[index] = if self.count == 0 {
                single
            } else {
                let Ok(empty) =
                    nx_net::history_frame_len(&HistoryMessage::Push { ops: Vec::new() }, format)
                else {
                    return false;
                };
                // JSON adds a comma; wincode uses a fixed-width vector length.
                // Size each entry once instead of repeatedly encoding the prefix.
                self.bytes[index]
                    .saturating_add(single - empty)
                    .saturating_add(usize::from(format == nx_net::SerializationFormat::Json))
            };
            if next[index] > REPLAY_BYTES || next[index] - 4 > max_message_size {
                return false;
            }
        }
        self.bytes = next;
        self.count += 1;
        true
    }
}

pub(super) async fn broadcast_batch(
    context: &BroadcastLoopContext,
    op_rx: &mut super::delivery::Receiver<AcceptedLocalOp>,
    first: AcceptedLocalOp,
    pending: &mut Option<AcceptedLocalOp>,
) {
    let load = super::pacing::Load {
        queued: op_rx.len().saturating_add(1),
        capacity: op_rx.max_capacity(),
    };
    let byte_budget = context.max_message_size;
    let mut batch = vec![first.into_sequenced()];
    let mut sizes = PushFrameSize::default();
    if !sizes.include(&batch[0], byte_budget) {
        context.metrics.record_sync_error();
        debug!("local gossip entry exceeds byte budget; durable repair retains accepted operation");
        return;
    }
    let seed = super::delivery::seed(batch[0].op.id.as_str())
        ^ super::delivery::seed(context.local_id.as_str());

    // Give producers a tiny window to fill the channel so sustained load is
    // sent in larger PushOps messages instead of many tiny network writes.
    context
        .node
        .sleep_until(
            context.node.now()
                + super::delivery::Jitter::new(seed)
                    .delay(BROADCAST_COALESCE_DELAY.saturating_mul(2)),
        )
        .await;

    for _ in 1..BROADCAST_BATCH_MAX {
        match op_rx.try_recv() {
            Ok(op) => {
                let entry = op.clone().into_sequenced();
                if !sizes.include(&entry, byte_budget) {
                    *pending = Some(op);
                    break;
                }
                batch.push(entry);
            }
            Err(_) => break,
        }
    }

    let count = batch.len();
    let peers = context.node.connected_peers().await;
    let cluster_size = estimate_gossip_size(
        &context.local_id,
        &context.membership.borrow().records,
        &peers,
    );
    let recipients = select_gossip_peers(
        &context.local_id,
        &peers,
        cluster_size,
        None,
        context.fanout_constant,
        context.max_fanout,
        &context.fanout_cursor,
    );
    context.metrics.peers.fanout(false, false, recipients.len());
    if recipients.is_empty() {
        context.metrics.peers.fanout(false, true, 0);
        return;
    }

    let started = context.node.now();
    let mut sent = 0;
    let mut failed = 0;
    let mut turn = super::delivery::Turn::new();
    for peer in recipients.into_iter().take(super::delivery::TURN_MESSAGES) {
        match send_paced_history(
            &context.node,
            &context.metrics,
            &context.pacing,
            &context.membership,
            &peer,
            HistoryMessage::Push { ops: batch.clone() },
            (load, &mut turn),
        )
        .await
        {
            Ok(true) => sent += 1,
            Ok(false) => {}
            Err(error) => {
                failed += 1;
                debug!(%peer, %error, count, "gossip send failed; durable history remains available for repair");
            }
        }
    }
    if sent > 0 {
        context.metrics.record_broadcast_batch(count);
        context
            .metrics
            .record_sync_latency(context.node.now().saturating_duration_since(started));
        debug!(count, recipients = sent, failed, "gossip batch sent");
    }
    context.metrics.peers.fanout(false, true, sent + failed);
    if failed > 0 {
        context.metrics.record_sync_error();
        warn!(
            count,
            sent,
            failed,
            "gossip batch partially failed; durable history remains available for repair"
        );
    }
}

pub(super) async fn drain_broadcast_queue(
    context: &BroadcastLoopContext,
    op_rx: &mut super::delivery::Receiver<AcceptedLocalOp>,
    mut pending: Option<AcceptedLocalOp>,
) {
    while let Some(first) = pending.take().or_else(|| op_rx.try_recv().ok()) {
        broadcast_batch(context, op_rx, first, &mut pending).await;
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

#[cfg(test)]
pub(super) async fn drain_node_events(
    event_rx: &mut mpsc::Receiver<NodeEvent>,
    context: &NodeEventContext,
) {
    let mut priority_event_rx = None;
    while let Ok(event) = event_rx.try_recv() {
        let _ = handle_node_events(
            event,
            event_rx,
            &mut priority_event_rx,
            false,
            context,
            None,
        )
        .await;
    }
}

pub(super) const PRIORITY_EVENT_BURST: usize = 8;
pub(super) const REMOTE_HISTORY_COALESCE_WINDOW: Duration = Duration::from_millis(5);

pub(super) async fn drain_node_event_queues(
    event_rx: &mut mpsc::Receiver<NodeEvent>,
    priority_event_rx: &mut Option<mpsc::Receiver<NodeEvent>>,
    context: &NodeEventContext,
) {
    let mut priority_events = 0;
    loop {
        if (priority_events < PRIORITY_EVENT_BURST || event_rx.is_empty())
            && let Some(event) = priority_event_rx
                .as_mut()
                .and_then(|receiver| receiver.try_recv().ok())
        {
            priority_events += 1;
            handle_node_event(event, context).await;
            continue;
        }

        match event_rx.try_recv() {
            Ok(event) => {
                priority_events = 0;
                let _ =
                    handle_node_events(event, event_rx, priority_event_rx, false, context, None)
                        .await;
            }
            Err(_) => {
                if let Some(event) = priority_event_rx
                    .as_mut()
                    .and_then(|receiver| receiver.try_recv().ok())
                {
                    priority_events += 1;
                    handle_node_event(event, context).await;
                } else {
                    break;
                }
            }
        }
    }
}

struct IncomingPush {
    from: NodeId,
    addr: String,
    ops: Vec<nx_sync::SequencedOp>,
}

struct IncomingReplay {
    from: NodeId,
    addr: String,
    request_id: nx_sync::OpId,
    origin: NodeId,
    start: u64,
    end: u64,
    ops: Vec<nx_sync::SequencedOp>,
}

enum IncomingHistory {
    Push(IncomingPush),
    Replay(IncomingReplay),
}

impl IncomingHistory {
    fn into_event(self) -> NodeEvent {
        match self {
            Self::Push(push) => NodeEvent::HistoryReceived {
                from: push.from,
                addr: push.addr,
                message: HistoryMessage::Push { ops: push.ops },
            },
            Self::Replay(replay) => replay.into_event(),
        }
    }

    fn ops(&self) -> &[nx_sync::SequencedOp] {
        match self {
            Self::Push(push) => &push.ops,
            Self::Replay(replay) => &replay.ops,
        }
    }
}

impl IncomingReplay {
    fn into_event(self) -> NodeEvent {
        NodeEvent::HistoryReceived {
            from: self.from,
            addr: self.addr,
            message: HistoryMessage::Replay {
                request_id: self.request_id,
                origin: self.origin,
                start: self.start,
                end: self.end,
                ops: self.ops,
            },
        }
    }
}

fn push_work(event: &NodeEvent) -> Option<(usize, usize)> {
    match event {
        NodeEvent::HistoryReceived {
            message: HistoryMessage::Push { ops },
            ..
        } if !ops.is_empty() && ops.len() <= 1024 => {
            let bytes = ops.iter().try_fold(0usize, |total, entry| {
                total.checked_add(super::delivery::entry_bytes(&entry.op, &entry.position)?)
            })?;
            Some((ops.len(), bytes))
        }
        _ => None,
    }
}

fn replay_work(event: &NodeEvent) -> Option<(usize, usize)> {
    match event {
        NodeEvent::HistoryReceived {
            message: HistoryMessage::Replay { ops, .. },
            ..
        } if !ops.is_empty() && ops.len() <= 1024 => {
            let mut encoded = super::delivery::ByteCount::default();
            serde_json::to_writer(&mut encoded, ops).ok()?;
            if encoded.0 > REPLAY_BYTES {
                return None;
            }
            let bytes = ops.iter().try_fold(0usize, |total, entry| {
                total.checked_add(super::delivery::entry_bytes(&entry.op, &entry.position)?)
            })?;
            Some((ops.len(), bytes))
        }
        _ => None,
    }
}

fn remote_history_work(event: &NodeEvent) -> Option<(usize, usize)> {
    push_work(event).or_else(|| replay_work(event))
}

#[cfg(test)]
fn remote_history_stop_reason(event: &NodeEvent) -> &'static str {
    match event {
        NodeEvent::HistoryReceived {
            message: HistoryMessage::Summary { .. },
            ..
        } => "control_summary",
        NodeEvent::HistoryReceived {
            message: HistoryMessage::SummaryRequest { .. },
            ..
        } => "control_summary_request",
        NodeEvent::HistoryReceived {
            message: HistoryMessage::Request { .. },
            ..
        } => "control_range_request",
        NodeEvent::HistoryReceived {
            message: HistoryMessage::Unavailable { .. },
            ..
        } => "control_unavailable",
        NodeEvent::HistoryReceived {
            message:
                HistoryMessage::BaselineRequest { .. }
                | HistoryMessage::Baseline { .. }
                | HistoryMessage::BaselineUnavailable { .. },
            ..
        } => "control_baseline",
        NodeEvent::HistoryReceived {
            message: HistoryMessage::Push { ops },
            ..
        } if ops.is_empty() => "empty_push",
        NodeEvent::HistoryReceived {
            message: HistoryMessage::Replay { ops, .. },
            ..
        } if ops.is_empty() => "empty_replay",
        NodeEvent::HistoryReceived {
            message: HistoryMessage::Push { .. } | HistoryMessage::Replay { .. },
            ..
        } => "invalid_data_envelope",
        NodeEvent::OpsReceived { .. } | NodeEvent::PullRequested { .. } => "legacy_event",
        _ => "other_event",
    }
}

#[cfg(test)]
pub(super) fn trace_remote_history_batch(
    kind: &'static str,
    pushes: usize,
    replays: usize,
    operations: usize,
    bytes: usize,
    stop_reason: &'static str,
) {
    let envelopes = pushes + replays;
    tracing::debug!(
        target: "nx_core::sync_manager::replication",
        history_batch_kind = kind,
        history_batch_stop = stop_reason,
        history_batch_envelopes = envelopes as u64,
        history_batch_pushes = pushes as u64,
        history_batch_replays = replays as u64,
        history_batch_operations = operations as u64,
        history_batch_estimated_bytes = bytes as u64,
        "bounded remote history batch"
    );
}

#[cfg(test)]
pub(super) fn trace_dequeued_history(event: &NodeEvent) {
    if let NodeEvent::HistoryReceived { from, message, .. } = event
        && let Some(trace) = nx_net::HistoryTrace::from_message(from.as_str(), message)
    {
        trace.emit("dequeued");
    }
}

async fn collect_remote_history(
    first: NodeEvent,
    event_rx: &mut mpsc::Receiver<NodeEvent>,
    priority_event_rx: &mut Option<mpsc::Receiver<NodeEvent>>,
    allow_priority_interrupt: bool,
    coalesce_window: Option<Duration>,
) -> (Vec<IncomingHistory>, Option<NodeEvent>, Option<NodeEvent>) {
    enum BatchRead {
        Data(Option<NodeEvent>),
        Priority(Option<NodeEvent>),
        Timeout,
    }

    let mut history = Vec::new();
    let mut pending = Some(first);
    let mut pending_priority = None;
    let mut operations = 0;
    let mut bytes = 0;
    let deadline = coalesce_window
        .filter(|_| {
            pending
                .as_ref()
                .is_some_and(|event| remote_history_work(event).is_some())
        })
        .map(|window| tokio::time::Instant::now() + window);
    #[cfg(test)]
    let mut stop_reason = "queue_empty";
    while history.len() < super::delivery::TURN_MESSAGES {
        if allow_priority_interrupt
            && deadline.is_some()
            && let Some(priority_rx) = priority_event_rx.as_mut()
        {
            match priority_rx.try_recv() {
                Ok(event) => {
                    #[cfg(test)]
                    trace_dequeued_history(&event);
                    pending_priority = Some(event);
                    #[cfg(test)]
                    {
                        stop_reason = "control_priority";
                    }
                    break;
                }
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    priority_event_rx.take();
                }
                Err(mpsc::error::TryRecvError::Empty) => {}
            }
        }
        #[cfg(test)]
        let from_pending = pending.is_some();
        let event = match pending.take() {
            Some(event) => Some(event),
            None => match event_rx.try_recv() {
                Ok(event) => Some(event),
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    #[cfg(test)]
                    {
                        stop_reason = "data_channel_closed";
                    }
                    break;
                }
                Err(mpsc::error::TryRecvError::Empty) => {
                    if let Some(deadline) = deadline {
                        let read = if allow_priority_interrupt
                            && let Some(priority_rx) = priority_event_rx.as_mut()
                        {
                            tokio::select! {
                                biased;
                                event = priority_rx.recv() => BatchRead::Priority(event),
                                event = event_rx.recv() => BatchRead::Data(event),
                                _ = tokio::time::sleep_until(deadline) => BatchRead::Timeout,
                            }
                        } else {
                            tokio::select! {
                                event = event_rx.recv() => BatchRead::Data(event),
                                _ = tokio::time::sleep_until(deadline) => BatchRead::Timeout,
                            }
                        };
                        match read {
                            BatchRead::Data(Some(event)) => Some(event),
                            BatchRead::Data(None) => {
                                #[cfg(test)]
                                {
                                    stop_reason = "data_channel_closed";
                                }
                                break;
                            }
                            BatchRead::Priority(Some(event)) => {
                                #[cfg(test)]
                                trace_dequeued_history(&event);
                                pending_priority = Some(event);
                                #[cfg(test)]
                                {
                                    stop_reason = "control_priority";
                                }
                                break;
                            }
                            BatchRead::Priority(None) => {
                                priority_event_rx.take();
                                continue;
                            }
                            BatchRead::Timeout => {
                                #[cfg(test)]
                                {
                                    stop_reason = "coalesce_timeout";
                                }
                                break;
                            }
                        }
                    } else {
                        None
                    }
                }
            },
        };
        let Some(event) = event else {
            #[cfg(test)]
            {
                stop_reason = "queue_empty";
            }
            break;
        };
        #[cfg(test)]
        if !from_pending {
            trace_dequeued_history(&event);
        }
        let Some((count, size)) = remote_history_work(&event) else {
            #[cfg(test)]
            {
                stop_reason = remote_history_stop_reason(&event);
            }
            pending = Some(event);
            break;
        };
        if operations + count > 1024 {
            #[cfg(test)]
            {
                stop_reason = "operation_budget";
            }
            pending = Some(event);
            break;
        }
        if bytes + size > super::delivery::TURN_BYTES {
            #[cfg(test)]
            {
                stop_reason = "byte_budget";
            }
            pending = Some(event);
            break;
        }
        let incoming = match event {
            NodeEvent::HistoryReceived {
                from,
                addr,
                message: HistoryMessage::Push { ops },
            } => IncomingHistory::Push(IncomingPush { from, addr, ops }),
            NodeEvent::HistoryReceived {
                from,
                addr,
                message:
                    HistoryMessage::Replay {
                        request_id,
                        origin,
                        start,
                        end,
                        ops,
                    },
            } => IncomingHistory::Replay(IncomingReplay {
                from,
                addr,
                request_id,
                origin,
                start,
                end,
                ops,
            }),
            event => {
                pending = Some(event);
                break;
            }
        };
        operations += count;
        bytes += size;
        history.push(incoming);
    }
    #[cfg(test)]
    if history.len() == super::delivery::TURN_MESSAGES {
        stop_reason = "message_limit";
    }
    #[cfg(test)]
    if !history.is_empty() {
        let pushes = history
            .iter()
            .filter(|envelope| matches!(envelope, IncomingHistory::Push(_)))
            .count();
        let replays = history.len() - pushes;
        let kind = if pushes == history.len() {
            "push_only"
        } else if pushes == 0 {
            "replay_only"
        } else {
            "mixed"
        };
        trace_remote_history_batch(kind, pushes, replays, operations, bytes, stop_reason);
    }
    (history, pending, pending_priority)
}

pub(super) async fn handle_node_events(
    first: NodeEvent,
    event_rx: &mut mpsc::Receiver<NodeEvent>,
    priority_event_rx: &mut Option<mpsc::Receiver<NodeEvent>>,
    allow_priority_interrupt: bool,
    context: &NodeEventContext,
    coalesce_window: Option<Duration>,
) -> Option<NodeEvent> {
    let (history, pending, pending_priority) = collect_remote_history(
        first,
        event_rx,
        priority_event_rx,
        allow_priority_interrupt,
        coalesce_window,
    )
    .await;
    if history.len() == 1 {
        if let Some(envelope) = history.into_iter().next() {
            handle_node_event(envelope.into_event(), context).await;
        }
    } else if !history.is_empty() {
        handle_remote_history_batch(history, context).await;
    }
    if let Some(event) = pending {
        handle_node_event(event, context).await;
    }
    pending_priority
}

async fn finish_history_event(
    from: NodeId,
    addr: &str,
    observed_id: Option<String>,
    result: &anyhow::Result<()>,
    context: &NodeEventContext,
) {
    if let Err(error) = result {
        context
            .metrics
            .peers
            .increment(crate::peer_metrics::Counter::RecoveryError);
        context.metrics.record_sync_error();
        if let Some(rejected) = error.downcast_ref::<super::baseline::Rejected>() {
            // Decoder errors can contain supplied values; log only the bounded reason.
            warn!(%from, reason = ?rejected.0, "replication baseline rejected");
        } else {
            warn!(%from, %addr, error = %format_args!("{error:#}"), "replication history exchange failed");
        }
    } else if let Some(id) = observed_id {
        context
            .anti_entropy_watermarks
            .write()
            .await
            .insert(from, id);
    }
    context
        .node
        .refresh_recovery(&context.metrics, Arc::clone(&context.store))
        .await;
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
            let result = handle_history_message(&addr, message, context).await;
            finish_history_event(from, &addr, observed_id, &result, context).await;
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
            // A new session may bridge a healed partition: walk its whole summary now
            // instead of one page per anti-entropy interval.
            {
                let mut cursors = context.history_cursors.write().await;
                let cursor = cursors.entry(addr.clone()).or_default();
                cursor.catch_up = true;
                cursor.after_origin = None;
                cursor.walk_gap = false;
                cursor.rewalks = 0;
            }
            if let Err(error) = request_summary(
                &context.node,
                &context.repair_budget,
                &context.history_cursors,
                &addr,
                context.history_request_timeout,
                None,
            )
            .await
            {
                debug!(peer = %addr, %error, "catch-up summary request deferred");
            }
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
        storage_executor: context.node.storage_executor(),
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
    request_history_or_baseline_inner(addr, origin, start, end, context)
        .await
        .map(|_| ())
}

async fn request_history_or_baseline_inner(
    addr: &str,
    origin: nx_sync::NodeId,
    start: u64,
    end: u64,
    context: &NodeEventContext,
) -> anyhow::Result<bool> {
    let request_id = context.node.request_id();
    let baseline_revision = if start == 0 {
        let store = Arc::clone(&context.store);
        let tracked_origin = origin.clone();
        Some(
            context
                .node
                .storage_executor()
                .execute(move || super::storage::recovery_revision(&store, &tracked_origin))
                .await?,
        )
    } else {
        None
    };
    {
        let mut requests = context.history_requests.write().await;
        let now = context.node.now();
        // A fresh request for this origin to another peer is likely to repair it;
        // only once it has aged past the hedge delay may another peer be asked.
        let hedge_window = context
            .history_request_timeout
            .saturating_sub(REPAIR_HEDGE_AFTER);
        if requests.iter().any(|(peer, pending)| {
            peer != addr
                && pending.get(origin.as_str()).is_some_and(|request| {
                    request.deadline > now && request.deadline.duration_since(now) > hedge_window
                })
        }) {
            #[cfg(test)]
            trace_recovery_event("range_request_deferred_hedge", 0);
            debug!(%addr, %origin, "deferring history range: fresh request pending at another peer");
            return Ok(false);
        }
        let pending = requests.entry(addr.to_owned()).or_default();
        #[cfg(test)]
        {
            let expired = pending
                .values()
                .filter(|request| request.deadline <= now)
                .count() as u64;
            if expired > 0 {
                trace_recovery_event("range_request_expired", expired);
            }
        }
        pending.retain(|_, request| request.deadline > now);
        if pending.contains_key(origin.as_str()) {
            #[cfg(test)]
            trace_recovery_event("range_request_deferred_same_peer", 0);
            debug!(%addr, %origin, "deferring history range: request pending or peer budget full");
            return Ok(false);
        }
        if pending.len() >= SUMMARY_PAGE {
            #[cfg(test)]
            trace_recovery_event("range_request_deferred_peer_limit", 0);
            debug!(%addr, %origin, "deferring history range: request pending or peer budget full");
            return Ok(false);
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
                baseline_revision,
                deadline,
            },
        );
        #[cfg(test)]
        nx_net::HistoryTrace::new("request", addr, &origin, &request_id, start, end)
            .emit("request_created");
    }
    if let Err(error) = send_repair_history(
        &context.node,
        &context.repair_budget,
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
        None,
    )
    .await
    {
        complete_history_request(addr, &origin, &request_id, start, end, context).await;
        #[cfg(test)]
        trace_recovery_send_failure(if start == 0 { "baseline" } else { "range" }, &error);
        return Err(error.into());
    }
    if start == 0 {
        #[cfg(test)]
        trace_recovery_event("baseline_request_sent", 0);
        context
            .metrics
            .peers
            .increment(crate::peer_metrics::Counter::BaselineRequested);
    } else {
        #[cfg(test)]
        trace_recovery_event(
            "range_request_sent",
            end.saturating_sub(start).saturating_add(1),
        );
    }
    Ok(true)
}

async fn complete_history_request(
    addr: &str,
    origin: &nx_sync::NodeId,
    request_id: &nx_sync::OpId,
    start: u64,
    end: u64,
    context: &NodeEventContext,
) -> Option<PendingHistoryRequest> {
    #[cfg(test)]
    let trace = nx_net::HistoryTrace::new("response", addr, origin, request_id, start, end);
    #[cfg(test)]
    trace.emit("request_lock_begin");
    let mut requests = context.history_requests.write().await;
    #[cfg(test)]
    trace.emit("request_lock_acquired");
    if let Some(pending) = requests.get_mut(addr)
        && let Some(request) = pending.get(origin.as_str())
        && request.request_id == *request_id
        && request.deadline > context.node.now()
        && request.start == start
        && end >= start
        && end <= request.end
    {
        #[cfg(test)]
        debug!(
            target: "nx_core::sync_manager::replication",
            recovery_trace_stage = "request_lookup",
            peer = addr, origin = %origin, request_id = %request_id,
            start, end, decision = "matched",
            "test recovery timeline"
        );
        return pending.remove(origin.as_str());
    }
    #[cfg(test)]
    {
        let request = requests
            .get(addr)
            .and_then(|pending| pending.get(origin.as_str()));
        let decision = match request {
            None => "missing",
            Some(request) if request.request_id != *request_id => "superseded",
            Some(request) if request.start != start || end < start || end > request.end => {
                "invalid_interval"
            }
            Some(_) => "expired",
        };
        debug!(
            target: "nx_core::sync_manager::replication",
            recovery_trace_stage = "request_lookup",
            peer = addr, origin = %origin, request_id = %request_id,
            start, end, decision,
            "test recovery timeline"
        );
    }
    None
}

async fn complete_replay_request(
    addr: &str,
    request_id: &nx_sync::OpId,
    origin: &nx_sync::NodeId,
    start: u64,
    end: u64,
    ops: &[nx_sync::SequencedOp],
    context: &NodeEventContext,
) -> anyhow::Result<bool> {
    validate_range(start, end)?;
    anyhow::ensure!(
        ops.len() as u64 == end - start + 1,
        "partial replay interval"
    );
    anyhow::ensure!(
        serde_json::to_vec(ops)?.len() <= REPLAY_BYTES,
        "replay exceeds byte budget"
    );
    for (index, entry) in ops.iter().enumerate() {
        anyhow::ensure!(
            entry.op.origin == *origin && entry.position.sequence == start + index as u64,
            "replay position mismatch"
        );
    }
    let Some(_request) =
        complete_history_request(addr, origin, request_id, start, end, context).await
    else {
        debug!(%addr, %origin, %request_id, "ignoring stale or unsolicited history replay");
        return Ok(false);
    };
    #[cfg(test)]
    {
        nx_net::HistoryTrace::new("replay", addr, origin, request_id, start, end)
            .emit("replay_admitted");
        let items = u64::try_from(ops.len()).unwrap_or(u64::MAX);
        trace_recovery_event("range_replay_received", items);
        trace_recovery_latency(
            "range_replay",
            request_age(
                context.node.now(),
                _request.deadline,
                context.history_request_timeout,
            ),
            items,
        );
    }
    Ok(true)
}

async fn finish_push(
    addr: &str,
    ops: &[nx_sync::SequencedOp],
    accepted: Vec<nx_sync::SequencedOp>,
    tracking_error: Option<anyhow::Error>,
    context: &NodeEventContext,
) -> anyhow::Result<()> {
    for summary in push_summaries(ops)? {
        let store = Arc::clone(&context.store);
        let tracked = summary.clone();
        let missing = context
            .node
            .storage_executor()
            .execute(move || super::storage::record_summary(&store, &tracked))
            .await?;
        if let Some((start, end)) = missing {
            request_history_range(addr, summary.origin, start, end, context).await?;
        }
    }
    if let Some(error) = tracking_error {
        return Err(error);
    }
    enqueue_forwarded_ops(addr, accepted, context);
    Ok(())
}

fn push_summaries(ops: &[nx_sync::SequencedOp]) -> anyhow::Result<Vec<nx_sync::HistorySummary>> {
    let mut origins = BTreeMap::<String, u64>::new();
    for entry in ops {
        anyhow::ensure!(
            entry.position.generation > 0 && entry.position.sequence > 0,
            "invalid pushed position"
        );
        origins
            .entry(entry.op.origin.as_str().to_owned())
            .and_modify(|high| *high = (*high).max(entry.position.sequence))
            .or_insert(entry.position.sequence);
    }
    Ok(origins
        .into_iter()
        .take(SUMMARY_PAGE)
        .map(|(origin, high_sequence)| nx_sync::HistorySummary {
            baseline_required: false,
            origin: NodeId::new(origin),
            high_sequence,
        })
        .collect())
}

type PushSummaryGroups = BTreeMap<String, Vec<nx_sync::HistorySummary>>;
type PushSummaryRepairs = BTreeMap<String, BTreeMap<String, Option<(u64, u64)>>>;

fn group_push_summaries(history: &[IncomingHistory]) -> anyhow::Result<PushSummaryGroups> {
    let mut groups = BTreeMap::<String, BTreeMap<String, nx_sync::HistorySummary>>::new();
    for envelope in history {
        let IncomingHistory::Push(push) = envelope else {
            continue;
        };
        for summary in push_summaries(&push.ops)? {
            groups
                .entry(push.addr.clone())
                .or_default()
                .entry(summary.origin.as_str().to_owned())
                .and_modify(|existing| {
                    existing.high_sequence = existing.high_sequence.max(summary.high_sequence);
                })
                .or_insert(summary);
        }
    }
    Ok(groups
        .into_iter()
        .map(|(addr, summaries)| (addr, summaries.into_values().collect()))
        .collect())
}

#[cfg(test)]
fn trace_remote_push_summary_timing(started: StdInstant, success: bool) {
    tracing::debug!(
        target: "nx_core::sync_manager::replication",
        message = "remote push summaries recorded",
        elapsed_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
        success,
        "phase timing"
    );
}

async fn record_grouped_push_summaries(
    groups: PushSummaryGroups,
    context: &NodeEventContext,
) -> (PushSummaryRepairs, HashSet<String>) {
    let mut repairs_by_source = PushSummaryRepairs::new();
    let mut failed_sources = HashSet::new();
    for (addr, summaries) in groups {
        #[cfg(test)]
        let started = StdInstant::now();
        let store = Arc::clone(&context.store);
        let result = context
            .node
            .storage_executor()
            .execute(move || {
                let mut repairs = BTreeMap::new();
                for chunk in summaries.chunks(SUMMARY_PAGE) {
                    for repair in super::storage::record_summaries(&store, chunk)? {
                        repairs.insert(repair.origin.as_str().to_owned(), repair.missing);
                    }
                }
                Ok(repairs)
            })
            .await;
        #[cfg(test)]
        trace_remote_push_summary_timing(started, result.is_ok());
        match result {
            Ok(repairs) => {
                repairs_by_source.insert(addr, repairs);
            }
            Err(error) => {
                debug!(%addr, %error, "batched push summary tracking failed; retrying individually");
                failed_sources.insert(addr);
            }
        }
    }
    (repairs_by_source, failed_sources)
}

async fn finish_push_with_recorded_summaries(
    addr: &str,
    ops: &[nx_sync::SequencedOp],
    accepted: Vec<nx_sync::SequencedOp>,
    repairs: &BTreeMap<String, Option<(u64, u64)>>,
    context: &NodeEventContext,
) -> anyhow::Result<()> {
    for summary in push_summaries(ops)? {
        let missing = repairs
            .get(summary.origin.as_str())
            .ok_or_else(|| anyhow::anyhow!("push summary missing for origin {}", summary.origin))?;
        if let Some((start, end)) = missing {
            request_history_range(addr, summary.origin, *start, *end, context).await?;
        }
    }
    enqueue_forwarded_ops(addr, accepted, context);
    Ok(())
}

async fn finish_replay_after_apply(
    addr: &str,
    origin: NodeId,
    accepted: Vec<nx_sync::SequencedOp>,
    context: &NodeEventContext,
) -> anyhow::Result<()> {
    enqueue_forwarded_ops(addr, accepted, context);
    let store = Arc::clone(&context.store);
    let tracked_origin = origin.clone();
    let missing = context
        .node
        .storage_executor()
        .execute(move || super::storage::missing_range(&store, &tracked_origin))
        .await?;
    if let Some((start, end)) = missing {
        request_history_range(addr, origin, start, end, context).await?;
    }
    Ok(())
}

async fn apply_prepared_replay(
    replay: &IncomingReplay,
    context: &NodeEventContext,
) -> anyhow::Result<()> {
    #[cfg(test)]
    let trace = nx_net::HistoryTrace::new(
        "replay",
        &replay.addr,
        &replay.origin,
        &replay.request_id,
        replay.start,
        replay.end,
    );
    #[cfg(test)]
    trace.emit("replay_apply_begin");
    let accepted =
        apply_sequenced_ops_and_collect(&replay.ops, &history_apply_context(context)).await?;
    #[cfg(test)]
    trace.emit("replay_published");
    finish_replay_after_apply(&replay.addr, replay.origin.clone(), accepted, context).await
}

async fn apply_remote_history_individually(
    history: Vec<IncomingHistory>,
    context: &NodeEventContext,
) {
    for envelope in history {
        match envelope {
            IncomingHistory::Push(push) => {
                handle_node_event(
                    NodeEvent::HistoryReceived {
                        from: push.from,
                        addr: push.addr,
                        message: HistoryMessage::Push { ops: push.ops },
                    },
                    context,
                )
                .await;
            }
            IncomingHistory::Replay(replay) => {
                let observed_id = replay
                    .ops
                    .last()
                    .map(|entry| entry.op.id.as_str().to_owned());
                let result = apply_prepared_replay(&replay, context).await;
                finish_history_event(replay.from, &replay.addr, observed_id, &result, context)
                    .await;
            }
        }
    }
}

async fn handle_remote_history_batch(history: Vec<IncomingHistory>, context: &NodeEventContext) {
    let mut admitted = Vec::with_capacity(history.len());
    for envelope in history {
        match envelope {
            IncomingHistory::Push(push) => admitted.push(IncomingHistory::Push(push)),
            IncomingHistory::Replay(replay) => {
                let observed_id = replay
                    .ops
                    .last()
                    .map(|entry| entry.op.id.as_str().to_owned());
                match complete_replay_request(
                    &replay.addr,
                    &replay.request_id,
                    &replay.origin,
                    replay.start,
                    replay.end,
                    &replay.ops,
                    context,
                )
                .await
                {
                    Ok(true) => admitted.push(IncomingHistory::Replay(replay)),
                    Ok(false) => {
                        let result = Ok(());
                        finish_history_event(
                            replay.from,
                            &replay.addr,
                            observed_id,
                            &result,
                            context,
                        )
                        .await;
                    }
                    Err(error) => {
                        let result = Err(error);
                        finish_history_event(
                            replay.from,
                            &replay.addr,
                            observed_id,
                            &result,
                            context,
                        )
                        .await;
                    }
                }
            }
        }
    }
    if admitted.is_empty() {
        return;
    }
    let ops = admitted
        .iter()
        .flat_map(|envelope| envelope.ops().iter().cloned())
        .collect::<Vec<_>>();
    #[cfg(test)]
    debug!(
        target: "nx_core::sync_manager::replication",
        recovery_trace_stage = "batch_apply_begin",
        envelopes = admitted.len() as u64, operations = ops.len() as u64,
        "test recovery timeline"
    );
    match apply_sequenced_ops_and_collect(&ops, &history_apply_context(context)).await {
        Ok(accepted) => {
            #[cfg(test)]
            for envelope in &admitted {
                if let IncomingHistory::Replay(replay) = envelope {
                    nx_net::HistoryTrace::new(
                        "replay",
                        &replay.addr,
                        &replay.origin,
                        &replay.request_id,
                        replay.start,
                        replay.end,
                    )
                    .emit("replay_published");
                }
            }
            let mut accepted = accepted
                .into_iter()
                .map(|entry| (entry.op.id.as_str().to_owned(), entry))
                .collect::<HashMap<_, _>>();
            let push_summary_recording = match group_push_summaries(&admitted) {
                Ok(groups) => Some(record_grouped_push_summaries(groups, context).await),
                Err(error) => {
                    debug!(%error, "cannot group push summaries; retrying individually");
                    None
                }
            };
            for envelope in admitted {
                let ops = envelope.ops();
                let forwarded = ops
                    .iter()
                    .filter_map(|entry| accepted.remove(entry.op.id.as_str()))
                    .collect();
                let observed_id = ops.last().map(|entry| entry.op.id.as_str().to_owned());
                match envelope {
                    IncomingHistory::Push(push) => {
                        let result = match &push_summary_recording {
                            Some((repairs, failed_sources))
                                if !failed_sources.contains(&push.addr)
                                    && let Some(source_repairs) = repairs.get(&push.addr) =>
                            {
                                finish_push_with_recorded_summaries(
                                    &push.addr,
                                    &push.ops,
                                    forwarded,
                                    source_repairs,
                                    context,
                                )
                                .await
                            }
                            _ => finish_push(&push.addr, &push.ops, forwarded, None, context).await,
                        };
                        finish_history_event(push.from, &push.addr, observed_id, &result, context)
                            .await;
                    }
                    IncomingHistory::Replay(replay) => {
                        let result = finish_replay_after_apply(
                            &replay.addr,
                            replay.origin,
                            forwarded,
                            context,
                        )
                        .await;
                        finish_history_event(
                            replay.from,
                            &replay.addr,
                            observed_id,
                            &result,
                            context,
                        )
                        .await;
                    }
                }
            }
        }
        Err(error) if error.is::<super::apply::RemoteBatchPreparation>() => {
            #[cfg(test)]
            debug!(
                target: "nx_core::sync_manager::replication",
                recovery_trace_stage = "preparation_fallback",
                envelopes = admitted.len() as u64,
                operations = ops.len() as u64,
                "test recovery timeline"
            );
            debug!(%error, "queued remote history batch rejected before persistence; handling envelopes individually");
            apply_remote_history_individually(admitted, context).await;
        }
        Err(error) => {
            let result = Err(error);
            for envelope in admitted {
                let (from, addr, observed_id) = match envelope {
                    IncomingHistory::Push(push) => (
                        push.from,
                        push.addr,
                        push.ops.last().map(|entry| entry.op.id.as_str().to_owned()),
                    ),
                    IncomingHistory::Replay(replay) => (
                        replay.from,
                        replay.addr,
                        replay
                            .ops
                            .last()
                            .map(|entry| entry.op.id.as_str().to_owned()),
                    ),
                };
                finish_history_event(from, &addr, observed_id, &result, context).await;
            }
        }
    }
}

async fn handle_history_message(
    addr: &str,
    message: HistoryMessage,
    context: &NodeEventContext,
) -> anyhow::Result<()> {
    // Bound response construction, including baseline export, before storage work.
    // Refusing a request leaves its requester to the existing bounded timeout and
    // periodic retry path; it does not delete any operation or baseline.
    #[cfg(test)]
    let recovery_work_event = match &message {
        HistoryMessage::SummaryRequest { .. } => "summary_work_budget_exhausted",
        HistoryMessage::Request { .. } => "range_work_budget_exhausted",
        HistoryMessage::BaselineRequest { .. } => "baseline_work_budget_exhausted",
        _ => "other_recovery_work_budget_exhausted",
    };
    if matches!(
        &message,
        HistoryMessage::SummaryRequest { .. }
            | HistoryMessage::Request { .. }
            | HistoryMessage::BaselineRequest { .. }
    ) && !context
        .repair_budget
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .admit_work(context.node.now())
    {
        #[cfg(test)]
        trace_recovery_event(recovery_work_event, 0);
        anyhow::bail!("recovery work budget exhausted; periodic repair will retry");
    }
    match message {
        HistoryMessage::Push { ops } => {
            anyhow::ensure!(
                ops.len() <= 1024,
                "sequenced batch exceeds operation budget"
            );
            let (accepted, tracking_error) = match apply_sequenced_ops_and_collect(
                &ops,
                &history_apply_context(context),
            )
            .await
            {
                Ok(accepted) => (accepted, None),
                Err(error)
                    if error
                        .downcast_ref::<super::sequence::TrackingLimit>()
                        .is_some() =>
                {
                    (Vec::new(), Some(error))
                }
                Err(error) => return Err(error),
            };
            finish_push(addr, &ops, accepted, tracking_error, context).await?;
        }
        HistoryMessage::SummaryRequest { after_origin } => {
            let store = Arc::clone(&context.store);
            let entries = context
                .node
                .storage_executor()
                .execute(move || -> anyhow::Result<_> {
                    let cursor = super::storage::summary_cursor(after_origin.as_deref())?;
                    let (entries, _) = super::storage::summary_page(&store, cursor.as_deref())?;
                    Ok(entries)
                })
                .await?;
            #[cfg(test)]
            let entry_count = entries.len() as u64;
            send_repair_history(
                &context.node,
                &context.repair_budget,
                addr,
                HistoryMessage::Summary { entries },
                None,
            )
            .await?;
            #[cfg(test)]
            trace_recovery_event("summary_page_sent", entry_count);
        }
        HistoryMessage::Summary { entries } => {
            anyhow::ensure!(
                entries.len() <= SUMMARY_PAGE,
                "history summary page exceeds budget"
            );
            #[cfg(test)]
            {
                trace_recovery_event("summary_page_received", entries.len() as u64);
                if let Some(deadline) = context
                    .history_cursors
                    .read()
                    .await
                    .get(addr)
                    .and_then(|cursor| cursor.request_deadline)
                {
                    trace_recovery_latency(
                        "summary",
                        request_age(
                            context.node.now(),
                            deadline,
                            context.history_request_timeout.min(SUMMARY_REQUEST_TIMEOUT),
                        ),
                        entries.len() as u64,
                    );
                }
            }
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
            let mut found_gap = false;
            let result = async {
                let store = Arc::clone(&context.store);
                #[cfg(test)]
                let record_started = std::time::Instant::now();
                let repairs_result = context
                    .node
                    .storage_executor()
                    .execute(move || super::storage::record_summaries(&store, &entries))
                    .await;
                #[cfg(test)]
                trace_recovery_latency(
                    "summary_record",
                    record_started.elapsed(),
                    repairs_result.as_ref().map_or(0, |repairs| {
                        u64::try_from(repairs.len()).unwrap_or(u64::MAX)
                    }),
                );
                let repairs = repairs_result?;
                #[cfg(test)]
                let gap_count = u64::try_from(repairs.len()).unwrap_or(u64::MAX);
                #[cfg(test)]
                let dispatch_started = std::time::Instant::now();
                let dispatch_result = async {
                    for repair in repairs {
                        #[cfg(test)]
                        let request_started = std::time::Instant::now();
                        let request_result = if repair.baseline {
                            found_gap = true;
                            request_baseline(addr, repair.origin, context).await
                        } else if let Some((start, end)) = repair.missing {
                            found_gap = true;
                            request_history_range(addr, repair.origin, start, end, context).await
                        } else {
                            Ok(())
                        };
                        #[cfg(test)]
                        trace_recovery_latency("summary_gap_send", request_started.elapsed(), 1);
                        request_result?;
                    }
                    Ok::<(), anyhow::Error>(())
                }
                .await;
                #[cfg(test)]
                trace_recovery_latency(
                    "summary_gap_dispatch",
                    dispatch_started.elapsed(),
                    gap_count,
                );
                dispatch_result?;
                Ok::<(), anyhow::Error>(())
            }
            .await;
            let continue_walk = {
                let mut cursors = context.history_cursors.write().await;
                let cursor = cursors.entry(addr.to_owned()).or_default();
                // A failed page has an unobserved suffix; retry it without skipping ahead.
                if result.is_ok() {
                    cursor.after_origin = next.clone();
                }
                cursor.request_deadline = None;
                cursor.catch_up |= found_gap || result.is_err();
                cursor.walk_gap |= found_gap;
                if result.is_err() {
                    false
                } else if next.is_some() {
                    cursor.catch_up
                } else {
                    // A walk that found gaps is repeated after a growing delay, so
                    // origins deferred to another peer's lost request are retried.
                    if cursor.catch_up && cursor.walk_gap && cursor.rewalks < CATCH_UP_REWALKS {
                        let delay = SUMMARY_REQUEST_TIMEOUT.saturating_mul(1 << cursor.rewalks);
                        cursor.rewalks += 1;
                        cursor.request_deadline = context.node.now().checked_add(delay);
                    } else {
                        cursor.catch_up = false;
                        cursor.rewalks = 0;
                    }
                    cursor.walk_gap = false;
                    false
                }
            };
            result?;
            if continue_walk
                && let Err(error) = request_summary(
                    &context.node,
                    &context.repair_budget,
                    &context.history_cursors,
                    addr,
                    context.history_request_timeout,
                    None,
                )
                .await
            {
                debug!(%addr, %error, "catch-up summary continuation deferred");
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
                let log = Arc::clone(&context.op_log).read_owned().await;
                let store = Arc::clone(&context.store);
                let requested_origin = origin.clone();
                let mut retained = context
                    .node
                    .storage_executor()
                    .execute(move || {
                        let mut retained = Vec::new();
                        for op in log.iter().filter(|op| op.origin == requested_origin) {
                            if let Some(entry) = super::storage::sequenced_op(&store, op)?
                                && entry.position.sequence >= start
                                && entry.position.sequence <= end
                            {
                                retained.push(entry);
                            }
                        }
                        Ok(retained)
                    })
                    .await?;
                retained.sort_by_key(|entry| entry.position.sequence);
                let mut page = Vec::new();
                for (expected, entry) in (start..=end).zip(retained) {
                    if entry.position.sequence != expected {
                        break;
                    }
                    page.push(entry);
                }
                let mut fitting = 0;
                let mut upper = page.len();
                // Try the complete page first; only oversized pages need a
                // logarithmic search, avoiding repeated encoding of large prefixes.
                let mut count = upper;
                while count > fitting {
                    let candidate = HistoryMessage::Replay {
                        request_id: request_id.clone(),
                        origin: origin.clone(),
                        start,
                        end: page[count - 1].position.sequence,
                        ops: page[..count].to_vec(),
                    };
                    match context
                        .node
                        .clone()
                        .prepare_history_to_addr(addr, candidate)
                        .await
                    {
                        Ok(prepared) if prepared.encoded_len() <= REPLAY_BYTES => fitting = count,
                        Ok(_) | Err(NetError::MessageTooLarge { .. }) => upper = count - 1,
                        Err(error) => return Err(error.into()),
                    }
                    count = fitting + (upper - fitting).div_ceil(2);
                }
                page.truncate(fitting);
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
            #[cfg(test)]
            let response_diagnostic = match &response {
                HistoryMessage::Replay { ops, .. } => Some(("range_replay_sent", ops.len() as u64)),
                HistoryMessage::Unavailable { start, end, .. } => Some((
                    "range_unavailable_sent",
                    end.saturating_sub(*start).saturating_add(1),
                )),
                _ => None,
            };
            send_repair_history(&context.node, &context.repair_budget, addr, response, None)
                .await?;
            #[cfg(test)]
            if let Some((event, items)) = response_diagnostic {
                trace_recovery_event(event, items);
            }
        }
        HistoryMessage::Replay {
            request_id,
            origin,
            start,
            end,
            ops,
        } => {
            if !complete_replay_request(addr, &request_id, &origin, start, end, &ops, context)
                .await?
            {
                return Ok(());
            }
            #[cfg(test)]
            let trace = nx_net::HistoryTrace::new("replay", addr, &origin, &request_id, start, end);
            #[cfg(test)]
            trace.emit("replay_apply_begin");
            let accepted =
                apply_sequenced_ops_and_collect(&ops, &history_apply_context(context)).await?;
            #[cfg(test)]
            trace.emit("replay_published");
            finish_replay_after_apply(addr, origin, accepted, context).await?;
        }
        HistoryMessage::Unavailable {
            request_id,
            origin,
            start,
            end,
        } => {
            validate_range(start, end)?;
            let Some(_request) =
                complete_history_request(addr, &origin, &request_id, start, end, context).await
            else {
                debug!(%addr, %origin, %request_id, "ignoring stale or unsolicited unavailable history");
                return Ok(());
            };
            #[cfg(test)]
            {
                trace_recovery_event(
                    "range_unavailable_received",
                    end.saturating_sub(start).saturating_add(1),
                );
                trace_recovery_latency(
                    "range_unavailable",
                    request_age(
                        context.node.now(),
                        _request.deadline,
                        context.history_request_timeout,
                    ),
                    end.saturating_sub(start).saturating_add(1),
                );
            }
            let store = Arc::clone(&context.store);
            let tracked_origin = origin.clone();
            let evidence = super::sequence::UnavailableHistory {
                source: addr.to_owned(),
                start,
                end,
                request_id,
            };
            let (recorded, previous) = context
                .node
                .storage_executor()
                .execute(move || {
                    super::storage::record_unavailable_and_previous(
                        &store,
                        &tracked_origin,
                        evidence,
                    )
                })
                .await?;
            context
                .metrics
                .peers
                .increment(crate::peer_metrics::Counter::HistoryUnavailable);
            warn!(%origin, start, end, "source cannot supply missing history; another source or state baseline is required");
            let mut alternative_requested = false;
            if recorded && previous.is_none() {
                let mut alternatives = context
                    .node
                    .connected_peers()
                    .await
                    .into_iter()
                    .map(|(peer, _)| peer)
                    .filter(|peer| peer != addr)
                    .collect::<Vec<_>>();
                alternatives.sort();
                if let Some(alternative) = alternatives.first() {
                    match request_history_or_baseline_inner(
                        alternative,
                        origin.clone(),
                        start,
                        end,
                        context,
                    )
                    .await
                    {
                        Ok(true) => alternative_requested = true,
                        Ok(false) => {}
                        Err(error) => {
                            context.metrics.record_sync_error();
                            debug!(%alternative, %error, "alternate range request deferred after unavailable replay");
                        }
                    }
                }
            }
            if !alternative_requested {
                if recorded {
                    signal_recovery_rotation(context, addr);
                }
                if let Err(error) = request_baseline(addr, origin, context).await {
                    context.metrics.record_sync_error();
                    debug!(%addr, %error, "baseline request deferred after unavailable replay; periodic repair will retry");
                }
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
                Err(error) => {
                    let Some(rejected) = error.downcast_ref::<super::baseline::Rejected>() else {
                        return Err(error.context("baseline export failed"));
                    };
                    HistoryMessage::BaselineUnavailable {
                        request_id,
                        origin,
                        reason: rejected.0,
                    }
                }
            };
            #[cfg(test)]
            let mut response_event = match &response {
                HistoryMessage::Baseline { .. } => "baseline_sent",
                HistoryMessage::BaselineUnavailable { .. } => "baseline_unavailable_sent",
                _ => "other_recovery_response_sent",
            };
            let correlation = match &response {
                HistoryMessage::Baseline {
                    request_id, origin, ..
                } => Some((request_id.clone(), origin.clone())),
                _ => None,
            };
            match send_repair_history(&context.node, &context.repair_budget, addr, response, None)
                .await
            {
                Err(NetError::MessageTooLarge { .. }) if correlation.is_some() => {
                    if let Some((request_id, origin)) = correlation {
                        send_repair_history(
                            &context.node,
                            &context.repair_budget,
                            addr,
                            HistoryMessage::BaselineUnavailable {
                                request_id,
                                origin,
                                reason: nx_sync::BaselineFailure::BudgetExceeded,
                            },
                            None,
                        )
                        .await?;
                        #[cfg(test)]
                        {
                            response_event = "baseline_unavailable_sent";
                        }
                    }
                }
                other => other?,
            }
            #[cfg(test)]
            trace_recovery_event(response_event, 0);
        }
        HistoryMessage::Baseline {
            request_id,
            origin,
            version,
            payload,
            digest,
        } => {
            let Some(request) =
                complete_history_request(addr, &origin, &request_id, 0, 0, context).await
            else {
                debug!(%addr, "ignoring stale or unsolicited baseline");
                return Ok(());
            };
            #[cfg(test)]
            let response_age = request_age(
                context.node.now(),
                request.deadline,
                context.history_request_timeout,
            );
            if let Err(error) = super::baseline::import(
                version,
                &payload,
                &digest,
                &origin,
                &history_apply_context(context),
            )
            .await
            {
                #[cfg(test)]
                trace_recovery_event(
                    if error.downcast_ref::<super::baseline::Rejected>().is_some() {
                        "baseline_import_rejected"
                    } else {
                        "baseline_import_error"
                    },
                    0,
                );
                if let Some(rejected) = error.downcast_ref::<super::baseline::Rejected>()
                    && let Some(revision) = request.baseline_revision
                {
                    match record_recovery_failure(context, &origin, addr, rejected.0, &revision)
                        .await
                    {
                        Ok(true) => signal_recovery_rotation(context, addr),
                        Ok(false) => {}
                        Err(record_error) => {
                            warn!(%origin, record_error = %format_args!("{record_error:#}"), "could not record baseline rejection");
                        }
                    }
                }
                return Err(error.context("baseline import failed"));
            }
            #[cfg(test)]
            {
                trace_recovery_event("baseline_import_accepted", 0);
                trace_recovery_latency("baseline_accepted", response_age, 0);
            }
            context
                .metrics
                .peers
                .increment(crate::peer_metrics::Counter::BaselineAccepted);
            clear_recovery_rotation(context, addr);
        }
        HistoryMessage::BaselineUnavailable {
            request_id,
            origin,
            reason,
        } => {
            let Some(request) =
                complete_history_request(addr, &origin, &request_id, 0, 0, context).await
            else {
                debug!(%addr, "ignoring stale or unsolicited baseline refusal");
                return Ok(());
            };
            let Some(revision) = request.baseline_revision else {
                return Ok(());
            };
            #[cfg(test)]
            {
                trace_recovery_event("baseline_unavailable_received", 0);
                trace_recovery_latency(
                    "baseline_unavailable",
                    request_age(
                        context.node.now(),
                        request.deadline,
                        context.history_request_timeout,
                    ),
                    0,
                );
            }
            if !record_recovery_failure(context, &origin, addr, reason, &revision).await? {
                #[cfg(test)]
                trace_recovery_event("baseline_refusal_superseded", 0);
                debug!(%origin, %addr, "ignoring baseline refusal superseded by changed receipt coverage");
                return Ok(());
            }
            signal_recovery_rotation(context, addr);
            #[cfg(test)]
            trace_recovery_event("baseline_rejected", 0);
            context.metrics.record_sync_error();
            context
                .metrics
                .peers
                .increment(crate::peer_metrics::Counter::BaselineRejected);
            warn!(%origin, ?reason, "gap unrecoverable from this source under the bounded baseline contract; other sources may still recover it");
        }
    }
    Ok(())
}

async fn record_recovery_failure(
    context: &NodeEventContext,
    origin: &NodeId,
    source: &str,
    reason: nx_sync::BaselineFailure,
    revision: &[u8; 32],
) -> anyhow::Result<bool> {
    let store = Arc::clone(&context.store);
    let origin = origin.clone();
    let source = source.to_owned();
    let revision = *revision;
    context
        .node
        .storage_executor()
        .execute(move || {
            super::storage::record_recovery_failure(&store, &origin, &source, reason, &revision)
        })
        .await
}

fn signal_recovery_rotation(context: &NodeEventContext, source_addr: &str) {
    let Some(sender) = &context.recovery_rotation else {
        return;
    };

    sender.send_modify(|request| {
        request.generation = request.generation.wrapping_add(1);
        request.source_addr = Some(source_addr.to_owned());
    });
}

fn clear_recovery_rotation(context: &NodeEventContext, source_addr: &str) {
    let Some(sender) = &context.recovery_rotation else {
        return;
    };

    sender.send_if_modified(|request| {
        if request.source_addr.as_deref() == Some(source_addr) {
            request.generation = request.generation.wrapping_add(1);
            request.source_addr = None;
            true
        } else {
            false
        }
    });
}

// End of main code. Test below:
#[cfg(test)]
mod tests {
    use super::*;
    use crate::observability::RuntimeMetrics;
    use crate::sync_manager::apply::apply_sequenced_ops;
    use crate::sync_manager::peer::{PeerHealth, PeerHealthState};
    use crate::sync_manager::storage::materialized_gcounter_key;
    use nx_net::{Node, NodeConfig, PROTOCOL_VERSION, WireError};
    use nx_sync::{GCounter, NodeId};
    use std::collections::HashMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn transport_push_packing_matches_real_codecs_without_fixed_margins() {
        let entries = (1..=4)
            .map(|sequence| nx_sync::SequencedOp {
                op: Op::lww_register_set(
                    NodeId::new("origin\\\""),
                    "key\0",
                    vec![255; 128],
                    sequence,
                ),
                position: nx_sync::OpPosition {
                    generation: u64::MAX,
                    sequence,
                },
            })
            .collect::<Vec<_>>();
        let mut sizes = PushFrameSize::default();
        for count in 1..=entries.len() {
            let message = HistoryMessage::Push {
                ops: entries[..count].to_vec(),
            };
            let actual = [
                nx_net::SerializationFormat::Json,
                nx_net::SerializationFormat::Bincode,
            ]
            .map(|format| nx_net::history_frame_len(&message, format).unwrap());
            let limit = actual.into_iter().max().unwrap() - 4;
            let before = sizes.bytes;
            assert!(!sizes.include(&entries[count - 1], limit - 1));
            assert_eq!(sizes.bytes, before);
            assert!(sizes.include(&entries[count - 1], limit));
            assert_eq!(sizes.bytes, actual);
        }
    }

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
    fn gossip_fanout_is_clamped_excludes_sender_and_rotates_neighbors() {
        let peers = (0..5)
            .map(|index| {
                (
                    format!("peer-{index}"),
                    NodeId::new(format!("node-{index}")),
                )
            })
            .collect::<Vec<_>>();
        let cursor = AtomicUsize::new(0);

        let local = NodeId::new("local");
        let first = select_gossip_peers(&local, &peers, 6, None, 0, 3, &cursor);
        let second = select_gossip_peers(&local, &peers, 6, None, 0, 3, &cursor);
        assert_eq!(first.len(), 3);
        assert_eq!(second.len(), 3);
        assert_eq!(first.iter().chain(&second).collect::<HashSet<_>>().len(), 5);
        let mut reversed = peers.clone();
        reversed.reverse();
        assert_eq!(
            first,
            select_gossip_peers(&local, &reversed, 6, None, 0, 3, &AtomicUsize::new(0))
        );
        let orders = (0..16)
            .map(|index| {
                select_gossip_peers(
                    &NodeId::new(format!("local-{index}")),
                    &peers,
                    6,
                    None,
                    0,
                    3,
                    &AtomicUsize::new(0),
                )
            })
            .collect::<HashSet<_>>();
        assert!(orders.len() > 8);

        let excluded =
            select_gossip_peers(&local, &peers, 6, Some("peer-2"), usize::MAX, 8, &cursor);
        assert_eq!(excluded.len(), 4);
        assert!(!excluded.iter().any(|peer| peer == "peer-2"));
    }

    #[test]
    fn gossip_fanout_uses_membership_estimate_and_configured_limits() {
        let peers = (0..16)
            .map(|index| {
                (
                    format!("peer-{index}"),
                    NodeId::new(format!("node-{index}")),
                )
            })
            .collect::<Vec<_>>();
        let records = (0..49)
            .map(|index| nx_net::MembershipRecord {
                member: format!("node-{index}"),
                reporter: format!("node-{index}"),
                version: nx_net::MembershipVersion {
                    epoch: 1,
                    incarnation: 0,
                },
                state: nx_net::MembershipState::Alive,
                endpoint: None,
            })
            .collect::<Vec<_>>();
        let local = NodeId::new("local");
        let size = estimate_gossip_size(&local, &records, &peers);
        assert_eq!(size, 50);
        let cursor = AtomicUsize::new(0);
        assert_eq!(
            select_gossip_peers(&local, &peers, size, None, 1, 64, &cursor).len(),
            7
        );
        assert_eq!(
            select_gossip_peers(&local, &peers, size, None, 1, 3, &cursor).len(),
            3
        );
        assert_eq!(
            select_gossip_peers(&local, &peers, size, None, usize::MAX, 64, &cursor).len(),
            16
        );
        assert!(select_gossip_peers(&local, &[], size, None, 1, 64, &cursor).is_empty());
        assert!(select_gossip_peers(&local, &peers, size, None, 1, 0, &cursor).is_empty());
        for (size, expected) in [
            (0, 0),
            (1, 0),
            (2, 1),
            (3, 2),
            (4, 2),
            (5, 3),
            (8, 3),
            (9, 4),
        ] {
            assert_eq!(
                select_gossip_peers(&local, &peers, size, None, 0, 64, &cursor).len(),
                expected
            );
        }
        assert_eq!(
            select_gossip_peers(&local, &peers, size, Some("peer-0"), 3, 64, &cursor).len(),
            9
        );
        assert_eq!(
            select_gossip_peers(&local, &peers, usize::MAX, None, usize::MAX, 64, &cursor).len(),
            16
        );
    }

    #[test]
    fn gossip_size_deduplicates_identity_and_preserves_active_terminal_contacts() {
        let mut record = nx_net::MembershipRecord {
            member: "peer".into(),
            reporter: "peer".into(),
            version: nx_net::MembershipVersion {
                epoch: 1,
                incarnation: 0,
            },
            state: nx_net::MembershipState::Alive,
            endpoint: None,
        };
        let local = NodeId::new("local");
        assert_eq!(estimate_gossip_size(&local, &[], &[]), 1);
        for state in [
            nx_net::MembershipState::Alive,
            nx_net::MembershipState::Suspect,
            nx_net::MembershipState::Dead,
            nx_net::MembershipState::Left,
        ] {
            record.state = state;
            let expected = if matches!(
                state,
                nx_net::MembershipState::Alive | nx_net::MembershipState::Suspect
            ) {
                2
            } else {
                1
            };
            assert_eq!(
                estimate_gossip_size(&local, &[record.clone()], &[]),
                expected
            );
            let peers = vec![
                ("old-endpoint".into(), NodeId::new("peer")),
                ("new-endpoint".into(), NodeId::new("peer")),
            ];
            record.version.epoch += 1;
            assert_eq!(
                estimate_gossip_size(&local, &[record.clone(), record.clone()], &peers),
                2
            );
        }
        record.member = "local".into();
        record.state = nx_net::MembershipState::Alive;
        assert_eq!(estimate_gossip_size(&local, &[record], &[]), 1);
    }

    #[test]
    #[ignore = "manual reproducible selector calibration, not a transport benchmark"]
    fn gossip_fanout_calibration() {
        let payload = HistoryMessage::Push {
            ops: vec![nx_sync::SequencedOp {
                op: Op::lww_register_set(
                    NodeId::new("calibration-origin"),
                    "calibration-payload",
                    vec![0; 256],
                    1,
                ),
                position: nx_sync::OpPosition {
                    generation: 1,
                    sequence: 1,
                },
            }],
        };
        let payload_bytes = serde_json::to_vec(&payload).unwrap().len();
        fn random(state: &mut u64) -> u64 {
            *state ^= *state << 13;
            *state ^= *state >> 7;
            *state ^= *state << 17;
            *state
        }
        for nodes in [8, 16, 32, 50] {
            for constant in 0..=3 {
                for loss_percent in [0, 10] {
                    for shared_order in [false, true] {
                        let mut totals = [(0usize, 0usize, 0usize, 0usize, 0usize); 2];
                        for seed in 1..=16 {
                            let mut rng = seed;
                            let peers = (0..nodes)
                                .map(|local| {
                                    let mut ranks = (0..nodes).collect::<Vec<_>>();
                                    if !shared_order {
                                        for index in (1..nodes).rev() {
                                            let other = random(&mut rng) as usize % (index + 1);
                                            ranks.swap(index, other);
                                        }
                                    }
                                    (0..nodes)
                                        .filter(|remote| *remote != local)
                                        .map(|remote| {
                                            (
                                                format!("{:04}-{remote}", ranks[remote]),
                                                NodeId::new(format!("node-{remote}")),
                                            )
                                        })
                                        .collect::<Vec<_>>()
                                })
                                .collect::<Vec<_>>();
                            let cursors =
                                (0..nodes).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>();
                            for sample in 0..32 {
                                let origin = random(&mut rng) as usize % nodes;
                                let mut seen = vec![false; nodes];
                                let mut events =
                                    std::collections::VecDeque::from([(origin, None, 0usize)]);
                                let mut reached = 0;
                                let mut messages = 0;
                                let mut duplicates = 0;
                                let mut max_hops = 0;
                                while let Some((local, source, hops)) = events.pop_front() {
                                    if seen[local] {
                                        duplicates += 1;
                                        continue;
                                    }
                                    seen[local] = true;
                                    reached += 1;
                                    max_hops = max_hops.max(hops);
                                    let excluded = source.and_then(|source: usize| {
                                        peers[local]
                                            .iter()
                                            .find(|(_, identity)| {
                                                *identity == NodeId::new(format!("node-{source}"))
                                            })
                                            .map(|(addr, _)| addr.as_str())
                                    });
                                    let identity = NodeId::new(format!("seed-{seed}-node-{local}"));
                                    for addr in select_gossip_peers(
                                        &identity,
                                        &peers[local],
                                        nodes,
                                        excluded,
                                        constant,
                                        64,
                                        &cursors[local],
                                    ) {
                                        messages += 1;
                                        if random(&mut rng) % 100 < loss_percent {
                                            continue;
                                        }
                                        let remote = addr
                                            .split_once('-')
                                            .unwrap()
                                            .1
                                            .parse::<usize>()
                                            .unwrap();
                                        events.push_back((remote, Some(local), hops + 1));
                                    }
                                }
                                let total = &mut totals[usize::from(sample != 0)];
                                total.0 += reached;
                                total.1 += usize::from(reached == nodes);
                                total.2 += messages;
                                total.3 += duplicates;
                                total.4 = total.4.max(max_hops);
                                assert!(messages <= nodes * (nodes - 1));
                            }
                        }
                        for (phase, (reached, complete, messages, duplicates, hops)) in
                            totals.into_iter().enumerate()
                        {
                            let samples = if phase == 0 { 16 } else { 16 * 31 };
                            if constant == crate::sync_config::DEFAULT_GOSSIP_FANOUT_CONSTANT {
                                assert!(reached as f64 / (samples * nodes) as f64 >= 0.999);
                                assert!(complete as f64 / samples as f64 >= 0.99);
                            }
                            println!(
                                "FANOUT_CALIBRATION {}",
                                serde_json::json!({
                                    "schema": 1, "nodes": nodes, "c": constant, "seeds": [1, 16],
                                    "samples": samples, "loss_percent": loss_percent,
                                    "address_order": if shared_order { "shared" } else { "per-node-seeded" },
                                    "phase": if phase == 0 { "cold" } else { "rotating" },
                                    "coverage": reached as f64 / (samples * nodes) as f64,
                                    "complete_fraction": complete as f64 / samples as f64,
                                    "messages_per_op": messages as f64 / samples as f64,
                                    "json_history_bytes_per_op": messages as f64 * payload_bytes as f64 / samples as f64,
                                    "value_bytes": 256, "single_push_payload_bytes": payload_bytes,
                                    "duplicate_deliveries_per_op": duplicates as f64 / samples as f64,
                                    "max_hops": hops, "anti_entropy": false,
                                })
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn anti_entropy_deadlines_are_checked_and_skip_missed_ticks_without_bursts() {
        let now = StdInstant::now();
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
            store: temp_store(),
            jitter_seed: 1,
            repair_budget: Arc::new(std::sync::Mutex::new(
                super::super::delivery::RepairBudget::new(StdInstant::now()),
            )),
            history_cursors: Arc::new(RwLock::new(HashMap::new())),
            request_timeout: Duration::from_secs(30),
            node: Arc::new(
                Node::try_new(NodeConfig::new(NodeId::generate(), "127.0.0.1:0")).unwrap(),
            ),
            interval: Duration::MAX,
            shutdown_rx,
            metrics: metrics(),
            membership: None,
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
        let (_membership_tx, membership) =
            watch::channel(crate::membership::runtime::Snapshot::default());
        let task = spawn_reconnect_loop(ReconnectLoopContext {
            local_id: NodeId::new("overflow"),
            membership,
            rotation_interval: super::super::neighbors::NEIGHBOR_ROTATION_INTERVAL,
            recovery_rotation: None,
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

        reconcile_reconnect_candidates(
            &mut state,
            &[],
            Duration::from_millis(10),
            &peer_health,
            now,
        )
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
            now,
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
            repair_budget: Arc::new(std::sync::Mutex::new(
                super::super::delivery::RepairBudget::new(StdInstant::now()),
            )),
            recovery_rotation: None,
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
            forward_tx: None,
            peer_health,
            active_connections: Arc::new(RwLock::new(HashMap::new())),
            anti_entropy_watermarks: Arc::new(RwLock::new(HashMap::new())),
            peer_dead_after_failures: 2,
        }
    }

    fn baseline_test_context() -> NodeEventContext {
        test_event_context(
            Arc::new(RwLock::new(HashMap::new())),
            Arc::new(RwLock::new(SeenOps::new(16))),
            Arc::new(RwLock::new(Vec::new())),
            temp_store(),
            metrics(),
            Arc::new(RwLock::new(HashMap::new())),
        )
    }

    fn queued_entries() -> Vec<nx_sync::SequencedOp> {
        queued_entries_for("queued-origin")
    }

    fn queued_entries_for(name: &str) -> Vec<nx_sync::SequencedOp> {
        let origin = NodeId::new(name);
        [
            Op::gcounter_increment(origin.clone(), "det-g", 7),
            Op::pncounter_increment(origin.clone(), "det-p", 9),
            Op::pncounter_decrement(origin.clone(), "det-p", 4),
            Op::lww_register_set(origin.clone(), "det-r", b"winner".to_vec(), 200),
            Op::lww_map_set(origin.clone(), "det-m", "field", b"removed".to_vec(), 100),
            Op::lww_map_remove(origin.clone(), "det-m", "field", 200),
            Op::orset_add(origin.clone(), "det-o", "removed", format!("{name}/tag")),
            Op::orset_remove(
                origin.clone(),
                "det-o",
                "removed",
                vec![format!("{name}/tag")],
            ),
            Op::orset_add(
                origin.clone(),
                "det-o",
                "present",
                format!("{name}/present-tag"),
            ),
            Op::rga_insert(
                origin.clone(),
                "det-a",
                format!("{name}/removed-id"),
                None::<String>,
                b"removed".to_vec(),
            ),
            Op::rga_delete(origin.clone(), "det-a", format!("{name}/removed-id")),
            Op::rga_insert(
                origin,
                "det-a",
                format!("{name}/present-id"),
                None::<String>,
                b"present".to_vec(),
            ),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, op)| nx_sync::SequencedOp {
            op,
            position: nx_sync::OpPosition {
                generation: 1,
                sequence: index as u64 + 1,
            },
        })
        .collect()
    }

    fn queued_event(ops: Vec<nx_sync::SequencedOp>, index: usize) -> NodeEvent {
        NodeEvent::HistoryReceived {
            from: NodeId::new(format!("source-{}", index % 2)),
            addr: format!("source-{}", index % 2),
            message: HistoryMessage::Push { ops },
        }
    }

    fn queued_replay_event(
        ops: Vec<nx_sync::SequencedOp>,
        request_id: nx_sync::OpId,
        source: &str,
    ) -> NodeEvent {
        let origin = ops
            .first()
            .map(|entry| entry.op.origin.clone())
            .unwrap_or_else(|| NodeId::new("empty-replay"));
        let start = ops.first().map_or(1, |entry| entry.position.sequence);
        let end = ops.last().map_or(0, |entry| entry.position.sequence);
        NodeEvent::HistoryReceived {
            from: NodeId::new(source),
            addr: source.to_owned(),
            message: HistoryMessage::Replay {
                request_id,
                origin,
                start,
                end,
                ops,
            },
        }
    }

    async fn queued_memory(context: &NodeEventContext) -> Vec<serde_json::Value> {
        [
            context.counters.read().await["det-g"].to_json().unwrap(),
            context.pncounters.read().await["det-p"].to_json().unwrap(),
            context.lww_registers.read().await["det-r"]
                .to_json()
                .unwrap(),
            context.lww_maps.read().await["det-m"].to_json().unwrap(),
            context.orsets.read().await["det-o"].to_json().unwrap(),
            context.rgas.read().await["det-a"].to_json().unwrap(),
        ]
        .into_iter()
        .map(|state| serde_json::from_str(&state).unwrap())
        .collect()
    }

    #[tokio::test]
    async fn queued_pushes_share_one_batch_and_preserve_every_family_and_forward_source() {
        let mut context = baseline_test_context();
        let reference = baseline_test_context();
        let entries = queued_entries();
        let (forward_tx, mut forwarded) = super::super::delivery::named_channel(
            64,
            super::super::delivery::QUEUE_BYTES,
            context.metrics.clone(),
            true,
        );
        context.forward_tx = Some(forward_tx);
        let (sender, mut events) = mpsc::channel(64);
        for (index, entry) in entries.iter().enumerate() {
            sender
                .try_send(queued_event(vec![entry.clone()], index))
                .unwrap();
            handle_node_event(queued_event(vec![entry.clone()], index), &reference).await;
        }
        let before = context.store.completed_batch_work().0;
        drain_node_events(&mut events, &context).await;
        assert_eq!(context.store.completed_batch_work().0 - before, 1);
        assert_eq!(
            reference.store.completed_batch_work().0,
            entries.len() as u64
        );
        assert_eq!(
            context.store.scan_prefix(b"__nx/").unwrap(),
            reference.store.scan_prefix(b"__nx/").unwrap()
        );
        assert_eq!(
            queued_memory(&context).await,
            queued_memory(&reference).await
        );
        assert_eq!(context.anti_entropy_watermarks.read().await.len(), 2);
        for (index, entry) in entries.iter().enumerate() {
            let actual = forwarded.try_recv().unwrap();
            assert_eq!(actual.entry, *entry);
            assert_eq!(actual.source_addr, format!("source-{}", index % 2));
            sender
                .try_send(queued_event(vec![entry.clone(), entry.clone()], index))
                .unwrap();
        }
        drain_node_events(&mut events, &context).await;
        assert_eq!(context.store.completed_batch_work().0 - before, 1);
        assert!(forwarded.try_recv().is_err());
        assert_eq!(
            queued_memory(&context).await,
            queued_memory(&reference).await
        );
    }

    #[tokio::test]
    async fn queued_benign_copies_preserve_six_families_and_first_forward_owner() {
        for persisted_prefix in [false, true] {
            let mut context = baseline_test_context();
            let reference = baseline_test_context();
            let entries = queued_entries();
            let first = usize::from(persisted_prefix);
            if persisted_prefix {
                for target in [&context, &reference] {
                    handle_node_event(queued_event(vec![entries[0].clone()], 0), target).await;
                }
            }
            let (forward_tx, mut forwarded) = super::super::delivery::named_channel(
                64,
                super::super::delivery::QUEUE_BYTES,
                context.metrics.clone(),
                true,
            );
            context.forward_tx = Some(forward_tx);
            let before_batches = context.store.completed_batch_work().0;
            let before_flushes = context.store.completed_flushes();
            let (sender, mut events) = mpsc::channel(64);
            for (index, entry) in entries.iter().enumerate().skip(first) {
                let ops = vec![entry.clone(), entry.clone(), entries[0].clone()];
                sender.try_send(queued_event(ops.clone(), index)).unwrap();
                handle_node_event(queued_event(ops, index), &reference).await;
            }

            drain_node_events(&mut events, &context).await;

            assert_eq!(context.store.completed_batch_work().0 - before_batches, 1);
            assert_eq!(context.store.completed_flushes() - before_flushes, 1);
            assert_eq!(
                queued_memory(&context).await,
                queued_memory(&reference).await
            );
            assert_eq!(
                context.store.scan_prefix(b"__nx/").unwrap(),
                reference.store.scan_prefix(b"__nx/").unwrap()
            );
            for (index, entry) in entries.iter().enumerate().skip(first) {
                let actual = forwarded.try_recv().unwrap();
                assert_eq!(actual.entry, *entry);
                assert_eq!(actual.source_addr, format!("source-{}", index % 2));
            }
            assert!(forwarded.try_recv().is_err());

            let before_duplicates = context.store.completed_flushes();
            for (index, entry) in entries.iter().enumerate() {
                sender
                    .try_send(queued_event(vec![entry.clone(), entry.clone()], index))
                    .unwrap();
            }
            drain_node_events(&mut events, &context).await;
            assert_eq!(context.store.completed_flushes(), before_duplicates);
            assert!(forwarded.try_recv().is_err());
            assert_eq!(
                queued_memory(&context).await,
                queued_memory(&reference).await
            );
        }
    }

    #[tokio::test]
    async fn queued_conflicting_copies_do_not_discard_valid_neighbors() {
        for conflict in ["payload", "origin", "generation", "sequence", "position"] {
            let mut context = baseline_test_context();
            let mut reference = baseline_test_context();
            let entries = queued_entries();
            let mut invalid = entries[0].clone();
            match conflict {
                "payload" => {
                    invalid.op.kind = nx_sync::OpKind::GCounterIncrement {
                        key: "det-g".into(),
                        increment: 8,
                    };
                }
                "origin" => invalid.op.origin = NodeId::new("conflicting-origin"),
                "generation" => invalid.position.generation = 2,
                "sequence" => invalid.position.sequence = 2,
                "position" => invalid.op.id = nx_sync::OpId::new("conflicting-position"),
                _ => unreachable!(),
            }
            let (forward_tx, mut forwarded) = super::super::delivery::named_channel(
                64,
                super::super::delivery::QUEUE_BYTES,
                context.metrics.clone(),
                true,
            );
            context.forward_tx = Some(forward_tx);
            let (reference_tx, mut reference_forwarded) = super::super::delivery::named_channel(
                64,
                super::super::delivery::QUEUE_BYTES,
                reference.metrics.clone(),
                true,
            );
            reference.forward_tx = Some(reference_tx);
            let (sender, mut events) = mpsc::channel(8);
            for (index, ops) in [
                vec![entries[0].clone()],
                vec![invalid, entries[1].clone()],
                vec![entries[1].clone()],
            ]
            .into_iter()
            .enumerate()
            {
                sender.try_send(queued_event(ops.clone(), index)).unwrap();
                handle_node_event(queued_event(ops, index), &reference).await;
            }

            drain_node_events(&mut events, &context).await;

            assert_eq!(
                context.store.scan_prefix(b"__nx/").unwrap(),
                reference.store.scan_prefix(b"__nx/").unwrap(),
                "{conflict}"
            );
            assert_eq!(context.store.completed_batch_work().0, 2, "{conflict}");
            assert_eq!(context.store.completed_flushes(), 2, "{conflict}");
            assert_eq!(context.counters.read().await["det-g"].value(), 7);
            assert_eq!(context.pncounters.read().await["det-p"].value(), 9);
            for entry in &entries[..2] {
                let actual = forwarded.try_recv().unwrap();
                let expected = reference_forwarded.try_recv().unwrap();
                assert_eq!(actual.entry, *entry);
                assert_eq!(actual.entry, expected.entry, "{conflict}");
                assert_eq!(actual.source_addr, expected.source_addr, "{conflict}");
            }
            assert!(forwarded.try_recv().is_err());
            assert!(reference_forwarded.try_recv().is_err());
            assert_eq!(
                context
                    .metrics
                    .render_for_test(&context.store)
                    .contains("numax_sync_errors_total 1\n"),
                conflict != "position",
                "{conflict}"
            );
        }
    }

    #[tokio::test]
    async fn queued_pushes_wait_for_flush_before_forwarding() {
        let mut context = baseline_test_context();
        let (forward_tx, mut forwarded) = super::super::delivery::named_channel(
            64,
            super::super::delivery::QUEUE_BYTES,
            context.metrics.clone(),
            true,
        );
        context.forward_tx = Some(forward_tx);
        let context = Arc::new(context);
        let entries = queued_entries();
        let (sender, mut events) = mpsc::channel(64);
        for (index, entry) in entries.iter().enumerate() {
            sender
                .try_send(queued_event(vec![entry.clone(), entry.clone()], index))
                .unwrap();
        }
        let (reached_tx, reached_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        context.store.pause_next_flush(reached_tx, resume_rx);
        let worker = Arc::clone(&context);
        let processing = tokio::spawn(async move { drain_node_events(&mut events, &worker).await });
        tokio::task::spawn_blocking(move || reached_rx.recv_timeout(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap();
        assert!(!processing.is_finished());
        assert!(forwarded.try_recv().is_err());
        assert!(context.counters.try_read().is_err());
        assert!(context.anti_entropy_watermarks.read().await.is_empty());
        resume_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), processing)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(context.store.completed_flushes(), 1);
        assert_eq!(context.store.completed_batch_work().0, 1);
        for (index, entry) in entries.iter().enumerate() {
            let actual = forwarded.try_recv().unwrap();
            assert_eq!(actual.entry, *entry);
            assert_eq!(actual.source_addr, format!("source-{}", index % 2));
        }
        assert!(forwarded.try_recv().is_err());
    }

    #[tokio::test]
    async fn queued_flush_failure_does_not_publish_forward_or_retry() {
        let mut context = baseline_test_context();
        let (forward_tx, mut forwarded) = super::super::delivery::named_channel(
            64,
            super::super::delivery::QUEUE_BYTES,
            context.metrics.clone(),
            true,
        );
        context.forward_tx = Some(forward_tx);
        let (sender, mut events) = mpsc::channel(64);
        for (index, entry) in queued_entries().into_iter().enumerate() {
            sender
                .try_send(queued_event(vec![entry.clone(), entry], index))
                .unwrap();
        }
        context.store.inject_disk_full_on_flush(true);
        drain_node_events(&mut events, &context).await;
        assert_eq!(context.store.completed_batch_work().0, 1);
        assert_eq!(context.store.completed_flushes(), 0);
        assert!(context.counters.read().await.is_empty());
        assert!(context.pncounters.read().await.is_empty());
        assert!(context.lww_registers.read().await.is_empty());
        assert!(context.lww_maps.read().await.is_empty());
        assert!(context.orsets.read().await.is_empty());
        assert!(context.rgas.read().await.is_empty());
        assert!(context.seen_ops.read().await.ids.is_empty());
        assert!(context.op_log.read().await.is_empty());
        assert!(context.anti_entropy_watermarks.read().await.is_empty());
        assert!(forwarded.try_recv().is_err());
    }

    #[tokio::test]
    async fn queued_invalid_envelope_does_not_discard_valid_neighbors() {
        let context = baseline_test_context();
        let reference = baseline_test_context();
        let entries = queued_entries();
        let mut invalid = entries[1].clone();
        invalid.position.generation = 0;
        let (sender, mut events) = mpsc::channel(8);
        for (index, entry) in [entries[0].clone(), invalid, entries[1].clone()]
            .into_iter()
            .enumerate()
        {
            sender.try_send(queued_event(vec![entry], index)).unwrap();
        }
        drain_node_events(&mut events, &context).await;
        for (index, entry) in entries[..2].iter().enumerate() {
            handle_node_event(queued_event(vec![entry.clone()], index), &reference).await;
        }
        assert_eq!(
            context.store.scan_prefix(b"__nx/").unwrap(),
            reference.store.scan_prefix(b"__nx/").unwrap()
        );
        assert_eq!(context.store.completed_batch_work().0, 2);
        assert_eq!(context.counters.read().await["det-g"].value(), 7);
        assert_eq!(context.pncounters.read().await["det-p"].value(), 9);
    }

    #[tokio::test]
    async fn queued_interleaved_origins_match_individual_six_family_application() {
        let context = baseline_test_context();
        let reference = baseline_test_context();
        let (sender, mut events) = mpsc::channel(64);
        for (left, right) in queued_entries_for("writer-a")
            .into_iter()
            .zip(queued_entries_for("writer-b"))
        {
            for (index, entry) in [left, right].into_iter().enumerate() {
                sender
                    .try_send(queued_event(vec![entry.clone(), entry.clone()], index))
                    .unwrap();
                handle_node_event(queued_event(vec![entry], index), &reference).await;
            }
        }
        drain_node_events(&mut events, &context).await;
        assert_eq!(context.store.completed_batch_work().0, 1);
        assert_eq!(
            queued_memory(&context).await,
            queued_memory(&reference).await
        );
        for key in [
            super::super::storage::durable_gcounter_state_key("det-g"),
            super::super::storage::durable_pncounter_state_key("det-p"),
            super::super::storage::durable_lww_register_state_key("det-r"),
            super::super::storage::durable_lww_map_state_key("det-m"),
            super::super::storage::durable_orset_state_key("det-o"),
            super::super::storage::durable_rga_state_key("det-a"),
        ] {
            let actual: serde_json::Value =
                serde_json::from_slice(&context.store.get(&key).unwrap().unwrap()).unwrap();
            let expected: serde_json::Value =
                serde_json::from_slice(&reference.store.get(&key).unwrap().unwrap()).unwrap();
            assert_eq!(actual, expected);
        }
        assert_eq!(
            super::super::storage::replication_progress(&context.store).unwrap(),
            super::super::storage::replication_progress(&reference.store).unwrap()
        );
    }

    #[tokio::test]
    async fn queued_mixed_history_matches_individual_six_family_application() {
        let context = baseline_test_context();
        let reference = baseline_test_context();
        let push_entries = queued_entries_for("mixed-writer-a");
        let replay_entries = queued_entries_for("mixed-writer-b");
        let replay_origin = NodeId::new("mixed-writer-b");
        let request_id = nx_sync::OpId::generate();
        let replay_source = "mixed-history-source";
        arm_replay_request(&context, replay_source, &replay_origin, &request_id).await;
        arm_replay_request(&reference, replay_source, &replay_origin, &request_id).await;
        let (sender, mut events) = mpsc::channel(8);
        sender
            .try_send(queued_event(push_entries.clone(), 0))
            .unwrap();
        sender
            .try_send(queued_replay_event(
                replay_entries.clone(),
                request_id.clone(),
                replay_source,
            ))
            .unwrap();
        handle_node_event(queued_event(push_entries, 0), &reference).await;
        handle_node_event(
            queued_replay_event(replay_entries, request_id, replay_source),
            &reference,
        )
        .await;

        drain_node_events(&mut events, &context).await;

        assert_eq!(context.store.completed_batch_work().0, 1);
        assert_eq!(
            queued_memory(&context).await,
            queued_memory(&reference).await
        );
        assert_eq!(
            context.store.scan_prefix(b"__nx/").unwrap(),
            reference.store.scan_prefix(b"__nx/").unwrap()
        );
        assert_eq!(
            super::super::storage::replication_progress(&context.store).unwrap(),
            super::super::storage::replication_progress(&reference.store).unwrap()
        );
    }

    #[tokio::test]
    async fn queued_storage_failure_does_not_publish_or_forward_or_retry() {
        let mut context = baseline_test_context();
        let (forward_tx, mut forwarded) = super::super::delivery::named_channel(
            64,
            super::super::delivery::QUEUE_BYTES,
            context.metrics.clone(),
            true,
        );
        context.forward_tx = Some(forward_tx);
        let (sender, mut events) = mpsc::channel(8);
        let entries = queued_entries();
        let before = context.store.scan_prefix(b"__nx/").unwrap();
        context.store.inject_disk_full_on_writes(true);
        for (index, entry) in entries[..2].iter().enumerate() {
            sender
                .try_send(queued_event(vec![entry.clone()], index))
                .unwrap();
        }
        drain_node_events(&mut events, &context).await;
        assert_eq!(context.store.scan_prefix(b"__nx/").unwrap(), before);
        assert_eq!(context.store.completed_batch_work().0, 0);
        assert!(context.counters.read().await.is_empty());
        assert!(context.pncounters.read().await.is_empty());
        assert!(context.seen_ops.read().await.ids.is_empty());
        assert!(context.op_log.read().await.is_empty());
        assert!(context.anti_entropy_watermarks.read().await.is_empty());
        assert!(forwarded.try_recv().is_err());
        assert!(
            context
                .metrics
                .render_for_test(&context.store)
                .contains("numax_remote_op_apply_errors_total 1\n")
        );
        context.store.inject_disk_full_on_writes(false);
    }

    #[tokio::test]
    async fn queued_pushes_and_replays_share_one_flush_and_publish_only_afterward() {
        let mut context = baseline_test_context();
        let (forward_tx, mut forwarded) = super::super::delivery::named_channel(
            64,
            super::super::delivery::QUEUE_BYTES,
            context.metrics.clone(),
            true,
        );
        context.forward_tx = Some(forward_tx);
        let mut entries = Vec::new();
        for (origin_name, source) in [
            ("replay-origin-a", "replay-source-a:1234"),
            ("replay-origin-b", "replay-source-b:1234"),
        ] {
            let origin = NodeId::new(origin_name);
            let request_id = nx_sync::OpId::generate();
            let entry = nx_sync::SequencedOp {
                op: Op::gcounter_increment(origin.clone(), "replay-batch", 1),
                position: nx_sync::OpPosition {
                    generation: 1,
                    sequence: 1,
                },
            };
            arm_replay_request(&context, source, &origin, &request_id).await;
            entries.push((entry, request_id, source));
        }

        let before_batches = context.store.completed_batch_work().0;
        let before_flushes = context.store.completed_flushes();
        let (sender, mut events) = mpsc::channel(16);
        let push_origin = NodeId::new("replay-origin-c");
        let push_entry = nx_sync::SequencedOp {
            op: Op::gcounter_increment(push_origin, "replay-batch", 1),
            position: nx_sync::OpPosition {
                generation: 1,
                sequence: 1,
            },
        };
        for (index, (entry, request_id, source)) in entries.iter().enumerate() {
            sender
                .try_send(queued_replay_event(
                    vec![entry.clone()],
                    request_id.clone(),
                    source,
                ))
                .unwrap();
            if index == 0 {
                sender
                    .try_send(queued_event(vec![push_entry.clone()], 2))
                    .unwrap();
            }
        }

        let (reached_tx, reached_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        context.store.pause_next_flush(reached_tx, resume_rx);
        let context = Arc::new(context);
        let worker = Arc::clone(&context);
        let processing = tokio::spawn(async move { drain_node_events(&mut events, &worker).await });
        tokio::task::spawn_blocking(move || reached_rx.recv_timeout(Duration::from_secs(5)))
            .await
            .unwrap()
            .unwrap();

        assert!(!processing.is_finished());
        assert!(forwarded.try_recv().is_err());
        assert!(context.counters.try_read().is_err());
        assert!(context.anti_entropy_watermarks.read().await.is_empty());
        resume_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), processing)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(context.store.completed_batch_work().0 - before_batches, 1);
        assert_eq!(context.store.completed_flushes() - before_flushes, 1);
        assert_eq!(context.counters.read().await["replay-batch"].value(), 3);
        let pending_requests = context.history_requests.read().await;
        for (_, request_id, _) in &entries {
            assert!(!pending_requests.values().any(|requests| {
                requests
                    .values()
                    .any(|request| &request.request_id == request_id)
            }));
        }
        drop(pending_requests);
        let mut forwarded_items = Vec::new();
        for _ in 0..=entries.len() {
            let item = forwarded.try_recv().unwrap();
            forwarded_items.push((item.entry.op.origin.as_str().to_owned(), item.source_addr));
        }
        forwarded_items.sort();
        assert_eq!(
            forwarded_items,
            vec![
                (
                    "replay-origin-a".to_owned(),
                    "replay-source-a:1234".to_owned(),
                ),
                (
                    "replay-origin-b".to_owned(),
                    "replay-source-b:1234".to_owned(),
                ),
                ("replay-origin-c".to_owned(), "source-0".to_owned()),
            ]
        );
        assert!(forwarded.try_recv().is_err());
    }

    #[tokio::test]
    async fn queued_mixed_history_ignores_stale_replay_and_applies_valid_push() {
        let mut context = baseline_test_context();
        let (forward_tx, mut forwarded) = super::super::delivery::named_channel(
            64,
            super::super::delivery::QUEUE_BYTES,
            context.metrics.clone(),
            true,
        );
        context.forward_tx = Some(forward_tx);
        let stale_origin = NodeId::new("mixed-stale-origin");
        let stale_entry = nx_sync::SequencedOp {
            op: Op::gcounter_increment(stale_origin, "stale-replay", 1),
            position: nx_sync::OpPosition {
                generation: 1,
                sequence: 1,
            },
        };
        let push_origin = NodeId::new("mixed-valid-origin");
        let push_entry = nx_sync::SequencedOp {
            op: Op::gcounter_increment(push_origin, "valid-push", 1),
            position: nx_sync::OpPosition {
                generation: 1,
                sequence: 1,
            },
        };
        let (sender, mut events) = mpsc::channel(8);
        sender
            .try_send(queued_replay_event(
                vec![stale_entry],
                nx_sync::OpId::generate(),
                "stale-replay-source",
            ))
            .unwrap();
        sender
            .try_send(queued_event(vec![push_entry.clone()], 0))
            .unwrap();

        drain_node_events(&mut events, &context).await;

        assert_eq!(context.store.completed_flushes(), 1);
        assert_eq!(context.counters.read().await["valid-push"].value(), 1);
        assert!(!context.counters.read().await.contains_key("stale-replay"));
        let item = forwarded.try_recv().unwrap();
        assert_eq!(item.entry, push_entry);
        assert_eq!(item.source_addr, "source-0");
        assert!(forwarded.try_recv().is_err());
    }

    #[tokio::test]
    async fn queued_push_summary_batches_keep_each_repair_source() {
        let context = baseline_test_context();
        let incoming = [
            ("push-summary-a", "source-0"),
            ("push-summary-b", "source-0"),
            ("push-summary-c", "source-1"),
        ]
        .into_iter()
        .map(|(origin, addr)| {
            IncomingHistory::Push(IncomingPush {
                from: NodeId::new(addr),
                addr: addr.to_owned(),
                ops: vec![nx_sync::SequencedOp {
                    op: Op::gcounter_increment(NodeId::new(origin), "push-summary-batch", 1),
                    position: nx_sync::OpPosition {
                        generation: 1,
                        sequence: 2,
                    },
                }],
            })
        })
        .collect::<Vec<_>>();

        let groups = group_push_summaries(&incoming).unwrap();
        let (repairs, failed_sources) = record_grouped_push_summaries(groups, &context).await;

        assert!(failed_sources.is_empty());
        assert_eq!(repairs["source-0"].len(), 2);
        assert_eq!(repairs["source-1"].len(), 1);
        for origin in ["push-summary-a", "push-summary-b"] {
            assert_eq!(repairs["source-0"][origin], Some((1, 2)));
        }
        assert_eq!(repairs["source-1"]["push-summary-c"], Some((1, 2)));
    }

    #[tokio::test]
    async fn shutdown_drain_processes_priority_and_data_event_queues() {
        let context = baseline_test_context();
        let (data_tx, mut event_rx) = mpsc::channel(PRIORITY_EVENT_BURST + 1);
        let (priority_tx, priority_rx) = mpsc::channel(PRIORITY_EVENT_BURST + 1);
        data_tx
            .send(NodeEvent::HistoryReceived {
                from: NodeId::new("data-peer"),
                addr: "data-peer".to_owned(),
                message: HistoryMessage::Push { ops: Vec::new() },
            })
            .await
            .unwrap();
        for _ in 0..=PRIORITY_EVENT_BURST {
            priority_tx
                .send(NodeEvent::HistoryReceived {
                    from: NodeId::new("priority-peer"),
                    addr: "priority-peer".to_owned(),
                    message: HistoryMessage::Summary {
                        entries: Vec::new(),
                    },
                })
                .await
                .unwrap();
        }

        let mut priority_event_rx = Some(priority_rx);
        drain_node_event_queues(&mut event_rx, &mut priority_event_rx, &context).await;

        assert!(event_rx.try_recv().is_err());
        assert!(priority_event_rx.as_mut().unwrap().try_recv().is_err());
    }

    #[tokio::test]
    async fn queued_mixed_history_storage_failure_does_not_publish_or_forward() {
        let mut context = baseline_test_context();
        let (forward_tx, mut forwarded) = super::super::delivery::named_channel(
            64,
            super::super::delivery::QUEUE_BYTES,
            context.metrics.clone(),
            true,
        );
        context.forward_tx = Some(forward_tx);
        let source = "replay-source:1234";
        let mut replay_events = Vec::new();
        let mut original_request_ids = Vec::new();
        for origin_name in ["replay-failed-a", "replay-failed-b"] {
            let origin = NodeId::new(origin_name);
            let request_id = nx_sync::OpId::generate();
            let entry = nx_sync::SequencedOp {
                op: Op::gcounter_increment(origin.clone(), "failed-replay-batch", 1),
                position: nx_sync::OpPosition {
                    generation: 1,
                    sequence: 1,
                },
            };
            arm_replay_request(&context, source, &origin, &request_id).await;
            original_request_ids.push(request_id.clone());
            replay_events.push(queued_replay_event(vec![entry], request_id, source));
        }
        let push_origin = NodeId::new("queued-push-failed");
        replay_events.insert(
            1,
            queued_event(
                vec![nx_sync::SequencedOp {
                    op: Op::gcounter_increment(push_origin, "failed-replay-batch", 1),
                    position: nx_sync::OpPosition {
                        generation: 1,
                        sequence: 1,
                    },
                }],
                0,
            ),
        );
        let before = context.store.scan_prefix(b"__nx/").unwrap();
        context.store.inject_disk_full_on_writes(true);
        let (sender, mut events) = mpsc::channel(16);
        for event in replay_events {
            sender.try_send(event).unwrap();
        }
        drain_node_events(&mut events, &context).await;

        assert_eq!(context.store.scan_prefix(b"__nx/").unwrap(), before);
        assert!(context.counters.read().await.is_empty());
        assert!(context.seen_ops.read().await.ids.is_empty());
        assert!(context.op_log.read().await.is_empty());
        let pending_requests = context.history_requests.read().await;
        for request_id in &original_request_ids {
            assert!(!pending_requests.values().any(|requests| {
                requests
                    .values()
                    .any(|request| &request.request_id == request_id)
            }));
        }
        drop(pending_requests);
        assert!(context.anti_entropy_watermarks.read().await.is_empty());
        assert!(forwarded.try_recv().is_err());
        assert!(
            context
                .metrics
                .render_for_test(&context.store)
                .contains("numax_remote_op_apply_errors_total 1\n")
        );
        context.store.inject_disk_full_on_writes(false);
    }

    #[tokio::test]
    async fn queued_mixed_history_duplicate_ids_commit_once() {
        let mut context = baseline_test_context();
        let (forward_tx, mut forwarded) = super::super::delivery::named_channel(
            64,
            super::super::delivery::QUEUE_BYTES,
            context.metrics.clone(),
            true,
        );
        context.forward_tx = Some(forward_tx);
        let origin = NodeId::new("mixed-duplicate-origin");
        let entry = nx_sync::SequencedOp {
            op: Op::gcounter_increment(origin.clone(), "mixed-duplicate", 1),
            position: nx_sync::OpPosition {
                generation: 1,
                sequence: 1,
            },
        };
        let request_id = nx_sync::OpId::generate();
        let replay_source = "mixed-duplicate-replay-source";
        arm_replay_request(&context, replay_source, &origin, &request_id).await;
        let (sender, mut events) = mpsc::channel(8);
        sender
            .try_send(queued_event(vec![entry.clone()], 0))
            .unwrap();
        sender
            .try_send(queued_replay_event(
                vec![entry.clone()],
                request_id.clone(),
                replay_source,
            ))
            .unwrap();
        let before_flushes = context.store.completed_flushes();

        drain_node_events(&mut events, &context).await;

        assert_eq!(context.store.completed_flushes() - before_flushes, 1);
        assert_eq!(context.counters.read().await["mixed-duplicate"].value(), 1);
        let item = forwarded.try_recv().unwrap();
        assert_eq!(item.entry, entry);
        assert_eq!(item.source_addr, "source-0");
        assert!(forwarded.try_recv().is_err());
        assert!(
            !context
                .history_requests
                .read()
                .await
                .values()
                .flat_map(|requests| requests.values())
                .any(|request| request.request_id == request_id)
        );
    }

    #[tokio::test]
    async fn queued_replays_with_duplicate_ids_keep_individual_commit_order() {
        let mut context = baseline_test_context();
        let (forward_tx, mut forwarded) = super::super::delivery::named_channel(
            64,
            super::super::delivery::QUEUE_BYTES,
            context.metrics.clone(),
            true,
        );
        context.forward_tx = Some(forward_tx);
        let source = "replay-source:1234";
        let mut events = Vec::new();
        for origin_name in ["replay-duplicate-a", "replay-duplicate-b"] {
            let origin = NodeId::new(origin_name);
            let request_id = nx_sync::OpId::generate();
            let mut op = Op::gcounter_increment(origin.clone(), "replay-duplicate", 1);
            op.id = nx_sync::OpId::new("replay-shared-id");
            let entry = nx_sync::SequencedOp {
                op,
                position: nx_sync::OpPosition {
                    generation: 1,
                    sequence: 1,
                },
            };
            arm_replay_request(&context, source, &origin, &request_id).await;
            events.push(queued_replay_event(vec![entry], request_id, source));
        }
        let before_flushes = context.store.completed_flushes();
        let (sender, mut event_rx) = mpsc::channel(16);
        for event in events {
            sender.try_send(event).unwrap();
        }
        drain_node_events(&mut event_rx, &context).await;

        assert_eq!(context.store.completed_flushes() - before_flushes, 1);
        assert_eq!(context.counters.read().await["replay-duplicate"].value(), 1);
        let item = forwarded.try_recv().unwrap();
        assert_eq!(item.source_addr, source);
        assert_eq!(item.entry.op.id.as_str(), "replay-shared-id");
        assert!(forwarded.try_recv().is_err());
        assert!(
            context
                .metrics
                .render_for_test(&context.store)
                .contains("numax_sync_errors_total 1\n")
        );
    }

    async fn collect_remote_history_without_coalescing(
        first: NodeEvent,
        event_rx: &mut mpsc::Receiver<NodeEvent>,
    ) -> (Vec<IncomingHistory>, Option<NodeEvent>) {
        let mut priority_event_rx = None;
        let (history, pending, priority) =
            collect_remote_history(first, event_rx, &mut priority_event_rx, false, None).await;
        assert!(priority.is_none());
        (history, pending)
    }

    #[tokio::test]
    async fn remote_history_collection_preserves_sources_and_combined_work_budgets() {
        let entry = queued_entries_for("replay-collector").remove(0);
        let (sender, mut events) = mpsc::channel(128);
        for _ in 0..=super::super::delivery::TURN_MESSAGES {
            sender
                .try_send(queued_replay_event(
                    vec![entry.clone()],
                    nx_sync::OpId::generate(),
                    "replay-source",
                ))
                .unwrap();
        }
        let first = events.try_recv().unwrap();
        let (history, pending) =
            collect_remote_history_without_coalescing(first, &mut events).await;
        assert_eq!(history.len(), super::super::delivery::TURN_MESSAGES);
        assert!(
            history
                .iter()
                .all(|envelope| matches!(envelope, IncomingHistory::Replay(_)))
        );
        assert!(pending.is_none());
        assert_eq!(events.len(), 1);
        events.try_recv().unwrap();

        let (sender, mut events) = mpsc::channel(8);
        sender
            .try_send(queued_replay_event(
                vec![entry.clone()],
                nx_sync::OpId::generate(),
                "replay-source-a",
            ))
            .unwrap();
        sender
            .try_send(queued_replay_event(
                vec![entry.clone()],
                nx_sync::OpId::generate(),
                "replay-source-b",
            ))
            .unwrap();
        sender
            .try_send(queued_replay_event(
                vec![entry.clone()],
                nx_sync::OpId::generate(),
                "replay-source-a",
            ))
            .unwrap();
        let first = events.try_recv().unwrap();
        let (history, pending) =
            collect_remote_history_without_coalescing(first, &mut events).await;
        assert_eq!(history.len(), 3);
        assert_eq!(
            history
                .iter()
                .map(|envelope| match envelope {
                    IncomingHistory::Replay(replay) => replay.addr.as_str(),
                    IncomingHistory::Push(_) => "unexpected-push",
                })
                .collect::<Vec<_>>(),
            vec!["replay-source-a", "replay-source-b", "replay-source-a"]
        );
        assert!(pending.is_none());
        assert!(events.is_empty());

        let (sender, mut events) = mpsc::channel(8);
        sender
            .try_send(queued_replay_event(
                vec![entry.clone()],
                nx_sync::OpId::generate(),
                "replay-source-a",
            ))
            .unwrap();
        sender
            .try_send(queued_event(vec![entry.clone()], 1))
            .unwrap();
        sender
            .try_send(queued_replay_event(
                vec![entry.clone()],
                nx_sync::OpId::generate(),
                "replay-source-b",
            ))
            .unwrap();
        sender
            .try_send(NodeEvent::PullRequested {
                from: NodeId::new("boundary"),
                addr: "boundary".into(),
                since_op_id: None,
            })
            .unwrap();
        let first = events.try_recv().unwrap();
        let (history, pending) =
            collect_remote_history_without_coalescing(first, &mut events).await;
        assert_eq!(history.len(), 3);
        assert!(matches!(&history[0], IncomingHistory::Replay(_)));
        assert!(matches!(&history[1], IncomingHistory::Push(_)));
        assert!(matches!(&history[2], IncomingHistory::Replay(_)));
        assert!(matches!(pending, Some(NodeEvent::PullRequested { .. })));
        assert!(events.is_empty());

        let large_origin = NodeId::new("replay-large");
        let large_entry = nx_sync::SequencedOp {
            op: Op::lww_register_set(large_origin, "large", vec![0; 60 * 1024], 1),
            position: nx_sync::OpPosition {
                generation: 1,
                sequence: 1,
            },
        };
        let large_ops = vec![large_entry; 56];
        assert!(
            replay_work(&queued_replay_event(
                large_ops.clone(),
                nx_sync::OpId::generate(),
                "replay-large-source",
            ))
            .is_some()
        );
        let (sender, mut events) = mpsc::channel(8);
        for index in 0..3 {
            let event = if index == 1 {
                queued_event(large_ops.clone(), index)
            } else {
                queued_replay_event(
                    large_ops.clone(),
                    nx_sync::OpId::generate(),
                    "replay-large-source",
                )
            };
            sender.try_send(event).unwrap();
        }
        let first = events.try_recv().unwrap();
        let (history, pending) =
            collect_remote_history_without_coalescing(first, &mut events).await;
        assert_eq!(history.len(), 2);
        assert!(pending.is_some());
        assert!(events.is_empty());

        let (sender, mut events) = mpsc::channel(4);
        sender
            .try_send(queued_event(vec![entry.clone(); 1024], 0))
            .unwrap();
        sender
            .try_send(queued_replay_event(
                vec![entry],
                nx_sync::OpId::generate(),
                "replay-source",
            ))
            .unwrap();
        let first = events.try_recv().unwrap();
        let (history, pending) =
            collect_remote_history_without_coalescing(first, &mut events).await;
        assert_eq!(history.len(), 1);
        assert!(matches!(&history[0], IncomingHistory::Push(push) if push.ops.len() == 1024));
        assert!(pending.is_some());
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn queued_collection_preserves_boundaries_and_work_limits() {
        let entry = queued_entries().remove(0);
        let (sender, mut events) = mpsc::channel(128);
        for index in 0..65 {
            sender
                .try_send(queued_event(vec![entry.clone()], index))
                .unwrap();
        }
        let first = events.try_recv().unwrap();
        let (history, pending) =
            collect_remote_history_without_coalescing(first, &mut events).await;
        assert_eq!(history.len(), super::super::delivery::TURN_MESSAGES);
        assert!(
            history
                .iter()
                .all(|envelope| matches!(envelope, IncomingHistory::Push(_)))
        );
        assert!(pending.is_none());
        assert_eq!(events.len(), 1);
        events.try_recv().unwrap();
        sender
            .try_send(queued_event(vec![entry.clone(); 1024], 0))
            .unwrap();
        sender
            .try_send(queued_event(vec![entry.clone()], 1))
            .unwrap();
        let first = events.try_recv().unwrap();
        let (history, pending) =
            collect_remote_history_without_coalescing(first, &mut events).await;
        assert_eq!(history.len(), 1);
        assert!(matches!(&history[0], IncomingHistory::Push(_)));
        assert!(pending.is_some());
        assert!(events.is_empty());
        sender
            .try_send(queued_event(vec![entry.clone()], 0))
            .unwrap();
        sender
            .try_send(NodeEvent::PullRequested {
                from: NodeId::new("boundary"),
                addr: "boundary".into(),
                since_op_id: None,
            })
            .unwrap();
        sender.try_send(queued_event(vec![entry], 1)).unwrap();
        let first = events.try_recv().unwrap();
        let (history, pending) =
            collect_remote_history_without_coalescing(first, &mut events).await;
        assert_eq!(history.len(), 1);
        assert!(matches!(&history[0], IncomingHistory::Push(_)));
        assert!(matches!(pending, Some(NodeEvent::PullRequested { .. })));
        assert_eq!(events.len(), 1);
        events.try_recv().unwrap();
        let large = nx_sync::SequencedOp {
            op: Op::lww_register_set(NodeId::new("large"), "large", vec![0; 32 * 1024], 1),
            position: nx_sync::OpPosition {
                generation: 1,
                sequence: 1,
            },
        };
        let first = queued_event(vec![large.clone(); 192], 0);
        let (_, bytes) = push_work(&first).unwrap();
        assert!(bytes < super::super::delivery::TURN_BYTES);
        assert!(bytes * 2 > super::super::delivery::TURN_BYTES);
        sender.try_send(queued_event(vec![large; 192], 1)).unwrap();
        let (history, pending) =
            collect_remote_history_without_coalescing(first, &mut events).await;
        assert_eq!(history.len(), 1);
        assert!(matches!(&history[0], IncomingHistory::Push(_)));
        assert!(pending.is_some());
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn coalesced_remote_history_flushes_once_after_second_event_arrives() {
        let context = baseline_test_context();
        let before_flushes = context.store.completed_flushes();
        let first = queued_event(
            queued_entries_for("coalesce-a")
                .into_iter()
                .take(1)
                .collect(),
            0,
        );
        let second = queued_event(
            queued_entries_for("coalesce-b")
                .into_iter()
                .take(1)
                .collect(),
            1,
        );
        let (sender, mut event_rx) = mpsc::channel(8);
        let mut priority_event_rx = None;
        let priority_event = {
            let processing = handle_node_events(
                first,
                &mut event_rx,
                &mut priority_event_rx,
                true,
                &context,
                Some(REMOTE_HISTORY_COALESCE_WINDOW),
            );
            tokio::pin!(processing);
            tokio::select! {
                biased;
                _ = &mut processing => panic!("collector returned before the coalescing window ended"),
                result = sender.send(second) => result.unwrap(),
            }
            processing.await
        };
        assert!(priority_event.is_none());
        assert_eq!(context.store.completed_flushes() - before_flushes, 1);
        assert_eq!(context.counters.read().await["det-g"].value(), 14);
        assert!(event_rx.is_empty());
    }

    #[tokio::test]
    async fn coalescing_returns_priority_event_without_consuming_later_data() {
        let context = baseline_test_context();
        let first = queued_event(
            queued_entries_for("coalesce-first")
                .into_iter()
                .take(1)
                .collect(),
            0,
        );
        let later_data = queued_event(
            queued_entries_for("coalesce-later")
                .into_iter()
                .take(1)
                .collect(),
            1,
        );
        let (data_sender, mut event_rx) = mpsc::channel(8);
        let (priority_sender, priority_receiver) = mpsc::channel(8);
        let mut priority_event_rx = Some(priority_receiver);
        let control = NodeEvent::PullRequested {
            from: NodeId::new("priority-boundary"),
            addr: "priority-boundary".into(),
            since_op_id: None,
        };
        let priority_event = {
            let processing = handle_node_events(
                first,
                &mut event_rx,
                &mut priority_event_rx,
                true,
                &context,
                Some(Duration::from_secs(5)),
            );
            tokio::pin!(processing);
            tokio::select! {
                biased;
                _ = &mut processing => panic!("collector returned before a priority event arrived"),
                result = priority_sender.send(control) => result.unwrap(),
            }
            data_sender.send(later_data).await.unwrap();
            processing.await
        };
        assert!(matches!(
            priority_event,
            Some(NodeEvent::PullRequested { .. })
        ));
        assert_eq!(event_rx.len(), 1);
        assert!(matches!(
            event_rx.try_recv(),
            Ok(NodeEvent::HistoryReceived { .. })
        ));
    }

    async fn arm_baseline_request(context: &NodeEventContext, origin: &NodeId) -> nx_sync::OpId {
        let id = nx_sync::OpId::generate();
        context
            .history_requests
            .write()
            .await
            .entry("source:1234".into())
            .or_default()
            .insert(
                origin.as_str().to_owned(),
                PendingHistoryRequest {
                    request_id: id.clone(),
                    start: 0,
                    end: 0,
                    baseline_revision: Some(
                        super::super::storage::recovery_revision(&context.store, origin).unwrap(),
                    ),
                    deadline: StdInstant::now() + Duration::from_secs(30),
                },
            );
        id
    }

    async fn arm_replay_request(
        context: &NodeEventContext,
        addr: &str,
        origin: &NodeId,
        request_id: &nx_sync::OpId,
    ) {
        context
            .history_requests
            .write()
            .await
            .entry(addr.to_owned())
            .or_default()
            .insert(
                origin.as_str().to_owned(),
                PendingHistoryRequest {
                    request_id: request_id.clone(),
                    start: 1,
                    end: 1,
                    baseline_revision: None,
                    deadline: StdInstant::now() + Duration::from_secs(30),
                },
            );
    }

    fn baseline_payload(origin: &NodeId, sequence: u64, legacy: bool) -> Vec<u8> {
        let mut counter = GCounter::new();
        counter.increment(origin, 7);
        serde_json::to_vec(&nx_sync::CrdtBaseline {
            origins: vec![nx_sync::BaselineOrigin {
                origin: origin.clone(),
                sequence,
                legacy,
            }],
            records: vec![nx_sync::BaselineRecord {
                family: 1,
                key: "visits".into(),
                state: counter.to_json().unwrap().into_bytes(),
            }],
        })
        .unwrap()
    }

    #[tokio::test]
    async fn baseline_live_refusal_after_other_source_repair_cannot_describe_a_new_gap() {
        for mode in ["complete", "partial", "legacy"] {
            let (rotation_tx, rotation_rx) = watch::channel(RecoveryRotation::default());
            let mut context = baseline_test_context();
            context.recovery_rotation = Some(rotation_tx);
            let origin = NodeId::new("origin");
            let legacy = mode == "legacy";
            super::super::storage::record_summary(
                &context.store,
                &nx_sync::HistorySummary {
                    origin: origin.clone(),
                    high_sequence: if legacy { 0 } else { 2 },
                    baseline_required: legacy,
                },
            )
            .unwrap();
            let request_id = arm_baseline_request(&context, &origin).await;
            if legacy {
                let payload = baseline_payload(&origin, 0, true);
                super::super::baseline::import(
                    super::super::baseline::VERSION,
                    &payload,
                    blake3::hash(&payload).as_bytes(),
                    &origin,
                    &history_apply_context(&context),
                )
                .await
                .unwrap();
            } else {
                let count = if mode == "partial" { 1 } else { 2 };
                let entries = (1..=count)
                    .map(|sequence| nx_sync::SequencedOp {
                        op: Op::gcounter_increment(origin.clone(), "visits", 1),
                        position: nx_sync::OpPosition {
                            generation: 1,
                            sequence,
                        },
                    })
                    .collect::<Vec<_>>();
                apply_sequenced_ops(&entries, &history_apply_context(&context))
                    .await
                    .unwrap();
            }
            // Even an unexpired attempt is stale after another source changed coverage.
            let before = context.store.scan_prefix(b"__nx/").unwrap();
            handle_history_message(
                "source:1234",
                HistoryMessage::BaselineUnavailable {
                    request_id,
                    origin: origin.clone(),
                    reason: nx_sync::BaselineFailure::SourceIncomplete,
                },
                &context,
            )
            .await
            .unwrap();
            assert_eq!(
                context.store.scan_prefix(b"__nx/").unwrap(),
                before,
                "{mode}"
            );
            assert_eq!(rotation_rx.borrow().generation, 0, "{mode}");
            super::super::storage::record_summary(
                &context.store,
                &nx_sync::HistorySummary {
                    origin: origin.clone(),
                    high_sequence: 3,
                    baseline_required: legacy,
                },
            )
            .unwrap();
            assert!(
                super::super::storage::replication_progress(&context.store).unwrap()[0]
                    .recovery_failure
                    .is_none(),
                "{mode}"
            );
        }
    }

    #[tokio::test]
    async fn baseline_failure_is_invalidated_by_partial_repair_or_a_new_target() {
        for change_high in [false, true] {
            let (rotation_tx, rotation_rx) = watch::channel(RecoveryRotation::default());
            let mut context = baseline_test_context();
            context.recovery_rotation = Some(rotation_tx);
            let origin = NodeId::new("origin");
            let summary = nx_sync::HistorySummary {
                origin: origin.clone(),
                high_sequence: 2,
                baseline_required: false,
            };
            super::super::storage::record_summary(&context.store, &summary).unwrap();
            let request_id = arm_baseline_request(&context, &origin).await;
            handle_history_message(
                "source:1234",
                HistoryMessage::BaselineUnavailable {
                    request_id,
                    origin: origin.clone(),
                    reason: nx_sync::BaselineFailure::BudgetExceeded,
                },
                &context,
            )
            .await
            .unwrap();
            assert_eq!(rotation_rx.borrow().generation, 1);
            assert_eq!(
                rotation_rx.borrow().source_addr.as_deref(),
                Some("source:1234")
            );
            // Repeating the same summary must retain a still-relevant failure.
            super::super::storage::record_summary(&context.store, &summary).unwrap();
            assert!(
                super::super::storage::replication_progress(&context.store).unwrap()[0]
                    .recovery_failure
                    .is_some()
            );
            if change_high {
                super::super::storage::record_summary(
                    &context.store,
                    &nx_sync::HistorySummary {
                        high_sequence: 3,
                        ..summary
                    },
                )
                .unwrap();
            } else {
                apply_sequenced_ops(
                    &[nx_sync::SequencedOp {
                        op: Op::gcounter_increment(origin, "visits", 1),
                        position: nx_sync::OpPosition {
                            generation: 1,
                            sequence: 1,
                        },
                    }],
                    &history_apply_context(&context),
                )
                .await
                .unwrap();
            }
            let progress = super::super::storage::replication_progress(&context.store).unwrap();
            assert!(progress[0].contiguous_sequence < progress[0].known_high_sequence);
            assert!(progress[0].recovery_failure.is_none());
        }
    }

    #[tokio::test]
    async fn successful_baseline_cancels_pending_recovery_rotation() {
        let (rotation_tx, rotation_rx) = watch::channel(RecoveryRotation::default());
        let mut context = baseline_test_context();
        context.recovery_rotation = Some(rotation_tx);
        let origin = NodeId::new("origin");
        super::super::storage::record_summary(
            &context.store,
            &nx_sync::HistorySummary {
                origin: origin.clone(),
                high_sequence: 1,
                baseline_required: false,
            },
        )
        .unwrap();
        let request_id = arm_baseline_request(&context, &origin).await;
        signal_recovery_rotation(&context, "source:1234");
        let payload = baseline_payload(&origin, 1, false);
        handle_history_message(
            "source:1234",
            HistoryMessage::Baseline {
                request_id,
                origin: origin.clone(),
                version: super::super::baseline::VERSION,
                digest: *blake3::hash(&payload).as_bytes(),
                payload,
            },
            &context,
        )
        .await
        .unwrap();

        let rotation = rotation_rx.borrow();
        assert_eq!(rotation.generation, 2);
        assert_eq!(rotation.source_addr, None);
    }

    #[tokio::test]
    async fn baseline_handler_preserves_storage_faults_and_does_not_label_them_invalid_state() {
        use nx_store::StoreError;
        for flush in [false, true] {
            let context = baseline_test_context();
            let origin = NodeId::new("origin");
            super::super::storage::record_summary(
                &context.store,
                &nx_sync::HistorySummary {
                    origin: origin.clone(),
                    high_sequence: 1,
                    baseline_required: false,
                },
            )
            .unwrap();
            let request_id = arm_baseline_request(&context, &origin).await;
            let payload = baseline_payload(&origin, 1, false);
            let before = context.store.scan_prefix(b"__nx/").unwrap();
            if flush {
                context.store.inject_disk_full_on_flush(true);
            } else {
                context.store.inject_disk_full_on_writes(true);
            }
            let error = handle_history_message(
                "source:1234",
                HistoryMessage::Baseline {
                    request_id,
                    origin,
                    version: super::super::baseline::VERSION,
                    digest: *blake3::hash(&payload).as_bytes(),
                    payload,
                },
                &context,
            )
            .await
            .unwrap_err();
            assert!(
                error
                    .downcast_ref::<super::super::baseline::Rejected>()
                    .is_none(),
                "{error:#}"
            );
            if flush {
                assert!(
                    matches!(
                        error.downcast_ref::<StoreError>(),
                        Some(StoreError::InjectedFlushFailure)
                    ),
                    "{error:#}"
                );
            } else {
                assert!(
                    matches!(
                        error.downcast_ref::<StoreError>(),
                        Some(StoreError::InjectedDiskFull)
                    ),
                    "{error:#}"
                );
                assert_eq!(context.store.scan_prefix(b"__nx/").unwrap(), before);
            }
            let progress = super::super::storage::replication_progress(&context.store).unwrap();
            assert!(progress[0].recovery_failure.is_none());
            assert_eq!(progress[0].baseline_unconfirmed, flush);
            assert_eq!(context.counters.read().await.contains_key("visits"), flush);
        }
        let context = baseline_test_context();
        context.store.inject_disk_full_on_flush(true);
        let error = handle_history_message(
            "source:1234",
            HistoryMessage::BaselineRequest {
                request_id: nx_sync::OpId::generate(),
                origin: NodeId::new("origin"),
                version: super::super::baseline::VERSION,
            },
            &context,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                error.downcast_ref::<StoreError>(),
                Some(StoreError::InjectedFlushFailure)
            ),
            "{error:#}"
        );
        assert!(
            error
                .downcast_ref::<super::super::baseline::Rejected>()
                .is_none()
        );
    }

    #[tokio::test]
    async fn baseline_handler_preserves_metadata_errors_in_export_and_import() {
        let origin = NodeId::new("origin");
        let context = baseline_test_context();
        super::super::storage::record_summary(
            &context.store,
            &nx_sync::HistorySummary {
                origin: origin.clone(),
                high_sequence: 1,
                baseline_required: false,
            },
        )
        .unwrap();
        let request_id = arm_baseline_request(&context, &origin).await;
        let payload = baseline_payload(&origin, 1, false);
        // Corrupt local accounting is an operational fault, not a source-cut conflict.
        context
            .store
            .set(b"__nx/crdt/sequence/usage", b"invalid-local-accounting")
            .unwrap();
        let before = context.store.scan_prefix(b"__nx/").unwrap();
        let error = handle_history_message(
            "source:1234",
            HistoryMessage::Baseline {
                request_id,
                origin: origin.clone(),
                version: super::super::baseline::VERSION,
                digest: *blake3::hash(&payload).as_bytes(),
                payload,
            },
            &context,
        )
        .await
        .unwrap_err();
        assert!(
            error
                .downcast_ref::<super::super::baseline::Rejected>()
                .is_none(),
            "{error:#}"
        );
        assert!(error.downcast_ref::<serde_json::Error>().is_some());
        assert_eq!(context.store.scan_prefix(b"__nx/").unwrap(), before);
        assert!(context.counters.read().await.is_empty());

        context
            .store
            .set(
                b"__nx/crdt/sequence/origin/origin",
                b"corrupt-local-receipt",
            )
            .unwrap();
        let error = handle_history_message(
            "source:1234",
            HistoryMessage::BaselineRequest {
                request_id: nx_sync::OpId::generate(),
                origin,
                version: super::super::baseline::VERSION,
            },
            &context,
        )
        .await
        .unwrap_err();
        assert!(
            error
                .downcast_ref::<super::super::baseline::Rejected>()
                .is_none(),
            "{error:#}"
        );
        assert!(error.downcast_ref::<serde_json::Error>().is_some());
    }

    #[tokio::test]
    async fn baseline_handler_still_records_invalid_payload_rejections() {
        let context = baseline_test_context();
        let origin = NodeId::new("origin");
        super::super::storage::record_summary(
            &context.store,
            &nx_sync::HistorySummary {
                origin: origin.clone(),
                high_sequence: 1,
                baseline_required: false,
            },
        )
        .unwrap();
        let request_id = arm_baseline_request(&context, &origin).await;
        let error = handle_history_message(
            "source:1234",
            HistoryMessage::Baseline {
                request_id,
                origin,
                version: super::super::baseline::VERSION,
                payload: vec![1],
                digest: [0; 32],
            },
            &context,
        )
        .await
        .unwrap_err();
        assert_eq!(
            error
                .downcast_ref::<super::super::baseline::Rejected>()
                .unwrap()
                .0,
            nx_sync::BaselineFailure::InvalidState
        );
        let progress = super::super::storage::replication_progress(&context.store).unwrap();
        assert_eq!(
            progress[0].recovery_failure.as_ref().unwrap().reason,
            nx_sync::BaselineFailure::InvalidState
        );
        assert_eq!(progress[0].contiguous_sequence, 0);
        assert!(context.counters.read().await.is_empty());
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
                        baseline_revision: Some(
                            super::super::storage::recovery_revision(&store, &origin).unwrap(),
                        ),
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
                    baseline_revision: Some(
                        super::super::storage::recovery_revision(&store, &origin).unwrap(),
                    ),
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

    #[test]
    fn connection_error_classes_are_bounded_and_do_not_include_error_text() {
        assert_eq!(
            connection_error_class(&NetError::Io(std::io::Error::from(
                std::io::ErrorKind::ConnectionReset
            ))),
            "io_connection_reset"
        );
        assert_eq!(
            connection_error_class(&NetError::ConnectionFailed(
                "outbound connection attempt limit reached: 8".into()
            )),
            "outbound_attempt_limit"
        );
        assert_eq!(
            connection_error_class(&NetError::ConnectionFailed(
                "sensitive endpoint-specific detail".into()
            )),
            "connection_failed"
        );
        assert_eq!(
            connection_error_class(&NetError::Wire(WireError::RateLimited {
                retry_after_ms: Some(50),
            })),
            "wire_retry_after"
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
        let (rotation_tx, rotation_rx) = watch::channel(RecoveryRotation::default());
        let mut context = test_event_context(
            Arc::new(RwLock::new(HashMap::new())),
            Arc::new(RwLock::new(SeenOps::new(1))),
            Arc::new(RwLock::new(Vec::new())),
            Arc::clone(&store),
            metrics(),
            Arc::new(RwLock::new(HashMap::new())),
        );
        context.recovery_rotation = Some(rotation_tx);
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
                    baseline_revision: None,
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
                    baseline_revision: None,
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
        assert_eq!(rotation_rx.borrow().generation, 1);
        assert_eq!(rotation_rx.borrow().source_addr.as_deref(), Some(addr));
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
    async fn unavailable_range_tries_one_connected_alternative_before_baseline() {
        use nx_sync::{HistorySummary, OpId};

        let store = temp_store();
        let (rotation_tx, rotation_rx) = watch::channel(RecoveryRotation::default());
        let mut local = Node::new(NodeConfig::new(
            NodeId::new("range-requester"),
            "127.0.0.1:0",
        ));
        let _local_events = local.take_event_receiver().unwrap();
        local.start_listener().await.unwrap();

        let alternative_id = NodeId::new("range-alternative");
        let mut alternative = Node::new(NodeConfig::new(alternative_id.clone(), "127.0.0.1:0"));
        let mut alternative_events = alternative.take_event_receiver().unwrap();
        let alternative_endpoint = alternative.start_listener().await.unwrap().to_string();
        local.connect_to_peer(&alternative_endpoint).await.unwrap();
        let alternative_addr = local
            .connected_peers()
            .await
            .into_iter()
            .find(|(_, id)| id == &alternative_id)
            .unwrap()
            .0;
        let local = Arc::new(local);

        let mut context = test_event_context(
            Arc::new(RwLock::new(HashMap::new())),
            Arc::new(RwLock::new(SeenOps::new(1))),
            Arc::new(RwLock::new(Vec::new())),
            Arc::clone(&store),
            metrics(),
            Arc::new(RwLock::new(HashMap::new())),
        );
        context.node = local.clone();
        context.recovery_rotation = Some(rotation_tx);
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
        let failed_source = "failed-source:1234";
        let first_request = OpId::generate();
        context.history_requests.write().await.insert(
            failed_source.to_owned(),
            HashMap::from([(
                origin.as_str().to_owned(),
                PendingHistoryRequest {
                    request_id: first_request.clone(),
                    start: 1,
                    end: 1,
                    baseline_revision: None,
                    deadline: StdInstant::now() + Duration::from_secs(30),
                },
            )]),
        );

        handle_history_message(
            failed_source,
            HistoryMessage::Unavailable {
                request_id: first_request,
                origin: origin.clone(),
                start: 1,
                end: 1,
            },
            &context,
        )
        .await
        .unwrap();
        assert_eq!(
            super::super::storage::replication_progress(&store).unwrap()[0]
                .unavailable_evidence
                .as_ref()
                .unwrap()
                .source,
            failed_source
        );
        let first_message = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let Some(event) = alternative_events.recv().await else {
                    panic!("alternative peer event stream closed");
                };
                if let NodeEvent::HistoryReceived { message, .. } = event {
                    break message;
                }
            }
        })
        .await
        .expect("alternative range request was not sent");
        let HistoryMessage::Request {
            request_id: alternate_request,
            origin: alternate_origin,
            start,
            end,
        } = first_message
        else {
            panic!("expected an alternate range request");
        };
        assert_eq!(alternate_origin, origin);
        assert_eq!((start, end), (1, 1));
        assert_eq!(rotation_rx.borrow().generation, 0);

        handle_history_message(
            &alternative_addr,
            HistoryMessage::Unavailable {
                request_id: alternate_request,
                origin: origin.clone(),
                start,
                end,
            },
            &context,
        )
        .await
        .unwrap();
        let fallback_message = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let Some(event) = alternative_events.recv().await else {
                    panic!("alternative peer event stream closed");
                };
                if let NodeEvent::HistoryReceived { message, .. } = event {
                    break message;
                }
            }
        })
        .await
        .expect("baseline fallback request was not sent");
        assert!(matches!(
            fallback_message,
            HistoryMessage::BaselineRequest { origin: requested, .. } if requested == origin
        ));
        assert_eq!(rotation_rx.borrow().generation, 1);
        assert_eq!(
            rotation_rx.borrow().source_addr.as_deref(),
            Some(alternative_addr.as_str())
        );

        local.shutdown().await;
        alternative.shutdown().await;
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
                    baseline_revision: None,
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
            store: temp_store(),
            jitter_seed: 1,
            repair_budget: Arc::new(std::sync::Mutex::new(
                super::super::delivery::RepairBudget::new(StdInstant::now()),
            )),
            history_cursors: Arc::clone(&cursors),
            request_timeout: Duration::from_secs(30),
            node: node.clone(),
            interval: Duration::from_millis(1),
            shutdown_rx,
            metrics: metrics(),
            membership: None,
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

    async fn listening_source(
        name: &str,
    ) -> (Node, tokio::sync::mpsc::Receiver<NodeEvent>, String) {
        let mut source = Node::new(NodeConfig::new(NodeId::new(name), "127.0.0.1:0"));
        let events = source.take_event_receiver().unwrap();
        let addr = source.start_listener().await.unwrap().to_string();
        (source, events, addr)
    }

    async fn next_history(
        events: &mut tokio::sync::mpsc::Receiver<NodeEvent>,
        within: Duration,
    ) -> Option<HistoryMessage> {
        tokio::time::timeout(within, async {
            loop {
                if let NodeEvent::HistoryReceived { message, .. } = events.recv().await? {
                    return Some(message);
                }
            }
        })
        .await
        .ok()
        .flatten()
    }

    fn summary_entries(count: usize, high_sequence: u64) -> Vec<nx_sync::HistorySummary> {
        (0..count)
            .map(|index| nx_sync::HistorySummary {
                origin: NodeId::new(format!("origin-{index:02}")),
                high_sequence,
                baseline_required: false,
            })
            .collect()
    }

    fn snapshot_with(
        member: &str,
        endpoint: &str,
        state: nx_net::MembershipState,
        incarnation: u64,
    ) -> crate::membership::runtime::Snapshot {
        crate::membership::runtime::Snapshot {
            records: vec![nx_net::MembershipRecord {
                member: member.into(),
                reporter: member.into(),
                version: nx_net::MembershipVersion {
                    epoch: 1,
                    incarnation,
                },
                state,
                endpoint: Some(endpoint.into()),
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn peer_connected_starts_catch_up_walk_from_first_page() {
        let (source, mut events, addr) = listening_source("source").await;
        let context = baseline_test_context();
        context.node.connect_to_peer(&addr).await.unwrap();
        context
            .history_cursors
            .write()
            .await
            .entry(addr.clone())
            .or_default()
            .after_origin = Some("origin-07".into());
        handle_node_event(
            NodeEvent::PeerConnected {
                node_id: NodeId::new("source"),
                addr: addr.clone(),
                peers_connected: 1,
            },
            &context,
        )
        .await;
        assert!(matches!(
            next_history(&mut events, Duration::from_secs(1)).await,
            Some(HistoryMessage::SummaryRequest { after_origin: None })
        ));
        let cursor = context.history_cursors.read().await[&addr].clone();
        assert!(cursor.catch_up && cursor.request_deadline.is_some());
        context.node.shutdown().await;
        source.shutdown().await;
    }

    #[tokio::test]
    async fn periodic_summary_walk_reaches_later_gap_and_preserves_pending_deadlines() {
        for format in [
            nx_net::SerializationFormat::Json,
            nx_net::SerializationFormat::Bincode,
        ] {
            let mut source = Node::new(
                NodeConfig::new(NodeId::new("source"), "127.0.0.1:0")
                    .with_serialization_format(format),
            );
            let mut events = source.take_event_receiver().unwrap();
            let addr = source.start_listener().await.unwrap().to_string();
            let mut context = baseline_test_context();
            context.node = Arc::new(Node::new(
                NodeConfig::new(NodeId::new("target"), "127.0.0.1:0")
                    .with_serialization_format(format),
            ));
            context.node.connect_to_peer(&addr).await.unwrap();
            let (_shutdown_tx, shutdown_rx) = watch::channel(false);
            let entropy = AntiEntropyLoopContext {
                store: Arc::clone(&context.store),
                jitter_seed: 1,
                repair_budget: Arc::clone(&context.repair_budget),
                history_cursors: Arc::clone(&context.history_cursors),
                request_timeout: context.history_request_timeout,
                node: Arc::clone(&context.node),
                interval: Duration::from_secs(30),
                shutdown_rx,
                metrics: Arc::clone(&context.metrics),
                membership: None,
            };
            let mut peer_cursor = 0;
            anti_entropy_turn(&entropy, &mut peer_cursor).await;
            assert!(matches!(
                next_history(&mut events, Duration::from_secs(1)).await,
                Some(HistoryMessage::SummaryRequest { after_origin: None })
            ));
            let pending = context.history_cursors.read().await[&addr].clone();
            anti_entropy_turn(&entropy, &mut peer_cursor).await;
            assert_eq!(
                context.history_cursors.read().await[&addr].request_deadline,
                pending.request_deadline
            );
            assert!(
                next_history(&mut events, Duration::from_millis(100))
                    .await
                    .is_none()
            );
            handle_history_message(
                &addr,
                HistoryMessage::Summary {
                    entries: summary_entries(SUMMARY_PAGE, 0),
                },
                &context,
            )
            .await
            .unwrap();
            assert!(matches!(
                next_history(&mut events, Duration::from_secs(1)).await,
                Some(HistoryMessage::SummaryRequest { after_origin: Some(after) }) if after == "origin-15"
            ));
            let origin = NodeId::new("origin-16");
            handle_history_message(
                &addr,
                HistoryMessage::Summary {
                    entries: vec![nx_sync::HistorySummary {
                        origin: origin.clone(),
                        high_sequence: 1,
                        baseline_required: false,
                    }],
                },
                &context,
            )
            .await
            .unwrap();
            let Some(HistoryMessage::Request {
                request_id,
                origin: requested_origin,
                start: 1,
                end: 1,
            }) = next_history(&mut events, Duration::from_secs(1)).await
            else {
                panic!("periodic walk did not request the later page's missing operation");
            };
            assert_eq!(requested_origin, origin);
            let backoff = context.history_cursors.read().await[&addr].clone();
            assert!(backoff.catch_up);
            assert_eq!(backoff.rewalks, 1);
            anti_entropy_turn(&entropy, &mut peer_cursor).await;
            let current = context.history_cursors.read().await[&addr].clone();
            assert_eq!(current.request_deadline, backoff.request_deadline);
            assert_eq!(current.after_origin, backoff.after_origin);
            assert_eq!(current.rewalks, backoff.rewalks);
            assert!(
                next_history(&mut events, Duration::from_millis(100))
                    .await
                    .is_none()
            );
            handle_history_message(
                &addr,
                HistoryMessage::Replay {
                    request_id,
                    origin: origin.clone(),
                    start: 1,
                    end: 1,
                    ops: vec![nx_sync::SequencedOp {
                        op: Op::gcounter_increment(origin.clone(), "periodic-page", 1),
                        position: nx_sync::OpPosition {
                            generation: 1,
                            sequence: 1,
                        },
                    }],
                },
                &context,
            )
            .await
            .unwrap();
            assert_eq!(context.counters.read().await["periodic-page"].value(), 1);
            assert_eq!(context.store.completed_flushes(), 1);
            let progress = super::super::storage::replication_progress(&context.store).unwrap();
            let receipt = progress
                .iter()
                .find(|entry| entry.origin == origin)
                .unwrap();
            assert_eq!(receipt.contiguous_sequence, 1);
            assert_eq!(receipt.known_high_sequence, 1);
            assert_eq!(receipt.sparse_positions, 0);
            assert!(!receipt.source_unavailable);
            assert!(receipt.recovery_failure.is_none());
            context.node.shutdown().await;
            source.shutdown().await;
        }
    }

    #[tokio::test]
    async fn failed_summary_page_retries_unprocessed_origins_without_reapplying_prefix() {
        let (source, mut events, addr) = listening_source("source").await;
        let context = baseline_test_context();
        context.node.connect_to_peer(&addr).await.unwrap();
        context
            .history_cursors
            .write()
            .await
            .entry(addr.clone())
            .or_default()
            .catch_up = true;
        {
            let mut budget = context.repair_budget.lock().unwrap();
            for _ in 1..super::super::delivery::TURN_MESSAGES {
                assert!(budget.admit(0, context.node.now()));
            }
        }
        let error = handle_history_message(
            &addr,
            HistoryMessage::Summary {
                entries: summary_entries(SUMMARY_PAGE, 1),
            },
            &context,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error.downcast_ref::<NetError>(),
            Some(NetError::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock
        ));
        let Some(HistoryMessage::Request {
            request_id,
            origin,
            start: 1,
            end: 1,
        }) = next_history(&mut events, Duration::from_secs(1)).await
        else {
            panic!("the admitted prefix request was not delivered");
        };
        assert_eq!(origin.as_str(), "origin-00");
        let progress = super::super::storage::replication_progress(&context.store).unwrap();
        assert_eq!(progress.len(), SUMMARY_PAGE);
        assert_eq!(progress.last().unwrap().known_high_sequence, 1);
        let cursor = context.history_cursors.read().await[&addr].clone();
        assert!(
            cursor.after_origin.is_none(),
            "failed page skipped its unprocessed suffix"
        );
        assert!(cursor.request_deadline.is_none());
        assert!(cursor.catch_up);
        assert_eq!(cursor.rewalks, 0);
        assert_eq!(context.history_requests.read().await[&addr].len(), 1);
        handle_history_message(
            &addr,
            HistoryMessage::Replay {
                request_id,
                origin: origin.clone(),
                start: 1,
                end: 1,
                ops: vec![nx_sync::SequencedOp {
                    op: Op::gcounter_increment(origin.clone(), "retry-page", 1),
                    position: nx_sync::OpPosition {
                        generation: 1,
                        sequence: 1,
                    },
                }],
            },
            &context,
        )
        .await
        .unwrap();
        *context.repair_budget.lock().unwrap() =
            super::super::delivery::RepairBudget::new(context.node.now());
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let entropy = AntiEntropyLoopContext {
            store: Arc::clone(&context.store),
            jitter_seed: 1,
            repair_budget: Arc::clone(&context.repair_budget),
            history_cursors: Arc::clone(&context.history_cursors),
            request_timeout: context.history_request_timeout,
            node: Arc::clone(&context.node),
            interval: Duration::from_secs(30),
            shutdown_rx,
            metrics: Arc::clone(&context.metrics),
            membership: None,
        };
        catch_up_turn(&entropy).await;
        assert!(matches!(
            next_history(&mut events, Duration::from_secs(1)).await,
            Some(HistoryMessage::SummaryRequest { after_origin: None })
        ));
        handle_history_message(
            &addr,
            HistoryMessage::Summary {
                entries: summary_entries(SUMMARY_PAGE, 1),
            },
            &context,
        )
        .await
        .unwrap();
        for index in 1..SUMMARY_PAGE {
            assert!(matches!(
                next_history(&mut events, Duration::from_secs(1)).await,
                Some(HistoryMessage::Request { origin, start: 1, end: 1, .. })
                    if origin.as_str() == format!("origin-{index:02}")
            ));
        }
        assert!(matches!(
            next_history(&mut events, Duration::from_secs(1)).await,
            Some(HistoryMessage::SummaryRequest { after_origin: Some(after) }) if after == "origin-15"
        ));
        assert_eq!(context.counters.read().await["retry-page"].value(), 1);
        assert_eq!(context.store.completed_flushes(), 1);
        let progress = super::super::storage::replication_progress(&context.store).unwrap();
        assert_eq!(progress.len(), SUMMARY_PAGE);
        let receipt = progress
            .iter()
            .find(|entry| entry.origin == origin)
            .unwrap();
        assert_eq!(receipt.contiguous_sequence, 1);
        assert_eq!(receipt.known_high_sequence, 1);
        assert_eq!(receipt.sparse_positions, 0);
        context.node.shutdown().await;
        source.shutdown().await;
    }

    #[tokio::test]
    async fn catch_up_walk_continues_to_wrap_and_rewalks_only_while_gaps_remain() {
        let (source, mut events, addr) = listening_source("source").await;
        let context = baseline_test_context();
        context.node.connect_to_peer(&addr).await.unwrap();
        context
            .history_cursors
            .write()
            .await
            .entry(addr.clone())
            .or_default()
            .catch_up = true;
        // A full page without gaps continues the walk immediately.
        handle_history_message(
            &addr,
            HistoryMessage::Summary {
                entries: summary_entries(SUMMARY_PAGE, 0),
            },
            &context,
        )
        .await
        .unwrap();
        assert!(matches!(
            next_history(&mut events, Duration::from_secs(1)).await,
            Some(HistoryMessage::SummaryRequest { after_origin: Some(after) }) if after == "origin-15"
        ));
        // A wrapped walk that found no gap ends the session without more requests.
        handle_history_message(&addr, HistoryMessage::Summary { entries: vec![] }, &context)
            .await
            .unwrap();
        assert!(!context.history_cursors.read().await[&addr].catch_up);
        assert!(
            next_history(&mut events, Duration::from_millis(200))
                .await
                .is_none()
        );

        // A walk that found a gap is repeated after growing delays, then given up.
        let mut delays = Vec::new();
        for _ in 0..=CATCH_UP_REWALKS {
            {
                let mut cursors = context.history_cursors.write().await;
                let cursor = cursors.get_mut(&addr).unwrap();
                cursor.catch_up = true;
                cursor.request_deadline = None;
            }
            context.history_requests.write().await.clear();
            let before = StdInstant::now();
            handle_history_message(
                &addr,
                HistoryMessage::Summary {
                    entries: summary_entries(1, 3),
                },
                &context,
            )
            .await
            .unwrap();
            let cursor = context.history_cursors.read().await[&addr].clone();
            delays.push(
                cursor
                    .catch_up
                    .then(|| cursor.request_deadline.unwrap().duration_since(before)),
            );
        }
        let expected: Vec<_> = (0..CATCH_UP_REWALKS)
            .map(|n| Some(SUMMARY_REQUEST_TIMEOUT * (1 << n)))
            .chain([None])
            .collect();
        for (delay, expected) in delays.iter().zip(&expected) {
            match (delay, expected) {
                (Some(delay), Some(expected)) => {
                    assert!(*delay >= *expected && *delay < *expected + Duration::from_secs(1))
                }
                (None, None) => {}
                other => panic!("unexpected rewalk schedule: {other:?} in {delays:?}"),
            }
        }
        assert_eq!(context.history_cursors.read().await[&addr].rewalks, 0);
        context.node.shutdown().await;
        source.shutdown().await;
    }

    #[tokio::test]
    async fn hedged_range_requests_defer_fresh_duplicates_then_ask_another_peer() {
        let (first, mut first_events, first_addr) = listening_source("first").await;
        let (second, mut second_events, second_addr) = listening_source("second").await;
        let context = baseline_test_context();
        context.node.connect_to_peer(&first_addr).await.unwrap();
        context.node.connect_to_peer(&second_addr).await.unwrap();
        let origin = NodeId::new("writer");
        request_history_range(&first_addr, origin.clone(), 1, 3, &context)
            .await
            .unwrap();
        assert!(matches!(
            next_history(&mut first_events, Duration::from_secs(1)).await,
            Some(HistoryMessage::Request {
                start: 1,
                end: 3,
                ..
            })
        ));
        request_history_range(&second_addr, origin.clone(), 1, 3, &context)
            .await
            .unwrap();
        assert!(
            next_history(&mut second_events, Duration::from_millis(200))
                .await
                .is_none(),
            "a fresh request at another peer was duplicated"
        );
        assert!(
            !context
                .history_requests
                .read()
                .await
                .get(&second_addr)
                .is_some_and(|pending| pending.contains_key(origin.as_str()))
        );
        // Once the first request has aged past the hedge delay, another peer is asked.
        context
            .history_requests
            .write()
            .await
            .get_mut(&first_addr)
            .unwrap()
            .get_mut(origin.as_str())
            .unwrap()
            .deadline = StdInstant::now() + context.history_request_timeout
            - REPAIR_HEDGE_AFTER
            - Duration::from_millis(100);
        request_history_range(&second_addr, origin.clone(), 1, 3, &context)
            .await
            .unwrap();
        assert!(matches!(
            next_history(&mut second_events, Duration::from_secs(1)).await,
            Some(HistoryMessage::Request {
                start: 1,
                end: 3,
                ..
            })
        ));
        context.node.shutdown().await;
        first.shutdown().await;
        second.shutdown().await;
    }

    fn quiet_anti_entropy(
        node: Arc<Node>,
        cursors: Arc<RwLock<HashMap<String, HistoryCursor>>>,
        membership: Option<watch::Receiver<crate::membership::runtime::Snapshot>>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> AntiEntropyLoopContext {
        AntiEntropyLoopContext {
            store: temp_store(),
            jitter_seed: 1,
            repair_budget: Arc::new(std::sync::Mutex::new(
                super::super::delivery::RepairBudget::new(StdInstant::now()),
            )),
            history_cursors: cursors,
            request_timeout: Duration::from_secs(30),
            node,
            // Periodic turns stay out of the observed window.
            interval: Duration::from_secs(3600),
            shutdown_rx,
            metrics: metrics(),
            membership,
        }
    }

    #[tokio::test]
    async fn revived_member_starts_catch_up_on_an_existing_connection() {
        let (source, mut events, addr) = listening_source("source").await;
        let node = Arc::new(Node::new(NodeConfig::new(
            NodeId::new("target"),
            "127.0.0.1:0",
        )));
        node.connect_to_peer(&addr).await.unwrap();
        let cursors = Arc::new(RwLock::new(HashMap::new()));
        let (membership_tx, membership_rx) = watch::channel(snapshot_with(
            "source",
            &addr,
            nx_net::MembershipState::Dead,
            0,
        ));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = spawn_anti_entropy_loop(quiet_anti_entropy(
            node.clone(),
            Arc::clone(&cursors),
            Some(membership_rx),
            shutdown_rx,
        ))
        .unwrap();
        assert!(
            next_history(&mut events, Duration::from_millis(300))
                .await
                .is_none()
        );
        membership_tx.send_replace(snapshot_with(
            "source",
            &addr,
            nx_net::MembershipState::Alive,
            1,
        ));
        assert!(matches!(
            next_history(&mut events, Duration::from_secs(1)).await,
            Some(HistoryMessage::SummaryRequest { after_origin: None })
        ));
        assert!(cursors.read().await[&addr].catch_up);
        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        node.shutdown().await;
        source.shutdown().await;
    }

    #[tokio::test]
    async fn unanswered_catch_up_summary_is_retried_despite_frequent_membership_updates() {
        let (source, mut events, addr) = listening_source("source").await;
        let node = Arc::new(Node::new(NodeConfig::new(
            NodeId::new("target"),
            "127.0.0.1:0",
        )));
        node.connect_to_peer(&addr).await.unwrap();
        let cursors = Arc::new(RwLock::new(HashMap::from([(
            addr.clone(),
            HistoryCursor {
                catch_up: true,
                request_deadline: Some(StdInstant::now()),
                ..Default::default()
            },
        )])));
        let (membership_tx, membership_rx) = watch::channel(snapshot_with(
            "source",
            &addr,
            nx_net::MembershipState::Alive,
            0,
        ));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = spawn_anti_entropy_loop(quiet_anti_entropy(
            node.clone(),
            Arc::clone(&cursors),
            Some(membership_rx),
            shutdown_rx,
        ))
        .unwrap();
        // Unrelated membership publications must not postpone the retry deadline.
        let churn = tokio::spawn(async move {
            loop {
                membership_tx.send_modify(|snapshot| snapshot.health += 1);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        assert!(matches!(
            next_history(
                &mut events,
                SUMMARY_REQUEST_TIMEOUT + Duration::from_secs(1)
            )
            .await,
            Some(HistoryMessage::SummaryRequest { .. })
        ));
        churn.abort();
        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        node.shutdown().await;
        source.shutdown().await;
    }

    #[tokio::test]
    async fn revived_member_resets_grown_reconnect_backoff() {
        let endpoint = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().to_string()
        };
        let node = Arc::new(Node::new(NodeConfig::new(
            NodeId::new("dialer"),
            "127.0.0.1:0",
        )));
        let metrics = metrics();
        let (membership_tx, membership_rx) = watch::channel(snapshot_with(
            "remote",
            &endpoint,
            nx_net::MembershipState::Dead,
            0,
        ));
        let (_candidates_tx, candidates_rx) = watch::channel(Arc::new(Vec::new()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = spawn_reconnect_loop(ReconnectLoopContext {
            local_id: NodeId::new("dialer"),
            membership: membership_rx,
            rotation_interval: Duration::from_secs(3600),
            recovery_rotation: None,
            node: node.clone(),
            candidates_rx,
            max_peers: 4,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(20),
            peer_dead_after_failures: 3,
            shutdown_rx,
            metrics: Arc::clone(&metrics),
            peer_health: Arc::new(RwLock::new(HashMap::new())),
        })
        .unwrap();
        // Failed dials to the unreachable member grow its backoff to several seconds.
        tokio::time::sleep(Duration::from_millis(3500)).await;
        let mut remote = Node::new(NodeConfig::new(NodeId::new("remote"), &endpoint));
        let _events = remote.take_event_receiver().unwrap();
        remote.start_listener().await.unwrap();
        let revived = StdInstant::now();
        membership_tx.send_replace(snapshot_with(
            "remote",
            &endpoint,
            nx_net::MembershipState::Alive,
            1,
        ));
        tokio::time::timeout(Duration::from_millis(1500), async {
            while node.connected_peer_count().await == 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("revived member waited for its grown backoff");
        assert!(revived.elapsed() < Duration::from_millis(1500));
        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        node.shutdown().await;
        remote.shutdown().await;
    }
}
