use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant as StdInstant};

use nx_net::Node;
use tokio::sync::RwLock;

use crate::observability::RuntimeMetrics;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerHealthState {
    Healthy,
    Suspect,
    Dead,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PeerHealth {
    pub(super) state: PeerHealthState,
    pub(super) consecutive_failures: u32,
}

impl Default for PeerHealth {
    fn default() -> Self {
        Self {
            state: PeerHealthState::Suspect,
            consecutive_failures: 0,
        }
    }
}

pub(super) struct PeerReconnectState {
    pub(super) addr: String,
    pub(super) delay: Duration,
    pub(super) next_attempt_at: StdInstant,
}

impl PeerReconnectState {
    pub(super) fn new(addr: String, initial_delay: Duration, now: StdInstant) -> Self {
        Self {
            addr,
            delay: initial_delay,
            next_attempt_at: now,
        }
    }

    pub(super) fn reset(&mut self, initial_delay: Duration, now: StdInstant) {
        self.delay = initial_delay;
        self.next_attempt_at = now;
    }

    pub(super) fn record_failure(&mut self, max_delay: Duration, now: StdInstant) -> Duration {
        let attempt_delay = self.delay;
        self.next_attempt_at = now + attempt_delay;
        self.delay = next_reconnect_delay(attempt_delay, max_delay);
        attempt_delay
    }
}

pub(super) struct ConfiguredPeerConnectContext<'a> {
    pub(super) node: &'a Node,
    pub(super) max_peers: usize,
    pub(super) peer_dead_after_failures: u32,
    pub(super) metrics: &'a Arc<RuntimeMetrics>,
    pub(super) peer_health: &'a Arc<RwLock<HashMap<String, PeerHealth>>>,
}

pub(super) enum ConfiguredPeerConnectOutcome {
    Connected,
    AlreadyConnected,
    SlotLimitReached,
    Failed,
}

pub(super) fn normalize_reconnect_delay(delay: Duration) -> Duration {
    delay.max(Duration::from_millis(1))
}

pub(super) fn next_reconnect_delay(current: Duration, max_delay: Duration) -> Duration {
    current.saturating_mul(2).min(max_delay.max(current))
}

pub(super) fn normalize_peer_dead_after_failures(failures: u32) -> u32 {
    failures.max(1)
}

pub(super) fn record_peer_success(peer_health: &mut HashMap<String, PeerHealth>, peer: &str) {
    let health = peer_health.entry(peer.to_string()).or_default();
    health.state = PeerHealthState::Healthy;
    health.consecutive_failures = 0;
}

pub(super) fn record_peer_failure(
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

pub(super) async fn mark_peer_success(
    peer_health: &Arc<RwLock<HashMap<String, PeerHealth>>>,
    peer: &str,
) {
    let mut peer_health = peer_health.write().await;
    record_peer_success(&mut peer_health, peer);
}

pub(super) async fn mark_peer_failure(
    peer_health: &Arc<RwLock<HashMap<String, PeerHealth>>>,
    peer: &str,
    dead_after_failures: u32,
) {
    let mut peer_health = peer_health.write().await;
    record_peer_failure(&mut peer_health, peer, dead_after_failures);
}

pub(super) async fn mark_known_peer_success(
    peer_health: &Arc<RwLock<HashMap<String, PeerHealth>>>,
    peer: &str,
) {
    let mut peer_health = peer_health.write().await;
    if peer_health.contains_key(peer) {
        record_peer_success(&mut peer_health, peer);
    }
}

pub(super) async fn mark_known_peer_failure(
    peer_health: &Arc<RwLock<HashMap<String, PeerHealth>>>,
    peer: &str,
    dead_after_failures: u32,
) {
    let mut peer_health = peer_health.write().await;
    if peer_health.contains_key(peer) {
        record_peer_failure(&mut peer_health, peer, dead_after_failures);
    }
}

// End of main code. Test below:
#[cfg(test)]
mod tests {
    use super::*;

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
}
