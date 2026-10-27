//! Deterministic AIMD byte pacing. The adapter supplies time, queue occupancy,
//! correlated direct-probe samples and local write service time separately.
use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::GossipRateConfig;
use nx_net::{ConnectionGeneration, ControlTiming};

const WINDOW: Duration = Duration::from_secs(1);
pub(super) const SAMPLE_TTL: Duration = Duration::from_secs(10);

#[derive(Clone, Copy)]
pub(super) struct Load {
    pub(super) queued: usize,
    pub(super) capacity: usize,
}
impl Load {
    fn high(self) -> bool {
        self.queued >= (self.capacity / 2).max(1)
    }
}

pub(super) struct Decision {
    pub(super) allowed: bool,
    pub(super) bytes_per_second: u64,
    pub(super) round_trip: Option<Duration>,
}

struct Rate {
    config: GossipRateConfig,
    rate: u64,
    credit: i128,
    remainder: u128,
    created: Instant,
    refilled: Instant,
    adjusted: Instant,
    last_sample: Option<Instant>,
    smoothed_rtt: Option<Duration>,
    baseline_rtt: Option<Duration>,
    rtt_congested: bool,
    slow_write: bool,
    healthy_write: bool,
    high_load: bool,
    local_feedback_at: Instant,
}
impl Rate {
    fn new(config: GossipRateConfig, now: Instant) -> Self {
        Self {
            config,
            rate: config.max_bytes_per_second,
            credit: config.burst_bytes as i128,
            remainder: 0,
            created: now,
            refilled: now,
            adjusted: now,
            last_sample: None,
            smoothed_rtt: None,
            baseline_rtt: None,
            rtt_congested: false,
            slow_write: false,
            healthy_write: false,
            high_load: false,
            local_feedback_at: now,
        }
    }

    fn expire_local_feedback(&mut self, now: Instant) {
        if now.saturating_duration_since(self.local_feedback_at) > SAMPLE_TTL {
            self.high_load = false;
            self.slow_write = false;
            self.healthy_write = false;
        }
        self.local_feedback_at = self.local_feedback_at.max(now);
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.refilled);
        if now < self.refilled {
            return;
        }
        self.refilled = now;
        let numerator = elapsed
            .as_nanos()
            .saturating_mul(u128::from(self.rate))
            .saturating_add(self.remainder);
        let bytes = (numerator / 1_000_000_000).min(i128::MAX as u128) as i128;
        self.credit = self
            .credit
            .saturating_add(bytes)
            .min(self.config.burst_bytes as i128);
        self.remainder = if self.credit == self.config.burst_bytes as i128 {
            0
        } else {
            numerator % 1_000_000_000
        };
    }

    fn sample(&mut self, now: Instant, sample: Option<ControlTiming>) {
        let Some(sample) = sample else {
            return;
        };
        if sample.round_trip.is_zero()
            || sample.started_at < self.created
            || sample.observed_at < sample.started_at
            || self
                .last_sample
                .is_some_and(|last| last >= sample.observed_at)
            || !now
                .checked_duration_since(sample.observed_at)
                .is_some_and(|age| age <= SAMPLE_TTL)
        {
            return;
        }
        if self
            .last_sample
            .is_none_or(|last| sample.observed_at.saturating_duration_since(last) > SAMPLE_TTL)
        {
            self.smoothed_rtt = None;
            self.baseline_rtt = None;
            self.rtt_congested = false;
        }
        self.last_sample = Some(sample.observed_at);
        self.baseline_rtt = Some(
            self.baseline_rtt
                .map_or(sample.round_trip, |base| base.min(sample.round_trip)),
        );
        self.smoothed_rtt = Some(self.smoothed_rtt.map_or(sample.round_trip, |old| {
            let nanos = (old
                .as_nanos()
                .saturating_mul(7)
                .saturating_add(sample.round_trip.as_nanos()))
                / 8;
            Duration::from_nanos(nanos.min(u64::MAX as u128) as u64)
        }));
        if let (Some(base), Some(smoothed)) = (self.baseline_rtt, self.smoothed_rtt) {
            // Enter on a smoothed rise, leave on new healthy evidence rather
            // than waiting for an old spike to decay through many probe periods.
            let low = base.saturating_add(base / 2).max(Duration::from_millis(5));
            let high = base.saturating_mul(2).max(Duration::from_millis(5));
            if sample.round_trip <= low {
                self.rtt_congested = false;
            } else if smoothed > high {
                self.rtt_congested = true;
            }
        }
    }

    fn fresh_rtt(&self, now: Instant) -> Option<Duration> {
        self.last_sample
            .filter(|last| {
                now.checked_duration_since(*last)
                    .is_some_and(|age| age <= SAMPLE_TTL)
            })
            .and(self.smoothed_rtt)
    }

    fn adjust(&mut self, now: Instant) {
        if now.saturating_duration_since(self.adjusted) < WINDOW {
            return;
        }
        self.adjusted = now;
        let fresh = self.fresh_rtt(now);
        let inflated = fresh.is_some() && self.rtt_congested;
        if self.slow_write || inflated {
            self.rate = (self.rate / 2).max(self.config.min_bytes_per_second);
        } else if self.high_load && (fresh.is_some() || self.healthy_write) {
            self.rate = self
                .rate
                .saturating_add((self.config.max_bytes_per_second / 16).max(1))
                .min(self.config.max_bytes_per_second);
        }
        self.slow_write = false;
        self.healthy_write = false;
        self.high_load = false;
    }

    fn admit(
        &mut self,
        bytes: usize,
        now: Instant,
        load: Load,
        sample: Option<ControlTiming>,
    ) -> Decision {
        self.refill(now);
        self.expire_local_feedback(now);
        self.high_load |= load.high();
        self.sample(now, sample);
        self.adjust(now);
        // A validated frame larger than the burst may overdraw once. Debt is
        // bounded by one frame; it cannot starve forever or bypass later pacing.
        let required = bytes.min(self.config.burst_bytes) as i128;
        let allowed = bytes > 0 && self.credit >= required;
        if allowed {
            self.credit -= bytes as i128;
        }
        Decision {
            allowed,
            bytes_per_second: self.rate,
            round_trip: self.fresh_rtt(now),
        }
    }

    fn sent(&mut self, now: Instant, service: Option<Duration>) {
        self.refill(now);
        self.expire_local_feedback(now);
        let threshold = self
            .fresh_rtt(now)
            .map_or(Duration::from_millis(20), |rtt| {
                rtt.saturating_mul(2).max(Duration::from_millis(20))
            });
        if service.is_none_or(|elapsed| elapsed > threshold) {
            self.slow_write = true;
        } else {
            self.healthy_write = true;
        }
        self.adjust(now);
    }
}

struct PeerRate {
    generation: ConnectionGeneration,
    rate: Rate,
}

/// Shared by local Push and remote forwarding; never held across I/O or sleeps.
pub(super) struct Pacer {
    config: GossipRateConfig,
    limit: usize,
    peers: HashMap<String, PeerRate>,
}
impl Pacer {
    pub(super) fn new(config: GossipRateConfig, limit: usize) -> Self {
        Self {
            config,
            limit,
            peers: HashMap::new(),
        }
    }

    pub(super) fn admit(
        &mut self,
        addr: &str,
        generation: ConnectionGeneration,
        bytes: usize,
        now: Instant,
        load: Load,
        sample: Option<ControlTiming>,
    ) -> Decision {
        self.peers.retain(|_, peer| !peer.generation.is_closed());
        if self
            .peers
            .get(addr)
            .is_some_and(|peer| peer.generation != generation)
        {
            self.peers.remove(addr);
        }
        if generation.is_closed()
            || !self.peers.contains_key(addr) && self.peers.len() >= self.limit
        {
            return Decision {
                allowed: false,
                bytes_per_second: 0,
                round_trip: None,
            };
        }
        self.peers
            .entry(addr.into())
            .or_insert_with(|| {
                let mut rate = Rate::new(self.config, now);
                rate.created = generation.created_at();
                PeerRate { generation, rate }
            })
            .rate
            .admit(bytes, now, load, sample)
    }

    pub(super) fn sent(
        &mut self,
        addr: &str,
        generation: &ConnectionGeneration,
        now: Instant,
        service: Option<Duration>,
    ) {
        if let Some(peer) = self
            .peers
            .get_mut(addr)
            .filter(|peer| &peer.generation == generation && !generation.is_closed())
        {
            peer.rate.sent(now, service);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config() -> GossipRateConfig {
        GossipRateConfig {
            min_bytes_per_second: 10,
            max_bytes_per_second: 160,
            burst_bytes: 100,
        }
    }
    fn load() -> Load {
        Load {
            queued: 80,
            capacity: 100,
        }
    }
    fn sample(now: Instant, ms: u64) -> Option<ControlTiming> {
        Some(ControlTiming {
            started_at: now,
            observed_at: now,
            round_trip: Duration::from_millis(ms),
        })
    }

    #[tokio::test]
    async fn peers_share_attempt_budgets_but_not_congestion_and_new_generations_reset() {
        use nx_net::{Node, NodeConfig};
        use nx_sync::{HistoryMessage, NodeId};
        let client = Node::new(NodeConfig::new(NodeId::new("client"), "127.0.0.1:0"));
        let a = Node::new(NodeConfig::new(NodeId::new("a"), "127.0.0.1:0"));
        let b = Node::new(NodeConfig::new(NodeId::new("b"), "127.0.0.1:0"));
        let addr_a = a.start_listener().await.unwrap().to_string();
        let addr_b = b.start_listener().await.unwrap().to_string();
        client.connect_to_peer(&addr_a).await.unwrap();
        client.connect_to_peer(&addr_b).await.unwrap();
        let message = || HistoryMessage::SummaryRequest { after_origin: None };
        let ga = client
            .prepare_history_to_addr(&addr_a, message())
            .await
            .unwrap()
            .generation();
        let gb = client
            .prepare_history_to_addr(&addr_b, message())
            .await
            .unwrap()
            .generation();
        let now = Instant::now();
        let mut pacer = Pacer::new(config(), 2);
        assert!(
            pacer
                .admit(&addr_a, ga.clone(), 100, now, load(), None)
                .allowed
        );
        assert!(
            !pacer
                .admit(&addr_a, ga.clone(), 1, now, load(), None)
                .allowed
        );
        assert!(
            pacer
                .admit(&addr_b, gb.clone(), 100, now, load(), None)
                .allowed
        );
        pacer.sent(&addr_a, &ga, now + WINDOW, None);
        assert_eq!(pacer.peers[&addr_a].rate.rate, 80);
        assert_eq!(pacer.peers[&addr_b].rate.rate, 160);
        client.disconnect_peer(&addr_a, &NodeId::new("a")).await;
        client.connect_to_peer(&addr_a).await.unwrap();
        let new = client
            .prepare_history_to_addr(&addr_a, message())
            .await
            .unwrap()
            .generation();
        assert!(ga != new);
        assert!(
            pacer
                .admit(&addr_a, new.clone(), 100, now + WINDOW, load(), None)
                .allowed
        );
        pacer.sent(&addr_a, &ga, now + WINDOW * 2, None);
        assert_eq!(pacer.peers[&addr_a].rate.rate, 160);
        assert!(pacer.peers.len() <= 2);
        client.shutdown().await;
        a.shutdown().await;
        b.shutdown().await;
    }

    #[test]
    fn an_idle_gap_does_not_reuse_old_pressure_or_success_to_accelerate() {
        let now = Instant::now();
        let mut rate = Rate::new(config(), now);
        rate.sent(now, None);
        rate.admit(1, now + WINDOW, load(), None);
        assert_eq!(rate.rate, 80);
        rate.admit(1, now + WINDOW, load(), None);
        rate.sent(now + WINDOW, Some(Duration::from_millis(1)));
        rate.admit(
            1,
            now + SAMPLE_TTL * 2,
            Load {
                queued: 0,
                capacity: 100,
            },
            None,
        );
        assert_eq!(rate.rate, 80);
    }

    #[test]
    fn sparse_forwarding_cannot_erase_local_pressure_in_the_shared_window() {
        let now = Instant::now();
        let mut rate = Rate::new(config(), now);
        rate.sent(now, Some(Duration::from_millis(100)));
        rate.admit(1, now + WINDOW, load(), None);
        assert_eq!(rate.rate, 80);
        rate.admit(1, now + WINDOW, load(), None);
        rate.admit(
            1,
            now + WINDOW,
            Load {
                queued: 0,
                capacity: 100,
            },
            None,
        );
        rate.sent(now + WINDOW, Some(Duration::from_millis(1)));
        rate.admit(
            1,
            now + WINDOW * 2,
            Load {
                queued: 0,
                capacity: 100,
            },
            None,
        );
        assert_eq!(rate.rate, 90);
    }

    #[test]
    fn hysteresis_needs_new_healthy_evidence_and_idle_queues_do_not_accelerate() {
        let now = Instant::now();
        let mut rate = Rate::new(config(), now);
        rate.admit(1, now, load(), sample(now, 10));
        rate.admit(1, now + WINDOW, load(), sample(now + WINDOW, 200));
        assert!(rate.rtt_congested);
        rate.admit(1, now + WINDOW * 2, load(), sample(now + WINDOW * 2, 16));
        assert!(rate.rtt_congested);
        let before = rate.rate;
        rate.admit(1, now + WINDOW * 3, load(), sample(now + WINDOW * 3, 10));
        assert!(!rate.rtt_congested);
        assert!(rate.smoothed_rtt.unwrap() > Duration::from_millis(20));
        assert_eq!(rate.rate, before + 10);
        let before = rate.rate;
        rate.admit(
            1,
            now + WINDOW * 4,
            Load {
                queued: 0,
                capacity: 100,
            },
            sample(now + WINDOW * 4, 10),
        );
        assert_eq!(rate.rate, before);
    }

    #[test]
    fn budget_is_shared_by_attempts_and_has_bounded_large_frame_debt() {
        let now = Instant::now();
        let mut rate = Rate::new(config(), now);
        assert!(rate.admit(60, now, load(), None).allowed);
        assert!(!rate.admit(60, now, load(), None).allowed);
        assert!(rate.admit(40, now, load(), None).allowed);
        assert!(!rate.admit(1, now, load(), None).allowed);
        let later = now + Duration::from_secs(100);
        assert!(rate.admit(300, later, load(), None).allowed);
        assert!(!rate.admit(1, later, load(), None).allowed);
        assert!(
            rate.admit(100, later + Duration::from_secs(2), load(), None)
                .allowed
        );
    }

    #[test]
    fn increased_rtt_reduces_rate_then_healthy_load_recovers_gradually() {
        let now = Instant::now();
        let mut rate = Rate::new(config(), now);
        rate.admit(1, now, load(), sample(now, 10));
        let later = now + WINDOW;
        let congested = rate.admit(1, later, load(), sample(later, 200));
        assert_eq!(congested.bytes_per_second, 80);
        assert_eq!(
            rate.admit(1, later, load(), sample(later, 200))
                .bytes_per_second,
            80
        );
        for index in 2..10 {
            let time = now + WINDOW * index;
            rate.admit(1, time, load(), sample(time, 200));
        }
        assert_eq!(rate.rate, config().min_bytes_per_second);
        let mut recovered = false;
        for index in 10..70 {
            let time = now + WINDOW * index;
            let previous = rate.rate;
            rate.admit(1, time, load(), sample(time, 10));
            assert!(rate.rate <= previous.saturating_add(10));
            recovered |= rate.rate > config().min_bytes_per_second;
        }
        assert!(recovered);
        assert_eq!(rate.rate, config().max_bytes_per_second);
    }

    #[test]
    fn stale_future_zero_or_previous_connection_samples_do_not_change_rtt() {
        let now = Instant::now();
        let mut rate = Rate::new(config(), now);
        for invalid in [
            sample(now - WINDOW, 100),
            sample(now + WINDOW, 100),
            sample(now, 0),
        ] {
            assert!(rate.admit(1, now, load(), invalid).round_trip.is_none());
        }
        let old_request = Some(ControlTiming {
            started_at: now - WINDOW,
            observed_at: now,
            round_trip: WINDOW,
        });
        assert!(rate.admit(1, now, load(), old_request).round_trip.is_none());
        rate.admit(1, now, load(), sample(now, 10));
        let later = now + SAMPLE_TTL + WINDOW;
        assert!(
            rate.admit(1, later, load(), sample(now, 200))
                .round_trip
                .is_none()
        );
    }

    #[test]
    fn queue_pressure_alone_does_not_reduce_rate_or_invent_rtt() {
        let now = Instant::now();
        let mut rate = Rate::new(config(), now);
        for index in 0..10 {
            let decision = rate.admit(1, now + WINDOW * index, load(), None);
            assert_eq!(decision.bytes_per_second, 160);
            assert!(decision.round_trip.is_none());
        }
        rate.sent(now + WINDOW * 10, Some(Duration::from_millis(200)));
        assert_eq!(rate.rate, 80);
        // Healthy writes only restore rate while the new window still has demand.
        rate.admit(1, now + WINDOW * 10, load(), None);
        rate.sent(now + WINDOW * 11, Some(Duration::from_millis(1)));
        assert_eq!(rate.rate, 90);
    }

    #[test]
    fn low_rates_keep_fractional_credit_and_idle_time_does_not_accumulate_bursts() {
        let now = Instant::now();
        let mut rate = Rate::new(config(), now);
        assert!(rate.admit(100, now, load(), None).allowed);
        for index in 1..=1000 {
            rate.refill(now + Duration::from_micros(index));
        }
        assert!(rate.credit == 0);
        assert!(rate.remainder > 0);
        rate.refill(now + Duration::from_secs(1000));
        assert_eq!(rate.credit, 100);
        rate.refill(now);
        assert_eq!(rate.credit, 100);
    }
}
