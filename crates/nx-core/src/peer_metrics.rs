use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy)]
pub(crate) enum Counter {
    ProbeDirect,
    ProbeIndirect,
    ProbeSuccess,
    ProbeFailed,
    ProbeDegraded,
    Suspect,
    Refuted,
    DeclaredDead,
    LearnedDead,
    Left,
    DialConnected,
    DialFailed,
    DialFatal,
    DialLimited,
    RetryFailure,
    RetryLimited,
    HistoryUnavailable,
    BaselineRequested,
    BaselineAccepted,
    BaselineRejected,
    RecoveryError,
}

const COUNTERS: [(&str, &str); 21] = [
    ("membership_probe_attempts", "route=\"direct\""),
    ("membership_probe_attempts", "route=\"indirect\""),
    ("membership_probe_results", "outcome=\"success\""),
    ("membership_probe_results", "outcome=\"failed\""),
    ("membership_probe_results", "outcome=\"local_degradation\""),
    ("membership_transitions", "transition=\"suspect\""),
    ("membership_transitions", "transition=\"refuted\""),
    ("membership_transitions", "transition=\"declared_dead\""),
    ("membership_transitions", "transition=\"learned_dead\""),
    ("membership_transitions", "transition=\"left\""),
    ("peer_dial_attempts", "outcome=\"connected\""),
    ("peer_dial_attempts", "outcome=\"failed\""),
    ("peer_dial_attempts", "outcome=\"fatal\""),
    ("peer_dial_attempts", "outcome=\"rate_limited\""),
    ("peer_retries_scheduled", "reason=\"failure\""),
    ("peer_retries_scheduled", "reason=\"rate_limited\""),
    ("recovery_events", "event=\"history_unavailable\""),
    ("recovery_events", "event=\"baseline_requested\""),
    ("recovery_events", "event=\"baseline_accepted\""),
    ("recovery_events", "event=\"baseline_rejected\""),
    ("recovery_events", "event=\"error\""),
];

#[derive(Clone, Copy)]
pub(crate) enum Gauge {
    LocalItems,
    LocalBytes,
    ForwardItems,
    ForwardBytes,
    CandidateItems,
    CandidateCapacity,
    ControlIngress,
    ControlCapacity,
    MembershipWrites,
    MembershipWriteCapacity,
    PendingOrigins,
    BaselineUnconfirmed,
    FailedOrigins,
    KnownCompleteOrigins,
    GapPositions,
}

const GAUGES: [(&str, &str); 15] = [
    ("queue_items", "queue=\"local\""),
    ("queue_bytes", "queue=\"local\""),
    ("queue_items", "queue=\"forward\""),
    ("queue_bytes", "queue=\"forward\""),
    ("queue_items", "queue=\"candidates\""),
    ("queue_capacity_items", "queue=\"candidates\""),
    ("queue_items", "queue=\"control_ingress\""),
    ("queue_capacity_items", "queue=\"control_ingress\""),
    ("queue_items", "queue=\"membership_writes\""),
    ("queue_capacity_items", "queue=\"membership_writes\""),
    ("recovery_origins", "state=\"pending\""),
    ("recovery_origins", "state=\"baseline_unconfirmed\""),
    ("recovery_origins", "state=\"source_rejected\""),
    ("recovery_origins", "state=\"no_known_gap\""),
    ("recovery_gap_positions", ""),
];

#[derive(Default)]
pub(crate) struct PeerMetrics {
    counters: [AtomicU64; COUNTERS.len()],
    gauges: [AtomicU64; GAUGES.len()],
    queue_slots: [AtomicU64; 2],
    queue_bytes: [AtomicU64; 2],
    queue_rejected: [AtomicU64; 6],
    fanout: [[AtomicU64; 9]; 4],
}

impl PeerMetrics {
    pub(crate) fn increment(&self, counter: Counter) {
        self.counters[counter as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn set(&self, gauge: Gauge, value: u64) {
        self.gauges[gauge as usize].store(value, Ordering::Relaxed);
    }

    pub(crate) fn queue_capacity(&self, forward: bool, slots: usize, bytes: usize) {
        let index = usize::from(forward);
        self.queue_slots[index].store(slots as u64, Ordering::Relaxed);
        self.queue_bytes[index].store(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn queue_change(&self, forward: bool, bytes: usize, add: bool) {
        let index = if forward {
            Gauge::ForwardItems
        } else {
            Gauge::LocalItems
        } as usize;
        let byte_index = if forward {
            Gauge::ForwardBytes
        } else {
            Gauge::LocalBytes
        } as usize;
        if add {
            self.gauges[index].fetch_add(1, Ordering::Relaxed);
            self.gauges[byte_index].fetch_add(bytes as u64, Ordering::Relaxed);
        } else {
            self.gauges[index].fetch_sub(1, Ordering::Relaxed);
            self.gauges[byte_index].fetch_sub(bytes as u64, Ordering::Relaxed);
        }
    }

    pub(crate) fn queue_reject(&self, forward: bool, reason: usize) {
        self.queue_rejected[usize::from(forward) * 3 + reason].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn fanout(&self, forward: bool, attempted: bool, count: usize) {
        let index = usize::from(forward) * 2 + usize::from(attempted);
        for (bucket, bound) in [0, 1, 2, 4, 8, 16, 32].into_iter().enumerate() {
            if count <= bound {
                self.fanout[index][bucket].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.fanout[index][7].fetch_add(1, Ordering::Relaxed);
        self.fanout[index][8].fetch_add(count as u64, Ordering::Relaxed);
    }

    pub(crate) fn render(&self, output: &mut String) {
        use std::fmt::Write;
        let mut previous = "";
        for ((name, labels), value) in COUNTERS.iter().zip(&self.counters) {
            if *name != previous {
                let _ = writeln!(
                    output,
                    "# HELP numax_{name}_total Bounded runtime events\n# TYPE numax_{name}_total counter"
                );
                previous = name;
            }
            let _ = writeln!(
                output,
                "numax_{name}_total{{{labels}}} {}",
                value.load(Ordering::Relaxed)
            );
        }
        let mut declared = std::collections::BTreeSet::new();
        for ((name, labels), value) in GAUGES.iter().zip(&self.gauges) {
            if declared.insert(name) {
                let _ = writeln!(
                    output,
                    "# HELP numax_{name} Current bounded runtime observation\n# TYPE numax_{name} gauge"
                );
            }
            let _ = writeln!(
                output,
                "numax_{name}{{{labels}}} {}",
                value.load(Ordering::Relaxed)
            );
        }
        output.push_str("# HELP numax_queue_capacity_bytes Queue byte budget\n# TYPE numax_queue_capacity_bytes gauge\n# HELP numax_queue_rejections_total Rejected volatile enqueue attempts\n# TYPE numax_queue_rejections_total counter\n");
        for (index, queue) in ["local", "forward"].iter().enumerate() {
            let _ = writeln!(
                output,
                "numax_queue_capacity_items{{queue=\"{queue}\"}} {}",
                self.queue_slots[index].load(Ordering::Relaxed)
            );
            let _ = writeln!(
                output,
                "numax_queue_capacity_bytes{{queue=\"{queue}\"}} {}",
                self.queue_bytes[index].load(Ordering::Relaxed)
            );
            for (reason, label) in ["slots", "bytes", "closed"].iter().enumerate() {
                let _ = writeln!(
                    output,
                    "numax_queue_rejections_total{{queue=\"{queue}\",reason=\"{label}\"}} {}",
                    self.queue_rejected[index * 3 + reason].load(Ordering::Relaxed)
                );
            }
        }
        output.push_str("# HELP numax_gossip_fanout Recipients per data batch\n# TYPE numax_gossip_fanout histogram\n");
        for (index, values) in self.fanout.iter().enumerate() {
            let path = if index < 2 { "local" } else { "forward" };
            let stage = if index % 2 == 0 {
                "selected"
            } else {
                "attempted"
            };
            for (bucket, bound) in ["0", "1", "2", "4", "8", "16", "32", "+Inf"]
                .iter()
                .enumerate()
            {
                let _ = writeln!(
                    output,
                    "numax_gossip_fanout_bucket{{path=\"{path}\",stage=\"{stage}\",le=\"{bound}\"}} {}",
                    values[bucket].load(Ordering::Relaxed)
                );
            }
            let _ = writeln!(
                output,
                "numax_gossip_fanout_count{{path=\"{path}\",stage=\"{stage}\"}} {}",
                values[7].load(Ordering::Relaxed)
            );
            let _ = writeln!(
                output,
                "numax_gossip_fanout_sum{{path=\"{path}\",stage=\"{stage}\"}} {}",
                values[8].load(Ordering::Relaxed)
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_series_are_fixed_and_queue_accounting_is_balanced() {
        let metrics = PeerMetrics::default();
        let mut before = String::new();
        metrics.render(&mut before);
        for _ in 0..10_000 {
            metrics.increment(Counter::DialFailed);
            metrics.queue_change(false, 23, true);
            metrics.queue_change(false, 23, false);
            metrics.fanout(false, false, 3);
        }
        let mut after = String::new();
        metrics.render(&mut after);
        let names = |text: &str| {
            text.lines()
                .filter(|line| !line.starts_with('#'))
                .map(|line| line.rsplit_once(' ').unwrap().0.to_owned())
                .collect::<Vec<_>>()
        };
        assert_eq!(names(&before), names(&after));
        assert!(after.contains("numax_queue_items{queue=\"local\"} 0"));
        assert!(after.contains("numax_queue_bytes{queue=\"local\"} 0"));
        assert!(after.contains("numax_peer_dial_attempts_total{outcome=\"failed\"} 10000"));
        assert!(after.contains(
            "numax_gossip_fanout_bucket{path=\"local\",stage=\"selected\",le=\"4\"} 10000"
        ));
    }
}
