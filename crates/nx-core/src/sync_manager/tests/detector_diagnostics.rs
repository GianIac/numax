use super::*;
use serde_json::{Value, json};
use std::io::{Seek, Write};
use std::sync::Mutex;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

const STAGES: [(&str, &str); 5] = [
    ("registry_wait", "local registry locks acquired"),
    ("storage_dispatch", "local storage job started"),
    ("lease_wait", "local storage lease acquired"),
    ("batch", "local storage batch completed"),
    ("flush", "local storage flush completed"),
];
const REMOTE_STAGES: [(&str, &str); 7] = [
    ("registry_wait", "remote registry locks acquired"),
    ("storage_dispatch", "remote storage job started"),
    ("preparation", "remote batch prepared"),
    ("batch", "remote storage batch completed"),
    ("flush", "remote storage flush completed"),
    ("publication", "remote registries published"),
    ("push_summary", "remote push summaries recorded"),
];
const REPAIR_TRACE_EVENT_LIMIT: usize = 64;
const TIMELINE_EVENT_LIMIT: usize = 4096;
const CRITICAL_SPAN_LIMIT: usize = 64;
const CRITICAL_SNAPSHOT_BYTES: usize = 32 * 1024;
const EVENT_LOOP_KINDS: [&str; 9] = [
    "push",
    "summary",
    "summary_request",
    "range_request",
    "replay",
    "unavailable",
    "baseline",
    "peer_lifecycle",
    "legacy",
];
const HISTORY_BATCH_KINDS: [&str; 3] = ["push_only", "replay_only", "mixed"];
const HISTORY_BATCH_STOP_REASONS: [&str; 17] = [
    "queue_empty",
    "message_limit",
    "operation_budget",
    "byte_budget",
    "coalesce_timeout",
    "control_priority",
    "control_summary",
    "control_summary_request",
    "control_range_request",
    "control_unavailable",
    "control_baseline",
    "empty_push",
    "empty_replay",
    "invalid_data_envelope",
    "legacy_event",
    "other_event",
    "data_channel_closed",
];
const HISTORY_BATCH_COUNTER_LIMIT: usize =
    HISTORY_BATCH_KINDS.len() * 6 + HISTORY_BATCH_STOP_REASONS.len();

#[derive(Default)]
struct Timing {
    count: AtomicU64,
    total_us: AtomicU64,
    max_us: AtomicU64,
    failed: AtomicU64,
}

#[derive(Clone, Copy, Default)]
struct TimingSnapshot {
    count: u64,
    total_us: u64,
    max_us: u64,
    failed: u64,
}

#[derive(Clone, Copy, Default)]
struct RecoveryCount {
    events: u64,
    items: u64,
}

#[derive(Clone, Copy)]
struct EventLoopQueues {
    data_before: u64,
    data_after: u64,
    priority_before: u64,
    priority_after: u64,
}

#[derive(Clone, Copy, Default)]
struct EventLoopTiming {
    count: u64,
    total_us: u64,
    max_us: u64,
    duration_buckets: [u64; 4],
    queue_before_total: u64,
    queue_after_total: u64,
    max_queue_before: u64,
    max_queue_after: u64,
    data_queue_before_total: u64,
    data_queue_after_total: u64,
    priority_queue_before_total: u64,
    priority_queue_after_total: u64,
    max_data_queue_before: u64,
    max_data_queue_after: u64,
    max_priority_queue_before: u64,
    max_priority_queue_after: u64,
}

#[derive(Default)]
struct EventLoopAggregate {
    count: u64,
    total_us: u64,
    duration_buckets: [u64; 4],
    queue_before_total: u64,
    queue_after_total: u64,
    max_duration_us: u64,
    max_queue_before: u64,
    max_queue_after: u64,
    data_queue_before_total: u64,
    data_queue_after_total: u64,
    priority_queue_before_total: u64,
    priority_queue_after_total: u64,
    max_data_queue_before: u64,
    max_data_queue_after: u64,
    max_priority_queue_before: u64,
    max_priority_queue_after: u64,
}

impl EventLoopTiming {
    fn record(&mut self, elapsed_us: u64, queues: EventLoopQueues) {
        let queue_before = queues.data_before.saturating_add(queues.priority_before);
        let queue_after = queues.data_after.saturating_add(queues.priority_after);
        self.count = self.count.saturating_add(1);
        self.total_us = self.total_us.saturating_add(elapsed_us);
        self.max_us = self.max_us.max(elapsed_us);
        let bucket = if elapsed_us < 100_000 {
            0
        } else if elapsed_us < 500_000 {
            1
        } else if elapsed_us < 2_000_000 {
            2
        } else {
            3
        };
        self.duration_buckets[bucket] = self.duration_buckets[bucket].saturating_add(1);
        self.queue_before_total = self.queue_before_total.saturating_add(queue_before);
        self.queue_after_total = self.queue_after_total.saturating_add(queue_after);
        self.max_queue_before = self.max_queue_before.max(queue_before);
        self.max_queue_after = self.max_queue_after.max(queue_after);
        self.data_queue_before_total = self
            .data_queue_before_total
            .saturating_add(queues.data_before);
        self.data_queue_after_total = self
            .data_queue_after_total
            .saturating_add(queues.data_after);
        self.priority_queue_before_total = self
            .priority_queue_before_total
            .saturating_add(queues.priority_before);
        self.priority_queue_after_total = self
            .priority_queue_after_total
            .saturating_add(queues.priority_after);
        self.max_data_queue_before = self.max_data_queue_before.max(queues.data_before);
        self.max_data_queue_after = self.max_data_queue_after.max(queues.data_after);
        self.max_priority_queue_before = self.max_priority_queue_before.max(queues.priority_before);
        self.max_priority_queue_after = self.max_priority_queue_after.max(queues.priority_after);
    }
}

#[derive(Clone, Default)]
pub(super) struct Diagnostics {
    timeline: Option<Arc<RecoveryTimeline>>,
    critical: Option<Arc<Mutex<CriticalPath>>>,
    timings: Arc<[Timing; STAGES.len()]>,
    remote_timings: Arc<Mutex<[TimingSnapshot; REMOTE_STAGES.len()]>>,
    events: Arc<Mutex<BTreeMap<String, u64>>>,
    history_batches: Arc<Mutex<BTreeMap<String, u64>>>,
    network_events: Arc<Mutex<BTreeMap<String, u64>>>,
    connection_errors: Arc<Mutex<BTreeMap<String, u64>>>,
    repair_trace_events: Arc<Mutex<BTreeMap<String, RecoveryCount>>>,
    event_loop: Arc<Mutex<BTreeMap<String, EventLoopTiming>>>,
    event_errors: Arc<AtomicU64>,
}

#[derive(Default)]
struct CriticalPath {
    active: BTreeMap<(u64, String, String), Value>,
    completed: std::collections::VecDeque<Value>,
    overwritten: u64,
    unix_anchor_us: u64,
}

impl CriticalPath {
    fn record(&mut self, fields: serde_json::Map<String, Value>) -> anyhow::Result<()> {
        let number = |key| {
            fields
                .get(key)
                .and_then(Value::as_u64)
                .with_context(|| format!("critical span missing {key}"))
        };
        let text = |key| {
            fields
                .get(key)
                .and_then(Value::as_str)
                .with_context(|| format!("critical span missing {key}"))
        };
        let id = number("job_id")?;
        let kind = text("span_kind")?.to_owned();
        let resource = text("resource")?.to_owned();
        let time = number("event_us")?;
        ensure!(
            matches!(kind.as_str(), "job" | "wait" | "hold" | "phase"),
            "invalid critical span kind"
        );
        ensure!(
            resource.len() <= 32 && text("thread_id")?.len() <= 64,
            "critical span metadata exceeds string bound"
        );
        ensure!(
            matches!(
                text("job_kind")?,
                "local" | "remote" | "baseline_export" | "baseline_install"
            ) && matches!(
                text("status")?,
                "open" | "acquired" | "started" | "success" | "error" | "cancelled"
            ),
            "invalid critical job metadata"
        );
        let key = (id, kind.clone(), resource);
        match text("critical_event")? {
            "begin" => {
                ensure!(
                    !self.active.contains_key(&key),
                    "duplicate critical span begin"
                );
                ensure!(
                    self.active.len() < CRITICAL_SPAN_LIMIT,
                    "critical active span budget exhausted"
                );
                if self.active.len() + self.completed.len() == CRITICAL_SPAN_LIMIT {
                    self.completed
                        .pop_front()
                        .context("critical completed eviction missing")?;
                    self.overwritten = self
                        .overwritten
                        .checked_add(1)
                        .context("critical overwrite count overflow")?;
                }
                let mut record = fields.clone();
                record.remove("critical_event");
                record.remove("critical_trace");
                record.remove("event_us");
                record.remove("release_begin_us");
                record.insert("start_us".into(), json!(time));
                self.active.insert(key, Value::Object(record));
            }
            "end" => {
                let mut record = self
                    .active
                    .remove(&key)
                    .context("critical span end without begin")?;
                ensure!(
                    time >= record["start_us"]
                        .as_u64()
                        .context("critical start missing")?,
                    "critical span time regressed"
                );
                record["end_us"] = json!(time);
                record["status"] = json!(text("status")?);
                record["end_thread_id"] = json!(text("thread_id")?);
                if kind == "hold" {
                    let lower = number("release_begin_us")?;
                    ensure!(lower <= time, "critical release interval regressed");
                    record["release_begin_us"] = json!(lower);
                    record["release_confirmed"] = fields
                        .get("release_confirmed")
                        .and_then(Value::as_bool)
                        .context("critical release confirmation missing")?
                        .into();
                }
                self.completed.push_back(record);
            }
            _ => anyhow::bail!("invalid critical span event"),
        }
        Ok(())
    }

    fn snapshot(&self, full: bool) -> anyhow::Result<Value> {
        let snapshot = json!({
            "captured_us": u64::try_from(crate::sync_manager::storage::critical_trace::clock().elapsed().as_micros())?,
            "unix_anchor_us": self.unix_anchor_us,
            "active": self.active.values().collect::<Vec<_>>(),
            "completed": if full { self.completed.iter().collect::<Vec<_>>() } else { Vec::new() },
            "completed_count": self.completed.len(),
            "overwritten_spans": self.overwritten, "span_limit": CRITICAL_SPAN_LIMIT,
            "interpretation": "source monotonic timestamps; confirmed owned-job release lies between release_begin_us and end_us, not at logger delivery; cancelled pre-dispatch release timing is unconfirmed; completed history intentionally rolls",
        });
        ensure!(
            serde_json::to_vec(&snapshot)?.len() <= CRITICAL_SNAPSHOT_BYTES,
            "critical snapshot exceeded its 32 KiB bound"
        );
        Ok(snapshot)
    }
}

#[test]
fn critical_path_ring_is_bounded_and_reports_overwrites() {
    let fields = |id, event, time| {
        serde_json::from_value(json!({
            "job_id": id, "job_kind": "local", "span_kind": "job", "resource": "job",
            "event_us": time, "critical_event": event, "status": "success", "thread_id": "test",
        }))
        .unwrap()
    };
    let mut ring = CriticalPath::default();
    for id in 0..65u64 {
        ring.record(fields(id, "begin", id * 2)).unwrap();
        ring.record(fields(id, "end", id * 2 + 1)).unwrap();
    }
    assert_eq!(ring.completed.len(), 64);
    assert_eq!(ring.overwritten, 1);
    assert_eq!(ring.snapshot(false).unwrap()["completed"], json!([]));
    assert_eq!(
        ring.snapshot(true).unwrap()["completed"]
            .as_array()
            .unwrap()
            .len(),
        64
    );
    assert!(ring.record(fields(66, "end", 140)).is_err());
    ring.record(fields(66, "begin", 140)).unwrap();
    assert!(ring.record(fields(66, "begin", 141)).is_err());
    assert!(ring.record(fields(66, "end", 139)).is_err());
    let mut active = CriticalPath::default();
    for id in 0..64u64 {
        active.record(fields(id, "begin", id)).unwrap();
    }
    assert!(active.record(fields(64, "begin", 64)).is_err());
    assert_eq!(active.active.len(), 64);
}

struct RecoveryTimeline {
    origin: String,
    started: StdInstant,
    unix_anchor_us: u64,
    buffer: Mutex<TimelineBuffer>,
}

struct TimelineBuffer {
    file: std::fs::File,
    events: Vec<Value>,
    dropped: u64,
}

impl RecoveryTimeline {
    fn record(&self, mut fields: serde_json::Map<String, Value>) -> anyhow::Result<()> {
        let captured_us = u64::try_from(self.started.elapsed().as_micros())?;
        let mut buffer = self
            .buffer
            .lock()
            .map_err(|_| anyhow::anyhow!("recovery timeline lock poisoned"))?;
        if buffer.events.len() == TIMELINE_EVENT_LIMIT {
            buffer.dropped = buffer
                .dropped
                .checked_add(1)
                .context("recovery timeline dropped counter overflow")?;
        } else {
            fields.insert("captured_us".into(), json!(captured_us));
            buffer.events.push(Value::Object(fields));
        }
        Ok(())
    }
}

fn write_timeline_snapshot(writer: &mut impl Write, report: &Value) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec(report).context("cannot encode recovery timeline")?;
    writer
        .write_all(&bytes)
        .context("cannot save recovery timeline")
}

#[derive(Default)]
struct TimelineFields(serde_json::Map<String, Value>);

impl TimelineFields {
    fn accepts(field: &Field) -> bool {
        matches!(
            field.name(),
            "recovery_trace_stage"
                | "kind"
                | "peer"
                | "origin"
                | "request_id"
                | "start"
                | "end"
                | "decision"
                | "elapsed_us"
                | "success"
                | "event_handler_kind"
                | "message"
                | "envelopes"
                | "operations"
                | "critical_trace"
                | "critical_trace_error"
                | "critical_event"
                | "job_id"
                | "job_kind"
                | "span_kind"
                | "resource"
                | "event_us"
                | "release_begin_us"
                | "status"
                | "thread_id"
                | "release_confirmed"
                | "critical_error_detail"
        )
    }
}

impl Visit for TimelineFields {
    fn record_u64(&mut self, field: &Field, value: u64) {
        if Self::accepts(field) {
            self.0.insert(field.name().into(), json!(value));
        }
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        if Self::accepts(field) {
            self.0.insert(field.name().into(), json!(value));
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if Self::accepts(field) {
            self.0.insert(field.name().into(), json!(value));
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if Self::accepts(field) {
            self.0.insert(
                field.name().into(),
                json!(format!("{value:?}").trim_matches('"')),
            );
        }
    }
}

impl Diagnostics {
    pub(super) fn with_critical_path() -> anyhow::Result<Self> {
        let clock = crate::sync_manager::storage::critical_trace::clock();
        let unix_anchor_us =
            u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?
                .checked_sub(u64::try_from(clock.elapsed().as_micros())?)
                .context("critical clock anchor underflow")?;
        Ok(Self {
            critical: Some(Arc::new(Mutex::new(CriticalPath {
                unix_anchor_us,
                ..CriticalPath::default()
            }))),
            ..Self::default()
        })
    }

    pub(super) fn critical_snapshot(&self, full: bool) -> anyhow::Result<Option<Value>> {
        let Some(critical) = &self.critical else {
            return Ok(None);
        };
        ensure!(
            self.event_errors.load(Ordering::Relaxed) == 0,
            "critical diagnostic recording failed"
        );
        Ok(Some(
            critical
                .lock()
                .map_err(|_| anyhow::anyhow!("critical path lock poisoned"))?
                .snapshot(full)?,
        ))
    }

    pub(super) fn with_timeline(path: &std::path::Path) -> anyhow::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .with_context(|| format!("cannot create recovery timeline {}", path.display()))?;
        Ok(Self {
            timeline: Some(Arc::new(RecoveryTimeline {
                origin: "detector-0".into(),
                started: StdInstant::now(),
                unix_anchor_us: u64::try_from(
                    SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros(),
                )?,
                buffer: Mutex::new(TimelineBuffer {
                    file,
                    events: Vec::new(),
                    dropped: 0,
                }),
            })),
            ..Self::default()
        })
    }

    pub(super) fn timeline_phase(&self, phase: &str) -> anyhow::Result<()> {
        if let Some(timeline) = &self.timeline {
            timeline.record(
                json!({"recovery_trace_stage": "phase", "phase": phase})
                    .as_object()
                    .context("invalid timeline phase")?
                    .clone(),
            )?;
        }
        Ok(())
    }

    pub(super) fn timeline_receipt(
        &self,
        progress: &[crate::sync_manager::ReplicationProgress],
    ) -> anyhow::Result<()> {
        if let Some(timeline) = &self.timeline {
            let receipt = progress
                .iter()
                .find(|entry| entry.origin.as_str() == timeline.origin);
            timeline.record(
                json!({
                    "recovery_trace_stage": "receipt_sample", "origin": timeline.origin,
                    "known": receipt.is_some(),
                    "contiguous": receipt.map(|entry| entry.contiguous_sequence),
                    "known_high": receipt.map(|entry| entry.known_high_sequence),
                    "sparse": receipt.map(|entry| entry.sparse_positions),
                    "recovery_failure": receipt.map(|entry| entry.recovery_failure.is_some()),
                })
                .as_object()
                .context("invalid timeline receipt")?
                .clone(),
            )?;
        }
        Ok(())
    }

    pub(super) fn save_timeline(&self) -> anyhow::Result<()> {
        if let Some(timeline) = &self.timeline {
            ensure!(
                self.event_errors.load(Ordering::Relaxed) == 0,
                "cannot certify recovery timeline: diagnostic errors"
            );
            let mut buffer = timeline
                .buffer
                .lock()
                .map_err(|_| anyhow::anyhow!("recovery timeline lock poisoned"))?;
            let report = json!({
                "version": 1, "origin": timeline.origin, "unix_anchor_us": timeline.unix_anchor_us,
                "clock": "captured_us is process-local monotonic; Unix anchor is approximate, not a synchronized clock",
                "limit": TIMELINE_EVENT_LIMIT, "dropped": buffer.dropped,
                "complete": buffer.dropped == 0, "events": buffer.events,
                "captured_through_us": u64::try_from(timeline.started.elapsed().as_micros())?,
                "queue_boundary": "queue_offer precedes send; queue_admitted follows send and may occur after dequeue",
            });
            buffer
                .file
                .rewind()
                .context("cannot rewind recovery timeline output")?;
            write_timeline_snapshot(&mut buffer.file, &report)?;
            let length = buffer
                .file
                .stream_position()
                .context("cannot measure recovery timeline output")?;
            buffer
                .file
                .set_len(length)
                .context("cannot finish recovery timeline output")?;
            buffer
                .file
                .flush()
                .context("cannot flush recovery timeline output")?;
        }
        Ok(())
    }

    pub(super) fn install(&self) -> anyhow::Result<()> {
        tracing::subscriber::set_global_default(self.clone())
            .context("cannot install worker diagnostic subscriber")?;
        if self.critical.is_some() {
            crate::sync_manager::storage::critical_trace::enable();
        }
        Ok(())
    }

    fn snapshot(&self) -> Value {
        Value::Object(
            STAGES
                .iter()
                .zip(self.timings.iter())
                .map(|((name, _), timing)| {
                    (
                        (*name).to_owned(),
                        json!({
                            "count": timing.count.load(Ordering::Relaxed),
                            "total_us": timing.total_us.load(Ordering::Relaxed),
                            "max_us": timing.max_us.load(Ordering::Relaxed),
                            "failed": timing.failed.load(Ordering::Relaxed),
                        }),
                    )
                })
                .collect(),
        )
    }

    fn remote_snapshot(&self) -> anyhow::Result<Value> {
        ensure!(
            self.event_errors.load(Ordering::Relaxed) == 0,
            "diagnostic event counter failed"
        );
        let timings = self
            .remote_timings
            .lock()
            .map_err(|_| anyhow::anyhow!("remote timing diagnostic lock poisoned"))?;
        Ok(Value::Object(
            REMOTE_STAGES
                .iter()
                .zip(timings.iter())
                .map(|((name, _), timing)| {
                    (
                        (*name).to_owned(),
                        json!({
                            "count": timing.count,
                            "total_us": timing.total_us,
                            "max_us": timing.max_us,
                            "failed": timing.failed,
                        }),
                    )
                })
                .collect(),
        ))
    }

    fn reset_remote_timings(&self) -> anyhow::Result<()> {
        *self
            .remote_timings
            .lock()
            .map_err(|_| anyhow::anyhow!("remote timing diagnostic lock poisoned"))? =
            [TimingSnapshot::default(); REMOTE_STAGES.len()];
        Ok(())
    }

    fn snapshot_events(
        &self,
        events: &Mutex<BTreeMap<String, u64>>,
    ) -> anyhow::Result<BTreeMap<String, u64>> {
        ensure!(
            self.event_errors.load(Ordering::Relaxed) == 0,
            "diagnostic event counter failed"
        );
        Ok(events
            .lock()
            .map_err(|_| anyhow::anyhow!("diagnostic event lock poisoned"))?
            .clone())
    }

    fn event_snapshot(&self) -> anyhow::Result<BTreeMap<String, u64>> {
        self.snapshot_events(&self.events)
    }

    fn network_event_snapshot(&self) -> anyhow::Result<BTreeMap<String, u64>> {
        self.snapshot_events(&self.network_events)
    }

    fn connection_error_snapshot(&self) -> anyhow::Result<BTreeMap<String, u64>> {
        self.snapshot_events(&self.connection_errors)
    }

    fn repair_trace_snapshot(&self) -> anyhow::Result<BTreeMap<String, RecoveryCount>> {
        ensure!(
            self.event_errors.load(Ordering::Relaxed) == 0,
            "diagnostic event counter failed"
        );
        Ok(self
            .repair_trace_events
            .lock()
            .map_err(|_| anyhow::anyhow!("diagnostic event lock poisoned"))?
            .clone())
    }

    fn history_batch_snapshot(&self) -> anyhow::Result<BTreeMap<String, u64>> {
        self.snapshot_events(&self.history_batches)
    }

    fn event_loop_snapshot(&self) -> anyhow::Result<BTreeMap<String, EventLoopTiming>> {
        ensure!(
            self.event_errors.load(Ordering::Relaxed) == 0,
            "diagnostic event counter failed"
        );
        Ok(self
            .event_loop
            .lock()
            .map_err(|_| anyhow::anyhow!("event-loop diagnostic lock poisoned"))?
            .clone())
    }

    fn record_event_loop(&self, kind: String, elapsed_us: u64, queues: EventLoopQueues) {
        if !EVENT_LOOP_KINDS.contains(&kind.as_str()) {
            self.event_errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
        match self.event_loop.lock() {
            Ok(mut events)
                if events.len() < EVENT_LOOP_KINDS.len() || events.contains_key(&kind) =>
            {
                events.entry(kind).or_default().record(elapsed_us, queues);
            }
            _ => {
                self.event_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn record_remote_timing(&self, stage: usize, elapsed_us: u64, failed: bool) {
        let mut timings = match self.remote_timings.lock() {
            Ok(timings) => timings,
            Err(_) => {
                self.event_errors.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        let timing = &mut timings[stage];
        let Some(total_us) = timing.total_us.checked_add(elapsed_us) else {
            self.event_errors.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let Some(count) = timing.count.checked_add(1) else {
            self.event_errors.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let Some(failed) = timing.failed.checked_add(u64::from(failed)) else {
            self.event_errors.fetch_add(1, Ordering::Relaxed);
            return;
        };
        timing.total_us = total_us;
        timing.count = count;
        timing.max_us = timing.max_us.max(elapsed_us);
        timing.failed = failed;
    }

    fn record_history_batch(&self, fields: &Fields) {
        let (
            Some(kind),
            Some(stop),
            Some(envelopes),
            Some(pushes),
            Some(replays),
            Some(operations),
            Some(bytes),
        ) = (
            fields.history_batch_kind.as_deref(),
            fields.history_batch_stop.as_deref(),
            fields.history_batch_envelopes,
            fields.history_batch_pushes,
            fields.history_batch_replays,
            fields.history_batch_operations,
            fields.history_batch_estimated_bytes,
        )
        else {
            self.event_errors.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if !HISTORY_BATCH_KINDS.contains(&kind) || !HISTORY_BATCH_STOP_REASONS.contains(&stop) {
            self.event_errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let prefix = format!("kind.{kind}.");
        let increments = [
            (format!("{prefix}batches"), 1),
            (format!("{prefix}envelopes"), envelopes),
            (format!("{prefix}push_envelopes"), pushes),
            (format!("{prefix}replay_envelopes"), replays),
            (format!("{prefix}operations"), operations),
            (format!("{prefix}estimated_bytes"), bytes),
            (format!("stop.{stop}"), 1),
        ];
        let mut counters = match self.history_batches.lock() {
            Ok(counters) => counters,
            Err(_) => {
                self.event_errors.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        if increments.iter().any(|(key, increment)| {
            (!counters.contains_key(key) && counters.len() >= HISTORY_BATCH_COUNTER_LIMIT)
                || counters
                    .get(key)
                    .is_some_and(|count| count.checked_add(*increment).is_none())
        }) {
            self.event_errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
        for (key, increment) in increments {
            *counters.entry(key).or_default() += increment;
        }
    }

    fn count_event(&self, events: &Mutex<BTreeMap<String, u64>>, key: String, limit: usize) {
        match events.lock() {
            Ok(mut events) if events.len() < limit || events.contains_key(&key) => {
                *events.entry(key).or_default() += 1;
            }
            _ => {
                self.event_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

#[derive(Default)]
struct Fields {
    stage: Option<usize>,
    remote_stage: Option<usize>,
    elapsed_us: Option<u64>,
    event_handler_kind: Option<String>,
    queue_depth_before: Option<u64>,
    queue_depth_after: Option<u64>,
    data_queue_depth_before: Option<u64>,
    data_queue_depth_after: Option<u64>,
    priority_queue_depth_before: Option<u64>,
    priority_queue_depth_after: Option<u64>,
    error_class: Option<String>,
    message: Option<String>,
    network_error_class: Option<String>,
    recovery_event: Option<String>,
    recovery_items: Option<u64>,
    history_batch_kind: Option<String>,
    history_batch_stop: Option<String>,
    history_batch_envelopes: Option<u64>,
    history_batch_pushes: Option<u64>,
    history_batch_replays: Option<u64>,
    history_batch_operations: Option<u64>,
    history_batch_estimated_bytes: Option<u64>,
    failed: bool,
}

impl Visit for Fields {
    fn record_u64(&mut self, field: &Field, value: u64) {
        match field.name() {
            "elapsed_us" => self.elapsed_us = Some(value),
            "recovery_items" => self.recovery_items = Some(value),
            "queue_depth_before" => self.queue_depth_before = Some(value),
            "queue_depth_after" => self.queue_depth_after = Some(value),
            "data_queue_depth_before" => self.data_queue_depth_before = Some(value),
            "data_queue_depth_after" => self.data_queue_depth_after = Some(value),
            "priority_queue_depth_before" => self.priority_queue_depth_before = Some(value),
            "priority_queue_depth_after" => self.priority_queue_depth_after = Some(value),
            "history_batch_envelopes" => self.history_batch_envelopes = Some(value),
            "history_batch_pushes" => self.history_batch_pushes = Some(value),
            "history_batch_replays" => self.history_batch_replays = Some(value),
            "history_batch_operations" => self.history_batch_operations = Some(value),
            "history_batch_estimated_bytes" => self.history_batch_estimated_bytes = Some(value),
            _ => {}
        }
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        if field.name() == "success" {
            self.failed = !value;
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "message" => {
                self.stage = STAGES.iter().position(|(_, text)| *text == value);
                self.remote_stage = REMOTE_STAGES.iter().position(|(_, text)| *text == value);
                self.message = Some(value.to_owned());
            }
            "error_class" => self.error_class = Some(value.to_owned()),
            "error" => {
                self.network_error_class = Some(network_error_class(value).to_owned());
            }
            "recovery_event" => self.recovery_event = Some(value.to_owned()),
            "event_handler_kind" => self.event_handler_kind = Some(value.to_owned()),
            "history_batch_kind" => self.history_batch_kind = Some(value.to_owned()),
            "history_batch_stop" => self.history_batch_stop = Some(value.to_owned()),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "message" => {
                let message = format!("{value:?}").trim_matches('"').to_owned();
                self.stage = STAGES.iter().position(|(_, text)| *text == message);
                self.remote_stage = REMOTE_STAGES.iter().position(|(_, text)| *text == message);
                self.message = Some(message);
            }
            "error_class" => {
                self.error_class = Some(format!("{value:?}").trim_matches('"').to_owned());
            }
            "error" => {
                self.network_error_class =
                    Some(network_error_class(&format!("{value:?}")).to_owned());
            }
            "recovery_event" => {
                self.recovery_event = Some(format!("{value:?}").trim_matches('"').to_owned());
            }
            "event_handler_kind" => {
                self.event_handler_kind = Some(format!("{value:?}").trim_matches('"').to_owned());
            }
            "history_batch_kind" => {
                self.history_batch_kind = Some(format!("{value:?}").trim_matches('"').to_owned());
            }
            "history_batch_stop" => {
                self.history_batch_stop = Some(format!("{value:?}").trim_matches('"').to_owned());
            }
            _ => {}
        }
    }
}

#[test]
fn recovery_timeline_preserves_queue_bounds_and_filters_payloads() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("timeline.json");
    let diagnostics = Diagnostics::with_timeline(&path).unwrap();
    diagnostics.timeline_phase("convergence").unwrap();
    tracing::subscriber::with_default(diagnostics.clone(), || {
        let message = nx_sync::HistoryMessage::Request {
            request_id: nx_sync::OpId::new("trace-request"),
            origin: NodeId::new("detector-0"),
            start: 1,
            end: 3,
        };
        let trace = nx_net::HistoryTrace::from_message("detector-1", &message).unwrap();
        trace.emit("queue_offer");
        trace.emit("dequeued");
        trace.emit("queue_admitted");
        tracing::debug!(
            target: "nx_core::sync_manager::replication",
            recovery_trace_stage = "request_lookup",
            origin = "detector-0", request_id = "trace-request",
            decision = "expired", payload = "ignored_payload",
            "test recovery timeline"
        );
        tracing::debug!(
            target: "nx_net::recovery_trace",
            recovery_trace_stage = "queue_offer", origin = "other-origin",
            "test recovery timeline"
        );
        tracing::debug!(
            target: "nx_core::sync_manager::storage",
            message = "remote storage flush completed", elapsed_us = 123u64, success = true,
            "phase timing"
        );
    });
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        0,
        "trace performed measurement-time file writes"
    );
    diagnostics.save_timeline().unwrap();
    let report: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(report["complete"], true);
    assert_eq!(report["dropped"], 0);
    let events = report["events"].as_array().unwrap();
    assert_eq!(events.len(), 6);
    assert_eq!(events[1]["recovery_trace_stage"], "queue_offer");
    assert_eq!(events[2]["recovery_trace_stage"], "dequeued");
    assert_eq!(events[3]["recovery_trace_stage"], "queue_admitted");
    assert_eq!(events[4]["decision"], "expired");
    assert_eq!(events[5]["recovery_trace_stage"], "flush");
    assert_eq!(events[5]["elapsed_us"], 123);
    assert!(
        events
            .iter()
            .all(|event| event["captured_us"].as_u64().is_some())
    );
    assert!(
        events
            .windows(2)
            .all(|pair| pair[0]["captured_us"].as_u64() <= pair[1]["captured_us"].as_u64())
    );
    assert!(!report.to_string().contains("ignored_payload"));
    assert!(!report.to_string().contains("other-origin"));
    diagnostics.timeline_phase("measurement_end").unwrap();
    diagnostics.save_timeline().unwrap();
    let updated: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(updated["events"].as_array().unwrap().len(), 7);
}

#[test]
fn recovery_timeline_reports_overflow_without_overwriting_artifacts() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("timeline.json");
    let diagnostics = Diagnostics::with_timeline(&path).unwrap();
    assert!(Diagnostics::with_timeline(&path).is_err());
    for _ in 0..TIMELINE_EVENT_LIMIT + 3 {
        diagnostics.timeline_phase("convergence").unwrap();
    }
    diagnostics.save_timeline().unwrap();
    let report: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(report["complete"], false);
    assert_eq!(report["dropped"], 3);
    assert_eq!(
        report["events"].as_array().unwrap().len(),
        TIMELINE_EVENT_LIMIT
    );
}

#[test]
fn recovery_timeline_snapshot_uses_one_write_and_propagates_errors() {
    #[derive(Default)]
    struct CountingWriter {
        writes: usize,
        bytes: Vec<u8>,
        fail: bool,
    }
    impl Write for CountingWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self.fail {
                return Err(std::io::Error::other("fixture output failure"));
            }
            self.writes += 1;
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let report = json!({"events": (0..TIMELINE_EVENT_LIMIT).map(|index| json!({
        "captured_us": index, "recovery_trace_stage": "queue_offer",
        "origin": "detector-0", "request_id": "fixture-request", "start": 1, "end": 3,
    })).collect::<Vec<_>>()});
    let mut writer = CountingWriter::default();
    write_timeline_snapshot(&mut writer, &report).unwrap();
    eprintln!(
        "TIMELINE_SNAPSHOT_WRITES calls={} bytes={}",
        writer.writes,
        writer.bytes.len()
    );
    assert_eq!(
        writer.writes, 1,
        "snapshot serialization amplifies file write calls"
    );
    assert_eq!(writer.bytes, serde_json::to_vec(&report).unwrap());
    let mut failing = CountingWriter {
        fail: true,
        ..CountingWriter::default()
    };
    assert!(write_timeline_snapshot(&mut failing, &report).is_err());
}

fn network_error_class(error: &str) -> &'static str {
    let error = error.trim_matches('"');
    if error.starts_with("peer connection limit reached:") {
        "peer_limit_reached"
    } else if error.contains("timed out") {
        "timeout"
    } else if error.starts_with("io error:") {
        "io_error"
    } else {
        "other"
    }
}

impl Subscriber for Diagnostics {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        if metadata.target() == crate::sync_manager::storage::critical_trace::TARGET {
            return self.critical.is_some();
        }
        if metadata.fields().field("recovery_trace_stage").is_some() {
            return self.timeline.is_some();
        }
        metadata.is_event()
            && ((*metadata.level() == tracing::Level::DEBUG
                && matches!(
                    metadata.target(),
                    "nx_core::sync_manager::manager"
                        | "nx_core::sync_manager::storage"
                        | "nx_core::sync_manager::apply"
                )
                && metadata.fields().field("elapsed_us").is_some())
                || (*metadata.level() == tracing::Level::DEBUG
                    && metadata.target() == "nx_core::sync_manager::manager"
                    && metadata.fields().field("event_handler_kind").is_some())
                || metadata.target() == "nx_core::sync_manager::replication"
                || metadata.target().starts_with("nx_net::node"))
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        // Only events are enabled; this subscriber holds no span state.
        Id::from_u64(1)
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let metadata = event.metadata();
        if metadata.target() == crate::sync_manager::storage::critical_trace::TARGET {
            let mut fields = TimelineFields::default();
            event.record(&mut fields);
            let recorded = self
                .critical
                .as_ref()
                .context("critical recorder missing")
                .and_then(|critical| {
                    if fields.0.get("critical_trace_error") == Some(&json!(true)) {
                        anyhow::bail!(
                            "critical producer failed: {:?}",
                            fields.0.get("critical_error_detail")
                        );
                    }
                    critical
                        .lock()
                        .map_err(|_| anyhow::anyhow!("critical path lock poisoned"))?
                        .record(fields.0)
                });
            if let Err(error) = recorded {
                self.event_errors.fetch_add(1, Ordering::Relaxed);
                eprintln!("critical path diagnostic failed: {error:#}");
            }
            return;
        }
        if let Some(timeline) = &self.timeline {
            let mut fields = TimelineFields::default();
            event.record(&mut fields);
            let remote_stage = fields
                .0
                .get("message")
                .and_then(Value::as_str)
                .and_then(|message| REMOTE_STAGES.iter().find(|(_, text)| *text == message));
            if let Some((stage, _)) = remote_stage {
                fields.0.insert("recovery_trace_stage".into(), json!(stage));
            }
            let selected = match fields.0.get("origin").and_then(Value::as_str) {
                Some(origin) => origin == timeline.origin,
                None => {
                    fields.0.contains_key("recovery_trace_stage")
                        || fields.0.contains_key("event_handler_kind")
                }
            };
            if selected && timeline.record(fields.0).is_err() {
                self.event_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
        if metadata.fields().field("recovery_trace_stage").is_some() {
            return;
        }
        if metadata.target() == "nx_core::sync_manager::replication" {
            let mut fields = Fields::default();
            event.record(&mut fields);
            if let Some(stage) = fields.remote_stage {
                if let Some(elapsed_us) = fields.elapsed_us {
                    self.record_remote_timing(stage, elapsed_us, fields.failed);
                } else {
                    self.event_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
            if fields.recovery_event.is_none() {
                let key = format!(
                    "{}:{}",
                    metadata.file().unwrap_or(metadata.target()),
                    metadata.line().unwrap_or(0)
                );
                self.count_event(&self.events, key, 128);
            }
            if let Some(error_class) = fields.error_class.as_deref() {
                self.count_event(&self.connection_errors, error_class.to_owned(), 16);
            }
            if let Some(recovery_event) = fields.recovery_event.as_deref() {
                match self.repair_trace_events.lock() {
                    Ok(mut events)
                        if events.len() < REPAIR_TRACE_EVENT_LIMIT
                            || events.contains_key(recovery_event) =>
                    {
                        let event = events.entry(recovery_event.to_owned()).or_default();
                        match event.events.checked_add(1).zip(
                            event
                                .items
                                .checked_add(fields.recovery_items.unwrap_or_default()),
                        ) {
                            Some((count, items)) => {
                                event.events = count;
                                event.items = items;
                            }
                            None => {
                                self.event_errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    _ => {
                        self.event_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            if fields.history_batch_kind.is_some() || fields.history_batch_stop.is_some() {
                self.record_history_batch(&fields);
            }
            return;
        }
        if metadata.target().starts_with("nx_net::node") {
            let mut fields = Fields::default();
            event.record(&mut fields);
            let class = match fields.message.as_deref() {
                Some("incoming connection") => "incoming_connection",
                Some("rejecting incoming connection: peer limit reached") => {
                    "incoming_peer_limit_rejected"
                }
                Some("connection error") => match fields.network_error_class.as_deref() {
                    Some("peer_limit_reached") => "connection_error.peer_limit_reached",
                    Some("timeout") => "connection_error.timeout",
                    Some("io_error") => "connection_error.io_error",
                    Some("other") => "connection_error.other",
                    _ => "connection_error.unclassified",
                },
                _ => "other",
            };
            self.count_event(&self.network_events, class.to_owned(), 8);
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        if let Some(kind) = fields.event_handler_kind {
            if let (
                Some(elapsed_us),
                Some(queue_before),
                Some(queue_after),
                Some(data_queue_before),
                Some(data_queue_after),
                Some(priority_queue_before),
                Some(priority_queue_after),
            ) = (
                fields.elapsed_us,
                fields.queue_depth_before,
                fields.queue_depth_after,
                fields.data_queue_depth_before,
                fields.data_queue_depth_after,
                fields.priority_queue_depth_before,
                fields.priority_queue_depth_after,
            ) {
                let queues = EventLoopQueues {
                    data_before: data_queue_before,
                    data_after: data_queue_after,
                    priority_before: priority_queue_before,
                    priority_after: priority_queue_after,
                };
                if queue_before != queues.data_before.saturating_add(queues.priority_before)
                    || queue_after != queues.data_after.saturating_add(queues.priority_after)
                {
                    self.event_errors.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.record_event_loop(kind, elapsed_us, queues);
                }
            } else {
                self.event_errors.fetch_add(1, Ordering::Relaxed);
            }
            return;
        }
        if let Some(stage) = fields.remote_stage {
            if let Some(elapsed_us) = fields.elapsed_us {
                self.record_remote_timing(stage, elapsed_us, fields.failed);
            } else {
                self.event_errors.fetch_add(1, Ordering::Relaxed);
            }
            return;
        }
        if let (Some(stage), Some(elapsed_us)) = (fields.stage, fields.elapsed_us) {
            let timing = &self.timings[stage];
            timing.total_us.fetch_add(elapsed_us, Ordering::Relaxed);
            timing.max_us.fetch_max(elapsed_us, Ordering::Relaxed);
            timing
                .failed
                .fetch_add(u64::from(fields.failed), Ordering::Relaxed);
            timing.count.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
        Some(tracing::level_filters::LevelFilter::DEBUG)
    }
}

pub(super) struct Baseline {
    work: BTreeMap<&'static str, u64>,
    flushes: u64,
    batch_work: (u64, u64),
    single_mutation_batches: u64,
    events: BTreeMap<String, u64>,
    history_batches: BTreeMap<String, u64>,
    network_events: BTreeMap<String, u64>,
    connection_errors: BTreeMap<String, u64>,
    repair_trace_events: BTreeMap<String, RecoveryCount>,
    event_loop: BTreeMap<String, EventLoopTiming>,
}

impl Baseline {
    pub(super) fn new(node: &SyncManager, diagnostics: &Diagnostics) -> anyhow::Result<Self> {
        diagnostics.reset_remote_timings()?;
        Ok(Self {
            work: node.metrics.replication_work_for_test(),
            flushes: node.store.completed_flushes(),
            batch_work: node.store.completed_batch_work(),
            single_mutation_batches: node.store.completed_single_mutation_batches(),
            events: diagnostics.event_snapshot()?,
            history_batches: diagnostics.history_batch_snapshot()?,
            network_events: diagnostics.network_event_snapshot()?,
            connection_errors: diagnostics.connection_error_snapshot()?,
            repair_trace_events: diagnostics.repair_trace_snapshot()?,
            event_loop: diagnostics.event_loop_snapshot()?,
        })
    }

    pub(super) fn sample(
        &self,
        node: &SyncManager,
        diagnostics: &Diagnostics,
    ) -> anyhow::Result<Value> {
        let work = node
            .metrics
            .replication_work_for_test()
            .into_iter()
            .map(|(key, value)| {
                let before = self.work.get(key).context("missing work baseline")?;
                Ok((
                    key,
                    value
                        .checked_sub(*before)
                        .context("work counter regressed")?,
                ))
            })
            .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
        let events = diagnostics
            .event_snapshot()?
            .into_iter()
            .map(|(key, value)| {
                let count = value
                    .checked_sub(*self.events.get(&key).unwrap_or(&0))
                    .context("diagnostic event counter regressed")?;
                Ok((key, count))
            })
            .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
        let history_batches =
            count_delta(diagnostics.history_batch_snapshot()?, &self.history_batches)?;
        let network_events =
            count_delta(diagnostics.network_event_snapshot()?, &self.network_events)?;
        let connection_errors = count_delta(
            diagnostics.connection_error_snapshot()?,
            &self.connection_errors,
        )?;
        let repair_trace_events = recovery_count_delta(
            diagnostics.repair_trace_snapshot()?,
            &self.repair_trace_events,
        )?;
        let event_loop = event_loop_delta(diagnostics.event_loop_snapshot()?, &self.event_loop)?;
        let (batches, bytes) = node.store.completed_batch_work();
        let active_flush = node
            .store
            .active_flush()
            .map(|(sequence, elapsed)| {
                Ok::<_, anyhow::Error>(json!({
                    "sequence": sequence,
                    "elapsed_us": u64::try_from(elapsed.as_micros())?,
                }))
            })
            .transpose()?;
        let mut sample = json!({
            "local_stages": diagnostics.snapshot(),
            "remote_apply_stages": diagnostics.remote_snapshot()?,
            "replication_work": work,
            "explicit_flushes_completed": node.store.completed_flushes()
                .checked_sub(self.flushes).context("flush counter regressed")?,
            "active_covering_flush": active_flush,
            "store_batches": batches.checked_sub(self.batch_work.0).context("batch counter regressed")?,
            "store_logical_bytes": bytes.checked_sub(self.batch_work.1).context("batch byte counter regressed")?,
            "store_single_mutation_batches": node.store.completed_single_mutation_batches()
                .checked_sub(self.single_mutation_batches).context("single-mutation batch counter regressed")?,
            "replication_events": events,
            "history_batches": history_batches,
            "network_events": network_events,
            "connection_error_classes": connection_errors,
            "repair_trace_events": repair_trace_events.events,
            "repair_trace_items": repair_trace_events.items,
            "event_loop": event_loop,
        });
        if let Some(critical) = diagnostics.critical_snapshot(false)? {
            sample["critical_path"] = critical;
        }
        Ok(sample)
    }
}

fn event_loop_delta(
    current: BTreeMap<String, EventLoopTiming>,
    baseline: &BTreeMap<String, EventLoopTiming>,
) -> anyhow::Result<BTreeMap<String, Value>> {
    current
        .into_iter()
        .map(|(kind, timing)| {
            let before = baseline.get(&kind).copied().unwrap_or_default();
            let count = timing
                .count
                .checked_sub(before.count)
                .context("event-loop diagnostic count regressed")?;
            let total_us = timing
                .total_us
                .checked_sub(before.total_us)
                .context("event-loop diagnostic duration regressed")?;
            let mut buckets = [0; 4];
            for (index, bucket) in buckets.iter_mut().enumerate() {
                *bucket = timing.duration_buckets[index]
                    .checked_sub(before.duration_buckets[index])
                    .context("event-loop diagnostic bucket regressed")?;
            }
            let queue_before_total = timing
                .queue_before_total
                .checked_sub(before.queue_before_total)
                .context("event-loop queue-before count regressed")?;
            let queue_after_total = timing
                .queue_after_total
                .checked_sub(before.queue_after_total)
                .context("event-loop queue-after count regressed")?;
            let data_queue_before_total = timing
                .data_queue_before_total
                .checked_sub(before.data_queue_before_total)
                .context("event-loop data queue-before count regressed")?;
            let data_queue_after_total = timing
                .data_queue_after_total
                .checked_sub(before.data_queue_after_total)
                .context("event-loop data queue-after count regressed")?;
            let priority_queue_before_total = timing
                .priority_queue_before_total
                .checked_sub(before.priority_queue_before_total)
                .context("event-loop priority queue-before count regressed")?;
            let priority_queue_after_total = timing
                .priority_queue_after_total
                .checked_sub(before.priority_queue_after_total)
                .context("event-loop priority queue-after count regressed")?;
            Ok((
                kind,
                json!({
                    "count": count,
                    "total_us": total_us,
                    "mean_us": total_us / count.max(1),
                    "duration_buckets": {
                        "lt_100ms": buckets[0],
                        "100ms_to_500ms": buckets[1],
                        "500ms_to_2s": buckets[2],
                        "gte_2s": buckets[3],
                    },
                    "queue_depth_before_total": queue_before_total,
                    "queue_depth_after_total": queue_after_total,
                    "mean_queue_depth_before": queue_before_total / count.max(1),
                    "mean_queue_depth_after": queue_after_total / count.max(1),
                    "data_queue_depth_before_total": data_queue_before_total,
                    "data_queue_depth_after_total": data_queue_after_total,
                    "priority_queue_depth_before_total": priority_queue_before_total,
                    "priority_queue_depth_after_total": priority_queue_after_total,
                    "mean_data_queue_depth_before": data_queue_before_total / count.max(1),
                    "mean_data_queue_depth_after": data_queue_after_total / count.max(1),
                    "mean_priority_queue_depth_before": priority_queue_before_total / count.max(1),
                    "mean_priority_queue_depth_after": priority_queue_after_total / count.max(1),
                    "max_duration_us_lifetime": timing.max_us,
                    "max_queue_depth_before_lifetime": timing.max_queue_before,
                    "max_queue_depth_after_lifetime": timing.max_queue_after,
                    "max_data_queue_depth_before_lifetime": timing.max_data_queue_before,
                    "max_data_queue_depth_after_lifetime": timing.max_data_queue_after,
                    "max_priority_queue_depth_before_lifetime": timing.max_priority_queue_before,
                    "max_priority_queue_depth_after_lifetime": timing.max_priority_queue_after,
                }),
            ))
        })
        .collect()
}

struct RecoveryCountDelta {
    events: BTreeMap<String, u64>,
    items: BTreeMap<String, u64>,
}

fn recovery_count_delta(
    current: BTreeMap<String, RecoveryCount>,
    baseline: &BTreeMap<String, RecoveryCount>,
) -> anyhow::Result<RecoveryCountDelta> {
    let mut events = BTreeMap::new();
    let mut items = BTreeMap::new();
    for (key, value) in current {
        let previous = baseline.get(&key).copied().unwrap_or_default();
        events.insert(
            key.clone(),
            value
                .events
                .checked_sub(previous.events)
                .context("repair event counter regressed")?,
        );
        items.insert(
            key,
            value
                .items
                .checked_sub(previous.items)
                .context("repair item counter regressed")?,
        );
    }
    Ok(RecoveryCountDelta { events, items })
}

fn count_delta(
    current: BTreeMap<String, u64>,
    baseline: &BTreeMap<String, u64>,
) -> anyhow::Result<BTreeMap<String, u64>> {
    current
        .into_iter()
        .map(|(key, value)| {
            let previous = baseline.get(&key).copied().unwrap_or_default();
            Ok((
                key,
                value
                    .checked_sub(previous)
                    .context("diagnostic counter regressed")?,
            ))
        })
        .collect()
}

pub(super) fn aggregate<'a>(rows: impl Iterator<Item = &'a Value>) -> anyhow::Result<Value> {
    let mut stages = BTreeMap::<String, BTreeMap<String, u64>>::new();
    let mut remote_apply_stages = BTreeMap::<String, BTreeMap<String, u64>>::new();
    let mut work = BTreeMap::<String, u64>::new();
    let mut flushes = 0u64;
    let mut store_work = BTreeMap::<String, u64>::new();
    let mut events = BTreeMap::<String, u64>::new();
    let mut history_batches = BTreeMap::<String, u64>::new();
    let mut network_events = BTreeMap::<String, u64>::new();
    let mut connection_errors = BTreeMap::<String, u64>::new();
    let mut repair_trace_events = BTreeMap::<String, u64>::new();
    let mut repair_trace_items = BTreeMap::<String, u64>::new();
    let mut event_loop = BTreeMap::<String, EventLoopAggregate>::new();
    let mut workers = 0;
    for row in rows {
        let diagnostics = &row["diagnostics"];
        flushes = flushes
            .checked_add(super::process::number(
                diagnostics,
                "explicit_flushes_completed",
            )?)
            .context("flush aggregation overflow")?;
        for (stage, _) in STAGES {
            for field in ["count", "total_us", "max_us", "failed"] {
                let value = super::process::number(&diagnostics["local_stages"][stage], field)?;
                let total = stages
                    .entry(stage.to_owned())
                    .or_default()
                    .entry(field.to_owned())
                    .or_default();
                *total = if field == "max_us" {
                    (*total).max(value)
                } else {
                    total
                        .checked_add(value)
                        .context("stage aggregation overflow")?
                };
            }
        }
        for (stage, _) in REMOTE_STAGES {
            for field in ["count", "total_us", "max_us", "failed"] {
                let value =
                    super::process::number(&diagnostics["remote_apply_stages"][stage], field)?;
                let total = remote_apply_stages
                    .entry(stage.to_owned())
                    .or_default()
                    .entry(field.to_owned())
                    .or_default();
                *total = if field == "max_us" {
                    (*total).max(value)
                } else {
                    total
                        .checked_add(value)
                        .context("remote apply stage aggregation overflow")?
                };
            }
        }
        for (key, value) in diagnostics["replication_work"]
            .as_object()
            .context("missing replication work sample")?
        {
            let total = work.entry(key.clone()).or_default();
            *total = total
                .checked_add(value.as_u64().context("invalid work counter")?)
                .context("work aggregation overflow")?;
        }
        for key in [
            "store_batches",
            "store_logical_bytes",
            "store_single_mutation_batches",
        ] {
            let total = store_work.entry(key.to_owned()).or_default();
            *total = total
                .checked_add(super::process::number(diagnostics, key)?)
                .context("store work aggregation overflow")?;
        }
        for (key, value) in diagnostics["replication_events"]
            .as_object()
            .context("missing replication event sample")?
        {
            let total = events.entry(key.clone()).or_default();
            *total = total
                .checked_add(value.as_u64().context("invalid replication event count")?)
                .context("replication event aggregation overflow")?;
        }
        aggregate_counts(
            &mut network_events,
            &diagnostics["network_events"],
            "network event",
        )?;
        aggregate_counts(
            &mut connection_errors,
            &diagnostics["connection_error_classes"],
            "connection error",
        )?;
        aggregate_counts(
            &mut repair_trace_events,
            &diagnostics["repair_trace_events"],
            "repair trace event",
        )?;
        aggregate_counts(
            &mut repair_trace_items,
            &diagnostics["repair_trace_items"],
            "repair trace item",
        )?;
        aggregate_counts(
            &mut history_batches,
            &diagnostics["history_batches"],
            "history batch",
        )?;
        aggregate_event_loop(&mut event_loop, &diagnostics["event_loop"])?;
        workers += 1;
    }
    let event_loop = event_loop
        .into_iter()
        .map(|(kind, timing)| {
            (
                kind,
                json!({
                    "count": timing.count,
                    "total_us": timing.total_us,
                    "mean_us": timing.total_us / timing.count.max(1),
                    "duration_buckets": {
                        "lt_100ms": timing.duration_buckets[0],
                        "100ms_to_500ms": timing.duration_buckets[1],
                        "500ms_to_2s": timing.duration_buckets[2],
                        "gte_2s": timing.duration_buckets[3],
                    },
                    "queue_depth_before_total": timing.queue_before_total,
                    "queue_depth_after_total": timing.queue_after_total,
                    "mean_queue_depth_before": timing.queue_before_total / timing.count.max(1),
                    "mean_queue_depth_after": timing.queue_after_total / timing.count.max(1),
                    "data_queue_depth_before_total": timing.data_queue_before_total,
                    "data_queue_depth_after_total": timing.data_queue_after_total,
                    "priority_queue_depth_before_total": timing.priority_queue_before_total,
                    "priority_queue_depth_after_total": timing.priority_queue_after_total,
                    "mean_data_queue_depth_before": timing.data_queue_before_total / timing.count.max(1),
                    "mean_data_queue_depth_after": timing.data_queue_after_total / timing.count.max(1),
                    "mean_priority_queue_depth_before": timing.priority_queue_before_total / timing.count.max(1),
                    "mean_priority_queue_depth_after": timing.priority_queue_after_total / timing.count.max(1),
                    "max_duration_us_lifetime": timing.max_duration_us,
                    "max_queue_depth_before_lifetime": timing.max_queue_before,
                    "max_queue_depth_after_lifetime": timing.max_queue_after,
                    "max_data_queue_depth_before_lifetime": timing.max_data_queue_before,
                    "max_data_queue_depth_after_lifetime": timing.max_data_queue_after,
                    "max_priority_queue_depth_before_lifetime": timing.max_priority_queue_before,
                    "max_priority_queue_depth_after_lifetime": timing.max_priority_queue_after,
                }),
            )
        })
        .collect::<BTreeMap<_, _>>();
    Ok(json!({
        "workers": workers, "local_stages": stages,
        "remote_apply_stages": remote_apply_stages,
        "replication_work": work, "explicit_flushes_completed": flushes,
        "store_work": store_work, "replication_events": events,
        "history_batches": history_batches,
        "network_events": network_events, "connection_error_classes": connection_errors,
        "repair_trace_events": repair_trace_events, "repair_trace_items": repair_trace_items,
        "event_loop": event_loop,
        "scope": "remote apply stage and history batch diagnostics since the latest phase baseline; replication counters since nominal start, including drain and in-flight setup work; not a coherent per-operation trace or physical fsync count"
    }))
}

fn aggregate_counts(
    total: &mut BTreeMap<String, u64>,
    source: &Value,
    description: &str,
) -> anyhow::Result<()> {
    for (key, value) in source
        .as_object()
        .with_context(|| format!("missing {description} sample"))?
    {
        let total = total.entry(key.clone()).or_default();
        *total = total
            .checked_add(
                value
                    .as_u64()
                    .with_context(|| format!("invalid {description} count"))?,
            )
            .with_context(|| format!("{description} count overflow"))?;
    }
    Ok(())
}

fn aggregate_event_loop(
    total: &mut BTreeMap<String, EventLoopAggregate>,
    source: &Value,
) -> anyhow::Result<()> {
    let values = source
        .as_object()
        .context("missing event-loop diagnostic sample")?;
    for (kind, sample) in values {
        let count = super::process::number(sample, "count")?;
        let timing = total.entry(kind.clone()).or_default();
        timing.count = timing
            .count
            .checked_add(count)
            .context("event-loop count aggregation overflow")?;
        timing.total_us = timing
            .total_us
            .checked_add(super::process::number(sample, "total_us")?)
            .context("event-loop duration aggregation overflow")?;
        timing.queue_before_total = timing
            .queue_before_total
            .checked_add(super::process::number(sample, "queue_depth_before_total")?)
            .context("event-loop queue-before aggregation overflow")?;
        timing.queue_after_total = timing
            .queue_after_total
            .checked_add(super::process::number(sample, "queue_depth_after_total")?)
            .context("event-loop queue-after aggregation overflow")?;
        for (total, field) in [
            (
                &mut timing.data_queue_before_total,
                "data_queue_depth_before_total",
            ),
            (
                &mut timing.data_queue_after_total,
                "data_queue_depth_after_total",
            ),
            (
                &mut timing.priority_queue_before_total,
                "priority_queue_depth_before_total",
            ),
            (
                &mut timing.priority_queue_after_total,
                "priority_queue_depth_after_total",
            ),
        ] {
            *total = total
                .checked_add(super::process::number(sample, field)?)
                .context("event-loop lane queue aggregation overflow")?;
        }
        timing.max_duration_us = timing
            .max_duration_us
            .max(super::process::number(sample, "max_duration_us_lifetime")?);
        timing.max_queue_before = timing.max_queue_before.max(super::process::number(
            sample,
            "max_queue_depth_before_lifetime",
        )?);
        timing.max_queue_after = timing.max_queue_after.max(super::process::number(
            sample,
            "max_queue_depth_after_lifetime",
        )?);
        for (total, field) in [
            (
                &mut timing.max_data_queue_before,
                "max_data_queue_depth_before_lifetime",
            ),
            (
                &mut timing.max_data_queue_after,
                "max_data_queue_depth_after_lifetime",
            ),
            (
                &mut timing.max_priority_queue_before,
                "max_priority_queue_depth_before_lifetime",
            ),
            (
                &mut timing.max_priority_queue_after,
                "max_priority_queue_depth_after_lifetime",
            ),
        ] {
            *total = (*total).max(super::process::number(sample, field)?);
        }
        for (index, label) in ["lt_100ms", "100ms_to_500ms", "500ms_to_2s", "gte_2s"]
            .into_iter()
            .enumerate()
        {
            timing.duration_buckets[index] = timing.duration_buckets[index]
                .checked_add(super::process::number(&sample["duration_buckets"], label)?)
                .context("event-loop duration bucket aggregation overflow")?;
        }
    }
    Ok(())
}

#[test]
fn timing_subscriber_filters_and_aggregates_existing_events() {
    let diagnostics = Diagnostics::default();
    tracing::subscriber::with_default(diagnostics.clone(), || {
        tracing::debug!(target: "nx_core::sync_manager::manager", elapsed_us = 7u64, "local registry locks acquired");
        tracing::debug!(target: "nx_core::sync_manager::manager", elapsed_us = 3u64, "local storage job started");
        tracing::debug!(target: "nx_core::sync_manager::storage", elapsed_us = 5u64, "local storage lease acquired");
        tracing::debug!(target: "nx_core::sync_manager::storage", elapsed_us = 11u64, success = true, "local storage batch completed");
        tracing::debug!(target: "nx_core::sync_manager::storage", elapsed_us = 13u64, success = true, "local storage flush completed");
        tracing::debug!(target: "nx_core::sync_manager::storage", elapsed_us = 17u64, success = false, "local storage flush completed");
        tracing::debug!(target: "unrelated", elapsed_us = 999u64, "local storage flush completed");
        tracing::debug!(target: "nx_core::sync_manager::storage", elapsed_us = 999u64, "unrelated event");
    });
    let snapshot = diagnostics.snapshot();
    for (stage, expected) in [
        ("registry_wait", 7),
        ("storage_dispatch", 3),
        ("lease_wait", 5),
        ("batch", 11),
    ] {
        assert_eq!(snapshot[stage]["count"], 1);
        assert_eq!(snapshot[stage]["total_us"], expected);
    }
    assert_eq!(snapshot["flush"]["count"], 2);
    assert_eq!(snapshot["flush"]["total_us"], 30);
    assert_eq!(snapshot["flush"]["max_us"], 17);
    assert_eq!(snapshot["flush"]["failed"], 1);
}

#[test]
fn timing_subscriber_aggregates_remote_apply_stages() {
    let diagnostics = Diagnostics::default();
    tracing::subscriber::with_default(diagnostics.clone(), || {
        tracing::debug!(target: "nx_core::sync_manager::apply", message = "remote registry locks acquired", elapsed_us = 7u64, success = true, "bounded remote apply diagnostic");
        tracing::debug!(target: "nx_core::sync_manager::apply", message = "remote storage job started", elapsed_us = 3u64, success = true, "bounded remote apply diagnostic");
        tracing::debug!(target: "nx_core::sync_manager::apply", message = "remote batch prepared", elapsed_us = 5u64, success = true, "bounded remote apply diagnostic");
        tracing::debug!(target: "nx_core::sync_manager::storage", message = "remote storage batch completed", elapsed_us = 11u64, success = true, "bounded remote persistence diagnostic");
        tracing::debug!(target: "nx_core::sync_manager::storage", message = "remote storage flush completed", elapsed_us = 13u64, success = false, "bounded remote persistence diagnostic");
        tracing::debug!(target: "nx_core::sync_manager::apply", message = "remote registries published", elapsed_us = 17u64, success = true, "bounded remote apply diagnostic");
        tracing::debug!(target: "unrelated", message = "remote batch prepared", elapsed_us = 999u64, success = true, "unrelated event");
    });

    let snapshot = diagnostics.remote_snapshot().unwrap();
    for (stage, expected) in [
        ("registry_wait", 7),
        ("storage_dispatch", 3),
        ("preparation", 5),
        ("batch", 11),
        ("publication", 17),
    ] {
        assert_eq!(snapshot[stage]["count"], 1);
        assert_eq!(snapshot[stage]["total_us"], expected);
        assert_eq!(snapshot[stage]["failed"], 0);
    }
    assert_eq!(snapshot["flush"]["count"], 1);
    assert_eq!(snapshot["flush"]["total_us"], 13);
    assert_eq!(snapshot["flush"]["failed"], 1);
}

#[test]
fn diagnostics_report_phase_local_remote_timings_and_batch_deltas() {
    let directory = tempfile::tempdir().unwrap();
    let node = SyncManager::try_new(
        NodeId::new("phase-diagnostic"),
        SyncConfig::new(),
        Arc::new(NxStore::open(directory.path()).unwrap()),
        metrics(),
    )
    .unwrap();
    let diagnostics = Diagnostics::default();
    tracing::subscriber::with_default(diagnostics.clone(), || {
        tracing::debug!(target: "nx_core::sync_manager::storage", message = "remote storage flush completed", elapsed_us = 100u64, success = true, "pre-phase timing");
        crate::sync_manager::replication::trace_remote_history_batch(
            "push_only",
            1,
            0,
            2,
            128,
            "queue_empty",
        );
    });
    let baseline = Baseline::new(&node, &diagnostics).unwrap();
    tracing::subscriber::with_default(diagnostics.clone(), || {
        tracing::debug!(target: "nx_core::sync_manager::storage", message = "remote storage flush completed", elapsed_us = 7u64, success = true, "phase timing");
        tracing::debug!(target: "nx_core::sync_manager::storage", message = "remote storage flush completed", elapsed_us = 9u64, success = false, "phase timing");
        tracing::debug!(target: "nx_core::sync_manager::apply", message = "remote batch prepared", elapsed_us = 5u64, success = true, "phase timing");
        tracing::debug!(target: "nx_core::sync_manager::replication", message = "remote push summaries recorded", elapsed_us = 11u64, success = true, "phase timing");
        crate::sync_manager::replication::trace_remote_history_batch(
            "mixed",
            2,
            1,
            4,
            512,
            "control_summary",
        );
        crate::sync_manager::replication::trace_remote_history_batch(
            "push_only",
            1,
            0,
            1,
            128,
            "coalesce_timeout",
        );
        crate::sync_manager::replication::trace_remote_history_batch(
            "replay_only",
            0,
            1,
            1,
            128,
            "control_priority",
        );
    });

    let sample = baseline.sample(&node, &diagnostics).unwrap();

    assert_eq!(sample["remote_apply_stages"]["flush"]["count"], 2);
    assert_eq!(sample["remote_apply_stages"]["flush"]["total_us"], 16);
    assert_eq!(sample["remote_apply_stages"]["flush"]["max_us"], 9);
    assert_eq!(sample["remote_apply_stages"]["flush"]["failed"], 1);
    assert_eq!(sample["remote_apply_stages"]["preparation"]["count"], 1);
    assert_eq!(sample["remote_apply_stages"]["push_summary"]["count"], 1);
    assert_eq!(
        sample["remote_apply_stages"]["push_summary"]["total_us"],
        11
    );
    assert_eq!(sample["history_batches"]["kind.mixed.batches"], 1);
    assert_eq!(sample["history_batches"]["kind.mixed.envelopes"], 3);
    assert_eq!(sample["history_batches"]["kind.mixed.push_envelopes"], 2);
    assert_eq!(sample["history_batches"]["kind.mixed.replay_envelopes"], 1);
    assert_eq!(sample["history_batches"]["kind.mixed.operations"], 4);
    assert_eq!(sample["history_batches"]["kind.mixed.estimated_bytes"], 512);
    assert_eq!(sample["history_batches"]["stop.control_summary"], 1);
    assert_eq!(sample["history_batches"]["stop.coalesce_timeout"], 1);
    assert_eq!(sample["history_batches"]["stop.control_priority"], 1);
}

#[test]
fn event_loop_diagnostics_capture_bounded_latency_queue_and_repair_work() {
    let diagnostics = Diagnostics::default();
    tracing::subscriber::with_default(diagnostics.clone(), || {
        let summary_kind = "summary";
        tracing::debug!(
            target: "nx_core::sync_manager::manager",
            event_handler_kind = %summary_kind,
            elapsed_us = 300_000u64,
            queue_depth_before = 4u64,
            queue_depth_after = 8u64,
            data_queue_depth_before = 2u64,
            data_queue_depth_after = 5u64,
            priority_queue_depth_before = 2u64,
            priority_queue_depth_after = 3u64,
            "bounded event-loop diagnostic"
        );
        tracing::debug!(
            target: "nx_core::sync_manager::manager",
            event_handler_kind = "push",
            elapsed_us = 50_000u64,
            queue_depth_before = 2u64,
            queue_depth_after = 1u64,
            data_queue_depth_before = 2u64,
            data_queue_depth_after = 1u64,
            priority_queue_depth_before = 0u64,
            priority_queue_depth_after = 0u64,
            "bounded event-loop diagnostic"
        );
        crate::sync_manager::replication::trace_recovery_latency(
            "summary_gap_send",
            std::time::Duration::from_millis(300),
            1,
        );
    });

    let delta =
        event_loop_delta(diagnostics.event_loop_snapshot().unwrap(), &BTreeMap::new()).unwrap();
    assert_eq!(delta["summary"]["count"], 1);
    assert_eq!(delta["summary"]["duration_buckets"]["100ms_to_500ms"], 1);
    assert_eq!(delta["summary"]["mean_queue_depth_before"], 4);
    assert_eq!(delta["summary"]["mean_queue_depth_after"], 8);
    assert_eq!(delta["summary"]["mean_data_queue_depth_before"], 2);
    assert_eq!(delta["summary"]["mean_data_queue_depth_after"], 5);
    assert_eq!(delta["summary"]["mean_priority_queue_depth_before"], 2);
    assert_eq!(delta["summary"]["mean_priority_queue_depth_after"], 3);
    assert_eq!(delta["push"]["duration_buckets"]["lt_100ms"], 1);
    assert_eq!(delta["push"]["mean_priority_queue_depth_before"], 0);
    assert_eq!(
        diagnostics.repair_trace_snapshot().unwrap()["summary_gap_send_lt_500ms"].events,
        1
    );
}

#[test]
fn summary_batch_diagnostic_counts_only_changed_successful_writes() {
    if run_in_isolated_test_process(
        "sync_manager::manager::tests::detector_soak::diagnostics::summary_batch_diagnostic_counts_only_changed_successful_writes",
    ) {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let store = NxStore::open(directory.path()).unwrap();
    let diagnostics = Diagnostics::default();
    let mut summary = nx_sync::HistorySummary {
        origin: NodeId::new("summary-diagnostic"),
        high_sequence: 1,
        baseline_required: false,
    };
    diagnostics.install().unwrap();
    crate::sync_manager::storage::record_summary(&store, &summary).unwrap();
    crate::sync_manager::storage::record_summary(&store, &summary).unwrap();
    store.inject_disk_full_on_writes(true);
    summary.high_sequence = 2;
    assert!(crate::sync_manager::storage::record_summary(&store, &summary).is_err());
    assert_eq!(store.completed_batch_work().0, 1);
    let events = diagnostics.event_snapshot().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events.values().copied().sum::<u64>(), 1);
    assert!(
        events
            .keys()
            .all(|key| key.contains("storage/sequence.rs:"))
    );
}

#[test]
fn work_snapshot_and_aggregation_do_not_need_store_scans() {
    let directory = tempfile::tempdir().unwrap();
    let node = SyncManager::try_new(
        NodeId::new("diagnostic"),
        SyncConfig::new(),
        Arc::new(NxStore::open(directory.path()).unwrap()),
        metrics(),
    )
    .unwrap();
    let diagnostics = Diagnostics::default();
    let baseline = Baseline::new(&node, &diagnostics).unwrap();
    node.store.apply_batch(&[(b"k", b"value")], &[]).unwrap();
    node.store.inject_disk_full_on_writes(true);
    assert!(
        node.store
            .apply_batch(&[(b"other", b"value")], &[])
            .is_err()
    );
    node.store.inject_disk_full_on_writes(false);
    node.store
        .acquire_write_lease()
        .unwrap()
        .apply_batch(&[], &[b"k"])
        .unwrap();
    node.store
        .apply_batch(&[(b"a", b"x"), (b"b", b"y")], &[])
        .unwrap();
    tracing::subscriber::with_default(diagnostics.clone(), || {
        tracing::warn!(target: "nx_core::sync_manager::replication", secret = "not collected", "diagnostic test");
        tracing::debug!(target: "nx_core::sync_manager::replication", error_class = "io_connection_reset", error = "not collected", "configured peer connect failed");
        tracing::debug!(target: "nx_core::sync_manager::replication", recovery_event = "range_request_sent", recovery_items = 3u64, "test recovery diagnostic");
        tracing::warn!(target: "nx_net::node", addr = "not collected", "rejecting incoming connection: peer limit reached");
        tracing::error!(
            target: "nx_net::node",
            addr = "not collected",
            error = %nx_net::NetError::PeerLimitReached(4),
            "connection error"
        );
    });
    node.metrics
        .record_remote_op_batch(4, 3, 1, Duration::from_millis(2), false);
    node.metrics.record_broadcast_batch(2);
    let sample = baseline.sample(&node, &diagnostics).unwrap();
    let row = json!({"diagnostics": sample});
    let total = aggregate([&row, &row].into_iter()).unwrap();
    assert_eq!(total["workers"], 2);
    assert_eq!(total["replication_work"]["remote_ops_received_total"], 8);
    assert_eq!(total["replication_work"]["remote_ops_applied_total"], 6);
    assert_eq!(total["replication_work"]["remote_ops_duplicate_total"], 2);
    assert_eq!(
        total["replication_work"]["remote_op_batch_apply_sum_ns"],
        4_000_000
    );
    assert_eq!(total["replication_work"]["broadcast_ops_total"], 4);
    assert_eq!(total["store_work"]["store_batches"], 6);
    assert_eq!(total["store_work"]["store_logical_bytes"], 22);
    assert_eq!(total["store_work"]["store_single_mutation_batches"], 4);
    assert_eq!(
        total["replication_events"]
            .as_object()
            .unwrap()
            .values()
            .map(|value| value.as_u64().unwrap())
            .sum::<u64>(),
        4
    );
    assert_eq!(
        total["replication_events"]
            .as_object()
            .unwrap()
            .values()
            .next()
            .unwrap(),
        2
    );
    assert_eq!(total["connection_error_classes"]["io_connection_reset"], 2);
    assert_eq!(total["repair_trace_events"]["range_request_sent"], 2);
    assert_eq!(total["repair_trace_items"]["range_request_sent"], 6);
    assert_eq!(total["network_events"]["incoming_peer_limit_rejected"], 2);
    assert_eq!(
        total["network_events"]["connection_error.peer_limit_reached"],
        2
    );
    assert!(aggregate([&json!({})].into_iter()).is_err());
}
