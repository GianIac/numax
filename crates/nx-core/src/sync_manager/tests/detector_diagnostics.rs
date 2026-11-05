use super::*;
use serde_json::{Value, json};
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

#[derive(Default)]
struct Timing {
    count: AtomicU64,
    total_us: AtomicU64,
    max_us: AtomicU64,
    failed: AtomicU64,
}

#[derive(Clone, Default)]
pub(super) struct Diagnostics {
    timings: Arc<[Timing; STAGES.len()]>,
}

impl Diagnostics {
    pub(super) fn install(&self) -> anyhow::Result<()> {
        tracing::subscriber::set_global_default(self.clone())
            .context("cannot install worker diagnostic subscriber")
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
}

#[derive(Default)]
struct Fields {
    stage: Option<usize>,
    elapsed_us: Option<u64>,
    failed: bool,
}

impl Visit for Fields {
    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "elapsed_us" {
            self.elapsed_us = Some(value);
        }
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        if field.name() == "success" {
            self.failed = !value;
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let message = format!("{value:?}");
            self.stage = STAGES.iter().position(|(_, text)| *text == message);
        }
    }
}

impl Subscriber for Diagnostics {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.is_event()
            && *metadata.level() == tracing::Level::DEBUG
            && matches!(
                metadata.target(),
                "nx_core::sync_manager::manager" | "nx_core::sync_manager::storage"
            )
            && metadata.fields().field("elapsed_us").is_some()
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
        let mut fields = Fields::default();
        event.record(&mut fields);
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
}

impl Baseline {
    pub(super) fn new(node: &SyncManager) -> Self {
        Self {
            work: node.metrics.replication_work_for_test(),
            flushes: node.store.completed_flushes(),
        }
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
        Ok(json!({
            "local_stages": diagnostics.snapshot(),
            "replication_work": work,
            "explicit_flushes_completed": node.store.completed_flushes()
                .checked_sub(self.flushes).context("flush counter regressed")?,
        }))
    }
}

pub(super) fn aggregate<'a>(rows: impl Iterator<Item = &'a Value>) -> anyhow::Result<Value> {
    let mut stages = BTreeMap::<String, BTreeMap<String, u64>>::new();
    let mut work = BTreeMap::<String, u64>::new();
    let mut flushes = 0u64;
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
        for (key, value) in diagnostics["replication_work"]
            .as_object()
            .context("missing replication work sample")?
        {
            let total = work.entry(key.clone()).or_default();
            *total = total
                .checked_add(value.as_u64().context("invalid work counter")?)
                .context("work aggregation overflow")?;
        }
        workers += 1;
    }
    Ok(json!({
        "workers": workers, "local_stages": stages,
        "replication_work": work, "explicit_flushes_completed": flushes,
        "scope": "completed local stages; replication counters since nominal start, including drain and in-flight setup work; not a coherent per-operation trace or physical fsync count"
    }))
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
fn work_snapshot_and_aggregation_do_not_need_store_scans() {
    let directory = tempfile::tempdir().unwrap();
    let node = SyncManager::try_new(
        NodeId::new("diagnostic"),
        SyncConfig::new(),
        Arc::new(NxStore::open(directory.path()).unwrap()),
        metrics(),
    )
    .unwrap();
    let baseline = Baseline::new(&node);
    node.metrics
        .record_remote_op_batch(4, 3, 1, Duration::from_millis(2), false);
    node.metrics.record_broadcast_batch(2);
    let sample = baseline.sample(&node, &Diagnostics::default()).unwrap();
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
    assert!(aggregate([&json!({})].into_iter()).is_err());
}
