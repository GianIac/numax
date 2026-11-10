use super::*;
use anyhow::{Context, ensure};
use nx_net::{MembershipState, SerializationFormat};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::task::JoinSet;

const KEY: &str = "detector-nominal";
const FORMATION_LIMIT: Duration = Duration::from_secs(300);
const DETECTION_LIMIT: Duration = Duration::from_secs(120);
const CONVERGENCE_LIMIT: Duration = Duration::from_secs(30);
const MARKER: &str = "NUMAX_DETECTOR_SOAK_CHILD";
#[path = "detector_diagnostics.rs"]
mod diagnostics;
#[path = "detector_process.rs"]
mod process;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn detector_smoke_json_and_bincode() {
    let name = "sync_manager::manager::tests::detector_soak::detector_smoke_json_and_bincode";
    if child(name, Duration::from_secs(180)) {
        return;
    }
    for format in [SerializationFormat::Json, SerializationFormat::Bincode] {
        scenario(3, Duration::from_secs(8), format).await.unwrap();
    }
    process::scenario(3, Duration::from_secs(8), Duration::from_secs(1))
        .await
        .unwrap();
    process::storage_scenario(3, Duration::from_secs(8), Duration::from_secs(1))
        .await
        .unwrap();
    process::storage_metadata_scenario(3, Duration::from_secs(8), Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "100 independent durable stores; diagnostic baseline without networking"]
async fn storage_100_processes_preflight() {
    let name = "sync_manager::manager::tests::detector_soak::storage_100_processes_preflight";
    if child(name, Duration::from_secs(600)) {
        return;
    }
    process::storage_scenario(100, Duration::from_secs(30), Duration::from_secs(10))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "100 real TCP nodes, short preflight before the one-hour campaign"]
async fn detector_100_nodes_preflight_json() {
    let name = "sync_manager::manager::tests::detector_soak::detector_100_nodes_preflight_json";
    if child(name, Duration::from_secs(600)) {
        return;
    }
    process::scenario(100, Duration::from_secs(30), Duration::from_secs(10))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "controlled 1/100-process remote apply comparison without networking"]
async fn remote_history_1_vs_100_processes() {
    let name = "sync_manager::manager::tests::detector_soak::remote_history_1_vs_100_processes";
    if child(name, Duration::from_secs(1200)) {
        return;
    }
    process::remote_history_comparison().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "replay an existing bounded remote trace without networking"]
async fn remote_history_replay_captured_trace() {
    let name = "sync_manager::manager::tests::detector_soak::remote_history_replay_captured_trace";
    if child(name, Duration::from_secs(1200)) {
        return;
    }
    let trace_path = std::env::var_os("NUMAX_DETECTOR_REPLAY_TRACE")
        .map(std::path::PathBuf::from)
        .expect("remote history replay requires NUMAX_DETECTOR_REPLAY_TRACE");
    let result_path = std::env::var_os("NUMAX_DETECTOR_REPLAY_RESULT")
        .map(std::path::PathBuf::from)
        .expect("remote history replay requires NUMAX_DETECTOR_REPLAY_RESULT");
    let report = process::replay_remote_history_trace(&trace_path, &result_path)
        .await
        .unwrap();
    println!("REMOTE_HISTORY_REPLAY {}", report);
}

#[cfg(target_os = "macos")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "three paired 100-process diagnostic comparisons; not a production setting"]
async fn periodic_flush_100_processes_ab() {
    let name = "sync_manager::manager::tests::detector_soak::periodic_flush_100_processes_ab";
    if child(name, Duration::from_secs(1200)) {
        return;
    }
    let directory = std::env::var_os("NUMAX_DETECTOR_TRACE_DIR")
        .map(std::path::PathBuf::from)
        .expect("periodic A/B requires an existing artifact directory");
    process::periodic_flush_comparison(100, 3, &directory)
        .await
        .unwrap();
}

#[cfg(target_os = "macos")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn periodic_flush_three_process_smoke() {
    let name = "sync_manager::manager::tests::detector_soak::periodic_flush_three_process_smoke";
    if child(name, Duration::from_secs(180)) {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    assert!(
        process::periodic_flush_comparison(101, 1, directory.path())
            .await
            .is_err()
    );
    let report = process::periodic_flush_comparison(3, 1, directory.path())
        .await
        .unwrap();
    assert_eq!(report["pairs"][0]["a_periodic_flush_ms"], 500);
    assert!(report["pairs"][0]["b_periodic_flush_ms"].is_null());
    assert_eq!(report["pairs"][0]["reopen_verified_workers_per_case"], 3);
    assert!(
        directory
            .path()
            .join("pair-1-a-default/case-report.json")
            .is_file()
    );
    assert!(
        directory
            .path()
            .join("pair-1-b-disabled/host-iostat.stdout")
            .is_file()
    );
    assert!(
        process::periodic_flush_comparison(3, 1, directory.path())
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn detector_convergence_smoke_json() {
    let name = "sync_manager::manager::tests::detector_soak::detector_convergence_smoke_json";
    if child(name, Duration::from_secs(180)) {
        return;
    }
    process::convergence_scenario(3, Duration::from_secs(8), Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "100 real processes; separate post-load convergence verification"]
async fn detector_100_nodes_convergence_json() {
    let name = "sync_manager::manager::tests::detector_soak::detector_100_nodes_convergence_json";
    if child(name, Duration::from_secs(600)) {
        return;
    }
    process::convergence_scenario(100, Duration::from_secs(30), Duration::from_secs(10))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "100 real TCP nodes, one real hour of nominal load, then transport failure"]
async fn detector_100_nodes_one_hour_json() {
    let name = "sync_manager::manager::tests::detector_soak::detector_100_nodes_one_hour_json";
    if child(name, Duration::from_secs(4500)) {
        return;
    }
    process::scenario(100, Duration::from_secs(3600), Duration::from_secs(10))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "100 real TCP nodes, one real hour, post-load replica health, then transport failure"]
async fn detector_100_nodes_one_hour_with_replication_health_json() {
    let name = "sync_manager::manager::tests::detector_soak::detector_100_nodes_one_hour_with_replication_health_json";
    if child(name, Duration::from_secs(4500)) {
        return;
    }
    process::convergence_scenario(100, Duration::from_secs(3600), Duration::from_secs(10))
        .await
        .unwrap();
}

#[cfg(target_os = "macos")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "100 real processes; stop at first native stall capture, not a nominal certification"]
async fn detector_100_nodes_nominal_stall_capture_json() {
    let name = "sync_manager::manager::tests::detector_soak::detector_100_nodes_nominal_stall_capture_json";
    if child(name, Duration::from_secs(4500)) {
        return;
    }
    process::nominal_stall_scenario().await.unwrap();
}

#[cfg(target_os = "macos")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_stall_capture_process_smoke_json() {
    let name =
        "sync_manager::manager::tests::detector_soak::native_stall_capture_process_smoke_json";
    if child(name, Duration::from_secs(180)) {
        return;
    }
    process::native_stall_capture_smoke().await.unwrap();
}

#[test]
fn soak_output_keeps_real_result_after_long_progress() {
    let name = "sync_manager::manager::tests::detector_soak::soak_output_keeps_real_result_after_long_progress";
    if std::env::var_os(MARKER).as_deref() == Some(std::ffi::OsStr::new(name)) {
        for minute in 1..=800 {
            println!(
                "DETECTOR_PROGRESS {}",
                serde_json::json!({
                    "elapsed_s": minute * 60, "nodes": 100, "completed": 0,
                    "rss_sum_sampled_peak_bytes": 4_064_145_408u64,
                })
            );
        }
        println!("SOAK_CAPTURE_TAIL");
        return;
    }
    let _scenario = process_scenario_io();
    let first = isolated_test_output(name, MARKER, name, Duration::from_secs(30));
    assert_eq!(first.len(), 64 * 1024);
    if std::env::var_os("NUMAX_SHUTDOWN_TRACE").is_none() {
        assert!(!String::from_utf8_lossy(&first).contains("SOAK_CAPTURE_TAIL"));
    }
    let output = isolated_test_output_tail(name, MARKER, name, Duration::from_secs(30));
    assert_eq!(output.len(), 64 * 1024);
    let text = String::from_utf8_lossy(&output);
    assert!(
        text.lines()
            .any(|line| line.starts_with("test result: ok. 1 passed; 0 failed;"))
    );
    assert!(text.contains("SOAK_CAPTURE_TAIL"));
    println!(
        "SOAK_OUTPUT_CAPTURE_REGRESSION captured_bytes={} real_footer_verified=true",
        output.len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "storage-only diagnostic at 100 durable requests/s, not the nominal detector profile"]
async fn storage_100_processes_high_load_preflight() {
    let name =
        "sync_manager::manager::tests::detector_soak::storage_100_processes_high_load_preflight";
    if child(name, Duration::from_secs(600)) {
        return;
    }
    process::storage_scenario(100, Duration::from_secs(30), Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "storage-only diagnostic with metadata-sized batches, not a detector criterion"]
async fn storage_100_processes_metadata_preflight() {
    let name =
        "sync_manager::manager::tests::detector_soak::storage_100_processes_metadata_preflight";
    if child(name, Duration::from_secs(600)) {
        return;
    }
    process::storage_metadata_scenario(100, Duration::from_secs(30), Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "same metadata records in one atomic batch; storage-only causal comparison"]
async fn storage_100_processes_batched_metadata_preflight() {
    let name = "sync_manager::manager::tests::detector_soak::storage_100_processes_batched_metadata_preflight";
    if child(name, Duration::from_secs(600)) {
        return;
    }
    process::storage_batched_metadata_scenario(
        100,
        Duration::from_secs(30),
        Duration::from_secs(1),
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit 100-request/s load profile; previously saturated the Mac host"]
async fn detector_100_nodes_high_load_preflight_json() {
    let name =
        "sync_manager::manager::tests::detector_soak::detector_100_nodes_high_load_preflight_json";
    if child(name, Duration::from_secs(600)) {
        return;
    }
    process::scenario(100, Duration::from_secs(10), Duration::from_secs(1))
        .await
        .unwrap();
}

fn child(name: &str, lifetime: Duration) -> bool {
    if std::env::var_os(MARKER).as_deref() == Some(std::ffi::OsStr::new(name)) {
        return false;
    }
    let _scenario = process_scenario_io();
    let output = isolated_test_output_tail(name, MARKER, name, lifetime);
    let text = String::from_utf8_lossy(&output);
    assert!(
        text.lines()
            .any(|line| line.starts_with("test result: ok. 1 passed; 0 failed;")),
        "isolated detector scenario did not execute exactly one successful test:\n{text}"
    );
    print!("{text}");
    true
}

async fn exchange_owner(
    network: &nx_net::Node,
    endpoint: &str,
    request: nx_net::ControlRequest,
    expected: &str,
) -> anyhow::Result<u64> {
    for attempt in 0..=5u64 {
        match network
            .control_request(endpoint, request.clone(), Duration::from_millis(500))
            .await
        {
            Ok((peer, reply)) => {
                ensure!(
                    peer.as_str() == expected && reply.owner.member == expected,
                    "bootstrap owner/session mismatch"
                );
                return Ok(attempt);
            }
            Err(error) => {
                let delay = match &error {
                    nx_net::NetError::Wire(nx_net::WireError::RateLimited { retry_after_ms }) => {
                        Duration::from_millis(retry_after_ms.unwrap_or(100))
                    }
                    nx_net::NetError::PeerLimitReached(_) => Duration::from_millis(100),
                    _ => return Err(error).context("bootstrap exchange failed"),
                };
                ensure!(
                    attempt < 5 && delay <= Duration::from_secs(1),
                    "bootstrap backpressure exhausted for {expected}: {error}"
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
    anyhow::bail!("bootstrap retry budget exhausted")
}

#[derive(Clone, Debug, Default)]
struct Counts {
    suspect: u64,
    refuted: u64,
    declared_dead: u64,
    learned_dead: u64,
    probe_success: u64,
    probe_failed: u64,
    probe_degraded: u64,
}

impl Counts {
    fn raw(&self) -> serde_json::Value {
        serde_json::json!({
            "suspect": self.suspect, "refuted": self.refuted,
            "declared_dead": self.declared_dead, "learned_dead": self.learned_dead,
            "probe_success": self.probe_success, "probe_failed": self.probe_failed,
            "probe_degraded": self.probe_degraded
        })
    }

    fn from_raw(value: &serde_json::Value) -> anyhow::Result<Self> {
        let get = |key| {
            value
                .get(key)
                .and_then(serde_json::Value::as_u64)
                .with_context(|| format!("missing worker counter {key}"))
        };
        Ok(Self {
            suspect: get("suspect")?,
            refuted: get("refuted")?,
            declared_dead: get("declared_dead")?,
            learned_dead: get("learned_dead")?,
            probe_success: get("probe_success")?,
            probe_failed: get("probe_failed")?,
            probe_degraded: get("probe_degraded")?,
        })
    }

    fn add(&mut self, other: &Self) {
        self.suspect += other.suspect;
        self.refuted += other.refuted;
        self.declared_dead += other.declared_dead;
        self.learned_dead += other.learned_dead;
        self.probe_success += other.probe_success;
        self.probe_failed += other.probe_failed;
        self.probe_degraded += other.probe_degraded;
    }

    fn delta(&self, before: &Self) -> Self {
        Self {
            suspect: self.suspect - before.suspect,
            refuted: self.refuted - before.refuted,
            declared_dead: self.declared_dead - before.declared_dead,
            learned_dead: self.learned_dead - before.learned_dead,
            probe_success: self.probe_success - before.probe_success,
            probe_failed: self.probe_failed - before.probe_failed,
            probe_degraded: self.probe_degraded - before.probe_degraded,
        }
    }

    fn json(&self, observer_hours: f64) -> serde_json::Value {
        serde_json::json!({
            "suspect_observations": self.suspect,
            "refuted_observations": self.refuted,
            "local_dead_declarations": self.declared_dead,
            "learned_dead_observations": self.learned_dead,
            "successful_probes": self.probe_success,
            "failed_probes": self.probe_failed,
            "locally_degraded_probes": self.probe_degraded,
            "suspect_observations_per_observer_hour": self.suspect as f64 / observer_hours,
            "refuted_observations_per_observer_hour": self.refuted as f64 / observer_hours,
            "local_dead_declarations_per_observer_hour": self.declared_dead as f64 / observer_hours,
        })
    }
}

fn metric(text: &str, name: &str) -> anyhow::Result<u64> {
    text.lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(' '))
        .with_context(|| format!("missing detector metric {name}"))?
        .parse()
        .with_context(|| format!("invalid detector metric {name}"))
}

fn healthy(nodes: &[SyncManager], allowed_dead: Option<&str>) -> anyhow::Result<()> {
    for node in nodes {
        if let Some(handle) = &node.membership {
            handle
                .snapshot
                .has_changed()
                .context("membership actor stopped unexpectedly")?;
            let snapshot = handle.snapshot.borrow();
            ensure!(
                snapshot.error.is_none(),
                "membership error: {:?}",
                snapshot.error
            );
            ensure!(
                snapshot.records.len() == nodes.len(),
                "membership view lost a subject: observer={} records={} expected={}",
                node.node_id,
                snapshot.records.len(),
                nodes.len()
            );
            for record in &snapshot.records {
                ensure!(
                    !matches!(record.state, MembershipState::Dead | MembershipState::Left)
                        || Some(record.member.as_str()) == allowed_dead,
                    "unexpected terminal membership state: observer={} record={record:?}",
                    node.node_id
                );
            }
        } else {
            ensure!(
                Some(node.node_id.as_str()) == allowed_dead,
                "healthy node {} has no membership actor",
                node.node_id
            );
        }
    }
    Ok(())
}

fn observe(nodes: &[SyncManager]) -> anyhow::Result<(Counts, BTreeMap<String, u64>)> {
    let mut total = Counts::default();
    let mut queues = BTreeMap::new();
    for node in nodes {
        let mut text = String::new();
        node.metrics.peers.render(&mut text);
        total.add(&Counts {
            suspect: metric(
                &text,
                "numax_membership_transitions_total{transition=\"suspect\"}",
            )?,
            refuted: metric(
                &text,
                "numax_membership_transitions_total{transition=\"refuted\"}",
            )?,
            declared_dead: metric(
                &text,
                "numax_membership_transitions_total{transition=\"declared_dead\"}",
            )?,
            learned_dead: metric(
                &text,
                "numax_membership_transitions_total{transition=\"learned_dead\"}",
            )?,
            probe_success: metric(
                &text,
                "numax_membership_probe_results_total{outcome=\"success\"}",
            )?,
            probe_failed: metric(
                &text,
                "numax_membership_probe_results_total{outcome=\"failed\"}",
            )?,
            probe_degraded: metric(
                &text,
                "numax_membership_probe_results_total{outcome=\"local_degradation\"}",
            )?,
        });
        for line in text.lines().filter(|line| {
            line.starts_with("numax_queue_items{")
                || line.starts_with("numax_queue_bytes{")
                || line.starts_with("numax_queue_rejections_total{")
        }) {
            let (key, value) = line
                .rsplit_once(' ')
                .context("queue metric has no numeric value")?;
            *queues.entry(key.to_owned()).or_default() += value.parse::<u64>()?;
        }
    }
    Ok((total, queues))
}

#[derive(Default)]
struct Writer {
    offered: AtomicU64,
    accepted: AtomicU64,
    accepted_in_window: AtomicU64,
    max_pending: AtomicU64,
    max_flush_us: AtomicU64,
    max_start_lateness_us: AtomicU64,
    pending: std::sync::Mutex<BTreeMap<u64, StdInstant>>,
}

struct PendingSubmission {
    stats: Arc<Writer>,
    sequence: u64,
}

impl Drop for PendingSubmission {
    fn drop(&mut self) {
        self.stats
            .pending
            .lock()
            .expect("test pending submissions lock")
            .remove(&self.sequence);
    }
}

impl Writer {
    fn pending_sample(&self) -> anyhow::Result<serde_json::Value> {
        let pending = self
            .pending
            .lock()
            .map_err(|_| anyhow::anyhow!("pending submissions diagnostic lock poisoned"))?;
        let oldest = pending.iter().min_by_key(|(_, started)| **started);
        Ok(serde_json::json!({
            "count": pending.len(),
            "oldest_sequence": oldest.map(|(sequence, _)| *sequence),
            "oldest_elapsed_us": oldest
                .map(|(_, started)| u64::try_from(started.elapsed().as_micros()))
                .transpose()?,
        }))
    }
}

#[test]
fn live_flush_snapshot_does_not_wait_for_write_lease() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(NxStore::open(directory.path()).unwrap());
    store.set(b"live-flush", b"value").unwrap();
    let (reached_tx, reached_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    store.pause_next_flush(reached_tx, resume_rx);
    let flushing = store.clone();
    let worker = std::thread::spawn(move || flushing.flush());
    reached_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let observed = store.clone();
    let (snapshot_tx, snapshot_rx) = std::sync::mpsc::channel();
    let observer = std::thread::spawn(move || {
        snapshot_tx.send(observed.active_flush()).unwrap();
    });
    let snapshot = snapshot_rx.recv_timeout(Duration::from_secs(1));
    resume_tx.send(()).unwrap();
    worker.join().unwrap().unwrap();
    observer.join().unwrap();
    let (sequence, _) = snapshot.unwrap().expect("paused flush must be observable");
    assert!(store.active_flush().is_none());
    store.flush().unwrap();
    assert!(store.active_flush().is_none());
    assert_eq!(store.completed_flushes(), 2);
    store.inject_disk_full_on_flush(true);
    assert!(store.flush().is_err());
    assert!(store.active_flush().is_none());
    assert!(store.flush().is_err());
    assert!(store.active_flush().is_none());
    assert_eq!(sequence, 0);
}

#[test]
fn periodic_flush_disabled_preserves_explicit_flush_and_error_barrier() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(NxStore::open_without_periodic_flush(directory.path()).unwrap());
    assert!(store.periodic_flush_interval_ms().is_none());
    let (reached_tx, reached_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    store.pause_next_flush(reached_tx, resume_rx);
    let flushing = store.clone();
    let worker = std::thread::spawn(move || {
        let lease = flushing.acquire_write_lease().unwrap();
        lease
            .apply_batch(&[(b"periodic-control", b"value")], &[])
            .unwrap();
        done_tx.send(lease.flush()).unwrap();
    });
    reached_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let unfinished = done_rx.try_recv();
    let completed_flushes = store.completed_flushes();
    resume_tx.send(()).unwrap();
    worker.join().unwrap();
    assert!(matches!(
        unfinished,
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    assert_eq!(completed_flushes, 0);
    done_rx.recv().unwrap().unwrap();
    assert_eq!(store.completed_flushes(), 1);
    store.inject_disk_full_on_flush(true);
    assert!(matches!(
        store.flush(),
        Err(nx_store::StoreError::InjectedFlushFailure)
    ));
    assert!(matches!(
        store.set(b"next", b"value"),
        Err(nx_store::StoreError::DurabilityUncertain)
    ));
    drop(store);
    let reopened = NxStore::open(directory.path()).unwrap();
    assert_eq!(reopened.periodic_flush_interval_ms(), Some(500));
    assert_eq!(
        reopened.get(b"periodic-control").unwrap(),
        Some(b"value".to_vec())
    );
    let not_directory = directory.path().join("not-directory");
    std::fs::write(&not_directory, b"existing file").unwrap();
    assert!(matches!(
        NxStore::open_without_periodic_flush(&not_directory),
        Err(nx_store::StoreError::NotADirectory(_))
    ));
}
#[tokio::test]
async fn pending_submission_diagnostics_reset_after_cancellation() {
    let stats = Arc::new(Writer::default());
    stats.pending.lock().unwrap().insert(7, StdInstant::now());
    let submission = PendingSubmission {
        stats: stats.clone(),
        sequence: 7,
    };
    let (started, ready) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let _submission = submission;
        started.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    ready.await.unwrap();
    let snapshot = stats.pending_sample().unwrap();
    assert_eq!(snapshot["count"], 1);
    assert_eq!(snapshot["oldest_sequence"], 7);
    assert!(snapshot["oldest_elapsed_us"].is_u64());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(stats.pending_sample().unwrap()["count"], 0);
}

fn drain_limit(period: Duration) -> anyhow::Result<Duration> {
    Ok(period
        .checked_mul(4)
        .context("final drain duration overflow")?
        .max(Duration::from_secs(10)))
}

#[derive(Clone)]
enum WriteTarget {
    Replicated(Arc<SyncHandle>),
    Storage(Arc<NxStore>),
    StorageMetadata(Arc<NxStore>),
    StorageMetadataBatched(Arc<NxStore>),
}

fn storage_counter(store: &NxStore) -> anyhow::Result<u64> {
    match store.get(KEY.as_bytes())? {
        None => Ok(0),
        Some(bytes) => Ok(u64::from_le_bytes(bytes.try_into().map_err(|_| {
            anyhow::anyhow!("invalid storage baseline counter encoding")
        })?)),
    }
}

impl WriteTarget {
    async fn submit(&self) -> anyhow::Result<()> {
        match self {
            Self::Replicated(handle) => handle.submit_gcounter_increment(KEY, 1).await,
            Self::Storage(store)
            | Self::StorageMetadata(store)
            | Self::StorageMetadataBatched(store) => {
                let metadata = matches!(
                    self,
                    Self::StorageMetadata(_) | Self::StorageMetadataBatched(_)
                );
                let batched = matches!(self, Self::StorageMetadataBatched(_));
                let store = store.clone();
                tokio::task::spawn_blocking(move || {
                    let lease = store.acquire_write_lease()?;
                    let next = storage_counter(&store)?
                        .checked_add(1)
                        .context("storage baseline counter overflow")?
                        .to_le_bytes();
                    if batched {
                        let keys = (0..5)
                            .map(|index| storage_metadata_key(u64::from_le_bytes(next), index))
                            .collect::<Vec<_>>();
                        let payload = [0u8; 2048];
                        let mut sets = keys
                            .iter()
                            .map(|key| (key.as_bytes(), payload.as_slice()))
                            .collect::<Vec<_>>();
                        sets.push((KEY.as_bytes(), next.as_slice()));
                        lease.apply_batch(&sets, &[])?;
                    } else {
                        if metadata {
                            for index in 0..5 {
                                let key = storage_metadata_key(u64::from_le_bytes(next), index);
                                lease.apply_batch(&[(key.as_bytes(), &[0u8; 2048])], &[])?;
                            }
                        }
                        lease.apply_batch(&[(KEY.as_bytes(), &next)], &[])?;
                    }
                    lease.flush()?;
                    anyhow::Ok(())
                })
                .await?
            }
        }
    }
}

fn storage_metadata_key(sequence: u64, index: usize) -> String {
    format!("metadata/{sequence}/{index}")
}

fn verify_storage_metadata(store: &NxStore, operations: u64) -> anyhow::Result<()> {
    for sequence in 1..=operations {
        for index in 0..5 {
            ensure!(
                store
                    .get(storage_metadata_key(sequence, index).as_bytes())?
                    .as_deref()
                    == Some([0u8; 2048].as_slice()),
                "storage metadata record missing or invalid"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn storage_metadata_profile_flushes_all_records_and_reports_failure() {
    for batched in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(NxStore::open(directory.path()).unwrap());
        let target = |store| {
            if batched {
                WriteTarget::StorageMetadataBatched(store)
            } else {
                WriteTarget::StorageMetadata(store)
            }
        };
        target(store.clone()).submit().await.unwrap();
        assert_eq!(storage_counter(&store).unwrap(), 1);
        assert_eq!(store.completed_batch_work().0, if batched { 1 } else { 6 });
        assert_eq!(store.completed_flushes(), 1);
        verify_storage_metadata(&store, 1).unwrap();
        drop(store);
        let store = Arc::new(NxStore::open(directory.path()).unwrap());
        verify_storage_metadata(&store, 1).unwrap();
        store.inject_disk_full_on_flush(true);
        assert!(target(store.clone()).submit().await.is_err());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storage_baseline_requires_flush_and_reports_storage_errors() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(NxStore::open(directory.path()).unwrap());
    let (reached, checkpoint) = std::sync::mpsc::channel();
    let (resume, paused) = std::sync::mpsc::channel();
    store.pause_next_flush(reached, paused);
    let target = WriteTarget::Storage(store.clone());
    let task = tokio::spawn(async move { target.submit().await });
    let checkpoint =
        tokio::task::spawn_blocking(move || checkpoint.recv_timeout(Duration::from_secs(5)))
            .await
            .unwrap();
    let returned_before_flush = task.is_finished();
    resume.send(()).unwrap();
    checkpoint.unwrap();
    assert!(!returned_before_flush);
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(store.completed_flushes(), 1);
    assert_eq!(storage_counter(&store).unwrap(), 1);
    drop(store);
    let reopened = NxStore::open(directory.path()).unwrap();
    assert_eq!(storage_counter(&reopened).unwrap(), 1);

    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(NxStore::open(directory.path()).unwrap());
    store.inject_disk_full_on_flush(true);
    assert!(WriteTarget::Storage(store.clone()).submit().await.is_err());
    assert_eq!(store.completed_flushes(), 0);

    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(NxStore::open(directory.path()).unwrap());
    store.set(KEY.as_bytes(), b"invalid").unwrap();
    let error = WriteTarget::Storage(store.clone())
        .submit()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("counter encoding"));
    assert_eq!(store.completed_flushes(), 0);
}

async fn write_load(
    target: WriteTarget,
    stats: Arc<Writer>,
    start: Instant,
    offset: Duration,
    duration: Duration,
    period: Duration,
    index: usize,
) -> anyhow::Result<()> {
    let final_drain = drain_limit(period)?;
    ensure!(
        period.subsec_nanos() == 0
            && period.as_secs() > 0
            && duration.as_secs().is_multiple_of(period.as_secs()),
        "invalid nominal load period"
    );
    let mut pending = JoinSet::new();
    let result = async {
        for step in 0..duration.as_secs() / period.as_secs() {
            let due = start
                + Duration::from_secs(
                    step.checked_mul(period.as_secs())
                        .context("load schedule overflow")?,
                )
                + offset;
            tokio::time::sleep_until(due).await;
            let lateness = Instant::now().saturating_duration_since(due);
            stats
                .max_start_lateness_us
                .fetch_max(u64::try_from(lateness.as_micros())?, Ordering::Relaxed);
            ensure!(
                lateness < period,
                "nominal writer {index} missed its scheduled period"
            );
            while let Some(result) = pending.try_join_next() {
                result??;
            }
            ensure!(
                pending.len() < 4,
                "nominal writer {index} saturated its four pending submissions"
            );
            stats.offered.fetch_add(1, Ordering::Relaxed);
            stats
                .max_pending
                .fetch_max(u64::try_from(pending.len() + 1)?, Ordering::Relaxed);
            let target = target.clone();
            let stats = stats.clone();
            {
                let mut active = stats
                    .pending
                    .lock()
                    .map_err(|_| anyhow::anyhow!("pending submissions diagnostic lock poisoned"))?;
                ensure!(active.len() < 4, "pending diagnostic exceeded writer limit");
                ensure!(
                    active.insert(step, StdInstant::now()).is_none(),
                    "duplicate pending submission diagnostic identity"
                );
            }
            let submission = PendingSubmission {
                stats: stats.clone(),
                sequence: step,
            };
            pending.spawn(async move {
                let _submission = submission;
                let flush_started = StdInstant::now();
                target.submit().await?;
                stats.accepted.fetch_add(1, Ordering::Relaxed);
                if Instant::now() <= start + duration {
                    stats.accepted_in_window.fetch_add(1, Ordering::Relaxed);
                }
                stats.max_flush_us.fetch_max(
                    u64::try_from(flush_started.elapsed().as_micros())?,
                    Ordering::Relaxed,
                );
                anyhow::Ok(())
            });
        }
        tokio::time::timeout_at(start + duration + final_drain, async {
            while let Some(result) = pending.join_next().await {
                result??;
            }
            anyhow::Ok(())
        })
        .await
        .context("nominal submissions exceeded the bounded final drain")??;
        anyhow::Ok(())
    }
    .await;
    pending.shutdown().await;
    result
}

struct Cluster {
    nodes: Vec<SyncManager>,
    directories: Vec<tempfile::TempDir>,
}

impl Cluster {
    async fn populate(&mut self, count: usize, format: SerializationFormat) -> anyhow::Result<()> {
        let addresses = (0..count).map(|_| free_addr()).collect::<Vec<_>>();
        for index in 0..count {
            let directory = tempfile::tempdir()?;
            let config = SyncConfig::new()
                .with_listen_addr(addresses[index].clone())
                .with_max_peers(4)
                .with_serialization_format(format)
                .with_peer(addresses[(index + count - 1) % count].clone())
                .with_peer(addresses[(index + 1) % count].clone());
            let store = Arc::new(NxStore::open(directory.path())?);
            self.nodes.push(SyncManager::try_new(
                NodeId::new(format!("detector-{index}")),
                config,
                store,
                metrics(),
            )?);
            self.directories.push(directory);
            self.nodes.last_mut().unwrap().start().await?;
        }
        Ok(())
    }

    async fn bootstrap(&self) -> anyhow::Result<()> {
        let mut owners = Vec::new();
        for node in &self.nodes {
            let mut view = node.membership.as_ref().unwrap().snapshot.clone();
            let snapshot = view
                .wait_for(|snapshot| {
                    snapshot
                        .records
                        .iter()
                        .any(|record| record.member == node.node_id.as_str())
                })
                .await?;
            owners.push(
                snapshot
                    .records
                    .iter()
                    .find(|record| record.member == node.node_id.as_str())
                    .context("bootstrap owner missing")?
                    .clone(),
            );
        }
        let count = self.nodes.len();
        let limited = Arc::new(AtomicU64::new(0));
        // Each round has one outbound and one inbound setup request per node.
        for round in 1..count {
            let mut jobs = JoinSet::new();
            for (index, node) in self.nodes.iter().enumerate() {
                let target = &owners[(index + round) % count];
                let endpoint = target
                    .endpoint
                    .clone()
                    .context("bootstrap target has no endpoint")?;
                let expected = target.member.clone();
                let network = node
                    .node
                    .as_ref()
                    .context("bootstrap node has no transport")?
                    .clone();
                let request = nx_net::ControlRequest {
                    request_id: 1_000_000 + u64::try_from(round * count + index)?,
                    owner: owners[index].clone(),
                    updates: vec![],
                    kind: nx_net::ControlKind::Exchange { after: None },
                };
                let limited = limited.clone();
                jobs.spawn(async move {
                    let retries = exchange_owner(&network, &endpoint, request, &expected)
                        .await
                        .with_context(|| {
                            format!("bootstrap round={round} node={index} target={expected}")
                        })?;
                    limited.fetch_add(retries, Ordering::Relaxed);
                    anyhow::Ok(())
                });
            }
            while let Some(result) = jobs.join_next().await {
                result??;
            }
        }
        eprintln!(
            "DETECTOR_PHASE nodes={count} phase=bootstrap-exchanges-complete backpressure_retries={}",
            limited.load(Ordering::Relaxed)
        );
        Ok(())
    }

    async fn formed(&self) -> anyhow::Result<()> {
        let started = StdInstant::now();
        let result = tokio::time::timeout(FORMATION_LIMIT, async {
            let mut poll = tokio::time::interval(Duration::from_millis(100));
            let mut last_progress = 0;
            loop {
                poll.tick().await;
                let ready = self.nodes.iter().all(|node| {
                    let snapshot = node.membership.as_ref().unwrap().snapshot.borrow();
                    snapshot.error.is_none()
                        && snapshot.records.len() == self.nodes.len()
                        && snapshot
                            .records
                            .iter()
                            .all(|record| record.state == MembershipState::Alive)
                        && snapshot.successful_probes > 0
                });
                if ready {
                    return;
                }
                if started.elapsed().as_secs() / 30 > last_progress {
                    last_progress = started.elapsed().as_secs() / 30;
                    let sizes = self.nodes.iter().map(|node| {
                        node.membership.as_ref().unwrap().snapshot.borrow().records.len()
                    }).collect::<Vec<_>>();
                    eprintln!("DETECTOR_FORMATION_PROGRESS {}", serde_json::json!({
                        "elapsed_s": started.elapsed().as_secs(),
                        "min_records": sizes.iter().min(), "max_records": sizes.iter().max(),
                        "complete_views": sizes.iter().filter(|size| **size == self.nodes.len()).count()
                    }));
                }
            }
        })
        .await;
        if result.is_err() {
            let nodes = self.nodes.iter().map(|node| {
                let snapshot = node.membership.as_ref().unwrap().snapshot.borrow();
                serde_json::json!({
                    "node": node.node_id.as_str(), "records": snapshot.records.len(),
                    "suspect": snapshot.records.iter().filter(|record| record.state == MembershipState::Suspect).count(),
                    "dead": snapshot.records.iter().filter(|record| record.state == MembershipState::Dead).count(),
                    "left": snapshot.records.iter().filter(|record| record.state == MembershipState::Left).count(),
                    "health": snapshot.health, "probe_success": snapshot.successful_probes,
                    "probe_failed": snapshot.failed_probes, "error": snapshot.error
                })
            }).collect::<Vec<_>>();
            eprintln!(
                "DETECTOR_FORMATION_FAILURE {}",
                serde_json::json!({
                    "elapsed_s": started.elapsed().as_secs_f64(), "nodes": nodes,
                    "counts": observe(&self.nodes)?.0.json(self.nodes.len() as f64 * started.elapsed().as_secs_f64() / 3600.0)
                })
            );
        }
        result.context("detector cluster did not form")?;
        ensure!(
            started.elapsed() <= FORMATION_LIMIT,
            "formation completed late"
        );
        Ok(())
    }

    async fn stop(&mut self) -> anyhow::Result<()> {
        let mut jobs = JoinSet::new();
        for mut node in self.nodes.drain(..) {
            jobs.spawn(async move { node.shutdown().await });
        }
        let result = tokio::time::timeout(Duration::from_secs(120), async {
            while let Some(result) = jobs.join_next().await {
                result??;
            }
            anyhow::Ok(())
        })
        .await
        .context("detector cluster shutdown timed out")?;
        jobs.shutdown().await;
        result
    }
}

fn cpu_seconds(value: &str) -> anyhow::Result<f64> {
    let (days, clock) = match value.split_once('-') {
        Some((days, clock)) => (days.parse::<u64>()?, clock),
        None => (0, value),
    };
    let parts = clock
        .split(':')
        .map(str::parse::<f64>)
        .collect::<Result<Vec<_>, _>>()?;
    let seconds = match parts.as_slice() {
        [minutes, seconds] => minutes * 60.0 + seconds,
        [hours, minutes, seconds] => hours * 3600.0 + minutes * 60.0 + seconds,
        _ => anyhow::bail!("unsupported ps CPU time: {value}"),
    } + days as f64 * 86400.0;
    ensure!(
        parts.iter().all(|part| part.is_finite() && *part >= 0.0),
        "invalid ps CPU time component"
    );
    ensure!(
        parts.last().is_some_and(|seconds| *seconds < 60.0),
        "invalid ps CPU seconds"
    );
    if parts.len() == 3 {
        ensure!(parts[1] < 60.0, "invalid ps CPU minutes");
    }
    ensure!(seconds.is_finite() && seconds >= 0.0, "invalid ps CPU time");
    Ok(seconds)
}

async fn process_usage() -> anyhow::Result<(u64, f64)> {
    tokio::task::spawn_blocking(|| {
        let output = std::process::Command::new("ps")
            .args(["-p", &std::process::id().to_string(), "-o", "rss=,time="])
            .output()
            .context("cannot sample detector process resources with ps")?;
        ensure!(
            output.status.success(),
            "ps failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = std::str::from_utf8(&output.stdout)?;
        let parts = text.split_whitespace().collect::<Vec<_>>();
        ensure!(parts.len() == 2, "unexpected ps resource sample: {text}");
        let rss = parts[0]
            .parse::<u64>()?
            .checked_mul(1024)
            .context("RSS overflow")?;
        anyhow::Ok((rss, cpu_seconds(parts[1])?))
    })
    .await?
}

async fn nominal(cluster: &Cluster, duration: Duration) -> anyhow::Result<serde_json::Value> {
    let count = cluster.nodes.len();
    healthy(&cluster.nodes, None)?;
    let (before, _) = observe(&cluster.nodes)?;
    let (rss_start, cpu_start) = process_usage().await?;
    let started = StdInstant::now();
    let clock_start = Instant::now();
    let mut writers = JoinSet::new();
    let stats = (0..count)
        .map(|_| Arc::new(Writer::default()))
        .collect::<Vec<_>>();
    for (index, node) in cluster.nodes.iter().enumerate() {
        let handle = node.handle();
        let stats = stats[index].clone();
        let offset = Duration::from_micros(u64::try_from(index)? * 500_000 / u64::try_from(count)?);
        writers.spawn(write_load(
            WriteTarget::Replicated(Arc::new(handle)),
            stats,
            clock_start,
            offset,
            duration,
            Duration::from_secs(1),
            index,
        ));
    }
    let mut peak_queues = BTreeMap::<String, u64>::new();
    let mut rss_peak = rss_start;
    let mut samples = 1u64;
    let mut last_resource_sample = Duration::ZERO;
    let mut last_progress = 0;
    let result = async {
        let mut poll = tokio::time::interval(Duration::from_secs(1));
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(clock_start + duration) => break,
                result = writers.join_next(), if !writers.is_empty() => {
                    result.context("writer result missing")???;
                }
                _ = poll.tick() => {
                    let (current, queues) = observe(&cluster.nodes)?;
                    let last_counts = current.delta(&before);
                    ensure!(last_counts.declared_dead == 0 && last_counts.learned_dead == 0,
                        "false failure verdict during nominal load: {last_counts:?}");
                    healthy(&cluster.nodes, None)?;
                    for (key, value) in queues {
                        peak_queues.entry(key).and_modify(|peak| *peak = (*peak).max(value)).or_insert(value);
                    }
                    let elapsed = started.elapsed();
                    if elapsed.saturating_sub(last_resource_sample) >= Duration::from_secs(10) {
                        rss_peak = rss_peak.max(process_usage().await?.0);
                        samples += 1;
                        last_resource_sample = elapsed;
                    }
                    if elapsed.as_secs() / 60 > last_progress {
                        last_progress = elapsed.as_secs() / 60;
                        println!("DETECTOR_PROGRESS {}", serde_json::json!({
                            "elapsed_s": elapsed.as_secs(), "nodes": count,
                            "accepted": stats.iter().map(|stats| stats.accepted.load(Ordering::Relaxed)).sum::<u64>(),
                            "suspect_observations": last_counts.suspect,
                            "refuted_observations": last_counts.refuted,
                            "false_local_declarations": last_counts.declared_dead,
                            "rss_sampled_peak_bytes": rss_peak
                        }));
                    }
                }
            }
        }
        ensure!(started.elapsed() >= duration, "nominal observation ended before the required real duration");
        while let Some(result) = writers.try_join_next() {
            result??;
        }
        ensure!(writers.is_empty(), "nominal writers did not finish within the observation window");
        for (index, stats) in stats.iter().enumerate() {
            ensure!(stats.accepted.load(Ordering::Relaxed) == duration.as_secs(),
                "writer {index} did not achieve one accepted operation per second");
        }
        let last_counts = observe(&cluster.nodes)?.0.delta(&before);
        ensure!(last_counts.declared_dead == 0 && last_counts.learned_dead == 0,
            "false failure verdict at observation close: {last_counts:?}");
        ensure!(last_counts.probe_success > 0, "no successful probes during nominal observation");
        healthy(&cluster.nodes, None)?;
        anyhow::Ok(())
    }.await;
    let elapsed = started.elapsed();
    writers.shutdown().await;
    let (rss_end, cpu_end) = process_usage().await?;
    ensure!(cpu_end >= cpu_start, "process CPU time regressed");
    let observer_hours = count as f64 * elapsed.as_secs_f64() / 3600.0;
    let after = observe(&cluster.nodes)?.0.delta(&before);
    let report = serde_json::json!({
        "requested_duration_s": duration.as_secs(), "observed_duration_s": elapsed.as_secs_f64(),
        "passed": result.is_ok(), "error": result.as_ref().err().map(|error| format!("{error:#}")),
        "formation_counts_excluded_from_nominal_rates": {
            "suspect": before.suspect, "refuted": before.refuted,
            "declared_dead": before.declared_dead, "learned_dead": before.learned_dead},
        "counts": after.json(observer_hours), "observer_hours": observer_hours,
        "interpretation": "all subjects were intentionally healthy; suspect observations are false-positive suspicions, not unique cluster-wide incidents; refuted and learned events can be observed by multiple nodes",
        "workload": {"family": "GCounter", "key": KEY, "increment": 1,
            "key_bytes": KEY.len(), "increment_bytes": 8, "wire_frame_bytes": "not measured; framing, IDs and batching add variable overhead",
            "target_ops_per_s_per_node": 1, "schedule": "fixed one-second periods, node offsets within the first half-second; no catch-up after a missed second",
            "accepted_per_node": stats.iter().map(|stats| stats.accepted.load(Ordering::Relaxed)).collect::<Vec<_>>(),
            "max_flush_confirmed_submit_us": stats.iter().map(|stats| stats.max_flush_us.load(Ordering::Relaxed)).max(),
            "max_writer_start_lateness_us": stats.iter().map(|stats| stats.max_start_lateness_us.load(Ordering::Relaxed)).max()},
        "resources": {"scope": "entire isolated cluster process, not per-node",
            "rss_start_bytes": rss_start, "rss_end_bytes": rss_end,
            "rss_sampled_peak_bytes": rss_peak.max(rss_end), "rss_samples": samples + 1,
            "rss_sample_period_s": 10, "cpu_seconds": cpu_end - cpu_start,
            "cpu_percent_one_core": (cpu_end - cpu_start) / elapsed.as_secs_f64() * 100.0,
            "sampled_aggregate_queue_peaks_and_rejection_totals": peak_queues}
    });
    println!("DETECTOR_NOMINAL_REPORT {report}");
    result?;
    Ok(report)
}

async fn failure(cluster: &mut Cluster) -> anyhow::Result<serde_json::Value> {
    let target_index = cluster.nodes.len() - 1;
    let target_id = cluster.nodes[target_index].node_id.to_string();
    healthy(&cluster.nodes, None)?;
    let before_by_observer = cluster
        .nodes
        .iter()
        .map(|node| observe(std::slice::from_ref(node)).map(|sample| sample.0))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let before = observe(&cluster.nodes)?.0;
    let started = StdInstant::now();
    let membership = cluster.nodes[target_index]
        .membership
        .take()
        .context("fault target has no membership actor")?;
    let mut closed = membership.snapshot.clone();
    // Drop aborts the actor without persisting or publishing a graceful leave.
    drop(membership);
    tokio::time::timeout(Duration::from_secs(5), async {
        while closed.changed().await.is_ok() {}
    })
    .await
    .context("fault target membership actor did not stop")?;
    ensure!(
        !closed
            .borrow()
            .records
            .iter()
            .any(|record| record.member == target_id && record.state == MembershipState::Left),
        "fault injection unexpectedly performed a graceful leave"
    );
    cluster.nodes[target_index]
        .node
        .as_ref()
        .context("fault target has no transport")?
        .shutdown()
        .await;
    let outage_confirmed_ms = started.elapsed().as_millis();
    let mut suspect_ms = vec![None; target_index];
    let mut dead_ms = vec![None; target_index];
    let result = tokio::time::timeout(DETECTION_LIMIT, async {
        let mut poll = tokio::time::interval(Duration::from_millis(100));
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            poll.tick().await;
            healthy(&cluster.nodes, Some(&target_id))?;
            for (index, node) in cluster.nodes[..target_index].iter().enumerate() {
                let snapshot = node.membership.as_ref().unwrap().snapshot.borrow();
                if let Some(record) = snapshot
                    .records
                    .iter()
                    .find(|record| record.member == target_id)
                {
                    if record.state == MembershipState::Suspect && suspect_ms[index].is_none() {
                        suspect_ms[index] = Some(started.elapsed().as_millis());
                    }
                    if record.state == MembershipState::Dead && dead_ms[index].is_none() {
                        dead_ms[index] = Some(started.elapsed().as_millis());
                    }
                    ensure!(
                        record.state != MembershipState::Left,
                        "fault target was learned as graceful leave"
                    );
                }
            }
            if dead_ms.iter().all(Option::is_some) {
                return anyhow::Ok(());
            }
        }
    })
    .await;
    let counts = observe(&cluster.nodes)?.0.delta(&before);
    let observers = cluster.nodes[..target_index]
        .iter()
        .enumerate()
        .map(|(index, node)| {
            let current = observe(std::slice::from_ref(node))?
                .0
                .delta(&before_by_observer[index]);
            anyhow::Ok(serde_json::json!({
                "node": node.node_id.as_str(),
                "suspect_observed_ms": suspect_ms[index], "dead_observed_ms": dead_ms[index],
                "local_dead_declarations": current.declared_dead,
                "learned_dead_observations": current.learned_dead
            }))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let report = serde_json::json!({
        "target": target_id, "fault": "abort membership actor, await snapshot closure, stop actual TCP transport; no graceful leave",
        "outage_confirmed_after_request_ms": outage_confirmed_ms,
        "latency_reference": "fault request; actual transport unavailability begins within the request/confirmation interval",
        "sampling_ms": 100, "observation_limit_s": DETECTION_LIMIT.as_secs(),
        "new_writes_during_failure_observation": false,
        "suspect_observed_ms_by_survivor": suspect_ms,
        "dead_observed_ms_by_survivor": dead_ms,
        "suspect_null_means": "no suspect state sampled; a survivor may learn Dead directly",
        "counts": counts.json(cluster.nodes.len() as f64 * started.elapsed().as_secs_f64() / 3600.0),
        "observers": observers,
        "elapsed_ms": started.elapsed().as_millis(),
        "passed": matches!(&result, Ok(Ok(()))) && started.elapsed() <= DETECTION_LIMIT,
    });
    println!("DETECTOR_FAILURE_REPORT {report}");
    result.context("not every survivor detected the real transport failure")??;
    ensure!(
        started.elapsed() <= DETECTION_LIMIT,
        "failure detection completed late"
    );
    ensure!(
        counts.declared_dead > 0,
        "no detector declared the failed node dead"
    );
    Ok(report)
}

async fn scenario(
    count: usize,
    duration: Duration,
    format: SerializationFormat,
) -> anyhow::Result<()> {
    let mut cluster = Cluster {
        nodes: Vec::new(),
        directories: Vec::new(),
    };
    println!(
        "DETECTOR_CONFIG {}",
        serde_json::json!({
            "version": 1, "nodes": count, "codec": format!("{format:?}"),
            "platform": std::env::consts::OS, "nominal_seconds": duration.as_secs(),
            "topology": "candidate ring; bounded real TCP owner exchanges prepare full membership; production data-neighbor rotation",
            "max_data_peers_per_node": 4,
            "detector_timing": "unmodified production defaults: 2s probe interval, 500ms RPC budget, 5-15s suspicion, health max 8",
            "randomness": "production OS-seeded probe choices; wall-clock scheduling is not deterministic replay",
            "transport": "real unauthenticated loopback TCP; no injected packet loss",
            "retention_operations_per_node": crate::sync_config::DEFAULT_OP_LOG_LIMIT
        })
    );
    let result = async {
        cluster.populate(count, format).await?;
        cluster.bootstrap().await?;
        cluster.formed().await?;
        eprintln!("DETECTOR_PHASE nodes={count} codec={format:?} phase=formed nominal_s={}", duration.as_secs());
        let nominal = nominal(&cluster, duration).await?;
        let failure = failure(&mut cluster).await?;
        println!("DETECTOR_REPORT {}", serde_json::json!({
            "version": 1, "nodes": count, "codec": format!("{format:?}"),
            "platform": std::env::consts::OS,
            "topology": "candidate ring; bounded real TCP owner exchanges prepare full membership; production data-neighbor rotation",
            "max_data_peers_per_node": 4,
            "detector_timing": "unmodified production defaults: 2s probe interval, 500ms RPC budget, 5-15s suspicion, health max 8",
            "randomness": "production OS-seeded probe choices; wall-clock scheduling is not deterministic replay",
            "transport": "real unauthenticated loopback TCP; no injected packet loss",
            "retention_operations_per_node": crate::sync_config::DEFAULT_OP_LOG_LIMIT,
            "nominal": nominal, "true_transport_failure": failure
        }));
        anyhow::Ok(())
    }.await;
    let shutdown = cluster.stop().await;
    ensure!(
        result.is_ok() && shutdown.is_ok(),
        "detector scenario={result:?}; shutdown={shutdown:?}"
    );
    Ok(())
}

#[test]
fn resource_cpu_time_formats_and_required_metrics() {
    assert_eq!(cpu_seconds("2:03.50").unwrap(), 123.5);
    assert_eq!(cpu_seconds("01:02:03").unwrap(), 3723.0);
    assert_eq!(cpu_seconds("2-01:02:03").unwrap(), 176523.0);
    assert!(cpu_seconds("invalid").is_err());
    assert!(cpu_seconds("NaN:00").is_err());
    assert!(cpu_seconds("1:-5").is_err());
    assert!(cpu_seconds("1:60").is_err());
    assert_eq!(
        metric("sample{a=\"b\"} 12\n", "sample{a=\"b\"}").unwrap(),
        12
    );
    assert!(metric("", "missing").is_err());
    assert!(metric("sample invalid\n", "sample").is_err());
}
