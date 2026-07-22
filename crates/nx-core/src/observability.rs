use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Result, anyhow};
use nx_store::Store as NxStore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{debug, warn};

pub const DEFAULT_OBSERVABILITY_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_WASM_MODULE_METRIC_LABELS: usize = 128;
const WASM_MODULE_OVERFLOW_LABEL: &str = "overflow";
const REMOTE_OP_APPLY_BUCKETS: [(&str, u64); 15] = [
    ("0.0001", 100_000),
    ("0.00025", 250_000),
    ("0.0005", 500_000),
    ("0.001", 1_000_000),
    ("0.0025", 2_500_000),
    ("0.005", 5_000_000),
    ("0.01", 10_000_000),
    ("0.025", 25_000_000),
    ("0.05", 50_000_000),
    ("0.1", 100_000_000),
    ("0.25", 250_000_000),
    ("0.5", 500_000_000),
    ("1", 1_000_000_000),
    ("2.5", 2_500_000_000),
    ("5", 5_000_000_000),
];

#[derive(Debug, Clone)]
pub struct ObservabilityConfig {
    pub listen_addr: String,
    pub request_timeout: Duration,
}

impl ObservabilityConfig {
    pub fn new(listen_addr: impl Into<String>) -> Self {
        Self {
            listen_addr: listen_addr.into(),
            request_timeout: DEFAULT_OBSERVABILITY_REQUEST_TIMEOUT,
        }
    }

    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = request_timeout;
        self
    }
}

#[derive(Default)]
pub struct RuntimeMetrics {
    ops_total: AtomicU64,
    peers_connected: AtomicUsize,
    sync_latency_ms: AtomicU64,
    sync_errors_total: AtomicU64,
    observability_requests_total: AtomicU64,
    observability_errors_total: AtomicU64,
    peer_connects_total: AtomicU64,
    peer_disconnects_total: AtomicU64,
    broadcast_batches_total: AtomicU64,
    broadcast_ops_total: AtomicU64,
    remote_ops_received_total: AtomicU64,
    remote_ops_applied_total: AtomicU64,
    remote_ops_duplicate_total: AtomicU64,
    remote_op_batches_total: AtomicU64,
    remote_op_apply_errors_total: AtomicU64,
    remote_op_batch_apply_duration: DurationHistogram,
    wasm_modules: Mutex<BTreeMap<String, WasmModuleMetrics>>,
    ready: AtomicBool,
}

#[derive(Clone, Default)]
struct WasmModuleMetrics {
    invocations_ok: u64,
    invocations_error: u64,
    cache_hits: u64,
    cache_misses: u64,
    compilation_duration_ns: u64,
    instantiations: u64,
    instantiation_duration_ns: u64,
    executions: u64,
    execution_duration_ns: u64,
    linear_memory_current_bytes: u64,
    linear_memory_peak_bytes: u64,
    linear_memory_growth_bytes: u64,
}

#[derive(Default)]
struct DurationHistogram {
    buckets: [AtomicU64; REMOTE_OP_APPLY_BUCKETS.len()],
    sum_ns: AtomicU64,
    count: AtomicU64,
}

impl DurationHistogram {
    fn record(&self, duration: Duration) {
        let nanoseconds = duration_ns(duration);
        // Increment count before the finite bucket so a concurrent scrape
        // never observes a bucket value greater than the +Inf bucket.
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_ns.fetch_add(nanoseconds, Ordering::Relaxed);
        if let Some(index) = REMOTE_OP_APPLY_BUCKETS
            .iter()
            .position(|(_, upper_bound_ns)| nanoseconds <= *upper_bound_ns)
        {
            self.buckets[index].fetch_add(1, Ordering::Relaxed);
        }
    }

    fn snapshot(&self) -> DurationHistogramSnapshot {
        DurationHistogramSnapshot {
            buckets: std::array::from_fn(|index| self.buckets[index].load(Ordering::Relaxed)),
            sum_ns: self.sum_ns.load(Ordering::Relaxed),
            count: self.count.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default)]
struct DurationHistogramSnapshot {
    buckets: [u64; REMOTE_OP_APPLY_BUCKETS.len()],
    sum_ns: u64,
    count: u64,
}

impl RuntimeMetrics {
    pub fn record_ops(&self, count: u64) {
        self.ops_total.fetch_add(count, Ordering::Relaxed);
    }

    pub fn set_peers_connected(&self, count: usize) {
        self.peers_connected.store(count, Ordering::Relaxed);
    }

    pub fn record_sync_latency(&self, duration: Duration) {
        self.sync_latency_ms
            .store(duration.as_millis() as u64, Ordering::Relaxed);
    }

    pub fn record_sync_error(&self) {
        self.sync_errors_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_observability_request(&self) {
        self.observability_requests_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_observability_error(&self) {
        self.observability_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_peer_connect(&self) {
        self.peer_connects_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_peer_disconnect(&self) {
        self.peer_disconnects_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_broadcast_batch(&self, ops: usize) {
        self.broadcast_batches_total.fetch_add(1, Ordering::Relaxed);
        self.broadcast_ops_total
            .fetch_add(ops as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_remote_op_batch(
        &self,
        received: usize,
        applied: usize,
        duplicates: usize,
        duration: Duration,
        failed: bool,
    ) {
        self.remote_ops_received_total
            .fetch_add(received as u64, Ordering::Relaxed);
        self.remote_ops_applied_total
            .fetch_add(applied as u64, Ordering::Relaxed);
        self.remote_ops_duplicate_total
            .fetch_add(duplicates as u64, Ordering::Relaxed);
        self.remote_op_batches_total.fetch_add(1, Ordering::Relaxed);
        if failed {
            self.remote_op_apply_errors_total
                .fetch_add(1, Ordering::Relaxed);
        }
        self.remote_op_batch_apply_duration.record(duration);
    }

    pub(crate) fn record_wasm_cache_lookup(&self, module: &str, hit: bool) {
        self.update_wasm_module(module, |metrics| {
            if hit {
                metrics.cache_hits = metrics.cache_hits.saturating_add(1);
            } else {
                metrics.cache_misses = metrics.cache_misses.saturating_add(1);
            }
        });
    }

    pub(crate) fn record_wasm_compilation(&self, module: &str, duration: Duration) {
        self.update_wasm_module(module, |metrics| {
            metrics.compilation_duration_ns = metrics
                .compilation_duration_ns
                .saturating_add(duration_ns(duration));
        });
    }

    pub(crate) fn record_wasm_instantiation(&self, module: &str, duration: Duration) {
        self.update_wasm_module(module, |metrics| {
            metrics.instantiations = metrics.instantiations.saturating_add(1);
            metrics.instantiation_duration_ns = metrics
                .instantiation_duration_ns
                .saturating_add(duration_ns(duration));
        });
    }

    pub(crate) fn record_wasm_execution(
        &self,
        module: &str,
        duration: Duration,
        initial_memory_bytes: u64,
        final_memory_bytes: u64,
    ) {
        self.update_wasm_module(module, |metrics| {
            metrics.executions = metrics.executions.saturating_add(1);
            metrics.execution_duration_ns = metrics
                .execution_duration_ns
                .saturating_add(duration_ns(duration));
            metrics.linear_memory_current_bytes = final_memory_bytes;
            metrics.linear_memory_peak_bytes =
                metrics.linear_memory_peak_bytes.max(final_memory_bytes);
            metrics.linear_memory_growth_bytes = metrics
                .linear_memory_growth_bytes
                .saturating_add(final_memory_bytes.saturating_sub(initial_memory_bytes));
        });
    }

    pub(crate) fn record_wasm_invocation(&self, module: &str, success: bool) {
        self.update_wasm_module(module, |metrics| {
            if success {
                metrics.invocations_ok = metrics.invocations_ok.saturating_add(1);
            } else {
                metrics.invocations_error = metrics.invocations_error.saturating_add(1);
            }
        });
    }

    fn update_wasm_module(&self, module: &str, update: impl FnOnce(&mut WasmModuleMetrics)) {
        let mut modules = self
            .wasm_modules
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let label =
            if modules.contains_key(module) || modules.len() < MAX_WASM_MODULE_METRIC_LABELS - 1 {
                module
            } else {
                WASM_MODULE_OVERFLOW_LABEL
            };
        if let Some(metrics) = modules.get_mut(label) {
            update(metrics);
            return;
        }

        let mut metrics = WasmModuleMetrics::default();
        update(&mut metrics);
        modules.insert(label.to_string(), metrics);
    }

    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Relaxed);
    }

    fn snapshot(&self, store: &NxStore) -> MetricsSnapshot {
        let store_stats = match store.stats() {
            Ok(stats) => stats,
            Err(e) => {
                warn!(error = %e, "failed to read store stats");
                nx_store::StoreStats { keys: 0, bytes: 0 }
            }
        };

        let wasm_modules = self
            .wasm_modules
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();

        MetricsSnapshot {
            ops_total: self.ops_total.load(Ordering::Relaxed),
            peers_connected: self.peers_connected.load(Ordering::Relaxed),
            sync_latency_ms: self.sync_latency_ms.load(Ordering::Relaxed),
            sync_errors_total: self.sync_errors_total.load(Ordering::Relaxed),
            observability_requests_total: self.observability_requests_total.load(Ordering::Relaxed),
            observability_errors_total: self.observability_errors_total.load(Ordering::Relaxed),
            peer_connects_total: self.peer_connects_total.load(Ordering::Relaxed),
            peer_disconnects_total: self.peer_disconnects_total.load(Ordering::Relaxed),
            broadcast_batches_total: self.broadcast_batches_total.load(Ordering::Relaxed),
            broadcast_ops_total: self.broadcast_ops_total.load(Ordering::Relaxed),
            remote_ops_received_total: self.remote_ops_received_total.load(Ordering::Relaxed),
            remote_ops_applied_total: self.remote_ops_applied_total.load(Ordering::Relaxed),
            remote_ops_duplicate_total: self.remote_ops_duplicate_total.load(Ordering::Relaxed),
            remote_op_batches_total: self.remote_op_batches_total.load(Ordering::Relaxed),
            remote_op_apply_errors_total: self.remote_op_apply_errors_total.load(Ordering::Relaxed),
            remote_op_batch_apply_duration: self.remote_op_batch_apply_duration.snapshot(),
            store_keys: store_stats.keys,
            store_bytes: store_stats.bytes,
            wasm_modules,
        }
    }

    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn render_for_test(&self, store: &NxStore) -> String {
        render_metrics(self.snapshot(store))
    }
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

struct MetricsSnapshot {
    ops_total: u64,
    peers_connected: usize,
    sync_latency_ms: u64,
    sync_errors_total: u64,
    observability_requests_total: u64,
    observability_errors_total: u64,
    peer_connects_total: u64,
    peer_disconnects_total: u64,
    broadcast_batches_total: u64,
    broadcast_ops_total: u64,
    remote_ops_received_total: u64,
    remote_ops_applied_total: u64,
    remote_ops_duplicate_total: u64,
    remote_op_batches_total: u64,
    remote_op_apply_errors_total: u64,
    remote_op_batch_apply_duration: DurationHistogramSnapshot,
    store_keys: u64,
    store_bytes: u64,
    wasm_modules: BTreeMap<String, WasmModuleMetrics>,
}

pub struct ObservabilityServer {
    shutdown_tx: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl ObservabilityServer {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        if let Err(e) = self.task.await {
            warn!(error = %e, "observability server failed during shutdown");
        }
    }
}

pub async fn start_server(
    config: ObservabilityConfig,
    metrics: Arc<RuntimeMetrics>,
    store: Arc<NxStore>,
) -> Result<(SocketAddr, ObservabilityServer)> {
    let listener = TcpListener::bind(&config.listen_addr).await?;
    let bound_addr = listener.local_addr()?;
    let request_timeout = config.request_timeout;
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        debug!("observability server shutdown requested");
                        break;
                    }
                }
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _addr)) => {
                            let metrics = Arc::clone(&metrics);
                            let store = Arc::clone(&store);
                            tokio::spawn(async move {
                                let metrics_for_error = Arc::clone(&metrics);
                                if let Err(e) = handle_connection(stream, metrics, store, request_timeout).await {
                                    metrics_for_error.record_observability_error();
                                    debug!(error = %e, "observability request failed");
                                }
                            });
                        }
                        Err(e) => {
                            metrics.record_observability_error();
                            warn!(error = %e, "observability accept failed");
                        }
                    }
                }
            }
        }
    });

    Ok((bound_addr, ObservabilityServer { shutdown_tx, task }))
}

async fn handle_connection(
    mut stream: TcpStream,
    metrics: Arc<RuntimeMetrics>,
    store: Arc<NxStore>,
    request_timeout: Duration,
) -> Result<()> {
    metrics.record_observability_request();

    let mut buf = [0u8; 1024];
    let n = timeout(request_timeout, stream.read(&mut buf))
        .await
        .map_err(|_| anyhow!("observability request timed out"))??;
    let request =
        std::str::from_utf8(&buf[..n]).map_err(|e| anyhow!("invalid HTTP request: {e}"))?;
    let path = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");

    let (status, content_type, body) = match path {
        "/health" => ("200 OK", "text/plain; charset=utf-8", "ok\n".to_string()),
        "/ready" if metrics.is_ready() => {
            ("200 OK", "text/plain; charset=utf-8", "ready\n".to_string())
        }
        "/ready" => (
            "503 Service Unavailable",
            "text/plain; charset=utf-8",
            "not ready\n".to_string(),
        ),
        "/metrics" => (
            "200 OK",
            "text/plain; version=0.0.4; charset=utf-8",
            render_metrics(metrics.snapshot(&store)),
        ),
        _ => (
            "404 Not Found",
            "text/plain; charset=utf-8",
            "not found\n".to_string(),
        ),
    };

    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    timeout(request_timeout, stream.write_all(response.as_bytes()))
        .await
        .map_err(|_| anyhow!("observability response write timed out"))??;
    timeout(request_timeout, stream.flush())
        .await
        .map_err(|_| anyhow!("observability response flush timed out"))??;
    Ok(())
}

fn render_metrics(snapshot: MetricsSnapshot) -> String {
    let mut rendered = format!(
        "# HELP numax_ops_total Operations processed\n\
         # TYPE numax_ops_total counter\n\
         numax_ops_total {}\n\
         # HELP numax_peers_connected Active peers\n\
         # TYPE numax_peers_connected gauge\n\
         numax_peers_connected {}\n\
         # HELP numax_sync_latency_ms Last sync latency in milliseconds\n\
         # TYPE numax_sync_latency_ms gauge\n\
         numax_sync_latency_ms {}\n\
         # HELP numax_sync_errors_total Sync errors\n\
         # TYPE numax_sync_errors_total counter\n\
         numax_sync_errors_total {}\n\
         # HELP numax_observability_requests_total Observability requests\n\
         # TYPE numax_observability_requests_total counter\n\
         numax_observability_requests_total {}\n\
         # HELP numax_observability_errors_total Observability request errors\n\
         # TYPE numax_observability_errors_total counter\n\
         numax_observability_errors_total {}\n\
         # HELP numax_peer_connects_total Peer connections observed\n\
         # TYPE numax_peer_connects_total counter\n\
         numax_peer_connects_total {}\n\
         # HELP numax_peer_disconnects_total Peer disconnections observed\n\
         # TYPE numax_peer_disconnects_total counter\n\
         numax_peer_disconnects_total {}\n\
         # HELP numax_broadcast_batches_total Broadcast batches sent\n\
         # TYPE numax_broadcast_batches_total counter\n\
         numax_broadcast_batches_total {}\n\
         # HELP numax_broadcast_ops_total Broadcast ops sent\n\
         # TYPE numax_broadcast_ops_total counter\n\
         numax_broadcast_ops_total {}\n\
         # HELP numax_remote_ops_received_total Remote operations received before deduplication\n\
         # TYPE numax_remote_ops_received_total counter\n\
         numax_remote_ops_received_total {}\n\
         # HELP numax_remote_ops_applied_total Remote operations successfully applied\n\
         # TYPE numax_remote_ops_applied_total counter\n\
         numax_remote_ops_applied_total {}\n\
         # HELP numax_remote_ops_duplicate_total Duplicate remote operations skipped\n\
         # TYPE numax_remote_ops_duplicate_total counter\n\
         numax_remote_ops_duplicate_total {}\n\
         # HELP numax_remote_op_batches_total Non-empty remote operation batches processed\n\
         # TYPE numax_remote_op_batches_total counter\n\
         numax_remote_op_batches_total {}\n\
         # HELP numax_remote_op_apply_errors_total Remote operation batches that failed during apply or persistence\n\
         # TYPE numax_remote_op_apply_errors_total counter\n\
         numax_remote_op_apply_errors_total {}\n\
         # HELP numax_store_keys Keys in the local store\n\
         # TYPE numax_store_keys gauge\n\
         numax_store_keys {}\n\
         # HELP numax_store_bytes Bytes used by local store keys and values\n\
         # TYPE numax_store_bytes gauge\n\
         numax_store_bytes {}\n",
        snapshot.ops_total,
        snapshot.peers_connected,
        snapshot.sync_latency_ms,
        snapshot.sync_errors_total,
        snapshot.observability_requests_total,
        snapshot.observability_errors_total,
        snapshot.peer_connects_total,
        snapshot.peer_disconnects_total,
        snapshot.broadcast_batches_total,
        snapshot.broadcast_ops_total,
        snapshot.remote_ops_received_total,
        snapshot.remote_ops_applied_total,
        snapshot.remote_ops_duplicate_total,
        snapshot.remote_op_batches_total,
        snapshot.remote_op_apply_errors_total,
        snapshot.store_keys,
        snapshot.store_bytes
    );

    render_duration_histogram(
        &mut rendered,
        "numax_remote_op_batch_apply_duration_seconds",
        "Time to deduplicate, apply, persist, and commit a remote operation batch",
        &snapshot.remote_op_batch_apply_duration,
    );

    rendered.push_str(
        "# HELP numax_wasm_invocations_total WASM module invocations by outcome\n\
         # TYPE numax_wasm_invocations_total counter\n\
         # HELP numax_wasm_module_cache_lookups_total WASM module cache lookups by result\n\
         # TYPE numax_wasm_module_cache_lookups_total counter\n\
         # HELP numax_wasm_compilation_duration_seconds_total Time spent compiling WASM modules\n\
         # TYPE numax_wasm_compilation_duration_seconds_total counter\n\
         # HELP numax_wasm_instantiation_duration_seconds_total Time spent instantiating WASM modules\n\
         # TYPE numax_wasm_instantiation_duration_seconds_total counter\n\
         # HELP numax_wasm_instantiations_total WASM module instantiations\n\
         # TYPE numax_wasm_instantiations_total counter\n\
         # HELP numax_wasm_execution_duration_seconds_total Time spent executing WASM entrypoints\n\
         # TYPE numax_wasm_execution_duration_seconds_total counter\n\
         # HELP numax_wasm_executions_total WASM entrypoint executions\n\
         # TYPE numax_wasm_executions_total counter\n\
         # HELP numax_wasm_linear_memory_current_bytes Linear memory after the latest WASM invocation\n\
         # TYPE numax_wasm_linear_memory_current_bytes gauge\n\
         # HELP numax_wasm_linear_memory_peak_bytes Highest observed WASM linear-memory size\n\
         # TYPE numax_wasm_linear_memory_peak_bytes gauge\n\
         # HELP numax_wasm_linear_memory_growth_bytes_total Cumulative WASM linear-memory growth\n\
         # TYPE numax_wasm_linear_memory_growth_bytes_total counter\n",
    );

    for (module, metrics) in snapshot.wasm_modules {
        rendered.push_str(&format!(
            "numax_wasm_invocations_total{{module=\"{module}\",status=\"ok\"}} {}\n\
             numax_wasm_invocations_total{{module=\"{module}\",status=\"error\"}} {}\n\
             numax_wasm_module_cache_lookups_total{{module=\"{module}\",result=\"hit\"}} {}\n\
             numax_wasm_module_cache_lookups_total{{module=\"{module}\",result=\"miss\"}} {}\n\
             numax_wasm_compilation_duration_seconds_total{{module=\"{module}\"}} {:.9}\n\
             numax_wasm_instantiation_duration_seconds_total{{module=\"{module}\"}} {:.9}\n\
             numax_wasm_instantiations_total{{module=\"{module}\"}} {}\n\
             numax_wasm_execution_duration_seconds_total{{module=\"{module}\"}} {:.9}\n\
             numax_wasm_executions_total{{module=\"{module}\"}} {}\n\
             numax_wasm_linear_memory_current_bytes{{module=\"{module}\"}} {}\n\
             numax_wasm_linear_memory_peak_bytes{{module=\"{module}\"}} {}\n\
             numax_wasm_linear_memory_growth_bytes_total{{module=\"{module}\"}} {}\n",
            metrics.invocations_ok,
            metrics.invocations_error,
            metrics.cache_hits,
            metrics.cache_misses,
            seconds(metrics.compilation_duration_ns),
            seconds(metrics.instantiation_duration_ns),
            metrics.instantiations,
            seconds(metrics.execution_duration_ns),
            metrics.executions,
            metrics.linear_memory_current_bytes,
            metrics.linear_memory_peak_bytes,
            metrics.linear_memory_growth_bytes,
        ));
    }

    rendered
}

fn seconds(nanoseconds: u64) -> f64 {
    nanoseconds as f64 / 1_000_000_000.0
}

fn render_duration_histogram(
    rendered: &mut String,
    name: &str,
    help: &str,
    histogram: &DurationHistogramSnapshot,
) {
    rendered.push_str(&format!("# HELP {name} {help}\n# TYPE {name} histogram\n"));
    let mut cumulative = 0u64;
    for ((upper_bound, _), count) in REMOTE_OP_APPLY_BUCKETS.iter().zip(&histogram.buckets) {
        cumulative = cumulative.saturating_add(*count);
        rendered.push_str(&format!(
            "{name}_bucket{{le=\"{upper_bound}\"}} {cumulative}\n"
        ));
    }
    rendered.push_str(&format!(
        "{name}_bucket{{le=\"+Inf\"}} {}\n{name}_sum {:.9}\n{name}_count {}\n",
        histogram.count,
        seconds(histogram.sum_ns),
        histogram.count,
    ));
}

#[cfg(test)]
mod tests {
    use super::*;

    static TEST_STORE_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_store() -> Arc<NxStore> {
        let mut path = std::env::temp_dir();
        let id = TEST_STORE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let process_id = std::process::id();
        path.push(format!("numax-observability-test-{process_id}-{id}"));
        Arc::new(NxStore::open(path).unwrap())
    }

    #[test]
    fn metrics_are_rendered_in_prometheus_format() {
        let snapshot = MetricsSnapshot {
            ops_total: 7,
            peers_connected: 2,
            sync_latency_ms: 15,
            sync_errors_total: 1,
            observability_requests_total: 9,
            observability_errors_total: 1,
            peer_connects_total: 3,
            peer_disconnects_total: 2,
            broadcast_batches_total: 4,
            broadcast_ops_total: 8,
            remote_ops_received_total: 6,
            remote_ops_applied_total: 4,
            remote_ops_duplicate_total: 2,
            remote_op_batches_total: 3,
            remote_op_apply_errors_total: 1,
            remote_op_batch_apply_duration: DurationHistogramSnapshot::default(),
            store_keys: 3,
            store_bytes: 42,
            wasm_modules: BTreeMap::new(),
        };
        let rendered = render_metrics(snapshot);

        assert!(rendered.contains("numax_ops_total 7"));
        assert!(rendered.contains("numax_peers_connected 2"));
        assert!(rendered.contains("numax_sync_latency_ms 15"));
        assert!(rendered.contains("numax_sync_errors_total 1"));
        assert!(rendered.contains("numax_observability_requests_total 9"));
        assert!(rendered.contains("numax_observability_errors_total 1"));
        assert!(rendered.contains("numax_peer_connects_total 3"));
        assert!(rendered.contains("numax_peer_disconnects_total 2"));
        assert!(rendered.contains("numax_broadcast_batches_total 4"));
        assert!(rendered.contains("numax_broadcast_ops_total 8"));
        assert!(rendered.contains("numax_remote_ops_received_total 6"));
        assert!(rendered.contains("numax_remote_ops_applied_total 4"));
        assert!(rendered.contains("numax_remote_ops_duplicate_total 2"));
        assert!(rendered.contains("numax_remote_op_batches_total 3"));
        assert!(rendered.contains("numax_remote_op_apply_errors_total 1"));
        assert!(rendered.contains("numax_store_keys 3"));
        assert!(rendered.contains("numax_store_bytes 42"));
    }

    #[test]
    fn remote_op_batch_histogram_renders_cumulative_buckets_sum_and_count() {
        let metrics = RuntimeMetrics::default();
        let store = temp_store();

        metrics.record_remote_op_batch(4, 3, 1, Duration::from_millis(3), false);
        metrics.record_remote_op_batch(2, 0, 0, Duration::from_secs(2), true);

        let rendered = metrics.render_for_test(&store);
        assert!(
            rendered
                .contains("numax_remote_op_batch_apply_duration_seconds_bucket{le=\"0.0025\"} 0")
        );
        assert!(
            rendered
                .contains("numax_remote_op_batch_apply_duration_seconds_bucket{le=\"0.005\"} 1")
        );
        assert!(
            rendered.contains("numax_remote_op_batch_apply_duration_seconds_bucket{le=\"2.5\"} 2")
        );
        assert!(
            rendered.contains("numax_remote_op_batch_apply_duration_seconds_bucket{le=\"+Inf\"} 2")
        );
        assert!(rendered.contains("numax_remote_op_batch_apply_duration_seconds_sum 2.003000000"));
        assert!(rendered.contains("numax_remote_op_batch_apply_duration_seconds_count 2"));
    }

    #[test]
    fn wasm_module_metrics_are_rendered_with_a_stable_module_label() {
        let metrics = RuntimeMetrics::default();
        let store = temp_store();
        let module = "0123456789abcdef";

        metrics.record_wasm_cache_lookup(module, false);
        metrics.record_wasm_cache_lookup(module, true);
        metrics.record_wasm_compilation(module, Duration::from_millis(4));
        metrics.record_wasm_instantiation(module, Duration::from_millis(2));
        metrics.record_wasm_execution(module, Duration::from_millis(3), 65_536, 131_072);
        metrics.record_wasm_invocation(module, true);
        metrics.record_wasm_invocation(module, false);

        let rendered = metrics.render_for_test(&store);

        assert!(rendered.contains(&format!(
            "numax_wasm_invocations_total{{module=\"{module}\",status=\"ok\"}} 1"
        )));
        assert!(rendered.contains(&format!(
            "numax_wasm_invocations_total{{module=\"{module}\",status=\"error\"}} 1"
        )));
        assert!(rendered.contains(&format!(
            "numax_wasm_module_cache_lookups_total{{module=\"{module}\",result=\"hit\"}} 1"
        )));
        assert!(rendered.contains(&format!(
            "numax_wasm_execution_duration_seconds_total{{module=\"{module}\"}} 0.003000000"
        )));
        assert!(rendered.contains(&format!(
            "numax_wasm_linear_memory_current_bytes{{module=\"{module}\"}} 131072"
        )));
        assert!(rendered.contains(&format!(
            "numax_wasm_linear_memory_growth_bytes_total{{module=\"{module}\"}} 65536"
        )));
    }

    #[test]
    fn wasm_module_metrics_bound_label_cardinality() {
        let metrics = RuntimeMetrics::default();
        let store = temp_store();

        for module in 0..MAX_WASM_MODULE_METRIC_LABELS + 10 {
            metrics.record_wasm_invocation(&format!("module-{module}"), true);
        }

        let snapshot = metrics.snapshot(&store);
        assert_eq!(snapshot.wasm_modules.len(), MAX_WASM_MODULE_METRIC_LABELS);
        assert_eq!(
            snapshot
                .wasm_modules
                .get(WASM_MODULE_OVERFLOW_LABEL)
                .unwrap()
                .invocations_ok,
            11
        );
    }

    #[tokio::test]
    async fn observability_server_serves_health_ready_and_metrics() {
        let metrics = Arc::new(RuntimeMetrics::default());
        metrics.set_ready(true);
        metrics.record_ops(2);
        let store = temp_store();
        store.set(b"a", b"b").unwrap();
        let (addr, server) = start_server(
            ObservabilityConfig::new("127.0.0.1:0"),
            Arc::clone(&metrics),
            Arc::clone(&store),
        )
        .await
        .unwrap();

        assert!(request(addr, "/health").await.contains("ok"));
        assert!(request(addr, "/ready").await.contains("ready"));

        let metrics_response = request(addr, "/metrics").await;
        assert!(metrics_response.contains("HTTP/1.1 200 OK"));
        assert!(metrics_response.contains("text/plain; version=0.0.4"));
        assert!(metrics_response.contains("numax_ops_total 2"));
        assert!(metrics_response.contains("numax_observability_requests_total 3"));

        server.shutdown().await;
    }

    #[tokio::test]
    async fn ready_returns_503_before_runtime_is_ready() {
        let metrics = Arc::new(RuntimeMetrics::default());
        let store = temp_store();
        let (addr, server) = start_server(
            ObservabilityConfig::new("127.0.0.1:0"),
            Arc::clone(&metrics),
            Arc::clone(&store),
        )
        .await
        .unwrap();

        let response = request(addr, "/ready").await;

        assert!(response.contains("HTTP/1.1 503 Service Unavailable"));
        assert!(response.contains("not ready"));

        server.shutdown().await;
    }

    #[tokio::test]
    async fn unknown_path_returns_404() {
        let metrics = Arc::new(RuntimeMetrics::default());
        let store = temp_store();
        let (addr, server) = start_server(
            ObservabilityConfig::new("127.0.0.1:0"),
            Arc::clone(&metrics),
            Arc::clone(&store),
        )
        .await
        .unwrap();

        let response = request(addr, "/missing").await;

        assert!(response.contains("HTTP/1.1 404 Not Found"));
        assert!(response.contains("not found"));

        server.shutdown().await;
    }

    #[tokio::test]
    async fn idle_observability_connection_times_out() {
        let metrics = Arc::new(RuntimeMetrics::default());
        let store = temp_store();
        let config =
            ObservabilityConfig::new("127.0.0.1:0").with_request_timeout(Duration::from_millis(25));
        let (addr, server) = start_server(config, Arc::clone(&metrics), Arc::clone(&store))
            .await
            .unwrap();

        let _stream = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(75)).await;

        let metrics_response = request(addr, "/metrics").await;
        assert!(metrics_response.contains("numax_observability_errors_total 1"));

        server.shutdown().await;
    }

    async fn request(addr: SocketAddr, path: &str) -> String {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let request = format!("GET {path} HTTP/1.1\r\nhost: localhost\r\n\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        String::from_utf8(buf).unwrap()
    }
}
