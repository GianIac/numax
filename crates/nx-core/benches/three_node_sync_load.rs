use std::env;
use std::fs;
#[cfg(all(feature = "cpu-profiling", target_os = "linux"))]
use std::fs::File;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nx_core::SyncConfig;
use nx_core::observability::RuntimeMetrics;
use nx_core::sync_manager::{SyncHandle, SyncManager};
use nx_store::Store;
use nx_sync::{GCounter, NodeId, Op};
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval, sleep, timeout};

#[cfg(all(feature = "heap-profiling", target_os = "linux"))]
#[global_allocator]
static HEAP_PROFILING_ALLOCATOR: dhat::Alloc = dhat::Alloc;

const DEFAULT_DURATION_SECS: u64 = 30;
const DEFAULT_TARGET_OPS_SEC_PER_NODE: u64 = 1_000;
const DEFAULT_SETTLE_SECS: u64 = 10;
const DEFAULT_NODE_COUNT: usize = 3;
const TICK: Duration = Duration::from_millis(100);
const HISTOGRAM_MAX_MICROS: usize = 1_000_000;
const SEND_TIMEOUT: Duration = Duration::from_secs(5);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const LOAD_LIMIT_HEADROOM_OPS: u64 = 100_000;
const LOAD_TEST_MAX_MESSAGE_SIZE: usize = 256 * 1024 * 1024;
const REPORT_SCHEMA_VERSION: u32 = 1;
#[cfg(all(feature = "cpu-profiling", target_os = "linux"))]
const CPU_PROFILE_FREQUENCY_HZ: i32 = 100;
#[cfg(all(feature = "cpu-profiling", target_os = "linux"))]
const CPU_PROFILE_BLOCKLIST: &[&str] = &["libc", "libgcc", "pthread", "vdso"];

#[derive(Debug)]
struct Config {
    nodes: usize,
    duration: Duration,
    target_ops_sec_per_node: u64,
    settle: Duration,
    anti_entropy_interval: Duration,
    report: Option<PathBuf>,
    cpu_profile: Option<PathBuf>,
    heap_profile: Option<PathBuf>,
}

#[cfg(all(feature = "cpu-profiling", target_os = "linux"))]
enum CpuProfiler {
    Disabled,
    Enabled {
        guard: pprof::ProfilerGuard<'static>,
        output: PathBuf,
        file: File,
    },
}

#[cfg(not(all(feature = "cpu-profiling", target_os = "linux")))]
struct CpuProfiler;

#[cfg(all(feature = "heap-profiling", target_os = "linux"))]
enum HeapProfiler {
    Disabled,
    Enabled {
        profiler: dhat::Profiler,
        output: PathBuf,
    },
}

#[cfg(not(all(feature = "heap-profiling", target_os = "linux")))]
struct HeapProfiler;

#[derive(Debug)]
struct Report {
    scenario: &'static str,
    nodes: usize,
    duration_secs: u64,
    load_duration_secs: f64,
    total_duration_secs: f64,
    target_ops_sec_per_node: u64,
    target_ops_sec_total: u64,
    ops_total: u64,
    ops_sec_avg: f64,
    errors_total: u64,
    queued_ops_limit: usize,
    op_log_limit: usize,
    seen_ops_limit: usize,
    anti_entropy_interval_secs: u64,
    expected_counter: u64,
    observed_counters: Vec<u64>,
    converged: bool,
    convergence_wait_secs: f64,
    rss_bytes: Option<u64>,
    latency: LatencySnapshot,
}

#[derive(Debug)]
struct LatencySnapshot {
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
    avg_ms: f64,
}

struct LatencyHistogram {
    buckets: Vec<u64>,
    count: u64,
    total_micros: u128,
    max_micros: u64,
}

struct BenchNode {
    manager: SyncManager,
    handle: SyncHandle,
    _store_dir: tempfile::TempDir,
}

impl LatencyHistogram {
    fn new() -> Self {
        Self {
            buckets: vec![0; HISTOGRAM_MAX_MICROS + 2],
            count: 0,
            total_micros: 0,
            max_micros: 0,
        }
    }

    fn record(&mut self, latency: Duration) {
        let micros = latency.as_micros().min(u64::MAX as u128) as u64;
        let bucket = (micros as usize).min(HISTOGRAM_MAX_MICROS + 1);
        self.buckets[bucket] += 1;
        self.count += 1;
        self.total_micros += micros as u128;
        self.max_micros = self.max_micros.max(micros);
    }

    fn merge(&mut self, other: &Self) {
        for (target, source) in self.buckets.iter_mut().zip(other.buckets.iter()) {
            *target += source;
        }
        self.count += other.count;
        self.total_micros += other.total_micros;
        self.max_micros = self.max_micros.max(other.max_micros);
    }

    fn snapshot(&self) -> LatencySnapshot {
        LatencySnapshot {
            p50_ms: micros_to_ms(self.percentile(50.0)),
            p95_ms: micros_to_ms(self.percentile(95.0)),
            p99_ms: micros_to_ms(self.percentile(99.0)),
            max_ms: micros_to_ms(self.max_micros),
            avg_ms: if self.count == 0 {
                0.0
            } else {
                micros_to_ms((self.total_micros / self.count as u128) as u64)
            },
        }
    }

    fn percentile(&self, percentile: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }

        let target = ((self.count as f64) * percentile / 100.0).ceil() as u64;
        let mut seen = 0u64;
        for (micros, count) in self.buckets.iter().enumerate() {
            seen += count;
            if seen >= target {
                return micros.min(HISTOGRAM_MAX_MICROS) as u64;
            }
        }

        self.max_micros
    }
}

fn main() {
    if let Err(err) = run() {
        eprintln!("three_node_sync_load failed: {err}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let config = Config::parse(env::args().skip(1))?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .enable_io()
        .build()
        .map_err(|e| format!("build tokio runtime: {e}"))?;

    runtime.block_on(run_async(config))
}

async fn run_async(config: Config) -> Result<(), String> {
    let key = "load:counter";
    let addrs = reserve_addrs(config.nodes)?;
    let limits = LoadLimits::for_config(&config);
    let mut nodes = start_full_mesh(&addrs, limits, config.anti_entropy_interval).await?;
    wait_for_full_mesh(&nodes).await?;

    let cpu_profiler = CpuProfiler::start(config.cpu_profile.clone())?;
    let heap_profiler = HeapProfiler::start(config.heap_profile.clone())?;
    let started = Instant::now();
    let mut producers = Vec::new();
    for node in &nodes {
        producers.push(spawn_producer(
            node.handle.clone(),
            key.to_string(),
            config.duration,
            config.target_ops_sec_per_node,
        ));
    }

    let mut ops_total = 0u64;
    let mut errors_total = 0u64;
    let mut latencies = LatencyHistogram::new();
    for producer in producers {
        let result = timeout(config.duration + SEND_TIMEOUT, producer)
            .await
            .map_err(|_| "producer task timed out".to_string())?
            .map_err(|e| format!("producer task failed: {e}"))?;
        ops_total += result.ops_total;
        errors_total += result.errors_total;
        latencies.merge(&result.latencies);
    }

    let load_elapsed = started.elapsed();
    cpu_profiler.finish()?;
    heap_profiler.finish()?;
    let expected_counter = ops_total - errors_total;
    let convergence_started = Instant::now();
    let converged = wait_for_convergence(&nodes, key, expected_counter, config.settle).await;
    let observed_counters = observed_counters(&nodes, key).await;

    let report = Report {
        scenario: "multi-node-sync-gcounter",
        nodes: nodes.len(),
        duration_secs: config.duration.as_secs(),
        load_duration_secs: load_elapsed.as_secs_f64(),
        total_duration_secs: started.elapsed().as_secs_f64(),
        target_ops_sec_per_node: config.target_ops_sec_per_node,
        target_ops_sec_total: config.target_ops_sec_per_node * nodes.len() as u64,
        ops_total,
        ops_sec_avg: ops_total as f64 / load_elapsed.as_secs_f64(),
        errors_total,
        queued_ops_limit: limits.queued_ops_limit,
        op_log_limit: limits.op_log_limit,
        seen_ops_limit: limits.seen_ops_limit,
        anti_entropy_interval_secs: config.anti_entropy_interval.as_secs(),
        expected_counter,
        observed_counters,
        converged,
        convergence_wait_secs: convergence_started.elapsed().as_secs_f64(),
        rss_bytes: current_rss_bytes(),
        latency: latencies.snapshot(),
    };
    let json = report.to_json();

    println!("{json}");
    if let Some(report_path) = &config.report {
        if let Some(parent) = report_path.parent() {
            fs::create_dir_all(parent).map_err(|e| format!("create report dir: {e}"))?;
        }
        fs::write(report_path, format!("{json}\n")).map_err(|e| format!("write report: {e}"))?;
    }

    let mut shutdown_errors = 0u64;
    for node in &mut nodes {
        match timeout(SHUTDOWN_TIMEOUT, node.manager.shutdown()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                shutdown_errors += 1;
                eprintln!("sync manager shutdown failed: {e}");
            }
            Err(_) => {
                shutdown_errors += 1;
                eprintln!("sync manager shutdown timed out after {SHUTDOWN_TIMEOUT:?}");
            }
        }
    }

    if report.errors_total > 0 {
        return Err(format!(
            "benchmark completed with {} errors",
            report.errors_total
        ));
    }
    if shutdown_errors > 0 {
        return Err(format!(
            "benchmark completed but {shutdown_errors} sync managers failed to shut down cleanly"
        ));
    }
    if !report.converged {
        return Err(format!(
            "nodes did not converge to {expected_counter}; observed {:?}",
            report.observed_counters
        ));
    }

    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct LoadLimits {
    queued_ops_limit: usize,
    op_log_limit: usize,
    seen_ops_limit: usize,
    max_message_size: usize,
}

impl LoadLimits {
    fn for_config(config: &Config) -> Self {
        let per_node_expected = config
            .target_ops_sec_per_node
            .saturating_mul(config.duration.as_secs())
            .saturating_add(LOAD_LIMIT_HEADROOM_OPS);
        let total_expected = per_node_expected.saturating_mul(config.nodes as u64);

        Self {
            queued_ops_limit: per_node_expected.min(usize::MAX as u64) as usize,
            op_log_limit: per_node_expected.min(usize::MAX as u64) as usize,
            seen_ops_limit: total_expected.min(usize::MAX as u64) as usize,
            max_message_size: LOAD_TEST_MAX_MESSAGE_SIZE,
        }
    }
}

async fn start_full_mesh(
    addrs: &[String],
    limits: LoadLimits,
    anti_entropy_interval: Duration,
) -> Result<Vec<BenchNode>, String> {
    let mut nodes = Vec::new();
    for (index, listen_addr) in addrs.iter().enumerate() {
        let mut config = SyncConfig::new()
            .with_listen_addr(listen_addr.clone())
            .with_queued_ops_limit(limits.queued_ops_limit)
            .with_op_log_limit(limits.op_log_limit)
            .with_seen_ops_limit(limits.seen_ops_limit)
            .with_max_message_size(limits.max_message_size)
            .with_reconnect_backoff(Duration::from_millis(50), Duration::from_millis(250))
            .with_anti_entropy_interval(anti_entropy_interval);
        for (peer_index, peer_addr) in addrs.iter().enumerate() {
            if peer_index != index {
                config = config.with_peer(peer_addr.clone());
            }
        }

        let store_dir = tempfile::tempdir().map_err(|e| format!("create temp store dir: {e}"))?;
        let store =
            Arc::new(Store::open(store_dir.path()).map_err(|e| format!("open store: {e}"))?);
        let mut manager = SyncManager::new(
            NodeId::generate(),
            config,
            store,
            Arc::new(RuntimeMetrics::default()),
        );
        let handle = manager.handle();
        manager
            .start()
            .await
            .map_err(|e| format!("start sync manager: {e}"))?;
        nodes.push(BenchNode {
            manager,
            handle,
            _store_dir: store_dir,
        });
    }

    Ok(nodes)
}

fn spawn_producer(
    handle: SyncHandle,
    key: String,
    duration: Duration,
    target_ops_sec: u64,
) -> JoinHandle<ProducerResult> {
    tokio::spawn(async move {
        let started = Instant::now();
        let target_ops = target_ops_sec.saturating_mul(duration.as_secs());
        let mut ticker = interval(TICK);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Burst);
        let mut result = ProducerResult {
            ops_total: 0,
            errors_total: 0,
            latencies: LatencyHistogram::new(),
        };

        while result.ops_total + result.errors_total < target_ops {
            ticker.tick().await;
            let elapsed = started.elapsed().min(duration);
            let expected_by_now = target_ops.min(
                (elapsed.as_nanos().saturating_mul(target_ops_sec as u128)
                    / Duration::from_secs(1).as_nanos()) as u64,
            );
            let produced = result.ops_total + result.errors_total;
            let due_ops = expected_by_now.saturating_sub(produced);

            for _ in 0..due_ops {
                let op_started = Instant::now();
                match local_increment(&handle, &key).await {
                    Ok(()) => {
                        result.ops_total += 1;
                        result.latencies.record(op_started.elapsed());
                    }
                    Err(_) => result.errors_total += 1,
                }
            }
        }

        result
    })
}

struct ProducerResult {
    ops_total: u64,
    errors_total: u64,
    latencies: LatencyHistogram,
}

async fn local_increment(handle: &SyncHandle, key: &str) -> Result<(), ()> {
    let op = Op::gcounter_increment(handle.node_id().clone(), key, 1);
    timeout(SEND_TIMEOUT, handle.op_sender().send(op))
        .await
        .map_err(|_| ())?
        .map_err(|_| ())?;

    {
        let counters = handle.counters();
        let mut counters = counters.write().await;
        let mut counter = counters.get(key).cloned().unwrap_or_else(GCounter::new);
        counter.increment(handle.node_id(), 1);
        counters.insert(key.to_string(), counter);
    }

    Ok(())
}

async fn wait_for_full_mesh(nodes: &[BenchNode]) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let expected_peers = nodes.len().saturating_sub(1);
    loop {
        let counts = futures_counts(nodes).await;
        if counts.iter().all(|count| *count >= expected_peers) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("nodes did not form a full mesh; counts={counts:?}"));
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_convergence(
    nodes: &[BenchNode],
    key: &str,
    expected: u64,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        let values = observed_counters(nodes, key).await;
        if values.iter().all(|value| *value == expected) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn observed_counters(nodes: &[BenchNode], key: &str) -> Vec<u64> {
    let mut values = Vec::with_capacity(nodes.len());
    for node in nodes {
        values.push(node.manager.get_counter_value(key).await);
    }
    values
}

async fn futures_counts(nodes: &[BenchNode]) -> Vec<usize> {
    let mut counts = Vec::with_capacity(nodes.len());
    for node in nodes {
        counts.push(node.manager.connected_peer_count().await);
    }
    counts
}

impl Config {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut config = Self {
            nodes: DEFAULT_NODE_COUNT,
            duration: Duration::from_secs(DEFAULT_DURATION_SECS),
            target_ops_sec_per_node: DEFAULT_TARGET_OPS_SEC_PER_NODE,
            settle: Duration::from_secs(DEFAULT_SETTLE_SECS),
            anti_entropy_interval: Duration::ZERO,
            report: None,
            cpu_profile: None,
            heap_profile: None,
        };

        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--nodes" => {
                    config.nodes = parse_next(&mut args, &arg)?;
                }
                "--duration-secs" => {
                    config.duration = Duration::from_secs(parse_next(&mut args, &arg)?);
                }
                "--target-ops-sec-per-node" => {
                    config.target_ops_sec_per_node = parse_next(&mut args, &arg)?;
                }
                "--settle-secs" => {
                    config.settle = Duration::from_secs(parse_next(&mut args, &arg)?);
                }
                "--anti-entropy-secs" => {
                    config.anti_entropy_interval =
                        Duration::from_secs(parse_next(&mut args, &arg)?);
                }
                "--report" => {
                    config.report = Some(PathBuf::from(next_value(&mut args, &arg)?));
                }
                "--cpu-profile" => {
                    config.cpu_profile = Some(PathBuf::from(next_value(&mut args, &arg)?));
                }
                "--heap-profile" => {
                    config.heap_profile = Some(PathBuf::from(next_value(&mut args, &arg)?));
                }
                "--bench" => {}
                "--help" | "-h" => {
                    print_help();
                    process::exit(0);
                }
                other => return Err(format!("unknown argument: {other}")),
            }
        }

        if config.nodes < 2 {
            return Err("--nodes must be at least 2".to_string());
        }
        if config.duration.is_zero() {
            return Err("--duration-secs must be greater than zero".to_string());
        }
        if config.target_ops_sec_per_node == 0 {
            return Err("--target-ops-sec-per-node must be greater than zero".to_string());
        }
        if config.cpu_profile.is_some() && config.heap_profile.is_some() {
            return Err(
                "--cpu-profile and --heap-profile must run in separate processes".to_string(),
            );
        }
        if config.cpu_profile.is_some()
            && !cfg!(all(feature = "cpu-profiling", target_os = "linux"))
        {
            return Err("--cpu-profile requires the cpu-profiling feature on Linux".to_string());
        }
        if config.heap_profile.is_some()
            && !cfg!(all(feature = "heap-profiling", target_os = "linux"))
        {
            return Err("--heap-profile requires the heap-profiling feature on Linux".to_string());
        }
        if config.anti_entropy_interval.is_zero() {
            config.anti_entropy_interval = config
                .duration
                .saturating_add(config.settle)
                .saturating_add(Duration::from_secs(60));
        }
        Ok(config)
    }
}

#[cfg(all(feature = "cpu-profiling", target_os = "linux"))]
impl CpuProfiler {
    fn start(output: Option<PathBuf>) -> Result<Self, String> {
        let Some(output) = output else {
            return Ok(Self::Disabled);
        };
        if let Some(parent) = output
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(|e| format!("create CPU profile directory: {e}"))?;
        }
        let file = File::create(&output)
            .map_err(|e| format!("create CPU profile {}: {e}", output.display()))?;
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(CPU_PROFILE_FREQUENCY_HZ)
            .blocklist(CPU_PROFILE_BLOCKLIST)
            .build()
            .map_err(|e| format!("start CPU profiler: {e}"))?;
        Ok(Self::Enabled {
            guard,
            output,
            file,
        })
    }

    fn finish(self) -> Result<(), String> {
        let Self::Enabled {
            guard,
            output,
            file,
        } = self
        else {
            return Ok(());
        };
        let report = guard
            .report()
            .build()
            .map_err(|e| format!("build CPU profile report: {e}"))?;
        report
            .flamegraph(file)
            .map_err(|e| format!("write CPU flamegraph {}: {e}", output.display()))?;
        println!("CPU flamegraph written to {}", output.display());
        Ok(())
    }
}

#[cfg(all(feature = "heap-profiling", target_os = "linux"))]
impl HeapProfiler {
    fn start(output: Option<PathBuf>) -> Result<Self, String> {
        let Some(output) = output else {
            return Ok(Self::Disabled);
        };
        if let Some(parent) = output
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)
                .map_err(|e| format!("create heap profile directory: {e}"))?;
        }
        let profiler = dhat::Profiler::builder().file_name(&output).build();
        Ok(Self::Enabled { profiler, output })
    }

    fn finish(self) -> Result<(), String> {
        let Self::Enabled { profiler, output } = self else {
            return Ok(());
        };
        let stats = dhat::HeapStats::get();
        drop(profiler);
        let profile_size = fs::metadata(&output)
            .map_err(|e| format!("read heap profile {}: {e}", output.display()))?
            .len();
        if profile_size == 0 {
            return Err(format!("heap profile {} is empty", output.display()));
        }
        println!(
            "Heap profile written to {} (total: {} bytes in {} allocations; peak: {} bytes in {} allocations; retained: {} bytes in {} allocations)",
            output.display(),
            stats.total_bytes,
            stats.total_blocks,
            stats.max_bytes,
            stats.max_blocks,
            stats.curr_bytes,
            stats.curr_blocks
        );
        Ok(())
    }
}

#[cfg(not(all(feature = "heap-profiling", target_os = "linux")))]
impl HeapProfiler {
    fn start(output: Option<PathBuf>) -> Result<Self, String> {
        if output.is_some() {
            return Err("--heap-profile requires the heap-profiling feature on Linux".to_string());
        }
        Ok(Self)
    }

    fn finish(self) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(not(all(feature = "cpu-profiling", target_os = "linux")))]
impl CpuProfiler {
    fn start(output: Option<PathBuf>) -> Result<Self, String> {
        if output.is_some() {
            return Err("--cpu-profile requires the cpu-profiling feature on Linux".to_string());
        }
        Ok(Self)
    }

    fn finish(self) -> Result<(), String> {
        Ok(())
    }
}

impl Report {
    fn to_json(&self) -> String {
        format!(
            concat!(
                "{{\n",
                "  \"report_schema_version\": {},\n",
                "  \"crate\": \"nx-core\",\n",
                "  \"benchmark\": \"three_node_sync_load\",\n",
                "  \"scenario\": \"{}\",\n",
                "  \"profile\": {{\n",
                "    \"nodes\": {},\n",
                "    \"duration_secs\": {},\n",
                "    \"target_ops_sec_per_node\": {},\n",
                "    \"anti_entropy_interval_secs\": {}\n",
                "  }},\n",
                "  \"nodes\": {},\n",
                "  \"load_duration_secs\": {:.3},\n",
                "  \"total_duration_secs\": {:.3},\n",
                "  \"target_ops_sec_per_node\": {},\n",
                "  \"target_ops_sec_total\": {},\n",
                "  \"ops_total\": {},\n",
                "  \"ops_sec_avg\": {:.2},\n",
                "  \"errors_total\": {},\n",
                "  \"queued_ops_limit\": {},\n",
                "  \"op_log_limit\": {},\n",
                "  \"seen_ops_limit\": {},\n",
                "  \"anti_entropy_interval_secs\": {},\n",
                "  \"expected_counter\": {},\n",
                "  \"observed_counters\": [{}],\n",
                "  \"converged\": {},\n",
                "  \"convergence_wait_secs\": {:.3},\n",
                "  \"resources\": {{\n",
                "    \"rss_bytes\": {}\n",
                "  }},\n",
                "  \"latency_ms\": {{\n",
                "    \"p50\": {:.3},\n",
                "    \"p95\": {:.3},\n",
                "    \"p99\": {:.3},\n",
                "    \"max\": {:.3},\n",
                "    \"avg\": {:.3}\n",
                "  }}\n",
                "}}"
            ),
            REPORT_SCHEMA_VERSION,
            self.scenario,
            self.nodes,
            self.duration_secs,
            self.target_ops_sec_per_node,
            self.anti_entropy_interval_secs,
            self.nodes,
            self.load_duration_secs,
            self.total_duration_secs,
            self.target_ops_sec_per_node,
            self.target_ops_sec_total,
            self.ops_total,
            self.ops_sec_avg,
            self.errors_total,
            self.queued_ops_limit,
            self.op_log_limit,
            self.seen_ops_limit,
            self.anti_entropy_interval_secs,
            self.expected_counter,
            join_u64s(&self.observed_counters),
            self.converged,
            self.convergence_wait_secs,
            json_option_u64(self.rss_bytes),
            self.latency.p50_ms,
            self.latency.p95_ms,
            self.latency.p99_ms,
            self.latency.max_ms,
            self.latency.avg_ms
        )
    }
}

fn reserve_addrs(count: usize) -> Result<Vec<String>, String> {
    let mut addrs = Vec::with_capacity(count);
    for _ in 0..count {
        addrs.push(free_addr()?);
    }
    Ok(addrs)
}

fn free_addr() -> Result<String, String> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|e| format!("bind temp addr: {e}"))?;
    let addr = listener
        .local_addr()
        .map_err(|e| format!("read temp addr: {e}"))?;
    Ok(addr.to_string())
}

fn join_u64s(values: &[u64]) -> String {
    values
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

fn parse_next<T: std::str::FromStr>(
    args: &mut impl Iterator<Item = String>,
    name: &str,
) -> Result<T, String> {
    let value = next_value(args, name)?;
    value
        .parse()
        .map_err(|_| format!("invalid value for {name}: {value}"))
}

fn next_value(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("missing value for argument {name}"))
}

fn micros_to_ms(micros: u64) -> f64 {
    micros as f64 / 1_000.0
}

fn json_option_u64(value: Option<u64>) -> String {
    match value {
        Some(value) => value.to_string(),
        None => "null".to_string(),
    }
}

#[cfg(target_os = "linux")]
fn current_rss_bytes() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        let Some(rest) = line.strip_prefix("VmRSS:") else {
            continue;
        };
        let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
        return kb.checked_mul(1024);
    }
    None
}

#[cfg(not(target_os = "linux"))]
fn current_rss_bytes() -> Option<u64> {
    None
}

fn print_help() {
    println!(
        "\
Multi-node continuous sync benchmark

Options:
  --nodes N                         Number of nodes in full mesh (default: {DEFAULT_NODE_COUNT})
  --duration-secs N                 Duration in seconds (default: {DEFAULT_DURATION_SECS})
  --target-ops-sec-per-node N       Target increment ops/sec per node (default: {DEFAULT_TARGET_OPS_SEC_PER_NODE})
  --settle-secs N                   Time allowed for final convergence (default: {DEFAULT_SETTLE_SECS})
  --anti-entropy-secs N             Periodic pull interval; default keeps repair traffic outside the run window
  --report PATH                     Write JSON report to PATH
  --cpu-profile PATH                Write a load-phase CPU flamegraph (Linux, requires feature cpu-profiling)
  --heap-profile PATH               Write a load-phase DHAT heap profile (Linux, requires feature heap-profiling)

Smoke example:
  cargo bench -p nx-core --bench three_node_sync_load -- --duration-secs 10 --target-ops-sec-per-node 1000

CPU profiling example (Linux):
  cargo bench --profile profiling -p nx-core --features cpu-profiling --bench three_node_sync_load -- --duration-secs 10 --target-ops-sec-per-node 1000 --cpu-profile reports/profiling/three-node-sync-load.svg

Heap profiling example (Linux):
  cargo bench --profile profiling -p nx-core --features heap-profiling --bench three_node_sync_load -- --duration-secs 5 --target-ops-sec-per-node 250 --heap-profile reports/profiling/three-node-sync-load-heap.json
"
    );
}
