use std::env;
#[cfg(all(feature = "cpu-profiling", target_os = "linux"))]
use std::fmt::Write as _;
use std::fs;
#[cfg(all(feature = "cpu-profiling", target_os = "linux"))]
use std::fs::File;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nx_core::SyncConfig;
use nx_core::observability::RuntimeMetrics;
use nx_core::sync_manager::{SyncHandle, SyncManager};
use nx_store::Store;
use nx_sync::NodeId;
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
const REPORT_SCHEMA_VERSION: u32 = 2;
const ACCEPTANCE_PROFILE: &str = "local-durable-flush-v1";
const MAX_ERROR_DETAILS: usize = 16;
const MAX_ERROR_DETAIL_CHARS: usize = 1024;
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
    attempted_ops_total: u64,
    uncertain_ops_total: u64,
    target_ops_total: u64,
    cleanup_errors_total: u64,
    report_complete: bool,
    error_details: Vec<String>,
    queued_ops_limit: usize,
    op_log_limit: usize,
    seen_ops_limit: usize,
    anti_entropy_interval_secs: u64,
    observed_counters: Vec<u64>,
    converged: bool,
    convergence_wait_secs: f64,
    rss_bytes: Option<u64>,
    latency: LatencySnapshot,
    latencies: LatencyHistogram,
}

#[derive(Debug)]
struct LatencySnapshot {
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
    avg_ms: f64,
}

#[derive(Debug)]
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
    let limits = LoadLimits::for_config(&config);
    let mut report = Report::new(&config, limits);
    let mut nodes = Vec::new();
    let result = run_load(&config, limits, &mut nodes, &mut report).await;
    finish_run(&config, report, &mut nodes, result).await
}

async fn run_load(
    config: &Config,
    limits: LoadLimits,
    nodes: &mut Vec<BenchNode>,
    report: &mut Report,
) -> Result<(), String> {
    let key = "load:counter";
    let addrs = reserve_addrs(config.nodes)?;
    start_full_mesh(nodes, &addrs, limits, config.anti_entropy_interval).await?;
    wait_for_full_mesh(nodes).await?;
    let cpu_profiler = CpuProfiler::start(config.cpu_profile.clone())?;
    let heap_profiler = HeapProfiler::start(config.heap_profile.clone())?;
    let started = Instant::now();
    let mut producers = Vec::new();
    for node in nodes.iter() {
        producers.push(spawn_producer(
            node.handle.clone(),
            key.to_string(),
            config.duration,
            config.target_ops_sec_per_node,
        ));
    }

    for producer in producers {
        let result = collect_producer(producer, config.duration + SEND_TIMEOUT).await;
        report.add_producer(&result);
    }

    let load_elapsed = started.elapsed();
    report.load_duration_secs = load_elapsed.as_secs_f64();
    report.ops_sec_avg = report.ops_total as f64 / load_elapsed.as_secs_f64();
    for result in [cpu_profiler.finish(), heap_profiler.finish()] {
        if let Err(error) = result {
            push_error(&mut report.error_details, error);
        }
    }
    let convergence_started = Instant::now();
    if let Some(expected) = report.exact_expected_counter() {
        report.converged = wait_for_convergence(nodes, key, expected, config.settle).await;
    }
    match timeout(SEND_TIMEOUT, observed_counters(nodes, key)).await {
        Ok(values) => report.observed_counters = values,
        Err(_) => push_error(
            &mut report.error_details,
            "counter observation timed out".into(),
        ),
    }
    report.convergence_wait_secs = convergence_started.elapsed().as_secs_f64();
    report.total_duration_secs = started.elapsed().as_secs_f64();
    report.rss_bytes = current_rss_bytes();
    Ok(())
}

async fn finish_run(
    config: &Config,
    mut report: Report,
    nodes: &mut [BenchNode],
    result: Result<(), String>,
) -> Result<(), String> {
    let cleanup_started = Instant::now();
    let cleanup_errors = cleanup_nodes(nodes).await;
    report.finish(result, cleanup_errors, cleanup_started.elapsed());
    let json = report.to_json();
    println!("{json}");
    if let Some(report_path) = &config.report {
        if let Some(parent) = report_path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent).map_err(|e| format!("create report dir: {e}"))?;
        }
        fs::write(report_path, format!("{json}\n")).map_err(|e| format!("write report: {e}"))?;
    }

    if !report.report_complete {
        return Err(format!(
            "incomplete benchmark: target={}, attempted={}, acknowledged={}, failed_or_uncertain={}, cleanup_errors={}, converged={}; {}",
            report.target_ops_total,
            report.attempted_ops_total,
            report.ops_total,
            report.errors_total,
            report.cleanup_errors_total,
            report.converged,
            report.error_details.join("; ")
        ));
    }
    Ok(())
}

async fn cleanup_nodes(nodes: &mut [BenchNode]) -> Vec<String> {
    let mut errors = Vec::new();
    for (index, node) in nodes.iter_mut().enumerate() {
        match timeout(SHUTDOWN_TIMEOUT, node.manager.shutdown()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                errors.push(format!("node {index} sync manager shutdown failed: {e}"));
            }
            Err(_) => {
                errors.push(format!(
                    "node {index} sync manager shutdown timed out after {SHUTDOWN_TIMEOUT:?}"
                ));
            }
        }
    }
    errors
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
    nodes: &mut Vec<BenchNode>,
    addrs: &[String],
    limits: LoadLimits,
    anti_entropy_interval: Duration,
) -> Result<(), String> {
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

        let store_dir = tempfile::Builder::new()
            .prefix("benchmark-store-")
            .tempdir_in(".")
            .map_err(|e| format!("create benchmark store dir: {e}"))?;
        let store =
            Arc::new(Store::open(store_dir.path()).map_err(|e| format!("open store: {e}"))?);
        let manager = SyncManager::new(
            NodeId::generate(),
            config,
            store,
            Arc::new(RuntimeMetrics::default()),
        );
        let handle = manager.handle();
        nodes.push(BenchNode {
            manager,
            handle,
            _store_dir: store_dir,
        });
        nodes[index]
            .manager
            .start()
            .await
            .map_err(|e| format!("start sync manager {index}: {e}"))?;
    }

    Ok(())
}

fn spawn_producer(
    handle: SyncHandle,
    key: String,
    duration: Duration,
    target_ops_sec: u64,
) -> Producer {
    let progress = Arc::new(Mutex::new(ProducerResult::new()));
    let state = progress.clone();
    let task = tokio::spawn(async move {
        let started = Instant::now();
        let target_ops = target_ops_sec.saturating_mul(duration.as_secs());
        let mut ticker = interval(TICK);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Burst);
        while state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .attempted_ops_total
            < target_ops
        {
            ticker.tick().await;
            let elapsed = started.elapsed().min(duration);
            let expected_by_now = target_ops.min(
                (elapsed.as_nanos().saturating_mul(target_ops_sec as u128)
                    / Duration::from_secs(1).as_nanos()) as u64,
            );
            let produced = state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .attempted_ops_total;
            let due_ops = expected_by_now.saturating_sub(produced);

            for _ in 0..due_ops {
                let op_started = Instant::now();
                state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .begin_attempt();
                let result = local_increment(&handle, &key).await;
                state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .finish_attempt(result, op_started.elapsed());
            }
        }
    });
    Producer { task, progress }
}

struct Producer {
    task: JoinHandle<()>,
    progress: Arc<Mutex<ProducerResult>>,
}

struct ProducerResult {
    attempted_ops_total: u64,
    ops_total: u64,
    errors_total: u64,
    uncertain_ops_total: u64,
    in_flight: bool,
    error_details: Vec<String>,
    latencies: LatencyHistogram,
}

impl ProducerResult {
    fn new() -> Self {
        Self {
            attempted_ops_total: 0,
            ops_total: 0,
            errors_total: 0,
            uncertain_ops_total: 0,
            in_flight: false,
            error_details: Vec::new(),
            latencies: LatencyHistogram::new(),
        }
    }

    fn begin_attempt(&mut self) {
        self.attempted_ops_total += 1;
        self.in_flight = true;
    }

    fn finish_attempt(&mut self, result: Result<(), String>, elapsed: Duration) {
        self.in_flight = false;
        match result {
            Ok(()) => {
                self.ops_total += 1;
                self.latencies.record(elapsed);
            }
            Err(error) => {
                self.errors_total += 1;
                // A failed acknowledgement does not prove that the transaction rolled back.
                self.uncertain_ops_total += 1;
                push_error(&mut self.error_details, error);
            }
        }
    }
}

async fn collect_producer(mut producer: Producer, deadline: Duration) -> ProducerResult {
    let error = match timeout(deadline, &mut producer.task).await {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(format!("producer task failed: {error}")),
        Err(_) => {
            producer.task.abort();
            let joined = producer.task.await;
            Some(format!(
                "producer task timed out; aborted and joined: {joined:?}"
            ))
        }
    };
    let mut progress = producer.progress.lock().unwrap_or_else(|e| e.into_inner());
    if progress.in_flight {
        progress.in_flight = false;
        progress.errors_total += 1;
        progress.uncertain_ops_total += 1;
    }
    if let Some(error) = error {
        push_error(&mut progress.error_details, error);
    }
    std::mem::replace(&mut *progress, ProducerResult::new())
}

fn push_error(details: &mut Vec<String>, error: String) {
    if details.len() < MAX_ERROR_DETAILS {
        details.push(error.chars().take(MAX_ERROR_DETAIL_CHARS).collect());
    }
}

async fn local_increment(handle: &SyncHandle, key: &str) -> Result<(), String> {
    timeout(SEND_TIMEOUT, handle.submit_gcounter_increment(key, 1))
        .await
        .map_err(|_| format!("increment timed out after {SEND_TIMEOUT:?}; outcome unknown"))?
        .map_err(|error| format!("increment failed; outcome uncertain: {error:#}"))
}

async fn wait_for_full_mesh(nodes: &[BenchNode]) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let expected_peers = nodes.len().saturating_sub(1);
    loop {
        let counts = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            futures_counts(nodes),
        )
        .await
        .map_err(|_| "full mesh observation timed out".to_string())?;
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
        let Ok(values) = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            observed_counters(nodes, key),
        )
        .await
        else {
            return false;
        };
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
        write_cpu_flamegraph(&report, file)
            .map_err(|e| format!("write CPU flamegraph {}: {e}", output.display()))?;
        println!("CPU flamegraph written to {}", output.display());
        Ok(())
    }
}

#[cfg(all(feature = "cpu-profiling", target_os = "linux"))]
fn write_cpu_flamegraph(report: &pprof::Report, file: File) -> std::io::Result<()> {
    let lines: Vec<String> = report
        .data
        .iter()
        .map(|(frames, samples)| {
            let mut line = frames.thread_name_or_id();
            line.push(';');
            for frame in frames.frames.iter().rev() {
                for symbol in frame.iter().rev() {
                    let _ = write!(&mut line, "{symbol};");
                }
            }
            line.pop();
            let _ = write!(&mut line, " {samples}");
            line
        })
        .collect();

    inferno::flamegraph::from_lines(
        &mut inferno::flamegraph::Options::default(),
        lines.iter().map(String::as_str),
        file,
    )
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
    fn new(config: &Config, limits: LoadLimits) -> Self {
        let latencies = LatencyHistogram::new();
        Self {
            scenario: "multi-node-sync-gcounter",
            nodes: config.nodes,
            duration_secs: config.duration.as_secs(),
            load_duration_secs: 0.0,
            total_duration_secs: 0.0,
            target_ops_sec_per_node: config.target_ops_sec_per_node,
            target_ops_sec_total: config
                .target_ops_sec_per_node
                .saturating_mul(config.nodes as u64),
            target_ops_total: config
                .target_ops_sec_per_node
                .saturating_mul(config.duration.as_secs())
                .saturating_mul(config.nodes as u64),
            ops_total: 0,
            ops_sec_avg: 0.0,
            errors_total: 0,
            attempted_ops_total: 0,
            uncertain_ops_total: 0,
            cleanup_errors_total: 0,
            report_complete: false,
            error_details: Vec::new(),
            queued_ops_limit: limits.queued_ops_limit,
            op_log_limit: limits.op_log_limit,
            seen_ops_limit: limits.seen_ops_limit,
            anti_entropy_interval_secs: config.anti_entropy_interval.as_secs(),
            observed_counters: Vec::new(),
            converged: false,
            convergence_wait_secs: 0.0,
            rss_bytes: current_rss_bytes(),
            latency: latencies.snapshot(),
            latencies,
        }
    }

    fn add_producer(&mut self, result: &ProducerResult) {
        self.ops_total += result.ops_total;
        self.attempted_ops_total += result.attempted_ops_total;
        self.errors_total += result.errors_total;
        self.uncertain_ops_total += result.uncertain_ops_total;
        self.latencies.merge(&result.latencies);
        self.latency = self.latencies.snapshot();
        for error in &result.error_details {
            push_error(&mut self.error_details, error.clone());
        }
    }

    fn exact_expected_counter(&self) -> Option<u64> {
        (self.uncertain_ops_total == 0).then_some(self.ops_total)
    }

    fn finish(
        &mut self,
        result: Result<(), String>,
        cleanup_errors: Vec<String>,
        cleanup_duration: Duration,
    ) {
        self.total_duration_secs += cleanup_duration.as_secs_f64();
        if let Err(error) = result {
            push_error(&mut self.error_details, error);
        }
        self.cleanup_errors_total = cleanup_errors.len() as u64;
        for error in cleanup_errors {
            push_error(&mut self.error_details, error);
        }
        if self.uncertain_ops_total > self.errors_total
            || self.ops_total.checked_add(self.errors_total) != Some(self.attempted_ops_total)
        {
            push_error(
                &mut self.error_details,
                "inconsistent operation outcome accounting".into(),
            );
        }
        if self.attempted_ops_total != self.target_ops_total {
            push_error(
                &mut self.error_details,
                format!(
                    "missed workload: target={}, attempted={}",
                    self.target_ops_total, self.attempted_ops_total
                ),
            );
        }
        if !self.converged || self.exact_expected_counter().is_none() {
            push_error(
                &mut self.error_details,
                "exact convergence was not established".into(),
            );
        }
        self.report_complete = self.error_details.is_empty()
            && self.attempted_ops_total == self.target_ops_total
            && self.ops_total == self.target_ops_total
            && self.errors_total == 0
            && self.uncertain_ops_total == 0
            && self.cleanup_errors_total == 0
            && self.converged;
    }

    fn to_json(&self) -> String {
        serde_json::json!({
            "report_schema_version": REPORT_SCHEMA_VERSION,
            "crate": "nx-core",
            "benchmark": "three_node_sync_load",
            "scenario": self.scenario,
            "profile": {
                "acceptance": ACCEPTANCE_PROFILE,
                "nodes": self.nodes,
                "duration_secs": self.duration_secs,
                "target_ops_sec_per_node": self.target_ops_sec_per_node,
                "anti_entropy_interval_secs": self.anti_entropy_interval_secs,
            },
            "nodes": self.nodes,
            "load_duration_secs": self.load_duration_secs,
            "total_duration_secs": self.total_duration_secs,
            "target_ops_sec_per_node": self.target_ops_sec_per_node,
            "target_ops_sec_total": self.target_ops_sec_total,
            "target_ops_total": self.target_ops_total,
            "attempted_ops_total": self.attempted_ops_total,
            "ops_total": self.ops_total,
            "ops_sec_avg": self.ops_sec_avg,
            "errors_total": self.errors_total,
            "definite_errors_total": self.errors_total.saturating_sub(self.uncertain_ops_total),
            "uncertain_ops_total": self.uncertain_ops_total,
            "unattempted_ops_total": self.target_ops_total.saturating_sub(self.attempted_ops_total),
            "cleanup_errors_total": self.cleanup_errors_total,
            "report_complete": self.report_complete,
            "error_details": self.error_details,
            "queued_ops_limit": self.queued_ops_limit,
            "op_log_limit": self.op_log_limit,
            "seen_ops_limit": self.seen_ops_limit,
            "anti_entropy_interval_secs": self.anti_entropy_interval_secs,
            "expected_counter": self.exact_expected_counter(),
            "observed_counters": self.observed_counters,
            "converged": self.converged && self.exact_expected_counter().is_some(),
            "convergence_wait_secs": self.convergence_wait_secs,
            "resources": { "rss_bytes": self.rss_bytes },
            "latency_ms": {
                "p50": self.latency.p50_ms,
                "p95": self.latency.p95_ms,
                "p99": self.latency.p99_ms,
                "max": self.latency.max_ms,
                "avg": self.latency.avg_ms,
            },
        })
        .to_string()
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

#[cfg(test)]
// The harness-free bench compiles these helpers; the integration harness runs the tests.
#[allow(dead_code)]
mod tests {
    use super::*;

    fn config() -> Config {
        Config::parse(
            ["--duration-secs", "1", "--target-ops-sec-per-node", "1"]
                .into_iter()
                .map(str::to_string),
        )
        .unwrap()
    }

    #[test]
    fn successes_are_not_reduced_by_failed_attempts() {
        let mut progress = ProducerResult::new();
        progress.begin_attempt();
        progress.finish_attempt(Ok(()), Duration::ZERO);
        progress.begin_attempt();
        progress.finish_attempt(Err("storage flush failed".into()), Duration::ZERO);
        let mut report = Report::new(&config(), LoadLimits::for_config(&config()));
        report.add_producer(&progress);
        assert_eq!(report.ops_total, 1);
        assert_eq!(report.attempted_ops_total, 2);
        assert_eq!(report.errors_total, 1);
        assert_eq!(report.uncertain_ops_total, 1);
        assert_eq!(report.exact_expected_counter(), None);
        let json: serde_json::Value = serde_json::from_str(&report.to_json()).unwrap();
        assert_eq!(json["definite_errors_total"], 0);
        assert_eq!(
            json["attempted_ops_total"].as_u64().unwrap(),
            json["ops_total"].as_u64().unwrap()
                + json["definite_errors_total"].as_u64().unwrap()
                + json["uncertain_ops_total"].as_u64().unwrap()
        );
        assert!(
            report
                .error_details
                .iter()
                .any(|e| e.contains("storage flush failed"))
        );
    }

    #[test]
    fn acknowledged_successes_define_the_exact_counter() {
        let mut progress = ProducerResult::new();
        for _ in 0..3 {
            progress.begin_attempt();
            progress.finish_attempt(Ok(()), Duration::ZERO);
        }
        let mut report = Report::new(&config(), LoadLimits::for_config(&config()));
        report.add_producer(&progress);
        assert_eq!(report.exact_expected_counter(), Some(3));
    }

    #[tokio::test]
    async fn timed_out_producer_is_aborted_joined_and_progress_is_preserved() {
        let progress = Arc::new(std::sync::Mutex::new(ProducerResult::new()));
        let state = progress.clone();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            {
                let mut state = state.lock().unwrap();
                state.begin_attempt();
                state.finish_attempt(Ok(()), Duration::ZERO);
                state.begin_attempt();
            }
            entered_tx.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        let abort = task.abort_handle();
        entered_rx.await.unwrap();
        let result = collect_producer(Producer { task, progress }, Duration::ZERO).await;
        assert!(abort.is_finished(), "producer must not remain detached");
        assert_eq!(result.ops_total, 1);
        assert_eq!(result.attempted_ops_total, 2);
        assert_eq!(result.uncertain_ops_total, 1);
        assert_eq!(result.errors_total, result.uncertain_ops_total);
        let mut report = Report::new(&config(), LoadLimits::for_config(&config()));
        report.add_producer(&result);
        assert_eq!(report.exact_expected_counter(), None);
        let json: serde_json::Value = serde_json::from_str(&report.to_json()).unwrap();
        assert_eq!(json["definite_errors_total"], 0);
        assert!(result.error_details.iter().any(|e| e.contains("timed out")));
    }

    #[tokio::test]
    async fn panicking_producer_retains_the_in_flight_unknown() {
        let progress = Arc::new(std::sync::Mutex::new(ProducerResult::new()));
        progress.lock().unwrap().begin_attempt();
        let task = tokio::spawn(async { panic!("injected producer failure") });
        let result = collect_producer(Producer { task, progress }, SEND_TIMEOUT).await;
        assert_eq!(result.uncertain_ops_total, 1);
        assert!(
            result
                .error_details
                .iter()
                .any(|e| e.contains("producer task failed"))
        );
    }

    #[tokio::test]
    async fn failure_report_is_written_after_cleanup_with_unknown_and_missed_work() {
        let dir = tempfile::Builder::new()
            .prefix("benchmark-test-")
            .tempdir_in(".")
            .unwrap();
        let mut config = config();
        config.report = Some(dir.path().join("report.json"));
        let mut report = Report::new(&config, LoadLimits::for_config(&config));
        let mut progress = ProducerResult::new();
        progress.begin_attempt();
        progress.finish_attempt(
            Err("operation timed out; outcome unknown".into()),
            Duration::ZERO,
        );
        report.add_producer(&progress);
        let result = finish_run(
            &config,
            report,
            &mut [],
            Err("producer task timed out".into()),
        )
        .await;
        assert!(result.is_err());
        let json: serde_json::Value =
            serde_json::from_slice(&fs::read(config.report.unwrap()).unwrap()).unwrap();
        assert_eq!(json["report_complete"], false);
        assert_eq!(json["expected_counter"], serde_json::Value::Null);
        assert_eq!(json["converged"], false);
        assert_eq!(json["uncertain_ops_total"], 1);
        assert_eq!(json["unattempted_ops_total"], 2);
        assert_eq!(json["cleanup_errors_total"], 0);
        assert!(json["error_details"].as_array().unwrap().len() >= 2);
    }

    #[tokio::test]
    async fn setup_failure_keeps_started_managers_owned_for_cleanup() {
        let dir = tempfile::Builder::new()
            .prefix("benchmark-test-")
            .tempdir_in(".")
            .unwrap();
        let mut config = config();
        config.report = Some(dir.path().join("setup-failure.json"));
        let addr = free_addr().unwrap();
        let mut nodes = Vec::new();
        let result = start_full_mesh(
            &mut nodes,
            &[addr.clone(), addr.clone()],
            LoadLimits::for_config(&config),
            config.anti_entropy_interval,
        )
        .await;
        assert!(result.is_err());
        assert_eq!(nodes.len(), 2);
        let report = Report::new(&config, LoadLimits::for_config(&config));
        assert!(
            finish_run(&config, report, &mut nodes, result)
                .await
                .is_err()
        );
        let json: serde_json::Value =
            serde_json::from_slice(&fs::read(config.report.unwrap()).unwrap()).unwrap();
        assert_eq!(json["cleanup_errors_total"], 0);
        assert_eq!(json["report_complete"], false);
        assert!(
            json["error_details"][0]
                .as_str()
                .unwrap()
                .contains("start sync manager")
        );
        let listener = TcpListener::bind(addr).expect("cleanup must release the listener");
        drop(listener);
    }

    #[test]
    fn failure_details_are_bounded_without_losing_counts() {
        let mut progress = ProducerResult::new();
        for _ in 0..100 {
            progress.begin_attempt();
            progress.finish_attempt(Err("injected failure".repeat(200)), Duration::ZERO);
        }
        assert_eq!(progress.errors_total, 100);
        assert_eq!(progress.uncertain_ops_total, 100);
        assert!(progress.error_details.len() <= MAX_ERROR_DETAILS);
        assert!(
            progress
                .error_details
                .iter()
                .all(|detail| detail.chars().count() <= MAX_ERROR_DETAIL_CHARS)
        );
    }

    #[test]
    fn cleanup_failure_invalidates_an_otherwise_complete_report() {
        let mut report = Report::new(&config(), LoadLimits::for_config(&config()));
        report.ops_total = report.target_ops_total;
        report.attempted_ops_total = report.target_ops_total;
        report.converged = true;
        report.finish(
            Ok(()),
            vec!["injected shutdown failure".into()],
            Duration::ZERO,
        );
        let json: serde_json::Value = serde_json::from_str(&report.to_json()).unwrap();
        assert_eq!(json["cleanup_errors_total"], 1);
        assert_eq!(json["report_complete"], false);
        assert!(
            json["error_details"][0]
                .as_str()
                .unwrap()
                .contains("shutdown failure")
        );
    }

    #[test]
    fn total_duration_includes_cleanup_even_when_shutdown_fails() {
        let mut report = Report::new(&config(), LoadLimits::for_config(&config()));
        report.total_duration_secs = 45.043;
        report.load_duration_secs = 45.036;
        report.finish(
            Ok(()),
            vec!["injected shutdown timeout".into()],
            Duration::from_secs(10),
        );
        let json: serde_json::Value = serde_json::from_str(&report.to_json()).unwrap();
        assert!((json["total_duration_secs"].as_f64().unwrap() - 55.043).abs() < 0.000_001);
        assert_eq!(json["load_duration_secs"], 45.036);
        assert_eq!(json["cleanup_errors_total"], 1);
        assert_eq!(json["report_complete"], false);
    }

    #[test]
    fn report_separates_definite_and_uncertain_errors_without_double_counting() {
        let mut report = Report::new(&config(), LoadLimits::for_config(&config()));
        report.attempted_ops_total = 3;
        report.ops_total = 1;
        report.errors_total = 2;
        report.uncertain_ops_total = 1;
        let json: serde_json::Value = serde_json::from_str(&report.to_json()).unwrap();
        assert_eq!(json["definite_errors_total"], 1);
        assert_eq!(json["errors_total"], 2);
        assert_eq!(
            json["attempted_ops_total"].as_u64().unwrap(),
            json["ops_total"].as_u64().unwrap()
                + json["definite_errors_total"].as_u64().unwrap()
                + json["uncertain_ops_total"].as_u64().unwrap()
        );
    }

    #[test]
    fn successful_complete_report_retains_the_acceptance_profile() {
        let mut report = Report::new(&config(), LoadLimits::for_config(&config()));
        report.ops_total = report.target_ops_total;
        report.attempted_ops_total = report.target_ops_total;
        report.observed_counters = vec![report.ops_total; report.nodes];
        report.converged = true;
        report.finish(Ok(()), Vec::new(), Duration::ZERO);
        let json: serde_json::Value = serde_json::from_str(&report.to_json()).unwrap();
        assert_eq!(json["report_complete"], true);
        assert_eq!(json["report_schema_version"], 2);
        assert_eq!(json["profile"]["acceptance"], ACCEPTANCE_PROFILE);
        assert_eq!(json["expected_counter"], report.ops_total);
    }
}
