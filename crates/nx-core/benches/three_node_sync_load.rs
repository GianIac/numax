use std::env;
use std::fs;
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
use tokio::time::{sleep, timeout};

const DEFAULT_DURATION_SECS: u64 = 30;
const DEFAULT_TARGET_OPS_SEC_PER_NODE: u64 = 1_000;
const DEFAULT_SETTLE_SECS: u64 = 10;
const TICK: Duration = Duration::from_millis(100);
const HISTOGRAM_MAX_MICROS: usize = 1_000_000;
const SEND_TIMEOUT: Duration = Duration::from_secs(5);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug)]
struct Config {
    duration: Duration,
    target_ops_sec_per_node: u64,
    settle: Duration,
    report: Option<PathBuf>,
}

#[derive(Debug)]
struct Report {
    scenario: &'static str,
    nodes: usize,
    load_duration_secs: f64,
    total_duration_secs: f64,
    target_ops_sec_per_node: u64,
    target_ops_sec_total: u64,
    ops_total: u64,
    ops_sec_avg: f64,
    errors_total: u64,
    expected_counter: u64,
    observed_counters: [u64; 3],
    converged: bool,
    convergence_wait_secs: f64,
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
    let addrs = [free_addr()?, free_addr()?, free_addr()?];
    let mut nodes = start_full_mesh(&addrs).await?;
    wait_for_full_mesh(&nodes).await?;

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
    let expected_counter = ops_total - errors_total;
    let convergence_started = Instant::now();
    let converged = wait_for_convergence(&nodes, key, expected_counter, config.settle).await;
    let observed_counters = [
        nodes[0].manager.get_counter_value(key).await,
        nodes[1].manager.get_counter_value(key).await,
        nodes[2].manager.get_counter_value(key).await,
    ];

    let report = Report {
        scenario: "three-node-sync-gcounter",
        nodes: nodes.len(),
        load_duration_secs: load_elapsed.as_secs_f64(),
        total_duration_secs: started.elapsed().as_secs_f64(),
        target_ops_sec_per_node: config.target_ops_sec_per_node,
        target_ops_sec_total: config.target_ops_sec_per_node * nodes.len() as u64,
        ops_total,
        ops_sec_avg: ops_total as f64 / load_elapsed.as_secs_f64(),
        errors_total,
        expected_counter,
        observed_counters,
        converged,
        convergence_wait_secs: convergence_started.elapsed().as_secs_f64(),
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

async fn start_full_mesh(addrs: &[String; 3]) -> Result<Vec<BenchNode>, String> {
    let configs = [
        SyncConfig::new()
            .with_listen_addr(addrs[0].clone())
            .with_peer(addrs[1].clone())
            .with_peer(addrs[2].clone())
            .with_queued_ops_limit(100_000)
            .with_op_log_limit(200_000)
            .with_seen_ops_limit(300_000)
            .with_reconnect_backoff(Duration::from_millis(50), Duration::from_millis(250))
            .with_anti_entropy_interval(Duration::from_secs(60)),
        SyncConfig::new()
            .with_listen_addr(addrs[1].clone())
            .with_peer(addrs[0].clone())
            .with_peer(addrs[2].clone())
            .with_queued_ops_limit(100_000)
            .with_op_log_limit(200_000)
            .with_seen_ops_limit(300_000)
            .with_reconnect_backoff(Duration::from_millis(50), Duration::from_millis(250))
            .with_anti_entropy_interval(Duration::from_secs(60)),
        SyncConfig::new()
            .with_listen_addr(addrs[2].clone())
            .with_peer(addrs[0].clone())
            .with_peer(addrs[1].clone())
            .with_queued_ops_limit(100_000)
            .with_op_log_limit(200_000)
            .with_seen_ops_limit(300_000)
            .with_reconnect_backoff(Duration::from_millis(50), Duration::from_millis(250))
            .with_anti_entropy_interval(Duration::from_secs(60)),
    ];

    let mut nodes = Vec::new();
    for config in configs {
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
        let deadline = started + duration;
        let ops_per_tick = (target_ops_sec / ticks_per_second()).max(1);
        let mut next_tick = started;
        let mut result = ProducerResult {
            ops_total: 0,
            errors_total: 0,
            latencies: LatencyHistogram::new(),
        };

        while Instant::now() < deadline {
            for _ in 0..ops_per_tick {
                if Instant::now() >= deadline {
                    break;
                }

                let op_started = Instant::now();
                match local_increment(&handle, &key).await {
                    Ok(()) => {
                        result.ops_total += 1;
                        result.latencies.record(op_started.elapsed());
                    }
                    Err(_) => result.errors_total += 1,
                }
            }

            next_tick += TICK;
            let now = Instant::now();
            if next_tick > now {
                sleep(next_tick - now).await;
            } else {
                next_tick = now;
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
    loop {
        let counts = futures_counts(nodes).await;
        if counts.iter().all(|count| *count >= 2) {
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
        let values = [
            nodes[0].manager.get_counter_value(key).await,
            nodes[1].manager.get_counter_value(key).await,
            nodes[2].manager.get_counter_value(key).await,
        ];
        if values.iter().all(|value| *value == expected) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        sleep(Duration::from_millis(100)).await;
    }
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
            duration: Duration::from_secs(DEFAULT_DURATION_SECS),
            target_ops_sec_per_node: DEFAULT_TARGET_OPS_SEC_PER_NODE,
            settle: Duration::from_secs(DEFAULT_SETTLE_SECS),
            report: None,
        };

        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--duration-secs" => {
                    config.duration = Duration::from_secs(parse_next(&mut args, &arg)?);
                }
                "--target-ops-sec-per-node" => {
                    config.target_ops_sec_per_node = parse_next(&mut args, &arg)?;
                }
                "--settle-secs" => {
                    config.settle = Duration::from_secs(parse_next(&mut args, &arg)?);
                }
                "--report" => {
                    config.report = Some(PathBuf::from(next_value(&mut args, &arg)?));
                }
                "--bench" => {}
                "--help" | "-h" => {
                    print_help();
                    process::exit(0);
                }
                other => return Err(format!("unknown argument: {other}")),
            }
        }

        if config.duration.is_zero() {
            return Err("--duration-secs must be greater than zero".to_string());
        }
        if config.target_ops_sec_per_node == 0 {
            return Err("--target-ops-sec-per-node must be greater than zero".to_string());
        }
        Ok(config)
    }
}

impl Report {
    fn to_json(&self) -> String {
        format!(
            concat!(
                "{{\n",
                "  \"scenario\": \"{}\",\n",
                "  \"nodes\": {},\n",
                "  \"load_duration_secs\": {:.3},\n",
                "  \"total_duration_secs\": {:.3},\n",
                "  \"target_ops_sec_per_node\": {},\n",
                "  \"target_ops_sec_total\": {},\n",
                "  \"ops_total\": {},\n",
                "  \"ops_sec_avg\": {:.2},\n",
                "  \"errors_total\": {},\n",
                "  \"expected_counter\": {},\n",
                "  \"observed_counters\": [{}, {}, {}],\n",
                "  \"converged\": {},\n",
                "  \"convergence_wait_secs\": {:.3},\n",
                "  \"latency_ms\": {{\n",
                "    \"p50\": {:.3},\n",
                "    \"p95\": {:.3},\n",
                "    \"p99\": {:.3},\n",
                "    \"max\": {:.3},\n",
                "    \"avg\": {:.3}\n",
                "  }}\n",
                "}}"
            ),
            self.scenario,
            self.nodes,
            self.load_duration_secs,
            self.total_duration_secs,
            self.target_ops_sec_per_node,
            self.target_ops_sec_total,
            self.ops_total,
            self.ops_sec_avg,
            self.errors_total,
            self.expected_counter,
            self.observed_counters[0],
            self.observed_counters[1],
            self.observed_counters[2],
            self.converged,
            self.convergence_wait_secs,
            self.latency.p50_ms,
            self.latency.p95_ms,
            self.latency.p99_ms,
            self.latency.max_ms,
            self.latency.avg_ms
        )
    }
}

fn free_addr() -> Result<String, String> {
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|e| format!("bind temp addr: {e}"))?;
    let addr = listener
        .local_addr()
        .map_err(|e| format!("read temp addr: {e}"))?;
    Ok(addr.to_string())
}

fn ticks_per_second() -> u64 {
    Duration::from_secs(1).as_millis() as u64 / TICK.as_millis() as u64
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

fn print_help() {
    println!(
        "\
Three-node continuous sync benchmark

Options:
  --duration-secs N                 Duration in seconds (default: {DEFAULT_DURATION_SECS})
  --target-ops-sec-per-node N       Target increment ops/sec per node (default: {DEFAULT_TARGET_OPS_SEC_PER_NODE})
  --settle-secs N                   Time allowed for final convergence (default: {DEFAULT_SETTLE_SECS})
  --report PATH                     Write JSON report to PATH

Smoke example:
  cargo bench -p nx-core --bench three_node_sync_load -- --duration-secs 10 --target-ops-sec-per-node 1000
"
    );
}
