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
use tokio::time::{MissedTickBehavior, interval, sleep, timeout};

const DEFAULT_NODE_COUNT: usize = 3;
const DEFAULT_DURATION_SECS: u64 = 60;
const DEFAULT_TARGET_OPS_SEC: u64 = 100;
const DEFAULT_RESTART_EVERY_SECS: u64 = 10;
const DEFAULT_SETTLE_SECS: u64 = 30;
const TICK: Duration = Duration::from_millis(100);
const HISTOGRAM_MAX_MICROS: usize = 1_000_000;
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const LOAD_LIMIT_HEADROOM_OPS: u64 = 100_000;
const LOAD_TEST_MAX_MESSAGE_SIZE: usize = 256 * 1024 * 1024;

#[derive(Debug)]
struct Config {
    nodes: usize,
    duration: Duration,
    target_ops_sec: u64,
    restart_every: Duration,
    settle: Duration,
    report: Option<PathBuf>,
}

#[derive(Debug)]
struct Report {
    scenario: &'static str,
    nodes: usize,
    load_duration_secs: f64,
    total_duration_secs: f64,
    target_ops_sec: u64,
    ops_total: u64,
    ops_sec_avg: f64,
    errors_total: u64,
    restarts_total: u64,
    queued_ops_limit: usize,
    op_log_limit: usize,
    seen_ops_limit: usize,
    expected_counter: u64,
    observed_counters: Vec<u64>,
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
    node_id: NodeId,
    config: SyncConfig,
    store: Arc<Store>,
    _store_dir: tempfile::TempDir,
}

#[derive(Debug, Clone, Copy)]
struct LoadLimits {
    queued_ops_limit: usize,
    op_log_limit: usize,
    seen_ops_limit: usize,
    max_message_size: usize,
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

impl LoadLimits {
    fn for_config(config: &Config) -> Self {
        let expected_ops = config
            .target_ops_sec
            .saturating_mul(config.duration.as_secs())
            .saturating_add(LOAD_LIMIT_HEADROOM_OPS);
        let total_seen = expected_ops.saturating_mul(config.nodes as u64);

        Self {
            queued_ops_limit: expected_ops.min(usize::MAX as u64) as usize,
            op_log_limit: expected_ops.min(usize::MAX as u64) as usize,
            seen_ops_limit: total_seen.min(usize::MAX as u64) as usize,
            max_message_size: LOAD_TEST_MAX_MESSAGE_SIZE,
        }
    }
}

fn main() {
    if let Err(err) = run() {
        eprintln!("chaos_sync_load failed: {err}");
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
    let key = "chaos:counter";
    let addrs = reserve_addrs(config.nodes)?;
    let limits = LoadLimits::for_config(&config);
    let mut nodes = start_full_mesh(&addrs, limits).await?;
    wait_for_full_mesh(&nodes).await?;

    let started = Instant::now();
    let mut load_tick = interval(TICK);
    load_tick.set_missed_tick_behavior(MissedTickBehavior::Burst);
    let mut restart_tick = interval(config.restart_every);
    restart_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    restart_tick.tick().await;

    let target_ops = config
        .target_ops_sec
        .saturating_mul(config.duration.as_secs());
    let mut ops_total = 0u64;
    let mut errors_total = 0u64;
    let mut restarts_total = 0u64;
    let mut restart_index = 1usize;
    let mut latencies = LatencyHistogram::new();

    while ops_total + errors_total < target_ops {
        tokio::select! {
            _ = load_tick.tick() => {
                let elapsed = started.elapsed().min(config.duration);
                let expected_by_now = target_ops.min(
                    (elapsed.as_nanos().saturating_mul(config.target_ops_sec as u128)
                        / Duration::from_secs(1).as_nanos()) as u64,
                );
                let produced = ops_total + errors_total;
                let due_ops = expected_by_now.saturating_sub(produced);

                for _ in 0..due_ops {
                    let op_started = Instant::now();
                    match local_increment(&nodes[0].handle, key).await {
                        Ok(()) => {
                            ops_total += 1;
                            latencies.record(op_started.elapsed());
                        }
                        Err(()) => errors_total += 1,
                    }
                }
            }
            _ = restart_tick.tick(), if nodes.len() > 1 && started.elapsed() < config.duration => {
                restart_node(&mut nodes[restart_index]).await?;
                restarts_total += 1;
                restart_index += 1;
                if restart_index >= nodes.len() {
                    restart_index = 1;
                }
            }
        }
    }

    let load_elapsed = started.elapsed();
    let expected_counter = ops_total - errors_total;
    let convergence_started = Instant::now();
    let converged = wait_for_convergence(&nodes, key, expected_counter, config.settle).await;
    let observed_counters = observed_counters(&nodes, key).await;

    let report = Report {
        scenario: "chaos-sync-restart-gcounter",
        nodes: nodes.len(),
        load_duration_secs: load_elapsed.as_secs_f64(),
        total_duration_secs: started.elapsed().as_secs_f64(),
        target_ops_sec: config.target_ops_sec,
        ops_total,
        ops_sec_avg: ops_total as f64 / load_elapsed.as_secs_f64(),
        errors_total,
        restarts_total,
        queued_ops_limit: limits.queued_ops_limit,
        op_log_limit: limits.op_log_limit,
        seen_ops_limit: limits.seen_ops_limit,
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

    let shutdown_errors = shutdown_all(&mut nodes).await;
    if report.errors_total > 0 {
        return Err(format!(
            "chaos benchmark completed with {} producer errors",
            report.errors_total
        ));
    }
    if shutdown_errors > 0 {
        return Err(format!(
            "chaos benchmark completed but {shutdown_errors} sync managers failed to shut down cleanly"
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

async fn start_full_mesh(addrs: &[String], limits: LoadLimits) -> Result<Vec<BenchNode>, String> {
    let mut nodes = Vec::new();
    for (index, listen_addr) in addrs.iter().enumerate() {
        let mut config = SyncConfig::new()
            .with_listen_addr(listen_addr.clone())
            .with_queued_ops_limit(limits.queued_ops_limit)
            .with_op_log_limit(limits.op_log_limit)
            .with_seen_ops_limit(limits.seen_ops_limit)
            .with_max_message_size(limits.max_message_size)
            .with_reconnect_backoff(Duration::from_millis(50), Duration::from_millis(250))
            .with_anti_entropy_interval(Duration::from_secs(1));
        for (peer_index, peer_addr) in addrs.iter().enumerate() {
            if peer_index != index {
                config = config.with_peer(peer_addr.clone());
            }
        }

        let store_dir = tempfile::tempdir().map_err(|e| format!("create temp store dir: {e}"))?;
        let store =
            Arc::new(Store::open(store_dir.path()).map_err(|e| format!("open store: {e}"))?);
        let node_id = NodeId::generate();
        let mut manager = SyncManager::new(
            node_id.clone(),
            config.clone(),
            Arc::clone(&store),
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
            node_id,
            config,
            store,
            _store_dir: store_dir,
        });
    }

    Ok(nodes)
}

async fn restart_node(node: &mut BenchNode) -> Result<(), String> {
    timeout(SHUTDOWN_TIMEOUT, node.manager.shutdown())
        .await
        .map_err(|_| "restart shutdown timed out".to_string())?
        .map_err(|e| format!("restart shutdown failed: {e}"))?;

    let mut manager = SyncManager::new(
        node.node_id.clone(),
        node.config.clone(),
        Arc::clone(&node.store),
        Arc::new(RuntimeMetrics::default()),
    );
    let handle = manager.handle();
    manager
        .start()
        .await
        .map_err(|e| format!("restart start failed: {e}"))?;
    node.manager = manager;
    node.handle = handle;
    Ok(())
}

async fn local_increment(handle: &SyncHandle, key: &str) -> Result<(), ()> {
    let op = Op::gcounter_increment(handle.node_id().clone(), key, 1);
    handle.op_sender().send(op).await.map_err(|_| ())?;

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
        let counts = connected_counts(nodes).await;
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
    timeout_duration: Duration,
) -> bool {
    let deadline = Instant::now() + timeout_duration;
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

async fn connected_counts(nodes: &[BenchNode]) -> Vec<usize> {
    let mut counts = Vec::with_capacity(nodes.len());
    for node in nodes {
        counts.push(node.manager.connected_peer_count().await);
    }
    counts
}

async fn shutdown_all(nodes: &mut [BenchNode]) -> u64 {
    let mut shutdown_errors = 0u64;
    for node in nodes {
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
    shutdown_errors
}

impl Config {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut config = Self {
            nodes: DEFAULT_NODE_COUNT,
            duration: Duration::from_secs(DEFAULT_DURATION_SECS),
            target_ops_sec: DEFAULT_TARGET_OPS_SEC,
            restart_every: Duration::from_secs(DEFAULT_RESTART_EVERY_SECS),
            settle: Duration::from_secs(DEFAULT_SETTLE_SECS),
            report: None,
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
                "--target-ops-sec" => {
                    config.target_ops_sec = parse_next(&mut args, &arg)?;
                }
                "--restart-every-secs" => {
                    config.restart_every = Duration::from_secs(parse_next(&mut args, &arg)?);
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

        if config.nodes < 2 {
            return Err("--nodes must be at least 2".to_string());
        }
        if config.duration.is_zero() {
            return Err("--duration-secs must be greater than zero".to_string());
        }
        if config.target_ops_sec == 0 {
            return Err("--target-ops-sec must be greater than zero".to_string());
        }
        if config.restart_every.is_zero() {
            return Err("--restart-every-secs must be greater than zero".to_string());
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
                "  \"target_ops_sec\": {},\n",
                "  \"ops_total\": {},\n",
                "  \"ops_sec_avg\": {:.2},\n",
                "  \"errors_total\": {},\n",
                "  \"restarts_total\": {},\n",
                "  \"queued_ops_limit\": {},\n",
                "  \"op_log_limit\": {},\n",
                "  \"seen_ops_limit\": {},\n",
                "  \"expected_counter\": {},\n",
                "  \"observed_counters\": [{}],\n",
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
            self.target_ops_sec,
            self.ops_total,
            self.ops_sec_avg,
            self.errors_total,
            self.restarts_total,
            self.queued_ops_limit,
            self.op_log_limit,
            self.seen_ops_limit,
            self.expected_counter,
            join_u64s(&self.observed_counters),
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

fn print_help() {
    println!(
        "\
Chaos sync load benchmark

Options:
  --nodes N                         Number of full-mesh nodes (default: {DEFAULT_NODE_COUNT})
  --duration-secs N                 Duration in seconds (default: {DEFAULT_DURATION_SECS})
  --target-ops-sec N                Source-node increment ops/sec (default: {DEFAULT_TARGET_OPS_SEC})
  --restart-every-secs N            Restart a follower every N seconds (default: {DEFAULT_RESTART_EVERY_SECS})
  --settle-secs N                   Time allowed for final convergence (default: {DEFAULT_SETTLE_SECS})
  --report PATH                     Write JSON report to PATH

Smoke example:
  cargo bench -p nx-core --bench chaos_sync_load -- --duration-secs 30 --target-ops-sec 100 --restart-every-secs 10
"
    );
}
