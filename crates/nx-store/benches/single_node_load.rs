use std::env;
use std::fs;
use std::path::PathBuf;
use std::process;
use std::thread;
use std::time::{Duration, Instant};

use nx_store::Store;

const DEFAULT_DURATION_SECS: u64 = 30;
const DEFAULT_TARGET_OPS_SEC: u64 = 10_000;
const DEFAULT_VALUE_SIZE: usize = 256;
const DEFAULT_KEY_SPACE: u64 = 1_000_000;
const TICK: Duration = Duration::from_millis(100);
const HISTOGRAM_MAX_MICROS: usize = 1_000_000;
const REPORT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug)]
struct Config {
    duration: Duration,
    target_ops_sec: u64,
    value_size: usize,
    key_space: u64,
    data_dir: Option<PathBuf>,
    report: Option<PathBuf>,
}

#[derive(Debug)]
struct Report {
    scenario: &'static str,
    requested_duration_secs: u64,
    duration_secs: f64,
    target_ops_sec: u64,
    ops_total: u64,
    ops_sec_avg: f64,
    errors_total: u64,
    value_size: usize,
    key_space: u64,
    data_dir: String,
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

fn main() {
    if let Err(err) = run() {
        eprintln!("single_node_load failed: {err}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let config = Config::parse(env::args().skip(1))?;
    let temp_dir;
    let data_dir = match &config.data_dir {
        Some(path) => path.clone(),
        None => {
            temp_dir = tempfile::tempdir().map_err(|e| format!("create temp dir: {e}"))?;
            temp_dir.path().to_path_buf()
        }
    };

    let store = Store::open(&data_dir).map_err(|e| format!("open store: {e}"))?;
    let value = vec![b'x'; config.value_size];
    let started = Instant::now();
    let deadline = started + config.duration;
    let ops_per_tick = (config.target_ops_sec / ticks_per_second()).max(1);
    let mut next_tick = started;
    let mut op_index = 0u64;
    let mut errors_total = 0u64;
    let mut latencies = LatencyHistogram::new();

    while Instant::now() < deadline {
        for _ in 0..ops_per_tick {
            if Instant::now() >= deadline {
                break;
            }

            let key = format!("load:{:016}", op_index % config.key_space);
            let op_started = Instant::now();
            match store.set(key.as_bytes(), &value) {
                Ok(()) => latencies.record(op_started.elapsed()),
                Err(_) => errors_total += 1,
            }
            op_index += 1;
        }

        next_tick += TICK;
        let now = Instant::now();
        if next_tick > now {
            thread::sleep(next_tick - now);
        } else {
            next_tick = now;
        }
    }

    store.flush().map_err(|e| format!("flush store: {e}"))?;
    let elapsed = started.elapsed();
    let report = Report {
        scenario: "single-node-store-write",
        requested_duration_secs: config.duration.as_secs(),
        duration_secs: elapsed.as_secs_f64(),
        target_ops_sec: config.target_ops_sec,
        ops_total: op_index,
        ops_sec_avg: op_index as f64 / elapsed.as_secs_f64(),
        errors_total,
        value_size: config.value_size,
        key_space: config.key_space,
        data_dir: data_dir.display().to_string(),
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

    Ok(())
}

impl Config {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut config = Self {
            duration: Duration::from_secs(DEFAULT_DURATION_SECS),
            target_ops_sec: DEFAULT_TARGET_OPS_SEC,
            value_size: DEFAULT_VALUE_SIZE,
            key_space: DEFAULT_KEY_SPACE,
            data_dir: None,
            report: None,
        };

        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--duration-secs" => {
                    config.duration = Duration::from_secs(parse_next(&mut args, &arg)?);
                }
                "--target-ops-sec" => {
                    config.target_ops_sec = parse_next(&mut args, &arg)?;
                }
                "--value-size" => {
                    config.value_size = parse_next(&mut args, &arg)?;
                }
                "--key-space" => {
                    config.key_space = parse_next(&mut args, &arg)?;
                }
                "--data-dir" => {
                    config.data_dir = Some(PathBuf::from(next_value(&mut args, &arg)?));
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
        if config.target_ops_sec == 0 {
            return Err("--target-ops-sec must be greater than zero".to_string());
        }
        if config.value_size == 0 {
            return Err("--value-size must be greater than zero".to_string());
        }
        if config.key_space == 0 {
            return Err("--key-space must be greater than zero".to_string());
        }

        Ok(config)
    }
}

impl Report {
    fn to_json(&self) -> String {
        format!(
            concat!(
                "{{\n",
                "  \"report_schema_version\": {},\n",
                "  \"crate\": \"nx-store\",\n",
                "  \"benchmark\": \"single_node_load\",\n",
                "  \"scenario\": \"{}\",\n",
                "  \"profile\": {{\n",
                "    \"duration_secs\": {},\n",
                "    \"target_ops_sec\": {},\n",
                "    \"value_size\": {},\n",
                "    \"key_space\": {}\n",
                "  }},\n",
                "  \"duration_secs\": {:.3},\n",
                "  \"target_ops_sec\": {},\n",
                "  \"ops_total\": {},\n",
                "  \"ops_sec_avg\": {:.2},\n",
                "  \"errors_total\": {},\n",
                "  \"value_size\": {},\n",
                "  \"key_space\": {},\n",
                "  \"data_dir\": \"{}\",\n",
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
            self.requested_duration_secs,
            self.target_ops_sec,
            self.value_size,
            self.key_space,
            self.duration_secs,
            self.target_ops_sec,
            self.ops_total,
            self.ops_sec_avg,
            self.errors_total,
            self.value_size,
            self.key_space,
            json_escape(&self.data_dir),
            json_option_u64(self.rss_bytes),
            self.latency.p50_ms,
            self.latency.p95_ms,
            self.latency.p99_ms,
            self.latency.max_ms,
            self.latency.avg_ms
        )
    }
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

fn json_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
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
Single-node nx-store load benchmark

Options:
  --duration-secs N     Duration in seconds (default: {DEFAULT_DURATION_SECS})
  --target-ops-sec N    Target write operations per second (default: {DEFAULT_TARGET_OPS_SEC})
  --value-size N        Value size in bytes (default: {DEFAULT_VALUE_SIZE})
  --key-space N         Number of keys to cycle through (default: {DEFAULT_KEY_SPACE})
  --data-dir PATH       Store path (default: temporary directory)
  --report PATH         Write JSON report to PATH

Release-gate example:
  cargo bench -p nx-store --bench single_node_load -- --duration-secs 3600 --target-ops-sec 10000
"
    );
}
