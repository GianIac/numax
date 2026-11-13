use super::*;
use serde_json::{Value, json};
use std::io::{BufRead, Read, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError};

const WORKER: &str = "sync_manager::manager::tests::detector_soak::process::worker";
const CONFIG: &str = "NUMAX_DETECTOR_WORKER";
const PREFIX: &str = "DETECTOR_WORKER ";
const MAX_WORKER_OUTPUT_LINE_BYTES: usize = 64 * 1024;
const MAX_RECOVERY_DETAIL_SAMPLES: usize = 8;
const MAX_CONVERGENCE_DETAIL_WORKERS: usize = 4;
const DETECTOR_PROBE_SEED: u64 = 0x4e55_4d41_5801_0001;
const PROBE_SEED_ENV: &str = "NUMAX_TEST_PROBE_SEED";
const TIMELINE_DIR_ENV: &str = "NUMAX_DETECTOR_TRACE_DIR";
#[cfg(target_os = "macos")]
const STALL_DIR_ENV: &str = "NUMAX_DETECTOR_STALL_DIR";
const STALL_TRIGGER_US: u64 = 10_000_000;
const MAX_NATIVE_OUTPUT_BYTES: usize = 1024 * 1024;
const NATIVE_COMMAND_LIMIT: Duration = Duration::from_secs(10);

struct NativeCommand {
    child: Child,
    readers: Vec<std::thread::JoinHandle<anyhow::Result<Vec<u8>>>>,
    overflow: Arc<AtomicBool>,
}

impl Drop for NativeCommand {
    fn drop(&mut self) {
        match self.child.try_wait() {
            Ok(Some(_)) => {}
            Ok(None) => {
                if let Err(error) = self.child.kill() {
                    eprintln!("native diagnostic kill failed: {error}");
                }
                if let Err(error) = self.child.wait() {
                    eprintln!("native diagnostic reap failed: {error}");
                }
            }
            Err(error) => {
                eprintln!("native diagnostic status failed: {error}");
                if let Err(error) = self.child.kill() {
                    eprintln!("native diagnostic kill failed: {error}");
                }
                if let Err(error) = self.child.wait() {
                    eprintln!("native diagnostic reap failed: {error}");
                }
            }
        }
        for reader in self.readers.drain(..) {
            match reader.join() {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => eprintln!("native diagnostic output cleanup failed: {error:#}"),
                Err(_) => eprintln!("native diagnostic output reader panicked"),
            }
        }
    }
}

fn native_reader(
    input: impl Read + Send + 'static,
    overflow: Arc<AtomicBool>,
) -> std::thread::JoinHandle<anyhow::Result<Vec<u8>>> {
    std::thread::spawn(move || {
        let mut bytes = Vec::new();
        input
            .take(u64::try_from(MAX_NATIVE_OUTPUT_BYTES + 1)?)
            .read_to_end(&mut bytes)?;
        if bytes.len() > MAX_NATIVE_OUTPUT_BYTES {
            overflow.store(true, Ordering::Release);
            anyhow::bail!("native diagnostic output exceeded its byte cap");
        }
        Ok(bytes)
    })
}

async fn native_output(
    mut command: Command,
    limit: Duration,
) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
    let child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("cannot start native diagnostic command")?;
    let mut owned = NativeCommand {
        child,
        readers: Vec::new(),
        overflow: Arc::new(AtomicBool::new(false)),
    };
    let stdout = owned.child.stdout.take().context("native stdout missing")?;
    let stderr = owned.child.stderr.take().context("native stderr missing")?;
    owned
        .readers
        .push(native_reader(stdout, owned.overflow.clone()));
    owned
        .readers
        .push(native_reader(stderr, owned.overflow.clone()));
    let deadline = Instant::now() + limit;
    let exit = loop {
        ensure!(
            !owned.overflow.load(Ordering::Acquire),
            "native diagnostic output exceeded its byte cap; pid={}",
            owned.child.id()
        );
        if let Some(exit) = owned.child.try_wait()? {
            break exit;
        }
        ensure!(
            Instant::now() < deadline,
            "native diagnostic timeout; pid={}",
            owned.child.id()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let mut output = Vec::new();
    let mut errors = Vec::new();
    for reader in owned.readers.drain(..) {
        match reader.join() {
            Ok(Ok(bytes)) => output.push(bytes),
            Ok(Err(error)) => errors.push(format!("{error:#}")),
            Err(_) => errors.push("native output reader panicked".into()),
        }
    }
    ensure!(
        errors.is_empty(),
        "native diagnostic output errors: {errors:?}"
    );
    let stderr = output.pop().context("native stderr result missing")?;
    let stdout = output.pop().context("native stdout result missing")?;
    ensure!(
        exit.success(),
        "native diagnostic exited {exit}; stderr_bytes={}: {}",
        stderr.len(),
        String::from_utf8_lossy(&stderr[..stderr.len().min(4096)])
    );
    Ok((stdout, stderr))
}

fn save_native_output(
    directory: &std::path::Path,
    name: &str,
    output: &(Vec<u8>, Vec<u8>),
) -> anyhow::Result<()> {
    for (suffix, bytes) in [("stdout", &output.0), ("stderr", &output.1)] {
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(directory.join(format!("{name}.{suffix}")))?;
        file.write_all(bytes)?;
    }
    Ok(())
}

fn stall_targets(rows: &[Value]) -> anyhow::Result<Vec<usize>> {
    let mut pending = Vec::new();
    for (index, row) in rows.iter().enumerate().filter(|(_, row)| !row.is_null()) {
        let sample = &row["pending_submissions"];
        if sample.is_null() {
            continue;
        }
        let count = number(sample, "count")?;
        ensure!(
            count <= 4,
            "pending submission sample exceeded writer limit"
        );
        if count > 0 {
            let age = number(sample, "oldest_elapsed_us")?;
            if age >= STALL_TRIGGER_US {
                pending.push((index, age));
            }
        }
    }
    pending
        .sort_by(|(index_a, age_a), (index_b, age_b)| age_b.cmp(age_a).then(index_a.cmp(index_b)));
    Ok(pending
        .into_iter()
        .take(3)
        .map(|(index, _)| index)
        .collect())
}

async fn capture_stall(
    group: &mut Processes,
    directory: &std::path::Path,
    elapsed: Duration,
    critical: bool,
    fail_native: bool,
    capture: &mut NativeCapture,
) -> anyhow::Result<()> {
    let before = group.rows();
    let targets = stall_targets(&before)?;
    ensure!(
        !targets.is_empty(),
        "stall disappeared before capture selection"
    );
    let capture_started = StdInstant::now();
    capture.report = Some(json!({
        "outcome": "capture_in_progress", "nominal_complete": false,
        "critical_path_enabled": critical, "injected_native_failure": fail_native,
        "elapsed_before_capture_us": u64::try_from(elapsed.as_micros())?,
        "trigger_oldest_submission_us": STALL_TRIGGER_US,
        "workers": targets.iter().map(|&index| json!({
            "index": index, "pid": group.children[index].child.id(), "before": before[index],
        })).collect::<Vec<_>>(),
        "status_samples_before": before,
        "interpretation": "asynchronous samples; source intervals are not logger delivery order; partial captures never certify nominal success",
    }));
    if critical {
        capture
            .critical_snapshots(group, &targets, 1, "critical_before")
            .await?;
    }
    let mut commands = JoinSet::new();
    for &index in &targets {
        let pid = group.children[index].child.id();
        commands.spawn(async move {
            let program = if fail_native {
                "/usr/bin/false"
            } else {
                "/usr/bin/sample"
            };
            let mut command = Command::new(program);
            command.args([
                pid.to_string(),
                "1".into(),
                "-file".into(),
                "/dev/stdout".into(),
            ]);
            (
                format!("worker-{index}.sample"),
                native_output(command, NATIVE_COMMAND_LIMIT)
                    .await
                    .with_context(|| {
                        format!("native program={program}, target_worker={index}, target_pid={pid}")
                    }),
            )
        });
    }
    for (name, program, args) in [
        ("host-vm-stat", "/usr/bin/vm_stat", vec![]),
        (
            "host-iostat",
            "/usr/sbin/iostat",
            vec!["-w", "1", "-c", "2"],
        ),
    ] {
        commands.spawn(async move {
            let mut command = Command::new(program);
            command.args(args);
            (
                name.to_owned(),
                native_output(command, NATIVE_COMMAND_LIMIT)
                    .await
                    .with_context(|| format!("native program={program}, command={name}")),
            )
        });
    }
    let mut errors = Vec::new();
    while let Some(output) = commands.join_next().await {
        match output {
            Ok((name, Ok(output))) => {
                if critical {
                    capture.outputs.push(NamedNativeOutput { name, output });
                } else if let Err(error) = save_native_output(directory, &name, &output) {
                    errors.push(format!("native export {name}: {error:#}"));
                }
            }
            Ok((name, Err(error))) => errors.push(format!("{name}: {error:#}")),
            Err(error) => errors.push(format!("native task failed: {error}")),
        }
    }
    commands.shutdown().await;
    capture.report.as_mut().context("capture report missing")?["native_command_errors"] =
        json!(errors);
    if critical {
        let result = capture
            .critical_snapshots(group, &targets, 2, "critical_after")
            .await;
        if let Err(error) = result {
            errors.push(format!("post-capture snapshots: {error:#}"));
        }
        let mut replay_traces = BTreeMap::new();
        if let Err(error) = group.remote_replay_snapshots(3, &mut replay_traces).await {
            errors.push(format!("remote replay trace snapshots: {error:#}"));
        } else {
            let workers = replay_traces
                .into_iter()
                .map(|(index, trace)| json!({"index": index, "trace": trace}))
                .collect::<Vec<_>>();
            let batch_count = workers.iter().try_fold(0u64, |total, worker| {
                total
                    .checked_add(number(&worker["trace"], "batch_count")?)
                    .context("remote replay batch count overflow")
            })?;
            let operation_count = workers.iter().try_fold(0u64, |total, worker| {
                total
                    .checked_add(number(&worker["trace"], "operation_count")?)
                    .context("remote replay operation count overflow")
            })?;
            let bytes = serde_json::to_vec(&json!({"version": 1, "workers": workers}))?;
            let path = directory.join("remote-replay-trace.json");
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .with_context(|| format!("cannot create {}", path.display()))?;
            file.write_all(&bytes)
                .with_context(|| format!("cannot write {}", path.display()))?;
            capture.report.as_mut().context("capture report missing")?["remote_replay_trace"] = json!({
                "file": "remote-replay-trace.json",
                "workers": workers.len(),
                "batches": batch_count,
                "operations": operation_count,
                "bytes": bytes.len(),
                "blake3": blake3::hash(&bytes).to_hex().to_string(),
            });
        }
    }
    let after = group.rows();
    let report = capture.report.as_mut().context("capture report missing")?;
    report["capture_duration_us"] = json!(u64::try_from(capture_started.elapsed().as_micros())?);
    report["status_samples_after"] = json!(after);
    for worker in report["workers"]
        .as_array_mut()
        .context("capture workers missing")?
    {
        let index = usize::try_from(number(worker, "index")?)?;
        worker["after"] = after[index].clone();
        worker["same_flush_at_last_samples"] = json!(
            !worker["before"]["diagnostics"]["active_covering_flush"].is_null()
                && worker["before"]["diagnostics"]["active_covering_flush"]["sequence"]
                    == worker["after"]["diagnostics"]["active_covering_flush"]["sequence"]
        );
    }
    ensure!(errors.is_empty(), "native capture failed: {errors:?}");
    report["outcome"] = json!("diagnostic_interrupted");
    Ok(())
}

#[derive(Default)]
struct NativeCapture {
    report: Option<Value>,
    outputs: Vec<NamedNativeOutput>,
}

impl NativeCapture {
    async fn critical_snapshots(
        &mut self,
        group: &mut Processes,
        targets: &[usize],
        id: u64,
        field: &str,
    ) -> anyhow::Result<()> {
        let mut snapshots = BTreeMap::new();
        let result = group.critical_snapshots(targets, id, &mut snapshots).await;
        for worker in self.report.as_mut().context("capture report missing")?["workers"]
            .as_array_mut()
            .context("capture workers missing")?
        {
            let index = usize::try_from(number(worker, "index")?)?;
            if let Some(snapshot) = snapshots.remove(&index) {
                worker[field] = snapshot;
            }
        }
        result
    }
}

struct NamedNativeOutput {
    name: String,
    output: (Vec<u8>, Vec<u8>),
}

#[test]
fn live_stall_trigger_is_bounded_and_uses_pending_not_completed_latency() {
    assert_eq!(
        stall_targets(&[json!({
            "pending_submissions": {"count": 1, "oldest_elapsed_us": STALL_TRIGGER_US},
        })])
        .unwrap(),
        [0]
    );
    assert_eq!(
        stall_targets(&[
            json!({"pending_submissions": {"count": 1, "oldest_elapsed_us": STALL_TRIGGER_US}}),
            json!({"pending_submissions": {"count": 1, "oldest_elapsed_us": STALL_TRIGGER_US}}),
        ])
        .unwrap(),
        [0, 1]
    );
    let rows = (0..100)
        .map(|index| {
            json!({
                "pending_submissions": {"count": 4, "oldest_elapsed_us": STALL_TRIGGER_US + index},
                "max_flush_us": 0,
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(stall_targets(&rows).unwrap(), [99, 98, 97]);
    assert!(
        stall_targets(&[json!({
            "pending_submissions": {"count": 0, "oldest_elapsed_us": null},
            "max_flush_us": 999_999_999,
        })])
        .unwrap()
        .is_empty()
    );
    assert!(
        stall_targets(&[json!({
            "pending_submissions": {"count": 1, "oldest_elapsed_us": STALL_TRIGGER_US - 1},
        })])
        .unwrap()
        .is_empty()
    );
    assert!(
        stall_targets(&[json!({
            "pending_submissions": {"count": 5, "oldest_elapsed_us": STALL_TRIGGER_US},
        })])
        .is_err()
    );
}

#[test]
#[ignore = "internal native-command ownership fixture"]
fn native_command_child() {
    let mode = std::env::var("NUMAX_NATIVE_COMMAND_MODE").unwrap();
    std::fs::write(
        std::env::var_os("NUMAX_NATIVE_PID_FILE").unwrap(),
        std::process::id().to_string(),
    )
    .unwrap();
    match mode.as_str() {
        "timeout" => loop {
            std::thread::park();
        },
        "cap" => {
            let bytes = vec![b'x'; MAX_NATIVE_OUTPUT_BYTES + 4096];
            std::io::stdout().write_all(&bytes).unwrap();
        }
        "failure" => panic!("deliberate native command failure"),
        _ => panic!("invalid internal native command mode"),
    }
}

#[cfg(unix)]
#[tokio::test]
async fn native_commands_report_failure_timeout_cap_and_reap() {
    let directory = tempfile::tempdir().unwrap();
    let name = "sync_manager::manager::tests::detector_soak::process::native_command_child";
    for (mode, expected) in [
        ("failure", "exited"),
        ("timeout", "timeout"),
        ("cap", "byte cap"),
    ] {
        let pid_file = directory.path().join(mode);
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", name, "--ignored", "--nocapture"])
            .env("NUMAX_NATIVE_COMMAND_MODE", mode)
            .env("NUMAX_NATIVE_PID_FILE", &pid_file);
        let error = native_output(command, Duration::from_secs(2))
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains(expected), "{error:#}");
        let pid = std::fs::read_to_string(pid_file).unwrap();
        assert!(
            !Command::new("/bin/kill")
                .args(["-0", &pid])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .unwrap()
                .success()
        );
    }
}

#[cfg(target_os = "macos")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_sample_resolves_real_store_flush_pause() {
    let name = "sync_manager::manager::tests::detector_soak::process::native_sample_resolves_real_store_flush_pause";
    if child(name, Duration::from_secs(60)) {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(NxStore::open(directory.path()).unwrap());
    store.set(b"native-sample", b"value").unwrap();
    let (reached, ready) = std::sync::mpsc::channel();
    let (resume, continue_flush) = std::sync::mpsc::channel();
    store.pause_next_flush(reached, continue_flush);
    let flushing = store.clone();
    let worker = std::thread::spawn(move || flushing.flush());
    ready.recv_timeout(Duration::from_secs(5)).unwrap();
    let mut command = Command::new("/usr/bin/sample");
    command.args([
        std::process::id().to_string(),
        "1".into(),
        "-file".into(),
        "/dev/stdout".into(),
    ]);
    let captured = native_output(command, NATIVE_COMMAND_LIMIT).await;
    resume.send(()).unwrap();
    worker.join().unwrap().unwrap();
    let captured = captured.unwrap();
    if let Some(directory) = std::env::var_os(STALL_DIR_ENV) {
        save_native_output(
            std::path::Path::new(&directory),
            "native-smoke.sample",
            &captured,
        )
        .unwrap();
    }
    let text = String::from_utf8_lossy(&captured.0);
    // Release inlines the pause helper; check its blocking receive below the real flush.
    let branch = text
        .lines()
        .skip_while(|line| !(line.contains("nx_store") && line.contains("flush_unlocked")))
        .take_while(|line| !line.contains("Thread_"))
        .take(48)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        branch.contains("flush_unlocked"),
        "Rust flush symbols unavailable"
    );
    assert!(
        branch.contains("mpmc") && branch.contains("recv"),
        "flush receive boundary not resolved"
    );
    assert!(
        branch.contains("__psynch_cvwait")
            || branch.contains("__ulock_wait")
            || branch.contains("semaphore_wait_trap"),
        "flush receive kernel wait not resolved"
    );
    println!(
        "NATIVE_SAMPLE_SMOKE bytes={} real_flush_pause_resolved=true",
        captured.0.len()
    );
}

#[derive(Clone, Copy)]
enum StorageWorkload {
    Counter,
    Metadata,
    MetadataBatched,
    RemoteHistory,
    RemoteHistoryWithoutPeriodicFlush,
}

const REMOTE_IO_ORIGINS: usize = 100;
const REMOTE_IO_NEW_OPS: usize = 37;
const REMOTE_IO_SEED: u64 = 0x4e55_4d41_5849_4f01;

struct RemoteHistoryFixture {
    prefix: Vec<nx_sync::SequencedOp>,
    envelopes: Vec<Vec<nx_sync::SequencedOp>>,
    expected: Vec<nx_sync::SequencedOp>,
    digest: String,
}

fn remote_history_fixture() -> anyhow::Result<RemoteHistoryFixture> {
    let entry = |origin: usize, sequence: u64| nx_sync::SequencedOp {
        op: Op {
            id: nx_sync::OpId::new(format!("{REMOTE_IO_SEED:016x}-{origin:03}-{sequence}")),
            origin: NodeId::new(format!("detector-{origin}")),
            kind: nx_sync::OpKind::GCounterIncrement {
                key: KEY.into(),
                increment: 1,
            },
        },
        position: nx_sync::OpPosition {
            generation: 1,
            sequence,
        },
    };
    let prefix = (0..REMOTE_IO_ORIGINS)
        .flat_map(|origin| [entry(origin, 1), entry(origin, 2)])
        .collect::<Vec<_>>();
    let new = (0..REMOTE_IO_NEW_OPS)
        .map(|origin| entry(origin, 3))
        .collect::<Vec<_>>();
    let mut envelopes = new
        .iter()
        .map(|op| vec![op.clone(), prefix[1].clone()])
        .collect::<Vec<_>>();
    envelopes.extend((0..10).map(|_| vec![prefix[1].clone(), prefix[3].clone()]));
    envelopes.extend((0..17).map(|_| vec![prefix[1].clone()]));
    ensure!(
        envelopes.len() == 64 && envelopes.iter().map(Vec::len).sum::<usize>() == 111,
        "remote history fixture dimensions changed"
    );
    let digest = blake3::hash(&serde_json::to_vec(&(&prefix, &envelopes))?)
        .to_hex()
        .to_string();
    let mut expected = prefix.clone();
    expected.extend(new);
    Ok(RemoteHistoryFixture {
        prefix,
        envelopes,
        expected,
        digest,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecordedRemoteEntry {
    id: String,
    origin: String,
    generation: u64,
    sequence: u64,
    size: u64,
}

impl RecordedRemoteEntry {
    fn parse(value: &Value) -> anyhow::Result<Self> {
        ensure!(
            value["expected_workload"] == true,
            "replay trace contains an operation outside the nominal GCounter workload"
        );
        Ok(Self {
            id: value["id"]
                .as_str()
                .context("replay trace operation ID missing")?
                .to_owned(),
            origin: value["origin"]
                .as_str()
                .context("replay trace operation origin missing")?
                .to_owned(),
            generation: number(value, "generation")?,
            sequence: number(value, "sequence")?,
            size: number(value, "size")?,
        })
    }

    fn sequenced_op(&self) -> nx_sync::SequencedOp {
        nx_sync::SequencedOp {
            op: Op {
                id: nx_sync::OpId::new(self.id.clone()),
                origin: NodeId::new(self.origin.clone()),
                kind: nx_sync::OpKind::GCounterIncrement {
                    key: KEY.into(),
                    increment: 1,
                },
            },
            position: nx_sync::OpPosition {
                generation: self.generation,
                sequence: self.sequence,
            },
        }
    }
}

struct RecordedRemoteBatch {
    at_us: u64,
    applied_ids: Vec<String>,
    duplicate_ids: Vec<String>,
    entries: Vec<RecordedRemoteEntry>,
    live_completion_us: u64,
}

struct RecordedRemoteTrace {
    batches: Vec<RecordedRemoteBatch>,
    source_window_start_us: u64,
    source_window_complete: bool,
    dropped_batches: u64,
    dropped_operations: u64,
}

fn replay_ids(value: &Value, key: &str) -> anyhow::Result<Vec<String>> {
    value
        .get(key)
        .and_then(Value::as_array)
        .with_context(|| format!("replay trace {key} missing"))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .context("replay trace operation classification contains invalid ID")
                .map(str::to_owned)
        })
        .collect()
}

fn parse_remote_replay_batches(trace: &Value) -> anyhow::Result<RecordedRemoteTrace> {
    ensure!(
        trace["complete"] == true,
        "remote replay trace is incomplete"
    );
    let source_window_complete = trace["window_complete"]
        .as_bool()
        .context("remote replay trace window completeness missing")?;
    let dropped_batches = number(trace, "dropped_batch_count")?;
    let dropped_operations = number(trace, "dropped_operation_count")?;
    ensure!(
        source_window_complete == (dropped_batches == 0 && dropped_operations == 0),
        "remote replay trace window truncation metadata is inconsistent"
    );
    let batches = trace["batches"]
        .as_array()
        .context("remote replay trace batches missing")?;
    ensure!(
        usize::try_from(number(trace, "batch_count")?)? == batches.len(),
        "remote replay trace batch count mismatch"
    );
    let mut result = Vec::with_capacity(batches.len());
    let mut operation_count = 0usize;
    let mut previous_at_us = 0;
    for batch in batches {
        let at_us = number(batch, "at_us")?;
        let applied_at_us = number(batch, "applied_at_us")?;
        ensure!(
            at_us >= previous_at_us && applied_at_us >= at_us,
            "remote replay trace timestamps are not monotonic"
        );
        previous_at_us = at_us;
        let entries = batch["entries"]
            .as_array()
            .context("remote replay trace operation entries missing")?
            .iter()
            .map(RecordedRemoteEntry::parse)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let applied_ids = replay_ids(batch, "applied_ids")?;
        let duplicate_ids = replay_ids(batch, "duplicate_ids")?;
        ensure!(
            usize::try_from(number(batch, "operations")?)? == entries.len()
                && usize::try_from(number(batch, "applied_ops")?)? == applied_ids.len()
                && usize::try_from(number(batch, "duplicate_ops")?)? == duplicate_ids.len()
                && entries.len() == applied_ids.len() + duplicate_ids.len()
                && batch["success"] == true,
            "remote replay batch counts or status are inconsistent"
        );
        operation_count = operation_count
            .checked_add(entries.len())
            .context("remote replay operation count overflow")?;
        result.push(RecordedRemoteBatch {
            at_us,
            applied_ids,
            duplicate_ids,
            entries,
            live_completion_us: applied_at_us - at_us,
        });
    }
    ensure!(
        operation_count == usize::try_from(number(trace, "operation_count")?)?,
        "remote replay trace operation count mismatch"
    );
    let source_window_start_us = result.first().map_or(0, |batch| batch.at_us);
    for batch in &mut result {
        batch.at_us = batch
            .at_us
            .checked_sub(source_window_start_us)
            .context("remote replay trace timestamp precedes retained window")?;
    }
    Ok(RecordedRemoteTrace {
        batches: result,
        source_window_start_us,
        source_window_complete,
        dropped_batches,
        dropped_operations,
    })
}

fn initial_replay_duplicates(
    batches: &[RecordedRemoteBatch],
) -> anyhow::Result<Vec<RecordedRemoteEntry>> {
    let mut applied_before = std::collections::HashSet::new();
    let mut initial = BTreeMap::<String, RecordedRemoteEntry>::new();
    let mut known_entries = BTreeMap::<String, RecordedRemoteEntry>::new();
    for batch in batches {
        for entry in &batch.entries {
            if let Some(previous) = known_entries.insert(entry.id.clone(), entry.clone()) {
                ensure!(
                    previous == *entry,
                    "replay trace reuses an operation ID with different content"
                );
            }
        }
        let applied_in_batch = batch
            .applied_ids
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        for id in &batch.duplicate_ids {
            if !applied_before.contains(id) && !applied_in_batch.contains(id) {
                let entry = known_entries
                    .get(id)
                    .with_context(|| format!("duplicate operation {id} missing from trace"))?;
                initial.entry(id.clone()).or_insert_with(|| entry.clone());
            }
        }
        applied_before.extend(batch.applied_ids.iter().cloned());
    }
    let mut entries = initial.into_values().collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        (&left.origin, left.sequence, &left.id).cmp(&(&right.origin, right.sequence, &right.id))
    });
    Ok(entries)
}

fn distribution(values: &mut [u64]) -> anyhow::Result<Value> {
    ensure!(
        !values.is_empty(),
        "cannot summarize an empty replay distribution"
    );
    values.sort_unstable();
    let percentile = |numerator: usize, denominator: usize| {
        let rank = values
            .len()
            .saturating_mul(numerator)
            .div_ceil(denominator)
            .saturating_sub(1);
        values[rank]
    };
    Ok(json!({
        "count": values.len(),
        "min": values[0],
        "median": percentile(1, 2),
        "p95": percentile(95, 100),
        "max": values[values.len() - 1],
    }))
}

async fn replay_remote_history_worker(
    index: usize,
    trace: RecordedRemoteTrace,
    directory: std::path::PathBuf,
) -> anyhow::Result<Value> {
    let store = Arc::new(NxStore::open(&directory)?);
    let manager = Arc::new(SyncManager::try_new(
        NodeId::new(format!("remote-replay-{index}")),
        SyncConfig::new(),
        store.clone(),
        metrics(),
    )?);
    let batches = trace.batches;
    let seeds = initial_replay_duplicates(&batches)?;
    if !seeds.is_empty() {
        let seed_ops = seeds
            .iter()
            .map(RecordedRemoteEntry::sequenced_op)
            .collect::<Vec<_>>();
        let applied = crate::sync_manager::apply::apply_sequenced_ops_and_collect(
            &seed_ops,
            &remote_apply_context(&manager),
        )
        .await?;
        ensure!(
            applied.len() == seed_ops.len(),
            "replay baseline contained duplicate seed operations"
        );
    }

    let started = Instant::now();
    let schedule_start = Instant::now();
    let mut apply_durations = Vec::with_capacity(batches.len());
    let mut lateness = Vec::with_capacity(batches.len());
    let mut live_completion = Vec::with_capacity(batches.len());
    let mut applied_count = 0usize;
    let mut duplicate_count = 0usize;
    let mut observed_applied = std::collections::HashSet::new();
    for batch in &batches {
        let deadline = schedule_start + Duration::from_micros(batch.at_us);
        tokio::time::sleep_until(deadline).await;
        let ops = batch
            .entries
            .iter()
            .map(RecordedRemoteEntry::sequenced_op)
            .collect::<Vec<_>>();
        for (entry, op) in batch.entries.iter().zip(&ops) {
            ensure!(
                crate::sync_manager::delivery::entry_bytes(&op.op, &op.position)
                    == usize::try_from(entry.size).ok(),
                "replayed operation {} does not preserve its recorded byte size",
                entry.id
            );
        }
        let apply_started = Instant::now();
        let applied = crate::sync_manager::apply::apply_sequenced_ops_and_collect(
            &ops,
            &remote_apply_context(&manager),
        )
        .await?;
        let elapsed_us = u64::try_from(apply_started.elapsed().as_micros())?;
        let applied_ids = applied
            .iter()
            .map(|entry| entry.op.id.as_str().to_owned())
            .collect::<Vec<_>>();
        let mut remaining_applied = applied_ids
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        let mut actual_applied_ids = Vec::with_capacity(applied_ids.len());
        let mut actual_duplicate_ids = Vec::with_capacity(ops.len() - applied_ids.len());
        for entry in &batch.entries {
            if remaining_applied.remove(&entry.id) {
                actual_applied_ids.push(entry.id.clone());
            } else {
                actual_duplicate_ids.push(entry.id.clone());
            }
        }
        ensure!(
            actual_applied_ids == batch.applied_ids && actual_duplicate_ids == batch.duplicate_ids,
            "replay changed applied/duplicate classification for worker {index}"
        );
        observed_applied.extend(actual_applied_ids.iter().cloned());
        applied_count = applied_count
            .checked_add(actual_applied_ids.len())
            .context("replay applied operation count overflow")?;
        duplicate_count = duplicate_count
            .checked_add(actual_duplicate_ids.len())
            .context("replay duplicate operation count overflow")?;
        apply_durations.push(elapsed_us);
        lateness.push(u64::try_from(
            Instant::now()
                .saturating_duration_since(deadline)
                .as_micros(),
        )?);
        live_completion.push(batch.live_completion_us);
    }
    let elapsed_us = u64::try_from(started.elapsed().as_micros())?;
    let expected_value = u64::try_from(seeds.len() + observed_applied.len())?;
    ensure!(
        manager.get_counter_value(KEY).await == expected_value,
        "remote replay in-memory counter mismatch for worker {index}"
    );
    drop(manager);
    drop(store);

    let reopened_store = Arc::new(NxStore::open(&directory)?);
    let reopened = SyncManager::try_new(
        NodeId::new(format!("remote-replay-{index}")),
        SyncConfig::new(),
        reopened_store,
        metrics(),
    )?;
    ensure!(
        reopened.get_counter_value(KEY).await == expected_value,
        "remote replay durable counter mismatch for worker {index}"
    );
    Ok(json!({
        "index": index,
        "batches": batches.len(),
        "input_operations": applied_count + duplicate_count,
        "applied_operations": applied_count,
        "duplicate_operations": duplicate_count,
        "initial_duplicate_seeds": seeds.len(),
        "source_window_complete": trace.source_window_complete,
        "source_window_start_us": trace.source_window_start_us,
        "dropped_batches": trace.dropped_batches,
        "dropped_operations": trace.dropped_operations,
        "counter_after_reopen": expected_value,
        "elapsed_us": elapsed_us,
        "batch_apply_us": apply_durations,
        "batch_lateness_us": lateness,
        "live_batch_to_apply_completion_us": live_completion,
    }))
}

pub(super) async fn replay_remote_history_trace(
    trace_path: &std::path::Path,
    result_path: &std::path::Path,
) -> anyhow::Result<Value> {
    let trace_bytes = std::fs::read(trace_path)
        .with_context(|| format!("cannot read replay trace {}", trace_path.display()))?;
    let source_trace: Value = serde_json::from_slice(&trace_bytes)?;
    ensure!(
        source_trace["version"] == 1,
        "unsupported remote replay trace"
    );
    let workers = source_trace["workers"]
        .as_array()
        .context("remote replay trace workers missing")?;
    ensure!(
        !workers.is_empty() && workers.len() <= 100,
        "remote replay worker count outside supported bounds"
    );
    let directory = tempfile::tempdir()?;
    let started = StdInstant::now();
    let mut tasks = JoinSet::new();
    let mut seen_indices = std::collections::HashSet::new();
    let mut total_batches = 0usize;
    let mut total_operations = 0usize;
    let mut total_dropped_batches = 0u64;
    let mut total_dropped_operations = 0u64;
    let mut all_windows_complete = true;
    let mut live_batch_to_apply = Vec::new();
    for worker in workers {
        let index = usize::try_from(number(worker, "index")?)?;
        ensure!(
            seen_indices.insert(index),
            "duplicate remote replay worker index"
        );
        let trace = parse_remote_replay_batches(&worker["trace"])?;
        ensure!(
            trace.batches.len() <= 512
                && trace
                    .batches
                    .iter()
                    .map(|batch| batch.entries.len())
                    .sum::<usize>()
                    <= 4096,
            "remote replay trace exceeds per-worker bounds"
        );
        for batch in &trace.batches {
            live_batch_to_apply.push(batch.live_completion_us);
            total_operations = total_operations
                .checked_add(batch.entries.len())
                .context("remote replay total operation count overflow")?;
        }
        total_batches = total_batches
            .checked_add(trace.batches.len())
            .context("remote replay total batch count overflow")?;
        total_dropped_batches = total_dropped_batches
            .checked_add(trace.dropped_batches)
            .context("remote replay dropped batch count overflow")?;
        total_dropped_operations = total_dropped_operations
            .checked_add(trace.dropped_operations)
            .context("remote replay dropped operation count overflow")?;
        all_windows_complete &= trace.source_window_complete;
        let worker_directory = directory.path().join(format!("worker-{index}"));
        std::fs::create_dir(&worker_directory)?;
        tasks.spawn(replay_remote_history_worker(index, trace, worker_directory));
    }
    ensure!(
        total_operations > 0,
        "remote replay trace has no operations"
    );
    let mut worker_results = Vec::with_capacity(workers.len());
    while let Some(result) = tasks.join_next().await {
        worker_results.push(result??);
    }
    worker_results.sort_by_key(|worker| worker["index"].as_u64().unwrap_or(u64::MAX));
    let mut replay_batch_apply = worker_results
        .iter()
        .flat_map(|worker| worker["batch_apply_us"].as_array().into_iter().flatten())
        .map(|value| value.as_u64().context("invalid replay apply duration"))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut replay_batch_lateness = worker_results
        .iter()
        .flat_map(|worker| worker["batch_lateness_us"].as_array().into_iter().flatten())
        .map(|value| value.as_u64().context("invalid replay lateness"))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let report = json!({
        "version": 1,
        "experiment": "captured remote GCounter apply batches replayed through the real persistence path without networking",
        "source_trace_blake3": blake3::hash(&trace_bytes).to_hex().to_string(),
        "workers": worker_results,
        "worker_count": workers.len(),
        "batch_count": total_batches,
        "operation_count": total_operations,
        "source_windows_complete": all_windows_complete,
        "dropped_batch_count": total_dropped_batches,
        "dropped_operation_count": total_dropped_operations,
        "replay_elapsed_us": u64::try_from(started.elapsed().as_micros())?,
        "live_batch_to_apply_completion_us": distribution(&mut live_batch_to_apply)?,
        "replay_batch_apply_us": distribution(&mut replay_batch_apply)?,
        "replay_batch_lateness_us": distribution(&mut replay_batch_lateness)?,
        "interpretation": "reuses captured operation IDs, origins, positions, payload kind, per-entry byte sizes, duplicate classification, and batch offsets; excludes TCP, detector probes, connection queues, and event arrival before batch collection; each worker is a concurrent store in this process",
    });
    let bytes = serde_json::to_vec(&report)?;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(result_path)
        .with_context(|| format!("cannot create replay report {}", result_path.display()))?;
    file.write_all(&bytes)
        .with_context(|| format!("cannot write replay report {}", result_path.display()))?;
    Ok(report)
}

#[tokio::test]
async fn captured_remote_history_replay_preserves_duplicates_and_durability() -> anyhow::Result<()>
{
    let directory = tempfile::tempdir()?;
    let trace_path = directory.path().join("trace.json");
    let result_path = directory.path().join("result.json");
    let entry = |id: &str, origin: &str, sequence| {
        let recorded = RecordedRemoteEntry {
            id: id.into(),
            origin: origin.into(),
            generation: 1,
            sequence,
            size: 0,
        };
        let op = recorded.sequenced_op();
        json!({
            "id": recorded.id,
            "origin": recorded.origin,
            "generation": recorded.generation,
            "sequence": recorded.sequence,
            "size": crate::sync_manager::delivery::entry_bytes(&op.op, &op.position).unwrap(),
            "expected_workload": true,
        })
    };
    let old = entry("old-op", "origin-1", 1);
    let new = entry("new-op", "origin-1", 2);
    let trace = json!({
        "version": 1,
        "workers": [{
            "index": 0,
            "trace": {
                "version": 1,
                "complete": true,
                "window_complete": true,
                "dropped_batch_count": 0,
                "dropped_operation_count": 0,
                "operation_count": 3,
                "batch_count": 2,
                "batches": [
                    {
                        "at_us": 0,
                        "applied_at_us": 10,
                        "operations": 1,
                        "entries": [old],
                        "applied_ops": 0,
                        "duplicate_ops": 1,
                        "applied_ids": [],
                        "duplicate_ids": ["old-op"],
                        "success": true,
                    },
                    {
                        "at_us": 100,
                        "applied_at_us": 120,
                        "operations": 2,
                        "entries": [old, new],
                        "applied_ops": 1,
                        "duplicate_ops": 1,
                        "applied_ids": ["new-op"],
                        "duplicate_ids": ["old-op"],
                        "success": true,
                    },
                ],
            },
        }],
    });
    std::fs::write(&trace_path, serde_json::to_vec(&trace)?)?;

    let report = replay_remote_history_trace(&trace_path, &result_path).await?;

    assert_eq!(report["worker_count"], 1);
    assert_eq!(report["batch_count"], 2);
    assert_eq!(report["operation_count"], 3);
    assert_eq!(report["workers"][0]["initial_duplicate_seeds"], 1);
    assert_eq!(report["workers"][0]["applied_operations"], 1);
    assert_eq!(report["workers"][0]["duplicate_operations"], 2);
    assert_eq!(report["workers"][0]["counter_after_reopen"], 2);
    assert!(result_path.is_file());
    Ok(())
}

fn remote_history_context(store: Arc<NxStore>) -> NodeEventContext {
    test_event_context(
        Arc::new(RwLock::new(HashMap::new())),
        Arc::new(RwLock::new(SeenOps::new(1024))),
        Arc::new(RwLock::new(Vec::new())),
        store,
        metrics(),
        Arc::new(RwLock::new(HashMap::new())),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn critical_baseline_export_identifies_reader_waiting_for_store_lease() -> anyhow::Result<()>
{
    let name = "sync_manager::manager::tests::detector_soak::process::critical_baseline_export_identifies_reader_waiting_for_store_lease";
    if child(name, Duration::from_secs(120)) {
        return Ok(());
    }
    let diagnostics = diagnostics::Diagnostics::with_critical_path()?;
    diagnostics.install()?;
    let directory = tempfile::tempdir()?;
    let store = Arc::new(NxStore::open(directory.path())?);
    let node = Arc::new(SyncManager::try_new(
        NodeId::new("baseline-reader"),
        SyncConfig::new(),
        store.clone(),
        metrics(),
    )?);
    let fixture = remote_history_fixture()?;
    crate::sync_manager::apply::apply_sequenced_ops(&fixture.prefix, &remote_apply_context(&node))
        .await?;
    let (ready, reached) = std::sync::mpsc::channel();
    let (resume, release) = std::sync::mpsc::channel();
    let blocked_store = store.clone();
    let holder = std::thread::spawn(move || -> anyhow::Result<()> {
        let _lease = blocked_store.acquire_write_lease()?;
        ready.send(())?;
        release.recv()?;
        Ok(())
    });
    reached.recv_timeout(Duration::from_secs(5))?;
    let exporting_node = node.clone();
    let exporting = tokio::spawn(async move {
        crate::sync_manager::baseline::export(&remote_apply_context(&exporting_node)).await
    });
    let before = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = diagnostics
                .critical_snapshot(true)?
                .context("critical recorder inactive")?;
            if snapshot["active"]
                .as_array()
                .context("active spans missing")?
                .iter()
                .any(|span| {
                    span["job_kind"] == "baseline_export"
                        && span["span_kind"] == "phase"
                        && span["resource"] == "lease_wait"
                })
            {
                break anyhow::Ok(snapshot);
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    let handle = node.handle();
    let mut local = Box::pin(handle.submit_gcounter_increment(KEY, 1));
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(local.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    let queued = diagnostics
        .critical_snapshot(true)?
        .context("critical recorder inactive")?;
    let active = queued["active"]
        .as_array()
        .context("active spans missing")?;
    let reader = active
        .iter()
        .find(|span| {
            span["job_kind"] == "baseline_export"
                && span["span_kind"] == "hold"
                && span["resource"] == "seen"
        })
        .context("baseline read owner missing")?;
    let reader_id = reader["job_id"].clone();
    ensure!(
        active.iter().any(|span| span["job_kind"] == "local"
            && span["span_kind"] == "wait"
            && span["resource"] == "seen"),
        "local seen wait missing"
    );
    ensure!(
        store.active_flush().is_none(),
        "lease wait incorrectly reported as foreground flush"
    );
    ensure!(
        node.seen_ops.try_write().is_err(),
        "baseline reader released its cut early"
    );
    resume.send(())?;
    holder
        .join()
        .map_err(|_| anyhow::anyhow!("lease holder panicked"))??;
    let payload = tokio::time::timeout(Duration::from_secs(5), exporting).await???;
    tokio::time::timeout(Duration::from_secs(5), local.as_mut()).await??;
    ensure!(
        node.get_counter_value(KEY).await == 201
            && read_materialized(&store, KEY) == 201
            && read_durable_gcounter_state(&store, KEY).value() == 201,
        "queued local write did not become durable"
    );
    let after = diagnostics
        .critical_snapshot(true)?
        .context("critical recorder inactive")?;
    ensure!(
        after["completed"]
            .as_array()
            .context("completed spans missing")?
            .iter()
            .any(|span| span["job_id"] == reader_id
                && span["resource"] == "seen"
                && span["span_kind"] == "hold"
                && span["release_confirmed"] == true
                && span["status"] == "success"),
        "baseline release not confirmed"
    );
    // Exercise the real installer without changing its complete-cut/confirmation contract.
    let receiving_store = temp_store();
    let receiving = SyncManager::try_new(
        NodeId::new("baseline-receiver"),
        SyncConfig::new(),
        receiving_store.clone(),
        metrics(),
    )?;
    crate::sync_manager::baseline::import(
        crate::sync_manager::baseline::VERSION,
        &payload,
        blake3::hash(&payload).as_bytes(),
        &fixture.prefix[0].op.origin,
        &remote_apply_context(&receiving),
    )
    .await?;
    ensure!(
        receiving.get_counter_value(KEY).await == 200
            && read_durable_gcounter_state(&receiving_store, KEY).value() == 200,
        "baseline cut included a later local write"
    );
    let installed = diagnostics
        .critical_snapshot(true)?
        .context("critical recorder inactive")?;
    ensure!(
        installed["completed"]
            .as_array()
            .context("completed spans missing")?
            .iter()
            .any(|span| span["job_kind"] == "baseline_install"
                && span["span_kind"] == "hold"
                && span["resource"] == "seen"
                && span["release_confirmed"] == true),
        "installer write owner not traced"
    );
    println!(
        "CRITICAL_BASELINE_OWNER_REGRESSION {}",
        json!({
            "reader_job": reader_id, "queued_without_foreground_flush": true,
            "source_capture_us": before["captured_us"], "export_cut": 200, "local_durable": 201,
        })
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn critical_path_tracks_real_remote_holder_and_owned_job_lifetime() -> anyhow::Result<()> {
    let name = "sync_manager::manager::tests::detector_soak::process::critical_path_tracks_real_remote_holder_and_owned_job_lifetime";
    if child(name, Duration::from_secs(120)) {
        return Ok(());
    }
    let diagnostics = diagnostics::Diagnostics::with_critical_path()?;
    diagnostics.install()?;
    let fixture = remote_history_fixture()?;
    for (cancel_remote, cancel_local) in [(false, false), (true, false), (false, true)] {
        let directory = tempfile::tempdir()?;
        let store = Arc::new(NxStore::open(directory.path())?);
        let node = Arc::new(SyncManager::try_new(
            NodeId::new("critical-local"),
            SyncConfig::new(),
            store.clone(),
            metrics(),
        )?);
        crate::sync_manager::apply::apply_sequenced_ops(
            &fixture.prefix,
            &remote_apply_context(&node),
        )
        .await?;
        verify_remote_history_store(&store, &fixture.prefix)?;
        let (reached, ready) = std::sync::mpsc::channel();
        let (resume, proceed) = std::sync::mpsc::channel();
        store.pause_next_flush(reached, proceed);
        let remote_node = node.clone();
        let ops = fixture
            .envelopes
            .iter()
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        let mut remote = Some(tokio::spawn(async move {
            crate::sync_manager::apply::apply_sequenced_ops(
                &ops,
                &remote_apply_context(&remote_node),
            )
            .await
        }));
        tokio::task::spawn_blocking(move || ready.recv_timeout(Duration::from_secs(5))).await??;
        let handle = node.handle();
        let mut local = Some(Box::pin(handle.submit_gcounter_increment(KEY, 1)));
        std::future::poll_fn(|cx| {
            assert!(
                std::future::Future::poll(local.as_mut().unwrap().as_mut(), cx).is_pending(),
                "local writer did not queue behind the real remote holder"
            );
            std::task::Poll::Ready(())
        })
        .await;
        let before = diagnostics
            .critical_snapshot(true)?
            .context("critical recorder inactive")?;
        let active = before["active"]
            .as_array()
            .context("critical active spans missing")?;
        let remote_id = active
            .iter()
            .find(|span| {
                span["job_kind"] == "remote"
                    && span["span_kind"] == "phase"
                    && span["resource"] == "flush"
            })
            .context("real remote covering flush not traced")?["job_id"]
            .clone();
        let local_id = active
            .iter()
            .find(|span| {
                span["job_kind"] == "local"
                    && span["span_kind"] == "wait"
                    && span["resource"] == "seen"
            })
            .context("queued local seen wait not traced")?["job_id"]
            .clone();
        ensure!(
            active.iter().any(|span| span["job_id"] == remote_id
                && span["span_kind"] == "hold"
                && span["resource"] == "seen"),
            "remote seen holder missing"
        );
        ensure!(
            node.counters.try_read().is_err(),
            "paused flush released publication registry early"
        );
        if cancel_remote {
            let remote = remote.take().context("remote caller handle missing")?;
            remote.abort();
            ensure!(
                remote.await.unwrap_err().is_cancelled(),
                "remote caller did not cancel"
            );
            let snapshot = diagnostics
                .critical_snapshot(true)?
                .context("critical recorder inactive")?;
            ensure!(
                snapshot["active"]
                    .as_array()
                    .context("active spans missing")?
                    .iter()
                    .any(|span| span["job_id"] == remote_id && span["resource"] == "flush"),
                "caller cancellation falsely ended its owned storage job"
            );
        }
        if cancel_local {
            drop(local.take());
        }
        resume.send(()).context("flush pause receiver closed")?;
        if let Some(remote) = remote {
            let result = tokio::time::timeout(Duration::from_secs(5), remote).await??;
            result?;
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let snapshot = diagnostics
                    .critical_snapshot(true)?
                    .context("critical recorder inactive")?;
                if snapshot["completed"]
                    .as_array()
                    .context("completed spans missing")?
                    .iter()
                    .any(|span| span["job_id"] == remote_id && span["span_kind"] == "job")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
            anyhow::Ok(())
        })
        .await??;
        if let Some(local) = &mut local {
            let result = tokio::time::timeout(Duration::from_secs(5), local.as_mut()).await?;
            result?;
        }
        let after = diagnostics
            .critical_snapshot(true)?
            .context("critical recorder inactive")?;
        let completed = after["completed"]
            .as_array()
            .context("critical completed spans missing")?;
        let held = completed
            .iter()
            .find(|span| {
                span["job_id"] == remote_id
                    && span["span_kind"] == "hold"
                    && span["resource"] == "seen"
            })
            .context("remote holder release missing")?;
        ensure!(
            held["release_confirmed"] == true
                && number(held, "release_begin_us")? <= number(held, "end_us")?,
            "owned callback did not confirm its release interval"
        );
        ensure!(held["status"] == "success", "remote outcome misreported");
        if cancel_local {
            let partial = completed
                .iter()
                .find(|span| {
                    span["job_id"] == local_id
                        && span["span_kind"] == "hold"
                        && span["resource"] == "admission"
                })
                .context("cancelled admission hold missing")?;
            ensure!(
                partial["status"] == "cancelled" && partial["release_confirmed"] == false,
                "pre-dispatch cancellation falsely certified release timing"
            );
        } else {
            let wait = completed
                .iter()
                .find(|span| {
                    span["job_id"] == local_id
                        && span["span_kind"] == "wait"
                        && span["resource"] == "seen"
                })
                .context("completed local wait missing")?;
            ensure!(
                number(wait, "start_us")? < number(held, "release_begin_us")?
                    && number(held, "end_us")? <= number(wait, "end_us")?,
                "source timestamps do not correlate the actual holder and waiter"
            );
        }
        let expected = if cancel_local { 237 } else { 238 };
        ensure!(
            node.get_counter_value(KEY).await == expected,
            "publication result differs from storage outcome"
        );
        ensure!(
            read_materialized(&store, KEY) == expected
                && read_durable_gcounter_state(&store, KEY).value() == expected,
            "durable state differs"
        );
        println!(
            "CRITICAL_PATH_REGRESSION {}",
            json!({
                "cancel_remote": cancel_remote, "fail_flush": false, "cancel_local": cancel_local,
                "remote_job": remote_id, "local_job": local_id, "state": expected,
            })
        );
    }
    let directory = tempfile::tempdir()?;
    let store = Arc::new(NxStore::open(directory.path())?);
    let node = SyncManager::try_new(
        NodeId::new("critical-error"),
        SyncConfig::new(),
        store.clone(),
        metrics(),
    )?;
    crate::sync_manager::apply::apply_sequenced_ops(&fixture.prefix, &remote_apply_context(&node))
        .await?;
    let start = diagnostics
        .critical_snapshot(false)?
        .context("critical recorder inactive")?;
    // The existing injection is checked before the pause hook, so arm it
    // before this separate error attempt rather than after a paused flush.
    store.inject_disk_full_on_flush(true);
    let ops = fixture
        .envelopes
        .iter()
        .flatten()
        .cloned()
        .collect::<Vec<_>>();
    ensure!(
        crate::sync_manager::apply::apply_sequenced_ops(&ops, &remote_apply_context(&node))
            .await
            .is_err(),
        "remote flush failure was accepted"
    );
    ensure!(
        node.handle()
            .submit_gcounter_increment(KEY, 1)
            .await
            .is_err(),
        "uncertain durability was accepted locally"
    );
    ensure!(
        node.get_counter_value(KEY).await == 200,
        "failed flush published state"
    );
    let snapshot = diagnostics
        .critical_snapshot(true)?
        .context("critical recorder inactive")?;
    let completed = snapshot["completed"]
        .as_array()
        .context("completed spans missing")?;
    let failed = completed
        .iter()
        .find(|span| {
            span["job_kind"] == "remote"
                && span["span_kind"] == "job"
                && span["status"] == "error"
                && span["start_us"]
                    .as_u64()
                    .is_some_and(|time| time >= start["captured_us"].as_u64().unwrap())
        })
        .context("failed remote owned job missing")?;
    ensure!(
        completed
            .iter()
            .any(|span| span["job_id"] == failed["job_id"]
                && span["span_kind"] == "phase"
                && span["resource"] == "flush"
                && span["status"] == "error"),
        "failed flush phase missing"
    );
    ensure!(
        completed
            .iter()
            .any(|span| span["job_id"] == failed["job_id"]
                && span["span_kind"] == "hold"
                && span["resource"] == "seen"
                && span["release_confirmed"] == true
                && span["status"] == "error"),
        "error release missing"
    );
    println!(
        "CRITICAL_PATH_REGRESSION {}",
        json!({"fail_flush": true, "state": 200, "owned_job": failed["job_id"]})
    );
    Ok(())
}

async fn apply_profile_envelopes(
    context: &NodeEventContext,
    envelopes: &[Vec<nx_sync::SequencedOp>],
) -> anyhow::Result<()> {
    ensure!(
        !envelopes.is_empty() && envelopes.len() <= 64,
        "invalid remote profile envelope count"
    );
    let (sender, mut events) = mpsc::channel(64);
    for ops in envelopes {
        sender
            .try_send(NodeEvent::HistoryReceived {
                from: NodeId::new("remote-io-source"),
                addr: "remote-io-source".into(),
                message: nx_sync::HistoryMessage::Push { ops: ops.clone() },
            })
            .context("remote profile event queue rejected input")?;
    }
    drop(sender);
    let first = events
        .recv()
        .await
        .context("remote profile input missing")?;
    let pending = handle_node_events(
        first,
        &mut events,
        &mut None,
        false,
        context,
        Some(REMOTE_HISTORY_COALESCE_WINDOW),
    )
    .await;
    ensure!(
        pending.is_none()
            && matches!(
                events.try_recv(),
                Err(mpsc::error::TryRecvError::Disconnected)
            ),
        "remote profile did not consume exactly the supplied envelopes"
    );
    Ok(())
}

fn verify_remote_history_store(
    store: &NxStore,
    expected: &[nx_sync::SequencedOp],
) -> anyhow::Result<()> {
    let target = u64::try_from(expected.len())?;
    let bytes = store
        .get(&materialized_gcounter_key(KEY))?
        .context("remote profile materialized counter missing")?;
    ensure!(
        u64::from_le_bytes(
            bytes.try_into().map_err(|_| anyhow::anyhow!(
                "invalid remote profile materialized counter length"
            ))?
        ) == target,
        "remote profile materialized counter mismatch"
    );
    let state = store
        .get(&durable_gcounter_state_key(KEY))?
        .context("remote profile durable counter missing")?;
    ensure!(
        parse_durable_gcounter_state(&state)?.value() == target,
        "remote profile durable counter mismatch"
    );
    let expected_ids = expected
        .iter()
        .map(|entry| entry.op.id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let (seen, seen_sequence) = hydrate_seen_ops(store, 1024)?;
    let (log, log_sequence) = hydrate_op_log(store, 1024)?;
    ensure!(
        seen_sequence == target
            && log_sequence == target
            && seen.ids.len() == expected.len()
            && log.len() == expected.len()
            && log
                .iter()
                .map(|op| op.id.as_str())
                .collect::<std::collections::BTreeSet<_>>()
                == expected_ids,
        "remote profile replay/dedup metadata mismatch"
    );
    for entry in expected {
        ensure!(
            store.exists(&seen_op_store_key(entry.op.id.as_str()))?,
            "remote profile lost a seen operation"
        );
        ensure!(
            log.iter().find(|op| op.id == entry.op.id) == Some(&entry.op)
                && crate::sync_manager::storage::sequenced_op(store, &entry.op)?.as_ref()
                    == Some(entry),
            "remote profile retained operation identity or payload mismatch"
        );
    }
    let mut sequences = BTreeMap::<&str, u64>::new();
    for entry in expected {
        sequences
            .entry(entry.op.origin.as_str())
            .and_modify(|sequence| *sequence = (*sequence).max(entry.position.sequence))
            .or_insert(entry.position.sequence);
    }
    let progress = replication_progress(store)?;
    ensure!(
        progress.len() == sequences.len(),
        "remote profile receipt origin count mismatch"
    );
    for receipt in progress {
        let sequence = *sequences
            .get(receipt.origin.as_str())
            .context("unexpected remote profile receipt origin")?;
        ensure!(
            receipt.contiguous_sequence == sequence
                && receipt.known_high_sequence == sequence
                && receipt.sparse_positions == 0
                && !receipt.source_unavailable
                && !receipt.baseline_missing
                && !receipt.baseline_unconfirmed
                && receipt.recovery_failure.is_none(),
            "remote profile incomplete receipt for {}",
            receipt.origin
        );
    }
    Ok(())
}

async fn remote_history_work(
    context: &NodeEventContext,
    fixture: &RemoteHistoryFixture,
    diagnostics: &diagnostics::Diagnostics,
) -> anyhow::Result<Value> {
    let before = context.store.completed_batch_work();
    let flushes = context.store.completed_flushes();
    diagnostics.timeline_phase("remote_io")?;
    let started = StdInstant::now();
    apply_profile_envelopes(context, &fixture.envelopes).await?;
    let elapsed = started.elapsed().as_secs_f64();
    diagnostics.timeline_phase("remote_io_complete")?;
    let after = context.store.completed_batch_work();
    let batches = after
        .0
        .checked_sub(before.0)
        .context("batch counter regressed")?;
    let bytes = after
        .1
        .checked_sub(before.1)
        .context("batch byte counter regressed")?;
    let flushes = context
        .store
        .completed_flushes()
        .checked_sub(flushes)
        .context("flush counter regressed")?;
    ensure!(
        flushes == 1,
        "remote profile covering flushes: expected 1, got {flushes}"
    );
    ensure!(
        batches == 1 && bytes > 0,
        "remote profile persistence work changed"
    );
    verify_remote_history_store(&context.store, &fixture.expected)?;
    ensure!(
        context
            .counters
            .read()
            .await
            .get(KEY)
            .context("remote profile counter missing")?
            .value()
            == u64::try_from(fixture.expected.len())?,
        "remote profile registry counter mismatch"
    );
    Ok(json!({
        "elapsed_s": elapsed, "batches": batches, "logical_batch_bytes": bytes,
        "flushes": flushes, "input_digest": fixture.digest, "new_ops": REMOTE_IO_NEW_OPS,
        "durable_ops": fixture.expected.len(), "origins": REMOTE_IO_ORIGINS,
    }))
}

fn remote_profile_timings(path: &std::path::Path) -> anyhow::Result<Value> {
    let snapshot: Value = serde_json::from_slice(&std::fs::read(path)?)?;
    ensure!(
        snapshot["complete"] == true && number(&snapshot, "dropped")? == 0,
        "remote profile timeline incomplete"
    );
    let events = snapshot["events"]
        .as_array()
        .context("remote profile events missing")?;
    let phase = |name| -> anyhow::Result<u64> {
        let matches = events
            .iter()
            .filter(|event| event["phase"] == name)
            .collect::<Vec<_>>();
        ensure!(matches.len() == 1, "remote profile phase {name} not unique");
        number(matches[0], "captured_us")
    };
    let begin = phase("remote_io")?;
    let end = phase("remote_io_complete")?;
    let mut samples = BTreeMap::<String, Vec<u64>>::new();
    for event in events {
        let captured = number(event, "captured_us")?;
        if captured < begin || captured > end {
            continue;
        }
        if let Some(
            stage @ ("registry_wait" | "storage_dispatch" | "preparation" | "batch" | "flush"
            | "publication" | "push_summary"),
        ) = event["recovery_trace_stage"].as_str()
        {
            ensure!(
                event["success"] != false,
                "remote profile stage {stage} failed"
            );
            samples
                .entry(stage.into())
                .or_default()
                .push(number(event, "elapsed_us")?);
        }
    }
    ensure!(
        samples.get("flush").map(Vec::len) == Some(1),
        "remote profile did not capture the single covering flush duration"
    );
    Ok(serde_json::to_value(samples)?)
}

async fn remote_history_worker(
    directory: &str,
    store: Arc<NxStore>,
    trace_path: &std::path::Path,
) -> anyhow::Result<()> {
    let diagnostics = diagnostics::Diagnostics::with_timeline(trace_path)?;
    diagnostics.install()?;
    let fixture = remote_history_fixture()?;
    let context = remote_history_context(store.clone());
    let (mut commands, input) = command_input();
    let result = async {
        apply_profile_envelopes(&context, std::slice::from_ref(&fixture.prefix)).await?;
        verify_remote_history_store(&store, &fixture.prefix)?;
        emit(json!({"phase": "ready", "input_digest": fixture.digest,
            "periodic_flush_ms": store.periodic_flush_interval_ms()}));
        let command = commands
            .recv()
            .await
            .context("remote profile coordinator closed input")??;
        ensure!(
            command["command"] == "remote-io",
            "remote profile expected start command"
        );
        let wait = number(&command, "start_epoch_ms")?
            .checked_sub(epoch_ms()?)
            .context("remote profile start command arrived late")?;
        emit(json!({"phase": "remote-armed"}));
        tokio::time::sleep(Duration::from_millis(wait)).await;
        let work = remote_history_work(&context, &fixture, &diagnostics).await?;
        emit(json!({"phase": "remote-processed", "work": work}));
        let command = commands
            .recv()
            .await
            .context("remote profile coordinator closed input")??;
        ensure!(
            command["command"] == "report",
            "remote profile expected report barrier"
        );
        diagnostics.timeline_phase("measurement_end")?;
        diagnostics.save_timeline()?;
        emit(json!({"phase": "remote-report", "timings_us": remote_profile_timings(trace_path)?}));
        let command = commands
            .recv()
            .await
            .context("remote profile coordinator closed input")??;
        ensure!(
            command["command"] == "stop",
            "remote profile expected stop command"
        );
        anyhow::Ok(())
    }
    .await;
    diagnostics.save_timeline()?;
    drop(context);
    drop(store);
    if result.is_ok() {
        input
            .join()
            .map_err(|_| anyhow::anyhow!("remote profile input reader panicked"))?;
        let reopened = NxStore::open(directory)?;
        verify_remote_history_store(&reopened, &fixture.expected)?;
    }
    result
}

#[tokio::test]
async fn remote_history_benign_duplicates_share_one_flush() {
    let context = remote_history_context(temp_store());
    let reference = remote_history_context(temp_store());
    let fixture = remote_history_fixture().unwrap();
    for target in [&context, &reference] {
        apply_profile_envelopes(target, std::slice::from_ref(&fixture.prefix))
            .await
            .unwrap();
    }
    let before = context.store.completed_batch_work();
    let before_flushes = context.store.completed_flushes();
    let reference_before = reference.store.completed_batch_work();
    let reference_flushes = reference.store.completed_flushes();

    apply_profile_envelopes(&context, &fixture.envelopes)
        .await
        .unwrap();
    for envelope in &fixture.envelopes {
        apply_profile_envelopes(&reference, std::slice::from_ref(envelope))
            .await
            .unwrap();
    }

    verify_remote_history_store(&context.store, &fixture.expected).unwrap();
    verify_remote_history_store(&reference.store, &fixture.expected).unwrap();
    assert_eq!(context.counters.read().await[KEY].value(), 237);
    assert_eq!(context.store.completed_flushes() - before_flushes, 1);
    let after = context.store.completed_batch_work();
    let reference_after = reference.store.completed_batch_work();
    assert_eq!(after.0 - before.0, 1);
    assert_eq!(reference_after.0 - reference_before.0, 37);
    assert_eq!(reference.store.completed_flushes() - reference_flushes, 37);
    println!(
        "REMOTE_HISTORY_DUPLICATE_REGRESSION {}",
        json!({
            "input_digest": fixture.digest, "envelopes": 64, "input_entries": 111,
            "new_ops": 37, "persisted_copies": 74, "durable_ops": 237,
            "grouped_batches": after.0 - before.0,
            "grouped_flushes": context.store.completed_flushes() - before_flushes,
            "grouped_logical_bytes": after.1 - before.1,
            "individual_batches": reference_after.0 - reference_before.0,
            "individual_flushes": reference.store.completed_flushes() - reference_flushes,
            "individual_logical_bytes": reference_after.1 - reference_before.1,
        })
    );
}

#[tokio::test]
async fn remote_history_fixture_preserves_exact_work_and_restart() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(NxStore::open(directory.path()).unwrap());
    let context = remote_history_context(store.clone());
    let fixture = remote_history_fixture().unwrap();
    assert_eq!(
        fixture.digest,
        "bf1baaa4554b1231b12cecd015ebd76b71868d067858ff6080fd39b804d14cbe"
    );
    apply_profile_envelopes(&context, std::slice::from_ref(&fixture.prefix))
        .await
        .unwrap();
    verify_remote_history_store(&store, &fixture.prefix).unwrap();
    let work = remote_history_work(&context, &fixture, &diagnostics::Diagnostics::default())
        .await
        .unwrap();
    assert_eq!(work["flushes"], 1);
    assert_eq!(work["durable_ops"], 237);
    drop(context);
    drop(store);
    let reopened = NxStore::open(directory.path()).unwrap();
    verify_remote_history_store(&reopened, &fixture.expected).unwrap();
}

#[tokio::test]
async fn remote_history_fixture_reports_storage_failure() {
    let context = remote_history_context(temp_store());
    let fixture = remote_history_fixture().unwrap();
    apply_profile_envelopes(&context, std::slice::from_ref(&fixture.prefix))
        .await
        .unwrap();
    context.store.inject_disk_full_on_flush(true);
    let error = remote_history_work(&context, &fixture, &diagnostics::Diagnostics::default())
        .await
        .expect_err("failed persistence was reported as success");
    assert!(error.to_string().contains("covering flushes"));
    assert_eq!(context.counters.read().await[KEY].value(), 200);
}

pub(super) fn number(value: &Value, key: &str) -> anyhow::Result<u64> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .with_context(|| format!("missing worker field {key}"))
}

fn emit(value: Value) {
    let line = format!("{PREFIX}{value}");
    assert!(
        line.len().saturating_add(1) <= MAX_WORKER_OUTPUT_LINE_BYTES,
        "worker diagnostic line exceeds 64 KiB: {} bytes",
        line.len().saturating_add(1)
    );
    println!("{line}");
}

fn bounded_diagnostic_samples<T>(items: impl IntoIterator<Item = T>) -> (usize, Vec<T>) {
    let mut count = 0;
    let mut samples = Vec::with_capacity(MAX_RECOVERY_DETAIL_SAMPLES);
    for item in items {
        count += 1;
        if samples.len() < MAX_RECOVERY_DETAIL_SAMPLES {
            samples.push(item);
        }
    }
    (count, samples)
}

fn epoch_ms() -> anyhow::Result<u64> {
    Ok(u64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
    )?)
}

fn status(
    node: &SyncManager,
    stats: &Writer,
    before: &Counts,
    diagnostics: &diagnostics::Diagnostics,
    baseline: &diagnostics::Baseline,
) -> anyhow::Result<Value> {
    let handle = node
        .membership
        .as_ref()
        .context("worker membership missing")?;
    handle
        .snapshot
        .has_changed()
        .context("worker membership actor exited")?;
    let snapshot = handle.snapshot.borrow();
    ensure!(
        snapshot.error.is_none(),
        "membership error: {:?}",
        snapshot.error
    );
    let (counts, queues) = observe(std::slice::from_ref(node))?;
    Ok(json!({
        "diagnostics": baseline.sample(node, diagnostics)?,
        "phase": "status", "records": snapshot.records.len(),
        "all_alive": snapshot.records.iter().all(|record| record.state == MembershipState::Alive),
        "terminal": snapshot.records.iter().filter(|record| matches!(record.state, MembershipState::Dead | MembershipState::Left))
            .map(|record| json!({"member": record.member, "state": format!("{:?}", record.state)})).collect::<Vec<_>>(),
        "counters": counts.raw(), "nominal_counters": counts.delta(before).raw(),
        "queues": queues, "accepted": stats.accepted.load(Ordering::Relaxed),
        "offered": stats.offered.load(Ordering::Relaxed),
        "accepted_in_window": stats.accepted_in_window.load(Ordering::Relaxed),
        "max_pending": stats.max_pending.load(Ordering::Relaxed),
        "pending_submissions": stats.pending_sample()?,
        "max_flush_us": stats.max_flush_us.load(Ordering::Relaxed),
        "max_lateness_us": stats.max_start_lateness_us.load(Ordering::Relaxed)
    }))
}

struct RecoveryProgressInputs<'a> {
    expected_origins: &'a std::collections::HashSet<String>,
    operations: u64,
    progress: &'a [crate::sync_manager::ReplicationProgress],
    // Test-only scheduling seam: queue a writer while the sampler holds its read guard.
    after_requests_read: Option<Box<dyn FnOnce() + Send + 'a>>,
}

async fn recovery_progress_sample(
    node: &SyncManager,
    diagnostics: &diagnostics::Diagnostics,
    diagnostic_baseline: &diagnostics::Baseline,
    elapsed: Duration,
    value: u64,
    expected: u64,
    inputs: RecoveryProgressInputs<'_>,
) -> anyhow::Result<Value> {
    let known_origins = inputs
        .progress
        .iter()
        .filter(|entry| inputs.expected_origins.contains(entry.origin.as_str()))
        .collect::<Vec<_>>();
    let incomplete_receipts = known_origins
        .iter()
        .filter(|entry| {
            entry.contiguous_sequence != inputs.operations
                || entry.known_high_sequence != inputs.operations
                || entry.sparse_positions != 0
                || entry.source_unavailable
                || entry.baseline_missing
                || entry.baseline_unconfirmed
                || entry.recovery_failure.is_some()
        })
        .map(|entry| {
            json!({
                "origin": entry.origin.as_str(),
                "contiguous": entry.contiguous_sequence,
                "known_high": entry.known_high_sequence,
                "sparse": entry.sparse_positions,
                "source_unavailable": entry.source_unavailable,
                "baseline_missing": entry.baseline_missing,
                "baseline_unconfirmed": entry.baseline_unconfirmed,
                "recovery_failure": entry.recovery_failure.is_some(),
            })
        })
        .collect::<Vec<_>>();
    let (incomplete_receipt_count, incomplete_receipts) =
        bounded_diagnostic_samples(incomplete_receipts);
    let active_peers = node
        .node
        .as_ref()
        .context("worker peer node missing")?
        .connected_peers()
        .await
        .into_iter()
        .map(|(_, id)| id.as_str().to_owned())
        .collect::<Vec<_>>();
    let mut replication_work = node.metrics.replication_work_for_test();
    replication_work.retain(|name, _| {
        matches!(
            *name,
            "remote_ops_applied_total"
                | "remote_ops_duplicate_total"
                | "remote_op_apply_errors_total"
                | "sync_errors_total"
        )
    });
    let mut recovery_metrics = String::new();
    node.metrics.peers.render(&mut recovery_metrics);
    let recovery_events = [
        "history_unavailable",
        "baseline_requested",
        "baseline_accepted",
        "baseline_rejected",
        "error",
    ]
    .into_iter()
    .map(|event| {
        let label = format!("numax_recovery_events_total{{event=\"{event}\"}} ");
        let value = recovery_metrics
            .lines()
            .find_map(|line| line.strip_prefix(&label))
            .context("recovery metric missing")?
            .parse::<u64>()?;
        Ok::<_, anyhow::Error>((event, value))
    })
    .collect::<anyhow::Result<std::collections::BTreeMap<_, _>>>()?;

    let now = node.clock.now();
    let cursors = node.history_cursors.read().await;
    let summary_pending = cursors
        .values()
        .filter(|cursor| {
            cursor
                .request_deadline
                .is_some_and(|deadline| deadline > now)
        })
        .count();
    let summary_expired = cursors
        .values()
        .filter(|cursor| {
            cursor
                .request_deadline
                .is_some_and(|deadline| deadline <= now)
        })
        .count();
    let catch_up = cursors.values().filter(|cursor| cursor.catch_up).count();
    let cursor_states = cursors
        .values()
        .take(MAX_RECOVERY_DETAIL_SAMPLES)
        .map(|cursor| {
            json!({
                "after_origin": cursor.after_origin,
                "catch_up": cursor.catch_up,
                "walk_gap": cursor.walk_gap,
                "rewalks": cursor.rewalks,
                "deadline_remaining_ms": cursor.request_deadline
                    .map(|deadline| deadline.saturating_duration_since(now).as_millis()),
            })
        })
        .collect::<Vec<_>>();
    drop(cursors);

    let (
        pending_ranges,
        expired_ranges,
        pending_range_age_ms,
        pending_range_detail_count,
        pending_range_details,
    ) = {
        let requests_guard = node.history_requests.read().await;
        if let Some(after_read) = inputs.after_requests_read {
            after_read();
        }
        let requests = requests_guard
            .values()
            .flat_map(|origins| origins.values())
            .collect::<Vec<_>>();
        let pending_ranges = requests
            .iter()
            .filter(|request| request.deadline > now)
            .count();
        let expired_ranges = requests
            .iter()
            .filter(|request| request.deadline <= now)
            .count();
        let pending_range_age_ms = requests
            .iter()
            .filter(|request| request.deadline > now)
            .map(|request| {
                node.config
                    .socket_timeout
                    .saturating_sub(request.deadline.duration_since(now))
                    .as_millis()
            })
            .max()
            .unwrap_or_default();
        let (pending_range_detail_count, pending_range_details) = bounded_diagnostic_samples(
            requests_guard
                .values()
                .flat_map(|origins| origins.iter())
                .filter(|(_, request)| request.deadline > now)
                .map(|(origin, request)| {
                    json!({
                        "origin": origin,
                        "start": request.start,
                        "end": request.end,
                        "remaining_ms": request.deadline.saturating_duration_since(now).as_millis(),
                    })
                }),
        );
        (
            pending_ranges,
            expired_ranges,
            pending_range_age_ms,
            pending_range_detail_count,
            pending_range_details,
        )
    };
    let diagnostics = diagnostic_baseline.sample(node, diagnostics)?;

    Ok(json!({
        "elapsed_ms": elapsed.as_millis(),
        "value": value,
        "expected": expected,
        "known_origins": known_origins.len(),
        "contiguous_ops": known_origins.iter().map(|entry| entry.contiguous_sequence.min(inputs.operations)).sum::<u64>(),
        "known_high_ops": known_origins.iter().map(|entry| entry.known_high_sequence.min(inputs.operations)).sum::<u64>(),
        "origins_with_gaps": known_origins.iter().filter(|entry| entry.contiguous_sequence < entry.known_high_sequence).count(),
        "sparse_positions": known_origins.iter().map(|entry| entry.sparse_positions).sum::<usize>(),
        "source_unavailable": known_origins.iter().filter(|entry| entry.source_unavailable).count(),
        "recovery_failures": known_origins.iter().filter(|entry| entry.recovery_failure.is_some()).count(),
        "incomplete_receipt_count": incomplete_receipt_count,
        "incomplete_receipts": incomplete_receipts,
        "active_peers": active_peers,
        "summary_requests_pending": summary_pending,
        "summary_requests_expired": summary_expired,
        "catch_up_peers": catch_up,
        "summary_cursors": cursor_states,
        "range_requests_pending": pending_ranges,
        "range_requests_expired": expired_ranges,
        "oldest_range_request_age_ms": pending_range_age_ms,
        "pending_range_details_count": pending_range_detail_count,
        "pending_ranges": pending_range_details,
        "replication_work": replication_work,
        "recovery_events": recovery_events,
        "repair_trace_events": diagnostics["repair_trace_events"],
        "repair_trace_items": diagnostics["repair_trace_items"],
        "connection_error_classes": diagnostics["connection_error_classes"],
        "network_events": diagnostics["network_events"],
        "event_loop": diagnostics["event_loop"],
        "history_batches": diagnostics["history_batches"],
        "remote_apply_stages": diagnostics["remote_apply_stages"],
    }))
}

fn numeric_summary(mut values: Vec<u64>) -> Value {
    values.sort_unstable();
    json!({
        "min": values.first(),
        "median": values.get(values.len() / 2),
        "max": values.last(),
    })
}

fn aggregate_sample_counts(
    samples: &[(usize, Value)],
    key: &str,
) -> anyhow::Result<std::collections::BTreeMap<String, u64>> {
    let mut totals = std::collections::BTreeMap::new();
    for (_, sample) in samples {
        for (name, value) in sample[key]
            .as_object()
            .with_context(|| format!("convergence sample missing {key}"))?
        {
            let total = totals.entry(name.clone()).or_insert(0u64);
            *total = total
                .checked_add(
                    value
                        .as_u64()
                        .with_context(|| format!("invalid {key} counter"))?,
                )
                .with_context(|| format!("{key} counter overflow"))?;
        }
    }
    Ok(totals)
}

fn nominal_status_samples<'a>(rows: &[&'a Value]) -> Vec<&'a Value> {
    rows.iter()
        .copied()
        .filter(|row| row.get("nominal_counters").is_some())
        .collect()
}

fn convergence_failure_report(
    samples: &[(usize, Value)],
    expected_workers: usize,
) -> anyhow::Result<Value> {
    let numeric = |key: &str| {
        samples
            .iter()
            .filter_map(|(_, sample)| sample[key].as_u64())
            .collect::<Vec<_>>()
    };
    let mut least_progress = samples.iter().collect::<Vec<_>>();
    least_progress.sort_by_key(|(worker, sample)| {
        (
            sample["value"].as_u64().unwrap_or(u64::MAX),
            sample["elapsed_ms"].as_u64().unwrap_or_default(),
            *worker,
        )
    });
    let least_progress = least_progress
        .into_iter()
        .take(MAX_CONVERGENCE_DETAIL_WORKERS)
        .map(|(worker, sample)| {
            json!({
                "worker": worker,
                "elapsed_ms": sample["elapsed_ms"],
                "value": sample["value"],
                "contiguous_ops": sample["contiguous_ops"],
                "known_high_ops": sample["known_high_ops"],
                "known_origins": sample["known_origins"],
                "origins_with_gaps": sample["origins_with_gaps"],
                "range_requests_pending": sample["range_requests_pending"],
                "range_requests_expired": sample["range_requests_expired"],
                "summary_requests_pending": sample["summary_requests_pending"],
                "summary_requests_expired": sample["summary_requests_expired"],
                "active_peers": sample["active_peers"],
                "connection_error_classes": sample["connection_error_classes"],
                "network_events": sample["network_events"],
                "recovery_events": sample["recovery_events"],
                "repair_trace_events": sample["repair_trace_events"],
                "repair_trace_items": sample["repair_trace_items"],
                "event_loop": sample["event_loop"],
                "history_batches": sample["history_batches"],
                "remote_apply_stages": sample["remote_apply_stages"],
                "incomplete_receipts": sample["incomplete_receipts"].as_array()
                    .map(|entries| entries.iter().take(3).cloned().collect::<Vec<_>>())
                    .unwrap_or_default(),
                "pending_ranges": sample["pending_ranges"].as_array()
                    .map(|entries| entries.iter().take(3).cloned().collect::<Vec<_>>())
                    .unwrap_or_default(),
            })
        })
        .collect::<Vec<_>>();
    let node_ids = samples
        .iter()
        .map(|(worker, _)| format!("detector-{worker}"))
        .collect::<std::collections::BTreeSet<_>>();
    let mut peer_graph = node_ids
        .iter()
        .map(|node_id| (node_id.clone(), std::collections::BTreeSet::new()))
        .collect::<std::collections::BTreeMap<_, _>>();
    for (worker, sample) in samples {
        let node_id = format!("detector-{worker}");
        let peers = sample["active_peers"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .filter(|peer| node_ids.contains(*peer) && *peer != node_id)
            .map(str::to_owned)
            .collect::<Vec<_>>();
        for peer in peers {
            peer_graph
                .entry(node_id.clone())
                .or_default()
                .insert(peer.clone());
            peer_graph.entry(peer).or_default().insert(node_id.clone());
        }
    }
    let mut unvisited = node_ids.clone();
    let mut component_sizes = Vec::new();
    while let Some(start) = unvisited.pop_first() {
        let mut pending = vec![start];
        let mut component_size = 0usize;
        while let Some(node_id) = pending.pop() {
            component_size += 1;
            if let Some(peers) = peer_graph.get(&node_id) {
                for peer in peers {
                    if unvisited.remove(peer) {
                        pending.push(peer.clone());
                    }
                }
            }
        }
        component_sizes.push(component_size);
    }
    let peer_graph_edges = peer_graph
        .values()
        .map(std::collections::BTreeSet::len)
        .sum::<usize>()
        / 2;
    component_sizes.sort_unstable();
    let peer_graph_summary = json!({
        "snapshot_workers": samples.len(),
        "undirected_edges": peer_graph_edges,
        "components": component_sizes.len(),
        "smallest_component": component_sizes.first().copied().unwrap_or_default(),
        "largest_component": component_sizes.last().copied().unwrap_or_default(),
        "isolated_workers": component_sizes.iter().filter(|size| **size == 1).count(),
        "scope": "undirected union of per-worker snapshots; samples are not a simultaneous graph observation"
    });
    let active_peer_counts = samples
        .iter()
        .filter_map(|(_, sample)| {
            sample["active_peers"]
                .as_array()
                .map(|peers| u64::try_from(peers.len()).unwrap_or_default())
        })
        .collect();
    let unique_active_peer_counts = samples
        .iter()
        .filter_map(|(_, sample)| {
            sample["active_peers"].as_array().map(|peers| {
                peers
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<std::collections::BTreeSet<_>>()
                    .len() as u64
            })
        })
        .collect::<Vec<_>>();
    let duplicate_peer_samples = samples
        .iter()
        .filter_map(|(_, sample)| {
            let peers = sample["active_peers"].as_array()?;
            let unique = peers
                .iter()
                .filter_map(Value::as_str)
                .collect::<std::collections::BTreeSet<_>>()
                .len();
            Some(peers.len().saturating_sub(unique))
        })
        .collect::<Vec<_>>();
    let workers_with_duplicate_peer_ids = duplicate_peer_samples
        .iter()
        .filter(|duplicates| **duplicates > 0)
        .count();
    let duplicate_active_peer_slots = duplicate_peer_samples.iter().sum::<usize>();

    Ok(json!({
        "workers_sampled": samples.len(),
        "workers_expected": expected_workers,
        "elapsed_ms": numeric_summary(numeric("elapsed_ms")),
        "value": numeric_summary(numeric("value")),
        "contiguous_ops": numeric_summary(numeric("contiguous_ops")),
        "known_high_ops": numeric_summary(numeric("known_high_ops")),
        "known_origins": numeric_summary(numeric("known_origins")),
        "origins_with_gaps": numeric_summary(numeric("origins_with_gaps")),
        "range_requests_pending": numeric_summary(numeric("range_requests_pending")),
        "range_requests_expired": numeric_summary(numeric("range_requests_expired")),
        "summary_requests_pending": numeric_summary(numeric("summary_requests_pending")),
        "summary_requests_expired": numeric_summary(numeric("summary_requests_expired")),
        "active_peer_count": numeric_summary(active_peer_counts),
        "unique_active_peer_count": numeric_summary(unique_active_peer_counts),
        "workers_with_duplicate_peer_ids": workers_with_duplicate_peer_ids,
        "duplicate_active_peer_slots": duplicate_active_peer_slots,
        "peer_graph": peer_graph_summary,
        "connection_error_classes": aggregate_sample_counts(samples, "connection_error_classes")?,
        "network_events": aggregate_sample_counts(samples, "network_events")?,
        "repair_trace_events": aggregate_sample_counts(samples, "repair_trace_events")?,
        "repair_trace_items": aggregate_sample_counts(samples, "repair_trace_items")?,
        "least_progress_workers": least_progress,
    }))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "internal worker; invoked only by the detector process coordinator"]
async fn worker() {
    let result = worker_run().await;
    if let Err(error) = &result {
        emit(json!({"phase": "error", "error": format!("{error:#}")}));
    }
    result.unwrap();
}

async fn worker_run() -> anyhow::Result<()> {
    let config: Value = serde_json::from_str(&std::env::var(CONFIG)?)?;
    let index = usize::try_from(number(&config, "index")?)?;
    let addresses: Vec<String> = serde_json::from_value(config["addresses"].clone())?;
    let count = addresses.len();
    ensure!(
        (count > 1 || (count == 1 && config["remote_history_profile"] == true)) && index < count,
        "invalid detector worker configuration"
    );
    let directory = config["directory"]
        .as_str()
        .context("worker datastore path missing")?;
    let disable_periodic_flush = config["disable_periodic_flush"]
        .as_bool()
        .context("worker periodic-flush control missing or invalid")?;
    ensure!(
        !disable_periodic_flush || config["remote_history_profile"] == true,
        "periodic-flush control is only valid for the remote diagnostic"
    );
    let store = Arc::new(if disable_periodic_flush {
        NxStore::open_without_periodic_flush(directory)?
    } else {
        NxStore::open(directory)?
    });
    if config["remote_history_profile"] == true {
        let trace_path = config["trace_path"]
            .as_str()
            .context("remote profile trace path missing")?;
        return remote_history_worker(directory, store, std::path::Path::new(trace_path)).await;
    }
    if config["storage_only"] == true {
        return storage_worker_run(
            index,
            count,
            directory,
            store,
            config["storage_metadata"] == true,
            config["metadata_batched"] == true,
        )
        .await;
    }
    let critical = config["critical_path"]
        .as_bool()
        .context("critical path control missing")?;
    let diagnostics = if critical {
        ensure!(
            config["trace_path"].is_null(),
            "critical path and legacy file timeline are separate modes"
        );
        diagnostics::Diagnostics::with_critical_path()?
    } else {
        match config.get("trace_path").filter(|value| !value.is_null()) {
            Some(path) => diagnostics::Diagnostics::with_timeline(std::path::Path::new(
                path.as_str().context("invalid recovery trace path")?,
            ))?,
            None => diagnostics::Diagnostics::default(),
        }
    };
    diagnostics.install()?;
    let node_config = SyncConfig::new()
        .with_listen_addr(addresses[index].clone())
        .with_max_peers(4)
        .with_serialization_format(SerializationFormat::Json)
        .with_peer(addresses[(index + count - 1) % count].clone())
        .with_peer(addresses[(index + 1) % count].clone());
    let mut node = SyncManager::try_new(
        NodeId::new(format!("detector-{index}")),
        node_config,
        store,
        metrics(),
    )?;
    node.start().await?;
    let (mut commands, input) = command_input();
    let stats = Arc::new(Writer::default());
    let mut _controlled_pause = None;
    let mut writer: Option<tokio::task::JoinHandle<anyhow::Result<()>>> = None;
    let result = async {
        let mut view = node.membership.as_ref().unwrap().snapshot.clone();
        let snapshot = view.wait_for(|snapshot| snapshot.records.iter().any(|record| record.member == node.node_id.as_str())).await?;
        let owner = snapshot.records.iter().find(|record| record.member == node.node_id.as_str()).context("worker owner missing")?.clone();
        drop(snapshot);
        emit(json!({"phase": "ready", "owner": owner}));
        let mut poll = tokio::time::interval(Duration::from_secs(1));
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut before = observe(std::slice::from_ref(&node))?.0;
        let mut work_before = diagnostics::Baseline::new(&node, &diagnostics)?;
        let mut nominal_end = None;
        let mut nominal_started = None;
        let mut duration = Duration::ZERO;
        let mut final_drain = Duration::from_secs(10);
        let mut operations = 0;
        let mut failure_target: Option<String> = None;
        let mut previous_state = None;
        loop {
            tokio::select! {
                biased;
                command = commands.recv() => {
                    let command = command.context("detector coordinator closed worker input")??;
                    match command["command"].as_str().context("worker command missing")? {
                        "bootstrap" => {
                            let owners: Vec<nx_net::MembershipRecord> = serde_json::from_value(command["owners"].clone())?;
                            ensure!(owners.len() == count, "bootstrap owner count mismatch");
                            for round in 1..count {
                                let target = &owners[(index + round) % count];
                                let endpoint = target.endpoint.as_ref().context("bootstrap endpoint missing")?;
                                let request = nx_net::ControlRequest {
                                    request_id: 1_000_000 + u64::try_from(round * count + index)?,
                                    owner: owner.clone(), updates: vec![],
                                    kind: nx_net::ControlKind::Exchange { after: None },
                                };
                                let network = node.node.as_ref().unwrap();
                                exchange_owner(network, endpoint, request, &target.member).await?;
                            }
                            emit(json!({"phase": "bootstrapped"}));
                        }
                        "nominal" => {
                            diagnostics.timeline_phase("nominal_armed")?;
                            ensure!(writer.is_none() && nominal_end.is_none(), "duplicate nominal command");
                            duration = Duration::from_secs(number(&command, "duration_s")?);
                            let period = Duration::from_secs(number(&command, "period_s")?);
                            final_drain = drain_limit(period)?;
                            ensure!(period.as_secs() > 0 && duration.as_secs().is_multiple_of(period.as_secs()), "invalid worker load period");
                            operations = duration.as_secs() / period.as_secs();
                            let wait = number(&command, "start_epoch_ms")?.checked_sub(epoch_ms()?).context("nominal start command arrived late")?;
                            emit(json!({"phase": "nominal-armed"}));
                            tokio::time::sleep(Duration::from_millis(wait)).await;
                            before = observe(std::slice::from_ref(&node))?.0;
                            work_before = diagnostics::Baseline::new(&node, &diagnostics)?;
                            if config["critical_path"] == true {
                                diagnostics.begin_remote_replay_capture()?;
                            }
                            if config["pause_first_nominal_flush"] == true {
                                let (reached, ready) = std::sync::mpsc::channel();
                                let (resume, continue_flush) = std::sync::mpsc::channel();
                                node.store.pause_next_flush(reached, continue_flush);
                                _controlled_pause = Some((ready, resume));
                            }
                            let start = Instant::now();
                            nominal_started = Some(StdInstant::now());
                            nominal_end = Some(start + duration);
                            let offset = Duration::from_micros(u64::try_from(index)? * u64::try_from(period.as_micros())? / u64::try_from(count)?);
                            writer = Some(tokio::spawn(write_load(WriteTarget::Replicated(Arc::new(node.handle())), stats.clone(), start, offset, duration, period, index)));
                        }
                        "failure" => {
                            ensure!(writer.is_none() && nominal_end.is_none(), "failure command before nominal completion");
                            failure_target = Some(command["target"].as_str().context("failure target missing")?.to_owned());
                            before = observe(std::slice::from_ref(&node))?.0;
                            poll = tokio::time::interval(Duration::from_millis(100));
                            poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                            emit(json!({"phase": "failure-armed", "counters": before.raw()}));
                        }
                        "convergence" => {
                            ensure!(writer.is_none() && nominal_end.is_none(), "convergence before nominal completion");
                            let expected = operations.checked_mul(u64::try_from(count)?).context("convergence total overflow")?;
                            let expected_origins = (0..count).map(|origin| format!("detector-{origin}"))
                                .collect::<std::collections::HashSet<_>>();
                            let started = StdInstant::now();
                            let deadline = Instant::now() + CONVERGENCE_LIMIT;
                            let convergence_diagnostics = diagnostics::Baseline::new(&node, &diagnostics)?;
                            diagnostics.timeline_phase("convergence")?;
                            let mut next_diagnostic = Duration::from_secs(5);
                            let mut progress_samples = Vec::new();
                            loop {
                                let value = node.get_counter_value(KEY).await;
                                let progress = node.handle().replication_progress()?;
                                let complete_receipt = |entry: &crate::sync_manager::ReplicationProgress| {
                                    expected_origins.contains(entry.origin.as_str())
                                        && entry.contiguous_sequence == operations
                                        && entry.known_high_sequence == operations
                                        && entry.sparse_positions == 0
                                        && !entry.source_unavailable
                                        && !entry.baseline_missing
                                        && !entry.baseline_unconfirmed
                                        && entry.recovery_failure.is_none()
                                };
                                let receipts_complete = progress.len() == count && progress.iter().all(complete_receipt);
                                let elapsed = started.elapsed();
                                if elapsed >= next_diagnostic || elapsed >= CONVERGENCE_LIMIT {
                                    diagnostics.timeline_receipt(&progress)?;
                                    let sample = recovery_progress_sample(
                                        &node,
                                        &diagnostics,
                                        &convergence_diagnostics,
                                        elapsed,
                                        value,
                                        expected,
                                        RecoveryProgressInputs {
                                            expected_origins: &expected_origins,
                                            operations,
                                            progress: &progress,
                                            after_requests_read: None,
                                        },
                                    ).await?;
                                    progress_samples.push(sample.clone());
                                    emit(json!({"phase": "status", "convergence": sample}));
                                    next_diagnostic = elapsed.saturating_add(Duration::from_secs(5));
                                }
                                if value == expected && receipts_complete {
                                    diagnostics.timeline_receipt(&progress)?;
                                    let verified_at = elapsed;
                                    ensure!(read_materialized(&node.store, KEY) == expected, "convergence materialized value mismatch");
                                    ensure!(read_durable_gcounter_state(&node.store, KEY).value() == expected, "convergence durable state mismatch");
                                    ensure!(Instant::now() < deadline,
                                        "convergence exceeded its declared limit: value={value}/{expected}, complete_origins={}, receipt_check_ms={}, durable_check_ms={}",
                                        progress.len(), verified_at.as_millis(), started.elapsed().saturating_sub(verified_at).as_millis());
                                    diagnostics.timeline_phase("convergence_verified")?;
                                    diagnostics.save_timeline()?;
                                    emit(json!({"phase": "converged", "value": expected, "origins": progress.len(),
                                        "operations_per_origin": operations, "elapsed_ms": started.elapsed().as_millis()}));
                                    break;
                                }
                                ensure!(Instant::now() < deadline,
                                    "convergence exceeded its declared limit: value={value}/{expected}, origins={}/{count}, incomplete_receipts={:?}, progress_samples={}",
                                    progress.len(), progress.iter().filter(|entry| !complete_receipt(entry)).take(8)
                                        .map(|entry| (entry.origin.as_str(), entry.contiguous_sequence, entry.known_high_sequence, entry.sparse_positions, entry.source_unavailable))
                                        .collect::<Vec<_>>(),
                                    serde_json::to_string(&progress_samples)?);
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            }
                        }
                        "critical-snapshot" => {
                            let snapshot = diagnostics.critical_snapshot(true)?
                                .context("critical snapshot requested for an inactive recorder")?;
                            emit(json!({"phase": "critical-snapshot",
                                "request_id": number(&command, "request_id")?, "snapshot": snapshot}));
                        }
                        "remote-replay-snapshot" => {
                            let trace = diagnostics.remote_replay_snapshot()?;
                            emit(json!({"phase": "remote-replay-snapshot",
                                "request_id": number(&command, "request_id")?, "trace": trace}));
                        }
                        "stop" => break,
                        other => anyhow::bail!("unknown worker command {other}"),
                    }
                }
                _ = poll.tick() => {
                    if let Some(task) = &writer
                        && task.is_finished() {
                        writer.take().unwrap().await??;
                    }
                    let current = status(&node, &stats, &before, &diagnostics, &work_before)?;
                    if let Some(end) = nominal_end {
                        let counts = Counts::from_raw(&current["nominal_counters"])?;
                        ensure!(counts.declared_dead == 0 && counts.learned_dead == 0,
                            "false failure verdict during nominal load: {counts:?}");
                        ensure!(current["records"].as_u64() == Some(u64::try_from(count)?),
                            "worker lost membership subjects");
                        ensure!(current["terminal"].as_array().context("terminal records missing")?.is_empty(),
                            "terminal membership state during nominal load: {}", current["terminal"]);
                        if Instant::now() >= end {
                            ensure!(stats.offered.load(Ordering::Relaxed) == operations, "nominal offered load not achieved");
                            ensure!(Instant::now() <= end + final_drain, "nominal final drain exceeded its declared limit");
                            if writer.is_some() {
                                emit(current);
                                continue;
                            }
                            ensure!(stats.accepted.load(Ordering::Relaxed) == operations, "not every offered operation was confirmed");
                            ensure!(counts.probe_success > 0, "worker had no successful probes in the nominal window");
                            let elapsed = nominal_started.context("nominal start missing")?.elapsed();
                            ensure!(elapsed >= duration, "nominal window shorter than real duration");
                            nominal_end = None;
                            emit(json!({"phase": "nominal-complete", "elapsed_s": elapsed.as_secs_f64(), "status": current}));
                        } else {
                            emit(current);
                        }
                    } else if let Some(target) = &failure_target {
                        ensure!(current["terminal"].as_array().context("terminal records missing")?.iter()
                            .all(|record| record["member"].as_str() == Some(target.as_str()) && record["state"] == "Dead"),
                            "unexpected terminal state after true failure: {}", current["terminal"]);
                        let snapshot = node.membership.as_ref().unwrap().snapshot.borrow();
                        let state = snapshot.records.iter().find(|record| &record.member == target).map(|record| record.state);
                        if state != previous_state {
                            previous_state = state;
                            emit(json!({"phase": "failure-state", "state": format!("{state:?}"), "status": current}));
                        }
                    } else {
                        emit(current);
                    }
                }
            }
        }
        anyhow::Ok(())
    }.await;
    if let Some(task) = writer {
        task.abort();
        match task.await {
            Err(error) if error.is_cancelled() => {}
            Err(error) => return Err(error).context("writer join failed during cleanup"),
            Ok(result) => result?,
        }
    }
    let trace_save = diagnostics
        .timeline_phase("measurement_end")
        .and_then(|()| diagnostics.save_timeline());
    let shutdown = finish_shutdown(&mut node, Instant::now() + Duration::from_secs(120)).await;
    if let Ok(pending_calls) = &shutdown {
        emit(json!({"phase": "shutdown-complete", "pending_calls": pending_calls}));
    }
    // The parent closes stdin after its stop command, allowing this reader to finish.
    if result.is_ok() {
        input
            .join()
            .map_err(|_| anyhow::anyhow!("worker input reader panicked"))?;
    }
    ensure!(
        result.is_ok() && shutdown.is_ok() && trace_save.is_ok(),
        "worker={result:?}; shutdown={shutdown:?}; timeline={trace_save:?}"
    );
    Ok(())
}

async fn finish_shutdown(node: &mut SyncManager, deadline: Instant) -> anyhow::Result<u64> {
    tokio::time::timeout_at(deadline, async {
        let mut pending_calls = 0u64;
        loop {
            match node.shutdown().await {
                Ok(()) => return Ok(pending_calls),
                Err(error) if error.is::<crate::sync_manager::ShutdownPending>() => {
                    pending_calls += 1;
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await
    .context("detector worker durable shutdown exceeded its existing total budget")?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_shutdown_retains_services_and_propagates_durable_outcome() {
    for fail_flush in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(NxStore::open(directory.path()).unwrap());
        let endpoint = free_addr();
        let (mut manager, handle) = started_manager_with_store(
            NodeId::new(format!("pending-leave-{fail_flush}")),
            SyncConfig::new().with_listen_addr(endpoint.clone()),
            Arc::clone(&store),
        )
        .await;
        let snapshot = manager.membership.as_ref().unwrap().snapshot.clone();
        let before = store.completed_flushes();
        if fail_flush {
            store.inject_disk_full_on_flush(true);
        } else {
            let (reached, paused) = std::sync::mpsc::channel();
            let (resume, released) = std::sync::mpsc::channel();
            store.pause_next_flush(reached, released);
            let stopping = tokio::spawn(async move {
                let result = manager.shutdown().await;
                (manager, result)
            });
            tokio::task::spawn_blocking(move || paused.recv_timeout(Duration::from_secs(5)))
                .await
                .unwrap()
                .unwrap();
            let (retained, result) = stopping.await.unwrap();
            manager = retained;
            assert!(matches!(
                result
                    .unwrap_err()
                    .downcast_ref::<crate::sync_manager::ShutdownPending>(),
                Some(crate::sync_manager::ShutdownPending::DurabilityUnconfirmed)
            ));
            assert!(manager.membership.is_some());
            assert!(manager.discovery_coordinator.is_some());
            assert_eq!(store.completed_flushes(), before);
            assert!(!snapshot.borrow().records.iter().any(|record| {
                record.member == manager.node_id.as_str() && record.state == MembershipState::Left
            }));
            drop(tokio::net::TcpStream::connect(&endpoint).await.unwrap());
            resume.send(()).unwrap();
        }
        let result = finish_shutdown(&mut manager, Instant::now() + Duration::from_secs(5)).await;
        if fail_flush {
            let error = result.unwrap_err();
            assert!(!error.is::<crate::sync_manager::ShutdownPending>());
            assert!(error.to_string().contains("membership storage"));
            assert_eq!(store.completed_flushes(), before);
            assert!(snapshot.borrow().error.is_some());
            assert!(!snapshot.borrow().records.iter().any(|record| {
                record.member == manager.node_id.as_str() && record.state == MembershipState::Left
            }));
            assert!(
                finish_shutdown(&mut manager, Instant::now() + Duration::from_secs(5))
                    .await
                    .is_err()
            );
        } else {
            assert_eq!(result.unwrap(), 0);
            assert!(manager.membership.is_none());
            assert_eq!(store.completed_flushes(), before + 1);
            assert!(snapshot.borrow().records.iter().any(|record| {
                record.member == manager.node_id.as_str() && record.state == MembershipState::Left
            }));
        }
        drop(handle);
        drop(manager);
        drop(store);
        NxStore::open(directory.path()).expect("shutdown released its datastore");
    }
}

fn command_input() -> (
    tokio::sync::mpsc::Receiver<anyhow::Result<Value>>,
    std::thread::JoinHandle<()>,
) {
    let (sender, receiver) = tokio::sync::mpsc::channel(8);
    let input = std::thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            let command = line
                .map_err(anyhow::Error::from)
                .and_then(|line| Ok(serde_json::from_str::<Value>(&line)?));
            if sender.blocking_send(command).is_err() {
                break;
            }
        }
    });
    (receiver, input)
}

async fn storage_worker_run(
    index: usize,
    count: usize,
    directory: &str,
    store: Arc<NxStore>,
    metadata: bool,
    batched: bool,
) -> anyhow::Result<()> {
    let (mut commands, input) = command_input();
    let stats = Arc::new(Writer::default());
    let mut writer = None;
    let mut operations = 0;
    let result = async {
        emit(json!({"phase": "ready"}));
        let command = commands.recv().await.context("storage coordinator closed input")??;
        ensure!(command["command"] == "nominal", "storage worker expected nominal command");
        let duration = Duration::from_secs(number(&command, "duration_s")?);
        let period = Duration::from_secs(number(&command, "period_s")?);
        ensure!(period.as_secs() > 0 && duration.as_secs().is_multiple_of(period.as_secs()), "invalid storage load period");
        operations = duration.as_secs() / period.as_secs();
        let wait = number(&command, "start_epoch_ms")?.checked_sub(epoch_ms()?).context("storage start command arrived late")?;
        emit(json!({"phase": "nominal-armed"}));
        tokio::time::sleep(Duration::from_millis(wait)).await;
        let started = StdInstant::now();
        let start = Instant::now();
        let offset = Duration::from_micros(u64::try_from(index)? * u64::try_from(period.as_micros())? / u64::try_from(count)?);
        let target = if batched { WriteTarget::StorageMetadataBatched(store.clone()) } else if metadata { WriteTarget::StorageMetadata(store.clone()) } else { WriteTarget::Storage(store.clone()) };
        writer = Some(tokio::spawn(write_load(target, stats.clone(), start, offset, duration, period, index)));
        let mut poll = tokio::time::interval(Duration::from_secs(1));
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            poll.tick().await;
            if let Some(task) = &writer
                && task.is_finished() {
                writer.take().unwrap().await??;
            }
            let current = json!({
                "phase": "status", "queues": {}, "detector_enabled": false,
                "accepted": stats.accepted.load(Ordering::Relaxed),
                "offered": stats.offered.load(Ordering::Relaxed),
                "accepted_in_window": stats.accepted_in_window.load(Ordering::Relaxed),
                "max_pending": stats.max_pending.load(Ordering::Relaxed),
                "max_flush_us": stats.max_flush_us.load(Ordering::Relaxed),
                "max_lateness_us": stats.max_start_lateness_us.load(Ordering::Relaxed)
            });
            if Instant::now() >= start + duration && writer.is_none() {
                ensure!(stats.offered.load(Ordering::Relaxed) == operations
                    && stats.accepted.load(Ordering::Relaxed) == operations,
                    "storage worker did not confirm the entire offered load");
                ensure!(storage_counter(&store)? == operations, "storage baseline lost an increment");
                if metadata { verify_storage_metadata(&store, operations)?; }
                emit(json!({"phase": "nominal-complete", "elapsed_s": started.elapsed().as_secs_f64(), "status": current}));
                break;
            }
            emit(current);
        }
        let command = commands.recv().await.context("storage coordinator closed input")??;
        ensure!(command["command"] == "stop", "storage worker expected stop command");
        anyhow::Ok(())
    }.await;
    if let Some(task) = writer {
        task.abort();
        match task.await {
            Err(error) if error.is_cancelled() => {}
            Err(error) => return Err(error).context("storage writer cleanup join failed"),
            Ok(outcome) => outcome?,
        }
    }
    drop(store);
    if result.is_ok() {
        input
            .join()
            .map_err(|_| anyhow::anyhow!("storage input reader panicked"))?;
        let reopened = NxStore::open(directory)?;
        ensure!(
            storage_counter(&reopened)? == operations,
            "storage baseline counter did not survive reopen"
        );
        if metadata {
            verify_storage_metadata(&reopened, operations)?;
        }
    }
    result
}

enum Event {
    Message(usize, Value),
    Closed(usize),
    Invalid(usize, String),
}

struct Process {
    child: Child,
    input: Option<ChildStdin>,
    stderr: std::path::PathBuf,
}

impl Drop for Process {
    fn drop(&mut self) {
        match self.child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) => {}
            Err(error) => eprintln!("detector worker status cleanup failed: {error}"),
        }
        if let Err(error) = self.child.kill() {
            eprintln!("detector worker kill cleanup failed: {error}");
        }
        if let Err(error) = self.child.wait() {
            eprintln!("detector worker reap cleanup failed: {error}");
        }
    }
}

struct Processes {
    children: Vec<Process>,
    readers: Vec<std::thread::JoinHandle<()>>,
    events: Receiver<Event>,
    overflow: Arc<AtomicBool>,
    latest: Vec<watch::Receiver<Value>>,
    storage_only: bool,
    trace_directory: Option<std::path::PathBuf>,
    shutdown_failures: Vec<BTreeMap<&'static str, String>>,
    _directory: tempfile::TempDir,
}

fn send_event(sender: &SyncSender<Event>, overflow: &AtomicBool, event: Event) -> bool {
    match sender.try_send(event) {
        Ok(()) => true,
        Err(std::sync::mpsc::TrySendError::Full(_)) => {
            overflow.store(true, Ordering::Relaxed);
            false
        }
        Err(std::sync::mpsc::TrySendError::Disconnected(_)) => false,
    }
}

fn record_message(
    index: usize,
    value: Value,
    latest: &watch::Sender<Value>,
    events: &SyncSender<Event>,
    overflow: &AtomicBool,
) -> bool {
    if value["phase"] == "status" {
        latest.send_replace(value);
        return true;
    }
    if value["phase"] == "nominal-complete" || value["phase"] == "failure-state" {
        latest.send_replace(value["status"].clone());
    }
    send_event(events, overflow, Event::Message(index, value))
}

#[test]
fn diagnostic_shutdown_collects_completions_after_worker_error() {
    let directory = tempfile::tempdir().unwrap();
    let mut children = Vec::new();
    for index in 0..2 {
        let stderr = directory.path().join(format!("worker-{index}.stderr"));
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--list")
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(&stderr).unwrap())
            .spawn()
            .unwrap();
        assert!(child.wait().unwrap().success());
        children.push(Process {
            child,
            input: None,
            stderr,
        });
    }
    let (sender, events) = std::sync::mpsc::sync_channel(512);
    for (index, value) in [
        (0, json!({"phase": "shutdown-complete", "pending_calls": 0})),
        (
            0,
            json!({"phase": "error", "error": "deliberate worker failure"}),
        ),
        (1, json!({"phase": "shutdown-complete", "pending_calls": 0})),
    ] {
        sender.send(Event::Message(index, value)).unwrap();
    }
    let mut group = Processes {
        children,
        readers: Vec::new(),
        events,
        overflow: Arc::new(AtomicBool::new(false)),
        latest: Vec::new(),
        storage_only: false,
        trace_directory: Some(directory.path().to_owned()),
        shutdown_failures: vec![BTreeMap::new(); 2],
        _directory: directory,
    };
    let mut completed = vec![None; 2];
    group
        .shutdown_events(&mut completed)
        .expect("diagnostic cleanup discarded later worker completions");
    assert_eq!(completed, [Some(0), Some(0)]);
    assert!(group.shutdown_failures[0]["reported"].contains("deliberate worker failure"));
    assert!(group.shutdown_failures[1].is_empty());

    group.trace_directory = None;
    sender
        .send(Event::Message(
            0,
            json!({"phase": "error", "error": "ordinary failure"}),
        ))
        .unwrap();
    assert!(
        group.shutdown_events(&mut completed).is_err(),
        "ordinary cleanup must remain fail-fast"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn diagnostic_failure_preserves_three_process_timelines() {
    let name = "sync_manager::manager::tests::detector_soak::process::diagnostic_failure_preserves_three_process_timelines";
    if child(name, Duration::from_secs(180)) {
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let mut group =
        Processes::spawn_with_trace(3, None, Some(directory.path().to_owned())).unwrap();
    group
        .phase("ready", None, Duration::from_secs(20))
        .await
        .unwrap();
    group
        .command(0, &json!({"command": "deliberate-diagnostic-error"}))
        .unwrap();
    let error = group
        .stop(None)
        .await
        .expect_err("failed worker was reported as successful");
    assert!(error.to_string().contains("collected failures"));
    drop(group);
    for index in 0..3 {
        let report: Value = serde_json::from_slice(
            &std::fs::read(directory.path().join(format!("worker-{index}.json"))).unwrap(),
        )
        .unwrap();
        assert_eq!(report["complete"], true);
        assert_eq!(report["dropped"], 0);
        assert!(
            report["events"]
                .as_array()
                .unwrap()
                .iter()
                .any(|event| event["phase"] == "measurement_end")
        );
    }
    let failures: Value = serde_json::from_slice(
        &std::fs::read(directory.path().join("shutdown-failures.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(failures["failed_workers"], json!([0]));
    assert_eq!(failures["completed_workers"], 3);
    assert_eq!(failures["total_budget_s"], 120);
    assert!(
        failures["errors"][0]
            .to_string()
            .contains("deliberate-diagnostic-error")
    );
}

impl Processes {
    async fn force_stop_diagnostic(&mut self) -> anyhow::Result<()> {
        for worker in &mut self.children {
            worker.input.take();
            if worker.child.try_wait()?.is_none() {
                worker
                    .child
                    .kill()
                    .context("cannot terminate diagnostic worker")?;
            }
        }
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let mut running = false;
            for worker in &mut self.children {
                running |= worker.child.try_wait()?.is_none();
            }
            if !running {
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "diagnostic worker reap exceeded existing shutdown budget"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        for reader in self.readers.drain(..) {
            reader
                .join()
                .map_err(|_| anyhow::anyhow!("diagnostic worker output reader panicked"))?;
        }
        Ok(())
    }

    fn spawn(count: usize, storage: Option<StorageWorkload>) -> anyhow::Result<Self> {
        let trace_directory = std::env::var_os(TIMELINE_DIR_ENV).map(std::path::PathBuf::from);
        Self::spawn_with_trace(count, storage, trace_directory)
    }

    fn spawn_with_trace(
        count: usize,
        storage: Option<StorageWorkload>,
        trace_directory: Option<std::path::PathBuf>,
    ) -> anyhow::Result<Self> {
        Self::spawn_with_options(count, storage, trace_directory, false, false)
    }

    fn spawn_with_options(
        count: usize,
        storage: Option<StorageWorkload>,
        trace_directory: Option<std::path::PathBuf>,
        controlled_pause: bool,
        critical_path: bool,
    ) -> anyhow::Result<Self> {
        ensure!(
            !critical_path || (storage.is_none() && trace_directory.is_none()),
            "critical path is restricted to a network diagnostic without file timeline"
        );
        let remote_history = matches!(
            storage,
            Some(
                StorageWorkload::RemoteHistory | StorageWorkload::RemoteHistoryWithoutPeriodicFlush
            )
        );
        if let Some(path) = &trace_directory {
            path.to_str()
                .context("recovery trace directory is not valid UTF-8")?;
            ensure!(
                (storage.is_none() || remote_history) && path.is_dir(),
                "recovery trace requires a network or remote-history scenario and an existing artifact directory"
            );
            eprintln!("DETECTOR_TIMELINE_DIR {}", path.display());
        }
        let directory = tempfile::tempdir()?;
        let addresses = (0..count).map(|_| free_addr()).collect::<Vec<_>>();
        let (sender, events) = std::sync::mpsc::sync_channel(512);
        let mut group = Self {
            children: Vec::new(),
            readers: Vec::new(),
            events,
            overflow: Arc::new(AtomicBool::new(false)),
            latest: Vec::new(),
            storage_only: storage.is_some(),
            trace_directory: trace_directory.clone(),
            shutdown_failures: vec![BTreeMap::new(); count],
            _directory: directory,
        };
        for index in 0..count {
            let stderr = group
                ._directory
                .path()
                .join(format!("worker-{index}.stderr"));
            let config = json!({"index": index, "addresses": addresses, "storage_only": storage.is_some(),
                "critical_path": critical_path,
                "pause_first_nominal_flush": controlled_pause && index == 0,
                "storage_metadata": matches!(storage, Some(StorageWorkload::Metadata | StorageWorkload::MetadataBatched)),
                "metadata_batched": matches!(storage, Some(StorageWorkload::MetadataBatched)),
                "directory": group._directory.path().join(format!("store-{index}")),
                "remote_history_profile": remote_history,
                "disable_periodic_flush": matches!(storage, Some(StorageWorkload::RemoteHistoryWithoutPeriodicFlush)),
                "trace_path": trace_directory.as_ref().map(|path| path.join(format!("worker-{index}.json")))
                    .or_else(|| remote_history
                        .then(|| group._directory.path().join(format!("worker-{index}.json"))))});
            let mut command = Command::new(std::env::current_exe()?);
            command
                .args(["--exact", WORKER, "--ignored", "--nocapture"])
                .env(CONFIG, config.to_string())
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(std::fs::File::create(&stderr)?);
            if storage.is_none() {
                command.env(PROBE_SEED_ENV, DETECTOR_PROBE_SEED.to_string());
            }
            let mut child = command
                .spawn()
                .with_context(|| format!("cannot spawn detector worker {index}"))?;
            let input = child.stdin.take().context("worker input pipe missing")?;
            let stdout = child.stdout.take().context("worker output pipe missing")?;
            group.children.push(Process {
                child,
                input: Some(input),
                stderr,
            });
            let sender = sender.clone();
            let overflow = group.overflow.clone();
            let (latest, view) = watch::channel(Value::Null);
            group.latest.push(view);
            group.readers.push(std::thread::spawn(move || {
                let mut reader = std::io::BufReader::new(stdout);
                loop {
                    let mut line = String::new();
                    match reader.by_ref().take(65537).read_line(&mut line) {
                        Ok(0) => {
                            send_event(&sender, &overflow, Event::Closed(index));
                            break;
                        }
                        Ok(size) if size <= 65536 => {
                            if let Some((_, payload)) = line.split_once(PREFIX) {
                                match serde_json::from_str::<Value>(payload) {
                                    Ok(value) => {
                                        if !record_message(
                                            index, value, &latest, &sender, &overflow,
                                        ) {
                                            break;
                                        }
                                    }
                                    Err(error) => {
                                        send_event(
                                            &sender,
                                            &overflow,
                                            Event::Invalid(index, error.to_string()),
                                        );
                                        break;
                                    }
                                }
                            }
                        }
                        Ok(_) => {
                            send_event(
                                &sender,
                                &overflow,
                                Event::Invalid(index, "worker output line exceeds 64 KiB".into()),
                            );
                            break;
                        }
                        Err(error) => {
                            send_event(
                                &sender,
                                &overflow,
                                Event::Invalid(index, error.to_string()),
                            );
                            break;
                        }
                    }
                }
            }));
        }
        Ok(group)
    }

    fn command(&mut self, index: usize, value: &Value) -> anyhow::Result<()> {
        let input = self.children[index]
            .input
            .as_mut()
            .context("worker command pipe closed")?;
        writeln!(input, "{value}")?;
        input.flush()?;
        Ok(())
    }

    fn broadcast(&mut self, value: &Value, except: Option<usize>) -> anyhow::Result<()> {
        for index in 0..self.children.len() {
            if Some(index) != except {
                self.command(index, value)?;
            }
        }
        Ok(())
    }

    async fn critical_snapshots(
        &mut self,
        targets: &[usize],
        request_id: u64,
        snapshots: &mut BTreeMap<usize, Value>,
    ) -> anyhow::Result<()> {
        ensure!(
            !targets.is_empty() && targets.len() <= 3,
            "critical snapshot selection exceeds bound"
        );
        for &index in targets {
            ensure!(
                index < self.children.len(),
                "critical snapshot worker out of range"
            );
            self.command(
                index,
                &json!({"command": "critical-snapshot", "request_id": request_id}),
            )?;
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while snapshots.len() != targets.len() {
            ensure!(
                Instant::now() < deadline,
                "critical snapshot response timeout"
            );
            while let Some((index, value)) = self.receive()? {
                if value["phase"] == "critical-snapshot" {
                    ensure!(
                        targets.contains(&index) && value["request_id"] == request_id,
                        "unexpected critical snapshot response"
                    );
                    ensure!(
                        value["snapshot"].is_object(),
                        "critical snapshot response missing body"
                    );
                    ensure!(
                        snapshots.insert(index, value["snapshot"].clone()).is_none(),
                        "duplicate critical snapshot response"
                    );
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }

    async fn remote_replay_snapshots(
        &mut self,
        request_id: u64,
        snapshots: &mut BTreeMap<usize, Value>,
    ) -> anyhow::Result<()> {
        let targets = (0..self.children.len()).collect::<Vec<_>>();
        for &index in &targets {
            self.command(
                index,
                &json!({"command": "remote-replay-snapshot", "request_id": request_id}),
            )?;
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while snapshots.len() != targets.len() {
            ensure!(
                Instant::now() < deadline,
                "remote replay snapshot response timeout"
            );
            while let Some((index, value)) = self.receive()? {
                if value["phase"] == "remote-replay-snapshot" {
                    ensure!(
                        targets.contains(&index) && value["request_id"] == request_id,
                        "unexpected remote replay snapshot response"
                    );
                    ensure!(
                        value["trace"].is_object(),
                        "remote replay snapshot response missing body"
                    );
                    ensure!(
                        snapshots.insert(index, value["trace"].clone()).is_none(),
                        "duplicate remote replay snapshot response"
                    );
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }

    fn diagnostic(&self, index: usize) -> anyhow::Result<String> {
        use std::io::{Seek, SeekFrom};
        let mut file = std::fs::File::open(&self.children[index].stderr)?;
        file.seek(SeekFrom::Start(file.metadata()?.len().saturating_sub(8192)))?;
        let mut bytes = Vec::new();
        file.take(8192).read_to_end(&mut bytes)?;
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }

    fn receive(&mut self) -> anyhow::Result<Option<(usize, Value)>> {
        ensure!(
            !self.overflow.load(Ordering::Relaxed),
            "bounded detector event queue overflowed"
        );
        match self.events.try_recv() {
            Ok(Event::Message(index, value)) => {
                ensure!(
                    value["phase"] != "error",
                    "worker {index} failed: {value}; stderr={}",
                    self.diagnostic(index)?
                );
                Ok(Some((index, value)))
            }
            Ok(Event::Closed(index)) => anyhow::bail!(
                "worker {index} exited unexpectedly: {}",
                self.diagnostic(index)?
            ),
            Ok(Event::Invalid(index, error)) => {
                anyhow::bail!("invalid worker {index} output: {error}")
            }
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => anyhow::bail!("all detector worker readers exited"),
        }
    }

    async fn phase(
        &mut self,
        phase: &str,
        except: Option<usize>,
        limit: Duration,
    ) -> anyhow::Result<Vec<Value>> {
        let deadline = Instant::now() + limit;
        let mut values = vec![Value::Null; self.children.len()];
        while values
            .iter()
            .enumerate()
            .any(|(index, value)| Some(index) != except && value.is_null())
        {
            ensure!(
                Instant::now() < deadline,
                "timed out waiting for detector worker phase {phase}: {}/{} completed",
                values.iter().filter(|value| !value.is_null()).count(),
                values.len() - usize::from(except.is_some())
            );
            while let Some((index, value)) = self.receive()? {
                if value["phase"] == phase {
                    values[index] = value;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(values)
    }

    fn rows(&self) -> Vec<Value> {
        self.latest
            .iter()
            .map(|view| view.borrow().clone())
            .collect()
    }

    async fn stop(&mut self, killed: Option<usize>) -> anyhow::Result<()> {
        let killed_pid = killed.map(|index| self.children[index].child.id());
        for index in 0..self.children.len() {
            if Some(index) != killed
                && self.children[index].input.is_some()
                && let Err(error) = self.command(index, &json!({"command": "stop"}))
            {
                if self.trace_directory.is_none() {
                    return Err(error);
                }
                self.shutdown_failures[index].insert("command", format!("{error:#}"));
            }
            self.children[index].input.take();
        }
        let deadline = Instant::now() + Duration::from_secs(120);
        let mut completed = vec![None; self.children.len()];
        loop {
            self.shutdown_events(&mut completed)?;
            let mut running = false;
            for index in 0..self.children.len() {
                let pid = self.children[index].child.id();
                match self.children[index].child.try_wait()? {
                    None => running = true,
                    Some(status) if !status.success() && Some(pid) != killed_pid => {
                        if self.trace_directory.is_none() {
                            anyhow::bail!(
                                "worker {index} shutdown failed: {status}; stderr={}",
                                self.diagnostic(index)?
                            );
                        }
                        if !self.shutdown_failures[index].contains_key("exit") {
                            let error = format!(
                                "worker {index} shutdown failed: {status}; stderr={}",
                                self.diagnostic(index)?
                            );
                            self.shutdown_failures[index].insert("exit", error);
                        }
                    }
                    Some(_) => {}
                }
            }
            if !running {
                break;
            }
            ensure!(Instant::now() < deadline, "detector workers did not stop");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        for reader in self.readers.drain(..) {
            reader
                .join()
                .map_err(|_| anyhow::anyhow!("detector output reader panicked"))?;
        }
        self.shutdown_events(&mut completed)?;
        let failed_workers = self
            .shutdown_failures
            .iter()
            .enumerate()
            .filter(|(_, errors)| !errors.is_empty())
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        if !failed_workers.is_empty() {
            let path = self
                .trace_directory
                .as_ref()
                .context("diagnostic error directory missing")?
                .join("shutdown-failures.json");
            let report = json!({
                "failed_workers": failed_workers, "errors": self.shutdown_failures,
                "completed_workers": completed.iter().flatten().count(), "total_budget_s": 120,
            });
            let bytes = serde_json::to_vec(&report)?;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .with_context(|| {
                    format!("cannot create diagnostic failure report {}", path.display())
                })?;
            file.write_all(&bytes)
                .context("cannot save diagnostic failure report")?;
            file.flush()
                .context("cannot flush diagnostic failure report")?;
            anyhow::bail!(
                "diagnostic cleanup collected failures from workers {failed_workers:?}; details={}",
                path.display()
            );
        }
        if !self.storage_only {
            ensure!(
                completed
                    .iter()
                    .enumerate()
                    .all(|(index, calls)| Some(index) == killed || calls.is_some()),
                "worker exited without confirming its durable shutdown"
            );
            println!(
                "DETECTOR_SHUTDOWN_REPORT {}",
                json!({
                    "completed_workers": completed.iter().flatten().count(),
                    "pending_calls": completed.iter().flatten().sum::<u64>(),
                    "workers_with_pending_calls": completed.iter().flatten().filter(|calls| **calls > 0).count(),
                    "total_budget_s": 120,
                })
            );
        }
        Ok(())
    }

    fn shutdown_events(&mut self, completed: &mut [Option<u64>]) -> anyhow::Result<()> {
        ensure!(
            !self.overflow.load(Ordering::Relaxed),
            "detector event queue overflow during shutdown"
        );
        while let Ok(event) = self.events.try_recv() {
            match event {
                Event::Message(index, value) if value["phase"] == "error" => {
                    let error = format!(
                        "worker {index} failed during shutdown: {value}; stderr={}",
                        self.diagnostic(index)?
                    );
                    if self.trace_directory.is_none() {
                        anyhow::bail!("{error}");
                    }
                    self.shutdown_failures[index]
                        .entry("reported")
                        .or_insert(error);
                }
                Event::Message(index, value) if value["phase"] == "shutdown-complete" => {
                    ensure!(
                        completed[index].is_none(),
                        "duplicate worker shutdown completion"
                    );
                    completed[index] = Some(number(&value, "pending_calls")?);
                }
                Event::Invalid(index, error) => {
                    anyhow::bail!("invalid worker {index} shutdown output: {error}");
                }
                Event::Message(_, _) | Event::Closed(_) => {}
            }
        }
        Ok(())
    }

    async fn usage(&self) -> anyhow::Result<(u64, f64)> {
        let pids = self
            .children
            .iter()
            .map(|process| process.child.id())
            .collect::<Vec<_>>();
        tokio::task::spawn_blocking(move || {
            let list = pids
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(",");
            let output = Command::new("ps")
                .args(["-p", &list, "-o", "pid=,rss=,time="])
                .output()?;
            ensure!(
                output.status.success(),
                "worker ps sampling failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let text = std::str::from_utf8(&output.stdout)?;
            let mut seen = std::collections::BTreeSet::new();
            let mut rss = 0u64;
            let mut cpu = 0.0;
            for line in text.lines() {
                let fields = line.split_whitespace().collect::<Vec<_>>();
                ensure!(fields.len() == 3, "invalid worker resource sample {line}");
                let pid = fields[0].parse::<u32>()?;
                ensure!(
                    pids.contains(&pid) && seen.insert(pid),
                    "unexpected worker PID in resource sample"
                );
                rss = rss
                    .checked_add(
                        fields[1]
                            .parse::<u64>()?
                            .checked_mul(1024)
                            .context("worker RSS overflow")?,
                    )
                    .context("aggregate RSS overflow")?;
                cpu += cpu_seconds(fields[2])?;
            }
            ensure!(
                seen.len() == pids.len(),
                "worker disappeared from resource sample"
            );
            anyhow::Ok((rss, cpu))
        })
        .await?
    }
}

fn remote_timing_summary(mut values: Vec<u64>) -> anyhow::Result<Value> {
    ensure!(!values.is_empty(), "remote profile timing sample missing");
    values.sort_unstable();
    let mut summary = numeric_summary(values.clone());
    summary["count"] = json!(values.len());
    summary["total_us"] = json!(values.iter().sum::<u64>());
    summary["p95"] = json!(values[(values.len() * 95).div_ceil(100) - 1]);
    Ok(summary)
}

async fn remote_history_scenario(
    count: usize,
    trace_directory: Option<std::path::PathBuf>,
) -> anyhow::Result<Value> {
    remote_history_scenario_with_periodic_flush(count, trace_directory, true, false).await
}

async fn remote_history_scenario_with_periodic_flush(
    count: usize,
    trace_directory: Option<std::path::PathBuf>,
    periodic_flush: bool,
    collect_host: bool,
) -> anyhow::Result<Value> {
    ensure!(
        (1..=100).contains(&count),
        "invalid remote profile process count"
    );
    ensure!(
        !collect_host || (cfg!(target_os = "macos") && trace_directory.is_some()),
        "remote host sampling requires macOS and an artifact directory"
    );
    let fixture = remote_history_fixture()?;
    let storage = if periodic_flush {
        StorageWorkload::RemoteHistory
    } else {
        StorageWorkload::RemoteHistoryWithoutPeriodicFlush
    };
    let mut group = Processes::spawn_with_trace(count, Some(storage), trace_directory.clone())?;
    let mut host_tasks = JoinSet::new();
    let result = async {
        let ready = group.phase("ready", None, FORMATION_LIMIT).await?;
        ensure!(ready.iter().all(|row| row["input_digest"] == fixture.digest),
            "remote profile workers have different inputs");
        let periodic_flush_ms = ready[0]["periodic_flush_ms"].clone();
        ensure!(
            ready.iter().all(|row| row["periodic_flush_ms"] == periodic_flush_ms)
                && if periodic_flush {
                    periodic_flush_ms == 500
                } else {
                    periodic_flush_ms.is_null()
                },
            "remote profile actual Sled periodic-flush configuration mismatch"
        );
        if collect_host {
            let output = native_output(Command::new("/usr/bin/vm_stat"), NATIVE_COMMAND_LIMIT).await?;
            save_native_output(
                trace_directory.as_ref().context("remote host directory missing")?,
                "host-vm-stat-before", &output,
            )?;
        }
        let (rss_start, cpu_start) = group.usage().await?;
        let started = StdInstant::now();
        let work_start_epoch_ms = epoch_ms()? + 3000;
        if collect_host {
            host_tasks.spawn(async move {
                let wait = work_start_epoch_ms.checked_sub(epoch_ms()?)
                    .context("remote IO sampler missed scheduled start")?;
                tokio::time::sleep(Duration::from_millis(wait)).await;
                let sample_start_epoch_ms = epoch_ms()?;
                let mut command = Command::new("/usr/sbin/iostat");
                command.args(["-w", "1", "-c", "2"]);
                let output = native_output(command, NATIVE_COMMAND_LIMIT).await?;
                anyhow::Ok((output, json!({
                    "start_epoch_ms": sample_start_epoch_ms,
                    "end_epoch_ms": epoch_ms()?,
                    "interpretation": "first frame since boot; second interval includes profiler/host activity and may extend beyond short data work; not syscall latency or per-worker IO"
                })))
            });
        }
        group.broadcast(&json!({"command": "remote-io", "start_epoch_ms": work_start_epoch_ms}), None)?;
        group.phase("remote-armed", None, Duration::from_secs(3)).await?;
        let mut completed = vec![Value::Null; count];
        let mut rss_peak = rss_start;
        let mut rss_samples = 1;
        let mut last_sample = 0;
        while completed.iter().any(Value::is_null) {
            while let Some((index, row)) = group.receive()? {
                if row["phase"] == "remote-processed" {
                    completed[index] = row;
                }
            }
            ensure!(started.elapsed() < Duration::from_secs(120),
                "remote profile exceeded its separate 120-second diagnostic work budget");
            let tick = started.elapsed().as_secs() / 10;
            if tick > last_sample {
                last_sample = tick;
                rss_peak = rss_peak.max(group.usage().await?.0);
                rss_samples += 1;
            }
            if completed.iter().any(Value::is_null) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        let (rss_end, cpu_end) = group.usage().await?;
        let measured_s = started.elapsed().as_secs_f64();
        let work_complete_epoch_ms = epoch_ms()?;
        ensure!(cpu_end >= cpu_start, "remote profile worker CPU time regressed");
        let bytes = number(&completed[0]["work"], "logical_batch_bytes")?;
        for row in &completed {
            let work = &row["work"];
            ensure!(
                work["input_digest"] == fixture.digest
                    && number(work, "batches")? == 1
                    && number(work, "flushes")? == 1
                    && number(work, "logical_batch_bytes")? == bytes
                    && number(work, "durable_ops")? == 237,
                "remote profile persistence quantities are not equivalent per worker"
            );
        }
        // No worker exports while another worker is still measuring storage.
        group.broadcast(&json!({"command": "report"}), None)?;
        let reports = group.phase("remote-report", None, DETECTION_LIMIT).await?;
        let mut all = BTreeMap::<String, Vec<u64>>::new();
        let mut workers = Vec::new();
        for (index, row) in reports.iter().enumerate() {
            let mut timings = BTreeMap::<String, Value>::new();
            for (stage, values) in row["timings_us"].as_object()
                .context("remote profile timing report missing")? {
                let values = values.as_array().context("invalid remote profile timing array")?
                    .iter().map(|value| value.as_u64().context("invalid remote profile timing"))
                    .collect::<anyhow::Result<Vec<_>>>()?;
                all.entry(stage.clone()).or_default().extend(&values);
                timings.insert(stage.clone(), remote_timing_summary(values)?);
            }
            workers.push(json!({"worker": index, "work": completed[index]["work"],
                "timings_us": timings}));
        }
        let timings = all.into_iter().map(|(stage, values)| {
            Ok((stage, remote_timing_summary(values)?))
        }).collect::<anyhow::Result<BTreeMap<_, _>>>()?;
        Ok(json!({
            "version": 1, "processes": count, "network": false, "detector_enabled": false,
            "seed": REMOTE_IO_SEED, "input_digest": fixture.digest,
            "prefix_ops_per_worker": 200, "origins_per_worker": 100,
            "envelopes_per_worker": 64, "input_entries_per_worker": 111,
            "new_ops_per_worker": 37, "already_persisted_copies_per_worker": 74,
            "batches_per_worker": 1, "logical_batch_bytes_per_worker": bytes,
            "flushes_per_worker": 1, "timings_us": timings, "workers": workers,
            "periodic_flush_ms": periodic_flush_ms,
            "work_start_epoch_ms": work_start_epoch_ms,
            "work_complete_epoch_ms": work_complete_epoch_ms,
            "resources": {"scope": "worker processes only; includes coordinated start waiting and verification, excludes prefix setup, timeline export and coordinator; RSS double-counts shared pages",
                "cpu_seconds": cpu_end - cpu_start, "cpu_measurement_window_s": measured_s,
                "cpu_percent_one_core": (cpu_end - cpu_start) / measured_s * 100.0,
                "rss_sum_start_bytes": rss_start, "rss_sum_end_bytes": rss_end,
                "rss_sum_sampled_peak_bytes": rss_peak.max(rss_end), "rss_samples": rss_samples + 1,
                "sampling_s": 10},
            "interpretation": "controlled real remote apply/storage fixture; not detector accuracy, a nominal campaign, or byte-for-byte replay of historical worker 14"
        }))
    }.await;
    let host_result = async {
        let mut samples = Vec::new();
        while let Some(output) = host_tasks.join_next().await {
            let (output, sample) = output.context("remote host sampler task failed")??;
            save_native_output(
                trace_directory
                    .as_ref()
                    .context("remote host directory missing")?,
                "host-iostat",
                &output,
            )?;
            samples.push(sample);
        }
        if collect_host {
            let output =
                native_output(Command::new("/usr/bin/vm_stat"), NATIVE_COMMAND_LIMIT).await?;
            save_native_output(
                trace_directory
                    .as_ref()
                    .context("remote host directory missing")?,
                "host-vm-stat-after",
                &output,
            )?;
        }
        anyhow::Ok(samples)
    }
    .await;
    let cleanup = group.stop(None).await;
    match (result, host_result, cleanup) {
        (Ok(mut report), Ok(host), Ok(())) => {
            if collect_host {
                report["host_sampling"] = json!(host);
            }
            report["reopen_verified_workers"] = json!(count);
            Ok(report)
        }
        (result, host, cleanup) => anyhow::bail!(
            "remote profile result: {:#?}; host sampling: {:#?}; durable shutdown/reopen: {:#?}",
            result.err(),
            host.err(),
            cleanup.err()
        ),
    }
}

fn remote_profile_console(report: &Value) -> anyhow::Result<String> {
    let compact = report
        .as_object()
        .context("remote profile console report is not an object")?
        .iter()
        .filter(|(key, _)| key.as_str() != "workers")
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<serde_json::Map<_, _>>();
    let output = serde_json::to_string(&compact)?;
    ensure!(
        output.len() <= 16 * 1024,
        "remote profile console summary exceeds its 16 KiB budget"
    );
    Ok(output)
}

fn ensure_remote_history_equivalent(left: &Value, right: &Value) -> anyhow::Result<()> {
    for field in [
        "seed",
        "input_digest",
        "prefix_ops_per_worker",
        "origins_per_worker",
        "envelopes_per_worker",
        "input_entries_per_worker",
        "new_ops_per_worker",
        "already_persisted_copies_per_worker",
        "batches_per_worker",
        "logical_batch_bytes_per_worker",
        "flushes_per_worker",
    ] {
        let left = left
            .get(field)
            .with_context(|| format!("left remote case missing {field}"))?;
        let right = right
            .get(field)
            .with_context(|| format!("right remote case missing {field}"))?;
        ensure!(left == right, "remote cases differ in {field}");
    }
    Ok(())
}

#[test]
fn periodic_flush_equivalence_rejects_changed_or_missing_quantities() {
    let report = json!({
        "seed": REMOTE_IO_SEED, "input_digest": "same",
        "prefix_ops_per_worker": 200, "origins_per_worker": 100,
        "envelopes_per_worker": 64, "input_entries_per_worker": 111,
        "new_ops_per_worker": 37, "already_persisted_copies_per_worker": 74,
        "batches_per_worker": 1, "logical_batch_bytes_per_worker": 14346,
        "flushes_per_worker": 1,
    });
    ensure_remote_history_equivalent(&report, &report).unwrap();
    for field in report.as_object().unwrap().keys() {
        let mut changed = report.clone();
        changed[field] = Value::Null;
        assert!(ensure_remote_history_equivalent(&report, &changed).is_err());
        changed.as_object_mut().unwrap().remove(field);
        assert!(ensure_remote_history_equivalent(&report, &changed).is_err());
    }
}

#[cfg(target_os = "macos")]
fn save_remote_case(path: &std::path::Path, report: &Value) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec(report)?;
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)?;
    file.write_all(&bytes)?;
    Ok(())
}

#[cfg(target_os = "macos")]
pub(super) async fn periodic_flush_comparison(
    count: usize,
    pairs: usize,
    directory: &std::path::Path,
) -> anyhow::Result<Value> {
    ensure!(
        matches!(count, 3 | 100) && (1..=3).contains(&pairs) && directory.is_dir(),
        "periodic-flush comparison requires 3/100 workers, 1..3 pairs and an existing directory"
    );
    let mut pair_reports = Vec::new();
    for pair in 0..pairs {
        let mut reports = BTreeMap::new();
        let order = if pair % 2 == 0 {
            [true, false]
        } else {
            [false, true]
        };
        for periodic_flush in order {
            let mode = if periodic_flush {
                "a-default"
            } else {
                "b-disabled"
            };
            let path = directory.join(format!("pair-{}-{mode}", pair + 1));
            std::fs::create_dir(&path)
                .context("cannot exclusively create periodic-flush case directory")?;
            let result = remote_history_scenario_with_periodic_flush(
                count,
                Some(path.clone()),
                periodic_flush,
                true,
            )
            .await;
            let report = match result {
                Ok(report) => report,
                Err(error) => {
                    save_remote_case(
                        &path.join("case-failure.json"),
                        &json!({
                            "pair": pair + 1, "mode": mode, "processes": count,
                            "error": format!("{error:#}"), "nominal_detector_requirement_closed": false,
                        }),
                    )?;
                    return Err(error)
                        .with_context(|| format!("periodic-flush pair {} mode {mode}", pair + 1));
                }
            };
            save_remote_case(&path.join("case-report.json"), &report)?;
            reports.insert(periodic_flush, report);
        }
        let a = reports
            .get(&true)
            .context("periodic-flush A report missing")?;
        let b = reports
            .get(&false)
            .context("periodic-flush B report missing")?;
        ensure_remote_history_equivalent(a, b)?;
        ensure!(
            a["processes"] == b["processes"]
                && a["reopen_verified_workers"] == count
                && b["reopen_verified_workers"] == count,
            "periodic-flush pair count or durable reopen mismatch"
        );
        pair_reports.push(json!({
            "pair": pair + 1, "order": if pair % 2 == 0 { "A/B" } else { "B/A" },
            "processes_per_case": count, "input_digest": a["input_digest"],
            "logical_batch_bytes_per_worker": a["logical_batch_bytes_per_worker"],
            "a_periodic_flush_ms": a["periodic_flush_ms"],
            "b_periodic_flush_ms": b["periodic_flush_ms"],
            "a_timings_us": a["timings_us"], "b_timings_us": b["timings_us"],
            "reopen_verified_workers_per_case": count,
        }));
    }
    let report = json!({
        "version": 1, "processes_per_case": count, "pairs": pair_reports,
        "seed": REMOTE_IO_SEED, "fixture_and_durable_equivalence_passed": true,
        "nominal_detector_requirement_closed": false,
        "interpretation": "diagnostic A/B of the whole periodic flush/GC service; explicit foreground flushes unchanged; no speedup assertion or production setting change; short host intervals may extend beyond data work",
    });
    save_remote_case(&directory.join("periodic-flush-comparison.json"), &report)?;
    println!(
        "PERIODIC_FLUSH_AB_REPORT {}",
        remote_profile_console(&report)?
    );
    Ok(report)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn periodic_flush_explicit_ack_survives_process_crash() {
    let name = "sync_manager::manager::tests::detector_soak::process::periodic_flush_explicit_ack_survives_process_crash";
    if child(name, Duration::from_secs(180)) {
        return;
    }
    let fixture = remote_history_fixture().unwrap();
    for storage in [
        StorageWorkload::RemoteHistory,
        StorageWorkload::RemoteHistoryWithoutPeriodicFlush,
    ] {
        let mut group = Processes::spawn_with_trace(1, Some(storage), None).unwrap();
        let result = async {
            let ready = group.phase("ready", None, FORMATION_LIMIT).await?;
            let expected = if matches!(storage, StorageWorkload::RemoteHistory) {
                json!(500)
            } else {
                Value::Null
            };
            ensure!(
                ready[0]["periodic_flush_ms"] == expected,
                "crash worker actual periodic mode mismatch"
            );
            group.broadcast(
                &json!({"command": "remote-io", "start_epoch_ms": epoch_ms()? + 3000}),
                None,
            )?;
            group
                .phase("remote-armed", None, Duration::from_secs(3))
                .await?;
            let completed = group
                .phase("remote-processed", None, DETECTION_LIMIT)
                .await?;
            ensure!(
                completed[0]["work"]["flushes"] == 1,
                "crash worker explicit flush acknowledgement missing"
            );
            group.children[0].child.kill()?;
            group.children[0].child.wait()?;
            let reopened = NxStore::open(group._directory.path().join("store-0"))?;
            verify_remote_history_store(&reopened, &fixture.expected)?;
            anyhow::Ok(())
        }
        .await;
        let cleanup = group.stop(Some(0)).await;
        result.unwrap();
        cleanup.unwrap();
    }
}

#[test]
fn remote_profile_output_keeps_isolated_result_within_capture_limit() {
    let name = "sync_manager::manager::tests::detector_soak::process::remote_profile_output_keeps_isolated_result_within_capture_limit";
    if child(name, Duration::from_secs(30)) {
        return;
    }
    let report = |count| {
        json!({
            "processes": count, "timings_us": {"flush": {"count": count, "median": 123}},
            "workers": (0..count).map(|index| json!({"worker": index, "detail": "x".repeat(1024)}))
                .collect::<Vec<_>>(),
        })
    };
    assert!(serde_json::to_vec(&report(100)).unwrap().len() > 64 * 1024);
    let marker = "test result: ok. 1 passed; 0 failed;";
    let reports = format!(
        "REMOTE_HISTORY_PROFILE_REPORT {}\nREMOTE_HISTORY_PROFILE_REPORT {}\n",
        remote_profile_console(&report(1)).unwrap(),
        remote_profile_console(&report(100)).unwrap(),
    );
    let output = format!("{reports}{marker}\n");
    let captured = String::from_utf8_lossy(&output.as_bytes()[..output.len().min(64 * 1024)]);
    assert!(
        output.len() < 64 * 1024 && captured.lines().any(|line| line == marker),
        "remote profile output hides the isolated test result: {} bytes",
        output.len()
    );
    let compact: Value =
        serde_json::from_str(&remote_profile_console(&report(100)).unwrap()).unwrap();
    assert_eq!(compact["processes"], 100);
    assert!(compact.get("workers").is_none());
    assert!(remote_profile_console(&Value::Null).is_err());
    print!("{reports}");
}

pub(super) async fn remote_history_comparison() -> anyhow::Result<()> {
    let directory = std::env::var_os(TIMELINE_DIR_ENV)
        .map(std::path::PathBuf::from)
        .context("remote comparison requires NUMAX_DETECTOR_TRACE_DIR")?;
    ensure!(
        directory.is_dir(),
        "remote comparison artifact directory does not exist"
    );
    let mut reports = Vec::new();
    for count in [1, 100] {
        let path = directory.join(format!("processes-{count}"));
        std::fs::create_dir(&path)
            .context("cannot exclusively create remote comparison case directory")?;
        let report = remote_history_scenario(count, Some(path)).await?;
        println!(
            "REMOTE_HISTORY_PROFILE_REPORT {}",
            remote_profile_console(&report)?
        );
        reports.push(report);
    }
    ensure_remote_history_equivalent(&reports[0], &reports[1])?;
    println!(
        "REMOTE_HISTORY_COMPARISON {}",
        json!({
            "passed_fixture_and_equivalence": true,
            "flush_median_us_1": reports[0]["timings_us"]["flush"]["median"],
            "flush_median_us_100": reports[1]["timings_us"]["flush"]["median"],
            "logical_batch_bytes_per_worker": reports[0]["logical_batch_bytes_per_worker"],
            "input_digest": reports[0]["input_digest"],
            "nominal_detector_requirement_closed": false,
        })
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_history_three_process_fixture() {
    let name = "sync_manager::manager::tests::detector_soak::process::remote_history_three_process_fixture";
    if child(name, Duration::from_secs(180)) {
        return;
    }
    let report = remote_history_scenario(3, None).await.unwrap();
    assert_eq!(report["processes"], 3);
    assert_eq!(report["timings_us"]["flush"]["count"], 3);
    println!("REMOTE_HISTORY_SMOKE_REPORT {report}");
}

pub(super) async fn scenario(
    count: usize,
    duration: Duration,
    period: Duration,
) -> anyhow::Result<()> {
    run_scenario(count, duration, period, None, false).await
}

pub(super) async fn convergence_scenario(
    count: usize,
    duration: Duration,
    period: Duration,
) -> anyhow::Result<()> {
    run_scenario(count, duration, period, None, true).await
}

#[cfg(target_os = "macos")]
pub(super) async fn nominal_stall_scenario() -> anyhow::Result<()> {
    let directory = std::env::var_os(STALL_DIR_ENV)
        .map(std::path::PathBuf::from)
        .context("NUMAX_DETECTOR_STALL_DIR must name an existing artifact directory")?;
    ensure!(
        cfg!(target_os = "macos") && directory.is_dir(),
        "native stall capture requires macOS and an existing artifact directory"
    );
    run_scenario_with_stall_capture(
        100,
        Duration::from_secs(3600),
        Duration::from_secs(10),
        None,
        false,
        Some(StallCaptureOptions {
            directory: &directory,
            controlled_pause: false,
            critical_path: false,
            fail_native: false,
        }),
    )
    .await
}

#[cfg(target_os = "macos")]
pub(super) async fn native_stall_capture_smoke() -> anyhow::Result<()> {
    for (controlled_pause, critical_path) in
        [(true, false), (false, false), (true, true), (false, true)]
    {
        let directory = tempfile::tempdir()?;
        run_scenario_with_stall_capture(
            3,
            if controlled_pause {
                Duration::from_secs(40)
            } else {
                Duration::from_secs(8)
            },
            if controlled_pause {
                Duration::from_secs(10)
            } else {
                Duration::from_secs(1)
            },
            None,
            false,
            Some(StallCaptureOptions {
                directory: directory.path(),
                controlled_pause,
                critical_path,
                fail_native: false,
            }),
        )
        .await?;
        let report: Value =
            serde_json::from_slice(&std::fs::read(directory.path().join("stall-report.json"))?)?;
        if controlled_pause {
            ensure!(
                report["outcome"] == "diagnostic_interrupted"
                    && report["nominal_complete"] == false,
                "controlled pause incorrectly certified the nominal load"
            );
            let workers = report["workers"]
                .as_array()
                .context("native smoke workers missing")?;
            ensure!(
                !workers.is_empty() && workers.len() <= 3,
                "native smoke capture cardinality"
            );
            if critical_path {
                ensure!(
                    workers
                        .iter()
                        .all(|worker| worker["critical_before"].is_object()
                            && worker["critical_after"].is_object()
                            && worker["critical_before"]["span_limit"] == 64),
                    "bounded on-demand critical snapshots missing"
                );
                ensure!(
                    report["native_export_error"].is_null(),
                    "deferred native export failed"
                );
            }
            ensure!(
                workers
                    .iter()
                    .any(|worker| worker["index"] == 0
                        && worker["same_flush_at_last_samples"] == true),
                "paused live flush not correlated"
            );
            let stack = std::fs::read_to_string(directory.path().join("worker-0.sample.stdout"))?;
            ensure!(
                stack.contains("flush_unlocked") && stack.contains("mpmc"),
                "real worker flush wait not visible"
            );
        } else {
            ensure!(
                report["outcome"] == "no_stall_observed"
                    && report["nominal_complete"] == !critical_path,
                "no-stall diagnostic outcome incorrect"
            );
        }
    }
    println!(
        "NATIVE_CAPTURE_PROCESS_SMOKE controlled_pause=true no_stall=true critical_path=true forced_reap_complete=true"
    );
    Ok(())
}

#[cfg(target_os = "macos")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn critical_capture_preserves_snapshots_and_completed_outputs_on_native_failure()
-> anyhow::Result<()> {
    let name = "sync_manager::manager::tests::detector_soak::process::critical_capture_preserves_snapshots_and_completed_outputs_on_native_failure";
    if child(name, Duration::from_secs(120)) {
        return Ok(());
    }
    let directory = tempfile::tempdir()?;
    let result = run_scenario_with_stall_capture(
        3,
        Duration::from_secs(40),
        Duration::from_secs(10),
        None,
        false,
        Some(StallCaptureOptions {
            directory: directory.path(),
            controlled_pause: true,
            critical_path: true,
            fail_native: true,
        }),
    )
    .await;
    ensure!(result.is_err(), "failed native command incorrectly passed");
    let report: Value =
        serde_json::from_slice(&std::fs::read(directory.path().join("stall-report.json"))?)?;
    ensure!(
        report["outcome"] == "diagnostic_failed"
            && report["nominal_complete"] == false
            && report["cleanup_error"].is_null()
            && report["native_export_error"].is_null(),
        "failed capture lifecycle/result misreported"
    );
    let workers = report["workers"]
        .as_array()
        .context("failed capture dropped workers")?;
    ensure!(
        !workers.is_empty()
            && workers.len() <= 3
            && workers
                .iter()
                .all(|worker| worker["critical_before"].is_object()
                    && worker["critical_after"].is_object()),
        "native failure discarded already collected snapshots"
    );
    let error = report["execution_error"]
        .as_str()
        .context("capture error missing")?;
    ensure!(
        error.contains("/usr/bin/false")
            && error.contains("target_worker=0")
            && error.contains("target_pid="),
        "failing native command identity missing"
    );
    ensure!(
        !std::fs::read(directory.path().join("host-vm-stat.stdout"))?.is_empty()
            && !std::fs::read(directory.path().join("host-iostat.stdout"))?.is_empty(),
        "native failure discarded completed bounded outputs"
    );
    println!(
        "CRITICAL_CAPTURE_FAILURE_REGRESSION snapshots_retained=true completed_outputs_retained=true command_identified=true reaped=true"
    );
    Ok(())
}

pub(super) async fn storage_scenario(
    count: usize,
    duration: Duration,
    period: Duration,
) -> anyhow::Result<()> {
    run_scenario(
        count,
        duration,
        period,
        Some(StorageWorkload::Counter),
        false,
    )
    .await
}

pub(super) async fn storage_metadata_scenario(
    count: usize,
    duration: Duration,
    period: Duration,
) -> anyhow::Result<()> {
    run_scenario(
        count,
        duration,
        period,
        Some(StorageWorkload::Metadata),
        false,
    )
    .await
}

pub(super) async fn storage_batched_metadata_scenario(
    count: usize,
    duration: Duration,
    period: Duration,
) -> anyhow::Result<()> {
    run_scenario(
        count,
        duration,
        period,
        Some(StorageWorkload::MetadataBatched),
        false,
    )
    .await
}

async fn run_scenario(
    count: usize,
    duration: Duration,
    period: Duration,
    storage: Option<StorageWorkload>,
    verify_convergence: bool,
) -> anyhow::Result<()> {
    run_scenario_with_stall_capture(count, duration, period, storage, verify_convergence, None)
        .await
}

struct StallCaptureOptions<'a> {
    directory: &'a std::path::Path,
    controlled_pause: bool,
    critical_path: bool,
    fail_native: bool,
}

#[cfg(target_os = "macos")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "bounded real 100-process diagnostic; requires NUMAX_DETECTOR_STALL_DIR"]
async fn critical_path_100_processes() {
    let name = "sync_manager::manager::tests::detector_soak::process::critical_path_100_processes";
    if child(name, Duration::from_secs(600)) {
        return;
    }
    let directory = std::env::var_os(STALL_DIR_ENV)
        .map(std::path::PathBuf::from)
        .expect("NUMAX_DETECTOR_STALL_DIR must name an existing artifact directory");
    assert!(directory.is_dir());
    run_scenario_with_stall_capture(
        100,
        Duration::from_secs(300),
        Duration::from_secs(10),
        None,
        false,
        Some(StallCaptureOptions {
            directory: &directory,
            controlled_pause: false,
            critical_path: true,
            fail_native: false,
        }),
    )
    .await
    .unwrap();
}

async fn run_scenario_with_stall_capture(
    count: usize,
    duration: Duration,
    period: Duration,
    storage: Option<StorageWorkload>,
    verify_convergence: bool,
    capture: Option<StallCaptureOptions<'_>>,
) -> anyhow::Result<()> {
    let capture_directory = capture.as_ref().map(|options| options.directory);
    let controlled_pause = capture
        .as_ref()
        .is_some_and(|options| options.controlled_pause);
    let critical_path = capture
        .as_ref()
        .is_some_and(|options| options.critical_path);
    let fail_native = capture.as_ref().is_some_and(|options| options.fail_native);
    ensure!(
        !fail_native || (critical_path && controlled_pause && count == 3),
        "native failure injection is restricted to the controlled three-worker regression"
    );
    ensure!(
        !critical_path
            || (storage.is_none() && !verify_convergence && duration <= Duration::from_secs(300)),
        "critical path must remain a bounded diagnostic, not a nominal certification"
    );
    ensure!(
        !controlled_pause || (capture_directory.is_some() && count == 3 && storage.is_none()),
        "controlled flush pause is restricted to the three-worker native smoke"
    );
    let storage_only = storage.is_some();
    let storage_metadata = matches!(
        storage,
        Some(StorageWorkload::Metadata | StorageWorkload::MetadataBatched)
    );
    let metadata_batched = matches!(storage, Some(StorageWorkload::MetadataBatched));
    ensure!(
        period.as_secs() > 0 && duration.as_secs().is_multiple_of(period.as_secs()),
        "invalid multiprocess nominal period"
    );
    let operations = duration.as_secs() / period.as_secs();
    let final_drain = drain_limit(period)?;
    println!(
        "{} {}",
        if storage_only {
            "STORAGE_BASELINE_CONFIG"
        } else {
            "DETECTOR_CONFIG"
        },
        json!({
            "version": 2, "nodes": count, "worker_processes": count,
            "codec": if storage_only { None } else { Some("Json") },
            "storage_only": storage_only, "metadata_traffic": storage_metadata, "metadata_batched": metadata_batched, "detector_enabled": !storage_only,
            "storage_workload": match storage {
                Some(StorageWorkload::Counter) => Some("one eight-byte counter; atomic increment batch and durable flush per request"),
                Some(StorageWorkload::Metadata) => Some("five new 2KiB records plus an eight-byte counter; six atomic batches then durable flush per request"),
                Some(StorageWorkload::MetadataBatched) => Some("five new 2KiB records plus an eight-byte counter; one atomic batch then durable flush per request"),
                Some(StorageWorkload::RemoteHistory | StorageWorkload::RemoteHistoryWithoutPeriodicFlush) => anyhow::bail!("remote history requires its dedicated comparison scenario"),
                None => None,
            },
            "nominal_duration_s": duration.as_secs(), "period_s_per_node": period.as_secs(),
            "native_stall_diagnostic": capture_directory.is_some(),
            "native_stall_trigger_us": capture_directory.map(|_| STALL_TRIGGER_US),
            "controlled_flush_pause": controlled_pause,
            "critical_path_enabled": critical_path, "critical_span_limit": if critical_path { Some(64) } else { None },
            "offered_ops_per_s": count as f64 / period.as_secs_f64(),
            "max_pending_per_node": 4, "final_drain_limit_s": final_drain.as_secs(),
            "max_data_peers_per_node": if storage_only { None } else { Some(4) },
            "detector_timing": if storage_only { None } else { Some("unmodified production defaults") },
            "probe_seed": if storage_only { None } else { Some(DETECTOR_PROBE_SEED) },
            "randomness": if storage_only { None } else { Some("probe choices use a reported fixed test seed; wall-clock scheduling remains nondeterministic") }
        })
    );
    let mut group = if controlled_pause || critical_path {
        Processes::spawn_with_options(count, storage, None, controlled_pause, critical_path)?
    } else {
        Processes::spawn(count, storage)?
    };
    let mut killed = None;
    let mut completed_nominal_report = None;
    let mut native_capture = NativeCapture::default();
    let result = async {
        let ready = group.phase("ready", None, FORMATION_LIMIT).await?;
        if !storage_only {
        let owners = ready.iter().map(|value| value["owner"].clone()).collect::<Vec<_>>();
        group.broadcast(&json!({"command": "bootstrap", "owners": owners}), None)?;
        group.phase("bootstrapped", None, FORMATION_LIMIT).await?;
        let formation_deadline = Instant::now() + FORMATION_LIMIT;
        loop {
            while group.receive()?.is_some() {}
            if group.rows().iter().all(|value| value["records"].as_u64() == Some(count as u64) && value["all_alive"] == true) { break; }
            ensure!(Instant::now() < formation_deadline, "multiprocess cluster did not form");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        }
        eprintln!("DETECTOR_PHASE nodes={count} processes={count} phase={} nominal_s={}",
            if storage_only { "storage-ready" } else { "formed" }, duration.as_secs());
        if let Some(directory) = capture_directory {
            let output = native_output(Command::new("/usr/bin/vm_stat"), NATIVE_COMMAND_LIMIT).await?;
            save_native_output(directory, "host-vm-stat-before-nominal", &output)?;
        }
        let (rss_start, cpu_start) = group.usage().await?;
        let started = StdInstant::now();
        group.broadcast(&json!({"command": "nominal", "duration_s": duration.as_secs(), "period_s": period.as_secs(), "start_epoch_ms": epoch_ms()? + 3000}), None)?;
        group.phase("nominal-armed", None, Duration::from_secs(3)).await?;
        let mut completed = vec![Value::Null; count];
        let mut peak_queues = BTreeMap::<String, u64>::new();
        let mut rss_peak = rss_start;
        let mut resource_samples = 1u64;
        let mut last_sample = 0;
        let mut last_progress = 0;
        while completed.iter().any(Value::is_null) {
            while let Some((index, value)) = group.receive()? {
                if value["phase"] == "nominal-complete" { completed[index] = value; }
            }
            if completed.iter().all(|value| !value.is_null()) { break; }
            if critical_path && started.elapsed() >= duration {
                native_capture.report = Some(json!({
                    "outcome": "no_stall_observed", "nominal_complete": false,
                    "diagnostic_window_complete": true, "diagnostic_window_s": duration.as_secs(),
                    "critical_path_enabled": true, "trigger_oldest_submission_us": STALL_TRIGGER_US,
                    "status_samples": group.rows(),
                    "interpretation": "bounded diagnostic stopped at its ceiling; no final drain, replica health, true-failure phase or one-hour certification",
                }));
                return anyhow::Ok(());
            }
            if let Some(directory) = capture_directory
                && !stall_targets(&group.rows())?.is_empty()
            {
                capture_stall(&mut group, directory, started.elapsed(), critical_path, fail_native, &mut native_capture).await?;
                while group.receive()?.is_some() {}
                return anyhow::Ok(());
            }
            ensure!(started.elapsed() < duration + final_drain + Duration::from_secs(5), "nominal workers exceeded observation window");
            let mut queues = BTreeMap::<String, u64>::new();
            for status in group.rows().iter().filter(|value| !value.is_null()) {
                for (key, value) in status["queues"].as_object().context("worker queue sample missing")? {
                    *queues.entry(key.clone()).or_default() += value.as_u64().context("invalid worker queue value")?;
                }
            }
            for (key, value) in queues { peak_queues.entry(key).and_modify(|peak| *peak = (*peak).max(value)).or_insert(value); }
            let elapsed = started.elapsed().as_secs();
            if elapsed / 10 > last_sample {
                last_sample = elapsed / 10;
                rss_peak = rss_peak.max(group.usage().await?.0);
                resource_samples += 1;
            }
            if elapsed / 60 > last_progress {
                last_progress = elapsed / 60;
                println!("DETECTOR_PROGRESS {}", json!({"elapsed_s": elapsed, "nodes": count, "completed": completed.iter().filter(|value| !value.is_null()).count(), "rss_sum_sampled_peak_bytes": rss_peak}));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let (rss_end, cpu_end) = group.usage().await?;
        ensure!(cpu_end >= cpu_start, "aggregate worker CPU time regressed");
        let measured_seconds = started.elapsed().as_secs_f64();
        let mut nominal_counts = Counts::default();
        let mut accepted = Vec::new();
        let mut accepted_in_window = Vec::new();
        let mut max_submit_us = 0;
        let mut max_writer_lateness_us = 0;
        let mut max_pending_observed = 0;
        for value in &completed {
            ensure!(value["elapsed_s"].as_f64().context("worker elapsed time missing")? >= duration.as_secs_f64(), "short nominal worker window");
            if !storage_only {
                nominal_counts.add(&Counts::from_raw(&value["status"]["nominal_counters"])?);
            }
            accepted.push(number(&value["status"], "accepted")?);
            accepted_in_window.push(number(&value["status"], "accepted_in_window")?);
            max_submit_us = max_submit_us.max(number(&value["status"], "max_flush_us")?);
            max_writer_lateness_us = max_writer_lateness_us.max(number(&value["status"], "max_lateness_us")?);
            max_pending_observed = max_pending_observed.max(number(&value["status"], "max_pending")?);
            ensure!(number(&value["status"], "offered")? == operations, "offered load mismatch");
        }
        ensure!(accepted.iter().all(|accepted| *accepted == operations), "nominal load was not achieved");
        ensure!(nominal_counts.declared_dead == 0 && nominal_counts.learned_dead == 0, "nominal false failures");
        let observer_hours = completed.iter().map(|value| value["elapsed_s"].as_f64().context("missing observed duration"))
            .collect::<anyhow::Result<Vec<_>>>()?.iter().sum::<f64>() / 3600.0;
        let nominal = json!({
            "diagnostics": if storage_only { Value::Null } else { diagnostics::aggregate(completed.iter().map(|value| &value["status"]))? },
            "requested_duration_s": duration.as_secs(), "worker_elapsed_s": completed.iter().map(|value| value["elapsed_s"].clone()).collect::<Vec<_>>(),
            "counts": if storage_only { Value::Null } else { nominal_counts.json(observer_hours) }, "observer_hours": observer_hours,
            "detector_enabled": !storage_only, "storage_only": storage_only,
            "accepted_per_node": accepted, "accepted_total": accepted.iter().sum::<u64>(),
            "accepted_within_nominal_window_per_node": accepted_in_window,
            "confirmed_ops_per_s_within_nominal_window": accepted_in_window.iter().sum::<u64>() as f64 / duration.as_secs_f64(),
            "offered_ops_per_s": count as f64 / period.as_secs_f64(), "period_s_per_node": period.as_secs(),
            "max_pending_per_node": 4, "final_drain_limit_s": final_drain.as_secs(),
            "max_submit_us": max_submit_us, "max_writer_lateness_us": max_writer_lateness_us,
            "max_pending_observed_per_node": max_pending_observed,
            "resources": {"scope": "sum of worker process samples; coordinator excluded; RSS sums count shared pages more than once",
                "rss_sum_start_bytes": rss_start, "rss_sum_end_bytes": rss_end,
                "rss_sum_sampled_peak_bytes": rss_peak.max(rss_end), "rss_samples": resource_samples + 1,
                "sampling_s": 10, "cpu_seconds": cpu_end - cpu_start,
                "cpu_measurement_window_s": measured_seconds, "cpu_percent_one_core": (cpu_end - cpu_start) / measured_seconds * 100.0,
                "sampled_aggregate_queue_peaks_and_rejection_totals": peak_queues}
        });
        if storage_only {
            println!("STORAGE_BASELINE_REPORT {}", json!({
                "version": 1, "processes": count, "passed_nominal": true, "metadata_traffic": storage_metadata, "metadata_batched": metadata_batched, "nominal": nominal,
                "interpretation": "durable storage baseline only; no membership, replication or failure-detector validation"
            }));
            return anyhow::Ok(());
        }
        completed_nominal_report = Some(nominal.clone());
        println!("DETECTOR_NOMINAL_REPORT {nominal}");
        if capture_directory.is_some() {
            native_capture.report = Some(json!({
                "outcome": "no_stall_observed", "nominal_complete": !critical_path,
                "diagnostic_window_complete": critical_path, "critical_path_enabled": critical_path,
                "trigger_oldest_submission_us": STALL_TRIGGER_US,
                "nominal": nominal,
                "interpretation": "no qualifying stall observed; the stall hypothesis remains indeterminate; no post-load health or true-failure phase was executed",
            }));
            return anyhow::Ok(());
        }
        if verify_convergence {
            let started = StdInstant::now();
            group.broadcast(&json!({"command": "convergence"}), None)?;
            let converged = match group.phase("converged", None, CONVERGENCE_LIMIT).await {
                Ok(converged) => converged,
                Err(error) => {
                    let samples = group
                        .rows()
                        .into_iter()
                        .enumerate()
                        .filter_map(|(worker, row)| {
                            let sample = row.get("convergence")?;
                            sample
                                .is_object()
                                .then(|| (worker, sample.clone()))
                        })
                        .collect::<Vec<_>>();
                    println!(
                        "DETECTOR_CONVERGENCE_FAILURE_REPORT {}",
                        convergence_failure_report(&samples, count)?
                    );
                    return Err(error).context("convergence failure with bounded all-worker diagnostics");
                }
            };
            ensure!(started.elapsed() < CONVERGENCE_LIMIT, "cluster convergence exceeded its declared limit");
            let expected = operations.checked_mul(u64::try_from(count)?).context("convergence total overflow")?;
            ensure!(converged.iter().all(|value| value["value"] == expected
                && value["origins"] == count && value["operations_per_origin"] == operations),
                "incomplete cluster convergence report");
            println!("DETECTOR_CONVERGENCE_REPORT {}", json!({
                "nodes": count, "value_per_node": expected, "origins_per_node": count,
                "operations_per_origin": operations, "elapsed_ms": started.elapsed().as_millis(),
                "limit_s": CONVERGENCE_LIMIT.as_secs(), "new_writes": false,
                "checked": ["in_memory", "materialized", "durable_state", "contiguous_receipts"],
            }));
        }
        let target = count - 1;
        let target_id = format!("detector-{target}");
        group.broadcast(&json!({"command": "failure", "target": target_id}), Some(target))?;
        let armed = group.phase("failure-armed", Some(target), Duration::from_secs(5)).await?;
        let fault_started = StdInstant::now();
        group.children[target].child.kill()?;
        group.children[target].child.wait()?;
        killed = Some(target);
        let confirmed_ms = fault_started.elapsed().as_millis();
        let mut suspect = vec![None; target];
        let mut dead = vec![None; target];
        let mut observers = vec![Value::Null; target];
        while dead.iter().any(Option::is_none) {
            ensure!(fault_started.elapsed() <= DETECTION_LIMIT, "not every survivor detected the crashed worker");
            match group.events.try_recv() {
                Ok(Event::Closed(index)) if index == target => {}
                Ok(Event::Message(index, value)) => {
                    ensure!(value["phase"] != "error", "worker {index} failed during crash detection: {value}");
                    if index != target && value["phase"] == "failure-state" {
                        let state = value["state"].as_str().context("failure state missing")?;
                        if state == "Some(Suspect)" && suspect[index].is_none() { suspect[index] = Some(fault_started.elapsed().as_millis()); }
                        if state == "Some(Dead)" && dead[index].is_none() {
                            dead[index] = Some(fault_started.elapsed().as_millis());
                            let counts = Counts::from_raw(&value["status"]["counters"])?.delta(&Counts::from_raw(&armed[index]["counters"])?);
                            observers[index] = json!({"node": format!("detector-{index}"), "suspect_observed_ms": suspect[index],
                                "dead_observed_ms": dead[index], "local_declarations": counts.declared_dead, "learned_dead": counts.learned_dead});
                        }
                    }
                }
                Ok(Event::Closed(index)) => anyhow::bail!("surviving worker {index} exited: {}", group.diagnostic(index)?),
                Ok(Event::Invalid(index, error)) => anyhow::bail!("invalid worker {index} output: {error}"),
                Err(TryRecvError::Empty) => tokio::time::sleep(Duration::from_millis(10)).await,
                Err(TryRecvError::Disconnected) => anyhow::bail!("all worker readers exited during detection"),
            }
            ensure!(!group.overflow.load(Ordering::Relaxed), "detector event queue overflow during detection");
        }
        ensure!(observers.iter().any(|value| value["local_declarations"].as_u64().is_some_and(|count| count > 0)), "no local declaration of true failure");
        let failure = json!({"fault": "kill and reap one actual worker process without graceful leave",
            "target": target_id, "outage_confirmed_ms": confirmed_ms, "observers": observers,
            "sampling_ms": 100, "latency_reference": "fault request to coordinator receipt of sampled state; includes IPC/collection delay",
            "limit_s": DETECTION_LIMIT.as_secs(), "new_writes": false});
        println!("DETECTOR_FAILURE_REPORT {failure}");
        println!("DETECTOR_REPORT {}", json!({"version": 2, "nodes": count, "worker_processes": count,
            "codec": "Json", "platform": std::env::consts::OS,
            "topology": "candidate ring, full membership through bounded real owner Exchange setup, four data peers maximum per node",
            "detector": "production timing and OS randomness; not deterministic replay",
            "workload": {"family": "GCounter", "key": KEY, "increment": 1, "key_bytes": KEY.len(),
                "target_offered_ops_per_s_per_node": 1.0 / period.as_secs_f64()},
            "transport": "real unauthenticated loopback TCP; no injected packet loss",
            "retention_operations_per_node": crate::sync_config::DEFAULT_OP_LOG_LIMIT,
            "nominal": nominal, "true_failure": failure}));
        anyhow::Ok(())
    }.await;
    if let Some(directory) = capture_directory {
        let last_samples = group.rows();
        let cleanup = group.force_stop_diagnostic().await;
        let mut report = match native_capture.report {
            Some(report) => report,
            None => {
                let error = result.as_ref().err().context(
                    "diagnostic returned success without a capture or explicit no-stall report",
                )?;
                json!({
                    "outcome": "diagnostic_failed",
                    "nominal_complete": !critical_path && completed_nominal_report.is_some(),
                    "error": format!("{error:#}"),
                    "status_samples": last_samples,
                    "interpretation": "diagnostic execution failed; no nominal success or one-hour rate inferred",
                })
            }
        };
        if let Err(error) = &result {
            report["outcome"] = json!("diagnostic_failed");
            report["error"] = json!(format!("{error:#}"));
            report["status_samples"] = json!(last_samples);
        }
        report["execution_error"] = json!(result.as_ref().err().map(|error| format!("{error:#}")));
        report["cleanup_error"] = json!(cleanup.as_ref().err().map(|error| format!("{error:#}")));
        let export = native_capture
            .outputs
            .iter()
            .try_for_each(|output| save_native_output(directory, &output.name, &output.output));
        report["native_export_error"] =
            json!(export.as_ref().err().map(|error| format!("{error:#}")));
        let bytes = serde_json::to_vec(&report)?;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(directory.join("stall-report.json"))?;
        file.write_all(&bytes)?;
        println!(
            "DETECTOR_STALL_REPORT {}",
            json!({
                "outcome": report["outcome"], "nominal_complete": report["nominal_complete"],
                "directory": directory, "workers_captured": report["workers"].as_array().map(Vec::len),
                "forced_stop": true, "cleanup_ok": cleanup.is_ok(),
            })
        );
        ensure!(
            result.is_ok() && cleanup.is_ok() && export.is_ok(),
            "native stall diagnostic={result:?}; cleanup={cleanup:?}; export={export:?}"
        );
        return Ok(());
    }
    if let Err(error) = &result {
        let samples = group.rows();
        let rows = samples
            .iter()
            .filter(|value| !value.is_null())
            .collect::<Vec<_>>();
        let nominal_rows = nominal_status_samples(&rows);
        let mut counters = Counts::default();
        let mut accepted = 0u64;
        let mut max_flush = 0u64;
        let mut max_lateness = 0u64;
        for row in &nominal_rows {
            if !storage_only {
                counters.add(&Counts::from_raw(&row["nominal_counters"])?);
            }
            accepted += number(row, "accepted")?;
            max_flush = max_flush.max(number(row, "max_flush_us")?);
            max_lateness = max_lateness.max(number(row, "max_lateness_us")?);
        }
        println!(
            "{} {}",
            if storage_only {
                "STORAGE_BASELINE_PARTIAL_REPORT"
            } else {
                "DETECTOR_PARTIAL_REPORT"
            },
            json!({
                "diagnostics": if storage_only || nominal_rows.is_empty() { Value::Null } else { diagnostics::aggregate(nominal_rows.iter().copied())? },
                "completed_nominal_report": completed_nominal_report.as_ref().map(|nominal| json!({
                    "accepted_total": nominal["accepted_total"],
                    "accepted_within_nominal_window_per_node": nominal["accepted_within_nominal_window_per_node"],
                    "confirmed_ops_per_s_within_nominal_window": nominal["confirmed_ops_per_s_within_nominal_window"],
                    "counts": nominal["counts"],
                    "max_pending_observed_per_node": nominal["max_pending_observed_per_node"]
                })),
                "passed": false, "error": format!("{error:#}"), "nodes": count, "storage_only": storage_only,
                "metadata_traffic": storage_metadata,
                "metadata_batched": metadata_batched,
                "worker_processes": count, "requested_nominal_duration_s": duration.as_secs(), "period_s_per_node": period.as_secs(),
                "status_samples_available": rows.len(), "counts_at_last_worker_samples": if storage_only { Value::Null } else { counters.raw() },
                "status_samples_with_nominal_counters": nominal_rows.len(),
                "status_samples_without_nominal_counters": rows.len().saturating_sub(nominal_rows.len()),
                "accepted_at_last_samples": accepted, "max_flush_us_at_last_samples": max_flush,
                "max_writer_lateness_us_at_last_samples": max_lateness,
                "interpretation": "last received asynchronous samples, not a completed nominal window; no success or one-hour rate inferred"
            })
        );
    }
    let shutdown = group.stop(killed).await;
    ensure!(
        result.is_ok() && shutdown.is_ok(),
        "multiprocess detector={result:?}; shutdown={shutdown:?}"
    );
    Ok(())
}

#[test]
fn periodic_status_is_coalesced_without_discarding_critical_events() {
    let (events, receiver) = std::sync::mpsc::sync_channel(1);
    let (latest, view) = watch::channel(Value::Null);
    let overflow = AtomicBool::new(false);
    for sequence in 0..1000 {
        assert!(record_message(
            0,
            json!({"phase": "status", "sequence": sequence}),
            &latest,
            &events,
            &overflow
        ));
    }

    assert_eq!(view.borrow()["sequence"], 999);
    assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));
    assert!(record_message(
        0,
        json!({"phase": "error", "error": "regression"}),
        &latest,
        &events,
        &overflow
    ));
    assert!(
        matches!(receiver.try_recv(), Ok(Event::Message(0, value)) if value["error"] == "regression")
    );
    assert!(!overflow.load(Ordering::Relaxed));
    assert!(record_message(
        0,
        json!({"phase": "nominal-complete", "status": {"accepted": 3}}),
        &latest,
        &events,
        &overflow
    ));
    assert_eq!(view.borrow()["accepted"], 3);
    assert!(!record_message(
        0,
        json!({"phase": "error"}),
        &latest,
        &events,
        &overflow
    ));
    assert!(overflow.load(Ordering::Relaxed));
}

#[test]
fn partial_report_aggregates_only_statuses_with_nominal_counters() {
    let nominal = json!({"nominal_counters": {"suspect": 0}});
    let convergence = json!({"convergence": {"value": 173}});
    let rows = vec![&nominal, &convergence];

    assert_eq!(nominal_status_samples(&rows), vec![&nominal]);
}

#[test]
fn recovery_diagnostic_examples_are_bounded_and_counted() {
    let incomplete_receipts = (0..100).map(|index| {
        json!({
            "origin": format!("detector-{index}"),
            "contiguous": 2,
            "known_high": 3,
            "sparse": 0,
            "source_unavailable": true,
            "baseline_missing": false,
            "baseline_unconfirmed": false,
            "recovery_failure": false,
        })
    });
    let pending_ranges = (0..100).map(|index| {
        json!({
            "origin": format!("detector-{index}"),
            "start": 2,
            "end": 3,
            "remaining_ms": 10_000,
        })
    });
    let (incomplete_count, incomplete_samples) = bounded_diagnostic_samples(incomplete_receipts);
    let (pending_count, pending_samples) = bounded_diagnostic_samples(pending_ranges);

    assert_eq!(incomplete_count, 100);
    assert_eq!(incomplete_samples.len(), MAX_RECOVERY_DETAIL_SAMPLES);
    assert_eq!(pending_count, 100);
    assert_eq!(pending_samples.len(), MAX_RECOVERY_DETAIL_SAMPLES);

    let line = format!(
        "{PREFIX}{}",
        json!({
            "incomplete_receipt_count": incomplete_count,
            "incomplete_receipts": incomplete_samples,
            "pending_range_details_count": pending_count,
            "pending_ranges": pending_samples,
        })
    );
    assert!(line.len() < MAX_WORKER_OUTPUT_LINE_BYTES);
}

#[tokio::test]
async fn recovery_sample_releases_requests_for_queued_writer() {
    use std::future::Future;
    use std::task::{Context, Waker};

    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(NxStore::open(directory.path()).unwrap());
    let (mut node, _) = started_manager_with_store(
        NodeId::new("diagnostic-reader"),
        SyncConfig::new().with_listen_addr(free_addr()),
        store,
    )
    .await;
    let now = node.clock.now();
    let pending_count = MAX_RECOVERY_DETAIL_SAMPLES + 2;
    let mut entries = HashMap::new();
    for index in 0..=pending_count {
        entries.insert(
            format!("origin-{index}"),
            crate::sync_manager::types::PendingHistoryRequest {
                request_id: nx_sync::OpId::new(format!("request-{index}")),
                start: 1,
                end: 3,
                baseline_revision: None,
                deadline: if index == pending_count {
                    now
                } else {
                    now + node.config.socket_timeout
                },
            },
        );
    }
    node.history_requests
        .write()
        .await
        .insert("source".into(), entries);
    let diagnostics = diagnostics::Diagnostics::default();
    let baseline = diagnostics::Baseline::new(&node, &diagnostics).unwrap();
    let origins = std::collections::HashSet::new();
    let mut writer = Box::pin(node.history_requests.write());
    let mut queued = false;
    let sample = tokio::time::timeout(
        Duration::from_secs(2),
        recovery_progress_sample(
            &node,
            &diagnostics,
            &baseline,
            Duration::ZERO,
            0,
            0,
            RecoveryProgressInputs {
                expected_origins: &origins,
                operations: 0,
                progress: &[],
                after_requests_read: Some(Box::new(|| {
                    // Polling registers this writer in Tokio's FIFO lock queue.
                    // No scheduler timing or sleep is needed to force the interleaving.
                    assert!(
                        writer
                            .as_mut()
                            .poll(&mut Context::from_waker(Waker::noop()))
                            .is_pending()
                    );
                    queued = true;
                })),
            },
        ),
    )
    .await;
    assert!(
        queued,
        "writer was not queued under the sampler's read guard"
    );
    let mut guard = tokio::time::timeout(Duration::from_secs(2), writer)
        .await
        .expect("sampler retained its read guard");
    guard.clear();
    drop(guard);
    node.shutdown().await.unwrap();
    let sample = sample
        .expect("sampler blocked behind its own queued writer")
        .unwrap();
    assert_eq!(sample["range_requests_pending"], pending_count);
    assert_eq!(sample["range_requests_expired"], 1);
    assert_eq!(sample["pending_range_details_count"], pending_count);
    let details = sample["pending_ranges"].as_array().unwrap();
    assert_eq!(details.len(), MAX_RECOVERY_DETAIL_SAMPLES);
    assert!(
        details
            .iter()
            .all(|entry| entry["start"] == 1 && entry["end"] == 3)
    );
}
