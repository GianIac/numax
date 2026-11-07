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

#[derive(Clone, Copy)]
enum StorageWorkload {
    Counter,
    Metadata,
    MetadataBatched,
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
        "max_flush_us": stats.max_flush_us.load(Ordering::Relaxed),
        "max_lateness_us": stats.max_start_lateness_us.load(Ordering::Relaxed)
    }))
}

struct RecoveryProgressInputs<'a> {
    expected_origins: &'a std::collections::HashSet<String>,
    operations: u64,
    progress: &'a [crate::sync_manager::ReplicationProgress],
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

    let requests = node.history_requests.read().await;
    let requests = requests
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
        node.history_requests
            .read()
            .await
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
        count > 1 && index < count,
        "invalid detector worker configuration"
    );
    let directory = config["directory"]
        .as_str()
        .context("worker datastore path missing")?;
    let store = Arc::new(NxStore::open(directory)?);
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
    let diagnostics = diagnostics::Diagnostics::default();
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
                                        },
                                    ).await?;
                                    progress_samples.push(sample.clone());
                                    emit(json!({"phase": "status", "convergence": sample}));
                                    next_diagnostic = elapsed.saturating_add(Duration::from_secs(5));
                                }
                                if value == expected && receipts_complete {
                                    let verified_at = elapsed;
                                    ensure!(read_materialized(&node.store, KEY) == expected, "convergence materialized value mismatch");
                                    ensure!(read_durable_gcounter_state(&node.store, KEY).value() == expected, "convergence durable state mismatch");
                                    ensure!(Instant::now() < deadline,
                                        "convergence exceeded its declared limit: value={value}/{expected}, complete_origins={}, receipt_check_ms={}, durable_check_ms={}",
                                        progress.len(), verified_at.as_millis(), started.elapsed().saturating_sub(verified_at).as_millis());
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
        result.is_ok() && shutdown.is_ok(),
        "worker={result:?}; shutdown={shutdown:?}"
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

impl Processes {
    fn spawn(count: usize, storage: Option<StorageWorkload>) -> anyhow::Result<Self> {
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
            _directory: directory,
        };
        for index in 0..count {
            let stderr = group
                ._directory
                .path()
                .join(format!("worker-{index}.stderr"));
            let config = json!({"index": index, "addresses": addresses, "storage_only": storage.is_some(),
                "storage_metadata": matches!(storage, Some(StorageWorkload::Metadata | StorageWorkload::MetadataBatched)),
                "metadata_batched": matches!(storage, Some(StorageWorkload::MetadataBatched)),
                "directory": group._directory.path().join(format!("store-{index}"))});
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
            if Some(index) != killed && self.children[index].input.is_some() {
                self.command(index, &json!({"command": "stop"}))?;
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
                    Some(status) => ensure!(
                        status.success() || Some(pid) == killed_pid,
                        "worker {index} shutdown failed: {status}; stderr={}",
                        self.diagnostic(index)?
                    ),
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
                    anyhow::bail!(
                        "worker {index} failed during shutdown: {value}; stderr={}",
                        self.diagnostic(index)?
                    );
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
                None => None,
            },
            "nominal_duration_s": duration.as_secs(), "period_s_per_node": period.as_secs(),
            "offered_ops_per_s": count as f64 / period.as_secs_f64(),
            "max_pending_per_node": 4, "final_drain_limit_s": final_drain.as_secs(),
            "max_data_peers_per_node": if storage_only { None } else { Some(4) },
            "detector_timing": if storage_only { None } else { Some("unmodified production defaults") },
            "probe_seed": if storage_only { None } else { Some(DETECTOR_PROBE_SEED) },
            "randomness": if storage_only { None } else { Some("probe choices use a reported fixed test seed; wall-clock scheduling remains nondeterministic") }
        })
    );
    let mut group = Processes::spawn(count, storage)?;
    let mut killed = None;
    let mut completed_nominal_report = None;
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
