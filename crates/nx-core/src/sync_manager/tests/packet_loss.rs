use super::*;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::Command;

const GROUP_CONFIG: &str = "NUMAX_PACKET_LOSS_GROUPS";
const LOAD_ROUNDS: usize = 3;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "four real workers writing during recovery; packet-loss runner required"]
async fn groups_load_smoke() {
    let name = "sync_manager::manager::tests::detector_soak::packet_loss::groups_load_smoke";
    if child(name, Duration::from_secs(600)) {
        return;
    }
    scenario_with_load(4, Duration::from_secs(3), Some(Duration::from_secs(1)))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "50 real workers writing for 30 seconds during recovery; packet-loss runner required"]
async fn partition_50_nodes_with_load() {
    let name =
        "sync_manager::manager::tests::detector_soak::packet_loss::partition_50_nodes_with_load";
    if child(name, Duration::from_secs(900)) {
        return;
    }
    scenario_with_load(50, Duration::from_secs(60), Some(Duration::from_secs(10)))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "four real workers in private namespaces; packet-loss runner required"]
async fn groups_smoke() {
    let name = "sync_manager::manager::tests::detector_soak::packet_loss::groups_smoke";
    if child(name, Duration::from_secs(600)) {
        return;
    }
    scenario(4, Duration::from_secs(3)).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "50 real workers, 60-second kernel partition; packet-loss runner required"]
async fn partition_50_nodes() {
    let name = "sync_manager::manager::tests::detector_soak::packet_loss::partition_50_nodes";
    if child(name, Duration::from_secs(900)) {
        return;
    }
    scenario(50, Duration::from_secs(60)).await.unwrap();
}

fn namespace_command(path: &str, program: &str, args: &[&str]) -> anyhow::Result<Vec<u8>> {
    let current = std::fs::read_link("/proc/self/ns/net")?;
    let target = std::fs::read_link(path)?;
    ensure!(
        current != target,
        "refusing group configuration in coordinator namespace"
    );
    let output = Command::new("nsenter")
        .arg(format!("--net={path}"))
        .arg("--")
        .arg(program)
        .args(args)
        .output()?;
    ensure!(
        output.status.success(),
        "namespace command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(output.stdout)
}

fn set_loss(paths: &[String], loss: u64) -> anyhow::Result<()> {
    for path in paths {
        let interfaces: Value =
            serde_json::from_slice(&namespace_command(path, "ip", &["-j", "link", "show"])?)?;
        ensure!(
            interfaces.as_array().context("group links missing")?.len() == 2
                && interfaces
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|entry| entry["ifname"] == "nx-link"),
            "expected isolated loopback/veth group"
        );
        namespace_command(
            path,
            "tc",
            &[
                "qdisc",
                "change",
                "dev",
                "nx-link",
                "root",
                "netem",
                "loss",
                "random",
                &format!("{loss}%"),
            ],
        )?;
    }
    Ok(())
}

fn snapshot(paths: &[String], directory: &Path, phase: &str) -> anyhow::Result<()> {
    for (group, path) in paths.iter().enumerate() {
        std::fs::write(
            directory.join(format!("group-{group}-{phase}-qdisc.json")),
            namespace_command(path, "tc", &["-j", "-s", "qdisc", "show", "dev", "nx-link"])?,
        )?;
        std::fs::write(
            directory.join(format!("group-{group}-{phase}-tcp.json")),
            namespace_command(path, "nstat", &["-a", "-z", "-j"])?,
        )?;
    }
    Ok(())
}

async fn scenario(count: usize, partition_duration: Duration) -> anyhow::Result<()> {
    scenario_with_load(count, partition_duration, None).await
}

async fn scenario_with_load(
    count: usize,
    partition_duration: Duration,
    load_period: Option<Duration>,
) -> anyhow::Result<()> {
    let mut config: Value = serde_json::from_str(
        &std::env::var(GROUP_CONFIG).context("packet-loss runner configuration required")?,
    )?;
    let paths: Vec<String> = serde_json::from_value(config["namespaces"].clone())?;
    ensure!(
        paths.len() == 2 && paths[0] != paths[1],
        "two distinct group namespaces required"
    );
    let loss = config["loss_percent"]
        .as_u64()
        .context("loss probability missing")?;
    ensure!(loss == 0 || loss == 10, "unsupported packet-loss envelope");
    let directory = PathBuf::from(
        config["directory"]
            .as_str()
            .context("artifact directory missing")?,
    );
    ensure!(directory.is_dir(), "artifact directory missing");
    let addresses: Vec<_> = (0..count)
        .map(|index| format!("10.200.0.{}:{}", 1 + index / (count / 2), 19000 + index))
        .collect();
    config["addresses"] = json!(addresses);
    set_loss(&paths, loss)?;
    snapshot(&paths, &directory, "initial")?;
    let mut processes = process::Processes::spawn_packet_loss(count, &config)?;
    let result = async {
        let ready = processes.phase("ready", None, FORMATION_LIMIT).await?;
        let owners: Vec<_> = ready.iter().map(|row| row["owner"].clone()).collect();
        processes.broadcast(&json!({"command": "bootstrap", "owners": owners}), None)?;
        processes.phase("bootstrapped", None, FORMATION_LIMIT).await?;
        let formation = StdInstant::now();
        loop {
            processes.broadcast(&json!({"command": "inspect"}), None)?;
            let rows = processes.phase("inspected", None, Duration::from_secs(10)).await?;
            if rows.iter().all(|row| row["members"] == count && row["all_alive"] == true) {
                break;
            }
            ensure!(formation.elapsed() < FORMATION_LIMIT, "healthy membership formation exceeded limit");
        }
        snapshot(&paths, &directory, "before-partition")?;
        set_loss(&paths, 100)?;
        let partition_started = Instant::now();
        let partition_wall = StdInstant::now();
        processes.broadcast(&json!({"command": "write"}), None)?;
        let written = processes.phase("written", None, partition_duration).await?;
        ensure!(written.iter().all(|row| row["accepted"] == 12), "incomplete flush-confirmed workload");
        tokio::time::sleep_until(partition_started + partition_duration).await;
        ensure!(partition_wall.elapsed() >= partition_duration, "partition was shorter than requested");
        let actual_partition_ms = partition_wall.elapsed().as_millis();
        snapshot(&paths, &directory, "end-partition")?;
        let recovery = StdInstant::now();
        set_loss(&paths, loss)?;
        snapshot(&paths, &directory, "after-rejoin")?;
        let mut backlog_elapsed_ms = None;
        let mut accepted_at_backlog_samples = 0u64;
        let mut load_reports = Vec::new();
        let mut final_recovery = None;
        if let Some(period) = load_period {
            processes.broadcast(&json!({"command": "load", "period_ms": period.as_millis()}), None)?;
            processes.phase("load-started", None, Duration::from_secs(10)).await?;
            processes.broadcast(&json!({"command": "backlog"}), None)?;
            let backlog = processes.phase("backlog-recovered", None, CONVERGENCE_LIMIT).await?;
            ensure!(recovery.elapsed() < CONVERGENCE_LIMIT, "initial backlog recovery exceeded 30 seconds from rejoin");
            ensure!(backlog.iter().all(|row| row["writer_active"] == true), "initial backlog was not verified while writers were active");
            for row in &backlog {
                accepted_at_backlog_samples += row["load_accepted"].as_u64().context("backlog load count missing")?;
            }
            ensure!(accepted_at_backlog_samples > 0, "no new flush-confirmed writes overlapped backlog observation");
            backlog_elapsed_ms = Some(recovery.elapsed().as_millis());
            snapshot(&paths, &directory, "after-backlog")?;
            processes.broadcast(&json!({"command": "finish-load"}), None)?;
            load_reports = processes.phase("loaded", None, period * LOAD_ROUNDS as u32 + CONVERGENCE_LIMIT).await?;
            final_recovery = Some(StdInstant::now());
            ensure!(load_reports.iter().all(|row| row["accepted"] == LOAD_ROUNDS * 12), "incomplete recovery-load workload");
            snapshot(&paths, &directory, "after-load")?;
        }
        processes.broadcast(&json!({"command": "verify"}), None)?;
        let converged = processes.phase("converged", None, CONVERGENCE_LIMIT).await?;
        let state_field = if load_period.is_some() { "state_digest" } else { "states" };
        ensure!(converged.iter().all(|row| if load_period.is_some() { row[state_field].is_string() } else { row[state_field].is_array() }), "durable verification evidence missing");
        ensure!(converged.iter().all(|row| row[state_field] == converged[0][state_field]), "durable six-family states diverged");
        let recovery_elapsed = final_recovery.as_ref().unwrap_or(&recovery).elapsed();
        ensure!(recovery_elapsed < CONVERGENCE_LIMIT, "recovery exceeded its 30-second verification limit");
        snapshot(&paths, &directory, "after-recovery")?;
        let rounds = if load_period.is_some() { 1 + LOAD_ROUNDS } else { 1 };
        let report = json!({"version": 1, "nodes": count, "groups": [count / 2, count / 2],
            "codec": config["codec"], "accepted_operations": count * 12 * rounds, "families": 6,
            "initial_operations": count * 12, "load_operations": count * 12 * (rounds - 1),
            "partition_duration_ms": actual_partition_ms,
            "recovery_elapsed_ms": recovery_elapsed.as_millis(), "recovery_limit_s": 30,
            "recovery_clock": if load_period.is_some() { "final verification after writer completion" } else { "first rejoin command" },
            "backlog_recovery_elapsed_ms": backlog_elapsed_ms, "backlog_recovery_limit_s": 30,
            "load_period_ms": load_period.map(|period| period.as_millis()),
            "load_window_ms": load_period.map(|period| (period * LOAD_ROUNDS as u32).as_millis()),
            "load_target_operations_per_second": load_period.map(|period| count as f64 * 12.0 / period.as_secs_f64()),
            "durable_state_digest": converged[0]["state_digest"],
            "accepted_at_backlog_samples": accepted_at_backlog_samples,
            "load_reports": load_reports,
            "loss_percent": loss, "probe_seed": 5644502651023458305u64, "packet_loss_seed": null,
            "writes_after_rejoin": load_period.is_some(), "verified": ["in_memory", "materialized", "durable_state", "contiguous_receipts"],
            "worker_receipts": converged.iter().map(|row| row["receipt_summary"].clone()).collect::<Vec<_>>(), "passed": true});
        std::fs::write(directory.join("cluster-report.json"), serde_json::to_vec_pretty(&report)?)?;
        println!("PACKET_LOSS_CLUSTER_REPORT {report}");
        anyhow::Ok(())
    }.await;
    let restored = set_loss(&paths, loss);
    let stopped = processes.stop(None).await;
    ensure!(
        result.is_ok() && restored.is_ok() && stopped.is_ok(),
        "scenario={result:?}; restore={restored:?}; shutdown={stopped:?}"
    );
    Ok(())
}

pub(super) async fn worker(config: &Value, store: Arc<NxStore>) -> anyhow::Result<()> {
    let index = usize::try_from(config["index"].as_u64().context("worker index missing")?)?;
    let addresses: Vec<String> = serde_json::from_value(config["addresses"].clone())?;
    let count = addresses.len();
    let format = match config["codec"].as_str() {
        Some("json") => SerializationFormat::Json,
        Some("bincode") => SerializationFormat::Bincode,
        _ => anyhow::bail!("unsupported packet codec"),
    };
    let mut node = SyncManager::try_new(
        NodeId::new(format!("detector-{index}")),
        SyncConfig::new()
            .with_listen_addr(addresses[index].clone())
            .with_max_peers(4)
            .with_serialization_format(format)
            .with_peer(addresses[(index + count - 1) % count].clone())
            .with_peer(addresses[(index + 1) % count].clone()),
        store,
        metrics(),
    )?;
    node.start().await?;
    let (mut commands, input) = process::command_input();
    let mut writer = None;
    let load_accepted = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut workload_rounds = 1usize;
    let result = async {
        let mut view = node.membership.as_ref().context("membership missing")?.snapshot.clone();
        let snapshot = view.wait_for(|snapshot| snapshot.records.iter().any(|record| record.member == node.node_id.as_str())).await?;
        let owner = snapshot.records.iter().find(|record| record.member == node.node_id.as_str()).context("owner missing")?.clone();
        drop(snapshot);
        process::emit(json!({"phase": "ready", "owner": owner}));
        while let Some(command) = commands.recv().await {
            let command = command?;
            match command["command"].as_str() {
                Some("bootstrap") => {
                    let owners: Vec<nx_net::MembershipRecord> = serde_json::from_value(command["owners"].clone())?;
                    ensure!(owners.len() == count, "owner count mismatch");
                    let bootstrap_deadline = Instant::now() + FORMATION_LIMIT;
                    for round in 1..count {
                        let target = &owners[(index + round) % count];
                        let request = nx_net::ControlRequest { request_id: 1_000_000 + (round * count + index) as u64,
                            owner: owner.clone(), updates: vec![], kind: nx_net::ControlKind::Exchange { after: None } };
                        loop {
                            match exchange_owner(node.node.as_ref().context("transport missing")?, target.endpoint.as_ref().context("endpoint missing")?, request.clone(), &target.member).await {
                                Ok(_) => break,
                                Err(error) if matches!(error.downcast_ref::<nx_net::NetError>(), Some(nx_net::NetError::Timeout)) => {
                                    ensure!(Instant::now() < bootstrap_deadline, "packet bootstrap exhausted formation budget: {error:#}");
                                }
                                Err(error) => return Err(error),
                            }
                        }
                    }
                    process::emit(json!({"phase": "bootstrapped"}));
                }
                Some("inspect") => {
                    let snapshot = node.membership.as_ref().context("membership missing")?.snapshot.borrow();
                    process::emit(json!({"phase": "inspected", "members": snapshot.records.len(),
                        "all_alive": snapshot.records.iter().all(|record| record.state == MembershipState::Alive)}));
                }
                Some("write") => {
                    local_six_family_workload(&node.handle(), index as u64).await;
                    process::emit(json!({"phase": "written", "accepted": 12}));
                }
                Some("load") => {
                    ensure!(writer.is_none() && workload_rounds == 1, "writer already started");
                    let period = Duration::from_millis(command["period_ms"].as_u64().context("load period missing")?);
                    ensure!(period == Duration::from_secs(1) || period == Duration::from_secs(10), "unsupported load period");
                    writer = Some(tokio::spawn(write_during_recovery(node.handle(), index, count, period, load_accepted.clone())));
                    process::emit(json!({"phase": "load-started"}));
                }
                Some("backlog") => {
                    let started = StdInstant::now();
                    let mut poll = tokio::time::interval(Duration::from_millis(50));
                    loop {
                        let task = writer.as_ref().context("backlog observation requires active writer")?;
                        if task.is_finished() {
                            let completed = writer.take().context("writer missing")?.await.context("recovery writer failed")?;
                            anyhow::bail!("writer ended before backlog recovery: {completed:?}");
                        }
                        let progress = node.handle().replication_progress()?;
                        if progress.len() == count && progress.iter().all(|entry| {
                            (0..count).any(|origin| entry.origin.as_str() == format!("detector-{origin}"))
                                && entry.contiguous_sequence >= 12 && !entry.source_unavailable
                                && !entry.baseline_missing && !entry.baseline_unconfirmed && entry.recovery_failure.is_none()
                        }) {
                            ensure!(started.elapsed() < CONVERGENCE_LIMIT, "initial backlog verification exceeded limit");
                            process::emit(json!({"phase": "backlog-recovered", "writer_active": true,
                                "load_accepted": load_accepted.load(std::sync::atomic::Ordering::Relaxed)}));
                            break;
                        }
                        ensure!(started.elapsed() < CONVERGENCE_LIMIT, "worker {index} initial backlog recovery timed out: {progress:?}");
                        poll.tick().await;
                    }
                }
                Some("finish-load") => {
                    let report = writer.take().context("writer missing")?.await.context("recovery writer failed")??;
                    workload_rounds += LOAD_ROUNDS;
                    process::emit(report);
                }
                Some("verify") => {
                    ensure!(writer.is_none(), "final verification requires stopped writer");
                    let started = StdInstant::now();
                    let mut poll = tokio::time::interval(Duration::from_millis(50));
                    loop {
                        let progress = node.handle().replication_progress()?;
                        let covered = progress.len() == count && progress.iter().all(|entry| {
                            (0..count).any(|origin| entry.origin.as_str() == format!("detector-{origin}"))
                                && entry.contiguous_sequence == (workload_rounds * 12) as u64 && entry.known_high_sequence == (workload_rounds * 12) as u64
                                && entry.sparse_positions == 0 && !entry.source_unavailable
                                && !entry.baseline_missing && !entry.baseline_unconfirmed && entry.recovery_failure.is_none()
                        });
                        let expected = (count * workload_rounds) as u64;
                        let expected_pncounter = i64::try_from(count * workload_rounds)? * 5;
                        if covered && node.get_counter_value("det-g").await == expected * 7
                            && node.get_pncounter_value("det-p").await == expected_pncounter
                            && node.get_lww_register_value("det-r").await == Some(b"winner".to_vec())
                            && node.get_lww_map_entries("det-m").await.is_empty()
                            && node.get_orset_elements("det-o").await == vec!["present"]
                            && node.get_rga_values("det-a").await == vec![b"present".to_vec(); count * workload_rounds] {
                            ensure!(read_materialized(&node.store, "det-g") == expected * 7
                                && read_materialized_pncounter(&node.store, "det-p") == expected_pncounter
                                && read_materialized_lww_register(&node.store, "det-r") == b"winner"
                                && read_materialized_lww_map(&node.store, "det-m").is_empty()
                                && read_materialized_orset(&node.store, "det-o") == vec!["present"]
                                && read_materialized_rga(&node.store, "det-a") == vec![b"present".to_vec(); count * workload_rounds], "materialized state mismatch");
                            let states = six_family_states(&node.store);
                            ensure!(started.elapsed() < CONVERGENCE_LIMIT, "durable verification exceeded recovery limit");
                            let mut report = json!({"phase": "converged",
                                "receipt_summary": {"node": index, "origins": count, "sequence_per_origin": workload_rounds * 12, "sparse_positions": 0},
                                "elapsed_ms": started.elapsed().as_millis()});
                            if workload_rounds == 1 {
                                report["states"] = json!(states);
                            } else {
                                report["state_digest"] = json!(blake3::hash(&serde_json::to_vec(&states)?).to_hex().to_string());
                            }
                            process::emit(report);
                            break;
                        }
                        ensure!(started.elapsed() < CONVERGENCE_LIMIT, "worker {index} recovery timed out: {progress:?}");
                        poll.tick().await;
                    }
                }
                Some("stop") => break,
                _ => anyhow::bail!("unknown packet worker command"),
            }
        }
        anyhow::Ok(())
    }.await;
    if let Some(task) = writer.take() {
        task.abort();
        let _ = task.await;
    }
    let shutdown =
        process::finish_shutdown(&mut node, Instant::now() + Duration::from_secs(120)).await;
    if let Ok(pending_calls) = shutdown {
        process::emit(json!({"phase": "shutdown-complete", "pending_calls": pending_calls}));
    }
    if result.is_ok() {
        input
            .join()
            .map_err(|_| anyhow::anyhow!("packet worker input reader panicked"))?;
    }
    ensure!(
        result.is_ok() && shutdown.is_ok(),
        "packet worker={result:?}; shutdown={shutdown:?}"
    );
    Ok(())
}

async fn write_during_recovery(
    handle: SyncHandle,
    index: usize,
    count: usize,
    period: Duration,
    accepted: Arc<std::sync::atomic::AtomicU64>,
) -> anyhow::Result<Value> {
    let started = Instant::now();
    let wall = StdInstant::now();
    let window = period * LOAD_ROUNDS as u32;
    let offset = period.mul_f64(index as f64 / count as f64);
    let mut maximum_lateness_us = 0u128;
    let mut maximum_batch_us = 0u128;
    for round in 0..LOAD_ROUNDS {
        let scheduled = started + offset + period * round as u32;
        tokio::time::sleep_until(scheduled).await;
        let lateness = Instant::now().saturating_duration_since(scheduled);
        ensure!(
            lateness < period,
            "worker {index} missed load generation period"
        );
        maximum_lateness_us = maximum_lateness_us.max(lateness.as_micros());
        let batch = StdInstant::now();
        tokio::time::timeout(
            period,
            local_six_family_workload(&handle, (index + count * (round + 1)) as u64),
        )
        .await
        .context("recovery writer batch exceeded one period")?;
        accepted.fetch_add(12, std::sync::atomic::Ordering::Relaxed);
        maximum_batch_us = maximum_batch_us.max(batch.elapsed().as_micros());
        ensure!(
            Instant::now() <= started + window,
            "worker {index} load confirmation exceeded active window"
        );
    }
    tokio::time::sleep_until(started + window).await;
    ensure!(wall.elapsed() >= window, "writer window ended early");
    Ok(
        json!({"phase": "loaded", "node": index, "offered": LOAD_ROUNDS * 12,
        "accepted": accepted.load(std::sync::atomic::Ordering::Relaxed),
        "window_elapsed_ms": wall.elapsed().as_millis(), "maximum_lateness_us": maximum_lateness_us,
        "maximum_batch_us": maximum_batch_us, "pending_batch_limit": 1}),
    )
}
