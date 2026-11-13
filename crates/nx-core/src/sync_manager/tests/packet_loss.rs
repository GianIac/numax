use super::*;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::Command;

const GROUP_CONFIG: &str = "NUMAX_PACKET_LOSS_GROUPS";

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
        processes.broadcast(&json!({"command": "verify"}), None)?;
        let converged = processes.phase("converged", None, CONVERGENCE_LIMIT).await?;
        ensure!(converged.iter().all(|row| row["states"] == converged[0]["states"]), "durable six-family states diverged");
        let recovery_elapsed = recovery.elapsed();
        ensure!(recovery_elapsed < CONVERGENCE_LIMIT, "recovery exceeded 30 seconds from rejoin");
        snapshot(&paths, &directory, "after-recovery")?;
        let report = json!({"version": 1, "nodes": count, "groups": [count / 2, count / 2],
            "codec": config["codec"], "accepted_operations": count * 12, "families": 6,
            "partition_duration_ms": actual_partition_ms,
            "recovery_elapsed_ms": recovery_elapsed.as_millis(), "recovery_limit_s": 30,
            "loss_percent": loss, "probe_seed": 5644502651023458305u64, "packet_loss_seed": null,
            "writes_after_rejoin": false, "verified": ["in_memory", "materialized", "durable_state", "contiguous_receipts"],
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
                Some("verify") => {
                    let started = StdInstant::now();
                    let mut poll = tokio::time::interval(Duration::from_millis(50));
                    loop {
                        let progress = node.handle().replication_progress()?;
                        let covered = progress.len() == count && progress.iter().all(|entry| {
                            (0..count).any(|origin| entry.origin.as_str() == format!("detector-{origin}"))
                                && entry.contiguous_sequence == 12 && entry.known_high_sequence == 12
                                && entry.sparse_positions == 0 && !entry.source_unavailable
                                && !entry.baseline_missing && !entry.baseline_unconfirmed && entry.recovery_failure.is_none()
                        });
                        let expected = count as u64;
                        let expected_pncounter = i64::try_from(count)? * 5;
                        if covered && node.get_counter_value("det-g").await == expected * 7
                            && node.get_pncounter_value("det-p").await == expected_pncounter
                            && node.get_lww_register_value("det-r").await == Some(b"winner".to_vec())
                            && node.get_lww_map_entries("det-m").await.is_empty()
                            && node.get_orset_elements("det-o").await == vec!["present"]
                            && node.get_rga_values("det-a").await == vec![b"present".to_vec(); count] {
                            ensure!(read_materialized(&node.store, "det-g") == expected * 7
                                && read_materialized_pncounter(&node.store, "det-p") == expected_pncounter
                                && read_materialized_lww_register(&node.store, "det-r") == b"winner"
                                && read_materialized_lww_map(&node.store, "det-m").is_empty()
                                && read_materialized_orset(&node.store, "det-o") == vec!["present"]
                                && read_materialized_rga(&node.store, "det-a") == vec![b"present".to_vec(); count], "materialized state mismatch");
                            let states = six_family_states(&node.store);
                            ensure!(started.elapsed() < CONVERGENCE_LIMIT, "durable verification exceeded recovery limit");
                            process::emit(json!({"phase": "converged", "states": states,
                                "receipt_summary": {"node": index, "origins": count, "sequence_per_origin": 12, "sparse_positions": 0},
                                "elapsed_ms": started.elapsed().as_millis()}));
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
