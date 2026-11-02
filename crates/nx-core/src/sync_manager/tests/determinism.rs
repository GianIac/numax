use super::*;
use crate::membership::runtime::{Observation, simulation::Simulation};
use crate::peer_io::simulation::{Incoming, Network, Tasks};
use crate::peer_io::{PeerIo, clock::Clock, random::SeededIds};
use std::collections::BTreeMap;

enum Work {
    Membership(usize, Observation),
    Done,
}
struct Replica {
    _candidates_tx: watch::Sender<Arc<Vec<String>>>,
    manager: SyncManager,
    io: Arc<dyn PeerIo>,
    membership: Simulation,
    context: Arc<NodeEventContext>,
}
struct Runner {
    network: Arc<Network>,
    replicas: Vec<Replica>,
    tasks: Tasks<Work>,
    decisions: Vec<String>,
    stopping: bool,
}
impl Runner {
    fn record(&mut self, decision: String) {
        assert!(
            self.decisions.len() < 65_536,
            "simulation decision trace budget exhausted"
        );
        self.decisions.push(decision);
    }
    fn new(seed: u64, format: nx_net::SerializationFormat) -> Self {
        Self::with_loss(seed, format, 10)
    }
    fn with_loss(seed: u64, format: nx_net::SerializationFormat, loss: u64) -> Self {
        let network = Network::with_timing(
            seed,
            format,
            loss,
            Duration::from_millis(5),
            Duration::from_millis(1),
            2,
        );
        let mut runner = Self {
            network,
            replicas: Vec::new(),
            tasks: Tasks::new(),
            decisions: Vec::new(),
            stopping: false,
        };
        for index in 0..3 {
            let addr = format!("127.0.0.1:{}", 12000 + index);
            let id = format!("simulation-{index}");
            let config = SyncConfig::new()
                .with_max_peers(2)
                .with_serialization_format(format)
                .with_anti_entropy_interval(Duration::from_millis(100))
                .with_socket_timeout(Duration::from_millis(100))
                .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(40));
            let mut manager =
                SyncManager::try_new(NodeId::new(&id), config, temp_store(), metrics()).unwrap();
            manager.clock = runner.network.clock.clone();
            manager.ids = Arc::new(SeededIds::new(seed, &id, manager.generation, "local"));
            let io = runner.network.endpoint(&addr, &id, manager.generation);
            let membership = Simulation::new(
                &id,
                &addr,
                io.clone(),
                manager.store.clone(),
                manager.metrics.clone(),
                seed,
            )
            .unwrap();
            let snapshot = membership.snapshot();
            *manager.membership_view.lock().unwrap() = Some(snapshot.clone());
            let repair_budget = Arc::new(StdMutex::new(
                crate::sync_manager::delivery::RepairBudget::new(io.now()),
            ));
            let pacing = Arc::new(StdMutex::new(crate::sync_manager::pacing::Pacer::new(
                manager.config.gossip_rate,
                2,
            )));
            let (forward_tx, forward_rx) = crate::sync_manager::delivery::named_channel(
                manager.config.queued_ops_limit,
                crate::sync_manager::delivery::QUEUE_BYTES,
                manager.metrics.clone(),
                true,
            );
            let context = Arc::new(NodeEventContext {
                repair_budget: repair_budget.clone(),
                history_cursors: manager.history_cursors.clone(),
                history_requests: manager.history_requests.clone(),
                history_request_timeout: manager.config.socket_timeout,
                counters: manager.counters.clone(),
                pncounters: manager.pncounters.clone(),
                lww_registers: manager.lww_registers.clone(),
                lww_maps: manager.lww_maps.clone(),
                orsets: manager.orsets.clone(),
                rgas: manager.rgas.clone(),
                seen_ops: manager.seen_ops.clone(),
                seen_ops_next_sequence: manager.seen_ops_next_sequence.clone(),
                op_log: manager.op_log.clone(),
                op_log_next_sequence: manager.op_log_next_sequence.clone(),
                op_log_limit: manager.config.op_log_limit,
                store: manager.store.clone(),
                metrics: manager.metrics.clone(),
                node: io.clone(),
                forward_tx: Some(forward_tx),
                peer_health: manager.peer_health.clone(),
                active_connections: manager.active_connections.clone(),
                anti_entropy_watermarks: manager.anti_entropy_watermarks.clone(),
                peer_dead_after_failures: manager.config.peer_dead_after_failures,
            });
            let broadcast = BroadcastLoopContext {
                max_message_size: io.max_message_size(),
                pacing: pacing.clone(),
                local_id: manager.node_id.clone(),
                membership: snapshot.clone(),
                node: io.clone(),
                metrics: manager.metrics.clone(),
                fanout_constant: manager.config.gossip_fanout_constant,
                max_fanout: manager.config.gossip_max_fanout,
                fanout_cursor: manager.fanout_cursor.clone(),
            };
            let rx = manager.op_rx.take().unwrap();
            let stopped = manager.shutdown_tx.subscribe();
            runner.tasks.spawn(async move {
                broadcast_loop(broadcast, rx, stopped).await;
                Work::Done
            });
            let forward = ForwardLoopContext {
                max_message_size: io.max_message_size(),
                pacing,
                local_id: manager.node_id.clone(),
                membership: snapshot.clone(),
                node: io.clone(),
                metrics: manager.metrics.clone(),
                fanout_constant: manager.config.gossip_fanout_constant,
                max_fanout: manager.config.gossip_max_fanout,
                fanout_cursor: manager.fanout_cursor.clone(),
            };
            let stopped = manager.shutdown_tx.subscribe();
            runner.tasks.spawn(async move {
                forward_loop(forward, forward_rx, stopped).await;
                Work::Done
            });
            let peers: Vec<_> = (0..3)
                .filter(|peer| (*peer as isize - index as isize).abs() == 1)
                .map(|peer| format!("127.0.0.1:{}", 12000 + peer))
                .collect();
            let (candidates_tx, candidates_rx) = watch::channel(Arc::new(peers));
            let reconnect = ReconnectLoopContext {
                local_id: manager.node_id.clone(),
                membership: snapshot,
                node: io.clone(),
                rotation_interval: crate::sync_manager::neighbors::NEIGHBOR_ROTATION_INTERVAL,
                candidates_rx,
                max_peers: 2,
                initial_delay: manager.config.reconnect_initial_delay,
                max_delay: manager.config.reconnect_max_delay,
                peer_dead_after_failures: manager.config.peer_dead_after_failures,
                shutdown_rx: manager.shutdown_tx.subscribe(),
                metrics: manager.metrics.clone(),
                peer_health: manager.peer_health.clone(),
            };
            runner.tasks.spawn(async move {
                reconnect_loop(reconnect).await;
                Work::Done
            });
            let entropy = AntiEntropyLoopContext {
                store: manager.store.clone(),
                jitter_seed: crate::sync_manager::delivery::seed(&id),
                repair_budget,
                history_cursors: manager.history_cursors.clone(),
                request_timeout: manager.config.socket_timeout,
                node: io.clone(),
                interval: manager.config.anti_entropy_interval,
                shutdown_rx: manager.shutdown_tx.subscribe(),
                metrics: manager.metrics.clone(),
            };
            runner.tasks.spawn(async move {
                anti_entropy_loop(entropy).await;
                Work::Done
            });
            runner.replicas.push(Replica {
                _candidates_tx: candidates_tx,
                manager,
                io,
                membership,
                context,
            });
            runner.tick(index, runner.network.clock.now());
        }
        runner
    }
    fn tick(&mut self, index: usize, at: StdInstant) {
        let clock = self.network.clock.clone();
        self.tasks.spawn(async move {
            clock.sleep_until(at).await;
            Work::Membership(index, Observation::Tick(at))
        });
    }
    async fn quiesce(&mut self) {
        for _ in 0..4096 {
            let mut ready = self.tasks.poll_ready();
            for replica in &mut self.replicas {
                for dispatch in replica.membership.dispatches() {
                    if self.stopping {
                        continue;
                    }
                    replica.membership.observe(dispatch).await.unwrap();
                }
            }
            ready.sort_by_key(|(id, event)| match event {
                Work::Membership(node, Observation::Written(_)) => (0, *node, *id),
                Work::Membership(node, Observation::Completed(_)) => (1, *node, *id),
                Work::Membership(node, Observation::Tick(_)) => (2, *node, *id),
                Work::Membership(node, _) => (3, *node, *id),
                Work::Done => (4, 0, *id),
            });
            let mut progress = !ready.is_empty();
            for (id, event) in ready {
                if matches!(event, Work::Done) {
                    self.record(format!(
                        "done:{id}:{}",
                        self.network
                            .clock
                            .now()
                            .duration_since(self.network.clock.origin)
                            .as_nanos()
                    ));
                }
                if let Work::Membership(index, observation) = event {
                    if self.stopping && !matches!(observation, Observation::Written(_)) {
                        continue;
                    }
                    let tick = matches!(observation, Observation::Tick(_));
                    self.replicas[index]
                        .membership
                        .observe(observation)
                        .await
                        .unwrap();
                    let snapshot = self.replicas[index].membership.snapshot();
                    self.record(format!(
                        "{}:{index}:{:?}:{}",
                        self.network
                            .clock
                            .now()
                            .duration_since(self.network.clock.origin)
                            .as_nanos(),
                        snapshot.borrow().records,
                        snapshot.borrow().health
                    ));
                    if tick {
                        self.tick(index, self.network.clock.now() + Duration::from_millis(25));
                    }
                }
            }
            for index in 0..self.replicas.len() {
                for job in self.replicas[index].membership.jobs() {
                    if self.stopping {
                        continue;
                    }
                    self.tasks
                        .spawn(async move { Work::Membership(index, job.await) });
                    progress = true;
                }
                for write in self.replicas[index].membership.writes() {
                    self.tasks
                        .spawn(async move { Work::Membership(index, write.await) });
                    progress = true;
                }
            }
            while let Some(delivery) = self.network.pop_ready() {
                let index = delivery
                    .to
                    .rsplit(':')
                    .next()
                    .unwrap()
                    .parse::<usize>()
                    .unwrap()
                    - 12000;
                match delivery.event {
                    Incoming::Control(event) => {
                        if !self.stopping {
                            self.replicas[index]
                                .membership
                                .observe(Observation::Incoming(Box::new(event)))
                                .await
                                .unwrap();
                        }
                    }
                    Incoming::Data(event) => {
                        self.record(format!("incoming:{index}:{event:?}"));
                        let context = self.replicas[index].context.clone();
                        self.tasks.spawn(async move {
                            handle_node_event(event, &context).await;
                            Work::Done
                        });
                    }
                }
                progress = true;
            }
            if !progress {
                return;
            }
        }
        panic!("simulation failed to quiesce within microstep budget");
    }
    async fn until(&mut self, end: Duration) {
        let deadline = self.network.clock.origin + end;
        for _ in 0..65_536 {
            self.quiesce().await;
            let next = [
                self.network.clock.next_deadline(),
                self.network.next_delivery(),
            ]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or(deadline);
            if next > deadline || self.network.clock.now() >= deadline {
                return;
            }
            self.network.clock.advance_to(next);
        }
        panic!("simulation event budget exhausted");
    }
    async fn shutdown(mut self) {
        self.stopping = true;
        for replica in &mut self.replicas {
            replica.membership.leave().await.unwrap();
            replica.manager.shutdown_tx.send_replace(true);
        }
        self.quiesce().await;
        for replica in &self.replicas {
            replica.io.shutdown().await;
        }
        // Dropping the executor cancels every remaining RPC and timer.
        drop(self.tasks);
        assert!(self.network.clock.next_deadline().is_none());
    }
}

async fn scenario(seed: u64, format: nx_net::SerializationFormat) -> serde_json::Value {
    let mut runner = Runner::new(seed, format);
    for index in [0, 2] {
        let handle = runner.replicas[index].manager.handle();
        local_six_family_workload(&handle, index as u64).await;
    }
    runner.until(Duration::from_secs(3)).await;
    let expected = six_family_states(&runner.replicas[0].manager.store);
    for replica in &runner.replicas {
        assert_eq!(six_family_states(&replica.manager.store), expected);
        assert_eq!(
            read_materialized(&replica.manager.store, "det-g"),
            14,
            "seed={seed}; time={:?}; progress={:?}; tail={:?}; decisions={:?}",
            runner
                .network
                .clock
                .now()
                .duration_since(runner.network.clock.origin),
            replica.manager.handle().replication_progress(),
            runner
                .network
                .trace()
                .iter()
                .rev()
                .take(20)
                .collect::<Vec<_>>(),
            (
                runner
                    .decisions
                    .iter()
                    .filter(|d| d.starts_with("incoming:"))
                    .take(10)
                    .collect::<Vec<_>>(),
                runner
                    .replicas
                    .iter()
                    .map(|r| r.manager.handle().replication_progress().unwrap())
                    .collect::<Vec<_>>()
            )
        );
        assert_eq!(
            read_materialized_pncounter(&replica.manager.store, "det-p"),
            10
        );
        assert_eq!(
            read_materialized_orset(&replica.manager.store, "det-o"),
            vec!["present"]
        );
        let progress: BTreeMap<_, _> = replica
            .manager
            .handle()
            .replication_progress()
            .unwrap()
            .into_iter()
            .map(|p| {
                (
                    p.origin.to_string(),
                    (
                        p.contiguous_sequence,
                        p.sparse_positions,
                        p.recovery_failure,
                    ),
                )
            })
            .collect();
        for origin in ["simulation-0", "simulation-2"] {
            assert_eq!(progress[origin], (12, 0, None));
        }
        assert!(replica.io.connected_peer_count().await <= 2);
        assert!(
            replica
                .membership
                .snapshot()
                .borrow()
                .records
                .iter()
                .all(|r| r.state == nx_net::MembershipState::Alive)
        );
    }
    let report = serde_json::json!({
        "version": 1, "seed": seed, "codec": format!("{format:?}"),
        "topology": "initial three-node chain, production neighbor selection thereafter",
        "loss": "10% independent directed message draws",
        "latency_ms": 5, "writer_service_ms": 1, "virtual_duration_ms": 3000,
        "recovery_request_timeout_ms": 100, "anti_entropy_interval_ms": 100,
        "operations": 24, "retention_operations": 10000,
        "network_trace": runner.network.trace(), "membership_trace": runner.decisions,
        "state": expected,
    });
    runner.shutdown().await;
    report
}

#[tokio::test]
async fn long_control_data_partition_json() {
    if run_in_isolated_test_process(
        "sync_manager::manager::tests::determinism::long_control_data_partition_json",
    ) {
        return;
    }
    replay_partition(nx_net::SerializationFormat::Json).await;
}

#[tokio::test]
async fn long_control_data_partition_bincode() {
    if run_in_isolated_test_process(
        "sync_manager::manager::tests::determinism::long_control_data_partition_bincode",
    ) {
        return;
    }
    replay_partition(nx_net::SerializationFormat::Bincode).await;
}

async fn replay_partition(format: nx_net::SerializationFormat) {
    let first = partition_scenario(42, format).await;
    println!(
        "PARTITION_V1 {format:?} {}",
        blake3::hash(&serde_json::to_vec(&first).unwrap())
    );
}

async fn partition_scenario(seed: u64, format: nx_net::SerializationFormat) -> serde_json::Value {
    let started = StdInstant::now();
    let mut runner = Runner::with_loss(seed, format, 0);
    runner.until(Duration::from_secs(3)).await;
    for replica in &runner.replicas {
        let snapshot = replica.membership.snapshot();
        let snapshot = snapshot.borrow();
        assert_eq!(snapshot.records.len(), 3);
        assert!(
            snapshot
                .records
                .iter()
                .all(|r| r.state == nx_net::MembershipState::Alive)
        );
    }
    for peer in [1, 2] {
        runner.network.partition(
            "127.0.0.1:12000",
            &format!("127.0.0.1:{}", 12000 + peer),
            true,
        );
    }
    for index in [0, 2] {
        local_six_family_workload(&runner.replicas[index].manager.handle(), index as u64).await;
    }
    runner.until(Duration::from_secs(63)).await;
    eprintln!(
        "PARTITION_PHASE codec={format:?} phase=dead elapsed_ms={}",
        started.elapsed().as_millis()
    );
    assert_eq!(
        runner
            .network
            .clock
            .now()
            .duration_since(runner.network.clock.origin),
        Duration::from_secs(63)
    );
    for (index, peer) in [(0, 1), (0, 2), (1, 0), (2, 0)] {
        let replica = &runner.replicas[index];
        let member = format!("simulation-{peer}");
        let snapshot = replica.membership.snapshot();
        assert!(
            snapshot
                .borrow()
                .records
                .iter()
                .any(|r| r.member == member && r.state == nx_net::MembershipState::Dead)
        );
        assert_durable_dead(&replica.manager.store, &member);
    }
    let probes: Vec<_> = runner
        .replicas
        .iter()
        .map(|r| r.membership.snapshot().borrow().successful_probes)
        .collect();
    for peer in [1, 2] {
        runner.network.partition(
            "127.0.0.1:12000",
            &format!("127.0.0.1:{}", 12000 + peer),
            false,
        );
    }
    let mut recovered_at = None;
    // No new writes after rejoin. Each until() drains work at every intervening deadline.
    for second in 1..=30 {
        runner.until(Duration::from_secs(63 + second)).await;
        let expected = six_family_states(&runner.replicas[0].manager.store);
        let complete = runner.replicas.iter().enumerate().all(|(index, replica)| {
            let snapshot = replica.membership.snapshot();
            let snapshot = snapshot.borrow();
            snapshot.error.is_none()
                && snapshot
                    .records
                    .iter()
                    .all(|r| r.state == nx_net::MembershipState::Alive)
                && snapshot.successful_probes > probes[index]
                && six_family_states(&replica.manager.store) == expected
                && replica
                    .manager
                    .handle()
                    .replication_progress()
                    .unwrap()
                    .iter()
                    .filter(|p| {
                        p.origin.as_str() == "simulation-0" || p.origin.as_str() == "simulation-2"
                    })
                    .filter(|p| {
                        p.contiguous_sequence == 12
                            && p.sparse_positions == 0
                            && p.recovery_failure.is_none()
                            && !p.baseline_unconfirmed
                    })
                    .count()
                    == 2
        });
        if complete {
            recovered_at = Some(second);
            break;
        }
    }
    assert!(
        recovered_at.is_some(),
        "seed={seed} codec={format:?} recovery exceeded 30 virtual seconds; snapshots={:?}",
        runner
            .replicas
            .iter()
            .map(|r| r.membership.snapshot().borrow().clone())
            .collect::<Vec<_>>()
    );
    eprintln!(
        "PARTITION_PHASE codec={format:?} phase=recovered recovered_by_virtual_s={} elapsed_ms={}",
        recovered_at.unwrap(),
        started.elapsed().as_millis()
    );
    for replica in &runner.replicas {
        assert_six_family_memory(&replica.manager).await;
        assert_six_family_materialized(&replica.manager.store);
        let owner = replica.membership.snapshot();
        assert!(
            owner
                .borrow()
                .records
                .iter()
                .find(|r| r.member == replica.manager.node_id.as_str())
                .unwrap()
                .version
                .incarnation
                > 0
        );
    }
    let report = serde_json::json!({
        "version": 1, "seed": seed, "codec": format!("{format:?}"),
        "topology": "initial three-node chain, production neighbor selection thereafter; cut {0}|{1,2}",
        "partition_start_ms": 3000, "partition_duration_ms": 60000,
        "loss": "bidirectional data and control cut; no additional random loss",
        "latency_ms": 5, "writer_service_ms": 1, "operations": 24,
        "retention_operations": 10000, "anti_entropy_interval_ms": 100,
        "recovery_limit_ms": 30000, "recovered_by_ms": recovered_at.unwrap() * 1000,
        "network_trace": runner.network.trace(), "membership_trace": runner.decisions,
        "state": six_family_states(&runner.replicas[0].manager.store),
    });
    runner.shutdown().await;
    report
}

#[tokio::test]
async fn deterministic_six_family_replay_json() {
    repeated_codec_replay(nx_net::SerializationFormat::Json).await;
}

#[tokio::test]
async fn deterministic_six_family_replay_bincode() {
    repeated_codec_replay(nx_net::SerializationFormat::Bincode).await;
}

fn replay_digest(output: &[u8], format: nx_net::SerializationFormat) -> blake3::Hash {
    let output = std::str::from_utf8(output).unwrap();
    let prefix = format!("DETERMINISM {format:?} ");
    let mut digests = output.lines().filter_map(|line| line.strip_prefix(&prefix));
    let hash = blake3::Hash::from_hex(digests.next().expect("replay digest missing")).unwrap();
    assert!(digests.next().is_none(), "multiple replay digests");
    hash
}

async fn repeated_codec_replay(format: nx_net::SerializationFormat) {
    const STAGE: &str = "NUMAX_DETERMINISM_STAGE";
    let test_name = std::thread::current().name().unwrap().to_owned();
    let stage = std::env::var_os(STAGE);
    let repeat = match stage.as_deref() {
        None => {
            let started = StdInstant::now();
            let lifetime = Duration::from_secs(180);
            let output = isolated_test_output(&test_name, STAGE, "repeat", lifetime);
            let expected = replay_digest(&output, format);
            for index in 0..3 {
                let remaining = lifetime
                    .checked_sub(started.elapsed())
                    .filter(|remaining| !remaining.is_zero())
                    .expect("codec replay exceeded its total lifetime");
                let output = isolated_test_output(
                    &test_name,
                    STAGE,
                    "compare",
                    remaining.min(Duration::from_secs(60)),
                );
                assert_eq!(
                    replay_digest(&output, format),
                    expected,
                    "cross-process trace mismatch for {format:?}, child {index}",
                );
            }
            assert!(
                started.elapsed() < lifetime,
                "codec replay exceeded its total lifetime"
            );
            return;
        }
        Some(stage) if stage == "repeat" => true,
        Some(stage) if stage == "compare" => false,
        Some(_) => panic!("invalid deterministic replay stage"),
    };
    let started = StdInstant::now();
    let expected = scenario(42, format).await;
    eprintln!(
        "REPLAY_RUN codec={format:?} iteration=0 elapsed_ms={}",
        started.elapsed().as_millis(),
    );
    if repeat {
        for iteration in 1..10 {
            let run_started = StdInstant::now();
            assert_eq!(scenario(42, format).await, expected);
            eprintln!(
                "REPLAY_RUN codec={format:?} iteration={iteration} elapsed_ms={}",
                run_started.elapsed().as_millis(),
            );
        }
        let alternate = scenario(7, format).await;
        assert_ne!(
            alternate["network_trace"], expected["network_trace"],
            "seed did not change decisions"
        );
    }
    let hash = blake3::hash(&serde_json::to_vec(&expected).unwrap());
    println!("DETERMINISM {format:?} {hash}");
    eprintln!(
        "REPLAY_CODEC codec={format:?} elapsed_ms={}",
        started.elapsed().as_millis(),
    );
}

#[tokio::test]
async fn deterministic_shutdown_before_first_tick() {
    Runner::new(42, nx_net::SerializationFormat::Json)
        .shutdown()
        .await;
}

#[tokio::test]
async fn deterministic_local_flush_failure_restart_preserves_unknown_outcome() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(NxStore::open(directory.path()).unwrap());
    let mut manager = SyncManager::try_new(
        NodeId::new("simulation-0"),
        SyncConfig::new(),
        store.clone(),
        metrics(),
    )
    .unwrap();
    manager.clock = crate::peer_io::simulation::Clock::new();
    manager.ids = Arc::new(SeededIds::new(
        42,
        "simulation-0",
        manager.generation,
        "local",
    ));
    let handle = manager.handle();
    store.inject_disk_full_on_flush(true);
    assert!(handle.increment_gcounter("failed-flush", 7).await.is_err());
    assert_eq!(manager.get_counter_value("failed-flush").await, 0);
    assert!(manager.seen_ops.read().await.ids.is_empty());
    store.inject_disk_full_on_flush(false);
    assert!(
        handle.increment_gcounter("failed-flush", 5).await.is_err(),
        "failed durability must fence writes until reopen"
    );
    drop(handle);
    drop(manager);
    drop(store);
    // A rejected flush is an unknown outcome: the accepted batch may survive.
    // Restart loads that state instead of pretending it was rolled back.
    let mut restarted = SyncManager::try_new(
        NodeId::new("simulation-0"),
        SyncConfig::new(),
        Arc::new(NxStore::open(directory.path()).unwrap()),
        metrics(),
    )
    .unwrap();
    let store = restarted.store.clone();
    restarted.ids = Arc::new(SeededIds::new(
        42,
        "simulation-0",
        restarted.generation,
        "local",
    ));
    assert_eq!(restarted.get_counter_value("failed-flush").await, 7);
    let accepted = restarted
        .handle()
        .increment_gcounter("failed-flush", 5)
        .await
        .unwrap();
    assert_eq!(accepted.position().sequence, 2);
    assert_eq!(read_materialized(&store, "failed-flush"), 12);
    assert_eq!(
        read_durable_gcounter_state(&store, "failed-flush").value(),
        12
    );
    assert_eq!(
        restarted.handle().replication_progress().unwrap()[0].contiguous_sequence,
        2
    );
}
