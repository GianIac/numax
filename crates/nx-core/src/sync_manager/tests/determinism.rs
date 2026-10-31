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
        let network = Network::with_timing(
            seed,
            format,
            10,
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

fn states(store: &NxStore) -> Vec<serde_json::Value> {
    [
        read_durable_gcounter_state(store, "det-g")
            .to_json()
            .unwrap(),
        read_durable_pncounter_state(store, "det-p")
            .to_json()
            .unwrap(),
        read_durable_lww_register_state(store, "det-r")
            .to_json()
            .unwrap(),
        read_durable_lww_map_state(store, "det-m")
            .to_json()
            .unwrap(),
        read_durable_orset_state(store, "det-o").to_json().unwrap(),
        read_durable_rga_state(store, "det-a").to_json().unwrap(),
    ]
    .into_iter()
    .map(|json| serde_json::from_str(&json).unwrap())
    .collect()
}
async fn scenario(seed: u64, format: nx_net::SerializationFormat) -> serde_json::Value {
    let mut runner = Runner::new(seed, format);
    for index in [0, 2] {
        let handle = runner.replicas[index].manager.handle();
        local_increment(&handle, "det-g", 7).await;
        local_pncounter_inc(&handle, "det-p", 9).await;
        local_pncounter_dec(&handle, "det-p", 4).await;
        local_lww_register_set(&handle, "det-r", b"winner", 200 + index as u64).await;
        local_lww_map_set(&handle, "det-m", "field", b"removed", 100).await;
        local_lww_map_remove(&handle, "det-m", "field", 200).await;
        local_orset_add(&handle, "det-o", "removed").await;
        local_orset_remove(&handle, "det-o", "removed").await;
        local_orset_add(&handle, "det-o", "present").await;
        let removed = local_rga_insert_after(&handle, "det-a", None, b"removed").await;
        local_rga_delete(&handle, "det-a", &removed).await;
        local_rga_insert_after(&handle, "det-a", None, b"present").await;
    }
    runner.until(Duration::from_secs(3)).await;
    let expected = states(&runner.replicas[0].manager.store);
    for replica in &runner.replicas {
        assert_eq!(states(&replica.manager.store), expected);
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
async fn deterministic_six_family_replay() {
    let test_name = std::thread::current().name().unwrap().to_owned();
    if std::env::var_os("NUMAX_DETERMINISM_ISOLATED").is_none() {
        // sled's process-global lock must not delay unrelated real-time tests.
        // This lifetime covers all repeated runs, not their virtual deadline.
        isolated_test_output(
            &test_name,
            "NUMAX_DETERMINISM_ISOLATED",
            "1",
            Duration::from_secs(180),
        );
        return;
    }
    let child = std::env::var_os("NUMAX_DETERMINISM_CHILD").is_some();
    let mut markers = Vec::new();
    for format in [
        nx_net::SerializationFormat::Json,
        nx_net::SerializationFormat::Bincode,
    ] {
        let expected = scenario(42, format).await;
        if !child {
            for _ in 1..10 {
                assert_eq!(scenario(42, format).await, expected);
            }
            let alternate = scenario(7, format).await;
            assert_ne!(
                alternate["network_trace"], expected["network_trace"],
                "seed did not change decisions"
            );
        }

        let hash = blake3::hash(&serde_json::to_vec(&expected).unwrap());
        let marker = format!("DETERMINISM {format:?} {hash}");
        println!("{marker}");
        markers.push(marker);
    }
    if !child {
        for _ in 0..3 {
            let output = isolated_test_output(
                &test_name,
                "NUMAX_DETERMINISM_CHILD",
                "1",
                Duration::from_secs(60),
            );
            for marker in &markers {
                assert!(
                    String::from_utf8_lossy(&output).contains(marker),
                    "cross-process trace mismatch"
                );
            }
        }
    }
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
