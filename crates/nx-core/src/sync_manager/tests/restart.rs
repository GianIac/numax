use super::*;
use nx_net::{MembershipState, SerializationFormat};

const NODES: usize = 3;
const RECOVERY_LIMIT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug)]
enum RestartMode {
    Rolling,
    FullShutdown,
}

#[tokio::test]
async fn rolling_restart_every_node_recovers_six_families_json_and_bincode() {
    if run_in_isolated_test_process(
        "sync_manager::manager::tests::restart::rolling_restart_every_node_recovers_six_families_json_and_bincode",
    ) {
        return;
    }
    restart_scenario(RestartMode::Rolling).await;
}

#[tokio::test]
async fn full_cluster_shutdown_recovers_six_families_json_and_bincode() {
    if run_in_isolated_test_process(
        "sync_manager::manager::tests::restart::full_cluster_shutdown_recovers_six_families_json_and_bincode",
    ) {
        return;
    }
    restart_scenario(RestartMode::FullShutdown).await;
}

struct Cluster {
    directories: [tempfile::TempDir; NODES],
    configs: [SyncConfig; NODES],
    nodes: [Option<SyncManager>; NODES],
    sequences: [u64; NODES],
    epochs: [u64; NODES],
    generations: [u64; NODES],
    persisted_states: [Option<Vec<serde_json::Value>>; NODES],
}

impl Cluster {
    fn new(format: SerializationFormat) -> Self {
        let addresses: [String; NODES] = std::array::from_fn(|_| free_addr());
        let configs = std::array::from_fn(|index| {
            let mut config = SyncConfig::new()
                .with_listen_addr(addresses[index].clone())
                .with_serialization_format(format)
                .with_max_peers(NODES - 1)
                .with_anti_entropy_interval(Duration::from_millis(25))
                .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(40));
            for (peer, address) in addresses.iter().enumerate() {
                if peer != index {
                    config = config.with_peer(address.clone());
                }
            }
            config
        });
        Self {
            directories: std::array::from_fn(|_| tempfile::tempdir().unwrap()),
            configs,
            nodes: std::array::from_fn(|_| None),
            sequences: [0; NODES],
            epochs: [0; NODES],
            generations: [0; NODES],
            persisted_states: std::array::from_fn(|_| None),
        }
    }

    async fn start(&mut self, index: usize) {
        assert!(self.nodes[index].is_none());
        let id = NodeId::new(format!("restart-{index}"));
        let store = Arc::new(NxStore::open(self.directories[index].path()).unwrap());
        let mut node =
            SyncManager::try_new(id.clone(), self.configs[index].clone(), store, metrics())
                .unwrap();
        assert_eq!(node.node_id, id);
        assert!(node.generation > self.generations[index]);
        self.generations[index] = node.generation;
        if let Some(expected) = &self.persisted_states[index] {
            assert_eq!(&six_family_states(&node.store), expected);
        }
        if self.sequences[index] > 0 {
            let progress = node.handle().replication_progress().unwrap();
            let local = progress.iter().find(|entry| entry.origin == id).unwrap();
            assert_eq!(local.contiguous_sequence, self.sequences[index]);
            assert_eq!(local.sparse_positions, 0);
            assert!(!local.baseline_unconfirmed);
        }
        node.start().await.unwrap();
        let epoch = node
            .membership
            .as_ref()
            .unwrap()
            .snapshot
            .borrow()
            .records
            .iter()
            .find(|record| record.member == id.as_str())
            .unwrap()
            .version
            .epoch;
        assert!(epoch > self.epochs[index]);
        self.epochs[index] = epoch;
        self.nodes[index] = Some(node);
    }

    async fn stop(&mut self, index: usize) {
        let mut node = self.nodes[index].take().unwrap();
        node.shutdown().await.unwrap();
        self.persisted_states[index] = Some(six_family_states(&node.store));
        drop(node);
        wait_for_store_file_release(self.directories[index].path())
            .await
            .unwrap();
    }

    async fn stop_all(&mut self) {
        let [a, b, c] = &mut self.nodes;
        let results = tokio::join!(
            a.as_mut().unwrap().shutdown(),
            b.as_mut().unwrap().shutdown(),
            c.as_mut().unwrap().shutdown()
        );
        results.0.unwrap();
        results.1.unwrap();
        results.2.unwrap();
        for index in 0..NODES {
            let node = self.nodes[index].take().unwrap();
            self.persisted_states[index] = Some(six_family_states(&node.store));
            drop(node);
            wait_for_store_file_release(self.directories[index].path())
                .await
                .unwrap();
        }
        assert!(self.nodes.iter().all(Option::is_none));
    }

    async fn write(&mut self, index: usize) {
        let node = self.nodes[index].as_ref().unwrap();
        local_six_family_workload(&node.handle(), self.sequences[index] + index as u64).await;
        self.sequences[index] += 12;
        let progress = node.handle().replication_progress().unwrap();
        let local = progress
            .iter()
            .find(|entry| entry.origin == node.node_id)
            .unwrap();
        assert_eq!(local.contiguous_sequence, self.sequences[index]);
        assert_eq!(local.sparse_positions, 0);
        let log = node.op_log.read().await;
        let latest = log
            .iter()
            .rev()
            .find(|op| op.origin == node.node_id)
            .unwrap();
        let latest = sequenced_op(&node.store, latest).unwrap().unwrap();
        assert_eq!(latest.position.sequence, self.sequences[index]);
        assert_eq!(latest.position.generation, node.generation);
    }

    async fn wait_recovered(&self, phase: &str) -> u128 {
        let started = StdInstant::now();
        let workloads = self.sequences.iter().sum::<u64>() / 12;
        let result = tokio::time::timeout(RECOVERY_LIMIT, async {
            let mut poll = tokio::time::interval(Duration::from_millis(10));
            loop {
                poll.tick().await;
                let mut complete = true;
                for node in self.nodes.iter().map(|node| node.as_ref().unwrap()) {
                    let membership = node.membership.as_ref().unwrap().snapshot.borrow();
                    complete &= membership.error.is_none()
                        && self.epochs.iter().enumerate().all(|(index, epoch)| {
                            membership.records.iter().any(|record| {
                                record.member == format!("restart-{index}")
                                    && record.state == MembershipState::Alive
                                    && record.version.epoch == *epoch
                            })
                        });
                    drop(membership);
                    complete &= node.connected_peer_count().await > 0;
                    let progress = node.handle().replication_progress().unwrap();
                    complete &= self.sequences.iter().enumerate().all(|(index, sequence)| {
                        progress.iter().any(|entry| {
                            entry.origin.as_str() == format!("restart-{index}")
                                && entry.contiguous_sequence == *sequence
                                && entry.known_high_sequence == *sequence
                                && entry.sparse_positions == 0
                                && entry.recovery_failure.is_none()
                                && !entry.baseline_missing
                                && !entry.baseline_unconfirmed
                        })
                    });
                    complete &= node.get_counter_value("det-g").await == 7 * workloads
                        && node.get_pncounter_value("det-p").await == 5 * workloads as i64
                        && node.get_lww_register_value("det-r").await == Some(b"winner".to_vec())
                        && node.get_lww_map_entries("det-m").await.is_empty()
                        && node.get_orset_elements("det-o").await == vec!["present"]
                        && node.get_rga_values("det-a").await
                            == vec![b"present".to_vec(); workloads as usize];
                }
                if complete {
                    let expected = six_family_states(&self.nodes[0].as_ref().unwrap().store);
                    if self
                        .nodes
                        .iter()
                        .all(|node| six_family_states(&node.as_ref().unwrap().store) == expected)
                    {
                        break;
                    }
                }
            }
        })
        .await;
        if result.is_err() {
            let nodes = self
                .nodes
                .iter()
                .map(|node| node.as_ref().unwrap())
                .collect::<Vec<_>>();
            log_sync_phase(phase, started.elapsed(), &nodes);
        }
        assert!(
            result.is_ok() && started.elapsed() < RECOVERY_LIMIT,
            "{phase}: recovery exceeded {RECOVERY_LIMIT:?}"
        );
        for node in self.nodes.iter().map(|node| node.as_ref().unwrap()) {
            assert_eq!(read_materialized(&node.store, "det-g"), 7 * workloads);
            assert_eq!(
                read_materialized_pncounter(&node.store, "det-p"),
                5 * workloads as i64
            );
            assert_eq!(
                read_materialized_lww_register(&node.store, "det-r"),
                b"winner"
            );
            assert!(read_materialized_lww_map(&node.store, "det-m").is_empty());
            assert_eq!(
                read_materialized_orset(&node.store, "det-o"),
                vec!["present"]
            );
            assert_eq!(
                read_materialized_rga(&node.store, "det-a"),
                vec![b"present".to_vec(); workloads as usize]
            );
        }
        started.elapsed().as_millis()
    }
}

async fn restart_scenario(mode: RestartMode) {
    for format in [SerializationFormat::Json, SerializationFormat::Bincode] {
        let mut cluster = Cluster::new(format);
        for index in 0..NODES {
            cluster.start(index).await;
        }
        for index in 0..NODES {
            cluster.write(index).await;
        }
        cluster.wait_recovered("initial-cluster").await;
        let initial_epochs = cluster.epochs;
        let initial_generations = cluster.generations;
        let mut recovery_ms = Vec::new();
        match mode {
            RestartMode::Rolling => {
                for offline in 0..NODES {
                    cluster.stop(offline).await;
                    for online in 0..NODES {
                        if online != offline {
                            cluster.write(online).await;
                        }
                    }
                    cluster.start(offline).await;
                    recovery_ms.push(cluster.wait_recovered("rolling-rejoin").await);
                    cluster.write(offline).await;
                    cluster.wait_recovered("rolling-new-writes").await;
                }
                assert_eq!(cluster.sequences, [48; NODES]);
            }
            RestartMode::FullShutdown => {
                cluster.stop_all().await;
                for index in 0..NODES {
                    cluster.start(index).await;
                }
                recovery_ms.push(cluster.wait_recovered("full-cluster-rejoin").await);
                for index in 0..NODES {
                    cluster.write(index).await;
                }
                cluster.wait_recovered("full-cluster-new-writes").await;
                assert_eq!(cluster.sequences, [24; NODES]);
            }
        }
        for index in 0..NODES {
            assert!(cluster.epochs[index] > initial_epochs[index]);
            assert!(cluster.generations[index] > initial_generations[index]);
        }
        println!(
            "RESTART_REPORT {}",
            serde_json::json!({
                "version": 1, "mode": format!("{mode:?}"), "codec": format!("{format:?}"),
                "nodes": NODES, "restarted_nodes": NODES, "transport": "real loopback TCP",
                "shutdown": "graceful; stores closed and reopened",
                "topology": "static full mesh", "max_peers": NODES - 1,
                "anti_entropy_interval_ms": 25, "reconnect_backoff_ms": [10, 40],
                "retention_operations": cluster.configs[0].op_log_limit,
                "families": 6, "operations_per_workload": 12,
                "final_sequences": cluster.sequences,
                "initial_epochs": initial_epochs, "final_epochs": cluster.epochs,
                "initial_generations": initial_generations, "final_generations": cluster.generations,
                "recovery_limit_ms": RECOVERY_LIMIT.as_millis(),
                "recovery_ms": recovery_ms,
            })
        );
        cluster.stop_all().await;
    }
}
