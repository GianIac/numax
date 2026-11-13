use super::*;
use nx_net::{MembershipState, SerializationFormat};

#[tokio::test]
async fn real_control_data_partition_json() {
    if run_in_isolated_test_process(
        "sync_manager::manager::tests::topology::real_control_data_partition_json",
    ) {
        return;
    }
    real_control_data_partition(SerializationFormat::Json).await;
}

#[tokio::test]
async fn real_control_data_partition_bincode() {
    if run_in_isolated_test_process(
        "sync_manager::manager::tests::topology::real_control_data_partition_bincode",
    ) {
        return;
    }
    real_control_data_partition(SerializationFormat::Bincode).await;
}

#[tokio::test]
async fn history_summary_storage_wait_does_not_stall_control() {
    history_storage_wait_does_not_stall_control(StorageWaitCase::Summary).await;
}

#[tokio::test]
async fn history_unavailable_storage_wait_does_not_stall_control() {
    history_storage_wait_does_not_stall_control(StorageWaitCase::Unavailable).await;
}

#[tokio::test]
async fn baseline_unavailable_storage_wait_does_not_stall_control() {
    history_storage_wait_does_not_stall_control(StorageWaitCase::BaselineUnavailable).await;
}

#[tokio::test]
async fn remote_push_storage_wait_does_not_stall_control() {
    history_storage_wait_does_not_stall_control(StorageWaitCase::Push).await;
}

#[tokio::test]
async fn local_acceptance_storage_wait_does_not_stall_control() {
    history_storage_wait_does_not_stall_control(StorageWaitCase::Local).await;
}

#[derive(Clone, Copy, Debug)]
enum StorageWaitCase {
    Summary,
    Unavailable,
    BaselineUnavailable,
    Push,
    Local,
}

async fn history_storage_wait_does_not_stall_control(case: StorageWaitCase) {
    use std::future::Future;

    let (mut manager, _, store) =
        started_manager_with_config(SyncConfig::new().with_listen_addr("127.0.0.1:0")).await;
    let owner = test_owner_record(&mut manager).await;
    let context = test_event_context(
        Arc::clone(&manager.counters),
        Arc::clone(&manager.seen_ops),
        Arc::clone(&manager.op_log),
        Arc::clone(&store),
        Arc::clone(&manager.metrics),
        Arc::clone(&manager.peer_health),
    );
    let cursors = Arc::clone(&context.history_cursors);
    let request_deadline = StdInstant::now() + Duration::from_secs(30);
    let origin = if matches!(case, StorageWaitCase::Local) {
        manager.node_id().clone()
    } else {
        NodeId::new("storage-wait-origin")
    };
    let request_id = nx_sync::OpId::generate();
    if !matches!(case, StorageWaitCase::Summary | StorageWaitCase::Local) {
        record_summary(
            &store,
            &nx_sync::HistorySummary {
                origin: origin.clone(),
                high_sequence: 1,
                baseline_required: false,
            },
        )
        .unwrap();
    }
    if matches!(
        case,
        StorageWaitCase::Unavailable | StorageWaitCase::BaselineUnavailable
    ) {
        let baseline = matches!(case, StorageWaitCase::BaselineUnavailable);
        context
            .history_requests
            .write()
            .await
            .entry("unused".into())
            .or_default()
            .insert(
                origin.to_string(),
                PendingHistoryRequest {
                    request_id: request_id.clone(),
                    start: u64::from(!baseline),
                    end: u64::from(!baseline),
                    baseline_revision: baseline
                        .then(|| recovery_revision(&store, &origin).unwrap()),
                    deadline: request_deadline,
                },
            );
    }
    let message = match case {
        StorageWaitCase::Summary => Some(nx_sync::HistoryMessage::Summary {
            entries: vec![nx_sync::HistorySummary {
                baseline_required: false,
                origin: origin.clone(),
                high_sequence: 0,
            }],
        }),
        StorageWaitCase::Unavailable => Some(nx_sync::HistoryMessage::Unavailable {
            request_id,
            origin: origin.clone(),
            start: 1,
            end: 1,
        }),
        StorageWaitCase::BaselineUnavailable => {
            Some(nx_sync::HistoryMessage::BaselineUnavailable {
                request_id,
                origin: origin.clone(),
                reason: nx_sync::BaselineFailure::BudgetExceeded,
            })
        }
        StorageWaitCase::Push => Some(nx_sync::HistoryMessage::Push {
            ops: vec![nx_sync::SequencedOp {
                op: Op::gcounter_increment(origin.clone(), "storage-wait", 7),
                position: nx_sync::OpPosition {
                    generation: 1,
                    sequence: 1,
                },
            }],
        }),
        StorageWaitCase::Local => None,
    };
    {
        let mut cursors = cursors.write().await;
        let cursor = cursors.entry("unused".into()).or_default();
        cursor.after_origin = Some("previous-page".into());
        cursor.request_deadline = Some(request_deadline);
    }
    let (held_tx, held_rx) = tokio::sync::oneshot::channel();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (probe_tx, probe_rx) = std::sync::mpsc::channel();
    let held_store = Arc::clone(&store);
    let holder = std::thread::spawn(move || {
        let lease = held_store.acquire_write_lease().unwrap();
        held_tx.send(()).unwrap();
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let control_progress = probe_rx.recv_timeout(Duration::from_secs(2));
        drop(lease);
        control_progress
    });
    held_rx.await.unwrap();
    let (polled_tx, polled_rx) = tokio::sync::oneshot::channel();
    let local_handle = manager.handle();
    let processing = tokio::spawn(async move {
        let mut polled_tx = Some(polled_tx);
        let handler = async move {
            if let Some(message) = message {
                handle_node_event(
                    NodeEvent::HistoryReceived {
                        from: NodeId::new("summary-source"),
                        addr: "unused".into(),
                        message,
                    },
                    &context,
                )
                .await;
            } else {
                local_handle
                    .increment_gcounter("storage-wait", 7)
                    .await
                    .unwrap();
            }
        };
        let mut handler = std::pin::pin!(handler);
        entered_tx.send(()).unwrap();
        std::future::poll_fn(|cx| {
            let result = handler.as_mut().poll(cx);
            if let Some(polled_tx) = polled_tx.take() {
                polled_tx.send(()).unwrap();
            }
            result
        })
        .await;
    });
    polled_rx.await.unwrap();
    let during_storage = {
        let cursors = cursors.read().await;
        let cursor = &cursors["unused"];
        (cursor.after_origin.clone(), cursor.request_deadline)
    };
    let requester = Node::new(NodeConfig::new(NodeId::new("probe-source"), "127.0.0.1:0"));
    let probe = requester
        .control_request(
            owner.endpoint.as_ref().unwrap(),
            nx_net::ControlRequest {
                request_id: 1,
                owner: nx_net::MembershipRecord {
                    member: "probe-source".into(),
                    reporter: "probe-source".into(),
                    endpoint: None,
                    state: MembershipState::Alive,
                    version: nx_net::MembershipVersion {
                        epoch: 1,
                        incarnation: 0,
                    },
                },
                updates: vec![],
                kind: nx_net::ControlKind::Probe {
                    target: owner.member,
                    version: owner.version,
                },
            },
            Duration::from_millis(500),
        )
        .await;
    let acknowledged = probe.as_ref().is_ok_and(|(_, reply)| reply.ack);
    let delivered = probe_tx.send(acknowledged);
    processing.await.unwrap();
    let progress = holder.join().unwrap();
    let after_storage = {
        let cursors = cursors.read().await;
        let cursor = &cursors["unused"];
        (cursor.after_origin.clone(), cursor.request_deadline)
    };
    requester.shutdown().await;
    traced_shutdown(&mut manager).await;
    let receipts = replication_progress(&store).unwrap();
    let receipt = receipts
        .iter()
        .find(|receipt| receipt.origin == origin)
        .unwrap();
    match case {
        StorageWaitCase::Summary => {
            assert_eq!(after_storage, (None, None));
        }
        StorageWaitCase::Unavailable => {
            assert!(receipt.source_unavailable);
            assert_eq!(receipt.contiguous_sequence, 0);
        }
        StorageWaitCase::BaselineUnavailable => {
            assert_eq!(
                receipt.recovery_failure.as_ref().unwrap().reason,
                nx_sync::BaselineFailure::BudgetExceeded
            );
            assert_eq!(receipt.contiguous_sequence, 0);
        }
        StorageWaitCase::Push | StorageWaitCase::Local => {
            assert_eq!(receipt.contiguous_sequence, 1);
            assert_eq!(read_materialized(&store, "storage-wait"), 7);
            assert_eq!(
                read_durable_gcounter_state(&store, "storage-wait").value(),
                7
            );
            assert_eq!(manager.get_counter_value("storage-wait").await, 7);
        }
    }
    assert!(acknowledged, "{probe:?}");
    assert_eq!(
        progress,
        Ok(true),
        "{case:?}: control did not progress while the storage lease was held"
    );
    assert!(delivered.is_ok(), "{delivered:?}");
    assert_eq!(
        during_storage,
        (Some("previous-page".into()), Some(request_deadline)),
        "summary request was released before storage processing completed"
    );
}

#[tokio::test]
async fn history_summary_storage_failure_releases_request_and_reports_error() {
    let store = temp_store();
    let context = test_event_context(
        Arc::new(RwLock::new(HashMap::new())),
        Arc::new(RwLock::new(SeenOps::new(1024))),
        Arc::new(RwLock::new(Vec::new())),
        Arc::clone(&store),
        metrics(),
        Arc::new(RwLock::new(HashMap::new())),
    );
    context
        .history_cursors
        .write()
        .await
        .entry("source".into())
        .or_default()
        .request_deadline = Some(StdInstant::now() + Duration::from_secs(30));
    store.inject_disk_full_on_writes(true);
    handle_node_event(
        NodeEvent::HistoryReceived {
            from: NodeId::new("source"),
            addr: "source".into(),
            message: nx_sync::HistoryMessage::Summary {
                entries: vec![nx_sync::HistorySummary {
                    baseline_required: false,
                    origin: NodeId::new("origin"),
                    high_sequence: 1,
                }],
            },
        },
        &context,
    )
    .await;
    store.inject_disk_full_on_writes(false);
    assert!(
        context.history_cursors.read().await["source"]
            .request_deadline
            .is_none()
    );
    assert!(context.history_requests.read().await.is_empty());
    assert!(
        store
            .get(b"__nx/crdt/sequence/origin/origin")
            .unwrap()
            .is_none()
    );
    assert!(
        context
            .metrics
            .render_for_test(&store)
            .contains("numax_sync_errors_total 1")
    );
}

async fn wait_mutual_state(a: &SyncManager, b: &SyncManager, expected: MembershipState) {
    let admission_started = StdInstant::now();
    let admission = tokio::time::timeout(Duration::from_secs(30), async {
        let mut poll = tokio::time::interval(Duration::from_millis(10));
        loop {
            let complete = [(a, b), (b, a)].into_iter().all(|(local, remote)| {
                let snapshot = local.membership.as_ref().unwrap().snapshot.borrow();
                assert!(snapshot.error.is_none(), "{snapshot:?}");
                snapshot
                    .records
                    .iter()
                    .any(|r| r.member == remote.node_id.as_str() && r.state == expected)
            });
            if complete {
                break;
            }
            poll.tick().await;
        }
    })
    .await;
    if admission.is_err() {
        log_sync_phase("control-data-state", admission_started.elapsed(), &[a, b]);
    }
    admission.unwrap();
}

async fn real_control_data_partition(format: SerializationFormat) {
    let target_a = free_addr();
    let target_b = free_addr();
    let gate_a = TestDataGate::new(target_a.clone()).await;
    let gate_b = TestDataGate::new(target_b.clone()).await;
    let make = |id: &str, target: &str, advertised: &str, candidate: &str| {
        SyncManager::try_new_with_discovery(
            NodeId::new(id),
            SyncConfig::new()
                .with_listen_addr(target)
                .with_serialization_format(format)
                .with_anti_entropy_interval(Duration::from_millis(100))
                .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(40)),
            temp_store(),
            metrics(),
            vec![crate::DiscoveryProvider::new(
                "static",
                Arc::new(crate::StaticDiscovery::new(vec![candidate.into()])),
            )],
            crate::DiscoveryRuntimeConfig::new().with_advertised_endpoint(advertised),
        )
        .unwrap()
    };
    let mut a = make("tcp-left", &target_a, &gate_a.endpoint, &gate_b.endpoint);
    let mut b = make("tcp-right", &target_b, &gate_b.endpoint, &gate_a.endpoint);
    gate_a.open.send_replace(true);
    gate_b.open.send_replace(true);
    a.start().await.unwrap();
    b.start().await.unwrap();
    wait_mutual_state(&a, &b, MembershipState::Alive).await;
    let admission_started = StdInstant::now();
    let admission = tokio::time::timeout(Duration::from_secs(30), async {
        let mut poll = tokio::time::interval(Duration::from_millis(10));
        loop {
            if a.connected_peer_count().await > 0
                && b.connected_peer_count().await > 0
                && [&a, &b].into_iter().all(|m| {
                    m.membership
                        .as_ref()
                        .unwrap()
                        .snapshot
                        .borrow()
                        .successful_probes
                        > 0
                })
            {
                break;
            }
            poll.tick().await;
        }
    })
    .await;
    if admission.is_err() {
        log_sync_phase(
            "control-data-admission",
            admission_started.elapsed(),
            &[&a, &b],
        );
    }
    admission.unwrap();
    eprintln!(
        "TCP_ADMISSION codec={format:?} sessions_left={} sessions_right={}",
        a.connected_peer_count().await,
        b.connected_peer_count().await
    );
    for (local, remote) in [(&a, &b), (&b, &a)] {
        assert!(
            local
                .node
                .as_ref()
                .unwrap()
                .connected_peers()
                .await
                .iter()
                .all(|(_, id)| id == &remote.node_id)
        );
    }
    gate_a.open.send_replace(false);
    gate_b.open.send_replace(false);
    local_six_family_workload(&a.handle(), 0).await;
    local_six_family_workload(&b.handle(), 2).await;
    wait_mutual_state(&a, &b, MembershipState::Dead).await;
    assert_durable_dead(&a.store, b.node_id.as_str());
    assert_durable_dead(&b.store, a.node_id.as_str());
    assert_eq!(a.connected_peer_count().await, 0);
    assert_eq!(b.connected_peer_count().await, 0);
    assert_eq!(a.get_counter_value("det-g").await, 7);
    assert_eq!(b.get_counter_value("det-g").await, 7);
    let probes: Vec<_> = [&a, &b]
        .into_iter()
        .map(|m| {
            m.membership
                .as_ref()
                .unwrap()
                .snapshot
                .borrow()
                .successful_probes
        })
        .collect();
    gate_a.open.send_replace(true);
    gate_b.open.send_replace(true);
    let rejoined = StdInstant::now();
    wait_mutual_state(&a, &b, MembershipState::Alive).await;
    tokio::time::timeout_at(
        Instant::from_std(rejoined + Duration::from_secs(30)),
        async {
            let mut poll = tokio::time::interval(Duration::from_millis(10));
            loop {
                let mut complete = true;
                for (index, manager) in [&a, &b].into_iter().enumerate() {
                    complete &= manager.get_counter_value("det-g").await == 14
                        && manager
                            .membership
                            .as_ref()
                            .unwrap()
                            .snapshot
                            .borrow()
                            .successful_probes
                            > probes[index]
                        && manager
                            .handle()
                            .replication_progress()
                            .unwrap()
                            .iter()
                            .filter(|p| {
                                p.contiguous_sequence == 12
                                    && p.sparse_positions == 0
                                    && p.recovery_failure.is_none()
                            })
                            .count()
                            == 2;
                }
                if complete {
                    break;
                }
                poll.tick().await;
            }
        },
    )
    .await
    .unwrap();
    assert!(rejoined.elapsed() < Duration::from_secs(30));
    assert_eq!(six_family_states(&a.store), six_family_states(&b.store));
    for manager in [&a, &b] {
        assert_six_family_memory(manager).await;
        assert_six_family_materialized(&manager.store);
        let progress = manager.handle().replication_progress().unwrap();
        assert_eq!(progress.len(), 2);
        assert!(progress.iter().all(|p| p.contiguous_sequence == 12
            && p.sparse_positions == 0
            && p.recovery_failure.is_none()));
        assert!(
            manager
                .membership
                .as_ref()
                .unwrap()
                .snapshot
                .borrow()
                .records
                .iter()
                .find(|r| r.member == manager.node_id.as_str())
                .unwrap()
                .version
                .incarnation
                > 0
        );
    }
    traced_shutdown(&mut a).await;
    traced_shutdown(&mut b).await;
}

#[tokio::test]
async fn rotating_full_slot_recovers_six_families_after_data_partition() {
    if run_in_isolated_test_process(
        "sync_manager::manager::tests::topology::rotating_full_slot_recovers_six_families_after_data_partition",
    ) {
        return;
    }
    for format in [SerializationFormat::Json, SerializationFormat::Bincode] {
        let config = || {
            SyncConfig::new()
                .with_listen_addr("127.0.0.1:0")
                .with_max_peers(1)
                .with_serialization_format(format)
                .with_anti_entropy_interval(Duration::from_millis(10))
                .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(40))
        };
        let (mut a, _, sa) = started_manager_with_config(config()).await;
        let (mut b, hb, sb) = started_manager_with_config(config()).await;
        let (mut c, hc, sc) = started_manager_with_config(config()).await;
        for manager in [&mut a, &mut b, &mut c] {
            stop_test_reconnect(manager).await;
        }
        let mut b_contact = test_owner_record(&mut b).await;
        let mut c_contact = test_owner_record(&mut c).await;
        let gate = TestDataGate::new(c_contact.endpoint.clone().unwrap()).await;
        c_contact.endpoint = Some(gate.endpoint.clone());
        // Inject terminal failure judgments into the production reconnect input.
        // This tests recovery from Dead, not detector accuracy or packet loss.
        b_contact.state = MembershipState::Dead;
        c_contact.state = MembershipState::Dead;
        a.connect_to_peer(b_contact.endpoint.as_ref().unwrap())
            .await
            .unwrap();
        wait_for_connected_peer(&b).await;
        let _inputs = test_topology_reconnect(
            &mut a,
            vec![b_contact, c_contact],
            Duration::from_millis(100),
        )
        .await;
        for handle in [&hb, &hc] {
            local_increment(handle, "topology-g", 1).await;
            local_pncounter_inc(handle, "topology-p", 2).await;
            local_pncounter_dec(handle, "topology-p", 1).await;
            local_lww_register_set(handle, "topology-r", b"value", 100).await;
            local_lww_map_set(handle, "topology-m", "field", b"value", 100).await;
            local_orset_add(handle, "topology-o", "removed").await;
            local_orset_remove(handle, "topology-o", "removed").await;
            local_orset_add(handle, "topology-o", "value").await;
            let removed = local_rga_insert_after(handle, "topology-a", None, b"removed").await;
            local_rga_delete(handle, "topology-a", &removed).await;
            local_rga_insert_after(handle, "topology-a", None, b"value").await;
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut poll = tokio::time::interval(Duration::from_millis(5));
            while gate.rejected.load(std::sync::atomic::Ordering::Relaxed) == 0 {
                assert!(a.connected_peer_count().await <= 1);
                poll.tick().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(c.get_counter_value("topology-g").await, 1);
        let expected = [
            read_durable_gcounter_state(&sb, "topology-g")
                .merged_with(&read_durable_gcounter_state(&sc, "topology-g"))
                .to_json()
                .unwrap(),
            read_durable_pncounter_state(&sb, "topology-p")
                .merged_with(&read_durable_pncounter_state(&sc, "topology-p"))
                .to_json()
                .unwrap(),
            read_durable_lww_register_state(&sb, "topology-r")
                .merged_with(&read_durable_lww_register_state(&sc, "topology-r"))
                .to_json()
                .unwrap(),
            read_durable_lww_map_state(&sb, "topology-m")
                .merged_with(&read_durable_lww_map_state(&sc, "topology-m"))
                .to_json()
                .unwrap(),
            read_durable_orset_state(&sb, "topology-o")
                .merged_with(&read_durable_orset_state(&sc, "topology-o"))
                .to_json()
                .unwrap(),
            read_durable_rga_state(&sb, "topology-a")
                .merged_with(&read_durable_rga_state(&sc, "topology-a"))
                .to_json()
                .unwrap(),
        ]
        .map(|json| serde_json::from_str::<serde_json::Value>(&json).unwrap());
        gate.open.send_replace(true);
        let phase_started = StdInstant::now();
        let mut longest_scan = Duration::ZERO;
        let mut scans = 0;
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            let mut poll = tokio::time::interval(Duration::from_millis(5));
            loop {
                let mut complete = true;
                for manager in [&a, &b, &c] {
                    assert!(manager.connected_peer_count().await <= 1);
                    complete &= manager.get_counter_value("topology-g").await == 2
                        && manager.get_pncounter_value("topology-p").await == 2
                        && manager.get_lww_register_value("topology-r").await
                            == Some(b"value".to_vec())
                        && manager.get_lww_map_entries("topology-m").await
                            == vec![("field".into(), b"value".to_vec())]
                        && manager.get_orset_elements("topology-o").await
                            == vec!["value".to_string()]
                        && manager.get_rga_values("topology-a").await == vec![b"value".to_vec(); 2];
                    let progress = manager.handle().replication_progress().unwrap();
                    complete &= [&b.node_id, &c.node_id].into_iter().all(|origin| {
                        progress.iter().any(|entry| {
                            &entry.origin == origin
                                && entry.contiguous_sequence == 11
                                && entry.known_high_sequence == 11
                                && entry.sparse_positions == 0
                                && entry.recovery_failure.is_none()
                                && !entry.baseline_missing
                                && !entry.baseline_unconfirmed
                        })
                    });
                }
                if complete {
                    let scan_started = StdInstant::now();
                    complete = [&sa, &sb, &sc].iter().all(|store| {
                        [
                            read_durable_gcounter_state(store, "topology-g")
                                .to_json()
                                .unwrap(),
                            read_durable_pncounter_state(store, "topology-p")
                                .to_json()
                                .unwrap(),
                            read_durable_lww_register_state(store, "topology-r")
                                .to_json()
                                .unwrap(),
                            read_durable_lww_map_state(store, "topology-m")
                                .to_json()
                                .unwrap(),
                            read_durable_orset_state(store, "topology-o")
                                .to_json()
                                .unwrap(),
                            read_durable_rga_state(store, "topology-a")
                                .to_json()
                                .unwrap(),
                        ]
                        .map(|json| serde_json::from_str::<serde_json::Value>(&json).unwrap())
                            == expected
                    });
                    longest_scan = longest_scan.max(scan_started.elapsed());
                    scans += 1;
                }
                if complete {
                    break;
                }
                poll.tick().await;
            }
        })
        .await;
        if result.is_err() {
            log_sync_phase(
                "rotating-slot-recovery",
                phase_started.elapsed(),
                &[&a, &b, &c],
            );
            eprintln!(
                "SYNC_SCAN codec={format:?} count={scans} longest_ms={}",
                longest_scan.as_millis(),
            );
        }
        result.unwrap();
        for store in [&sa, &sb, &sc] {
            assert_eq!(read_materialized(store, "topology-g"), 2);
            assert_eq!(read_durable_gcounter_state(store, "topology-g").value(), 2);
            assert_eq!(read_materialized_pncounter(store, "topology-p"), 2);
            assert_eq!(read_durable_pncounter_state(store, "topology-p").value(), 2);
            assert_eq!(
                read_materialized_lww_register(store, "topology-r"),
                b"value"
            );
            assert_eq!(
                read_durable_lww_register_state(store, "topology-r").value(),
                b"value"
            );
            assert_eq!(
                read_materialized_lww_map(store, "topology-m"),
                vec![("field".into(), b"value".to_vec())]
            );
            assert_eq!(
                read_durable_lww_map_state(store, "topology-m").entries(),
                vec![("field".into(), b"value".to_vec())]
            );
            assert_eq!(
                read_materialized_orset(store, "topology-o"),
                vec!["value".to_string()]
            );
            assert_eq!(
                read_durable_orset_state(store, "topology-o").elements(),
                vec!["value".to_string()]
            );
            assert_eq!(
                read_materialized_rga(store, "topology-a"),
                vec![b"value".to_vec(); 2]
            );
            assert_eq!(
                read_durable_rga_state(store, "topology-a").values(),
                vec![b"value".to_vec(); 2]
            );
        }
        for manager in [&mut a, &mut b, &mut c] {
            manager.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn two_full_pairs_reconnect_after_mutual_dead_data_partition() {
    if run_in_isolated_test_process(
        "sync_manager::manager::tests::topology::two_full_pairs_reconnect_after_mutual_dead_data_partition",
    ) {
        return;
    }
    let config = || {
        SyncConfig::new()
            .with_listen_addr("127.0.0.1:0")
            .with_max_peers(1)
            .with_anti_entropy_interval(Duration::from_millis(10))
    };
    let (mut a, ha, _) = started_manager_with_config(config()).await;
    let (mut b, _, _) = started_manager_with_config(config()).await;
    let (mut c, hc, _) = started_manager_with_config(config()).await;
    let (mut d, _, _) = started_manager_with_config(config()).await;
    for manager in [&mut a, &mut b, &mut c, &mut d] {
        stop_test_reconnect(manager).await;
    }
    let mut ra = test_owner_record(&mut a).await;
    let rb = test_owner_record(&mut b).await;
    let mut rc = test_owner_record(&mut c).await;
    let rd = test_owner_record(&mut d).await;
    a.connect_to_peer(rb.endpoint.as_ref().unwrap())
        .await
        .unwrap();
    c.connect_to_peer(rd.endpoint.as_ref().unwrap())
        .await
        .unwrap();
    let gate_a = TestDataGate::new(ra.endpoint.clone().unwrap()).await;
    let gate_c = TestDataGate::new(rc.endpoint.clone().unwrap()).await;
    ra.endpoint = Some(gate_a.endpoint.clone());
    rc.endpoint = Some(gate_c.endpoint.clone());
    ra.state = MembershipState::Dead;
    rc.state = MembershipState::Dead;
    let _inputs_a = test_topology_reconnect(&mut a, vec![rb, rc], Duration::from_millis(100)).await;
    let _inputs_c = test_topology_reconnect(&mut c, vec![rd, ra], Duration::from_millis(130)).await;
    local_increment(&ha, "partition-left", 7).await;
    local_increment(&hc, "partition-right", 11).await;
    wait_for_counter(&b, "partition-left", 7).await;
    wait_for_counter(&d, "partition-right", 11).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut poll = tokio::time::interval(Duration::from_millis(5));
        while gate_a.rejected.load(std::sync::atomic::Ordering::Relaxed) == 0
            || gate_c.rejected.load(std::sync::atomic::Ordering::Relaxed) == 0
        {
            poll.tick().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(b.get_counter_value("partition-right").await, 0);
    assert_eq!(d.get_counter_value("partition-left").await, 0);
    gate_a.open.send_replace(true);
    gate_c.open.send_replace(true);
    let phase_started = StdInstant::now();
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        let mut poll = tokio::time::interval(Duration::from_millis(5));
        loop {
            let mut complete = true;
            for manager in [&a, &b, &c, &d] {
                assert!(manager.connected_peer_count().await <= 1);
                complete &= manager.get_counter_value("partition-left").await == 7
                    && manager.get_counter_value("partition-right").await == 11;
            }
            if complete {
                break;
            }
            poll.tick().await;
        }
    })
    .await;
    if result.is_err() {
        log_sync_phase(
            "two-pair-recovery",
            phase_started.elapsed(),
            &[&a, &b, &c, &d],
        );
    }
    result.unwrap();
    for manager in [&mut a, &mut b, &mut c, &mut d] {
        traced_shutdown(manager).await;
    }
}

#[tokio::test]
async fn admitted_owner_endpoint_becomes_a_data_neighbor_without_discovery() {
    let config = || {
        SyncConfig::new()
            .with_listen_addr("127.0.0.1:0")
            .with_max_peers(1)
    };
    let (mut a, _, _) = started_manager_with_config(config()).await;
    let (mut b, _, _) = started_manager_with_config(config()).await;
    let (mut c, _, _) = started_manager_with_config(config()).await;
    for manager in [&mut a, &mut b, &mut c] {
        stop_test_reconnect(manager).await;
    }

    let owner_a = test_owner_record(&mut a).await;
    let owner_b = test_owner_record(&mut b).await;
    let owner_c = test_owner_record(&mut c).await;
    a.connect_to_peer(owner_b.endpoint.as_ref().unwrap())
        .await
        .unwrap();
    c.node
        .as_ref()
        .unwrap()
        .control_request(
            owner_a.endpoint.as_ref().unwrap(),
            nx_net::ControlRequest {
                request_id: 1,
                owner: owner_c,
                updates: vec![],
                kind: nx_net::ControlKind::Exchange { after: None },
            },
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    let (_candidates_tx, candidates_rx) = tokio::sync::watch::channel(Arc::new(vec![]));
    a.reconnect_task = spawn_reconnect_loop(ReconnectLoopContext {
        local_id: a.node_id.clone(),
        membership: a.membership.as_ref().unwrap().snapshot.clone(),
        rotation_interval: Duration::from_millis(100),
        recovery_rotation: None,
        node: (a.node.as_ref().unwrap()).clone(),
        candidates_rx,
        max_peers: 1,
        initial_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(40),
        peer_dead_after_failures: 1,
        shutdown_rx: a.shutdown_tx.subscribe(),
        metrics: Arc::clone(&a.metrics),
        peer_health: Arc::clone(&a.peer_health),
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut poll = tokio::time::interval(Duration::from_millis(5));
        while !a
            .node
            .as_ref()
            .unwrap()
            .connected_peers()
            .await
            .iter()
            .any(|(_, id)| id == &c.node_id)
        {
            assert!(a.connected_peer_count().await <= 1);
            poll.tick().await;
        }
    })
    .await
    .unwrap();
    assert!(a.peer_candidates().is_empty());
    for manager in [&mut a, &mut b, &mut c] {
        manager.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn recovery_rotation_replaces_the_unavailable_source_with_an_alternative() {
    let mut local = nx_net::Node::new(nx_net::NodeConfig::new(
        NodeId::new("recovery-dialer"),
        "127.0.0.1:0",
    ));
    let _local_events = local.take_event_receiver().unwrap();
    local.start_listener().await.unwrap();

    let mut source = nx_net::Node::new(nx_net::NodeConfig::new(
        NodeId::new("unavailable-source"),
        "127.0.0.1:0",
    ));
    let _source_events = source.take_event_receiver().unwrap();
    let source_endpoint = source.start_listener().await.unwrap().to_string();

    let alternative_id = NodeId::new("alternative-source");
    let mut alternative = nx_net::Node::new(nx_net::NodeConfig::new(
        alternative_id.clone(),
        "127.0.0.1:0",
    ));
    let _alternative_events = alternative.take_event_receiver().unwrap();
    let alternative_endpoint = alternative.start_listener().await.unwrap().to_string();

    local.connect_to_peer(&source_endpoint).await.unwrap();
    let node = Arc::new(local);
    let (_candidates_tx, candidates_rx) = watch::channel(Arc::new(vec![alternative_endpoint]));
    let (_membership_tx, membership) =
        watch::channel(crate::membership::runtime::Snapshot::default());
    let (rotation_tx, rotation_rx) =
        watch::channel(crate::sync_manager::types::RecoveryRotation::default());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = spawn_reconnect_loop(ReconnectLoopContext {
        local_id: NodeId::new("recovery-dialer"),
        membership,
        rotation_interval: Duration::from_secs(3600),
        recovery_rotation: Some(rotation_rx),
        node: node.clone(),
        candidates_rx,
        max_peers: 1,
        initial_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(40),
        peer_dead_after_failures: 1,
        shutdown_rx,
        metrics: Arc::new(RuntimeMetrics::default()),
        peer_health: Arc::new(RwLock::new(HashMap::new())),
    });

    rotation_tx.send_replace(crate::sync_manager::types::RecoveryRotation {
        generation: 1,
        source_addr: Some(source_endpoint),
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let connected = node.connected_peers().await;
            assert!(connected.len() <= 1);
            if connected.iter().any(|(_, id)| id == &alternative_id) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("recovery rotation did not connect the alternative source");

    shutdown_tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), task.unwrap())
        .await
        .unwrap()
        .unwrap();
    node.shutdown().await;
    source.shutdown().await;
    alternative.shutdown().await;
}
