use super::*;

#[tokio::test]
async fn byte_rejected_attempts_survive_restart_and_repair_every_family() {
    byte_rejected_recovery(false).await;
}

#[tokio::test]
async fn byte_rejected_remote_forwarding_survives_restart_and_repairs_every_family() {
    byte_rejected_recovery(true).await;
}

async fn byte_rejected_recovery(remote_forwarding: bool) {
    for format in [
        nx_net::SerializationFormat::Json,
        nx_net::SerializationFormat::Bincode,
    ] {
        let config = || {
            SyncConfig::new()
                .with_listen_addr("127.0.0.1:0")
                .with_anti_entropy_interval(Duration::from_millis(10))
                .with_serialization_format(format)
        };
        let mut sa = temp_store();
        let mut original = SyncManager::try_new(
            NodeId::new("byte-limited-source"),
            config(),
            Arc::clone(&sa),
            metrics(),
        )
        .unwrap();
        // Exercise the production byte admission policy with a smaller allowance.
        // Every operation is accepted durably, and every volatile attempt drops.
        let (tx, rx) = crate::sync_manager::delivery::channel(16, 1, Arc::clone(&original.metrics));
        original.op_tx = tx;
        original.op_rx = Some(rx);
        let ha = original.handle();
        local_increment(&ha, "byte-g", 7).await;
        local_pncounter_inc(&ha, "byte-p", 9).await;
        local_pncounter_dec(&ha, "byte-p", 4).await;
        local_lww_register_set(&ha, "byte-r", b"winner", 200).await;
        local_lww_map_set(&ha, "byte-m", "field", b"removed", 100).await;
        local_lww_map_remove(&ha, "byte-m", "field", 200).await;
        local_orset_add(&ha, "byte-o", "removed").await;
        local_orset_remove(&ha, "byte-o", "removed").await;
        let removed = local_rga_insert_after(&ha, "byte-a", None, b"removed").await;
        local_rga_delete(&ha, "byte-a", &removed).await;
        assert_eq!(original.op_rx.as_ref().unwrap().len(), 0);
        assert_eq!(original.op_log.read().await.len(), 10);
        let progress = ha.replication_progress().unwrap();
        let replay = original
            .op_log
            .read()
            .await
            .iter()
            .map(|op| sequenced_op(&sa, op).unwrap().unwrap())
            .collect::<Vec<_>>();
        drop(ha);
        drop(original);
        let restart_identity = if remote_forwarding {
            let relay_store = temp_store();
            let relay = SyncManager::try_new(
                NodeId::new("relay"),
                config(),
                Arc::clone(&relay_store),
                metrics(),
            )
            .unwrap();
            let mut context = test_event_context(
                Arc::clone(&relay.counters),
                Arc::clone(&relay.seen_ops),
                Arc::clone(&relay.op_log),
                Arc::clone(&relay_store),
                Arc::clone(&relay.metrics),
                Arc::clone(&relay.peer_health),
            );
            let (forward_tx, forward_rx) =
                crate::sync_manager::delivery::channel(16, 1, Arc::clone(&relay.metrics));
            context.forward_tx = Some(forward_tx);
            handle_node_event(
                NodeEvent::HistoryReceived {
                    from: NodeId::new("byte-limited-source"),
                    addr: "127.0.0.1:9000".into(),
                    message: nx_sync::HistoryMessage::Push { ops: replay },
                },
                &context,
            )
            .await;
            assert_eq!(forward_rx.len(), 0);
            assert_eq!(relay.op_log.read().await.len(), 10);
            assert_eq!(relay.handle().replication_progress().unwrap(), progress);
            drop(context);
            drop(relay);
            sa = relay_store;
            NodeId::new("relay")
        } else {
            NodeId::new("byte-limited-source")
        };
        // Rehydrate with no volatile delivery attempts left to drive convergence.
        let mut a =
            SyncManager::try_new(restart_identity, config(), Arc::clone(&sa), metrics()).unwrap();
        assert_eq!(a.handle().replication_progress().unwrap(), progress);
        a.start().await.unwrap();
        let (mut b, _, sb) = started_manager_with_config(config()).await;
        let endpoint = test_owner_record(&mut a).await.endpoint.unwrap();
        b.connect_to_peer(&endpoint).await.unwrap();
        wait_for_counter(&b, "byte-g", 7).await;
        wait_for_pncounter(&b, "byte-p", 5).await;
        wait_for_lww_register(&b, "byte-r", b"winner").await;
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut poll = tokio::time::interval(Duration::from_millis(5));
            loop {
                let complete = [
                    durable_gcounter_state_key("byte-g"),
                    durable_pncounter_state_key("byte-p"),
                    durable_lww_register_state_key("byte-r"),
                    durable_lww_map_state_key("byte-m"),
                    durable_orset_state_key("byte-o"),
                    durable_rga_state_key("byte-a"),
                ]
                .iter()
                .all(|key| match (sa.get(key).unwrap(), sb.get(key).unwrap()) {
                    (Some(left), Some(right)) => {
                        serde_json::from_slice::<serde_json::Value>(&left).unwrap()
                            == serde_json::from_slice::<serde_json::Value>(&right).unwrap()
                    }
                    _ => false,
                });
                if complete {
                    break;
                }
                poll.tick().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(read_materialized(&sb, "byte-g"), 7);
        assert_eq!(read_materialized_pncounter(&sb, "byte-p"), 5);
        assert_eq!(read_materialized_lww_register(&sb, "byte-r"), b"winner");
        assert!(read_materialized_lww_map(&sb, "byte-m").is_empty());
        assert!(read_materialized_orset(&sb, "byte-o").is_empty());
        assert!(read_materialized_rga(&sb, "byte-a").is_empty());
        let remote_progress = b.handle().replication_progress().unwrap();
        for expected in progress {
            assert!(
                remote_progress
                    .iter()
                    .any(|remote| remote.origin == expected.origin
                        && remote.contiguous_sequence >= expected.contiguous_sequence)
            );
        }
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn local_batch_splits_at_wire_budget_without_anti_entropy() {
    for format in [
        nx_net::SerializationFormat::Json,
        nx_net::SerializationFormat::Bincode,
    ] {
        let config = || {
            SyncConfig::new()
                .with_listen_addr("127.0.0.1:0")
                .with_max_message_size(4096)
                .with_serialization_format(format)
        };
        let (mut a, ha, _) = started_manager_with_config(config()).await;
        let (mut b, _, _) = started_manager_with_config(config()).await;
        for manager in [&mut a, &mut b] {
            if let Some(task) = manager.anti_entropy_task.take() {
                task.abort();
                let _ = task.await;
            }
        }
        let endpoint = test_owner_record(&mut b).await.endpoint.unwrap();
        a.connect_to_peer(&endpoint).await.unwrap();
        let value = vec![b'x'; 600];
        // Buffer all operations before starting the drain so one batch would
        // exceed 4096 bytes in both codecs without byte-aware splitting.
        let task = a.broadcast_task.take().unwrap();
        task.abort();
        let _ = task.await;
        let (tx, mut rx) = crate::sync_manager::delivery::channel(
            16,
            crate::sync_manager::delivery::QUEUE_BYTES,
            Arc::clone(&a.metrics),
        );
        for i in 0..8 {
            let op = ha
                .set_lww_register(&format!("wire-{i}"), value.clone(), 100)
                .await
                .unwrap();
            tx.try_send(op).unwrap();
        }
        let snapshot = a.membership.as_ref().unwrap().snapshot.clone();
        let context = crate::sync_manager::types::BroadcastLoopContext {
            max_message_size: 4096,
            pacing: Arc::new(StdMutex::new(crate::sync_manager::pacing::Pacer::new(
                a.config.gossip_rate,
                a.config.max_peers,
            ))),
            local_id: a.node_id.clone(),
            membership: snapshot,
            node: Arc::clone(a.node.as_ref().unwrap()),
            metrics: Arc::clone(&a.metrics),
            fanout_constant: a.config.gossip_fanout_constant,
            max_fanout: a.config.gossip_max_fanout,
            fanout_cursor: Arc::clone(&a.fanout_cursor),
        };
        drain_broadcast_queue(&context, &mut rx, None).await;
        for i in 0..8 {
            wait_for_lww_register(&b, &format!("wire-{i}"), &value).await;
        }
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn exhausted_repair_budget_defers_then_recovers_without_losing_history() {
    let config = || {
        SyncConfig::new()
            .with_listen_addr("127.0.0.1:0")
            .with_anti_entropy_interval(Duration::from_millis(10))
    };
    let (mut a, ha, _) = started_manager_with_config(config()).await;
    let (mut b, _, _) = started_manager_with_config(config()).await;
    for manager in [&mut a, &mut b] {
        if let Some(task) = manager.anti_entropy_task.take() {
            task.abort();
            let _ = task.await;
        }
    }
    // No connected peer can receive the first send. Repair is the sole path.
    local_increment(&ha, "repair-budget", 17).await;
    let task = a.broadcast_task.take().unwrap();
    task.abort();
    let _ = task.await;
    let endpoint = test_owner_record(&mut a).await.endpoint.unwrap();
    b.connect_to_peer(&endpoint).await.unwrap();
    let budget = Arc::new(StdMutex::new(
        crate::sync_manager::delivery::RepairBudget::new(StdInstant::now()),
    ));
    assert!(
        budget
            .lock()
            .unwrap()
            .admit(crate::sync_manager::delivery::TURN_BYTES, StdInstant::now())
    );
    assert!(!budget.lock().unwrap().admit(1, StdInstant::now()));
    b.anti_entropy_task = spawn_anti_entropy_loop(AntiEntropyLoopContext {
        jitter_seed: 42,
        repair_budget: budget,
        history_cursors: Arc::clone(&b.history_cursors),
        // Retry deadlines are short, while the TCP idle timeout stays at its
        // production default: the deliberately exhausted allowance lasts 1 s.
        request_timeout: Duration::from_millis(200),
        node: Arc::clone(b.node.as_ref().unwrap()),
        interval: b.config.anti_entropy_interval,
        shutdown_rx: b.shutdown_tx.subscribe(),
        metrics: Arc::clone(&b.metrics),
    });
    wait_for_counter(&b, "repair-budget", 17).await;
    let progress = b.handle().replication_progress().unwrap();
    assert!(progress.iter().any(|p| p.origin == *ha.node_id()
        && p.contiguous_sequence == 1
        && p.sparse_positions == 0
        && p.recovery_failure.is_none()));
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn replay_pages_respect_small_wire_budget() {
    wire_limited_recovery(false).await;
}

#[tokio::test]
async fn baseline_that_cannot_fit_wire_budget_fails_explicitly() {
    wire_limited_recovery(true).await;
}

async fn wire_limited_recovery(expire_history: bool) {
    for format in [
        nx_net::SerializationFormat::Json,
        nx_net::SerializationFormat::Bincode,
    ] {
        let config = || {
            SyncConfig::new()
                .with_listen_addr("127.0.0.1:0")
                .with_max_message_size(4096)
                .with_serialization_format(format)
                .with_op_log_limit(if expire_history { 1 } else { 32 })
                .with_anti_entropy_interval(Duration::from_millis(10))
        };
        let (mut a, ha, _) = started_manager_with_config(config()).await;
        let (mut b, _, _) = started_manager_with_config(config()).await;
        // There is no delivery attempt: only the production repair path can help.
        let value = vec![b'x'; 600];
        for index in 0..8 {
            ha.set_lww_register(&format!("repair-wire-{index}"), value.clone(), 100)
                .await
                .unwrap();
        }
        let endpoint = test_owner_record(&mut a).await.endpoint.unwrap();
        b.connect_to_peer(&endpoint).await.unwrap();
        if expire_history {
            tokio::time::timeout(Duration::from_secs(5), async {
                let mut poll = tokio::time::interval(Duration::from_millis(5));
                loop {
                    if b.handle()
                        .replication_progress()
                        .unwrap()
                        .iter()
                        .any(|progress| {
                            progress.recovery_failure.as_ref().is_some_and(|failure| {
                                failure.reason == nx_sync::BaselineFailure::BudgetExceeded
                            })
                        })
                    {
                        break;
                    }
                    poll.tick().await;
                }
            })
            .await
            .unwrap();
            assert!(
                b.handle()
                    .replication_progress()
                    .unwrap()
                    .iter()
                    .any(|p| p.contiguous_sequence < p.known_high_sequence)
            );
        } else {
            for index in 0..8 {
                wait_for_lww_register(&b, &format!("repair-wire-{index}"), &value).await;
            }
            assert!(
                b.handle()
                    .replication_progress()
                    .unwrap()
                    .iter()
                    .any(|p| p.contiguous_sequence == 8 && p.sparse_positions == 0)
            );
        }
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    }
}
