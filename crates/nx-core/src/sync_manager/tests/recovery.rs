use super::*;
use crate::sync_manager::baseline;
use nx_sync::{BaselineFailure, CrdtBaseline, HistoryMessage, OpPosition, SequencedOp};

fn manager(id: &str, store: Arc<NxStore>) -> SyncManager {
    SyncManager::try_new(
        NodeId::new(id),
        SyncConfig::new()
            .with_op_log_limit(1)
            .with_seen_ops_limit(1),
        store,
        metrics(),
    )
    .unwrap()
}

fn operations() -> Vec<SequencedOp> {
    let kinds = vec![
        OpKind::GCounterIncrement {
            key: "counter".into(),
            increment: 7,
        },
        OpKind::PNCounterIncrement {
            key: "pn".into(),
            increment: 9,
        },
        OpKind::PNCounterDecrement {
            key: "pn".into(),
            decrement: 4,
        },
        OpKind::LwwRegisterSet {
            key: "register".into(),
            value: b"winner".to_vec(),
            timestamp_ms: 8,
        },
        OpKind::LwwMapSet {
            key: "map".into(),
            field: "field".into(),
            value: vec![1],
            timestamp_ms: 7,
        },
        OpKind::LwwMapRemove {
            key: "map".into(),
            field: "field".into(),
            timestamp_ms: 8,
        },
        OpKind::ORSetAdd {
            key: "set".into(),
            element: "removed".into(),
            tag: "tag".into(),
        },
        OpKind::ORSetRemove {
            key: "set".into(),
            element: "removed".into(),
            observed_tags: vec!["tag".into()],
        },
        OpKind::RgaInsert {
            key: "rga".into(),
            id: "root".into(),
            parent: None,
            value: vec![1],
        },
        OpKind::RgaDelete {
            key: "rga".into(),
            id: "root".into(),
        },
        OpKind::RgaInsert {
            key: "rga".into(),
            id: "child".into(),
            parent: Some("root".into()),
            value: vec![2],
        },
        OpKind::GCounterIncrement {
            key: "counter".into(),
            increment: 3,
        },
    ];
    kinds
        .into_iter()
        .enumerate()
        .map(|(index, kind)| SequencedOp {
            op: Op {
                id: nx_sync::OpId::new(format!("op-{index}")),
                origin: NodeId::new("writer"),
                kind,
            },
            position: OpPosition {
                generation: 1,
                sequence: index as u64 + 1,
            },
        })
        .collect()
}

fn numeric_operations() -> Vec<SequencedOp> {
    ["writer", "other"]
        .into_iter()
        .flat_map(|origin| {
            [
                Op::gcounter_increment(
                    NodeId::new(origin),
                    "counter",
                    if origin == "writer" { u64::MAX } else { 1 },
                ),
                Op::pncounter_increment(NodeId::new(origin), "pn", u64::MAX),
                Op::pncounter_decrement(
                    NodeId::new(origin),
                    "pn",
                    if origin == "writer" {
                        u64::MAX
                    } else {
                        u64::MAX - 5
                    },
                ),
            ]
            .into_iter()
            .enumerate()
            .map(|(index, op)| SequencedOp {
                op,
                position: OpPosition {
                    generation: 1,
                    sequence: index as u64 + 1,
                },
            })
        })
        .collect()
}

async fn assert_numeric_values(manager: &SyncManager, gcounter: u64, pncounter: i64) {
    assert_eq!(manager.get_counter_value("counter").await, gcounter);
    assert_eq!(manager.get_pncounter_value("pn").await, pncounter);
    assert_eq!(read_materialized(&manager.store, "counter"), gcounter);
    assert_eq!(read_materialized_pncounter(&manager.store, "pn"), pncounter);
    assert_eq!(
        read_durable_gcounter_state(&manager.store, "counter").value(),
        gcounter
    );
    assert_eq!(
        read_durable_pncounter_state(&manager.store, "pn").value(),
        pncounter
    );
}

async fn seed(manager: &SyncManager) {
    apply_sequenced_ops(&operations(), &remote_apply_context(manager))
        .await
        .unwrap();
}

async fn transfer(source: &SyncManager, target: &SyncManager) -> anyhow::Result<()> {
    let payload = baseline::export(&remote_apply_context(source)).await?;
    baseline::import(
        baseline::VERSION,
        &payload,
        blake3::hash(&payload).as_bytes(),
        &NodeId::new("writer"),
        &remote_apply_context(target),
    )
    .await
}

fn assert_durable_equal(source: &NxStore, target: &NxStore) {
    for table in crate::sync_manager::schema::StoreTable::ALL
        .into_iter()
        .take(12)
    {
        let expected = source.scan_prefix(table.data_prefix().as_bytes()).unwrap();
        let actual = target.scan_prefix(table.data_prefix().as_bytes()).unwrap();
        assert_eq!(expected.len(), actual.len(), "{table}");
        for ((key, expected), (actual_key, actual)) in expected.iter().zip(&actual) {
            assert_eq!(key, actual_key);
            if table.name().ends_with("state") {
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(expected).unwrap(),
                    serde_json::from_slice::<serde_json::Value>(actual).unwrap(),
                    "{table}"
                );
            } else {
                assert_eq!(expected, actual, "{table}");
            }
        }
    }
}

#[tokio::test]
async fn baseline_all_families_cover_expired_history_sparse_receipts_and_delayed_duplicates() {
    let source = manager("source", temp_store());
    let target = manager("target", temp_store());
    seed(&source).await;
    // Receiver already applied the later increment: max-merging partial slots would be unsafe.
    apply_sequenced_ops(
        &[operations().pop().unwrap()],
        &remote_apply_context(&target),
    )
    .await
    .unwrap();
    assert_eq!(target.get_counter_value("counter").await, 3);
    assert_eq!(source.op_log.read().await.len(), 1);
    transfer(&source, &target).await.unwrap();
    assert_eq!(target.get_counter_value("counter").await, 10);
    assert_durable_equal(&source.store, &target.store);
    let progress = target.handle().replication_progress().unwrap();
    assert_eq!(progress[0].contiguous_sequence, 12);
    assert_eq!(progress[0].sparse_positions, 0);
    assert!(!progress[0].baseline_unconfirmed);
    // All old positions remain duplicates after seen/op-log eviction.
    apply_sequenced_ops(&operations(), &remote_apply_context(&target))
        .await
        .unwrap();
    transfer(&source, &target).await.unwrap();
    assert_durable_equal(&source.store, &target.store);
    // New positions carrying obsolete content cannot resurrect deleted items.
    let mut obsolete = vec![
        operations()[4].clone(),
        operations()[6].clone(),
        operations()[8].clone(),
    ];
    for (i, entry) in obsolete.iter_mut().enumerate() {
        entry.position.sequence = 13 + i as u64;
        entry.op.id = nx_sync::OpId::new(format!("late-{i}"));
    }
    apply_sequenced_ops(&obsolete, &remote_apply_context(&target))
        .await
        .unwrap();
    assert!(target.get_lww_map_entries("map").await.is_empty());
    assert!(target.get_orset_elements("set").await.is_empty());
    assert_eq!(target.get_rga_values("rga").await, vec![vec![2]]);
}

#[tokio::test]
async fn remote_apply_forwards_only_new_persisted_operations_for_all_families() {
    let target = manager("forward-target", temp_store());
    let entries = operations();

    let forwarded = apply_sequenced_ops_and_collect(&entries, &remote_apply_context(&target))
        .await
        .unwrap();
    assert_eq!(forwarded, entries);
    assert_eq!(target.get_counter_value("counter").await, 10);
    assert_eq!(target.get_pncounter_value("pn").await, 5);
    assert_eq!(
        target.get_lww_register_value("register").await,
        Some(b"winner".to_vec())
    );
    assert!(target.get_lww_map_entries("map").await.is_empty());
    assert!(target.get_orset_elements("set").await.is_empty());
    assert_eq!(target.get_rga_values("rga").await, vec![vec![2]]);

    let duplicate = apply_sequenced_ops_and_collect(&entries, &remote_apply_context(&target))
        .await
        .unwrap();
    assert!(duplicate.is_empty());
}

#[tokio::test]
async fn remote_apply_storage_failure_returns_no_forwardable_operations() {
    let target = manager("forward-failed", temp_store());
    target.store.inject_disk_full_on_writes(true);

    assert!(
        apply_sequenced_ops_and_collect(&operations(), &remote_apply_context(&target))
            .await
            .is_err()
    );
    assert_eq!(target.get_counter_value("counter").await, 0);
    assert!(target.op_log.read().await.is_empty());
}

#[tokio::test]
async fn cancelled_remote_apply_finishes_persistence_and_publication() {
    cancelled_storage_work_finishes_publication(false).await;
}

#[tokio::test]
async fn cancelled_baseline_import_finishes_persistence_and_publication() {
    cancelled_storage_work_finishes_publication(true).await;
}

async fn cancelled_storage_work_finishes_publication(import_baseline: bool) {
    use std::future::Future;

    let source = manager("source", temp_store());
    seed(&source).await;
    let payload = baseline::export(&remote_apply_context(&source))
        .await
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let target = Arc::new(manager(
        "target",
        Arc::new(NxStore::open(directory.path()).unwrap()),
    ));
    let (held_tx, held_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let held_store = Arc::clone(&target.store);
    let holder = std::thread::spawn(move || {
        let lease = held_store.acquire_write_lease().unwrap();
        held_tx.send(()).unwrap();
        let released = release_rx.recv_timeout(Duration::from_secs(5));
        drop(lease);
        released.unwrap();
    });
    held_rx.await.unwrap();

    let worker_target = Arc::clone(&target);
    let (polled_tx, polled_rx) = tokio::sync::oneshot::channel();
    let processing = tokio::spawn(async move {
        let work = async {
            let context = remote_apply_context(&worker_target);
            if import_baseline {
                baseline::import(
                    baseline::VERSION,
                    &payload,
                    blake3::hash(&payload).as_bytes(),
                    &NodeId::new("writer"),
                    &context,
                )
                .await
            } else {
                apply_sequenced_ops(&operations(), &context).await
            }
        };
        let mut work = std::pin::pin!(work);
        let mut polled_tx = Some(polled_tx);
        std::future::poll_fn(|cx| {
            let result = work.as_mut().poll(cx);
            if let Some(sender) = polled_tx.take() {
                sender.send(()).unwrap();
            }
            result
        })
        .await
    });
    polled_rx.await.unwrap();
    processing.abort();
    assert!(processing.await.unwrap_err().is_cancelled());
    assert!(
        target.seen_ops.try_write().is_err(),
        "the storage worker must retain its publication guards after caller cancellation"
    );
    release_tx.send(()).unwrap();
    drop(
        tokio::time::timeout(Duration::from_secs(5), target.seen_ops.write())
            .await
            .unwrap(),
    );
    holder.join().unwrap();

    assert_durable_equal(&source.store, &target.store);
    assert_eq!(target.get_counter_value("counter").await, 10);
    assert_eq!(target.get_pncounter_value("pn").await, 5);
    assert_eq!(
        target.get_lww_register_value("register").await,
        Some(b"winner".to_vec())
    );
    assert!(target.get_lww_map_entries("map").await.is_empty());
    assert!(target.get_orset_elements("set").await.is_empty());
    assert_eq!(target.get_rga_values("rga").await, vec![vec![2]]);
    let progress = target.handle().replication_progress().unwrap();
    assert_eq!(progress[0].contiguous_sequence, 12);
    assert_eq!(progress[0].sparse_positions, 0);
    assert!(!progress[0].baseline_unconfirmed);
    if !import_baseline {
        assert_eq!(target.op_log.read().await.len(), 1);
        assert_eq!(target.seen_ops.read().await.ids.len(), 1);
    }
    seed(&target).await;
    assert_durable_equal(&source.store, &target.store);
    drop(target);
    wait_for_store_file_release(directory.path()).await.unwrap();
    let reopened = manager("target", Arc::new(NxStore::open(directory.path()).unwrap()));
    assert_durable_equal(&source.store, &reopened.store);
    assert_eq!(reopened.handle().replication_progress().unwrap(), progress);
    assert_eq!(reopened.get_counter_value("counter").await, 10);
    assert_eq!(reopened.get_rga_values("rga").await, vec![vec![2]]);
}

#[tokio::test]
async fn baseline_rejects_incomparable_cuts_and_preserves_local_writes() {
    let source = manager("source", temp_store());
    let target = manager("target", temp_store());
    seed(&source).await;
    target
        .handle()
        .increment_gcounter("local", 42)
        .await
        .unwrap();
    let before = target.store.scan_prefix(b"__nx/crdt/").unwrap();
    let error = transfer(&source, &target).await.unwrap_err();
    assert_eq!(
        error.downcast_ref::<baseline::Rejected>().unwrap().0,
        BaselineFailure::CoverageConflict
    );
    assert_eq!(before, target.store.scan_prefix(b"__nx/crdt/").unwrap());
    assert_eq!(target.get_counter_value("local").await, 42);
    // Once the source has the local operation, its cut safely dominates the recipient.
    let local = target.op_log.read().await[0].clone();
    let entry = sequenced_op(&target.store, &local).unwrap().unwrap();
    apply_sequenced_ops(&[entry], &remote_apply_context(&source))
        .await
        .unwrap();
    transfer(&source, &target).await.unwrap();
    assert_eq!(target.get_counter_value("local").await, 42);
    assert_eq!(target.get_counter_value("counter").await, 10);
    target
        .handle()
        .increment_gcounter("local", 1)
        .await
        .unwrap();
    assert_eq!(target.get_counter_value("local").await, 43);
}

// Arm a live request fixture, then use the production correlation and event handler.
async fn deliver_requested_history(
    manager: &SyncManager,
    source: &str,
    start: u64,
    end: u64,
    message: HistoryMessage,
) {
    let (request_id, origin) = match &message {
        HistoryMessage::Baseline {
            request_id, origin, ..
        }
        | HistoryMessage::Replay {
            request_id, origin, ..
        } => (request_id.clone(), origin.clone()),
        _ => panic!("expected a baseline or replay response"),
    };
    let mut context = test_event_context(
        manager.counters.clone(),
        manager.seen_ops.clone(),
        manager.op_log.clone(),
        manager.store.clone(),
        manager.metrics.clone(),
        manager.peer_health.clone(),
    );
    context.pncounters = manager.pncounters.clone();
    context.lww_registers = manager.lww_registers.clone();
    context.lww_maps = manager.lww_maps.clone();
    context.orsets = manager.orsets.clone();
    context.rgas = manager.rgas.clone();
    context.seen_ops_next_sequence = manager.seen_ops_next_sequence.clone();
    context.op_log_next_sequence = manager.op_log_next_sequence.clone();
    context.op_log_limit = manager.config.op_log_limit;
    context
        .history_requests
        .write()
        .await
        .entry(source.into())
        .or_default()
        .insert(
            origin.as_str().into(),
            crate::sync_manager::types::PendingHistoryRequest {
                request_id,
                start,
                end,
                baseline_revision: if start == 0 {
                    Some(recovery_revision(&manager.store, &origin).unwrap())
                } else {
                    None
                },
                deadline: StdInstant::now() + Duration::from_secs(30),
            },
        );
    handle_node_event(
        NodeEvent::HistoryReceived {
            from: NodeId::new(source),
            addr: source.into(),
            message,
        },
        &context,
    )
    .await;
    assert!(context.history_requests.read().await[source].is_empty());
}

#[tokio::test]
async fn baseline_coverage_conflict_survives_restart_then_correlated_repair_preserves_local_writes()
{
    for replay in [true, false] {
        let source = manager("source", temp_store());
        seed(&source).await;
        let directory = tempfile::tempdir().unwrap();
        let target = manager("target", Arc::new(NxStore::open(directory.path()).unwrap()));
        let local = target
            .handle()
            .increment_gcounter("counter", 42)
            .await
            .unwrap()
            .into_sequenced();
        record_summary(
            &target.store,
            &nx_sync::HistorySummary {
                origin: NodeId::new("writer"),
                high_sequence: 12,
                baseline_required: false,
            },
        )
        .unwrap();
        let payload = baseline::export(&remote_apply_context(&source))
            .await
            .unwrap();
        let before_state = read_durable_gcounter_state(&target.store, "counter");
        deliver_requested_history(
            &target,
            "source:1234",
            0,
            0,
            HistoryMessage::Baseline {
                request_id: nx_sync::OpId::new("conflicting-baseline"),
                origin: NodeId::new("writer"),
                version: baseline::VERSION,
                digest: *blake3::hash(&payload).as_bytes(),
                payload,
            },
        )
        .await;
        assert_eq!(target.get_counter_value("counter").await, 42);
        assert_eq!(read_materialized(&target.store, "counter"), 42);
        assert_eq!(
            serde_json::to_value(read_durable_gcounter_state(&target.store, "counter")).unwrap(),
            serde_json::to_value(&before_state).unwrap()
        );
        let before_restart = target.handle().replication_progress().unwrap();
        assert_eq!(before_restart.len(), 2);
        let local_progress = before_restart
            .iter()
            .find(|p| p.origin.as_str() == "target")
            .unwrap();
        assert_eq!(local_progress.contiguous_sequence, 1);
        assert_eq!(local_progress.known_high_sequence, 1);
        assert_eq!(local_progress.sparse_positions, 0);
        let writer = before_restart
            .iter()
            .find(|p| p.origin.as_str() == "writer")
            .unwrap();
        assert_eq!(writer.contiguous_sequence, 0);
        assert_eq!(writer.known_high_sequence, 12);
        assert_eq!(writer.sparse_positions, 0);
        assert_eq!(
            writer.recovery_failure.as_ref().unwrap().reason,
            BaselineFailure::CoverageConflict
        );
        assert_eq!(
            missing_range(&target.store, &NodeId::new("writer")).unwrap(),
            Some((1, 12))
        );
        target.store.flush().unwrap();
        drop(target);
        wait_for_store_file_release(directory.path()).await.unwrap();
        let target = manager("target", Arc::new(NxStore::open(directory.path()).unwrap()));
        assert_eq!(
            target.handle().replication_progress().unwrap(),
            before_restart
        );
        assert_eq!(target.get_counter_value("counter").await, 42);
        assert_eq!(
            read_durable_gcounter_state(&target.store, "counter").value_for(&NodeId::new("target")),
            42
        );

        let repair = SyncManager::try_new(
            NodeId::new("repair"),
            SyncConfig::new().with_op_log_limit(32),
            temp_store(),
            metrics(),
        )
        .unwrap();
        seed(&repair).await;
        let message = if replay {
            let mut retained = repair
                .op_log
                .read()
                .await
                .iter()
                .map(|op| sequenced_op(&repair.store, op).unwrap().unwrap())
                .collect::<Vec<_>>();
            retained.sort_by_key(|entry| entry.position.sequence);
            assert_eq!(retained.len(), 12);
            HistoryMessage::Replay {
                request_id: nx_sync::OpId::new("available-replay"),
                origin: NodeId::new("writer"),
                start: 1,
                end: 12,
                ops: retained,
            }
        } else {
            apply_sequenced_ops(std::slice::from_ref(&local), &remote_apply_context(&repair))
                .await
                .unwrap();
            let payload = baseline::export(&remote_apply_context(&repair))
                .await
                .unwrap();
            HistoryMessage::Baseline {
                request_id: nx_sync::OpId::new("dominating-alternate-source"),
                origin: NodeId::new("writer"),
                version: baseline::VERSION,
                digest: *blake3::hash(&payload).as_bytes(),
                payload,
            }
        };
        deliver_requested_history(
            &target,
            "repair:1234",
            if replay { 1 } else { 0 },
            if replay { 12 } else { 0 },
            message,
        )
        .await;
        if replay {
            apply_sequenced_ops(&[local], &remote_apply_context(&repair))
                .await
                .unwrap();
        }
        assert_eq!(target.get_counter_value("counter").await, 52);
        assert_eq!(read_materialized(&target.store, "counter"), 52);
        assert_durable_equal(&repair.store, &target.store);
        let progress = target.handle().replication_progress().unwrap();
        assert_eq!(progress.len(), 2);
        for progress in progress {
            assert_eq!(
                progress.contiguous_sequence,
                if progress.origin.as_str() == "writer" {
                    12
                } else {
                    1
                }
            );
            assert_eq!(progress.known_high_sequence, progress.contiguous_sequence);
            assert_eq!(progress.sparse_positions, 0);
            assert!(progress.recovery_failure.is_none());
            assert!(!progress.baseline_unconfirmed);
        }
        assert_eq!(
            missing_range(&target.store, &NodeId::new("writer")).unwrap(),
            None
        );
        apply_sequenced_ops(&operations(), &remote_apply_context(&target))
            .await
            .unwrap();
        assert_durable_equal(&repair.store, &target.store);
    }
}

#[tokio::test]
async fn counter_totals_over_u64_materialize_locally_remotely_and_restart() {
    let directory = tempfile::tempdir().unwrap();
    let source = manager("local", Arc::new(NxStore::open(directory.path()).unwrap()));
    let entries = numeric_operations();
    apply_sequenced_ops(&entries[..3], &remote_apply_context(&source))
        .await
        .unwrap();
    let handle = source.handle();
    let local = vec![
        handle
            .increment_gcounter("counter", 1)
            .await
            .unwrap()
            .into_sequenced(),
        handle
            .increment_pncounter("pn", 9)
            .await
            .unwrap()
            .into_sequenced(),
        handle
            .decrement_pncounter("pn", 4)
            .await
            .unwrap()
            .into_sequenced(),
    ];
    assert_numeric_values(&source, u64::MAX, 5).await;
    let remote = manager("remote", temp_store());
    apply_sequenced_ops(&local, &remote_apply_context(&remote))
        .await
        .unwrap();
    apply_sequenced_ops(&entries[..3], &remote_apply_context(&remote))
        .await
        .unwrap();
    assert_numeric_values(&remote, u64::MAX, 5).await;
    assert_durable_equal(&source.store, &remote.store);
    let before = source.handle().replication_progress().unwrap();
    source.store.flush().unwrap();
    drop(handle);
    drop(source);
    wait_for_store_file_release(directory.path()).await.unwrap();
    let source = manager("local", Arc::new(NxStore::open(directory.path()).unwrap()));
    assert_numeric_values(&source, u64::MAX, 5).await;
    assert_durable_equal(&source.store, &remote.store);
    assert_eq!(source.handle().replication_progress().unwrap(), before);
    let gcounter = read_durable_gcounter_state(&source.store, "counter");
    let pncounter = read_durable_pncounter_state(&source.store, "pn");
    assert_eq!(gcounter.value_for(&NodeId::new("writer")), u64::MAX);
    assert_eq!(gcounter.value_for(&NodeId::new("local")), 1);
    assert_eq!(pncounter.positive_for(&NodeId::new("writer")), u64::MAX);
    assert_eq!(pncounter.negative_for(&NodeId::new("writer")), u64::MAX);
    assert_eq!(pncounter.positive_for(&NodeId::new("local")), 9);
    assert_eq!(pncounter.negative_for(&NodeId::new("local")), 4);
    apply_sequenced_ops(&local, &remote_apply_context(&source))
        .await
        .unwrap();
    apply_sequenced_ops(&entries[..3], &remote_apply_context(&source))
        .await
        .unwrap();
    assert_numeric_values(&source, u64::MAX, 5).await;
    assert_durable_equal(&source.store, &remote.store);
}

#[tokio::test]
async fn baseline_accepts_counter_totals_over_u64_and_preserves_state_and_coverage_on_restart() {
    let source = manager("source", temp_store());
    let entries = numeric_operations();
    apply_sequenced_ops(&entries, &remote_apply_context(&source))
        .await
        .unwrap();
    assert_numeric_values(&source, u64::MAX, 5).await;
    let directory = tempfile::tempdir().unwrap();
    let target = manager("target", Arc::new(NxStore::open(directory.path()).unwrap()));
    apply_sequenced_ops(
        &[entries.last().unwrap().clone()],
        &remote_apply_context(&target),
    )
    .await
    .unwrap();
    assert_eq!(
        target.handle().replication_progress().unwrap()[0].contiguous_sequence,
        0
    );
    transfer(&source, &target).await.unwrap();
    assert_numeric_values(&target, u64::MAX, 5).await;
    assert_durable_equal(&source.store, &target.store);
    let before = target.handle().replication_progress().unwrap();
    assert_eq!(before.len(), 2);
    for progress in &before {
        assert_eq!(progress.contiguous_sequence, 3);
        assert_eq!(progress.known_high_sequence, 3);
        assert_eq!(progress.sparse_positions, 0);
        assert!(!progress.baseline_unconfirmed);
        assert!(progress.recovery_failure.is_none());
    }
    drop(target);
    wait_for_store_file_release(directory.path()).await.unwrap();
    let target = manager("target", Arc::new(NxStore::open(directory.path()).unwrap()));
    assert_numeric_values(&target, u64::MAX, 5).await;
    assert_durable_equal(&source.store, &target.store);
    assert_eq!(target.handle().replication_progress().unwrap(), before);
    apply_sequenced_ops(&entries, &remote_apply_context(&target))
        .await
        .unwrap();
    transfer(&source, &target).await.unwrap();
    assert_durable_equal(&source.store, &target.store);
    assert_numeric_values(&target, u64::MAX, 5).await;
}

#[tokio::test]
async fn baseline_validation_and_storage_failures_do_not_partially_publish() {
    let source = manager("source", temp_store());
    let directory = tempfile::tempdir().unwrap();
    let target = manager("target", Arc::new(NxStore::open(directory.path()).unwrap()));
    seed(&source).await;
    let payload = baseline::export(&remote_apply_context(&source))
        .await
        .unwrap();
    let digest = *blake3::hash(&payload).as_bytes();
    let before = target.store.scan_prefix(b"__nx/crdt/").unwrap();
    for (version, data, hash) in [
        (0, payload.clone(), digest),
        (baseline::VERSION, payload.clone(), [0; 32]),
        (
            baseline::VERSION,
            payload[..payload.len() - 1].to_vec(),
            digest,
        ),
        (baseline::VERSION, vec![0; baseline::MAX_BYTES + 1], [0; 32]),
    ] {
        assert!(
            baseline::import(
                version,
                &data,
                &hash,
                &NodeId::new("writer"),
                &remote_apply_context(&target)
            )
            .await
            .is_err()
        );
        assert_eq!(before, target.store.scan_prefix(b"__nx/crdt/").unwrap());
    }
    let original: CrdtBaseline = serde_json::from_slice(&payload).unwrap();
    let mut bad_states = Vec::new();
    let mut duplicate = original.clone();
    duplicate.records.push(duplicate.records[0].clone());
    bad_states.push(duplicate);
    let mut unknown = original.clone();
    unknown.records[0].family = 99;
    bad_states.push(unknown);
    let mut incomplete = original.clone();
    incomplete.origins.clear();
    bad_states.push(incomplete);
    let mut corrupt = original.clone();
    corrupt.records[0].state = b"{}".to_vec();
    bad_states.push(corrupt);
    for invalid_slots in [
        serde_json::json!({"": 1}),
        serde_json::json!({"a".repeat(257): 1}),
        serde_json::json!({"a": -1}),
        serde_json::json!({"a": 18446744073709551616.0}),
        serde_json::json!({"a": 1.5}),
    ] {
        for (family, side) in [(1, None), (2, Some("positive")), (2, Some("negative"))] {
            let mut invalid = original.clone();
            let record = invalid
                .records
                .iter_mut()
                .find(|record| record.family == family)
                .unwrap();
            let mut state: serde_json::Value = serde_json::from_slice(&record.state).unwrap();
            if let Some(side) = side {
                state[side]["counts"] = invalid_slots.clone();
            } else {
                state["counts"] = invalid_slots.clone();
            }
            record.state = serde_json::to_vec(&state).unwrap();
            bad_states.push(invalid);
        }
    }
    let mut cyclic = original.clone();
    cyclic.records.last_mut().unwrap().state =
        br#"{"elements":{"root":{"id":"root","parent":"root","value":[]}},"tombstones":[]}"#
            .to_vec();
    bad_states.push(cyclic);
    for invalid in bad_states {
        let data = serde_json::to_vec(&invalid).unwrap();
        assert!(
            baseline::import(
                baseline::VERSION,
                &data,
                blake3::hash(&data).as_bytes(),
                &NodeId::new("writer"),
                &remote_apply_context(&target)
            )
            .await
            .is_err()
        );
        assert_eq!(before, target.store.scan_prefix(b"__nx/crdt/").unwrap());
    }
    target.store.inject_disk_full_on_writes(true);
    assert!(transfer(&source, &target).await.is_err());
    target.store.inject_disk_full_on_writes(false);
    assert_eq!(before, target.store.scan_prefix(b"__nx/crdt/").unwrap());
    assert_eq!(target.get_counter_value("counter").await, 0);
    target.store.inject_disk_full_on_flush(true);
    assert!(transfer(&source, &target).await.is_err());
    assert_eq!(target.get_counter_value("counter").await, 10);
    assert!(target.handle().replication_progress().unwrap()[0].baseline_unconfirmed);
    assert!(
        baseline::export(&remote_apply_context(&target))
            .await
            .is_err()
    );
    target.store.inject_disk_full_on_flush(false);
    assert!(
        transfer(&source, &target).await.is_err(),
        "a failed flush fences writes until reopen"
    );
    drop(target);
    let target = manager("target", Arc::new(NxStore::open(directory.path()).unwrap()));
    assert!(!target.handle().replication_progress().unwrap()[0].baseline_unconfirmed);
    assert_durable_equal(&source.store, &target.store);
}

#[tokio::test]
async fn baseline_incomplete_and_oversized_sources_fail_explicitly() {
    let source = manager("source", temp_store());
    apply_sequenced_ops(&[operations()[1].clone()], &remote_apply_context(&source))
        .await
        .unwrap();
    let error = baseline::export(&remote_apply_context(&source))
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<baseline::Rejected>().unwrap().0,
        BaselineFailure::SourceIncomplete
    );
    let source = manager("large", temp_store());
    source
        .handle()
        .set_lww_register("large", vec![1; baseline::MAX_BYTES], 1)
        .await
        .unwrap();
    let error = baseline::export(&remote_apply_context(&source))
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<baseline::Rejected>().unwrap().0,
        BaselineFailure::BudgetExceeded
    );
}

#[tokio::test]
async fn baseline_survives_restart_and_replay_continues_after_the_cut() {
    let source = manager("source", temp_store());
    seed(&source).await;
    let directory = tempfile::tempdir().unwrap();
    {
        let target = manager("target", Arc::new(NxStore::open(directory.path()).unwrap()));
        transfer(&source, &target).await.unwrap();
    }
    let target = manager("target", Arc::new(NxStore::open(directory.path()).unwrap()));
    apply_sequenced_ops(&operations(), &remote_apply_context(&target))
        .await
        .unwrap();
    assert_durable_equal(&source.store, &target.store);
    let mut next = operations()[0].clone();
    next.position.sequence = 13;
    next.op.id = nx_sync::OpId::new("new-after-baseline");
    apply_sequenced_ops(&[next], &remote_apply_context(&target))
        .await
        .unwrap();
    assert_eq!(target.get_counter_value("counter").await, 17);
    assert_eq!(
        target.handle().replication_progress().unwrap()[0].contiguous_sequence,
        13
    );
}

#[tokio::test]
async fn baseline_new_node_recovers_immutable_v015_fixture_and_legacy_provenance() {
    let store = temp_store();
    for (key, value) in
        crate::sync_manager::fixtures::parse_fixture(include_str!("../fixtures/v0_1_5/records.hex"))
    {
        store.set(&key, &value).unwrap();
    }
    crate::sync_manager::migrate_sync_schema(
        &store,
        crate::sync_manager::MigrationOptions::default(),
    )
    .unwrap();
    let source = manager("legacy", store);
    let target = manager("target", temp_store());
    let payload = baseline::export(&remote_apply_context(&source))
        .await
        .unwrap();
    baseline::import(
        baseline::VERSION,
        &payload,
        blake3::hash(&payload).as_bytes(),
        &NodeId::new("legacy"),
        &remote_apply_context(&target),
    )
    .await
    .unwrap();
    assert_durable_equal(&source.store, &target.store);
    let progress = target.handle().replication_progress().unwrap();
    assert!(progress[0].baseline_required);
    assert!(!progress[0].baseline_missing);
    assert!(
        baseline::export(&remote_apply_context(&target))
            .await
            .is_ok()
    );
    // Another legacy store has independent history and cannot be overwritten by this cut.
    let separate = manager("other-legacy", {
        let store = temp_store();
        store
            .set(&materialized_gcounter_key("old"), &1u64.to_le_bytes())
            .unwrap();
        crate::sync_manager::migrate_sync_schema(
            &store,
            crate::sync_manager::MigrationOptions::default(),
        )
        .unwrap();
        store
    });
    assert!(
        baseline::import(
            baseline::VERSION,
            &payload,
            blake3::hash(&payload).as_bytes(),
            &NodeId::new("legacy"),
            &remote_apply_context(&separate)
        )
        .await
        .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn baseline_late_join_recovers_over_real_tcp_json_and_bincode() {
    for format in [
        nx_net::SerializationFormat::Json,
        nx_net::SerializationFormat::Bincode,
    ] {
        let mut source = manager("source", temp_store());
        seed(&source).await;
        let addr = free_addr();
        source.config.listen_addr = Some(addr.clone());
        source.config.serialization_format = format;
        source.config.anti_entropy_interval = Duration::from_millis(25);
        source.start().await.unwrap();
        let mut target = manager("target", temp_store());
        target.config.listen_addr = Some("127.0.0.1:0".into());
        target.config.serialization_format = format;
        target.config.anti_entropy_interval = Duration::from_millis(25);
        target.start().await.unwrap();
        target.connect_to_peer(&addr).await.unwrap();
        let replication_result = tokio::time::timeout(Duration::from_secs(10), async {
            let mut poll = tokio::time::interval(Duration::from_millis(10));
            loop {
                poll.tick().await;
                let progress = target.handle().replication_progress().unwrap();
                if progress
                    .iter()
                    .any(|p| p.origin.as_str() == "writer" && p.contiguous_sequence == 12)
                {
                    break;
                }
            }
        })
        .await;
        assert!(
            replication_result.is_ok(),
            "timed out waiting for late-join recovery: progress={:?}, connected_peers={}",
            target.handle().replication_progress().unwrap(),
            target.connected_peer_count().await
        );
        assert_durable_equal(&source.store, &target.store);
        assert_eq!(target.get_counter_value("counter").await, 10);
        source.shutdown().await.unwrap();
        target.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn baseline_crash_child() {
    let Ok(path) = std::env::var("NUMAX_D3_CRASH_STORE") else {
        return;
    };
    let source = manager("source", temp_store());
    seed(&source).await;
    let target = manager("target", Arc::new(NxStore::open(path).unwrap()));
    let point = match std::env::var("NUMAX_D3_CRASH_POINT").unwrap().as_str() {
        "before" => CrashCheckpoint::BeforeBatch,
        "after" => CrashCheckpoint::AfterBatch,
        _ => panic!("invalid baseline crash checkpoint"),
    };
    target.store.inject_crash_checkpoint(
        point,
        std::env::var("NUMAX_D3_CRASH_MARKER").unwrap().into(),
    );
    transfer(&source, &target).await.unwrap();
    panic!("baseline passed armed crash checkpoint");
}

#[test]
fn baseline_process_crash_preserves_atomic_state_and_coverage() {
    for point in ["before", "after"] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store");
        let marker = directory.path().join("marker");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "sync_manager::manager::tests::recovery::baseline_crash_child",
            ])
            .env("NUMAX_D3_CRASH_STORE", &path)
            .env("NUMAX_D3_CRASH_POINT", point)
            .env("NUMAX_D3_CRASH_MARKER", &marker)
            .spawn()
            .unwrap();
        let deadline = StdInstant::now() + std::time::Duration::from_secs(10);
        while !marker.exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "child exited before baseline checkpoint"
            );
            if StdInstant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("baseline checkpoint not reached");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let store = Arc::new(NxStore::open(&path).unwrap());
        let survived = store.exists(&materialized_gcounter_key("counter")).unwrap();
        if point == "before" {
            assert!(!survived);
        }
        for prefix in [
            GCOUNTER_STATE_STORE_PREFIX,
            PNCOUNTER_STATE_STORE_PREFIX,
            LWW_REGISTER_STATE_STORE_PREFIX,
            LWW_MAP_STATE_STORE_PREFIX,
            ORSET_STATE_STORE_PREFIX,
            RGA_STATE_STORE_PREFIX,
        ] {
            assert_eq!(store.prefix_exists(prefix.as_bytes()).unwrap(), survived);
        }
        let progress = replication_progress(&store).unwrap();
        assert_eq!(!progress.is_empty(), survived);
        if survived {
            assert_eq!(progress[0].contiguous_sequence, 12);
            assert!(progress[0].baseline_unconfirmed);
        }
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let target = manager("target", store);
            let source = manager("source", temp_store());
            seed(&source).await;
            transfer(&source, &target).await.unwrap();
            assert_durable_equal(&source.store, &target.store);
            assert!(!target.handle().replication_progress().unwrap()[0].baseline_unconfirmed);
        });
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn baseline_unrecoverable_source_is_observable_and_another_source_can_repair() {
    let mut incomplete = manager("incomplete", temp_store());
    apply_sequenced_ops(
        &[operations().pop().unwrap()],
        &remote_apply_context(&incomplete),
    )
    .await
    .unwrap();
    let incomplete_addr = free_addr();
    incomplete.config.listen_addr = Some(incomplete_addr.clone());
    incomplete.config.anti_entropy_interval = Duration::from_millis(25);
    incomplete.start().await.unwrap();
    let mut target = manager("target", temp_store());
    target.config.listen_addr = Some("127.0.0.1:0".into());
    target.config.anti_entropy_interval = Duration::from_millis(25);
    target.start().await.unwrap();
    target.connect_to_peer(&incomplete_addr).await.unwrap();
    let failure_result = tokio::time::timeout(Duration::from_secs(10), async {
        let mut poll = tokio::time::interval(Duration::from_millis(10));
        loop {
            poll.tick().await;
            let progress = target.handle().replication_progress().unwrap();
            if let Some(p) = progress.first()
                && let Some(failure) = &p.recovery_failure
            {
                assert_eq!(p.contiguous_sequence, 0);
                assert_eq!(failure.reason, BaselineFailure::SourceIncomplete);
                break;
            }
        }
    })
    .await;
    assert!(
        failure_result.is_ok(),
        "timed out waiting for source-scoped failure: progress={:?}, connected_peers={}",
        target.handle().replication_progress().unwrap(),
        target.connected_peer_count().await
    );
    assert_eq!(target.get_counter_value("counter").await, 0);
    assert_eq!(target.connected_peer_count().await, 1);
    let mut complete = manager("complete", temp_store());
    seed(&complete).await;
    let complete_addr = free_addr();
    complete.config.listen_addr = Some(complete_addr.clone());
    complete.config.anti_entropy_interval = Duration::from_millis(25);
    complete.start().await.unwrap();
    target.connect_to_peer(&complete_addr).await.unwrap();
    let repair_result = tokio::time::timeout(Duration::from_secs(10), async {
        let mut poll = tokio::time::interval(Duration::from_millis(10));
        loop {
            poll.tick().await;
            if target
                .handle()
                .replication_progress()
                .unwrap()
                .iter()
                .any(|p| {
                    p.origin.as_str() == "writer"
                        && p.contiguous_sequence == 12
                        && p.recovery_failure.is_none()
                })
            {
                break;
            }
        }
    })
    .await;
    let peers = target.node.as_ref().unwrap().connected_peers().await;
    let summary_cursors = target
        .history_cursors
        .read()
        .await
        .iter()
        .map(|(addr, cursor)| {
            (
                addr.clone(),
                cursor.after_origin.clone(),
                cursor.request_deadline.is_some(),
            )
        })
        .collect::<Vec<_>>();
    assert!(
        repair_result.is_ok(),
        "timed out waiting for alternate-source repair: progress={:?}, peers={peers:?}, summary_cursors={summary_cursors:?}, expected_source={complete_addr}",
        target.handle().replication_progress().unwrap(),
    );
    assert_durable_equal(&complete.store, &target.store);
    incomplete.shutdown().await.unwrap();
    complete.shutdown().await.unwrap();
    target.shutdown().await.unwrap();
}
