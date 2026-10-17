use super::*;
use nx_sync::{HistorySummary, OpPosition, SequencedOp};

#[tokio::test]
async fn sequenced_unavailable_evidence_survives_reopen_and_clears_only_its_interval() {
    use crate::sync_manager::UnavailableHistory;
    let directory = tempfile::tempdir().unwrap();
    let evidence = UnavailableHistory {
        source: "127.0.0.1:1234".into(),
        start: 1,
        end: 1,
        request_id: nx_sync::OpId::generate(),
    };
    let origin = NodeId::new("origin");
    {
        let store = NxStore::open(directory.path()).unwrap();
        SyncManager::try_new(
            NodeId::new("target"),
            SyncConfig::new(),
            Arc::new(store.clone()),
            metrics(),
        )
        .unwrap();
        record_summary(
            &store,
            &HistorySummary {
                origin: origin.clone(),
                high_sequence: 2,
                baseline_required: false,
            },
        )
        .unwrap();
        record_unavailable(&store, &origin, evidence.clone()).unwrap();
        store.flush().unwrap();
    }
    let store = Arc::new(NxStore::open(directory.path()).unwrap());
    let target = SyncManager::try_new(
        NodeId::new("target"),
        SyncConfig::new(),
        Arc::clone(&store),
        metrics(),
    )
    .unwrap();
    assert_eq!(
        target.handle().replication_progress().unwrap()[0].unavailable_evidence,
        Some(evidence)
    );
    apply(&target, &[entry("origin", 1, 1)]).await.unwrap();
    let progress = target.handle().replication_progress().unwrap();
    assert_eq!(progress[0].contiguous_sequence, 1);
    assert_eq!(progress[0].known_high_sequence, 2);
    assert!(!progress[0].source_unavailable);
    assert!(progress[0].unavailable_evidence.is_none());
}

#[tokio::test]
async fn sequenced_remote_crash_child() {
    let Ok(path) = std::env::var("NUMAX_D2_CRASH_STORE") else {
        return;
    };
    let store = Arc::new(NxStore::open(path).unwrap());
    let target = SyncManager::try_new(
        NodeId::new("target"),
        SyncConfig::new(),
        Arc::clone(&store),
        metrics(),
    )
    .unwrap();
    let checkpoint = match std::env::var("NUMAX_D2_CRASH_POINT").unwrap().as_str() {
        "before" => CrashCheckpoint::BeforeBatch,
        "after" => CrashCheckpoint::AfterBatch,
        _ => panic!("unknown remote crash checkpoint"),
    };
    store.inject_crash_checkpoint(
        checkpoint,
        std::env::var("NUMAX_D2_CRASH_MARKER").unwrap().into(),
    );
    let mut operation = entry("remote", 1, 7);
    operation.op.id = nx_sync::OpId::new("remote-crash-op");
    apply(&target, &[operation]).await.unwrap();
    panic!("remote apply passed armed checkpoint");
}

#[test]
fn sequenced_remote_crash_keeps_state_position_and_receipt_atomic() {
    for checkpoint in ["before", "after"] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("store");
        let marker = directory.path().join("marker");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "sync_manager::manager::tests::sequencing::sequenced_remote_crash_child",
            ])
            .env("NUMAX_D2_CRASH_STORE", &path)
            .env("NUMAX_D2_CRASH_POINT", checkpoint)
            .env("NUMAX_D2_CRASH_MARKER", &marker)
            .spawn()
            .unwrap();
        let deadline = StdInstant::now() + std::time::Duration::from_secs(10);
        while !marker.exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "child exited before checkpoint"
            );
            if StdInstant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("remote crash checkpoint not reached");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let store = Arc::new(NxStore::open(&path).unwrap());
        let survived = store
            .get(&materialized_gcounter_key("visits"))
            .unwrap()
            .is_some();
        if checkpoint == "before" {
            assert!(!survived);
        }
        assert_eq!(
            store
                .get(&seen_op_store_key("remote-crash-op"))
                .unwrap()
                .is_some(),
            survived
        );
        assert_eq!(
            store
                .get(&op_log_store_key("remote-crash-op"))
                .unwrap()
                .is_some(),
            survived
        );
        assert_eq!(
            store
                .get(&position_key("remote-crash-op"))
                .unwrap()
                .is_some(),
            survived
        );
        assert_eq!(!replication_progress(&store).unwrap().is_empty(), survived);
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let target = SyncManager::try_new(
                NodeId::new("target"),
                SyncConfig::new(),
                Arc::clone(&store),
                metrics(),
            )
            .unwrap();
            let mut operation = entry("remote", 1, 7);
            operation.op.id = nx_sync::OpId::new("remote-crash-op");
            apply(&target, &[operation]).await.unwrap();
            assert_eq!(target.get_counter_value("visits").await, 7);
            assert_eq!(read_materialized(&store, "visits"), 7);
            assert_eq!(
                target.handle().replication_progress().unwrap()[0].contiguous_sequence,
                1
            );
        });
    }
}

async fn apply(manager: &SyncManager, operations: &[SequencedOp]) -> anyhow::Result<()> {
    apply_sequenced_ops(
        operations,
        &RemoteOpApplyContext {
            counters: &manager.counters,
            pncounters: &manager.pncounters,
            lww_registers: &manager.lww_registers,
            lww_maps: &manager.lww_maps,
            orsets: &manager.orsets,
            rgas: &manager.rgas,
            seen_ops: &manager.seen_ops,
            seen_ops_next_sequence: &manager.seen_ops_next_sequence,
            op_log: &manager.op_log,
            op_log_next_sequence: &manager.op_log_next_sequence,
            op_log_limit: manager.config.op_log_limit,
            store: &manager.store,
            metrics: &manager.metrics,
        },
    )
    .await
}

fn entry(origin: &str, seq: u64, increment: u64) -> SequencedOp {
    SequencedOp {
        op: Op::gcounter_increment(NodeId::new(origin), "visits", increment),
        position: OpPosition {
            generation: 1,
            sequence: seq,
        },
    }
}

#[tokio::test]
async fn sequenced_remote_batch_retention_bounds_replay_records_and_positions() {
    let store = temp_store();
    let target = SyncManager::try_new(
        NodeId::new("target"),
        SyncConfig::new()
            .with_seen_ops_limit(1)
            .with_op_log_limit(1),
        Arc::clone(&store),
        metrics(),
    )
    .unwrap();
    for start in [1, 5] {
        let operations = (start..start + 4)
            .map(|sequence| entry("origin", sequence, 1))
            .collect::<Vec<_>>();
        apply(&target, &operations).await.unwrap();
        let last = operations.last().unwrap();
        assert_eq!(
            target.op_log.read().await.as_slice(),
            std::slice::from_ref(&last.op)
        );
        let retained = store.scan_prefix(OP_LOG_STORE_PREFIX.as_bytes()).unwrap();
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].0, op_log_store_key(last.op.id.as_str()));
        assert_eq!(
            store
                .scan_prefix(b"__nx/crdt/sequence/replay/")
                .unwrap()
                .len(),
            1
        );
        for operation in &operations[..operations.len() - 1] {
            assert!(
                store
                    .get(&position_key(operation.op.id.as_str()))
                    .unwrap()
                    .is_none()
            );
        }
        apply(&target, &operations).await.unwrap();
        assert_eq!(target.get_counter_value("visits").await, start + 3);
        assert_eq!(
            target.handle().replication_progress().unwrap()[0].contiguous_sequence,
            start + 3
        );
    }
}

#[tokio::test]
async fn sequenced_sparse_duplicate_survives_restart_before_gap_closes() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(NxStore::open(directory.path()).unwrap());
    let config = SyncConfig::new()
        .with_seen_ops_limit(1)
        .with_op_log_limit(1);
    let target = SyncManager::try_new(
        NodeId::new("target"),
        config.clone(),
        Arc::clone(&store),
        metrics(),
    )
    .unwrap();
    let second = entry("origin", 2, 2);
    apply(&target, std::slice::from_ref(&second)).await.unwrap();
    apply(&target, &[entry("other-origin", 1, 10)])
        .await
        .unwrap();
    assert!(!target.seen_ops.read().await.contains(second.op.id.as_str()));
    store.flush().unwrap();
    drop(target);
    drop(store);
    let store = Arc::new(NxStore::open(directory.path()).unwrap());
    let target =
        SyncManager::try_new(NodeId::new("target"), config, Arc::clone(&store), metrics()).unwrap();
    apply(&target, &[second]).await.unwrap();
    assert_eq!(target.get_counter_value("visits").await, 12);
    apply(&target, &[entry("origin", 1, 1)]).await.unwrap();
    assert_eq!(target.get_counter_value("visits").await, 13);
    let progress = target.handle().replication_progress().unwrap();
    assert_eq!(
        progress
            .iter()
            .find(|p| p.origin.as_str() == "origin")
            .unwrap()
            .contiguous_sequence,
        2
    );
}

#[tokio::test]
async fn sequenced_legacy_baseline_is_visible_and_unsequenced_replay_is_rejected() {
    let store = temp_store();
    let mut counter = GCounter::new();
    counter.increment(&NodeId::new("historical-origin"), 7);
    persist_gcounter_state(&store, "visits", &counter).unwrap();
    let target = SyncManager::try_new(
        NodeId::new("target"),
        SyncConfig::new(),
        Arc::clone(&store),
        metrics(),
    )
    .unwrap();
    let context = test_event_context(
        Arc::clone(&target.counters),
        Arc::clone(&target.seen_ops),
        Arc::clone(&target.op_log),
        Arc::clone(&store),
        metrics(),
        Arc::new(RwLock::new(HashMap::new())),
    );
    handle_node_event(
        NodeEvent::OpsReceived {
            from: NodeId::new("legacy-peer"),
            ops: vec![Op::gcounter_increment(
                NodeId::new("historical-origin"),
                "visits",
                7,
            )],
        },
        &context,
    )
    .await;
    assert_eq!(target.get_counter_value("visits").await, 7);
    assert_eq!(read_materialized(&store, "visits"), 7);
    let progress = target.handle().replication_progress().unwrap();
    assert!(progress[0].baseline_required);
    assert_eq!(progress[0].contiguous_sequence, 0);
    let (summaries, _) = summary_page(&store, None).unwrap();
    assert!(summaries[0].baseline_required);
    let receiver = temp_store();
    record_summary(&receiver, &summaries[0]).unwrap();
    assert!(replication_progress(&receiver).unwrap()[0].baseline_required);
}

#[tokio::test]
async fn sequenced_all_families_ignore_delayed_replay_after_eviction_and_restart() {
    let source_store = temp_store();
    let source = SyncManager::try_new(
        NodeId::new("source"),
        SyncConfig::new(),
        source_store,
        metrics(),
    )
    .unwrap();
    let handle = source.handle();
    let mut operations = vec![
        handle
            .increment_gcounter("visits", 7)
            .await
            .unwrap()
            .into_sequenced(),
        handle
            .increment_pncounter("balance", 9)
            .await
            .unwrap()
            .into_sequenced(),
        handle
            .decrement_pncounter("balance", 4)
            .await
            .unwrap()
            .into_sequenced(),
        handle
            .set_lww_register("status", b"ready".to_vec(), 100)
            .await
            .unwrap()
            .into_sequenced(),
        handle
            .set_lww_map("settings", "mode", b"ready".to_vec(), 100)
            .await
            .unwrap()
            .into_sequenced(),
        handle
            .remove_lww_map("settings", "mode", 101)
            .await
            .unwrap()
            .into_sequenced(),
        handle
            .add_orset("tags", "blue")
            .await
            .unwrap()
            .into_sequenced(),
        handle
            .remove_orset("tags", "blue")
            .await
            .unwrap()
            .unwrap()
            .into_sequenced(),
    ];
    let insert = Op::rga_insert_with_op_id(
        handle.node_id().clone(),
        "comments",
        None::<String>,
        b"hello".to_vec(),
    );
    operations.push(
        handle
            .insert_rga(insert.clone())
            .await
            .unwrap()
            .into_sequenced(),
    );
    let child = Op::rga_insert_with_op_id(
        handle.node_id().clone(),
        "comments",
        Some(insert.id.as_str().to_owned()),
        b"reply".to_vec(),
    );
    operations.push(
        handle
            .insert_rga(child.clone())
            .await
            .unwrap()
            .into_sequenced(),
    );
    operations.push(
        handle
            .delete_rga("comments", child.id.as_str())
            .await
            .unwrap()
            .into_sequenced(),
    );
    operations.push(
        handle
            .delete_rga("comments", insert.id.as_str())
            .await
            .unwrap()
            .into_sequenced(),
    );
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(NxStore::open(directory.path()).unwrap());
    let config = SyncConfig::new()
        .with_seen_ops_limit(1)
        .with_op_log_limit(1);
    let target_id = NodeId::new("target");
    let target = SyncManager::try_new(
        target_id.clone(),
        config.clone(),
        Arc::clone(&store),
        metrics(),
    )
    .unwrap();
    // Separate arrivals preserve the reversed dependency order through production apply.
    for operation in operations.iter().rev() {
        apply(&target, std::slice::from_ref(operation))
            .await
            .unwrap();
    }
    assert!(
        !target
            .seen_ops
            .read()
            .await
            .contains(operations[operations.len() - 1].op.id.as_str())
    );
    apply(&target, &operations).await.unwrap();
    assert_eq!(
        target.handle().replication_progress().unwrap()[0].contiguous_sequence,
        operations.len() as u64
    );
    assert_eq!(read_materialized(&store, "visits"), 7);
    assert_eq!(read_materialized_pncounter(&store, "balance"), 5);
    assert_eq!(read_materialized_lww_register(&store, "status"), b"ready");
    assert!(read_materialized_lww_map(&store, "settings").is_empty());
    assert!(read_materialized_orset(&store, "tags").is_empty());
    assert!(read_materialized_rga(&store, "comments").is_empty());
    assert_eq!(
        read_durable_lww_map_state(&store, "settings"),
        read_durable_lww_map_state(&source.store, "settings")
    );
    assert_eq!(
        read_durable_orset_state(&store, "tags"),
        read_durable_orset_state(&source.store, "tags")
    );
    assert_eq!(
        read_durable_rga_state(&store, "comments"),
        read_durable_rga_state(&source.store, "comments")
    );
    store.flush().unwrap();
    drop(target);
    drop(store);
    let store = Arc::new(NxStore::open(directory.path()).unwrap());
    let target = SyncManager::try_new(target_id, config, Arc::clone(&store), metrics()).unwrap();
    apply(&target, &operations).await.unwrap();
    assert_eq!(target.get_counter_value("visits").await, 7);
    assert_eq!(target.get_pncounter_value("balance").await, 5);
    assert_eq!(target.op_log.read().await.len(), 1);
    assert_eq!(
        target.handle().replication_progress().unwrap()[0].sparse_positions,
        0
    );
}

#[tokio::test]
async fn sequenced_gap_conflict_and_storage_failure_do_not_publish_partial_state() {
    let store = temp_store();
    let target = SyncManager::try_new(
        NodeId::new("target"),
        SyncConfig::new(),
        Arc::clone(&store),
        metrics(),
    )
    .unwrap();
    let second = entry("origin", 2, 2);
    apply(&target, std::slice::from_ref(&second)).await.unwrap();
    let progress = target.handle().replication_progress().unwrap();
    assert_eq!(progress[0].contiguous_sequence, 0);
    assert_eq!(progress[0].known_high_sequence, 2);
    let before = store.scan_prefix(SEQUENCE_PREFIX.as_bytes()).unwrap();
    assert!(apply(&target, &[entry("origin", 2, 200)]).await.is_err());
    assert_eq!(
        store.scan_prefix(SEQUENCE_PREFIX.as_bytes()).unwrap(),
        before
    );
    store.inject_disk_full_on_writes(true);
    let first = entry("origin", 1, 1);
    assert!(apply(&target, std::slice::from_ref(&first)).await.is_err());
    assert_eq!(target.get_counter_value("visits").await, 2);
    assert_eq!(read_materialized(&store, "visits"), 2);
    store.inject_disk_full_on_writes(false);
    apply(&target, &[first]).await.unwrap();
    assert_eq!(target.get_counter_value("visits").await, 3);
    assert_eq!(
        target.handle().replication_progress().unwrap()[0].contiguous_sequence,
        2
    );
}

#[tokio::test]
async fn sequenced_retained_uuid_conflicts_after_seen_eviction_preserve_replay() {
    let store = temp_store();
    let target = SyncManager::try_new(
        NodeId::new("target"),
        SyncConfig::new()
            .with_seen_ops_limit(1)
            .with_op_log_limit(3),
        Arc::clone(&store),
        metrics(),
    )
    .unwrap();
    let original = entry("origin", 1, 1);
    apply(&target, std::slice::from_ref(&original))
        .await
        .unwrap();
    apply(&target, &[entry("other", 1, 1)]).await.unwrap();
    assert!(
        !target
            .seen_ops
            .read()
            .await
            .contains(original.op.id.as_str())
    );
    let before = store.scan_prefix(b"__nx/").unwrap();
    let log = target.op_log.read().await.clone();
    let mut conflicts = Vec::new();
    let mut changed = original.clone();
    changed.position.sequence = 2;
    conflicts.push(changed);
    let mut changed = entry("origin", 2, 10);
    changed.op.id = original.op.id.clone();
    conflicts.push(changed);
    let mut changed = entry("another-origin", 1, 10);
    changed.op.id = original.op.id.clone();
    conflicts.push(changed);
    let mut changed = original.clone();
    changed.position.generation += 1;
    conflicts.push(changed);
    let mut changed = entry("origin", 1, 10);
    changed.op.id = original.op.id.clone();
    conflicts.push(changed);
    for conflict in conflicts {
        assert!(apply(&target, &[conflict]).await.is_err());
        assert_eq!(store.scan_prefix(b"__nx/").unwrap(), before);
        assert_eq!(*target.op_log.read().await, log);
        assert_eq!(target.get_counter_value("visits").await, 2);
        assert_eq!(
            sequenced_op(&store, &original.op).unwrap(),
            Some(original.clone())
        );
    }
    apply(&target, &[original]).await.unwrap();
    assert_eq!(store.scan_prefix(b"__nx/").unwrap(), before);
}

#[tokio::test]
async fn sequenced_allocator_continues_after_restart_and_fences_stale_handles() {
    let store = temp_store();
    let id = NodeId::new("source");
    let old =
        SyncManager::try_new(id.clone(), SyncConfig::new(), Arc::clone(&store), metrics()).unwrap();
    let first = old
        .handle()
        .increment_gcounter("visits", 1)
        .await
        .unwrap()
        .into_sequenced();
    let new = SyncManager::try_new(id, SyncConfig::new(), Arc::clone(&store), metrics()).unwrap();
    assert!(
        old.handle()
            .increment_gcounter("visits", 100)
            .await
            .is_err()
    );
    let second = new
        .handle()
        .increment_gcounter("visits", 1)
        .await
        .unwrap()
        .into_sequenced();
    assert_eq!(second.position.sequence, first.position.sequence + 1);
    assert_eq!(second.position.generation, first.position.generation + 1);
    assert_eq!(read_materialized(&store, "visits"), 2);
    assert!(
        SyncManager::try_new(
            NodeId::new("wrong-owner"),
            SyncConfig::new(),
            store,
            metrics()
        )
        .is_err()
    );
}

#[tokio::test]
async fn sequenced_global_sparse_budget_rejects_without_state_changes() {
    use crate::sync_manager::sequence::{MAX_SPARSE, MAX_TOTAL_SPARSE};

    let store = temp_store();
    let target = SyncManager::try_new(
        NodeId::new("target"),
        SyncConfig::new()
            .with_seen_ops_limit(1)
            .with_op_log_limit(1),
        Arc::clone(&store),
        metrics(),
    )
    .unwrap();
    for index in 0..MAX_TOTAL_SPARSE / MAX_SPARSE {
        let operations = (2..=MAX_SPARSE as u64 + 1)
            .map(|seq| entry(&format!("origin-{index}"), seq, 1))
            .collect::<Vec<_>>();
        apply(&target, &operations).await.unwrap();
    }
    let before = store.scan_prefix(SEQUENCE_PREFIX.as_bytes()).unwrap();
    assert!(apply(&target, &[entry("overflow", 2, 1)]).await.is_err());
    assert_eq!(
        store.scan_prefix(SEQUENCE_PREFIX.as_bytes()).unwrap(),
        before
    );
    assert_eq!(
        target.get_counter_value("visits").await,
        MAX_TOTAL_SPARSE as u64
    );
    apply(&target, &[entry("origin-0", 1, 1)]).await.unwrap();
    apply(&target, &[entry("overflow", 2, 1)]).await.unwrap();
    let progress = target.handle().replication_progress().unwrap();
    assert_eq!(
        progress
            .iter()
            .find(|p| p.origin.as_str() == "origin-0")
            .unwrap()
            .contiguous_sequence,
        MAX_SPARSE as u64 + 1
    );
}

#[tokio::test]
async fn sequenced_atomic_batch_checks_final_sparse_budget_not_origin_order() {
    use crate::sync_manager::sequence::{MAX_SPARSE, MAX_TOTAL_SPARSE};
    let store = temp_store();
    let target = SyncManager::try_new(
        NodeId::new("target"),
        SyncConfig::new()
            .with_seen_ops_limit(1)
            .with_op_log_limit(1),
        Arc::clone(&store),
        metrics(),
    )
    .unwrap();
    apply(&target, &[entry("a", 2, 1)]).await.unwrap();
    let operations = (2..=MAX_SPARSE as u64)
        .map(|seq| entry("z", seq, 1))
        .collect::<Vec<_>>();
    apply(&target, &operations).await.unwrap();
    for index in 0..31 {
        let operations = (2..=MAX_SPARSE as u64 + 1)
            .map(|seq| entry(&format!("m-{index}"), seq, 1))
            .collect::<Vec<_>>();
        apply(&target, &operations).await.unwrap();
    }
    assert_eq!(
        target
            .handle()
            .replication_progress()
            .unwrap()
            .iter()
            .map(|p| p.sparse_positions)
            .sum::<usize>(),
        MAX_TOTAL_SPARSE
    );
    apply(&target, &[entry("a", 3, 1), entry("z", 1, 1)])
        .await
        .unwrap();
    let progress = target.handle().replication_progress().unwrap();
    assert_eq!(
        progress.iter().map(|p| p.sparse_positions).sum::<usize>(),
        7938
    );
    assert_eq!(
        progress
            .iter()
            .find(|p| p.origin.as_str() == "z")
            .unwrap()
            .contiguous_sequence,
        256
    );
    assert_eq!(
        read_materialized(&store, "visits"),
        MAX_TOTAL_SPARSE as u64 + 2
    );
}

#[test]
fn sequenced_summaries_paginate_and_global_origin_budget_preserves_frontiers() {
    let store = temp_store();
    for index in 0..crate::sync_manager::sequence::MAX_ORIGINS {
        record_summary(
            &store,
            &HistorySummary {
                baseline_required: false,
                origin: NodeId::new(format!("origin-{index:04}")),
                high_sequence: 2,
            },
        )
        .unwrap();
    }
    let before = replication_progress(&store).unwrap();
    assert!(
        record_summary(
            &store,
            &HistorySummary {
                baseline_required: false,
                origin: NodeId::new("overflow"),
                high_sequence: 10
            }
        )
        .is_err()
    );
    assert_eq!(replication_progress(&store).unwrap(), before);
    let mut cursor = None;
    let mut visited = Vec::new();
    loop {
        let (entries, next) = summary_page(&store, cursor.as_deref()).unwrap();
        if entries.is_empty() {
            break;
        }
        assert!(entries.len() <= crate::sync_manager::sequence::SUMMARY_PAGE);
        visited.extend(
            entries
                .into_iter()
                .map(|entry| entry.origin.as_str().to_owned()),
        );
        cursor = next;
    }
    assert_eq!(visited.len(), before.len());
    assert!(visited.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(
        before
            .iter()
            .all(|progress| progress.contiguous_sequence == 0)
    );
}

#[tokio::test]
async fn sequenced_last_lost_operation_recovers_without_new_writes_in_both_formats() {
    for format in [
        crate::SerializationFormat::Json,
        crate::SerializationFormat::Bincode,
    ] {
        let addr = free_addr();
        let (mut source, handle, _) = started_manager_with_config(
            SyncConfig::new()
                .with_listen_addr(addr.clone())
                .with_serialization_format(format)
                .with_anti_entropy_interval(Duration::from_millis(10)),
        )
        .await;
        // Persist without enqueue: the final push is lost and no newer op can reveal it.
        handle.increment_gcounter("suffix", 3).await.unwrap();
        let (mut target, _, _) = started_manager_with_config(
            SyncConfig::new()
                .with_listen_addr(free_addr())
                .with_peer(addr)
                .with_serialization_format(format)
                .with_anti_entropy_interval(Duration::from_millis(10)),
        )
        .await;
        wait_for_counter(&target, "suffix", 3).await;
        let progress = target.handle().replication_progress().unwrap();
        let source_progress = progress
            .iter()
            .find(|p| &p.origin == source.node_id())
            .unwrap();
        assert_eq!(source_progress.contiguous_sequence, 1);
        source.shutdown().await.unwrap();
        target.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn sequenced_relay_recovers_lost_suffix_with_origin_offline_in_both_formats() {
    for format in [
        crate::SerializationFormat::Json,
        crate::SerializationFormat::Bincode,
    ] {
        let addr = free_addr();
        let store = temp_store();
        let mut relay = SyncManager::try_new(
            NodeId::new("relay"),
            SyncConfig::new()
                .with_listen_addr(addr.clone())
                .with_serialization_format(format),
            Arc::clone(&store),
            metrics(),
        )
        .unwrap();
        let first = entry("offline-origin", 1, 1);
        let mut second = entry("offline-origin", 2, 2);
        second.position.generation = 2;
        apply(&relay, &[first.clone(), second]).await.unwrap();
        relay.start().await.unwrap();

        let mut target = SyncManager::try_new(
            NodeId::new("target"),
            SyncConfig::new()
                .with_listen_addr(free_addr())
                .with_peer(addr)
                .with_serialization_format(format)
                .with_anti_entropy_interval(Duration::from_millis(10)),
            temp_store(),
            metrics(),
        )
        .unwrap();
        apply(&target, &[first]).await.unwrap();
        assert_eq!(
            target.handle().replication_progress().unwrap()[0].known_high_sequence,
            1
        );
        target.start().await.unwrap();
        wait_for_counter(&target, "visits", 3).await;
        let progress = target.handle().replication_progress().unwrap();
        assert_eq!(progress[0].origin.as_str(), "offline-origin");
        assert_eq!(progress[0].contiguous_sequence, 2);
        assert!(!progress[0].source_unavailable);
        relay.shutdown().await.unwrap();
        target.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn sequenced_replay_paginates_and_recovers_maximum_guest_value_in_json() {
    let addr = free_addr();
    let (mut source, handle, _) = started_manager_with_config(
        SyncConfig::new()
            .with_listen_addr(addr.clone())
            .with_serialization_format(crate::SerializationFormat::Json),
    )
    .await;
    for _ in 0..300 {
        handle.increment_gcounter("pages", 1).await.unwrap();
    }
    let value = vec![255u8; 1024 * 1024];
    handle
        .set_lww_register("large", value.clone(), 1)
        .await
        .unwrap();
    let (mut target, _, _) = started_manager_with_config(
        SyncConfig::new()
            .with_listen_addr(free_addr())
            .with_peer(addr.clone())
            .with_serialization_format(crate::SerializationFormat::Json)
            .with_seen_ops_limit(1)
            .with_op_log_limit(1)
            .with_anti_entropy_interval(Duration::from_secs(60)),
    )
    .await;
    wait_for_connected_peer(&target).await;
    target
        .node
        .as_ref()
        .unwrap()
        .send_history_to_addr(
            &addr,
            nx_sync::HistoryMessage::SummaryRequest { after_origin: None },
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if target.get_lww_register_value("large").await == Some(value.clone()) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("paged JSON recovery did not complete");
    assert_eq!(target.get_counter_value("pages").await, 300);
    assert_eq!(
        target.handle().replication_progress().unwrap()[0].contiguous_sequence,
        301
    );
    source.shutdown().await.unwrap();
    target.shutdown().await.unwrap();
}

#[tokio::test]
async fn sequenced_expired_history_reports_source_unavailable_without_false_completion() {
    let addr = free_addr();
    let (mut source, handle, _) = started_manager_with_config(
        SyncConfig::new()
            .with_listen_addr(addr.clone())
            .with_op_log_limit(1)
            .with_anti_entropy_interval(Duration::from_millis(10)),
    )
    .await;
    handle.increment_gcounter("expired", 1).await.unwrap();
    handle.increment_gcounter("expired", 2).await.unwrap();
    let (mut target, _, _) = started_manager_with_config(
        SyncConfig::new()
            .with_listen_addr(free_addr())
            .with_peer(addr)
            .with_anti_entropy_interval(Duration::from_millis(10)),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if target
                .handle()
                .replication_progress()
                .unwrap()
                .iter()
                .any(|p| p.source_unavailable)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let progress = target.handle().replication_progress().unwrap();
    assert_eq!(progress[0].contiguous_sequence, 0);
    assert_eq!(progress[0].known_high_sequence, 2);
    assert_eq!(target.get_counter_value("expired").await, 0);
    source.shutdown().await.unwrap();
    target.shutdown().await.unwrap();
}
