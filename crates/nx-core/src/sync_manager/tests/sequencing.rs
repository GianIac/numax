use super::*;
use nx_sync::{HistorySummary, OpPosition, SequencedOp};

#[test]
fn unchanged_history_summary_does_not_require_a_storage_write() {
    let store = temp_store();
    let summary = HistorySummary {
        baseline_required: false,
        origin: NodeId::new("summary-origin"),
        high_sequence: 7,
    };
    let missing = record_summary(&store, &summary).unwrap();
    assert_eq!(missing, Some((1, 7)));
    let keys = [
        b"__nx/crdt/sequence/origin/summary-origin".as_slice(),
        b"__nx/crdt/sequence/usage".as_slice(),
    ];
    let before = keys.map(|key| store.get(key).unwrap());
    store.inject_disk_full_on_writes(true);
    let repeated = record_summary(&store, &summary);
    store.inject_disk_full_on_writes(false);
    assert_eq!(before, keys.map(|key| store.get(key).unwrap()));
    assert_eq!(
        repeated.unwrap_or_else(|error| panic!("unchanged summary attempted a write: {error:#}")),
        missing,
    );
}

#[test]
fn history_summary_changes_preserve_validation_and_recovery_metadata() {
    let store = temp_store();
    let mut summary = HistorySummary {
        baseline_required: false,
        origin: NodeId::new("summary-origin"),
        high_sequence: 7,
    };
    record_summary(&store, &summary).unwrap();
    let revision = recovery_revision(&store, &summary.origin).unwrap();
    assert!(
        record_recovery_failure(
            &store,
            &summary.origin,
            "source",
            nx_sync::BaselineFailure::CoverageConflict,
            &revision,
        )
        .unwrap()
    );
    let before = replication_progress(&store).unwrap();
    store.inject_disk_full_on_writes(true);
    let unchanged = record_summary(&store, &summary);
    summary.high_sequence = 6;
    let stale = record_summary(&store, &summary);
    summary.high_sequence = 8;
    let changed = record_summary(&store, &summary);
    store.inject_disk_full_on_writes(false);
    assert_eq!(unchanged.unwrap(), Some((1, 7)));
    assert_eq!(stale.unwrap(), Some((1, 7)));
    assert!(changed.is_err());
    assert_eq!(replication_progress(&store).unwrap(), before);
    assert_eq!(record_summary(&store, &summary).unwrap(), Some((1, 8)));
    assert!(
        replication_progress(&store).unwrap()[0]
            .recovery_failure
            .is_none()
    );
    summary.baseline_required = true;
    record_summary(&store, &summary).unwrap();
    assert!(needs_baseline(&store, summary.origin.as_str()).unwrap());
    summary.origin = NodeId::new("");
    assert!(record_summary(&store, &summary).is_err());
}

#[test]
fn history_summary_page_records_all_origins_in_one_atomic_batch() {
    let store = temp_store();
    let summaries = ["summary-origin-a", "summary-origin-b"].map(|origin| HistorySummary {
        baseline_required: false,
        origin: NodeId::new(origin),
        high_sequence: 3,
    });
    let before_batches = store.completed_batch_work().0;

    let repairs = record_summaries(&store, &summaries).unwrap();

    assert_eq!(
        repairs
            .iter()
            .map(|repair| (repair.origin.as_str(), repair.missing, repair.baseline))
            .collect::<Vec<_>>(),
        vec![
            ("summary-origin-a", Some((1, 3)), false),
            ("summary-origin-b", Some((1, 3)), false),
        ]
    );
    assert_eq!(store.completed_batch_work().0 - before_batches, 1);
    assert_eq!(
        replication_progress(&store)
            .unwrap()
            .iter()
            .map(|progress| (
                progress.origin.as_str().to_owned(),
                progress.known_high_sequence
            ))
            .collect::<Vec<_>>(),
        vec![
            ("summary-origin-a".to_owned(), 3),
            ("summary-origin-b".to_owned(), 3),
        ]
    );
}

#[test]
fn history_summary_page_storage_failure_does_not_persist_a_prefix() {
    let store = temp_store();
    let summaries = ["summary-origin-a", "summary-origin-b"].map(|origin| HistorySummary {
        baseline_required: false,
        origin: NodeId::new(origin),
        high_sequence: 3,
    });
    store.inject_disk_full_on_writes(true);

    let result = record_summaries(&store, &summaries);

    store.inject_disk_full_on_writes(false);
    assert!(result.is_err());
    assert!(replication_progress(&store).unwrap().is_empty());
}

fn transport_replay(entry: SequencedOp) -> nx_sync::HistoryMessage {
    nx_sync::HistoryMessage::Replay {
        request_id: nx_sync::OpId::new("\0".repeat(128)),
        origin: entry.op.origin.clone(),
        start: entry.position.sequence,
        end: entry.position.sequence,
        ops: vec![entry],
    }
}

#[test]
fn transport_preflight_checks_full_envelope_and_negotiable_codecs_at_exact_boundary() {
    use crate::sync_manager::types::LocalTransportLimits;
    use nx_net::{SerializationFormat, history_frame_len};
    for position in [
        OpPosition {
            generation: 1,
            sequence: 1,
        },
        OpPosition {
            generation: u64::MAX,
            sequence: u64::MAX,
        },
    ] {
        let entry = SequencedOp {
            op: Op::lww_register_set(
                NodeId::new("origin\\\"\n".repeat(20)),
                "key\\\"\0".repeat(16),
                [0, 9, 255, 128].repeat(256),
                u64::MAX,
            ),
            position,
        };
        let message = transport_replay(entry.clone());
        let json = history_frame_len(&message, SerializationFormat::Json).unwrap();
        let binary = history_frame_len(&message, SerializationFormat::Bincode).unwrap();
        assert!(
            json > binary,
            "JSON escaping and numeric byte arrays must be counted"
        );
        for format in [SerializationFormat::Json, SerializationFormat::Bincode] {
            let limits = LocalTransportLimits {
                max_message_size: json - 4,
                serialization_format: format,
            };
            crate::sync_manager::storage::validate_local_transport(&entry, limits).unwrap();
            let error = crate::sync_manager::storage::validate_local_transport(
                &entry,
                LocalTransportLimits {
                    max_message_size: limits.max_message_size - 1,
                    ..limits
                },
            )
            .unwrap_err();
            assert!(error.to_string().contains("transport message limit"));
        }
        let error = crate::sync_manager::storage::validate_local_transport(
            &entry,
            LocalTransportLimits {
                max_message_size: binary - 4,
                serialization_format: SerializationFormat::Bincode,
            },
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("transport message limit"),
            "Bincode must also allow JSON fallback"
        );
    }
}

#[test]
fn transport_preflight_retains_replay_byte_budget_even_with_large_transport_limit() {
    let entry = SequencedOp {
        op: Op::lww_register_set(NodeId::new("origin"), "key", vec![255; 2 * 1024 * 1024], 1),
        position: OpPosition {
            generation: 1,
            sequence: 1,
        },
    };
    let error = crate::sync_manager::storage::validate_local_transport(
        &entry,
        crate::sync_manager::types::LocalTransportLimits {
            max_message_size: u32::MAX as usize,
            serialization_format: crate::SerializationFormat::Bincode,
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("replay byte budget"));
}

#[tokio::test]
async fn transport_replay_budget_exact_boundary_survives_remote_apply_without_fixed_margin() {
    let budget = crate::sync_manager::sequence::REPLAY_BYTES;
    let mut entry = SequencedOp {
        op: Op::lww_register_set(NodeId::new("origin"), "", vec![255], 1),
        position: OpPosition {
            generation: 1,
            sequence: 1,
        },
    };
    let initial = nx_net::history_frame_len(
        &transport_replay(entry.clone()),
        crate::SerializationFormat::Json,
    )
    .unwrap();
    let remaining = budget - initial;
    let OpKind::LwwRegisterSet { key, value, .. } = &mut entry.op.kind else {
        unreachable!()
    };
    key.push_str(&"x".repeat(remaining % 4));
    value.resize(1 + remaining / 4, 255);
    assert_eq!(
        nx_net::history_frame_len(
            &transport_replay(entry.clone()),
            crate::SerializationFormat::Json
        )
        .unwrap(),
        budget,
    );
    assert!(serde_json::to_vec(&entry).unwrap().len() + 1024 > budget);
    let limits = crate::sync_manager::types::LocalTransportLimits {
        max_message_size: u32::MAX as usize,
        serialization_format: crate::SerializationFormat::Bincode,
    };
    crate::sync_manager::storage::validate_local_transport(&entry, limits).unwrap();
    let mut oversized = entry.clone();
    let OpKind::LwwRegisterSet { key, .. } = &mut oversized.op.kind else {
        unreachable!()
    };
    key.push('x');
    let error =
        crate::sync_manager::storage::validate_local_transport(&oversized, limits).unwrap_err();
    assert!(error.to_string().contains("replay byte budget"));
    let store = temp_store();
    let target =
        SyncManager::try_new(NodeId::new("target"), SyncConfig::new(), store, metrics()).unwrap();
    apply(&target, std::slice::from_ref(&entry)).await.unwrap();
    assert_eq!(
        target.handle().replication_progress().unwrap()[0].contiguous_sequence,
        1
    );
    assert_eq!(
        target.op_log.read().await.as_slice(),
        std::slice::from_ref(&entry.op)
    );
}

#[tokio::test]
async fn transport_host_api_preserves_internal_error_code_and_rejects_before_acceptance() {
    let store = temp_store();
    let manager = SyncManager::try_new(
        NodeId::new("host-local"),
        SyncConfig::new().with_max_message_size(64),
        Arc::clone(&store),
        metrics(),
    )
    .unwrap();
    let before = store.scan_prefix(b"").unwrap();
    let engine = wasmtime::Engine::default();
    let mut linker = wasmtime::Linker::new(&engine);
    crate::host_api::crdt::add_to_linker(&mut linker).unwrap();
    let module = wasmtime::Module::new(
        &engine,
        r#"
        (module
          (import "nx" "crdt_lww_set" (func $set (param i32 i32 i32 i32) (result i32)))
          (memory (export "memory") 1)
          (data (i32.const 0) "key")
          (data (i32.const 16) "\ff\00\ff")
          (func (export "run") (result i32)
            i32.const 0 i32.const 3 i32.const 16 i32.const 3 call $set))
    "#,
    )
    .unwrap();
    let mut guest = wasmtime::Store::new(
        &engine,
        crate::runtime::HostState {
            wasi: None,
            store: Arc::clone(&store),
            sync_handle: Some(manager.handle()),
            module_id: Arc::from("transport-preflight"),
            limits: wasmtime::StoreLimitsBuilder::new().build(),
        },
    );
    let instance = linker.instantiate_async(&mut guest, &module).await.unwrap();
    let run = instance
        .get_typed_func::<(), i32>(&mut guest, "run")
        .unwrap();
    assert_eq!(run.call_async(&mut guest, ()).await.unwrap(), -3);
    assert_eq!(store.scan_prefix(b"").unwrap(), before);
    assert!(manager.lww_registers.read().await.is_empty());
    assert!(manager.seen_ops.read().await.ids.is_empty());
    assert!(manager.op_log.read().await.is_empty());
}

#[tokio::test]
async fn transport_rejection_preserves_all_local_crdt_families_and_sequence_metadata() {
    for format in [
        crate::SerializationFormat::Json,
        crate::SerializationFormat::Bincode,
    ] {
        let store = temp_store();
        let manager = SyncManager::try_new(
            NodeId::new("local"),
            SyncConfig::new().with_serialization_format(format),
            Arc::clone(&store),
            metrics(),
        )
        .unwrap();
        let handle = manager.handle();
        handle.increment_gcounter("gc", 1).await.unwrap();
        handle.increment_pncounter("pn", 2).await.unwrap();
        handle.set_lww_register("reg", vec![1], 1).await.unwrap();
        handle
            .set_lww_map("map", "field", vec![2], 1)
            .await
            .unwrap();
        handle.add_orset("set", "element").await.unwrap();
        let inserted = handle
            .insert_rga(handle.rga_insert_op("rga", None, vec![3]))
            .await
            .unwrap();
        let before = store.scan_prefix(b"").unwrap();
        let before_seen = manager.seen_ops.read().await.ids.clone();
        let before_log = manager.op_log.read().await.clone();
        let seen_sequence = manager.seen_ops_next_sequence.load(Ordering::Relaxed);
        let log_sequence = manager.op_log_next_sequence.load(Ordering::Relaxed);
        let registries = (
            format!("{:?}", manager.counters.read().await),
            format!("{:?}", manager.pncounters.read().await),
            format!("{:?}", manager.lww_registers.read().await),
            format!("{:?}", manager.lww_maps.read().await),
            format!("{:?}", manager.orsets.read().await),
            format!("{:?}", manager.rgas.read().await),
        );
        let mut restricted = handle.clone();
        restricted.local_limits.max_message_size = 64;
        assert!(restricted.increment_gcounter("gc", 7).await.is_err());
        assert!(restricted.increment_pncounter("pn", 7).await.is_err());
        assert!(restricted.decrement_pncounter("pn", 7).await.is_err());
        assert!(
            restricted
                .set_lww_register("reg", vec![255; 128], 2)
                .await
                .is_err()
        );
        assert!(
            restricted
                .set_lww_map("map", "field", vec![255; 128], 2)
                .await
                .is_err()
        );
        assert!(restricted.remove_lww_map("map", "field", 2).await.is_err());
        assert!(restricted.add_orset("set", "new").await.is_err());
        assert!(restricted.remove_orset("set", "element").await.is_err());
        assert!(
            restricted
                .insert_rga(restricted.rga_insert_op("rga", None, vec![255; 128]))
                .await
                .is_err()
        );
        assert!(
            restricted
                .delete_rga("rga", inserted.id.as_str())
                .await
                .is_err()
        );
        assert_eq!(store.scan_prefix(b"").unwrap(), before);
        assert_eq!(manager.seen_ops.read().await.ids, before_seen);
        assert_eq!(*manager.op_log.read().await, before_log);
        assert_eq!(
            manager.seen_ops_next_sequence.load(Ordering::Relaxed),
            seen_sequence
        );
        assert_eq!(
            manager.op_log_next_sequence.load(Ordering::Relaxed),
            log_sequence
        );
        assert_eq!(
            registries,
            (
                format!("{:?}", manager.counters.read().await),
                format!("{:?}", manager.pncounters.read().await),
                format!("{:?}", manager.lww_registers.read().await),
                format!("{:?}", manager.lww_maps.read().await),
                format!("{:?}", manager.orsets.read().await),
                format!("{:?}", manager.rgas.read().await),
            )
        );
        let next = handle.increment_gcounter("gc", 1).await.unwrap();
        assert_eq!(next.position().sequence, inserted.position().sequence + 1);
    }
}

#[tokio::test]
async fn transport_accepted_exact_limit_operation_replays_over_tcp_including_json_fallback() {
    use nx_net::{SerializationFormat, history_frame_len};
    use nx_sync::HistoryMessage;
    for (local_format, peer_format) in [
        (SerializationFormat::Json, SerializationFormat::Json),
        (SerializationFormat::Bincode, SerializationFormat::Bincode),
        (SerializationFormat::Bincode, SerializationFormat::Json),
    ] {
        let origin = NodeId::new("origin\\\"");
        let value = vec![255; 2048];
        let mut expected = Op::lww_register_set(origin.clone(), "key\\\"", value.clone(), 1);
        expected.id = nx_sync::OpId::new("x".repeat(36));
        let entry = SequencedOp {
            op: expected,
            position: OpPosition {
                generation: 1,
                sequence: 1,
            },
        };
        let limit =
            history_frame_len(&transport_replay(entry), SerializationFormat::Json).unwrap() - 4;
        let store = temp_store();
        let address = free_addr();
        let mut source = SyncManager::try_new(
            origin.clone(),
            SyncConfig::new()
                .with_listen_addr(address.clone())
                .with_serialization_format(local_format)
                .with_max_message_size(limit),
            Arc::clone(&store),
            metrics(),
        )
        .unwrap();
        let handle = source.handle();
        let accepted = handle
            .set_lww_register("key\\\"", value, 1)
            .await
            .unwrap()
            .into_sequenced();
        assert_eq!(
            history_frame_len(
                &transport_replay(accepted.clone()),
                SerializationFormat::Json
            )
            .unwrap(),
            limit + 4
        );
        let before = store.scan_prefix(b"").unwrap();
        assert!(
            handle
                .set_lww_register("key\\\"", vec![255; 2049], 1)
                .await
                .is_err()
        );
        assert_eq!(store.scan_prefix(b"").unwrap(), before);
        source.start().await.unwrap();
        let mut peer = Node::new(
            NodeConfig::new(NodeId::new("requester"), "127.0.0.1:0")
                .with_serialization_format(peer_format),
        );
        let mut events = peer.take_event_receiver().unwrap();
        peer.connect_to_peer(&address).await.unwrap();
        let request_id = nx_sync::OpId::new("\0".repeat(128));
        peer.send_history_to_addr(
            &address,
            HistoryMessage::Request {
                request_id: request_id.clone(),
                origin,
                start: 1,
                end: 1,
            },
        )
        .await
        .unwrap();
        let ops = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(NodeEvent::HistoryReceived { message, .. }) = events.recv().await {
                    match message {
                        HistoryMessage::Replay {
                            request_id: received,
                            ops,
                            ..
                        } => {
                            assert_eq!(received, request_id);
                            break ops;
                        }
                        HistoryMessage::Unavailable { .. } => {
                            panic!("accepted retained operation was excluded from replay")
                        }
                        _ => {}
                    }
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(ops, vec![accepted]);
        peer.shutdown().await;
        source.shutdown().await.unwrap();
    }
}

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
        record_unavailable_and_previous(&store, &origin, evidence.clone()).unwrap();
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
        "before_flush" => CrashCheckpoint::BeforeFlush,
        "after_flush" => CrashCheckpoint::AfterFlush,
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
    for checkpoint in ["before", "after", "before_flush", "after_flush"] {
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
        if checkpoint == "after_flush" {
            assert!(survived, "flush-confirmed remote operation was lost");
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
            storage_executor: crate::peer_io::StorageExecutor::Blocking,
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
async fn migrated_v015_all_families_preserve_state_and_reject_legacy_after_eviction_and_reopen() {
    use crate::sync_manager::{MigrationOptions, fixtures::parse_fixture, migrate_sync_schema};

    let records = parse_fixture(include_str!("../fixtures/v0_1_5/records.hex"));
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(NxStore::open(directory.path()).unwrap());
    for (key, value) in &records {
        store.set(key, value).unwrap();
    }
    migrate_sync_schema(&store, MigrationOptions::default()).unwrap();
    let legacy: Vec<_> = records
        .iter()
        .filter(|(key, _)| key.starts_with(b"__nx/crdt/op-log/"))
        .map(|(_, value)| parse_durable_op_log_value(value).unwrap().1)
        .collect();
    assert_eq!(legacy.len(), 6);
    for op in &legacy {
        assert!(sequenced_op(&store, op).unwrap().is_none());
    }
    let config = SyncConfig::new()
        .with_seen_ops_limit(1)
        .with_op_log_limit(1);
    let origin = NodeId::new("fixture-node-a");
    let target = SyncManager::try_new(
        origin.clone(),
        config.clone(),
        Arc::clone(&store),
        metrics(),
    )
    .unwrap();
    let progress = target.handle().replication_progress().unwrap();
    assert_eq!(progress.len(), 1);
    assert_eq!(progress[0].contiguous_sequence, 0);
    assert_eq!(progress[0].known_high_sequence, 0);
    assert!(progress[0].baseline_required);
    // Make real new writes so old seen IDs and replay entries are evicted.
    // New coverage must never certify the pre-sequence baseline as recovered.
    for _ in 0..2 {
        target
            .handle()
            .increment_gcounter("new-writes", 1)
            .await
            .unwrap();
    }
    for op in &legacy {
        assert!(
            store
                .get(&seen_op_store_key(op.id.as_str()))
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .get(&op_log_store_key(op.id.as_str()))
                .unwrap()
                .is_none()
        );
    }
    store.flush().unwrap();
    drop(target);
    drop(store);
    let store = Arc::new(NxStore::open(directory.path()).unwrap());
    let target = SyncManager::try_new(origin, config, Arc::clone(&store), metrics()).unwrap();
    let mut context = test_event_context(
        Arc::clone(&target.counters),
        Arc::clone(&target.seen_ops),
        Arc::clone(&target.op_log),
        Arc::clone(&store),
        metrics(),
        Arc::new(RwLock::new(HashMap::new())),
    );
    context.pncounters = Arc::clone(&target.pncounters);
    context.lww_registers = Arc::clone(&target.lww_registers);
    context.lww_maps = Arc::clone(&target.lww_maps);
    context.orsets = Arc::clone(&target.orsets);
    context.rgas = Arc::clone(&target.rgas);
    let before = store.scan_prefix(b"").unwrap();
    handle_node_event(
        NodeEvent::OpsReceived {
            from: NodeId::new("legacy-peer"),
            ops: legacy,
        },
        &context,
    )
    .await;
    assert_eq!(store.scan_prefix(b"").unwrap(), before);
    assert!(
        context
            .metrics
            .render_for_test(&store)
            .contains("numax_sync_errors_total 1\n")
    );
    assert_eq!(target.get_counter_value("visits").await, 12);
    assert_eq!(target.get_pncounter_value("stock").await, 6);
    assert_eq!(
        target.get_lww_register_value("status").await,
        Some(b"online".to_vec())
    );
    assert_eq!(
        target.get_lww_map_entries("settings").await,
        vec![("region".to_owned(), b"eu".to_vec())]
    );
    assert_eq!(target.get_orset_elements("tags").await, vec!["blue"]);
    assert_eq!(
        target.get_rga_values("document").await,
        vec![b"hello".to_vec()]
    );
    // Compare original durable AND materialized bytes, including map/set/RGA tombstones.
    for (key, value) in &records {
        if key.starts_with(b"__nx/crdt/")
            && !key.starts_with(b"__nx/crdt/seen-op/")
            && !key.starts_with(b"__nx/crdt/op-log/")
        {
            assert_eq!(
                store.get(key).unwrap().as_ref(),
                Some(value),
                "historical key: {key:?}"
            );
        }
    }
    let progress = target.handle().replication_progress().unwrap();
    assert_eq!(progress[0].contiguous_sequence, 2);
    assert!(progress[0].baseline_required);
    let (summaries, _) = summary_page(&store, None).unwrap();
    assert_eq!(summaries.len(), 1);
    assert!(summaries[0].baseline_required);
    let receiver = temp_store();
    record_summary(&receiver, &summaries[0]).unwrap();
    let progress = replication_progress(&receiver).unwrap();
    assert_eq!(progress[0].contiguous_sequence, 0);
    assert_eq!(progress[0].known_high_sequence, 2);
    assert!(progress[0].baseline_required);
}

#[tokio::test]
async fn migrated_v015_tcp_history_advertises_baseline_and_never_replays_legacy_in_both_formats() {
    use crate::sync_manager::{MigrationOptions, fixtures::parse_fixture, migrate_sync_schema};
    use nx_sync::{HistoryMessage, OpId};

    for format in [
        crate::SerializationFormat::Json,
        crate::SerializationFormat::Bincode,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(NxStore::open(directory.path()).unwrap());
        for (key, value) in parse_fixture(include_str!("../fixtures/v0_1_5/records.hex")) {
            store.set(&key, &value).unwrap();
        }
        migrate_sync_schema(&store, MigrationOptions::default()).unwrap();
        let addr = free_addr();
        let origin = NodeId::new("fixture-node-a");
        let mut source = SyncManager::try_new(
            origin.clone(),
            SyncConfig::new()
                .with_listen_addr(addr.clone())
                .with_serialization_format(format),
            Arc::clone(&store),
            metrics(),
        )
        .unwrap();
        source.start().await.unwrap();
        assert_eq!(source.op_log.read().await.len(), 6);
        let mut peer = Node::new(
            NodeConfig::new(NodeId::new("fresh-peer"), "127.0.0.1:0")
                .with_serialization_format(format),
        );
        let mut events = peer.take_event_receiver().unwrap();
        peer.connect_to_peer(&addr).await.unwrap();
        peer.send_history_to_addr(&addr, HistoryMessage::SummaryRequest { after_origin: None })
            .await
            .unwrap();
        let summary = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match events.recv().await.unwrap() {
                    NodeEvent::HistoryReceived {
                        message: HistoryMessage::Summary { entries },
                        ..
                    } => break entries,
                    NodeEvent::OpsReceived { .. }
                    | NodeEvent::HistoryReceived {
                        message: HistoryMessage::Push { .. } | HistoryMessage::Replay { .. },
                        ..
                    } => panic!("legacy operation was sent to a fresh peer"),
                    _ => {}
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(
            summary,
            vec![HistorySummary {
                origin: origin.clone(),
                high_sequence: 0,
                baseline_required: true
            }]
        );
        let request_id = OpId::new("legacy-must-not-be-renumbered");
        peer.send_history_to_addr(
            &addr,
            HistoryMessage::Request {
                request_id: request_id.clone(),
                origin: origin.clone(),
                start: 1,
                end: 6,
            },
        )
        .await
        .unwrap();
        let response = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match events.recv().await.unwrap() {
                    NodeEvent::HistoryReceived {
                        message: message @ HistoryMessage::Unavailable { .. },
                        ..
                    } => break message,
                    NodeEvent::OpsReceived { .. }
                    | NodeEvent::HistoryReceived {
                        message: HistoryMessage::Push { .. } | HistoryMessage::Replay { .. },
                        ..
                    } => panic!("legacy operation was replayed as sequenced history"),
                    _ => {}
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(
            response,
            HistoryMessage::Unavailable {
                request_id,
                origin,
                start: 1,
                end: 6
            }
        );
        assert!(!store.prefix_exists(b"__nx/crdt/sequence/replay/").unwrap());
        peer.shutdown().await;
        source.shutdown().await.unwrap();
    }
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
    assert_eq!(
        target.get_lww_register_value("status").await,
        Some(b"ready".to_vec())
    );
    assert!(target.get_lww_map_entries("settings").await.is_empty());
    assert!(target.get_orset_elements("tags").await.is_empty());
    assert!(target.get_rga_values("comments").await.is_empty());
    assert_eq!(read_materialized(&store, "visits"), 7);
    assert_eq!(read_materialized_pncounter(&store, "balance"), 5);
    assert_eq!(read_materialized_lww_register(&store, "status"), b"ready");
    assert!(read_materialized_lww_map(&store, "settings").is_empty());
    assert!(read_materialized_orset(&store, "tags").is_empty());
    assert!(read_materialized_rga(&store, "comments").is_empty());
    assert_eq!(
        serde_json::to_value(read_durable_gcounter_state(&store, "visits")).unwrap(),
        serde_json::to_value(read_durable_gcounter_state(&source.store, "visits")).unwrap()
    );
    assert_eq!(
        serde_json::to_value(read_durable_pncounter_state(&store, "balance")).unwrap(),
        serde_json::to_value(read_durable_pncounter_state(&source.store, "balance")).unwrap()
    );
    assert_eq!(
        read_durable_lww_register_state(&store, "status"),
        read_durable_lww_register_state(&source.store, "status")
    );
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
    assert_eq!(target.op_log.read().await.len(), 1);
    let progress = target.handle().replication_progress().unwrap();
    assert_eq!(progress.len(), 1);
    assert_eq!(progress[0].contiguous_sequence, operations.len() as u64);
    assert_eq!(progress[0].known_high_sequence, operations.len() as u64);
    assert_eq!(progress[0].sparse_positions, 0);
    assert!(progress[0].recovery_failure.is_none());
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
