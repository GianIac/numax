use super::*;
use crate::control::{ControlError, PeerObservation, PeerView};

#[tokio::test]
async fn manual_candidate_lifecycle_concurrency_capacity_and_restart() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(NxStore::open(directory.path()).unwrap());
    let config = SyncConfig::new().with_listen_addr("127.0.0.1:0");
    let make = |store| {
        SyncManager::try_new_with_discovery(
            NodeId::new("manual-owner"),
            config.clone(),
            store,
            metrics(),
            vec![crate::DiscoveryProvider::new(
                "static",
                Arc::new(crate::StaticDiscovery::new(vec!["example.com:9000".into()])),
            )],
            crate::DiscoveryRuntimeConfig::default().with_max_candidates(2),
        )
        .unwrap()
    };
    let mut manager = make(Arc::clone(&store));
    let handle = manager.handle();
    assert_eq!(
        handle.add_peer("example.com:9000".into()).await,
        Err(ControlError::ServiceUnavailable)
    );
    manager.start().await.unwrap();
    assert_eq!(
        handle.add_peer("0.0.0.0:9000".into()).await,
        Err(ControlError::InvalidPeer)
    );
    let endpoint = test_owner_record(&mut manager).await.endpoint.unwrap();
    assert_eq!(
        handle.add_peer(endpoint).await,
        Err(ControlError::InvalidPeer)
    );
    let (a, b) = tokio::join!(
        handle.add_peer("EXAMPLE.COM.:9000".into()),
        handle.add_peer("example.com:9000".into()),
    );
    assert_ne!(a.unwrap().created, b.unwrap().created);
    let candidates = handle.inspect_peers(PeerView::Candidates).await.unwrap();
    assert!(
        matches!(&candidates[0], PeerObservation::Candidate { candidate, .. }
        if candidate.sources == ["manual", "static"] && candidate.persisted)
    );
    handle.add_peer("127.0.0.1:1".into()).await.unwrap();
    assert_eq!(
        handle.add_peer("127.0.0.1:2".into()).await,
        Err(ControlError::PeerCapacityExceeded)
    );
    manager.shutdown().await.unwrap();
    assert_eq!(
        handle.add_peer("127.0.0.1:3".into()).await,
        Err(ControlError::ServiceUnavailable)
    );
    drop(handle);
    drop(manager);
    drop(store);
    let mut restarted = make(Arc::new(NxStore::open(directory.path()).unwrap()));
    restarted.start().await.unwrap();
    assert_eq!(restarted.peer_candidates().len(), 2);
    assert!(
        !restarted
            .handle()
            .add_peer("127.0.0.1:1".into())
            .await
            .unwrap()
            .created
    );
    restarted.shutdown().await.unwrap();
}

#[tokio::test]
async fn manual_flush_failure_never_publishes_candidate() {
    let (mut manager, handle, store) =
        started_manager_with_config(SyncConfig::new().with_listen_addr("127.0.0.1:0")).await;
    store.inject_disk_full_on_flush(true);
    assert!(matches!(
        handle.add_peer("127.0.0.1:1".into()).await,
        Err(ControlError::PeerAcceptanceUncertain(_))
    ));
    assert!(manager.peer_candidates().is_empty());
    assert!(handle.connected_peers().await.is_empty());
    store.inject_disk_full_on_flush(false);
    // The existing durability fence intentionally survives clearing the injected fault.
    assert!(handle.add_peer("127.0.0.1:1".into()).await.is_err());
    let _ = manager.shutdown().await;
}

#[tokio::test]
async fn manual_candidate_survives_rejected_handshake_without_becoming_a_connection() {
    let config = || {
        SyncConfig::new()
            .with_listen_addr("127.0.0.1:0")
            .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(40))
    };
    let (mut owner, handle, _) = started_manager_with_config(config()).await;
    let mut impersonator =
        SyncManager::try_new(owner.node_id().clone(), config(), temp_store(), metrics()).unwrap();
    impersonator.start().await.unwrap();
    let endpoint = test_owner_record(&mut impersonator).await.endpoint.unwrap();
    assert!(handle.add_peer(endpoint.clone()).await.unwrap().created);
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut tick = tokio::time::interval(Duration::from_millis(5));
        loop {
            if owner
                .peer_health
                .read()
                .await
                .get(&endpoint)
                .is_some_and(|health| health.consecutive_failures > 0)
            {
                break;
            }
            tick.tick().await;
        }
    })
    .await
    .unwrap();
    assert!(handle.active_connections().await.is_empty());
    assert!(
        matches!(&handle.inspect_peers(PeerView::Candidates).await.unwrap()[0],
        PeerObservation::Candidate { candidate, .. }
        if candidate.address == endpoint && candidate.persisted)
    );
    assert!(
        handle
            .inspect_peers(PeerView::Membership)
            .await
            .unwrap()
            .iter()
            .all(
                |observation| matches!(observation, PeerObservation::Membership { record, .. }
                if record.member == owner.node_id().as_str())
            )
    );
    owner.shutdown().await.unwrap();
    impersonator.shutdown().await.unwrap();
}

#[tokio::test]
async fn manual_access_is_unavailable_after_partial_startup_rollback() {
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let store = temp_store();
    super::super::super::manual::persist(&store, "default", "127.0.0.1:1").unwrap();
    let mut manager = SyncManager::try_new(
        NodeId::new("rollback-owner"),
        SyncConfig::new().with_listen_addr(occupied.local_addr().unwrap().to_string()),
        Arc::clone(&store),
        metrics(),
    )
    .unwrap();
    let handle = manager.handle();
    assert!(manager.start().await.is_err());
    assert!(manager.node.is_none());
    assert!(manager.discovery_coordinator.is_none());
    assert_eq!(
        handle.add_peer("127.0.0.1:2".into()).await,
        Err(ControlError::ServiceUnavailable)
    );
    assert_eq!(
        super::super::super::manual::load(&store, "default", 2).unwrap(),
        ["127.0.0.1:1"]
    );
    drop(occupied);
    manager.start().await.unwrap();
    assert!(manager.node.is_some());
    assert!(manager.discovery_coordinator.is_some());
    assert!(handle.add_peer("127.0.0.1:2".into()).await.unwrap().created);
    manager.shutdown().await.unwrap();
    assert!(manager.start().await.is_err());
    assert!(manager.discovery_coordinator.is_none());
}

#[tokio::test]
async fn manual_startup_retry_reuses_file_discovery_after_bind_failure() {
    use crate::{FileWatchDiscovery, FileWatchDiscoveryConfig, PeerDiscovery};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("peers");
    tokio::fs::write(&path, "127.0.0.1:1").await.unwrap();
    let provider = Arc::new(FileWatchDiscovery::new(FileWatchDiscoveryConfig::new(path)).unwrap());
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut manager = SyncManager::try_new_with_discovery(
        NodeId::new("dynamic-rollback-owner"),
        SyncConfig::new().with_listen_addr(occupied.local_addr().unwrap().to_string()),
        temp_store(),
        metrics(),
        vec![crate::DiscoveryProvider::new("file", provider.clone())],
        crate::DiscoveryRuntimeConfig::default(),
    )
    .unwrap();
    let handle = manager.handle();
    assert!(manager.start().await.is_err());
    assert!(manager.event_task.is_none());
    assert!(manager.broadcast_task.is_none());
    assert!(manager.reconnect_task.is_none());
    assert!(manager.membership.is_none());
    drop(occupied);
    manager.start().await.unwrap();
    assert_eq!(manager.peer_candidates(), ["127.0.0.1:1"]);
    assert!(handle.add_peer("127.0.0.1:2".into()).await.unwrap().created);
    manager.shutdown().await.unwrap();
    assert!(provider.watch().await.is_err());
}

#[tokio::test]
async fn manual_activation_failure_rolls_back_listener_membership_and_preserves_receiver() {
    struct StopAtAnnouncement {
        access: StdMutex<Option<crate::sync_manager::candidates::CandidateAccess>>,
        discovery: crate::StaticDiscovery,
    }
    #[async_trait::async_trait]
    impl crate::PeerDiscovery for StopAtAnnouncement {
        fn announcement_support(&self) -> crate::AnnouncementSupport {
            crate::AnnouncementSupport::Required
        }
        async fn discover(&self) -> Result<crate::DiscoverySnapshot, crate::DiscoveryError> {
            self.discovery.discover().await
        }
        async fn watch(&self) -> Result<crate::DiscoveryWatch, crate::DiscoveryError> {
            self.discovery.watch().await
        }
        async fn announce(&self, _: &crate::PeerAnnouncement) -> Result<(), crate::DiscoveryError> {
            self.access.lock().unwrap().as_ref().unwrap().stop();
            Ok(())
        }
    }
    let provider = Arc::new(StopAtAnnouncement {
        access: StdMutex::new(None),
        discovery: crate::StaticDiscovery::new(Vec::new()),
    });
    let address = free_addr();
    let mut manager = SyncManager::try_new_with_discovery(
        NodeId::new("activation-failure"),
        SyncConfig::new().with_listen_addr(&address),
        temp_store(),
        metrics(),
        vec![crate::DiscoveryProvider::new(
            "stop-at-announcement",
            provider.clone(),
        )],
        crate::DiscoveryRuntimeConfig::default(),
    )
    .unwrap();
    *provider.access.lock().unwrap() = Some(manager.candidates.clone());
    assert!(manager.start().await.is_err());
    assert!(manager.node.is_none());
    assert!(manager.membership.is_none());
    assert!(manager.discovery_coordinator.is_none());
    assert!(manager.event_task.is_none());
    assert!(manager.broadcast_task.is_none());
    assert!(manager.forward_task.is_none());
    assert!(manager.reconnect_task.is_none());
    assert!(manager.anti_entropy_task.is_none());
    assert!(manager.op_rx.is_some());
    assert!(manager.membership_view.lock().unwrap().is_none());
    let _released = std::net::TcpListener::bind(address).unwrap();
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn manual_persisted_local_endpoint_is_filtered_before_global_startup_capacity() {
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let local = occupied.local_addr().unwrap().to_string();
    let store = temp_store();
    super::super::super::manual::persist(&store, "default", &local).unwrap();
    drop(occupied);
    let mut manager = SyncManager::try_new_with_discovery(
        NodeId::new("local-suppression"),
        SyncConfig::new().with_listen_addr(&local),
        Arc::clone(&store),
        metrics(),
        vec![crate::DiscoveryProvider::new(
            "static",
            Arc::new(crate::StaticDiscovery::new(vec!["127.0.0.1:1".into()])),
        )],
        crate::DiscoveryRuntimeConfig::default().with_max_candidates(1),
    )
    .unwrap();
    let handle = manager.handle();
    manager.start().await.unwrap();
    assert_eq!(manager.peer_candidates(), ["127.0.0.1:1"]);
    assert!(
        matches!(&handle.inspect_peers(PeerView::Candidates).await.unwrap()[0],
        PeerObservation::Candidate { candidate, .. }
        if candidate.sources == ["static"] && !candidate.persisted)
    );
    assert_eq!(
        handle.add_peer("127.0.0.1:1".into()).await,
        Err(ControlError::PeerCapacityExceeded)
    );
    assert_eq!(
        super::super::super::manual::load(&store, "default", 1).unwrap(),
        [local]
    );
    manager.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_recovery_scan_permit_survives_cancelled_request_and_handle_clones() {
    let store = temp_store();
    let mut manager = SyncManager::try_new(
        NodeId::new("bounded-inspection"),
        SyncConfig::new(),
        store,
        metrics(),
    )
    .unwrap();
    let handle = manager.handle();
    let (reached, checkpoint) = std::sync::mpsc::channel();
    let (resume, release) = std::sync::mpsc::channel();
    let request = tokio::spawn({
        let limit = Arc::clone(&handle.recovery_scan_limit);
        async move {
            run_recovery_scan(&limit, move || {
                reached.send(()).unwrap();
                release.recv().unwrap();
                Ok(Vec::new())
            })
            .await
        }
    });
    tokio::task::spawn_blocking(move || checkpoint.recv_timeout(Duration::from_secs(2)).unwrap())
        .await
        .unwrap();
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    assert_eq!(handle.recovery_scan_limit.available_permits(), 0);
    for _ in 0..32 {
        assert!(matches!(
            handle.clone().inspect_peers(PeerView::Recovery).await,
            Err(ControlError::ServiceUnavailable)
        ));
    }
    resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        let _completed = handle.recovery_scan_limit.acquire().await.unwrap();
    })
    .await
    .unwrap();
    assert!(
        handle
            .inspect_peers(PeerView::Recovery)
            .await
            .unwrap()
            .is_empty()
    );
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn manual_introspection_distinguishes_candidate_and_source_recovery_failure() {
    let (mut manager, handle, store) =
        started_manager_with_config(SyncConfig::new().with_listen_addr("127.0.0.1:0")).await;
    handle.add_peer("127.0.0.1:1".into()).await.unwrap();
    let origin = NodeId::new("missing-origin");
    record_summary(
        &store,
        &nx_sync::HistorySummary {
            origin: origin.clone(),
            high_sequence: 4,
            baseline_required: true,
        },
    )
    .unwrap();
    let revision = recovery_revision(&store, &origin).unwrap();
    assert!(
        record_recovery_failure(
            &store,
            &origin,
            "127.0.0.1:1",
            nx_sync::BaselineFailure::SourceIncomplete,
            &revision,
        )
        .unwrap()
    );
    let recovery = handle.inspect_peers(PeerView::Recovery).await.unwrap();
    assert!(
        matches!(&recovery[0], PeerObservation::Recovery { progress, in_flight }
        if progress.origin == origin
            && progress.known_high_sequence == 4
            && progress.contiguous_sequence == 0
            && progress.baseline_missing
            && progress.recovery_failure.as_ref().unwrap().reason
                == nx_sync::BaselineFailure::SourceIncomplete
            && !in_flight)
    );
    assert_eq!(
        handle
            .inspect_peers(PeerView::Candidates)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        handle
            .inspect_peers(PeerView::Connections)
            .await
            .unwrap()
            .is_empty()
    );
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn manual_peer_uses_real_admission_membership_and_reconnection() {
    for format in [
        nx_net::SerializationFormat::Json,
        nx_net::SerializationFormat::Bincode,
    ] {
        let config = || {
            SyncConfig::new()
                .with_listen_addr("127.0.0.1:0")
                .with_serialization_format(format)
                .with_reconnect_backoff(Duration::from_millis(10), Duration::from_millis(40))
                .with_anti_entropy_interval(Duration::from_millis(20))
        };
        let (mut a, ha, _) = started_manager_with_config(config()).await;
        let (mut b, _, _) = started_manager_with_config(config()).await;
        let endpoint = test_owner_record(&mut b).await.endpoint.unwrap();
        ha.add_peer(endpoint).await.unwrap();
        tokio::time::timeout(Duration::from_secs(8), async {
            let mut poll = tokio::time::interval(Duration::from_millis(10));
            loop {
                let snapshot = a.membership.as_ref().unwrap().snapshot.borrow().clone();
                if snapshot.successful_probes > 0
                    && snapshot
                        .records
                        .iter()
                        .any(|record| record.member == b.node_id().as_str())
                {
                    break;
                }
                poll.tick().await;
            }
        })
        .await
        .unwrap();
        let connections = ha.active_connections().await;
        assert_eq!(connections.len(), 1);
        assert_eq!(
            connections[0].identity.verification,
            PeerIdentityVerification::Unverified
        );
        let old_connections = a.metrics.render_for_test(&a.store);
        assert!(
            old_connections.contains("numax_membership_probe_attempts_total{route=\"direct\"}")
        );
        let (address, identity) = ha.connected_peers().await.pop().unwrap();
        assert!(
            a.node
                .as_ref()
                .unwrap()
                .disconnect_peer(&address, &identity)
                .await
        );
        wait_for_connected_peer(&a).await;
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    }
}
