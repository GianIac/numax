use super::*;
use nx_net::{MembershipState, SerializationFormat};

#[tokio::test]
async fn rotating_full_slot_recovers_six_families_after_data_partition() {
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
        tokio::time::timeout(Duration::from_secs(10), async {
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
                }
                if complete {
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
                }
                if complete {
                    break;
                }
                poll.tick().await;
            }
        })
        .await
        .unwrap();
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
    tokio::time::timeout(Duration::from_secs(10), async {
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
    .await
    .unwrap();
    for manager in [&mut a, &mut b, &mut c, &mut d] {
        manager.shutdown().await.unwrap();
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
        node: Arc::clone(a.node.as_ref().unwrap()),
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
