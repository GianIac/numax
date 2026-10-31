use super::*;

#[tokio::test]
async fn rate_limited_fast_path_retains_all_families_for_periodic_repair() {
    for format in [
        nx_net::SerializationFormat::Json,
        nx_net::SerializationFormat::Bincode,
    ] {
        let config = || {
            SyncConfig::new()
                .with_listen_addr("127.0.0.1:0")
                .with_serialization_format(format)
                .with_gossip_rate(crate::GossipRateConfig {
                    min_bytes_per_second: 1,
                    max_bytes_per_second: 1,
                    burst_bytes: 1,
                })
        };
        let (mut a, ha, sa) = started_manager_with_config(config()).await;
        let (mut b, _, sb) = started_manager_with_config(config()).await;
        for manager in [&mut a, &mut b] {
            if let Some(task) = manager.anti_entropy_task.take() {
                task.abort();
                let _ = task.await;
            }
        }
        let endpoint = test_owner_record(&mut b).await.endpoint.unwrap();
        a.connect_to_peer(&endpoint).await.unwrap();
        local_increment(&ha, "warmup", 1).await;
        wait_for_counter(&b, "warmup", 1).await;
        // Warmup consumed this connection's one-byte burst. Every following
        // gossip frame is deferred; only the separate repair path can deliver it.
        local_increment(&ha, "paced-g", 7).await;
        local_pncounter_inc(&ha, "paced-p", 9).await;
        local_pncounter_dec(&ha, "paced-p", 4).await;
        local_lww_register_set(&ha, "paced-r", b"winner", 200).await;
        local_lww_map_set(&ha, "paced-m", "field", b"removed", 100).await;
        local_lww_map_remove(&ha, "paced-m", "field", 200).await;
        local_orset_add(&ha, "paced-o", "removed").await;
        local_orset_remove(&ha, "paced-o", "removed").await;
        let removed = local_rga_insert_after(&ha, "paced-a", None, b"removed").await;
        local_rga_delete(&ha, "paced-a", &removed).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut poll = tokio::time::interval(Duration::from_millis(5));
            loop {
                let rendered = a.metrics.render_for_test(&sa);
                let limited = rendered
                    .lines()
                    .find_map(|line| line.strip_prefix("numax_gossip_rate_limited_total "))
                    .unwrap()
                    .parse::<u64>()
                    .unwrap();
                if limited > 0 {
                    break;
                }
                poll.tick().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(b.get_counter_value("paced-g").await, 0);
        // Resume the production anti-entropy task with its existing independent budget.
        b.anti_entropy_task = spawn_anti_entropy_loop(AntiEntropyLoopContext {
            store: Arc::clone(&b.store),
            jitter_seed: 1,
            repair_budget: Arc::new(std::sync::Mutex::new(
                crate::sync_manager::delivery::RepairBudget::new(StdInstant::now()),
            )),
            history_cursors: Arc::clone(&b.history_cursors),
            request_timeout: b.config.socket_timeout,
            node: (b.node.as_ref().unwrap()).clone(),
            interval: Duration::from_millis(10),
            shutdown_rx: b.shutdown_tx.subscribe(),
            metrics: Arc::clone(&b.metrics),
        });
        wait_for_counter(&b, "paced-g", 7).await;
        wait_for_pncounter(&b, "paced-p", 5).await;
        wait_for_lww_register(&b, "paced-r", b"winner").await;
        // A visible empty set/map/list is insufficient: wait for durable remove
        // records and compare full state, including observed tags and tombstones.
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut poll = tokio::time::interval(Duration::from_millis(5));
            loop {
                let complete = [
                    durable_gcounter_state_key("paced-g"),
                    durable_pncounter_state_key("paced-p"),
                    durable_lww_register_state_key("paced-r"),
                    durable_lww_map_state_key("paced-m"),
                    durable_orset_state_key("paced-o"),
                    durable_rga_state_key("paced-a"),
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
        assert_eq!(read_materialized(&sb, "paced-g"), 7);
        assert_eq!(read_materialized_pncounter(&sb, "paced-p"), 5);
        assert_eq!(read_materialized_lww_register(&sb, "paced-r"), b"winner");
        assert!(read_materialized_lww_map(&sb, "paced-m").is_empty());
        assert!(read_materialized_orset(&sb, "paced-o").is_empty());
        assert!(read_materialized_rga(&sb, "paced-a").is_empty());
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn real_direct_probe_rtt_reduces_and_recovers_gossip_rate() {
    let mut peer = ControlledControlPeer::start().await;
    let maximum = 256 * 1024;
    let (mut manager, handle, store) = started_manager_with_config(
        SyncConfig::new()
            .with_listen_addr("127.0.0.1:0")
            .with_queued_ops_limit(1)
            .with_gossip_rate(crate::GossipRateConfig {
                min_bytes_per_second: 64 * 1024,
                max_bytes_per_second: maximum,
                burst_bytes: 64 * 1024,
            }),
    )
    .await;
    manager.connect_to_peer(&peer.endpoint).await.unwrap();
    local_increment(&handle, "rtt-load", 1).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut poll = tokio::time::interval(Duration::from_millis(20));
        loop {
            local_increment(&handle, "rtt-load", 1).await;
            if manager
                .metrics
                .render_for_test(&store)
                .contains("numax_gossip_control_rtt_seconds ")
            {
                break;
            }
            poll.tick().await;
        }
    })
    .await
    .unwrap();
    let changed = StdInstant::now();
    peer.delay_ms
        .store(150, std::sync::atomic::Ordering::Relaxed);
    let low = tokio::time::timeout(Duration::from_secs(8), async {
        let mut poll = tokio::time::interval(Duration::from_millis(20));
        loop {
            local_increment(&handle, "rtt-load", 1).await;
            let has_slow_sample = manager
                .membership
                .as_ref()
                .unwrap()
                .snapshot
                .borrow()
                .probe_timings
                .iter()
                .any(|sample| {
                    sample.timing.observed_at > changed
                        && sample.timing.round_trip >= Duration::from_millis(150)
                });
            let rate = metric_value(
                &manager.metrics,
                &store,
                "numax_gossip_rate_bytes_per_second",
            );
            if has_slow_sample && rate < maximum {
                break rate;
            }
            poll.tick().await;
        }
    })
    .await
    .unwrap();
    peer.delay_ms
        .store(10, std::sync::atomic::Ordering::Relaxed);
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut poll = tokio::time::interval(Duration::from_millis(20));
        let mut previous = low;
        loop {
            local_increment(&handle, "rtt-load", 1).await;
            let rate = metric_value(
                &manager.metrics,
                &store,
                "numax_gossip_rate_bytes_per_second",
            );
            assert!(rate >= 64 * 1024 && rate <= maximum);
            if rate > previous {
                assert!(rate - previous <= maximum / 16);
                break;
            }
            previous = rate;
            poll.tick().await;
        }
    })
    .await
    .unwrap();
    manager.shutdown().await.unwrap();
    peer.shutdown().await;
}
