use super::super::support::*;

#[tokio::test]
async fn manual_register_recovers_history_and_new_writes() {
    for format in [
        nx_net::SerializationFormat::Json,
        nx_net::SerializationFormat::Bincode,
    ] {
        let (mut a, ha, sa, mut b, hb, sb, endpoint) = manual_pair(format).await;
        local_lww_register_set(&ha, "manual-r", b"before", 100).await;
        hb.add_peer(endpoint).await.unwrap();
        wait_for_lww_register(&b, "manual-r", b"before").await;
        local_lww_register_set(&hb, "manual-r", b"after", 200).await;
        wait_for_lww_register(&a, "manual-r", b"after").await;
        assert_eq!(
            read_materialized_lww_register(&sa, "manual-r"),
            read_materialized_lww_register(&sb, "manual-r")
        );
        assert_eq!(
            read_durable_lww_register_state(&sa, "manual-r"),
            read_durable_lww_register_state(&sb, "manual-r")
        );
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn e2e_two_nodes_lww_register_sets_converge_to_latest_value() {
    let key = "status:user-1";
    let addr_a = free_addr();
    let addr_b = free_addr();
    let (manager_a, handle_a, store_a) = started_manager(addr_a.clone()).await;
    let (manager_b, handle_b, store_b) = started_manager(addr_b.clone()).await;

    manager_a.connect_to_peer(&addr_b).await.unwrap();
    manager_b.connect_to_peer(&addr_a).await.unwrap();

    tokio::join!(
        local_lww_register_set(&handle_a, key, b"online", 100),
        local_lww_register_set(&handle_b, key, b"away", 200),
    );

    wait_for_lww_register(&manager_a, key, b"away").await;
    wait_for_lww_register(&manager_b, key, b"away").await;
    assert_eq!(
        read_materialized_lww_register(&store_a, key),
        b"away".to_vec()
    );
    assert_eq!(
        read_materialized_lww_register(&store_b, key),
        b"away".to_vec()
    );

    let state_a = read_durable_lww_register_state(&store_a, key);
    let state_b = read_durable_lww_register_state(&store_b, key);
    assert_eq!(state_a.value(), b"away");
    assert_eq!(state_b.value(), b"away");
    assert_eq!(state_a.timestamp_ms(), 200);
    assert_eq!(state_b.timestamp_ms(), 200);
    assert_eq!(state_a.writer(), handle_b.node_id());
    assert_eq!(state_b.writer(), handle_b.node_id());
}
