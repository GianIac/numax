use super::super::support::*;

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
