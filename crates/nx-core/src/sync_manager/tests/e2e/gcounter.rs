use super::super::support::*;

#[tokio::test]
async fn e2e_two_nodes_push_ops_materializes_on_peer() {
    let key = "visits";
    let addr_a = free_addr();
    let addr_b = free_addr();
    let (manager_a, handle_a, _store_a) = started_manager(addr_a).await;
    let (manager_b, _handle_b, store_b) = started_manager(addr_b.clone()).await;

    manager_a.connect_to_peer(&addr_b).await.unwrap();
    local_increment(&handle_a, key, 1).await;

    wait_for_counter(&manager_b, key, 1).await;
    assert_eq!(read_materialized(&store_b, key), 1);
}

#[tokio::test]
async fn e2e_two_nodes_parallel_increments_converge() {
    let key = "visits";
    let addr_a = free_addr();
    let addr_b = free_addr();
    let (manager_a, handle_a, store_a) = started_manager(addr_a.clone()).await;
    let (manager_b, handle_b, store_b) = started_manager(addr_b.clone()).await;

    manager_a.connect_to_peer(&addr_b).await.unwrap();
    manager_b.connect_to_peer(&addr_a).await.unwrap();

    tokio::join!(
        local_increment(&handle_a, key, 1),
        local_increment(&handle_b, key, 1),
    );

    wait_for_counter(&manager_a, key, 2).await;
    wait_for_counter(&manager_b, key, 2).await;
    assert_eq!(read_materialized(&store_a, key), 2);
    assert_eq!(read_materialized(&store_b, key), 2);
}
