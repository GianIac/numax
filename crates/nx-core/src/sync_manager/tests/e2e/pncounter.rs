use super::super::support::*;

#[tokio::test]
async fn e2e_two_nodes_pncounter_inc_dec_converge() {
    let key = "inventory:sku-1";
    let addr_a = free_addr();
    let addr_b = free_addr();
    let (manager_a, handle_a, store_a) = started_manager(addr_a.clone()).await;
    let (manager_b, handle_b, store_b) = started_manager(addr_b.clone()).await;

    manager_a.connect_to_peer(&addr_b).await.unwrap();
    manager_b.connect_to_peer(&addr_a).await.unwrap();

    tokio::join!(
        local_pncounter_inc(&handle_a, key, 10),
        local_pncounter_dec(&handle_b, key, 4),
    );

    wait_for_pncounter(&manager_a, key, 6).await;
    wait_for_pncounter(&manager_b, key, 6).await;
    assert_eq!(read_materialized_pncounter(&store_a, key), 6);
    assert_eq!(read_materialized_pncounter(&store_b, key), 6);
}
