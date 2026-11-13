use super::super::support::*;

#[tokio::test]
async fn manual_orset_recovers_history_and_new_writes() {
    for format in [
        nx_net::SerializationFormat::Json,
        nx_net::SerializationFormat::Bincode,
    ] {
        let (mut a, ha, sa, mut b, hb, sb, endpoint) = manual_pair(format).await;
        local_orset_add(&ha, "manual-o", "value").await;
        hb.add_peer(endpoint).await.unwrap();
        wait_for_orset(&b, "manual-o", &["value"]).await;
        local_orset_remove(&hb, "manual-o", "value").await;
        wait_for_orset(&a, "manual-o", &[]).await;
        assert_eq!(
            read_materialized_orset(&sa, "manual-o"),
            read_materialized_orset(&sb, "manual-o")
        );
        assert_eq!(
            read_durable_orset_state(&sa, "manual-o"),
            read_durable_orset_state(&sb, "manual-o")
        );
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn e2e_two_nodes_orset_adds_and_observed_remove_converge() {
    let key = "tags:item-1";
    let addr_a = free_addr();
    let addr_b = free_addr();
    let (manager_a, handle_a, store_a) = started_manager(addr_a.clone()).await;
    let (manager_b, handle_b, store_b) = started_manager(addr_b.clone()).await;

    manager_a.connect_to_peer(&addr_b).await.unwrap();
    manager_b.connect_to_peer(&addr_a).await.unwrap();

    tokio::join!(
        local_orset_add(&handle_a, key, "blue"),
        local_orset_add(&handle_b, key, "red"),
    );

    wait_for_orset(&manager_a, key, &["blue", "red"]).await;
    wait_for_orset(&manager_b, key, &["blue", "red"]).await;

    local_orset_remove(&handle_a, key, "blue").await;

    wait_for_orset(&manager_a, key, &["red"]).await;
    wait_for_orset(&manager_b, key, &["red"]).await;
    assert_eq!(
        read_materialized_orset(&store_a, key),
        vec!["red".to_string()]
    );
    assert_eq!(
        read_materialized_orset(&store_b, key),
        vec!["red".to_string()]
    );

    let state_a = read_durable_orset_state(&store_a, key);
    let state_b = read_durable_orset_state(&store_b, key);
    assert!(!state_a.contains("blue"));
    assert!(!state_b.contains("blue"));
    assert!(state_a.contains("red"));
    assert!(state_b.contains("red"));
}
