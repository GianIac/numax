use super::super::support::*;

#[tokio::test]
async fn manual_rga_recovers_history_and_new_writes() {
    for format in [
        nx_net::SerializationFormat::Json,
        nx_net::SerializationFormat::Bincode,
    ] {
        let (mut a, ha, sa, mut b, hb, sb, endpoint) = manual_pair(format).await;
        let first = local_rga_insert_after(&ha, "manual-a", None, b"before").await;
        hb.add_peer(endpoint).await.unwrap();
        wait_for_rga(&b, "manual-a", &[b"before"]).await;
        local_rga_delete(&hb, "manual-a", &first).await;
        wait_for_rga(&a, "manual-a", &[]).await;
        assert_eq!(
            read_materialized_rga(&sa, "manual-a"),
            read_materialized_rga(&sb, "manual-a")
        );
        assert_eq!(
            read_durable_rga_state(&sa, "manual-a"),
            read_durable_rga_state(&sb, "manual-a")
        );
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn e2e_two_nodes_rga_insert_delete_converge() {
    let key = "comments:doc-1";
    let addr_a = free_addr();
    let addr_b = free_addr();
    let (manager_a, handle_a, store_a) = started_manager(addr_a.clone()).await;
    let (manager_b, handle_b, store_b) = started_manager(addr_b.clone()).await;

    manager_a.connect_to_peer(&addr_b).await.unwrap();
    manager_b.connect_to_peer(&addr_a).await.unwrap();

    let first_id = local_rga_insert_after(&handle_a, key, None, b"first").await;
    wait_for_rga(&manager_a, key, &[b"first"]).await;
    wait_for_rga(&manager_b, key, &[b"first"]).await;

    let second_id =
        local_rga_insert_after(&handle_b, key, Some(first_id.as_str()), b"second").await;
    wait_for_rga(&manager_a, key, &[b"first", b"second"]).await;
    wait_for_rga(&manager_b, key, &[b"first", b"second"]).await;

    local_rga_delete(&handle_a, key, &first_id).await;
    wait_for_rga(&manager_a, key, &[b"second"]).await;
    wait_for_rga(&manager_b, key, &[b"second"]).await;
    assert_eq!(
        read_materialized_rga(&store_a, key),
        vec![b"second".to_vec()]
    );
    assert_eq!(
        read_materialized_rga(&store_b, key),
        vec![b"second".to_vec()]
    );

    let state_a = read_durable_rga_state(&store_a, key);
    let state_b = read_durable_rga_state(&store_b, key);
    assert!(!state_a.contains(&first_id));
    assert!(!state_b.contains(&first_id));
    assert!(state_a.contains(&second_id));
    assert!(state_b.contains(&second_id));
}
