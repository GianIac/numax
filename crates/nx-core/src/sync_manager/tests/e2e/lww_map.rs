use super::super::support::*;

#[tokio::test]
async fn manual_map_recovers_history_and_new_writes() {
    for format in [
        nx_net::SerializationFormat::Json,
        nx_net::SerializationFormat::Bincode,
    ] {
        let (mut a, ha, sa, mut b, hb, sb, endpoint) = manual_pair(format).await;
        local_lww_map_set(&ha, "manual-m", "field", b"before", 100).await;
        hb.add_peer(endpoint).await.unwrap();
        wait_for_lww_map(&b, "manual-m", &[("field", b"before")]).await;
        local_lww_map_remove(&hb, "manual-m", "field", 200).await;
        wait_for_lww_map(&a, "manual-m", &[]).await;
        assert_eq!(
            read_materialized_lww_map(&sa, "manual-m"),
            read_materialized_lww_map(&sb, "manual-m")
        );
        assert_eq!(
            read_durable_lww_map_state(&sa, "manual-m"),
            read_durable_lww_map_state(&sb, "manual-m")
        );
        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn e2e_two_nodes_lww_map_sets_and_remove_converge() {
    let key = "settings:service-a";
    let addr_a = free_addr();
    let addr_b = free_addr();
    let (manager_a, handle_a, store_a) = started_manager(addr_a.clone()).await;
    let (manager_b, handle_b, store_b) = started_manager(addr_b.clone()).await;

    manager_a.connect_to_peer(&addr_b).await.unwrap();
    manager_b.connect_to_peer(&addr_a).await.unwrap();

    tokio::join!(
        local_lww_map_set(&handle_a, key, "theme", b"dark", 100),
        local_lww_map_set(&handle_b, key, "region", b"eu", 100),
    );

    wait_for_lww_map(&manager_a, key, &[("region", b"eu"), ("theme", b"dark")]).await;
    wait_for_lww_map(&manager_b, key, &[("region", b"eu"), ("theme", b"dark")]).await;

    tokio::join!(
        local_lww_map_set(&handle_a, key, "theme", b"light", 200),
        local_lww_map_remove(&handle_b, key, "region", 300),
    );

    wait_for_lww_map(&manager_a, key, &[("theme", b"light")]).await;
    wait_for_lww_map(&manager_b, key, &[("theme", b"light")]).await;
    assert_eq!(
        read_materialized_lww_map(&store_a, key),
        vec![("theme".to_string(), b"light".to_vec())]
    );
    assert_eq!(
        read_materialized_lww_map(&store_b, key),
        vec![("theme".to_string(), b"light".to_vec())]
    );

    let state_a = read_durable_lww_map_state(&store_a, key);
    let state_b = read_durable_lww_map_state(&store_b, key);
    assert_eq!(state_a.get_bytes("theme"), Some(b"light".to_vec()));
    assert_eq!(state_b.get_bytes("theme"), Some(b"light".to_vec()));
    assert!(!state_a.entry("region").unwrap().is_visible());
    assert!(!state_b.entry("region").unwrap().is_visible());
}
