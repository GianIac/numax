use super::super::support::*;

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
