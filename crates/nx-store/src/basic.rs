use nx_store::Store;

#[test]
fn roundtrip_get_set_delete() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();

    store.set(b"key", b"value").unwrap();
    assert_eq!(store.get(b"key").unwrap(), Some(b"value".to_vec()));

    store.delete(b"key").unwrap();
    assert_eq!(store.get(b"key").unwrap(), None);
}

#[test]
fn scan_prefix_works() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();

    store.set(b"cart:1", b"a").unwrap();
    store.set(b"cart:2", b"b").unwrap();
    store.set(b"user:1", b"x").unwrap();

    let items = store.scan_prefix(b"cart:").unwrap();
    let keys: Vec<_> = items.into_iter().map(|(k, _)| k).collect();

    assert!(keys.contains(&b"cart:1".to_vec()));
    assert!(keys.contains(&b"cart:2".to_vec()));
    assert!(!keys.contains(&b"user:1".to_vec()));
}
