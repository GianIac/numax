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
fn get_returns_none_when_missing() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();

    assert_eq!(store.get(b"missing").unwrap(), None);
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

#[test]
fn flush_works() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();

    store.set(b"key", b"value").unwrap();
    store.flush().unwrap();

    assert_eq!(store.get(b"key").unwrap(), Some(b"value".to_vec()));
}

#[test]
fn open_creates_dir_if_missing() {
    let tmp = tempfile::tempdir().unwrap();
    let db_dir = tmp.path().join("nx-store-data-does-not-exist-yet");

    assert!(!db_dir.exists());
    let _store = Store::open(&db_dir).unwrap();
    assert!(db_dir.exists());
    assert!(db_dir.is_dir());
}

#[test]
fn open_errors_if_path_is_file() {
    let tmp = tempfile::tempdir().unwrap();
    let file_path = tmp.path().join("not_a_dir");
    std::fs::write(&file_path, b"x").unwrap();

    let res = Store::open(&file_path);
    assert!(res.is_err());

    let msg = res.err().unwrap().to_string();
    assert!(msg.contains("not a directory"));
}
