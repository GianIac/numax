mod error;
mod store;

pub use error::StoreError;
pub use store::Store;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_set_and_get() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        store.set(b"key1", b"value1").unwrap();
        let val = store.get(b"key1").unwrap();

        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_get_nonexistent() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        let val = store.get(b"nonexistent").unwrap();
        assert_eq!(val, None);
    }

    #[test]
    fn test_overwrite() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        store.set(b"key1", b"value1").unwrap();
        store.set(b"key1", b"value2").unwrap();

        let val = store.get(b"key1").unwrap();
        assert_eq!(val, Some(b"value2".to_vec()));
    }

    #[test]
    fn test_delete() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        store.set(b"key1", b"value1").unwrap();
        store.delete(b"key1").unwrap();

        let val = store.get(b"key1").unwrap();
        assert_eq!(val, None);
    }

    #[test]
    fn test_multiple_keys() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        store.set(b"key1", b"value1").unwrap();
        store.set(b"key2", b"value2").unwrap();
        store.set(b"key3", b"value3").unwrap();

        assert_eq!(store.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(store.get(b"key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(store.get(b"key3").unwrap(), Some(b"value3".to_vec()));
    }
}
