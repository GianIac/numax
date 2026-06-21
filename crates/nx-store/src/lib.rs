mod error;
mod store;

pub use error::StoreError;
pub use store::{Store, StoreStats, StoreWriteLease};

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
    fn test_exists() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        assert!(!store.exists(b"key1").unwrap());

        store.set(b"key1", b"value1").unwrap();
        assert!(store.exists(b"key1").unwrap());

        store.delete(b"key1").unwrap();
        assert!(!store.exists(b"key1").unwrap());
    }

    #[test]
    fn test_prefix_exists_reads_at_most_the_first_matching_record() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.set(b"app:a", b"1").unwrap();
        store.set(b"app:b", b"2").unwrap();

        assert!(store.prefix_exists(b"app:").unwrap());
        assert!(!store.prefix_exists(b"missing:").unwrap());
    }

    #[test]
    fn test_write_lease_blocks_mutations_from_store_clones() {
        use std::sync::mpsc;
        use std::time::Duration;

        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let writer = store.clone();
        let lease = store.acquire_write_lease().unwrap();
        let (started_sender, started_receiver) = mpsc::channel();
        let (completed_sender, completed_receiver) = mpsc::channel();

        let writer_thread = std::thread::spawn(move || {
            started_sender.send(()).unwrap();
            writer.set(b"concurrent", b"value").unwrap();
            completed_sender.send(()).unwrap();
        });

        started_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        assert!(
            completed_receiver
                .recv_timeout(Duration::from_millis(50))
                .is_err()
        );
        lease.set(b"migration", b"value").unwrap();
        drop(lease);
        completed_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        writer_thread.join().unwrap();

        assert_eq!(store.get(b"migration").unwrap(), Some(b"value".to_vec()));
        assert_eq!(store.get(b"concurrent").unwrap(), Some(b"value".to_vec()));
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

    #[test]
    fn test_apply_batch_sets_and_deletes_atomically() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        store.set(b"old", b"value").unwrap();
        store
            .apply_batch(
                &[(b"new".as_slice(), b"value".as_slice())],
                &[b"old".as_slice()],
            )
            .unwrap();

        assert_eq!(store.get(b"new").unwrap(), Some(b"value".to_vec()));
        assert_eq!(store.get(b"old").unwrap(), None);
    }

    #[test]
    fn test_scan_prefix_page_paginates_visible_keys() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        store.set(b"app:a", b"1").unwrap();
        store.set(b"app:b", b"2").unwrap();
        store.set(b"app:c", b"3").unwrap();
        store.set(b"other:a", b"4").unwrap();

        let first = store.scan_prefix_page(b"app:", 0, 2, None).unwrap();
        assert_eq!(
            first,
            vec![
                (b"app:a".to_vec(), b"1".to_vec()),
                (b"app:b".to_vec(), b"2".to_vec())
            ]
        );

        let second = store.scan_prefix_page(b"app:", 2, 2, None).unwrap();
        assert_eq!(second, vec![(b"app:c".to_vec(), b"3".to_vec())]);
    }

    #[test]
    fn test_scan_prefix_page_excludes_reserved_prefix() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        store.set(b"app:a", b"1").unwrap();
        store.set(b"__nx/internal", b"secret").unwrap();

        let rows = store.scan_prefix_page(b"", 0, 10, Some(b"__nx/")).unwrap();
        assert_eq!(rows, vec![(b"app:a".to_vec(), b"1".to_vec())]);
    }

    #[test]
    fn test_scan_prefix_page_after_uses_key_cursor() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        store.set(b"app:a", b"1").unwrap();
        store.set(b"app:b", b"2").unwrap();
        store.set(b"app:c", b"3").unwrap();
        store.set(b"other:a", b"4").unwrap();

        let first = store
            .scan_prefix_page_after(b"app:", None, 2, None)
            .unwrap();
        assert_eq!(
            first,
            vec![
                (b"app:a".to_vec(), b"1".to_vec()),
                (b"app:b".to_vec(), b"2".to_vec())
            ]
        );

        let second = store
            .scan_prefix_page_after(b"app:", Some(b"app:b"), 2, None)
            .unwrap();
        assert_eq!(second, vec![(b"app:c".to_vec(), b"3".to_vec())]);
    }

    #[test]
    fn test_scan_prefix_page_after_does_not_shift_when_key_is_inserted_before_cursor() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        store.set(b"app:b", b"2").unwrap();
        store.set(b"app:c", b"3").unwrap();

        let first = store
            .scan_prefix_page_after(b"app:", None, 1, None)
            .unwrap();
        assert_eq!(first, vec![(b"app:b".to_vec(), b"2".to_vec())]);

        store.set(b"app:a", b"1").unwrap();

        let second = store
            .scan_prefix_page_after(b"app:", Some(b"app:b"), 10, None)
            .unwrap();
        assert_eq!(second, vec![(b"app:c".to_vec(), b"3".to_vec())]);
    }

    #[test]
    fn test_scan_prefix_page_after_bounded_respects_record_and_byte_limits() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.set(b"app:a", b"1111").unwrap();
        store.set(b"app:b", b"2222").unwrap();
        store.set(b"app:c", b"3333").unwrap();

        let rows = store
            .scan_prefix_page_after_bounded(b"app:", None, 10, 18)
            .unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, b"app:a");
        assert_eq!(rows[1].0, b"app:b");
    }

    #[test]
    fn test_scan_prefix_page_after_bounded_rejects_oversized_record() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.set(b"app:large", b"0123456789").unwrap();

        assert!(matches!(
            store.scan_prefix_page_after_bounded(b"app:", None, 10, 8),
            Err(StoreError::ScanBatchByteLimitExceeded { .. })
        ));
    }

    #[test]
    fn test_keys_prefix_page_paginates_visible_keys() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        store.set(b"app:a", b"1").unwrap();
        store.set(b"app:b", b"2").unwrap();
        store.set(b"app:c", b"3").unwrap();
        store.set(b"other:a", b"4").unwrap();

        let first = store.keys_prefix_page(b"app:", 0, 2, None).unwrap();
        assert_eq!(first, vec![b"app:a".to_vec(), b"app:b".to_vec()]);

        let second = store.keys_prefix_page(b"app:", 2, 2, None).unwrap();
        assert_eq!(second, vec![b"app:c".to_vec()]);
    }

    #[test]
    fn test_keys_prefix_page_excludes_reserved_prefix() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        store.set(b"app:a", b"1").unwrap();
        store.set(b"__nx/internal", b"secret").unwrap();

        let keys = store.keys_prefix_page(b"", 0, 10, Some(b"__nx/")).unwrap();
        assert_eq!(keys, vec![b"app:a".to_vec()]);
    }

    #[test]
    fn test_keys_prefix_page_after_uses_key_cursor() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        store.set(b"app:a", b"1").unwrap();
        store.set(b"app:b", b"2").unwrap();
        store.set(b"app:c", b"3").unwrap();
        store.set(b"other:a", b"4").unwrap();

        let first = store
            .keys_prefix_page_after(b"app:", None, 2, None)
            .unwrap();
        assert_eq!(first, vec![b"app:a".to_vec(), b"app:b".to_vec()]);

        let second = store
            .keys_prefix_page_after(b"app:", Some(b"app:b"), 2, None)
            .unwrap();
        assert_eq!(second, vec![b"app:c".to_vec()]);
    }

    #[test]
    fn test_stats_counts_keys_and_bytes() {
        let dir = tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();

        store.set(b"a", b"one").unwrap();
        store.set(b"bb", b"two").unwrap();

        let stats = store.stats().unwrap();

        assert_eq!(stats.keys, 2);
        assert_eq!(stats.bytes, 9);
    }
}
