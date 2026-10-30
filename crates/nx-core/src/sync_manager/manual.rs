use nx_store::Store;

use crate::control::ControlError;

use super::canonicalize_endpoint;

const PREFIX: &[u8] = b"__nx/discovery/manual/";
const SCHEMA: &[u8] = b"__nx/discovery/manual/schema";
const CLUSTER: &[u8] = b"__nx/discovery/manual/cluster";
const PEERS: &[u8] = b"__nx/discovery/manual/peer/";
const HEADER: &[u8] = b"NXMP\0\x01\0\0";

pub(super) fn load(store: &Store, cluster: &str, capacity: usize) -> anyhow::Result<Vec<String>> {
    let lease = store.acquire_write_lease()?;
    let mut after = None;
    let mut peers = Vec::new();
    let mut header = false;
    let mut scope = false;
    let mut count = 0usize;
    loop {
        let page = store.scan_prefix_page_after_bounded(PREFIX, after.as_deref(), 64, 32 * 1024)?;
        if page.is_empty() {
            break;
        }
        for (key, value) in page {
            count = count
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("manual peer count overflow"))?;
            anyhow::ensure!(
                key.len() + value.len() <= 512,
                "oversized manual peer record"
            );
            if key == SCHEMA {
                anyhow::ensure!(value == HEADER, "unsupported manual peer schema");
                header = true;
            } else if key == CLUSTER {
                anyhow::ensure!(value == cluster.as_bytes(), "manual peer cluster mismatch");
                scope = true;
            } else if let Some(endpoint) = key.strip_prefix(PEERS) {
                let endpoint = std::str::from_utf8(endpoint)?;
                anyhow::ensure!(
                    value.is_empty()
                        && canonicalize_endpoint(endpoint)? == endpoint
                        && endpoint.len() <= 259,
                    "invalid manual peer record"
                );
                anyhow::ensure!(peers.len() < capacity, "manual peer capacity exceeded");
                peers.push(endpoint.to_owned());
            } else {
                anyhow::bail!("unknown manual peer record");
            }
            after = Some(key);
        }
    }
    if count > 0 {
        anyhow::ensure!(header && scope, "incomplete manual peer namespace");
        lease.flush()?;
    }
    Ok(peers)
}

pub(super) fn persist(store: &Store, cluster: &str, endpoint: &str) -> Result<(), ControlError> {
    let key = [PEERS, endpoint.as_bytes()].concat();
    let lease = store
        .acquire_write_lease()
        .map_err(|error| ControlError::Storage(error.to_string()))?;
    lease
        .apply_batch(
            &[(SCHEMA, HEADER), (CLUSTER, cluster.as_bytes()), (&key, b"")],
            &[],
        )
        .map_err(|error| ControlError::Storage(error.to_string()))?;
    lease
        .flush()
        .map_err(|error| ControlError::PeerAcceptanceUncertain(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_storage_roundtrip_scope_and_invalid_records() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        assert!(load(&store, "default", 2).unwrap().is_empty());
        persist(&store, "default", "example.com:9000").unwrap();
        persist(&store, "default", "example.com:9000").unwrap();
        assert_eq!(load(&store, "default", 1).unwrap(), ["example.com:9000"]);
        assert!(load(&store, "other", 2).is_err());
        assert!(load(&store, "default", 0).is_err());
        store.set(SCHEMA, b"future").unwrap();
        assert!(load(&store, "default", 2).is_err());
    }

    #[test]
    fn manual_storage_never_confirms_failed_write_or_flush() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store.inject_disk_full_on_writes(true);
        assert!(matches!(
            persist(&store, "default", "a:1"),
            Err(ControlError::Storage(_))
        ));
        store.inject_disk_full_on_writes(false);
        assert!(load(&store, "default", 2).unwrap().is_empty());
        store.inject_disk_full_on_flush(true);
        assert!(matches!(
            persist(&store, "default", "a:1"),
            Err(ControlError::PeerAcceptanceUncertain(_))
        ));
        assert!(persist(&store, "default", "b:2").is_err());
        drop(store);
        let store = Store::open(dir.path()).unwrap();
        assert_eq!(load(&store, "default", 2).unwrap(), ["a:1"]);
    }

    #[test]
    fn manual_storage_crash_child() {
        let Ok(path) = std::env::var("NUMAX_MANUAL_CRASH_STORE") else {
            return;
        };
        let store = Store::open(path).unwrap();
        let checkpoint = match std::env::var("NUMAX_MANUAL_CRASH_POINT").unwrap().as_str() {
            "before_batch" => nx_store::CrashCheckpoint::BeforeBatch,
            "before_flush" => nx_store::CrashCheckpoint::BeforeFlush,
            "after_flush" => nx_store::CrashCheckpoint::AfterFlush,
            other => panic!("unknown manual crash checkpoint: {other}"),
        };
        store.inject_crash_checkpoint(
            checkpoint,
            std::env::var("NUMAX_MANUAL_CRASH_MARKER").unwrap().into(),
        );
        persist(&store, "default", "example.com:9000").unwrap();
        panic!("manual persistence passed armed crash checkpoint");
    }

    #[test]
    fn manual_storage_process_crash_preserves_complete_records() {
        for checkpoint in ["before_batch", "before_flush", "after_flush"] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("store");
            let marker = directory.path().join("checkpoint");
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "sync_manager::manual::tests::manual_storage_crash_child",
                    "--nocapture",
                ])
                .env("NUMAX_MANUAL_CRASH_STORE", &path)
                .env("NUMAX_MANUAL_CRASH_POINT", checkpoint)
                .env("NUMAX_MANUAL_CRASH_MARKER", &marker)
                .spawn()
                .unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while !marker.exists() {
                if let Some(status) = child.try_wait().unwrap() {
                    panic!("manual crash child exited before {checkpoint}: {status}");
                }
                if std::time::Instant::now() >= deadline {
                    child.kill().unwrap();
                    child.wait().unwrap();
                    panic!("manual crash child did not reach {checkpoint}");
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            child.kill().unwrap();
            child.wait().unwrap();
            let store = Store::open(path).unwrap();
            let peers = load(&store, "default", 1).unwrap();
            match checkpoint {
                "before_batch" => assert!(peers.is_empty()),
                "after_flush" => assert_eq!(peers, ["example.com:9000"]),
                _ => assert!(peers.is_empty() || peers == ["example.com:9000"]),
            }
        }
    }
}
