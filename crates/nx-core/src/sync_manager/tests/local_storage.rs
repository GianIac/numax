use super::*;
use std::future::Future;

async fn mutate(handle: &SyncHandle, family: usize) -> anyhow::Result<AcceptedLocalOp> {
    match family {
        0 => handle.increment_gcounter("local-storage", 1).await,
        1 => handle.increment_pncounter("local-storage", 1).await,
        2 => handle.set_lww_register("local-storage", vec![1], 1).await,
        3 => {
            handle
                .set_lww_map("local-storage", "field", vec![1], 1)
                .await
        }
        4 => handle.add_orset("local-storage", "element").await,
        5 => {
            let op = handle.rga_insert_op("local-storage", None, vec![1]);
            handle.insert_rga(op).await
        }
        _ => unreachable!(),
    }
}

async fn assert_value(manager: &SyncManager, family: usize) {
    let key = "local-storage";
    let store = &manager.store;
    match family {
        0 => {
            assert_eq!(manager.get_counter_value(key).await, 1);
            assert_eq!(read_materialized(store, key), 1);
            assert_eq!(read_durable_gcounter_state(store, key).value(), 1);
        }
        1 => {
            assert_eq!(manager.get_pncounter_value(key).await, 1);
            assert_eq!(read_materialized_pncounter(store, key), 1);
            assert_eq!(read_durable_pncounter_state(store, key).value(), 1);
        }
        2 => {
            assert_eq!(manager.get_lww_register_value(key).await, Some(vec![1]));
            assert_eq!(read_materialized_lww_register(store, key), vec![1]);
            assert_eq!(
                read_durable_lww_register_state(store, key).value_bytes(),
                vec![1]
            );
        }
        3 => {
            let expected = vec![("field".to_string(), vec![1])];
            assert_eq!(manager.get_lww_map_entries(key).await, expected);
            assert_eq!(read_materialized_lww_map(store, key), expected);
            assert_eq!(read_durable_lww_map_state(store, key).entries(), expected);
        }
        4 => {
            assert_eq!(manager.get_orset_elements(key).await, vec!["element"]);
            assert_eq!(read_materialized_orset(store, key), vec!["element"]);
            assert_eq!(
                read_durable_orset_state(store, key).elements(),
                vec!["element"]
            );
        }
        5 => {
            assert_eq!(manager.get_rga_values(key).await, vec![vec![1]]);
            assert_eq!(read_materialized_rga(store, key), vec![vec![1]]);
            assert_eq!(read_durable_rga_state(store, key).values(), vec![vec![1]]);
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn local_storage_wait_keeps_timers_responsive_for_every_family() {
    check_storage_wait(false).await;
}

#[tokio::test]
async fn local_flush_keeps_timers_responsive_and_survives_cancellation_for_every_family() {
    check_storage_wait(true).await;
}

#[tokio::test]
async fn local_cancellation_before_dispatch_writes_nothing_for_every_family() {
    let mut manager = SyncManager::new(
        NodeId::new("local-cancel"),
        SyncConfig::new(),
        temp_store(),
        metrics(),
    );
    let handle = manager.handle();
    let flushes_before = manager.store.completed_flushes();
    for family in 0..6 {
        let seen = manager.seen_ops.write().await;
        {
            let mut work = std::pin::pin!(mutate(&handle, family));
            let pending = std::future::poll_fn(|cx| {
                std::task::Poll::Ready(work.as_mut().poll(cx).is_pending())
            })
            .await;
            assert!(pending);
        }
        drop(seen);
        assert!(manager.op_log.read().await.is_empty());
        assert_eq!(manager.store.completed_flushes(), flushes_before);
    }
    manager.shutdown().await.unwrap();
    for family in 0..6 {
        assert!(mutate(&handle, family).await.is_err());
    }
    assert!(manager.op_log.read().await.is_empty());
}

async fn check_storage_wait(pause_flush: bool) {
    for family in 0..6 {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(NxStore::open(directory.path()).unwrap());
        let mut manager = SyncManager::new(
            NodeId::new("local-storage"),
            SyncConfig::new(),
            store.clone(),
            metrics(),
        );
        let handle = manager.handle();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (progress_tx, progress_rx) = std::sync::mpsc::channel();
        let held_store = store.clone();
        let holder = std::thread::spawn(move || {
            if pause_flush {
                let (reached_tx, reached_rx) = std::sync::mpsc::channel();
                let (resume_tx, resume_rx) = std::sync::mpsc::channel();
                held_store.pause_next_flush(reached_tx, resume_rx);
                ready_tx.send(()).unwrap();
                reached_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                let progressed = progress_rx.recv_timeout(Duration::from_millis(250)).is_ok();
                resume_tx.send(()).unwrap();
                progressed
            } else {
                let lease = held_store.acquire_write_lease().unwrap();
                ready_tx.send(()).unwrap();
                entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                let progressed = progress_rx.recv_timeout(Duration::from_millis(250)).is_ok();
                drop(lease);
                progressed
            }
        });
        ready_rx.await.unwrap();
        let worker_handle = handle.clone();
        let (polled_tx, polled_rx) = tokio::sync::oneshot::channel();
        let processing = tokio::spawn(async move {
            let mut work = std::pin::pin!(mutate(&worker_handle, family));
            let mut polled_tx = Some(polled_tx);
            entered_tx.send(()).unwrap_or(());
            std::future::poll_fn(|cx| {
                let result = work.as_mut().poll(cx);
                if let Some(sender) = polled_tx.take() {
                    sender.send(result.is_pending()).unwrap();
                }
                result
            })
            .await
        });
        let pending = polled_rx.await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        processing.abort();
        let cancelled = processing.await.is_err_and(|error| error.is_cancelled());
        let shutdown_pending = tokio::time::timeout(Duration::from_millis(20), manager.shutdown())
            .await
            .is_err();
        let _ = progress_tx.send(());
        let progressed = holder.join().unwrap();
        tokio::time::timeout(Duration::from_secs(5), manager.shutdown())
            .await
            .unwrap()
            .unwrap();
        let log = manager.op_log.read().await.clone();
        let published = log.len();
        if published == 1 {
            assert_value(&manager, family).await;
            assert!(manager.seen_ops.read().await.contains(log[0].id.as_str()));
            assert!(
                store
                    .get(&op_log_store_key(log[0].id.as_str()))
                    .unwrap()
                    .is_some()
            );
            assert_eq!(
                handle.replication_progress().unwrap()[0].contiguous_sequence,
                1
            );
        }
        drop(handle);
        drop(manager);
        drop(store);
        wait_for_store_file_release(directory.path()).await.unwrap();
        let reopened = SyncManager::new(
            NodeId::new("local-storage"),
            SyncConfig::new(),
            Arc::new(NxStore::open(directory.path()).unwrap()),
            metrics(),
        );
        assert_eq!(reopened.op_log.read().await.len(), 1);
        assert_value(&reopened, family).await;
        assert!(
            progressed,
            "async timer stalled: family {family}, flush={pause_flush}"
        );
        assert!(
            pending,
            "storage work returned before it could be cancelled"
        );
        assert!(cancelled, "caller did not cancel while storage was pending");
        assert!(
            shutdown_pending,
            "shutdown did not account for dispatched local work"
        );
        assert_eq!(published, 1, "shutdown returned before local publication");
    }
}
