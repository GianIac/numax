use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Result, anyhow};
use nx_store::Store as NxStore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{debug, warn};

pub const DEFAULT_OBSERVABILITY_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct ObservabilityConfig {
    pub listen_addr: String,
    pub request_timeout: Duration,
}

impl ObservabilityConfig {
    pub fn new(listen_addr: impl Into<String>) -> Self {
        Self {
            listen_addr: listen_addr.into(),
            request_timeout: DEFAULT_OBSERVABILITY_REQUEST_TIMEOUT,
        }
    }

    pub fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = request_timeout;
        self
    }
}

#[derive(Default)]
pub struct RuntimeMetrics {
    ops_total: AtomicU64,
    peers_connected: AtomicUsize,
    sync_latency_ms: AtomicU64,
    sync_errors_total: AtomicU64,
    observability_requests_total: AtomicU64,
    observability_errors_total: AtomicU64,
    peer_connects_total: AtomicU64,
    peer_disconnects_total: AtomicU64,
    broadcast_batches_total: AtomicU64,
    broadcast_ops_total: AtomicU64,
    ready: AtomicBool,
}

impl RuntimeMetrics {
    pub fn record_ops(&self, count: u64) {
        self.ops_total.fetch_add(count, Ordering::Relaxed);
    }

    pub fn set_peers_connected(&self, count: usize) {
        self.peers_connected.store(count, Ordering::Relaxed);
    }

    pub fn record_sync_latency(&self, duration: Duration) {
        self.sync_latency_ms
            .store(duration.as_millis() as u64, Ordering::Relaxed);
    }

    pub fn record_sync_error(&self) {
        self.sync_errors_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_observability_request(&self) {
        self.observability_requests_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_observability_error(&self) {
        self.observability_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_peer_connect(&self) {
        self.peer_connects_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_peer_disconnect(&self) {
        self.peer_disconnects_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_broadcast_batch(&self, ops: usize) {
        self.broadcast_batches_total.fetch_add(1, Ordering::Relaxed);
        self.broadcast_ops_total
            .fetch_add(ops as u64, Ordering::Relaxed);
    }

    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Relaxed);
    }

    fn snapshot(&self, store: &NxStore) -> MetricsSnapshot {
        let store_stats = match store.stats() {
            Ok(stats) => stats,
            Err(e) => {
                warn!(error = %e, "failed to read store stats");
                nx_store::StoreStats { keys: 0, bytes: 0 }
            }
        };

        MetricsSnapshot {
            ops_total: self.ops_total.load(Ordering::Relaxed),
            peers_connected: self.peers_connected.load(Ordering::Relaxed),
            sync_latency_ms: self.sync_latency_ms.load(Ordering::Relaxed),
            sync_errors_total: self.sync_errors_total.load(Ordering::Relaxed),
            observability_requests_total: self.observability_requests_total.load(Ordering::Relaxed),
            observability_errors_total: self.observability_errors_total.load(Ordering::Relaxed),
            peer_connects_total: self.peer_connects_total.load(Ordering::Relaxed),
            peer_disconnects_total: self.peer_disconnects_total.load(Ordering::Relaxed),
            broadcast_batches_total: self.broadcast_batches_total.load(Ordering::Relaxed),
            broadcast_ops_total: self.broadcast_ops_total.load(Ordering::Relaxed),
            store_keys: store_stats.keys,
            store_bytes: store_stats.bytes,
        }
    }

    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }
}

struct MetricsSnapshot {
    ops_total: u64,
    peers_connected: usize,
    sync_latency_ms: u64,
    sync_errors_total: u64,
    observability_requests_total: u64,
    observability_errors_total: u64,
    peer_connects_total: u64,
    peer_disconnects_total: u64,
    broadcast_batches_total: u64,
    broadcast_ops_total: u64,
    store_keys: u64,
    store_bytes: u64,
}

pub struct ObservabilityServer {
    shutdown_tx: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl ObservabilityServer {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        if let Err(e) = self.task.await {
            warn!(error = %e, "observability server failed during shutdown");
        }
    }
}

pub async fn start_server(
    config: ObservabilityConfig,
    metrics: Arc<RuntimeMetrics>,
    store: Arc<NxStore>,
) -> Result<(SocketAddr, ObservabilityServer)> {
    let listener = TcpListener::bind(&config.listen_addr).await?;
    let bound_addr = listener.local_addr()?;
    let request_timeout = config.request_timeout;
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        debug!("observability server shutdown requested");
                        break;
                    }
                }
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _addr)) => {
                            let metrics = Arc::clone(&metrics);
                            let store = Arc::clone(&store);
                            tokio::spawn(async move {
                                let metrics_for_error = Arc::clone(&metrics);
                                if let Err(e) = handle_connection(stream, metrics, store, request_timeout).await {
                                    metrics_for_error.record_observability_error();
                                    debug!(error = %e, "observability request failed");
                                }
                            });
                        }
                        Err(e) => {
                            metrics.record_observability_error();
                            warn!(error = %e, "observability accept failed");
                        }
                    }
                }
            }
        }
    });

    Ok((bound_addr, ObservabilityServer { shutdown_tx, task }))
}

async fn handle_connection(
    mut stream: TcpStream,
    metrics: Arc<RuntimeMetrics>,
    store: Arc<NxStore>,
    request_timeout: Duration,
) -> Result<()> {
    metrics.record_observability_request();

    let mut buf = [0u8; 1024];
    let n = timeout(request_timeout, stream.read(&mut buf))
        .await
        .map_err(|_| anyhow!("observability request timed out"))??;
    let request =
        std::str::from_utf8(&buf[..n]).map_err(|e| anyhow!("invalid HTTP request: {e}"))?;
    let path = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");

    let (status, content_type, body) = match path {
        "/health" => ("200 OK", "text/plain; charset=utf-8", "ok\n".to_string()),
        "/ready" if metrics.is_ready() => {
            ("200 OK", "text/plain; charset=utf-8", "ready\n".to_string())
        }
        "/ready" => (
            "503 Service Unavailable",
            "text/plain; charset=utf-8",
            "not ready\n".to_string(),
        ),
        "/metrics" => (
            "200 OK",
            "text/plain; version=0.0.4; charset=utf-8",
            render_metrics(metrics.snapshot(&store)),
        ),
        _ => (
            "404 Not Found",
            "text/plain; charset=utf-8",
            "not found\n".to_string(),
        ),
    };

    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    timeout(request_timeout, stream.write_all(response.as_bytes()))
        .await
        .map_err(|_| anyhow!("observability response write timed out"))??;
    timeout(request_timeout, stream.flush())
        .await
        .map_err(|_| anyhow!("observability response flush timed out"))??;
    Ok(())
}

fn render_metrics(snapshot: MetricsSnapshot) -> String {
    format!(
        "# HELP numax_ops_total Operations processed\n\
         # TYPE numax_ops_total counter\n\
         numax_ops_total {}\n\
         # HELP numax_peers_connected Active peers\n\
         # TYPE numax_peers_connected gauge\n\
         numax_peers_connected {}\n\
         # HELP numax_sync_latency_ms Last sync latency in milliseconds\n\
         # TYPE numax_sync_latency_ms gauge\n\
         numax_sync_latency_ms {}\n\
         # HELP numax_sync_errors_total Sync errors\n\
         # TYPE numax_sync_errors_total counter\n\
         numax_sync_errors_total {}\n\
         # HELP numax_observability_requests_total Observability requests\n\
         # TYPE numax_observability_requests_total counter\n\
         numax_observability_requests_total {}\n\
         # HELP numax_observability_errors_total Observability request errors\n\
         # TYPE numax_observability_errors_total counter\n\
         numax_observability_errors_total {}\n\
         # HELP numax_peer_connects_total Peer connections observed\n\
         # TYPE numax_peer_connects_total counter\n\
         numax_peer_connects_total {}\n\
         # HELP numax_peer_disconnects_total Peer disconnections observed\n\
         # TYPE numax_peer_disconnects_total counter\n\
         numax_peer_disconnects_total {}\n\
         # HELP numax_broadcast_batches_total Broadcast batches sent\n\
         # TYPE numax_broadcast_batches_total counter\n\
         numax_broadcast_batches_total {}\n\
         # HELP numax_broadcast_ops_total Broadcast ops sent\n\
         # TYPE numax_broadcast_ops_total counter\n\
         numax_broadcast_ops_total {}\n\
         # HELP numax_store_keys Keys in the local store\n\
         # TYPE numax_store_keys gauge\n\
         numax_store_keys {}\n\
         # HELP numax_store_bytes Bytes used by local store keys and values\n\
         # TYPE numax_store_bytes gauge\n\
         numax_store_bytes {}\n",
        snapshot.ops_total,
        snapshot.peers_connected,
        snapshot.sync_latency_ms,
        snapshot.sync_errors_total,
        snapshot.observability_requests_total,
        snapshot.observability_errors_total,
        snapshot.peer_connects_total,
        snapshot.peer_disconnects_total,
        snapshot.broadcast_batches_total,
        snapshot.broadcast_ops_total,
        snapshot.store_keys,
        snapshot.store_bytes
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    static TEST_STORE_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_store() -> Arc<NxStore> {
        let mut path = std::env::temp_dir();
        let id = TEST_STORE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let process_id = std::process::id();
        path.push(format!("numax-observability-test-{process_id}-{id}"));
        Arc::new(NxStore::open(path).unwrap())
    }

    #[test]
    fn metrics_are_rendered_in_prometheus_format() {
        let snapshot = MetricsSnapshot {
            ops_total: 7,
            peers_connected: 2,
            sync_latency_ms: 15,
            sync_errors_total: 1,
            observability_requests_total: 9,
            observability_errors_total: 1,
            peer_connects_total: 3,
            peer_disconnects_total: 2,
            broadcast_batches_total: 4,
            broadcast_ops_total: 8,
            store_keys: 3,
            store_bytes: 42,
        };
        let rendered = render_metrics(snapshot);

        assert!(rendered.contains("numax_ops_total 7"));
        assert!(rendered.contains("numax_peers_connected 2"));
        assert!(rendered.contains("numax_sync_latency_ms 15"));
        assert!(rendered.contains("numax_sync_errors_total 1"));
        assert!(rendered.contains("numax_observability_requests_total 9"));
        assert!(rendered.contains("numax_observability_errors_total 1"));
        assert!(rendered.contains("numax_peer_connects_total 3"));
        assert!(rendered.contains("numax_peer_disconnects_total 2"));
        assert!(rendered.contains("numax_broadcast_batches_total 4"));
        assert!(rendered.contains("numax_broadcast_ops_total 8"));
        assert!(rendered.contains("numax_store_keys 3"));
        assert!(rendered.contains("numax_store_bytes 42"));
    }

    #[tokio::test]
    async fn observability_server_serves_health_ready_and_metrics() {
        let metrics = Arc::new(RuntimeMetrics::default());
        metrics.set_ready(true);
        metrics.record_ops(2);
        let store = temp_store();
        store.set(b"a", b"b").unwrap();
        let (addr, server) = start_server(
            ObservabilityConfig::new("127.0.0.1:0"),
            Arc::clone(&metrics),
            Arc::clone(&store),
        )
        .await
        .unwrap();

        assert!(request(addr, "/health").await.contains("ok"));
        assert!(request(addr, "/ready").await.contains("ready"));

        let metrics_response = request(addr, "/metrics").await;
        assert!(metrics_response.contains("HTTP/1.1 200 OK"));
        assert!(metrics_response.contains("text/plain; version=0.0.4"));
        assert!(metrics_response.contains("numax_ops_total 2"));
        assert!(metrics_response.contains("numax_observability_requests_total 3"));

        server.shutdown().await;
    }

    #[tokio::test]
    async fn ready_returns_503_before_runtime_is_ready() {
        let metrics = Arc::new(RuntimeMetrics::default());
        let store = temp_store();
        let (addr, server) = start_server(
            ObservabilityConfig::new("127.0.0.1:0"),
            Arc::clone(&metrics),
            Arc::clone(&store),
        )
        .await
        .unwrap();

        let response = request(addr, "/ready").await;

        assert!(response.contains("HTTP/1.1 503 Service Unavailable"));
        assert!(response.contains("not ready"));

        server.shutdown().await;
    }

    #[tokio::test]
    async fn unknown_path_returns_404() {
        let metrics = Arc::new(RuntimeMetrics::default());
        let store = temp_store();
        let (addr, server) = start_server(
            ObservabilityConfig::new("127.0.0.1:0"),
            Arc::clone(&metrics),
            Arc::clone(&store),
        )
        .await
        .unwrap();

        let response = request(addr, "/missing").await;

        assert!(response.contains("HTTP/1.1 404 Not Found"));
        assert!(response.contains("not found"));

        server.shutdown().await;
    }

    #[tokio::test]
    async fn idle_observability_connection_times_out() {
        let metrics = Arc::new(RuntimeMetrics::default());
        let store = temp_store();
        let config =
            ObservabilityConfig::new("127.0.0.1:0").with_request_timeout(Duration::from_millis(25));
        let (addr, server) = start_server(config, Arc::clone(&metrics), Arc::clone(&store))
            .await
            .unwrap();

        let _stream = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(75)).await;

        let metrics_response = request(addr, "/metrics").await;
        assert!(metrics_response.contains("numax_observability_errors_total 1"));

        server.shutdown().await;
    }

    async fn request(addr: SocketAddr, path: &str) -> String {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let request = format!("GET {path} HTTP/1.1\r\nhost: localhost\r\n\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        String::from_utf8(buf).unwrap()
    }
}
