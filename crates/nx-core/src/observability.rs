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

const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct ObservabilityConfig {
    pub listen_addr: String,
}

impl ObservabilityConfig {
    pub fn new(listen_addr: impl Into<String>) -> Self {
        Self {
            listen_addr: listen_addr.into(),
        }
    }
}

#[derive(Default)]
pub struct RuntimeMetrics {
    ops_total: AtomicU64,
    peers_connected: AtomicUsize,
    sync_latency_ms: AtomicU64,
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
                                if let Err(e) = handle_connection(stream, metrics, store).await {
                                    debug!(error = %e, "observability request failed");
                                }
                            });
                        }
                        Err(e) => warn!(error = %e, "observability accept failed"),
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
) -> Result<()> {
    let mut buf = [0u8; 1024];
    let n = timeout(REQUEST_TIMEOUT, stream.read(&mut buf))
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
    timeout(REQUEST_TIMEOUT, stream.write_all(response.as_bytes()))
        .await
        .map_err(|_| anyhow!("observability response write timed out"))??;
    timeout(REQUEST_TIMEOUT, stream.flush())
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
         # HELP numax_store_keys Keys in the local store\n\
         # TYPE numax_store_keys gauge\n\
         numax_store_keys {}\n\
         # HELP numax_store_bytes Bytes used by local store keys and values\n\
         # TYPE numax_store_bytes gauge\n\
         numax_store_bytes {}\n",
        snapshot.ops_total,
        snapshot.peers_connected,
        snapshot.sync_latency_ms,
        snapshot.store_keys,
        snapshot.store_bytes
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_store() -> Arc<NxStore> {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        path.push(format!("numax-observability-test-{nanos}"));
        Arc::new(NxStore::open(path).unwrap())
    }

    #[test]
    fn metrics_are_rendered_in_prometheus_format() {
        let snapshot = MetricsSnapshot {
            ops_total: 7,
            peers_connected: 2,
            sync_latency_ms: 15,
            store_keys: 3,
            store_bytes: 42,
        };
        let rendered = render_metrics(snapshot);

        assert!(rendered.contains("numax_ops_total 7"));
        assert!(rendered.contains("numax_peers_connected 2"));
        assert!(rendered.contains("numax_sync_latency_ms 15"));
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
        assert!(
            request(addr, "/metrics")
                .await
                .contains("numax_ops_total 2")
        );

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
