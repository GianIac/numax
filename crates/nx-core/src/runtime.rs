use anyhow::{Result, anyhow};
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use wasmtime::{Engine, Linker, Module, Store};
use wasmtime_wasi::{WasiCtx, p1};

use nx_store::Store as NxStore;
use nx_sync::NodeId;

use crate::host_api;
use crate::observability::{
    ObservabilityConfig, ObservabilityServer, RuntimeMetrics, start_server,
};
use crate::sync_config::SyncConfig;
use crate::sync_manager::{SyncHandle, SyncManager};

pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
const NODE_ID_STORE_KEY: &[u8] = b"__nx/runtime/node_id";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownSignal {
    Interrupt,
    Terminate,
    Hangup,
}

// Runtime config
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// enable or not wasi support
    pub enable_wasi: bool,

    /// Maximum memory limit per module (not yet enforced)
    pub max_memory_bytes: Option<u64>,

    /// The path to the datastore.
    pub datastore_path: PathBuf,

    /// config sync (None = sync disabled).
    pub sync: Option<SyncConfig>,

    /// Observability HTTP endpoint (None = disabled).
    pub observability: Option<ObservabilityConfig>,

    /// Identifier exposed to the guest module through the System host API.
    pub module_id: String,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            enable_wasi: true,
            max_memory_bytes: None,
            datastore_path: PathBuf::from("./nx-data"),
            sync: None,
            observability: None,
            module_id: "unknown".to_string(),
        }
    }
}

/// Host status associated with the Store.
pub struct HostState {
    pub wasi: Option<p1::WasiP1Ctx>,
    pub store: Arc<NxStore>,
    pub sync_handle: Option<SyncHandle>,
    pub module_id: Arc<str>,
}

pub struct Runtime {
    engine: Engine,
    linker: Linker<HostState>,
    config: RuntimeConfig,
    store: Arc<NxStore>,
    metrics: Arc<RuntimeMetrics>,

    sync_manager: Option<SyncManager>,
    sync_handle: Option<SyncHandle>,
    observability_server: Option<ObservabilityServer>,
}

impl Runtime {
    pub fn new(config: RuntimeConfig) -> Result<Self> {
        // Engine: async support is required so wasmtime can yield across host calls
        let mut wasm_cfg = wasmtime::Config::new();
        wasm_cfg.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable);

        let engine = Engine::new(&wasm_cfg)?;
        let mut linker: Linker<HostState> = Linker::new(&engine);

        // host_log (namespace "nx", func "host_log")
        host_api::log::add_to_linker(&mut linker)?;

        // host_db (namespace "nx", func "db_get", "db_set" etc.)
        host_api::db::add_to_linker(&mut linker)?;

        // host_time (namespace "nx", func "time_now", "time_monotonic")
        host_api::time::add_to_linker(&mut linker)?;

        // host_crypto (namespace "nx", func "random_bytes", "hash_sha256", "hash_blake3")
        host_api::crypto::add_to_linker(&mut linker)?;

        // host_system (namespace "nx", func "env_get", "module_id", "abort")
        host_api::system::add_to_linker(&mut linker)?;

        // host_net (namespace "nx", func "net_node_id", "net_peers")
        host_api::net::add_to_linker(&mut linker)?;

        // host_crdt (namespace "nx", func "crdt_gcounter_inc", "crdt_gcounter_value")
        host_api::crdt::add_to_linker(&mut linker)?;

        // WASI base (preview1 / p1) — async variant, required by async engine.
        if config.enable_wasi {
            wasmtime_wasi::p1::add_to_linker_async(&mut linker, |state: &mut HostState| {
                state
                    .wasi
                    .as_mut()
                    .expect("WASI enabled but not initialized in the store")
            })?;
        }

        // open the datastore ONE TIME HERE
        let store = NxStore::open(&config.datastore_path)
            .map_err(|e| anyhow!("Failed to open datastore: {e}"))?;
        let store = Arc::new(store);
        let metrics = Arc::new(RuntimeMetrics::default());
        metrics.set_ready(config.sync.is_none());

        // Initialize SyncManager if configured, and derive its handle up-front so every HostState built afterwards sees the same op channel.
        let (sync_manager, sync_handle) = if let Some(ref sync_config) = config.sync {
            let node_id = load_or_create_node_id(&store)?;
            let manager = SyncManager::new(
                node_id,
                sync_config.clone(),
                Arc::clone(&store),
                Arc::clone(&metrics),
            );
            let handle = manager.handle();
            (Some(manager), Some(handle))
        } else {
            (None, None)
        };

        Ok(Self {
            engine,
            linker,
            config,
            store,
            metrics,
            sync_manager,
            sync_handle,
            observability_server: None,
        })
    }

    /// Start the optional observability HTTP endpoint.
    pub async fn start_observability(&mut self) -> Result<Option<std::net::SocketAddr>> {
        let Some(config) = self.config.observability.clone() else {
            return Ok(None);
        };
        if self.observability_server.is_some() {
            return Ok(None);
        }

        let (addr, server) =
            start_server(config, Arc::clone(&self.metrics), Arc::clone(&self.store)).await?;
        self.observability_server = Some(server);
        tracing::info!(addr = %addr, "observability endpoint started");
        Ok(Some(addr))
    }

    /// Start sync networking
    pub async fn start_sync(&mut self) -> Result<()> {
        let Some(manager) = self.sync_manager.as_mut() else {
            tracing::debug!("start_sync: no sync configured, skipping");
            return Ok(());
        };
        manager
            .start()
            .await
            .map_err(|e| anyhow!("sync manager failed to start: {e}"))?;
        self.metrics.set_ready(true);
        tracing::info!(
            node_id = %manager.node_id(),
            "sync manager started"
        );
        Ok(())
    }

    /// Stop sync background tasks and close network connections.
    pub async fn shutdown(&mut self) -> Result<()> {
        self.shutdown_with_timeout(DEFAULT_SHUTDOWN_TIMEOUT).await
    }

    /// Stop background tasks and flush the local store, bounded by `timeout`.
    pub async fn shutdown_with_timeout(&mut self, timeout: Duration) -> Result<()> {
        tokio::time::timeout(timeout, self.shutdown_inner())
            .await
            .map_err(|_| anyhow!("shutdown timed out after {:?}", timeout))?
    }

    async fn shutdown_inner(&mut self) -> Result<()> {
        self.metrics.set_ready(false);

        if let Some(manager) = self.sync_manager.as_mut() {
            manager
                .shutdown()
                .await
                .map_err(|e| anyhow!("sync manager failed to shut down: {e}"))?;
        } else {
            tracing::debug!("shutdown: no sync configured, skipping sync manager");
        }

        self.store
            .flush()
            .map_err(|e| anyhow!("failed to flush datastore: {e}"))?;

        if let Some(server) = self.observability_server.take() {
            server.shutdown().await;
        }

        tracing::info!("runtime shutdown complete");
        Ok(())
    }

    /// Return a clonable handle to the SyncManager (if present).
    pub fn sync_handle(&self) -> Option<SyncHandle> {
        self.sync_handle.clone()
    }

    /// Return true when this runtime owns an active sync manager.
    pub fn sync_enabled(&self) -> bool {
        self.sync_handle.is_some()
    }

    /// Return the current value of a GCounter, if sync is enabled.
    pub async fn get_counter_value(&self, key: &str) -> Option<u64> {
        let manager = self.sync_manager.as_ref()?;
        Some(manager.get_counter_value(key).await)
    }

    /// Return the current value of a PNCounter, if sync is enabled.
    pub async fn get_pncounter_value(&self, key: &str) -> Option<i64> {
        let manager = self.sync_manager.as_ref()?;
        Some(manager.get_pncounter_value(key).await)
    }

    /// Return the current value of an LWW-Register, if sync is enabled.
    pub async fn get_lww_register_value(&self, key: &str) -> Option<Option<Vec<u8>>> {
        let manager = self.sync_manager.as_ref()?;
        Some(manager.get_lww_register_value(key).await)
    }

    /// Return the visible elements of an ORSet, if sync is enabled.
    pub async fn get_orset_elements(&self, key: &str) -> Option<Vec<String>> {
        let manager = self.sync_manager.as_ref()?;
        Some(manager.get_orset_elements(key).await)
    }

    /// Return the visible entries of an LWW-Map, if sync is enabled.
    pub async fn get_lww_map_entries(&self, key: &str) -> Option<Vec<(String, Vec<u8>)>> {
        let manager = self.sync_manager.as_ref()?;
        Some(manager.get_lww_map_entries(key).await)
    }

    /// Return the visible values of an RGA, if sync is enabled.
    pub async fn get_rga_values(&self, key: &str) -> Option<Vec<Vec<u8>>> {
        let manager = self.sync_manager.as_ref()?;
        Some(manager.get_rga_values(key).await)
    }

    /// Keep the runtime alive while sync background tasks do their work.
    pub async fn serve(&self) -> Result<()> {
        let _ = self
            .serve_until_shutdown(wait_for_shutdown_signal())
            .await?;
        Ok(())
    }

    /// Testable variant of `serve`: the caller provides the shutdown signal.
    pub async fn serve_until_shutdown<S>(&self, shutdown: S) -> Result<Option<ShutdownSignal>>
    where
        S: Future<Output = ShutdownSignal>,
    {
        if !self.sync_enabled() {
            tracing::debug!("serve: sync disabled, nothing to keep alive");
            return Ok(None);
        }

        tracing::info!("runtime entering long-running sync mode");
        let signal = shutdown.await;
        tracing::info!(?signal, "runtime shutdown requested");

        Ok(Some(signal))
    }

    /// Keep sync alive for a bounded settle window, then return.
    pub async fn settle_for(&self, duration: Duration) -> Result<()> {
        if !self.sync_enabled() {
            tracing::debug!("settle_for: sync disabled, nothing to settle");
            return Ok(());
        }

        tracing::info!(?duration, "runtime entering sync settle mode");
        tokio::time::sleep(duration).await;
        tracing::info!("runtime settle complete");

        Ok(())
    }

    /// Keep sync alive before the guest runs, retrying configured peers during the window.
    pub async fn wait_before_run(&self, duration: Duration) -> Result<()> {
        if !self.sync_enabled() {
            tracing::debug!("wait_before_run: sync disabled, nothing to wait for");
            return Ok(());
        }

        tracing::info!(?duration, "runtime waiting before guest run");
        let deadline = Instant::now() + duration;

        loop {
            if let Some(manager) = self.sync_manager.as_ref() {
                manager.reconnect_configured_peers().await;
            }

            let now = Instant::now();
            if now >= deadline {
                break;
            }

            let remaining = deadline - now;
            tokio::time::sleep(remaining.min(Duration::from_millis(100))).await;
        }

        Ok(())
    }

    pub async fn run_module(&self, wasm_bytes: &[u8]) -> Result<()> {
        // handle shared to the DB
        let store_db = Arc::clone(&self.store);
        let module_id: Arc<str> = Arc::from(self.config.module_id.as_str());

        // Builds the host state for this run
        let host_state = if self.config.enable_wasi {
            let wasi = WasiCtx::builder().inherit_stdio().inherit_args().build_p1();

            HostState {
                wasi: Some(wasi),
                store: store_db,
                sync_handle: self.sync_handle.clone(),
                module_id,
            }
        } else {
            HostState {
                wasi: None,
                store: store_db,
                sync_handle: self.sync_handle.clone(),
                module_id,
            }
        };

        let mut store = Store::new(&self.engine, host_state);

        // Form completion / validation
        let module = Module::new(&self.engine, wasm_bytes)
            .map_err(|e| anyhow!("Invalid module (compile/validate): {e}"))?;

        // Instantiation
        let instance = self
            .linker
            .instantiate_async(&mut store, &module)
            .await
            .map_err(|e| anyhow!("Link error while instantiating module: {e}"))?;

        // Entrypoint: run / _start
        let run = instance
            .get_typed_func::<(), ()>(&mut store, "run")
            .or_else(|_| instance.get_typed_func::<(), ()>(&mut store, "_start"))
            .map_err(|e| anyhow!("No entrypoint found (expected `run` or `_start`): {e}"))?;

        // Execution
        run.call_async(&mut store, ())
            .await
            .map_err(|e| anyhow!("Error while executing module: {e}"))?;

        Ok(())
    }
}

fn load_or_create_node_id(store: &NxStore) -> Result<NodeId> {
    if let Some(bytes) = store.get(NODE_ID_STORE_KEY)? {
        let id = String::from_utf8(bytes)
            .map_err(|e| anyhow!("stored runtime node id is not valid UTF-8: {e}"))?;
        if id.is_empty() {
            return Err(anyhow!("stored runtime node id is empty"));
        }
        return Ok(NodeId::new(id));
    }

    let node_id = NodeId::generate();
    store.set(NODE_ID_STORE_KEY, node_id.as_str().as_bytes())?;
    store.flush()?;
    Ok(node_id)
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> ShutdownSignal {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigint = match signal(SignalKind::interrupt()) {
        Ok(signal) => signal,
        Err(e) => {
            tracing::warn!(error = %e, "failed to listen for SIGINT; falling back to ctrl_c");
            let _ = tokio::signal::ctrl_c().await;
            return ShutdownSignal::Interrupt;
        }
    };
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(signal) => signal,
        Err(e) => {
            tracing::warn!(error = %e, "failed to listen for SIGTERM; falling back to ctrl_c");
            let _ = tokio::signal::ctrl_c().await;
            return ShutdownSignal::Interrupt;
        }
    };
    let mut sighup = match signal(SignalKind::hangup()) {
        Ok(signal) => signal,
        Err(e) => {
            tracing::warn!(error = %e, "failed to listen for SIGHUP; falling back to ctrl_c");
            let _ = tokio::signal::ctrl_c().await;
            return ShutdownSignal::Interrupt;
        }
    };

    tokio::select! {
        _ = sigint.recv() => ShutdownSignal::Interrupt,
        _ = sigterm.recv() => ShutdownSignal::Terminate,
        _ = sighup.recv() => ShutdownSignal::Hangup,
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> ShutdownSignal {
    if let Err(e) = tokio::signal::ctrl_c().await {
        tracing::warn!(error = %e, "failed to listen for Ctrl+C");
    }

    ShutdownSignal::Interrupt
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync_config::SyncConfig;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::oneshot;
    use tokio::time::Instant;
    use tokio::time::{Duration, timeout};

    fn temp_datastore_path(prefix: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        path.push(format!("{prefix}-{nanos}"));
        path
    }

    #[tokio::test]
    async fn serve_returns_immediately_when_sync_is_disabled() {
        let config = RuntimeConfig {
            datastore_path: temp_datastore_path("numax-runtime-nosync-test"),
            ..RuntimeConfig::default()
        };
        let runtime = Runtime::new(config).unwrap();
        let (_tx, rx) = oneshot::channel::<()>();

        runtime
            .serve_until_shutdown(async {
                let _ = rx.await;
                ShutdownSignal::Interrupt
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn serve_keeps_runtime_alive_until_shutdown() {
        let config = RuntimeConfig {
            datastore_path: temp_datastore_path("numax-runtime-serve-test"),
            sync: Some(SyncConfig::new().with_listen_addr("127.0.0.1:0")),
            ..RuntimeConfig::default()
        };
        let runtime = Runtime::new(config).unwrap();
        let (_tx, rx) = oneshot::channel::<()>();

        assert!(
            timeout(
                Duration::from_millis(25),
                runtime.serve_until_shutdown(async {
                    let _ = rx.await;
                    ShutdownSignal::Interrupt
                })
            )
            .await
            .is_err()
        );

        let (tx, rx) = oneshot::channel::<()>();
        tx.send(()).unwrap();
        let signal = timeout(
            Duration::from_secs(1),
            runtime.serve_until_shutdown(async {
                let _ = rx.await;
                ShutdownSignal::Terminate
            }),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(signal, Some(ShutdownSignal::Terminate));
    }

    #[tokio::test]
    async fn serve_returns_none_when_sync_is_disabled() {
        let config = RuntimeConfig {
            datastore_path: temp_datastore_path("numax-runtime-nosync-signal-test"),
            ..RuntimeConfig::default()
        };
        let runtime = Runtime::new(config).unwrap();

        let signal = runtime
            .serve_until_shutdown(async { ShutdownSignal::Hangup })
            .await
            .unwrap();

        assert_eq!(signal, None);
    }

    #[tokio::test]
    async fn settle_returns_immediately_when_sync_is_disabled() {
        let config = RuntimeConfig {
            datastore_path: temp_datastore_path("numax-runtime-settle-nosync-test"),
            ..RuntimeConfig::default()
        };
        let runtime = Runtime::new(config).unwrap();
        let started = Instant::now();

        runtime.settle_for(Duration::from_secs(5)).await.unwrap();

        assert!(started.elapsed() < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn settle_waits_for_requested_duration_when_sync_is_enabled() {
        let config = RuntimeConfig {
            datastore_path: temp_datastore_path("numax-runtime-settle-test"),
            sync: Some(SyncConfig::new().with_listen_addr("127.0.0.1:0")),
            ..RuntimeConfig::default()
        };
        let runtime = Runtime::new(config).unwrap();
        let duration = Duration::from_millis(25);
        let started = Instant::now();

        runtime.settle_for(duration).await.unwrap();

        assert!(started.elapsed() >= duration);
    }

    #[tokio::test]
    async fn shutdown_with_timeout_flushes_store_without_sync() {
        let config = RuntimeConfig {
            datastore_path: temp_datastore_path("numax-runtime-shutdown-flush-test"),
            ..RuntimeConfig::default()
        };
        let mut runtime = Runtime::new(config).unwrap();

        runtime.store.set(b"key", b"value").unwrap();
        runtime
            .shutdown_with_timeout(Duration::from_secs(1))
            .await
            .unwrap();

        assert_eq!(runtime.store.get(b"key").unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn sync_runtime_reuses_persisted_node_id() {
        let datastore_path = temp_datastore_path("numax-runtime-node-id-test");
        let config = RuntimeConfig {
            datastore_path: datastore_path.clone(),
            sync: Some(SyncConfig::new().with_listen_addr("127.0.0.1:0")),
            ..RuntimeConfig::default()
        };

        let first = Runtime::new(config).unwrap();
        let first_node_id = first.sync_manager.as_ref().unwrap().node_id().clone();
        drop(first);

        let second = Runtime::new(RuntimeConfig {
            datastore_path,
            sync: Some(SyncConfig::new().with_listen_addr("127.0.0.1:0")),
            ..RuntimeConfig::default()
        })
        .unwrap();

        assert_eq!(
            second.sync_manager.as_ref().unwrap().node_id(),
            &first_node_id
        );
    }
}
