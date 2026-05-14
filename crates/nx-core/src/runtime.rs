use anyhow::{Result, anyhow};
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Duration as TokioDuration, Instant};
use wasmtime::{Engine, Linker, Module, Store};
use wasmtime_wasi::{WasiCtx, p1};

use nx_store::Store as NxStore;
use nx_sync::NodeId;

use crate::host_api;
use crate::sync_config::SyncConfig;
use crate::sync_manager::{SyncHandle, SyncManager};

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
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            enable_wasi: true,
            max_memory_bytes: None,
            datastore_path: PathBuf::from("./nx-data"),
            sync: None,
        }
    }
}

/// Host status associated with the Store.
pub struct HostState {
    pub wasi: Option<p1::WasiP1Ctx>,
    pub store: Arc<NxStore>,
    pub sync_handle: Option<SyncHandle>,
}

pub struct Runtime {
    engine: Engine,
    linker: Linker<HostState>,
    config: RuntimeConfig,
    store: Arc<NxStore>,

    sync_manager: Option<SyncManager>,
    sync_handle: Option<SyncHandle>,
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

        // Initialize SyncManager if configured, and derive its handle up-front so every HostState built afterwards sees the same op channel.
        let (sync_manager, sync_handle) = if let Some(ref sync_config) = config.sync {
            let node_id = NodeId::generate();
            let manager = SyncManager::new(node_id, sync_config.clone(), Arc::clone(&store));
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
            sync_manager,
            sync_handle,
        })
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
        tracing::info!(
            node_id = %manager.node_id(),
            "sync manager started"
        );
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

    /// Keep the runtime alive while sync background tasks do their work.
    pub async fn serve(&self) -> Result<()> {
        self.serve_until_shutdown(async {
            if let Err(e) = tokio::signal::ctrl_c().await {
                tracing::warn!(error = %e, "failed to listen for shutdown signal");
            }
        })
        .await
    }

    /// Testable variant of `serve`: the caller provides the shutdown signal.
    pub async fn serve_until_shutdown<S>(&self, shutdown: S) -> Result<()>
    where
        S: Future<Output = ()>,
    {
        if !self.sync_enabled() {
            tracing::debug!("serve: sync disabled, nothing to keep alive");
            return Ok(());
        }

        tracing::info!("runtime entering long-running sync mode");
        shutdown.await;
        tracing::info!("runtime shutdown requested");

        Ok(())
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
        let deadline = Instant::now() + TokioDuration::from(duration);

        loop {
            if let Some(manager) = self.sync_manager.as_ref() {
                manager.reconnect_configured_peers().await;
            }

            let now = Instant::now();
            if now >= deadline {
                break;
            }

            let remaining = deadline - now;
            tokio::time::sleep(remaining.min(TokioDuration::from_millis(100))).await;
        }

        Ok(())
    }

    pub async fn run_module(&self, wasm_bytes: &[u8]) -> Result<()> {
        // handle shared to the DB
        let store_db = Arc::clone(&self.store);

        // Builds the host state for this run
        let host_state = if self.config.enable_wasi {
            let wasi = WasiCtx::builder().inherit_stdio().inherit_args().build_p1();

            HostState {
                wasi: Some(wasi),
                store: store_db,
                sync_handle: self.sync_handle.clone(),
            }
        } else {
            HostState {
                wasi: None,
                store: store_db,
                sync_handle: self.sync_handle.clone(),
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
                })
            )
            .await
            .is_err()
        );

        let (tx, rx) = oneshot::channel::<()>();
        tx.send(()).unwrap();
        timeout(
            Duration::from_secs(1),
            runtime.serve_until_shutdown(async {
                let _ = rx.await;
            }),
        )
        .await
        .unwrap()
        .unwrap();
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
}
