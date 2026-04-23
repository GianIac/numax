use anyhow::{Result, anyhow};
use std::path::PathBuf;
use std::sync::Arc;
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
            let manager = SyncManager::new(node_id, sync_config.clone());
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