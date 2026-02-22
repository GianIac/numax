use anyhow::{Result, anyhow};
use std::path::PathBuf;
use std::sync::Arc;
use wasmtime::{Engine, Linker, Module, Store};
use wasmtime_wasi::{WasiCtx, p1};

use nx_store::Store as NxStore;
use nx_sync::NodeId;

use crate::host_api;
use crate::sync_config::SyncConfig;
use crate::sync_manager::SyncManager;

/// Configurazione del runtime.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Abilita o meno il supporto WASI (stdout, args, ecc.).
    pub enable_wasi: bool,

    /// Limite massimo di memoria per modulo (non ancora applicato).
    pub max_memory_bytes: Option<u64>,

    /// The path to the datastore.
    pub datastore_path: PathBuf,

    /// Configurazione sync (None = sync disabilitato).
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

/// Stato host associato allo Store.
pub struct HostState {
    pub wasi: Option<p1::WasiP1Ctx>,
    pub store: Arc<NxStore>,
    pub sync_manager: Option<Arc<SyncManager>>,
}

pub struct Runtime {
    engine: Engine,
    linker: Linker<HostState>,
    config: RuntimeConfig,
    store: Arc<NxStore>,
    sync_manager: Option<Arc<SyncManager>>,
}

impl Runtime {
    pub fn new(config: RuntimeConfig) -> Result<Self> {
        // Engine con config (utile per debug/backtrace; limiti memory opzionali in futuro)
        let mut wasm_cfg = wasmtime::Config::new();
        wasm_cfg.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable);
        let engine = Engine::new(&wasm_cfg)?;

        let mut linker: Linker<HostState> = Linker::new(&engine);

        // host_log (namespace "nx", func "host_log")
        host_api::log::add_to_linker(&mut linker)?;
        // host_db (namespace "nx", func "db_get", "db_set" etc.)
        host_api::db::add_to_linker(&mut linker)?;

        // WASI base (preview1 / p1)
        if config.enable_wasi {
            wasmtime_wasi::p1::add_to_linker_sync(&mut linker, |state: &mut HostState| {
                state
                    .wasi
                    .as_mut()
                    .expect("WASI abilitato ma non inizializzato nello store")
            })?;
        }

        // apri il datastore UNA SOLA VOLTA qui
        let store = NxStore::open(&config.datastore_path)
            .map_err(|e| anyhow!("Failed to open datastore: {e}"))?;
        let store = Arc::new(store);

        // Inizializza SyncManager se configurato
        let sync_manager = if let Some(ref sync_config) = config.sync {
            let node_id = NodeId::generate();
            let manager = SyncManager::new(node_id, sync_config.clone());
            Some(Arc::new(manager))
        } else {
            None
        };

        Ok(Self {
            engine,
            linker,
            config,
            store,
            sync_manager,
        })
    }

    /// Avvia il sync networking (se configurato).
    /// Deve essere chiamato prima di run_module in contesto async.
    pub async fn start_sync(&self) -> Result<()> {
        if let Some(ref manager) = self.sync_manager {
            // SyncManager::start richiede &mut self, ma abbiamo Arc
            // Per ora skip - implementazione completa richiede refactor
            tracing::info!(
                "sync manager configured (full start requires async runtime integration)"
            );
        }
        Ok(())
    }

    /// Restituisce il SyncManager (se presente).
    pub fn sync_manager(&self) -> Option<Arc<SyncManager>> {
        self.sync_manager.clone()
    }

    pub fn run_module(&self, wasm_bytes: &[u8]) -> Result<()> {
        // handle condiviso al DB
        let store_db = Arc::clone(&self.store);

        // 1. Costruisce lo stato host per questo run
        let host_state = if self.config.enable_wasi {
            let wasi = WasiCtx::builder().inherit_stdio().inherit_args().build_p1();

            HostState {
                wasi: Some(wasi),
                store: store_db,
                sync_manager: self.sync_manager.clone(),
            }
        } else {
            HostState {
                wasi: None,
                store: store_db,
                sync_manager: self.sync_manager.clone(),
            }
        };

        let mut store = Store::new(&self.engine, host_state);

        // 2. Compilazione / validazione modulo
        let module = Module::new(&self.engine, wasm_bytes)
            .map_err(|e| anyhow!("Invalid module (compile/validate): {e}"))?;

        // 3. Istanziazione (linking import/host)
        let instance = self
            .linker
            .instantiate(&mut store, &module)
            .map_err(|e| anyhow!("Link error while instantiating module: {e}"))?;

        // 4. Entrypoint: prima `run`, poi `_start`
        let run = instance
            .get_typed_func::<(), ()>(&mut store, "run")
            .or_else(|_| instance.get_typed_func::<(), ()>(&mut store, "_start"))
            .map_err(|e| anyhow!("No entrypoint found (expected `run` or `_start`): {e}"))?;

        // 5. Esecuzione
        run.call(&mut store, ())
            .map_err(|e| anyhow!("Error while executing module: {e}"))?;

        Ok(())
    }
}
