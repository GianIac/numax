use anyhow::{anyhow, Result};
use wasmtime::{Engine, Linker, Module, Store};
use wasmtime_wasi::{p1, WasiCtx};
use std::path::PathBuf;
use nx_store::Store as NxStore;
use crate::host_api;

/// Configurazione del runtime.
/// Crescerà con store path, permessi, ecc. nelle fasi successive.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Abilita o meno il supporto WASI (stdout, args, ecc.).
    pub enable_wasi: bool,

    /// Limite massimo di memoria per modulo (non ancora applicato).
    /// Lo teniamo già in config per coerenza con la roadmap.
    pub max_memory_bytes: Option<u64>,

    /// The path to the datastore.
    pub datastore_path: PathBuf,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            enable_wasi: true,
            max_memory_bytes: None,
            datastore_path: PathBuf::from("./nx-data"),
        }
    }
}

/// Stato host associato allo Store.
/// In Fase 1 contiene solo WASI, ma potrà crescere (store, sync, ecc.).
pub struct HostState {
    pub wasi: Option<p1::WasiP1Ctx>,
    pub store: NxStore,
}

pub struct Runtime {
    engine: Engine,
    linker: Linker<HostState>,
    config: RuntimeConfig,
    store: NxStore, // <-- FIX: store inizializzato una volta sola e clonato per HostState
}

impl Runtime {
    pub fn new(config: RuntimeConfig) -> Result<Self> {
        // In futuro qui possiamo usare wasmtime::Config
        // per applicare limiti di memoria, ecc.
        let engine = Engine::default();

        let mut linker: Linker<HostState> = Linker::new(&engine);

        // host_log (namespace "nx", func "host_log")
        host_api::log::add_to_linker(&mut linker)?;
        // host_db (namespace "nx", func "db_get", "db_set" etc.)
        host_api::db::add_to_linker(&mut linker)?;

        // WASI base (preview1 / p1)
        if config.enable_wasi {
            // Mappa HostState -> &mut WasiP1Ctx per le funzioni WASI.
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

        Ok(Self {
            engine,
            linker,
            config,
            store,
        })
    }

    pub fn run_module(&self, wasm_bytes: &[u8]) -> Result<()> {
        // Usa clone del singleton.
        let store_db = self.store.clone();

        // 1. Costruisce lo stato host per questo run
        let host_state = if self.config.enable_wasi {
            let wasi = WasiCtx::builder()
                .inherit_stdio()
                .inherit_args()
                .build_p1();

            HostState {
                wasi: Some(wasi),
                store: store_db,
            }
        } else {
            HostState {
                wasi: None,
                store: store_db,
            }
        };

        let mut store = Store::new(&self.engine, host_state);

        // 2. Compilazione / validazione modulo
        let module = Module::new(&self.engine, wasm_bytes)
            .map_err(|_e| anyhow!("Invalid module (failed to compile/validate)"))?;

        // 3. Istanziazione (linking import/host)
        let instance = self
            .linker
            .instantiate(&mut store, &module)
            .map_err(|_e| {
                anyhow!("Link error while instantiating module (missing import or incompatible types)")
            })?;

        // 4. Entrypoint: prima `run`, poi `_start`
        let run = instance
            .get_typed_func::<(), ()>(&mut store, "run")
            .or_else(|_| instance.get_typed_func::<(), ()>(&mut store, "_start"))
            .map_err(|_e| anyhow!("No entrypoint found (expected `run` or `_start`)"))?;

        // 5. Esecuzione
        run.call(&mut store, ())
            .map_err(|_e| anyhow!("Error while executing module"))?;

        Ok(())
    }
}
