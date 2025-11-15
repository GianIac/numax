use anyhow::Result;
use wasmtime::{Engine, Linker, Module, Store};

use crate::host_api;

pub struct RuntimeConfig {
    // TODO: config for future f. (store path, peers, permessi, ecc.)
}

pub struct Runtime {
    engine: Engine,
    linker: Linker<()>,
}

impl Runtime {
    pub fn new(_config: RuntimeConfig) -> Result<Self> {
        let engine = Engine::default();
        let mut linker: Linker<()> = Linker::new(&engine);

        // host_log
        host_api::log::add_to_linker(&mut linker)?;

        Ok(Self { engine, linker })
    }

    pub fn run_module(&self, wasm_bytes: &[u8]) -> Result<()> {
        // state (()) for the moment
        let mut store = Store::new(&self.engine, ());
        let module = Module::new(&self.engine, wasm_bytes)?;

        let instance = self.linker.instantiate(&mut store, &module)?;

        // try first "run", later "_start"
        let run = instance
            .get_typed_func::<(), ()>(&mut store, "run")
            .or_else(|_| instance.get_typed_func::<(), ()>(&mut store, "_start"))?;

        run.call(&mut store, ())?;

        Ok(())
    }
}
