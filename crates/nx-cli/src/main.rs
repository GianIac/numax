use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use nx_core::runtime::{Runtime, RuntimeConfig};

#[derive(Parser, Debug)]
#[command(name = "nx")]
#[command(about = "Numax CLI", long_about = None)]
enum Cli {
    /// Execute a WebAssembly module
    Run {
        /// Path to the .wasm module
        module: PathBuf,

        /// Datastore directory path (default: ./nx-data)
        #[arg(long, value_name = "PATH")]
        datastore_path: Option<PathBuf>,
    },
}

fn main() {
    if let Err(e) = real_main() {
        eprintln!("[nx-cli] error: {e}");
        std::process::exit(1);
    }
}

fn real_main() -> Result<()> {
    let cli = Cli::parse();

    match cli {
        Cli::Run {
            module,
            datastore_path,
        } => {
            let bytes = fs::read(&module)?;

            let mut cfg = RuntimeConfig::default();
            if let Some(p) = datastore_path {
                cfg.datastore_path = p;
            }

            let rt = Runtime::new(cfg)?;
            rt.run_module(&bytes)?;
        }
    }

    Ok(())
}
