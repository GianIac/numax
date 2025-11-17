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
        Cli::Run { module } => {
            let bytes = fs::read(&module)?;

            // Per ora usiamo la config di default:
            // - WASI abilitato
            // - nessun limite di memoria/fuel
            let rt = Runtime::new(RuntimeConfig::default())?;
            rt.run_module(&bytes)?;
        }
    }

    Ok(())
}
