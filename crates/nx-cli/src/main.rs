use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use nx_core::runtime::{Runtime, RuntimeConfig};

#[derive(Parser, Debug)]
#[command(name = "nx")]
#[command(about = "NumaX CLI", long_about = None)]
enum Cli {
    /// exec WASM modul
    Run {
        /// Path to the .wasm modul
        module: PathBuf,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli {
        Cli::Run { module } => {
            let bytes = fs::read(&module)?;
            let rt = Runtime::new(RuntimeConfig {})?;
            rt.run_module(&bytes)?;
        }
    }

    Ok(())
}
