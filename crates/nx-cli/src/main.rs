use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use nx_core::runtime::{Runtime, RuntimeConfig};
use nx_core::SyncConfig;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "nx")]
#[command(about = "Numax CLI - distributed WASM runtime", long_about = None)]
enum Cli {
    /// Execute a WebAssembly module
    Run {
        /// Path to the .wasm module
        module: PathBuf,

        /// Datastore directory path (default: ./nx-data)
        #[arg(long, value_name = "PATH")]
        datastore_path: Option<PathBuf>,

        /// Enable sync and listen on this address (e.g., "0.0.0.0:9000")
        #[arg(long, value_name = "ADDR")]
        listen: Option<String>,

        /// Peer addresses to connect to (can be repeated)
        #[arg(long = "peer", value_name = "ADDR")]
        peers: Vec<String>,

        /// Key prefixes to replicate (can be repeated, e.g., "counter:")
        #[arg(long = "sync-prefix", value_name = "PREFIX")]
        sync_prefixes: Vec<String>,

        /// Enable verbose logging
        #[arg(short, long)]
        verbose: bool,
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
            listen,
            peers,
            sync_prefixes,
            verbose,
        } => {
            // Setup logging
            let log_level = if verbose { "debug" } else { "info" };
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level)),
                )
                .init();

            let bytes = fs::read(&module)?;

            let mut cfg = RuntimeConfig::default();

            if let Some(p) = datastore_path {
                cfg.datastore_path = p;
            }

            // Configura sync se abilitato
            if listen.is_some() || !sync_prefixes.is_empty() {
                let mut sync_cfg = SyncConfig::new();

                if let Some(addr) = listen {
                    sync_cfg = sync_cfg.with_listen_addr(addr);
                }

                for peer in peers {
                    sync_cfg = sync_cfg.with_peer(peer);
                }

                for prefix in sync_prefixes {
                    sync_cfg = sync_cfg.with_prefix(prefix);
                }

                if sync_cfg.is_enabled() {
                    info!(
                        listen = ?sync_cfg.listen_addr,
                        prefixes = ?sync_cfg.replicated_prefixes,
                        peers = ?sync_cfg.peers,
                        "sync enabled"
                    );
                    cfg.sync = Some(sync_cfg);
                }
            }

            let rt = Runtime::new(cfg)?;
            rt.run_module(&bytes)?;
        }
    }

    Ok(())
}