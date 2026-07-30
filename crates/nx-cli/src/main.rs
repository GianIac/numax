mod config;

use std::fs;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};
use config::*;
use nx_core::runtime::{DEFAULT_SHUTDOWN_TIMEOUT, Runtime, RuntimeConfig};
use nx_core::sync_manager::{
    DEFAULT_MIGRATION_BATCH_BYTES, DEFAULT_MIGRATION_BATCH_SIZE, MigrationOptions,
    migrate_sync_schema_at_path,
};
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "nx")]
#[command(version)]
#[command(about = "Numax CLI - distributed WASM runtime", long_about = None)]
// Parsed once at process startup; keeping variants inline preserves clap readability.
#[allow(clippy::large_enum_variant)]
enum Cli {
    /// Execute a WebAssembly module
    Run {
        /// Path to the .wasm module
        module: PathBuf,

        /// Datastore directory path (default: ./nx-data)
        #[arg(long, value_name = "PATH")]
        datastore_path: Option<PathBuf>,

        /// Path to a Numax TOML configuration file.
        #[arg(long, value_name = "PATH")]
        config: Option<PathBuf>,

        /// Enable sync and listen on this address (e.g., "0.0.0.0:9000")
        #[arg(long, value_name = "ADDR")]
        listen: Option<String>,

        /// Peer addresses to connect to (can be repeated). Requires --listen.
        #[arg(long = "peer", value_name = "ADDR")]
        peers: Vec<String>,

        /// Keep sync alive for a bounded duration after running the module (e.g. 500ms, 5s, 2m).
        #[arg(long, value_name = "DURATION", value_parser = parse_duration)]
        settle_for: Option<Duration>,

        /// Wait for a bounded duration after starting sync and before running the module.
        #[arg(long, value_name = "DURATION", value_parser = parse_duration)]
        wait_before_run: Option<Duration>,

        /// Print the final value of a GCounter after settle/serve completes.
        #[arg(long, value_name = "KEY")]
        print_gcounter: Option<String>,

        /// Print the final value of a PNCounter after settle/serve completes.
        #[arg(long, value_name = "KEY")]
        print_pncounter: Option<String>,

        /// Print the final value of an LWW-Register after settle/serve completes.
        #[arg(long, value_name = "KEY")]
        print_lww_register: Option<String>,

        /// Print the final visible entries of an LWW-Map after settle/serve completes.
        #[arg(long, value_name = "KEY")]
        print_lww_map: Option<String>,

        /// Print the final visible elements of an ORSet after settle/serve completes.
        #[arg(long, value_name = "KEY")]
        print_orset: Option<String>,

        /// Print the final visible values of an RGA after settle/serve completes.
        #[arg(long, value_name = "KEY")]
        print_rga: Option<String>,

        /// Maximum time allowed for shutdown before returning an error.
        #[arg(long, value_name = "DURATION", value_parser = parse_duration)]
        shutdown_timeout: Option<Duration>,

        /// Enable verbose logging
        #[arg(short, long)]
        verbose: bool,

        /// Logging level (trace, debug, info, warn, error).
        #[arg(long, value_name = "LEVEL")]
        log_level: Option<String>,

        /// Logging format.
        #[arg(long, value_enum, value_name = "FORMAT")]
        log_format: Option<LogFormat>,

        /// Enable the observability HTTP endpoint (e.g. "127.0.0.1:9100").
        #[arg(long, value_name = "ADDR")]
        observability_listen: Option<String>,

        /// Expose Tokio task diagnostics to tokio-console (requires the tokio-console feature).
        #[arg(long)]
        tokio_console: bool,

        /// Path to this node's TLS certificate (PEM)
        #[arg(long, value_name = "PATH")]
        tls_cert: Option<PathBuf>,

        /// Path to this node's TLS private key (PEM)
        #[arg(long, value_name = "PATH")]
        tls_key: Option<PathBuf>,

        /// Path to the CA certificate used to verify peers (PEM, enables mTLS)
        #[arg(long, value_name = "PATH")]
        tls_ca: Option<PathBuf>,

        /// Comma-separated allowlist of peer NodeIds (hex). Requires --tls-ca.
        #[arg(long, value_name = "ID1,ID2,...")]
        allowed_peers: Option<String>,

        /// Skip TLS certificate verification (DEVELOPMENT ONLY).
        #[arg(long)]
        tls_insecure: bool,

        /// Use JSON for the sync wire protocol instead of bincode.
        #[arg(long)]
        debug_protocol: bool,
    },

    /// Inspect and validate Numax configuration files
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },

    /// Migrate a datastore schema offline without running a module
    Migrate {
        /// Datastore directory path to migrate.
        #[arg(long, value_name = "PATH", default_value = "./nx-data")]
        datastore_path: PathBuf,

        /// Maximum records processed per migration batch.
        #[arg(long, value_name = "COUNT", default_value_t = DEFAULT_MIGRATION_BATCH_SIZE, value_parser = parse_nonzero_u32)]
        max_records: NonZeroU32,

        /// Maximum bytes processed per migration batch, including generated mutations.
        #[arg(long, value_name = "BYTES", default_value_t = DEFAULT_MIGRATION_BATCH_BYTES, value_parser = parse_nonzero_byte_size)]
        max_bytes: NonZeroUsize,
    },
}

#[derive(Subcommand, Debug)]
enum ConfigCommand {
    /// Generate a commented Numax TOML configuration file
    Init {
        /// Path where the Numax TOML configuration file will be written.
        #[arg(long, value_name = "PATH", default_value = "numax.toml")]
        output: PathBuf,

        /// Overwrite the output file if it already exists.
        #[arg(long)]
        force: bool,
    },

    /// Validate a Numax TOML configuration file without running a module
    Validate {
        /// Path to the Numax TOML configuration file.
        #[arg(long, value_name = "PATH", default_value = "numax.toml")]
        config: PathBuf,
    },

    /// Show resolved configuration after applying CLI/env/file/default precedence
    Show {
        /// Path to the Numax TOML configuration file.
        #[arg(long, value_name = "PATH", default_value = "numax.toml")]
        config: PathBuf,

        /// Show the effective configuration.
        #[arg(long)]
        effective: bool,
    },
}

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    if let Err(e) = rt.block_on(real_main()) {
        eprintln!("[nx-cli] error: {e}");
        std::process::exit(1);
    }
}

async fn real_main() -> Result<()> {
    let cli = Cli::parse();

    match cli {
        Cli::Run {
            module,
            datastore_path,
            config,
            listen,
            peers,
            settle_for,
            wait_before_run,
            print_gcounter,
            print_pncounter,
            print_lww_register,
            print_lww_map,
            print_orset,
            print_rga,
            shutdown_timeout,
            verbose,
            log_level,
            log_format,
            observability_listen,
            tokio_console,
            tls_cert,
            tls_key,
            tls_ca,
            allowed_peers,
            tls_insecure,
            debug_protocol,
        } => {
            let file_config = load_run_config(config.as_ref())?;
            let cli = RunCliOptions {
                datastore_path,
                listen,
                peers,
                observability_listen,
                tls_cert,
                tls_key,
                tls_ca,
                allowed_peers,
                tls_insecure,
                debug_protocol,
                verbose,
                log_level,
                log_format,
            };
            let effective = EffectiveRunConfig::resolve(cli, &file_config)?;

            // Setup logging
            init_logging(&effective.log_level, effective.log_format, tokio_console)?;

            validate_settle_mode(&effective.sync, settle_for)?;
            validate_wait_before_run(&effective.sync, wait_before_run)?;
            validate_print_gcounter(&effective.sync, &print_gcounter)?;
            validate_print_pncounter(&effective.sync, &print_pncounter)?;
            validate_print_lww_register(&effective.sync, &print_lww_register)?;
            validate_print_lww_map(&effective.sync, &print_lww_map)?;
            validate_print_orset(&effective.sync, &print_orset)?;
            validate_print_rga(&effective.sync, &print_rga)?;

            // Read the wasm module
            let bytes = fs::read(&module)?;

            // Build the runtime config
            let mut cfg = RuntimeConfig::default();
            if let Some(p) = effective.datastore_path {
                cfg.datastore_path = p;
            }
            cfg.module_id = module.to_string_lossy().into_owned();
            if let Some(s) = effective.sync {
                info!(
                    listen = ?s.listen_addr,
                    peers = ?s.peers,
                    tls = s.tls.is_some(),
                    serialization_format = ?s.serialization_format,
                    "sync enabled"
                );
                cfg.sync = Some(s);
            }
            cfg.observability = effective.observability;

            let mut rt = Runtime::new(cfg)?;
            let run_result: Result<()> = async {
                rt.start_observability().await?;
                rt.start_sync().await?;
                if let Some(duration) = wait_before_run {
                    rt.wait_before_run(duration).await?;
                }
                rt.run_module(&bytes).await?;
                match settle_for {
                    Some(duration) => rt.settle_for(duration).await?,
                    None if rt.sync_enabled() => rt.serve().await?,
                    None => {}
                }
                if let Some(key) = print_gcounter {
                    let value = rt
                        .get_counter_value(&key)
                        .await
                        .ok_or_else(|| anyhow::anyhow!("--print-gcounter requires sync"))?;
                    println!("{key} = {value}");
                }
                if let Some(key) = print_pncounter {
                    let value = rt
                        .get_pncounter_value(&key)
                        .await
                        .ok_or_else(|| anyhow::anyhow!("--print-pncounter requires sync"))?;
                    println!("{key} = {value}");
                }
                if let Some(key) = print_lww_register {
                    let value = rt
                        .get_lww_register_value(&key)
                        .await
                        .ok_or_else(|| anyhow::anyhow!("--print-lww-register requires sync"))?;
                    match value {
                        Some(bytes) => println!("{key} = {}", String::from_utf8_lossy(&bytes)),
                        None => println!("{key} = <unset>"),
                    }
                }
                if let Some(key) = print_lww_map {
                    let entries = rt
                        .get_lww_map_entries(&key)
                        .await
                        .ok_or_else(|| anyhow::anyhow!("--print-lww-map requires sync"))?;
                    let formatted = entries
                        .iter()
                        .map(|(field, value)| format!("{field}={}", String::from_utf8_lossy(value)))
                        .collect::<Vec<_>>()
                        .join(", ");
                    println!("{key} = {{{formatted}}}");
                }
                if let Some(key) = print_orset {
                    let elements = rt
                        .get_orset_elements(&key)
                        .await
                        .ok_or_else(|| anyhow::anyhow!("--print-orset requires sync"))?;
                    println!("{key} = [{}]", elements.join(", "));
                }
                if let Some(key) = print_rga {
                    let values = rt
                        .get_rga_values(&key)
                        .await
                        .ok_or_else(|| anyhow::anyhow!("--print-rga requires sync"))?;
                    let formatted = values
                        .iter()
                        .map(|value| String::from_utf8_lossy(value).to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    println!("{key} = [{formatted}]");
                }
                Ok(())
            }
            .await;

            let shutdown_result = rt
                .shutdown_with_timeout(shutdown_timeout.unwrap_or(DEFAULT_SHUTDOWN_TIMEOUT))
                .await;

            run_result?;
            shutdown_result?;
        }
        Cli::Config { command } => match command {
            ConfigCommand::Init { output, force } => {
                init_config_file(&output, force)?;
                println!("configuration written: {}", output.display());
            }
            ConfigCommand::Validate { config } => {
                load_run_config(Some(&config))?;
                println!("configuration is valid: {}", config.display());
            }
            ConfigCommand::Show { config, effective } => {
                if !effective {
                    anyhow::bail!("nx config show currently requires --effective");
                }
                let file_config = load_run_config_or_default(Some(&config))?;
                let effective =
                    EffectiveRunConfig::resolve(RunCliOptions::default(), &file_config)?;
                print!("{}", effective.render_effective_toml());
            }
        },
        Cli::Migrate {
            datastore_path,
            max_records,
            max_bytes,
        } => {
            let options = MigrationOptions {
                max_records,
                max_bytes,
            };
            migrate_sync_schema_at_path(&datastore_path, options)?;
            println!("datastore migrated: {}", datastore_path.display());
        }
    }

    Ok(())
}

fn parse_nonzero_u32(input: &str) -> Result<NonZeroU32> {
    let value = input.parse::<u32>()?;
    NonZeroU32::new(value).ok_or_else(|| anyhow::anyhow!("value must be greater than zero"))
}

fn parse_nonzero_byte_size(input: &str) -> Result<NonZeroUsize> {
    let value = parse_byte_size(input)?;
    NonZeroUsize::new(value).ok_or_else(|| anyhow::anyhow!("value must be greater than zero"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nx_core::{SerializationFormat, SyncConfig, TlsConfig};
    use std::path::PathBuf;

    fn p(s: &str) -> Option<PathBuf> {
        Some(PathBuf::from(s))
    }

    mod duration_parser {
        use super::*;

        #[test]
        fn parses_milliseconds() {
            assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        }

        #[test]
        fn parses_seconds() {
            assert_eq!(parse_duration("5s").unwrap(), Duration::from_secs(5));
        }

        #[test]
        fn parses_minutes() {
            assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
        }

        #[test]
        fn parses_plain_number_as_seconds() {
            assert_eq!(parse_duration("3").unwrap(), Duration::from_secs(3));
        }

        #[test]
        fn rejects_invalid_duration() {
            assert!(parse_duration("soon").is_err());
        }

        #[test]
        fn rejects_zero_duration() {
            assert!(parse_duration("0s").is_err());
        }
    }

    mod byte_size_parser {
        use super::*;

        #[test]
        fn parses_mib() {
            assert_eq!(parse_byte_size("16MiB").unwrap(), 16 * 1024 * 1024);
        }

        #[test]
        fn parses_kib_with_space() {
            assert_eq!(parse_byte_size("4 KiB").unwrap(), 4 * 1024);
        }

        #[test]
        fn parses_plain_bytes() {
            assert_eq!(parse_byte_size("128").unwrap(), 128);
        }

        #[test]
        fn rejects_zero() {
            assert!(parse_byte_size("0MiB").is_err());
        }
    }

    mod file_config {
        use super::*;

        fn cli_defaults() -> RunCliOptions {
            RunCliOptions {
                datastore_path: None,
                listen: None,
                peers: Vec::new(),
                observability_listen: None,
                tls_cert: None,
                tls_key: None,
                tls_ca: None,
                allowed_peers: None,
                tls_insecure: false,
                debug_protocol: false,
                verbose: false,
                log_level: None,
                log_format: None,
            }
        }

        #[test]
        fn parses_limits_toml() {
            let cfg: RunFileConfig = toml::from_str(
                r#"
                [limits]
                max_peers = 64
                queued_ops_limit = 10000
                op_log_limit = 20000
                seen_ops_limit = 100000
                max_message_size = "16MiB"
                socket_timeout_secs = 30
                reconnect_initial_delay = "500ms"
                reconnect_max_delay = "30s"
                peer_dead_after_failures = 3
                anti_entropy_interval = "30s"
                "#,
            )
            .unwrap();

            let limits = cfg.limits.unwrap();
            assert_eq!(limits.max_peers, Some(64));
            assert_eq!(limits.queued_ops_limit, Some(10_000));
            assert_eq!(limits.op_log_limit, Some(20_000));
            assert_eq!(limits.seen_ops_limit, Some(100_000));
            assert_eq!(limits.max_message_size.as_deref(), Some("16MiB"));
            assert_eq!(limits.socket_timeout_secs, Some(30));
            assert_eq!(limits.reconnect_initial_delay.as_deref(), Some("500ms"));
            assert_eq!(limits.reconnect_max_delay.as_deref(), Some("30s"));
            assert_eq!(limits.peer_dead_after_failures, Some(3));
            assert_eq!(limits.anti_entropy_interval.as_deref(), Some("30s"));
        }

        #[test]
        fn parses_network_tls_storage_and_discovery_toml() {
            let cfg: RunFileConfig = toml::from_str(
                r#"
                [network]
                listen = "0.0.0.0:9000"
                peers = ["127.0.0.1:9001", "127.0.0.1:9002"]
                serialization_format = "bincode"

                [tls]
                cert = "./certs/node.pem"
                key = "./certs/node-key.pem"
                ca = "./certs/ca.pem"
                allowed_peers = ["node-a", "node-b"]
                insecure = false

                [storage]
                datastore_path = "./data/node-a"

                [discovery]
                mode = "static"
                "#,
            )
            .unwrap();

            let network = cfg.network.unwrap();
            assert_eq!(network.listen.as_deref(), Some("0.0.0.0:9000"));
            assert_eq!(
                network.peers.as_deref(),
                Some(["127.0.0.1:9001".to_string(), "127.0.0.1:9002".to_string()].as_slice())
            );
            assert_eq!(
                network.serialization_format,
                Some(WireSerializationFormat::Bincode)
            );

            let tls = cfg.tls.unwrap();
            assert_eq!(
                tls.cert.as_deref(),
                Some(std::path::Path::new("./certs/node.pem"))
            );
            assert_eq!(
                tls.key.as_deref(),
                Some(std::path::Path::new("./certs/node-key.pem"))
            );
            assert_eq!(
                tls.ca.as_deref(),
                Some(std::path::Path::new("./certs/ca.pem"))
            );
            assert_eq!(
                tls.allowed_peers.as_deref(),
                Some(["node-a".to_string(), "node-b".to_string()].as_slice())
            );
            assert_eq!(tls.insecure, Some(false));

            let storage = cfg.storage.unwrap();
            assert_eq!(
                storage.datastore_path.as_deref(),
                Some(std::path::Path::new("./data/node-a"))
            );

            let discovery = cfg.discovery.unwrap();
            assert_eq!(discovery.mode, Some(DiscoveryMode::Static));
        }

        #[test]
        fn rejects_unknown_file_config_fields() {
            let err = toml::from_str::<RunFileConfig>(
                r#"
                [network]
                listen = "0.0.0.0:9000"
                typo = true
                "#,
            )
            .unwrap_err()
            .to_string();

            assert!(err.contains("unknown field"), "got: {err}");
        }

        #[test]
        fn effective_config_uses_file_when_cli_and_env_are_absent() {
            let file_config: RunFileConfig = toml::from_str(
                r#"
                [network]
                listen = "0.0.0.0:9000"
                peers = ["127.0.0.1:9001"]
                serialization_format = "json"

                [storage]
                datastore_path = "./data/file"

                [observability]
                listen = "127.0.0.1:9100"
                log_level = "warn"
                log_format = "json"

                [limits]
                queued_ops_limit = 123
                "#,
            )
            .unwrap();

            let effective = EffectiveRunConfig::resolve_with_env(
                cli_defaults(),
                EnvRunConfig::default(),
                &file_config,
            )
            .unwrap();

            assert_eq!(
                effective.datastore_path.as_deref(),
                Some(std::path::Path::new("./data/file"))
            );
            assert_eq!(effective.log_level, "warn");
            assert_eq!(effective.log_format, LogFormat::Json);
            assert_eq!(
                effective
                    .observability
                    .as_ref()
                    .map(|cfg| cfg.listen_addr.as_str()),
                Some("127.0.0.1:9100")
            );

            let sync = effective.sync.unwrap();
            assert_eq!(sync.listen_addr.as_deref(), Some("0.0.0.0:9000"));
            assert_eq!(sync.peers, vec!["127.0.0.1:9001".to_string()]);
            assert_eq!(sync.serialization_format, SerializationFormat::Json);
            assert_eq!(sync.queued_ops_limit, 123);
        }

        #[test]
        fn effective_config_precedence_is_cli_then_env_then_file() {
            let file_config: RunFileConfig = toml::from_str(
                r#"
                [network]
                listen = "0.0.0.0:9000"
                peers = ["file:1"]

                [storage]
                datastore_path = "./data/file"
                "#,
            )
            .unwrap();
            let mut cli = cli_defaults();
            cli.datastore_path = Some(PathBuf::from("./data/cli"));
            cli.listen = Some("0.0.0.0:9100".into());
            cli.peers = vec!["cli:1".into()];
            let env_config = EnvRunConfig {
                datastore_path: Some(PathBuf::from("./data/env")),
                listen: Some("0.0.0.0:9200".into()),
                peers: Some(vec!["env:1".into()]),
                ..EnvRunConfig::default()
            };

            let effective =
                EffectiveRunConfig::resolve_with_env(cli, env_config, &file_config).unwrap();

            assert_eq!(
                effective.datastore_path.as_deref(),
                Some(std::path::Path::new("./data/cli"))
            );
            let sync = effective.sync.unwrap();
            assert_eq!(sync.listen_addr.as_deref(), Some("0.0.0.0:9100"));
            assert_eq!(sync.peers, vec!["cli:1".to_string()]);
        }

        #[test]
        fn parses_observability_toml() {
            let cfg: RunFileConfig = toml::from_str(
                r#"
                [observability]
                listen = "127.0.0.1:9100"
                log_level = "debug"
                log_format = "json"
                request_timeout_secs = 7
                "#,
            )
            .unwrap();

            let observability = cfg.observability.unwrap();
            assert_eq!(observability.listen.as_deref(), Some("127.0.0.1:9100"));
            assert_eq!(observability.log_level.as_deref(), Some("debug"));
            assert_eq!(observability.log_format, Some(LogFormat::Json));
            assert_eq!(observability.request_timeout_secs, Some(7));
        }

        #[test]
        fn applies_limits_to_sync_config() {
            let limits = LimitsFileConfig {
                max_peers: Some(8),
                queued_ops_limit: Some(256),
                op_log_limit: Some(512),
                seen_ops_limit: Some(1024),
                max_message_size: Some("2MiB".into()),
                socket_timeout_secs: Some(5),
                reconnect_initial_delay: Some("250ms".into()),
                reconnect_max_delay: Some("5s".into()),
                peer_dead_after_failures: Some(4),
                anti_entropy_interval: Some("2s".into()),
            };

            let cfg = apply_limit_config(SyncConfig::new(), Some(&limits)).unwrap();

            assert_eq!(cfg.max_peers, 8);
            assert_eq!(cfg.queued_ops_limit, 256);
            assert_eq!(cfg.op_log_limit, 512);
            assert_eq!(cfg.seen_ops_limit, 1024);
            assert_eq!(cfg.max_message_size, 2 * 1024 * 1024);
            assert_eq!(cfg.socket_timeout, Duration::from_secs(5));
            assert_eq!(cfg.reconnect_initial_delay, Duration::from_millis(250));
            assert_eq!(cfg.reconnect_max_delay, Duration::from_secs(5));
            assert_eq!(cfg.peer_dead_after_failures, 4);
            assert_eq!(cfg.anti_entropy_interval, Duration::from_secs(2));
        }

        #[test]
        fn rejects_empty_queue_limit() {
            let limits = LimitsFileConfig {
                max_peers: None,
                queued_ops_limit: Some(0),
                op_log_limit: None,
                seen_ops_limit: None,
                max_message_size: None,
                socket_timeout_secs: None,
                reconnect_initial_delay: None,
                reconnect_max_delay: None,
                peer_dead_after_failures: None,
                anti_entropy_interval: None,
            };

            assert!(apply_limit_config(SyncConfig::new(), Some(&limits)).is_err());
        }

        #[test]
        fn builds_observability_from_file_config() {
            let cfg = ObservabilityFileConfig {
                listen: Some("127.0.0.1:9100".into()),
                log_level: None,
                log_format: None,
                request_timeout_secs: Some(7),
            };

            let observability = build_observability_config(None, Some(&cfg))
                .unwrap()
                .unwrap();

            assert_eq!(observability.listen_addr, "127.0.0.1:9100");
            assert_eq!(observability.request_timeout, Duration::from_secs(7));
        }

        #[test]
        fn cli_observability_listen_overrides_file_config() {
            let cfg = ObservabilityFileConfig {
                listen: Some("127.0.0.1:9100".into()),
                log_level: None,
                log_format: None,
                request_timeout_secs: None,
            };

            let observability =
                build_observability_config(Some("127.0.0.1:9200".into()), Some(&cfg))
                    .unwrap()
                    .unwrap();

            assert_eq!(observability.listen_addr, "127.0.0.1:9200");
        }

        #[test]
        fn rejects_empty_observability_timeout() {
            let cfg = ObservabilityFileConfig {
                listen: Some("127.0.0.1:9100".into()),
                log_level: None,
                log_format: None,
                request_timeout_secs: Some(0),
            };

            assert!(build_observability_config(None, Some(&cfg)).is_err());
        }

        #[test]
        fn resolves_log_level_from_cli_then_file_then_verbose() {
            let cfg = ObservabilityFileConfig {
                listen: None,
                log_level: Some("warn".into()),
                log_format: None,
                request_timeout_secs: None,
            };

            assert_eq!(
                resolve_log_level(false, Some("debug".into()), Some(&cfg)).unwrap(),
                "debug"
            );
            assert_eq!(resolve_log_level(false, None, Some(&cfg)).unwrap(), "warn");
            assert_eq!(resolve_log_level(true, None, None).unwrap(), "debug");
        }

        #[test]
        fn rejects_invalid_log_level() {
            assert!(resolve_log_level(false, Some("loud".into()), None).is_err());
        }
    }

    // validate_tls_flags
    mod validate_tls {
        use super::*;

        #[test]
        fn no_tls_flags_is_ok() {
            assert!(validate_tls_flags(&None, &None, &None, &None, false).is_ok());
        }

        #[test]
        fn cert_without_key_fails() {
            let r = validate_tls_flags(&p("a.pem"), &None, &None, &None, false);
            assert!(r.is_err());
            assert!(
                r.unwrap_err()
                    .to_string()
                    .contains("must be provided together")
            );
        }

        #[test]
        fn key_without_cert_fails() {
            assert!(validate_tls_flags(&None, &p("k.pem"), &None, &None, false).is_err());
        }

        #[test]
        fn cert_and_key_ok() {
            assert!(validate_tls_flags(&p("a.pem"), &p("k.pem"), &None, &None, false).is_ok());
        }

        #[test]
        fn full_mtls_ok() {
            assert!(
                validate_tls_flags(&p("a.pem"), &p("k.pem"), &p("ca.pem"), &None, false).is_ok()
            );
        }

        #[test]
        fn insecure_with_ca_fails() {
            assert!(validate_tls_flags(&None, &None, &p("ca.pem"), &None, true).is_err());
        }

        #[test]
        fn insecure_with_allowlist_fails() {
            let list = Some("abc,def".to_string());
            assert!(validate_tls_flags(&None, &None, &None, &list, true).is_err());
        }

        #[test]
        fn allowlist_without_ca_fails() {
            let list = Some("abc".to_string());
            assert!(validate_tls_flags(&p("a.pem"), &p("k.pem"), &None, &list, false).is_err());
        }

        #[test]
        fn allowlist_with_ca_ok() {
            let list = Some("abc,def".to_string());
            assert!(
                validate_tls_flags(&p("a.pem"), &p("k.pem"), &p("ca.pem"), &list, false).is_ok()
            );
        }

        #[test]
        fn insecure_alone_ok() {
            assert!(validate_tls_flags(&None, &None, &None, &None, true).is_ok());
        }
    }

    mod validate_settle {
        use super::*;

        #[test]
        fn settle_without_sync_fails() {
            let err = validate_settle_mode(&None, Some(Duration::from_secs(1)))
                .unwrap_err()
                .to_string();
            assert!(err.contains("--settle-for requires sync"));
        }

        #[test]
        fn settle_with_sync_is_ok() {
            let sync = Some(SyncConfig::new().with_listen_addr("127.0.0.1:9000"));
            assert!(validate_settle_mode(&sync, Some(Duration::from_secs(1))).is_ok());
        }

        #[test]
        fn no_settle_without_sync_is_ok() {
            assert!(validate_settle_mode(&None, None).is_ok());
        }
    }

    mod validate_wait_before_run {
        use super::*;

        #[test]
        fn wait_without_sync_fails() {
            let err = validate_wait_before_run(&None, Some(Duration::from_secs(1)))
                .unwrap_err()
                .to_string();
            assert!(err.contains("--wait-before-run requires sync"));
        }

        #[test]
        fn wait_with_sync_is_ok() {
            let sync = Some(SyncConfig::new().with_listen_addr("127.0.0.1:9000"));
            assert!(validate_wait_before_run(&sync, Some(Duration::from_secs(1))).is_ok());
        }
    }

    mod validate_print_counter {
        use super::*;

        #[test]
        fn print_gcounter_without_sync_fails() {
            let err = validate_print_gcounter(&None, &Some("counter:visits".to_string()))
                .unwrap_err()
                .to_string();
            assert!(err.contains("--print-gcounter requires sync"));
        }

        #[test]
        fn print_gcounter_with_sync_is_ok() {
            let sync = Some(SyncConfig::new().with_listen_addr("127.0.0.1:9000"));
            assert!(validate_print_gcounter(&sync, &Some("counter:visits".to_string())).is_ok());
        }

        #[test]
        fn print_pncounter_without_sync_fails() {
            let err = validate_print_pncounter(&None, &Some("inventory:sku-1".to_string()))
                .unwrap_err()
                .to_string();
            assert!(err.contains("--print-pncounter requires sync"));
        }

        #[test]
        fn print_pncounter_with_sync_is_ok() {
            let sync = Some(SyncConfig::new().with_listen_addr("127.0.0.1:9000"));
            assert!(validate_print_pncounter(&sync, &Some("inventory:sku-1".to_string())).is_ok());
        }

        #[test]
        fn print_lww_register_without_sync_fails() {
            let err = validate_print_lww_register(&None, &Some("status:service-a".to_string()))
                .unwrap_err()
                .to_string();
            assert!(err.contains("--print-lww-register requires sync"));
        }

        #[test]
        fn print_lww_register_with_sync_is_ok() {
            let sync = Some(SyncConfig::new().with_listen_addr("127.0.0.1:9000"));
            assert!(
                validate_print_lww_register(&sync, &Some("status:service-a".to_string())).is_ok()
            );
        }

        #[test]
        fn print_lww_map_without_sync_fails() {
            let err = validate_print_lww_map(&None, &Some("settings:service-a".to_string()))
                .unwrap_err()
                .to_string();
            assert!(err.contains("--print-lww-map requires sync"));
        }

        #[test]
        fn print_lww_map_with_sync_is_ok() {
            let sync = Some(SyncConfig::new().with_listen_addr("127.0.0.1:9000"));
            assert!(validate_print_lww_map(&sync, &Some("settings:service-a".to_string())).is_ok());
        }

        #[test]
        fn print_orset_without_sync_fails() {
            let err = validate_print_orset(&None, &Some("tags:doc-1".to_string()))
                .unwrap_err()
                .to_string();
            assert!(err.contains("--print-orset requires sync"));
        }

        #[test]
        fn print_orset_with_sync_is_ok() {
            let sync = Some(SyncConfig::new().with_listen_addr("127.0.0.1:9000"));
            assert!(validate_print_orset(&sync, &Some("tags:doc-1".to_string())).is_ok());
        }

        #[test]
        fn print_rga_without_sync_fails() {
            let err = validate_print_rga(&None, &Some("comments:doc-1".to_string()))
                .unwrap_err()
                .to_string();
            assert!(err.contains("--print-rga requires sync"));
        }

        #[test]
        fn print_rga_with_sync_is_ok() {
            let sync = Some(SyncConfig::new().with_listen_addr("127.0.0.1:9000"));
            assert!(validate_print_rga(&sync, &Some("comments:doc-1".to_string())).is_ok());
        }
    }

    // build_tls_config
    mod build_tls {
        use super::*;

        #[test]
        fn returns_none_without_flags() {
            assert!(build_tls_config(None, None, None, None, false).is_none());
        }

        #[test]
        fn insecure_overrides_everything() {
            let cfg = build_tls_config(None, None, None, None, true).unwrap();
            assert!(cfg.insecure);
            assert!(!cfg.is_enabled());
        }

        #[test]
        fn cert_key_only_no_mtls() {
            let cfg = build_tls_config(p("c.pem"), p("k.pem"), None, None, false).unwrap();
            assert!(cfg.is_enabled());
            assert!(!cfg.is_mtls_enabled());
        }

        #[test]
        fn cert_key_ca_enables_mtls() {
            let cfg = build_tls_config(p("c.pem"), p("k.pem"), p("ca.pem"), None, false).unwrap();
            assert!(cfg.is_mtls_enabled());
        }

        #[test]
        fn allowlist_parsed_with_trim_and_dedup() {
            let list = Some(" abc , def, abc ,  ".to_string());
            let cfg = build_tls_config(p("c.pem"), p("k.pem"), p("ca.pem"), list, false).unwrap();
            let allowed = cfg.allowed_peers.unwrap();
            assert_eq!(allowed.len(), 2);
            assert!(allowed.contains("abc"));
            assert!(allowed.contains("def"));
        }

        #[test]
        fn empty_allowlist_string_does_not_set_allowed_peers() {
            let list = Some(",,,".to_string());
            let cfg = build_tls_config(p("c.pem"), p("k.pem"), p("ca.pem"), list, false).unwrap();
            assert!(cfg.allowed_peers.is_none());
        }
    }

    // build_sync_config
    mod build_sync {
        use super::*;

        #[test]
        fn no_flags_is_none() {
            let r = build_sync_config(None, vec![], None, false, None).unwrap();
            assert!(r.is_none());
        }

        #[test]
        fn listen_alone_is_some() {
            let cfg = build_sync_config(Some("0.0.0.0:9000".into()), vec![], None, false, None)
                .unwrap()
                .expect("sync should be enabled with --listen alone");
            assert!(cfg.is_enabled());
            assert!(cfg.peers.is_empty());
            assert_eq!(cfg.listen_addr.as_deref(), Some("0.0.0.0:9000"));
            assert_eq!(cfg.serialization_format, SerializationFormat::Bincode);
        }

        #[test]
        fn peer_without_listen_is_error() {
            let r = build_sync_config(None, vec!["127.0.0.1:9000".into()], None, false, None);
            assert!(r.is_err(), "peers without --listen must fail loudly");
            let err = r.unwrap_err().to_string();
            assert!(err.contains("requires --listen"), "got: {err}");
        }

        #[test]
        fn limits_config_without_listen_is_error() {
            let r = build_sync_config(None, vec![], None, true, None);
            assert!(r.is_err(), "limits without --listen must fail loudly");
            let err = r.unwrap_err().to_string();
            assert!(err.contains("requires --listen"), "got: {err}");
        }

        #[test]
        fn debug_protocol_without_listen_is_error() {
            let r = build_sync_config(None, vec![], None, false, Some(SerializationFormat::Json));
            assert!(
                r.is_err(),
                "--debug-protocol without --listen must fail loudly"
            );
            let err = r.unwrap_err().to_string();
            assert!(err.contains("requires --listen"), "got: {err}");
        }

        #[test]
        fn debug_protocol_uses_json_wire_format() {
            let cfg = build_sync_config(
                Some("0.0.0.0:9000".into()),
                vec![],
                None,
                false,
                Some(SerializationFormat::Json),
            )
            .unwrap()
            .unwrap();
            assert_eq!(cfg.serialization_format, SerializationFormat::Json);
        }

        #[test]
        fn listen_and_peers_is_some() {
            let cfg = build_sync_config(
                Some("0.0.0.0:9000".into()),
                vec!["a:1".into(), "b:2".into()],
                None,
                false,
                None,
            )
            .unwrap()
            .unwrap();
            assert_eq!(cfg.peers, vec!["a:1".to_string(), "b:2".to_string()]);
            assert_eq!(cfg.listen_addr.as_deref(), Some("0.0.0.0:9000"));
            assert!(cfg.tls.is_none());
        }

        #[test]
        fn tls_is_propagated() {
            let tls = Some(TlsConfig::insecure_dev());
            let cfg = build_sync_config(Some("0.0.0.0:9000".into()), vec![], tls, false, None)
                .unwrap()
                .unwrap();
            assert!(cfg.tls.is_some());
        }
    }

    // clap parsing
    mod clap_parsing {
        use super::*;
        use clap::CommandFactory;

        #[test]
        fn minimal_args() {
            let cli = Cli::try_parse_from(["nx", "run", "x.wasm"]).unwrap();
            match cli {
                Cli::Run {
                    module,
                    peers,
                    verbose,
                    tls_insecure,
                    ..
                } => {
                    assert_eq!(module, PathBuf::from("x.wasm"));
                    assert!(peers.is_empty());
                    assert!(!verbose);
                    assert!(!tls_insecure);
                }
                _ => panic!("expected run command"),
            }
        }

        #[test]
        fn missing_module_fails() {
            assert!(Cli::try_parse_from(["nx", "run"]).is_err());
        }

        #[test]
        fn unknown_flag_fails() {
            assert!(Cli::try_parse_from(["nx", "run", "x.wasm", "--bogus"]).is_err());
        }

        #[test]
        fn sync_prefix_flag_removed() {
            // Regression guard: `--sync-prefix` has been removed in favor of
            // the replicate-by-intent model (nx_sdk::crdt::*).
            assert!(
                Cli::try_parse_from(["nx", "run", "x.wasm", "--sync-prefix", "counter:"]).is_err()
            );
        }

        #[test]
        fn no_subcommand_fails() {
            assert!(Cli::try_parse_from(["nx"]).is_err());
        }

        #[test]
        fn version_flag_prints_crate_version() {
            let err = Cli::try_parse_from(["nx", "--version"]).unwrap_err();
            assert_eq!(err.kind(), clap::error::ErrorKind::DisplayVersion);
            let output = err.to_string();
            assert!(output.contains("nx"), "version output: {output}");
            assert!(
                output.contains(env!("CARGO_PKG_VERSION")),
                "version output: {output}"
            );
        }

        #[test]
        fn version_short_flag_prints_crate_version() {
            let err = Cli::try_parse_from(["nx", "-V"]).unwrap_err();
            assert_eq!(err.kind(), clap::error::ErrorKind::DisplayVersion);
            let output = err.to_string();
            assert!(output.contains("nx"), "version output: {output}");
            assert!(
                output.contains(env!("CARGO_PKG_VERSION")),
                "version output: {output}"
            );
        }

        #[test]
        fn help_includes_version_flag() {
            let help = Cli::command().render_long_help().to_string();
            assert!(help.contains("--version"), "help output: {help}");
        }

        #[test]
        fn repeated_peers_collected() {
            let cli = Cli::try_parse_from([
                "nx", "run", "x.wasm", "--peer", "a:1", "--peer", "b:2", "--peer", "c:3",
            ])
            .unwrap();
            match cli {
                Cli::Run { peers, .. } => assert_eq!(peers, vec!["a:1", "b:2", "c:3"]),
                _ => panic!("expected run command"),
            }
        }

        #[test]
        fn verbose_short_flag() {
            let cli = Cli::try_parse_from(["nx", "run", "x.wasm", "-v"]).unwrap();
            match cli {
                Cli::Run { verbose, .. } => assert!(verbose),
                _ => panic!("expected run command"),
            }
        }

        #[test]
        fn verbose_long_flag() {
            let cli = Cli::try_parse_from(["nx", "run", "x.wasm", "--verbose"]).unwrap();
            match cli {
                Cli::Run { verbose, .. } => assert!(verbose),
                _ => panic!("expected run command"),
            }
        }

        #[test]
        fn datastore_path_parsed() {
            let cli = Cli::try_parse_from(["nx", "run", "x.wasm", "--datastore-path", "/tmp/nx"])
                .unwrap();
            match cli {
                Cli::Run { datastore_path, .. } => {
                    assert_eq!(datastore_path, Some(PathBuf::from("/tmp/nx")));
                }
                _ => panic!("expected run command"),
            }
        }

        #[test]
        fn config_path_parsed() {
            let cli =
                Cli::try_parse_from(["nx", "run", "x.wasm", "--config", "numax.toml"]).unwrap();
            match cli {
                Cli::Run { config, .. } => {
                    assert_eq!(config, Some(PathBuf::from("numax.toml")));
                }
                _ => panic!("expected run command"),
            }
        }

        #[test]
        fn migrate_defaults_parsed() {
            let cli = Cli::try_parse_from(["nx", "migrate"]).unwrap();
            match cli {
                Cli::Migrate {
                    datastore_path,
                    max_records,
                    max_bytes,
                } => {
                    assert_eq!(datastore_path, PathBuf::from("./nx-data"));
                    assert_eq!(max_records, DEFAULT_MIGRATION_BATCH_SIZE);
                    assert_eq!(max_bytes, DEFAULT_MIGRATION_BATCH_BYTES);
                }
                _ => panic!("expected migrate command"),
            }
        }

        #[test]
        fn migrate_custom_limits_parsed() {
            let cli = Cli::try_parse_from([
                "nx",
                "migrate",
                "--datastore-path",
                "/tmp/nx-data",
                "--max-records",
                "7",
                "--max-bytes",
                "8KiB",
            ])
            .unwrap();
            match cli {
                Cli::Migrate {
                    datastore_path,
                    max_records,
                    max_bytes,
                } => {
                    assert_eq!(datastore_path, PathBuf::from("/tmp/nx-data"));
                    assert_eq!(max_records.get(), 7);
                    assert_eq!(max_bytes.get(), 8 * 1024);
                }
                _ => panic!("expected migrate command"),
            }
        }

        #[test]
        fn migrate_rejects_zero_limits() {
            assert!(Cli::try_parse_from(["nx", "migrate", "--max-records", "0"]).is_err());
            assert!(Cli::try_parse_from(["nx", "migrate", "--max-bytes", "0"]).is_err());
        }

        #[test]
        fn config_validate_default_path_parsed() {
            let cli = Cli::try_parse_from(["nx", "config", "validate"]).unwrap();
            match cli {
                Cli::Config {
                    command: ConfigCommand::Validate { config },
                } => assert_eq!(config, PathBuf::from("numax.toml")),
                Cli::Run { .. } => panic!("expected config command"),
                _ => panic!("expected config validate command"),
            }
        }

        #[test]
        fn config_validate_custom_path_parsed() {
            let cli =
                Cli::try_parse_from(["nx", "config", "validate", "--config", "prod.toml"]).unwrap();
            match cli {
                Cli::Config {
                    command: ConfigCommand::Validate { config },
                } => assert_eq!(config, PathBuf::from("prod.toml")),
                Cli::Run { .. } => panic!("expected config command"),
                _ => panic!("expected config validate command"),
            }
        }

        #[test]
        fn config_init_default_output_parsed() {
            let cli = Cli::try_parse_from(["nx", "config", "init"]).unwrap();
            match cli {
                Cli::Config {
                    command: ConfigCommand::Init { output, force },
                } => {
                    assert_eq!(output, PathBuf::from("numax.toml"));
                    assert!(!force);
                }
                Cli::Run { .. } => panic!("expected config command"),
                _ => panic!("expected config init command"),
            }
        }

        #[test]
        fn config_init_custom_output_and_force_parsed() {
            let cli =
                Cli::try_parse_from(["nx", "config", "init", "--output", "prod.toml", "--force"])
                    .unwrap();
            match cli {
                Cli::Config {
                    command: ConfigCommand::Init { output, force },
                } => {
                    assert_eq!(output, PathBuf::from("prod.toml"));
                    assert!(force);
                }
                Cli::Run { .. } => panic!("expected config command"),
                _ => panic!("expected config init command"),
            }
        }

        #[test]
        fn config_show_effective_parsed() {
            let cli = Cli::try_parse_from([
                "nx",
                "config",
                "show",
                "--config",
                "prod.toml",
                "--effective",
            ])
            .unwrap();
            match cli {
                Cli::Config {
                    command: ConfigCommand::Show { config, effective },
                } => {
                    assert_eq!(config, PathBuf::from("prod.toml"));
                    assert!(effective);
                }
                Cli::Run { .. } => panic!("expected config command"),
                _ => panic!("expected config show command"),
            }
        }

        #[test]
        fn observability_flags_parsed() {
            let cli = Cli::try_parse_from([
                "nx",
                "run",
                "x.wasm",
                "--observability-listen",
                "127.0.0.1:9100",
                "--log-level",
                "debug",
                "--log-format",
                "json",
            ])
            .unwrap();
            match cli {
                Cli::Run {
                    observability_listen,
                    log_level,
                    log_format,
                    ..
                } => {
                    assert_eq!(observability_listen.as_deref(), Some("127.0.0.1:9100"));
                    assert_eq!(log_level.as_deref(), Some("debug"));
                    assert_eq!(log_format, Some(LogFormat::Json));
                }
                _ => panic!("expected run command"),
            }
        }

        #[test]
        fn tokio_console_flag_parsed() {
            let cli = Cli::try_parse_from(["nx", "run", "x.wasm", "--tokio-console"]).unwrap();
            match cli {
                Cli::Run { tokio_console, .. } => assert!(tokio_console),
                _ => panic!("expected run command"),
            }
        }

        #[test]
        fn listen_parsed() {
            let cli =
                Cli::try_parse_from(["nx", "run", "x.wasm", "--listen", "127.0.0.1:9000"]).unwrap();
            match cli {
                Cli::Run { listen, .. } => {
                    assert_eq!(listen.as_deref(), Some("127.0.0.1:9000"));
                }
                _ => panic!("expected run command"),
            }
        }

        #[test]
        fn debug_protocol_parsed() {
            let cli = Cli::try_parse_from(["nx", "run", "x.wasm", "--debug-protocol"]).unwrap();
            match cli {
                Cli::Run { debug_protocol, .. } => assert!(debug_protocol),
                _ => panic!("expected run command"),
            }
        }

        #[test]
        fn settle_for_parsed() {
            let cli = Cli::try_parse_from(["nx", "run", "x.wasm", "--settle-for", "5s"]).unwrap();
            match cli {
                Cli::Run { settle_for, .. } => {
                    assert_eq!(settle_for, Some(Duration::from_secs(5)));
                }
                _ => panic!("expected run command"),
            }
        }

        #[test]
        fn invalid_settle_for_fails() {
            assert!(Cli::try_parse_from(["nx", "run", "x.wasm", "--settle-for", "later"]).is_err());
        }

        #[test]
        fn wait_before_run_parsed() {
            let cli =
                Cli::try_parse_from(["nx", "run", "x.wasm", "--wait-before-run", "500ms"]).unwrap();
            match cli {
                Cli::Run {
                    wait_before_run, ..
                } => {
                    assert_eq!(wait_before_run, Some(Duration::from_millis(500)));
                }
                _ => panic!("expected run command"),
            }
        }

        #[test]
        fn print_gcounter_parsed() {
            let cli =
                Cli::try_parse_from(["nx", "run", "x.wasm", "--print-gcounter", "counter:visits"])
                    .unwrap();
            match cli {
                Cli::Run { print_gcounter, .. } => {
                    assert_eq!(print_gcounter.as_deref(), Some("counter:visits"));
                }
                _ => panic!("expected run command"),
            }
        }

        #[test]
        fn print_pncounter_parsed() {
            let cli = Cli::try_parse_from([
                "nx",
                "run",
                "x.wasm",
                "--print-pncounter",
                "inventory:sku-1",
            ])
            .unwrap();
            match cli {
                Cli::Run {
                    print_pncounter, ..
                } => {
                    assert_eq!(print_pncounter.as_deref(), Some("inventory:sku-1"));
                }
                _ => panic!("expected run command"),
            }
        }

        #[test]
        fn print_lww_register_parsed() {
            let cli = Cli::try_parse_from([
                "nx",
                "run",
                "x.wasm",
                "--print-lww-register",
                "status:service-a",
            ])
            .unwrap();
            match cli {
                Cli::Run {
                    print_lww_register, ..
                } => {
                    assert_eq!(print_lww_register.as_deref(), Some("status:service-a"));
                }
                _ => panic!("expected run command"),
            }
        }

        #[test]
        fn print_lww_map_parsed() {
            let cli = Cli::try_parse_from([
                "nx",
                "run",
                "x.wasm",
                "--print-lww-map",
                "settings:service-a",
            ])
            .unwrap();
            match cli {
                Cli::Run { print_lww_map, .. } => {
                    assert_eq!(print_lww_map.as_deref(), Some("settings:service-a"));
                }
                _ => panic!("expected run command"),
            }
        }

        #[test]
        fn print_orset_parsed() {
            let cli = Cli::try_parse_from(["nx", "run", "x.wasm", "--print-orset", "tags:doc-1"])
                .unwrap();
            match cli {
                Cli::Run { print_orset, .. } => {
                    assert_eq!(print_orset.as_deref(), Some("tags:doc-1"));
                }
                _ => panic!("expected run command"),
            }
        }

        #[test]
        fn print_rga_parsed() {
            let cli = Cli::try_parse_from(["nx", "run", "x.wasm", "--print-rga", "comments:doc-1"])
                .unwrap();
            match cli {
                Cli::Run { print_rga, .. } => {
                    assert_eq!(print_rga.as_deref(), Some("comments:doc-1"));
                }
                _ => panic!("expected run command"),
            }
        }

        #[test]
        fn shutdown_timeout_parsed() {
            let cli =
                Cli::try_parse_from(["nx", "run", "x.wasm", "--shutdown-timeout", "10s"]).unwrap();
            match cli {
                Cli::Run {
                    shutdown_timeout, ..
                } => {
                    assert_eq!(shutdown_timeout, Some(Duration::from_secs(10)));
                }
                _ => panic!("expected run command"),
            }
        }

        #[test]
        fn invalid_shutdown_timeout_fails() {
            assert!(
                Cli::try_parse_from(["nx", "run", "x.wasm", "--shutdown-timeout", "nope"]).is_err()
            );
        }

        #[test]
        fn clap_parses_all_tls_flags() {
            let cli = Cli::try_parse_from([
                "nx",
                "run",
                "x.wasm",
                "--tls-cert",
                "c.pem",
                "--tls-key",
                "k.pem",
                "--tls-ca",
                "ca.pem",
                "--allowed-peers",
                "abc,def",
            ])
            .expect("parse must succeed");

            match cli {
                Cli::Run {
                    tls_cert,
                    tls_key,
                    tls_ca,
                    allowed_peers,
                    tls_insecure,
                    ..
                } => {
                    assert_eq!(tls_cert.unwrap().to_string_lossy(), "c.pem");
                    assert_eq!(tls_key.unwrap().to_string_lossy(), "k.pem");
                    assert_eq!(tls_ca.unwrap().to_string_lossy(), "ca.pem");
                    assert_eq!(allowed_peers.as_deref(), Some("abc,def"));
                    assert!(!tls_insecure);
                }
                _ => panic!("expected run command"),
            }
        }

        #[test]
        fn tls_insecure_flag_parsed() {
            let cli = Cli::try_parse_from(["nx", "run", "x.wasm", "--tls-insecure"]).unwrap();
            match cli {
                Cli::Run { tls_insecure, .. } => assert!(tls_insecure),
                _ => panic!("expected run command"),
            }
        }
    }
}
