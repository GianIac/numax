use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Result, bail};
use clap::Parser;
use nx_core::runtime::{Runtime, RuntimeConfig};
use nx_core::{SyncConfig, TlsConfig};
use tracing::{info, warn};

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

        /// Enable verbose logging
        #[arg(short, long)]
        verbose: bool,

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
            listen,
            peers,
            settle_for,
            wait_before_run,
            print_gcounter,
            verbose,
            tls_cert,
            tls_key,
            tls_ca,
            allowed_peers,
            tls_insecure,
        } => {
            // Setup logging
            let log_level = if verbose { "debug" } else { "info" };
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level)),
                )
                .init();

            // Validate TLS flag combinations
            validate_tls_flags(&tls_cert, &tls_key, &tls_ca, &allowed_peers, tls_insecure)?;

            // Build TlsConfig (if any TLS-related flag was provided)
            let tls = build_tls_config(tls_cert, tls_key, tls_ca, allowed_peers, tls_insecure);

            // Build SyncConfig (if sync flags were provided)
            let sync = build_sync_config(listen, peers, tls)?;
            validate_settle_mode(&sync, settle_for)?;
            validate_wait_before_run(&sync, wait_before_run)?;
            validate_print_gcounter(&sync, &print_gcounter)?;

            // Read the wasm module
            let bytes = fs::read(&module)?;

            // Build the runtime config
            let mut cfg = RuntimeConfig::default();
            if let Some(p) = datastore_path {
                cfg.datastore_path = p;
            }
            if let Some(s) = sync {
                info!(
                    listen = ?s.listen_addr,
                    peers = ?s.peers,
                    tls = s.tls.is_some(),
                    "sync enabled"
                );
                cfg.sync = Some(s);
            }

            let mut rt = Runtime::new(cfg)?;
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
            rt.shutdown().await?;
        }
    }

    Ok(())
}

/// Validate that the TLS-related CLI flags form a coherent combination.
fn validate_tls_flags(
    tls_cert: &Option<PathBuf>,
    tls_key: &Option<PathBuf>,
    tls_ca: &Option<PathBuf>,
    allowed_peers: &Option<String>,
    tls_insecure: bool,
) -> Result<()> {
    if tls_cert.is_some() ^ tls_key.is_some() {
        bail!("--tls-cert and --tls-key must be provided together");
    }
    if tls_insecure && (tls_ca.is_some() || allowed_peers.is_some()) {
        bail!("--tls-insecure is mutually exclusive with --tls-ca and --allowed-peers");
    }
    if allowed_peers.is_some() && tls_ca.is_none() && !tls_insecure {
        bail!("--allowed-peers requires --tls-ca (peers must be authenticated via mTLS)");
    }
    Ok(())
}

fn parse_duration(input: &str) -> Result<Duration, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("expected duration like 500ms, 5s, or 2m".to_string());
    }

    let (number, multiplier) = if let Some(number) = input.strip_suffix("ms") {
        (number, 1)
    } else if let Some(number) = input.strip_suffix('s') {
        (number, 1_000)
    } else if let Some(number) = input.strip_suffix('m') {
        (number, 60_000)
    } else {
        (input, 1_000)
    };

    let amount = number
        .parse::<u64>()
        .map_err(|_| "expected duration like 500ms, 5s, or 2m".to_string())?;
    if amount == 0 {
        return Err("duration must be greater than zero".to_string());
    }

    let millis = amount
        .checked_mul(multiplier)
        .ok_or_else(|| "duration is too large".to_string())?;

    Ok(Duration::from_millis(millis))
}

fn validate_settle_mode(sync: &Option<SyncConfig>, settle_for: Option<Duration>) -> Result<()> {
    if settle_for.is_some() && sync.is_none() {
        bail!("--settle-for requires sync to be enabled with --listen");
    }

    Ok(())
}

fn validate_wait_before_run(
    sync: &Option<SyncConfig>,
    wait_before_run: Option<Duration>,
) -> Result<()> {
    if wait_before_run.is_some() && sync.is_none() {
        bail!("--wait-before-run requires sync to be enabled with --listen");
    }

    Ok(())
}

fn validate_print_gcounter(
    sync: &Option<SyncConfig>,
    print_gcounter: &Option<String>,
) -> Result<()> {
    if print_gcounter.is_some() && sync.is_none() {
        bail!("--print-gcounter requires sync to be enabled with --listen");
    }

    Ok(())
}

/// Build TlsConfig from CLI flags.
fn build_tls_config(
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    tls_ca: Option<PathBuf>,
    allowed_peers: Option<String>,
    tls_insecure: bool,
) -> Option<TlsConfig> {
    if tls_insecure {
        warn!("--tls-insecure enabled: peer verification disabled, DO NOT USE IN PRODUCTION");
        return Some(TlsConfig::insecure_dev());
    }

    let (cert, key) = match (tls_cert, tls_key) {
        (Some(c), Some(k)) => (c, k),
        _ => return None,
    };

    let cert_s = cert.to_string_lossy().into_owned();
    let key_s = key.to_string_lossy().into_owned();

    let mut cfg = match tls_ca {
        Some(ca) => TlsConfig::new(cert_s, key_s, ca.to_string_lossy().into_owned()),
        None => TlsConfig {
            cert_path: Some(cert_s),
            key_path: Some(key_s),
            ca_path: None,
            allowed_peers: None,
            insecure: false,
        },
    };

    if let Some(list) = allowed_peers {
        let set: HashSet<String> = list
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if !set.is_empty() {
            cfg = cfg.with_allowed_peers(set);
        }
    }

    Some(cfg)
}

/// Build a SyncConfig from CLI flags.
fn build_sync_config(
    listen: Option<String>,
    peers: Vec<String>,
    tls: Option<TlsConfig>,
) -> Result<Option<SyncConfig>> {
    if listen.is_none() && peers.is_empty() {
        return Ok(None);
    }

    if listen.is_none() {
        bail!(
            "--peer requires --listen: dialer-only mode is not yet supported. \
             Pass --listen <addr> to enable sync."
        );
    }

    let mut cfg = SyncConfig::new();
    if let Some(addr) = listen {
        cfg = cfg.with_listen_addr(addr);
    }
    for p in peers {
        cfg = cfg.with_peer(p);
    }
    if let Some(t) = tls {
        cfg = cfg.with_tls(t);
    }

    debug_assert!(cfg.is_enabled());
    Ok(Some(cfg))
}

#[cfg(test)]
mod tests {
    use super::*;
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
        fn print_without_sync_fails() {
            let err = validate_print_gcounter(&None, &Some("counter:visits".to_string()))
                .unwrap_err()
                .to_string();
            assert!(err.contains("--print-gcounter requires sync"));
        }

        #[test]
        fn print_with_sync_is_ok() {
            let sync = Some(SyncConfig::new().with_listen_addr("127.0.0.1:9000"));
            assert!(validate_print_gcounter(&sync, &Some("counter:visits".to_string())).is_ok());
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
            let r = build_sync_config(None, vec![], None).unwrap();
            assert!(r.is_none());
        }

        #[test]
        fn listen_alone_is_some() {
            let cfg = build_sync_config(Some("0.0.0.0:9000".into()), vec![], None)
                .unwrap()
                .expect("sync should be enabled with --listen alone");
            assert!(cfg.is_enabled());
            assert!(cfg.peers.is_empty());
            assert_eq!(cfg.listen_addr.as_deref(), Some("0.0.0.0:9000"));
        }

        #[test]
        fn peer_without_listen_is_error() {
            let r = build_sync_config(None, vec!["127.0.0.1:9000".into()], None);
            assert!(r.is_err(), "peers without --listen must fail loudly");
            let err = r.unwrap_err().to_string();
            assert!(err.contains("--peer requires --listen"), "got: {err}");
        }

        #[test]
        fn listen_and_peers_is_some() {
            let cfg = build_sync_config(
                Some("0.0.0.0:9000".into()),
                vec!["a:1".into(), "b:2".into()],
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
            let cfg = build_sync_config(Some("0.0.0.0:9000".into()), vec![], tls)
                .unwrap()
                .unwrap();
            assert!(cfg.tls.is_some());
        }
    }

    // clap parsing
    mod clap_parsing {
        use super::*;

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
        fn repeated_peers_collected() {
            let cli = Cli::try_parse_from([
                "nx", "run", "x.wasm", "--peer", "a:1", "--peer", "b:2", "--peer", "c:3",
            ])
            .unwrap();
            match cli {
                Cli::Run { peers, .. } => assert_eq!(peers, vec!["a:1", "b:2", "c:3"]),
            }
        }

        #[test]
        fn verbose_short_flag() {
            let cli = Cli::try_parse_from(["nx", "run", "x.wasm", "-v"]).unwrap();
            match cli {
                Cli::Run { verbose, .. } => assert!(verbose),
            }
        }

        #[test]
        fn verbose_long_flag() {
            let cli = Cli::try_parse_from(["nx", "run", "x.wasm", "--verbose"]).unwrap();
            match cli {
                Cli::Run { verbose, .. } => assert!(verbose),
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
            }
        }

        #[test]
        fn settle_for_parsed() {
            let cli = Cli::try_parse_from(["nx", "run", "x.wasm", "--settle-for", "5s"]).unwrap();
            match cli {
                Cli::Run { settle_for, .. } => {
                    assert_eq!(settle_for, Some(Duration::from_secs(5)));
                }
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
            }
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
            }
        }

        #[test]
        fn tls_insecure_flag_parsed() {
            let cli = Cli::try_parse_from(["nx", "run", "x.wasm", "--tls-insecure"]).unwrap();
            match cli {
                Cli::Run { tls_insecure, .. } => assert!(tls_insecure),
            }
        }
    }
}
