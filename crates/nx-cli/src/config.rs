use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{env, fs};

use anyhow::{Context, Result, bail};
use clap::ValueEnum;
use nx_api::{DEFAULT_MANAGEMENT_LISTEN, DEFAULT_MANAGEMENT_REQUEST_TIMEOUT, ManagementConfig};
use nx_core::runtime::RuntimeConfig;
use nx_core::{
    BootstrapDiscoverySettings, DnsSrvDiscoverySettings, FileDiscoverySettings,
    MdnsDiscoverySettings, ObservabilityConfig, RuntimeDiscoveryConfig, RuntimeDiscoveryMode,
    SerializationFormat, SyncConfig, TlsConfig,
};
use serde::Deserialize;
use tracing::warn;
#[cfg(feature = "tokio-console")]
use tracing_subscriber::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub(crate) enum LogFormat {
    Text,
    Json,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct RunFileConfig {
    pub(crate) network: Option<NetworkFileConfig>,
    pub(crate) tls: Option<TlsFileConfig>,
    pub(crate) storage: Option<StorageFileConfig>,
    pub(crate) limits: Option<LimitsFileConfig>,
    pub(crate) observability: Option<ObservabilityFileConfig>,
    pub(crate) management: Option<ManagementFileConfig>,
    pub(crate) discovery: Option<DiscoveryFileConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NetworkFileConfig {
    pub(crate) listen: Option<String>,
    pub(crate) peers: Option<Vec<String>>,
    pub(crate) serialization_format: Option<WireSerializationFormat>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum WireSerializationFormat {
    Bincode,
    Json,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TlsFileConfig {
    pub(crate) cert: Option<PathBuf>,
    pub(crate) key: Option<PathBuf>,
    pub(crate) ca: Option<PathBuf>,
    pub(crate) allowed_peers: Option<Vec<String>>,
    pub(crate) insecure: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StorageFileConfig {
    pub(crate) datastore_path: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LimitsFileConfig {
    pub(crate) max_peers: Option<usize>,
    pub(crate) queued_ops_limit: Option<usize>,
    pub(crate) op_log_limit: Option<usize>,
    pub(crate) seen_ops_limit: Option<usize>,
    pub(crate) max_message_size: Option<String>,
    pub(crate) socket_timeout_secs: Option<u64>,
    pub(crate) reconnect_initial_delay: Option<String>,
    pub(crate) reconnect_max_delay: Option<String>,
    pub(crate) peer_dead_after_failures: Option<u32>,
    pub(crate) anti_entropy_interval: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObservabilityFileConfig {
    pub(crate) listen: Option<String>,
    pub(crate) log_level: Option<String>,
    pub(crate) log_format: Option<LogFormat>,
    pub(crate) request_timeout_secs: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ManagementFileConfig {
    pub(crate) listen: Option<String>,
    pub(crate) token_file: Option<PathBuf>,
    pub(crate) allow_non_loopback: Option<bool>,
    pub(crate) request_timeout_secs: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DiscoveryFileConfig {
    pub(crate) mode: Option<DiscoveryMode>,
    pub(crate) cluster_id: Option<String>,
    pub(crate) advertised_endpoint: Option<String>,
    pub(crate) max_candidates: Option<usize>,
    pub(crate) seeds: Option<Vec<String>>,
    pub(crate) refresh_interval: Option<String>,
    pub(crate) retry_initial: Option<String>,
    pub(crate) retry_max: Option<String>,
    pub(crate) stale_after: Option<String>,
    pub(crate) max_seeds: Option<usize>,
    pub(crate) instance_name: Option<String>,
    pub(crate) max_instances: Option<usize>,
    pub(crate) service_name: Option<String>,
    pub(crate) retry_interval: Option<String>,
    pub(crate) max_refresh_interval: Option<String>,
    pub(crate) path: Option<PathBuf>,
    pub(crate) poll_interval: Option<String>,
    pub(crate) max_file_bytes: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum DiscoveryMode {
    Static,
    Bootstrap,
    Mdns,
    DnsSrv,
    File,
}

#[derive(Debug, Default)]
pub(crate) struct RunCliOptions {
    pub(crate) datastore_path: Option<PathBuf>,
    pub(crate) listen: Option<String>,
    pub(crate) peers: Vec<String>,
    pub(crate) observability_listen: Option<String>,
    pub(crate) tls_cert: Option<PathBuf>,
    pub(crate) tls_key: Option<PathBuf>,
    pub(crate) tls_ca: Option<PathBuf>,
    pub(crate) allowed_peers: Option<String>,
    pub(crate) tls_insecure: bool,
    pub(crate) debug_protocol: bool,
    pub(crate) verbose: bool,
    pub(crate) log_level: Option<String>,
    pub(crate) log_format: Option<LogFormat>,
    pub(crate) discovery_mode: Option<DiscoveryMode>,
    pub(crate) bootstrap_seeds: Vec<String>,
    pub(crate) mdns_instance: Option<String>,
    pub(crate) dns_srv_name: Option<String>,
    pub(crate) peer_file: Option<PathBuf>,
}

#[derive(Debug)]
pub(crate) struct EffectiveRunConfig {
    pub(crate) datastore_path: Option<PathBuf>,
    pub(crate) sync: Option<SyncConfig>,
    pub(crate) observability: Option<ObservabilityConfig>,
    pub(crate) management: Option<ManagementConfig>,
    pub(crate) discovery: RuntimeDiscoveryConfig,
    pub(crate) log_level: String,
    pub(crate) log_format: LogFormat,
}

impl EffectiveRunConfig {
    pub(crate) fn resolve(cli: RunCliOptions, file_config: &RunFileConfig) -> Result<Self> {
        let env_config = EnvRunConfig::from_env()?;
        Self::resolve_with_env(cli, env_config, file_config)
    }

    pub(crate) fn resolve_with_env(
        cli: RunCliOptions,
        env_config: EnvRunConfig,
        file_config: &RunFileConfig,
    ) -> Result<Self> {
        let env_has_sync_inputs = env_config.has_sync_inputs();
        let mut discovery =
            resolve_discovery_config(&cli, &env_config, file_config.discovery.as_ref())?;
        let management = build_management_config(&env_config, file_config.management.as_ref())?;
        let datastore_path = cli
            .datastore_path
            .or(env_config.datastore_path)
            .or_else(|| {
                file_config
                    .storage
                    .as_ref()
                    .and_then(|storage| storage.datastore_path.clone())
            });

        let log_level = resolve_log_level(
            cli.verbose,
            cli.log_level.or(env_config.log_level),
            file_config.observability.as_ref(),
        )?;
        let log_format = resolve_log_format(
            cli.log_format.or(env_config.log_format),
            file_config.observability.as_ref(),
        );

        let tls_cert = cli
            .tls_cert
            .or(env_config.tls_cert)
            .or_else(|| file_config.tls.as_ref().and_then(|tls| tls.cert.clone()));
        let tls_key = cli
            .tls_key
            .or(env_config.tls_key)
            .or_else(|| file_config.tls.as_ref().and_then(|tls| tls.key.clone()));
        let tls_ca = cli
            .tls_ca
            .or(env_config.tls_ca)
            .or_else(|| file_config.tls.as_ref().and_then(|tls| tls.ca.clone()));
        let allowed_peers = cli
            .allowed_peers
            .or(env_config.allowed_peers)
            .or_else(|| file_allowed_peers_as_csv(file_config.tls.as_ref()));
        let tls_insecure = cli.tls_insecure
            || env_config.tls_insecure.unwrap_or(false)
            || file_config
                .tls
                .as_ref()
                .and_then(|tls| tls.insecure)
                .unwrap_or(false);

        validate_tls_flags(&tls_cert, &tls_key, &tls_ca, &allowed_peers, tls_insecure)?;
        let tls = build_tls_config(tls_cert, tls_key, tls_ca, allowed_peers, tls_insecure);

        let listen = cli.listen.or(env_config.listen).or_else(|| {
            file_config
                .network
                .as_ref()
                .and_then(|network| network.listen.clone())
        });
        let peers = if !cli.peers.is_empty() {
            cli.peers
        } else if let Some(peers) = env_config.peers {
            peers
        } else {
            file_config
                .network
                .as_ref()
                .and_then(|network| network.peers.clone())
                .unwrap_or_default()
        };
        discovery.max_candidates = discovery.max_candidates.max(peers.len());
        let serialization_format = if cli.debug_protocol {
            Some(SerializationFormat::Json)
        } else if let Some(format) = env_config.serialization_format {
            Some(format)
        } else {
            file_config
                .network
                .as_ref()
                .and_then(|network| network.serialization_format)
                .map(Into::into)
        };

        let force_sync = file_config.limits.is_some()
            || file_config.tls.is_some()
            || file_config.network.is_some()
            || env_has_sync_inputs
            || !matches!(discovery.mode, RuntimeDiscoveryMode::Static)
            || tls.is_some()
            || serialization_format.is_some();
        let sync = build_sync_config(listen, peers, tls, force_sync, serialization_format)?
            .map(|sync| apply_limit_config(sync, file_config.limits.as_ref()))
            .transpose()?;

        let observability = build_observability_config(
            cli.observability_listen.or(env_config.observability_listen),
            file_config.observability.as_ref(),
        )?;
        Ok(Self {
            datastore_path,
            sync,
            observability,
            management,
            discovery,
            log_level,
            log_format,
        })
    }

    pub(crate) fn render_effective_toml(&self) -> String {
        let default_runtime = RuntimeConfig::default();
        let datastore_path = self
            .datastore_path
            .as_ref()
            .unwrap_or(&default_runtime.datastore_path);
        let mut out = String::new();

        out.push_str("[storage]\n");
        out.push_str(&format!(
            "datastore_path = \"{}\"\n\n",
            escape_toml(&datastore_path.to_string_lossy())
        ));

        out.push_str("[network]\n");
        match &self.sync {
            Some(sync) => {
                out.push_str("enabled = true\n");
                out.push_str(&format!(
                    "listen = \"{}\"\n",
                    escape_toml(sync.listen_addr.as_deref().unwrap_or(""))
                ));
                out.push_str(&format!("peers = {}\n", render_string_list(&sync.peers)));
                out.push_str(&format!(
                    "serialization_format = \"{}\"\n\n",
                    render_serialization_format(sync.serialization_format)
                ));
            }
            None => {
                out.push_str("enabled = false\n");
                out.push_str("peers = []\n");
                out.push_str("serialization_format = \"bincode\"\n\n");
            }
        }

        out.push_str("[tls]\n");
        match self.sync.as_ref().and_then(|sync| sync.tls.as_ref()) {
            Some(tls) => {
                out.push_str(&format!("enabled = {}\n", tls.is_enabled()));
                out.push_str(&format!("insecure = {}\n", tls.insecure));
                render_optional_string(&mut out, "cert", tls.cert_path.as_deref());
                render_optional_string(&mut out, "key", tls.key_path.as_deref());
                render_optional_string(&mut out, "ca", tls.ca_path.as_deref());
                let allowed_peers = tls
                    .allowed_peers
                    .as_ref()
                    .map(|peers| {
                        let mut peers = peers.iter().cloned().collect::<Vec<_>>();
                        peers.sort();
                        peers
                    })
                    .unwrap_or_default();
                out.push_str(&format!(
                    "allowed_peers = {}\n\n",
                    render_string_list(&allowed_peers)
                ));
            }
            None => {
                out.push_str("enabled = false\n");
                out.push_str("insecure = false\n");
                out.push_str("allowed_peers = []\n\n");
            }
        }

        out.push_str("[observability]\n");
        match &self.observability {
            Some(observability) => {
                out.push_str("enabled = true\n");
                out.push_str(&format!(
                    "listen = \"{}\"\n",
                    escape_toml(&observability.listen_addr)
                ));
                out.push_str(&format!(
                    "request_timeout = \"{}\"\n",
                    render_duration(observability.request_timeout)
                ));
            }
            None => {
                out.push_str("enabled = false\n");
            }
        }
        out.push_str(&format!(
            "log_level = \"{}\"\n",
            escape_toml(&self.log_level)
        ));
        out.push_str(&format!(
            "log_format = \"{}\"\n\n",
            render_log_format(self.log_format)
        ));

        out.push_str("[management]\n");
        match &self.management {
            Some(management) => {
                out.push_str("enabled = true\n");
                out.push_str(&format!("listen = \"{}\"\n", management.listen_addr()));
                out.push_str("token_configured = true\n");
                out.push_str(&format!(
                    "allow_non_loopback = {}\n",
                    management.allows_non_loopback()
                ));
                out.push_str(&format!(
                    "request_timeout = \"{}\"\n",
                    render_duration(management.request_timeout())
                ));
            }
            None => out.push_str("enabled = false\n"),
        }
        out.push('\n');

        out.push_str("[limits]\n");
        if let Some(sync) = &self.sync {
            out.push_str(&format!("max_peers = {}\n", sync.max_peers));
            out.push_str(&format!("queued_ops_limit = {}\n", sync.queued_ops_limit));
            out.push_str(&format!("op_log_limit = {}\n", sync.op_log_limit));
            out.push_str(&format!("seen_ops_limit = {}\n", sync.seen_ops_limit));
            out.push_str(&format!("max_message_size = {}\n", sync.max_message_size));
            out.push_str(&format!(
                "socket_timeout = \"{}\"\n",
                render_duration(sync.socket_timeout)
            ));
            out.push_str(&format!(
                "reconnect_initial_delay = \"{}\"\n",
                render_duration(sync.reconnect_initial_delay)
            ));
            out.push_str(&format!(
                "reconnect_max_delay = \"{}\"\n",
                render_duration(sync.reconnect_max_delay)
            ));
            out.push_str(&format!(
                "peer_dead_after_failures = {}\n",
                sync.peer_dead_after_failures
            ));
            out.push_str(&format!(
                "anti_entropy_interval = \"{}\"\n\n",
                render_duration(sync.anti_entropy_interval)
            ));
        } else {
            out.push_str("# Sync limits are inactive because sync is disabled.\n\n");
        }

        out.push_str("[discovery]\n");
        render_discovery_config(&mut out, &self.discovery);
        out
    }
}

pub(crate) const CONFIG_TEMPLATE: &str = r#"# Numax configuration file.
# Precedence: CLI flags > NX_* environment variables > this file > defaults.

[storage]
datastore_path = "./nx-data"

[network]
listen = "0.0.0.0:9000"
peers = []
serialization_format = "bincode"

[tls]
# cert = "./certs/node.pem"
# key = "./certs/node-key.pem"
# ca = "./certs/ca.pem"
allowed_peers = []
insecure = false

[observability]
# listen = "127.0.0.1:9100"
log_level = "info"
log_format = "text"
request_timeout_secs = 5

[management]
# listen = "127.0.0.1:9102"
# token_file = "./management.token"
allow_non_loopback = false
request_timeout_secs = 10

[limits]
max_peers = 64
queued_ops_limit = 10000
op_log_limit = 10000
seen_ops_limit = 100000
max_message_size = "16MiB"
socket_timeout_secs = 30
reconnect_initial_delay = "500ms"
reconnect_max_delay = "30s"
peer_dead_after_failures = 3
anti_entropy_interval = "30s"

[discovery]
mode = "static"
# cluster_id = "default"
# advertised_endpoint = "127.0.0.1:9000"
# max_candidates = 1024
# Bootstrap: seeds, refresh_interval, retry_initial, retry_max, stale_after, max_seeds
# mDNS: instance_name, max_instances
# DNS-SRV: service_name, retry_interval, max_refresh_interval
# File: path, poll_interval, max_file_bytes
"#;

pub(crate) fn init_config_file(path: &Path, force: bool) -> Result<()> {
    if path.exists() && !force {
        bail!(
            "{} already exists; pass --force to overwrite it",
            path.display()
        );
    }

    fs::write(path, CONFIG_TEMPLATE)?;
    Ok(())
}

#[derive(Debug, Default)]
pub(crate) struct EnvRunConfig {
    pub(crate) datastore_path: Option<PathBuf>,
    pub(crate) listen: Option<String>,
    pub(crate) peers: Option<Vec<String>>,
    pub(crate) observability_listen: Option<String>,
    pub(crate) management_listen: Option<String>,
    pub(crate) management_token: Option<SecretValue>,
    pub(crate) management_token_file: Option<PathBuf>,
    pub(crate) management_allow_non_loopback: Option<bool>,
    pub(crate) management_request_timeout_secs: Option<u64>,
    pub(crate) tls_cert: Option<PathBuf>,
    pub(crate) tls_key: Option<PathBuf>,
    pub(crate) tls_ca: Option<PathBuf>,
    pub(crate) allowed_peers: Option<String>,
    pub(crate) tls_insecure: Option<bool>,
    pub(crate) serialization_format: Option<SerializationFormat>,
    pub(crate) log_level: Option<String>,
    pub(crate) log_format: Option<LogFormat>,
    pub(crate) discovery_mode: Option<DiscoveryMode>,
    pub(crate) discovery_cluster_id: Option<String>,
    pub(crate) discovery_advertised_endpoint: Option<String>,
    pub(crate) discovery_max_candidates: Option<usize>,
    pub(crate) discovery_seeds: Option<Vec<String>>,
    pub(crate) discovery_refresh_interval: Option<String>,
    pub(crate) discovery_retry_initial: Option<String>,
    pub(crate) discovery_retry_max: Option<String>,
    pub(crate) discovery_stale_after: Option<String>,
    pub(crate) discovery_max_seeds: Option<usize>,
    pub(crate) discovery_instance_name: Option<String>,
    pub(crate) discovery_max_instances: Option<usize>,
    pub(crate) discovery_service_name: Option<String>,
    pub(crate) discovery_retry_interval: Option<String>,
    pub(crate) discovery_max_refresh_interval: Option<String>,
    pub(crate) discovery_file: Option<PathBuf>,
    pub(crate) discovery_poll_interval: Option<String>,
    pub(crate) discovery_max_file_bytes: Option<String>,
}

impl EnvRunConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            datastore_path: env_path("NX_DATASTORE_PATH"),
            listen: env_non_empty("NX_LISTEN")?,
            peers: env_peers()?,
            observability_listen: env_non_empty("NX_OBSERVABILITY_LISTEN")?,
            management_listen: env_non_empty("NX_MANAGEMENT_LISTEN")?,
            management_token: env_non_empty("NX_MANAGEMENT_TOKEN")?.map(SecretValue),
            management_token_file: env_path("NX_MANAGEMENT_TOKEN_FILE"),
            management_allow_non_loopback: env_bool("NX_MANAGEMENT_ALLOW_NON_LOOPBACK")?,
            management_request_timeout_secs: env_u64("NX_MANAGEMENT_REQUEST_TIMEOUT_SECS")?,
            tls_cert: env_path("NX_TLS_CERT"),
            tls_key: env_path("NX_TLS_KEY"),
            tls_ca: env_path("NX_TLS_CA"),
            allowed_peers: env_non_empty("NX_ALLOWED_PEERS")?,
            tls_insecure: env_bool("NX_TLS_INSECURE")?,
            serialization_format: env_serialization_format()?,
            log_level: env_non_empty("NX_LOG_LEVEL")?,
            log_format: env_log_format()?,
            discovery_mode: env_discovery_mode()?,
            discovery_cluster_id: env_non_empty("NX_DISCOVERY_CLUSTER_ID")?,
            discovery_advertised_endpoint: env_non_empty("NX_DISCOVERY_ADVERTISED_ENDPOINT")?,
            discovery_max_candidates: env_usize("NX_DISCOVERY_MAX_CANDIDATES")?,
            discovery_seeds: env_csv("NX_DISCOVERY_SEEDS")?,
            discovery_refresh_interval: env_non_empty("NX_DISCOVERY_REFRESH_INTERVAL")?,
            discovery_retry_initial: env_non_empty("NX_DISCOVERY_RETRY_INITIAL")?,
            discovery_retry_max: env_non_empty("NX_DISCOVERY_RETRY_MAX")?,
            discovery_stale_after: env_non_empty("NX_DISCOVERY_STALE_AFTER")?,
            discovery_max_seeds: env_usize("NX_DISCOVERY_MAX_SEEDS")?,
            discovery_instance_name: env_non_empty("NX_DISCOVERY_INSTANCE_NAME")?,
            discovery_max_instances: env_usize("NX_DISCOVERY_MAX_INSTANCES")?,
            discovery_service_name: env_non_empty("NX_DISCOVERY_SERVICE_NAME")?,
            discovery_retry_interval: env_non_empty("NX_DISCOVERY_RETRY_INTERVAL")?,
            discovery_max_refresh_interval: env_non_empty("NX_DISCOVERY_MAX_REFRESH_INTERVAL")?,
            discovery_file: env_path("NX_DISCOVERY_FILE"),
            discovery_poll_interval: env_non_empty("NX_DISCOVERY_POLL_INTERVAL")?,
            discovery_max_file_bytes: env_non_empty("NX_DISCOVERY_MAX_FILE_BYTES")?,
        })
    }

    fn has_sync_inputs(&self) -> bool {
        self.listen.is_some()
            || self.peers.as_ref().is_some_and(|peers| !peers.is_empty())
            || self.tls_cert.is_some()
            || self.tls_key.is_some()
            || self.tls_ca.is_some()
            || self.allowed_peers.is_some()
            || self.tls_insecure.unwrap_or(false)
            || self.serialization_format.is_some()
            || self.discovery_mode.is_some()
            || self.discovery_cluster_id.is_some()
            || self.discovery_advertised_endpoint.is_some()
            || self.discovery_max_candidates.is_some()
            || self.discovery_seeds.is_some()
            || self.discovery_instance_name.is_some()
            || self.discovery_service_name.is_some()
            || self.discovery_file.is_some()
    }
}

pub(crate) struct SecretValue(String);

impl SecretValue {
    #[cfg(test)]
    pub(crate) fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretValue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

impl From<WireSerializationFormat> for SerializationFormat {
    fn from(value: WireSerializationFormat) -> Self {
        match value {
            WireSerializationFormat::Bincode => Self::Bincode,
            WireSerializationFormat::Json => Self::Json,
        }
    }
}

fn file_allowed_peers_as_csv(tls: Option<&TlsFileConfig>) -> Option<String> {
    tls.and_then(|tls| tls.allowed_peers.as_ref())
        .map(|peers| peers.join(","))
        .filter(|peers| !peers.is_empty())
}

fn render_optional_string(out: &mut String, name: &str, value: Option<&str>) {
    if let Some(value) = value {
        out.push_str(&format!("{name} = \"{}\"\n", escape_toml(value)));
    }
}

fn render_string_list(values: &[String]) -> String {
    let values = values
        .iter()
        .map(|value| format!("\"{}\"", escape_toml(value)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{values}]")
}

fn render_serialization_format(format: SerializationFormat) -> &'static str {
    match format {
        SerializationFormat::Bincode => "bincode",
        SerializationFormat::Json => "json",
    }
}

fn render_log_format(format: LogFormat) -> &'static str {
    match format {
        LogFormat::Text => "text",
        LogFormat::Json => "json",
    }
}

fn render_discovery_config(out: &mut String, config: &RuntimeDiscoveryConfig) {
    let mode = match &config.mode {
        RuntimeDiscoveryMode::Static => "static",
        RuntimeDiscoveryMode::Bootstrap(_) => "bootstrap",
        RuntimeDiscoveryMode::Mdns(_) => "mdns",
        RuntimeDiscoveryMode::DnsSrv(_) => "dns-srv",
        RuntimeDiscoveryMode::File(_) => "file",
    };
    out.push_str(&format!("mode = \"{mode}\"\n"));
    out.push_str(&format!(
        "cluster_id = \"{}\"\n",
        escape_toml(&config.cluster_id)
    ));
    render_optional_string(
        out,
        "advertised_endpoint",
        config.advertised_endpoint.as_deref(),
    );
    out.push_str(&format!("max_candidates = {}\n", config.max_candidates));
    match &config.mode {
        RuntimeDiscoveryMode::Static => {}
        RuntimeDiscoveryMode::Bootstrap(settings) => {
            out.push_str(&format!(
                "seeds = {}\n",
                render_string_list(&settings.seeds)
            ));
            out.push_str(&format!(
                "refresh_interval = \"{}\"\n",
                render_duration(settings.refresh_interval)
            ));
            out.push_str(&format!(
                "retry_initial = \"{}\"\n",
                render_duration(settings.retry_initial)
            ));
            out.push_str(&format!(
                "retry_max = \"{}\"\n",
                render_duration(settings.retry_max)
            ));
            out.push_str(&format!(
                "stale_after = \"{}\"\n",
                render_duration(settings.stale_after)
            ));
            out.push_str(&format!("max_seeds = {}\n", settings.max_seeds));
        }
        RuntimeDiscoveryMode::Mdns(settings) => {
            out.push_str(&format!(
                "instance_name = \"{}\"\n",
                escape_toml(&settings.instance_name)
            ));
            out.push_str(&format!("max_instances = {}\n", settings.max_instances));
        }
        RuntimeDiscoveryMode::DnsSrv(settings) => {
            out.push_str(&format!(
                "service_name = \"{}\"\n",
                escape_toml(&settings.service_name)
            ));
            out.push_str(&format!(
                "retry_interval = \"{}\"\n",
                render_duration(settings.retry_interval)
            ));
            out.push_str(&format!(
                "max_refresh_interval = \"{}\"\n",
                render_duration(settings.max_refresh_interval)
            ));
        }
        RuntimeDiscoveryMode::File(settings) => {
            out.push_str(&format!(
                "path = \"{}\"\n",
                escape_toml(&settings.path.to_string_lossy())
            ));
            out.push_str(&format!(
                "poll_interval = \"{}\"\n",
                render_duration(settings.poll_interval)
            ));
            out.push_str(&format!("max_file_bytes = {}\n", settings.max_file_bytes));
        }
    }
}

fn render_duration(duration: Duration) -> String {
    let millis = duration.as_millis();
    if millis.is_multiple_of(60_000) {
        format!("{}m", millis / 60_000)
    } else if millis.is_multiple_of(1_000) {
        format!("{}s", millis / 1_000)
    } else {
        format!("{millis}ms")
    }
}

fn escape_toml(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn env_path(name: &str) -> Option<PathBuf> {
    env::var_os(name)
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
}

fn env_non_empty(name: &str) -> Result<Option<String>> {
    match env::var(name) {
        Ok(value) => {
            validate_non_empty(name, &value)?;
            Ok(Some(value))
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => bail!("{name} must be valid unicode"),
    }
}

fn env_peers() -> Result<Option<Vec<String>>> {
    let mut peers = Vec::new();
    if let Some(peer) = env_non_empty("NX_PEER")? {
        peers.push(peer);
    }
    if let Some(value) = env_non_empty("NX_PEERS")? {
        for peer in value.split(',') {
            validate_non_empty("NX_PEERS", peer)?;
            peers.push(peer.trim().to_string());
        }
    }

    if peers.is_empty() {
        Ok(None)
    } else {
        Ok(Some(peers))
    }
}

fn env_csv(name: &str) -> Result<Option<Vec<String>>> {
    let Some(value) = env_non_empty(name)? else {
        return Ok(None);
    };
    value
        .split(',')
        .map(|item| {
            validate_non_empty(name, item)?;
            Ok(item.trim().to_string())
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

fn env_usize(name: &str) -> Result<Option<usize>> {
    let Some(value) = env_non_empty(name)? else {
        return Ok(None);
    };
    value
        .parse::<usize>()
        .map(Some)
        .with_context(|| format!("{name} must be an unsigned integer"))
}

fn env_discovery_mode() -> Result<Option<DiscoveryMode>> {
    let Some(value) = env_non_empty("NX_DISCOVERY_MODE")? else {
        return Ok(None);
    };
    match value.to_ascii_lowercase().as_str() {
        "static" => Ok(Some(DiscoveryMode::Static)),
        "bootstrap" => Ok(Some(DiscoveryMode::Bootstrap)),
        "mdns" => Ok(Some(DiscoveryMode::Mdns)),
        "dns-srv" => Ok(Some(DiscoveryMode::DnsSrv)),
        "file" => Ok(Some(DiscoveryMode::File)),
        _ => bail!("NX_DISCOVERY_MODE must be one of static, bootstrap, mdns, dns-srv, file"),
    }
}

fn resolve_discovery_config(
    cli: &RunCliOptions,
    env: &EnvRunConfig,
    file: Option<&DiscoveryFileConfig>,
) -> Result<RuntimeDiscoveryConfig> {
    let mode = cli
        .discovery_mode
        .or(env.discovery_mode)
        .or_else(|| file.and_then(|config| config.mode))
        .unwrap_or(DiscoveryMode::Static);
    validate_discovery_mode_fields(mode, cli, env, file)?;

    let cluster_id = env
        .discovery_cluster_id
        .clone()
        .or_else(|| file.and_then(|config| config.cluster_id.clone()))
        .unwrap_or_else(|| nx_core::DEFAULT_DISCOVERY_CLUSTER.to_string());
    let advertised_endpoint = env
        .discovery_advertised_endpoint
        .clone()
        .or_else(|| file.and_then(|config| config.advertised_endpoint.clone()));
    let max_candidates = env
        .discovery_max_candidates
        .or_else(|| file.and_then(|config| config.max_candidates))
        .unwrap_or(nx_core::DEFAULT_MAX_PEER_CANDIDATES);
    validate_non_empty("discovery.cluster_id", &cluster_id)?;
    validate_optional_non_empty(
        "discovery.advertised_endpoint",
        advertised_endpoint.as_deref(),
    )?;
    if max_candidates == 0 {
        bail!("discovery.max_candidates must be greater than zero");
    }

    let resolved_mode = match mode {
        DiscoveryMode::Static => RuntimeDiscoveryMode::Static,
        DiscoveryMode::Bootstrap => {
            let seeds = if !cli.bootstrap_seeds.is_empty() {
                cli.bootstrap_seeds.clone()
            } else {
                env.discovery_seeds
                    .clone()
                    .or_else(|| file.and_then(|config| config.seeds.clone()))
                    .unwrap_or_default()
            };
            if seeds.is_empty() {
                bail!("discovery.seeds is required when discovery.mode = \"bootstrap\"");
            }
            for seed in &seeds {
                validate_non_empty("discovery.seeds", seed)?;
            }
            let mut settings = BootstrapDiscoverySettings::new(seeds);
            settings.refresh_interval = resolve_discovery_duration(
                env.discovery_refresh_interval.as_deref(),
                file.and_then(|config| config.refresh_interval.as_deref()),
                settings.refresh_interval,
                "discovery.refresh_interval",
            )?;
            settings.retry_initial = resolve_discovery_duration(
                env.discovery_retry_initial.as_deref(),
                file.and_then(|config| config.retry_initial.as_deref()),
                settings.retry_initial,
                "discovery.retry_initial",
            )?;
            settings.retry_max = resolve_discovery_duration(
                env.discovery_retry_max.as_deref(),
                file.and_then(|config| config.retry_max.as_deref()),
                settings.retry_max,
                "discovery.retry_max",
            )?;
            settings.stale_after = resolve_discovery_duration(
                env.discovery_stale_after.as_deref(),
                file.and_then(|config| config.stale_after.as_deref()),
                settings.stale_after,
                "discovery.stale_after",
            )?;
            settings.max_seeds = env
                .discovery_max_seeds
                .or_else(|| file.and_then(|config| config.max_seeds))
                .unwrap_or(settings.max_seeds);
            validate_non_zero("discovery.max_seeds", settings.max_seeds)?;
            if settings.retry_initial > settings.retry_max {
                bail!("discovery.retry_initial must be less than or equal to discovery.retry_max");
            }
            RuntimeDiscoveryMode::Bootstrap(settings)
        }
        DiscoveryMode::Mdns => {
            let instance_name = cli
                .mdns_instance
                .clone()
                .or_else(|| env.discovery_instance_name.clone())
                .or_else(|| file.and_then(|config| config.instance_name.clone()))
                .context("discovery.instance_name is required when discovery.mode = \"mdns\"")?;
            validate_non_empty("discovery.instance_name", &instance_name)?;
            let mut settings = MdnsDiscoverySettings::new(instance_name);
            settings.max_instances = env
                .discovery_max_instances
                .or_else(|| file.and_then(|config| config.max_instances))
                .unwrap_or(settings.max_instances);
            validate_non_zero("discovery.max_instances", settings.max_instances)?;
            RuntimeDiscoveryMode::Mdns(settings)
        }
        DiscoveryMode::DnsSrv => {
            let service_name = cli
                .dns_srv_name
                .clone()
                .or_else(|| env.discovery_service_name.clone())
                .or_else(|| file.and_then(|config| config.service_name.clone()))
                .context("discovery.service_name is required when discovery.mode = \"dns-srv\"")?;
            validate_non_empty("discovery.service_name", &service_name)?;
            let mut settings = DnsSrvDiscoverySettings::new(service_name);
            settings.retry_interval = resolve_discovery_duration(
                env.discovery_retry_interval.as_deref(),
                file.and_then(|config| config.retry_interval.as_deref()),
                settings.retry_interval,
                "discovery.retry_interval",
            )?;
            settings.max_refresh_interval = resolve_discovery_duration(
                env.discovery_max_refresh_interval.as_deref(),
                file.and_then(|config| config.max_refresh_interval.as_deref()),
                settings.max_refresh_interval,
                "discovery.max_refresh_interval",
            )?;
            RuntimeDiscoveryMode::DnsSrv(settings)
        }
        DiscoveryMode::File => {
            let path = cli
                .peer_file
                .clone()
                .or_else(|| env.discovery_file.clone())
                .or_else(|| file.and_then(|config| config.path.clone()))
                .context("discovery.path is required when discovery.mode = \"file\"")?;
            validate_optional_path("discovery.path", Some(&path))?;
            let mut settings = FileDiscoverySettings::new(path);
            settings.poll_interval = resolve_discovery_duration(
                env.discovery_poll_interval.as_deref(),
                file.and_then(|config| config.poll_interval.as_deref()),
                settings.poll_interval,
                "discovery.poll_interval",
            )?;
            settings.max_file_bytes = env
                .discovery_max_file_bytes
                .as_deref()
                .or_else(|| file.and_then(|config| config.max_file_bytes.as_deref()))
                .map(parse_byte_size)
                .transpose()?
                .unwrap_or(settings.max_file_bytes);
            RuntimeDiscoveryMode::File(settings)
        }
    };

    Ok(RuntimeDiscoveryConfig {
        cluster_id,
        advertised_endpoint,
        max_candidates,
        mode: resolved_mode,
    })
}

fn resolve_discovery_duration(
    env: Option<&str>,
    file: Option<&str>,
    default: Duration,
    name: &str,
) -> Result<Duration> {
    env.or(file)
        .map(|value| parse_duration(value).map_err(|error| anyhow::anyhow!("{name}: {error}")))
        .transpose()
        .map(|duration| duration.unwrap_or(default))
}

fn validate_discovery_mode_fields(
    mode: DiscoveryMode,
    cli: &RunCliOptions,
    env: &EnvRunConfig,
    file: Option<&DiscoveryFileConfig>,
) -> Result<()> {
    let include_env = cli.discovery_mode.is_none();
    let include_file = include_env && env.discovery_mode.is_none();
    let bootstrap = !cli.bootstrap_seeds.is_empty()
        || include_env
            && (env.discovery_seeds.is_some()
                || env.discovery_refresh_interval.is_some()
                || env.discovery_retry_initial.is_some()
                || env.discovery_retry_max.is_some()
                || env.discovery_stale_after.is_some()
                || env.discovery_max_seeds.is_some())
        || include_file
            && file.is_some_and(|config| {
                config.seeds.is_some()
                    || config.refresh_interval.is_some()
                    || config.retry_initial.is_some()
                    || config.retry_max.is_some()
                    || config.stale_after.is_some()
                    || config.max_seeds.is_some()
            });
    let mdns = cli.mdns_instance.is_some()
        || include_env
            && (env.discovery_instance_name.is_some() || env.discovery_max_instances.is_some())
        || include_file
            && file.is_some_and(|config| {
                config.instance_name.is_some() || config.max_instances.is_some()
            });
    let dns_srv = cli.dns_srv_name.is_some()
        || include_env
            && (env.discovery_service_name.is_some()
                || env.discovery_retry_interval.is_some()
                || env.discovery_max_refresh_interval.is_some())
        || include_file
            && file.is_some_and(|config| {
                config.service_name.is_some()
                    || config.retry_interval.is_some()
                    || config.max_refresh_interval.is_some()
            });
    let file_watch = cli.peer_file.is_some()
        || include_env
            && (env.discovery_file.is_some()
                || env.discovery_poll_interval.is_some()
                || env.discovery_max_file_bytes.is_some())
        || include_file
            && file.is_some_and(|config| {
                config.path.is_some()
                    || config.poll_interval.is_some()
                    || config.max_file_bytes.is_some()
            });
    let invalid = match mode {
        DiscoveryMode::Static => bootstrap || mdns || dns_srv || file_watch,
        DiscoveryMode::Bootstrap => mdns || dns_srv || file_watch,
        DiscoveryMode::Mdns => bootstrap || dns_srv || file_watch,
        DiscoveryMode::DnsSrv => bootstrap || mdns || file_watch,
        DiscoveryMode::File => bootstrap || mdns || dns_srv,
    };
    if invalid {
        bail!("discovery contains fields that are not valid for mode {mode}");
    }
    Ok(())
}

impl std::fmt::Display for DiscoveryMode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Static => "static",
            Self::Bootstrap => "bootstrap",
            Self::Mdns => "mdns",
            Self::DnsSrv => "dns-srv",
            Self::File => "file",
        })
    }
}

fn env_bool(name: &str) -> Result<Option<bool>> {
    let Some(value) = env_non_empty(name)? else {
        return Ok(None);
    };
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(Some(true)),
        "0" | "false" | "no" | "off" => Ok(Some(false)),
        _ => bail!("{name} must be one of true, false, 1, 0, yes, no, on, off"),
    }
}

fn env_u64(name: &str) -> Result<Option<u64>> {
    let Some(value) = env_non_empty(name)? else {
        return Ok(None);
    };
    value
        .parse::<u64>()
        .map(Some)
        .with_context(|| format!("{name} must be an unsigned integer"))
}

fn env_serialization_format() -> Result<Option<SerializationFormat>> {
    let Some(value) = env_non_empty("NX_SERIALIZATION_FORMAT")? else {
        return Ok(None);
    };
    match value.to_ascii_lowercase().as_str() {
        "bincode" => Ok(Some(SerializationFormat::Bincode)),
        "json" => Ok(Some(SerializationFormat::Json)),
        _ => bail!("NX_SERIALIZATION_FORMAT must be one of bincode, json"),
    }
}

fn env_log_format() -> Result<Option<LogFormat>> {
    let Some(value) = env_non_empty("NX_LOG_FORMAT")? else {
        return Ok(None);
    };
    match value.to_ascii_lowercase().as_str() {
        "text" => Ok(Some(LogFormat::Text)),
        "json" => Ok(Some(LogFormat::Json)),
        _ => bail!("NX_LOG_FORMAT must be one of text, json"),
    }
}

pub(crate) fn load_run_config(path: Option<&PathBuf>) -> Result<RunFileConfig> {
    let Some(path) = path else {
        return Ok(RunFileConfig::default());
    };

    let text = fs::read_to_string(path)?;
    let config = toml::from_str(&text)?;
    validate_run_file_config(&config)?;
    Ok(config)
}

pub(crate) fn load_run_config_or_default(path: Option<&PathBuf>) -> Result<RunFileConfig> {
    let Some(path) = path else {
        return Ok(RunFileConfig::default());
    };

    if !path.exists() {
        return Ok(RunFileConfig::default());
    }

    load_run_config(Some(path))
}

pub(crate) fn validate_run_file_config(config: &RunFileConfig) -> Result<()> {
    if let Some(network) = &config.network {
        validate_optional_non_empty("network.listen", network.listen.as_deref())?;
        if let Some(peers) = &network.peers {
            for peer in peers {
                validate_non_empty("network.peers[]", peer)?;
            }
        }
        match network.serialization_format {
            Some(WireSerializationFormat::Bincode | WireSerializationFormat::Json) | None => {}
        }
    }

    if let Some(tls) = &config.tls {
        validate_optional_path("tls.cert", tls.cert.as_ref())?;
        validate_optional_path("tls.key", tls.key.as_ref())?;
        validate_optional_path("tls.ca", tls.ca.as_ref())?;
        if let Some(allowed_peers) = &tls.allowed_peers {
            for peer in allowed_peers {
                validate_non_empty("tls.allowed_peers[]", peer)?;
            }
        }

        let insecure = tls.insecure.unwrap_or(false);
        if tls.cert.is_some() ^ tls.key.is_some() {
            bail!("tls.cert and tls.key must be provided together");
        }
        if insecure && (tls.ca.is_some() || tls.allowed_peers.is_some()) {
            bail!("tls.insecure is mutually exclusive with tls.ca and tls.allowed_peers");
        }
        if tls
            .allowed_peers
            .as_ref()
            .is_some_and(|allowed_peers| !allowed_peers.is_empty())
            && tls.ca.is_none()
            && !insecure
        {
            bail!("tls.allowed_peers requires tls.ca");
        }
    }

    if let Some(storage) = &config.storage {
        validate_optional_path("storage.datastore_path", storage.datastore_path.as_ref())?;
    }

    if let Some(limits) = &config.limits {
        validate_optional_non_zero("limits.max_peers", limits.max_peers)?;
        validate_optional_non_zero("limits.queued_ops_limit", limits.queued_ops_limit)?;
        validate_optional_non_zero("limits.op_log_limit", limits.op_log_limit)?;
        validate_optional_non_zero("limits.seen_ops_limit", limits.seen_ops_limit)?;
        if let Some(max_message_size) = &limits.max_message_size {
            parse_byte_size(max_message_size)?;
        }
        validate_optional_non_zero("limits.socket_timeout_secs", limits.socket_timeout_secs)?;
        validate_optional_duration(
            "limits.reconnect_initial_delay",
            limits.reconnect_initial_delay.as_deref(),
        )?;
        validate_optional_duration(
            "limits.reconnect_max_delay",
            limits.reconnect_max_delay.as_deref(),
        )?;
        if limits.reconnect_initial_delay.is_some() ^ limits.reconnect_max_delay.is_some() {
            bail!(
                "limits.reconnect_initial_delay and limits.reconnect_max_delay must be provided together"
            );
        }
        if let (Some(initial), Some(max)) = (
            limits.reconnect_initial_delay.as_deref(),
            limits.reconnect_max_delay.as_deref(),
        ) {
            let initial = parse_duration(initial).map_err(|e| anyhow::anyhow!("{e}"))?;
            let max = parse_duration(max).map_err(|e| anyhow::anyhow!("{e}"))?;
            if initial > max {
                bail!(
                    "limits.reconnect_initial_delay must be less than or equal to limits.reconnect_max_delay"
                );
            }
        }
        validate_optional_non_zero(
            "limits.peer_dead_after_failures",
            limits.peer_dead_after_failures,
        )?;
        validate_optional_duration(
            "limits.anti_entropy_interval",
            limits.anti_entropy_interval.as_deref(),
        )?;
    }

    if let Some(observability) = &config.observability {
        validate_optional_non_empty("observability.listen", observability.listen.as_deref())?;
        validate_optional_non_empty(
            "observability.log_level",
            observability.log_level.as_deref(),
        )?;
        if let Some(format) = observability.log_format {
            match format {
                LogFormat::Text | LogFormat::Json => {}
            }
        }
        validate_optional_non_zero(
            "observability.request_timeout_secs",
            observability.request_timeout_secs,
        )?;
    }

    if let Some(management) = &config.management {
        validate_optional_non_empty("management.listen", management.listen.as_deref())?;
        validate_optional_path("management.token_file", management.token_file.as_ref())?;
        validate_optional_non_zero(
            "management.request_timeout_secs",
            management.request_timeout_secs,
        )?;
    }

    resolve_discovery_config(
        &RunCliOptions::default(),
        &EnvRunConfig::default(),
        config.discovery.as_ref(),
    )?;

    Ok(())
}

fn validate_non_empty(name: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{name} must not be empty");
    }
    Ok(())
}

fn validate_non_zero(name: &str, value: usize) -> Result<()> {
    if value == 0 {
        bail!("{name} must be greater than zero");
    }
    Ok(())
}

fn validate_optional_non_empty(name: &str, value: Option<&str>) -> Result<()> {
    if let Some(value) = value {
        validate_non_empty(name, value)?;
    }
    Ok(())
}

fn validate_optional_path(name: &str, value: Option<&PathBuf>) -> Result<()> {
    if let Some(value) = value
        && value.as_os_str().is_empty()
    {
        bail!("{name} must not be empty");
    }
    Ok(())
}

fn validate_optional_non_zero<T>(name: &str, value: Option<T>) -> Result<()>
where
    T: PartialEq + From<u8>,
{
    if value == Some(T::from(0)) {
        bail!("{name} must be greater than zero");
    }
    Ok(())
}

fn validate_optional_duration(name: &str, value: Option<&str>) -> Result<()> {
    if let Some(value) = value {
        parse_duration(value).map_err(|e| anyhow::anyhow!("{name}: {e}"))?;
    }
    Ok(())
}

pub(crate) fn init_logging(
    log_level: &str,
    log_format: LogFormat,
    tokio_console: bool,
) -> Result<()> {
    let env_filter = || {
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level))
    };

    #[cfg(feature = "tokio-console")]
    if tokio_console {
        let console_layer = console_subscriber::spawn();
        match log_format {
            LogFormat::Text => tracing_subscriber::registry()
                .with(console_layer)
                .with(tracing_subscriber::fmt::layer().with_filter(env_filter()))
                .try_init()
                .map_err(|e| anyhow::anyhow!("initialize tracing subscriber: {e}"))?,
            LogFormat::Json => tracing_subscriber::registry()
                .with(console_layer)
                .with(
                    tracing_subscriber::fmt::layer()
                        .json()
                        .with_filter(env_filter()),
                )
                .try_init()
                .map_err(|e| anyhow::anyhow!("initialize tracing subscriber: {e}"))?,
        }
        return Ok(());
    }

    #[cfg(not(feature = "tokio-console"))]
    if tokio_console {
        bail!(
            "--tokio-console requires a binary built with --features tokio-console and RUSTFLAGS=\"--cfg tokio_unstable\""
        );
    }

    match log_format {
        LogFormat::Text => tracing_subscriber::fmt()
            .with_env_filter(env_filter())
            .try_init()
            .map_err(|e| anyhow::anyhow!("initialize tracing subscriber: {e}"))?,
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_env_filter(env_filter())
            .try_init()
            .map_err(|e| anyhow::anyhow!("initialize tracing subscriber: {e}"))?,
    }

    Ok(())
}

pub(crate) fn resolve_log_level(
    verbose: bool,
    cli_log_level: Option<String>,
    observability: Option<&ObservabilityFileConfig>,
) -> Result<String> {
    let level = cli_log_level
        .or_else(|| observability.and_then(|cfg| cfg.log_level.clone()))
        .unwrap_or_else(|| {
            if verbose {
                "debug".to_string()
            } else {
                "info".to_string()
            }
        });

    match level.as_str() {
        "trace" | "debug" | "info" | "warn" | "error" => Ok(level),
        _ => bail!("log level must be one of trace, debug, info, warn, error"),
    }
}

fn resolve_log_format(
    cli_log_format: Option<LogFormat>,
    observability: Option<&ObservabilityFileConfig>,
) -> LogFormat {
    cli_log_format
        .or_else(|| observability.and_then(|cfg| cfg.log_format))
        .unwrap_or(LogFormat::Text)
}

pub(crate) fn build_observability_config(
    listen: Option<String>,
    observability: Option<&ObservabilityFileConfig>,
) -> Result<Option<ObservabilityConfig>> {
    let Some(listen_addr) = listen.or_else(|| observability.and_then(|cfg| cfg.listen.clone()))
    else {
        return Ok(None);
    };

    let mut config = ObservabilityConfig::new(listen_addr);
    if let Some(request_timeout_secs) = observability.and_then(|cfg| cfg.request_timeout_secs) {
        if request_timeout_secs == 0 {
            bail!("observability.request_timeout_secs must be greater than zero");
        }
        config = config.with_request_timeout(Duration::from_secs(request_timeout_secs));
    }

    Ok(Some(config))
}

pub(crate) fn build_management_config(
    env_config: &EnvRunConfig,
    management: Option<&ManagementFileConfig>,
) -> Result<Option<ManagementConfig>> {
    let token = match env_config.management_token.as_ref() {
        Some(token) => Some(token.expose().to_string()),
        None => {
            let token_file = env_config
                .management_token_file
                .as_ref()
                .or_else(|| management.and_then(|config| config.token_file.as_ref()));
            token_file.map(read_management_token).transpose()?
        }
    };
    let Some(token) = token else {
        return Ok(None);
    };

    let listen = env_config
        .management_listen
        .clone()
        .or_else(|| management.and_then(|config| config.listen.clone()))
        .unwrap_or_else(|| DEFAULT_MANAGEMENT_LISTEN.to_string());
    let allow_non_loopback = env_config
        .management_allow_non_loopback
        .or_else(|| management.and_then(|config| config.allow_non_loopback))
        .unwrap_or(false);
    let request_timeout_secs = env_config
        .management_request_timeout_secs
        .or_else(|| management.and_then(|config| config.request_timeout_secs));

    let mut config = ManagementConfig::new(&listen, &token, allow_non_loopback)?;
    if let Some(seconds) = request_timeout_secs {
        config = config.with_request_timeout(Duration::from_secs(seconds))?;
    } else {
        config = config.with_request_timeout(DEFAULT_MANAGEMENT_REQUEST_TIMEOUT)?;
    }
    Ok(Some(config))
}

fn read_management_token(path: &PathBuf) -> Result<String> {
    let token = fs::read_to_string(path)
        .with_context(|| format!("read management token file {}", path.display()))?;
    Ok(token.trim_end_matches(['\r', '\n']).to_string())
}

pub(crate) fn apply_limit_config(
    mut sync: SyncConfig,
    limits: Option<&LimitsFileConfig>,
) -> Result<SyncConfig> {
    let Some(limits) = limits else {
        return Ok(sync);
    };

    if let Some(max_peers) = limits.max_peers {
        sync = sync.with_max_peers(max_peers);
    }
    if let Some(queued_ops_limit) = limits.queued_ops_limit {
        if queued_ops_limit == 0 {
            bail!("limits.queued_ops_limit must be greater than zero");
        }
        sync = sync.with_queued_ops_limit(queued_ops_limit);
    }
    if let Some(op_log_limit) = limits.op_log_limit {
        if op_log_limit == 0 {
            bail!("limits.op_log_limit must be greater than zero");
        }
        sync = sync.with_op_log_limit(op_log_limit);
    }
    if let Some(seen_ops_limit) = limits.seen_ops_limit {
        if seen_ops_limit == 0 {
            bail!("limits.seen_ops_limit must be greater than zero");
        }
        sync = sync.with_seen_ops_limit(seen_ops_limit);
    }
    if let Some(max_message_size) = &limits.max_message_size {
        sync = sync.with_max_message_size(parse_byte_size(max_message_size)?);
    }
    if let Some(socket_timeout_secs) = limits.socket_timeout_secs {
        if socket_timeout_secs == 0 {
            bail!("limits.socket_timeout_secs must be greater than zero");
        }
        sync = sync.with_socket_timeout(Duration::from_secs(socket_timeout_secs));
    }
    if let (Some(initial), Some(max)) = (
        limits.reconnect_initial_delay.as_deref(),
        limits.reconnect_max_delay.as_deref(),
    ) {
        let initial = parse_duration(initial).map_err(|e| anyhow::anyhow!("{e}"))?;
        let max = parse_duration(max).map_err(|e| anyhow::anyhow!("{e}"))?;
        sync = sync.with_reconnect_backoff(initial, max);
    }
    if let Some(peer_dead_after_failures) = limits.peer_dead_after_failures {
        if peer_dead_after_failures == 0 {
            bail!("limits.peer_dead_after_failures must be greater than zero");
        }
        sync = sync.with_peer_dead_after_failures(peer_dead_after_failures);
    }
    if let Some(anti_entropy_interval) = &limits.anti_entropy_interval {
        let interval = parse_duration(anti_entropy_interval).map_err(|e| anyhow::anyhow!("{e}"))?;
        sync = sync.with_anti_entropy_interval(interval);
    }

    Ok(sync)
}

pub(crate) fn parse_byte_size(input: &str) -> Result<usize> {
    let input = input.trim();
    if input.is_empty() {
        bail!("expected byte size like 16MiB");
    }

    let compact = input.replace(' ', "");
    let (number, multiplier) = if let Some(n) = compact.strip_suffix("MiB") {
        (n, 1024usize * 1024)
    } else if let Some(n) = compact.strip_suffix("KiB") {
        (n, 1024usize)
    } else if let Some(n) = compact.strip_suffix('B') {
        (n, 1usize)
    } else {
        (compact.as_str(), 1usize)
    };

    let amount = number
        .parse::<usize>()
        .map_err(|_| anyhow::anyhow!("expected byte size like 16MiB"))?;
    if amount == 0 {
        bail!("byte size must be greater than zero");
    }

    amount
        .checked_mul(multiplier)
        .ok_or_else(|| anyhow::anyhow!("byte size is too large"))
}

/// Validate that the TLS-related CLI flags form a coherent combination.
pub(crate) fn validate_tls_flags(
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

pub(crate) fn parse_duration(input: &str) -> Result<Duration, String> {
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

pub(crate) fn validate_settle_mode(
    sync: &Option<SyncConfig>,
    settle_for: Option<Duration>,
) -> Result<()> {
    if settle_for.is_some() && sync.is_none() {
        bail!("--settle-for requires sync to be enabled with --listen");
    }

    Ok(())
}

pub(crate) fn validate_wait_before_run(
    sync: &Option<SyncConfig>,
    wait_before_run: Option<Duration>,
) -> Result<()> {
    if wait_before_run.is_some() && sync.is_none() {
        bail!("--wait-before-run requires sync to be enabled with --listen");
    }

    Ok(())
}

pub(crate) fn validate_print_gcounter(
    sync: &Option<SyncConfig>,
    print_gcounter: &Option<String>,
) -> Result<()> {
    if print_gcounter.is_some() && sync.is_none() {
        bail!("--print-gcounter requires sync to be enabled with --listen");
    }

    Ok(())
}

pub(crate) fn validate_print_pncounter(
    sync: &Option<SyncConfig>,
    print_pncounter: &Option<String>,
) -> Result<()> {
    if print_pncounter.is_some() && sync.is_none() {
        bail!("--print-pncounter requires sync to be enabled with --listen");
    }

    Ok(())
}

pub(crate) fn validate_print_lww_register(
    sync: &Option<SyncConfig>,
    print_lww_register: &Option<String>,
) -> Result<()> {
    if print_lww_register.is_some() && sync.is_none() {
        bail!("--print-lww-register requires sync to be enabled with --listen");
    }

    Ok(())
}

pub(crate) fn validate_print_lww_map(
    sync: &Option<SyncConfig>,
    print_lww_map: &Option<String>,
) -> Result<()> {
    if print_lww_map.is_some() && sync.is_none() {
        bail!("--print-lww-map requires sync to be enabled with --listen");
    }

    Ok(())
}

pub(crate) fn validate_print_orset(
    sync: &Option<SyncConfig>,
    print_orset: &Option<String>,
) -> Result<()> {
    if print_orset.is_some() && sync.is_none() {
        bail!("--print-orset requires sync to be enabled with --listen");
    }

    Ok(())
}

pub(crate) fn validate_print_rga(
    sync: &Option<SyncConfig>,
    print_rga: &Option<String>,
) -> Result<()> {
    if print_rga.is_some() && sync.is_none() {
        bail!("--print-rga requires sync to be enabled with --listen");
    }

    Ok(())
}

/// Build TlsConfig from CLI flags.
pub(crate) fn build_tls_config(
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
pub(crate) fn build_sync_config(
    listen: Option<String>,
    peers: Vec<String>,
    tls: Option<TlsConfig>,
    force_enabled: bool,
    serialization_format: Option<SerializationFormat>,
) -> Result<Option<SyncConfig>> {
    if listen.is_none() && peers.is_empty() && !force_enabled && serialization_format.is_none() {
        return Ok(None);
    }

    if listen.is_none() {
        bail!(
            "sync configuration requires --listen: dialer-only mode is not yet supported. \
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
    if let Some(serialization_format) = serialization_format {
        cfg = cfg.with_serialization_format(serialization_format);
    }

    debug_assert!(cfg.is_enabled());
    Ok(Some(cfg))
}
