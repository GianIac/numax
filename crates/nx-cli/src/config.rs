use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{env, fs};

use anyhow::{Result, bail};
use clap::ValueEnum;
use nx_core::runtime::RuntimeConfig;
use nx_core::{ObservabilityConfig, SerializationFormat, SyncConfig, TlsConfig};
use serde::Deserialize;
use tracing::warn;

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
pub(crate) struct DiscoveryFileConfig {
    pub(crate) mode: Option<DiscoveryMode>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum DiscoveryMode {
    Static,
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
}

#[derive(Debug)]
pub(crate) struct EffectiveRunConfig {
    pub(crate) datastore_path: Option<PathBuf>,
    pub(crate) sync: Option<SyncConfig>,
    pub(crate) observability: Option<ObservabilityConfig>,
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
        out.push_str("mode = \"static\"\n");
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
    pub(crate) tls_cert: Option<PathBuf>,
    pub(crate) tls_key: Option<PathBuf>,
    pub(crate) tls_ca: Option<PathBuf>,
    pub(crate) allowed_peers: Option<String>,
    pub(crate) tls_insecure: Option<bool>,
    pub(crate) serialization_format: Option<SerializationFormat>,
    pub(crate) log_level: Option<String>,
    pub(crate) log_format: Option<LogFormat>,
}

impl EnvRunConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            datastore_path: env_path("NX_DATASTORE_PATH"),
            listen: env_non_empty("NX_LISTEN")?,
            peers: env_peers()?,
            observability_listen: env_non_empty("NX_OBSERVABILITY_LISTEN")?,
            tls_cert: env_path("NX_TLS_CERT"),
            tls_key: env_path("NX_TLS_KEY"),
            tls_ca: env_path("NX_TLS_CA"),
            allowed_peers: env_non_empty("NX_ALLOWED_PEERS")?,
            tls_insecure: env_bool("NX_TLS_INSECURE")?,
            serialization_format: env_serialization_format()?,
            log_level: env_non_empty("NX_LOG_LEVEL")?,
            log_format: env_log_format()?,
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

fn validate_run_file_config(config: &RunFileConfig) -> Result<()> {
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
        if tls.allowed_peers.is_some() && tls.ca.is_none() && !insecure {
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

    if let Some(discovery) = &config.discovery {
        match discovery.mode {
            Some(DiscoveryMode::Static) | None => {}
        }
    }

    Ok(())
}

fn validate_non_empty(name: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{name} must not be empty");
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

pub(crate) fn init_logging(log_level: &str, log_format: LogFormat) {
    match log_format {
        LogFormat::Text => tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level)),
            )
            .init(),
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level)),
            )
            .init(),
    }
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
