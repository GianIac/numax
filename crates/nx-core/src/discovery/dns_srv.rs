use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use hickory_resolver::TokioResolver;
use hickory_resolver::proto::rr::rdata::SRV;
use hickory_resolver::proto::rr::{RData, RecordType};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::dynamic::{AbortOnDropTask, DynamicState};
use super::{
    DEFAULT_DISCOVERY_CLUSTER, DEFAULT_DISCOVERY_EVENT_CAPACITY, DEFAULT_MAX_PEER_CANDIDATES,
    DiscoveryError, DiscoverySnapshot, DiscoveryWatch, PeerAnnouncement, PeerDiscovery,
};

const PROVIDER: &str = "dns-srv";
const DEFAULT_RETRY_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_MAX_REFRESH_INTERVAL: Duration = Duration::from_secs(300);

/// DNS-SRV lookup and refresh policy.
#[derive(Debug, Clone)]
pub struct DnsSrvDiscoveryConfig {
    pub service_name: String,
    pub cluster_id: String,
    pub retry_interval: Duration,
    pub max_refresh_interval: Duration,
    pub max_candidates: usize,
    pub event_capacity: usize,
}

impl DnsSrvDiscoveryConfig {
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            cluster_id: DEFAULT_DISCOVERY_CLUSTER.to_string(),
            retry_interval: DEFAULT_RETRY_INTERVAL,
            max_refresh_interval: DEFAULT_MAX_REFRESH_INTERVAL,
            max_candidates: DEFAULT_MAX_PEER_CANDIDATES,
            event_capacity: DEFAULT_DISCOVERY_EVENT_CAPACITY,
        }
    }
}

#[derive(Debug)]
struct SrvAnswer {
    records: Vec<SRV>,
    valid_until: Instant,
}

struct Lifecycle {
    stopped: bool,
    shutdown: Option<watch::Sender<bool>>,
    task: Option<JoinHandle<()>>,
}

struct Inner {
    config: DnsSrvDiscoveryConfig,
    state: Arc<DynamicState>,
    resolver: TokioResolver,
    lifecycle: StdMutex<Lifecycle>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        let lifecycle = self
            .lifecycle
            .get_mut()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(shutdown) = lifecycle.shutdown.take() {
            let _ = shutdown.send(true);
        }
        if let Some(task) = lifecycle.task.take() {
            task.abort();
        }
    }
}

/// Discovers connection candidates from a DNS SRV record.
///
/// SRV priority is retained as deterministic ordering metadata only; results
/// remain unauthenticated candidates. A successful empty/NXDOMAIN response
/// removes the previous view. Transient resolver failures keep the last valid
/// view only until its DNS expiry.
pub struct DnsSrvDiscovery {
    inner: Arc<Inner>,
}

impl DnsSrvDiscovery {
    pub fn new(config: DnsSrvDiscoveryConfig) -> Result<Self, DiscoveryError> {
        validate_config(&config)?;
        let resolver = TokioResolver::builder_tokio()
            .and_then(|builder| builder.build())
            .map_err(|error| {
                provider_error(
                    format!("cannot load system DNS configuration: {error}"),
                    false,
                )
            })?;
        Ok(Self::with_resolver(config, resolver))
    }

    fn with_resolver(config: DnsSrvDiscoveryConfig, resolver: TokioResolver) -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Arc::new(DynamicState::new(config.event_capacity)),
                config,
                resolver,
                lifecycle: StdMutex::new(Lifecycle {
                    stopped: false,
                    shutdown: None,
                    task: None,
                }),
            }),
        }
    }

    async fn ensure_started(&self) -> Result<(), DiscoveryError> {
        let mut lifecycle = self
            .inner
            .lifecycle
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if lifecycle.stopped {
            return Err(provider_error("provider is shut down", false));
        }
        if lifecycle.task.is_some() {
            return Ok(());
        }

        let (shutdown, shutdown_rx) = watch::channel(false);
        let config = self.inner.config.clone();
        let resolver = self.inner.resolver.clone();
        let state = Arc::clone(&self.inner.state);
        lifecycle.shutdown = Some(shutdown);
        lifecycle.task = Some(tokio::spawn(async move {
            run_dns_refresh(config, resolver, state, shutdown_rx).await;
        }));
        Ok(())
    }
}

#[async_trait]
impl PeerDiscovery for DnsSrvDiscovery {
    fn cluster_id(&self) -> &str {
        &self.inner.config.cluster_id
    }

    async fn discover(&self) -> Result<DiscoverySnapshot, DiscoveryError> {
        self.ensure_started().await?;
        Ok(self.inner.state.snapshot())
    }

    async fn announce(&self, _announcement: &PeerAnnouncement) -> Result<(), DiscoveryError> {
        Err(DiscoveryError::Unsupported {
            provider: PROVIDER.to_string(),
            operation: "announcement",
        })
    }

    async fn watch(&self) -> Result<DiscoveryWatch, DiscoveryError> {
        self.ensure_started().await?;
        Ok(self.inner.state.watch())
    }

    fn request_shutdown(&self) {
        let mut lifecycle = self
            .inner
            .lifecycle
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        lifecycle.stopped = true;
        if let Some(shutdown) = lifecycle.shutdown.as_ref() {
            let _ = shutdown.send(true);
        }
    }

    async fn shutdown(&self) -> Result<(), DiscoveryError> {
        self.request_shutdown();
        let task = {
            let mut lifecycle = self
                .inner
                .lifecycle
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            lifecycle.shutdown.take();
            lifecycle.task.take()
        };
        if let Some(task) = task {
            AbortOnDropTask::new(task)
                .join()
                .await
                .map_err(|error| provider_error(format!("refresh task failed: {error}"), false))?;
        }
        self.inner.state.replace(Vec::new());
        Ok(())
    }
}

async fn run_dns_refresh(
    config: DnsSrvDiscoveryConfig,
    resolver: TokioResolver,
    state: Arc<DynamicState>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut valid_until = None;
    let mut next_refresh = Instant::now();
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
            }
            _ = tokio::time::sleep_until(next_refresh) => {
                match lookup(&config, &resolver).await {
                    Ok(answer) => {
                        state.replace(records_to_peers(answer.records, config.max_candidates));
                        let now = Instant::now();
                        valid_until = Some(answer.valid_until);
                        next_refresh = answer.valid_until.min(now + config.max_refresh_interval);
                        if next_refresh <= now {
                            next_refresh = now + config.retry_interval;
                        }
                    }
                    Err(error) => {
                        let now = Instant::now();
                        if valid_until.is_some_and(|deadline| now >= deadline) {
                            state.replace(Vec::new());
                        }
                        tracing::warn!(%error, name = %config.service_name, "DNS-SRV discovery refresh failed");
                        next_refresh = now + config.retry_interval;
                    }
                }
            }
        }
    }
}

async fn lookup(
    config: &DnsSrvDiscoveryConfig,
    resolver: &TokioResolver,
) -> Result<SrvAnswer, DiscoveryError> {
    match inner_lookup(config, resolver).await {
        Ok(answer) => Ok(answer),
        Err(error) if error.is_no_records_found() => Ok(SrvAnswer {
            records: Vec::new(),
            valid_until: Instant::now() + config.max_refresh_interval,
        }),
        Err(error) => Err(provider_error(
            format!("lookup of {} failed: {error}", config.service_name),
            true,
        )),
    }
}

async fn inner_lookup(
    config: &DnsSrvDiscoveryConfig,
    resolver: &TokioResolver,
) -> Result<SrvAnswer, hickory_resolver::net::NetError> {
    let lookup = resolver
        .lookup(&config.service_name, RecordType::SRV)
        .await?;
    let valid_until = lookup.valid_until().into();
    let records = lookup
        .answers()
        .iter()
        .filter_map(|record| match &record.data {
            RData::SRV(srv) => Some(srv.clone()),
            _ => None,
        })
        .collect();
    Ok(SrvAnswer {
        records,
        valid_until,
    })
}

fn records_to_peers(mut records: Vec<SRV>, max_candidates: usize) -> Vec<String> {
    records.sort_by_key(|record| {
        (
            record.priority,
            record.target.to_utf8(),
            record.port,
            record.weight,
        )
    });
    records
        .into_iter()
        .filter(|record| record.port != 0 && !record.target.is_root())
        .filter_map(|record| {
            let endpoint = format!(
                "{}:{}",
                record.target.to_utf8().trim_end_matches('.'),
                record.port
            );
            crate::sync_manager::canonicalize_endpoint(&endpoint).ok()
        })
        .fold(Vec::new(), |mut peers, endpoint| {
            if peers.len() < max_candidates && !peers.contains(&endpoint) {
                peers.push(endpoint);
            }
            peers
        })
}

fn validate_config(config: &DnsSrvDiscoveryConfig) -> Result<(), DiscoveryError> {
    if !config.service_name.ends_with('.') {
        return Err(invalid(
            "service_name must be a fully-qualified name ending with '.'",
        ));
    }
    let labels = config
        .service_name
        .trim_end_matches('.')
        .split('.')
        .collect::<Vec<_>>();
    let valid_service = labels.first().is_some_and(|label| {
        label.len() > 1
            && label.starts_with('_')
            && label[1..]
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    });
    let valid_protocol = labels.get(1).is_some_and(|label| {
        label.eq_ignore_ascii_case("_tcp") || label.eq_ignore_ascii_case("_udp")
    });
    if labels.len() < 3 || !valid_service || !valid_protocol {
        return Err(invalid(
            "service_name must use the fully-qualified _service._tcp|_udp.domain. form",
        ));
    }
    if hickory_resolver::proto::rr::Name::from_ascii(&config.service_name).is_err() {
        return Err(invalid("service_name is not a valid DNS name"));
    }
    if config.cluster_id.trim().is_empty() {
        return Err(invalid("cluster_id must not be empty"));
    }
    if config.retry_interval.is_zero()
        || config.max_refresh_interval.is_zero()
        || config.max_candidates == 0
        || config.event_capacity == 0
    {
        return Err(invalid("intervals and limits must be greater than zero"));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> DiscoveryError {
    DiscoveryError::InvalidConfiguration {
        provider: PROVIDER.to_string(),
        message: message.into(),
    }
}

fn provider_error(message: impl Into<String>, retryable: bool) -> DiscoveryError {
    DiscoveryError::Provider {
        provider: PROVIDER.to_string(),
        message: message.into(),
        retryable,
    }
}

#[cfg(test)]
mod tests {
    use hickory_resolver::proto::rr::Name;

    use super::*;

    #[test]
    fn srv_records_are_bounded_deduplicated_and_deterministic() {
        let records = vec![
            SRV::new(20, 0, 9002, Name::from_ascii("b.example.").unwrap()),
            SRV::new(10, 1, 9001, Name::from_ascii("a.example.").unwrap()),
            SRV::new(10, 1, 9001, Name::from_ascii("a.example.").unwrap()),
        ];
        assert_eq!(
            records_to_peers(records, 2),
            ["a.example:9001", "b.example:9002"]
        );
    }

    #[test]
    fn srv_records_reject_undialable_targets_and_normalize_dns_names() {
        let records = vec![
            SRV::new(1, 0, 9000, Name::from_ascii("0.0.0.0.").unwrap()),
            SRV::new(1, 0, 9000, Name::from_ascii("BAD_NAME.").unwrap()),
            SRV::new(1, 0, 9000, Name::from_ascii("Peer.Example.").unwrap()),
            SRV::new(1, 0, 9000, Name::from_ascii("peer.example.").unwrap()),
            SRV::new(1, 0, 0, Name::from_ascii("zero.example.").unwrap()),
            SRV::new(1, 0, 9000, Name::root()),
        ];

        assert_eq!(records_to_peers(records, 8), ["peer.example:9000"]);
    }

    #[test]
    fn invalid_configuration_is_rejected_without_starting_a_task() {
        let mut config = DnsSrvDiscoveryConfig::new("not-srv.example");
        config.max_candidates = 0;
        assert!(DnsSrvDiscovery::new(config).is_err());

        assert!(DnsSrvDiscovery::new(DnsSrvDiscoveryConfig::new("_numax.example.")).is_err());
        assert!(DnsSrvDiscovery::new(DnsSrvDiscoveryConfig::new("_numax._http.example.")).is_err());
    }
}
