use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use hickory_resolver::TokioResolver;
use hickory_resolver::net::{DnsError, NetError as DnsNetError};
use hickory_resolver::proto::rr::rdata::SRV;
use hickory_resolver::proto::rr::{RData, RecordType};
use tokio::sync::watch;
use tokio::time::Instant;

use super::dynamic::{DynamicState, ProviderTask, checked_deadline, validate_durations};
use super::{
    DEFAULT_DISCOVERY_CLUSTER, DEFAULT_DISCOVERY_EVENT_CAPACITY, DEFAULT_MAX_PEER_CANDIDATES,
    DiscoveryError, DiscoverySnapshot, DiscoveryWatch, PeerAnnouncement, PeerDiscovery,
    validate_event_capacity,
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
    /// Event channel capacity in `1..=super::MAX_DISCOVERY_EVENT_CAPACITY`.
    /// Defaults to [`DEFAULT_DISCOVERY_EVENT_CAPACITY`]; validated by the provider constructor.
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

#[async_trait]
trait SrvResolver: Send + Sync {
    async fn lookup(&self, config: &DnsSrvDiscoveryConfig) -> Result<SrvAnswer, DiscoveryError>;
}

struct HickorySrvResolver(TokioResolver);

#[async_trait]
impl SrvResolver for HickorySrvResolver {
    async fn lookup(&self, config: &DnsSrvDiscoveryConfig) -> Result<SrvAnswer, DiscoveryError> {
        lookup(config, &self.0).await
    }
}

struct Lifecycle {
    stopped: bool,
    shutdown: Option<watch::Sender<bool>>,
    task: Option<ProviderTask>,
}

struct Inner {
    config: DnsSrvDiscoveryConfig,
    state: Arc<DynamicState>,
    resolver: Arc<dyn SrvResolver>,
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
        Ok(Self::with_resolver(
            config,
            Arc::new(HickorySrvResolver(resolver)),
        ))
    }

    fn with_resolver(config: DnsSrvDiscoveryConfig, resolver: Arc<dyn SrvResolver>) -> Self {
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
        if let Some(task) = lifecycle.task.as_ref()
            && task.running(PROVIDER, &self.inner.state)?
        {
            return Ok(());
        }

        let (shutdown, shutdown_rx) = watch::channel(false);
        let config = self.inner.config.clone();
        let resolver = self.inner.resolver.clone();
        let state = Arc::clone(&self.inner.state);
        lifecycle.shutdown = Some(shutdown);
        lifecycle.task = Some(ProviderTask::spawn(
            PROVIDER,
            state.clone(),
            shutdown_rx.clone(),
            run_dns_refresh(config, resolver, state, shutdown_rx),
            || async { Ok(()) },
        ));
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
        self.inner.state.live_watch()
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
            let lifecycle = self
                .inner
                .lifecycle
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            lifecycle.task.clone()
        };
        match task {
            Some(task) => task.join().await,
            None => Ok(()),
        }
    }
}

async fn run_dns_refresh(
    config: DnsSrvDiscoveryConfig,
    resolver: Arc<dyn SrvResolver>,
    state: Arc<DynamicState>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), DiscoveryError> {
    let mut valid_until = None;
    let mut next_refresh = Instant::now();
    loop {
        if *shutdown.borrow() || shutdown.has_changed().is_err() {
            return Ok(());
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
            }
            _ = tokio::time::sleep_until(next_refresh) => {
                let query = resolver.lookup(&config);
                tokio::pin!(query);
                let result = loop {
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                return Ok(());
                            }
                        }
                        result = &mut query => break result,
                        _ = wait_for_dns_expiry(valid_until) => {
                            state.replace(Vec::new());
                            valid_until = None;
                        }
                    }
                };
                match result {
                    Ok(answer) => {
                        (valid_until, next_refresh) = apply_dns_answer(&config, &state, answer, valid_until)?;
                    }
                    Err(error) => {
                        let now = Instant::now();
                        if valid_until.is_some_and(|deadline| now >= deadline) {
                            state.replace(Vec::new());
                        }
                        tracing::warn!(%error, name = %config.service_name, "DNS-SRV discovery refresh failed");
                        if matches!(error, DiscoveryError::Provider { retryable: false, .. }) {
                            return Err(error);
                        }
                        next_refresh = retry_deadline(now, config.retry_interval, valid_until)?;
                    }
                }
            }
        }
    }
    Ok(())
}

fn apply_dns_answer(
    config: &DnsSrvDiscoveryConfig,
    state: &DynamicState,
    answer: SrvAnswer,
    previous_valid_until: Option<Instant>,
) -> Result<(Option<Instant>, Instant), DiscoveryError> {
    let now = Instant::now();
    if answer.valid_until <= now {
        state.replace(Vec::new());
        return Ok((
            None,
            checked_deadline(now, config.retry_interval, PROVIDER, "retry_interval")?,
        ));
    }
    let refresh = checked_deadline(
        now,
        config.max_refresh_interval,
        PROVIDER,
        "max_refresh_interval",
    )?;
    let peers = records_to_peers(answer.records, config.max_candidates);
    if previous_valid_until.is_some_and(|previous| answer.valid_until <= previous) {
        // Hickory can return the same cached answer before its original expiry.
        // Only a newly validated DNS lifetime renews candidate observations.
        state.replace(peers);
    } else {
        state.observe(peers);
    }
    Ok((Some(answer.valid_until), answer.valid_until.min(refresh)))
}

async fn wait_for_dns_expiry(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

async fn lookup(
    config: &DnsSrvDiscoveryConfig,
    resolver: &TokioResolver,
) -> Result<SrvAnswer, DiscoveryError> {
    match inner_lookup(config, resolver).await {
        Ok(answer) => Ok(answer),
        Err(DnsNetError::Dns(DnsError::NoRecordsFound(no_records))) => {
            let negative_ttl =
                bounded_negative_ttl(no_records.negative_ttl, config.max_refresh_interval);
            Ok(SrvAnswer {
                records: Vec::new(),
                valid_until: checked_deadline(
                    Instant::now(),
                    negative_ttl,
                    PROVIDER,
                    "negative_ttl",
                )?,
            })
        }
        Err(error) => Err(provider_error(
            format!("lookup of {} failed: {error}", config.service_name),
            true,
        )),
    }
}

fn retry_deadline(
    now: Instant,
    retry_interval: Duration,
    valid_until: Option<Instant>,
) -> Result<Instant, DiscoveryError> {
    let retry = checked_deadline(now, retry_interval, PROVIDER, "retry_interval")?;
    Ok(valid_until
        .filter(|deadline| *deadline > now)
        .map(|deadline| deadline.min(retry))
        .unwrap_or(retry))
}

fn bounded_negative_ttl(negative_ttl: Option<u32>, max_refresh_interval: Duration) -> Duration {
    negative_ttl
        .map(|seconds| Duration::from_secs(u64::from(seconds)))
        .unwrap_or(max_refresh_interval)
        .min(max_refresh_interval)
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

pub(super) fn validate_config(config: &DnsSrvDiscoveryConfig) -> Result<(), DiscoveryError> {
    validate_event_capacity(PROVIDER, config.event_capacity)?;
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
    {
        return Err(invalid("intervals and limits must be greater than zero"));
    }
    validate_durations(
        PROVIDER,
        &[
            ("retry_interval", config.retry_interval),
            ("max_refresh_interval", config.max_refresh_interval),
        ],
    )
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
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use hickory_resolver::proto::rr::Name;

    use super::*;

    #[test]
    fn extreme_durations_are_rejected_before_resolver_construction() {
        for field in ["retry_interval", "max_refresh_interval"] {
            let mut config = DnsSrvDiscoveryConfig::new("_numax._tcp.example.");
            if field == "retry_interval" {
                config.retry_interval = Duration::MAX;
            } else {
                config.max_refresh_interval = Duration::MAX;
            }
            assert!(matches!(DnsSrvDiscovery::new(config),
                Err(DiscoveryError::InvalidConfiguration { provider, message })
                    if provider == PROVIDER && message.contains(field)));
        }
    }

    #[test]
    fn runtime_deadline_overflow_does_not_publish_or_renew_cached_answers() {
        let mut config = DnsSrvDiscoveryConfig::new("_numax._tcp.example.");
        let now = Instant::now();
        let state = DynamicState::new(8);
        state.observe_at(vec![("peer.example:9000".into(), now.into_std())]);
        let cached = state.snapshot();
        let valid_until = now + Duration::from_secs(10);
        config.max_refresh_interval = Duration::MAX;
        let error = apply_dns_answer(
            &config,
            &state,
            SrvAnswer {
                records: vec![SRV::new(
                    0,
                    0,
                    9000,
                    Name::from_ascii("peer.example.").unwrap(),
                )],
                valid_until,
            },
            Some(valid_until),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            DiscoveryError::Provider {
                retryable: false,
                ..
            }
        ));
        assert_eq!(state.snapshot(), cached);
        assert!(retry_deadline(now, Duration::MAX, Some(valid_until)).is_err());
        assert!(retry_deadline(now, Duration::MAX, None).is_err());
        let boundary = super::super::dynamic::deadline_boundary();
        assert!(retry_deadline(boundary, config.retry_interval, None).is_err());
        assert!(
            checked_deadline(
                now,
                bounded_negative_ttl(None, Duration::MAX),
                PROVIDER,
                "negative_ttl"
            )
            .is_err()
        );
        config.retry_interval = Duration::MAX;
        assert!(
            apply_dns_answer(
                &config,
                &state,
                SrvAnswer {
                    records: Vec::new(),
                    valid_until: now,
                },
                Some(valid_until)
            )
            .is_err()
        );
        assert!(state.snapshot().peers().is_empty());
    }

    #[tokio::test]
    async fn panicked_refresh_is_reported_after_clearing_snapshot() {
        let (provider, mut events) = panic_provider().await;
        super::super::dynamic::assert_invalidated(&mut events).await;
        let result = provider.shutdown().await;
        assert!(
            matches!(result, Err(DiscoveryError::Provider { retryable: false, message, .. })
            if message.contains("provider task failed") && message.contains("panic"))
        );
        assert!(provider.inner.state.snapshot().peers().is_empty());
        assert!(provider.inner.state.watch().snapshot().peers().is_empty());
        assert!(provider.watch().await.is_err());
    }

    async fn panic_provider() -> (DnsSrvDiscovery, DiscoveryWatch) {
        let record = SRV::new(0, 0, 9000, Name::from_ascii("cached.example.").unwrap());
        let resolver = Arc::new(SequenceResolver {
            steps: Mutex::new(VecDeque::from([
                ResolverStep::Success(vec![record.clone()], Duration::from_secs(30)),
                ResolverStep::Panic,
                ResolverStep::Success(vec![record], Duration::from_secs(30)),
            ])),
        });
        let mut config = DnsSrvDiscoveryConfig::new("_numax._tcp.example.");
        config.max_refresh_interval = Duration::from_millis(10);
        let provider = DnsSrvDiscovery::with_resolver(config, resolver);
        let mut events = provider.watch().await.unwrap();
        assert_eq!(
            super::super::next_changed_peers(&mut events, &[]).await,
            ["cached.example:9000"]
        );
        (provider, events)
    }

    #[tokio::test]
    async fn panic_invalidates_and_concurrent_subscribers_restart_one_dns_generation() {
        let (provider, mut events) = panic_provider().await;
        let old = provider
            .inner
            .lifecycle
            .lock()
            .unwrap()
            .task
            .clone()
            .unwrap();
        super::super::dynamic::assert_invalidated(&mut events).await;
        assert!(old.clone().join().await.is_err());
        assert!(provider.inner.state.snapshot().peers().is_empty());
        let (first, second) = tokio::join!(provider.watch(), provider.watch());
        let mut first = first.unwrap();
        second.unwrap();
        let current = provider
            .inner
            .lifecycle
            .lock()
            .unwrap()
            .task
            .clone()
            .unwrap();
        assert!(!old.same_generation(&current));
        provider.watch().await.unwrap();
        assert!(
            current.same_generation(
                provider
                    .inner
                    .lifecycle
                    .lock()
                    .unwrap()
                    .task
                    .as_ref()
                    .unwrap()
            )
        );
        assert_eq!(
            super::super::next_changed_peers(&mut first, &[]).await,
            ["cached.example:9000"]
        );
        provider.shutdown().await.unwrap();
        assert!(provider.watch().await.is_err());
    }

    #[tokio::test]
    async fn fatal_refresh_error_invalidates_but_does_not_restart() {
        let record = SRV::new(0, 0, 9000, Name::from_ascii("cached.example.").unwrap());
        let resolver = Arc::new(SequenceResolver {
            steps: Mutex::new(VecDeque::from([
                ResolverStep::Success(vec![record], Duration::from_secs(30)),
                ResolverStep::Fatal,
                ResolverStep::Panic,
            ])),
        });
        let mut config = DnsSrvDiscoveryConfig::new("_numax._tcp.example.");
        config.max_refresh_interval = Duration::from_millis(10);
        let provider = DnsSrvDiscovery::with_resolver(config, resolver.clone());
        let mut events = provider.watch().await.unwrap();
        assert_eq!(
            super::super::next_changed_peers(&mut events, &[]).await,
            ["cached.example:9000"]
        );
        super::super::dynamic::assert_invalidated(&mut events).await;
        let task = provider
            .inner
            .lifecycle
            .lock()
            .unwrap()
            .task
            .clone()
            .unwrap();
        assert_eq!(
            task.clone().join().await,
            Err(provider_error("injected fatal resolver error", false))
        );
        assert!(matches!(
            provider.watch().await,
            Err(DiscoveryError::Provider {
                retryable: false,
                ..
            })
        ));
        assert!(provider.inner.state.snapshot().peers().is_empty());
        assert!(
            task.same_generation(
                provider
                    .inner
                    .lifecycle
                    .lock()
                    .unwrap()
                    .task
                    .as_ref()
                    .unwrap()
            )
        );
        assert_eq!(resolver.steps.lock().unwrap().len(), 1);
        assert!(provider.shutdown().await.is_err());
    }

    enum ResolverStep {
        Success(Vec<SRV>, Duration),
        TransientFailure,
        Panic,
        Fatal,
    }

    struct SequenceResolver {
        steps: Mutex<VecDeque<ResolverStep>>,
    }

    #[async_trait]
    impl SrvResolver for SequenceResolver {
        async fn lookup(
            &self,
            _config: &DnsSrvDiscoveryConfig,
        ) -> Result<SrvAnswer, DiscoveryError> {
            let step = self.steps.lock().unwrap().pop_front();
            match step {
                Some(ResolverStep::Panic) => panic!("injected DNS refresh panic"),
                Some(ResolverStep::Fatal) => {
                    Err(provider_error("injected fatal resolver error", false))
                }
                Some(ResolverStep::Success(records, ttl)) => Ok(SrvAnswer {
                    records,
                    valid_until: Instant::now() + ttl,
                }),
                Some(ResolverStep::TransientFailure) => {
                    Err(provider_error("temporary resolver failure", true))
                }
                None => std::future::pending().await,
            }
        }
    }

    struct PendingResolver;

    #[async_trait]
    impl SrvResolver for PendingResolver {
        async fn lookup(
            &self,
            _config: &DnsSrvDiscoveryConfig,
        ) -> Result<SrvAnswer, DiscoveryError> {
            std::future::pending().await
        }
    }

    #[test]
    fn identical_fresh_dns_answer_renews_but_cached_answer_preserves_observation() {
        let config = DnsSrvDiscoveryConfig::new("_numax._tcp.example.");
        let state = DynamicState::new(8);
        let old = std::time::Instant::now() - Duration::from_secs(1);
        state.observe_at(vec![("peer.example:9000".into(), old)]);
        let record = SRV::new(0, 0, 9000, Name::from_ascii("peer.example.").unwrap());
        let valid_until = Instant::now() + Duration::from_secs(10);
        apply_dns_answer(
            &config,
            &state,
            SrvAnswer {
                records: vec![record.clone()],
                valid_until,
            },
            None,
        )
        .unwrap();
        let fresh = state.snapshot();
        assert_eq!(fresh.peers(), ["peer.example:9000"]);
        assert!(fresh.observations().unwrap()[0] > old);
        apply_dns_answer(
            &config,
            &state,
            SrvAnswer {
                records: vec![record],
                valid_until,
            },
            Some(valid_until),
        )
        .unwrap();
        assert_eq!(state.snapshot(), fresh);
    }

    #[tokio::test]
    async fn dns_view_expires_even_while_refresh_is_stalled() {
        let resolver = Arc::new(SequenceResolver {
            steps: Mutex::new(VecDeque::from([ResolverStep::Success(
                vec![SRV::new(
                    0,
                    0,
                    9000,
                    Name::from_ascii("peer.example.").unwrap(),
                )],
                Duration::from_millis(30),
            )])),
        });
        let mut config = DnsSrvDiscoveryConfig::new("_numax._tcp.example.");
        config.max_refresh_interval = Duration::from_millis(5);
        let provider = DnsSrvDiscovery::with_resolver(config, resolver);
        let mut watch = provider.watch().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            assert_eq!(
                super::super::observed_peers(watch.recv().await.unwrap().change),
                ["peer.example:9000"]
            );
            assert!(super::super::observed_peers(watch.recv().await.unwrap().change).is_empty());
        })
        .await
        .unwrap();
        provider.shutdown().await.unwrap();
    }

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

    #[test]
    fn transient_retry_never_outlives_the_last_valid_view() {
        let now = Instant::now();
        let valid_until = now + Duration::from_secs(2);

        assert_eq!(
            retry_deadline(now, Duration::from_secs(30), Some(valid_until)).unwrap(),
            valid_until
        );
        assert_eq!(
            retry_deadline(now, Duration::from_secs(1), Some(valid_until)).unwrap(),
            now + Duration::from_secs(1)
        );
    }

    #[test]
    fn negative_dns_ttl_is_preserved_and_bounded() {
        assert_eq!(
            bounded_negative_ttl(Some(30), Duration::from_secs(60)),
            Duration::from_secs(30)
        );
        assert_eq!(
            bounded_negative_ttl(Some(120), Duration::from_secs(60)),
            Duration::from_secs(60)
        );
        assert_eq!(
            bounded_negative_ttl(None, Duration::from_secs(60)),
            Duration::from_secs(60)
        );
    }

    #[test]
    fn already_expired_answers_are_not_published() {
        let config = DnsSrvDiscoveryConfig::new("_numax._tcp.example.");
        let state = DynamicState::new(8);
        state.replace(vec!["stale.example:9000".into()]);
        let answer = SrvAnswer {
            records: vec![SRV::new(
                0,
                0,
                9001,
                Name::from_ascii("expired.example.").unwrap(),
            )],
            valid_until: Instant::now(),
        };

        let (valid_until, _) = apply_dns_answer(&config, &state, answer, None).unwrap();

        assert!(valid_until.is_none());
        assert!(state.snapshot().peers().is_empty());
    }

    #[tokio::test]
    async fn refresh_expires_stale_data_and_recovers_after_a_transient_error() {
        let first = SRV::new(0, 0, 9001, Name::from_ascii("first.example.").unwrap());
        let second = SRV::new(0, 0, 9002, Name::from_ascii("second.example.").unwrap());
        let resolver = Arc::new(SequenceResolver {
            steps: Mutex::new(VecDeque::from([
                ResolverStep::Success(vec![first], Duration::from_millis(20)),
                ResolverStep::TransientFailure,
                ResolverStep::Success(vec![second], Duration::from_secs(1)),
            ])),
        });
        let mut config = DnsSrvDiscoveryConfig::new("_numax._tcp.example.");
        config.retry_interval = Duration::from_millis(10);
        let provider = DnsSrvDiscovery::with_resolver(config, resolver);
        let mut watch = provider.watch().await.unwrap();

        let first = tokio::time::timeout(Duration::from_secs(1), watch.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            super::super::observed_peers(first.change),
            ["first.example:9001"]
        );
        let expired = tokio::time::timeout(Duration::from_secs(1), watch.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            super::super::observed_peers(expired.change),
            Vec::<String>::new()
        );
        let recovered = tokio::time::timeout(Duration::from_secs(1), watch.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            super::super::observed_peers(recovered.change),
            ["second.example:9002"]
        );

        provider.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn requested_shutdown_clears_populated_snapshot_and_is_terminal() {
        let resolver = Arc::new(SequenceResolver {
            steps: Mutex::new(VecDeque::from([ResolverStep::Success(
                vec![SRV::new(
                    0,
                    0,
                    9000,
                    Name::from_ascii("cached.example.").unwrap(),
                )],
                Duration::from_secs(30),
            )])),
        });
        let provider = DnsSrvDiscovery::with_resolver(
            DnsSrvDiscoveryConfig::new("_numax._tcp.example."),
            resolver,
        );
        let mut events = provider.watch().await.unwrap();
        assert_eq!(
            super::super::next_changed_peers(&mut events, &[]).await,
            ["cached.example:9000"]
        );
        provider.shutdown().await.unwrap();
        provider.shutdown().await.unwrap();
        assert!(provider.inner.state.snapshot().peers().is_empty());
        assert!(
            super::super::next_changed_peers(&mut events, &["cached.example:9000".into()])
                .await
                .is_empty()
        );
        assert!(provider.watch().await.is_err());
        assert!(provider.discover().await.is_err());
    }

    #[tokio::test]
    async fn shutdown_cancels_a_stalled_lookup() {
        let config = DnsSrvDiscoveryConfig::new("_numax._tcp.example.");
        let provider = DnsSrvDiscovery::with_resolver(config, Arc::new(PendingResolver));
        provider.watch().await.unwrap();
        tokio::task::yield_now().await;

        tokio::time::timeout(Duration::from_secs(1), provider.shutdown())
            .await
            .unwrap()
            .unwrap();
    }
}
