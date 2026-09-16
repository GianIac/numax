use std::collections::{HashMap, HashSet};
use std::future::{Future, pending};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use nx_net::{BootstrapClient, BootstrapClientConfig, BootstrapRequest, NetError, WireRetryPolicy};
use tokio::sync::watch;
use tokio::time::Instant;

use super::dynamic::{DynamicState, ProviderTask, checked_deadline, validate_durations};
use super::{
    AnnouncementSupport, DEFAULT_DISCOVERY_CLUSTER, DEFAULT_DISCOVERY_EVENT_CAPACITY,
    DEFAULT_MAX_PEER_CANDIDATES, DiscoveryError, DiscoverySnapshot, DiscoveryWatch,
    PeerAnnouncement, PeerDiscovery, validate_event_capacity,
};

const PROVIDER: &str = "bootstrap";
const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(20);
const DEFAULT_RETRY_INITIAL: Duration = Duration::from_millis(500);
const DEFAULT_RETRY_MAX: Duration = Duration::from_secs(30);
const DEFAULT_STALE_AFTER: Duration = Duration::from_secs(120);
const DEFAULT_MAX_SEEDS: usize = 32;
const SHUTDOWN_WITHDRAWAL_BUDGET: Duration = Duration::from_secs(4);

/// Seed probing, retention, and delivery policy for bootstrap gossip.
#[derive(Debug, Clone)]
pub struct BootstrapGossipDiscoveryConfig {
    pub seeds: Vec<String>,
    pub cluster_id: String,
    pub refresh_interval: Duration,
    pub retry_initial: Duration,
    pub retry_max: Duration,
    pub stale_after: Duration,
    pub max_seeds: usize,
    pub max_candidates: usize,
    /// Event channel capacity in `1..=super::MAX_DISCOVERY_EVENT_CAPACITY`.
    /// Defaults to [`DEFAULT_DISCOVERY_EVENT_CAPACITY`]; validated by the provider constructor.
    pub event_capacity: usize,
}

impl BootstrapGossipDiscoveryConfig {
    pub fn new(seeds: Vec<String>) -> Self {
        Self {
            seeds,
            cluster_id: DEFAULT_DISCOVERY_CLUSTER.to_string(),
            refresh_interval: DEFAULT_REFRESH_INTERVAL,
            retry_initial: DEFAULT_RETRY_INITIAL,
            retry_max: DEFAULT_RETRY_MAX,
            stale_after: DEFAULT_STALE_AFTER,
            max_seeds: DEFAULT_MAX_SEEDS,
            max_candidates: DEFAULT_MAX_PEER_CANDIDATES,
            event_capacity: DEFAULT_DISCOVERY_EVENT_CAPACITY,
        }
    }
}

struct Lifecycle {
    stopped: bool,
    shutdown: Option<watch::Sender<bool>>,
    task: Option<ProviderTask>,
}

struct Inner {
    config: BootstrapGossipDiscoveryConfig,
    client: BootstrapClient,
    state: Arc<DynamicState>,
    announcement_tx: watch::Sender<Option<String>>,
    announced_seeds: Arc<StdMutex<HashSet<String>>>,
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

/// Learns bounded endpoint suggestions from authenticated bootstrap seeds.
///
/// Authentication covers the seed that returned a response. Suggested
/// endpoints remain untrusted candidates and are authenticated independently
/// if the normal reconnection loop later dials them.
pub struct BootstrapGossipDiscovery {
    inner: Arc<Inner>,
}

impl BootstrapGossipDiscovery {
    pub fn new(
        mut config: BootstrapGossipDiscoveryConfig,
        client_config: BootstrapClientConfig,
    ) -> Result<Self, DiscoveryError> {
        validate_config(&config)?;
        if config.max_candidates > client_config.max_response_candidates {
            return Err(invalid(format!(
                "max_candidates exceeds the bootstrap client response limit of {}",
                client_config.max_response_candidates
            )));
        }
        let mut seen = HashSet::new();
        let mut seeds = Vec::with_capacity(config.seeds.len());
        for seed in &config.seeds {
            let seed = crate::sync_manager::canonicalize_endpoint(seed)
                .map_err(|error| invalid(format!("invalid bootstrap seed {seed:?}: {error}")))?;
            if seen.insert(seed.clone()) {
                seeds.push(seed);
            }
        }
        config.seeds = seeds;
        let client = BootstrapClient::new(client_config)
            .map_err(|error| invalid(format!("invalid bootstrap client: {error}")))?;
        let (announcement_tx, _) = watch::channel(None);
        Ok(Self {
            inner: Arc::new(Inner {
                state: Arc::new(DynamicState::new(config.event_capacity)),
                config,
                client,
                announcement_tx,
                announced_seeds: Arc::new(StdMutex::new(HashSet::new())),
                lifecycle: StdMutex::new(Lifecycle {
                    stopped: false,
                    shutdown: None,
                    task: None,
                }),
            }),
        })
    }

    fn ensure_started(&self) -> Result<(), DiscoveryError> {
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
        let client = self.inner.client.clone();
        let state = Arc::clone(&self.inner.state);
        let announcement_rx = self.inner.announcement_tx.subscribe();
        let announced_seeds = Arc::clone(&self.inner.announced_seeds);
        let mut cleanup = BootstrapCleanup {
            client: self.inner.client.clone(),
            cluster_id: self.inner.config.cluster_id.clone(),
            announcement_tx: self.inner.announcement_tx.clone(),
            announced_seeds: Arc::clone(&self.inner.announced_seeds),
            state: Arc::clone(&self.inner.state),
            preserve_announcement: true,
        };
        let cleanup_shutdown = shutdown_rx.clone();
        lifecycle.shutdown = Some(shutdown);
        lifecycle.task = Some(ProviderTask::spawn(
            PROVIDER,
            state.clone(),
            shutdown_rx.clone(),
            async move {
                run_bootstrap(
                    config,
                    client,
                    state,
                    announcement_rx,
                    announced_seeds,
                    shutdown_rx,
                )
                .await;
                Ok(())
            },
            move || async move {
                let result = cleanup.withdraw().await;
                cleanup.preserve_announcement =
                    !*cleanup_shutdown.borrow() && cleanup_shutdown.has_changed().is_ok();
                drop(cleanup);
                result
            },
        ));
        Ok(())
    }
}

#[async_trait]
impl PeerDiscovery for BootstrapGossipDiscovery {
    fn cluster_id(&self) -> &str {
        &self.inner.config.cluster_id
    }

    fn announcement_support(&self) -> AnnouncementSupport {
        AnnouncementSupport::Required
    }

    async fn discover(&self) -> Result<DiscoverySnapshot, DiscoveryError> {
        self.ensure_started()?;
        Ok(self.inner.state.snapshot())
    }

    async fn announce(&self, announcement: &PeerAnnouncement) -> Result<(), DiscoveryError> {
        let lifecycle = self
            .inner
            .lifecycle
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if lifecycle.stopped {
            return Err(provider_error("provider is shut down", false));
        }
        let endpoint = crate::sync_manager::canonicalize_endpoint(&announcement.endpoint)
            .map_err(|error| provider_error(error.to_string(), false))?;
        // Serialize publication with request_shutdown, not just its check.
        self.inner.announcement_tx.send_replace(Some(endpoint));
        Ok(())
    }

    async fn watch(&self) -> Result<DiscoveryWatch, DiscoveryError> {
        self.ensure_started()?;
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
        let result = match task {
            Some(task) => task.join().await,
            None => Ok(()),
        };
        self.inner.announcement_tx.send_replace(None);
        result
    }
}

struct BootstrapCleanup<C: SeedClient = BootstrapClient> {
    preserve_announcement: bool,
    client: C,
    cluster_id: String,
    announcement_tx: watch::Sender<Option<String>>,
    announced_seeds: Arc<StdMutex<HashSet<String>>>,
    state: Arc<DynamicState>,
}

impl<C: SeedClient> BootstrapCleanup<C> {
    async fn withdraw(&self) -> Result<(), DiscoveryError> {
        if self.announcement_tx.borrow().is_some() {
            let announced_seeds = self
                .announced_seeds
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .iter()
                .cloned()
                .collect::<Vec<_>>();
            let deadline = checked_deadline(
                Instant::now(),
                SHUTDOWN_WITHDRAWAL_BUDGET,
                PROVIDER,
                "withdrawal",
            )?;
            for (index, seed) in announced_seeds.iter().enumerate() {
                let now = Instant::now();
                let remaining = deadline.saturating_duration_since(now);
                if remaining.is_zero() {
                    break;
                }
                let remaining_seeds = u32::try_from(announced_seeds.len() - index)
                    .unwrap_or(u32::MAX)
                    .max(1);
                let request = BootstrapRequest::new(self.cluster_id.clone(), 1);
                let slot_deadline = checked_deadline(
                    now,
                    remaining / remaining_seeds,
                    PROVIDER,
                    "withdrawal slot",
                )?;
                match tokio::time::timeout_at(slot_deadline, self.client.query(seed, request)).await
                {
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => {
                        tracing::debug!(%error, %seed, "bootstrap announcement withdrawal failed");
                    }
                    Err(_) => {
                        tracing::debug!(%seed, "bootstrap announcement withdrawal timed out");
                    }
                }
            }
        }
        Ok(())
    }
}

impl<C: SeedClient> Drop for BootstrapCleanup<C> {
    fn drop(&mut self) {
        self.announced_seeds
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
        if !self.preserve_announcement {
            self.announcement_tx.send_replace(None);
        }
        self.state.replace(Vec::new());
    }
}

struct SeedView {
    endpoints: Vec<String>,
    expires_at: Instant,
    observed_at: std::time::Instant,
}

struct SeedSchedule {
    next_probe: Instant,
    not_before: Instant,
    retry_delay: Duration,
    disabled: bool,
}

impl SeedSchedule {
    fn deadline(&self) -> Option<Instant> {
        (!self.disabled).then_some(self.next_probe.max(self.not_before))
    }

    fn announce(&mut self, now: Instant) {
        self.next_probe = now;
    }

    fn failed(
        &mut self,
        config: &BootstrapGossipDiscoveryConfig,
        error: &NetError,
        now: Instant,
    ) -> Result<(), DiscoveryError> {
        self.disabled = bootstrap_error_is_fatal(error);
        // Preserve the configured cap on server-requested backoff, but retain
        // an absolute barrier independent of view expiry and announcements.
        // Disable before checking: an unrepresentable barrier must never turn
        // into an immediate retry, including after a new announcement.
        let was_disabled = self.disabled;
        self.disabled = true;
        self.not_before = checked_deadline(
            now,
            bootstrap_retry_after(error)
                .unwrap_or_default()
                .min(config.retry_max),
            PROVIDER,
            "retry_after",
        )?;
        self.next_probe = checked_deadline(now, self.retry_delay, PROVIDER, "retry_delay")?;
        self.retry_delay = self.retry_delay.saturating_mul(2).min(config.retry_max);
        self.disabled = was_disabled;
        Ok(())
    }
}

#[async_trait]
trait SeedClient: Send + Sync {
    async fn query(
        &self,
        seed: &str,
        request: BootstrapRequest,
    ) -> Result<nx_net::BootstrapResponse, NetError>;
}

#[async_trait]
impl SeedClient for BootstrapClient {
    async fn query(
        &self,
        seed: &str,
        request: BootstrapRequest,
    ) -> Result<nx_net::BootstrapResponse, NetError> {
        BootstrapClient::query(self, seed, request).await
    }
}

async fn run_bootstrap(
    config: BootstrapGossipDiscoveryConfig,
    client: impl SeedClient,
    state: Arc<DynamicState>,
    mut announcement_rx: watch::Receiver<Option<String>>,
    announced_seeds: Arc<StdMutex<HashSet<String>>>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut views = HashMap::<String, SeedView>::new();
    let now = Instant::now();
    let mut schedules: Vec<_> = config
        .seeds
        .iter()
        .map(|_| SeedSchedule {
            next_probe: now,
            not_before: now,
            retry_delay: config.retry_initial,
            disabled: false,
        })
        .collect();

    loop {
        if *shutdown_rx.borrow() || shutdown_rx.has_changed().is_err() {
            return;
        }
        let announcement = announcement_rx.borrow_and_update().clone();
        for (seed, schedule) in config.seeds.iter().zip(&mut schedules) {
            if schedule
                .deadline()
                .is_none_or(|deadline| deadline > Instant::now())
            {
                continue;
            }
            let mut request =
                BootstrapRequest::new(config.cluster_id.clone(), config.max_candidates);
            if let Some(endpoint) = &announcement {
                request = request.with_advertised_endpoint(endpoint.clone());
                // Sending may apply the announcement even when the response
                // is lost, the query is cancelled, or decoding fails.
                announced_seeds
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .insert(seed.clone());
            }
            let Some(result) = await_query_with_expiry(
                client.query(seed, request),
                &config,
                &mut views,
                &state,
                &mut shutdown_rx,
            )
            .await
            else {
                return;
            };
            match result {
                Ok(response) => {
                    schedule.retry_delay = config.retry_initial;
                    let now = Instant::now();
                    let deadlines = seed_deadlines(now, &config, response.candidate_ttl);
                    let (refresh, expires_at) = match deadlines {
                        Ok(deadlines) => deadlines,
                        Err(error) => {
                            tracing::error!(%error, %seed, "disabling bootstrap seed schedule");
                            schedule.disabled = true;
                            views.remove(seed);
                            publish_views(&config, &mut views, &state);
                            continue;
                        }
                    };
                    schedule.next_probe = refresh;
                    let mut endpoints = Vec::with_capacity(response.endpoints.len() + 1);
                    endpoints.push(seed.clone());
                    for endpoint in response.endpoints {
                        if !endpoints.contains(&endpoint) {
                            endpoints.push(endpoint);
                        }
                    }
                    endpoints.truncate(config.max_candidates);
                    views.insert(
                        seed.clone(),
                        SeedView {
                            endpoints,
                            observed_at: std::time::Instant::now(),
                            expires_at,
                        },
                    );
                }
                Err(error) => {
                    if let Err(error) = schedule.failed(&config, &error, Instant::now()) {
                        tracing::error!(%error, %seed, "disabling bootstrap seed schedule");
                    }
                    tracing::debug!(%error, %seed, "bootstrap seed query failed");
                }
            }
            publish_views(&config, &mut views, &state);
        }

        publish_views(&config, &mut views, &state);
        let next_expiry = views.values().map(|view| view.expires_at).min();
        let deadline = schedules
            .iter()
            .filter_map(SeedSchedule::deadline)
            .chain(next_expiry)
            .min();
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
            changed = announcement_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                let now = Instant::now();
                for schedule in &mut schedules { schedule.announce(now); }
            }
            _ = sleep_until_optional(deadline) => {}
        }
    }
}

fn seed_deadlines(
    now: Instant,
    config: &BootstrapGossipDiscoveryConfig,
    candidate_ttl: Duration,
) -> Result<(Instant, Instant), DiscoveryError> {
    let refresh = checked_deadline(now, config.refresh_interval, PROVIDER, "refresh_interval")?;
    let expiry = checked_deadline(
        now,
        candidate_ttl.min(config.stale_after),
        PROVIDER,
        "candidate_ttl",
    )?;
    Ok((refresh, expiry))
}

async fn await_query_with_expiry<F, T>(
    query: F,
    config: &BootstrapGossipDiscoveryConfig,
    views: &mut HashMap<String, SeedView>,
    state: &DynamicState,
    shutdown: &mut watch::Receiver<bool>,
) -> Option<T>
where
    F: Future<Output = T>,
{
    tokio::pin!(query);
    loop {
        if *shutdown.borrow() || shutdown.has_changed().is_err() {
            return None;
        }
        let next_expiry = views.values().map(|view| view.expires_at).min();
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return None;
                }
            }
            result = &mut query => return Some(result),
            _ = sleep_until_optional(next_expiry) => {
                publish_views(config, views, state);
            }
        }
    }
}

async fn sleep_until_optional(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => pending().await,
    }
}

fn publish_views(
    config: &BootstrapGossipDiscoveryConfig,
    views: &mut HashMap<String, SeedView>,
    state: &DynamicState,
) {
    let now = Instant::now();
    views.retain(|_, view| view.expires_at > now);
    let peers = flatten_views(&config.seeds, views, config.max_candidates);
    state.observe_at(
        peers
            .into_iter()
            .filter_map(|peer| {
                let observed_at = views
                    .values()
                    .filter(|view| view.endpoints.contains(&peer))
                    .map(|view| view.observed_at)
                    .max()?;
                Some((peer, observed_at))
            })
            .collect(),
    );
}

fn flatten_views(
    seeds: &[String],
    views: &HashMap<String, SeedView>,
    max_candidates: usize,
) -> Vec<String> {
    let mut result = Vec::new();
    for seed in seeds {
        let Some(view) = views.get(seed) else {
            continue;
        };
        for endpoint in &view.endpoints {
            if result.len() == max_candidates {
                return result;
            }
            if !result.contains(endpoint) {
                result.push(endpoint.clone());
            }
        }
    }
    result
}

fn bootstrap_error_is_fatal(error: &NetError) -> bool {
    matches!(error, NetError::InvalidConfig(_))
        || matches!(
            error,
            NetError::Wire(wire)
                if matches!(wire.retry_policy(), WireRetryPolicy::Fatal | WireRetryPolicy::RequestFatal)
        )
}

fn bootstrap_retry_after(error: &NetError) -> Option<Duration> {
    match error {
        NetError::Wire(wire) => match wire.retry_policy() {
            WireRetryPolicy::RetryAfter(delay) => Some(delay),
            _ => None,
        },
        _ => None,
    }
}

fn validate_config(config: &BootstrapGossipDiscoveryConfig) -> Result<(), DiscoveryError> {
    validate_event_capacity(PROVIDER, config.event_capacity)?;
    if config.seeds.is_empty() {
        return Err(invalid("at least one bootstrap seed is required"));
    }
    if config.seeds.len() > config.max_seeds {
        return Err(invalid(format!(
            "bootstrap seed count exceeds the {} seed limit",
            config.max_seeds
        )));
    }
    if config.cluster_id.is_empty() || config.cluster_id.len() > 128 {
        return Err(invalid("cluster_id length must be in 1..=128 bytes"));
    }
    if config.refresh_interval.is_zero()
        || config.retry_initial.is_zero()
        || config.retry_max < config.retry_initial
        || config.stale_after.is_zero()
        || config.max_seeds == 0
        || config.max_candidates == 0
        || config.max_candidates > nx_net::MAX_BOOTSTRAP_RESPONSE_CAPACITY
    {
        return Err(invalid("intervals and limits are inconsistent"));
    }
    validate_durations(
        PROVIDER,
        &[
            ("refresh_interval", config.refresh_interval),
            ("retry_initial", config.retry_initial),
            ("retry_max", config.retry_max),
            ("stale_after", config.stale_after),
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
    use super::*;
    use nx_net::{BootstrapServerConfig, Node, NodeConfig};
    use nx_sync::NodeId;

    #[test]
    fn extreme_durations_are_rejected_before_starting() {
        for field in [
            "refresh_interval",
            "retry_initial",
            "retry_max",
            "stale_after",
        ] {
            let mut config = BootstrapGossipDiscoveryConfig::new(vec!["seed:9000".into()]);
            match field {
                "refresh_interval" => config.refresh_interval = Duration::MAX,
                "retry_initial" => {
                    config.retry_initial = Duration::MAX;
                    config.retry_max = Duration::MAX;
                }
                "retry_max" => config.retry_max = Duration::MAX,
                "stale_after" => config.stale_after = Duration::MAX,
                _ => unreachable!(),
            }
            assert!(matches!(
                BootstrapGossipDiscovery::new(config, BootstrapClientConfig::new(NodeId::new("client"))),
                Err(DiscoveryError::InvalidConfiguration { provider, message })
                    if provider == PROVIDER && message.contains(field)
            ));
        }
    }

    #[test]
    fn runtime_overflow_disables_retry_even_after_announcement() {
        let now = Instant::now();
        let mut config = BootstrapGossipDiscoveryConfig::new(vec!["seed:9000".into()]);
        let mut schedule = SeedSchedule {
            next_probe: now,
            not_before: now,
            retry_delay: Duration::MAX,
            disabled: false,
        };
        let error = NetError::Wire(nx_net::WireError::RateLimited {
            retry_after_ms: Some(200),
        });
        assert!(matches!(
            schedule.failed(&config, &error, now),
            Err(DiscoveryError::Provider {
                retryable: false,
                ..
            })
        ));
        schedule.announce(now);
        assert_eq!(schedule.deadline(), None);
        assert!(schedule.not_before >= now + Duration::from_millis(200));

        config.refresh_interval = Duration::MAX;
        assert!(seed_deadlines(now, &config, Duration::from_secs(1)).is_err());
        config.refresh_interval = Duration::from_secs(1);
        config.stale_after = Duration::MAX;
        assert!(seed_deadlines(now, &config, Duration::MAX).is_err());
    }

    #[test]
    fn representable_seed_policy_rejects_overflow_after_clock_advance() {
        let config = BootstrapGossipDiscoveryConfig::new(vec!["seed:9000".into()]);
        validate_config(&config).unwrap();
        let now = super::super::dynamic::deadline_boundary();
        let mut schedule = SeedSchedule {
            next_probe: now,
            not_before: now,
            retry_delay: config.retry_initial,
            disabled: false,
        };
        let error = NetError::Wire(nx_net::WireError::RateLimited {
            retry_after_ms: Some(2000),
        });
        assert!(schedule.failed(&config, &error, now).is_err());
        schedule.announce(now);
        assert_eq!(schedule.deadline(), None);
        assert!(seed_deadlines(now, &config, Duration::from_secs(1)).is_err());
    }

    async fn assert_panicked_shutdown_withdraws(restart: bool) {
        let seed = Node::try_new(
            NodeConfig::new(NodeId::new("seed"), "127.0.0.1:0")
                .with_bootstrap_server(BootstrapServerConfig::new("default").unwrap()),
        )
        .unwrap();
        let bound = seed.start_listener().await.unwrap().to_string();
        seed.announce_bootstrap_endpoint(bound.clone()).unwrap();
        let mut config = BootstrapGossipDiscoveryConfig::new(vec![bound.clone()]);
        config.max_candidates = 4;
        config.refresh_interval = Duration::from_millis(10);
        let provider = BootstrapGossipDiscovery::new(
            config,
            BootstrapClientConfig::new(NodeId::new("client")),
        )
        .unwrap();
        let advertised = "127.0.0.1:43111";
        provider
            .announce(&PeerAnnouncement {
                endpoint: advertised.into(),
            })
            .await
            .unwrap();
        let mut events = provider.watch().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap();
        let observer =
            BootstrapClient::new(BootstrapClientConfig::new(NodeId::new("observer"))).unwrap();
        let before = observer
            .query(&bound, BootstrapRequest::new("default", 4))
            .await
            .unwrap();
        assert!(before.endpoints.contains(&advertised.to_string()));

        let old = provider
            .inner
            .lifecycle
            .lock()
            .unwrap()
            .task
            .clone()
            .unwrap();
        provider.inner.state.panic_on_next_observation();
        super::super::dynamic::assert_invalidated(&mut events).await;
        let result = tokio::time::timeout(Duration::from_secs(5), old.clone().join())
            .await
            .unwrap();
        assert!(
            matches!(result, Err(DiscoveryError::Provider { retryable: false, message, .. })
            if message.contains("provider task failed") && message.contains("panic"))
        );
        assert!(provider.inner.state.snapshot().peers().is_empty());
        assert!(provider.inner.state.watch().snapshot().peers().is_empty());
        assert!(provider.inner.announced_seeds.lock().unwrap().is_empty());
        let after = observer
            .query(&bound, BootstrapRequest::new("default", 4))
            .await
            .unwrap();
        assert_eq!(after.endpoints, std::slice::from_ref(&bound));
        if restart {
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
            tokio::time::timeout(Duration::from_secs(2), first.recv())
                .await
                .unwrap()
                .unwrap();
            let renewed = observer
                .query(&bound, BootstrapRequest::new("default", 4))
                .await
                .unwrap();
            assert!(renewed.endpoints.contains(&advertised.to_string()));
            provider.shutdown().await.unwrap();
        } else {
            assert!(provider.shutdown().await.is_err());
        }
        assert!(provider.watch().await.is_err());
        assert!(provider.inner.announcement_tx.borrow().is_none());
        seed.shutdown().await;
    }

    #[tokio::test]
    async fn panicked_probe_still_withdraws_and_clears_snapshot() {
        assert_panicked_shutdown_withdraws(false).await;
    }

    #[tokio::test]
    async fn panic_invalidates_and_concurrent_subscribers_restart_one_bootstrap_generation() {
        assert_panicked_shutdown_withdraws(true).await;
    }

    #[derive(Clone, Copy)]
    enum AnnouncementAck {
        Pending,
        Panic,
        Lost,
    }

    #[derive(Clone)]
    struct AppliedWithoutAck {
        applied: Arc<StdMutex<Option<String>>>,
        calls: tokio::sync::mpsc::Sender<&'static str>,
        withdrawal_ack: Arc<tokio::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>>,
        announcement_ack: AnnouncementAck,
    }

    #[async_trait]
    impl SeedClient for AppliedWithoutAck {
        async fn query(
            &self,
            _seed: &str,
            request: BootstrapRequest,
        ) -> Result<nx_net::BootstrapResponse, NetError> {
            if let Some(endpoint) = request.advertised_endpoint {
                *self.applied.lock().unwrap() = Some(endpoint);
                self.calls.send("applied-without-ack").await.unwrap();
                return match self.announcement_ack {
                    AnnouncementAck::Pending => pending().await,
                    AnnouncementAck::Panic => panic!("injected probe panic after seed application"),
                    AnnouncementAck::Lost => Err(NetError::Timeout),
                };
            }
            self.applied.lock().unwrap().take();
            self.calls.send("withdrawal-applied").await.unwrap();
            if let Some(ack) = self.withdrawal_ack.lock().await.take() {
                ack.await.unwrap();
            }
            Ok(nx_net::BootstrapResponse {
                seed_node_id: NodeId::new("seed"),
                endpoints: Vec::new(),
                candidate_ttl: Duration::from_secs(30),
            })
        }
    }

    async fn assert_lost_ack_is_withdrawn(announcement_ack: AnnouncementAck, drop_provider: bool) {
        let panic = matches!(announcement_ack, AnnouncementAck::Panic);
        let mut config = BootstrapGossipDiscoveryConfig::new(vec!["seed:9000".into()]);
        config.max_candidates = 4;
        let provider = BootstrapGossipDiscovery::new(
            config.clone(),
            BootstrapClientConfig::new(NodeId::new("client")),
        )
        .unwrap();
        provider
            .announce(&PeerAnnouncement {
                endpoint: "local:9000".into(),
            })
            .await
            .unwrap();
        let (calls, mut call_rx) = tokio::sync::mpsc::channel(8);
        let (release, released) = tokio::sync::oneshot::channel();
        let client = AppliedWithoutAck {
            applied: Arc::new(StdMutex::new(None)),
            calls,
            withdrawal_ack: Arc::new(tokio::sync::Mutex::new(Some(released))),
            announcement_ack,
        };
        let cleanup = BootstrapCleanup {
            client: client.clone(),
            cluster_id: config.cluster_id.clone(),
            announcement_tx: provider.inner.announcement_tx.clone(),
            announced_seeds: provider.inner.announced_seeds.clone(),
            state: provider.inner.state.clone(),
            preserve_announcement: false,
        };
        let (stop, stop_rx) = watch::channel(false);
        // A full prior view must be cleared even if query panics before any ACK.
        provider.inner.state.observe(vec!["cached:9000".into()]);
        let mut events = provider.inner.state.watch();
        let state = provider.inner.state.clone();
        let announcement = provider.inner.announcement_tx.subscribe();
        let seeds = provider.inner.announced_seeds.clone();
        let worker_client = client.clone();
        let task = ProviderTask::spawn(
            PROVIDER,
            state.clone(),
            stop_rx.clone(),
            async move {
                run_bootstrap(config, worker_client, state, announcement, seeds, stop_rx).await;
                Ok(())
            },
            move || async move { cleanup.withdraw().await },
        );
        let completion = task.clone();
        {
            let mut lifecycle = provider.inner.lifecycle.lock().unwrap();
            lifecycle.shutdown = Some(stop);
            lifecycle.task = Some(task);
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            assert_eq!(call_rx.recv().await, Some("applied-without-ack"));
            assert!(provider.inner.announced_seeds.lock().unwrap().contains("seed:9000"));
            if panic {
                super::super::dynamic::assert_invalidated(&mut events).await;
            } else {
                assert_eq!(client.applied.lock().unwrap().as_deref(), Some("local:9000"));
            }
            if matches!(announcement_ack, AnnouncementAck::Lost) {
                // Publication follows processing the failed query. The seed
                // must remain tracked even after the timeout result is handled.
                assert!(super::super::observed_peers(events.recv().await.unwrap().change).is_empty());
                assert!(provider.inner.announced_seeds.lock().unwrap().contains("seed:9000"));
            }
            if drop_provider {
                let state = provider.inner.state.clone();
                let seeds = provider.inner.announced_seeds.clone();
                let announcement = provider.inner.announcement_tx.clone();
                drop(provider);
                assert_eq!(call_rx.recv().await, Some("withdrawal-applied"));
                assert!(!completion.completion_ready());
                release.send(()).unwrap();
                completion.join().await.unwrap();
                assert!(client.applied.lock().unwrap().is_none());
                assert!(state.snapshot().peers().is_empty());
                assert!(seeds.lock().unwrap().is_empty());
                assert!(announcement.borrow().is_none());
                return;
            }
            let mut waiter = Box::pin(provider.shutdown());
            std::future::poll_fn(|cx| {
                assert!(waiter.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            }).await;
            assert_eq!(call_rx.recv().await, Some("withdrawal-applied"));
            drop(waiter);
            assert!(!completion.completion_ready());
            // The seed applied withdrawal; only its ACK is deliberately held.
            assert!(client.applied.lock().unwrap().is_none());
            release.send(()).unwrap();
            let result = completion.join().await;
            if panic {
                assert!(matches!(result, Err(DiscoveryError::Provider { message, .. }) if message.contains("panic")));
                assert!(provider.shutdown().await.is_err());
            } else {
                result.unwrap();
                provider.shutdown().await.unwrap();
            }
            assert!(provider.inner.state.snapshot().peers().is_empty());
            assert!(provider.inner.announced_seeds.lock().unwrap().is_empty());
            assert!(provider.inner.announcement_tx.borrow().is_none());
            assert!(call_rx.try_recv().is_err());
            assert!(provider.watch().await.is_err());
        }).await.unwrap();
    }

    #[tokio::test]
    async fn lost_announcement_ack_is_withdrawn_after_cancelled_shutdown_wait() {
        assert_lost_ack_is_withdrawn(AnnouncementAck::Pending, false).await;
    }

    #[tokio::test]
    async fn timed_out_announcement_ack_keeps_seed_tracked_for_withdrawal() {
        assert_lost_ack_is_withdrawn(AnnouncementAck::Lost, false).await;
    }

    #[tokio::test]
    async fn panic_after_seed_application_still_withdraws_without_announcement_ack() {
        assert_lost_ack_is_withdrawn(AnnouncementAck::Panic, false).await;
    }

    #[tokio::test]
    async fn dropping_provider_still_withdraws_an_announcement_without_ack() {
        assert_lost_ack_is_withdrawn(AnnouncementAck::Pending, true).await;
    }

    #[tokio::test]
    async fn missing_withdrawal_ack_remains_bounded_best_effort_and_idempotent() {
        let (calls, mut call_rx) = tokio::sync::mpsc::channel(8);
        let (_release, released) = tokio::sync::oneshot::channel();
        let client = AppliedWithoutAck {
            applied: Arc::new(StdMutex::new(Some("local:9000".into()))),
            calls,
            withdrawal_ack: Arc::new(tokio::sync::Mutex::new(Some(released))),
            announcement_ack: AnnouncementAck::Pending,
        };
        let (announcement_tx, _) = watch::channel(Some("local:9000".into()));
        let cleanup = BootstrapCleanup {
            client: client.clone(),
            cluster_id: "default".into(),
            announcement_tx,
            announced_seeds: Arc::new(StdMutex::new(HashSet::from(["seed:9000".into()]))),
            state: Arc::new(DynamicState::new(8)),
            preserve_announcement: false,
        };
        tokio::time::timeout(
            SHUTDOWN_WITHDRAWAL_BUDGET + Duration::from_secs(1),
            cleanup.withdraw(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(call_rx.recv().await, Some("withdrawal-applied"));
        assert!(client.applied.lock().unwrap().is_none());
        // Repeating removal of the same NodeId remains harmless.
        cleanup.withdraw().await.unwrap();
        assert_eq!(call_rx.recv().await, Some("withdrawal-applied"));
        assert!(client.applied.lock().unwrap().is_none());
    }

    struct ControlledClient {
        calls: tokio::sync::mpsc::Sender<(String, Instant)>,
        limited_calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl SeedClient for ControlledClient {
        async fn query(
            &self,
            seed: &str,
            _request: BootstrapRequest,
        ) -> Result<nx_net::BootstrapResponse, NetError> {
            self.calls
                .send((seed.to_string(), Instant::now()))
                .await
                .unwrap();
            if seed == "limited:1"
                && self
                    .limited_calls
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    > 0
            {
                return Err(NetError::Wire(nx_net::WireError::RateLimited {
                    retry_after_ms: Some(200),
                }));
            }
            Ok(nx_net::BootstrapResponse {
                seed_node_id: NodeId::new(seed),
                endpoints: Vec::new(),
                candidate_ttl: Duration::from_millis(40),
            })
        }
    }

    #[tokio::test]
    async fn retry_after_survives_actual_view_expiry_and_announcement_while_healthy_seeds_progress()
    {
        let mut config =
            BootstrapGossipDiscoveryConfig::new(vec!["limited:1".into(), "healthy:2".into()]);
        config.refresh_interval = Duration::from_millis(10);
        config.retry_initial = Duration::from_millis(10);
        config.retry_max = Duration::from_millis(100); // configured cap still applies
        let state = Arc::new(DynamicState::new(128));
        let mut events = state.watch();
        let (calls, mut calls_rx) = tokio::sync::mpsc::channel(128);
        let client = ControlledClient {
            calls,
            limited_calls: std::sync::atomic::AtomicUsize::new(0),
        };
        let (announcement, announcement_rx) = watch::channel(None);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(run_bootstrap(
            config,
            client,
            state,
            announcement_rx,
            Arc::new(StdMutex::new(HashSet::new())),
            shutdown_rx,
        ));
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut limited = 0;
            let limited_at = loop {
                let (seed, at) = calls_rx.recv().await.unwrap();
                if seed == "limited:1" {
                    limited += 1;
                }
                if limited == 2 {
                    break at;
                }
            };
            // Wait for the limited seed's retained view to actually disappear.
            loop {
                let peers = super::super::observed_peers(events.recv().await.unwrap().change);
                if !peers.contains(&"limited:1".to_string()) {
                    break;
                }
            }
            announcement.send_replace(Some("local:3".into()));
            let mut healthy_progress = false;
            loop {
                let (seed, at) = calls_rx.recv().await.unwrap();
                if seed == "healthy:2"
                    && at >= limited_at
                    && at < limited_at + Duration::from_millis(100)
                {
                    healthy_progress = true;
                }
                if seed == "limited:1" {
                    assert!(at >= limited_at + Duration::from_millis(100));
                    assert!(healthy_progress);
                    break;
                }
            }
        })
        .await
        .unwrap();
        shutdown.send_replace(true);
        task.await.unwrap();
    }

    #[test]
    fn cached_seed_views_do_not_refresh_observations_on_failure_or_other_seed_expiry() {
        let config = BootstrapGossipDiscoveryConfig::new(vec!["a:1".into(), "b:2".into()]);
        let state = DynamicState::new(8);
        let now = std::time::Instant::now();
        let mut views = HashMap::from([
            (
                "a:1".into(),
                SeedView {
                    endpoints: vec!["a:1".into()],
                    expires_at: Instant::now() + Duration::from_secs(10),
                    observed_at: now,
                },
            ),
            (
                "b:2".into(),
                SeedView {
                    endpoints: vec!["b:2".into()],
                    expires_at: Instant::now() + Duration::from_secs(10),
                    observed_at: now,
                },
            ),
        ]);
        publish_views(&config, &mut views, &state);
        let first = state.snapshot();
        publish_views(&config, &mut views, &state);
        assert_eq!(first, state.snapshot());
        views.get_mut("b:2").unwrap().expires_at = Instant::now();
        publish_views(&config, &mut views, &state);
        assert_eq!(state.snapshot().peers(), ["a:1"]);
        assert_eq!(state.snapshot().observations().unwrap(), [now]);
    }

    #[test]
    fn views_are_bounded_deduplicated_and_follow_seed_order() {
        let views = HashMap::from([
            (
                "a:1".into(),
                SeedView {
                    endpoints: vec!["a:1".into(), "shared:3".into()],
                    expires_at: Instant::now() + Duration::from_secs(1),
                    observed_at: std::time::Instant::now(),
                },
            ),
            (
                "b:2".into(),
                SeedView {
                    endpoints: vec!["b:2".into(), "shared:3".into()],
                    expires_at: Instant::now() + Duration::from_secs(1),
                    observed_at: std::time::Instant::now(),
                },
            ),
        ]);
        assert_eq!(
            flatten_views(&["b:2".into(), "a:1".into()], &views, 3),
            ["b:2", "shared:3", "a:1"]
        );
    }

    #[test]
    fn invalid_or_unbounded_seed_configuration_is_rejected() {
        let mut config = BootstrapGossipDiscoveryConfig::new(vec!["seed:9000".into()]);
        config.max_seeds = 0;
        assert!(validate_config(&config).is_err());

        config.max_seeds = 1;
        config.max_candidates = nx_net::MAX_BOOTSTRAP_RESPONSE_CAPACITY;
        assert!(validate_config(&config).is_ok());
        config.max_candidates += 1;
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn expiry_and_announcement_do_not_override_a_seeds_not_before() {
        let now = Instant::now();
        let mut schedule = SeedSchedule {
            next_probe: now,
            not_before: now + Duration::from_secs(30),
            retry_delay: Duration::from_secs(1),
            disabled: false,
        };
        let mut healthy = SeedSchedule {
            next_probe: now + Duration::from_secs(5),
            not_before: now,
            retry_delay: Duration::from_secs(1),
            disabled: false,
        };
        schedule.announce(now + Duration::from_secs(2));
        healthy.announce(now + Duration::from_secs(2));
        assert_eq!(schedule.deadline(), Some(now + Duration::from_secs(30)));
        assert_eq!(healthy.deadline(), Some(now + Duration::from_secs(2)));
        // An expiry wakeup never changes the per-seed schedule.
        let expiry = now + Duration::from_secs(3);
        assert!(schedule.deadline().unwrap() > expiry);
    }

    #[tokio::test]
    async fn candidate_view_expires_while_a_seed_query_is_stalled() {
        let config = BootstrapGossipDiscoveryConfig::new(vec!["seed:9000".into()]);
        let state = DynamicState::new(8);
        state.replace(vec!["peer:9000".into()]);
        let mut watch = state.watch();
        let mut views = HashMap::from([(
            "seed:9000".into(),
            SeedView {
                endpoints: vec!["peer:9000".into()],
                expires_at: Instant::now() + Duration::from_millis(10),
                observed_at: std::time::Instant::now(),
            },
        )]);
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let wait = await_query_with_expiry(
            std::future::pending::<()>(),
            &config,
            &mut views,
            &state,
            &mut shutdown_rx,
        );
        tokio::pin!(wait);
        let event = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::select! {
                _ = &mut wait => panic!("pending query unexpectedly completed"),
                event = watch.recv() => event.unwrap(),
            }
        })
        .await
        .unwrap();

        assert_eq!(
            super::super::observed_peers(event.change),
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn provider_learns_candidates_and_withdraws_its_announcement() {
        let seed = Node::new(
            NodeConfig::new(NodeId::new("seed"), "127.0.0.1:0").with_bootstrap_server(
                BootstrapServerConfig::new("cluster-a")
                    .unwrap()
                    .with_max_response_candidates(4)
                    .unwrap(),
            ),
        );
        let bound = seed.start_listener().await.unwrap();
        seed.announce_bootstrap_endpoint(bound.to_string()).unwrap();

        let mut client_config = BootstrapClientConfig::new(NodeId::new("client"));
        client_config.max_response_candidates = 4;
        let mut config = BootstrapGossipDiscoveryConfig::new(vec![bound.to_string()]);
        config.cluster_id = "cluster-a".into();
        config.max_candidates = 4;
        config.refresh_interval = Duration::from_secs(1);
        config.retry_initial = Duration::from_millis(10);
        config.retry_max = Duration::from_millis(20);
        let provider = BootstrapGossipDiscovery::new(config, client_config).unwrap();
        provider
            .announce(&PeerAnnouncement {
                endpoint: "127.0.0.1:43111".into(),
            })
            .await
            .unwrap();
        let mut watch = provider.watch().await.unwrap();

        let event = tokio::time::timeout(Duration::from_secs(2), watch.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            super::super::observed_peers(event.change),
            vec![bound.to_string()]
        );
        provider.shutdown().await.unwrap();
        provider.shutdown().await.unwrap();
        assert!(provider.inner.state.snapshot().peers().is_empty());
        assert!(provider.inner.announced_seeds.lock().unwrap().is_empty());
        assert!(provider.inner.announcement_tx.borrow().is_none());
        assert!(provider.watch().await.is_err());

        let observer =
            BootstrapClient::new(BootstrapClientConfig::new(NodeId::new("observer"))).unwrap();
        let response = observer
            .query(&bound.to_string(), BootstrapRequest::new("cluster-a", 4))
            .await
            .unwrap();
        assert_eq!(response.endpoints, [bound.to_string()]);
        seed.shutdown().await;
    }

    #[tokio::test]
    async fn provider_expires_candidates_and_recovers_after_seed_restart() {
        let server_config = BootstrapServerConfig::new("cluster-a")
            .unwrap()
            .with_candidate_ttl(Duration::from_millis(40))
            .unwrap();
        let seed = Node::new(
            NodeConfig::new(NodeId::new("seed"), "127.0.0.1:0")
                .with_bootstrap_server(server_config.clone()),
        );
        let bound = seed.start_listener().await.unwrap();
        seed.announce_bootstrap_endpoint(bound.to_string()).unwrap();

        let mut client_config = BootstrapClientConfig::new(NodeId::new("client"));
        client_config.max_response_candidates = 4;
        let mut config = BootstrapGossipDiscoveryConfig::new(vec![bound.to_string()]);
        config.cluster_id = "cluster-a".into();
        config.max_candidates = 4;
        config.refresh_interval = Duration::from_millis(10);
        config.retry_initial = Duration::from_millis(10);
        config.retry_max = Duration::from_millis(20);
        config.stale_after = Duration::from_millis(40);
        let provider = BootstrapGossipDiscovery::new(config, client_config).unwrap();
        let mut watch = provider.watch().await.unwrap();

        let discovered = tokio::time::timeout(Duration::from_secs(2), watch.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            super::super::observed_peers(discovered.change),
            vec![bound.to_string()]
        );

        seed.shutdown().await;
        assert!(
            super::super::next_changed_peers(&mut watch, &[bound.to_string()])
                .await
                .is_empty()
        );

        let restarted = Node::new(
            NodeConfig::new(NodeId::new("seed-restarted"), bound.to_string())
                .with_bootstrap_server(server_config),
        );
        restarted.start_listener().await.unwrap();
        restarted
            .announce_bootstrap_endpoint(bound.to_string())
            .unwrap();
        let recovered = tokio::time::timeout(Duration::from_secs(2), watch.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            super::super::observed_peers(recovered.change),
            vec![bound.to_string()]
        );

        provider.shutdown().await.unwrap();
        restarted.shutdown().await;
    }
}
