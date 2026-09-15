use std::collections::{HashMap, HashSet};
use std::future::{Future, pending};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use nx_net::{BootstrapClient, BootstrapClientConfig, BootstrapRequest, NetError, WireRetryPolicy};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::dynamic::{AbortOnDropTask, DynamicState};
use super::{
    AnnouncementSupport, DEFAULT_DISCOVERY_CLUSTER, DEFAULT_DISCOVERY_EVENT_CAPACITY,
    DEFAULT_MAX_PEER_CANDIDATES, DiscoveryError, DiscoverySnapshot, DiscoveryWatch,
    PeerAnnouncement, PeerDiscovery,
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
    task: Option<JoinHandle<()>>,
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
        if let Some(task) = lifecycle.task.take() {
            task.abort();
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
        if lifecycle.task.is_some() {
            return Ok(());
        }
        let (shutdown, shutdown_rx) = watch::channel(false);
        let config = self.inner.config.clone();
        let client = self.inner.client.clone();
        let state = Arc::clone(&self.inner.state);
        let announcement_rx = self.inner.announcement_tx.subscribe();
        let announced_seeds = Arc::clone(&self.inner.announced_seeds);
        lifecycle.shutdown = Some(shutdown);
        lifecycle.task = Some(tokio::spawn(async move {
            run_bootstrap(
                config,
                client,
                state,
                announcement_rx,
                announced_seeds,
                shutdown_rx,
            )
            .await;
        }));
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
        if self
            .inner
            .lifecycle
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .stopped
        {
            return Err(provider_error("provider is shut down", false));
        }
        let endpoint = crate::sync_manager::canonicalize_endpoint(&announcement.endpoint)
            .map_err(|error| provider_error(error.to_string(), false))?;
        self.inner.announcement_tx.send_replace(Some(endpoint));
        Ok(())
    }

    async fn watch(&self) -> Result<DiscoveryWatch, DiscoveryError> {
        self.ensure_started()?;
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
                .map_err(|error| provider_error(format!("probe task failed: {error}"), false))?;
        }

        if self.inner.announcement_tx.borrow().is_some() {
            let announced_seeds = self
                .inner
                .announced_seeds
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .iter()
                .cloned()
                .collect::<Vec<_>>();
            let deadline = Instant::now() + SHUTDOWN_WITHDRAWAL_BUDGET;
            for (index, seed) in announced_seeds.iter().enumerate() {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let remaining_seeds = u32::try_from(announced_seeds.len() - index)
                    .unwrap_or(u32::MAX)
                    .max(1);
                let request = BootstrapRequest::new(self.inner.config.cluster_id.clone(), 1);
                match tokio::time::timeout(
                    remaining / remaining_seeds,
                    self.inner.client.query(seed, request),
                )
                .await
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
        self.inner
            .announced_seeds
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
        self.inner.announcement_tx.send_replace(None);
        self.inner.state.replace(Vec::new());
        Ok(())
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

    fn failed(&mut self, config: &BootstrapGossipDiscoveryConfig, error: &NetError, now: Instant) {
        self.disabled = bootstrap_error_is_fatal(error);
        // Preserve the configured cap on server-requested backoff, but retain
        // an absolute barrier independent of view expiry and announcements.
        self.not_before = now
            + bootstrap_retry_after(error)
                .unwrap_or_default()
                .min(config.retry_max);
        self.next_probe = now + self.retry_delay;
        self.retry_delay = self.retry_delay.saturating_mul(2).min(config.retry_max);
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
                    schedule.next_probe = Instant::now() + config.refresh_interval;
                    if announcement.is_some() {
                        announced_seeds
                            .lock()
                            .unwrap_or_else(|error| error.into_inner())
                            .insert(seed.clone());
                    }
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
                            expires_at: Instant::now()
                                + response.candidate_ttl.min(config.stale_after),
                        },
                    );
                }
                Err(error) => {
                    schedule.failed(&config, &error, Instant::now());
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
    matches!(
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
        || config.event_capacity == 0
    {
        return Err(invalid("intervals and limits are inconsistent"));
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
    use super::*;
    use nx_net::{BootstrapServerConfig, Node, NodeConfig};
    use nx_sync::NodeId;

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
