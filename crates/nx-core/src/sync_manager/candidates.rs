use std::collections::{HashMap, HashSet};
use std::future::pending;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant as StdInstant};

use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant as TokioInstant;
use tracing::{debug, warn};

use crate::discovery::{
    AbortOnDropTask, AnnouncementSupport, DiscoveryChange, DiscoveryError, DiscoveryProvider,
    DiscoveryRuntimeConfig, DiscoverySnapshot, DiscoveryWatch, PeerAnnouncement,
};

const DISCOVERY_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(500);
const DISCOVERY_RETRY_MAX_DELAY: Duration = Duration::from_secs(30);
const DISCOVERY_OPERATION_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy)]
struct CandidateContribution {
    expires_at: Option<StdInstant>,
}

#[derive(Debug, Clone, Default)]
struct CandidateRecord {
    sources: HashMap<String, CandidateContribution>,
}

#[derive(Debug, Clone)]
struct CandidateRegistry {
    max_candidates: usize,
    order: Vec<String>,
    source_order: Vec<String>,
    source_candidates: HashMap<String, Vec<String>>,
    records: HashMap<String, CandidateRecord>,
    local_endpoints: HashSet<String>,
}

impl CandidateRegistry {
    fn new(max_candidates: usize) -> Result<Self, DiscoveryError> {
        if max_candidates == 0 {
            return Err(configuration_error(
                "coordinator",
                "max_candidates must be greater than zero",
            ));
        }
        Ok(Self {
            max_candidates,
            order: Vec::new(),
            source_order: Vec::new(),
            source_candidates: HashMap::new(),
            records: HashMap::new(),
            local_endpoints: HashSet::new(),
        })
    }

    fn endpoints(&self) -> Arc<Vec<String>> {
        Arc::new(
            self.order
                .iter()
                .filter(|endpoint| self.records.contains_key(*endpoint))
                .cloned()
                .collect(),
        )
    }

    fn replace_source(
        &mut self,
        source_id: &str,
        peers: &[String],
        ttl: Option<Duration>,
        now: StdInstant,
    ) -> Result<bool, DiscoveryError> {
        if peers.len() > self.max_candidates {
            return Err(configuration_error(
                "coordinator",
                format!(
                    "discovery snapshot exceeds the {} candidate limit",
                    self.max_candidates
                ),
            ));
        }
        let mut canonical = Vec::new();
        let mut seen = HashSet::new();
        for peer in peers {
            let endpoint = match canonicalize_endpoint(peer) {
                Ok(endpoint) => endpoint,
                Err(error) => {
                    warn!(
                        source = %source_id,
                        endpoint = %peer,
                        error = %error,
                        "rejected invalid discovery snapshot candidate"
                    );
                    continue;
                }
            };
            if seen.insert(endpoint.clone()) {
                canonical.push(endpoint);
            }
        }
        if canonical.len() > self.max_candidates {
            return Err(configuration_error(
                "coordinator",
                format!(
                    "discovery snapshot exceeds the {} candidate limit",
                    self.max_candidates
                ),
            ));
        }

        let before = self.endpoints();
        let mut updated = self.clone();
        updated.register_source(source_id);
        updated
            .source_candidates
            .insert(source_id.to_string(), canonical.clone());
        let retained = canonical.iter().cloned().collect::<HashSet<_>>();
        updated.remove_source_except(source_id, &retained);
        for endpoint in canonical {
            updated.add(source_id, endpoint, ttl, now)?;
        }
        updated.rebuild_order();
        let changed = updated.endpoints() != before;
        *self = updated;
        Ok(changed)
    }

    fn replace_snapshot(
        &mut self,
        source_id: &str,
        snapshot: &DiscoverySnapshot,
        ttl: Option<Duration>,
        now: StdInstant,
    ) -> Result<bool, DiscoveryError> {
        let Some(observations) = snapshot.observations() else {
            return self.replace_source(source_id, snapshot.peers(), ttl, now);
        };
        if snapshot.peers().len() > self.max_candidates {
            return Err(configuration_error(
                source_id,
                "discovery snapshot exceeds candidate limit",
            ));
        }
        let mut observed = HashMap::<String, StdInstant>::new();
        let mut peers = Vec::new();
        for (peer, at) in snapshot.peers().iter().zip(observations) {
            if let Some(ttl) = ttl {
                let deadline = at.checked_add(ttl).ok_or_else(|| {
                    configuration_error(source_id, "candidate_ttl exceeds the platform time range")
                })?;
                if deadline <= now {
                    continue;
                }
            }
            if let Ok(peer) = canonicalize_endpoint(peer) {
                peers.push(peer.clone());
                observed
                    .entry(peer)
                    .and_modify(|old| *old = (*old).max(*at))
                    .or_insert(*at);
            }
        }
        let before = self.endpoints();
        let mut updated = self.clone();
        updated.replace_source(source_id, &peers, None, now)?;
        for (peer, at) in observed {
            updated.add(source_id, peer, ttl, at)?;
        }
        let changed = updated.endpoints() != before;
        *self = updated;
        Ok(changed)
    }

    fn add(
        &mut self,
        source_id: &str,
        endpoint: String,
        ttl: Option<Duration>,
        now: StdInstant,
    ) -> Result<bool, DiscoveryError> {
        if self.local_endpoints.contains(&endpoint) {
            return Ok(false);
        }
        let is_new = !self.records.contains_key(&endpoint);
        if is_new && self.records.len() >= self.max_candidates {
            return Err(configuration_error(
                "coordinator",
                format!("peer candidate limit reached: {}", self.max_candidates),
            ));
        }
        let expires_at = match ttl {
            Some(ttl) => Some(now.checked_add(ttl).ok_or_else(|| {
                configuration_error(source_id, "candidate_ttl exceeds the platform time range")
            })?),
            None => None,
        };
        let previous_order = self.order.clone();
        self.register_source(source_id);
        let source_candidates = self
            .source_candidates
            .entry(source_id.to_string())
            .or_default();
        if !source_candidates.contains(&endpoint) {
            source_candidates.push(endpoint.clone());
        }
        self.records
            .entry(endpoint.clone())
            .or_default()
            .sources
            .insert(source_id.to_string(), CandidateContribution { expires_at });
        self.rebuild_order();
        Ok(self.order != previous_order)
    }

    fn remove(&mut self, source_id: &str, endpoint: &str) -> bool {
        let before = self.endpoints();
        if let Some(candidates) = self.source_candidates.get_mut(source_id) {
            candidates.retain(|candidate| candidate != endpoint);
        }
        if let Some(record) = self.records.get_mut(endpoint) {
            record.sources.remove(source_id);
        }
        self.prune_empty();
        self.prune_source_candidates();
        self.rebuild_order();
        self.endpoints() != before
    }

    fn remove_source_except(&mut self, source_id: &str, retained: &HashSet<String>) {
        for (endpoint, record) in &mut self.records {
            if !retained.contains(endpoint) {
                record.sources.remove(source_id);
            }
        }
        self.prune_empty();
    }

    fn source_unavailable(&mut self, source_id: &str, leased: bool) -> bool {
        if leased {
            return false;
        }
        let before = self.endpoints();
        self.source_candidates.remove(source_id);
        for record in self.records.values_mut() {
            record.sources.remove(source_id);
        }
        self.prune_empty();
        self.prune_source_candidates();
        self.rebuild_order();
        self.endpoints() != before
    }

    fn set_local_endpoints(&mut self, endpoints: Vec<String>) -> bool {
        let before = self.endpoints();
        self.local_endpoints.clear();
        for endpoint in endpoints {
            self.local_endpoints.insert(endpoint);
        }
        self.records
            .retain(|endpoint, _| !self.local_endpoints.contains(endpoint));
        self.prune_source_candidates();
        self.rebuild_order();
        self.endpoints() != before
    }

    fn expire(&mut self, now: StdInstant) -> bool {
        let before = self.endpoints();
        for record in self.records.values_mut() {
            record
                .sources
                .retain(|_, source| source.expires_at.is_none_or(|deadline| deadline > now));
        }
        self.prune_empty();
        self.prune_source_candidates();
        self.rebuild_order();
        self.endpoints() != before
    }

    fn next_expiry(&self) -> Option<StdInstant> {
        self.records
            .values()
            .flat_map(|record| record.sources.values())
            .filter_map(|source| source.expires_at)
            .min()
    }

    fn prune_empty(&mut self) -> bool {
        let before = self.records.len();
        self.records.retain(|_, record| !record.sources.is_empty());
        self.records.len() != before
    }

    fn register_source(&mut self, source_id: &str) {
        if !self.source_order.iter().any(|known| known == source_id) {
            self.source_order.push(source_id.to_string());
        }
    }

    fn prune_source_candidates(&mut self) {
        for (source_id, candidates) in &mut self.source_candidates {
            candidates.retain(|endpoint| {
                self.records
                    .get(endpoint)
                    .is_some_and(|record| record.sources.contains_key(source_id))
            });
        }
        self.source_candidates
            .retain(|_, candidates| !candidates.is_empty());
    }

    fn rebuild_order(&mut self) {
        let mut order = Vec::with_capacity(self.records.len());
        for source_id in &self.source_order {
            let Some(candidates) = self.source_candidates.get(source_id) else {
                continue;
            };
            for endpoint in candidates {
                if self
                    .records
                    .get(endpoint)
                    .is_some_and(|record| record.sources.contains_key(source_id))
                    && !order.contains(endpoint)
                {
                    order.push(endpoint.clone());
                }
            }
        }
        self.order = order;
    }
}

enum CandidateCommand {
    Snapshot {
        source_id: String,
        snapshot: DiscoverySnapshot,
        ttl: Option<Duration>,
    },
    ReplaceSource {
        source_id: String,
        peers: Vec<String>,
        ttl: Option<Duration>,
    },
    Add {
        source_id: String,
        endpoint: String,
        ttl: Option<Duration>,
    },
    Remove {
        source_id: String,
        endpoint: String,
    },
    SourceUnavailable {
        source_id: String,
        leased: bool,
    },
    SetLocalEndpoints {
        endpoints: Vec<String>,
        reply: oneshot::Sender<()>,
    },
}

pub(super) struct DiscoveryCoordinator {
    config: DiscoveryRuntimeConfig,
    providers: Vec<DiscoveryProvider>,
    candidates_rx: watch::Receiver<Arc<Vec<String>>>,
    command_tx: mpsc::Sender<CandidateCommand>,
    shutdown_tx: watch::Sender<bool>,
    coordinator_task: Option<JoinHandle<()>>,
    provider_tasks: Vec<JoinHandle<()>>,
}

impl Drop for DiscoveryCoordinator {
    fn drop(&mut self) {
        self.request_shutdown();
        for task in &self.provider_tasks {
            task.abort();
        }
        if let Some(task) = &self.coordinator_task {
            task.abort();
        }
    }
}

impl DiscoveryCoordinator {
    pub(super) async fn start(
        providers: Vec<DiscoveryProvider>,
        config: DiscoveryRuntimeConfig,
    ) -> Result<Self, DiscoveryError> {
        validate_discovery_config(&providers, &config)?;

        let mut registry = CandidateRegistry::new(config.max_candidates())?;
        let mut initial_watches = Vec::with_capacity(providers.len());
        for source in &providers {
            let provider_watch =
                match tokio::time::timeout(DISCOVERY_OPERATION_TIMEOUT, source.provider().watch())
                    .await
                {
                    Ok(Ok(provider_watch)) => provider_watch,
                    Ok(Err(error)) => {
                        rollback_providers(&providers).await;
                        return Err(error);
                    }
                    Err(_) => {
                        rollback_providers(&providers).await;
                        return Err(provider_timeout(source.source_id(), "watch"));
                    }
                };
            if let Err(error) = registry.replace_snapshot(
                source.source_id(),
                provider_watch.snapshot(),
                source.candidate_ttl(),
                StdInstant::now(),
            ) {
                rollback_providers(&providers).await;
                return Err(error);
            }
            initial_watches.push(provider_watch);
        }

        let (candidates_tx, candidates_rx) = watch::channel(registry.endpoints());
        let command_capacity = config.max_candidates().clamp(1, 4096);
        let (command_tx, command_rx) = mpsc::channel(command_capacity);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let coordinator_task = Some(tokio::spawn(run_candidate_registry(
            registry,
            candidates_tx,
            command_rx,
            shutdown_rx,
        )));

        let provider_tasks = providers
            .iter()
            .cloned()
            .zip(initial_watches)
            .map(|(source, provider_watch)| {
                tokio::spawn(run_provider_watch(
                    source,
                    provider_watch,
                    command_tx.clone(),
                    shutdown_tx.subscribe(),
                ))
            })
            .collect();

        Ok(Self {
            config,
            providers,
            candidates_rx,
            command_tx,
            shutdown_tx,
            coordinator_task,
            provider_tasks,
        })
    }

    pub(super) fn candidates(&self) -> watch::Receiver<Arc<Vec<String>>> {
        self.candidates_rx.clone()
    }

    pub(super) fn request_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
        for source in &self.providers {
            source.provider().request_shutdown();
        }
    }

    pub(super) async fn configure_local_endpoint(
        &self,
        bound_addr: SocketAddr,
    ) -> Result<Option<String>, DiscoveryError> {
        let advertised =
            resolve_advertised_endpoint(bound_addr, self.config.advertised_endpoint())?;
        let mut local_endpoints = Vec::with_capacity(2);
        if !bound_addr.ip().is_unspecified() {
            local_endpoints.push(bound_addr.to_string());
        }
        if let Some(endpoint) = &advertised
            && !local_endpoints.contains(endpoint)
        {
            local_endpoints.push(endpoint.clone());
        }
        let (reply, response) = oneshot::channel();
        self.command_tx
            .send(CandidateCommand::SetLocalEndpoints {
                endpoints: local_endpoints,
                reply,
            })
            .await
            .map_err(|_| DiscoveryError::WatchClosed)?;
        response.await.map_err(|_| DiscoveryError::WatchClosed)?;
        Ok(advertised)
    }

    pub(super) async fn announce(
        &self,
        advertised_endpoint: Option<&str>,
    ) -> Result<(), DiscoveryError> {
        for source in &self.providers {
            match source.provider().announcement_support() {
                AnnouncementSupport::Unsupported => continue,
                AnnouncementSupport::Optional if advertised_endpoint.is_none() => continue,
                AnnouncementSupport::Required if advertised_endpoint.is_none() => {
                    return Err(configuration_error(
                        source.source_id(),
                        "a wildcard listener requires an explicit advertised endpoint",
                    ));
                }
                AnnouncementSupport::Optional | AnnouncementSupport::Required => {}
            }
            let Some(endpoint) = advertised_endpoint else {
                continue;
            };
            tokio::time::timeout(
                DISCOVERY_OPERATION_TIMEOUT,
                source.provider().announce(&PeerAnnouncement {
                    endpoint: endpoint.to_string(),
                }),
            )
            .await
            .map_err(|_| provider_timeout(source.source_id(), "announcement"))??;
        }
        Ok(())
    }

    pub(super) async fn shutdown(&mut self) -> Result<(), DiscoveryError> {
        self.request_shutdown();
        for task in self.provider_tasks.drain(..) {
            let _ = AbortOnDropTask::new(task).join().await;
        }
        if let Some(task) = self.coordinator_task.take() {
            let _ = AbortOnDropTask::new(task).join().await;
        }

        shutdown_providers(&self.providers).await
    }
}

async fn run_candidate_registry(
    mut registry: CandidateRegistry,
    candidates_tx: watch::Sender<Arc<Vec<String>>>,
    mut command_rx: mpsc::Receiver<CandidateCommand>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        let next_expiry = registry.next_expiry();
        tokio::select! {
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
            command = command_rx.recv() => {
                let Some(command) = command else {
                    break;
                };
                let command = match command {
                    CandidateCommand::SetLocalEndpoints { endpoints, reply } => {
                        let changed = registry.set_local_endpoints(endpoints);
                        if changed {
                            candidates_tx.send_replace(registry.endpoints());
                        }
                        let _ = reply.send(());
                        continue;
                    }
                    command => command,
                };
                let changed = apply_candidate_command(&mut registry, command);
                if changed {
                    candidates_tx.send_replace(registry.endpoints());
                }
            }
            _ = wait_for_expiry(next_expiry) => {
                if registry.expire(StdInstant::now()) {
                    candidates_tx.send_replace(registry.endpoints());
                }
            }
        }
    }
    debug!("peer candidate coordinator terminated");
}

fn apply_candidate_command(registry: &mut CandidateRegistry, command: CandidateCommand) -> bool {
    let now = StdInstant::now();
    match command {
        CandidateCommand::Snapshot {
            source_id,
            snapshot,
            ttl,
        } => match registry.replace_snapshot(&source_id, &snapshot, ttl, now) {
            Ok(changed) => changed,
            Err(error) => {
                warn!(source = %source_id, %error, "rejected observed discovery snapshot");
                false
            }
        },
        CandidateCommand::ReplaceSource {
            source_id,
            peers,
            ttl,
        } => match registry.replace_source(&source_id, &peers, ttl, now) {
            Ok(changed) => changed,
            Err(error) => {
                warn!(source = %source_id, error = %error, "rejected discovery snapshot");
                false
            }
        },
        CandidateCommand::Add {
            source_id,
            endpoint,
            ttl,
        } => match canonicalize_endpoint(&endpoint)
            .and_then(|endpoint| registry.add(&source_id, endpoint, ttl, now))
        {
            Ok(changed) => changed,
            Err(error) => {
                warn!(source = %source_id, error = %error, "rejected discovery candidate");
                false
            }
        },
        CandidateCommand::Remove {
            source_id,
            endpoint,
        } => match canonicalize_endpoint(&endpoint) {
            Ok(endpoint) => registry.remove(&source_id, &endpoint),
            Err(error) => {
                warn!(source = %source_id, error = %error, "rejected discovery candidate removal");
                false
            }
        },
        CandidateCommand::SourceUnavailable { source_id, leased } => {
            registry.source_unavailable(&source_id, leased)
        }
        CandidateCommand::SetLocalEndpoints { .. } => false,
    }
}

async fn run_provider_watch(
    source: DiscoveryProvider,
    mut provider_watch: DiscoveryWatch,
    command_tx: mpsc::Sender<CandidateCommand>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let mut retry_delay = DISCOVERY_RETRY_INITIAL_DELAY;
    loop {
        let result = tokio::select! {
            _ = shutdown_rx.changed() => break,
            result = provider_watch.recv() => result,
        };

        match result {
            Ok(event) => {
                retry_delay = DISCOVERY_RETRY_INITIAL_DELAY;
                let command = match event.change {
                    DiscoveryChange::Observed(snapshot) => CandidateCommand::Snapshot {
                        source_id: source.source_id().to_string(),
                        snapshot,
                        ttl: source.candidate_ttl(),
                    },
                    DiscoveryChange::Added(endpoint) => CandidateCommand::Add {
                        source_id: source.source_id().to_string(),
                        endpoint,
                        ttl: source.candidate_ttl(),
                    },
                    DiscoveryChange::Removed(endpoint) => CandidateCommand::Remove {
                        source_id: source.source_id().to_string(),
                        endpoint,
                    },
                    DiscoveryChange::Replaced(peers) => CandidateCommand::ReplaceSource {
                        source_id: source.source_id().to_string(),
                        peers,
                        ttl: source.candidate_ttl(),
                    },
                };
                if !send_command(&command_tx, command, &mut shutdown_rx).await {
                    break;
                }
            }
            Err(error) => {
                let leased = source.candidate_ttl().is_some();
                if !send_command(
                    &command_tx,
                    CandidateCommand::SourceUnavailable {
                        source_id: source.source_id().to_string(),
                        leased,
                    },
                    &mut shutdown_rx,
                )
                .await
                {
                    break;
                }
                if !discovery_error_is_retryable(&error) {
                    warn!(source = %source.source_id(), error = %error, "discovery watch stopped");
                    break;
                }
                debug!(source = %source.source_id(), error = %error, "resubscribing discovery watch");
                if !wait_for_retry(retry_delay, &mut shutdown_rx).await {
                    break;
                }
                retry_delay = retry_delay.saturating_mul(2).min(DISCOVERY_RETRY_MAX_DELAY);
                let watch_result = tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    result = tokio::time::timeout(
                        DISCOVERY_OPERATION_TIMEOUT,
                        source.provider().watch(),
                    ) => result,
                };
                match watch_result {
                    Ok(Ok(new_watch)) => {
                        let snapshot = new_watch.snapshot().clone();
                        if !send_command(
                            &command_tx,
                            CandidateCommand::Snapshot {
                                source_id: source.source_id().to_string(),
                                snapshot,
                                ttl: source.candidate_ttl(),
                            },
                            &mut shutdown_rx,
                        )
                        .await
                        {
                            break;
                        }
                        provider_watch = new_watch;
                        retry_delay = DISCOVERY_RETRY_INITIAL_DELAY;
                    }
                    Ok(Err(error)) if !discovery_error_is_retryable(&error) => {
                        warn!(source = %source.source_id(), error = %error, "discovery provider failed permanently");
                        break;
                    }
                    Ok(Err(error)) => {
                        debug!(source = %source.source_id(), error = %error, "discovery resubscribe failed");
                    }
                    Err(_) => {
                        debug!(source = %source.source_id(), "discovery resubscribe timed out");
                    }
                }
            }
        }
    }
    debug!(source = %source.source_id(), "discovery watch task terminated");
}

async fn send_command(
    command_tx: &mpsc::Sender<CandidateCommand>,
    command: CandidateCommand,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        _ = shutdown_rx.changed() => false,
        result = command_tx.send(command) => result.is_ok(),
    }
}

async fn wait_for_retry(delay: Duration, shutdown_rx: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        _ = shutdown_rx.changed() => false,
        _ = tokio::time::sleep(delay) => true,
    }
}

async fn wait_for_expiry(deadline: Option<StdInstant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(TokioInstant::from_std(deadline)).await,
        None => pending::<()>().await,
    }
}

fn discovery_error_is_retryable(error: &DiscoveryError) -> bool {
    match error {
        DiscoveryError::Provider { retryable, .. } => *retryable,
        DiscoveryError::WatchOverflow { .. }
        | DiscoveryError::WatchRevision { .. }
        | DiscoveryError::WatchInvalidated
        | DiscoveryError::WatchClosed => true,
        DiscoveryError::InvalidConfiguration { .. } | DiscoveryError::Unsupported { .. } => false,
    }
}

fn validate_discovery_config(
    providers: &[DiscoveryProvider],
    config: &DiscoveryRuntimeConfig,
) -> Result<(), DiscoveryError> {
    validate_identifier("cluster_id", config.cluster_id())?;
    if let Some(endpoint) = config.advertised_endpoint() {
        let (host, port) = parse_host_port(endpoint, true)?;
        canonicalize_host_port(&host, port.max(1))?;
    }
    let mut source_ids = HashSet::new();
    for source in providers {
        validate_identifier("source_id", source.source_id())?;
        if !source_ids.insert(source.source_id()) {
            return Err(configuration_error(
                "coordinator",
                format!("duplicate discovery source: {}", source.source_id()),
            ));
        }
        if source.provider().cluster_id() != config.cluster_id() {
            return Err(configuration_error(
                source.source_id(),
                format!(
                    "provider cluster '{}' does not match local cluster '{}'",
                    source.provider().cluster_id(),
                    config.cluster_id()
                ),
            ));
        }
        if source.candidate_ttl() == Some(Duration::ZERO) {
            return Err(configuration_error(
                source.source_id(),
                "candidate_ttl must be greater than zero",
            ));
        }
    }
    Ok(())
}

fn validate_identifier(name: &str, value: &str) -> Result<(), DiscoveryError> {
    let valid = !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if valid {
        Ok(())
    } else {
        Err(configuration_error(
            "coordinator",
            format!("{name} must be 1..=128 ASCII letters, digits, '.', '_' or '-'"),
        ))
    }
}

fn resolve_advertised_endpoint(
    bound_addr: SocketAddr,
    configured: Option<&str>,
) -> Result<Option<String>, DiscoveryError> {
    match configured {
        Some(configured) => {
            let (host, port) = parse_host_port(configured, true)?;
            let port = if port == 0 { bound_addr.port() } else { port };
            canonicalize_host_port(&host, port).map(Some)
        }
        None if bound_addr.ip().is_unspecified() => Ok(None),
        None => Ok(Some(bound_addr.to_string())),
    }
}

pub(crate) fn canonicalize_endpoint(endpoint: &str) -> Result<String, DiscoveryError> {
    let (host, port) = parse_host_port(endpoint, false)?;
    canonicalize_host_port(&host, port)
}

fn parse_host_port(endpoint: &str, allow_zero_port: bool) -> Result<(String, u16), DiscoveryError> {
    if endpoint.trim() != endpoint || endpoint.is_empty() {
        return Err(configuration_error(
            "coordinator",
            format!("invalid peer endpoint: {endpoint:?}"),
        ));
    }
    if let Ok(socket) = endpoint.parse::<SocketAddr>() {
        if !allow_zero_port && socket.port() == 0 {
            return Err(configuration_error(
                "coordinator",
                "peer endpoint port must be greater than zero",
            ));
        }
        return Ok((socket.ip().to_string(), socket.port()));
    }
    let Some((host, port)) = endpoint.rsplit_once(':') else {
        return Err(configuration_error(
            "coordinator",
            format!("peer endpoint must include a port: {endpoint}"),
        ));
    };
    let host = host.strip_suffix('.').unwrap_or(host);
    if !valid_dns_name(host) {
        return Err(configuration_error(
            "coordinator",
            format!("invalid peer endpoint host: {host:?}"),
        ));
    }
    let port = port.parse::<u16>().map_err(|_| {
        configuration_error(
            "coordinator",
            format!("invalid peer endpoint port: {port:?}"),
        )
    })?;
    if !allow_zero_port && port == 0 {
        return Err(configuration_error(
            "coordinator",
            "peer endpoint port must be greater than zero",
        ));
    }
    Ok((host.to_ascii_lowercase(), port))
}

fn valid_dns_name(host: &str) -> bool {
    !host.is_empty()
        && host.len() <= 253
        && !host.contains(':')
        && host.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                && label
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_alphanumeric)
                && label
                    .as_bytes()
                    .last()
                    .is_some_and(u8::is_ascii_alphanumeric)
        })
}

fn canonicalize_host_port(host: &str, port: u16) -> Result<String, DiscoveryError> {
    if port == 0 {
        return Err(configuration_error(
            "coordinator",
            "advertised endpoint resolved to port zero",
        ));
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        let undialable = ip.is_unspecified()
            || ip.is_multicast()
            || matches!(ip, IpAddr::V4(address) if address.is_broadcast())
            || matches!(ip, IpAddr::V6(address) if address.is_unicast_link_local());
        if undialable {
            return Err(configuration_error(
                "coordinator",
                "peer endpoint must use a dialable unicast IP address",
            ));
        }
        return Ok(SocketAddr::new(ip, port).to_string());
    }
    Ok(format!("{}:{port}", host.to_ascii_lowercase()))
}

fn configuration_error(provider: &str, message: impl Into<String>) -> DiscoveryError {
    DiscoveryError::InvalidConfiguration {
        provider: provider.to_string(),
        message: message.into(),
    }
}

fn provider_timeout(provider: &str, operation: &str) -> DiscoveryError {
    DiscoveryError::Provider {
        provider: provider.to_string(),
        message: format!("{operation} timed out"),
        retryable: true,
    }
}

async fn shutdown_providers(providers: &[DiscoveryProvider]) -> Result<(), DiscoveryError> {
    let mut tasks = tokio::task::JoinSet::new();
    for source in providers {
        source.provider().request_shutdown();
        let source_id = source.source_id().to_string();
        let provider = Arc::clone(source.provider());
        tasks.spawn(async move {
            tokio::time::timeout(DISCOVERY_OPERATION_TIMEOUT, provider.shutdown())
                .await
                .map_err(|_| provider_timeout(&source_id, "shutdown"))?
        });
    }

    let mut first_error = None;
    while let Some(result) = tasks.join_next().await {
        let result = match result {
            Ok(result) => result,
            Err(error) => Err(DiscoveryError::Provider {
                provider: "coordinator".to_string(),
                message: format!("shutdown task failed: {error}"),
                retryable: false,
            }),
        };
        if let Err(error) = result
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

async fn rollback_providers(providers: &[DiscoveryProvider]) {
    if let Err(error) = shutdown_providers(providers).await {
        warn!(error = %error, "discovery provider rollback failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    #[tokio::test]
    async fn identical_observation_renews_lease_but_cached_snapshot_really_expires() {
        let mut registry = CandidateRegistry::new(2).unwrap();
        let now = StdInstant::now();
        let ttl = Duration::from_millis(30);
        let old = DiscoverySnapshot::observed(1, vec![("peer:1".into(), now - ttl / 2)]);
        let fresh = DiscoverySnapshot::observed(2, vec![("peer:1".into(), now)]);
        registry
            .replace_snapshot("file", &old, Some(ttl), now)
            .unwrap();
        assert_eq!(registry.next_expiry(), Some(now + ttl / 2));
        assert!(
            !registry
                .replace_snapshot("file", &fresh, Some(ttl), now)
                .unwrap()
        );
        assert_eq!(registry.next_expiry(), Some(now + ttl));
        assert!(
            !registry
                .replace_snapshot("file", &fresh, Some(ttl), now + ttl / 2)
                .unwrap()
        );
        assert_eq!(registry.next_expiry(), Some(now + ttl));
        let (candidates_tx, mut candidates_rx) = watch::channel(registry.endpoints());
        let (command_tx, command_rx) = mpsc::channel(2);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(run_candidate_registry(
            registry,
            candidates_tx,
            command_rx,
            shutdown_rx,
        ));
        tokio::time::timeout(Duration::from_secs(2), candidates_rx.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(candidates_rx.borrow_and_update().is_empty());
        // Even after actual expiry, replay/resubscription cannot resurrect it.
        command_tx
            .send(CandidateCommand::Snapshot {
                source_id: "file".into(),
                snapshot: fresh,
                ttl: Some(ttl),
            })
            .await
            .unwrap();
        let (reply, response) = oneshot::channel();
        command_tx
            .send(CandidateCommand::SetLocalEndpoints {
                endpoints: Vec::new(),
                reply,
            })
            .await
            .unwrap();
        response.await.unwrap();
        assert!(!candidates_rx.has_changed().unwrap());
        assert!(candidates_rx.borrow().is_empty());
        shutdown_tx.send_replace(true);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn successful_identical_file_reads_keep_candidates_alive_then_invalid_file_expires() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("peers");
        tokio::fs::write(&path, "peer:1\n").await.unwrap();
        let mut config = crate::FileWatchDiscoveryConfig::new(&path);
        config.poll_interval = Duration::from_millis(10);
        let discovery = Arc::new(crate::FileWatchDiscovery::new(config).unwrap());
        let ttl = Duration::from_millis(150);
        let provider = DiscoveryProvider::new("file", discovery.clone()).with_candidate_ttl(ttl);
        let mut coordinator =
            DiscoveryCoordinator::start(vec![provider], DiscoveryRuntimeConfig::new())
                .await
                .unwrap();
        let mut candidates = coordinator.candidates();
        let mut observations = crate::PeerDiscovery::watch(discovery.as_ref())
            .await
            .unwrap();
        let until = StdInstant::now() + ttl * 2;
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let event = observations.recv().await.unwrap();
                let DiscoveryChange::Observed(snapshot) = event.change else {
                    panic!("missing observation");
                };
                if snapshot.observations().unwrap()[0] >= until {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(&**candidates.borrow(), &["peer:1"]);
        assert!(!candidates.has_changed().unwrap());
        tokio::fs::write(&path, "invalid-endpoint\n").await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), candidates.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(candidates.borrow().is_empty());
        coordinator.shutdown().await.unwrap();
    }

    struct MutableDiscovery {
        state: StdMutex<(u64, Vec<String>)>,
        events: tokio::sync::broadcast::Sender<crate::DiscoveryEvent>,
        announced: StdMutex<Vec<String>>,
        stopped: std::sync::atomic::AtomicBool,
        fail_watch: bool,
        cluster: &'static str,
    }

    impl MutableDiscovery {
        fn new(peers: Vec<String>) -> Self {
            let (events, _) = tokio::sync::broadcast::channel(8);
            Self {
                state: StdMutex::new((0, peers)),
                events,
                announced: StdMutex::new(Vec::new()),
                stopped: std::sync::atomic::AtomicBool::new(false),
                fail_watch: false,
                cluster: crate::DEFAULT_DISCOVERY_CLUSTER,
            }
        }

        fn failing() -> Self {
            Self {
                fail_watch: true,
                ..Self::new(Vec::new())
            }
        }

        fn add(&self, endpoint: &str) {
            let mut state = self.state.lock().unwrap();
            state.0 += 1;
            state.1.push(endpoint.to_string());
            let _ = self.events.send(crate::DiscoveryEvent {
                revision: state.0,
                change: DiscoveryChange::Added(endpoint.to_string()),
            });
        }

        fn remove(&self, endpoint: &str) {
            let mut state = self.state.lock().unwrap();
            state.0 += 1;
            state.1.retain(|candidate| candidate != endpoint);
            let _ = self.events.send(crate::DiscoveryEvent {
                revision: state.0,
                change: DiscoveryChange::Removed(endpoint.to_string()),
            });
        }
    }

    #[async_trait::async_trait]
    impl crate::PeerDiscovery for MutableDiscovery {
        fn cluster_id(&self) -> &str {
            self.cluster
        }

        fn announcement_support(&self) -> AnnouncementSupport {
            AnnouncementSupport::Required
        }

        async fn discover(&self) -> Result<crate::DiscoverySnapshot, DiscoveryError> {
            let state = self.state.lock().unwrap();
            Ok(crate::DiscoverySnapshot::new(state.0, state.1.clone()))
        }

        async fn announce(&self, announcement: &PeerAnnouncement) -> Result<(), DiscoveryError> {
            self.announced
                .lock()
                .unwrap()
                .push(announcement.endpoint.clone());
            Ok(())
        }

        async fn watch(&self) -> Result<DiscoveryWatch, DiscoveryError> {
            if self.fail_watch {
                return Err(DiscoveryError::Provider {
                    provider: "failing".to_string(),
                    message: "watch failed".to_string(),
                    retryable: false,
                });
            }
            let state = self.state.lock().unwrap();
            let events = self.events.subscribe();
            Ok(DiscoveryWatch::new(
                crate::DiscoverySnapshot::new(state.0, state.1.clone()),
                events,
            ))
        }

        async fn shutdown(&self) -> Result<(), DiscoveryError> {
            self.stopped
                .store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn registry_deduplicates_sources_and_removes_only_the_last_contribution() {
        let mut registry = CandidateRegistry::new(4).unwrap();
        let now = StdInstant::now();
        registry
            .add("one", "peer.example:9000".to_string(), None, now)
            .unwrap();
        registry
            .add("two", "peer.example:9000".to_string(), None, now)
            .unwrap();

        assert!(!registry.remove("one", "peer.example:9000"));
        assert_eq!(&*registry.endpoints(), &["peer.example:9000"]);
        assert!(registry.remove("two", "peer.example:9000"));
        assert!(registry.endpoints().is_empty());
    }

    #[test]
    fn registry_expiry_preserves_other_sources() {
        let mut registry = CandidateRegistry::new(4).unwrap();
        let now = StdInstant::now();
        registry
            .add(
                "leased",
                "peer.example:9000".to_string(),
                Some(Duration::from_millis(5)),
                now,
            )
            .unwrap();
        registry
            .add("static", "peer.example:9000".to_string(), None, now)
            .unwrap();

        assert!(!registry.expire(now + Duration::from_millis(10)));
        assert_eq!(&*registry.endpoints(), &["peer.example:9000"]);
        assert!(registry.source_unavailable("static", false));
        assert!(registry.endpoints().is_empty());
    }

    #[test]
    fn registry_enforces_global_candidate_limit_atomically() {
        let mut registry = CandidateRegistry::new(1).unwrap();
        registry
            .replace_source(
                "static",
                &["one.example:9000".to_string()],
                None,
                StdInstant::now(),
            )
            .unwrap();

        assert!(
            registry
                .replace_source(
                    "static",
                    &[
                        "one.example:9000".to_string(),
                        "two.example:9000".to_string()
                    ],
                    None,
                    StdInstant::now(),
                )
                .is_err()
        );
        assert_eq!(&*registry.endpoints(), &["one.example:9000"]);
    }

    #[test]
    fn registry_applies_pure_source_reordering_atomically() {
        let mut registry = CandidateRegistry::new(4).unwrap();
        let now = StdInstant::now();
        registry
            .replace_source(
                "dynamic",
                &["one.example:9000".into(), "two.example:9000".into()],
                None,
                now,
            )
            .unwrap();

        assert!(
            registry
                .replace_source(
                    "dynamic",
                    &["two.example:9000".into(), "one.example:9000".into()],
                    None,
                    now,
                )
                .unwrap()
        );
        assert_eq!(
            &*registry.endpoints(),
            &["two.example:9000", "one.example:9000"]
        );
    }

    #[test]
    fn removing_a_priority_contribution_publishes_the_new_source_order() {
        let mut registry = CandidateRegistry::new(4).unwrap();
        let now = StdInstant::now();
        registry
            .replace_source("first", &["shared.example:9000".into()], None, now)
            .unwrap();
        registry
            .replace_source(
                "second",
                &["other.example:9000".into(), "shared.example:9000".into()],
                None,
                now,
            )
            .unwrap();
        assert_eq!(
            &*registry.endpoints(),
            &["shared.example:9000", "other.example:9000"]
        );

        assert!(registry.remove("first", "shared.example:9000"));
        assert_eq!(
            &*registry.endpoints(),
            &["other.example:9000", "shared.example:9000"]
        );
    }

    #[test]
    fn expiry_prunes_historical_source_candidates() {
        let mut registry = CandidateRegistry::new(2).unwrap();
        let now = StdInstant::now();
        registry
            .add(
                "leased",
                "old.example:9000".into(),
                Some(Duration::from_millis(1)),
                now,
            )
            .unwrap();

        assert!(registry.expire(now + Duration::from_millis(2)));
        assert!(registry.endpoints().is_empty());
        assert!(!registry.source_candidates.contains_key("leased"));

        registry
            .add(
                "leased",
                "new.example:9000".into(),
                Some(Duration::from_millis(1)),
                now,
            )
            .unwrap();
        assert_eq!(&*registry.endpoints(), &["new.example:9000"]);
    }

    #[test]
    fn registry_skips_invalid_snapshot_entries_without_losing_valid_candidates() {
        let mut registry = CandidateRegistry::new(4).unwrap();

        registry
            .replace_source(
                "static",
                &[
                    "not-an-endpoint".to_string(),
                    "Peer.Example:9000".to_string(),
                    "0.0.0.0:9001".to_string(),
                    "other.example:9002".to_string(),
                ],
                None,
                StdInstant::now(),
            )
            .unwrap();

        assert_eq!(
            &*registry.endpoints(),
            &["peer.example:9000", "other.example:9002"]
        );
    }

    #[test]
    fn local_endpoint_is_removed_and_rejected_on_refresh() {
        let mut registry = CandidateRegistry::new(4).unwrap();
        let now = StdInstant::now();
        registry
            .add("static", "127.0.0.1:9000".to_string(), None, now)
            .unwrap();

        assert!(registry.set_local_endpoints(vec!["127.0.0.1:9000".to_string()]));
        assert!(
            !registry
                .add("static", "127.0.0.1:9000".to_string(), None, now)
                .unwrap()
        );
        assert!(registry.endpoints().is_empty());
    }

    #[test]
    fn endpoint_validation_supports_dns_and_ipv6_but_rejects_undialable_values() {
        assert_eq!(
            canonicalize_endpoint("Peer.Example:9000").unwrap(),
            "peer.example:9000"
        );
        assert_eq!(canonicalize_endpoint("[::1]:9000").unwrap(), "[::1]:9000");
        assert!(canonicalize_endpoint("0.0.0.0:9000").is_err());
        assert!(canonicalize_endpoint("224.0.0.1:9000").is_err());
        assert!(canonicalize_endpoint("255.255.255.255:9000").is_err());
        assert!(canonicalize_endpoint("[ff02::1]:9000").is_err());
        assert!(canonicalize_endpoint("[fe80::1]:9000").is_err());
        assert!(canonicalize_endpoint("peer.example:0").is_err());
        assert!(canonicalize_endpoint(" peer.example:9000").is_err());
        assert!(canonicalize_endpoint("_service.example:9000").is_err());
        assert!(canonicalize_endpoint("-peer.example:9000").is_err());
        assert_eq!(
            canonicalize_endpoint("Peer.Example.:9000").unwrap(),
            "peer.example:9000"
        );
    }

    #[test]
    fn registry_rejects_a_ttl_that_cannot_be_represented() {
        let mut registry = CandidateRegistry::new(1).unwrap();
        assert!(
            registry
                .add(
                    "leased",
                    "peer.example:9000".to_string(),
                    Some(Duration::MAX),
                    StdInstant::now(),
                )
                .is_err()
        );
        assert!(registry.endpoints().is_empty());
        assert!(registry.source_order.is_empty());
        assert!(registry.source_candidates.is_empty());
    }

    #[test]
    fn advertised_endpoint_uses_bound_port_and_requires_host_for_wildcard() {
        let bound = "0.0.0.0:43123".parse().unwrap();
        assert_eq!(resolve_advertised_endpoint(bound, None).unwrap(), None);
        assert_eq!(
            resolve_advertised_endpoint(bound, Some("node.example:0")).unwrap(),
            Some("node.example:43123".to_string())
        );
        assert!(resolve_advertised_endpoint(bound, Some("0.0.0.0:9000")).is_err());
    }

    #[tokio::test]
    async fn coordinator_updates_an_initially_empty_snapshot_and_owns_lifecycle() {
        let discovery = Arc::new(MutableDiscovery::new(Vec::new()));
        let provider = DiscoveryProvider::new("dynamic", discovery.clone());
        let config = DiscoveryRuntimeConfig::new()
            .with_advertised_endpoint("node.example:0")
            .with_max_candidates(4);
        let mut coordinator = DiscoveryCoordinator::start(vec![provider], config)
            .await
            .unwrap();
        let mut candidates = coordinator.candidates();
        assert!(candidates.borrow().is_empty());

        discovery.add("Peer.Example:9000");
        tokio::time::timeout(Duration::from_secs(1), candidates.changed())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&**candidates.borrow_and_update(), &["peer.example:9000"]);

        discovery.remove("peer.example:9000");
        tokio::time::timeout(Duration::from_secs(1), candidates.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(candidates.borrow_and_update().is_empty());

        let advertised = coordinator
            .configure_local_endpoint("0.0.0.0:43123".parse().unwrap())
            .await
            .unwrap();
        coordinator.announce(advertised.as_deref()).await.unwrap();
        coordinator.shutdown().await.unwrap();

        assert_eq!(
            discovery.announced.lock().unwrap().as_slice(),
            ["node.example:43123"]
        );
        assert!(discovery.stopped.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn coordinator_rolls_back_providers_after_partial_watch_startup() {
        let first = Arc::new(MutableDiscovery::new(Vec::new()));
        let failing = Arc::new(MutableDiscovery::failing());
        let providers = vec![
            DiscoveryProvider::new("first", first.clone()),
            DiscoveryProvider::new("failing", failing.clone()),
        ];

        assert!(
            DiscoveryCoordinator::start(providers, DiscoveryRuntimeConfig::default())
                .await
                .is_err()
        );
        assert!(first.stopped.load(std::sync::atomic::Ordering::SeqCst));
        assert!(failing.stopped.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn coordinator_rejects_a_provider_from_another_cluster() {
        let discovery = Arc::new(MutableDiscovery {
            cluster: "other-cluster",
            ..MutableDiscovery::new(Vec::new())
        });
        let result = DiscoveryCoordinator::start(
            vec![DiscoveryProvider::new("foreign", discovery)],
            DiscoveryRuntimeConfig::default(),
        )
        .await;

        assert!(matches!(
            result,
            Err(DiscoveryError::InvalidConfiguration { provider, .. })
                if provider == "foreign"
        ));
    }
}
