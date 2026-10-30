use std::collections::{HashMap, HashSet};
use std::future::pending;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant as StdInstant};

use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant as TokioInstant;
use tracing::{debug, warn};

use crate::control::{ControlError, PeerCandidate, PeerRegistration};
use crate::discovery::{
    AbortOnDropTask, AnnouncementSupport, DiscoveryChange, DiscoveryError, DiscoveryProvider,
    DiscoveryRuntimeConfig, DiscoverySnapshot, DiscoveryWatch, PeerAnnouncement,
};

#[derive(Clone, Default)]
pub(super) struct CandidateAccess(Arc<Mutex<AccessState>>);

#[derive(Default)]
struct AccessState {
    metrics: Arc<crate::observability::RuntimeMetrics>,
    generation: u64,
    terminal: bool,
    sender: Option<mpsc::Sender<CandidateCommand>>,
    snapshot: Vec<PeerCandidate>,
}

impl CandidateAccess {
    pub(super) fn with_metrics(metrics: Arc<crate::observability::RuntimeMetrics>) -> Self {
        Self(Arc::new(Mutex::new(AccessState {
            metrics,
            ..Default::default()
        })))
    }

    pub(super) async fn add(&self, address: String) -> Result<PeerRegistration, ControlError> {
        if address.len() > 259 {
            return Err(ControlError::InvalidPeer);
        }
        let endpoint = canonicalize_endpoint(&address).map_err(|_| ControlError::InvalidPeer)?;
        if endpoint.len() > 259 {
            return Err(ControlError::InvalidPeer);
        }
        let (reply, response) = oneshot::channel();
        {
            let state = self
                .0
                .lock()
                .map_err(|_| ControlError::Internal("candidate access lock poisoned".into()))?;
            if state.terminal {
                return Err(ControlError::ServiceUnavailable);
            }
            let sender = state
                .sender
                .as_ref()
                .ok_or(ControlError::ServiceUnavailable)?;
            sender
                .try_send(CandidateCommand::Manual {
                    endpoint,
                    reply: RegistrationReply {
                        sender: Some(reply),
                        started: false,
                    },
                })
                .map_err(|_| ControlError::ServiceUnavailable)?;
            state.metrics.peers.set(
                crate::peer_metrics::Gauge::CandidateItems,
                (sender.max_capacity() - sender.capacity()) as u64,
            );
        }
        response.await.map_err(|_| {
            ControlError::PeerAcceptanceUncertain(
                "candidate coordinator closed before confirmation".into(),
            )
        })?
    }

    pub(super) fn snapshot(&self) -> Result<Vec<PeerCandidate>, ControlError> {
        let state = self
            .0
            .lock()
            .map_err(|_| ControlError::Internal("candidate access lock poisoned".into()))?;
        if state.terminal || state.sender.is_none() {
            return Err(ControlError::ServiceUnavailable);
        }
        Ok(state.snapshot.clone())
    }

    pub(super) fn check_startable(&self) -> Result<(), ControlError> {
        let state = self
            .0
            .lock()
            .map_err(|_| ControlError::Internal("candidate access lock poisoned".into()))?;
        if state.terminal || state.sender.is_some() {
            return Err(ControlError::ServiceUnavailable);
        }
        Ok(())
    }

    fn begin_generation(&self) -> Result<CandidateGeneration, DiscoveryError> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| configuration_error("coordinator", "candidate access lock poisoned"))?;
        if state.terminal || state.sender.is_some() {
            return Err(configuration_error(
                "coordinator",
                "candidate access unavailable",
            ));
        }
        state.generation = state
            .generation
            .checked_add(1)
            .ok_or_else(|| configuration_error("coordinator", "candidate generation exhausted"))?;
        state.snapshot.clear();
        Ok(CandidateGeneration {
            access: self.clone(),
            id: state.generation,
        })
    }

    pub(super) fn stop(&self) {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        state.terminal = true;
        state.sender = None;
    }
}

#[derive(Clone)]
struct CandidateGeneration {
    access: CandidateAccess,
    id: u64,
}

impl CandidateGeneration {
    fn publish(&self, registry: &CandidateRegistry) {
        let mut state = self
            .access
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.generation != self.id || state.terminal {
            return;
        }
        state.snapshot = registry
            .order
            .iter()
            .filter_map(|address| {
                let record = registry.records.get(address)?;
                let mut sources: Vec<_> = record.sources.keys().cloned().collect();
                sources.sort();
                Some(PeerCandidate {
                    address: address.clone(),
                    persisted: record.sources.contains_key("manual"),
                    sources,
                })
            })
            .collect();
        if let Some(sender) = &state.sender {
            state.metrics.peers.set(
                crate::peer_metrics::Gauge::CandidateItems,
                (sender.max_capacity() - sender.capacity()) as u64,
            );
        }
    }

    fn deactivate(&self) {
        let mut state = self
            .access
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.generation != self.id {
            return;
        }
        state.sender = None;
        state.snapshot.clear();
        state
            .metrics
            .peers
            .set(crate::peer_metrics::Gauge::CandidateItems, 0);
    }

    fn running(&self) -> bool {
        self.access.0.lock().is_ok_and(|state| {
            state.generation == self.id && !state.terminal && state.sender.is_some()
        })
    }
}

const DISCOVERY_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(500);
const DISCOVERY_RETRY_MAX_DELAY: Duration = Duration::from_secs(30);
const DISCOVERY_OPERATION_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy)]
#[cfg_attr(test, derive(PartialEq, Eq))]
struct CandidateContribution {
    expires_at: Option<StdInstant>,
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(test, derive(PartialEq, Eq))]
struct CandidateRecord {
    sources: HashMap<String, CandidateContribution>,
}

#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq, Eq))]
struct CandidateRegistry {
    max_candidates: usize,
    order: Vec<String>,
    source_order: Vec<String>,
    source_candidates: HashMap<String, Vec<String>>,
    records: HashMap<String, CandidateRecord>,
    local_endpoints: HashSet<String>,
    #[cfg(test)]
    order_rebuilds: usize,
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
            #[cfg(test)]
            order_rebuilds: 0,
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
        // Empty, invalid-only and local-only snapshots do not acquire a lease.
        let expires_at = match ttl {
            Some(ttl)
                if canonical
                    .iter()
                    .any(|peer| !self.local_endpoints.contains(peer)) =>
            {
                Some(now.checked_add(ttl).ok_or_else(|| {
                    configuration_error(source_id, "candidate_ttl exceeds the platform time range")
                })?)
            }
            _ => None,
        };
        self.replace_contributions(
            source_id,
            canonical
                .into_iter()
                .map(|endpoint| (endpoint, CandidateContribution { expires_at }))
                .collect(),
        )
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
        let mut positions = HashMap::<String, usize>::new();
        let mut contributions: Vec<(String, CandidateContribution)> = Vec::new();
        for (peer, at) in snapshot.peers().iter().zip(observations) {
            let expires_at = if let Some(ttl) = ttl {
                let deadline = at.checked_add(ttl).ok_or_else(|| {
                    configuration_error(source_id, "candidate_ttl exceeds the platform time range")
                })?;
                if deadline <= now {
                    continue;
                }
                Some(deadline)
            } else {
                None
            };
            if let Ok(peer) = canonicalize_endpoint(peer) {
                if let Some(&position) = positions.get(&peer) {
                    // Keep the first live occurrence's position and the newest lease.
                    let contribution = &mut contributions[position].1;
                    contribution.expires_at = contribution.expires_at.max(expires_at);
                } else {
                    positions.insert(peer.clone(), contributions.len());
                    contributions.push((peer, CandidateContribution { expires_at }));
                }
            }
        }
        self.replace_contributions(source_id, contributions)
    }

    fn replace_contributions(
        &mut self,
        source_id: &str,
        contributions: Vec<(String, CandidateContribution)>,
    ) -> Result<bool, DiscoveryError> {
        // Prepare the entire replacement off-registry: a global capacity error
        // must not publish removals, reordered sources or partially renewed leases.
        let mut updated = self.clone();
        updated.register_source(source_id);
        let retained = contributions
            .iter()
            .map(|(endpoint, _)| endpoint.clone())
            .collect::<HashSet<_>>();
        updated.remove_source_except(source_id, &retained);
        let mut candidates = Vec::with_capacity(contributions.len());
        for (endpoint, contribution) in contributions {
            candidates.push(endpoint.clone());
            if updated.local_endpoints.contains(&endpoint) {
                continue;
            }
            if !updated.records.contains_key(&endpoint)
                && updated.records.len() >= updated.max_candidates
            {
                return Err(configuration_error(
                    "coordinator",
                    format!("peer candidate limit reached: {}", updated.max_candidates),
                ));
            }
            updated
                .records
                .entry(endpoint)
                .or_default()
                .sources
                .insert(source_id.to_string(), contribution);
        }
        updated
            .source_candidates
            .insert(source_id.to_string(), candidates);
        updated.rebuild_order();
        let changed = updated.order != self.order;
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
        #[cfg(test)]
        {
            self.order_rebuilds += 1;
        }
        let mut order = Vec::with_capacity(self.records.len());
        let mut seen = HashSet::with_capacity(self.records.len());
        for source_id in &self.source_order {
            let Some(candidates) = self.source_candidates.get(source_id) else {
                continue;
            };
            for endpoint in candidates {
                if self
                    .records
                    .get(endpoint)
                    .is_some_and(|record| record.sources.contains_key(source_id))
                    && seen.insert(endpoint)
                {
                    order.push(endpoint.clone());
                }
            }
        }
        self.order = order;
    }
}

enum CandidateCommand {
    Manual {
        endpoint: String,
        reply: RegistrationReply,
    },
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

struct RegistrationReply {
    sender: Option<oneshot::Sender<Result<PeerRegistration, ControlError>>>,
    started: bool,
}

impl RegistrationReply {
    fn send(mut self, result: Result<PeerRegistration, ControlError>) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(result);
        }
    }
}

impl Drop for RegistrationReply {
    fn drop(&mut self) {
        let error = if self.started {
            ControlError::PeerAcceptanceUncertain(
                "candidate coordinator stopped before persistence confirmation".into(),
            )
        } else {
            ControlError::ServiceUnavailable
        };
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(Err(error));
        }
    }
}

pub(super) struct DiscoveryCoordinator {
    generation: CandidateGeneration,
    config: DiscoveryRuntimeConfig,
    providers: Vec<DiscoveryProvider>,
    candidates_rx: watch::Receiver<Arc<Vec<String>>>,
    command_tx: mpsc::Sender<CandidateCommand>,
    shutdown_tx: watch::Sender<bool>,
    coordinator_task: Option<JoinHandle<()>>,
    provider_tasks: Vec<JoinHandle<()>>,
    prepared: Option<PreparedCandidates>,
    finished: bool,
}

struct PreparedCandidates {
    watches: Vec<DiscoveryWatch>,
    manual: Vec<String>,
    store: Option<Arc<nx_store::Store>>,
    commands: mpsc::Receiver<CandidateCommand>,
    candidates: watch::Sender<Arc<Vec<String>>>,
}

impl Drop for DiscoveryCoordinator {
    fn drop(&mut self) {
        if !self.finished {
            self.request_rollback();
        }
        for task in &self.provider_tasks {
            task.abort();
        }
        if let Some(task) = &self.coordinator_task {
            task.abort();
        }
    }
}

impl DiscoveryCoordinator {
    #[cfg(test)]
    pub(super) async fn start(
        providers: Vec<DiscoveryProvider>,
        config: DiscoveryRuntimeConfig,
    ) -> Result<Self, DiscoveryError> {
        Self::start_inner(providers, config, None, CandidateAccess::default()).await
    }

    #[cfg(test)]
    pub(super) async fn start_managed(
        providers: Vec<DiscoveryProvider>,
        config: DiscoveryRuntimeConfig,
        store: Arc<nx_store::Store>,
        access: CandidateAccess,
    ) -> Result<Self, DiscoveryError> {
        let mut coordinator = Self::prepare_managed(providers, config, store, access).await?;
        if let Err(error) = coordinator.initialize(Vec::new()) {
            let _ = coordinator.rollback().await;
            return Err(error);
        }
        Ok(coordinator)
    }

    pub(super) async fn prepare_managed(
        providers: Vec<DiscoveryProvider>,
        config: DiscoveryRuntimeConfig,
        store: Arc<nx_store::Store>,
        access: CandidateAccess,
    ) -> Result<Self, DiscoveryError> {
        Self::prepare(providers, config, Some(store), access).await
    }

    #[cfg(test)]
    async fn start_inner(
        providers: Vec<DiscoveryProvider>,
        config: DiscoveryRuntimeConfig,
        store: Option<Arc<nx_store::Store>>,
        access: CandidateAccess,
    ) -> Result<Self, DiscoveryError> {
        let mut coordinator = Self::prepare(providers, config, store, access).await?;
        if let Err(error) = coordinator.initialize(Vec::new()) {
            let _ = coordinator.rollback().await;
            return Err(error);
        }
        Ok(coordinator)
    }

    async fn prepare(
        providers: Vec<DiscoveryProvider>,
        config: DiscoveryRuntimeConfig,
        store: Option<Arc<nx_store::Store>>,
        access: CandidateAccess,
    ) -> Result<Self, DiscoveryError> {
        validate_discovery_config(&providers, &config)?;
        let generation = access.begin_generation()?;

        let manual = if let Some(store) = &store {
            let store = Arc::clone(store);
            let cluster = config.cluster_id().to_owned();
            let capacity = config.max_candidates();
            tokio::task::spawn_blocking(move || super::manual::load(&store, &cluster, capacity))
                .await
                .map_err(|error| configuration_error("manual", error.to_string()))?
                .map_err(|error| configuration_error("manual", error.to_string()))?
        } else {
            Vec::new()
        };
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
            if provider_watch.snapshot().peers().len() > config.max_candidates() {
                rollback_providers(&providers).await;
                return Err(configuration_error(
                    source.source_id(),
                    "discovery snapshot exceeds candidate limit",
                ));
            }
            initial_watches.push(provider_watch);
        }

        let (candidates_tx, candidates_rx) = watch::channel(Arc::new(Vec::new()));
        let command_capacity = config.max_candidates().clamp(1, 4096);
        let (command_tx, command_rx) = mpsc::channel(command_capacity);
        let (shutdown_tx, _) = watch::channel(false);

        Ok(Self {
            generation,
            config,
            providers,
            candidates_rx,
            command_tx,
            shutdown_tx,
            coordinator_task: None,
            provider_tasks: Vec::new(),
            finished: false,
            prepared: Some(PreparedCandidates {
                watches: initial_watches,
                manual,
                store,
                commands: command_rx,
                candidates: candidates_tx,
            }),
        })
    }

    fn initialize(&mut self, local_endpoints: Vec<String>) -> Result<(), DiscoveryError> {
        let Some(prepared) = self.prepared.take() else {
            return Err(configuration_error("coordinator", "already initialized"));
        };
        let mut registry = CandidateRegistry::new(self.config.max_candidates())?;
        registry.set_local_endpoints(local_endpoints);
        for (source, provider_watch) in self.providers.iter().zip(&prepared.watches) {
            registry.replace_snapshot(
                source.source_id(),
                provider_watch.snapshot(),
                source.candidate_ttl(),
                StdInstant::now(),
            )?;
        }
        registry.replace_source("manual", &prepared.manual, None, StdInstant::now())?;
        self.generation.publish(&registry);
        prepared.candidates.send_replace(registry.endpoints());
        self.candidates_rx.borrow_and_update();
        self.coordinator_task = Some(tokio::spawn(run_candidate_registry(
            registry,
            prepared.manual.into_iter().collect(),
            prepared.candidates,
            prepared.commands,
            self.shutdown_tx.subscribe(),
            prepared
                .store
                .map(|store| (store, self.config.cluster_id().to_owned())),
            self.generation.clone(),
        )));
        self.provider_tasks = self
            .providers
            .iter()
            .cloned()
            .zip(prepared.watches)
            .map(|(source, watch)| {
                tokio::spawn(run_provider_watch(
                    source,
                    watch,
                    self.command_tx.clone(),
                    self.shutdown_tx.subscribe(),
                ))
            })
            .collect();
        Ok(())
    }

    pub(super) fn candidates(&self) -> watch::Receiver<Arc<Vec<String>>> {
        self.candidates_rx.clone()
    }

    pub(super) fn activate(&self) -> Result<(), ControlError> {
        let mut state = self
            .generation
            .access
            .0
            .lock()
            .map_err(|_| ControlError::Internal("candidate access lock poisoned".into()))?;
        if state.terminal
            || state.generation != self.generation.id
            || *self.shutdown_tx.borrow()
            || self.coordinator_task.is_none()
            || self.command_tx.is_closed()
        {
            return Err(ControlError::ServiceUnavailable);
        }
        state.sender = Some(self.command_tx.clone());
        state.metrics.peers.set(
            crate::peer_metrics::Gauge::CandidateCapacity,
            self.command_tx.max_capacity() as u64,
        );
        Ok(())
    }

    pub(super) fn request_shutdown(&self) {
        self.generation.access.stop();
        self.shutdown_tx.send_replace(true);
        for source in &self.providers {
            source.provider().request_shutdown();
        }
    }

    fn request_rollback(&self) {
        self.generation.deactivate();
        self.shutdown_tx.send_replace(true);
        for source in &self.providers {
            source.provider().request_rollback();
        }
    }

    pub(super) async fn configure_local_endpoint(
        &mut self,
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
        if self.prepared.is_some() {
            self.initialize(local_endpoints)?;
            return Ok(advertised);
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
        self.finish(true).await
    }

    pub(super) async fn rollback(&mut self) -> Result<(), DiscoveryError> {
        self.request_rollback();
        self.finish(false).await
    }

    async fn finish(&mut self, terminal: bool) -> Result<(), DiscoveryError> {
        self.finished = true;
        let tasks: Vec<_> = self
            .provider_tasks
            .drain(..)
            .chain(self.coordinator_task.take())
            .map(AbortOnDropTask::new)
            .collect();
        let joined = tokio::time::timeout(DISCOVERY_OPERATION_TIMEOUT, async move {
            for task in tasks {
                task.join().await.map_err(|error| {
                    configuration_error("coordinator", format!("candidate task failed: {error}"))
                })?;
            }
            Ok(())
        })
        .await
        .map_err(|_| provider_timeout("coordinator", "candidate shutdown"))
        .and_then(|result| result);
        let providers = finish_providers(&self.providers, terminal).await;
        joined.and(providers)
    }
}

async fn run_candidate_registry(
    mut registry: CandidateRegistry,
    mut persisted_manual: HashSet<String>,
    candidates_tx: watch::Sender<Arc<Vec<String>>>,
    mut command_rx: mpsc::Receiver<CandidateCommand>,
    mut shutdown_rx: watch::Receiver<bool>,
    manual_store: Option<(Arc<nx_store::Store>, String)>,
    generation: CandidateGeneration,
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
                    CandidateCommand::Manual { endpoint, mut reply } => {
                        let before = registry.endpoints();
                        let running = generation.running();
                        let result = if !running {
                            Err(ControlError::ServiceUnavailable)
                        } else {
                            reply.started = true;
                            accept_manual(
                                &mut registry,
                                &mut persisted_manual,
                                &endpoint,
                                manual_store.as_ref(),
                            ).await
                        };
                        if let Err(error) = &result {
                            warn!(%error, "manual peer registration failed");
                        }
                        generation.publish(&registry);
                        if registry.endpoints() != before {
                            candidates_tx.send_replace(registry.endpoints());
                        }
                        reply.send(result);
                        continue;
                    }
                    CandidateCommand::SetLocalEndpoints { endpoints, reply } => {
                        let changed = registry.set_local_endpoints(endpoints);
                        if changed {
                            candidates_tx.send_replace(registry.endpoints());
                        }
                        generation.publish(&registry);
                        let _ = reply.send(());
                        continue;
                    }
                    command => command,
                };
                let changed = apply_candidate_command(&mut registry, command);
                generation.publish(&registry);
                if changed {
                    candidates_tx.send_replace(registry.endpoints());
                }
            }
            _ = wait_for_expiry(next_expiry) => {
                if registry.expire(StdInstant::now()) {
                    candidates_tx.send_replace(registry.endpoints());
                }
                generation.publish(&registry);
            }
        }
    }
    generation.deactivate();
    command_rx.close();
    while let Ok(command) = command_rx.try_recv() {
        if let CandidateCommand::Manual { reply, .. } = command {
            reply.send(Err(ControlError::ServiceUnavailable));
        }
    }
    debug!("peer candidate coordinator terminated");
}

async fn accept_manual(
    registry: &mut CandidateRegistry,
    persisted_manual: &mut HashSet<String>,
    endpoint: &str,
    storage: Option<&(Arc<nx_store::Store>, String)>,
) -> Result<PeerRegistration, ControlError> {
    if registry.local_endpoints.contains(endpoint) {
        return Err(ControlError::InvalidPeer);
    }
    if registry
        .records
        .get(endpoint)
        .is_some_and(|record| record.sources.contains_key("manual"))
    {
        return Ok(PeerRegistration {
            address: endpoint.to_owned(),
            created: false,
        });
    }
    if (!registry.records.contains_key(endpoint)
        && registry.records.len() >= registry.max_candidates)
        || (!persisted_manual.contains(endpoint)
            && persisted_manual.len() >= registry.max_candidates)
    {
        return Err(ControlError::PeerCapacityExceeded);
    }
    let mut prepared = registry.clone();
    prepared
        .add("manual", endpoint.to_owned(), None, StdInstant::now())
        .map_err(|error| ControlError::Internal(error.to_string()))?;
    let (store, cluster) = storage.ok_or(ControlError::ServiceUnavailable)?;
    let store = Arc::clone(store);
    let cluster = cluster.clone();
    let address = endpoint.to_owned();
    tokio::task::spawn_blocking(move || super::manual::persist(&store, &cluster, &address))
        .await
        .map_err(|error| ControlError::PeerAcceptanceUncertain(error.to_string()))??;
    persisted_manual.insert(endpoint.to_owned());
    *registry = prepared;
    Ok(PeerRegistration {
        address: endpoint.to_owned(),
        created: true,
    })
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
        CandidateCommand::SetLocalEndpoints { .. } | CandidateCommand::Manual { .. } => false,
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
        if source.source_id() == "manual" {
            return Err(configuration_error(
                "manual",
                "reserved discovery source identifier",
            ));
        }
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

async fn finish_providers(
    providers: &[DiscoveryProvider],
    terminal: bool,
) -> Result<(), DiscoveryError> {
    let mut tasks = tokio::task::JoinSet::new();
    for source in providers {
        if terminal {
            source.provider().request_shutdown();
        } else {
            source.provider().request_rollback();
        }
        let source_id = source.source_id().to_string();
        let provider = Arc::clone(source.provider());
        tasks.spawn(async move {
            tokio::time::timeout(DISCOVERY_OPERATION_TIMEOUT, async {
                if terminal {
                    provider.shutdown().await
                } else {
                    provider.rollback().await
                }
            })
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
    if let Err(error) = finish_providers(providers, false).await {
        warn!(error = %error, "discovery provider rollback failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    #[test]
    fn bulk_replacement_preserves_canonical_first_occurrence_and_source_priority() {
        let mut registry = CandidateRegistry::new(8).unwrap();
        let now = StdInstant::now();
        registry.set_local_endpoints(vec!["local:1".into()]);
        registry
            .replace_source("first", &["old:1".into(), "shared:1".into()], None, now)
            .unwrap();
        registry
            .replace_source("second", &["other:1".into(), "shared:1".into()], None, now)
            .unwrap();

        assert!(
            registry
                .replace_source(
                    "first",
                    &[
                        "B.:1".into(),
                        "Shared:1".into(),
                        "b:1".into(),
                        "A:1".into(),
                        "shared.:1".into(),
                        "Local.:1".into(),
                        "invalid".into(),
                    ],
                    None,
                    now,
                )
                .unwrap()
        );
        assert_eq!(
            &*registry.endpoints(),
            &["b:1", "shared:1", "a:1", "other:1"]
        );
        assert_eq!(
            registry.source_candidates["first"],
            ["b:1", "shared:1", "a:1", "local:1"]
        );
        assert_eq!(registry.records["shared:1"].sources.len(), 2);
        assert!(!registry.records.contains_key("old:1"));
        assert!(!registry.records.contains_key("local:1"));

        assert!(registry.replace_source("first", &[], None, now).unwrap());
        assert_eq!(&*registry.endpoints(), &["other:1", "shared:1"]);
        assert_eq!(registry.records["shared:1"].sources.len(), 1);
        registry
            .replace_source("first", &["shared:1".into()], None, now)
            .unwrap();
        assert_eq!(&*registry.endpoints(), &["shared:1", "other:1"]);
        assert_eq!(registry.source_order, ["first", "second"]);
    }

    #[test]
    fn bulk_limits_count_raw_duplicates_invalid_local_and_expired_entries() {
        let mut registry = CandidateRegistry::new(2).unwrap();
        let now = StdInstant::now();
        let ttl = Duration::from_secs(10);
        registry.set_local_endpoints(vec!["local:1".into()]);
        registry
            .replace_source("existing", &["old:1".into()], Some(ttl), now)
            .unwrap();
        let before = registry.clone();
        for peers in [
            vec!["Peer:1".into(), "peer.:1".into(), "peer:1".into()],
            vec!["invalid".into(), "local:1".into(), "expired:1".into()],
        ] {
            for source in ["existing", "new"] {
                assert!(
                    registry
                        .replace_source(source, &peers, Some(ttl), now)
                        .is_err()
                );
                assert_eq!(registry, before);
                let snapshot = DiscoverySnapshot::observed(
                    1,
                    peers
                        .iter()
                        .cloned()
                        .map(|peer| (peer, now - ttl))
                        .collect(),
                );
                assert!(
                    registry
                        .replace_snapshot(source, &snapshot, Some(ttl), now)
                        .is_err()
                );
                assert_eq!(registry, before);
            }
        }
    }

    #[test]
    fn bulk_global_limit_rolls_back_removal_reordering_and_lease_renewal() {
        let mut registry = CandidateRegistry::new(3).unwrap();
        let now = StdInstant::now();
        let ttl = Duration::from_secs(10);
        registry
            .replace_source(
                "first",
                &["old:1".into(), "shared:1".into()],
                Some(ttl),
                now,
            )
            .unwrap();
        registry
            .replace_source("second", &["other:1".into(), "shared:1".into()], None, now)
            .unwrap();
        let before = registry.clone();
        let peers = vec!["shared:1".into(), "new:1".into(), "overflow:1".into()];
        let later = now + ttl / 2;
        let snapshot = DiscoverySnapshot::observed(
            1,
            peers.iter().cloned().map(|peer| (peer, later)).collect(),
        );
        for source in ["first", "new-source"] {
            assert!(
                registry
                    .replace_source(source, &peers, Some(ttl), later)
                    .is_err()
            );
            assert_eq!(registry, before);
            assert!(
                registry
                    .replace_snapshot(source, &snapshot, Some(ttl), later)
                    .is_err()
            );
            assert_eq!(registry, before);
        }
        assert!(
            registry
                .add("new-source", "overflow:1".into(), None, now)
                .is_err()
        );
        assert_eq!(registry, before);

        // Replacing an exclusive contribution releases its slot before admission.
        assert!(
            registry
                .replace_source("first", &peers[..2], Some(ttl), later)
                .unwrap()
        );
        assert_eq!(&*registry.endpoints(), &["shared:1", "new:1", "other:1"]);
        assert_eq!(registry.records["shared:1"].sources.len(), 2);
        assert_eq!(registry.next_expiry(), Some(later + ttl));
    }

    #[test]
    fn bulk_ttl_overflow_leaves_all_registry_state_unchanged() {
        let mut registry = CandidateRegistry::new(3).unwrap();
        let now = StdInstant::now();
        registry
            .replace_source(
                "first",
                &["old:1".into()],
                Some(Duration::from_secs(10)),
                now,
            )
            .unwrap();
        registry.set_local_endpoints(vec!["local:1".into()]);
        let before = registry.clone();
        for source in ["first", "new-source"] {
            assert!(
                registry
                    .replace_source(source, &["new:1".into()], Some(Duration::MAX), now)
                    .is_err()
            );
            assert_eq!(registry, before);
            assert!(
                registry
                    .add(source, "old:1".into(), Some(Duration::MAX), now)
                    .is_err()
            );
            assert_eq!(registry, before);
            // Observations validate time arithmetic even for invalid/local entries.
            for peer in ["new:1", "invalid", "local:1"] {
                let snapshot = DiscoverySnapshot::observed(1, vec![(peer.into(), now)]);
                assert!(
                    registry
                        .replace_snapshot(source, &snapshot, Some(Duration::MAX), now)
                        .is_err()
                );
                assert_eq!(registry, before);
            }
        }
        // Plain snapshots, unlike observations, never lease filtered entries.
        assert!(
            registry
                .replace_source(
                    "first",
                    &["invalid".into(), "local:1".into()],
                    Some(Duration::MAX),
                    now
                )
                .unwrap()
        );
        assert!(registry.records.is_empty());
        assert!(
            !registry
                .replace_source("first", &[], Some(Duration::MAX), now)
                .unwrap()
        );
    }

    #[test]
    fn bulk_observations_keep_first_live_order_and_newest_per_endpoint_lease() {
        let mut registry = CandidateRegistry::new(8).unwrap();
        let now = StdInstant::now();
        let ttl = Duration::from_secs(10);
        registry.set_local_endpoints(vec!["local:1".into()]);
        registry
            .replace_source("observed", &["old:1".into()], None, now)
            .unwrap();
        registry
            .replace_source("static", &["persistent:1".into(), "a:1".into()], None, now)
            .unwrap();
        let snapshot = DiscoverySnapshot::observed(
            1,
            vec![
                ("B:1".into(), now - ttl),
                ("A.:1".into(), now - Duration::from_secs(3)),
                ("b:1".into(), now - Duration::from_secs(2)),
                ("a:1".into(), now - Duration::from_secs(1)),
                ("A:1".into(), now - Duration::from_secs(2)),
                ("local:1".into(), now),
                ("invalid".into(), now),
                ("expired:1".into(), now - ttl),
            ],
        );
        assert!(
            registry
                .replace_snapshot("observed", &snapshot, Some(ttl), now)
                .unwrap()
        );
        assert_eq!(&*registry.endpoints(), &["a:1", "b:1", "persistent:1"]);
        let first_deadline = now + ttl - Duration::from_secs(2);
        let last_deadline = now + ttl - Duration::from_secs(1);
        assert_eq!(registry.next_expiry(), Some(first_deadline));
        assert_eq!(
            registry.records["b:1"].sources["observed"].expires_at,
            Some(first_deadline)
        );
        assert_eq!(
            registry.records["a:1"].sources["observed"].expires_at,
            Some(last_deadline)
        );
        assert!(
            !registry
                .replace_snapshot("observed", &snapshot, Some(ttl), now + ttl / 2)
                .unwrap()
        );
        assert_eq!(registry.next_expiry(), Some(first_deadline));
        assert!(!registry.source_unavailable("observed", true));
        assert!(!registry.expire(first_deadline - Duration::from_nanos(1)));
        assert!(registry.expire(first_deadline));
        assert_eq!(&*registry.endpoints(), &["a:1", "persistent:1"]);
        assert_eq!(registry.next_expiry(), Some(last_deadline));
        assert!(registry.expire(last_deadline));
        assert_eq!(&*registry.endpoints(), &["persistent:1", "a:1"]);
        assert_eq!(registry.next_expiry(), None);
        assert!(
            !registry
                .replace_snapshot("observed", &snapshot, Some(ttl), now + ttl)
                .unwrap()
        );
        assert_eq!(&*registry.endpoints(), &["persistent:1", "a:1"]);
    }

    #[test]
    fn bulk_unleased_observations_and_plain_refresh_preserve_lease_semantics() {
        let mut registry = CandidateRegistry::new(3).unwrap();
        let now = StdInstant::now();
        let ttl = Duration::from_secs(10);
        let peers = vec!["b:1".into(), "a:1".into()];
        registry
            .replace_source("first", &peers, Some(ttl), now)
            .unwrap();
        assert!(
            !registry
                .replace_source("first", &peers, Some(ttl), now + ttl / 2)
                .unwrap()
        );
        assert_eq!(registry.next_expiry(), Some(now + ttl * 3 / 2));
        let snapshot = DiscoverySnapshot::observed(
            1,
            vec![
                ("B.:1".into(), now - ttl * 2),
                ("a:1".into(), now - ttl),
                ("b:1".into(), now),
            ],
        );
        assert!(
            !registry
                .replace_snapshot("first", &snapshot, None, now)
                .unwrap()
        );
        assert_eq!(&*registry.endpoints(), &["b:1", "a:1"]);
        assert_eq!(registry.next_expiry(), None);
        assert!(!registry.expire(now + ttl * 10));
        assert!(registry.source_unavailable("first", false));
        assert!(registry.records.is_empty());
    }

    #[test]
    fn bulk_4097_candidates_rebuild_order_once_per_replacement() {
        let count = 4097;
        let mut registry = CandidateRegistry::new(count).unwrap();
        let now = StdInstant::now();
        let ttl = Duration::from_secs(10);
        let peers = (0..count)
            .map(|index| format!("peer-{index}.invalid:9000"))
            .collect::<Vec<_>>();
        assert!(
            registry
                .replace_source("first", &peers, Some(ttl), now)
                .unwrap()
        );
        assert_eq!(registry.order_rebuilds, 1);
        assert_eq!(&*registry.endpoints(), &peers);
        assert!(
            !registry
                .replace_source("second", &peers, None, now)
                .unwrap()
        );
        assert_eq!(registry.order_rebuilds, 2);

        let reversed = peers.iter().rev().cloned().collect::<Vec<_>>();
        let snapshot = DiscoverySnapshot::observed(
            1,
            reversed.iter().cloned().map(|peer| (peer, now)).collect(),
        );
        assert!(
            registry
                .replace_snapshot("first", &snapshot, Some(ttl), now)
                .unwrap()
        );
        assert_eq!(registry.order_rebuilds, 3);
        assert_eq!(&*registry.endpoints(), &reversed);
        assert!(
            registry
                .records
                .values()
                .all(|record| record.sources.len() == 2)
        );
        assert!(
            !registry
                .replace_snapshot("first", &snapshot, Some(ttl), now + ttl / 2)
                .unwrap()
        );
        assert_eq!(registry.order_rebuilds, 4);
        assert_eq!(registry.next_expiry(), Some(now + ttl));
        assert!(registry.expire(now + ttl));
        assert_eq!(&*registry.endpoints(), &peers);
        assert!(
            registry
                .records
                .values()
                .all(|record| record.sources.len() == 1)
        );
    }

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
            HashSet::new(),
            candidates_tx,
            command_rx,
            shutdown_rx,
            None,
            CandidateAccess::default().begin_generation().unwrap(),
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

    #[tokio::test]
    async fn coordinator_recovers_a_real_file_provider_after_worker_panic() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("peers");
        tokio::fs::write(&path, "before:9000\n").await.unwrap();
        let mut config = crate::FileWatchDiscoveryConfig::new(&path);
        config.poll_interval = Duration::from_millis(10);
        let discovery = Arc::new(crate::FileWatchDiscovery::new(config).unwrap());
        let source = DiscoveryProvider::new("file", discovery.clone());
        let mut coordinator =
            DiscoveryCoordinator::start(vec![source], DiscoveryRuntimeConfig::new())
                .await
                .unwrap();
        let mut candidates = coordinator.candidates();
        assert_eq!(&**candidates.borrow_and_update(), &["before:9000"]);
        discovery.panic_on_next_observation();
        tokio::time::timeout(Duration::from_secs(3), async {
            candidates.changed().await.unwrap();
            assert!(candidates.borrow_and_update().is_empty());
            // Only the coordinator may restart the provider. Do not mask a
            // dead subscription by calling discover/watch from this test.
            tokio::fs::write(&path, "after:9000\n").await.unwrap();
            candidates.changed().await.unwrap();
            assert_eq!(&**candidates.borrow_and_update(), &["after:9000"]);
        })
        .await
        .unwrap();
        coordinator.shutdown().await.unwrap();
        assert!(
            crate::PeerDiscovery::watch(discovery.as_ref())
                .await
                .is_err()
        );
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn manual_shutdown_budget_does_not_wait_for_a_stalled_flush() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(nx_store::Store::open(directory.path()).unwrap());
        let access = CandidateAccess::default();
        let mut coordinator = DiscoveryCoordinator::start_managed(
            vec![],
            DiscoveryRuntimeConfig::default(),
            Arc::clone(&store),
            access.clone(),
        )
        .await
        .unwrap();
        coordinator.activate().unwrap();
        let (reached, checkpoint) = std::sync::mpsc::channel();
        let (resume, release) = std::sync::mpsc::channel();
        store.pause_next_flush(reached, release);
        let request = tokio::spawn({
            let access = access.clone();
            async move { access.add("127.0.0.1:1".into()).await }
        });
        tokio::task::spawn_blocking(move || {
            checkpoint.recv_timeout(Duration::from_secs(5)).unwrap()
        })
        .await
        .unwrap();
        let queued = tokio::spawn({
            let access = access.clone();
            async move { access.add("127.0.0.1:2".into()).await }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            let mut tick = tokio::time::interval(Duration::from_millis(5));
            while coordinator.command_tx.capacity() == coordinator.command_tx.max_capacity() {
                tick.tick().await;
            }
        })
        .await
        .unwrap();
        let stopped = tokio::time::timeout(
            DISCOVERY_OPERATION_TIMEOUT + Duration::from_secs(1),
            coordinator.shutdown(),
        )
        .await
        .unwrap();
        assert!(
            matches!(stopped, Err(DiscoveryError::Provider { message, .. })
            if message.contains("candidate shutdown timed out"))
        );
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), request)
                .await
                .unwrap()
                .unwrap(),
            Err(ControlError::PeerAcceptanceUncertain(_))
        ));
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), queued)
                .await
                .unwrap()
                .unwrap(),
            Err(ControlError::ServiceUnavailable),
        );
        assert_eq!(access.snapshot(), Err(ControlError::ServiceUnavailable));
        resume.send(()).unwrap();
        let persisted = tokio::time::timeout(
            Duration::from_secs(2),
            tokio::task::spawn_blocking(move || super::super::manual::load(&store, "default", 1)),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert_eq!(persisted, ["127.0.0.1:1"]);
        assert!(coordinator.candidates().borrow().is_empty());
    }

    #[tokio::test]
    async fn manual_rollback_does_not_make_access_terminal_or_invalidate_a_new_generation() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(nx_store::Store::open(directory.path()).unwrap());
        let access = CandidateAccess::default();
        let mut old = DiscoveryCoordinator::start_managed(
            vec![],
            DiscoveryRuntimeConfig::default(),
            Arc::clone(&store),
            access.clone(),
        )
        .await
        .unwrap();
        old.activate().unwrap();
        let old_generation = old.generation.clone();
        old.rollback().await.unwrap();
        let mut current = DiscoveryCoordinator::start_managed(
            vec![],
            DiscoveryRuntimeConfig::default(),
            store,
            access.clone(),
        )
        .await
        .unwrap();
        current.activate().unwrap();
        access.add("127.0.0.1:1".into()).await.unwrap();
        old_generation.publish(&CandidateRegistry::new(1).unwrap());
        old_generation.deactivate();
        drop(old);
        assert_eq!(access.snapshot().unwrap()[0].address, "127.0.0.1:1");
        current.shutdown().await.unwrap();
        assert!(access.check_startable().is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn manual_flush_transaction_outlives_request_cancellation_and_shutdown() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(nx_store::Store::open(directory.path()).unwrap());
        let access = CandidateAccess::default();
        let mut coordinator = DiscoveryCoordinator::start_managed(
            vec![],
            DiscoveryRuntimeConfig::default(),
            Arc::clone(&store),
            access.clone(),
        )
        .await
        .unwrap();
        coordinator.activate().unwrap();
        let (reached, checkpoint) = std::sync::mpsc::channel();
        let (resume, release) = std::sync::mpsc::channel();
        store.pause_next_flush(reached, release);
        let request = tokio::spawn({
            let access = access.clone();
            async move { access.add("127.0.0.1:1".into()).await }
        });
        tokio::task::spawn_blocking(move || {
            checkpoint.recv_timeout(Duration::from_secs(5)).unwrap()
        })
        .await
        .unwrap();
        assert!(coordinator.candidates().borrow().is_empty());
        request.abort();
        let _ = request.await;
        coordinator.request_shutdown();
        assert_eq!(
            access.add("127.0.0.1:2".into()).await,
            Err(ControlError::ServiceUnavailable)
        );
        resume.send(()).unwrap();
        coordinator.shutdown().await.unwrap();
        assert_eq!(
            super::super::manual::load(&store, "default", 2).unwrap(),
            ["127.0.0.1:1"]
        );
    }

    #[tokio::test]
    async fn manual_contribution_survives_source_withdrawal_and_watch_is_coherent() {
        use crate::PeerDiscovery;
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(nx_store::Store::open(directory.path()).unwrap());
        let provider = Arc::new(MutableDiscovery::new(vec!["127.0.0.1:1".into()]));
        let mut watch = provider.watch().await.unwrap();
        let access = CandidateAccess::default();
        let mut coordinator = DiscoveryCoordinator::start_managed(
            vec![DiscoveryProvider::new("other", provider.clone())],
            DiscoveryRuntimeConfig::default(),
            store,
            access.clone(),
        )
        .await
        .unwrap();
        coordinator.activate().unwrap();
        assert_eq!(watch.snapshot().peers(), ["127.0.0.1:1"]);
        access.add("127.0.0.1:1".into()).await.unwrap();
        provider.remove("127.0.0.1:1");
        tokio::time::timeout(Duration::from_secs(2), watch.recv())
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut tick = tokio::time::interval(Duration::from_millis(5));
            loop {
                let snapshot = access.snapshot().unwrap();
                if snapshot.len() == 1 && snapshot[0].sources == ["manual"] {
                    break;
                }
                tick.tick().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            coordinator.candidates().borrow().as_ref(),
            &vec!["127.0.0.1:1".to_owned()]
        );
        coordinator.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn manual_capacity_includes_persisted_local_records_and_serializes_insertions() {
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(nx_store::Store::open(directory.path()).unwrap());
        super::super::manual::persist(&store, "default", "127.0.0.1:1").unwrap();
        let access = CandidateAccess::default();
        let mut coordinator = DiscoveryCoordinator::start_managed(
            vec![],
            DiscoveryRuntimeConfig::default().with_max_candidates(2),
            Arc::clone(&store),
            access.clone(),
        )
        .await
        .unwrap();
        coordinator
            .configure_local_endpoint("127.0.0.1:1".parse().unwrap())
            .await
            .unwrap();
        coordinator.activate().unwrap();
        assert!(access.snapshot().unwrap().is_empty());
        let mut changes = coordinator.candidates();
        let (a, b) = tokio::join!(
            access.add("127.0.0.1:2".into()),
            access.add("127.0.0.1:3".into()),
        );
        let accepted = match (a, b) {
            (Ok(accepted), Err(ControlError::PeerCapacityExceeded))
            | (Err(ControlError::PeerCapacityExceeded), Ok(accepted)) => accepted,
            other => panic!("expected one accepted candidate and one capacity error: {other:?}"),
        };
        assert!(accepted.created);
        assert!(changes.has_changed().unwrap());
        assert_eq!(
            changes.borrow_and_update().as_ref(),
            std::slice::from_ref(&accepted.address)
        );
        assert_eq!(access.snapshot().unwrap()[0].address, accepted.address);
        assert_eq!(
            access.add("127.0.0.1:1".into()).await,
            Err(ControlError::InvalidPeer)
        );
        assert_eq!(
            super::super::manual::load(&store, "default", 2)
                .unwrap()
                .len(),
            2
        );
        coordinator.shutdown().await.unwrap();
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
