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
    AnnouncementSupport, DiscoveryChange, DiscoveryError, DiscoveryProvider,
    DiscoveryRuntimeConfig, DiscoveryWatch, PeerAnnouncement,
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
        let mut canonical = Vec::new();
        let mut seen = HashSet::new();
        for peer in peers {
            let endpoint = canonicalize_endpoint(peer)?;
            if seen.insert(endpoint.clone()) {
                canonical.push(endpoint);
            }
        }

        let mut updated = self.clone();
        let retained = canonical.iter().cloned().collect::<HashSet<_>>();
        updated.remove_source_except(source_id, &retained);
        for endpoint in canonical {
            updated.add(source_id, endpoint, ttl, now)?;
        }
        let changed = updated.endpoints() != self.endpoints();
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
        self.records
            .entry(endpoint.clone())
            .or_default()
            .sources
            .insert(source_id.to_string(), CandidateContribution { expires_at });
        if is_new {
            self.order.push(endpoint);
        }
        Ok(is_new)
    }

    fn remove(&mut self, source_id: &str, endpoint: &str) -> bool {
        let Some(record) = self.records.get_mut(endpoint) else {
            return false;
        };
        record.sources.remove(source_id);
        self.prune_empty()
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
        for record in self.records.values_mut() {
            record.sources.remove(source_id);
        }
        self.prune_empty()
    }

    fn set_local_endpoints(&mut self, endpoints: Vec<String>) -> bool {
        self.local_endpoints.clear();
        for endpoint in endpoints {
            self.local_endpoints.insert(endpoint);
        }
        let before = self.records.len();
        self.records
            .retain(|endpoint, _| !self.local_endpoints.contains(endpoint));
        self.prune_order();
        self.records.len() != before
    }

    fn expire(&mut self, now: StdInstant) -> bool {
        for record in self.records.values_mut() {
            record
                .sources
                .retain(|_, source| source.expires_at.is_none_or(|deadline| deadline > now));
        }
        self.prune_empty()
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
        self.prune_order();
        self.records.len() != before
    }

    fn prune_order(&mut self) {
        self.order
            .retain(|endpoint| self.records.contains_key(endpoint));
    }
}

enum CandidateCommand {
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
        let _ = self.shutdown_tx.send(true);
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
            if let Err(error) = registry.replace_source(
                source.source_id(),
                provider_watch.snapshot().peers(),
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
        let _ = self.shutdown_tx.send(true);
        for task in self.provider_tasks.drain(..) {
            let _ = task.await;
        }
        if let Some(task) = self.coordinator_task.take() {
            let _ = task.await;
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
                    DiscoveryChange::Added(endpoint) => CandidateCommand::Add {
                        source_id: source.source_id().to_string(),
                        endpoint,
                        ttl: source.candidate_ttl(),
                    },
                    DiscoveryChange::Removed(endpoint) => CandidateCommand::Remove {
                        source_id: source.source_id().to_string(),
                        endpoint,
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
                        let peers = new_watch.snapshot().peers().to_vec();
                        if !send_command(
                            &command_tx,
                            CandidateCommand::ReplaceSource {
                                source_id: source.source_id().to_string(),
                                peers,
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

fn canonicalize_endpoint(endpoint: &str) -> Result<String, DiscoveryError> {
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
        if ip.is_unspecified() {
            return Err(configuration_error(
                "coordinator",
                "advertised endpoint cannot use an unspecified IP address",
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
