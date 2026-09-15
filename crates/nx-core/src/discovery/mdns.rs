use std::collections::{BTreeSet, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant as StdInstant};

use async_trait::async_trait;
use mdns_sd::{
    DaemonEvent, DaemonStatus, DnsNameChange, RRType, ServiceDaemon, ServiceEvent, ServiceInfo,
};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::dynamic::DynamicState;
use super::{
    AnnouncementSupport, DEFAULT_DISCOVERY_CLUSTER, DEFAULT_DISCOVERY_EVENT_CAPACITY,
    DEFAULT_MAX_PEER_CANDIDATES, DiscoveryError, DiscoverySnapshot, DiscoveryWatch,
    PeerAnnouncement, PeerDiscovery,
};

const PROVIDER: &str = "mdns";
const SERVICE_BASE: &str = "_numax._tcp.local.";
const DEFAULT_MAX_INSTANCES: usize = 1024;
const SHUTDOWN_BUDGET: Duration = Duration::from_secs(4);

/// LAN mDNS discovery and announcement limits.
#[derive(Debug, Clone)]
pub struct MdnsDiscoveryConfig {
    pub instance_name: String,
    pub cluster_id: String,
    pub max_instances: usize,
    pub max_candidates: usize,
    pub event_capacity: usize,
}

impl MdnsDiscoveryConfig {
    pub fn new(instance_name: impl Into<String>) -> Self {
        Self {
            instance_name: instance_name.into(),
            cluster_id: DEFAULT_DISCOVERY_CLUSTER.to_string(),
            max_instances: DEFAULT_MAX_INSTANCES,
            max_candidates: DEFAULT_MAX_PEER_CANDIDATES,
            event_capacity: DEFAULT_DISCOVERY_EVENT_CAPACITY,
        }
    }
}

struct Lifecycle {
    stopped: bool,
    shutdown: Option<watch::Sender<bool>>,
    task: Option<JoinHandle<()>>,
    daemon: Option<ServiceDaemon>,
    completion: Option<watch::Receiver<Option<Result<(), DiscoveryError>>>>,
}

struct Inner {
    config: MdnsDiscoveryConfig,
    service_type: String,
    state: Arc<DynamicState>,
    own_fullname: Arc<StdMutex<Option<String>>>,
    own_endpoint: Arc<StdMutex<Option<String>>>,
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
        // The browse task owns the bounded withdrawal sequence. Do not abort
        // it when its caller is dropped: it must still consume both ACKs.
    }
}

/// Discovers and advertises Numax endpoints on the local multicast domain.
///
/// mDNS instance names, TXT data, and addresses are routing hints only. They
/// never become peer identity or authorization evidence.
pub struct MdnsDiscovery {
    inner: Arc<Inner>,
}

impl MdnsDiscovery {
    pub fn new(config: MdnsDiscoveryConfig) -> Result<Self, DiscoveryError> {
        validate_config(&config)?;
        Ok(Self {
            inner: Arc::new(Inner {
                service_type: cluster_service_type(&config.cluster_id),
                state: Arc::new(DynamicState::new(config.event_capacity)),
                own_fullname: Arc::new(StdMutex::new(None)),
                own_endpoint: Arc::new(StdMutex::new(None)),
                config,
                lifecycle: StdMutex::new(Lifecycle {
                    stopped: false,
                    shutdown: None,
                    task: None,
                    daemon: None,
                    completion: None,
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
        if let Some(task) = lifecycle.task.as_ref() {
            if !task.is_finished() {
                return Ok(());
            }
            lifecycle.task.take();
            lifecycle.daemon.take();
        }

        let daemon = ServiceDaemon::new()
            .map_err(|error| provider_error(format!("cannot start mDNS daemon: {error}"), false))?;
        let monitor = match daemon.monitor() {
            Ok(monitor) => monitor,
            Err(error) => {
                let _ = daemon.shutdown();
                return Err(provider_error(
                    format!("cannot monitor mDNS daemon: {error}"),
                    true,
                ));
            }
        };
        let events = match daemon.browse(&self.inner.service_type) {
            Ok(events) => events,
            Err(error) => {
                let _ = daemon.shutdown();
                return Err(provider_error(
                    format!("cannot browse mDNS service: {error}"),
                    true,
                ));
            }
        };
        if let Some(endpoint) = self
            .inner
            .own_endpoint
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
        {
            let (service, fullname) =
                build_service(&self.inner.config, &self.inner.service_type, &endpoint)?;
            if let Err(error) = daemon.register(service) {
                let _ = daemon.stop_browse(&self.inner.service_type);
                let _ = daemon.shutdown();
                return Err(provider_error(
                    format!("cannot restore mDNS announcement: {error}"),
                    true,
                ));
            }
            *self
                .inner
                .own_fullname
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = Some(fullname);
        }
        let (shutdown, shutdown_rx) = watch::channel(false);
        let (completion_tx, completion_rx) = watch::channel(None);
        let config = self.inner.config.clone();
        let state = Arc::clone(&self.inner.state);
        let own_fullname = Arc::clone(&self.inner.own_fullname);
        let own_endpoint = Arc::clone(&self.inner.own_endpoint);
        // Construct the guard before spawning: cancellation before the first
        // task poll must still release the external daemon.
        let cleanup = DaemonCleanup {
            daemon: daemon.clone(),
            service_type: self.inner.service_type.clone(),
            own_fullname: Arc::clone(&own_fullname),
            finished: false,
        };
        lifecycle.shutdown = Some(shutdown);
        lifecycle.daemon = Some(daemon);
        lifecycle.completion = Some(completion_rx);
        lifecycle.task = Some(tokio::spawn(async move {
            let result = run_mdns_browse(
                config,
                state,
                own_fullname,
                own_endpoint,
                events,
                monitor,
                cleanup,
                shutdown_rx,
            )
            .await;
            if let Err(error) = &result {
                tracing::warn!(%error, provider = PROVIDER, "mDNS cleanup failed");
            }
            completion_tx.send_replace(Some(result));
        }));
        Ok(())
    }
}

#[async_trait]
impl PeerDiscovery for MdnsDiscovery {
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
        self.ensure_started()?;
        let endpoint = crate::sync_manager::canonicalize_endpoint(&announcement.endpoint)
            .map_err(|error| provider_error(error.to_string(), false))?;
        let (service, fullname) =
            build_service(&self.inner.config, &self.inner.service_type, &endpoint)?;

        let lifecycle = self
            .inner
            .lifecycle
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if lifecycle.stopped {
            return Err(provider_error("provider is shut down", false));
        }
        let daemon = lifecycle
            .daemon
            .clone()
            .ok_or_else(|| provider_error("mDNS daemon is unavailable", true))?;
        // mdns-sd treats registering an existing full name as an in-place
        // re-announcement. Keeping the previous registration until this command
        // is accepted avoids a withdrawal gap when an endpoint is updated.
        let previous_own = self
            .inner
            .own_fullname
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .replace(fullname.clone());
        let previous_endpoint = self
            .inner
            .own_endpoint
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .replace(endpoint);
        if let Err(error) = daemon.register(service) {
            *self
                .inner
                .own_fullname
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = previous_own;
            *self
                .inner
                .own_endpoint
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = previous_endpoint;
            return Err(provider_error(
                format!("cannot register mDNS service: {error}"),
                true,
            ));
        }
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
        if let Some(shutdown) = &lifecycle.shutdown {
            shutdown.send_replace(true);
        }
    }

    async fn shutdown(&self) -> Result<(), DiscoveryError> {
        self.request_shutdown();
        let completion = {
            let lifecycle = self
                .inner
                .lifecycle
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            lifecycle.completion.clone()
        };
        wait_for_shutdown(completion).await
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_mdns_browse(
    config: MdnsDiscoveryConfig,
    state: Arc<DynamicState>,
    own_fullname: Arc<StdMutex<Option<String>>>,
    own_endpoint: Arc<StdMutex<Option<String>>>,
    events: mdns_sd::Receiver<ServiceEvent>,
    monitor: mdns_sd::Receiver<DaemonEvent>,
    mut cleanup: DaemonCleanup,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), DiscoveryError> {
    let mut instances = HashMap::<String, InstanceView>::new();
    let mut order = Vec::<String>::new();
    let mut expected_shutdown = false;
    loop {
        if *shutdown.borrow() {
            expected_shutdown = true;
            break;
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    expected_shutdown = true;
                    break;
                }
            }
            event = events.recv_async() => match event {
                Ok(ServiceEvent::ServiceResolved(service)) => {
                    let fullname = service.get_fullname().to_string();
                    let endpoints = bounded_mdns_endpoints(
                        service.get_addresses().iter().map(|address| address.to_ip_addr()),
                        service.get_port(),
                        config.max_candidates,
                    );
                    let matches_fullname = own_fullname.lock().unwrap_or_else(|error| error.into_inner())
                        .as_ref().is_some_and(|own| own == &fullname);
                    let matches_endpoint = own_endpoint.lock().unwrap_or_else(|error| error.into_inner())
                        .as_ref().is_some_and(|own| endpoints.contains(own));
                    if matches_fullname || matches_endpoint || service.get_property_val_str("cluster") != Some(config.cluster_id.as_str()) {
                        if remove_instance(&mut instances, &mut order, &fullname) {
                            publish_instances(&state, &instances, &order, config.max_candidates);
                        }
                        continue;
                    }
                    store_instance(&mut instances, &mut order, fullname, endpoints, &config);
                    publish_instances(&state, &instances, &order, config.max_candidates);
                }
                Ok(ServiceEvent::ServiceRemoved(_, fullname)) => {
                    if remove_instance(&mut instances, &mut order, &fullname) {
                        publish_instances(&state, &instances, &order, config.max_candidates);
                    }
                }
                Ok(ServiceEvent::SearchStopped(_)) => {
                    expected_shutdown = *shutdown.borrow();
                    if !expected_shutdown {
                        tracing::warn!(provider = PROVIDER, "mDNS browse stopped unexpectedly");
                    }
                    break;
                }
                Err(error) => {
                    tracing::warn!(%error, provider = PROVIDER, "mDNS event stream ended");
                    break;
                }
                Ok(_) => {}
            },
            event = monitor.recv_async() => match event {
                Ok(DaemonEvent::NameChange(change)) => {
                    if update_own_fullname(&own_fullname, &change) {
                        tracing::debug!(
                            original = %change.original,
                            new_name = %change.new_name,
                            "mDNS renamed the local service after a conflict"
                        );
                    }
                }
                Ok(DaemonEvent::Error(error)) => {
                    tracing::warn!(%error, provider = PROVIDER, "mDNS daemon failed");
                    break;
                }
                Err(error) => {
                    tracing::warn!(%error, provider = PROVIDER, "mDNS monitor stream ended");
                    break;
                }
                Ok(_) => {}
            }
        }
    }
    state.replace(Vec::new());
    if !expected_shutdown {
        state.invalidate_watches();
    }
    let result = shutdown_daemon(&mut cleanup, SHUTDOWN_BUDGET).await;
    if expected_shutdown {
        own_endpoint
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
    }
    result
}

async fn wait_for_shutdown(
    completion: Option<watch::Receiver<Option<Result<(), DiscoveryError>>>>,
) -> Result<(), DiscoveryError> {
    let Some(mut completion) = completion else {
        return Ok(());
    };
    loop {
        if let Some(result) = completion.borrow_and_update().clone() {
            return result;
        }
        completion.changed().await.map_err(|_| {
            provider_error(
                "mDNS cleanup task ended without an acknowledgement result",
                false,
            )
        })?;
    }
}

#[async_trait]
trait ShutdownDaemon: Send {
    async fn unregister(&mut self) -> Result<(), DiscoveryError>;
    async fn shutdown(&mut self) -> Result<(), DiscoveryError>;
}

struct DaemonCleanup {
    daemon: ServiceDaemon,
    service_type: String,
    own_fullname: Arc<StdMutex<Option<String>>>,
    finished: bool,
}

#[async_trait]
impl ShutdownDaemon for DaemonCleanup {
    async fn unregister(&mut self) -> Result<(), DiscoveryError> {
        let fullname = self
            .own_fullname
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        if let Some(fullname) = fullname {
            let ack = enqueue_daemon_command(|| self.daemon.unregister(&fullname)).await?;
            // OK and NotFound both mean the registration is no longer owned.
            ack.recv_async().await.map_err(|error| {
                provider_error(
                    format!("mDNS unregister acknowledgement failed: {error}"),
                    false,
                )
            })?;
            self.own_fullname
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take();
        }
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), DiscoveryError> {
        let _ = self.daemon.stop_browse(&self.service_type);
        let ack = enqueue_daemon_command(|| self.daemon.shutdown()).await?;
        let status = ack.recv_async().await.map_err(|error| {
            provider_error(
                format!("mDNS shutdown acknowledgement failed: {error}"),
                false,
            )
        })?;
        if status != DaemonStatus::Shutdown {
            return Err(provider_error(
                "unexpected mDNS shutdown acknowledgement",
                false,
            ));
        }
        self.finished = true;
        Ok(())
    }
}

async fn enqueue_daemon_command<T>(
    mut send: impl FnMut() -> mdns_sd::Result<T>,
) -> Result<T, DiscoveryError> {
    loop {
        match send() {
            Ok(result) => return Ok(result),
            // The enclosing ACK deadline also bounds command-queue retries.
            Err(mdns_sd::Error::Again) => tokio::time::sleep(Duration::from_millis(10)).await,
            Err(error) => {
                return Err(provider_error(
                    format!("mDNS command failed: {error}"),
                    false,
                ));
            }
        }
    }
}

impl Drop for DaemonCleanup {
    fn drop(&mut self) {
        if !self.finished {
            // Runtime teardown/panic fallback only; normal shutdown has one
            // owner and awaits ACKs. UDP delivery to every LAN peer is not guaranteed.
            if let Some(fullname) = self
                .own_fullname
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .as_deref()
            {
                let _ = self.daemon.unregister(fullname);
            }
            let _ = self.daemon.stop_browse(&self.service_type);
            let _ = self.daemon.shutdown();
        }
    }
}

async fn shutdown_daemon(
    daemon: &mut impl ShutdownDaemon,
    budget: Duration,
) -> Result<(), DiscoveryError> {
    let now = tokio::time::Instant::now();
    // Reserve half the common deadline for daemon termination, even when
    // withdrawal errors or its ACK never arrives.
    let withdrawal = tokio::time::timeout_at(now + budget / 2, daemon.unregister())
        .await
        .unwrap_or_else(|_| {
            Err(provider_error(
                "mDNS unregister acknowledgement timed out",
                false,
            ))
        });
    let shutdown = tokio::time::timeout_at(now + budget, daemon.shutdown())
        .await
        .unwrap_or_else(|_| {
            Err(provider_error(
                "mDNS shutdown acknowledgement timed out",
                false,
            ))
        });
    withdrawal.and(shutdown)
}

struct InstanceView {
    endpoints: Box<[String]>,
    observed_at: StdInstant,
}

fn store_instance(
    instances: &mut HashMap<String, InstanceView>,
    order: &mut Vec<String>,
    fullname: String,
    mut endpoints: Vec<String>,
    config: &MdnsDiscoveryConfig,
) {
    if !instances.contains_key(&fullname) && instances.len() >= config.max_instances {
        return;
    }
    // Duplicate contributions also consume capacity. A replacement reclaims
    // its own old allocation before admission; overflow is not retained off-view.
    let used: usize = instances
        .iter()
        .filter(|(name, _)| *name != &fullname)
        .map(|(_, view)| view.endpoints.len())
        .sum();
    endpoints.truncate(config.max_candidates.saturating_sub(used));
    if endpoints.is_empty() {
        remove_instance(instances, order, &fullname);
        return;
    }
    if !instances.contains_key(&fullname) {
        order.push(fullname.clone());
    }
    instances.insert(
        fullname,
        InstanceView {
            // Truncating a Vec alone retains its original capacity per instance.
            // Boxed storage also releases that otherwise multiplicative slack.
            endpoints: endpoints.into_boxed_slice(),
            observed_at: StdInstant::now(),
        },
    );
}

fn publish_instances(
    state: &DynamicState,
    instances: &HashMap<String, InstanceView>,
    order: &[String],
    max_candidates: usize,
) {
    let peers = flatten_instances(instances, order, max_candidates);
    state.observe_at(
        peers
            .into_iter()
            .filter_map(|peer| {
                let at = instances
                    .values()
                    .filter(|view| view.endpoints.contains(&peer))
                    .map(|view| view.observed_at)
                    .max()?;
                Some((peer, at))
            })
            .collect(),
    );
}

fn update_own_fullname(own_fullname: &StdMutex<Option<String>>, change: &DnsNameChange) -> bool {
    if change.rr_type != RRType::SRV {
        return false;
    }
    let mut own = own_fullname
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if !own
        .as_deref()
        .is_some_and(|fullname| fullname.eq_ignore_ascii_case(&change.original))
    {
        return false;
    }
    *own = Some(change.new_name.clone());
    true
}

fn remove_instance(
    instances: &mut HashMap<String, InstanceView>,
    order: &mut Vec<String>,
    fullname: &str,
) -> bool {
    let removed = instances.remove(fullname).is_some();
    if removed {
        order.retain(|known| known != fullname);
    }
    removed
}

fn flatten_instances(
    instances: &HashMap<String, InstanceView>,
    order: &[String],
    max_candidates: usize,
) -> Vec<String> {
    let mut peers = Vec::new();
    for fullname in order {
        let Some(view) = instances.get(fullname) else {
            continue;
        };
        for endpoint in &view.endpoints {
            if peers.len() == max_candidates {
                return peers;
            }
            if !peers.contains(endpoint) {
                peers.push(endpoint.clone());
            }
        }
    }
    peers
}

fn dialable_mdns_address(address: IpAddr, port: u16) -> Option<String> {
    if port == 0 || address.is_unspecified() || address.is_multicast() {
        return None;
    }
    if matches!(address, IpAddr::V6(address) if address.is_unicast_link_local()) {
        return None;
    }
    Some(SocketAddr::new(address, port).to_string())
}

fn bounded_mdns_endpoints(
    addresses: impl IntoIterator<Item = IpAddr>,
    port: u16,
    max_candidates: usize,
) -> Vec<String> {
    let mut endpoints = BTreeSet::new();
    for address in addresses {
        if let Some(endpoint) = dialable_mdns_address(address, port) {
            endpoints.insert(endpoint);
            if endpoints.len() > max_candidates {
                endpoints.pop_last();
            }
        }
    }
    endpoints.into_iter().collect()
}

fn cluster_service_type(cluster_id: &str) -> String {
    let hash = blake3::hash(cluster_id.as_bytes()).to_hex();
    format!("_c{}._sub.{SERVICE_BASE}", &hash[..16])
}

fn split_endpoint(endpoint: &str) -> Result<(String, u16), DiscoveryError> {
    if let Ok(socket) = endpoint.parse::<SocketAddr>() {
        return Ok((socket.ip().to_string(), socket.port()));
    }
    let (host, port) = endpoint
        .rsplit_once(':')
        .ok_or_else(|| provider_error("advertised endpoint must include a port", false))?;
    let port = port
        .parse::<u16>()
        .map_err(|_| provider_error("advertised endpoint has an invalid port", false))?;
    Ok((host.to_string(), port))
}

fn build_service(
    config: &MdnsDiscoveryConfig,
    service_type: &str,
    endpoint: &str,
) -> Result<(ServiceInfo, String), DiscoveryError> {
    let (host, port) = split_endpoint(endpoint)?;
    let hostname = format!(
        "numax-{}.local.",
        &blake3::hash(config.instance_name.as_bytes()).to_hex()[..16]
    );
    let properties = &[("cluster", config.cluster_id.as_str())];
    let service = match host.parse::<IpAddr>() {
        Ok(ip) => ServiceInfo::new(
            service_type,
            &config.instance_name,
            &hostname,
            ip,
            port,
            properties.as_slice(),
        ),
        Err(_) if host.ends_with(".local") => ServiceInfo::new(
            service_type,
            &config.instance_name,
            &format!("{host}."),
            "",
            port,
            properties.as_slice(),
        )
        .map(ServiceInfo::enable_addr_auto),
        Err(_) => {
            return Err(provider_error(
                "mDNS announcements require an IP address or .local hostname",
                false,
            ));
        }
    }
    .map_err(|error| provider_error(format!("invalid mDNS service: {error}"), false))?;
    let fullname = service.get_fullname().to_string();
    Ok((service, fullname))
}

fn validate_config(config: &MdnsDiscoveryConfig) -> Result<(), DiscoveryError> {
    if config.instance_name.is_empty() || config.instance_name.len() > 63 {
        return Err(invalid("instance_name length must be in 1..=63 bytes"));
    }
    if config.instance_name.chars().any(char::is_control) {
        return Err(invalid("instance_name must not contain control characters"));
    }
    if config.cluster_id.is_empty() || config.cluster_id.len() > 128 {
        return Err(invalid("cluster_id length must be in 1..=128 bytes"));
    }
    if config.max_instances == 0 || config.max_candidates == 0 || config.event_capacity == 0 {
        return Err(invalid(
            "limits and event_capacity must be greater than zero",
        ));
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
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::*;

    fn instance(endpoints: Vec<String>) -> InstanceView {
        InstanceView {
            endpoints: endpoints.into_boxed_slice(),
            observed_at: StdInstant::now(),
        }
    }

    #[test]
    fn global_endpoint_budget_counts_duplicates_and_reclaims_removals_and_replacements() {
        let mut config = MdnsDiscoveryConfig::new("test");
        config.max_candidates = 32;
        config.max_instances = 1024;
        let mut instances = HashMap::new();
        let mut order = Vec::new();
        let endpoints: Vec<_> = (1..=16).map(|port| format!("127.0.0.1:{port}")).collect();
        for index in 0..1024 {
            store_instance(
                &mut instances,
                &mut order,
                format!("peer-{index}"),
                endpoints.clone(),
                &config,
            );
            assert!(
                instances
                    .values()
                    .map(|view| view.endpoints.len())
                    .sum::<usize>()
                    <= config.max_candidates
            );
        }
        assert_eq!(order, ["peer-0", "peer-1"]);
        assert_eq!(flatten_instances(&instances, &order, 32), endpoints);
        store_instance(
            &mut instances,
            &mut order,
            "peer-0".into(),
            vec!["127.0.0.1:99".into()],
            &config,
        );
        store_instance(
            &mut instances,
            &mut order,
            "replacement".into(),
            endpoints.clone(),
            &config,
        );
        assert_eq!(
            instances["replacement"].endpoints.as_ref(),
            &endpoints[..15]
        );
        assert_eq!(
            instances
                .values()
                .map(|view| view.endpoints.len())
                .sum::<usize>(),
            32
        );
        assert!(remove_instance(&mut instances, &mut order, "peer-1"));
        store_instance(
            &mut instances,
            &mut order,
            "after-removal".into(),
            endpoints.clone(),
            &config,
        );
        assert_eq!(
            instances["after-removal"].endpoints.as_ref(),
            endpoints.as_slice()
        );
        assert_eq!(order, ["peer-0", "replacement", "after-removal"]);
        assert_eq!(
            instances
                .values()
                .map(|view| view.endpoints.len())
                .sum::<usize>(),
            32
        );
    }

    #[test]
    fn removing_one_instance_does_not_renew_other_instances() {
        let config = MdnsDiscoveryConfig::new("test");
        let mut instances = HashMap::new();
        let mut order = Vec::new();
        store_instance(
            &mut instances,
            &mut order,
            "a".into(),
            vec!["a:1".into()],
            &config,
        );
        store_instance(
            &mut instances,
            &mut order,
            "b".into(),
            vec!["b:2".into()],
            &config,
        );
        let observed = instances["a"].observed_at;
        let state = DynamicState::new(8);
        publish_instances(&state, &instances, &order, 8);
        remove_instance(&mut instances, &mut order, "b");
        publish_instances(&state, &instances, &order, 8);
        assert_eq!(state.snapshot().observations().unwrap(), [observed]);
    }

    struct ControlledDaemon {
        calls: tokio::sync::mpsc::Sender<&'static str>,
        unregister_ack: Option<tokio::sync::oneshot::Receiver<Result<(), DiscoveryError>>>,
        shutdown_ack: Option<tokio::sync::oneshot::Receiver<Result<(), DiscoveryError>>>,
    }

    #[async_trait]
    impl ShutdownDaemon for ControlledDaemon {
        async fn unregister(&mut self) -> Result<(), DiscoveryError> {
            self.calls.send("unregister").await.unwrap();
            self.unregister_ack
                .take()
                .unwrap()
                .await
                .map_err(|_| provider_error("unregister ack channel closed", false))?
        }
        async fn shutdown(&mut self) -> Result<(), DiscoveryError> {
            self.calls.send("shutdown").await.unwrap();
            self.shutdown_ack
                .take()
                .unwrap()
                .await
                .map_err(|_| provider_error("shutdown ack channel closed", false))?
        }
    }

    #[tokio::test]
    async fn shutdown_awaits_both_acks_and_waiter_cancellation_preserves_the_single_owner() {
        let (calls, mut call_rx) = tokio::sync::mpsc::channel(2);
        let (unregister_tx, unregister_ack) = tokio::sync::oneshot::channel();
        let (shutdown_tx, shutdown_ack) = tokio::sync::oneshot::channel();
        let mut daemon = ControlledDaemon {
            calls,
            unregister_ack: Some(unregister_ack),
            shutdown_ack: Some(shutdown_ack),
        };
        let (complete_tx, complete_rx) = watch::channel(None);
        let owner = tokio::spawn(async move {
            complete_tx.send_replace(Some(
                shutdown_daemon(&mut daemon, Duration::from_secs(2)).await,
            ));
        });
        let waiter_rx = complete_rx.clone();
        let waiter = tokio::spawn(wait_for_shutdown(Some(waiter_rx)));
        assert_eq!(call_rx.recv().await, Some("unregister"));
        assert!(call_rx.try_recv().is_err());
        assert!(complete_rx.borrow().is_none());
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        unregister_tx.send(Ok(())).unwrap();
        assert_eq!(call_rx.recv().await, Some("shutdown"));
        assert!(complete_rx.borrow().is_none());
        shutdown_tx.send(Ok(())).unwrap();
        wait_for_shutdown(Some(complete_rx.clone())).await.unwrap();
        wait_for_shutdown(Some(complete_rx)).await.unwrap();
        owner.await.unwrap();
        assert!(call_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn dropping_provider_signals_cleanup_without_aborting_acknowledgements() {
        let provider = MdnsDiscovery::new(MdnsDiscoveryConfig::new("drop-test")).unwrap();
        let (calls, mut call_rx) = tokio::sync::mpsc::channel(2);
        let (unregister_tx, unregister_ack) = tokio::sync::oneshot::channel();
        let (shutdown_tx, shutdown_ack) = tokio::sync::oneshot::channel();
        let mut daemon = ControlledDaemon {
            calls,
            unregister_ack: Some(unregister_ack),
            shutdown_ack: Some(shutdown_ack),
        };
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        let (complete_tx, complete_rx) = watch::channel(None);
        let task = tokio::spawn(async move {
            cancel_rx.changed().await.unwrap();
            assert!(*cancel_rx.borrow());
            complete_tx.send_replace(Some(
                shutdown_daemon(&mut daemon, Duration::from_secs(2)).await,
            ));
        });
        {
            let mut lifecycle = provider.inner.lifecycle.lock().unwrap();
            lifecycle.shutdown = Some(cancel_tx);
            lifecycle.task = Some(task);
            lifecycle.completion = Some(complete_rx.clone());
        }
        drop(provider);
        tokio::time::timeout(Duration::from_secs(2), async {
            assert_eq!(call_rx.recv().await, Some("unregister"));
            unregister_tx.send(Ok(())).unwrap();
            assert_eq!(call_rx.recv().await, Some("shutdown"));
            shutdown_tx.send(Ok(())).unwrap();
            wait_for_shutdown(Some(complete_rx)).await.unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn withdrawal_error_still_awaits_daemon_shutdown_and_reports_error() {
        let (calls, mut call_rx) = tokio::sync::mpsc::channel(2);
        let (unregister_tx, unregister_ack) = tokio::sync::oneshot::channel();
        let (shutdown_tx, shutdown_ack) = tokio::sync::oneshot::channel();
        let mut daemon = ControlledDaemon {
            calls,
            unregister_ack: Some(unregister_ack),
            shutdown_ack: Some(shutdown_ack),
        };
        let task =
            tokio::spawn(async move { shutdown_daemon(&mut daemon, Duration::from_secs(2)).await });
        assert_eq!(call_rx.recv().await, Some("unregister"));
        drop(unregister_tx);
        assert_eq!(call_rx.recv().await, Some("shutdown"));
        assert!(!task.is_finished());
        shutdown_tx.send(Ok(())).unwrap();
        assert_eq!(
            task.await.unwrap(),
            Err(provider_error("unregister ack channel closed", false))
        );
    }

    #[tokio::test]
    async fn missing_ack_deadlines_bound_withdrawal_and_shutdown() {
        let (calls, mut call_rx) = tokio::sync::mpsc::channel(2);
        let (_unregister_tx, unregister_ack) = tokio::sync::oneshot::channel();
        let (_shutdown_tx, shutdown_ack) = tokio::sync::oneshot::channel();
        let mut daemon = ControlledDaemon {
            calls,
            unregister_ack: Some(unregister_ack),
            shutdown_ack: Some(shutdown_ack),
        };
        let task =
            tokio::spawn(
                async move { shutdown_daemon(&mut daemon, Duration::from_millis(20)).await },
            );
        let result = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(call_rx.recv().await, Some("unregister"));
        assert_eq!(call_rx.recv().await, Some("shutdown"));
        assert_eq!(
            result,
            Err(provider_error(
                "mDNS unregister acknowledgement timed out",
                false
            ))
        );
    }

    #[tokio::test]
    async fn daemon_shutdown_ack_error_is_not_reported_as_success() {
        let (calls, _call_rx) = tokio::sync::mpsc::channel(2);
        let (unregister_tx, unregister_ack) = tokio::sync::oneshot::channel();
        let (shutdown_tx, shutdown_ack) = tokio::sync::oneshot::channel();
        unregister_tx.send(Ok(())).unwrap();
        drop(shutdown_tx);
        let mut daemon = ControlledDaemon {
            calls,
            unregister_ack: Some(unregister_ack),
            shutdown_ack: Some(shutdown_ack),
        };
        assert_eq!(
            shutdown_daemon(&mut daemon, Duration::from_secs(1)).await,
            Err(provider_error("shutdown ack channel closed", false))
        );
    }

    #[tokio::test]
    async fn full_daemon_queue_is_retried_but_permanent_errors_are_not() {
        let mut attempts = 0;
        let value = enqueue_daemon_command(|| {
            attempts += 1;
            if attempts == 1 {
                Err(mdns_sd::Error::Again)
            } else {
                Ok(42)
            }
        })
        .await
        .unwrap();
        assert_eq!(value, 42);
        assert_eq!(attempts, 2);
        assert!(
            enqueue_daemon_command::<()>(|| Err(mdns_sd::Error::DaemonShutdown))
                .await
                .is_err()
        );
    }

    #[test]
    fn cluster_service_types_are_stable_and_isolated() {
        assert_eq!(cluster_service_type("a"), cluster_service_type("a"));
        assert_ne!(cluster_service_type("a"), cluster_service_type("b"));
        assert!(cluster_service_type("a").ends_with(SERVICE_BASE));
    }

    #[test]
    fn reducer_deduplicates_shared_endpoints_and_preserves_instance_order() {
        let instances = HashMap::from([
            ("a".to_string(), instance(vec!["127.0.0.1:1".to_string()])),
            (
                "b".to_string(),
                instance(vec!["127.0.0.1:1".to_string(), "127.0.0.1:2".to_string()]),
            ),
        ]);
        assert_eq!(
            flatten_instances(&instances, &["a".into(), "b".into()], 8),
            ["127.0.0.1:1", "127.0.0.1:2"]
        );
    }

    #[test]
    fn undialable_addresses_are_filtered() {
        assert!(dialable_mdns_address("0.0.0.0".parse().unwrap(), 9000).is_none());
        assert!(dialable_mdns_address("ff02::1".parse().unwrap(), 9000).is_none());
        assert!(dialable_mdns_address("fe80::1".parse().unwrap(), 9000).is_none());
        assert_eq!(
            dialable_mdns_address("127.0.0.1".parse().unwrap(), 9000),
            Some("127.0.0.1:9000".into())
        );
    }

    #[test]
    fn resolved_instance_addresses_are_bounded_and_deterministic() {
        let addresses = [
            "127.0.0.3".parse().unwrap(),
            "127.0.0.1".parse().unwrap(),
            "127.0.0.2".parse().unwrap(),
            "127.0.0.1".parse().unwrap(),
        ];

        assert_eq!(
            bounded_mdns_endpoints(addresses, 9000, 2),
            ["127.0.0.1:9000", "127.0.0.2:9000"]
        );
    }

    #[test]
    fn rejected_resolution_removes_a_previously_accepted_instance() {
        let mut instances = HashMap::from([(
            "peer._numax._tcp.local.".into(),
            instance(vec!["127.0.0.1:9000".into()]),
        )]);
        let mut order = vec!["peer._numax._tcp.local.".into()];

        assert!(remove_instance(
            &mut instances,
            &mut order,
            "peer._numax._tcp.local."
        ));
        assert!(instances.is_empty());
        assert!(order.is_empty());
    }

    #[test]
    fn service_name_conflicts_update_the_self_filter() {
        let own_fullname = StdMutex::new(Some("node._numax._tcp.local.".into()));
        let change = DnsNameChange {
            original: "node._numax._tcp.local.".into(),
            new_name: "node (2)._numax._tcp.local.".into(),
            rr_type: RRType::SRV,
            intf_name: "test".into(),
        };

        assert!(update_own_fullname(&own_fullname, &change));
        assert_eq!(
            own_fullname.into_inner().unwrap(),
            Some("node (2)._numax._tcp.local.".into())
        );
    }

    #[tokio::test]
    #[ignore = "requires local multicast mDNS networking"]
    async fn two_daemons_discover_and_remove_an_announced_endpoint() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let suffix = format!("{}-{nonce}", std::process::id());
        let cluster = format!("mdns-test-{suffix}");
        let mut publisher_config = MdnsDiscoveryConfig::new(format!("publisher-{suffix}"));
        publisher_config.cluster_id = cluster.clone();
        let mut observer_config = MdnsDiscoveryConfig::new(format!("observer-{suffix}"));
        observer_config.cluster_id = cluster;
        let publisher = MdnsDiscovery::new(publisher_config).unwrap();
        let observer = MdnsDiscovery::new(observer_config).unwrap();
        let endpoint = "127.0.0.1:43111";
        let mut watch = observer.watch().await.unwrap();

        publisher
            .announce(&PeerAnnouncement {
                endpoint: endpoint.into(),
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if super::super::observed_peers(watch.recv().await.unwrap().change)
                    == vec![endpoint.to_string()]
                {
                    break;
                }
            }
        })
        .await
        .unwrap();

        publisher.shutdown().await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if super::super::observed_peers(watch.recv().await.unwrap().change).is_empty() {
                    break;
                }
            }
        })
        .await
        .unwrap();

        observer.shutdown().await.unwrap();
    }
}
