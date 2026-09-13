use std::collections::{BTreeSet, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex as StdMutex};

use async_trait::async_trait;
use mdns_sd::{DaemonEvent, DnsNameChange, RRType, ServiceDaemon, ServiceEvent, ServiceInfo};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::dynamic::{AbortOnDropTask, DynamicState};
use super::{
    AnnouncementSupport, DEFAULT_DISCOVERY_CLUSTER, DEFAULT_DISCOVERY_EVENT_CAPACITY,
    DEFAULT_MAX_PEER_CANDIDATES, DiscoveryError, DiscoverySnapshot, DiscoveryWatch,
    PeerAnnouncement, PeerDiscovery,
};

const PROVIDER: &str = "mdns";
const SERVICE_BASE: &str = "_numax._tcp.local.";
const DEFAULT_MAX_INSTANCES: usize = 1024;

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
        if let Some(task) = lifecycle.task.take() {
            task.abort();
        }
        if let Some(daemon) = lifecycle.daemon.take() {
            if let Some(fullname) = self
                .own_fullname
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
            {
                let _ = daemon.unregister(&fullname);
            }
            let _ = daemon.stop_browse(&self.service_type);
            let _ = daemon.shutdown();
        }
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
        let config = self.inner.config.clone();
        let state = Arc::clone(&self.inner.state);
        let own_fullname = Arc::clone(&self.inner.own_fullname);
        let own_endpoint = Arc::clone(&self.inner.own_endpoint);
        let task_daemon = daemon.clone();
        let service_type = self.inner.service_type.clone();
        lifecycle.shutdown = Some(shutdown);
        lifecycle.daemon = Some(daemon);
        lifecycle.task = Some(tokio::spawn(async move {
            run_mdns_browse(
                config,
                state,
                own_fullname,
                own_endpoint,
                events,
                monitor,
                task_daemon,
                service_type,
                shutdown_rx,
            )
            .await;
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
        let (daemon, fullname) = {
            let mut lifecycle = self
                .inner
                .lifecycle
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if lifecycle.stopped {
                return;
            }
            lifecycle.stopped = true;
            if let Some(shutdown) = lifecycle.shutdown.as_ref() {
                let _ = shutdown.send(true);
            }
            (
                lifecycle.daemon.clone(),
                self.inner
                    .own_fullname
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .take(),
            )
        };
        *self
            .inner
            .own_endpoint
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = None;
        self.inner.state.replace(Vec::new());
        if let Some(daemon) = daemon {
            if let Some(fullname) = fullname
                && let Err(error) = daemon.unregister(&fullname)
            {
                tracing::warn!(%error, provider = PROVIDER, "cannot request mDNS withdrawal");
            }
            if let Err(error) = daemon.stop_browse(&self.inner.service_type) {
                tracing::debug!(%error, provider = PROVIDER, "cannot request mDNS browse stop");
            }
            if let Err(error) = daemon.shutdown() {
                tracing::warn!(%error, provider = PROVIDER, "cannot request mDNS daemon shutdown");
            }
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
            lifecycle.daemon.take();
            lifecycle.task.take().map(AbortOnDropTask::new)
        };
        if let Some(task) = task
            && let Err(error) = task.join().await
        {
            return Err(provider_error(
                format!("browse task failed: {error}"),
                false,
            ));
        }
        *self
            .inner
            .own_fullname
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = None;
        self.inner.state.replace(Vec::new());
        Ok(())
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
    daemon: ServiceDaemon,
    service_type: String,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut instances = HashMap::<String, Vec<String>>::new();
    let mut order = Vec::<String>::new();
    let mut expected_shutdown = false;
    loop {
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
                            state.replace(flatten_instances(&instances, &order, config.max_candidates));
                        }
                        continue;
                    }
                    if !instances.contains_key(&fullname) && instances.len() >= config.max_instances {
                        tracing::warn!(provider = PROVIDER, limit = config.max_instances, "ignoring mDNS instance beyond limit");
                        continue;
                    }
                    if !instances.contains_key(&fullname) {
                        order.push(fullname.clone());
                    }
                    instances.insert(fullname, endpoints);
                    state.replace(flatten_instances(&instances, &order, config.max_candidates));
                }
                Ok(ServiceEvent::ServiceRemoved(_, fullname)) => {
                    if remove_instance(&mut instances, &mut order, &fullname) {
                        state.replace(flatten_instances(&instances, &order, config.max_candidates));
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
    if let Some(fullname) = own_fullname
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .as_deref()
    {
        let _ = daemon.unregister(fullname);
    }
    let _ = daemon.stop_browse(&service_type);
    let _ = daemon.shutdown();
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
    instances: &mut HashMap<String, Vec<String>>,
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
    instances: &HashMap<String, Vec<String>>,
    order: &[String],
    max_candidates: usize,
) -> Vec<String> {
    let mut peers = Vec::new();
    for fullname in order {
        let Some(endpoints) = instances.get(fullname) else {
            continue;
        };
        for endpoint in endpoints {
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

    #[test]
    fn cluster_service_types_are_stable_and_isolated() {
        assert_eq!(cluster_service_type("a"), cluster_service_type("a"));
        assert_ne!(cluster_service_type("a"), cluster_service_type("b"));
        assert!(cluster_service_type("a").ends_with(SERVICE_BASE));
    }

    #[test]
    fn reducer_deduplicates_shared_endpoints_and_preserves_instance_order() {
        let instances = HashMap::from([
            ("a".to_string(), vec!["127.0.0.1:1".to_string()]),
            (
                "b".to_string(),
                vec!["127.0.0.1:1".to_string(), "127.0.0.1:2".to_string()],
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
            vec!["127.0.0.1:9000".into()],
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
                if watch.recv().await.unwrap().change
                    == super::super::DiscoveryChange::Replaced(vec![endpoint.into()])
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
                if watch.recv().await.unwrap().change
                    == super::super::DiscoveryChange::Replaced(Vec::new())
                {
                    break;
                }
            }
        })
        .await
        .unwrap();

        observer.shutdown().await.unwrap();
    }
}
