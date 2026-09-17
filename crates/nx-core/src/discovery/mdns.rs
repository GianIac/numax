use std::collections::{BTreeSet, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant as StdInstant};

use async_trait::async_trait;
use mdns_sd::{
    DaemonEvent, DaemonStatus, DnsNameChange, RRType, ServiceDaemon, ServiceEvent, ServiceInfo,
};
use tokio::sync::{mpsc, oneshot, watch};

use super::dynamic::{DynamicState, ProviderTask};
use super::{
    AnnouncementSupport, DEFAULT_DISCOVERY_CLUSTER, DEFAULT_DISCOVERY_EVENT_CAPACITY,
    DEFAULT_MAX_PEER_CANDIDATES, DiscoveryError, DiscoverySnapshot, DiscoveryWatch,
    PeerAnnouncement, PeerDiscovery, validate_event_capacity,
};

const PROVIDER: &str = "mdns";
const SERVICE_BASE: &str = "_numax._tcp.local.";
const DEFAULT_MAX_INSTANCES: usize = 1024;
const SHUTDOWN_BUDGET: Duration = Duration::from_secs(4);
const MAX_OWN_HISTORY: usize = 1024;

struct AnnounceRequest {
    endpoint: String,
    reply: oneshot::Sender<Result<(), DiscoveryError>>,
}

/// LAN mDNS discovery and announcement limits.
#[derive(Debug, Clone)]
pub struct MdnsDiscoveryConfig {
    pub instance_name: String,
    pub cluster_id: String,
    pub max_instances: usize,
    pub max_candidates: usize,
    /// Event and announcement channel capacity in `1..=super::MAX_DISCOVERY_EVENT_CAPACITY`.
    /// Defaults to [`DEFAULT_DISCOVERY_EVENT_CAPACITY`]; validated by the provider constructor.
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
    shutdown: Option<ShutdownRequest>,
    task: Option<ProviderTask>,
    announcements: Option<mpsc::Sender<AnnounceRequest>>,
}

struct ShutdownRequest {
    requested: watch::Sender<bool>,
    deadline: watch::Sender<Option<tokio::time::Instant>>,
}

struct MdnsGeneration {
    shutdown: ShutdownRequest,
    announcements: mpsc::Sender<AnnounceRequest>,
    task: ProviderTask,
}

struct Inner {
    config: MdnsDiscoveryConfig,
    service_type: String,
    state: Arc<DynamicState>,
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
            request_shutdown(&shutdown, SHUTDOWN_BUDGET);
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
                own_endpoint: Arc::new(StdMutex::new(None)),
                config,
                lifecycle: StdMutex::new(Lifecycle {
                    stopped: false,
                    shutdown: None,
                    task: None,
                    announcements: None,
                }),
            }),
        })
    }

    fn ensure_started(&self) -> Result<(), DiscoveryError> {
        self.ensure_started_with(|| self.start_generation())
    }

    fn ensure_started_with(
        &self,
        start: impl FnOnce() -> Result<MdnsGeneration, DiscoveryError>,
    ) -> Result<(), DiscoveryError> {
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

        let generation = start()?;
        lifecycle.shutdown = Some(generation.shutdown);
        lifecycle.announcements = Some(generation.announcements);
        lifecycle.task = Some(generation.task);
        Ok(())
    }

    fn start_generation(&self) -> Result<MdnsGeneration, DiscoveryError> {
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
        let mut owned = OwnedAnnouncements::default();
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
            owned.accept(fullname, endpoint);
        }
        let (shutdown, shutdown_rx, shutdown_deadline_rx) = shutdown_channels();
        let (announcements_tx, announcements_rx) = mpsc::channel(self.inner.config.event_capacity);
        let config = self.inner.config.clone();
        let state = Arc::clone(&self.inner.state);
        let own_endpoint = Arc::clone(&self.inner.own_endpoint);
        // Construct the guard before spawning: cancellation before the first
        // task poll must still release the external daemon.
        let cleanup = DaemonCleanup {
            daemon: LiveDaemon {
                daemon,
                service_type: self.inner.service_type.clone(),
            },
            owned,
            finished: false,
        };
        let task = start_mdns_task(
            config,
            state,
            own_endpoint,
            events,
            monitor,
            cleanup,
            announcements_rx,
            shutdown_rx,
            shutdown_deadline_rx,
        );
        Ok(MdnsGeneration {
            shutdown,
            announcements: announcements_tx,
            task,
        })
    }

    fn request_shutdown_with_budget(&self, budget: Duration) {
        let mut lifecycle = self
            .inner
            .lifecycle
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        lifecycle.stopped = true;
        if let Some(shutdown) = &lifecycle.shutdown {
            request_shutdown(shutdown, budget);
        }
        self.inner
            .own_endpoint
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
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
        let (reply, response) = oneshot::channel();
        {
            let lifecycle = self
                .inner
                .lifecycle
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if lifecycle.stopped {
                return Err(provider_error("provider is shut down", false));
            }
            lifecycle
                .announcements
                .as_ref()
                .ok_or_else(|| provider_error("mDNS daemon is unavailable", true))?
                .try_send(AnnounceRequest { endpoint, reply })
                .map_err(|error| {
                    provider_error(format!("cannot queue mDNS announcement: {error}"), true)
                })?;
        }
        // Once queued, the browse task owns the transaction, even if this
        // waiter is cancelled. It also serializes NameChange and shutdown.
        response
            .await
            .map_err(|_| provider_error("mDNS announcement task stopped", true))?
    }

    async fn watch(&self) -> Result<DiscoveryWatch, DiscoveryError> {
        self.ensure_started()?;
        self.inner.state.live_watch()
    }

    fn request_shutdown(&self) {
        self.request_shutdown_with_budget(SHUTDOWN_BUDGET);
    }

    async fn shutdown(&self) -> Result<(), DiscoveryError> {
        self.request_shutdown();
        let completion = {
            let lifecycle = self
                .inner
                .lifecycle
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            lifecycle.task.clone()
        };
        match completion {
            Some(task) => task.join().await,
            None => Ok(()),
        }
    }
}

fn request_shutdown(shutdown: &ShutdownRequest, budget: Duration) {
    // Publish the stop intent before the worker-visible deadline. Otherwise the
    // worker could exit between the two notifications and be misclassified by
    // the supervisor as an unexpected termination.
    shutdown.requested.send_replace(true);
    if shutdown.deadline.borrow().is_none() {
        shutdown.deadline.send_replace(Some(deadline_after(budget)));
    }
}

fn deadline_after(budget: Duration) -> tokio::time::Instant {
    let now = tokio::time::Instant::now();
    now.checked_add(budget).unwrap_or(now)
}

fn shutdown_channels() -> (
    ShutdownRequest,
    watch::Receiver<bool>,
    watch::Receiver<Option<tokio::time::Instant>>,
) {
    let (requested, requested_rx) = watch::channel(false);
    let (deadline, deadline_rx) = watch::channel(None);
    (
        ShutdownRequest {
            requested,
            deadline,
        },
        requested_rx,
        deadline_rx,
    )
}

#[async_trait]
trait MdnsReceiver<T>: Send {
    async fn next(&mut self) -> Result<T, DiscoveryError>;
}

#[async_trait]
impl<T: Send + 'static> MdnsReceiver<T> for mdns_sd::Receiver<T> {
    async fn next(&mut self) -> Result<T, DiscoveryError> {
        self.recv_async()
            .await
            .map_err(|error| provider_error(format!("mDNS event stream ended: {error}"), true))
    }
}

#[allow(clippy::too_many_arguments)]
fn start_mdns_task<D: RegistrationDaemon + 'static>(
    config: MdnsDiscoveryConfig,
    state: Arc<DynamicState>,
    own_endpoint: Arc<StdMutex<Option<String>>>,
    events: impl MdnsReceiver<ServiceEvent> + 'static,
    monitor: impl MdnsReceiver<DaemonEvent> + 'static,
    cleanup: DaemonCleanup<D>,
    announcements: mpsc::Receiver<AnnounceRequest>,
    shutdown: watch::Receiver<bool>,
    shutdown_deadline: watch::Receiver<Option<tokio::time::Instant>>,
) -> ProviderTask {
    let cleanup = Arc::new(tokio::sync::Mutex::new(cleanup));
    let worker_cleanup = cleanup.clone();
    let worker_state = state.clone();
    let worker_endpoint = own_endpoint.clone();
    let worker_shutdown_deadline = shutdown_deadline.clone();
    ProviderTask::spawn(
        PROVIDER,
        state,
        shutdown.clone(),
        async move {
            let mut cleanup = worker_cleanup.lock().await;
            run_mdns_events(
                config,
                worker_state,
                worker_endpoint,
                events,
                monitor,
                &mut *cleanup,
                announcements,
                worker_shutdown_deadline,
            )
            .await
        },
        move || async move {
            // The worker has been joined, including after panic. Its async lock
            // guard is gone, while original registration keys remain owned here.
            let deadline = shutdown_deadline
                .borrow()
                .unwrap_or_else(|| deadline_after(SHUTDOWN_BUDGET));
            let result = shutdown_daemon_until(&mut *cleanup.lock().await, deadline).await;
            if *shutdown.borrow() || shutdown.has_changed().is_err() {
                own_endpoint
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .take();
            }
            // DaemonCleanup's fallback must also finish before restart admission.
            drop(cleanup);
            result
        },
    )
}

#[allow(clippy::too_many_arguments)]
async fn run_mdns_events<D: RegistrationDaemon>(
    config: MdnsDiscoveryConfig,
    state: Arc<DynamicState>,
    own_endpoint: Arc<StdMutex<Option<String>>>,
    mut events: impl MdnsReceiver<ServiceEvent>,
    mut monitor: impl MdnsReceiver<DaemonEvent>,
    cleanup: &mut DaemonCleanup<D>,
    mut announcements: mpsc::Receiver<AnnounceRequest>,
    mut shutdown: watch::Receiver<Option<tokio::time::Instant>>,
) -> Result<(), DiscoveryError> {
    let mut instances = HashMap::<String, InstanceView>::new();
    let mut order = Vec::<String>::new();
    let mut expected_shutdown = false;
    loop {
        if shutdown.borrow().is_some() {
            expected_shutdown = true;
            break;
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || shutdown.borrow().is_some() {
                    expected_shutdown = true;
                    break;
                }
            }
            Some(request) = announcements.recv() => {
                if shutdown.borrow().is_some() {
                    let _ = request.reply.send(Err(provider_error("provider is shut down", false)));
                    expected_shutdown = true;
                    break;
                }
                let result = replace_announcement_with_shutdown(
                    cleanup,
                    &config,
                    request.endpoint,
                    &mut shutdown,
                ).await;
                if let Some(current) = &cleanup.owned.current {
                    *own_endpoint.lock().unwrap_or_else(|error| error.into_inner()) =
                        Some(current.endpoint.clone());
                }
                remove_owned_instances(&cleanup.owned, &mut instances, &mut order);
                publish_instances(&state, &instances, &order, config.max_candidates);
                let _ = request.reply.send(result);
                // A failed retirement must not accumulate registrations on
                // subsequent updates. Cleanup still owns both original keys.
                expected_shutdown = shutdown.borrow().is_some();
                if expected_shutdown || cleanup.owned.keys.len() > 1 {
                    break;
                }
            }
            event = events.next() => match event {
                Ok(ServiceEvent::ServiceResolved(service)) => {
                    let fullname = service.get_fullname().to_string();
                    let endpoints = bounded_mdns_endpoints(
                        service.get_addresses().iter().map(|address| address.to_ip_addr()),
                        service.get_port(),
                        config.max_candidates,
                    );
                    if cleanup.owned.matches(&fullname, &endpoints) || service.get_property_val_str("cluster") != Some(config.cluster_id.as_str()) {
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
                    expected_shutdown = shutdown.borrow().is_some();
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
            event = monitor.next() => match event {
                Ok(DaemonEvent::NameChange(change)) => {
                    let updated = match cleanup.owned.name_change(&change) {
                        Ok(updated) => updated,
                        Err(error) => {
                            tracing::warn!(%error, "mDNS own-name history exhausted");
                            break;
                        }
                    };
                    if updated {
                        remove_owned_instances(&cleanup.owned, &mut instances, &mut order);
                        publish_instances(&state, &instances, &order, config.max_candidates);
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
    announcements.close();
    while let Ok(request) = announcements.try_recv() {
        let error = if expected_shutdown || shutdown.borrow().is_some() {
            provider_error("provider is shut down", false)
        } else {
            provider_error("mDNS announcement task stopped", true)
        };
        let _ = request.reply.send(Err(error));
    }
    if expected_shutdown {
        Ok(())
    } else {
        Err(provider_error("mDNS browse ended unexpectedly", true))
    }
}

#[cfg(test)]
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
    async fn unregister(&mut self, deadline: tokio::time::Instant) -> Result<(), DiscoveryError>;
    async fn shutdown(&mut self) -> Result<(), DiscoveryError>;
}

#[async_trait]
trait RegistrationDaemon: Send {
    fn register(&mut self, service: ServiceInfo) -> Result<(), DiscoveryError>;
    async fn withdraw(&mut self, key: &str) -> Result<(), DiscoveryError>;
    async fn terminate(&mut self) -> Result<(), DiscoveryError>;
    fn fallback(&mut self, keys: &BTreeSet<String>);
}

struct LiveDaemon {
    daemon: ServiceDaemon,
    service_type: String,
}

struct DaemonCleanup<D: RegistrationDaemon = LiveDaemon> {
    daemon: D,
    owned: OwnedAnnouncements,
    finished: bool,
}

#[async_trait]
impl<D: RegistrationDaemon> ShutdownDaemon for DaemonCleanup<D> {
    async fn unregister(&mut self, deadline: tokio::time::Instant) -> Result<(), DiscoveryError> {
        let mut result = Ok(());
        let keys = self.owned.keys.clone();
        for (index, key) in keys.iter().enumerate() {
            let remaining_keys = (keys.len() - index) as u32;
            let now = tokio::time::Instant::now();
            let key_deadline = now
                .checked_add(deadline.saturating_duration_since(now) / remaining_keys)
                .unwrap_or(deadline)
                .min(deadline);
            let withdrawal = tokio::time::timeout_at(key_deadline, self.daemon.withdraw(key))
                .await
                .unwrap_or_else(|_| {
                    Err(provider_error(
                        "mDNS unregister acknowledgement timed out",
                        false,
                    ))
                });
            match withdrawal {
                Ok(()) => {
                    self.owned.keys.remove(key);
                }
                Err(error) => {
                    result = result.and(Err(error));
                }
            }
        }
        result
    }

    async fn shutdown(&mut self) -> Result<(), DiscoveryError> {
        self.daemon.terminate().await?;
        self.owned = OwnedAnnouncements::default();
        self.finished = true;
        Ok(())
    }
}

#[async_trait]
impl RegistrationDaemon for LiveDaemon {
    fn register(&mut self, service: ServiceInfo) -> Result<(), DiscoveryError> {
        self.daemon
            .register(service)
            .map_err(|error| provider_error(format!("cannot register mDNS service: {error}"), true))
    }

    async fn withdraw(&mut self, key: &str) -> Result<(), DiscoveryError> {
        let ack = enqueue_daemon_command(|| self.daemon.unregister(key)).await?;
        // OK and NotFound both mean this original registration key is gone.
        ack.recv_async().await.map_err(|error| {
            provider_error(
                format!("mDNS unregister acknowledgement failed: {error}"),
                false,
            )
        })?;
        Ok(())
    }

    async fn terminate(&mut self) -> Result<(), DiscoveryError> {
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
        Ok(())
    }

    fn fallback(&mut self, keys: &BTreeSet<String>) {
        for key in keys {
            let _ = self.daemon.unregister(key);
        }
        let _ = self.daemon.stop_browse(&self.service_type);
        let _ = self.daemon.shutdown();
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

impl<D: RegistrationDaemon> Drop for DaemonCleanup<D> {
    fn drop(&mut self) {
        if !self.finished {
            // Runtime teardown/panic fallback only; normal shutdown has one
            // owner and awaits ACKs. UDP delivery to every LAN peer is not guaranteed.
            self.daemon.fallback(&self.owned.keys);
        }
    }
}

async fn shutdown_daemon_until(
    daemon: &mut impl ShutdownDaemon,
    deadline: tokio::time::Instant,
) -> Result<(), DiscoveryError> {
    let now = tokio::time::Instant::now();
    // Reserve half the common deadline for daemon termination, even when
    // withdrawal errors or its ACK never arrives.
    let withdrawal_deadline = now
        .checked_add(deadline.saturating_duration_since(now) / 2)
        .unwrap_or(deadline)
        .min(deadline);
    let withdrawal =
        tokio::time::timeout_at(withdrawal_deadline, daemon.unregister(withdrawal_deadline))
            .await
            .unwrap_or_else(|_| {
                Err(provider_error(
                    "mDNS unregister acknowledgement timed out",
                    false,
                ))
            });
    let shutdown = daemon.shutdown();
    tokio::pin!(shutdown);
    let shutdown = tokio::select! {
        biased;
        result = &mut shutdown => result,
        () = tokio::time::sleep_until(deadline) => Err(provider_error(
            "mDNS shutdown acknowledgement timed out",
            false,
        )),
    };
    withdrawal.and(shutdown)
}

#[cfg(test)]
async fn shutdown_daemon(
    daemon: &mut impl ShutdownDaemon,
    budget: Duration,
) -> Result<(), DiscoveryError> {
    shutdown_daemon_until(daemon, deadline_after(budget)).await
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

struct CurrentAnnouncement {
    key: String,
    endpoint: String,
}

#[derive(Default)]
struct OwnedAnnouncements {
    current: Option<CurrentAnnouncement>,
    // mdns-sd 0.21.3 register_service/remove_entry use the original lowercase
    // ServiceInfo fullname. NameChange only updates per-interface wire aliases;
    // unregister_service resolves those aliases when constructing goodbyes.
    keys: BTreeSet<String>,
    // Keep retired names/endpoints until daemon termination: browse and monitor
    // streams are independent, and cached/queued resolutions can arrive late.
    names: BTreeSet<String>,
    endpoints: BTreeSet<String>,
    generation: usize,
}

impl OwnedAnnouncements {
    fn accept(&mut self, fullname: String, endpoint: String) {
        let key = fullname.to_lowercase();
        self.keys.insert(key.clone());
        self.names.insert(key.clone());
        self.endpoints.insert(endpoint.clone());
        self.current = Some(CurrentAnnouncement { key, endpoint });
        self.generation += 1;
    }

    fn matches(&self, fullname: &str, endpoints: &[String]) -> bool {
        self.names.contains(&fullname.to_lowercase())
            || endpoints
                .iter()
                .any(|endpoint| self.endpoints.contains(endpoint))
    }

    fn name_change(&mut self, change: &DnsNameChange) -> Result<bool, DiscoveryError> {
        if change.rr_type != RRType::SRV || !self.names.contains(&change.original.to_lowercase()) {
            return Ok(false);
        }
        let name = change.new_name.to_lowercase();
        if !self.names.contains(&name) && self.names.len() >= MAX_OWN_HISTORY {
            return Err(provider_error("mDNS own-name history limit reached", true));
        }
        self.names.insert(name);
        Ok(true)
    }
}

#[cfg(test)]
async fn replace_announcement<D: RegistrationDaemon>(
    cleanup: &mut DaemonCleanup<D>,
    config: &MdnsDiscoveryConfig,
    endpoint: String,
) -> Result<(), DiscoveryError> {
    let (_shutdown, mut shutdown) = watch::channel(None);
    replace_announcement_with_shutdown(cleanup, config, endpoint, &mut shutdown).await
}

async fn replace_announcement_with_shutdown<D: RegistrationDaemon>(
    cleanup: &mut DaemonCleanup<D>,
    config: &MdnsDiscoveryConfig,
    endpoint: String,
    shutdown: &mut watch::Receiver<Option<tokio::time::Instant>>,
) -> Result<(), DiscoveryError> {
    if cleanup.owned.names.len() >= MAX_OWN_HISTORY
        || cleanup.owned.endpoints.len() >= MAX_OWN_HISTORY
        || cleanup.owned.keys.len() > 1
    {
        return Err(provider_error(
            "mDNS announcement history limit reached",
            true,
        ));
    }
    let mut config = config.clone();
    if cleanup.owned.current.is_some() {
        // A distinct ORIGINAL key lets us register first (failure leaves the
        // old service intact), then withdraw its old ServiceInfo/endpoint.
        // Reusing the key would overwrite that info before its goodbye; using
        // an observed alias would unregister NotFound instead of the service.
        config.instance_name = format!(
            "nx-{}-{}",
            &blake3::hash(config.instance_name.as_bytes()).to_hex()[..16],
            cleanup.owned.generation,
        );
    }
    let (service, fullname) = build_service(
        &config,
        &cluster_service_type(&config.cluster_id),
        &endpoint,
    )?;
    if cleanup.owned.names.contains(&fullname.to_lowercase()) {
        return Err(provider_error(
            "mDNS replacement key is already owned",
            true,
        ));
    }
    let previous = cleanup
        .owned
        .current
        .as_ref()
        .map(|current| current.key.clone());
    cleanup.daemon.register(service)?;
    cleanup.owned.accept(fullname, endpoint);
    if let Some(previous) = previous {
        let withdrawal = cleanup.daemon.withdraw(&previous);
        tokio::pin!(withdrawal);
        let timeout = tokio::time::sleep(SHUTDOWN_BUDGET / 2);
        tokio::pin!(timeout);
        let result = tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || shutdown.borrow().is_some() {
                    Err(provider_error("provider is shut down", false))
                } else {
                    Err(provider_error("mDNS announcement task stopped", true))
                }
            }
            result = &mut withdrawal => result,
            () = &mut timeout => {
                Err(provider_error("mDNS replacement withdrawal timed out", true))
            }
        };
        result?;
        cleanup.owned.keys.remove(&previous);
    }
    Ok(())
}

fn remove_owned_instances(
    owned: &OwnedAnnouncements,
    instances: &mut HashMap<String, InstanceView>,
    order: &mut Vec<String>,
) {
    instances.retain(|name, view| !owned.matches(name, &view.endpoints));
    order.retain(|name| instances.contains_key(name));
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
    validate_event_capacity(PROVIDER, config.event_capacity)?;
    if config.instance_name.is_empty() || config.instance_name.len() > 63 {
        return Err(invalid("instance_name length must be in 1..=63 bytes"));
    }
    if config.instance_name.chars().any(char::is_control) {
        return Err(invalid("instance_name must not contain control characters"));
    }
    if config.cluster_id.is_empty() || config.cluster_id.len() > 128 {
        return Err(invalid("cluster_id length must be in 1..=128 bytes"));
    }
    if config.max_instances == 0 || config.max_candidates == 0 {
        return Err(invalid("limits must be greater than zero"));
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
        async fn unregister(
            &mut self,
            _deadline: tokio::time::Instant,
        ) -> Result<(), DiscoveryError> {
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
        let (shutdown, mut cancel_rx, _deadline_rx) = shutdown_channels();
        let task = ProviderTask::spawn(
            PROVIDER,
            provider.inner.state.clone(),
            cancel_rx.clone(),
            async move {
                cancel_rx.changed().await.unwrap();
                assert!(*cancel_rx.borrow());
                Ok(())
            },
            move || async move { shutdown_daemon(&mut daemon, Duration::from_secs(2)).await },
        );
        let completion = task.clone();
        {
            let mut lifecycle = provider.inner.lifecycle.lock().unwrap();
            lifecycle.shutdown = Some(shutdown);
            lifecycle.task = Some(task);
        }
        drop(provider);
        tokio::time::timeout(Duration::from_secs(2), async {
            assert_eq!(call_rx.recv().await, Some("unregister"));
            unregister_tx.send(Ok(())).unwrap();
            assert_eq!(call_rx.recv().await, Some("shutdown"));
            shutdown_tx.send(Ok(())).unwrap();
            completion.join().await.unwrap();
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
        let mut owned = OwnedAnnouncements::default();
        owned.accept("node._numax._tcp.local.".into(), "127.0.0.1:9000".into());
        let change = DnsNameChange {
            original: "node._numax._tcp.local.".into(),
            new_name: "node (2)._numax._tcp.local.".into(),
            rr_type: RRType::SRV,
            intf_name: "test".into(),
        };

        assert!(owned.name_change(&change).unwrap());
        assert!(owned.matches(&change.original.to_uppercase(), &[]));
        assert!(owned.matches(&change.new_name.to_uppercase(), &[]));
        assert_eq!(owned.keys, BTreeSet::from([change.original.clone()]));
        let mut other_interface = change.clone();
        other_interface.new_name = "node (3)._numax._tcp.local.".into();
        assert!(owned.name_change(&other_interface).unwrap());
        assert!(owned.matches(&change.new_name, &[]));
        assert!(owned.matches(&other_interface.new_name, &[]));
    }

    #[derive(Default)]
    struct FakeRegistrations {
        active: HashMap<String, ServiceInfo>,
        calls: Vec<String>,
        fail_register: bool,
        fail_withdraw: bool,
    }

    struct FakeDaemon {
        state: Arc<StdMutex<FakeRegistrations>>,
        withdrawal: Option<(oneshot::Sender<String>, oneshot::Receiver<()>)>,
        missing_withdraw_acks: bool,
        termination: Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>,
    }

    #[async_trait]
    impl RegistrationDaemon for FakeDaemon {
        fn register(&mut self, service: ServiceInfo) -> Result<(), DiscoveryError> {
            let mut state = self.state.lock().unwrap();
            if state.fail_register {
                return Err(provider_error("injected register failure", true));
            }
            let key = service.get_fullname().to_lowercase();
            state.calls.push(format!("register:{key}"));
            state.active.insert(key, service);
            Ok(())
        }

        async fn withdraw(&mut self, key: &str) -> Result<(), DiscoveryError> {
            if let Some((started, ack)) = self.withdrawal.take() {
                started.send(key.to_string()).unwrap();
                ack.await
                    .map_err(|_| provider_error("injected missing ACK", false))?;
            }
            {
                let mut state = self.state.lock().unwrap();
                state.calls.push(format!("unregister:{key}"));
                if state.fail_withdraw {
                    return Err(provider_error("injected withdrawal failure", false));
                }
            }
            if self.missing_withdraw_acks {
                std::future::pending::<()>().await;
            }
            let mut state = self.state.lock().unwrap();
            // Unlike a wire alias, only the original key removes the record.
            state.active.remove(key);
            Ok(())
        }

        async fn terminate(&mut self) -> Result<(), DiscoveryError> {
            self.state.lock().unwrap().calls.push("shutdown".into());
            if let Some((started, ack)) = self.termination.take() {
                started.send(()).unwrap();
                ack.await
                    .map_err(|_| provider_error("injected missing shutdown ACK", false))?;
            }
            // A confirmed daemon termination retires every registration, even
            // if an individual unregister ACK was lost.
            self.state.lock().unwrap().active.clear();
            Ok(())
        }

        fn fallback(&mut self, keys: &BTreeSet<String>) {
            let mut state = self.state.lock().unwrap();
            for key in keys {
                state.calls.push(format!("fallback:{key}"));
                state.active.remove(key);
            }
        }
    }

    fn fake_cleanup() -> DaemonCleanup<FakeDaemon> {
        DaemonCleanup {
            daemon: FakeDaemon {
                state: Arc::new(StdMutex::new(FakeRegistrations::default())),
                withdrawal: None,
                missing_withdraw_acks: false,
                termination: None,
            },
            owned: OwnedAnnouncements::default(),
            finished: false,
        }
    }

    struct FakeEvents<T> {
        receiver: mpsc::Receiver<(T, oneshot::Sender<()>)>,
        processed: Option<oneshot::Sender<()>>,
    }

    #[async_trait]
    impl<T: Send + 'static> MdnsReceiver<T> for FakeEvents<T> {
        async fn next(&mut self) -> Result<T, DiscoveryError> {
            // The next poll acknowledges that the previous event's handler
            // completed, not merely that its input was dequeued.
            if let Some(processed) = self.processed.take() {
                let _ = processed.send(());
            }
            let (event, processed) = self
                .receiver
                .recv()
                .await
                .ok_or_else(|| provider_error("fake stream closed", false))?;
            self.processed = Some(processed);
            Ok(event)
        }
    }

    async fn deliver<T: Send>(sender: &mpsc::Sender<(T, oneshot::Sender<()>)>, event: T) {
        let (processed, ack) = oneshot::channel();
        assert!(sender.send((event, processed)).await.is_ok());
        tokio::time::timeout(Duration::from_secs(2), ack)
            .await
            .unwrap()
            .unwrap();
    }

    type EventSender<T> = mpsc::Sender<(T, oneshot::Sender<()>)>;

    fn fake_generation(
        provider: &MdnsDiscovery,
        cleanup: DaemonCleanup<FakeDaemon>,
    ) -> (
        MdnsGeneration,
        EventSender<ServiceEvent>,
        EventSender<DaemonEvent>,
    ) {
        let (events, event_rx) = mpsc::channel(8);
        let (monitor, monitor_rx) = mpsc::channel(8);
        let (announcements, announcement_rx) = mpsc::channel(8);
        let (shutdown, shutdown_rx, shutdown_deadline_rx) = shutdown_channels();
        let task = start_mdns_task(
            provider.inner.config.clone(),
            provider.inner.state.clone(),
            provider.inner.own_endpoint.clone(),
            FakeEvents {
                receiver: event_rx,
                processed: None,
            },
            FakeEvents {
                receiver: monitor_rx,
                processed: None,
            },
            cleanup,
            announcement_rx,
            shutdown_rx,
            shutdown_deadline_rx,
        );
        (
            MdnsGeneration {
                task,
                shutdown,
                announcements,
            },
            events,
            monitor,
        )
    }

    async fn assert_mdns_finalization_blocks_restart(panic: bool) {
        let config = MdnsDiscoveryConfig::new("supervised");
        let provider = MdnsDiscovery::new(config.clone()).unwrap();
        let mut cleanup = fake_cleanup();
        replace_announcement(&mut cleanup, &config, "127.0.0.1:9000".into())
            .await
            .unwrap();
        let registrations = cleanup.daemon.state.clone();
        let (entered, termination) = oneshot::channel();
        let (release, released) = oneshot::channel();
        cleanup.daemon.termination = Some((entered, released));
        let (generation, events, _monitor) = fake_generation(&provider, cleanup);
        let old = generation.task.clone();
        provider.ensure_started_with(|| Ok(generation)).unwrap();
        let mut observed = provider.watch().await.unwrap();
        let mut foreign = config.clone();
        foreign.instance_name = "foreign".into();
        let (service, _) =
            build_service(&foreign, &provider.inner.service_type, "127.0.0.2:9000").unwrap();
        let service = service.as_resolved_service();
        deliver(
            &events,
            ServiceEvent::ServiceResolved(Box::new(service.clone())),
        )
        .await;
        assert_eq!(
            super::super::next_changed_peers(&mut observed, &[]).await,
            ["127.0.0.2:9000"]
        );
        assert_eq!(provider.inner.state.snapshot().peers(), ["127.0.0.2:9000"]);
        let event = if panic {
            provider.inner.state.panic_on_next_observation();
            ServiceEvent::ServiceResolved(Box::new(service.clone()))
        } else {
            ServiceEvent::SearchStopped(provider.inner.service_type.clone())
        };
        let (processed, _ack) = oneshot::channel();
        events.send((event, processed)).await.unwrap();
        super::super::dynamic::assert_invalidated(&mut observed).await;
        termination.await.unwrap();
        assert!(provider.inner.state.snapshot().peers().is_empty());
        // Longer than the coordinator's first 500ms retry. A resubscription
        // must keep failing instead of attaching to the dying generation.
        assert!(
            tokio::time::timeout(Duration::from_millis(600), old.clone().join())
                .await
                .is_err()
        );
        assert!(
            provider
                .ensure_started_with(|| panic!("cleanup is still running"))
                .is_err()
        );
        let (first, second) = tokio::join!(provider.watch(), provider.watch());
        assert!(matches!(first, Err(DiscoveryError::WatchClosed)));
        assert!(matches!(second, Err(DiscoveryError::WatchClosed)));
        assert!(!old.completion_ready());
        release.send(()).unwrap();
        let error = old.clone().join().await.unwrap_err();
        if panic {
            assert!(
                matches!(error, DiscoveryError::Provider { message, retryable: false, .. }
                    if message.contains("provider task failed") && message.contains("panic"))
            );
        } else {
            assert!(matches!(
                error,
                DiscoveryError::Provider {
                    retryable: true,
                    ..
                }
            ));
        }
        assert!(registrations.lock().unwrap().active.is_empty());
        assert_eq!(
            registrations.lock().unwrap().calls.last().unwrap(),
            "shutdown"
        );
        let starts = std::sync::atomic::AtomicUsize::new(0);
        let retained = StdMutex::new(Vec::new());
        let start = || {
            // Admission is serialized with shutdown and only follows cleanup.
            starts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            assert!(registrations.lock().unwrap().active.is_empty());
            let (generation, events, monitor) = fake_generation(&provider, fake_cleanup());
            retained.lock().unwrap().push((events, monitor));
            Ok(generation)
        };
        let (first, second) = tokio::join!(async { provider.ensure_started_with(start) }, async {
            provider.ensure_started_with(start)
        },);
        first.unwrap();
        second.unwrap();
        assert_eq!(starts.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(
            !old.same_generation(
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
        let mut fresh = provider.watch().await.unwrap();
        assert!(fresh.snapshot().peers().is_empty());
        let sender = retained.lock().unwrap()[0].0.clone();
        deliver(&sender, ServiceEvent::ServiceResolved(Box::new(service))).await;
        assert_eq!(
            super::super::next_changed_peers(&mut fresh, &[]).await,
            ["127.0.0.2:9000"]
        );
        provider.shutdown().await.unwrap();
        provider.shutdown().await.unwrap();
        assert!(provider.inner.state.snapshot().peers().is_empty());
        assert!(provider.inner.own_endpoint.lock().unwrap().is_none());
        assert!(
            provider
                .ensure_started_with(|| panic!("shutdown is terminal"))
                .is_err()
        );
    }

    #[tokio::test]
    async fn shutdown_after_unexpected_finalization_clears_preserved_announcement() {
        let provider = MdnsDiscovery::new(MdnsDiscoveryConfig::new("late-shutdown")).unwrap();
        let cleanup = fake_cleanup();
        let registrations = cleanup.daemon.state.clone();
        let (generation, events, _monitor) = fake_generation(&provider, cleanup);
        let task = generation.task.clone();
        provider.ensure_started_with(|| Ok(generation)).unwrap();
        provider
            .announce(&PeerAnnouncement {
                endpoint: "127.0.0.1:9000".into(),
            })
            .await
            .unwrap();
        drop(events);
        assert!(task.join().await.is_err());
        assert!(registrations.lock().unwrap().active.is_empty());
        // Unexpected exit preserves the desired endpoint for a possible restart.
        assert!(provider.inner.own_endpoint.lock().unwrap().is_some());
        assert!(provider.shutdown().await.is_err());
        assert!(provider.inner.own_endpoint.lock().unwrap().is_none());
        assert!(provider.shutdown().await.is_err());
        assert!(provider.watch().await.is_err());
    }

    #[tokio::test]
    async fn panic_clears_snapshot_and_delayed_cleanup_blocks_restart_until_one_new_generation() {
        assert_mdns_finalization_blocks_restart(true).await;
    }

    #[tokio::test]
    async fn controlled_browse_error_blocks_restart_until_delayed_cleanup_finishes() {
        assert_mdns_finalization_blocks_restart(false).await;
    }

    #[tokio::test]
    async fn browse_owner_serializes_name_changes_reannouncements_and_shutdown() {
        let config = MdnsDiscoveryConfig::new("actor");
        let cleanup = fake_cleanup();
        let daemon = Arc::clone(&cleanup.daemon.state);
        let state = Arc::new(DynamicState::new(8));
        let endpoint = Arc::new(StdMutex::new(None));
        let (events, event_rx) = mpsc::channel(8);
        let (monitor, monitor_rx) = mpsc::channel(8);
        let (announcements, announcement_rx) = mpsc::channel(8);
        let (stop, stop_rx, stop_deadline_rx) = shutdown_channels();
        let task = start_mdns_task(
            config.clone(),
            Arc::clone(&state),
            Arc::clone(&endpoint),
            FakeEvents {
                receiver: event_rx,
                processed: None,
            },
            FakeEvents {
                receiver: monitor_rx,
                processed: None,
            },
            cleanup,
            announcement_rx,
            stop_rx,
            stop_deadline_rx,
        );
        let (reply, response) = oneshot::channel();
        announcements
            .send(AnnounceRequest {
                endpoint: "127.0.0.1:9000".into(),
                reply,
            })
            .await
            .unwrap();
        response.await.unwrap().unwrap();
        let original = daemon.lock().unwrap().active.keys().next().unwrap().clone();
        let mut alias_config = config.clone();
        alias_config.instance_name = "actor (2)".into();
        let (service, alias) = build_service(
            &alias_config,
            &cluster_service_type(&config.cluster_id),
            "127.0.0.2:9000",
        )
        .unwrap();
        let resolved = service.as_resolved_service();
        // Simulate .local auto-address resolution preceding its monitor event.
        deliver(
            &events,
            ServiceEvent::ServiceResolved(Box::new(resolved.clone())),
        )
        .await;
        assert_eq!(state.snapshot().peers(), ["127.0.0.2:9000"]);
        deliver(
            &monitor,
            DaemonEvent::NameChange(DnsNameChange {
                original: original.clone(),
                new_name: alias.clone(),
                rr_type: RRType::SRV,
                intf_name: "controlled".into(),
            }),
        )
        .await;
        assert!(state.snapshot().peers().is_empty());
        let (reply, response) = oneshot::channel();
        announcements
            .send(AnnounceRequest {
                endpoint: "127.0.0.1:9001".into(),
                reply,
            })
            .await
            .unwrap();
        response.await.unwrap().unwrap();
        assert_eq!(endpoint.lock().unwrap().as_deref(), Some("127.0.0.1:9001"));
        let replacement = daemon.lock().unwrap().active.keys().next().unwrap().clone();
        assert_ne!(original, replacement);
        assert_eq!(daemon.lock().unwrap().active.len(), 1);
        deliver(&events, ServiceEvent::ServiceResolved(Box::new(resolved))).await;
        assert!(state.snapshot().peers().is_empty());
        // Another reannouncement (unchanged endpoint) still retires its key.
        let (reply, response) = oneshot::channel();
        announcements
            .send(AnnounceRequest {
                endpoint: "127.0.0.1:9001".into(),
                reply,
            })
            .await
            .unwrap();
        response.await.unwrap().unwrap();
        assert_eq!(daemon.lock().unwrap().active.len(), 1);
        assert!(!daemon.lock().unwrap().active.contains_key(&replacement));
        request_shutdown(&stop, SHUTDOWN_BUDGET);
        // A request queued concurrently with shutdown must never register.
        let (reply, response) = oneshot::channel();
        announcements
            .send(AnnounceRequest {
                endpoint: "127.0.0.1:9002".into(),
                reply,
            })
            .await
            .unwrap();
        assert!(response.await.unwrap().is_err());
        tokio::time::timeout(SHUTDOWN_BUDGET, task.join())
            .await
            .unwrap()
            .unwrap();
        assert!(endpoint.lock().unwrap().is_none());
        let daemon = daemon.lock().unwrap();
        assert!(daemon.active.is_empty());
        assert!(
            !daemon
                .calls
                .iter()
                .any(|call| call == &format!("unregister:{alias}"))
        );
        assert_eq!(daemon.calls.last().unwrap(), "shutdown");
    }

    fn rename_event(owned: &mut OwnedAnnouncements, original: &str, alias: &str) {
        let event = DaemonEvent::NameChange(DnsNameChange {
            original: original.into(),
            new_name: alias.into(),
            rr_type: RRType::SRV,
            intf_name: "controlled".into(),
        });
        if let DaemonEvent::NameChange(change) = event {
            assert!(owned.name_change(&change).unwrap());
        }
    }

    #[tokio::test]
    async fn renamed_reannouncement_withdraws_original_key_and_filters_late_aliases() {
        let config = MdnsDiscoveryConfig::new("Node");
        let mut cleanup = fake_cleanup();
        let old = "127.0.0.1:9000";
        let new = "127.0.0.1:9001";
        replace_announcement(&mut cleanup, &config, old.into())
            .await
            .unwrap();
        let original = cleanup.owned.current.as_ref().unwrap().key.clone();
        let alias = "Node (2)._numax._tcp.local.";
        rename_event(&mut cleanup.owned, &original, alias);
        let mut instances = HashMap::from([
            (alias.into(), instance(vec![old.into()])),
            ("foreign".into(), instance(vec!["127.0.0.1:9999".into()])),
        ]);
        let mut order = vec![alias.into(), "foreign".into()];
        replace_announcement(&mut cleanup, &config, new.into())
            .await
            .unwrap();
        let replacement = cleanup.owned.current.as_ref().unwrap().key.clone();
        assert_ne!(original, replacement);
        {
            let daemon = cleanup.daemon.state.lock().unwrap();
            assert_eq!(daemon.active.len(), 1);
            assert_eq!(daemon.active[&replacement].get_port(), 9001);
            assert_eq!(
                daemon.calls,
                [
                    format!("register:{original}"),
                    format!("register:{replacement}"),
                    format!("unregister:{original}")
                ]
            );
        }
        // Delayed per-interface renames must still match the retired original.
        rename_event(&mut cleanup.owned, &original, "Node (3)._numax._tcp.local.");
        assert!(cleanup.owned.matches(alias, &[]));
        assert!(cleanup.owned.matches(&original, &[]));
        assert!(cleanup.owned.matches(&replacement, &[]));
        assert!(cleanup.owned.matches("unknown", &[old.into()]));
        assert!(cleanup.owned.matches("unknown", &[new.into()]));
        remove_owned_instances(&cleanup.owned, &mut instances, &mut order);
        assert_eq!(order, ["foreign"]);
        let state = DynamicState::new(8);
        publish_instances(&state, &instances, &order, 8);
        assert_eq!(flatten_instances(&instances, &order, 8), ["127.0.0.1:9999"]);
        shutdown_daemon(&mut cleanup, SHUTDOWN_BUDGET)
            .await
            .unwrap();
        let daemon = cleanup.daemon.state.lock().unwrap();
        assert!(daemon.active.is_empty());
        assert_eq!(
            &daemon.calls[3..],
            [format!("unregister:{replacement}"), "shutdown".into()]
        );
        assert!(cleanup.owned.names.is_empty());
    }

    #[tokio::test]
    async fn failed_registration_preserves_previous_key_endpoint_and_alias() {
        let config = MdnsDiscoveryConfig::new("rollback");
        let mut cleanup = fake_cleanup();
        replace_announcement(&mut cleanup, &config, "127.0.0.1:9000".into())
            .await
            .unwrap();
        let original = cleanup.owned.current.as_ref().unwrap().key.clone();
        let alias = "rollback (2)._numax._tcp.local.";
        rename_event(&mut cleanup.owned, &original, alias);
        cleanup.daemon.state.lock().unwrap().fail_register = true;
        assert!(
            replace_announcement(&mut cleanup, &config, "127.0.0.1:9001".into())
                .await
                .is_err()
        );
        assert_eq!(cleanup.owned.current.as_ref().unwrap().key, original);
        assert_eq!(
            cleanup.owned.current.as_ref().unwrap().endpoint,
            "127.0.0.1:9000"
        );
        assert!(cleanup.owned.matches(alias, &[]));
        assert!(!cleanup.owned.matches("unknown", &["127.0.0.1:9001".into()]));
        assert_eq!(
            cleanup.daemon.state.lock().unwrap().calls,
            [format!("register:{original}")]
        );
        shutdown_daemon(&mut cleanup, SHUTDOWN_BUDGET)
            .await
            .unwrap();
        assert!(cleanup.daemon.state.lock().unwrap().active.is_empty());
    }

    #[tokio::test]
    async fn failed_retirement_retains_both_keys_for_acknowledged_cleanup() {
        let config = MdnsDiscoveryConfig::new("retirement");
        let mut cleanup = fake_cleanup();
        replace_announcement(&mut cleanup, &config, "127.0.0.1:9000".into())
            .await
            .unwrap();
        cleanup.daemon.state.lock().unwrap().fail_withdraw = true;
        assert!(
            replace_announcement(&mut cleanup, &config, "127.0.0.1:9001".into())
                .await
                .is_err()
        );
        assert_eq!(cleanup.owned.keys.len(), 2);
        assert!(
            replace_announcement(&mut cleanup, &config, "127.0.0.1:9002".into())
                .await
                .is_err()
        );
        let keys = cleanup.owned.keys.clone();
        cleanup.daemon.state.lock().unwrap().fail_withdraw = false;
        shutdown_daemon(&mut cleanup, SHUTDOWN_BUDGET)
            .await
            .unwrap();
        let daemon = cleanup.daemon.state.lock().unwrap();
        assert!(daemon.active.is_empty());
        for key in keys {
            assert!(daemon.calls[3..].contains(&format!("unregister:{key}")));
        }
        assert_eq!(daemon.calls.last().unwrap(), "shutdown");
    }

    #[tokio::test]
    async fn cancelled_announcement_waiter_does_not_cancel_retirement_or_shutdown() {
        let config = MdnsDiscoveryConfig::new("cancel");
        let mut cleanup = fake_cleanup();
        replace_announcement(&mut cleanup, &config, "127.0.0.1:9000".into())
            .await
            .unwrap();
        let original = cleanup.owned.current.as_ref().unwrap().key.clone();
        rename_event(
            &mut cleanup.owned,
            &original,
            "cancel (2)._numax._tcp.local.",
        );
        let (started, entered) = oneshot::channel();
        let (ack, release) = oneshot::channel();
        cleanup.daemon.withdrawal = Some((started, release));
        let state = Arc::clone(&cleanup.daemon.state);
        let provider = Arc::new(MdnsDiscovery::new(config.clone()).unwrap());
        let (_events, event_rx) = mpsc::channel(8);
        let (_monitor, monitor_rx) = mpsc::channel(8);
        let (announcements, announcement_rx) = mpsc::channel(8);
        let (stop, stop_rx, stop_deadline_rx) = shutdown_channels();
        {
            let mut lifecycle = provider.inner.lifecycle.lock().unwrap();
            let task = start_mdns_task(
                config,
                Arc::clone(&provider.inner.state),
                Arc::clone(&provider.inner.own_endpoint),
                FakeEvents {
                    receiver: event_rx,
                    processed: None,
                },
                FakeEvents {
                    receiver: monitor_rx,
                    processed: None,
                },
                cleanup,
                announcement_rx,
                stop_rx,
                stop_deadline_rx,
            );
            lifecycle.task = Some(task);
            lifecycle.announcements = Some(announcements);
            lifecycle.shutdown = Some(stop);
        }
        let caller = Arc::clone(&provider);
        let waiter = tokio::spawn(async move {
            caller
                .announce(&PeerAnnouncement {
                    endpoint: "127.0.0.1:9001".into(),
                })
                .await
        });
        assert_eq!(entered.await.unwrap(), original);
        assert_eq!(state.lock().unwrap().active.len(), 2);
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        provider.request_shutdown();
        assert!(
            provider
                .announce(&PeerAnnouncement {
                    endpoint: "127.0.0.1:9002".into()
                })
                .await
                .is_err()
        );
        assert!(
            !provider
                .inner
                .lifecycle
                .lock()
                .unwrap()
                .task
                .as_ref()
                .unwrap()
                .completion_ready()
        );
        ack.send(()).unwrap();
        tokio::time::timeout(SHUTDOWN_BUDGET, provider.shutdown())
            .await
            .unwrap()
            .unwrap();
        let state = state.lock().unwrap();
        assert!(state.active.is_empty());
        assert_eq!(state.calls.last().unwrap(), "shutdown");
    }

    #[tokio::test]
    async fn shutdown_during_replacement_uses_one_deadline_and_cleans_both_keys() {
        let config = MdnsDiscoveryConfig::new("shared-deadline");
        let mut cleanup = fake_cleanup();
        replace_announcement(&mut cleanup, &config, "127.0.0.1:9000".into())
            .await
            .unwrap();
        let original = cleanup.owned.current.as_ref().unwrap().key.clone();
        let (started, entered) = oneshot::channel();
        let (_ack, missing_ack) = oneshot::channel();
        cleanup.daemon.withdrawal = Some((started, missing_ack));
        cleanup.daemon.missing_withdraw_acks = true;
        let registrations = Arc::clone(&cleanup.daemon.state);
        let provider = Arc::new(MdnsDiscovery::new(config).unwrap());
        let (generation, _events, _monitor) = fake_generation(&provider, cleanup);
        let completion = generation.task.clone();
        provider.ensure_started_with(|| Ok(generation)).unwrap();

        let caller = Arc::clone(&provider);
        let announcement = tokio::spawn(async move {
            caller
                .announce(&PeerAnnouncement {
                    endpoint: "127.0.0.1:9001".into(),
                })
                .await
        });
        assert_eq!(entered.await.unwrap(), original);
        assert_eq!(registrations.lock().unwrap().active.len(), 2);

        let budget = Duration::from_millis(120);
        provider.request_shutdown_with_budget(budget);
        let first_deadline = provider
            .inner
            .lifecycle
            .lock()
            .unwrap()
            .shutdown
            .as_ref()
            .and_then(|shutdown| *shutdown.deadline.borrow())
            .unwrap();
        assert_eq!(
            announcement.await.unwrap(),
            Err(provider_error("provider is shut down", false))
        );
        provider.request_shutdown_with_budget(Duration::from_secs(30));
        let repeated_deadline = provider
            .inner
            .lifecycle
            .lock()
            .unwrap()
            .shutdown
            .as_ref()
            .and_then(|shutdown| *shutdown.deadline.borrow())
            .unwrap();
        assert_eq!(repeated_deadline, first_deadline);

        let result = tokio::time::timeout(Duration::from_millis(500), provider.shutdown())
            .await
            .expect("shutdown renewed its deadline");
        assert!(result.is_err());
        assert!(completion.completion_ready());
        let state = registrations.lock().unwrap();
        let unregisters: Vec<_> = state
            .calls
            .iter()
            .filter(|call| call.starts_with("unregister:"))
            .collect();
        assert_eq!(unregisters.len(), 2);
        assert_ne!(unregisters[0], unregisters[1]);
        assert_eq!(state.calls.last().unwrap(), "shutdown");
        assert!(state.active.is_empty());
    }

    #[tokio::test]
    async fn alias_history_is_bounded_and_does_not_discard_owned_names() {
        let mut cleanup = fake_cleanup();
        let config = MdnsDiscoveryConfig::new("bounded");
        replace_announcement(&mut cleanup, &config, "127.0.0.1:9000".into())
            .await
            .unwrap();
        let original = cleanup.owned.current.as_ref().unwrap().key.clone();
        for index in 1..MAX_OWN_HISTORY {
            rename_event(
                &mut cleanup.owned,
                &original,
                &format!("bounded ({index})._numax._tcp.local."),
            );
        }
        assert_eq!(cleanup.owned.names.len(), MAX_OWN_HISTORY);
        assert!(
            cleanup
                .owned
                .name_change(&DnsNameChange {
                    original: original.clone(),
                    new_name: "overflow._numax._tcp.local.".into(),
                    rr_type: RRType::SRV,
                    intf_name: "controlled".into(),
                })
                .is_err()
        );
        assert!(
            replace_announcement(&mut cleanup, &config, "127.0.0.1:9001".into())
                .await
                .is_err()
        );
        assert!(cleanup.owned.matches(&original, &[]));
        assert_eq!(cleanup.owned.keys.len(), 1);
        shutdown_daemon(&mut cleanup, SHUTDOWN_BUDGET)
            .await
            .unwrap();
        assert!(cleanup.daemon.state.lock().unwrap().active.is_empty());
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

        let replacement = "127.0.0.1:43112";
        publisher
            .announce(&PeerAnnouncement {
                endpoint: replacement.into(),
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if super::super::observed_peers(watch.recv().await.unwrap().change)
                    == vec![replacement.to_string()]
                {
                    break;
                }
            }
        })
        .await
        .unwrap();
        assert!(publisher.discover().await.unwrap().peers().is_empty());

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
