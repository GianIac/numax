//! Versioned message-level transport. It does not model TCP packet retransmission.
use super::clock::Clock as ClockPort;
use super::random::{IdSource, Random, SeededIds};
use super::*;
use nx_net::{ControlIncoming, NetError, NodeEvent, SerializationFormat};
use std::collections::{BTreeMap, BTreeSet};
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll, Wake, Waker};

pub(crate) struct Lease {
    pub(crate) created: Instant,
    pub(crate) closed: AtomicBool,
}
impl PartialEq for Lease {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}
impl Eq for Lease {}

struct Time {
    now: Instant,
    serial: u64,
    timers: BTreeMap<(Instant, u64), Waker>,
}
pub(crate) struct Clock {
    pub(crate) origin: Instant,
    time: Mutex<Time>,
}
impl Clock {
    pub(crate) fn new() -> Arc<Self> {
        let origin = Instant::now();
        Arc::new(Self {
            origin,
            time: Mutex::new(Time {
                now: origin,
                serial: 0,
                timers: BTreeMap::new(),
            }),
        })
    }
    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.time
            .lock()
            .unwrap()
            .timers
            .first_key_value()
            .map(|(key, _)| key.0)
    }
    pub(crate) fn advance_to(&self, now: Instant) {
        let wakers = {
            let mut time = self.time.lock().unwrap();
            assert!(now >= time.now, "simulation clock regression");
            time.now = now;
            let mut wakers = Vec::new();
            while time
                .timers
                .first_key_value()
                .is_some_and(|(key, _)| key.0 <= now)
            {
                wakers.push(time.timers.pop_first().unwrap().1);
            }
            wakers
        };
        for waker in wakers {
            waker.wake();
        }
    }
}
struct Timer<'a> {
    clock: &'a Clock,
    key: (Instant, u64),
}
impl Drop for Timer<'_> {
    fn drop(&mut self) {
        self.clock.time.lock().unwrap().timers.remove(&self.key);
    }
}
#[async_trait]
impl ClockPort for Clock {
    fn now(&self) -> Instant {
        self.time.lock().unwrap().now
    }
    async fn sleep_until(&self, deadline: Instant) {
        let serial = {
            let mut time = self.time.lock().unwrap();
            let serial = time.serial;
            time.serial = serial
                .checked_add(1)
                .expect("simulation timer counter exhausted");
            serial
        };
        let guard = Timer {
            clock: self,
            key: (deadline, serial),
        };
        poll_fn(|cx| {
            let mut time = self.time.lock().unwrap();
            if time.now >= deadline {
                return Poll::Ready(());
            }
            time.timers.insert(guard.key, cx.waker().clone());
            Poll::Pending
        })
        .await;
    }
}

/// Only ready tasks are polled, in stable allocation order. No arbitrary yields.
pub(crate) struct Tasks<T> {
    serial: u64,
    ready: Arc<Mutex<BTreeSet<u64>>>,
    tasks: BTreeMap<u64, Pin<Box<dyn Future<Output = T> + Send>>>,
}
struct TaskWake {
    id: u64,
    ready: Arc<Mutex<BTreeSet<u64>>>,
}
impl Wake for TaskWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.ready.lock().unwrap().insert(self.id);
    }
}
impl<T> Tasks<T> {
    pub(crate) fn new() -> Self {
        Self {
            serial: 0,
            ready: Arc::new(Mutex::new(BTreeSet::new())),
            tasks: BTreeMap::new(),
        }
    }
    pub(crate) fn spawn(&mut self, task: impl Future<Output = T> + Send + 'static) -> u64 {
        assert!(self.tasks.len() < 4096, "simulation task budget exhausted");
        let id = self.serial;
        self.serial = id
            .checked_add(1)
            .expect("simulation task counter exhausted");
        // Tokio's ambient cooperative budget is an executor detail, not simulated
        // scheduling delay. This executor bounds work explicitly instead.
        self.tasks
            .insert(id, Box::pin(tokio::task::coop::unconstrained(task)));
        self.ready.lock().unwrap().insert(id);
        id
    }
    pub(crate) fn poll_ready(&mut self) -> Vec<(u64, T)> {
        let mut completed = Vec::new();
        for _ in 0..65_536 {
            let Some(id) = self.ready.lock().unwrap().pop_first() else {
                return completed;
            };
            let Some(task) = self.tasks.get_mut(&id) else {
                continue;
            };
            let waker = Waker::from(Arc::new(TaskWake {
                id,
                ready: self.ready.clone(),
            }));
            if let Poll::Ready(result) = task.as_mut().poll(&mut Context::from_waker(&waker)) {
                self.tasks.remove(&id);
                completed.push((id, result));
            }
        }
        panic!("simulation microstep budget exhausted");
    }
    pub(crate) fn cancel(&mut self, id: u64) {
        self.tasks.remove(&id);
        self.ready.lock().unwrap().remove(&id);
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
}

pub(crate) enum Incoming {
    Data(NodeEvent),
    Control(ControlIncoming),
}
pub(crate) struct Delivery {
    pub(crate) to: String,
    pub(crate) event: Incoming,
}
struct Queued {
    bytes: usize,
    delivery: Delivery,
    generations: Vec<Arc<Lease>>,
}
struct Connection {
    lease: Arc<Lease>,
    info: PeerConnectionInfo,
}
struct State {
    serial: u64,
    bytes: usize,
    queue: BTreeMap<(Instant, String, String, u64), Queued>,
    connections: BTreeMap<(String, String), Connection>,
    endpoints: BTreeMap<String, NodeId>,
    sessions: BTreeMap<String, Arc<Lease>>,
    control_incoming: BTreeMap<String, Arc<tokio::sync::Semaphore>>,
    random: BTreeMap<(String, String, &'static str), Random>,
    partition: BTreeSet<(String, String)>,
    trace: Vec<String>,
}
pub(crate) struct Network {
    pub(crate) clock: Arc<Clock>,
    state: Mutex<State>,
    seed: u64,
    pub(crate) format: SerializationFormat,
    pub(crate) latency: Duration,
    pub(crate) service: Duration,
    pub(crate) loss_percent: u64,
    pub(crate) max_message_size: usize,
    pub(crate) max_peers: usize,
}
impl Network {
    pub(crate) fn new(seed: u64, format: SerializationFormat, loss_percent: u64) -> Arc<Self> {
        Self::with_timing(
            seed,
            format,
            loss_percent,
            Duration::from_millis(5),
            Duration::from_millis(1),
            8,
        )
    }
    pub(crate) fn with_timing(
        seed: u64,
        format: SerializationFormat,
        loss_percent: u64,
        latency: Duration,
        service: Duration,
        max_peers: usize,
    ) -> Arc<Self> {
        assert!(loss_percent <= 100);
        assert!(max_peers > 0);
        Arc::new(Self {
            clock: Clock::new(),
            seed,
            format,
            loss_percent,
            latency,
            service,
            max_message_size: nx_net::DEFAULT_MAX_MESSAGE_SIZE,
            max_peers,
            state: Mutex::new(State {
                serial: 0,
                bytes: 0,
                queue: BTreeMap::new(),
                connections: BTreeMap::new(),
                endpoints: BTreeMap::new(),
                sessions: BTreeMap::new(),
                control_incoming: BTreeMap::new(),
                random: BTreeMap::new(),
                partition: BTreeSet::new(),
                trace: Vec::new(),
            }),
        })
    }
    pub(crate) fn endpoint(
        self: &Arc<Self>,
        addr: &str,
        id: &str,
        generation: u64,
    ) -> Arc<dyn PeerIo> {
        let session = Arc::new(Lease {
            created: self.clock.now(),
            closed: AtomicBool::new(false),
        });
        let mut state = self.state.lock().unwrap();
        assert!(
            state
                .sessions
                .get(addr)
                .is_none_or(|s| s.closed.load(Ordering::Relaxed)),
            "simulation endpoint restarted without shutdown"
        );
        state.endpoints.insert(addr.into(), NodeId::new(id));
        state.sessions.insert(addr.into(), session.clone());
        state.control_incoming.insert(
            addr.into(),
            Arc::new(tokio::sync::Semaphore::new(nx_net::MAX_CONTROL_REQUESTS)),
        );
        Arc::new(Endpoint {
            network: self.clone(),
            addr: addr.into(),
            session,
            control_outgoing: Arc::new(tokio::sync::Semaphore::new(nx_net::MAX_CONTROL_REQUESTS)),
            ids: SeededIds::new(self.seed, id, generation, "history"),
        })
    }
    fn record(state: &mut State, now: Duration, text: String) {
        assert!(
            state.trace.len() < 65_536,
            "simulation trace budget exhausted"
        );
        state.trace.push(format!("{}:{text}", now.as_nanos()));
    }
    pub(crate) fn trace(&self) -> Vec<String> {
        self.state.lock().unwrap().trace.clone()
    }
    fn dropped(&self, from: &str, to: &str, purpose: &'static str) -> bool {
        let mut state = self.state.lock().unwrap();
        let blocked = state.partition.contains(&(from.into(), to.into()));
        let draw = state
            .random
            .entry((from.into(), to.into(), purpose))
            .or_insert_with(|| Random::stream(self.seed, &format!("{from}>{to}"), purpose))
            .choose(100);
        let lost = blocked || draw < self.loss_percent as usize;
        Self::record(
            &mut state,
            self.clock.now().duration_since(self.clock.origin),
            format!(
                "{purpose}:{from}>{to}:{}",
                if lost { "lost" } else { "sent" }
            ),
        );
        lost
    }
    fn enqueue(
        &self,
        from: &str,
        to: &str,
        bytes: usize,
        event: Incoming,
        delay: Duration,
    ) -> NetResult<()> {
        let mut state = self.state.lock().unwrap();
        let at = self
            .clock
            .now()
            .checked_add(delay)
            .ok_or(NetError::Timeout)?;
        Self::enqueue_locked(&mut state, at, from, to, bytes, event)
    }
    fn enqueue_locked(
        state: &mut State,
        at: Instant,
        from: &str,
        to: &str,
        bytes: usize,
        event: Incoming,
    ) -> NetResult<()> {
        if state.queue.len() >= 4096 || state.bytes.saturating_add(bytes) > 16 * 1024 * 1024 {
            return Err(NetError::PeerLimitReached(4096));
        }
        let serial = state.serial;
        state.serial = serial
            .checked_add(1)
            .ok_or(NetError::PeerLimitReached(4096))?;
        let generations = match &event {
            Incoming::Data(NodeEvent::HistoryReceived { .. }) => state
                .connections
                .get(&(from.into(), to.into()))
                .map(|c| vec![c.lease.clone()])
                .unwrap_or_default(),
            Incoming::Control(_) => [from, to]
                .into_iter()
                .filter_map(|addr| state.sessions.get(addr).cloned())
                .collect(),
            _ => Vec::new(),
        };
        state.bytes += bytes;
        state.queue.insert(
            (at, to.into(), from.into(), serial),
            Queued {
                bytes,
                generations,
                delivery: Delivery {
                    to: to.into(),
                    event,
                },
            },
        );
        Ok(())
    }
    pub(crate) fn next_delivery(&self) -> Option<Instant> {
        self.state
            .lock()
            .unwrap()
            .queue
            .first_key_value()
            .map(|(key, _)| key.0)
    }
    pub(crate) fn pop_ready(&self) -> Option<Delivery> {
        let mut state = self.state.lock().unwrap();
        loop {
            if state
                .queue
                .first_key_value()
                .is_none_or(|(key, _)| key.0 > self.clock.now())
            {
                return None;
            }
            let ((_, to, from, _), queued) = state.queue.pop_first().unwrap();
            state.bytes -= queued.bytes;
            let closed = queued
                .generations
                .iter()
                .any(|g| g.closed.load(Ordering::Relaxed));
            let partitioned = !queued.generations.is_empty()
                && state.partition.contains(&(from.clone(), to.clone()));
            if closed || partitioned {
                Self::record(
                    &mut state,
                    self.clock.now().duration_since(self.clock.origin),
                    format!(
                        "delivery-rejected:{from}>{to}:{}",
                        if closed { "stale" } else { "partition" }
                    ),
                );
                continue;
            }
            return Some(queued.delivery);
        }
    }
    pub(crate) fn partition(&self, a: &str, b: &str, blocked: bool) {
        let mut state = self.state.lock().unwrap();
        for pair in [(a.to_owned(), b.to_owned()), (b.to_owned(), a.to_owned())] {
            if blocked {
                state.partition.insert(pair);
            } else {
                state.partition.remove(&pair);
            }
        }
    }
    fn connect(&self, a: &str, b: &str) -> NetResult<()> {
        let mut state = self.state.lock().unwrap();
        let a_id = state
            .endpoints
            .get(a)
            .cloned()
            .ok_or_else(|| NetError::ConnectionFailed(a.into()))?;
        let b_id = state
            .endpoints
            .get(b)
            .cloned()
            .ok_or_else(|| NetError::ConnectionFailed(b.into()))?;
        if [a, b]
            .iter()
            .any(|addr| state.sessions[*addr].closed.load(Ordering::Relaxed))
        {
            return Err(NetError::PeerDisconnected(b.into()));
        }
        if a == b {
            return Err(NetError::ConnectionFailed("self connection".into()));
        }
        if state
            .connections
            .get(&(a.into(), b.into()))
            .is_some_and(|c| !c.lease.closed.load(Ordering::Relaxed))
        {
            return Ok(());
        }
        if state.partition.contains(&(a.into(), b.into())) {
            return Err(NetError::ConnectionFailed("simulated partition".into()));
        }
        if state.queue.len() > 4094 || state.serial > u64::MAX - 2 {
            return Err(NetError::PeerLimitReached(4096));
        }
        for addr in [a, b] {
            if state
                .connections
                .iter()
                .filter(|((from, _), c)| from == addr && !c.lease.closed.load(Ordering::Relaxed))
                .count()
                >= self.max_peers
            {
                return Err(NetError::PeerLimitReached(self.max_peers));
            }
        }
        for (from, to, id, direction) in [
            (a, b, b_id, nx_net::ConnectionDirection::Outbound),
            (b, a, a_id, nx_net::ConnectionDirection::Inbound),
        ] {
            state.connections.insert(
                (from.into(), to.into()),
                Connection {
                    lease: Arc::new(Lease {
                        created: self.clock.now(),
                        closed: AtomicBool::new(false),
                    }),
                    info: PeerConnectionInfo {
                        transport_addr: to.into(),
                        dialed_endpoint: (direction == nx_net::ConnectionDirection::Outbound)
                            .then(|| to.into()),
                        direction,
                        identity: nx_net::PeerIdentity {
                            node_id: id.clone(),
                            verification: nx_net::PeerIdentityVerification::Unverified,
                        },
                    },
                },
            );
            let peers_connected = state
                .connections
                .iter()
                .filter(|((source, _), c)| {
                    source == from && !c.lease.closed.load(Ordering::Relaxed)
                })
                .count();
            Self::enqueue_locked(
                &mut state,
                self.clock.now(),
                to,
                from,
                0,
                Incoming::Data(NodeEvent::PeerConnected {
                    node_id: id,
                    addr: to.into(),
                    peers_connected,
                }),
            )?;
        }
        Ok(())
    }
}
struct Endpoint {
    network: Arc<Network>,
    addr: String,
    ids: SeededIds,
    session: Arc<Lease>,
    control_outgoing: Arc<tokio::sync::Semaphore>,
}
struct Frame {
    network: Arc<Network>,
    from: String,
    to: String,
    id: NodeId,
    generation: Arc<Lease>,
    frame: Vec<u8>,
}
#[async_trait]
impl Prepared for Frame {
    fn encoded_len(&self) -> usize {
        self.frame.len()
    }
    fn generation(&self) -> Generation {
        Generation::Simulated(self.generation.clone())
    }
    fn peer_id(&self) -> &NodeId {
        &self.id
    }
    async fn send(self: Box<Self>, budget: Duration) -> NetResult<Duration> {
        let started = self.network.clock.now();
        if self.network.service >= budget {
            self.network
                .clock
                .sleep_until(started.checked_add(budget).ok_or(NetError::Timeout)?)
                .await;
            return Err(NetError::Timeout);
        }
        self.network
            .clock
            .sleep_until(started + self.network.service)
            .await;
        if self.generation.closed.load(Ordering::Relaxed) {
            return Err(NetError::PeerDisconnected(self.to));
        }
        if !self.network.dropped(&self.from, &self.to, "history") {
            Network::record(
                &mut self.network.state.lock().unwrap(),
                self.network
                    .clock
                    .now()
                    .duration_since(self.network.clock.origin),
                format!(
                    "frame:{}>{}:{}",
                    self.from,
                    self.to,
                    blake3::hash(&self.frame)
                ),
            );
            let message = nx_net::testing::decode_history(&self.frame)?;
            let from = self.network.state.lock().unwrap().endpoints[&self.from].clone();
            self.network.enqueue(
                &self.from,
                &self.to,
                self.frame.len(),
                Incoming::Data(NodeEvent::HistoryReceived {
                    from,
                    addr: self.from.clone(),
                    message,
                }),
                self.network.latency,
            )?;
        }
        Ok(self.network.clock.now().duration_since(started))
    }
}
#[async_trait]
impl PeerIo for Endpoint {
    fn now(&self) -> Instant {
        self.network.clock.now()
    }
    async fn sleep_until(&self, deadline: Instant) {
        self.network.clock.sleep_until(deadline).await;
    }
    fn request_id(&self) -> OpId {
        self.ids.next()
    }
    fn max_message_size(&self) -> usize {
        self.network.max_message_size
    }
    async fn connected_peers(&self) -> Vec<(String, NodeId)> {
        self.network
            .state
            .lock()
            .unwrap()
            .connections
            .iter()
            .filter(|((from, _), c)| from == &self.addr && !c.lease.closed.load(Ordering::Relaxed))
            .map(|((_, to), c)| (to.clone(), c.info.identity.node_id.clone()))
            .collect()
    }
    async fn connection_info(&self, addr: &str) -> Option<PeerConnectionInfo> {
        self.network
            .state
            .lock()
            .unwrap()
            .connections
            .get(&(self.addr.clone(), addr.into()))
            .filter(|c| !c.lease.closed.load(Ordering::Relaxed))
            .map(|c| c.info.clone())
    }
    async fn connect_to_peer(&self, addr: &str) -> NetResult<()> {
        self.network.connect(&self.addr, addr)
    }
    async fn disconnect_peer(&self, addr: &str, expected: &NodeId) -> bool {
        let mut state = self.network.state.lock().unwrap();
        if state
            .connections
            .get(&(self.addr.clone(), addr.into()))
            .is_none_or(|c| c.info.identity.node_id != *expected)
        {
            return false;
        }
        assert!(
            state.queue.len() <= 4094 && state.serial <= u64::MAX - 2,
            "simulation lifecycle event budget exhausted"
        );
        for key in [
            (self.addr.clone(), addr.into()),
            (addr.into(), self.addr.clone()),
        ] {
            if let Some(c) = state.connections.get(&key) {
                if c.lease.closed.swap(true, Ordering::Relaxed) {
                    continue;
                }
                let node_id = c.info.identity.node_id.clone();
                let peers_connected = state
                    .connections
                    .iter()
                    .filter(|((source, _), c)| {
                        source == &key.0 && !c.lease.closed.load(Ordering::Relaxed)
                    })
                    .count();
                Network::enqueue_locked(
                    &mut state,
                    self.now(),
                    &key.1,
                    &key.0,
                    0,
                    Incoming::Data(NodeEvent::PeerDisconnected {
                        node_id,
                        addr: key.1.clone(),
                        peers_connected,
                    }),
                )
                .expect("checked lifecycle capacity");
            }
        }
        true
    }
    async fn prepare_history_to_addr(
        self: Arc<Self>,
        addr: &str,
        message: HistoryMessage,
    ) -> NetResult<Box<dyn Prepared>> {
        let frame = nx_net::testing::encode_history(message, self.network.format)?;
        if frame.len().saturating_sub(4) > self.max_message_size() {
            return Err(NetError::InvalidMessage(
                "simulation frame exceeds limit".into(),
            ));
        }
        let state = self.network.state.lock().unwrap();
        let c = state
            .connections
            .get(&(self.addr.clone(), addr.into()))
            .filter(|c| !c.lease.closed.load(Ordering::Relaxed))
            .ok_or_else(|| NetError::PeerDisconnected(addr.into()))?;
        Ok(Box::new(Frame {
            network: self.network.clone(),
            from: self.addr.clone(),
            to: addr.into(),
            id: c.info.identity.node_id.clone(),
            generation: c.lease.clone(),
            frame,
        }))
    }
    async fn control_request_timed_tracked(
        &self,
        addr: &str,
        request: ControlRequest,
        budget: Duration,
        dispatched: Option<oneshot::Sender<()>>,
    ) -> NetResult<(NodeId, ControlReply, ControlTiming)> {
        let started_at = self.now();
        let deadline = started_at.checked_add(budget).ok_or(NetError::Timeout)?;
        let (from, peer, session, incoming) = {
            let state = self.network.state.lock().unwrap();
            (
                state.endpoints[&self.addr].clone(),
                state
                    .endpoints
                    .get(addr)
                    .cloned()
                    .ok_or_else(|| NetError::ConnectionFailed(addr.into()))?,
                state.sessions[addr].clone(),
                state.control_incoming[addr].clone(),
            )
        };
        if self.session.closed.load(Ordering::Relaxed) || session.closed.load(Ordering::Relaxed) {
            return Err(NetError::PeerDisconnected(addr.into()));
        }
        let busy = || {
            NetError::Io(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "simulation control capacity exhausted",
            ))
        };
        let _outgoing = self
            .control_outgoing
            .clone()
            .try_acquire_owned()
            .map_err(|_| busy())?;
        let _incoming = incoming.try_acquire_owned().map_err(|_| busy())?;
        let request_id = request.request_id;
        let (bytes, request) =
            nx_net::testing::control_request(from.clone(), request, self.network.format)?;
        let (respond_to, received) = oneshot::channel();
        if self.network.service >= budget {
            self.sleep_until(deadline).await;
            return Err(NetError::Timeout);
        }
        self.sleep_until(started_at + self.network.service).await;
        if self.session.closed.load(Ordering::Relaxed) || session.closed.load(Ordering::Relaxed) {
            return Err(NetError::PeerDisconnected(addr.into()));
        }
        if !self.network.dropped(&self.addr, addr, "control-request") {
            self.network.enqueue(
                &self.addr,
                addr,
                bytes,
                Incoming::Control(ControlIncoming {
                    from,
                    verification: nx_net::PeerIdentityVerification::Unverified,
                    request,
                    respond_to,
                }),
                self.network.latency,
            )?;
        }
        if let Some(dispatched) = dispatched {
            let _ = dispatched.send(());
        }
        let reply = tokio::select! {
            biased;
            _ = self.sleep_until(deadline) => None,
            reply = received => reply.ok(),
        };
        let Some(reply) = reply else {
            self.sleep_until(deadline).await;
            return Err(NetError::Timeout);
        };
        let (_, reply) =
            nx_net::testing::control_reply(peer.clone(), request_id, reply, self.network.format)?;
        if self.network.dropped(addr, &self.addr, "control-reply") {
            self.sleep_until(deadline).await;
            return Err(NetError::Timeout);
        }
        self.sleep_until((self.now() + self.network.latency).min(deadline))
            .await;
        if self.now() >= deadline {
            return Err(NetError::Timeout);
        }
        let partitioned = self
            .network
            .state
            .lock()
            .unwrap()
            .partition
            .contains(&(addr.into(), self.addr.clone()));
        if partitioned {
            self.sleep_until(deadline).await;
            return Err(NetError::Timeout);
        }
        if self.session.closed.load(Ordering::Relaxed) || session.closed.load(Ordering::Relaxed) {
            return Err(NetError::PeerDisconnected(addr.into()));
        }
        let observed_at = self.now();
        Ok((
            peer,
            reply,
            ControlTiming {
                started_at,
                observed_at,
                round_trip: observed_at.duration_since(started_at),
            },
        ))
    }
    async fn shutdown(&self) {
        for (addr, id) in self.connected_peers().await {
            self.disconnect_peer(&addr, &id).await;
        }
        self.session.closed.store(true, Ordering::Relaxed);
    }
    async fn refresh_recovery(
        &self,
        metrics: &Arc<crate::observability::RuntimeMetrics>,
        store: Arc<nx_store::Store>,
    ) {
        match crate::sync_manager::storage_progress(&store) {
            Ok(progress) => metrics.observe_recovery(progress),
            Err(error) => {
                metrics.record_sync_error();
                tracing::error!(%error, "simulation recovery observation failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn timers_cancel_and_zero_latency_tasks_quiesce() {
        let clock = Clock::new();
        let mut tasks = Tasks::new();
        let c = clock.clone();
        let id = tasks.spawn(async move {
            c.sleep_until(c.now() + Duration::from_secs(1)).await;
            7
        });
        assert!(tasks.poll_ready().is_empty());
        assert!(clock.next_deadline().is_some());
        tasks.cancel(id);
        assert!(clock.next_deadline().is_none());
        tasks.spawn(async { 9 });
        assert_eq!(tasks.poll_ready().len(), 1);
        assert!(tasks.is_empty());
    }

    #[tokio::test]
    async fn writer_deadlines_partitions_and_stale_generations() {
        for format in [SerializationFormat::Json, SerializationFormat::Bincode] {
            let network = Network::new(42, format, 0);
            let a = network.endpoint("a:1", "a", 1);
            let b = network.endpoint("b:1", "b", 1);
            a.connect_to_peer("b:1").await.unwrap();
            assert!(matches!(
                network.pop_ready().unwrap().event,
                Incoming::Data(NodeEvent::PeerConnected { .. })
            ));
            assert!(matches!(
                network.pop_ready().unwrap().event,
                Incoming::Data(NodeEvent::PeerConnected { .. })
            ));
            let mut tasks = Tasks::new();
            for nanos in [999_999, 1_000_000, 1_000_001] {
                let frame = a
                    .clone()
                    .prepare_history_to_addr(
                        "b:1",
                        HistoryMessage::SummaryRequest { after_origin: None },
                    )
                    .await
                    .unwrap();
                let started = a.now();
                tasks.spawn(async move { frame.send(Duration::from_nanos(nanos)).await });
                assert!(tasks.poll_ready().is_empty());
                network
                    .clock
                    .advance_to(started + Duration::from_nanos(nanos.min(1_000_000)));
                let result = tasks.poll_ready().pop().unwrap().1;
                assert_eq!(result.is_ok(), nanos > 1_000_000);
            }
            // The successful writer queued a frame, but the link partitions
            // before delivery. It must not mutate the receiving replica.
            network.partition("a:1", "b:1", true);
            network.clock.advance_to(network.next_delivery().unwrap());
            assert!(network.pop_ready().is_none());
            network.partition("a:1", "b:1", false);
            let frame = a
                .clone()
                .prepare_history_to_addr(
                    "b:1",
                    HistoryMessage::SummaryRequest { after_origin: None },
                )
                .await
                .unwrap();
            let generation = frame.generation();
            a.disconnect_peer("b:1", &NodeId::new("b")).await;
            assert!(generation.is_closed());
            a.connect_to_peer("b:1").await.unwrap();
            tasks.spawn(async move { frame.send(Duration::from_secs(1)).await });
            tasks.poll_ready();
            network
                .clock
                .advance_to(network.clock.next_deadline().unwrap());
            assert!(matches!(
                tasks.poll_ready().pop().unwrap().1,
                Err(NetError::PeerDisconnected(_))
            ));
            a.shutdown().await;
            b.shutdown().await;
            assert!(tasks.is_empty());
            assert!(network.clock.next_deadline().is_none());
        }
    }

    #[tokio::test]
    async fn zero_latency_control_deadlines_and_cancellation_release_capacity() {
        use nx_net::{ControlKind, MembershipRecord, MembershipState, MembershipVersion};
        let owner = |id: &str| MembershipRecord {
            member: id.into(),
            reporter: id.into(),
            endpoint: None,
            state: MembershipState::Alive,
            version: MembershipVersion {
                epoch: 1,
                incarnation: 0,
            },
        };
        for format in [SerializationFormat::Json, SerializationFormat::Bincode] {
            for delay in [999, 1000, 1001] {
                let network =
                    Network::with_timing(42, format, 0, Duration::ZERO, Duration::ZERO, 2);
                let a = network.endpoint("a:1", "a", 1);
                let b = network.endpoint("b:1", "b", 1);
                let request = ControlRequest {
                    request_id: 1,
                    owner: owner("a"),
                    updates: Vec::new(),
                    kind: ControlKind::Exchange { after: None },
                };
                let mut tasks = Tasks::new();
                let io = a.clone();
                tasks.spawn(async move {
                    io.control_request("b:1", request, Duration::from_micros(1000))
                        .await
                });
                assert!(tasks.poll_ready().is_empty());
                let Incoming::Control(incoming) = network.pop_ready().unwrap().event else {
                    panic!("expected control");
                };
                network
                    .clock
                    .advance_to(network.clock.origin + Duration::from_micros(delay));
                incoming
                    .respond_to
                    .send(ControlReply {
                        request_id: 1,
                        owner: owner("b"),
                        ack: true,
                        nack: false,
                        records: Vec::new(),
                        next: None,
                    })
                    .unwrap();
                assert_eq!(tasks.poll_ready().pop().unwrap().1.is_ok(), delay < 1000);
                a.shutdown().await;
                b.shutdown().await;
                assert!(network.clock.next_deadline().is_none());
            }
            let network = Network::with_timing(42, format, 100, Duration::ZERO, Duration::ZERO, 2);
            let a = network.endpoint("a:1", "a", 1);
            let _b = network.endpoint("b:1", "b", 1);
            let mut tasks = Tasks::new();
            let mut ids = Vec::new();
            for request_id in 1..=nx_net::MAX_CONTROL_REQUESTS + 1 {
                let request = ControlRequest {
                    request_id: request_id as u64,
                    owner: owner("a"),
                    updates: Vec::new(),
                    kind: ControlKind::Exchange { after: None },
                };
                let io = a.clone();
                ids.push(tasks.spawn(async move {
                    io.control_request("b:1", request, Duration::from_secs(1))
                        .await
                }));
            }
            let results = tasks.poll_ready();
            assert_eq!(results.len(), 1);
            assert!(
                matches!(&results[0].1, Err(NetError::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock)
            );
            for id in ids {
                tasks.cancel(id);
            }
            assert!(tasks.is_empty());
            assert!(network.clock.next_deadline().is_none());
            // Cancellation must return both the incoming and outgoing permits.
            let io = a.clone();
            tasks.spawn(async move {
                io.control_request(
                    "b:1",
                    ControlRequest {
                        request_id: 99,
                        owner: owner("a"),
                        updates: Vec::new(),
                        kind: ControlKind::Exchange { after: None },
                    },
                    Duration::from_secs(1),
                )
                .await
            });
            assert!(tasks.poll_ready().is_empty());
            drop(tasks);
            assert!(network.clock.next_deadline().is_none());
        }
    }
}
