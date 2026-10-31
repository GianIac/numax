//! Discrete-event message transport. This models message loss, not TCP packet loss.
use super::*;
use std::collections::BTreeMap;
use std::sync::{Mutex, atomic::{AtomicBool, Ordering}};
use nx_net::{NetError, SerializationFormat, WireMessage, WireMessageKind};
use tokio::sync::watch;

pub(crate) struct Lease {
    pub(crate) created: Instant,
    pub(crate) closed: AtomicBool,
}
impl PartialEq for Lease { fn eq(&self, other: &Self) -> bool { std::ptr::eq(self, other) } }
impl Eq for Lease {}

pub(crate) struct Clock {
    time: watch::Sender<Instant>,
}
impl Clock {
    pub(crate) fn new() -> Arc<Self> { Arc::new(Self { time: watch::channel(Instant::now()).0 }) }
    pub(crate) fn now(&self) -> Instant { *self.time.borrow() }
    pub(crate) fn advance_to(&self, now: Instant) {
        assert!(now >= self.now(), "simulation clock regression");
        self.time.send_replace(now);
    }
    async fn sleep_until(&self, deadline: Instant) {
        let mut time = self.time.subscribe();
        let _ = time.wait_for(|now| *now >= deadline).await;
    }
}

pub(crate) struct Delivery {
    pub(crate) from: String,
    pub(crate) to: String,
    pub(crate) message: HistoryMessage,
}
struct State {
    serial: u64,
    queue: BTreeMap<(Instant, String, u64), Delivery>,
    leases: BTreeMap<(String, String), Arc<Lease>>,
    random: super::random::Random,
    lost: usize,
}
pub(crate) struct Network {
    pub(crate) clock: Arc<Clock>,
    state: Mutex<State>,
    pub(crate) format: SerializationFormat,
    pub(crate) latency: Duration,
    pub(crate) loss_percent: u64,
}
impl Network {
    pub(crate) fn new(seed: u64, format: SerializationFormat, loss_percent: u64) -> Arc<Self> {
        assert!(loss_percent <= 100);
        Arc::new(Self { clock: Clock::new(), state: Mutex::new(State {
            serial: 0, queue: BTreeMap::new(), leases: BTreeMap::new(),
            random: super::random::Random::stream(seed, "network", "loss"), lost: 0,
        }), format, latency: Duration::from_millis(5), loss_percent })
    }
    pub(crate) fn endpoint(self: &Arc<Self>, id: &str, seed: u64) -> Arc<dyn PeerIo> {
        Arc::new(Endpoint { network: self.clone(), id: id.into(), ids: Mutex::new(super::random::Random::stream(seed, id, "requests")) })
    }
    pub(crate) fn connect(&self, a: &str, b: &str) {
        let mut state = self.state.lock().unwrap();
        for (a,b) in [(a,b),(b,a)] {
            state.leases.insert((a.into(),b.into()), Arc::new(Lease { created: self.clock.now(), closed: AtomicBool::new(false) }));
        }
    }
    pub(crate) fn pop(&self) -> Option<Delivery> {
        let ((at, _, _), delivery) = self.state.lock().unwrap().queue.pop_first()?;
        self.clock.advance_to(at.max(self.clock.now()));
        Some(delivery)
    }
    pub(crate) fn lost(&self) -> usize { self.state.lock().unwrap().lost }
}
struct Endpoint { network: Arc<Network>, id: String, ids: Mutex<super::random::Random> }
struct Frame { network: Arc<Network>, from: String, to: NodeId, generation: Arc<Lease>, message: HistoryMessage, bytes: usize }
#[async_trait]
impl Prepared for Frame {
    fn encoded_len(&self) -> usize { self.bytes }
    fn generation(&self) -> Generation { Generation::Simulated(self.generation.clone()) }
    fn peer_id(&self) -> &NodeId { &self.to }
    async fn send(self: Box<Self>, _budget: Duration) -> NetResult<Duration> {
        if self.generation.closed.load(Ordering::Relaxed) { return Err(NetError::PeerDisconnected(self.to.to_string())); }
        let mut state = self.network.state.lock().unwrap();
        if state.queue.len() >= 4096 { return Err(NetError::PeerLimitReached(4096)); }
        if state.random.next() % 100 < self.network.loss_percent {
            state.lost += 1;
            return Ok(Duration::ZERO);
        }
        let serial = state.serial;
        state.serial = serial.checked_add(1).ok_or(NetError::PeerLimitReached(4096))?;
        state.queue.insert((self.network.clock.now() + self.network.latency, self.to.to_string(), serial), Delivery { from: self.from, to: self.to.to_string(), message: self.message });
        Ok(Duration::ZERO)
    }
}
#[async_trait]
impl PeerIo for Endpoint {
    fn now(&self) -> Instant { self.network.clock.now() }
    async fn sleep_until(&self, deadline: Instant) { self.network.clock.sleep_until(deadline).await; }
    fn request_id(&self) -> OpId { OpId::new(format!("sim-{:016x}", self.ids.lock().unwrap().next())) }
    fn max_message_size(&self) -> usize { nx_net::DEFAULT_MAX_MESSAGE_SIZE }
    async fn connected_peers(&self) -> Vec<(String, NodeId)> {
        self.network.state.lock().unwrap().leases.iter().filter(|((from,_), lease)| from == &self.id && !lease.closed.load(Ordering::Relaxed)).map(|((_,to),_)| (to.clone(), NodeId::new(to))).collect()
    }
    async fn connection_info(&self, _addr: &str) -> Option<PeerConnectionInfo> { None }
    async fn connect_to_peer(&self, addr: &str) -> NetResult<()> { self.network.connect(&self.id, addr); Ok(()) }
    async fn disconnect_peer(&self, addr: &str, expected: &NodeId) -> bool {
        if addr != expected.as_str() { return false; }
        let state = self.network.state.lock().unwrap();
        if let Some(lease) = state.leases.get(&(self.id.clone(), addr.into())) { lease.closed.store(true, Ordering::Relaxed); return true; }
        false
    }
    async fn prepare_history_to_addr(self: Arc<Self>, addr: &str, message: HistoryMessage) -> NetResult<Box<dyn Prepared>> {
        let generation = self.network.state.lock().unwrap().leases.get(&(self.id.clone(),addr.into())).cloned().filter(|lease| !lease.closed.load(Ordering::Relaxed)).ok_or_else(|| NetError::PeerDisconnected(addr.into()))?;
        let bytes = WireMessage { kind: WireMessageKind::History { message: message.clone() } }.to_bytes_with_format(self.network.format)?.len();
        if bytes.saturating_sub(4) > self.max_message_size() { return Err(NetError::InvalidMessage("simulation frame exceeds limit".into())); }
        Ok(Box::new(Frame { network: self.network.clone(), from: self.id.clone(), to: NodeId::new(addr), generation, message, bytes }))
    }
    async fn control_request_timed_tracked(&self, _addr: &str, _request: ControlRequest, _budget: Duration, _dispatched: Option<oneshot::Sender<()>>) -> NetResult<(NodeId, ControlReply, ControlTiming)> { Err(NetError::Timeout) }
    async fn shutdown(&self) {
        for (addr, id) in self.connected_peers().await { self.disconnect_peer(&addr, &id).await; }
    }
}
