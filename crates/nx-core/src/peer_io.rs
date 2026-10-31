//! Internal I/O boundary for the production coordinators and deterministic runner.
//! Decisions remain in membership/sync_manager; adapters supply observations only.
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use nx_net::{ControlReply, ControlRequest, ControlTiming, NetResult, Node, PeerConnectionInfo};
use nx_sync::{HistoryMessage, NodeId, OpId};
use tokio::sync::oneshot;
pub(crate) mod clock;

#[derive(Clone, PartialEq, Eq)]
pub(crate) enum Generation {
    Tcp(nx_net::ConnectionGeneration),
    #[cfg(test)]
    Simulated(Arc<simulation::Lease>),
}
impl Generation {
    pub(crate) fn created_at(&self) -> Instant {
        match self {
            Self::Tcp(value) => value.created_at(),
            #[cfg(test)]
            Self::Simulated(value) => value.created,
        }
    }
    pub(crate) fn is_closed(&self) -> bool {
        match self {
            Self::Tcp(value) => value.is_closed(),
            #[cfg(test)]
            Self::Simulated(value) => value.closed.load(std::sync::atomic::Ordering::Relaxed),
        }
    }
}

#[async_trait]
pub(crate) trait Prepared: Send + Sync {
    fn encoded_len(&self) -> usize;
    fn generation(&self) -> Generation;
    fn peer_id(&self) -> &NodeId;
    async fn send(self: Box<Self>, budget: Duration) -> NetResult<Duration>;
}

struct TcpFrame(Arc<Node>, nx_net::PreparedHistory);
#[async_trait]
impl Prepared for TcpFrame {
    fn encoded_len(&self) -> usize {
        self.1.encoded_len()
    }
    fn generation(&self) -> Generation {
        Generation::Tcp(self.1.generation())
    }
    fn peer_id(&self) -> &NodeId {
        self.1.peer_id()
    }
    async fn send(self: Box<Self>, budget: Duration) -> NetResult<Duration> {
        self.0
            .send_prepared_history_with_timeout(self.1, budget)
            .await
    }
}

#[async_trait]
pub(crate) trait PeerIo: Send + Sync {
    fn now(&self) -> Instant;
    async fn sleep_until(&self, deadline: Instant);
    fn request_id(&self) -> OpId;
    fn max_message_size(&self) -> usize;
    async fn connected_peers(&self) -> Vec<(String, NodeId)>;
    async fn connection_info(&self, addr: &str) -> Option<PeerConnectionInfo>;
    async fn connect_to_peer(&self, addr: &str) -> NetResult<()>;
    async fn disconnect_peer(&self, addr: &str, expected: &NodeId) -> bool;
    async fn prepare_history_to_addr(
        self: Arc<Self>,
        addr: &str,
        message: HistoryMessage,
    ) -> NetResult<Box<dyn Prepared>>;
    async fn control_request_timed_tracked(
        &self,
        addr: &str,
        request: ControlRequest,
        budget: Duration,
        dispatched: Option<oneshot::Sender<()>>,
    ) -> NetResult<(NodeId, ControlReply, ControlTiming)>;
    async fn shutdown(&self);
    async fn refresh_recovery(
        &self,
        metrics: &Arc<crate::observability::RuntimeMetrics>,
        store: Arc<nx_store::Store>,
    ) {
        metrics.refresh_recovery(store).await;
    }
    async fn connected_peer_count(&self) -> usize {
        self.connected_peers().await.len()
    }
    async fn is_connected_addr(&self, addr: &str) -> bool {
        self.connected_peers()
            .await
            .iter()
            .any(|(peer, _)| peer == addr)
    }
    async fn control_request(
        &self,
        addr: &str,
        request: ControlRequest,
        budget: Duration,
    ) -> NetResult<(NodeId, ControlReply)> {
        self.control_request_timed_tracked(addr, request, budget, None)
            .await
            .map(|(id, reply, _)| (id, reply))
    }
    async fn send_prepared_history_with_timeout(
        &self,
        prepared: Box<dyn Prepared>,
        budget: Duration,
    ) -> NetResult<Duration> {
        prepared.send(budget).await
    }
}

#[async_trait]
impl PeerIo for Node {
    fn now(&self) -> Instant {
        clock::Clock::now(&clock::SystemClock)
    }
    async fn sleep_until(&self, deadline: Instant) {
        clock::Clock::sleep_until(&clock::SystemClock, deadline).await;
    }
    fn request_id(&self) -> OpId {
        OpId::generate()
    }
    fn max_message_size(&self) -> usize {
        Node::max_message_size(self)
    }
    async fn connected_peers(&self) -> Vec<(String, NodeId)> {
        let mut peers = Node::connected_peers(self).await;
        // HashMap traversal must not select the first repair/rotation recipient.
        peers.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.as_str().cmp(b.1.as_str())));
        peers
    }
    async fn connection_info(&self, addr: &str) -> Option<PeerConnectionInfo> {
        Node::connection_info(self, addr).await
    }
    async fn connect_to_peer(&self, addr: &str) -> NetResult<()> {
        Node::connect_to_peer(self, addr).await
    }
    async fn disconnect_peer(&self, addr: &str, expected: &NodeId) -> bool {
        Node::disconnect_peer(self, addr, expected).await
    }
    async fn prepare_history_to_addr(
        self: Arc<Self>,
        addr: &str,
        message: HistoryMessage,
    ) -> NetResult<Box<dyn Prepared>> {
        let prepared = Node::prepare_history_to_addr(&self, addr, message).await?;
        Ok(Box::new(TcpFrame(self, prepared)))
    }
    async fn control_request_timed_tracked(
        &self,
        addr: &str,
        request: ControlRequest,
        budget: Duration,
        dispatched: Option<oneshot::Sender<()>>,
    ) -> NetResult<(NodeId, ControlReply, ControlTiming)> {
        Node::control_request_timed_tracked(self, addr, request, budget, dispatched).await
    }
    async fn shutdown(&self) {
        Node::shutdown(self).await;
    }
    async fn connected_peer_count(&self) -> usize {
        Node::connected_peer_count(self).await
    }
    async fn is_connected_addr(&self, addr: &str) -> bool {
        Node::is_connected_addr(self, addr).await
    }
}

#[cfg(test)]
pub(crate) mod simulation;

pub(crate) mod random;
