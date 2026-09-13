use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::broadcast;

mod bootstrap_gossip;
mod dns_srv;
mod dynamic;
mod file_watch;
mod mdns;

pub use bootstrap_gossip::{BootstrapGossipDiscovery, BootstrapGossipDiscoveryConfig};
pub use dns_srv::{DnsSrvDiscovery, DnsSrvDiscoveryConfig};
pub(crate) use dynamic::AbortOnDropTask;
pub use file_watch::{FileWatchDiscovery, FileWatchDiscoveryConfig};
pub use mdns::{MdnsDiscovery, MdnsDiscoveryConfig};

/// Default number of discovery events retained for each provider watch channel.
pub const DEFAULT_DISCOVERY_EVENT_CAPACITY: usize = 128;

/// Default maximum number of peer candidates retained by the coordinator.
pub const DEFAULT_MAX_PEER_CANDIDATES: usize = 1024;

/// Default logical cluster used when no explicit discovery scope is supplied.
pub const DEFAULT_DISCOVERY_CLUSTER: &str = "default";

/// Whether a provider can publish the local advertised endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnouncementSupport {
    Unsupported,
    Optional,
    Required,
}

/// Runtime policy shared by all discovery sources for one node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveryRuntimeConfig {
    cluster_id: String,
    advertised_endpoint: Option<String>,
    max_candidates: usize,
}

impl Default for DiscoveryRuntimeConfig {
    fn default() -> Self {
        Self {
            cluster_id: DEFAULT_DISCOVERY_CLUSTER.to_string(),
            advertised_endpoint: None,
            max_candidates: DEFAULT_MAX_PEER_CANDIDATES,
        }
    }
}

impl DiscoveryRuntimeConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_cluster_id(mut self, cluster_id: impl Into<String>) -> Self {
        self.cluster_id = cluster_id.into();
        self
    }

    pub fn with_advertised_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.advertised_endpoint = Some(endpoint.into());
        self
    }

    pub fn with_max_candidates(mut self, max_candidates: usize) -> Self {
        self.max_candidates = max_candidates;
        self
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub fn advertised_endpoint(&self) -> Option<&str> {
        self.advertised_endpoint.as_deref()
    }

    pub fn max_candidates(&self) -> usize {
        self.max_candidates
    }
}

/// A named provider contribution and its optional candidate lease duration.
#[derive(Clone)]
pub struct DiscoveryProvider {
    source_id: String,
    provider: Arc<dyn PeerDiscovery>,
    candidate_ttl: Option<Duration>,
}

impl fmt::Debug for DiscoveryProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DiscoveryProvider")
            .field("source_id", &self.source_id)
            .field("cluster_id", &self.provider.cluster_id())
            .field("candidate_ttl", &self.candidate_ttl)
            .finish_non_exhaustive()
    }
}

impl DiscoveryProvider {
    pub fn new(source_id: impl Into<String>, provider: Arc<dyn PeerDiscovery>) -> Self {
        Self {
            source_id: source_id.into(),
            provider,
            candidate_ttl: None,
        }
    }

    pub fn with_candidate_ttl(mut self, candidate_ttl: Duration) -> Self {
        self.candidate_ttl = Some(candidate_ttl);
        self
    }

    pub fn source_id(&self) -> &str {
        &self.source_id
    }

    pub fn provider(&self) -> &Arc<dyn PeerDiscovery> {
        &self.provider
    }

    pub fn candidate_ttl(&self) -> Option<Duration> {
        self.candidate_ttl
    }
}

/// A complete provider view at one logical revision.
///
/// Revisions are contiguous within a watch. A snapshot returned by
/// [`PeerDiscovery::watch`] is atomic with the event subscription: its first
/// event has revision `snapshot.revision() + 1`, and every later event advances
/// it by one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoverySnapshot {
    revision: u64,
    peers: Vec<String>,
}

impl DiscoverySnapshot {
    pub fn new(revision: u64, peers: Vec<String>) -> Self {
        Self { revision, peers }
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn peers(&self) -> &[String] {
        &self.peers
    }

    pub fn into_peers(self) -> Vec<String> {
        self.peers
    }
}

/// A change occurring after the snapshot associated with a watch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveryEvent {
    pub revision: u64,
    pub change: DiscoveryChange,
}

/// A change to the provider's peer candidates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscoveryChange {
    Added(String),
    Removed(String),
    /// Atomically replace the provider's complete ordered contribution.
    Replaced(Vec<String>),
}

/// The endpoint a provider is asked to announce.
///
/// An announcement advertises only a connection candidate. It does not assert
/// peer identity, cluster membership, or authorization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerAnnouncement {
    pub endpoint: String,
}

/// Errors exposed by discovery providers and watch delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscoveryError {
    InvalidConfiguration {
        provider: String,
        message: String,
    },
    Provider {
        provider: String,
        message: String,
        retryable: bool,
    },
    Unsupported {
        provider: String,
        operation: &'static str,
    },
    WatchOverflow {
        missed: u64,
    },
    WatchRevision {
        previous: u64,
        received: u64,
    },
    WatchInvalidated,
    WatchClosed,
}

impl fmt::Display for DiscoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration { provider, message } => {
                write!(
                    formatter,
                    "{provider} discovery configuration is invalid: {message}"
                )
            }
            Self::Provider {
                provider,
                message,
                retryable,
            } => write!(
                formatter,
                "{provider} discovery provider failed (retryable: {retryable}): {message}"
            ),
            Self::Unsupported {
                provider,
                operation,
            } => write!(
                formatter,
                "{provider} discovery provider does not support {operation}"
            ),
            Self::WatchOverflow { missed } => {
                write!(formatter, "discovery watch missed {missed} event(s)")
            }
            Self::WatchRevision { previous, received } => write!(
                formatter,
                "discovery watch received revision {received} after revision {previous}"
            ),
            Self::WatchInvalidated => {
                formatter.write_str("discovery watch is invalidated; create a fresh watch")
            }
            Self::WatchClosed => formatter.write_str("discovery watch closed"),
        }
    }
}

impl Error for DiscoveryError {}

/// An atomic snapshot and its bounded stream of subsequent changes.
///
/// A lagging consumer receives [`DiscoveryError::WatchOverflow`] rather than a
/// silently incomplete view and must create a fresh watch. Dropping this value
/// cancels the subscription synchronously; it never owns a background task.
#[derive(Debug)]
pub struct DiscoveryWatch {
    snapshot: DiscoverySnapshot,
    events: broadcast::Receiver<DiscoveryEvent>,
    last_revision: u64,
    invalidated: bool,
}

impl DiscoveryWatch {
    /// Build a watch from an atomically captured snapshot and bounded receiver.
    ///
    /// Provider implementations must create the receiver and snapshot under
    /// the same state synchronization boundary, subscribing first, so no
    /// transition can occur between them unnoticed.
    pub fn new(snapshot: DiscoverySnapshot, events: broadcast::Receiver<DiscoveryEvent>) -> Self {
        let last_revision = snapshot.revision();
        Self {
            snapshot,
            events,
            last_revision,
            invalidated: false,
        }
    }

    pub fn snapshot(&self) -> &DiscoverySnapshot {
        &self.snapshot
    }

    pub async fn recv(&mut self) -> Result<DiscoveryEvent, DiscoveryError> {
        if self.invalidated {
            return Err(DiscoveryError::WatchInvalidated);
        }

        match self.events.recv().await {
            Ok(event) => {
                if self.last_revision.checked_add(1) != Some(event.revision) {
                    self.invalidated = true;
                    return Err(DiscoveryError::WatchRevision {
                        previous: self.last_revision,
                        received: event.revision,
                    });
                }
                self.last_revision = event.revision;
                Ok(event)
            }
            Err(broadcast::error::RecvError::Lagged(missed)) => {
                self.invalidated = true;
                Err(DiscoveryError::WatchOverflow { missed })
            }
            Err(broadcast::error::RecvError::Closed) => Err(DiscoveryError::WatchClosed),
        }
    }
}

/// Source of peer connection candidates.
///
/// Discovery never authorizes a candidate. Every resulting connection still
/// passes through the existing transport limits, TLS/mTLS checks, allowlists,
/// and wire handshake.
#[async_trait]
pub trait PeerDiscovery: Send + Sync {
    /// Logical cluster whose candidates and announcements this provider serves.
    fn cluster_id(&self) -> &str {
        DEFAULT_DISCOVERY_CLUSTER
    }

    /// Declare whether startup must publish an advertised endpoint.
    fn announcement_support(&self) -> AnnouncementSupport {
        AnnouncementSupport::Unsupported
    }

    /// Return the provider's complete view at one logical revision.
    async fn discover(&self) -> Result<DiscoverySnapshot, DiscoveryError>;

    /// Advertise a local endpoint, or return [`DiscoveryError::Unsupported`].
    async fn announce(&self, announcement: &PeerAnnouncement) -> Result<(), DiscoveryError>;

    /// Atomically subscribe to changes and return the snapshot they follow.
    ///
    /// Delivery must be bounded. Providers must make overflow observable and
    /// must not silently discard events. Dropping the returned watch cancels
    /// that subscription.
    async fn watch(&self) -> Result<DiscoveryWatch, DiscoveryError>;

    /// Stop provider-owned work and withdraw announcements made by this node.
    ///
    /// This hook is synchronous so an owner can initiate cancellation before
    /// awaiting unrelated shutdown work. Implementations with background work
    /// must make repeated calls safe and return promptly.
    fn request_shutdown(&self) {}

    /// Wait for provider-owned work to stop and complete bounded withdrawal.
    async fn shutdown(&self) -> Result<(), DiscoveryError> {
        self.request_shutdown();
        Ok(())
    }
}

/// Backward-compatible discovery provider for explicitly configured peers.
///
/// Static discovery intentionally preserves input order and duplicates. Peer
/// candidate deduplication and connection admission remain responsibilities of
/// the coordinator and networking path.
#[derive(Debug)]
pub struct StaticDiscovery {
    peers: Vec<String>,
    event_tx: broadcast::Sender<DiscoveryEvent>,
}

impl StaticDiscovery {
    pub fn new(peers: Vec<String>) -> Self {
        Self::with_event_capacity(peers, DEFAULT_DISCOVERY_EVENT_CAPACITY)
    }

    pub fn with_event_capacity(peers: Vec<String>, event_capacity: usize) -> Self {
        let (event_tx, _) = broadcast::channel(event_capacity.max(1));
        Self { peers, event_tx }
    }

    fn snapshot(&self) -> DiscoverySnapshot {
        DiscoverySnapshot::new(0, self.peers.clone())
    }
}

#[async_trait]
impl PeerDiscovery for StaticDiscovery {
    async fn discover(&self) -> Result<DiscoverySnapshot, DiscoveryError> {
        Ok(self.snapshot())
    }

    async fn announce(&self, _announcement: &PeerAnnouncement) -> Result<(), DiscoveryError> {
        Err(DiscoveryError::Unsupported {
            provider: "static".to_string(),
            operation: "announcement",
        })
    }

    async fn watch(&self) -> Result<DiscoveryWatch, DiscoveryError> {
        // Subscribe before taking the snapshot. StaticDiscovery is immutable,
        // while dynamic providers must use the same ordering under their state
        // synchronization boundary to preserve this no-gap contract.
        let events = self.event_tx.subscribe();
        Ok(DiscoveryWatch::new(self.snapshot(), events))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[tokio::test]
    async fn static_snapshot_preserves_order_and_duplicates() {
        let discovery = StaticDiscovery::new(vec![
            "peer-b:9000".to_string(),
            "peer-a:9000".to_string(),
            "peer-b:9000".to_string(),
        ]);

        let snapshot = discovery.discover().await.unwrap();

        assert_eq!(snapshot.revision(), 0);
        assert_eq!(
            snapshot.peers(),
            ["peer-b:9000", "peer-a:9000", "peer-b:9000"]
        );
    }

    #[tokio::test]
    async fn static_watch_snapshot_matches_discover_without_a_gap() {
        let discovery = StaticDiscovery::new(vec!["peer-a:9000".to_string()]);

        let discovered = discovery.discover().await.unwrap();
        let watch = discovery.watch().await.unwrap();

        assert_eq!(watch.snapshot(), &discovered);
    }

    #[tokio::test]
    async fn static_announcement_is_explicitly_unsupported() {
        let discovery = StaticDiscovery::new(Vec::new());
        let announcement = PeerAnnouncement {
            endpoint: "127.0.0.1:9000".to_string(),
        };

        let error = discovery.announce(&announcement).await.unwrap_err();

        assert_eq!(
            error,
            DiscoveryError::Unsupported {
                provider: "static".to_string(),
                operation: "announcement",
            }
        );
    }

    #[tokio::test]
    async fn lagging_watch_reports_overflow() {
        let (event_tx, events) = broadcast::channel(1);
        let mut watch = DiscoveryWatch::new(DiscoverySnapshot::new(0, Vec::new()), events);
        let first = DiscoveryEvent {
            revision: 1,
            change: DiscoveryChange::Added("peer-a:9000".to_string()),
        };
        let second = DiscoveryEvent {
            revision: 2,
            change: DiscoveryChange::Added("peer-b:9000".to_string()),
        };
        event_tx.send(first).unwrap();
        event_tx.send(second).unwrap();

        assert_eq!(
            watch.recv().await.unwrap_err(),
            DiscoveryError::WatchOverflow { missed: 1 }
        );
        assert_eq!(
            watch.recv().await.unwrap_err(),
            DiscoveryError::WatchInvalidated
        );
    }

    #[tokio::test]
    async fn watch_accepts_contiguous_revisions() {
        let (event_tx, events) = broadcast::channel(1);
        let mut watch = DiscoveryWatch::new(DiscoverySnapshot::new(4, Vec::new()), events);
        let expected = DiscoveryEvent {
            revision: 5,
            change: DiscoveryChange::Added("peer-a:9000".to_string()),
        };
        event_tx.send(expected.clone()).unwrap();

        assert_eq!(watch.recv().await.unwrap(), expected);
    }

    #[tokio::test]
    async fn watch_rejects_non_contiguous_revisions() {
        let (event_tx, events) = broadcast::channel(1);
        let mut watch = DiscoveryWatch::new(DiscoverySnapshot::new(4, Vec::new()), events);
        event_tx
            .send(DiscoveryEvent {
                revision: 6,
                change: DiscoveryChange::Added("peer-a:9000".to_string()),
            })
            .unwrap();

        assert_eq!(
            watch.recv().await.unwrap_err(),
            DiscoveryError::WatchRevision {
                previous: 4,
                received: 6,
            }
        );
        assert_eq!(
            watch.recv().await.unwrap_err(),
            DiscoveryError::WatchInvalidated
        );
    }

    #[tokio::test]
    async fn dropping_watch_cancels_subscription_without_a_task() {
        let discovery = StaticDiscovery::new(Vec::new());
        let watch = discovery.watch().await.unwrap();
        assert_eq!(discovery.event_tx.receiver_count(), 1);

        drop(watch);

        assert_eq!(discovery.event_tx.receiver_count(), 0);
    }

    #[tokio::test]
    async fn watch_reports_provider_closure() {
        let (event_tx, events) = broadcast::channel(1);
        let mut watch = DiscoveryWatch::new(DiscoverySnapshot::new(0, Vec::new()), events);
        drop(event_tx);

        assert_eq!(watch.recv().await.unwrap_err(), DiscoveryError::WatchClosed);
    }

    #[test]
    fn peer_discovery_is_object_safe() {
        let discovery: Arc<dyn PeerDiscovery> = Arc::new(StaticDiscovery::new(Vec::new()));
        assert_eq!(Arc::strong_count(&discovery), 1);
    }
}
