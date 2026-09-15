use std::sync::{Mutex, MutexGuard};

use tokio::sync::broadcast;
use tokio::task::{JoinError, JoinHandle};

use super::{DiscoveryChange, DiscoveryEvent, DiscoverySnapshot, DiscoveryWatch};

/// Aborts a detached Tokio task if the shutdown future owning it is cancelled.
pub(crate) struct AbortOnDropTask(Option<JoinHandle<()>>);

impl AbortOnDropTask {
    pub(crate) fn new(task: JoinHandle<()>) -> Self {
        Self(Some(task))
    }

    pub(crate) async fn join(mut self) -> Result<(), JoinError> {
        let Some(task) = self.0.as_mut() else {
            return Ok(());
        };
        let result = task.await;
        self.0.take();
        result
    }
}

impl Drop for AbortOnDropTask {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

/// Shared, bounded state for providers whose complete view changes over time.
pub(super) struct DynamicState {
    inner: Mutex<State>,
    event_capacity: usize,
}

struct State {
    revision: u64,
    peers: Vec<String>,
    observations: Vec<std::time::Instant>,
    events: broadcast::Sender<DiscoveryEvent>,
}

impl DynamicState {
    pub(super) fn new(event_capacity: usize) -> Self {
        let (events, _) = broadcast::channel(event_capacity.max(1));
        Self {
            inner: Mutex::new(State {
                revision: 0,
                peers: Vec::new(),
                observations: Vec::new(),
                events,
            }),
            event_capacity: event_capacity.max(1),
        }
    }

    pub(super) fn snapshot(&self) -> DiscoverySnapshot {
        let state = self.lock();
        state.snapshot()
    }

    pub(super) fn watch(&self) -> DiscoveryWatch {
        // Subscription and snapshot are captured while producers are excluded,
        // so a transition cannot fall into a snapshot/watch gap.
        let state = self.lock();
        let receiver = state.events.subscribe();
        DiscoveryWatch::new(state.snapshot(), receiver)
    }

    /// Replace the complete view as one revision so consumers never observe a
    /// transient partial diff or lose a pure ordering change.
    pub(super) fn replace(&self, peers: Vec<String>) {
        let mut state = self.lock();
        if state.peers == peers {
            return;
        }
        let observations = peers
            .iter()
            .map(|peer| {
                state
                    .peers
                    .iter()
                    .position(|old| old == peer)
                    .map(|index| state.observations[index])
                    .unwrap_or_else(std::time::Instant::now)
            })
            .collect();
        self.publish(&mut state, peers, observations);
    }

    /// Successful observation of the entire view, including an identical view.
    pub(super) fn observe(&self, peers: Vec<String>) {
        let now = std::time::Instant::now();
        self.observe_at(peers.into_iter().map(|peer| (peer, now)).collect());
    }

    /// Aggregate views preserve each endpoint's latest successful observation.
    pub(super) fn observe_at(&self, peers: Vec<(String, std::time::Instant)>) {
        let (peers, observations) = peers.into_iter().unzip();
        let mut state = self.lock();
        if state.peers == peers && state.observations == observations {
            return;
        }
        self.publish(&mut state, peers, observations);
    }

    fn publish(
        &self,
        state: &mut State,
        peers: Vec<String>,
        observations: Vec<std::time::Instant>,
    ) {
        let Some(revision) = state.revision.checked_add(1) else {
            tracing::error!("discovery revision space exhausted; rejecting provider update");
            return;
        };
        state.peers = peers;
        state.observations = observations;
        state.revision = revision;
        let event = DiscoveryEvent {
            revision: state.revision,
            change: DiscoveryChange::Observed(state.snapshot()),
        };
        let _ = state.events.send(event);
    }

    /// Close current subscriptions while preserving the latest snapshot for a
    /// fresh watch after a provider-level restart.
    pub(super) fn invalidate_watches(&self) {
        let mut state = self.lock();
        let (events, _) = broadcast::channel(self.event_capacity);
        state.events = events;
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl State {
    fn snapshot(&self) -> DiscoverySnapshot {
        DiscoverySnapshot::observed(
            self.revision,
            self.peers
                .iter()
                .cloned()
                .zip(self.observations.iter().copied())
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::DiscoveryError;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use std::time::Duration;

    #[tokio::test]
    async fn identical_fresh_observations_advance_but_cached_republication_does_not() {
        let state = DynamicState::new(2);
        let now = std::time::Instant::now();
        state.observe_at(vec![("a:1".into(), now)]);
        let mut watch = state.watch();
        let snapshot = watch.snapshot().clone();
        state.replace(vec!["a:1".into()]);
        assert_eq!(state.snapshot(), snapshot);
        state.observe_at(vec![("a:1".into(), now + Duration::from_secs(1))]);
        let event = watch.recv().await.unwrap();
        assert_eq!(event.revision, snapshot.revision() + 1);
        let DiscoveryChange::Observed(ref fresh) = event.change else {
            panic!("missing observation");
        };
        assert_eq!(fresh.peers(), snapshot.peers());
        assert_ne!(fresh.observations(), snapshot.observations());
        assert_eq!(fresh, state.watch().snapshot());
        for offset in 2..8 {
            state.observe_at(vec![("a:1".into(), now + Duration::from_secs(offset))]);
        }
        assert!(matches!(
            watch.recv().await,
            Err(DiscoveryError::WatchOverflow { .. })
        ));
        assert_eq!(state.snapshot(), *state.watch().snapshot());
    }

    #[tokio::test]
    async fn replacement_has_a_contiguous_watch_stream() {
        let state = DynamicState::new(8);
        state.replace(vec!["a:1".into()]);
        let mut watch = state.watch();
        state.replace(vec!["b:2".into(), "c:3".into()]);

        let event = watch.recv().await.unwrap();
        assert_eq!(event.revision, 2);
        assert_eq!(super::super::observed_peers(event.change), ["b:2", "c:3"]);
        assert_eq!(state.snapshot().peers(), ["b:2", "c:3"]);
    }

    #[tokio::test]
    async fn invalidation_closes_existing_watches_and_preserves_the_snapshot() {
        let state = DynamicState::new(8);
        state.replace(vec!["a:1".into()]);
        let mut old_watch = state.watch();

        state.invalidate_watches();

        assert_eq!(
            old_watch.recv().await.unwrap_err(),
            DiscoveryError::WatchClosed
        );
        let fresh_watch = state.watch();
        assert_eq!(fresh_watch.snapshot().revision(), 1);
        assert_eq!(fresh_watch.snapshot().peers(), ["a:1"]);
    }

    #[tokio::test]
    async fn dropping_an_owned_shutdown_handle_aborts_the_task() {
        struct Dropped(Arc<AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let task_dropped = Arc::clone(&dropped);
        let task = tokio::spawn(async move {
            let _guard = Dropped(task_dropped);
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        });
        started_rx.await.unwrap();

        drop(AbortOnDropTask::new(task));
        tokio::time::timeout(Duration::from_secs(1), async {
            while !dropped.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
