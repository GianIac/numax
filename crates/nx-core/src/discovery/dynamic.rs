use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::sync::{broadcast, watch};
use tokio::task::{JoinError, JoinHandle};
use tokio::time::Instant;

use super::{
    DiscoveryChange, DiscoveryError, DiscoveryEvent, DiscoverySnapshot, DiscoveryWatch,
    MAX_DISCOVERY_EVENT_CAPACITY,
};

pub(super) fn checked_deadline(
    now: Instant,
    duration: Duration,
    provider: &str,
    field: &str,
) -> Result<Instant, DiscoveryError> {
    now.checked_add(duration)
        .ok_or_else(|| DiscoveryError::Provider {
            provider: provider.into(),
            message: format!("{field} deadline is not representable"),
            retryable: false,
        })
}

pub(super) fn validate_durations(
    provider: &str,
    durations: &[(&str, Duration)],
) -> Result<(), DiscoveryError> {
    let now = Instant::now();
    for (field, duration) in durations {
        if now.checked_add(*duration).is_none() {
            return Err(DiscoveryError::InvalidConfiguration {
                provider: provider.into(),
                message: format!("{field} deadline is not representable"),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn deadline_boundary() -> Instant {
    let now = Instant::now();
    // Find the platform's actual boundary, rather than inventing a cap.
    let (mut low, mut high) = (0, u64::MAX);
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if now.checked_add(Duration::from_secs(middle)).is_some() {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    now.checked_add(Duration::from_secs(low)).unwrap()
}

/// One owned generation. Completion is published only after the worker was
/// joined and external cleanup finished; JoinHandle::is_finished is not a
/// restart barrier. Dropping a caller never takes ownership of this sequence.
/// The provider signals stop on Drop; the supervisor then completes its bounded
/// cleanup even if no waiter remains. Runtime teardown aborts its child task.
#[derive(Clone)]
pub(super) struct ProviderTask {
    provider: &'static str,
    _supervisor: Arc<JoinHandle<()>>,
    completion: watch::Receiver<Option<TaskCompletion>>,
}

#[derive(Clone)]
struct TaskCompletion {
    result: Result<(), DiscoveryError>,
    restart: Result<(), DiscoveryError>,
}

impl ProviderTask {
    pub(super) fn spawn<F, C>(
        provider: &'static str,
        state: Arc<DynamicState>,
        shutdown: watch::Receiver<bool>,
        worker: F,
        cleanup: impl FnOnce() -> C + Send + 'static,
    ) -> Self
    where
        F: Future<Output = Result<(), DiscoveryError>> + Send + 'static,
        C: Future<Output = Result<(), DiscoveryError>> + Send + 'static,
    {
        let (complete, completion) = watch::channel(None);
        state.activate();
        // Construct guards before spawning, including for runtime teardown.
        let mut final_state = FinalizeState { state, armed: true };
        let supervisor = tokio::spawn(async move {
            let mut worker = AbortOnDropJoin(tokio::spawn(worker));
            // Workers select on shutdown at blocking operations. Let them
            // finish bounded transactions (notably mDNS retirement) normally.
            let joined = (&mut worker.0).await;
            let requested = *shutdown.borrow() || shutdown.has_changed().is_err();
            let (result, restart) = match joined {
                Ok(Ok(())) => (Ok(()), Ok(())),
                Ok(Err(error)) => (Err(error.clone()), Err(error)),
                Err(error) => {
                    let error = DiscoveryError::Provider {
                        provider: provider.into(),
                        message: format!("provider task failed: {error}"),
                        retryable: false,
                    };
                    // A panic is recoverable by a new generation, but is still
                    // reported by shutdown rather than silently discarded.
                    // Requested stop is cooperative, not an abort: a cancelled
                    // worker's JoinError must also remain observable.
                    (Err(error), Ok(()))
                }
            };
            // A stop request racing a failure does not make that failure an
            // orderly exit. Existing subscribers must still be invalidated.
            final_state.state.finish(!requested || result.is_err());
            // Cleanup is fallible too: catch its panic via JoinError without
            // losing the completion signal or admitting an unsafe restart.
            let mut cleanup = AbortOnDropJoin(tokio::spawn(async move { cleanup().await }));
            let cleaned = (&mut cleanup.0).await.unwrap_or_else(|error| {
                Err(DiscoveryError::Provider {
                    provider: provider.into(),
                    message: format!("provider cleanup task failed: {error}"),
                    retryable: false,
                })
            });
            if let Err(error) = &cleaned {
                tracing::warn!(%error, provider, "discovery cleanup failed");
                final_state.state.finish(true);
            }
            // Retryability of the worker and proof of resource retirement are
            // independent. Even a retryable cleanup error cannot authorize a
            // replacement that may overlap the previous external resources.
            let restart = cleaned
                .clone()
                .map_err(|error| DiscoveryError::Provider {
                    provider: provider.into(),
                    message: format!(
                        "provider cleanup was not confirmed; restart blocked: {error}"
                    ),
                    retryable: false,
                })
                .and(restart);
            let result = result.and(cleaned);
            if let Err(error) = &result {
                tracing::warn!(%error, provider, "discovery generation ended");
            }
            // Disarm before publishing completion: no old-generation state
            // mutation is permitted after a replacement is admitted.
            final_state.armed = false;
            drop(final_state);
            complete.send_replace(Some(TaskCompletion { result, restart }));
        });
        Self {
            provider,
            _supervisor: Arc::new(supervisor),
            completion,
        }
    }

    /// true means the current generation is live. During cleanup callers must
    /// retry, not subscribe to a watch whose producer has already exited.
    pub(super) fn running(
        &self,
        provider: &str,
        state: &DynamicState,
    ) -> Result<bool, DiscoveryError> {
        // Keep the read guard through the closed-channel check. Otherwise a
        // completion published between these reads could be mistaken for a
        // supervisor failure merely because its sender has already dropped.
        let completion = self.completion.borrow();
        if let Some(done) = completion.as_ref() {
            if let Err(error) = &done.restart
                && !matches!(
                    error,
                    DiscoveryError::Provider {
                        retryable: true,
                        ..
                    }
                )
            {
                return Err(error.clone());
            }
            return Ok(false);
        }
        if self.completion.has_changed().is_err() {
            return Err(DiscoveryError::Provider {
                provider: provider.into(),
                message: "generation supervisor failed".into(),
                retryable: false,
            });
        }
        state.check_available()?;
        Ok(true)
    }

    pub(super) async fn join(mut self) -> Result<(), DiscoveryError> {
        loop {
            if let Some(done) = self.completion.borrow_and_update().clone() {
                return done.result;
            }
            self.completion
                .changed()
                .await
                .map_err(|_| DiscoveryError::Provider {
                    provider: self.provider.into(),
                    message: "generation supervisor failed".into(),
                    retryable: false,
                })?;
        }
    }

    #[cfg(test)]
    pub(super) fn same_generation(&self, other: &Self) -> bool {
        self.completion.same_channel(&other.completion)
    }

    #[cfg(test)]
    pub(super) fn completion_ready(&self) -> bool {
        self.completion.borrow().is_some()
    }
}

struct AbortOnDropJoin<T>(JoinHandle<T>);
impl<T> Drop for AbortOnDropJoin<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct FinalizeState {
    state: Arc<DynamicState>,
    armed: bool,
}
impl Drop for FinalizeState {
    fn drop(&mut self) {
        if self.armed {
            self.state.finish(true);
        }
    }
}

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
    #[cfg(test)]
    panic_next_observation: std::sync::atomic::AtomicBool,
}

struct State {
    available: bool,
    revision: u64,
    peers: Vec<String>,
    observations: Vec<std::time::Instant>,
    events: broadcast::Sender<DiscoveryEvent>,
}

impl DynamicState {
    pub(super) fn new(event_capacity: usize) -> Self {
        // Public provider constructors reject out-of-range capacities. Clamp
        // defensively for internal callers, including later channel rotations,
        // so unchecked values cannot trigger oversized allocation or overflow.
        let event_capacity = event_capacity.clamp(1, MAX_DISCOVERY_EVENT_CAPACITY);
        let (events, _) = broadcast::channel(event_capacity);
        Self {
            inner: Mutex::new(State {
                available: true,
                revision: 0,
                peers: Vec::new(),
                observations: Vec::new(),
                events,
            }),
            event_capacity,
            #[cfg(test)]
            panic_next_observation: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub(super) fn snapshot(&self) -> DiscoverySnapshot {
        let state = self.lock();
        state.snapshot()
    }

    #[cfg(test)]
    pub(super) fn watch(&self) -> DiscoveryWatch {
        // Subscription and snapshot are captured while producers are excluded,
        // so a transition cannot fall into a snapshot/watch gap.
        let state = self.lock();
        let receiver = state.events.subscribe();
        DiscoveryWatch::new(state.snapshot(), receiver)
    }

    pub(super) fn live_watch(&self) -> Result<DiscoveryWatch, DiscoveryError> {
        let state = self.lock();
        if !state.available {
            return Err(DiscoveryError::WatchClosed);
        }
        Ok(DiscoveryWatch::new(
            state.snapshot(),
            state.events.subscribe(),
        ))
    }

    fn check_available(&self) -> Result<(), DiscoveryError> {
        if self.lock().available {
            Ok(())
        } else {
            Err(DiscoveryError::WatchClosed)
        }
    }

    fn activate(&self) {
        self.lock().available = true;
    }

    fn finish(&self, unexpected: bool) {
        let mut state = self.lock();
        state.available = false;
        if !state.peers.is_empty() {
            self.publish(&mut state, Vec::new(), Vec::new());
        }
        if unexpected {
            let (events, _) = broadcast::channel(self.event_capacity);
            state.events = events;
        }
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
        #[cfg(test)]
        assert!(
            !self
                .panic_next_observation
                .swap(false, std::sync::atomic::Ordering::SeqCst),
            "injected provider observation panic"
        );
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
    #[cfg(test)]
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

    #[cfg(test)]
    pub(super) fn panic_on_next_observation(&self) {
        self.panic_next_observation
            .store(true, std::sync::atomic::Ordering::SeqCst);
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
pub(super) async fn assert_invalidated(watch: &mut DiscoveryWatch) {
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut cleared = false;
        loop {
            match watch.recv().await {
                Ok(event) => {
                    if super::observed_peers(event.change).is_empty() {
                        cleared = true;
                    }
                }
                Err(DiscoveryError::WatchClosed) => {
                    assert!(cleared);
                    break;
                }
                other => panic!("unexpected watch result: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
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
    async fn internal_event_capacity_clamp_survives_channel_rotation() {
        for (capacity, normalized) in [
            (0, 1),
            (1, 1),
            (MAX_DISCOVERY_EVENT_CAPACITY, MAX_DISCOVERY_EVENT_CAPACITY),
            (
                MAX_DISCOVERY_EVENT_CAPACITY + 1,
                MAX_DISCOVERY_EVENT_CAPACITY,
            ),
            (usize::MAX, MAX_DISCOVERY_EVENT_CAPACITY),
        ] {
            let state = DynamicState::new(capacity);
            assert_eq!(state.event_capacity, normalized);
            for _ in 0..2 {
                let mut events = state.watch();
                for revision in 0..=normalized {
                    state.replace(vec![format!("peer-{revision}:9000")]);
                }
                assert_eq!(
                    events.recv().await,
                    Err(DiscoveryError::WatchOverflow { missed: 1 })
                );
                state.invalidate_watches();
            }
        }
    }

    #[test]
    fn representable_duration_can_overflow_only_after_the_clock_advances() {
        let last = deadline_boundary();
        let one_second = Duration::from_secs(1);
        assert!(validate_durations("test", &[("delay", one_second)]).is_ok());
        assert!(checked_deadline(last - one_second, one_second, "test", "delay").is_ok());
        assert!(matches!(
            checked_deadline(last, one_second, "test", "delay"),
            Err(DiscoveryError::Provider {
                retryable: false,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn dropping_a_shutdown_waiter_does_not_cancel_owned_cleanup() {
        let state = Arc::new(DynamicState::new(8));
        state.observe(vec!["cached:9000".into()]);
        let (stop, stop_rx) = watch::channel(true);
        let (release, released) = tokio::sync::oneshot::channel::<()>();
        let owner = ProviderTask::spawn(
            "test",
            state.clone(),
            stop_rx,
            async { Ok(()) },
            move || async move {
                released.await.unwrap();
                Ok(())
            },
        );
        let mut waiter = Box::pin(owner.clone().join());
        std::future::poll_fn(|cx| {
            assert!(waiter.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        drop(waiter);
        release.send(()).unwrap();
        owner.join().await.unwrap();
        assert!(state.snapshot().peers().is_empty());
        drop(stop);
    }

    #[tokio::test]
    async fn requested_stop_does_not_hide_worker_errors_or_panics_from_watches() {
        for panic in [false, true] {
            let state = Arc::new(DynamicState::new(8));
            state.observe(vec!["cached:9000".into()]);
            let mut events = state.watch();
            let (_stop, stop_rx) = watch::channel(true);
            let task = ProviderTask::spawn(
                "test",
                state.clone(),
                stop_rx,
                async move {
                    assert!(!panic, "injected panic during requested shutdown");
                    Err(DiscoveryError::Provider {
                        provider: "test".into(),
                        message: "worker failed during requested shutdown".into(),
                        retryable: true,
                    })
                },
                || async { Ok(()) },
            );
            let error = task.join().await.unwrap_err();
            if panic {
                assert!(matches!(error, DiscoveryError::Provider { message, .. }
                    if message.contains("provider task failed") && message.contains("panic")));
            }
            assert!(state.snapshot().peers().is_empty());
            assert_invalidated(&mut events).await;
        }
    }

    #[tokio::test]
    async fn failed_cleanup_is_a_terminal_restart_barrier_even_for_retryable_errors() {
        for requested in [false, true] {
            let state = Arc::new(DynamicState::new(8));
            state.observe(vec!["cached:9000".into()]);
            let mut events = state.watch();
            let (_stop, stop_rx) = watch::channel(requested);
            let error = DiscoveryError::Provider {
                provider: "test".into(),
                message: "external cleanup not confirmed".into(),
                retryable: true,
            };
            let cleanup_error = error.clone();
            let task = ProviderTask::spawn(
                "test",
                state.clone(),
                stop_rx,
                async { Ok(()) },
                move || async move { Err(cleanup_error) },
            );
            assert_eq!(task.clone().join().await, Err(error));
            assert!(matches!(
                task.running("test", &state),
                Err(DiscoveryError::Provider {
                    retryable: false,
                    ..
                })
            ));
            assert!(state.live_watch().is_err());
            assert_invalidated(&mut events).await;
        }
    }

    #[tokio::test]
    async fn successful_requested_stop_clears_without_invalidating_existing_watch() {
        let state = Arc::new(DynamicState::new(8));
        state.observe(vec!["cached:9000".into()]);
        let mut events = state.watch();
        let (_stop, stop_rx) = watch::channel(true);
        let task =
            ProviderTask::spawn("test", state.clone(), stop_rx, async { Ok(()) }, || async {
                Ok(())
            });
        task.join().await.unwrap();
        assert!(super::super::observed_peers(events.recv().await.unwrap().change).is_empty());
        assert!(state.snapshot().peers().is_empty());
        assert!(state.live_watch().is_err());
        let mut next = Box::pin(events.recv());
        std::future::poll_fn(|cx| {
            assert!(next.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
    }

    #[tokio::test]
    async fn cleanup_panic_completes_with_join_error_and_prevents_restart() {
        let state = Arc::new(DynamicState::new(8));
        state.observe(vec!["cached:9000".into()]);
        let mut events = state.watch();
        let (_stop, stop_rx) = watch::channel(false);
        let owner =
            ProviderTask::spawn("test", state.clone(), stop_rx, async { Ok(()) }, || async {
                panic!("injected cleanup panic");
            });
        assert_invalidated(&mut events).await;
        let result = tokio::time::timeout(Duration::from_secs(1), owner.clone().join())
            .await
            .unwrap();
        assert!(
            matches!(result, Err(DiscoveryError::Provider { message, retryable: false, .. })
            if message.contains("cleanup task failed") && message.contains("panic"))
        );
        assert!(owner.running("test", &state).is_err());
        assert!(state.live_watch().is_err());
        assert!(state.snapshot().peers().is_empty());
    }

    #[tokio::test]
    async fn completion_not_joinhandle_finished_is_the_restart_barrier() {
        let state = Arc::new(DynamicState::new(8));
        state.observe(vec!["cached:9000".into()]);
        let mut events = state.watch();
        let (_stop, stop_rx) = watch::channel(false);
        let (release, released) = tokio::sync::oneshot::channel();
        let owner = ProviderTask::spawn(
            "test",
            state.clone(),
            stop_rx,
            async {
                panic!("worker panic");
            },
            move || async move {
                released.await.unwrap();
                Ok(())
            },
        );
        assert_invalidated(&mut events).await;
        assert!(owner.running("test", &state).is_err());
        assert!(state.live_watch().is_err());
        release.send(()).unwrap();
        assert!(owner.clone().join().await.is_err());
        // Pretend the supervisor has not returned from its final poll yet:
        // completion is sufficient because it cannot mutate state afterwards.
        assert!(!owner.running("test", &state).unwrap());
        state.activate();
        state.observe(vec!["replacement:9000".into()]);
        tokio::task::yield_now().await;
        assert_eq!(state.snapshot().peers(), ["replacement:9000"]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn completion_publication_and_sender_drop_do_not_report_supervisor_failure() {
        tokio::time::timeout(Duration::from_secs(3), async {
            for _ in 0..128 {
                let state = Arc::new(DynamicState::new(8));
                let (_stop, stop_rx) = watch::channel(false);
                let owner = ProviderTask::spawn(
                    "test",
                    state.clone(),
                    stop_rx,
                    async { Ok(()) },
                    || async { Ok(()) },
                );
                loop {
                    match owner.running("test", &state) {
                        Ok(false) => break,
                        Ok(true) | Err(DiscoveryError::WatchClosed) => {
                            tokio::task::yield_now().await
                        }
                        other => panic!("completion race reported a failure: {other:?}"),
                    }
                }
                owner.join().await.unwrap();
            }
        })
        .await
        .unwrap();
    }

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
