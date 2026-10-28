//! Volatile delivery attempts. Items have already been persisted before enqueue.
//! Byte admission never changes acceptance or durable replay/baseline retention.
use std::io::{self, Write};
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use crate::observability::RuntimeMetrics;

pub(super) const QUEUE_BYTES: usize = 8 * 1024 * 1024;
pub(super) const TURN_BYTES: usize = 16 * 1024 * 1024;
pub(super) const TURN_MESSAGES: usize = 64;
pub(super) const SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(250);

pub(crate) trait Item {
    fn delivery_bytes(&self) -> Option<usize>;
}

#[derive(Default)]
pub(super) struct ByteCount(pub(super) usize);
impl Write for ByteCount {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("delivery size overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) fn entry_bytes(op: &nx_sync::Op, position: &nx_sync::OpPosition) -> Option<usize> {
    let mut count = ByteCount::default();
    serde_json::to_writer(&mut count, op).ok()?;
    serde_json::to_writer(&mut count, position).ok()?;
    count.0.checked_add(32)
}

struct Queued<T> {
    item: T,
    _bytes: OwnedSemaphorePermit,
}

struct Budget {
    bytes: Arc<Semaphore>,
    metrics: Arc<RuntimeMetrics>,
}
impl Budget {
    fn admit<T: Item>(&self, item: T) -> Option<Queued<T>> {
        let permit = item
            .delivery_bytes()
            .and_then(|size| u32::try_from(size.max(1)).ok())
            .and_then(|size| Arc::clone(&self.bytes).try_acquire_many_owned(size).ok());
        match permit {
            Some(permit) => Some(Queued {
                item,
                _bytes: permit,
            }),
            None => {
                self.metrics.record_sync_error();
                tracing::debug!(
                    "delivery byte budget exhausted; durable repair retains accepted operation"
                );
                None
            }
        }
    }
}

pub(crate) struct Sender<T> {
    tx: mpsc::Sender<Queued<T>>,
    budget: Arc<Budget>,
}
impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            budget: Arc::clone(&self.budget),
        }
    }
}
pub(crate) struct Receiver<T>(mpsc::Receiver<Queued<T>>);
pub(crate) struct Permit<'a, T> {
    permit: mpsc::Permit<'a, Queued<T>>,
    budget: &'a Budget,
}
pub(crate) struct OwnedPermit<T> {
    permit: mpsc::OwnedPermit<Queued<T>>,
    budget: Arc<Budget>,
}

pub(super) fn channel<T>(
    slots: usize,
    bytes: usize,
    metrics: Arc<RuntimeMetrics>,
) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = mpsc::channel(slots.max(1));
    (
        Sender {
            tx,
            budget: Arc::new(Budget {
                bytes: Arc::new(Semaphore::new(bytes)),
                metrics,
            }),
        },
        Receiver(rx),
    )
}
impl<T: Item> Sender<T> {
    pub(crate) fn try_reserve(&self) -> Result<Permit<'_, T>, mpsc::error::TrySendError<()>> {
        Ok(Permit {
            permit: self.tx.try_reserve()?,
            budget: &self.budget,
        })
    }
    pub(crate) async fn reserve_owned(self) -> Result<OwnedPermit<T>, mpsc::error::SendError<()>> {
        Ok(OwnedPermit {
            permit: self.tx.reserve_owned().await?,
            budget: self.budget,
        })
    }
    pub(super) fn try_send(&self, item: T) -> Result<(), mpsc::error::TrySendError<()>> {
        self.try_reserve()?.send(item);
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn capacity(&self) -> usize {
        self.tx.capacity()
    }
    #[cfg(test)]
    pub(crate) fn max_capacity(&self) -> usize {
        self.tx.max_capacity()
    }
}
impl<T: Item> Permit<'_, T> {
    pub(crate) fn send(self, item: T) {
        if let Some(item) = self.budget.admit(item) {
            self.permit.send(item);
        }
    }
}
impl<T: Item> OwnedPermit<T> {
    pub(crate) fn send(self, item: T) {
        if let Some(item) = self.budget.admit(item) {
            self.permit.send(item);
        }
    }
}
impl<T> Receiver<T> {
    pub(crate) async fn recv(&mut self) -> Option<T> {
        self.0.recv().await.map(|queued| queued.item)
    }
    pub(crate) fn try_recv(&mut self) -> Result<T, mpsc::error::TryRecvError> {
        self.0.try_recv().map(|queued| queued.item)
    }
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }
    pub(crate) fn max_capacity(&self) -> usize {
        self.0.max_capacity()
    }
}

/// One turn has a strict wire-byte and message budget. Recovery has a separate
/// turn so saturating gossip cannot spend the repair allowance.
pub(super) struct Turn {
    bytes: usize,
    messages: usize,
}
impl Turn {
    pub(super) fn new() -> Self {
        Self {
            bytes: TURN_BYTES,
            messages: TURN_MESSAGES,
        }
    }
    pub(super) fn admit(&mut self, bytes: usize) -> bool {
        if bytes > self.bytes || self.messages == 0 {
            return false;
        }
        self.bytes -= bytes;
        self.messages -= 1;
        true
    }
}

/// Seedable jitter used by production and tests. No ambient clock or RNG calls.
pub(super) struct Jitter(u64);
impl Jitter {
    pub(super) fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }
    pub(super) fn delay(&mut self, ceiling: std::time::Duration) -> std::time::Duration {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        let bound = ceiling.as_nanos().min(u128::from(u64::MAX - 1)) as u64;
        std::time::Duration::from_nanos(self.0 % (bound + 1))
    }
}

/// Independent node-wide recovery allowance; missed windows never accumulate.
pub(super) struct RepairBudget {
    start: std::time::Instant,
    turn: Turn,
    work: usize,
}
impl RepairBudget {
    pub(super) fn new(now: std::time::Instant) -> Self {
        Self {
            start: now,
            turn: Turn::new(),
            work: TURN_MESSAGES,
        }
    }
    fn refresh(&mut self, now: std::time::Instant) {
        if now.saturating_duration_since(self.start) >= std::time::Duration::from_secs(1) {
            self.start = now;
            self.turn = Turn::new();
            self.work = TURN_MESSAGES;
        }
    }
    pub(super) fn admit_work(&mut self, now: std::time::Instant) -> bool {
        self.refresh(now);
        if self.work == 0 {
            return false;
        }
        self.work -= 1;
        true
    }
    pub(super) fn admit(&mut self, bytes: usize, now: std::time::Instant) -> bool {
        self.refresh(now);
        self.turn.admit(bytes)
    }
}

pub(super) fn seed(identity: &str) -> u64 {
    let digest = blake3::hash(identity.as_bytes());
    let mut seed = [0; 8];
    seed.copy_from_slice(&digest.as_bytes()[..8]);
    u64::from_le_bytes(seed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    struct Payload(usize);
    impl Item for Payload {
        fn delivery_bytes(&self) -> Option<usize> {
            Some(self.0)
        }
    }
    fn metrics() -> Arc<RuntimeMetrics> {
        Arc::new(RuntimeMetrics::default())
    }

    #[tokio::test]
    async fn byte_capacity_returns_on_receive_drop_and_cancelled_reservations() {
        let (tx, mut rx) = channel(4, 10, metrics());
        tx.try_send(Payload(6)).unwrap();
        // Slots remain, but another six bytes cannot fit. Only this attempt drops.
        tx.try_send(Payload(6)).unwrap();
        assert_eq!(rx.len(), 1);
        assert_eq!(tx.budget.bytes.available_permits(), 4);
        assert_eq!(rx.recv().await.unwrap().0, 6);
        assert_eq!(tx.budget.bytes.available_permits(), 10);
        let reserved = tx.clone().reserve_owned().await.unwrap();
        drop(reserved);
        assert_eq!(tx.capacity(), 4);
        tx.try_send(Payload(10)).unwrap();
        drop(rx);
        assert_eq!(tx.budget.bytes.available_permits(), 10);
        assert!(tx.try_reserve().is_err());
    }

    #[tokio::test]
    async fn oversize_attempt_never_waits_for_impossible_capacity() {
        let (tx, mut rx) = channel(2, 10, metrics());
        tx.clone().reserve_owned().await.unwrap().send(Payload(11));
        assert!(rx.try_recv().is_err());
        assert_eq!(tx.capacity(), 2);
        tx.try_send(Payload(10)).unwrap();
        assert_eq!(rx.recv().await.unwrap().0, 10);
    }

    #[test]
    fn jitter_is_reproducible_bounded_and_seed_sensitive() {
        let ceiling = Duration::from_millis(20);
        let mut a = Jitter::new(7);
        let mut b = Jitter::new(7);
        let mut other = Jitter::new(8);
        let mut differs = false;
        for _ in 0..1000 {
            let delay = a.delay(ceiling);
            assert!(delay <= ceiling);
            assert_eq!(delay, b.delay(ceiling));
            differs |= delay != other.delay(ceiling);
        }
        assert!(differs);
        assert_eq!(a.delay(Duration::ZERO), Duration::ZERO);
    }

    #[test]
    fn budgets_bound_messages_bytes_and_skip_missed_windows() {
        let now = Instant::now();
        let mut budget = RepairBudget::new(now);
        for _ in 0..TURN_MESSAGES {
            assert!(budget.admit(1, now));
        }
        assert!(!budget.admit(1, now));
        assert!(!budget.admit(1, now + Duration::from_millis(999)));
        let later = now + Duration::from_secs(20);
        assert!(budget.admit(TURN_BYTES, later));
        assert!(!budget.admit(1, later));
        // A backwards clock cannot replenish an exhausted window.
        assert!(!budget.admit(1, now));
        assert!(budget.admit(1, later + Duration::from_secs(1)));
        let mut work = RepairBudget::new(now);
        for _ in 0..TURN_MESSAGES {
            assert!(work.admit_work(now));
        }
        assert!(!work.admit_work(now));
        assert!(work.admit_work(now + Duration::from_secs(1)));
        let mut turn = Turn::new();
        assert!(!turn.admit(TURN_BYTES + 1));
        assert!(turn.admit(TURN_BYTES));
        assert!(!turn.admit(1));
    }
}
