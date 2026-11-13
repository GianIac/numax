//! Deterministic receipt tracking shared by production apply and its tests.
use std::collections::BTreeMap;

use nx_sync::{NodeId, OpId, OpPosition};
use serde_json::{from_slice, to_vec};

// Hard resource budgets, independent of replay/seen-ID retention. The byte
// budget includes serialized keys and values and is checked before committing.
pub(super) const MAX_ORIGINS: usize = 1024;
pub(super) const MAX_SPARSE: usize = 256;
pub(super) const MAX_TOTAL_SPARSE: usize = 8192;
pub(super) const MAX_TRACKING_BYTES: usize = 4 * 1024 * 1024;
pub(super) const MAX_GAP: u64 = 65_536;
pub(super) const SUMMARY_PAGE: usize = 16;
pub(super) const REPLAY_PAGE: u64 = 128;
// Fits a maximum 1 MiB guest value even in JSON byte-array form, plus metadata.
pub(super) const REPLAY_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug)]
pub(super) enum TrackingLimit {
    GapDistance,
    SparsePositions,
    GlobalBudget,
}

impl std::fmt::Display for TrackingLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::GapDistance => "operation exceeds gap-distance budget",
            Self::SparsePositions => "sparse-position budget exhausted",
            Self::GlobalBudget => "global history tracking budget exhausted",
        })
    }
}

impl std::error::Error for TrackingLimit {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationProgress {
    pub origin: NodeId,
    pub contiguous_sequence: u64,
    pub known_high_sequence: u64,
    pub sparse_positions: usize,
    /// A source could not supply a requested interval; another source may still have it.
    pub source_unavailable: bool,
    pub unavailable_evidence: Option<UnavailableHistory>,
    pub baseline_required: bool,
    /// The advertised unsequenced baseline has not been installed here.
    pub baseline_missing: bool,
    /// A complete transfer batch exists, but its flush has not been confirmed.
    pub baseline_unconfirmed: bool,
    /// Failure of the bounded recovery contract at one source, not global loss proof.
    pub recovery_failure: Option<RecoveryFailure>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryFailure {
    pub source: String,
    pub reason: nx_sync::BaselineFailure,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnavailableHistory {
    pub source: String,
    pub start: u64,
    pub end: u64,
    pub request_id: OpId,
}

type StoredUnavailable = Option<(String, u64, u64, OpId)>;

// Explicit JSON conversion avoids a new host dependency on serde derives.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct ReceiptTracker {
    pub contiguous: u64,
    pub high: u64,
    pub sparse: BTreeMap<u64, (u64, String)>,
    pub unavailable: Option<UnavailableHistory>,
    pub baseline_required: bool,
}

impl ReceiptTracker {
    pub fn validate_identity(&self, position: OpPosition, id: &str) -> anyhow::Result<()> {
        anyhow::ensure!(
            position.sequence > 0 && position.generation > 0,
            "zero operation position"
        );
        anyhow::ensure!(
            !id.is_empty() && id.len() <= 128,
            "invalid operation identity length"
        );
        if let Some((generation, previous_id)) = self.sparse.get(&position.sequence) {
            anyhow::ensure!(
                *generation == position.generation && previous_id == id,
                "conflicting origin sequence"
            );
        }
        Ok(())
    }

    pub fn observe(&mut self, position: OpPosition, id: &str) -> anyhow::Result<bool> {
        self.validate_identity(position, id)?;
        if position.sequence <= self.contiguous || self.sparse.contains_key(&position.sequence) {
            return Ok(false);
        }
        if position.sequence - self.contiguous > MAX_GAP {
            return Err(TrackingLimit::GapDistance.into());
        }
        if self.contiguous.checked_add(1) == Some(position.sequence) {
            self.contiguous = position.sequence;
            while let Some(next) = self.contiguous.checked_add(1) {
                if self.sparse.remove(&next).is_none() {
                    break;
                }
                self.contiguous = next;
            }
        } else {
            if self.sparse.len() >= MAX_SPARSE {
                return Err(TrackingLimit::SparsePositions.into());
            }
            self.sparse
                .insert(position.sequence, (position.generation, id.to_owned()));
        }
        self.high = self.high.max(position.sequence);
        if self
            .unavailable
            .as_ref()
            .is_some_and(|evidence| !self.has_missing(evidence.start, evidence.end))
        {
            self.unavailable = None;
        }
        Ok(true)
    }

    pub fn has_missing(&self, start: u64, end: u64) -> bool {
        let start = start.max(self.contiguous.saturating_add(1));
        if self.contiguous == u64::MAX || start > end {
            return false;
        }
        end - start >= self.sparse.len() as u64
            || self.sparse.range(start..=end).count() as u64 != end - start + 1
    }

    pub fn missing_page(&self) -> Option<(u64, u64)> {
        if self.contiguous >= self.high {
            return None;
        }
        let start = self.contiguous.checked_add(1)?;
        let end = self.high.min(start.saturating_add(REPLAY_PAGE - 1));
        // Include covered sparse positions so scattered gaps share a bounded round trip.
        Some((start, end))
    }

    pub fn encode(&self) -> anyhow::Result<Vec<u8>> {
        Ok(to_vec(&(
            self.contiguous,
            self.high,
            &self.sparse,
            self.unavailable.as_ref().map(|evidence| {
                (
                    &evidence.source,
                    evidence.start,
                    evidence.end,
                    &evidence.request_id,
                )
            }),
            self.baseline_required,
        ))?)
    }

    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(
            bytes.len() <= MAX_TRACKING_BYTES,
            "tracking record exceeds byte budget"
        );
        let (contiguous, high, sparse, unavailable, baseline_required): (
            u64,
            u64,
            BTreeMap<u64, (u64, String)>,
            StoredUnavailable,
            bool,
        ) = from_slice(bytes)?;
        anyhow::ensure!(
            high >= contiguous && sparse.len() <= MAX_SPARSE,
            "invalid receipt tracker"
        );
        for (seq, (generation, id)) in &sparse {
            anyhow::ensure!(
                *seq > contiguous
                    && *seq <= high
                    && *seq - contiguous <= MAX_GAP
                    && contiguous.checked_add(1) != Some(*seq),
                "invalid sparse position"
            );
            anyhow::ensure!(
                *generation > 0 && !id.is_empty() && id.len() <= 128,
                "invalid sparse identity"
            );
        }
        let unavailable = unavailable.map(|(source, start, end, request_id)| UnavailableHistory {
            source,
            start,
            end,
            request_id,
        });
        if let Some(evidence) = &unavailable {
            anyhow::ensure!(
                !evidence.source.is_empty()
                    && evidence.source.len() <= 512
                    && evidence.start > 0
                    && evidence.end >= evidence.start
                    && evidence.end - evidence.start < REPLAY_PAGE
                    && !evidence.request_id.as_str().is_empty()
                    && evidence.request_id.as_str().len() <= 128,
                "invalid unavailable history evidence"
            );
        }
        let tracker = Self {
            contiguous,
            high,
            sparse,
            unavailable,
            baseline_required,
        };
        anyhow::ensure!(
            tracker.unavailable.as_ref().is_none_or(|evidence| {
                evidence.end <= high && tracker.has_missing(evidence.start, evidence.end)
            }),
            "unavailable evidence no longer describes a gap"
        );
        Ok(tracker)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn position(sequence: u64) -> OpPosition {
        OpPosition {
            generation: 1,
            sequence,
        }
    }

    #[test]
    fn sparse_receipts_close_only_contiguous_prefix_and_survive_compaction() {
        let mut tracker = ReceiptTracker::default();
        assert!(tracker.observe(position(2), "b").unwrap());
        assert_eq!(tracker.contiguous, 0);
        assert_eq!(tracker.missing_page(), Some((1, 2)));
        assert!(!tracker.observe(position(2), "b").unwrap());
        assert!(tracker.observe(position(2), "conflict").is_err());
        assert!(tracker.observe(position(1), "a").unwrap());
        let mut restored = ReceiptTracker::decode(&tracker.encode().unwrap()).unwrap();
        assert_eq!(restored.contiguous, 2);
        assert!(restored.sparse.is_empty());
        assert!(
            !restored
                .observe(position(1), "compacted-id-is-unknown")
                .unwrap()
        );
        restored.high = 3;
        assert_eq!(restored.missing_page(), Some((3, 3)));
        assert_eq!(restored.contiguous, 2);
    }

    #[test]
    fn limits_reject_without_mutating_and_earliest_replay_can_progress() {
        let mut tracker = ReceiptTracker::default();
        let before = tracker.clone();
        assert!(tracker.observe(position(MAX_GAP + 1), "far").is_err());
        assert_eq!(tracker, before);
        for seq in 2..=MAX_SPARSE as u64 + 1 {
            tracker
                .observe(position(seq), &format!("op-{seq}"))
                .unwrap();
        }
        let before = tracker.clone();
        assert!(
            tracker
                .observe(position(MAX_SPARSE as u64 + 2), "full")
                .is_err()
        );
        assert_eq!(tracker, before);
        tracker.observe(position(1), "first").unwrap();
        assert_eq!(tracker.contiguous, MAX_SPARSE as u64 + 1);
    }

    #[test]
    fn far_ahead_summary_recovers_in_bounded_pages_without_advancing_on_maximum() {
        let mut tracker = ReceiptTracker {
            high: MAX_GAP + 1,
            ..ReceiptTracker::default()
        };
        let before = tracker.clone();
        assert!(tracker.observe(position(tracker.high), "far").is_err());
        assert_eq!(tracker, before);
        while let Some((start, end)) = tracker.missing_page() {
            assert!(end - start < REPLAY_PAGE);
            for sequence in start..=end {
                assert!(tracker.observe(position(sequence), "recovered").unwrap());
            }
        }
        assert_eq!(tracker.contiguous, MAX_GAP + 1);
        assert!(!tracker.observe(position(MAX_GAP + 1), "far").unwrap());
    }

    #[test]
    fn scattered_gaps_share_a_replay_page_and_received_positions_remain_duplicates() {
        let mut tracker = ReceiptTracker::default();
        for sequence in (2..=100).step_by(2) {
            tracker.observe(position(sequence), "received").unwrap();
        }
        assert_eq!(tracker.contiguous, 0);
        assert_eq!(tracker.missing_page(), Some((1, 100)));
        let mut applied = 0;
        for sequence in 1..=100 {
            applied += usize::from(tracker.observe(position(sequence), "received").unwrap());
        }
        assert_eq!(applied, 50);
        assert_eq!(tracker.contiguous, 100);
        assert!(tracker.sparse.is_empty());
        assert_eq!(tracker.missing_page(), None);
    }

    #[test]
    fn invalid_positions_and_sparse_generation_conflicts_preserve_tracking() {
        let mut tracker = ReceiptTracker::default();
        tracker.observe(position(2), "second").unwrap();
        for invalid in [
            OpPosition {
                generation: 2,
                sequence: 2,
            },
            OpPosition {
                generation: 0,
                sequence: 3,
            },
            position(0),
        ] {
            let before = tracker.clone();
            assert!(tracker.observe(invalid, "second").is_err());
            assert_eq!(tracker, before);
        }
        assert!(tracker.observe(position(3), "").is_err());
        assert!(tracker.observe(position(3), &"x".repeat(129)).is_err());
    }

    #[test]
    fn corrupt_receipts_fail_closed_and_exhausted_frontiers_do_not_wrap() {
        for bytes in [
            b"not-json".as_slice(),
            b"[2,1,{},false,false]",
            b"[0,2,{\"1\":[1,\"op\"]},false,false]",
            b"[0,2,{\"2\":[0,\"op\"]},false,false]",
        ] {
            assert!(ReceiptTracker::decode(bytes).is_err());
        }
        assert!(ReceiptTracker::decode(&vec![0; MAX_TRACKING_BYTES + 1]).is_err());
        let mut tracker = ReceiptTracker {
            contiguous: u64::MAX - 1,
            high: u64::MAX,
            ..ReceiptTracker::default()
        };
        assert_eq!(tracker.missing_page(), Some((u64::MAX, u64::MAX)));
        tracker.observe(position(u64::MAX), "last").unwrap();
        assert_eq!(tracker.contiguous, u64::MAX);
        assert_eq!(tracker.missing_page(), None);
        assert!(!tracker.observe(position(u64::MAX), "duplicate").unwrap());
    }
}
