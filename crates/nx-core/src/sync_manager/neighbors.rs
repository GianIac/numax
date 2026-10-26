//! Bounded topology policy shared by the runtime and deterministic tests.
//! Membership supplies admitted contacts, not authority to bypass a new handshake.
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use nx_net::{MembershipRecord, MembershipState};

use super::canonicalize_endpoint;

pub(super) const RECOVERY_CONTACT_LIMIT: usize = 64;
const WITHDRAWN_ENDPOINT_LIMIT: usize = 4096;
pub(super) const NEIGHBOR_ROTATION_INTERVAL: Duration = Duration::from_secs(30);

pub(super) struct Neighbors {
    discovery: BTreeSet<String>,
    // Keep withdrawals even when owner admission arrives after the source removal.
    withdrawn: BTreeSet<String>,
    recovery_suppressed: bool,
    cursor: usize,
    recovery_cursor: usize,
    last_rotation: Instant,
}

impl Neighbors {
    pub(super) fn new(now: Instant) -> Self {
        Self {
            discovery: BTreeSet::new(),
            withdrawn: BTreeSet::new(),
            recovery_suppressed: false,
            cursor: 0,
            recovery_cursor: 0,
            last_rotation: now,
        }
    }

    pub(super) fn candidates(
        &mut self,
        discovery: &[String],
        records: &[MembershipRecord],
        local_id: &str,
    ) -> Vec<String> {
        let current: BTreeSet<_> = discovery.iter().cloned().collect();
        self.withdrawn
            .retain(|endpoint| !current.contains(endpoint));
        for endpoint in self.discovery.difference(&current) {
            if self.withdrawn.len() == WITHDRAWN_ENDPOINT_LIMIT
                && !self.withdrawn.contains(endpoint)
            {
                // Never evict a withdrawal and silently authorize its endpoint
                // through a later membership snapshot. Discovery remains usable.
                self.recovery_suppressed = true;
                break;
            }
            self.withdrawn.insert(endpoint.clone());
        }
        self.discovery = current;

        // Left closes recovery even if discovery still advertises that endpoint.
        // A later owner admission with a fresh epoch supplies an eligible record.
        let left: BTreeSet<_> = records
            .iter()
            .filter(|record| record.state == MembershipState::Left)
            .filter_map(|record| record.endpoint.as_ref())
            .filter_map(|endpoint| canonicalize_endpoint(endpoint).ok())
            .collect();
        let mut candidates: Vec<_> = discovery
            .iter()
            .filter(|endpoint| !left.contains(*endpoint))
            .cloned()
            .collect();
        let mut seen: BTreeSet<_> = candidates.iter().cloned().collect();
        let mut recovery: Vec<_> = records
            .iter()
            .filter(|record| {
                !self.recovery_suppressed
                    && record.member != local_id
                    && record.state != MembershipState::Left
            })
            .filter_map(|record| record.endpoint.as_ref())
            .filter_map(|endpoint| canonicalize_endpoint(endpoint).ok())
            .filter(|endpoint| !self.withdrawn.contains(endpoint) && !left.contains(endpoint))
            .collect();
        recovery.sort();
        recovery.dedup();
        if !recovery.is_empty() {
            let start = self.recovery_cursor % recovery.len();
            for offset in 0..recovery.len().min(RECOVERY_CONTACT_LIMIT) {
                let endpoint = &recovery[(start + offset) % recovery.len()];
                if seen.insert(endpoint.clone()) {
                    candidates.push(endpoint.clone());
                }
            }
        }
        if !candidates.is_empty() {
            let start = self.cursor % candidates.len();
            candidates.rotate_left(start);
        }
        candidates
    }

    pub(super) fn recovery_suppressed(&self) -> bool {
        self.recovery_suppressed
    }

    /// Every turn advances the first dial, including turns interrupted by full slots.
    pub(super) fn advance(&mut self) {
        self.cursor = self.cursor.wrapping_add(1);
    }

    /// At most one active connection can be replaced in each interval, even if
    /// the replacement fails. Time comes from the caller, never a hidden clock.
    pub(super) fn rotate(&mut self, now: Instant, interval: Duration) -> bool {
        if now.saturating_duration_since(self.last_rotation) < interval {
            return false;
        }
        self.last_rotation = now;
        self.recovery_cursor = self.recovery_cursor.wrapping_add(RECOVERY_CONTACT_LIMIT);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nx_net::MembershipVersion;

    fn member(index: usize, state: MembershipState) -> MembershipRecord {
        MembershipRecord {
            member: format!("node-{index}"),
            version: MembershipVersion {
                epoch: 1,
                incarnation: 0,
            },
            state,
            endpoint: Some(format!("127.0.0.1:{}", 10000 + index)),
            reporter: format!("node-{index}"),
        }
    }

    #[test]
    fn dead_contact_survives_but_leave_and_discovery_withdrawal_stop_retries() {
        let mut policy = Neighbors::new(Instant::now());
        let mut record = member(1, MembershipState::Dead);
        let endpoint = record.endpoint.clone().unwrap();
        assert_eq!(
            policy.candidates(&[], &[record.clone()], "local"),
            vec![endpoint.clone()]
        );
        policy.candidates(std::slice::from_ref(&endpoint), &[record.clone()], "local");
        assert!(
            policy
                .candidates(&[], &[record.clone()], "local")
                .is_empty()
        );
        assert!(
            policy
                .candidates(&[], &[record.clone()], "local")
                .is_empty()
        );
        assert_eq!(
            policy.candidates(std::slice::from_ref(&endpoint), &[record.clone()], "local"),
            vec![endpoint.clone()]
        );
        record.state = MembershipState::Left;
        assert!(
            policy
                .candidates(&[endpoint], &[record], "local")
                .is_empty()
        );
    }

    #[test]
    fn withdrawal_before_owner_admission_cannot_be_reintroduced_by_membership() {
        let mut policy = Neighbors::new(Instant::now());
        let record = member(1, MembershipState::Alive);
        let endpoint = record.endpoint.clone().unwrap();
        policy.candidates(&[endpoint], &[], "local");
        assert!(policy.candidates(&[], &[], "local").is_empty());
        assert!(policy.candidates(&[], &[record], "local").is_empty());
    }

    #[test]
    fn withdrawal_budget_fails_closed_without_disabling_live_discovery() {
        let mut policy = Neighbors::new(Instant::now());
        let candidates: Vec<_> = (0..=WITHDRAWN_ENDPOINT_LIMIT)
            .map(|index| format!("127.0.0.1:{}", index + 1))
            .collect();
        policy.candidates(&candidates, &[], "local");
        policy.candidates(&[], &[], "local");
        assert_eq!(policy.withdrawn.len(), WITHDRAWN_ENDPOINT_LIMIT);
        assert!(policy.recovery_suppressed());
        let contact = member(1, MembershipState::Alive);
        assert!(policy.candidates(&[], &[contact], "local").is_empty());
        assert_eq!(
            policy.candidates(&candidates[..1], &[], "local"),
            candidates[..1]
        );
    }

    #[test]
    fn bounded_recovery_windows_rotate_and_time_does_not_refresh_on_snapshot_reads() {
        let now = Instant::now();
        let mut policy = Neighbors::new(now);
        let records: Vec<_> = (0..128)
            .map(|index| member(index, MembershipState::Dead))
            .collect();
        let first: BTreeSet<_> = policy
            .candidates(&[], &records, "local")
            .into_iter()
            .collect();
        assert_eq!(first.len(), RECOVERY_CONTACT_LIMIT);
        assert!(!policy.rotate(now, NEIGHBOR_ROTATION_INTERVAL));
        assert_eq!(
            policy
                .candidates(&[], &records, "local")
                .into_iter()
                .collect::<BTreeSet<_>>(),
            first
        );
        assert!(policy.rotate(now + NEIGHBOR_ROTATION_INTERVAL, NEIGHBOR_ROTATION_INTERVAL));
        assert!(!policy.rotate(now + NEIGHBOR_ROTATION_INTERVAL, NEIGHBOR_ROTATION_INTERVAL));
        let second: BTreeSet<_> = policy
            .candidates(&[], &records, "local")
            .into_iter()
            .collect();
        assert_eq!(second.len(), RECOVERY_CONTACT_LIMIT);
        assert!(first.is_disjoint(&second));
    }

    #[test]
    fn every_candidate_gets_first_turn_and_invalid_or_local_contacts_are_excluded() {
        let mut policy = Neighbors::new(Instant::now());
        let candidates = vec![
            "127.0.0.1:1".into(),
            "127.0.0.1:2".into(),
            "127.0.0.1:3".into(),
        ];
        for candidate in &candidates {
            assert_eq!(&policy.candidates(&candidates, &[], "local")[0], candidate);
            policy.advance();
        }
        let mut invalid = member(1, MembershipState::Alive);
        invalid.endpoint = Some("0.0.0.0:0".into());
        let own = member(2, MembershipState::Alive);
        assert!(policy.candidates(&[], &[invalid, own], "node-2").is_empty());
    }
}
